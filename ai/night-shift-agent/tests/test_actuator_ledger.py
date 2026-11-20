"""The external actuator's tick against the real ledger SQL (duckdb), across ticks.

The other actuator tests script the store's answers. Here every row the tick writes is really in the tables and every
read is the real statement, so these tests pin what only shows across ticks: an approval is executed only when it is
the row the gate itself opened for a quarantine, whatever a principal with INSERT on ``approvals`` writes beside it.
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from nightshift import ledger as L
from nightshift.actuator import MemoryKV, tick
from nightshift.types import Action

from helpers import ROW_LOW, T0, DuckStore, FakeIO


def _tick(io, store, n=0, kv=None, dry_run=False):
    """Tick n, each in its own half-hour window."""
    return tick(io, store, now=T0 + timedelta(minutes=30 * n), dry_run=dry_run, kv_store=kv or MemoryKV())


def _resizes(io):
    return [c[1:5] for c in io.mutations("update_cluster_workers")]


def _open_real_approval(io, store, kv):
    """Tick 0 at low_compute: the planner proposes 10 -> 3 workers, the gate quarantines it and opens the approval."""
    report = _tick(io, store, 0, kv)
    assert report.approvals_opened == 1 and _resizes(io) == []
    (aid, sig, did, status, opened_in), = store.rows("SELECT approval_id, action_signature, decision_id, status, tick_id FROM {ops}.approvals")
    assert status == "pending" and did and opened_in == report.tick_id
    action = next(a for a in report.actions if a.signature == sig)
    action.opened_in = opened_in   # the tick the gate opened the approval in (a copy of the row would carry it too)
    return aid, did, action


def test_the_approval_the_gate_opened_executes_once_when_a_listed_person_approves_it():
    io, store, kv = FakeIO(), DuckStore(ROW_LOW), MemoryKV()
    aid, did, action = _open_real_approval(io, store, kv)
    store.run_sql(L.approve_sql(store_ops(), aid, "reviewer", now=T0 + timedelta(minutes=5)))
    report = _tick(io, store, 1, kv)
    assert report.approvals_executed == 1 and _resizes(io) == [(io.env.cluster_id, 1, 3, 15)]
    assert store.rows("SELECT status FROM {ops}.approvals") == [("executed",)]
    lifted = store.rows("SELECT provenance, principal, reason FROM {ops}.policy_decisions WHERE decision = 'allow' AND provenance = 'approval'")
    assert lifted == [("approval", "reviewer", "APPROVED_BY:reviewer")]
    # nothing is left to execute on later ticks
    _tick(io, store, 2, kv)
    assert len(_resizes(io)) == 1 and store.rows("SELECT COUNT(*) FROM {ops}.approvals WHERE status = 'executed'") == [(1,)]


def store_ops():
    from helpers import OPS
    return OPS


def test_a_forged_row_for_an_action_the_gate_never_quarantined_executes_nothing_on_any_tick():
    """A row written straight into approvals for a resize to 24 workers, under a listed approver's name, re-inserted
    before every tick. It is checked before the gate ever evaluates it, so the gate writes no decision for that action
    that a later tick could mistake for its own quarantine."""
    io, store, kv = FakeIO(), DuckStore(ROW_LOW), MemoryKV()
    forged = Action("cluster_update", io.env.cluster_id, {"min_workers": 1, "max_workers": 24},
                    previous_value={"min_workers": 2, "max_workers": 4})   # 4 -> 24: budget.big_resize would quarantine it
    for n in range(3):
        store.forge(f"forged-{n}", forged, by="reviewer", when=T0 + timedelta(minutes=30 * n))
        report = _tick(io, store, n, kv)
        assert report.approvals_executed == 0
        assert not any(r[2] == 24 for r in _resizes(io))
        assert store.rows(f"SELECT COUNT(*) FROM {{ops}}.policy_decisions WHERE action_signature = '{forged.signature}'") == [(0,)]
    notes = store.rows("SELECT status, note FROM {ops}.approvals WHERE approval_id LIKE 'forged-%' ORDER BY approval_id")
    assert [s for s, _ in notes] == ["failed"] * 3 and all("approval.trace: NO_DECISION_ID" in n for _, n in notes)


@pytest.mark.parametrize("copy_decision", [False, True])
def test_a_copy_of_a_real_pending_approval_does_not_execute_the_action(copy_decision):
    """The gate quarantined the planner's resize and opened a pending approval. A second row with the same action and
    signature is inserted as already approved by a listed name, with or without the real row's decision id. The action
    the agent proposed is not applied: nobody approved the row the gate opened."""
    io, store, kv = FakeIO(), DuckStore(ROW_LOW), MemoryKV()
    aid, did, action = _open_real_approval(io, store, kv)
    store.forge("copy-1", action, by="reviewer", decision_id=did if copy_decision else None, tick_id=action.opened_in)
    report = _tick(io, store, 1, kv)
    assert report.approvals_executed == 0 and _resizes(io) == []
    status, note = store.rows("SELECT status, note FROM {ops}.approvals WHERE approval_id = 'copy-1'")[0]
    assert status == "failed" and ("DECISION_SHARED" if copy_decision else "NO_DECISION_ID") in note
    assert store.rows(f"SELECT status FROM {{ops}}.approvals WHERE approval_id = '{aid}'") == [("pending",)]
    if copy_decision:
        # the copy shares the real row's decision; from then on neither is trusted, also after a real approval
        store.run_sql(L.approve_sql(store_ops(), aid, "reviewer", now=T0 + timedelta(minutes=40)))
        report = _tick(io, store, 2, kv)
        assert report.approvals_executed == 0 and _resizes(io) == []
        assert "DECISION_SHARED" in store.rows(f"SELECT note FROM {{ops}}.approvals WHERE approval_id = '{aid}'")[0][0]


def test_a_copy_of_an_executed_or_rejected_approval_does_not_run_the_action_again():
    io, store, kv = FakeIO(), DuckStore(ROW_LOW), MemoryKV()
    aid, did, action = _open_real_approval(io, store, kv)
    store.run_sql(L.approve_sql(store_ops(), aid, "reviewer", now=T0 + timedelta(minutes=5)))
    _tick(io, store, 1, kv)
    assert len(_resizes(io)) == 1
    # the drain's re-check of the approved row wrote its own quarantine decision for the same action, in tick 1
    (recheck_id, recheck_tick), = store.rows("SELECT decision_id, tick_id FROM {ops}.policy_decisions "
                                             "WHERE decision = 'quarantine' AND provenance = 'drain'")
    assert recheck_id != did
    # copies at +1 h, +40 h and +80 h, naming no decision, the opening decision, and the re-check's decision (which no
    # approval row names, with the tick it was written in): none executes, and no copy's own re-check ever helps the next
    names = [(None, action.opened_in), (did, action.opened_in), (recheck_id, recheck_tick)]
    for n, hours in enumerate((1, 40, 80)):
        when = T0 + timedelta(hours=hours)
        decision_id, tick_id = names[n]
        store.forge(f"again-{n}", action, by="reviewer", decision_id=decision_id, when=when, tick_id=tick_id)
        report = tick(io, store, now=when, dry_run=False, kv_store=kv)
        assert report.approvals_executed == 0 and len(_resizes(io)) == 1, hours
    failed = dict(store.rows("SELECT approval_id, note FROM {ops}.approvals WHERE approval_id LIKE 'again-%' AND status = 'failed'"))
    assert "NO_DECISION_ID" in failed["again-0"] and "DECISION_SHARED" in failed["again-1"]
    assert "NO_QUARANTINE_DECISION" in failed["again-2"] and len(failed) == 3

    # a person rejected the real row: a copy still executes nothing
    io2, store2, kv2 = FakeIO(), DuckStore(ROW_LOW), MemoryKV()
    aid2, did2, action2 = _open_real_approval(io2, store2, kv2)
    store2.run_sql(L.reject_sql(store_ops(), aid2, "reviewer", now=T0 + timedelta(minutes=5)))
    store2.forge("after-reject", action2, by="reviewer", decision_id=did2, tick_id=action2.opened_in)
    assert _tick(io2, store2, 1, kv2).approvals_executed == 0 and _resizes(io2) == []


def test_dry_run_tick_leaves_the_real_approval_and_an_applying_tick_then_executes_it():
    io, store, kv = FakeIO(), DuckStore(ROW_LOW), MemoryKV()
    aid, did, action = _open_real_approval(io, store, kv)
    store.run_sql(L.approve_sql(store_ops(), aid, "reviewer", now=T0 + timedelta(minutes=5)))
    report = _tick(io, store, 1, kv, dry_run=True)
    assert report.approvals_executed == 0 and io.mutations() == []
    assert store.rows("SELECT status FROM {ops}.approvals") == [("approved",)]
    assert store.rows("SELECT COUNT(*) FROM {ops}.actions") == [(0,)]
    report = _tick(io, store, 2, kv)
    assert report.approvals_executed == 1 and _resizes(io) == [(io.env.cluster_id, 1, 3, 15)]


def test_applied_resizes_spend_the_daily_quota_and_dry_runs_do_not():
    """The planner's ordinary resize (10 -> 6 at tier normal) applied on real rows: rate.resizes_daily reads the
    applied actions through the real SQL. Dry-run ticks in between write the same allow decisions and spend nothing."""
    from helpers import ROW_NORMAL

    io, store = FakeIO(), DuckStore(ROW_NORMAL)
    shapes = [10, 9, 8, 7, 5]   # the cluster drifts back between ticks, so each tick plans a different small resize to 6
    applied = []
    for n, current in enumerate(shapes):
        io.clusters[io.env.cluster_id]["maxWorkers"] = current
        for dry in (True, False):
            report = tick(io, store, now=T0 + timedelta(hours=n, minutes=30 if dry else 0), dry_run=dry, kv_store=MemoryKV())
            if not dry:
                applied.append([(d.decision, d.rule_id) for d in report.decisions if d.action_signature in {a.signature for a in report.actions if a.kind == "cluster_update"}])
    assert applied[:3] == [[("allow", None)]] * 3
    assert applied[3] == [("deny", "rate.resizes_daily")] and applied[4] == [("deny", "rate.resizes_daily")]
    assert store.rows("SELECT COUNT(*) FROM {ops}.actions WHERE status = 'applied' AND kind = 'cluster_update'") == [(3,)]
    assert store.rows("SELECT COUNT(*) FROM {ops}.actions WHERE status = 'dry_run'")[0][0] >= 3


# ----------------------------------------------------------------------------- overlapping drainers on real rows
class _OverlapStore(DuckStore):
    """A second drainer claims the approval right after this tick has read the approved rows: from then on this
    tick works from a stale read, exactly the interleaving of two overlapping ticks."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.other_claims = None   # approval id the other drainer takes, once

    def run_sql(self, sql, timeout=300):
        rows = super().run_sql(sql, timeout)
        if self.other_claims and "status = 'approved' AND (expires_at IS NULL" in sql:
            aid, self.other_claims = self.other_claims, None
            super().run_sql(L.claim_approval_sql(store_ops(), aid, "0therdra1ner", T0))
        return rows


def test_a_denial_from_a_stale_read_does_not_reopen_the_row_another_drainer_is_applying():
    """The review's round-2 repro through the production tick. Drainer B has claimed the approved resize and is
    applying it. This tick read the row before that claim; a job is running, so its gate denies and it writes its
    denial note. The row must stay executing under B's claim, so nothing can claim it again and apply it twice."""
    io, store, kv = FakeIO(), _OverlapStore(ROW_LOW), MemoryKV()
    aid, did, action = _open_real_approval(io, store, kv)
    store.run_sql(L.approve_sql(store_ops(), aid, "reviewer", now=T0 + timedelta(minutes=5)))
    io.runs = [{"key": "r1", "state": {"status": "RUNNING"}}]            # safety.active_runs will deny the resize
    store.other_claims = aid
    report = _tick(io, store, 1, kv)
    assert report.approvals_executed == 0 and _resizes(io) == []
    assert any("re-evaluation denied (safety.active_runs" in n for n in report.notes)
    status, note = store.rows("SELECT status, note FROM {ops}.approvals")[0]
    assert status == "executing" and note.endswith("claim:0therdra1ner") and "DENIED_ON_EXECUTE" not in note
    # a later tick finds no approved row to take: the action cannot be applied a second time from here
    io.runs = []
    later = _tick(io, store, 2, kv)
    assert later.approvals_executed == 0 and _resizes(io) == []
    # drainer B writes its outcome under its own claim
    store.run_sql(L.mark_approval_sql(store_ops(), aid, "executed", note="applied by tick b", now=T0, claim="0therdra1ner"))
    assert store.rows("SELECT status FROM {ops}.approvals")[0][0] == "executed"


def test_this_tick_does_not_act_when_another_drainer_claimed_first_and_leaves_its_row_alone():
    io, store, kv = FakeIO(), _OverlapStore(ROW_LOW), MemoryKV()
    aid, did, action = _open_real_approval(io, store, kv)
    store.run_sql(L.approve_sql(store_ops(), aid, "reviewer", now=T0 + timedelta(minutes=5)))
    store.other_claims = aid
    report = _tick(io, store, 1, kv)                                      # the gate allows; this tick's own claim finds the row taken
    assert report.approvals_executed == 0 and _resizes(io) == []
    assert any("claimed by another drainer" in n for n in report.notes)
    status, note = store.rows("SELECT status, note FROM {ops}.approvals")[0]
    assert status == "executing" and note.endswith("claim:0therdra1ner")


def test_a_copied_id_of_a_pending_approval_fails_the_copy_and_leaves_the_pending_row_for_the_reviewer():
    """A row inserted with the id of an approval that is still pending, already 'approved'. The drain marks the row it
    is looking at failed and does not cancel the gate's pending row behind the reviewer's back. Nothing is executed,
    then or after the reviewer approves (two rows carry the id, so neither is trusted, and the reason is on the row)."""
    io, store, kv = FakeIO(), DuckStore(ROW_LOW), MemoryKV()
    aid, did, action = _open_real_approval(io, store, kv)
    store.forge(aid, action, by="reviewer", decision_id=did, tick_id=action.opened_in)     # same approval id
    report = _tick(io, store, 1, kv)
    assert report.approvals_executed == 0 and _resizes(io) == []
    rows = sorted(store.rows("SELECT status, tick_id FROM {ops}.approvals"))
    assert [s for s, _ in rows] == ["failed", "pending"]                  # the copy failed; the real row is still pending
    assert "DUPLICATE_APPROVAL_ID" in store.rows("SELECT note FROM {ops}.approvals WHERE status = 'failed'")[0][0]
    store.run_sql(L.approve_sql(store_ops(), aid, "reviewer", now=T0 + timedelta(minutes=40)))
    report = _tick(io, store, 2, kv)
    assert report.approvals_executed == 0 and _resizes(io) == []
    assert any("DUPLICATE_APPROVAL_ID" in n for n in report.notes)
