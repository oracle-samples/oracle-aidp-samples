"""The shared approval drain (nightshift.drain), as the heartbeat calls it: a statement_repair a person approved becomes
a runbook row through the gate, a stale or missing verdict leaves it approved with DENIED_ON_EXECUTE, control-plane kinds
are left for the external actuator, and a bad row is marked failed. Fakes come from tests/helpers.py."""
from __future__ import annotations

import pytest

from nightshift import gate
from nightshift.drain import drain_approvals
from nightshift.types import Decision

from helpers import (DSHA, OPS, ROW_NORMAL, T0, UPSERT, TABLE, FakeIO, RepairStore, _action_json, _approved_row, _ctx, _quarantined_action,
                     _repair)


def _engine(store, written: list):
    def write_decision(d: Decision, a, provenance="heartbeat", principal=None):
        written.append((d.decision, d.rule_id, provenance, principal))
        store.run_sql(f"INSERT INTO {OPS}.policy_decisions VALUES ('{d.decision}')")
        return "did"

    engine = gate.PolicyEngine(gate.default_rules(), lambda d, a: write_decision(d, a), decision_counter=lambda *a, **k: 0)
    return engine, write_decision


def test_fresh_verdict_repair_becomes_runbook_and_is_marked_executed():
    a = _repair()
    store = RepairStore([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)])
    ctx = _ctx(FakeIO())
    written: list = []
    engine, wd = _engine(store, written)
    events = []
    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0, log=lambda ev, **kw: events.append((ev, kw)))
    assert rep["expired"] == "ok" and rep["errors"] == [] and rep["left"] == []
    assert rep["executed"] == [{"approval_id": "ap-1", "runbook_id": rep["executed"][0]["runbook_id"], "approved_by": "reviewer"}]
    # the gate records its quarantine first, then the lift, attributed to the person who approved
    assert written == [("quarantine", "tier.repair_quarantine", "heartbeat", None), ("allow", "tier.repair_quarantine", "approval", "reviewer")]
    assert store.count(f"MERGE INTO {OPS}.runbooks") == 1
    assert store.idx(f"UPDATE {OPS}.approvals SET status = 'expired'") < store.idx(f"MERGE INTO {OPS}.runbooks")
    assert store.idx(f"INSERT INTO {OPS}.policy_decisions") < store.idx(f"MERGE INTO {OPS}.runbooks")   # decision row first
    assert "'applied'" in store.sql[store.idx(f"INSERT INTO {OPS}.actions")]
    assert "applied by tick tick0000" in store.sql[store.idx("SET status = 'executed'")]
    assert [e for e, _ in events] == ["approval_executed"] and events[0][1]["approved_by"] == "reviewer"
    assert "approved_signatures" not in ctx.kv


@pytest.mark.parametrize("rows,reason", [
    ([], "NO_REPLAY_VERDICT"),
    ([(DSHA, "replay_safe", 30.0)], "STALE"),
    ([(DSHA, "silent_wrong", 0.5)], "SILENT_WRONG"),
])
def test_missing_stale_or_wrong_verdict_leaves_the_row_approved(rows, reason):
    a = _repair()
    store = RepairStore(rows, budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)])
    ctx = _ctx(FakeIO())
    engine, wd = _engine(store, [])
    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0)
    assert rep["executed"] == [] and store.count(f"MERGE INTO {OPS}.runbooks") == 0
    assert rep["left"][0]["approval_id"] == "ap-1" and "verify.replay_required" in rep["left"][0]["reason"] and reason in rep["left"][0]["reason"]
    note = store.sql[store.idx("DENIED_ON_EXECUTE")]
    assert note.startswith(f"UPDATE {OPS}.approvals SET status = 'approved'")


def test_control_plane_kinds_are_left_for_the_actuator_without_evaluation():
    a = _quarantined_action(FakeIO())   # a cluster_update the budget rule quarantined
    store = RepairStore([], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a, aid="ap-cl")])
    ctx = _ctx(FakeIO())
    written: list = []
    engine, wd = _engine(store, written)
    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0)
    assert rep["executed"] == [] and written == [] and rep["errors"] == []
    assert rep["left"] == [{"approval_id": "ap-cl", "kind": "cluster_update", "reason": rep["left"][0]["reason"]}]
    assert "external actuator" in rep["left"][0]["reason"]
    assert store.count(f"UPDATE {OPS}.approvals SET status") == 1   # only the expiry sweep touched the table


def test_signature_mismatch_and_bad_json_are_marked_failed():
    a = _repair()
    bad_sig = ("ap-sig", "not-the-signature", _action_json(a), "reviewer", None)
    bad_json = ("ap-json", a.signature, "{not json", "reviewer", None)
    store = RepairStore([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[bad_sig, bad_json])
    ctx = _ctx(FakeIO())
    engine, wd = _engine(store, [])
    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0)
    assert rep["executed"] == [] and store.count(f"MERGE INTO {OPS}.runbooks") == 0
    assert sorted(e.split(":")[0] for e in rep["errors"]) == ["ap-json", "ap-sig"]
    assert "SIGNATURE_MISMATCH" in store.sql[store.idx("'ap-sig'")] or any("SIGNATURE_MISMATCH" in s for s in store.sql)
    assert any("BAD_ACTION_JSON" in s for s in store.sql)


def test_apply_failure_marks_the_row_failed_and_reports_it():
    a = _repair()
    # the draft row names another statement: _apply_statement_repair refuses, the row is failed, nothing is written
    store = RepairStore([(DSHA, "replay_safe", 0.5)], draft_row=(UPSERT, TABLE, "inc1", "z" * 64, "accepted"),
                        budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)])
    ctx = _ctx(FakeIO())
    engine, wd = _engine(store, [])
    events = []
    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0, log=lambda ev, **kw: events.append(ev))
    assert rep["executed"] == [] and len(rep["errors"]) == 1 and "different statement" in rep["errors"][0]
    assert store.count(f"MERGE INTO {OPS}.runbooks") == 0
    assert "'failed'" in store.sql[store.idx(f"INSERT INTO {OPS}.actions")]
    assert events == ["approval_failed"]


def test_read_failures_fail_closed():
    a = _repair()
    class FailingVerdicts(RepairStore):
        def run_sql(self, sql, timeout=300):
            if "ROW_NUMBER()" in sql:
                raise RuntimeError("verdict read failed")
            return super().run_sql(sql, timeout)

    store = FailingVerdicts([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)])
    ctx = _ctx(FakeIO())
    engine, wd = _engine(store, [])
    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0)
    assert rep["executed"] == [] and any("replay verdicts read failed" in e for e in rep["errors"])
    assert rep["left"] and "NO_REPLAY_VERDICT" in rep["left"][0]["reason"]


@pytest.mark.parametrize("by,approvers,reason", [
    ("", None, "missing or a service identity"),
    ("unknown", None, "missing or a service identity"),
    ("dataflow", None, "missing or a service identity"),
    ("nightshift_heartbeat", None, "missing or a service identity"),
    ("reviewer", ["priya", "lee"], "not on the NS_APPROVERS list"),
])
def test_service_or_unlisted_approver_cannot_lift_the_quarantine(by, approvers, reason):
    a = _repair()
    store = RepairStore([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a, by=by)])
    ctx = _ctx(FakeIO())
    written: list = []
    engine, wd = _engine(store, written)
    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0, approvers=approvers)
    assert rep["executed"] == [] and written == [] and store.count(f"MERGE INTO {OPS}.runbooks") == 0
    assert rep["left"][0]["reason"].startswith("approver.identity") and reason in rep["left"][0]["reason"]
    assert "DENIED_ON_EXECUTE approver.identity" in store.sql[store.idx("DENIED_ON_EXECUTE")]


def test_listed_approver_passes_the_identity_check():
    a = _repair()
    store = RepairStore([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a, by="Reviewer")])
    engine, wd = _engine(store, [])
    rep = drain_approvals(store, OPS, _ctx(FakeIO()), engine, wd, "tick0000", T0, approvers=["reviewer", "priya"])
    assert len(rep["executed"]) == 1 and store.count(f"MERGE INTO {OPS}.runbooks") == 1


def test_forged_repair_approval_without_a_quarantine_decision_is_marked_failed():
    """The in-cluster drain needs the gate's own quarantine decision for the action too: a statement_repair row
    inserted straight into approvals writes no runbook."""
    a = _repair()
    store = RepairStore([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)], traced=False)
    engine, wd = _engine(store, [])
    rep = drain_approvals(store, OPS, _ctx(FakeIO()), engine, wd, "tick0000", T0)
    assert rep["executed"] == [] and store.count(f"MERGE INTO {OPS}.runbooks") == 0
    assert "approval.trace: NO_QUARANTINE_DECISION" in rep["errors"][0]
    assert "approval.trace" in store.sql[store.idx("SET status = 'failed'")]


def test_repair_claimed_by_another_drainer_is_left_alone():
    a = _repair()
    store = RepairStore([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)], claim_lost=True)
    engine, wd = _engine(store, [])
    rep = drain_approvals(store, OPS, _ctx(FakeIO()), engine, wd, "tick0000", T0)
    assert rep["executed"] == [] and store.count(f"MERGE INTO {OPS}.runbooks") == 0
    assert rep["left"] == [{"approval_id": "ap-1", "kind": "statement_repair", "reason": "claimed by another drainer"}]



# ----------------------------------------------------------------------------- a ledger write that fails mid-drain
class FailingWrites(RepairStore):
    """RepairStore where a statement containing `needle` raises, the way a concurrent update or a missing grant does."""

    def __init__(self, needle, *a, **kw):
        super().__init__(*a, **kw)
        self.needle = needle

    def run_sql(self, sql, timeout=300):
        if self.needle in sql:
            raise RuntimeError("ConcurrentAppendException: files were added by a concurrent update")
        return super().run_sql(sql, timeout)


def test_a_row_whose_ledger_write_fails_is_reported_and_the_next_row_is_still_drained():
    a = _repair()
    bad_sig = ("ap-sig", "not-the-signature", _action_json(a), "reviewer", None)
    store = FailingWrites("SIGNATURE_MISMATCH", [(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL],
                          approved_rows=[bad_sig, _approved_row(a)])
    engine, wd = _engine(store, [])

    rep = drain_approvals(store, OPS, _ctx(FakeIO()), engine, wd, "tick0000", T0)

    assert [e["approval_id"] for e in rep["executed"]] == ["ap-1"]
    assert len(rep["errors"]) == 1
    assert rep["errors"][0].startswith("ap-sig: ") and "ConcurrentAppendException" in rep["errors"][0]


def test_an_outcome_write_that_fails_after_the_claim_is_reported_not_raised():
    """The runbook is written and the row is this drainer's; the write that would mark it executed fails. The drain
    writes nothing else to the row (it is left executing, the state that expires and is never picked up again), and
    the report says which row and why."""
    a = _repair()
    store = FailingWrites("SET status = 'executed'", [(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL],
                          approved_rows=[_approved_row(a)])
    engine, wd = _engine(store, [])
    ctx = _ctx(FakeIO())
    events = []

    rep = drain_approvals(store, OPS, ctx, engine, wd, "tick0000", T0, log=lambda ev, **kw: events.append(ev))

    assert rep["executed"] == [] and rep["left"] == []
    assert len(rep["errors"]) == 1
    assert rep["errors"][0].startswith("ap-1: ") and "ConcurrentAppendException" in rep["errors"][0]
    assert events == ["approval_drain_error"]
    assert "approved_signatures" not in ctx.kv
    assert store.count(f"MERGE INTO {OPS}.runbooks") == 1
    row_writes = [s for s in store.sql if s.startswith(f"UPDATE {OPS}.approvals SET status") and "'ap-1'" in s]
    assert len(row_writes) == 1 and row_writes[0].startswith(f"UPDATE {OPS}.approvals SET status = 'executing'")
