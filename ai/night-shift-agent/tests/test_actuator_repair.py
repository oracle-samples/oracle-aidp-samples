"""The external actuator drains an approved statement_repair: the replay-verdict load keyed by the DRAFT hash, the
quarantine lifted by the approval, _apply_statement_repair writing the runbook even in dry_run (it is a ledger row, not a
control-plane call), and the DENIED_ON_EXECUTE path when the verdict is missing, stale or filed under the wrong hash."""
from __future__ import annotations

import pytest

from helpers import DSHA, OPS, ROW_NORMAL, SSHA, TABLE, UPSERT, FakeIO, RepairStore, _approved_row, _repair, run


@pytest.mark.parametrize("dry_run", [True, False])
def test_tick_executes_approved_repair_with_fresh_verdict(dry_run):
    a = _repair()
    store = RepairStore([(DSHA, "replay_safe", 0.5)], budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)])
    rep = run(FakeIO(), store, dry_run=dry_run)
    assert rep.approvals_executed == 1
    assert rep.decisions[0].decision == "allow" and rep.decisions[0].rule_id == "tier.repair_quarantine"
    assert rep.outcomes[a.signature]["op"] == "runbook_upsert" and rep.outcomes[a.signature]["approved_by"] == "reviewer"
    assert store.count(f"MERGE INTO {OPS}.runbooks") == 1
    assert "'applied'" in store.sql[store.idx(f"INSERT INTO {OPS}.actions")]
    assert "applied by tick" in store.sql[store.idx("SET status = 'executed'")]
    assert store.idx(f"INSERT INTO {OPS}.policy_decisions") < store.idx(f"MERGE INTO {OPS}.runbooks")


@pytest.mark.parametrize("rows,reason", [
    ([], "NO_REPLAY_VERDICT"),
    ([(DSHA, "replay_safe", 30.0)], "STALE"),
    ([(DSHA, "silent_wrong", 0.5)], "SILENT_WRONG"),
    ([(SSHA, "replay_safe", 0.5)], "NO_REPLAY_VERDICT"),       # verdict filed under the ORIGINAL statement, not the draft
])
def test_tick_leaves_repair_approved_when_verdict_missing_stale_or_wrong(rows, reason):
    a = _repair()
    store = RepairStore(rows, budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)])
    rep = run(FakeIO(), store)
    assert rep.approvals_executed == 0 and store.count(f"MERGE INTO {OPS}.runbooks") == 0
    note = store.sql[store.idx("DENIED_ON_EXECUTE")]
    assert note.startswith(f"UPDATE {OPS}.approvals SET status = 'approved'") and "verify.replay_required" in note and reason in note


def test_tick_marks_repair_failed_when_draft_row_disagrees_with_approval():
    a = _repair()
    store = RepairStore([(DSHA, "replay_safe", 0.5)], draft_row=(UPSERT, TABLE, "inc1", "z" * 64, "accepted"),
                        budget_rows=[ROW_NORMAL], approved_rows=[_approved_row(a)])
    rep = run(FakeIO(), store, dry_run=False)
    assert rep.approvals_executed == 0 and store.count(f"MERGE INTO {OPS}.runbooks") == 0
    failed = store.sql[store.idx("SET status = 'failed'")]
    assert "different statement" in failed
