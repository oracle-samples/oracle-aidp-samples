"""Tests for the aidp-fusion-status health classifier.

Pins the truthful state-vs-live cross-reference: health keys off the LIVE table
(evidence), with the state row as overlay — so a 'success' state row next to a
missing table reads STALE, and a table with no state row reads UNTRACKED.
"""
from __future__ import annotations

import sys
from pathlib import Path

_SKILL = Path(__file__).resolve().parents[2] / "skills" / "aidp-fusion-status"
sys.path.insert(0, str(_SKILL))

import status_report  # noqa: E402


def _classify(state, live, known=None):
    out = status_report.build_status({"state": state, "live": live, "known_nodes": known or []})
    return {n["dataset"]: n["health"] for n in out["nodes"]}, out


def test_healthy_success_with_live_rows():
    h, _ = _classify(
        [{"dataset_id": "ar_invoice_summary", "layer": "gold", "status": "success", "row_count": 49}],
        {"ar_invoice_summary": {"exists": True, "row_count": 49}},
    )
    assert h["ar_invoice_summary"] == "HEALTHY"


def test_stale_success_state_but_table_missing():
    # The load-bearing case: state says built, live table absent -> STALE.
    h, out = _classify(
        [{"dataset_id": "supplier_spend", "layer": "gold", "status": "success", "row_count": 100}],
        {},  # no live table
    )
    assert h["supplier_spend"] == "STALE"
    assert "supplier_spend" in out["attention"]


def test_failed_status():
    h, out = _classify(
        [{"dataset_id": "gl_balance", "layer": "gold", "status": "failed"}],
        {"gl_balance": {"exists": True, "row_count": 5}},
    )
    assert h["gl_balance"] == "FAILED"          # failed overrides live presence
    assert "gl_balance" in out["attention"]


def test_skipped_carries_reason():
    _, out = _classify(
        [{"dataset_id": "dim_calendar", "layer": "silver", "status": "skipped",
          "skip_reason": "cascade"}],
        {"dim_calendar": {"exists": True, "row_count": 4018}},
    )
    node = next(n for n in out["nodes"] if n["dataset"] == "dim_calendar")
    assert node["health"] == "SKIPPED" and node["skipReason"] == "cascade"


def test_deferred_is_not_stale_or_attention():
    h, out = _classify(
        [{"dataset_id": "po_backlog", "layer": "gold", "status": "deferred"}],
        {},
    )
    assert h["po_backlog"] == "DEFERRED"
    assert "po_backlog" not in out["attention"]   # intentional, not an alert


def test_empty_ok_success_zero_rows():
    h, _ = _classify(
        [{"dataset_id": "ap_aging", "layer": "gold", "status": "success", "row_count": 0}],
        {"ap_aging": {"exists": True, "row_count": 0}},
    )
    assert h["ap_aging"] == "EMPTY_OK"


def test_untracked_live_table_without_state_row():
    h, out = _classify(
        [],
        {"gold.mystery_mart": {"exists": True, "row_count": 7}},
    )
    assert h["mystery_mart"] == "UNTRACKED"
    assert "mystery_mart" in out["attention"]


def test_audit_and_underscore_tables_not_untracked():
    h, _ = _classify(
        [],
        {"fusion_autopilot_state": {"exists": True, "row_count": 30},
         "_phase5_probe": {"exists": True, "row_count": 1}},
    )
    assert "fusion_autopilot_state" not in h and "_phase5_probe" not in h


def test_never_run_declared_node_absent_everywhere():
    h, out = _classify([], {}, known=["po_backlog"])
    assert h["po_backlog"] == "NEVER_RUN"
    assert "po_backlog" in out["attention"]


def test_schema_qualified_live_keys_match_by_short_name():
    h, _ = _classify(
        [{"dataset_id": "gl_balance", "layer": "gold", "status": "success", "row_count": 10}],
        {"fusion_catalog.gold.gl_balance": {"exists": True, "row_count": 10}},
    )
    assert h["gl_balance"] == "HEALTHY"
