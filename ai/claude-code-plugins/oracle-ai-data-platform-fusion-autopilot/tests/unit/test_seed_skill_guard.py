"""Destructive-guard fail-closed matrix — the load-bearing safety test.

``seed`` does `CREATE OR REPLACE TABLE` on silver/gold, so the guard MUST
confirm whenever target-table emptiness cannot be *proven*. This drives
``guard.classify_guard`` through every state and asserts that exactly one —
all-targets-empty-and-readable — proceeds without a prompt.

Two fail-open regressions this pins:
  - **State ≠ tables**: a populated physical target forces confirm even when
    run-state would read "never run / empty". The decision keys off target
    facts, never ``last_status``.
  - **Unreadable ≠ empty**: any un-inspectable target -> confirm.
"""
from __future__ import annotations

import sys
from pathlib import Path

_SKILL_DIR = Path(__file__).resolve().parents[2] / "skills" / "aidp-fusion-seed"
sys.path.insert(0, str(_SKILL_DIR))

import guard  # noqa: E402


def _t(name, *, exists, rows, readable):
    return {
        "target_table": name,
        "target_exists": exists,
        "target_row_count": rows,
        "readable": readable,
    }


def test_status_json_unsupported_always_confirms():
    """Today's CLI (no --json): emptiness cannot be proven -> confirm."""
    # Even if a caller passed 'empty-looking' targets, lack of support wins.
    targets = [_t("silver.dim_supplier", exists=False, rows=0, readable=True)]
    d = guard.classify_guard(targets, status_json_supported=False)
    assert d["decision"] == guard.CONFIRM


def test_pyspark_missing_unreadable_confirms():
    """PySpark unavailable -> every target readable:false -> confirm."""
    targets = [
        _t("silver.dim_supplier", exists=None, rows=None, readable=False),
        _t("gold.supplier_spend", exists=None, rows=None, readable=False),
    ]
    d = guard.classify_guard(targets, status_json_supported=True)
    assert d["decision"] == guard.CONFIRM
    assert "silver.dim_supplier" in d["unprovable_tables"]


def test_partial_unreadable_confirms():
    """One readable+empty, one unreadable -> still confirm (never infer empty from a subset)."""
    targets = [
        _t("silver.dim_supplier", exists=False, rows=0, readable=True),
        _t("gold.supplier_spend", exists=True, rows=None, readable=False),
    ]
    d = guard.classify_guard(targets, status_json_supported=True)
    assert d["decision"] == guard.CONFIRM
    assert d["unprovable_tables"] == ["gold.supplier_spend"]


def test_populated_target_confirms():
    targets = [_t("silver.dim_supplier", exists=True, rows=4213, readable=True)]
    d = guard.classify_guard(targets, status_json_supported=True)
    assert d["decision"] == guard.CONFIRM
    assert d["populated_tables"] == ["silver.dim_supplier"]


def test_stale_state_but_populated_target_confirms():
    """REGRESSION: run-state is irrelevant — a populated physical target confirms.

    The fact dicts model the table itself; there is no `last_status` field that
    could flip the decision. A populated target -> confirm even though state
    rows might say the dataset 'never ran'. This is the trap that would let a
    naive state-read `CREATE OR REPLACE` a populated mart with no prompt.
    """
    targets = [
        _t("gold.supplier_spend", exists=True, rows=999, readable=True),
        _t("silver.dim_supplier", exists=False, rows=0, readable=True),
    ]
    d = guard.classify_guard(targets, status_json_supported=True)
    assert d["decision"] == guard.CONFIRM
    assert d["populated_tables"] == ["gold.supplier_spend"]


def test_exists_unknown_count_confirms():
    """Table exists but row count unknown -> treat as populated -> confirm."""
    targets = [_t("silver.dim_account", exists=True, rows=None, readable=True)]
    d = guard.classify_guard(targets, status_json_supported=True)
    assert d["decision"] == guard.CONFIRM


def test_no_targets_confirms():
    d = guard.classify_guard([], status_json_supported=True)
    assert d["decision"] == guard.CONFIRM


def test_all_targets_empty_and_readable_proceeds():
    """The ONLY proceed case: every resolved target inspectable AND empty/absent."""
    targets = [
        _t("bronze.erp_suppliers", exists=False, rows=None, readable=True),
        _t("silver.dim_supplier", exists=True, rows=0, readable=True),
        _t("gold.supplier_spend", exists=False, rows=0, readable=True),
    ]
    d = guard.classify_guard(targets, status_json_supported=True)
    assert d["decision"] == guard.PROCEED
    assert d["populated_tables"] == []
    assert d["unprovable_tables"] == []


def test_only_one_state_proceeds_across_matrix():
    """Exactly one of the matrix states proceeds; all others confirm."""
    matrix = [
        # (targets, status_json_supported)
        ([_t("t", exists=False, rows=0, readable=True)], False),         # unsupported
        ([_t("t", exists=None, rows=None, readable=False)], True),        # unreadable
        ([_t("t", exists=True, rows=5, readable=True)], True),            # populated
        ([], True),                                                       # no targets
        ([_t("t", exists=False, rows=0, readable=True)], True),           # empty+readable
    ]
    decisions = [
        guard.classify_guard(t, status_json_supported=s)["decision"]
        for t, s in matrix
    ]
    assert decisions.count(guard.PROCEED) == 1
    assert decisions[-1] == guard.PROCEED
