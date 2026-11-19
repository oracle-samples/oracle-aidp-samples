"""Unit tests for the pure COA plausibility + multi-COA gate (M2).

Covers: Tier A union-existence + per-arm distinctness (cross-chart reuse OK,
same-chart dup fails), Tier B per-chart natural-account contradiction with
sample-floor guard, and the multi-COA fail-closed gate weighing active rows.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_gate import (
    AIDPF_2016_COA_DUP_ROLE_COLUMN,
    AIDPF_2017_COA_NATURAL_ACCOUNT_CONTRADICTION,
    AIDPF_2018_MULTI_COA_UNCONFIGURED,
    AIDPF_2042_REQUIRED_COLUMN_MISSING,
    ChartProbe,
    check_distinctness,
    check_existence_union,
    check_multi_coa,
    check_natural_account,
)


def _codes(errs):
    return {c for c, _ in errs}


# --- Tier A: existence (union) ----------------------------------------------


def test_existence_union_missing_column_fails() -> None:
    errs = check_existence_union(
        {"CodeCombinationSegment4"}, {"CodeCombinationSegment1"}
    )
    assert AIDPF_2042_REQUIRED_COLUMN_MISSING in _codes(errs)


def test_existence_union_present_passes() -> None:
    errs = check_existence_union(
        {"CodeCombinationSegment1"},
        {"CodeCombinationSegment1", "CodeCombinationSegment2"},
    )
    assert errs == []


# --- Tier A: per-arm distinctness -------------------------------------------


def test_cross_chart_column_reuse_is_valid() -> None:
    """Segment1 = balancing in chart 100 AND cost_center in chart 200 -> valid."""
    arms = {
        "100": {
            "coa.balancing": "CodeCombinationSegment1",
            "coa.cost_center": "CodeCombinationSegment2",
            "coa.natural_account": "CodeCombinationSegment3",
        },
        "200": {
            "coa.balancing": "CodeCombinationSegment4",
            "coa.cost_center": "CodeCombinationSegment1",
            "coa.natural_account": "CodeCombinationSegment5",
        },
    }
    assert check_distinctness(arms) == []


def test_same_chart_duplicate_role_columns_fail() -> None:
    arms = {
        "default": {
            "coa.balancing": "CodeCombinationSegment1",
            "coa.cost_center": "CodeCombinationSegment1",  # dup within one arm
            "coa.natural_account": "CodeCombinationSegment3",
        }
    }
    assert AIDPF_2016_COA_DUP_ROLE_COLUMN in _codes(check_distinctness(arms))


# --- Tier B: natural-account contradiction ----------------------------------


def test_strong_contradiction_above_floor_fails() -> None:
    probe = ChartProbe(
        chart_id="100",
        active_row_count=5000,
        natural_account_distinct=400,
        natural_account_ambiguous=300,  # 75% ambiguous
    )
    res = check_natural_account(probe)
    assert AIDPF_2017_COA_NATURAL_ACCOUNT_CONTRADICTION in _codes(res.errors)


def test_contradiction_below_sample_floor_only_warns() -> None:
    probe = ChartProbe(
        chart_id="100",
        active_row_count=10,  # below floor
        natural_account_distinct=4,
        natural_account_ambiguous=3,
    )
    res = check_natural_account(probe)
    assert res.errors == []
    assert res.warnings


def test_clean_natural_account_passes() -> None:
    probe = ChartProbe(
        chart_id="100",
        active_row_count=5000,
        natural_account_distinct=400,
        natural_account_ambiguous=0,
    )
    res = check_natural_account(probe)
    assert res.ok and not res.warnings


# --- Multi-COA gate ---------------------------------------------------------


def test_multi_coa_without_acceptance_fails() -> None:
    errs = check_multi_coa(
        {"100": 15000, "200": 8000}, singleton_accepted=False, has_by_chart=False
    )
    assert AIDPF_2018_MULTI_COA_UNCONFIGURED in _codes(errs)


def test_multi_coa_with_singleton_accepted_passes() -> None:
    errs = check_multi_coa(
        {"100": 15000, "200": 8000}, singleton_accepted=True, has_by_chart=False
    )
    assert errs == []


def test_multi_coa_with_by_chart_passes() -> None:
    errs = check_multi_coa(
        {"100": 15000, "200": 8000}, singleton_accepted=False, has_by_chart=True
    )
    assert errs == []


def test_single_active_chart_with_inactive_legacy_passes() -> None:
    """A stray legacy/inactive chart (0 active rows) must not trip the gate."""
    errs = check_multi_coa(
        {"100": 15000, "999": 0}, singleton_accepted=False, has_by_chart=False
    )
    assert errs == []
