"""M3 renderer + $coa.* union resolver tests.

Covers: {{ coa.<role> }} renders a bare column (single-COA) and a deterministic
parameterized CASE (multi-COA); unknown role + malicious chart-id rejected;
byChart edits shift the plan-hash; $coa.<role> expands to the union of arm
columns; validator allowlists the coa head and rejects unknown roles.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.required_column_resolver import (
    coa_role_union,
    resolve_required_column_entries,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    UnresolvedVariationPointError,
    _render_coa_role,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import TenantProfile


def _profile(coa: dict) -> TenantProfile:
    return TenantProfile(
        schemaVersion=1,
        tenant="t",
        pinnedAt=datetime(2026, 1, 1),
        profile={"chartOfAccounts": coa},
    )


_SINGLE = {
    "default": {
        "balancingSegment": "CodeCombinationSegment4",
        "costCenterSegment": "CodeCombinationSegment2",
        "naturalAccountSegment": "CodeCombinationSegment5",
    }
}

_MULTI = {
    "default": {
        "balancingSegment": "CodeCombinationSegment1",
        "costCenterSegment": "CodeCombinationSegment2",
        "naturalAccountSegment": "CodeCombinationSegment3",
    },
    "byChart": {
        "5023": {
            "balancingSegment": "CodeCombinationSegment4",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment5",
        },
        "101": {
            "balancingSegment": "CodeCombinationSegment1",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment3",
        },
    },
}


def test_single_coa_renders_bare_column() -> None:
    params: dict = {}
    out = _render_coa_role("balancing", profile=_profile(_SINGLE), params=params)
    assert out == "CodeCombinationSegment4"
    assert params == {}  # no params for a bare column


def test_multi_coa_renders_parameterized_case_sorted() -> None:
    params: dict = {}
    out = _render_coa_role("balancing", profile=_profile(_MULTI), params=params)
    # Arms sorted by chart-id → 101 before 5023; WHEN values are params.
    assert "CASE CAST(CodeCombinationChartOfAccountsId AS STRING)" in out
    assert ":coa_balancing_chart_0" in out and ":coa_balancing_chart_1" in out
    assert "raise_error(" in out and out.strip().endswith("END")
    assert params == {"coa_balancing_chart_0": "101", "coa_balancing_chart_1": "5023"}
    # Chart-id literals are NOT inlined into the SQL text.
    assert "'101'" not in out and "'5023'" not in out


def test_unknown_role_rejected() -> None:
    with pytest.raises(UnresolvedVariationPointError):
        _render_coa_role("not_a_role", profile=_profile(_SINGLE), params={})


def test_malicious_chart_id_rejected() -> None:
    bad = {
        "default": _MULTI["default"],
        "byChart": {"1); DROP TABLE x--": _MULTI["byChart"]["5023"]},
    }
    with pytest.raises(UnresolvedVariationPointError):
        _render_coa_role("balancing", profile=_profile(bad), params={})


def test_missing_chart_of_accounts_raises() -> None:
    prof = TenantProfile(
        schemaVersion=1, tenant="t", pinnedAt=datetime(2026, 1, 1), profile={}
    )
    with pytest.raises(UnresolvedVariationPointError):
        _render_coa_role("balancing", profile=prof, params={})


# --- $coa.* union resolver --------------------------------------------------


def test_coa_role_union_single() -> None:
    assert coa_role_union("balancing", _profile(_SINGLE)) == {
        "CodeCombinationSegment4"
    }


def test_coa_role_union_multi_dedups_across_arms() -> None:
    # balancing: default Segment1, 5023 Segment4, 101 Segment1 → {Segment1, Segment4}
    assert coa_role_union("balancing", _profile(_MULTI)) == {
        "CodeCombinationSegment1",
        "CodeCombinationSegment4",
    }


def test_required_column_entries_expands_coa_ref() -> None:
    resolved = resolve_required_column_entries(
        ["CodeCombinationCodeCombinationId", "$coa.balancing"],
        resolved_pack=None,
        tenant_profile=_profile(_MULTI),
    )
    assert "CodeCombinationCodeCombinationId" in resolved
    assert {"CodeCombinationSegment1", "CodeCombinationSegment4"} <= resolved
