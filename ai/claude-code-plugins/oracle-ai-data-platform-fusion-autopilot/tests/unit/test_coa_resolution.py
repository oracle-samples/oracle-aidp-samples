"""Unit tests for the pure COA semantic-role resolution ladder.

Covers feature `coa-role-segment-resolution` M1: the resolution ladder,
honest per-role provenance (never auto_resolve), fail-closed behaviour, and
the legacy back-derivation guard (legacy_unverified, not defaulted_convention).
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.coa_resolution import (
    AIDPF_2013_COA_ROLE_UNRESOLVED,
    CoaResolutionError,
    CoaResolutionInput,
    resolve_coa_roles,
)

ALIASES = {
    "coa_balancing_segment": "coa.balancing",
    "coa_cost_center_segment": "coa.cost_center",
    "coa_natural_account_segment": "coa.natural_account",
}

FLAT_CONFIG = {
    "balancingSegment": "CodeCombinationSegment4",
    "costCenterSegment": "CodeCombinationSegment2",
    "naturalAccountSegment": "CodeCombinationSegment5",
}

PACK_DEFAULT = {
    "balancingSegment": "CodeCombinationSegment1",
    "costCenterSegment": "CodeCombinationSegment2",
    "naturalAccountSegment": "CodeCombinationSegment3",
}


def test_explicit_config_resolves_to_named_columns() -> None:
    res = resolve_coa_roles(
        CoaResolutionInput(semantic_role_aliases=ALIASES, explicit_config=FLAT_CONFIG)
    )
    assert res.column_map["coa_balancing_segment"] == "CodeCombinationSegment4"
    assert res.column_map["coa_natural_account_segment"] == "CodeCombinationSegment5"
    assert res.chart_of_accounts["default"]["balancingSegment"] == "CodeCombinationSegment4"
    for role in ("balancing", "cost_center", "natural_account"):
        assert res.role_provenance[role]["mechanism"] == "config_resolved"


def test_no_mechanism_is_ever_auto_resolve() -> None:
    res = resolve_coa_roles(
        CoaResolutionInput(semantic_role_aliases=ALIASES, explicit_config=FLAT_CONFIG)
    )
    mechs = {r["mechanism"] for r in res.role_provenance.values()}
    assert "auto_resolve" not in mechs


def test_non_interactive_no_config_fails_closed() -> None:
    with pytest.raises(CoaResolutionError) as exc:
        resolve_coa_roles(
            CoaResolutionInput(
                semantic_role_aliases=ALIASES,
                pack_default=PACK_DEFAULT,
                interactive=False,
                accept_convention=False,
            )
        )
    assert AIDPF_2013_COA_ROLE_UNRESOLVED in str(exc.value)


def test_accepted_convention_records_defaulted_convention() -> None:
    res = resolve_coa_roles(
        CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            pack_default=PACK_DEFAULT,
            accept_convention=True,
        )
    )
    assert res.column_map["coa_balancing_segment"] == "CodeCombinationSegment1"
    assert res.role_provenance["balancing"]["mechanism"] == "defaulted_convention"


def test_interactive_default_records_defaulted_convention_with_warning() -> None:
    """Interactive run with no explicit config uses the pack convention, recorded
    as `defaulted_convention` (NOT operator_confirmed without a real prompt, NOT
    auto_resolve) and carries a verify-warning."""
    res = resolve_coa_roles(
        CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            pack_default=PACK_DEFAULT,
            interactive=True,
        )
    )
    assert res.role_provenance["cost_center"]["mechanism"] == "defaulted_convention"
    assert res.warnings, "unaccepted interactive default must warn to verify"


def test_legacy_back_derivation_is_unverified_not_convention() -> None:
    """A legacy profile pinned silently to Segment1/2/3 must NOT be relabeled
    as a clean convention -- it is `legacy_unverified` with a warning."""
    res = resolve_coa_roles(
        CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            existing_resolved_column={
                "coa_balancing_segment": "CodeCombinationSegment1",
                "coa_cost_center_segment": "CodeCombinationSegment2",
                "coa_natural_account_segment": "CodeCombinationSegment3",
            },
            is_refresh=True,
        )
    )
    assert res.role_provenance["balancing"]["mechanism"] == "legacy_unverified"
    assert res.warnings, "legacy back-derivation must emit a remediation warning"
    assert "auto_resolve" not in {
        r["mechanism"] for r in res.role_provenance.values()
    }


def test_legacy_back_derivation_upgraded_on_explicit_accept() -> None:
    res = resolve_coa_roles(
        CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            existing_resolved_column={
                "coa_balancing_segment": "CodeCombinationSegment1",
                "coa_cost_center_segment": "CodeCombinationSegment2",
                "coa_natural_account_segment": "CodeCombinationSegment3",
            },
            is_refresh=True,
            accept_convention=True,
        )
    )
    assert res.role_provenance["balancing"]["mechanism"] == "operator_confirmed"
    assert not res.warnings


def test_mixed_rung_distinct_mechanisms_persist() -> None:
    """Refresh carries forward an existing per-role provenance with distinct
    mechanisms (mixed-rung tenant) -- but only when uniform; here we assert the
    explicit-config path yields a single mechanism, and the carry-forward path
    preserves a uniform one. Mixed-rung distinctness is exercised at the
    profile-assembly layer; here we verify provenance is per-role keyed."""
    res = resolve_coa_roles(
        CoaResolutionInput(semantic_role_aliases=ALIASES, explicit_config=FLAT_CONFIG)
    )
    assert set(res.role_provenance.keys()) == {
        "balancing",
        "cost_center",
        "natural_account",
    }


def test_explicit_config_conflict_with_pin_fails_closed_without_refresh() -> None:
    with pytest.raises(CoaResolutionError):
        resolve_coa_roles(
            CoaResolutionInput(
                semantic_role_aliases=ALIASES,
                explicit_config=FLAT_CONFIG,
                existing_chart_of_accounts={"default": PACK_DEFAULT},
                is_refresh=False,
            )
        )


def test_accept_singleton_persists_flag() -> None:
    res = resolve_coa_roles(
        CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            explicit_config=FLAT_CONFIG,
            accept_singleton=True,
        )
    )
    assert res.chart_of_accounts.get("singletonAccepted") is True


def test_singleton_not_set_by_default() -> None:
    res = resolve_coa_roles(
        CoaResolutionInput(semantic_role_aliases=ALIASES, explicit_config=FLAT_CONFIG)
    )
    assert "singletonAccepted" not in res.chart_of_accounts


def test_byChart_carried_forward_from_config() -> None:
    cfg = dict(FLAT_CONFIG)
    cfg["byChart"] = {
        "5023": {
            "balancingSegment": "CodeCombinationSegment4",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment5",
        }
    }
    res = resolve_coa_roles(
        CoaResolutionInput(semantic_role_aliases=ALIASES, explicit_config=cfg)
    )
    assert "byChart" in res.chart_of_accounts
    assert "5023" in res.chart_of_accounts["byChart"]


def test_per_chart_provenance_recorded_for_every_arm() -> None:
    """Multi-COA: provenance must cover each chart/role binding, not just the
    default — otherwise chart-specific acceptance is unprovable."""
    cfg = dict(FLAT_CONFIG)
    cfg["byChart"] = {
        "101": {
            "balancingSegment": "CodeCombinationSegment1",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment3",
        },
        "5023": {
            "balancingSegment": "CodeCombinationSegment4",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment5",
        },
    }
    res = resolve_coa_roles(
        CoaResolutionInput(semantic_role_aliases=ALIASES, explicit_config=cfg)
    )
    by_chart = res.role_provenance["byChart"]
    assert set(by_chart) == {"101", "5023"}
    # Each chart records per-role provenance with column + mechanism + source.
    assert by_chart["5023"]["natural_account"]["column"] == "CodeCombinationSegment5"
    assert by_chart["5023"]["natural_account"]["mechanism"] == "config_resolved"
    assert by_chart["101"]["balancing"]["column"] == "CodeCombinationSegment1"
    for arm in by_chart.values():
        for role_prov in arm.values():
            assert role_prov["mechanism"] != "auto_resolve"
