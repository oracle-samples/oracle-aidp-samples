"""Unit tests for the additive-vs-mutating COA classifier + projections.

Feature: incremental-coa-chart-onboarding. Locks in the correctness points the
plan reviews hammered on:
  * Round-1 Finding 1 — default-only→byChart where an existing protected chart
    moves to a different column → mutating.
  * Round-2 Finding 2 — renderer-equivalent resolution: a removed arm equal to
    the default becomes UNMAPPED → mutating (not "unchanged").
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_change import (
    UNMAPPED,
    classify_coa_change,
    resolve_rendered,
)


def _proj(default=None, by_chart=None, singleton=False) -> dict:
    return {
        "default": default or {},
        "byChart": by_chart or {},
        "singletonAccepted": singleton,
    }


# Shorthand role maps.
_S3 = {"coa.natural_account": "SEGMENT3"}
_S4 = {"coa.natural_account": "SEGMENT4"}


class TestResolveRendered:
    def test_no_bychart_returns_default(self) -> None:
        assert resolve_rendered(_proj(default=_S3), "101") == _S3

    def test_bychart_hit_returns_arm(self) -> None:
        assert resolve_rendered(_proj(default=_S3, by_chart={"101": _S4}), "101") == _S4

    def test_bychart_miss_is_unmapped(self) -> None:
        # byChart present but this chart has no arm → renderer hits raise_error.
        assert resolve_rendered(_proj(default=_S3, by_chart={"101": _S4}), "999") is UNMAPPED

    def test_unmapped_never_equals_a_mapping(self) -> None:
        assert UNMAPPED != _S3
        assert UNMAPPED != {}


class TestClassifyAdditive:
    def test_new_chart_added_is_additive(self) -> None:
        prior = _proj(default=_S3, by_chart={"101": _S3})
        incoming = _proj(default=_S3, by_chart={"101": _S3, "202": _S4})
        assert classify_coa_change(prior, incoming, ["101"]) == "additive"

    def test_new_chart_widening_union_is_additive(self) -> None:
        # New chart uses a column no existing arm used — still additive; the
        # protected chart 101 is unchanged.
        prior = _proj(by_chart={"101": {"coa.natural_account": "SEGMENT3"}})
        incoming = _proj(by_chart={
            "101": {"coa.natural_account": "SEGMENT3"},
            "202": {"coa.natural_account": "SEGMENT5"},
        })
        assert classify_coa_change(prior, incoming, ["101"]) == "additive"

    def test_new_chart_not_yet_materialised_is_additive(self) -> None:
        # 202 is not in protected_charts (no dim rows yet) → maps freely.
        prior = _proj(default=_S3, by_chart={"101": _S3})
        incoming = _proj(default=_S3, by_chart={"101": _S3, "202": _S4})
        assert classify_coa_change(prior, incoming, ["101"]) == "additive"

    def test_identical_projection(self) -> None:
        p = _proj(default=_S3, by_chart={"101": _S3})
        assert classify_coa_change(p, dict(p), ["101"]) == "identical"


class TestClassifyMutating:
    def test_default_only_to_bychart_existing_chart_moves_is_mutating(self) -> None:
        # Round-1 Finding 1: prior is bare-default (every active chart == SEGMENT3);
        # incoming adds an arm giving the EXISTING protected chart 101 a different
        # column. Looks "new" (prior byChart empty) but is a reclassification.
        prior = _proj(default=_S3)  # no byChart → 101 resolves to SEGMENT3
        incoming = _proj(default=_S3, by_chart={"101": _S4, "202": _S3})
        assert classify_coa_change(prior, incoming, ["101"]) == "mutating"

    def test_removed_arm_equal_to_default_is_mutating(self) -> None:
        # Round-2 Finding 2: 101's arm equals the default column, then is removed
        # while 202 keeps multi-COA rendering active. Under renderer-equivalent
        # resolution 101 becomes UNMAPPED (raise_error), NOT the default.
        prior = _proj(default=_S3, by_chart={"101": _S3, "202": _S4})
        incoming = _proj(default=_S3, by_chart={"202": _S4})
        assert classify_coa_change(prior, incoming, ["101"]) == "mutating"

    def test_existing_arm_column_move_is_mutating(self) -> None:
        prior = _proj(by_chart={"101": _S3, "202": _S4})
        incoming = _proj(by_chart={"101": _S4, "202": _S4})
        assert classify_coa_change(prior, incoming, ["101"]) == "mutating"

    def test_default_changed_is_mutating_when_default_only(self) -> None:
        prior = _proj(default=_S3)
        incoming = _proj(default=_S4)
        assert classify_coa_change(prior, incoming, ["101"]) == "mutating"

    def test_disguised_mutation_new_arm_plus_existing_change(self) -> None:
        # New arm 303 AND existing protected 101 changed → mutating.
        prior = _proj(by_chart={"101": _S3})
        incoming = _proj(by_chart={"101": _S4, "303": _S3})
        assert classify_coa_change(prior, incoming, ["101"]) == "mutating"
