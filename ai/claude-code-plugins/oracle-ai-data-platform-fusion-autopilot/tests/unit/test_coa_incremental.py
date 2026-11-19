"""Unit tests for the additive-COA incremental decision core (no Spark).

Feature: incremental-coa-chart-onboarding. Covers the pure decision function
and the CoaIncrementalContext acceptance orchestration with injected fakes.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_change import (
    coa_projection_of,
    projection_to_coa_dict,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_incremental import (
    CoaIncrementalContext,
    decide_coa_accept,
)


_S3 = {"coa.natural_account": "SEGMENT3"}
_S4 = {"coa.natural_account": "SEGMENT4"}


def _proj(default=None, by_chart=None, singleton=False) -> dict:
    return {
        "default": default or {},
        "byChart": by_chart or {},
        "singletonAccepted": singleton,
    }


class TestProjectionInverse:
    def test_round_trip_via_profile(self) -> None:
        proj = _proj(default=_S3, by_chart={"101": _S4}, singleton=True)
        coa_dict = projection_to_coa_dict(proj)
        # Re-project from a profile carrying that chartOfAccounts dict.
        profile = SimpleNamespace(profile={"chartOfAccounts": coa_dict})
        assert coa_projection_of(profile) == proj

    def test_no_bychart_omits_key(self) -> None:
        coa_dict = projection_to_coa_dict(_proj(default=_S3))
        assert "byChart" not in coa_dict
        assert coa_dict["default"] == {"naturalAccountSegment": "SEGMENT3"}


class TestDecideCoaAccept:
    def test_source_accepts_without_checkpoint(self) -> None:
        assert decide_coa_accept(
            verdict="additive", is_coa_source=True, coa_checkpoint_passed=False,
            prior_equivalent_hash="H", stored_prior_hash="H",
        )

    def test_downstream_needs_checkpoint(self) -> None:
        assert not decide_coa_accept(
            verdict="additive", is_coa_source=False, coa_checkpoint_passed=False,
            prior_equivalent_hash="H", stored_prior_hash="H",
        )
        assert decide_coa_accept(
            verdict="additive", is_coa_source=False, coa_checkpoint_passed=True,
            prior_equivalent_hash="H", stored_prior_hash="H",
        )

    def test_hash_mismatch_blocks(self) -> None:
        assert not decide_coa_accept(
            verdict="additive", is_coa_source=True, coa_checkpoint_passed=True,
            prior_equivalent_hash="X", stored_prior_hash="H",
        )

    def test_none_hash_blocks(self) -> None:
        assert not decide_coa_accept(
            verdict="additive", is_coa_source=True, coa_checkpoint_passed=True,
            prior_equivalent_hash=None, stored_prior_hash="H",
        )

    @pytest.mark.parametrize("verdict", ["mutating", "identical", None])
    def test_non_additive_blocks(self, verdict) -> None:
        assert not decide_coa_accept(
            verdict=verdict, is_coa_source=True, coa_checkpoint_passed=True,
            prior_equivalent_hash="H", stored_prior_hash="H",
        )


def _ctx(active=True, protected=("101",), checkpoint=True, baseline=None, incoming=None):
    prior = baseline if baseline is not None else _proj(default=_S3, by_chart={"101": _S3})
    manifest = {"coa_projection": prior, "profile_hash": "PRIOR_PH"}
    return CoaIncrementalContext(
        active=active,
        incoming_coa=incoming if incoming is not None
        else _proj(default=_S3, by_chart={"101": _S3, "202": _S4}),
        protected_charts=frozenset(protected) if protected is not None else None,
        coa_source_ids=frozenset({"gl_coa"}),
        coa_checkpoint_passed=checkpoint,
        manifest_by_run_id=lambda rid: manifest if rid == "R1" else None,
    )


class TestCoaAcceptReason:
    def _node(self, node_id="dim_account"):
        return SimpleNamespace(id=node_id)

    def test_additive_consumer_accepts_when_hash_matches(self) -> None:
        ctx = _ctx()
        reason = ctx.coa_accept_reason(
            node=self._node(), prior_run_id="R1", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "H",  # prior-equivalent == stored
        )
        assert reason is not None and "additive" in reason

    def test_blocks_when_recompute_diverges(self) -> None:
        ctx = _ctx()
        assert ctx.coa_accept_reason(
            node=self._node(), prior_run_id="R1", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "OTHER",
        ) is None

    def test_inactive_returns_none(self) -> None:
        ctx = _ctx(active=False)
        assert ctx.coa_accept_reason(
            node=self._node(), prior_run_id="R1", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "H",
        ) is None

    def test_failclosed_when_protected_unreadable(self) -> None:
        ctx = _ctx(protected=None)  # None = fail-closed read
        assert ctx.coa_accept_reason(
            node=self._node(), prior_run_id="R1", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "H",
        ) is None

    def test_no_paired_manifest_blocks(self) -> None:
        ctx = _ctx()
        assert ctx.coa_accept_reason(
            node=self._node(), prior_run_id="UNKNOWN", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "H",
        ) is None

    def test_v1_baseline_without_projection_blocks(self) -> None:
        ctx = _ctx()
        ctx.manifest_by_run_id = lambda rid: {"profile_hash": "PH"}  # no coa_projection
        assert ctx.coa_accept_reason(
            node=self._node(), prior_run_id="R1", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "H",
        ) is None

    def test_mutating_change_blocks(self) -> None:
        # incoming moves existing protected chart 101 to a different column.
        ctx = _ctx(incoming=_proj(default=_S3, by_chart={"101": _S4}))
        assert ctx.coa_accept_reason(
            node=self._node(), prior_run_id="R1", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "H",
        ) is None

    def test_source_node_accepts_without_checkpoint(self) -> None:
        ctx = _ctx(checkpoint=False)
        reason = ctx.coa_accept_reason(
            node=self._node("gl_coa"), prior_run_id="R1", stored_prior_hash="H",
            recompute_hash=lambda coa, ph: "H",
        )
        assert reason is not None
