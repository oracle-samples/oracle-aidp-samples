"""``CoaGateResult.below_sample_floor`` — explicit sample-sufficiency evidence.

The additive field closes the clean-but-undersized gap: a zero-ambiguity probe
below the sample floor produces neither error nor warning, so through the old
result it was indistinguishable from a strongly-verified chart. The verifier
maps such charts to ``VERIFIED_WEAK`` (or ``UNVERIFIED`` on zero evidence)
without ever re-deriving the ``SAMPLE_FLOOR_*`` thresholds.

Pass/fail semantics are asserted UNCHANGED throughout.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_gate import (
    AIDPF_2017_COA_NATURAL_ACCOUNT_CONTRADICTION,
    SAMPLE_FLOOR_DISTINCT,
    SAMPLE_FLOOR_ROWS,
    ChartProbe,
    check_natural_account,
)


def _probe(*, active: int, distinct: int, ambiguous: int = 0) -> ChartProbe:
    return ChartProbe(
        chart_id="138",
        active_row_count=active,
        natural_account_distinct=distinct,
        natural_account_ambiguous=ambiguous,
    )


class TestBelowSampleFloorEvidence:
    def test_zero_ambiguity_below_row_floor(self) -> None:
        res = check_natural_account(
            _probe(active=SAMPLE_FLOOR_ROWS - 1, distinct=SAMPLE_FLOOR_DISTINCT)
        )
        assert res.below_sample_floor is True
        assert res.ok and not res.warnings  # clean — but NOT strong evidence

    def test_zero_ambiguity_below_distinct_floor(self) -> None:
        res = check_natural_account(
            _probe(active=SAMPLE_FLOOR_ROWS, distinct=SAMPLE_FLOOR_DISTINCT - 1)
        )
        assert res.below_sample_floor is True
        assert res.ok and not res.warnings

    def test_zero_ambiguity_above_both_floors(self) -> None:
        res = check_natural_account(
            _probe(active=SAMPLE_FLOOR_ROWS, distinct=SAMPLE_FLOOR_DISTINCT)
        )
        assert res.below_sample_floor is False
        assert res.ok and not res.warnings

    def test_zero_evidence_is_flagged(self) -> None:
        # n == 0 early-returns today; the flag distinguishes "no evidence"
        # from "clean and sufficient".
        res = check_natural_account(_probe(active=0, distinct=0))
        assert res.below_sample_floor is True
        assert res.ok and not res.warnings

    def test_zero_evidence_with_active_rows_is_flagged(self) -> None:
        # Active rows but every natural-account value NULL: still zero
        # observations — the verifier must treat this as UNVERIFIED.
        res = check_natural_account(_probe(active=500, distinct=0))
        assert res.below_sample_floor is True
        assert res.ok


class TestPassFailSemanticsUnchanged:
    def test_strong_contradiction_above_floors_still_hard_fails(self) -> None:
        res = check_natural_account(_probe(active=402, distinct=402, ambiguous=212))
        assert [c for c, _ in res.errors] == [
            AIDPF_2017_COA_NATURAL_ACCOUNT_CONTRADICTION
        ]
        assert res.below_sample_floor is False

    def test_contradiction_below_floor_still_downgrades_to_warning(self) -> None:
        res = check_natural_account(_probe(active=50, distinct=10, ambiguous=6))
        assert res.ok
        assert len(res.warnings) == 1
        assert res.below_sample_floor is True

    def test_ambiguous_above_warn_threshold_still_warns(self) -> None:
        res = check_natural_account(_probe(active=500, distinct=100, ambiguous=25))
        assert res.ok
        assert len(res.warnings) == 1
        assert res.below_sample_floor is False
