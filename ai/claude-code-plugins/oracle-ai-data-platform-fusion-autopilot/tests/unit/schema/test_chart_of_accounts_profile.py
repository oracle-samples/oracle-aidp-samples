"""Unit tests for ``ChartOfAccountsProfile`` — the COA shape contract reused by
the structural COA gate (feature: fail-fast-seed-validation).

Covers both accepted shapes (flat/legacy + nested ``default``), their mutual
exclusivity, the ``byChart``-requires-a-default rule, and the ``singletonAccepted``
``StrictBool`` tightening (a hand-edited ``"false"`` string must NOT coerce to
``True`` and bypass the multi-COA gate).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    ChartOfAccountsProfile,
)

_FLAT = {
    "balancingSegment": "CodeCombinationSegment1",
    "costCenterSegment": "CodeCombinationSegment2",
    "naturalAccountSegment": "CodeCombinationSegment3",
}
_NESTED = {"default": dict(_FLAT)}


class TestAcceptedShapes:
    def test_flat_legacy_shape_accepted(self) -> None:
        coa = ChartOfAccountsProfile.model_validate(_FLAT)
        rd = coa.resolved_default()
        assert rd is not None
        assert rd.balancing_segment == "CodeCombinationSegment1"

    def test_nested_default_shape_accepted(self) -> None:
        coa = ChartOfAccountsProfile.model_validate(_NESTED)
        rd = coa.resolved_default()
        assert rd is not None
        assert rd.natural_account_segment == "CodeCombinationSegment3"

    def test_flat_plus_nested_default_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ChartOfAccountsProfile.model_validate({**_FLAT, **_NESTED})

    def test_incomplete_flat_arm_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ChartOfAccountsProfile.model_validate(
                {"balancingSegment": "CodeCombinationSegment1"}
            )

    def test_bychart_without_default_rejected(self) -> None:
        """``byChart`` with no effective default cannot render fallback rows."""
        with pytest.raises(ValidationError):
            ChartOfAccountsProfile.model_validate(
                {"byChart": {"101": dict(_FLAT)}}
            )

    def test_bychart_with_default_accepted(self) -> None:
        coa = ChartOfAccountsProfile.model_validate(
            {**_NESTED, "byChart": {"101": dict(_FLAT)}}
        )
        assert set(coa.arms()) == {"default", "101"}


class TestSingletonAcceptedStrictBool:
    def test_default_false(self) -> None:
        assert ChartOfAccountsProfile.model_validate(_FLAT).singleton_accepted is False

    @pytest.mark.parametrize("value", [True, False])
    def test_native_bool_accepted(self, value: bool) -> None:
        coa = ChartOfAccountsProfile.model_validate(
            {**_FLAT, "singletonAccepted": value}
        )
        assert coa.singleton_accepted is value

    @pytest.mark.parametrize("value", ["false", "true", 1, 0, "yes", "on"])
    def test_string_and_int_rejected(self, value: object) -> None:
        """The historical ``bool("false") is True`` bug: a non-bool must be
        rejected, never coerced to ``True`` (which silently bypassed AIDPF-2018)."""
        with pytest.raises(ValidationError):
            ChartOfAccountsProfile.model_validate(
                {**_FLAT, "singletonAccepted": value}
            )
