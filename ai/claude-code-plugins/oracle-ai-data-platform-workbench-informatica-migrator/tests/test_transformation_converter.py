"""Tests for TransformationConverter -- specifically the Router path.

A Router group with no condition is Informatica's DEFAULT group: it must
receive rows matching *none* of the other groups, not "all rows" (that was
a real bug -- every group got every row, i.e. silent row duplication into
every downstream target). These tests assert on generated CODE CONTENT
(the actual filter expressions emitted), not just that some code came out
-- a "# TODO: add condition" stub must fail these tests, because that
stub is exactly the bug this file guards against.
"""
from __future__ import annotations

import unittest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.models import Transformation, TransformationType


class TestRouterConversion(unittest.TestCase):
    def setUp(self) -> None:
        self.converter = TransformationConverter()

    def _router_tx(self, groups: list[dict]) -> Transformation:
        return Transformation(
            name="RTR_QUALITY",
            type=TransformationType.ROUTER,
            router_groups=groups,
        )

    def test_three_conditions_plus_default_emit_four_real_filters(self) -> None:
        tx = self._router_tx([
            {"name": "valid", "condition": "IS_VALID_CURRENCY = 1 AND IS_POSITIVE = 1"},
            {"name": "invalid_currency", "condition": "IS_VALID_CURRENCY = 0"},
            {"name": "negative_amount", "condition": "IS_POSITIVE = 0"},
            {"name": "null_amount", "condition": ""},  # DEFAULT group
        ])
        lines = self.converter.convert(tx, input_df="df_source")
        code = "\n".join(lines)

        # No group may fall back to the old "guess" stub -- that stub is
        # the bug (every group silently got every row).
        self.assertNotIn("# TODO: add condition", code)

        # Every conditioned group gets a real .filter(...), not a bare
        # passthrough assignment.
        self.assertIn('df_valid = df_source.filter(', code)
        self.assertIn('df_invalid_currency = df_source.filter(', code)
        self.assertIn('df_negative_amount = df_source.filter(', code)
        self.assertIn("F.col('IS_VALID_CURRENCY')", code)
        self.assertIn("F.col('IS_POSITIVE')", code)

        # The default group must be the negation of the OTHER groups'
        # conditions, joined with the real conditions (not "= df_source"
        # with no filter at all).
        self.assertIn('df_null_amount = df_source.filter(~(', code)
        # It must reference all three conditioned groups' columns, proving
        # it's a real negation and not a copy-paste of one condition.
        default_line = next(l for l in lines if l.startswith("df_null_amount ="))
        self.assertIn("IS_VALID_CURRENCY", default_line)
        self.assertIn("IS_POSITIVE", default_line)

    def test_default_group_is_valid_python_and_mutually_exclusive_in_spirit(self) -> None:
        """The four emitted filter expressions should be syntactically valid
        PySpark-shaped code (checked structurally, not executed -- no Spark
        session in this unit test)."""
        tx = self._router_tx([
            {"name": "high", "condition": "AMOUNT > 1000"},
            {"name": "low", "condition": ""},  # DEFAULT
        ])
        lines = self.converter.convert(tx, input_df="df")
        code = "\n".join(lines)
        self.assertIn("df_high = df.filter(", code)
        self.assertIn("df_low = df.filter(~(", code)
        self.assertNotIn("# TODO: add condition", code)

    def test_ambiguous_default_is_flagged_for_review_not_guessed(self) -> None:
        """More than one group with no condition is malformed Informatica
        input (only one DEFAULT group is legal). We must not silently pick
        one -- flag both for manual review instead."""
        tx = self._router_tx([
            {"name": "group_a", "condition": ""},
            {"name": "group_b", "condition": ""},
        ])
        lines = self.converter.convert(tx, input_df="df")
        code = "\n".join(lines)
        self.assertIn("REVIEW REQUIRED", code)
        self.assertIn("group_a", code)
        self.assertIn("group_b", code)
        # Must not silently fall back to the old "all rows" guess for either.
        self.assertNotIn("# TODO: add condition", code)

    def test_no_conditioned_groups_default_gets_everything(self) -> None:
        """If every group is a DEFAULT-shaped (no-condition) group except
        it's a single group total, there's nothing to negate against --
        the single default group legitimately gets everything."""
        tx = self._router_tx([{"name": "only_group", "condition": ""}])
        lines = self.converter.convert(tx, input_df="df")
        code = "\n".join(lines)
        self.assertIn("df_only_group = df", code)
        self.assertNotIn("# TODO: add condition", code)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# An unconvertible condition must refuse, never filter everything out
# ---------------------------------------------------------------------------

from infa2aidp.converters.transformation_converter import (  # noqa: E402
    _conversion_failed,
    _refuse_lines,
)


def test_conversion_failed_recognises_the_placeholder():
    """The helper existed and was never called, which is how the defect
    survived: _convert_or_flag returns F.lit(None) plus a review when it
    cannot convert, and both call sites emitted the review as a comment and
    used the placeholder anyway."""
    assert _conversion_failed("F.lit(None)", "REVIEW REQUIRED: could not convert `x`")
    # converted, with only a divergence note attached
    assert not _conversion_failed("F.col('A') > 1", "NOTE: rounds differently")
    assert not _conversion_failed("F.lit(None)", "")


def test_a_filter_whose_condition_cannot_be_converted_refuses():
    """.filter(F.lit(None)) is not a NULL condition -- it is FALSE for every
    row, so the notebook ran, wrote nothing and reported success."""
    tx = Transformation(name="FIL_X", type=TransformationType.FILTER)
    tx.filter_condition = ":LKP.SOMETHING(x) > 0"      # not convertible
    out = "\n".join(TransformationConverter().convert(tx, "df_source", "df"))
    assert "filter(F.lit(None))" not in out
    assert "REVIEW REQUIRED" in out
    assert "raise NotImplementedError" in out


def test_a_router_group_whose_condition_cannot_be_converted_refuses():
    tx = Transformation(name="RTR_X", type=TransformationType.ROUTER)
    tx.router_groups = [
        {"name": "ok", "condition": "AMT > 0", "type": "OUTPUT"},
        {"name": "bad", "condition": ":LKP.SOMETHING(x) > 0", "type": "OUTPUT"},
    ]
    out = "\n".join(TransformationConverter().convert(tx, "df_source", "df"))
    assert "df_ok = df_source.filter(" in out      # the good group still converts
    assert "filter(F.lit(None))" not in out
    assert "raise NotImplementedError" in out
    assert "RTR_X" in out


def test_a_convertible_condition_is_unaffected():
    """A guard that fires on good input gets switched off."""
    tx = Transformation(name="FIL_OK", type=TransformationType.FILTER)
    tx.filter_condition = "AMT > 100"
    out = "\n".join(TransformationConverter().convert(tx, "df_source", "df"))
    assert "filter(" in out
    assert "raise NotImplementedError" not in out


def test_refuse_lines_name_what_failed_and_why():
    lines = _refuse_lines("Filter 'FIL_X'", "REVIEW REQUIRED: could not convert `q`")
    text = "\n".join(lines)
    assert "FIL_X" in text
    assert "could not convert" in text
    assert "silently drop every row" in text
