"""Tests for Expression-transformation variable ports.

Informatica evaluates a PORTTYPE="VARIABLE" port row-by-row, in the order
it's declared, and later ports (including OUTPUT ports) may reference it.
Before this fix, `_resolve_direction` had no VARIABLE case, so every
variable port fell through to INPUT and transformation_converter.py's
OUTPUT/INPUT_OUTPUT allowlist silently dropped its expression entirely --
running totals, prior-row comparisons, change detection, all gone with no
error and no comment.

These tests assert on the generated CODE CONTENT (the actual withColumn
calls and their relative order), not merely that some code was produced --
a hollow "# Expression: ..." comment with nothing else must fail these
tests.
"""
from __future__ import annotations

import unittest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.models import (
    DataFlowDirection,
    Transformation,
    TransformationField,
    TransformationType,
)


class TestVariablePortEmitted(unittest.TestCase):
    """A variable port's expression must be emitted, and emitted before
    any later port that references it."""

    def setUp(self) -> None:
        self.converter = TransformationConverter()

    def _tx(self) -> Transformation:
        return Transformation(
            name="EXP_RUNNING",
            type=TransformationType.EXPRESSION,
            fields=[
                TransformationField(
                    name="AMOUNT", datatype="number",
                    direction=DataFlowDirection.INPUT,
                ),
                # Declared BEFORE the output port, and does NOT reference
                # itself -- a plain intermediate calculation.
                TransformationField(
                    name="v_double", datatype="number",
                    direction=DataFlowDirection.VARIABLE,
                    expression="AMOUNT * 2",
                ),
                # References the variable port declared above it.
                TransformationField(
                    name="FINAL_AMOUNT", datatype="number",
                    direction=DataFlowDirection.OUTPUT,
                    expression="v_double + 1",
                ),
            ],
        )

    def test_variable_port_expression_is_emitted(self) -> None:
        lines = self.converter.convert(self._tx(), input_df="df")
        code = "\n".join(lines)
        self.assertIn('withColumn("v_double"', code)
        self.assertIn("F.col('AMOUNT') * 2", code)

    def test_variable_port_emitted_before_the_port_that_references_it(self) -> None:
        lines = self.converter.convert(self._tx(), input_df="df")
        var_idx = next(
            i for i, l in enumerate(lines) if 'withColumn("v_double"' in l
        )
        final_idx = next(
            i for i, l in enumerate(lines) if 'withColumn("FINAL_AMOUNT"' in l
        )
        self.assertLess(
            var_idx, final_idx,
            "v_double must be computed before FINAL_AMOUNT, which reads it",
        )
        # And FINAL_AMOUNT's expression must actually reference the
        # variable port's column, not silently drop it either.
        final_line = lines[final_idx]
        self.assertIn("v_double", final_line)


class TestSelfReferencingVariablePort(unittest.TestCase):
    """A variable port that references itself (v_run = v_run + AMOUNT) is
    row-ordered running state -- it needs a window function, which is
    separate (M2) work. This must become an explicit review item, never a
    silent (and wrong) row-independent guess."""

    def setUp(self) -> None:
        self.converter = TransformationConverter()

    def _tx(self) -> Transformation:
        return Transformation(
            name="EXP_RUNNING_TOTAL",
            type=TransformationType.EXPRESSION,
            fields=[
                TransformationField(
                    name="AMOUNT", datatype="number",
                    direction=DataFlowDirection.INPUT,
                ),
                TransformationField(
                    name="v_run", datatype="number",
                    direction=DataFlowDirection.VARIABLE,
                    expression="v_run + AMOUNT",
                ),
                TransformationField(
                    name="RUNNING_TOTAL", datatype="number",
                    direction=DataFlowDirection.OUTPUT,
                    expression="v_run",
                ),
            ],
        )

    def test_self_reference_becomes_a_running_window(self) -> None:
        """v_run = v_run + AMOUNT is a running total: a cumulative sum over
        the row order, starting from the numeric initial value 0."""
        lines = self.converter.convert(self._tx(), input_df="df")
        code = "\n".join(lines)
        self.assertIn("Stateful variable ports (v_run)", code)
        self.assertIn("F.lit(0) + F.sum(F.col('AMOUNT')).over(_wr)", code)

    def test_an_unsupported_self_reference_is_still_a_review_item(self) -> None:
        tx = self._tx()
        for f in tx.fields:
            if f.name == "v_run":
                f.expression = "v_run * AMOUNT"
        code = "\n".join(self.converter.convert(tx, input_df="df"))
        self.assertIn("REVIEW REQUIRED", code)
        self.assertIn("window function", code.lower())

    def test_self_reference_is_not_silently_translated(self) -> None:
        """It must NOT emit a naive row-independent withColumn that just
        adds v_run + AMOUNT as if it were a stateless expression -- that
        would silently produce a wrong answer (every row would get
        AMOUNT + NULL instead of a running total)."""
        lines = self.converter.convert(self._tx(), input_df="df")
        code = "\n".join(lines)
        v_run_lines = [l for l in code.splitlines() if 'withColumn("v_run"' in l]
        self.assertTrue(v_run_lines, "expected a placeholder withColumn for v_run")
        for line in v_run_lines:
            self.assertIn("F.lit(None)", line)
            self.assertNotIn("F.col('v_run')", line)

    def test_generated_code_is_still_syntactically_valid(self) -> None:
        """The review-item path must still be runnable Python (modulo the
        reviewer's own follow-up) -- no comment spliced mid-statement.
        ast.parse doesn't execute anything; it just proves no "#" landed
        mid-line and broke a statement."""
        import ast
        lines = self.converter.convert(self._tx(), input_df="df")
        ast.parse("\n".join(lines))


if __name__ == "__main__":
    unittest.main()
