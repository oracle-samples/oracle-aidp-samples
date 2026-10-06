"""Regression tests for: `NOT IN`, `REPLACESTR`/`REPLACECHR`
argument indices and semantics, and the unmapped-function fallback.

These assert on generated CODE CONTENT, never merely that something was
produced -- a hollow "# TODO ..." placeholder must fail these tests.
"""
from __future__ import annotations

import ast
import unittest

from infa2aidp.converters.expression_converter import (
    ExpressionConverter,
    UnconvertibleExpression,
)
from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.models import (
    DataFlowDirection,
    Transformation,
    TransformationField,
    TransformationType,
)


class TestNotIn(unittest.TestCase):
    """`X NOT IN (...)` used to raise AttributeError ('_Tokenizer' object
    has no attribute '_pos') because the parser referenced t._pos/t._tokens
    instead of the tokenizer's real attributes t.pos/t.tokens -- and
    convert()'s except clause only caught (IndexError, ValueError), so the
    AttributeError escaped uncaught."""

    def setUp(self) -> None:
        self.conv = ExpressionConverter()

    def test_not_in_converts_without_raising(self) -> None:
        result = self.conv.convert("STATUS NOT IN ('A','B')")
        self.assertIn("isin", result)
        self.assertIn("~", result)

    def test_not_in_references_the_right_column_and_values(self) -> None:
        result = self.conv.convert("STATUS NOT IN ('A','B')")
        self.assertIn("F.col('STATUS')", result)
        self.assertIn("'A'", result)
        self.assertIn("'B'", result)

    def test_plain_in_still_works(self) -> None:
        result = self.conv.convert("STATUS IN ('COMPLETED','SHIPPED')")
        self.assertIn(".isin(", result)
        self.assertNotIn("~", result)


class TestReplaceStr(unittest.TestCase):
    """REPLACESTR(CaseFlag, Input, Old, New) -- the previous port used
    args[0]/args[1]/args[2] (CaseFlag as the input column, Old as the
    replacement, New dropped entirely)."""

    def setUp(self) -> None:
        self.conv = ExpressionConverter()

    def test_replacestr_uses_correct_argument_positions(self) -> None:
        result = self.conv.convert("REPLACESTR(0, NAME, 'old', 'new')")
        # NAME (args[1]) is the input column being operated on.
        self.assertIn("F.col('NAME')", result)
        # 'new' (args[3]) is the replacement -- must be present at all.
        self.assertIn("new", result)
        # The CaseFlag (0) must NOT appear as if it were the input column.
        self.assertNotIn("regexp_replace(0", result)

    def test_replacestr_escapes_regex_metacharacters_in_old(self) -> None:
        """regexp_replace's pattern arg is a regex -- a literal '.' in Old
        must not become "match any character"."""
        result = self.conv.convert("REPLACESTR(0, FILENAME, '.', '_')")
        self.assertIn(r"\.", result)  # escaped, not a bare '.'

    def test_replacestr_case_flag_follows_the_reference(self) -> None:
        """CaseFlag 0 or NULL is case-INsensitive, non-zero case-sensitive
        (Transformation Language Reference). This was inverted: 0 compiled
        to a case-sensitive replace and 1 was refused."""
        self.assertEqual(self.conv.convert("REPLACESTR(1, NAME, 'old', 'new')"),
                         "F.regexp_replace(F.col('NAME'), 'old', 'new')")
        self.assertEqual(self.conv.convert("REPLACESTR(0, NAME, 'old', 'new')"),
                         "F.regexp_replace(F.col('NAME'), '(?i)old', 'new')")
        with self.assertRaises(UnconvertibleExpression):
            self.conv.convert("REPLACESTR(FLAG, NAME, 'old', 'new')")


class TestReplaceChr(unittest.TestCase):
    """REPLACECHR(CaseFlag, Input, OldCharSet, NewChar) is CHARACTER-SET
    semantics -- every character in OldCharSet gets replaced -- not a
    substring/regex match. F.translate is the correct PySpark mapping;
    F.regexp_replace('aeiou', ...) would only match the literal 5-letter
    substring "aeiou"."""

    def setUp(self) -> None:
        self.conv = ExpressionConverter()

    def test_replacechr_uses_translate_not_regexp_replace(self) -> None:
        result = self.conv.convert("REPLACECHR(0, NAME, 'aeiou', '*')")
        self.assertIn("F.translate(", result)
        self.assertNotIn("regexp_replace", result)

    def test_replacechr_replaces_all_five_vowels(self) -> None:
        """Exercise the emitted code for real: F.translate must be given
        a charset covering all five vowels, so each is replaced
        independently (not just the literal substring "aeiou")."""
        # CaseFlag 1: case-sensitive, so exactly the five lower-case vowels.
        result = self.conv.convert("REPLACECHR(1, NAME, 'aeiou', '*')")
        self.assertIn("'aeiou'", result)
        # A plausible pyspark.sql.functions stub, just enough to actually
        # execute the generated translate() call and check its behavior.
        calls = []

        class _FakeCol:
            def __init__(self, name):
                self.name = name

        class _F:
            @staticmethod
            def col(name):
                return _FakeCol(name)

            @staticmethod
            def translate(col, matching, replace):
                calls.append((matching, replace))
                return (col, matching, replace)

        ns = {"F": _F(), "NAME": _FakeCol("NAME")}
        col, matching, replace = eval(result, ns)  # noqa: S307 -- test-only, fixed input
        self.assertEqual(matching, "aeiou")
        self.assertEqual(replace, "*****")  # one '*' per vowel, all 5


class TestUnmappedFunctionFallback(unittest.TestCase):
    """The unmapped-function fallback used to return
    '# TODO: unmapped function X\\nX(...)' -- a '#' plus embedded newline
    that, once interpolated into a withColumn(...) call, comments out the
    rest of the line (and the second physical line becomes a bare,
    dangling statement). This must now raise a typed exception instead,
    and the caller (TransformationConverter) must turn that into a review
    item plus a *valid* placeholder -- never broken Python."""

    def setUp(self) -> None:
        self.conv = ExpressionConverter()
        self.tx_converter = TransformationConverter()

    def test_unmapped_function_raises_instead_of_returning_broken_code(self) -> None:
        with self.assertRaises(UnconvertibleExpression):
            self.conv.convert("SOME_UNKNOWN_FUNC(A, B)")

    def test_unmapped_function_result_never_contains_hash_or_newline(self) -> None:
        """Defense in depth: even if some future change makes convert()
        return instead of raise, the value must never contain a bare '#'
        or a newline -- both corrupt an enclosing expression."""
        try:
            result = self.conv.convert("SOME_UNKNOWN_FUNC(A, B)")
        except UnconvertibleExpression:
            return  # raising is the expected/preferred outcome
        self.assertNotIn("#", result)
        self.assertNotIn("\n", result)

    def test_transformation_converter_emits_valid_code_for_unmapped_function(self) -> None:
        """End to end: an Expression transformation with an output field
        that calls an unmapped function must still produce syntactically
        valid, non-hollow PySpark -- a review item as its own comment
        line, and a safe placeholder, never a spliced comment."""
        tx = Transformation(
            name="EXP_WEIRD",
            type=TransformationType.EXPRESSION,
            fields=[
                TransformationField(
                    name="A", datatype="string", direction=DataFlowDirection.INPUT,
                ),
                TransformationField(
                    name="B", datatype="string", direction=DataFlowDirection.INPUT,
                ),
                TransformationField(
                    name="RESULT", datatype="string",
                    direction=DataFlowDirection.OUTPUT,
                    expression="SOME_UNKNOWN_FUNC(A, B)",
                ),
            ],
        )
        lines = self.tx_converter.convert(tx, input_df="df")
        code = "\n".join(lines)

        self.assertIn("REVIEW REQUIRED", code)
        self.assertIn('withColumn("RESULT"', code)
        self.assertIn("F.lit(None)", code)
        # The whole generated cell must still be valid Python.
        ast.parse(code)


if __name__ == "__main__":
    unittest.main()
