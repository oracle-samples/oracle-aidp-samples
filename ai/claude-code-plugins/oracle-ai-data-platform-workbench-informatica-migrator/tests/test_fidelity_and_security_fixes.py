"""Five reproducible fidelity/security defects in the deterministic
compiler path. LLM generation remains the default; ``use_llm`` is
untouched by these fixes.

1. ``||`` bound TIGHTER than +/- (between mul/div and unary), so
   ``'x' || A + B`` came out as ``concat('x', A) + B`` instead of
   ``concat('x', A + B)``. Informatica's real precedence is
   unary > * / > + - > || > comparison > NOT > AND > OR.

2. ADD_TO_DATE/DATE_DIFF/TRUNC each carried their own partial unit map and
   silently fell back to day-granularity for anything not in it -- 'W'
   (week), 'Q' (quarter, ~89 days off) and 'J' (Julian day) all silently
   became +1 DAY with no signal. Now one shared table
   (``expression_converter._DATE_UNIT_FAMILY``); an unrecognized unit
   raises ``UnconvertibleExpression`` instead of guessing.

3. ``security.py``'s guards (input-size cap, XXE rejection, and the new
   linear/quote-aware/non-recursive nesting-depth scan) were never called
   from ``xml_parser.py`` or ``version_detector.py`` -- customer XML hit
   ``ET.parse``/``ET.fromstring`` completely unvetted.

4. A malformed XML file logged two errors and returned a MigrationResult
   with zero mappings -- indistinguishable from a legitimately empty
   export. ``InformaticaXMLParser.parse()`` on a single named file now
   raises ``XmlParseError``; a folder/batch of files still skips-and-
   reports per file (``_parse_folder``), matching the policy already used
   by ``migrator.run_migration`` / ``cli._cmd_analyze`` for multi-file
   inputs.

5. A Sorter's per-TRANSFORMFIELD SORTDIRECTION was collapsed into one
   shared ``tx.sort_direction``, overwritten on every iteration -- the
   LAST key's direction silently applied to every key. Direction is now
   stored per key (``{"field", "direction"}`` dicts, the same shape
   ``iics_parser.py`` and the Rank branch already used).
"""
from __future__ import annotations

import os
import tempfile
import unittest

from infa2aidp.converters.expression_converter import (
    ExpressionConverter,
    UnconvertibleExpression,
)
from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.parsers.security import SecurityError
from infa2aidp.parsers.version_detector import detect_version
from infa2aidp.parsers.xml_parser import InformaticaXMLParser, XmlParseError


# ──────────────────────────────────────── || binds loosest ──

class TestConcatPrecedence(unittest.TestCase):
    def setUp(self):
        self.conv = ExpressionConverter()

    # ``||`` is emitted as concat_ws('', ...) -- Informatica's || ignores a
    # NULL operand (same rule as its CONCAT function), Spark's concat()
    # returns NULL when ANY operand is NULL.

    def test_concat_with_trailing_arithmetic_keeps_arithmetic_inside_concat(self):
        result = self.conv.convert("'x' || A + B")
        # The whole "A + B" must be ONE concat operand -- concat('x', A + B) --
        # not concat('x', A) + B (arithmetic escaping the concat).
        self.assertEqual(
            result, "F.concat_ws('', F.lit('x'), F.col('A') + F.col('B'))"
        )

    def test_concat_with_leading_arithmetic_keeps_arithmetic_inside_concat(self):
        result = self.conv.convert("A + B || 'x'")
        self.assertEqual(
            result, "F.concat_ws('', F.col('A') + F.col('B'), F.lit('x'))"
        )

    def test_concat_still_binds_looser_than_comparison_on_both_sides(self):
        # A comparison chain with concat on both sides -- each side's ||
        # chain resolves to one concat() before the comparison operator
        # runs, and the arithmetic precedence *within* an operand is
        # untouched by this fix. The comparison itself is Informatica's
        # three-valued ``==`` (NULL when either side is NULL), not
        # eqNullSafe.
        result = self.conv.convert("A || B = C || D")
        # Each side is NULL only when both of its operands are (CONCAT/||).
        side = ("F.when(F.coalesce((F.col('{0}')).cast('string'), (F.col('{1}')).cast('string'))"
                ".isNull(), F.lit(None).cast('string')).otherwise(F.concat_ws('', F.col('{0}'), F.col('{1}')))")
        self.assertEqual(result, f"({side.format('A', 'B')} == {side.format('C', 'D')})")

    def test_plain_concat_chain_unaffected(self):
        result = self.conv.convert("FIRST_NAME || ' ' || LAST_NAME")
        self.assertEqual(
            result,
            "F.concat_ws('', F.col('FIRST_NAME'), F.lit(' '), F.col('LAST_NAME'))",
        )


# ────────────────────────────── date units must not default ──

class TestDateUnitTable(unittest.TestCase):
    def setUp(self):
        self.conv = ExpressionConverter()

    # ADD_TO_DATE keeps the time of day (Informatica Date/Time is a
    # timestamp), so the result is ``col + make_interval(...)``; F.date_add /
    # F.add_months return a DATE and dropped the time component. The unit
    # lands in the matching make_interval slot: years, months, weeks, days,
    # hours, mins, secs.
    #
    # The expected strings below all gained F.lit() around a literal amount.
    # make_interval takes COLUMN arguments, and a bare "(1)" evaluates to a
    # Python int, so every one of these raised NOT_EXPECTED_TYPE at run time
    # and ADD_TO_DATE did not work for any unit. These tests asserted the
    # emitted text and passed over it -- which is what the executable tests
    # in test_expression_execution.py exist to catch.

    def test_add_to_date_week_is_seven_days_not_one(self):
        result = self.conv.convert("ADD_TO_DATE(D,'W',1)")
        self.assertEqual(
            result,
            "(F.col('D') + F.make_interval(F.lit(0), F.lit(0), F.lit(1), F.lit(0), "
            "F.lit(0), F.lit(0), F.lit(0)))",
        )

    def test_add_to_date_quarter_is_three_months_not_one_day(self):
        result = self.conv.convert("ADD_TO_DATE(D,'Q',1)")
        self.assertEqual(
            result,
            "(F.col('D') + F.make_interval(F.lit(0), (F.lit(1) * F.lit(3)), F.lit(0), "
            "F.lit(0), F.lit(0), F.lit(0), F.lit(0)))",
        )

    def test_add_to_date_julian_day_is_day_granularity(self):
        result = self.conv.convert("ADD_TO_DATE(D,'J',1)")
        self.assertEqual(
            result,
            "(F.col('D') + F.make_interval(F.lit(0), F.lit(0), F.lit(0), F.lit(1), "
            "F.lit(0), F.lit(0), F.lit(0)))",
        )

    def test_add_to_date_hours_with_a_column_amount_parses_and_is_a_column(self):
        """A column amount must reach make_interval unwrapped.

        Renamed from "..._is_valid_python", which it was: ast.parse passed
        on the broken form too, because a bare int argument is perfectly
        valid Python and only fails when Spark type-checks it. Parsing is
        not the property worth asserting here.
        """
        import ast
        result = self.conv.convert("ADD_TO_DATE(TS,'HH',N_HOURS)")
        ast.parse(result)
        self.assertIn("F.make_interval(", result)
        # Already a Column -- must NOT be wrapped in F.lit again.
        self.assertIn("F.col('N_HOURS')", result)
        self.assertNotIn("F.lit(F.col('N_HOURS'))", result)

    def test_add_to_date_unknown_unit_raises(self):
        with self.assertRaises(UnconvertibleExpression):
            self.conv.convert("ADD_TO_DATE(D,'ZZ',1)")

    def test_date_diff_quarter(self):
        """'Q' resolves to the quarter family, not to days.

        The expected string lost its ``.cast('int')`` when DATE_DIFF was
        corrected to return a fractional difference, which is what
        Informatica returns. That cast was incidental to what this test is
        for -- the assertion here is about unit resolution.
        """
        result = self.conv.convert("DATE_DIFF(D1,D2,'Q')")
        self.assertEqual(
            result,
            "(F.months_between(F.col('D1'), F.col('D2')) / 3)",
        )

    def test_date_diff_week(self):
        """'W' resolves to the week family, not to days."""
        result = self.conv.convert("DATE_DIFF(D1,D2,'W')")
        self.assertEqual(
            result,
            "((F.unix_timestamp(F.col('D1')) - F.unix_timestamp(F.col('D2'))) / 604800.0)",
        )

    def test_date_diff_unknown_unit_raises(self):
        with self.assertRaises(UnconvertibleExpression):
            self.conv.convert("DATE_DIFF(D1,D2,'ZZ')")

    def test_trunc_quarter(self):
        result = self.conv.convert("TRUNC(D,'Q')")
        self.assertEqual(result, "F.date_trunc('quarter', F.col('D'))")

    def test_trunc_week(self):
        result = self.conv.convert("TRUNC(D,'W')")
        self.assertEqual(result, "F.date_trunc('week', F.col('D'))")

    def test_trunc_unknown_unit_raises(self):
        with self.assertRaises(UnconvertibleExpression):
            self.conv.convert("TRUNC(D,'ZZ')")

    def test_trunc_with_no_unit_defaults_to_day_without_raising(self):
        # No unit argument at all is TRUNC's own documented default -- NOT
        # the same thing as an explicit-but-unrecognized unit, which must
        # raise instead of guessing.
        result = self.conv.convert("TRUNC(D)")
        self.assertEqual(result, "F.date_trunc('day', F.col('D'))")


# ───────────────────────────── XML security guards wired ──

class TestXmlSecurityGuardsWired(unittest.TestCase):
    def _write(self, content: str) -> str:
        d = tempfile.mkdtemp()
        path = os.path.join(d, "test.xml")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def test_entity_declaration_is_rejected_by_xml_parser(self):
        path = self._write(
            '<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
            "<foo>&xxe;</foo>"
        )
        with self.assertRaises(SecurityError):
            InformaticaXMLParser().parse(path)

    def test_entity_declaration_is_rejected_by_version_detector(self):
        path = self._write(
            '<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
            "<foo>&xxe;</foo>"
        )
        with self.assertRaises(SecurityError):
            detect_version(path)

    def test_legitimate_powermart_doctype_still_parses(self):
        path = self._write(
            '<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">'
            '<POWERMART><REPOSITORY NAME="R"><FOLDER NAME="F"/>'
            "</REPOSITORY></POWERMART>"
        )
        result = InformaticaXMLParser().parse(path)
        self.assertEqual(result.folder_name, "F")

    def test_excessive_nesting_depth_is_rejected(self):
        depth = 300
        path = self._write("<a>" * depth + "</a>" * depth)
        with self.assertRaises(SecurityError):
            InformaticaXMLParser().parse(path)

    def test_oversized_input_is_rejected(self):
        path = self._write("<a>" + ("x" * (11 * 1024 * 1024)) + "</a>")
        with self.assertRaises(SecurityError):
            InformaticaXMLParser().parse(path)


# ──────────────────────── a parse failure must raise ──

class TestMalformedXmlRaises(unittest.TestCase):
    def test_single_malformed_file_raises_not_silently_empty(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "bad.xml")
        with open(path, "w", encoding="utf-8") as f:
            f.write("<not well formed")
        with self.assertRaises(XmlParseError):
            InformaticaXMLParser().parse(path)

    def test_folder_with_one_bad_file_still_returns_the_good_ones(self):
        # Batch behaviour is unchanged: _parse_folder skips-and-reports a
        # bad file rather than raising for the whole directory, matching
        # the existing per-file skip policy in migrator.run_migration /
        # cli._cmd_analyze for multi-file inputs.
        d = tempfile.mkdtemp()
        good = os.path.join(d, "good.xml")
        with open(good, "w", encoding="utf-8") as f:
            f.write(
                '<POWERMART><REPOSITORY NAME="R"><FOLDER NAME="F">'
                '<MAPPING NAME="m1"/></FOLDER></REPOSITORY></POWERMART>'
            )
        bad = os.path.join(d, "bad.xml")
        with open(bad, "w", encoding="utf-8") as f:
            f.write("<not well formed")
        result = InformaticaXMLParser().parse(d)
        self.assertEqual(len(result.mappings), 1)
        self.assertEqual(result.mappings[0].name, "m1")


# ──────────────────────────── per-key sorter direction ──

class TestPerKeySorterDirection(unittest.TestCase):
    XML = """<POWERMART><REPOSITORY NAME="R"><FOLDER NAME="F">
<MAPPING NAME="m1">
<TRANSFORMATION NAME="SRT1" TYPE="Sorter">
  <TRANSFORMFIELD NAME="AMOUNT" PORTTYPE="INPUT/OUTPUT" ISSORTKEY="YES" SORTDIRECTION="DESC"/>
  <TRANSFORMFIELD NAME="TXN_DATE" PORTTYPE="INPUT/OUTPUT" ISSORTKEY="YES" SORTDIRECTION="ASC"/>
</TRANSFORMATION>
</MAPPING>
</FOLDER></REPOSITORY></POWERMART>"""

    @classmethod
    def setUpClass(cls):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sorter.xml")
        with open(path, "w", encoding="utf-8") as f:
            f.write(cls.XML)
        cls.result = InformaticaXMLParser().parse(path)

    def _tx(self):
        return self.result.mappings[0].transformations[0]

    def test_sort_keys_carry_their_own_direction(self):
        keys = self._tx().sort_keys
        self.assertEqual(
            keys,
            [
                {"field": "AMOUNT", "direction": "DESC"},
                {"field": "TXN_DATE", "direction": "ASC"},
            ],
        )

    def test_generated_orderby_honours_each_keys_own_direction(self):
        lines = TransformationConverter().convert(
            self._tx(), input_df="df", output_df="df"
        )
        code = "\n".join(lines)
        # NULLs sort HIGH in Informatica unless "Null Treated Low" = YES, so
        # descending puts them first and ascending puts them last.
        self.assertIn(
            'df = df.orderBy(F.col("AMOUNT").desc_nulls_first(), F.col("TXN_DATE").asc_nulls_last())',
            code,
        )


class TestSorterSortKeyElementShape(unittest.TestCase):
    """Bug #16: some PowerCenter exports mark a Sorter's keys via
    TRANSFORMFIELD ISSORTKEY/SORTDIRECTION attributes (covered above);
    others -- e.g. the shipped time_series_rollup.xml fixture -- instead
    emit dedicated <SORTKEY NAME="..." DIRECTION="..."/> child elements
    alongside the TRANSFORMFIELDs. Reading only the attribute shape left
    this shape resolving to sort_keys=[], a silent no-op: the generated
    notebook line was ``df = df  # TODO: no sort keys found`` and the
    Sorter did nothing. Same defect class as the Router
    EXPRESSION/CONDITION bug -- read one shape where the export writes
    another.
    """

    XML = """<POWERMART><REPOSITORY NAME="R"><FOLDER NAME="F">
<MAPPING NAME="m1">
<TRANSFORMATION NAME="SRT1" TYPE="Sorter">
  <SORTKEY NAME="READING_DATE" DIRECTION="ASC"/>
  <SORTKEY NAME="SENSOR_ID" DIRECTION="DESC"/>
  <TRANSFORMFIELD NAME="SENSOR_ID" PORTTYPE="INPUT/OUTPUT"/>
  <TRANSFORMFIELD NAME="READING_DATE" PORTTYPE="INPUT/OUTPUT"/>
</TRANSFORMATION>
</MAPPING>
</FOLDER></REPOSITORY></POWERMART>"""

    @classmethod
    def setUpClass(cls):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sorter_sortkey_elements.xml")
        with open(path, "w", encoding="utf-8") as f:
            f.write(cls.XML)
        cls.result = InformaticaXMLParser().parse(path)

    def _tx(self):
        return self.result.mappings[0].transformations[0]

    def test_sortkey_elements_resolve_real_keys_not_empty(self):
        keys = self._tx().sort_keys
        self.assertEqual(
            keys,
            [
                {"field": "READING_DATE", "direction": "ASC"},
                {"field": "SENSOR_ID", "direction": "DESC"},
            ],
        )

    def test_generated_orderby_is_not_a_noop(self):
        lines = TransformationConverter().convert(
            self._tx(), input_df="df", output_df="df"
        )
        code = "\n".join(lines)
        self.assertIn(
            'df = df.orderBy(F.col("READING_DATE").asc_nulls_last(), F.col("SENSOR_ID").desc_nulls_first())',
            code,
        )
        self.assertNotIn("no sort keys found", code)


if __name__ == "__main__":
    unittest.main()
