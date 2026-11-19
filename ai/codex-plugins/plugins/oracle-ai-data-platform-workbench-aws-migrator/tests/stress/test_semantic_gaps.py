"""Constructs that produce wrong Spark output rather than no output.

These are the cases a parse check can never catch: the SQL is syntactically
fine, it runs, and it returns something different from what Athena returned.
Every expectation below was measured on Spark 3.5.9 and is recorded beside
the case.

Two are rewrites because the corrected form was verified to execute:

    date_format literal escaping   'yyyy-MM-dd''T''…'  -> Unknown pattern letter: T
                                   'yyyy-MM-dd\\'T\\'…'  -> 2024-01-03T10:20:30

The rest are flags: Spark accepts them and silently means something else, and
no rewrite is provable without knowing column types or the target's NULL
policy.
"""
from __future__ import annotations

import unittest

from aws_aidp.translate.athena_to_spark_sql import translate


def flags(sql: str) -> set[str]:
    return {f.rule for f in translate(sql).findings if f.severity == "flag"}


def out(sql: str) -> str:
    return translate(sql).translated_sql


class DateFormatLiteralEscapingTests(unittest.TestCase):
    """Spark reads '' inside a pattern as two adjacent literals."""

    def test_literal_letters_use_backslash_escaping(self):
        # Spark 3.5.9: the '' form raises "Unknown pattern letter: T".
        result = out("SELECT date_format(ts, '%Y-%m-%dT%H:%i:%s') FROM t")
        self.assertIn(r"\'T\'", result)
        self.assertNotIn("''T''", result)

    def test_multi_letter_literal_run_is_escaped(self):
        result = out("SELECT date_format(ts, 'Day %d of %M') FROM t")
        self.assertNotIn("''", result)
        self.assertIn(r"\'", result)

    def test_format_without_literals_is_unchanged_in_shape(self):
        result = out("SELECT date_format(ts, '%Y-%m-%d') FROM t")
        self.assertIn("'yyyy-MM-dd'", result)
        self.assertNotIn("\\", result)


class TrinoStringLiteralTests(unittest.TestCase):
    """Athena escapes a quote by doubling it; Spark concatenates instead."""

    def test_doubled_quote_literal_is_flagged(self):
        # Spark 3.5.9: 'it''s' -> 'its'; the apostrophe is silently dropped.
        self.assertIn("string_literal_escaping", flags("SELECT 'it''s' FROM t"))

    def test_realistic_surname_case_is_flagged(self):
        self.assertIn("string_literal_escaping",
                      flags("SELECT * FROM t WHERE surname = 'O''Brien'"))

    def test_backslash_literal_is_flagged(self):
        # Athena keeps the backslash; Spark turns \n into a newline.
        self.assertIn("string_literal_escaping",
                      flags(r"SELECT 'C:\temp\new' FROM t"))

    def test_plain_literal_is_not_flagged(self):
        self.assertNotIn("string_literal_escaping",
                         flags("SELECT 'plain text', 'a-b_c' FROM t"))

    def test_empty_string_is_not_flagged(self):
        self.assertNotIn("string_literal_escaping", flags("SELECT '' FROM t"))


class ArrayConstructorTests(unittest.TestCase):
    """Spark has no ARRAY[...] constructor and no array(...)/map(...) types."""

    def test_array_bracket_constructor_is_flagged(self):
        for sql in ("SELECT ARRAY[1,2,3] FROM t",
                    "SELECT ARRAY['a','b'][1] FROM t",
                    "SELECT approx_percentile(x, ARRAY[0.5,1.0]) FROM t",
                    "SELECT width_bucket(7, ARRAY[1,5,10]) FROM t",
                    "SELECT MAP(ARRAY['a'], ARRAY[1]) FROM t"):
            with self.subTest(sql=sql):
                self.assertIn("array_constructor", flags(sql))

    def test_presto_collection_type_syntax_is_flagged(self):
        for sql in ("SELECT CAST(NULL AS ARRAY(VARCHAR)) FROM t",
                    "CREATE TABLE t (b array(varchar), c map(varchar,bigint))"):
            with self.subTest(sql=sql):
                self.assertIn("array_constructor", flags(sql))

    def test_spark_array_function_is_not_flagged(self):
        self.assertNotIn("array_constructor", flags("SELECT array(1,2,3) FROM t"))

    def test_ordinary_subscript_is_not_flagged(self):
        self.assertNotIn("array_constructor", flags("SELECT m['k'] FROM t"))


class IntegerDivisionTests(unittest.TestCase):
    """Presto truncates integer division; Spark returns a double."""

    def test_integer_literal_division_is_flagged(self):
        # Spark 3.5.9: 7/2 -> 3.5. Presto: 3.
        self.assertIn("integer_division", flags("SELECT 7/2 FROM t"))

    def test_ratio_of_counts_is_flagged(self):
        # count() is provably integer on both engines; this is routine analytics.
        for sql in ("SELECT count(x)/count(*) FROM t",
                    "SELECT COUNT(DISTINCT a) / COUNT(*) FROM t"):
            with self.subTest(sql=sql):
                self.assertIn("integer_division", flags(sql))

    def test_decimal_literal_division_is_not_flagged(self):
        for sql in ("SELECT 7.0/2 FROM t", "SELECT 7/2.0 FROM t"):
            with self.subTest(sql=sql):
                self.assertNotIn("integer_division", flags(sql))

    def test_bare_column_division_is_not_flagged(self):
        # Operand types are unknown without a catalog; flagging every "/"
        # would bury the review queue.
        self.assertNotIn("integer_division", flags("SELECT a/b FROM t"))

    def test_division_inside_a_literal_is_not_flagged(self):
        self.assertNotIn("integer_division", flags("SELECT '7/2' AS s FROM t"))


class ExtractDayOfWeekTests(unittest.TestCase):
    """Presto counts weeks from Monday, Spark from Sunday."""

    def test_extract_dow_is_flagged(self):
        # Spark 3.5.9 returns 4 where Athena returns 3.
        for sql in ("SELECT EXTRACT(DOW FROM d) FROM t",
                    "SELECT EXTRACT(DAY_OF_WEEK FROM d) FROM t",
                    "SELECT extract(dow from order_date) FROM t"):
            with self.subTest(sql=sql):
                self.assertIn("day_of_week", flags(sql))

    def test_other_extract_fields_are_not_flagged(self):
        for sql in ("SELECT EXTRACT(YEAR FROM d) FROM t",
                    "SELECT EXTRACT(MONTH FROM d) FROM t",
                    "SELECT EXTRACT(DAY FROM d) FROM t"):
            with self.subTest(sql=sql):
                self.assertNotIn("day_of_week", flags(sql))


class GreatestLeastNullTests(unittest.TestCase):
    """Athena returns NULL if any argument is NULL; Spark skips NULLs."""

    def test_greatest_and_least_are_flagged(self):
        # Spark 3.5.9: greatest(1, NULL) -> 1. Athena: NULL.
        for sql in ("SELECT greatest(1, NULL) FROM t",
                    "SELECT least(a, b) FROM t",
                    "SELECT GREATEST(a, b, c) FROM t"):
            with self.subTest(sql=sql):
                self.assertIn("greatest_least_null_semantics", flags(sql))

    def test_similar_names_are_not_flagged(self):
        self.assertNotIn("greatest_least_null_semantics",
                         flags("SELECT greatest_hits FROM t"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
