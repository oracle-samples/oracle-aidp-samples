"""Nested-call handling, found by running the rules over 952 real T-SQL objects
from microsoft/sql-server-samples.

Two real objects crashed with "overlapping replacements". The guard was right
to refuse -- the alternative was mangled SQL -- but the object failed entirely.
A nested match cannot be rewritten in the same pass as its parent, because the
parent's replacement is built from the original argument text; dropping the
inner match and re-running picks it up.
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql, kind="view"):
    return tsql.translate(sql, kind=kind)


class NestedScalarTests(unittest.TestCase):
    def test_getdate_inside_isnull_does_not_crash(self):
        out = _t("SELECT ISNULL(a, GETDATE()) FROM t").translated_sql
        self.assertEqual(out, "SELECT coalesce(a, current_timestamp()) FROM t")

    def test_both_rewrites_are_reported(self):
        rules = {f.rule for f in _t("SELECT ISNULL(a, GETDATE()) FROM t").findings}
        self.assertIn("SQ30_ISNULL", rules)
        self.assertIn("SQ30_GETDATE", rules)

    def test_the_real_shape_that_crashed(self):
        sql = ("SELECT datediff(ISNULL(po.ActualDeliveryDate, GETDATE()), "
               "po.OrderDate) AS d FROM po")
        out = _t(sql).translated_sql
        self.assertIn("coalesce(po.ActualDeliveryDate, current_timestamp())", out)

    def test_triple_nesting(self):
        out = _t("SELECT ISNULL(ISNULL(a, GETDATE()), SYSDATETIME())").translated_sql
        self.assertEqual(
            out,
            "SELECT coalesce(coalesce(a, current_timestamp()), current_timestamp())")

    def test_nested_convert(self):
        # This asserted `CAST(x AS VARCHAR)` while SQ81 passed the T-SQL type
        # name straight through. SQ81 now maps it, and maps it the same way
        # SQ60 maps a bare `CAST(x AS varchar(20))` -- to STRING -- so the two
        # paths agree. The nesting, which is what this test is about, is
        # unchanged.
        out = _t("SELECT CONVERT(INT, CONVERT(VARCHAR, x))").translated_sql
        self.assertEqual(out, "SELECT CAST(CAST(x AS STRING) AS INT)")


class NestedConcatTests(unittest.TestCase):
    def test_concat_run_inside_a_parenthesised_expression(self):
        sql = "SELECT 'a' + (CASE WHEN x = 1 THEN 'b' + y ELSE 'c' END) + 'd'"
        out = _t(sql).translated_sql
        self.assertIn("concat(", out)
        self.assertNotIn(" + ", out.replace("concat(", ""))

    def test_inner_run_is_also_converted(self):
        sql = "SELECT 'a' + (CASE WHEN x = 1 THEN 'b' + y ELSE 'c' END)"
        out = _t(sql).translated_sql
        self.assertEqual(out.count("concat("), 2)

    def test_deeply_nested_replace_chain_does_not_crash(self):
        sql = ("SELECT '{' + (CASE t WHEN 'P' THEN '[' + "
               "REPLACE(REPLACE(g, 'A', 'B'), 'C', 'D') + ']' ELSE NULL END) + '}'")
        result = _t(sql)
        self.assertIn("concat(", result.translated_sql)


class NestedCharindexTests(unittest.TestCase):
    """SQ21 and SQ20 collected every match in one pass and applied them
    together, so a nested call of the same kind overlapped its parent. The
    guard in apply_replacements then refused, and because that refusal is a
    bare ValueError the user saw an internal message about offsets:

        SELECT SUBSTRING(s, CHARINDEX('-', s)+1, CHARINDEX('-', SUBSTRING(s,
        CHARINDEX('-', s)+1, 99))) FROM dbo.t
          -> ValueError: overlapping replacements: 69-86 overlaps a range
             ending at 94

    runner.py catches it, so the asset degraded to an error row rather than
    killing the run -- but the row said nothing about the user's SQL.
    """

    def test_the_real_shape_that_crashed(self):
        sql = ("SELECT SUBSTRING(s, CHARINDEX('-', s)+1, CHARINDEX('-', "
               "SUBSTRING(s, CHARINDEX('-', s)+1, 99))) FROM dbo.t")
        out = tsql.translate(sql, kind="warehouse_ddl", item="W").translated_sql
        self.assertEqual(
            out,
            "SELECT SUBSTRING(s, locate('-', s)+1, locate('-', "
            "SUBSTRING(s, locate('-', s)+1, 99))) FROM default.W.t")
        self.assertNotIn("CHARINDEX", out)

    def test_every_nested_occurrence_is_reported(self):
        sql = ("SELECT SUBSTRING(s, CHARINDEX('-', s)+1, CHARINDEX('-', "
               "SUBSTRING(s, CHARINDEX('-', s)+1, 99))) FROM dbo.t")
        findings = tsql.translate(sql, kind="warehouse_ddl", item="W").findings
        self.assertEqual(sum(1 for f in findings if f.rule == "SQ21_CHARINDEX"), 3)

    def test_charindex_directly_inside_charindex(self):
        out = _t("SELECT CHARINDEX('-', CHARINDEX('-', s))").translated_sql
        self.assertEqual(out, "SELECT locate('-', locate('-', s))")

    def test_datediff_directly_inside_datediff(self):
        # The sibling rule collected matches the same way and failed the same
        # way: "overlapping replacements: 24-43 overlaps a range ending at 44".
        out = _t("SELECT DATEDIFF(day, a, DATEDIFF(day, b, c))").translated_sql
        self.assertEqual(out, "SELECT datediff(datediff(c, b), a)")

    def test_a_flagged_unit_inside_a_rewritten_one_still_reports(self):
        result = _t("SELECT DATEDIFF(day, a, DATEDIFF(month, b, c))")
        rules = [f.rule for f in result.findings]
        self.assertIn("SQ20_DATEDIFF", rules)
        self.assertIn("SQ20_DATEDIFF_UNIT", rules)


class PassBudgetTests(unittest.TestCase):
    def test_nesting_deeper_than_the_budget_is_flagged_not_silent(self):
        # _to_fixed_point rewrites one level per pass and stops after eight.
        # It used to stop quietly, which left T-SQL in the output with no
        # finding against it -- unrunnable SQL graded PASS. Measured: ten
        # nested ISNULLs leave two behind.
        deep = "SELECT " + "ISNULL(a, " * 10 + "GETDATE()" + ")" * 10
        result = _t(deep)
        self.assertIn("ISNULL", result.translated_sql)
        self.assertIn("SQ03_NESTING_TOO_DEEP", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)

    def test_nesting_within_the_budget_raises_no_such_flag(self):
        result = _t("SELECT ISNULL(a, ISNULL(b, GETDATE()))")
        self.assertNotIn("SQ03_NESTING_TOO_DEEP", {f.rule for f in result.findings})
        self.assertNotIn("ISNULL", result.translated_sql)


class CorpusRegressionTests(unittest.TestCase):
    def test_no_rule_ever_raises_on_pathological_nesting(self):
        cases = [
            "SELECT ISNULL(a, ISNULL(b, GETDATE()))",
            "SELECT 'a' + 'b' + (SELECT 'c' + 'd')",
            "SELECT CONVERT(INT, ISNULL(a, 0))",
            "SELECT IIF(x, ISNULL(a, GETDATE()), SYSDATETIME())",
            "SELECT SPACE(LEN(a))",
            "SELECT DATEADD(day, 1, ISNULL(d, GETDATE()))",
            "SELECT CHARINDEX('-', CHARINDEX('-', CHARINDEX('-', s)))",
            "SELECT DATEDIFF(day, DATEDIFF(day, a, b), DATEDIFF(day, c, d))",
            "SELECT CHARINDEX('-', SUBSTRING(s, CHARINDEX('-', s), 5))",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.assertIsInstance(_t(sql).translated_sql, str)


if __name__ == "__main__":
    unittest.main()
