"""T-SQL constructs Spark accepts and reads differently, or rejects outright.

SQ54: `SELECT alias = expr` is a column alias in T-SQL and a comparison in
Spark. MEASURED on the AIDP cluster (Spark 3.5.0, ANSI off, 2026-09-30):
`SELECT total = qty * price AS v FROM (SELECT 6 AS total, 2 AS qty,
3 AS price)` returned `true`, where T-SQL means a column `total` holding 6.
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql, kind="view", item="W"):
    return tsql.translate(sql, kind=kind, item=item)


def _rules(result):
    return [f.rule for f in result.findings]


class AliasAssignmentTests(unittest.TestCase):
    """SQ54. Before this rule every positive case below came back with the
    `alias = expr` text unchanged and no finding."""

    def assertRewrites(self, sql, expected, count=1):
        result = _t(sql)
        self.assertEqual(result.translated_sql, expected)
        self.assertEqual(_rules(result).count("SQ54_ALIAS_ASSIGNMENT"), count)

    def assertUntouched(self, sql):
        result = _t(sql)
        self.assertNotIn("SQ54_ALIAS_ASSIGNMENT", _rules(result))
        return result

    def test_the_measured_statement(self):
        self.assertRewrites("SELECT total = qty * price FROM dbo.t",
                            "SELECT qty * price AS total FROM default.W.t")

    def test_a_bracketed_alias_comes_out_backticked(self):
        self.assertRewrites(
            "SELECT [Full Name] = first + ' ' + last, id FROM dbo.t",
            "SELECT concat(first, ' ', last) AS `Full Name`, id "
            "FROM default.W.t")

    def test_a_case_expression_keeps_its_own_comparisons(self):
        self.assertRewrites(
            "SELECT x = CASE WHEN a = b THEN 1 ELSE 0 END FROM dbo.t",
            "SELECT CASE WHEN a = b THEN 1 ELSE 0 END AS x FROM default.W.t")

    def test_distinct_and_top_prefixes(self):
        self.assertRewrites(
            "SELECT DISTINCT TOP 5 x = a + 1 FROM dbo.t",
            "SELECT DISTINCT a + 1 AS x FROM default.W.t LIMIT 5")
        result = _t("SELECT TOP (5) PERCENT x = a FROM dbo.t")
        self.assertIn("a AS x", result.translated_sql)

    def test_cte_and_union_branches_each_rewrite(self):
        self.assertRewrites(
            "WITH c AS (SELECT x = a FROM dbo.t) "
            "SELECT y = x FROM c UNION ALL SELECT z = 1",
            "WITH c AS (SELECT a AS x FROM default.W.t) "
            "SELECT x AS y FROM c UNION ALL SELECT 1 AS z", count=3)

    def test_a_subquery_item_and_its_inner_list_both_rewrite(self):
        self.assertRewrites(
            "SELECT n = (SELECT m = max(a) FROM dbo.u WHERE u.k = t.k) "
            "FROM dbo.t",
            "SELECT (SELECT max(a) AS m FROM default.W.u WHERE u.k = t.k) "
            "AS n FROM default.W.t", count=2)

    def test_every_item_of_a_list_is_read(self):
        self.assertRewrites("SELECT a = 1, b, c = d + 1 FROM dbo.t",
                            "SELECT 1 AS a, b, d + 1 AS c FROM default.W.t",
                            count=2)

    def test_a_string_literal_alias(self):
        self.assertRewrites("SELECT 'Grand Total' = SUM(x) FROM dbo.t",
                            "SELECT SUM(x) AS `Grand Total` FROM default.W.t")

    def test_the_alias_goes_before_a_trailing_comment(self):
        result = _t("SELECT x = a -- note\nFROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT a AS x -- note\nFROM default.W.t")

    def test_where_on_having_are_not_touched(self):
        self.assertUntouched(
            "SELECT a FROM dbo.t JOIN dbo.u ON t.k = u.k WHERE a = 1 "
            "GROUP BY a HAVING COUNT(*) = 2")

    def test_update_set_is_not_touched(self):
        result = self.assertUntouched("UPDATE dbo.t SET a = b")
        self.assertIn("SET a = b", result.translated_sql)

    def test_variable_assignment_is_not_touched(self):
        result = self.assertUntouched("SELECT @v = a, @w = b FROM dbo.t")
        self.assertIn("@v = a", result.translated_sql)

    def test_iif_and_function_arguments_are_not_touched(self):
        result = self.assertUntouched(
            "SELECT IIF(a = b, 1, 0) AS f, COALESCE(NULLIF(a, 0), 1) FROM dbo.t")
        self.assertIn("if(a = b, 1, 0)", result.translated_sql)

    def test_a_subquery_comparison_is_not_touched(self):
        self.assertUntouched(
            "SELECT a FROM dbo.t WHERE a = (SELECT max(b) FROM dbo.u)")

    def test_a_qualified_column_comparison_is_not_an_alias(self):
        self.assertUntouched("SELECT a FROM dbo.t WHERE t.a = 1")

    def test_an_existing_as_alias_is_not_touched(self):
        self.assertUntouched("SELECT qty * price AS total FROM dbo.t")


class TableHintTests(unittest.TestCase):
    """SQ55 / SQ80_HINT. MEASURED on the AIDP cluster (Spark 3.5.0,
    2026-09-30): `MERGE INTO <t> WITH (HOLDLOCK) AS tgt USING ...` ->
    PARSE_SYNTAX_ERROR "missing 'USING'"; the hint also hid the MERGE from
    SQ80_MERGE and left its USING source unqualified."""

    MERGE = ("MERGE INTO dbo.t WITH (HOLDLOCK) AS tgt USING dbo.s AS src "
             "ON tgt.id = src.id WHEN MATCHED THEN DELETE;")

    def test_a_hinted_merge_loses_the_hint_and_keeps_its_flag(self):
        out = _t(self.MERGE)
        self.assertNotIn("WITH (", out.translated_sql)
        self.assertIn("MERGE INTO default.W.t AS tgt USING default.W.s AS src", out.translated_sql)
        self.assertIn("SQ80_MERGE", _rules(out))
        self.assertIn("SQ55_TABLE_HINT", _rules(out))

    def test_a_merge_without_into_is_fixed_too(self):
        out = _t(self.MERGE.replace("MERGE INTO", "MERGE").replace("HOLDLOCK", "SERIALIZABLE"))
        self.assertIn("MERGE INTO default.W.t AS tgt USING default.W.s", out.translated_sql)

    def test_read_changing_hints_are_removed_and_flagged(self):
        for sql in ("SELECT a FROM dbo.t WITH (NOLOCK)", "SELECT a FROM dbo.t (NOLOCK)",
                    "SELECT a FROM dbo.t WITH (READPAST, UPDLOCK)",
                    "SELECT a FROM dbo.t WITH (READUNCOMMITTED)"):
            with self.subTest(sql=sql):
                out = _t(sql)
                self.assertEqual(out.translated_sql, "SELECT a FROM default.W.t")
                self.assertIn(("SQ80_HINT", "flag"), [(f.rule, f.severity) for f in out.findings])

    def test_locking_and_plan_hints_are_a_rewrite(self):
        for sql, want in (("INSERT INTO dbo.t WITH (TABLOCK) SELECT * FROM dbo.s",
                           "INSERT INTO default.W.t SELECT * FROM default.W.s"),
                          ("UPDATE dbo.t WITH (ROWLOCK) SET a = 1", "UPDATE default.W.t SET a = 1"),
                          ("SELECT a FROM dbo.t WITH (INDEX(ix_a))", "SELECT a FROM default.W.t")):
            with self.subTest(sql=sql):
                out = _t(sql)
                self.assertEqual(out.translated_sql, want)
                self.assertNotIn("SQ80_HINT", _rules(out))

    def test_lookalikes_are_left_alone(self):
        for sql in ("WITH c AS (SELECT 1 AS x) SELECT x FROM c",
                    "SELECT f(nolock) FROM dbo.t", "SELECT 'WITH (NOLOCK)' AS s FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertNotIn("SQ55_TABLE_HINT", _rules(_t(sql)))


if __name__ == "__main__":
    unittest.main()
