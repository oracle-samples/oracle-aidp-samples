"""Gaps found by parsing generated SQL with Spark 3.5.0 on a live AIDP cluster.

31 real SQL Server objects were translated and handed to Spark's own parser.
Five were accepted. These tests pin the causes of the rest that were ours --
two of which were rules producing invalid SQL, not merely missing coverage.
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql, kind="view"):
    return tsql.translate(sql, kind=kind).translated_sql


class BracketedTypeTests(unittest.TestCase):
    """SQ10 backticked [int] into `int`, which Spark rejects as a data type."""

    def test_bracketed_type_is_unwrapped_not_backticked(self):
        # `int` is already valid Spark, so the type rules correctly leave it
        # alone; what matters is that it is a bare word, not an identifier.
        out = _t("CREATE TABLE CUSTOMER([CustomerID] [int] NOT NULL)", kind="table")
        self.assertIn("`CustomerID` int", out)
        self.assertNotIn("`int`", out)

    def test_bracketed_type_with_length_still_maps(self):
        out = _t("CREATE TABLE t([a] [nvarchar](50) NOT NULL)", kind="table")
        self.assertIn("`a` STRING", out)
        self.assertNotIn("`nvarchar`", out)

    def test_a_bracketed_column_name_is_still_backticked(self):
        out = _t("CREATE TABLE t([claim id] INT)", kind="table")
        self.assertIn("`claim id`", out)

    def test_bracketed_decimal_keeps_precision(self):
        out = _t("CREATE TABLE t([a] [decimal](7,2))", kind="table")
        self.assertIn("decimal(7,2)", out.lower())
        self.assertNotIn("`decimal`", out)

    def test_a_type_that_does_need_mapping_is_uppercased(self):
        out = _t("CREATE TABLE t([a] [numeric](7,2))", kind="table")
        self.assertIn("DECIMAL(7,2)", out)


class CaseKeywordTests(unittest.TestCase):
    """The concat scanner consumed the END keyword, destroying the CASE."""

    SRC = ("CREATE VIEW v AS SELECT CASE m WHEN 'a' THEN 'x' "
           "ELSE Left(m,1) END + ' ' + r AS mr FROM t")

    def test_end_keyword_is_never_swallowed(self):
        out = _t(self.SRC)
        self.assertIn("END", out)
        self.assertNotIn("concat(END", out)

    def test_declining_is_reported_not_silent(self):
        result = tsql.translate(self.SRC, kind="view")
        self.assertTrue(any(f.rule == "SQ41_CONCAT_KEYWORD" for f in result.findings))

    def test_a_normal_concat_still_works(self):
        self.assertIn("concat('x', y)", _t("SELECT 'x' + y FROM t"))

    def test_concat_inside_a_then_branch_still_works(self):
        out = _t("SELECT CASE WHEN a=1 THEN 'x' + b ELSE 'z' END FROM t")
        self.assertIn("concat('x', b)", out)
        self.assertIn("END", out)

    def test_other_keywords_are_also_not_terms(self):
        for kw in ("FROM", "WHERE", "THEN", "ELSE", "AND"):
            with self.subTest(kw=kw):
                self.assertNotIn(f"concat({kw}", _t(f"SELECT a {kw} 'x' + b FROM t"))


class CreateOrAlterTests(unittest.TestCase):
    """7 of 31 rejections. T-SQL's CREATE OR ALTER has a direct Spark form."""

    def test_create_or_alter_view_becomes_or_replace(self):
        out = _t("CREATE OR ALTER VIEW v AS SELECT 1")
        self.assertIn("CREATE OR REPLACE VIEW", out)
        self.assertNotIn("OR ALTER", out)

    def test_plain_create_view_is_untouched(self):
        self.assertIn("CREATE VIEW v", _t("CREATE VIEW v AS SELECT 1"))

    def test_it_is_reported_as_a_rewrite(self):
        r = tsql.translate("CREATE OR ALTER VIEW v AS SELECT 1", kind="view")
        self.assertTrue(any(f.rule == "SQ12_CREATE_OR_ALTER" for f in r.findings))


class FilegroupTests(unittest.TestCase):
    def test_on_primary_is_stripped(self):
        out = _t("CREATE TABLE t(a INT) ON [PRIMARY]", kind="table")
        self.assertNotIn("PRIMARY", out)
        self.assertIn("CREATE TABLE t", out)

    def test_textimage_on_is_stripped(self):
        out = _t("CREATE TABLE t(a INT) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]",
                 kind="table")
        self.assertNotIn("TEXTIMAGE_ON", out)

    def test_it_is_reported(self):
        r = tsql.translate("CREATE TABLE t(a INT) ON [PRIMARY]", kind="table")
        self.assertTrue(any(f.rule == "SQ71_FILEGROUP" for f in r.findings))


class NullabilityTests(unittest.TestCase):
    def test_explicit_null_marker_is_stripped(self):
        out = _t("CREATE TABLE t(mgr INT NULL, x INT)", kind="table")
        self.assertIn("mgr INT", out)
        self.assertNotIn("INT NULL", out)

    def test_not_null_is_preserved(self):
        self.assertIn("NOT NULL", _t("CREATE TABLE t(a INT NOT NULL)", kind="table"))

    def test_is_null_in_a_predicate_is_untouched(self):
        self.assertIn("IS NULL", _t("SELECT a FROM t WHERE a IS NULL"))


class InlineNamedConstraintTests(unittest.TestCase):
    def test_named_default_constraint_is_removed_and_flagged(self):
        src = "CREATE TABLE t(a INT CONSTRAINT DF_a DEFAULT 0, b INT)"
        out = _t(src, kind="table")
        self.assertNotIn("CONSTRAINT", out)
        r = tsql.translate(src, kind="table")
        self.assertTrue(any(f.rule.startswith("SQ70") for f in r.findings))


if __name__ == "__main__":
    unittest.main()
