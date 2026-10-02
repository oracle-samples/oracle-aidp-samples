import unittest

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
from fabric_aidp.translate import tsql_to_spark_sql as tsql

NS = "acmens"
HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"


def _nb(code: str) -> str:
    return HEADER + code + "\n"


def _translate(code, **kw):
    return nb2spark.translate(_nb(code), namespace=NS, **kw)


class PathRewriteTests(unittest.TestCase):
    def test_abfss_literal_is_rewritten(self):
        r = _translate('df = spark.read.parquet('
                       '"abfss://W@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Files/x")')
        self.assertIn("oci://W_Sales_Lakehouse@acmens/Files/x", r.translated_sql)
        self.assertEqual(r.flags, 0)
        self.assertGreaterEqual(r.changes, 1)

    def test_fuse_path_uses_the_default_lakehouse(self):
        r = _translate('p = "/lakehouse/default/Files/raw.csv"', default_lakehouse="Sales")
        self.assertIn("oci://Sales@acmens/Files/raw.csv", r.translated_sql)

    def test_unmappable_path_is_flagged_and_left_in_place(self):
        r = _translate('p = "/lakehouse/default/Files/raw.csv"')
        self.assertIn("/lakehouse/default/Files/raw.csv", r.translated_sql)
        self.assertEqual(r.flags, 1)

    def test_fstring_with_interpolation_is_flagged_not_rewritten(self):
        r = _translate('p = f"/lakehouse/default/Files/{name}.csv"',
                       default_lakehouse="Sales")
        self.assertIn("{name}", r.translated_sql)
        self.assertEqual(r.flags, 1)

    def test_fstring_without_interpolation_is_rewritten(self):
        r = _translate('p = f"/lakehouse/default/Files/raw.csv"',
                       default_lakehouse="Sales")
        self.assertIn("oci://Sales@acmens", r.translated_sql)

    def test_non_onelake_string_is_untouched(self):
        r = _translate('p = "just a label"')
        self.assertIn('"just a label"', r.translated_sql)
        self.assertEqual(r.changes, 0)
        self.assertEqual(r.flags, 0)


class NotebookUtilsTests(unittest.TestCase):
    def test_credentials_getsecret_is_flagged(self):
        r = _translate('s = notebookutils.credentials.getSecret("kv", "k")')
        self.assertEqual(r.flags, 1)
        self.assertIn("notebookutils.credentials.getSecret", r.findings[0].detail)

    def test_mssparkutils_alias_is_flagged_too(self):
        self.assertEqual(_translate('mssparkutils.fs.mount("a", "b")').flags, 1)

    def test_flagged_call_is_left_in_place(self):
        r = _translate('notebookutils.notebook.exit("done")')
        self.assertIn('notebookutils.notebook.exit("done")', r.translated_sql)

    def test_each_distinct_surface_is_flagged_once(self):
        r = _translate("notebookutils.fs.mount(a)\nnotebookutils.fs.mount(b)\n"
                       "notebookutils.lakehouse.get(c)")
        self.assertEqual(r.flags, 2)

    def test_import_line_is_commented_out(self):
        r = _translate("import notebookutils")
        self.assertIn("# import notebookutils", r.translated_sql)


class DisplayTests(unittest.TestCase):
    """Fabric's display() is a builtin that renders a Spark DataFrame, a
    pandas DataFrame and more. It used to become `<arg>.show()` -- a method
    only the first of those has, so `display(pdf)` raised AttributeError at
    run time. The call sites now stay as written and a shim does the
    dispatch; see DisplayShimTests in tests/test_notebook_runtime_fatal.py.
    """

    def test_a_simple_display_keeps_its_call_and_gains_the_shim(self):
        r = _translate("display(df)")
        self.assertIn("\ndisplay(df)", r.translated_sql)
        self.assertIn("def display(obj", r.translated_sql)
        self.assertEqual(r.flags, 0)

    def test_a_complex_display_argument_is_no_longer_flagged(self):
        """The old flag existed because the call-site rewrite could not
        handle the argument. The shim dispatches on the object, so there is
        nothing left for a human to decide."""
        r = _translate("display(df.groupBy('a').count())")
        self.assertEqual(r.flags, 0)
        self.assertIn("display(df.groupBy('a').count())", r.translated_sql)


class NotebookStructureTests(unittest.TestCase):
    def test_markdown_and_metadata_are_untouched(self):
        source = ("# Fabric notebook source\n\n"
                  "# MARKDOWN ********************\n\n# # Title\n\n"
                  "# CELL ********************\n\ndisplay(df)\n")
        out = nb2spark.translate(source, namespace=NS).translated_sql
        self.assertIn("# # Title", out)
        self.assertIn("# MARKDOWN ********************", out)

    def test_unparseable_notebook_is_one_flag_not_a_crash(self):
        r = nb2spark.translate("not a fabric notebook", namespace=NS)
        self.assertEqual(r.flags, 1)
        self.assertEqual(r.translated_sql, "not a fabric notebook")

    def test_clean_notebook_round_trips_unchanged(self):
        source = _nb("x = 1")
        self.assertEqual(nb2spark.translate(source, namespace=NS).translated_sql, source)


class TsqlTests(unittest.TestCase):
    def test_procedure_is_flagged_as_out_of_scope(self):
        r = tsql.translate("CREATE PROCEDURE dbo.sp AS BEGIN SELECT 1 END", kind="procedure")
        self.assertEqual(r.flags, 1)
        self.assertIn("control flow", r.findings[0].detail.lower())

    def test_control_flow_in_a_non_procedure_is_still_flagged(self):
        r = tsql.translate("DECLARE @x INT; SET @x = 1;", kind="other")
        self.assertTrue(any("control flow" in f.detail.lower() for f in r.findings))

    def test_plain_table_ddl_translates_cleanly(self):
        r = tsql.translate("CREATE TABLE dbo.claim (id INT)", kind="table")
        self.assertEqual(r.flags, 0)
        self.assertFalse(r.needs_manual_review)

    def test_clean_objects_reach_zero_flags(self):
        for sql in ("CREATE TABLE dbo.t (id INT)", "CREATE VIEW dbo.v AS SELECT 1",
                    "SELECT 1"):
            with self.subTest(sql=sql):
                self.assertFalse(tsql.translate(sql).needs_manual_review)

    def test_coverage_is_declared_per_translator(self):
        self.assertEqual(tsql.RULESET_COVERAGE, "tsql-v1")
        self.assertEqual(nb2spark.RULESET_COVERAGE, "notebook-v1")

    def test_source_is_returned_unchanged(self):
        sql = "CREATE TABLE dbo.claim (id INT)"
        self.assertEqual(tsql.translate(sql, kind="table").translated_sql, sql)

    def test_control_flow_keyword_inside_a_string_is_not_control_flow(self):
        r = tsql.translate("SELECT 'DECLARE @x' AS note", kind="other")
        self.assertFalse(any("control flow" in f.detail.lower() for f in r.findings))


if __name__ == "__main__":
    unittest.main()
