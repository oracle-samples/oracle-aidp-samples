import unittest

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark

CATALOG = {"tables": {
    "dbo.claim": {"tier": "warehouse_ddl", "name": "dbo.claim",
                  "warehouse": "AcmeDW"},
    "claims_raw": {"tier": "shortcut", "name": "claims_raw",
                   "target": "s3://acme/claims"},
    "claims_agg": {"tier": "notebook_inferred", "name": "claims_agg",
                   "created_by": "Build_Aggregates"},
}}
HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"

CLEAN = HEADER + (
    'df = spark.read.parquet('
    '"abfss://W@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Files/raw")\n'
    'claims = spark.table("dbo.claim")\n'
    'display(claims)\n')

GAPPY = HEADER + (
    'secret = notebookutils.credentials.getSecret("kv", "k")\n'
    'raw = spark.table("claims_raw")\n'
    'missing = spark.table("nope")\n')


def _t(source, **kw):
    kw.setdefault("namespace", "acmens")
    kw.setdefault("default_lakehouse", "Sales")
    kw.setdefault("catalog", CATALOG)
    return nb2spark.translate(source, **kw)


class CleanNotebookTests(unittest.TestCase):
    def test_clean_notebook_reaches_zero_flags(self):
        result = _t(CLEAN)
        self.assertEqual(result.flags, 0, [str(f) for f in result.findings])

    def test_every_expected_rewrite_lands(self):
        out = _t(CLEAN).translated_sql
        self.assertIn("oci://W_Sales_Lakehouse@acmens/Files/raw", out)
        # `AcmeDW`, not the notebook's default lakehouse `Sales`: the catalog
        # entry records the Warehouse that declared `dbo.claim`, and that is
        # the item the table lives in. The file path above is a different
        # question and still resolves against the lakehouse.
        self.assertIn('spark.table("default.AcmeDW.claim")', out)
        # `display(claims)` keeps its call site. Fabric's display() renders
        # a pandas DataFrame as happily as a Spark one, and `.show()` is a
        # method only the latter has; the spliced shim does the dispatch.
        self.assertIn("\ndisplay(claims)", out)
        self.assertIn("def display(obj", out)

    def test_round_trip_structure_is_preserved(self):
        out = _t(CLEAN).translated_sql
        self.assertTrue(out.startswith("# Fabric notebook source"))
        self.assertIn("# CELL ********************", out)


class GappyNotebookTests(unittest.TestCase):
    def test_exactly_the_expected_flags(self):
        result = _t(GAPPY)
        self.assertEqual(
            sorted({f.rule for f in result.findings if f.severity == "flag"}),
            ["NB03_NOTEBOOKUTILS", "NB11_TABLE_IS_SHORTCUT", "NB12_TABLE_UNKNOWN"])

    def test_flagged_constructs_are_left_in_place(self):
        out = _t(GAPPY).translated_sql
        self.assertIn("notebookutils.credentials.getSecret", out)
        self.assertIn('spark.table("claims_raw")', out)
        self.assertIn('spark.table("nope")', out)


class CoverageTests(unittest.TestCase):
    def test_coverage_is_declared(self):
        self.assertEqual(nb2spark.RULESET_COVERAGE, "notebook-v1")

    def test_translation_is_deterministic(self):
        first, second = _t(CLEAN), _t(CLEAN)
        self.assertEqual(first.translated_sql, second.translated_sql)
        self.assertEqual([str(f) for f in first.findings],
                         [str(f) for f in second.findings])


if __name__ == "__main__":
    unittest.main()
