"""Fix A (deterministic) — scripts/dbfs_oci.py: redirect ONLY large single DBFS
Spark reads to the oci:// backing; leave small files / dirs / non-DBFS / table
reads / writes untouched.

Pure-function + source-transform unit checks (no cluster). The runtime size gate
(os.path.getsize) and DESCRIBE VOLUME resolution are exercised live on-cluster
separately; here we lock the path mapping and the wrap transform.
"""

from __future__ import annotations

import pathlib
import sys
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import dbfs_oci  # noqa: E402


class TestDbfsSubpath(unittest.TestCase):
    def test_filestore(self):
        self.assertEqual(dbfs_oci.dbfs_subpath("/FileStore/tables/p/x.csv"),
                         "FileStore/tables/p/x.csv")

    def test_dbfs_slash(self):
        self.assertEqual(dbfs_oci.dbfs_subpath("/dbfs/FileStore/x"), "FileStore/x")

    def test_dbfs_scheme(self):
        self.assertEqual(dbfs_oci.dbfs_subpath("dbfs:/FileStore/x"), "FileStore/x")

    def test_already_volumes(self):
        self.assertEqual(dbfs_oci.dbfs_subpath("/Volumes/default/default/dbfs/FileStore/x"),
                         "FileStore/x")

    def test_volumes_custom_catalog_schema(self):
        self.assertEqual(
            dbfs_oci.dbfs_subpath("/Volumes/cat/sch/dbfs/a/b", catalog="cat", schema="sch"),
            "a/b")

    def test_non_dbfs_return_none(self):
        for p in ("oci://b@ns/x", "s3://b/x", "s3a://b/x", "/mnt/m/x",
                  "/some/local/x", "table_name", "db.table", ""):
            self.assertIsNone(dbfs_oci.dbfs_subpath(p), p)

    def test_non_str_none(self):
        self.assertIsNone(dbfs_oci.dbfs_subpath(None))
        self.assertIsNone(dbfs_oci.dbfs_subpath(123))

    def test_volumes_and_oci_builders(self):
        self.assertEqual(dbfs_oci.volumes_path("FileStore/x"),
                         "/Volumes/default/default/dbfs/FileStore/x")
        self.assertEqual(dbfs_oci.oci_path("FileStore/x", "oci://b@ns/root/"),
                         "oci://b@ns/root/FileStore/x")


class TestWrapSparkReads(unittest.TestCase):
    M = "_aidp_read_dbfs"

    def test_wraps_csv_literal(self):
        out = dbfs_oci.wrap_spark_reads("spark.read.csv('/FileStore/x.csv')")
        self.assertEqual(out, f"spark.read.csv({self.M}('/FileStore/x.csv'))")

    def test_wraps_only_first_arg_keeps_kwargs(self):
        src = "spark.read.csv(location+'/Files/v6.csv', header=True)"
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(
            out, f"spark.read.csv({self.M}(location+'/Files/v6.csv'), header=True)")

    def test_wraps_parquet_json_load(self):
        for m in ("parquet", "json", "load", "orc", "text"):
            src = f"df = spark.read.{m}(p)"
            self.assertIn(f"spark.read.{m}({self.M}(p))", dbfs_oci.wrap_spark_reads(src))

    def test_wraps_format_load(self):
        src = "spark.read.format('org.apache.hudi').load(path)"
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(out, f"spark.read.format('org.apache.hudi').load({self.M}(path))")

    def test_idempotent(self):
        once = dbfs_oci.wrap_spark_reads("spark.read.csv(p)")
        twice = dbfs_oci.wrap_spark_reads(once)
        self.assertEqual(once, twice)

    def test_does_not_wrap_table_read(self):
        src = 'spark.read.table("db.tbl")'
        self.assertEqual(dbfs_oci.wrap_spark_reads(src), src)  # table read untouched

    def test_does_not_touch_writes_or_pandas(self):
        src = ("df.write.mode('overwrite').csv('/FileStore/out')\n"
               "pd.read_csv('/FileStore/x.csv')\n"
               "june_pd.to_csv('/dbfs/FileStore/x.csv')")
        self.assertEqual(dbfs_oci.wrap_spark_reads(src), src)  # nothing matched

    def test_multiple_reads(self):
        src = "a=spark.read.csv(p1)\nb=spark.read.parquet(p2)"
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertIn(f"spark.read.csv({self.M}(p1))", out)
        self.assertIn(f"spark.read.parquet({self.M}(p2))", out)

    def test_no_spark_read_unchanged(self):
        src = "x = 1 + 2\nprint('hi')"
        self.assertEqual(dbfs_oci.wrap_spark_reads(src), src)

    def test_nested_parens_in_arg(self):
        src = "spark.read.csv(os.path.join(base, 'x.csv'))"
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(out, f"spark.read.csv({self.M}(os.path.join(base, 'x.csv')))")

    # ── Reviewer-flagged edge cases (fail-safe, mask-based) ──────────────────
    def _parses(self, code):
        import ast
        ast.parse(code)

    def test_wraps_option_chain_csv(self):
        # #4: .option(...) chains are the dominant real-world read idiom
        src = 'spark.read.option("header", True).csv(path)'
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(out, f'spark.read.option("header", True).csv({self.M}(path))')
        self._parses(out)

    def test_wraps_format_option_load_chain(self):
        src = 'spark.read.format("csv").option("h", 1).load(p)'
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(out, f'spark.read.format("csv").option("h", 1).load({self.M}(p))')
        self._parses(out)

    def test_inline_comment_in_read_arg_not_corrupted(self):
        # #2: an inline '#' with a ')' inside the call must NOT emit a SyntaxError.
        src = "df = spark.read.csv(  # load the data :)\n    path)"
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(out, src)          # left unchanged (fail-safe)
        self._parses(out)

    def test_read_pattern_inside_string_literal_untouched(self):
        # #3: a read pattern appearing inside a string literal must be left alone.
        src = 'msg = "run spark.read.csv(oldpath) now"'
        self.assertEqual(dbfs_oci.wrap_spark_reads(src), src)

    def test_real_read_after_a_string_mentioning_read(self):
        # mask must resync after the string so the REAL read still gets wrapped
        src = 'log("spark.read.csv(x)")\nspark.read.csv(realpath)'
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(
            out, f'log("spark.read.csv(x)")\nspark.read.csv({self.M}(realpath))')
        self._parses(out)

    def test_nested_option_call_is_safe_miss(self):
        # nested-paren modifier body defeats the regex -> unwrapped, still valid
        src = 'spark.read.option("k", get_opt()).csv(p)'
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(out, src)          # safe miss
        self._parses(out)

    def test_paren_inside_string_arg_not_miscounted(self):
        src = "spark.read.csv('/data/(weird)/x.csv', header=True)"
        out = dbfs_oci.wrap_spark_reads(src)
        self.assertEqual(
            out, f"spark.read.csv({self.M}('/data/(weird)/x.csv'), header=True)")
        self._parses(out)


class TestHelperSourceValid(unittest.TestCase):
    def test_helper_src_compiles(self):
        # the injected runtime helper must be valid Python
        compile(dbfs_oci.DBFS_READ_HELPER_SRC, "<DBFS_READ_HELPER_SRC>", "exec")

    def test_helper_defines_function(self):
        self.assertIn("def _aidp_read_dbfs", dbfs_oci.DBFS_READ_HELPER_SRC)
        self.assertIn("DESCRIBE VOLUME", dbfs_oci.DBFS_READ_HELPER_SRC)
        # generic: no hardcoded the pilot deployment backing
        self.assertNotIn("example-aidp-prod-data", dbfs_oci.DBFS_READ_HELPER_SRC)
        self.assertNotIn("exmplnspace1", dbfs_oci.DBFS_READ_HELPER_SRC)


if __name__ == "__main__":
    unittest.main()
