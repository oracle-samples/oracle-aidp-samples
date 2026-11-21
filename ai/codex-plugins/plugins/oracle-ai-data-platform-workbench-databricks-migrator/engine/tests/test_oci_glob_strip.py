"""Fix (2026-08-11) — reactive glob-strip for BucketNotFound on cross-tenancy
all-wildcard oci:// reads.

A cross-tenancy `.format('org.apache.hudi').load('oci://…/base/*/*/*')` fails with
OCI `BucketNotFound` (the wildcard expansion needs a bucket-level LIST the cluster
can't do cross-tenancy) even though the base location reads fine. The fix strips
the TRAILING all-wildcard glob so the read targets the base location, preserving
the cell's read `.format(...)` and every downstream op.

These tests lock the safety properties of `_strip_trailing_glob_oci_reads`:
  - it strips ONLY trailing '*' segments,
  - it NEVER rewrites when a wildcard would remain mid-path (leaves it for Opus),
  - it preserves the read format and any non-glob literals,
  - it is idempotent (can't loop),
  - it only touches oci:// literals.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from job_migrate import _strip_trailing_glob_oci_reads, _is_glob_fixable_bucketnotfound  # noqa: E402


class TestOciGlobStrip(unittest.TestCase):
    def test_the_hrc2tele_case(self):
        # The exact failing read from example_dues_all Cell 8.
        code = ("cd = spark.read.format('org.apache.hudi')"
                ".load('oci://oci-example-data-lake@exmplnspace2/raw-data/"
                "lending_customer_service/credit_analysis_data/*/*/*')")
        new, changed = _strip_trailing_glob_oci_reads(code)
        self.assertTrue(changed)
        self.assertIn(
            "'oci://oci-example-data-lake@exmplnspace2/raw-data/"
            "lending_customer_service/credit_analysis_data'", new)
        self.assertNotIn("*", new)
        # read format is NOT touched
        self.assertIn("spark.read.format('org.apache.hudi').load(", new)

    def test_single_trailing_star(self):
        new, changed = _strip_trailing_glob_oci_reads("df=spark.read.load('oci://b@ns/x/y/*')")
        self.assertTrue(changed)
        self.assertIn("'oci://b@ns/x/y'", new)

    def test_selective_partition_trailing_star_keeps_scope(self):
        # `.../year=2024/*` → `.../year=2024`: same scope (all files under 2024).
        new, changed = _strip_trailing_glob_oci_reads("spark.read.load('oci://b@ns/t/year=2024/*')")
        self.assertTrue(changed)
        self.assertIn("'oci://b@ns/t/year=2024'", new)
        self.assertNotIn("*", new)

    def test_midpath_wildcard_NOT_rewritten(self):
        # A wildcard would remain mid-path after stripping the trailing one → we
        # must NOT rewrite (could silently change the selected file set).
        code = "spark.read.load('oci://b@ns/t/*/region/*')"
        new, changed = _strip_trailing_glob_oci_reads(code)
        self.assertFalse(changed)
        self.assertEqual(new, code)

    def test_no_glob_untouched(self):
        code = "spark.read.table('x'); spark.read.load('oci://b@ns/t/credit_analysis_data')"
        new, changed = _strip_trailing_glob_oci_reads(code)
        self.assertFalse(changed)
        self.assertEqual(new, code)

    def test_fully_wildcard_bucket_not_rewritten(self):
        # Must not collapse to a malformed 'oci:/' or read an entire bucket.
        for code in ["spark.read.load('oci://*/*/*')",
                     "spark.read.load('oci://b@ns/*')"]:
            new, changed = _strip_trailing_glob_oci_reads(code)
            self.assertFalse(changed, code)
            self.assertEqual(new, code)

    def test_only_globbed_literal_changed_others_preserved(self):
        code = ("a=spark.read.load('oci://b@ns/keep/this')\n"
                "b=spark.read.format('org.apache.hudi').load('oci://b@ns/data/*/*/*')")
        new, changed = _strip_trailing_glob_oci_reads(code)
        self.assertTrue(changed)
        self.assertIn("'oci://b@ns/keep/this'", new)      # untouched
        self.assertIn("'oci://b@ns/data'", new)           # stripped
        self.assertNotIn("*", new)

    def test_idempotent(self):
        code = "spark.read.load('oci://b@ns/x/y/*/*/*')"
        once, c1 = _strip_trailing_glob_oci_reads(code)
        twice, c2 = _strip_trailing_glob_oci_reads(once)
        self.assertTrue(c1)
        self.assertFalse(c2)          # second pass is a no-op → cannot loop
        self.assertEqual(once, twice)

    def test_non_oci_glob_untouched(self):
        # Only oci:// literals are in scope; a /Volumes or s3 glob is not touched.
        code = "spark.read.load('/Volumes/a/b/*/*'); spark.read.load('s3://x/y/*')"
        new, changed = _strip_trailing_glob_oci_reads(code)
        self.assertFalse(changed)
        self.assertEqual(new, code)

    def test_double_quotes(self):
        new, changed = _strip_trailing_glob_oci_reads('spark.read.load("oci://b@ns/x/y/*/*")')
        self.assertTrue(changed)
        self.assertIn('"oci://b@ns/x/y"', new)

    def test_downstream_ops_preserved(self):
        code = ("cd=(spark.read.format('org.apache.hudi')"
                ".load('oci://b@ns/t/credit_analysis_data/*/*/*')"
                ".select('id','created_at').filter(col('x')>1).dropDuplicates())")
        new, changed = _strip_trailing_glob_oci_reads(code)
        self.assertTrue(changed)
        # every downstream op is intact
        for frag in [".select('id','created_at')", ".filter(col('x')>1)", ".dropDuplicates()"]:
            self.assertIn(frag, new)
        self.assertNotIn("*", new)


class TestGlobFixableGate(unittest.TestCase):
    """The eval-skip gate: True ONLY when the failure is a BucketNotFound on a Hudi
    read with a strippable trailing glob (so the deterministic glob-strip will handle
    it and the ~5-round Opus eval can be skipped). Must be conservative — any other
    failure keeps the eval."""

    GLOB_CELL = ("cd = spark.read.format('org.apache.hudi')"
                 ".load('oci://oci-example-data-lake@exmplnspace2/raw-data/x/credit_analysis_data/*/*/*')")
    BNF = "java.lang.RuntimeException: ... BucketNotFound ... Unable to determine if path is a directory"

    def test_true_for_hudi_glob_bucketnotfound(self):
        self.assertTrue(_is_glob_fixable_bucketnotfound(self.BNF, self.GLOB_CELL))

    def test_false_when_not_bucketnotfound(self):
        self.assertFalse(_is_glob_fixable_bucketnotfound("AnalysisException: something else", self.GLOB_CELL))

    def test_false_when_not_hudi(self):
        # BucketNotFound + glob but NOT a hudi read → not our deterministic case → keep eval
        parquet_glob = "df = spark.read.parquet('oci://b@ns/x/*/*/*')"
        self.assertFalse(_is_glob_fixable_bucketnotfound(self.BNF, parquet_glob))

    def test_false_when_no_strippable_glob(self):
        # hudi read, BucketNotFound, but NO trailing all-wildcard glob → strip is a no-op → keep eval
        base = "cd = spark.read.format('org.apache.hudi').load('oci://b@ns/x/credit_analysis_data')"
        self.assertFalse(_is_glob_fixable_bucketnotfound(self.BNF, base))

    def test_false_when_midpath_wildcard_only(self):
        # mid-path wildcard is NOT strippable (would leave a '*') → keep eval / defer to Opus
        mid = "cd = spark.read.format('org.apache.hudi').load('oci://b@ns/x/*/region/*')"
        # note: trailing '*' strips to '.../x/*/region' which still has '*' → strip refuses → False
        self.assertFalse(_is_glob_fixable_bucketnotfound(self.BNF, mid))

    def test_false_on_empty(self):
        self.assertFalse(_is_glob_fixable_bucketnotfound("", ""))
        self.assertFalse(_is_glob_fixable_bucketnotfound(None, None))


if __name__ == "__main__":
    unittest.main()
