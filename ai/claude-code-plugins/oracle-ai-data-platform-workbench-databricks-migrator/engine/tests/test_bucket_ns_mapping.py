"""Regression test for the S3→OCI bucket/namespace mapping hardening.

Guards against the 2026-08-03 defect found migrating the pilot deployment job
"Kafka vs raw sources transactions check" (DBX job 200000000000002):

  Source cell: archive_base_path = "s3a://examplecorp-store-alpha/archive/..."

  config/bucket_map.csv (a gitignored, regenerated deployment
  artifact) carried a PHANTOM entry `example-store-alpha-oci -> exmplnspace2`.
  That bucket name exists in NEITHER namespace (verified on-cluster via the
  hadoop-oci connector), while the real bucket `oci-example-store-alpha` lives
  at `exmplnspace1`. The phantom entry let the migrator resolve the archive
  read to `oci://example-store-alpha-oci@exmplnspace2/...` → BucketNotFound → 0
  rows → a runaway "scratch_card_archive_missing_percentage = 100.0" loop.

Because the mapping file is gitignored (and can be regenerated with the
phantom back), the durable fix is in the LOADER: sanitize_bucket_index() drops
any `<base>-oci` suffix bucket whose `oci-<base>` prefix twin disagrees on
namespace. This test exercises that logic on SYNTHETIC data so it runs in CI
without the deployment artifact.

Run: python -m pytest tests/test_bucket_ns_mapping.py -v
"""
from __future__ import annotations

import pathlib
import sys
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import context_tools as ct  # noqa: E402


class TestSanitizeBucketIndex(unittest.TestCase):
    def test_drops_namespace_mismatched_suffix_twin(self):
        idx = {
            "oci-example-store-alpha": "exmplnspace1",   # real
            "example-store-alpha-oci": "exmplnspace2",   # phantom (mismatch)
            "some-other-bucket": "exmplnspace1",
        }
        clean, dropped = ct.sanitize_bucket_index(idx)
        self.assertIn("example-store-alpha-oci", dropped)
        self.assertNotIn("example-store-alpha-oci", clean)
        self.assertEqual(clean["oci-example-store-alpha"], "exmplnspace1")
        self.assertIn("some-other-bucket", clean)

    def test_keeps_suffix_bucket_without_prefix_twin(self):
        # A `<base>-oci` bucket with no `oci-<base>` twin is a legitimate,
        # distinct bucket and must be preserved.
        idx = {"example-bucket-beta-oci": "exmplnspace2"}
        clean, dropped = ct.sanitize_bucket_index(idx)
        self.assertEqual(dropped, [])
        self.assertEqual(clean, idx)

    def test_keeps_agreeing_twins(self):
        # If both forms agree on namespace, neither is a phantom — keep both.
        idx = {"oci-foo": "exmplnspace1", "foo-oci": "exmplnspace1"}
        clean, dropped = ct.sanitize_bucket_index(idx)
        self.assertEqual(dropped, [])
        self.assertEqual(len(clean), 2)


class TestResolverAfterSanitize(unittest.TestCase):
    def test_resolver_picks_prefix_form(self):
        # Force a reload so the loader's sanitize step is applied, then confirm
        # the S3 name resolves to the real oci- prefix bucket at exmplnspace1.
        ct._BUCKET_MAPPING = None
        ct._BUCKET_NS_INDEX = {}
        r = ct.find_oci_bucket("examplecorp-store-alpha")
        # If the deployment mapping file is absent (CI), the index is empty and
        # there is nothing to resolve — the sanitize logic is covered above.
        if not r:
            self.skipTest("bucket mapping artifact not present (CI)")
        self.assertEqual(r[0]["oci_bucket"], "oci-example-store-alpha")
        self.assertEqual(r[0]["oci_namespace"], "exmplnspace1")


if __name__ == "__main__":
    unittest.main()
