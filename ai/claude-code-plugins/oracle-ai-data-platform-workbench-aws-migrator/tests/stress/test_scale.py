from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

from aws_aidp.migrate import migrate
from aws_aidp.plan import build_plan
from aws_aidp.translate.athena_to_spark_sql import translate as athena_translate
from aws_aidp.translate.glue_to_spark import translate as glue_translate
from tests.stress.helpers import athena_asset, plan_with


RUN_SLOW = os.environ.get("AWS_AIDP_RUN_SLOW") == "1"


# These two are hang guards, not benchmarks. Translation cost is linear in input
# size (measured: 0.69s / 1.35s / 2.71s / 5.50s for 10k / 20k / 40k / 80k lines),
# so the ceiling only has to be low enough to catch a hang or a super-linear
# blowup while staying green on slow or loaded hardware. A tight wall-clock bound
# turns an unrelated CI machine into a test failure, which is what it did before.
HANG_CEILING_SECONDS = 60.0


class CoreScaleTests(unittest.TestCase):
    def test_one_thousand_translations_do_not_hang(self):
        sql = "SELECT array_agg(x), cardinality(tags), zip(a,b) FROM t"
        py = 'df.write.parquet("s3://bucket/path")\n'
        start = time.perf_counter()
        for _ in range(1_000):
            athena_translate(sql)
            glue_translate(py, oci_namespace="ns")
        self.assertLess(time.perf_counter() - start, HANG_CEILING_SECONDS)

    def test_one_megabyte_clean_inputs_do_not_hang(self):
        sql = "SELECT 1 -- harmless text\n" * 40_000
        py = "value = 1  # harmless text\n" * 40_000
        start = time.perf_counter()
        athena_translate(sql)
        glue_translate(py, oci_namespace="ns")
        self.assertLess(time.perf_counter() - start, HANG_CEILING_SECONDS)


@unittest.skipUnless(RUN_SLOW, "set AWS_AIDP_RUN_SLOW=1 for release-scale tests")
class ReleaseScaleTests(unittest.TestCase):
    def test_ten_thousand_asset_plan(self):
        manifest = {
            "account_id": "offline",
            "region": "us-east-1",
            "scanned_at": "2026-01-01T00:00:00Z",
            "sources": {
                "s3": {
                    "items": [
                        {"name": f"bucket-{i}", "region": "us-east-1"}
                        for i in range(10_000)
                    ]
                }
            },
        }
        start = time.perf_counter()
        plan = build_plan(manifest, oci_namespace="ns")
        elapsed = time.perf_counter() - start
        self.assertEqual(len(plan["assets"]), 10_000)
        self.assertLess(elapsed, 30.0)

    def test_one_thousand_artifact_migration(self):
        assets = [
            athena_asset(
                asset_id=f"athena.query.q{i}", name=f"q{i}",
                query="SELECT cardinality(a) FROM t",
            )
            for i in range(1_000)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            start = time.perf_counter()
            report = migrate(plan_with(*assets), out_dir=Path(tmp), demo=True)
            self.assertEqual(len(report["results"]), 1_000)
            self.assertLess(time.perf_counter() - start, 60.0)


if __name__ == "__main__":
    unittest.main()
