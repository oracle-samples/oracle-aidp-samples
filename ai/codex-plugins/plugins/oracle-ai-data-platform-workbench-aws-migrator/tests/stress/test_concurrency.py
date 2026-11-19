from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from aws_aidp.migrate import migrate
from tests.stress.helpers import athena_asset, plan_with


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.plan = plan_with(*[
            athena_asset(
                asset_id=f"athena.query.q{i}", name=f"query-{i}",
                query=f"SELECT {i} AS value",
            )
            for i in range(20)
        ])

    def test_eight_parallel_isolated_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def execute(index):
                out = root / f"run-{index}"
                return migrate(self.plan, out_dir=out, filter_kind="athena", demo=True)

            with ThreadPoolExecutor(max_workers=8) as pool:
                reports = list(pool.map(execute, range(8)))
            self.assertTrue(all(report["counts"]["ok"] == 20 for report in reports))
            for index in range(8):
                saved = json.loads((root / f"run-{index}" / "report.json").read_text())
                self.assertEqual(len(saved["results"]), 20)

    def test_same_directory_parallel_runs_leave_valid_complete_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "shared"

            def execute(_):
                return migrate(self.plan, out_dir=out, filter_kind="athena", demo=True)

            with ThreadPoolExecutor(max_workers=8) as pool:
                reports = list(pool.map(execute, range(8)))
            self.assertTrue(all(report["counts"]["error"] == 0 for report in reports))
            saved = json.loads((out / "report.json").read_text())
            self.assertEqual(len(saved["results"]), 20)
            self.assertEqual(saved["counts"]["ok"], 20)
            self.assertEqual(len(list((out / "athena").glob("*.spark.sql"))), 20)
            self.assertFalse((out / ".aws-aidp-migration-in-progress").exists())

    def test_competing_plans_cannot_leave_mixed_report_and_artifacts(self):
        plan_a = plan_with(*[
            athena_asset(
                asset_id=f"athena.query.q{i}", name=f"shared-{i}", query=f"SELECT 'A-{i}'"
            )
            for i in range(12)
        ])
        plan_a["plan_id"] = "plan-A"
        plan_b = plan_with(*[
            athena_asset(
                asset_id=f"athena.query.q{i}", name=f"shared-{i}", query=f"SELECT 'B-{i}'"
            )
            for i in range(12)
        ])
        plan_b["plan_id"] = "plan-B"

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "shared"
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(
                    lambda plan: migrate(plan, out_dir=out, filter_kind="athena", demo=True),
                    (plan_a, plan_b),
                ))

            saved = json.loads((out / "report.json").read_text())
            markdown = (out / "report.md").read_text()
            self.assertIn(saved["report_id"], markdown)
            self.assertIn(saved["plan_id"], markdown)
            for row in saved["results"]:
                self.assertEqual(
                    (out / row["output_path"]).read_text().splitlines()[-1],
                    row["translated_sql"],
                )


if __name__ == "__main__":
    unittest.main()
