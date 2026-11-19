from __future__ import annotations

import json
import tempfile
import unittest
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files
from pathlib import Path

from aws_aidp import __version__
from aws_aidp.migrate import migrate
from aws_aidp.plan import build_plan
from aws_aidp.verify import verify
from tests.stress.helpers import REPO_ROOT, load_fixture, run_cli


class FixtureBaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = load_fixture()
        cls.plan = build_plan(cls.manifest, oci_namespace="stress-ns")

    def test_fixture_counts(self):
        sources = self.manifest["sources"]
        self.assertEqual(sources["s3"]["summary"]["bucket_count"], 5)
        self.assertEqual(sources["glue"]["summary"]["database_count"], 2)
        self.assertEqual(sources["glue"]["summary"]["table_count"], 50)
        self.assertEqual(sources["glue"]["summary"]["job_count"], 8)
        self.assertEqual(sources["athena"]["summary"]["named_query_count"], 20)
        self.assertEqual(sources["emr"]["summary"]["cluster_count"], 2)
        self.assertEqual(sources["emr"]["summary"]["notebook_execution_count"], 4)
        self.assertEqual(sources["sagemaker"]["summary"]["training_job_count"], 12)

    def test_plan_has_expected_asset_count(self):
        self.assertEqual(self.plan["summary"]["asset_count"], 113)
        self.assertEqual(sum(self.plan["summary"]["by_target_type"].values()), 113)

    def test_athena_migration_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = migrate(
                self.plan, out_dir=Path(tmp), filter_kind="athena", demo=True
            )
            # high_risk_zones carries a backslash regex literal whose meaning
            # differs between the Athena and Spark parsers (regex_escape_sequence).
            self.assertEqual(report["counts"]["ok"], 13)
            self.assertEqual(report["counts"]["needs_manual_review"], 7)
            self.assertEqual(report["counts"]["planned"], 0)
            result = verify(Path(tmp) / "report.json", filter_kind="athena")
            self.assertEqual(result["summary"], {
                "PASS": 13, "REVIEW": 7, "SKIP": 0, "FAIL": 0
            })

    def test_glue_filter_selects_entire_source_slice(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = migrate(
                self.plan, out_dir=Path(tmp), filter_kind="glue", demo=True
            )
            self.assertEqual(len(report["results"]), 60)
            self.assertEqual(report["counts"]["ok"], 0)
            self.assertEqual(report["counts"]["needs_manual_review"], 8)
            self.assertEqual(report["counts"]["planned"], 52)
            result = verify(Path(tmp) / "report.json", filter_kind="glue")
            self.assertEqual(result["summary"], {
                "PASS": 0, "REVIEW": 8, "SKIP": 52, "FAIL": 0
            })

    def test_fixture_sources_filter_is_honored(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "inventory.json"
            proc = run_cli(
                "inventory", "--fixture", "demo", "--sources", "glue,athena",
                "-o", str(output),
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            manifest = json.loads(output.read_text())
            self.assertEqual(set(manifest["sources"]), {"glue", "athena"})
            self.assertEqual(manifest["sources_scanned"], ["glue", "athena"])

    def test_runtime_version_matches_package_metadata(self):
        try:
            installed_version = version("aws-aidp-migrator")
        except PackageNotFoundError:
            self.skipTest("distribution metadata requires `pip install -e .`")
        self.assertEqual(__version__, installed_version)

    def test_package_version_uses_runtime_attribute(self):
        metadata = (REPO_ROOT / "pyproject.toml").read_text()
        self.assertIn('dynamic = ["version"]', metadata)
        self.assertIn('version = { attr = "aws_aidp.__version__" }', metadata)

    def test_demo_fixture_is_an_installed_package_resource(self):
        fixture = files("aws_aidp.fixtures").joinpath("demo-manifest.json")
        self.assertTrue(fixture.is_file())
        self.assertIn("sources", json.loads(fixture.read_text(encoding="utf-8")))

    def test_packaging_does_not_claim_top_level_fixtures_namespace(self):
        metadata = (REPO_ROOT / "pyproject.toml").read_text()
        self.assertNotIn('"fixtures*"', metadata)
        self.assertIn('"aws_aidp.fixtures" = ["*.json"]', metadata)

    def test_mcp_dependency_excludes_incompatible_major_version(self):
        metadata = (REPO_ROOT / "pyproject.toml").read_text()
        # the <2 cap is the contract; a python_version marker may follow it
        self.assertIn('mcp>=1.2,<2', metadata)


if __name__ == "__main__":
    unittest.main()
