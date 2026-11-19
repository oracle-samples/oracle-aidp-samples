import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.fixtures import demo_workspace_path
from fabric_aidp.inventory.manifest import ALL_SOURCES, build_manifest
from fabric_aidp.migrate.runner import migrate
from fabric_aidp.plan.planner import build_plan
from fabric_aidp.verify.checker import verify


class FixtureShapeTests(unittest.TestCase):
    def setUp(self):
        self.manifest = build_manifest(demo_workspace_path(), ALL_SOURCES)
        self.sources = self.manifest["sources"]

    def test_the_fixture_directory_exists(self):
        self.assertTrue(demo_workspace_path().is_dir())

    def test_every_source_is_populated(self):
        self.assertEqual(self.sources["notebook"]["summary"]["notebook_count"], 6)
        self.assertEqual(self.sources["warehouse"]["summary"]["warehouse_count"], 1)
        self.assertEqual(self.sources["lakehouse"]["summary"]["lakehouse_count"], 2)
        self.assertEqual(self.sources["pipeline"]["summary"]["pipeline_count"], 1)
        self.assertEqual(
            self.sources["semanticmodel"]["summary"]["semantic_model_count"], 1)

    def test_no_notebook_fails_to_parse(self):
        self.assertEqual(self.sources["notebook"]["summary"]["parse_error_count"], 0)

    def test_warehouse_object_mix(self):
        counts = self.sources["warehouse"]["summary"]["object_counts"]
        self.assertEqual(counts["table"], 8)
        self.assertEqual(counts["view"], 2)
        self.assertEqual(counts["procedure"], 3)

    def test_one_lakehouse_has_tracking_disabled(self):
        lakehouses = self.sources["lakehouse"]["items"]["lakehouses"]
        states = sorted(lh["tracking"]["shortcuts"] for lh in lakehouses)
        self.assertEqual(states, ["not_tracked", "tracked"])

    def test_untracked_lakehouse_reports_unknown_not_zero(self):
        untracked = next(lh for lh in self.sources["lakehouse"]["items"]["lakehouses"]
                         if lh["tracking"]["shortcuts"] == "not_tracked")
        self.assertIsNone(untracked["shortcut_count"])

    def test_all_three_shortcut_kinds_are_present(self):
        tracked = next(lh for lh in self.sources["lakehouse"]["items"]["lakehouses"]
                       if lh["tracking"]["shortcuts"] == "tracked")
        kinds = sorted(s["target_type"] for s in tracked["shortcuts"])
        self.assertEqual(kinds, ["AdlsGen2", "AmazonS3", "OneLake"])

    def test_catalog_uses_every_tier_that_can_appear(self):
        tiers = {e["tier"] for e in self.manifest["resolved_catalog"]["tables"].values()}
        self.assertIn("warehouse_ddl", tiers)
        self.assertIn("shortcut", tiers)
        self.assertIn("notebook_inferred", tiers)


class FixturePipelineTests(unittest.TestCase):
    def _run(self):
        manifest = build_manifest(demo_workspace_path(), ALL_SOURCES)
        plan = build_plan(manifest, oci_namespace="acmens")
        with TemporaryDirectory() as t:
            out = Path(t)
            report = migrate(plan, out_dir=out)
            return plan, report, verify(out / "report.json")

    def test_the_whole_pipeline_runs_without_error(self):
        _plan, report, result = self._run()
        self.assertEqual(report["counts"].get("error", 0), 0)
        self.assertEqual(result["summary"]["FAIL"], 0)

    def test_something_reaches_pass(self):
        _plan, _report, result = self._run()
        self.assertGreater(result["summary"]["PASS"], 0)

    def test_something_needs_review(self):
        _plan, _report, result = self._run()
        self.assertGreater(result["summary"]["REVIEW"], 0)

    def test_the_plan_has_no_dangling_dependencies(self):
        plan, _report, _result = self._run()
        self.assertEqual(plan["warnings"]["dangling_depends_on"], [])

    def test_the_expected_rules_actually_fire(self):
        _plan, report, _result = self._run()
        fired = {f["rule"] for row in report["results"] for f in row.get("findings", [])}
        for rule in ("NB11_TABLE_IS_SHORTCUT", "NB13_NO_DEFAULT_LAKEHOUSE",
                     "NB14_TABLE_INFERRED", "SQ01_PROCEDURAL", "SQ60_MONEY",
                     "SQ70_IDENTITY", "SC10_SHORTCUT_TARGET"):
            with self.subTest(rule=rule):
                self.assertIn(rule, fired)

    def test_no_sql_rule_fires_on_a_notebook(self):
        _plan, report, _result = self._run()
        for row in report["results"]:
            if row.get("kind") == "notebook":
                rules = [f["rule"] for f in row.get("findings", [])]
                self.assertFalse([r for r in rules if r.startswith("SQ")], rules)


if __name__ == "__main__":
    unittest.main()
