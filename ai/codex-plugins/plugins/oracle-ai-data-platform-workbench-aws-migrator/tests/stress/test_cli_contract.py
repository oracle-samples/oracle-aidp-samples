from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tests.stress.helpers import run_cli


class CliContractTests(unittest.TestCase):
    def assert_clean_failure(self, proc):
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn("Traceback (most recent call last)", proc.stderr)
        self.assertTrue(proc.stderr.strip())

    def test_unknown_fixture_is_clean_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_cli(
                "inventory", "--fixture", "missing-fixture", "-o",
                str(Path(tmp) / "inventory.json"),
            )
            self.assert_clean_failure(proc)

    def test_fixture_name_cannot_traverse_to_existing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_cli(
                "inventory", "--fixture", "../fixtures/demo", "-o",
                str(Path(tmp) / "inventory.json"),
            )
            self.assert_clean_failure(proc)
            self.assertIn("invalid fixture name", proc.stderr)

    def test_empty_fixture_name_does_not_fall_back_to_live_aws(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_cli(
                "inventory", "--fixture", "", "-o",
                str(Path(tmp) / "inventory.json"),
            )
            self.assert_clean_failure(proc)
            self.assertIn("invalid fixture name", proc.stderr)

    def test_unknown_source_is_rejected_even_in_fixture_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_cli(
                "inventory", "--fixture", "demo", "--sources", "glue,nope",
                "-o", str(Path(tmp) / "inventory.json"),
            )
            self.assert_clean_failure(proc)

    def test_duplicate_sources_are_scanned_once_in_requested_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "inventory.json"
            proc = run_cli(
                "inventory", "--fixture", "demo", "--sources", "ATHENA,glue,athena",
                "-o", str(output),
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            manifest = json.loads(output.read_text())
            self.assertEqual(manifest["sources_scanned"], ["athena", "glue"])
            self.assertEqual(list(manifest["sources"]), ["athena", "glue"])

    def test_unknown_migration_filter_is_rejected_without_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.json"
            plan.write_text(json.dumps({"plan_id": "p", "assets": []}))
            out = Path(tmp) / "out"
            proc = run_cli(
                "migrate", str(plan), "--demo", "--filter", "nope", "-o", str(out)
            )
            self.assert_clean_failure(proc)
            self.assertFalse((out / "report.json").exists())

    def test_unimplemented_live_migration_fails_instead_of_reporting_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.json"
            plan.write_text(json.dumps({"plan_id": "p", "assets": []}))
            out = Path(tmp) / "out"
            proc = run_cli("migrate", str(plan), "-o", str(out))
            self.assert_clean_failure(proc)
            self.assertIn("not implemented", proc.stderr)
            self.assertFalse((out / "report.json").exists())

    def test_malformed_manifest_is_clean_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "bad.json"
            manifest.write_text("{not-json")
            proc = run_cli("plan", str(manifest), "-o", str(Path(tmp) / "p.json"))
            self.assert_clean_failure(proc)

    def test_non_object_manifest_is_clean_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "bad.json"
            manifest.write_text("[]")
            proc = run_cli("plan", str(manifest), "-o", str(Path(tmp) / "p.json"))
            self.assert_clean_failure(proc)
            self.assertIn("must contain a JSON object", proc.stderr)

    def test_explicit_empty_namespace_is_clean_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(json.dumps({"sources": {}}))
            proc = run_cli(
                "plan", str(manifest), "--namespace", "", "-o", str(Path(tmp) / "p.json")
            )
            self.assert_clean_failure(proc)
            self.assertIn("namespace", proc.stderr.lower())

    def test_missing_plan_is_clean_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_cli(
                "migrate", str(Path(tmp) / "missing.json"), "--demo", "-o",
                str(Path(tmp) / "out"),
            )
            self.assert_clean_failure(proc)

    def test_non_object_plan_is_clean_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.json"
            plan.write_text("[]")
            proc = run_cli(
                "migrate", str(plan), "--demo", "-o", str(Path(tmp) / "out")
            )
            self.assert_clean_failure(proc)
            self.assertIn("must contain a JSON object", proc.stderr)

    def test_malformed_report_is_clean_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text("{}")
            proc = run_cli("verify", str(report))
            self.assert_clean_failure(proc)
            self.assertIn("incomplete", proc.stderr)

    def test_empty_manifest_produces_empty_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "empty.json"
            output = Path(tmp) / "plan.json"
            manifest.write_text(json.dumps({
                "account_id": "offline",
                "region": "us-east-1",
                "scanned_at": "2026-01-01T00:00:00Z",
                "sources": {},
            }))
            proc = run_cli("plan", str(manifest), "-o", str(output))
            self.assertEqual(proc.returncode, 0, proc.stderr)
            plan = json.loads(output.read_text())
            self.assertEqual(plan["summary"]["asset_count"], 0)
            self.assertEqual(plan["assets"], [])

    def test_cli_accepts_unicode_and_spaces_in_output_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "space dir" / "inventaire-é.json"
            proc = run_cli("inventory", "--fixture", "demo", "-o", str(output))
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertTrue(output.exists())


if __name__ == "__main__":
    unittest.main()
