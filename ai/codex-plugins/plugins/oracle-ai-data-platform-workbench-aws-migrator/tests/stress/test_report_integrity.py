from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aws_aidp.migrate import migrate
from aws_aidp.migrate import runner
from aws_aidp.verify import verify
from tests.stress.helpers import athena_asset, glue_asset, plan_with

_STATUSES = ("ok", "needs_manual_review", "planned", "skipped", "error")


def complete_report(results):
    counts = {status: 0 for status in _STATUSES}
    for row in results:
        if isinstance(row, dict) and isinstance(row.get("status"), str):
            if row["status"] in counts:
                counts[row["status"]] += 1
    return {"complete": True, "counts": counts, "results": results}


class ReportIntegrityTests(unittest.TestCase):
    def test_artifact_name_cannot_escape_output_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            report = migrate(
                plan_with(athena_asset(name="../../escaped")),
                out_dir=out,
                filter_kind="athena",
                demo=True,
            )
            result_path = (out / report["results"][0]["output_path"]).resolve()
            self.assertEqual(os.path.commonpath([str(result_path), str(out.resolve())]), str(out.resolve()))
            self.assertFalse((Path(tmp) / "escaped.spark.sql").exists())

    def test_duplicate_names_do_not_overwrite(self):
        first = athena_asset(asset_id="athena.query.q1", name="same", query="SELECT 1")
        second = athena_asset(asset_id="athena.query.q2", name="same", query="SELECT 2")
        with tempfile.TemporaryDirectory() as tmp:
            report = migrate(
                plan_with(first, second), out_dir=Path(tmp), filter_kind="athena", demo=True
            )
            paths = [r["output_path"] for r in report["results"]]
            self.assertEqual(len(set(paths)), 2)
            self.assertEqual({(Path(tmp) / p).read_text().splitlines()[-1] for p in paths}, {
                "SELECT 1", "SELECT 2"
            })

    def test_casefolded_names_do_not_collide_on_common_filesystems(self):
        first = athena_asset(asset_id="athena.query.q1", name="Sales", query="SELECT 1")
        second = athena_asset(asset_id="athena.query.q2", name="sales", query="SELECT 2")
        with tempfile.TemporaryDirectory() as tmp:
            report = migrate(plan_with(first, second), out_dir=Path(tmp), demo=True)
            paths = [row["output_path"] for row in report["results"]]
            self.assertEqual(len(set(paths)), 2)
            self.assertNotEqual(paths[0].casefold(), paths[1].casefold())

    def test_symlinked_artifact_directory_cannot_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "out"
            outside = root / "outside"
            out.mkdir()
            outside.mkdir()
            try:
                (out / "athena").symlink_to(outside, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("directory symlinks are not available")
            report = migrate(
                plan_with(athena_asset()), out_dir=out, filter_kind="athena", demo=True
            )
            self.assertEqual(report["counts"]["error"], 1)
            self.assertEqual(list(outside.iterdir()), [])

    def test_markdown_fence_in_source_does_not_break_report(self):
        source = "SELECT '```' AS markdown_fence"
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            migrate(
                plan_with(athena_asset(query=source)), out_dir=out,
                filter_kind="athena", demo=True,
            )
            report = (out / "report.md").read_text()
            self.assertIn("````sql", report)
            self.assertEqual(report.count("````"), 4)

    def test_untrusted_metadata_cannot_inject_artifact_header_code(self):
        asset = athena_asset(asset_id="athena.query.q1", query="SELECT 1")
        asset["source"]["id"] = "q1\nDROP TABLE protected"
        asset["source"]["workgroup"] = "primary\nSELECT secret"
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = migrate(plan_with(asset), out_dir=out, demo=True)
            lines = (out / report["results"][0]["output_path"]).read_text().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertTrue(lines[0].startswith("-- migrated"))
            self.assertEqual(lines[1], "SELECT 1")

    def test_glue_metadata_cannot_inject_python_header_code(self):
        asset = glue_asset(name="job\nraise RuntimeError('injected')", script="print('safe')")
        asset["source"]["script_location"] = "s3://scripts/job.py\nraise SystemExit()"
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = migrate(plan_with(asset), out_dir=out, filter_kind="glue", demo=True)
            artifact = (out / report["results"][0]["output_path"]).read_text()
            self.assertEqual(len(artifact.splitlines()), 2)
            self.assertTrue(artifact.splitlines()[0].startswith("# migrated"))
            self.assertEqual(artifact.splitlines()[1], "print('safe')")

    def test_interrupted_report_publish_leaves_in_progress_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            real_write = runner._write_text_atomic

            def fail_json(path, value):
                if path.name == "report.json":
                    raise OSError("simulated interruption")
                return real_write(path, value)

            with patch.object(runner, "_write_text_atomic", side_effect=fail_json):
                with self.assertRaisesRegex(OSError, "simulated interruption"):
                    migrate(plan_with(athena_asset()), out_dir=out, demo=True)
            self.assertTrue((out / ".aws-aidp-migration-in-progress").exists())
            self.assertFalse((out / "report.json").exists())

    def test_interrupted_reuse_cannot_expose_old_json_as_current(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            migrate(plan_with(athena_asset(query="SELECT 'old'")), out_dir=out, demo=True)
            real_write = runner._write_text_atomic

            def fail_json(path, value):
                if path.name == "report.json":
                    raise OSError("simulated interruption")
                return real_write(path, value)

            with patch.object(runner, "_write_text_atomic", side_effect=fail_json):
                with self.assertRaisesRegex(OSError, "simulated interruption"):
                    migrate(plan_with(athena_asset(query="SELECT 'new'")), out_dir=out, demo=True)
            self.assertFalse((out / "report.json").exists())
            self.assertTrue((out / ".aws-aidp-previous-report.json").exists())
            self.assertTrue((out / ".aws-aidp-migration-in-progress").exists())

            recovered = migrate(
                plan_with(athena_asset(query="SELECT 'recovered'")),
                out_dir=out,
                demo=True,
            )
            self.assertTrue(recovered["complete"])
            self.assertFalse((out / ".aws-aidp-migration-in-progress").exists())
            self.assertFalse((out / ".aws-aidp-previous-report.json").exists())
            self.assertFalse((out / ".aws-aidp-previous-report.md").exists())

    def test_atomic_write_failure_preserves_old_file_and_cleans_temp(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "value.txt"
            target.write_text("old")
            with patch.object(runner.os, "replace", side_effect=OSError("stop")):
                with self.assertRaisesRegex(OSError, "stop"):
                    runner._write_text_atomic(target, "new")
            self.assertEqual(target.read_text(), "old")
            self.assertEqual(list(Path(tmp).glob(".*.tmp")), [])

    def test_report_counts_match_results_and_verify(self):
        assets = [
            athena_asset(asset_id="athena.query.q1", query="SELECT 1"),
            athena_asset(
                asset_id="athena.query.q2", name="review",
                query="SELECT histogram(x) FROM t",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = migrate(plan_with(*assets), out_dir=out, demo=True)
            statuses: dict[str, int] = {}
            for row in report["results"]:
                statuses[row["status"]] = statuses.get(row["status"], 0) + 1
            for status, count in report["counts"].items():
                self.assertEqual(count, statuses.get(status, 0))
            checked = verify(out / "report.json")
            self.assertEqual(sum(checked["summary"].values()), len(report["results"]))

    def test_unknown_report_status_is_fail_not_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report([
                {"asset_id": "x", "kind": "mystery", "status": "unknown"}
            ])))
            result = verify(report)
            self.assertEqual(result["summary"]["FAIL"], 1)

    def test_non_string_report_status_is_fail_not_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report([
                {"asset_id": "athena.query.q1", "status": ["ok"]}
            ])))
            result = verify(report)
            self.assertEqual(result["summary"]["FAIL"], 1)

    def test_missing_results_cannot_be_reported_as_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps({"complete": True, "counts": {}}))
            with self.assertRaisesRegex(ValueError, "results"):
                verify(report)

    def test_non_object_report_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text("[]")
            with self.assertRaisesRegex(ValueError, "JSON object"):
                verify(report)

    def test_corrupt_result_row_is_a_failure_even_with_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report(["not-an-object"])))
            result = verify(report, filter_kind="athena")
            self.assertEqual(result["summary"]["FAIL"], 1)
            self.assertIn("invalid-result", result["rows"][0]["asset_id"])

    def test_missing_asset_id_cannot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            artifact = Path(tmp) / "query.sql"
            artifact.write_text("SELECT 1")
            report.write_text(json.dumps(complete_report([
                {"kind": "athena_query", "status": "ok", "output_path": artifact.name}
            ])))
            result = verify(report, filter_kind="athena")
            self.assertEqual(result["summary"]["FAIL"], 1)
            self.assertIn("missing-asset-id", result["rows"][0]["asset_id"])

    def test_kind_prefix_supports_source_filter_when_asset_id_is_legacy(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report([
                {"asset_id": "legacy-id", "kind": "sm_model", "status": "planned"}
            ])))
            result = verify(report, filter_kind="sagemaker")
            self.assertEqual(result["summary"]["SKIP"], 1)

    def test_unknown_direct_verification_filter_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report([])))
            with self.assertRaisesRegex(ValueError, "unknown verification filter"):
                verify(report, filter_kind="nope")

    def test_incomplete_report_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            document = complete_report([])
            document["complete"] = False
            report.write_text(json.dumps(document))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                verify(report)

    def test_in_progress_marker_rejects_stale_complete_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report([])))
            (Path(tmp) / ".aws-aidp-migration-in-progress").write_text("{}")
            with self.assertRaisesRegex(ValueError, "in-progress marker"):
                verify(report)

    def test_ok_status_with_flags_is_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            artifact = Path(tmp) / "query.sql"
            artifact.write_text("SELECT 1")
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report([{
                "asset_id": "athena.query.q1",
                "kind": "athena_query",
                "status": "ok",
                "flags": 1,
                "output_path": artifact.name,
            }])))
            result = verify(report)
            self.assertEqual(result["summary"]["FAIL"], 1)
            self.assertIn("flags", result["rows"][0]["note"])

    def test_missing_success_artifact_is_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(json.dumps(complete_report([{
                "asset_id": "athena.query.q1",
                "kind": "athena_query",
                "status": "ok",
                "flags": 0,
                "output_path": "missing.sql",
            }])))
            result = verify(report)
            self.assertEqual(result["summary"]["FAIL"], 1)
            self.assertIn("missing", result["rows"][0]["note"])

    def test_success_artifact_outside_report_directory_is_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "report"
            report_dir.mkdir()
            outside = Path(tmp) / "outside.sql"
            outside.write_text("SELECT 1")
            report = report_dir / "report.json"
            report.write_text(json.dumps(complete_report([{
                "asset_id": "athena.query.q1",
                "kind": "athena_query",
                "status": "ok",
                "flags": 0,
                "output_path": str(outside),
            }])))
            result = verify(report)
            self.assertEqual(result["summary"]["FAIL"], 1)
            self.assertIn("relative", result["rows"][0]["note"])

    def test_relative_output_directory_verifies_after_working_directory_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            build_cwd = root / "build-cwd"
            verify_cwd = root / "verify-cwd"
            build_cwd.mkdir()
            verify_cwd.mkdir()
            previous_cwd = Path.cwd()
            try:
                os.chdir(build_cwd)
                report = migrate(
                    plan_with(athena_asset(query="SELECT 1")),
                    out_dir=Path("build/out"),
                    filter_kind="athena",
                    demo=True,
                )
                report_path = (build_cwd / "build/out/report.json").resolve()
                self.assertEqual(
                    Path(report["results"][0]["output_path"]).parts[0], "athena"
                )
                self.assertFalse(Path(report["results"][0]["output_path"]).is_absolute())
                os.chdir(verify_cwd)
                result = verify(report_path)
            finally:
                os.chdir(previous_cwd)
            self.assertEqual(result["summary"], {
                "PASS": 1, "REVIEW": 0, "SKIP": 0, "FAIL": 0
            })

    def test_count_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            document = complete_report([])
            document["counts"]["ok"] = 1
            report.write_text(json.dumps(document))
            with self.assertRaisesRegex(ValueError, "count mismatch"):
                verify(report)

    def test_malformed_plan_asset_becomes_reported_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = migrate(
                {"plan_id": "bad", "assets": [{"id": "broken"}]},
                out_dir=Path(tmp), demo=True,
            )
            self.assertEqual(report["counts"]["error"], 1)
            self.assertEqual(report["results"][0]["status"], "error")

    def test_malformed_plan_shape_becomes_single_reported_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = migrate(None, out_dir=Path(tmp), demo=True)  # type: ignore[arg-type]
            self.assertEqual(report["counts"]["error"], 1)
            self.assertEqual(report["results"][0]["asset_id"], "<invalid-plan>")

    def test_unknown_direct_migration_filter_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "unknown migration filter"):
                migrate(plan_with(), out_dir=Path(tmp), filter_kind="typo", demo=True)

    def test_filtered_run_does_not_mark_other_service_artifacts_stale(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            glue_dir = out / "glue"
            glue_dir.mkdir(parents=True)
            (glue_dir / "existing.py").write_text("print('keep')")
            report = migrate(
                plan_with(athena_asset()), out_dir=out, filter_kind="athena", demo=True
            )
            self.assertEqual(report["stale_artifacts"], [])

    def test_reused_output_directory_reports_stale_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            first = athena_asset(asset_id="athena.query.first", name="first")
            second = athena_asset(asset_id="athena.query.second", name="second")
            migrate(plan_with(first, second), out_dir=out, demo=True)
            report = migrate(plan_with(first), out_dir=out, demo=True)
            self.assertEqual(len(report["stale_artifacts"]), 1)
            self.assertTrue(report["stale_artifacts"][0].endswith("second.spark.sql"))
            self.assertIn("Stale artifacts", (out / "report.md").read_text())

    def test_stale_scan_does_not_follow_external_directory_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "out"
            outside = root / "outside"
            out.mkdir()
            outside.mkdir()
            (outside / "secret.spark.sql").write_text("secret")
            try:
                (out / "athena").symlink_to(outside, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("directory symlinks are not available")
            report = migrate(plan_with(), out_dir=out, demo=True)
            self.assertEqual(report["stale_artifacts"], [])

    def test_symlinked_lock_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "out"
            out.mkdir()
            outside = root / "outside-lock"
            outside.write_text("do-not-touch")
            try:
                (out / ".aws-aidp-migrate.lock").symlink_to(outside)
            except (OSError, NotImplementedError):
                self.skipTest("file symlinks are not available")
            if not hasattr(os, "O_NOFOLLOW"):
                self.skipTest("platform cannot refuse lock-file symlinks atomically")
            with self.assertRaisesRegex(ValueError, "cannot safely lock"):
                migrate(plan_with(), out_dir=out, demo=True)
            self.assertEqual(outside.read_text(), "do-not-touch")


if __name__ == "__main__":
    unittest.main()
