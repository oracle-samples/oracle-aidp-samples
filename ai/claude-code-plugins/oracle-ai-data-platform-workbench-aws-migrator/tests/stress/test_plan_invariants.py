from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aws_aidp.migrate.runner import _matches_filter
from aws_aidp.plan import build_plan, write_plan
from tests.stress.helpers import load_fixture


def manifest_with_glue_location(location: str) -> dict:
    return {
        "account_id": "offline",
        "region": "us-east-1",
        "scanned_at": "2026-01-01T00:00:00Z",
        "sources": {
            "glue": {
                "items": {
                    "databases": [],
                    "jobs": [],
                    "tables": [{
                        "database": "db",
                        "name": "table",
                        "location": location,
                        "partition_keys": [],
                        "table_type": "EXTERNAL_TABLE",
                    }],
                }
            }
        },
    }


class PlanInvariantTests(unittest.TestCase):
    def test_s3_table_location_uses_oci_uri_shape(self):
        plan = build_plan(
            manifest_with_glue_location("s3://bucket/key/"), oci_namespace="ns"
        )
        self.assertEqual(
            plan["assets"][0]["target"]["location"], "oci://bucket@ns/key/"
        )

    def test_s3a_table_location_is_supported(self):
        plan = build_plan(
            manifest_with_glue_location("s3a://bucket/key"), oci_namespace="ns"
        )
        self.assertEqual(
            plan["assets"][0]["target"]["location"], "oci://bucket@ns/key"
        )

    def test_s3_scheme_is_case_insensitive(self):
        plan = build_plan(
            manifest_with_glue_location("S3://bucket/key"), oci_namespace="ns"
        )
        self.assertEqual(plan["assets"][0]["target"]["location"], "oci://bucket@ns/key")

    def test_invalid_s3_bucket_delimiter_is_flagged_for_review(self):
        plan = build_plan(
            manifest_with_glue_location("s3://bucket@other/key"), oci_namespace="ns"
        )
        asset = plan["assets"][0]
        self.assertEqual(asset["target"]["location"], "")
        self.assertIn("review", asset["note"].lower())

    def test_invalid_s3_bucket_colon_is_flagged_for_review(self):
        plan = build_plan(
            manifest_with_glue_location("s3://bucket:443/key"), oci_namespace="ns"
        )
        self.assertEqual(plan["assets"][0]["target"]["location"], "")

    def test_non_s3_table_location_is_not_silently_erased(self):
        plan = build_plan(
            manifest_with_glue_location("hdfs://host/path"), oci_namespace="ns"
        )
        asset = plan["assets"][0]
        self.assertEqual(asset["target"]["location"], "")
        self.assertIn("review", asset["note"].lower())

    def test_plan_summary_matches_assets(self):
        manifest = manifest_with_glue_location("s3://bucket/key")
        plan = build_plan(manifest, oci_namespace="ns")
        self.assertEqual(plan["summary"]["asset_count"], len(plan["assets"]))
        self.assertEqual(
            sum(plan["summary"]["by_target_type"].values()), len(plan["assets"])
        )

    def test_unicode_names_are_preserved_in_plan(self):
        manifest = manifest_with_glue_location("s3://bucket/key")
        manifest["sources"]["glue"]["items"]["tables"][0]["name"] = "clients-é"
        plan = build_plan(manifest, oci_namespace="ns")
        self.assertEqual(plan["assets"][0]["target"]["name"], "clients-é")

    def test_duplicate_asset_ids_are_rejected(self):
        manifest = manifest_with_glue_location("s3://bucket/key")
        table = manifest["sources"]["glue"]["items"]["tables"][0]
        manifest["sources"]["glue"]["items"]["tables"].append(dict(table))
        with self.assertRaisesRegex(ValueError, "duplicate asset id"):
            build_plan(manifest, oci_namespace="ns")

    def test_unknown_manifest_source_is_rejected_instead_of_ignored(self):
        manifest = {"sources": {"gluu": {"items": {}}}}
        with self.assertRaisesRegex(ValueError, "unsupported manifest source"):
            build_plan(manifest, oci_namespace="ns")

    def test_malformed_collection_is_rejected_with_context(self):
        manifest = manifest_with_glue_location("s3://bucket/key")
        manifest["sources"]["glue"]["items"]["tables"] = {}
        with self.assertRaisesRegex(ValueError, r"glue\.items\.tables"):
            build_plan(manifest, oci_namespace="ns")

    def test_missing_identity_is_rejected_before_plan_generation(self):
        manifest = manifest_with_glue_location("s3://bucket/key")
        manifest["sources"]["glue"]["items"]["tables"][0]["name"] = ""
        with self.assertRaisesRegex(ValueError, r"tables\[0\]\.name"):
            build_plan(manifest, oci_namespace="ns")

    def test_failed_s3_scanner_empty_object_remains_plannable(self):
        manifest = {"sources": {"s3": {"summary": {"error": "denied"}, "items": {}}}}
        plan = build_plan(manifest, oci_namespace="ns")
        self.assertEqual(plan["assets"], [])

    def test_emr_notebook_execution_without_optional_name_uses_id_as_target_name(self):
        manifest = {
            "sources": {
                "emr": {
                    "items": {
                        "clusters": [],
                        "notebooks": [{"id": "exec-123", "name": None}],
                    }
                }
            }
        }
        plan = build_plan(manifest, oci_namespace="ns")
        asset = plan["assets"][0]
        self.assertIsNone(asset["source"]["name"])
        self.assertEqual(asset["target"]["name"], "exec-123")

    def test_source_filters_cover_every_source_type_emitted_by_planner(self):
        plan = build_plan(load_fixture(), oci_namespace="ns")
        for asset in plan["assets"]:
            source_name = asset["id"].split(".", 1)[0]
            source_type = asset["source"]["type"]
            with self.subTest(source=source_name, source_type=source_type):
                self.assertTrue(_matches_filter(source_type, source_name))

    def test_unsafe_oci_namespace_is_rejected(self):
        for namespace in ("a/b", "a:b", "a@b", " leading", "has'quote", "has\\slash", "UPPER"):
            with self.subTest(namespace=namespace), self.assertRaisesRegex(ValueError, "OCI namespace"):
                build_plan(
                    manifest_with_glue_location("s3://bucket/key"),
                    oci_namespace=namespace,
                )

    def test_rapid_plans_have_distinct_ids(self):
        manifest = manifest_with_glue_location("s3://bucket/key")
        ids = {build_plan(manifest, oci_namespace="ns")["plan_id"] for _ in range(20)}
        self.assertEqual(len(ids), 20)
        self.assertTrue(all(re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{12}", value) for value in ids))

    def test_plan_write_is_atomic_and_preserves_previous_file_on_replace_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "plan.json"
            output.write_text("previous")
            with patch("pathlib.Path.replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    write_plan({"plan_id": "new"}, output)
            self.assertEqual(output.read_text(), "previous")
            self.assertEqual(list(output.parent.glob(".plan.json.*.tmp")), [])

    def test_plan_write_emits_valid_utf8_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = write_plan({"name": "clients-é"}, Path(tmp) / "plan.json")
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), {"name": "clients-é"})


if __name__ == "__main__":
    unittest.main()
