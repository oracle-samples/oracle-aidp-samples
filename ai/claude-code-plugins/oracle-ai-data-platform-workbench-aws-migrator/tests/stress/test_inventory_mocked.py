from __future__ import annotations

import io
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from aws_aidp.inventory import athena, emr, glue, s3, sagemaker
from aws_aidp.inventory import manifest as manifest_module
from aws_aidp.inventory.manifest import build_manifest, summarize, write_manifest


class Pages:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kwargs):
        return iter(self.pages)


class UncappedPages(Pages):
    def paginate(self, **kwargs):
        if "PaginationConfig" in kwargs:
            raise AssertionError("inventory must not silently cap paginated results")
        return super().paginate(**kwargs)


class ServiceClient:
    def __init__(self, services):
        self.services = services

    def client(self, name):
        value = self.services[name]
        if isinstance(value, Exception):
            raise value
        return value


class AthenaApi:
    def __init__(self, count):
        self.ids = [f"q{i}" for i in range(count)]
        self.batch_sizes = []

    def list_work_groups(self, **kwargs):
        return {"WorkGroups": [{"Name": "primary"}]}

    def get_paginator(self, name):
        if name != "list_named_queries":
            raise AssertionError(name)
        midpoint = len(self.ids) // 2
        return Pages([
            {"NamedQueryIds": self.ids[:midpoint]},
            {"NamedQueryIds": self.ids[midpoint:]},
        ])

    def batch_get_named_query(self, NamedQueryIds):
        self.batch_sizes.append(len(NamedQueryIds))
        return {
            "NamedQueries": [{
                "NamedQueryId": qid,
                "Name": qid,
                "QueryString": "SELECT 1",
            } for qid in NamedQueryIds]
        }


class AthenaPartialApi(AthenaApi):
    def batch_get_named_query(self, NamedQueryIds):
        if NamedQueryIds and NamedQueryIds[0] == "q0":
            raise PermissionError("denied")
        return super().batch_get_named_query(NamedQueryIds)


class GlueApi:
    def get_paginator(self, name):
        pages = {
            "get_databases": [{"DatabaseList": [
                {"Name": "ok"}, {"Name": "denied"}
            ]}],
            "get_tables": [{"TableList": [{
                "Name": "table",
                "StorageDescriptor": {"Location": "s3://bucket/table", "Columns": []},
            }]}],
            "get_jobs": [{"Jobs": [{
                "Name": "job",
                "Command": {"Name": "glueetl", "ScriptLocation": "s3://scripts/job.py"},
            }]}],
        }
        if name == "get_tables":
            return FailingDatabasePages(pages[name])
        return Pages(pages[name])


class FailingDatabasePages(Pages):
    def paginate(self, **kwargs):
        if kwargs.get("DatabaseName") == "denied":
            raise PermissionError("denied")
        return super().paginate(**kwargs)


class S3BodyApi:
    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(b"print('ok')\xff")}


class S3Api:
    def list_buckets(self):
        return {"Buckets": [{"Name": "a"}, {"Name": "b"}]}

    def get_bucket_location(self, Bucket):
        if Bucket == "b":
            raise PermissionError("denied")
        return {"LocationConstraint": None}


class S3LegacyEuApi:
    def list_buckets(self):
        return {"Buckets": [{"Name": "legacy-eu"}]}

    def get_bucket_location(self, Bucket):
        return {"LocationConstraint": "EU"}


class EmrApi:
    def get_paginator(self, name):
        if name == "list_clusters":
            return Pages([{"Clusters": [
                {"Id": "j1", "Name": "good", "Status": {"State": "RUNNING"}},
                {"Id": "j2", "Name": "partial", "Status": {"State": "WAITING"}},
            ]}])
        if name == "list_notebook_executions":
            return Pages([{"NotebookExecutions": []}])
        raise AssertionError(name)

    def describe_cluster(self, ClusterId):
        if ClusterId == "j2":
            raise PermissionError("denied")
        return {"Cluster": {"ReleaseLabel": "emr-7", "Applications": [{"Name": "Spark"}]}}


class SageMakerApi:
    def get_paginator(self, name):
        pages = {
            "list_notebook_instances": [{"NotebookInstances": [{
                "NotebookInstanceName": "nb", "NotebookInstanceStatus": "InService"
            }]}],
            "list_training_jobs": [
                {"TrainingJobSummaries": [
                    {"TrainingJobName": f"job-{i}", "TrainingJobStatus": "Completed"}
                    for i in range(start, start + 75)
                ]}
                for start in (0, 75)
            ],
            "list_models": [{"Models": [{"ModelName": "m"}]}],
            "list_pipelines": [{"PipelineSummaries": [{"PipelineName": "p"}]}],
        }
        page_type = UncappedPages if name == "list_training_jobs" else Pages
        return page_type(pages[name])


class SageMakerPartialApi(SageMakerApi):
    def get_paginator(self, name):
        if name == "list_models":
            raise PermissionError("models denied")
        return super().get_paginator(name)


class InventoryMockTests(unittest.TestCase):
    def test_manifest_builder_rejects_unknown_sources_before_credentials(self):
        with patch.object(manifest_module, "check_creds") as credentials:
            with self.assertRaisesRegex(ValueError, "unsupported source"):
                build_manifest(object(), ("gluu",))
        credentials.assert_not_called()

    def test_manifest_builder_deduplicates_direct_source_requests(self):
        calls = []
        fake_client = types.SimpleNamespace(
            cfg=types.SimpleNamespace(region="us-east-1")
        )

        def scanner(_client):
            calls.append("s3")
            return {"summary": {"bucket_count": 0}, "items": []}

        with patch.object(manifest_module, "check_creds", return_value="account"), patch.dict(
            manifest_module._SCANNERS, {"s3": scanner}, clear=True
        ):
            result = build_manifest(fake_client, ("s3", "s3"))
        self.assertEqual(calls, ["s3"])
        self.assertEqual(result["sources_scanned"], ["s3"])

    def test_athena_batch_boundaries(self):
        for count in (0, 1, 49, 50, 51, 100):
            with self.subTest(count=count):
                api = AthenaApi(count)
                result = athena.scan(ServiceClient({"athena": api}))
                self.assertEqual(result["summary"]["named_query_count"], count)
                self.assertTrue(all(size <= 50 for size in api.batch_sizes))
                self.assertEqual(sum(api.batch_sizes), count)

    def test_s3_partial_region_failure_is_represented(self):
        result = s3.scan(ServiceClient({"s3": S3Api()}))
        self.assertEqual(result["summary"]["bucket_count"], 2)
        self.assertEqual(result["summary"]["by_region"], {"us-east-1": 1, "unknown": 1})
        self.assertEqual(result["warnings"][0]["resource"], "b")

    def test_glue_partial_database_failure_and_invalid_utf8_script(self):
        result = glue.scan(ServiceClient({"glue": GlueApi(), "s3": S3BodyApi()}))
        self.assertEqual(result["summary"]["database_count"], 2)
        self.assertEqual(result["summary"]["table_count"], 1)
        self.assertEqual(result["summary"]["job_count"], 1)
        self.assertIn("\ufffd", result["items"]["jobs"][0]["script"])
        self.assertEqual(result["warnings"][0]["resource"], "denied")
        self.assertTrue(any(
            warning["scope"] == "job_script" and "not valid UTF-8" in warning["error"]
            for warning in result["warnings"]
        ))

    def test_s3_legacy_eu_location_is_normalized_to_real_region(self):
        result = s3.scan(ServiceClient({"s3": S3LegacyEuApi()}))
        self.assertEqual(result["items"][0]["region"], "eu-west-1")
        self.assertEqual(result["summary"]["by_region"], {"eu-west-1": 1})

    def test_emr_description_failure_keeps_cluster(self):
        result = emr.scan(ServiceClient({"emr": EmrApi()}))
        self.assertEqual(result["summary"]["cluster_count"], 2)
        partial = next(x for x in result["items"]["clusters"] if x["id"] == "j2")
        self.assertEqual(partial["release_label"], "")
        self.assertEqual(result["warnings"][0]["resource"], "j2")

    def test_sagemaker_collects_all_resource_groups(self):
        result = sagemaker.scan(ServiceClient({"sagemaker": SageMakerApi()}))
        self.assertEqual(result["summary"]["notebook_count"], 1)
        self.assertEqual(result["summary"]["training_job_count"], 150)
        self.assertEqual(result["summary"]["model_count"], 1)
        self.assertEqual(result["summary"]["pipeline_count"], 1)

    def test_athena_failed_batch_is_visible(self):
        result = athena.scan(ServiceClient({"athena": AthenaPartialApi(51)}))
        self.assertEqual(result["summary"]["named_query_count"], 1)
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("50 query definition", result["warnings"][0]["error"])

    def test_sagemaker_partial_failure_is_visible(self):
        result = sagemaker.scan(ServiceClient({"sagemaker": SageMakerPartialApi()}))
        self.assertEqual(result["summary"]["model_count"], 0)
        self.assertEqual(result["warnings"], [{
            "scope": "models", "resource": "*", "error": "models denied"
        }])

    def test_manifest_summary_reports_partial_warning_count(self):
        text = summarize({
            "account_id": "offline",
            "region": "us-east-1",
            "scanned_at": "2026-01-01T00:00:00Z",
            "sources": {
                "s3": {
                    "summary": {"bucket_count": 1},
                    "warnings": [{"scope": "bucket_region", "resource": "b", "error": "denied"}],
                }
            },
        })
        self.assertIn("warnings=1", text)

    def test_manifest_write_is_atomic_and_preserves_previous_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "inventory.json"
            output.write_text("previous", encoding="utf-8")
            with patch("aws_aidp.inventory.manifest.os.replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    write_manifest({"account_id": "new"}, output)
            self.assertEqual(output.read_text(encoding="utf-8"), "previous")
            self.assertEqual(list(output.parent.glob(".inventory.json.*.tmp")), [])

    def test_manifest_write_emits_valid_utf8_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = write_manifest({"account_id": "clients-é"}, Path(tmp) / "inventory.json")
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")),
                {"account_id": "clients-é"},
            )


if __name__ == "__main__":
    unittest.main()
