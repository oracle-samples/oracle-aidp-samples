import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory import pipeline as pl
from fabric_aidp.inventory import semanticmodel as sm
from fabric_aidp.inventory.git_workspace import discover_items

PIPELINE = {
    "properties": {"activities": [
        {"name": "RunIngest", "type": "TridentNotebook",
         "typeProperties": {"notebookId": "nb-1", "notebookName": "Ingest_Claims"}},
        {"name": "CopyRaw", "type": "Copy", "typeProperties": {}},
    ]}
}


def _item(root: Path, dirname: str, item_type: str, display: str, files=None) -> Path:
    d = root / dirname
    d.mkdir(parents=True)
    (d / ".platform").write_text(json.dumps({
        "config": {"logicalId": f"id-{display}"},
        "metadata": {"type": item_type, "displayName": display, "description": "desc"},
    }), encoding="utf-8")
    for name, body in (files or {}).items():
        (d / name).write_text(body, encoding="utf-8")
    return d


class PipelineTests(unittest.TestCase):
    def _scan(self, body, name="pipeline-content.json"):
        with TemporaryDirectory() as t:
            root = Path(t)
            _item(root, "Daily.DataPipeline", "DataPipeline", "Daily", {name: body})
            return pl.scan(discover_items(root))

    def test_activities_are_listed_with_name_and_type(self):
        out = self._scan(json.dumps(PIPELINE))
        activities = out["items"]["pipelines"][0]["activities"]
        self.assertEqual([a["name"] for a in activities], ["RunIngest", "CopyRaw"])
        self.assertEqual(activities[0]["type"], "TridentNotebook")

    def test_notebook_reference_is_captured_as_an_edge(self):
        out = self._scan(json.dumps(PIPELINE))
        self.assertEqual(out["items"]["pipelines"][0]["notebook_refs"], ["Ingest_Claims"])

    def test_non_notebook_activity_contributes_no_edge(self):
        body = json.dumps({"properties": {"activities": [
            {"name": "CopyRaw", "type": "Copy", "typeProperties": {}}]}})
        self.assertEqual(self._scan(body)["items"]["pipelines"][0]["notebook_refs"], [])

    def test_notebook_id_resolves_through_the_exports_logical_ids(self):
        # The real Git-export shape: no notebookName, and a same-workspace
        # notebook is referenced by its logicalId with an all-zero workspace.
        body = json.dumps({"properties": {"activities": [
            {"name": "Run", "type": "TridentNotebook", "typeProperties": {
                "notebookId": "ID-INGEST",
                "workspaceId": "00000000-0000-0000-0000-000000000000"}}]}})
        with TemporaryDirectory() as t:
            root = Path(t)
            _item(root, "Ingest.Notebook", "Notebook", "Ingest")
            _item(root, "Daily.DataPipeline", "DataPipeline", "Daily",
                  {"pipeline-content.json": body})
            record = pl.scan(discover_items(root))["items"]["pipelines"][0]
        self.assertEqual(record["notebook_refs"], ["Ingest"])
        self.assertEqual(record["activities"][0]["notebook"], "Ingest")

    def test_notebook_id_not_in_the_export_stays_the_raw_id(self):
        body = json.dumps({"properties": {"activities": [
            {"name": "Run", "type": "TridentNotebook", "typeProperties": {
                "notebookId": "0c96d0e9-ea78-4c25-848a-8c581c9c7b24",
                "workspaceId": "57bceddc-a995-44a7-bfb5-1d5f11ad1e98"}}]}})
        self.assertEqual(self._scan(body)["items"]["pipelines"][0]["notebook_refs"],
                         ["0c96d0e9-ea78-4c25-848a-8c581c9c7b24"])

    def test_conditions_policy_and_state_are_kept(self):
        body = json.dumps({"properties": {"activities": [
            {"name": "A", "type": "TridentNotebook", "typeProperties": {"notebookName": "Nb"}},
            {"name": "Alert", "type": "TridentNotebook", "state": "Inactive",
             "onInactiveMarkAs": "Skipped",
             "policy": {"timeout": "0.01:00:00", "retry": 1, "secureOutput": False},
             "dependsOn": [{"activity": "A", "dependencyConditions": ["Failed"]}],
             "typeProperties": {"notebookName": "Nb"}}]}})
        alert = self._scan(body)["items"]["pipelines"][0]["activities"][1]
        self.assertEqual(alert["depends_on_conditions"], {"A": ["Failed"]})
        self.assertEqual(alert["policy"], {"timeout": "0.01:00:00", "retry": 1})
        self.assertEqual((alert["state"], alert["on_inactive"]), ("Inactive", "Skipped"))

    def test_two_entries_for_one_upstream_are_one_edge(self):
        # PL_DupEdge: B wired to A on success and on failure, as two entries.
        # The second overwrote the first ({"A": ["Failed"]}) and A was listed
        # twice.
        body = json.dumps({"properties": {"activities": [
            {"name": "A", "type": "TridentNotebook", "typeProperties": {"notebookName": "Nb"}},
            {"name": "B", "type": "TridentNotebook", "typeProperties": {"notebookName": "Nb"},
             "dependsOn": [{"activity": "A", "dependencyConditions": ["Succeeded"]},
                           {"activity": "A", "dependencyConditions": ["Failed"]}]}]}})
        b = self._scan(body)["items"]["pipelines"][0]["activities"][1]
        self.assertEqual(b["depends_on"], ["A"])
        self.assertEqual(b["depends_on_conditions"], {"A": ["Failed", "Succeeded"]})

    def test_a_repeated_entry_without_conditions_counts_as_succeeded(self):
        body = json.dumps({"properties": {"activities": [
            {"name": "A", "type": "TridentNotebook", "typeProperties": {"notebookName": "Nb"}},
            {"name": "B", "type": "TridentNotebook", "typeProperties": {"notebookName": "Nb"},
             "dependsOn": [{"activity": "A"},
                           {"activity": "A", "dependencyConditions": ["Failed"]}]},
            {"name": "C", "type": "TridentNotebook", "typeProperties": {"notebookName": "Nb"},
             "dependsOn": [{"activity": "A"}]}]}})
        acts = self._scan(body)["items"]["pipelines"][0]["activities"]
        self.assertEqual(acts[1]["depends_on_conditions"], {"A": ["Failed", "Succeeded"]})
        # A single entry is kept as written.
        self.assertEqual(acts[2]["depends_on_conditions"], {"A": []})

    def test_top_level_activities_form_is_accepted(self):
        body = json.dumps({"activities": [
            {"name": "A", "type": "TridentNotebook",
             "typeProperties": {"notebookName": "Nb"}}]})
        self.assertEqual(self._scan(body)["items"]["pipelines"][0]["notebook_refs"], ["Nb"])

    def test_activity_count_is_summarised(self):
        self.assertEqual(self._scan(json.dumps(PIPELINE))["summary"]["activity_count"], 2)

    def test_malformed_pipeline_json_is_recorded_not_fatal(self):
        out = self._scan("{not json")
        record = out["items"]["pipelines"][0]
        self.assertEqual(record["activities"], [])
        self.assertIn("content_error", record)

    def test_missing_content_file_is_recorded(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _item(root, "Empty.DataPipeline", "DataPipeline", "Empty")
            out = pl.scan(discover_items(root))
        self.assertIn("content_error", out["items"]["pipelines"][0])

    def _scan_schedules(self, schedules=None):
        files = {"pipeline-content.json": json.dumps(PIPELINE)}
        if schedules is not None:
            files[".schedules"] = schedules
        with TemporaryDirectory() as t:
            root = Path(t)
            _item(root, "Daily.DataPipeline", "DataPipeline", "Daily", files)
            return pl.scan(discover_items(root))["items"]["pipelines"][0]

    def test_schedules_are_kept(self):
        # The probe workspace's PL_Params/.schedules, as Fabric Git writes it.
        record = self._scan_schedules(json.dumps({"schedules": [{
            "enabled": True, "jobType": "Pipeline", "configuration": {
                "type": "Cron", "startDateTime": "2026-01-01T02:00:00", "interval": 60,
                "endDateTime": "2027-01-01T00:00:00", "localTimeZoneId": "UTC"}}]}))
        self.assertEqual(record["schedules"], [{
            "enabled": True, "job_type": "Pipeline", "configuration": {
                "type": "Cron", "startDateTime": "2026-01-01T02:00:00", "interval": 60,
                "endDateTime": "2027-01-01T00:00:00", "localTimeZoneId": "UTC"}}])
        self.assertNotIn("schedules_error", record)
        self.assertEqual(record["notebook_refs"], ["Ingest_Claims"])

    def test_no_schedules_file_is_no_schedule(self):
        record = self._scan_schedules()
        self.assertEqual(record["schedules"], [])
        self.assertNotIn("schedules_error", record)

    def test_a_malformed_schedules_file_is_recorded_not_fatal(self):
        for body in ("{not json", json.dumps({"schedules": "daily"}), json.dumps([1])):
            record = self._scan_schedules(body)
            self.assertEqual(record["schedules"], [])
            self.assertIn(".schedules", record["schedules_error"])
            # The activities are still read.
            self.assertEqual(len(record["activities"]), 2)

    def test_a_non_object_schedule_entry_is_counted(self):
        record = self._scan_schedules(json.dumps({"schedules": [
            "x", {"enabled": False, "configuration": {"type": "Daily"}}]}))
        self.assertEqual([s["enabled"] for s in record["schedules"]], [False])
        self.assertIn("1 schedule entry is not an object", record["schedules_error"])

    def test_schedules_are_read_even_when_the_content_is_not(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _item(root, "Daily.DataPipeline", "DataPipeline", "Daily", {
                ".schedules": json.dumps({"schedules": [{"enabled": True,
                                                         "configuration": {}}]})})
            record = pl.scan(discover_items(root))["items"]["pipelines"][0]
        self.assertIn("content_error", record)
        self.assertEqual(len(record["schedules"]), 1)

    def test_no_pipelines_yields_an_empty_but_valid_shape(self):
        self.assertEqual(pl.scan([]), {
            "summary": {"pipeline_count": 0, "activity_count": 0},
            "items": {"pipelines": []},
        })


class SemanticModelTests(unittest.TestCase):
    def test_name_and_description_are_captured(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _item(root, "Sales.SemanticModel", "SemanticModel", "Sales")
            out = sm.scan(discover_items(root))
        record = out["items"]["semantic_models"][0]
        self.assertEqual(record["name"], "Sales")
        self.assertEqual(record["description"], "desc")
        self.assertEqual(out["summary"]["semantic_model_count"], 1)

    def test_no_models_yields_an_empty_but_valid_shape(self):
        self.assertEqual(sm.scan([]), {
            "summary": {"semantic_model_count": 0},
            "items": {"semantic_models": []},
        })


if __name__ == "__main__":
    unittest.main()
