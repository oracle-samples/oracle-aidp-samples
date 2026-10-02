import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.cli import main

NB = ("# Fabric notebook source\n\n# METADATA ********************\n\n"
      "# META {\n"
      '# META   "dependencies": { "lakehouse": { "default_lakehouse_name": "Sales" } }\n'
      "# META }\n\n# CELL ********************\n\n"
      'df = spark.read.parquet("/lakehouse/default/Files/raw")\n'
      'secret = notebookutils.credentials.getSecret("kv", "k")\n')


def _export(root: Path) -> Path:
    def item(dirname, item_type, display, files=None):
        d = root / dirname
        d.mkdir(parents=True)
        (d / ".platform").write_text(json.dumps({
            "config": {"logicalId": f"id-{display}"},
            "metadata": {"type": item_type, "displayName": display}}), encoding="utf-8")
        for name, body in (files or {}).items():
            (d / name).write_text(body, encoding="utf-8")
    item("Ingest.Notebook", "Notebook", "Ingest", {"notebook-content.py": NB})
    item("AcmeDW.Warehouse", "Warehouse", "AcmeDW",
         {"claim.sql": "CREATE TABLE dbo.claim (id BIGINT)"})
    item("Sales.Lakehouse", "Lakehouse", "Sales", {
        "alm.settings.json": json.dumps({"trackedObjectTypes": ["Shortcuts"]}),
        "shortcuts.metadata.json": json.dumps([{
            "path": "Tables", "name": "claims_raw",
            "target": {"type": "AmazonS3",
                       "amazonS3": {"location": "https://a.s3.amazonaws.com",
                                    "subpath": "/c"}}}])})
    return root


def _run(argv):
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        code = main(argv)
    return code, buffer.getvalue()


class EndToEndTests(unittest.TestCase):
    def test_all_four_verbs_run_in_sequence(self):
        with TemporaryDirectory() as t:
            tmp = Path(t)
            export = _export(tmp / "AcmeWS")
            manifest, plan = tmp / "inv.json", tmp / "plan.json"
            migrated = tmp / "migrated"

            code, text = _run(["inventory", str(export), "-o", str(manifest),
                               "--namespace", "acmens"])
            self.assertEqual(code, 0, text)
            self.assertTrue(manifest.is_file())

            code, text = _run(["plan", str(manifest), "-o", str(plan),
                               "--namespace", "acmens"])
            self.assertEqual(code, 0, text)

            code, text = _run(["migrate", str(plan), "-o", str(migrated)])
            self.assertEqual(code, 0, text)

            code, text = _run(["verify", str(migrated)])
            self.assertEqual(code, 0, text)
            self.assertIn("not execution-verified", text)

            # The notebook's OneLake path was rewritten...
            artifact = next((migrated / "notebooks").glob("*.py"))
            body = artifact.read_text(encoding="utf-8")
            self.assertIn("oci://Sales@acmens/Files/raw", body)
            # ...and the secret call was left in place, flagged.
            self.assertIn("notebookutils.credentials.getSecret", body)

            report = json.loads((migrated / "report.json").read_text(encoding="utf-8"))
            notebook_row = next(r for r in report["results"]
                                if r["asset_id"] == "notebook.Ingest")
            self.assertEqual(notebook_row["status"], "needs_manual_review")
            self.assertTrue(any(f["rule"] == "NB03_NOTEBOOKUTILS"
                                for f in notebook_row["findings"]))
            self.assertTrue(any(r["kind"] == "shortcut" and r["status"] == "ok"
                                for r in report["results"]))

    def test_inventory_summary_reports_catalog_tiers_and_coverage(self):
        with TemporaryDirectory() as t:
            tmp = Path(t)
            code, text = _run(["inventory", str(_export(tmp / "WS")),
                               "-o", str(tmp / "inv.json")])
        self.assertEqual(code, 0)
        self.assertIn("warehouse_ddl", text)
        self.assertIn("shortcuts: tracked", text)

    def test_one_malformed_meta_does_not_hide_the_other_notebooks(self):
        # Measured on main: inventory printed `notebook FAILED -- malformed
        # # META JSON`, the plan held no notebooks, migrate wrote nothing and
        # verify passed an empty report -- every verb exiting 0.
        from tests.test_inventory_notebook import BAD_CELL_META, BAD_NOTEBOOK_META

        def notebook(root, name, body):
            d = root / f"{name}.Notebook"
            d.mkdir(parents=True)
            (d / ".platform").write_text(json.dumps({
                "config": {"logicalId": f"id-{name}"},
                "metadata": {"type": "Notebook", "displayName": name}}),
                encoding="utf-8")
            (d / "notebook-content.py").write_text(body, encoding="utf-8")

        with TemporaryDirectory() as t:
            tmp = Path(t)
            export = tmp / "WS"
            notebook(export, "Good_A", NB)
            notebook(export, "Good_B", NB)
            notebook(export, "Bad_Cell_Meta", BAD_CELL_META)
            notebook(export, "Bad_Nb_Meta", BAD_NOTEBOOK_META)
            manifest, plan = tmp / "inv.json", tmp / "plan.json"
            migrated = tmp / "migrated"

            code, text = _run(["inventory", str(export), "-o", str(manifest),
                               "--sources", "notebook"])
            self.assertEqual(code, 0, text)
            self.assertNotIn("FAILED", text)
            self.assertIn("notebook_count=4", text)
            self.assertIn("parse_error_count=2", text)

            self.assertEqual(_run(["plan", str(manifest), "-o", str(plan)])[0], 0)
            planned = json.loads(plan.read_text(encoding="utf-8"))
            self.assertEqual(
                sorted(a["id"] for a in planned["assets"]),
                ["notebook.Bad_Cell_Meta", "notebook.Bad_Nb_Meta",
                 "notebook.Good_A", "notebook.Good_B"])

            code, text = _run(["migrate", str(plan), "-o", str(migrated)])
            self.assertEqual(code, 0, text)
            report = json.loads((migrated / "report.json").read_text(encoding="utf-8"))
            rows = {r["asset_id"]: r for r in report["results"]}
            self.assertEqual(len(rows), 4)
            for good in ("notebook.Good_A", "notebook.Good_B"):
                self.assertNotIn("NB06_UNPARSEABLE",
                                 {f["rule"] for f in rows[good]["findings"]})
                self.assertIn("oci://Sales@", (migrated / rows[good]["output_path"])
                              .read_text(encoding="utf-8"))
            for bad in ("notebook.Bad_Cell_Meta", "notebook.Bad_Nb_Meta"):
                # The same path as any other notebook that read but would
                # not parse: REVIEW, with the reason, never a bare error.
                self.assertEqual(rows[bad]["status"], "needs_manual_review")
                rules = {f["rule"]: f["detail"] for f in rows[bad]["findings"]}
                self.assertIn("malformed # META JSON", rules["NB06_UNPARSEABLE"])
                self.assertIn("NB07_SOURCE_UNREADABLE", rules)

            code, text = _run(["verify", str(migrated)])
            self.assertIn("REVIEW: 4", text)

    def test_verify_exits_1_when_a_result_failed(self):
        with TemporaryDirectory() as t:
            tmp = Path(t)
            (tmp / "report.json").write_text(json.dumps({
                "complete": True, "counts": {"error": 1},
                "results": [{"asset_id": "a.b", "kind": "notebook",
                             "status": "error", "error": "boom"}]}), encoding="utf-8")
            code, _ = _run(["verify", str(tmp)])
        self.assertEqual(code, 1)


PIPELINE_WITH_PARAMETERS = json.dumps({"properties": {
    "parameters": {"RunDate": {"type": "string", "defaultValue": "2026-09-01"}},
    "activities": [{
        "name": "Load Day", "type": "TridentNotebook", "dependsOn": [],
        "typeProperties": {
            "notebookName": "Ingest",
            "parameters": {
                "run_date": {"value": {"value": "@pipeline().parameters.RunDate",
                                       "type": "Expression"}, "type": "string"},
                "limit": {"value": 100, "type": "int"}}}}]}})


class PipelineParameterPlumbingTests(unittest.TestCase):
    """`parameters` has to survive two hand-offs, and neither was tested.

    The inventory reads `properties.parameters` off the pipeline, the planner
    copies it into the asset's `source`, and `migrate` copies it out again
    into the dict it hands the translator. Reverting either copy left every
    unit test in the suite green: `_parameters` is called directly with a
    `parameters` argument everywhere else, so the only thing that exercises
    the two copies is a run through all three verbs.

    The failure is quiet and it is worse than a drop. With the copies gone,
    `@pipeline().parameters.RunDate` resolves against an empty mapping, so
    the translator emits PL23 saying "the pipeline declares no parameter
    'RunDate'" -- which is false; the pipeline declares it three files
    upstream -- and the value is not carried. A reader is told the source is
    at fault for a defect in the plumbing.
    """

    def _migrate(self, tmp: Path):
        export = tmp / "WS"
        notebook = export / "Ingest.Notebook"
        notebook.mkdir(parents=True)
        (notebook / ".platform").write_text(json.dumps({
            "config": {"logicalId": "id-Ingest"},
            "metadata": {"type": "Notebook", "displayName": "Ingest"}}),
            encoding="utf-8")
        (notebook / "notebook-content.py").write_text(NB, encoding="utf-8")
        item = export / "PL_Params.DataPipeline"
        item.mkdir(parents=True)
        (item / ".platform").write_text(json.dumps({
            "config": {"logicalId": "id-PL_Params"},
            "metadata": {"type": "DataPipeline", "displayName": "PL_Params"}}),
            encoding="utf-8")
        (item / "pipeline-content.json").write_text(PIPELINE_WITH_PARAMETERS,
                                                    encoding="utf-8")
        manifest, plan = tmp / "inv.json", tmp / "plan.json"
        migrated = tmp / "migrated"
        for argv in (["inventory", str(export), "-o", str(manifest)],
                     ["plan", str(manifest), "-o", str(plan),
                      "--namespace", "acmens"],
                     ["migrate", str(plan), "-o", str(migrated)]):
            code, text = _run(argv)
            self.assertEqual(code, 0, text)
        return (json.loads(plan.read_text(encoding="utf-8")),
                json.loads((migrated / "jobs" / "PL_Params.job.json")
                           .read_text(encoding="utf-8")),
                json.loads((migrated / "report.json").read_text(encoding="utf-8")))

    def test_the_plan_carries_the_pipelines_declared_parameters(self):
        """planner.py's copy. Reverting it empties this dict."""
        with TemporaryDirectory() as t:
            plan, _, _ = self._migrate(Path(t))
        asset = next(a for a in plan["assets"] if a["id"] == "pipeline.PL_Params")
        self.assertEqual(asset["source"]["parameters"],
                         {"RunDate": {"type": "string",
                                      "defaultValue": "2026-09-01"}})

    def test_the_job_carries_the_value_the_pipeline_declared(self):
        """runner.py's copy. Reverting it leaves run_date out of the job."""
        with TemporaryDirectory() as t:
            _, job, _ = self._migrate(Path(t))
        # The pipeline's one task, not the setup task every migrated job now
        # runs first (migrate/setup.py, issue #50) -- which carries no
        # parameters and is pinned on its own in tests/test_setup_catalogs.py.
        from fabric_aidp.migrate.setup import SETUP_TASK_KEY
        [task] = [t for t in job["tasks"] if t["taskKey"] != SETUP_TASK_KEY]
        self.assertEqual(task["parameters"],
                         [{"name": "run_date", "value": "2026-09-01"},
                          {"name": "limit", "value": "100"}])

    def test_the_report_does_not_blame_the_pipeline_for_a_dropped_copy(self):
        """The quiet half: with either copy gone the reason is not just
        missing, it is wrong."""
        with TemporaryDirectory() as t:
            _, _, report = self._migrate(Path(t))
        row = next(r for r in report["results"]
                   if r["asset_id"] == "pipeline.PL_Params")
        details = " ".join(f["detail"] for f in row["findings"])
        self.assertNotIn("declares no parameter", details)
        rules = [f["rule"] for f in row["findings"]]
        self.assertIn("PL22_PARAMETER_DEFAULT", rules)
        self.assertIn("PL21_NOTEBOOK_PARAMETER", rules)


if __name__ == "__main__":
    unittest.main()
