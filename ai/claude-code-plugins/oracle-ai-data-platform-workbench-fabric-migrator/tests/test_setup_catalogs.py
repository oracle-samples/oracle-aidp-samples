"""The setup step: the AIDP schemas a migration writes into, created first.

Issue #50. AIDP cannot write a table into a schema that does not exist, and
nothing created one. MEASURED on an AIDP cluster (Spark 3.5.0), 2026-09-30: the
published demo job failed at its first task with "There is no database
hive.saleslake", and ran green with the right data once the two schemas were
created by hand. So these tests pin three things: that the right schemas are in
the script, that a name AIDP refuses is left out rather than allowed to fail,
and that every job runs the setup before anything that writes.
"""
import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.cli import main
from fabric_aidp.fixtures import demo_workspace_path
from fabric_aidp.migrate import setup
from fabric_aidp.publish.publisher import PublishError, plan_publish


def _plan(*assets, catalog="default"):
    return {"target_aidp": {"namespace": "ns", "catalog": catalog},
            "assets": list(assets)}


def _lakehouse(name):
    return {"id": f"lakehouse.{name}",
            "source": {"type": "fabric_lakehouse", "name": name},
            "target": {"type": "aidp_schema", "name": name, "namespace": "ns"}}


def _table(item, table):
    return {"id": f"warehouse.{item}.table.dbo.{table}",
            "source": {"type": "fabric_warehouse_object"},
            "target": {"type": "aidp_table", "schema": item,
                       "name": f"default.{item}.{table}"}}


def _notebook(name, binding=None):
    source = {"type": "fabric_notebook", "name": name}
    if binding:
        source["default_lakehouse"] = binding
    return {"id": f"notebook.{name}", "source": source,
            "target": {"type": "aidp_notebook", "name": name}}


class WhichSchemasTests(unittest.TestCase):
    def test_a_lakehouse_a_warehouse_and_a_notebook_binding_all_count(self):
        targets = setup.schema_targets(_plan(
            _lakehouse("SalesLake"), _table("AcmeDW", "claim"),
            _notebook("n1", binding="OtherLake")))
        self.assertEqual(sorted(targets), ["AcmeDW", "OtherLake", "SalesLake"])

    def test_a_binding_counts_even_when_the_lakehouse_is_not_in_the_export(self):
        """A write like `saveAsTable("x")` lands in the bound Lakehouse's
        schema whether or not that Lakehouse was exported. In the bundled
        estate, notebooks are bound to `SomeLakehouse` and `casadinpadure`,
        neither of which is an item in it."""
        plan = setup.setup_plan(_plan(_notebook("n", binding="SomeLakehouse")))
        self.assertEqual(plan["schemas"], ["SomeLakehouse"])

    def test_an_unbound_notebook_adds_nothing(self):
        self.assertEqual(setup.schema_targets(_plan(_notebook("n"))), {})

    def test_spellings_that_differ_only_in_case_are_one_schema(self):
        """AIDP folds case: `default.R2MixedCase` is stored as `r2mixedcase`
        and a second CREATE of the other spelling would be the same schema."""
        plan = setup.setup_plan(_plan(_lakehouse("SalesLake"),
                                      _notebook("n", binding="saleslake")))
        self.assertEqual(len(plan["schemas"]), 1)


class RefusedNamesTests(unittest.TestCase):
    """AIDP's metastore accepts only [a-z0-9_] after folding case. MEASURED:
    CREATE SCHEMA IF NOT EXISTS default.`fabric-data-engineering-ws_on-prem-
    warehouse-test-wh` raises MetaException, back-quoted or not."""

    def test_a_hyphenated_schema_is_left_out_and_listed(self):
        plan = setup.setup_plan(_plan(_lakehouse("SalesLake"),
                                      _table("on-prem-wh", "orders")))
        self.assertEqual(plan["schemas"], ["SalesLake"])
        self.assertEqual([r["schema"] for r in plan["refused"]], ["on-prem-wh"])
        self.assertFalse(any("on-prem-wh" in s for s in plan["statements"]))

    def test_the_refusal_says_where_the_name_came_from(self):
        plan = setup.setup_plan(_plan(_table("on-prem-wh", "orders")))
        self.assertEqual(plan["refused"][0]["from"],
                         ["warehouse.on-prem-wh.table.dbo.orders"])

    def test_only_refused_names_means_nothing_to_run(self):
        plan = setup.setup_plan(_plan(_table("on-prem-wh", "orders")))
        self.assertEqual(plan["statements"], [])


class StatementsTests(unittest.TestCase):
    def test_every_statement_is_if_not_exists(self):
        """Idempotent, so a job that runs it on every run changes nothing
        after the first. MEASURED on AIDP: the same CREATE SCHEMA IF NOT
        EXISTS twice is OK both times."""
        plan = setup.setup_plan(_plan(_lakehouse("A"), _table("B", "t")),
                                "mycat")
        self.assertTrue(plan["statements"])
        for statement in plan["statements"]:
            with self.subTest(statement=statement):
                self.assertIn("IF NOT EXISTS", statement)

    def test_the_default_catalog_is_not_created(self):
        """The demo job ran after only its two schemas were created, so
        `default` already exists on the cluster."""
        plan = setup.setup_plan(_plan(_lakehouse("A")))
        self.assertEqual(plan["statements"], ["CREATE SCHEMA IF NOT EXISTS default.A"])

    def test_another_catalog_is_created_first(self):
        plan = setup.setup_plan(_plan(_lakehouse("A")), "fabr2cat")
        self.assertEqual(plan["statements"],
                         ["CREATE CATALOG IF NOT EXISTS fabr2cat",
                          "CREATE SCHEMA IF NOT EXISTS fabr2cat.A"])

    def test_nothing_to_create_writes_nothing(self):
        with TemporaryDirectory() as tmp:
            entry = setup.write_setup(tmp, setup.setup_plan(_plan()))
            self.assertEqual(entry["statements"], 0)
            self.assertIsNone(entry["notebook"])
            self.assertFalse((Path(tmp) / "setup").exists())


class TheNotebookTests(unittest.TestCase):
    """The notebook runs every statement and fails at the end, so one
    unexpected failure cannot leave the schemas after it uncreated."""

    def _code(self):
        with TemporaryDirectory() as tmp:
            setup.write_setup(tmp, setup.setup_plan(_plan(
                _lakehouse("A"), _lakehouse("B"), _lakehouse("C"))))
            text = (Path(tmp) / setup.SETUP_NOTEBOOK).read_text(encoding="utf-8")
        return text.split("# CELL ********************", 1)[1]

    def _run(self, fail_on=None):
        ran = []

        class Spark:
            def sql(self, statement):
                ran.append(statement)
                if fail_on and fail_on in statement:
                    raise ValueError("refused")

        namespace = {"spark": Spark()}
        with redirect_stdout(io.StringIO()):
            try:
                exec(compile(self._code(), "setup", "exec"), namespace)
            except RuntimeError as exc:
                return ran, str(exc)
        return ran, None

    def test_it_runs_every_statement(self):
        ran, error = self._run()
        self.assertEqual(len(ran), 3)
        self.assertIsNone(error)

    def test_a_failure_does_not_stop_the_statements_after_it(self):
        ran, error = self._run(fail_on="default.A")
        self.assertEqual(len(ran), 3)
        self.assertIsNotNone(error)
        self.assertIn("default.A", error)
        self.assertIn("1 of 3", error)

    def test_it_converts_for_publish(self):
        from fabric_aidp.publish.to_ipynb import to_ipynb
        with TemporaryDirectory() as tmp:
            setup.write_setup(tmp, setup.setup_plan(_plan(_lakehouse("A"))))
            text = (Path(tmp) / setup.SETUP_NOTEBOOK).read_text(encoding="utf-8")
        cells = json.loads(to_ipynb(text))["cells"]
        self.assertEqual([c["cell_type"] for c in cells], ["markdown", "code"])


class SetupTaskTests(unittest.TestCase):
    JOB = json.dumps({"name": "J", "tasks": [
        {"taskKey": "a", "notebookPath": "/Workspace/a.py"},
        {"taskKey": "b", "notebookPath": "/Workspace/b.py",
         "dependsOn": [{"taskKey": "a"}]},
        {"taskKey": "c", "notebookPath": "/Workspace/c.py"},
    ]}, indent=2) + "\n"

    def _tasks(self, job=None):
        return json.loads(setup.add_setup_task(job or self.JOB,
                                               "/Workspace/00_setup_catalogs.py"))["tasks"]

    def test_the_setup_task_is_first(self):
        first = self._tasks()[0]
        self.assertEqual(first["taskKey"], "setup_catalogs")
        self.assertEqual(first["notebookPath"], "/Workspace/00_setup_catalogs.py")
        self.assertNotIn("dependsOn", first)

    def test_every_root_task_depends_on_it(self):
        tasks = {t["taskKey"]: t for t in self._tasks()}
        self.assertEqual(tasks["a"]["dependsOn"], [{"taskKey": "setup_catalogs"}])
        self.assertEqual(tasks["c"]["dependsOn"], [{"taskKey": "setup_catalogs"}])

    def test_a_task_with_dependencies_keeps_its_own(self):
        tasks = {t["taskKey"]: t for t in self._tasks()}
        self.assertEqual(tasks["b"]["dependsOn"], [{"taskKey": "a"}])

    def test_an_activity_already_called_setup_catalogs_is_not_overwritten(self):
        job = json.dumps({"tasks": [{"taskKey": "setup_catalogs"}]})
        keys = [t["taskKey"] for t in self._tasks(job)]
        self.assertEqual(keys, ["setup_catalogs_2", "setup_catalogs"])

    def test_the_file_keeps_its_trailing_newline(self):
        self.assertTrue(setup.add_setup_task(self.JOB, "/x.py").endswith("}\n"))


class TheMigrationTests(unittest.TestCase):
    """The bundled Acme estate, through inventory, plan and migrate."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        tmp = Path(cls._tmp.name)
        cls.out = tmp / "migrated"
        with redirect_stdout(io.StringIO()):
            for argv in (["inventory", str(demo_workspace_path()), "-o", str(tmp / "inv.json")],
                         ["plan", str(tmp / "inv.json"), "-o", str(tmp / "plan.json"),
                          "--namespace", "acmens"],
                         ["migrate", str(tmp / "plan.json"), "-o", str(cls.out)]):
                main(argv)
        cls.report = json.loads((cls.out / "report.json").read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_report_lists_the_setup(self):
        entry = self.report["setup"]
        self.assertEqual(entry["notebook"], setup.SETUP_NOTEBOOK)
        self.assertIn("SalesLake", entry["schemas"])
        self.assertIn("AcmeDW", entry["schemas"])

    def test_both_files_are_written(self):
        self.assertTrue((self.out / setup.SETUP_SQL).is_file())
        self.assertTrue((self.out / setup.SETUP_NOTEBOOK).is_file())

    def test_the_setup_files_are_not_reported_as_left_behind(self):
        self.assertEqual(self.report["unclaimed_artifacts"], [])

    def test_the_job_runs_setup_before_its_first_write(self):
        job = json.loads((self.out / "jobs" / "Daily_Claims.job.json")
                         .read_text(encoding="utf-8"))
        tasks = job["tasks"]
        self.assertEqual(tasks[0]["notebookPath"], "/Workspace/00_setup_catalogs.py")
        self.assertEqual(tasks[1]["dependsOn"], [{"taskKey": tasks[0]["taskKey"]}])

    def test_a_lakehouse_is_no_longer_skipped(self):
        """It was `planned -- no translator`, shown as SKIP and explained as
        expected: it hid the very step a first run fails on."""
        rows = {r["asset_id"]: r for r in self.report["results"]}
        row = rows["lakehouse.SalesLake"]
        self.assertEqual(row["status"], "ok")
        self.assertEqual([f["rule"] for f in row["findings"]], ["LH01_SCHEMA_IN_SETUP"])

    def test_report_md_says_setup_runs_first(self):
        self.assertIn("Setup runs first",
                      (self.out / "report.md").read_text(encoding="utf-8"))


class PublishTests(unittest.TestCase):
    """`publish` uploads the setup notebook first, and the job's setup task
    resolves to it through the same lookup every other task uses."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        tmp = Path(cls._tmp.name)
        cls.out = tmp / "migrated"
        with redirect_stdout(io.StringIO()):
            for argv in (["inventory", str(demo_workspace_path()), "-o", str(tmp / "inv.json")],
                         ["plan", str(tmp / "inv.json"), "-o", str(tmp / "plan.json"),
                          "--namespace", "acmens"],
                         ["migrate", str(tmp / "plan.json"), "-o", str(cls.out)]):
                main(argv)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_setup_notebook_is_uploaded_first(self):
        plan = plan_publish(self.out, prefix="me", cluster_key="ck")
        self.assertEqual(plan["notebooks"][0]["remote"],
                         "/Workspace/me/00_setup_catalogs.ipynb")

    def test_the_jobs_setup_task_points_at_the_uploaded_notebook(self):
        plan = plan_publish(self.out, prefix="me", cluster_key="ck")
        self.assertEqual(plan["blocked"], [])
        first = plan["jobs"][0]["definition"]["tasks"][0]
        self.assertEqual(first["notebookPath"], "/Workspace/me/00_setup_catalogs.ipynb")

    def test_a_notebook_with_the_setup_notebooks_name_is_refused(self):
        """Two notebooks on one remote path: every job's first task would
        run whichever was uploaded last."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = json.loads((self.out / "report.json").read_text(encoding="utf-8"))
            (out / "setup").mkdir()
            (out / setup.SETUP_NOTEBOOK).write_text(
                (self.out / setup.SETUP_NOTEBOOK).read_text(encoding="utf-8"))
            (out / "notebooks").mkdir()
            (out / "notebooks" / "00_setup_catalogs.py").write_text(
                "# Fabric notebook source\n\n# CELL ********************\n\nx = 1\n")
            report["results"] = [{"asset_id": "notebook.00_setup_catalogs",
                                  "kind": "notebook", "status": "ok",
                                  "output_path": "notebooks/00_setup_catalogs.py"}]
            (out / "report.json").write_text(json.dumps(report))
            with self.assertRaises(PublishError):
                plan_publish(out, prefix="me", cluster_key="ck")

    def test_a_report_with_no_setup_publishes_as_before(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = json.loads((self.out / "report.json").read_text(encoding="utf-8"))
            report.pop("setup")
            report["results"] = []
            (out / "report.json").write_text(json.dumps(report))
            self.assertEqual(plan_publish(out, prefix="me")["notebooks"], [])


if __name__ == "__main__":
    unittest.main()
