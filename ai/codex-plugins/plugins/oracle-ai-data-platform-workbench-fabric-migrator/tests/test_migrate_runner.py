import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.migrate.runner import IN_PROGRESS_MARKER, migrate
from fabric_aidp.sources import ALL_SOURCES

NB = ("# Fabric notebook source\n\n# CELL ********************\n\n"
      'df = spark.read.parquet("/lakehouse/default/Files/x")\n')


def _plan(**over):
    base = {
        "plan_id": "p-1",
        "target_aidp": {"namespace": "acmens"},
        "resolved_catalog": {"summary": {}, "tables": {}},
        "assets": [
            {"id": "notebook.Ingest",
             "source": {"type": "fabric_notebook", "name": "Ingest",
                        "default_lakehouse": "Sales", "content": NB},
             "target": {"type": "aidp_notebook", "name": "Ingest"},
             "transform_chain": ["fabric_notebook_to_spark"], "depends_on": []},
            {"id": "warehouse.table.dbo.claim",
             "source": {"type": "fabric_warehouse_table", "schema": "dbo",
                        "name": "claim", "sql": "CREATE TABLE dbo.claim (id INT)"},
             "target": {"type": "aidp_table", "schema": "dbo", "name": "claim"},
             "transform_chain": ["tsql_to_spark_sql"], "depends_on": []},
            {"id": "pipeline.Daily",
             "source": {"type": "fabric_pipeline", "name": "Daily",
                        "activities": [
                            {"name": "Copy in", "type": "Copy",
                             "notebook": "", "depends_on": []}]},
             "target": {"type": "aidp_job", "name": "Daily"},
             "transform_chain": ["pipeline_to_aidp_job"], "depends_on": []},
            {"id": "semanticmodel.Sales",
             "source": {"type": "fabric_semantic_model", "name": "Sales"},
             "target": {"type": "aidp_semantic_model", "name": "Sales"},
             "transform_chain": [], "depends_on": []},
        ],
    }
    base.update(over)
    return base


class ModeTests(unittest.TestCase):
    def test_migrate_writes_locally_without_any_mode_flag(self):
        """`--demo` used to be required, and its absence printed "live AIDP
        migration is not implemented" -- true before `publish` existed, and
        misleading afterwards. migrate always writes locally now, and the
        flag has been removed rather than kept as dead surface; see
        tests/test_cli_contract.py::DeadDemoFlagIsGoneTests for the trade."""
        with TemporaryDirectory() as t:
            report = migrate(_plan(), out_dir=Path(t))
        self.assertIn("results", report)
        self.assertTrue(report["results"])

    def test_passing_demo_is_now_a_TypeError_rather_than_a_no_op(self):
        """It silently did nothing, which is worse than refusing: a caller
        who passed it believed it selected a mode."""
        with TemporaryDirectory() as t:
            with self.assertRaises(TypeError):
                migrate(_plan(), out_dir=Path(t), demo=True)


    def test_unknown_filter_is_rejected(self):
        with TemporaryDirectory() as t:
            with self.assertRaises(ValueError):
                migrate(_plan(), out_dir=Path(t), filter_kind="nope")


class DemoRunTests(unittest.TestCase):
    def _run(self, plan=None, **kw):
        self.tmp = TemporaryDirectory()
        out = Path(self.tmp.name)
        return migrate(plan or _plan(), out_dir=out, **kw), out

    def tearDown(self):
        if hasattr(self, "tmp"):
            self.tmp.cleanup()

    def test_notebook_artifact_is_written(self):
        report, out = self._run()
        result = next(r for r in report["results"] if r["asset_id"] == "notebook.Ingest")
        artifact = out / result["output_path"]
        self.assertTrue(artifact.is_file())
        self.assertIn("oci://Sales@acmens/Files/x", artifact.read_text(encoding="utf-8"))

    def test_output_path_is_relative_to_the_report(self):
        report, _ = self._run()
        for result in report["results"]:
            if "output_path" in result:
                self.assertFalse(Path(result["output_path"]).is_absolute())

    def test_clean_warehouse_object_is_ok(self):
        report, _ = self._run()
        result = next(r for r in report["results"]
                      if r["asset_id"] == "warehouse.table.dbo.claim")
        self.assertEqual(result["status"], "ok")

    def test_untranslatable_asset_is_planned(self):
        """An asset type with no translator at all. Pipelines used to be the
        example here; they have one now, so a semantic model carries it."""
        report, _ = self._run()
        result = next(r for r in report["results"]
                      if r["asset_id"] == "semanticmodel.Sales")
        self.assertEqual(result["status"], "planned")

    def test_a_pipeline_with_a_non_notebook_activity_is_blocked(self):
        """Blocked, not partially emitted: a job holding only the notebook
        tasks would run and skip the Copy that fed them."""
        report, _ = self._run()
        result = next(r for r in report["results"] if r["asset_id"] == "pipeline.Daily")
        self.assertEqual(result["status"], "blocked")
        self.assertIn("Copy", str(result["findings"]))

    def test_a_pipeline_schedule_reaches_the_report(self):
        # The plan carries `.schedules` to the translator; before, it never
        # left the item directory.
        plan = _plan()
        pipeline = next(a for a in plan["assets"] if a["id"] == "pipeline.Daily")
        pipeline["source"]["schedules"] = [{"enabled": True, "configuration": {
            "type": "Cron", "interval": 60, "localTimeZoneId": "UTC"}}]
        report, _ = self._run(plan)
        result = next(r for r in report["results"] if r["asset_id"] == "pipeline.Daily")
        self.assertIn("PL20_SCHEDULE", [f["rule"] for f in result["findings"]])

    def test_a_blocked_pipeline_writes_no_job_file(self):
        report, out = self._run()
        self.assertEqual(list((out / "jobs").glob("*")) if (out / "jobs").is_dir() else [],
                         [])

    def test_counts_reconcile_with_results(self):
        report, _ = self._run()
        for status, count in report["counts"].items():
            self.assertEqual(
                count, sum(1 for r in report["results"] if r["status"] == status))

    def test_reports_are_written_and_marker_removed(self):
        _, out = self._run()
        self.assertTrue((out / "report.json").is_file())
        self.assertTrue((out / "report.md").is_file())
        self.assertFalse((out / IN_PROGRESS_MARKER).exists())

    def test_report_is_marked_complete(self):
        report, out = self._run()
        self.assertTrue(report["complete"])
        self.assertTrue(json.loads((out / "report.json").read_text())["complete"])

    def test_filter_limits_the_slice(self):
        report, _ = self._run(filter_kind="notebook")
        self.assertEqual([r["asset_id"] for r in report["results"]], ["notebook.Ingest"])

    def test_markdown_states_the_pass_limitation(self):
        _, out = self._run()
        text = (out / "report.md").read_text(encoding="utf-8")
        self.assertIn("not execution-verified", text)

    def test_inferred_table_produces_a_rewrite_finding(self):
        plan = _plan()
        plan["resolved_catalog"] = {"summary": {}, "tables": {
            "claims_agg": {"tier": "notebook_inferred", "created_by": "Build_Aggregates",
                           "name": "claims_agg"}}}
        plan["assets"][0]["source"]["content"] = (
            "# Fabric notebook source\n\n# CELL ********************\n\n"
            'df = spark.table("claims_agg")\n')
        report, _ = self._run(plan)
        result = next(r for r in report["results"] if r["asset_id"] == "notebook.Ingest")
        rules = [f["rule"] for f in result["findings"]]
        self.assertIn("NB14_TABLE_INFERRED", rules)
        inferred = next(f for f in result["findings"] if f["rule"] == "NB14_TABLE_INFERRED")
        # `info` here meant the report counted 0 changes on a file whose
        # table name it had just rewritten.
        self.assertEqual(inferred["severity"], "rewrite")
        self.assertIn("Build_Aggregates", inferred["detail"])

    def test_an_informational_finding_does_not_force_review(self):
        plan = _plan()
        plan["resolved_catalog"] = {"summary": {}, "tables": {
            "claims_agg": {"tier": "notebook_inferred", "created_by": "B",
                           "name": "claims_agg"}}}
        plan["assets"] = [plan["assets"][0]]
        plan["assets"][0]["source"]["default_lakehouse"] = "Sales"
        plan["assets"][0]["source"]["content"] = (
            "# Fabric notebook source\n\n# CELL ********************\n\n"
            'df = spark.table("claims_agg")\n')
        report, _ = self._run(plan)
        self.assertEqual(report["results"][0]["status"], "ok")

    def test_an_unreadable_item_is_blocked_with_a_reason(self):
        plan = _plan()
        plan["assets"] = [{
            "id": "unreadable.Bad.Notebook",
            "source": {"type": "fabric_unreadable_item", "name": "Bad.Notebook",
                       "path": "Bad.Notebook",
                       "reason": ".platform is not valid JSON"},
            "target": {"type": "aidp_unmigrated", "name": "Bad.Notebook"},
            "depends_on": [],
        }]
        report, _ = self._run(plan)
        row = report["results"][0]
        self.assertEqual(row["status"], "blocked")
        rules = [f["rule"] for f in row["findings"]]
        self.assertEqual(rules, ["INV01_ITEM_UNREADABLE"])
        self.assertIn("valid JSON", row["findings"][0]["detail"])
        self.assertIn("Bad.Notebook", row["findings"][0]["detail"])

    def test_a_lakehouse_with_unreadable_shortcuts_is_blocked_with_a_reason(self):
        """`tracked (0 found)` on a file that was never read graded clean.

        Same class as INV01: the operator cannot tell "we looked and there
        are none" from "we could not look", and only one of those is safe
        to migrate on.
        """
        plan = _plan()
        plan["assets"] = [{
            "id": "lakehouse.Sales",
            "source": {"type": "fabric_lakehouse", "name": "Sales",
                       "tracking": {"shortcuts": "unknown"},
                       "shortcut_count": None,
                       "shortcuts_error": "cannot read shortcuts.metadata.json: "
                                          "Expecting value: line 1 column 2"},
            "target": {"type": "aidp_schema", "name": "sales"},
            "depends_on": [],
        }]
        report, _ = self._run(plan)
        row = report["results"][0]
        self.assertEqual(row["status"], "blocked")
        self.assertEqual([f["rule"] for f in row["findings"]],
                         ["INV03_SHORTCUTS_UNREADABLE"])
        self.assertIn("Sales", row["findings"][0]["detail"])
        self.assertIn("Expecting value", row["findings"][0]["detail"])

    def test_a_lakehouse_that_read_cleanly_is_not_blocked(self):
        plan = _plan()
        plan["assets"] = [{
            "id": "lakehouse.Sales",
            "source": {"type": "fabric_lakehouse", "name": "Sales",
                       "tracking": {"shortcuts": "tracked"}, "shortcut_count": 0},
            "target": {"type": "aidp_schema", "name": "sales"},
            "depends_on": [],
        }]
        report, _ = self._run(plan)
        self.assertNotEqual(report["results"][0]["status"], "blocked")

    def test_an_unsupported_item_type_is_blocked_with_a_reason(self):
        plan = _plan()
        plan["assets"] = [{
            "id": "unsupported.Report.SalesReport",
            "source": {"type": "fabric_unsupported_item", "name": "SalesReport",
                       "item_type": "Report", "path": "SalesReport.Report",
                       "reason": "this tool has no scanner for Fabric item "
                                 "type 'Report'"},
            "target": {"type": "aidp_unmigrated", "name": "SalesReport"},
            "depends_on": [],
        }]
        report, _ = self._run(plan)
        row = report["results"][0]
        self.assertEqual(row["status"], "blocked")
        self.assertEqual([f["rule"] for f in row["findings"]],
                         ["INV02_ITEM_TYPE_UNSUPPORTED"])
        self.assertIn("Report", row["findings"][0]["detail"])
        self.assertIn("SalesReport", row["findings"][0]["detail"])

    def test_missing_content_is_flagged_not_crashed(self):
        plan = _plan()
        plan["assets"] = [{"id": "notebook.Bad", "source": {"type": "fabric_notebook"},
                           "target": {"type": "aidp_notebook"},
                           "transform_chain": [], "depends_on": []}]
        report, _ = self._run(plan)
        row = report["results"][0]
        self.assertEqual(row["status"], "needs_manual_review")
        self.assertEqual([f["rule"] for f in row["findings"]], ["NB06_UNPARSEABLE"])

    def test_a_malformed_cell_meta_is_refused_by_rule_not_as_a_bare_error(self):
        # `language_for` decoded the bad block mid-translation and raised;
        # the per-asset catch turned it into `status=error` with no rule, and
        # the other assets were only saved by that catch. A plan built
        # before the inventory learned to spot it carries no parse_error.
        from tests.test_inventory_notebook import BAD_CELL_META
        plan = _plan()
        plan["assets"] = [
            {"id": "notebook.Bad", "source": {"type": "fabric_notebook",
                                              "name": "Bad",
                                              "content": BAD_CELL_META},
             "target": {"type": "aidp_notebook"},
             "transform_chain": [], "depends_on": []},
            {"id": "notebook.Good", "source": {
                "type": "fabric_notebook", "name": "Good",
                "content": "# Fabric notebook source\n\n# CELL "
                           + "*" * 20 + "\n\nx = 1\n"},
             "target": {"type": "aidp_notebook"},
             "transform_chain": [], "depends_on": []}]
        report, _ = self._run(plan)
        rows = {r["asset_id"]: r for r in report["results"]}
        self.assertEqual(rows["notebook.Bad"]["status"], "needs_manual_review")
        self.assertEqual([f["rule"] for f in rows["notebook.Bad"]["findings"]],
                         ["NB06_UNPARSEABLE"])
        self.assertIn("malformed # META JSON",
                      rows["notebook.Bad"]["findings"][0]["detail"])
        self.assertEqual(rows["notebook.Good"]["status"], "ok")
        self.assertNotIn("error", report["counts"])

    def test_translator_exception_becomes_an_error_row(self):
        import fabric_aidp.migrate.runner as runner_mod
        original = runner_mod.nb2spark.translate

        def boom(*args, **kwargs):
            raise RuntimeError("translator exploded")

        runner_mod.nb2spark.translate = boom
        try:
            plan = _plan()
            plan["assets"] = [plan["assets"][0]]
            report, out = self._run(plan)
        finally:
            runner_mod.nb2spark.translate = original
        row = report["results"][0]
        self.assertEqual(row["status"], "error")
        self.assertIn("translator exploded", row["error"])
        # The run still completes and the marker is still cleared.
        self.assertTrue(report["complete"])
        self.assertFalse((out / IN_PROGRESS_MARKER).exists())

    def test_artifact_names_are_filesystem_safe(self):
        plan = _plan()
        plan["assets"] = [dict(plan["assets"][0])]
        plan["assets"][0]["source"] = dict(plan["assets"][0]["source"])
        plan["assets"][0]["source"]["name"] = "bad/name:with*chars"
        report, out = self._run(plan)
        artifact = out / report["results"][0]["output_path"]
        self.assertTrue(artifact.is_file())
        self.assertNotIn("/", artifact.name)

    def test_two_assets_with_the_same_name_do_not_collide(self):
        plan = _plan()
        second = json.loads(json.dumps(plan["assets"][0]))
        second["id"] = "notebook.Ingest2"
        plan["assets"] = [plan["assets"][0], second]
        report, out = self._run(plan)
        paths = {r["output_path"] for r in report["results"]}
        self.assertEqual(len(paths), 2)


class CatalogReachesArtifactsTests(unittest.TestCase):
    """`--catalog` used to stop at the plan: targets said `myc.…` and the
    emitted SQL still said `default.…`.

    Both tests are about the AIDP catalog *name*, so both need the table
    they reference to be one the resolved table catalog knows -- a different
    thing under a similar word, and the reason the translator's parameters
    are called `catalog` and `table_catalog`.

    `_plan()` carries `{"summary": {}, "tables": {}}`, and what that means
    has since been corrected. #32 supplied the real catalog here because an
    empty one made the reference come back SQ19_TABLE_UNKNOWN and left
    unwritten, which would have read as a catalog-naming regression. That
    answer was itself the defect: an empty catalog says "this run resolved
    nothing", not "this table does not exist", and the translators now
    decline to answer rather than reporting the second (see
    `catalog.names_no_tables`; the run says it once, in `migrate`).

    So these two would now pass on `_plan()`'s empty catalog as well -- the
    name would be rewritten, unresolved. The real catalog stays, and stays
    deliberately: these tests assert that the *plan's* catalog name reaches
    the emitted SQL, and asserting it down a path where nothing was resolved
    would test the weaker of the two routes to the same string.
    """

    @staticmethod
    def _plan_with_the_table_declared():
        plan = _plan()
        plan["target_aidp"] = {"namespace": "ns", "catalog": "myc"}
        plan["assets"] = [a for a in plan["assets"]
                          if a["source"]["type"] == "fabric_warehouse_table"]
        plan["assets"][0]["source"]["sql"] = (
            "CREATE VIEW dbo.v AS SELECT * FROM SalesLake.dbo.claim")
        plan["resolved_catalog"] = {
            "summary": {}, "shortcuts": {},
            "tables": {"saleslake.dbo.claim": {
                "tier": "warehouse_ddl", "owner": "SalesLake",
                "warehouse": "SalesLake", "name": "dbo.claim"}}}
        return plan

    def test_the_plans_catalog_is_used_in_the_emitted_sql(self):
        plan = self._plan_with_the_table_declared()
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            migrate(plan, out_dir=out)
            written = next((out / "warehouse").glob("*.spark.sql")).read_text()
        self.assertIn("myc.SalesLake.claim", written)
        self.assertNotIn("default.SalesLake.claim", written)

    def test_an_explicit_catalog_override_wins_over_the_plans_own(self):
        """`migrate --catalog` re-targets an existing plan without re-planning."""
        plan = self._plan_with_the_table_declared()
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            migrate(plan, out_dir=out, catalog="override")
            written = next((out / "warehouse").glob("*.spark.sql")).read_text()
        self.assertIn("override.SalesLake.claim", written)
        self.assertNotIn("myc.SalesLake.claim", written)


class TableCatalogReachesWarehouseSqlTests(unittest.TestCase):
    """The *resolved table* catalog reaching the T-SQL translator, which is
    a different catalog from the one above despite the shared word.

    It did not reach it at all: warehouse T-SQL was rewritten on name shape
    alone, so a view over a shortcut -- data in S3, not in the lakehouse --
    got a confident three-part name and graded `ok`, where the notebook
    branch of this same loop flagged the identical reference. The plan
    already carried `resolved_catalog`; only the warehouse branch never
    read it.
    """

    CATALOG = {
        "summary": {}, "shortcuts": {},
        "tables": {"saleslake.claims_raw_s3": {
            "tier": "shortcut", "owner": "SalesLake", "lakehouse": "SalesLake",
            "name": "claims_raw_s3", "target": "s3://acme-raw/claims"}}}

    def _warehouse_only(self, sql):
        plan = _plan()
        plan["resolved_catalog"] = self.CATALOG
        plan["assets"] = [a for a in plan["assets"]
                          if a["source"]["type"] == "fabric_warehouse_table"]
        plan["assets"][0]["source"]["warehouse"] = "AcmeDW"
        plan["assets"][0]["source"]["sql"] = sql
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = migrate(plan, out_dir=out)
            written = next((out / "warehouse").glob("*.spark.sql")).read_text()
        return report["results"][0], written

    def test_a_warehouse_view_over_a_shortcut_is_not_graded_ok(self):
        row, written = self._warehouse_only(
            "CREATE VIEW dbo.v AS SELECT * FROM SalesLake.claims_raw_s3")
        self.assertEqual(row["status"], "needs_manual_review")
        self.assertIn("SQ19_TABLE_IS_SHORTCUT",
                      [f["rule"] for f in row["findings"]])
        self.assertIn("FROM SalesLake.claims_raw_s3", written)

    def test_a_warehouse_view_over_a_table_nothing_declares_is_not_graded_ok(self):
        row, written = self._warehouse_only(
            "CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all")
        self.assertEqual(row["status"], "needs_manual_review")
        self.assertIn("SQ19_TABLE_UNKNOWN",
                      [f["rule"] for f in row["findings"]])
        self.assertNotIn("default.AcmeDW.nowhere_at_all", written)


class EmptyCatalogSaysItOncePerRunTests(unittest.TestCase):
    """An empty-but-present catalog was indistinguishable from no catalog.

    MEASURED on 03f019b:

        classify_reference({"tables": {}, "shortcuts": []}, "dbo.claim", "W")
          ->  ('unknown', None, [])

    and the translators' guards tested `not catalog`, which a plan's
    `{"summary": {}, "tables": {}}` passes. So every reference in the estate
    came back `unknown` and every object collected its own "no catalog tier
    knows this table" finding.

    "This run resolved nothing" and "we resolved this and it is absent" are
    different statements, and only the first is true here. It is said once,
    at the run, which is the only scope at which it *is* true -- the same
    three places `unclaimed_artifacts` is said, and for the same reason.
    """

    NOTE = "resolved table catalog"

    def _run(self, catalog, sql="CREATE VIEW dbo.v AS "
                                "SELECT * FROM dbo.nowhere_at_all"):
        plan = _plan()
        plan["resolved_catalog"] = catalog
        plan["assets"] = [a for a in plan["assets"]
                          if a["source"]["type"] == "fabric_warehouse_table"]
        plan["assets"][0]["source"]["warehouse"] = "AcmeDW"
        plan["assets"][0]["source"]["sql"] = sql
        lines = []
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = migrate(plan, out_dir=out, log=lines.append)
            written = next((out / "warehouse").glob("*.spark.sql")).read_text()
            markdown = (out / "report.md").read_text(encoding="utf-8")
        return report, written, markdown, "\n".join(lines)

    DECLARED = {"summary": {}, "shortcuts": {},
                "tables": {"acmedw.dbo.claim": {
                    "tier": "warehouse_ddl", "owner": "AcmeDW",
                    "warehouse": "AcmeDW", "name": "dbo.claim"}}}

    # -- the per-reference finding is gone ---------------------------------

    def test_no_object_is_told_its_table_does_not_exist(self):
        report, _, _, _ = self._run({"summary": {}, "tables": {}})
        self.assertNotIn("SQ19_TABLE_UNKNOWN",
                         [f["rule"] for f in report["results"][0]["findings"]])

    def test_an_empty_catalog_behaves_exactly_as_no_catalog_does(self):
        """The invariant. A plan that resolved nothing and a plan that
        carries no catalog field at all are the same state, and used to be
        two different ones."""
        empty, _, _, _ = self._run({"summary": {}, "tables": {}})
        absent, _, _, _ = self._run({})
        self.assertEqual(
            [f["rule"] for f in empty["results"][0]["findings"]],
            [f["rule"] for f in absent["results"][0]["findings"]])

    def test_the_name_is_still_rewritten_on_shape(self):
        """Unchanged behaviour, asserted because it is the thing the fix
        must not have broken: with nothing to consult the rules rewrite on
        name shape, which is what they did before any catalog existed."""
        _, written, _, _ = self._run({"summary": {}, "tables": {}})
        self.assertIn("default.AcmeDW.nowhere_at_all", written)

    # -- and a real catalog still answers ---------------------------------

    def test_a_declared_catalog_still_declines_an_unknown_table(self):
        report, written, _, _ = self._run(self.DECLARED)
        self.assertIn("SQ19_TABLE_UNKNOWN",
                      [f["rule"] for f in report["results"][0]["findings"]])
        self.assertNotIn("default.AcmeDW.nowhere_at_all", written)

    # -- said once, per run -----------------------------------------------

    def test_the_run_says_it_once_in_all_three_places(self):
        report, _, markdown, log = self._run({"summary": {}, "tables": {}})
        self.assertTrue(report["catalog_resolved_nothing"])
        self.assertIn(self.NOTE, log)
        self.assertIn(self.NOTE, markdown)
        self.assertEqual(log.count("names no tables"), 1)
        self.assertEqual(markdown.count("names no tables"), 1)

    def test_the_note_says_it_is_not_a_claim_about_the_tables(self):
        """The whole point of moving the statement. Without this sentence a
        reader takes the absent findings for a clean bill of health."""
        _, _, markdown, log = self._run({"summary": {}, "tables": {}})
        self.assertIn("not the same as the tables being absent", log)
        self.assertIn("not a claim that the tables are absent", markdown)
        self.assertIn("--tables-csv", log)

    def test_a_declared_catalog_says_nothing_about_being_empty(self):
        report, _, markdown, log = self._run(self.DECLARED)
        self.assertFalse(report["catalog_resolved_nothing"])
        self.assertNotIn(self.NOTE, log)
        self.assertNotIn(self.NOTE, markdown)

    def test_the_key_is_in_the_report_either_way(self):
        """A reader of report.json cannot otherwise tell "no reference was
        questionable" from "nothing was checked"."""
        for catalog in ({"summary": {}, "tables": {}}, self.DECLARED):
            with self.subTest(catalog=catalog):
                report, _, _, _ = self._run(catalog)
                self.assertIn("catalog_resolved_nothing", report)

    def test_a_run_that_migrated_nothing_says_nothing(self):
        """An empty run resolved nothing trivially, and a note about it
        would be noise on every filtered run that matched no asset."""
        plan = _plan()
        plan["resolved_catalog"] = {"summary": {}, "tables": {}}
        plan["assets"] = []
        lines = []
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = migrate(plan, out_dir=out, log=lines.append)
            markdown = (out / "report.md").read_text(encoding="utf-8")
        self.assertEqual(report["results"], [])
        self.assertNotIn(self.NOTE, "\n".join(lines))
        self.assertNotIn(self.NOTE, markdown)


class NamespacePlaceholderTests(unittest.TestCase):
    """`plan` without `--namespace` writes `<your-oci-namespace>` into the
    artifacts, and every one of them used to grade `ok` with zero flags.

    Measured on the bundled estate: three artifacts carried it -- two
    lakehouse shortcut notes and one notebook -- and `migrate` reported
    `error=0  ok=24`, so a PASS asset shipped a target that can never
    resolve. The placeholder is deliberate (the RUNBOOK offers it for a
    review pass with no tenancy), so it is flagged rather than refused: the
    artifact is still written, and it is `needs_manual_review` / REVIEW
    instead of `ok` / PASS.
    """

    PLACEHOLDER = "<your-oci-namespace>"

    def _run(self, namespace):
        plan = _plan()
        plan["target_aidp"] = {"namespace": namespace, "catalog": "default"}
        plan["assets"] = [
            plan["assets"][0],
            {"id": "lakehouse.shortcut.Sales.Tables/raw",
             # `Tables`, not `Files`: this class is about the namespace
             # placeholder, and a Files shortcut now carries a finding of
             # its own (SC12_NOT_A_TABLE), which would make
             # `test_a_real_namespace_adds_no_finding` measure that
             # instead.
             "source": {"type": "fabric_shortcut", "lakehouse": "Sales",
                        "name": "raw", "section": "Tables",
                        "target_type": "AmazonS3", "target": "s3://acme-raw/claims",
                        "external": True},
             "target": {"type": "aidp_dcat_external_table", "schema": "Sales",
                        "name": "raw"},
             "transform_chain": [], "depends_on": []},
        ]
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        return migrate(plan, out_dir=out), out

    def test_the_placeholder_really_does_reach_the_artifact(self):
        """Guards the test above it: if the placeholder stopped being emitted
        the flag test would pass for the wrong reason."""
        _, out = self._run(self.PLACEHOLDER)
        written = "".join(p.read_text(encoding="utf-8")
                          for p in sorted(out.rglob("*.md")))
        self.assertIn(self.PLACEHOLDER, written)

    def test_an_artifact_carrying_the_placeholder_cannot_grade_ok(self):
        report, _ = self._run(self.PLACEHOLDER)
        row = next(r for r in report["results"]
                   if r["asset_id"] == "lakehouse.shortcut.Sales.Tables/raw")
        self.assertEqual(row["status"], "needs_manual_review")
        self.assertIn("NS01_NAMESPACE_PLACEHOLDER",
                      [f["rule"] for f in row["findings"]])

    def test_the_finding_says_what_to_do(self):
        report, _ = self._run(self.PLACEHOLDER)
        row = next(r for r in report["results"]
                   if r["asset_id"] == "lakehouse.shortcut.Sales.Tables/raw")
        detail = next(f["detail"] for f in row["findings"]
                      if f["rule"] == "NS01_NAMESPACE_PLACEHOLDER")
        self.assertIn("--namespace", detail)

    def test_a_real_namespace_adds_no_finding(self):
        report, _ = self._run("acmens")
        row = next(r for r in report["results"]
                   if r["asset_id"] == "lakehouse.shortcut.Sales.Tables/raw")
        self.assertEqual(row["status"], "ok")
        self.assertNotIn("NS01_NAMESPACE_PLACEHOLDER",
                         [f["rule"] for f in row["findings"]])

    def test_the_notebook_slice_is_flagged_too(self):
        """Not just shortcuts: any artifact that bakes an oci:// path in."""
        report, _ = self._run(self.PLACEHOLDER)
        row = next(r for r in report["results"] if r["asset_id"] == "notebook.Ingest")
        self.assertNotEqual(row["status"], "ok")
        self.assertIn("NS01_NAMESPACE_PLACEHOLDER",
                      [f["rule"] for f in row["findings"]])


class FilteredRunNotebookRegistryTests(unittest.TestCase):
    """`--filter pipeline` used to tell every pipeline its notebook "was not
    migrated", which is false: the notebook is in the plan, and the filter
    chose not to emit it on this run.

    Measured on demo-input: unfiltered gave blocked=30 plus one review;
    `--filter pipeline` gave blocked=31, and the extra one was
    pipeline.Daily_Claims, refused with

        PL90_UNSUPPORTED_ACTIVITY - activity 'RunIngest' runs notebook
        '01_Ingest_Claims', which was not migrated

    01_Ingest_Claims is a notebook of that same plan. Where a notebook path
    is written is a property of the plan, not of which slice ran, so a
    filtered run resolves it from the plan -- and says, once per job, that
    this run did not write the files those tasks point at.
    """

    NB_PLAN = {
        "plan_id": "p-1",
        "target_aidp": {"namespace": "acmens", "catalog": "default"},
        "resolved_catalog": {},
        "assets": [
            {"id": "notebook.Ingest",
             "source": {"type": "fabric_notebook", "name": "Ingest",
                        "default_lakehouse": "Sales", "content": NB},
             "target": {"type": "aidp_notebook", "name": "Ingest"},
             "transform_chain": [], "depends_on": []},
            {"id": "pipeline.Daily",
             "source": {"type": "fabric_pipeline", "name": "Daily",
                        "activities": [{"name": "RunIngest",
                                        "type": "TridentNotebook",
                                        "notebook": "Ingest", "depends_on": []}]},
             "target": {"type": "aidp_job", "name": "Daily"},
             "transform_chain": [], "depends_on": ["notebook.Ingest"]},
        ],
    }

    def _run(self, **kw):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        plan = json.loads(json.dumps(self.NB_PLAN))
        return migrate(plan, out_dir=out, **kw), out

    def _row(self, report):
        return next(r for r in report["results"] if r["asset_id"] == "pipeline.Daily")

    def test_unfiltered_the_pipeline_translates(self):
        """The control: nothing about this pipeline is untranslatable."""
        report, _ = self._run()
        self.assertNotEqual(self._row(report)["status"], "blocked")

    def test_a_filtered_run_no_longer_claims_the_notebook_was_not_migrated(self):
        report, _ = self._run(filter_kind="pipeline")
        row = self._row(report)
        self.assertNotIn("was not migrated", json.dumps(row.get("findings", [])))
        self.assertNotEqual(row["status"], "blocked")

    def test_the_filtered_job_is_written_and_points_at_the_notebook(self):
        report, out = self._run(filter_kind="pipeline")
        row = self._row(report)
        job = json.loads((out / row["output_path"]).read_text(encoding="utf-8"))
        # Setup first even in a `--filter pipeline` run: the schemas a job
        # needs are a property of the plan, not of which slice ran, so the
        # setup is built from the whole plan (migrate/setup.py, issue #50).
        self.assertEqual([t["notebookPath"] for t in job["tasks"]],
                         ["/Workspace/00_setup_catalogs.py", "/Workspace/Ingest.py"])

    def test_the_filtered_job_says_this_run_did_not_write_the_notebooks(self):
        report, _ = self._run(filter_kind="pipeline")
        rules = [f["rule"] for f in self._row(report)["findings"]]
        self.assertIn("PL17_NOTEBOOK_NOT_IN_THIS_RUN", rules)

    def test_an_unfiltered_run_adds_no_such_finding(self):
        report, _ = self._run()
        rules = [f["rule"] for f in self._row(report)["findings"]]
        self.assertNotIn("PL17_NOTEBOOK_NOT_IN_THIS_RUN", rules)

    def test_a_notebook_absent_from_the_plan_is_still_blocked(self):
        """The genuine refusal must survive: no notebook, no job."""
        plan = json.loads(json.dumps(self.NB_PLAN))
        plan["assets"] = [plan["assets"][1]]
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        report = migrate(plan, out_dir=Path(tmp.name), filter_kind="pipeline")
        row = self._row(report)
        self.assertEqual(row["status"], "blocked")
        self.assertIn("was not migrated", json.dumps(row["findings"]))


class UnreadableNotebookTests(unittest.TestCase):
    """`inventory` records why a notebook's content file would not read; the
    runner used to translate the empty string it left behind anyway.

    Reproduced with a chmod 000 content file: inventory recorded
    `cannot read notebook-content.py: [Errno 13] Permission denied`, migrate
    wrote a 0-byte notebooks/Broken.py, graded it `needs_manual_review` with
    NB06_UNPARSEABLE ("not valid JSON") -- blaming the notebook format for a
    file it never opened -- and `publish` queued /Workspace/Broken.ipynb for
    upload. An empty notebook runs, and does nothing.

    Nothing was read, so nothing is emitted: the row is `blocked`, which
    reads REVIEW and which `publish` does not queue.
    """

    def _run(self, source):
        plan = {"plan_id": "p", "target_aidp": {"namespace": "acmens"},
                "resolved_catalog": {},
                "assets": [{"id": "notebook.Broken", "source": source,
                            "target": {"type": "aidp_notebook", "name": "Broken"},
                            "transform_chain": [], "depends_on": []}]}
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        return migrate(plan, out_dir=out), out

    UNREADABLE = {"type": "fabric_notebook", "name": "Broken", "content": "",
                  "parse_error": "cannot read notebook-content.py: "
                                 "[Errno 13] Permission denied"}

    def test_it_is_blocked_not_translated(self):
        report, _ = self._run(dict(self.UNREADABLE))
        self.assertEqual(report["results"][0]["status"], "blocked")

    def test_no_zero_byte_artifact_is_written(self):
        _, out = self._run(dict(self.UNREADABLE))
        self.assertEqual(list((out / "notebooks").glob("*"))
                         if (out / "notebooks").is_dir() else [], [])

    def test_the_inventorys_own_reason_reaches_the_report(self):
        report, _ = self._run(dict(self.UNREADABLE))
        row = report["results"][0]
        self.assertEqual([f["rule"] for f in row["findings"]],
                         ["NB07_SOURCE_UNREADABLE"])
        self.assertIn("Permission denied", row["findings"][0]["detail"])

    def test_it_does_not_blame_the_notebook_format(self):
        report, _ = self._run(dict(self.UNREADABLE))
        self.assertNotIn("NB06_UNPARSEABLE",
                         [f["rule"] for f in report["results"][0]["findings"]])

    def test_publish_does_not_queue_it(self):
        from fabric_aidp.publish.publisher import plan_publish
        _, out = self._run(dict(self.UNREADABLE))
        self.assertEqual(plan_publish(out)["notebooks"], [])

    def test_a_notebook_that_read_but_would_not_parse_is_still_translated(self):
        """Content present, parse_error set: there is text to run the
        line-based rules over, so it is translated and the inventory's reason
        is carried alongside rather than replacing it."""
        report, out = self._run({
            "type": "fabric_notebook", "name": "Broken",
            "content": 'spark.read.parquet("Files/x")\n',
            "parse_error": "not valid JSON: Expecting value"})
        row = report["results"][0]
        self.assertEqual(row["status"], "needs_manual_review")
        self.assertIn("NB07_SOURCE_UNREADABLE", [f["rule"] for f in row["findings"]])
        self.assertTrue((out / row["output_path"]).is_file())

    def test_a_clean_notebook_is_untouched(self):
        report, _ = self._run({"type": "fabric_notebook", "name": "Broken",
                               "content": NB, "default_lakehouse": "Sales"})
        self.assertEqual(report["results"][0]["status"], "ok")


class UnreadableWarehouseObjectTests(unittest.TestCase):
    """T4(b), the downstream half. `inventory` now records why a `.sql`
    would not decode; without this the runner translated the empty string
    it left behind, wrote a 0-byte `.spark.sql` and graded it PASS with
    zero findings -- the notebook defect above, reached through a bad
    encoding instead of a bad permission.

    Measured end to end on a workspace with two undecodable files:
    before, `plan` exited 2 with
    `duplicate asset id(s): warehouse.AcmeDW.other.<unreadable: 'utf-8'
    codec can't decode byte 0xe9 in position 46...>`; after, plan exits 0
    and migrate reports ok=1 needs_review=3 blocked=2, with verify at
    PASS 1 / REVIEW 5 / FAIL 0.
    """

    SOURCE = {"type": "fabric_warehouse_other", "warehouse": "AcmeDW",
              "schema": "", "name": "dupA", "file": "dupA.sql", "sql": "",
              "read_error": "'utf-8' codec can't decode byte 0xe9 in "
                            "position 46: invalid continuation byte"}

    def _run(self, source):
        plan = {"plan_id": "p", "target_aidp": {"namespace": "acmens"},
                "resolved_catalog": {},
                "assets": [{"id": "warehouse.AcmeDW.other.dupA",
                            "source": source,
                            "target": {"type": "aidp_sql_object",
                                       "schema": "", "name": "dupA"},
                            "transform_chain": [], "depends_on": []}]}
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        return migrate(plan, out_dir=out), out

    def test_it_is_blocked_not_translated(self):
        report, _ = self._run(dict(self.SOURCE))
        self.assertEqual(report["results"][0]["status"], "blocked")

    def test_no_zero_byte_artifact_is_written(self):
        _, out = self._run(dict(self.SOURCE))
        self.assertEqual(list((out / "warehouse").glob("*"))
                         if (out / "warehouse").is_dir() else [], [])

    def test_the_inventorys_own_reason_reaches_the_report(self):
        report, _ = self._run(dict(self.SOURCE))
        row = report["results"][0]
        self.assertEqual([f["rule"] for f in row["findings"]],
                         ["SQ00_SOURCE_UNREADABLE"])
        self.assertIn("codec", row["findings"][0]["detail"])
        self.assertIn("dupA.sql", row["findings"][0]["detail"])

    def test_it_does_not_grade_an_empty_object_pass(self):
        report, _ = self._run(dict(self.SOURCE))
        self.assertNotEqual(report["results"][0]["status"], "ok")

    def test_a_readable_object_is_untouched(self):
        source = dict(self.SOURCE, read_error="",
                      sql="CREATE TABLE dbo.dupA (id INT)")
        report, out = self._run(source)
        self.assertEqual(report["results"][0]["status"], "ok")
        self.assertTrue((out / report["results"][0]["output_path"]).is_file())

    def test_an_object_with_no_read_error_key_at_all_still_translates(self):
        """A plan written by an older inventory has no `read_error`."""
        source = {k: v for k, v in self.SOURCE.items() if k != "read_error"}
        source["sql"] = "CREATE TABLE dbo.dupA (id INT)"
        report, _ = self._run(source)
        self.assertEqual(report["results"][0]["status"], "ok")


class MarkdownShowsEveryOutcomeTests(unittest.TestCase):
    """report.md listed only `ok` and `needs_manual_review` rows.

    On demo-input that hid 42 blocked assets: the summary table said
    `blocked | 42` and the 542-line body did not name one of them, let
    alone say why. An errored asset was hidden the same way -- asset id,
    message and all -- so a reader of the Markdown could not tell which
    asset failed or what it said.
    """

    def _md(self, plan):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        migrate(plan, out_dir=out)
        return (out / "report.md").read_text(encoding="utf-8")

    def test_a_blocked_asset_is_named_with_its_reason(self):
        text = self._md(_plan())
        self.assertIn("pipeline.Daily", text)
        self.assertIn("PL90_UNSUPPORTED_ACTIVITY", text)
        self.assertIn("has no AIDP job equivalent", text)

    def test_an_errored_asset_is_named_with_its_message(self):
        plan = _plan()
        plan["target_aidp"] = {"namespace": "BAD NS"}
        plan["assets"] = [plan["assets"][0]]
        plan["assets"][0]["source"]["content"] = (
            "# Fabric notebook source\n\n# CELL ********************\n\n"
            'df = spark.read.parquet("abfss://ws@onelake.dfs.fabric.microsoft'
            '.com/Sales.Lakehouse/Files/x")\n')
        text = self._md(plan)
        self.assertIn("notebook.Ingest", text)
        self.assertIn("OCI namespace must be lowercase", text)

    def test_translated_assets_are_still_listed(self):
        self.assertIn("warehouse.table.dbo.claim", self._md(_plan()))



class UnresolvedNotebookRefTests(unittest.TestCase):
    """`edges.py` records an unresolvable `notebookutils.notebook.run(nb)`
    "so the report can tell a reviewer that a dependency exists but its
    name could not be determined". It reached no report: the notebook
    graded `ok` / PASS with a dependency nobody had been told about.
    """

    def _report(self, refs):
        plan = _plan()
        plan["assets"] = [dict(plan["assets"][0])]
        plan["assets"][0]["source"] = dict(plan["assets"][0]["source"],
                                           unresolved_refs=refs)
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        report = migrate(plan, out_dir=Path(tmp.name))
        return next(r for r in report["results"]
                    if r["asset_id"] == "notebook.Ingest")

    def test_the_reference_reaches_the_report(self):
        row = self._report(["nb"])
        self.assertIn("NB29_UNRESOLVED_NOTEBOOK_REF",
                      [f["rule"] for f in row["findings"]])

    def test_it_stops_the_notebook_grading_ok(self):
        self.assertEqual(self._report(["nb"])["status"], "needs_manual_review")

    def test_the_finding_names_the_expression_it_could_not_read(self):
        detail = next(f["detail"] for f in self._report(["nb"])["findings"]
                      if f["rule"] == "NB29_UNRESOLVED_NOTEBOOK_REF")
        self.assertIn("nb", detail)
        self.assertIn("run time", detail)

    def test_one_finding_per_reference(self):
        row = self._report(["nb", "other"])
        self.assertEqual(
            sum(1 for f in row["findings"]
                if f["rule"] == "NB29_UNRESOLVED_NOTEBOOK_REF"), 2)

    def test_a_notebook_with_none_is_untouched(self):
        row = self._report([])
        self.assertNotIn("NB29_UNRESOLVED_NOTEBOOK_REF",
                         [f["rule"] for f in row["findings"]])
        self.assertEqual(row["status"], "ok")


GUID = "2925655f-0293-4f32-8bc6-86ab989099a7"


class LakehouseGuidIndexTests(unittest.TestCase):
    """The plan's `lakehouse_catalog` never reached the notebook translator.

    `translate` has taken a `guid_index` since NB01 shipped, and this is
    its only caller. It passed nothing, so a notebook bound only by GUID
    got NB02_ONELAKE_UNMAPPED -- "recorded only as the GUID ..." -- while
    the plan sitting beside it held that GUID's display name, read out of
    another notebook's binding by `plan/planner._lakehouse_catalog`. The
    Dataflow branch of the same loop has passed that map as `lakehouses=`
    since Dataflows landed.

    A producer, a parameter, and nothing joining them.
    """

    NB = ("# Fabric notebook source\n\n# CELL ********************\n\n"
          'df = spark.read.parquet("/lakehouse/default/Files/x")\n')

    def _row(self, **plan_over):
        plan = _plan(**plan_over)
        plan["assets"] = [{
            "id": "notebook.ByGuid",
            "source": {"type": "fabric_notebook", "name": "ByGuid",
                       "default_lakehouse": GUID, "content": self.NB},
            "target": {"type": "aidp_notebook", "name": "ByGuid"},
            "transform_chain": ["fabric_notebook_to_spark"],
            "depends_on": []}]
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        report = migrate(plan, out_dir=out)
        row = report["results"][0]
        return row, (out / row["output_path"]).read_text("utf-8")

    def test_without_an_index_the_guid_cannot_be_named(self):
        """Unchanged and correct when the plan has nothing to offer: a
        GUID is a legal bucket name, so emitting one would be a confident
        rewrite naming a bucket nobody would create."""
        row, text = self._row(lakehouse_catalog={})
        self.assertIn("NB02_ONELAKE_UNMAPPED",
                      [f["rule"] for f in row["findings"]])
        self.assertIn("/lakehouse/default/Files/x", text)

    def test_the_plans_index_resolves_it(self):
        row, text = self._row(lakehouse_catalog={GUID: "SomeLakehouse"})
        self.assertIn("oci://SomeLakehouse@acmens/Files/x", text)
        self.assertEqual([f["rule"] for f in row["findings"]],
                         ["NB01_ONELAKE_PATH"])

    def test_a_plan_without_the_key_at_all_still_runs(self):
        """A plan file written by an older release has no
        `lakehouse_catalog`, and that has to keep meaning "no index"."""
        row, _text = self._row()
        self.assertIn("NB02_ONELAKE_UNMAPPED",
                      [f["rule"] for f in row["findings"]])

class FilterCannotHideARefusalTests(unittest.TestCase):
    """`--filter` slices by source, and two kinds of row belong to no source.

    `fabric_unreadable_item` (INV01) is a Fabric item directory whose
    identity would not read; `fabric_unsupported_item` (INV02) is an item of
    a type no scanner claims. Neither is in `PLAN_TYPES_BY_SOURCE` -- by
    design, and `fabric_aidp/sources.py` says so -- so the runner's
    `source_type not in FILTER_KINDS[filter_kind]` test excluded them from
    every one of the six slices.

    MEASURED on an export holding one notebook, one unreadable `.platform`
    and one Dashboard, before this change:

        migrate, no filter               3 rows: notebook, the two refusals
        migrate --filter notebook        1 row:  notebook
        --filter warehouse, lakehouse, pipeline, semanticmodel, dataflow
                                         0 rows each

    Two refusals, present in one of seven runs and reachable only by running
    with no filter at all. That is the hole `verify --filter` closed for a
    FAIL -- "a failure is never hidden by a filter" -- and it is worse here,
    because a FAIL at least surfaces in its own slice and these surface in
    none.

    So a row the tool cannot attribute to any slice is never excluded by a
    filter. Including it is free: every branch that handles one writes no
    artifact, so a filtered run's output directory is unchanged.
    """

    ASSETS = [
        {"id": "notebook.Ingest",
         "source": {"type": "fabric_notebook", "name": "Ingest",
                    "default_lakehouse": None, "content": NB},
         "target": {"type": "aidp_notebook", "name": "Ingest"},
         "transform_chain": [], "depends_on": []},
        {"id": "unreadable.Broken.Report",
         "source": {"type": "fabric_unreadable_item", "name": "Broken.Report",
                    "path": "Broken.Report",
                    "reason": ".platform is not valid JSON"},
         "target": {"type": "aidp_unmigrated", "name": "Broken.Report"},
         "transform_chain": [], "depends_on": []},
        {"id": "unsupported.Dashboard.Dash",
         "source": {"type": "fabric_unsupported_item", "name": "Dash",
                    "item_type": "Dashboard", "path": "Dash.Dashboard",
                    "reason": "this tool has no scanner for Fabric item "
                              "type 'Dashboard'"},
         "target": {"type": "aidp_unmigrated", "name": "Dash"},
         "transform_chain": [], "depends_on": []},
    ]

    REFUSALS = ("unreadable.Broken.Report", "unsupported.Dashboard.Dash")

    def _run(self, filter_kind=None):
        plan = {"plan_id": "p", "target_aidp": {"namespace": "acmens"},
                "resolved_catalog": {}, "assets": self.ASSETS}
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name)
        return migrate(plan, out_dir=out, filter_kind=filter_kind), out

    def test_every_slice_still_reports_both_refusals(self):
        for slice_name in ALL_SOURCES:
            with self.subTest(filter=slice_name):
                report, _ = self._run(slice_name)
                ids = [r["asset_id"] for r in report["results"]]
                for refusal in self.REFUSALS:
                    self.assertIn(
                        refusal, ids,
                        f"--filter {slice_name} dropped {refusal}, a refusal "
                        f"that belongs to no slice and so can be seen in no "
                        f"other one")

    def test_a_refusal_shown_this_way_says_why_it_is_there(self):
        report, _ = self._run("warehouse")
        row = next(r for r in report["results"]
                   if r["asset_id"] == "unreadable.Broken.Report")
        detail = row["findings"][-1]["detail"]
        self.assertIn("--filter warehouse", detail)
        self.assertIn("no source slice", detail)
        # and the reason it was refused is still the first thing said
        self.assertTrue(detail.startswith("Broken.Report is a Fabric item"),
                        detail)

    def test_an_unfiltered_run_adds_no_such_sentence(self):
        report, _ = self._run()
        for row in report["results"]:
            for finding in row.get("findings", []):
                self.assertNotIn("--filter", finding["detail"])

    def test_the_slice_the_user_asked_for_is_still_the_only_one_translated(self):
        """Shown, not run: the refusals are added to the report, never to
        the translated set, so `--filter notebook` still translates exactly
        the notebook slice."""
        report, _ = self._run("notebook")
        translated = [r["asset_id"] for r in report["results"]
                      if r["status"] in ("ok", "needs_manual_review")]
        self.assertEqual(translated, ["notebook.Ingest"])

    def test_a_filtered_run_writes_no_artifact_for_them(self):
        report, out = self._run("warehouse")
        self.assertEqual(
            sorted(p.name for p in out.rglob("*") if p.is_file()),
            ["report.html", "report.json", "report.md"])
        self.assertEqual(report["counts"].get("blocked"), 2)

    def test_they_are_counted_not_merely_printed(self):
        """A filtered run's `blocked` count is the one the operator reads
        off the console line, so it has to include them."""
        for slice_name in ALL_SOURCES:
            with self.subTest(filter=slice_name):
                report, _ = self._run(slice_name)
                self.assertEqual(report["counts"].get("blocked"), 2)


class UnclaimedArtifactTests(unittest.TestCase):
    """`migrate` writes into an existing directory and never deletes, so a
    second run with a different `--filter` leaves the first run's artifacts
    beside a report that does not mention them.

    MEASURED on the bundled demo estate, two runs into /tmp/cl-out:

        migrate --filter warehouse   warehouse/ -> 22 files, 22 rows
        migrate --filter notebook    warehouse/ -> still 22 files
                                     report.json -> 31 rows, all kind=notebook

        artifacts on disk                  53
        artifacts the report vouches for   31
        orphaned (report does not mention) 22

    Nothing is deleted here, and that is deliberate. `-o` names a directory
    the user chose, and migrating slices into one directory is a workflow
    this tool *recommends*: PL17_NOTEBOOK_NOT_IN_THIS_RUN tells the reader to
    "migrate the notebook slice into the same output directory before
    publishing". Emptying the directory would break the advice the runner
    gives.

    What was wrong is that the directory did not say so. `publish` is
    already safe -- it reads report.json rather than globbing, and
    tests/test_publish.py::RefusalTests::
    test_an_artifact_the_report_does_not_list_is_not_published pins that --
    but a person opening the directory, or any tool that globs it, saw 53
    files with no way to tell which 22 the report disowns. So the report now
    names them.
    """

    NB_ASSET = {"id": "notebook.Ingest",
                "source": {"type": "fabric_notebook", "name": "Ingest",
                           "default_lakehouse": None, "content": NB},
                "target": {"type": "aidp_notebook", "name": "Ingest"},
                "transform_chain": [], "depends_on": []}
    WH_ASSET = {"id": "warehouse.W.table.dbo.claim",
                "source": {"type": "fabric_warehouse_table", "warehouse": "W",
                           "schema": "dbo", "name": "claim",
                           "sql": "CREATE TABLE dbo.claim (id INT)"},
                "target": {"type": "aidp_table", "schema": "dbo",
                           "name": "claim"},
                "transform_chain": [], "depends_on": []}

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.out = Path(tmp.name) / "migrated"
        self.plan = {"plan_id": "p", "target_aidp": {"namespace": "acmens"},
                     "resolved_catalog": {},
                     "assets": [self.NB_ASSET, self.WH_ASSET]}
        self.lines = []

    def _run(self, filter_kind=None):
        return migrate(self.plan, out_dir=self.out, filter_kind=filter_kind,
                       log=self.lines.append)

    def test_a_fresh_run_claims_everything_it_wrote(self):
        report = self._run()
        self.assertEqual(report["unclaimed_artifacts"], [])

    def test_a_second_filter_names_the_first_runs_artifacts(self):
        self._run("warehouse")
        report = self._run("notebook")
        self.assertEqual(report["unclaimed_artifacts"],
                         ["warehouse/dbo.claim.spark.sql"])

    def test_they_are_still_on_disk_afterwards(self):
        """Named, never deleted: `-o` is a directory the user chose."""
        self._run("warehouse")
        self._run("notebook")
        self.assertTrue((self.out / "warehouse" / "dbo.claim.spark.sql").is_file())
        self.assertTrue((self.out / "notebooks" / "Ingest.py").is_file())

    def test_re_running_the_same_slice_claims_its_own_output_again(self):
        """The files are overwritten in place, so nothing is orphaned."""
        self._run("warehouse")
        self.assertEqual(self._run("warehouse")["unclaimed_artifacts"], [])

    def test_the_reports_own_files_are_never_called_unclaimed(self):
        self._run()
        report = self._run()
        for name in ("report.json", "report.md", "report.html"):
            self.assertNotIn(name, report["unclaimed_artifacts"])

    def test_a_file_a_user_dropped_in_is_named_too(self):
        """Anything the report does not vouch for, whoever wrote it. A
        hand-edited `.py` beside the generated ones is exactly as
        unaccounted-for as a stale one."""
        self.out.mkdir(parents=True, exist_ok=True)
        (self.out / "notes.txt").write_text("mine", encoding="utf-8")
        self.assertEqual(self._run()["unclaimed_artifacts"], ["notes.txt"])

    def test_the_run_says_so_on_the_console(self):
        self._run("warehouse")
        self.lines.clear()
        self._run("notebook")
        said = "\n".join(self.lines)
        self.assertIn("1 file", said)
        self.assertIn("not written by this run", said)

    def test_report_md_names_each_one(self):
        self._run("warehouse")
        self._run("notebook")
        markdown = (self.out / "report.md").read_text(encoding="utf-8")
        self.assertIn("Not written by this run", markdown)
        self.assertIn("warehouse/dbo.claim.spark.sql", markdown)

    def test_report_md_stays_quiet_when_there_are_none(self):
        self._run()
        markdown = (self.out / "report.md").read_text(encoding="utf-8")
        self.assertNotIn("Not written by this run", markdown)

    def test_a_blocked_row_writes_nothing_so_orphans_nothing(self):
        """The in-progress marker is a dotfile and is skipped; so is any
        temp file `_atomic` was interrupted mid-write on."""
        self._run()
        (self.out / ".fabric-aidp-leftover.tmp").write_text("x", encoding="utf-8")
        self.assertEqual(self._run()["unclaimed_artifacts"], [])


if __name__ == "__main__":
    unittest.main()
