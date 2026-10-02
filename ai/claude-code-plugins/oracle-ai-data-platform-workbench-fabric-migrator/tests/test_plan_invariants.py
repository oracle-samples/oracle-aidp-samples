import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.plan.planner import build_plan, summarize_plan, write_plan


def _manifest(**over):
    base = {
        "source": "fabric-git", "workspace_name": "WS",
        "sources_scanned": ["notebook", "warehouse", "lakehouse", "pipeline",
                            "semanticmodel"],
        "sources": {
            "notebook": {"items": {"notebooks": [
                {"name": "Ingest", "logical_id": "n1", "language": "python",
                 "content": "x = 1", "default_lakehouse": "Sales",
                 "edges": {"run": ["Common"], "notebook_run": [], "unresolved": []}},
                {"name": "Common", "logical_id": "n2", "language": "python",
                 "content": "y = 2", "default_lakehouse": None,
                 "edges": {"run": [], "notebook_run": [], "unresolved": []}},
            ]}},
            "warehouse": {"items": {"warehouses": [
                {"name": "DW", "logical_id": "w1", "objects": [
                    {"kind": "table", "schema": "dbo", "name": "claim",
                     "file": "claim.sql", "sql": "CREATE TABLE dbo.claim (id INT)"},
                    {"kind": "procedure", "schema": "dbo", "name": "sp_load",
                     "file": "sp.sql", "sql": "CREATE PROCEDURE dbo.sp_load AS BEGIN END"},
                ]}]}},
            "lakehouse": {"items": {"lakehouses": [
                {"name": "Sales", "logical_id": "l1",
                 "tracking": {"shortcuts": "tracked"},
                 "shortcuts": [{"name": "claims_raw", "section": "Tables",
                                "target_type": "AmazonS3", "target": "s3://a/c",
                                "external": True}],
                 "shortcut_count": 1},
            ]}},
            "pipeline": {"items": {"pipelines": [
                {"name": "Daily", "logical_id": "p1", "activities": [],
                 "notebook_refs": ["Ingest"]},
            ]}},
            "semanticmodel": {"items": {"semantic_models": [
                {"name": "SalesModel", "logical_id": "s1", "description": ""},
            ]}},
        },
        "resolved_catalog": {"summary": {}, "tables": {}},
    }
    base.update(over)
    return base


class UnreadableItemAssetTests(unittest.TestCase):
    """An item the discovery step could not read must reach the plan and
    the migration report. Dropping it made the asset count smaller with no
    reason given anywhere -- the whole point of the finding."""

    def _plan(self):
        return build_plan(_manifest(unreadable_items=[
            {"name": "Bad.Notebook", "path": "Bad.Notebook",
             "reason": ".platform is not valid JSON"},
        ]), oci_namespace="acmens")

    def test_it_becomes_an_asset(self):
        ids = [a["id"] for a in self._plan()["assets"]]
        self.assertIn("unreadable.Bad.Notebook", ids)

    def test_it_raises_the_asset_count(self):
        clean = build_plan(_manifest(), oci_namespace="acmens")
        self.assertEqual(self._plan()["summary"]["asset_count"],
                         clean["summary"]["asset_count"] + 1)

    def test_it_carries_the_reason(self):
        asset = next(a for a in self._plan()["assets"]
                     if a["id"] == "unreadable.Bad.Notebook")
        self.assertEqual(asset["source"]["type"], "fabric_unreadable_item")
        self.assertIn("valid JSON", asset["source"]["reason"])
        self.assertEqual(asset["target"]["type"], "aidp_unmigrated")

    def test_a_clean_manifest_adds_nothing(self):
        ids = [a["id"] for a in build_plan(_manifest(),
                                           oci_namespace="acmens")["assets"]]
        self.assertFalse([i for i in ids if i.startswith("unreadable.")])


class UnsupportedItemAssetTests(unittest.TestCase):
    """An item type this tool cannot migrate is discovered and then
    disappears: it is in no source's items, so it reaches neither the
    manifest nor the plan and the operator is never told it was skipped."""

    def _plan(self):
        return build_plan(_manifest(unsupported_items=[
            {"name": "SalesReport", "item_type": "Report",
             "path": "SalesReport.Report",
             "reason": "this tool has no scanner for Fabric item type 'Report'"},
        ]), oci_namespace="acmens")

    def test_it_becomes_an_asset(self):
        self.assertIn("unsupported.Report.SalesReport",
                      [a["id"] for a in self._plan()["assets"]])

    def test_it_raises_the_asset_count(self):
        clean = build_plan(_manifest(), oci_namespace="acmens")
        self.assertEqual(self._plan()["summary"]["asset_count"],
                         clean["summary"]["asset_count"] + 1)

    def test_it_carries_the_type_and_the_reason(self):
        asset = next(a for a in self._plan()["assets"]
                     if a["id"] == "unsupported.Report.SalesReport")
        self.assertEqual(asset["source"]["type"], "fabric_unsupported_item")
        self.assertEqual(asset["source"]["item_type"], "Report")
        self.assertIn("no scanner", asset["source"]["reason"])
        self.assertEqual(asset["target"]["type"], "aidp_unmigrated")

    def test_a_clean_manifest_adds_nothing(self):
        ids = [a["id"] for a in build_plan(_manifest(),
                                           oci_namespace="acmens")["assets"]]
        self.assertFalse([i for i in ids if i.startswith("unsupported.")])


class AssetTests(unittest.TestCase):
    def setUp(self):
        self.plan = build_plan(_manifest(), oci_namespace="acmens")
        self.ids = [a["id"] for a in self.plan["assets"]]

    def test_every_item_becomes_an_asset(self):
        for expected in ("notebook.Ingest", "notebook.Common",
                         "warehouse.DW.table.dbo.claim", "warehouse.DW.procedure.dbo.sp_load",
                         "lakehouse.Sales", "lakehouse.shortcut.Sales.claims_raw",
                         "pipeline.Daily", "semanticmodel.SalesModel"):
            self.assertIn(expected, self.ids)

    def test_asset_ids_are_unique(self):
        self.assertEqual(len(self.ids), len(set(self.ids)))

    def test_notebook_targets_an_aidp_notebook(self):
        asset = next(a for a in self.plan["assets"] if a["id"] == "notebook.Ingest")
        self.assertEqual(asset["target"]["type"], "aidp_notebook")
        self.assertEqual(asset["transform_chain"],
                         ["fabric_notebook_to_spark", "create_aidp_notebook"])

    def test_warehouse_table_targets_an_aidp_table(self):
        asset = next(a for a in self.plan["assets"]
                     if a["id"] == "warehouse.DW.table.dbo.claim")
        self.assertEqual(asset["target"]["type"], "aidp_table")
        self.assertEqual(asset["transform_chain"],
                         ["tsql_to_spark_sql", "create_aidp_table"])

    def test_external_shortcut_targets_an_external_table(self):
        asset = next(a for a in self.plan["assets"]
                     if a["id"] == "lakehouse.shortcut.Sales.claims_raw")
        self.assertEqual(asset["target"]["type"], "aidp_dcat_external_table")
        self.assertEqual(asset["transform_chain"],
                         ["shortcut_target_to_oci", "create_aidp_external_table"])

    def test_an_unreadable_shortcuts_file_reaches_the_lakehouse_asset(self):
        """The reason has to travel, or the report has nothing to print."""
        manifest = _manifest()
        lakehouse = (manifest["sources"]["lakehouse"]["items"]["lakehouses"][0])
        lakehouse["shortcuts_error"] = (
            "cannot read shortcuts.metadata.json: Expecting value")
        lakehouse["shortcut_count"] = None
        lakehouse["tracking"] = {"shortcuts": "unknown"}
        asset = next(a for a in build_plan(manifest, oci_namespace="acmens")["assets"]
                     if a["id"] == "lakehouse.Sales")
        self.assertIn("Expecting value", asset["source"]["shortcuts_error"])

    def test_a_readable_lakehouse_carries_no_error(self):
        asset = next(a for a in self.plan["assets"] if a["id"] == "lakehouse.Sales")
        self.assertEqual(asset["source"].get("shortcuts_error", ""), "")

    def test_a_shortcut_asset_carries_the_bucket_field(self):
        """`S3Compatible` records its bucket outside the URI, so the plan has
        to carry it or the translator cannot name the right target."""
        asset = next(a for a in self.plan["assets"]
                     if a["id"] == "lakehouse.shortcut.Sales.claims_raw")
        self.assertIn("bucket", asset["source"])

    def test_namespace_is_recorded(self):
        self.assertEqual(self.plan["target_aidp"]["namespace"], "acmens")

    def test_summary_counts_by_target_type(self):
        self.assertEqual(self.plan["summary"]["asset_count"], len(self.ids))
        self.assertIn("aidp_notebook", self.plan["summary"]["by_target_type"])


class DependencyTests(unittest.TestCase):
    def test_notebook_depends_on_its_default_lakehouse_and_run_target(self):
        plan = build_plan(_manifest(), oci_namespace="acmens")
        asset = next(a for a in plan["assets"] if a["id"] == "notebook.Ingest")
        self.assertIn("lakehouse.Sales", asset["depends_on"])
        self.assertIn("notebook.Common", asset["depends_on"])

    def test_pipeline_depends_on_the_notebook_it_invokes(self):
        plan = build_plan(_manifest(), oci_namespace="acmens")
        asset = next(a for a in plan["assets"] if a["id"] == "pipeline.Daily")
        self.assertEqual(asset["depends_on"], ["notebook.Ingest"])

    def test_pipeline_schedules_are_carried_to_the_translator(self):
        m = _manifest()
        row = m["sources"]["pipeline"]["items"]["pipelines"][0]
        row["schedules"] = [{"enabled": True, "configuration": {"type": "Cron"}}]
        row["schedules_error"] = "x"
        asset = next(a for a in build_plan(m, oci_namespace="acmens")["assets"]
                     if a["id"] == "pipeline.Daily")
        self.assertEqual(asset["source"]["schedules"], row["schedules"])
        self.assertEqual(asset["source"]["schedules_error"], "x")

    def test_dependency_comes_before_its_dependent(self):
        plan = build_plan(_manifest(), oci_namespace="acmens")
        ids = [a["id"] for a in plan["assets"]]
        self.assertLess(ids.index("notebook.Common"), ids.index("notebook.Ingest"))
        self.assertLess(ids.index("notebook.Ingest"), ids.index("pipeline.Daily"))

    def test_dangling_reference_is_dropped_and_warned(self):
        m = _manifest()
        m["sources"]["notebook"]["items"]["notebooks"][0]["edges"]["run"] = ["Ghost"]
        plan = build_plan(m, oci_namespace="acmens")
        asset = next(a for a in plan["assets"] if a["id"] == "notebook.Ingest")
        self.assertNotIn("notebook.Ghost", asset["depends_on"])
        self.assertIn("notebook.Ghost", plan["warnings"]["dangling_depends_on"])

    def test_cycle_is_a_warning_not_an_error(self):
        m = _manifest()
        notebooks = m["sources"]["notebook"]["items"]["notebooks"]
        notebooks[0]["edges"]["run"] = ["Common"]
        notebooks[1]["edges"]["run"] = ["Ingest"]
        plan = build_plan(m, oci_namespace="acmens")
        self.assertEqual(sorted(plan["warnings"]["cycles"]),
                         ["notebook.Common", "notebook.Ingest"])
        self.assertEqual(plan["warnings"]["blocked_by_cycle"], ["pipeline.Daily"])
        self.assertEqual(len(plan["assets"]), 8)

    def test_cycle_members_are_emitted_last(self):
        m = _manifest()
        notebooks = m["sources"]["notebook"]["items"]["notebooks"]
        notebooks[0]["edges"]["run"] = ["Common"]
        notebooks[1]["edges"]["run"] = ["Ingest"]
        ids = [a["id"] for a in build_plan(m, oci_namespace="acmens")["assets"]]
        self.assertGreater(ids.index("notebook.Common"),
                           ids.index("warehouse.DW.table.dbo.claim"))

    def test_ordering_is_deterministic(self):
        first = [a["id"] for a in build_plan(_manifest(), oci_namespace="ns")["assets"]]
        second = [a["id"] for a in build_plan(_manifest(), oci_namespace="ns")["assets"]]
        self.assertEqual(first, second)


class ValidationTests(unittest.TestCase):
    def test_non_dict_manifest_is_rejected(self):
        with self.assertRaises(ValueError):
            build_plan([], oci_namespace="ns")

    def test_bad_namespace_is_rejected(self):
        with self.assertRaises(ValueError):
            build_plan(_manifest(), oci_namespace="Bad NS!")

    def test_placeholder_namespace_is_allowed(self):
        self.assertIsNotNone(build_plan(_manifest()))

    def test_missing_sources_key_yields_an_empty_plan(self):
        plan = build_plan({"sources": {}}, oci_namespace="ns")
        self.assertEqual(plan["assets"], [])


class OutputTests(unittest.TestCase):
    def test_write_plan_round_trips(self):
        with TemporaryDirectory() as t:
            out = write_plan(build_plan(_manifest(), oci_namespace="ns"),
                             Path(t) / "p" / "plan.json")
            self.assertEqual(json.loads(out.read_text(encoding="utf-8"))
                             ["target_aidp"]["namespace"], "ns")

    def test_summary_lists_target_types(self):
        text = summarize_plan(build_plan(_manifest(), oci_namespace="ns"))
        self.assertIn("aidp_notebook", text)

    def test_summary_shows_warnings(self):
        m = _manifest()
        m["sources"]["notebook"]["items"]["notebooks"][0]["edges"]["run"] = ["Ghost"]
        self.assertIn("dangling", summarize_plan(build_plan(m, oci_namespace="ns")))


class WarehouseIdCollisionTests(unittest.TestCase):
    """Two Warehouses in one workspace may each hold a `dbo.CurrentDate`.

    The asset id omitted the warehouse name, so they collided and `plan`
    refused the entire workspace -- not the object, the workspace. Found by
    staging 78 real third-party artifacts as input; the synthetic demo estate
    has one warehouse and could never surface it.
    """

    def _manifest(self):
        def wh(name):
            return {"name": name, "logical_id": name, "objects": [
                {"kind": "table", "schema": "dbo", "name": "CurrentDate",
                 "file": "t.sql", "sql": "CREATE TABLE dbo.CurrentDate (d DATE)"}]}
        return {"sources": {"warehouse": {"summary": {},
                                          "items": {"warehouses": [wh("Primary"),
                                                                   wh("Secondary")]}}},
                "resolved_catalog": {"tables": {}}}

    def test_two_warehouses_with_the_same_table_both_plan(self):
        plan = build_plan(self._manifest())
        ids = [a["id"] for a in plan["assets"]]
        self.assertEqual(len(ids), len(set(ids)), f"duplicate asset ids: {ids}")
        self.assertEqual(len(ids), 2)

    def test_the_warehouse_name_is_in_the_id(self):
        ids = [a["id"] for a in build_plan(self._manifest())["assets"]]
        self.assertIn("warehouse.Primary.table.dbo.CurrentDate", ids)
        self.assertIn("warehouse.Secondary.table.dbo.CurrentDate", ids)


class OneSchemaMeaningTests(unittest.TestCase):
    """A Lakehouse planned as `aidp_schema name=SalesLake` while a Warehouse
    table planned as `aidp_table schema=dbo`. The same word meant the Fabric
    item in one target and the Fabric schema in the other."""

    def _manifest(self):
        return {"sources": {
            "lakehouse": {"summary": {}, "items": {"lakehouses": [
                {"name": "SalesLake", "logical_id": "lh", "shortcuts": []}]}},
            "warehouse": {"summary": {}, "items": {"warehouses": [
                {"name": "AcmeDW", "logical_id": "wh", "objects": [
                    {"kind": "table", "schema": "dbo", "name": "claim",
                     "file": "t.sql", "sql": "CREATE TABLE dbo.claim (i INT)"}]}]}},
        }, "resolved_catalog": {"tables": {}}}

    def _targets(self, **kw):
        return {a["id"]: a["target"] for a in build_plan(self._manifest(), **kw)["assets"]}

    def test_a_lakehouse_schema_and_a_warehouse_schema_agree(self):
        targets = self._targets()
        lakehouse = targets["lakehouse.SalesLake"]
        table = targets["warehouse.AcmeDW.table.dbo.claim"]
        self.assertEqual(lakehouse["name"], "SalesLake")
        self.assertEqual(table["name"], "default.AcmeDW.claim")

    def test_the_warehouse_item_is_not_lost(self):
        table = self._targets()["warehouse.AcmeDW.table.dbo.claim"]
        self.assertIn("AcmeDW", table["name"])
        self.assertNotIn("default.dbo.", table["name"])

    def test_the_catalog_reaches_the_plan(self):
        table = self._targets(catalog="myc")["warehouse.AcmeDW.table.dbo.claim"]
        self.assertTrue(table["name"].startswith("myc."))



class AssetIdCollisionTests(unittest.TestCase):
    """Three shapes of legal Fabric export that `plan` refused outright.

    Each one is ordinary -- a lakehouse with a `Files/x` and a `Tables/x`,
    a DacFx project with one stem in two schema folders, two notebooks of
    one display name in two workspace folders -- and in each the export
    already carries what tells the two apart. The id carried only the
    name, so `build_plan` raised `duplicate asset id(s)` and the operator
    lost the whole workspace over it.
    """

    def _ids(self, manifest):
        return [a["id"] for a in build_plan(manifest)["assets"]]

    def _lakehouse(self, shortcuts):
        return {"sources": {"lakehouse": {"summary": {}, "items": {"lakehouses": [
            {"name": "SalesLake", "logical_id": "l1", "folder": "",
             "tracking": {"shortcuts": "tracked"},
             "shortcuts": shortcuts, "shortcut_count": len(shortcuts)}]}}},
            "resolved_catalog": {"tables": {}}}

    @staticmethod
    def _shortcut(name, path):
        return {"name": name, "section": path.split("/")[0], "path": path,
                "schema": (path.split("/") + [""])[1],
                "table_name": None, "target_type": "AmazonS3",
                "target": "s3://a/c", "bucket": "", "external": True}

    def test_one_name_in_two_sections_is_two_shortcuts(self):
        ids = self._ids(self._lakehouse([
            self._shortcut("claims_raw_s3", "Tables"),
            self._shortcut("claims_raw_s3", "Files")]))
        self.assertEqual(len(ids), len(set(ids)), ids)
        self.assertIn("lakehouse.shortcut.SalesLake.Tables/claims_raw_s3", ids)
        self.assertIn("lakehouse.shortcut.SalesLake.Files/claims_raw_s3", ids)

    def test_one_name_in_two_table_schemas_is_two_shortcuts(self):
        ids = self._ids(self._lakehouse([
            self._shortcut("shared_dim_date", "Tables"),
            self._shortcut("shared_dim_date", "Tables/dbo")]))
        self.assertEqual(len(ids), len(set(ids)), ids)
        self.assertIn("lakehouse.shortcut.SalesLake.Tables/dbo/shared_dim_date",
                      ids)

    def test_one_sql_stem_in_two_schema_folders_is_two_objects(self):
        """`CREATE INDEX` classifies as `other`, so the name falls back to
        the filename stem and `schemas/dbo/tables/helper.sql` and
        `schemas/sales/tables/helper.sql` produced one id."""
        manifest = {"sources": {"warehouse": {"summary": {}, "items": {
            "warehouses": [{"name": "AcmeDW", "logical_id": "w", "folder": "",
                            "objects": [
                {"kind": "other", "schema": "", "name": "helper",
                 "file": f"schemas/{schema}/tables/helper.sql",
                 "sql": "CREATE INDEX ix ON t(a);"}
                for schema in ("dbo", "sales")]}]}}},
            "resolved_catalog": {"tables": {}}}
        ids = self._ids(manifest)
        self.assertEqual(len(ids), len(set(ids)), ids)
        self.assertIn(
            "warehouse.AcmeDW.other.schemas/dbo/tables/helper.sql", ids)
        self.assertIn(
            "warehouse.AcmeDW.other.schemas/sales/tables/helper.sql", ids)

    @staticmethod
    def _two_helper_sql_manifest():
        return {"sources": {"warehouse": {"summary": {}, "items": {
            "warehouses": [{"name": "AcmeDW", "logical_id": "w", "folder": "",
                            "objects": [
                {"kind": "other", "schema": "", "name": "helper",
                 "file": f"schemas/{schema}/tables/helper.sql",
                 "sql": "CREATE INDEX ix ON t(a);"}
                for schema in ("dbo", "sales")]}]}}},
            "resolved_catalog": {"tables": {}}}

    def _targets(self, manifest):
        return {a["id"]: a["target"] for a in build_plan(manifest)["assets"]}

    def test_an_unclassified_object_is_not_given_a_table_name(self):
        """The id collision was fixed and the *target* collision was not:
        both `helper.sql` files still promised `default.AcmeDW.helper`.

        Making the name unique would have been the wrong answer. A
        `CREATE INDEX` script has no AIDP table to be -- the planner already
        knows it, since `transform_chain` omits `create_aidp_table` for
        `other` and `target.type` is `aidp_sql_object` rather than
        `aidp_table`. The three-part name was the one place the plan still
        claimed a table it never promised to create.
        """
        targets = self._targets(self._two_helper_sql_manifest())
        for asset_id, target in sorted(targets.items()):
            with self.subTest(asset=asset_id):
                self.assertEqual(target["type"], "aidp_sql_object")
                self.assertNotIn(
                    ".", target["name"].replace(".sql", ""),
                    f"{asset_id} targets {target['name']!r}, which is a "
                    f"catalog.schema.table name for a script that creates "
                    f"no table")
                self.assertNotEqual(target["name"], "default.AcmeDW.helper")

    def test_two_unclassified_files_do_not_share_one_target(self):
        """Falls out of the fix above rather than being it: the file is the
        only identity an unclassified object has, so naming the target by it
        cannot collide where the id does not."""
        targets = self._targets(self._two_helper_sql_manifest())
        names = [t["name"] for t in targets.values()]
        self.assertEqual(len(set(names)), 2, names)
        self.assertEqual(
            sorted(names),
            ["schemas/dbo/tables/helper.sql", "schemas/sales/tables/helper.sql"])

    def test_a_classified_object_keeps_its_table_shaped_target(self):
        """The fix is confined to `other`. A procedure or a function has a
        real `schema.name` the database enforces, so it keeps a qualified
        AIDP name; only the kind that means "we could not tell" loses one."""
        manifest = {"sources": {"warehouse": {"summary": {}, "items": {
            "warehouses": [{"name": "AcmeDW", "logical_id": "w", "folder": "",
                            "objects": [
                {"kind": kind, "schema": "dbo", "name": "thing",
                 "file": f"schemas/dbo/{kind}/thing.sql", "sql": "SELECT 1"}
                for kind in ("table", "view", "procedure", "function")]}]}}},
            "resolved_catalog": {"tables": {}}}
        for asset_id, target in sorted(self._targets(manifest).items()):
            with self.subTest(asset=asset_id):
                self.assertEqual(target["name"], "default.AcmeDW.thing")
                self.assertEqual(target["schema"], "AcmeDW")

    def test_a_classified_object_is_still_named_by_its_schema_and_name(self):
        """The file only replaces a name that was never a database name. A
        table keeps `schema.name`, which is the identity the database
        itself enforces and the one a reader greps for."""
        manifest = {"sources": {"warehouse": {"summary": {}, "items": {
            "warehouses": [{"name": "AcmeDW", "logical_id": "w", "folder": "",
                            "objects": [
                {"kind": "table", "schema": "dbo", "name": "claim",
                 "file": "schemas/dbo/tables/claim.sql",
                 "sql": "CREATE TABLE dbo.claim (id INT)"}]}]}}},
            "resolved_catalog": {"tables": {}}}
        self.assertEqual(self._ids(manifest),
                         ["warehouse.AcmeDW.table.dbo.claim"])

    def test_one_notebook_name_in_two_folders_is_two_notebooks(self):
        manifest = {"sources": {"notebook": {"summary": {}, "items": {
            "notebooks": [
                {"name": "Shared_Load", "logical_id": f"n{i}", "folder": folder,
                 "language": "python", "content": "x = 1",
                 "default_lakehouse": None,
                 "edges": {"run": [], "notebook_run": [], "unresolved": []}}
                for i, folder in enumerate(("teamA", "teamB"))]}}},
            "resolved_catalog": {"tables": {}}}
        ids = self._ids(manifest)
        self.assertEqual(len(ids), len(set(ids)), ids)
        self.assertEqual(sorted(ids),
                         ["notebook.teamA/Shared_Load",
                          "notebook.teamB/Shared_Load"])

    def test_an_item_at_the_root_keeps_the_short_id(self):
        """The disambiguator is only spelt out when there is something to
        disambiguate. All 106 assets of the bundled demo estate sit at the
        export root and none of their ids changed."""
        ids = [a["id"] for a in build_plan(_manifest(),
                                           oci_namespace="acmens")["assets"]]
        self.assertIn("notebook.Ingest", ids)
        self.assertIn("pipeline.Daily", ids)
        self.assertIn("semanticmodel.SalesModel", ids)


class FolderedReferenceTests(unittest.TestCase):
    """A `%run` and an ExecutePipeline name their target by display name,
    never by folder, so once the id carries a folder the literal
    `notebook.<name>` stops being anybody's id and the edge would dangle."""

    def _manifest(self, folder="teamA"):
        return {"sources": {
            "notebook": {"summary": {}, "items": {"notebooks": [
                {"name": "Child", "logical_id": "n1", "folder": folder,
                 "language": "python", "content": "y = 2",
                 "default_lakehouse": None,
                 "edges": {"run": [], "notebook_run": [], "unresolved": []}},
                {"name": "Parent", "logical_id": "n2", "folder": "",
                 "language": "python", "content": "%run Child",
                 "default_lakehouse": None,
                 "edges": {"run": ["Child"], "notebook_run": [],
                           "unresolved": []}}]}},
            "pipeline": {"summary": {}, "items": {"pipelines": [
                {"name": "Daily", "logical_id": "p1", "folder": "",
                 "activities": [], "notebook_refs": ["Child"],
                 "pipeline_refs": []}]}}},
            "resolved_catalog": {"tables": {}}}

    def test_a_run_edge_reaches_the_notebook_in_its_folder(self):
        plan = build_plan(self._manifest())
        parent = next(a for a in plan["assets"] if a["id"] == "notebook.Parent")
        self.assertEqual(parent["depends_on"], ["notebook.teamA/Child"])
        self.assertEqual(plan["warnings"]["dangling_depends_on"], [])

    def test_a_pipeline_notebook_ref_reaches_it_too(self):
        plan = build_plan(self._manifest())
        daily = next(a for a in plan["assets"] if a["id"] == "pipeline.Daily")
        self.assertEqual(daily["depends_on"], ["notebook.teamA/Child"])

    def test_an_ambiguous_name_is_reported_rather_than_guessed(self):
        """Two folders both hold a `Child`: the reference is ambiguous in
        the export itself, so choosing one would be a confident wrong edge.
        It falls back to the unqualified form, which matches no asset and
        so is reported as dangling."""
        manifest = self._manifest()
        notebooks = manifest["sources"]["notebook"]["items"]["notebooks"]
        notebooks.append(dict(notebooks[0], logical_id="n3", folder="teamB"))
        plan = build_plan(manifest)
        parent = next(a for a in plan["assets"] if a["id"] == "notebook.Parent")
        self.assertEqual(parent["depends_on"], [])
        self.assertIn("notebook.Child", plan["warnings"]["dangling_depends_on"])


class ShortcutSchemaTests(unittest.TestCase):
    """`Tables/sales` says the shortcut is the table `sales.regional_claims`.

    The scanner read the schema, the manifest recorded it, and the plan
    dropped it: the target came out `SalesLake.regional_claims`, which is
    also what a top-level `Tables/regional_claims` produces.
    """

    def _assets(self, path):
        manifest = {"sources": {"lakehouse": {"summary": {}, "items": {
            "lakehouses": [{"name": "SalesLake", "logical_id": "l", "folder": "",
                            "tracking": {"shortcuts": "tracked"},
                            "shortcut_count": 1,
                            "shortcuts": [{
                                "name": "regional_claims",
                                "section": path.split("/")[0],
                                "schema": (path.split("/") + [""])[1],
                                "path": path,
                                "table_name": "sales.regional_claims",
                                "target_type": "AmazonS3",
                                "target": "s3://a/c", "bucket": "",
                                "external": True}]}]}}},
            "resolved_catalog": {"tables": {}}}
        return {a["id"]: a for a in build_plan(manifest,
                                               oci_namespace="acmens")["assets"]}

    def test_the_schema_reaches_the_aidp_schema_name(self):
        asset = self._assets("Tables/sales")[
            "lakehouse.shortcut.SalesLake.Tables/sales/regional_claims"]
        self.assertEqual(asset["target"]["schema"], "SalesLake_sales")

    def test_a_schemaless_shortcut_keeps_the_bare_lakehouse_schema(self):
        asset = self._assets("Tables")[
            "lakehouse.shortcut.SalesLake.Tables/regional_claims"]
        self.assertEqual(asset["target"]["schema"], "SalesLake")

    def test_the_path_and_table_name_reach_the_asset(self):
        asset = self._assets("Tables/sales")[
            "lakehouse.shortcut.SalesLake.Tables/sales/regional_claims"]
        self.assertEqual(asset["source"]["path"], "Tables/sales")
        self.assertEqual(asset["source"]["schema"], "sales")
        self.assertEqual(asset["source"]["table_name"], "sales.regional_claims")


class FilesShortcutTargetTests(unittest.TestCase):
    """`Files/adls_landing` is a folder of objects. There is no table there.

    It was planned as `aidp_dcat_external_table`, which promised the
    operator a table the migration cannot produce and AIDP has nothing to
    register. The location is real and this tool already maps it, so that
    is what the target says it is.
    """

    def _assets(self, section):
        manifest = {"sources": {"lakehouse": {"summary": {}, "items": {
            "lakehouses": [{"name": "SalesLake", "logical_id": "l", "folder": "",
                            "tracking": {"shortcuts": "tracked"},
                            "shortcut_count": 1,
                            "shortcuts": [{
                                "name": "adls_landing", "section": section,
                                "schema": "", "path": section,
                                "table_name": None,
                                "target_type": "AdlsGen2",
                                "target": "https://a.dfs.core.windows.net/l",
                                "bucket": "", "external": True}]}]}}},
            "resolved_catalog": {"tables": {}}}
        return {a["id"]: a for a in build_plan(manifest,
                                               oci_namespace="acmens")["assets"]}

    def test_a_files_shortcut_is_not_planned_as_a_table(self):
        asset = self._assets("Files")[
            "lakehouse.shortcut.SalesLake.Files/adls_landing"]
        self.assertEqual(asset["target"]["type"], "aidp_object_storage_location")
        self.assertNotIn("create_aidp_external_table", asset["transform_chain"])

    def test_a_tables_shortcut_still_is(self):
        asset = self._assets("Tables")[
            "lakehouse.shortcut.SalesLake.Tables/adls_landing"]
        self.assertEqual(asset["target"]["type"], "aidp_dcat_external_table")
        self.assertIn("create_aidp_external_table", asset["transform_chain"])


class WarehouseOrderingTests(unittest.TestCase):
    """A view over a table has to be created after it.

    `_warehouse_assets` set `"depends_on": []` as a literal, so no
    warehouse object was ever ordered after another: a plan with
    `dbo.a_top` over `dbo.z_mid` over `dbo.base` emitted a_top first,
    which is the order the plan's own docstring promises it does not
    produce.
    """

    def _manifest(self, objects):
        return {"sources": {"warehouse": {"summary": {}, "items": {
            "warehouses": [{"name": "DW", "logical_id": "w", "folder": "",
                            "objects": objects}]}}},
            "resolved_catalog": {"tables": {}}}

    @staticmethod
    def _obj(kind, name, sql, reads, schema="dbo"):
        return {"kind": kind, "schema": schema, "name": name,
                "file": f"{name}.sql", "sql": sql, "reads": reads}

    def _plan(self, objects):
        return build_plan(self._manifest(objects))

    def test_a_view_depends_on_the_table_it_selects_from(self):
        plan = self._plan([
            self._obj("table", "base", "CREATE TABLE dbo.base (id INT)", []),
            self._obj("view", "v", "CREATE VIEW dbo.v AS SELECT * FROM dbo.base",
                      ["dbo.base"])])
        view = next(a for a in plan["assets"]
                    if a["id"] == "warehouse.DW.view.dbo.v")
        self.assertEqual(view["depends_on"], ["warehouse.DW.table.dbo.base"])

    def test_the_producer_is_emitted_first(self):
        """Named so alphabetical order is the wrong order."""
        plan = self._plan([
            self._obj("view", "a_top", "SELECT * FROM dbo.z_mid", ["dbo.z_mid"]),
            self._obj("view", "z_mid", "SELECT * FROM dbo.base", ["dbo.base"]),
            self._obj("table", "base", "CREATE TABLE dbo.base (id INT)", [])])
        order = [a["id"] for a in plan["assets"]]
        self.assertLess(order.index("warehouse.DW.table.dbo.base"),
                        order.index("warehouse.DW.view.dbo.z_mid"))
        self.assertLess(order.index("warehouse.DW.view.dbo.z_mid"),
                        order.index("warehouse.DW.view.dbo.a_top"))

    def test_an_unqualified_reference_resolves_to_the_default_schema(self):
        plan = self._plan([
            self._obj("table", "base", "CREATE TABLE dbo.base (id INT)", []),
            self._obj("view", "v", "SELECT * FROM base", ["base"])])
        view = next(a for a in plan["assets"]
                    if a["id"] == "warehouse.DW.view.dbo.v")
        self.assertEqual(view["depends_on"], ["warehouse.DW.table.dbo.base"])

    def test_a_reference_to_another_database_is_not_an_edge(self):
        plan = self._plan([
            self._obj("view", "v", "SELECT * FROM Other.dbo.base",
                      ["Other.dbo.base"])])
        view = next(a for a in plan["assets"]
                    if a["id"] == "warehouse.DW.view.dbo.v")
        self.assertEqual(view["depends_on"], [])
        self.assertEqual(plan["warnings"]["dangling_depends_on"], [])

    def test_a_self_reference_is_not_an_edge(self):
        """A procedure that writes the table it reads is one node, not a
        cycle of one."""
        plan = self._plan([
            self._obj("table", "t", "CREATE TABLE dbo.t (id INT)", ["dbo.t"])])
        asset = plan["assets"][0]
        self.assertEqual(asset["depends_on"], [])
        self.assertEqual(plan["warnings"]["cycles"], [])

    def test_two_objects_that_read_each_other_are_reported_as_a_cycle(self):
        """Fed to `_strongly_connected` like any other edge, so the plan
        reports it and orders them last rather than refusing."""
        plan = self._plan([
            self._obj("view", "a", "SELECT * FROM dbo.b", ["dbo.b"]),
            self._obj("view", "b", "SELECT * FROM dbo.a", ["dbo.a"])])
        self.assertEqual(plan["warnings"]["cycles"],
                         ["warehouse.DW.view.dbo.a", "warehouse.DW.view.dbo.b"])

    def test_a_bare_name_two_schemas_both_claim_is_left_alone(self):
        plan = self._plan([
            self._obj("table", "helper", "CREATE TABLE dbo.helper (i INT)", []),
            self._obj("table", "helper", "CREATE TABLE sales.helper (i INT)", [],
                      schema="sales"),
            self._obj("view", "v", "SELECT * FROM sales.helper", ["helper"])])
        view = next(a for a in plan["assets"]
                    if a["id"] == "warehouse.DW.view.dbo.v")
        self.assertEqual(view["depends_on"], ["warehouse.DW.table.dbo.helper"])

    def test_an_object_in_another_warehouse_is_not_an_edge(self):
        manifest = {"sources": {"warehouse": {"summary": {}, "items": {
            "warehouses": [
                {"name": "DW", "logical_id": "w1", "folder": "", "objects": [
                    self._obj("view", "v", "SELECT * FROM dbo.base", ["dbo.base"])]},
                {"name": "Other", "logical_id": "w2", "folder": "", "objects": [
                    self._obj("table", "base", "CREATE TABLE dbo.base (i INT)", [])]},
            ]}}}, "resolved_catalog": {"tables": {}}}
        plan = build_plan(manifest)
        view = next(a for a in plan["assets"]
                    if a["id"] == "warehouse.DW.view.dbo.v")
        self.assertEqual(view["depends_on"], [])


class NotebookEdgeReachTests(unittest.TestCase):
    """The plan has to carry what the inventory found.

    `runMultiple` launches children exactly as `%run` does, and an
    `unresolved` reference exists so "the report can tell a reviewer that
    a dependency exists but its name could not be determined" -- the
    words are `edges.py`'s own. Neither reached the plan: `grep
    unresolved fabric_aidp/plan/planner.py` found two comments and no
    code.
    """

    def _manifest(self, edges):
        return {"sources": {"notebook": {"summary": {}, "items": {"notebooks": [
            {"name": "Child", "logical_id": "n1", "folder": "",
             "language": "python", "content": "", "default_lakehouse": None,
             "edges": {"run": [], "notebook_run": [], "run_multiple": [],
                       "unresolved": []}},
            {"name": "Parent", "logical_id": "n2", "folder": "",
             "language": "python", "content": "", "default_lakehouse": None,
             "edges": edges}]}}},
            "resolved_catalog": {"tables": {}}}

    def _parent(self, edges):
        plan = build_plan(self._manifest(edges))
        return next(a for a in plan["assets"] if a["id"] == "notebook.Parent")

    def test_a_run_multiple_edge_becomes_a_dependency(self):
        parent = self._parent({"run": [], "notebook_run": [],
                               "run_multiple": ["Child"], "unresolved": []})
        self.assertEqual(parent["depends_on"], ["notebook.Child"])

    def test_an_unresolved_reference_reaches_the_asset(self):
        parent = self._parent({"run": [], "notebook_run": [],
                               "run_multiple": [], "unresolved": ["nb"]})
        self.assertEqual(parent["source"]["unresolved_refs"], ["nb"])

    def test_an_unresolved_reference_is_not_a_dependency(self):
        """It names nothing, so there is no edge to draw -- which is why
        it has to be reported instead."""
        parent = self._parent({"run": [], "notebook_run": [],
                               "run_multiple": [], "unresolved": ["nb"]})
        self.assertEqual(parent["depends_on"], [])

    def test_a_notebook_with_no_unresolved_reference_says_so(self):
        parent = self._parent({"run": [], "notebook_run": [],
                               "run_multiple": [], "unresolved": []})
        self.assertEqual(parent["source"]["unresolved_refs"], [])


class PipelineContentErrorTests(unittest.TestCase):
    """The planner carried `schedules_error` and dropped `content_error`.

    One error field through, the other away, and the one it dropped is
    the one that decides whether the report says "empty" or "could not be
    read".
    """

    def _asset(self, **over):
        row = {"name": "Broken", "logical_id": "p", "folder": "",
               "activities": [], "notebook_refs": [], "pipeline_refs": []}
        row.update(over)
        manifest = {"sources": {"pipeline": {"summary": {}, "items": {
            "pipelines": [row]}}}, "resolved_catalog": {"tables": {}}}
        return build_plan(manifest)["assets"][0]

    def test_the_content_error_reaches_the_asset(self):
        asset = self._asset(content_error="cannot read pipeline-content.json")
        self.assertEqual(asset["source"]["content_error"],
                         "cannot read pipeline-content.json")

    def test_a_readable_pipeline_carries_an_empty_one(self):
        self.assertEqual(self._asset()["source"]["content_error"], "")

    def test_the_schedules_error_still_travels_beside_it(self):
        asset = self._asset(content_error="a", schedules_error="b")
        self.assertEqual((asset["source"]["content_error"],
                          asset["source"]["schedules_error"]), ("a", "b"))


class NotebookTableOrderingTests(unittest.TestCase):
    """A reader was planned before the notebook that writes its table.

    Two notebooks linked only by `agg_claims`, named so alphabetical
    order is the wrong order, came out

      0  notebook.a_reader   depends_on= []
      1  notebook.z_writer   depends_on= []

    `extract_written_tables` already found `saveAsTable`, so the writes
    were known; they just never became edges.
    """

    @staticmethod
    def _nb(name, reads=(), writes=()):
        return {"name": name, "logical_id": name, "folder": "",
                "language": "python", "content": "", "default_lakehouse": None,
                "edges": {"run": [], "notebook_run": [], "run_multiple": [],
                          "unresolved": []},
                "reads": list(reads), "writes": list(writes)}

    def _plan(self, notebooks):
        return build_plan({"sources": {"notebook": {"summary": {}, "items": {
            "notebooks": notebooks}}}, "resolved_catalog": {"tables": {}}})

    def test_the_reader_depends_on_the_writer(self):
        plan = self._plan([self._nb("a_reader", reads=["agg_claims"]),
                           self._nb("z_writer", writes=["agg_claims"])])
        reader = next(a for a in plan["assets"] if a["id"] == "notebook.a_reader")
        self.assertEqual(reader["depends_on"], ["notebook.z_writer"])

    def test_the_writer_is_emitted_first(self):
        plan = self._plan([self._nb("a_reader", reads=["agg_claims"]),
                           self._nb("z_writer", writes=["agg_claims"])])
        order = [a["id"] for a in plan["assets"]]
        self.assertLess(order.index("notebook.z_writer"),
                        order.index("notebook.a_reader"))

    def test_a_qualified_read_matches_a_bare_write(self):
        """`catalog.py` resolves references by dotted suffix; so does this."""
        plan = self._plan([self._nb("reader", reads=["SalesLake.agg_claims"]),
                           self._nb("writer", writes=["agg_claims"])])
        reader = next(a for a in plan["assets"] if a["id"] == "notebook.reader")
        self.assertEqual(reader["depends_on"], ["notebook.writer"])

    def test_a_different_schema_is_a_different_table(self):
        plan = self._plan([self._nb("reader", reads=["sales.claims"]),
                           self._nb("writer", writes=["dbo.claims"])])
        reader = next(a for a in plan["assets"] if a["id"] == "notebook.reader")
        self.assertEqual(reader["depends_on"], [])

    def test_a_notebook_that_reads_what_it_writes_has_no_self_edge(self):
        plan = self._plan([self._nb("both", reads=["agg"], writes=["agg"])])
        self.assertEqual(plan["assets"][0]["depends_on"], [])
        self.assertEqual(plan["warnings"]["cycles"], [])

    def test_two_writers_both_become_edges(self):
        """A reader has to come after both; dropping the ambiguity would
        drop a real ordering constraint."""
        plan = self._plan([self._nb("reader", reads=["agg"]),
                           self._nb("w1", writes=["agg"]),
                           self._nb("w2", writes=["agg"])])
        reader = next(a for a in plan["assets"] if a["id"] == "notebook.reader")
        self.assertEqual(reader["depends_on"],
                         ["notebook.w1", "notebook.w2"])

    def test_two_notebooks_that_feed_each_other_are_reported_as_a_cycle(self):
        plan = self._plan([self._nb("a", reads=["x"], writes=["y"]),
                           self._nb("b", reads=["y"], writes=["x"])])
        self.assertEqual(plan["warnings"]["cycles"],
                         ["notebook.a", "notebook.b"])

    def test_a_read_nobody_writes_is_not_a_dangling_edge(self):
        plan = self._plan([self._nb("reader", reads=["somewhere_else"])])
        self.assertEqual(plan["assets"][0]["depends_on"], [])
        self.assertEqual(plan["warnings"]["dangling_depends_on"], [])

    def test_a_manifest_with_no_reads_or_writes_still_plans(self):
        """An older manifest has neither key."""
        row = self._nb("plain")
        del row["reads"], row["writes"]
        self.assertEqual(self._plan([row])["assets"][0]["depends_on"], [])


class NamespacePlaceholderSummaryTests(unittest.TestCase):
    """`plan` with no `--namespace` silently produced a plan whose every
    oci:// path was a placeholder, and said nothing about it."""

    def _summary(self, **kw):
        return summarize_plan(build_plan({"workspace_name": "w"}, **kw))

    def test_the_placeholder_is_called_out(self):
        text = self._summary()
        self.assertIn("<your-oci-namespace>", text)
        self.assertIn("--namespace", text)

    def test_a_real_namespace_says_nothing(self):
        self.assertNotIn("<your-oci-namespace>", self._summary(oci_namespace="acmens"))


class TopologicalOrderTests(unittest.TestCase):
    """The ordering promise itself, as an invariant rather than a sample.

    `plan`'s whole reason to order assets is that a producer must precede
    its consumers, and this file checked that with hand-picked pairs --
    `base` before `z_mid`, `z_writer` before `a_reader`. Each of those
    catches the one edge it names. None of them says the property holds,
    so a `_ordered` that got some other edge backwards passed.

    The property: for every asset, every dependency still in the plan
    appears at a strictly lower index. The two documented exceptions are
    the assets `_ordered` could not place -- members of a cycle, and
    assets downstream of one -- which the planner appends last and names
    in `warnings.cycles` and `warnings.blocked_by_cycle`.

    WHAT THE REAL ESTATE CAN AND CANNOT SHOW. MEASURED 2026-09-29:
    the Acme estate carries 28 assets and 15 dependency edges, the staged
    demo input 106 assets and the same 15 edges -- and in *both*, 0 of the
    15 run against alphabetical order. So replacing `_ordered`'s result
    with `sorted(...)` leaves the bundled-estate test green. It is a real
    invariant over real data and it is not, by itself, a regression guard;
    `test_a_chain_named_against_the_order_it_needs` is the one that fails
    on that break, which is also why the older tests in this file pick
    names like `z_writer` and `a_reader`.
    """

    @staticmethod
    def _object(kind, name, reads):
        return {"kind": kind, "schema": "dbo", "name": name,
                "file": f"{name}.sql", "reads": reads,
                "sql": (f"CREATE TABLE dbo.{name} (i INT)" if kind == "table"
                        else f"CREATE VIEW dbo.{name} AS SELECT * FROM "
                             + ", ".join(reads))}

    @staticmethod
    def _warehouse(objects):
        return {"sources": {"warehouse": {"summary": {}, "items": {
            "warehouses": [{"name": "DW", "logical_id": "w", "folder": "",
                            "objects": objects}]}}},
            "resolved_catalog": {"tables": {}}}

    def _assert_topological(self, plan, label):
        assets = plan["assets"]
        index = {asset["id"]: i for i, asset in enumerate(assets)}
        unplaceable = (set(plan["warnings"]["cycles"])
                       | set(plan["warnings"]["blocked_by_cycle"]))
        self.assertTrue(assets, f"{label}: the plan has no assets to order")
        checked = 0
        for asset in assets:
            if asset["id"] in unplaceable:
                continue
            for dependency in asset["depends_on"]:
                if dependency in unplaceable:
                    continue
                self.assertIn(
                    dependency, index,
                    f"{label}: {asset['id']} depends on {dependency}, which "
                    f"is not in the plan -- a dangling edge should have been "
                    f"filtered out before ordering")
                checked += 1
                self.assertLess(
                    index[dependency], index[asset["id"]],
                    f"{label}: {asset['id']} is ordered before its "
                    f"dependency {dependency}, so a consumer precedes its "
                    f"producer")
        return checked

    def test_the_bundled_estate_is_topologically_ordered(self):
        """Real data, every edge, rather than a chosen pair. Weak on its
        own -- see the class docstring for the measurement -- but it is
        the only check here that grows with the fixture: an edge the
        estate gains later that does run against alphabetical order is
        covered the day it lands, with nobody having to add a test."""
        from fabric_aidp.fixtures import demo_workspace_path
        from fabric_aidp.inventory.manifest import build_manifest
        from fabric_aidp.sources import ALL_SOURCES

        plan = build_plan(build_manifest(demo_workspace_path(), ALL_SOURCES),
                          oci_namespace="acmens")
        edges = self._assert_topological(plan, "bundled estate")
        self.assertGreater(edges, 0, "the bundled estate has no edges to order")

    def test_the_synthetic_manifest_is_topologically_ordered(self):
        self._assert_topological(build_plan(_manifest(), oci_namespace="ns"),
                                 "synthetic manifest")

    def test_a_chain_named_against_the_order_it_needs(self):
        """Alphabetical order is the wrong order at every link, so a sort
        that quietly fell back to it fails here rather than at one edge."""
        plan = build_plan(self._warehouse([
            self._object("view", "a_fourth", ["dbo.b_third"]),
            self._object("view", "b_third", ["dbo.c_second"]),
            self._object("view", "c_second", ["dbo.d_first"]),
            self._object("table", "d_first", []),
        ]), oci_namespace="ns")
        self.assertEqual(self._assert_topological(plan, "chain"), 3)
        self.assertEqual(
            [a["id"] for a in plan["assets"]],
            ["warehouse.DW.table.dbo.d_first", "warehouse.DW.view.dbo.c_second",
             "warehouse.DW.view.dbo.b_third", "warehouse.DW.view.dbo.a_fourth"])

    def test_a_cycle_does_not_break_the_invariant_for_everything_else(self):
        """Two views referencing each other cannot both come first. The
        planner names them and orders them last, and every asset outside
        the cycle still satisfies the property."""
        plan = build_plan(self._warehouse([
            self._object("table", "base", []),
            self._object("view", "left", ["dbo.right_v"]),
            self._object("view", "right_v", ["dbo.left"]),
            self._object("view", "downstream", ["dbo.left"]),
        ]), oci_namespace="ns")
        self.assertTrue(plan["warnings"]["cycles"],
                        "the planner did not detect the cycle")
        self._assert_topological(plan, "cycle")
        # and the unplaceable ones really are last
        order = [a["id"] for a in plan["assets"]]
        unplaceable = (set(plan["warnings"]["cycles"])
                       | set(plan["warnings"]["blocked_by_cycle"]))
        placed = [i for i, a in enumerate(order) if a not in unplaceable]
        self.assertEqual(placed, list(range(len(order) - len(unplaceable))))


if __name__ == "__main__":
    unittest.main()
