"""Resolving the lakehouse GUID a Dataflow reads from and writes to.

`migrate/runner.py` read `plan.get("lakehouse_catalog")` and nothing ever
wrote it, so it was always None: every Dataflow read and every Dataflow
write took the unresolved branch and emitted a two-part
`<catalog>.<table>` name, flagged "lakehouse GUID ... is not in the
catalog" -- wording that invites the operator to go and add it to a
catalog that has no such field.

The export does name a lakehouse GUID, in exactly one place. Fabric writes
a notebook's binding as a pair:

    "dependencies": {"lakehouse": {
        "default_lakehouse": "2925655f-0293-4f32-8bc6-86ab989099a7",
        "default_lakehouse_name": "SomeLakehouse", ...}}

Both halves, written by Fabric, in the same object -- and
`default_lakehouse` is the workspace item id, the same identifier M's
`lakehouseId` navigates by. Two such pairs exist in the shipped real
notebook corpus. The notebook reader was throwing the GUID away whenever a
name was present, so the one authoritative mapping in the export never
reached anything.

What is deliberately NOT used: the Lakehouse item's `.platform`
`config.logicalId`. That is a git-generated identifier, not the workspace
item id, and across every Lakehouse item available here (3) it matches
none of the 29 distinct lakehouseIds the Dataflows navigate to. Treating a
match there as a resolution would be dressing a coincidence as an answer.
"""
import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory.manifest import build_manifest
from fabric_aidp.plan.planner import build_plan, load_supplied_lakehouses
from fabric_aidp.translate import m_to_pyspark as m
from fabric_aidp.translate.notebook_format import parse_any

GUID = "2925655f-0293-4f32-8bc6-86ab989099a7"

NOTEBOOK = (
    "# Fabric notebook source\n\n"
    "# METADATA ********************\n\n"
    "# META {\n"
    '# META   "dependencies": { "lakehouse": {\n'
    f'# META     "default_lakehouse": "{GUID}",\n'
    '# META     "default_lakehouse_name": "SomeLakehouse",\n'
    '# META     "default_lakehouse_workspace_id": "ws-1" } }\n'
    "# META }\n\n"
    "# CELL ********************\n\n"
    "df = spark.table('claim')\n")

# A notebook bound by GUID alone -- Fabric writes this when the item has
# been deleted or the export predates the name field.
NOTEBOOK_NO_NAME = NOTEBOOK.replace(
    '# META     "default_lakehouse_name": "SomeLakehouse",\n', "")

MASHUP = f'''section Section1;
[DataDestinations = {{[Definition = [Kind = "Reference", \
QueryName = "T_DataDestination", IsNewTarget = true], \
Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}}]
shared T = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{{[workspaceId = "ws-1"]}}[Data],
  #"Navigation 1" = Navigation{{[lakehouseId = "{GUID}"]}}[Data],
  #"Navigation 2" = #"Navigation 1"{{[Id = "claim", ItemKind = "Table"]}}[Data]
in
  #"Navigation 2";
shared T_DataDestination = let
  Pattern = Lakehouse.Contents([EnableFolding = false]),
  Navigation_1 = Pattern{{[workspaceId = "ws-1"]}}[Data],
  Navigation_2 = Navigation_1{{[lakehouseId = "{GUID}"]}}[Data],
  TableNavigation = Navigation_2{{[Id = "dim_claim", ItemKind = "Table"]}}?[Data]?
in
  TableNavigation;
'''


def _item(root, dirname, kind, display, files):
    path = root / dirname
    path.mkdir(parents=True)
    (path / ".platform").write_text(json.dumps({
        "metadata": {"type": kind, "displayName": display},
        "config": {"logicalId": f"id-{display}"}}), encoding="utf-8")
    for name, body in files.items():
        (path / name).write_text(body, encoding="utf-8")


def _workspace(root, notebook=NOTEBOOK):
    _item(root, "Bind.Notebook", "Notebook", "Bind",
          {"notebook-content.py": notebook})
    _item(root, "Sales.Dataflow", "Dataflow", "Sales",
          {"mashup.pq": MASHUP, "queryMetadata.json": '{"queriesMetadata": {}}'})
    return root


class NotebookBindingTests(unittest.TestCase):
    """The GUID has to survive the notebook reader, which discarded it."""

    def test_the_guid_is_kept_alongside_the_name(self):
        nb = parse_any(NOTEBOOK)
        self.assertEqual(nb.default_lakehouse, "SomeLakehouse")
        self.assertEqual(nb.default_lakehouse_id, GUID)

    def test_a_binding_with_no_name_still_reports_its_guid(self):
        nb = parse_any(NOTEBOOK_NO_NAME)
        self.assertEqual(nb.default_lakehouse_id, GUID)
        # `default_lakehouse` falls back to the GUID when there is no name;
        # that is a display value, not a second mapping.
        self.assertEqual(nb.default_lakehouse, GUID)

    def test_an_unbound_notebook_has_no_guid(self):
        self.assertIsNone(parse_any(
            "# Fabric notebook source\n\n# CELL ********************\n\nx = 1\n"
        ).default_lakehouse_id)

    def test_the_scan_records_it(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "ws"
            _workspace(root)
            manifest = build_manifest(root, ("notebook",))
        nb = manifest["sources"]["notebook"]["items"]["notebooks"][0]
        self.assertEqual(nb["default_lakehouse_id"], GUID)


class PlanLakehouseCatalogTests(unittest.TestCase):
    def _plan(self, notebook=NOTEBOOK):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "ws"
            _workspace(root, notebook)
            return build_plan(build_manifest(root, ("notebook", "dataflow")),
                              oci_namespace="acmens")

    def test_the_plan_carries_the_key_the_runner_reads(self):
        # `migrate/runner.py` has read `plan["lakehouse_catalog"]` since
        # Dataflows landed. Nothing wrote it.
        self.assertEqual(self._plan()["lakehouse_catalog"],
                         {GUID: "SomeLakehouse"})

    def test_a_binding_with_no_name_contributes_nothing(self):
        # `{guid: guid}` would put the GUID in the *schema* position of a
        # table name -- a confident-looking name built from the thing we
        # could not resolve.
        self.assertEqual(self._plan(NOTEBOOK_NO_NAME)["lakehouse_catalog"], {})

    def test_an_export_with_no_notebook_binding_gets_an_empty_map(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "ws"
            _item(root, "Sales.Dataflow", "Dataflow", "Sales",
                  {"mashup.pq": MASHUP,
                   "queryMetadata.json": '{"queriesMetadata": {}}'})
            plan = build_plan(build_manifest(root, ("dataflow",)),
                              oci_namespace="acmens")
        self.assertEqual(plan["lakehouse_catalog"], {})

    def test_the_lakehouse_item_logical_id_is_not_used(self):
        # `.platform` logicalId is a git identifier, not the workspace item
        # id M navigates by. Resolving on it would be a guess.
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "ws"
            _item(root, "Sales.Dataflow", "Dataflow", "Sales",
                  {"mashup.pq": MASHUP,
                   "queryMetadata.json": '{"queriesMetadata": {}}'})
            _item(root, "Some.Lakehouse", "Lakehouse", "SomeLakehouse", {})
            plan = build_plan(build_manifest(root, ("dataflow", "lakehouse")),
                              oci_namespace="acmens")
        self.assertEqual(plan["lakehouse_catalog"], {})


class SuppliedLakehouseMapTests(unittest.TestCase):
    """`plan --lakehouses`: the operator naming what the export cannot.

    CONFIRMED before this existed: nothing on any verb mapped a lakehouse
    GUID to a name. `plan --catalog` is the AIDP catalog a table name is
    built under and `inventory --tables-csv` lists tables; neither is this,
    and `m_to_pyspark.unresolved_lakehouse` said so in its own docstring
    while offering the operator no way to fix it. MEASURED on the bundled
    demo estate: 3 findings cite an unresolved lakehouse GUID, and the only
    remedy the advice could give was "export the whole workspace so that
    binding is present" -- which is not always possible, and is not
    something the person running the tool can always do.

    This is the same shape as `--tables-csv`, which exists for exactly this
    reason: the export cannot say, so let the operator say. A GUID -> name
    map is a fact about the tenant, not a guess the tool makes.
    """

    OTHER = "33333333-3333-3333-3333-333333333333"

    def _csv(self, body):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "lakehouses.csv"
        path.write_text(body, encoding="utf-8")
        return path

    def _plan(self, body, notebook=NOTEBOOK):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "ws"
            _workspace(root, notebook)
            return build_plan(build_manifest(root, ("notebook", "dataflow")),
                              oci_namespace="acmens",
                              lakehouses=load_supplied_lakehouses(self._csv(body)))

    def test_a_supplied_name_reaches_the_plan(self):
        plan = self._plan(f"id,name\n{self.OTHER},Analytics\n",
                          notebook=NOTEBOOK_NO_NAME)
        self.assertEqual(plan["lakehouse_catalog"], {self.OTHER: "Analytics"})

    def test_it_resolves_a_guid_the_export_never_names(self):
        """The case the flag is for: no notebook is bound to the lakehouse
        the Dataflow reads, so the export cannot name it and only the
        operator can."""
        plan = self._plan(f"id,name\n{GUID},Analytics\n",
                          notebook=NOTEBOOK_NO_NAME)
        self.assertEqual(plan["lakehouse_catalog"], {GUID: "Analytics"})

    def test_it_merges_with_what_the_export_did_name(self):
        plan = self._plan(f"id,name\n{self.OTHER},Analytics\n")
        self.assertEqual(plan["lakehouse_catalog"],
                         {GUID: "SomeLakehouse", self.OTHER: "Analytics"})

    def test_the_flag_the_operator_typed_beats_the_export(self):
        """`cli._namespace` already states the rule this follows: a flag
        someone typed beats ambient configuration. They know their tenant;
        a Git export can be stale or partial."""
        plan = self._plan(f"id,name\n{GUID},Renamed\n")
        self.assertEqual(plan["lakehouse_catalog"], {GUID: "Renamed"})

    def test_a_row_naming_a_lakehouse_after_its_own_guid_is_dropped(self):
        """`{guid: guid}` splices the unresolved thing into the schema
        position of a table name and makes an unknown look answered -- the
        same reason the notebook binding drops it."""
        self.assertEqual(
            load_supplied_lakehouses(self._csv(f"id,name\n{GUID},{GUID}\n")), {})

    def test_a_row_with_no_name_is_dropped(self):
        self.assertEqual(
            load_supplied_lakehouses(self._csv(f"id,name\n{GUID},\n")), {})

    def test_a_capitalised_header_is_read(self):
        """`--tables-csv` shipped broken for exactly this: `Table` passed
        validation and then matched no row, so the flag silently did
        nothing. Same file format, same folding."""
        self.assertEqual(
            load_supplied_lakehouses(self._csv(f"Id,Name\n{GUID},Analytics\n")),
            {GUID: "Analytics"})

    def test_a_file_with_no_id_column_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            load_supplied_lakehouses(self._csv("guid,name\na,b\n"))
        self.assertIn("'id'", str(caught.exception))

    def test_a_file_with_no_name_column_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            load_supplied_lakehouses(self._csv("id,lakehouse\na,b\n"))
        self.assertIn("'name'", str(caught.exception))

    def test_a_file_that_yields_nothing_is_refused_rather_than_ignored(self):
        """The operator handed this a map and got no map. Silence is how
        `--tables-csv` did nothing for a release."""
        with self.assertRaises(ValueError) as caught:
            load_supplied_lakehouses(self._csv("id;name\na;b\n"))
        self.assertIn("'id'", str(caught.exception))

    def test_an_unreadable_file_names_itself(self):
        with self.assertRaises(ValueError) as caught:
            load_supplied_lakehouses("/nonexistent/lakehouses.csv")
        self.assertIn("lakehouses.csv", str(caught.exception))

    def test_the_cli_accepts_the_flag_and_it_reaches_the_plan(self):
        from fabric_aidp.cli import main
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _workspace(root / "ws", NOTEBOOK_NO_NAME)
            csv_path = root / "lh.csv"
            csv_path.write_text(f"id,name\n{GUID},Analytics\n", encoding="utf-8")
            inv, plan_path = root / "inv.json", root / "plan.json"
            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(["inventory", str(root / "ws"), "-o", str(inv)]), 0)
                self.assertEqual(
                    main(["plan", str(inv), "-o", str(plan_path),
                          "--namespace", "acmens",
                          "--lakehouses", str(csv_path)]), 0)
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
        self.assertEqual(plan["lakehouse_catalog"], {GUID: "Analytics"})

    def test_a_supplied_name_removes_the_unresolved_flag_end_to_end(self):
        """The point of the whole thing. Without the map the Dataflow's read
        and write are two-part names flagged for a human; with it they are
        three-part and the findings are rewrites.

        Node-gated, like every other end-to-end M test here.
        """
        from fabric_aidp.translate import m_parser
        if not m_parser.parser_available():
            raise unittest.SkipTest("Node + mparse not installed")
        from fabric_aidp.migrate.runner import migrate

        def run(lakehouses):
            with TemporaryDirectory() as tmp:
                root = Path(tmp) / "ws"
                _workspace(root, NOTEBOOK_NO_NAME)
                plan = build_plan(
                    build_manifest(root, ("notebook", "dataflow")),
                    oci_namespace="acmens", lakehouses=lakehouses)
                out = Path(tmp) / "m"
                report = migrate(plan, out_dir=out)
                row = next(r for r in report["results"]
                           if r["asset_id"] == "dataflow.Sales.T")
                return (out / row["output_path"]).read_text(encoding="utf-8"), row

        body, row = run(None)
        self.assertIn('spark.table("default.claim")', body)
        self.assertIn(("M10_SOURCE_LAKEHOUSE", "flag"),
                      [(f["rule"], f["severity"]) for f in row["findings"]])

        body, row = run({GUID: "Analytics"})
        self.assertIn('spark.table("default.Analytics.claim")', body)
        self.assertIn('saveAsTable("default.Analytics.dim_claim")', body)
        rules = [(f["rule"], f["severity"]) for f in row["findings"]]
        self.assertIn(("M10_SOURCE_LAKEHOUSE", "rewrite"), rules)
        self.assertNotIn(("M10_SOURCE_LAKEHOUSE", "flag"), rules)


class ResolvedWriteTests(unittest.TestCase):
    """End to end: a resolvable GUID gives a three-part name and no flag."""

    @classmethod
    def setUpClass(cls):
        from fabric_aidp.translate import m_parser
        if not m_parser.parser_available():
            raise unittest.SkipTest("Node + mparse not installed")
        from fabric_aidp.migrate.runner import migrate
        cls._tmp = TemporaryDirectory()
        root = Path(cls._tmp.name) / "ws"
        _workspace(root)
        plan = build_plan(build_manifest(root, ("notebook", "dataflow")),
                          oci_namespace="acmens")
        cls.out = Path(cls._tmp.name) / "migrated"
        cls.report = migrate(plan, out_dir=cls.out)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _row(self):
        return next(r for r in self.report["results"]
                    if r["asset_id"] == "dataflow.Sales.T")

    def test_the_write_is_fully_qualified(self):
        body = (self.out / self._row()["output_path"]).read_text(encoding="utf-8")
        self.assertIn('saveAsTable("default.SomeLakehouse.dim_claim")', body)

    def test_the_read_is_fully_qualified_too(self):
        body = (self.out / self._row()["output_path"]).read_text(encoding="utf-8")
        self.assertIn('spark.table("default.SomeLakehouse.claim")', body)

    def test_a_resolved_destination_is_a_rewrite_not_a_flag(self):
        rules = [(f["rule"], f["severity"]) for f in self._row()["findings"]]
        self.assertIn(("M12_DESTINATION", "rewrite"), rules)
        self.assertNotIn(("M12_DESTINATION", "flag"), rules)


class UnresolvedWordingTests(unittest.TestCase):
    """What the flag says when the export cannot name the lakehouse.

    "is not in the catalog" reads as something the operator forgot to
    supply. There is no catalog with a lakehouse-GUID field; the export
    either carries a notebook bound to that lakehouse or it does not.
    """

    SOURCE = [
        {"name": "Pattern", "fn": "Lakehouse.Contents", "args": ["[]"],
         "nav": [], "inputs": [], "raw": ""},
        {"name": "Nav1", "fn": "Pattern", "args": [],
         "nav": ['{[lakehouseId = "lh-1"]}', "[Data]"], "inputs": [], "raw": ""},
        {"name": "Nav2", "fn": "Nav1", "args": [],
         "nav": ['{[Id = "claim", ItemKind = "Table"]}', "[Data]"],
         "inputs": [], "raw": ""},
    ]
    ATTRS = ('[DataDestinations = {[Definition = [Kind = "Reference", '
             'QueryName = "H", IsNewTarget = true], Settings = [Kind = "Automatic"]]}]')

    def _findings(self, lakehouses=None):
        query = {"name": "Q", "attrs": self.ATTRS, "steps": list(self.SOURCE),
                 "final": "Nav2"}
        helper = {"name": "H", "attrs": None, "steps": list(self.SOURCE),
                  "final": "Nav2"}
        result = m.translate_query(query, queries_by_name={"H": helper},
                                   lakehouses=lakehouses)
        return {f.rule: f.detail for f in result.findings}

    def test_the_write_does_not_blame_a_catalog(self):
        detail = self._findings()["M12_DESTINATION"]
        self.assertNotIn("not in the catalog", detail)

    def test_the_write_names_the_guid_and_the_remedy(self):
        detail = self._findings()["M12_DESTINATION"]
        self.assertIn("lh-1", detail)
        self.assertIn("default_lakehouse_name", detail)
        self.assertIn("export the whole workspace", detail.casefold())

    def test_the_write_still_says_it_is_not_recoverable(self):
        self.assertIn("not recoverable", self._findings()["M12_DESTINATION"])

    def test_the_row_specific_fact_comes_first(self):
        # `verify` prints one line per asset, so the head of the detail is
        # what a reader sees. The shared explanation is the tail.
        self.assertTrue(self._findings()["M12_DESTINATION"].startswith("writes "))
        self.assertTrue(self._findings()["M10_SOURCE_LAKEHOUSE"].startswith("table "))

    def test_the_read_says_the_same_thing(self):
        detail = self._findings()["M10_SOURCE_LAKEHOUSE"]
        self.assertNotIn("not in the catalog", detail)
        self.assertIn("default_lakehouse_name", detail)

    def test_a_resolved_lakehouse_says_none_of_it(self):
        details = self._findings({"lh-1": "Sales"})
        self.assertNotIn("default_lakehouse_name", details["M10_SOURCE_LAKEHOUSE"])
        self.assertNotIn("default_lakehouse_name", details["M12_DESTINATION"])


if __name__ == "__main__":
    unittest.main()
