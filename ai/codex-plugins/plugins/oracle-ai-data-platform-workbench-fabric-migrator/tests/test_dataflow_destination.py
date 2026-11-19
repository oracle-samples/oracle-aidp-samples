"""Where a migrated Dataflow writes, end to end.

`translate_query` is unit-tested in test_m_rules.py; what this file guards is
the *carriage* of the destination from the export to the generated file.
Inventory reads the section attribute, the plan asset has to carry it, and
the runner has to hand it back to the translator. Drop it anywhere along
that chain and the query still translates, still grades, and silently stops
writing -- which is the defect these tests exist for.

Everything here needs the Node parser, so it skips without one; the precedent
is tests/test_m_corpus.py.
"""
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory import dataflow
from fabric_aidp.inventory.git_workspace import discover_items
from fabric_aidp.inventory.manifest import build_manifest
from fabric_aidp.migrate.runner import migrate
from fabric_aidp.plan.planner import build_plan
from fabric_aidp.translate import m_parser

# 023.pq in shape: a section-level default destination, a `DefaultDestination`
# member holding only workspace + lakehouse, and a member bound to it that
# names no table of its own.
BOUND_MASHUP = '''[DefaultOutputDestinationSettings = [DestinationDefinition = \
[Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], \
UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], \
StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false])\
{[workspaceId = "ws-1"]}[Data]{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data];
[BindToDefaultDestination = true]
shared courses = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ws-1"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "courses_raw", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
'''


def _workspace(tmp, mashup=BOUND_MASHUP, name="Enrol"):
    item = Path(tmp) / f"{name}.Dataflow"
    item.mkdir(parents=True)
    (item / ".platform").write_text(json.dumps({
        "metadata": {"type": "Dataflow", "displayName": name},
        "config": {"logicalId": "11111111-1111-1111-1111-111111111111"}}),
        encoding="utf-8")
    (item / "mashup.pq").write_text(mashup, encoding="utf-8")
    (item / "queryMetadata.json").write_text(
        json.dumps({"queriesMetadata": {}}), encoding="utf-8")
    return item


@unittest.skipUnless(m_parser.parser_available(), "Node + mparse not installed")
class DefaultDestinationCarriageTests(unittest.TestCase):
    """The section attribute has to survive inventory -> plan -> migrate."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        root = Path(cls._tmp.name) / "ws"
        _workspace(root)
        cls.manifest = build_manifest(root, ("dataflow",))
        cls.plan = build_plan(cls.manifest, oci_namespace="acmens")
        out = Path(cls._tmp.name) / "migrated"
        cls.report = migrate(cls.plan, out_dir=out)
        cls.out = out

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _row(self):
        return next(r for r in self.report["results"]
                    if r["asset_id"] == "dataflow.Enrol.courses")

    def test_the_inventory_records_the_section_attribute(self):
        flow = self.manifest["sources"]["dataflow"]["items"]["dataflows"][0]
        self.assertIn("DefaultOutputDestinationSettings", flow["section_attrs"])

    def test_the_plan_asset_carries_it(self):
        asset = next(a for a in self.plan["assets"]
                     if a["id"] == "dataflow.Enrol.courses")
        self.assertIn("DefaultOutputDestinationSettings",
                      asset["source"]["section_attrs"])

    def test_the_generated_file_writes_somewhere(self):
        row = self._row()
        self.assertIn(row["status"], ("ok", "needs_manual_review"), row)
        body = (self.out / row["output_path"]).read_text(encoding="utf-8")
        self.assertIn('.write.mode("overwrite").saveAsTable(', body)

    def test_the_table_is_named_after_the_query(self):
        body = (self.out / self._row()["output_path"]).read_text(encoding="utf-8")
        self.assertIn('saveAsTable("default.courses")', body)

    def test_the_derivation_is_reported_not_silent(self):
        self.assertIn("M17_DEFAULT_DESTINATION",
                      [f["rule"] for f in self._row()["findings"]])

    def test_dropping_the_section_attribute_refuses_rather_than_writing_nothing(self):
        """The bite-proof, as a test: an asset that lost the attribute must
        block, not emit a file that computes a result and discards it."""
        with TemporaryDirectory() as tmp:
            plan = json.loads(json.dumps(self.plan))
            for asset in plan["assets"]:
                asset["source"].pop("section_attrs", None)
            report = migrate(plan, out_dir=Path(tmp) / "out")
            row = next(r for r in report["results"]
                       if r["asset_id"] == "dataflow.Enrol.courses")
            self.assertEqual(row["status"], "blocked")
            self.assertIn("BindToDefaultDestination", str(row["findings"]))


# 009.pq in shape. The member is declared `shared #"... csv_DataDestination"`
# and the attribute spells the same name unquoted -- so a raw dict lookup on
# the attribute's spelling misses a query that is right there in the file.
QUOTED_HELPER_MASHUP = '''section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", \
QueryName = "squirrel-data csv_DataDestination", IsNewTarget = true], \
Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"squirrel-data csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ws-1"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "squirrel_raw", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"squirrel-data csv_DataDestination" = let
  Pattern = Lakehouse.Contents([EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "ws-1"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data],
  TableNavigation = Navigation_2{[Id = "squirrel_data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
'''


@unittest.skipUnless(m_parser.parser_available(), "Node + mparse not installed")
class QuotedHelperNameTests(unittest.TestCase):
    """A `#"quoted name"` destination helper must be found, not reported
    missing. 3 of the 24 corpus destination references are of this shape
    (009, 028, 030), and the demo's 009 said in full:

        M90_UNSUPPORTED_STEP: DataDestinations names
        'squirrel-data csv_DataDestination', which this export does not
        contain

    about a member declared three lines below the attribute naming it."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        root = Path(cls._tmp.name) / "ws"
        _workspace(root, QUOTED_HELPER_MASHUP, name="Squirrels")
        cls.manifest = build_manifest(root, ("dataflow",))
        cls.plan = build_plan(cls.manifest, oci_namespace="acmens")
        cls.out = Path(cls._tmp.name) / "migrated"
        cls.report = migrate(cls.plan, out_dir=cls.out)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _row(self):
        return next(r for r in self.report["results"]
                    if r["asset_id"].startswith("dataflow.Squirrels."))

    def test_the_inventory_finds_the_helper(self):
        flow = self.manifest["sources"]["dataflow"]["items"]["dataflows"][0]
        entry = next(q for q in flow["queries"] if q["kind"] == "pipeline")
        self.assertIsNotNone(
            entry["helper"],
            "the helper is in the export; looking it up by the attribute's "
            "unquoted spelling must find the quoted declaration")

    def test_it_is_not_reported_as_missing_from_the_export(self):
        self.assertNotIn("does not contain", str(self._row()["findings"]))

    def test_the_query_writes_to_the_helper_s_table(self):
        row = self._row()
        self.assertIn(row["status"], ("ok", "needs_manual_review"), row)
        body = (self.out / row["output_path"]).read_text(encoding="utf-8")
        self.assertIn('saveAsTable("default.squirrel_data")', body)


@unittest.skipUnless(m_parser.parser_available(), "Node + mparse not installed")
class SectionAttributeScanTests(unittest.TestCase):
    def _scan(self, mashup):
        with TemporaryDirectory() as tmp:
            _workspace(tmp, mashup)
            return dataflow.scan(discover_items(tmp))

    def test_a_section_with_no_attributes_records_an_empty_string(self):
        result = self._scan(
            'section Section1;\nshared q = let a = Lakehouse.Contents(null) in a;\n')
        self.assertEqual(
            result["items"]["dataflows"][0]["section_attrs"], "")


if __name__ == "__main__":
    unittest.main()
