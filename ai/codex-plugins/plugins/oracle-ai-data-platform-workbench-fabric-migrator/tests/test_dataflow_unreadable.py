"""A Dataflow nothing could be read from must still reach the report.

`_dataflow_assets` planned only `kind == "pipeline"` queries. A Dataflow
whose mashup.pq will not parse has no queries at all, and one scanned
without the Node parser has only `uncounted` ones -- so both produced zero
assets and the item vanished from plan, migrate and verify. The inventory
said "3 dataflows"; the plan said nothing, and the asset count was quietly
smaller with nothing anywhere saying why.

Same class as the `.platform` and shortcuts defects, and the same remedy:
one asset per item, reported blocked, with the reason in the finding. The
precedent is INV01_ITEM_UNREADABLE in migrate/runner.py.

None of this needs Node -- the no-parser half is the common case for anyone
who has not run `npm install`, and it must be exercised where Node *is*
installed too, so the parser is forced off rather than assumed off.
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
from fabric_aidp.verify.checker import verify

GOOD = '''section Section1;
shared T = let
  Pattern = Lakehouse.Contents([]),
  Nav = Pattern{[Id = "claim", ItemKind = "Table"]}[Data]
in
  Nav;
'''
BROKEN = "section S; shared x = let in in;"


def _workspace(tmp, mashups):
    """{display name: mashup text or None for a missing mashup.pq}."""
    for name, body in mashups.items():
        item = Path(tmp) / f"{name}.Dataflow"
        item.mkdir(parents=True)
        (item / ".platform").write_text(json.dumps({
            "metadata": {"type": "Dataflow", "displayName": name},
            "config": {"logicalId": f"id-{name}"}}), encoding="utf-8")
        if body is not None:
            (item / "mashup.pq").write_text(body, encoding="utf-8")
    return Path(tmp)


def _plan_of(root):
    return build_plan(build_manifest(root, ("dataflow",)), oci_namespace="acmens")


class NoParserTests(unittest.TestCase):
    """The common case: Node is not installed, or `npm install` was skipped."""

    def setUp(self):
        self.real = dataflow.m_parser.parser_available
        dataflow.m_parser.parser_available = lambda *a, **k: False
        self.addCleanup(setattr, dataflow.m_parser, "parser_available", self.real)
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = _workspace(self._tmp.name, {"Sales": GOOD})

    def test_the_dataflow_reaches_the_plan(self):
        plan = _plan_of(self.root)
        self.assertIn("dataflow.Sales", [a["id"] for a in plan["assets"]])

    def test_it_is_not_reported_as_migratable(self):
        plan = _plan_of(self.root)
        asset = next(a for a in plan["assets"] if a["id"] == "dataflow.Sales")
        self.assertEqual(asset["target"]["type"], "aidp_unmigrated")

    def test_migrate_blocks_it_and_says_why(self):
        with TemporaryDirectory() as out:
            report = migrate(_plan_of(self.root), out_dir=Path(out))
            row = next(r for r in report["results"]
                       if r["asset_id"] == "dataflow.Sales")
        self.assertEqual(row["status"], "blocked")
        self.assertEqual(row["findings"][0]["rule"],
                         "INV05_DATAFLOW_NOT_TRANSLATED")
        self.assertIn("Node", row["findings"][0]["detail"])

    def test_the_query_count_it_could_see_is_reported(self):
        with TemporaryDirectory() as out:
            report = migrate(_plan_of(self.root), out_dir=Path(out))
            row = next(r for r in report["results"]
                       if r["asset_id"] == "dataflow.Sales")
        self.assertIn("1 quer", row["findings"][0]["detail"])

    def test_verify_shows_it(self):
        with TemporaryDirectory() as out:
            migrate(_plan_of(self.root), out_dir=Path(out))
            result = verify(Path(out) / "report.json")
        row = next(r for r in result["rows"]
                   if r["asset_id"] == "dataflow.Sales")
        self.assertEqual(row["verdict"], "REVIEW")


class UnparseableTests(unittest.TestCase):
    """A mashup.pq the parser rejects, and one that is not there at all."""

    def _row(self, mashups, asset_id):
        with TemporaryDirectory() as tmp:
            root = _workspace(tmp, mashups)
            plan = _plan_of(root)
            with TemporaryDirectory() as out:
                report = migrate(plan, out_dir=Path(out))
            self.assertIn(asset_id, [a["id"] for a in plan["assets"]])
            return next(r for r in report["results"]
                        if r["asset_id"] == asset_id)

    @unittest.skipUnless(dataflow.m_parser.parser_available(),
                         "Node + mparse not installed")
    def test_an_unparseable_mashup_is_reported_not_dropped(self):
        row = self._row({"Broken": BROKEN}, "dataflow.Broken")
        self.assertEqual(row["status"], "blocked")
        self.assertEqual(row["findings"][0]["rule"], "INV04_DATAFLOW_UNREADABLE")

    def test_a_missing_mashup_is_reported_not_dropped(self):
        row = self._row({"Empty": None}, "dataflow.Empty")
        self.assertEqual(row["status"], "blocked")
        self.assertEqual(row["findings"][0]["rule"], "INV04_DATAFLOW_UNREADABLE")
        self.assertIn("mashup.pq", row["findings"][0]["detail"])

    @unittest.skipUnless(dataflow.m_parser.parser_available(),
                         "Node + mparse not installed")
    def test_a_readable_dataflow_beside_a_broken_one_still_migrates(self):
        with TemporaryDirectory() as tmp:
            root = _workspace(tmp, {"Broken": BROKEN, "Sales": GOOD})
            ids = [a["id"] for a in _plan_of(root)["assets"]]
        self.assertIn("dataflow.Broken", ids)
        self.assertIn("dataflow.Sales.T", ids)

    @unittest.skipUnless(dataflow.m_parser.parser_available(),
                         "Node + mparse not installed")
    def test_a_dataflow_that_parses_gets_no_unreadable_asset(self):
        with TemporaryDirectory() as tmp:
            root = _workspace(tmp, {"Sales": GOOD})
            ids = [a["id"] for a in _plan_of(root)["assets"]]
        self.assertNotIn("dataflow.Sales", ids)
        self.assertIn("dataflow.Sales.T", ids)

    @unittest.skipUnless(dataflow.m_parser.parser_available(),
                         "Node + mparse not installed")
    def test_an_empty_section_is_not_called_unreadable(self):
        # 4 of the demo's 15 Dataflows are `section Section1;` and nothing
        # else. There is nothing in them to migrate and nothing went wrong,
        # so they get no asset -- reporting them blocked would be noise.
        with TemporaryDirectory() as tmp:
            root = _workspace(tmp, {"Hollow": "section Section1;\n"})
            ids = [a["id"] for a in _plan_of(root)["assets"]]
        self.assertEqual(ids, [])


class ScanRecordTests(unittest.TestCase):
    def test_the_scan_marks_a_dataflow_it_could_not_translate(self):
        real = dataflow.m_parser.parser_available
        dataflow.m_parser.parser_available = lambda *a, **k: False
        self.addCleanup(setattr, dataflow.m_parser, "parser_available", real)
        with TemporaryDirectory() as tmp:
            result = dataflow.scan(discover_items(_workspace(tmp, {"Sales": GOOD})))
        flow = result["items"]["dataflows"][0]
        # None, never 0: an absent tool is reported as absent, not rendered
        # as an empty result. The same rule `translatable_count` follows.
        self.assertIsNone(flow["translated"])
        self.assertEqual(flow["counted"], 1)
        self.assertEqual(flow["parser"], "unavailable")


if __name__ == "__main__":
    unittest.main()
