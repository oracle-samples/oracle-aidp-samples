import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory.manifest import (
    ALL_SOURCES, build_manifest, summarize, write_manifest,
)

NB = ("# Fabric notebook source\n\n# METADATA ********************\n\n"
      "# META {\n"
      '# META   "dependencies": { "lakehouse": { "default_lakehouse_name": "Sales" } }\n'
      "# META }\n\n# CELL ********************\n\n"
      '%run Common_Utils\ndf.write.saveAsTable("claims_agg")\n')


def _export(root: Path) -> Path:
    def item(dirname, item_type, display, files=None):
        d = root / dirname
        d.mkdir(parents=True)
        (d / ".platform").write_text(json.dumps({
            "config": {"logicalId": f"id-{display}"},
            "metadata": {"type": item_type, "displayName": display},
        }), encoding="utf-8")
        for name, body in (files or {}).items():
            (d / name).write_text(body, encoding="utf-8")
    item("Ingest.Notebook", "Notebook", "Ingest", {"notebook-content.py": NB})
    item("AcmeDW.Warehouse", "Warehouse", "AcmeDW",
         {"claim.sql": "CREATE TABLE dbo.claim (id BIGINT)"})
    item("Sales.Lakehouse", "Lakehouse", "Sales", {
        "shortcuts.metadata.json": json.dumps([{
            "path": "Tables", "name": "claims_raw",
            "target": {"type": "AmazonS3",
                       "amazonS3": {"location": "https://a.s3.amazonaws.com",
                                    "subpath": "/c"}}}]),
    })
    return root


class BuildTests(unittest.TestCase):
    def _manifest(self, sources=ALL_SOURCES, catalog_csv=None):
        with TemporaryDirectory() as t:
            root = _export(Path(t))
            return build_manifest(root, sources, catalog_csv=catalog_csv)

    def test_top_level_shape(self):
        m = self._manifest()
        for key in ("source", "workspace_name", "workspace_path", "scanned_at",
                    "sources_scanned", "sources", "resolved_catalog"):
            self.assertIn(key, m)
        self.assertEqual(m["source"], "fabric-git")

    def test_all_five_sources_are_present(self):
        self.assertEqual(set(self._manifest()["sources"]), set(ALL_SOURCES))

    def test_a_source_subset_scans_only_those(self):
        m = self._manifest(sources=("notebook",))
        self.assertEqual(list(m["sources"]), ["notebook"])
        self.assertEqual(m["sources_scanned"], ["notebook"])

    def test_unknown_source_is_rejected(self):
        with TemporaryDirectory() as t:
            with self.assertRaises(ValueError):
                build_manifest(_export(Path(t)), ("nope",))

    def test_empty_source_list_is_rejected(self):
        with TemporaryDirectory() as t:
            with self.assertRaises(ValueError):
                build_manifest(_export(Path(t)), ())

    def test_duplicate_sources_are_scanned_once(self):
        m = self._manifest(sources=("notebook", "notebook"))
        self.assertEqual(m["sources_scanned"], ["notebook"])

    def test_catalog_merges_all_tiers(self):
        """Keys carry the owning Fabric item since the catalog stopped
        letting two items' same-named tables shadow each other."""
        tables = self._manifest()["resolved_catalog"]["tables"]
        tiers = {e["name"]: e["tier"] for e in tables.values()}
        self.assertEqual(tiers["dbo.claim"], "warehouse_ddl")
        self.assertEqual(tiers["claims_raw"], "shortcut")
        self.assertEqual(tiers["claims_agg"], "notebook_inferred")

    def test_supplied_catalog_csv_is_loaded(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "ws")
            csv_path = Path(t) / "tables.csv"
            csv_path.write_text("table\nclaims_agg\n", encoding="utf-8")
            m = build_manifest(root, ALL_SOURCES, catalog_csv=csv_path)
        self.assertEqual(m["resolved_catalog"]["tables"]["claims_agg"]["tier"], "supplied")

    def test_an_unreadable_item_is_recorded_not_dropped(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "ws")
            (root / "Bad.Notebook").mkdir()
            (root / "Bad.Notebook" / ".platform").write_text(
                "{ this is not json", encoding="utf-8")
            m = build_manifest(root, ALL_SOURCES)
        self.assertEqual([u["name"] for u in m["unreadable_items"]],
                         ["Bad.Notebook"])
        self.assertIn("json", m["unreadable_items"][0]["reason"].lower())
        self.assertEqual(m["unreadable_items"][0]["path"], "Bad.Notebook")

    def test_a_clean_export_records_no_unreadable_items(self):
        self.assertEqual(self._manifest()["unreadable_items"], [])

    def test_the_summary_names_every_unreadable_item(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "ws")
            (root / "Bad.Notebook").mkdir()
            (root / "Bad.Notebook" / ".platform").write_text("{", encoding="utf-8")
            text = summarize(build_manifest(root, ALL_SOURCES))
        self.assertIn("Bad.Notebook", text)
        self.assertIn("1 item", text)

    def test_an_unmigratable_item_type_is_recorded_not_dropped(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "ws")
            for dirname, item_type, display in (
                    ("SalesReport.Report", "Report", "SalesReport"),
                    ("ProdEnv.Environment", "Environment", "ProdEnv"),
                    ("Churn.MLModel", "MLModel", "Churn")):
                d = root / dirname
                d.mkdir()
                (d / ".platform").write_text(json.dumps({
                    "config": {"logicalId": display},
                    "metadata": {"type": item_type, "displayName": display},
                }), encoding="utf-8")
            m = build_manifest(root, ALL_SOURCES)
        self.assertEqual(
            [(u["item_type"], u["name"]) for u in m["unsupported_items"]],
            [("Environment", "ProdEnv"), ("MLModel", "Churn"),
             ("Report", "SalesReport")])
        self.assertIn("no scanner", m["unsupported_items"][0]["reason"])

    def test_a_type_whose_scanner_was_not_selected_says_so(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "ws")
            m = build_manifest(root, ("notebook",))
        by_type = {u["item_type"]: u["reason"] for u in m["unsupported_items"]}
        self.assertIn("Warehouse", by_type)
        self.assertIn("not selected", by_type["Warehouse"])
        self.assertNotIn("Notebook", by_type)

    def test_a_fully_supported_export_records_none(self):
        self.assertEqual(self._manifest()["unsupported_items"], [])

    def test_the_summary_names_every_unmigratable_type(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "ws")
            d = root / "SalesReport.Report"
            d.mkdir()
            (d / ".platform").write_text(json.dumps({
                "metadata": {"type": "Report", "displayName": "SalesReport"},
            }), encoding="utf-8")
            text = summarize(build_manifest(root, ALL_SOURCES))
        self.assertIn("Report", text)
        self.assertIn("SalesReport", text)

    def test_workspace_name_is_the_export_directory_name(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "AcmeAnalytics")
            self.assertEqual(build_manifest(root, ALL_SOURCES)["workspace_name"],
                             "AcmeAnalytics")

    def test_scanner_failure_is_isolated_to_its_source(self):
        import fabric_aidp.inventory.manifest as manifest_mod
        original = manifest_mod._SCANNERS["pipeline"]

        def boom(items, **kw):
            raise RuntimeError("scanner exploded")

        manifest_mod._SCANNERS["pipeline"] = boom
        try:
            m = self._manifest()
        finally:
            manifest_mod._SCANNERS["pipeline"] = original
        self.assertIn("error", m["sources"]["pipeline"]["summary"])
        self.assertEqual(m["sources"]["notebook"]["summary"]["notebook_count"], 1)


class WriteAndSummarizeTests(unittest.TestCase):
    def test_write_is_atomic_and_round_trips(self):
        with TemporaryDirectory() as t:
            root = _export(Path(t) / "ws")
            m = build_manifest(root, ALL_SOURCES)
            out = write_manifest(m, Path(t) / "out" / "manifest.json")
            self.assertEqual(json.loads(out.read_text(encoding="utf-8"))["source"],
                             "fabric-git")

    def test_summary_names_each_source(self):
        with TemporaryDirectory() as t:
            text = summarize(build_manifest(_export(Path(t)), ALL_SOURCES))
        for source in ALL_SOURCES:
            self.assertIn(source, text)

    def test_summary_reports_catalog_tiers(self):
        with TemporaryDirectory() as t:
            text = summarize(build_manifest(_export(Path(t)), ALL_SOURCES))
        self.assertIn("warehouse_ddl", text)
        self.assertIn("notebook_inferred", text)

    def test_summary_says_unknown_for_untracked_shortcuts(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            d = root / "Bare.Lakehouse"
            d.mkdir(parents=True)
            (d / ".platform").write_text(json.dumps({
                "config": {"logicalId": "x"},
                "metadata": {"type": "Lakehouse", "displayName": "Bare"},
            }), encoding="utf-8")
            text = summarize(build_manifest(root, ("lakehouse",)))
        self.assertIn("unknown", text)
    def test_summary_prints_why_a_shortcuts_file_could_not_be_read(self):
        """`tracked (0 found)` used to be all the operator saw."""
        with TemporaryDirectory() as t:
            root = Path(t)
            d = root / "Sales.Lakehouse"
            d.mkdir(parents=True)
            (d / ".platform").write_text(json.dumps({
                "config": {"logicalId": "x"},
                "metadata": {"type": "Lakehouse", "displayName": "Sales"},
            }), encoding="utf-8")
            (d / "alm.settings.json").write_text(
                json.dumps({"objectTypes": [{"name": "Shortcuts",
                                             "state": "Enabled"}]}),
                encoding="utf-8")
            (d / "shortcuts.metadata.json").write_text("{not json",
                                                       encoding="utf-8")
            text = summarize(build_manifest(root, ("lakehouse",)))
        self.assertNotIn("0 found", text)
        self.assertIn("cannot read shortcuts.metadata.json", text)


if __name__ == "__main__":
    unittest.main()
