"""Real Lakehouse schemas, vendored from microsoft/fabric-cicd.

Every assertion here was written from a real file, not from documentation.
Three schemas this tool had guessed at turned out to be wrong:

  shortcuts.metadata.json  `path` is "/Tables/dbo" or "/Files", not "Tables"
  alm.settings.json        objectTypes is [{name, state}], not a list of names
  lakehouse.metadata.json  exists at all, and carries defaultSchema
"""
import json
import unittest
from pathlib import Path

from fabric_aidp.inventory import lakehouse as lh_mod
from fabric_aidp.inventory.catalog import build_catalog
from fabric_aidp.inventory.git_workspace import discover_items

REAL = Path(__file__).resolve().parent / "fixtures" / "real" / "lakehouse"


class RealSchemaTests(unittest.TestCase):
    def setUp(self):
        self.items = discover_items(REAL)
        self.out = lh_mod.scan(self.items)
        self.by_name = {r["name"]: r for r in self.out["items"]["lakehouses"]}

    def test_all_three_lakehouses_are_found(self):
        self.assertEqual(sorted(self.by_name),
                         ["Diabetes_LH", "TargetForShortcutLH", "WithSchema"])

    def test_real_alm_settings_are_understood(self):
        """objectTypes is a list of {name, state}, not a list of names."""
        tracking = self.by_name["Diabetes_LH"]["tracking"]
        self.assertEqual(tracking["shortcuts"], "tracked")
        self.assertEqual(tracking["data_access_roles"], "not_tracked")

    def test_section_is_normalised_from_a_slash_path(self):
        shortcuts = self.by_name["TargetForShortcutLH"]["shortcuts"]
        sections = sorted({s["section"] for s in shortcuts})
        self.assertEqual(sections, ["Files", "Tables"])

    def test_full_path_is_preserved_alongside_the_section(self):
        shortcuts = self.by_name["TargetForShortcutLH"]["shortcuts"]
        tables = next(s for s in shortcuts if s["section"] == "Tables")
        self.assertEqual(tables["path"], "/Tables/dbo")

    def test_a_schema_qualified_shortcut_gets_a_qualified_table_name(self):
        shortcuts = self.by_name["TargetForShortcutLH"]["shortcuts"]
        tables = next(s for s in shortcuts if s["section"] == "Tables")
        self.assertEqual(tables["table_name"], "dbo.publicholidays")

    def test_a_files_shortcut_has_no_table_name(self):
        shortcuts = self.by_name["TargetForShortcutLH"]["shortcuts"]
        files = next(s for s in shortcuts if s["section"] == "Files")
        self.assertIsNone(files["table_name"])

    def test_internal_onelake_targets_are_recognised(self):
        shortcuts = self.by_name["TargetForShortcutLH"]["shortcuts"]
        self.assertTrue(all(s["target_type"] == "OneLake" for s in shortcuts))
        self.assertTrue(all(not s["external"] for s in shortcuts))


class RealTargetTypeCoverageTests(unittest.TestCase):
    """Which shortcut target types a real export says it can contain.

    Diabetes_LH's alm.settings.json enumerates the sub-object types Fabric
    tracks under Shortcuts. That list is the authoritative set of target
    types a Git export can carry, and it is the evidence behind the per-type
    shape table in shortcut_to_oci: eight types, one of which
    (OneDriveSharePoint) the scanner had no payload key for at all, so a real
    SharePoint shortcut was reported as "unrecognised shortcut target shape"
    with no URL recorded rather than as a location this tool cannot map.
    """

    def _declared_types(self):
        settings = json.loads(
            (REAL / "Diabetes_LH.Lakehouse" / "alm.settings.json")
            .read_text(encoding="utf-8"))
        shortcuts = next(o for o in settings["objectTypes"]
                         if o["name"] == "Shortcuts")
        return {sub["name"].split(".", 1)[1].casefold()
                for sub in shortcuts["subObjectTypes"]}

    def test_the_fixture_still_enumerates_eight_target_types(self):
        self.assertEqual(len(self._declared_types()), 8)

    def test_every_declared_target_type_has_a_known_payload_shape(self):
        known = set(lh_mod._EXTERNAL_TARGETS) | {"onelake"}
        self.assertEqual(self._declared_types() - known, set())


class RealCatalogTests(unittest.TestCase):
    def test_a_real_table_shortcut_reaches_the_catalog(self):
        """The bug this file exists for: /Tables/dbo never matched "tables"."""
        out = lh_mod.scan(discover_items(REAL))
        catalog = build_catalog({"lakehouse": out})
        tiers = {e["name"].casefold(): e["tier"]
                 for e in catalog["tables"].values()}
        self.assertIn("dbo.publicholidays", tiers)
        self.assertEqual(tiers["dbo.publicholidays"], "shortcut")

    def test_files_shortcuts_do_not_become_tables(self):
        out = lh_mod.scan(discover_items(REAL))
        catalog = build_catalog({"lakehouse": out})
        names = [e["name"] for e in catalog["tables"].values()]
        self.assertNotIn("sample_datasets", names)
        self.assertNotIn("images", names)

    def test_the_catalog_is_not_empty_on_real_data(self):
        out = lh_mod.scan(discover_items(REAL))
        catalog = build_catalog({"lakehouse": out})
        self.assertGreater(catalog["summary"]["shortcut"], 0)


if __name__ == "__main__":
    unittest.main()
