import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory.git_workspace import (
    FabricItem, NotAFabricExport, UnreadableItem, discover_items, items_of_type,
)


def _v2_item(root: Path, dirname: str, item_type: str, display: str,
             logical_id: str = "11111111-1111-1111-1111-111111111111") -> Path:
    d = root / dirname
    d.mkdir(parents=True)
    (d / ".platform").write_text(json.dumps({
        "version": "2.0",
        "config": {"logicalId": logical_id},
        "metadata": {"type": item_type, "displayName": display, "description": "d"},
    }), encoding="utf-8")
    return d


class DiscoveryTests(unittest.TestCase):
    def test_finds_v2_items_and_reads_type_from_platform(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _v2_item(root, "Ingest.Notebook", "Notebook", "Ingest")
            _v2_item(root, "Sales.Lakehouse", "Lakehouse", "Sales")
            items = discover_items(root)
            self.assertEqual([(i.item_type, i.name) for i in items],
                             [("Lakehouse", "Sales"), ("Notebook", "Ingest")])

    def test_platform_type_wins_over_a_stale_directory_suffix(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _v2_item(root, "Renamed.Notebook", "Lakehouse", "ActuallyALakehouse")
            self.assertEqual(discover_items(root)[0].item_type, "Lakehouse")

    def test_finds_items_nested_in_subdirectories(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _v2_item(root, "team/sub/Deep.Notebook", "Notebook", "Deep")
            self.assertEqual([i.name for i in discover_items(root)], ["Deep"])

    def test_reads_v1_system_files(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            d = root / "Old.Notebook"
            d.mkdir()
            (d / "item.metadata.json").write_text(
                json.dumps({"type": "Notebook", "displayName": "Old"}), encoding="utf-8")
            (d / "item.config.json").write_text(
                json.dumps({"version": "1.0", "logicalId": "abc"}), encoding="utf-8")
            item = discover_items(root)[0]
            self.assertEqual((item.name, item.item_type, item.logical_id),
                             ("Old", "Notebook", "abc"))

    def test_git_directory_is_never_traversed(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _v2_item(root, ".git/objects/Sneaky.Notebook", "Notebook", "Sneaky")
            _v2_item(root, "Real.Notebook", "Notebook", "Real")
            self.assertEqual([i.name for i in discover_items(root)], ["Real"])

    def test_malformed_platform_json_is_skipped_not_fatal(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            bad = root / "Bad.Notebook"
            bad.mkdir()
            (bad / ".platform").write_text("{not json", encoding="utf-8")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            self.assertEqual([i.name for i in discover_items(root)], ["Good"])


class UnreadableItemTests(unittest.TestCase):
    """An item whose `.platform` cannot be read used to vanish without a
    word: no error, no warning, no record. Fabric identifies an item *by*
    that file, so an unreadable one is exactly when a human needs telling.
    """

    def _discover(self, root):
        problems = []
        return discover_items(root, problems=problems), problems

    def test_unparseable_platform_is_reported_not_dropped(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            (root / "Bad.Notebook").mkdir()
            (root / "Bad.Notebook" / ".platform").write_text(
                "{ this is not json", encoding="utf-8")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            items, problems = self._discover(root)
        self.assertEqual([i.name for i in items], ["Good"])
        self.assertEqual([p.path.name for p in problems], ["Bad.Notebook"])
        self.assertIn("json", problems[0].reason.lower())

    def test_platform_without_metadata_is_reported(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            (root / "NoMeta.Notebook").mkdir()
            (root / "NoMeta.Notebook" / ".platform").write_text(
                json.dumps({"config": {"logicalId": "x"}}), encoding="utf-8")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            items, problems = self._discover(root)
        self.assertEqual([i.name for i in items], ["Good"])
        self.assertEqual([p.path.name for p in problems], ["NoMeta.Notebook"])
        self.assertIn("metadata", problems[0].reason.lower())

    def test_platform_without_a_type_is_reported(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            (root / "NoType.Notebook").mkdir()
            (root / "NoType.Notebook" / ".platform").write_text(
                json.dumps({"metadata": {"displayName": "x"}}), encoding="utf-8")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            _items, problems = self._discover(root)
        self.assertEqual([p.path.name for p in problems], ["NoType.Notebook"])
        self.assertIn("type", problems[0].reason.lower())

    def test_a_broken_v1_metadata_file_is_reported(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            (root / "Old.Notebook").mkdir()
            (root / "Old.Notebook" / "item.metadata.json").write_text(
                "{nope", encoding="utf-8")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            _items, problems = self._discover(root)
        self.assertEqual([p.path.name for p in problems], ["Old.Notebook"])

    def test_an_ordinary_directory_is_not_a_problem(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            (root / "docs").mkdir()
            (root / "docs" / "readme.txt").write_text("hi", encoding="utf-8")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            _items, problems = self._discover(root)
        self.assertEqual(problems, [])

    def test_an_unreadable_item_directory_is_not_descended_into(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            (root / "Bad.Notebook").mkdir()
            (root / "Bad.Notebook" / ".platform").write_text("{", encoding="utf-8")
            _v2_item(root, "Bad.Notebook/Inner.Notebook", "Notebook", "Inner")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            items, problems = self._discover(root)
        self.assertEqual([i.name for i in items], ["Good"])
        self.assertEqual(len(problems), 1)

    def test_an_export_of_nothing_but_unreadable_items_says_so(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            (root / "Bad.Notebook").mkdir()
            (root / "Bad.Notebook" / ".platform").write_text("{", encoding="utf-8")
            with self.assertRaises(NotAFabricExport) as caught:
                discover_items(root)
        self.assertIn("Bad.Notebook", str(caught.exception))

    def test_problems_are_ordered_deterministically(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            for n in ("Zeta", "Alpha", "Mid"):
                (root / f"{n}.Notebook").mkdir()
                (root / f"{n}.Notebook" / ".platform").write_text(
                    "{", encoding="utf-8")
            _v2_item(root, "Good.Notebook", "Notebook", "Good")
            _items, problems = self._discover(root)
        self.assertEqual([p.path.name for p in problems],
                         ["Alpha.Notebook", "Mid.Notebook", "Zeta.Notebook"])

    def test_an_unreadable_item_is_not_a_fabric_item(self):
        self.assertFalse(isinstance(
            UnreadableItem(Path("x"), "why"), FabricItem))

    def test_display_name_falls_back_to_directory_stem(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            d = root / "Fallback.Notebook"
            d.mkdir()
            (d / ".platform").write_text(json.dumps({
                "config": {"logicalId": "x"}, "metadata": {"type": "Notebook"},
            }), encoding="utf-8")
            self.assertEqual(discover_items(root)[0].name, "Fallback")

    def test_ordering_is_deterministic(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            for n in ("Zeta", "Alpha", "Mid"):
                _v2_item(root, f"{n}.Notebook", "Notebook", n)
            self.assertEqual([i.name for i in discover_items(root)],
                             ["Alpha", "Mid", "Zeta"])

    def test_directory_with_no_items_is_rejected(self):
        with TemporaryDirectory() as t:
            with self.assertRaises(NotAFabricExport):
                discover_items(Path(t))

    def test_missing_root_is_rejected(self):
        with self.assertRaises(NotAFabricExport):
            discover_items(Path("/nonexistent/fabric/export"))


class FolderTests(unittest.TestCase):
    """A display name is unique inside a workspace *folder*, not inside a
    workspace, so an item at `teamA/Shared_Load.Notebook` and one at
    `teamB/Shared_Load.Notebook` are two items with one name. The folder is
    in the export and was not in the record, so `plan` had nothing to tell
    them apart with and refused the whole workspace.
    """

    def test_an_item_at_the_root_has_no_folder(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _v2_item(root, "Ingest.Notebook", "Notebook", "Ingest")
            self.assertEqual(discover_items(root)[0].folder, "")

    def test_a_nested_item_records_the_folder_that_holds_it(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _v2_item(root, "teamA/sub/Deep.Notebook", "Notebook", "Deep")
            self.assertEqual(discover_items(root)[0].folder, "teamA/sub")

    def test_two_folders_may_hold_the_same_display_name(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _v2_item(root, "teamA/Shared_Load.Notebook", "Notebook",
                     "Shared_Load", logical_id="aaaaaaaa-0000-0000-0000-000000000001")
            _v2_item(root, "teamB/Shared_Load.Notebook", "Notebook",
                     "Shared_Load", logical_id="aaaaaaaa-0000-0000-0000-000000000002")
            items = discover_items(root)
            self.assertEqual(sorted(i.folder for i in items), ["teamA", "teamB"])

    def test_the_folder_is_part_of_the_sort_key(self):
        """Without it the order of two same-named items came from the walk."""
        with TemporaryDirectory() as t:
            root = Path(t)
            for folder in ("teamB", "teamA"):
                _v2_item(root, f"{folder}/Shared_Load.Notebook", "Notebook",
                         "Shared_Load", logical_id=f"aaaaaaaa-0000-0000-0000-{folder:0>12}")
            self.assertEqual([i.folder for i in discover_items(root)],
                             ["teamA", "teamB"])


class FilterTests(unittest.TestCase):
    def test_items_of_type_is_case_insensitive(self):
        items = [FabricItem("A", "Notebook", "1", Path("."), ""),
                 FabricItem("B", "Lakehouse", "2", Path("."), "")]
        self.assertEqual([i.name for i in items_of_type(items, "notebook")], ["A"])


if __name__ == "__main__":
    unittest.main()
