"""A UTF-8 byte-order mark on export files must not change what is found.

Windows editors and some Git tooling write one. Before this, a BOM on a
`.platform` dropped the item, on a pipeline or shortcut JSON it read as
empty, on a notebook it was "not valid JSON", on a warehouse `.sql` it went
through into Spark SQL, and on `mashup.pq` every query of the Dataflow
vanished -- each silently.
"""
import json
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.fixtures import demo_workspace_path
from fabric_aidp.inventory.catalog import load_supplied_catalog
from fabric_aidp.inventory.manifest import build_manifest
from fabric_aidp.translate import m_parser

BOM = b"\xef\xbb\xbf"
_TEXT = {".platform", ".json", ".py", ".ipynb", ".sql", ".pq", ".md"}


def _stable(manifest):
    return {k: v for k, v in manifest.items()
            if k not in ("workspace_name", "workspace_path", "scanned_at")}


class BomTests(unittest.TestCase):
    def test_a_bom_on_every_file_changes_nothing(self):
        with TemporaryDirectory() as tmp:
            plain, bommed = Path(tmp) / "plain", Path(tmp) / "bom"
            shutil.copytree(demo_workspace_path(), plain)
            shutil.copytree(demo_workspace_path(), bommed)
            touched = 0
            for path in bommed.rglob("*"):
                if path.is_file() and (path.suffix in _TEXT or path.name in _TEXT):
                    path.write_bytes(BOM + path.read_bytes())
                    touched += 1
            self.assertGreater(touched, 10)
            expected = json.dumps(_stable(build_manifest(plain)), sort_keys=True)
            actual = json.dumps(_stable(build_manifest(bommed)), sort_keys=True)
        self.assertNotIn("\\ufeff", actual)
        self.assertEqual(actual, expected)

    @unittest.skipUnless(m_parser.parser_available(), "Node + mparse not installed")
    def test_a_bom_on_mashup_pq_still_parses(self):
        # Without Node, inventory only counts queries by regex, which a BOM
        # never broke -- so this half needs the parser.
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "mashup.pq"
            path.write_bytes(BOM + b"section Section1;\nshared q = let a = 1 in a;\n")
            self.assertEqual([q["name"] for q in m_parser.parse_file(path)["queries"]], ["q"])

    def test_a_bom_on_the_catalog_csv_keeps_the_header(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "catalog.csv"
            path.write_bytes(BOM + b"table\nsales.orders\n")
            self.assertTrue(load_supplied_catalog(path))


if __name__ == "__main__":
    unittest.main()
