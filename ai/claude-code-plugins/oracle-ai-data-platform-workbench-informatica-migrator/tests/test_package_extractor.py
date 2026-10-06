"""Tests for parsers.package_extractor (salvaged from the deleted api/_common.py)."""

import tempfile
import unittest
import zipfile
from pathlib import Path

from infa2aidp.parsers.package_extractor import extract_package


class TestExtractPackage(unittest.TestCase):
    """extract_package() on valid ZIPs, non-ZIPs, and path-traversal attempts."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_valid_zip_extracts(self):
        archive = self.tmp_path / "package.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("mapping.json", '{"name": "m_test"}')

        dest = self.tmp_path / "out"
        result = extract_package(archive, dest)

        self.assertEqual(result, dest)
        self.assertTrue((dest / "mapping.json").exists())
        self.assertEqual(
            (dest / "mapping.json").read_text(encoding="utf-8"), '{"name": "m_test"}'
        )

    def test_non_zip_raises_value_error(self):
        not_a_zip = self.tmp_path / "not_a_package.txt"
        not_a_zip.write_text("this is not a zip file", encoding="utf-8")

        with self.assertRaises(ValueError):
            extract_package(not_a_zip, self.tmp_path / "out")

    def test_path_traversal_raises_value_error(self):
        archive = self.tmp_path / "malicious.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("../escape.json", '{"evil": true}')

        with self.assertRaises(ValueError):
            extract_package(archive, self.tmp_path / "out")


if __name__ == "__main__":
    unittest.main()
