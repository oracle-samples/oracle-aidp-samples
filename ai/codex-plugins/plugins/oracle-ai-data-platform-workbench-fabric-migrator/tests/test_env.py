import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from fabric_aidp._env import load_dotenv


class DotenvTests(unittest.TestCase):
    def _load(self, body):
        with TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=True):
            path = Path(tmp) / ".env"
            path.write_text(body, encoding="utf-8")
            load_dotenv(path)
            return dict(os.environ)

    def test_the_tools_own_keys_are_loaded(self):
        env = self._load("OCI_NAMESPACE=ns\nAIDP_WORKSPACE_KEY='ws'\n")
        self.assertEqual((env["OCI_NAMESPACE"], env["AIDP_WORKSPACE_KEY"]), ("ns", "ws"))

    def test_other_keys_are_ignored(self):
        # NODE_OPTIONS=--require ./x.js would run code inside the M parser.
        env = self._load("NODE_OPTIONS=--require ./x.js\nPATH=/tmp\nOCI_NAMESPACE=ns\n")
        self.assertNotIn("NODE_OPTIONS", env)
        self.assertNotIn("PATH", env)

    def test_a_bom_does_not_eat_the_first_key(self):
        with TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=True):
            path = Path(tmp) / ".env"
            path.write_bytes(b"\xef\xbb\xbfOCI_NAMESPACE=ns\n")
            load_dotenv(path)
            self.assertEqual(os.environ.get("OCI_NAMESPACE"), "ns")


if __name__ == "__main__":
    unittest.main()
