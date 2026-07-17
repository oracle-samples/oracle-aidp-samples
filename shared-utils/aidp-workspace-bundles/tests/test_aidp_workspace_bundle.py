"""Focused tests for migration utility parsing and pagination safeguards."""

import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).parents[1] / "aidp_workspace_bundle.py"
SPEC = importlib.util.spec_from_file_location("aidp_workspace_bundle", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class AidpWorkspaceBundleTests(unittest.TestCase):
    def test_parse_resource_accepts_supported_types(self):
        self.assertEqual(MODULE.parse_resource("job:daily"), {"resourceType": "JOB", "resourceKey": "daily"})
        self.assertEqual(MODULE.parse_resource("AGENTFLOW:agent_1"), {"resourceType": "AGENTFLOW", "resourceKey": "agent_1"})

    def test_parse_resource_rejects_invalid_values(self):
        for value in ("daily", "TABLE:daily", "JOB:"):
            with self.assertRaises(MODULE.AidpApiError):
                MODULE.parse_resource(value)

    def test_validate_path_blocks_traversal(self):
        MODULE.validate_path("/Workspace/migration")
        for path in ("/Workspace/../secrets", "/Workspace/a/../../secrets", "/tmp/migration"):
            with self.assertRaises(MODULE.AidpApiError):
                MODULE.validate_path(path)

    def test_safe_file_path_stays_under_root(self):
        root = pathlib.Path("/tmp/aidp-test-root")
        self.assertEqual(MODULE.safe_file_path(root, "/Shared/a.py"), (root / "Shared" / "a.py").resolve())
        with self.assertRaises(MODULE.AidpApiError):
            MODULE.safe_file_path(root, "/../../outside")

    def test_list_all_follows_next_page(self):
        original = MODULE.raw_request
        calls = []

        def fake_request(_args, _method, path, **_kwargs):
            calls.append(path)
            if "page=token-2" not in path:
                return {"status": "200 OK", "data": {"items": [{"key": "one"}]}, "headers": {"opc-next-page": "token-2"}}
            return {"status": "200 OK", "data": {"items": [{"key": "two"}]}, "headers": {}}

        MODULE.raw_request = fake_request
        try:
            self.assertEqual(MODULE.list_all(object(), "/workspaces?limit=1000"), [{"key": "one"}, {"key": "two"}])
            self.assertEqual(len(calls), 2)
        finally:
            MODULE.raw_request = original

    def test_list_all_rejects_repeated_page_token(self):
        original = MODULE.raw_request

        def fake_request(_args, _method, _path, **_kwargs):
            return {"status": "200 OK", "data": {"items": []}, "headers": {"opc-next-page": "repeat"}}

        MODULE.raw_request = fake_request
        try:
            with self.assertRaises(MODULE.AidpApiError):
                MODULE.list_all(object(), "/workspaces?limit=1000")
        finally:
            MODULE.raw_request = original


if __name__ == "__main__":
    unittest.main()
