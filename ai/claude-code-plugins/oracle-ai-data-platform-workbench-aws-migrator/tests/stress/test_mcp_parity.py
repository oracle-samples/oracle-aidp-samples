from __future__ import annotations

import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.stress.helpers import run_cli


HAS_MCP = importlib.util.find_spec("mcp") is not None


@unittest.skipUnless(HAS_MCP, "install the mcp extra to enable wrapper parity tests")
class McpParityTests(unittest.TestCase):
    def test_inventory_wrapper_matches_cli_fixture_counts(self):
        from aws_aidp.mcp_server import inventory

        with tempfile.TemporaryDirectory() as tmp:
            cli_out = Path(tmp) / "cli.json"
            mcp_out = Path(tmp) / "mcp.json"
            proc = run_cli("inventory", "--fixture", "demo", "-o", str(cli_out))
            self.assertEqual(proc.returncode, 0, proc.stderr)
            output = inventory(fixture="demo", output=str(mcp_out))
            self.assertNotIn("[exit", output)
            cli_manifest = json.loads(cli_out.read_text())
            mcp_manifest = json.loads(mcp_out.read_text())
            # The wrapper call can cross a wall-clock second; timestamps are not
            # part of the command-parity contract.
            cli_manifest.pop("scanned_at", None)
            mcp_manifest.pop("scanned_at", None)
            self.assertEqual(cli_manifest, mcp_manifest)

    def test_wrapper_timeout_is_reported_cleanly(self):
        from aws_aidp.mcp_server import _run

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(
            cmd=["aws-aidp"], timeout=1
        )):
            output = _run(["inventory"], timeout=1)
        self.assertIn("[exit timeout after 1s]", output)

    def test_complete_offline_workflow_matches_cli_counts_and_artifacts(self):
        from aws_aidp.mcp_server import inventory, migrate, plan, verify

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inventory_path = root / "inventory.json"
            plan_path = root / "plan.json"
            cli_out = root / "cli-migrated"
            mcp_out = root / "mcp-migrated"

            self.assertNotIn("[exit", inventory(fixture="demo", output=str(inventory_path)))
            self.assertNotIn("[exit", plan(str(inventory_path), output=str(plan_path), namespace="ns"))

            cli = run_cli(
                "migrate", str(plan_path), "--demo", "--filter", "athena",
                "-o", str(cli_out),
            )
            self.assertEqual(cli.returncode, 0, cli.stderr)
            self.assertNotIn("[exit", migrate(
                str(plan_path), demo=True, filter="athena", out_dir=str(mcp_out)
            ))

            cli_report = json.loads((cli_out / "report.json").read_text())
            mcp_report = json.loads((mcp_out / "report.json").read_text())
            self.assertEqual(cli_report["counts"], mcp_report["counts"])
            self.assertEqual(
                [(r["asset_id"], r["status"], r["flags"]) for r in cli_report["results"]],
                [(r["asset_id"], r["status"], r["flags"]) for r in mcp_report["results"]],
            )
            cli_artifacts = sorted(
                path.read_text() for path in (cli_out / "athena").glob("*.spark.sql")
            )
            mcp_artifacts = sorted(
                path.read_text() for path in (mcp_out / "athena").glob("*.spark.sql")
            )
            self.assertEqual(cli_artifacts, mcp_artifacts)
            self.assertNotIn("[exit", verify(str(mcp_out), filter="athena"))

    def test_invalid_filter_has_same_nonzero_exit_contract(self):
        from aws_aidp.mcp_server import migrate

        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            plan_path.write_text(json.dumps({"plan_id": "p", "assets": []}))
            direct = run_cli(
                "migrate", str(plan_path), "--demo", "--filter", "invalid",
                "-o", str(Path(tmp) / "direct"),
            )
            wrapped = migrate(
                str(plan_path), demo=True, filter="invalid",
                out_dir=str(Path(tmp) / "wrapped"),
            )
            self.assertNotEqual(direct.returncode, 0)
            self.assertTrue(wrapped.startswith(f"[exit {direct.returncode}]"))


if __name__ == "__main__":
    unittest.main()
