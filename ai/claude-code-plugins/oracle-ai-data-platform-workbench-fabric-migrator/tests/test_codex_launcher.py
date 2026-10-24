"""The Codex plugin can start its MCP server, and knows where the user works.

MEASURED by installing the plugin into a throwaway CODEX_HOME and launching
its server through Codex itself (a stand-in `python3` recorded what it was
given), 2026-10-11:

    "PYTHONPATH": "${PLUGIN_ROOT}"         passed through as that literal text
    "PYTHONPATH": "${CLAUDE_PLUGIN_ROOT}"  same -- Codex expands neither
    "cwd": "."                             the plugin's installed folder
    a variable exported in the shell       NOT passed to the server
    "env_vars": ["FABRIC_AIDP_WORK_ROOT"]  passed

So the Codex copy's server never started: with the placeholder unexpanded,
`python3 -m fabric_aidp.mcp_server` cannot find the package. That was true of
the copy first published (`${CLAUDE_PLUGIN_ROOT}`) and stayed true after it
was changed to `${PLUGIN_ROOT}`, which is when `test_plugin_frontmatter`
started failing in that copy.

The fix is a Codex-only config, `.codex-mcp.json`, named by
`.codex-plugin/plugin.json`: it starts the server inside its own folder (the
launcher the AWS Codex plugin uses) and forwards FABRIC_AIDP_WORK_ROOT.
Started there, the server cannot see the user's project -- its cwd and $PWD
are both the plugin cache -- so the config marks the launch, and the server
refuses to default its work root to its own install. Claude Code keeps
`.mcp.json`, and both plugin copies are the same tree.
"""
import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from tests.test_mcp_parity import _load_with_stub

ROOT = Path(__file__).resolve().parents[1]
CODEX_CONFIG = ROOT / ".codex-mcp.json"


def _server(path):
    return json.loads(path.read_text(encoding="utf-8"))["mcpServers"]["fabric-aidp-migrator"]


class CodexConfigTests(unittest.TestCase):
    def test_the_codex_manifest_names_its_own_mcp_config(self):
        manifest = json.loads((ROOT / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest.get("mcpServers"), "./.codex-mcp.json")
        self.assertTrue(CODEX_CONFIG.is_file())

    def test_it_starts_the_server_inside_the_plugin_folder(self):
        """The only way Codex finds the package: it resolves `cwd` against
        the plugin root and expands nothing else."""
        server = _server(CODEX_CONFIG)
        self.assertEqual(server["command"], "python3")
        self.assertEqual(server["args"], ["-m", "fabric_aidp.mcp_server"])
        self.assertEqual(server["cwd"], ".")

    def test_it_uses_no_placeholder_codex_would_leave_unexpanded(self):
        self.assertNotIn("${", CODEX_CONFIG.read_text(encoding="utf-8"))
        self.assertNotIn("PYTHONPATH", _server(CODEX_CONFIG).get("env", {}))

    def test_it_forwards_the_work_root_and_marks_the_launch(self):
        """A shell export does not reach a Codex MCP server unless listed."""
        server = _server(CODEX_CONFIG)
        self.assertIn("FABRIC_AIDP_WORK_ROOT", server["env_vars"])
        self.assertEqual(server["env"]["FABRIC_AIDP_LAUNCHED_BY"], "codex")

    def test_claude_and_codex_run_the_same_server(self):
        claude, codex = _server(ROOT / ".mcp.json"), _server(CODEX_CONFIG)
        self.assertEqual((claude["command"], claude["args"]), (codex["command"], codex["args"]))

    def test_the_claude_config_does_not_carry_the_codex_marker(self):
        """Claude Code starts the server in the project, so the cwd default
        is right there and must not be refused."""
        self.assertNotIn("FABRIC_AIDP_LAUNCHED_BY", _server(ROOT / ".mcp.json").get("env", {}))

    def test_the_sdist_ships_it(self):
        self.assertIn("include .codex-mcp.json",
                      (ROOT / "MANIFEST.in").read_text(encoding="utf-8").splitlines())


class LaunchedByCodexTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.plugin = Path(self._tmp.name).resolve()
        self.module, _server = _load_with_stub()

    def _env(self, **values):
        patch = mock.patch.dict(os.environ, values)
        patch.start()
        self.addCleanup(patch.stop)
        if "FABRIC_AIDP_WORK_ROOT" not in values:
            os.environ.pop("FABRIC_AIDP_WORK_ROOT", None)

    def test_without_a_work_root_it_refuses_to_use_its_own_folder(self):
        self._env(FABRIC_AIDP_LAUNCHED_BY="codex")
        with mock.patch("pathlib.Path.cwd", return_value=self.plugin):
            with self.assertRaises(ValueError) as ctx:
                self.module.work_root()
        message = str(ctx.exception)
        self.assertIn("export FABRIC_AIDP_WORK_ROOT=", message)
        self.assertIn(str(self.plugin), message)

    def test_a_tool_call_is_refused_before_the_cli_runs(self):
        self._env(FABRIC_AIDP_LAUNCHED_BY="codex")
        with mock.patch("pathlib.Path.cwd", return_value=self.plugin), \
                mock.patch("subprocess.run") as run:
            with self.assertRaises(ValueError):
                self.module.inventory(export_dir="export")
        run.assert_not_called()

    def test_with_the_work_root_forwarded_it_is_used(self):
        work = self.plugin / "migration"
        work.mkdir()
        self._env(FABRIC_AIDP_LAUNCHED_BY="codex", FABRIC_AIDP_WORK_ROOT=str(work))
        with mock.patch("pathlib.Path.cwd", return_value=self.plugin):
            self.assertEqual(self.module.work_root(), work)

    def test_without_the_marker_the_cwd_default_is_unchanged(self):
        """Claude Code, and a checkout of this repository, are not Codex."""
        self._env()
        os.environ.pop("FABRIC_AIDP_LAUNCHED_BY", None)
        with mock.patch("pathlib.Path.cwd", return_value=self.plugin):
            self.assertEqual(self.module.work_root(), self.plugin)


if __name__ == "__main__":
    unittest.main()
