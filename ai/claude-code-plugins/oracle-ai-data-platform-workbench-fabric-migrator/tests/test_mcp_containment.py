"""The MCP server keeps the CLI inside a work root and a scrubbed environment.

Security review SEC-AIDP-SAMPLES-004: `mcp_server._run` used to call
`subprocess.run` with no `cwd` and no `env`, and the tools passed every
path argument straight through. An MCP client is steered by the files it
reads, so a tool call is not a trustworthy place for a path to come from,
and a developer shell carries GitHub, AWS, npm, PyPI and Slack credentials
that have nothing to do with a migration. Every test here failed against
that version of the file.
"""
import inspect
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from tests.test_mcp_parity import _load_with_stub

ROOT = Path(__file__).resolve().parents[1]

#: Every (tool, parameter) that names a file or directory. `fixture`,
#: `sources`, `filter`, `namespace`, `catalog`, `prefix` and `cluster_key`
#: are not paths, and the CLI's `choices` reject the first three anyway.
PATH_PARAMETERS = (
    ("inventory", "export_dir"), ("inventory", "tables_csv"), ("inventory", "output"),
    ("plan", "manifest"), ("plan", "output"), ("plan", "lakehouses"),
    ("migrate", "plan_path"), ("migrate", "out_dir"),
    ("verify", "report_or_dir"),
    ("publish", "out_dir"),
)

#: The kind of thing a developer shell has exported. None may reach the CLI.
PLANTED_SECRETS = {
    "GITHUB_TOKEN": "ghp_planted",
    "GH_TOKEN": "gho_planted",
    "AWS_ACCESS_KEY_ID": "AKIAPLANTED",
    "AWS_SECRET_ACCESS_KEY": "planted/aws",
    "NPM_TOKEN": "npm_planted",
    "NPM_CONFIG_TOKEN": "npm_planted2",
    "TWINE_PASSWORD": "pypi-planted",
    "SLACK_TOKEN": "xoxb-planted",
    "SLACK_BOT_TOKEN": "xoxb-planted2",
    "SOME_SERVICE_SECRET": "planted",
    "DB_PASSWORD": "planted",
    "AZURE_CREDENTIALS": "planted",
    "STRIPE_API_KEY": "sk_planted",
    "NODE_OPTIONS": "--require ./x.js",
}


def _required(tool, overrides):
    """Keyword arguments that let `tool` run: required positionals filled with
    a name, plus `overrides`."""
    kwargs = {}
    for name, spec in inspect.signature(tool).parameters.items():
        if spec.default is inspect.Parameter.empty:
            kwargs[name] = f"{name}.json"
    kwargs.update(overrides)
    return kwargs


class _Completed:
    returncode = 0
    stdout = "ran"
    stderr = ""


class _WorkRootCase(unittest.TestCase):
    """A fresh temp work root per test, named by the variable the server reads."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        patch = mock.patch.dict(os.environ, {"FABRIC_AIDP_WORK_ROOT": str(self.root)})
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(self._tmp.cleanup)
        self.module, _server = _load_with_stub()

    def _outside(self) -> Path:
        """An absolute path that exists and is not under the work root."""
        outside = Path(self._tmp.name).resolve().parent / "fabric-mcp-outside-probe"
        outside.mkdir(exist_ok=True)
        self.addCleanup(lambda: outside.rmdir() if outside.is_dir() else None)
        return outside


class PathPolicyTests(_WorkRootCase):
    def test_traversal_is_refused_before_the_cli_runs(self):
        for tool_name, parameter in PATH_PARAMETERS:
            with self.subTest(tool=tool_name, parameter=parameter):
                tool = getattr(self.module, tool_name)
                with mock.patch("subprocess.run") as run:
                    with self.assertRaises(ValueError) as ctx:
                        tool(**_required(tool, {parameter: "../escaped.json"}))
                run.assert_not_called()
                self.assertIn(str(self.root), str(ctx.exception))
                self.assertIn("FABRIC_AIDP_WORK_ROOT", str(ctx.exception))

    def test_a_deep_traversal_through_a_real_subdirectory_is_still_refused(self):
        (self.root / "exports" / "ws").mkdir(parents=True)
        with mock.patch("subprocess.run") as run:
            with self.assertRaises(ValueError):
                self.module.inventory(export_dir="exports/ws/../../../etc")
        run.assert_not_called()

    def test_an_absolute_path_outside_the_root_is_refused(self):
        outside = self._outside()
        for tool_name, parameter in PATH_PARAMETERS:
            with self.subTest(tool=tool_name, parameter=parameter):
                tool = getattr(self.module, tool_name)
                with mock.patch("subprocess.run") as run:
                    with self.assertRaises(ValueError) as ctx:
                        tool(**_required(tool, {parameter: str(outside / "x.json")}))
                run.assert_not_called()
                self.assertIn(str(self.root), str(ctx.exception))

    def test_the_root_itself_cannot_be_escaped_by_a_sibling_prefix(self):
        """`/work-root-evil` shares a string prefix with `/work-root`; a
        startswith check would let it through."""
        sibling = Path(str(self.root) + "-evil")
        with mock.patch("subprocess.run") as run:
            with self.assertRaises(ValueError):
                self.module.verify(report_or_dir=str(sibling / "migrated"))
        run.assert_not_called()

    def test_an_absolute_path_inside_the_root_is_accepted_as_given(self):
        target = self.root / "exports" / "ws"
        target.mkdir(parents=True)
        seen = []
        with mock.patch("subprocess.run", side_effect=lambda argv, **kw: (
                seen.append(list(argv)) or _Completed())):
            self.module.inventory(export_dir=str(target), output="out/inv.json")
        argv = seen[0][3:]
        self.assertIn(str(target), argv)
        self.assertIn(str(self.root / "out" / "inv.json"), argv)

    def test_a_relative_path_with_a_directory_part_resolves_under_the_root(self):
        seen = []
        with mock.patch("subprocess.run", side_effect=lambda argv, **kw: (
                seen.append(list(argv)) or _Completed())):
            self.module.migrate(plan_path="runs/a/plan.json", out_dir="runs/a/migrated")
        argv = seen[0][3:]
        self.assertEqual(argv, ["migrate", str(self.root / "runs" / "a" / "plan.json"),
                                "-o", str(self.root / "runs" / "a" / "migrated")])

    def test_a_bare_artifact_name_is_namespaced_to_this_servers_run(self):
        """Two sessions that both accept `inv.json` must not overwrite each
        other, and `plan(manifest="inv.json")` must find what
        `inventory(output="inv.json")` wrote in the same session."""
        module = self.module
        run_dir = self.root / module.RUNS_DIR / module.RUN_ID
        self.assertEqual(module.RUNS_DIR, "fabric-aidp-runs")
        self.assertIn(str(os.getpid()), module.RUN_ID)
        seen = []
        with mock.patch("subprocess.run", side_effect=lambda argv, **kw: (
                seen.append(list(argv)) or _Completed())):
            module.inventory(fixture="demo")
            module.plan(manifest="inv.json")
            module.migrate(plan_path="plan.json")
            module.verify()
            module.publish()
        argv = [call[3:] for call in seen]
        self.assertEqual(argv[0], ["inventory", "-o", str(run_dir / "inv.json"),
                                   "--fixture", "demo"])
        self.assertEqual(argv[1], ["plan", str(run_dir / "inv.json"),
                                   "-o", str(run_dir / "plan.json")])
        self.assertEqual(argv[2], ["migrate", str(run_dir / "plan.json"),
                                   "-o", str(run_dir / "migrated")])
        self.assertEqual(argv[3], ["verify", str(run_dir / "migrated")])
        self.assertEqual(argv[4], ["publish", str(run_dir / "migrated")])

    def test_external_inputs_are_not_moved_into_the_run_directory(self):
        """An export directory or a tables CSV is the user's, not an artifact
        of this run; a bare name means `<root>/<name>`."""
        seen = []
        with mock.patch("subprocess.run", side_effect=lambda argv, **kw: (
                seen.append(list(argv)) or _Completed())):
            self.module.inventory(export_dir="export", tables_csv="tables.csv")
        argv = seen[0][3:]
        self.assertIn(str(self.root / "export"), argv)
        self.assertIn(str(self.root / "tables.csv"), argv)

    def test_the_result_displays_the_root_and_the_resolved_paths(self):
        with mock.patch("subprocess.run", return_value=_Completed()):
            text = self.module.publish(out_dir="migrated", prefix="me")
        lines = text.splitlines()
        self.assertEqual(lines[0], f"work root: {self.root}")
        self.assertEqual(lines[1], "out_dir: %s" % (
            self.root / self.module.RUNS_DIR / self.module.RUN_ID / "migrated"))
        self.assertEqual(lines[-1], "ran")

    def test_the_root_defaults_to_the_servers_cwd(self):
        with mock.patch.dict(os.environ):
            del os.environ["FABRIC_AIDP_WORK_ROOT"]
            self.assertEqual(self.module.work_root(), Path.cwd().resolve())

    def test_a_root_that_is_not_a_directory_is_refused_with_its_name(self):
        with mock.patch.dict(os.environ, {"FABRIC_AIDP_WORK_ROOT": str(self.root / "nope")}):
            with self.assertRaises(ValueError) as ctx:
                self.module.work_root()
        self.assertIn("FABRIC_AIDP_WORK_ROOT", str(ctx.exception))
        self.assertIn("nope", str(ctx.exception))

    def test_the_child_runs_in_the_work_root(self):
        """The CLI reads `.env` from its cwd. That has to be the root the
        user configured, not wherever the MCP client happened to start."""
        with mock.patch("subprocess.run", return_value=_Completed()) as run:
            self.module.verify()
        self.assertEqual(run.call_args.kwargs["cwd"], str(self.root))


class EnvironmentPolicyTests(_WorkRootCase):
    def test_planted_secrets_never_reach_the_child(self):
        with mock.patch.dict(os.environ, PLANTED_SECRETS):
            with mock.patch("subprocess.run", return_value=_Completed()) as run:
                self.module.verify()
        env = run.call_args.kwargs["env"]
        for name in PLANTED_SECRETS:
            with self.subTest(variable=name):
                self.assertNotIn(name, env)
        self.assertIn("PATH", env)

    def test_the_filter_is_an_allowlist_not_a_denylist(self):
        """A name no deny-list anticipated is still out."""
        env = self.module.child_env({"PATH": "/bin", "MY_COMPANY_SSO_COOKIE": "x",
                                     "KUBECONFIG": "/home/me/.kube/config"})
        self.assertEqual(env, {"PATH": "/bin"})

    def test_nothing_the_child_needs_is_dropped(self):
        source = {
            "PATH": "p", "HOME": "h", "LANG": "C.UTF-8", "LC_ALL": "C", "LC_CTYPE": "C",
            "TMPDIR": "/tmp",
            "SYSTEMROOT": r"C:\Windows", "COMSPEC": "cmd.exe", "PATHEXT": ".EXE",
            "TEMP": "t", "TMP": "t", "USERPROFILE": "u", "APPDATA": "a", "LOCALAPPDATA": "l",
            "PYTHONPATH": "/plugin", "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1",
            "OCI_CONFIG_FILE": "/home/me/.oci/config", "OCI_CLI_PROFILE": "DEFAULT",
            "OCI_CONFIG_PROFILE": "DEFAULT", "OCI_NAMESPACE": "ns",
            "AIDP_WORKSPACE_KEY": "ws", "AIDP_CLUSTER_KEY": "ck", "AIDP_INSTANCE_ID": "ocid",
            "FABRIC_AIDP_WORK_ROOT": "/work",
        }
        self.assertEqual(self.module.child_env(source), source)

    def test_names_are_matched_without_regard_to_case(self):
        """Windows hands `SystemRoot` to a process that reads `SYSTEMROOT`."""
        env = self.module.child_env({"SystemRoot": r"C:\Windows", "Path": "p",
                                     "Github_Token": "x"})
        self.assertEqual(set(env), {"SystemRoot", "Path"})

    def test_oci_variables_pass_by_name_not_prefix(self):
        """OCI_ also covers key-file paths and session tokens. The selectors
        the SDK needs are named; the rest stays in the server."""
        env = self.module.child_env({"OCI_CONFIG_FILE": "c", "OCI_CLI_KEY_FILE": "k",
                                     "OCI_CLI_SECURITY_TOKEN_FILE": "t"})
        self.assertEqual(env, {"OCI_CONFIG_FILE": "c"})

    def test_the_package_root_is_on_the_childs_pythonpath(self):
        """The child used to find `fabric_aidp` only because it inherited a
        cwd that was a checkout. It now runs in the work root, so the server
        has to say where its own package is."""
        with mock.patch.dict(os.environ, {"PYTHONPATH": "/elsewhere"}):
            with mock.patch("subprocess.run", return_value=_Completed()) as run:
                self.module.verify()
        entries = run.call_args.kwargs["env"]["PYTHONPATH"].split(os.pathsep)
        self.assertEqual(entries, [str(ROOT), "/elsewhere"])


class ChildStillRunsTests(_WorkRootCase):
    """The containment must not break the one thing the server is for."""

    def test_inventory_of_the_demo_fixture_succeeds_in_a_temp_work_root(self):
        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "ghp_planted"}):
            text = self.module.inventory(fixture="demo")
        self.assertNotIn("[exit", text, text)
        written = self.root / self.module.RUNS_DIR / self.module.RUN_ID / "inv.json"
        self.assertTrue(written.is_file(), text)
        self.assertEqual(text.splitlines()[0], f"work root: {self.root}")
        self.assertIn(str(written), text)

    def test_an_interpreter_started_with_the_scrubbed_env_lacks_the_secret(self):
        """Not a mock: a real child, with exactly the environment `_run`
        builds, reports what it can see."""
        with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "ghp_planted"}):
            with mock.patch("subprocess.run", return_value=_Completed()) as run:
                self.module.verify()
            kwargs = run.call_args.kwargs
        probe = subprocess.run(
            [sys.executable, "-c",
             "import os; print(sorted(k for k in os.environ if k.upper() == 'GITHUB_TOKEN'))"],
            cwd=kwargs["cwd"], env=kwargs["env"], capture_output=True, text=True,
            timeout=60)
        self.assertEqual(probe.returncode, 0, probe.stderr)
        self.assertEqual(probe.stdout.strip(), "[]")


class PublishStaysDryRunTests(unittest.TestCase):
    def test_no_tool_has_an_apply_parameter(self):
        module, server = _load_with_stub()
        for name in server.registered:
            with self.subTest(tool=name):
                self.assertNotIn("apply", inspect.signature(getattr(module, name)).parameters)

    def test_publish_never_builds_apply_into_argv(self):
        module, _server = _load_with_stub()
        with TemporaryDirectory() as tmp, mock.patch.dict(
                os.environ, {"FABRIC_AIDP_WORK_ROOT": tmp}):
            with mock.patch("subprocess.run", return_value=_Completed()) as run:
                module.publish(out_dir="migrated", prefix="me", cluster_key="ck")
        argv = run.call_args.args[0]
        self.assertNotIn("--apply", argv)
        self.assertEqual(argv[3], "publish")

    def test_the_docs_say_apply_is_cli_only_and_name_the_work_root(self):
        source = (ROOT / "fabric_aidp" / "mcp_server.py").read_text(encoding="utf-8")
        self.assertIn("--apply", source)
        for name in ("README.md", "docs/RUNBOOK.md", "CHANGELOG.md"):
            with self.subTest(file=name):
                text = (ROOT / name).read_text(encoding="utf-8")
                self.assertTrue("FABRIC_AIDP_WORK_ROOT" in text,
                                f"{name} does not document FABRIC_AIDP_WORK_ROOT")


if __name__ == "__main__":
    unittest.main()
