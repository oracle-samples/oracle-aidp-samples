import ast
import io
import inspect
import json
import contextlib
import unittest

from fabric_aidp.translate import m_parser
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.cli import build_parser, main

ROOT = Path(__file__).resolve().parent.parent

NB = ("# Fabric notebook source\n\n# CELL ********************\n\n"
      'df = spark.read.parquet("/lakehouse/default/Files/x")\n')


def _export(root: Path) -> Path:
    d = root / "Ingest.Notebook"
    d.mkdir(parents=True)
    (d / ".platform").write_text(json.dumps({
        "config": {"logicalId": "n1"},
        "metadata": {"type": "Notebook", "displayName": "Ingest"}}), encoding="utf-8")
    (d / "notebook-content.py").write_text(NB, encoding="utf-8")
    return root


def _run(argv):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = main(argv)
    return code, out.getvalue() + err.getvalue()


class ParserTests(unittest.TestCase):
    def test_four_verbs_are_registered(self):
        parser = build_parser()
        actions = [a for a in parser._actions if hasattr(a, "choices") and a.choices]
        verbs = set()
        for action in actions:
            verbs |= set(action.choices or {})
        for verb in ("inventory", "plan", "migrate", "verify"):
            self.assertIn(verb, verbs)

    def test_no_verb_is_a_usage_error(self):
        # argparse writes the usage error to the real stderr, which printed
        # "error: the following arguments are required: cmd" in the middle of
        # an otherwise-passing run. A reader seeing `error:` scroll past
        # reasonably concludes something broke. Capture it.
        with contextlib.redirect_stderr(io.StringIO()) as captured:
            with self.assertRaises(SystemExit):
                build_parser().parse_args([])
        self.assertIn("required", captured.getvalue())


class ErrorTests(unittest.TestCase):
    def test_missing_export_directory_exits_2(self):
        code, text = _run(["inventory", "/nonexistent/export"])
        self.assertEqual(code, 2)
        self.assertIn("error:", text)

    def test_missing_manifest_exits_2(self):
        code, _ = _run(["plan", "/nonexistent/manifest.json"])
        self.assertEqual(code, 2)

    def test_migrate_without_demo_succeeds(self):
        """No mode flag needed: migrate writes artifacts locally, and
        `publish` is the separate verb that reaches AIDP."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv, plan = out / "inv.json", out / "plan.json"
            self.assertEqual(_run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            self.assertEqual(_run(["plan", str(inv), "-o", str(plan)])[0], 0)
            code, _text = _run(["migrate", str(plan), "-o", str(out / "m")])
            self.assertEqual(code, 0)
            self.assertTrue((out / "m" / "report.json").is_file())


    def test_plan_catalog_flag_reaches_the_plan(self):
        """`plan --catalog` used to have nowhere to go: `build_plan` took no
        `catalog` keyword at all, so every table planned under `default`
        regardless of the flag."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv, plan_path = out / "inv.json", out / "plan.json"
            self.assertEqual(
                _run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            self.assertEqual(
                _run(["plan", str(inv), "-o", str(plan_path), "--catalog", "myc"])[0],
                0)
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(plan["target_aidp"]["catalog"], "myc")
            table = next(a["target"] for a in plan["assets"]
                        if a["id"] == "warehouse.AcmeDW.table.dbo.claim")
            self.assertEqual(table["name"], "myc.AcmeDW.claim")

    def test_migrate_catalog_flag_overrides_the_plans_own_catalog(self):
        """`migrate --catalog` re-targets an already-built plan without
        re-planning. Left unset (the common case), the plan's own recorded
        catalog is used untouched -- see test_migrate_runner.py for that."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv, plan_path = out / "inv.json", out / "plan.json"
            self.assertEqual(
                _run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            self.assertEqual(_run(["plan", str(inv), "-o", str(plan_path)])[0], 0)
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(plan["target_aidp"]["catalog"], "default")
            asset = next(a for a in plan["assets"]
                        if a["id"] == "warehouse.AcmeDW.table.dbo.claim")
            asset["source"]["sql"] = (
                "CREATE VIEW dbo.v AS SELECT * FROM AcmeDW.dbo.claim")
            plan_path.write_text(json.dumps(plan), encoding="utf-8")

            code, _text = _run(
                ["migrate", str(plan_path), "-o", str(out / "m"), "--catalog", "myc"])
            self.assertEqual(code, 0)
            report = json.loads((out / "m" / "report.json").read_text(encoding="utf-8"))
            row = next(r for r in report["results"]
                      if r["asset_id"] == "warehouse.AcmeDW.table.dbo.claim")
            written = (out / "m" / row["output_path"]).read_text(encoding="utf-8")
            self.assertIn("myc.AcmeDW.claim", written)
            self.assertNotIn("default.AcmeDW.claim", written)

    def test_a_catalog_with_a_dot_is_refused(self):
        """`--catalog a.b` did not make an odd-looking name, it made a
        four-part one. `--namespace` has been validated since the first
        release; this had nothing."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv = out / "inv.json"
            self.assertEqual(
                _run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            code, text = _run(["plan", str(inv), "-o", str(out / "p.json"),
                               "--catalog", "a.b"])
        self.assertEqual(code, 2)
        self.assertIn("catalog", text.lower())

    def test_a_catalog_with_a_space_is_refused(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv = out / "inv.json"
            self.assertEqual(
                _run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            code, _text = _run(["plan", str(inv), "-o", str(out / "p.json"),
                                "--catalog", "my catalog"])
        self.assertEqual(code, 2)

    def test_migrate_also_refuses_a_malformed_catalog(self):
        """`migrate --catalog` bypasses `plan` entirely."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv, plan_path = out / "inv.json", out / "plan.json"
            self.assertEqual(
                _run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            self.assertEqual(_run(["plan", str(inv), "-o", str(plan_path)])[0], 0)
            code, _text = _run(["migrate", str(plan_path), "-o", str(out / "m"),
                                "--catalog", "a.b"])
        self.assertEqual(code, 2)


class TablesCsvFlagTests(unittest.TestCase):
    """`--catalog` meant two unrelated things in one CLI: a CSV of known
    tables on `inventory`, the AIDP catalog on `plan` and `migrate`."""

    def test_inventory_takes_tables_csv(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            csv = out / "t.csv"
            csv.write_text("table\ndbo.extra\n", encoding="utf-8")
            code, _text = _run(["inventory", "--fixture", "demo",
                                "--tables-csv", str(csv), "-o", str(out / "i.json")])
            self.assertEqual(code, 0)
            manifest = json.loads((out / "i.json").read_text(encoding="utf-8"))
        self.assertIn("dbo.extra", manifest["resolved_catalog"]["tables"])

    def test_inventory_no_longer_takes_catalog(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            with self.assertRaises(SystemExit):
                _run(["inventory", "--fixture", "demo", "--catalog", "t.csv",
                      "-o", str(out / "i.json")])

    def test_verify_on_a_missing_report_exits_2(self):
        code, _ = _run(["verify", "/nonexistent/dir"])
        self.assertEqual(code, 2)

    def test_manifest_that_is_not_an_object_exits_2(self):
        with TemporaryDirectory() as t:
            bad = Path(t) / "m.json"
            bad.write_text("[1, 2]", encoding="utf-8")
            code, text = _run(["plan", str(bad)])
        self.assertEqual(code, 2)
        self.assertIn("JSON object", text)

    def test_unknown_source_exits_2(self):
        with TemporaryDirectory() as t:
            code, _ = _run(["inventory", str(_export(Path(t))), "--sources", "nope"])
        self.assertEqual(code, 2)


class EncodingTests(unittest.TestCase):
    def test_help_survives_a_non_utf8_pipe(self):
        # A redirected stdout on Windows is cp1252; the arrow in the parser
        # description raised UnicodeEncodeError and --help exited 1.
        import os, subprocess, sys
        env = dict(os.environ, PYTHONIOENCODING="cp1252", PYTHONUTF8="0")
        done = subprocess.run([sys.executable, "-m", "fabric_aidp.cli", "--help"],
                              capture_output=True, env=env, stdin=subprocess.DEVNULL,
                              cwd=str(Path(__file__).resolve().parents[1]))
        self.assertEqual(done.returncode, 0, done.stderr.decode("utf-8", "replace"))
        self.assertIn("→", done.stdout.decode("utf-8"))




@unittest.skipUnless(m_parser.parser_available(),
                     "Node + mparse not installed; no Dataflow parses, so the "
                     "dataflow slice is legitimately empty")
class SourceSliceContractTests(unittest.TestCase):
    """`--filter` must offer exactly what the verbs accept.

    argparse took its choices from the inventory's source list and the runner
    kept its own, so `migrate --help` advertised `--filter dataflow` and
    `migrate --filter dataflow` answered "unknown migration filter
    'dataflow'". `verify --filter dataflow` was the same split. Dataflows are
    migrated -- 16 of the 106 assets in the bundled estate -- so the help was
    right and both runners were wrong.
    """

    def _choices(self, verb):
        parser = build_parser()
        sub = next(a for a in parser._actions if a.dest == "cmd")
        action = next(a for a in sub.choices[verb]._actions if a.dest == "filter")
        return set(action.choices)

    def test_every_advertised_migrate_filter_is_accepted(self):
        from fabric_aidp.migrate.runner import FILTER_KINDS
        self.assertEqual(self._choices("migrate"), set(FILTER_KINDS))

    def test_every_advertised_verify_filter_is_accepted(self):
        from fabric_aidp.verify.checker import SOURCE_NAMES
        self.assertEqual(self._choices("verify"), set(SOURCE_NAMES))

    def test_migrate_and_verify_offer_the_same_slices(self):
        self.assertEqual(self._choices("migrate"), self._choices("verify"))

    def test_migrate_filter_dataflow_runs_the_dataflow_slice(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv, plan = out / "inv.json", out / "plan.json"
            self.assertEqual(
                _run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            self.assertEqual(_run(["plan", str(inv), "-o", str(plan),
                                   "--namespace", "demons"])[0], 0)
            code, text = _run(["migrate", str(plan), "-o", str(out / "m"),
                               "--filter", "dataflow"])
            self.assertEqual(code, 0, text)
            report = json.loads((out / "m" / "report.json").read_text(encoding="utf-8"))
        kinds = {r["kind"] for r in report["results"]}
        self.assertTrue(report["results"], "the dataflow slice ran zero assets")
        self.assertEqual(kinds, {"dataflow_query"})

    def test_verify_filter_dataflow_selects_the_dataflow_rows(self):
        from fabric_aidp.verify.checker import verify as verify_report
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = {"report_id": "r", "complete": True,
                      "counts": {"blocked": 1, "planned": 1},
                      "results": [
                          {"asset_id": "dataflow.Flow.q", "kind": "dataflow_query",
                           "status": "blocked"},
                          {"asset_id": "notebook.N", "kind": "notebook",
                           "status": "planned"}]}
            path = out / "report.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            result = verify_report(path, filter_kind="dataflow")
        self.assertEqual([r["asset_id"] for r in result["rows"]], ["dataflow.Flow.q"])


class NamespaceSourcesTests(unittest.TestCase):
    """Where the OCI namespace is allowed to come from, and in what order.

    Two halves, both reproduced. `inventory --namespace flagns` is
    documented as "recorded for plan" and was parsed and then never read --
    the string appears nowhere in the manifest. And `.env` was loaded only
    by `inventory`, so `OCI_NAMESPACE=envns` in the working directory left
    `plan` on the placeholder.

    Precedence: `plan --namespace`, then the namespace `inventory
    --namespace` recorded in this manifest, then $OCI_NAMESPACE (which .env
    feeds), then the placeholder. A flag someone typed beats ambient
    configuration even when they typed it one verb earlier.
    """

    def _inventory(self, out: Path, *extra):
        code, text = _run(["inventory", "--fixture", "demo",
                           "-o", str(out / "inv.json"), *extra])
        self.assertEqual(code, 0, text)
        return out / "inv.json"

    def _plan_namespace(self, inv: Path, out: Path, *extra):
        code, text = _run(["plan", str(inv), "-o", str(out / "p.json"), *extra])
        self.assertEqual(code, 0, text)
        return json.loads((out / "p.json").read_text(
            encoding="utf-8"))["target_aidp"]["namespace"]

    def test_inventory_namespace_is_recorded_in_the_manifest(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv = self._inventory(out, "--namespace", "flagns")
            manifest = json.loads(inv.read_text(encoding="utf-8"))
        self.assertEqual(manifest["oci_namespace"], "flagns")

    def test_the_inventorys_namespace_reaches_the_plan(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv = self._inventory(out, "--namespace", "flagns")
            self.assertEqual(self._plan_namespace(inv, out), "flagns")

    def test_plan_namespace_wins_over_the_inventorys(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv = self._inventory(out, "--namespace", "flagns")
            self.assertEqual(
                self._plan_namespace(inv, out, "--namespace", "planns"), "planns")

    def test_a_bad_inventory_namespace_is_refused_at_inventory_time(self):
        with TemporaryDirectory() as tmp:
            code, text = _run(["inventory", "--fixture", "demo", "--namespace",
                               "Bad NS", "-o", str(Path(tmp) / "i.json")])
        self.assertEqual(code, 2)
        self.assertIn("namespace", text.lower())

    def test_no_namespace_anywhere_is_still_the_placeholder(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv = self._inventory(out)
            self.assertEqual(self._plan_namespace(inv, out), "<your-oci-namespace>")

    def test_plan_reads_oci_namespace_from_a_dotenv(self):
        """`plan` never called load_dotenv, so a .env beside the manifest was
        read by `inventory` and ignored by every other verb."""
        import os
        import subprocess
        import sys
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / ".env").write_text("OCI_NAMESPACE=envns\n", encoding="utf-8")
            env = {k: v for k, v in os.environ.items() if k != "OCI_NAMESPACE"}
            env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
            for argv in (["inventory", "--fixture", "demo", "-o", "inv.json"],
                         ["plan", "inv.json", "-o", "p.json"]):
                done = subprocess.run([sys.executable, "-m", "fabric_aidp.cli", *argv],
                                      capture_output=True, cwd=str(out), env=env)
                self.assertEqual(done.returncode, 0, done.stderr.decode("utf-8", "replace"))
            plan = json.loads((out / "p.json").read_text(encoding="utf-8"))
        self.assertEqual(plan["target_aidp"]["namespace"], "envns")


class DeadDemoFlagIsGoneTests(unittest.TestCase):
    """`migrate --demo` was accepted and ignored: dead CLI surface.

    THE TRADE. Removing it breaks any script still passing it -- argparse
    exits 2 with "unrecognized arguments: --demo". Keeping it means a flag
    that does nothing sits in `migrate --help` and in the MCP tool signature
    forever, and four tests exist only to stop the documentation teaching it.

    Removed, for two reasons that are both facts rather than preferences.
    Nothing has shipped: CHANGELOG.md says "0.1.0 -- unreleased ... Nothing
    has been published to an index yet", so no installed copy can be passing
    it, and the only in-tree callers were `mcp_server.migrate` (whose `demo`
    parameter defaulted to True, so every MCP call passed it) and one
    end-to-end test -- both fixed in the same commit. And the repository
    settled this exact question one change earlier, in the other direction
    from keeping: `inventory --catalog` was *renamed* to `--tables-csv`
    rather than aliased, and cli.py still records why -- "Nothing is
    released, so it is renamed rather than aliased". Keeping `--demo` while
    having renamed `--catalog` would be two answers to one question.

    0.1.0 is the last moment this is free.
    """

    def test_the_cli_no_longer_accepts_it(self):
        """argparse exits 2 on an unknown option before `main` can catch
        anything, and writes the reason on the way out. Captured, because a
        bare `SystemExit: 2` would not say which flag."""
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as caught:
                main(["migrate", "plan.json", "--demo"])
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("--demo", stderr.getvalue())
        self.assertIn("unrecognized arguments", stderr.getvalue())

    def test_the_parser_has_no_such_option(self):
        parser = build_parser()
        sub = next(a for a in parser._actions if a.dest == "cmd")
        options = {flag for action in sub.choices["migrate"]._actions
                   for flag in action.option_strings}
        self.assertNotIn("--demo", options)

    def test_the_runner_takes_no_demo_argument(self):
        from fabric_aidp.migrate.runner import migrate as run_migrate
        self.assertNotIn("demo", inspect.signature(run_migrate).parameters)

    def test_migrate_still_writes_locally_with_no_mode_flag_at_all(self):
        """The reason the flag could go: `migrate` has written locally
        unconditionally since `publish` became the verb that writes to
        AIDP."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            inv, plan = out / "inv.json", out / "plan.json"
            self.assertEqual(
                _run(["inventory", "--fixture", "demo", "-o", str(inv)])[0], 0)
            self.assertEqual(_run(["plan", str(inv), "-o", str(plan),
                                   "--namespace", "demons"])[0], 0)
            code, text = _run(["migrate", str(plan), "-o", str(out / "m")])
            self.assertEqual(code, 0, text)
            self.assertTrue((out / "m" / "report.json").is_file())

    def test_no_shipped_surface_still_builds_the_flag(self):
        """The MCP server appended `--demo` on every call, because its own
        `demo` parameter defaulted to True. A tool that builds argv the CLI
        rejects is broken for every agent that calls it.

        A string literal, not a text search: the comments recording why the
        flag went away name it, and should. What must not exist is code that
        can put it on a command line.
        """
        for path in sorted((ROOT / "fabric_aidp").rglob("*.py")):
            if "fixtures" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            literals = {node.value for node in ast.walk(tree)
                        if isinstance(node, ast.Constant)
                        and isinstance(node.value, str)}
            with self.subTest(file=path.name):
                # assertFalse, not assertNotIn: a failed assertNotIn prints
                # every string literal in the module, and the useful half of
                # that message is the one flag.
                self.assertFalse(
                    "--demo" in literals,
                    f"{path.name} still has a `--demo` string literal, so "
                    f"something there can still build argv the CLI rejects")


if __name__ == "__main__":
    unittest.main()
