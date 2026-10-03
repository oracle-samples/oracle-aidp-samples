"""The CLI's handlers, run in-process.

tests/test_cli.py pins the command set; the handlers themselves were only
ever reached through subprocesses, which neither coverage nor a failing
assertion inside them can see. Here each handler runs through ``main()``
with its library entry point replaced by a recording fake (so nothing talks
to AIDP, PowerCenter, an LLM or a database), and what is pinned is the
contract a CI job relies on: which arguments reach the library, what is
written where, and the exit code.
"""
from __future__ import annotations

import json
import os
import types

import pytest

from infa2aidp import cli
from infa2aidp.cli import _collect_input_files, _merge_migration_results, build_parser, main
from infa2aidp.models import InfaVersion, Mapping, MigrationResult

ROOT = os.path.dirname(os.path.dirname(__file__))
ORDERS = os.path.join(ROOT, "tests", "fixtures", "corpus", "orders_transform.xml")


# ---------------------------------------------------------------------------
# Parser and dispatcher
# ---------------------------------------------------------------------------

class TestDispatcher:
    def test_no_command_prints_help_and_exits_2(self, capsys):
        assert main([]) == 2
        assert "usage: infa2aidp" in capsys.readouterr().out

    def test_defaults_for_each_command(self):
        p = build_parser()
        a = p.parse_args(["analyze", "-i", "x"])
        assert (a.output, a.format, a.verbose) == ("./analysis_report", "all", False)
        m = p.parse_args(["migrate", "-i", "x", "-o", "y"])
        assert (m.workers, m.use_llm, m.target_catalog_type) == (1, False, cli._config.TARGET_CATALOG_TYPE)
        d = p.parse_args(["deploy", "-i", "x", "--dry-run"])
        assert d.dry_run and d.output is None and not d.overwrite
        disc = p.parse_args(["discover", "--host", "h"])
        assert (disc.port, disc.method, disc.output) == (6005, "auto", "./crawl_output")
        r = p.parse_args(["rag", "stats"])
        assert (r.action, r.output) == ("stats", "rag_export.json")

    def test_required_flags_and_choices_are_enforced(self):
        p = build_parser()
        for argv in (["analyze"], ["migrate", "-i", "x"], ["discover"],
                     ["analyze", "-i", "x", "--format", "pdf"],
                     ["migrate", "-i", "x", "-o", "y", "--target-catalog-type", "hive"]):
            with pytest.raises(SystemExit):
                p.parse_args(argv)

    def test_handler_exception_is_exit_1_unless_verbose(self, monkeypatch):
        def boom(args):
            raise RuntimeError("kaput")
        monkeypatch.setitem(cli.COMMANDS, "version", cli.Command("version", "v", boom))
        assert main(["version"]) == 1
        with pytest.raises(RuntimeError, match="kaput"):
            main(["version", "-v"])

    def test_keyboard_interrupt_is_130(self, monkeypatch):
        def interrupted(args):
            raise KeyboardInterrupt
        monkeypatch.setitem(cli.COMMANDS, "version", cli.Command("version", "v", interrupted))
        assert main(["version"]) == 130

    def test_version_reports_llm_configuration(self, monkeypatch, capsys):
        monkeypatch.setattr(cli._config, "ANTHROPIC_API_KEY", "")
        assert main(["version"]) == 0
        assert "rule-based only" in capsys.readouterr().out
        monkeypatch.setattr(cli._config, "ANTHROPIC_API_KEY", "set")
        monkeypatch.setattr(cli._config, "CLAUDE_MODEL", "model-x")
        assert main(["version"]) == 0
        assert "configured (model: model-x)" in capsys.readouterr().out


class TestInputCollection:
    def test_file_directory_and_errors(self, tmp_path):
        f = tmp_path / "one.xml"
        f.write_text("<x/>", encoding="utf-8")
        assert _collect_input_files(str(f)) == [str(f)]
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "two.JSON").write_text("{}", encoding="utf-8")
        (tmp_path / "notes.txt").write_text("x", encoding="utf-8")
        got = _collect_input_files(str(tmp_path))
        assert [os.path.basename(p) for p in got] == ["one.xml", "two.JSON"]
        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises(FileNotFoundError, match="No Informatica exports"):
            _collect_input_files(str(empty))
        with pytest.raises(FileNotFoundError, match="does not exist"):
            _collect_input_files(str(tmp_path / "missing"))

    def test_merge_keeps_first_version_repository_and_folder(self):
        a = MigrationResult(mappings=[Mapping("m1")])
        b = MigrationResult(version=InfaVersion.V10_5, version_detail="10.5.4",
                            repository_name="REP", folder_name="F1",
                            mappings=[Mapping("m2")], sessions=["s"], workflows=["w"],
                            compatibility_issues=["i"])
        c = MigrationResult(version=InfaVersion.V9, version_detail="9.6",
                            repository_name="OTHER", folder_name="F2")
        m = _merge_migration_results([a, b, c])
        assert [x.name for x in m.mappings] == ["m1", "m2"]
        assert (m.sessions, m.workflows, m.compatibility_issues) == (["s"], ["w"], ["i"])
        assert (m.version, m.version_detail) == (InfaVersion.V10_5, "10.5.4")
        assert (m.repository_name, m.folder_name) == ("REP", "F1")


# ---------------------------------------------------------------------------
# analyze
# ---------------------------------------------------------------------------

class TestAnalyze:
    def test_analyze_a_real_export_writes_reports(self, tmp_path, capsys):
        out = tmp_path / "rep"
        assert main(["analyze", "-i", ORDERS, "-o", str(out), "--format", "json"]) == 0
        printed = capsys.readouterr().out
        assert "Mappings: 1" in printed and "Compatibility:" in printed
        assert any(p.suffix == ".json" for p in out.iterdir())

    def test_one_bad_file_is_skipped_and_reported(self, tmp_path, capsys):
        src = tmp_path / "in"
        src.mkdir()
        (src / "a_good.xml").write_bytes(open(ORDERS, "rb").read())
        (src / "b_bad.xml").write_text("<not-informatica", encoding="utf-8")
        assert main(["analyze", "-i", str(src), "-o", str(tmp_path / "rep"), "--format", "markdown"]) == 0
        printed = capsys.readouterr().out
        assert "Skipped 1 file(s)" in printed and "b_bad.xml" in printed

    def test_every_file_failing_is_exit_1(self, tmp_path):
        bad = tmp_path / "bad.xml"
        bad.write_text("<nope", encoding="utf-8")
        assert main(["analyze", "-i", str(bad), "-o", str(tmp_path / "rep")]) == 1


# ---------------------------------------------------------------------------
# migrate / deploy
# ---------------------------------------------------------------------------

class TestMigrate:
    def test_flags_reach_run_migration(self, tmp_path, monkeypatch, capsys):
        import infa2aidp.migrator as migrator
        seen = {}

        def fake_run(inputs, output, **kw):
            seen.update(inputs=inputs, output=output, **kw)
            return types.SimpleNamespace(optimize_suggestions=3)
        monkeypatch.setattr(migrator, "run_migration", fake_run)
        monkeypatch.setattr(migrator, "format_run_summary", lambda r: "SUMMARY-LINE")
        rc = main(["migrate", "-i", ORDERS, "-o", str(tmp_path), "--use-llm", "--agentic",
                   "--custom-rules", "r.yaml", "--params", "p.par", "--workers", "4",
                   "--comparison", "--skip-lineage", "--skip-optimize",
                   "--target-catalog-type", "adw",
                   "--schedule-timezone", "America/New_York"])
        assert rc == 0
        assert "SUMMARY-LINE" in capsys.readouterr().out
        assert seen == dict(inputs=[ORDERS], output=str(tmp_path), use_llm=True, agentic=True,
                            custom_rules_path="r.yaml", params_path="p.par", max_workers=4,
                            emit_comparison=True, skip_lineage=True, skip_optimize=True,
                            target_catalog_type="adw",
                            schedule_timezone="America/New_York")


class FakeDeployer:
    instances: list = []

    def __init__(self, config):
        self.config = config
        self.deployed = None
        FakeDeployer.instances.append(self)

    def deploy(self, nb_dir, wf_dir):
        self.deployed = (nb_dir, wf_dir)
        return types.SimpleNamespace(total_uploaded=2, total_updated=0, total_failed=self.fail,
                                     total_skipped=1, total_dry_run=2, dry_run=self.config.dry_run)

    fail = 0

    def generate_deploy_report(self, result, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("report")


class TestDeploy:
    @pytest.fixture(autouse=True)
    def _isolate(self, monkeypatch):
        import infa2aidp.deployer.deployer as dep
        FakeDeployer.instances = []
        FakeDeployer.fail = 0
        monkeypatch.setattr(dep, "AIDPDeployer", FakeDeployer)
        for name in ("AIDP_REGION", "AIDP_INSTANCE_ID", "AIDP_WORKSPACE_KEY", "OCI_PROFILE"):
            monkeypatch.setattr(cli._config, name, "", raising=False)
            monkeypatch.delenv(name, raising=False)
        for name in ("AIDP_CLUSTER_KEY", "AIDP_WORKSPACE_PATH"):
            monkeypatch.delenv(name, raising=False)

    def test_missing_target_is_refused_without_dry_run(self, tmp_path):
        assert main(["deploy", "-i", str(tmp_path)]) == 1
        assert FakeDeployer.instances == []

    def test_dry_run_needs_no_target_and_reports_beside_the_input(self, tmp_path, capsys):
        (tmp_path / "workflows").mkdir()
        assert main(["deploy", "-i", str(tmp_path), "--dry-run"]) == 0
        (d,) = FakeDeployer.instances
        assert d.config.dry_run and d.config.oci_profile == "DEFAULT"
        assert d.config.workspace_path == "/Workspace/Migrated"
        assert d.deployed == (str(tmp_path), os.path.join(str(tmp_path), "workflows"))
        assert (tmp_path / "reports" / "deploy_report.md").read_text(encoding="utf-8") == "report"
        out = capsys.readouterr().out
        assert "Would deploy: 2" in out and "DRY RUN" in out

    def test_flags_build_the_config_and_failures_exit_1(self, tmp_path, monkeypatch):
        FakeDeployer.fail = 1
        monkeypatch.setenv("AIDP_CLUSTER_KEY", "ck-env")
        rc = main(["deploy", "-i", str(tmp_path), "-o", str(tmp_path / "o"), "--region", "r",
                   "--instance-id", "dl", "--workspace-key", "ws", "--profile", "P",
                   "--workspace-path", "/Workspace/X", "--overwrite"])
        assert rc == 1
        (d,) = FakeDeployer.instances
        c = d.config
        assert (c.region, c.instance_id, c.workspace_key, c.oci_profile) == ("r", "dl", "ws", "P")
        assert (c.cluster_key, c.workspace_path, c.overwrite, c.dry_run) == ("ck-env", "/Workspace/X", True, False)
        assert d.deployed == (str(tmp_path), None)       # no workflows/ dir
        assert (tmp_path / "o" / "deploy_report.md").exists()


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------

class TestReconcile:
    def _run(self, tmp_path, monkeypatch, statuses, fmt="all"):
        import infa2aidp.reconciler.reconciler as rec
        from infa2aidp.reconciler.models import ReconcileResult

        class FakeReconciler:
            def load_config(self, path):
                assert path == "cfg.yaml"
                return [object()] * len(statuses)

            def reconcile_batch(self, configs):
                return [ReconcileResult(config_name=f"c{i}", reconcile_type="row_count", status=s)
                        for i, s in enumerate(statuses)]
        monkeypatch.setattr(rec, "DataReconciler", FakeReconciler)
        out = tmp_path / "rr"
        rc = main(["reconcile", "-c", "cfg.yaml", "-o", str(out), "--format", fmt])
        return rc, out

    def test_writes_requested_formats(self, tmp_path, monkeypatch):
        rc, out = self._run(tmp_path, monkeypatch, ["PASSED"])
        assert rc == 0
        assert sorted(p.name for p in out.iterdir()) == [
            "reconcile_report.csv", "reconcile_report.json", "reconcile_report.md"]
        rc, out2 = self._run(tmp_path / "b", monkeypatch, ["PASSED"], fmt="json")
        assert [p.name for p in out2.iterdir()] == ["reconcile_report.json"]

    def test_failed_reconciliation_exits_nonzero(self, tmp_path, monkeypatch, capsys):
        rc, _ = self._run(tmp_path, monkeypatch, ["PASSED", "FAILED"])
        assert "1 passed, 1 failed, 0 error(s)" in capsys.readouterr().out
        assert rc == 1


# ---------------------------------------------------------------------------
# optimize
# ---------------------------------------------------------------------------

PY_NOTEBOOK = "df = orders.join(lkp_cust, 'ID')\n"


def _ipynb(code: str) -> str:
    return json.dumps({"cells": [
        {"cell_type": "markdown", "source": ["# title"]},
        {"cell_type": "code", "source": [code]},
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5})


class TestOptimize:
    def test_missing_directory_is_exit_1(self, tmp_path):
        assert main(["optimize", "-i", str(tmp_path / "nope"), "-o", str(tmp_path / "o")]) == 1

    def test_report_covers_py_and_ipynb_and_nothing_is_rewritten_by_default(self, tmp_path, capsys):
        src = tmp_path / "nb"
        (src / "sub").mkdir(parents=True)
        (src / "a.py").write_text(PY_NOTEBOOK, encoding="utf-8")
        (src / "sub" / "b.ipynb").write_text(_ipynb(PY_NOTEBOOK), encoding="utf-8")
        (src / "c.ipynb").write_text("not json", encoding="utf-8")
        out = tmp_path / "o"
        assert main(["optimize", "-i", str(src), "-o", str(out)]) == 0
        md = (out / "optimization_report.md").read_text(encoding="utf-8")
        assert "- Notebooks analyzed: 3" in md
        assert "## a.py" in md and "## b.ipynb" in md
        assert "3 notebook(s)" in capsys.readouterr().out
        assert (src / "a.py").read_text(encoding="utf-8") == PY_NOTEBOOK

    def test_auto_apply_rewrites_a_python_notebook(self, tmp_path):
        src = tmp_path / "nb"
        src.mkdir()
        (src / "a.py").write_text(PY_NOTEBOOK, encoding="utf-8")
        assert main(["optimize", "-i", str(src), "-o", str(tmp_path / "o"), "--auto-apply"]) == 0
        assert ".join(F.broadcast(lkp_cust)," in (src / "a.py").read_text(encoding="utf-8")

    def test_auto_apply_keeps_an_ipynb_a_notebook(self, tmp_path):
        src = tmp_path / "nb"
        src.mkdir()
        (src / "b.ipynb").write_text(_ipynb(PY_NOTEBOOK), encoding="utf-8")
        assert main(["optimize", "-i", str(src), "-o", str(tmp_path / "o"), "--auto-apply"]) == 0
        nb = json.loads((src / "b.ipynb").read_text(encoding="utf-8"))
        assert [c["cell_type"] for c in nb["cells"]] == ["markdown", "code"]
        assert ".join(F.broadcast(lkp_cust)," in "".join(nb["cells"][1]["source"])


# ---------------------------------------------------------------------------
# review / rag / lineage / discover
# ---------------------------------------------------------------------------

class TestReview:
    @pytest.fixture
    def fakes(self, monkeypatch):
        import infa2aidp.agents.pipeline as pipeline
        import infa2aidp.agents.reviewer as reviewer
        calls = {}

        class FakeReviewer:
            def generate_review_file(self, records, path, include_confidence):
                calls["generate"] = (list(records), path, include_confidence)
                return len(records)

            def import_review_file(self, path):
                calls["import"] = path
                return [types.SimpleNamespace(decision=d) for d in ("approved", "approved", "edited", "rejected")]

            def generate_review_report(self, reviewed, path):
                calls["report"] = path

        class FakePipeline:
            def convert_mapping(self, mapping):
                return [f"rec:{mapping.name}"]
        monkeypatch.setattr(reviewer, "HumanReviewer", FakeReviewer)
        monkeypatch.setattr(pipeline, "ConversionPipeline", FakePipeline)
        return calls

    def test_generate_converts_every_mapping(self, tmp_path, fakes, capsys):
        out = tmp_path / "r" / "review.yaml"
        assert main(["review", "generate", "-i", ORDERS, "-o", str(out)]) == 0
        records, path, conf = fakes["generate"]
        assert records and all(r.startswith("rec:") for r in records)
        assert (path, conf) == (str(out), ["LOW", "MANUAL", "MEDIUM"])
        assert out.parent.is_dir()
        assert f"({len(records)} item(s) to review)" in capsys.readouterr().out

    def test_import_summarises_decisions(self, tmp_path, fakes, capsys):
        assert main(["review", "import", "-i", "done.yaml", "-o", str(tmp_path / "o")]) == 0
        assert fakes["import"] == "done.yaml"
        assert fakes["report"] == os.path.join(str(tmp_path / "o"), "review_report.md")
        assert "Approved: 2  Edited: 1  Rejected: 1" in capsys.readouterr().out


class TestRag:
    @pytest.fixture
    def store(self, monkeypatch):
        import infa2aidp.agents.rag_store as rag

        class FakeStore:
            last = None

            def __init__(self):
                self.entries = [types.SimpleNamespace(id=i, transformation_type="Expression",
                                                      approved=i % 2 == 0, used_count=i)
                                for i in range(3)]
                self.saved = False
                self.exported = self.imported = None
                FakeStore.last = self

            def stats(self):
                return {"total_entries": len(self.entries), "approved_entries": 2, "total_retrievals": 7}

            def list_entries(self, approved_only=False):
                return [e for e in self.entries if e.approved or not approved_only]

            def export(self, path):
                self.exported = path

            def import_entries(self, path):
                self.imported = path

            def _save(self):
                self.saved = True
        monkeypatch.setattr(rag, "RAGStore", FakeStore)
        return FakeStore

    def test_stats_list_export_import_clear(self, store, capsys):
        assert main(["rag", "stats"]) == 0
        assert "Entries: 3  Approved: 2  Retrievals: 7" in capsys.readouterr().out
        assert main(["rag", "list", "--approved-only"]) == 0
        listed = capsys.readouterr().out.splitlines()
        assert listed == ["[0] Expression -- approved -- used 0x", "[2] Expression -- approved -- used 2x"]
        assert main(["rag", "export", "-o", "x.json"]) == 0
        assert store.last.exported == "x.json"
        assert main(["rag", "import", "-i", "in.json"]) == 0
        assert store.last.imported == "in.json"
        assert main(["rag", "clear"]) == 0
        assert store.last.entries == [] and store.last.saved
        assert "Cleared 3 entries" in capsys.readouterr().out


class TestLineageAndDiscover:
    def test_lineage_report_per_mapping(self, tmp_path, capsys):
        out = tmp_path / "lin"
        assert main(["lineage", "-i", ORDERS, "-o", str(out)]) == 0
        assert "Lineage generated for 1 mapping(s)" in capsys.readouterr().out
        assert any(out.iterdir())

    def test_discover_wires_credentials_folders_and_report(self, tmp_path, monkeypatch, capsys):
        import infa2aidp.crawlers.informatica_crawler as crawler_mod
        monkeypatch.setenv("INFA_PASSWORD", "from-env")
        seen = {}

        class FakeCrawler:
            def __init__(self, cfg):
                seen["cfg"] = cfg

            def connect(self, method):
                seen["method"] = method
                return "soap"

            def crawl_repository(self, out_dir, folders=None, export_xml=False):
                seen["crawl"] = (out_dir, folders, export_xml)
                return types.SimpleNamespace(mappings=[1, 2], workflows=[1], exported_xml_files=[],
                                             errors=["login failed"])

            def generate_inventory_report(self, result, path):
                seen["report"] = path

            def disconnect(self):
                seen["closed"] = True

        monkeypatch.setattr(crawler_mod, "InformaticaCrawler", FakeCrawler)
        rc = main(["discover", "--host", "pc.example", "--user", "admin", "--repo", "REP",
                   "--method", "soap", "--folders", "SALES,HR", "-o", str(tmp_path)])
        assert rc == 1                     # errors and nothing exported
        cfg = seen["cfg"]
        assert (cfg.host, cfg.port, cfg.username, cfg.password, cfg.repository) == (
            "pc.example", 6005, "admin", "from-env", "REP")
        assert seen["method"] == "soap"
        assert seen["crawl"] == (os.path.join(str(tmp_path), "exported_xml"), ["SALES", "HR"], True)
        assert seen["report"] == os.path.join(str(tmp_path), "infa_inventory_report.md")
        assert seen["closed"]
        assert "2 mappings, 1 workflows, 0 XMLs exported" in capsys.readouterr().out
