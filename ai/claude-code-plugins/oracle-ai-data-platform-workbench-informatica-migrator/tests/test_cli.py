"""CLI dispatcher contract tests."""
from __future__ import annotations

import pytest

from infa2aidp.cli import COMMANDS, main


EXPECTED = {
    "discover", "analyze", "migrate", "deploy", "reconcile",
    "optimize", "review", "rag", "lineage", "version",
}


def test_command_set_is_exactly_the_supported_commands():
    assert set(COMMANDS) == EXPECTED


def test_every_command_has_help_and_a_callable_handler():
    for name, cmd in COMMANDS.items():
        assert cmd.help.strip(), f"{name} has no help text"
        assert callable(cmd.handler), f"{name} handler is not callable"


def test_no_dashboard_command():
    assert "dashboard" not in COMMANDS
    assert "crawl" not in COMMANDS


def test_version_command_exits_zero(capsys):
    assert main(["version"]) == 0
    out = capsys.readouterr().out
    assert "infa2aidp" in out


def test_unknown_command_exits_nonzero():
    with pytest.raises(SystemExit) as exc:
        main(["frobnicate"])
    assert exc.value.code != 0


def test_no_args_prints_help_and_exits_nonzero(capsys):
    assert main([]) != 0
    assert "usage" in capsys.readouterr().out.lower()


def test_build_parser_has_a_subparser_per_command():
    from infa2aidp.cli import build_parser
    parser = build_parser()
    sub_actions = [
        a for a in parser._subparsers._group_actions
        if hasattr(a, "choices")
    ]
    choices = sub_actions[0].choices
    assert set(choices) == EXPECTED


def test_discover_requires_host():
    with pytest.raises(SystemExit):
        main(["discover"])


def test_reconcile_requires_config():
    with pytest.raises(SystemExit):
        main(["reconcile"])


def test_migrate_has_comparison_and_workers_flags():
    from infa2aidp.cli import build_parser
    parser = build_parser()
    sub_actions = [a for a in parser._subparsers._group_actions if hasattr(a, "choices")][0]
    migrate_opts = {
        opt for action in sub_actions.choices["migrate"]._actions
        for opt in action.option_strings
    }
    assert "--comparison" in migrate_opts
    assert "--workers" in migrate_opts


def test_migrate_defaults_to_one_worker():
    from infa2aidp.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["migrate", "-i", "in.xml", "-o", "out"])
    assert args.workers == 1
    assert args.comparison is False


def test_migrate_is_a_thin_adapter_onto_run_migration(monkeypatch, tmp_path, capsys):
    """cli._cmd_migrate must delegate all orchestration to
    infa2aidp.migrator.run_migration rather than reimplementing it."""
    calls = {}

    class _FakeResult:
        notebooks = 3
        workflows = 1
        output_dir = str(tmp_path)
        confidence_summary = {"auto_rate": 80.0}
        fidelity_summary = {}
        optimize_suggestions = 0
        workflow_reviews = {}
        workflow_assumptions = {}

    def _fake_run_migration(inputs, output_dir, **kwargs):
        calls["inputs"] = inputs
        calls["output_dir"] = output_dir
        calls["kwargs"] = kwargs
        return _FakeResult()

    monkeypatch.setattr("infa2aidp.migrator.run_migration", _fake_run_migration)

    xml_file = tmp_path / "m.xml"
    xml_file.write_text("<MAPPING/>", encoding="utf-8")

    exit_code = main([
        "migrate", "-i", str(xml_file), "-o", str(tmp_path),
        "--use-llm", "--comparison", "--workers", "3",
    ])
    assert exit_code == 0
    assert calls["kwargs"]["use_llm"] is True
    assert calls["kwargs"]["emit_comparison"] is True
    assert calls["kwargs"]["max_workers"] == 3
    out = capsys.readouterr().out
    assert "3 notebook(s)" in out
    assert "80.0%" in out


def test_deploy_workspace_path_falls_back_to_env_var(monkeypatch, tmp_path):
    """Regression test: the old CLI compared args.workspace_path to the
    sentinel default '/Migrated' to decide whether to honor
    AIDP_WORKSPACE_PATH, which could never fire because argparse's own
    default was that same sentinel. The flag must now default to None so
    the env var actually takes effect when --workspace-path isn't passed."""
    captured = {}

    class _FakeResult:
        total_uploaded = 0
        total_updated = 0
        total_failed = 0
        total_skipped = 0
        total_dry_run = 0
        dry_run = True

    class _FakeDeployer:
        def __init__(self, config):
            captured["workspace_path"] = config.workspace_path

        def deploy(self, *a, **kw):
            return _FakeResult()

        def generate_deploy_report(self, *a, **kw):
            pass

    monkeypatch.setattr("infa2aidp.deployer.deployer.AIDPDeployer", _FakeDeployer)
    # Any folder under /Workspace is valid; a path outside it is refused by
    # DeployConfig (see test_live_run_regressions.py).
    monkeypatch.setenv("AIDP_WORKSPACE_PATH", "/Workspace/CustomFolder")

    (tmp_path / "workflows").mkdir()
    exit_code = main(["deploy", "-i", str(tmp_path), "-o", str(tmp_path / "report"), "--dry-run"])

    assert exit_code == 0
    assert captured["workspace_path"] == "/Workspace/CustomFolder"
