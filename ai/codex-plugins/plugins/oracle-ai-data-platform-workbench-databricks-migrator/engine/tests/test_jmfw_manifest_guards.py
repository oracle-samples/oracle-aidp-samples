"""Pinning tests for scripts/job_migrate_from_workflow.py manifest guards
(PR #68 review round 2).

  - derive_export_base: the GIT export-subtree alternation is UPPERCASE-only.
    A lowercase user folder like /notebooks/git/ must NOT derive an export
    base (false positive that wrongly engaged EXPORT_BASE path-collapsing
    for ordinary jobs).
  - build_dep_resolution (--manifest branch): a task with a missing or null
    notebook_path is skipped with a warning instead of crashing the run
    (KeyError) or inserting a None parent key into the map.
"""
import asyncio
import json
import os
import sys
from types import SimpleNamespace

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "scripts"))

import job_migrate_from_workflow as jmfw  # noqa: E402
import job_migrate as jm  # noqa: E402


# ─── derive_export_base: GIT alternation is uppercase-only ─────────────────

def test_lowercase_git_user_folder_does_not_derive_export_base():
    manifest = {"jobs": [{"tasks": [
        {"task_key": "t1",
         # A user's OWN folder named notebooks/git — NOT a dbx_export tree.
         "notebook_path": "/Workspace/Users/bob@co.com/notebooks/git/helper"},
    ]}]}
    assert jmfw.derive_export_base(manifest) == ""


def test_capitalized_git_user_folder_does_not_derive_export_base():
    manifest = {"jobs": [{"tasks": [
        {"task_key": "t1",
         "notebook_path": "/Workspace/Users/bob@co.com/notebooks/Git/helper"},
    ]}]}
    assert jmfw.derive_export_base(manifest) == ""


def test_uppercase_git_export_tree_still_derives_export_base():
    manifest = {"jobs": [{"tasks": [
        {"task_key": "t1",
         "notebook_path": "/Workspace/exports/notebooks/GIT/utils/x"},
    ]}]}
    assert jmfw.derive_export_base(manifest) == "exports/notebooks"


def test_users_export_tree_still_derives_export_base():
    manifest = {"jobs": [{"tasks": [
        {"task_key": "t1",
         "notebook_path": "/Workspace/exports/notebooks/Users/a@co.com/nb1"},
    ]}]}
    assert jmfw.derive_export_base(manifest) == "exports/notebooks"


@pytest.mark.parametrize("override,source", [
    ("/Users/test/child.ipynb", "cli"),
    ("", "workflow"),
])
def test_prebuilt_manifest_cli_notebook_path_keeps_audit_accurate(
        monkeypatch, tmp_path, override, source):
    import aidp_config
    import cluster_lifecycle

    base = "/Workspace/export/notebooks"
    child = base + "/Users/test/child.ipynb"
    job = {
        "job_name": "example", "tasks": [{
            "task_key": "parent", "cluster_id": "test-cluster",
            "notebook_path": base + "/Users/test/parent.ipynb",
            "base_parameters": {"notebook_path": child},
            "run_deps": [{
                "original_path": "/Users/test/child.ipynb",
                "path": child, "exists": True, "nested_deps": [],
            }],
        }],
        "parameters": {"notebook_path": child},
        "parameters_meta": {"notebook_path": {
            "source": "workflow", "widget": False, "value_present": True,
        }},
        "execution_layers": [["parent"]],
    }
    manifest = {"jobs": [job]}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    monkeypatch.setattr(aidp_config, "load_config", lambda **kwargs: SimpleNamespace(
        runtime=SimpleNamespace(readiness_report_path="", readiness_report_max_age_s=1800),
        aidp=SimpleNamespace(lake_ocid="lake", workspace_id="workspace", oci_profile="DEFAULT"),
        paths=SimpleNamespace(migrate_output="/Workspace/migrated"),
        bucket_mapping=SimpleNamespace(path="mapping.csv"),
    ))
    monkeypatch.setattr(cluster_lifecycle, "configure", lambda config: None)
    monkeypatch.setattr(sys, "argv", [
        "job_migrate_from_workflow.py", "--manifest", str(manifest_path),
        "--validate-only", "--param", f"notebook_path={override}",
    ])

    monkeypatch.setattr(jmfw.json, "load", lambda file: manifest)
    asyncio.run(jmfw.main())
    assert job["parameters"]["notebook_path"] == child
    assert job["parameters_meta"]["notebook_path"]["source"] == source


@pytest.mark.parametrize("run_deps", [
    None,
    [{"exists": True, "nested_deps": []}],
    [{"path": "/Workspace/Users/test/child.ipynb", "exists": True}],
    [{"path": "/Workspace/Users/test/child.ipynb", "exists": False,
      "nested_deps": []}],
])
def test_workflow_default_rejects_incomplete_manifest_before_migration(
        monkeypatch, tmp_path, run_deps):
    import aidp_config
    import cluster_lifecycle

    job = {"job_name": "example", "tasks": [{
        "task_key": "parent", "notebook_path": "/Workspace/Users/test/parent.ipynb",
        "cluster_id": "test-cluster",
    }], "execution_layers": [["parent"]]}
    if run_deps is not None:
        job["tasks"][0]["run_deps"] = run_deps
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"jobs": [job]}))
    monkeypatch.setattr(aidp_config, "load_config", lambda **kwargs: SimpleNamespace(
        runtime=SimpleNamespace(readiness_report_path="", readiness_report_max_age_s=1800),
        aidp=SimpleNamespace(lake_ocid="lake", workspace_id="workspace", oci_profile="DEFAULT"),
        paths=SimpleNamespace(migrate_output="/Workspace/migrated"),
        bucket_mapping=SimpleNamespace(path="mapping.csv"),
    ))
    monkeypatch.setattr(cluster_lifecycle, "configure", lambda config: None)
    monkeypatch.setattr(sys, "argv", [
        "job_migrate_from_workflow.py", "--manifest", str(manifest_path),
        "--validate-only",
    ])
    with pytest.raises(ValueError) as exc:
        asyncio.run(jmfw.main())
    message = str(exc.value)
    assert "--legacy-task-dependency-scan" in message
    assert "python3 scripts/build_dag_from_workflow.py" in message
    assert "--job-key <workflow-key>" in message


def test_workflow_legacy_flag_explicitly_restores_scan_mode(
        monkeypatch, tmp_path, capsys):
    import aidp_config
    import cluster_lifecycle

    job = {"job_name": "example", "tasks": [{
        "task_key": "parent", "notebook_path": "/Workspace/Users/test/parent.ipynb",
        "cluster_id": "test-cluster",
    }], "execution_layers": [["parent"]]}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"jobs": [job]}))
    monkeypatch.setattr(aidp_config, "load_config", lambda **kwargs: SimpleNamespace(
        runtime=SimpleNamespace(readiness_report_path="", readiness_report_max_age_s=1800),
        aidp=SimpleNamespace(lake_ocid="lake", workspace_id="workspace", oci_profile="DEFAULT"),
        paths=SimpleNamespace(migrate_output="/Workspace/migrated"),
        bucket_mapping=SimpleNamespace(path="mapping.csv"),
    ))
    monkeypatch.setattr(cluster_lifecycle, "configure", lambda config: None)
    monkeypatch.setattr(sys, "argv", [
        "job_migrate_from_workflow.py", "--manifest", str(manifest_path),
        "--validate-only", "--legacy-task-dependency-scan",
    ])
    asyncio.run(jmfw.main())
    assert "legacy task scans and job-end backfill" in capsys.readouterr().out


@pytest.mark.parametrize("legacy_scan,manifest_mode", [
    (False, True),
    (True, False),
])
def test_workflow_mode_dispatches_to_expected_migration_path(
        legacy_scan, manifest_mode):
    seen = []

    async def process_job(job, session, *, skip_task_dependency_scan):
        seen.append(skip_task_dependency_scan)
        return {"job_name": job["job_name"]}

    migrator = SimpleNamespace(process_job=process_job)
    job = {"job_name": "example"}
    result = asyncio.run(jmfw._run_workflow_job(
        job, object(), migrator, legacy_scan,
    ))
    assert seen == [manifest_mode]
    assert result == {"job_name": "example"}


# ─── build_dep_resolution: missing/null notebook_path guard ────────────────

_VALID_TASK = {
    "task_key": "t1",
    "notebook_path": "/Workspace/exports/notebooks/Users/a@co.com/nb1",
    "run_deps": [{"path": "/Workspace/exports/notebooks/Users/a@co.com/child",
                  "nested_deps": []}],
}


def test_missing_notebook_path_key_is_skipped_with_warning(capsys):
    job = {"tasks": [{"task_key": "t0"}, dict(_VALID_TASK)]}
    dep_map = jmfw.build_dep_resolution(job)  # must not raise KeyError
    assert None not in dep_map
    assert dep_map == {
        _VALID_TASK["notebook_path"]: [_VALID_TASK["run_deps"][0]["path"]],
        _VALID_TASK["run_deps"][0]["path"]: [],
    }
    out = capsys.readouterr().out
    assert "WARNING" in out and "'t0'" in out and "notebook_path" in out


def test_null_notebook_path_is_skipped_not_none_keyed(capsys):
    job = {"tasks": [
        {"task_key": "t0", "notebook_path": None, "run_deps": []},
        dict(_VALID_TASK),
    ]}
    dep_map = jmfw.build_dep_resolution(job)
    assert None not in dep_map
    assert "" not in dep_map
    assert _VALID_TASK["notebook_path"] in dep_map
    out = capsys.readouterr().out
    assert "WARNING" in out and "'t0'" in out


class _BootstrapSession:
    def __init__(self, results):
        self.results = iter(results)
        self.executed = []
        self.registered = []

    async def execute(self, code, timeout):
        self.executed.append((code, timeout))
        return next(self.results)

    def register_bootstrap(self, code):
        self.registered.append(code)


def test_bootstrap_snippets_execute_individually_and_register_after_success():
    session = _BootstrapSession([
        {"status": "ok", "outputs": []},
        {"status": "ok", "outputs": []},
    ])

    asyncio.run(
        jm.execute_bootstrap_snippets(session, ["first()", "  second()  "])
    )

    assert session.executed == [("first()", 60), ("second()", 60)]
    assert session.registered == ["first()", "second()"]


def test_all_migration_entry_points_share_checked_bootstrap_policy():
    import bootstrap_execution
    import cluster_session

    assert jm.execute_bootstrap_snippets is bootstrap_execution.execute_bootstrap_snippets
    assert cluster_session.execute_bootstrap_snippets is bootstrap_execution.execute_bootstrap_snippets


def test_bootstrap_snippets_fail_loud_and_do_not_run_following_statements():
    session = _BootstrapSession([
        {"status": "error", "outputs": [{"text": "first failed"}]},
        {"status": "ok", "outputs": []},
    ])

    with pytest.raises(RuntimeError, match="bootstrap statement 1/2 failed"):
        asyncio.run(
            jm.execute_bootstrap_snippets(
                session, ["first()", "second()"]
            )
        )

    assert session.executed == [("first()", 60)]
    assert session.registered == []


# ─── cluster readiness: complete Phase-1 gate, once per cluster ────────────

def test_prepare_migration_cluster_once_uses_complete_gate(monkeypatch):
    import phase1_cluster_readiness

    calls = []

    class Lifecycle:
        async def ensure_cluster_running(self, cluster_id):
            calls.append(("running", cluster_id))

    lifecycle = Lifecycle()

    async def fake_prepare(received_lifecycle, cluster_id):
        calls.append((received_lifecycle, cluster_id))

    monkeypatch.setattr(
        phase1_cluster_readiness, "prepare_cluster", fake_prepare
    )
    prepared = set()

    assert asyncio.run(
        jmfw.prepare_migration_cluster_once(lifecycle, "cluster-a", prepared)
    ) is True
    assert asyncio.run(
        jmfw.prepare_migration_cluster_once(lifecycle, "cluster-a", prepared)
    ) is False
    assert calls == [
        (lifecycle, "cluster-a"),
        ("running", "cluster-a"),
    ]
    assert prepared == {"cluster-a"}


def test_prepare_migration_cluster_once_cache_hit_rechecks_liveness(monkeypatch):
    class Lifecycle:
        def __init__(self):
            self.running = []

        async def ensure_cluster_running(self, cluster_id):
            self.running.append(cluster_id)

    lifecycle = Lifecycle()
    prepared = {"cluster-a"}

    assert asyncio.run(
        jmfw.prepare_migration_cluster_once(lifecycle, "cluster-a", prepared)
    ) is False
    assert lifecycle.running == ["cluster-a"]


def test_prepare_migration_cluster_once_does_not_cache_failure(monkeypatch):
    import phase1_cluster_readiness

    async def fail_prepare(_lifecycle, _cluster_id):
        raise RuntimeError("HTTP 409")

    monkeypatch.setattr(
        phase1_cluster_readiness, "prepare_cluster", fail_prepare
    )
    prepared = set()

    try:
        asyncio.run(
            jmfw.prepare_migration_cluster_once(object(), "cluster-a", prepared)
        )
    except RuntimeError as exc:
        assert "409" in str(exc)
    else:
        raise AssertionError("expected readiness failure")

    assert prepared == set()


def test_prepare_migration_cluster_once_reuses_live_phase1_report(monkeypatch):
    import phase1_cluster_readiness

    lifecycle = object()
    prepare_calls = []

    async def fake_prepare(*args):
        prepare_calls.append(args)

    monkeypatch.setattr(
        phase1_cluster_readiness, "prepare_cluster", fake_prepare
    )
    monkeypatch.setattr(
        phase1_cluster_readiness,
        "reusable_cluster_report_is_live",
        lambda *_args: (True, "verified"),
    )
    prepared = set()
    reused = {"cluster-a": {"cluster_id": "cluster-a", "status": "READY"}}

    assert asyncio.run(
        jmfw.prepare_migration_cluster_once(
            lifecycle,
            "cluster-a",
            prepared,
            reusable_reports=reused,
        )
    ) is False
    assert prepare_calls == []
    assert prepared == {"cluster-a"}


def test_explicit_readiness_report_missing_or_bad_json_fails_loud(tmp_path):
    with pytest.raises(FileNotFoundError):
        jmfw.load_phase1_readiness_report(
            str(tmp_path / "missing.json"), "expected", 1800
        )

    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{not-json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        jmfw.load_phase1_readiness_report(str(bad_json), "expected", 1800)


def test_semantically_stale_readiness_report_warns_and_falls_back(
    tmp_path, capsys
):
    report = tmp_path / "stale.json"
    report.write_text(json.dumps({"status": "FAILED"}), encoding="utf-8")

    assert jmfw.load_phase1_readiness_report(
        str(report), "expected", 1800
    ) == {}
    assert "complete live gate will run" in capsys.readouterr().out


def test_readiness_settings_use_yaml_unless_cli_overrides():
    runtime = SimpleNamespace(
        readiness_report_path="/Workspace/reports/from-yaml.json",
        readiness_report_max_age_s=900,
    )

    assert jmfw.resolve_readiness_settings(None, None, runtime) == (
        "/Workspace/reports/from-yaml.json",
        900,
    )
    assert jmfw.resolve_readiness_settings(
        "/Workspace/reports/from-cli.json", 120, runtime
    ) == ("/Workspace/reports/from-cli.json", 120)
    with pytest.raises(ValueError, match="between 1 and 86400"):
        jmfw.resolve_readiness_settings(None, 0, runtime)
