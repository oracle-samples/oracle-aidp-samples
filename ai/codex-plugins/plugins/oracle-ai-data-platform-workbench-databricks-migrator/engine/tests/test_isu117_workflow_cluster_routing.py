"""REF-117: a legacy runbook's --cluster must not prepare its control cluster.

The 2026-09-17 run aaaaaaaa-0000-4000-8000-000000000105 failed
preparing old FAILED wheels on the orchestration cluster before task execution.
"""
import asyncio
import builtins
import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from test_readiness_report_preflight import configure_runner, runner, lifecycle
import phase1_cluster_readiness as readiness


@pytest.fixture
def runner_environment(monkeypatch, tmp_path):
    import aidp_executor
    import agent_migrate
    import build_dag_from_workflow as dag
    import context_tools
    import job_migrate as jm
    # main configures these modules; restore all pre-existing attributes after
    # each test so an orchestration test cannot contaminate the remaining suite.
    for module in (runner, lifecycle, aidp_executor, agent_migrate, context_tools, jm):
        for name, value in list(vars(module).items()):
            if not name.startswith("__"):
                monkeypatch.setattr(module, name, value)
    monkeypatch.setattr(lifecycle, "configure", lambda _: None)
    monkeypatch.setattr(lifecycle, "resolve_aidp_compat_whl", lambda: "fixture.whl")
    monkeypatch.setattr(jm, "apply_cfg_to_modules", lambda *a, **k: None)
    monkeypatch.setattr(runner._glob, "glob", lambda *_: [])
    def no_network(*args, **kwargs):
        raise AssertionError("No live network is allowed in this regression")
    monkeypatch.setattr(runner, "get_oci_signer", no_network)
    monkeypatch.setattr(aidp_executor, "get_oci_signer", no_network)
    import requests
    monkeypatch.setattr(requests, "get", no_network)
    prepare = AsyncMock()
    monkeypatch.setattr(readiness, "prepare_cluster", prepare)
    argv = configure_runner(monkeypatch, tmp_path, "none", tmp_path / "unused.json")
    return SimpleNamespace(argv=argv, prepare=prepare, dag=dag, jm=jm)


@pytest.mark.parametrize("legacy_cluster", [None, "orchestrator", "task-a"])
@pytest.mark.parametrize("validate_only", [False, True])
@pytest.mark.parametrize("use_job_default", [False, True])
def test_job_key_validation_uses_workflow_assignment(monkeypatch, runner_environment, legacy_cluster, validate_only, use_job_default, capsys):
    env = runner_environment
    monkeypatch.setattr(env.dag, "fetch_job_definition", lambda *a, **k: {
        "name": "fixture-job", "jobClusters": [{"clusterKey": "task-a"}] if use_job_default else [],
        "tasks": [{"type": "NOTEBOOK_TASK", "taskKey": "first",
        "notebookPath": "/Workspace/example/first.ipynb",
        "cluster": {} if use_job_default else {"clusterKey": "task-a"}}]})
    argv = env.argv + ["--job-key", "fixture-job"]
    if legacy_cluster:
        argv += ["--cluster", legacy_cluster]
    if validate_only:
        argv += ["--validate-only"]
    monkeypatch.setattr(sys, "argv", argv)
    connected = []
    class ReachedValidation(Exception):
        pass
    def session(**kwargs):
        connected.append(kwargs["cluster_id"])
        return SimpleNamespace(connect=AsyncMock(side_effect=ReachedValidation))
    monkeypatch.setattr(runner, "AIDPSession", session)
    with pytest.raises(ReachedValidation):
        asyncio.run(runner.main())
    assert connected == ["task-a"]
    assert [call.args[1] for call in env.prepare.await_args_list] == ["task-a"]
    if legacy_cluster:
        assert "DEPRECATED --cluster" in capsys.readouterr().out


@pytest.mark.parametrize("legacy_cluster", [None, "orchestrator", "task-a"])
@pytest.mark.parametrize("validate_only", [False, True])
@pytest.mark.parametrize("advisory_status", [200, 403])
def test_manifest_does_not_bootstrap_control_cluster(monkeypatch, tmp_path, runner_environment, legacy_cluster, validate_only, advisory_status, capsys):
    env = runner_environment
    import requests
    import dbx_export.cluster_converter as converter
    advisory_clusters = []
    def advisory_get(url, **kwargs):
        advisory_clusters.append(url.rsplit("/", 1)[-1])
        return SimpleNamespace(status_code=advisory_status, json=lambda: {
            "clusterRuntimeConfig": {"sparkAdvancedConfigurations": {}}})
    monkeypatch.setattr(runner, "get_oci_signer", lambda *_: ({}, object()))
    monkeypatch.setattr(requests, "get", advisory_get)
    monkeypatch.setattr(converter, "recommended_spark_config", lambda _: {
        "spark.serializer": "create-only", "spark.example.setting": "expected"})
    monkeypatch.setattr(converter, "_autoscale_keys", lambda _: set())
    job = {"job_name": "fixture-job", "tasks": [
        {"task_key": "first", "notebook_path": "/Workspace/example/first.ipynb", "cluster_id": "task-a", "run_deps": []},
        {"task_key": "second", "notebook_path": "/Workspace/example/second.ipynb", "cluster_id": "task-b", "run_deps": []},
    ], "execution_layers": [["first"], ["second"]]}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"jobs": [job]}), encoding="utf-8")
    argv = env.argv + ["--manifest", str(manifest)]
    if legacy_cluster:
        argv += ["--cluster", legacy_cluster]
    if validate_only:
        argv += ["--validate-only"]
    monkeypatch.setattr(sys, "argv", argv)
    session = SimpleNamespace(cluster_id=None, connect=AsyncMock(), close=AsyncMock(),
                              execute=AsyncMock(return_value={"status": "ok", "outputs": []}))
    fake = ModuleType("cluster_session")
    fake.cluster = session
    monkeypatch.setitem(sys.modules, "cluster_session", fake)
    real_import = builtins.__import__
    def imports(name, *args, **kwargs):
        if name == "cluster_session":
            sys.modules[name] = fake  # main deliberately evicts its kernel copy.
            return fake
        return real_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", imports)
    async def process(received_job, received_session, *, skip_task_dependency_scan):
        assert received_job["tasks"] == job["tasks"]
        assert received_session is session and session.cluster_id is None
        assert skip_task_dependency_scan is True
        return {"job_name": "fixture-job", "total_ok": 2, "total_failed": 0}
    process_job = AsyncMock(side_effect=process)
    monkeypatch.setattr(env.jm, "process_job", process_job)
    asyncio.run(runner.main())
    env.prepare.assert_not_awaited()
    session.connect.assert_not_awaited()
    assert process_job.await_count == int(not validate_only)
    assert sorted(advisory_clusters) == ([] if validate_only else ["task-a", "task-b"])
    output = capsys.readouterr().out
    if not validate_only:
        if advisory_status == 200:
            assert "RECOMMENDED SPARK CONFIG PRE-FLIGHT" in output
            assert "spark.example.setting" in output
            assert "spark.serializer" not in output
        else:
            assert "HTTP 403" in output
    if legacy_cluster:
        assert "DEPRECATED --cluster" in output


def test_new_installed_wheel_does_not_mask_failed_legacy_attachment():
    fake = SimpleNamespace(_best_artifact_registration=lifecycle._best_artifact_registration, get_installed_libraries=lambda _: [
        {"path": "/Workspace/dependencies/aidp_compat-old.whl", "status": "FAILED", "stateMessage": "Missing or NotAuthorized"},
        {"path": "/Workspace/dependencies/libs/aidp_compat-new.whl", "status": "INSTALLED"},
    ])
    with pytest.raises(RuntimeError, match="aidp_compat-old.whl"):
        asyncio.run(readiness.wait_for_attached_libraries_to_settle(fake, "task-a"))


@pytest.mark.parametrize("mode", ["job", "manifest"])
@pytest.mark.parametrize("cluster_value", [None, "", "absent"])
@pytest.mark.parametrize("legacy_cluster", [None, "orchestrator"])
@pytest.mark.parametrize("validate_only", [False, True])
def test_legacy_cluster_cannot_fill_missing_assignment(monkeypatch, tmp_path, runner_environment, mode, cluster_value, legacy_cluster, validate_only, capsys):
    env = runner_environment
    argv = env.argv.copy()
    if legacy_cluster:
        argv += ["--cluster", legacy_cluster]
    if validate_only:
        argv += ["--validate-only"]
    assignment = {} if cluster_value == "absent" else {"clusterKey": cluster_value}
    if mode == "job":
        monkeypatch.setattr(env.dag, "fetch_job_definition", lambda *a, **k: {
            "name": "fixture-job", "tasks": [{"type": "NOTEBOOK_TASK", "taskKey": "unassigned",
            "notebookPath": "/Workspace/example/task.ipynb", "cluster": assignment}]})
        argv += ["--job-key", "fixture-job"]
    else:
        manifest = tmp_path / "unassigned.json"
        assignment = {} if cluster_value == "absent" else {"cluster_id": cluster_value}
        manifest.write_text(json.dumps({"jobs": [{"job_name": "fixture-job", "tasks": [
            {"task_key": "unassigned", "notebook_path": "/Workspace/example/task.ipynb", **assignment}
        ]}]}), encoding="utf-8")
        argv += ["--manifest", str(manifest)]
    monkeypatch.setattr(sys, "argv", argv)
    session = Mock(side_effect=AssertionError("Missing assignment must not create a session"))
    process_job = AsyncMock(side_effect=AssertionError("Missing assignment must not migrate"))
    monkeypatch.setattr(runner, "AIDPSession", session)
    monkeypatch.setattr(env.jm, "process_job", process_job)
    with pytest.raises(SystemExit) as exc:
        asyncio.run(runner.main())
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert "ERROR: Missing workflow/manifest cluster assignment for task(s): unassigned" in captured.out
    assert "Assign task clusters or a workflow default" in captured.out
    assert "No cluster was selected for these tasks" in captured.out
    assert "Traceback" not in captured.out + captured.err
    env.prepare.assert_not_awaited()
    session.assert_not_called()
    process_job.assert_not_awaited()


def test_explicit_workflow_default_remains_valid():
    runner.require_task_cluster_assignments({
        "default_cluster": "workflow-owned", "tasks": [{"task_key": "inherited"}],
    })


def test_readiness_report_key_excludes_legacy_control_cluster(monkeypatch, tmp_path, runner_environment):
    env = runner_environment
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"jobs": [{"job_name": "fixture-job", "tasks": [
        {"task_key": "first", "cluster_id": "task-a", "notebook_path": "/Workspace/example/first.ipynb", "run_deps": []},
        {"task_key": "second", "cluster_id": "task-b", "notebook_path": "/Workspace/example/second.ipynb", "run_deps": []},
    ]}]}), encoding="utf-8")
    monkeypatch.setattr(readiness, "read_readiness_report", lambda _: {})
    keys = []
    def load_report(path, key, age):
        keys.append(key)
        return {}
    monkeypatch.setattr(runner, "load_phase1_readiness_report", load_report)
    monkeypatch.setattr(sys, "argv", env.argv + ["--manifest", str(manifest),
        "--cluster", "orchestrator", "--readiness-report", "fixture-report.json", "--validate-only"])
    asyncio.run(runner.main())
    assert keys == [readiness.build_readiness_key(lifecycle, ["task-a", "task-b"])]
    env.prepare.assert_not_awaited()
