"""PR90 review 5631346397: report configuration must precede cluster work."""

import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import agent_migrate  # noqa: F401 -- imported with clean env, so the effort-knob
#                         test below exercises validate_effort_env(), not this import
import cluster_lifecycle as lifecycle
import job_migrate_from_workflow as runner
import phase1_cluster_readiness as readiness

LAKE = "ocid1.aidataplatform.oc1.us-ashburn-1.example"
WORKSPACE = "00000000-0000-0000-0000-000000000000"


def configure_runner(monkeypatch, tmp_path, source, report_path):
    for key in list(os.environ):
        if key.startswith("AIDP_"):
            monkeypatch.delenv(key)
    mapping = tmp_path / "mapping.csv"
    mapping.write_text("s3_bucket,oci_bucket,oci_namespace\n")
    config = {
        "aidp": {"region": "us-ashburn-1", "lake_ocid": LAKE,
                 "workspace_id": WORKSPACE, "oci_profile": "TEST", "default_namespace": "example"},
        "paths": {"migrate_output": "/Workspace/example/output", "export_output": "/Workspace/example/export"},
        "bucket_mapping": {"path": str(mapping)},
        "dbx_source": {"host": "https://example.invalid"},
        "secrets": {}, "spark_config": {}, "autoscale_config": {},
        "runtime": {"readiness_report_path": str(report_path) if source == "yaml" else "",
                    "cell_execute_timeout_s": 300, "fix_max_attempts": 5, "ws_recycle_interval_s": 300},
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    argv = ["runner", "--config", str(path), "--aidp-base", "https://example.invalid/20240831"]
    if source == "cli":
        argv += ["--readiness-report", str(report_path)]
    elif source == "env":
        monkeypatch.setenv("AIDP_READINESS_REPORT", str(report_path))
    return argv


@pytest.mark.parametrize("source", ["cli", "env", "yaml"])
@pytest.mark.parametrize("mode", ["job", "manifest"])
@pytest.mark.parametrize("validate_only", [False, True])
@pytest.mark.parametrize("contents", [None, "{invalid", "[]", '{"clusters":[null]}', '{"clusters":[{"libraries_after":null}]}'])
def test_bad_report_fails_before_any_cluster_or_workflow_io(
    monkeypatch, tmp_path, source, mode, validate_only, contents
):
    report = tmp_path / "report.json"
    if contents is not None:
        report.write_text(contents)
    argv = configure_runner(monkeypatch, tmp_path, source, report)
    argv += ["--job-key", "test-job"] if mode == "job" else ["--manifest", str(tmp_path / "never-opened.json")]
    if validate_only:
        argv.append("--validate-only")
    monkeypatch.setattr(sys, "argv", argv)

    def no_io(*args, **kwargs):
        pytest.fail("invalid report reached control-plane work")

    monkeypatch.setattr(lifecycle, "configure", no_io)
    import build_dag_from_workflow as dag
    monkeypatch.setattr(dag, "fetch_job_definition", no_io)
    with pytest.raises((ValueError, FileNotFoundError), match="report.json"):
        asyncio.run(runner.main())


@pytest.mark.parametrize("value", ["", " ", "\t"])
def test_empty_cli_override_cannot_disable_configured_report(value):
    with pytest.raises(ValueError, match="empty override"):
        runner.resolve_readiness_settings(value, None, SimpleNamespace(readiness_report_path="configured.json"))


@pytest.mark.parametrize("source", ["cli", "env", "yaml"])
@pytest.mark.parametrize("validate_only", [False, True])
@pytest.mark.parametrize("drift", [False, True])
def test_job_key_first_gate_uses_report_and_checks_live_state(
    monkeypatch, tmp_path, source, validate_only, drift
):
    report = tmp_path / "report.json"
    argv = configure_runner(monkeypatch, tmp_path, source, report) + ["--job-key", "test-job"]
    if validate_only:
        argv.append("--validate-only")
    monkeypatch.setattr(sys, "argv", argv)
    # Restore global configuration after exercising main's real setup.
    for name in ("AIDP_BASE", "DATALAKE_OCID", "WORKSPACE_ID", "OCI_PROFILE", "_SIGNER"):
        monkeypatch.setattr(lifecycle, name, getattr(lifecycle, name))
    monkeypatch.setattr(lifecycle, "configure", lambda _: None)
    contract = SimpleNamespace(
        AIDP_BASE="https://example.invalid/20240831", DATALAKE_OCID=LAKE, WORKSPACE_ID=WORKSPACE,
        TARGET_RUNTIME_REQUIREMENTS_PATH=lifecycle.TARGET_RUNTIME_REQUIREMENTS_PATH,
        TARGET_WHEEL_PATH=lifecycle.TARGET_WHEEL_PATH, BASELINE_JARS=lifecycle.BASELINE_JARS,
    )
    report.write_text(json.dumps({
        "status": "READY", "generated_at": datetime.now(timezone.utc).isoformat(),
        "readiness_key": readiness.build_readiness_key(contract, ["test-cluster"]),
        "clusters": [{"cluster_id": "test-cluster", "status": "READY", "libraries_after": []}],
    }))
    import build_dag_from_workflow as dag
    monkeypatch.setattr(dag, "fetch_job_definition", lambda *a, **kw: {
        "name": "test-job", "jobClusters": [{"clusterKey": "test-cluster"}],
    })
    monkeypatch.setattr(dag, "extract_tasks_from_job", lambda _: [{
        "task_key": "task", "depends_on": [], "cluster_id": "test-cluster",
        "notebook_path": "/Workspace/example/task.ipynb",
    }])
    monkeypatch.setattr(lifecycle, "resolve_aidp_compat_whl", lambda: "example.whl")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _: "STOPPED" if drift else "ACTIVE")
    monkeypatch.setattr(lifecycle, "get_installed_libraries", lambda _: [])
    prepare = AsyncMock()
    monkeypatch.setattr(readiness, "prepare_cluster", prepare)

    class ReachedConnect(Exception):
        pass

    session = SimpleNamespace(connect=AsyncMock(side_effect=ReachedConnect))
    monkeypatch.setattr(runner, "AIDPSession", lambda **kw: session)
    with pytest.raises(ReachedConnect):
        asyncio.run(runner.main())
    assert prepare.await_count == int(drift)
    session.connect.assert_awaited_once()


@pytest.mark.parametrize("knob", ["AIDP_EFFORT_MIGRATE", "AIDP_EFFORT_EVAL",
                                  "AIDP_EFFORT_FIX_FIRST_ATTEMPT"])
@pytest.mark.parametrize("validate_only", [False, True])
def test_bad_effort_knob_fails_before_any_cluster_or_workflow_io(
    monkeypatch, tmp_path, knob, validate_only
):
    """#185 review A6: agent_migrate reads the effort knobs at import, which the
    runner reached only after cluster preparation (and never under
    --validate-only). A typo must fail at the same point as a bad report."""
    argv = configure_runner(monkeypatch, tmp_path, "none", tmp_path / "unused.json")
    monkeypatch.setenv(knob, "med")
    argv += ["--job-key", "test-job"]
    if validate_only:
        argv.append("--validate-only")
    monkeypatch.setattr(sys, "argv", argv)

    def no_io(*args, **kwargs):
        pytest.fail("invalid effort knob reached control-plane work")

    monkeypatch.setattr(lifecycle, "configure", no_io)
    import build_dag_from_workflow as dag
    monkeypatch.setattr(dag, "fetch_job_definition", no_io)
    with pytest.raises(ValueError, match=knob):
        asyncio.run(runner.main())
