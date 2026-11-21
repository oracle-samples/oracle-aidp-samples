"""REF-209: a timed-out import probe must never certify packages available.

2026-10-16 live: exact mlflow+xgboost probe took 102.76s. Runs ccccbbd2
and cccca825 timed out at 60s and then queued parameter setup behind it.
Category 2: execution acknowledgments and task/job orchestration.
"""
import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cluster_lifecycle as lifecycle
import job_migrate as jm

MARKER = "__AIDP_MIG_MISSING_BEGIN____AIDP_MIG_MISSING_END__"


class ProbeSession:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def execute(self, code, **kwargs):
        self.calls.append((code, kwargs))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def response(status="ok", text=MARKER):
    return {"status": status, "outputs": [{"type": "stream", "text": text}]}


@pytest.mark.parametrize("text", [MARKER, "[]" + MARKER, MARKER + "\n" + MARKER,
    json.dumps([{"type": "TEXT_PLAIN", "value": MARKER}])])
@pytest.mark.parametrize("budget", [None, 47])
def test_completed_probe_uses_default_or_callers_budget(text, budget, capsys):
    session = ProbeSession(response(text=text))
    result = asyncio.run(lifecycle.ensure_requirements_installed(
        "test-cluster", session, ["mlflow", "xgboost"], "/Workspace/test/output",
        **({} if budget is None else {"timeout": budget})))
    assert result["installed"] == []
    assert session.calls[0][1]["timeout"] == (600 if budget is None else budget)
    assert "All 2 packages available" in capsys.readouterr().out


FAILURES = [response("error", "Execution timed out after 600s"),
            response("error", MARKER), response("aborted", MARKER),
            response(text=""), response(text="mlflow"),
            response(text="__AIDP_MIG_MISSING_BEGIN__"),
            {}, TimeoutError("transport timeout")]


@pytest.mark.parametrize("result", FAILURES)
def test_incomplete_probe_never_reports_available_or_installs(result, monkeypatch, capsys):
    session = ProbeSession(result)

    async def no_discovery(*args, **kwargs):
        pytest.fail("an incomplete probe must not reach discovery or installation")

    monkeypatch.setattr(lifecycle, "_discover_local_modules", no_discovery)
    with pytest.raises(lifecycle.ImportProbeError, match="probe"):
        asyncio.run(lifecycle.ensure_requirements_installed(
            "test-cluster", session, ["mlflow", "xgboost"],
            "/Workspace/test/output", notebook_paths=["/Workspace/test/source.ipynb"]))
    assert len(session.calls) == 1
    assert "All 2 packages available" not in capsys.readouterr().out


@pytest.mark.parametrize("result", FAILURES)
def test_probe_failure_stops_before_parameter_setup_and_customer_code(
        result, monkeypatch, tmp_path):
    import cell_analyzer
    from test_conditional_exit_below_execute import _patch_common
    notebook = {"cells": [{"cell_type": "code", "source": ["import mlflow"],
                          "metadata": {}, "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", False)

    async def analyze(*args, **kwargs):
        return [{"cell_index": 0, "action": "execute", "description": "",
                 "dependencies": ["mlflow", "xgboost"], "risks": [],
                 "changes_needed": [], "storage_paths": [], "table_refs": [], "fuse_risks": []}]

    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    session = ProbeSession(result)
    session.cluster_id = "test-cluster"
    with pytest.raises(lifecycle.ImportProbeError):
        asyncio.run(jm.process_notebook("/Workspace/test/source.ipynb", session,
                    "test_job", "first", {"marker": "current"}))
    assert len(session.calls) == 1
    assert "__AIDP_MIG_MISSING_BEGIN__" in session.calls[0][0]


@pytest.mark.parametrize("strict", [False, True])
def test_probe_failure_halts_all_remaining_job_tasks(monkeypatch, tmp_path, strict):
    from test_manifest_task_scan_flag import _setup, _job, Session
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    job = _job([])
    job["tasks"].extend([
        {"task_key": "same_layer", "notebook_path": "/Workspace/test/second.ipynb", "run_deps": [], "depends_on": []},
        {"task_key": "next_layer", "notebook_path": "/Workspace/test/third.ipynb", "run_deps": [], "depends_on": ["parent"]}])
    job["execution_layers"] = [["parent", "same_layer"], ["next_layer"]]
    called = []

    async def fail(path, *args, **kwargs):
        called.append(path)
        raise lifecycle.ImportProbeError("Dependency import probe failed")

    monkeypatch.setattr(jm, "process_notebook", fail)
    monkeypatch.setattr(jm, "download_notebook", lambda *args: b"notebook")
    result = asyncio.run(jm.process_job(job, Session(), skip_task_dependency_scan=True))
    assert len(called) == 1
    tasks = result["task_results"]
    assert tasks["parent"]["status"] == "ERROR"
    assert tasks["same_layer"]["status"] == "BLOCKED"
    assert tasks["next_layer"]["status"] == "BLOCKED"


@pytest.mark.parametrize("result, detail", [
    ({"status": "error", "outputs": [{"type": "error", "ename": "TimeoutError",
       "evalue": "Execution timed out after 47s"}]}, "TimeoutError: Execution timed out after 47s"),
    ({"status": "error", "outputs": [{"type": "error", "ename": "SessionDead",
       "evalue": "Session not connected."}]}, "SessionDead: Session not connected."),
    (response("aborted", ""), "status 'aborted'"),
    (response("error", "executor stream failure"), "executor stream failure"),
    (response(text=""), "did not return its completion marker"),
    (TimeoutError("transport timeout"), "TimeoutError: transport timeout"),
])
def test_probe_diagnostics_preserve_error_packages_and_budget(result, detail):
    session = ProbeSession(result)
    with pytest.raises(lifecycle.ImportProbeError) as caught:
        asyncio.run(lifecycle.ensure_requirements_installed(
            "test-cluster", session, ["xgboost", "mlflow"], "/Workspace/test/output", timeout=47))
    message = str(caught.value)
    assert detail in message
    assert "availability of mlflow, xgboost is UNRESOLVED" in message
    assert "execution stopped (probe budget 47s)" in message
    assert session.calls[0][1]["timeout"] == 47
    if isinstance(result, Exception):
        assert caught.value.__cause__ is result
