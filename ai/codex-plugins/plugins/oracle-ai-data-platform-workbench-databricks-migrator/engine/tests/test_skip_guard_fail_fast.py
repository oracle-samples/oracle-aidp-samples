"""REF-201 — missing upstream data FAILS the cell; the model must not skip-guard it.

Incident (Example_Monitoring_Generic_Job, task Publish_Metrics, JOB_REPORT
2026-09-28): an upstream cross-job trigger returned ``{}`` at migration time;
the fix loop wrapped 21 dependent cells in absent-upstream guards
(``if 'csi_output_file_path' in output: ... else: print("Skipping ...")``),
every guarded cell executed cleanly by taking the skip branch, and the task was
graded PASS with ``DataUnavail = 0``. The green clone run (c0e04a43,
2026-09-29) published nothing; once the trigger was fixed the first guarded
write failed (cf5c9acb, 2026-10-01).

These tests drive the REAL ``process_job`` → ``process_notebook`` cell loop
(fake kernel, scripted model repair; no cluster, no API key). The fail-fast
contract is enforced by FIX_PROMPT only ("NO SKIP GUARDS FOR MISSING UPSTREAM
DATA"): a model that follows it keeps the original code and records the gap,
and the cell is reported DATA_UNAVAILABLE, never PASS. There is no deterministic
backstop: a model that adds a skip guard anyway is NOT rejected — the
``SKIP_GUARD_RESIDUAL`` tests pin that false-PASS residual as strict xfails.
"""
import asyncio
import json
from pathlib import Path

import pytest

from test_pip_magic_gate import _ReportingKernel, _patch_common, cluster_lifecycle, jm

import agent_migrate

TRIGGER = "output = {}  # upstream CSI trigger returned nothing at migration time"
BUILD = "scores = spark.createDataFrame(pd.read_csv(output['csi_output_file_path']))"
WRITE = "createPermanentTable(scores, 'm_rev_scores_v2', 'ds_model_monitoring', 'append', 'delta')"

GUARDED = {
    BUILD: (
        "if 'csi_output_file_path' in output:  # Oracle tool modification: guard for missing upstream CSI output\n"
        f"    {BUILD}\n"
        "else:\n"
        "    scores = None\n"
        "    print(\"Skipping scores build: upstream CSI job returned no 'csi_output_file_path'.\")"
    ),
    WRITE: (
        "if scores is not None:  # Oracle tool modification: upstream CSI job produced no output at migration time\n"
        f"    {WRITE}\n"
        "else:\n"
        "    print('Skipping createPermanentTable for m_rev_scores_v2: scores is None.')"
    ),
}
NOTES = {
    BUILD: "DATA_UNAVAIL: upstream CSI job returned no 'csi_output_file_path' at migration time",
    WRITE: "DATA_UNAVAIL: scores was never built — upstream CSI output missing",
}
CHILD_PATH = "/Workspace/test_pip/test_pip/notebooks/dep.ipynb"
SKIP_GUARD_RESIDUAL = pytest.mark.xfail(
    strict=True,
    reason="prompt-only (PR #220): a skip guard the model adds despite FIX_PROMPT is not "
           "rejected — false-PASS residual, pending owner decision")


class _UpstreamKernel(_ReportingKernel):
    """Dependent cells fail like the incident (or succeed when data is present)."""

    def __init__(self, data_present=False):
        super().__init__()
        self.data_present = data_present

    async def execute(self, code, **kwargs):
        for raw, done in ((BUILD, "built 1200 scores"), (WRITE, "appended 1200 rows")):
            if raw in code:
                self.executed.append(code)
                if code.strip() in (GUARDED[raw], GUARDED[raw].strip()) or "else:" in code:
                    # A guarded cell really runs cleanly: it takes the skip branch.
                    return {"status": "ok", "outputs": [
                        {"type": "stream", "name": "stdout", "text": "Skipping: no upstream data\n"}]}
                if self.data_present:
                    return {"status": "ok", "outputs": [
                        {"type": "stream", "name": "stdout", "text": done + "\n"}]}
                return {"status": "error", "outputs": [{
                    "type": "error", "ename": "KeyError", "evalue": "'csi_output_file_path'",
                    "traceback": ["KeyError: 'csi_output_file_path'"]}]}
        return await super().execute(code, **kwargs)


def _notebook(sources):
    return {"cells": [{"cell_type": "code", "source": [s], "metadata": {},
                       "outputs": [], "execution_count": None} for s in sources],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}


def _run(monkeypatch, tmp_path, sources, *, note=True, data_present=False, child_sources=None,
         comply=False):
    """Run a one-task job. The scripted repair answers with the incident's skip
    guard (``comply=False``) or, following FIX_PROMPT, with the original code
    unchanged (``comply=True``); ``note`` makes it also record the DATA_UNAVAIL gap."""
    _patch_common(monkeypatch, tmp_path, _notebook(sources), [])
    fix_calls = []

    async def noop(*a, **kw):
        return None

    async def no_backfill(*a, **kw):
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    async def mapped_child(*a, **kw):
        return CHILD_PATH

    async def fix(code, *a, **k):
        fix_calls.append({"code": code, "extra_context": k.get("extra_context", "")})
        for raw, guarded in GUARDED.items():
            if raw in code:
                if note:
                    jm._current_cell_notes.append(NOTES[raw])
                return code if comply else guarded
        return code

    async def evaluator(*a, **k):
        return ("PASS: output looks correct", {"input": 0, "output": 0})

    async def summarize(*a, **k):
        return "cell"

    async def monitor(*a, **k):
        return "local", "upstream output absent"

    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "call_opus_with_tools", evaluator)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "monitor_analyze_failures", monitor)
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 3)
    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", noop)
    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: None)
    monkeypatch.setattr(jm, "stage_export_resources", noop)
    monkeypatch.setattr(jm, "_ensure_redirect_schema", noop)
    monkeypatch.setattr(jm, "backfill_missing_deps", no_backfill)
    monkeypatch.setattr(jm, "ensure_migrated", mapped_child)
    monkeypatch.setattr(jm, "_resolve_nbrun_child_path", lambda *a, **kw: CHILD_PATH)
    monkeypatch.setattr(jm, "CODE_ONLY", False)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    monkeypatch.setattr(jm, "START_TASK", None)
    monkeypatch.setattr(jm, "ONLY_TASKS", [])
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_inlined_nb_run_children", set())
    if child_sources is not None:
        monkeypatch.setitem(jm._notebook_content_cache, CHILD_PATH,
                            json.dumps(_notebook(child_sources)))
        monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    kernel = _UpstreamKernel(data_present=data_present)
    job = {"job_name": "test_pip", "parameters": {}, "root": "",
           "execution_layers": [["task"]],
           "tasks": [{"task_key": "task", "notebook_path": "/Workspace/test_pip/source.ipynb",
                      "depends_on": [], "run_deps": []}]}
    job_result = asyncio.run(jm.process_job(job, kernel))
    return job_result, kernel, fix_calls


def _saved_sources(kernel):
    return ["".join(c["source"]) for p, t in kernel.files.items() if p.endswith(".ipynb")
            for c in json.loads(t)["cells"]]


def _guarded_code_ran(kernel):
    return any("if scores is not None" in c or "'csi_output_file_path' in output" in c
               for c in kernel.executed)


def test_model_following_the_prompt_reports_the_gap_data_unavailable(monkeypatch, tmp_path):
    job_result, kernel, fix_calls = _run(monkeypatch, tmp_path, [TRIGGER, BUILD, WRITE],
                                         comply=True)
    task = job_result["task_results"]["task"]
    assert task["status"] == "PARTIAL_DATA_UNAVAILABLE", task
    assert task["ok"] == 1 and task["data_unavailable"] == 1 and task["failed"] == 0, task
    assert job_result["total_data_unavailable"] == 1, "runner exits non-zero on it"
    assert not any(WRITE in c for c in kernel.executed), "upstream data gap blocks the write"
    assert fix_calls, "the repair loop ran"
    saved = _saved_sources(kernel)
    assert any(BUILD in s for s in saved) and any(WRITE in s for s in saved)
    assert not any("is not None" in s or "Skipping" in s for s in saved), saved


@SKIP_GUARD_RESIDUAL
def test_skip_guard_is_rejected_and_the_gap_is_reported_data_unavailable(monkeypatch, tmp_path):
    job_result, kernel, fix_calls = _run(monkeypatch, tmp_path, [TRIGGER, BUILD, WRITE])
    task = job_result["task_results"]["task"]
    assert task["status"] == "PARTIAL_DATA_UNAVAILABLE", task
    assert task["ok"] == 1, "only the trigger cell actually did its work"
    assert task["data_unavailable"] == 2 and task["failed"] == 0
    assert job_result["total_data_unavailable"] == 2, "runner exits non-zero on it"
    assert fix_calls, "the repair loop ran"
    assert not _guarded_code_ran(kernel), "a rejected skip guard is never executed"
    saved = _saved_sources(kernel)
    assert saved, "notebook saved"
    assert not any("is not None" in s or "Skipping" in s for s in saved), saved
    assert any(BUILD in s for s in saved) and any(WRITE in s for s in saved)


@SKIP_GUARD_RESIDUAL
def test_skip_guard_without_a_data_unavail_note_still_never_passes(monkeypatch, tmp_path):
    job_result, kernel, _ = _run(monkeypatch, tmp_path, [TRIGGER, BUILD, WRITE], note=False)
    task = job_result["task_results"]["task"]
    # Without the note the cell is an ordinary failure: the existing fail-fast
    # stops execution there and migrates the remaining cell code-only.
    assert task["status"] == "PARTIAL", task
    assert task["failed"] == 1 and task["ok"] == 1, task
    assert not any(WRITE in c for c in kernel.executed), "nothing executes after the failure"
    assert not _guarded_code_ran(kernel)
    assert not any("is not None" in s for s in _saved_sources(kernel))


@SKIP_GUARD_RESIDUAL
def test_rejection_reason_is_fed_back_to_the_next_repair_attempt(monkeypatch, tmp_path):
    _, _, fix_calls = _run(monkeypatch, tmp_path, [TRIGGER, BUILD], note=False)
    build_calls = [c for c in fix_calls if BUILD in c["code"]]
    assert len(build_calls) >= 2, "the model gets another attempt after the rejection"
    assert "Skip guards are forbidden" not in build_calls[0]["extra_context"]
    later = build_calls[1]["extra_context"]
    assert "skip guard `if 'csi_output_file_path' in output`" in later
    assert "make_note(\"DATA_UNAVAIL:" in later


def test_cell_with_its_data_present_runs_unchanged_and_passes(monkeypatch, tmp_path):
    job_result, kernel, fix_calls = _run(monkeypatch, tmp_path, [TRIGGER, BUILD, WRITE],
                                         data_present=True)
    task = job_result["task_results"]["task"]
    assert task["status"] == "PASS", task
    assert task["ok"] == 3 and fix_calls == []


@SKIP_GUARD_RESIDUAL
def test_skip_guard_in_inlined_child_is_rejected(monkeypatch, tmp_path):
    job_result, kernel, fix_calls = _run(monkeypatch, tmp_path, ["%run ./dep", "z = 1"],
                                         child_sources=[TRIGGER, BUILD, WRITE])
    assert not _guarded_code_ran(kernel), "a rejected skip guard is never executed"
    build_calls = [c for c in fix_calls if BUILD in c["code"]]
    assert len(build_calls) >= 2
    assert "skip guard `if 'csi_output_file_path' in output`" in build_calls[1]["extra_context"]
    assert not any("is not None" in s or "Skipping" in s for s in _saved_sources(kernel))


@SKIP_GUARD_RESIDUAL
def test_fixup_replay_refuses_an_added_skip_guard():
    with pytest.raises(jm.FixupPersistenceError, match="skip guard"):
        jm._check_replay_source(WRITE, GUARDED[WRITE])


# ── FIX_PROMPT: fail fast, no guard prescription ──────────────────────────────

def test_fix_prompt_no_longer_prescribes_a_skip_guard():
    prompt = agent_migrate.FIX_PROMPT
    assert "EMPTY-DATAFRAME GUARD" not in prompt
    assert "The canonical fix (from a manually-validated AIDP migration) is a GUARD CLAUSE" not in prompt
    assert "Prefer the guard over algorithmic mutation" not in prompt
    rule = prompt.split("NO SKIP GUARDS FOR MISSING UPSTREAM DATA (applies", 1)[1][:1500]
    assert "Do NOT wrap the cell" in rule and "falsely reported as migrated" in rule


def test_fix_prompt_requires_fail_fast_with_data_unavail():
    prompt = agent_migrate.FIX_PROMPT
    block = prompt.split("NO SKIP GUARDS FOR MISSING UPSTREAM DATA (applies", 1)[1][:1500]
    assert "make_note(\"DATA_UNAVAIL:" in block
    assert "Do NOT modify the algorithm" in block, "the algorithm-mutation ban is kept"


def test_make_note_tool_does_not_suggest_a_null_check_guard():
    make_note = next(t for t in agent_migrate.OCI_PATH_TOOLS if t["name"] == "make_note")
    assert "null-check guard" not in make_note["description"]


def test_fix_prompt_keeps_the_job_trigger_placeholder_guard():
    block = agent_migrate.FIX_PROMPT.split("NO SKIP GUARDS FOR MISSING UPSTREAM DATA (applies", 1)[1]
    block = block.split("SPARK TABLE COLUMNS ARE LOWERCASE", 1)[0]
    assert 'TARGET_JOB_KEY == "REPLACE_WITH_AIDP_WORKFLOW_KEY"' in block
    assert "keep it as is" in block
