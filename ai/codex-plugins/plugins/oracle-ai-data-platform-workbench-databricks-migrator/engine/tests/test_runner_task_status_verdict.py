"""The workflow runner's exit follows each task's terminal status, not counters.

Incident 2026-10-12 (spend_category_tickets_automation, Phase 2 run
cccc1da3): the engine recorded ``Helper_Category_Matching_Ticket_Push: FAIL``
(token ledger, final 11:26:03Z), yet the Phase 2 notebook task ended SUCCESS
56 s later and the controller then failed on a missing final clone. The runner decided its
exit from cell counters only (``fail or errored or data_unavail``), and several
terminal statuses carry no such count — e.g. FAIL after a second kernel loss,
PARTIAL_STATE_LOST, ACCEPTANCE_CONTRACT_VIOLATED, RAY_CONVERSION_UNSUPPORTED,
or PARTIAL from failed dependency cells. A zero exit turns the AIDP notebook
task green, so any non-success status must raise.
"""
import inspect
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import job_migrate_from_workflow as jmfw  # noqa: E402


@pytest.mark.parametrize("status", [
    "PASS", "PASS_CONDITIONAL_EXIT_COVERAGE", "PASS_DATA_SUBSTITUTED",
    "ALREADY_MIGRATED", "SKIPPED", "SKIP_MISSING_DEPS",
])
def test_successful_task_statuses_keep_the_run_green(status):
    assert jmfw.unsuccessful_tasks({"t": {"status": status}}, code_only=False) == {}


@pytest.mark.parametrize("task_result", [
    # Kernel lost on the original run and on the one whole-task retry.
    {"status": "FAIL", "error": "ConnectionDied wiped kernel state on both runs"},
    {"status": "PARTIAL", "ok": 9, "failed": 0, "child_failed": 1},
    {"status": "PARTIAL_STATE_LOST", "ok": 9, "failed": 0},
    {"status": "ACCEPTANCE_CONTRACT_VIOLATED", "ok": 9, "failed": 0},
    {"status": "RAY_CONVERSION_UNSUPPORTED", "error": "unsupported"},
    {"status": "PARTIAL_DATA_UNAVAILABLE", "ok": 3, "failed": 0, "data_unavailable": 0},
    {"status": "CONVERTED_NOT_VALIDATED"},
    # A task notebook that does not parse was never migrated.
    {"status": "NON_NOTEBOOK_SKIPPED"},
])
def test_failure_statuses_without_counters_fail_the_run(task_result):
    verdict = jmfw.unsuccessful_tasks({"HelperLib": task_result}, code_only=False)
    assert verdict == {"HelperLib": task_result["status"]}


def test_converted_not_validated_is_success_only_in_code_only_mode():
    results = {"t": {"status": "CONVERTED_NOT_VALIDATED"}}
    assert jmfw.unsuccessful_tasks(results, code_only=True) == {}
    assert jmfw.unsuccessful_tasks(results, code_only=False) == {
        "t": "CONVERTED_NOT_VALIDATED"}


@pytest.mark.parametrize("task_result,label", [
    ({"status": "A_STATUS_ADDED_LATER"}, "A_STATUS_ADDED_LATER"),
    ({"error": "no status recorded"}, "UNKNOWN"),
    ("not a result dict", "UNKNOWN"),
])
def test_unrecognised_task_results_fail_closed(task_result, label):
    assert jmfw.unsuccessful_tasks({"t": task_result}, code_only=False) == {"t": label}


def test_mixed_job_reports_only_the_unsuccessful_tasks():
    results = {"a": {"status": "PASS"}, "b": {"status": "PARTIAL"},
               "c": {"status": "BLOCKED", "blocked_by": "b"}}
    assert jmfw.unsuccessful_tasks(results, code_only=False) == {
        "b": "PARTIAL", "c": "BLOCKED"}


def test_runner_main_raises_on_unsuccessful_task_statuses():
    # main() needs a live cluster session to reach its summary; pin that the
    # summary consults the per-task verdict and that the verdict feeds the
    # existing non-zero exit (RuntimeError, see the SystemExit note there).
    source = inspect.getsource(jmfw.main)
    assert 'unsuccessful_tasks(result.get("task_results"), args.code_only)' in source
    summary = source[source.index("unsuccessful_tasks(result.get"):]
    assert "migration_failed = True" in summary.split("await session.close()")[0]


# ── A halted run is never green ──────────────────────────────────────────────
# With --strict-cell-gating and --legacy-task-dependency-scan, a task whose
# external %run dependency is missing halts the run but keeps the graceful
# SKIP_MISSING_DEPS status. When it is the last task to run, nothing after the
# halt is recorded as BLOCKED, so the statuses alone read as success.

class _Session:
    cluster_id = "test-cluster"

    async def execute(self, code, **kwargs):
        return {"status": "ok", "outputs": [{"type": "stream", "text": "MISSING\n"}]}

    async def run_stateless(self, code, **kwargs):
        return {"status": "ok", "outputs": []}


@pytest.mark.parametrize("strict,halted", [
    (True, "strict gate: missing dependencies for only_task"),
    (False, None),
])
def test_process_job_reports_why_it_halted(monkeypatch, tmp_path, strict, halted):
    import asyncio
    import job_migrate as jm

    async def noop(*args, **kwargs):
        return None

    async def no_backfill(*args, **kwargs):
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    for name, value in {
        "MIGRATE_OUTPUT": str(tmp_path / "migrated"), "EXPORT_BASE": "",
        "JOB_ROOT": "", "_migration_cache": {}, "SKIP_MIGRATED": False,
        "CODE_ONLY": False, "STRICT_CELL_GATING": strict, "START_TASK": "",
        "ONLY_TASKS": [], "_get_throttle_coord": lambda: None,
        "_bump_cluster_idle_timeout": lambda cluster_id: None,
        "_revert_all_idle_timeouts": lambda: [],
        "stage_export_resources": noop, "_ensure_redirect_schema": noop,
        "backfill_missing_deps": no_backfill,
        "process_notebook": lambda *a, **kw: pytest.fail("a skipped task never migrates"),
    }.items():
        monkeypatch.setattr(jm, name, value)
    job = {"job_name": "halt_verdict", "default_cluster": _Session.cluster_id,
           "execution_layers": [["only_task"]],
           "tasks": [{"task_key": "only_task", "depends_on": [],
                      "notebook_path": "/Workspace/Users/test/task.ipynb",
                      "run_deps": [{"path": "/Workspace/Users/test/gone.ipynb",
                                    "location": "external", "exists": False,
                                    "nested_deps": []}]}]}

    result = asyncio.run(jm.process_job(job, _Session(), skip_task_dependency_scan=False))

    assert result["task_results"]["only_task"]["status"] == "SKIP_MISSING_DEPS"
    assert result["job_halted"] == halted
    # The status alone keeps the run green; only the halt reason can fail it.
    assert jmfw.unsuccessful_tasks(result["task_results"], code_only=False) == {}


def test_runner_main_fails_a_halted_run():
    source = inspect.getsource(jmfw.main)
    summary = source[source.index("MIGRATION COMPLETE"):].split("await session.close()")[0]
    halt_check = summary[summary.index('result.get("job_halted")'):]
    assert "migration_failed = True" in halt_check
    assert "run halted:" in halt_check
