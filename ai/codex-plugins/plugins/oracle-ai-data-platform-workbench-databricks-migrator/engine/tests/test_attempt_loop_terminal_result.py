"""Every executed cell ends the attempt loop passed or with a cell result.

#185 review round 3 / owner decision 5. process_notebook's attempt loop leaves
an attempt by `continue` (a deterministic rewrite or a retry) or by `break`. On
the final attempt a `continue` ends the loop before the retries-exhausted
handling, and the fix-step error and DIRECT_EXECUTE breaks never wrote a
result. The cell then had no cell_results entry, job_should_stop stayed False,
and with a passing sibling the task read PASS with the cell unverified.

Each test drives the real cell loop (harness from
test_conditional_exit_below_execute) through one of those exits, with a
passing sibling cell first so a silent exit would surface as a task PASS.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import job_migrate as jm  # noqa: E402
from test_conditional_exit_below_execute import _Kernel, _patch_common  # noqa: E402
from test_eval_verdict_gate import _notebook  # noqa: E402

SIBLING = "SIBLING_OK = 1\n"   # base fake: status ok, no output -> passes, no eval


def _error(evalue, ename="RuntimeError"):
    return {"status": "error", "outputs": [{
        "type": "error", "ename": ename, "evalue": evalue,
        "traceback": [f"{ename}: {evalue}"]}]}


GENERIC = _error("synthetic failure")


class _SequenceKernel(_Kernel):
    """Code containing a marker gets that marker's scripted results in order
    (the last one repeats); anything else runs on the base fake."""

    def __init__(self, scripts):
        super().__init__()
        self.scripts = scripts
        self.runs = {marker: 0 for marker in scripts}

    async def execute(self, code, **kwargs):
        for marker, results in self.scripts.items():
            if marker in code:
                self.executed.append(code)
                n = self.runs[marker]
                self.runs[marker] += 1
                return results[min(n, len(results) - 1)]
        return await super().execute(code, **kwargs)


async def _unchanged_fix(code, error, context, attempt, extra_context="", **k):
    return code


def _run(monkeypatch, tmp_path, *sources, kernel, max_attempts=2, fix=_unchanged_fix,
         session_pool=None, direct_execute=False):
    _patch_common(monkeypatch, tmp_path, _notebook(*sources))
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", max_attempts)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", direct_execute)
    monkeypatch.setattr(jm, "call_fix", fix)
    return asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_terminal", "01_task", {},
        run_all=True, session_pool=session_pool))


def _assert_cell_failed_and_job_stopped(result, cell):
    assert result["status"] != "PASS", result
    assert result["job_should_stop"]
    errors = [cr for cr in result["cell_results"]
              if cr["cell"] == cell and cr["status"] == "error"]
    assert len(errors) == 1, result["cell_results"]
    assert "no terminal result" in errors[0]["note"]
    return errors[0]


def test_oci_glob_strip_on_the_final_attempt(monkeypatch, tmp_path):
    import context_tools
    monkeypatch.setattr(jm.dbfs_oci, "wrap_spark_reads", lambda code: code)  # glob strip only
    # Hermetic: oci:// literals go through the bucket->namespace mapping, whose
    # module-level index another test may leave empty (fail-loud by design).
    monkeypatch.setattr(context_tools, "_BUCKET_NS_INDEX", {"bkt": "ns"})
    kernel = _SequenceKernel({"SENTINEL": [
        GENERIC, _error("BucketNotFound: bkt", "Py4JJavaError")]})
    result = _run(monkeypatch, tmp_path, SIBLING,
                  "SENTINEL = spark.read.format('org.apache.hudi')"
                  ".load('oci://bkt@ns/base/*/*')\n", kernel=kernel)
    _assert_cell_failed_and_job_stopped(result, cell=1)
    assert kernel.runs["SENTINEL"] == 2, "the stripped read never ran"


def test_dbfs_read_wrap_on_the_final_attempt(monkeypatch, tmp_path):
    async def fix_adds_a_read(code, error, context, attempt, extra_context="", **k):
        return "SENTINEL = spark.read.parquet('dbfs:/FileStore/x.parquet')\n"

    kernel = _SequenceKernel({"SENTINEL": [GENERIC]})
    result = _run(monkeypatch, tmp_path, SIBLING, "SENTINEL = 1\n",
                  kernel=kernel, fix=fix_adds_a_read)
    _assert_cell_failed_and_job_stopped(result, cell=1)
    assert kernel.runs["SENTINEL"] == 2


def test_pyspark_auto_import_on_the_final_attempt(monkeypatch, tmp_path):
    async def resolve(symbol, session):
        return f"from pyspark.sql.functions import {symbol}"

    monkeypatch.setattr(jm, "_resolve_pyspark_import", resolve)
    kernel = _SequenceKernel({"SENTINEL": [
        GENERIC, _error("name 'col' is not defined", "NameError")]})
    result = _run(monkeypatch, tmp_path, SIBLING, "SENTINEL = col('x')\n", kernel=kernel)
    _assert_cell_failed_and_job_stopped(result, cell=1)
    assert kernel.runs["SENTINEL"] == 2


def test_data_recovery_on_the_final_attempt(monkeypatch, tmp_path):
    async def recovery(code, output, session, **k):
        return "OVERRIDE_DATES = 1\n"

    monkeypatch.setattr(jm, "attempt_data_recovery", recovery)
    kernel = _SequenceKernel({"SENTINEL": [
        GENERIC, _error("list index out of range", "IndexError")]})
    result = _run(monkeypatch, tmp_path, SIBLING,
                  "SENTINEL = spark.sql('select 1').collect()[0]\n", kernel=kernel)
    _assert_cell_failed_and_job_stopped(result, cell=1)
    assert kernel.runs["SENTINEL"] == 2


def test_monitor_reconnect_on_the_final_attempt(monkeypatch, tmp_path):
    """The reconnect fires only on attempt 0 after CONSECUTIVE_FAIL_THRESHOLD
    failures, so it is the final attempt at fix_max_attempts=1. Two OCI
    permission failures (recorded, no stop) build the streak."""
    async def systemic(errors, notebook_path):
        return "systemic", "synthetic systemic diagnosis"

    class _Pool:
        async def release(self, session):
            pass

        async def acquire(self):
            return kernel

    monkeypatch.setattr(jm, "monitor_analyze_failures", systemic)
    denied = {"status": "ok", "outputs": [
        {"type": "stream", "name": "stdout", "text": "404, NotAuthorizedOrNotFound\n"}]}
    kernel = _SequenceKernel({"SENTINEL_P1": [denied], "SENTINEL_P2": [denied],
                              "SENTINEL_R": [_error("unknown error on the cluster")]})
    result = _run(monkeypatch, tmp_path, SIBLING, "SENTINEL_P1 = 1\n", "SENTINEL_P2 = 1\n",
                  "SENTINEL_R = 1\n", kernel=kernel, max_attempts=1, session_pool=_Pool())
    _assert_cell_failed_and_job_stopped(result, cell=3)
    assert kernel.runs["SENTINEL_R"] == 1


def test_fix_step_error_ends_the_cell_with_a_result(monkeypatch, tmp_path):
    async def fix_raises(code, error, context, attempt, extra_context="", **k):
        raise RuntimeError("synthetic fix-step failure")

    kernel = _SequenceKernel({"SENTINEL": [GENERIC]})
    result = _run(monkeypatch, tmp_path, SIBLING, "SENTINEL = 1\n",
                  kernel=kernel, fix=fix_raises)
    error = _assert_cell_failed_and_job_stopped(result, cell=1)
    assert "synthetic fix-step failure" in error["note"]


def test_direct_execute_failure_is_recorded_and_the_run_moves_on(monkeypatch, tmp_path):
    """DIRECT_EXECUTE (WS test mode, no AI fixing) records the failure and
    keeps executing the remaining cells, as its comment always said."""
    kernel = _SequenceKernel({"SENTINEL": [GENERIC]})
    result = _run(monkeypatch, tmp_path, "SENTINEL = 1\n", SIBLING,
                  kernel=kernel, direct_execute=True)
    errors = [cr for cr in result["cell_results"] if cr["status"] == "error"]
    assert [cr["cell"] for cr in errors] == [0]
    assert "no terminal result" in errors[0]["note"]
    assert (result["failed"], result["ok"]) == (1, 1)
    assert result["status"] != "PASS" and not result["job_should_stop"]
    assert any("SIBLING_OK" in code for code in kernel.executed)
