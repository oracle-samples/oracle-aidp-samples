"""#129 review P2 — executed cell-loop coverage.

The Python path must classify a genuinely-missing TABLE as DATA_UNAVAILABLE via
terminal *output* attribution, not only when Opus emits the DATA_UNAVAIL note
(the %scala/%sql/child paths already do output-based). Drives the REAL
process_notebook fix loop with a fake kernel; only the kernel, model repair, and
the Opus evaluator are mocked — no live cluster, no API key.

Pins note × no-note × default × strict: all four must land
PARTIAL_DATA_UNAVAILABLE with failed=0 (before the fix, the no-note cases fell
through to FAIL).
"""
import asyncio
import json

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, jm

_MISSING = "nonexistent_db_check.dummy_missing_table_xyz"


class _MissingTableKernel(_Kernel):
    async def execute(self, code, **kwargs):
        # The customer read of the non-existent table fails with the real Spark
        # signature; everything else (save-back, bootstrap) uses the base kernel.
        if "spark.table(" in code and _MISSING in code:
            self.executed.append(code)
            msg = f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{_MISSING}` cannot be found"
            return {"status": "error", "outputs": [{
                "type": "error", "ename": "AnalysisException", "evalue": msg,
                "traceback": [f"org.apache.spark.sql.AnalysisException: {msg}"]}]}
        return await super().execute(code, **kwargs)


def _notebook(src):
    return {"cells": [{"cell_type": "code", "source": [src], "metadata": {},
                       "outputs": [], "execution_count": None}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}


def _run(monkeypatch, tmp_path, *, emit_note, strict):
    src = f'df = spark.table("{_MISSING}")\nprint(df.count())'
    _patch_common(monkeypatch, tmp_path, _notebook(src), [])
    kernel = _MissingTableKernel()

    async def fix(code, *a, **k):
        # Model repair leaves the read unchanged (it is a data gap, not a code
        # bug), so the cell keeps failing to exhaustion. Optionally emit the note.
        if emit_note:
            jm._current_cell_notes.append(f"DATA_UNAVAIL: {_MISSING} not found in AIDP catalog")
        return code

    async def evaluator(*a, **k):
        # Stand in for the Opus verify-evaluator: report an issue, no make_note.
        return ("ISSUE: table not found", {"input": 0, "output": 0})

    async def summarize(*a, **k):
        return "missing table cell"

    async def monitor(*a, **k):
        return "local", "table not found"

    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "call_opus_with_tools", evaluator)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "monitor_analyze_failures", monitor)
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    return asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))


@pytest.mark.parametrize("strict", [False, True], ids=["default", "strict"])
@pytest.mark.parametrize("emit_note", [True, False], ids=["note", "no_note"])
def test_python_missing_table_classified_data_unavailable(monkeypatch, tmp_path, emit_note, strict):
    result = _run(monkeypatch, tmp_path, emit_note=emit_note, strict=strict)
    ctx = f"(emit_note={emit_note}, strict={strict}) -> {result.get('status')}"
    assert result["status"] == "PARTIAL_DATA_UNAVAILABLE", ctx
    assert int(result.get("failed", 0) or 0) == 0, f"must not be a cell FAIL {ctx}"
    assert int(result.get("data_unavailable", 0) or 0) >= 1, f"must count data-unavailable {ctx}"
