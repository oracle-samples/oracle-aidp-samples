"""Cell-loop coverage for the conditional-exit convert-only contract.

Review of PR #98 (a reviewer, 2026-09-11) found that the convert-only guard sat
AFTER the %sql/%scala/%pip/%run execute-first branches, so cells of those types
BELOW a fired conditional exit were still EXECUTED (SQL run, pip installed, %run
child inlined) — violating the "never execute the un-taken branch" contract and
losing code when a %run child exited (the parent `break` dropped every cell
below it from the saved notebook).

Unlike the discriminator tests in test_conditional_exit_coverage.py, these drive
the REAL process_notebook cell loop with a fake kernel (the exit "fires" when the
executed code carries the AIDP exit marker) and assert, by execution, that NO
cell type below a fired conditional exit is executed and NONE is lost from the
saved notebook. This is the harness the review used; the earlier docstring claim
that "unit tests alone can't drive the WebSocket cell loop" was wrong.
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import agent_migrate as am  # noqa: E402
import cell_analyzer  # noqa: E402
import job_migrate as jm  # noqa: E402


# ── Unit: guard-line helper (report quotes the CONDITION, not the exit call) ──

def test_guard_line_quotes_enclosing_if():
    src = "current_hour = now().hour\nif current_hour % 2 == 0:\n    dbutils.notebook.exit('even')"
    assert jm._conditional_exit_guard_line(src) == "if current_hour % 2 == 0:"


def test_guard_line_quotes_data_count_guard():
    src = "if data_count == 0:\n    aidputils.notebook.exit('no data')"
    assert jm._conditional_exit_guard_line(src) == "if data_count == 0:"


def test_guard_line_uses_generic_fallback_when_no_guard():
    assert jm._conditional_exit_guard_line("dbutils.notebook.exit('x')") is None
    assert "inspect this cell" in jm._conditional_exit_guard_hint(
        "dbutils.notebook.exit('x')")


def test_guard_line_ignores_commented_exit():
    assert jm._conditional_exit_guard_line(
        "# dbutils.notebook.exit('x')\nprint(1)") is None


# ── Unit: %sql table-ref note must not demote the cell off the SQL path ──

def test_sql_table_ref_note_keeps_sql_magic_first():
    """_preprocess_cell_source step 7 must insert the note BELOW %sql, so the
    later is_sql (startswith '%sql') check still matches and AIDP does not parse
    a leading '--' as Python (SyntaxError)."""
    out = jm._preprocess_cell_source("%sql\nSELECT * FROM my_table\n")
    assert out.lstrip().startswith("%sql"), out
    if "Oracle tool modification: table refs" in out:
        lines = out.splitlines()
        assert lines[0].strip() == "%sql"
        assert lines[1].startswith("--"), lines


# ── Cell-loop harness ──

class _Kernel:
    cluster_id = "test-cluster"

    def __init__(self):
        self.executed = []
        self.files = {}

    async def execute(self, code, **kwargs):
        self.executed.append(code)
        # The real kernel executes guarded parameter-isolation snippets and
        # emits this marker only after setup/restore verification succeeds.
        # This cell-loop fake does not interpret Python, so model that verified
        # success explicitly rather than making unrelated conditional-exit
        # tests fail at the new fail-closed %run boundary.
        if "__AIDP_ISO_OK__" in code:
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout", "text": "__AIDP_ISO_OK__\n"}
            ]}
        # Capture base64 file saves (the notebook save-back path).
        match = re.search(r"with builtins.open\('([^']+)', 'w[ba]'\) as f:", code)
        encoded = re.search(r"base64.b64decode\('([^']+)'\)", code)
        # Job-level report files are written whole (temporary sibling, then
        # os.replace) and confirmed by a printed marker; record the final path.
        report = re.search(r"^_path = '([^']+)'$", code, re.M)
        if report and encoded and "__AIDP_REPORT_WRITTEN__" in code:
            self.files[report.group(1)] = base64.b64decode(encoded.group(1)).decode()
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout",
                                                 "text": "__AIDP_REPORT_WRITTEN__\n"}]}
        if match and encoded:
            self.files[match.group(1)] = base64.b64decode(encoded.group(1)).decode()
            return {"status": "ok", "outputs": []}
        # The conditional exit FIRES: report the AIDP exit marker in output.
        if "notebook.exit(" in code and not code.lstrip().startswith("#"):
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout", "text": "__AIDP_NOTEBOOK_EXIT__ done\n"}]}
        return {"status": "ok", "outputs": []}


def _patch_common(monkeypatch, tmp_path, notebook):
    async def analyze(cells, *a, **k):
        return [{"cell_index": i, "action": "execute", "description": "x",
                 "risks": [], "changes_needed": [], "storage_paths": [],
                 "table_refs": [], "fuse_risks": []} for i in range(len(cells))]

    async def noop(*a, **k):
        return None

    async def verified(*a, **k):
        return True

    # Hermetic: the convert-only path is deterministic and must never call the
    # LLM. Route the (Python) exit cell to the deterministic as-is path (no
    # Databricks markers → no Opus) and make any Opus call a hard failure, so
    # the test never constructs the Anthropic client (which requires an API key
    # absent on CI) and proves no cell here goes through LLM migration.
    def _no_llm(*a, **k):
        raise AssertionError("LLM must not be invoked in the convert-only path")
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: False)
    monkeypatch.setattr(jm, "call_opus_with_tools", _no_llm)

    monkeypatch.setattr(jm, "download_notebook", lambda *a, **k: json.dumps(notebook).encode())
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    monkeypatch.setattr(jm, "load_catalog_snapshot", lambda: "")
    monkeypatch.setattr(jm, "get_bucket_mapping_context", lambda: "")
    monkeypatch.setattr(jm, "_inject_write_guard", lambda code: code)
    monkeypatch.setattr(jm, "registry_update", noop)
    monkeypatch.setattr(jm, "verify_upload", verified)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", False)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_cond")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/test_cond/notebooks")
    monkeypatch.setattr(jm.tempfile, "mkdtemp", lambda **k: str(tmp_path))
    monkeypatch.setattr(jm.shutil, "rmtree", lambda *a, **k: None)
    monkeypatch.setattr(jm.asyncio, "sleep", noop)


def _saved_sources(kernel):
    finals = [json.loads(t) for p, t in kernel.files.items() if p.endswith(".ipynb")]
    assert finals, "no notebook artifact saved"
    # All saved copies are identical; use the first.
    return "\n".join("".join(c["source"]) for c in finals[0]["cells"]), finals[0]


def test_magic_and_python_cells_below_conditional_exit_are_not_executed(monkeypatch, tmp_path):
    """The core Major-1 regression: %sql / %pip / %run / Python cells BELOW a
    fired conditional exit must be CONVERTED (preserved, code-only) and NEVER
    executed, and none may be lost from the saved notebook."""
    notebook = {
        "cells": [{"cell_type": "code", "source": [s], "metadata": {}, "outputs": [],
                   "execution_count": None} for s in (
            "if MIGRATION_TIME_FLAG:\n    aidputils.notebook.exit('done')\n",  # 0: fires
            "%sql\nSELECT * FROM sentinel_sql_table\n",                        # 1: below
            "%pip install sentinel_pip_pkg\n",                                # 2: below
            "%run /Workspace/some/sentinel_run_child\n",                      # 3: below
            "SENTINEL_PY_BELOW = 1\n",                                        # 4: below
        )],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    _patch_common(monkeypatch, tmp_path, notebook)

    inline_calls = []
    async def spy_inline(child_path, *a, **k):
        inline_calls.append(child_path)
        return {"status": "ok", "did_exit": False, "cells_ok": 1, "cells_fixed": 0}
    monkeypatch.setattr(jm, "_inline_child_notebook", spy_inline)

    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_cond", "01_task", {}))

    # The convert-only terminal only fires when cells below the exit were
    # converted — proving all four below-exit cells took the convert-only path.
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result

    # The below-exit cells were NOT executed on the kernel (raw source strings
    # only appear base64-encoded inside the save calls, never as direct exec).
    joined_exec = "\n".join(e for e in kernel.executed
                            if "base64.b64decode" not in e)
    for sentinel in ("sentinel_sql_table", "sentinel_pip_pkg", "SENTINEL_PY_BELOW = 1"):
        assert sentinel not in joined_exec, f"{sentinel} was executed below the exit"
    # No %run child was inlined/executed below the exit.
    assert inline_calls == [], inline_calls

    # No code loss: every below-exit cell is preserved in the saved notebook.
    saved_src, saved_nb = _saved_sources(kernel)
    assert len(saved_nb["cells"]) == 5, "cell(s) lost from saved notebook"
    for sentinel in ("sentinel_sql_table", "sentinel_pip_pkg", "sentinel_run_child",
                     "SENTINEL_PY_BELOW = 1"):
        assert sentinel in saved_src, f"{sentinel} missing from saved notebook (code loss)"


def test_run_child_exit_does_not_truncate_parent(monkeypatch, tmp_path):
    """Major 2: a %run child that fires notebook.exit() must NOT drop the
    parent's remaining cells (old `break` = code loss). They are converted
    code-only and preserved."""
    notebook = {
        "cells": [{"cell_type": "code", "source": [s], "metadata": {}, "outputs": [],
                   "execution_count": None} for s in (
            "%run /Workspace/some/exiting_child\n",   # 0: child exits
            "SENTINEL_AFTER_RUN = 1\n",               # 1: must be preserved, not executed
            "%sql\nSELECT * FROM sentinel_after_sql\n",  # 2: preserved, not executed
        )],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    _patch_common(monkeypatch, tmp_path, notebook)

    async def child_exits(child_path, *a, **k):
        return {"status": "ok", "did_exit": True, "exit_value": "stop",
                "cells_ok": 1, "cells_fixed": 0}
    monkeypatch.setattr(jm, "_inline_child_notebook", child_exits)

    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/parent.ipynb", kernel, "test_cond", "01_task", {}))

    assert result["status"] in ("PASS", "PASS_CONDITIONAL_EXIT_COVERAGE"), result
    joined_exec = "\n".join(e for e in kernel.executed if "base64.b64decode" not in e)
    assert "SENTINEL_AFTER_RUN = 1" not in joined_exec
    assert "sentinel_after_sql" not in joined_exec

    saved_src, saved_nb = _saved_sources(kernel)
    assert len(saved_nb["cells"]) == 3, "parent cells lost after %run-child exit"
    assert "SENTINEL_AFTER_RUN = 1" in saved_src
    assert "sentinel_after_sql" in saved_src
