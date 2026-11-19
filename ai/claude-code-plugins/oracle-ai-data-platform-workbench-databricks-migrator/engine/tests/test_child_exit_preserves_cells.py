"""Regression guard: child-inline notebook.exit must preserve remaining cells (PR #52 / B3).

A %run child that exits early (native aidputils/oidlUtils exit → stdout sentinel)
previously broke out of the child loop BEFORE the exiting cell was recorded in
migrated_child_cells. That left the list shorter than the on-disk code-cell count,
which trips the save-back drift tripwire → status=error → the parent marks the
CHILD FAILED and fails the WHOLE task — but only when the child ALSO had a fixed
cell (save-back runs only then). This PR's own sentinel makes native exits common,
so the dormant path became reachable.

Fix: the exit branch records the exiting cell + every post-exit cell (count now
matches on-disk → no tripwire). Successful child returns also carry an explicit
did_exit flag, so normal completion is distinct from notebook.exit(None), while a
value-returning child that needed no fix still returns its value.

The executed regression below also checks that exit bypasses ordinary cell
finalization: recording the exit cell a second time makes the saved notebook
LONGER than the original and prevents save-back just as surely as dropping it.

Run: python -m pytest tests/test_child_exit_preserves_cells.py -v
"""
import os
import re
import asyncio
import contextlib
import copy
import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm
import cluster_lifecycle

SRC = open(os.path.join(os.path.dirname(__file__), "..", "scripts", "job_migrate.py")).read()


def _child_exit_branch():
    m = re.search(r"if _child_nb_exit:.*?child_nb_exit = True\s*\n\s*break", SRC, re.DOTALL)
    assert m, "child-inline exit branch (if _child_nb_exit: ... break) not found"
    return m.group(0)


def test_exit_branch_records_exiting_and_remaining_cells():
    block = _child_exit_branch()
    # the exiting cell itself must be recorded
    assert "migrated_child_cells.append(child_source)" in block, \
        "exiting child cell not appended to migrated_child_cells"
    # and every cell after it (so the count matches on-disk and the tripwire is happy)
    assert "_preserve_exit_tail(" in block and "ci + 1" in block
    tail = SRC[SRC.index("    def _preserve_exit_tail("):SRC.index("    # ── Fix 7h", SRC.index("    def _preserve_exit_tail("))]
    assert "child_cells[start:]" in tail, "post-exit child cells not preserved"
    assert "migrated_child_cells.append(_rest_src)" in tail


def test_exit_value_computed_before_zero_fix_early_return():
    idx_val = SRC.find('_child_exit_value = ""')
    idx_compute = SRC.find("_child_exit_value = _m_ev.group")
    idx_ret = SRC.find("No fixes needed, keeping existing file")
    assert idx_val != -1, "_child_exit_value not computed"
    assert idx_compute != -1, "exit-value parsing not found"
    assert idx_ret != -1, "zero-fix early return not found"
    assert idx_val < idx_compute < idx_ret, \
        "exit_value must be computed BEFORE the zero-fix early return"

    helper_start = SRC.find("def _child_result", idx_val, idx_ret)
    helper_end = SRC.find("for ci, child_source", helper_start, idx_ret)
    assert helper_start != -1 and helper_end != -1, \
        "central child-result builder not found"
    helper = SRC[helper_start:helper_end]
    assert '"did_exit": child_nb_exit' in helper, \
        "child-result builder does not report the explicit exit state"
    assert "if child_nb_exit:" in helper
    assert 'result["exit_value"]' in helper, \
        "a genuine child exit does not carry its exit value"

    block = SRC[idx_ret: idx_ret + 160]
    assert 'return _child_result("ok")' in block, \
        "zero-fix early return bypasses the child-result contract"


def test_parent_stops_only_for_explicit_child_exit_flag():
    marker = "# %run shares parent namespace"
    idx = SRC.find(marker)
    assert idx != -1, "parent %run result handling not found"
    block = SRC[idx: idx + 700]
    assert 'if child_result.get("did_exit") is True:' in block
    assert 'if "exit_value" in child_result:' not in block, \
        "key presence would falsely stop the parent after a normal child completion"


@pytest.mark.parametrize("prefix,tail,expected_installs,unresolved", [
    ([], ["!pip install tailpkg\nTAIL_SENTINEL = 42", "AFTER_TAIL = 1"], [], False),
    (["BEFORE_EXIT = 1"], ["!pip install tailpkg\nTAIL_SENTINEL = 42"], [], False),
    (["!pip install beforepkg", "BEFORE_EXIT = 1"], [], ["beforepkg"], False),
    ([], ["!pip install -r requirements.txt", "AFTER_TAIL = 1"], [], True),
    ([], ["!pip install tailpkg > /tmp/pip.log 2>&1\nTAIL_SENTINEL = 42"], [], False),
])
@pytest.mark.parametrize("exit_source", [
    "aidputils.notebook.exit('done')",
    "if STOP_EARLY:\n    aidputils.notebook.exit('done')",
])
def test_child_exit_saves_each_cell_once(
        monkeypatch, tmp_path, prefix, tail, expected_installs, unresolved, exit_source):
    """Drive the real child loop and its count/save scripts against a temp file.

    The earlier all-ok kernel never answered the on-disk count probe, allowing
    duplicate cells to pass unnoticed. Here the emitted probe and save script
    execute on the actual fixture, and the resulting notebook is read back.
    Customer code execution and library installs remain mocked.
    """
    path = tmp_path / "child.ipynb"
    sources = prefix + [exit_source] + tail
    notebook = {
        "cells": [{"cell_type": "code", "source": [src], "metadata": {},
                   "outputs": [], "execution_count": None} for src in sources],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    path.write_text(json.dumps(notebook))
    installs = []
    messages = []

    async def install(cluster_id, session, pkg, *args, **kwargs):
        installs.append(pkg)
        return True

    async def no_local_modules(*args, **kwargs):
        return set(), set(), []

    async def summarize(*args, **kwargs):
        return "fixture cell"

    class Log:
        def log(self, message):
            messages.append(message)

    class Kernel:
        cluster_id = "fixture-cluster"

        def __init__(self):
            self.executed = []
            self.count_checks = 0
            self.saves = 0

        async def execute(self, code, **kwargs):
            if "__AIDP_CHILD_COUNT__" in code or "all_cells = payload.get" in code:
                self.count_checks += int("__AIDP_CHILD_COUNT__" in code)
                self.saves += int("all_cells = payload.get" in code)
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    exec(code, {})
                text = output.getvalue()
            else:
                self.executed.append(code)
                text = ("__AIDP_NOTEBOOK_EXIT__ value=done\n"
                        if "aidputils.notebook.exit('done')" in code else "")
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout", "text": text}]}

    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "output"))
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "_notebook_content_cache", {str(path): json.dumps(notebook)})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", install)
    monkeypatch.setattr(cluster_lifecycle, "_discover_local_modules", no_local_modules)
    kernel = Kernel()
    result = asyncio.run(jm._inline_child_notebook(
        str(path), kernel, Log(), "fixture", "", "", "", {}))

    assert result["status"] == "ok", result
    assert result["did_exit"] is True
    assert result["exit_value"] == "done"
    assert result["cells_ok"] == len(prefix) + 1
    assert bool(result.get("unresolved_pip")) is unresolved
    assert installs == expected_installs
    assert kernel.count_checks == kernel.saves == 1
    saved = json.loads(path.read_text())
    saved_sources = ["".join(c["source"]) for c in saved["cells"]]
    assert len(saved_sources) == len(sources)
    assert saved_sources.count(exit_source) == 1
    expected = copy.deepcopy(sources)
    for i, src in enumerate(expected):
        if src.startswith("!pip install"):
            original_lines = src.split("\n")
            assert saved_sources[i].split("\n")[0].startswith("# AIDP:")
            assert saved_sources[i].split("\n")[0].endswith(original_lines[0])
            assert saved_sources[i].split("\n")[1:] == original_lines[1:]
        else:
            assert saved_sources[i] == src
    assert not any(jm._PIP_INSTALL_LINE_RE.match(line)
                   for src in saved_sources for line in src.split("\n"))
    assert not any("TAIL_SENTINEL" in code or "AFTER_TAIL" in code
                   for code in kernel.executed)
    assert not any("FAILED — keeping code UNCHANGED" in msg for msg in messages)
    if any("2>&1" in src for src in sources):
        assert any("not reproduced" in src for src in saved_sources)
        assert any("not reproduced" in msg for msg in messages)
    cached = json.loads(jm._notebook_content_cache[str(path)])
    assert ["".join(c["source"]) for c in cached["cells"]] == saved_sources
