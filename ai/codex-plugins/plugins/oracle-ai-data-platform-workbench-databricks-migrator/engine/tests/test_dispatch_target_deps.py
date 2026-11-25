"""Dynamic-dispatch targets enter the DAG manifest as ordinary run_deps.

Faced: Example_Base_Job, 2026-10-13 — generate_base dispatches a
runtime-computed notebook path per for-each element; the 31 exported model
notebooks never entered the manifest, so the migrated dispatch had no target
(NotebookExecutionException at the migrated models path). The augmentation
pairs the job-wide directory-enumeration hints (same detector dbx_export uses
to export those directories) with the per-task dynamic-dispatch warning and
resolves the exported .ipynb files through the normal recursive dep machinery.
"""
import asyncio
import os
import sys

import pytest

_SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import build_dag_from_workflow as bdw  # noqa: E402


def _nb(src):
    return {"cells": [{"cell_type": "code", "source": src,
                       "metadata": {}, "outputs": []}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}


INIT_SRC = (
    'TARGET_FOLDER = "models"\n'
    'url = f"https://api.github.com/repos/o/r/contents/{TARGET_FOLDER}?ref=b"\n'
    "directories = requests.get(url).json()\n"
    "for item in directories:\n"
    '    model = {"yaml_path": f"{item[\'path\']}/params.yaml"}\n'
    '    with open("../" + model["yaml_path"], "r") as fh:\n'
    "        fh.read()\n"
)
DISPATCH_SRC = (
    'target_notebook = aidputils.widgets.getParameter("target_notebook", "")\n'
    'aidputils.notebook.run(f"../{target_notebook}", 3600, {})\n'
)

BASE = "/Workspace/scratch/export"
INIT_NB = f"{BASE}/notebooks/GIT/notebooks/init.ipynb"
DRIVER_NB = f"{BASE}/notebooks/GIT/notebooks/driver.ipynb"
GB_NB = f"{BASE}/notebooks/GIT/notebooks/generate_base.ipynb"
MODEL_NB = f"{BASE}/notebooks/GIT/models/MODEL_A/base.ipynb"

NOTEBOOKS = {
    INIT_NB: _nb(INIT_SRC),
    DRIVER_NB: _nb("x = 1\n"),
    GB_NB: _nb(DISPATCH_SRC),
}


def test_dispatch_targets_are_added_as_run_deps(monkeypatch):
    async def fake_read(_session, path):
        return NOTEBOOKS.get(path)

    async def fake_find(_session, anchor_dir, hints):
        assert anchor_dir == os.path.dirname(GB_NB)
        assert "models" in hints
        return [MODEL_NB]

    resolved = []

    async def fake_resolve(target, _depth, _params):
        resolved.append(target)
        return {"original_path": target, "path": target, "location": "external",
                "exists": True, "nested_deps": []}

    monkeypatch.setattr(bdw, "_read_notebook_json", fake_read)
    monkeypatch.setattr(bdw, "_find_dispatch_targets", fake_find)

    tasks = [
        {"task_key": "init", "notebook_path": INIT_NB, "parameters": []},
        {"task_key": "run_base", "notebook_path": DRIVER_NB, "parameters": []},
    ]
    run_deps = {
        "init": [],
        "run_base": [{"original_path": GB_NB, "path": GB_NB,
                      "location": "external", "exists": True, "nested_deps": []}],
    }
    asyncio.run(bdw._augment_dynamic_dispatch_deps(
        object(), tasks, run_deps, BASE, None,
        fake_resolve, lambda _t: {},
    ))
    assert resolved == [MODEL_NB]
    added = [e for e in run_deps["run_base"] if e["path"] == MODEL_NB]
    assert len(added) == 1
    assert "dispatch-target" in added[0]["note"]
    assert run_deps["init"] == []  # no dynamic dispatch there — untouched


def test_jobs_without_both_signals_are_untouched(monkeypatch):
    async def fake_read(_session, path):
        # dynamic dispatch exists, but NO enumeration hints anywhere
        return _nb(DISPATCH_SRC) if path == GB_NB else _nb("y = 2\n")

    called = []

    async def fake_find(*_a):
        called.append(True)
        return []

    monkeypatch.setattr(bdw, "_read_notebook_json", fake_read)
    monkeypatch.setattr(bdw, "_find_dispatch_targets", fake_find)
    tasks = [{"task_key": "t", "notebook_path": GB_NB, "parameters": []}]
    run_deps = {"t": []}
    asyncio.run(bdw._augment_dynamic_dispatch_deps(
        object(), tasks, run_deps, BASE, None,
        None, lambda _t: {},
    ))
    assert run_deps == {"t": []}
    assert not called


def test_truncated_dispatch_scan_fails_loudly(monkeypatch):
    """P2 regression (2026-10-13 review): a partial target list must never be
    presented as complete — omitted notebooks would silently miss migration."""
    async def fake_run(_session, _code, timeout=60):
        import json as _json
        return {"outputs": [{"type": "stream",
                             "text": _json.dumps({"found": ["a.ipynb"], "truncated": True})}]}

    monkeypatch.setattr(bdw, "run", fake_run)
    with pytest.raises(RuntimeError, match="truncated"):
        asyncio.run(bdw._find_dispatch_targets(object(), "/Workspace/x", {"models"}))


def test_unverifiable_walk_output_fails_loudly(monkeypatch):
    """Review finding 2026-10-14 (2/3): once both signals exist, a failed walk
    probe must not masquerade as a legitimately empty scan."""
    async def fake_run(_session, _code, timeout=60):
        return {"outputs": [{"type": "stream", "text": "Command ID x failed with ..."}]}

    monkeypatch.setattr(bdw, "run", fake_run)
    with pytest.raises(RuntimeError, match="unverifiable|failed probe"):
        asyncio.run(bdw._find_dispatch_targets(object(), "/Workspace/x", {"models"}))


def test_unreadable_notebook_warns_and_returns_none(monkeypatch, capsys):
    """Review finding 2026-10-14 (2/3): read failures are surfaced per
    notebook (missing, corrupt, unparseable) — never a silent None."""
    import json as _json

    async def fake_run_error(_session, _code, timeout=60):
        return {"outputs": [{"type": "stream",
                             "text": _json.dumps({"error": "ValueError: bad json", "nb": None})}]}

    monkeypatch.setattr(bdw, "run", fake_run_error)
    out = asyncio.run(bdw._read_notebook_json(object(), "/Workspace/x/nb.ipynb"))
    assert out is None
    assert "cannot read /Workspace/x/nb.ipynb" in capsys.readouterr().out

    async def fake_run_garbage(_session, _code, timeout=60):
        return {"outputs": [{"type": "stream", "text": "not-json at all"}]}

    monkeypatch.setattr(bdw, "run", fake_run_garbage)
    out = asyncio.run(bdw._read_notebook_json(object(), "/Workspace/x/nb2.ipynb"))
    assert out is None
    assert "unverified scan of /Workspace/x/nb2.ipynb" in capsys.readouterr().out
