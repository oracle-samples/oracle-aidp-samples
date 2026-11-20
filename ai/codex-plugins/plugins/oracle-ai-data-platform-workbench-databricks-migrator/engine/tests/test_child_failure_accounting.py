"""Failed cells inside inlined dependency notebooks must reach the task verdict.

Incident 2026-10-13 (spend_category_tickets_automation, Phase 2 run
c6f163e0): four dependencies inlined below one ``notebook.run`` cell left 45
cells failed after every retry. The default (non-strict) inline loop keeps executing past a failed
child cell so passing cells' definitions still land in the kernel, but it
returned ``ok`` without the failures, and the task reported
``RESULT: PASS | Fail:0``. The final workflow clone was then built over
unvalidated dependency code. On 2026-10-12 the same failures surfaced only
because a pending dependency repair turned them into a save-back refusal.

Execution still continues past a failed child cell; only the verdict changes.
Driven through the real cell loops; only customer execution and the model
repair are mocked.
"""
import pytest

import test_child_exit_conversion as tce
from test_child_exit_conversion import _run
from test_pip_magic_gate import jm

BROKEN = "BROKEN_DEP_CELL = 1  # CHILD_BROKEN_SENTINEL"
CHILD = ["GOOD_DEP_CELL = 1", BROKEN, "AFTER_BROKEN_CELL = 2"]


class _FailingKernel(tce._ExitKernel):
    """Every execution of the sentinel cell fails, as an unfixable dependency
    cell does on the cluster; generated count/save scripts still run."""

    async def execute(self, code, **kwargs):
        if ("CHILD_BROKEN_SENTINEL" in code
                and "__AIDP_CHILD_COUNT__" not in code
                and "base64.b64decode" not in code):
            self.executed.append(code)
            return {"status": "error", "outputs": [{
                "type": "error", "ename": "ValueError",
                "evalue": "fixture dependency failure",
                "traceback": ["ValueError: fixture dependency failure"]}]}
        return await super().execute(code, **kwargs)


@pytest.fixture
def failing_dependency(monkeypatch):
    monkeypatch.setattr(tce, "_ExitKernel", _FailingKernel)

    async def unchanged_repair(code, *args, **kwargs):
        return code

    monkeypatch.setattr(jm, "call_fix", unchanged_repair)


def _executed(kernel):
    return "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)


def test_failed_dependency_cell_is_returned_to_the_caller(
        monkeypatch, tmp_path, failing_dependency):
    result, saved, kernel, _, _ = _run(monkeypatch, tmp_path, {"child": CHILD})

    # The inline still continues: later cells execute, the cell is unchanged.
    assert result["status"] == "ok", result
    assert "AFTER_BROKEN_CELL = 2" in _executed(kernel)
    assert saved["child"][1] == BROKEN
    assert result.get("failed_cells") == [
        "child.ipynb: 1 cell(s) failed after all retries"], result


@pytest.mark.parametrize("invocation", ["%run ./leaf", 'dbutils.notebook.run("./leaf", 60)'])
def test_nested_dependency_failure_bubbles_up_the_inline_chain(
        monkeypatch, tmp_path, failing_dependency, invocation):
    result, _, _, _, _ = _run(
        monkeypatch, tmp_path,
        {"child": [invocation, "CHILD_TAIL = 1"],
         "leaf": CHILD})

    if invocation.startswith("dbutils"):
        # Native dispatch must not execute a child whose preparation failed.
        assert result["status"] == "error", result
        assert "leaf.ipynb" in result["error"]
        return
    assert result["status"] == "ok", result
    assert result.get("failed_cells") == [
        "leaf.ipynb: 1 cell(s) failed after all retries"], result


def test_passing_dependency_reports_no_failed_cells(monkeypatch, tmp_path):
    result, _, _, _, _ = _run(
        monkeypatch, tmp_path, {"child": ["GOOD_DEP_CELL = 1"]})

    assert result["status"] == "ok", result
    assert "failed_cells" not in result


# Every place a parent inlines a child: %run, %scala notebook.run, Python
# notebook.run, and the cohort fan-out of a variable notebook.run target.
INLINE_PATHS = [
    ("%run /Workspace/child", False),
    ('dbutils.notebook.run("/Workspace/child", 60)', False),
    ('%scala\ndbutils.notebook.run("/Workspace/child", 60)', False),
    ("dbutils.notebook.run(dynamic_path, 60)", True),
]


@pytest.mark.parametrize("invocation,cohort", INLINE_PATHS)
def test_failed_dependency_cell_demotes_the_parent_task(
        monkeypatch, tmp_path, failing_dependency, invocation, cohort):
    result, _, kernel, _, _ = _run(
        monkeypatch, tmp_path, {"child": CHILD},
        parent=[invocation, "PARENT_TAIL = 1"], cohort=cohort)

    assert result["status"] == "PARTIAL", result
    assert result["child_failed"] == 1, result
    assert "PARENT_TAIL = 1" in _executed(kernel)


@pytest.mark.parametrize("invocation,cohort", INLINE_PATHS)
def test_strict_gating_still_fails_the_task_on_a_failed_dependency_cell(
        monkeypatch, tmp_path, failing_dependency, invocation, cohort):
    result, _, _, _, _ = _run(
        monkeypatch, tmp_path, {"child": CHILD},
        parent=[invocation, "PARENT_TAIL = 1"], strict=True, cohort=cohort)

    assert result["status"] == "FAIL", result


def test_failed_grandchild_cell_demotes_the_parent_task(
        monkeypatch, tmp_path, failing_dependency):
    result, _, _, _, _ = _run(
        monkeypatch, tmp_path,
        {"child": ["%run ./leaf", "CHILD_TAIL = 1"], "leaf": CHILD},
        parent=["%run /Workspace/child"])

    assert result["status"] == "PARTIAL", result
    assert result["child_failed"] == 1, result


def test_passing_dependency_keeps_the_parent_task_green(monkeypatch, tmp_path):
    result, _, _, _, _ = _run(
        monkeypatch, tmp_path, {"child": ["GOOD_DEP_CELL = 1"]},
        parent=["%run /Workspace/child"])

    assert result["status"] == "PASS", result
    assert result["child_failed"] == 0, result


def _child_repair_owners(child_name):
    return {e["_child_replay_owner"] for e in jm._cell_history
            if str(e.get("notebook_path", "")).endswith(child_name)
            and e.get("_child_replay_owner") is not None}


@pytest.mark.parametrize("leaf,repairable", [
    (["LEAF_OK = 1"], True),
    (CHILD, False),
])
def test_dependency_with_failed_nested_cells_is_not_offered_for_repair(
        monkeypatch, tmp_path, failing_dependency, leaf, repairable):
    # A completed dependency may be repaired and saved later only when it fully
    # validated; failed cells in a nested child count like the child's own.
    _run(monkeypatch, tmp_path,
         {"child": ["%run ./leaf", "CHILD_TAIL = 1"], "leaf": leaf})

    owners = _child_repair_owners("child.ipynb")
    assert owners
    assert all((o.completed_repair is not None) is repairable for o in owners)


def test_repair_rerun_with_failed_nested_cells_is_refused(monkeypatch):
    original = "def helper():\n    return 1\n"
    owner = jm._ChildReplayOwner()
    owner.close()
    owner.source_hashes = jm._child_source_hashes([original])
    owner.runtime_source_hashes = owner.source_hashes

    async def rerun(slot, code, session):
        return {"status": "ok", "did_exit": False,
                "failed_cells": ["leaf.ipynb: 1 cell(s) failed after all retries"]}

    owner.completed_repair = rerun
    entry = {"is_child": True, "_child_replay_owner": owner,
             "_child_replay_slot": 0, "notebook_path": "/Workspace/x/child.ipynb",
             "_task_key": "t", "replay_code": original, "cell_idx": 0}
    monkeypatch.setattr(jm, "_cell_history", [entry])

    with pytest.raises(jm.FixupPersistenceError):
        tce.asyncio.run(jm._repair_completed_dependency(
            entry, "def helper():\n    return 2\n", "fixture", session=object()))


@pytest.mark.parametrize('strict', [False, True])
@pytest.mark.parametrize('language', ['python', 'scala'])
def test_native_nested_failure_stops_dispatch_and_fails_parent(
        monkeypatch, tmp_path, failing_dependency, strict, language):
    source = f'%{language}\ndbutils.notebook.run("/Workspace/leaf", 60)'
    result, saved, kernel, _, _ = _run(
        monkeypatch, tmp_path, {'child': [source], 'leaf': CHILD},
        parent=['%run /Workspace/child'], strict=strict)
    assert result['status'] == 'FAIL', result
    assert saved['child'] == [source]
    assert not any('notebook.run(_aidp_run_args_' in code for code in kernel.executed)
    owners = _child_repair_owners('child.ipynb')
    assert all(owner.completed_repair is None for owner in owners)
