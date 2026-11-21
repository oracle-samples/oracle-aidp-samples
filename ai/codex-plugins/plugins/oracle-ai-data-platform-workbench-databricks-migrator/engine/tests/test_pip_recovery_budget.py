"""Explicit pip dependencies must not spend the missing-import recovery budget.

Drive the real parent/child cell loops. Only the kernel, installer, and model
repair are mocked; no packages are installed and no live cluster is contacted.
"""
import asyncio
import json
import re

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, jm, cluster_lifecycle


def _notebook(sources):
    return {"cells": [{"cell_type": "code", "source": [s], "metadata": {},
                       "outputs": [], "execution_count": None} for s in sources],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}


class _ImportKernel(_Kernel):
    def __init__(self, always_missing=()):
        super().__init__()
        self.available = set()
        self.always_missing = set(always_missing)

    async def execute(self, code, **kwargs):
        # Match only the fixture's executable imports, never bootstrap/save code.
        imports = re.findall(r"(?m)^import ((?:need_|decl_)\w+|boto3)\s*$", code)
        for module in imports:
            if module not in self.available or module in self.always_missing:
                self.executed.append(code)
                message = f"No module named '{module}'"
                return {"status": "error", "outputs": [{
                    "type": "error", "ename": "ModuleNotFoundError", "evalue": message,
                    "traceback": [f"ModuleNotFoundError: {message}"]}]}
        return await super().execute(code, **kwargs)


def _run(monkeypatch, tmp_path, sources, *, path="parent", children=None,
         limit=2, outcomes=None, always_missing=(), repair_succeeds=False,
         strict=False, direct=False, cohort=False):
    calls, fixes, states = [], [], []
    _patch_common(monkeypatch, tmp_path, _notebook(sources), [])
    kernel = _ImportKernel(always_missing)
    outcomes = outcomes or {}

    async def install(cluster_id, session, package, *args, **kwargs):
        calls.append(package)
        outcome = outcomes.get(package, True)
        if isinstance(outcome, list):
            index = calls.count(package) - 1
            outcome = outcome[min(index, len(outcome) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        if outcome:
            kernel.available.add(package)
        return outcome

    async def fix(code, *args, **kwargs):
        fixes.append(code)
        # A simulated successful repair resolves the fixture's imports without
        # changing its code. Otherwise retain the error to exercise bounded retries.
        if repair_succeeds:
            kernel.available.update(re.findall(r"(?m)^import (\w+)\s*$", code))
            kernel.always_missing.clear()
        return code

    async def summarize(*args, **kwargs):
        return "fixture import cell"

    async def monitor(*args, **kwargs):
        return "local", "fixture import error"

    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", install)
    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "monitor_analyze_failures", monitor)
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **kw: "")
    monkeypatch.setattr(jm, "MAX_AUTO_INSTALLS", limit)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 3)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", direct)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_inlined_nb_run_children", set())
    mapping = {}
    for name, contents in (children or {}).items():
        child_path = f"/Workspace/test_pip/notebooks/{name}.ipynb"
        jm._notebook_content_cache[child_path] = json.dumps(_notebook(contents))
        mapping[f"/Workspace/test_pip/{name}.ipynb"] = child_path
        mapping[f"/Workspace/test_pip/notebooks/{name}.ipynb"] = child_path
    monkeypatch.setattr(jm, "_resolve_nbrun_child_path", lambda name, *a, **kw:
                        f"/Workspace/test_pip/notebooks/{name.rsplit('/', 1)[-1].removesuffix('.ipynb')}.ipynb")
    if cohort:
        monkeypatch.setattr(jm, "extract_notebook_run_cohort_map",
                            lambda nb: {"./dep": "items"})

        async def samples(*a, **kw):
            return {"items": "fixture"}

        monkeypatch.setattr(jm, "extract_cohort_samples", samples)

    real_child = jm._inline_child_notebook

    async def observe_child(*args, **kwargs):
        states.append((kwargs.get("auto_installed_modules"),
                       kwargs.get("recovery_attempted_modules")))
        return await real_child(*args, **kwargs)

    monkeypatch.setattr(jm, "_inline_child_notebook", observe_child)
    if path == "parent":
        result = asyncio.run(jm.process_notebook(
            "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {},
            dep_path_map=mapping))
    else:
        child_path = "/Workspace/test_pip/notebooks/standalone.ipynb"
        jm._notebook_content_cache[child_path] = json.dumps(_notebook(sources))
        result = asyncio.run(real_child(
            child_path, kernel, jm.NotebookLog(child_path, "test_pip", "child"),
            "test_pip", "", "", "", mapping))
    return result, calls, fixes, kernel, states


@pytest.mark.parametrize("path", ["parent", "child"])
@pytest.mark.parametrize("count,limit", [(2, 2), (3, 2), (10, 10)])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("direct", [False, True])
def test_declared_dependencies_do_not_exhaust_recovery(
        monkeypatch, tmp_path, path, count, limit, strict, direct):
    declared = [f"decl_{i}" for i in range(count)]
    sources = ["!pip install " + " ".join(declared), "import need_extra"]
    result, calls, fixes, _, _ = _run(
        monkeypatch, tmp_path, sources, path=path, limit=limit, strict=strict, direct=direct)
    assert calls == declared + ["need_extra"]
    assert fixes == [], "normal dependency installs exhausted automatic recovery"
    assert result["status"] == ("PASS" if path == "parent" else "ok"), result
    assert result["ok" if path == "parent" else "cells_ok"] == 2


@pytest.mark.parametrize("invocation,cohort", [
    ("%run ./dep", False), ('dbutils.notebook.run("./dep", 60)', False),
    ('%scala\ndbutils.notebook.run("./dep", 60)', False),
    ('dbutils.notebook.run(dynamic_path, 60)', True),
])
def test_parent_declared_installs_leave_recovery_for_child(monkeypatch, tmp_path, invocation, cohort):
    result, calls, fixes, _, states = _run(
        monkeypatch, tmp_path, ["!pip install decl_a decl_b", invocation],
        children={"dep": ["import need_child"]}, cohort=cohort)
    assert calls == ["decl_a", "decl_b", "need_child"]
    assert fixes == []
    assert result["status"] == "PASS", result
    assert states and all(s[1] == {"need_child"} for s in states)


def test_child_declared_installs_leave_recovery_for_parent(monkeypatch, tmp_path):
    result, calls, fixes, _, states = _run(
        monkeypatch, tmp_path, ["%run ./dep", "import need_parent"],
        children={"dep": ["!pip install decl_a decl_b"]})
    assert calls == ["decl_a", "decl_b", "need_parent"]
    assert fixes == []
    assert result["status"] == "PASS", result
    assert states[0][1] == {"need_parent"}


@pytest.mark.parametrize("path", ["parent", "child"])
@pytest.mark.parametrize("outcome", [True, False, RuntimeError("installer unavailable")])
def test_recovery_limit_counts_attempts_even_when_install_fails(
        monkeypatch, tmp_path, capsys, path, outcome):
    result, calls, fixes, _, _ = _run(
        monkeypatch, tmp_path, ["import need_a", "import need_b", "import need_c"],
        path=path, outcomes={"need_a": outcome}, repair_succeeds=True)
    assert calls == ["need_a", "need_b"], "the third distinct recovery exceeds the cap"
    assert len(fixes) == (1 if outcome is True else 2)
    assert result["status"] == ("PASS" if path == "parent" else "ok"), result
    logs = capsys.readouterr().out
    assert "recovery attempt 1/2" in logs
    assert "recovery attempt 2/2" in logs
    assert "2 recovery attempts; explicit pip installs do not count" in logs


@pytest.mark.parametrize("path", ["parent", "child"])
@pytest.mark.parametrize("outcome", [True, False, RuntimeError("installer unavailable")])
def test_repeated_missing_module_does_not_loop_installs(monkeypatch, tmp_path, path, outcome):
    _, calls, fixes, kernel, _ = _run(
        monkeypatch, tmp_path, ["import need_stuck"], path=path,
        outcomes={"need_stuck": outcome}, always_missing={"need_stuck"})
    assert calls == ["need_stuck"]
    # Keep existing retry accounting: a successful install consumes an iteration
    # of the parent's for-loop, but not a repair attempt in the child's while-loop.
    assert len(fixes) == (1 if path == "parent" and outcome is True else 2)
    executions = sum(code.strip() == "import need_stuck" for code in kernel.executed)
    assert executions <= 4


@pytest.mark.parametrize("path", ["parent", "child"])
@pytest.mark.parametrize("sources", [
    ["!pip install need_a", "!pip install need_a", "import need_a"],
    ["import need_a", "!pip install need_a", "import need_a"],
])
def test_successful_installs_are_deduplicated_across_both_sources(
        monkeypatch, tmp_path, path, sources):
    result, calls, fixes, _, _ = _run(monkeypatch, tmp_path, sources, path=path)
    assert calls == ["need_a"]
    assert fixes == []
    assert result["status"] == ("PASS" if path == "parent" else "ok"), result


@pytest.mark.parametrize("path", ["parent", "child"])
def test_failed_recovery_is_not_mistaken_for_a_successful_explicit_install(
        monkeypatch, tmp_path, path):
    _, calls, fixes, _, _ = _run(
        monkeypatch, tmp_path, ["import need_a", "!pip install need_a"], path=path,
        outcomes={"need_a": [False, True]}, repair_succeeds=True)
    assert calls == ["need_a", "need_a"]
    assert len(fixes) == 1


@pytest.mark.parametrize("path", ["parent", "child"])
def test_failed_explicit_install_still_allows_missing_import_recovery(
        monkeypatch, tmp_path, path):
    result, calls, fixes, _, _ = _run(
        monkeypatch, tmp_path, ["!pip install need_a", "import need_a"], path=path,
        outcomes={"need_a": [False, True]})
    assert calls == ["need_a", "need_a"]
    assert fixes == []
    if path == "parent":
        assert result["status"] == "PARTIAL", "the earlier explicit failure must remain reported"
    else:
        assert result["failed_installs"] == ["need_a"]


@pytest.mark.parametrize("path", ["parent", "child"])
def test_full_recovery_budget_does_not_cap_declared_dependencies(monkeypatch, tmp_path, path):
    declared = [f"decl_{i}" for i in range(11)]
    result, calls, fixes, _, _ = _run(
        monkeypatch, tmp_path, ["import need_a", "import need_b",
                                "!pip install " + " ".join(declared)], path=path)
    assert calls == ["need_a", "need_b"] + declared
    assert fixes == []
    assert result["status"] == ("PASS" if path == "parent" else "ok"), result


@pytest.mark.parametrize("path", ["parent", "child"])
def test_aws_exclusion_does_not_spend_a_recovery_attempt(monkeypatch, tmp_path, path):
    result, calls, fixes, _, _ = _run(
        monkeypatch, tmp_path, ["import boto3", "import need_a"], path=path,
        limit=1, repair_succeeds=True)
    assert calls == ["need_a"]
    assert len(fixes) == 1
    assert result["status"] == ("PASS" if path == "parent" else "ok"), result


@pytest.mark.parametrize("nested", [False, True])
def test_recovery_budget_is_shared_with_sibling_and_nested_children(
        monkeypatch, tmp_path, nested):
    sources = ["!pip install decl_a decl_b", "import need_parent", "%run ./dep"]
    children = {"dep": ["import need_child"], "grand": ["import need_grand"]}
    if nested:
        children["dep"].append("%run ./grand")
    else:
        sources.append("%run ./grand")
    sources.append("import need_later")
    result, calls, fixes, _, states = _run(
        monkeypatch, tmp_path, sources, children=children, repair_succeeds=True)
    assert calls == ["decl_a", "decl_b", "need_parent", "need_child"]
    assert len(fixes) == 2
    assert result["status"] == "PASS", result
    assert len(states) == 2
    assert states[0][0] is states[1][0], "successful installs must deduplicate across children"
    assert states[0][1] is states[1][1], "each child must not get a fresh recovery budget"
    assert states[0][1] == {"need_parent", "need_child"}
    assert states[0][0] == {"decl_a", "decl_b", "need_parent", "need_child"}
