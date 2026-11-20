"""Explicit pip distributions do not acquire local-source import semantics.

The old local-resolver cases are retained as collision inputs: a same-named
source file must neither substitute for an explicit distribution requirement
nor trigger injected sys.path/module switching. Missing-import recovery still
uses its existing local-module guard.
"""
import asyncio
import contextlib
import io
import json
import sys
import types

import pytest

from test_pip_magic_gate import (
    jm, cluster_lifecycle as lifecycle, _drive_strict, _patch_common,
    _saved_notebook, _Kernel,
)


class ProbeKernel:
    async def execute(self, code, **kwargs):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exec(code, {})
        return {"status": "ok", "outputs": [{"type": "stream", "text": output.getvalue()}]}


def run_loop(monkeypatch, tmp_path, role, source, strict, scan, kernel=None):
    nb = {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                    "outputs": [], "execution_count": None}],
          "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    installs = []
    _patch_common(monkeypatch, tmp_path, nb, installs)
    monkeypatch.setattr(lifecycle, "_discover_local_modules", scan)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", role == "child")
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    kernel = kernel or _Kernel()
    if role == "parent":
        result = asyncio.run(jm.process_notebook(
            "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "task", {}))
        saved = "".join(_saved_notebook(kernel)["cells"][0]["source"])
    else:
        path = "/Workspace/test_pip/notebooks/child.ipynb"
        jm._notebook_content_cache[path] = json.dumps(nb)
        result = asyncio.run(jm._inline_child_notebook(
            path, kernel, jm.NotebookLog(path, "test_pip", "child"), "test_pip", "", "", "", {}))
        saved = "".join(json.loads(jm._notebook_content_cache[path])["cells"][0]["source"])
    return result, installs, saved, kernel


@pytest.mark.parametrize("result", [
    {"status": "ok", "outputs": []},
    {"status": "error", "outputs": []},
    {"status": "ok", "outputs": [{"type": "error", "evalue": "scan failed"}]},
    {"status": "ok", "outputs": [{"type": "stream", "text": "not json"}]},
])
def test_scan_failure_does_not_become_empty_success(result):
    class Kernel:
        async def execute(self, *args, **kwargs):
            return result
    with pytest.raises(RuntimeError, match="scan"):
        asyncio.run(lifecycle._discover_local_modules(
            Kernel(), ["/Workspace/project/nb.ipynb"], {"helpers"}, strict=True))


def test_legacy_boolean_local_skip_remains_false(monkeypatch):
    async def discover(*args, **kwargs):
        return {"helpers"}, set(), ["/Workspace/project/helpers.py"]
    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    assert asyncio.run(lifecycle.install_missing_package(
        "test", ProbeKernel(), "helpers", "/unused",
        notebook_paths=["/Workspace/project/nb.ipynb"])) is False


@pytest.mark.parametrize("response,cause", [
    (ConnectionError("fixture disconnected"), "fixture disconnected"),
    (TimeoutError("fixture timed out"), "fixture timed out"),
    ({"status": "error", "outputs": []}, "kernel error"),
    ({"status": "ok", "outputs": [{"type": "error", "evalue": "fixture error"}]}, "kernel error"),
    ({"status": "ok", "outputs": [{"output_type": "error", "evalue": "fixture error"}]}, "kernel error"),
    ({"status": "ok", "outputs": []}, "no local-module scan result"),
    ({"status": "ok", "outputs": [{"type": "stream", "text": "__AIDP_LOCAL_SCAN__not json"}]}, "scan parse failed"),
    ({"status": "ok", "outputs": [{"type": "stream", "text": '__AIDP_LOCAL_SCAN__"not a mapping"'}]}, "invalid local-module scan result"),
    ({"status": "ok", "outputs": [{"type": "stream", "text": '__AIDP_LOCAL_SCAN__{"helpers": "not paths"}'}]}, "invalid local-module scan result"),
])
def test_scan_failure_keeps_its_diagnostic_without_retry(response, cause):
    # Preserve the old scan-error pins at the scanner boundary. Explicit pip
    # no longer calls it; its remaining callers must not get fabricated hits.
    calls = []
    class Kernel:
        async def execute(self, code, **kwargs):
            calls.append(code)
            if isinstance(response, Exception):
                raise response
            return response
    with pytest.raises(RuntimeError, match=cause):
        asyncio.run(lifecycle._discover_local_modules(
            Kernel(), ["/Workspace/project/nb.ipynb"], {"helpers"}, strict=True))
    assert len(calls) == 1


@pytest.mark.parametrize("wrapper", ["plain", "json", "empty_prefix", "duplicate"])
def test_real_discovery_output_formats(tmp_path, wrapper):
    (tmp_path / "pip_fixture_helper.py").write_text("VALUE = 42\n")
    class WrappedKernel(ProbeKernel):
        async def execute(self, code, **kwargs):
            # Scope the real generated scanner to this test's directory.
            code = code.replace('roots = ["/Workspace/project"]',
                                f"roots = [{str(tmp_path)!r}]")
            result = await super().execute(code, **kwargs)
            value = result["outputs"][0]["text"]
            if wrapper == "json":
                value = json.dumps([{"type": "TEXT_PLAIN", "value": value}])
            elif wrapper == "empty_prefix":
                value = "[]" + value
            elif wrapper == "duplicate":
                value += value
            result["outputs"][0]["text"] = value
            return result
    matched, _, _ = asyncio.run(lifecycle._discover_local_modules(
        WrappedKernel(), ["/Workspace/project/notebook.ipynb"], {"pip_fixture_helper"},
        max_ancestor_depth=1, strict=True))
    assert matched == {"pip_fixture_helper"}


@pytest.mark.parametrize("role", ["parent", "child"])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
@pytest.mark.parametrize("case", ["module", "package", "missing", "unrelated", "ambiguous",
                                  "empty_package", "conflict", "escape", "scan_error", "native", "pinned",
                                  "pinned_form_feed", "pinned_unicode_separator"])
def test_real_loops_local_dependency_matrix(monkeypatch, tmp_path, role, strict, mixed, case):
    # Explicit 'pip install name' requests a distribution, not a sibling .py.
    # This replaces the removed implicit local-path injection contract.
    spec = "pip_fixture_helper==1.0" if case.startswith("pinned") else "pip_fixture_helper"
    source = f"%pip install {spec}\n"
    if case == "pinned_form_feed":
        source = "# source context\f\n" + source
    elif case == "pinned_unicode_separator":
        source = "# source context\u2028\n" + source
    if case == "native":
        source += "%sql SELECT 42 AS SENTINEL"
    elif mixed:
        source += "import pip_fixture_helper\nSENTINEL = 42"

    scans = []
    async def discover(*args, **kwargs):
        scans.append(args)
        if case == "scan_error":
            raise RuntimeError("local-module scan parse failed: no local-module scan result")
        matches = {
            "module": ["/source/pip_fixture_helper.py"],
            "package": ["/source/pip_fixture_helper/"],
            "unrelated": ["/source/other/pip_fixture_helper.py"],
            "ambiguous": ["/source/a/pip_fixture_helper.py", "/source/b/pip_fixture_helper.py"],
            "empty_package": ["/source/pip_fixture_helper/"],
            "escape": ["/outside/pip_fixture_helper.py"],
        }.get(case, [])
        return ({"pip_fixture_helper"} if matches else set()), set(), matches

    loaded = types.ModuleType("pip_fixture_helper")
    loaded.__file__ = "/source/pip_fixture_helper.py"
    monkeypatch.setitem(sys.modules, "pip_fixture_helper", loaded)
    original_path = list(sys.path)
    result, installs, saved, kernel = run_loop(
        monkeypatch, tmp_path, role, source, strict, discover)
    assert result["status"] == ("PASS" if role == "parent" else "ok"), result
    assert not result.get("unresolved_pip")

    assert scans == []
    assert installs == [spec]
    assert sys.modules["pip_fixture_helper"] is loaded
    assert sys.path == original_path
    assert "_aidp_local_dependency" not in saved
    assert "verified local source" not in saved
    assert not jm._find_pip_installs(saved)[0]
    executed = "\n".join(kernel.executed)
    assert "__AIDP_LOCAL_SCAN__" not in executed
    assert "__AIDP_LOCAL_DEP__" not in executed
    if mixed and case != "native":
        assert "SENTINEL = 42" in executed
    assert spec in saved


@pytest.mark.parametrize("role", ["parent", "child"])
@pytest.mark.parametrize("package", ["prophet", "requests", "mlflow", "typing_extensions", "slack_sdk", "pulp"])
def test_incident_scan_response_cannot_block_explicit_pip(monkeypatch, tmp_path, role, package):
    real_scan = lifecycle._discover_local_modules
    class ScanFailureKernel(_Kernel):
        async def execute(self, code, **kwargs):
            if "__AIDP_LOCAL_SCAN__" in code or "os.walk(" in code:
                self.executed.append(code)
                return {"status": "ok", "outputs": []}
            return await super().execute(code, **kwargs)

    source = f"# old install\n!pip install {package}\nSENTINEL = 42"
    result, installs, saved, kernel = run_loop(
        monkeypatch, tmp_path, role, source, False, real_scan, ScanFailureKernel())
    assert result["status"] == ("PASS" if role == "parent" else "ok"), result
    assert installs == [package]
    assert any("SENTINEL = 42" in code for code in kernel.executed)
    assert not any("__AIDP_LOCAL_SCAN__" in code or "os.walk(" in code for code in kernel.executed)


def test_code_only_does_not_probe_or_claim_verified_local_source(monkeypatch, tmp_path):
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path, ["%pip install pip_fixture_helper\nimport pip_fixture_helper"],
        True, run_all=False)
    probes = []
    async def discovery(*args, **kwargs):
        probes.append(args)
        raise AssertionError("code-only cannot verify local source")
    monkeypatch.setattr(lifecycle, "_discover_local_modules", discovery)
    result = asyncio.run(coro)
    assert result["status"] == "CONVERTED_NOT_VALIDATED"
    assert not probes and not installs
    saved = "".join(_saved_notebook(kernel)["cells"][0]["source"])
    assert "not installed by this run" in saved
    assert "verified local source" not in saved
    assert not jm._find_pip_installs(saved)[0]


def test_pip_rewrite_preserves_docstring_future_import_and_original_code(monkeypatch, tmp_path):
    original = ('"""A module docstring."""\nfrom __future__ import annotations\n'
                '%pip install pip_fixture_helper\nX = 42')
    async def no_scan(*args, **kwargs):
        pytest.fail("explicit pip must not scan local sources")
    _, installs, saved, _ = run_loop(monkeypatch, tmp_path, "parent", original, True, no_scan)
    namespace = {}
    exec(saved, namespace)
    assert namespace["__doc__"] == "A module docstring."
    assert namespace["X"] == 42
    assert installs == ["pip_fixture_helper"]
    assert "_aidp_local_dependency" not in saved


@pytest.mark.parametrize("external_first", [False, True])
def test_two_children_share_distribution_cache_without_switching_local_sources(
        monkeypatch, tmp_path, external_first):
    from test_child_exit_conversion import _notebook
    source = (["%pip install pip_fixture_helper"] if external_first else []) + [
        "%run /Workspace/childone", "%run /Workspace/childtwo"]
    installs = []
    _patch_common(monkeypatch, tmp_path, _notebook(source), installs)
    mapping, cache = {}, {}
    for name in ("childone", "childtwo"):
        path = f"/Workspace/test_pip/notebooks/{name}.ipynb"
        mapping[f"/Workspace/{name}.ipynb"] = path
        cache[path] = json.dumps(_notebook(["%pip install pip_fixture_helper\nSENTINEL = 42"]))
    async def no_scan(*args, **kwargs):
        pytest.fail("distribution cache must not switch to a child's source tree")
    monkeypatch.setattr(lifecycle, "_discover_local_modules", no_scan)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "_notebook_content_cache", cache)
    monkeypatch.setattr(jm, "_inlined_nb_run_children", set())
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/parent.ipynb", kernel, "test_pip", "task", {}, dep_path_map=mapping))
    assert result["status"] == "PASS", result
    assert installs == ["pip_fixture_helper"]
    for path in mapping.values():
        saved = "".join(json.loads(jm._notebook_content_cache[path])["cells"][0]["source"])
        assert not jm._find_pip_installs(saved)[0]
        assert "SENTINEL = 42" in saved
        assert "_aidp_local_dependency" not in saved
