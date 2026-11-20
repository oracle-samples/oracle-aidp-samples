"""Explicit pip constraints survive resolution, caching and both notebook loops."""
import asyncio
import json
from types import SimpleNamespace

import pytest
from packaging.requirements import Requirement

from test_pip_magic_gate import (
    _drive_child, _drive_strict, _saved_notebook, _patch_common, _Kernel,
    cluster_lifecycle as lifecycle, jm,
)


@pytest.fixture(autouse=True)
def configured_migration_output(monkeypatch):
    # The public runner configures this before calling the pip installer.
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_output")


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("specs,expected", [
    (("version_fixture==2.21.0",), "version_fixture==2.21.0"),
    (("version_fixture>=2,<3",), "version_fixture>=2,<3"),
    (("version_fixture~=2.1",), "version_fixture~=2.1"),
    (("version_fixture==2.*",), "version_fixture==2.*"),
    (("version_fixture", "version_fixture>=2", "version_fixture<3"),
     "version_fixture>=2,<3"),
    (("version_fixture==1", "version_fixture==2"), "version_fixture==1,==2"),
])
def test_resolver_preserves_all_constraints_even_when_name_cached(monkeypatch, cached, specs, expected):
    installs = []

    async def discover(*args, **kwargs):
        return set(), set(), []

    async def install(cluster_id, session, spec, output_path, **kwargs):
        installs.append(spec)
        return True

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    monkeypatch.setattr(lifecycle, "install_missing_package", install)
    result = asyncio.run(jm._resolve_pip_cell_packages(
        ["version_fixture"], SimpleNamespace(cluster_id="test"), "job",
        {"version_fixture"} if cached else set(), SimpleNamespace(log=lambda message: None),
        "\n".join("%pip install " + spec for spec in specs)))
    assert result == []
    assert [Requirement(spec) for spec in installs] == [Requirement(expected)]


_REQUESTS = [
    (["version_fixture==2.21.0"], ["version_fixture==2.21.0"]),
    (["'version_fixture>=2,<3'"], ["version_fixture>=2,<3"]),
    (["version_fixture>=2 version_fixture<3"], ["version_fixture>=2,<3"]),
    (["version_fixture>=2\n!pip install version_fixture<3"], ["version_fixture>=2,<3"]),
    (["version_fixture", "version_fixture==2.21.0", "version_fixture==2.21.0",
      "version_fixture==2.22.0"],
     ["version_fixture", "version_fixture==2.21.0", "version_fixture==2.21.0",
      "version_fixture==2.22.0"]),
    (["version_fixture", "version_fixture"], ["version_fixture"]),
]


@pytest.mark.parametrize("args,expected", _REQUESTS)
@pytest.mark.parametrize("role", ["parent", "child"])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("ok", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_notebook_loops_preserve_constraints_and_failure_gates(
        monkeypatch, tmp_path, args, expected, role, strict, ok, mixed):
    sources = [f"%pip install {arg}" + ("\nSENTINEL = 42" if mixed else "") for arg in args]
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    if role == "parent":
        kernel, installs, coro = _drive_strict(monkeypatch, tmp_path, sources, ok)
        monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
        if strict and not ok:
            with pytest.raises(RuntimeError, match="were not installed"):
                asyncio.run(coro)
        else:
            result = asyncio.run(coro)
            assert result["status"] == ("PASS" if ok else "PARTIAL" if mixed else "FAIL"), result
            saved = "\n".join("".join(c["source"]) for c in _saved_notebook(kernel)["cells"])
            assert not jm._find_pip_installs(saved)[0]
            if mixed:
                assert "SENTINEL = 42" in saved
            if not ok:
                assert jm._PIP_NOTE_FAILED in saved
    else:
        result, installs = _drive_child(monkeypatch, sources, ok, strict)
        assert result["status"] == ("error" if strict and not ok else "ok"), result
        if strict and not ok:
            assert "were not installed" in result["error"]
        else:
            assert bool(result.get("failed_installs")) is not ok, result
    if strict and not ok:
        expected = expected[:1]
    elif not ok and args == ["version_fixture", "version_fixture"]:
        # Failed installs must not enter even the unpinned name cache.
        expected = ["version_fixture", "version_fixture"]
    assert [Requirement(spec) for spec in installs] == [Requirement(spec) for spec in expected]


@pytest.mark.parametrize("ok", [False, True])
def test_parent_name_cache_does_not_satisfy_child_pin(monkeypatch, tmp_path, ok):
    def notebook(sources):
        return {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                           "outputs": [], "execution_count": None} for source in sources],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}

    installs = []
    parent = notebook(["%pip install version_fixture", "%run /Workspace/child"])
    _patch_common(monkeypatch, tmp_path, parent, installs)
    path = "/Workspace/test_pip/notebooks/child.ipynb"
    child = notebook(["%pip install version_fixture==2.21.0"])
    monkeypatch.setattr(jm, "_notebook_content_cache", {path: json.dumps(child)})
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)

    async def install(cluster_id, session, spec, output_path, **kwargs):
        installs.append(spec)
        return True if spec == "version_fixture" else ok

    monkeypatch.setattr(lifecycle, "install_missing_package", install)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/parent.ipynb", _Kernel(), "test_pip", "parent", {},
        dep_path_map={"/Workspace/child.ipynb": path}))
    assert installs == ["version_fixture", "version_fixture==2.21.0"]
    assert result["status"] == ("PASS" if ok else "PARTIAL"), result
    saved = "".join(json.loads(jm._notebook_content_cache[path])["cells"][0]["source"])
    assert not jm._find_pip_installs(saved)[0]
    assert "version_fixture==2.21.0" in saved
    if not ok:
        assert jm._PIP_NOTE_FAILED in saved


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("satisfied", [False, True])
def test_resolver_reaches_real_installer_version_check_and_persistence(
        monkeypatch, tmp_path, cached, satisfied):
    # Run the existing installer and generated kernel probes against local
    # pytest metadata. Only cluster-library service calls are mocked.
    import importlib.metadata
    from test_customization_pack import _ExecSession, _pip_env, _req_file

    _, installs = _pip_env(monkeypatch)

    async def discover(*args, **kwargs):
        return set(), set(), []

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    version = importlib.metadata.version("pytest") if satisfied else "0.0.1"
    spec = f"pytest=={version}"
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", tmp_path.as_posix())
    session = _ExecSession()
    session.cluster_id = "test"
    result = asyncio.run(jm._resolve_pip_cell_packages(
        ["pytest"], session, "job", {"pytest"} if cached else set(),
        SimpleNamespace(log=lambda message: None), "%pip install " + spec))
    assert result == []
    assert len(installs) == (0 if satisfied else 1)
    assert spec in _req_file(tmp_path / "job").read_text()


def test_explicit_distribution_with_different_import_name_uses_metadata(
        monkeypatch, tmp_path):
    """A successful PyYAML install must not be verified as ``import PyYAML``."""
    from test_customization_pack import _ExecSession

    installs = []

    async def poll(*args, **kwargs):
        return True

    async def no_sleep(*args, **kwargs):
        return None

    class _ForceInstallSession(_ExecSession):
        cluster_id = "test"

        def __init__(self):
            self.force_missing_once = True

        async def execute(self, code, timeout=None):
            # Force the install branch once. The post-install distribution
            # verification then executes against real local PyYAML metadata.
            if self.force_missing_once and "_md.version('PyYAML')" in code:
                self.force_missing_once = False
                return {"status": "ok", "outputs": [
                    {"type": "stream", "text": "MISSING\n"}
                ]}
            return await super().execute(code, timeout)

    monkeypatch.setattr(lifecycle, "get_installed_libraries", lambda cluster_id: [])
    monkeypatch.setattr(lifecycle, "_resolve_pip_name", lambda name: name)
    monkeypatch.setattr(
        lifecycle, "install_library",
        lambda cluster_id, path: installs.append(path),
    )
    monkeypatch.setattr(lifecycle, "_poll_library_install", poll)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", tmp_path.as_posix())

    failed = asyncio.run(jm._resolve_pip_cell_packages(
        ["PyYAML"], _ForceInstallSession(), "job", set(),
        SimpleNamespace(log=lambda message: None), "%pip install PyYAML",
    ))

    assert failed == []
    assert len(installs) == 1
