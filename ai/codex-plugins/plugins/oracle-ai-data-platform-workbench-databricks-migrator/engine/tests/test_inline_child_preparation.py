"""Manifest helpers must be prepared before execution; inline reads never rediscover.

Run c6f9277d (2026-10-16): all_utils -> ReadBatchFlow executed successfully,
then refused save-back because its owner was still /Workspace/Users/....
"""
import asyncio
import contextlib
import contextvars
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_child_exit_conversion import _ExitKernel, _notebook
from test_pip_magic_gate import _patch_common, jm


class FilesystemKernel(_ExitKernel):
    async def execute(self, code, **kwargs):
        if "__AIDP_ISO_OK__" in code:
            return await super().execute(code, **kwargs)
        if any(marker in code for marker in (
            "found_path = None", "src = ''.join", "with open('",
            "with builtins.open('", "p = '",
        )):
            self.executed.append(code)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                exec(code, {})
            return {"status": "ok", "outputs": [
                {"type": "stream", "text": out.getvalue()}]}
        return await super().execute(code, **kwargs)


@pytest.fixture
def flow(monkeypatch, tmp_path):
    _patch_common(monkeypatch, tmp_path, _notebook([]), [])
    source = tmp_path / "source"
    source.mkdir()
    output = tmp_path / "output"
    root = Path(jm.migration_job_output_path(str(output), "fixture")) / "notebooks"
    root.mkdir(parents=True)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(output))
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(root))
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "JOB_ROOT", str(source))
    declared = {jm.normalize_nb_path(str(source / name))
                for name in ("helper.ipynb", "outer.ipynb")}
    declared.add("missing.ipynb")
    monkeypatch.setattr(jm, "_manifest_dependency_paths",
                        contextvars.ContextVar("manifest_test", default=declared))
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    monkeypatch.setattr(jm, "_inlined_nb_run_children", set())
    async def summarize(*args, **kwargs):
        return "fixture cell"

    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "download_notebook", lambda path: Path('/' + path.lstrip('/')).read_bytes())
    return source, root


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("reader", ["filesystem", "cache", "api"])
def test_real_preparation_persists_and_rewrites_original_helper(monkeypatch, flow, nested, reader):
    source, root = flow
    helper = source / "helper.ipynb"
    helper.write_text(json.dumps(_notebook([
        "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(oidlUtils)",
        "def fixture_value():\n    return 42",
    ])))
    outer = source / "outer.ipynb"
    outer.write_text(json.dumps(_notebook([f'%run "{helper}"'])))
    originals = {p: p.read_bytes() for p in (helper, outer)}
    entry = outer if nested else helper
    # Production preparation runs at the manifest/job boundary, never inside
    # inline execution. Preserve the historical reader matrix for prepared files.
    manifest = {jm.normalize_nb_path(str(helper)): [],
                jm.normalize_nb_path(str(outer)): [
                    {"path": str(helper), "original_path": str(helper),
                     "exists": True, "nested_deps": []}]}
    kernel = FilesystemKernel(fire=False)
    prepared = asyncio.run(jm.ensure_migrated(
        str(entry), kernel, job_name="fixture", manifest_dependency_map=manifest))
    mapping = {str(entry): prepared}
    if reader != "cache":
        jm._notebook_content_cache.clear()
    if reader == "api":
        # A migrated read is allowed even with an original cached beside it.
        jm._notebook_content_cache[str(entry)] = entry.read_text()
    result = asyncio.run(jm._inline_child_notebook(
        str(entry), kernel, SimpleNamespace(log=lambda *a: None),
        "fixture", "", "", "", mapping))
    assert result["status"] == "ok", result
    assert result["resolved_path"].startswith(str(root) + "/")
    helper_key = jm.normalize_nb_path(str(helper))
    migrated_helper = root / helper_key
    assert migrated_helper.is_file()
    migrated_sources = "\n".join("".join(c["source"]) for c in json.loads(migrated_helper.read_text())["cells"])
    assert "from aidp_compat import" in migrated_sources
    if nested:
        assert str(migrated_helper) in (root / jm.normalize_nb_path(str(outer))).read_text()
    assert all(p.read_bytes() == data for p, data in originals.items())
    assert jm._migration_cache[helper_key] == str(migrated_helper)
    # A fresh reader must use the persisted artifact, not a warm source cache.
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    again = asyncio.run(jm._inline_child_notebook(
        result["resolved_path"], FilesystemKernel(fire=False),
        SimpleNamespace(log=lambda *a: None), "fixture", "", "", "", mapping))
    assert again["status"] == "ok", again


@pytest.mark.parametrize("provenance", ["map", "migration_cache", "content_cache"])
def test_recorded_destination_wins_over_cached_original(monkeypatch, flow, provenance):
    source, root = flow
    original = source / "helper.ipynb"
    original.write_text(json.dumps(_notebook(["VALUE = 1"])))
    migrated = root / (jm.normalize_nb_path(str(original)) if provenance == "content_cache" else "repaired.ipynb")
    migrated.parent.mkdir(parents=True, exist_ok=True)
    migrated.write_text(json.dumps(_notebook(["VALUE = 99"])))
    jm._notebook_content_cache[str(original)] = original.read_text()
    mapping = {str(original): str(migrated)} if provenance == "map" else {}
    if provenance == "migration_cache":
        jm._migration_cache[jm.normalize_nb_path(str(original))] = str(migrated)
    if provenance == "content_cache":
        jm._notebook_content_cache[str(migrated)] = migrated.read_text()
    kernel = FilesystemKernel(fire=False)
    result = asyncio.run(jm._inline_child_notebook(
        str(original), kernel, SimpleNamespace(log=lambda *a: None),
        "fixture", "", "", "", mapping))
    assert result["status"] == "ok", result
    assert result["resolved_path"] == str(migrated)
    assert "VALUE = 99" in kernel.executed
    assert "VALUE = 1" not in kernel.executed


def test_preparation_uses_export_instead_of_stale_original(monkeypatch, flow):
    source, root = flow
    original = source / "helper.ipynb"
    original.write_text(json.dumps(_notebook(["%run ./stale_missing_child"])))
    exported = source / "exported"
    relative = jm.normalize_nb_path(str(original))
    exported_helper = exported / relative
    exported_helper.parent.mkdir(parents=True)
    exported_helper.write_text(json.dumps(_notebook([
        "# %run ./stale_missing_child\nVALUE = 99",
    ])))
    original_bytes = original.read_bytes()
    monkeypatch.setattr(jm, "EXPORT_BASE", str(exported))
    kernel = FilesystemKernel(fire=False)
    prepared = asyncio.run(jm.ensure_migrated(
        str(original), kernel, job_name="fixture",
        manifest_dependency_map={relative: []}))
    result = asyncio.run(jm._inline_child_notebook(
        str(original), kernel, SimpleNamespace(log=lambda *a: None),
        "fixture", "", "", "", {str(original): prepared}))
    assert result["status"] == "ok", result
    assert "# %run ./stale_missing_child\nVALUE = 99" in kernel.executed
    assert original.read_bytes() == original_bytes
    assert "# %run ./stale_missing_child\nVALUE = 99" in [
        "".join(c["source"]) for c in json.loads((root / relative).read_text())["cells"]]


@pytest.mark.parametrize("failure", [None, "outside", "raised"])
def test_failed_preparation_never_executes_original(monkeypatch, flow, failure):
    source, root = flow
    helper = source / "helper.ipynb"
    helper.write_text(json.dumps(_notebook(["ORIGINAL_MUST_NOT_EXECUTE = 1"])))
    original = helper.read_bytes()
    calls = []

    async def prepare(path, session, **kwargs):
        calls.append(path)
        if failure == "raised":
            raise RuntimeError("fixture upload verification failed")
        return str(helper) if failure == "outside" else None

    monkeypatch.setattr(jm, "ensure_migrated", prepare)
    kernel = FilesystemKernel(fire=False)
    # Missing artifact variants must all refuse original execution, including
    # a constructed destination and a destination recorded but unreadable.
    entry = str(helper) if failure is None else str(root / "missing.ipynb")
    mapping = {str(helper): entry} if failure == "raised" else {}
    result = asyncio.run(jm._inline_child_notebook(
        entry, kernel, SimpleNamespace(log=lambda *a: None),
        "fixture", "", "", "", mapping))
    assert result["status"] == "error"
    assert result["kind"] == "MANIFEST_DEPENDENCY_UNPREPARED"
    assert calls == []  # no runtime ensure_migrated, not even as a fallback
    assert not any(code == "ORIGINAL_MUST_NOT_EXECUTE = 1" for code in kernel.executed)
    assert helper.read_bytes() == original
    assert not jm._migration_cache


def test_code_only_preparation_preserves_parent_repair_ownership(monkeypatch, flow):
    source, root = flow
    helper = source / "helper.ipynb"
    helper.write_text(json.dumps(_notebook(["VALUE = 42"])))
    owner = jm._ChildReplayOwner()
    owner.completed_repair = object()
    repair = owner.completed_repair
    jm._cell_history.append({"_child_replay_owner": owner,
                             "_task_key": jm._active_history_task_key()})
    kernel = FilesystemKernel(fire=False)

    async def run():
        bindings = jm._reset_active_slack_bindings()
        result = await jm._inline_child_notebook(
            str(helper), kernel, SimpleNamespace(log=lambda *a: None),
            "fixture", "", "", "", {})
        assert jm._current_slack_bindings() is bindings
        return result

    assert asyncio.run(run())["kind"] == "MANIFEST_DEPENDENCY_UNPREPARED"
    assert owner.completed_repair is repair


def test_code_only_preparation_cannot_clear_parent_persistence_failure(monkeypatch, flow):
    source, root = flow
    helper = source / "helper.ipynb"
    helper.write_text(json.dumps(_notebook(["VALUE = 42"])))
    monkeypatch.setattr(jm, "_fixup_persistence_failures", ["earlier unsaved repair"])
    result = asyncio.run(jm._inline_child_notebook(
        str(helper), FilesystemKernel(fire=False), SimpleNamespace(log=lambda *a: None),
        "fixture", "", "", "", {}))
    assert result["status"] == "error", result
    assert jm._fixup_persistence_failures == ["earlier unsaved repair"]


def test_standalone_conversion_still_resets_previous_task_failures(monkeypatch, flow):
    source, root = flow
    helper = source / "helper.ipynb"
    helper.write_text(json.dumps(_notebook(["VALUE = 42"])))
    monkeypatch.setattr(jm, "_fixup_persistence_failures", ["previous task failure"])
    result = asyncio.run(jm.process_notebook(
        str(helper), FilesystemKernel(fire=False), "fixture", "dep_helper", {}, run_all=False))
    assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    assert not jm._fixup_persistence_failures


def test_reachable_indented_exit_uses_existing_repair_loop(monkeypatch, tmp_path):
    from test_pip_magic_gate import _Kernel

    source = " dbutils.notebook.exit(0)"
    _patch_common(monkeypatch, tmp_path, _notebook([source]), [])
    repairs = []

    async def migrate(*args, **kwargs):
        return source, {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", migrate)

    async def fix(code, error, *args, **kwargs):
        repairs.append(error)
        assert "unexpected indent" in error
        return ("# dbutils.notebook.exit(0)\n"
                "# Oracle tool modification: removed stray top-level indentation\n"
                "aidputils.notebook.exit(0)")

    monkeypatch.setattr(jm, "call_fix", fix)

    class ExitKernel(_Kernel):
        async def execute(self, code, **kwargs):
            if any(line.startswith(" ") and "notebook.exit(0)" in line for line in code.splitlines()):
                return {"status": "error", "outputs": [{"type": "error",
                        "ename": "IndentationError", "evalue": "unexpected indent",
                        "traceback": ["IndentationError: unexpected indent"]}]}
            if "aidputils.notebook.exit(0)" in code and "base64" not in code:
                return {"status": "ok", "outputs": [{"type": "stream",
                        "text": "__AIDP_NOTEBOOK_EXIT__ value=0\n"}]}
            return await super().execute(code, **kwargs)

    kernel = ExitKernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/fixture/parent.ipynb", kernel, "fixture", "main", {}))
    assert result["status"] == "PASS", result
    assert len(repairs) == 1
    saved = next(json.loads(data) for path, data in kernel.files.items() if path.endswith("final.ipynb"))
    sources = "\n".join("".join(c["source"]) for c in saved["cells"])
    assert "\naidputils.notebook.exit(0)" in sources


def test_explicit_legacy_mode_keeps_original_reader(monkeypatch, flow):
    source, _ = flow
    helper = source / "legacy.ipynb"
    helper.write_text(json.dumps(_notebook(["VALUE = 7"])))
    monkeypatch.setattr(jm, "_manifest_dependency_paths",
                        contextvars.ContextVar("legacy_test", default=None))
    kernel = FilesystemKernel(fire=False)
    result = asyncio.run(jm._inline_child_notebook(
        str(helper), kernel, SimpleNamespace(log=lambda *a: None),
        "fixture", "", "", "", {}))
    assert result["status"] == "ok", result
    assert "VALUE = 7" in kernel.executed


def test_undeclared_warm_migrated_artifact_cannot_bypass_manifest(monkeypatch, flow):
    source, root = flow
    original = source / "undeclared.ipynb"
    migrated = root / jm.normalize_nb_path(str(original))
    jm._migration_cache[jm.normalize_nb_path(str(original))] = str(migrated)
    jm._notebook_content_cache[str(migrated)] = json.dumps(_notebook(["UNDECLARED = True"]))
    kernel = FilesystemKernel(fire=False)
    for entry in (str(original), str(migrated)):
        result = asyncio.run(jm._inline_child_notebook(
            entry, kernel, SimpleNamespace(log=lambda *a: None),
            "fixture", "", "", "", {}))
        assert result["kind"] == "MANIFEST_DEPENDENCY_UNPREPARED"
        assert "absent from the validated dependency manifest" in result["error"]
    assert not kernel.executed
