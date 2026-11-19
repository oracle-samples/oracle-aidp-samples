"""The migration registry must never claim what the disk does not hold.

A --skip-migrated resume reuses a migrated notebook only over its recorded
status, so two registry I/O failures became resume hazards:

- A failed write reported "Saved": save_registry ignored the kernel's result.
  A failed task's notebook was uploaded, its FAIL never reached disk, and the
  older PASS made a later resume accept the failed copy.
- A failed read (e.g. a timeout) returned {}; load_registry cached it and the
  next save rewrote the job's registry with only this run's entries.
"""
import asyncio
import contextlib
import copy
import io
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm  # noqa: E402

pytestmark = pytest.mark.real_registry

JOB = "persistence_job"
NB_A = "/Workspace/Users/x/a.ipynb"
NB_B = "/Workspace/Users/x/b.ipynb"


class Kernel:
    """Runs the registry's cell code for real against tmp_path files."""
    cluster_id = "test-cluster"

    def __init__(self, *, fail_writes=False, fail_reads=False, lose_output=False):
        self.fail_writes, self.fail_reads, self.lose_output = fail_writes, fail_reads, lose_output

    async def execute(self, code, **kwargs):
        if self.fail_reads and "json.load(f)" in code:
            raise asyncio.TimeoutError()
        if self.fail_writes and "b64decode" in code:
            return {"status": "error", "outputs": [
                {"type": "error", "ename": "OSError", "evalue": "[Errno 5] Input/output error",
                 "traceback": []}]}
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            exec(code, {})
        text = "" if self.lose_output else out.getvalue()
        return {"status": "ok", "outputs": [{"type": "stream", "text": text}] if text else []}


@pytest.fixture
def registry_file(monkeypatch, tmp_path):
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm, "_registry_cache", {})
    monkeypatch.setattr(jm, "_all_registries_merged", {})
    path = Path(jm._registry_path_for_job(JOB))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _entries(status):
    return {NB_A: {"migrated_path": "/m/a.ipynb", "job_name": JOB, "status": status},
            NB_B: {"migrated_path": "/m/b.ipynb", "job_name": JOB, "status": "PASS"}}


def test_confirmed_save_round_trips(registry_file):
    asyncio.run(jm.registry_update(Kernel(), NB_A, "/m/a.ipynb", JOB, "FAIL"))
    assert json.loads(registry_file.read_text())[NB_A]["status"] == "FAIL"
    assert not [p for p in registry_file.parent.iterdir() if p.name.endswith(".aidp-tmp")]


def _statuses(path):
    return {k: v["status"] for k, v in json.loads(path.read_text()).items()}


def test_failed_save_raises_and_keeps_the_previous_file(registry_file, capsys):
    registry_file.write_text(json.dumps(_entries("PASS")))
    jm._registry_cache[JOB] = copy.deepcopy(_entries("PASS"))

    with pytest.raises(jm.RegistryIOError, match="registry not saved"):
        asyncio.run(jm.registry_update(Kernel(fail_writes=True), NB_A, "/m/a.ipynb", JOB, "FAIL"))

    assert _statuses(registry_file) == {NB_A: "PASS", NB_B: "PASS"}  # whole file or nothing
    assert "Saved" not in capsys.readouterr().out


def test_save_without_confirmation_raises(registry_file, capsys):
    # The write may have landed; without its marker it is still not confirmed.
    jm._registry_cache[JOB] = copy.deepcopy(_entries("PASS"))
    with pytest.raises(jm.RegistryIOError, match="write not confirmed"):
        asyncio.run(jm.registry_update(Kernel(lose_output=True), NB_A, "/m/a.ipynb", JOB, "FAIL"))
    assert "Saved" not in capsys.readouterr().out


def test_failed_read_raises_caches_nothing_and_never_overwrites(registry_file):
    registry_file.write_text(json.dumps(_entries("PASS")))

    with pytest.raises(jm.RegistryIOError, match="failed to read registry"):
        asyncio.run(jm.registry_update(Kernel(fail_reads=True), NB_A, "/m/a.ipynb", JOB, "FAIL"))

    assert JOB not in jm._registry_cache
    assert _statuses(registry_file) == {NB_A: "PASS", NB_B: "PASS"}
    # The next read is a fresh attempt, not a cached empty registry.
    asyncio.run(jm.registry_update(Kernel(), NB_A, "/m/a.ipynb", JOB, "FAIL"))
    assert _statuses(registry_file) == {NB_A: "FAIL", NB_B: "PASS"}


def test_missing_registry_reads_as_empty(registry_file):
    assert asyncio.run(jm.load_registry(Kernel(), JOB)) == {}


def test_unreadable_peer_registry_is_skipped_by_lookups(registry_file, monkeypatch, capsys):
    registry_file.write_text(json.dumps(_entries("PASS")))
    peer = registry_file.parent.parent / "peer_job" / "migration_registry.json"
    peer.parent.mkdir(parents=True)
    peer.write_text("{}")
    kernel = Kernel()
    execute = kernel.execute

    async def peer_times_out(code, **kwargs):
        if "json.load(f)" in code and str(peer) in code:
            raise asyncio.TimeoutError()
        return await execute(code, **kwargs)

    monkeypatch.setattr(kernel, "execute", peer_times_out)
    monkeypatch.setattr(jm, "_all_registries_loaded", False)
    merged = asyncio.run(jm._load_all_registries(kernel))
    assert merged[NB_A]["status"] == "PASS"
    assert "peer_job" in capsys.readouterr().out


@pytest.mark.parametrize("prior,marked", [
    ("PASS", True), ("PASS_DATA_SUBSTITUTED", True), ("PARTIAL", True),
    ("CONVERTED_NOT_VALIDATED", True), ("FAIL", False), (None, False)])
def test_a_reusable_entry_is_marked_in_progress_before_its_copy_is_replaced(
        registry_file, prior, marked):
    if prior:
        registry_file.write_text(json.dumps({NB_A: {"migrated_path": "/m/a.ipynb", "status": prior}}))
    asyncio.run(jm._invalidate_reusable_entry(Kernel(), NB_A, "/m/a.ipynb", JOB))
    on_disk = json.loads(registry_file.read_text()) if registry_file.exists() else {}
    assert on_disk.get(NB_A, {}).get("status") == ("IN_PROGRESS" if marked else prior)
    assert not jm.resume_may_reuse("IN_PROGRESS", dependency=True)


# ── process_notebook: a task whose final status cannot be saved ─────────────

class _ArtifactKernel:
    """Acknowledges artifact uploads; registry I/O goes through a patched store."""
    cluster_id = "test-cluster"

    async def execute(self, code, **kwargs):
        if "_task_expected =" in code:
            return {"status": "ok", "outputs": [{"type": "stream", "text": "__AIDP_ISO_OK__\n"}]}
        return {"status": "ok", "outputs": []}


def _store(monkeypatch, initial, fail_on_save=None):
    """In-memory registry; ``fail_on_save`` = which save call (1-based) fails."""
    store = copy.deepcopy(initial)
    saves = []

    async def load(session, job_name):
        return jm._registry_cache.setdefault(job_name, copy.deepcopy(store))

    async def save(session, job_name, registry):
        saves.append({k: v["status"] for k, v in registry.items()})
        if fail_on_save == len(saves):
            raise jm.RegistryIOError("registry not saved to <fixture>: synthetic I/O error")
        store.clear()
        store.update(copy.deepcopy(registry))
        jm._registry_cache[job_name] = registry  # cached only once saved

    monkeypatch.setattr(jm, "_registry_cache", {})
    monkeypatch.setattr(jm, "load_registry", load)
    monkeypatch.setattr(jm, "save_registry", save)
    return store, saves


@pytest.fixture
def notebook_run(monkeypatch, tmp_path):
    import agent_migrate as am
    import cell_analyzer

    async def analyze(cells, *args, **kwargs):
        return [{"cell_index": i, "action": "execute", "description": "plain Python",
                 "risks": [], "changes_needed": [], "storage_paths": [],
                 "table_refs": [], "fuse_risks": []} for i in range(len(cells))]

    async def noop(*args, **kwargs):
        return None

    async def verified(*args, **kwargs):
        return True

    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    monkeypatch.setattr(jm, "load_catalog_snapshot", lambda: "")
    monkeypatch.setattr(jm, "get_bucket_mapping_context", lambda: "")
    monkeypatch.setattr(jm, "_inject_write_guard", lambda code: code)
    monkeypatch.setattr(jm, "verify_upload", verified)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", False)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/persistence_fixture")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/persistence_fixture/notebooks")
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))
    monkeypatch.setattr(jm.shutil, "rmtree", lambda *args, **kwargs: None)
    monkeypatch.setattr(jm.asyncio, "sleep", noop)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(am, "_cell_history", [])
    monkeypatch.setattr(jm, "_cell_history", am._cell_history)
    monkeypatch.setattr(am, "_fixup_persistence_failures", [])
    # A code-only conversion that fails: the notebook is still uploaded.
    notebook = {"cells": [{"cell_type": "code", "source": ['%run "unterminated'],
                           "outputs": [], "metadata": {}}]}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    path = "/Workspace/persistence_fixture/task.ipynb"

    def run():
        return asyncio.run(jm.process_notebook(path, _ArtifactKernel(), JOB, "task", {},
                                               run_all=False))
    return path, run


def test_failed_task_whose_status_cannot_be_saved_is_not_resumable(monkeypatch, notebook_run):
    path, run = notebook_run
    key = jm.registry_key(path)
    store, saves = _store(monkeypatch, {key: {"migrated_path": "x", "status": "PASS"}},
                          fail_on_save=2)

    result = run()

    assert result["status"] == "FAIL"
    assert result["kind"] == "REGISTRY_PERSISTENCE"
    assert saves == [{key: "IN_PROGRESS"}, {key: "FAIL"}]
    assert store[key]["status"] == "IN_PROGRESS"
    assert not jm.resume_may_reuse(store[key]["status"], dependency=True)


def test_unwritable_registry_withholds_the_replacement_notebook(monkeypatch, notebook_run):
    path, run = notebook_run
    key = jm.registry_key(path)
    store, saves = _store(monkeypatch, {key: {"migrated_path": "x", "status": "PASS"}},
                          fail_on_save=1)
    uploads = []
    execute = _ArtifactKernel.execute

    async def recording(self, code, **kwargs):
        if "notebooks/task.ipynb" in code and "b64decode" in code:
            uploads.append(code)
        return await execute(self, code, **kwargs)

    monkeypatch.setattr(_ArtifactKernel, "execute", recording)
    result = run()

    assert result["status"] == "FAIL"
    assert result["kind"] == "REGISTRY_PERSISTENCE"
    assert uploads == []  # the old copy, still described by its entry, stays
    assert store[key]["status"] == "PASS"


def test_saved_status_is_recorded_as_before(monkeypatch, notebook_run):
    path, run = notebook_run
    key = jm.registry_key(path)
    store, _ = _store(monkeypatch, {})
    result = run()
    assert result["status"] == "FAIL"
    assert result.get("kind") != "REGISTRY_PERSISTENCE"
    assert store[key]["status"] == "FAIL"


# ── ensure_migrated: a dependency whose final status cannot be saved ─────────

def test_dependency_whose_status_cannot_be_saved_is_converted_again_on_resume(
        monkeypatch, tmp_path):
    dep = "/Workspace/Users/x/dep.ipynb"
    key = jm.registry_key(dep)
    store, saves = _store(monkeypatch, {key: {"migrated_path": "x", "status": "PASS"}},
                          fail_on_save=2)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path / JOB / "notebooks"))
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "download_notebook", lambda path: json.dumps({"cells": [
        {"cell_type": "code", "source": ["x = 1\n"]}]}).encode())
    outcomes = iter([{"status": "FAIL"}, {"status": "CONVERTED_NOT_VALIDATED"}])
    converted = []

    async def convert(path, *args, **kwargs):
        converted.append(path)
        return next(outcomes)

    class Session:
        cluster_id = "test-cluster"

        async def execute(self, code, **kwargs):
            return {"status": "ok", "outputs": [{"type": "stream", "text": "VALID\n"}]}

    monkeypatch.setattr(jm, "process_notebook", convert)
    deps = {"Users/x/dep.ipynb": []}

    # A fresh run replaces the copy; its FAIL cannot be saved.
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    with pytest.raises(jm.RegistryIOError):
        asyncio.run(jm.ensure_migrated(dep, Session(), job_name=JOB, manifest_dependency_map=deps))
    assert store[key]["status"] == "IN_PROGRESS"

    # A resume over that copy converts it again instead of trusting the old PASS.
    monkeypatch.setattr(jm, "SKIP_MIGRATED", True)
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "_registry_cache", {})
    asyncio.run(jm.ensure_migrated(dep, Session(), job_name=JOB, manifest_dependency_map=deps))
    assert converted == ["Users/x/dep.ipynb", "Users/x/dep.ipynb"]
    assert store[key]["status"] == "CONVERTED_NOT_VALIDATED"


# ── The cache holds only confirmed state; one registry save at a time ───────
# A failed IN_PROGRESS save used to leave the cached entry IN_PROGRESS while the
# disk kept PASS, so a second task skipped the write and replaced the copy.
# Concurrent saves (parallel dependency conversions) interleave their chunks,
# so an older snapshot could be replaced over a newer one.

class SerializedKernel(Kernel):
    """One kernel call at a time, yielding between calls like the live session."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lock = asyncio.Lock()

    async def execute(self, code, **kwargs):
        async with self.lock:
            await asyncio.sleep(0)
            return await super().execute(code, **kwargs)


def test_failed_save_leaves_the_cache_at_the_confirmed_state(registry_file):
    registry_file.write_text(json.dumps({NB_A: {"migrated_path": "/m/a.ipynb", "status": "PASS"}}))
    failing = Kernel(fail_writes=True)

    with pytest.raises(jm.RegistryIOError):
        asyncio.run(jm._invalidate_reusable_entry(failing, NB_A, "/m/a.ipynb", JOB))
    assert jm._registry_cache[JOB][NB_A]["status"] == "PASS"

    # A second task replacing the same copy must still try (and here fail) to
    # stop the PASS vouching for it, instead of trusting an unsaved IN_PROGRESS.
    with pytest.raises(jm.RegistryIOError):
        asyncio.run(jm._invalidate_reusable_entry(failing, NB_A, "/m/a.ipynb", JOB))
    assert _statuses(registry_file) == {NB_A: "PASS"}


def _boundary_registry():
    """A registry whose save takes one more chunk with a long status than a short one."""
    import base64

    def chunks(registry):
        return -(-len(base64.b64encode(json.dumps(registry, indent=1).encode())) // 45000)

    def with_status(base, status):
        return {**base, NB_A: {"migrated_path": "/m/a.ipynb", "job_name": JOB,
                               "status": status, "timestamp": "2026-10-15T00:00:00.000000"}}

    for pad in range(30000, 40000):
        base = {NB_B: {"migrated_path": "p" * pad, "status": "PASS"}}
        if chunks(with_status(base, "CONVERTED_NOT_VALIDATED")) > chunks(with_status(base, "FAIL")):
            return base
    raise AssertionError("no chunk boundary found")


def test_concurrent_saves_leave_the_newest_status_on_disk(registry_file):
    base = _boundary_registry()
    registry_file.write_text(json.dumps(base))
    kernel = SerializedKernel()

    async def both():
        await asyncio.gather(
            jm.registry_update(kernel, NB_A, "/m/a.ipynb", JOB, "CONVERTED_NOT_VALIDATED"),
            jm.registry_update(kernel, NB_A, "/m/a.ipynb", JOB, "FAIL"))

    asyncio.run(both())
    assert _statuses(registry_file)[NB_A] == "FAIL"
    assert jm._registry_cache[JOB][NB_A]["status"] == "FAIL"


def test_concurrent_saves_keep_every_entry(registry_file):
    kernel = SerializedKernel()
    paths = [f"/Workspace/Users/x/n{i}.ipynb" for i in range(6)]

    async def all_of_them():
        await asyncio.gather(*(jm.registry_update(kernel, p, "/m/x.ipynb", JOB, "PASS")
                               for p in paths))

    asyncio.run(all_of_them())
    assert sorted(_statuses(registry_file)) == sorted(paths)


# ── The read locates its payload by marker ──────────────────────────────────
# Live run c06683a3 (2026-10-15): during parallel dependency conversions the
# registry read's output did not parse ("Expecting value: line 1 column 1"),
# while the batch check next to it came back duplicated. AIDP can duplicate or
# interleave a cell's stdout, so the read prints a marker before the JSON.

class NoisyKernel(Kernel):
    def __init__(self, noise, **kwargs):
        super().__init__(**kwargs)
        self.noise = noise

    async def execute(self, code, **kwargs):
        result = await super().execute(code, **kwargs)
        if "json.load(f)" in code:
            text = "".join(o.get("text", "") for o in result["outputs"])
            result = {"status": "ok", "outputs": [{"type": "stream", "text": self.noise(text)}]}
        return result


@pytest.mark.parametrize("noise", [
    lambda text: "[]stray kernel line\n" + text,
    lambda text: text + text,
    lambda text: "MISSING\n" + text + text,
], ids=["junk_before", "duplicated", "both"])
def test_registry_read_tolerates_interleaved_and_duplicated_output(registry_file, noise):
    registry_file.write_text(json.dumps(_entries("PASS")))
    assert asyncio.run(jm.load_registry(NoisyKernel(noise), JOB)) == _entries("PASS")


@pytest.mark.parametrize("noise", [lambda text: "", lambda text: "MISSING\n"],
                         ids=["no_output", "unrelated_output"])
def test_registry_read_without_its_marker_raises_with_what_came_back(registry_file, noise):
    registry_file.write_text(json.dumps(_entries("PASS")))
    with pytest.raises(jm.RegistryIOError, match="failed to read registry") as caught:
        asyncio.run(jm.load_registry(NoisyKernel(noise), JOB))
    assert "output:" in str(caught.value)
    assert JOB not in jm._registry_cache


# ── A corrupt registry is either moved aside or left untouched ──────────────
# The kernel quarantines an unparseable registry by renaming it. When the
# rename failed, the read still returned an empty registry (logging it as
# "quarantined"), and the next save replaced the surviving file.

CORRUPT = '{"' + NB_A + '": {"migrated_path": "/m/a.ipynb", "status": "PASS"}, "truncat'


def test_corrupt_registry_whose_quarantine_fails_is_never_overwritten(registry_file, monkeypatch):
    registry_file.write_text(CORRUPT)

    def refuse(*args, **kwargs):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(os, "rename", refuse)  # the kernel-side quarantine rename
    with pytest.raises(jm.RegistryIOError, match="could not be quarantined"):
        asyncio.run(jm.load_registry(Kernel(), JOB))
    assert JOB not in jm._registry_cache
    with pytest.raises(jm.RegistryIOError):
        asyncio.run(jm.registry_update(Kernel(), NB_B, "/m/b.ipynb", JOB, "FAIL"))
    assert registry_file.read_text() == CORRUPT


def test_quarantined_corrupt_registry_is_kept_aside_and_a_fresh_one_starts(registry_file):
    registry_file.write_text(CORRUPT)

    assert asyncio.run(jm.load_registry(Kernel(), JOB)) == {}
    asyncio.run(jm.registry_update(Kernel(), NB_B, "/m/b.ipynb", JOB, "FAIL"))

    kept = [p for p in registry_file.parent.iterdir() if ".corrupt." in p.name]
    assert len(kept) == 1 and kept[0].read_text() == CORRUPT
    assert _statuses(registry_file) == {NB_B: "FAIL"}
