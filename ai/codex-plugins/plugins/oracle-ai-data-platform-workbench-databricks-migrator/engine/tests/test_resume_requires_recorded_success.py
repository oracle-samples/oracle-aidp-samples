"""A --skip-migrated resume reuses a migrated notebook only over a recorded success.

Non-strict runs still upload a failed task's notebook (its registry entry then
records FAIL / PARTIAL). The resume path treated "the file exists" as "already
migrated": the task came back ALREADY_MIGRATED, its registry entry was rewritten
to PASS, and the workflow runner exited zero, so Phase 2 went green and the
final clone was built over the failed notebook. Dependencies were reused the same
way, and a failed dependency conversion never replaced an older success entry.
"""
import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm  # noqa: E402


TASK = "/Workspace/Users/test/task.ipynb"
DEP = "/Workspace/Users/test/dep.ipynb"
SUB_OK = "/Workspace/Users/test/sub_ok.ipynb"
SUB_FAILED = "/Workspace/Users/test/sub_failed.ipynb"
JOB = "resume_verdict"
SOURCE = json.dumps({"cells": [{"cell_type": "code", "source": ["x = 1\n"]}]}).encode()


class Session:
    """Every migrated copy exists on disk; only the registry verdict differs."""
    cluster_id = "test-cluster"

    def __init__(self, fs_answer="VALID:3"):
        self.fs_answer = fs_answer

    async def execute(self, code, **kwargs):
        text = self.fs_answer if "VALID:" in code else "VALID"
        return {"status": "ok", "outputs": [{"type": "stream", "text": text + "\n"}]}

    async def run_stateless(self, code, **kwargs):
        paths = [line.split("'")[1] for line in code.splitlines()
                 if line.strip().startswith("p = '")]
        return {"status": "ok", "outputs": [
            {"type": "stream", "text": json.dumps({p: "VALID" for p in paths})}]}


def _setup(monkeypatch, tmp_path, registry, code_only=False):
    async def noop(*args, **kwargs):
        return None

    async def no_backfill(*args, **kwargs):
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    async def load_registry(session, job_name):
        assert job_name == JOB
        return registry

    writes = []

    async def write_entry(session, original_path, migrated_path, job_name, status):
        writes.append((jm.registry_key(original_path), status))
        registry[jm.registry_key(original_path)] = {
            "migrated_path": migrated_path, "status": status}

    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "migrated"))
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path / "migrated" / JOB / "notebooks"))
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "SKIP_MIGRATED", True)
    monkeypatch.setattr(jm, "CODE_ONLY", code_only)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "START_TASK", "")
    monkeypatch.setattr(jm, "ONLY_TASKS", [])
    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: None)
    monkeypatch.setattr(jm, "_bump_cluster_idle_timeout", lambda cluster_id: None)
    monkeypatch.setattr(jm, "_revert_all_idle_timeouts", lambda: [])
    monkeypatch.setattr(jm, "stage_export_resources", noop)
    monkeypatch.setattr(jm, "_ensure_redirect_schema", noop)
    monkeypatch.setattr(jm, "backfill_missing_deps", no_backfill)
    monkeypatch.setattr(jm, "load_registry", load_registry)
    # Every registry write (status or IN_PROGRESS) goes through this one function.
    monkeypatch.setattr(jm, "_registry_update_locked", write_entry)
    monkeypatch.setattr(jm, "download_notebook", lambda path: SOURCE)
    return writes


def _entry(status):
    return {} if status is None else {jm.registry_key(TASK): {
        "migrated_path": f"{jm.MIGRATED_BASE}/Users/test/task.ipynb", "status": status}}


def _task_job():
    return {"job_name": JOB, "default_cluster": Session.cluster_id,
            "execution_layers": [["t"]],
            "tasks": [{"task_key": "t", "notebook_path": TASK, "depends_on": [],
                       "run_deps": []}]}


# (code_only, last recorded status, reused). Execution runs reuse only a
# validated verdict; code-only runs also reuse their own unexecuted conversion.
RESUME_GRID = [
    (False, "PASS", True),
    (False, "PASS_CONDITIONAL_EXIT_COVERAGE", True),
    (False, "PASS_DATA_SUBSTITUTED", True),
    (False, "FAIL", False),
    (False, "PARTIAL", False),
    (False, "PARTIAL_STATE_LOST", False),
    (False, "CONVERTED_NOT_VALIDATED", False),
    (False, None, False),
    (True, "CONVERTED_NOT_VALIDATED", True),
    (True, "PASS", True),
    (True, "FAIL", False),
    (True, None, False),
]


@pytest.mark.parametrize("resume_source", ["filesystem", "registry"])
@pytest.mark.parametrize("code_only,prior_status,reused", RESUME_GRID)
def test_task_resume_reuses_only_a_recorded_success(
        monkeypatch, tmp_path, resume_source, code_only, prior_status, reused):
    registry = {}
    writes = _setup(monkeypatch, tmp_path, registry, code_only=code_only)
    registry.update(_entry(prior_status))
    migrated = []

    async def migrate(path, *args, **kwargs):
        migrated.append(path)
        return {"path": path, "status": "PASS", "ok": 1, "failed": 0}

    async def lookup(session, path):
        assert path == TASK
        return f"{jm.MIGRATED_BASE}/Users/test/task.ipynb"

    monkeypatch.setattr(jm, "process_notebook", migrate)
    monkeypatch.setattr(jm, "registry_lookup", lookup)
    session = Session("VALID:3" if resume_source == "filesystem" else "MISSING")

    result = asyncio.run(jm.process_job(_task_job(), session,
                                        skip_task_dependency_scan=True))

    status = result["task_results"]["t"]["status"]
    if reused:
        assert status == "ALREADY_MIGRATED"
        assert migrated == []
        # The verdict that allowed the reuse stays as recorded.
        assert writes == []
        assert registry[jm.registry_key(TASK)]["status"] == prior_status
    else:
        assert status == "PASS"
        assert migrated == [TASK]


def test_resumed_failure_is_not_rewritten_to_pass(monkeypatch, tmp_path):
    registry = {}
    _setup(monkeypatch, tmp_path, registry)
    registry.update(_entry("FAIL"))

    async def still_failing(path, *args, **kwargs):
        return {"path": path, "status": "FAIL", "ok": 0, "failed": 1}

    monkeypatch.setattr(jm, "process_notebook", still_failing)
    result = asyncio.run(jm.process_job(_task_job(), Session(),
                                        skip_task_dependency_scan=True))

    assert result["task_results"]["t"]["status"] == "FAIL"
    assert registry[jm.registry_key(TASK)]["status"] == "FAIL"


@pytest.mark.parametrize("prior_status,reused", [
    ("PASS", True), ("PARTIAL", True), ("CONVERTED_NOT_VALIDATED", True),
    ("PASS_DATA_SUBSTITUTED", True),
    ("FAIL", False), ("RAY_CONVERSION_UNSUPPORTED", False), (None, False),
])
@pytest.mark.parametrize("manifest_mode", [True, False])
def test_dependency_resume_reuses_only_an_accepted_conversion(
        monkeypatch, tmp_path, prior_status, reused, manifest_mode):
    registry = {}
    _setup(monkeypatch, tmp_path, registry)
    if prior_status:
        registry[jm.registry_key(DEP)] = {
            "migrated_path": f"{jm.MIGRATED_BASE}/Users/test/dep.ipynb",
            "status": prior_status}
    converted = []

    async def convert(path, *args, **kwargs):
        converted.append(path)
        return {"status": "CONVERTED_NOT_VALIDATED"}

    async def lookup(session, path):
        return f"{jm.MIGRATED_BASE}/Users/test/dep.ipynb"

    monkeypatch.setattr(jm, "process_notebook", convert)
    monkeypatch.setattr(jm, "registry_lookup", lookup)
    monkeypatch.setattr(jm, "scan_for_run_deps", lambda content: [])
    kwargs = ({"manifest_dependency_map": {"Users/test/dep.ipynb": []}}
              if manifest_mode else {})

    migrated_path = asyncio.run(jm.ensure_migrated(DEP, Session(), job_name=JOB, **kwargs))

    assert migrated_path.endswith("Users/test/dep.ipynb")
    assert converted == ([] if reused else ["Users/test/dep.ipynb"])


def test_batched_sub_dependencies_reuse_only_accepted_conversions(monkeypatch, tmp_path):
    registry = {}
    _setup(monkeypatch, tmp_path, registry)
    registry[jm.registry_key(SUB_OK)] = {"migrated_path": "x", "status": "PASS"}
    registry[jm.registry_key(SUB_FAILED)] = {"migrated_path": "y", "status": "FAIL"}
    converted = []

    async def convert(path, *args, **kwargs):
        converted.append(path)
        return {"status": "CONVERTED_NOT_VALIDATED"}

    monkeypatch.setattr(jm, "process_notebook", convert)
    dependency_map = {
        "Users/test/dep.ipynb": [
            {"path": SUB_OK, "nested_deps": []},
            {"path": SUB_FAILED, "nested_deps": []},
        ],
        "Users/test/sub_ok.ipynb": [],
        "Users/test/sub_failed.ipynb": [],
    }

    asyncio.run(jm.ensure_migrated(DEP, Session(), job_name=JOB,
                                   manifest_dependency_map=dependency_map))

    assert converted == ["Users/test/sub_failed.ipynb", "Users/test/dep.ipynb"]


def test_failed_dependency_conversion_replaces_an_older_success(monkeypatch, tmp_path):
    registry = {}
    _setup(monkeypatch, tmp_path, registry)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    registry[jm.registry_key(DEP)] = {"migrated_path": "x", "status": "PASS"}

    async def convert(path, *args, **kwargs):
        return {"status": "FAIL"}

    monkeypatch.setattr(jm, "process_notebook", convert)
    migrated_path = asyncio.run(jm.ensure_migrated(
        DEP, Session("MISSING"), job_name=JOB,
        manifest_dependency_map={"Users/test/dep.ipynb": []}))

    assert migrated_path is None
    assert registry[jm.registry_key(DEP)]["status"] == "FAIL"


def test_default_runs_never_reuse_a_recorded_copy(monkeypatch, tmp_path):
    registry = {}
    writes = _setup(monkeypatch, tmp_path, registry)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    registry[jm.registry_key(DEP)] = {"migrated_path": "x", "status": "PASS"}
    converted = []

    async def convert(path, *args, **kwargs):
        converted.append(path)
        return {"status": "CONVERTED_NOT_VALIDATED"}

    monkeypatch.setattr(jm, "process_notebook", convert)
    asyncio.run(jm.ensure_migrated(DEP, Session(), job_name=JOB,
                                   manifest_dependency_map={"Users/test/dep.ipynb": []}))

    # --no-skip-migrated converts again; the registry is read only to stop the
    # old entry vouching for the copy this run replaces.
    assert converted == ["Users/test/dep.ipynb"]
    assert writes == [(jm.registry_key(DEP), "IN_PROGRESS"),
                      (jm.registry_key(DEP), "CONVERTED_NOT_VALIDATED")]


# ── Export layout: the resume reads the key the run wrote ───────────────────
# In an exported job the manifest's resolved_path is workspace-relative under
# the export base, and process_notebook records the task under that form;
# ensure_migrated records a dependency under its collapsed /Workspace/Users form.

EXPORT_BASE = "export/notebooks"
EXPORTED_TASK = f"/Workspace/{EXPORT_BASE}/Users/test/task.ipynb"
EXPORTED_DEP = f"/Workspace/{EXPORT_BASE}/Users/test/dep.ipynb"


@pytest.mark.parametrize("prior_status,reused", [("PASS", True), ("PARTIAL", False)])
def test_exported_task_resume_reads_the_status_its_run_recorded(
        monkeypatch, tmp_path, prior_status, reused):
    registry = {}
    _setup(monkeypatch, tmp_path, registry)
    monkeypatch.setattr(jm, "EXPORT_BASE", EXPORT_BASE)
    resolved = EXPORTED_TASK.removeprefix("/Workspace/")
    # The key process_notebook(nb_path=resolved_path) writes after a run.
    registry[jm.registry_key(resolved)] = {"migrated_path": "x", "status": prior_status}
    migrated = []

    async def migrate(path, *args, **kwargs):
        migrated.append(path)
        return {"path": path, "status": "PARTIAL", "ok": 1, "failed": 0}

    monkeypatch.setattr(jm, "process_notebook", migrate)
    job = _task_job()
    job["tasks"][0].update(notebook_path=EXPORTED_TASK, resolved_path=resolved)

    result = asyncio.run(jm.process_job(job, Session(), skip_task_dependency_scan=True))

    assert result["task_results"]["t"]["status"] == (
        "ALREADY_MIGRATED" if reused else "PARTIAL")
    assert migrated == ([] if reused else [resolved])


def test_exported_dependency_failure_is_migrated_again_on_resume(monkeypatch, tmp_path):
    registry = {}
    _setup(monkeypatch, tmp_path, registry)
    monkeypatch.setattr(jm, "EXPORT_BASE", EXPORT_BASE)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    registry[jm.registry_key(EXPORTED_DEP)] = {"migrated_path": "x", "status": "PASS"}
    outcomes = iter([{"status": "FAIL"}, {"status": "CONVERTED_NOT_VALIDATED"}])
    converted = []

    async def convert(path, *args, **kwargs):
        converted.append(path)
        return next(outcomes)

    monkeypatch.setattr(jm, "process_notebook", convert)
    dependency_map = {"Users/test/dep.ipynb": []}

    # A fresh run whose conversion fails, then a resume over its output.
    assert asyncio.run(jm.ensure_migrated(
        EXPORTED_DEP, Session(), job_name=JOB,
        manifest_dependency_map=dependency_map)) is None
    monkeypatch.setattr(jm, "SKIP_MIGRATED", True)
    monkeypatch.setattr(jm, "_migration_cache", {})
    resumed = asyncio.run(jm.ensure_migrated(
        EXPORTED_DEP, Session(), job_name=JOB,
        manifest_dependency_map=dependency_map))

    assert resumed.endswith("Users/test/dep.ipynb")
    assert converted == [f"{EXPORT_BASE}/Users/test/dep.ipynb"] * 2
