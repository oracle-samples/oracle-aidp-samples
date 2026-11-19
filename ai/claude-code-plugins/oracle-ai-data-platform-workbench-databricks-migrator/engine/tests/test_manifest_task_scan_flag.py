"""Workflow manifest mode and explicit legacy scans at the job boundary."""

import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm  # noqa: E402
import build_dag_from_workflow as workflow_dag  # noqa: E402


PARENT = "/Workspace/Users/test/parent.ipynb"
CHILD = "/Workspace/Users/test/child.ipynb"
GRANDCHILD = "/Workspace/Users/test/grandchild.ipynb"


class Session:
    cluster_id = "test-cluster"

    async def execute(self, code, **kwargs):
        return {"status": "ok", "outputs": [{"type": "stream", "text": "MISSING\n"}]}

    async def run_stateless(self, code, **kwargs):
        return {"status": "ok", "outputs": []}


def _job(run_deps):
    task = {"task_key": "parent", "notebook_path": PARENT,
            "depends_on": []}
    if run_deps is not None:
        task["run_deps"] = run_deps
    return {"job_name": "manifest_scan_flag", "tasks": [task],
            "execution_layers": [["parent"]],
            "default_cluster": Session.cluster_id}


def _setup(monkeypatch, tmp_path):
    async def noop(*args, **kwargs):
        return None

    async def no_backfill(*args, **kwargs):
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "migrated"))
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    monkeypatch.setattr(jm, "CODE_ONLY", True)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "START_TASK", "")
    monkeypatch.setattr(jm, "ONLY_TASKS", [])
    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: None)
    monkeypatch.setattr(jm, "_bump_cluster_idle_timeout", lambda cluster_id: None)
    monkeypatch.setattr(jm, "_revert_all_idle_timeouts", lambda: [])
    monkeypatch.setattr(jm, "stage_export_resources", noop)
    monkeypatch.setattr(jm, "backfill_missing_deps", no_backfill)


@pytest.mark.parametrize("legacy_scan", [False, True])
@pytest.mark.parametrize("strict_gating", [False, True])
@pytest.mark.parametrize("empty_job", [False, True])
def test_dependency_mode_preserves_paths_and_scan_contract(
        monkeypatch, tmp_path, legacy_scan, strict_gating, empty_job):
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict_gating)
    scans = []
    backfill_calls = []
    prepared = []
    preparation_options = []
    task_maps = []
    received_params = []

    def download(path):
        scans.append(path)
        return b"parent notebook"

    def scan(content):
        assert content == b"parent notebook"
        return [(CHILD, {})]

    async def prepare(path, *args, **kwargs):
        prepared.append(path)
        preparation_options.append(kwargs)
        return f"{jm.MIGRATE_OUTPUT}/manifest_scan_flag/notebooks/{path.removeprefix('/Workspace/')}"

    async def migrate(path, *args, **kwargs):
        assert (jm._manifest_dependency_paths.get() is not None) is (not legacy_scan)
        task_maps.append(dict(kwargs["dep_path_map"]))
        received_params.append(dict(args[3]))
        return {"path": path, "status": "PASS", "ok": 1, "failed": 0}

    async def backfill(*args, **kwargs):
        backfill_calls.append((args, kwargs))
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    monkeypatch.setattr(jm, "download_notebook", download)
    monkeypatch.setattr(jm, "scan_for_run_deps", scan)
    monkeypatch.setattr(jm, "ensure_migrated", prepare)
    monkeypatch.setattr(jm, "process_notebook", migrate)
    monkeypatch.setattr(jm, "backfill_missing_deps", backfill)

    job = _job([{
        "path": CHILD, "exists": True, "nested_deps": [
            {"path": GRANDCHILD, "exists": True, "nested_deps": []},
        ],
    }])
    job["parameters"] = {"notebook_path": "" if empty_job else GRANDCHILD}
    if empty_job:
        job["tasks"][0]["base_parameters"] = {"notebook_path": CHILD}
    result = asyncio.run(jm.process_job(
        job, Session(), skip_task_dependency_scan=not legacy_scan,
    ))

    assert jm._manifest_dependency_paths.get() is None
    assert result["task_results"]["parent"]["status"] == "PASS"
    assert scans == ([PARENT] if legacy_scan else [])
    assert len(backfill_calls) == (1 if legacy_scan else 0)
    if backfill_calls:
        assert backfill_calls[0][0][1] == "manifest_scan_flag"
        assert backfill_calls[0][1] == {}
    assert prepared == ([CHILD, GRANDCHILD] if legacy_scan else [GRANDCHILD, CHILD])
    assert all(("manifest_dependency_map" in options) != legacy_scan
               for options in preparation_options)
    assert len(task_maps) == 1
    for path in (CHILD, GRANDCHILD):
        assert task_maps[0][path].endswith(path.removeprefix("/Workspace/"))
    assert received_params[0]["notebook_path"].endswith(
        "manifest_scan_flag/notebooks/Users/test/"
        + ("child.ipynb" if empty_job else "grandchild.ipynb")
    )


def test_task_scan_flag_rejects_missing_run_deps(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)

    def unexpected_download(path):
        pytest.fail("task source must not be scanned when the flag is set")

    monkeypatch.setattr(jm, "download_notebook", unexpected_download)
    with pytest.raises(ValueError, match="has no valid run_deps list"):
        asyncio.run(jm.process_job(
            _job(None), Session(), skip_task_dependency_scan=True,
        ))


def test_task_scan_flag_rejects_malformed_nested_edge(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)
    with pytest.raises(ValueError, match="run_deps entry without a path"):
        asyncio.run(jm.process_job(_job([{
            "path": CHILD, "exists": True, "nested_deps": [{"exists": True}],
        }]), Session(), skip_task_dependency_scan=True))


def test_task_scan_flag_rejects_missing_nested_list(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)
    with pytest.raises(ValueError, match="no valid run_deps list"):
        asyncio.run(jm.process_job(_job([{
            "path": CHILD, "exists": True,
        }]), Session(), skip_task_dependency_scan=True))


def test_manifest_mode_prepares_task_child_without_scanning_it(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path / "migrated" / "job" / "notebooks"))
    job = {"tasks": [
        {"task_key": "parent", "notebook_path": PARENT, "run_deps": [
            {"path": CHILD, "location": "task", "exists": True, "nested_deps": []},
        ]},
        {"task_key": "child", "notebook_path": CHILD, "run_deps": [
            {"path": GRANDCHILD, "location": "external", "exists": True,
             "nested_deps": []},
        ]},
    ]}
    child_map = jm._manifest_dependency_map(job)
    prepared = []
    child_maps = {}

    def forbidden_scan(content):
        pytest.fail("ensure_migrated must not scan children in manifest mode")

    async def convert(path, *args, **kwargs):
        prepared.append(path)
        child_maps[path] = dict(kwargs["dep_path_map"])
        return {"status": "CONVERTED_NOT_VALIDATED"}

    async def registry_update(*args, **kwargs):
        return None

    source = json.dumps({"cells": [{"cell_type": "code", "source": ["print(1)\n"]}]}).encode()
    monkeypatch.setattr(jm, "scan_for_run_deps", forbidden_scan)
    monkeypatch.setattr(jm, "download_notebook", lambda path: source)
    monkeypatch.setattr(jm, "process_notebook", convert)
    monkeypatch.setattr(jm, "registry_update", registry_update)

    result = asyncio.run(jm.ensure_migrated(
        CHILD, Session(), manifest_dependency_map=child_map,
    ))
    assert result.endswith("Users/test/child.ipynb")
    assert [Path(path).name for path in prepared] == ["grandchild.ipynb", "child.ipynb"]
    assert child_maps[prepared[-1]][GRANDCHILD].endswith("Users/test/grandchild.ipynb")


@pytest.mark.parametrize("batch", [False, True])
def test_manifest_nested_sanitized_source_alias_reaches_child_rewrite(
        monkeypatch, tmp_path, batch):
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path / "migrated" / "notebooks"))
    exported = "/Workspace/Users/test/Job_1.ipynb"
    original = "/Workspace/Users/test/Job-1.py"
    child_map = {
        "Users/test/child.ipynb": [{
            "path": exported, "original_path": original,
            "exists": True, "nested_deps": [],
        }],
        "Users/test/Job_1.ipynb": [],
    }
    if batch:
        child_map["Users/test/child.ipynb"].append({
            "path": GRANDCHILD, "original_path": GRANDCHILD,
            "exists": True, "nested_deps": [],
        })
        child_map["Users/test/grandchild.ipynb"] = []
    source = json.dumps({"cells": [{"cell_type": "code", "source": ["print(1)\n"]}]}).encode()
    monkeypatch.setattr(jm, "download_notebook", lambda path: source)
    seen = {}

    async def convert(path, *args, **kwargs):
        seen[path] = dict(kwargs["dep_path_map"])
        return {"status": "CONVERTED_NOT_VALIDATED"}

    async def registry_update(*args, **kwargs):
        return None

    monkeypatch.setattr(jm, "process_notebook", convert)
    monkeypatch.setattr(jm, "registry_update", registry_update)
    asyncio.run(jm.ensure_migrated(
        CHILD, Session(), manifest_dependency_map=child_map,
    ))
    assert seen["Users/test/child.ipynb"][original].endswith("Users/test/Job_1.ipynb")


@pytest.mark.parametrize("shared_task_notebook", [False, True])
def test_manifest_mode_prepares_binding_specific_children_of_shared_driver(
        monkeypatch, tmp_path, shared_task_notebook):
    _setup(monkeypatch, tmp_path)
    base = "/Workspace/export/notebooks"
    driver = f"{base}/Users/test/driver.ipynb"
    tasks = [
        {"task_key": key,
         "notebook_path": f"{base}/Users/test/{'shared' if shared_task_notebook else key}.ipynb",
         "depends_on": [], "parameters": [{"name": "variant", "value": variant}]}
        for key, variant in (("A", "X"), ("B", "Y"))
    ]

    async def scan(session, path, parameter_bindings=None, **kwargs):
        if path in {task["notebook_path"] for task in tasks}:
            return True, [driver]
        if path == driver:
            return True, [f"{base}/Users/test/{parameter_bindings['variant']}.ipynb"]
        return True, []

    monkeypatch.setattr(workflow_dag, "scan_external_notebook", scan)
    deps = asyncio.run(workflow_dag.discover_run_deps_for_tasks(
        object(), tasks, base, base,
    ))
    job = workflow_dag.build_manifest(
        "binding_job", tasks, [["A"], ["B"]], deps,
        {"parameters": []}, base_prefix=base, source_prefix=base,
    )["jobs"][0]
    job["default_cluster"] = Session.cluster_id
    monkeypatch.setattr(jm, "EXPORT_BASE", "export/notebooks")
    source = json.dumps({"cells": [{"cell_type": "code", "source": ["print(1)\n"]}]}).encode()
    monkeypatch.setattr(jm, "download_notebook", lambda path: source)
    monkeypatch.setattr(jm, "scan_for_run_deps", lambda content: pytest.fail(
        "manifest mode must not scan dependencies"))

    converted = []

    async def convert(path, *args, **kwargs):
        converted.append(path)
        return {"path": path, "status": "PASS", "ok": 1, "failed": 0}

    async def registry_update(*args, **kwargs):
        return None

    monkeypatch.setattr(jm, "process_notebook", convert)
    monkeypatch.setattr(jm, "registry_update", registry_update)
    result = asyncio.run(jm.process_job(
        job, Session(), skip_task_dependency_scan=True,
    ))

    assert result["task_results"]["A"]["status"] == "PASS"
    assert result["task_results"]["B"]["status"] == "PASS"
    assert any(path.endswith("/X.ipynb") for path in converted)
    assert any(path.endswith("/Y.ipynb") for path in converted)
    assert len([path for path in converted if path.endswith("/driver.ipynb")]) == 1


@pytest.mark.parametrize("resume_source", ["filesystem", "registry"])
@pytest.mark.parametrize("descendant_available", [False, True])
def test_manifest_resume_verifies_task_child_and_grandchild_before_skip(
        monkeypatch, tmp_path, resume_source, descendant_available):
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", True)
    job = {
        "job_name": "manifest_scan_flag",
        "tasks": [
            {"task_key": "parent", "notebook_path": PARENT, "depends_on": [],
             "run_deps": [{"path": CHILD, "exists": True, "nested_deps": []}]},
            {"task_key": "child", "notebook_path": CHILD, "depends_on": [],
             "run_deps": [{"path": GRANDCHILD, "exists": True,
                           "nested_deps": []}]},
        ],
        "execution_layers": [["parent"]],
        "default_cluster": Session.cluster_id,
    }

    class ResumeSession(Session):
        async def execute(self, code, **kwargs):
            output = "VALID:1\n" if resume_source == "filesystem" else "MISSING\n"
            return {"status": "ok", "outputs": [{"type": "stream", "text": output}]}

    prepared = []

    async def prepare(path, *args, **kwargs):
        prepared.append(path)
        if path == GRANDCHILD and not descendant_available:
            return None
        return f"{jm.MIGRATED_BASE}/{path.removeprefix('/Workspace/')}"

    async def lookup(session, path):
        assert path == PARENT
        return f"{jm.MIGRATED_BASE}/Users/test/parent.ipynb"

    async def registry_update(*args, **kwargs):
        return None

    async def load_registry(session, job_name):
        # A resume reuses the parent only over its recorded success.
        return {jm.registry_key(PARENT): {"status": "PASS"}}

    monkeypatch.setattr(jm, "ensure_migrated", prepare)
    monkeypatch.setattr(jm, "registry_lookup", lookup)
    monkeypatch.setattr(jm, "registry_update", registry_update)
    monkeypatch.setattr(jm, "load_registry", load_registry)
    monkeypatch.setattr(jm, "process_notebook", lambda *a, **kw: pytest.fail(
        "resumed parent must not be migrated"))
    result = asyncio.run(jm.process_job(
        job, ResumeSession(), skip_task_dependency_scan=True,
    ))

    status = result["task_results"]["parent"]["status"]
    assert status == ("ALREADY_MIGRATED" if descendant_available else "FAIL")
    assert prepared == ([GRANDCHILD, CHILD] if descendant_available else [GRANDCHILD])


def test_manifest_resume_rechecks_stale_registry_child_before_reuse(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", True)
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path / "migrated" / "notebooks"))
    lookups = []
    converted = []

    async def lookup(session, path):
        lookups.append(path)
        return f"{jm.MIGRATED_BASE}/stale.ipynb"

    async def convert(path, *args, **kwargs):
        converted.append(path)
        return {"status": "CONVERTED_NOT_VALIDATED"}

    async def registry_update(*args, **kwargs):
        return None

    source = json.dumps({"cells": [{"cell_type": "code", "source": ["print(1)\n"]}]}).encode()
    monkeypatch.setattr(jm, "registry_lookup", lookup)
    monkeypatch.setattr(jm, "download_notebook", lambda path: source)
    monkeypatch.setattr(jm, "process_notebook", convert)
    monkeypatch.setattr(jm, "registry_update", registry_update)
    result = asyncio.run(jm.ensure_migrated(
        CHILD, Session(), manifest_dependency_map={
            "Users/test/child.ipynb": [],
        },
    ))
    assert result.endswith("Users/test/child.ipynb")
    assert lookups == []
    assert converted == ["Users/test/child.ipynb"]


def test_default_backfill_migrates_unmigrated_child(monkeypatch, tmp_path):
    real_backfill = jm.backfill_missing_deps
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "backfill_missing_deps", real_backfill)
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path / "migrated" / "job" / "notebooks"))
    target = "/Workspace/Users/test/unlisted.ipynb"
    calls = []

    class BackfillSession(Session):
        async def run_stateless(self, code, **kwargs):
            calls.append(code)
            output = [
                json.dumps([f"{jm.MIGRATED_BASE}/parent.ipynb"]),
                json.dumps([[f"{jm.MIGRATED_BASE}/parent.ipynb", target]]),
                json.dumps({f"{jm.MIGRATED_BASE}/Users/test/unlisted.ipynb": False}),
            ][(len(calls) - 1) % 3]
            return {"status": "ok", "outputs": [{"type": "stream", "text": output}]}

    prepared = []

    async def prepare(path, *args, **kwargs):
        prepared.append(path)
        return f"{jm.MIGRATED_BASE}/Users/test/unlisted.ipynb"

    monkeypatch.setattr(jm, "ensure_migrated", prepare)
    stats = asyncio.run(jm.backfill_missing_deps(
        BackfillSession(), "job",
    ))
    assert stats["failed"] == []
    assert stats["migrated"] == 1
    assert prepared == [target]
    assert len(calls) == 5


def test_manifest_mode_skips_backfill_after_partial_task(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path)

    async def forbidden_backfill(*args, **kwargs):
        pytest.fail("manifest mode must not scan migrated notebooks")

    async def migrate(*args, **kwargs):
        return {"status": "PARTIAL_DATA_UNAVAILABLE", "ok": 0,
                "failed": 0, "data_unavailable": 1}

    monkeypatch.setattr(jm, "backfill_missing_deps", forbidden_backfill)
    monkeypatch.setattr(jm, "process_notebook", migrate)
    result = asyncio.run(jm.process_job(
        _job([]), Session(), skip_task_dependency_scan=True,
    ))
    assert result["task_results"]["parent"]["status"] == "PARTIAL_DATA_UNAVAILABLE"


@pytest.mark.parametrize("skip_scan", [False, True])
@pytest.mark.parametrize("parameter_source", ["task", "job", "job_override"])
@pytest.mark.parametrize("path_state", ["exported", "original", "migrated", "unrelated"])
def test_notebook_path_parameter_uses_prepared_child_at_job_boundary(
        monkeypatch, tmp_path, skip_scan, parameter_source, path_state):
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "EXPORT_BASE", "export/notebooks")
    exported_child = "/Workspace/export/notebooks/Users/test/child.ipynb"
    migrated_child = (
        f"{jm.MIGRATE_OUTPUT}/manifest_scan_flag/notebooks/Users/test/child.ipynb"
    )
    value = {
        "exported": exported_child,
        "original": CHILD,
        "migrated": migrated_child,
        "unrelated": "/Workspace/Users/other/child.ipynb",
    }[path_state]
    job = _job([{"path": exported_child, "exists": True, "nested_deps": []}])
    if parameter_source == "task":
        job["tasks"][0]["base_parameters"] = {"notebook_path": value}
    else:
        job["parameters"] = {"notebook_path": value}
        if parameter_source == "job_override":
            job["tasks"][0]["base_parameters"] = {"notebook_path": "old task value"}
    received = []
    task_param_snapshots = []
    monkeypatch.setattr(jm, "set_current_task_params", lambda params: task_param_snapshots.append(dict(params)))

    monkeypatch.setattr(jm, "download_notebook", lambda path: b"parent notebook")
    monkeypatch.setattr(jm, "scan_for_run_deps", lambda content: [(exported_child, {})])

    async def prepare(path, *args, **kwargs):
        return migrated_child

    async def migrate(path, session, job_name, task_key, parameters, **kwargs):
        received.append(dict(parameters))
        return {"status": "PASS", "ok": 1, "failed": 0}

    monkeypatch.setattr(jm, "ensure_migrated", prepare)
    monkeypatch.setattr(jm, "process_notebook", migrate)
    asyncio.run(jm.process_job(job, Session(), skip_task_dependency_scan=skip_scan))

    expected = migrated_child if path_state != "unrelated" else value
    assert received[0]["notebook_path"] == expected
    assert task_param_snapshots[-1]["notebook_path"] == expected
    # The source manifest stays an export-time record; only the run uses the
    # migrated value, and cloning rewrites its workflow payload separately.
    if parameter_source == "task":
        assert job["tasks"][0]["base_parameters"]["notebook_path"] == value
    else:
        assert job["parameters"]["notebook_path"] == value


@pytest.mark.parametrize("failure", [RuntimeError("job failed"), asyncio.CancelledError()])
def test_manifest_mode_context_is_restored_when_job_raises(monkeypatch, tmp_path, failure):
    _setup(monkeypatch, tmp_path)

    async def fail(*args, **kwargs):
        assert jm._manifest_dependency_paths.get() is not None
        raise failure

    monkeypatch.setattr(jm, "_process_job_inner", fail)

    async def run():
        with pytest.raises(type(failure)):
            await jm.process_job(_job([]), Session(), skip_task_dependency_scan=True)
        assert jm._manifest_dependency_paths.get() is None

    asyncio.run(run())
