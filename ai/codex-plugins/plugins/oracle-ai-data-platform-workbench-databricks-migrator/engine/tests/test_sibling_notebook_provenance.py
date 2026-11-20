"""Incidental Git siblings must not contribute notebook execution edges."""

import json
from types import SimpleNamespace

import pytest

from dbx_export.dbx_client import DbxClient, DbxExportError, ExportedFile
from dbx_export.dependency_resolver import DependencyResolver


def _notebook(source):
    raw = ("# Databricks notebook source\n" + source + "\n").encode()
    assert DbxClient._is_dbx_notebook_source(raw)
    return DbxClient._dbx_source_to_ipynb(raw)


class _GitJob:
    def __init__(self, task_source, files):
        self.files = {"project/task.py": (True, task_source), **files}
        self.fetches = []

    def get_job(self, job_id):
        assert str(job_id) == "1"
        return {"job_id": 1, "settings": {
            "name": "example", "git_source": {"git_url": "https://example.invalid/repo"},
            "tasks": [{"task_key": "task", "notebook_task": {
                "notebook_path": "project/task", "source": "GIT",
            }}],
        }}

    def parse_github_owner_repo(self, _url):
        return "owner", "repo"

    def list_github_dir(self, _owner, _repo, directory, _branch):
        assert directory == "project"
        return {path.rsplit("/", 1)[-1][:-3]: path.rsplit("/", 1)[-1]
                for path in self.files if path.endswith(".py")}

    def list_github_dir_entries(self, _owner, _repo, _directory, _branch):
        return []

    def export_notebook_from_git(self, path, _owner, _repo, _branch):
        self.fetches.append(path)
        if path not in self.files and path + ".py" in self.files:
            path += ".py"
        if path not in self.files:
            raise DbxExportError("not_found", f"{path} missing", 404)
        is_notebook, source = self.files[path]
        if is_notebook:
            return ExportedFile(_notebook(source), True, ".ipynb")
        return ExportedFile(source.encode(), False, ".py")


def _resolve(task_source, files):
    dbx = _GitJob(task_source, files)
    graph = DependencyResolver(dbx, log=lambda _message: None).resolve("1")
    return graph, dbx


def test_unrelated_notebook_sibling_does_not_follow_missing_child():
    graph, dbx = _resolve("VALUE = 1", {
        "project/helper.py": (False, "VALUE = 1"),
        "project/unrelated.py": (True, "%run ./all_utils"),
    })
    assert graph.notebooks["project/helper"].role == "py_sibling"
    assert graph.notebooks["project/unrelated"].role == "py_sibling"
    assert graph.notebooks["project/unrelated"].is_notebook
    assert "project/unrelated" in dbx.fetches
    assert "project/all_utils" not in graph.notebooks


def test_explicit_sibling_reference_promotes_and_follows_child():
    graph, dbx = _resolve("%run ./needed", {
        "project/needed.py": (True, "%run ./all_utils"),
    })
    assert graph.notebooks["project/needed"].role == "run_dep"
    assert "project/task" in graph.notebooks["project/needed"].referenced_by
    assert graph.notebooks["project/all_utils"].export_error == "not_found"
    assert dbx.fetches.count("project/needed") == 1


def test_late_reference_promotes_downloaded_sibling_without_refetch():
    graph, dbx = _resolve("%run ./middle", {
        "project/needed.py": (True, "%run ./all_utils"),
        "project/middle.py": (True, "%run ./needed"),
    })
    assert graph.notebooks["project/needed"].role == "run_dep"
    assert "project/middle" in graph.notebooks["project/needed"].referenced_by
    assert graph.notebooks["project/all_utils"].export_error == "not_found"
    assert dbx.fetches.count("project/needed") == 1


def test_child_job_in_incidental_sibling_is_not_traversed():
    graph, _ = _resolve("VALUE = 1", {
        "project/unrelated.py": (True, "dbutils.jobs.runNow(job_id=99)"),
    })
    assert set(graph.jobs) == {"1"}


def test_required_sibling_child_job_is_traversed():
    class JobWithChild(_GitJob):
        def get_job(self, job_id):
            if str(job_id) == "99":
                return {"job_id": 99, "settings": {"name": "child", "tasks": []}}
            return super().get_job(job_id)

    dbx = JobWithChild("%run ./needed", {
        "project/needed.py": (True, "dbutils.jobs.runNow(job_id=99)"),
    })
    graph = DependencyResolver(dbx, log=lambda _message: None).resolve("1")
    assert set(graph.jobs) == {"1", "99"}


def test_child_job_task_promotion_rescans_already_popped_sibling():
    class MultiJob(_GitJob):
        def get_job(self, job_id):
            if str(job_id) == "2":
                return {"job_id": 2, "settings": {
                    "name": "child", "git_source": {"git_url": "https://example.invalid/repo"},
                    "tasks": [{"task_key": "sibling", "notebook_task": {
                        "notebook_path": "project/sibling", "source": "GIT",
                    }}],
                }}
            if str(job_id) == "3":
                return {"job_id": 3, "settings": {"name": "grandchild", "tasks": []}}
            return super().get_job(job_id)

        def list_github_dir(self, _owner, _repo, directory, _branch):
            return {path.rsplit("/", 1)[-1][:-3]: path.rsplit("/", 1)[-1]
                    for path in self.files
                    if path.startswith(directory + "/") and path.endswith(".py")}

    dbx = MultiJob("%run ../other/level2", {
        "project/sibling.py": (True, "dbutils.jobs.runNow(job_id=3)"),
        "other/level2.py": (True, "dbutils.jobs.runNow(job_id=2)"),
    })
    graph = DependencyResolver(dbx, log=lambda _message: None).resolve("1")
    assert graph.notebooks["project/sibling"].role == "task"
    assert graph.notebooks["project/sibling"].nb_dict is not None
    assert set(graph.jobs) == {"1", "2", "3"}
    assert dbx.fetches.count("project/sibling") == 1


def test_resolved_notebook_run_call_promotes_sibling():
    graph, dbx = _resolve('child = "./needed"\ndbutils.notebook.run(child, 30)\n', {
        "project/needed.py": (True, "%run ./all_utils"),
    })
    assert graph.notebooks["project/needed"].role == "run_dep"
    assert graph.notebooks["project/all_utils"].export_error == "not_found"
    assert dbx.fetches.count("project/needed") == 1


@pytest.mark.parametrize("task_source", [
    "%run ./helper",
    'dbutils.notebook.run("./helper", 0)',
])
def test_explicit_notebook_reference_to_plain_sibling_fails(task_source):
    graph, _ = _resolve(task_source, {
        "project/helper.py": (False, "VALUE = 1"),
    })
    assert graph.notebooks["project/helper"].role == "run_dep"
    assert graph.notebooks["project/helper"].export_error == "not_a_notebook"


def test_later_explicit_reference_upgrades_inferred_plain_file():
    graph, _ = _resolve("%run ./first\n%run ./second", {
        "project/first.py": (True, 'helper = "./plain_util"\n'
                                   'dbutils.notebook.run(runtime_path, 0)'),
        "project/second.py": (True, "%run ./plain_util"),
        "project/plain_util.py": (False, "VALUE = 1"),
    })
    assert graph.notebooks["project/plain_util"].explicit_run_ref
    assert graph.notebooks["project/plain_util"].export_error == "not_a_notebook"


@pytest.mark.parametrize("task_source", [
    'helper = "./plain_util"\ndbutils.notebook.run(runtime_path, 0)',
    'work_items = [("./plain_util", "./real_child")]\n'
    'for row in work_items:\n    dbutils.notebook.run(row[0], 0)',
])
def test_inferred_plain_file_is_not_a_failed_run_dependency(task_source):
    graph, _ = _resolve(task_source, {
        "project/plain_util.py": (False, "VALUE = 1"),
    })
    node = graph.notebooks["project/plain_util"]
    assert node.is_notebook is False
    assert node.role == "py_sibling"
    assert node.export_error is None
    assert any("project/plain_util" in ref for ref in graph.external_refs)


def test_percent_run_namespace_rescan_does_not_requeue_inferred_plain_file():
    dbx = _GitJob(
        '%run ./config\nhelper = "./plain_util"\n'
        'dbutils.notebook.run(runtime_path, 0)',
        {
            "project/config.py": (True, "VALUE = 1"),
            "project/plain_util.py": (False, "VALUE = 1"),
        },
    )
    plain_file_events = []

    def bounded_log(message):
        if "plain_util" in message:
            plain_file_events.append(message)
        assert len(plain_file_events) < 12, "inferred plain file was requeued"

    graph = DependencyResolver(dbx, log=bounded_log).resolve("1")
    plain = graph.notebooks["project/plain_util"]
    assert plain.role == "py_sibling"
    assert plain.inferred_plain_file
    assert plain.export_error is None
    assert "inferred_plain_file:project/plain_util" in graph.external_refs
    assert dbx.fetches.count("project/plain_util") == 1
    assert plain.referenced_by.count("project/task") == 1


def test_explicit_reference_after_plain_file_classification_still_fails():
    class ChildJob(_GitJob):
        def get_job(self, job_id):
            if str(job_id) == "2":
                return {"job_id": 2, "settings": {
                    "name": "child", "git_source": {"git_url": "https://example.invalid/repo"},
                    "tasks": [{"task_key": "later", "notebook_task": {
                        "notebook_path": "other/later", "source": "GIT",
                    }}],
                }}
            return super().get_job(job_id)

        def list_github_dir(self, _owner, _repo, directory, _branch):
            return {path.rsplit("/", 1)[-1][:-3]: path.rsplit("/", 1)[-1]
                    for path in self.files
                    if path.startswith(directory + "/") and path.endswith(".py")}

    dbx = ChildJob(
        'helper = "./plain_util"\n'
        'dbutils.notebook.run(runtime_path, 0)\n'
        'dbutils.jobs.runNow(job_id=2)',
        {
            "project/plain_util.py": (False, "VALUE = 1"),
            "other/later.py": (True, "%run ../project/plain_util"),
        },
    )
    graph = DependencyResolver(dbx, log=lambda _message: None).resolve("1")
    plain = graph.notebooks["project/plain_util"]
    assert plain.inferred_plain_file
    assert plain.explicit_run_ref
    assert plain.role == "run_dep"
    assert plain.export_error == "not_a_notebook"
    assert dbx.fetches.count("project/plain_util") == 1


def test_later_inferred_reference_preserves_plain_file_provenance():
    class ChildJob(_GitJob):
        def get_job(self, job_id):
            if str(job_id) == "2":
                return {"job_id": 2, "settings": {
                    "name": "child", "git_source": {"git_url": "https://example.invalid/repo"},
                    "tasks": [{"task_key": "later", "notebook_task": {
                        "notebook_path": "other/later", "source": "GIT",
                    }}],
                }}
            return super().get_job(job_id)

        def list_github_dir(self, _owner, _repo, directory, _branch):
            return {path.rsplit("/", 1)[-1][:-3]: path.rsplit("/", 1)[-1]
                    for path in self.files
                    if path.startswith(directory + "/") and path.endswith(".py")}

    dbx = ChildJob("%run ./first", {
        "project/plain_util.py": (False, "VALUE = 1"),
        "project/first.py": (True, 'helper = "./plain_util"\n'
                                   'dbutils.notebook.run(runtime_path, 0)\n'
                                   'dbutils.jobs.runNow(job_id=2)'),
        "other/later.py": (True, 'helper = "../project/plain_util"\n'
                                 'dbutils.notebook.run(runtime_path, 0)'),
    })
    graph = DependencyResolver(dbx, log=lambda _message: None).resolve("1")
    plain = graph.notebooks["project/plain_util"]
    assert plain.role == "py_sibling"
    assert plain.inferred_plain_file
    assert plain.export_error is None
    assert "project/first" in plain.referenced_by
    assert plain.referenced_by.count("other/later") == 1
    assert dbx.fetches.count("project/plain_util") == 1


@pytest.mark.parametrize("task_source,expected", [
    ("VALUE = 1", 0),
    ("%run ./needed", 6),
    ('dbutils.notebook.run("./helper", 0)', 6),
    ('helper = "./plain_util"\ndbutils.notebook.run(runtime_path, 0)', 0),
    ('%run ./config\nhelper = "./plain_util"\n'
     'dbutils.notebook.run(runtime_path, 0)', 0),
    ('work_items = [("./plain_util", "./needed")]\n'
     'for row in work_items:\n    dbutils.notebook.run(row[0], 0)', 0),
])
def test_strict_export_gates_only_reachable_child(monkeypatch, tmp_path,
                                                   task_source, expected):
    """Exercise phase 1 and the real strict gate with the sibling collision."""
    from dbx_export import __main__ as exporter

    dbx = _GitJob(task_source, {
        "project/config.py": (True, "VALUE = 1"),
        "project/needed.py": (True, "%run ./all_utils"),
        "project/helper.py": (False, "VALUE = 1"),
        "project/plain_util.py": (False, "VALUE = 1"),
    })
    config = SimpleNamespace(
        aidp=SimpleNamespace(lake_ocid="lake", workspace_id="ws", oci_profile="P"),
        paths=SimpleNamespace(export_output="/Workspace/export",
                              target_runtime_requirements_file=None,
                              compatibility_policy_file=None),
        dbx_source=SimpleNamespace(host=None, proxy=None),
        secrets=SimpleNamespace(dbx_proxy_basic_user=None, dbx_proxy_basic_pass=None),
    )
    monkeypatch.setattr("aidp_config.load_config", lambda **_kw: config)
    monkeypatch.setattr(exporter, "configured_lib_repository_dir", lambda _p: "/libs")
    monkeypatch.setattr(exporter, "get_oci_signer", lambda **_kw: ({}, object()))
    monkeypatch.setattr(exporter, "AidpClient", lambda **_kw: object())
    monkeypatch.setattr(exporter, "DbxClient", lambda *_a, **_kw: dbx)
    monkeypatch.setattr(exporter, "_limits_startup_gate", lambda _a: None)
    monkeypatch.setattr(exporter.ph_mod, "new_run_id", lambda: "sibling-provenance")
    for name in (
        "phase1_fetch_files", "phase1_discover_init_script_artifacts",
        "phase1_discover_existing_cluster_libraries", "discover_notebook_pip_deps",
        "consolidate_cluster_requirements", "resolve_failed_jar_fallbacks",
        "reconcile_failed_declared_jars",
    ):
        monkeypatch.setattr(exporter, name, lambda *_a, **_kw: None)
    class JarDiscovery:
        index_fallbacks = []
        resolutions, index_errors, blockers, allow_unresolved = [], [], [], True

        @staticmethod
        def to_dict():
            return {"summary": {"missing": 0, "ambiguous": 0, "index_errors": 0}}

        @staticmethod
        def write(*_a):
            return str(tmp_path / "jars.json")

    monkeypatch.setattr(exporter, "discover_notebook_jars", lambda *_a, **_kw: JarDiscovery())
    code = exporter.main([
        "--job-id", "1", "--dbx-proxy", "https://proxy.invalid",
        "--cluster-key", "cluster", "--report-dir", str(tmp_path),
        "--dry-run", "--strict-deps",
    ])
    assert code == expected
    report = json.loads((tmp_path / "export_dbx_job_1_sibling-provenance.json").read_text())
    if expected:
        if "%run ./needed" in task_source:
            assert "project/all_utils" in dbx.fetches
        else:
            assert "project/helper" in dbx.fetches
    else:
        assert "project/all_utils" not in dbx.fetches
        if "plain_util" in task_source:
            assert "inferred_plain_file:project/plain_util" in report["external_refs"]
    assert report["dbx_root_job_id"] == "1"
