"""Export run 2026-10-27T11:51:33Z-cdbad22a (REF-278): discovery after a parent fallback.

The 404-only parent-whitespace fallback fetches "Controllers/Base.py" for the
task path "Controllers /Base"; the requested folder does not exist. Sibling,
resource and load_config discovery still listed the requested folder, so the
missing listing was treated as empty and the notebook's companion modules and
files were silently not exported (only a stdout WARN). Discovery lists the
folder the notebook was fetched from; audit references keep the task path.
"""
import json
from urllib.parse import unquote, urlsplit

import pytest
import requests

from dbx_export import __main__ as exporter
from dbx_export import dependency_resolver as resolver_module
from dbx_export.dbx_client import DbxClient, DbxConfig
from dbx_export.dependency_resolver import DependencyResolver, NotebookNode
from test_export_path_plan import ROOT, MemoryAidp, main_harness, report  # noqa: F401 (fixture)

NOTEBOOK = (b"# Databricks notebook source\n"
            b"from constants import *\n"
            b"body = open('templates/body.txt').read()\n"
            b"cfg = load_config('conf')\n")
OTHER = b"# Databricks notebook source\nVALUE = 2\n"


def _tree(folder):
    return {f"{folder}/Base.py": NOTEBOOK,
            # The module names a resource folder the notebook itself never mentions.
            f"{folder}/constants.py": b"TABLE = 'database/table.csv'\n",
            f"{folder}/database/table.csv": b"a,b\n",
            f"{folder}/templates/body.txt": b"hello\n",
            f"{folder}/conf/settings.yaml": b"k: v\n"}


def _companions(folder):
    return {f"{folder}/constants": "py_sibling", f"{folder}/database/table.csv": "resource",
            f"{folder}/templates/body.txt": "resource", f"{folder}/conf/settings.yaml": "resource"}


def _tree_client(monkeypatch, files, *, statuses=None):
    """Serve an in-memory Git tree whose listings include directory entries."""
    statuses = statuses or {}

    def get(url, **kwargs):
        path = unquote(urlsplit(url).path.split("/contents/", 1)[1])
        response = requests.Response()
        response.status_code = statuses.get(path, 200)
        if response.status_code != 200:
            response._content = b'{"message":"synthetic HTTP failure"}'
            return response
        if path in files:
            response._content = files[path]
            return response
        entries = {}
        for name in files:
            if name.startswith(path + "/"):
                head, _, rest = name[len(path) + 1:].partition("/")
                entries[head] = "dir" if rest else "file"
        response.status_code = 200 if entries else 404
        response._content = json.dumps([{"name": n, "type": t} for n, t in sorted(entries.items())]).encode()
        return response

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", get)
    return DbxClient(DbxConfig(proxy="proxy.example:8080", proxy_max_retries=1))


def _resolve(monkeypatch, files, tasks, **serving):
    client = _tree_client(monkeypatch, files, **serving)
    job = {"settings": {"name": "job", "git_source": {"git_url": "https://github.com/owner/repo", "git_branch": "main"},
           "tasks": [{"task_key": key, "notebook_task": {"source": "GIT", "notebook_path": path}} for key, path in tasks]}}
    monkeypatch.setattr(client, "get_job", lambda job_id: job)
    return DependencyResolver(client, log=lambda message: None).resolve("42")


@pytest.mark.parametrize("folder", ["Controllers", "Root Folder/Sub Folder"])
@pytest.mark.parametrize("extension", ["", ".py"])
def test_trimmed_task_discovers_the_fetched_folder_companions(monkeypatch, folder, extension):
    task = f"{folder} /Base{extension}"
    graph = _resolve(monkeypatch, _tree(folder), [("base", task)])
    # The fetched folder also lists the notebook itself under its fetched path.
    assert {key: node.role for key, node in graph.notebooks.items()} == {
        task: "task", f"{folder}/Base": "py_sibling", **_companions(folder)}
    assert not graph.notebooks[task].resource_discovery_failures
    for key in _companions(folder):
        companion = graph.notebooks[key]
        assert companion.export_error is None and companion.content is not None
        assert any(ref.endswith(f"-of:{task}") for ref in companion.referenced_by)


@pytest.mark.parametrize("task,folder", [
    ("Controllers/Base", "Controllers"),
    # The spaced folder really exists: exact-first fetching keeps listing it.
    ("Controllers /Base", "Controllers "),
])
def test_discovery_without_the_fallback_is_unchanged(monkeypatch, task, folder):
    graph = _resolve(monkeypatch, _tree(folder), [("base", task)])
    assert graph.notebooks[task].resolved_git_path == f"{folder}/Base.py"
    assert {key: node.role for key, node in graph.notebooks.items()} == {task: "task", **_companions(folder)}
    assert not graph.notebooks[task].resource_discovery_failures


@pytest.mark.parametrize("order", ["trimmed_first", "canonical_first"])
def test_task_order_does_not_change_discovered_artifacts(monkeypatch, order):
    tasks = [("base", "Controllers /Base"), ("other", "Controllers/Other")]
    graph = _resolve(monkeypatch, {**_tree("Controllers"), "Controllers/Other.py": OTHER},
                     tasks if order == "trimmed_first" else tasks[::-1])
    assert set(graph.notebooks) == {"Controllers /Base", "Controllers/Base", "Controllers/Other",
                                    *_companions("Controllers")}


def test_listing_failure_in_the_fetched_folder_is_recorded_like_the_canonical_path(monkeypatch):
    statuses = {"Controllers": 403}
    canonical = _resolve(monkeypatch, _tree("Controllers"), [("base", "Controllers/Base.py")], statuses=statuses)
    trimmed = _resolve(monkeypatch, _tree("Controllers"), [("base", "Controllers /Base.py")], statuses=statuses)

    def outcome(graph, task):
        failures = {(item["directory"], item["reason"]) for item in graph.notebooks[task].resource_discovery_failures}
        return set(graph.notebooks) - {task}, failures

    assert outcome(trimmed, "Controllers /Base.py") == outcome(canonical, "Controllers/Base.py")
    assert ("Controllers", "GIT_DIRECTORY_LIST_FAILED") in outcome(trimmed, "Controllers /Base.py")[1]


def test_trimmed_single_task_export_uploads_its_companions_once(monkeypatch):
    graph = _resolve(monkeypatch, _tree("Controllers"), [("base", "Controllers /Base")])
    aidp, result = MemoryAidp(), report()
    paths, failed = exporter.phase2_upload_notebooks(aidp, graph, ROOT, False, "run", "42", result)
    assert not failed and not result.path_plan_errors
    # The task and the sibling found under its fetched path are one Git file.
    assert sum(message.startswith("INFO notebook-source-aliases") for message in result.errors) == 1
    assert {paths[key] for key in _companions("Controllers")} <= set(aidp.writes)
    assert len(aidp.writes) == len(set(aidp.writes)) == 5


# The alias rule alone let this export succeed while the recovered notebook's
# own resource was searched for in the missing spaced folder; its canonical
# alias is copied without traversal, so nothing else found the file.
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("canonical_task", [True, False], ids=["with-canonical-task", "single-task"])
def test_main_success_includes_the_recovered_notebook_companions(main_harness, monkeypatch, overwrite, canonical_task):  # noqa: F811
    aidp, tmp_path = main_harness
    files = {"Controllers/Base.py": b"# Databricks notebook source\nfrom constants import *\n"
                                    b"body = open('templates/body.txt').read()\n",
             "Controllers/constants.py": b"VALUE = 1\n",
             "Controllers/templates/body.txt": b"hello\n",
             "Controllers/Other.py": OTHER}
    tasks = [("base", "Controllers /Base")] + ([("other", "Controllers/Other")] if canonical_task else [])
    graph = _resolve(monkeypatch, files, tasks)
    monkeypatch.setattr(exporter, "phase1_discover", lambda *args, **kwargs: graph)
    phase3 = []
    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows", lambda **kwargs: phase3.append(kwargs))
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path), "--strict", "--strict-deps"]
    argv += ["--overwrite"] if overwrite else []
    assert exporter.main(argv) == exporter.ExitCode.OK
    saved = json.loads(next(tmp_path.glob("export_dbx_job_42_*.json")).read_text())
    assert not saved["path_plan_errors"] and len(phase3) == 1
    folder = f"{ROOT}/notebooks/GIT/Controllers"
    for companion in ("constants.py", "templates/body.txt"):
        assert aidp.writes.count(f"{folder}/{companion}") == 1, companion
    # The task and its canonical alias share one upload in both overwrite modes.
    assert aidp.writes.count(f"{folder}/Base.ipynb") == 1


@pytest.mark.parametrize("dbx_path,resolved,expected", [
    ("Controllers /Base", "Controllers/Base.py", "Controllers/Base"),
    ("Root Folder/Sub Folder /Base.py", "Root Folder/Sub Folder/Base.py", "Root Folder/Sub Folder/Base.py"),
    ("/Controllers /Base", "Controllers/Base.py", "/Controllers/Base"),
    # No fallback: extension lookup, the leading slash, a real spaced folder, no fetch.
    ("Controllers/Base", "Controllers/Base.ipynb", "Controllers/Base"),
    ("/Controllers/Base", "Controllers/Base.py", "/Controllers/Base"),
    ("Controllers /Base", "Controllers /Base.py", "Controllers /Base"),
    ("Controllers /Base", None, "Controllers /Base"),
    ("Base", "Base.py", "Base"),
])
def test_fetched_location_moves_only_a_fallback_parent(dbx_path, resolved, expected):
    node = NotebookNode(dbx_path=dbx_path, role="task", git_owner="owner", git_repo="repo", resolved_git_path=resolved)
    assert resolver_module._fetched_location(node, dbx_path) == expected
    workspace = NotebookNode(dbx_path=dbx_path, role="task", resolved_git_path=resolved)
    assert resolver_module._fetched_location(workspace, dbx_path) == dbx_path
