"""Export run 2026-10-27T11:51:33Z-cdbad22a (REF-278): one Git file, two references.

A task path whose parent carries a space absent from Git is fetched through the
404-only parent-whitespace fallback, while another notebook's sibling scan names
the same repository file without the space. Both references sanitize to one
export destination. The verified Git source alias rule accepted only the
extensionless and exact spellings of a file, so the single file was refused as
two sources (exit 17). A reference the fallback recovered now names the fetched
file as well: it is uploaded once and both discovery rows share its outcome.
Distinct files still collide.
"""
import copy
import json

import pytest

from dbx_export import __main__ as exporter
from dbx_export import path_plan
from dbx_export.aidp_client import AidpApiError
from dbx_export.dependency_resolver import DependencyResolver, NotebookNode
from dbx_export.job_converter import convert_dbx_job
from dbx_export.sanitizer import KIND_NOTEBOOK, dbx_to_aidp_path
from test_export_path_plan import ROOT, MemoryAidp, main_harness, report  # noqa: F401 (fixture)
from test_git_notebook_parent_whitespace import _repository_client

SOURCE = b"# Databricks notebook source\nVALUE = 1\n"
OTHER = b"# Databricks notebook source\nVALUE = 2\n"
IPYNB = json.dumps({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": []}).encode()
FILES = {"Controllers/Base.py": SOURCE, "Controllers/Other.py": OTHER}
TRIMMED = ("base", "Controllers /Base", {})
SIBLING_SCAN = ("other", "Controllers/Other", {})
BASE = dbx_to_aidp_path("Controllers/Base", ROOT, KIND_NOTEBOOK, is_git=True)
ALIAS = "INFO notebook-source-aliases"


def _job(*tasks):
    return {"job_id": "42", "settings": {
        "name": "Controllers",
        "git_source": {"git_url": "https://github.com/owner/repo", "git_branch": "main"},
        "tasks": [{"task_key": key, "notebook_task": {"source": "GIT", "notebook_path": path, **extra}}
                  for key, path, extra in tasks],
    }}


def _resolve(monkeypatch, files, job, **serving):
    client, calls = _repository_client(monkeypatch, files, **serving)
    monkeypatch.setattr(client, "get_job", lambda job_id: job)
    return DependencyResolver(client, log=lambda message: None).resolve("42"), calls


def _upload(graph, overwrite=False, aidp=None):
    aidp, result = aidp or MemoryAidp(), report()
    paths, failed = exporter.phase2_upload_notebooks(aidp, graph, ROOT, overwrite, "run", "42", result)
    return aidp, result, paths, failed


def _shared(graph, *sources):
    """Return the sources in graph order: the uploaded one first, then its aliases."""
    return [path for path in graph.notebooks if path in sources]


class _MovedBranch(dict):
    """Serve later reads of the named repository files with different bytes."""

    def __init__(self, files, moved):
        super().__init__(files)
        self.moved, self.reads = moved, {}

    def __getitem__(self, path):
        self.reads[path] = self.reads.get(path, 0) + 1
        if path in self.moved and self.reads[path] > 1:
            return self.moved[path]
        return super().__getitem__(path)


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("order", ["sibling_scan_first", "trimmed_task_first"])
def test_trimmed_task_and_canonical_sibling_export_one_file(monkeypatch, order, overwrite):
    job = _job(*([SIBLING_SCAN, TRIMMED] if order == "sibling_scan_first" else [TRIMMED, SIBLING_SCAN]))
    graph, _ = _resolve(monkeypatch, FILES, job)
    assert {key: node.role for key, node in graph.notebooks.items()} == {
        "Controllers /Base": "task", "Controllers/Other": "task", "Controllers/Base": "py_sibling"}
    assert graph.notebooks["Controllers /Base"].resolved_git_path == "Controllers/Base.py"
    assert graph.notebooks["Controllers/Base"].resolved_git_path == "Controllers/Base.py"
    aidp, result, paths, failed = _upload(graph, overwrite)
    assert not failed and not result.path_plan_errors
    assert paths["Controllers /Base"] == paths["Controllers/Base"] == BASE
    # One physical upload; both discovery rows report its outcome.
    assert aidp.writes.count(BASE) == 1
    rows = {row.dbx_path: row for row in result.notebooks}
    canonical, alias = _shared(graph, "Controllers /Base", "Controllers/Base")
    assert rows[canonical].status == rows[alias].status == "VALIDATED"
    assert rows[alias].alias_of == canonical and rows[canonical].alias_of is None
    assert sum(message.startswith(ALIAS) for message in result.errors) == 1
    tasks = {task["taskKey"]: task for task in convert_dbx_job(job, ROOT, "tool", {}, notebook_path_map=paths)["tasks"]}
    assert tasks["base"]["notebookPath"] == BASE


def test_trimmed_relative_run_target_aliases_the_canonical_sibling(monkeypatch):
    files = {**FILES, "Controllers/Base.py": SOURCE + b"# MAGIC %run ./Utils\n", "Controllers/Utils.py": SOURCE}
    graph, _ = _resolve(monkeypatch, files, _job(SIBLING_SCAN, TRIMMED))
    assert graph.notebooks["Controllers /Utils"].role == "run_dep"
    assert graph.notebooks["Controllers /Utils"].resolved_git_path == graph.notebooks["Controllers/Utils"].resolved_git_path
    aidp, result, _, failed = _upload(graph)
    assert not failed and not result.path_plan_errors
    assert sum(message.startswith(ALIAS) for message in result.errors) == 2
    assert len(aidp.writes) == len(set(aidp.writes)) == 3


@pytest.mark.parametrize("spellings", [
    ("Controllers /Base", "Controllers/Base.py"),
    ("Controllers /Base.py", "Controllers/Base"),
    ("Controllers /Base.py", "Controllers/Base.py"),
    ("Controllers /Base", "Controllers/Base", "Controllers/Base.py"),
])
@pytest.mark.parametrize("overwrite", [False, True])
def test_fallback_reference_joins_each_spelling_of_the_fetched_file(monkeypatch, spellings, overwrite):
    job = _job(*[(f"task{index}", path, {}) for index, path in enumerate(spellings)])
    graph, _ = _resolve(monkeypatch, {"Controllers/Base.py": SOURCE}, job)
    assert {graph.notebooks[path].resolved_git_path for path in spellings} == {"Controllers/Base.py"}
    aidp, result, paths, failed = _upload(graph, overwrite)
    assert not failed and not result.path_plan_errors
    assert {paths[path] for path in spellings} == {BASE} and aidp.writes == [BASE]
    rows = {row.dbx_path: row for row in result.notebooks}
    canonical, *aliases = _shared(graph, *spellings)
    assert all(rows[path].status == "VALIDATED" for path in spellings)
    assert all(rows[alias].alias_of == canonical for alias in aliases)


@pytest.mark.parametrize("references,resolved,accepted", [
    (("Controllers /Base", "Controllers/Base"), "Controllers/Base.py", True),
    (("Controllers  /Base", "Controllers/Base.py"), "Controllers/Base.py", True),
    (("/Controllers /Base.py", "Controllers/Base"), "Controllers/Base.py", True),
    (("Controllers /Base", "Controllers/Base"), "Controllers/Base.ipynb", True),
    # A whitespace-only parent is retried at the repository root.
    ((" /Base", "Base"), "Base.py", True),
    # The extensionless and exact spellings are accepted as before.
    (("Controllers/Base", "Controllers/Base.py"), "Controllers/Base.py", True),
    # Leading-slash spellings keep the existing identity comparison.
    (("/Controllers/Base.py", "Controllers/Base.py"), "Controllers/Base.py", False),
    # Only the parent's outer whitespace is retried by the fallback.
    (("Root /Sub/Base", "Root/Sub/Base"), "Root/Sub/Base.py", False),
    (("Controllers /Base", "Other/Base"), "Controllers/Base.py", False),
    # The rule's file types are unchanged.
    (("Controllers /Base", "Controllers/Base"), "Controllers/Base.scala", False),
])
def test_fallback_references_are_recognized_by_their_trimmed_parent(references, resolved, accepted):
    entries = {("notebook", source): NotebookNode(
        dbx_path=source, role="task", content=SOURCE, nb_dict={"cells": []}, git_owner="owner",
        git_repo="repo", git_branch="main", resolved_git_path=resolved) for source in references}
    assert path_plan._git_source_path_aliases(list(entries), entries) is accepted


@pytest.mark.parametrize("files,tasks", [
    # The exact spaced directory exists: exact-first fetching keeps two files.
    ({"Controllers /Base.py": SOURCE, **FILES}, [SIBLING_SCAN, TRIMMED]),
    ({"A B/Base.py": SOURCE, "A_B/Base.py": SOURCE}, [("one", "A B/Base", {}), ("two", "A_B/Base", {})]),
    # Extension lookup prefers Base.ipynb over the explicitly requested Base.py.
    ({"Controllers/Base.ipynb": IPYNB, "Controllers/Base.py": SOURCE},
     [("one", "Controllers/Base", {}), ("two", "Controllers/Base.py", {})]),
], ids=["exact-spaced-directory", "sanitized-directory-names", "extension-lookup"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_distinct_repository_files_still_refuse_one_destination(monkeypatch, files, tasks, overwrite):
    graph, _ = _resolve(monkeypatch, files, _job(*tasks))
    aidp, result, _, failed = _upload(graph, overwrite)
    assert failed and not aidp.writes
    assert len(result.path_plan_errors) == 2
    assert all("duplicate export destination" in error["reason"] for error in result.path_plan_errors)


def test_fallback_reference_to_a_different_file_still_collides(monkeypatch):
    # The trimmed folder's extension lookup prefers Base.ipynb; Base.py is another file.
    files = {"Controllers/Base.ipynb": IPYNB, "Controllers/Base.py": SOURCE}
    graph, _ = _resolve(monkeypatch, files, _job(("one", "Controllers /Base", {}), ("two", "Controllers/Base.py", {})))
    assert graph.notebooks["Controllers /Base"].resolved_git_path == "Controllers/Base.ipynb"
    aidp, result, _, failed = _upload(graph)
    assert failed and not aidp.writes
    assert {"Controllers /Base", "Controllers/Base.py"} <= {error["source"] for error in result.path_plan_errors}
    assert not any(message.startswith(ALIAS) for message in result.errors)


@pytest.mark.parametrize("overwrite", [False, True])
def test_one_repository_file_with_changed_content_is_refused(monkeypatch, overwrite):
    files = _MovedBranch(FILES, {"Controllers/Base.py": OTHER})
    graph, _ = _resolve(monkeypatch, files, _job(SIBLING_SCAN, TRIMMED))
    aidp, result, _, failed = _upload(graph, overwrite)
    assert failed and not aidp.writes
    reasons = {error["source"]: error["reason"] for error in result.path_plan_errors}
    assert set(reasons) == {"Controllers /Base", "Controllers/Base"}
    assert all("conflicting content for notebook source aliases" in reason for reason in reasons.values())


@pytest.mark.parametrize("status", [401, 403, 429, 500, 503])
def test_non_404_spaced_parent_failure_stays_visible(monkeypatch, status):
    graph, _ = _resolve(monkeypatch, FILES, _job(SIBLING_SCAN, TRIMMED), statuses={"Controllers ": status})
    node = graph.notebooks["Controllers /Base"]
    assert node.export_error == "github_list_failed" and str(status) in node.error_detail
    assert node.resolved_git_path is None
    # The failed task still reserves its destination against the sibling.
    aidp, result, _, failed = _upload(graph)
    assert "Controllers /Base" in failed and not aidp.writes
    assert not any(message.startswith(ALIAS) for message in result.errors)


def test_tasks_sharing_the_file_keep_their_own_parameters(monkeypatch):
    job = _job(("a", "Controllers /Base", {"base_parameters": {"mode": "a"}}),
               ("b", "Controllers/Base", {"base_parameters": {"mode": "b"}}))
    graph, _ = _resolve(monkeypatch, {"Controllers/Base.py": SOURCE}, job)
    assert {key: (node.role, node.parameter_bindings["mode"]) for key, node in graph.notebooks.items()} == {
        "Controllers /Base": ("task", "a"), "Controllers/Base": ("task", "b")}
    aidp, result, paths, failed = _upload(graph)
    assert not failed and not result.path_plan_errors and aidp.writes == [BASE]
    tasks = {task["taskKey"]: task for task in convert_dbx_job(job, ROOT, "tool", {}, notebook_path_map=paths)["tasks"]}
    assert tasks["a"]["notebookPath"] == tasks["b"]["notebookPath"] == BASE
    assert [(p["name"], p["value"]) for p in tasks["a"]["parameters"]] == [("mode", "a")]
    assert [(p["name"], p["value"]) for p in tasks["b"]["parameters"]] == [("mode", "b")]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("failure", ["upload", "readback", "checksum"])
def test_trimmed_aliases_share_a_failed_upload(monkeypatch, reverse, failure):
    graph, _ = _resolve(monkeypatch, FILES, _job(SIBLING_SCAN, TRIMMED))
    if reverse:
        graph.notebooks = dict(reversed(list(graph.notebooks.items())))
    aidp = MemoryAidp()
    put, get = aidp.put_notebook, aidp.get_notebook

    def put_notebook(path, notebook):
        if path == BASE and failure == "upload":
            raise AidpApiError("put", 500, "injected failure")
        return put(path, notebook)

    def get_notebook(path):
        if path != BASE or path not in aidp.notebooks:
            return get(path)  # the existence probe before the upload
        if failure == "readback":
            raise AidpApiError("get", 500, "injected failure")
        return {"cells": [{"cell_type": "code", "source": ["VALUE = 3"]}]} if failure == "checksum" else get(path)

    monkeypatch.setattr(aidp, "put_notebook", put_notebook)
    monkeypatch.setattr(aidp, "get_notebook", get_notebook)
    aidp, result, _, failed = _upload(graph, aidp=aidp)
    # Whichever reference uploads, the failure reaches the task through its alias row.
    assert failed == ["Controllers /Base"]
    assert aidp.writes.count(BASE) == (0 if failure == "upload" else 1)
    rows = {row.dbx_path: row for row in result.notebooks}
    canonical, alias = _shared(graph, "Controllers /Base", "Controllers/Base")
    assert rows[alias].alias_of == canonical
    assert rows[canonical].status == rows[alias].status == (
        "FINGERPRINT_MISMATCH" if failure == "checksum" else "FAILED")
    assert rows["Controllers/Other"].status == "VALIDATED"


@pytest.mark.parametrize("state", ["neither", "current", "legacy", "both"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_trimmed_aliases_share_the_verified_existing_destination(monkeypatch, state, overwrite):
    folder = "Owner's Controllers"
    files = {f"{folder}/Base.py": SOURCE, f"{folder}/Other.py": OTHER}
    graph, _ = _resolve(monkeypatch, files, _job(("base", f"{folder} /Base", {}), ("other", f"{folder}/Other", {})))
    current = dbx_to_aidp_path(f"{folder}/Base", ROOT, KIND_NOTEBOOK, is_git=True)
    legacy = dbx_to_aidp_path(f"{folder}/Base", ROOT, KIND_NOTEBOOK, is_git=True, legacy=True)
    assert current != legacy
    aidp = MemoryAidp()
    existing = copy.deepcopy(graph.notebooks[f"{folder} /Base"].nb_dict)
    for path in {"current": [current], "legacy": [legacy], "both": [current, legacy]}.get(state, []):
        aidp.notebooks[path] = copy.deepcopy(existing)
    aidp, result, paths, failed = _upload(graph, overwrite, aidp)
    if state == "both":
        assert failed and not aidp.writes
        assert all("both current and legacy" in error["reason"] for error in result.path_plan_errors)
        return
    assert not failed and not result.path_plan_errors
    chosen = legacy if state == "legacy" else current
    assert paths[f"{folder} /Base"] == paths[f"{folder}/Base"] == chosen
    # Verified existing content is reused without rewriting, even with --overwrite.
    assert aidp.writes.count(chosen) == (0 if state != "neither" else 1)


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("strict", [False, True])
def test_main_exports_trimmed_aliases_instead_of_exit_17(main_harness, monkeypatch, dry_run, overwrite, strict):  # noqa: F811
    aidp, tmp_path = main_harness
    graph, _ = _resolve(monkeypatch, FILES, _job(SIBLING_SCAN, TRIMMED))
    monkeypatch.setattr(exporter, "phase1_discover", lambda *args, **kwargs: graph)
    phase3 = []
    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows", lambda **kwargs: phase3.append(kwargs))
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path)]
    argv += [flag for flag, enabled in [("--dry-run", dry_run), ("--overwrite", overwrite), ("--strict", strict)] if enabled]
    assert exporter.main(argv) == exporter.ExitCode.OK
    saved = json.loads(next(tmp_path.glob("export_dbx_job_42_*.json")).read_text())
    assert not saved["path_plan_errors"]
    assert any(error.startswith(ALIAS) for error in saved["errors"])
    assert len(phase3) == (0 if dry_run else 1)
    assert aidp.writes.count(BASE) == (0 if dry_run else 1)


@pytest.mark.parametrize("strict_deps", [False, True])
def test_main_stops_when_the_shared_trimmed_upload_fails(main_harness, monkeypatch, strict_deps):  # noqa: F811
    aidp, tmp_path = main_harness
    graph, _ = _resolve(monkeypatch, FILES, _job(SIBLING_SCAN, TRIMMED))
    put = aidp.put_notebook

    def put_notebook(path, notebook):
        if path == BASE:
            raise AidpApiError("put", 500, "injected failure")
        return put(path, notebook)

    monkeypatch.setattr(aidp, "put_notebook", put_notebook)
    monkeypatch.setattr(exporter, "phase1_discover", lambda *args, **kwargs: graph)
    phase3 = []
    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows", lambda **kwargs: phase3.append(kwargs))
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path)]
    argv += ["--strict-deps"] if strict_deps else []
    expected = exporter.ExitCode.STRICT_DEPS_FAILED if strict_deps else exporter.ExitCode.MANDATORY_FILE_UPLOAD_FAILED
    assert exporter.main(argv) == expected
    saved = json.loads(next(tmp_path.glob("export_dbx_job_42_*.json")).read_text())
    rows = {row["dbx_path"]: row for row in saved["notebooks"]}
    assert rows["Controllers /Base"]["status"] == rows["Controllers/Base"]["status"] == "FAILED"
    assert not phase3
