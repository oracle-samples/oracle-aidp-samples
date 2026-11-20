"""A speculative path miss must not become a confirmed notebook failure."""

import asyncio
import json

import pytest

from dbx_export.dbx_client import DbxExportError, ExportedFile
from dbx_export.__main__ import pre_flight
from dbx_export.dependency_resolver import (
    DependencyResolver, extract_run_targets, portable_run_extractor_source,
)
from dbx_export.report import Report
from scripts import build_dag, build_dag_from_workflow


PARENT = "DxGy/main"
CANDIDATE = "DxGy/Asia/Kolkata"


def _notebook(source):
    return {"nbformat": 4, "nbformat_minor": 5, "metadata": {},
            "cells": [{"cell_type": "code", "source": source}]}


@pytest.mark.parametrize("portable", [False, True])
def test_loose_provenance_is_removed_when_same_target_is_explicit(portable):
    extractor = extract_run_targets
    if portable:
        scope = {}
        exec(portable_run_extractor_source(), scope)
        extractor = scope["extract_run_targets"]

    source = _notebook(
        'timezone = "Asia/Kolkata"\n'
        'dbutils.notebook.run(query_path, 0)\n'
    )
    loose_only = set()
    assert extractor(source, PARENT, loose_only=loose_only) == [CANDIDATE]
    assert loose_only == {CANDIDATE}

    source["cells"].append({"cell_type": "code", "source": "%run ./Asia/Kolkata"})
    loose_only = set()
    assert extractor(source, PARENT, loose_only=loose_only) == [CANDIDATE]
    assert loose_only == set()


@pytest.mark.parametrize("portable", [False, True])
def test_loose_provenance_is_removed_after_workspace_path_rewrite(portable):
    extractor = extract_run_targets
    if portable:
        scope = {}
        exec(portable_run_extractor_source(), scope)
        extractor = scope["extract_run_targets"]

    source = _notebook(
        'label = "Users/u/child"\n'
        'dbutils.notebook.run(query_path, 0)\n'
    )
    source["cells"].append({"cell_type": "code", "source": "%run /Users/u/child"})
    loose_only = set()
    assert extractor(source, PARENT, loose_only=loose_only) == [
        "DxGy/Users/u/child", "/Users/u/child",
    ]
    assert loose_only == set()


class _GitJob:
    def __init__(self, source, missing_status=404):
        self.source = source
        self.missing_status = missing_status

    @staticmethod
    def parse_github_owner_repo(_url):
        return "example", "repo"

    def get_job(self, _job_id):
        return {"settings": {
            "git_source": {"git_url": "https://example.test/repo", "git_branch": "main"},
            "tasks": [{"task_key": "main", "notebook_task": {
                "notebook_path": PARENT, "source": "GIT",
            }}],
        }}

    def list_github_dir(self, *_args):
        return {}

    def export_notebook_from_git(self, path, *_args):
        if path == PARENT:
            return ExportedFile(json.dumps(_notebook(self.source)).encode(), True, ".ipynb")
        raise DbxExportError("git_path_unresolved" if self.missing_status == 404
                             else "github_fetch_failed", "lookup failed",
                             self.missing_status)


@pytest.mark.parametrize("status,explicit,skipped", [
    (404, False, True),
    (404, True, False),
    (503, False, False),
])
def test_exporter_only_skips_confirmed_loose_miss(status, explicit, skipped):
    source = 'timezone = "Asia/Kolkata"\ndbutils.notebook.run(query_path, 0)\n'
    if explicit:
        source += "%run ./Asia/Kolkata\n"
    messages = []
    graph = DependencyResolver(_GitJob(source, status), log=messages.append).resolve("1")
    if skipped:
        assert CANDIDATE not in graph.notebooks
        assert graph.skipped_loose_candidates == [{
            "path": CANDIDATE, "referenced_by": PARENT,
            "reason": "git_path_unresolved",
        }]
        assert any("[WARN][loose-candidate-skip]" in line and CANDIDATE in line
                   for line in messages)
        report = Report("run", "1", "/Workspace/export", "AI_TOOL_")
        report.skipped_loose_candidates.extend(graph.skipped_loose_candidates)
        assert report.to_dict()["skipped_loose_candidates"] == graph.skipped_loose_candidates
        assert "Loose candidates:   1 absent, skipped with warning" in pre_flight(
            graph, "/Workspace/export", "AI_TOOL_", "1",
        )
    else:
        assert graph.notebooks[CANDIDATE].export_error is not None
        assert graph.skipped_loose_candidates == []


def test_workspace_loose_miss_and_present_child_use_same_provenance_rule():
    parent = "/Jobs/main"
    missing = "/Jobs/Asia/Kolkata"
    present = "/Jobs/Queries/RU"
    source = ('timezone = "Asia/Kolkata"\n'
              'candidates = ["./Queries/RU"]\n'
              'dbutils.notebook.run(query_path, 0)\n')

    class WorkspaceJob:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{"task_key": "main", "notebook_task": {
                "notebook_path": parent,
            }}]}}

        def list_workspace_folder(self, _path):
            return []

        def get_workspace_status(self, path):
            return {"object_type": "NOTEBOOK"} if path == present else None

        def export_notebook(self, path):
            if path == missing:
                raise DbxExportError("not_found", "no object", 404)
            body = source if path == parent else "print('child')"
            return ExportedFile(json.dumps(_notebook(body)).encode(), True, ".ipynb")

    graph = DependencyResolver(WorkspaceJob(), log=lambda _message: None).resolve("1")
    assert missing not in graph.notebooks
    assert graph.notebooks[present].export_error is None
    assert graph.skipped_loose_candidates[0]["path"] == missing


def test_percent_run_namespace_does_not_reenqueue_skipped_candidate():
    config = "DxGy/cfg"
    child = "DxGy/Queries/RU"

    class GitConfigJob(_GitJob):
        def export_notebook_from_git(self, path, *_args):
            sources = {
                PARENT: "%run ./cfg\ndbutils.notebook.run(query_path, 0)",
                config: ('paths = {"ru": "./Queries/RU", '
                         '"timezone": "Asia/Kolkata"}\n'
                         'query_path = paths[selector]'),
                child: "print('child')",
            }
            if path not in sources:
                raise DbxExportError("git_path_unresolved", "no object", 404)
            return ExportedFile(
                json.dumps(_notebook(sources[path])).encode(), True, ".ipynb",
            )

    messages = []
    graph = DependencyResolver(GitConfigJob(""), log=messages.append).resolve("1")
    assert child in graph.notebooks
    assert graph.notebooks[child].export_error is None
    assert CANDIDATE not in graph.notebooks
    assert [finding["path"] for finding in graph.skipped_loose_candidates] == [CANDIDATE]
    assert sum("[WARN][loose-candidate-skip]" in line for line in messages) == 1


def test_exporter_direct_reference_from_second_parent_prevents_skip():
    second = "DxGy/other"

    class TwoTaskGitJob(_GitJob):
        def get_job(self, _job_id):
            job = super().get_job(_job_id)
            job["settings"]["tasks"].append({"task_key": "other", "notebook_task": {
                "notebook_path": second, "source": "GIT",
            }})
            return job

        def export_notebook_from_git(self, path, *_args):
            if path == second:
                return ExportedFile(
                    json.dumps(_notebook("%run ./Asia/Kolkata")).encode(),
                    True, ".ipynb",
                )
            return super().export_notebook_from_git(path, *_args)

    graph = DependencyResolver(TwoTaskGitJob(
        'timezone = "Asia/Kolkata"\ndbutils.notebook.run(query_path, 0)'
    ), log=lambda _message: None).resolve("1")
    assert graph.notebooks[CANDIDATE].loose_only is False
    assert graph.notebooks[CANDIDATE].export_error == "git_path_unresolved"
    assert graph.skipped_loose_candidates == []


def test_explicit_workspace_reference_is_not_skipped_after_loose_path_rewrite():
    child = "/Users/u/child"

    class GitWithWorkspaceChild(_GitJob):
        def export_notebook_from_git(self, path, *_args):
            if path == PARENT:
                source = _notebook(
                    'label = "Users/u/child"\n'
                    'dbutils.notebook.run(query_path, 0)\n'
                )
                source["cells"].append({"cell_type": "code", "source": f"%run {child}"})
                return ExportedFile(json.dumps(source).encode(), True, ".ipynb")
            return super().export_notebook_from_git(path, *_args)

        def get_workspace_status(self, _path):
            return None

        def export_notebook(self, path):
            assert path == child
            raise DbxExportError("not_found", "no object", 404)

    messages = []
    graph = DependencyResolver(GitWithWorkspaceChild(""), log=messages.append).resolve("1")
    assert graph.notebooks[child].loose_only is False
    assert graph.notebooks[child].export_error == "not_found"
    assert graph.skipped_loose_candidates == []
    assert any("[nb-export-fail]" in line and child in line for line in messages)


TASK = "/Workspace/Jobs/main.ipynb"
CHILD = "/Workspace/Jobs/child.ipynb"
LOOSE = "/Workspace/Jobs/Asia/Kolkata.ipynb"


@pytest.mark.parametrize("direct,nested", [
    (False, False),
    (True, False),
    (False, True),
])
def test_workflow_dag_skips_only_missing_loose_edge(monkeypatch, capsys, direct, nested):
    async def scan(_session, path, **_kwargs):
        if path == TASK:
            if nested:
                return True, [CHILD]
            return True, build_dag.RunTargets([LOOSE], [] if direct else [LOOSE])
        if path == CHILD:
            return True, build_dag.RunTargets([LOOSE], [LOOSE])
        assert path == LOOSE
        return False, []

    monkeypatch.setattr(build_dag_from_workflow, "scan_external_notebook", scan)
    result = asyncio.run(build_dag_from_workflow.discover_run_deps_for_tasks(
        object(), [{"task_key": "main", "notebook_path": TASK}],
    ))
    if direct:
        assert result["main"][0]["path"] == LOOSE
        assert result["main"][0]["exists"] is False
        assert build_dag.collect_missing_external(result) == [
            {"path": LOOSE, "referenced_by": ["main"]},
        ]
        assert "loose-candidate-skip" not in capsys.readouterr().out
    elif nested:
        assert result["main"][0]["path"] == CHILD
        assert result["main"][0]["nested_deps"] == []
        assert "[WARN][loose-candidate-skip]" in capsys.readouterr().out
    else:
        assert result == {"main": []}
        assert build_dag.collect_missing_external(result) == []
        assert "[WARN][loose-candidate-skip]" in capsys.readouterr().out


def test_workflow_dag_keeps_loose_candidate_when_found(monkeypatch):
    async def scan(_session, path, **_kwargs):
        if path == TASK:
            return True, build_dag.RunTargets([LOOSE], [LOOSE])
        assert path == LOOSE
        return True, []

    monkeypatch.setattr(build_dag_from_workflow, "scan_external_notebook", scan)
    result = asyncio.run(build_dag_from_workflow.discover_run_deps_for_tasks(
        object(), [{"task_key": "main", "notebook_path": TASK}],
    ))
    assert result["main"][0]["exists"] is True


def test_cluster_scan_carries_portable_loose_provenance(monkeypatch):
    captured = []

    async def fake_run(_session, code, timeout=60):
        captured.append(code)
        return {"outputs": [{"type": "stream", "text": json.dumps({
            "exists": True, "runs": [LOOSE.removesuffix(".ipynb")],
            "loose_only": [LOOSE.removesuffix(".ipynb")],
        }) + "\n"}]}

    monkeypatch.setattr(build_dag, "run", fake_run)
    exists, runs = asyncio.run(build_dag.scan_external_notebook(object(), TASK))
    assert exists is True
    assert runs == [LOOSE]
    assert runs.loose_only == {LOOSE}
    compile(captured[0], "<cluster-scan>", "exec")


def test_cluster_scan_error_does_not_become_absent_candidate(monkeypatch):
    calls = 0

    async def fake_run(_session, _code, timeout=60):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"outputs": [{"type": "stream", "text": "not JSON\n"}]}
        raise OSError("kernel unavailable")

    monkeypatch.setattr(build_dag, "run", fake_run)
    with pytest.raises(RuntimeError, match="Cannot verify notebook existence"):
        asyncio.run(build_dag.scan_external_notebook(object(), LOOSE))
