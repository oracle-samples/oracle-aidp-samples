"""PR104 P1: unknown selectors must retain generic absolute workspace notebooks."""

import json

import pytest

from dbx_export.__main__ import _notebooks_reachable_from
from dbx_export.dbx_client import DbxExportError, ExportedFile
from dbx_export.dependency_resolver import (
    DependencyResolver,
    extract_run_targets,
    portable_run_extractor_source,
)


def _notebook(source):
    return {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"cell_type": "code", "source": source},
    ]}


@pytest.fixture(params=[False, True], ids=["local", "portable"])
def extractor(request):
    if request.param:
        scope = {}
        exec(portable_run_extractor_source(), scope)
        return scope["extract_run_targets"]
    return extract_run_targets


@pytest.mark.parametrize("candidate,expected", [
    ("/Jobs/child", "/Jobs/child"),
    ("/Optimiser_Utils/Randomizer trigger", "/Optimiser_Utils/Randomizer trigger"),
    ("/RootNotebook", "/RootNotebook"),
    ("/VolumesReports/child", "/VolumesReports/child"),
    ("/mnt_jobs/child", "/mnt_jobs/child"),
    ("/tmp_utils/child", "/tmp_utils/child"),
    ("/FileStoreHelpers/child", "/FileStoreHelpers/child"),
    ("/user/team/child", "/user/team/child"),
    ("/user/hive/warehouseReports/child", "/user/hive/warehouseReports/child"),
    ("/Workspace/Volumes/catalog/schema/notebook", "/Volumes/catalog/schema/notebook"),
    ("/Workspace/tmp/child", "/tmp/child"),
    ("/tmp/../Jobs/child", "/Jobs/child"),
    ("./mnt/child", "/Jobs/mnt/child"),
    ("mnt/child", "/Jobs/mnt/child"),
])
@pytest.mark.parametrize("call", [
    "dbutils.notebook.run(config[selector], 0)",
    "target = config[selector]\nalias = target\ndbutils.notebook.run(alias, 0)",
])
def test_unknown_selector_retains_generic_workspace_paths(extractor, candidate, expected, call):
    source = _notebook("config = {'child': " + repr(candidate) + "}\n"
                       "selector = dbutils.widgets.get('selector')\n" + call)
    messages = []
    assert extractor(source, "/Jobs/main", log=messages.append) == [expected]
    assert any("selector not statically known; taking 1 candidate path(s)" in line
               for line in messages)
    assert not any("resolved via bounded" in line for line in messages)


_DATA_ROOTS = ["/Volumes", "/volumes", "/Volume", "/volume", "/dbfs", "/mnt",
               "/FileStore", "/databricks-datasets", "/databricks-results", "/tmp",
               "/local_disk0", "/databricks", "/user/hive/warehouse"]


@pytest.mark.parametrize("candidate", _DATA_ROOTS + [root + "/data" for root in _DATA_ROOTS] + [
    "/Jobs/../mnt/data", "/Jobs/../Volumes", "/Jobs/../tmp/data", "/Workspace/../mnt/data",
    "https://example.com/child", "s3://bucket/child", "dbfs:/mnt/data",
    "/Jobs/model.json", "/Jobs/archive.zip", "/Jobs/image.png", "/Jobs/output.csv",
    "/Jobs/", "/Jobs/{runtime}", "/Jobs//child", "/", "/Jobs/..",
])
def test_candidate_and_loose_scans_exclude_data_paths_and_assets(extractor, candidate):
    source = _notebook("config = {'value': " + repr(candidate) + "}\n"
                       "target = config[selector]\ndbutils.notebook.run(target, 0)")
    loose = _notebook("candidate = " + repr(candidate) + "\n"
                      "dbutils.notebook.run(runtime_target, 0)")
    messages = []
    assert extractor(source, "/Jobs/main", log=messages.append) == []
    assert extractor(loose, "/Jobs/main") == []
    assert any("taking 0 candidate path(s)" in line and candidate in line
               for line in messages)


@pytest.mark.parametrize("candidate", ["/Jobs/child", "/RootNotebook", "simple_child",
                                       "/mnt/explicit_notebook"])
def test_known_selector_keeps_existing_resolution_without_loose_scan(extractor, candidate):
    source = _notebook("config = {'selected': " + repr(candidate) +
                       ", 'other': '/Other/unrelated'}\n"
                       "target = config['selected']\ndbutils.notebook.run(target, 0)")
    expected = candidate if candidate.startswith("/") else "/Jobs/" + candidate
    messages = []
    assert extractor(source, "/Jobs/main", log=messages.append) == [expected]
    assert not any("selector not statically known" in line for line in messages)


@pytest.mark.parametrize("missing_child", [False, True])
def test_resolver_imported_config_alias_keeps_absolute_children_reachable(missing_child):
    first = "/Jobs/child"
    second = "/Optimiser_Utils/Randomizer trigger"
    notebooks = {
        "/Jobs/main": _notebook("%run ./Config/cfg\n"
                                "alias = target\ndbutils.notebook.run(alias, 0)\n"),
        "/Jobs/Config/cfg": _notebook(
            "config = {'a': " + repr(first) + ", 'b': " + repr(second) + ", 'label': 'job'}\n"
            "selector = dbutils.widgets.get('selector')\ntarget = config[selector]\n"),
        first: _notebook("print('first')"),
        second: _notebook("print('second')"),
    }
    requested = []
    messages = []

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{"task_key": "main", "notebook_task": {
                "notebook_path": "/Jobs/main",
            }}]}}

        def export_notebook(self, path):
            requested.append(path)
            if missing_child and path == second:
                raise DbxExportError("not_found", "missing notebook fixture", 404)
            return ExportedFile(json.dumps(notebooks[path]).encode(), True, ".ipynb")

        def get_workspace_status(self, _path):
            return {"object_type": "NOTEBOOK"}

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx(), log=messages.append).resolve("1")
    assert {first, second} <= set(requested)
    reachable = _notebooks_reachable_from(graph, {"/Jobs/main"})
    assert first in reachable
    assert graph.notebooks[first].referenced_by == ["/Jobs/main"]
    assert graph.notebooks[first].export_error is None
    if missing_child:
        assert second not in reachable
        assert second not in graph.notebooks
        assert graph.skipped_loose_candidates[0]["path"] == second
        assert any("[WARN][loose-candidate-skip] " + second in line
                   for line in messages)
    else:
        assert second in reachable
        assert graph.notebooks[second].referenced_by == ["/Jobs/main"]
        assert graph.notebooks[second].export_error is None
