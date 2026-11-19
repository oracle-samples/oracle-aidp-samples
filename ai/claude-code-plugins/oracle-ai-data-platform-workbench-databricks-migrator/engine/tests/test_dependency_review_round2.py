"""Executed PR104 review repros: uncertain selectors and Python parse fidelity."""
import json

import pytest

from dbx_export.dbx_client import ExportedFile
from dbx_export.dependency_resolver import (
    DependencyResolver,
    extract_child_jobs,
    extract_local_resource_hints,
    extract_run_targets,
    extract_workspace_resource_paths,
    portable_run_extractor_source,
)


def notebook(*sources):
    return {"nbformat": 4, "nbformat_minor": 5, "metadata": {},
            "cells": [{"cell_type": "code", "source": source} for source in sources]}


@pytest.mark.parametrize("spelling", [
    "dbutils.notebook.run(config[selector], 0)",
    "target = config[selector]\ndbutils.notebook.run(target, 0)",
    "target = config[selector]\nalias: str = target\ndbutils.notebook.run(alias, 0)",
    "target = config[selector]\nfor alias in [target]:\n    dbutils.notebook.run(alias, 0)",
])
@pytest.mark.parametrize("portable", [False, True])
def test_unknown_selector_provenance_survives_assignment(spelling, portable):
    extract = extract_run_targets
    if portable:
        scope = {}
        exec(portable_run_extractor_source(), scope)
        extract = scope["extract_run_targets"]
    messages = []
    source = notebook(
        "config = {'a': 'Queries/segment_a', 'b': translate_path('Queries/segment_b'), "
        "'timeout': 3600, 'name': 'job', 'owner': 'team@example.com', "
        "'url': 'https://example.com/notebook', 'bad': './bad//path'}\n"
        "selector = dbutils.widgets.get('selector')\n",
        spelling,
    )
    assert set(extract(source, "/Jobs/main", log=messages.append)) == {
        "/Jobs/Queries/segment_a", "/Jobs/Queries/segment_b",
    }
    assert any("selector not statically known; taking 1 candidate path(s)" in m
               for m in messages)
    assert any("dropped non-path values" in m and "3600" in m and "job" in m
               and "team@example.com" in m for m in messages)
    assert not any("resolved via bounded" in m for m in messages)


@pytest.mark.parametrize("prefix", ["", "%run ./cfg\n"])
@pytest.mark.parametrize("expression", ["(a\n %b)", "('fmt %s'\n %prefix)"])
def test_no_space_modulo_does_not_hide_later_child(prefix, expression):
    source = notebook(prefix + "unrelated = " + expression +
                      "\nchild = './child6'\ndbutils.notebook.run(child, 0)\n")
    assert "/Jobs/child6" in extract_run_targets(source, "/Jobs/main")
    assert "./child6" not in extract_local_resource_hints(source)


@pytest.mark.parametrize("prefix", ["", "%run ./cfg\n"])
def test_no_space_format_does_not_fabricate_notebook(prefix):
    source = notebook(prefix + "path = ('%s/child_fmt'\n %prefix\n)\n"
                      "dbutils.notebook.run(path, 0)\n")
    assert extract_run_targets(source, "/Jobs/main") == (
        ["/Jobs/cfg"] if prefix else []
    )


def test_magic_tolerant_parsing_covers_resource_and_child_job_consumers():
    source = notebook("%run ./cfg\n"
                      "from helper_pkg import thing\n"
                      "with open('/Workspace/Shared/conf/settings.yaml') as handle:\n"
                      "    settings = handle.read()\n"
                      "dbutils.jobs.runNow(123)\n")
    assert extract_child_jobs(source) == ["123"]
    assert "helper_pkg" in extract_local_resource_hints(source)
    assert extract_workspace_resource_paths(source) == {
        "/Workspace/Shared/conf/settings.yaml",
    }


def test_known_selector_does_not_enable_unrelated_candidates():
    messages = []
    source = notebook("config = {'a': 'child'}\n"
                      "target = config['a']\n"
                      "unrelated = 'Other/asset'\n"
                      "dbutils.notebook.run(target, 0)\n")
    assert extract_run_targets(source, "/Jobs/main", log=messages.append) == ["/Jobs/child"]
    assert not any("candidate path" in m for m in messages)


def test_known_map_field_does_not_inherit_unrelated_metadata_selector():
    source = notebook("config = {'path': './nb_a', 'metadata': metadata[unknown]}\n"
                      "unrelated = './nb_b'\n"
                      "dbutils.notebook.run(config['path'], 0)\n")
    messages = []
    assert extract_run_targets(source, "/Jobs/main", log=messages.append) == ["/Jobs/nb_a"]
    assert not any("selector not statically known" in m for m in messages)


def test_selected_map_field_keeps_its_own_candidate_provenance():
    source = notebook("paths = {'a': './nb_a', 'b': translate_path('./nb_b')}\n"
                      "config = {'path': paths[unknown], 'metadata': 'job'}\n"
                      "dbutils.notebook.run(config['path'], 0)\n")
    messages = []
    assert set(extract_run_targets(source, "/Jobs/main", log=messages.append)) == {
        "/Jobs/nb_a", "/Jobs/nb_b",
    }
    assert any("selector not statically known" in m for m in messages)


@pytest.mark.parametrize("reverse", [False, True])
def test_equal_mapping_values_retain_uncertainty_in_either_assignment_order(reverse):
    assignments = ["config = {'path': './nb_a'}",
                   "config = {'path': paths[unknown]}"]
    if reverse:
        assignments.reverse()
    source = notebook("paths = {'a': './nb_a', 'b': translate_path('./nb_b')}\n"
                      + "\n".join(assignments) +
                      "\ndbutils.notebook.run(config['path'], 0)\n")
    messages = []
    assert set(extract_run_targets(source, "/Jobs/main", log=messages.append)) == {
        "/Jobs/nb_a", "/Jobs/nb_b",
    }
    assert any("selector not statically known" in m for m in messages)


@pytest.mark.parametrize("dispatch", [
    "alias = target\ndbutils.notebook.run(alias, 0)\n",
    "for alias in [target]:\n    dbutils.notebook.run(alias, 0)\n",
])
def test_resolver_preserves_candidate_provenance_from_percent_run_config(dispatch):
    notebooks = {
        "/Jobs/main": notebook("%run ./cfg\n" + dispatch),
        "/Jobs/cfg": notebook(
            "config = {'a': 'Queries/segment_a', 'metadata': 3600, "
            "'b': translate_path('Queries/segment_b')}\n"
            "selector = dbutils.widgets.get('selector')\n"
            "target = config[selector]\n"),
        "/Jobs/Queries/segment_a": notebook("print('child')\n"),
        "/Jobs/Queries/segment_b": notebook("print('opaque-child')\n"),
    }
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
            return ExportedFile(json.dumps(notebooks[path]).encode(), True, ".ipynb")

        def get_workspace_status(self, _path):
            return {"object_type": "NOTEBOOK"}

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx(), log=messages.append).resolve("1")
    assert "/Jobs/Queries/segment_a" in graph.notebooks
    assert "/Jobs/Queries/segment_b" in graph.notebooks
    assert graph.notebooks["/Jobs/Queries/segment_a"].referenced_by == ["/Jobs/main"]
    assert any("selector not statically known" in m and "/Jobs/main" in m for m in messages)


@pytest.mark.parametrize("candidate", ["./x", "a/b", "My Config/child", "../x"])
def test_short_and_bare_relative_candidates_share_the_loose_scan_contract(candidate):
    source = notebook("config = {'candidate': " + repr(candidate) + "}\n"
                      "target = config[unknown_selector]\n"
                      "dbutils.notebook.run(target, 0)\n")
    loose = notebook("candidate = " + repr(candidate) + "\n"
                     "dbutils.notebook.run(unknown_target, 0)\n")
    expected = extract_run_targets(notebook("dbutils.notebook.run(" + repr(candidate) + ", 0)"),
                                   "/Jobs/main")
    assert extract_run_targets(source, "/Jobs/main") == expected
    assert extract_run_targets(loose, "/Jobs/main") == expected


@pytest.mark.parametrize("candidate", ["./bad//path", "a/http://host", "https://host/path",
                                       "folder/data.csv", "folder/", "folder/{dynamic}"])
def test_non_notebook_values_are_filtered_equally_for_selector_and_loose_scan(candidate):
    source = notebook("config = {'metadata': " + repr(candidate) + "}\n"
                      "target = config[unknown_selector]\n"
                      "dbutils.notebook.run(target, 0)\n")
    loose = notebook("candidate = " + repr(candidate) + "\n"
                     "dbutils.notebook.run(unknown_target, 0)\n")
    messages = []
    assert extract_run_targets(source, "/Jobs/main", log=messages.append) == []
    assert extract_run_targets(loose, "/Jobs/main") == []
    assert any("selector not statically known; taking 0 candidate path(s)" in m
               and candidate in m for m in messages)
