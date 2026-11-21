"""Regression for Git-backed notebooks listing model YAML via GitHub at runtime."""

import json

import pytest

from dbx_export.__main__ import phase2_upload_notebooks
from dbx_export.dbx_client import ExportedFile
from dbx_export.dependency_resolver import (
    DependencyResolver,
    extract_local_resource_hints,
)
from dbx_export.report import Report


def _notebook(source):
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [{
            "cell_type": "code",
            "source": source.splitlines(keepends=True),
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        }],
    }


def _dynamic_github_model_source():
    return (
        'REPO_OWNER = "owner"\n'
        'REPO_NAME = "repo"\n'
        'TARGET_FOLDER = "models"\n'
        'BRANCH = dbutils.widgets.get("branch")\n'
        'models_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{TARGET_FOLDER}?ref={BRANCH}"\n'
        'directories = requests.get(models_url).json()\n'
        'for item in directories:\n'
        '    subfolder_path = item["path"]\n'
        '    model = {"yaml_path": f"{subfolder_path}/params.yaml"}\n'
        '    with open("../" + model["yaml_path"], "r") as stream:\n'
        '        params = stream.read()\n'
    )


@pytest.mark.parametrize("opener", ["open", "builtins.open", "io.open"])
@pytest.mark.parametrize("prefix", ["../", "./"])
@pytest.mark.parametrize("direct_get", [False, True])
@pytest.mark.parametrize("arguments, expect_resources", [
    ("positional_read", True),
    ("keyword_url", True),
    ("keyword_file_mode", True),
    ("keyword_write", False),
    ("positional_write", False),
])
def test_dynamic_git_model_yaml_is_discovered_and_uploaded(
    opener, prefix, direct_get, arguments, expect_resources,
):
    source = _dynamic_github_model_source().replace('with open(', f'with {opener}(')
    source = source.replace('"../" +', f'"{prefix}" +')
    if direct_get:
        source = source.replace('requests.get(models_url)',
            'requests.get(f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{TARGET_FOLDER}?ref={BRANCH}")')
    if arguments == "keyword_url":
        source = source.replace('requests.get(', 'requests.get(url=')
    elif arguments == "keyword_file_mode":
        source = source.replace(f'with {opener}(', f'with {opener}(file=')
        source = source.replace(', "r") as stream:', ', mode="r") as stream:')
    elif arguments == "keyword_write":
        source = source.replace(', "r") as stream:', ', mode="w") as stream:')
    elif arguments == "positional_write":
        source = source.replace(', "r") as stream:', ', "w") as stream:')
    notebook = _notebook(source)
    paths = {
        "models/model_a/params.yaml": b"name: model_a\n",
        "models/model_b/params.yaml": b"name: model_b\n",
    }
    expected_paths = paths if expect_resources else {}

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return "owner", "repo"

        def get_job(self, _job_id):
            return {"settings": {
                "git_source": {
                    "git_url": "https://github.com/owner/repo.git",
                    "git_branch": "test-branch",
                },
                "tasks": [{
                    "task_key": "init",
                    "notebook_task": {
                        "source": "GIT",
                        "notebook_path": "notebooks/init",
                    },
                }],
            }}

        def export_notebook_from_git(self, path, *_context):
            assert path == "notebooks/init"
            return ExportedFile(json.dumps(notebook).encode(), True, ".ipynb")

        def export_resource_from_git(self, path, *_context):
            return ExportedFile(paths[path], False, ".yaml")

        def list_github_dir(self, _owner, _repo, directory, _branch):
            return {"models": "models"} if directory == "" else {}

        def list_github_dir_entries(self, _owner, _repo, directory, _branch):
            if directory == "models":
                return [
                    {"name": "model_a", "type": "dir"},
                    {"name": "model_b", "type": "dir"},
                ]
            if directory in {"models/model_a", "models/model_b"}:
                return [{"name": "params.yaml", "type": "file"}]
            return []

    assert extract_local_resource_hints(notebook) == ({"models"} if expect_resources else set())
    resolver = DependencyResolver(FakeDbx(), log=lambda _message: None)
    graph = resolver.resolve("1")
    repeated = resolver.resolve("1")
    assert set(repeated.notebooks) == set(graph.notebooks)
    assert {path for path, node in graph.notebooks.items() if node.role == "resource"} == set(expected_paths)

    class FakeAidp:
        def __init__(self):
            self.notebooks = {}
            self.files = {}

        def get_notebook(self, path):
            return self.notebooks.get(path)

        def put_notebook(self, path, value):
            self.notebooks[path] = value

        def object_exists(self, path):
            return path in self.files

        def upload_file(self, path, content, *, overwrite):
            assert not overwrite
            self.files[path] = content

        def download_file(self, path):
            return self.files[path]

    aidp = FakeAidp()
    report = Report(run_id="test", dbx_root_job_id="1", export_output="/Workspace/export", prefix="")
    _, failed_tasks = phase2_upload_notebooks(
        aidp, graph, "/Workspace/export", False, "test", "1", report,
    )

    assert failed_tasks == []
    notebook_path = next(iter(aidp.notebooks))
    assert notebook_path.endswith("/GIT/notebooks/init.ipynb")
    assert {
        path.removeprefix(notebook_path.removesuffix("notebooks/init.ipynb")): content
        for path, content in aidp.files.items()
    } == expected_paths
    assert not report.errors


@pytest.mark.parametrize("old, new, expected", [
    ('directories = requests.get(models_url).json()', 'directories = []', set()),
    ('with open("../" + model["yaml_path"], "r") as stream:',
     'with contextlib.nullcontext(model["yaml_path"]) as stream:', set()),
    ('TARGET_FOLDER = "models"',
     'TARGET_FOLDER = dbutils.widgets.get("folder")', set()),
    ('with open("../" + model["yaml_path"], "r") as stream:',
     'with open("../" + model["yaml_path"], "w") as stream:', set()),
    ('https://api.github.com/repos/', 'https://example.com/repos/', set()),
    ('TARGET_FOLDER = "models"', 'TARGET_FOLDER = "../models"', set()),
    ('TARGET_FOLDER = "models"', 'TARGET_FOLDER = "models/subdir"', {"models"}),
])
def test_github_contents_hint_requires_a_known_root_and_dynamic_local_read(
    old, new, expected,
):
    source = _dynamic_github_model_source().replace(old, new)
    assert extract_local_resource_hints(_notebook(source)) == expected


@pytest.mark.parametrize("include_models", [True, False])
def test_workspace_resource_discovery_uses_same_static_root_without_guessing(
    include_models,
):
    notebook = _notebook(_dynamic_github_model_source())
    resource = "/Jobs/models/model_a/params.yaml"

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{
                "task_key": "init",
                "notebook_task": {"notebook_path": "/Jobs/init"},
            }]}}

        def export_notebook(self, path):
            assert path == "/Jobs/init"
            return ExportedFile(json.dumps(notebook).encode(), True, ".ipynb")

        def export_workspace_resource(self, path):
            assert path == resource
            return ExportedFile(b"name: model_a\n", False, ".yaml")

        def list_workspace_folder(self, path):
            if path == "/Jobs":
                return ([{"path": "/Jobs/models", "object_type": "DIRECTORY"}]
                        if include_models else [])
            if path == "/Jobs/models":
                return [{"path": "/Jobs/models/model_a", "object_type": "DIRECTORY"}]
            if path == "/Jobs/models/model_a":
                return [{"path": resource, "object_type": "FILE"}]
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _message: None).resolve("1")

    actual_resources = {
        path for path, node in graph.notebooks.items() if node.role == "resource"
    }
    assert actual_resources == ({resource} if include_models else set())
    if include_models:
        assert graph.notebooks[resource].content == b"name: model_a\n"
