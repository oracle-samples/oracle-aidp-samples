"""Run c6cf9ac8 (2026-10-16): task parent had a space absent from Git.

Scope: preserve exact Git paths and try a trimmed parent only after 404.
Cover extensionless and explicit filenames without changing the basename.
"""

import json
from urllib.parse import unquote, urlsplit

import pytest
import requests

from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError
from dbx_export.dependency_resolver import DependencyResolver
from dbx_export.__main__ import pre_flight


def _client(monkeypatch, parent, filename, *, fetch_status=200, listed=True,
            directory_status=200, notebook_source=False):
    calls = []
    body = (json.dumps({"nbformat": 4, "nbformat_minor": 5,
                        "metadata": {}, "cells": []}).encode()
            if filename.endswith(".ipynb") else b"VALUE = 1\n")
    if notebook_source and filename.endswith(".py"):
        body = b"# Databricks notebook source\nVALUE = 1\n"
    elif notebook_source and filename.endswith(".scala"):
        body = b"// Databricks notebook source\nval value = 1\n"

    def get(url, **kwargs):
        path = unquote(urlsplit(url).path.split("/contents/", 1)[1])
        calls.append(path)
        response = requests.Response()
        response.status_code = 404
        response._content = b'{"message":"Not Found"}'
        if path == parent:
            response.status_code = directory_status
            response._content = json.dumps(
                [{"name": filename, "type": "file"}] if listed else []
            ).encode()
        elif path == (f"{parent}/{filename}" if parent else filename):
            response.status_code = fetch_status
            response._content = body if fetch_status == 200 else b"fetch failed"
        return response

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", get)
    return DbxClient(DbxConfig(proxy="proxy.example:8080", proxy_max_retries=1)), calls, body


@pytest.mark.parametrize("parent", ["Controllers ", " Controllers", " Controllers ", "Controllers"])
@pytest.mark.parametrize("extension", ["", ".py", ".ipynb"])
def test_git_notebook_parent_is_trimmed_before_listing_and_fetch(
    monkeypatch, capsys, parent, extension,
):
    filename = "Base Controller" + (extension or ".py")
    client, calls, body = _client(monkeypatch, "Controllers", filename)
    exported = client.export_notebook_from_git(
        f"{parent}/Base Controller{extension}", "owner", "repo", "main",
    )
    assert exported.content == body
    assert exported.resolved_git_path == f"Controllers/{filename}"
    exact_miss = []
    if parent != parent.strip():
        exact_miss = [parent if not extension else f"{parent}/{filename}"]
    assert calls == exact_miss + (["Controllers"] if not extension else []) + [f"Controllers/{filename}"]
    output = capsys.readouterr().out
    if parent != parent.strip():
        assert "[git-parent-trim]" in output
        assert repr(parent) in output
        assert repr("Controllers") in output
    else:
        assert "[git-parent-trim]" not in output


@pytest.mark.parametrize("path,parent,filename", [
    ("Root Folder/Sub Folder /Base", "Root Folder/Sub Folder", "Base.py"),
    ("Base", "", "Base.py"),
    ("Base.py", "", "Base.py"),
    ("Controllers / Base", "Controllers", " Base.py"),
    ("Controllers / Base.py", "Controllers", " Base.py"),
    ("Controllers /Base ", "Controllers", "Base .py"),
])
def test_parent_trim_preserves_internal_spaces_and_notebook_name(
    monkeypatch, path, parent, filename,
):
    client, calls, body = _client(monkeypatch, parent, filename)
    assert client.export_notebook_from_git(path, "owner", "repo", "main").content == body
    assert calls[-1] == (f"{parent}/{filename}" if parent else filename)


@pytest.mark.parametrize("extension", ["", ".py"])
def test_job_task_resolution_uses_parent_trim(monkeypatch, extension):
    client, calls, body = _client(monkeypatch, "Master Controller", "Base Controller.py")
    source_path = f"Master Controller /Base Controller{extension}"
    monkeypatch.setattr(client, "get_job", lambda _job: {"settings": {
        "git_source": {"git_url": "https://github.com/owner/repo", "git_branch": "main"},
        "tasks": [{"task_key": "controller", "notebook_task": {
            "source": "GIT", "notebook_path": source_path,
        }}],
    }})
    graph = DependencyResolver(client, log=lambda _message: None).resolve("1")
    node = graph.notebooks[source_path]
    assert node.export_error is None
    assert node.content == body
    assert node.referenced_by == ["job:1/task:controller"]
    assert calls[-1] == "Master Controller/Base Controller.py"


@pytest.mark.parametrize("extension", ["", ".py"])
@pytest.mark.parametrize("status", [403, 404])
def test_trimmed_notebook_fetch_still_fails_loudly(monkeypatch, extension, status):
    client, _, _ = _client(monkeypatch, "Controllers", "Base.py", fetch_status=status)
    with pytest.raises(DbxExportError) as caught:
        client.export_notebook_from_git(f"Controllers /Base{extension}", "owner", "repo", "main")
    assert caught.value.status == status
    assert caught.value.reason == ("git_not_found" if status == 404 else "github_fetch_failed")


def test_trimmed_parent_without_matching_notebook_still_fails(monkeypatch):
    client, calls, _ = _client(monkeypatch, "Controllers", "Base.py", listed=False)
    with pytest.raises(DbxExportError) as caught:
        client.export_notebook_from_git("Controllers /Base", "owner", "repo", "main")
    assert caught.value.reason == "git_path_unresolved"
    assert calls == ["Controllers ", "Controllers"]


def _resolve_job_task(monkeypatch, client, source_path, route):
    """Exercise real routing/discovery with all service responses mocked."""
    notebook_task = {"task_key": "controller", "notebook_task": {
        "source": "GIT", "notebook_path": source_path,
    }}
    settings = {
        "name": "Controller",
        "git_source": {"git_url": "https://github.com/owner/repo", "git_branch": "main"},
        "tasks": [notebook_task],
    }
    jobs = {"1": {"settings": settings}}
    expected_ref = "job:1/task:controller"
    if route == "for_each":
        settings["tasks"] = [{"task_key": "controller", "for_each_task": {
            "inputs": "[1]", "task": notebook_task,
        }}]
        expected_ref += "/for_each"
    elif route == "child_job":
        jobs = {
            "1": {"settings": {"name": "Root", "tasks": [{
                "task_key": "child", "run_job_task": {"job_id": 2},
            }]}},
            "2": {"settings": settings},
        }
        expected_ref = "job:2/task:controller"
    monkeypatch.setattr(client, "get_job", lambda job_id: jobs[str(job_id)])
    messages = []
    graph = DependencyResolver(client, log=messages.append).resolve("1")
    node = graph.notebooks[source_path.strip()]
    assert node.role == "task"
    assert [ref for ref in node.referenced_by if ref.startswith("job:")] == [expected_ref]
    assert (node.git_owner, node.git_repo, node.git_branch) == ("owner", "repo", "main")
    assert not any(job.fetch_error for job in graph.jobs.values())
    return graph, node, messages


# Contract grid: whitespace sides x lookup/format/content x job entry point.
# Leading whitespace is already removed by job intake's whole-path strip;
# trailing parent whitespace must still reach the new client correction.
@pytest.mark.parametrize("parent", ["Controllers", " Controllers", "Controllers ", " Controllers "])
@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension,filename,notebook_source", [
    ("", "Base.py", False),
    ("", "Base.py", True),
    ("", "Base.ipynb", False),
    ("", "Base.scala", False),
    ("", "Base.scala", True),
    ("", "Base.sql", False),
    ("", "Base.r", False),
    (".py", "Base.py", False),
    (".py", "Base.py", True),
    (".scala", "Base.scala", False),
    (".scala", "Base.scala", True),
    (".ipynb", "Base.ipynb", False),
    (".sql", "Base.sql", False),
    (".r", "Base.r", False),
])
def test_job_parent_trim_success_contract_grid(
    monkeypatch, capsys, parent, route, extension, filename, notebook_source,
):
    client, calls, body = _client(
        monkeypatch, "Controllers", filename, notebook_source=notebook_source,
    )
    graph, node, _ = _resolve_job_task(
        monkeypatch, client, f"{parent}/Base{extension}", route,
    )
    assert node.export_error is None
    assert node.content is not None
    is_notebook = notebook_source or filename.endswith(".ipynb")
    assert node.is_notebook == is_notebook
    assert (node.nb_dict is not None) == is_notebook
    assert node.final_extension == (".ipynb" if is_notebook else filename[filename.rfind("."):])
    if not notebook_source:
        assert node.content == body
    else:
        expected_code = "val value = 1" if filename.endswith(".scala") else "VALUE = 1"
        assert expected_code in node.content.decode()
    assert f"Controllers/{filename}" in calls
    assert node.resolved_git_path == f"Controllers/{filename}"
    # Job intake already removes leading whole-path whitespace.
    exact_miss = []
    if parent.endswith(" "):
        exact_miss = ["Controllers " if not extension else f"Controllers /{filename}"]
    expected_fetch = exact_miss + (["Controllers"] if not extension else []) + [f"Controllers/{filename}"]
    assert calls[:len(expected_fetch)] == expected_fetch
    assert "1 OK, 0 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")
    # Intake removes leading path whitespace before the client sees it.
    assert ("[git-parent-trim]" in capsys.readouterr().out) == parent.endswith(" ")


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("source_path,parent,filename", [
    ("Base", "", "Base.py"),
    ("Base.py", "", "Base.py"),
    ("/Base.py", "", "Base.py"),
    ("Root Folder/Sub Folder /Base", "Root Folder/Sub Folder", "Base.py"),
    ("Root Folder/Sub Folder /Base.ipynb", "Root Folder/Sub Folder", "Base.ipynb"),
    ("Controllers / Base", "Controllers", " Base.py"),
    ("Controllers / Base.py", "Controllers", " Base.py"),
    ("Controllers /Base .py", "Controllers", "Base .py"),
    ("Root Folder /Sub Folder/Base.py", "Root Folder /Sub Folder", "Base.py"),
])
def test_job_parent_trim_preserves_path_and_basename_contract(
    monkeypatch, route, source_path, parent, filename,
):
    client, calls, body = _client(monkeypatch, parent, filename)
    graph, node, _ = _resolve_job_task(monkeypatch, client, source_path, route)
    assert node.export_error is None
    assert node.content == body
    assert node.dbx_path == source_path.strip()
    expected_fetch = f"{parent}/{filename}" if parent else filename
    assert expected_fetch in calls
    assert node.resolved_git_path == expected_fetch
    assert "1 OK, 0 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("parent", ["Controllers", " Controllers", "Controllers ", " Controllers "])
@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("stage,status,listed,reason", [
    ("directory", 403, True, "github_list_failed"),
    ("directory", 404, True, "git_path_unresolved"),
    ("directory", 503, True, "github_list_failed"),
    ("directory", 200, False, "git_path_unresolved"),
    ("fetch", 403, True, "github_fetch_failed"),
    ("fetch", 404, True, "git_not_found"),
    ("fetch", 503, True, "github_fetch_failed"),
])
def test_job_parent_trim_failure_contract_grid(
    monkeypatch, parent, route, stage, status, listed, reason,
):
    client, calls, _ = _client(
        monkeypatch, "Controllers", "Base.py", listed=listed,
        directory_status=status if stage == "directory" else 200,
        fetch_status=status if stage == "fetch" else 200,
    )
    graph, node, messages = _resolve_job_task(
        monkeypatch, client, f"{parent}/Base", route,
    )
    assert node.export_error == reason
    assert node.error_detail
    assert node.content is None
    assert node.nb_dict is None
    assert node.resolved_git_path is None
    assert any("[nb-export-fail]" in message and reason in message for message in messages)
    expected_calls = (["Controllers "] if parent.endswith(" ") else []) + ["Controllers"]
    if stage == "fetch":
        expected_calls.append("Controllers/Base.py")
    assert calls == expected_calls
    assert "0 OK, 1 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("parent", ["Controllers", " Controllers", "Controllers ", " Controllers "])
@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension", [".py", ".ipynb", ".scala", ".sql", ".r"])
@pytest.mark.parametrize("status,reason", [(403, "github_fetch_failed"), (404, "git_not_found"), (503, "github_fetch_failed")])
def test_job_explicit_extension_fetch_failure_contract_grid(
    monkeypatch, parent, route, extension, status, reason,
):
    client, calls, _ = _client(monkeypatch, "Controllers", "Base" + extension, fetch_status=status)
    graph, node, messages = _resolve_job_task(
        monkeypatch, client, f"{parent}/Base{extension}", route,
    )
    assert node.export_error == reason
    assert node.error_detail
    assert node.content is None
    assert node.nb_dict is None
    assert node.resolved_git_path is None
    exact_miss = ["Controllers /Base" + extension] if parent.endswith(" ") else []
    assert calls == exact_miss + ["Controllers/Base" + extension]
    assert any("[nb-export-fail]" in message and reason in message for message in messages)
    assert "0 OK, 1 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")



def _repository_client(monkeypatch, files, *, statuses=None, transport_errors=None):
    """Serve a bounded in-memory Git tree; no service requests escape."""
    calls = []
    statuses = statuses or {}
    transport_errors = transport_errors or {}

    def get(url, **kwargs):
        path = unquote(urlsplit(url).path.split("/contents/", 1)[1])
        calls.append(path)
        if path in transport_errors:
            raise transport_errors[path]("synthetic transport failure")
        response = requests.Response()
        response.status_code = statuses.get(path, 200)
        if response.status_code != 200:
            response._content = b'{"message":"synthetic HTTP failure"}'
        elif path in files:
            response._content = files[path]
        else:
            entries = [{"name": name.rpartition("/")[2], "type": "file"}
                       for name in files if name.rpartition("/")[0] == path]
            response.status_code = 200 if entries else 404
            response._content = json.dumps(entries).encode()
        return response

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", get)
    return DbxClient(DbxConfig(proxy="proxy.example:8080", proxy_max_retries=1)), calls


def _marked_file(extension, marker):
    if extension == ".ipynb":
        return json.dumps({"nbformat": 4, "nbformat_minor": 5,
                           "metadata": {"origin": marker}, "cells": []}).encode()
    return f"# origin: {marker}\nVALUE = 1\n".encode()


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension", ["", ".py", ".ipynb", ".scala", ".sql", ".r"])
@pytest.mark.parametrize("state", ["both", "exact_only", "trimmed_only", "neither"])
def test_exact_first_job_directory_state_matrix(monkeypatch, capsys, route, extension, state):
    filename = "Base" + (extension or ".py")
    exact_path = "Controllers /" + filename
    trimmed_path = "Controllers/" + filename
    exact = _marked_file(extension or ".py", "exact")
    trimmed = _marked_file(extension or ".py", "trimmed")
    files = {}
    if state in ("both", "exact_only"):
        files[exact_path] = exact
    if state in ("both", "trimmed_only"):
        files[trimmed_path] = trimmed
    client, calls = _repository_client(monkeypatch, files)
    graph, node, _ = _resolve_job_task(monkeypatch, client, "Controllers /Base" + extension, route)
    output = capsys.readouterr().out
    assert calls[0] == ("Controllers " if not extension else exact_path)
    if state in ("both", "exact_only"):
        assert node.content == exact
        assert node.resolved_git_path == exact_path
        assert node.export_error is None
        assert trimmed_path not in calls
        assert "Controllers" not in calls
        assert "[git-parent-trim]" not in output
        assert "1 OK, 0 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")
    elif state == "trimmed_only":
        assert node.content == trimmed
        assert node.resolved_git_path == trimmed_path
        assert node.export_error is None
        assert trimmed_path in calls
        assert output.count("[git-parent-trim]") == 1
        assert "1 OK, 0 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")
    else:
        assert node.content is None
        assert node.resolved_git_path is None
        assert node.export_error == ("git_path_unresolved" if not extension else "git_not_found")
        assert output.count("[git-parent-trim]") == 1
        assert calls == (["Controllers ", "Controllers"] if not extension else [exact_path, trimmed_path])
        assert "0 OK, 1 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension", ["", ".py"])
@pytest.mark.parametrize("status", [401, 403, 429, 500, 503])
def test_exact_non_404_failure_never_tries_trimmed_parent(monkeypatch, capsys, route, extension, status):
    exact_request = "Controllers " if not extension else "Controllers /Base.py"
    client, calls = _repository_client(
        monkeypatch, {"Controllers/Base.py": b"VALUE = 2\n"},
        statuses={exact_request: status},
    )
    graph, node, _ = _resolve_job_task(monkeypatch, client, "Controllers /Base" + extension, route)
    assert node.content is None
    assert node.export_error == ("github_list_failed" if not extension else "github_fetch_failed")
    assert str(status) in node.error_detail
    assert calls == [exact_request]
    assert "[git-parent-trim]" not in capsys.readouterr().out
    assert "0 OK, 1 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension", ["", ".py"])
@pytest.mark.parametrize("error_type", [requests.Timeout, requests.ConnectionError])
def test_exact_transport_failure_never_tries_trimmed_parent(monkeypatch, capsys, route, extension, error_type):
    exact_request = "Controllers " if not extension else "Controllers /Base.py"
    client, calls = _repository_client(
        monkeypatch, {"Controllers/Base.py": b"VALUE = 2\n"},
        transport_errors={exact_request: error_type},
    )
    graph, node, _ = _resolve_job_task(monkeypatch, client, "Controllers /Base" + extension, route)
    assert node.content is None
    assert node.export_error == ("github_list_failed" if not extension else "github_fetch_failed")
    assert calls == [exact_request]
    assert "[git-parent-trim]" not in capsys.readouterr().out
    assert "0 OK, 1 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension", ["", ".py"])
def test_exact_conversion_failure_never_tries_trimmed_parent(monkeypatch, capsys, route, extension):
    body = b"# Databricks notebook source\nVALUE = 1\n"
    client, calls = _repository_client(
        monkeypatch, {"Controllers /Base.py": body, "Controllers/Base.py": body},
    )
    monkeypatch.setattr(client, "_validate_conversion", lambda *args, **kwargs: (False, "synthetic conversion failure"))
    graph, node, _ = _resolve_job_task(monkeypatch, client, "Controllers /Base" + extension, route)
    assert node.content is None
    assert node.export_error == "conversion_validation_failed"
    assert "Controllers/Base.py" not in calls
    assert "Controllers" not in calls
    assert "[git-parent-trim]" not in capsys.readouterr().out
    assert "0 OK, 1 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension", ["", ".ipynb"])
def test_exact_malformed_notebook_never_tries_trimmed_parent(monkeypatch, capsys, route, extension):
    client, calls = _repository_client(
        monkeypatch, {"Controllers /Base.ipynb": b"not JSON", "Controllers/Base.ipynb": _marked_file(".ipynb", "trimmed")},
    )
    graph, node, _ = _resolve_job_task(monkeypatch, client, "Controllers /Base" + extension, route)
    assert node.export_error == "malformed_notebook"
    assert "Controllers/Base.ipynb" not in calls
    assert "Controllers" not in calls
    assert "[git-parent-trim]" not in capsys.readouterr().out
    assert "0 OK, 1 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
@pytest.mark.parametrize("extension", ["", ".py"])
def test_exact_fetch_404_after_successful_listing_can_fallback(monkeypatch, capsys, route, extension):
    exact = b"VALUE = 1\n"
    trimmed = b"VALUE = 2\n"
    client, calls = _repository_client(
        monkeypatch, {"Controllers /Base.py": exact, "Controllers/Base.py": trimmed},
        statuses={"Controllers /Base.py": 404},
    )
    graph, node, _ = _resolve_job_task(monkeypatch, client, "Controllers /Base" + extension, route)
    assert node.export_error is None
    assert node.content == trimmed
    expected = (["Controllers "] if not extension else []) + ["Controllers /Base.py"]
    expected += (["Controllers"] if not extension else []) + ["Controllers/Base.py"]
    assert calls == expected
    assert capsys.readouterr().out.count("[git-parent-trim]") == 1
    assert "1 OK, 0 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")


@pytest.mark.parametrize("route", ["direct", "for_each", "child_job"])
def test_exact_directory_without_requested_filename_can_fallback(monkeypatch, capsys, route):
    body = b"VALUE = 2\n"
    client, calls = _repository_client(
        monkeypatch, {"Controllers /Other.py": b"VALUE = 1\n", "Controllers/Base.py": body},
    )
    graph, node, _ = _resolve_job_task(monkeypatch, client, "Controllers /Base", route)
    assert node.export_error is None
    assert node.content == body
    assert calls == ["Controllers ", "Controllers", "Controllers/Base.py"]
    assert capsys.readouterr().out.count("[git-parent-trim]") == 1
    assert "1 OK, 0 export-failed" in pre_flight(graph, "/Workspace/export", "TEST_", "1")
