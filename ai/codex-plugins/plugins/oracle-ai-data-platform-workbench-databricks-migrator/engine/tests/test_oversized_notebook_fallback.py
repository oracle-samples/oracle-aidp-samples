import base64
import json

import pytest

from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError


SOURCE_NOTEBOOK = (
    b"# Databricks notebook source\n"
    b"value = 1\n"
    b"# COMMAND ----------\n"
    b"# MAGIC %md\n"
    b"# MAGIC documentation\n"
)


def _ipynb_bytes():
    return json.dumps({
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [{
            "cell_type": "code",
            "metadata": {},
            "source": ["value = 1\n"],
            "outputs": [],
            "execution_count": None,
        }],
    }).encode("utf-8")


class Response:
    def __init__(self, status_code, *, body=None, content=None):
        self.status_code = status_code
        self._body = body
        if content is None and body is not None:
            content = json.dumps(body).encode("utf-8")
        self.content = content or b""
        self.text = (
            json.dumps(body) if body is not None
            else self.content.decode("utf-8", errors="replace")
        )

    def json(self):
        if self._body is None:
            raise ValueError("not JSON")
        return self._body

    def iter_content(self, chunk_size):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start:start + chunk_size]


def _oversized(message=None, error_code="MAX_NOTEBOOK_SIZE_EXCEEDED"):
    return Response(400, body={
        "error_code": error_code,
        "message": message or "Notebook size exceeded the byte limit: 105119000",
    })


def _client():
    return DbxClient(DbxConfig(host="https://example", token="x"))


def test_oversized_jupyter_uses_direct_download(monkeypatch):
    calls = []
    responses = iter((_oversized(), Response(200, content=_ipynb_bytes())))

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return next(responses)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    exported = _client().export_notebook("/Workspace/Users/u/large")

    assert exported.is_notebook is True
    assert exported.export_method == "workspace_jupyter_direct"
    assert json.loads(exported.content)["cells"][0]["source"] == ["value = 1\n"]
    assert "direct_download" not in calls[0][1]["params"]
    assert calls[1][1]["params"]["direct_download"] is True
    assert calls[1][1]["params"]["format"] == "JUPYTER"


def test_oversized_jupyter_falls_back_to_direct_source_without_outputs(monkeypatch):
    calls = []
    responses = iter((
        _oversized(
            'The notebook at "/Users/u/large" has exceeded the memory limit. '
            "Try clearing cell outputs or removing visualizations to reduce the size.",
            error_code="BAD_REQUEST",
        ),
        _oversized(),
        Response(200, content=SOURCE_NOTEBOOK),
    ))

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return next(responses)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    exported = _client().export_notebook("/Workspace/Users/u/large")
    notebook = json.loads(exported.content)

    assert exported.is_notebook is True
    assert exported.export_method == "workspace_source_direct"
    assert len(notebook["cells"]) == 2
    assert all(cell.get("outputs", []) == [] for cell in notebook["cells"])
    assert calls[2][1]["params"] == {
        "path": "/Users/u/large",
        "format": "SOURCE",
        "direct_download": True,
    }


def test_oversized_source_uses_raw_workspace_files_as_last_read_only_fallback(monkeypatch):
    calls = []
    responses = iter((
        _oversized(),
        _oversized(),
        _oversized(),
        Response(200, content=SOURCE_NOTEBOOK),
    ))

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return next(responses)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    exported = _client().export_notebook(
        "/Workspace/Users/u/folder with spaces/large"
    )

    assert exported.is_notebook is True
    assert exported.export_method == "workspace_files_raw"
    assert len(json.loads(exported.content)["cells"]) == 2
    assert "/workspace-files/Users/u/folder%20with%20spaces/large" in calls[3][0]


def test_oversized_workspace_files_adds_ipynb_extension_after_bare_path_404(monkeypatch):
    calls = []
    responses = iter((
        _oversized(),
        _oversized(),
        _oversized(),
        Response(404, body={"message": "File not found."}),
        Response(200, content=_ipynb_bytes()),
    ))

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return next(responses)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    exported = _client().export_notebook(
        "/Workspace/Users/u/folder with spaces/large"
    )

    assert exported.is_notebook is True
    assert exported.export_method == "workspace_files_raw"
    assert json.loads(exported.content)["nbformat"] == 4
    assert calls[3][0].endswith(
        "/workspace-files/Users/u/folder%20with%20spaces/large"
    )
    assert calls[4][0].endswith(
        "/workspace-files/Users/u/folder%20with%20spaces/large.ipynb"
    )


def test_oversized_fallback_exhaustion_has_precise_platform_limit_reason(monkeypatch):
    responses = iter((
        _oversized(),
        _oversized(),
        _oversized(),
        Response(404, body={"error_code": "NOT_FOUND", "message": "not a file"}),
        Response(404, body={"error_code": "NOT_FOUND", "message": "not a file"}),
    ))
    monkeypatch.setattr(
        "dbx_export.dbx_client.requests.get", lambda *args, **kwargs: next(responses)
    )

    with pytest.raises(DbxExportError) as caught:
        _client().export_notebook("/Workspace/Users/u/large")

    assert caught.value.reason == "source_platform_size_limit"
    assert "JUPYTER encoded" in caught.value.detail
    assert "SOURCE direct" in caught.value.detail
    assert "workspace-files raw" in caught.value.detail


def test_oversized_raw_fallback_refuses_unverifiable_bytes(monkeypatch):
    responses = iter((
        _oversized(),
        _oversized(),
        _oversized(),
        Response(200, content=b"not a notebook"),
    ))
    monkeypatch.setattr(
        "dbx_export.dbx_client.requests.get", lambda *args, **kwargs: next(responses)
    )

    with pytest.raises(DbxExportError) as caught:
        _client().export_notebook("/Workspace/Users/u/large")

    assert caught.value.reason == "oversized_fallback_invalid"


def test_path_word_payload_does_not_trigger_oversized_classification(monkeypatch):
    response = Response(400, body={
        "error_code": "INVALID_PARAMETER_VALUE",
        "message": (
            "Path (/Users/u/EX_PAYLOAD_RUN_JOBS) is not an exportable asset. "
            "type=mlflowExperiment"
        ),
    })
    monkeypatch.setattr(
        "dbx_export.dbx_client.requests.get", lambda *args, **kwargs: response
    )

    with pytest.raises(DbxExportError) as caught:
        _client()._workspace_export_raw(
            "/Users/u/EX_PAYLOAD_RUN_JOBS", "JUPYTER"
        )

    assert caught.value.reason == "bad_request"


def test_encoded_source_size_error_escalates_to_direct_source(monkeypatch):
    responses = iter((
        Response(400, body={
            "error_code": "INVALID_PARAMETER_VALUE",
            "message": "unsupported format JUPYTER",
        }),
        _oversized(),
        Response(200, content=SOURCE_NOTEBOOK),
    ))
    monkeypatch.setattr(
        "dbx_export.dbx_client.requests.get", lambda *args, **kwargs: next(responses)
    )

    exported = _client().export_notebook("/Workspace/Users/u/large")

    assert exported.is_notebook is True
    assert len(json.loads(exported.content)["cells"]) == 2


def test_direct_download_accepts_proxy_base64_envelope(monkeypatch):
    responses = iter((
        _oversized(),
        Response(200, body={
            "content": base64.b64encode(_ipynb_bytes()).decode("ascii"),
        }),
    ))
    monkeypatch.setattr(
        "dbx_export.dbx_client.requests.get", lambda *args, **kwargs: next(responses)
    )

    exported = _client().export_notebook("/Workspace/Users/u/large")

    assert exported.export_method == "workspace_jupyter_direct"
    assert json.loads(exported.content)["nbformat"] == 4
