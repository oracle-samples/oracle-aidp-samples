import base64

import pytest

from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError


class _Response:
    def __init__(
        self, status_code, *, content=b"", headers=None, text="", json_data=None
    ):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.text = text
        self.json_data = json_data
        self.closed = False

    def json(self):
        if self.json_data is not None:
            return self.json_data
        return {"content": base64.b64encode(self.content).decode("ascii")}

    def close(self):
        self.closed = True


def test_plain_workspace_resource_retries_429(monkeypatch):
    responses = [
        _Response(429, headers={"Retry-After": "0"}, text="rate limited"),
        _Response(200, content=b"VALUE = 1\n"),
    ]
    calls = []
    sleeps = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", sleeps.append)

    client = DbxClient(DbxConfig(host="https://example", token="token"))
    result = client.export_workspace_file("/Workspace/project/constants.py")

    assert result == b"VALUE = 1\n"
    assert len(calls) == 2
    assert sleeps == [0.0]


def test_jar_workspace_file_does_not_retry_429(monkeypatch):
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response(429, headers={"Retry-After": "0"}, text="rate limited")

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr(
        "dbx_export.dbx_client.time.sleep",
        lambda _delay: pytest.fail("JAR export must not retry"),
    )

    client = DbxClient(DbxConfig(host="https://example", token="token"))
    with pytest.raises(DbxExportError) as caught:
        client.export_workspace_file("/Workspace/libraries/model.jar")

    assert caught.value.status == 429
    assert len(calls) == 1


def test_workspace_folder_listing_retries_429(monkeypatch, capsys):
    responses = [
        _Response(429, headers={"Retry-After": "0"}, text="rate limited"),
        _Response(200, json_data={
            "objects": [{"path": "/Workspace/project/constants.py", "object_type": "FILE"}]
        }),
    ]
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _delay: None)

    client = DbxClient(DbxConfig(host="https://example", token="token"))
    result = client.list_workspace_folder("/Workspace/project")

    assert result == [
        {"path": "/Workspace/project/constants.py", "object_type": "FILE"}
    ]
    assert len(calls) == 2
    assert "workspace folder listing /Workspace/project HTTP 429; retry 1/7" in (
        capsys.readouterr().out
    )


def test_workspace_folder_listing_fails_after_429_retries(monkeypatch):
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response(429, headers={"Retry-After": "0"}, text="rate limited")

    monkeypatch.setattr("dbx_export.dbx_client.MAX_EXPORT_RETRIES", 2)
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _delay: None)

    client = DbxClient(DbxConfig(host="https://example", token="token"))
    with pytest.raises(DbxExportError) as caught:
        client.list_workspace_folder("/Workspace/project")

    assert caught.value.reason == "workspace_list_retries_exhausted"
    assert caught.value.status == 429
    assert len(calls) == 2
