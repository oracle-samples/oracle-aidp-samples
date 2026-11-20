"""GitHub-via-proxy reads retry transient 429/5xx, count from config.

Incident: the customer proxy fronting GitHub returned HTTP 429 (nginx/1.25.5)
on list_github_dir_entries during the examplecorp auto_ft export (2026-09-20). The
listing was previously swallowed into an empty directory and surfaced
downstream as a misleading `git_path_unresolved`. These tests pin that a
transient 429 is now retried (honouring Retry-After) and that the attempt
count is driven by dbx_source.proxy_max_retries.
"""

import pytest

from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError


class _Response:
    def __init__(self, status_code, *, headers=None, text="", json_data=None, content=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.json_data = json_data
        self.content = content
        self.closed = False

    def json(self):
        return self.json_data

    def close(self):
        self.closed = True


def _proxy_client(max_retries):
    return DbxClient(DbxConfig(proxy="proxy.example:8080", proxy_max_retries=max_retries))


def test_github_dir_listing_retries_429_then_succeeds(monkeypatch):
    entries = [{"name": "AutoRefit.py", "type": "file"}]
    responses = [
        _Response(429, headers={"Retry-After": "0"}, text="rate limited"),
        _Response(200, json_data=entries),
    ]
    calls = []
    sleeps = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", sleeps.append)

    client = _proxy_client(20)
    result = client.list_github_dir_entries("example-data-science", "auto_ft", "Feature_Generation", "master")

    assert result == entries          # retried through the 429 rather than empty
    assert len(calls) == 2
    assert sleeps == [0.0]            # Retry-After honoured


def test_github_dir_listing_persistent_429_raises_and_does_not_cache(monkeypatch, capsys):
    # A persistent 429 exhausts EXACTLY proxy_max_retries attempts (here 3),
    # proving the customer knob drives the loop; then it RAISES rather than
    # caching an empty dir (a transient outage must not look like a missing
    # path or poison later lookups).
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response(429, headers={"Retry-After": "0"}, text="rate limited")

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _d: None)

    client = _proxy_client(3)
    with pytest.raises(DbxExportError) as caught:
        client.list_github_dir_entries("owner", "repo", "dir", "main")

    assert caught.value.reason == "github_list_failed"
    assert caught.value.status == 429
    assert len(calls) == 3                       # count from config, not MAX_EXPORT_RETRIES
    # Nothing cached — a subsequent lookup re-requests rather than returning [].
    calls.clear()
    with pytest.raises(DbxExportError):
        client.list_github_dir_entries("owner", "repo", "dir", "main")
    assert len(calls) == 3, "transient failure must NOT be cached"


def test_github_dir_listing_permanent_404_caches_empty(monkeypatch):
    # A genuinely-absent dir (404) is NOT transient: report empty AND cache it,
    # so we neither raise nor re-request a real permanent miss.
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response(404, text="not found")

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _d: None)

    client = _proxy_client(20)
    assert client.list_github_dir_entries("owner", "repo", "gone", "main") == []
    assert len(calls) == 1                       # 404 is not retried
    # Cached: the second lookup does not hit the network.
    assert client.list_github_dir_entries("owner", "repo", "gone", "main") == []
    assert len(calls) == 1


def test_github_dir_listing_403_raises_and_not_cached(monkeypatch):
    # A 403 (auth) is neither transient nor a 404 "absent dir": it is a
    # consistent failure, so we do NOT retry it, do NOT cache it as empty, and
    # fail loud instead of silently dropping the dir.
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response(403, text="forbidden")

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _d: None)

    client = _proxy_client(20)
    with pytest.raises(DbxExportError) as caught:
        client.list_github_dir_entries("owner", "repo", "dir", "main")
    assert caught.value.reason == "github_list_failed"
    assert caught.value.status == 403
    assert len(calls) == 1                       # not retried (not transient)
    # Not cached — a later lookup re-requests rather than returning [].
    calls.clear()
    with pytest.raises(DbxExportError):
        client.list_github_dir_entries("owner", "repo", "dir", "main")
    assert len(calls) == 1


def test_notebook_fetch_retries_429_then_succeeds(monkeypatch):
    # The notebook-fetch path (export_notebook_from_git) must also retry a
    # transient 429 — not just the dir listing.
    responses = [
        _Response(429, headers={"Retry-After": "0"}, text="rate limited"),
        _Response(200, content=b"VALUE = 1\n"),
    ]
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _d: None)

    client = _proxy_client(20)
    # `.py` with extension skips dir resolution and fetches directly.
    exported = client.export_notebook_from_git("folder/constants.py", "o", "r", "b")
    assert exported.content == b"VALUE = 1\n"
    assert len(calls) == 2


def test_notebook_fetch_persistent_429_raises(monkeypatch):
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return _Response(429, headers={"Retry-After": "0"}, text="rate limited")

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _d: None)

    client = _proxy_client(3)
    with pytest.raises(DbxExportError) as caught:
        client.export_notebook_from_git("folder/constants.py", "o", "r", "b")
    assert caught.value.reason == "github_fetch_failed"
    assert caught.value.status == 429
    assert len(calls) == 3                       # attempt count from proxy_max_retries


def test_resource_fetch_retries_429_then_succeeds(monkeypatch):
    responses = [
        _Response(429, headers={"Retry-After": "0"}, text="rate limited"),
        _Response(200, content=b"cfg: 1\n"),
    ]
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _d: None)

    client = _proxy_client(20)
    exported = client.export_resource_from_git("configs/default.yaml", "o", "r", "b")
    assert exported.content == b"cfg: 1\n"
    assert len(calls) == 2


def test_per_attempt_delay_is_clamped(monkeypatch):
    # A hostile/large Retry-After must not wedge a read: the per-attempt sleep is
    # clamped to _GITHUB_RETRY_MAX_DELAY_SEC.
    import dbx_export.dbx_client as m
    responses = [
        _Response(429, headers={"Retry-After": "3600"}, text="rate limited"),
        _Response(200, json_data=[]),
    ]
    sleeps = []
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", lambda *a, **k: responses.pop(0))
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", sleeps.append)

    _proxy_client(20).list_github_dir_entries("o", "r", "d", "b")
    assert sleeps == [float(m._GITHUB_RETRY_MAX_DELAY_SEC)]   # 3600 clamped to the cap
