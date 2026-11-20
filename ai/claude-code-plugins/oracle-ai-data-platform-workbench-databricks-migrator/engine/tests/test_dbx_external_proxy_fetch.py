"""Regression coverage for the proxy JAR-fetch feature (scoped re-raise of
the work reviewed on PR #67 / dev_branch, abc1234).

Covers:
  - DbxConfig fails loud (no hardcoded credential default) when proxy mode is
    used without explicit credentials.
  - forward_proxy_url() percent-encodes credentials, closing the leak/URL-
    breakage the PR #67 review found for passwords containing '/', '@', or
    whitespace.
  - _scrub_proxy_creds redacts userinfo from any text before it can reach a
    log line or exception message.
  - fetch_external_via_proxy: disabled-by-default gate, retry-on-5xx,
    rejects a 200 HTML error page as if it were binary content (the PR #67
    review's "no content-type/size/integrity check" finding), and never
    leaks credentials through a network-error or bad-status exception.
  - phase1_fetch_files actually wires fetched external bytes into
    node.content instead of discarding them (PR #67 review's M1 finding —
    the proxy fetch previously downloaded the jar then threw it away).
"""
from __future__ import annotations

import pytest
import requests

from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError, _scrub_proxy_creds
from dbx_export.dependency_resolver import FileNode, Graph
from dbx_export.__main__ import phase1_fetch_files


class _Response:
    def __init__(self, status_code, *, content=b"", text="", headers=None):
        self.status_code = status_code
        self.content = content
        self.text = text or content.decode("utf-8", errors="replace")
        self.headers = headers or {}


# Clearly-fake fixture credentials — never reuse a real (even retired)
# proxy credential in the tree.
_FAKE_USER = "proxyuser"
_FAKE_PASS = "proxypass"

# Zip/jar local-file-header magic the fetch guard requires.
_JAR_MAGIC = b"PK\x03\x04"


def _proxy_config(**overrides):
    kwargs = dict(proxy="host:8080", proxy_basic_user=_FAKE_USER, proxy_basic_pass=_FAKE_PASS)
    kwargs.update(overrides)
    return DbxConfig(**kwargs)


# ─── Credentials: no hardcoded default, OPTIONAL basic auth, percent-encoded ─

def test_no_hardcoded_credential_default():
    cfg = DbxConfig(proxy="host:8080")
    assert cfg.proxy_basic_user is None
    assert cfg.proxy_basic_pass is None


def test_http_kwargs_no_auth_without_credentials():
    # Proxy basic auth is optional — the pilot proxy needs none, so
    # `--dbx-proxy` alone must work (no ValueError, no auth attached).
    cfg = DbxConfig(proxy="host:8080")
    assert cfg.http_kwargs() == {}


def test_http_kwargs_applies_basic_auth_when_both_supplied():
    cfg = DbxConfig(proxy="host:8080", proxy_basic_user="u", proxy_basic_pass="p")
    kwargs = cfg.http_kwargs()
    assert "auth" in kwargs
    assert (kwargs["auth"].username, kwargs["auth"].password) == ("u", "p")


def test_forward_proxy_url_no_auth_without_credentials():
    # No creds → plain proxy URL (no userinfo), not a raise.
    cfg = DbxConfig(proxy="host:8080")
    assert cfg.forward_proxy_url() == "http://host:8080"


def test_http_kwargs_no_auth_with_only_username():
    # Partial credentials are a config mistake — never send half a basic-auth
    # pair; proceed WITHOUT authentication.
    cfg = DbxConfig(proxy="host:8080", proxy_basic_user="u")
    assert cfg.http_kwargs() == {}


def test_http_kwargs_no_auth_with_only_password():
    cfg = DbxConfig(proxy="host:8080", proxy_basic_pass="p")
    assert cfg.http_kwargs() == {}


def test_forward_proxy_url_no_auth_with_partial_credentials():
    assert DbxConfig(proxy="host:8080", proxy_basic_user="u").forward_proxy_url() == (
        "http://host:8080"
    )
    assert DbxConfig(proxy="host:8080", proxy_basic_pass="p").forward_proxy_url() == (
        "http://host:8080"
    )


def test_forward_proxy_url_percent_encodes_special_characters():
    """A password containing '/', '@', or a space must not appear raw in the
    URL — PR #67's review found the un-encoded version both leaked partially
    through the scrub regex and broke the URL's own syntax."""
    cfg = _proxy_config(proxy_basic_user="u", proxy_basic_pass="p/a s@s")
    url = cfg.forward_proxy_url()
    assert url == "http://u:p%2Fa%20s%40s@host:8080"
    assert "p/a s@s" not in url


def test_forward_proxy_url_none_without_proxy():
    cfg = DbxConfig(proxy_basic_user="u", proxy_basic_pass="p")
    assert cfg.forward_proxy_url() is None


def test_scrub_proxy_creds_redacts_userinfo():
    raw = f"ProxyError: http://{_FAKE_USER}:{_FAKE_PASS}@host:8080 -> tunnel failed"
    assert _scrub_proxy_creds(raw) == "ProxyError: http://***:***@host:8080 -> tunnel failed"


# ─── fetch_external_via_proxy ──────────────────────────────────────────────

def test_fetch_external_via_proxy_disabled_by_default():
    client = DbxClient(_proxy_config(fetch_external_via_proxy=False))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("s3://bucket/key.jar")
    assert caught.value.reason == "external_fetch_disabled"


def test_fetch_external_via_proxy_success(monkeypatch):
    captured = {}

    jar_body = _JAR_MAGIC + b"x" * 200

    def fake_get(url, proxies=None, timeout=None):
        captured["url"] = url
        captured["proxies"] = proxies
        return _Response(200, content=jar_body, headers={"Content-Type": "application/java-archive"})

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    content = client.fetch_external_via_proxy("s3://my-bucket/path/to/lib.jar")

    assert content == jar_body
    assert captured["url"] == "https://my-bucket.s3.amazonaws.com/path/to/lib.jar"
    assert captured["proxies"]["http"] == f"http://{_FAKE_USER}:{_FAKE_PASS}@host:8080"


def test_fetch_external_via_proxy_retries_5xx(monkeypatch):
    jar_body = _JAR_MAGIC + b"y" * 150
    responses = [
        _Response(502, text="bad gateway"),
        _Response(200, content=jar_body),
    ]
    calls = []

    def fake_get(*args, **kwargs):
        calls.append(1)
        return responses.pop(0)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _delay: None)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    content = client.fetch_external_via_proxy("https://example.com/lib.jar")

    assert content == jar_body
    assert len(calls) == 2


def test_fetch_external_via_proxy_rejects_html_error_page(monkeypatch):
    """A misconfigured proxy/edge can return HTTP 200 with an HTML error
    body instead of the real binary — PR #67 review's finding that nothing
    validated content-type/size before accepting the response."""

    def fake_get(url, proxies=None, timeout=None):
        return _Response(
            200,
            content=b"<html><body>Access Denied</body></html>" + b" " * 100,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _delay: None)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("https://example.com/lib.jar")
    assert caught.value.reason == "not_a_jar"
    assert "text/html" in caught.value.detail


def test_fetch_external_via_proxy_rejects_text_plain_html_body(monkeypatch):
    """A 200 with a text/plain (or missing) Content-Type used to pass the
    guard even when the body was an HTML error page — the PK magic-byte
    check must now reject any non-archive body regardless of content-type."""
    body = b"<html><body>Access Denied by upstream</body></html>" + b" " * 100
    assert len(body) >= 100

    def fake_get(url, proxies=None, timeout=None):
        return _Response(200, content=body, headers={"Content-Type": "text/plain"})

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("https://example.com/lib.jar")
    assert caught.value.reason == "not_a_jar"
    assert "PK magic" in caught.value.detail


def test_fetch_external_via_proxy_accepts_pk_prefixed_body(monkeypatch):
    """A body carrying the zip local-file-header magic is accepted even
    with a generic content-type (S3 often serves binary/octet-stream)."""
    jar_body = _JAR_MAGIC + b"\x00" * 196

    def fake_get(url, proxies=None, timeout=None):
        return _Response(200, content=jar_body, headers={"Content-Type": "binary/octet-stream"})

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    assert client.fetch_external_via_proxy("s3://bucket/lib.jar") == jar_body


def test_fetch_external_via_proxy_rejects_oversized_content_length(monkeypatch):
    """A declared Content-Length over the cap is rejected before the body
    is accepted, even when the body itself is a valid archive."""
    from dbx_export.dbx_client import MAX_EXTERNAL_FETCH_BYTES

    def fake_get(url, proxies=None, timeout=None):
        return _Response(
            200,
            content=_JAR_MAGIC + b"x" * 200,
            headers={
                "Content-Type": "application/java-archive",
                "Content-Length": str(MAX_EXTERNAL_FETCH_BYTES + 1),
            },
        )

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("s3://bucket/lib.jar")
    assert caught.value.reason == "external_fetch_too_large"


def test_fetch_external_via_proxy_rejects_oversized_body(monkeypatch):
    """The actual body length is enforced too — a proxy can omit or lie
    about Content-Length. (Cap monkeypatched down so the test does not
    allocate 256 MB.)"""
    monkeypatch.setattr("dbx_export.dbx_client.MAX_EXTERNAL_FETCH_BYTES", 1024)

    def fake_get(url, proxies=None, timeout=None):
        return _Response(200, content=_JAR_MAGIC + b"x" * 2048)

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("s3://bucket/lib.jar")
    assert caught.value.reason == "external_fetch_too_large"


def test_fetch_external_via_proxy_never_leaks_creds_on_network_error(monkeypatch):
    """A ProxyError from requests/urllib3 can embed the full proxy URL
    (including basic-auth credentials) in its str(). The resulting
    DbxExportError.detail must never contain them — this is a hard security
    requirement, not just tidiness."""

    def fake_get(url, proxies=None, timeout=None):
        raise requests.exceptions.ProxyError(
            f"HTTPSConnectionPool: Unable to connect to proxy "
            f"{proxies['https']} for {url}"
        )

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _delay: None)
    monkeypatch.setattr("dbx_export.dbx_client.MAX_EXPORT_RETRIES", 1)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("s3://bucket/key.jar")

    assert f"{_FAKE_USER}:{_FAKE_PASS}" not in str(caught.value)
    assert f"{_FAKE_USER}:{_FAKE_PASS}" not in caught.value.detail
    assert "***:***@host:8080" in caught.value.detail


def test_fetch_external_via_proxy_never_leaks_creds_on_bad_status(monkeypatch):
    def fake_get(url, proxies=None, timeout=None):
        return _Response(
            403,
            text=f"Forbidden — denied for tunnel via {proxies['https']}",
        )

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("s3://bucket/key.jar")

    assert f"{_FAKE_USER}:{_FAKE_PASS}" not in caught.value.detail
    assert "***:***@host:8080" in caught.value.detail


def test_fetch_external_via_proxy_rejects_non_s3_http_scheme():
    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("dbfs:/FileStore/jars/lib.jar")
    assert caught.value.reason == "unsupported_scheme"


def test_unsupported_scheme_detail_scrubs_embedded_userinfo():
    """A library URI carrying its own basic-auth userinfo must not leak the
    credential through the unsupported_scheme error detail."""
    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("ftp://bob:sekret@host/x.jar")
    assert caught.value.reason == "unsupported_scheme"
    assert "sekret" not in caught.value.detail
    assert "sekret" not in str(caught.value)
    assert "***:***@" in caught.value.detail


def test_malformed_s3_uri_detail_scrubs_embedded_userinfo():
    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    with pytest.raises(DbxExportError) as caught:
        client.fetch_external_via_proxy("s3://bob:sekret@bucketonly")
    assert caught.value.reason == "malformed_uri"
    assert "sekret" not in caught.value.detail
    assert "sekret" not in str(caught.value)


def test_success_path_never_touches_response_text(monkeypatch):
    """r.text decodes the entire body — on a multi-hundred-MB jar that is a
    pure waste. The success path must never access it (fix for the eager
    last_text computation that ran twice per successful 200 fetch)."""
    jar_body = _JAR_MAGIC + b"\x00" * 300

    class _CountingResponse:
        status_code = 200
        content = jar_body
        headers = {"Content-Type": "application/java-archive"}
        text_accesses = 0

        @property
        def text(self):
            type(self).text_accesses += 1
            return self.content.decode("utf-8", errors="replace")

    def fake_get(url, proxies=None, timeout=None):
        return _CountingResponse()

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    assert client.fetch_external_via_proxy("s3://bucket/lib.jar") == jar_body
    assert _CountingResponse.text_accesses == 0


# ─── fetch_file_uri wiring ──────────────────────────────────────────────────

def test_fetch_file_uri_returns_record_only_marker_when_proxy_fetch_disabled():
    client = DbxClient(_proxy_config(fetch_external_via_proxy=False))
    content, src_type = client.fetch_file_uri("s3://bucket/key.jar")
    assert (content, src_type) == (b"", "external")


def test_fetch_file_uri_returns_real_bytes_when_proxy_fetch_enabled(monkeypatch):
    jar_body = _JAR_MAGIC + b"z" * 150

    def fake_get(url, proxies=None, timeout=None):
        return _Response(200, content=jar_body, headers={"Content-Type": "application/java-archive"})

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", fake_get)

    client = DbxClient(_proxy_config(fetch_external_via_proxy=True))
    content, src_type = client.fetch_file_uri("s3://bucket/key.jar")
    assert content == jar_body
    assert src_type == "external"


# ─── phase1_fetch_files: PR #67 review's M1 finding ────────────────────────
# "the jar is fetched through the proxy ... then discarded, ending as
# EXTERNAL_RECORD_ONLY exactly as if the flag were off." Confirmed by
# execution against the reviewed head; this proves the scoped re-raise
# actually wires the bytes into node.content instead of repeating that bug.

def test_phase1_fetch_files_stores_content_for_successful_external_fetch(monkeypatch):
    graph = Graph()
    graph.files["s3://bucket/key.jar"] = FileNode(dbx_source="s3://bucket/key.jar", kind="jar")

    class _StubClient:
        def fetch_file_uri(self, uri):
            return b"real-jar-bytes", "external"

    phase1_fetch_files(_StubClient(), graph)

    node = graph.files["s3://bucket/key.jar"]
    assert node.content == b"real-jar-bytes"
    assert node.fetch_error is None


def test_main_rejects_external_fetch_flag_in_host_mode(monkeypatch, tmp_path, capsys):
    """--fetch-external-jars-via-proxy is only meaningful in proxy mode; in
    host+token mode it used to turn every benign EXTERNAL_RECORD_ONLY jar
    into a hard no_forward_proxy FAILURE (and abort the whole export under
    --strict-jars). main() must reject the combination at arg level."""
    import os
    import shutil
    from pathlib import Path

    from dbx_export.__main__ import ExitCode, main

    repo_root = Path(__file__).resolve().parents[1]
    shutil.copy(
        repo_root / "tests" / "config" / "fixtures" / "minimal.yaml",
        tmp_path / "aidp_migrator.yaml",
    )
    # Loader fail-fast requires bucket_mapping.path to exist on disk.
    (tmp_path / "mapping.json").write_text("{}")
    for key in list(os.environ):
        if key.startswith("AIDP_"):
            monkeypatch.delenv(key, raising=False)
    for key in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DBX_PROXY",
                "DBX_PROXY_BASIC_USER", "DBX_PROXY_BASIC_PASS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)

    rc = main([
        "--dbx-host", "https://example.cloud.databricks.com",
        "--dbx-token", "dapi-test",
        "--fetch-external-jars-via-proxy",
    ])

    assert rc == ExitCode.BAD_ARGS
    err = capsys.readouterr().err
    assert "--fetch-external-jars-via-proxy requires --dbx-proxy" in err


# ─── proxy-credential precedence matrix (atomic-pair resolution) ─────────────
# Sources: CLI/env × YAML secrets, each complete / partial / absent. Contract:
#   complete CLI/env pair  >  complete YAML pair  >  none (unauthenticated);
#   any partial pair remaining AFTER resolution is a hard BAD_ARGS error —
#   never a silent unauthenticated run against an auth-requiring proxy.

def _proxy_main_env(monkeypatch, tmp_path, yaml_secrets: str = "secrets: {}"):
    """Prepare an isolated proxy-mode config dir; return the main() callable."""
    import os
    from pathlib import Path

    from dbx_export.__main__ import main

    repo_root = Path(__file__).resolve().parents[1]
    cfg_text = (repo_root / "tests" / "config" / "fixtures" / "minimal.yaml").read_text()
    # Fixture is host-mode; these tests need proxy-mode so args.dbx_host stays None.
    cfg_text = cfg_text.replace(
        "  host: https://test.cloud.databricks.com", "  proxy: host:8080"
    )
    cfg_text = cfg_text.replace("secrets: {}", yaml_secrets)
    (tmp_path / "aidp_migrator.yaml").write_text(cfg_text)
    (tmp_path / "mapping.json").write_text("{}")
    for key in list(os.environ):
        if key.startswith("AIDP_"):
            monkeypatch.delenv(key, raising=False)
    for key in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DBX_PROXY",
                "DBX_PROXY_BASIC_USER", "DBX_PROXY_BASIC_PASS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)
    return main


_YAML_CRED_PAIR = (
    "secrets:\n"
    "  dbx_proxy_basic_user: yamluser\n"
    "  dbx_proxy_basic_pass: yamlpass\n"
)
_YAML_CRED_PARTIAL = (
    "secrets:\n"
    "  dbx_proxy_basic_user: yamluser\n"
)


def test_main_partial_cli_with_complete_yaml_uses_yaml_pair(monkeypatch, tmp_path, capsys):
    """A stale partial env/CLI value must NOT suppress a complete YAML pair —
    the exact hole the #79 review found (would 401 against an auth proxy)."""
    from dbx_export.__main__ import ExitCode
    main = _proxy_main_env(monkeypatch, tmp_path, _YAML_CRED_PAIR)

    rc = main(["--dbx-proxy", "host:8080", "--proxy-basic-user", "staleuser"])

    captured = capsys.readouterr()
    assert "resolved from config secrets" in captured.out
    assert "(CLI/env) ignored" in captured.out
    # Got PAST the credential stage; stops at the job-id gate, not on creds.
    assert "--job-id is required" in captured.err
    assert rc == ExitCode.BAD_ARGS


def test_main_partial_yaml_pair_fails_bad_args(monkeypatch, tmp_path, capsys):
    """A YAML with only one member of the pair is a configuration error —
    fail at startup instead of silently ignoring it."""
    from dbx_export.__main__ import ExitCode
    main = _proxy_main_env(monkeypatch, tmp_path, _YAML_CRED_PARTIAL)

    rc = main(["--dbx-proxy", "host:8080"])

    assert rc == ExitCode.BAD_ARGS
    assert "must be configured as a pair" in capsys.readouterr().err


def test_main_complete_cli_pair_overrides_yaml(monkeypatch, tmp_path, capsys):
    from dbx_export.__main__ import ExitCode
    main = _proxy_main_env(monkeypatch, tmp_path, _YAML_CRED_PAIR)

    rc = main(["--dbx-proxy", "host:8080",
               "--proxy-basic-user", "cliuser", "--proxy-basic-pass", "clipass"])

    captured = capsys.readouterr()
    # YAML never consulted — no resolution log.
    assert "resolved from config secrets" not in captured.out
    assert "--job-id is required" in captured.err
    assert rc == ExitCode.BAD_ARGS


def test_main_partial_cli_without_yaml_pair_fails_bad_args(monkeypatch, tmp_path, capsys):
    """Partial CLI/env and no YAML pair: hard error (supersedes the old
    warn-and-proceed — proceeding unauthenticated reproduces the 401)."""
    from dbx_export.__main__ import ExitCode
    main = _proxy_main_env(monkeypatch, tmp_path)

    rc = main(["--dbx-proxy", "host:8080", "--proxy-basic-user", "onlyuser"])

    err = capsys.readouterr().err
    assert rc == ExitCode.BAD_ARGS
    assert "proxy basic auth needs both" in err
    assert "--job-id is required" not in err  # failed AT the credential stage


def test_main_no_credentials_anywhere_proceeds_unauthenticated(monkeypatch, tmp_path, capsys):
    """Optional-auth semantics preserved: no creds in CLI/env/YAML is valid
    (proxy without basic auth) — main() proceeds past the credential stage."""
    from dbx_export.__main__ import ExitCode
    main = _proxy_main_env(monkeypatch, tmp_path)

    rc = main(["--dbx-proxy", "host:8080"])

    captured = capsys.readouterr()
    assert "--job-id is required" in captured.err
    assert "must be configured as a pair" not in captured.err
    assert "proxy basic auth needs both" not in captured.err
    assert rc == ExitCode.BAD_ARGS


def test_phase1_fetch_files_still_marks_skipped_when_fetch_disabled():
    graph = Graph()
    graph.files["s3://bucket/key.jar"] = FileNode(dbx_source="s3://bucket/key.jar", kind="jar")

    class _StubClient:
        def fetch_file_uri(self, uri):
            return b"", "external"

    phase1_fetch_files(_StubClient(), graph)

    node = graph.files["s3://bucket/key.jar"]
    assert node.content is None
    assert node.fetch_error == "external_uri_skipped"
