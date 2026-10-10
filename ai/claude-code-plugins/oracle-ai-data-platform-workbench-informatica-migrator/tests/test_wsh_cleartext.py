"""The repository password never goes to the Web Services Hub in the clear
(SEC-AIDP-SAMPLES-INFA-H3).

``_connect_soap`` took the URL scheme from the PORT NUMBER -- ``https`` only
when the port was literally 7343 -- and the CLI default was ``--port 6005``
(the Informatica domain gateway, not a hub port), so every documented
``discover`` invocation POSTed the ``LoginRequest``, whose body is the
PowerCenter repository password, over plain http; ``connect("auto")`` then
fell through to pmrep with the password already on the wire. The TLS
verification setting pinned by test_crawler_security.py had nothing to
verify on an ``http://`` URL.

Pinned here:
  - The auto-built hub URL is ``https://`` on EVERY port (default, 7333,
    6005), and the CLI default port is 7343.
  - An ``http://`` hub URL without the opt-in is refused BEFORE any request
    is made -- ``InsecureTransportError`` (a ``ConnectionError``) -- in
    ``--method soap`` and in ``auto``, where pmrep is still tried and the
    skipped SOAP attempt is said at WARNING level.
  - ``allow_insecure_http`` (``--insecure-http`` / ``INFA_WSH_ALLOW_HTTP=1``)
    allows http, with a WARNING naming the host.
  - An explicit ``https://`` ``wsh_url`` on any port is used as-is;
    ``--wsh-url`` / ``INFA_WSH_URL`` reach the config from the CLI.
  - The skill and env.template no longer document the cleartext default.

No network: the session's ``post`` is replaced with a recorder.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import pytest

import infa2aidp.crawlers.informatica_crawler as crawler_mod
from infa2aidp.cli import build_parser, main
from infa2aidp.crawlers.informatica_crawler import InfaConnectionConfig, InformaticaCrawler

ROOT = Path(__file__).resolve().parent.parent
PASSWORD = "Hunter2!-do-not-leak"


class _Recorder:
    """Stands in for ``requests.Session.post``: records every call, answers a
    successful Login so the crawler goes on exactly as it would live."""

    class _OK:
        text = "<r><SessionId>0123456789abcdef0123</SessionId></r>"

        def raise_for_status(self):
            pass

    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def __call__(self, url, data=None, headers=None, timeout=None):
        self.calls.append((url, (data or b"").decode("utf-8", "replace")))
        return self._OK()

    @property
    def urls(self):
        return [u for u, _ in self.calls]


def _crawler(**overrides) -> tuple[InformaticaCrawler, _Recorder]:
    kwargs = dict(host="pc.customer.local", username="Administrator", password=PASSWORD,
                  repository="REPO", domain="DOM")
    kwargs.update(overrides)
    c = InformaticaCrawler(InfaConnectionConfig(**kwargs))
    rec = _Recorder()
    c._http.post = rec
    return c, rec


# ── https on every port ───────────────────────────────────────────────

def test_the_default_config_logs_in_over_https():
    c, rec = _crawler()
    c._connect_soap()
    assert rec.urls == ["https://pc.customer.local:7343/wsh/services/MetadataService"]
    assert PASSWORD in rec.calls[0][1], "it IS the login -- the point is the scheme it went over"
    assert c.config.wsh_url.startswith("https://")


@pytest.mark.parametrize("port", [7333, 6005, 8080])
def test_no_port_number_turns_the_scheme_into_http(port):
    """The scheme used to follow the port: https for 7343, http otherwise."""
    c, rec = _crawler(port=port)
    c._connect_soap()
    assert rec.urls == [f"https://pc.customer.local:{port}/wsh/services/MetadataService"]


def test_an_explicit_https_url_on_any_port_is_used_as_is():
    c, rec = _crawler(wsh_url="https://infa-server:8443/wsh/services")
    c._connect_soap()
    assert rec.urls == ["https://infa-server:8443/wsh/services/MetadataService"]
    assert c.config.wsh_url == "https://infa-server:8443/wsh/services"


# ── http is refused before anything is sent ───────────────────────────

def test_an_http_url_without_the_opt_in_is_refused_before_any_request():
    c, rec = _crawler(wsh_url="http://pc.customer.local:7333/wsh/services")
    with pytest.raises(ConnectionError) as info:
        c._connect_soap()
    assert rec.calls == [], "nothing may leave the process -- the first call is the password"
    assert "cleartext" in str(info.value).lower()
    assert "--insecure-http" in str(info.value) and "INFA_WSH_ALLOW_HTTP" in str(info.value)
    assert PASSWORD not in str(info.value)
    assert c._session_id is None


def test_the_refusal_is_a_connection_error_the_cli_already_handles():
    err = getattr(crawler_mod, "InsecureTransportError", None)
    assert err is not None and issubclass(err, ConnectionError)


def test_auto_mode_sends_nothing_and_still_tries_pmrep(caplog):
    """connect("auto") used to POST the password, get a SOAP fault from the
    gateway port, and quietly carry on to pmrep."""
    c, rec = _crawler(wsh_url="http://pc.customer.local:6005/wsh/services")
    tried = []
    c._connect_pmrep = lambda: tried.append("pmrep")
    with caplog.at_level(logging.WARNING):
        assert c.connect("auto") == "pmrep"
    assert rec.calls == []
    assert tried == ["pmrep"]
    assert "cleartext" in caplog.text.lower(), "the skipped SOAP attempt is said, not buried in debug"
    assert PASSWORD not in caplog.text


def test_explicit_soap_method_fails_clearly_over_http():
    c, rec = _crawler(wsh_url="http://pc.customer.local:7333/wsh/services")
    with pytest.raises(ConnectionError) as info:
        c.connect("soap")
    assert rec.calls == []
    assert "cleartext" in str(info.value).lower()


# ── the opt-in ────────────────────────────────────────────────────────

def test_the_opt_in_allows_http_and_warns_naming_the_host(caplog):
    c, rec = _crawler(port=7333, allow_insecure_http=True)
    with caplog.at_level(logging.WARNING):
        c._connect_soap()
    assert rec.urls == ["http://pc.customer.local:7333/wsh/services/MetadataService"]
    assert "pc.customer.local" in caplog.text and "cleartext" in caplog.text.lower()
    assert PASSWORD not in caplog.text


def test_the_opt_in_also_accepts_an_explicit_http_url():
    c, rec = _crawler(wsh_url="http://lab-hub:7333/wsh/services", allow_insecure_http=True)
    c._connect_soap()
    assert rec.urls == ["http://lab-hub:7333/wsh/services/MetadataService"]


def test_the_opt_in_does_not_downgrade_an_https_url():
    c, rec = _crawler(wsh_url="https://infa-server:7343/wsh/services", allow_insecure_http=True)
    c._connect_soap()
    assert rec.urls == ["https://infa-server:7343/wsh/services/MetadataService"]


def test_the_opt_in_is_off_by_default():
    assert InfaConnectionConfig(host="h").allow_insecure_http is False


# ── CLI wiring ────────────────────────────────────────────────────────

def test_the_cli_default_port_is_the_https_hub_port():
    args = build_parser().parse_args(["discover", "--host", "h"])
    assert args.port == 7343
    assert args.wsh_url is None and args.insecure_http is False


@pytest.fixture
def seen_config(monkeypatch):
    """Capture the InfaConnectionConfig _cmd_discover builds; no connection is made."""
    import types

    seen: dict = {}

    class FakeCrawler:
        def __init__(self, cfg):
            seen["cfg"] = cfg

        def connect(self, method):
            return "pmrep"

        def crawl_repository(self, out_dir, folders=None, export_xml=False):
            return types.SimpleNamespace(mappings=[], workflows=[], exported_xml_files=["x.xml"], errors=[])

        def generate_inventory_report(self, result, path):
            pass

        def disconnect(self):
            pass

    monkeypatch.setattr(crawler_mod, "InformaticaCrawler", FakeCrawler)
    monkeypatch.setenv("INFA_PASSWORD", PASSWORD)
    for key in ("INFA_WSH_URL", "INFA_WSH_ALLOW_HTTP"):
        monkeypatch.delenv(key, raising=False)
    return seen


def test_cli_flags_reach_the_config(seen_config, tmp_path):
    rc = main(["discover", "--host", "pc.example", "--wsh-url", "https://pc.example:8443/wsh/services",
               "--insecure-http", "-o", str(tmp_path)])
    assert rc == 0
    cfg = seen_config["cfg"]
    assert (cfg.port, cfg.wsh_url, cfg.allow_insecure_http) == (
        7343, "https://pc.example:8443/wsh/services", True)


def test_cli_defaults_are_https_and_no_opt_in(seen_config, tmp_path):
    assert main(["discover", "--host", "pc.example", "-o", str(tmp_path)]) == 0
    cfg = seen_config["cfg"]
    assert (cfg.port, cfg.wsh_url, cfg.allow_insecure_http) == (7343, "", False)
    assert cfg.resolved_wsh_url() == "https://pc.example:7343/wsh/services"


def test_env_vars_reach_the_config(seen_config, tmp_path, monkeypatch):
    monkeypatch.setenv("INFA_WSH_URL", "https://pc.example:9443/wsh/services")
    monkeypatch.setenv("INFA_WSH_ALLOW_HTTP", "1")
    assert main(["discover", "--host", "pc.example", "-o", str(tmp_path)]) == 0
    cfg = seen_config["cfg"]
    assert (cfg.wsh_url, cfg.allow_insecure_http) == ("https://pc.example:9443/wsh/services", True)


@pytest.mark.parametrize("value", ["0", "", "yes", "true"])
def test_only_the_literal_1_opts_in(seen_config, tmp_path, monkeypatch, value):
    monkeypatch.setenv("INFA_WSH_ALLOW_HTTP", value)
    assert main(["discover", "--host", "pc.example", "-o", str(tmp_path)]) == 0
    assert seen_config["cfg"].allow_insecure_http is False


def test_a_wsh_url_with_credentials_is_still_refused(seen_config, tmp_path, caplog):
    rc = main(["discover", "--host", "pc.example", "--wsh-url",
               f"https://admin:{PASSWORD}@pc.example:7343/wsh/services", "-o", str(tmp_path)])
    assert rc == 1
    assert PASSWORD not in caplog.text


# ── documentation ─────────────────────────────────────────────────────

def test_the_skill_documents_the_https_default_not_the_gateway_port():
    text = (ROOT / "skills" / "infa-discover" / "SKILL.md").read_text(encoding="utf-8")
    assert "6005" not in text
    assert "--port 7343" in text
    assert "--insecure-http" in text and "--wsh-url" in text


def test_env_template_documents_the_opt_in():
    text = (ROOT / "env.template").read_text(encoding="utf-8")
    assert "INFA_WSH_ALLOW_HTTP" in text and "INFA_WSH_URL" in text


def test_the_module_docstring_does_not_advertise_http():
    doc = crawler_mod.__doc__ or ""
    assert not re.search(r"http://<host>", doc)
