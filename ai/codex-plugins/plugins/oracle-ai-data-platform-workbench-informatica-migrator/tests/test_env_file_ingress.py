"""A planted .env cannot redirect the LLM key or the process's HTTP trust
(SEC-AIDP-SAMPLES-INFA-H2).

``infa2aidp.config`` used to read ``.env`` from the CURRENT DIRECTORY on
import and copy every ``KEY=value`` into ``os.environ``. The CLI is run
from inside customer export bundles, and the Anthropic / OpenAI SDKs read
``ANTHROPIC_BASE_URL`` / ``OPENAI_BASE_URL`` -- and httpx reads
``HTTPS_PROXY`` and ``SSL_CERT_FILE`` -- from the environment. A ``.env``
shipped in a bundle therefore sent the operator's real API key (exported in
the shell, as the README says) and the customer's mapping metadata to a
host of the bundle's choosing on the first ``--use-llm`` call, silently.

Pinned here:
  - A ``.env`` in the current directory is NOT read; it is reported with a
    WARNING naming the file and the opt-in (``INFA2AIDP_ENV_FILE``).
  - ``~/.infa2aidp/.env`` is still read when a cwd ``.env`` exists (it used
    to be shadowed by it), and an explicit ``INFA2AIDP_ENV_FILE`` is read.
  - Transport-steering names (``*_BASE_URL``, ``*_PROXY``, ``SSL_CERT_FILE``,
    ``REQUESTS_CA_BUNDLE``, ``OCI_CONFIG_FILE``, ``PYTHONPATH``...) are refused
    from ANY .env file, and unknown keys are ignored -- in both cases a
    WARNING names the key, never the value. Every key ``env.template``
    documents is allowed.
  - The SDK clients are constructed with an explicit ``base_url``: the
    vendor endpoint unless the SHELL exports an override.
  - ``AIDP_REGION`` is interpolated into a hostname, so it must be one DNS
    label.

These tests re-import ``infa2aidp.config`` under a controlled cwd / HOME
and put the environment back afterwards; no network, nothing under the
real home directory is touched.
"""
from __future__ import annotations

import importlib
import logging
import os
import re
import types
from pathlib import Path

import pytest

from infa2aidp import config as cfg

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE_VAR = getattr(cfg, "ENV_FILE_VAR", "INFA2AIDP_ENV_FILE")

STEERING = {
    "ANTHROPIC_BASE_URL": "https://attacker.invalid/anthropic",
    "OPENAI_BASE_URL": "https://attacker.invalid/v1",
    "HTTPS_PROXY": "http://attacker.invalid:8080",
    "HTTP_PROXY": "http://attacker.invalid:8080",
    "SSL_CERT_FILE": "/srv/evil/ca.pem",
    "REQUESTS_CA_BUNDLE": "/srv/evil/ca.pem",
    "OCI_CONFIG_FILE": "/srv/evil/oci-config",
    "PYTHONPATH": "/srv/evil",
}
MARKER_VALUE = "attacker.invalid"


def _write_env(path: Path, **pairs: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{k}={v}\n" for k, v in pairs.items()), encoding="utf-8")
    return path


@pytest.fixture
def env_sandbox(tmp_path, monkeypatch):
    """A fake HOME (with an empty ~/.infa2aidp), a scratch cwd, none of the
    steering names set, and a full os.environ snapshot restored at the end --
    monkeypatch only restores the keys it touched, and the loader under test
    adds keys of its own."""
    snapshot = dict(os.environ)
    home = tmp_path / "home"
    (home / ".infa2aidp").mkdir(parents=True)
    cwd = tmp_path / "customer_export"
    cwd.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    for key in list(STEERING) + [ENV_FILE_VAR, "ANTHROPIC_API_KEY", "OPENAI_API_KEY",
                                 "TARGET_SCORE", "FOO_BAR", "INFA_USER"]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(cwd)
    assert Path.home() == home, "Path.home() must follow the fake home for this test to mean anything"
    yield types.SimpleNamespace(home=home, cwd=cwd, home_env=home / ".infa2aidp" / ".env",
                                cwd_env=cwd / ".env")
    os.environ.clear()
    os.environ.update(snapshot)
    monkeypatch.undo()
    importlib.reload(cfg)  # module constants back to the real environment


def _reload_config():
    return importlib.reload(cfg)


# ── where a .env may come from ────────────────────────────────────────

def test_a_dotenv_in_the_current_directory_is_not_read(env_sandbox, caplog):
    _write_env(env_sandbox.cwd_env, ANTHROPIC_API_KEY="sk-planted", TARGET_SCORE="11", **STEERING)
    with caplog.at_level(logging.WARNING, logger="infa2aidp.config"):
        _reload_config()
    for key in STEERING:
        assert key not in os.environ, key
    assert "ANTHROPIC_API_KEY" not in os.environ
    assert "TARGET_SCORE" not in os.environ
    assert cfg.TARGET_SCORE == 90, "the default, not the planted value"
    assert MARKER_VALUE not in caplog.text, "a refused value is never echoed"
    # ...and the operator is told, by file name and with the way to opt in
    assert "Ignoring" in caplog.text and str(env_sandbox.cwd_env) in caplog.text
    assert ENV_FILE_VAR in caplog.text


def test_the_home_dotenv_is_read_even_when_a_cwd_dotenv_exists(env_sandbox):
    """It used to be shadowed: the first file found was the only one read."""
    _write_env(env_sandbox.home_env, TARGET_SCORE="77", INFA_USER="repo_admin")
    _write_env(env_sandbox.cwd_env, TARGET_SCORE="11", INFA_USER="planted")
    _reload_config()
    assert os.environ["TARGET_SCORE"] == "77"
    assert os.environ["INFA_USER"] == "repo_admin"
    assert cfg.TARGET_SCORE == 77
    assert Path(cfg.get_env_file_path()) == env_sandbox.home_env


def test_an_exported_value_is_never_overwritten_by_a_dotenv(env_sandbox, monkeypatch):
    monkeypatch.setenv("TARGET_SCORE", "42")
    _write_env(env_sandbox.home_env, TARGET_SCORE="77")
    _reload_config()
    assert cfg.TARGET_SCORE == 42


def test_an_explicit_env_file_is_read_and_may_be_the_cwd_one(env_sandbox, monkeypatch, caplog):
    _write_env(env_sandbox.cwd_env, TARGET_SCORE="55")
    monkeypatch.setenv(ENV_FILE_VAR, str(env_sandbox.cwd_env))
    with caplog.at_level(logging.WARNING, logger="infa2aidp.config"):
        _reload_config()
    assert cfg.TARGET_SCORE == 55
    assert "Ignoring" not in caplog.text, "the opted-in file is not also reported as ignored"
    assert Path(cfg.get_env_file_path()) == env_sandbox.cwd_env


def test_an_explicit_env_file_elsewhere_is_read_before_the_home_one(env_sandbox, monkeypatch, tmp_path):
    explicit = _write_env(tmp_path / "project" / "infa.env", TARGET_SCORE="61", MAX_ATTEMPTS="2")
    _write_env(env_sandbox.home_env, TARGET_SCORE="77", BATCH_WORKERS="9")
    monkeypatch.setenv(ENV_FILE_VAR, str(explicit))
    _reload_config()
    assert (cfg.TARGET_SCORE, cfg.MAX_ATTEMPTS, cfg.BATCH_WORKERS) == (61, 2, 9)


def test_a_missing_explicit_env_file_is_reported(env_sandbox, monkeypatch, caplog):
    monkeypatch.setenv(ENV_FILE_VAR, str(env_sandbox.cwd / "nope.env"))
    with caplog.at_level(logging.WARNING, logger="infa2aidp.config"):
        _reload_config()
    assert "nope.env" in caplog.text and "not a file" in caplog.text


# ── what a .env may set ───────────────────────────────────────────────

def test_steering_keys_are_refused_even_from_the_home_file(env_sandbox, caplog):
    _write_env(env_sandbox.home_env, TARGET_SCORE="66", **STEERING)
    with caplog.at_level(logging.WARNING, logger="infa2aidp.config"):
        _reload_config()
    for key in STEERING:
        assert key not in os.environ, key
    assert cfg.TARGET_SCORE == 66, "the allowed key in the same file is still applied"
    for key in STEERING:
        assert key in caplog.text, f"the refusal names {key}"
    assert MARKER_VALUE not in caplog.text and "/srv/evil" not in caplog.text


def test_unknown_keys_are_ignored_and_named(env_sandbox, caplog):
    _write_env(env_sandbox.home_env, FOO_BAR="1", TARGET_SCORE="66")
    with caplog.at_level(logging.WARNING, logger="infa2aidp.config"):
        _reload_config()
    assert "FOO_BAR" not in os.environ
    assert "FOO_BAR" in caplog.text
    assert cfg.TARGET_SCORE == 66


needs_allowlist = pytest.mark.skipif(not hasattr(cfg, "env_key_allowed"), reason="no allow-list")


@needs_allowlist
@pytest.mark.parametrize("key", [
    "ANTHROPIC_API_KEY", "CLAUDE_MODEL", "CLAUDE_MAX_TOKENS", "TARGET_SCORE", "RULE_BASED_FALLBACK",
    "TARGET_CATALOG_TYPE", "OCI_PROFILE", "AIDP_REGION", "AIDP_WORKSPACE_PATH", "AIDP_CLUSTER_KEY",
    "INFA_PASSWORD", "INFA_CA_BUNDLE", "INFA_TLS_VERIFY", "LLM_PROVIDER", "LLM_MAX_TOKENS",
    "OPENAI_API_KEY", "OPENAI_MODEL",
])
def test_documented_keys_are_allowed(key):
    assert cfg.env_key_allowed(key)


@needs_allowlist
@pytest.mark.parametrize("key", list(STEERING) + [
    "OPENAI_API_BASE", "ALL_PROXY", "NO_PROXY", "SSL_CERT_DIR", "CURL_CA_BUNDLE", "OCI_CLI_CONFIG_FILE",
    "PYTHONSTARTUP", "PATH", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "NODE_OPTIONS",
    "openai_base_url", "AIDP_BASE_URL", "INFA_PROXY", "", "SOME_RANDOM_KEY",
])
def test_steering_and_unknown_keys_are_not_allowed(key):
    assert not cfg.env_key_allowed(key)


@needs_allowlist
def test_every_key_env_template_documents_is_allowed():
    """The allow-list is only honest if the template cannot document a key
    the loader then throws away."""
    keys = re.findall(r"^\s*#?\s*([A-Z][A-Z0-9_]+)=", (ROOT / "env.template").read_text(encoding="utf-8"),
                      re.MULTILINE)
    assert keys, "env.template lists no keys?"
    refused = [k for k in keys if not cfg.env_key_allowed(k)]
    assert refused == []


# ── the SDK clients are pinned to the vendor endpoint ─────────────────

class _RecordingSDK:
    """Stands in for the anthropic / openai module: records constructor kwargs."""

    def __init__(self):
        self.kwargs = None

    def __call__(self, **kwargs):
        self.kwargs = kwargs
        return types.SimpleNamespace(**kwargs)


def test_anthropic_client_is_built_with_the_vendor_endpoint(monkeypatch):
    import infa2aidp.handlers.codellama_handler as handler_mod

    sdk = _RecordingSDK()
    monkeypatch.setattr(handler_mod, "_anthropic_mod", types.SimpleNamespace(Anthropic=sdk))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.delenv("ANTHROPIC_BASE_URL", raising=False)
    handler_mod.LLMHandler()
    assert sdk.kwargs is not None
    assert sdk.kwargs.get("base_url") == "https://api.anthropic.com"


def test_anthropic_endpoint_follows_a_shell_export_only(monkeypatch):
    import infa2aidp.handlers.codellama_handler as handler_mod

    sdk = _RecordingSDK()
    monkeypatch.setattr(handler_mod, "_anthropic_mod", types.SimpleNamespace(Anthropic=sdk))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://llm-gateway.corp.example/anthropic")
    handler_mod.LLMHandler()
    assert sdk.kwargs["base_url"] == "https://llm-gateway.corp.example/anthropic"


def test_openai_client_is_built_with_the_vendor_endpoint(monkeypatch):
    handler_mod = pytest.importorskip("infa2aidp.handlers.openai_handler",
                                      reason="OpenAI handler exists on the Codex build only")
    sdk = _RecordingSDK()
    monkeypatch.setattr(handler_mod, "_openai_mod", types.SimpleNamespace(OpenAI=sdk))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-proj-test")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    handler_mod.OpenAIHandler()
    assert sdk.kwargs is not None
    assert sdk.kwargs.get("base_url") == "https://api.openai.com/v1"


def test_planted_dotenv_cannot_move_the_real_anthropic_client(env_sandbox, monkeypatch):
    """End to end with the real SDK when it is installed: the hunter's repro."""
    pytest.importorskip("anthropic")
    import infa2aidp.handlers.codellama_handler as handler_mod

    if handler_mod._anthropic_mod is None:  # pragma: no cover
        pytest.skip("anthropic not importable here")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-REAL-OPERATOR-KEY")
    _write_env(env_sandbox.cwd_env, ANTHROPIC_BASE_URL=STEERING["ANTHROPIC_BASE_URL"])
    _reload_config()
    client = handler_mod.LLMHandler()._claude_client
    assert str(client.base_url).rstrip("/") == "https://api.anthropic.com"


def test_planted_dotenv_cannot_move_the_real_openai_client(env_sandbox, monkeypatch):
    pytest.importorskip("openai")
    handler_mod = pytest.importorskip("infa2aidp.handlers.openai_handler",
                                      reason="OpenAI handler exists on the Codex build only")
    if handler_mod._openai_mod is None:  # pragma: no cover
        pytest.skip("openai not importable here")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-proj-REAL-OPERATOR-KEY")
    _write_env(env_sandbox.cwd_env, OPENAI_BASE_URL=STEERING["OPENAI_BASE_URL"])
    _reload_config()
    client = handler_mod.OpenAIHandler()._client
    assert str(client.base_url).rstrip("/") == "https://api.openai.com/v1"


# ── AIDP_REGION becomes a hostname ────────────────────────────────────

@pytest.mark.parametrize("region", ["us-ashburn-1", "eu-frankfurt-1", "ap-mumbai-1", "r1"])
def test_a_region_label_is_accepted(region):
    from infa2aidp.deployer.aidp_client import AIDPClient

    c = AIDPClient(region, "ocid1.datalake.x", signer=object())
    assert c.base_url.startswith(f"https://aidp.{region}.oci.oraclecloud.com/")


@pytest.mark.parametrize("region", [
    "attacker.example", "us-ashburn-1.attacker.example", "x/", "us-ashburn-1/#", "a@b", "", " us-ashburn-1",
])
def test_a_region_that_would_change_the_host_is_refused(region):
    from infa2aidp.deployer.aidp_client import AIDPClient

    with pytest.raises(ValueError):
        AIDPClient(region, "ocid1.datalake.x", signer=object())
