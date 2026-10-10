"""SEC-AIDP-SAMPLES-006: dbutils.secrets shim is Vault-only by default.

- Default lookup never scans AIDP_SECRET_* and never opens the JSON file.
- Plaintext fallbacks require AIDP_ALLOW_PLAINTEXT_SECRETS=1 and an explicit
  AIDP_SECRETS_FILE; the file must be owner-only on POSIX (mode check is
  skipped on Windows, where st_mode carries no ACL information).
- Plaintext mode logs ONE line that names the mode and never a value.
- AIDP_SECRET_SCOPES restricts get/list/listScopes; list operations never
  reveal values.
"""
import json
import logging
import os

import pytest

from aidp_compat import secrets as secrets_mod
from aidp_compat.secrets import AIDPSecretsUtils

SECRET_VALUE = "s3cr3t-value-do-not-print"


def _no_vault(monkeypatch):
    """Make Vault lookups fail the way they do without AIDP_VAULT_OCID."""
    monkeypatch.setattr(AIDPSecretsUtils, "_get_from_oci_vault",
                        lambda self, scope, key: (_ for _ in ()).throw(ValueError("no vault")))


def _write_secrets_file(tmp_path, payload, mode=0o600):
    p = tmp_path / "secrets.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    if os.name == "posix":
        os.chmod(str(p), mode)
    return str(p)


# ── default: Vault only ────────────────────────────────────────────────
def test_default_lookup_never_reads_aidp_secret_env(monkeypatch):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_SECRET_MYSCOPE_TOKEN", SECRET_VALUE)
    calls = []
    orig = AIDPSecretsUtils._load_env_secrets
    monkeypatch.setattr(AIDPSecretsUtils, "_load_env_secrets",
                        lambda self: calls.append("env") or orig(self))
    s = AIDPSecretsUtils()
    assert calls == [], "AIDP_SECRET_* must not be scanned unless plaintext mode is on"
    with pytest.raises(KeyError):
        s.get("myscope", "token")
    assert s.listScopes() == []
    assert s.list("myscope") == []


def test_default_lookup_never_opens_the_json_file(monkeypatch, tmp_path):
    _no_vault(monkeypatch)
    bad = tmp_path / "secrets.json"
    bad.write_text("{ this is not json", encoding="utf-8")   # would raise if parsed
    monkeypatch.setenv("AIDP_SECRETS_FILE", str(bad))
    with pytest.raises(KeyError):
        AIDPSecretsUtils().get("db", "password")


def test_default_lookup_consults_vault(monkeypatch):
    monkeypatch.setenv("AIDP_SECRET_DB_PASSWORD", "env-value-must-lose")
    monkeypatch.setattr(AIDPSecretsUtils, "_get_from_oci_vault",
                        lambda self, scope, key: f"vault:{scope}/{key}")
    s = AIDPSecretsUtils()
    assert s.get("db", "password") == "vault:db/password"
    assert s.getBytes("db", "password") == b"vault:db/password"


def test_default_mode_logs_nothing_about_plaintext(caplog):
    with caplog.at_level(logging.INFO, logger="aidp_compat.secrets"):
        AIDPSecretsUtils()
    assert "plaintext" not in caplog.text.lower()


# ── opt-in plaintext mode ─────────────────────────────────────────────
def test_plaintext_mode_requires_exact_opt_in(monkeypatch):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_SECRET_MYSCOPE_TOKEN", SECRET_VALUE)
    for value in ("true", "yes", "0", ""):
        monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", value)
        with pytest.raises(KeyError):
            AIDPSecretsUtils().get("myscope", "token")
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    assert AIDPSecretsUtils().get("myscope", "token") == SECRET_VALUE


def test_plaintext_mode_logs_one_line_without_values(monkeypatch, caplog):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    monkeypatch.setenv("AIDP_SECRET_MYSCOPE_TOKEN", SECRET_VALUE)
    with caplog.at_level(logging.INFO, logger="aidp_compat.secrets"):
        s = AIDPSecretsUtils()
        assert s.get("myscope", "token") == SECRET_VALUE
    lines = [r for r in caplog.records if "insecure plaintext secrets mode active" in r.getMessage()]
    assert len(lines) == 1
    assert lines[0].levelno == logging.WARNING
    assert SECRET_VALUE not in caplog.text


def test_plaintext_file_requires_explicit_path(monkeypatch, tmp_path):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    # No AIDP_SECRETS_FILE: there is no implicit default location any more.
    with pytest.raises(KeyError):
        AIDPSecretsUtils().get("db", "password")


def test_plaintext_file_is_read_in_plaintext_mode(monkeypatch, tmp_path):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    monkeypatch.setenv("AIDP_SECRETS_FILE", _write_secrets_file(tmp_path, {"db": {"password": SECRET_VALUE}}))
    s = AIDPSecretsUtils()
    assert s.get("db", "password") == SECRET_VALUE
    with pytest.raises(KeyError):
        s.get("db", "missing")


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode bits only; Windows ACLs are not in st_mode")
def test_plaintext_file_mode_0644_is_refused_0600_passes(monkeypatch, tmp_path):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    path = _write_secrets_file(tmp_path, {"db": {"password": SECRET_VALUE}}, mode=0o644)
    monkeypatch.setenv("AIDP_SECRETS_FILE", path)
    with pytest.raises(PermissionError) as ei:
        AIDPSecretsUtils().get("db", "password")
    assert "owner-only" in str(ei.value) and SECRET_VALUE not in str(ei.value)
    os.chmod(path, 0o600)
    assert AIDPSecretsUtils().get("db", "password") == SECRET_VALUE


@pytest.mark.skipif(os.name == "posix", reason="Windows-only: the mode check is skipped with a note")
def test_plaintext_file_mode_check_skipped_on_windows(monkeypatch, tmp_path, caplog):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    monkeypatch.setenv("AIDP_SECRETS_FILE", _write_secrets_file(tmp_path, {"db": {"password": SECRET_VALUE}}))
    with caplog.at_level(logging.INFO, logger="aidp_compat.secrets"):
        assert AIDPSecretsUtils().get("db", "password") == SECRET_VALUE
    assert "permission check skipped" in caplog.text
    assert SECRET_VALUE not in caplog.text


def test_check_owner_only_helper(tmp_path):
    p = tmp_path / "f.json"
    p.write_text("{}")
    if os.name == "posix":
        os.chmod(str(p), 0o640)
        with pytest.raises(PermissionError):
            secrets_mod._check_owner_only(str(p))
        os.chmod(str(p), 0o600)
    secrets_mod._check_owner_only(str(p))   # no error: owner-only (or Windows skip)


# ── scope allowlist and non-disclosure ────────────────────────────────
def test_scope_allowlist_restricts_get_list_and_listscopes(monkeypatch):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    monkeypatch.setenv("AIDP_SECRET_ALLOWED_TOKEN", SECRET_VALUE)
    monkeypatch.setenv("AIDP_SECRET_HIDDEN_TOKEN", "other-" + SECRET_VALUE)
    monkeypatch.setenv("AIDP_SECRET_SCOPES", "allowed, Other")
    s = AIDPSecretsUtils()
    assert s.listScopes() == [{"name": "allowed"}]
    assert s.get("allowed", "token") == SECRET_VALUE
    assert s.list("allowed") == [{"key": "token", "lastUpdatedTimestamp": 0}]
    with pytest.raises(PermissionError):
        s.get("hidden", "token")
    with pytest.raises(PermissionError):
        s.list("hidden")


def test_scope_allowlist_applies_to_vault_lookups_too(monkeypatch):
    monkeypatch.setenv("AIDP_SECRET_SCOPES", "allowed")
    monkeypatch.setattr(AIDPSecretsUtils, "_get_from_oci_vault",
                        lambda self, scope, key: f"vault:{scope}/{key}")
    s = AIDPSecretsUtils()
    assert s.get("allowed", "k") == "vault:allowed/k"
    with pytest.raises(PermissionError):
        s.get("prod", "k")


def test_unset_or_blank_scope_allowlist_means_unrestricted(monkeypatch):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    monkeypatch.setenv("AIDP_SECRET_A_K", "v")
    monkeypatch.setenv("AIDP_SECRET_SCOPES", "  ")
    assert AIDPSecretsUtils().listScopes() == [{"name": "a"}]


def test_list_operations_never_reveal_values(monkeypatch, capsys):
    _no_vault(monkeypatch)
    monkeypatch.setenv("AIDP_ALLOW_PLAINTEXT_SECRETS", "1")
    monkeypatch.setenv("AIDP_SECRET_DB_PASSWORD", SECRET_VALUE)
    s = AIDPSecretsUtils()
    assert SECRET_VALUE not in repr(s.list("db"))
    assert SECRET_VALUE not in repr(s.listScopes())
    s.help()
    out = capsys.readouterr().out
    assert SECRET_VALUE not in out
    assert "AIDP_ALLOW_PLAINTEXT_SECRETS" in out
