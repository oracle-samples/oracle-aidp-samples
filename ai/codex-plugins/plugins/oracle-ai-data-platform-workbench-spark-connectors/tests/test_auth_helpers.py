"""Unit tests for the auth helpers — no live OCI calls."""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import pytest

from oracle_ai_data_platform_connectors.auth import oci_config, secrets, wallet


# --- wallet -----------------------------------------------------------------


def _make_wallet_zip(files: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def test_wallet_extracts_zip_to_tmp(tmp_path, monkeypatch):
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    target = tmp_path / "wallet"
    # Reuse helper but force /tmp prefix-check by aliasing tmp_path under /tmp.
    # We can't actually create files under /tmp on every CI runner reliably,
    # so we patch the prefix check.
    zip_bytes = _make_wallet_zip(
        {"tnsnames.ora": "X", "cwallet.sso": b"\x00\x01"}
    )

    # Patch the prefix guard for the duration of this test so /tmp_path counts.
    real_resolve = Path.resolve

    def fake_resolve(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return real_resolve(self, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", fake_resolve)
    monkeypatch.setattr(
        wallet, "_write_world_readable",
        lambda p, d: Path(p).write_bytes(d),
    )

    # Bypass the /tmp guard by passing a path that starts with /tmp literally.
    # On Windows under bash the test path is real, so we just smoke-test the
    # zip extraction logic via the internal helper.
    out_dir = tmp_path / "extracted"
    out_dir.mkdir()
    wallet._extract_zip_bytes_to(zip_bytes, out_dir)

    assert (out_dir / "tnsnames.ora").read_text() == "X"
    assert (out_dir / "cwallet.sso").read_bytes() == b"\x00\x01"


def test_wallet_rejects_non_tmp_target(tmp_path):
    with pytest.raises(ValueError, match="must be under /tmp"):
        wallet.write_wallet_to_tmp(b"", target_dir=str(tmp_path))


def test_wallet_rejects_missing_path():
    with pytest.raises(FileNotFoundError):
        wallet.write_wallet_to_tmp("/tmp/does-not-exist-xyz.zip", target_dir="/tmp/x")


# --- oci_config -------------------------------------------------------------


def test_from_inline_pem_returns_key_content_not_key_file():
    config = oci_config.from_inline_pem(
        user_ocid="ocid1.user.oc1..u",
        tenancy_ocid="ocid1.tenancy.oc1..t",
        fingerprint="aa:bb",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nMI...\n-----END PRIVATE KEY-----",
        region="us-ashburn-1",
    )

    # Critical: key_content is set, key_file is NOT (avoids FUSE round-trip).
    assert "key_content" in config
    assert "key_file" not in config
    assert config["region"] == "us-ashburn-1"


def test_from_inline_pem_includes_passphrase_when_provided():
    config = oci_config.from_inline_pem(
        user_ocid="u",
        tenancy_ocid="t",
        fingerprint="aa:bb",
        private_key_pem="x",
        region="us-ashburn-1",
        pass_phrase="secret",
    )
    assert config["pass_phrase"] == "secret"


def test_from_inline_pem_omits_passphrase_when_none():
    config = oci_config.from_inline_pem(
        user_ocid="u",
        tenancy_ocid="t",
        fingerprint="aa:bb",
        private_key_pem="x",
        region="us-ashburn-1",
    )
    assert "pass_phrase" not in config


# --- secrets ----------------------------------------------------------------


def test_secret_resolves_from_env(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("MY_SECRET", "from-env")
    assert secrets.get_secret("my_secret") == "from-env"


def test_secret_uppercase_lookup(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("ATP_PASSWORD", "p")
    # Lowercase name, env var is uppercase — should still resolve.
    assert secrets.get_secret("atp_password") == "p"


def test_secret_returns_default_when_missing(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
    assert secrets.get_secret("does_not_exist", default="fallback") == "fallback"


def test_secret_raises_when_no_default(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.delenv("MISSING_NO_DEFAULT", raising=False)
    with pytest.raises(KeyError):
        secrets.get_secret("missing_no_default")


def test_secret_vault_failure_falls_through_to_env(monkeypatch):
    """If OCI Vault lookup raises, env-var fallback still works."""
    monkeypatch.setenv("OCI_VAULT_ID", "ocid1.vault.oc1..fake")
    monkeypatch.setenv("FALLBACK_VAR", "from-env")

    def boom(*_a, **_k):
        raise RuntimeError("vault unreachable")

    monkeypatch.setattr(secrets, "_get_from_vault", boom)
    assert secrets.get_secret("fallback_var") == "from-env"
