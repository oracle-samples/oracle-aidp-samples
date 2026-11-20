"""Unit tests for scripts/oci_credential_helper.

Injects a stub `aidputils` onto `sys.modules['__main__']` because that is
exactly how the AIDP cluster kernel makes aidputils available at runtime —
NOT as an installable Python module. The stub records every get() call so
we assert the helper fetches exactly the expected (credential_name, key)
pairs — a mismatch would silently authenticate against the wrong credential
in production.
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


# A dummy PEM block that is well-formed enough for oci.signer.Signer to accept.
# We generate a fresh one per test so the tests don't share state.
def _make_dummy_pem() -> str:
    # Generate a valid RSA key on the fly (not cached — avoids fixture-share bugs).
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture
def stub_aidputils(monkeypatch):
    """Mimic AIDP cluster kernel: inject `aidputils` as an attribute on
    `sys.modules['__main__']`. The helper reaches it via
    `_get_aidputils_from_kernel` (getattr on __main__), NOT via
    `import aidputils` (which fails on the real AIDP cluster kernel too)."""
    calls = []
    values = {}

    def _get(name, key=None):
        calls.append((name, key))
        return values[(name, key)]

    aidputils_stub = types.SimpleNamespace(
        secrets=types.SimpleNamespace(get=_get))
    _main = sys.modules["__main__"]
    monkeypatch.setattr(_main, "aidputils", aidputils_stub, raising=False)
    # Belt-and-suspenders: `import aidputils` must fail — the helper must NOT
    # rely on module-level import (broken on cluster).
    monkeypatch.setitem(sys.modules, "aidputils", None)
    return {"calls": calls, "values": values}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Every test starts with no credential-name env overrides."""
    for var in [
        "AIDP_CRED_OCI_NAME",
        "AIDP_CRED_SECRETS_NAME",
        "AIDP_CRED_OCI_KEY_USER",
        "AIDP_CRED_OCI_KEY_FINGERPRINT",
        "AIDP_CRED_OCI_KEY_TENANCY",
        "AIDP_CRED_OCI_KEY_REGION",
        "AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT",
    ]:
        monkeypatch.delenv(var, raising=False)


# ─── get_signer ─────────────────────────────────────────────────────────────

def test_get_signer_fetches_expected_keys_from_default_credential(stub_aidputils):
    """Default credential name is migration_tool_api; default key names are the 5
    OCI Signer fields. get_signer() must fetch all 5 from that credential."""
    pem = _make_dummy_pem()
    v = stub_aidputils["values"]
    v[("migration_tool_api", "tenancy")]           = "ocid1.tenancy.oc1..aaa"
    v[("migration_tool_api", "user")]              = "ocid1.user.oc1..bbb"
    v[("migration_tool_api", "fingerprint")]       = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
    v[("migration_tool_api", "region")]            = "ap-mumbai-1"
    v[("migration_tool_api", "key_file_content")]  = pem

    import oci_credential_helper as h
    signer = h.get_signer()

    # signer construction succeeded
    assert signer is not None
    # get_signer() itself fetches 4 keys (tenancy/user/fingerprint/key_file_content).
    # `region` is fetched separately via get_region() — belongs to the client URL,
    # not the request signature.
    keys_asked = {key for (name, key) in stub_aidputils["calls"] if name == "migration_tool_api"}
    assert keys_asked == {"tenancy", "user", "fingerprint", "key_file_content"}


def test_get_signer_credential_name_overridable_via_env(stub_aidputils, monkeypatch):
    """Setting AIDP_CRED_OCI_NAME changes which credential the helper reads from
    without touching code — supports the aidp.oci_credentials.name YAML wiring."""
    pem = _make_dummy_pem()
    monkeypatch.setenv("AIDP_CRED_OCI_NAME", "custom_creds_alpha")
    v = stub_aidputils["values"]
    v[("custom_creds_alpha", "tenancy")]           = "ocid1.tenancy.oc1..aaa"
    v[("custom_creds_alpha", "user")]              = "ocid1.user.oc1..bbb"
    v[("custom_creds_alpha", "fingerprint")]       = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
    v[("custom_creds_alpha", "region")]            = "ap-mumbai-1"
    v[("custom_creds_alpha", "key_file_content")]  = pem

    import oci_credential_helper as h
    h.get_signer()

    names_asked = {name for (name, key) in stub_aidputils["calls"]}
    assert names_asked == {"custom_creds_alpha"}
    # default credential name must NOT appear
    assert "migration_tool_api" not in names_asked


def test_get_signer_key_names_overridable_via_env(stub_aidputils, monkeypatch):
    """Per-field key-name overrides let a customer rename a key in AIDP UI and
    reconcile via the YAML `keys:` block (wired through env vars at bootstrap)."""
    pem = _make_dummy_pem()
    monkeypatch.setenv("AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT", "pem_content")
    v = stub_aidputils["values"]
    v[("migration_tool_api", "tenancy")]     = "ocid1.tenancy.oc1..aaa"
    v[("migration_tool_api", "user")]        = "ocid1.user.oc1..bbb"
    v[("migration_tool_api", "fingerprint")] = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
    v[("migration_tool_api", "region")]      = "ap-mumbai-1"
    v[("migration_tool_api", "pem_content")] = pem  # renamed key

    import oci_credential_helper as h
    h.get_signer()

    keys_asked = {key for (name, key) in stub_aidputils["calls"]}
    assert "pem_content" in keys_asked
    assert "key_file_content" not in keys_asked  # default not used


def test_get_signer_raises_without_aidputils(monkeypatch):
    """Laptop-side has no aidputils on __main__. Helper must fail loudly with
    ImportError so callers switch to the standard OCI SDK resolution path
    (the docstring on this module tells laptop callers not to import it)."""
    monkeypatch.delattr(sys.modules["__main__"], "aidputils", raising=False)
    monkeypatch.setitem(sys.modules, "aidputils", None)
    import oci_credential_helper as h
    with pytest.raises(ImportError, match="aidputils global not found"):
        h.get_signer()


# ─── get_region ─────────────────────────────────────────────────────────────

def test_get_region_reads_region_key(stub_aidputils):
    stub_aidputils["values"][("migration_tool_api", "region")] = "us-phoenix-1"
    import oci_credential_helper as h
    assert h.get_region() == "us-phoenix-1"


# ─── get_secret ─────────────────────────────────────────────────────────────

def test_get_secret_reads_from_secrets_credential(stub_aidputils):
    """get_secret(key) fetches from migration_tool_secrets — a DIFFERENT credential
    than migration_tool_api. Mixing them would leak OCI creds into the wrong scope."""
    stub_aidputils["values"][("migration_tool_secrets", "mlflow-tracking-uri")] = "https://mlflow.example"
    stub_aidputils["values"][("migration_tool_secrets", "entity_crypto-arn")]          = "arn:aws:sts::111:role/x"

    import oci_credential_helper as h
    assert h.get_secret("mlflow-tracking-uri") == "https://mlflow.example"
    assert h.get_secret("entity_crypto-arn") == "arn:aws:sts::111:role/x"

    names_asked = {name for (name, key) in stub_aidputils["calls"]}
    assert names_asked == {"migration_tool_secrets"}
    assert "migration_tool_api" not in names_asked


def test_get_secret_credential_name_overridable_via_env(stub_aidputils, monkeypatch):
    monkeypatch.setenv("AIDP_CRED_SECRETS_NAME", "custom_secrets_beta")
    stub_aidputils["values"][("custom_secrets_beta", "x")] = "y"

    import oci_credential_helper as h
    assert h.get_secret("x") == "y"

    names_asked = {name for (name, key) in stub_aidputils["calls"]}
    assert names_asked == {"custom_secrets_beta"}
