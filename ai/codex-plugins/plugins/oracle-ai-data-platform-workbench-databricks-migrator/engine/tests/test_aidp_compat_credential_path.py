"""Verify aidp_compat modules use aidputils.secrets on cluster (never a config
file) and fall back to file-based OCI SDK resolution on laptop dev.

These tests guard against regressions where a code path in aidp_compat/fs.py,
secrets.py, or budget.py silently reintroduces a hardcoded workspace config
file path (e.g. `/Workspace/connection-keys/Config`) or bypasses the
credential store entirely.
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_dummy_pem() -> str:
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
    """Mimic the AIDP cluster kernel: inject `aidputils` as an attribute on
    `sys.modules['__main__']` (NOT as a real Python module). The kernel does
    the same thing at startup — `import aidputils` fails on AIDP; you reach
    it via `sys.modules['__main__'].aidputils`. See aidp_compat/fs.py::
    _get_aidputils_from_kernel for the cluster-side lookup path."""
    calls = []
    values = {}

    def _get(name, key=None):
        calls.append((name, key))
        return values[(name, key)]

    aidputils_stub = types.SimpleNamespace(
        secrets=types.SimpleNamespace(get=_get))
    _main = sys.modules["__main__"]
    monkeypatch.setattr(_main, "aidputils", aidputils_stub, raising=False)
    # Belt-and-suspenders: also ensure `import aidputils` FAILS during the
    # test — the code we're validating must NOT rely on module-level import.
    monkeypatch.setitem(sys.modules, "aidputils", None)
    # Seed with a full valid credential
    pem = _make_dummy_pem()
    values[("migration_tool_api", "tenancy")]           = "ocid1.tenancy.oc1..t"
    values[("migration_tool_api", "user")]              = "ocid1.user.oc1..u"
    values[("migration_tool_api", "fingerprint")]       = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
    values[("migration_tool_api", "region")]            = "ap-mumbai-1"
    values[("migration_tool_api", "key_file_content")]  = pem
    return {"calls": calls, "values": values}


@pytest.fixture(autouse=True)
def _reset_fs_client_cache():
    """aidp_compat/fs.py caches its OCI client at module level. Reset between
    tests so each test exercises the cache-miss branch (the branch we're
    validating). Without this, tests running after the first would use whatever
    client the first test constructed."""
    import aidp_compat.fs as _fs
    _fs._oci_storage_client = None
    _fs._oci_storage_namespace = None
    _fs._oci_storage_region = None
    yield
    _fs._oci_storage_client = None
    _fs._oci_storage_namespace = None
    _fs._oci_storage_region = None


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in [
        "AIDP_CRED_OCI_NAME",
        "AIDP_CRED_OCI_KEY_USER",
        "AIDP_CRED_OCI_KEY_FINGERPRINT",
        "AIDP_CRED_OCI_KEY_TENANCY",
        "AIDP_CRED_OCI_KEY_REGION",
        "AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT",
        "OCI_CONFIG_FILE",
        "OCI_CONFIG_PROFILE",
    ]:
        monkeypatch.delenv(var, raising=False)


# ─── aidp_compat/fs.py ─────────────────────────────────────────────────────

def test_fs_uses_aidputils_when_available_and_never_reads_config_file(stub_aidputils):
    """On cluster (aidputils present), fs.py must NOT call oci.config.from_file —
    doing so would still read `/Workspace/connection-keys/Config` in
    production. All 4 signer keys + region must come from aidputils.secrets.get."""
    import aidp_compat.fs as _fs
    with patch("oci.config.from_file") as file_config, \
         patch("oci.object_storage.ObjectStorageClient") as osc:
        _fs._get_oci_storage_client()
        assert file_config.call_count == 0, "fs.py must not touch a config file on cluster"
        osc.assert_called_once()
    # Every fetch went to migration_tool_api — nothing leaked to migration_tool_secrets etc.
    creds_asked = {name for (name, _) in stub_aidputils["calls"]}
    assert creds_asked == {"migration_tool_api"}
    keys_asked = {key for (_, key) in stub_aidputils["calls"]}
    assert keys_asked == {"tenancy", "user", "fingerprint", "region", "key_file_content"}


def test_fs_raises_when_aidputils_missing(monkeypatch):
    """aidp_compat/fs.py is cluster-only — it must NEVER read an OCI config
    file. When `aidputils` is not injected on `__main__` (i.e., not on an
    AIDP cluster kernel), `_get_oci_storage_client()` must raise ImportError
    with a clear message, not silently fall through to a config file."""
    # Ensure no aidputils attribute on __main__
    monkeypatch.delattr(sys.modules["__main__"], "aidputils", raising=False)
    monkeypatch.setitem(sys.modules, "aidputils", None)
    import aidp_compat.fs as _fs
    with patch("oci.config.from_file") as file_config, \
         patch("oci.object_storage.ObjectStorageClient") as osc:
        with pytest.raises(ImportError, match="aidputils global not found"):
            _fs._get_oci_storage_client()
        assert file_config.call_count == 0, \
            "fs.py must NEVER call oci.config.from_file (cluster-only module)"
        assert osc.call_count == 0, \
            "no client should be constructed when aidputils is unavailable"


def test_fs_key_names_overridable_via_env(monkeypatch, stub_aidputils):
    """A customer renames the PEM key in AIDP UI — env var lets fs.py pick it up."""
    monkeypatch.setenv("AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT", "pem_content_v2")
    stub_aidputils["values"][("migration_tool_api", "pem_content_v2")] = \
        stub_aidputils["values"].pop(("migration_tool_api", "key_file_content"))
    import aidp_compat.fs as _fs
    with patch("oci.object_storage.ObjectStorageClient"):
        _fs._get_oci_storage_client()
    keys_asked = {key for (_, key) in stub_aidputils["calls"]}
    assert "pem_content_v2" in keys_asked
    assert "key_file_content" not in keys_asked


# ─── aidp_compat/budget.py ────────────────────────────────────────────────

def test_budget_make_signer_prefers_explicit_cfg(stub_aidputils):
    """Explicit cfg dict (arg) beats aidputils. Preserves callers that already
    pass a config — no behavior change for them."""
    import aidp_compat.budget as _b
    explicit_cfg = {"tenancy": "t2", "user": "u2", "fingerprint": "f2",
                    "key_file": "/tmp/x.pem", "region": "us-phoenix-1"}
    with patch("oci.signer.Signer") as signer_ctor:
        _b.make_signer(cfg=explicit_cfg)
        args, kwargs = signer_ctor.call_args
        # aidputils was NOT queried (calls list empty)
        assert len(stub_aidputils["calls"]) == 0
        assert kwargs["tenancy"] == "t2"


def test_budget_make_signer_uses_aidputils_when_no_cfg(stub_aidputils):
    """No explicit cfg + aidputils present → aidputils fetch, no file read."""
    import aidp_compat.budget as _b
    with patch("oci.config.from_file") as file_config, \
         patch("oci.signer.Signer") as signer_ctor:
        _b.make_signer()
        file_config.assert_not_called()  # never touches disk on cluster path
        signer_ctor.assert_called_once()
    # 4 fetches: tenancy, user, fingerprint, key_file_content (region is a client
    # concern, not a signer concern — budget.make_signer doesn't need it).
    assert len(stub_aidputils["calls"]) == 4


def test_budget_make_signer_falls_back_when_no_cfg_no_aidputils(monkeypatch):
    """No cfg + no aidputils → falls back to load_oci_config (file path)."""
    monkeypatch.delitem(sys.modules, "aidputils", raising=False)
    real_import = __import__

    def _blocker(name, *a, **kw):
        if name == "aidputils":
            raise ImportError("simulated laptop dev: no aidputils")
        return real_import(name, *a, **kw)

    monkeypatch.setattr("builtins.__import__", _blocker)
    import aidp_compat.budget as _b
    fake_cfg = {"tenancy": "t", "user": "u", "fingerprint": "f",
                "key_file": "/nonexistent.pem", "region": "us-phoenix-1"}
    with patch.object(_b, "load_oci_config", return_value=fake_cfg) as loader, \
         patch("oci.signer.Signer"):
        _b.make_signer()
        loader.assert_called_once()  # laptop fallback engaged


# ─── Regression guard: aidp_compat must NOT reference the deleted hardcoded path ────

def test_aidp_compat_source_has_no_hardcoded_workspace_config_path():
    """After Phase 2 the /Workspace/connection-keys/Config literal must
    NOT appear in aidp_compat source. A recurrence would silently reintroduce
    the file-based auth path we removed."""
    forbidden = "/Workspace/connection-keys/Config"
    aidp_compat_dir = REPO_ROOT / "aidp_compat"
    hits = []
    for py in aidp_compat_dir.rglob("*.py"):
        if "build/" in str(py):
            continue
        text = py.read_text(encoding="utf-8", errors="ignore")
        if forbidden in text:
            hits.append(str(py.relative_to(REPO_ROOT)))
    assert not hits, f"Forbidden path re-introduced in aidp_compat: {hits}"
