"""Verify dbx_export/auth.py's get_oci_signer takes the right branch.

dbx_export runs on the AIDP cluster (via `%run dbx_export/run.py` — DBX
endpoint is not reachable from laptop for the pilot deployment and similar customers).
So `get_oci_signer` MUST prefer the AIDP Credential Store path when
aidputils is present on __main__ (the AIDP kernel injection), and fall
back to standard OCI SDK resolution only when it isn't (laptop dev with
direct DBX access, or off-cluster tests).

Guards against three regressions:
  1. Silently reading `oci.config.from_file()` on cluster (Phase-2 goal)
  2. Silently ignoring `AIDP_CRED_OCI_KEY_*` env-var renames
  3. Breaking the `security_token_file` profile on laptop
"""
from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


def _make_dummy_pem() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """dbx_export.auth reads AIDP_CRED_OCI_NAME + AIDP_CRED_OCI_KEY_* +
    OCI_CONFIG_FILE — isolate every test from ambient env."""
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


@pytest.fixture
def stub_aidputils_on_main(monkeypatch):
    """Mimic AIDP cluster kernel: inject `aidputils` as an attribute on
    `sys.modules['__main__']`. dbx_export/auth.py::_get_aidputils_from_kernel
    reaches it via `getattr(sys.modules['__main__'], 'aidputils', None)`."""
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
    pem = _make_dummy_pem()
    values[("migration_tool_api", "tenancy")]           = "ocid1.tenancy.oc1..t"
    values[("migration_tool_api", "user")]              = "ocid1.user.oc1..u"
    values[("migration_tool_api", "fingerprint")]       = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
    values[("migration_tool_api", "region")]            = "ap-mumbai-1"
    values[("migration_tool_api", "key_file_content")]  = pem
    return {"calls": calls, "values": values}


@pytest.fixture
def no_aidputils_on_main(monkeypatch):
    """Mimic laptop / off-cluster: `__main__` has no `aidputils` attribute
    AND `import aidputils` fails. dbx_export/auth must fall through to
    standard OCI SDK resolution."""
    monkeypatch.delattr(sys.modules["__main__"], "aidputils", raising=False)
    monkeypatch.setitem(sys.modules, "aidputils", None)


# ─── Cluster path ─────────────────────────────────────────────────────

def test_cluster_path_uses_aidputils_and_never_reads_config_file(stub_aidputils_on_main):
    """On cluster (aidputils on __main__), get_oci_signer MUST NOT call
    oci.config.from_file — Phase-2 goal is no OCI config file on cluster."""
    from dbx_export.auth import get_oci_signer
    with patch("oci.config.from_file") as file_config:
        config, signer = get_oci_signer()
    assert file_config.call_count == 0, \
        "cluster path must not touch a config file"
    # 5 keys fetched from the credential store — the exact set AIDP's
    # validate_config requires (region/tenancy/user/fingerprint/key_content).
    assert set(config.keys()) == {
        "region", "tenancy", "user", "fingerprint", "key_content",
    }
    # And nothing leaked to a different credential name.
    creds_asked = {name for (name, _) in stub_aidputils_on_main["calls"]}
    assert creds_asked == {"migration_tool_api"}
    keys_asked = {key for (_, key) in stub_aidputils_on_main["calls"]}
    assert keys_asked == {
        "tenancy", "user", "fingerprint", "region", "key_file_content"
    }


def test_cluster_path_ignores_config_path_arg(stub_aidputils_on_main):
    """When aidputils is present on cluster, config_path arg must be
    silently absorbed (not read as a file). Old --oci-config-path invocations
    that still pass a path continue to work — the arg becomes a no-op."""
    from dbx_export.auth import get_oci_signer
    with patch("oci.config.from_file") as file_config:
        get_oci_signer(config_path="/should/never/be/read")
    assert file_config.call_count == 0


def test_cluster_path_credential_name_overridable_via_env(monkeypatch, stub_aidputils_on_main):
    """A customer renames the credential in AIDP UI — env var AIDP_CRED_OCI_NAME
    lets get_oci_signer pick it up without a code change."""
    # Re-key the stub's values dict for the new credential name.
    for (name, key), val in list(stub_aidputils_on_main["values"].items()):
        stub_aidputils_on_main["values"][("custom_cred_alpha", key)] = val
    monkeypatch.setenv("AIDP_CRED_OCI_NAME", "custom_cred_alpha")
    from dbx_export.auth import get_oci_signer
    config, signer = get_oci_signer()
    creds_asked = {name for (name, _) in stub_aidputils_on_main["calls"]}
    assert creds_asked == {"custom_cred_alpha"}


def test_cluster_path_key_names_overridable_via_env(monkeypatch, stub_aidputils_on_main):
    """A customer renames the PEM key in AIDP UI — AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT
    env var picks up the new name without editing dbx_export."""
    monkeypatch.setenv("AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT", "pem_content_v2")
    stub_aidputils_on_main["values"][("migration_tool_api", "pem_content_v2")] = \
        stub_aidputils_on_main["values"].pop(("migration_tool_api", "key_file_content"))
    from dbx_export.auth import get_oci_signer
    get_oci_signer()
    keys_asked = {key for (_, key) in stub_aidputils_on_main["calls"]}
    assert "pem_content_v2" in keys_asked
    assert "key_file_content" not in keys_asked


# ─── Laptop / off-cluster fallback ────────────────────────────────────

def test_laptop_falls_back_to_config_file(no_aidputils_on_main):
    """Off-cluster (no aidputils on __main__) must fall through to
    oci.config.from_file — customers with direct DBX access from laptop
    still work via standard OCI SDK resolution."""
    from dbx_export.auth import get_oci_signer
    fake_cfg = {
        "tenancy": "ocid1.tenancy.oc1..t",
        "user": "ocid1.user.oc1..u",
        "fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
        "key_file": "/nonexistent/key.pem",
        "region": "us-phoenix-1",
    }
    with patch("oci.config.from_file", return_value=fake_cfg) as file_config, \
         patch("oci.signer.Signer") as signer_ctor:
        get_oci_signer(profile="DEFAULT")
        assert file_config.call_count == 1
        signer_ctor.assert_called_once()


def test_laptop_security_token_profile_still_works(no_aidputils_on_main, tmp_path):
    """Federated laptop dev uses OCI security-token profiles (via
    `oci session authenticate`). dbx_export/auth.py's laptop fallback
    detects security_token_file and returns a SecurityTokenSigner instead
    of an API-key Signer. This path must survive the cluster-branch add."""
    from dbx_export.auth import get_oci_signer
    # Fake token + PEM files pointed at by the fake config.
    tok = tmp_path / "session-token"
    tok.write_text("fake-token-blob\n")
    pem_path = tmp_path / "key.pem"
    pem_path.write_text(_make_dummy_pem())
    fake_cfg = {
        "security_token_file": str(tok),
        "key_file": str(pem_path),
        "region": "us-ashburn-1",
        "pass_phrase": None,
    }
    with patch("oci.config.from_file", return_value=fake_cfg), \
         patch("oci.signer.load_private_key_from_file") as loader, \
         patch("oci.auth.signers.SecurityTokenSigner") as st_ctor:
        loader.return_value = MagicMock()
        get_oci_signer(profile="SESSION_PROFILE")
        st_ctor.assert_called_once()
        # Pin the ARGUMENTS, not just the call: the token string read from
        # security_token_file and the key loaded from key_file must be what
        # reaches the signer (a mutation that swaps/breaks either still
        # passed the ctor-called-only assertion).
        st_args, st_kwargs = st_ctor.call_args
        flat = list(st_args) + list(st_kwargs.values())
        assert "fake-token-blob" in flat, "token not passed to SecurityTokenSigner"
        assert loader.return_value in flat, "loaded key not passed to SecurityTokenSigner"
        loader.assert_called_once()
        assert loader.call_args[0][0] == str(pem_path), "wrong key_file path to loader"


def test_cluster_detection_survives_ipython_percent_run_main_swap(monkeypatch):
    """IPython-semantics %run swaps sys.modules['__main__'] to a FRESH module
    for the script's duration (IPython core/magics/execution.py) — hiding the
    kernel-injected aidputils global. Since dbx_export's own documented
    deployment is `%run dbx_export/run.py`, the cluster detection must also
    reach the kernel's user namespace via get_ipython().user_ns, or the code
    silently takes the laptop fallback ON CLUSTER (and, with --oci-config-path
    passed per the old run.py example, silently reads a workspace config file
    — defeating Phase 2). This test simulates the swap: fresh __main__ WITHOUT
    aidputils + a get_ipython() whose user_ns HAS it."""
    import types as _types
    from dbx_export import auth as _auth

    kernel_aidputils = object()

    fresh_main = _types.ModuleType("__main__")
    monkeypatch.setitem(sys.modules, "__main__", fresh_main)

    class _FakeIPython:
        user_ns = {"aidputils": kernel_aidputils}

    fake_ipython_mod = _types.ModuleType("IPython")
    fake_ipython_mod.get_ipython = lambda: _FakeIPython()
    monkeypatch.setitem(sys.modules, "IPython", fake_ipython_mod)

    assert _auth._get_aidputils_from_kernel() is kernel_aidputils, (
        "cluster detection lost the kernel aidputils across the %run "
        "__main__ swap — laptop fallback would run on cluster"
    )

    # And with NO IPython at all (plain laptop python), still cleanly None.
    monkeypatch.delitem(sys.modules, "IPython")
    import builtins as _builtins
    _real_import = _builtins.__import__
    def _no_ipython(name, *a, **kw):
        if name == "IPython":
            raise ImportError("no IPython on laptop")
        return _real_import(name, *a, **kw)
    monkeypatch.setattr(_builtins, "__import__", _no_ipython)
    assert _auth._get_aidputils_from_kernel() is None


def test_dbx_export_auth_source_has_no_hardcoded_workspace_config_path():
    """Guard against silently reintroducing a hardcoded workspace config
    file path (e.g. `/Workspace/connection-keys/Config`) inside
    dbx_export/auth.py — same guard as aidp_compat has."""
    from pathlib import Path
    repo_root = Path(__file__).resolve().parents[1]
    src = (repo_root / "dbx_export" / "auth.py").read_text()
    forbidden_snippets = [
        "/Workspace/connection-keys",
        "connection-keys/Config",
    ]
    for snippet in forbidden_snippets:
        assert snippet not in src, (
            f"dbx_export/auth.py must not hardcode {snippet!r} — customers vary"
        )
