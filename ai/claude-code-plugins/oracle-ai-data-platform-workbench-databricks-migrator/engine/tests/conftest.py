"""Suite-wide isolation for the aidp_compat security tests.

The notebook policy and the secrets shim read process environment variables
(``AIDP_SANDBOX_*``, ``AIDP_NOTEBOOK_POLICY_*``, ``AIDP_SECRET*``,
``AIDP_ALLOW_PLAINTEXT_SECRETS``). A developer's shell may carry real values,
so every test starts from a clean slate and the process-wide policy / policy
log are reset between tests.
"""
import os
import sys

import pytest

ENGINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ENGINE_DIR not in sys.path:
    sys.path.insert(0, ENGINE_DIR)

try:
    from aidp_compat import notebook_policy
except ImportError:  # pre-SEC-005 tree: the secrets tests can still run
    notebook_policy = None

_SCRUB_PREFIXES = ("AIDP_SANDBOX_", "AIDP_NOTEBOOK_POLICY_", "AIDP_SECRET")
_SCRUB_EXACT = ("AIDP_ALLOW_PLAINTEXT_SECRETS", "AIDP_SECRETS_FILE", "AIDP_MOUNT_CONFIG",
                "AIDP_MOUNTS_JSON", "AIDP_VAULT_OCID")


@pytest.fixture(autouse=True)
def _clean_policy_state(monkeypatch, tmp_path):
    for key in list(os.environ):
        if key.startswith(_SCRUB_PREFIXES) or key in _SCRUB_EXACT:
            monkeypatch.delenv(key, raising=False)
    # Point the fs shim at a mount config that does not exist so no developer
    # mount mapping leaks into path translation.
    monkeypatch.setenv("AIDP_MOUNT_CONFIG", str(tmp_path / "no-mounts.json"))
    if notebook_policy is not None:
        notebook_policy.set_sandbox_policy(None)
        notebook_policy.clear_policy_log()
    yield
    if notebook_policy is not None:
        notebook_policy.set_sandbox_policy(None)
        notebook_policy.clear_policy_log()


@pytest.fixture
def sandbox_env(monkeypatch, tmp_path):
    """Declare a sandbox: catalog ``default``, schema ``sbx``, an OCI prefix and
    a local staging prefix under tmp_path (so file-level assertions can be
    exercised without Object Storage)."""
    local_prefix = str(tmp_path / "sandbox") + os.sep
    os.makedirs(local_prefix, exist_ok=True)
    monkeypatch.setenv("AIDP_SANDBOX_CATALOG", "default")
    monkeypatch.setenv("AIDP_SANDBOX_SCHEMA", "sbx")
    monkeypatch.setenv("AIDP_SANDBOX_PREFIX", "oci://b@ns/sbx/," + local_prefix)
    return {"catalog": "default", "schema": "sbx", "prefix": "oci://b@ns/sbx/",
            "local_prefix": local_prefix}
