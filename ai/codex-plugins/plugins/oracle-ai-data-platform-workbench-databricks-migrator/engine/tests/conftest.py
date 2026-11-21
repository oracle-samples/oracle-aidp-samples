"""Suite-wide isolation for the aidp_compat security tests.

The notebook policy and the secrets shim read process environment variables
(``AIDP_SANDBOX_*``, ``AIDP_NOTEBOOK_POLICY_*``, ``AIDP_SECRET*``,
``AIDP_ALLOW_PLAINTEXT_SECRETS``). A developer's shell may carry real values,
so every test starts from a clean slate and the process-wide policy / policy
log are reset between tests.
"""
import copy
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

_SCRUB_PREFIXES = ("AIDP_SANDBOX_", "AIDP_NOTEBOOK_POLICY_", "AIDP_SECRET", "_AIDP_PLANNED_")
_SCRUB_EXACT = ("AIDP_ALLOW_PLAINTEXT_SECRETS", "AIDP_SECRETS_FILE", "AIDP_MOUNT_CONFIG",
                "AIDP_MOUNTS_JSON", "AIDP_VAULT_OCID")


@pytest.fixture(autouse=True)
def _clear_planned_secret_refs():
    """job_migrate accumulates planned dbutils.secrets refs in a module global
    (reset per job in production); isolate tests from each other."""
    jm = sys.modules.get("job_migrate")
    if jm is not None and hasattr(jm, "clear_planned_secret_refs"):
        jm.clear_planned_secret_refs()
    yield
    jm = sys.modules.get("job_migrate")
    if jm is not None and hasattr(jm, "clear_planned_secret_refs"):
        jm.clear_planned_secret_refs()


@pytest.fixture(autouse=True)
def _clean_policy_state(monkeypatch, tmp_path):
    for key in list(os.environ):
        if key.startswith(_SCRUB_PREFIXES) or key in _SCRUB_EXACT:
            monkeypatch.delenv(key, raising=False)
    # Point the fs shim at a mount config that does not exist so no developer
    # mount mapping leaks into path translation.
    monkeypatch.setenv("AIDP_MOUNT_CONFIG", str(tmp_path / "no-mounts.json"))
    if notebook_policy is not None:
        # The policy is a frozen one-shot snapshot per process; only the test
        # hook may forget it (set_sandbox_policy(None) is refused once declared).
        notebook_policy._reset_policy_state()
        notebook_policy.clear_policy_log()
    yield
    if notebook_policy is not None:
        notebook_policy._reset_policy_state()
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

@pytest.fixture(autouse=True)
def _restore_exporter_repository(monkeypatch):
    """Exporter CLI tests must not leak their configured repository to others."""
    from dbx_export import aidp_client

    monkeypatch.setattr(
        aidp_client, "_artifact_alias_dirs_configured",
        aidp_client._artifact_alias_dirs_configured,
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "real_registry: run job_migrate's real registry read/write code")


@pytest.fixture(autouse=True)
def _in_memory_migration_registry(request, monkeypatch):
    """Most fake kernels in this suite do not model the job's registry file. The
    registry read and save fail the task when the kernel does not answer them,
    so those tests get an in-memory registry instead. Registry I/O tests opt out
    with ``pytest.mark.real_registry``."""
    jm = sys.modules.get("job_migrate")
    if jm is None or request.node.get_closest_marker("real_registry"):
        return
    saved = {}

    async def load_registry(session, job_name):
        return jm._registry_cache.setdefault(job_name, copy.deepcopy(saved.get(job_name, {})))

    async def save_registry(session, job_name, registry):
        jm._registry_cache[job_name] = registry
        saved[job_name] = copy.deepcopy(registry)

    monkeypatch.setattr(jm, "_registry_cache", {})
    monkeypatch.setattr(jm, "load_registry", load_registry)
    monkeypatch.setattr(jm, "save_registry", save_registry)
