"""Regression tests for build_env_bootstrap_snippet — the kernel-bootstrap snippet
that seeds os.environ on the cluster with AIDP_REGION / AIDP_LAKE_OCID /
AIDP_WORKSPACE_ID from cfg. These seed the MIGRATION kernel only — the snippet
is not saved into the notebook. The job-runtime contract for the same values
(the env AIDP sets on every cluster, read by the trigger template) is pinned in
tests/test_job_trigger_coordinates.py (REF-208).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture(autouse=True)
def _reset_jm_globals(monkeypatch):
    """job_migrate module globals are set by main()/apply_cfg_to_modules; the
    snippet reads them directly. Fake them per-test so we don't need a full
    cfg load."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "AIDP_BASE",     "https://aidp.ap-mumbai-1.oci.oraclecloud.com/20240831", raising=False)
    monkeypatch.setattr(jm, "DATALAKE_OCID", "ocid1.aidataplatform.oc1.ap-mumbai-1.abcXYZ",         raising=False)
    monkeypatch.setattr(jm, "WORKSPACE_ID",  "00000000-0000-4000-8000-000000000001",                raising=False)
    for k in ["AIDP_CRED_OCI_NAME", "AIDP_CRED_SECRETS_NAME",
              "AIDP_CRED_OCI_KEY_USER", "AIDP_CRED_OCI_KEY_FINGERPRINT",
              "AIDP_CRED_OCI_KEY_TENANCY", "AIDP_CRED_OCI_KEY_REGION",
              "AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT"]:
        monkeypatch.delenv(k, raising=False)


def _exec_snippet(snippet: str) -> dict:
    """Parse the snippet's `os.environ.setdefault('K', 'V')` calls into a dict.

    Text-based extraction (rather than exec-with-monkeypatch) avoids mutating
    the real os.environ during tests. The snippet's output is deterministic
    — it emits only `os.environ.setdefault(<repr>, <repr>)` lines — so ast
    parsing is precise and immune to test-order interactions.
    """
    import ast
    seen: dict = {}
    tree = ast.parse(snippet)
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Subscript)
                and ast.unparse(node.targets[0].value) == "os.environ"
                and isinstance(node.value, ast.Constant)):
            seen[node.targets[0].slice.value] = node.value.value
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setdefault"
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "environ"
                and len(node.args) == 2
                and all(isinstance(a, ast.Constant) for a in node.args)):
            seen[node.args[0].value] = node.args[1].value
    return seen


def test_snippet_emits_region_lake_workspace_env_vars():
    import job_migrate as jm
    snippet = jm.build_env_bootstrap_snippet()
    written = _exec_snippet(snippet)
    assert written["AIDP_REGION"] == "ap-mumbai-1"
    assert written["AIDP_LAKE_OCID"] == "ocid1.aidataplatform.oc1.ap-mumbai-1.abcXYZ"
    assert written["AIDP_WORKSPACE_ID"] == "00000000-0000-4000-8000-000000000001"


def test_snippet_region_derived_from_aidp_base():
    """Region comes from parsing AIDP_BASE URL, not from a separate module global."""
    import job_migrate as jm
    import types
    jm.AIDP_BASE = "https://aidp.us-phoenix-1.oci.oraclecloud.com/20240831"
    snippet = jm.build_env_bootstrap_snippet()
    written = _exec_snippet(snippet)
    assert written["AIDP_REGION"] == "us-phoenix-1"


def test_snippet_omits_default_credential_name_overrides():
    """When credential names are at their defaults, snippet must NOT set
    AIDP_CRED_OCI_NAME (would clobber a customer's shell-level override)."""
    import job_migrate as jm
    snippet = jm.build_env_bootstrap_snippet()
    written = _exec_snippet(snippet)
    assert "AIDP_CRED_OCI_NAME" not in written
    assert "AIDP_CRED_SECRETS_NAME" not in written


def test_snippet_emits_non_default_credential_name_overrides(monkeypatch):
    """When laptop side has AIDP_CRED_OCI_NAME set (from cfg.aidp.oci_credentials.name),
    the snippet must propagate it to the cluster kernel."""
    monkeypatch.setenv("AIDP_CRED_OCI_NAME", "custom_creds_alpha")
    monkeypatch.setenv("AIDP_CRED_SECRETS_NAME", "custom_secrets_beta")
    import job_migrate as jm
    snippet = jm.build_env_bootstrap_snippet()
    written = _exec_snippet(snippet)
    assert written["AIDP_CRED_OCI_NAME"] == "custom_creds_alpha"
    assert written["AIDP_CRED_SECRETS_NAME"] == "custom_secrets_beta"


def test_snippet_emits_per_field_key_overrides(monkeypatch):
    """Per-field key overrides for OCI credential fields (from
    cfg.aidp.oci_credentials.keys) are propagated to cluster kernel."""
    monkeypatch.setenv("AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT", "pem_content_v2")
    monkeypatch.setenv("AIDP_CRED_OCI_KEY_TENANCY", "tenant_ocid")
    import job_migrate as jm
    snippet = jm.build_env_bootstrap_snippet()
    written = _exec_snippet(snippet)
    assert written["AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT"] == "pem_content_v2"
    assert written["AIDP_CRED_OCI_KEY_TENANCY"] == "tenant_ocid"
    # Non-overridden fields must NOT be emitted (avoid clobbering shell defaults)
    assert "AIDP_CRED_OCI_KEY_USER" not in written


def test_snippet_uses_setdefault_not_setitem():
    """Coordinates retain setdefault; absent OCI config clears stale references."""
    import job_migrate as jm
    snippet = jm.build_env_bootstrap_snippet()
    assert "os.environ.setdefault" in snippet
    assert "os.environ[" not in snippet
    assert "os.environ.pop('AIDP_CRED_OCI_NAME', None)" in snippet
