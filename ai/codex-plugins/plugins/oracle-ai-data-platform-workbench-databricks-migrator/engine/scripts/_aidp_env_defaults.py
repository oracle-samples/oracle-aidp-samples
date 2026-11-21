"""Shared helper: load customer AIDP defaults for standalone utility scripts.

Ancillary utility scripts (fetch_spark_logs.py, deploy_aidp_compat.py,
install_cluster_libraries.py, ...) each need lake_ocid / workspace_id /
oci_profile / region to build their own AIDP URLs and OCI signers. Before
Phase 2 they hardcoded these to the pilot deployment values, blocking generic use.

Resolution order (first non-empty wins):
  1. aidp_config.load_config() — reads whatever ./aidp_migrator.yaml,
     ~/aidp_migrator.yaml, or $AIDP_CONFIG_PATH points at.
  2. Env vars AIDP_LAKE_OCID / AIDP_WORKSPACE_ID / AIDP_OCI_PROFILE / AIDP_REGION
     (these match the loader's `_ENV_OVERRIDES` whitelist).
  3. Empty string — scripts print a clear error at API-call time rather than
     crashing at import.

Import-time silent — never raises. Scripts that require these values check
them at use time; scripts that only need a subset don't pay for a missing one.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _ensure_repo_on_path() -> None:
    """aidp_config lives at repo root. Add it to sys.path if not already there."""
    here = Path(__file__).resolve().parent
    repo_root = here.parent
    for p in (str(repo_root), str(here)):
        if p not in sys.path:
            sys.path.insert(0, p)


def load_aidp_defaults() -> dict:
    """Return {lake_ocid, workspace_id, oci_profile, region, migrate_output} for
    the current customer.

    Never raises. Missing values come back as empty strings.
    """
    _ensure_repo_on_path()
    try:
        from aidp_config import load_config
        c = load_config()
        return {
            "lake_ocid":      c.aidp.lake_ocid,
            "workspace_id":   c.aidp.workspace_id,
            "oci_profile":    c.aidp.oci_profile,
            "region":         c.aidp.region,
            "migrate_output": c.paths.migrate_output,
        }
    except Exception as e:
        # Env-var fallback (matches loader.py::_ENV_OVERRIDES key names).
        # Never raises by contract, but a BROKEN yaml (parse/validation error,
        # as opposed to no yaml at all) must not be silently identical to
        # "no yaml": warn so the operator knows their file was ignored.
        if type(e).__name__ == "ConfigError" and "No customer configuration file found" not in str(e):
            print(f"[aidp_env_defaults] WARN: aidp_migrator.yaml was found but "
                  f"could not be used ({str(e).splitlines()[0]}) — falling back "
                  f"to AIDP_* env vars.", file=sys.stderr)
        return {
            "lake_ocid":      os.environ.get("AIDP_LAKE_OCID", ""),
            "workspace_id":   os.environ.get("AIDP_WORKSPACE_ID", ""),
            "oci_profile":    os.environ.get("AIDP_OCI_PROFILE", "DEFAULT"),
            "region":         os.environ.get("AIDP_REGION", ""),
            "migrate_output": os.environ.get("AIDP_MIGRATE_OUTPUT", ""),
        }


def aidp_base_url(region: str) -> str:
    """Compose the AIDP data-plane REST base URL for a region."""
    return f"https://aidp.{region}.oci.oraclecloud.com/20240831"
