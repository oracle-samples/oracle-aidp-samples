#!/usr/bin/env python3
"""
Install Libraries on AIDP Compute Cluster
==========================================
Uses the AIDP cluster libraries API to install JARs and pip packages.
This is the proper way - no manual file copying or cluster restarts needed.

Usage:
    python3 install_cluster_libraries.py [--cluster <id>]
    python3 install_cluster_libraries.py --status  # check install status
"""

import json
import os
import sys
import time
import argparse
import oci
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _aidp_env_defaults import load_aidp_defaults, aidp_base_url
_defs = load_aidp_defaults()
DATALAKE_OCID = _defs["lake_ocid"]
WORKSPACE_ID  = _defs["workspace_id"]
OCI_PROFILE   = _defs["oci_profile"]
AIDP_BASE     = aidp_base_url(_defs["region"]) if _defs["region"] else ""

# Baseline JARs (generic Spark deps, not customer-specific).
# Per-job JARs are attached separately by dbx_export at cluster-provision time.
# Paths come from cluster_lifecycle so the YAML-configured dependency
# repository (paths.lib_repository_dir) is the single source of truth.
from cluster_lifecycle import BASELINE_JARS

JARS_TO_INSTALL = list(BASELINE_JARS)

# Target-cluster runtime pins. Per-job pip deps live in <job>/job_requirements.txt.
# NOTE: default_requirements.txt (anthropic, websocket-client, ...) is the
# CONTROL-cluster file — pointing this ops script at it installed
# migration-control deps onto customer target clusters (PR#58 re-review
# finding). Pass --control-requirements only when preparing the migration
# control cluster itself.
REQUIREMENTS_PATH = "/Workspace/migration-dependencies/target_runtime_requirements.txt"
CONTROL_REQUIREMENTS_PATH = "/Workspace/migration-dependencies/default_requirements.txt"


def get_signer(profile=OCI_PROFILE, config_path=None):
    # Single auth source: oci_auth (Credential Store on-cluster → config file on
    # laptop). config_path = the CLI --config, so its credential mapping is the
    # one used on the store path (PR #234 Param review).
    from oci_auth import get_oci_signer as _shared
    _, signer = _shared(profile, config_path=config_path)
    return signer


def libraries_url(cluster_id):
    return f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/clusters/{cluster_id}/libraries"


def get_libraries(signer, cluster_id):
    """Get current library status."""
    resp = requests.get(libraries_url(cluster_id), auth=signer)
    resp.raise_for_status()
    return resp.json().get("items", [])


def install_libraries(signer, cluster_id, items):
    """Install libraries via PATCH."""
    body = {"items": items}
    resp = requests.patch(libraries_url(cluster_id), auth=signer,
                          json=body, headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Install libraries on AIDP cluster")
    parser.add_argument("--cluster", required=True,
                        help="AIDP cluster ID (required — no default)")
    parser.add_argument("--profile", default=OCI_PROFILE)
    parser.add_argument("--status", action="store_true", help="Just check status")
    parser.add_argument("--jars-only", action="store_true")
    parser.add_argument("--pip-only", action="store_true")
    parser.add_argument("--control-requirements", action="store_true",
                        help="Install the CONTROL-cluster requirements file "
                             "(anthropic, websocket-client, ...) instead of the "
                             "target runtime pins. Only for the migration "
                             "control cluster — never a customer target cluster.")
    parser.add_argument("--config", default=None,
                        help="Path to aidp_migrator.yaml (falls back to "
                             "$AIDP_CONFIG_PATH / default discovery). Resolves "
                             "paths.lib_repository_dir for the baseline JARs.")
    args = parser.parse_args()

    # Re-resolve the dependency repository from the SAME --config the migrator
    # uses, then refresh the derived JAR list.
    import cluster_lifecycle
    cluster_lifecycle.configure(args.config)
    global JARS_TO_INSTALL
    JARS_TO_INSTALL = list(cluster_lifecycle.BASELINE_JARS)

    signer = get_signer(args.profile, config_path=args.config)

    if args.status:
        print(f"Library status for cluster {args.cluster}:")
        libs = get_libraries(signer, args.cluster)
        for lib in libs:
            status = lib.get("status", "?")
            path = lib.get("path", lib.get("name", "?"))
            msg = lib.get("stateMessage", "")
            print(f"  [{status}] {os.path.basename(path)}")
            if msg:
                print(f"           {msg}")
        return

    print(f"Installing libraries on cluster {args.cluster}")
    print("=" * 60)

    # Check what's already installed
    existing = get_libraries(signer, args.cluster)
    existing_paths = {lib.get("path", "") for lib in existing}

    items = []

    # JARs
    if not args.pip_only:
        for jar in JARS_TO_INSTALL:
            if jar in existing_paths:
                print(f"  SKIP (exists): {os.path.basename(jar)}")
            else:
                items.append({
                    "operation": "INSTALL",
                    "type": "WORKSPACE_FILE",
                    "path": jar,
                })
                print(f"  INSTALL: {os.path.basename(jar)}")

    # requirements.txt
    if not args.jars_only:
        req_path = CONTROL_REQUIREMENTS_PATH if args.control_requirements else REQUIREMENTS_PATH
        if req_path not in existing_paths:
            items.append({
                "operation": "INSTALL",
                "type": "WORKSPACE_FILE",
                "path": req_path,
            })
            print(f"  INSTALL: requirements.txt")
        else:
            print(f"  SKIP (exists): requirements.txt")

    if not items:
        print("\nAll libraries already installed.")
        return

    print(f"\nInstalling {len(items)} libraries...")
    result = install_libraries(signer, args.cluster, items)

    for item in result.get("items", []):
        path = item.get("path", "?")
        status = item.get("status", "?")
        print(f"  [{status}] {os.path.basename(path)}")

    print(f"\nLibraries submitted. The cluster will install them automatically.")
    print(f"Check status with: python3 {__file__} --cluster {args.cluster} --status")


if __name__ == "__main__":
    main()
