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

import os as _os
AIDP_BASE = _os.environ.get("AIDP_BASE") or f"https://aidp.{_os.environ.get('OCI_REGION', '<OCI_REGION>')}.oci.oraclecloud.com/20240831"
DATALAKE_OCID = _os.environ.get("AIDP_DATALAKE_OCID") or "<DATALAKE_OCID>"
WORKSPACE_ID = _os.environ.get("AIDP_WORKSPACE_ID") or "<WORKSPACE_ID>"
DEFAULT_CLUSTER = _os.environ.get("AIDP_CLUSTER_ID") or "<CLUSTER_ID>"
OCI_PROFILE = "CUSTOMER"

# JARs to install (paths on the workspace)
JARS_TO_INSTALL = [
    "/Workspace/migration-dependencies/jars/hudi-spark3.5-bundle_2.12-0.15.0.jar",
    "/Workspace/migration-dependencies/jars/feature_lib_assembly.jar",
    "/Workspace/migration-dependencies/jars/featurelib2_assembly.jar",
    "/Workspace/migration-dependencies/jars/feature_lib3_assembly.jar",
    "/Workspace/migration-dependencies/jars/message_parser_assembly.jar",
    "/Workspace/migration-dependencies/jars/decryptor_assembly.jar",
    "/Workspace/migration-dependencies/jars/Spark_Decrypt_UDF_1.0.2_Final.jar",
    "/Workspace/migration-dependencies/jars/scala-logging_2.12-3.9.5.jar",
]

# requirements.txt for pip packages
REQUIREMENTS_PATH = "/Workspace/migration-dependencies/requirements.txt"


def get_signer(profile=OCI_PROFILE):
    config = oci.config.from_file(profile_name=profile)
    return oci.signer.Signer(
        tenancy=config["tenancy"], user=config["user"],
        fingerprint=config["fingerprint"],
        private_key_file_location=config["key_file"],
    )


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
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--profile", default=OCI_PROFILE)
    parser.add_argument("--status", action="store_true", help="Just check status")
    parser.add_argument("--jars-only", action="store_true")
    parser.add_argument("--pip-only", action="store_true")
    args = parser.parse_args()

    signer = get_signer(args.profile)

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
        if REQUIREMENTS_PATH not in existing_paths:
            items.append({
                "operation": "INSTALL",
                "type": "WORKSPACE_FILE",
                "path": REQUIREMENTS_PATH,
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
