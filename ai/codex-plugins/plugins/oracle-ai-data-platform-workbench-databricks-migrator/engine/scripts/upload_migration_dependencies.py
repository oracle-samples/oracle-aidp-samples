#!/usr/bin/env python3
"""
Upload Migration Dependencies to AIDP Workspace
=================================================
Uploads the requirements.txt and all required JARs to the
'migration-dependencies' folder on the AIDP workspace.

This is a self-contained script that future migrators can run
whenever dependencies change.

Usage:
    python3 upload_migration_dependencies.py [--profile DEFAULT]
"""

import json
import os
import sys
import time
import argparse
import oci
import requests

# ─── Configuration ────────────────────────────────────────────────────

AIDP_BASE = "https://aidp.<OCI_REGION>.oci.oraclecloud.com/20240831"
DATALAKE_OCID = os.environ.get("AIDP_DATALAKE", "")
WORKSPACE_ID = os.environ.get("AIDP_WORKSPACE", "")
TARGET_FOLDER = "migration-dependencies"

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DEPS_DIR = os.path.join(PROJECT_DIR, "migration-dependencies")
LOCAL_JARS_DIR = os.path.join(PROJECT_DIR, "jars")

def discover_local_jars() -> dict:
    """Return local JAR files from supported dependency folders by basename."""
    jars = {}
    for folder in (LOCAL_JARS_DIR, LOCAL_DEPS_DIR):
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            if name.lower().endswith(".jar"):
                jars.setdefault(name, os.path.join(folder, name))
    return jars


def get_oci_signer(profile: str):
    config = oci.config.from_file(profile_name=profile)
    signer = oci.signer.Signer(
        tenancy=config["tenancy"],
        user=config["user"],
        fingerprint=config["fingerprint"],
        private_key_file_location=config["key_file"],
    )
    return signer


def create_folder(signer, folder_name: str):
    """Create a folder on the AIDP workspace."""
    url = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/objects/actions/createDirectory"
    headers = {
        "Content-Type": "application/json",
        "path": folder_name,
    }
    resp = requests.post(url, auth=signer, headers=headers, data="")
    if resp.status_code in (200, 201, 409):  # 409 = already exists
        print(f"  Folder '{folder_name}': OK")
        return True
    else:
        print(f"  Folder '{folder_name}': {resp.status_code} {resp.text[:200]}")
        return False


def upload_file(signer, local_path: str, remote_path: str):
    """Upload a file to the AIDP workspace."""
    # Step 1: Get upload URL
    url = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/actions/uploadFileMeta"

    file_size = os.path.getsize(local_path)
    headers = {
        "Content-Type": "application/json",
        "path": remote_path,
        "type": "FILE",
    }

    resp = requests.post(url, auth=signer, headers=headers, data="")
    if resp.status_code not in (200, 201):
        print(f"  Upload meta failed for {remote_path}: {resp.status_code} {resp.text[:200]}")
        return False

    data = resp.json()
    par_url = data.get("parUrl")

    if not par_url:
        print(f"  No parUrl for {remote_path}")
        return False

    # Step 2: Upload actual content to PAR URL
    with open(local_path, "rb") as f:
        content = f.read()

    resp = requests.put(par_url, data=content, headers={
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(content)),
    })

    if resp.status_code in (200, 201):
        print(f"  Uploaded: {remote_path} ({file_size:,} bytes)")
        return True
    else:
        print(f"  Upload failed for {remote_path}: {resp.status_code} {resp.text[:200]}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload migration dependencies to AIDP")
    parser.add_argument("--profile", default="DEFAULT", help="OCI config profile")
    parser.add_argument("--dry-run", action="store_true", help="Just list what would be uploaded")
    args = parser.parse_args()

    print("=" * 60)
    print("Upload Migration Dependencies to AIDP")
    print("=" * 60)
    print(f"Target: {TARGET_FOLDER}/")
    print(f"Workspace: {WORKSPACE_ID}")
    print(f"Profile: {args.profile}")
    print()

    # Collect files to upload
    uploads = []

    # requirements.txt
    req_path = os.path.join(LOCAL_DEPS_DIR, "requirements.txt")
    if os.path.exists(req_path):
        uploads.append((req_path, f"{TARGET_FOLDER}/requirements.txt"))
    else:
        print(f"WARNING: {req_path} not found")

    # JARs
    local_jars = discover_local_jars()
    if not local_jars:
        print(f"WARNING: no local JAR files found under {LOCAL_JARS_DIR} or {LOCAL_DEPS_DIR}")
    for jar_name, local in local_jars.items():
        uploads.append((local, f"{TARGET_FOLDER}/jars/{jar_name}"))

    # copy_jars.sh init script
    copy_jars_script = os.path.join(LOCAL_DEPS_DIR, "copy_jars.sh")
    if os.path.exists(copy_jars_script):
        uploads.append((copy_jars_script, f"{TARGET_FOLDER}/copy_jars.sh"))

    print(f"\nFiles to upload: {len(uploads)}")
    for local, remote in uploads:
        size = os.path.getsize(local)
        print(f"  {remote} ({size:,} bytes)")

    if args.dry_run:
        print("\nDry run - no files uploaded.")
        return

    # Upload
    print("\nUploading...")
    signer = get_oci_signer(args.profile)

    # Create folders
    create_folder(signer, TARGET_FOLDER)
    create_folder(signer, f"{TARGET_FOLDER}/jars")

    # Upload files
    success = 0
    failed = 0
    for local, remote in uploads:
        try:
            if upload_file(signer, local, remote):
                success += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ERROR uploading {remote}: {e}")
            failed += 1
        time.sleep(0.3)  # Rate limiting

    print(f"\n{'=' * 60}")
    print(f"Upload complete: {success} success, {failed} failed")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
