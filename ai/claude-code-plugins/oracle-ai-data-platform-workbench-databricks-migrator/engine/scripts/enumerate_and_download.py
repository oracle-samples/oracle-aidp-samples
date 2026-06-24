#!/usr/bin/env python3
"""
AIDP Notebook Enumerator & Downloader
Recursively lists all objects in the AIDP workspace and downloads .ipynb notebooks.
Uses downloadFileMeta to get PAR URLs for actual file download.
"""

import json
import os
import sys
import time
import urllib.parse
import oci
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# AIDP Configuration
import os as _os
AIDP_BASE = _os.environ.get("AIDP_BASE") or f"https://aidp.{_os.environ.get('OCI_REGION', '<OCI_REGION>')}.oci.oraclecloud.com/20240831"
DATALAKE_OCID = _os.environ.get("AIDP_DATALAKE_OCID") or "<DATALAKE_OCID>"
WORKSPACE_ID = _os.environ.get("AIDP_WORKSPACE_ID") or "<WORKSPACE_ID>"
OCI_PROFILE = "CUSTOMER"
OBJECTS_URL = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/objects"
DOWNLOAD_META_URL = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/actions/downloadFileMeta"

# Output paths
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS_DIR = os.path.join(PROJECT_DIR, "notebooks")
REPORTS_DIR = os.path.join(PROJECT_DIR, "reports")
INVENTORY_FILE = os.path.join(REPORTS_DIR, "full_inventory.json")
NOTEBOOK_LIST_FILE = os.path.join(REPORTS_DIR, "notebook_list.json")

def get_oci_signer():
    """Create OCI signer for API authentication."""
    config = oci.config.from_file(profile_name=OCI_PROFILE)
    signer = oci.signer.Signer(
        tenancy=config["tenancy"],
        user=config["user"],
        fingerprint=config["fingerprint"],
        private_key_file_location=config["key_file"],
        private_key_content=config.get("key_content")
    )
    return config, signer

def list_objects(signer, path=""):
    """List objects at a given path in the AIDP workspace."""
    url = f"{OBJECTS_URL}?path={urllib.parse.quote(path, safe='')}"
    resp = requests.get(url, auth=signer)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items", [])

def enumerate_all_objects(signer, path="", depth=0):
    """Recursively enumerate all objects in the workspace."""
    prefix = "  " * depth
    items = list_objects(signer, path)
    all_items = []

    for item in items:
        item_path = item["path"]
        item_type = item["type"]

        print(f"{prefix}[{item_type}] {item_path}")
        all_items.append(item)

        if item_type == "FOLDER":
            sub_items = enumerate_all_objects(signer, item_path, depth + 1)
            all_items.extend(sub_items)

    return all_items

def get_download_url(signer, file_path, file_type="NOTEBOOK"):
    """Get PAR URL for downloading a file via downloadFileMeta."""
    headers = {
        "Content-Type": "application/json",
        "path": file_path,
        "type": file_type
    }
    resp = requests.post(DOWNLOAD_META_URL, auth=signer, headers=headers, data="")
    resp.raise_for_status()
    data = resp.json()
    return data.get("parUrl")

def download_notebook(signer, notebook_path, local_dir, file_type="NOTEBOOK"):
    """Download a single notebook from AIDP using downloadFileMeta -> PAR URL."""
    # Step 1: Get PAR URL
    par_url = get_download_url(signer, notebook_path, file_type)
    if not par_url:
        raise Exception("No parUrl returned from downloadFileMeta")

    # Step 2: Download actual content from PAR URL
    resp = requests.get(par_url)
    resp.raise_for_status()

    # Create directory structure locally
    local_path = os.path.join(local_dir, notebook_path)
    os.makedirs(os.path.dirname(local_path) if os.path.dirname(local_path) else local_dir, exist_ok=True)

    with open(local_path, 'wb') as f:
        f.write(resp.content)

    return local_path, len(resp.content)

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    os.makedirs(NOTEBOOKS_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

    print("=" * 60)
    print("AIDP Workspace Enumerator & Notebook Downloader")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    config, signer = get_oci_signer()
    print("\nOCI authentication configured successfully.\n")

    if mode in ("all", "enumerate"):
        # Phase 1: Enumerate
        print("PHASE 1: Enumerating all objects...")
        print("-" * 40)
        all_items = enumerate_all_objects(signer)

        with open(INVENTORY_FILE, 'w') as f:
            json.dump(all_items, f, indent=2, default=str)

        folders = [i for i in all_items if i["type"] == "FOLDER"]
        notebooks = [i for i in all_items if i["type"] == "NOTEBOOK" or i["path"].endswith(".ipynb")]
        files = [i for i in all_items if i["type"] == "FILE"]

        print(f"\n{'=' * 40}")
        print(f"INVENTORY SUMMARY")
        print(f"  Total items:  {len(all_items)}")
        print(f"  Folders:      {len(folders)}")
        print(f"  Notebooks:    {len(notebooks)}")
        print(f"  Other files:  {len(files)}")
        print(f"{'=' * 40}")

        notebook_list = [{"path": n["path"], "displayName": n.get("displayName", ""),
                           "createdBy": n.get("createdBy", ""), "timeCreated": n.get("timeCreated", ""),
                           "type": n.get("type", "NOTEBOOK")}
                         for n in notebooks]
        with open(NOTEBOOK_LIST_FILE, 'w') as f:
            json.dump(notebook_list, f, indent=2)

        print(f"Saved inventory to: {INVENTORY_FILE}")
        print(f"Saved notebook list ({len(notebook_list)} notebooks) to: {NOTEBOOK_LIST_FILE}")

    if mode in ("all", "download"):
        # Phase 2: Download
        if not os.path.exists(NOTEBOOK_LIST_FILE):
            print("ERROR: No notebook list found. Run enumerate first.")
            sys.exit(1)

        with open(NOTEBOOK_LIST_FILE) as f:
            notebook_list = json.load(f)

        print(f"\nPHASE 2: Downloading {len(notebook_list)} notebooks...")
        print("-" * 40)

        downloaded = []
        failed = []
        skipped = []

        for i, nb in enumerate(notebook_list):
            nb_path = nb["path"]
            local_path = os.path.join(NOTEBOOKS_DIR, nb_path)

            # Skip if already downloaded
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                skipped.append({"path": nb_path, "local_path": local_path})
                print(f"  [{i+1}/{len(notebook_list)}] SKIP (exists): {nb_path}")
                continue

            print(f"  [{i+1}/{len(notebook_list)}] Downloading: {nb_path}...", end=" ", flush=True)
            try:
                file_type = nb.get("type", "NOTEBOOK")
                local_path, size = download_notebook(signer, nb_path, NOTEBOOKS_DIR, file_type)
                downloaded.append({"path": nb_path, "local_path": local_path, "size": size})
                print(f"OK ({size:,} bytes)")
            except Exception as e:
                failed.append({"path": nb_path, "error": str(e)})
                print(f"FAILED: {e}")

            # Small delay to avoid rate limiting
            time.sleep(0.1)

        download_report = {
            "timestamp": datetime.now().isoformat(),
            "total_notebooks": len(notebook_list),
            "downloaded": len(downloaded),
            "skipped": len(skipped),
            "failed": len(failed),
            "downloaded_files": downloaded,
            "skipped_files": skipped,
            "failed_files": failed
        }

        download_report_path = os.path.join(REPORTS_DIR, "download_report.json")
        with open(download_report_path, 'w') as f:
            json.dump(download_report, f, indent=2)

        print(f"\n{'=' * 40}")
        print(f"DOWNLOAD SUMMARY")
        print(f"  Downloaded: {len(downloaded)}")
        print(f"  Skipped:    {len(skipped)}")
        print(f"  Failed:     {len(failed)}")
        print(f"{'=' * 40}")

        if failed:
            print("\nFailed downloads:")
            for f_item in failed:
                print(f"  - {f_item['path']}: {f_item['error']}")

    print(f"\nCompleted: {datetime.now().isoformat()}")

if __name__ == "__main__":
    main()
