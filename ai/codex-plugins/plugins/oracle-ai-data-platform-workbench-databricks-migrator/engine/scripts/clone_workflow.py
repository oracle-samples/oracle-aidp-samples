#!/usr/bin/env python3
"""
Clone AIDP Workflow with Migrated Notebook Paths
==================================================
Fetches an existing AIDP workflow, replaces each task's notebookPath
with the migrated path from a migration registry, and creates a new
workflow via the AIDP Job API.

Everything else (tasks, dependencies, clusters, parameters, timeouts)
is preserved exactly as the original.

Usage:
    # Dry run — see the payload without creating anything
    python3 clone_workflow.py \
        --job-key <CLUSTER_ID_ALT> \
        --registry reports/sample_registry.json \
        --dry-run

    # Create the cloned workflow
    python3 clone_workflow.py \
        --job-key <CLUSTER_ID_ALT> \
        --registry reports/sample_registry.json
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime

import requests as http_requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import get_oci_signer, REGION_MAP

# ─── Defaults (DEFAULT environment) ──────────────────────────────────────

DEFAULT_LAKE_OCID = os.environ.get("AIDP_DATALAKE", "")
DEFAULT_WORKSPACE_ID = os.environ.get("AIDP_WORKSPACE", "")
DEFAULT_OCI_PROFILE = os.environ.get("OCI_PROFILE", "DEFAULT")


def tprint(*args, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


def _get_region(lake_ocid):
    parts = lake_ocid.split(".")
    region_code = parts[3] if len(parts) > 3 else "iad"
    return REGION_MAP.get(region_code, "<OCI_REGION>")


def _api_base(lake_ocid):
    region = _get_region(lake_ocid)
    return f"https://aidp.{region}.oci.oraclecloud.com/20240831/dataLakes/{lake_ocid}"


# ─── AIDP Job API ─────────────────────────────────────────────────────

def fetch_job(workspace_id, job_key, lake_ocid, oci_profile):
    """GET /workspaces/{ws}/jobs/{key} → full job definition."""
    _, signer = get_oci_signer(oci_profile)
    url = f"{_api_base(lake_ocid)}/workspaces/{workspace_id}/jobs/{job_key}"
    resp = http_requests.get(url, auth=signer, headers={"Content-Type": "application/json"})
    if resp.status_code != 200:
        print(f"ERROR: GET /jobs/{job_key} failed: HTTP {resp.status_code}")
        print(f"  {resp.text[:500]}")
        sys.exit(1)
    return resp.json()


def create_job(workspace_id, payload, lake_ocid, oci_profile):
    """POST /workspaces/{ws}/jobs → create job shell, return response with key."""
    _, signer = get_oci_signer(oci_profile)
    url = f"{_api_base(lake_ocid)}/workspaces/{workspace_id}/jobs"
    resp = http_requests.post(url, json=payload, auth=signer,
                              headers={"Content-Type": "application/json"})
    if resp.status_code not in (200, 201, 202):
        print(f"ERROR: POST /jobs failed: HTTP {resp.status_code}")
        print(f"  {resp.text[:500]}")
        sys.exit(1)
    return resp.json()


def update_job(workspace_id, job_key, payload, lake_ocid, oci_profile):
    """PUT /workspaces/{ws}/jobs/{key} → update job with tasks."""
    _, signer = get_oci_signer(oci_profile)
    url = f"{_api_base(lake_ocid)}/workspaces/{workspace_id}/jobs/{job_key}"
    resp = http_requests.put(url, json=payload, auth=signer,
                             headers={"Content-Type": "application/json"})
    if resp.status_code not in (200, 201, 202):
        print(f"ERROR: PUT /jobs/{job_key} failed: HTTP {resp.status_code}")
        print(f"  {resp.text[:500]}")
        sys.exit(1)
    return resp.json()


# ─── Registry ─────────────────────────────────────────────────────────

def _normalize_key(path):
    """Normalize notebook path to registry key form.
    Matches normalize_nb_path() in job_migrate.py: strip /Workspace/, strip
    Users/ prefix, strip leading /, replace spaces with underscores."""
    if path.startswith("/Workspace/"):
        path = path[len("/Workspace/"):]
    if path.startswith("Users/"):
        path = path[len("Users/"):]
    if path.startswith("/"):
        path = path[1:]
    path = path.replace(" ", "_")
    return path


def load_registry(registry_path):
    """Load migration_registry.json from local file."""
    with open(registry_path) as f:
        reg = json.load(f)
    tprint(f"Registry loaded: {len(reg)} entries from {registry_path}")
    return reg


def resolve_migrated_path(original_path, registry):
    """Look up migrated path from registry, trying normalization variants.

    Registry keys may carry the original path in several shapes:
      - relative path without prefix (e.g. GIT/foo/bar.ipynb)
      - with Users/ prefix (e.g. Users/alice@x.com/foo/bar.ipynb)
      - with /Workspace/ prefix (e.g. /Workspace/Users/alice@x.com/foo/bar.ipynb)
      - relative to a user's home, with the Users/<email>/ prefix stripped
        (e.g. .bundle/Monitoring/dev/files/x.ipynb — common for Databricks
        Asset Bundles where the registry key is the bundle-relative path).
    Try each variant against the registry so any of these encodings match.
    As a final fallback, try suffix-matching every registry key against the
    workflow path — catches the long-tail of shapes we did not anticipate.
    """
    key = _normalize_key(original_path)

    # Build candidate keys from the original path, covering all common
    # registry-key shapes. Order matters: prefer fuller matches first.
    raw = original_path.replace(" ", "_")
    stripped_ws = raw[len("/Workspace/"):] if raw.startswith("/Workspace/") else raw
    stripped_ws = stripped_ws.lstrip("/")
    # Also strip the Users/<email>/ prefix — some registry keys are stored
    # relative to the user's home directory rather than the workspace root.
    stripped_user = re.sub(r"^Users/[^/]+/", "", stripped_ws)

    candidates = []
    for c in (raw, stripped_ws, stripped_user, key):
        if c and c not in candidates:
            candidates.append(c)

    # Expand each candidate with/without .ipynb suffix variants
    expanded = []
    for c in candidates:
        if c not in expanded:
            expanded.append(c)
        if c.endswith(".ipynb"):
            stem = c[:-6]
            if stem not in expanded:
                expanded.append(stem)
        else:
            withext = c + ".ipynb"
            if withext not in expanded:
                expanded.append(withext)

    for c in expanded:
        entry = registry.get(c)
        if entry and entry.get("migrated_path"):
            return entry["migrated_path"]

    # Fallback: basename only (registry may have filename keys)
    basename = os.path.basename(key)
    for c in (basename, basename + ".ipynb" if not basename.endswith(".ipynb") else basename):
        entry = registry.get(c)
        if entry and entry.get("migrated_path"):
            return entry["migrated_path"]

    # Fallback: last 2 path segments (e.g. Early_Rewards/notebook.ipynb)
    parts = key.replace("\\", "/").split("/")
    if len(parts) >= 2:
        rel2 = "/".join(parts[-2:])
        entry = registry.get(rel2)
        if entry and entry.get("migrated_path"):
            return entry["migrated_path"]

    # Last resort: suffix-match every registry key against the workflow path.
    # Catches keys whose shape we did not anticipate above. Match by full key
    # equality at a path boundary so a key for "foo.ipynb" does not accidentally
    # resolve a path like ".../barfoo.ipynb".
    norm_path = ("/" + stripped_ws).replace("//", "/")
    for reg_key, entry in registry.items():
        if not entry or not entry.get("migrated_path"):
            continue
        if not reg_key:
            continue
        rk = reg_key.lstrip("/")
        if norm_path.endswith("/" + rk) or norm_path == "/" + rk:
            return entry["migrated_path"]

    return None


# ─── Clone Logic ──────────────────────────────────────────────────────

def clone_workflow(job_data, registry):
    """Build cloned job payload with migrated notebook paths.

    Returns (payload, resolved, unresolved) where:
      payload    — the PUT body ready for the API
      resolved   — list of (task_key, original, migrated) tuples
      unresolved — list of (task_key, original) tuples that had no registry entry
    """
    original_name = job_data.get("name", "workflow")
    clone_name = f"clone_{original_name}"

    resolved = []
    unresolved = []

    # Deep-copy tasks, swap notebookPath to the migrated location.
    cloned_tasks = []
    for task in job_data.get("tasks", []):
        if task.get("type") != "NOTEBOOK_TASK":
            cloned_tasks.append(task)
            continue

        original_path = task.get("notebookPath", "")
        migrated = resolve_migrated_path(original_path, registry)
        if migrated:
            # Collapse accidental // (an OUTPUT_BASE trailing-slash artifact,
            # e.g. /Workspace/ai_dbx_migrated//ai_tool_...): AIDP parses the path
            # into nodes and rejects the empty segment as "empty node name @27"
            # (27 = the character position of the second slash).
            migrated = re.sub(r"(?<!:)/{2,}", "/", migrated)
            resolved.append((task["taskKey"], original_path, migrated))
        else:
            unresolved.append((task["taskKey"], original_path))

        cloned_task = dict(task)
        if migrated:
            cloned_task["notebookPath"] = migrated
        cloned_tasks.append(cloned_task)

    # Build payload — keep everything from original, override name and tasks
    payload = {
        "name": clone_name,
        "path": job_data.get("path", "/Workspace/jobs"),
        "description": job_data.get("description", ""),
        "maxConcurrentRuns": job_data.get("maxConcurrentRuns", 1),
        "jobClusters": job_data.get("jobClusters", []),
        "tasks": cloned_tasks,
    }
    # Preserve job-level parameters if present
    if job_data.get("parameters"):
        payload["parameters"] = job_data["parameters"]

    return payload, resolved, unresolved


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Clone an AIDP workflow with migrated notebook paths")
    parser.add_argument("--job-key", required=True,
                        help="Existing workflow job key (UUID)")
    parser.add_argument("--registry", required=True,
                        help="Local path to migration_registry.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print payload without creating the workflow")
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID,
                        help="AIDP lake OCID (default: DEFAULT)")
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID,
                        help="AIDP workspace UUID (default: DEFAULT)")
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE,
                        help="OCI config profile (default: DEFAULT)")
    args = parser.parse_args()

    # Step 1: Fetch existing workflow
    tprint(f"Fetching workflow: {args.job_key}")
    job_data = fetch_job(args.workspace_id, args.job_key,
                         args.lake_ocid, args.oci_profile)
    tprint(f"Workflow: {job_data.get('name')} — {len(job_data.get('tasks', []))} tasks")

    # Step 2: Load registry
    registry = load_registry(args.registry)

    # Step 3: Build cloned payload with migrated paths
    payload, resolved, unresolved = clone_workflow(job_data, registry)

    # Print mapping summary
    tprint(f"\nPath mappings ({len(resolved)} resolved, {len(unresolved)} unresolved):")
    for task_key, orig, mig in resolved:
        print(f"  OK  {task_key}")
        print(f"      {orig}")
        print(f"   -> {mig}")
    for task_key, orig in unresolved:
        print(f"  MISSING  {task_key}")
        print(f"           {orig}")

    if unresolved:
        print(f"\nERROR: {len(unresolved)} task(s) have no migrated path in registry. Cannot clone.")
        sys.exit(1)

    # Step 4: Dry run or create
    if args.dry_run:
        tprint("\n--dry-run: payload that would be sent:\n")
        print(json.dumps(payload, indent=2))
        return

    # Step 5: POST to create job shell
    shell = {
        "name": payload["name"],
        "path": payload["path"],
        "description": payload.get("description", ""),
        "maxConcurrentRuns": payload.get("maxConcurrentRuns", 1),
    }
    tprint(f"Creating job shell: {shell['name']}")
    created = create_job(args.workspace_id, shell, args.lake_ocid, args.oci_profile)
    new_key = created.get("key")
    tprint(f"Job shell created: key={new_key}")

    # Step 6: PUT to add tasks
    tprint(f"Adding {len(payload['tasks'])} tasks to {new_key}")
    update_job(args.workspace_id, new_key, payload, args.lake_ocid, args.oci_profile)

    tprint(f"\n{'='*60}")
    tprint(f"Cloned workflow created successfully!")
    tprint(f"{'='*60}")
    tprint(f"  Original Workflow Name: {job_data.get('name')}")
    tprint(f"  Original Workflow ID:   {args.job_key}")
    tprint(f"  New Workflow Name:      {payload['name']}")
    tprint(f"  New Workflow ID:        {new_key}")
    tprint(f"  Tasks:                  {len(payload['tasks'])}")
    tprint(f"{'='*60}")


if __name__ == "__main__":
    main()
