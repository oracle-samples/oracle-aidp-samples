#!/usr/bin/env python3
"""
Build DAG from AIDP Workflow/Job definition.
=============================================
Fetches a job definition from the AIDP Job API, validates all notebook
paths exist on the cluster, discovers %run dependencies for each task,
and outputs a manifest compatible with job_migrate.py.

Unlike build_dag.py (which scans a directory), this script uses the
AIDP workflow structure as the source of truth for tasks and their
inter-task dependencies (dependsOn).

Usage:
    # Build manifest from an AIDP workflow job
    python3 build_dag_from_workflow.py \\
        --job-key <job-uuid> \\
        --job-name "MyJob" \\
        --output reports/my_manifest.json \\
        --cluster <cluster-uuid>

    # Just validate notebooks exist and print task graph
    python3 build_dag_from_workflow.py \\
        --job-key <job-uuid> \\
        --validate-only
"""

import argparse
import asyncio
import json
import os
import sys
import uuid

import oci
import requests as http_requests

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _scripts_dir)

# Only evict build_dag from the kernel cache — it was the one missing 'unwrap'.
# Do NOT evict context_tools / aidp_executor / cluster_session etc.: the kernel
# loaded those at start-up with correct content; forcing a disk re-read hits the
# FUSE page-cache which may still show an older file version.
import glob as _glob
sys.modules.pop('build_dag', None)
for _pyc in _glob.glob(os.path.join(_scripts_dir, "__pycache__", "build_dag.cpython-*.pyc")):
    try:
        os.remove(_pyc)
    except OSError:
        pass

from aidp_executor import AIDPSession, get_oci_signer

# Reuse helpers from build_dag
from build_dag import (
    unwrap, run, scan_external_notebook, _normalize_ws_path,
    collect_missing_external, MAX_RECURSION_DEPTH,
)

# ─── AIDP Environment Defaults ──────────────────────────────────────

# Generic — no hardcoded tenant/AIDP-instance config. AIDP_BASE is derived
# from the lake-OCID region (see _get_region); lake/workspace/cluster are
# required (no default); OCI profile defaults to "DEFAULT".
AIDP_BASE = None
DEFAULT_LAKE_OCID = None
DEFAULT_WORKSPACE_ID = None
DEFAULT_OCI_PROFILE = "DEFAULT"
DEFAULT_CLUSTER = None


# ─── AIDP Job API ───────────────────────────────────────────────────

def fetch_job_definition(workspace_id, job_key, lake_ocid=DEFAULT_LAKE_OCID,
                         oci_profile=DEFAULT_OCI_PROFILE):
    """Fetch job definition from AIDP Job API.

    GET /workspaces/{ws}/jobs/{key}
    Returns the full job object including tasks[], jobClusters[], etc.
    """
    config, signer = get_oci_signer(oci_profile)

    # Build URL from lake OCID
    region = _get_region(lake_ocid)
    base_url = f"https://aidp.{region}.oci.oraclecloud.com/20240831"
    url = f"{base_url}/dataLakes/{lake_ocid}/workspaces/{workspace_id}/jobs/{job_key}"

    resp = http_requests.get(url, auth=signer, headers={"Content-Type": "application/json"})
    if resp.status_code != 200:
        print(f"ERROR: Failed to fetch job {job_key}: HTTP {resp.status_code}")
        print(f"  Response: {resp.text[:500]}")
        sys.exit(1)

    return resp.json()


def _get_region(lake_ocid):
    """Extract region from AIDP Lake OCID."""
    from aidp_executor import REGION_MAP
    parts = lake_ocid.split(".")
    region_code = parts[3] if len(parts) > 3 else "iad"
    return REGION_MAP.get(region_code, "<OCI_REGION>")


def extract_tasks_from_job(job_data):
    """Extract notebook tasks from job definition.

    Returns list of dicts with: task_key, notebook_path, depends_on, parameters, timeout, cluster_id
    Filters to NOTEBOOK_TASK type only.
    """
    tasks = []
    for task in job_data.get("tasks", []):
        if task.get("type") != "NOTEBOOK_TASK":
            print(f"  Skipping non-notebook task: {task.get('taskKey')} (type={task.get('type')})")
            continue

        notebook_path = task.get("notebookPath", "")
        if not notebook_path:
            print(f"  WARNING: Task {task.get('taskKey')} has no notebookPath, skipping")
            continue

        # Per-task cluster from workflow definition (may be None)
        task_cluster = task.get("cluster", {}).get("clusterKey") if isinstance(task.get("cluster"), dict) else None

        tasks.append({
            "task_key": task["taskKey"],
            "notebook_path": notebook_path,
            "cluster_id": task_cluster,
            "depends_on": [d.get("taskKey", d) if isinstance(d, dict) else d
                           for d in task.get("dependsOn", [])],
            "parameters": task.get("parameters", []),
            "timeout": task.get("timeoutSeconds", 3600),
            "max_retries": task.get("maxRetries", 0),
            "run_if": task.get("runIf", "ALL_SUCCESS"),
        })

    return tasks


# ─── Notebook Validation ────────────────────────────────────────────

async def validate_notebooks_exist(session, tasks):
    """Check that all task notebooks exist on the cluster.

    Returns (valid: list, missing: list) where missing contains
    {"task_key": str, "notebook_path": str} for each missing notebook.

    Fails fast — all notebooks must exist before proceeding.
    """
    # Build a single cluster call to check all paths at once
    paths = [t["notebook_path"] for t in tasks]
    paths_json = json.dumps(paths)

    result = await run(session, f"""
import json, os
paths = {paths_json}
status = {{}}
for p in paths:
    status[p] = os.path.exists(p)
print(json.dumps(status))
""", timeout=60)

    output = unwrap(result.get("outputs", []))
    try:
        status = json.loads(output)
    except Exception:
        print(f"ERROR: Could not validate notebook paths: {output[:300]}")
        sys.exit(1)

    valid = []
    missing = []
    for task in tasks:
        path = task["notebook_path"]
        if status.get(path, False):
            valid.append(task)
        else:
            missing.append(task)

    return valid, missing


# ─── %run Dependency Discovery ──────────────────────────────────────

def _to_export_paths(norm_path, base_prefix):
    """Map a normalized /Workspace/... notebook path to (original_path, migrated_path).

    original_path = the raw Databricks workspace path (/Workspace/<canonical>).
    migrated_path = the same notebook under the export base
                    (<base_prefix>/<canonical>), where base_prefix is
                    "<target-base>/notebooks".

    Idempotent: a path already under base_prefix maps to itself for the
    migrated form. `canonical` is the path with the /Workspace (or base) prefix
    stripped, e.g. "/Users/foo@bar/nb.ipynb".
    """
    if base_prefix and norm_path.startswith(base_prefix + "/"):
        canonical = norm_path[len(base_prefix):]            # -> /Users/...
    elif norm_path.startswith("/Workspace/"):
        canonical = norm_path[len("/Workspace"):]           # -> /Users/...
    else:
        canonical = norm_path if norm_path.startswith("/") else "/" + norm_path
    original = "/Workspace" + canonical
    migrated = (base_prefix + canonical) if base_prefix else original
    return original, migrated


async def discover_run_deps_for_tasks(session, tasks, base_prefix=""):
    """For each task notebook, discover %run / notebook.run dependencies.

    Returns {task_key: [run_dep_entry, ...]}. Each entry records both the
    `original_path` (raw Databricks reference) and the migrated `path` (under
    the export base). Resolution, scanning and existence validation all happen
    against the migrated path — the raw copy is never relied upon.
    """
    # Cache for external deps (shared across all tasks), keyed by migrated path.
    _ext_cache = {}
    all_task_paths = {t["notebook_path"] for t in tasks}  # already migrated paths

    async def _resolve_dep(ref_path, depth):
        """Recursively resolve a single notebook's deps, on the migrated path."""
        norm = _normalize_ws_path(ref_path)
        original, migrated = _to_export_paths(norm, base_prefix)
        if migrated in _ext_cache:
            return _ext_cache[migrated]

        if depth >= MAX_RECURSION_DEPTH:
            entry = {"original_path": original, "path": migrated,
                     "location": "external", "exists": None,
                     "nested_deps": [], "note": "max recursion depth reached"}
            _ext_cache[migrated] = entry
            return entry

        # Mark in-progress (cycle detection). `requested_key` is the key the
        # marker is stored under; if a name-variant resolve below reassigns
        # `migrated`, we must also overwrite this key with the real entry, or it
        # leaks a stale "circular dependency" marker that future lookups hit.
        requested_key = migrated
        _ext_cache[migrated] = {"original_path": original, "path": migrated,
                                "location": "external", "exists": None,
                                "nested_deps": [], "note": "circular dependency"}

        # Scan + validate the MIGRATED copy under the export base, not the raw ref.
        exists, child_runs = await scan_external_notebook(session, migrated)

        # Try name variants if not found: space↔underscore, hyphen→underscore.
        # original_path keeps the source form; migrated tracks the real file.
        if not exists:
            variants = set()
            alt = migrated.replace(" ", "_") if " " in migrated else migrated.replace("_", " ")
            variants.add(_normalize_ws_path(alt))
            if "-" in migrated:
                variants.add(_normalize_ws_path(migrated.replace("-", "_")))
            if " " in migrated or "-" in migrated:
                variants.add(_normalize_ws_path(migrated.replace(" ", "_").replace("-", "_")))
            variants.discard(migrated)
            for alt in variants:
                alt_exists, alt_runs = await scan_external_notebook(session, alt)
                if alt_exists:
                    print(f"    -> Resolved via variant: {migrated} -> {alt}")
                    migrated = alt
                    exists, child_runs = alt_exists, alt_runs
                    break

        nested = []
        seen_children = set()
        if exists and child_runs:
            for child_path in child_runs:
                c_norm = _normalize_ws_path(child_path)
                c_original, c_migrated = _to_export_paths(c_norm, base_prefix)
                if c_migrated in seen_children:
                    continue
                seen_children.add(c_migrated)
                if c_migrated in all_task_paths:
                    nested.append({"original_path": c_original, "path": c_migrated,
                                   "location": "task", "exists": True, "nested_deps": []})
                else:
                    nested.append(await _resolve_dep(child_path, depth + 1))

        entry = {"original_path": original, "path": migrated,
                 "location": "external", "exists": exists, "nested_deps": nested}
        _ext_cache[migrated] = entry
        # If a variant resolve reassigned `migrated`, replace the stale
        # in-progress "circular dependency" marker under the original key with
        # the resolved entry so future lookups by that key don't get a ghost.
        if requested_key != migrated:
            _ext_cache[requested_key] = entry
        return entry

    run_deps = {}
    for task in tasks:
        path = task["notebook_path"]
        print(f"  Scanning %run deps: {task['task_key']} ({os.path.basename(path)})")
        exists, direct_runs = await scan_external_notebook(session, path)

        # Deduplicate %run targets by migrated path
        seen_targets = set()
        unique_runs = []
        for target in direct_runs:
            _, t_migrated = _to_export_paths(_normalize_ws_path(target), base_prefix)
            if t_migrated not in seen_targets:
                seen_targets.add(t_migrated)
                unique_runs.append(target)

        task_deps = []
        for target in unique_runs:
            t_original, t_migrated = _to_export_paths(_normalize_ws_path(target), base_prefix)
            if t_migrated in all_task_paths:
                task_deps.append({"original_path": t_original, "path": t_migrated,
                                  "location": "task", "exists": True, "nested_deps": []})
            else:
                print(f"    External dep: {t_migrated}")
                entry = await _resolve_dep(target, 0)
                task_deps.append(entry)

        run_deps[task["task_key"]] = task_deps

    return run_deps


# ─── DAG Building ───────────────────────────────────────────────────

def build_execution_layers(tasks):
    """Build execution layers from task dependencies (dependsOn).

    Layer 0 = tasks with no dependencies, Layer N = depends on Layer N-1.
    Uses hard dependencies from the AIDP workflow definition.
    """
    task_map = {t["task_key"]: t for t in tasks}
    layers = []
    assigned = set()
    remaining = set(task_map.keys())

    while remaining:
        layer = []
        for key in remaining:
            deps = set(task_map[key]["depends_on"])
            # Only count deps that are in our task set
            unmet = deps - assigned
            unmet = unmet & remaining
            if not unmet:
                layer.append(key)

        if not layer:
            # Circular dependency — break by picking one
            pick = next(iter(remaining))
            layer = [pick]
            print(f"WARNING: Circular dependency detected, breaking with {pick}")

        layers.append(sorted(layer))
        assigned.update(layer)
        remaining -= set(layer)

    return layers


def build_manifest(job_name, tasks, layers, run_deps, job_data, base_prefix=""):
    """Build manifest compatible with job_migrate.py.

    Uses the AIDP workflow's task structure + %run deps.
    """
    task_entries = []
    for task in tasks:
        key = task["task_key"]
        path = task["notebook_path"]
        original_path, _ = _to_export_paths(_normalize_ws_path(path), base_prefix)

        task_entry = {
            "task_key": key,
            "notebook_path": path,
            "original_path": original_path,
            "depends_on": task["depends_on"],
            "base_parameters": {p.get("key", p.get("name", "")): p.get("value", "")
                                for p in (task.get("parameters") or [])
                                if isinstance(p, dict)},
            "resolved_path": path.replace("/Workspace/", "", 1) if path.startswith("/Workspace/") else path,
            "resolution_status": "workflow",
            "timeout": task.get("timeout", 3600),
            "source": "aidp_workflow",
        }
        # Per-task cluster from workflow definition
        if task.get("cluster_id"):
            task_entry["cluster_id"] = task["cluster_id"]

        if key in run_deps:
            task_entry["run_deps"] = run_deps[key]

        task_entries.append(task_entry)

    # Derive root from common prefix of all notebook paths
    all_paths = [t["resolved_path"] for t in task_entries]
    if all_paths:
        root = os.path.commonpath(all_paths) if len(all_paths) > 1 else os.path.dirname(all_paths[0])
    else:
        root = ""

    # Strip root to preserve Users/.../ structure
    strip_root = root
    users_idx = root.find("Users/")
    if users_idx > 0:
        strip_root = root[:users_idx].rstrip("/")

    # Job-level parameters from workflow definition
    job_params = {}
    for p in job_data.get("parameters", []):
        if isinstance(p, dict):
            job_params[p.get("key", p.get("name", ""))] = p.get("value", "")

    # Default cluster from jobClusters (first cluster if multiple)
    job_clusters = job_data.get("jobClusters", [])
    default_cluster = None
    if job_clusters:
        default_cluster = job_clusters[0].get("clusterKey")

    job_entry = {
        "job_name": job_name,
        "job_id": job_data.get("key", 0),
        "root": strip_root,
        "source": "aidp_workflow",
        "workflow_job_key": job_data.get("key", ""),
        "workflow_job_name": job_data.get("name", ""),
        "tasks": task_entries,
        "parameters": job_params,
        "execution_layers": layers,
    }
    if default_cluster:
        job_entry["default_cluster"] = default_cluster

    return {
        "notes": (
            "'notebook_path' and run_dep 'path' are migrated AIDP locations under "
            "the export base — this is what downstream tooling (job_migrate, data "
            "validation) should consume, and existence is validated there. "
            "'original_path' is the source Databricks path, kept for traceability. "
            "The notebooks' internal %run/notebook.run references still use the "
            "original paths and must be rewritten to the migrated paths during the "
            "actual code-migration step."
        ),
        "jobs": [job_entry],
    }


# ─── Main ───────────────────────────────────────────────────────────

async def main():
    from aidp_executor import DEFAULT_LAKE_OCID, DEFAULT_WORKSPACE_ID, DEFAULT_OCI_PROFILE

    parser = argparse.ArgumentParser(
        description="Build DAG from AIDP workflow/job definition")
    parser.add_argument("--job-key", required=True,
                        help="AIDP job key (UUID) from the workflow")
    parser.add_argument("--job-name",
                        help="Job name for manifest (default: from workflow)")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER,
                        help="AIDP cluster ID for scanning notebooks (default: %(default)s)")
    parser.add_argument("--output",
                        help="Output file for manifest (default: stdout)")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate notebooks exist and print task graph")
    parser.add_argument("--target-base", default="/Workspace/ai_dbx_exported",
                        help="AIDP export base the notebooks were migrated to. Nested "
                             "deps are re-rooted and validated under <target-base>/notebooks "
                             "(default: %(default)s)")
    # AIDP environment
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID, required=DEFAULT_LAKE_OCID is None,
                        help="AIDP data lake OCID (required)")
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID, required=DEFAULT_WORKSPACE_ID is None,
                        help="AIDP workspace UUID (required)")
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE,
                        help="OCI config profile (default: %(default)s)")
    args = parser.parse_args()

    # Derive the export base prefix that nested deps are re-rooted under.
    _tb = args.target_base.rstrip("/")
    if not _tb.startswith("/Workspace"):
        _tb = "/Workspace/" + _tb.lstrip("/")
    base_prefix = _tb + "/notebooks"

    # ── Step 1: Fetch job definition from AIDP API ──
    print(f"Fetching job definition: {args.job_key}")
    job_data = fetch_job_definition(
        args.workspace_id, args.job_key,
        lake_ocid=args.lake_ocid, oci_profile=args.oci_profile)

    job_name = args.job_name or job_data.get("name", "").replace(".job", "") or args.job_key
    print(f"Job: {job_data.get('name', '?')} ({job_name})")

    # ── Step 2: Extract notebook tasks ──
    tasks = extract_tasks_from_job(job_data)
    if not tasks:
        print("ERROR: No notebook tasks found in job definition")
        sys.exit(1)

    print(f"Found {len(tasks)} notebook task(s):")
    for t in tasks:
        deps_str = f" (depends: {', '.join(t['depends_on'])})" if t["depends_on"] else ""
        print(f"  {t['task_key']}: {os.path.basename(t['notebook_path'])}{deps_str}")

    # ── Step 3: Connect to cluster and validate all notebooks exist ──
    print(f"\nConnecting to cluster {args.cluster}...")
    session = AIDPSession(lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
                          cluster_id=args.cluster, oci_profile=args.oci_profile, session_name=job_data.get("key") or f"dag_{uuid.uuid4().hex[:8]}")
    await session.connect()

    print("Validating notebook paths...")
    valid, missing = await validate_notebooks_exist(session, tasks)

    if missing:
        print(f"\nERROR: {len(missing)} notebook(s) NOT FOUND on cluster:")
        for m in missing:
            print(f"  ! {m['task_key']}: {m['notebook_path']}")
        print("\nJob cannot proceed — all notebooks must exist before migration.")
        print("Upload the missing notebooks to the workspace and retry.")
        await session.close()
        sys.exit(1)

    print(f"All {len(valid)} notebook(s) validated ✓")

    # ── Step 4: Discover %run dependencies ──
    print("\nScanning %run dependencies...")
    run_deps = await discover_run_deps_for_tasks(session, tasks, base_prefix)

    await session.close()

    # Check for missing external deps
    missing_ext = collect_missing_external(run_deps)
    if missing_ext:
        print(f"\nERROR: {len(missing_ext)} external dependency notebook(s) NOT FOUND:")
        for m in missing_ext:
            print(f"  ! {m['path']}")
            for ref in m["referenced_by"]:
                print(f"    referenced by: {ref}")
        print("\nJob cannot proceed — all %run dependencies must exist.")
        print("Upload the missing notebooks to the workspace and retry.")
        sys.exit(1)

    # ── Step 5: Build execution layers ──
    layers = build_execution_layers(tasks)

    print(f"\nExecution Layers ({len(layers)} layers):")
    for i, layer in enumerate(layers):
        print(f"  Layer {i}: {layer}")

    if args.validate_only:
        # Print task graph with %run deps
        print(f"\n{'='*60}")
        print(f"Task Graph: {job_name}")
        print(f"{'='*60}")
        for task in tasks:
            key = task["task_key"]
            deps_str = f" → depends: [{', '.join(task['depends_on'])}]" if task["depends_on"] else ""
            print(f"  {key}{deps_str}")
            for rd in run_deps.get(key, []):
                loc = rd.get("location", "?")
                exists = "✓" if rd.get("exists") else "✗"
                print(f"    %run [{loc}] {rd['path']} {exists}")
        return

    # ── Step 6: Build and write manifest ──
    manifest = build_manifest(job_name, tasks, layers, run_deps, job_data, base_prefix)

    total_tasks = len(manifest["jobs"][0]["tasks"])
    print(f"\nJob: {job_name}")
    print(f"Tasks: {total_tasks}")
    print(f"Layers: {len(layers)}")

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"Manifest written to {args.output}")
    else:
        print(f"\n{json.dumps(manifest, indent=2)}")


if __name__ == "__main__":
    asyncio.run(main())
