#!/usr/bin/env python3
"""
Job Pre-flight: Discovery & Resolution
========================================
Parses the Databricks job definitions, resolves all notebook paths,
discovers child notebooks, inspects init scripts, and produces a manifest.

Usage:
    python3 job_preflight.py --csv <path_to>/metadata_jobs.csv
"""

import csv
import json
import os
import sys
import re
import tempfile
import shutil
import argparse
import urllib.parse
from collections import defaultdict
from datetime import datetime, date
from typing import List, Dict, Optional, Set, Tuple

import oci
import requests as http_requests

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# AIDP config
AIDP_BASE = "https://aidp.<OCI_REGION>.oci.oraclecloud.com/20240831"
DATALAKE_OCID = "<DATALAKE_OCID>"
WORKSPACE_ID = "<WORKSPACE_ID>"
DOWNLOAD_META_URL = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/actions/downloadFileMeta"
OCI_PROFILE = "DEFAULT"


def get_signer():
    config = oci.config.from_file(profile_name=OCI_PROFILE)
    return oci.signer.Signer(
        tenancy=config["tenancy"], user=config["user"],
        fingerprint=config["fingerprint"],
        private_key_file_location=config["key_file"],
    )


def try_download(signer, path: str, file_type: str = "NOTEBOOK") -> Optional[bytes]:
    """Try to download a file from AIDP. Returns content or None."""
    try:
        headers = {"Content-Type": "application/json", "path": path, "type": file_type}
        resp = http_requests.post(DOWNLOAD_META_URL, auth=signer, headers=headers, data="")
        if resp.status_code != 200:
            return None
        par_url = resp.json().get("parUrl")
        if not par_url:
            return None
        resp = http_requests.get(par_url)
        if resp.status_code == 200:
            return resp.content
        return None
    except Exception:
        return None


# ─── CSV Parsing ──────────────────────────────────────────────────────

def parse_jobs_csv(csv_path: str) -> List[dict]:
    """Parse the metadata CSV and extract job definitions."""
    jobs = []
    with open(csv_path, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("name", "").strip()
            if not name:
                continue
            try:
                job_json = json.loads(row.get("json", "{}"))
            except json.JSONDecodeError:
                continue

            settings = job_json.get("settings", {})
            tasks = settings.get("tasks", [])
            parameters = settings.get("parameters", [])
            job_clusters = settings.get("job_clusters", [])

            # Extract init scripts from all job clusters
            init_scripts = set()
            for jc in job_clusters:
                nc = jc.get("new_cluster", {})
                for script in nc.get("init_scripts", []):
                    ws = script.get("workspace", {})
                    dest = ws.get("destination", "")
                    if dest:
                        init_scripts.add(dest)

            # Extract notebook paths and task DAG
            job_tasks = []
            for task in tasks:
                task_key = task.get("task_key", "")
                nb_task = task.get("notebook_task", {})
                nb_path = nb_task.get("notebook_path", "")
                base_params = nb_task.get("base_parameters", {})
                deps = [d["task_key"] for d in task.get("depends_on", [])]
                if nb_path:
                    job_tasks.append({
                        "task_key": task_key,
                        "notebook_path": nb_path,
                        "depends_on": deps,
                        "base_parameters": base_params,
                    })

            # Compute parameter defaults
            param_defaults = {}
            for p in parameters:
                pname = p.get("name", "")
                pdefault = p.get("default", "")
                # Substitute Databricks template vars with real values
                if "{{job.start_time.iso_date}}" in pdefault:
                    pdefault = date.today().isoformat()
                param_defaults[pname] = pdefault

            jobs.append({
                "job_name": name,
                "job_id": job_json.get("job_id", ""),
                "tasks": job_tasks,
                "parameters": param_defaults,
                "init_scripts": sorted(init_scripts),
                "status": row.get("status", ""),
                "can_migrated": row.get("CanMigrated", ""),
            })

    return jobs


def topological_sort(tasks: List[dict]) -> List[List[str]]:
    """Topological sort tasks into layers for parallel execution."""
    # Build adjacency and in-degree
    in_degree = {t["task_key"]: 0 for t in tasks}
    dependents = defaultdict(list)
    for t in tasks:
        for dep in t["depends_on"]:
            if dep in in_degree:
                dependents[dep].append(t["task_key"])
                in_degree[t["task_key"]] += 1

    layers = []
    remaining = dict(in_degree)

    while remaining:
        # Find all tasks with 0 in-degree
        layer = [k for k, v in remaining.items() if v == 0]
        if not layer:
            # Cycle detected
            break
        layers.append(sorted(layer))
        for k in layer:
            del remaining[k]
            for dep in dependents.get(k, []):
                if dep in remaining:
                    remaining[dep] -= 1

    return layers


# ─── Notebook Resolution ─────────────────────────────────────────────

def load_workspace_inventory() -> Set[str]:
    """Load known workspace paths from the notebook list."""
    inv_path = os.path.join(PROJECT_DIR, "reports", "notebook_list.json")
    if not os.path.exists(inv_path):
        return set()
    with open(inv_path) as f:
        items = json.load(f)
    # Paths in inventory are relative (no /Workspace/ prefix)
    return {item["path"] for item in items}


def normalize_path(path: str) -> str:
    """Normalize a Databricks notebook path to AIDP workspace path."""
    # Strip /Workspace/ prefix for inventory matching
    if path.startswith("/Workspace/"):
        return path[len("/Workspace/"):]
    if path.startswith("/"):
        return path[1:]
    return path


def resolve_notebook_path(signer, path: str, inventory: Set[str]) -> Tuple[str, str]:
    """Try to resolve a notebook path. Returns (resolved_path, status)."""
    normalized = normalize_path(path)

    # Try 1: Exact match in inventory
    if normalized in inventory:
        return normalized, "found_in_inventory"

    # Try 2: With .ipynb extension
    if not normalized.endswith(".ipynb"):
        with_ext = normalized + ".ipynb"
        if with_ext in inventory:
            return with_ext, "found_with_extension"

    # Try 3: Without .ipynb extension
    if normalized.endswith(".ipynb"):
        without_ext = normalized[:-6]
        if without_ext in inventory:
            return without_ext, "found_without_extension"

    # Try 4: URL-decode special chars
    decoded = urllib.parse.unquote(normalized)
    if decoded != normalized and decoded in inventory:
        return decoded, "found_url_decoded"

    # Try 5: Path normalization - spaces to underscores, remove special chars
    # Databricks paths often have spaces/parens that get normalized on AIDP
    alt_path = normalized.replace(" ", "_").replace("(", "").replace(")", "")
    for candidate in [alt_path, alt_path + ".ipynb"]:
        if candidate in inventory:
            return candidate, "found_normalized"

    # Try 6: Check in migration_staging paths
    staging_prefixes = [
        "testing/param/automation/<internal_staging>/",
        "testing/param/automation/dbxToaidp/migration_staging/",
        "<validation_dir>/",
    ]
    for prefix in staging_prefixes:
        for candidate in [prefix + normalized, prefix + normalized + ".ipynb",
                          prefix + alt_path, prefix + alt_path + ".ipynb"]:
            if candidate in inventory:
                return candidate, f"found_in_{prefix.rstrip('/').split('/')[-1]}"

    # Try 7: Basename search with user directory matching
    basename = os.path.basename(normalized)
    basename_ipynb = basename + ".ipynb" if not basename.endswith(".ipynb") else basename
    matches = [p for p in inventory
               if os.path.basename(p) == basename or os.path.basename(p) == basename_ipynb]

    if matches:
        # Prefer match in same user directory
        user_parts = normalized.split("/")
        if len(user_parts) >= 2:
            user_dir = "/".join(user_parts[:2])
            user_matches = [m for m in matches if user_dir in m]
            if user_matches:
                return user_matches[0], "found_by_basename_user_match"
        # Otherwise take the first non-testing/non-backup match
        for m in matches:
            if not m.startswith("bkp/") and not m.startswith("testing/"):
                return m, "found_by_basename"
        return matches[0], "found_by_basename_any"

    # Try 8: Direct download attempt (confirms existence even if not in inventory)
    for attempt_path in [normalized, normalized + ".ipynb"]:
        content = try_download(signer, attempt_path)
        if content:
            return attempt_path, "found_by_download"

    return normalized, "NOT_FOUND"


# ─── Child Notebook Discovery ────────────────────────────────────────

def scan_for_children(notebook_content: str, notebook_path: str) -> List[str]:
    """Scan notebook source for %run and dbutils.notebook.run references."""
    try:
        nb = json.loads(notebook_content)
    except json.JSONDecodeError:
        return []

    children = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))

        # %run magic
        run_matches = re.findall(r'%run\s+(/[^\s$]+)', source)
        for match in run_matches:
            # Remove any $widget references
            clean = re.sub(r'\$\w+', '', match).strip()
            children.append(clean)

        # dbutils.notebook.run
        nb_run_matches = re.findall(
            r'dbutils\.notebook\.run\s*\(\s*["\']([^"\']+)["\']', source
        )
        children.extend(nb_run_matches)

        # oidlUtils.notebook.run
        oidl_matches = re.findall(
            r'oidlUtils\.notebook\.run\s*\(\s*["\']([^"\']+)["\']', source
        )
        children.extend(oidl_matches)

    return children


def discover_children(signer, root_paths: List[str], inventory: Set[str]) -> Dict[str, List[str]]:
    """Recursively discover all child notebooks."""
    graph = {}
    visited = set()
    queue = list(root_paths)

    while queue:
        path = queue.pop(0)
        if path in visited:
            continue
        visited.add(path)

        # Download notebook
        content = try_download(signer, path)
        if content is None:
            # Try with .ipynb
            content = try_download(signer, path + ".ipynb")
        if content is None:
            graph[path] = []
            continue

        children = scan_for_children(content.decode('utf-8', errors='replace'), path)

        # Resolve child paths
        resolved_children = []
        for child in children:
            child_normalized = normalize_path(child)
            resolved, status = resolve_notebook_path(signer, child, inventory)
            resolved_children.append(resolved)
            if resolved not in visited:
                queue.append(resolved)

        graph[path] = resolved_children

    return graph


# ─── Init Script Inspection ──────────────────────────────────────────

def inspect_init_scripts(signer, script_paths: List[str]) -> List[dict]:
    """Download and parse init scripts."""
    results = []
    for path in script_paths:
        normalized = normalize_path(path)
        content = try_download(signer, normalized, file_type="FILE")
        if content is None:
            results.append({"path": path, "status": "NOT_FOUND", "actions": []})
            continue

        script_text = content.decode('utf-8', errors='replace')
        actions = []

        # Parse pip installs
        for match in re.findall(r'pip\s+install\s+(.+)', script_text):
            actions.append({"type": "pip_install", "packages": match.strip()})

        # Parse JAR copies
        for match in re.findall(r'cp\s+.*\.jar\s+(\S+)', script_text):
            actions.append({"type": "jar_copy", "destination": match})

        # Parse env vars
        for match in re.findall(r'export\s+(\w+)=(.+)', script_text):
            actions.append({"type": "env_var", "key": match[0], "value": match[1].strip()})

        # Parse spark-defaults
        for match in re.findall(r'spark\.(\S+)\s+(\S+)', script_text):
            actions.append({"type": "spark_config", "key": f"spark.{match[0]}", "value": match[1]})

        results.append({
            "path": path,
            "status": "OK",
            "content_preview": script_text[:500],
            "actions": actions,
        })

    return results


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Job pre-flight discovery")
    parser.add_argument("--csv", required=True, help="Path to metadata_jobs.csv")
    args = parser.parse_args()

    print("=" * 60)
    print("Job Pre-flight: Discovery & Resolution")
    print("=" * 60)
    print(f"CSV: {args.csv}")
    print(f"Date: {datetime.now().isoformat()}")

    sig = get_signer()
    inventory = load_workspace_inventory()
    print(f"Workspace inventory: {len(inventory)} items")

    # Step 1: Parse CSV
    print("\n--- Step 1: Parse job definitions ---")
    jobs = parse_jobs_csv(args.csv)
    all_notebook_paths = set()
    all_init_scripts = set()
    for job in jobs:
        for task in job["tasks"]:
            all_notebook_paths.add(task["notebook_path"])
        all_init_scripts.update(job["init_scripts"])

    print(f"Jobs: {len(jobs)}")
    print(f"Unique notebook paths: {len(all_notebook_paths)}")
    print(f"Init scripts: {len(all_init_scripts)}")

    # Step 2: Resolve notebook paths
    print("\n--- Step 2: Resolve notebook paths ---")
    resolution_results = {}
    found = 0
    missing = 0
    for path in sorted(all_notebook_paths):
        resolved, status = resolve_notebook_path(sig, path, inventory)
        resolution_results[path] = {"resolved": resolved, "status": status}
        if "NOT_FOUND" in status:
            missing += 1
            print(f"  MISSING: {path}")
        else:
            found += 1
            print(f"  OK [{status}]: {path}")

    print(f"\nResolved: {found}, Missing: {missing}")

    # Update task paths with resolved versions
    for job in jobs:
        for task in job["tasks"]:
            res = resolution_results.get(task["notebook_path"], {})
            task["resolved_path"] = res.get("resolved", normalize_path(task["notebook_path"]))
            task["resolution_status"] = res.get("status", "UNKNOWN")

    # Step 3: Build DAG ordering per job
    print("\n--- Step 3: Build DAG ordering ---")
    for job in jobs:
        layers = topological_sort(job["tasks"])
        job["execution_layers"] = layers
        print(f"  {job['job_name']}: {len(layers)} layers, {len(job['tasks'])} tasks")

    # Step 4: Discover child notebooks
    print("\n--- Step 4: Discover child notebooks ---")
    resolved_paths = [r["resolved"] for r in resolution_results.values() if r["status"] != "NOT_FOUND"]
    child_graph = discover_children(sig, resolved_paths, inventory)

    all_children = set()
    for parent, children in child_graph.items():
        for child in children:
            if child not in all_notebook_paths:
                all_children.add(child)

    print(f"Discovered {len(all_children)} child notebooks not in the 71 job notebooks")
    for child in sorted(all_children):
        print(f"  CHILD: {child}")

    # Step 5: Inspect init scripts
    print("\n--- Step 5: Inspect init scripts ---")
    init_results = inspect_init_scripts(sig, sorted(all_init_scripts))
    for ir in init_results:
        print(f"  {ir['path']}: {ir['status']} ({len(ir['actions'])} actions)")
        for action in ir["actions"][:5]:
            print(f"    {action['type']}: {action.get('packages', action.get('key', action.get('destination', '')))}")

    # Step 6: Save manifest
    manifest = {
        "timestamp": datetime.now().isoformat(),
        "csv_source": args.csv,
        "total_jobs": len(jobs),
        "total_notebooks": len(all_notebook_paths),
        "total_children_discovered": len(all_children),
        "notebooks_resolved": found,
        "notebooks_missing": missing,
        "jobs": jobs,
        "resolution_results": resolution_results,
        "child_notebook_graph": child_graph,
        "child_notebooks": sorted(all_children),
        "init_script_analysis": init_results,
        "output_base_path": "/Workspace/<output-base>/job-migration",
    }

    manifest_path = os.path.join(PROJECT_DIR, "reports", "job_manifest.json")
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"Manifest saved: {manifest_path}")
    print(f"Jobs: {len(jobs)}")
    print(f"Notebooks: {found} resolved, {missing} missing")
    print(f"Children: {len(all_children)} discovered")
    print(f"Init scripts: {len(init_results)} analyzed")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
