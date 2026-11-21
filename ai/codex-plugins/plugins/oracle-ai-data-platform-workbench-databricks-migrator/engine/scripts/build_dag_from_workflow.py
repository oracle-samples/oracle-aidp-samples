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
import posixpath
import re
import sys
import uuid

import oci
import requests as http_requests

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _scripts_dir)
# Also add project root so we can import dbx_export.sanitizer (single source
# of truth for path sanitization — must match what dbx_export applied at
# upload time, else lookups fail with false-positive "not found").
sys.path.insert(0, os.path.dirname(_scripts_dir))
from dbx_export.sanitizer import rewrite_embedded_workspace_ref as _rewrite_embedded_workspace_ref

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
from notebook_path_parameters import effective_task_parameters, update_parameters

# Reuse helpers from build_dag
from build_dag import (
    unwrap, run, scan_external_notebook, _normalize_ws_path,
    collect_missing_external, MAX_RECURSION_DEPTH,
    _to_export_paths, _notebook_path_candidates,
)

# ─── AIDP Environment Defaults ──────────────────────────────────────

# Generic — no hardcoded customer/AIDP-instance config. AIDP_BASE is derived
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
    region_code = parts[3] if len(parts) > 3 else "bom"
    return REGION_MAP.get(region_code, "ap-mumbai-1")


def fetch_run_parameters(workspace_id, run_key, lake_ocid=DEFAULT_LAKE_OCID,
                         oci_profile=DEFAULT_OCI_PROFILE):
    """Fetch job-run parameters from AIDP.

    GET /workspaces/{ws}/jobRuns/{run_key}
    Returns dict: {parameter_name: value_str}. Empty dict on any error
    (fail-soft — allows DAG build to continue with placeholder params).

    Fix 6 (2026-07-25) — used by --params-from-run: extract the exact
    parameter values a prior successful production run received at trigger
    time. This closes the gap identified during a pilot-customer AutoRefit migration
    (2026-07-25) where the workflow spec had no static task parameters
    but the notebooks read 11 widgets that only an external orchestrator
    populated at trigger time. Without those values, migration wastes
    hours retrying widgets that can never be resolved.

    The parameters returned come from the run's top-level `parameters`
    field — same one populated by the trigger caller (Airflow /
    external notebook / API call). Task-level parameters (usually empty
    for this pattern) are NOT merged in; if you need them, they're
    already captured by extract_tasks_from_job() into task.parameters.
    """
    if not run_key:
        return {}
    config, signer = get_oci_signer(oci_profile)
    region = _get_region(lake_ocid)
    base_url = f"https://aidp.{region}.oci.oraclecloud.com/20240831"
    # NOTE: `jobRuns` endpoint (verified 2026-07-25) — the alternative
    # /jobs/{key}/runs/{run_key} returns 404 for cross-workflow runs
    # (a run may belong to a workflow different from the one you're
    # currently migrating; jobRuns is workflow-agnostic).
    url = f"{base_url}/dataLakes/{lake_ocid}/workspaces/{workspace_id}/jobRuns/{run_key}"
    try:
        resp = http_requests.get(
            url, auth=signer, headers={"Content-Type": "application/json"},
            timeout=60,
        )
    except Exception as _fetch_err:
        print(f"WARNING: fetch_run_parameters({run_key}) network error: "
              f"{type(_fetch_err).__name__}: {_fetch_err}")
        return {}
    if resp.status_code != 200:
        print(f"WARNING: fetch_run_parameters({run_key}) HTTP {resp.status_code}: "
              f"{resp.text[:200]}")
        return {}
    d = resp.json()
    params = {}
    for p in d.get("parameters", []) or []:
        if not isinstance(p, dict):
            continue
        name = p.get("name") or p.get("key")
        value = p.get("value")
        if name is not None:
            params[str(name)] = "" if value is None else str(value)
    return params


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

def _infer_source_prefix(tasks):
    """Infer the source (dbx_export) base prefix from the common prefix of
    task notebook paths.

    Returns the longest prefix ending in "/notebooks" that all tasks share,
    or None if no such common prefix exists.

    Used to correctly strip the source base from resolved %run targets that
    live under the source location (before the tool prepends the target
    base). Without this, %run deps at /Workspace/<source-base>/notebooks/x
    get double-prefixed when the source ≠ target — see the ExampleVendor failure
    (2026-07-28): the doubled path was
      /Workspace/<TARGET>/notebooks/<SOURCE-base>/notebooks/GIT/.../imports.ipynb
    """
    paths = [t.get("notebook_path", "") for t in (tasks or []) if t.get("notebook_path")]
    if not paths:
        return None
    # Use posixpath (not os.path) because AIDP workspace paths are always POSIX
    # (`/Workspace/...`), independent of the host running this tool. the reviewer's
    # PR-35 review (2026-07-28) caught the identical bug class that
    # commit 239f6c9 fixed in build_dag._normalize_ws_path — on Windows,
    # os.path.commonpath uses '\\' and returns garbage for POSIX inputs,
    # making source_prefix silently None and re-introducing the very path-
    # doubling this feature exists to prevent.
    common = posixpath.commonpath(paths) if len(paths) > 1 else posixpath.dirname(paths[0])
    if not common.startswith("/Workspace/"):
        return None
    # Only recognize the common prefix if it contains a "/notebooks" segment —
    # dbx_export always uploads under <export-base>/notebooks/. If tasks live
    # directly under /Workspace/foo/ (no notebooks/), we don't have a source
    # base to strip and should fall through to /Workspace/ handling.
    m = re.match(r'^(/Workspace/.*?/notebooks)(?:/|$)', common)
    if m:
        return m.group(1)
    return None


async def discover_run_deps_for_tasks(
    session, tasks, base_prefix="", source_prefix=None,
    parameter_overrides=None,
):
    """For each task notebook, discover %run / notebook.run dependencies.

    Returns {task_key: [run_dep_entry, ...]}. Each entry records both the
    `original_path` (raw Databricks reference) and the migrated `path` (under
    the export base). Resolution, scanning and existence validation all happen
    against the migrated path — the raw copy is never relied upon.

    `source_prefix` (optional) is the dbx_export base where the tasks CURRENTLY
    live on the cluster. When TARGET (base_prefix) ≠ SOURCE (source_prefix),
    resolved %run paths that fall under source_prefix are stripped correctly
    to avoid double-prefixing. See `_to_export_paths` for the fallback order.
    """
    # Cache external deps by migrated path plus effective parameter bindings.
    _ext_cache = {}
    parameter_overrides = dict(parameter_overrides or {})
    # Task notebooks may still live under source_prefix when target and source
    # export bases differ. Identify a task by its target identity, then retain
    # its actual source path in the run_deps edge and manifest.
    task_path_by_target = {}
    for task in tasks:
        actual = task["notebook_path"]
        _, target = _to_export_paths(
            _normalize_ws_path(actual), base_prefix, source_prefix,
        )
        previous = task_path_by_target.get(target)
        if previous and previous != actual:
            raise ValueError(
                f"Conflicting task notebook paths for {target}: "
                f"{previous!r} and {actual!r}"
            )
        task_path_by_target[target] = actual
    # Namespace lookups need the same export layout as the surrounding
    # preflight. Omit defaults for existing no-export library callers.
    scan_context = {}
    if base_prefix or source_prefix:
        scan_context = {"base_prefix": base_prefix, "source_prefix": source_prefix}

    def _loose_migrated_targets(runs):
        def migrated(target):
            normalized = _normalize_ws_path(_rewrite_embedded_workspace_ref(target))
            return _to_export_paths(normalized, base_prefix, source_prefix)[1]

        inferred = getattr(runs, "loose_only", set())
        direct = {migrated(target) for target in runs if target not in inferred}
        return {migrated(target) for target in inferred} - direct

    def _skip_missing_loose(parent, target, entry, loose_targets):
        if entry["exists"] is False and entry["path"] in loose_targets:
            print(
                f"  [WARN][loose-candidate-skip] {entry['path']}: "
                f"inferred from {parent} as {target!r}; "
                "verified absent on AIDP; not a confirmed notebook dependency"
            )
            return True
        return False

    def _effective_parameters(task):
        # Match job_migrate.py runtime precedence: task base parameters first,
        # then explicit job/PARAM_OVERRIDES values win on conflict.
        effective = {
            p.get("key", p.get("name", "")): p.get("value", "")
            for p in (task.get("parameters") or []) if isinstance(p, dict)
        }
        effective = effective_task_parameters(effective, parameter_overrides)
        return {str(k): v for k, v in effective.items() if k}

    async def _resolve_dep(ref_path, depth, parameter_bindings):
        """Recursively resolve a single notebook's deps, on the migrated path."""
        norm = _normalize_ws_path(ref_path)
        original, migrated = _to_export_paths(norm, base_prefix, source_prefix)
        binding_key = json.dumps(parameter_bindings, sort_keys=True, default=str)
        cache_key = (migrated, binding_key)
        if cache_key in _ext_cache:
            return _ext_cache[cache_key]

        if depth >= MAX_RECURSION_DEPTH:
            entry = {"original_path": original, "path": migrated,
                     "location": "external", "exists": None,
                     "nested_deps": [], "note": "max recursion depth reached"}
            _ext_cache[cache_key] = entry
            return entry

        # Mark in-progress (cycle detection). `requested_key` is the key the
        # marker is stored under; if a name-variant resolve below reassigns
        # `migrated`, we must also overwrite this key with the real entry, or it
        # leaks a stale "circular dependency" marker that future lookups hit.
        requested_key = cache_key
        _ext_cache[cache_key] = {"original_path": original, "path": migrated,
                                 "location": "external", "exists": None,
                                 "nested_deps": [], "note": "circular dependency"}

        # Scan + validate the MIGRATED copy under the export base, not the raw ref.
        exists, child_runs = await scan_external_notebook(
            session, migrated, parameter_bindings=parameter_bindings, **scan_context,
        )

        # Try name variants if not found: space↔underscore, hyphen→underscore.
        # original_path keeps the source form; migrated tracks the real file.
        if not exists:
            for alt in _notebook_path_candidates(ref_path, base_prefix, source_prefix)[1:]:
                alt_exists, alt_runs = await scan_external_notebook(
                    session, alt, parameter_bindings=parameter_bindings, **scan_context,
                )
                if alt_exists:
                    print(f"    -> Resolved via variant: {migrated} -> {alt}")
                    migrated = alt
                    exists, child_runs = alt_exists, alt_runs
                    break

        nested = []
        seen_children = set()
        if exists and child_runs:
            loose_children = _loose_migrated_targets(child_runs)
            for child_path in child_runs:
                child_path = _rewrite_embedded_workspace_ref(child_path)
                c_norm = _normalize_ws_path(child_path)
                c_original, c_migrated = _to_export_paths(c_norm, base_prefix, source_prefix)
                if c_migrated in seen_children:
                    continue
                seen_children.add(c_migrated)
                if c_migrated in task_path_by_target:
                    nested.append({"original_path": c_original,
                                   "path": task_path_by_target[c_migrated],
                                   "location": "task", "exists": True, "nested_deps": []})
                else:
                    child_entry = await _resolve_dep(
                        child_path, depth + 1, parameter_bindings,
                    )
                    if not _skip_missing_loose(migrated, child_path,
                                               child_entry, loose_children):
                        nested.append(child_entry)

        entry = {"original_path": original, "path": migrated,
                 "location": "external", "exists": exists, "nested_deps": nested}
        resolved_cache_key = (migrated, binding_key)
        _ext_cache[resolved_cache_key] = entry
        # If a variant resolve reassigned `migrated`, replace the stale
        # in-progress "circular dependency" marker under the original key with
        # the resolved entry so future lookups by that key don't get a ghost.
        if requested_key != resolved_cache_key:
            _ext_cache[requested_key] = entry
        return entry

    run_deps = {}
    for task in tasks:
        path = task["notebook_path"]
        task_parameters = _effective_parameters(task)
        print(f"  Scanning %run deps: {task['task_key']} ({os.path.basename(path)})")
        exists, direct_runs = await scan_external_notebook(
            session, path, parameter_bindings=task_parameters, **scan_context,
        )
        loose_direct = _loose_migrated_targets(direct_runs)

        # Deduplicate %run targets by migrated path
        seen_targets = set()
        unique_runs = []
        for target in direct_runs:
            target = _rewrite_embedded_workspace_ref(target)
            _, t_migrated = _to_export_paths(_normalize_ws_path(target), base_prefix, source_prefix)
            if t_migrated not in seen_targets:
                seen_targets.add(t_migrated)
                unique_runs.append(target)

        task_deps = []
        for target in unique_runs:
            target = _rewrite_embedded_workspace_ref(target)
            t_original, t_migrated = _to_export_paths(_normalize_ws_path(target), base_prefix, source_prefix)
            if t_migrated in task_path_by_target:
                task_deps.append({"original_path": t_original,
                                  "path": task_path_by_target[t_migrated],
                                  "location": "task", "exists": True, "nested_deps": []})
            else:
                print(f"    External dep: {t_migrated}")
                entry = await _resolve_dep(target, 0, task_parameters)
                if not _skip_missing_loose(path, target, entry, loose_direct):
                    task_deps.append(entry)

        run_deps[task["task_key"]] = task_deps

    # Several regression harnesses AST-extract this function into an isolated
    # namespace; the augmentation helper is absent there and the pre-existing
    # contract is what those tests pin. At runtime the module global exists.
    _augment = globals().get("_augment_dynamic_dispatch_deps")
    if _augment is not None:
        await _augment(
            session, tasks, run_deps, base_prefix, source_prefix,
            _resolve_dep, _effective_parameters,
        )
    return run_deps


async def _read_notebook_json(session, path):
    """Load one notebook's JSON from the mounted workspace; None if unreadable.

    Best-effort by design: the authoritative dep scan already ran; this read
    only feeds the dispatch-target augmentation. An unreadable notebook is
    WARNED and skipped (the extractor's own "may be missed" warning already
    covers the residual), never a DAG-build failure.
    """
    try:
        result = await run(session, f"""
import json, os
path = {json.dumps(_normalize_ws_path(path))}
if not os.path.exists(path):
    print(json.dumps({{"error": "missing", "nb": None}}))
else:
    try:
        with open(path) as fh:
            print(json.dumps({{"error": None, "nb": json.load(fh)}}))
    except Exception as exc:
        print(json.dumps({{"error": type(exc).__name__ + ": " + str(exc)[:200], "nb": None}}))
""")
    except Exception as exc:
        print(f"  [WARN][dispatch-targets] cannot read {path} for augmentation: "
              f"{type(exc).__name__}: {exc}")
        return None
    try:
        payload = json.loads(unwrap(result.get("outputs", [])))
        if not isinstance(payload, dict):
            raise ValueError("unexpected scan payload")
    except (ValueError, TypeError) as exc:
        # A failed probe must be distinguishable from an absent notebook: a
        # silent None here could hide every hint and skip the augmentation
        # (review finding 2026-10-14).
        print(f"  [WARN][dispatch-targets] unverified scan of {path}: "
              f"{type(exc).__name__}: {exc}")
        return None
    if payload.get("error"):
        print(f"  [WARN][dispatch-targets] cannot read {path}: {payload['error']}")
        return None
    return payload.get("nb")


async def _find_dispatch_targets(session, anchor_dir, hint_dirs):
    """Nearest existing <ancestor>/<hint> dir to anchor_dir, walked for .ipynb."""
    result = await run(session, f"""
import json, os
anchor = {json.dumps(anchor_dir)}
hints = {json.dumps(sorted(hint_dirs))}
found, truncated = [], False
for hint in hints:
    probe = anchor
    for _ in range(6):
        candidate = os.path.join(probe, hint)
        if os.path.isdir(candidate):
            for dirpath, dirnames, filenames in os.walk(candidate):
                dirnames[:] = [d for d in dirnames if not d.startswith('.')]
                for name in sorted(filenames):
                    if name.endswith('.ipynb'):
                        found.append(os.path.join(dirpath, name))
                if len(found) > 500:
                    truncated = True
                    dirnames[:] = []
            break
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
print(json.dumps({{"found": found, "truncated": truncated}}))
""")
    try:
        payload = json.loads(unwrap(result.get("outputs", []))) or {}
        if not isinstance(payload, dict):
            raise ValueError("unexpected walk payload")
    except (ValueError, TypeError) as exc:
        # By this point BOTH signals exist (enumeration hints + a dynamic
        # dispatch), so expected targets are known to exist: an unverified
        # walk must fail the DAG build loudly, exactly like truncation —
        # an empty [] here would silently omit required children
        # (review finding 2026-10-14).
        raise RuntimeError(
            f"dispatch-target scan under {sorted(hint_dirs)} near "
            f"{anchor_dir} returned unverifiable output "
            f"({type(exc).__name__}: {exc}); refusing to treat a failed "
            "probe as an empty result"
        ) from exc
    if payload.get("truncated"):
        # A partial dispatch-target list silently omits notebooks from the
        # manifest — their runtime dispatch would fail. Fail the DAG build
        # loudly instead ("no half-fixes").
        raise RuntimeError(
            f"dispatch-target scan truncated above 500 notebooks under "
            f"{sorted(hint_dirs)} near {anchor_dir}; narrow the resource "
            "directory or raise the cap deliberately"
        )
    return payload.get("found", [])


async def _augment_dynamic_dispatch_deps(
    session, tasks, run_deps, base_prefix, source_prefix,
    resolve_dep, effective_parameters,
):
    """Attach dispatch-target notebooks for runtime-computed notebook.run calls.

    The static scan cannot resolve a notebook.run whose path is computed at
    runtime (it only WARNS "may be missed"). When the same job's notebooks
    carry the directory-enumeration pattern (extract_local_resource_hints —
    the same detector dbx_export uses to export those directories), the
    dispatch targets are already sitting under the export base: find the hint
    directory nearest the dynamically-dispatching notebook, take its .ipynb
    files, and resolve each through the ordinary recursive dep machinery so
    the migrator converts them (and their own deps) like any other run_dep.

    Faced: Example_Base_Job 2026-10-13 — generate_base dispatches
    f"../{{target}}" per for-each element; the 31 exported model notebooks
    never entered the manifest, so the migrated dispatch had no target.
    Every addition is printed; jobs without both signals are untouched.
    """
    from dbx_export.dependency_resolver import (
        extract_enumerated_resource_hints,
        extract_run_targets,
    )

    def _flatten(entries):
        for entry in entries:
            yield entry
            yield from _flatten(entry.get("nested_deps") or [])

    # The enumeration hints and the dynamic dispatch routinely live in
    # DIFFERENT tasks (EXBRIDGE: init enumerates models/, generate_base
    # dispatches into it) — collect hints JOB-wide, anchor dispatch per task.
    task_paths, nb_cache = {}, {}
    for task in tasks:
        paths = [task["notebook_path"]]
        for entry in _flatten(run_deps.get(task["task_key"], [])):
            if entry.get("exists") and entry.get("location") == "external":
                paths.append(entry["path"])
        task_paths[task["task_key"]] = paths
    job_hints = set()
    for nb_path in {p for paths in task_paths.values() for p in paths}:
        nb = await _read_notebook_json(session, nb_path)
        nb_cache[nb_path] = nb
        if nb:
            # Enumerated hints only: an ordinary import must not drag a
            # directory of unrelated notebooks into the manifest (review
            # finding 2026-10-13).
            job_hints |= extract_enumerated_resource_hints(nb)
    if not job_hints:
        return

    for task in tasks:
        existing = {
            entry.get("path")
            for entry in _flatten(run_deps.get(task["task_key"], []))
        }
        dynamic_anchor = None
        for nb_path in task_paths[task["task_key"]]:
            nb = nb_cache.get(nb_path)
            if not nb:
                continue
            warn_lines = []
            extract_run_targets(nb, nb_path, log=warn_lines.append)
            if any("non-literal path argument" in line for line in warn_lines):
                dynamic_anchor = nb_path
                break
        if not dynamic_anchor:
            continue
        hints = job_hints

        anchor_dir = os.path.dirname(_normalize_ws_path(dynamic_anchor))
        targets = await _find_dispatch_targets(session, anchor_dir, hints)
        added = 0
        task_parameters = effective_parameters(task)
        for target in targets:
            _, migrated = _to_export_paths(
                _normalize_ws_path(target), base_prefix, source_prefix)
            if migrated in existing:
                continue
            entry = await resolve_dep(target, 0, task_parameters)
            if entry.get("exists"):
                entry["note"] = (
                    "dispatch-target: runtime-computed notebook.run in "
                    + os.path.basename(dynamic_anchor)
                    + "; discovered via resource-dir enumeration"
                )
                run_deps[task["task_key"]].append(entry)
                existing.add(migrated)
                added += 1
        if added:
            print(
                f"  [dispatch-targets] {task['task_key']}: added {added} "
                f"notebook dep(s) under {sorted(hints)} for the "
                f"runtime-computed dispatch in {os.path.basename(dynamic_anchor)}"
            )


# ─── Fix 4 (2026-07-25): widget/parameter observability ──────────────────
#
# Rationale: complements the runtime fail-fast in job_migrate.py's
# process_notebook (Fix 5b). Here we surface the SAME information at
# DAG-build time so operators can see BEFORE launching migration:
#   - Which widgets each task's notebook reads
#   - Which of those have call-site defaults (safe if unresolved)
#   - Which are dynamic (widgets.get(var_name) — unresolvable statically)
#   - Which have declared defaults from in-source widgets.text(K, D)
#
# Regex constants MIRROR the ones in scripts/job_migrate.py's
# extract_widget_reads_and_decls() — kept in sync manually. The
# alternative (importing job_migrate.py) drags heavy dependencies
# into the DAG builder start-up; a small duplication of ~10 regex
# lines is the pragmatic trade-off.

_WIDGET_READ_RE_QUOTED = re.compile(
    # Matches: dbutils.widgets.get("X") | .get('X')
    #          oidlUtils.notebook.get_parameters().get("X"|'X')
    #          oidlUtils.parameters.getParameter("X"|'X')
    r'''(?:dbutils\.widgets\.get|'''
    r'''oidlUtils\.notebook\.get_parameters\s*\(\s*\)\s*\.get|'''
    r'''oidlUtils\.parameters\.getParameter)\s*\(\s*(['"])([A-Za-z_][A-Za-z0-9_]*)\1'''
)
_WIDGET_READ_HAS_DEFAULT_RE = re.compile(
    # Same match + a comma after the name (2-arg form) → has default at call site
    r'''(?:oidlUtils\.notebook\.get_parameters\s*\(\s*\)\s*\.get|'''
    r'''oidlUtils\.parameters\.getParameter)\s*\(\s*(['"])([A-Za-z_][A-Za-z0-9_]*)\1\s*,'''
)
_WIDGET_DECL_RE = re.compile(
    # dbutils.widgets.{text|dropdown|combobox|multiselect}("K", "default", ...)
    r'''dbutils\.widgets\.(?:text|dropdown|combobox|multiselect)\s*\(\s*'''
    r'''(['"])([A-Za-z_][A-Za-z0-9_]*)\1\s*,\s*(['"])([^'"]*)\3'''
)
_WIDGET_READ_DYNAMIC_RE = re.compile(
    # Matches: dbutils.widgets.get(var_name) or f"prefix_{env}" — non-quoted arg
    r'''(?:dbutils\.widgets\.get|'''
    r'''oidlUtils\.notebook\.get_parameters\s*\(\s*\)\s*\.get|'''
    r'''oidlUtils\.parameters\.getParameter)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*|f['"])'''
)


def _strip_line_comments(src):
    """Strip `#`-prefixed comment lines so a commented widget call is not
    treated as an active read. Mirrors _active_lines() in job_migrate.py."""
    if not src:
        return ""
    return "\n".join(
        ln for ln in src.splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    )


async def extract_widget_reads_from_notebook(session, aidp_path):
    """Read a notebook from the cluster, extract widget reads + declarations.

    Returns:
      {
        "reads": [{"name": str, "has_call_site_default": bool}],
        "declarations": {name: default},
        "dynamic": [str],   # unresolvable dynamic accesses
      }

    Uses the same cluster session as the rest of the DAG builder — one
    file read per notebook. Fail-soft: any error returns empty dicts so
    a bad notebook doesn't crash the whole DAG build.
    """
    try:
        code = (
            "import json, builtins\n"
            f"with builtins.open({aidp_path!r}) as _f: print(_f.read())\n"
        )
        result = await run(session, code, timeout=60)
        raw = unwrap(result.get("outputs", []))
        nb = json.loads(raw)
    except Exception as _e:
        return {"reads": [], "declarations": {}, "dynamic": [], "error": str(_e)[:120]}

    reads_names = set()             # names read WITHOUT call-site default
    reads_with_default = set()      # names read WITH call-site default
    declarations = {}
    dynamic = set()

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        active = _strip_line_comments(src)
        if not active.strip():
            continue

        # Widget declarations satisfy later reads within this notebook
        for m in _WIDGET_DECL_RE.finditer(active):
            declarations[m.group(2)] = m.group(4)

        # Reads with a call-site default first (subset of all reads)
        for m in _WIDGET_READ_HAS_DEFAULT_RE.finditer(active):
            reads_with_default.add(m.group(2))

        # All reads (with or without default). Overlap handled below.
        for m in _WIDGET_READ_RE_QUOTED.finditer(active):
            reads_names.add(m.group(2))

        # Dynamic (variable / f-string arg) — unresolvable statically
        for m in _WIDGET_READ_DYNAMIC_RE.finditer(active):
            snippet = m.group(1)
            if snippet.startswith(("'", '"')):
                continue  # already handled by quoted regex
            dynamic.add(snippet)

    # Assemble reads list: distinguish those with/without call-site default
    reads_list = []
    for name in sorted(reads_names):
        reads_list.append({
            "name": name,
            "has_call_site_default": name in reads_with_default,
        })

    return {
        "reads": reads_list,
        "declarations": declarations,      # in-source widgets.text(K, D)
        "dynamic": sorted(dynamic),        # unresolvable dynamic reads
    }


async def discover_widget_reads_for_tasks(session, tasks):
    """For each task notebook, populate widget_reads via extraction.

    Runs sequentially through the tasks to keep cluster-read load
    predictable (each task = one small file read).
    Returns {task_key: widget_info_dict}.
    """
    result = {}
    for t in tasks:
        info = await extract_widget_reads_from_notebook(
            session, t["notebook_path"]
        )
        result[t["task_key"]] = info
    return result


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


def _rewrite_manifest_notebook_path_parameters(
        task_entries, job_params, base_prefix, source_prefix):
    """Resolve notebook_path values through verified task and run_deps paths."""
    exported_by_reference = {}

    def add(reference, exported):
        if not reference or not exported:
            return
        key = _normalize_ws_path(reference)
        previous = exported_by_reference.get(key)
        if previous and previous != exported:
            raise ValueError(
                f"Conflicting exported notebooks for {reference!r}: "
                f"{previous!r} and {exported!r}"
            )
        exported_by_reference[key] = exported

    def walk(deps):
        for dep in deps:
            if dep.get("exists") is not True:
                continue
            exported = dep["path"]
            add(dep.get("original_path"), exported)
            add(exported, exported)
            walk(dep.get("nested_deps", []))

    for task in task_entries:
        exported = task["notebook_path"]
        add(task.get("original_path"), exported)
        add(exported, exported)
        walk(task.get("run_deps", []))

    def rewrite(params, scope):
        value = params.get("notebook_path")
        if not isinstance(value, str) or not value:
            return
        normalized = _normalize_ws_path(value)
        original, _ = _to_export_paths(normalized, base_prefix, source_prefix)
        exported = (exported_by_reference.get(normalized)
                    or exported_by_reference.get(_normalize_ws_path(original)))
        if exported:
            if exported != value:
                params["notebook_path"] = exported
                print(f"  {scope} notebook_path parameter: {value} -> {exported}")
        elif value.startswith((
                "/Workspace/", "/Users/", "/Repos/", "/Shared/", "/GIT/",
                "Workspace/", "Users/", "Repos/", "Shared/", "GIT/")):
            raise ValueError(
                f"{scope} notebook_path parameter {value!r} has no validated "
                "task or run_deps entry in the workflow manifest"
            )

    rewrite(job_params, "job")
    for task in task_entries:
        task_params = task["base_parameters"]
        # The job value wins when the migrator binds this task. Keep the
        # manifest's task value aligned with that validated effective value;
        # otherwise a shadowed source value can fail validation or be reused
        # later without having been included in the dependency scan.
        if ("notebook_path" in task_params
                and isinstance(job_params.get("notebook_path"), str)
                and job_params["notebook_path"]):
            previous = task_params["notebook_path"]
            effective = job_params["notebook_path"]
            if previous != effective:
                print(f"  task:{task['task_key']} notebook_path parameter: "
                      f"{previous} -> {effective} (job override)")
            task_params["notebook_path"] = effective
        else:
            rewrite(task_params, f"task:{task['task_key']}")


def build_manifest(job_name, tasks, layers, run_deps, job_data, base_prefix="",
                   widget_reads=None, run_parameters=None, source_prefix=None,
                   parameter_overrides=None):
    """Build manifest compatible with job_migrate.py.

    Uses the AIDP workflow's task structure + %run deps.

    Fix 4 (2026-07-25): if `widget_reads` (from discover_widget_reads_for_tasks)
    is provided, attach `widget_reads`, `widget_declarations`, `dynamic_widget_reads`
    to each task entry. This is purely additive: existing manifest consumers
    ignore unknown fields (all use `.get(K, default)` on task entries).

    Fix 6 (2026-07-25): if `run_parameters` (from --params-from-run) is provided,
    populate the DAG's `jobs[0].parameters` field with the union of:
      - All widget names read by any task (from widget_reads) — with values
        from run_parameters when the name matches, else empty string
      - All entries in run_parameters (some runs pass extra params not read
        as widgets — e.g. flow-control variables read by scala cells or
        used only in shell scripts we didn't parse)
    Widgets that already have a call-site default OR an in-source declaration
    with default are SKIPPED — those are already safe without a caller value.
    A parallel `parameters_meta` dict records the source of each parameter
    for audit trail (run vs empty-placeholder vs workflow-static).
    """
    task_entries = []
    for task in tasks:
        key = task["task_key"]
        path = task["notebook_path"]
        original_path, _ = _to_export_paths(_normalize_ws_path(path), base_prefix, source_prefix)

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

        # Fix 4: attach widget info if the discovery phase ran
        if widget_reads and key in widget_reads:
            _wi = widget_reads[key]
            # Only attach non-empty fields to keep manifest tidy
            if _wi.get("reads"):
                task_entry["widget_reads"] = _wi["reads"]
            if _wi.get("declarations"):
                task_entry["widget_declarations"] = _wi["declarations"]
            if _wi.get("dynamic"):
                task_entry["dynamic_widget_reads"] = _wi["dynamic"]
            if _wi.get("error"):
                task_entry["widget_reads_error"] = _wi["error"]

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

    # ── Fix 6 (2026-07-25): auto-populate widget/run params ──────────────
    # If --params-from-run was provided (run_parameters non-empty), OR if we
    # discovered widget reads (widget_reads populated), aggregate them here.
    # Result: jobs[0].parameters contains every param name the operator MIGHT
    # need to provide, either with a real value (from the prior run) or an
    # empty-string placeholder they can override in the DAG file.
    #
    # A `parameters_meta` audit trail records the origin of each param:
    #   - "run"            → value came from --params-from-run
    #   - "empty:widget"   → widget name discovered but no run value; empty placeholder
    #   - "workflow"       → from the workflow spec's job.parameters (static)
    #   - "cli"            → final explicit --param override
    # Existing DAG consumers only read `parameters`; they never look at
    # `parameters_meta`, so this is purely additive.
    parameters_meta: Dict[str, Dict[str, Any]] = {}
    for k, v in job_params.items():
        parameters_meta[k] = {"source": "workflow", "widget": False,
                              "value_present": v != ""}

    _run_params = run_parameters or {}
    # Names of widgets that are ALREADY SAFE (no external value needed):
    #   - call-site default: get_parameters().get("K", default) — Python
    #     supplies its own fallback
    #   - in-source declaration: dbutils.widgets.text("K", "default")
    _widget_names_safe_without_value: set = set()
    _widget_names_needing_value: set = set()
    if widget_reads:
        for _key, _wi in widget_reads.items():
            for _r in _wi.get("reads", []) or []:
                _wname = _r.get("name")
                if not _wname:
                    continue
                if _r.get("has_call_site_default"):
                    _widget_names_safe_without_value.add(_wname)
                else:
                    _widget_names_needing_value.add(_wname)
            for _dname in (_wi.get("declarations") or {}):
                _widget_names_safe_without_value.add(_dname)
    # A widget can appear in BOTH sets across different tasks (e.g. one task
    # calls .get("K", default), another calls .get("K")). Treat it as needing
    # a value — the strict caller wins. This avoids false-safe assumptions.
    _widget_names_needing_value |= (_widget_names_needing_value - _widget_names_safe_without_value)

    # 1) Populate widget names that need a value
    for _wname in sorted(_widget_names_needing_value):
        if _wname in job_params and not (
                _wname == "notebook_path" and job_params[_wname] == ""):
            # workflow spec already provided it; leave workflow value + meta
            continue
        if _wname in _run_params and not (
                _wname == "notebook_path" and _run_params[_wname] == ""):
            job_params[_wname] = _run_params[_wname]
            parameters_meta[_wname] = {
                "source": "run", "widget": True,
                "value_present": _run_params[_wname] != "",
            }
        else:
            # No value known — record in parameters_meta only. Do NOT add an
            # empty placeholder to job.parameters: downstream job_migrate.py
            # merges job.parameters ON TOP of task.base_parameters, and an
            # empty placeholder would silently WIPE a per-task value the
            # operator set (e.g. a task-level dateStr). parameters_meta is
            # the audit-trail slot and is not consumed as an override, so
            # operator visibility is preserved without the wipe hazard.
            parameters_meta[_wname] = {"source": "empty:widget", "widget": True, "value_present": False}

    # 2) Include any run params that WEREN'T detected as widget reads.
    #    Reason: some params are consumed in ways we don't statically parse
    #    (Scala cells, shell scripts, spark.conf lookups). Better to include
    #    than to silently drop — extra keys in manifest_params.json are
    #    harmless if no one reads them.
    for _rname, _rvalue in _run_params.items():
        if _rname == "notebook_path" and _rvalue == "":
            continue
        if _rname in job_params and not (
                _rname == "notebook_path" and job_params[_rname] == ""):
            continue  # already populated above
        job_params[_rname] = _rvalue
        parameters_meta[_rname] = {
            "source": "run", "widget": False, "value_present": _rvalue != "",
        }

    # The CLI's discovery parameters previously overwrote job.parameters after
    # build_manifest returned. Apply them here so the path rewrite sees the
    # final value, including --param notebook_path=... overrides.
    if parameter_overrides:
        for key, value in parameter_overrides.items():
            if key == "notebook_path" and value == "":
                print("  job notebook_path parameter: empty CLI override ignored")
                continue
            update_parameters(job_params, {key: value})
            parameters_meta[key] = {
                "source": "cli", "widget": key in _widget_names_needing_value,
                "value_present": value != "",
            }
    _rewrite_manifest_notebook_path_parameters(
        task_entries, job_params, base_prefix, source_prefix,
    )

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
    # Attach parameters_meta only if we actually populated any (i.e. widget
    # reads or --params-from-run gave us content). Empty dict adds noise for
    # legacy workflows with no widgets and no run params.
    if parameters_meta:
        job_entry["parameters_meta"] = parameters_meta
    if default_cluster:
        job_entry["default_cluster"] = default_cluster

    return {
        "notes": (
            "Task 'notebook_path', validated 'notebook_path' parameter values, "
            "and run_dep 'path' are AIDP locations under the export base — "
            "this is what downstream tooling (job_migrate, data validation) "
            "should consume. An unmatched notebook_path parameter fails "
            "manifest construction instead of inventing an exported path. "
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
    parser.add_argument("--cluster", required=True,
                        help="AIDP cluster ID for scanning notebooks (required — no default)")
    parser.add_argument("--output",
                        help="Output file for manifest (default: stdout)")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate notebooks exist and print task graph")
    parser.add_argument("--export-output", default=None,
                        help="AIDP export base the notebooks were migrated to. Nested "
                             "deps are re-rooted and validated under <target-base>/notebooks. "
                             "If omitted, auto-derived from the first task's notebook path "
                             "in the workflow (regex: ^(/Workspace/.*?)/notebooks/). "
                             "Fails loud if derivation fails.")
    parser.add_argument("--source-base", default=None,
                        help="Source (dbx_export) base where the task notebooks CURRENTLY "
                             "live on the cluster. Used to correctly strip the source "
                             "prefix from %%run targets that resolve under it — without "
                             "this, when --export-output ≠ source (typical when dbx_export "
                             "and migration output go to different folders), %%run "
                             "paths get double-prefixed. "
                             "If omitted, auto-derived from the common prefix of task "
                             "notebook paths ending at /notebooks. Safe to leave unset "
                             "unless auto-derivation guesses wrong.")
    parser.add_argument("--params-from-run", default=None, metavar="RUN_KEY",
                        help="Fix 6: fetch the `parameters` from a prior successful AIDP job "
                             "run and populate the DAG's jobs[0].parameters with those "
                             "values. Also adds every widget name read by any task "
                             "(from widget_reads discovery) — with an empty-string "
                             "placeholder if no matching run value — so the operator can "
                             "see the full set of overridable params directly in the DAG. "
                             "Use when the workflow trigger receives params from an "
                             "external orchestrator (Airflow / notebook.run) instead of "
                             "from static task base_parameters. RUN_KEY = the AIDP job run "
                             "UUID (GET /jobRuns/{RUN_KEY}).")
    parser.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE",
        help="Override a workflow parameter during dependency discovery and write it "
             "to the generated manifest. Repeat for multiple values.",
    )
    # AIDP environment — default None so YAML (via --config) is primary source.
    parser.add_argument("--config", default=None,
                        help="Path to aidp_migrator.yaml. Falls back to $AIDP_CONFIG_PATH, "
                             "./aidp_migrator.yaml, ~/aidp_migrator.yaml (discovery order).")
    parser.add_argument("--lake-ocid", default=None,
                        help="Override cfg.aidp.lake_ocid for this run only.")
    parser.add_argument("--workspace-id", default=None,
                        help="Override cfg.aidp.workspace_id for this run only.")
    parser.add_argument("--oci-profile", default=None,
                        help="Override cfg.aidp.oci_profile for this run only.")
    args = parser.parse_args()

    # ── Load customer YAML + backfill any arg the user did not pass ──
    from aidp_config import load_config as _load_cfg
    _cfg = _load_cfg(cli_config_path=args.config)
    # Honor an explicit --config for the on-cluster Credential-Store signer:
    # get_oci_signer -> _apply_cred_env_from_config re-discovers the YAML and only
    # sees $AIDP_CONFIG_PATH / default locations, NOT --config. Export it so a
    # custom YAML passed only via --config still drives AIDP_CRED_* on the cluster
    # (else a renamed credential falls back to default names -> 401). No-op when
    # the config is already discoverable. (a reviewer #129 review P1.)
    if args.config:
        os.environ["AIDP_CONFIG_PATH"] = args.config
    if not args.lake_ocid:
        args.lake_ocid = _cfg.aidp.lake_ocid
    if not args.workspace_id:
        args.workspace_id = _cfg.aidp.workspace_id
    if not args.oci_profile:
        args.oci_profile = _cfg.aidp.oci_profile
    if not args.export_output:
        args.export_output = _cfg.paths.export_output

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

    # ── Derive the export base prefix that nested deps are re-rooted under. ──
    # If --export-output was not provided, auto-derive from the first task's
    # notebook path. The path is expected to have shape
    #     /Workspace/<export-base>/notebooks/<user-tree>/<file>.ipynb
    # so we capture everything up to (but excluding) /notebooks/.
    if args.export_output is None:
        import re as _re
        _base = None
        for _t in tasks:
            _p = _t.get("notebook_path", "")
            _m = _re.match(r'^(/Workspace/.*?)/notebooks/', _p)
            if _m:
                _base = _m.group(1)
                break
        if not _base:
            print(f"ERROR: Cannot auto-derive --export-output from workflow task "
                  f"notebook paths (regex ^(/Workspace/.*?)/notebooks/ did not match). "
                  f"Pass --export-output explicitly. Sample task path: "
                  f"{tasks[0].get('notebook_path', '?')!r}")
            sys.exit(1)
        args.export_output = _base
        print(f"Auto-derived --export-output: {_base}")
    _tb = args.export_output.rstrip("/")
    if not _tb.startswith("/Workspace"):
        _tb = "/Workspace/" + _tb.lstrip("/")
    base_prefix = _tb + "/notebooks"

    # Auto-derive --source-base from task notebook paths' common prefix if not
    # provided. `source_prefix` (= <source-base>/notebooks) is passed to
    # _to_export_paths so it can strip the source location correctly when
    # target ≠ source, preventing the path-doubling bug (2026-07-28 ExampleVendor
    # failure: /Workspace/<TARGET>/notebooks/<SOURCE>/notebooks/... ).
    if args.source_base is None:
        source_prefix = _infer_source_prefix(tasks)
        if source_prefix and source_prefix != base_prefix:
            print(f"Auto-derived --source-base: {source_prefix[:-len('/notebooks')]}")
    else:
        _sb = args.source_base.rstrip("/")
        if not _sb.startswith("/Workspace"):
            _sb = "/Workspace/" + _sb.lstrip("/")
        source_prefix = _sb + "/notebooks"

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

    # Run-derived values must be present during dependency discovery.  The
    # manifest rewrite below validates notebook_path against those discovered
    # edges, so fetching them afterward would reject a valid parameter child.
    run_parameters = {}
    if args.params_from_run:
        print(f"\nFetching parameters from run: {args.params_from_run}")
        run_parameters = fetch_run_parameters(
            args.workspace_id, args.params_from_run,
            lake_ocid=args.lake_ocid, oci_profile=args.oci_profile,
        )
        if run_parameters:
            print(f"Run parameters found: {len(run_parameters)} key(s): "
                  f"{sorted(run_parameters.keys())}")
        else:
            print(f"WARNING: no parameters retrieved from run "
                  f"{args.params_from_run} — the manifest's parameters will "
                  f"still be populated from widget-read names (with empty "
                  f"placeholders), but you'll need to fill in values manually.")

    # ── Step 4: Discover %run dependencies ──
    print("\nScanning %run dependencies...")
    discovery_parameters = {
        p.get("key", p.get("name", "")): p.get("value", "")
        for p in (job_data.get("parameters") or []) if isinstance(p, dict)
    }
    # Match build_manifest's precedence: workflow values win over run values;
    # explicit CLI values win over both. Task values are applied underneath
    # these job-level bindings by discover_run_deps_for_tasks().
    for key, value in run_parameters.items():
        if key not in discovery_parameters or (
                key == "notebook_path" and discovery_parameters[key] == ""):
            update_parameters(discovery_parameters, {key: value})
    cli_parameters = {}
    for raw in args.param:
        key, separator, value = raw.partition("=")
        if not separator or not key.strip():
            print(f"ERROR: invalid --param {raw!r}; expected KEY=VALUE")
            await session.close()
            sys.exit(2)
        cli_parameters[key.strip()] = value
        update_parameters(discovery_parameters, {key.strip(): value})
    run_deps = await discover_run_deps_for_tasks(
        session, tasks, base_prefix, source_prefix,
        parameter_overrides=discovery_parameters,
    )

    # ── Step 4b (Fix 4): scan each task notebook for widget/parameter reads ──
    # This surfaces widget dependencies BEFORE migration starts. Reads that
    # can't be resolved from base_parameters, workflow parameters, --param
    # CLI, or in-source widgets.text() defaults would otherwise cause the
    # migrated notebook to fail on eval(None) / NameError cascades at run
    # time (per a pilot-customer AutoRefit 2026-07-25 root-cause analysis).
    # Attached to each task entry in the manifest; consumers use .get(...) so
    # this is a purely additive schema change.
    print("\nScanning widget parameter reads...")
    widget_reads = await discover_widget_reads_for_tasks(session, tasks)
    _total_widgets = sum(len(v.get("reads", [])) for v in widget_reads.values())
    print(f"Widget reads: {_total_widgets} total across {len(tasks)} task(s)")

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
    manifest = build_manifest(job_name, tasks, layers, run_deps, job_data,
                              base_prefix, widget_reads=widget_reads,
                              run_parameters=run_parameters,
                              source_prefix=source_prefix,
                              parameter_overrides=cli_parameters)

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
