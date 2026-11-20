#!/usr/bin/env python3
"""
DEPRECATED as a CLI — use scripts/build_dag_from_workflow.py instead.
=====================================================================
Directory-scan DAG building predates AIDP workflows and misses workflow-level
task structure. When an AIDP workflow exists (the normal case), the manifest
MUST come from build_dag_from_workflow.py --job-key. This module remains
importable as a library for %run-dependency discovery.

Build DAG from notebook %run dependencies.
=============================================
Given a root notebook or directory on AIDP, discovers all %run and
dbutils.notebook.run dependencies, builds a DAG, and outputs a
job manifest entry that job_migrate.py can consume.

Recursively follows external %run targets (notebooks outside --root)
to discover the full dependency tree, including nested deps.

Usage:
    python3 build_dag.py --root "Users/user1@example.com/Payments/Example_Optimizer_Realtime" \
        --entry "4_Metric_Calculation_Automated.ipynb" \
        --job-name "Example_Optimizer_Realtime" \
        --cluster <CLUSTER-ID>

    # Just print the dependency graph
    python3 build_dag.py --root "Users/user1@example.com/Payments/Example_Optimizer_Realtime" --graph-only
"""

import argparse
import asyncio
import inspect
import json
import os
import posixpath
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, PROJECT_DIR)
from aidp_executor import AIDPSession
from context_tools import _unwrap_aidp_text
from dbx_export.dependency_resolver import portable_run_extractor_source
from dbx_export.sanitizer import (
    _legacy_sanitize_segment, sanitize_segment, sanitize_path as _sanitize_path,
    rewrite_embedded_workspace_ref as _rewrite_embedded_workspace_ref,
)

PORTABLE_RUN_EXTRACTOR_SOURCE = portable_run_extractor_source()
DEFAULT_CLUSTER = None  # generic — required via --cluster (no hardcoded default)

MAX_RECURSION_DEPTH = 10


class RunTargets(list):
    """Notebook targets with the subset inferred only by uncertain scanning."""

    def __init__(self, targets=(), loose_only=()):
        super().__init__(targets)
        self.loose_only = set(loose_only)


def unwrap(outputs):
    """Extract text from AIDP session outputs, handling JSON wrapping."""
    text = ""
    seen = set()
    for o in outputs:
        if o.get("type") == "stream":
            raw = o.get("text", "")
            # AIDP sometimes prepends an empty-envelope artifact (exactly "[]",
            # no trailing newline) before the real stdout. A genuine print([])
            # arrives as "[]\n", so skip only the exact bare-"[]" case.
            if raw == "[]":
                continue
            val = _unwrap_aidp_text(raw)
            if val not in seen:
                text += val
                seen.add(val)
        elif o.get("type") == "execute_result":
            data = o.get("data", {})
            raw = data.get("text/plain", "")
            val = _unwrap_aidp_text(raw)
            if val not in seen:
                text += val
                seen.add(val)
    return text


async def run(session, code, timeout=60):
    """Thin wrapper: AIDPSession exposes _execute_locked, not execute."""
    return await session._execute_locked(code, timeout=timeout)


async def discover_notebooks(session, workspace_root):
    """List all .ipynb files under a workspace directory (or return the single file)."""
    result = await run(session, f"""
import os, json
base = "/Workspace/{workspace_root}"
nbs = []
if os.path.isfile(base) and base.endswith(".ipynb"):
    nbs.append(os.path.basename(base))
else:
    for root, dirs, files in os.walk(base):
        for f in files:
            if f.endswith(".ipynb"):
                nbs.append(os.path.join(root, f)[len(base)+1:])
print(json.dumps(nbs))
""", timeout=60)
    output = unwrap(result.get("outputs", []))
    try:
        return json.loads(output)
    except:
        print(f"ERROR discovering notebooks: {output[:200]}")
        return []


async def extract_dependencies(session, workspace_root, parameter_bindings=None):
    """Extract %run and notebook.run dependencies from all notebooks."""
    parameter_bindings = dict(parameter_bindings or {})
    result = await run(session,f"""
import json, os

{PORTABLE_RUN_EXTRACTOR_SOURCE}

base = "/Workspace/{workspace_root}"
workspace_root = {json.dumps(workspace_root)}
parameter_bindings = {json.dumps(parameter_bindings)}
deps = {{}}

# Collect notebook paths to process
_nb_paths = []
if os.path.isfile(base) and base.endswith(".ipynb"):
    _nb_paths.append(base)
    base = os.path.dirname(base)
else:
    for root, dirs, files in os.walk(base):
        for f in files:
            if f.endswith(".ipynb"):
                _nb_paths.append(os.path.join(root, f))

for path in _nb_paths:
        rel = path[len(base)+1:]
        try:
            with open(path) as fh:
                nb = json.load(fh)
            cell_count = 0
            code_cells = 0
            for cell in nb.get("cells", []):
                cell_count += 1
                src = "".join(cell.get("source", []))
                if cell.get("cell_type") == "code" and src.strip():
                    code_cells += 1

            # %run / notebook.run extraction is handled EXCLUSIVELY by the
            # portable extractor below. The legacy per-cell regex scan that
            # used to live here was dead code (its result was discarded) and,
            # worse, a crash vector after the fail-loud change: a whitespace-
            # only "%run " line raised IndexError in the legacy parser, which
            # the error surfacing then escalated into a RuntimeError that
            # killed discovery for the entire notebook — for a line the real
            # extractor handles fine (PR#58 re-review finding).
            shared_runs = extract_run_targets(
                nb, path, parameter_bindings=parameter_bindings,
            )
            runs = []
            root_prefixes = (
                "/" + workspace_root.strip("/") + "/",
                "/Workspace/" + workspace_root.strip("/") + "/",
            )
            for target in shared_runs:
                normalized = target.replace("\\\\", "/").rstrip("/")
                for prefix in root_prefixes:
                    if normalized.startswith(prefix):
                        normalized = normalized[len(prefix):]
                        break
                if not normalized.endswith(".ipynb"):
                    normalized += ".ipynb"
                runs.append(normalized)
            deps[rel] = {{"runs": list(dict.fromkeys(runs)), "cells": cell_count, "code_cells": code_cells}}
        except Exception as e:
            deps[rel] = {{"runs": [], "cells": 0, "code_cells": 0, "error": str(e)[:100]}}

print(json.dumps(deps))
""", timeout=120)
    output = unwrap(result.get("outputs", []))
    try:
        data = json.loads(output)
        errors = {
            path: info.get("error")
            for path, info in data.items()
            if isinstance(info, dict) and info.get("error")
        }
        if errors:
            raise RuntimeError(
                "Dependency extraction failed inside the AIDP kernel: "
                + json.dumps(errors, sort_keys=True)
            )
        return data
    except RuntimeError:
        raise
    except:
        print(f"ERROR extracting deps: {output[:500]}")
        return {}


def _normalize_ws_path(path):
    """Normalize workspace paths: ensure /Workspace/ prefix, .ipynb suffix, resolve '..' segments.

    Divergence note (PR-34 review, 2026-07-27): for a bare `Workspace/foo` input,
    this function produces `/Workspace/Workspace/foo.ipynb` (the else-branch
    prepends `/Workspace/` unconditionally). That's the CORRECT behavior for
    build_dag's discovery scan — DBX APIs return paths under `/Workspace/`, and
    a raw scan target of `Workspace/foo` is interpreted here as "the folder
    literally named 'Workspace' at the DBX root". This DISAGREES with
    job_migrate.py's `_fix_run` post-PR-34, which treats bare `Workspace/foo`
    as caller-relative. The disagreement is intentional: at discovery time we
    canonicalize to the DBX REST form; at migration-rewrite time we mimic
    vanilla %run semantics. If you change either, check both.
    """
    path = path.rstrip("/")
    if not path.endswith(".ipynb"):
        path += ".ipynb"
    if path.startswith("/Workspace/"):
        pass
    elif path.startswith("/"):
        path = f"/Workspace{path}"
    else:
        path = f"/Workspace/{path}"
    # Resolve '..' and '.' segments (e.g. /Workspace/GIT/src/notebooks/../scripts/utils.ipynb
    #   -> /Workspace/GIT/src/scripts/utils.ipynb). Use posixpath so a Windows
    # client doesn't collapse '/' to '\' in paths shipped to the Linux cluster.
    path = posixpath.normpath(path)
    return path


def _to_export_paths(norm_path, base_prefix, source_prefix=None):
    """Map a normalized workspace path to its original and sanitized export paths.

    Strip the target base first, then a distinct source base, then /Workspace.
    This priority preserves already migrated paths and avoids double-prefixing
    references when the source export and migration target differ. Kept here
    so the workflow resolver and generated %run namespace scan share exactly
    the same mapping and upload sanitizer.
    """
    if base_prefix and norm_path.startswith(base_prefix + "/"):
        canonical = norm_path[len(base_prefix):]
    elif source_prefix and source_prefix != base_prefix and norm_path.startswith(source_prefix + "/"):
        canonical = norm_path[len(source_prefix):]
    elif norm_path.startswith("/Workspace/"):
        canonical = norm_path[len("/Workspace"):]
    else:
        canonical = norm_path if norm_path.startswith("/") else "/" + norm_path
    original = "/Workspace" + canonical
    canonical_sanitized = _sanitize_path(canonical) if base_prefix else canonical
    migrated = (base_prefix + canonical_sanitized) if base_prefix else original
    return original, migrated


def _notebook_path_candidates(path, base_prefix="", source_prefix=None):
    """Share migrated-first lookup and legacy name variants across DAG scans."""
    norm = _normalize_ws_path(_rewrite_embedded_workspace_ref(path))
    original, migrated = _to_export_paths(norm, base_prefix, source_prefix)
    candidates = [migrated]
    if base_prefix:
        legacy_suffix = _sanitize_path(original[len("/Workspace"):], legacy=True)
        if base_prefix + legacy_suffix != migrated:
            candidates.append(base_prefix + legacy_suffix)
            if source_prefix and source_prefix != base_prefix:
                candidates.append(source_prefix + legacy_suffix)
    alt = migrated.replace(" ", "_") if " " in migrated else migrated.replace("_", " ")
    candidates.append(_normalize_ws_path(alt))
    if "-" in migrated:
        candidates.append(_normalize_ws_path(migrated.replace("-", "_")))
    if " " in migrated or "-" in migrated:
        candidates.append(_normalize_ws_path(migrated.replace(" ", "_").replace("-", "_")))
    return list(dict.fromkeys(candidates))


_PORTABLE_PATH_LOOKUP_SOURCE = "\n\n".join(inspect.getsource(helper) for helper in (
    _legacy_sanitize_segment, sanitize_segment, _sanitize_path, _rewrite_embedded_workspace_ref,
    _normalize_ws_path, _to_export_paths, _notebook_path_candidates,
)) + "\n_sanitize_path = sanitize_path\n_rewrite_embedded_workspace_ref = rewrite_embedded_workspace_ref\n"


async def scan_external_notebook(
    session, notebook_path, parameter_bindings=None, *, base_prefix="", source_prefix=None,
):
    """Read a single notebook on the cluster and extract its %run / notebook.run targets.

    Returns (exists: bool, runs: list[str]) where runs are raw target paths.
    """
    full_path = _normalize_ws_path(notebook_path)
    parameter_bindings = dict(parameter_bindings or {})

    scan_code = f"""
import json, os

{PORTABLE_RUN_EXTRACTOR_SOURCE}
{_PORTABLE_PATH_LOOKUP_SOURCE}

path = {json.dumps(full_path)}
parameter_bindings = {json.dumps(parameter_bindings)}
base_prefix = {json.dumps(base_prefix)}
source_prefix = {source_prefix!r}
namespace_skipped = []
diagnostics = []
if not os.path.exists(path):
    print(json.dumps({{"exists": False, "runs": [], "namespace_skipped": []}}))
else:
    try:
        with open(path) as fh:
            nb = json.load(fh)
        # %run executes its child in this notebook's namespace.  Import only
        # the child's static assignments into the caller's parameter bindings;
        # do not flatten child cells into the caller.  A child-local relative
        # notebook.run must be resolved from the child's path by the recursive
        # scanner, never from this root task.
        def _percent_run_namespace(current_nb, current_path, seen=None):
            seen = set(seen or ())
            if current_path in seen:
                return {{}}
            seen.add(current_path)
            namespace = {{}}
            for child in _extract_percent_run_targets(current_nb, current_path):
                candidates = _notebook_path_candidates(child, base_prefix, source_prefix)
                tried = []
                child_path = None
                for candidate in candidates:
                    tried.append(candidate)
                    if os.path.exists(candidate):
                        child_path = candidate
                        break
                if child_path is None:
                    namespace_skipped.append({{"target": child, "tried": tried, "reason": "not found"}})
                    continue
                try:
                    with open(child_path) as child_fh:
                        child_nb = json.load(child_fh)
                except (OSError, ValueError, TypeError) as exc:
                    namespace_skipped.append({{"target": child, "tried": tried,
                                              "reason": "unreadable: " + str(exc)}})
                    continue
                child_bindings = dict(parameter_bindings)
                _merge_parameter_bindings(child_bindings, _percent_run_namespace(
                    child_nb, child_path, seen
                ))
                trees = []
                for cell in child_nb.get("cells", []):
                    if cell.get("cell_type") != "code":
                        continue
                    source = cell.get("source", "")
                    source = "".join(source) if isinstance(source, list) else str(source or "")
                    try:
                        trees.append(_parse_python_source(_active_source(source)))
                    except SyntaxError:
                        continue
                _merge_parameter_bindings(namespace, _static_environment(trees, child_bindings))
            return namespace
        _merge_parameter_bindings(parameter_bindings, _percent_run_namespace(nb, path))
        loose_only = set()
        runs = extract_run_targets(
            nb, path, parameter_bindings=parameter_bindings, log=diagnostics.append,
            loose_only=loose_only,
        )
        print(json.dumps({{"exists": True, "runs": list(dict.fromkeys(runs)),
                          "loose_only": sorted(loose_only),
                          "namespace_skipped": namespace_skipped, "diagnostics": diagnostics}}))
    except Exception as e:
        print(json.dumps({{"exists": True, "runs": [], "error": str(e)[:100]}}))
"""
    # An existence probe cannot stand in for dependency extraction. The
    # incident scan timed out, then an existence-only retry fabricated runs=[]
    # and produced an incomplete authoritative manifest. Retry the SAME scan;
    # if neither attempt yields a complete result, stop manifest construction.
    for attempt in range(1, 3):
        try:
            result = await run(session, scan_code, timeout=60)
            if result.get("status", "ok") != "ok":
                raise ValueError("Dependency scan execution did not succeed")
            output = unwrap(result.get("outputs", []))
            data = json.loads(output)
            if (not isinstance(data, dict) or not isinstance(data.get("exists"), bool)
                    or not isinstance(data.get("runs"), list)
                    or any(not isinstance(path, str) for path in data["runs"])):
                raise ValueError("Incomplete dependency scan result")
            if data.get("error"):
                raise RuntimeError(
                    f"Dependency extraction failed for {notebook_path}: "
                    f"{data['error']}"
                )
            for skipped in data.get("namespace_skipped", []):
                print(
                    f"  [namespace-skip] {notebook_path}: %run {skipped['target']} "
                    f"{skipped['reason']} — static config values NOT imported; "
                    f"tried: {', '.join(skipped['tried'])}"
                )
            for diagnostic in data.get("diagnostics", []):
                print(diagnostic)
            runs = []
            for target in data.get("runs", []):
                normalized = str(target).strip().rstrip("/")
                if normalized and not normalized.endswith(".ipynb"):
                    normalized += ".ipynb"
                if normalized:
                    runs.append(posixpath.normpath(normalized))
            inferred = set()
            for target in data.get("loose_only", []):
                normalized = str(target).strip().rstrip("/")
                if normalized and not normalized.endswith(".ipynb"):
                    normalized += ".ipynb"
                if normalized:
                    inferred.add(posixpath.normpath(normalized))
            return data.get("exists", False), RunTargets(dict.fromkeys(runs), inferred)
        except RuntimeError:
            raise
        except Exception as exc:
            print(f"  WARNING: dependency scan attempt {attempt}/2 failed for "
                  f"{notebook_path}: {type(exc).__name__}")
            if attempt == 2:
                raise RuntimeError(
                    f"Cannot verify notebook existence or dependencies for "
                    f"{notebook_path}: dependency extraction failed after 2 attempts"
                ) from exc


async def resolve_run_deps_recursive(
    session, workspace_root, deps, parameter_bindings=None,
):
    """Recursively discover all %run dependencies, including external notebooks.

    For each notebook in deps, classifies its %run targets as internal (under --root)
    or external. External targets are scanned on the cluster to discover their own
    nested deps, recursively up to MAX_RECURSION_DEPTH.

    Returns {notebook_rel_path: [run_dep_entry, ...]} where each run_dep_entry is:
        {"path": str, "location": "internal"|"external", "exists": bool|None,
         "nested_deps": [run_dep_entry, ...]}
    """
    all_internal = set(deps.keys())
    parameter_bindings = dict(parameter_bindings or {})
    # Cache: external path -> (exists, nested_deps list)
    _ext_cache = {}

    async def _resolve_external(path, depth):
        """Recursively resolve a single external notebook path."""
        path = _normalize_ws_path(path)
        if path in _ext_cache:
            return _ext_cache[path]

        if depth >= MAX_RECURSION_DEPTH:
            entry = {"path": path, "location": "external", "exists": None,
                     "nested_deps": [], "note": "max recursion depth reached"}
            _ext_cache[path] = entry
            return entry

        # Mark THIS path as in-progress before scanning children.
        # Only a true cycle (child -> ... -> this path) will hit this placeholder.
        _ext_cache[path] = {"path": path, "location": "external", "exists": None,
                            "nested_deps": [], "note": "circular dependency"}

        original_path = path
        exists, child_runs = await scan_external_notebook(
            session, path, parameter_bindings=parameter_bindings,
        )

        # If not found, retry with name variants: space↔underscore, hyphen→underscore
        if not exists:
            variants = set()
            alt_path = path.replace(" ", "_") if " " in path else path.replace("_", " ")
            variants.add(_normalize_ws_path(alt_path))
            if "-" in path:
                variants.add(_normalize_ws_path(path.replace("-", "_")))
            if " " in path or "-" in path:
                variants.add(_normalize_ws_path(path.replace(" ", "_").replace("-", "_")))
            variants.discard(path)
            for alt_path in variants:
                alt_exists, alt_runs = await scan_external_notebook(
                    session, alt_path, parameter_bindings=parameter_bindings,
                )
                if alt_exists:
                    print(f"    -> Resolved via variant: {path} -> {alt_path}")
                    path = alt_path
                    exists, child_runs = alt_exists, alt_runs
                    break

        nested = []
        if exists and child_runs:
            for child_path in child_runs:
                if child_path in all_internal:
                    nested.append({"path": child_path, "location": "internal",
                                   "exists": True, "nested_deps": []})
                else:
                    child_entry = await _resolve_external(child_path, depth + 1)
                    nested.append(child_entry)

        entry = {"path": path, "location": "external", "exists": exists,
                 "nested_deps": nested}
        if path != original_path:
            entry["original_path"] = original_path
        _ext_cache[path] = entry
        return entry

    result = {}
    for nb, info in deps.items():
        run_deps = []
        for target in info.get("runs", []):
            norm_target = _normalize_ws_path(target)
            if target in all_internal:
                run_deps.append({"path": target, "location": "internal",
                                 "exists": True, "nested_deps": []})
            else:
                indent = "  " if len(deps) > 1 else ""
                print(f"{indent}[{os.path.basename(nb)}] Scanning external dep: {norm_target}")
                entry = await _resolve_external(norm_target, 0)
                run_deps.append(entry)
        result[nb] = run_deps

    return result


def build_layers(deps, entry_points=None):
    """Build execution layers from dependency graph.
    Layer 0 = leaves (no deps), Layer N = depends on Layer N-1."""

    all_nbs = set(deps.keys())

    # Build reverse dep map: what does each notebook depend on?
    nb_deps = {}
    for nb, info in deps.items():
        nb_deps[nb] = set(info.get("runs", []))

    # If entry points specified, only include notebooks reachable from them
    if entry_points:
        reachable = set()
        queue = list(entry_points)
        while queue:
            nb = queue.pop(0)
            if nb in reachable:
                continue
            reachable.add(nb)
            for dep in nb_deps.get(nb, []):
                if dep in all_nbs:
                    queue.append(dep)
        all_nbs = reachable

    # Topological sort into layers
    layers = []
    assigned = set()
    remaining = set(all_nbs)

    while remaining:
        # Find notebooks whose deps are all assigned
        layer = []
        for nb in remaining:
            unmet = nb_deps.get(nb, set()) - assigned
            # Only count deps that are in our set
            unmet = unmet & all_nbs
            if not unmet:
                layer.append(nb)

        if not layer:
            # Circular dependency - break by picking one
            layer = [next(iter(remaining))]
            print(f"WARNING: Circular dependency detected, breaking with {layer[0]}")

        layers.append(sorted(layer))
        assigned.update(layer)
        remaining -= set(layer)

    return layers


def build_manifest_entry(job_name, workspace_root, layers, deps, run_deps_map=None,
                         parameters=None):
    """Build a job manifest entry compatible with job_migrate.py."""
    tasks = []
    for i, layer in enumerate(layers):
        for nb in layer:
            task_key = os.path.splitext(os.path.basename(nb))[0]
            # Deduplicate task keys
            existing_keys = [t["task_key"] for t in tasks]
            if task_key in existing_keys:
                task_key = f"{task_key}_{i}"

            info = deps.get(nb, {})
            depends_on = []
            for dep in info.get("runs", []):
                dep_key = os.path.splitext(os.path.basename(dep))[0]
                if dep_key in existing_keys:
                    depends_on.append(dep_key)

            task_entry = {
                "task_key": task_key,
                "notebook_path": f"/Workspace/{workspace_root}/{nb}",
                "depends_on": depends_on,
                "base_parameters": {},
                "resolved_path": f"{workspace_root}/{nb}",
                "resolution_status": "direct",
                "cells": info.get("cells", 0),
                "code_cells": info.get("code_cells", 0),
            }

            if run_deps_map and nb in run_deps_map:
                task_entry["run_deps"] = run_deps_map[nb]

            tasks.append(task_entry)

    # Map notebook filenames to task_keys for execution_layers
    nb_to_key = {}
    for t in tasks:
        # resolved_path = workspace_root/nb, extract nb part
        nb_rel = t["resolved_path"][len(workspace_root) + 1:]
        nb_to_key[nb_rel] = t["task_key"]

    # root tells job_migrate.py what prefix to strip from notebook paths.
    # If the root contains "Users/", only strip up to (not including) that part
    # so output preserves the Users/.../ directory structure.
    strip_root = workspace_root
    users_idx = workspace_root.find("Users/")
    if users_idx > 0:
        strip_root = workspace_root[:users_idx].rstrip("/")

    return {"jobs": [{
        "job_name": job_name,
        "job_id": 0,
        "root": strip_root,
        "tasks": tasks,
        "parameters": parameters or {},
        "execution_layers": [sorted(nb_to_key.get(nb, nb) for nb in layer) for layer in layers],
    }]}


def collect_missing_external(run_deps_map):
    """Walk the run_deps_map and collect all external deps where exists=False.

    Returns deduplicated list of {"path": str, "referenced_by": [str, ...]} dicts.
    """
    # path -> set of referrers
    missing_map = {}

    def _walk(entries, parent_nb):
        for entry in entries:
            if entry.get("location") == "external" and entry.get("exists") is False:
                p = entry["path"]
                if p not in missing_map:
                    missing_map[p] = set()
                missing_map[p].add(parent_nb)
            _walk(entry.get("nested_deps", []), entry.get("path", parent_nb))

    for nb, entries in run_deps_map.items():
        _walk(entries, nb)
    return [{"path": p, "referenced_by": sorted(refs)} for p, refs in missing_map.items()]


async def main():
    """DEPRECATED CLI ENTRY POINT — use `scripts/build_dag_from_workflow.py` instead.

    This directory-scan builder walks a workspace directory to reconstruct a
    %run dependency graph. The workflow-driven counterpart
    (build_dag_from_workflow.py) fetches the same DAG straight from the AIDP
    workflow definition, including task parameters and per-task cluster IDs
    — none of which this legacy CLI can see.

    Retained as a library helper (the underlying builder is still imported
    from elsewhere) and for one-off ad-hoc DAG rebuilds. Do NOT invoke this
    from new automation.
    """
    from aidp_executor import DEFAULT_LAKE_OCID, DEFAULT_WORKSPACE_ID, DEFAULT_OCI_PROFILE
    parser = argparse.ArgumentParser(description="Build DAG from notebook dependencies")
    parser.add_argument("--root", required=True, help="Workspace root path (e.g. Users/user1@example.com/Payments/Example_Optimizer_Realtime)")
    parser.add_argument("--entry", help="Entry point notebook(s), comma-separated. If omitted, includes all.")
    parser.add_argument("--job-name", help="Job name for manifest entry")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER, required=DEFAULT_CLUSTER is None,
                        help="AIDP cluster ID (required)")
    parser.add_argument("--graph-only", action="store_true", help="Just print the dependency graph")
    parser.add_argument("--output", help="Output file for manifest entry (default: stdout)")
    parser.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE",
        help="Parameter binding used to resolve widget-based notebook.run paths. "
             "Repeat for multiple values.",
    )
    # AIDP environment — override these for other-customer deployments
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID, required=DEFAULT_LAKE_OCID is None,
                        help="AIDP data lake OCID (required)")
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID, required=DEFAULT_WORKSPACE_ID is None,
                        help="AIDP workspace UUID (required)")
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE,
                        help="OCI config profile name in ~/.oci/config (default: %(default)s)")
    args = parser.parse_args()

    parameter_bindings = {}
    for raw in args.param:
        key, separator, value = raw.partition("=")
        if not separator or not key.strip():
            parser.error(f"invalid --param {raw!r}; expected KEY=VALUE")
        parameter_bindings[key.strip()] = value

    # Strip leading /Workspace/ if user provided it
    if args.root.startswith("/Workspace/"):
        args.root = args.root[len("/Workspace/"):]

    # If root is a single .ipynb file, adjust root to parent directory
    _single_notebook = None
    if args.root.endswith(".ipynb"):
        _single_notebook = args.root
        args.root = os.path.dirname(args.root)

    session = AIDPSession(lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
                          cluster_id=args.cluster, oci_profile=args.oci_profile,
                          session_name=f"dag_{args.job_name or 'build'}")
    await session.connect()

    if _single_notebook:
        print(f"Single notebook: /Workspace/{_single_notebook}")
        notebooks = [os.path.basename(_single_notebook)]
    else:
        print(f"Discovering notebooks in /Workspace/{args.root}/ ...")
        notebooks = await discover_notebooks(session, args.root)
    print(f"Found {len(notebooks)} notebooks")

    print("Extracting dependencies...")
    deps = await extract_dependencies(
        session, args.root, parameter_bindings=parameter_bindings,
    )
    print(f"Analyzed {len(deps)} notebooks")

    # Recursively resolve all %run deps (internal + external + nested)
    print("Resolving external dependencies (recursive)...")
    run_deps_map = await resolve_run_deps_recursive(
        session, args.root, deps, parameter_bindings=parameter_bindings,
    )

    await session.close()

    # Print dependency graph
    if args.graph_only or not args.job_name:
        print(f"\n{'='*60}")
        print(f"Dependency Graph: {args.root}")
        print(f"{'='*60}")

        def _print_dep_tree(entries, indent=14):
            for entry in entries:
                loc = entry.get("location", "?")
                exists = entry.get("exists")
                path = entry["path"]
                if loc == "internal":
                    marker = "+"
                elif exists is True:
                    marker = "E"  # external but exists
                elif exists is False:
                    marker = "!"  # external and MISSING
                else:
                    marker = "?"
                prefix = " " * indent
                print(f"{prefix}[{marker}] -> {path}")
                nested = entry.get("nested_deps", [])
                if nested:
                    _print_dep_tree(nested, indent + 4)

        for nb in sorted(deps.keys()):
            info = deps[nb]
            cells = info.get("code_cells", 0)
            prefix = f"[{cells} cells]"
            dep_entries = run_deps_map.get(nb, [])
            if dep_entries:
                print(f"  {prefix:12s} {nb}")
                _print_dep_tree(dep_entries)
            else:
                print(f"  {prefix:12s} {nb} (leaf)")

        # Print missing external deps summary
        missing = collect_missing_external(run_deps_map)
        if missing:
            print(f"\n{'='*60}")
            print(f"MISSING EXTERNAL NOTEBOOKS ({len(missing)})")
            print(f"{'='*60}")
            for m in missing:
                print(f"  ! {m['path']}")
                for ref in m['referenced_by']:
                    print(f"    referenced by: {ref}")

        if args.graph_only:
            return

    # Build layers
    entry_points = None
    if args.entry:
        entry_points = []
        for e in args.entry.split(","):
            e = e.strip()
            if not e.endswith(".ipynb"):
                e += ".ipynb"
            entry_points.append(e)

    layers = build_layers(deps, entry_points)

    print(f"\n{'='*60}")
    print(f"Execution Layers ({len(layers)} layers)")
    print(f"{'='*60}")
    for i, layer in enumerate(layers):
        print(f"  Layer {i}: {layer}")

    # Build manifest
    job_name = args.job_name or os.path.basename(args.root)
    entry = build_manifest_entry(
        job_name, args.root, layers, deps, run_deps_map,
        parameters=parameter_bindings,
    )

    # Print missing external deps warning
    missing = collect_missing_external(run_deps_map)
    if missing:
        print(f"\nWARNING: {len(missing)} external notebook(s) NOT FOUND on cluster:")
        for m in missing:
            print(f"  ! {m['path']}  (referenced by {m['referenced_by']})")
        print("These will cause runtime failures during migration.")

    print(f"\nJob: {job_name}")
    print(f"Tasks: {len(entry['jobs'][0]['tasks'])}")
    print(f"Layers: {len(layers)}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(entry, f, indent=2)
        print(f"Manifest written to {args.output}")
    else:
        print(f"\n{json.dumps(entry, indent=2)}")


if __name__ == "__main__":
    print("=" * 70)
    print("WARNING: build_dag.py (directory scan) is DEPRECATED as a CLI.")
    print("Use scripts/build_dag_from_workflow.py --job-key <workflow-key>")
    print("whenever an AIDP workflow exists (the normal case).")
    print("=" * 70)
    asyncio.run(main())
