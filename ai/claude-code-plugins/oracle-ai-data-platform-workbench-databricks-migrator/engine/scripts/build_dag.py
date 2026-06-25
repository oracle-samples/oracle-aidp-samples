#!/usr/bin/env python3
"""
Build DAG from notebook %run dependencies.
=============================================
Given a root notebook or directory on AIDP, discovers all %run and
dbutils.notebook.run dependencies, builds a DAG, and outputs a
job manifest entry that job_migrate.py can consume.

Recursively follows external %run targets (notebooks outside --root)
to discover the full dependency tree, including nested deps.

Usage:
    python3 build_dag.py --root "Users/user@example.com/ExampleProject/ExampleJob" \
        --entry "<entry_notebook>.ipynb" \
        --job-name "ExampleJob" \
        --cluster <CLUSTER_ID>

    # Just print the dependency graph
    python3 build_dag.py --root "Users/user@example.com/ExampleProject/ExampleJob" --graph-only
"""

import argparse
import asyncio
import json
import os
import posixpath
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession
from context_tools import _unwrap_aidp_text

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CLUSTER = None  # generic — required via --cluster (no hardcoded default)

MAX_RECURSION_DEPTH = 10


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


async def extract_dependencies(session, workspace_root):
    """Extract %run and notebook.run dependencies from all notebooks."""
    result = await run(session,f"""
import json, os, re

base = "/Workspace/{workspace_root}"
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
            runs = []
            cell_count = 0
            code_cells = 0
            _default_lang = (nb.get("metadata", {{}}).get("language_info", {{}}).get("name", "") or "").lower()
            for cell in nb.get("cells", []):
                cell_count += 1
                src = "".join(cell.get("source", []))
                if cell.get("cell_type") == "code" and src.strip():
                    code_cells += 1

                # %run (only uncommented lines) — language-aware comment strip
                # (Python/R '#', Scala/Java '//', SQL '--'); notebook default + %magic.
                _first = next((l.strip() for l in src.splitlines() if l.strip()), "")
                _mm = re.match(r'^(?:#\\s*MAGIC\\s+)?%(\\w+)', _first)
                _clang = (_mm.group(1).lower() if _mm else _default_lang)
                _cmt = {{"python": ("#",), "py": ("#",), "r": ("#",), "scala": ("//",), "java": ("//",), "sql": ("--",)}}.get(_clang, ("#", "//", "--"))
                _active_src = "\\n".join(l for l in src.splitlines() if l.strip() and not l.strip().startswith(_cmt))
                for m in re.findall(r'%run\\s+([^\\n#]+)', _active_src):
                    target = m.strip()
                    if target.startswith('"') or target.startswith("'"):
                        q = target[0]
                        end = target.find(q, 1)
                        target = target[1:end] if end > 0 else target[1:].rstrip(q)
                    else:
                        target = target.split()[0]
                        target = re.sub(r'\\$\\w+', '', target).strip()
                    if target.startswith("./"):
                        d = os.path.dirname(rel)
                        target = os.path.join(d, target[2:]) if d else target[2:]
                    elif target.startswith("/Workspace/{workspace_root}/"):
                        target = target[len("/Workspace/{workspace_root}/"):]
                    elif target.startswith("/Workspace/{workspace_root}"):
                        target = target[len("/Workspace/{workspace_root}"):]
                        if target.startswith("/"):
                            target = target[1:]
                    target = target.strip()
                    if not target.endswith(".ipynb"):
                        target += ".ipynb"
                    runs.append(target)

                # dbutils.notebook.run (only uncommented lines)
                for m in re.findall(r'notebook\\.run\\s*\\(\\s*["\\'](.*?)["\\'\\)]', _active_src):
                    target = m.strip()
                    if target.startswith("/Workspace/{workspace_root}/"):
                        target = target[len("/Workspace/{workspace_root}/"):]
                    if not target.endswith(".ipynb"):
                        target += ".ipynb"
                    runs.append(target)

            deps[rel] = {{"runs": runs, "cells": cell_count, "code_cells": code_cells}}
        except Exception as e:
            deps[rel] = {{"runs": [], "cells": 0, "code_cells": 0, "error": str(e)[:100]}}

print(json.dumps(deps))
""", timeout=120)
    output = unwrap(result.get("outputs", []))
    try:
        return json.loads(output)
    except:
        print(f"ERROR extracting deps: {output[:500]}")
        return {}


def _normalize_ws_path(path):
    """Normalize workspace paths: ensure /Workspace/ prefix, .ipynb suffix, resolve '..' segments."""
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


async def scan_external_notebook(session, notebook_path):
    """Read a single notebook on the cluster and extract its %run / notebook.run targets.

    Returns (exists: bool, runs: list[str]) where runs are raw target paths.
    """
    full_path = _normalize_ws_path(notebook_path)

    result = await run(session,f"""
import json, os, re

path = {json.dumps(full_path)}
if not os.path.exists(path):
    print(json.dumps({{"exists": False, "runs": []}}))
else:
    try:
        with open(path) as fh:
            nb = json.load(fh)
        runs = []
        _default_lang = (nb.get("metadata", {{}}).get("language_info", {{}}).get("name", "") or "").lower()
        for cell in nb.get("cells", []):
            src = "".join(cell.get("source", []))
            # Language-aware comment strip: Python/R '#', Scala/Java '//', SQL '--'.
            # Use the notebook default + any per-cell %magic to pick the right marker.
            _first = next((l.strip() for l in src.splitlines() if l.strip()), "")
            _mm = re.match(r'^(?:#\\s*MAGIC\\s+)?%(\\w+)', _first)
            _clang = (_mm.group(1).lower() if _mm else _default_lang)
            _cmt = {{"python": ("#",), "py": ("#",), "r": ("#",), "scala": ("//",), "java": ("//",), "sql": ("--",)}}.get(_clang, ("#", "//", "--"))
            _active_src = "\\n".join(l for l in src.splitlines() if l.strip() and not l.strip().startswith(_cmt))
            # %run
            for m in re.findall(r'%run\\s+([^\\n#]+)', _active_src):
                target = m.strip()
                if target.startswith('"') or target.startswith("'"):
                    q = target[0]
                    end = target.find(q, 1)
                    target = target[1:end] if end > 0 else target[1:].rstrip(q)
                else:
                    target = target.split()[0]
                    target = re.sub(r'\\$\\w+', '', target).strip()
                if not target.endswith(".ipynb"):
                    target += ".ipynb"
                runs.append(target)
            # dbutils.notebook.run
            for m in re.findall(r'notebook\\.run\\s*\\(\\s*["\\'](.*?)["\\']', _active_src):
                target = m.strip()
                if not target.endswith(".ipynb"):
                    target += ".ipynb"
                runs.append(target)
            # oidlUtils.notebook.run
            for m in re.findall(r'oidlUtils\\.notebook\\.run\\s*\\(\\s*["\\'](.*?)["\\']', _active_src):
                target = m.strip()
                if not target.endswith(".ipynb"):
                    target += ".ipynb"
                runs.append(target)
        # Resolve relative paths against parent notebook's directory
        parent_dir = os.path.dirname(path)
        resolved = []
        for r in runs:
            if not r.startswith("/"):
                r = os.path.normpath(os.path.join(parent_dir, r))
            resolved.append(r)
        print(json.dumps({{"exists": True, "runs": list(set(resolved))}}))
    except Exception as e:
        print(json.dumps({{"exists": True, "runs": [], "error": str(e)[:100]}}))
""", timeout=60)
    output = unwrap(result.get("outputs", []))
    try:
        data = json.loads(output)
        return data.get("exists", False), data.get("runs", [])
    except Exception as e:
        # Retry once — transient cluster issues can return bad output
        print(f"  WARNING: scan attempt 1 failed for {notebook_path}: {str(e)[:100]} output={output[:200]}")
        try:
            result2 = await run(session, f"""
import json, os
path = {json.dumps(full_path)}
print(json.dumps({{"exists": os.path.exists(path), "runs": []}}))
""", timeout=60)
            output2 = unwrap(result2.get("outputs", []))
            data2 = json.loads(output2)
            return data2.get("exists", False), data2.get("runs", [])
        except Exception as e2:
            print(f"  WARNING: scan attempt 2 failed for {notebook_path}: {str(e2)[:100]}")
            return False, []


async def resolve_run_deps_recursive(session, workspace_root, deps):
    """Recursively discover all %run dependencies, including external notebooks.

    For each notebook in deps, classifies its %run targets as internal (under --root)
    or external. External targets are scanned on the cluster to discover their own
    nested deps, recursively up to MAX_RECURSION_DEPTH.

    Returns {notebook_rel_path: [run_dep_entry, ...]} where each run_dep_entry is:
        {"path": str, "location": "internal"|"external", "exists": bool|None,
         "nested_deps": [run_dep_entry, ...]}
    """
    all_internal = set(deps.keys())
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
        exists, child_runs = await scan_external_notebook(session, path)

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
                alt_exists, alt_runs = await scan_external_notebook(session, alt_path)
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
    from aidp_executor import DEFAULT_LAKE_OCID, DEFAULT_WORKSPACE_ID, DEFAULT_OCI_PROFILE
    parser = argparse.ArgumentParser(description="Build DAG from notebook dependencies")
    parser.add_argument("--root", required=True, help="Workspace root path (e.g. Users/user@example.com/ExampleProject/ExampleJob)")
    parser.add_argument("--entry", help="Entry point notebook(s), comma-separated. If omitted, includes all.")
    parser.add_argument("--job-name", help="Job name for manifest entry")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER, required=DEFAULT_CLUSTER is None,
                        help="AIDP cluster ID (required)")
    parser.add_argument("--graph-only", action="store_true", help="Just print the dependency graph")
    parser.add_argument("--output", help="Output file for manifest entry (default: stdout)")
    # AIDP environment — override these for deployments using different defaults
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID, required=DEFAULT_LAKE_OCID is None,
                        help="AIDP data lake OCID (required)")
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID, required=DEFAULT_WORKSPACE_ID is None,
                        help="AIDP workspace UUID (required)")
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE,
                        help="OCI config profile name in ~/.oci/config (default: %(default)s)")
    args = parser.parse_args()

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
    deps = await extract_dependencies(session, args.root)
    print(f"Analyzed {len(deps)} notebooks")

    # Recursively resolve all %run deps (internal + external + nested)
    print("Resolving external dependencies (recursive)...")
    run_deps_map = await resolve_run_deps_recursive(session, args.root, deps)

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
    entry = build_manifest_entry(job_name, args.root, layers, deps, run_deps_map)

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
    asyncio.run(main())
