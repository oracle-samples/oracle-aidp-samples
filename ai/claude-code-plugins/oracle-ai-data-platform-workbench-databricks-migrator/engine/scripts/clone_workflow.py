#!/usr/bin/env python3
"""
Clone AIDP Workflow with Migrated Notebook Paths
==================================================
Fetches an existing AIDP workflow, replaces each task's notebookPath
with the migrated path from a migration registry, and creates a new
workflow via the AIDP Job API.

Everything else (tasks, dependencies, clusters, timeouts) is preserved.
The notebook_path parameter is rewritten when it names a migrated notebook.

Usage:
    # Dry run — see the payload without creating anything
    python3 clone_workflow.py \
        --job-key 00000000-0000-4000-8000-000000000123 \
        --registry reports/behavioral_registry.json \
        --dry-run

    # Create the cloned workflow
    python3 clone_workflow.py \
        --job-key 00000000-0000-4000-8000-000000000123 \
        --registry reports/behavioral_registry.json
"""

import argparse
import copy
import json
import os
import re
import sys
import urllib.parse
from datetime import datetime

import requests as http_requests

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)                     # scripts/ — for aidp_executor
sys.path.insert(0, os.path.dirname(_HERE))    # repo root — for aidp_config
from aidp_executor import get_oci_signer, REGION_MAP
from dbx_export.sanitizer import sanitize_path

# ─── Defaults ─────────────────────────────────────────────────────────
# CLI args default to None so the customer YAML (via --config) is the primary
# source of truth. Pass --lake-ocid / --workspace-id / --oci-profile only to
# override the YAML value for this one run.

DEFAULT_LAKE_OCID = None
DEFAULT_WORKSPACE_ID = None
DEFAULT_OCI_PROFILE = None


def tprint(*args, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


def _get_region(lake_ocid):
    parts = lake_ocid.split(".")
    region_code = parts[3] if len(parts) > 3 else "bom"
    return REGION_MAP.get(region_code, "ap-mumbai-1")


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
    workspace_prefixed = f"/Workspace/{stripped_ws}" if stripped_ws else ""
    workspace_relative = workspace_prefixed.lstrip("/")

    candidates = []
    for c in (raw, workspace_prefixed, workspace_relative, stripped_ws, stripped_user, key):
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

    # Fallback: last 2 path segments (e.g. Example_Rewards/notebook.ipynb)
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
        if (norm_path.endswith("/" + rk) or norm_path == "/" + rk or
                ("/Workspace" + norm_path).endswith("/" + rk) or
                ("/Workspace" + norm_path) == "/" + rk):
            return entry["migrated_path"]

    return None


def _workflow_export_roots(job_data):
    """Find the known export roots from source task notebook paths."""
    roots = set()
    for task in job_data.get("tasks", []):
        if task.get("type") != "NOTEBOOK_TASK":
            continue
        path = task.get("notebookPath", "")
        match = re.match(r"^(.*/notebooks/)(?=(?:Users|Repos|Shared|GIT)/)", path)
        if match:
            roots.add(match.group(1))
    return sorted(roots, key=len, reverse=True)


def _original_path_aliases(path, export_roots):
    """Include the raw workspace path represented by a known export path."""
    aliases = [path]
    for root in export_roots:
        if path.startswith(root):
            aliases.append("/Workspace/" + path[len(root):])
    return aliases


def _git_path_aliases(aliases):
    """Add the exporter's GIT/ namespace for raw source references."""
    variants = []
    # Git-backed exports insert GIT/ between the export root and original
    # workspace path. A source parameter can still name the raw path.
    for alias in aliases:
        relative = alias.removeprefix("/Workspace/").lstrip("/")
        if relative.startswith("GIT/"):
            variants.append("/Workspace/" + relative[len("GIT/"):])
        elif relative.startswith(("Users/", "Repos/", "Shared/")):
            variants.append("/Workspace/GIT/" + relative)
    return list(dict.fromkeys(variants))


def _notebook_match_key(path):
    """Use the exporter's filename contract for full-path registry matching."""
    relative = path.replace("\\", "/").rstrip("/")
    relative = relative.removeprefix("/Workspace/").removeprefix("Workspace/").lstrip("/")
    for extension in (".ipynb", ".py", ".scala", ".sql", ".r"):
        if relative.lower().endswith(extension):
            relative = relative[:-len(extension)]
            break
    return sanitize_path(relative) + ".ipynb"


def rewrite_notebook_path_parameters(payload, registry, source_workflow=None):
    """Replace exported child references in cloned workflow parameters.

    Registry lookup for task notebookPath permits suffix fallbacks. Parameter
    values need stricter matching so one same-named child cannot be selected
    for another; only a full workspace path match is accepted here.
    """
    export_roots = _workflow_export_roots(source_workflow or {})
    migrated_values = {
        entry["migrated_path"] for entry in registry.values()
        if isinstance(entry, dict) and entry.get("migrated_path")
    }

    def rewrite(parameters, scope):
        if not isinstance(parameters, list):
            return
        for parameter in parameters:
            if (not isinstance(parameter, dict)
                    or parameter.get("name") != "notebook_path"):
                continue
            value = parameter.get("value")
            if not isinstance(value, str) or not value:
                continue
            if value in migrated_values:
                continue
            aliases = _original_path_aliases(value, export_roots)

            def matches_for(candidates):
                keys = {_notebook_match_key(alias) for alias in candidates}
                return {
                    entry["migrated_path"] for source, entry in registry.items()
                    if isinstance(source, str) and isinstance(entry, dict)
                    and entry.get("migrated_path")
                    and _notebook_match_key(source) in keys
                }

            primary_matches = matches_for(aliases)
            git_matches = matches_for(_git_path_aliases(aliases))
            explicit_namespace = (
                any(value.startswith(root) for root in export_roots)
                or value.startswith(("/Workspace/GIT/", "Workspace/GIT/",
                                     "/GIT/", "GIT/"))
            )
            # A named export/GIT namespace must match that namespace. Only a
            # raw workspace reference may consider both export layouts.
            matches = primary_matches if explicit_namespace else (
                primary_matches | git_matches
            )
            if len(matches) > 1:
                raise ValueError(
                    f"Ambiguous migrated child for {scope} notebook_path={value!r}"
                )
            if matches:
                migrated = next(iter(matches))
                parameter["value"] = migrated
                tprint(f"  {scope} notebook_path parameter: {value} -> {migrated}")
            elif value.startswith((
                "/Workspace/", "Workspace/", "/Users/", "Users/",
                "/Repos/", "Repos/", "/Shared/", "Shared/", "/GIT/", "GIT/",
            )):
                raise ValueError(
                    f"No migrated child for {scope} notebook_path={value!r}"
                )

    rewrite(payload.get("parameters"), "job")
    for task in payload.get("tasks", []):
        if task.get("type") == "NOTEBOOK_TASK":
            rewrite(task.get("parameters"), f"task:{task.get('taskKey', '?')}")


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
    export_roots = _workflow_export_roots(job_data)

    # Deep-copy tasks, swap notebookPath to the migrated location.
    cloned_tasks = []
    for task in job_data.get("tasks", []):
        if task.get("type") != "NOTEBOOK_TASK":
            cloned_tasks.append(task)
            continue

        original_path = task.get("notebookPath", "")
        aliases = _original_path_aliases(original_path, export_roots)
        # Prefer exact full-path registry hits over resolve_migrated_path's
        # legacy basename fallback, which can select a peer notebook.
        exact_matches = set()
        for alias in aliases:
            candidates = (alias, alias.removeprefix("/Workspace/"))
            for candidate in candidates:
                for variant in (candidate, candidate[:-6] if candidate.endswith(".ipynb")
                                else candidate + ".ipynb"):
                    entry = registry.get(variant)
                    if entry and entry.get("migrated_path"):
                        exact_matches.add(entry["migrated_path"])
        if len(exact_matches) > 1:
            raise ValueError(f"Ambiguous migrated task notebook {original_path!r}")
        migrated = next(iter(exact_matches)) if exact_matches else None
        if not migrated:
            migrated = resolve_migrated_path(original_path, registry)
        if migrated:
            # Collapse accidental // (an MIGRATE_OUTPUT trailing-slash artifact,
            # e.g. /Workspace/ai_dbx_migrated//ai_tool_...): AIDP parses the path
            # into nodes and rejects the empty segment as "empty node name @27"
            # (27 = the character position of the second slash).
            migrated = re.sub(r"(?<!:)/{2,}", "/", migrated)
            resolved.append((task["taskKey"], original_path, migrated))
        else:
            unresolved.append((task["taskKey"], original_path))

        cloned_task = copy.deepcopy(task)
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
    # Deep-copy job parameters so rewriting the clone never mutates its source.
    if job_data.get("parameters"):
        payload["parameters"] = copy.deepcopy(job_data["parameters"])

    rewrite_notebook_path_parameters(payload, registry, job_data)

    return payload, resolved, unresolved


def parse_parameter_overrides(raw_values):
    """Parse repeatable CLI key=value strings into a validated dictionary."""
    overrides = {}
    for raw in raw_values or []:
        if "=" not in raw:
            raise ValueError(f"Invalid --param {raw!r}; expected key=value")
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --param {raw!r}; parameter name is empty")
        overrides[key] = value
    return overrides


def apply_parameter_overrides(payload, overrides):
    """Apply runbook overrides as job parameters and matching task parameters.

    AIDP task parameters are lists of ``{"name": ..., "value": ...}``
    objects. The source workflow can declare the names with empty values;
    copying it verbatim therefore loses the concrete values used during
    migration. Every runbook override is added to the cloned workflow's
    job-level parameters so it is visible in the workflow UI and available to
    every task. Matching task declarations are also populated for compatibility
    with notebook-task parameter injection.

    Returns ``(applied, unmatched)`` where applied contains
    ``(scope, name, value)`` tuples for audit logging.
    """
    if not overrides:
        return [], []

    applied = []
    matched_names = set()

    def apply_list(parameters, scope):
        if not isinstance(parameters, list):
            return
        for parameter in parameters:
            if not isinstance(parameter, dict):
                continue
            name = parameter.get("name")
            if name not in overrides:
                continue
            value = str(overrides[name])
            parameter["value"] = value
            matched_names.add(name)
            applied.append((scope, name, value))

    job_parameters = payload.setdefault("parameters", [])
    if not isinstance(job_parameters, list):
        job_parameters = []
        payload["parameters"] = job_parameters
    job_by_name = {
        parameter.get("name"): parameter
        for parameter in job_parameters
        if isinstance(parameter, dict) and parameter.get("name")
    }
    for name, raw_value in overrides.items():
        value = str(raw_value)
        parameter = job_by_name.get(name)
        if parameter is None:
            parameter = {"name": name, "value": value}
            job_parameters.append(parameter)
            job_by_name[name] = parameter
        else:
            parameter["value"] = value
        matched_names.add(name)
        applied.append(("job", name, value))

    for task in payload.get("tasks", []):
        apply_list(task.get("parameters"), f"task:{task.get('taskKey', '?')}")

    unmatched = sorted(set(overrides) - matched_names)
    return applied, unmatched


def build_clone_payload(job_data, registry, overrides):
    """Apply CLI overrides before validating the final cloned parameters."""
    source_for_clone = copy.deepcopy(job_data)
    applied, unmatched = apply_parameter_overrides(source_for_clone, overrides)
    payload, resolved, unresolved = clone_workflow(source_for_clone, registry)
    return payload, resolved, unresolved, applied, unmatched


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
    # AIDP env — default None so YAML (via --config) is primary source.
    parser.add_argument("--config", default=None,
                        help="Path to aidp_migrator.yaml. Falls back to $AIDP_CONFIG_PATH, "
                             "./aidp_migrator.yaml, ~/aidp_migrator.yaml (discovery order).")
    parser.add_argument("--lake-ocid", default=None,
                        help="Override cfg.aidp.lake_ocid for this run only.")
    parser.add_argument("--workspace-id", default=None,
                        help="Override cfg.aidp.workspace_id for this run only.")
    parser.add_argument("--oci-profile", default=None,
                        help="Override cfg.aidp.oci_profile for this run only.")
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Override a matching job/task parameter value in the clone. "
            "Repeat for multiple parameters."
        ),
    )
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

    # Step 1: Fetch existing workflow
    tprint(f"Fetching workflow: {args.job_key}")
    job_data = fetch_job(args.workspace_id, args.job_key,
                         args.lake_ocid, args.oci_profile)
    tprint(f"Workflow: {job_data.get('name')} — {len(job_data.get('tasks', []))} tasks")

    # Step 2: Load registry
    registry = load_registry(args.registry)

    try:
        parameter_overrides = parse_parameter_overrides(args.param)
    except ValueError as exc:
        parser.error(str(exc))
    # Step 3: Build cloned payload with migrated paths. Validate only after
    # overrides have replaced stale source parameter values.
    (payload, resolved, unresolved,
     applied_parameters, unmatched_parameters) = build_clone_payload(
        job_data, registry, parameter_overrides,
    )

    # Print mapping summary
    tprint(f"\nPath mappings ({len(resolved)} resolved, {len(unresolved)} unresolved):")
    for task_key, orig, mig in resolved:
        print(f"  OK  {task_key}")
        print(f"      {orig}")
        print(f"   -> {mig}")
    for task_key, orig in unresolved:
        print(f"  MISSING  {task_key}")
        print(f"           {orig}")

    if parameter_overrides:
        tprint(f"\nParameter overrides ({len(applied_parameters)} applied):")
        for scope, name, value in applied_parameters:
            print(f"  OK  {scope}  {name}={value}")
        for name in unmatched_parameters:
            print(
                f"  SKIP  {name}={parameter_overrides[name]} "
                "(not declared by the source workflow)"
            )

    if unresolved:
        print(f"\nERROR: {len(unresolved)} task(s) have no migrated path in registry. Cannot clone.")
        sys.exit(1)

    # Step 4: Dry run or create
    if args.dry_run:
        tprint("\n--dry-run: payload that would be sent:\n")
        print(json.dumps(payload, indent=2))
        return

    # Step 5: Create OR update. If a workflow with this name already exists
    # (from a prior run of this script), PUT the new payload onto the existing
    # key rather than POSTing a fresh one — POST would 409 on the duplicate
    # name (AIDP enforces name uniqueness within a workspace).
    _, _signer = get_oci_signer(args.oci_profile)
    _base = _api_base(args.lake_ocid)
    _list_url = f"{_base}/workspaces/{args.workspace_id}/jobs"
    _existing_key = None
    _next = None
    while True:
        _u = _list_url if not _next else f"{_list_url}?page={urllib.parse.quote(_next)}"
        _r = http_requests.get(_u, auth=_signer)
        if _r.status_code != 200:
            break
        for _j in (_r.json().get("items") or []):
            if (_j.get("name") or "") == payload["name"]:
                _existing_key = _j.get("key")
                break
        if _existing_key:
            break
        _next = _r.headers.get("opc-next-page")
        if not _next:
            break

    if _existing_key:
        tprint(f"Workflow '{payload['name']}' already exists (key={_existing_key}) — updating in place")
        new_key = _existing_key
    else:
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

    # Step 6: PUT to add/replace tasks
    tprint(f"{'Updating' if _existing_key else 'Adding'} {len(payload['tasks'])} tasks to {new_key}")
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
