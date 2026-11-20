#!/usr/bin/env python3
"""
Check Data Availability for a Workflow
=========================================
Scans notebooks in an AIDP workflow for READ data dependencies (storage paths
and table references), maps S3/DBFS paths to OCI equivalents, and verifies
availability on the AIDP cluster.

Produces a per-notebook data readiness report to help decide whether to
proceed with migration.

Usage:
    # Check data for a workflow job
    python3 check_data_availability_for_workflow.py \\
        --job-key <JOB-KEY> \\
        --cluster <CLUSTER-ID>

    # From a pre-built manifest
    python3 check_data_availability_for_workflow.py \\
        --manifest reports/example_workflow_manifest_1.json \\
        --cluster <CLUSTER-ID>

    # Check only specific tasks
    python3 check_data_availability_for_workflow.py \\
        --job-key <JOB-KEY> --cluster <CLUSTER-ID> \\
        --only-tasks "02_BaseData_Payments,05_Payments_Feature"
"""

import argparse
import asyncio
import csv
import hashlib
import io
import json
import os
import posixpath
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aidp_executor import AIDPSession, get_oci_signer, get_region_from_ocid
from build_dag_from_workflow import (
    fetch_job_definition,
    extract_tasks_from_job,
)
from data_check_extract import extract_from_notebook
from data_check_imports import ImportedModuleSymbols, repository_notebook_dir

import requests as _http_requests  # local notebook fetch via AIDP downloadFileMeta

# ─── Defaults ───────────────────────────────────────────────────────

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# CLI args default to None so the customer YAML (via --config) is the primary
# source of truth. Pass --lake-ocid / --workspace-id / --oci-profile only to
# override the YAML value for this one run.
DEFAULT_LAKE_OCID = None
DEFAULT_WORKSPACE_ID = None
DEFAULT_OCI_PROFILE = None
_ACTIVE_CONTROL_SESSION = None
OCI_PATH_PROBE_BATCH_SIZE = 5
OCI_PATH_PROBE_TIMEOUT_SECONDS = 180
PROBE_REPLACEMENT_SESSION_BUDGET = 4

# ─── Bucket → Namespace Mapping ────────────────────────────────────

_BUCKET_NS: dict = {}


def _set_bucket_ns_mapping(rows, source: str) -> None:
    """Replace the bucket index from parsed CSV rows."""
    global _BUCKET_NS
    mapping = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        oci_bucket = (row.get("oci_bucket") or "").strip()
        namespace = (row.get("oci_namespace") or "").strip()
        if oci_bucket and namespace:
            mapping[oci_bucket] = namespace
    try:
        from context_tools import sanitize_bucket_index
        mapping, dropped = sanitize_bucket_index(mapping)
        if dropped:
            print(
                f"[data-check] dropped {len(dropped)} phantom -oci "
                f"twin(s): {dropped}"
            )
    except Exception as exc:  # pragma: no cover - defensive; never block the check
        print(f"[data-check] WARN: bucket-index sanitize skipped: {exc}")
    _BUCKET_NS = mapping
    print(
        f"[data-check] Loaded {len(_BUCKET_NS)} bucket-to-namespace "
        f"mappings from {source}"
    )


def load_bucket_ns_mapping_text(content: str, source: str = "workspace CSV") -> None:
    """Load bucket mappings from CSV text downloaded from AIDP workspace."""
    _set_bucket_ns_mapping(csv.DictReader(io.StringIO(content)), source)


def load_bucket_ns_mapping(mapping_path: str = None):
    """Load {oci_bucket_name: namespace} from the bucket-mapping CSV.

    CSV columns: s3_bucket, oci_bucket, oci_namespace. Same file the main
    migration reads via cfg.bucket_mapping.path (context_tools.load_bucket_mapping).
    """
    if mapping_path is None:
        mapping_path = os.path.join(PROJECT_DIR, "config", "bucket_map.csv")
    if not os.path.exists(mapping_path):
        print(f"WARNING: {mapping_path} not found — S3→OCI mapping unavailable")
        return
    with open(mapping_path, newline="", encoding="utf-8") as handle:
        _set_bucket_ns_mapping(csv.DictReader(handle), mapping_path)


def map_to_oci_path(source_path: str) -> dict:
    """Map an S3/DBFS/mnt path to OCI path using bucket→namespace index.

    Returns: {
        "source": original path,
        "oci_path": mapped oci:// path or None,
        "bucket": extracted bucket name,
        "namespace": resolved namespace or None,
    }
    """
    bucket = None
    sub_path = ""

    if source_path.startswith("s3://") or source_path.startswith("s3a://"):
        m = re.match(r's3a?://([^/]+)(/.*)?', source_path)
        if m:
            bucket = m.group(1)
            sub_path = m.group(2) or "/"
    elif source_path.startswith("dbfs:/"):
        m = re.match(r'dbfs:/(?:mnt/)?([^/]+)(/.*)?', source_path)
        if m:
            bucket = m.group(1)
            sub_path = m.group(2) or "/"
    elif source_path.startswith("/mnt/"):
        m = re.match(r'/mnt/([^/]+)(/.*)?', source_path)
        if m:
            bucket = m.group(1)
            sub_path = m.group(2) or "/"

    if not bucket:
        return {"source": source_path, "oci_path": None, "bucket": None, "namespace": None}

    # Direct lookup in bucket→namespace mapping
    ns = _BUCKET_NS.get(bucket)
    if not ns:
        # Try with oci- prefix
        ns = _BUCKET_NS.get(f"oci-{bucket}")
        if ns:
            bucket = f"oci-{bucket}"
    if not ns:
        # Try stripping oci- prefix
        if bucket.startswith("oci-"):
            ns = _BUCKET_NS.get(bucket[4:])
            if ns:
                bucket = bucket[4:]

    oci_path = f"oci://{bucket}@{ns}{sub_path}" if ns else None
    return {"source": source_path, "oci_path": oci_path, "bucket": bucket, "namespace": ns}


# ─── Read vs Write Classification ──────────────────────────────────

# Patterns that indicate READ operations
_READ_PATTERNS = [
    r'spark\.read\.',
    r'spark\.table\s*\(',
    r'spark\.sql\s*\(\s*["\'](?:SELECT|SHOW|DESCRIBE|WITH)',
    r'pd\.read_',
    r'pandas\.read_',
    r'open\s*\([^)]*["\']r["\']',
    r'open\s*\([^)]*\)\s*(?!.*["\']w)',
    r'dbutils\.fs\.ls\s*\(',
    r'dbutils\.fs\.head\s*\(',
    r'\.load\s*\(',
    r'FROM\s+',
    r'JOIN\s+',
    r'\.option\(.*\.load\(',
    r'sc\.textFile\s*\(',
    r'sc\.wholeTextFiles\s*\(',
]

# Patterns that indicate WRITE operations
_WRITE_PATTERNS = [
    r'\.write\.',
    r'\.save\s*\(',
    r'\.saveAsTable\s*\(',
    r'INSERT\s+(?:INTO|OVERWRITE)',
    r'CREATE\s+TABLE',
    r'dbutils\.fs\.put\s*\(',
    r'dbutils\.fs\.mkdirs\s*\(',
    r'dbutils\.fs\.cp\s*\(',
    r'dbutils\.fs\.mv\s*\(',
    r'open\s*\([^)]*["\']w["\']',
    r'\.to_csv\s*\(',
    r'\.to_parquet\s*\(',
    r'\.to_json\s*\(',
]


def classify_path_usage(source_code: str, path: str) -> str:
    """Classify whether a path is used for READ or WRITE in the given code.

    Returns 'read', 'write', or 'unknown'.
    Looks at the lines containing the path to determine context.
    """
    lines_with_path = []
    for line in source_code.splitlines():
        if path in line or (len(path) > 10 and path[:10] in line):
            lines_with_path.append(line)

    if not lines_with_path:
        return "unknown"

    context = "\n".join(lines_with_path)

    is_read = any(re.search(p, context, re.IGNORECASE) for p in _READ_PATTERNS)
    is_write = any(re.search(p, context, re.IGNORECASE) for p in _WRITE_PATTERNS)

    if is_write and not is_read:
        return "write"
    if is_read and not is_write:
        return "read"
    if is_read and is_write:
        return "read"  # if both, treat as read (conservative)
    return "unknown"


def classify_table_usage(source_code: str, table: str) -> str:
    """Classify whether a table is READ or WRITE."""
    lines_with_table = [l for l in source_code.splitlines() if table in l]
    if not lines_with_table:
        return "unknown"

    context = "\n".join(lines_with_table)

    write_patterns = [
        r'INSERT\s+(?:INTO|OVERWRITE)\s+' + re.escape(table),
        r'CREATE\s+TABLE.*' + re.escape(table),
        r'\.saveAsTable\s*\(\s*["\']' + re.escape(table),
        r'\.write\.',
    ]
    read_patterns = [
        r'FROM\s+' + re.escape(table),
        r'JOIN\s+' + re.escape(table),
        r'spark\.table\s*\(\s*["\']' + re.escape(table),
        r'spark\.read\.table\s*\(\s*["\']' + re.escape(table),
        r'SELECT.*FROM.*' + re.escape(table),
    ]

    is_write = any(re.search(p, context, re.IGNORECASE) for p in write_patterns)
    is_read = any(re.search(p, context, re.IGNORECASE) for p in read_patterns)

    if is_write and not is_read:
        return "write"
    if is_read:
        return "read"
    return "unknown"


# ─── AIDP Output Helpers ────────────────────────────────────────────

def _unwrap(raw: str) -> str:
    """Unwrap AIDP JSON output wrapper.

    AIDP wraps display output as [{"type": ..., "value": ...}]; an empty
    wrapper [] carries no value and must be dropped, otherwise it gets
    concatenated ahead of real stdout (e.g. "[]{...json...}") and breaks the
    json.loads() at the call sites.
    """
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            if not parsed:
                return ""  # empty wrapper — no value
            if "value" in parsed[0]:
                return parsed[0]["value"]
    except Exception:
        pass
    return raw


def unwrap_outputs(outputs: list) -> str:
    """Extract text from AIDP session outputs."""
    text = ""
    seen = set()
    for o in outputs:
        raw = ""
        if o.get("type") == "stream":
            raw = o.get("text", "")
        elif o.get("type") == "execute_result":
            raw = o.get("data", {}).get("text/plain", "")
        elif o.get("type") == "error":
            ename = str(o.get("ename") or "").strip()
            evalue = str(o.get("evalue") or "").strip()
            raw = ": ".join(value for value in (ename, evalue) if value)
        val = _unwrap(raw)
        if val and val not in seen:
            text += val
            seen.add(val)
    return text


def parse_cluster_json(output: str) -> dict:
    """Parse the result object from concatenated cluster stdout.

    Every cluster block here ends with exactly one `print(json.dumps(<dict>))`,
    but Spark job-progress events (JSON arrays like [{"stageStatus":"running"}])
    and empty [] display wrappers get interleaved into the stream ahead of it.
    Since every real result is a JSON OBJECT ({...}) and the noise is arrays,
    scan for the LAST top-level {...} span (string-aware brace matching; objects
    nested inside arrays are ignored) and parse that. Raises ValueError if none.
    """
    try:
        v = json.loads(output)
        if isinstance(v, dict):
            return v
    except Exception:
        pass

    spans = []
    depth = 0
    obj_start = None
    in_str = False
    esc = False
    quote = ""
    for i, ch in enumerate(output):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            continue
        if ch in ('"', "'"):
            in_str = True
            quote = ch
        elif ch in "{[":
            if ch == "{" and depth == 0:
                obj_start = i
            depth += 1
        elif ch in "}]":
            if depth > 0:
                depth -= 1
                if depth == 0 and ch == "}" and obj_start is not None:
                    spans.append((obj_start, i + 1))
                    obj_start = None
    for s, e in reversed(spans):
        try:
            return json.loads(output[s:e])
        except Exception:
            continue
    raise ValueError("no top-level JSON object found in cluster output")


# A "structural" filter term constrains shape, not a transferable data
# window: NULL guards (``col IS [NOT] NULL``) and column-to-column
# comparisons (``created_at <= updated_at``). A filter made ONLY of such
# terms — including AND-conjunctions rendered as a single string — must
# not become a PARTIAL migration scope or an on-cluster scope probe.
_STRUCTURAL_TERM = (
    r"\(?\s*[A-Za-z_][A-Za-z0-9_\.]*\s+IS\s+(?:NOT\s+)?NULL\s*\)?"
    r"|\(?\s*[A-Za-z_][A-Za-z0-9_\.]*\s*(?:==|=|!=|<>|<=|>=|<|>)\s*"
    r"[A-Za-z_][A-Za-z0-9_\.]*\s*\)?"
)
_STRUCTURAL_ONLY_RE = re.compile(
    r"\s*\(?\s*(?:" + _STRUCTURAL_TERM + r")"
    r"(?:\s+AND\s+(?:" + _STRUCTURAL_TERM + r"))*\s*\)?\s*",
    re.IGNORECASE,
)


def require_cluster_json(output: str, operation: str) -> dict:
    """Parse a probe result or fail without converting it to false emptiness."""
    try:
        return parse_cluster_json(output)
    except Exception as exc:
        raise RuntimeError(
            f"{operation} returned no valid JSON object. Raw cluster output "
            f"(first 500 chars): {output[:500]}"
        ) from exc


# ─── Data Reference Extraction ──────────────────────────────────────

def _download_notebook_json(signer, aidp_base: str, lake_ocid: str,
                            workspace_id: str, aidp_path: str) -> dict:
    """Download a notebook from AIDP workspace via `downloadFileMeta` + PAR
    URL and return the parsed JSON. Failures are raised so the caller records
    an explicit NOT_ANALYZED result instead of a false empty scan.

    All I/O is via the AIDP `/actions/downloadFileMeta` endpoint (same
    pattern used by `scripts/enumerate_and_download.py`) — no cluster,
    no FUSE, no `os.path.exists` on `/Workspace`.
    """
    # AIDP downloadFileMeta expects the workspace-relative path (no
    # leading /Workspace/). Strip if present.
    p = aidp_path
    if p.startswith("/Workspace/"):
        p = p[len("/Workspace/"):]
    elif p.startswith("/"):
        p = p[1:]
    if not p.endswith(".ipynb"):
        p = p + ".ipynb"

    url = f"{aidp_base}/dataLakes/{lake_ocid}/workspaces/{workspace_id}/actions/downloadFileMeta"
    headers = {"Content-Type": "application/json", "path": p, "type": "NOTEBOOK"}
    try:
        r = _http_requests.post(url, auth=signer, headers=headers, data="", timeout=30)
    except Exception as exc:
        raise RuntimeError(
            f"downloadFileMeta failed for {aidp_path}: {exc}"
        ) from exc
    if r.status_code != 200:
        raise RuntimeError(
            f"downloadFileMeta failed for {aidp_path}: HTTP {r.status_code}: "
            f"{r.text[:300]}"
        )
    par_url = (r.json() or {}).get("parUrl")
    if not par_url:
        raise RuntimeError(f"downloadFileMeta returned no PAR URL for {aidp_path}")
    try:
        r2 = _http_requests.get(par_url, timeout=60)
    except Exception as exc:
        raise RuntimeError(f"notebook download failed for {aidp_path}: {exc}") from exc
    if r2.status_code != 200:
        raise RuntimeError(
            f"notebook download failed for {aidp_path}: HTTP {r2.status_code}"
        )
    try:
        return json.loads(r2.content)
    except Exception as exc:
        raise RuntimeError(f"invalid notebook JSON for {aidp_path}: {exc}") from exc


def _match_notebook_run_target(parent_path: str, target: str,
                               notebook_paths: list) -> str:
    """Resolve a statically known notebook.run target to one manifest path."""
    target = str(target or "").strip().replace("\\", "/")
    if not target:
        return ""
    if not target.startswith("/"):
        target = posixpath.join(posixpath.dirname(parent_path), target)
    target = posixpath.normpath(target)

    def canonical(path: str) -> str:
        value = posixpath.normpath(str(path).replace("\\", "/"))
        if value.endswith(".ipynb"):
            value = value[:-6]
        return value.rstrip("/")

    wanted = canonical(target)
    exact = [path for path in notebook_paths if canonical(path) == wanted]
    if len(exact) == 1:
        return exact[0]
    suffix = wanted.removeprefix("/Workspace/").removeprefix("/")
    matches = [
        path for path in notebook_paths
        if canonical(path).removeprefix("/Workspace/").removeprefix("/").endswith(suffix)
    ]
    return matches[0] if len(matches) == 1 else ""


def _download_workspace_file(signer, aidp_base: str, lake_ocid: str,
                             workspace_id: str, aidp_path: str) -> bytes:
    """Download a regular workspace file without relying on FUSE."""
    path = aidp_path[len("/Workspace/"):] if aidp_path.startswith("/Workspace/") else aidp_path.lstrip("/")
    url = f"{aidp_base}/dataLakes/{lake_ocid}/workspaces/{workspace_id}/actions/downloadFileMeta"
    headers = {"Content-Type": "application/json", "path": path, "type": "FILE"}
    response = _http_requests.post(
        url, auth=signer, headers=headers, data="", timeout=60)
    response.raise_for_status()
    par_url = (response.json() or {}).get("parUrl")
    if not par_url:
        raise RuntimeError(f"No PAR URL returned for workspace file {aidp_path}")
    download = _http_requests.get(par_url, timeout=600)
    download.raise_for_status()
    return download.content


def _list_workspace_entries(signer, aidp_base: str, lake_ocid: str,
                            workspace_id: str, directory: str):
    """Return (file names, folder names) directly under a workspace folder.

    Returns None for a folder with no entries (the objects API answers an
    absent folder with an empty listing). Any other failure raises.
    """
    url = f"{aidp_base}/dataLakes/{lake_ocid}/workspaces/{workspace_id}/objects"
    files, folders = set(), set()
    page = None
    while True:
        params = {"path": directory, "limit": 1000}
        if page:
            params["page"] = page
        response = _http_requests.get(url, auth=signer, params=params, timeout=60)
        response.raise_for_status()
        for item in (response.json() or {}).get("items", []) or []:
            name = posixpath.basename(str(item.get("path", "")).rstrip("/"))
            (folders if item.get("type") == "FOLDER" else files).add(name)
        page = response.headers.get("opc-next-page")
        if not page:
            break
    if not files and not folders:
        return None
    return frozenset(files), frozenset(folders)


def _download_workspace_json(signer, aidp_base: str, lake_ocid: str,
                             workspace_id: str, aidp_path: str) -> dict:
    """Download and parse a regular workspace JSON file without FUSE."""
    return json.loads(_download_workspace_file(
        signer, aidp_base, lake_ocid, workspace_id, aidp_path,
    ))


def _aidp_control_plane_base(region: str) -> str:
    """Return the region-specific AIDP control-plane API base."""
    value = str(region or "").strip()
    if not re.fullmatch(r"[a-z0-9-]+", value):
        raise ValueError(f"Invalid AIDP region: {region!r}")
    return f"https://aidp.{value}.oci.oraclecloud.com/20240831"


def build_notebook_runtime_parameters(tasks: list,
                                      runtime_parameters: dict = None) -> dict:
    """Return effective parameters for each task notebook.

    This mirrors migration execution: task ``base_parameters`` are the base,
    then job/run parameters win on conflict. Dependency notebooks continue to
    use the global parameters because they do not own task-level parameters.
    """
    global_parameters = dict(runtime_parameters or {})
    by_notebook = {}
    def normalized(values) -> dict:
        if isinstance(values, dict):
            return dict(values)
        return {
            item.get("key", item.get("name", "")): item.get(
                "value", item.get("default", "")
            )
            for item in (values or [])
            if isinstance(item, dict) and item.get("key", item.get("name"))
        }

    def add_deps(deps: list, inherited: dict) -> None:
        for dep in deps or []:
            if not isinstance(dep, dict):
                continue
            effective = dict(inherited)
            effective.update(normalized(
                dep.get("parameters") or dep.get("arguments") or {}
            ))
            if dep.get("path"):
                by_notebook[dep["path"]] = effective
            add_deps(dep.get("nested_deps", []), effective)

    for task in tasks:
        notebook_path = task.get("notebook_path", "")
        if not notebook_path:
            continue
        base_parameters = normalized(
            task.get("base_parameters") or task.get("parameters") or {}
        )
        effective = dict(base_parameters)
        effective.update(global_parameters)
        by_notebook[notebook_path] = effective
        add_deps(task.get("run_deps", []), effective)
    return by_notebook


def _task_cluster_id(task: dict, fallback_cluster: str = "") -> str:
    """Resolve the AIDP cluster key assigned to one workflow task."""
    cluster = task.get("cluster_id") or task.get("cluster")
    if isinstance(cluster, dict):
        cluster = (
            cluster.get("cluster_id")
            or cluster.get("clusterId")
            or cluster.get("cluster_key")
            or cluster.get("clusterKey")
            or cluster.get("key")
        )
    return str(cluster or fallback_cluster or "").strip()


def build_notebook_cluster_map(tasks: list,
                               fallback_cluster: str = "") -> dict:
    """Map each task/dependency notebook to every job cluster it runs on.

    A ``%run`` dependency inherits its parent task's cluster.  A dependency
    shared by tasks on different clusters is therefore checked on each of
    those clusters.
    """
    by_notebook = {}

    def add(path: str, cluster_id: str) -> None:
        path = str(path or "").strip()
        if not path or not cluster_id:
            return
        clusters = by_notebook.setdefault(path, [])
        if cluster_id not in clusters:
            clusters.append(cluster_id)

    def add_deps(deps: list, cluster_id: str) -> None:
        for dep in deps or []:
            path = dep if isinstance(dep, str) else dep.get("path", "")
            add(path, cluster_id)
            if isinstance(dep, dict):
                add_deps(dep.get("nested_deps", []), cluster_id)

    for task in tasks:
        cluster_id = _task_cluster_id(task, fallback_cluster)
        add(task.get("notebook_path", ""), cluster_id)
        add_deps(task.get("run_deps", []), cluster_id)
    return by_notebook


def build_library_probe_plan(refs: dict,
                             notebook_clusters: dict) -> dict:
    """Group Python modules and JVM classes by actual job cluster."""
    plan = {}
    for notebook_path, data in refs.items():
        for cluster_id in notebook_clusters.get(notebook_path, []):
            entry = plan.setdefault(cluster_id, {
                "python": set(), "scala": set(), "notebooks": set(),
            })
            entry["python"].update(data.get("python_imports", []))
            entry["scala"].update(data.get("scala_imports", []))
            entry["notebooks"].add(notebook_path)
    return {
        cluster_id: {
            "python": sorted(data["python"]),
            "scala": sorted(data["scala"]),
            "notebooks": sorted(data["notebooks"]),
        }
        for cluster_id, data in plan.items()
        if data["python"] or data["scala"]
    }


async def probe_libraries_on_job_clusters(
    control_session,
    library_plan: dict,
    *,
    control_cluster_id: str,
    lake_ocid: str,
    workspace_id: str,
    oci_profile: str,
    job_name: str,
    session_factory=None,
    control_unavailable_reason: str | None = None,
) -> dict:
    """Probe each non-empty cluster plan without losing the data report.

    Stopped or deleted job clusters are routine outside a workflow run. Their
    connection failures are recorded as unverified dependency evidence while
    other clusters and the already-completed data checks continue.
    """
    session_factory = session_factory or AIDPSession
    results = {}
    for cluster_id, probe in library_plan.items():
        python_imports = probe.get("python", [])
        scala_imports = probe.get("scala", [])
        if not python_imports and not scala_imports:
            continue
        tprint(
            "Checking library/JAR availability on job cluster "
            f"{cluster_id[:12]}... "
            f"({len(python_imports)} Python, {len(scala_imports)} JVM)"
        )
        probe_session = control_session
        owns_probe_session = cluster_id != control_cluster_id
        if not owns_probe_session and probe_session is None:
            reason = control_unavailable_reason or "control probe session unavailable"
            error = (
                f"job-cluster dependency probe could not run on {cluster_id}: "
                f"{reason}"
            )
            tprint(f"WARNING: {error}")
            results[cluster_id] = {
                "python": {
                    name: {
                        "available": False,
                        "unverified": True,
                        "error": error,
                    }
                    for name in python_imports
                },
                "scala": {
                    name: {
                        "available": False,
                        "unverified": True,
                        "error": error,
                    }
                    for name in scala_imports
                },
            }
            continue
        if owns_probe_session:
            probe_session = session_factory(
                lake_ocid=lake_ocid,
                workspace_id=workspace_id,
                cluster_id=cluster_id,
                oci_profile=oci_profile,
                session_name=f"data_check_lib_{job_name}_{cluster_id[:8]}",
            )
        try:
            if owns_probe_session:
                await probe_session.connect()
            results[cluster_id] = await check_libraries(
                probe_session, python_imports, scala_imports
            )
        except Exception as exc:
            error = (
                f"job-cluster dependency probe could not run on {cluster_id}: "
                f"{type(exc).__name__}: {exc}"
            )
            tprint(f"WARNING: {error}")
            results[cluster_id] = {
                "python": {
                    name: {
                        "available": False,
                        "unverified": True,
                        "error": error,
                    }
                    for name in python_imports
                },
                "scala": {
                    name: {
                        "available": False,
                        "unverified": True,
                        "error": error,
                    }
                    for name in scala_imports
                },
            }
        finally:
            if owns_probe_session:
                try:
                    await probe_session.close()
                except Exception as exc:
                    tprint(
                        "WARNING: failed to close job-cluster probe session "
                        f"for {cluster_id}: {type(exc).__name__}: {exc}"
                    )
    return results


def merge_library_results_by_notebook(refs: dict,
                                      notebook_clusters: dict,
                                      cluster_results: dict) -> dict:
    """Project per-cluster library checks back onto notebook reports."""
    by_notebook = {}
    for notebook_path, data in refs.items():
        checked_clusters = notebook_clusters.get(notebook_path, [])
        merged = {"python": {}, "scala": {}}
        for kind, imports_key in (("python", "python_imports"),
                                  ("scala", "scala_imports")):
            for name in data.get(imports_key, []):
                attention = {}
                for cluster_id in checked_clusters:
                    info = (
                        cluster_results.get(cluster_id, {})
                        .get(kind, {})
                        .get(name)
                    )
                    if info is not None:
                        attention[cluster_id] = info
                if not attention:
                    continue
                available = all(
                    info.get("available", False)
                    for info in attention.values()
                )
                item = {
                    "available": available,
                    "checked_clusters": list(checked_clusters),
                    "cluster_results": attention,
                }
                verified_missing = any(
                    not info.get("available", False)
                    and not info.get("unverified", False)
                    for info in attention.values()
                )
                if (not verified_missing
                        and any(info.get("unverified") for info in attention.values())):
                    item["unverified"] = True
                if any(info.get("installed") for info in attention.values()):
                    item["installed"] = True
                for flag in ("submodule_missing", "import_error"):
                    if any(info.get(flag) for info in attention.values()):
                        item[flag] = True
                errors = [
                    f"{cluster_id}: {info['error']}"
                    for cluster_id, info in attention.items()
                    if info.get("error")
                ]
                if errors:
                    item["error"] = "; ".join(errors)
                merged[kind][name] = item
        by_notebook[notebook_path] = merged
    return {"by_notebook": by_notebook, "by_cluster": cluster_results}


async def extract_data_refs(session, notebook_paths: list,
                            aidp_base: str = None, lake_ocid: str = None,
                            workspace_id: str = None,
                            oci_profile: str = DEFAULT_OCI_PROFILE,
                            runtime_parameters: dict = None,
                            runtime_parameters_by_notebook: dict = None) -> dict:
    """Extract storage paths and table references from notebooks LOCALLY.

    Downloads each notebook via the AIDP workspace API (not the cluster's
    FUSE mount) and runs the extraction in-process via
    `data_check_extract.extract_from_notebook`. Cluster session is still
    accepted for API compatibility but is not used here — cluster is only
    needed later in `classify_refs_on_cluster` for existence checks.

    Returns per-notebook dict (unchanged public shape + new fields):
    {
        notebook_path: {
            "paths": [str, ...],
            "tables": [str, ...],
            "unresolved": [str, ...],   # variable-based reads + f-string URLs
            "python_imports": [str, ...],
            "scala_imports": [str, ...],
            "spark_reads": [{"api", "arg", "arg_is_literal"}, ...],   # NEW
            "spark_writes": [{"api", "arg", "arg_is_literal"}, ...],  # NEW
            "boto3_s3": [str, ...],     # NEW: boto3.client('s3') sightings
        }
    }
    """
    # Resolve auth + endpoints from module defaults if caller didn't pass
    lake_ocid = lake_ocid or DEFAULT_LAKE_OCID
    aidp_base = aidp_base or _aidp_control_plane_base(
        get_region_from_ocid(lake_ocid)
    )
    workspace_id = workspace_id or DEFAULT_WORKSPACE_ID
    _, signer = get_oci_signer(oci_profile)
    # ── Local extraction — no cluster round-trip ─────────────────────
    # Each notebook is fetched via AIDP downloadFileMeta (bounded, ~200ms/nb)
    # and parsed in-process by `data_check_extract.extract_from_notebook`.
    # Cluster session is kept on the parameter for API compatibility with
    # existing callers but is not used here — the extraction is pure Python
    # regex + state-machine work that has no reason to run on a cluster.
    #
    # This replaced a ~200-line f-string that was inlined into
    # `session.execute()` and had 3 historical bugs (comment stripper eating
    # URLs, variable-based `spark.read/write` calls not captured,
    # boto3.client('s3') not detected). See `scripts/data_check_extract.py`
    # and `tests/test_check_data_availability.py` for the rewrite.
    classified: dict = {}
    runtime_parameters_by_notebook = runtime_parameters_by_notebook or {}

    def _parameters_for(notebook_path: str) -> dict:
        return runtime_parameters_by_notebook.get(
            notebook_path, runtime_parameters or {})

    downloaded = {}
    download_errors = {}
    for nb_path in notebook_paths:
        try:
            downloaded[nb_path] = _download_notebook_json(
                signer, aidp_base, lake_ocid, workspace_id, nb_path)
        except Exception as exc:
            downloaded[nb_path] = None
            download_errors[nb_path] = str(exc)

    # notebook.run arguments override the child's inherited task/job values.
    # Repeat so a child can propagate effective parameters to a grandchild.
    for _ in range(max(1, len(notebook_paths))):
        changed = False
        for parent_path in notebook_paths:
            nb_json = downloaded.get(parent_path)
            if nb_json is None:
                continue
            preview = extract_from_notebook(
                nb_json, runtime_parameters=_parameters_for(parent_path)
            )
            for call in preview.get("notebook_runs", []):
                child_path = _match_notebook_run_target(
                    parent_path, call.get("path", ""), notebook_paths
                )
                if not child_path:
                    continue
                effective = dict(_parameters_for(parent_path))
                effective.update(call.get("parameters") or {})
                if runtime_parameters_by_notebook.get(child_path) != effective:
                    runtime_parameters_by_notebook[child_path] = effective
                    changed = True
        if not changed:
            break

    # Workspace modules a notebook imports from (``from imports.constants
    # import *``), resolved as Phase 2's sys.path would. The analyser applies
    # each import at its statement position; lookups that fail are reported
    # on the notebook as not analysed.
    modules = ImportedModuleSymbols(
        lambda directory: _list_workspace_entries(
            signer, aidp_base, lake_ocid, workspace_id, directory),
        lambda path: _download_workspace_file(
            signer, aidp_base, lake_ocid, workspace_id, path),
    )
    import_namespaces, import_gaps = {}, {}
    for nb_path in notebook_paths:
        nb_json = downloaded.get(nb_path)
        if nb_json is not None:
            import_namespaces[nb_path], import_gaps[nb_path] = modules.for_notebook(
                nb_json, nb_path)

    # Resolve constants from dependency notebooks before analysing roots.
    # Manifests normally list a task before its %run dependencies, hence the
    # reverse traversal. A second pass applies the merged symbols everywhere.
    shared_symbols = {}
    for nb_path in reversed(notebook_paths):
        nb_json = downloaded.get(nb_path)
        if nb_json is None:
            continue
        seed = extract_from_notebook(
            nb_json, runtime_parameters=_parameters_for(nb_path),
            initial_symbols=shared_symbols,
            notebook_dir=repository_notebook_dir(nb_path),
            import_namespaces=import_namespaces.get(nb_path))
        shared_symbols.update(seed.get("resolved_symbols", {}))

    for nb_path in notebook_paths:
        nb_json = downloaded.get(nb_path)
        if nb_json is None:
            # Notebook missing / unreadable — preserve empty entry to match
            # historical scanner's silent-skip behaviour.
            error = download_errors.get(nb_path, "notebook could not be read")
            classified[nb_path] = {
                "paths": [], "tables": [],
                "unresolved": [{
                    "kind": "NOT_ANALYZED",
                    "reason": error,
                    "notebook_path": nb_path,
                }],
                "python_imports": [], "scala_imports": [],
                "spark_reads": [], "spark_writes": [], "boto3_s3": [],
                "temporary_views": [],
                "mlflow_artifacts": [], "unresolved_mlflow_artifacts": [],
                "analysis_errors": [{
                    "type": "NOTEBOOK_DOWNLOAD_ERROR", "message": error,
                }],
                "not_analyzed": [{"kind": "notebook", "reason": error}],
            }
            continue
        agg = extract_from_notebook(
            nb_json, runtime_parameters=_parameters_for(nb_path),
            initial_symbols=shared_symbols,
            notebook_dir=repository_notebook_dir(nb_path),
            import_namespaces=import_namespaces.get(nb_path))
        agg.setdefault("not_analyzed", []).extend(import_gaps.get(nb_path, []))
        from slack_token_redaction import scrub_text
        for finding in agg.get("not_analyzed", []):
            if finding.get("kind") == "mlflow_registry" and finding.get("candidate_name"):
                finding["candidate_name"] = scrub_text(finding["candidate_name"])
        # Phase 1 makes no model call for load() sites. The extractor reports
        # every inventoried site as an UNRESOLVED_READ finding, so a read whose
        # dataset is not resolved always keeps the result from readiness.
        external_reads = []
        for call in agg.get("notebook_runs", []):
            if _match_notebook_run_target(
                nb_path, call.get("path", ""), notebook_paths
            ):
                continue
            agg.setdefault("not_analyzed", []).append({
                "kind": "notebook_run",
                "cell_number": call.get("cell_number"),
                "line_number": call.get("line_number"),
                "expression": call.get("expression"),
                "path": call.get("path"),
                "reason": (
                    "literal notebook target is outside the manifest or ambiguous"
                ),
            })
        classified[nb_path] = {
            "paths": agg["paths"],
            "tables": agg["tables"],
            "unresolved": (
                agg["unresolved"]
                + [{"kind": "NOT_ANALYZED", **item}
                   for item in agg.get("analysis_errors", [])]
                + [{"kind": "NOT_ANALYZED", **item}
                   for item in agg.get("not_analyzed", [])]
            ),
            "python_imports": agg["python_imports"],
            "scala_imports": agg["scala_imports"],
            "spark_reads": agg["spark_reads"],
            "spark_writes": agg["spark_writes"],
            "boto3_s3": agg["boto3_s3"],
            "temporary_views": agg.get("temporary_views", []),
            "cells_source": agg.get("cells_source", ""),
            "read_references": agg.get("read_references", []),
            "external_reads": external_reads,
            "excluded_reads": [],
            "write_references": agg.get("write_references", []),
            "unresolved_scopes": agg.get("unresolved_scopes", []),
            "mlflow_artifacts": agg.get("mlflow_artifacts", []),
            "unresolved_mlflow_artifacts": agg.get(
                "unresolved_mlflow_artifacts", []
            ),
            "runtime_anchor": agg.get("runtime_anchor"),
            "runtime_anchor_parameter": agg.get("runtime_anchor_parameter"),
            "analysis_errors": agg.get("analysis_errors", []),
            "notebook_runs": agg.get("notebook_runs", []),
            "not_analyzed": agg.get("not_analyzed", []),
        }
    return classified


# Migration policy: every Scylla keyspace is mirrored to the AIDP metastore as
# schema `scylla_<keyspace>`, table name unchanged (CELL_MIGRATE_PROMPT, FIX_PROMPT
# and the %scala Cassandra rewriter in job_migrate). test_connector_read_availability
# pins this constant to those three; change them together.
CASSANDRA_SOURCE_FORMAT = "org.apache.spark.sql.cassandra"
SCYLLA_MIRROR_SCHEMA_PREFIX = "scylla_"
# Unquoted CQL identifier. The name comes from notebook data, so anything else
# is never turned into SQL.
_CQL_IDENTIFIER = re.compile(r"[A-Za-z][A-Za-z0-9_]*")


def _external_target_table(read):
    """Return the Scylla mirror table for a Cassandra keyspace.table read, else None."""
    if read.get("source_connector") != CASSANDRA_SOURCE_FORMAT:
        return None
    keyspace, dot, table = str(read.get("source_object") or "").partition(".")
    if not dot or not all(_CQL_IDENTIFIER.fullmatch(name) for name in (keyspace, table)):
        return None
    return f"{SCYLLA_MIRROR_SCHEMA_PREFIX}{keyspace}.{table}"


def _external_target_path(source):
    """Reuse native path checks; OCI targets still require the namespace index."""
    if source.startswith(("/Volumes/", "/Workspace/")):
        return source
    if source.startswith("oci://"):
        authority = source[6:].split("/", 1)[0]
        bucket, separator, namespace = authority.partition("@")
        return source if separator and _BUCKET_NS.get(bucket) == namespace else None
    return map_to_oci_path(source).get("oci_path")


async def classify_refs_on_cluster(session, notebook_paths: list, refs: dict) -> dict:
    """Classify paths and tables as read/write by running regex on cluster.

    Returns same structure with usage field added.
    """
    # The structured AST pass can classify locally and retain filters, cell
    # provenance, formats, and resolved f-string paths. The old cluster regex
    # below cannot carry that information and also strips URLs containing //.
    for nb_path in notebook_paths:
        data = refs.get(nb_path, {})
        source = data.get("cells_source", "")
        reads = data.get("read_references", [])
        writes = data.get("write_references", [])
        created_at = {}
        for ref in writes:
            if (ref.get("usage") == "write_create"
                    and str(ref.get("write_mode") or "").lower() != "append"):
                key = (ref.get("object_type"), ref.get("object_name"))
                position = (ref.get("cell_number", 10**9), ref.get("line_number", 10**9))
                created_at[key] = min(created_at.get(key, (10**9, 10**9)), position)

        classified_paths = []
        classified_tables = []
        temporary_views = {
            str(view.get("name")): int(view.get("cell_number") or 0)
            for view in data.get("temporary_views", [])
            if isinstance(view, dict) and view.get("name")
        }

        def is_runtime_temp_view(name, cell_number=None):
            created_cell = temporary_views.get(str(name))
            if created_cell is None:
                return False
            if cell_number is None:
                return True
            return created_cell <= int(cell_number or 0)

        def add_unique(target, entry):
            marker = json.dumps(entry, sort_keys=True, default=str)
            if not any(json.dumps(x, sort_keys=True, default=str) == marker for x in target):
                target.append(entry)

        grouped_reads = {}
        for ref in reads:
            if (ref.get("object_type") == "table"
                    and is_runtime_temp_view(
                        ref.get("object_name"), ref.get("cell_number"))):
                continue
            key = (ref.get("object_type"), ref.get("object_name"))
            grouped_reads.setdefault(key, []).append(ref)
        selected_reads = []
        for key, candidates in grouped_reads.items():
            # Prefer reads that happen before this notebook creates/replaces the
            # object. Later reads are usually immediate validation of a freshly
            # produced output and are not migration inputs.
            created_position = created_at.get(key, (10**9, 10**9))
            external_candidates = [
                candidate for candidate in candidates
                if (candidate.get("cell_number", 10**9),
                    candidate.get("line_number", 10**9)) <= created_position
            ]
            candidate_pool = external_candidates or candidates
            observed_filters = []
            for candidate in candidates:
                for expression in candidate.get("filters") or []:
                    if expression not in observed_filters:
                        observed_filters.append(expression)
            migration_candidates = [
                candidate for candidate in candidate_pool
                if candidate.get("migration_filters")
            ]
            full_candidates = [
                candidate for candidate in candidate_pool
                if not candidate.get("filters")
            ]
            if full_candidates:
                # One genuine full read subsumes every filtered read of the
                # same object.  Selecting a partition in this mixed case
                # under-requests data needed by the full-table consumer.
                pool = [min(
                    full_candidates,
                    key=lambda x: (
                        x.get("cell_number", 10**9),
                        x.get("line_number", 10**9),
                        len(x.get("filters") or []),
                    ),
                )]
            elif migration_candidates:
                # Preserve one read per distinct migration scope.  Collapsing
                # these to one candidate under-migrates a table read through
                # multiple disjoint date/partition windows.
                scoped = {}
                for candidate in migration_candidates:
                    scope = tuple(candidate.get("migration_filters") or [])
                    current = scoped.get(scope)
                    position = (
                        candidate.get("cell_number", 10**9),
                        candidate.get("line_number", 10**9),
                    )
                    if current is None or position < current[0]:
                        scoped[scope] = (position, candidate)
                pool = [item[1] for item in scoped.values()]
            else:
                # Filters without a transferable partition/date scope still
                # require the object and therefore remain a full migration.
                full_requirement = dict(min(
                    candidate_pool,
                    key=lambda x: (
                        x.get("cell_number", 10**9),
                        x.get("line_number", 10**9),
                    ),
                ))
                structural_only = bool(full_requirement.get("filters")) and all(
                    _STRUCTURAL_ONLY_RE.fullmatch(expression or "")
                    for expression in full_requirement.get("filters", [])
                )
                if structural_only:
                    # Null/shape guards do not identify a transferable data
                    # partition. Preserve them as evidence, but request the
                    # complete source object for migration.
                    full_requirement["filters"] = []
                    full_requirement["migration_filters"] = []
                pool = [full_requirement]
            for candidate in pool:
                chosen = dict(candidate)
                if observed_filters:
                    chosen["observed_filters"] = observed_filters
                if chosen.get("migration_filters"):
                    chosen["filters"] = chosen.pop("migration_filters")
                selected_reads.append(chosen)

        for ref in selected_reads + writes:
            object_type = ref.get("object_type")
            object_name = ref.get("object_name")
            if object_type not in ("path", "table") or not object_name:
                continue
            entry_key = "path" if object_type == "path" else "table"
            entry = {entry_key: object_name, **{
                key: value for key, value in ref.items()
                if key not in ("object_type", "object_name")
            }}
            created_key = (object_type, object_name)
            if (ref.get("usage") == "read"
                    and created_at.get(created_key, (10**9, 10**9))
                    < (ref.get("cell_number", -1), ref.get("line_number", -1))):
                entry["usage"] = "write_create"
                entry["created_in_notebook"] = True
            add_unique(classified_paths if object_type == "path" else classified_tables, entry)

        connector_paths = {read["source_object"] for read in data.get("external_reads", [])
                           if read["object_type"] == "path"}
        for path in data.get("paths", []):
            # These are unlocated literal-scan candidates, not additional read
            # sites. A resolved connector read already accounts for the same path
            # and its routed target. Keep structured reads/writes above: they
            # can be distinct uses with their own format, location and filters.
            if path in connector_paths:
                continue
            if any(x.get("path") == path for x in classified_paths):
                continue
            prefix = path.rstrip("/") + "/"
            if any(str(x.get("path", "")).startswith(prefix) for x in classified_paths):
                continue
            add_unique(classified_paths, {
                "path": path,
                "usage": classify_path_usage(source, path),
            })

        for table in data.get("tables", []):
            if is_runtime_temp_view(table):
                continue
            if any(x.get("table") == table for x in classified_tables):
                continue
            add_unique(classified_tables, {
                "table": table,
                "usage": classify_table_usage(source, table),
            })

        data["paths"] = classified_paths
        data["tables"] = classified_tables
    return refs

    # Historical cluster classifier retained below as documentation.
    # Build classification request — send paths/tables per notebook
    # and let the cluster do the regex classification
    classify_input = {}
    for nb_path in notebook_paths:
        nb_refs = refs.get(nb_path, {})
        if nb_refs.get("paths") or nb_refs.get("tables"):
            classify_input[nb_path] = {
                "paths": nb_refs.get("paths", []),
                "tables": nb_refs.get("tables", []),
            }

    if not classify_input:
        return refs

    input_json = json.dumps(classify_input)
    result = await session.execute(f"""
import json, os, re

classify_input = {input_json}
results = {{}}

# Read patterns
READ_P = [r'spark\\.read\\.', r'spark\\.table\\s*\\(', r'pd\\.read_',
          r'\\.load\\s*\\(', r'FROM\\s+', r'JOIN\\s+',
          r'sc\\.textFile', r'sc\\.wholeTextFiles', r'dbutils\\.fs\\.ls']
# Write patterns
WRITE_P = [r'\\.write\\.', r'\\.save\\s*\\(', r'\\.saveAsTable',
           r'INSERT\\s+(?:INTO|OVERWRITE)', r'CREATE\\s+TABLE',
           r'dbutils\\.fs\\.put', r'\\.to_csv\\s*\\(', r'\\.to_parquet']

for nb_path, data in classify_input.items():
    candidates = [nb_path]
    if not nb_path.startswith("/Workspace"):
        candidates.append("/Workspace" + nb_path)
    if not nb_path.endswith(".ipynb"):
        candidates = [c + ".ipynb" for c in candidates] + candidates

    def _strip_comments(code):
        code = re.sub(r'(\\\"\\\"\\\"[\\s\\S]*?\\\"\\\"\\\")', '', code)
        code = re.sub(r"(\\\'\\\'\\\'[\\s\\S]*?\\\'\\\'\\\')", '', code)
        code = re.sub(r'/\\*[\\s\\S]*?\\*/', '', code)
        code = re.sub(r'#[^\\n]*', '', code)
        code = re.sub(r'//[^\\n]*', '', code)
        return code

    source = ""
    for path in candidates:
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    nb = json.load(fh)
                for cell in nb.get("cells", []):
                    source += _strip_comments("".join(cell.get("source", []))) + "\\n"
            except:
                pass
            break

    nb_result = {{"paths": [], "tables": []}}

    for p in data["paths"]:
        lines = [l for l in source.splitlines() if p in l]
        ctx = "\\n".join(lines)
        is_r = any(re.search(rp, ctx, re.IGNORECASE) for rp in READ_P)
        is_w = any(re.search(wp, ctx, re.IGNORECASE) for wp in WRITE_P)
        if is_w and not is_r:
            usage = "write"
        else:
            usage = "read"
        nb_result["paths"].append({{"path": p, "usage": usage}})

    _qt = chr(39) + chr(34)  # quotes: ' and "
    for t in data["tables"]:
        lines = [l for l in source.splitlines() if t in l]
        ctx = "\\n".join(lines)
        # Insert-style writes: table must exist
        insert_pats = [r'INSERT\\s+(?:INTO|OVERWRITE)\\s+' + re.escape(t)]
        # Create-style writes: table will be created
        create_pats = [r'CREATE\\s+TABLE.*' + re.escape(t),
                       r'\\.saveAsTable\\s*\\(\\s*[' + _qt + r']' + re.escape(t)]
        r_pats = [r'FROM\\s+' + re.escape(t),
                   r'JOIN\\s+' + re.escape(t),
                   r'spark\\.table\\s*\\(\\s*[' + _qt + r']' + re.escape(t)]
        is_r = any(re.search(rp, ctx, re.IGNORECASE) for rp in r_pats)
        is_insert = any(re.search(wp, ctx, re.IGNORECASE) for wp in insert_pats)
        is_create = any(re.search(wp, ctx, re.IGNORECASE) for wp in create_pats)
        if is_r:
            usage = "read"
        elif is_insert and not is_create:
            usage = "write_insert"
        elif is_create:
            usage = "write_create"
        else:
            usage = "read"
        nb_result["tables"].append({{"table": t, "usage": usage}})

    results[nb_path] = nb_result

print(json.dumps(results))
""", timeout=120)
    output = unwrap_outputs(result.get("outputs", []))
    try:
        classified = parse_cluster_json(output)
    except Exception as e:
        # Do NOT default to "read". A write misclassified as read slips past the
        # write-guard that redirects customer-data writes to the tmp bucket —
        # too dangerous to swallow. Fail loud so the operator can rerun.
        raise RuntimeError(
            "Read/write classification parse failed; refusing to default to 'read' "
            "(would risk treating a customer-data write as a read). "
            f"Raw cluster output (first 500): {output[:500]}"
        ) from e

    # Merge classification into refs
    for nb_path, data in classified.items():
        if nb_path in refs:
            refs[nb_path]["paths"] = data.get("paths", [])
            refs[nb_path]["tables"] = data.get("tables", [])
            refs[nb_path]["unresolved"] = refs[nb_path].get("unresolved", [])
    return refs


# ─── Availability Checks ────────────────────────────────────────────

class ProbeAttemptsExhausted(RuntimeError):
    """Both bounded warm-session execution attempts failed."""


class _ProbeSessionRecovery:
    """Use the caller's warm session and replace it only after probe failure.

    The data check is an advisory existence check.  Creating a cold kernel for
    each probe stage both multiplies session-create failures and can return
    transient metastore false negatives.  The already-used control session is
    therefore the happy-path session for every probe.  A fresh session is
    created only when execution or JSON parsing fails, warmed before use, and
    retained for later probes.  Replacement creation/connect/warm-up failures
    open the circuit immediately.  Probe-specific double failures consume a
    small run-wide replacement budget so two isolated bad batches do not
    suppress unrelated checks while deterministic platform failures remain
    bounded.
    """

    def __init__(self, control_session, stage_name: str, session_factory=None,
                 on_session_change=None):
        self.control = control_session
        self.stage_name = re.sub(
            r"[^a-z0-9]+", "_", stage_name.lower()
        ).strip("_")
        self.session_factory = session_factory or AIDPSession
        self.probe = control_session
        self._owns_probe = False
        self._generation = 0
        self._replacement_creations = 0
        self._replacement_budget = PROBE_REPLACEMENT_SESSION_BUDGET
        self._dead_error = None
        self._on_session_change = on_session_change

    @property
    def unavailable(self) -> bool:
        return self._dead_error is not None

    @property
    def unavailable_reason(self) -> str:
        return str(self._dead_error or "probe session is unavailable")

    def _publish_session(self, probe) -> None:
        if self._on_session_change is not None:
            self._on_session_change(probe)

    def _session_name(self) -> str:
        owner = str(getattr(self.control, "_session_name", "data_check"))
        owner_slug = re.sub(r"[^A-Za-z0-9_-]+", "_", owner).strip("_")
        digest = hashlib.sha256(owner.encode("utf-8")).hexdigest()[:8]
        owner_slug = (owner_slug or "data_check")[:36]
        return (
            f"data_check_probe_{owner_slug}_{digest}_{self.stage_name}_"
            f"{self._generation % 2}"
        )

    async def _discard_probe(self):
        probe, self.probe = self.probe, None
        if probe is not None:
            try:
                await probe.close()
            except Exception as exc:
                tprint(
                    "WARNING: failed to close discarded data-probe session: "
                    f"{type(exc).__name__}: {exc}"
                )
            self._generation += 1
        self._publish_session(None)
        self._owns_probe = False

    async def _warm_probe(self, probe):
        operation = "Replacement probe-session warm-up"
        result = await probe.execute(
            """
import json
spark.range(1).count()
spark.sql("SHOW DATABASES").collect()
print(json.dumps({"ready": True}))
""",
            timeout=180,
        )
        payload = _require_successful_probe_json(result, operation)
        if payload.get("ready") is not True:
            raise RuntimeError(f"{operation} returned unexpected payload: {payload}")

    async def _new_probe(self):
        if self.unavailable:
            raise RuntimeError(
                "Probe stage is unavailable after replacement failure: "
                f"{self.unavailable_reason}"
            ) from self._dead_error
        if self._replacement_creations >= self._replacement_budget:
            self._dead_error = RuntimeError(
                "replacement-session budget exhausted "
                f"({self._replacement_creations}/{self._replacement_budget})"
            )
            raise self._dead_error
        self._replacement_creations += 1
        try:
            probe = self.session_factory(
                lake_ocid=self.control.lake_ocid,
                workspace_id=self.control.workspace_id,
                cluster_id=self.control.cluster_id,
                oci_profile=self.control.oci_profile,
                session_name=self._session_name(),
                session_dir=getattr(
                    self.control, "_session_dir", "Workspace/aidp_tool_session"
                ),
            )
            self.probe = probe
            self._owns_probe = True
            # Publish immediately so main() can close this session if cancellation
            # interrupts connect or warm-up.
            self._publish_session(probe)
            await probe.connect()
            await self._warm_probe(probe)
        except BaseException as exc:
            self._dead_error = exc
            await self._discard_probe()
            raise
        return probe

    async def current_session(self):
        """Return the healthy session, or None after replacement is impossible."""
        if self.unavailable:
            return None
        if self.probe is not None:
            return self.probe
        try:
            return await self._new_probe()
        except Exception:
            # _new_probe records the exact connect/warm-up/budget failure in
            # _dead_error.  Stage boundaries must degrade later checks to
            # UNVERIFIED so main() can still persist the report.  Cancellation
            # and other BaseException subclasses intentionally still escape.
            return None

    async def execute_json(
        self, code: str, operation: str, timeout: int, attempts: int = 2
    ) -> dict:
        if self.unavailable:
            raise RuntimeError(
                f"{operation} not executed because the probe stage is unavailable: "
                f"{self.unavailable_reason}"
            ) from self._dead_error
        attempt_errors = []
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                tprint(f"  {operation}: attempt {attempt}/{attempts}")
                probe = self.probe or await self._new_probe()
                result = await probe.execute(code, timeout=timeout)
                return _require_successful_probe_json(result, operation)
            except Exception as exc:
                last_error = exc
                attempt_errors.append(
                    f"attempt {attempt} {type(exc).__name__}: {exc}"
                )
                await self._discard_probe()
                if self.unavailable:
                    break
                if attempt < attempts:
                    tprint(
                        f"  [data-check] {operation} failed: {exc}. "
                        "Replacing probe session and retrying once."
                    )
        raise ProbeAttemptsExhausted(
            f"{operation} failed after {len(attempt_errors)} "
            "warm-session attempts: "
            + " | ".join(attempt_errors)
        ) from last_error

    async def close(self):
        if self._owns_probe:
            await self._discard_probe()


def _unverified_local_results(paths: list, reason: str) -> dict:
    return {
        path: {
            "exists": "unverified",
            "probe_status": "UNVERIFIED",
            "error": reason,
        }
        for path in paths
    }


def _unverified_mlflow_results(artifacts: list, reason: str) -> dict:
    return {
        str(item.get("artifact_uri")): {
            "exists": "unverified",
            "probe_status": "UNVERIFIED",
            "probe_error": True,
            "error": reason,
        }
        for item in artifacts
        if isinstance(item, dict) and item.get("artifact_uri")
    }

def _require_successful_probe_json(result: dict, operation: str) -> dict:
    """Reject failed, empty, invalid, or otherwise unverified probe output."""
    outputs = unwrap_outputs(result.get("outputs", []))
    if result.get("status") == "error":
        raise RuntimeError(
            f"{operation} is UNVERIFIED: cluster execution failed. "
            f"Output (first 500 chars): {outputs[:500]}"
        )
    try:
        return require_cluster_json(outputs, operation)
    except Exception as exc:
        raise RuntimeError(f"{operation} is UNVERIFIED: {exc}") from exc


async def _execute_json_probe_with_recovery(
    session, code: str, operation: str, timeout: int, attempts: int = 2,
    probe_stage=None,
) -> dict:
    """Run a read-only JSON probe with replace-on-failure recovery."""
    if probe_stage is None:
        # Preserve the small executor protocol used by unit/custom callers and
        # never create a cold session merely because a caller has AIDP fields.
        result = await session.execute(code, timeout=timeout)
        return _require_successful_probe_json(result, operation)

    return await probe_stage.execute_json(code, operation, timeout, attempts)


async def check_tables_describe(session, tables: list, probe_stage=None) -> dict:
    """Check table availability using DESCRIBE TABLE. Returns {table: result}."""
    if not tables:
        return {}

    all_results = {}
    stage = probe_stage
    total = len(tables)
    for index, table in enumerate(tables, start=1):
        tables_json = json.dumps([table])
        code = f"""
import json
tables = {tables_json}
results = {{}}
for table in tables:
    # Try 2-part and 3-part names
    candidates = [table]
    parts = table.split(".")
    if len(parts) == 2:
        candidates.append(f"default.{{table}}")

    found = False
    for t in candidates:
        try:
            desc = spark.sql(f"DESCRIBE TABLE {{t}}")
            cols = desc.collect()
            schema_info = [f"{{r['col_name']}} {{r['data_type']}}" for r in cols[:10]]
            results[table] = {{"exists": True, "resolved": t,
                              "columns": len(cols),
                              "schema_preview": schema_info}}
            found = True
            break
        except Exception as e:
            err = str(e)[:150]
            if "not found" in err.lower() or "does not exist" in err.lower():
                continue
            results[table] = {{"exists": False, "error": err}}
            found = True
            break

    if not found:
        results[table] = {{"exists": False}}

print(json.dumps(results))
"""
        operation = f"Table availability check {index}/{total} ({table})"
        try:
            result = await _execute_json_probe_with_recovery(
                session, code, operation, timeout=120, attempts=2,
                probe_stage=stage,
            )
            all_results.update(result)
        except Exception as exc:
            if stage is None:
                raise
            tprint(f"  [UNVERIFIED] {table}: {exc}")
            all_results[table] = {
                "exists": "unverified",
                "probe_status": "UNVERIFIED",
                "error": str(exc),
                "operation": operation,
            }
    return all_results


def _concrete_glob_base_path(path: str) -> str:
    """Return the concrete directory prefix for a Hadoop glob path."""
    positions = [path.find(char) for char in ("*", "?", "[", "{")]
    positions = [position for position in positions if position >= 0]
    if not positions:
        return path
    wildcard_at = min(positions)
    slash_at = path.rfind("/", 0, wildcard_at)
    if slash_at < 0:
        return path
    return path[: slash_at + 1]


def _oci_path_probe_code(path_specs_json: str) -> str:
    """Kernel code for one OCI path-probe batch (factored for testability).

    REF-257 classification contract (Example_Alerts_Summary, DBX job
    100000000000003): `exists` is False ONLY when fs.exists() itself
    returned False. A probe EXCEPTION is never "missing" — the previous
    `str(e)[:150]` truncation cut the BmcException `Caused by` line off and
    the record collapsed to exists=False, so an authorization failure was
    counted in paths_missing and the shared-bucket failure surfaced only at
    Phase 2 as 404 BucketNotFound. Exceptions now classify as:

      access_denied : explicit 403 / AccessDenied
      access_error  : ListObjects 404 BucketNotFound — OCI deliberately
                      masks "not authorized" as "bucket not found", so the
                      two are indistinguishable from the probing side and
                      are reported as that explicit ambiguity
      probe_error   : anything else (connector/transport)

    The decisive cause line is preserved (first BmcException/Caused-by
    line, 400 chars) instead of a blind prefix of the Py4J wrapper.
    """
    return f"""
import json
path_specs = {path_specs_json}
results = {{}}

def _classify(exc, probe_path):
    full = str(exc)
    # The DECISIVE cause is the OCI service error (BmcException carries the
    # operation, status and the bucket-or-authorization sentence). Nested
    # chains put intermediate IOException "Caused by" wrappers FIRST —
    # stopping at the first match dropped the diagnosis (review 2026-10-28,
    # executed nested-cause repro). Prefer the BmcException line anywhere in
    # the chain; fall back to the first Caused-by, then the raw prefix.
    cause = ""
    fallback = ""
    for line in full.splitlines():
        if "BmcException" in line:
            cause = line.strip()[:400]
            break
        if not fallback and "Caused by" in line:
            fallback = line.strip()[:400]
    err = cause or fallback or full[:400]
    if "AccessDenied" in full or "(403" in full:
        return {{"exists": "access_denied", "error": err,
                 "probe_path": probe_path}}
    if "BucketNotFound" in full or "NotAuthorizedOrNotFound" in full:
        return {{"exists": "access_error", "error": err,
                 "probe_path": probe_path}}
    return {{"exists": "probe_error", "error": err,
             "probe_path": probe_path}}

for spec in path_specs:
    path = spec["path"]
    probe_path = spec["probe_path"]
    try:
        jvm = spark._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(probe_path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        p = jvm.org.apache.hadoop.fs.Path(probe_path)
        exists = fs.exists(p)
        if exists:
            try:
                status = fs.listStatus(p)
                count = 0
                for s in status:
                    count += 1
                    if count > 100:
                        break
                results[path] = {{"exists": True, "items": count,
                                  "probe_path": probe_path}}
            except Exception as le:
                # exists=True but the LISTING failed: a Head-allowed /
                # List-denied principal lands exactly here (the Mars IAM
                # shape) and Spark reads will fail at runtime. Recording
                # "found" here was a proven false-PASS (review 2026-10-27);
                # classify fail-closed like any probe exception.
                results[path] = _classify(le, probe_path)
        else:
            results[path] = {{"exists": False, "probe_path": probe_path}}
    except Exception as e:
        results[path] = _classify(e, probe_path)
print(json.dumps(results))
"""


# Probe-failure statuses (kernel value -> report probe_status). An entry in
# one of these states is NOT "missing": the object may well exist; the probe
# could not see it (REF-257).
_PROBE_FAILURE_STATUSES = {
    "access_denied": "ACCESS_DENIED",
    "access_error": "ACCESS_ERROR",
    "probe_error": "PROBE_ERROR",
}


def _render_path_status(exists) -> str:
    """Console label for a path probe outcome. Probe failures render as
    their own states — labelling them MISSING restated the REF-257 lie."""
    if exists is True:
        return "EXISTS"
    if exists == "unverified":
        return "UNVERIFIED"
    if exists == "access_denied":
        return "ACCESS DENIED"
    if exists == "access_error":
        return "ACCESS ERROR (bucket not found or not authorized)"
    if exists == "probe_error":
        return "PROBE ERROR"
    if exists == "timeout":
        # Local (/Volumes, /Workspace) probes report "timeout"; it is an
        # unanswered check, not an absence (review 2026-10-27).
        return "TIMEOUT"
    return "MISSING"



async def check_oci_paths(session, paths: list, probe_stage=None) -> dict:
    """Check OCI path availability via fs.listStatus() with timeout.

    Paths are grouped by URI authority (``bucket@namespace``).  This trades
    extra round trips for isolation when a job references many small buckets.
    After both execution attempts for one
    batch fail, later batches for that same storage endpoint are marked
    UNVERIFIED without consuming more replacement-session budget.  At the
    configured limits one failing endpoint can occupy at most two 180-second
    probes plus one 180-second replacement warm-up (about nine minutes) and
    consumes up to two of the four run-wide replacement creations (the retry
    and the successor used by a later source).  Two failing endpoints can
    exhaust that budget; a later source then opens the circuit and downstream
    probes are UNVERIFIED.  Table and scoped-reference probes share the same
    run-wide budget and are not grouped by storage endpoint.
    Returns {path: result}."""
    if not paths:
        return {}

    path_specs = [
        {"path": path, "probe_path": _concrete_glob_base_path(path)}
        for path in paths
    ]
    grouped_specs = {}
    for spec in path_specs:
        authority = spec["probe_path"].split("/", 3)[2]
        grouped_specs.setdefault(authority, []).append(spec)

    batch_size = OCI_PATH_PROBE_BATCH_SIZE
    all_results = {}
    stage = probe_stage
    total_batches = sum(
        (len(specs) + batch_size - 1) // batch_size
        for specs in grouped_specs.values()
    )
    batch_number = 0
    for authority, authority_specs in grouped_specs.items():
        endpoint_failed_at = None
        for offset in range(0, len(authority_specs), batch_size):
            batch_number += 1
            batch = authority_specs[offset:offset + batch_size]
            if endpoint_failed_at is not None:
                if stage is not None and stage.unavailable:
                    reason = (
                        "not executed because the probe stage is unavailable: "
                        f"{stage.unavailable_reason}"
                    )
                else:
                    reason = (
                        f"skipped after batch {endpoint_failed_at} double failure "
                        f"for OCI endpoint {authority}"
                    )
                operation = (
                    f"OCI path availability batch {batch_number}/{total_batches} "
                    f"({len(batch)} paths, timeout="
                    f"{OCI_PATH_PROBE_TIMEOUT_SECONDS}s)"
                )
                tprint(f"  [UNVERIFIED] {operation}: {reason}")
                for spec in batch:
                    all_results[spec["path"]] = {
                        "exists": "unverified",
                        "probe_status": "UNVERIFIED",
                        "probe_path": spec["probe_path"],
                        "error": reason,
                        "operation": operation,
                    }
                continue
            path_specs_json = json.dumps(batch)
            code = _oci_path_probe_code(path_specs_json)
            operation = (
                f"OCI path availability batch {batch_number}/{total_batches} "
                f"({len(batch)} paths, timeout={OCI_PATH_PROBE_TIMEOUT_SECONDS}s)"
            )
            probe_timeout = OCI_PATH_PROBE_TIMEOUT_SECONDS
            try:
                result = await _execute_json_probe_with_recovery(
                    session, code, operation, timeout=probe_timeout, attempts=2,
                    probe_stage=stage,
                )
                incomplete = [
                    spec["path"]
                    for spec in batch
                    if not isinstance(result.get(spec["path"]), dict)
                    or "exists" not in result[spec["path"]]
                ]
                if incomplete:
                    raise RuntimeError(
                        f"{operation} is UNVERIFIED: incomplete results for "
                        f"{incomplete!r}"
                    )
                all_results.update(result)
            except ProbeAttemptsExhausted as exc:
                if stage is None:
                    raise
                endpoint_failed_at = batch_number
                names = ", ".join(spec["path"] for spec in batch)
                tprint(f"  [UNVERIFIED] {operation}: {names}: {exc}")
                for spec in batch:
                    all_results[spec["path"]] = {
                        "exists": "unverified",
                        "probe_status": "UNVERIFIED",
                        "probe_path": spec["probe_path"],
                        "error": str(exc),
                        "operation": operation,
                    }
            except Exception as exc:
                if stage is None:
                    raise
                names = ", ".join(spec["path"] for spec in batch)
                tprint(f"  [UNVERIFIED] {operation}: {names}: {exc}")
                for spec in batch:
                    all_results[spec["path"]] = {
                        "exists": "unverified",
                        "probe_status": "UNVERIFIED",
                        "probe_path": spec["probe_path"],
                        "error": str(exc),
                        "operation": operation,
                    }
    return all_results


async def check_local_paths(session, paths: list, probe_stage=None) -> dict:
    """Check Volume/Workspace paths via ls on cluster.
    Returns {path: result}."""
    if not paths:
        return {}

    paths_json = json.dumps(paths)
    code = f"""
import json, subprocess
paths = {paths_json}
results = {{}}
for path in paths:
    try:
        r = subprocess.run(["ls", path], capture_output=True, timeout=10, text=True)
        if r.returncode == 0:
            items = [x for x in r.stdout.strip().splitlines() if x]
            results[path] = {{"exists": True, "items": len(items)}}
        else:
            results[path] = {{"exists": False, "error": r.stderr.strip()[:100]}}
    except subprocess.TimeoutExpired:
        results[path] = {{"exists": "timeout"}}
    except Exception as e:
        results[path] = {{"exists": False, "error": str(e)[:100]}}
print(json.dumps(results))
"""
    operation = "Local path availability check"
    try:
        result = await _execute_json_probe_with_recovery(
            session, code, operation, timeout=60, probe_stage=probe_stage,
        )
        incomplete = [
            path for path in paths
            if not isinstance(result.get(path), dict)
            or "exists" not in result[path]
        ]
        if incomplete:
            raise RuntimeError(
                f"{operation} is UNVERIFIED: incomplete results for "
                f"{incomplete!r}"
            )
        return result
    except Exception as exc:
        if probe_stage is None:
            raise
        tprint(f"  [UNVERIFIED] {operation}: {exc}")
        return _unverified_local_results(paths, f"{operation}: {exc}")


async def check_mlflow_artifacts(session, artifacts: list,
                                 probe_stage=None) -> dict:
    """Check MLflow run/model artifacts on the configured tracking server.

    The tracking URI is resolved inside the AIDP kernel so credentials never
    leave the cluster. Results are keyed by canonical ``runs:/``/``models:/``
    URI and contain only availability metadata, never the tracking URI.

    All artifacts use the kernel's single configured tracking server.  After
    both bounded execution attempts for one artifact fail, later artifacts on
    that tracker are marked UNVERIFIED rather than spending more of the four
    run-wide replacement creations.  At the configured limits the failing
    artifact can occupy two 180-second probes plus one 180-second replacement
    warm-up (about nine minutes).  A timeout may represent a slow
    ``list_artifacts`` call rather than a dead server; tracker-wide skipping is
    the deliberate bounded-runtime tradeoff and is surfaced in every record.
    """
    if not artifacts:
        return {}

    unique = {}
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        uri = str(artifact.get("artifact_uri") or "").strip()
        if uri:
            unique.setdefault(uri, artifact)
    if not unique:
        return {}

    all_results = {}
    tracker_failed_at = None
    for position, (uri, artifact) in enumerate(unique.items(), start=1):
        if tracker_failed_at is not None:
            if probe_stage is not None and probe_stage.unavailable:
                reason = (
                    "not executed because the probe stage is unavailable: "
                    f"{probe_stage.unavailable_reason}"
                )
            else:
                reason = (
                    f"skipped after artifact {tracker_failed_at} double failure "
                    "on the configured MLflow tracking server"
                )
            tprint(f"  [UNVERIFIED] {uri}: {reason}")
            all_results.update(
                _unverified_mlflow_results([artifact], reason)
            )
            continue
        specs_json = json.dumps([artifact])
        code = f"""
import json, os
specs = {specs_json}
results = {{}}

try:
    import mlflow
    from mlflow.tracking import MlflowClient

    tracking_uri = str(os.environ.get("MLFLOW_TRACKING_URI") or "").strip()
    if not tracking_uri:
        try:
            aidp = globals().get("aidputils")
            secret_name = os.environ.get(
                "AIDP_CRED_SECRETS_NAME", "migration_tool_secrets"
            )
            secret_key = os.environ.get(
                "AIDP_CRED_MLFLOW_KEY", "mlflow-tracking-uri"
            )
            if aidp is not None:
                tracking_uri = str(
                    aidp.secrets.get(secret_name, key=secret_key) or ""
                ).strip()
        except Exception:
            tracking_uri = ""
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    active_uri = str(mlflow.get_tracking_uri() or "")
    if not active_uri or active_uri.startswith("file:"):
        raise RuntimeError(
            "MLflow tracking URI is not configured for the AIDP cluster"
        )
    client = MlflowClient()

    for spec in specs:
        uri = spec["artifact_uri"]
        try:
            run_id = str(spec.get("run_id") or "").strip()
            artifact_path = str(spec.get("artifact_path") or "").strip().strip("/")
            expected_type = str(spec.get("object_type") or "").upper()
            if run_id:
                client.get_run(run_id)
                if not artifact_path:
                    results[uri] = {{"exists": True, "items": 1,
                                     "object_type": "DIRECTORY"}}
                    continue
                parent = artifact_path.rsplit("/", 1)[0] if "/" in artifact_path else ""
                entries = client.list_artifacts(run_id, parent or None)
                match = next(
                    (item for item in entries
                     if str(getattr(item, "path", "")).rstrip("/")
                     == artifact_path.rstrip("/")),
                    None,
                )
                exists = match is not None
                if not exists and expected_type == "DIRECTORY":
                    children = client.list_artifacts(run_id, artifact_path)
                    exists = bool(children)
                results[uri] = {{
                    "exists": exists,
                    "items": 1 if match is not None else 0,
                    "object_type": (
                        "DIRECTORY" if match is not None
                        and bool(getattr(match, "is_dir", False))
                        else expected_type
                    ),
                }}
            else:
                entries = mlflow.artifacts.list_artifacts(artifact_uri=uri)
                results[uri] = {{
                    "exists": True,
                    "items": len(entries or []),
                    "object_type": expected_type or "DIRECTORY",
                }}
        except Exception as exc:
            text = str(exc)
            missing = any(token in text.lower() for token in (
                "resource_does_not_exist", "not found", "does not exist", "404"
            ))
            results[uri] = {{
                "exists": False,
                "error": text[:300],
                "probe_error": not missing,
            }}
except Exception as exc:
    for spec in specs:
        results[spec["artifact_uri"]] = {{
            "exists": False,
            "error": str(exc)[:300],
            "probe_error": True,
        }}

print(json.dumps(results))
"""
        operation = (
            "MLflow artifact availability check "
            f"{position}/{len(unique)} ({uri})"
        )
        try:
            batch_results = await _execute_json_probe_with_recovery(
                session,
                code,
                operation,
                timeout=180,
                probe_stage=probe_stage,
            )
            if uri not in batch_results:
                raise RuntimeError(
                    f"{operation} returned no result for {uri}"
                )
            all_results[uri] = batch_results[uri]
        except ProbeAttemptsExhausted as exc:
            tracker_failed_at = position
            reason = f"{operation}: {exc}"
            tprint(f"  [UNVERIFIED] {uri}: {exc}")
            all_results.update(
                _unverified_mlflow_results([artifact], reason)
            )
        except Exception as exc:
            reason = f"{operation}: {exc}"
            tprint(f"  [UNVERIFIED] {uri}: {exc}")
            all_results.update(
                _unverified_mlflow_results([artifact], reason)
            )

    return all_results

def _scope_key(notebook_path: str, object_type: str, object_name: str,
               filters: list) -> str:
    return json.dumps(
        [notebook_path, object_type, object_name, filters],
        separators=(",", ":"), sort_keys=False)


def _safe_scope_predicate(filters: list) -> str:
    """Return a safe Spark SQL predicate generated by the local analyser."""
    if not filters or not all(isinstance(item, str) and item.strip() for item in filters):
        return ""
    predicate = " AND ".join(f"({item.strip()})" for item in filters)
    if any(token in predicate for token in (";", "--", "/*", "*/")):
        return ""
    # Reject SQL function calls. Parentheses are retained for grouping and
    # ``IN (...)`` lists, but executable functions such as reflect(...) are
    # outside the locally generated comparison/boolean contract.
    if re.search(
        r"\b(?!(?:AND|OR|NOT|IN)\b)[A-Za-z_][A-Za-z0-9_\.]*\s*\(",
        predicate,
        re.IGNORECASE,
    ):
        return ""
    # Scoped probing intentionally accepts only comparisons and boolean logic.
    # Complex UDF expressions are still reported, but are not executed.
    if not re.fullmatch(r"[A-Za-z0-9_\.\s'\"<>=!(),:+\-%]+", predicate):
        return ""
    return predicate


async def check_scoped_references(session, refs: dict,
                                  table_results: dict, oci_results: dict,
                                  probe_stage=None) -> dict:
    """Probe filtered table/path scopes after the base object exists.

    A zero-row scope is returned as ``scope_exists=False`` while preserving
    ``base_exists=True``. This is what allows the tracker to create a PARTIAL,
    PARTITION_BACKFILL, or DATE_RANGE_BACKFILL request.
    """
    specs = []
    for notebook_path, data in refs.items():
        for entry in data.get("tables", []):
            if not isinstance(entry, dict) or entry.get("usage") == "write_create":
                continue
            filters = entry.get("filters") or []
            predicate = _safe_scope_predicate(filters)
            table = entry.get("table")
            info = table_results.get(table, {})
            if predicate and info.get("exists") is True:
                specs.append({"key": _scope_key(notebook_path, "table", table, filters),
                              "type": "table", "name": info.get("resolved", table),
                              "predicate": predicate})
        for entry in data.get("paths", []):
            if not isinstance(entry, dict) or entry.get("usage") == "write":
                continue
            filters = entry.get("filters") or []
            predicate = _safe_scope_predicate(filters)
            source = entry.get("path")
            mapped = source if str(source).startswith("oci://") else map_to_oci_path(source).get("oci_path")
            info = oci_results.get(mapped, {}) if mapped else {}
            source_format = entry.get("source_format")
            if predicate and mapped and source_format and info.get("exists") is True:
                specs.append({"key": _scope_key(notebook_path, "path", source, filters),
                              "type": "path", "name": mapped,
                              "format": source_format, "predicate": predicate})
    if not specs:
        return {}

    results = {}
    stage = probe_stage
    batch_size = 5
    total_batches = (len(specs) + batch_size - 1) // batch_size
    for batch_number, start in enumerate(range(0, len(specs), batch_size), 1):
        batch = specs[start:start + batch_size]
        operation = (
            f"Scoped data availability batch {batch_number}/{total_batches} "
            f"({len(batch)} scopes)"
        )
        try:
            batch_results = await _execute_json_probe_with_recovery(
                session,
                f"""
import json
specs = {json.dumps(batch)}
results = {{}}
for spec in specs:
    try:
        if spec["type"] == "table":
            frame = spark.table(spec["name"])
        else:
            frame = spark.read.format(spec["format"]).load(spec["name"])
        has_rows = frame.where(spec["predicate"]).limit(1).count() > 0
        results[spec["key"]] = {{"base_exists": True, "scope_exists": has_rows,
                                 "predicate": spec["predicate"]}}
    except Exception as exc:
        results[spec["key"]] = {{"base_exists": True, "scope_exists": None,
                                 "predicate": spec["predicate"],
                                 "error": str(exc)[:300]}}
print(json.dumps(results))
""",
                operation,
                timeout=300,
                probe_stage=stage,
            )
            results.update(batch_results)
        except Exception as exc:
            if stage is None:
                raise
            names = ", ".join(spec["name"] for spec in batch)
            tprint(f"  [UNVERIFIED] {operation}: {names}: {exc}")
            for spec in batch:
                results[spec["key"]] = {
                    "base_exists": True,
                    "scope_exists": None,
                    "probe_status": "UNVERIFIED",
                    "predicate": spec["predicate"],
                    "error": str(exc),
                    "operation": operation,
                }
    return results

def _scala_class_forname_candidates(cls: str) -> list:
    """Return the ordered list of Class.forName candidates to try for `cls`.

    The extractor's `spark._jvm.<chain>` regex greedily captures method
    invocations and static-field / nested-object accesses alongside the class
    name, e.g. `spark._jvm.com.foo.Utils.getDate(ts)` yields
    ``"com.foo.Utils.getDate"`` — a method chain that Class.forName can never
    resolve. To handle that (plus Scala-object.method chains like
    ``functions.col``) without a fragile lowercase-tail heuristic in the
    extractor, also try the one-segment-shorter form.

    Candidates, in order:
      1. `cls`             — as extracted (matches bare classes)
      2. `cls$`            — Scala object form for `cls`
      3. `cls[:-last]`     — parent (matches method / static-field chains) if ≥ 2 segments remain
      4. `cls[:-last]$`    — Scala object form for the parent
    """
    cands = [cls, cls + "$"]
    if "." in cls:
        trimmed = cls.rsplit(".", 1)[0]
        if "." in trimmed:  # need at least X.Y to be a plausible FQN
            cands.extend([trimmed, trimmed + "$"])
    return cands


async def check_libraries(session, python_imports: list, scala_imports: list) -> dict:
    """Check library availability on cluster. Auto-installs missing Python packages via pip.

    Returns {"python": {module: {"available": bool, "installed": bool, "error": str}},
             "scala":  {class:  {"available": bool, "error": str}}}
    Only missing/failed entries are returned — available packages are omitted from report.
    """
    result = {"python": {}, "scala": {}}
    if not python_imports and not scala_imports:
        return result

    py_json = json.dumps(python_imports)
    sc_json = json.dumps(scala_imports)
    check_result = await session.execute(f"""
import json, importlib, subprocess

py_imports = {py_json}
sc_imports = {sc_json}
results = {{"python": {{}}, "scala": {{}}}}

# Fix 7j (2026-07-25): keep BOTH the top-level dedup (for install decisions —
# submodules like slack_sdk.errors share the same pip package as slack_sdk)
# AND the FULL dotted list (for submodule-presence checks). Without the full
# list, `from HelperFunctions.constants import *` passes the top-level check
# because `HelperFunctions` folder exists as a workspace package — but the
# `.constants` submodule is missing, and the notebook fails at runtime with
# ModuleNotFoundError. That's a pilot-customer AutoRefit APP_SOURCE_NAME cascade root
# cause: sibling constants.py wasn't exported (Fix 7i in dbx_export).
py_imports_full = sorted(set(py_imports))
py_imports = sorted({{m.split(".")[0] for m in py_imports}})

# Step 1: Check Python imports, collect missing
missing_py = []
for mod in py_imports:
    top = mod.split(".")[0]
    try:
        importlib.import_module(top)
    except ImportError:
        missing_py.append(mod)
    except Exception:
        missing_py.append(mod)

# Step 2: Try pip install for missing packages
installed = []
for mod in missing_py:
    top = mod.split(".")[0]
    # Map common import names to pip package names
    pip_name = {{"sklearn": "scikit-learn", "cv2": "opencv-python",
                "pil": "Pillow", "yaml": "pyyaml", "attr": "attrs",
                "dateutil": "python-dateutil", "bs4": "beautifulsoup4",
                "crypto": "pycryptodome"}}.get(top.lower(), top)
    try:
        r = subprocess.run(["pip", "install", "--quiet", pip_name],
                           capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            installed.append(mod)
    except Exception:
        pass

# Step 3: Re-check everything that was missing
for mod in missing_py:
    top = mod.split(".")[0]
    try:
        importlib.import_module(top)
        if mod in installed:
            results["python"][mod] = {{"available": True, "installed": True}}
        else:
            results["python"][mod] = {{"available": True}}
    except ImportError as e:
        results["python"][mod] = {{"available": False, "error": str(e)[:200]}}
    except Exception as e:
        results["python"][mod] = {{"available": False, "error": str(e)[:200]}}

# Step 4: (Fix 7j 2026-07-25) submodule-presence check.
# For every FULL dotted import like `pkg.sub.leaf`, try to import the full
# path. If the top-level succeeded but the full path fails, the submodule
# is MISSING (e.g. `HelperFunctions.constants` — the folder was exported
# but the .py wasn't). We can't auto-install submodules (they'd come from
# the same package as the top-level), so this is a hard-fail signal for
# the operator to act on: export the missing sibling .py OR provide the
# submodule via requirements.txt.
# Skips: single-segment names (already handled by top-level step) and
# entries whose top-level failed (submodule failure is implied by parent
# failure; no need to re-report).
for full in py_imports_full:
    if "." not in full:
        continue
    top = full.split(".")[0]
    # If top-level failed OR was never checked (available), the submodule
    # status is implied — skip to avoid noisy duplicate entries.
    if top in results["python"] and not results["python"][top].get("available", True):
        continue
    try:
        importlib.import_module(full)
    except ImportError as e:
        # Distinguish "submodule doesn't exist" from "top-level is fine but
        # this specific leaf isn't shipped". Reporter treats it as a hard
        # miss requiring an upstream fix.
        results["python"][full] = {{
            "available": False,
            "submodule_missing": True,
            "error": str(e)[:200],
        }}
    except Exception as e:
        # Other import-time errors (module runs code at import time and
        # raises) — surface them separately since they're a different
        # class of failure.
        results["python"][full] = {{
            "available": False,
            "import_error": True,
            "error": str(e)[:200],
        }}

# Scala/JVM classes — no auto-install possible. Class.forName cannot resolve
# wildcard/group imports or Scala type aliases (e.g. DataFrame = Dataset[Row]),
# and core platform namespaces are always present on the cluster — verify only
# genuine third-party classes to avoid false "missing" reports.
_CORE_JVM = ("java.", "javax.", "scala.", "org.apache.spark.", "spark.")
for cls in sc_imports:
    # Wildcard (pkg._), group (pkg.{{A,B}} -> captured as "pkg.") and trailing-dot
    # imports name a package, not a class — skip.
    if cls.endswith(".") or cls.endswith("._") or "{{" in cls or "}}" in cls:
        continue
    # Core platform classes are guaranteed present.
    if cls.startswith(_CORE_JVM):
        continue
    # Candidate list — MUST match _scala_class_forname_candidates() on the
    # laptop side (that helper is the unit-tested source of truth). Handles
    # extractor-captured method chains + Scala-object.method forms.
    _cands = [cls, cls + "$"]
    if "." in cls:
        _trim = cls.rsplit(".", 1)[0]
        if "." in _trim:
            _cands += [_trim, _trim + "$"]
    loaded = False
    _context_loader = (
        spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    )
    for cand in _cands:
        try:
            # AIDP/Spark attaches user JARs through the Spark context classloader.
            # Plain Class.forName can report a false miss even though notebook JVM
            # code can load the class normally.
            _context_loader.loadClass(cand)
            loaded = True
            break
        except Exception:
            try:
                spark._jvm.java.lang.Class.forName(cand)
                loaded = True
                break
            except Exception:
                pass
    if not loaded:
        results["scala"][cls] = {{"available": False, "error": f"Class not found: {{cls}}"}}

print(json.dumps(results))
""", timeout=300)
    output = unwrap_outputs(check_result.get("outputs", []))
    return require_cluster_json(output, "Library availability check")


# ─── Report Generation ──────────────────────────────────────────────

def tprint(*args, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


def generate_report(refs: dict, table_results: dict, oci_results: dict,
                    local_results: dict, lib_results: dict, job_name: str,
                    scope_results: dict = None,
                    mlflow_results: dict = None) -> dict:
    """Generate per-notebook data readiness report.

    Returns report dict and prints human-readable summary.
    """
    scope_results = scope_results or {}
    mlflow_results = mlflow_results or {}
    report = {
        "job_name": job_name,
        "generated_at": datetime.now().isoformat(),
        "summary": {"tables_found": 0, "tables_missing": 0,
                     "tables_unverified": 0,
                     "paths_found": 0, "paths_missing": 0,
                     "paths_unverified": 0,
                     "scopes_unverified": 0,
                     "paths_unmapped": 0, "paths_access_errors": 0,
                     "unresolved": 0,
                     "mlflow_artifacts_found": 0,
                     "mlflow_artifacts_missing": 0,
                     "mlflow_artifact_probe_errors": 0,
                     "libs_installed": 0, "libs_missing": 0,
                     "libs_unverified": 0},
        "notebooks": {},
    }
    if lib_results.get("probe_plan"):
        report["library_checks"] = [
            {
                "cluster_id": cluster_id,
                "notebooks": probe.get("notebooks", []),
                "python_imports_checked": len(probe.get("python", [])),
                "jvm_classes_checked": len(probe.get("scala", [])),
            }
            for cluster_id, probe in lib_results["probe_plan"].items()
        ]

    for nb_path, data in refs.items():
        notebook_lib_results = (
            lib_results.get("by_notebook", {}).get(nb_path, {})
            if "by_notebook" in lib_results
            else lib_results
        )
        py_lib_results = notebook_lib_results.get("python", {})
        sc_lib_results = notebook_lib_results.get("scala", {})
        glob_reads_by_path = {}
        for ref in data.get("read_references", []):
            if ref.get("api") in {"glob.glob", "glob.iglob"}:
                glob_reads_by_path.setdefault(ref.get("object_name"), []).append(ref)
        nb_name = os.path.basename(nb_path)
        nb_report = {"read_tables": [], "read_paths": [], "unresolved": data.get("unresolved", []),
                     "unresolved_scopes": data.get("unresolved_scopes", []),
                     "analysis_errors": data.get("analysis_errors", []),
                     "not_analyzed": data.get("not_analyzed", []),
                     "unresolved_mlflow_artifacts": data.get(
                         "unresolved_mlflow_artifacts", []
                     ),
                     "write_tables": [], "write_paths": [], "write_references": [],
                     "excluded_reads": data.get("excluded_reads", []),
                     "libraries": []}

        # Tables
        for t_entry in data.get("tables", []):
            table = t_entry["table"] if isinstance(t_entry, dict) else t_entry
            usage = t_entry.get("usage", "unknown") if isinstance(t_entry, dict) else "unknown"
            if usage == "write_create":
                nb_report["write_tables"].append(table)
                if isinstance(t_entry, dict):
                    nb_report["write_references"].append({
                        "object_type": "TABLE", "object_name": table,
                        **{key: value for key, value in t_entry.items() if key != "table"},
                    })
                continue

            info = table_results.get(table, {})
            entry = {"table": table, "exists": info.get("exists", False)}
            if isinstance(t_entry, dict):
                for key in ("filters", "observed_filters", "partition_column", "partition_value",
                            "start_date", "end_date", "source_format",
                            "cell_number", "line_number", "filter_resolution"):
                    if t_entry.get(key) is not None:
                        entry[key] = t_entry[key]
                filters = t_entry.get("filters") or []
                scoped = scope_results.get(_scope_key(nb_path, "table", table, filters))
                if scoped:
                    entry["base_exists"] = scoped.get("base_exists", entry["exists"])
                    entry["scope_exists"] = scoped.get("scope_exists")
                    entry["scope_predicate"] = scoped.get("predicate")
                    if scoped.get("scope_exists") is not None:
                        entry["exists"] = scoped["scope_exists"]
                    if scoped.get("error"):
                        entry["scope_check_error"] = scoped["error"]
                    scope_unverified = (
                        scoped.get("probe_status") == "UNVERIFIED"
                        or (
                            scoped.get("scope_exists") is None
                            and bool(scoped.get("error"))
                        )
                    )
                    if scope_unverified:
                        entry["exists"] = "unverified"
                        entry["scope_probe_status"] = "UNVERIFIED"
                        report["summary"]["scopes_unverified"] += 1
            # resolved_name only when it actually differs (e.g. resolved via default.)
            if info.get("resolved") and info["resolved"] != table:
                entry["resolved_name"] = info["resolved"]
            if info.get("columns"):
                entry["columns"] = info["columns"]
            if info.get("error"):
                entry["error"] = info["error"]
            if info.get("probe_status") == "UNVERIFIED":
                entry["probe_status"] = "UNVERIFIED"
            nb_report["read_tables"].append(entry)

            if entry.get("exists") is True:
                report["summary"]["tables_found"] += 1
            elif entry.get("exists") == "unverified":
                report["summary"]["tables_unverified"] += 1
            else:
                report["summary"]["tables_missing"] += 1

        # Paths
        for p_entry in data.get("paths", []):
            path = p_entry["path"] if isinstance(p_entry, dict) else p_entry
            usage = p_entry.get("usage", "unknown") if isinstance(p_entry, dict) else "unknown"
            if usage == "write":
                nb_report["write_paths"].append(path)
                if isinstance(p_entry, dict):
                    nb_report["write_references"].append({
                        "object_type": "PATH", "object_name": path,
                        **{key: value for key, value in p_entry.items() if key != "path"},
                    })
                continue

            entry = {"source": path}
            if isinstance(p_entry, dict):
                for key in ("filters", "observed_filters", "partition_column", "partition_value",
                            "start_date", "end_date", "source_format",
                            "cell_number", "line_number", "filter_resolution"):
                    if p_entry.get(key) is not None:
                        entry[key] = p_entry[key]

            if path.startswith("oci://"):
                info = oci_results.get(path, {})
                entry["type"] = "oci"
                entry["oci_path"] = path
                entry["exists"] = info.get("exists", False)
                if info.get("items"):
                    entry["items"] = info["items"]
                if info.get("error"):
                    entry["error"] = info["error"]
                if info.get("probe_status") == "UNVERIFIED":
                    entry["probe_status"] = "UNVERIFIED"
                if entry["exists"] in _PROBE_FAILURE_STATUSES:
                    entry["probe_status"] = _PROBE_FAILURE_STATUSES[entry["exists"]]
            elif path.startswith("/Volumes/") or path.startswith("/Workspace/"):
                info = local_results.get(path, {})
                entry["type"] = "volume" if path.startswith("/Volumes/") else "workspace"
                entry["exists"] = info.get("exists", False)
                if info.get("items"):
                    entry["items"] = info["items"]
                if info.get("error"):
                    entry["error"] = info["error"]
                if info.get("probe_status") == "UNVERIFIED":
                    entry["probe_status"] = "UNVERIFIED"
            else:
                # S3/DBFS/mnt — map to OCI
                mapped = map_to_oci_path(path)
                entry["type"] = "s3" if "s3" in path else "dbfs" if "dbfs" in path else "mnt"
                entry["oci_path"] = mapped["oci_path"]
                if mapped["oci_path"]:
                    info = oci_results.get(mapped["oci_path"], {})
                    entry["exists"] = info.get("exists", False)
                    if info.get("items"):
                        entry["items"] = info["items"]
                    if info.get("error"):
                        entry["error"] = info["error"]
                    if info.get("probe_status") == "UNVERIFIED":
                        entry["probe_status"] = "UNVERIFIED"
                    if entry["exists"] in _PROBE_FAILURE_STATUSES:
                        entry["probe_status"] = _PROBE_FAILURE_STATUSES[entry["exists"]]
                else:
                    entry["exists"] = "unmapped"
                    report["summary"]["paths_unmapped"] += 1
            if isinstance(p_entry, dict) and p_entry.get("filters"):
                scoped = scope_results.get(
                    _scope_key(nb_path, "path", path, p_entry["filters"]))
                if scoped:
                    entry["base_exists"] = scoped.get("base_exists", entry.get("exists"))
                    entry["scope_exists"] = scoped.get("scope_exists")
                    entry["scope_predicate"] = scoped.get("predicate")
                    if scoped.get("scope_exists") is not None:
                        entry["exists"] = scoped["scope_exists"]
                    if scoped.get("error"):
                        entry["scope_check_error"] = scoped["error"]
                    scope_unverified = (
                        scoped.get("probe_status") == "UNVERIFIED"
                        or (
                            scoped.get("scope_exists") is None
                            and bool(scoped.get("error"))
                        )
                    )
                    if scope_unverified:
                        entry["exists"] = "unverified"
                        entry["scope_probe_status"] = "UNVERIFIED"
                        report["summary"]["scopes_unverified"] += 1

            # Directory-prefix probes cannot establish Python glob matches.
            # Consult all read sites: classification can select another API
            # when a literal/Spark read shares this same path.
            glob_reads = glob_reads_by_path.get(path, [])
            if (not glob_reads and isinstance(p_entry, dict)
                    and p_entry.get("api") in {"glob.glob", "glob.iglob"}):
                glob_reads = [{**p_entry, "object_name": path}]
            if glob_reads:
                entry["glob_match_status"] = "UNVERIFIED"
                if (entry.get("exists") != "unmapped"
                        and entry.get("exists") not in _PROBE_FAILURE_STATUSES):
                    entry["exists"] = "unverified"
                    entry["probe_status"] = "UNVERIFIED"
                for ref in glob_reads:
                    finding = {
                        **ref, "kind": "UNVERIFIED_GLOB_MATCH",
                        "reason": (
                            "Python glob matches were not checked; directory availability "
                            "does not establish matching inputs. Manual review required."
                        ),
                    }
                    if finding not in nb_report["unresolved_scopes"]:
                        nb_report["unresolved_scopes"] = nb_report["unresolved_scopes"] + [finding]

            if entry.get("exists") is True:
                report["summary"]["paths_found"] += 1
            elif entry.get("exists") == "unverified":
                report["summary"]["paths_unverified"] += 1
            elif entry.get("exists") in _PROBE_FAILURE_STATUSES:
                # Probe failed — the object may exist; counting this as
                # "missing" pointed stakeholders at a data-copy request that
                # cannot fix an access failure (REF-257).
                report["summary"]["paths_access_errors"] += 1
            elif entry.get("exists") != "unmapped":
                report["summary"]["paths_missing"] += 1

            nb_report["read_paths"].append(entry)

        # MLflow artifacts use canonical target-neutral URIs. Keeping them in
        # read_paths lets the existing tracker planner reuse FILE/DIRECTORY
        # rows without a table schema change.
        for artifact in data.get("mlflow_artifacts", []):
            uri = artifact.get("artifact_uri")
            if not uri:
                continue
            info = mlflow_results.get(uri, {})
            entry = {
                "source": uri,
                "type": "mlflow",
                "source_system": "MLFLOW",
                "oci_path": uri,
                "source_format": "MLFLOW",
                "object_type": artifact.get("object_type", "DIRECTORY"),
                "exists": info.get("exists", False),
                "run_id": artifact.get("run_id"),
                "artifact_path": artifact.get("artifact_path"),
                "api": artifact.get("api"),
                "cell_number": artifact.get("cell_number"),
                "line_number": artifact.get("line_number"),
            }
            if info.get("items") is not None:
                entry["items"] = info["items"]
            if info.get("error"):
                entry["error"] = info["error"]
            if info.get("probe_error"):
                entry["probe_error"] = True
                report["summary"]["mlflow_artifact_probe_errors"] += 1
            if entry["exists"] is True:
                report["summary"]["paths_found"] += 1
                report["summary"]["mlflow_artifacts_found"] += 1
            elif entry["exists"] == "unverified":
                entry["probe_status"] = "UNVERIFIED"
                report["summary"]["paths_unverified"] += 1
            else:
                report["summary"]["paths_missing"] += 1
                report["summary"]["mlflow_artifacts_missing"] += 1
            nb_report["read_paths"].append(entry)

        for read in data.get("external_reads", []):
            _append_external_read_report(nb_report, report["summary"], read,
                                         table_results, oci_results, local_results)

        # Libraries — only report items that needed attention
        for mod in data.get("python_imports", []):
            info = py_lib_results.get(mod)
            if info is None:
                # Was already available — skip from report
                continue
            entry = {"module": mod, "type": "python", "available": info.get("available", False)}
            if info.get("installed"):
                entry["installed"] = True
                report["summary"]["libs_installed"] += 1
            if info.get("error"):
                entry["error"] = info["error"]
            if info.get("checked_clusters"):
                entry["checked_clusters"] = info["checked_clusters"]
            if info.get("cluster_results"):
                entry["cluster_results"] = info["cluster_results"]
            if info.get("unverified"):
                entry["unverified"] = True
            # Fix 7j: surface submodule-specific failure kind so operator sees
            # `mod` failed because the SUBMODULE isn't installed (a different
            # class of miss than "package X not on cluster") — needs an
            # upstream fix (export the sibling .py, or ship as part of the
            # package), not a pip install of X.
            if info.get("submodule_missing"):
                entry["submodule_missing"] = True
            if info.get("import_error"):
                entry["import_error"] = True
            nb_report["libraries"].append(entry)
            if info.get("unverified"):
                report["summary"]["libs_unverified"] += 1
            elif not info.get("available"):
                report["summary"]["libs_missing"] += 1
        for cls in data.get("scala_imports", []):
            info = sc_lib_results.get(cls)
            if info is None:
                # Was already on classpath — skip from report
                continue
            entry = {"module": cls, "type": "scala", "available": info.get("available", False)}
            if info.get("error"):
                entry["error"] = info["error"]
            if info.get("checked_clusters"):
                entry["checked_clusters"] = info["checked_clusters"]
            if info.get("cluster_results"):
                entry["cluster_results"] = info["cluster_results"]
            if info.get("unverified"):
                entry["unverified"] = True
            nb_report["libraries"].append(entry)
            if info.get("unverified"):
                report["summary"]["libs_unverified"] += 1
            elif not info.get("available"):
                report["summary"]["libs_missing"] += 1

        report["summary"]["unresolved"] += (
            len(nb_report["unresolved"]) + len(nb_report["unresolved_scopes"])
            + len(nb_report["unresolved_mlflow_artifacts"])
        )
        # Only include notebooks that actually have something to report — skip
        # the ones whose buckets are all empty (most notebooks) to cut noise.
        if any(nb_report[k] for k in ("read_tables", "read_paths", "write_tables",
                                      "write_paths", "unresolved", "unresolved_scopes",
                                      "unresolved_mlflow_artifacts", "libraries", "excluded_reads")):
            report["notebooks"][nb_path] = nb_report

    _aggregate_shared_access_failures(report)
    return report


def _aggregate_shared_access_failures(report: dict) -> None:
    """Surface probe access failures grouped by bucket@namespace (REF-257).

    The incident's report carried the root cause only as per-path error
    strings nothing read; Phase 2 then rediscovered the shared failure as
    404 BucketNotFound. One explicit authority-level finding names the
    shared root, its failure class, the preserved cause line, and every
    affected input — loud in both the report and the run output.
    """
    groups: dict = {}
    for nb_path, nb in (report.get("notebooks") or {}).items():
        for entry in nb.get("read_paths") or []:
            status = entry.get("probe_status")
            if status not in _PROBE_FAILURE_STATUSES.values():
                continue
            target = str(entry.get("oci_path") or entry.get("source") or "")
            if "://" not in target:
                continue
            authority = target.split("/", 3)[2] if target.count("/") >= 2 else target
            group = groups.setdefault(authority, {
                "authority": authority, "failure_classes": set(),
                "affected_paths": [], "cause": ""})
            group["failure_classes"].add(status)
            if target not in group["affected_paths"]:
                group["affected_paths"].append(target)
            if not group["cause"] and entry.get("error"):
                group["cause"] = str(entry["error"])[:400]
    if not groups:
        return
    findings = []
    for group in groups.values():
        finding = {**group, "failure_classes": sorted(group["failure_classes"])}
        findings.append(finding)
        tprint(f"  [ACCESS-FAILURE] shared bucket/prefix access failure: "
               f"{finding['authority']} ({', '.join(finding['failure_classes'])}) — "
               f"{len(finding['affected_paths'])} input(s) affected; "
               f"cause: {finding['cause'][:200]}")
    report["shared_access_failures"] = findings
    report["summary"]["shared_access_failures"] = len(findings)


def _append_external_read_report(notebook, summary, read, tables, paths, local_paths):
    """Keep a resolved connector read's identity through reporting."""
    entry = {**read, "source_format": read["source_connector"]}
    if read["object_type"] == "table":
        entry["table"] = read["source_object"]
        target = read.get("target_object")
        if not target:
            # No AIDP name is known for this connector's copy, so it is never
            # probed. Name the source for manual review instead of guessing one.
            entry.update(exists="unverified", probe_status="UNVERIFIED",
                         error="AIDP copy not checked: no known target for this connector source")
        else:
            info = tables.get(target, {})
            entry.update({key: info[key] for key in ("error", "columns") if key in info})
            entry["resolved_name"] = info.get("resolved") or target
            entry["exists"] = info.get("exists", "unverified")
            if entry["exists"] not in (True, False):
                entry["probe_status"] = "UNVERIFIED"
        counter = ("found" if entry["exists"] is True else
                   "missing" if entry["exists"] is False else "unverified")
        summary[f"tables_{counter}"] += 1
        notebook["read_tables"].append(entry)
        return
    entry.update(source=read["source_object"], type="connector")
    target = read.get("target_object")
    if not target:
        entry["exists"] = "unmapped"
        summary["paths_unmapped"] += 1
    else:
        results = local_paths if target.startswith(("/Volumes/", "/Workspace/")) else paths
        info = results.get(target, {})
        entry.update({key: info[key] for key in ("error", "probe_status", "items")
                      if key in info})
        entry["exists"] = info.get("exists", "unverified")
        if info.get("error"):
            entry["error"] = info["error"]
        # REF-257: a connector-resolved read against a failed target keeps the
        # access-failure diagnosis instead of degrading to UNVERIFIED.
        if entry["exists"] in _PROBE_FAILURE_STATUSES:
            entry["probe_status"] = _PROBE_FAILURE_STATUSES[entry["exists"]]
            summary["paths_access_errors"] += 1
        else:
            counter = ("found" if entry["exists"] is True else
                       "missing" if entry["exists"] is False else "unverified")
            summary[f"paths_{counter}"] += 1
            if entry["exists"] not in (True, False):
                entry["probe_status"] = "UNVERIFIED"
        if target.startswith("oci://"):
            entry["oci_path"] = target
    notebook["read_paths"].append(entry)


def attach_data_migration_requirements(report: dict, job_id: str) -> dict:
    """Embed the canonical, table-loader-neutral migration contract.

    The availability report is always produced first.  This pure planning
    step then derives requirements from that report and stores them under the
    versioned ``data_migration`` key.  Database loaders consume this section
    later and remain optional plug-ins.
    """
    from build_proactive_data_migration_plan import build_plan

    identifier = str(job_id or "").strip()
    if not identifier or identifier == "0":
        report.setdefault("warnings", []).append(
            "Data-migration requirements were not attached because the "
            "manifest has no source Databricks job identifier. The data "
            "availability report remains valid; pass --databricks-job-id to "
            "enable tracker-plan generation."
        )
        return report
    report["databricks_job_id"] = identifier
    report["data_migration"] = build_plan(report, identifier)
    return report


def print_report(report: dict):
    """Print human-readable report to console."""
    summary = report["summary"]

    for warning in report.get("warnings", []):
        print(f"WARNING: {warning}")

    for nb_path, nb_data in report["notebooks"].items():
        nb_name = os.path.basename(nb_path)
        tables = nb_data.get("read_tables", [])
        all_paths = nb_data.get("read_paths", [])
        mlflow_artifacts = [p for p in all_paths if p.get("type") == "mlflow"]
        paths = [p for p in all_paths if p.get("type") != "mlflow"]
        unresolved = nb_data.get("unresolved", [])
        unresolved_scopes = nb_data.get("unresolved_scopes", [])
        unresolved_mlflow = nb_data.get("unresolved_mlflow_artifacts", [])
        libraries = nb_data.get("libraries", [])
        excluded_reads = nb_data.get("excluded_reads", [])

        if not tables and not paths and not mlflow_artifacts and not unresolved \
                and not unresolved_scopes and not unresolved_mlflow and not libraries and not excluded_reads:
            continue

        print(f"\n{'─'*60}")
        print(f"  {nb_name}")
        print(f"{'─'*60}")

        if tables:
            print(f"\n  TABLES ({len(tables)}):")
            for t in tables:
                table = t["table"]
                resolved = t.get("resolved_name", "")
                resolved_str = f" → {resolved}" if resolved and resolved != table else ""
                if t.get("exists") is True:
                    cols = t.get("columns", "?")
                    print(f"    EXISTS ({cols} cols)  {table}{resolved_str}")
                elif t.get("exists") == "unverified":
                    print(f"    UNVERIFIED       {table}{resolved_str}")
                    if t.get("error"):
                        print(f"                     {t['error']}")
                elif t.get("error"):
                    print(f"    ERROR            {table}{resolved_str}")
                    print(f"                     {t['error']}")
                else:
                    print(f"    MISSING          {table}{resolved_str}")
                if t.get("scope_probe_status") == "UNVERIFIED":
                    print("                     FILTER/PARTITION SCOPE UNVERIFIED")
                if t.get("source_connector"):
                    print(f"                     Connector: {t['source_connector']}")

        if paths:
            print(f"\n  STORAGE PATHS ({len(paths)} reads):")
            for p in paths:
                source = p["source"]
                oci = p.get("oci_path", "")
                exists = p.get("exists", False)

                if p.get("type") in ("s3", "dbfs", "mnt"):
                    print(f"    Source: {source}")
                    if oci:
                        status = _render_path_status(exists)
                        items = f" ({p['items']} items)" if p.get("items") else ""
                        print(f"    OCI:    {oci}")
                        print(f"    Status: {status}{items}")
                    else:
                        print(f"    OCI:    UNMAPPED (bucket not in mapping)")
                    print()
                elif p.get("type") == "oci":
                    status = _render_path_status(exists)
                    items = f" ({p['items']} items)" if p.get("items") else ""
                    print(f"    {status}{items}  {source}")
                elif p.get("type") == "connector":
                    status = ("UNMAPPED" if exists == "unmapped"
                              else _render_path_status(exists))
                    print(f"    {status}  {source}")
                    print(f"      Checked: {p.get('target_object') or 'UNMAPPED (not in bucket mapping)'}")
                else:
                    status = "EXISTS" if exists is True else "UNVERIFIED" if exists == "unverified" else "TIMEOUT" if exists == "timeout" else "MISSING"
                    items = f" ({p['items']} items)" if p.get("items") else ""
                    print(f"    {status}{items}  {source}")
                if p.get("scope_probe_status") == "UNVERIFIED":
                    print("                     FILTER/PARTITION SCOPE UNVERIFIED")

        if mlflow_artifacts:
            print(f"\n  MLFLOW ARTIFACTS ({len(mlflow_artifacts)}):")
            for artifact in mlflow_artifacts:
                if artifact.get("probe_error"):
                    status = "PROBE ERROR"
                else:
                    status = "EXISTS" if artifact.get("exists") is True else "MISSING"
                print(
                    f"    {status:<11} [{artifact.get('object_type', 'DIRECTORY')}] "
                    f"{artifact['source']}"
                )
                if artifact.get("error"):
                    print(f"                {artifact['error']}")

        # Write targets (create-style, not checked — table will be created)
        write_tables = nb_data.get("write_tables", [])
        write_paths = nb_data.get("write_paths", [])
        if write_tables or write_paths:
            print(f"\n  WRITE TARGETS (create-style, not checked):")
            for t in write_tables:
                print(f"    TABLE  {t}")
            for p in write_paths:
                print(f"    PATH   {p}")

        if unresolved:
            print(f"\n  UNRESOLVED ({len(unresolved)}) — review source/parameters:")
            for u in unresolved:
                print(f"    {u}")

        if unresolved_scopes:
            print(f"\n  UNRESOLVED SCOPES ({len(unresolved_scopes)}) — review before loading:")
            for scope in unresolved_scopes:
                print(f"    {scope}")
        if excluded_reads:
            print(f"\n  EXCLUDED READ SITES ({len(excluded_reads)}):")
            for site in excluded_reads:
                print(f"    cell {site['cell_number']}, line {site['line_number']}: {site['reason']}")

        if unresolved_mlflow:
            print(
                f"\n  UNRESOLVED MLFLOW ARTIFACTS ({len(unresolved_mlflow)}) "
                "— review runtime parameters:"
            )
            for artifact in unresolved_mlflow:
                print(f"    {artifact}")

        # Libraries — only show items that needed attention
        unverified_libs = [l for l in libraries if l.get("unverified")]
        missing_libs = [
            l for l in libraries
            if not l.get("available") and not l.get("unverified")
        ]
        installed_libs = [l for l in libraries if l.get("available") and l.get("installed")]
        if missing_libs:
            print(f"\n  LIBRARIES — MISSING ({len(missing_libs)}):")
            for l in missing_libs:
                ltype = l["type"].upper()
                print(f"    MISSING  [{ltype}]  {l['module']}")
                if l.get("error"):
                    print(f"             {l['error']}")
        if installed_libs:
            print(f"\n  LIBRARIES — auto-installed ({len(installed_libs)}):")
            for l in installed_libs:
                print(f"    INSTALLED  [PYTHON]  {l['module']}")
        if unverified_libs:
            print(f"\n  LIBRARIES — UNVERIFIED ({len(unverified_libs)}):")
            for l in unverified_libs:
                print(f"    UNVERIFIED  [{l['type'].upper()}]  {l['module']}")
                if l.get("error"):
                    print(f"                {l['error']}")

    # Overall summary
    print(f"\n{'='*60}")
    print(f"  DATA AVAILABILITY SUMMARY: {report['job_name']}")
    print(f"{'='*60}")
    print(
        f"  Tables:     {summary['tables_found']} found, "
        f"{summary['tables_missing']} missing, "
        f"{summary.get('tables_unverified', 0)} unverified"
    )
    print(
        f"  Paths:      {summary['paths_found']} found, "
        f"{summary['paths_missing']} missing, "
        f"{summary['paths_unmapped']} unmapped, "
        f"{summary.get('paths_unverified', 0)} unverified"
    )
    print(f"  Scopes:     {summary.get('scopes_unverified', 0)} unverified")
    print(
        f"  MLflow:     {summary['mlflow_artifacts_found']} found, "
        f"{summary['mlflow_artifacts_missing']} missing, "
        f"{summary['mlflow_artifact_probe_errors']} probe errors"
    )
    print(
        f"  Libraries:  {summary['libs_installed']} auto-installed, "
        f"{summary['libs_missing']} missing, "
        f"{summary.get('libs_unverified', 0)} unverified"
    )
    print(f"  Unresolved: {summary['unresolved']} (source/parameter/analysis gaps)")
    migration = report.get("data_migration") or {}
    migration_summary = migration.get("summary") or {}
    if migration_summary:
        print(f"  Migration:  {migration_summary.get('requirements', 0)} requirements "
              f"({migration_summary.get('full', 0)} full, "
              f"{migration_summary.get('partial', 0)} partial, "
              f"{migration_summary.get('mapping_required', 0)} mapping required)")
        print(f"  Loadable:   {'yes' if migration.get('loadable') else 'no — numeric Databricks job ID required'}")
    total_missing = summary["tables_missing"] + summary["paths_missing"]
    libs_missing = summary["libs_missing"]
    needs_review = (
        summary["unresolved"]
        + summary.get("libs_unverified", 0)
        + summary.get("tables_unverified", 0)
        + summary.get("paths_unverified", 0)
        + summary.get("scopes_unverified", 0)
        # REF-257: an access/probe failure means availability is UNKNOWN —
        # it must never let the run read "ready to migrate".
        + summary.get("paths_access_errors", 0)
    )
    if (total_missing == 0 and summary["paths_unmapped"] == 0
            and libs_missing == 0 and needs_review == 0):
        print(f"\n  RESULT: ALL DEPENDENCIES AVAILABLE — ready to migrate")
    elif (total_missing == 0 and summary["paths_unmapped"] == 0
          and libs_missing == 0):
        print(
            f"\n  RESULT: DEPENDENCIES AVAILABLE WITH {needs_review} "
            "UNRESOLVED/UNVERIFIED CHECK(S) — manual review required"
        )
    elif libs_missing > 0:
        print(f"\n  RESULT: {libs_missing} library(s) MISSING — install before migrating")
        if total_missing > 0:
            print(f"          {total_missing} data source(s) also MISSING")
    elif total_missing == 0:
        print(f"\n  RESULT: DATA AVAILABLE (but {summary['paths_unmapped']} paths unmapped — check manually)")
    else:
        print(f"\n  RESULT: {total_missing} data source(s) MISSING — review before migrating")
    print(f"{'='*60}")


# ─── Main ────────────────────────────────────────────────────────────

async def _main_impl():
    global _ACTIVE_CONTROL_SESSION
    parser = argparse.ArgumentParser(
        description="Check data availability for notebooks in an AIDP workflow")

    parser.add_argument("--job-key",
                        help="AIDP job key (UUID) from the workflow")
    parser.add_argument("--manifest",
                        help="Pre-built manifest JSON (skips API fetch)")
    parser.add_argument("--cluster", required=True,
                        help="AIDP control/data-probe cluster ID. Python and "
                             "JVM/JAR checks run on each task's job cluster.")
    parser.add_argument("--only-tasks", default="",
                        help="Comma-separated task_key substrings to check")
    parser.add_argument("--output",
                        help="Save JSON report to file (optional)")
    parser.add_argument(
        "--databricks-job-id", default="",
        help="Source Databricks job ID for the embedded data_migration contract; "
             "overrides the manifest job_id")
    parser.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE",
        help="Runtime job parameter for resolving f-strings and date filters; repeatable")
    parser.add_argument("--bucket-mapping", default=None,
                        help="Override cfg.bucket_mapping.path for this run only.")

    # AIDP environment — default None so YAML (via --config) is primary source.
    parser.add_argument("--config", default=None,
                        help="Path to aidp_migrator.yaml. Falls back to $AIDP_CONFIG_PATH, "
                             "./aidp_migrator.yaml, ~/aidp_migrator.yaml (discovery order).")
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID,
                        help="Override cfg.aidp.lake_ocid for this run only.")
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID,
                        help="Override cfg.aidp.workspace_id for this run only.")
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE,
                        help="Override cfg.aidp.oci_profile for this run only.")
    parser.add_argument(
        "--oci-config-path", default="",
        help="OCI config file path; also supports workstation execution")

    args = parser.parse_args()

    if args.oci_config_path:
        os.environ["OCI_CONFIG_FILE"] = os.path.expanduser(args.oci_config_path)

    if not args.manifest and not args.job_key:
        parser.error("Either --job-key or --manifest is required")

    # ── Load customer YAML + backfill any arg the user did not pass ──
    # CLI flag wins; YAML fills in the rest.
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
    if not args.bucket_mapping:
        args.bucket_mapping = _cfg.bucket_mapping.path
    aidp_base = _aidp_control_plane_base(_cfg.aidp.region)

    # Load bucket mapping locally or directly from the AIDP workspace.
    if args.bucket_mapping.startswith("/Workspace/") and not os.path.exists(args.bucket_mapping):
        _, signer = get_oci_signer(args.oci_profile)
        mapping_content = _download_workspace_file(
            signer,
            aidp_base,
            args.lake_ocid, args.workspace_id, args.bucket_mapping)
        load_bucket_ns_mapping_text(
            mapping_content.decode("utf-8-sig"), args.bucket_mapping
        )
    else:
        load_bucket_ns_mapping(args.bucket_mapping)

    # ── Extract notebook paths ──
    if args.manifest:
        tprint(f"Loading manifest from {args.manifest}")
        if os.path.exists(args.manifest):
            with open(args.manifest) as f:
                manifest = json.load(f)
        elif args.manifest.startswith("/Workspace/"):
            _, signer = get_oci_signer(args.oci_profile)
            manifest = _download_workspace_json(
                signer,
                aidp_base,
                args.lake_ocid, args.workspace_id, args.manifest)
        else:
            raise FileNotFoundError(args.manifest)
        job = manifest["jobs"][0]
        tasks = job["tasks"]
        job_name = job.get("job_name", "unknown")
        job_default_cluster = _task_cluster_id({
            "cluster": job.get("default_cluster")
        })
    else:
        tprint(f"Fetching workflow: {args.job_key}")
        job_data = fetch_job_definition(
            args.workspace_id, args.job_key,
            lake_ocid=args.lake_ocid, oci_profile=args.oci_profile)
        job_name = job_data.get("name", "").replace(".job", "") or args.job_key
        tasks = extract_tasks_from_job(job_data)
        job_clusters = job_data.get("jobClusters") or []
        job_default_cluster = (
            job_clusters[0].get("clusterKey") if job_clusters else ""
        )

    if args.databricks_job_id:
        migration_job_id = args.databricks_job_id
    elif args.manifest:
        migration_job_id = (
            job.get("databricks_job_id")
            or job.get("source_job_id")
            or job.get("job_id")
        )
    else:
        migration_job_id = (
            job_data.get("databricksJobId")
            or job_data.get("databricks_job_id")
            or job_data.get("sourceJobId")
            or args.job_key
        )

    runtime_parameters = {}
    parameter_source = job if args.manifest else job_data
    declared_parameters = parameter_source.get("parameters", {})
    if isinstance(declared_parameters, dict):
        runtime_parameters.update(declared_parameters)
    elif isinstance(declared_parameters, list):
        for item in declared_parameters:
            if isinstance(item, dict) and item.get("name"):
                runtime_parameters[item["name"]] = item.get("value", item.get("default", ""))
    for item in args.param:
        if "=" not in item:
            parser.error(f"Invalid --param {item!r}; expected KEY=VALUE")
        key, value = item.split("=", 1)
        runtime_parameters[key.strip()] = value

    if not tasks:
        print("ERROR: No notebook tasks found")
        sys.exit(1)

    # Filter tasks
    only_tasks = [t.strip() for t in args.only_tasks.split(",") if t.strip()] if args.only_tasks else []
    if only_tasks:
        tasks = [t for t in tasks if any(ot in t["task_key"] for ot in only_tasks)]
        tprint(f"Filtered to {len(tasks)} task(s)")

    runtime_parameters_by_notebook = build_notebook_runtime_parameters(
        tasks, runtime_parameters)
    notebook_clusters = build_notebook_cluster_map(
        tasks, job_default_cluster or args.cluster)

    # Collect notebook paths (tasks + deps, recursively flattening nested_deps)
    def _flatten_deps(dep_list, out):
        for dep in dep_list:
            dp = dep if isinstance(dep, str) else dep.get("path", "")
            if dp and dp not in out:
                out.append(dp)
            if isinstance(dep, dict) and dep.get("nested_deps"):
                _flatten_deps(dep["nested_deps"], out)

    notebook_paths = []
    for t in tasks:
        nb = t.get("notebook_path", "")
        if nb and nb not in notebook_paths:
            notebook_paths.append(nb)
        _flatten_deps(t.get("run_deps", []), notebook_paths)

    tprint(f"Workflow: {job_name} ({len(tasks)} tasks, {len(notebook_paths)} notebooks)")

    # ── Connect to cluster ──
    tprint(f"Connecting to control/data-probe cluster {args.cluster[:12]}...")
    session = AIDPSession(lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
                          cluster_id=args.cluster, oci_profile=args.oci_profile,
                          session_name=f"data_check_{job_name}")
    _ACTIVE_CONTROL_SESSION = session
    await session.connect()

    # ── Step 1: Extract data references ──
    tprint("Extracting data references from notebooks...")
    refs = await extract_data_refs(
        session,
        notebook_paths,
        aidp_base=aidp_base,
        lake_ocid=args.lake_ocid,
        workspace_id=args.workspace_id,
        oci_profile=args.oci_profile,
        runtime_parameters=runtime_parameters,
        runtime_parameters_by_notebook=runtime_parameters_by_notebook,
    )
    total_paths = sum(len(r.get("paths", [])) for r in refs.values())
    total_tables = sum(len(r.get("tables", [])) for r in refs.values())
    tprint(f"Found {total_paths} storage paths, {total_tables} table references")

    # ── Step 2: Classify read vs write ──
    tprint("Classifying read vs write operations...")
    refs = await classify_refs_on_cluster(session, notebook_paths, refs)

    # Count read-only refs
    read_tables = set()
    read_oci_paths = []
    read_local_paths = []
    read_s3_mapped_oci = []

    for nb_path, data in refs.items():
        for read in data.get("external_reads", []):
            # Probe routed paths and Scylla mirror tables; other connector
            # tables have no AIDP name and are never probed.
            target = read.get("target_object")
            if not target:
                continue
            if read["object_type"] == "table":
                read_tables.add(target)
            elif target.startswith(("/Volumes/", "/Workspace/")):
                read_local_paths.append(target)
            else:
                read_oci_paths.append(target)
        for t_entry in data.get("tables", []):
            if isinstance(t_entry, dict) and t_entry.get("usage") not in ("write_create",):
                # Check read tables AND write_insert tables (INSERT INTO needs table to exist)
                read_tables.add(t_entry["table"])
            elif isinstance(t_entry, str):
                read_tables.add(t_entry)

        for p_entry in data.get("paths", []):
            path = p_entry["path"] if isinstance(p_entry, dict) else p_entry
            usage = p_entry.get("usage", "unknown") if isinstance(p_entry, dict) else "unknown"
            if usage == "write":
                continue

            if path.startswith("oci://"):
                read_oci_paths.append(path)
            elif path.startswith("/Volumes/") or path.startswith("/Workspace/"):
                read_local_paths.append(path)
            else:
                mapped = map_to_oci_path(path)
                if mapped["oci_path"]:
                    read_s3_mapped_oci.append(mapped["oci_path"])

    # Deduplicate
    read_tables = sorted(read_tables)
    read_oci_paths = sorted(set(read_oci_paths + read_s3_mapped_oci))
    read_local_paths = sorted(set(read_local_paths))

    # Gather library imports across all notebooks (deduplicated)
    all_python_imports = set()
    all_scala_imports = set()
    all_mlflow_artifacts = []
    for nb_path, data in refs.items():
        all_python_imports.update(data.get("python_imports", []))
        all_scala_imports.update(data.get("scala_imports", []))
        all_mlflow_artifacts.extend(data.get("mlflow_artifacts", []))
    all_python_imports = sorted(all_python_imports)
    all_scala_imports = sorted(all_scala_imports)

    tprint(f"Read dependencies: {len(read_tables)} tables, "
           f"{len(read_oci_paths)} OCI paths, {len(read_local_paths)} local paths")
    tprint(f"Library imports: {len(all_python_imports)} Python, {len(all_scala_imports)} Scala/JVM")
    tprint(f"MLflow artifacts: {len({a.get('artifact_uri') for a in all_mlflow_artifacts if a.get('artifact_uri')})}")

    # ── Step 3: Check availability ──
    # Reference extraction/classification above has already warmed this kernel.
    # Reuse it for every existence probe; replace it only after a real probe
    # failure, then retain the warmed replacement for all following stages.
    def _track_active_probe(active_session):
        global _ACTIVE_CONTROL_SESSION
        _ACTIVE_CONTROL_SESSION = active_session

    probe_stage = _ProbeSessionRecovery(
        session, "availability", on_session_change=_track_active_probe
    )

    tprint("Checking table availability (DESCRIBE TABLE)...")
    table_results = await check_tables_describe(
        session, read_tables, probe_stage=probe_stage)

    tprint("Checking OCI path availability (fs.listStatus)...")
    oci_results = await check_oci_paths(
        session, read_oci_paths, probe_stage=probe_stage)

    remaining_data_probes = bool(read_local_paths or all_mlflow_artifacts)
    if remaining_data_probes:
        session = await probe_stage.current_session()
    if remaining_data_probes and session is None:
        reason = (
            "Probe stage unavailable after replacement-session failure: "
            + probe_stage.unavailable_reason
        )
        tprint(f"WARNING: {reason}. Remaining data probes are UNVERIFIED.")
        local_results = _unverified_local_results(read_local_paths, reason)
        mlflow_results = _unverified_mlflow_results(
            all_mlflow_artifacts, reason
        )
    else:
        if read_local_paths:
            tprint("Checking Volume/Workspace paths (ls)...")
        local_results = await check_local_paths(
            session, read_local_paths, probe_stage=probe_stage
        )

        if all_mlflow_artifacts:
            tprint("Checking MLflow artifact availability...")
        mlflow_results = await check_mlflow_artifacts(
            session, all_mlflow_artifacts, probe_stage=probe_stage
        )

    tprint("Checking captured filter/partition scopes...")
    scope_results = await check_scoped_references(
        session, refs, table_results, oci_results, probe_stage=probe_stage)

    # Python packages and JVM/JAR classes must be checked where each notebook
    # actually executes.  The control cluster is only the data-probe host and
    # must not determine target-job dependency readiness.
    library_plan = build_library_probe_plan(refs, notebook_clusters)
    control_library_plan = library_plan.get(args.cluster, {})
    needs_control_library_probe = any(
        control_library_plan.get(kind)
        for kind in ("python", "scala")
    )
    if needs_control_library_probe:
        session = await probe_stage.current_session()
    cluster_lib_results = await probe_libraries_on_job_clusters(
        session,
        library_plan,
        control_cluster_id=args.cluster,
        lake_ocid=args.lake_ocid,
        workspace_id=args.workspace_id,
        oci_profile=args.oci_profile,
        job_name=job_name,
        control_unavailable_reason=(
            probe_stage.unavailable_reason if session is None else None
        ),
    )

    lib_results = merge_library_results_by_notebook(
        refs, notebook_clusters, cluster_lib_results)
    lib_results["probe_plan"] = library_plan

    # ── Step 4: Generate report ──
    report = generate_report(
        refs, table_results, oci_results, local_results, lib_results,
        job_name, scope_results=scope_results,
        mlflow_results=mlflow_results)
    attach_data_migration_requirements(report, migration_job_id)
    print_report(report)

    # Save JSON report if requested
    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        tprint(f"Report saved to {args.output}")


async def main():
    """Run the check and always close the control session on every exit path."""
    global _ACTIVE_CONTROL_SESSION
    try:
        await _main_impl()
    finally:
        session = _ACTIVE_CONTROL_SESSION
        _ACTIVE_CONTROL_SESSION = None
        if session is not None:
            try:
                await session.close()
            except Exception as exc:
                tprint(
                    "WARNING: failed to close control/data-probe session: "
                    f"{type(exc).__name__}: {exc}"
                )


if __name__ == "__main__":
    asyncio.run(main())
