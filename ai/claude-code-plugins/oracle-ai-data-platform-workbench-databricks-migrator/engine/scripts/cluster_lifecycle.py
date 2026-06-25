#!/usr/bin/env python3
"""
Cluster Lifecycle Management
=============================
Start/stop/poll AIDP compute clusters and ensure migration dependencies
are installed before connecting.

Uses the private AIDP data plane APIs:
  GET  /workspaces/{ws}/clusters/{key}              — cluster details + lifecycleState
  POST /workspaces/{ws}/clusters/{key}/actions/start — start cluster
  POST /workspaces/{ws}/clusters/{key}/actions/stop  — stop cluster
  GET  /workspaces/{ws}/clusters/{key}/libraries     — list installed libraries
  PATCH /workspaces/{ws}/clusters/{key}/libraries    — install libraries
"""

import asyncio
import os
import sys
import time

import oci
import requests as http_requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─── AIDP Environment (same as other scripts) ──────────────────────────

# Generic — no hardcoded customer/AIDP-instance config; set at runtime
# (job_migrate_from_workflow overrides these). Profile defaults to "DEFAULT".
AIDP_BASE = None
DATALAKE_OCID = None
WORKSPACE_ID = None
OCI_PROFILE = "DEFAULT"

# aidp_compat wheel. The actual file is resolved version-agnostically from
# AIDP_COMPAT_DEPS_DIR at install time (resolve_aidp_compat_whl) so the toolkit
# works with whatever aidp_compat-*.whl the user uploaded — no hardcoded version pin.
AIDP_COMPAT_DEPS_DIR = "/Workspace/migration-dependencies"
AIDP_COMPAT_PREFIX = f"{AIDP_COMPAT_DEPS_DIR}/aidp_compat-"


def _is_aidp_compat_lib(path: str) -> bool:
    """Recognize any aidp_compat-*.whl version as 'installed'."""
    return path.startswith(AIDP_COMPAT_PREFIX) and path.endswith(".whl")

# ─── OCI Signer ────────────────────────────────────────────────────────

_SIGNER = None


def _get_signer():
    global _SIGNER
    if _SIGNER is None:
        config = oci.config.from_file(profile_name=OCI_PROFILE)
        _SIGNER = oci.signer.Signer(
            tenancy=config["tenancy"], user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
        )
    return _SIGNER


def _list_workspace_objects(path: str) -> list:
    """List items under a workspace folder (read-only). Returns [] on any error."""
    url = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/objects"
    try:
        r = http_requests.get(url, params={"path": path}, auth=_get_signer(), timeout=30)
        if r.status_code == 200:
            return r.json().get("items", []) or []
    except Exception:
        pass
    return []


def resolve_aidp_compat_whl():
    """Resolve the uploaded aidp_compat-*.whl in AIDP_COMPAT_DEPS_DIR
    (version-agnostic). Returns the workspace path, or None if none present.
    Used both for the preflight prerequisite check and at install time, so the
    toolkit installs whatever version the user uploaded — no hardcoded pin."""
    for it in _list_workspace_objects(AIDP_COMPAT_DEPS_DIR):
        p = it.get("path", "")
        if _is_aidp_compat_lib(p):
            return p
    return None


def _cluster_url(cluster_id: str) -> str:
    return (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}"
            f"/workspaces/{WORKSPACE_ID}/clusters/{cluster_id}")


def _libraries_url(cluster_id: str) -> str:
    return f"{_cluster_url(cluster_id)}/libraries"


# ─── Cluster State ─────────────────────────────────────────────────────


def get_cluster_state(cluster_id: str) -> str:
    """Get cluster lifecycleState. Returns e.g. 'ACTIVE', 'INACTIVE', 'CREATING', etc."""
    signer = _get_signer()
    resp = http_requests.get(
        _cluster_url(cluster_id), auth=signer,
        headers={"Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()
    return data.get("lifecycleState") or data.get("state") or "UNKNOWN"


def get_cluster_display_name(cluster_id: str) -> str:
    """Get cluster display name for logging."""
    signer = _get_signer()
    try:
        resp = http_requests.get(
            _cluster_url(cluster_id), auth=signer,
            headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json().get("displayName", cluster_id[:12])
    except Exception:
        return cluster_id[:12]


def start_cluster(cluster_id: str):
    """Start a stopped cluster. POST /actions/start."""
    signer = _get_signer()
    url = f"{_cluster_url(cluster_id)}/actions/start"
    resp = http_requests.post(
        url, auth=signer, json={},
        headers={"Content-Type": "application/json"})
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Failed to start cluster {cluster_id}: "
            f"HTTP {resp.status_code} — {resp.text[:300]}")
    print(f"[cluster-lifecycle] Start request sent for {cluster_id[:12]}...", flush=True)


async def ensure_cluster_running(cluster_id: str, timeout: int = 1800):
    """Ensure cluster is ACTIVE. Start if needed, poll until ready.

    Args:
        cluster_id: AIDP cluster UUID
        timeout: max seconds to wait (default 30 min)

    Raises:
        RuntimeError if cluster doesn't become ACTIVE within timeout.
    """
    state = get_cluster_state(cluster_id)
    name = get_cluster_display_name(cluster_id)
    print(f"[cluster-lifecycle] Cluster {name} ({cluster_id[:12]}...): {state}", flush=True)

    if state == "ACTIVE":
        return

    if state in ("INACTIVE", "STOPPED"):
        start_cluster(cluster_id)
    elif state in ("CREATING", "STARTING", "UPDATING"):
        print(f"[cluster-lifecycle] Cluster is {state}, waiting...", flush=True)
    else:
        raise RuntimeError(
            f"Cluster {cluster_id} is in unexpected state '{state}' — "
            f"cannot auto-start. Please check the AIDP console.")

    # Poll until ACTIVE
    poll_interval = 30  # seconds
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        state = get_cluster_state(cluster_id)
        mins = elapsed // 60
        secs = elapsed % 60
        print(f"[cluster-lifecycle] {name}: {state} ({mins}m{secs:02d}s / {timeout // 60}m)", flush=True)

        if state == "ACTIVE":
            print(f"[cluster-lifecycle] Cluster {name} is ACTIVE.", flush=True)
            return
        if state in ("FAILED", "DELETED", "DELETING"):
            raise RuntimeError(
                f"Cluster {cluster_id} entered state '{state}' — cannot proceed.")

    raise RuntimeError(
        f"Cluster {cluster_id} did not become ACTIVE within {timeout // 60} minutes "
        f"(last state: {state}).")


# ─── Library Management ────────────────────────────────────────────────


def get_installed_libraries(cluster_id: str) -> list:
    """List libraries installed on the cluster."""
    signer = _get_signer()
    resp = http_requests.get(
        _libraries_url(cluster_id), auth=signer,
        headers={"Accept": "application/json"})
    resp.raise_for_status()
    return resp.json().get("items", [])


def install_library(cluster_id: str, workspace_path: str):
    """Install a workspace file (whl, jar, requirements.txt) as a cluster library."""
    signer = _get_signer()
    body = {
        "items": [{
            "operation": "INSTALL",
            "type": "WORKSPACE_FILE",
            "path": workspace_path,
        }]
    }
    resp = http_requests.patch(
        _libraries_url(cluster_id), auth=signer,
        json=body, headers={"Content-Type": "application/json"})
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Failed to install {workspace_path} on cluster {cluster_id}: "
            f"HTTP {resp.status_code} — {resp.text[:300]}")
    return resp.json()


async def ensure_aidp_compat_installed(cluster_id: str, timeout: int = 600):
    """Check if aidp_compat is installed on the cluster. Install if missing.

    Args:
        cluster_id: AIDP cluster UUID
        timeout: max seconds to wait for install (default 5 min)
    """
    libs = get_installed_libraries(cluster_id)
    matched_libs = [lib for lib in libs if _is_aidp_compat_lib(lib.get("path", ""))]
    # When multiple aidp_compat versions are listed (e.g. an old FAILED entry
    # alongside a new INSTALLED one after a manual upgrade), prefer the
    # INSTALLED/RESOLVED one so we don't clobber a working install.
    healthy = next((l for l in matched_libs if l.get("status", "") in ("INSTALLED", "RESOLVED")), None)
    pending = next((l for l in matched_libs if l.get("status", "") not in ("INSTALLED", "RESOLVED", "FAILED", "SKIPPED")), None)
    failed = next((l for l in matched_libs if l.get("status", "") in ("FAILED", "SKIPPED")), None)

    if healthy is not None:
        print(f"[cluster-lifecycle] aidp_compat already installed ({healthy.get('path','')}) on {cluster_id[:12]}...", flush=True)
        return

    # An install (or re-install) is needed — resolve the uploaded wheel
    # version-agnostically. If it isn't there, this is a missing PREREQUISITE.
    whl = resolve_aidp_compat_whl()
    if whl is None:
        raise RuntimeError(
            f"PREREQUISITE NOT MET: no aidp_compat-*.whl found in {AIDP_COMPAT_DEPS_DIR} "
            f"on this workspace. Upload the wheel (from aidp_compat/dist/) to "
            f"{AIDP_COMPAT_DEPS_DIR} before migrating, then retry.")

    if pending is not None:
        # PENDING / INSTALLING — let the poll loop wait for it
        print(f"[cluster-lifecycle] aidp_compat ({pending.get('path','')}) status is {pending.get('status','')}, waiting...", flush=True)
    elif failed is not None:
        print(f"[cluster-lifecycle] aidp_compat ({failed.get('path','')}) status is {failed.get('status','')}, re-installing from {whl}...", flush=True)
        install_library(cluster_id, whl)
    else:
        print(f"[cluster-lifecycle] aidp_compat not found on {cluster_id[:12]}..., installing from {whl}...", flush=True)
        install_library(cluster_id, whl)

    # Poll until installed
    poll_interval = 15
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        libs = get_installed_libraries(cluster_id)
        matched_libs = [lib for lib in libs if _is_aidp_compat_lib(lib.get("path", ""))]
        healthy = next((l for l in matched_libs if l.get("status", "") in ("INSTALLED", "RESOLVED")), None)
        pending = next((l for l in matched_libs if l.get("status", "") not in ("INSTALLED", "RESOLVED", "FAILED", "SKIPPED")), None)
        failed = next((l for l in matched_libs if l.get("status", "") in ("FAILED", "SKIPPED")), None)
        if healthy is not None:
            print(f"[cluster-lifecycle] aidp_compat installed successfully ({healthy.get('path','')}).", flush=True)
            return
        elif pending is not None:
            print(f"[cluster-lifecycle] aidp_compat ({pending.get('path','')}): {pending.get('status','')} ({elapsed}s / {timeout}s)", flush=True)
        elif failed is not None:
            msg = failed.get("stateMessage", "")
            raise RuntimeError(
                f"aidp_compat install FAILED on cluster {cluster_id} ({failed.get('path','')}): {msg}")
        else:
            # Library disappeared from list — re-install
            print(f"[cluster-lifecycle] aidp_compat disappeared, re-installing from {whl}...", flush=True)
            install_library(cluster_id, whl)

    print(f"[cluster-lifecycle] WARNING: aidp_compat install did not confirm "
          f"within {timeout}s. Proceeding anyway — bootstrap import will verify.", flush=True)


# ─── Requirements.txt Management ──────────────────────────────────────

# Fallback map: import name (lowercased) → pip package name.
# Used only when PyPI lookup fails or is unavailable.
_IMPORT_TO_PIP = {
    "sklearn": "scikit-learn",
    "cv2": "opencv-python",
    "pil": "Pillow",
    "yaml": "PyYAML",
    "bs4": "beautifulsoup4",
    "attr": "attrs",
    "dateutil": "python-dateutil",
    "google": "google-cloud-storage",
}

# Cache for PyPI lookups within a single run (avoids repeated HTTP calls)
_pypi_cache: dict = {}
_pypi_available: bool | None = None  # None = not tested yet


def _pypi_reachable() -> bool:
    """Quick one-time check if PyPI is reachable (cached for the process)."""
    global _pypi_available
    if _pypi_available is not None:
        return _pypi_available
    import urllib.request
    import urllib.error
    try:
        req = urllib.request.Request("https://pypi.org/pypi/pip/json", method="HEAD")
        urllib.request.urlopen(req, timeout=5)
        _pypi_available = True
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        _pypi_available = False
        print("[cluster-lifecycle] WARNING: PyPI unreachable — using fallback mapping only", flush=True)
    return _pypi_available


def _pypi_exists(name: str) -> bool:
    """Check if a package name exists on PyPI. Returns False on any error."""
    import urllib.request
    import urllib.error
    try:
        req = urllib.request.Request(
            f"https://pypi.org/pypi/{name}/json",
            method="HEAD",
        )
        urllib.request.urlopen(req, timeout=5)
        return True
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        return False


def _resolve_pip_name(import_name: str) -> str:
    """Resolve an import name to a pip package name.

    Strategy:
      1. Check known mapping first (pil→Pillow, cv2→opencv-python, etc.)
         These override PyPI because some stale/wrong packages exist under
         the import name (e.g. 'pil' on PyPI is the ancient PIL 1.1.6).
      2. If PyPI is reachable, validate the mapped/direct name exists
      3. If PyPI is unreachable, use the mapping or pass through as-is
         (same as old behavior — never blocks installs due to network)
    """
    lowered = import_name.strip().lower()

    if lowered in _pypi_cache:
        return _pypi_cache[lowered]

    # 1. Known mapping takes priority (authoritative overrides)
    mapped = _IMPORT_TO_PIP.get(lowered)

    if not _pypi_reachable():
        # Network down — use mapping if available, else pass through as-is
        result = mapped or lowered
        _pypi_cache[lowered] = result
        return result

    # 2. If mapped, validate mapped name on PyPI
    if mapped:
        if _pypi_exists(mapped):
            _pypi_cache[lowered] = mapped
            return mapped
        # Mapped name invalid on PyPI — warn but still use it (mapping is curated)
        print(f"[cluster-lifecycle] WARNING: mapped package '{mapped}' for import "
              f"'{import_name}' not found on PyPI — using anyway", flush=True)
        _pypi_cache[lowered] = mapped
        return mapped

    # 3. No mapping — validate import name directly on PyPI
    if _pypi_exists(lowered):
        _pypi_cache[lowered] = lowered
        return lowered

    # 4. Not on PyPI either — pass through with warning (let pip fail explicitly
    #    rather than silently skipping, which could mask a mapping we need to add)
    print(f"[cluster-lifecycle] WARNING: no PyPI package found for import "
          f"'{import_name}' — passing through as-is", flush=True)
    _pypi_cache[lowered] = lowered
    return lowered

# Standard library modules — never install these
_STDLIB = {
    "os", "sys", "json", "re", "io", "math", "time", "datetime", "collections",
    "functools", "itertools", "typing", "pathlib", "hashlib", "base64", "copy",
    "csv", "logging", "traceback", "subprocess", "tempfile", "shutil", "glob",
    "urllib", "http", "concurrent", "threading", "multiprocessing", "abc",
    "dataclasses", "enum", "struct", "pickle", "gzip", "zipfile", "contextlib",
    "operator", "decimal", "fractions", "warnings", "inspect", "ast", "textwrap",
    "string", "random", "secrets", "uuid", "socket", "ssl", "email", "html",
    "xml", "ctypes", "pprint", "builtins", "importlib", "types", "weakref",
    "array", "bisect", "heapq", "statistics", "cmath", "dis", "code", "codeop",
    "pdb", "profile", "timeit", "unittest", "doctest", "argparse", "getopt",
    "configparser", "tomllib", "shelve", "sqlite3", "platform", "signal",
    "select", "selectors", "mmap", "syslog", "queue", "asyncio",
}

# Packages that are part of the Spark runtime — already on cluster
_SPARK_BUILTIN = {
    "pyspark", "spark", "py4j", "dbutils",
}

# JVM package roots from Scala/Java imports — NOT Python packages.
# These leak in when %scala cells aren't filtered by the cell analyzer.
_JVM_ROOTS = {
    "java", "javax", "org", "scala", "com", "net", "io",
}

# Packages provided by aidp_compat — already installed
_AIDP_PROVIDED = {
    "aidp_compat", "aidp_dbutils", "oidlUtils",
}


async def _discover_local_modules(session, notebook_paths: list, candidates: set,
                                  max_ancestor_depth: int = 6) -> tuple:
    """Walk parent directories of each notebook on the cluster looking for
    .py files / package dirs whose name matches a candidate import.

    Returns (matched_modules, src_roots, match_paths) where:
      matched_modules — set of candidate names found as local files/dirs
      src_roots       — set of directories to add to sys.path so the imports resolve
      match_paths     — list of actual filesystem paths of each match (used to
                        compute the mirror root)
    """
    if not notebook_paths or not candidates:
        return set(), set(), []

    # Normalize workspace-relative paths to absolute. The job orchestrator
    # passes paths like "Users/foo/bar.ipynb" but the cluster filesystem
    # mounts them at "/Workspace/Users/foo/bar.ipynb".
    abs_notebook_paths = []
    for nb in notebook_paths:
        if not nb:
            continue
        if not nb.startswith("/"):
            nb = f"/Workspace/{nb.lstrip('/')}"
        abs_notebook_paths.append(nb)

    # Build the set of ancestor dirs to scan (up to max_ancestor_depth per notebook)
    roots_to_scan = set()
    for nb in abs_notebook_paths:
        p = os.path.dirname(nb)
        for _ in range(max_ancestor_depth):
            if not p or p == "/" or p.count("/") <= 2:
                break
            roots_to_scan.add(p)
            p = os.path.dirname(p)
    if not roots_to_scan:
        return set(), set(), []

    import json as _json
    code = (
        "import os, json\n"
        f"candidates = {_json.dumps(sorted(candidates))}\n"
        f"roots = {_json.dumps(sorted(roots_to_scan))}\n"
        "hits = {}  # name -> list of matched paths\n"
        "cand_set = set(candidates)\n"
        "_seen_dirs = set()\n"
        "for root in roots:\n"
        "    if not os.path.isdir(root):\n"
        "        continue\n"
        "    for dirpath, dirnames, filenames in os.walk(root):\n"
        "        if dirpath in _seen_dirs:\n"
        "            dirnames[:] = []\n"
        "            continue\n"
        "        _seen_dirs.add(dirpath)\n"
        "        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d not in ('__pycache__', 'node_modules', '.venv', 'site-packages')]\n"
        "        for fn in filenames:\n"
        "            if fn.endswith('.py'):\n"
        "                stem = fn[:-3]\n"
        "                if stem in cand_set:\n"
        "                    hits.setdefault(stem, []).append(os.path.join(dirpath, fn))\n"
        "        for d in dirnames:\n"
        "            if d in cand_set:\n"
        "                hits.setdefault(d, []).append(os.path.join(dirpath, d) + '/')\n"
        "print(json.dumps(hits))\n"
    )
    from aidp_executor import format_outputs
    try:
        result = await session.execute(code, timeout=180)
    except Exception as e:
        print(f"[cluster-lifecycle] local-module scan failed: {e}", flush=True)
        return set(), set(), []

    raw = format_outputs(result.get("outputs", []))
    try:
        parsed = _json.loads(raw)
        if isinstance(parsed, list) and parsed and "value" in parsed[0]:
            raw = parsed[0]["value"]
    except Exception:
        pass
    # AIDP can duplicate stdout lines — take the first JSON object
    first_line = raw.strip().splitlines()[0] if raw.strip() else "{}"
    try:
        hits = _json.loads(first_line)
    except Exception as e:
        print(f"[cluster-lifecycle] local-module scan parse failed: {e}", flush=True)
        return set(), set(), []

    matched = set(hits.keys())
    match_paths = []
    # Build sys.path entries: for each match, add the immediate parent dir
    # AND walk up adding ancestors so `from a.b.c import X` works too.
    src_roots = set()
    for name, paths in hits.items():
        for p in paths:
            match_paths.append(p)
            parent = os.path.dirname(p.rstrip("/"))
            cur = parent
            for _ in range(5):  # add up to 5 ancestor levels
                if not cur or cur == "/" or cur.count("/") <= 3:
                    break
                src_roots.add(cur)
                cur = os.path.dirname(cur)
    return matched, src_roots, match_paths


def _pick_mirror_root(match_paths: list) -> str:
    """Choose the source-tree root to mirror.

    Strategy: take the deepest common directory ancestor of all matched
    files, then walk up looking for a 'src' directory (a strong project-root
    convention). If 'src' is found, return it. Otherwise return the deepest
    common ancestor.
    """
    if not match_paths:
        return ""
    parents = [os.path.dirname(p.rstrip("/")) for p in match_paths]
    common = os.path.commonpath(parents) if len(parents) > 1 else parents[0]
    # Walk up from `common` looking for a 'src' ancestor (without exiting the user dir)
    cur = common
    for _ in range(6):
        if not cur or cur == "/" or cur.count("/") <= 3:
            break
        if os.path.basename(cur) == "src":
            return cur
        cur = os.path.dirname(cur)
    return common


async def _mirror_source_tree(session, mirror_src: str, mirror_dst: str,
                              timeout: int = 600) -> bool:
    """Copy ONLY `.py` files from `mirror_src` → `mirror_dst`, preserving
    directory structure. Returns True on success.

    Why only .py and not the whole tree: when mirror_dst lives inside the
    migrated-notebooks tree (so the customer sees one coherent project
    layout), the destination may already contain migrated `.ipynb` files
    saved by Pass-1 dep migration. A full copytree (or rmtree-then-copytree)
    would clobber those. By copying only `.py` files, we never touch
    notebook artifacts. Hidden / cache dirs are skipped on the source side.
    """
    import json as _json
    code = (
        "import os, shutil, json\n"
        f"src = {_json.dumps(mirror_src)}\n"
        f"dst = {_json.dumps(mirror_dst)}\n"
        "result = {'src': src, 'dst': dst, 'ok': False, 'files_copied': 0}\n"
        "try:\n"
        "    if not os.path.isdir(src):\n"
        "        result['error'] = f'source dir does not exist: {src}'\n"
        "    else:\n"
        "        _SKIP_DIRS = {'__pycache__', 'node_modules', '.venv', 'site-packages'}\n"
        "        cnt = 0\n"
        "        for _dp, _dns, _fns in os.walk(src):\n"
        "            _dns[:] = [d for d in _dns if not d.startswith('.') and d not in _SKIP_DIRS]\n"
        "            _rel = os.path.relpath(_dp, src)\n"
        "            _tgt = dst if _rel == '.' else os.path.join(dst, _rel)\n"
        "            os.makedirs(_tgt, exist_ok=True)\n"
        "            for _fn in _fns:\n"
        "                if _fn.endswith('.py'):\n"
        "                    shutil.copy2(os.path.join(_dp, _fn), os.path.join(_tgt, _fn))\n"
        "                    cnt += 1\n"
        "        result['files_copied'] = cnt\n"
        "        result['ok'] = True\n"
        "except Exception as _e:\n"
        "    result['error'] = str(_e)[:300]\n"
        "print(json.dumps(result))\n"
    )
    from aidp_executor import format_outputs
    try:
        res = await session.execute(code, timeout=timeout)
    except Exception as e:
        print(f"[cluster-lifecycle] mirror copy failed: {e}", flush=True)
        return False
    raw = format_outputs(res.get("outputs", []))
    try:
        parsed = _json.loads(raw)
        if isinstance(parsed, list) and parsed and "value" in parsed[0]:
            raw = parsed[0]["value"]
    except Exception:
        pass
    first_line = raw.strip().splitlines()[0] if raw.strip() else "{}"
    try:
        info = _json.loads(first_line)
    except Exception:
        return False
    if info.get("ok"):
        print(f"[cluster-lifecycle] Mirrored {mirror_src} → {mirror_dst} ({info.get('files_copied', 0)} .py files)", flush=True)
        return True
    print(f"[cluster-lifecycle] Mirror failed: {info.get('error', 'unknown')}", flush=True)
    return False


async def ensure_requirements_installed(
    cluster_id: str,
    session,
    import_names: list,
    job_output_path: str,
    timeout: int = 600,
    notebook_paths: list = None,
    migrated_base: str = "",
) -> dict:
    """Check which imports are missing on the cluster and install via requirements.txt.

    Args:
        cluster_id: AIDP cluster UUID
        session: ClusterSession for running checks on cluster
        import_names: list of top-level import names from notebook analysis
        job_output_path: workspace path for this job (e.g. {OUTPUT_BASE}/{job_name})
        timeout: max seconds to wait for library install
        notebook_paths: notebook paths whose source trees should be scanned for
            local modules (so we don't try to pip-install in-repo .py files).

    Returns:
        dict with keys 'installed' (pip packages installed), 'local_modules'
        (candidates resolved as in-tree .py files), and 'src_roots' (dirs to
        prepend to sys.path so local imports resolve).
    """
    # Filter to third-party packages only
    candidates = set()
    for name in import_names:
        stripped = name.strip()
        lowered = stripped.lower()
        if lowered in _STDLIB or lowered in _SPARK_BUILTIN or lowered in _AIDP_PROVIDED or lowered in _JVM_ROOTS:
            continue
        if not stripped or stripped.startswith("_"):
            continue
        candidates.add(stripped)  # preserve original case for importlib

    if not candidates:
        return {"installed": [], "local_modules": [], "src_roots": [], "mirror_root_orig": "", "mirror_root_dst": ""}

    # Check which are actually missing on the cluster. Redirect stdout/stderr
    # during the importlib calls so candidate modules with top-level print()
    # statements don't pollute our output stream (and don't broaden except —
    # if a candidate raises something other than ImportError, we still treat
    # it as missing/broken and surface it).
    check_code = (
        "import importlib, io, sys, contextlib\n"
        "missing = []\n"
        "_sink = io.StringIO()\n"
        "with contextlib.redirect_stdout(_sink), contextlib.redirect_stderr(_sink):\n"
    )
    for mod in sorted(candidates):
        check_code += (
            f"    try:\n        importlib.import_module('{mod}')\n"
            f"    except ImportError:\n        missing.append('{mod}')\n"
            f"    except Exception:\n        missing.append('{mod}')\n"
        )
    # Sentinel-wrap the result so any leftover stdout (e.g. early prints
    # before the redirect, or duplicated AIDP lines) can't be mistaken for
    # the missing-list payload.
    check_code += "print('__AIDP_MIG_MISSING_BEGIN__' + ','.join(missing) + '__AIDP_MIG_MISSING_END__')\n"

    from aidp_executor import format_outputs
    result = await session.execute(check_code, timeout=60)
    output = format_outputs(result.get("outputs", []))
    # Unwrap AIDP JSON wrapper
    try:
        import json as _json
        parsed = _json.loads(output)
        if isinstance(parsed, list) and parsed and "value" in parsed[0]:
            output = parsed[0]["value"]
    except Exception:
        pass
    output = output.strip()

    # Extract just the sentinel-wrapped payload
    import re as _re
    _m = _re.search(r'__AIDP_MIG_MISSING_BEGIN__(.*?)__AIDP_MIG_MISSING_END__', output, _re.DOTALL)
    if _m:
        payload = _m.group(1)
    else:
        # Fallback to old behavior if sentinels missing (compatibility)
        payload = output

    # Identifier filter — only accept valid Python identifiers as module names
    _IDENT_RE = _re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
    _parts = [p.strip() for p in payload.replace('\n', ',').split(',') if p.strip()]
    _parts = [p for p in _parts if _IDENT_RE.match(p)]

    if not _parts:
        print(f"[cluster-lifecycle] All {len(candidates)} packages available on cluster.", flush=True)
        return {"installed": [], "local_modules": [], "src_roots": [], "mirror_root_orig": "", "mirror_root_dst": ""}

    missing_imports = _parts
    if not missing_imports:
        return {"installed": [], "local_modules": [], "src_roots": [], "mirror_root_orig": "", "mirror_root_dst": ""}

    # ── Local-module scan + isolation ────────────────────────────────
    # Many "missing" modules are actually .py files / package dirs in the
    # notebook's own source tree (e.g. system_config, helpers).
    # We do NOT pip-install these. Critically, we ALSO copy the source tree
    # to a migration-scoped mirror under {job_output_path}/local_modules/...
    # so that any patches Opus generates during the fix loop land in the
    # mirror, not in the original source files.
    local_modules, src_roots = set(), set()
    mirror_root_orig = ""  # original source root that was mirrored
    mirror_root_dst = ""   # mirror destination under job_output_path
    if notebook_paths:
        local_modules, orig_src_roots, match_paths = await _discover_local_modules(
            session, notebook_paths, set(missing_imports)
        )
        if local_modules:
            print(f"[cluster-lifecycle] Local modules (NOT pip-installable): {', '.join(sorted(local_modules))}", flush=True)
            missing_imports = [m for m in missing_imports if m not in local_modules]

            mirror_root_orig = _pick_mirror_root(match_paths)
            if mirror_root_orig:
                # Place mirror under MIGRATED_BASE preserving the workspace-
                # relative path so the customer sees one coherent project
                # tree (migrated .ipynb + mirrored .py side by side). Fall
                # back to the old isolated location only if migrated_base
                # wasn't provided (caller compat).
                if migrated_base:
                    _rel = mirror_root_orig
                    if _rel.startswith("/Workspace/"):
                        _rel = _rel[len("/Workspace/"):]
                    else:
                        _rel = _rel.lstrip("/")
                    mirror_root_dst = f"{migrated_base}/{_rel}"
                else:
                    mirror_root_dst = f"{job_output_path}/local_modules/{os.path.basename(mirror_root_orig)}"
                ok = await _mirror_source_tree(session, mirror_root_orig, mirror_root_dst)
                if ok:
                    # Translate each discovered sys.path entry to its mirror equivalent
                    for orig in orig_src_roots:
                        if orig == mirror_root_orig:
                            src_roots.add(mirror_root_dst)
                        elif orig.startswith(mirror_root_orig + "/"):
                            rel = orig[len(mirror_root_orig) + 1:]
                            src_roots.add(f"{mirror_root_dst}/{rel}")
                        # Ancestors above mirror_root_orig are deliberately dropped —
                        # they'd point back at the customer source tree, defeating
                        # the isolation. Imports must resolve from within the mirror.
                    print(f"[cluster-lifecycle] sys.path mirror entries: {', '.join(sorted(src_roots))}", flush=True)
                else:
                    # Mirror failed — fall back to original src_roots (lossy but
                    # the cell still has a chance). Mutation risk is accepted only
                    # in this fallback path.
                    src_roots = orig_src_roots
                    print(f"[cluster-lifecycle] WARNING: mirror failed, falling back to original src tree (mutations will hit customer source)", flush=True)

    # Resolve import names to validated PyPI package names
    pip_packages = []
    _seen_pkgs = set()
    for imp in missing_imports:
        pip_name = _resolve_pip_name(imp)
        if pip_name not in _seen_pkgs:
            _seen_pkgs.add(pip_name)
            pip_packages.append(pip_name)

    if not pip_packages:
        return {"installed": [], "local_modules": sorted(local_modules),
                "src_roots": sorted(src_roots),
                "mirror_root_orig": mirror_root_orig, "mirror_root_dst": mirror_root_dst}

    print(f"[cluster-lifecycle] Missing packages: {', '.join(pip_packages)}", flush=True)

    req_workspace_path = f"{job_output_path}/requirements.txt"

    # Read existing requirements.txt to build cumulative set
    read_code = (
        f"try:\n"
        f"    with open('{req_workspace_path}') as f:\n"
        f"        print(f.read())\n"
        f"except FileNotFoundError:\n"
        f"    print('EMPTY')\n"
    )
    from aidp_executor import format_outputs
    from context_tools import _unwrap_aidp_text
    read_result = await session.execute(read_code, timeout=30)
    # _unwrap_aidp_text handles the AIDP output wrapper AND a leading empty "[]"
    # (e.g. "[]EMPTY"), which a manual [{"value":...}] unwrap misses — otherwise
    # "[]EMPTY" gets written back as a bogus requirement and pip install fails.
    existing_content = _unwrap_aidp_text(format_outputs(read_result.get("outputs", [])))
    existing_packages = set()
    if existing_content.strip() and existing_content.strip() != "EMPTY":
        for line in existing_content.strip().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and line != "EMPTY":
                existing_packages.add(line)

    # Check if all missing packages are already in requirements.txt —
    # they were installed previously, no need to re-install
    new_packages = [p for p in pip_packages if p not in existing_packages]
    if not new_packages:
        print(f"[cluster-lifecycle] All missing packages already in requirements.txt — skipping install", flush=True)
        return {"installed": [], "local_modules": sorted(local_modules),
                "src_roots": sorted(src_roots),
                "mirror_root_orig": mirror_root_orig, "mirror_root_dst": mirror_root_dst}

    # Merge: existing + new (cumulative)
    all_packages = sorted(existing_packages | set(pip_packages))
    req_content = "\n".join(all_packages) + "\n"

    # Upload via session (workspace is writable from cluster)
    upload_code = (
        f"import os\n"
        f"os.makedirs(os.path.dirname('{req_workspace_path}'), exist_ok=True)\n"
        f"with open('{req_workspace_path}', 'w') as f:\n"
        f"    f.write({repr(req_content)})\n"
        f"print('OK')\n"
    )
    await session.execute(upload_code, timeout=30)
    print(f"[cluster-lifecycle] Wrote {req_workspace_path} ({len(all_packages)} total, {len(new_packages)} new: {', '.join(new_packages)})", flush=True)

    # Install via cluster libraries API
    install_library(cluster_id, req_workspace_path)
    print(f"[cluster-lifecycle] Install request sent for requirements.txt", flush=True)

    # Poll until installed
    await _poll_library_install(cluster_id, req_workspace_path, timeout)

    return {"installed": pip_packages, "local_modules": sorted(local_modules),
            "src_roots": sorted(src_roots),
            "mirror_root_orig": mirror_root_orig, "mirror_root_dst": mirror_root_dst}


async def install_missing_package(
    cluster_id: str,
    session,
    module_name: str,
    job_output_path: str,
    timeout: int = 600,
    notebook_paths: list = None,
) -> bool:
    """Install a single missing package discovered at runtime.

    Reads existing requirements.txt, appends the new package, re-uploads,
    and re-installs via the cluster libraries API.

    Returns True if install succeeded. Returns False (without trying pip) if
    the module is detected as a local .py file in the notebook's source tree.
    """
    # Local-module guard: if the "missing" module is actually an in-tree
    # .py file or package dir, refuse to pip-install it (PyPI may have an
    # unrelated package with the same name, which would silently install
    # wrong code). The caller's fix loop should add the src root to sys.path
    # instead.
    if notebook_paths:
        local_modules, _, _ = await _discover_local_modules(
            session, notebook_paths, {module_name}
        )
        if module_name in local_modules:
            print(f"[cluster-lifecycle] {module_name} is a local module in the notebook tree — skipping pip install", flush=True)
            return False

    pip_name = _resolve_pip_name(module_name)
    req_workspace_path = f"{job_output_path}/requirements.txt"

    # Read existing requirements from cluster
    read_code = (
        f"try:\n"
        f"    with open('{req_workspace_path}') as f:\n"
        f"        print(f.read())\n"
        f"except FileNotFoundError:\n"
        f"    print('EMPTY')\n"
    )
    from aidp_executor import format_outputs
    from context_tools import _unwrap_aidp_text
    result = await session.execute(read_code, timeout=30)
    # See note in ensure_requirements_installed: unwrap a leading "[]" wrapper
    # too, else "[]EMPTY" is treated as a real requirement.
    output = _unwrap_aidp_text(format_outputs(result.get("outputs", [])))

    existing = set()
    if output.strip() and output.strip() != "EMPTY":
        existing = {line.strip() for line in output.strip().splitlines()
                    if line.strip() and line.strip() != "EMPTY"}

    if pip_name in existing:
        print(f"[cluster-lifecycle] {pip_name} already in requirements.txt — re-installing", flush=True)
    else:
        existing.add(pip_name)
        print(f"[cluster-lifecycle] Adding {pip_name} to requirements.txt", flush=True)

    # Re-write and re-install
    req_content = "\n".join(sorted(existing)) + "\n"
    upload_code = (
        f"import os\n"
        f"os.makedirs(os.path.dirname('{req_workspace_path}'), exist_ok=True)\n"
        f"with open('{req_workspace_path}', 'w') as f:\n"
        f"    f.write({repr(req_content)})\n"
        f"print('OK')\n"
    )
    await session.execute(upload_code, timeout=30)

    install_library(cluster_id, req_workspace_path)
    print(f"[cluster-lifecycle] Re-install request sent for requirements.txt", flush=True)

    return await _poll_library_install(cluster_id, req_workspace_path, timeout)


async def _poll_library_install(cluster_id: str, library_path: str, timeout: int = 600) -> bool:
    """Poll until a library install completes. Returns True if successful."""
    poll_interval = 10
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        libs = get_installed_libraries(cluster_id)
        for lib in libs:
            if lib.get("path") == library_path:
                status = lib.get("status", "")
                if status in ("INSTALLED", "RESOLVED"):
                    print(f"[cluster-lifecycle] {library_path} installed successfully.", flush=True)
                    return True
                elif status == "FAILED":
                    msg = lib.get("stateMessage", "")
                    print(f"[cluster-lifecycle] WARNING: {library_path} install FAILED: {msg}", flush=True)
                    return False
                else:
                    print(f"[cluster-lifecycle] {library_path}: {status} ({elapsed}s / {timeout}s)", flush=True)
                break

    print(f"[cluster-lifecycle] WARNING: {library_path} install did not confirm "
          f"within {timeout}s.", flush=True)
    return False
