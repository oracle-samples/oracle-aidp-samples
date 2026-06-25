#!/usr/bin/env python3
"""
Job Migration Orchestrator
============================
Migrates Databricks jobs to AIDP with:
- DAG-ordered execution within each job
- Claude Opus for analysis, migration, and fixing
- Real execution on AIDP cluster (ALL cells including writes)
- Circuit breaker monitoring to stop on cascading failures
- Parallel processing across jobs (20 concurrent notebooks)
- No local storage - download, process, upload, delete

Usage:
    python3 job_migrate.py --parallel 20
    python3 job_migrate.py --jobs <job_name> --parallel 1  # smoke test
"""

import anthropic
import ast
import asyncio
import json
import os
import sys
import re
import copy
import time
import base64
import tempfile
import shutil
import argparse
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple, Set, Any
from collections import defaultdict

# Timestamped print for all migration output
_original_print = print
def tprint(*args, **kwargs):
    """Print with timestamp prefix."""
    ts = datetime.now().strftime("%H:%M:%S")
    _original_print(f"[{ts}]", *args, **kwargs)

import oci
import requests as http_requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession, format_outputs, get_oci_signer
from context_tools import (
    load_catalog_snapshot, get_dependent_notebook_context,
    build_cell_context, extract_notebook_dependencies,
    fetch_recent_spark_errors,
    build_catalog_cache,
)

# ─── Output compaction for LLM prompts ───────────────────────────────

def _compact_output_for_llm(text: str, max_chars: int = 6000) -> str:
    """Collapse repeated/similar lines in execution output before sending to LLM.
    Spark progress lines like '[Stage 42: ====> (150 + 8) / 200]' repeat thousands
    of times during long jobs — useless for LLM evaluation but blow up token count.
    """
    if not text or len(text) <= max_chars:
        return text
    lines = text.split("\n")
    result = []
    prev_prefix = None
    repeat_count = 0
    for line in lines:
        # Signature: first 20 chars identifies repeated patterns
        prefix = line[:20].strip()
        if prefix == prev_prefix and prefix:
            repeat_count += 1
        else:
            if repeat_count > 0:
                result.append(f"  ... [{repeat_count} similar lines collapsed]")
            result.append(line)
            repeat_count = 0
            prev_prefix = prefix
    if repeat_count > 0:
        result.append(f"  ... [{repeat_count} similar lines collapsed]")
    compacted = "\n".join(result)
    # Final safety cap — keep first half + last half
    if len(compacted) > max_chars:
        half = max_chars // 2
        compacted = (compacted[:half]
                     + f"\n\n... [{len(compacted) - max_chars} chars truncated] ...\n\n"
                     + compacted[-half:])
    return compacted


# ─── Config ───────────────────────────────────────────────────────────

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Generic tool — no hardcoded customer/AIDP-instance config. These are provided
# at runtime via CLI args (required) or derived; main() applies them before use.
# AIDP_BASE is derived from the datalake OCID region; OCI profile defaults to "DEFAULT".
AIDP_BASE = None
DATALAKE_OCID = None
WORKSPACE_ID = None
DOWNLOAD_META_URL = None  # recomputed in main() once DATALAKE_OCID/WORKSPACE_ID are set
OCI_PROFILE = "DEFAULT"
DEFAULT_CLUSTER = None
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OUTPUT_BASE = None


def build_oidlutils_bridge_snippet(output_base: str, job_name: str) -> str:
    """Build the cluster-session bootstrap snippet that installs the
    `_AIDPOidlWrapper` global. SHARED between job_migrate.py (per-task connect)
    and job_migrate_from_workflow.py (startup connect) — both call this so the
    snippet stays in sync.

    The wrapper reads getParameter from two files (lookup order):
      1. task_values.json    — written by setTaskValue (cross-task, mutable)
      2. manifest_params.json — written by process_notebook at task start

    Idempotent via `type(oidlUtils).__name__ == '_AIDPOidlWrapper'` check.
    See tests/test_oidlutils_wrapper.py + /tmp/test_bootstrap_snippet_aidp.py.
    """
    tv_file = f"{output_base}/{job_name}/task_values.json"
    mp_file = f"{output_base}/{job_name}/manifest_params.json"
    return f"""
import json, os

_TV_FILE = {json.dumps(tv_file)}
_MP_FILE = {json.dumps(mp_file)}
os.makedirs(os.path.dirname(_TV_FILE), exist_ok=True)

class _AIDPParams:
    def __init__(self, native):
        self._native = native
    def _read(self, path):
        if os.path.exists(path):
            try:
                with open(path) as f:
                    return json.load(f)
            except Exception:
                return {{}}
        return {{}}
    def getParameter(self, name, default=None):
        tv = self._read(_TV_FILE)
        if name in tv:
            return tv[name]
        mp = self._read(_MP_FILE)
        if name in mp:
            return mp[name]
        return default
    def setTaskValue(self, name, value):
        data = self._read(_TV_FILE)
        data[name] = value
        with open(_TV_FILE, 'w') as f:
            json.dump(data, f)
    def getTaskValue(self, taskKey, name, default=""):
        # Mirror of setTaskValue's flat storage: look up by name only.
        # `taskKey` arg is accepted for API parity with Databricks /
        # native oidlUtils but ignored (we don't shard task_values.json
        # by source task — names are unique per-job in practice).
        tv = self._read(_TV_FILE)
        if name in tv:
            return tv[name]
        mp = self._read(_MP_FILE)
        if name in mp:
            return mp[name]
        return default
    def __getattr__(self, attr):
        return getattr(self._native, attr)

class _AIDPOidlWrapper:
    def __init__(self, native):
        self._native = native
        self.parameters = _AIDPParams(native.parameters)
    def __getattr__(self, attr):
        return getattr(self._native, attr)

if type(oidlUtils).__name__ != '_AIDPOidlWrapper':
    oidlUtils = _AIDPOidlWrapper(oidlUtils)
    print('[AIDP-BRIDGE] OK (INSTALLED, tv=' + _TV_FILE + ', mp=' + _MP_FILE + ')')
else:
    print('[AIDP-BRIDGE] OK (ALREADY_WRAPPED)')
"""


START_TASK = ""  # if set, skip all tasks before this key (substring match); set via --start-task
ONLY_TASKS = []  # if set, run ONLY these tasks (substring match on task_key); set via --only-tasks
SKIP_MIGRATED = True  # skip notebooks already migrated (checked via migration registry)
DIRECT_EXECUTE = False  # if True, skip AI analysis/migration and execute cells as-is (useful for WS tests)

MAX_FIX_RETRIES = 5  # Sonnet for 1-2, Opus for 3+
CONSECUTIVE_FAIL_THRESHOLD = 3  # trigger monitor within a notebook
CONSECUTIVE_NB_FAIL_THRESHOLD = 2  # stop entire job

# Detect AIDP compute cluster paused/down.
# When the Dataflow compute cluster is not running, AIDP prints this in the kernel output
# rather than raising an exception. The Python kernel (WS) stays alive — only Spark is down.
# Fix: disconnect + reconnect the WS (triggers AIDP to resume the compute cluster).
_CLUSTER_DOWN_RE = re.compile(
    r"Compute cluster [a-z0-9.\-:]+ is not running", re.IGNORECASE
)

# ─── OCI / Download ──────────────────────────────────────────────────

_SIGNER = None
def signer():
    global _SIGNER
    if not _SIGNER:
        _, _SIGNER = get_oci_signer(OCI_PROFILE)
    return _SIGNER


# ─── OCI Object Storage Fallback ────────────────────────────────────

OCI_BACKUP_BUCKET = "<oci_backup_bucket>"
OCI_BACKUP_NAMESPACE = "<WORKSPACE_NAMESPACE>"

_OS_CLIENT = None
def _get_os_client():
    """Lazy-init OCI Object Storage client."""
    global _OS_CLIENT
    if not _OS_CLIENT:
        config = oci.config.from_file(profile_name=OCI_PROFILE)
        _OS_CLIENT = oci.object_storage.ObjectStorageClient(config)
    return _OS_CLIENT


def upload_to_object_storage(local_file: str, object_name: str, log_fn=None) -> bool:
    """Upload a local file to OCI Object Storage as fallback.
    object_name: the key in the bucket (e.g. aidp-migration-tool-output/job/tasks/task/final.ipynb)
    Returns True on success."""
    try:
        client = _get_os_client()
        with open(local_file, 'rb') as f:
            client.put_object(OCI_BACKUP_NAMESPACE, OCI_BACKUP_BUCKET, object_name, f)
        if log_fn:
            log_fn(f"  [OCI backup] Uploaded: oci://{OCI_BACKUP_BUCKET}@{OCI_BACKUP_NAMESPACE}/{object_name}")
        return True
    except Exception as e:
        if log_fn:
            log_fn(f"  [OCI backup] FAILED: {object_name} — {str(e)[:200]}")
        return False


def fallback_upload_to_ocs(local_files: list, output_dir: str,
                           migrated_nb_path: str, log_fn=None) -> bool:
    """Upload all artifacts to OCI Object Storage when workspace write fails.

    local_files: list of (local_path, remote_filename) tuples
    output_dir: workspace output dir path (e.g. /Workspace/<output-base>/aidp-migration/job/tasks/task)
    migrated_nb_path: workspace migrated notebook path
    log_fn: logging function

    Mirrors the workspace path structure under the bucket, stripping /Workspace/ prefix.
    """
    if log_fn:
        log_fn(f"  [OCI backup] Falling back to Object Storage: oci://{OCI_BACKUP_BUCKET}@{OCI_BACKUP_NAMESPACE}/")

    # Strip /Workspace/ prefix to get clean object keys
    def to_object_key(ws_path):
        if ws_path.startswith("/Workspace/"):
            return ws_path[len("/Workspace/"):]
        elif ws_path.startswith("/"):
            return ws_path[1:]
        return ws_path

    all_ok = True

    # Upload task artifacts (reports, logs, notebook)
    for local_path, remote_name in local_files:
        object_key = f"{to_object_key(output_dir)}/{remote_name}"
        if not upload_to_object_storage(local_path, object_key, log_fn):
            all_ok = False

    # Upload migrated notebook to the notebooks/ path for dep resolution
    if migrated_nb_path:
        # Find final.ipynb from local_files
        final_local = None
        for local_path, remote_name in local_files:
            if remote_name == "final.ipynb":
                final_local = local_path
                break
        if final_local:
            nb_key = to_object_key(migrated_nb_path)
            if not upload_to_object_storage(final_local, nb_key, log_fn):
                all_ok = False

    if log_fn:
        log_fn(f"  [OCI backup] {'Complete' if all_ok else 'Partial — some uploads failed'}")
    return all_ok


def _download_path_variants(path: str) -> list:
    """Generate path variants to try when downloading.

    AIDP file names may differ from how they're referenced in code:
      - Original: "<Original Notebook Name>" (with spaces, no .ipynb)
      - Code reference may use: "<original_notebook_name>.ipynb"
    Try all combinations of {as-is, +.ipynb, -.ipynb} × {as-is, spaces↔underscores}.
    Order: most likely first (as-is, then add .ipynb, then transform).
    """
    variants = []
    seen = set()
    def add(p):
        if p and p not in seen:
            variants.append(p)
            seen.add(p)

    # 1. As-is
    add(path)
    # 2. Add .ipynb if missing
    if not path.endswith(".ipynb"):
        add(path + ".ipynb")
    # 3. Strip .ipynb if present (some AIDP files have no extension)
    if path.endswith(".ipynb"):
        add(path[:-6])
    # 4. Spaces → underscores (and combinations)
    if " " in path:
        u = path.replace(" ", "_")
        add(u)
        if not u.endswith(".ipynb"):
            add(u + ".ipynb")
    # 5. Underscores → spaces (try if no original spaces)
    if "_" in path and " " not in path:
        s = path.replace("_", " ")
        add(s)
        if not s.endswith(".ipynb"):
            add(s + ".ipynb")
        if s.endswith(".ipynb"):
            add(s[:-6])
    return variants


def download_notebook(path: str, max_retries: int = 3) -> Optional[bytes]:
    """Download a notebook from AIDP workspace with retry + backoff.

    Tries multiple path variants since AIDP file names may differ from how
    they're referenced (spaces vs underscores, with/without .ipynb).
    """
    variants = _download_path_variants(path)
    for attempt_path in variants:
        for retry in range(max_retries):
            try:
                headers = {"Content-Type": "application/json", "path": attempt_path, "type": "NOTEBOOK"}
                print(f"[download] POST {DOWNLOAD_META_URL} path={attempt_path!r} (attempt {retry+1}/{max_retries})")
                resp = http_requests.post(DOWNLOAD_META_URL, auth=signer(), headers=headers, data="",
                                          timeout=60)
                print(f"[download] downloadFileMeta status={resp.status_code} body={resp.text[:300]!r}")
                if resp.status_code == 429:
                    wait = min(10 * (retry + 1), 30)
                    print(f"[download] rate limited (429), waiting {wait}s before retry")
                    time.sleep(wait)
                    continue
                if resp.status_code != 200:
                    break  # not retryable, try next path variant
                par_url = resp.json().get("parUrl")
                if not par_url:
                    print(f"[download] no parUrl in response")
                    break
                resp = http_requests.get(par_url, timeout=60)
                print(f"[download] PAR GET status={resp.status_code} content_len={len(resp.content)}")
                if resp.status_code == 429:
                    wait = min(10 * (retry + 1), 30)
                    print(f"[download] PAR rate limited (429), waiting {wait}s before retry")
                    time.sleep(wait)
                    continue
                if resp.status_code == 200 and len(resp.content) > 10:
                    return resp.content
            except Exception as e:
                print(f"[download] exception for path={attempt_path!r} (attempt {retry+1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    wait = min(5 * (retry + 1), 15)
                    print(f"[download] retrying in {wait}s...")
                    time.sleep(wait)
                continue
    print(f"[download] FAILED — all {len(variants)} path variants exhausted for {path!r}: tried {variants}")
    return None


async def download_notebook_async(path: str) -> Optional[bytes]:
    """Non-blocking wrapper: runs download_notebook in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, download_notebook, path)


# ─── Claude API ───────────────────────────────────────────────────────

_CLIENT = None
def claude():
    global _CLIENT
    if not _CLIENT:
        _CLIENT = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return _CLIENT


# Import the prompts from agent_migrate.py
from agent_migrate import (
    ANALYSIS_PROMPT, MIGRATION_PROMPT, FIX_PROMPT,
    call_opus, call_fix, call_opus_with_tools, attempt_data_recovery,
    read_notebook as parse_notebook, _extract_code_from_response,
    _cell_history, _current_cell_notes, register_replay_fn, _summarize_cell_code,
    set_current_task_key, set_job_manifest, set_current_task_params,
    extract_pip_packages, _installed_packages, clear_installed_packages, get_installed_packages,
)
from context_tools import get_bucket_mapping_context

# Optional throttle / rate-limit hardening modules. Imported defensively so
# the validator runs even when the new modules are missing (e.g. a partial
# upgrade where job_migrate.py is updated but the new files are not yet
# deployed to the migration host).
try:
    from migration_429_detector import detect as _detect_rate_limit
except ImportError:  # pragma: no cover -- soft dependency
    _detect_rate_limit = None
try:
    from throttle_coordinator import ThrottleCoordinator as _ThrottleCoordinator
except ImportError:  # pragma: no cover -- soft dependency
    _ThrottleCoordinator = None

# Lazily-built process-global coordinator. Constructed on first 429/CB
# detection so unit tests of process_notebook do not require an env file.
_THROTTLE_COORD = None


def _get_throttle_coord():
    """Return a lazily-constructed ThrottleCoordinator if the module is available."""
    global _THROTTLE_COORD
    if _ThrottleCoordinator is None:
        return None
    if _THROTTLE_COORD is None:
        try:
            budget = int(os.environ.get("AIDP_THROTTLE_BUDGET", "48"))
        except ValueError:
            budget = 48
        try:
            _THROTTLE_COORD = _ThrottleCoordinator(budget=budget)
        except Exception:
            _THROTTLE_COORD = None
    return _THROTTLE_COORD


# ─── Cell History Replay ──────────────────────────────────────────────

async def _replay_cell_entry(entry: dict, why: str, session, log_fn=None) -> dict:
    """Re-run one history entry through the full execute+verify+fix loop.

    Injects `why` into fix context so Opus understands the rewind reason.
    Appends result to _cell_history (same as original cell loop).
    Returns: {"status": "ok"|"error", "final_code": str, "output": str}
    """
    from agent_migrate import _cell_history as _ch, _summarize_cell_code as _summarize
    current_code = entry["final_code"].split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
    MAX_REPLAY_RETRIES = 5
    output = ""
    final_status = "error"

    for attempt in range(MAX_REPLAY_RETRIES):
        # Write-redirect on replay too — replay re-executes a previously-
        # migrated cell during a fixup_cell rewind. The cell may write,
        # and the saved final_code is clean (no redirects); we MUST add
        # redirects here so production data is never touched.
        exec_code = _apply_write_redirects(current_code, source_op_hint=f"replay-cell-{entry.get('cell_idx', '?')}")
        exec_code = _apply_read_redirects(exec_code)
        exec_code = _inject_write_guard(exec_code)  # variable write dests → tmp
        try:
            result = await session.execute(exec_code, timeout=14400)
        except Exception as e:
            result = {"status": "error", "outputs": [{"type": "error", "ename": "SessionError", "evalue": str(e)[:200]}]}

        status = result.get("status", "error")
        output = format_outputs(result.get("outputs", []))

        # Check for notebook.exit() — treat as successful early stop.
        # Only match the NotebookExit exception class, NOT function call strings
        # like "dbutils.notebook.exit" which could appear in comments/logs in output.
        if status != "ok" and output:
            _exit_pats = ["NotebookExit"]
            if any(p in output for p in _exit_pats):
                if log_fn:
                    log_fn(f"  [replay] cell {entry['cell_idx']}: notebook.exit() called")
                final_status = "ok"
                break

        error_patterns = ["Traceback", "Exception:", "NameError", "TypeError",
                          "FileNotFoundError", "ZeroDivisionError"]
        has_error = (status != "ok") or any(
            p in (output or "") for p in error_patterns
        )

        if not has_error:
            if log_fn:
                log_fn(f"  [replay] cell {entry['cell_idx']}: OK (attempt {attempt+1})")
            final_status = "ok"
            break

        if log_fn:
            log_fn(f"  [replay] cell {entry['cell_idx']}: FAIL attempt {attempt+1}: {(output or '')[:120].replace(chr(10), ' ')}")

        if attempt < MAX_REPLAY_RETRIES - 1:
            rewind_ctx = (
                f"REWIND CONTEXT: This cell is being replayed as part of a fixup_cell rewind.\n"
                f"Root cause identified upstream: {why}\n"
                f"Fix this cell with awareness of the upstream issue. "
                f"Do not revert fixes already applied to this cell."
            )
            try:
                _pre_fix_code = current_code
                current_code = await call_fix(
                    current_code, _compact_output_for_llm(output) or "error",
                    [], attempt + 1,
                    extra_context=rewind_ctx, session=session, log_fn=log_fn,
                    notebook_path=entry.get("notebook_path", "")
                )
                current_code = _fix_path_replace_idempotency(current_code)
                current_code = _detect_table_to_path_regression(_pre_fix_code, current_code)
                current_code = _detect_path_returning_to_identifier_regression(_pre_fix_code, current_code)
            except Exception:
                break

    # Append to _cell_history
    summary = entry.get("summary", current_code.strip()[:100].replace("\n", " "))
    if entry.get("is_child") and not summary:
        summary = await _summarize(current_code)
    _ch.append({
        "index":          len(_ch),
        "notebook_path":  entry["notebook_path"],
        "cell_idx":       entry["cell_idx"],
        "summary":        summary[:200],
        "final_code":     current_code[:3000],
        "output_preview": output[:300] if output else "",
        "status":         final_status,
        "is_child":       entry.get("is_child", False),
        "last_note":      entry.get("last_note", ""),
    })

    return {"status": final_status, "final_code": current_code, "output": output}


register_replay_fn(_replay_cell_entry)


# ─── Session Pool ────────────────────────────────────────────────────

class SessionPool:
    """Pool of reusable AIDP WebSocket sessions."""

    def __init__(self, max_sessions: int, cluster_id: str):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._cluster_id = cluster_id
        self._max = max_sessions
        self._created = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> AIDPSession:
        # Try to get an existing session
        try:
            session = self._queue.get_nowait()
            if session.ws and session.ws.connected:
                return session
            # Stale session, create new
        except asyncio.QueueEmpty:
            pass

        # Create new session if under limit
        async with self._lock:
            if self._created < self._max:
                session = AIDPSession(cluster_id=self._cluster_id)
                await session.connect()
                # Bootstrap aidp_compat
                await session.execute(
                    "# aidp_compat installed as cluster library",
                    timeout=30
                )
                self._created += 1
                return session

        # At limit, wait for one to be returned
        session = await self._queue.get()
        if not (session.ws and session.ws.connected):
            session = AIDPSession(cluster_id=self._cluster_id)
            await session.connect()
            await session.execute(
                "# aidp_compat installed as cluster library",
                timeout=30
            )
        return session

    async def release(self, session: AIDPSession):
        await self._queue.put(session)

    async def close_all(self):
        while not self._queue.empty():
            try:
                session = self._queue.get_nowait()
                await session.close()
            except:
                pass


# ─── Monitor / Circuit Breaker ────────────────────────────────────────

async def monitor_analyze_failures(errors: List[str], notebook_path: str) -> Tuple[str, str]:
    """Ask Opus whether failures are systemic or notebook-specific.
    Returns (verdict: 'systemic'|'notebook_specific', diagnosis).
    """
    error_summary = "\n---\n".join(errors[:5])
    resp, _ = call_opus(
        "You are a Spark migration debugging expert. Analyze these cell execution failures and determine: is this a SYSTEMIC issue (missing dependency, broken cluster config, missing table/path that affects everything) or a NOTEBOOK-SPECIFIC issue (logic error in this particular notebook)? Reply with exactly one line: SYSTEMIC: <reason> or NOTEBOOK_SPECIFIC: <reason>",
        f"Notebook: {notebook_path}\n\nConsecutive failures:\n{error_summary}",
        max_tokens=500
    )
    resp = resp.strip()
    if resp.upper().startswith("SYSTEMIC"):
        return "systemic", resp
    return "notebook_specific", resp


# ─── Cell Migration Prompt ────────────────────────────────────────────

CELL_MIGRATE_PROMPT = """You are migrating a single Databricks cell to OCI AIDP (Python 3.11).

CRITICAL — NO EXECUTION-TIME SCAFFOLDING IN THE MIGRATED NOTEBOOK:
The migrated notebook must contain ONLY changes explicitly authorized by the rules in
this prompt. If a cell works in Databricks, its migrated equivalent should be the same
logic with these EXPLICIT substitutions applied — and nothing else:
  - dbutils.* → oidlUtils.* / aidp_compat.* (API renames listed below)
  - s3://, s3a://, dbfs:// → OCI paths via translate_path / suggest_oci_path
  - %run / notebook.run paths → migrated-path equivalents (MIGRATED DEPENDENCY PATHS)
  - Unsupported APIs → commented out with "# Oracle tool modification: <reason>"

Do NOT add anything else, even if it would make a failing cell pass. The fix you write
gets persisted to the saved notebook forever. Specifically FORBIDDEN:
  - Reading os.environ.get("AIDP_PARAMS") in user code. oidlUtils.parameters.getParameter()
    already reads AIDP_PARAMS internally. Any parallel json.loads(os.environ['AIDP_PARAMS'])
    is forbidden.
  - "if not X: X = json.loads(...) / os.environ.get(...)" defensive fallbacks for values
    that came from oidlUtils.parameters.* or dbutils.widgets.*. Empty means the parameter
    was not passed; surface that, do not paper over it.
  - **TABLE READS STAY AS TABLE READS — UNCONDITIONAL.** spark.read.table(X),
    spark.table(X), and spark.sql("...SELECT...FROM X") must NEVER be rewritten as
    spark.read.parquet(...), spark.read.format(...).load(...), spark.read.load(...),
    spark.read.csv(...), spark.read.json(...), or any other path-based read. AIDP
    supports Unity Catalog identifiers identically to Databricks — the table read
    IS the migration. This applies regardless of try/except wrapping, regardless
    of whether you obtained the path from DESCRIBE FORMATTED / DESCRIBE TABLE
    EXTENDED / describe_table tool, and regardless of whether the table appears
    "missing" or "empty" at probe time. If a table is broken, call make_note()
    and let the original Spark error fire — do not invent a parquet/path rescue.
  - "if len(df.columns) == 0" defensive empty-schema checks anywhere. Trust spark.read.table().
  - Inlining the body of a dbutils.notebook.run() / oidlUtils.notebook.run() target into
    the calling cell. If the call fails, fix the path or arguments — never paste the
    child notebook's cells into the parent. The dep is migrated separately; a clean
    oidlUtils.notebook.run(<migrated_path>, ...) is the correct migration.
  - sys.path.insert/append, !pip install, %pip install in cell code — these go in
    cluster libraries, not in the notebook.
  - try/except blocks that swallow errors silently or substitute defaults for code that
    worked in Databricks.
  - **NEVER inline-define customer writer-wrapper functions** to "fix" a NameError.
    The wrapper-call redirect at exec-time rewrites literal db/bucket args (e.g.,
    `createTable(df, 't', 'analytics_db', ...)` → `createTable(df, 't', '<oci_backup_bucket>_overwrite', ...)`)
    BEFORE the call is sent to the kernel. If the wrapper function is missing
    (NameError), defining a fresh copy of the customer's `def createTable(...)`
    locally in the cell is FORBIDDEN — that copy is NOT what the call site sees
    (the cell-text redirect has already changed the arg), AND if the rewrite missed
    something, the inline copy writes to whatever database_name was passed. The
    forbidden names: createTable, createTableOld, createTableIntermediate,
    createTableIntermediateOld, createTableIntermediate_append, createTableIntermediate_1,
    createTableInApp, createTable_oracle, saveTable, writeTable, write_to_delta,
    process_source, drop_database, drop_table, delete_table. If any of these are
    missing at runtime, call make_note() describing the failure and leave the cell
    code unchanged. The dep needs to be re-loaded — that's a systemic recovery, not
    a per-cell fix.

If the documented substitutions cannot make the cell pass, call make_note() describing
the failure and return the migrated code as-is. A failing cell with a clear note is
better than a "passing" cell with hidden runtime scaffolding that ships to production.

CRITICAL — NEVER drop or delete a cell:
The migrated notebook MUST have the same cell count as the source. At most, comment out
the cell's contents with "# Oracle tool modification: <reason>" — but the cell itself
must remain in the notebook. Utility cells often define helpers (functions, constants)
that downstream cells or other notebooks (via %run) depend on; silently dropping such a
cell causes NameError in callers, often hours later in unrelated tasks. If you cannot
migrate a cell's logic, comment all of it out — never delete.

CRITICAL — Path rewriting safety for %run / dbutils.notebook.run / oidlUtils.notebook.run:
1. NEVER prepend the MIGRATED_BASE prefix to a path that already starts with it. If the
   target path already begins with the migrated-base prefix (e.g.,
   "/Workspace/.../example_ai_notebook_migration/.../notebooks/"), use it as-is. Doubled
   prefixes like ".../notebooks/.../notebooks/..." are always wrong.
2. When matching a `%run` token (e.g., `<long_basename>`) against the MIGRATED DEPENDENCY PATHS list,
   match by EXACT basename equality only — never by prefix. A `%run <long_basename>` lookup must
   NOT resolve to the entry for `<short_basename>` and append the remainder, producing
   ".../<short_basename>.ipynb<digit>.ipynb". If the exact name isn't in the list, leave the original `%run`
   token untouched and call make_note describing the missing dep.
3. Every migrated `%run` path must end in exactly one ".ipynb" suffix. Patterns like
   ".ipynb<digit>.ipynb" (e.g., "<short_basename>.ipynb<digit>.ipynb") or ".ipynb.ipynb" indicate a path
   construction bug — never emit such paths.

CRITICAL — AWS / boto3 / Glue dependencies have NO AWS SDK on AIDP:
ANY AWS-specific code (boto3.client('glue'/'s3'/'secretsmanager'/'sts'/etc.), boto3.resource,
boto3.session.Session, sagemaker SDK, AWS-only imports) WILL fail with ModuleNotFoundError
on AIDP. For each such helper, replace IN-PLACE in the SAME notebook with the AIDP-native
equivalent — DO NOT comment out as "unused". "Unused-in-this-notebook" is NEVER safe because
utility notebooks define helpers consumed via %run by parent notebooks.
Tag every replacement with "# Oracle tool modification: replaced AWS X with AIDP equivalent".
Common patterns:
  - boto3 Glue table location lookups → spark.sql DESCRIBE FORMATTED (see Glue example below)
  - boto3 S3 read/write → spark.read.X(oci://...) / use suggest_oci_path tool first
  - boto3 SecretsManager → aidp_compat.secrets or OCI Vault
For services not listed, define a local AIDP equivalent inline using Spark catalog,
aidp_compat, or OCI SDK — keep the function signature identical so callers don't break.

CRITICAL — DESCRIBE DETAIL is Delta-only and FAILS on AIDP for non-Delta tables:
On AIDP, `spark.sql("DESCRIBE DETAIL <tbl>")` raises "Operation not allowed: DESCRIBE
DETAIL is only supported for Delta tables" whenever the underlying table is parquet,
ORC, CSV, JSON, Iceberg, or any non-Delta format. codebases commonly call
DESCRIBE DETAIL on parquet tables (e.g. SaveTableUtils.createTable looks up the existing
location before appending). Rewrite to DESCRIBE EXTENDED, which works universally on
AIDP — BUT note the result schema differs and the downstream access must change too:

  DESCRIBE DETAIL    → returns 1 Row with NAMED fields:  row['location'], row['format'], …
  DESCRIBE EXTENDED  → returns N Rows with col_name/data_type/comment columns;
                       Location appears as the row where col_name == 'Location'.

Rewrite pattern (apply BOTH the SQL and the result-access change together):

  # Before
  info = spark.sql(f"DESCRIBE DETAIL {tbl}").collect()[0]
  path = info['location']

  # After  -- Oracle tool modification: DESCRIBE DETAIL → DESCRIBE EXTENDED (AIDP-safe)
  rows = spark.sql(f"DESCRIBE EXTENDED {tbl}").collect()
  path = next(r['data_type'] for r in rows if r['col_name'] == 'Location')

Apply this rewrite IN-PLACE wherever you find `DESCRIBE DETAIL` — both inside helper
function bodies (createTable, etc.) and inline at call sites. If the cell only contains
the SQL call and the result-access is in a downstream cell, rewrite both cells in the
same migration pass. NEVER leave `DESCRIBE DETAIL` in migrated code. Do NOT rewrite
`DESCRIBE EXTENDED`, `DESCRIBE FORMATTED`, or `DESCRIBE` (plain) — only `DESCRIBE DETAIL`.

Context:
- aidp_compat is installed as a cluster library (just import it, no sys.path needed)
- spark (SparkSession) is pre-initialized. AIDP runs SPARK CONNECT: in %scala,
  `spark.sparkContext` and `sc` DO NOT EXIST (no `sparkContext` member on SparkSession).
  Never use them or anything derived (e.g. spark.sparkContext.hadoopConfiguration).
- /Workspace/ paths work as-is for local file access. ALL notebooks and files are under /Workspace/
- OCI paths (oci://) work via BmcFilesystem (configured at cluster level with API key auth)
- OCI Python SDK auth: API key via CLI config file at /Workspace/<oci-config-workspace-path>
  (DEFAULT profile). Canonical init pattern when migrating code that creates OCI clients:
    import oci
    _oci_config = oci.config.from_file("/Workspace/<oci-config-workspace-path>", "DEFAULT")
    _oci_signer = oci.signer.Signer(
        tenancy=_oci_config["tenancy"], user=_oci_config["user"],
        fingerprint=_oci_config["fingerprint"],
        private_key_file_location=_oci_config["key_file"],
        pass_phrase=oci.config.get_config_value_or_default(_oci_config, "pass_phrase"),
    )
    _oci_client = oci.object_storage.ObjectStorageClient(config=_oci_config, signer=_oci_signer)
  FORBIDDEN: oci.auth.signers.get_resource_principals_signer() — resource principal has known
  failure modes on AIDP and MUST NEVER be used. If customer code already uses the API-key init
  pattern (oci.config.from_file pointing under /Workspace/), PRESERVE that init code unchanged.
- All required JARs are on classpath (Hudi, your custom JARs, parser, UDF)

CRITICAL — NEVER use direct JVM Hadoop FileSystem calls in migrated notebooks:
The following patterns FAIL in customer's scheduled workflow runs and MUST NOT appear in any
migrated cell:
  spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
  spark.sparkContext._jsc.hadoopConfiguration()  # when used to construct an FS object
  jvm.org.apache.hadoop.fs.Path(...) + jvm.org.apache.hadoop.fs.FileSystem.get(...)
  fs.open(p) / fs.exists(p) / fs.listStatus(p) / fs.delete(p) / fs.rename(p) — when `fs` came
    from the direct jvm.org.apache.hadoop.fs.FileSystem.get(...) call above
These work during exploratory tool execution but break in workflow because the BmcFilesystem
JVM class isn't initialized the same way at scheduled-job execution time. Use these instead:
  - Spark APIs for tabular data: spark.read.parquet("oci://...") / df.write.parquet("oci://...")
    These ARE safe — Spark wires BmcFilesystem internally for SQL/DataFrame ops.
  - OCI Python SDK for non-Spark I/O (read a JSON config, list bucket contents, copy objects):
    Use the API key init pattern above + oci.object_storage.ObjectStorageClient methods like
    .get_object(...), .list_objects(...), .put_object(...). These work in both interactive
    and scheduled execution.
If you migrate boto3/S3 code to OCI, use the OCI Python SDK — NEVER substitute with direct
jvm Hadoop FS calls (that's the trap Opus fell into in past runs). If existing customer code
already uses jvm Hadoop FS for non-Spark I/O, REPLACE it with OCI Python SDK + API key (it
worked interactively in Databricks but won't work in AIDP workflow).

CRITICAL - NOTEBOOK DEPENDENCIES (%run / dbutils.notebook.run / oidlUtils.notebook.run):
- %run is supported on AIDP — keep %run cells as-is, do NOT convert to dbutils.notebook.run().
- Only update the path to the migrated location if a migrated dependency path is available.
  The MIGRATED DEPENDENCY PATHS section below lists the migrated paths.
  Use the EXACT migrated path provided. Do NOT invent paths or guess.
- dbutils.notebook.run() -> oidlUtils.notebook.run() (AIDP native equivalent)
  CRITICAL TIMEOUT: AIDP REJECTS a notebook.run timeoutSeconds of 0 or null
  (error: "timeoutSeconds is null or empty"). The 2nd positional arg of
  notebook.run is the timeout in seconds; Databricks used 0 = "no timeout".
  On AIDP you MUST pass a positive value — use 3600 (or the max supported).
  Applies to BOTH Scala and Python, e.g.:
    oidlUtils.notebook.run("/Workspace/.../child.ipynb", 3600, Map("a"->b))  // never 0
  CRITICAL RESULT-PARSING: when the notebook.run RESULT is parsed as JSON (e.g.
  `new JSONObject(oidlUtils.notebook.run(...))` in Scala, or json.loads(...) in
  Python), the child may legitimately return EMPTY or non-JSON output on AIDP.
  Do NOT parse the result inline. Capture it first and parse DEFENSIVELY — guard
  for empty/non-JSON, use tolerant getters, treat empty as success, and only fail
  on an explicit error status. Scala pattern:
    val raw = oidlUtils.notebook.run("/Workspace/.../child.ipynb", 3600, Map("dateStr"->d))
    if (raw != null && raw.trim.nonEmpty && raw.trim.startsWith("{")) {
      val j = new JSONObject(raw)
      val status = j.optString("status", "OK")   // optString/opt*, NOT getString
      if (status != "OK") throw new RuntimeException(s"child returned status=$status")
    } else {
      println(s"child returned empty/non-JSON output (treated as success): $raw")
    }
  Use opt*/get-with-default (tolerant) instead of getString (which throws on a
  missing key). NEVER wrap notebook.run failures in a silent try/catch that
  swallows them — real errors must propagate.
  PARAM KEYS: the 3rd arg Map keys are the names the CHILD reads via
  getParameter("<key>"). If the original used positional names (arg1/arg2...),
  prefer the child's real parameter names so the child resolves them (e.g.
  Map("dateStr"->d) not Map("arg1"->d)) — read the child with read_notebook_source
  to confirm the names it expects.
- dbutils.notebook.exit() -> oidlUtils.notebook.exit() (AIDP native equivalent)
  CRITICAL: NEVER comment out or replace notebook.exit() with pass/print. notebook.exit() controls
  execution flow — if disabled, all downstream cells execute in a code path that was supposed to be
  unreachable, causing cascading failures that are expensive to debug. ALWAYS translate it.
- dbutils.jobs.taskValues.set() -> oidlUtils.parameters.setTaskValue()
- dbutils.jobs.taskValues.get() -> oidlUtils.parameters.getParameter()
- dbutils.widgets.get("name") -> oidlUtils.parameters.getParameter("name", "")
  Comment out the original dbutils.widgets.get line and add the oidlUtils replacement below it.
  This works in both PySpark and Scala (oidlUtils is a JVM object exposed via Py4J).
- dbutils.widgets.text/dropdown/combobox/multiselect: Comment out these widget registration calls.
  They are not needed in AIDP — workflow parameters come through oidlUtils.parameters.
- oidlUtils is a NATIVE AIDP module — it is pre-loaded in every kernel. Do NOT import it.
  No "from aidp_compat import oidlUtils" or "import oidlUtils" — just use oidlUtils.xxx directly.
- EXTERNAL QUERY ENGINES → AIDP SPARK CATALOG (e.g. Trino, Athena/pyathena): AWS Athena and Trino/Presto are external
  query engines NOT available on AIDP. Do NOT install or connect to them (pyathena = AWS-only;
  trino needs an unreachable endpoint). The same tables are registered in the AIDP Spark catalog
  — read via Spark, KEEPING the SQL text:
    from pyathena import connect / import trino / as_pandas / try-except !pip install  → comment out
    conn = connect(...) / trino.dbapi.connect(...)                                      → remove
    pd.read_sql(QUERY, conn)         → spark.sql(QUERY).toPandas()   # keep pandas shape
    cur.execute(QUERY); cur.fetchall() → spark.sql(QUERY).collect()
    as_pandas(cursor)                → spark.sql(QUERY).toPandas()
  Table names in QUERY: keep schema.table (resolves in catalog); for trino catalog.schema.table
  DROP the leading trino catalog → schema.table. This is a TABLE READ (never a path read). The
  QUERY is Presto/Trino SQL — most ANSI SQL runs as-is in Spark SQL; translate engine-specific
  functions (approx_distinct→approx_count_distinct, cardinality→size; great_circle_distance has
  no Spark builtin → reimplement via Haversine) or make_note. If pyathena/trino are imported but
  never used to query (dead import), just comment the import + make_note — do NOT install/connect.
- COMMENTED-OUT CODE: Leave ALL commented-out code as-is. Do NOT uncomment, migrate, translate,
  or call tools (suggest_oci_path, explore_path, etc.) on code inside comments:
  Python: # line comments, triple-quoted strings (\"\"\"...\"\"\", '''...''')
  Scala: // line comments, /* ... */ block comments
  SQL: -- line comments
  Commented-out s3:// paths, boto3 imports, dbutils calls are dead code from previous migrations.
  Only migrate ACTIVE (uncommented) code. This saves significant time and money.
- If a notebook path is NOT in the migrated dependency list, do NOT call it AND do NOT
  inline its body. Use read_notebook_source to check what the notebook does, then either
  (a) leave the original notebook.run() / %run call commented out and call make_note()
  so the dependency can be migrated and the call restored later, or (b) raise an error
  explaining the dependency is missing. Inlining the child's cells into the parent is
  forbidden — it produces an opaque migrated notebook and divergent execution semantics.
- NEVER convert %run to dbutils.notebook.run() or oidlUtils.notebook.run(). Keep %run as %run.
- AIDP does not support spaces in paths. Replace spaces with underscores in ALL /Workspace/ paths
  in the code — %run paths, file open() paths, string literals referencing /Workspace/ directories, etc.
  (e.g. "/Workspace/Users/foo/My Folder/Utils" → "/Workspace/Users/foo/My_Folder/Utils").

CRITICAL - SLACK / NOTIFICATIONS:
Slack/notification cells are handled automatically — they are converted to Raw cell type with original code preserved.
They will NOT be sent to you for migration. Do NOT modify Slack/notification code if you see references to it in context.

CRITICAL - DBFS PATH TRANSLATION (/dbfs/ and dbfs:/):
In Databricks, /dbfs/ and dbfs:/ are filesystem views of DBFS (backed by S3 mounts).
In AIDP, the equivalent Volume is mounted at /Volumes/{catalog}/{schema}/dbfs/
(catalog and schema both default to 'default' in the customer's AIDP environment).

Translation rules — apply to ALL path strings in the cell, including f-strings and variables:
  /dbfs/FileStore/x  →  /Volumes/default/default/dbfs/FileStore/x
  dbfs:/FileStore/x  →  /Volumes/default/default/dbfs/FileStore/x

Two scenarios:
1. HARDCODED path in notebook (most common):
   BEFORE: base_path = "/dbfs/FileStore/example_user"
   AFTER:  base_path = "/Volumes/default/default/dbfs/FileStore/example_user"
   Or use: from aidp_compat import translate_path; base_path = translate_path("/dbfs/FileStore/example_user")

2. PATH LOADED FROM DB/CONFIG (cell_plan risks: dbfs_path_from_config):
   Wrap runtime values with translate_path() — handles /dbfs/, dbfs:/, s3://, /mnt/ all at once:
   from aidp_compat import translate_path
   raw = spark.sql("SELECT path FROM config.paths WHERE name='model'").collect()[0][0]
   path = translate_path(raw)
   AND call make_note("dbfs_path_from_config: path from DB — verify translation at runtime")

CRITICAL — Path-prefix replacements MUST be idempotent:
NEVER write an unconditional .replace() to translate a path prefix. .replace() runs
on every call and on every occurrence — if the same value is fed in twice (e.g. in a
loop, or because the variable was already translated upstream), or if the prefix
appears inside an already-translated path, the result is mangled.

API REMINDER (don't confuse .startswith with .replace):
  str.startswith(prefix) -> bool          # ONLY tests; does NOT modify the string
  str.replace(old, new)  -> str           # returns a NEW string with all occurrences swapped
You CANNOT do `s = s.startswith("/dbfs/", "/Volumes/...")` — that passes the second
arg as the `start` index (must be an int) and the call returns a bool. ALWAYS use
.startswith inside an `if` guard, then explicitly build the new string.

FORBIDDEN patterns (all produce wrong / mangled output):
  path = path.replace('/dbfs/', '/Volumes/default/default/dbfs/')        # non-idempotent
  path = path.replace('dbfs:/', '/Volumes/default/default/dbfs/')        # non-idempotent
  path = path.replace('s3://bucket', 'oci://oci-bucket@namespace')       # non-idempotent
  path = path.startswith('/dbfs/', '/Volumes/default/default/dbfs/')     # TypeError — wrong API

CORRECT — use one of these forms:

  # Option A — startswith guard, then build the new path explicitly:
  if path.startswith('/dbfs/'):
      path = '/Volumes/default/default/dbfs/' + path[len('/dbfs/'):]

  # Option B (PREFERRED) — translate_path() from aidp_compat. It is idempotent:
  # calling translate_path() on an already-translated path is a no-op. Handles
  # /dbfs/, dbfs:/, /mnt/, s3://, s3a://, /Volumes/, oci:// uniformly.
  from aidp_compat import translate_path
  path = translate_path(path)

Same rule for /mnt/, s3://, s3a://, /Workspace/ prefix substitutions: never use
unconditional .replace() — always guard with .startswith() or delegate to translate_path().

CRITICAL - TABLE READINESS (cell_plan changes_needed: MISSING or EMPTY SCHEMA):
If the cell_plan flags a table as MISSING or EMPTY_SCHEMA, the cell WILL fail at runtime.
DO NOT attempt to fix this in code — it requires data infra action.

For MISSING tables: Table not in AIDP catalog. Must be added to
/Workspace/<deploy_dir>/datafiles/tables_to_migrate.csv (format: ref_name,s3_source_path)
and wait for the hourly scheduled sync job.

For EMPTY_SCHEMA tables: Table shell registered but DESCRIBE returns 0 columns — data
not synced from source catalog. Requires infra investigation.

Action: call make_note("TABLE_BLOCKED: <table_name> is MISSING/EMPTY_SCHEMA — cannot
proceed until registered in AIDP catalog"). If ALL table refs in the cell are blocked, raise:
  RuntimeError("AIDP: Table <name> not available — add to tables_to_migrate.csv and re-run")
If SOME table refs are available, migrate the rest of the code normally. For the blocked
table access itself: leave the spark.read.table() / spark.sql() call as-is (do NOT wrap
it in try/except, do NOT add DESCRIBE TABLE EXTENDED probes, do NOT add spark.read.parquet
location fallbacks, do NOT add "if len(df.columns) == 0" checks). At the top of the cell,
add make_note() identifying the blocked table. The cell will fail at runtime with the
original Spark error, which is the correct signal that data infra action is required.

CRITICAL - OCI STORAGE PATH RULES (only for s3://, s3a://, oci:// paths):
- OCI bucket/namespace resolution ONLY applies when the code contains s3://, s3a://, or oci:// paths.
- All other paths (/FileStore/, /dbfs/, dbfs:/, /mnt/) are local DBFS paths — use translate_path()
  to convert to /Volumes/... as described above. Do NOT explore them in OCI object storage.
- ALREADY-MIGRATED OCI PATHS: If code already contains oci:// paths (especially with migration comments
  like "#Changed by oracle" or "// Changed by oracle"), these were set by a PREVIOUS migration run.
  PRESERVE them exactly as-is. Do NOT call suggest_oci_path on existing oci:// paths.
  Only call suggest_oci_path on s3:// or s3a:// paths that are in ACTIVE (uncommented) code.
- COMMENT-AWARENESS: Only migrate paths in ACTIVE code. Ignore paths inside comments:
  Python/PySpark: # line comments, triple-quoted strings (\"\"\"...\"\"\", '''...''')
  Scala: // line comments, /* ... */ block comments
  SQL: -- line comments
  If the s3:// path is commented out and an oci:// path is already the active replacement, do NOT
  re-map the oci:// path. The migration was already done.
- S3 paths (s3://bucket/...) must be translated to OCI (oci://oci-bucket@namespace/...)
- Use suggest_oci_path tool ONCE to get the correct OCI bucket name + namespace for any S3 path.
  The suggest_oci_path result is authoritative — use it exactly as returned. Do NOT modify the
  bucket name or namespace it returns.
- OCI path format: oci://{oci_bucket}@{namespace}/{sub_path}
- Do NOT guess namespaces. Each OCI bucket has one specific namespace from the mapping.
- After mapping, validate the path exists using explore_path or run_on_cluster — data may not be migrated yet.
- If a path is marked NOT FOUND with a suggested alternative that EXISTS, use the alternative
- PATH NOT FOUND RULE: If the bucket exists but the sub-path does not, do NOT keep guessing other
  buckets or sub-paths. Stop after at most 2 explore_path attempts for the same data. If the data is
  not found, use make_note to record the missing path and continue with the code as-is.
  The data may not be migrated to OCI yet — that is OK, just note it and move on.
- CROSS-TENANCY ACCESS: In AIDP, cross-tenancy OCI buckets require a registered table (real or dummy)
  for access. Direct spark.read from oci://bucket@namespace/... will fail with BucketNotFound even if
  the bucket exists. Therefore:
  1. AIDP LOCATION EXTRACTION (for code that returns a LOCATION STRING — NOT for data reads).

     SCOPE — exactly TWO kinds of sites get touched. Anything else stays byte-identical.

     A) FUNCTION DEFINITION SITES — where a helper is `def`-ed:
            def get_glue_table_s3_location(database, table):
                <body that calls boto3 Glue / does inline DESCRIBE FORMATTED / etc.>
        ACTION: replace ONLY the function body with the canonical AIDP snippet
        below. KEEP the function name, signature, and parameter names IDENTICAL.
        The function STILL RETURNS A PATH STRING — just derived from AIDP
        DESCRIBE FORMATTED instead of AWS Glue.

     B) INLINE EXTRACTION BLOCKS — where DESCRIBE FORMATTED is written out in
        the cell, no helper function:
            df = spark.sql("DESCRIBE FORMATTED ...")
            loc = df.filter(...).collect()[0][0]
        ACTION: replace those extraction lines with the canonical snippet.

     ABSOLUTELY FORBIDDEN — these are the over-migration mistakes we are
     correcting:

       - REWRITING A CALL SITE. A line like
             events_table_path = get_glue_table_s3_location('analytics', 'events_data')
         MUST STAY IDENTICAL. NEVER comment it out and replace with
             events_table_path = "analytics.events_data"
         or any other hardcoded catalog identifier. The call expression is
         correct as-is — the FUNCTION's body is what gets fixed (case A above),
         not the call.

       - HARDCODING A 3-PART CATALOG NAME ("db.schema.table") as the VALUE of a
         *_path / *_location / *_uri / *_s3_location variable. These variables
         hold PATH STRINGS; downstream code like
             spark.read.parquet(f'{events_table_path}/load_date=...')
         depends on them being paths. Hardcoding a catalog identifier corrupts
         every downstream consumer silently.

       - "BECAUSE AIDP READS FROM CATALOG, REPLACE THE CALL WITH A CATALOG
         REFERENCE." This reasoning is WRONG. AIDP returns the catalog table's
         LOCATION via DESCRIBE FORMATTED, which IS A PATH STRING. The function's
         return-contract is unchanged; only its implementation moves to
         DESCRIBE FORMATTED.

     WHEN THE FUNCTION DEFINITION IS NOT IN THIS CELL (only call sites are
     visible): LEAVE THE CALL SITES UNCHANGED. The function is defined in a
     %run-ed dep or imported module; that definition gets migrated when the
     dep is processed. You are NOT allowed to rewrite a call site to compensate
     for what the dep migration may or may not have done.

     WHEN A HELPER FUNCTION IS CALLED BUT NEITHER DEFINED LOCALLY NOR PROVIDED
     BY A KNOWN DEP: insert a canonical inline DEFINITION at the top of THIS
     cell (using the snippet below), with the SAME name and signature the call
     expects. The call site itself still stays unchanged.

     Canonical AIDP location-extraction snippet (the body of the function
     definition, or the replacement for an inline extraction block; argument
     names adjusted to match the surrounding signature):

       # Oracle tool modification: AIDP location extraction (DESCRIBE FORMATTED on AIDP metastore)
       rows = spark.sql(f"DESCRIBE FORMATTED `{database}`.`{table}`").collect()
       location = next(
           (r[1].strip() for r in rows if r[0].strip().lower() == "location"),
           None,
       )
       if not location:
           raise ValueError(f"Location not found for {database}.{table}")
       return location

     Snippet rules:
       - BACKTICKS around `{db}`.`{tbl}` for reserved-word safety.
       - POSITIONAL row[0] / row[1] access — AIDP's DESCRIBE FORMATTED column
         names differ slightly from Databricks; positional access is robust.
       - CASE-INSENSITIVE match (r[0].strip().lower() == "location") — AIDP
         row labels vary in case.

     Comment out (do not delete) the original boto3 imports and Glue client
     setup with "# Oracle tool modification: AWS SDK not available on AIDP".

     THIS PATTERN IS ONLY FOR PATH-STRING-RETURNING CODE. It is FORBIDDEN to
     apply this pattern to spark.read.table() / spark.table() /
     spark.sql("...FROM...") data reads — those stay as table reads (see the
     unconditional rule above).
  2. If search_catalog says a table does not exist, do NOT explore OCI paths for that data — direct
     bucket access will fail anyway. Use make_note to record the missing table and keep original code.
  3. Never hardcode oci:// paths as a replacement for table-based data access.
- SEARCH_CATALOG IS DEFINITIVE: If search_catalog returns no results for a table, the table does NOT exist
  in the catalog. Do not retry with name variations, different schemas, or partial names. Record the missing
  table with make_note and move on.

CRITICAL - MISSING IMPORTS (Databricks implicit vs AIDP explicit):
Databricks pre-imports many symbols into the kernel namespace. AIDP does NOT — every symbol
must be explicitly imported. When you see a name used in the cell without a corresponding
import, ADD the import at the top of the cell and tag it with "# Oracle tool modification: added missing import".

Check every unqualified name used in the cell. Common ones that Databricks provides implicitly
but AIDP requires explicitly:

Python:
- datetime, timedelta, date — add: from datetime import datetime, timedelta, date
- OrderedDict, defaultdict, Counter, namedtuple — add: from collections import <name>
- Any pyspark.sql.functions symbol used without F. prefix (col, lit, when, udf, etc.) — add: from pyspark.sql.functions import <symbol>
- Any pyspark.sql.types symbol used bare (StringType, IntegerType, StructType, etc.) — add: from pyspark.sql.types import <symbol>
- math, re, json, os, sys — add explicit import if used but not imported

Scala (%scala cells):
- JSONObject, JSONArray, JSONException — add: import org.json.JSONObject (etc.)
- SimpleDateFormat — add: import java.text.SimpleDateFormat
- ArrayBuffer — add: import scala.collection.mutable.ArrayBuffer
- Base64 — add: import java.util.Base64
- Any javax.crypto.*, java.nio.*, java.util.zip.* class used without import

Rule: If a symbol is USED in active code and NOT imported anywhere in the cell, add the
import. Tag every added import line with "# Oracle tool modification: added missing import".
Do NOT add imports for names that are clearly pre-initialized on AIDP (spark, sc, oidlUtils, display).

KNOWN MIGRATION PATTERNS — fix these directly during migration, no investigation needed:
- XGBoost gpu_hist: XGBoost 3.x removed 'gpu_hist'. Replace tree_method='gpu_hist' with
  tree_method='hist', device='cuda' everywhere (dicts, function params, space definitions).
- SparkTrials: Databricks-only. Replace SparkTrials(parallelism=N) with Trials().
  Add "from hyperopt import Trials" if not already imported.
- aidp_dbutils: Replace "from aidp_dbutils import _DBUtils" with
  "from aidp_compat import dbutils, displayHTML, sql, translate_path".
  Remove the companion "dbutils = _DBUtils(...)" line.

Rules:
- %pip install: Comment out original with "# AIDP: installed via cluster libraries API" comment.
  Do NOT run pip install via subprocess or run_on_cluster — packages are installed automatically
  by the migration tool via the AIDP cluster libraries API.
- Convert %sh to subprocess.run
- Replace aidp_dbutils imports with: from aidp_compat import dbutils, displayHTML, sql, translate_path
- display is available natively on AIDP — do NOT import it from aidp_compat.
  CRITICAL: AIDP's native `display()` works on **Spark DataFrames ONLY**.
  For everything else, use the library's OWN native display function:
    * Spark DataFrame      : `df.display()`     → `display(df)`           (AIDP native)
    * Pandas DataFrame     : `df.display()`     → `df` as last expression (pandas/IPython auto-render)
                              or explicit form  → `print(df.to_string())`
    * matplotlib figure    : `fig.display()`    → `plt.show()`            (matplotlib native)
    * plotly figure        : `fig.display()`    → `fig.show()`            (plotly native)
    * PIL Image            : `img.display()`    → `img.show()`            (PIL native)
    * numpy array / dict   : `arr.display()`    → `print(arr)`            (Python native)
    * Anything else        : use that library's own show/render/print method.
  Rule: NEVER pass a non-Spark object to AIDP's `display()`. Always use the
  library's NATIVE display function for the object type at hand.
  NEVER monkey-patch `DataFrame.display = ...` (no `_display_patch`, no
  `pyspark.sql.DataFrame.display = ...`, no `df.toPandas()`-backed shim).
  Such patches force `toPandas()`-style materialization which is extremely slow
  on chained Spark transforms and adds zero value during migration.
- Comment out spark.databricks.* configs
- Keep logic identical, only change Databricks-specific APIs

CRITICAL - CHANGE COMMENTS:
When you modify a line, comment out the original line ABOVE the new line, and tag the new line with "# Oracle tool modification:".
When you add a new line, tag it with "# Oracle tool modification:".
Examples:
  # path = "s3://my-bucket/data"  # Original
  path = translate_path("s3://my-bucket/data")  # Oracle tool modification: translated S3 path to OCI
  # spark.conf.set("spark.databricks.delta.optimizeWrite", "true")  # Oracle tool modification: Databricks-only config, commented out
  # from aidp_dbutils import _DBUtils  # Original
  from aidp_compat import dbutils, displayHTML, sql, translate_path  # Oracle tool modification: replaced Databricks dbutils
This makes it easy to identify and review all migration changes in the output notebook.

CRITICAL - MLFLOW:
  AIDP may not have MLflow pre-installed. The migration tool auto-installs missing packages
  via the cluster libraries API — do NOT run pip install yourself.
  1. Comment out any pip install mlflow lines with "# AIDP: installed via cluster libraries API"
  2. Add mlflow.set_tracking_uri() ONCE in the first cell that references mlflow in any way
     (import mlflow, MlflowClient(), mlflow.start_run(), etc.). Databricks pre-imports mlflow
     so notebooks may use it without an explicit import. If there is no `import mlflow`, add it:
       import mlflow
       mlflow.set_tracking_uri("https://admin:<MLFLOW_PASSWORD>@<MLFLOW_HOST>")
     Do not repeat in later cells.
  3. Convert MLflow 2.x deprecated APIs to 3.x equivalents when they fail:
     - get_latest_versions(name, stages=['Production']) → get_model_version_by_alias(name, "production")
     - get_latest_versions(name, stages=['Staging']) → get_model_version_by_alias(name, "staging")
     - Other stage-based APIs → use alias-based equivalents
  Do NOT rewrite MLflow client calls to raw requests.post() REST API calls.
  Do NOT add defensive fallbacks — if the API call fails, fix the API call.
  If MLflow tracking fails after 2 attempts, comment it out with:
    # TODO: MLflow — disabled for AIDP migration; re-enable after configuring AIDP MLflow tracking server
    print("AIDP: MLflow tracking skipped")

# --- FUSE WORKAROUNDS (DISABLED 2026-04-11: AIDP FUSE issues resolved) ---
# Uncomment these sections if FUSE consistency issues resurface on AIDP.
#
# CRITICAL - AIDP SAFE I/O (aidp_compat v0.4.4):
# - Replace pickle.dump()+open() with safe_pickle_dump() and safe_pickle_load() from aidp_compat
# - Replace df.write.parquet(path, mode='overwrite') where df was read from same path with safe_write_parquet() or safe_read_modify_write_parquet()
# - Replace df.write.saveAsTable(name, mode='overwrite') where df was read from same table with safe_save_as_table()
# - Replace pandas df.to_csv() with safe_pandas_to_csv()
# - Replace df.write.json(path) followed by spark.read.json(path) with safe_write_parquet/safe_pandas helpers
# - These handle AIDP /Volumes FUSE write-then-read consistency automatically
#
# CRITICAL - FUSE DOUBLE-READ FileNotFoundError:
# Reading the same /Volumes file more than once causes intermittent FileNotFoundError.
# FUSE evicts the kernel inode cache between reads — the second open() fails.
# Rules:
# 1. Replace ALL open(path, "r") and open(path, "rb") calls on /Volumes paths with safe_read_file()
# 2. For classes that read the same file in multiple methods, add safe_read_file() to every method
# 3. If reading JSON: json.loads(safe_read_file(path))
# 4. pd.read_csv is generally safe — use safe_read_file only for bare open() calls
#
# CRITICAL - TENSORFLOW SAVEDMODEL ON /VOLUMES:
# TF SavedModel files on /Volumes intermittently fail with: Input/output error [Op:RestoreV2]
# Rules:
# 1. After model.save() to /Volumes, inject time.sleep(5)
# 2. Replace tf.keras.models.load_model("/Volumes/...") with load_saved_model_from_volumes()
#
# CRITICAL - STALE SPARK LAZY EVAL:
# Spark lazy evaluation problem — inject safe_materialize() before write on flagged DataFrames.
# from aidp_compat import safe_materialize, safe_unpersist
# df = safe_materialize(df); df.write...; safe_unpersist(df)
#
# CRITICAL - PACKAGE FUSE RISKS:
# joblib: safe_joblib_dump/safe_joblib_load
# optuna: safe_optuna_create_study/finalize_optuna_study
# torch: save to /tmp/ then copy to /Volumes/
# h5py: use /tmp/ path, close file, then shutil.copy2() to /Volumes/
# sqlite3: connect to /tmp/db.sqlite; copy to /Volumes/ after conn.close()
# xgboost/lightgbm/catboost: add time.sleep(3) before model load
# --- END FUSE WORKAROUNDS ---

CRITICAL - SCALA AND SQL MAGIC CELLS (%scala, %sql):
AIDP supports Scala and SQL natively. By default, %scala and %sql cells are preserved as-is
and NOT sent for migration. Do NOT convert %scala or %sql cells to PySpark/Python.

EXCEPTION — %scala cells that use the Cassandra/ScyllaDB Spark connector (any of:
CassandraConnector, withSessionDo, session.execute, SimpleStatement,
com.datastax.spark.connector.*, "org.apache.spark.sql.cassandra" format string,
system_schema.tables enumeration) MUST be migrated to AIDP metastore Spark SQL while
KEEPING THE %scala MAGIC. Do NOT port to Python. Only swap the Cassandra API for
spark.sql / Spark DataFrame APIs. See "SCYLLADB → AIDP METASTORE MIGRATION" section
below for exact substitutions.

=== SCYLLADB → AIDP METASTORE MIGRATION (Cassandra-bearing %scala cells only) ===

Assumption: every ScyllaDB keyspace is mirrored to the AIDP metastore as a schema named
`scylla_<keyspace>` (the original keyspace name prefixed with `scylla_` for easy
identification). Every Scylla table is registered as a Spark table with the SAME table
name. Reference shape:  spark.sql("…  scylla_$keyspace.$table  …").
Concrete example: Scylla keyspace `sample_keyspace` → AIDP metastore schema
`scylla_sample_keyspace`; table `foo` → `scylla_sample_keyspace.foo`.

THIS IS A TEMPORARY MAPPING. Mark every migrated cell with this header (immediately
after the %scala magic line):

  // Oracle tool modification: ScyllaDB → AIDP metastore (temporary).
  //   Cassandra Cluster/Session calls removed; reads/writes now go through the
  //   Spark catalog (schema name = `scylla_<keyspace>`, table name preserved).
  //   Existence guards added so the cell no-ops if the metastore mirror is not
  //   yet populated. Revisit if Scylla becomes directly reachable on AIDP.

REMOVE entirely:
  import com.datastax.spark.connector.cql.CassandraConnector
  import com.datastax.spark.connector.*                  // any cassandra-spark-connector import
  import scala.collection.JavaConverters._               // only when used ONLY for Cassandra ResultSet conversion
  val cluster = ...; val session = ...                   // any Cassandra cluster/session construction
  CassandraConnector(spark.sparkContext).withSessionDo { session => BODY }
    → inline BODY at the top level (no wrapper); reference `spark` directly

REPLACE — reads:
  session.execute(s"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '$ks'")
    .all().asScala.map(_.getString("table_name"))
  → spark.sql(s"SHOW TABLES IN scylla_$ks").collect().map(_.getString(1))   // col 1 = tableName

  session.execute(s"SELECT col1, col2 FROM $ks.$t WHERE …")
  → spark.sql(s"SELECT col1, col2 FROM scylla_$ks.$t WHERE …").collect()    // or .toDF() for DataFrame chain

  session.execute(simpleStatement)                       // SimpleStatement-wrapped CQL
  → strip the SimpleStatement(...) wrapper; pass the bare query string to spark.sql(...) (prefix the schema with scylla_)

  spark.read.format("org.apache.spark.sql.cassandra")
       .options(Map("keyspace" -> ks, "table" -> t)).load()
  → spark.read.table(s"scylla_$ks.$t")

REPLACE — writes (ALWAYS use mode("overwrite") — see Write-Mode rule below):
  session.execute(s"INSERT INTO $ks.$t (col1, col2) VALUES ('a', 1)")
  → spark.sql(s"INSERT OVERWRITE scylla_$ks.$t VALUES ('a', 1)")             // overwrite, not append

  session.execute(s"UPDATE $ks.$t SET col=val WHERE pk=?", bindings)
  → spark.sql(s"UPDATE scylla_$ks.$t SET col=val WHERE pk=…")                // Delta only; call make_note if non-Delta

  session.execute(s"DELETE FROM $ks.$t WHERE …")
  → spark.sql(s"DELETE FROM scylla_$ks.$t WHERE …")                          // Delta only; call make_note if non-Delta

  df.write.format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> ks, "table" -> t)).save()
  → df.write.mode("overwrite").saveAsTable(s"scylla_$ks.$t")                 // ALWAYS overwrite — temporary mapping
                                                                              // add Write-Mode comment (see below)

REPLACE — DDL:
  session.execute(s"DROP TABLE $ks.$t")
  → spark.sql(s"DROP TABLE IF EXISTS scylla_$ks.$t")                         // ALWAYS add IF EXISTS

  session.execute(s"TRUNCATE $ks.$t")
  → spark.sql(s"TRUNCATE TABLE scylla_$ks.$t")

  session.execute(s"CREATE TABLE $ks.$t (col1 TYPE, col2 TYPE, PRIMARY KEY (col1)) WITH …")
  → spark.sql(s"CREATE TABLE IF NOT EXISTS scylla_$ks.$t (col1 TYPE, col2 TYPE) USING DELTA")
    // drop CQL-specific PRIMARY KEY / clustering / WITH options; preserve column types

PreparedStatement / BoundStatement:
  val ps = session.prepare("INSERT INTO …"); session.execute(ps.bind(a, b))
  → strip the prepare/bind dance; build the SQL string and pass to spark.sql(...) (prefix schema with scylla_)
  Prefer DataFrame writes for bulk inserts: bind values → Row → spark.createDataFrame(...).write...

Batch statements:
  BEGIN BATCH stmt1; stmt2; … APPLY BATCH
  → unroll: spark.sql(stmt1); spark.sql(stmt2); … (no transactional guarantee — acceptable for temporary mapping)

WRITE-MODE rule (mandatory for ALL writes to a Scylla-mirrored table):
  Always use .mode("overwrite") for DataFrame writes and INSERT OVERWRITE for SQL writes.
  Original Cassandra default is upsert; closest temporary Spark-side equivalent that gives
  deterministic results is overwrite. Mark every write with this exact inline comment:
    // Oracle tool modification: write mode forced to "overwrite" (was Cassandra upsert).
    // Temporary — restore original semantics when Scylla becomes directly reachable on AIDP.

EXISTENCE GUARDS — mandatory; inject before any operation that touches scylla_$ks.*:
  - Before SHOW TABLES IN scylla_$ks / any read on scylla_$ks.* / any DDL on scylla_$ks.*:
      val _schemas = spark.sql("SHOW SCHEMAS").collect().map(_.getString(0)).toSet
      if (!_schemas.contains(s"scylla_$keyspace")) {
        println(s"AIDP: schema 'scylla_$keyspace' not in metastore — skipping cell.")
      } else {
        // ... migrated body ...
      }
  - DROP statements ALWAYS use IF EXISTS (covers single-table absence).
  - For per-table reads/writes/updates/deletes (operating on a known table name), wrap with:
      val _tables = spark.sql(s"SHOW TABLES IN scylla_$ks").collect().map(_.getString(1)).toSet
      if (!_tables.contains(tableName)) {
        println(s"AIDP: table scylla_$ks.$tableName not present — skipping")
      } else {
        // ... read/write logic ...
      }

CQL-SPECIFIC SYNTAX HANDLING:
  - ALLOW FILTERING                              → drop the clause (Spark scans by default)
  - USING TTL / USING TIMESTAMP                  → drop and call make_note (no Spark equivalent)
  - IF EXISTS / IF NOT EXISTS lightweight txn    → drop and call make_note (not supported in Spark)
  - BEGIN BATCH … APPLY BATCH                    → unroll to sequential spark.sql(…) calls
  - Materialized views / secondary indexes / UDTs → call make_note for manual review

AFTER MIGRATION: cell MUST have ZERO references to: cassandra, Cluster, Session,
SimpleStatement, withSessionDo, system_schema, com.datastax. If any survive, call
make_note() to flag the cell for manual review.

=== END SCYLLADB → AIDP METASTORE MIGRATION ===

CRITICAL - DO NOT CHANGE SOURCE CODE LOGIC:
- Do NOT convert pandas to PySpark (e.g. pd.read_csv → spark.read.format('csv') is WRONG)
- Do NOT convert PySpark to pandas
- Do NOT change data processing logic, algorithms, or library choices
- Do NOT remove or stub code because you think a variable is "unused" or "dead code".
  You do NOT have full visibility into how variables are used across all cells and notebooks.
  Translate the code as-is — path translation, API changes — nothing more.
- Do NOT replace file reads (pickle.load, open, pd.read_csv, etc.) with hardcoded values.
  If the file path needs translation (e.g. /dbfs/ → /Volumes/), translate the path and keep the read.
- Do NOT invent problems that don't exist. Do NOT add workarounds for hypothetical issues
  (e.g. "unmocking" libraries, clearing sys.modules, invalidating caches, writing to /tmp
  then copying to /Volumes for "FUSE consistency") unless there is a REAL error during
  execution that requires it. FUSE issues are resolved on AIDP — write directly to /Volumes.
- Do NOT add defensive guards, null checks, or error handling that the original code didn't have.
  If the original code does `datay[:, 0][:5]`, keep it as-is — do NOT wrap it in shape checks.
  If it fails at runtime, the fix loop will handle it.
- Only change Databricks-specific APIs, paths, and infrastructure references
- Python/pandas version upgrades are OK (e.g. df.append → pd.concat for pandas 2.x)
- matplotlib: If a cell uses matplotlib for plotting/saving figures, just add at the top:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
  The 'Agg' backend is needed for headless environments (no display). No other changes needed.

CRITICAL - CODE QUALITY:
- No TODO comments, no placeholder code, no "implement later" stubs
  (EXCEPTION: unfixable Databricks infrastructure — see UNFIXABLE DATABRICKS INFRASTRUCTURE section above)
- No fallback/hack workarounds - fix the root cause
- No try/except that silently swallows errors without logging
- Do NOT add migration debug comments, fix-log annotations, or notes about the migration process
  into the submitted code. Use the make_note tool instead. The delivered code must look like
  production code, not migration output.

You MUST call the submit_code tool to return your migrated Python code. Do NOT return code as text.
The code field must contain ONLY valid executable Python. No markdown, no prose.

EXECUTION CONTEXT — READ BEFORE USING TOOLS:
- You are executing cells one at a time in a shared Spark kernel session.
- The kernel state is cumulative: all variables, imports, and DataFrames from previous cells are available.
- Child notebooks (%run / oidlUtils.notebook.run) are inlined: their cells execute in this same kernel namespace in sequence before the parent continues.
- The JOB EXECUTION HISTORY shown above lists every cell executed so far in this job (parent and child cells). Use it to understand the context of the current cell.

CELL HISTORY TOOLS (use when you suspect a root cause is upstream):
- make_note(note): record a migration concern or observation about this cell (max 500 chars). Call proactively when you notice patterns that may cause downstream failures.
- get_cell_history(from_index, to_index): scan history with summaries to find the upstream root cause. Supports negative from_index (e.g. -20 = last 20 entries).
- get_history_entry(index): get full code and output for a specific history entry.
- fixup_cell(start_index, why, fixed_code): rewind to a history index and replay all cells from there to current through the full execute+verify+fix loop. IDEMPOTENCY WARNING: replay re-executes every cell in the sequence — only use when cells are idempotent (no double file writes, no duplicate external calls).

CRITICAL - KNOWN AIDP MIGRATION PATTERNS:

DATA-FIRST PRINCIPLE: Before commenting out ANY cell, determine whether it produces a variable
or DataFrame that downstream cells depend on. If yes, you MUST find a working AIDP equivalent —
NOT a no-op. Use run_on_cluster, describe_table, explore_path, and suggest_oci_path to investigate.
Only comment out side-effect-only calls (triggers, notifications, metrics) that produce NO data
downstream — and even then, replace with print() confirming what was skipped.

DATA SOURCE REPLACEMENTS (must produce equivalent data):
- get_glue_table_s3_location(db, tbl): Use spark.sql(f"DESCRIBE FORMATTED {db}.{tbl}").collect()
  to extract the Location field. Or: from aidp_compat import get_glue_table_s3_location (drop-in shim).
  First use describe_table tool to verify the table exists in the AIDP catalog.
- boto3.client('s3') — ONLY when code uses boto3/botocore directly for S3 object access
  (get_object, put_object, list_objects, download_file, upload_file, etc.).
  Do NOT convert if the S3 path is accessed via Spark (spark.read), pandas, or other libraries
  that can use oci:// paths directly — for those, just change the path to oci://.

  AWS S3 is NOT accessible from AIDP — no AWS credentials. Convert direct boto3 S3 calls to
  OCI SDK with API key auth. The OCI bucket name = 'oci-' + S3 bucket name,
  namespace = '<WORKSPACE_NAMESPACE>'. Use suggest_oci_path to confirm.

  Setup (once per notebook):
    import oci, os
    _config_path = '/Workspace/<deploy_dir>/config'
    if not os.path.exists(_config_path):
        _config_path = '/Workspace/<deploy_dir>/config'
    _oci_config = oci.config.from_file(file_location=_config_path)
    _os_client = oci.object_storage.ObjectStorageClient(_oci_config)

  READ pattern (boto3 get_object → OCI get_object):
    response = _os_client.get_object('<WORKSPACE_NAMESPACE>', f'oci-{bucket_name}', s3_key)
    content = response.data.content.decode('utf-8')

  WRITE pattern (boto3 put_object → OCI put_object):
    _os_client.put_object('<WORKSPACE_NAMESPACE>', f'oci-{bucket_name}', s3_key, data)

  LIST pattern (boto3 list_objects_v2 → OCI list_objects):
    resp = _os_client.list_objects('<WORKSPACE_NAMESPACE>', f'oci-{bucket_name}', prefix=prefix)
    objects = resp.data.objects  # list of ObjectSummary

  IMPORTANT: When a function wraps boto3 (e.g. read_data_from_s3, dump_data_in_s3), rewrite the
  ENTIRE function body — do not just change the caller. The function itself must use OCI SDK.
  Also replace boto3/botocore imports with oci imports.
- boto3.client('glue').get_table(...): Use spark.sql('DESCRIBE FORMATTED db.table') or describe_table tool.
- s3a:// or s3:// paths in spark.read/spark.write: Use suggest_oci_path to translate to oci://bucket@namespace/key.
  These do NOT need OCI SDK — Spark can read/write oci:// paths natively.
- spark.read.format("org.apache.spark.sql.cassandra"): Use explore_path/describe_table to check
  if data was pre-exported to OCI or a Spark table. If found, read from there. If NOT found, raise
  RuntimeError("AIDP: Cassandra source not migrated — data at <source> not found in OCI catalog").
  NEVER silently return empty data.
- spark.read.format("hudi"): Keep the format — Hudi is supported on AIDP. Add comment:
  # AIDP: requires hudi-spark3.5-bundle_2.12-0.15.0.jar in cluster libs + spark.serializer=KryoSerializer

SIDE-EFFECT-ONLY CALLS (safe to comment out — these produce NO data):
IMPORTANT: Always keep original code as comments for reference, then add a print() after.
- JobRunAPI(...) / requests.post to api/2.0/jobs/run-now:
  Comment out original code, add: print("AIDP: Skipped Databricks job trigger — not applicable on OCI")
- internal-host.example / internal-gateway.example HTTP calls (notifications/pings only):
  Comment out original code, add: print("AIDP: Skipped call to internal AWS endpoint: <url>")
  If the call FETCHES DATA: use run_on_cluster + explore_path to find that data in OCI first.
- %pip install cells: Comment out original with "# AIDP: installed via cluster libraries API" comment.
  Do NOT run pip install via subprocess or run_on_cluster — the migration tool auto-installs packages.

UNFIXABLE DATABRICKS INFRASTRUCTURE (stub on first attempt — do NOT retry):
When a cell depends on Databricks-specific infrastructure that has NO AIDP equivalent,
do NOT attempt to make the call work. Instead, on the FIRST attempt:
1. Comment out the original code so it remains as a reference
2. Stub the return values so downstream cells can proceed
3. Add a print() explaining what was stubbed
4. Use make_note() to flag the concern

Detect by ANY of these signals:
- Direct Databricks REST API calls: requests.post/get to /api/2.0/jobs/*, /api/2.0/clusters/*
- Hardcoded Databricks job_id (large integer like <DATABRICKS_JOB_ID>) in any function call
- Wrapper functions that trigger Databricks jobs: run_job(), trigger_job(), submit_job(), etc.
  with job_id or run_name parameters
- AWS Secrets Manager / boto3.client('secretsmanager')
- Databricks cluster policies / dbutils.secrets
- Databricks-specific webhooks or internal URLs

Pattern:
  # --- STUBBED: Databricks job trigger not available on AIDP ---
  # Original:
  # job_result = run_job({"run_name": model_name, "job_id": <DATABRICKS_JOB_ID>, ...})
  #
  # Stub: preserve downstream dependency contract
  job_result = {"status": "STUBBED", "note": "Databricks job trigger not available on AIDP"}
  print("STUBBED: Databricks job trigger — original code preserved in comments above")

Rules:
- Identify ALL variables/DataFrames the cell produces that downstream cells may read
- Stub each with a sensible default (empty dict, -1, empty DataFrame with correct schema, etc.)
- If unsure what downstream cells need, use get_cell_history to check
- NEVER leave a stub that would cause a NameError in a later cell
- Use make_note("STUBBED: <what> — <why>") so the stub is tracked in migration notes

CODE FIXES (direct replacements, no data loss):
- datetime.now() / timedelta without import: Add from datetime import datetime, timedelta, date at cell top
- spark_catalog.schema.table: Replace with schema.table (spark_catalog catalog does not exist on AIDP)
- Table references: AIDP uses 3-part names (catalog.schema.table) with "default" as the catalog.
  If a table name is 2-part (schema.table), try default.schema.table.
  If a table name is 3-part with catalog `main` (or any source catalog that does not exist on
  AIDP), replace ONLY the catalog with `default`, keeping schema and table unchanged
  (e.g. main.sample_schema.t -> default.sample_schema.t). These are the ONLY fixes.
  Tables match exactly or they don't exist — do NOT explore, search variations, or guess names.
  If a table appears in VERIFIED TABLES as EXISTS, trust it — do NOT re-check.
  If a table does NOT exist after the 3-part name fix, use make_note to record it and keep
  original code. Do NOT try workarounds like CSV files or alternative sources.
- DELETE FROM / UPDATE / MERGE statements: Keep code as-is — do NOT rewrite.
  These are source-side logic and must be preserved exactly. Skip execution during migration
  (destructive operations can cause data loss). Validate syntax only.
- spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", ...):
  Replace with .option("mergeSchema", "true") on the write call
- dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson():
  The aidp_compat shim already handles this — dbutils.notebook.entry_point is mocked.
  If the code imports dbutils from aidp_compat, no change needed. If it constructs its
  own dbutils or calls entry_point without dbutils, replace with:
  dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
- dbutils.notebook.exit("message") -> oidlUtils.notebook.exit("message")
- dbutils.notebook.run("path", timeout) -> oidlUtils.notebook.run("path", timeout)
- /Volumes path in os.makedirs: Keep the full /Volumes/... path — never strip the prefix
- Hardcoded dapi... token in request headers: Replace with os.environ.get('DATABRICKS_TOKEN', '')
- df.write.mode('overwrite').saveAsTable(tbl) immediately followed by spark.table(tbl):
  Prefer (a) reuse the DataFrame variable directly, or (b) spark.catalog.refreshTable(tbl) between write and read:
    df.write.mode("overwrite").saveAsTable(tbl)
    spark.catalog.refreshTable(tbl)  # AIDP: ensure catalog sees updated table before read
    result = spark.table(tbl)

WHEN TO CONSIDER REWINDING: If you are on attempt 4 or higher (out of 5) and the error appears to have a root cause in an earlier cell, prefer fixup_cell over continuing to patch this cell."""


# Spark performance config cell prepended to every migrated notebook
AIDP_SPARK_CONFIG_CELL = """\
%python
# AIDP performance configuration — applied automatically by migration tool
# Some configs are cluster-level static (e.g. maxResultSize) — wrap all in try/except
for _k, _v in [
    ("spark.sql.execution.arrow.pyspark.enabled", "false"),
    # spark.driver.maxResultSize is static — set at cluster level, not at runtime
    ("spark.sql.execution.arrow.maxRecordsPerBatch", "100"),  # LOW: prevents driver OOM on 64GB cluster when converting large DFs via toPandas()
    ("spark.sql.constraintPropagation.enabled", "false"),
    ("spark.sql.execution.arrow.pyspark.fallback.enabled", "true"),
]:
    try:
        spark.conf.set(_k, _v)
    except Exception:
        pass  # static config — set at cluster level
"""

# Marker substring identifying the AIDP config cell. NOTE: the tool no longer
# prepends this cell to migrated notebooks (perf-config injection was removed).
# The marker + offset logic is RETAINED only for back-compat: notebooks migrated
# by an earlier tool version still carry the cell on disk, and _inline_child_notebook()
# must skip it when re-loading such a dep — otherwise child_cells would include
# the config cell and the save-back's `code_idx - config_offset` index math would
# shift every real cell by one (duplicating the cell at position 1 and dropping
# the last cell's migration). Freshly migrated notebooks have no such cell.
_CONFIG_MARKER = "# AIDP performance configuration"

# ── Data-recovery override block markers ─────────────────────────────
#
# When a cell fails with an empty-data signature (e.g. IndexError after a
# .collect()[0] on a date-filtered query), the migration tool attempts a
# one-shot "data recovery": query the upstream table for actual available
# dates and prepend an override block to the EXEC code that redefines the
# date variables. The cell then runs with the overrides; on success, the
# cell is tagged OK_DATA_SUBSTITUTED. The override block is BRACKETED by
# these markers so it can be reliably stripped before the migrated cell is
# saved to disk — the saved notebook must contain the customer's ORIGINAL
# date filter, not our test-only substitution.
_DATA_RECOVERY_BEGIN = "# === AIDP_DATA_RECOVERY_OVERRIDE_BEGIN (test-only — not saved) ==="
_DATA_RECOVERY_END   = "# === AIDP_DATA_RECOVERY_OVERRIDE_END ==="

# Regex that matches the whole override block including surrounding newlines.
# Strict: requires both markers; if END is missing, leaves source untouched
# (fail-safe — never accidentally strip user code).
_DATA_RECOVERY_BLOCK_RE = re.compile(
    rf"(?:^|\n){re.escape(_DATA_RECOVERY_BEGIN)}\n.*?\n{re.escape(_DATA_RECOVERY_END)}(?:\n|$)",
    re.DOTALL,
)


def _strip_data_recovery_block(source: str) -> str:
    """Remove any AIDP_DATA_RECOVERY_OVERRIDE block from source.

    Used before persisting a migrated cell to disk so the override (which
    exists only to make migration-time execution succeed) does NOT end up in
    the customer's saved notebook. Safe to call on sources that don't have
    the block — it's a no-op in that case. Fail-safe: requires both markers
    to strip; an orphan BEGIN with no END is left intact (rather than
    swallowing the rest of the cell).
    """
    if not source or _DATA_RECOVERY_BEGIN not in source:
        return source
    return _DATA_RECOVERY_BLOCK_RE.sub("\n", source).lstrip("\n")


# ── Empty-data failure classifier ────────────────────────────────────
#
# A cell that does `result = spark.sql(...).filter(...).collect()[0]` and
# whose filter returns 0 rows will raise `IndexError: list index out of
# range` at the indexed-access line. This is NOT a code error; the code is
# correct, the data is missing. We classify these failures distinctly so
# the fix loop can attempt a data-recovery substitution (different dates)
# instead of burning Opus retries trying to "fix" the unfixable.
_EMPTY_DATA_OUTPUT_PATTERNS = (
    "IndexError: list index out of range",
    "IllegalArgumentException: requirement failed: Required input is empty",
    "AnalysisException: Path does not exist",
    "PartitionNotFoundException",
)
_INDEXED_ACCESS_RE = re.compile(
    r"\.(?:collect\(\)\s*\[\s*\d+\s*\]|first\(\)|head\(\s*\d*\s*\))"
)


def _is_empty_data_failure(output: str, exec_code: str) -> bool:
    """Classify a cell failure as 'empty data' (not 'code error').

    Returns True when one of the following is observed:
      - `IndexError: list index out of range` appears in the output AND the
        executed source contains an indexed-access pattern after a query
        (.collect()[0], .first(), .head()).
      - The output contains a direct empty-result/partition-missing
        exception signature.

    Conservative: a cell with NO indexed access that raises IndexError is
    likely a real bug (not a data issue) — don't classify as empty-data.
    """
    if not output:
        return False
    out = output[:6000]  # bound scan
    # Direct empty-result exceptions
    for pat in _EMPTY_DATA_OUTPUT_PATTERNS[1:]:
        if pat in out:
            return True
    # IndexError requires the indexed-access pattern in the cell
    if "IndexError: list index out of range" in out:
        if exec_code and _INDEXED_ACCESS_RE.search(exec_code):
            return True
    return False


# ── Write-cell heuristic ─────────────────────────────────────────────
#
# Data-recovery substitution is only safe for READ_ONLY cells. A cell that
# WRITES (saveAsTable, .write.*, INSERT, DROP, etc.) with substituted dates
# would write to actual destinations with the wrong dates. Conservative:
# any write-like signature → not a candidate for data recovery.
_WRITE_OP_RE = re.compile(
    r"\.\s*write\s*\.|"
    r"\.saveAsTable\s*\(|"
    r"\.insertInto\s*\(|"
    r"\.writeTo\s*\(|"
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE|"
    r"DROP\s+TABLE|"
    r"DELETE\s+FROM|"
    r"INSERT\s+(?:INTO|OVERWRITE)|"
    r"TRUNCATE\s+TABLE|"
    r"\bput_object\s*\(|"
    r"\bdelete_object\s*\(|"
    r"os\.remove\s*\(|"
    r"shutil\.rmtree\s*\(",
    re.IGNORECASE,
)


def _is_write_cell(source: str) -> bool:
    """Heuristic: does this cell perform any write/destructive operation?

    Returns True if any write/DDL/destructive signature is found in the
    cell's active (uncommented) source. Conservative — better to skip
    data-recovery for a borderline cell than to write with substituted
    dates to a real destination.
    """
    if not source:
        return False
    active = _active_lines(source)
    return bool(_WRITE_OP_RE.search(active))


# ── Write-redirect (tool-only): never touch production data ───────────
#
# During migration tool execution, every write/INSERT/UPDATE/DELETE/MERGE/
# CREATE/DROP/TRUNCATE on a real OCI path or catalog table is redirected to
# a TOOL-OWNED destination so customer production data is never touched.
# The SAVED notebook keeps the original paths/tables intact;
# only the cluster-executed code (`exec_code`) is rewritten.
#
# Redirect targets:
#   • Tables: default.<oci_backup_bucket>_overwrite.<db>_<tbl>
#             where <db>_<tbl> comes from canonicalizing the customer's
#             identifier (1-/2-/3-part → 3-tuple → schema_table). Catalog
#             is dropped because AIDP only uses "default" anyway.
#   • Paths:  oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/<orig-suffix>
#             where <orig-suffix> is everything after `bucket@ns/` in the
#             original — preserves file/folder structure for predictability.
#
# Read semantics (Option A — data isolation):
#   • A read of `T` consults the redirect map. If a prior cell in this job
#     wrote to `T`, the read substitutes to `T'` (the redirect target).
#   • If `T` was NEVER written by the tool in this job, the read is left
#     as-is (reads customer's production — non-destructive).
#   • Net effect: tool never WRITES to production. Reads of "not-yet-
#     redirected" targets go to production. Downstream cells that depend
#     on the appended/inserted data see only what the tool itself wrote
#     (not the union with prior production data). Operators may accept this
#     because at customer-runtime the original code reads/writes production
#     as intended.
#
# Scope:
#   • Map is module-level and persists across tasks within the same job.
#     Reset between jobs via `clear_write_redirect_map()`.
#   • Skip `/Volumes/...`, `/Workspace/...` (AIDP-local FUSE; not OCI).
#   • Skip targets already in the tool namespace (idempotency on retries).

_REDIRECT_TABLE_PREFIX  = "default.<oci_backup_bucket>_overwrite"
_REDIRECT_BUCKET        = "<oci_backup_bucket>"
_REDIRECT_NAMESPACE     = "<WORKSPACE_NAMESPACE>"   # AIDP workspace namespace

# Module-level state. Reset via clear_write_redirect_map() between jobs.
# Two maps keyed by canonical form; the redirect-substitution functions
# normalize incoming identifiers to the canonical form before lookup so
# 2-part and 3-part references to the same logical table hit the same
# entry.
_write_redirect_table_map: Dict[Tuple[str, str, str], str] = {}
_write_redirect_path_map:  Dict[str, str] = {}
_write_redirect_log: List[Dict] = []         # per-entry audit for migration_report
_write_redirect_collisions: List[Dict] = []  # entries flagged for review


def clear_write_redirect_map() -> None:
    """Reset the job-wide write-redirect state. Call at the start of each
    new job migration. (Within a single job, the map persists across tasks
    so cross-task reads of tool-written tables resolve to the redirected
    target.)"""
    _write_redirect_table_map.clear()
    _write_redirect_path_map.clear()
    _write_redirect_log.clear()
    _write_redirect_collisions.clear()


def _strip_backticks(s: str) -> str:
    """Strip backticks and whitespace from a SQL-quoted identifier part."""
    return s.strip().strip("`").strip()


def _canonicalize_table_id(raw: str) -> Optional[Tuple[str, str, str]]:
    """Normalize a 1/2/3-part table identifier to (catalog, db, tbl).

      "tbl"             → ("default", "default", "tbl")
      "db.tbl"          → ("default", "db",      "tbl")
      "catalog.db.tbl"  → ("catalog", "db",      "tbl")

    Handles backtick-quoted identifiers. Returns None if the identifier is
    malformed (>3 parts, empty parts after stripping) so callers can skip.
    """
    if not raw or not raw.strip():
        return None
    parts = [_strip_backticks(p) for p in raw.split(".")]
    if any(not p for p in parts):
        return None
    if len(parts) == 1:
        return ("default", "default", parts[0])
    if len(parts) == 2:
        return ("default", parts[0], parts[1])
    if len(parts) == 3:
        return (parts[0], parts[1], parts[2])
    return None


def _redirect_target_for_table(canonical: Tuple[str, str, str]) -> str:
    """Build the redirected 3-part table name for a canonical identifier.
    Format: default.<oci_backup_bucket>_overwrite.<db>_<tbl>.
    Catalog from the canonical tuple is intentionally dropped because AIDP
    uses 'default' uniformly."""
    _, db, tbl = canonical
    return f"{_REDIRECT_TABLE_PREFIX}.{db}_{tbl}"


# Clusters on which the write-redirect schema has been created + verified.
# Per-CLUSTER, not per-process: each task may run on a different cluster, and
# the sandbox schema must exist on whichever cluster a task executes on.
_redirect_schema_ensured_clusters: set = set()

async def _ensure_redirect_schema(session) -> None:
    """Create the execution-time write-redirect sandbox schema if it is missing,
    on the CURRENTLY CONNECTED cluster.

    Redirected writes (see _REDIRECT_TABLE_PREFIX) land in this schema during
    migration execution so source/production data is never touched. The schema
    must exist before the first redirected write. MUST be called on a live
    connection (session.cluster_id set) — it executes CREATE SCHEMA on the
    cluster. Runs once per cluster (tracked in _redirect_schema_ensured_clusters);
    each task may run on a different cluster, so this is invoked per task after
    that task's connect. Each dotted part is backtick-quoted so the dot is a
    namespace separator, not part of one identifier.
    """
    cluster_id = getattr(session, "cluster_id", None)
    if cluster_id is None:
        # Misuse: this is a data-safety gate; silently skipping would let
        # redirected writes fall through to real production tables. Caller
        # MUST re-invoke after connecting to the per-task cluster.
        raise ValueError(
            "_ensure_redirect_schema requires a live cluster session "
            "(session.cluster_id is None). Caller must invoke this AFTER the "
            "per-task connect — the data-safety contract depends on the "
            "redirect schema existing on the cluster about to receive writes."
        )
    if cluster_id in _redirect_schema_ensured_clusters:
        return
    schema_name = _REDIRECT_TABLE_PREFIX.split(".")[-1]
    quoted = ".".join(f"`{p}`" for p in _REDIRECT_TABLE_PREFIX.split("."))
    # VERIFY, don't assume. A `CREATE SCHEMA IF NOT EXISTS` can return without
    # raising yet leave the schema absent (observed on AIDP — the call silently
    # no-ops, but the old code logged "ensured" regardless). So we check
    # databaseExists after each create attempt instead of trusting the absence
    # of an exception. Try the catalog-qualified form first, then a plain-name
    # fallback; if the schema STILL doesn't exist, raise — a missing redirect
    # target would let every redirected write fall through to the real table
    # (data-safety hole) and fail %sql/spark writes with "no database ...".
    attempts = (
        (f'spark.sql("CREATE SCHEMA IF NOT EXISTS {quoted}")', "qualified"),
        (f'spark.sql("CREATE SCHEMA IF NOT EXISTS `{schema_name}`")', "plain"),
    )
    for create_stmt, label in attempts:
        probe = (
            create_stmt + "\n"
            f'print("REDIRECT_SCHEMA_EXISTS=" + str(spark.catalog.databaseExists("{schema_name}")))'
        )
        try:
            result = await session.execute(probe, timeout=60)
            out = format_outputs(result.get("outputs", [])) or ""
        except Exception as e:
            tprint(f"[redirect] WARN schema create ({label}) raised: {str(e)[:120]}")
            continue
        if "REDIRECT_SCHEMA_EXISTS=True" in out:
            _redirect_schema_ensured_clusters.add(cluster_id)
            tprint(f"[redirect] verified write-redirect schema exists: {_REDIRECT_TABLE_PREFIX} on cluster {cluster_id[:12]}... (via {label})")
            return
        tprint(f"[redirect] WARN schema {schema_name!r} not present after {label} create")
    raise RuntimeError(
        f"[redirect] write-redirect schema {_REDIRECT_TABLE_PREFIX!r} could not be created/verified "
        f"on cluster {cluster_id}. Aborting: without it, redirected writes would fall through to real tables."
    )


# Match oci://bucket@namespace/suffix. Bucket and namespace are captured
# separately; everything after the first '/' after namespace is the suffix.
_OCI_URI_RE = re.compile(
    r"oci://(?P<bucket>[^/@\s\"']+)@(?P<ns>[^/\s\"']+)/(?P<suffix>[^\s\"']*)"
)


def _canonicalize_path(raw: str) -> Optional[str]:
    """Return the normalized form of an OCI path used as the redirect map
    key. Currently this is the input verbatim with surrounding whitespace
    stripped — two writes to byte-identical strings collide on the same
    map entry, which is what we want. (We do NOT canonicalize trailing
    slashes or case because OCI is case-sensitive and a trailing slash can
    change semantics for some readers.)"""
    if not raw:
        return None
    return raw.strip()


def _redirect_target_for_path(orig: str) -> Optional[str]:
    """Map an oci://bucket@ns/suffix path to
    oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/<orig-bucket>/<orig-suffix>.

    The original bucket name is preserved as the first path segment in
    the redirected URI. This prevents collisions when two source paths
    in different buckets share an identical suffix:

      oci://<source_bucket>@ns1/sales/2024/  →  oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/<source_bucket>/sales/2024/
      oci://other-prod@ns2/sales/2024/ →  oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/other-prod/sales/2024/

    Returns None for inputs that are not valid OCI URIs.
    """
    m = _OCI_URI_RE.match(orig.strip())
    if not m:
        return None
    bucket = m.group("bucket")
    suffix = m.group("suffix") or ""
    return f"oci://{_REDIRECT_BUCKET}@{_REDIRECT_NAMESPACE}/{bucket}/{suffix}"


def _is_volume_or_workspace_path(p: str) -> bool:
    """Skip predicate for redirect: AIDP-local FUSE paths are never
    redirected (they're not in OCI object storage)."""
    s = (p or "").strip()
    return s.startswith("/Volumes/") or s.startswith("/Workspace/")


def _is_already_redirected_table(raw: str) -> bool:
    """Idempotency: a table identifier whose canonicalized 3-tuple has db
    == '<oci_backup_bucket>_overwrite' is already in the tool namespace.
    Don't redirect again."""
    canon = _canonicalize_table_id(raw)
    if not canon:
        return False
    _, db, _ = canon
    return db.lower() == "<oci_backup_bucket>_overwrite"


def _is_already_redirected_path(raw: str) -> bool:
    """Idempotency: paths in <oci_backup_bucket>@<WORKSPACE_NAMESPACE> are the
    tool's own namespace — don't redirect again."""
    return f"oci://{_REDIRECT_BUCKET}@" in (raw or "")


# Detection regexes for write/DML/DDL targets. Each pattern captures the
# quoted identifier or path so we can extract and redirect. The regexes
# are intentionally LITERAL — dynamic destinations (f-strings, variables)
# are not redirected by Phase 1 (logged in migration_report so the user
# can manually review).

# Python writer: matches df.write.<chain>.saveAsTable("X") /
# .insertInto("X") / .writeTo("X"). The chain between .write and the
# terminal call may contain .mode(...), .format(...), .partitionBy(...),
# .option(...), etc.
_WRITER_TABLE_RE = re.compile(
    r"""\.write
        (?:\s*\.\s*[A-Za-z_]\w*\s*\([^)]*\))*       # chained .option(), .mode(), etc.
        \s*\.\s*(?:saveAsTable|insertInto|writeTo)
        \s*\(\s*(["'`])((?:(?!\1).)+?)\1            # quoted identifier
        \s*\)
    """,
    re.VERBOSE,
)

# Python writer to a path: .parquet("oci://...") / .csv(...) / .json(...) /
# .orc(...) / .text(...) / .save("oci://...") / .save() with .option("path", ...)
_WRITER_PATH_RE = re.compile(
    r"""\.write
        (?:\s*\.\s*[A-Za-z_]\w*\s*\([^)]*\))*
        \s*\.\s*(?:parquet|csv|json|orc|text|save|saveAsTextFile)
        \s*\(\s*(["'])((?:(?!\1).)+?)\1
        \s*[),]
    """,
    re.VERBOSE,
)

# .option("path", "oci://...") in a writer chain — captures the path.
_WRITER_OPTION_PATH_RE = re.compile(
    r"""\.option\s*\(\s*(["'])path\1\s*,\s*(["'])((?:(?!\2).)+?)\2\s*\)""",
    re.VERBOSE,
)

# SQL reserved keywords that the SQL-rewriting regexes (`_SQL_TABLE_OPS_RE`,
# `_SQL_KEYWORDS_RE`) can mistakenly capture as the table identifier when
# the actual identifier is a placeholder like `{}` / f-string `{var}` that
# doesn't match `[A-Za-z_]\w*`. The regex backtracks to a shorter `op`
# alternative and then captures the NEXT keyword (IF/EXISTS/OR/etc.) as
# the ident. Substituting these as if they were table names produces
# invalid SQL (e.g. `DROP TABLE default.default.IF EXISTS {}`).
#
# Every `_sub` callback MUST short-circuit when ident.upper() is in this
# set. Covers:
#   - existence clauses: IF / NOT / EXISTS
#   - CREATE OR REPLACE / INSERT INTO|OVERWRITE / VIEW
#   - DESCRIBE modifiers: TABLE / EXTENDED / FORMATTED / DETAIL / HISTORY / PARTITION
#   - JOIN/ON/USING/AS
#   - query clauses: SELECT / WHERE / GROUP / BY / ORDER / HAVING / LIMIT
#   - misc: VALUES / SET / WITH
_SQL_KEYWORD_NEVER_AS_IDENT = {
    "IF", "NOT", "EXISTS",
    "OR", "REPLACE", "VIEW",
    "INTO", "OVERWRITE",
    "TABLE", "EXTENDED", "FORMATTED", "DETAIL", "HISTORY", "PARTITION",
    "AS", "ON", "USING",
    "SELECT", "WHERE", "GROUP", "BY", "ORDER", "HAVING", "LIMIT",
    "VALUES", "SET", "WITH",
    "FROM", "JOIN",
}


# SQL DDL/DML — matches the table identifier following the keyword.
# Captures both quoted (`"corp"."sales_fct"` or ``corp`.`sales_fct``) and
# unquoted (`corp.sales_fct`) forms. The identifier-capturing group is
# non-greedy and stops at whitespace, parentheses, commas, or end of line.
_SQL_TABLE_OPS_RE = re.compile(
    r"""\b(?P<op>CREATE\s+(?:OR\s+REPLACE\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?
            |INSERT\s+INTO
            |INSERT\s+OVERWRITE(?:\s+TABLE)?
            |UPDATE
            |DELETE\s+FROM
            |MERGE\s+INTO
            |TRUNCATE\s+TABLE
            |DROP\s+TABLE(?:\s+IF\s+EXISTS)?
            |ALTER\s+TABLE
        )\s+
        (?P<ident>`[^`]+`(?:\.`[^`]+`){0,2}      # all-backticked 1/2/3-part
                |[A-Za-z_]\w*(?:\.\w+){0,2})     # bareword 1/2/3-part
    """,
    re.IGNORECASE | re.VERBOSE,
)

# SQL FROM <table>, JOIN <table>, INTO <table> (for SELECT side of MERGE) —
# captures reads.
_SQL_READ_RE = re.compile(
    r"""\b(?P<op>FROM|JOIN|USING)\s+
        (?P<ident>`[^`]+`(?:\.`[^`]+`){0,2}
                |[A-Za-z_]\w*(?:\.\w+){0,2})
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Python reader: spark.read.<chain>.table("X") / .parquet("oci://...") / etc.
_READER_TABLE_RE = re.compile(
    r"""(?:spark|\w+)\s*\.\s*(?:read\s*\.\s*table|table)
        \s*\(\s*(["'`])((?:(?!\1).)+?)\1\s*\)
    """,
    re.VERBOSE,
)

_READER_PATH_RE = re.compile(
    r"""(?:spark|\w+)\s*\.\s*read
        (?:\s*\.\s*[A-Za-z_]\w*\s*\([^)]*\))*
        \s*\.\s*(?:parquet|csv|json|orc|text|load)
        \s*\(\s*(["'])((?:(?!\1).)+?)\1\s*[),]
    """,
    re.VERBOSE,
)


def _record_redirect(kind: str, original: str, redirected: str, source_op: str) -> None:
    """Audit log + collision detection for the migration_report. Called
    every time a NEW entry is added to the redirect map."""
    if kind == "table":
        # Check if any prior entry maps to the same redirected target —
        # that's the cross-catalog/cross-schema collision case.
        collisions = [k for k, v in _write_redirect_table_map.items() if v == redirected]
        if len(collisions) > 1:
            _write_redirect_collisions.append({
                "kind": "table",
                "redirected": redirected,
                "originals": [".".join(c) for c in collisions],
                "note": "Multiple source tables redirect to the same target — last writer wins.",
            })
    elif kind == "path":
        collisions = [k for k, v in _write_redirect_path_map.items() if v == redirected]
        if len(collisions) > 1:
            _write_redirect_collisions.append({
                "kind": "path",
                "redirected": redirected,
                "originals": collisions,
                "note": "Multiple source paths redirect to the same target — last writer wins.",
            })
    _write_redirect_log.append({
        "kind": kind, "original": original, "redirected": redirected, "op": source_op,
    })
    # Live signal: one line per NEW redirect entry so the migration log
    # shows redirect activity as it happens (not just at task end).
    tprint(f"  [write-redirect] {kind}: {original!r} -> {redirected!r} ({source_op})")


def _apply_wrapper_call_redirect(exec_code: str, source_op_hint: str = "") -> str:
    """AST-based cell-text rewrite of customer writer-wrapper function calls.

    Reads reports/writer_wrappers.json — for every Call node whose function
    name is in the actionable catalog, redirect literal-string args at the
    declared db/bucket/path/full_id positions or matching kwargs to the
    tool's temp schema/bucket. Variable / non-literal args are left untouched
    (we cannot safely rewrite them; the runtime value is unknown at
    code-rewrite time).

    Why this exists (and replaces the older runtime monkey-patch approach):
      - Pure cell-text transform: no kernel-namespace state dependency.
      - Survives session restart / kernel re-creation transparently.
      - Saved customer code is NOT modified — rewrite is applied only to
        the exec_code sent to the kernel, NOT the cell source written back.

    Behaviour:
      - Unparseable code → return unchanged (don't crash the cell).
      - No catalog → return unchanged (graceful degradation).
      - Method calls (`is_method: true` in catalog): positions in the call
        are offset by -1 relative to the def (self is implicit).
      - Idempotent: if a literal is already the redirect target, it's left
        alone — no double rewrite.
    """
    if not exec_code:
        return exec_code
    catalog = _load_writer_wrappers_catalog()
    if not catalog.get("actionable"):
        return exec_code

    # Build lookup: function_name -> {is_method, positional_args, args}
    actionable: Dict[str, dict] = {}
    for entry in catalog.get("actionable", []):
        if isinstance(entry, dict) and entry.get("name"):
            actionable[entry["name"]] = {
                "is_method": entry.get("is_method", False),
                "positional_args": entry.get("positional_args", []),
                "args": entry.get("args", {}),
            }
    if not actionable:
        return exec_code

    try:
        tree = ast.parse(exec_code)
    except SyntaxError:
        # Cell isn't valid Python at this stage (probably a magic command or
        # mid-transform state). Skip silently.
        return exec_code

    _AIDP_REDIRECT_DB = _REDIRECT_TABLE_PREFIX.split(".", 1)[1]
    _AIDP_REDIRECT_BUCKET_NAME = _REDIRECT_BUCKET

    def _resolve_role(spec: dict, kw_name: Optional[str], pos_index: Optional[int]) -> Optional[str]:
        """Given a Call arg (kwarg name OR positional index), return the
        declared role from the catalog, or None if no role applies."""
        roles = spec["args"]
        if kw_name is not None:
            return roles.get(kw_name)
        positional = spec["positional_args"]
        offset = 1 if spec["is_method"] else 0
        def_idx = pos_index + offset
        if def_idx < 0 or def_idx >= len(positional):
            return None
        return roles.get(positional[def_idx])

    def _redirect_for_role(role: str, current: str) -> Optional[str]:
        """Compute the redirected value for `current` given the role.
        Returns None if no redirect applies (current already redirected,
        local path, etc.)."""
        if role == "db":
            if current == _AIDP_REDIRECT_DB:
                return None  # idempotent
            return _AIDP_REDIRECT_DB
        if role == "bucket":
            if current == _AIDP_REDIRECT_BUCKET_NAME:
                return None
            return _AIDP_REDIRECT_BUCKET_NAME
        if role == "path":
            # Leave Volume/Workspace paths and non-OCI paths alone.
            if _is_volume_or_workspace_path(current):
                return None
            if not current.startswith("oci://"):
                return None
            redirected = _redirect_target_for_path(current)
            return redirected if redirected != current else None
        if role == "full_id":
            # 'tbl' / 'db.tbl' / 'cat.db.tbl' → rewrite db slot to temp.
            canon = _canonicalize_table_id(current)
            if not canon:
                return None
            cat, db, tbl = canon
            if db == _AIDP_REDIRECT_DB:
                return None  # idempotent
            # Use catalog convention: 2-part `<temp_db>.<orig_db>_<tbl>`
            # to match _redirect_target_for_table behavior, but for full_id
            # form we just rewrite the db component (no underscore-merging).
            return f"{_AIDP_REDIRECT_DB}.{tbl}"
        return None

    changed: List[str] = []  # for logging

    def _maybe_rewrite_constant(node: ast.AST, role: str, fn_name: str) -> bool:
        """If `node` is a string Constant whose value should be redirected
        per `role`, replace its value in-place. Returns True if changed."""
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            return False
        current = node.value
        new_value = _redirect_for_role(role, current)
        if new_value is None:
            return False
        node.value = new_value
        changed.append(f"{fn_name}:{role} {current!r}->{new_value!r}")
        return True

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        # Determine the function name being called.
        fn = node.func
        if isinstance(fn, ast.Name):
            fn_name = fn.id
        elif isinstance(fn, ast.Attribute):
            fn_name = fn.attr
        else:
            continue
        spec = actionable.get(fn_name)
        if spec is None:
            continue
        # Positional args
        for i, arg in enumerate(node.args):
            role = _resolve_role(spec, None, i)
            if role is None:
                continue
            _maybe_rewrite_constant(arg, role, fn_name)
        # Keyword args
        for kw in node.keywords:
            if kw.arg is None:  # **kwargs splat
                continue
            role = _resolve_role(spec, kw.arg, None)
            if role is None:
                continue
            _maybe_rewrite_constant(kw.value, role, fn_name)

    if not changed:
        return exec_code  # no-op
    try:
        new_code = ast.unparse(tree)
    except Exception:
        # ast.unparse failure on some pathological tree — skip rewrite
        # silently rather than emitting broken code.
        return exec_code
    # Live signal: one line per cell that had any wrapper-arg rewritten.
    op = source_op_hint or "wrapper-call"
    tprint(f"  [wrapper-redirect] ({op}): {', '.join(changed[:6])}"
           + ("..." if len(changed) > 6 else ""))
    # Best-effort: also record the redirected destinations in the
    # write-redirect map so subsequent _apply_read_redirects calls in the
    # same job see them. Walk the rewritten args.
    # (Detailed per-arg recording is complex; the cell-text SQL redirect
    # below will capture saveAsTable / INSERT / spark.sql DDL targets
    # — which is the read-side requirement.)
    return new_code


# ── Runtime write-destination guard (catches VARIABLE write targets) ──────
# _apply_write_redirects only rewrites quoted oci:// / table LITERALS. Writes
# whose destination is a VARIABLE (e.g. df.write.json(coverageDump)) slip
# through. To GUARANTEE no write ever touches a real bucket, inject a tiny
# runtime helper that maps any oci://bucket@ns/suffix →
# oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/bucket/suffix (same convention as
# _redirect_target_for_path) and WRAP every write-terminal argument with it at
# EXEC time. Works for literals AND variables, in Scala and Python. EXEC-only —
# never saved.
_SCALA_WRITE_GUARD_FN = (
    'def _aidp__write_guard__(p: String): String = '
    'if (p != null && p.startsWith("oci://") && !p.startsWith("oci://' + _REDIRECT_BUCKET + '@")) '
    '{ val r = p.substring(6); val a = r.indexOf("@"); val s = r.indexOf("/", a); '
    'val b = if (a < 0) r else r.substring(0, a); '
    'val suf = if (s < 0) "" else r.substring(s + 1); '
    '"oci://' + _REDIRECT_BUCKET + '@' + _REDIRECT_NAMESPACE + '/" + b + "/" + suf } else p'
)
_PY_WRITE_GUARD_FN = (
    "def _aidp__write_guard__(p):\n"
    "    if isinstance(p, str) and p.startswith('oci://') and not p.startswith('oci://" + _REDIRECT_BUCKET + "@'):\n"
    "        r = p[6:]; a = r.find('@'); s = r.find('/', a); b = r if a < 0 else r[:a]; suf = '' if s < 0 else r[s+1:]\n"
    "        return 'oci://" + _REDIRECT_BUCKET + "@" + _REDIRECT_NAMESPACE + "/' + b + '/' + suf\n"
    "    return p"
)
_WRITE_PATH_TERMINALS = ("json", "parquet", "csv", "orc", "text", "save", "saveAsTextFile")


def _balance_arg(code: str, open_idx: int):
    """code[open_idx] must be '('. Return (inner_arg_str, close_paren_idx),
    string- and nest-aware. (None, -1) if unbalanced."""
    depth = 0
    i = open_idx
    n = len(code)
    in_str = None
    while i < n:
        c = code[i]
        if in_str is not None:
            if c == "\\":
                i += 2
                continue
            if c == in_str:
                in_str = None
        elif c in ("'", '"'):
            in_str = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return code[open_idx + 1:i], i
        i += 1
    return None, -1


def _wrap_write_dests(code: str, is_scala: bool) -> str:
    """Wrap every WRITE-terminal destination arg (df.write[.chain].<term>(ARG))
    with the runtime guard so VARIABLE destinations are redirected to tmp too.
    Only wraps terminals that follow a `.write` (never `.read`). Returns the
    rewritten code (unchanged if nothing to wrap)."""
    pw = "_aidp__write_guard__"  # low-collision name so customer code can't shadow it
    term_alt = "|".join(_WRITE_PATH_TERMINALS)
    term_re = re.compile(r'\.(?:' + term_alt + r')\s*\(')
    # Collect the '(' index of each write-terminal call that follows a `.write`.
    opens = []
    for wm in re.finditer(r'\.write\b', code):
        tm = term_re.search(code, wm.end())
        if tm:
            opens.append(tm.end() - 1)  # index of '('
    # Also wrap `.option("path", ARG)` arguments inside a writer chain.
    for om in re.finditer(r'\.option\s*\(\s*["\']path["\']\s*,', code):
        # only if a real `.write` (word-boundary, so NOT `.writeStream` /
        # `.write_metadata`) appears before it; widen the lookback so long
        # Scala writer chains aren't truncated.
        if re.search(r'\.write\b', code[max(0, om.start() - 1500):om.start()]):
            # the 2nd-arg open is the same paren; we wrap the whole option value
            opens.append(om.start() + code[om.start():].index("(") )
    # Process right-to-left so indices stay valid.
    for open_idx in sorted(set(opens), reverse=True):
        inner, close_idx = _balance_arg(code, open_idx)
        if inner is None:
            continue
        s = inner.strip()
        if not s or pw + "(" in inner:
            continue
        # For .option("path", X): inner == '"path", X' — wrap only the value.
        if s.lower().startswith('"path"') or s.lower().startswith("'path'"):
            comma = inner.find(",")
            if comma < 0:
                continue
            val = inner[comma + 1:]
            new_inner = inner[:comma + 1] + " " + pw + "(" + val.strip() + ")"
        else:
            new_inner = pw + "(" + s + ")"
        code = code[:open_idx + 1] + new_inner + code[close_idx:]
    return code


def _inject_write_guard(exec_code: str) -> str:
    """EXEC-only: redirect VARIABLE write destinations to the tmp bucket by
    wrapping write-terminal args with a runtime guard (+ injecting the guard
    def). Complements _apply_write_redirects (which handles literals)."""
    if not exec_code or ".write" not in exec_code:
        return exec_code
    is_scala = exec_code.lstrip().startswith("%scala")
    wrapped = _wrap_write_dests(exec_code, is_scala)
    if wrapped == exec_code:
        return exec_code
    if is_scala:
        lines = wrapped.split("\n")
        ins = 1 if (lines and lines[0].strip().startswith("%scala")) else 0
        lines.insert(ins, _SCALA_WRITE_GUARD_FN)
        return "\n".join(lines)
    # python (handle a leading %python magic line)
    lines = wrapped.split("\n")
    ins = 1 if (lines and lines[0].strip().startswith("%python")) else 0
    lines.insert(ins, _PY_WRITE_GUARD_FN)
    return "\n".join(lines)


def _apply_write_redirects(exec_code: str, source_op_hint: str = "") -> str:
    """Detect every write/DDL/DML target in `exec_code`, register a
    redirect entry for it, and substitute the redirected destination in
    the EXEC code. Idempotent: targets already in the tool namespace are
    skipped. Returns the modified exec_code (the saved cell is unchanged
    — callers pass exec_code, not current_code).
    """
    if not exec_code:
        return exec_code

    # Wrapper-call redirect runs FIRST: rewrites customer-wrapper call
    # args (createTable, drop_table, etc.) at literal-string positions.
    # The subsequent SQL/path regex passes then catch any inline writes
    # in the wrapper-rewritten code (e.g., spark.sql calls that quote
    # the now-redirected db name from a downstream f-string).
    exec_code = _apply_wrapper_call_redirect(exec_code, source_op_hint)

    modified = exec_code

    # ── Python writer → table ──
    for m in list(_WRITER_TABLE_RE.finditer(exec_code)):
        ident = m.group(2)
        if _is_already_redirected_table(ident):
            continue
        canon = _canonicalize_table_id(ident)
        if not canon:
            continue
        redirected = _redirect_target_for_table(canon)
        if canon not in _write_redirect_table_map:
            _write_redirect_table_map[canon] = redirected
            _record_redirect("table", ident, redirected, source_op_hint or "py-writer")
        # Substitute the literal identifier in the code. Match the same
        # quote style. Use string replace (safe — ident is captured from
        # the source, exists verbatim).
        quote = m.group(1)
        old_lit = f"{quote}{ident}{quote}"
        new_lit = f"{quote}{redirected}{quote}"
        modified = modified.replace(old_lit, new_lit)

    # ── Python writer → path ──
    for m in list(_WRITER_PATH_RE.finditer(exec_code)):
        path = m.group(2)
        if _is_volume_or_workspace_path(path) or _is_already_redirected_path(path):
            continue
        if not path.startswith("oci://"):
            # Phase 1: only redirect OCI URIs. Other path styles (s3://,
            # dbfs:/) should already be translated to OCI by upstream
            # _apply_s3_translations before we see them here.
            continue
        redirected = _redirect_target_for_path(path)
        if not redirected:
            continue
        canon = _canonicalize_path(path)
        if canon and canon not in _write_redirect_path_map:
            _write_redirect_path_map[canon] = redirected
            _record_redirect("path", path, redirected, source_op_hint or "py-writer-path")
        quote = m.group(1)
        old_lit = f"{quote}{path}{quote}"
        new_lit = f"{quote}{redirected}{quote}"
        modified = modified.replace(old_lit, new_lit)

    # ── Python writer .option("path", "...") ──
    for m in list(_WRITER_OPTION_PATH_RE.finditer(exec_code)):
        path = m.group(3)
        if _is_volume_or_workspace_path(path) or _is_already_redirected_path(path):
            continue
        if not path.startswith("oci://"):
            continue
        redirected = _redirect_target_for_path(path)
        if not redirected:
            continue
        canon = _canonicalize_path(path)
        if canon and canon not in _write_redirect_path_map:
            _write_redirect_path_map[canon] = redirected
            _record_redirect("path", path, redirected, "py-writer-option-path")
        quote = m.group(2)
        old_lit = f"{quote}{path}{quote}"
        new_lit = f"{quote}{redirected}{quote}"
        modified = modified.replace(old_lit, new_lit)

    # ── SQL DDL/DML ──
    # All DDL/DML ops (CREATE/INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/TRUNCATE)
    # go through the same identifier-substitution path: redirect the table
    # identifier to <oci_backup_bucket>_overwrite.<db>_<tbl>. source data
    # is never touched because the SQL now references the tool's temp schema.
    #
    # Why DROP TABLE no longer gets replaced with a no-op comment:
    #   Comment-only suppression broke `spark.sql("DROP TABLE ...")` calls —
    #   spark.sql() can't parse a string that is *only* a comment, raising
    #   RuntimeException. Substitution is safer: the DROP still executes,
    #   but against our temp table (which the tool created earlier), so
    #   customer's real table is untouched and our temp table cleans up.
    #   The writer-wrapper interceptor (drop_database/drop_table/delete_table)
    #   already provides an additional layer for wrapper-form DROPs.
    def _sql_op_sub(match):
        nonlocal modified
        # Idempotency guard: skip matches inside a SQL line-comment.
        full_text = match.string
        line_start = full_text.rfind("\n", 0, match.start()) + 1
        line_prefix = full_text[line_start:match.start()]
        if "--" in line_prefix:
            return match.group(0)

        op = match.group("op").strip().upper()
        ident = match.group("ident")
        # Regex-backtrack guard: when the SQL has a `.format()` / f-string
        # placeholder like `DROP TABLE IF EXISTS {}`, the `{}` does not
        # match the ident pattern, so the regex backtracks and treats `IF`
        # (or `NOT`, `EXISTS`, etc.) as the table identifier. Reject any
        # ident that is a SQL keyword reserved by the surrounding op clauses.
        if ident.upper() in _SQL_KEYWORD_NEVER_AS_IDENT:
            return match.group(0)
        if _is_already_redirected_table(ident):
            return match.group(0)
        canon = _canonicalize_table_id(ident)
        if not canon:
            return match.group(0)

        redirected = _redirect_target_for_table(canon)
        if canon not in _write_redirect_table_map:
            _write_redirect_table_map[canon] = redirected
            _record_redirect("table", ident, redirected, f"sql-{op.lower().split()[0]}")
        # Reconstruct the matched substring with the redirected identifier
        return match.group(0).replace(ident, redirected, 1)

    modified = _SQL_TABLE_OPS_RE.sub(_sql_op_sub, modified)

    return modified


def _apply_read_redirects(exec_code: str) -> str:
    """For every read target in `exec_code`, if it's in the redirect map
    (because a prior write registered it), substitute the redirect.
    Targets NOT in the map are left as-is — they read from the customer's
    production (read-only, non-destructive)."""
    if not exec_code or (not _write_redirect_table_map and not _write_redirect_path_map):
        return exec_code

    modified = exec_code

    # Python reader → table
    for m in list(_READER_TABLE_RE.finditer(exec_code)):
        ident = m.group(2)
        if _is_already_redirected_table(ident):
            continue
        canon = _canonicalize_table_id(ident)
        if not canon or canon not in _write_redirect_table_map:
            continue
        redirected = _write_redirect_table_map[canon]
        quote = m.group(1)
        old_lit = f"{quote}{ident}{quote}"
        new_lit = f"{quote}{redirected}{quote}"
        modified = modified.replace(old_lit, new_lit)

    # Python reader → path
    for m in list(_READER_PATH_RE.finditer(exec_code)):
        path = m.group(2)
        if _is_volume_or_workspace_path(path) or _is_already_redirected_path(path):
            continue
        canon = _canonicalize_path(path)
        if not canon or canon not in _write_redirect_path_map:
            continue
        redirected = _write_redirect_path_map[canon]
        quote = m.group(1)
        old_lit = f"{quote}{path}{quote}"
        new_lit = f"{quote}{redirected}{quote}"
        modified = modified.replace(old_lit, new_lit)

    # SQL FROM/JOIN — substitute identifiers that have a redirect entry.
    def _sql_read_sub(match):
        ident = match.group("ident")
        if _is_already_redirected_table(ident):
            return match.group(0)
        canon = _canonicalize_table_id(ident)
        if not canon or canon not in _write_redirect_table_map:
            return match.group(0)
        redirected = _write_redirect_table_map[canon]
        return match.group(0).replace(ident, redirected, 1)

    modified = _SQL_READ_RE.sub(_sql_read_sub, modified)

    return modified


def get_write_redirect_summary() -> Dict:
    """Snapshot of the current redirect map for migration_report rendering."""
    return {
        "tables": dict(_write_redirect_table_map),
        "paths": dict(_write_redirect_path_map),
        "log": list(_write_redirect_log),
        "collisions": list(_write_redirect_collisions),
    }


# ============================================================
# Writer-Wrapper Interceptors (runtime, kernel-side)
# ============================================================
#
# Cell-text write redirects (above) catch inline writes like
#   df.write.saveAsTable("real_db.tbl") / spark.sql("INSERT INTO ...")
# but DO NOT catch customer utility wrappers like
#   createTable(df, table_name="x", database_name="real_db", ...)
# because the actual write is hidden inside the function body.
#
# This block installs **runtime monkey-patches** on the customer's
# wrapper functions in the kernel namespace. After a dep notebook
# defines `createTable`, we replace `globals()["createTable"]` with a
# thin wrapper that rewrites `database_name`/`bucket_name`/path args
# before invoking the original. Saved customer code is NOT modified —
# this is in-kernel only, for the duration of tool execution.
#
# The catalog lives in reports/writer_wrappers.json and lists:
#   - actionable: wrappers with explicit (arg_name → role) mapping
#                 where role ∈ {table, db, bucket, path, full_id, mode}
#   - refuse:     wrappers that write to hardcoded targets and CANNOT
#                 be safely redirected — the interceptor raises if any
#                 of these are called.
#
# Roles redirected by the interceptor:
#   - db    → "<oci_backup_bucket>_overwrite"
#   - bucket→ "<oci_backup_bucket>"
#   - path  → oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/<orig-bucket>/<suffix>
#   - full_id (e.g. "db.tbl") → "<oci_backup_bucket>_overwrite.tbl"
# The interceptor leaves `table`, `mode`, and unknown roles alone.
#
# Idempotency:
#   - Install can be called many times; wrappers already in place are
#     detected via __aidp_redirected__ / __aidp_refused__ flags and skipped.
#   - Catalog is loaded once per process (cached at module level).

_WRITER_WRAPPER_CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports", "writer_wrappers.json",
)
_writer_wrapper_catalog_cache: Optional[dict] = None


def _load_writer_wrappers_catalog() -> dict:
    """Load and cache reports/writer_wrappers.json. Returns the parsed JSON.
    On missing file or parse failure, returns an empty {actionable:[], refuse:[]}
    so callers degrade gracefully (cell-text redirects still apply)."""
    global _writer_wrapper_catalog_cache
    if _writer_wrapper_catalog_cache is not None:
        return _writer_wrapper_catalog_cache
    try:
        with open(_WRITER_WRAPPER_CATALOG_PATH) as f:
            doc = json.load(f)
        if not isinstance(doc, dict):
            raise ValueError("catalog root is not a dict")
        doc.setdefault("actionable", [])
        doc.setdefault("refuse", [])
        _writer_wrapper_catalog_cache = doc
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        tprint(f"  [writer-wrappers] catalog unavailable ({type(e).__name__}: {e}) — "
               f"cell-text redirects still apply; runtime wrapper interception disabled.")
        _writer_wrapper_catalog_cache = {"actionable": [], "refuse": []}
    return _writer_wrapper_catalog_cache


def _build_writer_interceptor_install_code(catalog: dict) -> str:
    """Render the kernel-side Python that installs the interceptors.
    The returned string is self-contained — it imports only stdlib `re`/`inspect`
    and references no symbols from this Python package, because it runs in the
    AIDP cluster kernel, not in this process."""
    actionable_dict = {
        a["name"]: a.get("args", {})
        for a in catalog.get("actionable", [])
        if isinstance(a, dict) and a.get("name")
    }
    refuse_list = []
    for r in catalog.get("refuse", []):
        if isinstance(r, str):
            refuse_list.append({"name": r, "reason": "no-redirectable-arg"})
        elif isinstance(r, dict) and r.get("name"):
            refuse_list.append({"name": r["name"], "reason": r.get("reason", "no-redirectable-arg")})

    return (
        "# === AIDP runtime writer-wrapper interceptor (auto-installed by tool) ===\n"
        "import re as _aidp_re\n"
        "import inspect as _aidp_inspect\n"
        "import json as _aidp_json\n"
        "\n"
        f"_AIDP_REDIRECT_DB = {_REDIRECT_TABLE_PREFIX.split('.', 1)[1]!r}\n"
        f"_AIDP_REDIRECT_BUCKET = {_REDIRECT_BUCKET!r}\n"
        f"_AIDP_REDIRECT_NAMESPACE = {_REDIRECT_NAMESPACE!r}\n"
        "_AIDP_OCI_URI_RE = _aidp_re.compile(\n"
        "    r\"oci://(?P<bucket>[^/@\\s\\\"']+)@(?P<ns>[^/\\s\\\"']+)/(?P<suffix>[^\\s\\\"']*)\"\n"
        ")\n"
        "\n"
        "def _aidp_redirect_db(name):\n"
        "    if not isinstance(name, str) or not name.strip():\n"
        "        return name\n"
        "    if name == _AIDP_REDIRECT_DB:\n"
        "        return name\n"
        "    return _AIDP_REDIRECT_DB\n"
        "\n"
        "def _aidp_redirect_bucket(name):\n"
        "    if not isinstance(name, str) or not name.strip():\n"
        "        return name\n"
        "    if name == _AIDP_REDIRECT_BUCKET:\n"
        "        return name\n"
        "    return _AIDP_REDIRECT_BUCKET\n"
        "\n"
        "def _aidp_redirect_path(p):\n"
        "    # Only redirect OCI URIs. Leave /Volumes/, /Workspace/, relative paths,\n"
        "    # bare filenames, and non-OCI URIs unchanged (the customer's wrapper may\n"
        "    # treat these specially and our rewrite would change semantics).\n"
        "    if not isinstance(p, str) or not p.strip():\n"
        "        return p\n"
        "    if p.startswith('/Volumes/') or p.startswith('/Workspace/'):\n"
        "        return p\n"
        "    m = _AIDP_OCI_URI_RE.match(p.strip())\n"
        "    if not m:\n"
        "        return p\n"
        "    bucket = m.group('bucket')\n"
        "    if bucket == _AIDP_REDIRECT_BUCKET:\n"
        "        return p\n"
        "    suffix = m.group('suffix') or ''\n"
        "    return f'oci://{_AIDP_REDIRECT_BUCKET}@{_AIDP_REDIRECT_NAMESPACE}/{bucket}/{suffix}'\n"
        "\n"
        "def _aidp_redirect_full_id(s):\n"
        "    # 'tbl' / 'db.tbl' / 'catalog.db.tbl' → db slot replaced.\n"
        "    if not isinstance(s, str) or not s.strip():\n"
        "        return s\n"
        "    parts = [p.strip().strip('`') for p in s.split('.')]\n"
        "    if any(not p for p in parts):\n"
        "        return s\n"
        "    if len(parts) == 1:\n"
        "        return f'{_AIDP_REDIRECT_DB}.{parts[0]}'\n"
        "    if len(parts) == 2:\n"
        "        if parts[0] == _AIDP_REDIRECT_DB:\n"
        "            return s\n"
        "        return f'{_AIDP_REDIRECT_DB}.{parts[1]}'\n"
        "    if len(parts) == 3:\n"
        "        if parts[1] == _AIDP_REDIRECT_DB:\n"
        "            return s\n"
        "        return f'{parts[0]}.{_AIDP_REDIRECT_DB}.{parts[2]}'\n"
        "    return s\n"
        "\n"
        "class _AidpWriterRefused(Exception):\n"
        "    pass\n"
        "\n"
        "def _aidp_make_wrapper(orig, arg_roles, fn_name):\n"
        "    try:\n"
        "        sig = _aidp_inspect.signature(orig)\n"
        "    except (ValueError, TypeError):\n"
        "        sig = None\n"
        "    def wrapper(*args, **kwargs):\n"
        "        if sig is None:\n"
        "            return orig(*args, **kwargs)\n"
        "        try:\n"
        "            bound = sig.bind(*args, **kwargs)\n"
        "        except TypeError:\n"
        "            return orig(*args, **kwargs)\n"
        "        bound.apply_defaults()\n"
        "        replaced = {}\n"
        "        for arg_name, role in arg_roles.items():\n"
        "            if arg_name not in bound.arguments:\n"
        "                continue\n"
        "            val = bound.arguments[arg_name]\n"
        "            if role == 'db':\n"
        "                new = _aidp_redirect_db(val)\n"
        "            elif role == 'bucket':\n"
        "                new = _aidp_redirect_bucket(val)\n"
        "            elif role == 'path':\n"
        "                new = _aidp_redirect_path(val)\n"
        "            elif role == 'full_id':\n"
        "                new = _aidp_redirect_full_id(val)\n"
        "            else:\n"
        "                continue\n"
        "            if new != val:\n"
        "                bound.arguments[arg_name] = new\n"
        "                replaced[arg_name] = [val, new]\n"
        "        if replaced:\n"
        "            print(f'[AIDP redirect] {fn_name}: ' + _aidp_json.dumps(replaced))\n"
        "        return orig(*bound.args, **bound.kwargs)\n"
        "    wrapper.__name__ = fn_name\n"
        "    wrapper.__wrapped__ = orig\n"
        "    wrapper.__aidp_redirected__ = True\n"
        "    wrapper.__aidp_arg_roles__ = dict(arg_roles)\n"
        "    return wrapper\n"
        "\n"
        "def _aidp_make_refusal(fn_name, reason):\n"
        "    def refused(*args, **kwargs):\n"
        "        raise _AidpWriterRefused(\n"
        "            f'AIDP tool refuses to invoke {fn_name!r} during migration: ' + reason + '. '\n"
        "            f'This function writes to a hardcoded target that cannot be auto-redirected. '\n"
        "            f'Add it to reports/writer_wrappers.json with explicit arg roles, '\n"
        "            f'or run the original job outside the tool.'\n"
        "        )\n"
        "    refused.__name__ = fn_name\n"
        "    refused.__aidp_refused__ = True\n"
        "    return refused\n"
        "\n"
        f"_AIDP_WRITER_CATALOG = {actionable_dict!r}\n"
        f"_AIDP_REFUSE_LIST = {refuse_list!r}\n"
        "\n"
        "def _aidp_install_writer_interceptors():\n"
        "    g = globals()\n"
        "    installed, refused, skipped = [], [], []\n"
        "    for fn_name, arg_roles in _AIDP_WRITER_CATALOG.items():\n"
        "        orig = g.get(fn_name)\n"
        "        if orig is None:\n"
        "            skipped.append(fn_name)\n"
        "            continue\n"
        "        if getattr(orig, '__aidp_redirected__', False):\n"
        "            continue\n"
        "        g[fn_name] = _aidp_make_wrapper(orig, arg_roles, fn_name)\n"
        "        installed.append(fn_name)\n"
        "    for item in _AIDP_REFUSE_LIST:\n"
        "        fn_name, reason = item['name'], item['reason']\n"
        "        orig = g.get(fn_name)\n"
        "        if orig is None:\n"
        "            continue\n"
        "        if getattr(orig, '__aidp_refused__', False):\n"
        "            continue\n"
        "        g[fn_name] = _aidp_make_refusal(fn_name, reason)\n"
        "        refused.append(fn_name)\n"
        "    if installed or refused:\n"
        "        print(f'[AIDP writer-wrappers] installed={installed} refused={refused}')\n"
        "    return {'installed': installed, 'refused': refused, 'skipped': skipped}\n"
        "\n"
        "_aidp_install_writer_interceptors()\n"
    )


async def _install_writer_interceptors(session, log_fn=None) -> dict:
    """Run the install code in the kernel. Idempotent and cheap (~50ms).
    Returns a dict with installed/refused/skipped lists, or empty on failure.
    Safe to call after every dep notebook inline."""
    catalog = _load_writer_wrappers_catalog()
    if not catalog.get("actionable") and not catalog.get("refuse"):
        return {}
    code = _build_writer_interceptor_install_code(catalog)
    try:
        result = await session.execute(code, timeout=30)
    except Exception as e:
        if log_fn:
            log_fn(f"  [writer-wrappers] install failed: {e}")
        return {}
    status = result.get("status", "error")
    if status != "ok":
        if log_fn:
            from context_tools import _unwrap_aidp_text
            log_fn(f"  [writer-wrappers] install non-ok status: "
                   f"{_unwrap_aidp_text(format_outputs(result.get('outputs', [])))[:300]}")
        return {}
    return {"status": "ok"}

# Load-time invariant: AIDP_SPARK_CONFIG_CELL MUST contain _CONFIG_MARKER.
# _inline_child_notebook() filters cells by marker substring to skip the config
# cell when re-loading previously migrated dep notebooks. If a future edit ever
# drops the marker from AIDP_SPARK_CONFIG_CELL, the filter silently no-ops, the
# config cell is treated as a real cell, and the save-back's off-by-one shift
# bug returns (one cell duplicated, one cell lost). Fail loud at import.
assert _CONFIG_MARKER in AIDP_SPARK_CONFIG_CELL, (
    f"AIDP_SPARK_CONFIG_CELL must contain the marker substring "
    f"{_CONFIG_MARKER!r} — _inline_child_notebook() relies on it to filter "
    f"the config cell when loading dep notebooks from disk."
)

# Object Storage hardening cell — applies CircuitBreaker + retry tuning to the
# active SparkSession so OCI Object Storage 429 bursts during bulk migration
# do not cascade to a 30-second total stall. The wave size is read from the
# AIDP_WAVE_SIZE env var and dictates which throttle profile is selected
# (conservative <=8, balanced 9-200, aggressive >200). Failures are swallowed
# silently so notebooks still run on clusters where aidp_compat is older.
AIDP_THROTTLE_HARDENING_CELL = """\
# AIDP Object Storage hardening — applied automatically by migration tool
try:
    import os as _os
    _wave = int(_os.environ.get("AIDP_WAVE_SIZE", "48"))
    from aidp_compat.oci_throttle import tune_for_parallel_migration as _tpm
    _tpm(spark, concurrent_jobs=_wave, verbose=False)
except Exception as _e:
    # aidp_compat<0.5.0 or other init failure -- fall back to no-op so cells
    # still run. The Spark cluster's default CB will still trip on 429 bursts;
    # log so operators see the gap.
    print(f"[AIDP] OCI throttle hardening unavailable ({type(_e).__name__}): {_e}")
"""

# Runtime-only toPandas() safety limit — prevents driver OOM during migration execution.
# NOT saved in the migrated notebook. Monkey-patches DataFrame.toPandas() to auto-limit
# rows when the DataFrame exceeds TOPANDAS_ROW_LIMIT.
AIDP_TOPANDAS_SAFETY = """\
try:
    import pyspark.sql as _ps
    if not getattr(_ps.DataFrame, '_aidp_topandas_patched', False):
        _TOPANDAS_ROW_LIMIT = 500000
        _TOPANDAS_MAX_CELLS = 100_000_000  # rows x cols — ~800MB at 8 bytes/cell, safe for 64GB driver
        _original_toPandas = _ps.DataFrame.toPandas
        def _safe_toPandas(self, *args, **kwargs):
            try:
                _ncols = len(self.columns)
                _cnt = self.count()
                _cells = _cnt * _ncols
                if _cells > _TOPANDAS_MAX_CELLS:
                    _safe_rows = max(1000, _TOPANDAS_MAX_CELLS // max(_ncols, 1))
                    print(f"[AIDP] toPandas() on {_cnt:,} rows x {_ncols:,} cols ({_cells:,} cells) exceeds limit. Limiting to {_safe_rows:,} rows.")
                    return _original_toPandas(self.limit(_safe_rows), *args, **kwargs)
                if _cnt > _TOPANDAS_ROW_LIMIT:
                    print(f"[AIDP] toPandas() on {_cnt:,} rows exceeds limit ({_TOPANDAS_ROW_LIMIT:,}). Auto-limiting to avoid OOM.")
                    return _original_toPandas(self.limit(_TOPANDAS_ROW_LIMIT), *args, **kwargs)
            except Exception:
                pass
            return _original_toPandas(self, *args, **kwargs)
        _ps.DataFrame.toPandas = _safe_toPandas
        _ps.DataFrame._aidp_topandas_patched = True
except Exception:
    pass
"""


# ─── Per-Notebook Logger ──────────────────────────────────────────────

class NotebookLog:
    """Per-notebook log that captures all output for the report."""
    def __init__(self, notebook_path: str, job_name: str, task_key: str):
        self.lines = []
        self.notebook_path = notebook_path
        self.job_name = job_name
        self.task_key = task_key

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self.lines.append(line)
        # Also print to stdout with job/task prefix for visibility
        tprint(f"  [{self.job_name}/{self.task_key}] {msg}")

    def text(self) -> str:
        return "\n".join(self.lines)


# ─── Deterministic Pre/Post Migration Transforms ─────────────────────
#
# These run outside Opus to make critical transformations reliable.
# All three are additive: they never delete or reorder existing code.

# ── S3 → OCI Path Translation ────────────────────────────────────────

# Default tenancy namespace — used ONLY as a last resort when a bucket is not
# in the bucket→tenancy mapping. The mapping (config/oci_bucket_tenancy_mapping.json)
# is the authoritative source; this default should rarely be hit. It is the
# WORKSPACE/default tenancy namespace (NOT the data-lake <DATALAKE_NAMESPACE>).
_DEFAULT_TENANCY_NS = "<WORKSPACE_NAMESPACE>"

def _validate_migrated_run_paths(source: str, migrated_base: str) -> list:
    """Detect malformed `%run` / notebook.run paths produced by buggy migration.

    Catches three specific patterns observed in real failures, none of which can
    arise from a legitimate path:
      1. MIGRATED_BASE prepended twice (e.g. ".../notebooks/.../notebooks/...").
         The migrated_base string is a long unique prefix; appearing twice in a
         single path always indicates a doubling bug.
      2. ".ipynb<digit>.ipynb" mangling (e.g. "<short_basename>.ipynb<digit>.ipynb") — when a `%run`
         basename was prefix-matched against dep_path_map and the remainder was
         appended verbatim.
      3. Doubled ".ipynb.ipynb" suffix (e.g. "foo.ipynb.ipynb") — basename was
         already suffixed, then re-suffixed.

    Returns a list of issue strings (empty when the source is clean).
    Pure inspection — does not modify source. Intended for logging/warning
    after Opus produces a migrated cell.
    """
    issues: list = []
    if not source or not migrated_base:
        return issues
    # Match %run, dbutils.notebook.run, oidlUtils.notebook.run target paths.
    # Capture: argument up to whitespace or quote close.
    patterns = [
        r'%run\s+(\S+)',
        r'dbutils\.notebook\.run\s*\(\s*["\']([^"\']+)',
        r'oidlUtils\.notebook\.run\s*\(\s*["\']([^"\']+)',
    ]
    for pat in patterns:
        for m in re.finditer(pat, source):
            path = m.group(1)
            if migrated_base and path.count(migrated_base) > 1:
                issues.append(f"MIGRATED_BASE doubled in path: {path}")
            if re.search(r'\.ipynb\d+\.ipynb$', path):
                issues.append(f"Mangled basename (digit between .ipynb suffixes): {path}")
            if path.endswith(".ipynb.ipynb"):
                issues.append(f"Doubled .ipynb suffix: {path}")
    return issues


def _ns_for_oci_bucket(oci_bucket: str):
    """Return the authoritative namespace for an OCI bucket from the
    bucket→tenancy mapping (config/oci_bucket_tenancy_mapping.json), or None if
    the bucket isn't in the mapping. The mapping is the single source of truth
    for namespaces (user rule)."""
    try:
        import context_tools as _ct
        if not _ct._BUCKET_NS_INDEX:
            _ct.load_bucket_mapping()  # populates _ct._BUCKET_NS_INDEX from JSON
        return _ct._BUCKET_NS_INDEX.get(oci_bucket)
    except Exception:
        return None


def _apply_s3_translations(source: str) -> str:
    """Deterministically replace s3[a]://bucket/path → oci://oci-bucket@namespace/path.

    Rule (per the source convention):
      - OCI bucket name = oci-<s3_bucket>
      - Namespace ALWAYS from the bucket→tenancy mapping
        (config/oci_bucket_tenancy_mapping.json) — the single source of truth.
      - If the bucket isn't in the mapping, fall back to the default TENANCY
        namespace (_DEFAULT_TENANCY_NS = <WORKSPACE_NAMESPACE>), NEVER the data-lake ns.
    (The old s3_to_oci_bucket_mapping.csv no longer exists and is not used.)

    The resulting oci:// paths are directly readable on AIDP:
      df = spark.read.parquet("oci://oci-bucket@namespace/path")
    """
    def _sub(m):
        scheme = m.group(1)   # 's3' or 's3a'
        bucket = m.group(2)
        path   = m.group(3)   # everything after bucket/
        # source naming convention: oci-<s3_bucket_name>
        oci_b = f"oci-{bucket}"
        # Namespace ALWAYS from the bucket→tenancy mapping; default tenancy ns
        # only as a last resort for buckets absent from the mapping.
        ns    = _ns_for_oci_bucket(oci_b) or _DEFAULT_TENANCY_NS
        # Return ONLY the translated URI — no inline "# ..." comment. s3 paths
        # almost always sit inside a string literal ("s3a://b/p"), and the regex
        # replaces the URI *inside* the quotes; an appended comment would land
        # INSIDE the string and corrupt the path (e.g. "oci://b/p  # ..."). The
        # translation is recorded in migration_notes instead.
        return f"oci://{oci_b}@{ns}/{path}"

    # Match s3:// and s3a:// URIs; capture bucket and sub-path separately
    return re.sub(r'\b(s3a?)://([^/\s"\']+)/([^\s"\']*)', _sub, source)


# ── OCI namespace correction (mapping is the ONLY source of truth) ────────
#
# RULE (user-mandated): the OCI namespace for a bucket comes ALWAYS from the
# bucket→tenancy mapping (config/oci_bucket_tenancy_mapping.json) — never from
# the source notebook, never from Opus. The exported/source notebooks often
# carry a WRONG namespace (e.g. oci-customer-feature-bucket tagged
# @<DATALAKE_NAMESPACE> when the bucket actually lives in @<WORKSPACE_NAMESPACE> per the mapping
# AND the manually-migrated gold). So for EVERY oci://<bucket>@<ns>, we
# overwrite <ns> with mapping[bucket]. Buckets absent from the mapping are left
# untouched (we never guess). Applied to SAVED + EXEC code so production and
# validation both hit the correct namespace.
_OCI_NS_RE = re.compile(r'oci://([^@/\s"\']+)@([^/\s"\']+)')


def _apply_namespace_from_mapping(code: str) -> str:
    """Set every oci://<bucket>@<ns> namespace to the mapping's value for that
    bucket. The mapping is authoritative; source/Opus namespaces are ignored.
    Unknown buckets are left unchanged (no guessing)."""
    if not code or "oci://" not in code:
        return code
    try:
        from context_tools import _BUCKET_NS_INDEX, load_bucket_mapping
        if not _BUCKET_NS_INDEX:
            load_bucket_mapping()  # populates _BUCKET_NS_INDEX from the JSON
            from context_tools import _BUCKET_NS_INDEX  # re-import populated dict
    except Exception as e:
        # The mapping is authoritative for oci:// namespaces. If it can't load,
        # do NOT silently pass code through — that lets wrong namespaces survive
        # while the run still reports PASS. Fail loud and stop.
        tprint(f"  [namespace] FATAL: bucket→namespace mapping failed to load ({e!r}); "
               f"cannot guarantee oci:// namespaces — aborting to avoid wrong-namespace writes.")
        raise RuntimeError(
            "bucket→namespace mapping unavailable; refusing to proceed (would risk "
            "wrong-namespace writes). Check config/oci_bucket_tenancy_mapping.json."
        ) from e
    if not _BUCKET_NS_INDEX:
        tprint("  [namespace] FATAL: bucket→namespace mapping loaded but is EMPTY; "
               "cannot guarantee oci:// namespaces — aborting to avoid wrong-namespace writes.")
        raise RuntimeError(
            "bucket→namespace mapping is empty; refusing to proceed (would risk "
            "wrong-namespace writes). Check config/oci_bucket_tenancy_mapping.json."
        )
    def _sub(m):
        bucket, ns = m.group(1), m.group(2)
        correct = _BUCKET_NS_INDEX.get(bucket)
        if correct and correct != ns:
            return f"oci://{bucket}@{correct}"
        return m.group(0)
    return _OCI_NS_RE.sub(_sub, code)


# ── notebook.run timeout fix ─────────────────────────────────────────

# AIDP's notebook service REJECTS a notebook.run timeoutSeconds of 0 (or null)
# with: 400 InvalidParameter "timeoutSeconds is null or empty". Databricks used
# `0` to mean "no timeout", so migrated calls like
#   oidlUtils.notebook.run("/path", 0, Map(...))   (Scala)
#   oidlUtils.notebook.run("/path", 0, {...})       (Python, positional)
#   oidlUtils.notebook.run("/path", timeout=0, ...)  (Python, keyword)
# fail at execution. Rewrite a literal 0 timeout to a large positive value.
# (AIDP rejects a 0/null notebook.run timeout, so a literal 0 must be rewritten.)
_NB_RUN_TIMEOUT_DEFAULT = 3600  # seconds; AIDP needs a positive value (3600 = 1h)
# 2nd positional arg: notebook.run(<first-arg>, 0 <,|)>  — first arg may be a
# quoted path or a simple token/expression (no comma/paren inside).
_NB_RUN_TIMEOUT_POS_RE = re.compile(
    r'(\.notebook\.run\s*\(\s*(?:"[^"]*"|\'[^\']*\'|[^,()]+?)\s*,\s*)0(\s*[,)])'
)
# keyword form: notebook.run(..., timeout=0 ...) / timeout = 0
_NB_RUN_TIMEOUT_KW_RE = re.compile(
    r'(\.notebook\.run\s*\([^)]*?\btimeout\s*=\s*)0\b'
)


def _fix_notebook_run_timeout(source: str) -> str:
    """Rewrite notebook.run timeout 0 → positive (AIDP rejects 0/null)."""
    src = _NB_RUN_TIMEOUT_POS_RE.sub(rf'\g<1>{_NB_RUN_TIMEOUT_DEFAULT}\g<2>', source)
    src = _NB_RUN_TIMEOUT_KW_RE.sub(rf'\g<1>{_NB_RUN_TIMEOUT_DEFAULT}', src)
    return src


# ── notebook.run INLINE support (treat like %run) ────────────────────
# A (dbutils|oidlUtils).notebook.run(path, timeout, Map(...)) call embedded in a
# Scala/Python cell. To validate it like %run, we inline the child (execute+fix
# its cells in the parent kernel and save the fixed child), capture the child's
# notebook.exit value into a var, and substitute the call → that var for
# execution (the SAVED cell keeps notebook.run). On AIDP, notebook.exit throws
# (the value is not carried in the exception), but a Scala `var` persists across
# cells, so capture-by-assign works.
_NB_RUN_CALL_RE = re.compile(r'(?:dbutils|oidlUtils)\.notebook\.run\s*\(')


def _find_notebook_run_calls(src: str) -> list:
    """Find (dbutils|oidlUtils).notebook.run(...) calls via balanced-paren scan.

    Returns a list of dicts: {"full": <call text>, "path": <first-arg path or None>}.
    String-aware so parens inside string literals don't break matching.
    """
    out = []
    for m in _NB_RUN_CALL_RE.finditer(src):
        i = m.end() - 1  # index of '('
        depth = 0
        j = i
        instr = None
        while j < len(src):
            c = src[j]
            if instr:
                if c == instr and src[j - 1] != "\\":
                    instr = None
            elif c in ('"', "'"):
                instr = c
            elif c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if depth != 0:
            continue  # unbalanced — skip (don't mangle)
        full = src[m.start():j + 1]
        inner = src[i + 1:j]
        pm = re.match(r'\s*(["\'])(.*?)\1', inner)
        out.append({"full": full, "path": pm.group(2) if pm else None, "inner": inner})
    return out


def _parse_run_map_args(full_call: str) -> list:
    """Extract (key, value_expr) pairs from a notebook.run Map("k"->expr, ...)
    third argument. Scala Map(...) or Python {...} forms. Value expr is the raw
    in-scope expression (e.g. prevDayDateStr) — used to set the child's params."""
    pairs = []
    # Scala: "k" -> expr   |   Python: "k": expr
    for k, v in re.findall(r'["\']([^"\']+)["\']\s*(?:->|:)\s*([^,)}]+)', full_call):
        v = v.strip()
        if v:
            pairs.append((k, v))
    return pairs


# notebook.exit(...) — balanced-paren, for the EXEC-only capture rewrite.
_NB_EXIT_CALL_RE = re.compile(r'(?:dbutils|oidlUtils)\.notebook\.exit\s*\(')


def _rewrite_exit_to_capture(cell_src: str, conf_key: str) -> str:
    """EXEC-ONLY: rewrite a child cell's notebook.exit(X) → publish X to
    spark.conf[conf_key] (cross-language safe) instead of throwing, so the
    inlined parent can read the child's return value. Language-aware. The SAVED
    child keeps notebook.exit — this rewrite is applied only to the exec copy."""
    m = _NB_EXIT_CALL_RE.search(cell_src)
    if not m:
        return cell_src
    i = m.end() - 1
    depth = 0
    j = i
    instr = None
    while j < len(cell_src):
        c = cell_src[j]
        if instr:
            if c == instr and cell_src[j - 1] != "\\":
                instr = None
        elif c in ('"', "'"):
            instr = c
        elif c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
            if depth == 0:
                break
        j += 1
    if depth != 0:
        return cell_src
    inner = cell_src[i + 1:j]
    head = cell_src.lstrip()[:6].lower()
    if head.startswith("%scala"):
        repl = f'spark.conf.set("{conf_key}", String.valueOf({inner}))'
    elif head.startswith("%sql"):
        return cell_src  # SQL notebooks don't use notebook.exit this way
    else:
        repl = f'spark.conf.set("{conf_key}", str({inner}))'
    return cell_src[:m.start()] + repl + cell_src[j + 1:]


# ── Validation-only param injection (EXEC-only, never saved) ─────────
#
# A %scala cell uses the NATIVE oidlUtils.parameters, which has NO workflow
# parameter context in an interactive migration run (verified on-cluster:
# Scala getParameter returns the default). Python and Scala do NOT share local
# variables, so the Python param shim can't reach Scala. The only working
# cross-language channel is spark.conf. For VALIDATION ONLY, we publish each
# --param into spark.conf (see write_mp_code) and rewrite Scala
# oidlUtils.parameters.getParameter(...) reads to spark.conf.get(...) at
# EXECUTION TIME. The persisted cell is UNCHANGED — it keeps
# oidlUtils.parameters.getParameter, which is correct in a real AIDP workflow
# (the workflow supplies the parameter to native oidlUtils). This rewrite is
# NEVER saved. It deliberately does NOT touch dbutils.widgets.get — that must
# still fail → be migrated to oidlUtils by the fix loop (and saved).
_SCALA_PARAM_GETPARAM_RE = re.compile(
    r'oidlUtils\.parameters\.getParameter\(\s*"([^"]+)"\s*(?:,\s*([^)]*?))?\s*\)'
)


def _scala_param_exec_rewrite(code: str) -> str:
    """EXEC-ONLY: Scala oidlUtils.parameters.getParameter("X"[, d]) →
    spark.conf.get("spark.aidp.param.X", d). Validation convenience; never saved."""
    if not code.lstrip().startswith("%scala"):
        return code
    def _sub(m):
        name = m.group(1)
        default = (m.group(2) or "").strip() or '""'
        return f'spark.conf.get("spark.aidp.param.{name}", {default})'
    return _SCALA_PARAM_GETPARAM_RE.sub(_sub, code)


# ── DBFS-prefix path-replace idempotency fix ─────────────────────────

def _fix_path_replace_idempotency(source: str) -> str:
    """Fix non-idempotent / API-misuse path-prefix substitutions.

    Opus occasionally emits two broken patterns when translating /dbfs/ or
    dbfs:/ paths to /Volumes/...:

      Pattern A (non-idempotent):
        path = path.replace('/dbfs/', '/Volumes/default/default/dbfs/')
        - .replace() runs on EVERY occurrence; if `path` is already translated
          (e.g. via translate_path() upstream, or fed through a loop), the
          prefix appears mid-string and gets replaced again, producing
          /Volumes/.../Volumes/.../dbfs/...

      Pattern B (API misuse):
        path = path.startswith('/dbfs/', '/Volumes/default/default/dbfs/')
        - .startswith()'s second arg is the integer `start` index. Passing a
          string raises TypeError at runtime. And .startswith() returns bool,
          not str, so even if it succeeded `path` would become True/False.

    Both forms are rewritten to the idempotent guarded form:
      if path.startswith('/dbfs/'):
          path = '/Volumes/default/default/dbfs/' + path[len('/dbfs/'):]
    """
    def _sub(m):
        indent = m.group(1)
        lhs    = m.group(2)
        old_q  = m.group(4)
        old    = m.group(5)
        new_q  = m.group(6)
        new    = m.group(7)
        return (
            f"{indent}# Oracle tool modification: idempotent path-prefix substitution\n"
            f"{indent}if {lhs}.startswith({old_q}{old}{old_q}):\n"
            f"{indent}    {lhs} = {new_q}{new}{new_q} + {lhs}[len({old_q}{old}{old_q}):]"
        )

    # `<lhs> = <lhs>.(replace|startswith)("<old_prefix>", "<new_prefix>")`
    # where <old_prefix> looks like /dbfs/, dbfs:/, /mnt/<x>, etc., and
    # <new_prefix> looks like /Volumes/... (the AIDP volume mount).
    # LHS is captured via \w[\w.]* and backref-matched to ensure the call is
    # on the same variable being assigned.
    pat = re.compile(
        r"^([ \t]*)(\w[\w.]*)\s*=\s*\2"
        r"\.(replace|startswith)\s*\("
        r"\s*(['\"])(/?(?:dbfs[:/]|mnt/)[^'\"]*)\4"
        r"\s*,\s*"
        r"(['\"])(/Volumes/[^'\"]+)\6"
        r"\s*\)",
        re.MULTILINE,
    )
    return pat.sub(_sub, source)


# ── Table-read → path-read regression detector ───────────────────────

# Pattern to detect data-reading APIs that MUST stay as table reads:
#   spark.read.table("X") / df.read.table("X")
#   spark.table("X") / df.table("X")
# (The lookbehind allows the call to be on any object — spark, df, sql_ctx, etc.)
_TABLE_READ_RE = re.compile(
    r"\.(?:read\s*\.\s*table|table)\s*\(\s*[\"']",
    re.IGNORECASE,
)

# Pattern to detect path-based reads that Opus uses to "rescue" table reads:
#   spark.read.parquet("...") / .format("...").load(...) / .read.load("...")
_PATH_READ_RE = re.compile(
    r"\.read\s*\.\s*(?:parquet|format|load|csv|json|orc|text)\s*\(",
    re.IGNORECASE,
)


def _active_lines(src: str) -> str:
    """Strip `#`-prefixed comment lines so the detector only sees executable code.
    Triple-quoted strings are rare in cell-migration context; tolerable false
    positives are preferable to complex tokenizer logic for a soft warning.
    """
    return "\n".join(
        ln for ln in (src or "").splitlines()
        if not ln.lstrip().startswith("#")
    )


# Substrings that indicate the cell needs an AI migration pass. If NONE of
# these appear in the active (uncommented) source, the cell can be tried on
# the cluster as-is (modulo deterministic transforms) and only migrated if
# execution fails. This eliminates over-migration on pure-PySpark/Python cells.
#
# Lowercased for case-insensitive matching. Conservative — false positives just
# fall back to the existing Opus migration path (no harm); false negatives are
# caught by the execute+verify+fix loop's call_fix on the actual failure.
_DATABRICKS_MARKERS = (
    # Databricks-specific notebook control (must rename to oidlUtils.*)
    "dbutils.notebook.",
    "dbutils.fs.",
    "dbutils.widgets.",
    "dbutils.secrets.",
    "dbutils.library.",
    "dbutils.credentials.",
    # Magic commands handled by separate pipelines / require rewrite
    "%run",
    "%pip",
    "%sh",
    # Databricks-only Spark configs
    "spark.databricks.",
    # AWS SDKs not available on AIDP
    "boto3.",
    " boto3 ",
    "import boto3",
    # AWS Glue references
    "glue.client", "glue_client", "get_glue_table",
    # Object-storage URI schemes
    "s3://", "s3a://", "s3n://",
    # DBFS / mount paths
    "/dbfs/", "dbfs:/", "/mnt/",
    # Databricks-specific imports
    "from pyspark.dbutils",
    "from aidp_dbutils",
    "from databricks.",
    "import databricks",
    # Display / HTML helpers (work via aidp_compat at runtime, but the saved
    # notebook needs the explicit import — easier to let Opus add it)
    "displayhtml(",
    # Notebook-level helpers that need rename
    "notebook.exit(",
    "notebook.run(",
    "notebook.getcontext(",
    # source location-extraction patterns (per AIDP body-swap rule)
    "describe formatted",
    "describe extended",
    # DESCRIBE DETAIL is Delta-only; AIDP fails for non-Delta tables.
    # Must NOT be passed through AS_IS — Opus needs to rewrite to DESCRIBE EXTENDED
    # plus the matching downstream `.collect()[0][...]` access pattern.
    "describe detail",
)


def _has_databricks_markers(source: str) -> bool:
    """Quick check: does the active source contain any pattern that indicates
    an explicit AIDP migration is required? Used to gate the first-try-as-is
    optimization — if no markers are found, the cell is tried on the cluster
    without an upfront Opus call. Comments are stripped first."""
    if not source:
        return False
    active = _active_lines(source).lower()
    return any(m in active for m in _DATABRICKS_MARKERS)


def _detect_table_to_path_regression(original: str, migrated: str) -> str:
    """Soft-warning detector: when active code drops a `spark.read.table(...)` /
    `spark.table(...)` call AND adds a `spark.read.parquet/format/load/...`
    that wasn't in the original, prepend a REGRESSION_DETECTED comment to the
    migrated source so the regression is visible in the saved notebook and the
    migration report.

    Never auto-reverts (avoids ripping out other legitimate migrations baked
    into the same cell). Skips %scala / %sql cells — Python heuristic only.

    Returns the (possibly annotated) migrated source.
    """
    if not original or not migrated:
        return migrated
    head = migrated.lstrip()
    if head.startswith("%scala") or head.startswith("%sql"):
        return migrated

    orig_active = _active_lines(original)
    mig_active  = _active_lines(migrated)

    n_orig_table = len(_TABLE_READ_RE.findall(orig_active))
    n_mig_table  = len(_TABLE_READ_RE.findall(mig_active))
    n_orig_path  = len(_PATH_READ_RE.findall(orig_active))
    n_mig_path   = len(_PATH_READ_RE.findall(mig_active))

    if (n_orig_table > 0
            and n_mig_table < n_orig_table
            and n_mig_path > n_orig_path):
        warning = (
            "# Oracle tool modification: REGRESSION_DETECTED — table→path "
            "conversion. spark.read.table() / spark.table() must NOT be "
            "rewritten as spark.read.parquet/format/load. Review this cell "
            "and restore the original table read.\n"
        )
        return warning + migrated
    return migrated


# ── Path-returning-call → catalog-identifier regression detector ─────

# Matches `<var_name>_path / _location / _uri / _s3_location = <rhs>` assignments
# at the top of a line (after stripping comments). We only care about variables
# whose names declare an intent to hold a path/location string — downstream
# code that uses these as filesystem paths is what breaks when Opus replaces
# the RHS with a hardcoded catalog identifier.
_LOC_VAR_ASSIGN_RE = re.compile(
    r"^\s*([A-Za-z_]\w*_(?:path|location|uri|s3_location))\s*=\s*(.+?)\s*$",
    re.MULTILINE,
)

# A 2- or 3-part dotted catalog identifier: `db.table` or `catalog.db.table`.
# No slashes, no colons — that's the tell that it's NOT a path string.
_CATALOG_IDENT_RE = re.compile(
    r"^[A-Za-z_]\w*\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?$"
)


def _detect_path_returning_to_identifier_regression(original: str, migrated: str) -> str:
    """Soft-warning detector for the over-migration pattern where Opus
    replaces a path-returning function call with a hardcoded catalog identifier:

        # original (path-returning call):
        events_table_path = get_glue_table_s3_location('analytics', 'events_data')

        # migrated (hardcoded catalog ident — wrong):
        events_table_path = "analytics.events_data"

    The variable name advertises a PATH (`_path` / `_location` / `_uri` /
    `_s3_location` suffix); downstream consumers do
    `spark.read.parquet(f'{events_table_path}/load_date=...')`. Hardcoding a
    catalog identifier breaks every consumer silently.

    Heuristic: for each `<var>_(path|location|uri|s3_location) = <rhs>` line in
    the original, if the rhs is a function call (contains parens) AND the
    migrated assignment for the same var is a 2- or 3-part dotted string
    literal with no slashes, flag as REGRESSION_DETECTED. Soft warning only —
    prepends a comment to the migrated source; never auto-reverts. Skips
    %scala / %sql cells.
    """
    if not original or not migrated:
        return migrated
    head = migrated.lstrip()
    if head.startswith("%scala") or head.startswith("%sql"):
        return migrated

    orig_active = _active_lines(original)
    mig_active  = _active_lines(migrated)

    orig_vars = {m.group(1): m.group(2).strip() for m in _LOC_VAR_ASSIGN_RE.finditer(orig_active)}
    mig_vars  = {m.group(1): m.group(2).strip() for m in _LOC_VAR_ASSIGN_RE.finditer(mig_active)}

    regressed = []
    for var, orig_val in orig_vars.items():
        mig_val = mig_vars.get(var)
        if mig_val is None:
            continue
        # Original looks like a function call (contains parens)
        orig_is_call = "(" in orig_val and ")" in orig_val
        # Migrated is a string literal
        if not orig_is_call:
            continue
        is_string = (
            (mig_val.startswith('"') and mig_val.endswith('"')) or
            (mig_val.startswith("'") and mig_val.endswith("'"))
        )
        if not is_string:
            continue
        # Extract the literal content and check for catalog-identifier shape
        content = mig_val[1:-1]
        if "/" in content or ":" in content:
            continue  # not a catalog ident — looks like a path/URI
        if _CATALOG_IDENT_RE.match(content):
            regressed.append((var, orig_val[:60], content))

    if regressed:
        tprint(f"[regression-check:path→ident] FIRED — {len(regressed)} regressed var(s): {[r[0] for r in regressed]}")
        details = "; ".join(
            f"{var} (was '{call}' → now \"{ident}\")"
            for var, call, ident in regressed[:3]
        )
        warning = (
            f"# Oracle tool modification: REGRESSION_DETECTED — path-returning "
            f"call replaced with hardcoded catalog identifier. Variables that "
            f"held path strings now hold catalog names; downstream consumers "
            f"using them with spark.read.parquet/format/load will break. "
            f"Affected: {details}. Fix: restore the call sites verbatim and "
            f"body-swap the FUNCTION DEFINITION instead (never the call site).\n"
        )
        return warning + migrated
    return migrated


# ── Deterministic rename-only Databricks → oidlUtils API rewrites ─────
#
# These are pure API renames (no semantic shift) that parent cells get via
# CELL_MIGRATE_PROMPT but child cells (execute-first) would otherwise miss.
# Applied to BOTH parent and child source so behaviour is consistent and does
# NOT depend on a runtime crash to trigger an Opus rewrite. Unknown/exotic call
# shapes are left UNCHANGED (they still resolve via the aidp_compat shim within
# a session) rather than risk emitting wrong code.
#
# Covered:
#   dbutils.notebook.exit(...)        -> oidlUtils.notebook.exit(...)
#   dbutils.jobs.taskValues.set(k,v)  -> oidlUtils.parameters.setTaskValue(k, v)
#   dbutils.jobs.taskValues.get(...)  -> oidlUtils.parameters.getParameter(key, default)
#
# taskValues matters for correctness, not just tidiness: at real workflow
# runtime parent and child run as SEPARATE tasks/sessions, so the aidp_compat
# /tmp shim does NOT propagate values across them — only oidlUtils.parameters
# (AIDP-native) does. A child left on dbutils.jobs.taskValues silently breaks
# the inter-task hand-off.

# Left-boundary `(?<![\w.])` so a longer identifier ending in 'dbutils'
# (e.g. `mydbutils`, `self.dbutils`) is NOT matched and mangled.
_TV_SET_RE = re.compile(r'(?<![\w.])dbutils\.jobs\.taskValues\.set\s*\(')
_TV_GET_RE = re.compile(r'(?<![\w.])dbutils\.jobs\.taskValues\.get\s*\(')


def _match_close_paren(src: str, open_idx: int) -> int:
    """Index of the ')' matching the '(' at open_idx (string-aware), or -1."""
    depth = 0
    instr = None
    j = open_idx
    while j < len(src):
        c = src[j]
        if instr:
            if c == instr and src[j - 1] != "\\":
                instr = None
        elif c in ('"', "'"):
            instr = c
        elif c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
            if depth == 0:
                return j
        j += 1
    return -1


def _pos_is_commented(src: str, idx: int) -> bool:
    """True if idx sits after an unquoted '#' on its own line (comment)."""
    ls = src.rfind("\n", 0, idx) + 1
    seg = src[ls:idx]
    instr = None
    for k, c in enumerate(seg):
        if instr:
            if c == instr and seg[k - 1] != "\\":
                instr = None
        elif c in ('"', "'"):
            instr = c
        elif c == '#':
            return True
    return False


def _split_top_level_args(arg_str: str) -> list:
    """Split a call's inner argument string on top-level commas, respecting
    nested ()/[]/{} and string literals. Returns stripped arg strings."""
    args = []
    depth = 0
    instr = None
    cur = []
    for k, c in enumerate(arg_str):
        if instr:
            cur.append(c)
            if c == instr and arg_str[k - 1] != "\\":
                instr = None
        elif c in ('"', "'"):
            instr = c
            cur.append(c)
        elif c in '([{':
            depth += 1
            cur.append(c)
        elif c in ')]}':
            depth -= 1
            cur.append(c)
        elif c == ',' and depth == 0:
            args.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
    if "".join(cur).strip():
        args.append("".join(cur).strip())
    return args


def _classify_args(args: list, kw_names: set):
    """Split args into (positionals, {kwarg: value}). Returns None if any
    unknown kwarg is present (caller should leave the call unchanged)."""
    positionals = []
    kwargs = {}
    for a in args:
        m = re.match(r'^([A-Za-z_]\w*)\s*=\s*(.*)$', a, re.DOTALL)
        # A leading "name=" is a kwarg only if name is a known parameter — this
        # avoids misreading a positional expression like "a == b" or "x or y".
        if m and m.group(1) in kw_names:
            kwargs[m.group(1)] = m.group(2).strip()
        elif m and m.group(1) in ("key", "value", "taskKey", "default", "debugValue"):
            # known taskValues kw that doesn't belong to THIS method's set
            return None
        else:
            positionals.append(a)
    return positionals, kwargs


def _reshape_taskvalues_set(args: list):
    """dbutils.jobs.taskValues.set(key, value) → 'key, value' arg string, or None."""
    classified = _classify_args(args, {"key", "value"})
    if classified is None:
        return None
    pos, kw = classified
    key = kw.get("key")
    value = kw.get("value")
    if key is None and pos:
        key = pos.pop(0)
    if value is None and pos:
        value = pos.pop(0)
    if pos or key is None or value is None:
        return None
    return f"{key}, {value}"


def _reshape_taskvalues_get(args: list):
    """dbutils.jobs.taskValues.get(taskKey, key, default, debugValue) →
    'key' or 'key, default' arg string for getParameter, or None."""
    classified = _classify_args(args, {"taskKey", "key", "default", "debugValue"})
    if classified is None:
        return None
    pos, kw = classified
    order = ["taskKey", "key", "default", "debugValue"]
    vals = {n: kw.get(n) for n in order}
    pi = 0
    for n in order:
        if vals[n] is None and pi < len(pos):
            vals[n] = pos[pi]
            pi += 1
    if pi < len(pos) or vals["key"] is None:
        return None
    # taskKey is dropped (the AIDP bridge / native getParameter is flat, keyed
    # by name only — consistent with CELL_MIGRATE_PROMPT's .get→getParameter rule).
    default = vals["default"] if vals["default"] is not None else vals["debugValue"]
    return vals["key"] if default is None else f'{vals["key"]}, {default}'


def _rewrite_dbutils_api_renames(source: str) -> str:
    """Deterministically rename the rename-only Databricks APIs to oidlUtils.
    Idempotent (already-oidlUtils calls don't match) and comment-safe."""
    for regex, target, reshaper in (
        (_TV_SET_RE, "oidlUtils.parameters.setTaskValue", _reshape_taskvalues_set),
        (_TV_GET_RE, "oidlUtils.parameters.getParameter", _reshape_taskvalues_get),
    ):
        search_from = 0
        while True:
            m = regex.search(source, search_from)
            if not m:
                break
            if _pos_is_commented(source, m.start()):
                search_from = m.end()
                continue
            open_idx = m.end() - 1
            close_idx = _match_close_paren(source, open_idx)
            if close_idx == -1:
                search_from = m.end()
                continue
            new_args = reshaper(_split_top_level_args(source[open_idx + 1:close_idx]))
            if new_args is None:
                search_from = m.end()  # exotic shape — leave unchanged
                continue
            replacement = f"{target}({new_args})"
            source = source[:m.start()] + replacement + source[close_idx + 1:]
            search_from = m.start() + len(replacement)

    # notebook.exit — pure rename (skip commented-out originals). Use a
    # left-boundary regex (not str.replace) so `self.dbutils.notebook.exit(` /
    # `mydbutils.notebook.exit(` are not mangled into `...oidlUtils...`.
    _exit_re = re.compile(r'(?<![\w.])dbutils\.notebook\.exit\(')
    out_lines = []
    for line in source.split("\n"):
        if line.lstrip().startswith("#"):
            out_lines.append(line)
        else:
            out_lines.append(_exit_re.sub("oidlUtils.notebook.exit(", line))
    return "\n".join(out_lines)


# ── Internal Notebook Path Rewriting ─────────────────────────────────

# Old staging base paths that must be replaced with the per-job MIGRATED_BASE.
_OLD_NB_BASES = [
    "/Workspace/migration_staging/Notebooks",
    "/Workspace/migration_staging/notebooks",
]


def _rewrite_internal_paths(source: str, migrated_base: str, notebook_path: str = "",
                            dep_path_map: dict = None) -> str:
    """Replace old staging base paths and fix /Workspace/ path formatting.

    Applied per-cell AFTER Opus migration, BEFORE execution.
    1. Replaces known old staging base paths with current migrated_base.
    2. Resolves relative %run paths (./foo, ../bar, bare/name) to their full
       migrated absolute path based on the current notebook's location.
    3. Checks dep_path_map for exact migrated path (most reliable).
    4. Normalises remaining %run paths: spaces → underscores, adds .ipynb.
    """
    _dep_map = dep_path_map or {}

    def _lookup_dep_map(raw_path: str) -> str:
        """Check dep_path_map for the migrated path of a notebook reference.

        Tries (in order):
          1. Full normalized match (strip /Workspace/, spaces→_, lowercase)
          2. Suffix match — raw path ends with a known dep key (catches relative
             paths like './Config/0_Config' against absolute dep_map keys)
          3. Basename match — last path segment alone (catches bare-name %run
             where the notebook name is unique within the job)
        """
        # Normalize for comparison: strip /Workspace/, ./, ../, spaces→underscores
        def _norm(p):
            p = p.strip().replace(" ", "_")
            # Strip leading ./ and ../ segments — keep the meaningful suffix
            while p.startswith("./") or p.startswith("../"):
                p = p[2:] if p.startswith("./") else p[3:]
            p = p.lstrip("/")
            if p.startswith("Workspace/"):
                p = p[len("Workspace/"):]
            if not p.endswith(".ipynb") and not p.endswith(".py"):
                p += ".ipynb"
            return p.lower()

        raw_norm = _norm(raw_path)

        # 1. Exact match
        for orig, mig in _dep_map.items():
            if mig and _norm(orig) == raw_norm:
                return mig

        # 2. Suffix match — orig ends with raw_norm or vice-versa
        for orig, mig in _dep_map.items():
            if not mig:
                continue
            on = _norm(orig)
            if on.endswith("/" + raw_norm) or raw_norm.endswith("/" + on):
                return mig

        # 3. Basename-only match (last segment)
        raw_base = raw_norm.rsplit("/", 1)[-1]
        if raw_base and "." in raw_base:
            matches = [mig for orig, mig in _dep_map.items()
                       if mig and _norm(orig).rsplit("/", 1)[-1] == raw_base]
            # Only accept basename match if unambiguous
            if len(set(matches)) == 1:
                return matches[0]

        return ""

    # Pre-compute the migrated directory of the current notebook so we can
    # resolve relative %run paths.  Same approach as _inline_child_notebook().
    nb_migrated_dir = ""
    if notebook_path and migrated_base:
        nb_norm = normalize_nb_path(notebook_path)
        if not nb_norm.endswith(".ipynb"):
            nb_norm += ".ipynb"
        nb_migrated_dir = os.path.dirname(f"{migrated_base}/{nb_norm}")

    # 1. Replace old staging bases in the whole source text
    for old in _OLD_NB_BASES:
        if old in source:
            source = source.replace(old, migrated_base)
            tprint(f"  [path_rewrite] replaced {old!r} → {migrated_base!r}")

    # 2. Fix every %run line.
    #
    # AIDP cannot resolve relative %run paths the way Databricks can — every
    # %run must end up as an *absolute* path under MIGRATED_BASE. The branches
    # below are ordered to maximise the chance of producing a real migrated
    # path; the final fallback always emits an absolute path even if the
    # target wasn't in dep_path_map (Priority 5).
    def _fix_run(m):
        prefix = m.group(1)   # leading whitespace + '%run '
        raw_full = m.group(2).strip().strip('"\'')
        # Split path from trailing $param args (e.g. "%run /a/b $p1 $p2")
        # The path token is everything up to the first whitespace; preserve the
        # rest verbatim so $params survive the rewrite.
        _parts = raw_full.split(None, 1)
        raw = _parts[0]
        trailing = (" " + _parts[1]) if len(_parts) > 1 else ""
        is_relative = not raw.startswith("/") and not raw.startswith("Workspace/")

        # Compute leading-indent (whitespace at start of `prefix`) so we can
        # emit the "# Oracle tool modification" annotation on the line ABOVE
        # the %run line. IPython %run treats everything after the path as
        # arguments — an inline `# comment` is parsed as a path arg and
        # breaks resolution. Move the comment off-line.
        _indent = prefix[: len(prefix) - len(prefix.lstrip())]

        def _emit(path: str, note: str) -> str:
            return f"{_indent}# Oracle tool modification: {note}\n{prefix}{path}{trailing}"

        # — Priority 1: dep_path_map lookup on raw (handles abs + relative via
        #   suffix/basename match in _lookup_dep_map) —
        dep_hit = _lookup_dep_map(raw)
        if dep_hit:
            return _emit(dep_hit, "path → migrated dep path")

        # — Priority 2: Relative path resolution via nb_migrated_dir —
        if is_relative and nb_migrated_dir:
            resolved = os.path.normpath(os.path.join(nb_migrated_dir, raw))
            resolved = resolved.replace("\\", "/")
            fixed = resolved
            if not fixed.endswith(".ipynb") and not fixed.endswith(".py"):
                fixed += ".ipynb"
            # Try dep_path_map again with resolved absolute path
            dep_hit2 = _lookup_dep_map(fixed)
            if dep_hit2:
                return _emit(dep_hit2, "relative path → migrated dep path")
            return _emit(fixed, "relative path → full migrated path")

        # — Priority 3: Relative path with NO nb_migrated_dir — anchor at
        #   migrated_base directly. AIDP can't resolve relatives, so a wrong
        #   absolute is at least diagnosable, while a relative silently fails.
        if is_relative and migrated_base:
            stripped = raw
            while stripped.startswith("./") or stripped.startswith("../"):
                stripped = stripped[2:] if stripped.startswith("./") else stripped[3:]
            stripped = stripped.replace(" ", "_")
            fixed = f"{migrated_base}/{stripped}"
            if not fixed.endswith(".ipynb") and not fixed.endswith(".py"):
                fixed += ".ipynb"
            return _emit(fixed, "relative path → anchored at migrated_base")

        # — Priority 4: Absolute path — replace old staging bases + normalise —
        fixed = raw
        for old in _OLD_NB_BASES:
            if old in fixed:
                fixed = fixed.replace(old, migrated_base)
        fixed = fixed.replace(" ", "_")
        if not fixed.endswith(".ipynb") and not fixed.endswith(".py"):
            fixed += ".ipynb"

        # — Priority 5: If still pointing at /Workspace/<user>/... (cross-user
        #   absolute that wasn't rewritten by Priority 4), redirect under
        #   MIGRATED_BASE. The backfill pass will migrate the underlying
        #   notebook if it's not already migrated.
        if (fixed.startswith("/Workspace/") or fixed.startswith("Workspace/")) \
                and migrated_base and not fixed.startswith(migrated_base):
            normalized = normalize_nb_path(fixed)
            if not normalized.endswith(".ipynb") and not normalized.endswith(".py"):
                normalized += ".ipynb"
            fixed = f"{migrated_base}/{normalized}"
            return _emit(fixed, "cross-user path → migrated_base")

        if fixed != raw:
            return _emit(fixed, "path updated")
        return f"{prefix}{fixed}{trailing}"

    source = re.sub(
        r'(^\s*%run\s+)["\']?([^\n"\'#]+)["\']?',
        _fix_run,
        source,
        flags=re.MULTILINE,
    )

    # 3. dbutils.notebook.run(path, ...) / oidlUtils.notebook.run(path, ...)
    #    → oidlUtils.notebook.run(<absolute migrated path>, ...)
    #
    # Apply the same path resolution priorities used by %run (_fix_run): the
    # path argument must always be absolute under MIGRATED_BASE so AIDP can
    # locate it. Relative paths and cross-user absolute paths get redirected.
    def _resolve_run_path(raw_path: str) -> str:
        """Resolve a notebook.run() path to its absolute migrated equivalent.
        Mirrors _fix_run's priority chain so %run and notebook.run() agree."""
        is_relative = not raw_path.startswith("/") and not raw_path.startswith("Workspace/")

        # Priority 1: dep_path_map lookup
        dep_hit = _lookup_dep_map(raw_path)
        if dep_hit:
            fixed = dep_hit
            if "/Workspace/" in fixed:
                fixed = fixed.replace(" ", "_")
            if not fixed.endswith(".ipynb") and not fixed.endswith(".py"):
                fixed += ".ipynb"
            return fixed

        # Priority 2: relative path resolved against this notebook's migrated dir
        if is_relative and nb_migrated_dir:
            resolved = os.path.normpath(os.path.join(nb_migrated_dir, raw_path)).replace("\\", "/")
            if not resolved.endswith(".ipynb") and not resolved.endswith(".py"):
                resolved += ".ipynb"
            dep_hit2 = _lookup_dep_map(resolved)
            return dep_hit2 if dep_hit2 else resolved

        # Priority 3: relative with no nb_migrated_dir — anchor at migrated_base
        if is_relative and migrated_base:
            stripped = raw_path
            while stripped.startswith("./") or stripped.startswith("../"):
                stripped = stripped[2:] if stripped.startswith("./") else stripped[3:]
            stripped = stripped.replace(" ", "_")
            fixed = f"{migrated_base}/{stripped}"
            if not fixed.endswith(".ipynb") and not fixed.endswith(".py"):
                fixed += ".ipynb"
            return fixed

        # Priority 4: absolute path — replace old staging bases + normalise
        fixed = raw_path
        for old in _OLD_NB_BASES:
            if old in fixed:
                fixed = fixed.replace(old, migrated_base)
        fixed = fixed.replace(" ", "_")
        if not fixed.endswith(".ipynb") and not fixed.endswith(".py"):
            fixed += ".ipynb"

        # Priority 5: cross-user absolute → redirect under MIGRATED_BASE
        if (fixed.startswith("/Workspace/") or fixed.startswith("Workspace/")) \
                and migrated_base and not fixed.startswith(migrated_base):
            normalized = normalize_nb_path(fixed)
            if not normalized.endswith(".ipynb") and not normalized.endswith(".py"):
                normalized += ".ipynb"
            fixed = f"{migrated_base}/{normalized}"

        return fixed

    def _fix_notebook_run(m):
        full_match = m.group(0)
        quote      = m.group(1)
        raw_path   = m.group(2)

        # Defensive: strip any stray "  # Oracle tool modification..." that a prior
        # transform may have leaked into the path string.
        raw_path = re.split(r'\s*#\s*Oracle tool modification', raw_path, maxsplit=1)[0].rstrip()

        fixed_path = _resolve_run_path(raw_path)
        if fixed_path != raw_path:
            new_path = f"{quote}{fixed_path}{quote}"
            result = full_match.replace(f"{quote}{raw_path}{quote}", new_path)
        else:
            result = full_match

        # API rename always safe (just a function name change, no semantic shift)
        result = result.replace("dbutils.notebook.run(", "oidlUtils.notebook.run(", 1)
        # NOTE: do NOT append a trailing "# Oracle tool modification" comment.
        # The regex only matches up to the path quote — appending here lands
        # mid-statement and breaks the function call's closing args.
        return result

    # Match BOTH dbutils.notebook.run AND oidlUtils.notebook.run so already-
    # rewritten calls with relative paths still get path-fixed on subsequent runs.
    source = re.sub(
        r'(?:dbutils|oidlUtils)\.notebook\.run\s*\(\s*(["\'])([^"\']+)\1',
        _fix_notebook_run,
        source,
    )

    # AIDP rejects notebook.run timeoutSeconds=0/null — Databricks used 0 for
    # "no timeout". Rewrite a literal 0 timeout to a positive value so the
    # migrated call (Scala native or Python) doesn't fail with
    # "timeoutSeconds is null or empty". Idempotent + safe to apply repeatedly.
    source = _fix_notebook_run_timeout(source)

    # Rename-only API rewrites (taskValues, notebook.exit). Idempotent — safe to
    # apply here even though parent cells also get these via CELL_MIGRATE_PROMPT.
    source = _rewrite_dbutils_api_renames(source)

    # Deterministic source-catalog → default remap in string literals (e.g.
    # SCHEMA = "main.sample_schema"). _to_three_part only catches call idents;
    # this fixes literals in the SAVED notebook so they don't fail at runtime.
    # No-op unless register_catalog_remap() was called (needs catalog manifest).
    source = _apply_catalog_remap(source)

    return source


# ── Auto dbutils Import Injection ─────────────────────────────────────

_AIDP_IMPORT_LINE = (
    "from aidp_compat import dbutils, displayHTML, sql, translate_path"
    "  # Oracle tool modification: added by migration tool\n"
)


# Known Databricks implicit imports that AIDP requires explicitly.
# Format: (usage_pattern_regex, import_line_to_add, language)
# language: 'python' | 'scala' — scala entries only applied to %scala cells.
_IMPLICIT_IMPORT_RULES: list = [
    # ── Scala: org.json (Databricks pre-imports, AIDP does not) ──────────────
    (r'\bJSONObject\b',   "import org.json.JSONObject  // Oracle tool modification: added missing import",   "scala"),
    (r'\bJSONArray\b',    "import org.json.JSONArray  // Oracle tool modification: added missing import",    "scala"),
    (r'\bJSONException\b',"import org.json.JSONException  // Oracle tool modification: added missing import","scala"),
    # ── Scala: common Databricks-pre-available Spark/Java ────────────────────
    (r'\bSimpleDateFormat\b',
     "import java.text.SimpleDateFormat  // Oracle tool modification: added missing import", "scala"),
    (r'\bArrayBuffer\b',
     "import scala.collection.mutable.ArrayBuffer  // Oracle tool modification: added missing import", "scala"),
    (r'\bBase64\b',
     "import java.util.Base64  // Oracle tool modification: added missing import", "scala"),
    # ── Python: common names used without import in Databricks notebooks ──────
    (r'\bOrderedDict\b(?!\s*=)',
     "from collections import OrderedDict  # Oracle tool modification: added missing import", "python"),
    (r'\bdefaultdict\b(?!\s*=)',
     "from collections import defaultdict  # Oracle tool modification: added missing import", "python"),
    (r'\bCounter\b(?!\s*=)',
     "from collections import Counter  # Oracle tool modification: added missing import", "python"),
    (r'\bnamedtuple\b',
     "from collections import namedtuple  # Oracle tool modification: added missing import", "python"),
    (r'\bdatetime\b(?!\s*=)(?!\.)',
     "from datetime import datetime  # Oracle tool modification: added missing import", "python"),
    (r'\btimedelta\b(?!\s*=)',
     "from datetime import timedelta  # Oracle tool modification: added missing import", "python"),
    (r'\bdate\b(?!\s*=)(?!time)(?!\.)',
     "from datetime import date  # Oracle tool modification: added missing import", "python"),
]


def _add_missing_imports(source: str) -> str:
    """Deterministically inject known Databricks implicit imports that AIDP requires.

    Checks each known usage pattern against the active (uncommented) source.
    Only adds an import if:
    - The usage pattern is present in active code
    - The import line is NOT already present anywhere in the cell

    Applied per-cell AFTER Opus migration, BEFORE execution.
    """
    is_scala = source.lstrip().startswith("%scala")
    lang = "scala" if is_scala else "python"

    # Strip comments for usage detection (same approach as cell_analyzer.py)
    active = re.sub(r'#[^\n]*', '', source)          # Python line comments
    active = re.sub(r'//[^\n]*', '', active)          # Scala line comments
    active = re.sub(r'/\*[\s\S]*?\*/', '', active)    # Scala block comments
    active = re.sub(r'"""[\s\S]*?"""', '', active)    # Python triple-quoted strings
    active = re.sub(r"'''[\s\S]*?'''", '', active)    # Python triple-quoted strings

    imports_to_add = []
    for pattern, import_line, rule_lang in _IMPLICIT_IMPORT_RULES:
        if rule_lang != lang:
            continue
        if not re.search(pattern, active):
            continue
        # Check if import already present anywhere in the full source (including comments)
        import_token = import_line.split("  #")[0].split("  //")[0].strip()
        if import_token in source:
            continue
        imports_to_add.append(import_line)

    if not imports_to_add:
        return source

    # For Scala cells, insert after the %scala magic line
    # For Python cells, prepend before the first non-comment, non-blank line
    added_block = "\n".join(imports_to_add) + "\n"
    if is_scala:
        lines = source.split("\n")
        insert_at = 1 if lines[0].strip().startswith("%") else 0
        lines.insert(insert_at, added_block.rstrip())
        return "\n".join(lines)
    else:
        return added_block + source


# ── Databricks job-trigger rewriting ───────────────────────────────────
#
# Rewrites Databricks job invocations to call the AIDP-equivalent job via
# /Workspace/<deploy_dir>/<job_runner>.run_job_and_wait. Confirmed signature
# (verified by reading the source script):
#
#   run_job_and_wait(job_id: str, params: list = [], poll_interval=30, timeout=3600)
#
# AIDP `parameters` shape is list[{"key": str, "value": str}] — confirmed via
# build_dag_from_workflow.py:386. We convert at runtime so the original
# notebook_params dict (which may reference variables) is preserved.
#
# Detection patterns (V1 — most common in source notebooks):
#   1. job_call(<int_or_var>, <params_expr>)
#   2. job_calling(<int_or_var>, ...)
#   3. call_job_internal(<int_or_var>, <params_expr>, ...)
#   4. <var> = <int_literal_with_13+_digits>      (precedes #1-3 in same cell)
#
# Mapping source: module-level _db_to_aidp_job_map populated from the manifest's
# "db_to_aidp_job_map" field by _process_job_inner. Unmapped IDs accumulate in
# _unmapped_db_job_ids and are surfaced in JOB_REPORT.

_DB_JOB_ID_ASSIGN_RE = re.compile(
    r'^([ \t]*)(\w*job_id\w*)\s*=\s*(\d{13,})\s*$',
    re.MULTILINE,
)

# Match wrapper-function calls. Also match `result = job_call(...)` and `a, b = job_call(...)`.
_DB_JOB_CALL_RE = re.compile(
    r'^([ \t]*)([\w,\s\(\)]*?=\s*)?(?P<fn>job_call|job_calling|call_job_internal)\s*\(\s*'
    r'(?P<job_arg>\d{13,}|[a-zA-Z_]\w*)\s*'
    r'(?:,\s*(?P<params_arg>[^,)]+(?:\([^)]*\)[^,)]*)*))?'
    r'(?P<rest>[^)]*)\)',
    re.MULTILINE,
)

_AIDP_JOB_CALL_TEMPLATE = """\
{indent}# Oracle tool modification: Databricks job {db_id} -> AIDP job {aidp_uuid}
{indent}_aidp_params_in = {params_expr}
{indent}_aidp_params = (
{indent}    [{{"key": _k, "value": str(_v)}} for _k, _v in _aidp_params_in.items()]
{indent}    if isinstance(_aidp_params_in, dict)
{indent}    else (_aidp_params_in if isinstance(_aidp_params_in, list) else [])
{indent})
{indent}{lhs}_aidp_run_job_and_wait("{aidp_uuid}", _aidp_params)
{indent}if {status_var} != 'SUCCESS':
{indent}    raise Exception(f"AIDP job {aidp_uuid} failed: status={{{status_var}}}")"""

# When the Databricks job_id has no corresponding AIDP UUID in the manifest's
# db_to_aidp_job_map, we comment the original line and emit a clearly-marked
# print so it shows up in cell output. We do NOT raise — the cell continues —
# and the unmapped id is surfaced in JOB_REPORT for follow-up.
_AIDP_UNMAPPED_TEMPLATE = """\
{indent}# Oracle tool modification: UNMAPPED Databricks job {db_id} — add to manifest db_to_aidp_job_map and re-run
{indent}print("[Oracle migration] SKIPPED Databricks job {db_id}: no AIDP mapping in manifest db_to_aidp_job_map — see JOB_REPORT.md")"""

# ── AIDP run_job_and_wait helper (inlined into every notebook that uses it) ──
# Mirrors /Workspace/<deploy_dir>/<job_runner>.run_job_and_wait so migrated notebooks
# don't depend on that file existing. URL constants are substituted from the
# toolkit's runtime config (AIDP_BASE / DATALAKE_OCID / WORKSPACE_ID) so the
# helper is fully wired up at injection time.
_AIDP_INVOKE_HELPER_TEMPLATE = '''\
# Oracle tool modification: inlined AIDP run_job_and_wait helper
# (mirrors /Workspace/<deploy_dir>/<job_runner>.py — kept self-contained so the migrated
# notebook works without depending on that file existing on the cluster)
def _aidp_run_job_and_wait(job_id, params=None, poll_interval=30, timeout=3600):
    """Submit an AIDP job and block until it finishes. Returns the final
    `state.status` string (e.g. 'SUCCESS', 'FAILED', 'TERMINATED').
    """
    import oci, time
    import requests as _req
    if params is None:
        params = []
    _config_path = "{oci_config_path}"
    _config = oci.config.from_file(_config_path, "{oci_config_profile}")
    _signer = oci.signer.Signer(
        tenancy=_config["tenancy"],
        user=_config["user"],
        fingerprint=_config["fingerprint"],
        private_key_file_location=_config["key_file"],
        pass_phrase=oci.config.get_config_value_or_default(_config, "pass_phrase"),
    )
    _base = "{aidp_ws_url}"
    _post = _req.post(f"{{_base}}/jobRuns",
                      json={{"jobKey": job_id, "parameters": params}},
                      auth=_signer)
    if _post.status_code != 201:
        raise Exception(f"AIDP /jobRuns POST failed: {{_post.status_code}} {{_post.text}}")
    _run_key = _post.json()["key"]
    _elapsed = 0
    while True:
        _get = _req.get(f"{{_base}}/jobRuns/{{_run_key}}", auth=_signer)
        time.sleep(10)
        if _get.status_code != 200:
            raise Exception(f"AIDP /jobRuns/<key> GET failed: {{_get.status_code}} {{_get.text}}")
        _status = _get.json()["state"]["status"]
        if _status not in ("RUNNING", "PENDING", None):
            print(f"[Oracle migration] AIDP job {{_run_key}} finished: {{_status}}")
            return _status
        if _elapsed >= timeout:
            raise TimeoutError(f"AIDP job {{_run_key}} timed out after {{timeout}}s. Last status: {{_status}}")
        time.sleep(poll_interval)
        _elapsed += poll_interval
'''


def _build_aidp_invoke_helper() -> str:
    """Build the inlined helper source with workspace/region URL substituted in."""
    aidp_ws_url = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
    return _AIDP_INVOKE_HELPER_TEMPLATE.format(
        oci_config_path="/Workspace/<deploy_dir>/config",
        oci_config_profile="DEFAULT",
        aidp_ws_url=aidp_ws_url,
    )


def _normalize_job_triggers(source: str) -> tuple:
    """Rewrite Databricks job_call(...) etc. to AIDP run_job_and_wait(...).

    Returns (modified_source, changed_bool).
    Side-effects: appends to module-level _unmapped_db_job_ids set for any
    Databricks job_id that wasn't found in _db_to_aidp_job_map.
    """
    if not source.strip():
        return source, False
    # Quick-bail: skip cells that don't reference any of the trigger functions
    if not any(fn in source for fn in ("job_call", "job_calling", "call_job_internal")):
        return source, False

    changed = False

    # 1) Build var → int_job_id map from this cell's `job_id = <int>` assignments
    var_to_id: Dict[str, int] = {}
    for m in _DB_JOB_ID_ASSIGN_RE.finditer(source):
        var_to_id[m.group(2)] = int(m.group(3))

    lines = source.split("\n")
    out_lines: list = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m = _DB_JOB_CALL_RE.match(line)
        if not m:
            out_lines.append(line)
            i += 1
            continue

        indent = m.group(1) or ""
        lhs_raw = (m.group(2) or "").rstrip()
        job_arg = m.group("job_arg")
        params_arg = (m.group("params_arg") or "").strip() or "[]"

        # Resolve job_arg → int
        if job_arg.isdigit():
            db_id = int(job_arg)
        elif job_arg in var_to_id:
            db_id = var_to_id[job_arg]
        else:
            # Unresolvable: leave the line alone but flag for review
            out_lines.append(line)
            out_lines.append(f"{indent}# Oracle tool modification: job_call() with non-literal job_id {job_arg!r} — manual review required")
            i += 1
            changed = True
            continue

        aidp_uuid = _db_to_aidp_job_map.get(str(db_id))
        if not aidp_uuid:
            _unmapped_db_job_ids.add(db_id)
            block = _AIDP_UNMAPPED_TEMPLATE.format(
                indent=indent, db_id=db_id, original=line.strip().replace('"', '\\"'),
            )
            out_lines.append(f"{indent}# {line.lstrip()}")
            out_lines.append(block)
            i += 1
            changed = True
            continue

        # Determine status_var: if original had `status, foo = job_call(...)` or `status = job_call(...)`,
        # use that. Otherwise default to a fresh `_status` variable.
        status_var = "_status"
        lhs_for_template = "_status = "
        if lhs_raw.endswith("="):
            # e.g. "status = " or "result, st = "
            lhs_clean = lhs_raw.rstrip("= \t")
            # If single-target assignment, keep the user's name as the status var
            if "," not in lhs_clean and lhs_clean.isidentifier():
                status_var = lhs_clean
                lhs_for_template = f"{lhs_clean} = "
            else:
                # Tuple unpack like `a, b = job_call(...)`. Original returned a tuple,
                # AIDP returns a single status string — use _status and surface a note.
                lhs_for_template = "_status = "
                out_lines.append(
                    f"{indent}# Oracle tool modification: original was tuple unpack {lhs_raw}— AIDP returns single status string"
                )

        block = _AIDP_JOB_CALL_TEMPLATE.format(
            indent=indent,
            db_id=db_id,
            aidp_uuid=aidp_uuid,
            params_expr=params_arg,
            lhs=lhs_for_template,
            status_var=status_var,
        )
        # Comment out the original (preserves intent for review)
        out_lines.append(f"{indent}# {line.lstrip()}")
        out_lines.append(block)
        changed = True
        i += 1

    return "\n".join(out_lines), changed


# ── Table reference normalization (Hive 1/2-part → AIDP 3-part) ──────
#
# AIDP/Unity-style catalogs require fully-qualified `catalog.schema.table`
# names. Databricks Hive-metastore notebooks routinely use 1-part (`tbl`)
# or 2-part (`db.tbl`) — these silently fail on AIDP. We tag everything to
# the `default` catalog (and `default` schema for 1-part refs).
#
# Covers (per cell):
#   - spark.read.table("X") / spark.table("X") / df.read.table("X")
#   - df.write.saveAsTable("X") / df.writeTo("X") / df.write.insertInto("X")
#   - SQL inside spark.sql("...") / spark.sql("""...""")  — single & multiline
#   - SQL bodies of %sql cells
#
# Skips:
#   - already 3-part references (cat.schema.tbl)
#   - CTE references (WITH cte AS ... → cte is not a table)
#   - function calls (FROM range(10), FROM unnest(...))
#   - well-known SQL pseudo-tables (DUAL, VALUES, LATERAL)


def _to_three_part(ident: str) -> tuple:
    """Return (normalized_ident, changed). Preserves backticks if present.
    Backtick-quoted idents may contain hyphens / dollar signs (Spark allows
    arbitrary chars inside backticks); plain idents must be \\w only."""
    has_backticks = "`" in ident
    clean = ident.replace("`", "").strip()
    if has_backticks:
        # Backticked: allow any non-dot, non-whitespace char per segment
        valid = re.match(r'^[^\.\s]+(?:\.[^\.\s]+){0,2}$', clean)
    else:
        valid = re.match(r'^[\w]+(?:\.[\w]+){0,2}$', clean)
    if not valid:
        return ident, False
    parts = clean.split(".")
    if len(parts) == 3:
        # 3-part name. ONLY remap to 'default' if the source catalog is
        # explicitly listed in the catalog-remap manifest. We used to remap
        # every non-'default' catalog unconditionally, which silently destroyed
        # references to legitimate non-default catalogs (e.g. 'samples.tpch.x'
        # or a customer's 'analytics.gold.orders'). Now the operator opts in
        # per-catalog via --catalog-manifest.
        if parts[0].lower() == "default":
            return ident, False
        if parts[-1].lower() in {"dual", "values", "lateral"}:
            return ident, False
        # Gate behind the manifest: leave 3-part names alone unless the
        # source catalog is in _CATALOG_REMAP.
        src2 = ".".join(parts[:2])  # 'cat.schema' key form used by register_catalog_remap
        src3 = ".".join(parts)      # exact 3-part form
        if src3 not in _CATALOG_REMAP and src2 not in _CATALOG_REMAP and parts[0] not in {p.split(".")[0] for p in _CATALOG_REMAP}:
            return ident, False
        new = f"default.{parts[1]}.{parts[2]}"
        if has_backticks:
            new = ".".join(f"`{p}`" for p in new.split("."))
        return new, True
    if parts[-1].lower() in {"dual", "values", "lateral"}:
        return ident, False
    if len(parts) == 2:
        new = f"default.{clean}"
    else:
        new = f"default.default.{clean}"
    if has_backticks:
        new = ".".join(f"`{p}`" for p in new.split("."))
    return new, True


# ── Catalog-name remap in STRING LITERALS (source catalog → default) ──
# _to_three_part only rewrites table-API/SQL *call* identifiers it can see in
# the source. It cannot touch a string-literal assignment like
#   SCHEMA = "main.sample_schema"
# that is later used to build a table name via an f-string. At exec time the
# write-redirect masks this (so migration passes), but the SAVED notebook keeps
# `main.` and FAILS at real runtime (no `main` catalog on AIDP). We fix the
# SAVED code with an EXACT replacement driven by the catalog-migration manifest
# (known source names only → zero over-match; generic, not a hardcoded catalog).
_CATALOG_REMAP: Dict[str, str] = {}   # "main.sample_schema" -> "default.sample_schema"


def register_catalog_remap(manifest_path: str, default_catalog: str = "default") -> None:
    """Populate _CATALOG_REMAP from a catalog-migration manifest's schema_map /
    table_map keys (source `cat.schema[.table]` names). Each source whose first
    segment isn't already the default catalog maps to `<default>.<rest>`. No-op
    if the manifest is missing/unreadable."""
    global _CATALOG_REMAP
    try:
        with open(manifest_path) as f:
            man = json.load(f)
    except (OSError, ValueError) as e:
        tprint(f"[catalog-remap] WARN could not load manifest {manifest_path}: {e}")
        return
    names = set()
    names.update((man.get("schema_map") or {}).keys())
    names.update((man.get("table_map") or {}).keys())
    remap = {}
    for src in names:
        parts = src.split(".")
        if len(parts) >= 2 and parts[0].lower() != default_catalog.lower():
            remap[src] = default_catalog + "." + ".".join(parts[1:])
    _CATALOG_REMAP = remap
    if remap:
        tprint(f"[catalog-remap] loaded {len(remap)} source→{default_catalog} name remap(s)")


_CATALOG_REMAP_RES: dict = {}  # cache of compiled per-src patterns

def _apply_catalog_remap(source: str) -> str:
    """Exact-replace known source catalog.schema[.table] names with their
    default-catalog form. Longest names first so 3-part names are handled before
    the 2-part schema they contain. No-op until register_catalog_remap() runs.

    Word-boundary anchored on BOTH sides so a manifest entry like 'main.users'
    does NOT clobber 'main.users_active' or URLs like
    'https://main.users.example.com/foo'. The trailing assertion forbids any
    word char or dot, so 'main.users' does not match the prefix of
    'main.users.bar' — that latter name needs its own manifest entry.
    """
    if not _CATALOG_REMAP or not source:
        return source
    for src in sorted(_CATALOG_REMAP, key=len, reverse=True):
        pat = _CATALOG_REMAP_RES.get(src)
        if pat is None:
            pat = re.compile(rf'(?<![\w.]){re.escape(src)}(?![\w.])')
            _CATALOG_REMAP_RES[src] = pat
        source = pat.sub(_CATALOG_REMAP[src], source)
    return source


_SQL_KEYWORDS_RE = re.compile(
    r'(?<![A-Za-z_])'
    r'((?:'
    r'FROM|JOIN|UPDATE|TRUNCATE\s+TABLE|DROP\s+TABLE|ALTER\s+TABLE|'
    r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?|'
    r'INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|MERGE\s+INTO|'
    # DESCRIBE variants — list ALL the modifier keywords that can appear
    # between DESCRIBE and the table identifier. Without these listed
    # explicitly, e.g. `DESCRIBE DETAIL <tbl>` matches keyword=DESCRIBE +
    # ident=DETAIL, and DETAIL gets a `default.default.` prefix —
    # producing invalid SQL `DESCRIBE default.default.DETAIL <tbl>`.
    # Delta Lake: DETAIL, HISTORY. Spark/Hive: TABLE, EXTENDED, FORMATTED,
    # PARTITION. SHOW analogs are matched below in their own clause.
    r'DESCRIBE(?:\s+(?:TABLE|EXTENDED|FORMATTED|DETAIL|HISTORY|PARTITION))?|DESC|'
    r'REFRESH\s+TABLE|'
    r'CACHE\s+TABLE|UNCACHE\s+TABLE|ANALYZE\s+TABLE|MSCK\s+REPAIR\s+TABLE'
    r'))'
    r'\s+(`[^`]+`|[\w]+(?:\.[\w]+){0,2})'
    r'(?![A-Za-z_.\(])',
    re.IGNORECASE,
)

_PY_TBL_API_RE = re.compile(
    r'(\.(?:read\s*\.\s*table|table|saveAsTable|writeTo|insertInto)\s*\(\s*)'
    r'(["\'])([^"\']+)\2'
)

_SPARK_SQL_RES = [
    # Triple-quote forms — handle Python `r/f/u` and Scala `s/f` prefixes
    re.compile(r'(spark\.sql\s*\(\s*[a-zA-Z]?)(""")([\s\S]*?)("""\s*[,\)])'),
    re.compile(r"(spark\.sql\s*\(\s*[a-zA-Z]?)(''')([\s\S]*?)('''\s*[,\)])"),
    # Single-quote forms (single-line)
    re.compile(r'(spark\.sql\s*\(\s*[a-zA-Z]?)(")([^"\n]*)("\s*[,\)])'),
    re.compile(r"(spark\.sql\s*\(\s*[a-zA-Z]?)(')([^'\n]*)('\s*[,\)])"),
]


def _normalize_sql_text(sql: str, changed_box: list) -> str:
    """Apply SQL keyword-driven table-ref normalization, skipping CTE names."""
    cte_names = set()
    # WITH <name> AS (...)   — first CTE
    for m in re.finditer(r'\bWITH\s+(\w+)\s+AS\s*\(', sql, re.IGNORECASE):
        cte_names.add(m.group(1).lower())
    # , <name> AS (...)      — additional CTEs in a chain
    for m in re.finditer(r',\s*(\w+)\s+AS\s*\(', sql, re.IGNORECASE):
        cte_names.add(m.group(1).lower())

    def _sub(m):
        keyword, ident = m.group(1), m.group(2)
        # Skip if first segment is a CTE name
        first_seg = ident.replace("`", "").split(".")[0].lower()
        if first_seg in cte_names:
            return m.group(0)
        # Regex-backtrack guard: when the SQL contains a `.format()` /
        # f-string placeholder like `DROP TABLE IF EXISTS {}`, the `{}`
        # does NOT match the ident pattern, so the regex backtracks and
        # captures `IF` / `NOT` / `EXISTS` / DESCRIBE modifiers as the
        # table identifier. Substituting these as if they were table
        # names produces invalid SQL like
        #   DESCRIBE default.default.DETAIL <tbl>
        #   DROP TABLE default.default.IF EXISTS {}
        # Reject any ident that is a SQL reserved keyword. See the
        # _SQL_KEYWORD_NEVER_AS_IDENT module-level definition.
        if ident.replace("`", "").upper() in _SQL_KEYWORD_NEVER_AS_IDENT:
            return m.group(0)
        new_ident, did_change = _to_three_part(ident)
        if did_change:
            changed_box[0] = True
        return f"{keyword} {new_ident}"

    return _SQL_KEYWORDS_RE.sub(_sub, sql)


def _normalize_table_refs(source: str) -> tuple:
    """Normalize all table references to 3-part `catalog.schema.table` form.

    Returns (modified_source, changed_bool). Caller adds the comment annotation.
    """
    changed_box = [False]

    # 1. Python API: .table(...), .saveAsTable(...), .writeTo(...), .insertInto(...)
    def _py_sub(m):
        prefix, quote, ident = m.group(1), m.group(2), m.group(3)
        new_ident, did_change = _to_three_part(ident)
        if did_change:
            changed_box[0] = True
        return f'{prefix}{quote}{new_ident}{quote}'
    modified = _PY_TBL_API_RE.sub(_py_sub, source)

    # 2. spark.sql("..."), spark.sql("""..."""), single + triple quote variants
    def _spark_sql_sub(m):
        prefix, openq, body, closer = m.group(1), m.group(2), m.group(3), m.group(4)
        return f"{prefix}{openq}{_normalize_sql_text(body, changed_box)}{closer}"
    for pat in _SPARK_SQL_RES:
        modified = pat.sub(_spark_sql_sub, modified)

    # 3. %sql cells — entire body after the magic line
    if modified.lstrip().startswith("%sql"):
        first_nl = modified.find("\n")
        if first_nl >= 0:
            head = modified[:first_nl + 1]
            body = modified[first_nl + 1:]
            modified = head + _normalize_sql_text(body, changed_box)

    return modified, changed_box[0]


def _preprocess_scala_cell_source(source: str, dep_path_map: dict = None) -> str:
    """Scala-safe path normalization for %scala cells.

    Scala syntax (e.g. `Map("k" -> v)`) cannot be touched by the Python regex
    transforms in `_preprocess_cell_source` — they would corrupt the call.
    But path strings INSIDE notebook.run() calls still need to be normalized
    to match actual AIDP file names (space → underscore, +.ipynb, dep_path_map
    lookup), otherwise runtime calls fail because original Databricks paths
    don't resolve on AIDP.

    Only touches the path string inside `(dbutils|oidlUtils).notebook.run("...")`,
    leaves all other Scala syntax untouched.
    """
    if not source or not source.strip():
        return source

    import re as _re
    if dep_path_map is None:
        dep_path_map = {}

    def _lookup(raw):
        # Try multiple key variants — dep_path_map stores several forms per dep
        candidates = [raw]
        if not raw.endswith(".ipynb"):
            candidates.append(raw + ".ipynb")
        else:
            candidates.append(raw[:-6])
        if raw.startswith("/Workspace/"):
            candidates.append(raw[len("/Workspace/"):])
        for c in candidates:
            if c in dep_path_map and dep_path_map[c]:
                return dep_path_map[c]
        return None

    def _normalize_path(raw):
        # Strip stray comments that may have leaked in
        raw = _re.split(r'\s*#\s*Oracle tool modification', raw, maxsplit=1)[0].rstrip()
        hit = _lookup(raw)
        if hit:
            return hit
        # No mapping — apply minimal AIDP-compatible normalization
        fixed = raw
        if "/Workspace/" in fixed:
            fixed = fixed.replace(" ", "_")
        if not fixed.endswith(".ipynb") and not fixed.endswith(".py"):
            fixed += ".ipynb"
        return fixed

    # (dbutils|oidlUtils).notebook.run("path", ...) — also do API rename
    pattern = _re.compile(
        r'(dbutils|oidlUtils)(\.notebook\.run\s*\(\s*)(["\'])([^"\']+)\3'
    )

    def _replace(m):
        _api, middle, quote, raw_path = m.group(1), m.group(2), m.group(3), m.group(4)
        new_path = _normalize_path(raw_path)
        return f"oidlUtils{middle}{quote}{new_path}{quote}"

    modified = pattern.sub(_replace, source)

    # ── Table-ref normalization runs BEFORE the Cassandra block ────────
    # `_normalize_table_refs` only touches spark.sql/spark.table/.saveAsTable calls.
    # The original Cassandra cell uses session.execute(...) which it ignores. Running
    # the normalizer FIRST means the spark.sql(...) calls that our Cassandra-to-Spark
    # substitutions emit later won't be re-qualified to 3-part — which would otherwise
    # produce 4-segment names like `default.default.scylla_$ks.bar` (invalid).
    # Non-Scylla cells are unaffected (the Cassandra block below is a no-op for them).
    modified, _tbl_changed_early = _normalize_table_refs(modified)

    # ── Cassandra/Scylla → AIDP metastore (deterministic safety net) ──
    # Handles the most common Cassandra patterns before the cell is sent to Opus, so
    # we get correct output even if the LLM drifts. Applied ONLY to %scala cells
    # that actually contain Cassandra references (no-op otherwise — cheap check).
    # This is a TEMPORARY mapping: keyspace → scylla_<keyspace> schema, table name preserved.
    # Anything fancier than the patterns below falls through to Opus + CELL_MIGRATE_PROMPT
    # rules (see SCYLLADB → AIDP METASTORE MIGRATION section).
    _has_cassandra = bool(_re.search(
        r'\b(CassandraConnector|withSessionDo|SimpleStatement|session\.execute|'
        r'system_schema|com\.datastax|org\.apache\.spark\.sql\.cassandra)\b',
        modified,
    ))
    if _has_cassandra:
        _scylla_changed = False
        # 0a. Strip Cassandra/datastax imports — connector is not available on runtime cluster.
        _before = modified
        modified = _re.sub(
            r'^\s*import\s+com\.datastax\.spark\.connector\..*$\n?',
            '// Oracle tool modification: removed cassandra-spark-connector import (AIDP metastore migration)\n',
            modified,
            flags=_re.MULTILINE,
        )
        if modified != _before:
            _scylla_changed = True

        # 0b. Strip CassandraConnector(...).withSessionDo { ... } wrapper, keeping the body.
        # The body has already been translated to use spark.sql() by the substitutions below,
        # so `session` is no longer referenced — the wrapper is dead weight that ALSO requires
        # cassandra-spark-connector to be present on the runtime cluster. Strip with
        # brace-counting (regex can't reliably handle nested braces in the body).
        def _strip_withsession_wrapper(text):
            """Find CassandraConnector(...).withSessionDo { session => BODY } and inline BODY.
            Hard-bounded to MAX_ITER strips per cell so a regression in the replacement
            string can never cause an infinite loop."""
            MAX_ITER = 32
            anchor = _re.compile(r'CassandraConnector\s*\([^)]*\)\s*\.withSessionDo\s*\{')
            out = text
            iter_count = 0
            while iter_count < MAX_ITER:
                iter_count += 1
                m = anchor.search(out)
                if not m:
                    return out, out != text
                start = m.start()
                brace_open = m.end() - 1  # position of '{'
                depth = 1
                i = brace_open + 1
                while i < len(out) and depth > 0:
                    c = out[i]
                    if c == '{':
                        depth += 1
                    elif c == '}':
                        depth -= 1
                    i += 1
                if depth != 0:
                    # Unmatched brace — bail out without modifying
                    return out, out != text
                brace_close = i - 1
                # Detect optional arrow: "session =>" or "_ =>" at start of body
                inner = out[brace_open+1:brace_close]
                arrow_m = _re.match(r'\s*(?:\w+|_)\s*=>\s*', inner)
                body_offset = arrow_m.end() if arrow_m else 0
                body = out[brace_open+1+body_offset:brace_close].strip("\n")
                # Replace the entire `CassandraConnector(...).withSessionDo { ... }` with
                # a comment + the de-wrapped body. The comment must NOT contain the literal
                # regex anchor (CassandraConnector...withSessionDo...{) — otherwise the next
                # iteration of the outer while-loop would re-match its own replacement string
                # and recurse forever. Use safe paraphrasing instead.
                replacement = (
                    "// Oracle tool modification: Cassandra session wrapper stripped "
                    "(AIDP metastore migration -- session variable is no longer used "
                    "after body translation).\n"
                    + body
                )
                out = out[:start] + replacement + out[brace_close+1:]
            # unreachable

        _before = modified
        modified, _w_changed = _strip_withsession_wrapper(modified)
        if _w_changed:
            _scylla_changed = True

        # Substitutions below ALWAYS emit `scylla_<keyspace>` as the schema name. The
        # ScyllaDB keyspace is mirrored to the AIDP metastore as a schema prefixed with
        # `scylla_` for easy identification (e.g. sample_keyspace →
        # scylla_sample_keyspace). Original keyspace identifier is preserved
        # inside the prefix — Scala s-string interpolation handles the rest: a literal
        # "scylla_" + "$keyspaceVar" + ".<table>" → at runtime "scylla_<actualKeyspace>.<table>".

        # 1. session.execute("DROP TABLE …") → spark.sql with IF EXISTS + scylla_ schema prefix
        _before = modified
        modified = _re.sub(
            r'session\.execute\(\s*s?"DROP TABLE\s+([^"]+?)"\s*\)',
            r'spark.sql(s"DROP TABLE IF EXISTS scylla_\1")',
            modified,
        )
        if modified != _before:
            _scylla_changed = True
        # 2. session.execute("TRUNCATE …") → spark.sql("TRUNCATE TABLE …") + scylla_ schema prefix
        _before = modified
        modified = _re.sub(
            r'session\.execute\(\s*s?"TRUNCATE\s+([^"]+?)"\s*\)',
            r'spark.sql(s"TRUNCATE TABLE scylla_\1")',
            modified,
        )
        if modified != _before:
            _scylla_changed = True
        # 3. system_schema.tables enumeration → SHOW TABLES IN scylla_$ks (handles both
        #    .map(_.getString("table_name")) and .map(_.getString(0)) trailing forms)
        _before = modified
        modified = _re.sub(
            r"session\.execute\(\s*s?\"SELECT table_name FROM system_schema\.tables "
            r"WHERE keyspace_name\s*=\s*'\$(\w+)'\"\s*\)"
            r"\s*\.all\(\)\s*\.asScala"
            r"\s*\.map\(_\.getString\((?:\"table_name\"|0)\)\)",
            r'spark.sql(s"SHOW TABLES IN scylla_$\1").collect().map(_.getString(1))',
            modified,
        )
        if modified != _before:
            _scylla_changed = True
        # 4. Bulk DataFrame Cassandra read → spark.read.table with scylla_ schema prefix
        _before = modified
        modified = _re.sub(
            r'spark\.read\.format\(\s*"org\.apache\.spark\.sql\.cassandra"\s*\)'
            r'\s*\.options\(\s*Map\(\s*"keyspace"\s*->\s*(\w+)\s*,\s*"table"\s*->\s*(\w+)\s*\)\s*\)'
            r'\s*\.load\(\s*\)',
            r'spark.read.table(s"scylla_$\1.$\2")',
            modified,
        )
        if modified != _before:
            _scylla_changed = True
        # 5. Bulk DataFrame Cassandra write → saveAsTable with overwrite + scylla_ schema prefix
        _before = modified
        modified = _re.sub(
            r'(\b\w+)\.write\s*\.format\(\s*"org\.apache\.spark\.sql\.cassandra"\s*\)'
            r'\s*\.options\(\s*Map\(\s*"keyspace"\s*->\s*(\w+)\s*,\s*"table"\s*->\s*(\w+)\s*\)\s*\)'
            r'\s*\.save\(\s*\)',
            r'\1.write.mode("overwrite").saveAsTable(s"scylla_$\2.$\3")  '
            r'// Oracle tool modification: write mode forced to "overwrite" (was Cassandra upsert).'
            r' Temporary — restore original semantics when Scylla becomes directly reachable on AIDP.',
            modified,
        )
        if modified != _before:
            _scylla_changed = True
        # If anything changed, prepend the temporary-mapping marker just below the %scala magic.
        if _scylla_changed:
            _marker = (
                "// Oracle tool modification: ScyllaDB → AIDP metastore (temporary).\n"
                "//   Cassandra Cluster/Session calls replaced with Spark catalog access.\n"
                "//   Schema name = scylla_<keyspace> (original keyspace prefixed with\n"
                "//   `scylla_` for identification); table name preserved. Existence\n"
                "//   guards may be needed at the call site if the metastore mirror is\n"
                "//   not yet populated. Revisit if Scylla becomes directly reachable on AIDP."
            )
            _lines = modified.split("\n")
            if _lines and _lines[0].strip().startswith("%scala"):
                _lines.insert(1, _marker)
            else:
                _lines.insert(0, _marker)
            modified = "\n".join(_lines)

    # Table-ref normalization comment annotation. The actual normalization happened
    # EARLIER (before the Cassandra/Scylla block) so that scylla-mirrored 2-part
    # names (`scylla_$ks.bar`) don't get over-qualified. Annotation is added once.
    if _tbl_changed_early:
        # Scala line comments use //
        note = "// Oracle tool modification: table refs tagged to default catalog/schema (AIDP requires 3-part: catalog.schema.table)"
        # Insert after %scala magic line, not at top
        lines = modified.split("\n")
        if lines and lines[0].strip().startswith("%scala"):
            lines.insert(1, note)
        else:
            lines.insert(0, note)
        modified = "\n".join(lines)

    # dbutils.widgets.get("X") → oidlUtils.parameters.getParameter("X", "") (Scala).
    # `dbutils` is a Python object (aidp_compat) and is NOT in scope in the Scala kernel
    # ("not found: value dbutils"). The Python preprocessor (_preprocess_cell_source)
    # already does this conversion, but Scala cells use THIS preprocessor — so without
    # it, the conversion was left to the (flaky) Opus fix loop. Do it deterministically.
    # At EXEC time, _scala_param_exec_rewrite further bridges
    # oidlUtils.parameters.getParameter → spark.conf.get("spark.aidp.param.X"), so the
    # value is read from the injected spark.conf params during validation; the SAVED
    # cell keeps the production-correct oidlUtils form.
    _WIDGETS_GET_RE = _re.compile(r"dbutils\.widgets\.get\s*\(\s*([^)]+?)\s*\)")
    if _WIDGETS_GET_RE.search(modified):
        _wlines = []
        for _line in modified.split("\n"):
            _ls = _line.lstrip()
            if (not _ls.startswith("//")) and (not _ls.startswith("#")) and _WIDGETS_GET_RE.search(_line):
                _pad = " " * (len(_line) - len(_ls))
                _fixed = _WIDGETS_GET_RE.sub(
                    lambda m: f'oidlUtils.parameters.getParameter({m.group(1)}, "")', _line)
                _wlines.append(
                    f"{_pad}// Oracle tool modification: dbutils.widgets.get → oidlUtils.parameters.getParameter (AIDP Scala)\n"
                    f"{_pad}// {_ls}\n"
                    f"{_fixed}")
            else:
                _wlines.append(_line)
        modified = "\n".join(_wlines)

    # Authoritative OCI namespace: any oci://<bucket>@<ns> already in the source
    # gets <ns> overwritten with the mapping's value (exported notebooks often
    # carry a wrong namespace). Saved + exec both corrected.
    modified = _apply_namespace_from_mapping(modified)
    return modified


def _preprocess_cell_source(source: str, dep_path_map: dict = None) -> str:
    """Programmatic pre-processing applied to every cell source BEFORE Opus migration.

    Handles transformations that Opus cannot reliably do on its own:
    1. pandas S3 reads → wrap path with translate_path() (ocifs handles oci://)
    2. Old /Workspace/ internal paths → migrated paths from dep_path_map
    """
    if not source or not source.strip():
        return source

    import re as _re

    # ── 1. pandas S3 reads → wrap path with translate_path() ──────────
    # pandas stays pandas (ocifs is installed on the cluster, so pd.read_*
    # can read oci:// directly). Only the s3:// path is rewritten; all
    # other kwargs (sep, header, columns, storage_options, ...) are kept.
    _PD_S3_PATTERNS = [
        (
            _re.compile(r'pd\.read_csv\s*\(\s*(["\'])s3a?://([^"\']+)\1([^)]*)\)'),
            lambda m: (
                f'pd.read_csv(translate_path("s3://{m.group(2)}"){m.group(3)})'
            ),
        ),
        (
            _re.compile(r'pd\.read_parquet\s*\(\s*(["\'])s3a?://([^"\']+)\1([^)]*)\)'),
            lambda m: (
                f'pd.read_parquet(translate_path("s3://{m.group(2)}"){m.group(3)})'
            ),
        ),
        (
            _re.compile(r'pd\.read_json\s*\(\s*(["\'])s3a?://([^"\']+)\1([^)]*)\)'),
            lambda m: (
                f'pd.read_json(translate_path("s3://{m.group(2)}"){m.group(3)})'
            ),
        ),
    ]

    modified = source
    translate_import_needed = False
    for pattern, replacer in _PD_S3_PATTERNS:
        if pattern.search(modified):
            modified = pattern.sub(replacer, modified)
            translate_import_needed = True

    # Inject translate_path import if we made S3 replacements and it's not already imported
    if translate_import_needed and "translate_path" not in modified:
        modified = "from aidp_compat import translate_path  # Oracle tool modification: added for S3→OCI path translation\n" + modified

    # ── 2. %matplotlib inline → comment out ───────────────────────────
    # AIDP is a headless environment — %matplotlib inline is a Jupyter/Databricks magic
    # that does not work on AIDP. matplotlib.use('Agg') is injected by the migration
    # tool via CELL_MIGRATE_PROMPT when a cell uses matplotlib. Just comment out the magic.
    modified = _re.sub(
        r'^([ \t]*)(%matplotlib\s+\S+)',
        r'\1# Oracle tool modification: Databricks magic not supported on AIDP\n\1# \2',
        modified,
        flags=_re.MULTILINE,
    )

    # ── 2-restart. dbutils.library.restartPython() → comment out ──────
    # AIDP cluster doesn't support kernel restart from user code; package install
    # happens via cluster libraries API before the kernel starts. Calling this on
    # AIDP raises AttributeError or hangs the session. Just comment it out.
    modified = _re.sub(
        r'^([ \t]*)(dbutils\.library\.restartPython\s*\([^)]*\))',
        r'\1# Oracle tool modification: restartPython not supported on AIDP (cluster libraries managed externally)\n\1# \2',
        modified,
        flags=_re.MULTILINE,
    )

    # ── 11a. Unicode normalization ─────────────────────────────────────
    # Notebooks copy-pasted from docs/Slack often contain invisible Unicode chars
    # that break Python parsing in subtle ways:
    #   - U+00A0 (non-breaking space)  → looks like space, parser rejects
    #   - U+201C/U+201D (curly quotes) → not valid Python string delimiters
    #   - U+2018/U+2019 (curly apos)   → not valid Python string delimiters
    #   - U+2013/U+2014 (en/em dash)   → confusing in arithmetic / option flags
    #   - U+200B (zero-width space)    → invisible breakage
    _UNICODE_FIXES = [
        (" ", " "),    # non-breaking space → regular space
        ("“", '"'),    # left double quote
        ("”", '"'),    # right double quote
        ("‘", "'"),    # left single quote
        ("’", "'"),    # right single quote
        ("–", "-"),    # en dash
        ("—", "-"),    # em dash
        ("​", ""),     # zero-width space (just remove)
        ("﻿", ""),     # BOM (just remove)
    ]
    for bad, good in _UNICODE_FIXES:
        if bad in modified:
            modified = modified.replace(bad, good)

    # ── 11b. Triple-quote / unbalanced-quote table reads ──── REMOVED ──
    # Previously this transform tried to fix mismatched triple-quote typos
    # like `spark.read.table("""X"")` → `spark.read.table("X")`. The regex
    # `(''')([^"'\n]+?)(''|')` looked for `'''<body><single-or-double-quote>`
    # with body excluding any quote.
    #
    # Why this was REMOVED — Bug B (root-cause):
    # source notebooks frequently deactivate code blocks by wrapping them
    # in `'''...'''`. When the FIRST line of such a block contains an inner
    # string literal (e.g., `'''<varname> = spark.read.format('org.apache.hudi')...`),
    # the regex's non-greedy body stops at the first internal `'`, and that
    # internal `'` is captured as a "mismatched closer". The replacement
    # then strips TWO of the three opening quotes, producing
    #   `'<varname> = spark.read.format('org.apache.hudi')...`
    # which Python reads as an unterminated single-quoted string literal.
    #
    # This pattern can fire across multiple runs and cells.
    # Opus's call_fix loop eventually recovered each occurrence, but at the
    # cost of extra API calls and risk of mis-fix. Removing the transform
    # entirely is safer than trying to make the regex precise — real
    # mismatched-triple-quote typos in customer code are very rare, and
    # Opus's normal migration + fix pipeline handles them when they occur.

    # ── 11c. Strip inline comments from %run lines ─────────────────────
    # IPython parses everything after `%run <path>` as args/params — `# foo`
    # gets interpreted as a path arg and breaks resolution. Strip any trailing
    # `# ...` from %run lines BEFORE _rewrite_internal_paths sees them.
    # (The migration tool's own "# Oracle tool modification" comments are
    # appended AFTER this preprocess step by _rewrite_internal_paths, so
    # stripping here doesn't remove them.)
    def _strip_run_comment(m):
        prefix = m.group(1)   # leading whitespace + '%run '
        rest   = m.group(2)
        # Cut at first ' #' (space-hash) — preserves $params with no '#' in them
        comment_idx = rest.find(" #")
        if comment_idx >= 0:
            rest = rest[:comment_idx].rstrip()
        return f"{prefix}{rest}"
    modified = _re.sub(
        r'(^[ \t]*%run\s+)(.+)$',
        _strip_run_comment,
        modified,
        flags=_re.MULTILINE,
    )

    # ── 2a. %%scala → %scala normalization ─────────────────────────────
    # Databricks notebooks use single-% magic (%scala). Some converted notebooks
    # may have %%scala (Jupyter-style); normalize to %scala which AIDP supports.
    modified = _re.sub(r'^[ \t]*%%scala\b', '%scala', modified, flags=_re.MULTILINE)

    # ── 2b. notebook.run(path, 0, ...) → notebook.run(path, 3600, ...) ──
    # Databricks accepts timeout=0 as "no timeout"; AIDP rejects 0 as null/invalid.
    # Replace literal 0 timeout with 3600 (1 hour) — the second positional arg.
    # NOTE: no inline comment (would land mid-call and break syntax).
    modified = _re.sub(
        r'((?:dbutils|oidlUtils)\.notebook\.run\s*\(\s*[^,]+?\s*,\s*)0(\s*,)',
        r'\g<1>3600\g<2>',
        modified,
    )

    # ── 2c. OCI client init (oci.config.from_file + ObjectStorageClient) → wrap in try/except ──
    # Notebooks often hardcode `oci.config.from_file("/Workspace/<user>/Config", ...)` at
    # module-load time. If that path doesn't exist on AIDP, the entire notebook fails to
    # import. Wrap the OCI init block in try/except so the notebook still loads.
    _OCI_INIT_RE = _re.compile(
        r'^([ \t]*)(_\w+_config\s*=\s*oci\.config\.from_file\([^\n]+\)[\s\S]*?'
        r'_\w+_namespace\s*=\s*_\w+_client\.get_namespace\(\)\.data)[ \t]*$',
        _re.MULTILINE,
    )

    def _wrap_oci_init(m):
        indent = m.group(1)
        block = m.group(2)
        # Indent each line of the block by 4 more spaces
        indented_block = "\n".join(indent + "    " + l[len(indent):] if l.startswith(indent) else "    " + l
                                   for l in block.split("\n"))
        return (
            f"{indent}# Oracle tool modification: OCI init wrapped in try/except (config path may not exist on AIDP)\n"
            f"{indent}try:\n"
            f"{indented_block}\n"
            f"{indent}except Exception as _e:\n"
            f"{indent}    print(f'OCI client init skipped: {{_e}}')\n"
            f"{indent}    _oci_client = None\n"
            f"{indent}    _oci_namespace = None"
        )

    if _OCI_INIT_RE.search(modified):
        modified = _OCI_INIT_RE.sub(_wrap_oci_init, modified)

    # ── 3. Old /Workspace/ internal paths → migrated paths ────────────
    # Uses dep_path_map (original_path → migrated_path) built from the manifest.
    # Plain string replace (no inline comment) — the path may appear inside
    # quoted args of dbutils.notebook.run("path", ...) and appending a comment
    # mid-string corrupts the call.
    if dep_path_map:
        for orig_path, mig_path in dep_path_map.items():
            if not orig_path or not mig_path:
                continue
            orig_norm = orig_path.lstrip("/")
            ws_orig = f"/Workspace/{orig_norm}" if not orig_path.startswith("/Workspace/") else orig_path
            if ws_orig in modified:
                modified = modified.replace(ws_orig, mig_path)

    # ── 4. get_glue_table_s3_location call-site rewrite REMOVED (2026-05-19) ─
    # Earlier versions of this preprocess pattern-matched
    #   <var> = get_glue_table_s3_location("db", "tbl")
    # and rewrote the line to
    #   <var> = "db.tbl"
    # (a hardcoded catalog identifier). That was wrong: the variable name
    # advertises a PATH (the helper returns a storage-location STRING on
    # both Databricks and AIDP), and downstream code does
    #   spark.read.parquet(f'{<var>}/load_date=...')
    # which breaks when <var> holds a catalog identifier instead of a path.
    # The function is now expected to be body-swapped at its DEFINITION
    # SITE (see the AIDP LOCATION EXTRACTION rule in the prompts); call
    # sites must stay unchanged. This deterministic call-site rewrite
    # contradicted that contract.
    #
    # ── 5. spark.read.format('hudi').load(var) rewrite REMOVED (2026-05-19) ─
    # Tied to rule 4 above — it converted Hudi path reads into
    # spark.read.table calls assuming <var> was a catalog identifier
    # set by rule 4. With rule 4 removed, <var> is still a path string,
    # so the Hudi → spark.read.table conversion would now be wrong.
    # Hudi-specific migrations (if needed) are handled by Opus per-cell
    # with full prompt context.

    # ── 6. dbutils.widgets.get(...) → oidlUtils.parameters.getParameter(..., "") ──
    # Comment original line, add oidlUtils replacement directly below.
    _WIDGETS_GET_RE = _re.compile(r"dbutils\.widgets\.get\s*\(\s*([^)]+?)\s*\)")
    if _WIDGETS_GET_RE.search(modified):
        new_lines = []
        for line in modified.split("\n"):
            stripped = line.lstrip()
            if (not stripped.startswith("#")) and _WIDGETS_GET_RE.search(line):
                indent = len(line) - len(stripped)
                pad = " " * indent
                fixed_line = _WIDGETS_GET_RE.sub(
                    lambda m: f'oidlUtils.parameters.getParameter({m.group(1)}, "")',
                    line,
                )
                new_lines.append(
                    f"{pad}# Oracle tool modification: dbutils.widgets.get → oidlUtils.parameters.getParameter (AIDP)\n"
                    f"{pad}# {stripped}\n"
                    f"{fixed_line}"
                )
            else:
                new_lines.append(line)
        modified = "\n".join(new_lines)

    # ── 7-job. Databricks job triggers → AIDP run_job_and_wait ──────────
    # Detects job_call(...) / job_calling(...) / call_job_internal(...) with a
    # Databricks job_id (literal or var). Looks up the AIDP UUID in the
    # _db_to_aidp_job_map (loaded from manifest), rewrites to the AIDP form.
    # Unmapped IDs accumulate in _unmapped_db_job_ids and surface in JOB_REPORT.
    modified, _job_changed = _normalize_job_triggers(modified)

    # ── 7. Table refs → 3-part `default.default.tbl` / `default.db.tbl` ──
    # AIDP requires fully-qualified catalog.schema.table; Hive 1/2-part refs
    # silently fail. Also covers spark.sql() bodies and %sql cell content.
    modified, _tbl_changed = _normalize_table_refs(modified)
    if _tbl_changed:
        is_sql_cell = modified.lstrip().startswith("%sql")
        if is_sql_cell:
            note = "-- Oracle tool modification: table refs tagged to default catalog/schema (AIDP requires 3-part: catalog.schema.table)"
        else:
            note = "# Oracle tool modification: table refs tagged to default catalog/schema (AIDP requires 3-part: catalog.schema.table)"
        modified = note + "\n" + modified

    # Authoritative OCI namespace from the mapping (see scala preprocessor).
    modified = _apply_namespace_from_mapping(modified)
    return modified


def _ensure_dbutils_import(cells: list) -> list:
    """Prepend aidp_compat import to the first code cell if dbutils is used but not imported.

    Applied AFTER all cells are migrated, BEFORE notebook save.
    Safe to call multiple times — skips if import already present.
    """
    uses_dbutils = any(
        "dbutils" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    )
    if not uses_dbutils:
        return cells

    already = any(
        "from aidp_compat import" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    )
    if already:
        return cells

    # Never inject the Python import into a line-magic cell. A %run cell must
    # contain ONLY the %run line (AIDP parses it as a magic, not Python), and
    # %scala runs in a different kernel; %sql/%sh/%pip are likewise not Python.
    # Land the import in the first plain Python code cell instead.
    _MAGIC_PREFIXES = ("%run", "%scala", "%sql", "%sh", "%pip", "%fs", "%md", "%%")
    for c in cells:
        if c.get("cell_type") == "code":
            src = "".join(c.get("source", []))
            if src.lstrip().startswith(_MAGIC_PREFIXES):
                continue
            c["source"] = [_AIDP_IMPORT_LINE + src]
            tprint("  [dbutils_import] injected aidp_compat import into first Python code cell")
            return cells

    # All code cells are magic cells — insert a dedicated import cell at the top.
    import_cell = {
        "cell_type": "code",
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": [_AIDP_IMPORT_LINE],
    }
    insert_at = next(
        (i for i, c in enumerate(cells) if c.get("cell_type") == "code"), 0
    )
    cells.insert(insert_at, import_cell)
    tprint("  [dbutils_import] inserted dedicated aidp_compat import cell (all code cells were magic cells)")
    return cells


def _ensure_invoke_job_helper(cells: list) -> list:
    """If any cell calls _aidp_run_job_and_wait, inject the helper definition
    once at the top of the notebook so the migrated notebook is self-contained
    (no dependency on /Workspace/<deploy_dir>/<job_runner>.py existing).

    Idempotent — skips if the helper definition is already present.
    """
    uses_helper = any(
        "_aidp_run_job_and_wait(" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    )
    if not uses_helper:
        return cells

    already = any(
        "def _aidp_run_job_and_wait(" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    )
    if already:
        return cells

    helper_src = _build_aidp_invoke_helper()
    helper_cell = {
        "cell_type": "code",
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": [helper_src],
    }
    # Insert at the very top so the helper is available to every subsequent cell.
    new_cells = [helper_cell] + list(cells)
    tprint("  [invoke_job_helper] injected inlined run_job_and_wait helper into notebook")
    return new_cells


# ─── Inline Child Notebook Execution ─────────────────────────────────

# MIGRATED_BASE is set per-job in process_job(): {OUTPUT_BASE}/{job_name}/notebooks
MIGRATED_BASE = f"{OUTPUT_BASE}/notebooks"  # default, overridden per job
JOB_ROOT = ""  # workspace root from manifest, stripped from paths by normalize_nb_path
# Export base (workspace-relative, e.g. "ai_dbx_exported/notebooks"), set
# externally when the manifest's notebooks live under a dbx_export base
# (currently only job_migrate_from_workflow sets it). When set: (1) discovered
# %run/notebook.run deps are SOURCED from under this base (never the raw
# /Workspace/Users original), and (2) the base prefix is stripped when computing
# the migrated output path so it mirrors the original user tree (no
# path-doubling). Empty string = non-export flow, behaviour unchanged.
EXPORT_BASE = ""

def _native_lang_from_nb(nb: dict) -> str:
    """Return 'scala'/'sql' if a parsed notebook's default language is native
    Scala/SQL (from metadata), else ''. Used to prefix bare default-language
    cells with the explicit %scala/%sql magic so they aren't misclassified as
    Python (which would inject a Python bootstrap into a Scala cell)."""
    meta = nb.get("metadata", {}) or {}
    lang = (meta.get("language_info", {}).get("name")
            or meta.get("kernelspec", {}).get("language") or "").lower()
    return lang if lang in ("scala", "sql") else ""


async def _inline_child_notebook(
    child_path: str,
    session: AIDPSession,
    log: 'NotebookLog',
    job_name: str,
    analysis: str,
    catalog_context: str,
    dependent_context: str,
    dep_path_map: Dict[str, str],
    capture_conf_key: str = None,
) -> dict:
    """Inline a child notebook: read it, migrate+execute each cell in the
    current session (parent's namespace), fix failures. This replaces
    oidlUtils.notebook.run() / dbutils.notebook.run() with direct cell execution so fragments that
    depend on parent scope work correctly.

    Returns {"status": "ok"|"error", "cells_ok": N, "cells_fixed": N, "error": "..."}
    """
    # Read the child notebook cells.
    # Strategy: in-memory cache → cluster filesystem → AIDP source API download.
    # The in-memory cache bypasses FUSE entirely — os.path.exists() on AIDP's
    # /Workspace NFS/FUSE mount can return False even seconds after a verified upload
    # (observed: file verified at 12:53:28, os.path.exists False at 12:53:52,
    # same kernel, same session, no reconnects — not a cache timeout issue).
    from context_tools import _unwrap_aidp_text

    child_cells = None  # will be set by whichever source succeeds
    child_lang = ""     # native notebook language ('scala'/'sql') if applicable

    # --- Source 1: In-memory cache (fastest, most reliable) ---
    # Check for exact path and common variants (with/without .ipynb)
    cache_candidates = [child_path]
    if not child_path.endswith('.ipynb'):
        cache_candidates.append(child_path + '.ipynb')
    for cpath in cache_candidates:
        cached_content = _notebook_content_cache.get(cpath)
        if cached_content:
            try:
                nb = json.loads(cached_content)
                child_cells = []
                for cell in nb.get("cells", []):
                    if cell.get("cell_type") == "code":
                        src = "".join(cell.get("source", []))
                        if src.strip() and _CONFIG_MARKER not in src:
                            child_cells.append(src)
                child_lang = _native_lang_from_nb(nb)
                log.log(f"    [child:{child_path.split('/')[-1]}] Loaded from in-memory cache ({len(child_cells)} code cells)")
                break
            except Exception as e:
                log.log(f"    [child:{child_path.split('/')[-1]}] In-memory cache parse error: {e}")

    # --- Source 2: Cluster filesystem (os.path.exists with FUSE retry) ---
    if child_cells is None:
        safe_path = child_path.replace("'", "\\'")
        count_data = {"error": "not_tried"}
        for _fuse_attempt in range(1):
            count_result = await session.execute(f"""
import json, os, time
path = '{safe_path}'
if not path.endswith('.ipynb'):
    path += '.ipynb'
found_path = None
for p in [path, path.replace(' ', '_'), path.replace('_', ' ')]:
    if os.path.exists(p):
        found_path = p
        break
if not found_path:
    parent = os.path.dirname(path)
    if os.path.isdir(parent):
        _listing = os.listdir(parent)
        time.sleep(1)
        for p in [path, path.replace(' ', '_'), path.replace('_', ' ')]:
            if os.path.exists(p):
                found_path = p
                break
        if not found_path:
            print(json.dumps({{'error': 'not_found', 'parent_listing': _listing[:20]}}))
    else:
        print(json.dumps({{'error': 'not_found', 'parent_exists': False, 'parent': parent}}))
if found_path:
    with open(found_path) as f:
        nb = json.load(f)
    # Skip the AIDP config cell (prepended at save time by process_notebook).
    # Including it shifts every real cell by one on save-back.
    _CFG_MARKER = '# AIDP performance configuration'
    code_cells = [i for i, c in enumerate(nb.get('cells', []))
                  if c.get('cell_type') == 'code'
                  and ''.join(c.get('source', [])).strip()
                  and _CFG_MARKER not in ''.join(c.get('source', []))]
    print(json.dumps({{'count': len(code_cells), 'indices': code_cells, 'resolved_path': found_path, 'lang': (nb.get('metadata', {{}}).get('language_info', {{}}).get('name', '') or '')}}))
""", timeout=30)
            count_output = _unwrap_aidp_text(format_outputs(count_result.get("outputs", [])))

            try:
                count_data = json.loads(count_output)
            except:
                count_data = {"error": "parse_failed"}

            if not count_data.get("error"):
                break  # found it
            # Log diagnostic info from the cluster
            parent_listing = count_data.get("parent_listing")
            if parent_listing is not None:
                log.log(f"    [child:{child_path.split('/')[-1]}] FUSE miss (attempt {_fuse_attempt+1}), parent has {len(parent_listing)} files: {parent_listing[:5]}")
            elif count_data.get("parent_exists") is False:
                log.log(f"    [child:{child_path.split('/')[-1]}] FUSE miss (attempt {_fuse_attempt+1}), parent dir does not exist: {count_data.get('parent')}")
            else:
                log.log(f"    [child:{child_path.split('/')[-1]}] FUSE miss (attempt {_fuse_attempt+1})")
            if _fuse_attempt < 2:
                await asyncio.sleep(5)

        if not count_data.get("error"):
            child_lang = child_lang or (count_data.get("lang", "") or "").lower()
            # Read each cell individually from the cluster filesystem
            cell_indices = count_data.get("indices", [])
            child_cells = []
            for idx in cell_indices:
                cell_result = await session.execute(f"""
import json
with open('{safe_path}') as f:
    nb = json.load(f)
src = ''.join(nb['cells'][{idx}].get('source', []))
print(src)
""", timeout=30)
                cell_src = _unwrap_aidp_text(format_outputs(cell_result.get("outputs", [])))
                if cell_src.strip() and _CONFIG_MARKER not in cell_src:
                    child_cells.append(cell_src)

    # --- Source 3: Try migrated copy via direct open() (bypass FUSE os.path.exists) ---
    # FUSE's os.path.exists() is unreliable on AIDP — returns False even for files that
    # were just uploaded. But open() often succeeds. Try child_path directly first (it's
    # typically already a MIGRATED_BASE path), then fall back to _migration_cache lookups.
    if child_cells is None:
        migrated_paths_to_try = []
        # Try child_path itself first — it's usually MIGRATED_BASE/relative and the file
        # was uploaded to exactly this path by ensure_migrated().
        _cp = child_path if child_path.endswith(".ipynb") else child_path + ".ipynb"
        migrated_paths_to_try.append(_cp)

        # Check _migration_cache for normalized form of child_path.
        # When child_path starts with MIGRATED_BASE, normalize_nb_path() can't strip
        # JOB_ROOT (different prefix), so also try stripping MIGRATED_BASE directly
        # to get the relative key that ensure_migrated() used.
        _norm_keys_to_try = [
            normalize_nb_path(child_path),
            child_path.replace("/Workspace/", "").replace(" ", "_"),
        ]
        # Strip MIGRATED_BASE prefix to get the relative key (e.g. "ExampleUtils/0_Imports.ipynb")
        _migrated_prefix = MIGRATED_BASE.rstrip("/") + "/"
        if child_path.startswith(_migrated_prefix):
            _rel_key = child_path[len(_migrated_prefix):]
            _norm_keys_to_try.insert(0, _rel_key)
        for _norm_key in _norm_keys_to_try:
            if not _norm_key.endswith(".ipynb"):
                _norm_key += ".ipynb"
            cached_migrated = _migration_cache.get(_norm_key)
            if cached_migrated and cached_migrated not in migrated_paths_to_try:
                migrated_paths_to_try.append(cached_migrated)
        # Only construct MIGRATED_BASE path if child_path is NOT already under it
        # (otherwise we'd create a double-nested path)
        if not child_path.startswith(MIGRATED_BASE):
            _mb_path = f"{MIGRATED_BASE}/{normalize_nb_path(child_path)}"
            if not _mb_path.endswith(".ipynb"):
                _mb_path += ".ipynb"
            if _mb_path not in migrated_paths_to_try:
                migrated_paths_to_try.append(_mb_path)

        for mig_path in migrated_paths_to_try:
            safe_mig = mig_path.replace("'", "\\'")
            try:
                read_result = await session.execute(f"""
import json
try:
    with open('{safe_mig}') as f:
        nb = json.load(f)
    # Skip the AIDP config cell (prepended at save time). Including it would
    # shift every real cell by one when the save-back applies migrated_child_cells.
    _CFG_MARKER = '# AIDP performance configuration'
    cells = [i for i, c in enumerate(nb.get('cells', []))
             if c.get('cell_type') == 'code'
             and ''.join(c.get('source', [])).strip()
             and _CFG_MARKER not in ''.join(c.get('source', []))]
    print(json.dumps({{'ok': True, 'count': len(cells), 'lang': (nb.get('metadata', {{}}).get('language_info', {{}}).get('name', '') or ''), 'cells': [{{'src': ''.join(nb['cells'][i].get('source', []))}} for i in cells]}}))
except FileNotFoundError:
    print(json.dumps({{'ok': False}}))
except Exception as e:
    print(json.dumps({{'ok': False, 'error': str(e)[:200]}}))
""", timeout=30)
                read_output = _unwrap_aidp_text(format_outputs(read_result.get("outputs", [])))
                read_data = json.loads(read_output)
                if read_data.get("ok"):
                    child_lang = child_lang or (read_data.get("lang", "") or "").lower()
                    child_cells = [c["src"] for c in read_data["cells"]
                                   if c["src"].strip() and _CONFIG_MARKER not in c["src"]]
                    log.log(f"    [child:{child_path.split('/')[-1]}] Loaded migrated copy from {os.path.basename(mig_path)} ({len(child_cells)} code cells)")
                    break
            except Exception:
                pass

    # --- Source 4: AIDP source API download (original — last resort) ---
    if child_cells is None:
        nb_rel = child_path.replace("/Workspace/", "")
        if MIGRATED_BASE.startswith("/Workspace"):
            nb_rel = child_path.replace(MIGRATED_BASE + "/", "")
        if JOB_ROOT and not nb_rel.startswith(JOB_ROOT) and not nb_rel.startswith("Users/"):
            nb_rel = f"{JOB_ROOT.rstrip('/')}/{nb_rel}"
        log.log(f"    [child:{child_path.split('/')[-1]}] Falling back to original API download: {nb_rel}")
        content = await download_notebook_async(nb_rel)
        if content:
            try:
                nb = json.loads(content)
                child_lang = _native_lang_from_nb(nb)
                child_cells = []
                for cell in nb.get("cells", []):
                    if cell.get("cell_type") == "code":
                        src = "".join(cell.get("source", []))
                        if src.strip():
                            child_cells.append(src)
            except:
                return {"status": "error", "error": f"Child notebook parse failed: {child_path}"}
        else:
            return {"status": "error", "error": f"Child notebook not found: {child_path}"}
    child_name = os.path.basename(child_path)
    log.log(f"    [child:{child_name}] {child_path}: {len(child_cells)} code cells")

    # Native Scala/SQL notebook: cells are bare (Databricks omits the %scala/%sql
    # magic on default-language cells). Make the default explicit so the cell is
    # NOT misclassified as Python (which injects a Python aidp_compat bootstrap
    # into a Scala cell → java.lang.RuntimeException). Cells that already carry a
    # %magic (e.g. %python inside a Scala notebook) are left as-is.
    if child_lang in ("scala", "sql"):
        _nmagic = "%scala" if child_lang == "scala" else "%sql"
        _prefixed = 0
        for _i, _c in enumerate(child_cells):
            if _c.strip() and not _c.lstrip().startswith("%"):
                child_cells[_i] = _nmagic + "\n" + _c
                _prefixed += 1
        log.log(f"    [child:{child_name}] native {child_lang} notebook — made {_prefixed} bare cell(s) explicit ({_nmagic})")

    cells_ok = 0
    cells_fixed = 0
    child_cells_failed = 0
    migrated_child_cells = []
    child_cell_outputs = []  # parallel list of outputs for each cell

    _CORRUPTION_MARKERS = [
        "Command ID failed with java.lang.RuntimeException",
        "java.lang.Exception:",
        "java.lang.RuntimeException",
    ]

    child_nb_exit = False
    for ci, child_source in enumerate(child_cells):
        if child_nb_exit:
            break
        stripped = child_source.strip()

        # Detect corrupted cells (error traceback injected as cell source from a prior bad run).
        # Auto-fix to `pass` immediately — saves 9+ Opus rounds per cell.
        if any(m in stripped for m in _CORRUPTION_MARKERS):
            log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: CORRUPTION DETECTED — replacing with pass")
            child_cells[ci] = "pass"
            child_source = "pass"
            stripped = "pass"
            cells_fixed += 1

        # Detect %run or dbutils.notebook.run / oidlUtils.notebook.run - both need inlining
        run_path_to_inline = None

        if stripped.startswith("%run"):
            _run_line = stripped[4:].strip()
            # Handle quoted paths (e.g. %run "/path/with spaces/Utils")
            if _run_line.startswith('"') or _run_line.startswith("'"):
                _q = _run_line[0]
                _end = _run_line.find(_q, 1)
                run_path_to_inline = _run_line[1:_end] if _end > 0 else _run_line[1:].rstrip(_q)
            else:
                run_path_to_inline = _run_line.split()[0]
        else:
            # Check uncommented lines only — commented-out notebook.run() is dead code
            _active = "\n".join(l.strip() for l in stripped.splitlines() if l.strip() and not l.strip().startswith("#"))
            if "dbutils.notebook.run(" in _active or "oidlUtils.notebook.run(" in _active or "notebook.run(" in _active:
                # Extract path from dbutils.notebook.run("path", ...) or oidlUtils.notebook.run("path", ...)
                m = re.search(r'notebook\.run\s*\(\s*["\']([^"\']+)["\']', _active)
                if m:
                    run_path_to_inline = m.group(1)

        if run_path_to_inline:
            run_path_to_inline = re.sub(r'\$\w+', '', run_path_to_inline).strip()
            # AIDP converts spaces to underscores in paths
            run_path_to_inline = run_path_to_inline.replace(" ", "_")
            if not run_path_to_inline.endswith(".ipynb"):
                run_path_to_inline += ".ipynb"
            # Resolve relative paths relative to this notebook's directory.
            # child_path is the *migrated* path under MIGRATED_BASE, which mirrors the original
            # directory structure (via normalize_nb_path), so dirname(child_path) is correct.
            # Databricks treats ALL non-absolute %run paths as relative to the calling notebook:
            #   %run ../foo      -> parent dir
            #   %run ./foo       -> same dir
            #   %run SaveTable   -> same dir (bare name, no / prefix)
            if not run_path_to_inline.startswith("/"):
                child_dir = os.path.dirname(child_path)
                run_path_to_inline = os.path.normpath(os.path.join(child_dir, run_path_to_inline))
            # Look up migrated path (with space/underscore variants)
            resolved = None
            _rp_variants = {run_path_to_inline, run_path_to_inline.replace(" ", "_"), run_path_to_inline.replace("_", " ")}
            for orig, mig in dep_path_map.items():
                if not mig:
                    continue
                for _rpv in _rp_variants:
                    # Exact match or suffix match anchored at directory boundary
                    if orig == _rpv or _rpv.endswith("/" + orig) or orig.endswith("/" + _rpv.lstrip("/")):
                        resolved = mig
                        break
                if resolved:
                    break
            # Also check if the path itself is already under MIGRATED_BASE
            if not resolved and MIGRATED_BASE and run_path_to_inline.startswith(MIGRATED_BASE):
                resolved = run_path_to_inline
            # Fall back: check if a migrated version exists under MIGRATED_BASE.
            # This handles transitive deps (e.g. run.ipynb inside 0_Config.ipynb) that
            # are not in dep_path_map because they weren't direct deps of the task notebook.
            # Prefer the migrated version over the original to avoid reading corrupted originals.
            if not resolved:
                nb_norm = normalize_nb_path(run_path_to_inline)
                migrated_candidate = f"{MIGRATED_BASE}/{nb_norm}"
                # Check both the constructed candidate and the original path on cluster.
                # When run_path_to_inline is already under MIGRATED_BASE (relative %run
                # resolved against child_dir), candidate may equal run_path_to_inline —
                # still need to verify it exists.
                paths_to_try = []
                if migrated_candidate != run_path_to_inline:
                    paths_to_try.append(migrated_candidate)
                paths_to_try.append(run_path_to_inline)
                for candidate in paths_to_try:
                    safe_cand = candidate.replace("'", "\\'")
                    try:
                        exists_result = await session.run_stateless(
                            f"import os; print('yes' if os.path.exists('{safe_cand}') else 'no')",
                            timeout=15,
                        )
                        from context_tools import _unwrap_aidp_text
                        exists_out = _unwrap_aidp_text(format_outputs(exists_result.get("outputs", []))).strip()
                        if exists_out == "yes":
                            resolved = candidate
                            log.log(f"    [child:{child_name}] Resolved transitive dep to: {os.path.basename(candidate)}")
                            break
                    except Exception:
                        pass
            target = resolved or run_path_to_inline
            log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: run -> inlining nested: {os.path.basename(target)}")
            nested = await _inline_child_notebook(
                target, session, log, job_name,
                analysis, catalog_context, dependent_context, dep_path_map)
            if nested.get("status") == "ok":
                cells_ok += 1
                cells_fixed += nested.get("cells_fixed", 0)
            else:
                return {"status": "error", "error": f"Nested child failed: {target}: {nested.get('error', '')}",
                        "cells_ok": cells_ok, "cells_fixed": cells_fixed}
            migrated_child_cells.append(f'%run {target}')
            child_cell_outputs.append([])  # no direct output for inlined cells
            continue

        # Skip ONLY non-executable magics (%md, %sh, %fs, %pip, %conf, ...).
        # Executable language magics (%scala/%sql/%python/%pyspark/%r) MUST run in
        # the kernel so their definitions land in scope for later cells — a %run'd
        # native-Scala util (e.g. Utils.ipynb) defines vals/defs like
        # getDateStrFromEpoch in %scala cells; skipping them (the old behavior here)
        # silently dropped every cell of such a child, so those functions were never
        # defined and dependent cells failed with "not found: value ...".
        _first_magic = (stripped.split() or [""])[0].strip().lower()
        _EXEC_MAGICS = ("%scala", "%sql", "%python", "%pyspark", "%r")
        if (stripped.startswith("%") or stripped.startswith("!")) and _first_magic not in _EXEC_MAGICS:
            log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: skip magic: {stripped[:60]}")
            migrated_child_cells.append(child_source)
            child_cell_outputs.append([])
            cells_ok += 1
            continue

        # Deterministically rename the rename-only Databricks APIs (taskValues,
        # notebook.exit) to their AIDP-native oidlUtils equivalents BEFORE
        # execution. Parent cells get this via CELL_MIGRATE_PROMPT; child cells
        # are execute-first, so without this they keep dbutils.jobs.taskValues —
        # which only resolves via the in-session aidp_compat shim and does NOT
        # propagate across separate workflow tasks at real runtime.
        child_source = _rewrite_dbutils_api_renames(child_source)

        # The child cell is already pass-1 migrated (code-only).
        # Execute it directly. If it fails, THEN call Opus to fix.
        current_code = child_source
        original_code = child_source  # keep original for diff tracking
        fix_log = []  # track all fixes applied to this cell
        code_preview = stripped[:80].replace('\n', ' ')
        log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: exec ({len(stripped)} chars): {code_preview}")

        # Execute + fix loop (up to 5 attempts)
        MAX_CHILD_RETRIES = 5
        cell_ok = False
        _current_cell_notes.clear()
        for attempt in range(MAX_CHILD_RETRIES):
            # Strip any existing fix log comment before execution (don't execute comments)
            exec_code = current_code.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
            # notebook.run inline capture: publish this child's notebook.exit
            # value to spark.conf so the parent can read it (EXEC-only; the saved
            # child keeps notebook.exit).
            if capture_conf_key:
                exec_code = _rewrite_exit_to_capture(exec_code, capture_conf_key)
            # Param bridge (EXEC-only): a child invoked via notebook.run reads its
            # inputs with oidlUtils.parameters.getParameter("argN", ...). The parent
            # already published the notebook.run Map args to
            # spark.conf["spark.aidp.param.argN"] before inlining (see the scala
            # handler's _parse_run_map_args publish). But native getParameter has no
            # workflow-param context in a migration run, so without this rewrite the
            # child reads the default ("") — dropping every inter-notebook arg
            # (e.g. smsDumpPath). Mirror the parent: bridge Scala getParameter →
            # spark.conf.get at exec time. The SAVED child keeps the production-
            # correct oidlUtils form; this is never persisted.
            exec_code = _scala_param_exec_rewrite(exec_code)
            # Write-redirect (tool-only): same protection as the parent cell
            # path. Child cells may write to OCI/tables too — must be
            # redirected so customer data is never touched. Reads use any
            # redirects previously registered by parent or earlier child
            # cells (job-wide map).
            exec_code = _apply_write_redirects(exec_code, source_op_hint=f"child-cell-{ci}")
            exec_code = _inject_write_guard(exec_code)  # variable write dests → tmp
            exec_code = _apply_read_redirects(exec_code)

            try:
                result = await session.execute(exec_code, timeout=14400)
            except Exception as e:
                result = {"status": "error", "outputs": [{"type": "error", "ename": "SessionError", "evalue": str(e)[:200]}]}

            status = result.get("status", "error")
            output = format_outputs(result.get("outputs", []))

            # Cluster-down detection: reconnect WS + re-execute (same as main loop)
            if _CLUSTER_DOWN_RE.search(output or ""):
                log.log(f"    [child:{child_name}] Cell {ci}: AIDP compute cluster down — reconnecting WS...")
                await session.force_reconnect()
                log.log(f"    [child:{child_name}] Cell {ci}: retrying after reconnect...")
                result = await session.execute(exec_code, timeout=14400)
                status = result.get("status", "error")
                output = format_outputs(result.get("outputs", []))

            # Check for notebook.exit() — treat as successful early stop.
            # Only match NotebookExit exception, not function call strings in comments/logs.
            _child_nb_exit = False
            if status != "ok" and output:
                _exit_patterns = ["NotebookExit"]
                if any(p in output for p in _exit_patterns):
                    _child_nb_exit = True
            if _child_nb_exit:
                exit_msg = output.strip().split("\n")[-1][:200]
                log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: notebook.exit() — stopping child: {exit_msg}")
                cells_ok += 1
                _cell_history.append({
                    "index": len(_cell_history), "notebook_path": child_path,
                    "cell_idx": ci, "summary": f"notebook.exit(): {exit_msg}",
                    "final_code": exec_code[:3000], "output_preview": output[:300],
                    "status": "ok", "is_child": True, "last_note": "",
                })
                # Stop executing remaining child cells
                child_nb_exit = True
                break

            # Quick check: did it work?
            has_error = False
            if status != "ok":
                has_error = True
            else:
                error_patterns = ["Traceback", "Exception:", "NameError", "TypeError", "FileNotFoundError"]
                for pat in error_patterns:
                    if pat in output and not any(w in output.split(pat)[0][-100:] for w in ["WARN", "WARNING", "except"]):
                        has_error = True
                        break
                # AIDP surfaces %scala/%sql failures as stream text (status 'ok',
                # no type=='error') wrapped as "Command ID <uuid> failed with ...
                # Failing command: ..." — catch those explicitly.
                if not has_error and ("Failing command:" in output
                        or re.search(r"Command ID [0-9a-fA-F-]+ failed with", output)):
                    has_error = True

            if not has_error:
                cell_ok = True
                if attempt > 0:
                    cells_fixed += 1
                    log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: OK (fixed attempt {attempt})")
                else:
                    log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: OK")
                break

            # Fix with Opus
            if attempt < MAX_CHILD_RETRIES - 1:
                # Log the error with enough detail to debug
                error_preview = output[:200].replace('\n', ' ').replace('\x1b[', '')
                log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: FAIL (attempt {attempt+1}): {error_preview}")
                # Full output so the real exception (past the 200-char preview) is visible.
                for _ln in (output or "").split("\n")[:30]:
                    if _ln.strip():
                        log.log(f"    [child:{child_name}] OUTPUT: {_ln[:300]}")
                try:
                    bucket_ctx = get_bucket_mapping_context()
                    # Include fix history so Opus doesn't revert previous fixes
                    fix_history = ""
                    if fix_log:
                        fix_history = "\n\nPREVIOUS FIXES APPLIED TO THIS CELL (DO NOT REVERT THESE):\n"
                        for fl in fix_log:
                            fix_history += f"- {fl}\n"
                    # TODO: table names are same on AIDP (just default. prefix), full catalog not needed
                    # Full catalog: fix_context = f"{bucket_ctx}\n\n{catalog_context}\n\n..."
                    fix_context = f"{bucket_ctx}\n\n{catalog_context[:3000]}\n\nChild notebook: {child_path}\nThis cell runs in the PARENT notebook's namespace - all parent variables are available.{fix_history}"
                    prev_code = exec_code
                    current_code = await call_fix(exec_code, _compact_output_for_llm(output), [], attempt + 1,
                                                  extra_context=fix_context, session=session, log_fn=log.log,
                                                  notebook_path=child_path)
                    current_code = _fix_path_replace_idempotency(current_code)
                    current_code = _detect_table_to_path_regression(child_source, current_code)
                    current_code = _detect_path_returning_to_identifier_regression(child_source, current_code)
                    # OCI namespace is authoritative from the source/mapping — Opus
                    # must not invent/"correct" it (e.g. <DATALAKE_NAMESPACE> -> <WORKSPACE_NAMESPACE>).
                    current_code = _apply_namespace_from_mapping(current_code)
                    # Record what changed
                    if current_code != prev_code:
                        # Generate a brief diff description
                        diff_lines = []
                        prev_lines = prev_code.split("\n")
                        new_lines = current_code.split("\n")
                        for j, (old, new) in enumerate(zip(prev_lines, new_lines)):
                            if old != new:
                                diff_lines.append(f"L{j+1}: '{old[:60]}' -> '{new[:60]}'")
                        if len(new_lines) != len(prev_lines):
                            diff_lines.append(f"Lines: {len(prev_lines)} -> {len(new_lines)}")
                        fix_desc = f"Attempt {attempt+1}: {'; '.join(diff_lines[:3])}"
                        fix_log.append(fix_desc)
                except Exception as fix_err:
                    log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: fix call failed: {str(fix_err)[:150]}")
                    continue  # retry with same code rather than giving up
            else:
                log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: FAILED after {MAX_CHILD_RETRIES} attempts")
                break

        if cell_ok:
            cells_ok += 1
            # Track any pip install commands in child cell code
            for pkg in extract_pip_packages(current_code):
                _installed_packages.add(pkg)
            # Capture the execution outputs (unwrapped)
            from context_tools import _unwrap_aidp_text
            cell_outputs = []
            seen = set()
            for ro in result.get("outputs", []):
                if ro.get("type") == "stream":
                    text = _unwrap_aidp_text(ro.get("text", ""))
                    if text and text not in seen:
                        seen.add(text)
                        cell_outputs.append({"output_type": "stream", "name": ro.get("name", "stdout"), "text": [text]})
                elif ro.get("type") == "execute_result":
                    data = ro.get("data", {})
                    if "text/plain" in data:
                        data = dict(data)
                        data["text/plain"] = _unwrap_aidp_text(data["text/plain"])
                    cell_outputs.append({"output_type": "execute_result", "data": data, "metadata": {}, "execution_count": None})
            child_cell_outputs.append(cell_outputs)

            if fix_log:
                save_code = current_code.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
                save_code += "\n\n# === AIDP MIGRATION FIX LOG ===\n"
                for fl in fix_log:
                    save_code += f"# {fl}\n"
                # Append notes to fix log
                if _current_cell_notes:
                    for n in _current_cell_notes:
                        save_code += f"# NOTE: {n}\n"
                migrated_child_cells.append(save_code)
                final_child_code = save_code
            else:
                if _current_cell_notes:
                    note_save = current_code.rstrip() + "\n# === AIDP MIGRATION FIX LOG ===\n"
                    for n in _current_cell_notes:
                        note_save += f"# NOTE: {n}\n"
                    migrated_child_cells.append(note_save)
                    final_child_code = note_save
                else:
                    migrated_child_cells.append(child_source)
                    final_child_code = current_code

            # Append to job-wide cell history
            cell_summary_str = await _summarize_cell_code(current_code)
            _cell_history.append({
                "index":          len(_cell_history),
                "notebook_path":  child_path,
                "cell_idx":       ci,
                "summary":        cell_summary_str[:200],
                "final_code":     final_child_code[:3000],
                "output_preview": output[:300] if output else "",
                "status":         "ok",
                "is_child":       True,
                "last_note":      _current_cell_notes[-1] if _current_cell_notes else "",
            })
            _current_cell_notes.clear()
        else:
            # A child cell failed after all retries. Do NOT abort the whole inline, and
            # do NOT comment/alter the cell. We never auto-remove or no-op original code
            # just because it failed: the symbol it defines may be used by a DIFFERENT
            # job/task, and commenting it here (it's the shared migrated copy) would
            # silently break that job. Keep the cell's code UNCHANGED, flag the failure,
            # and KEEP EXECUTING the remaining cells so passing cells' defs still land in
            # the kernel. A failed UNUSED helper (e.g. isS3PathValid) then no longer blocks
            # the cells that DEFINE used symbols (getDateStrFromEpoch, cleanAddUdf, ...);
            # a failed USED symbol surfaces clearly where it is referenced.
            log.log(f"    [child:{child_name}] Cell {ci}/{len(child_cells)}: FAILED — keeping code UNCHANGED (not commented) and CONTINUING to remaining cells")
            migrated_child_cells.append(child_source)
            child_cell_outputs.append([])
            child_cells_failed += 1
            cell_summary_str = await _summarize_cell_code(current_code)
            _cell_history.append({
                "index":          len(_cell_history),
                "notebook_path":  child_path,
                "cell_idx":       ci,
                "summary":        cell_summary_str[:200],
                "final_code":     child_source[:3000],
                "output_preview": output[:300] if output else "",
                "status":         "error",
                "is_child":       True,
                "last_note":      _current_cell_notes[-1] if _current_cell_notes else "",
            })
            _current_cell_notes.clear()

    if child_cells_failed:
        log.log(f"    [child:{child_name}] {child_cells_failed} cell(s) failed; left UNCHANGED in the migrated child and inline CONTINUED so passing cells' defs are available in the kernel")

    # Always cache the child cells in memory so subsequent %run hits Source 1.
    # Without this, FUSE misses cause API re-download of the original unmigrated notebook.
    if child_path not in _notebook_content_cache:
        cache_nb = {"cells": []}
        for _mc in migrated_child_cells:
            clean = _mc.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
            cache_nb["cells"].append({"cell_type": "code", "source": [clean]})
        _notebook_content_cache[child_path] = json.dumps(cache_nb)
        if not child_path.endswith(".ipynb"):
            _notebook_content_cache[child_path + ".ipynb"] = _notebook_content_cache[child_path]

    # Save the migrated child notebook back to AIDP (only if we fixed something)
    if cells_fixed == 0:
        log.log(f"    [child:{child_name}] No fixes needed, keeping existing file")
        return {"status": "ok", "cells_ok": cells_ok, "cells_fixed": cells_fixed}

    # Safety: never write back to original workspace notebooks — only to migrated copies
    if not child_path.startswith(MIGRATED_BASE):
        log.log(f"    [child:{child_name}] WARNING: child_path not under MIGRATED_BASE, skipping save-back to protect original: {child_path}")
        return {"status": "ok", "cells_ok": cells_ok, "cells_fixed": cells_fixed}

    # Tripwire: confirm the on-disk file's non-config code-cell count matches
    # migrated_child_cells before save-back. A mismatch means the file has
    # drifted (e.g. previously mangled by the config-cell off-by-one bug, or
    # extra cells crept in). Saving anyway would propagate the corruption.
    # Skip save-back, surface the drift, and require manual file deletion +
    # re-run to regenerate the dep cleanly.
    # json.dumps yields a fully-escaped Python string literal (handles quotes,
    # backslashes, newlines) — child_path can now come from manifest-supplied
    # values, so a bare .replace("'", "\\'") is insufficient.
    safe_path_check = json.dumps(child_path)
    try:
        check_result = await session.execute(f"""
import json
try:
    with open({safe_path_check}) as f:
        _nb = json.load(f)
    _CFG_MARKER = '# AIDP performance configuration'
    _count = 0
    for c in _nb.get('cells', []):
        if c.get('cell_type') != 'code':
            continue
        _src = ''.join(c.get('source', []))
        if not _src.strip():
            continue
        if _CFG_MARKER in _src:
            continue
        _count += 1
    print(_count)
except Exception as _e:
    print('ERR:' + str(_e)[:200])
""", timeout=30)
        check_out = _unwrap_aidp_text(format_outputs(check_result.get("outputs", []))).strip()
        on_disk_count = int(check_out) if check_out.isdigit() else -1
    except Exception:
        on_disk_count = -1

    if on_disk_count >= 0 and on_disk_count != len(migrated_child_cells):
        log.log(
            f"    [child:{child_name}] TRIPWIRE: on-disk has {on_disk_count} real code cells "
            f"but migrated_child_cells has {len(migrated_child_cells)} — drift detected, "
            f"skipping save-back to avoid corrupting the file. "
            f"Delete {child_path} and re-run to regenerate it cleanly."
        )
        return {
            "status": "error",
            "error": (f"Cell-count drift for {child_path}: "
                      f"on-disk={on_disk_count}, migrated={len(migrated_child_cells)}. "
                      f"Delete the file and re-run."),
            "cells_ok": cells_ok,
            "cells_fixed": cells_fixed,
        }

    # Rewrite ALL code cells in the notebook — not just diffs.
    # The file on disk may have an AIDP_SPARK_CONFIG_CELL prepended by dep
    # migration, which shifts code_idx by 1.  If child_cells were loaded from
    # the original (Source 4, no config cell), a diff-only overlay would apply
    # fixes to the wrong cells (off-by-one).  Sending ALL cells and skipping
    # the config cell on the cluster side eliminates index alignment issues.
    try:
        safe_path = child_path.replace("'", "\\'")
        all_cells_map = {}
        outputs_map = {}
        for i, code in enumerate(migrated_child_cells):
            clean_code = code.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
            all_cells_map[str(i)] = clean_code
            if i < len(child_cell_outputs):
                outputs_map[str(i)] = child_cell_outputs[i]

        payload = json.dumps({"cells": all_cells_map, "outputs": outputs_map})
        payload_b64 = base64.b64encode(payload.encode("utf-8")).decode("ascii")

        await session.execute(f"""
import json, base64, builtins, os

path = '{safe_path}'
payload = json.loads(base64.b64decode('{payload_b64}').decode('utf-8'))
all_cells = payload.get('cells', {{}})
outputs = payload.get('outputs', {{}})

with open(path) as f:
    nb = json.load(f)

# Detect config cell offset: dep migration prepends AIDP_SPARK_CONFIG_CELL
# which shifts all code_idx by 1. child_cells don't include it.
CONFIG_MARKER = '# AIDP performance configuration'
config_offset = 0
for cell in nb.get('cells', []):
    if cell.get('cell_type') == 'code':
        src = ''.join(cell.get('source', []))
        if src.strip() and CONFIG_MARKER in src:
            config_offset = 1
        break  # only check first code cell

code_idx = 0
replaced = 0
for cell in nb.get('cells', []):
    if cell.get('cell_type') != 'code':
        continue
    src = ''.join(cell.get('source', []))
    if not src.strip():
        continue
    child_idx = str(code_idx - config_offset)
    if child_idx in all_cells:
        cell['source'] = [all_cells[child_idx]]
        replaced += 1
    if child_idx in outputs:
        cell['outputs'] = outputs[child_idx]
    code_idx += 1

os.makedirs(os.path.dirname(path), exist_ok=True)
with builtins.open(path, 'w') as f:
    json.dump(nb, f, indent=1)
print(f'Replaced {{replaced}}/{{code_idx}} cells (config_offset={{config_offset}})')
""", timeout=60)
        log.log(f"    [child:{child_name}] Saved {cells_fixed} fixed cells back to: {child_path}")
    except Exception as e:
        log.log(f"    [child:{child_name}] WARNING: Failed to save child: {e}")

    result = {"status": "ok", "cells_ok": cells_ok, "cells_fixed": cells_fixed}
    if child_nb_exit:
        # Extract exit value from the last output (notebook.exit("value") → "value")
        last_hist = _cell_history[-1] if _cell_history else {}
        exit_output = last_hist.get("output_preview", "")
        # Try to extract the value from NotebookExit("value") or similar
        import re as _re
        _exit_val_match = _re.search(r'NotebookExit[:\s]*(.+)', exit_output)
        result["exit_value"] = _exit_val_match.group(1).strip().strip("'\"") if _exit_val_match else ""

    # NOTE: Previously called _install_writer_interceptors() here to wrap
    # customer wrapper functions (createTable, drop_table, etc.) in the
    # kernel namespace at runtime. That approach was REPLACED with the
    # cell-text AST rewrite in _apply_wrapper_call_redirect() — the
    # interceptor's session-state dependency caused data-safety issues when
    # the AIDP kernel session died mid-task (writes escaped redirect because
    # Opus inline-defined the wrapper functions without our wrapping).
    # The cell-text rewrite has no kernel-state dependency; it's applied
    # to every cell's exec_code before send. The runtime install functions
    # are kept in the file as dormant code in case of future revival.

    return result


# ─── Upload Verification ─────────────────────────────────────────────

async def verify_upload(session, remote_path, retries=2, log_fn=None):
    """Verify a file was persisted on AIDP FUSE. Retry with sleep if missing.
    Returns True if file exists and has size > 0, False otherwise."""
    from context_tools import _unwrap_aidp_text
    for attempt in range(retries + 1):
        try:
            check = await session.execute(f"""
import os
p = '{remote_path}'
if os.path.exists(p):
    print(os.path.getsize(p))
else:
    print('MISSING')
""", timeout=15)
            out = _unwrap_aidp_text(format_outputs(check.get("outputs", [])))
            if "MISSING" not in out:
                # Defense-in-depth: pull the digits out of the first line rather
                # than int() the whole thing — guards against any residual AIDP
                # output-wrapper noise (e.g. a leading "[]") leaking through.
                _m = re.search(r"\d+", out.split('\n')[0])
                size = int(_m.group()) if _m else 0
                if size > 0:
                    return True
                if log_fn:
                    log_fn(f"  [verify] {os.path.basename(remote_path)} exists but size=0 (attempt {attempt+1})")
        except Exception as e:
            if log_fn:
                log_fn(f"  [verify] {os.path.basename(remote_path)} check error: {str(e)[:100]} (attempt {attempt+1})")

        if attempt < retries:
            if log_fn:
                log_fn(f"  [verify] {os.path.basename(remote_path)} not persisted (attempt {attempt+1}), retrying after 5s...")
            await asyncio.sleep(5)
    if log_fn:
        log_fn(f"  [verify] WARNING: {remote_path} failed verification after {retries+1} attempts")
    return False


# ─── Notebook Processing ─────────────────────────────────────────────

async def process_notebook(
    notebook_path: str,
    session: AIDPSession,
    job_name: str,
    task_key: str,
    parameters: Dict[str, str],
    run_all: bool = True,
    session_pool=None,  # for reconnect on systemic errors
    dep_path_map: Dict[str, str] = None,  # original_path -> migrated_path for %run deps
    acceptance_contract_dict: Optional[Dict[str, Any]] = None,  # optional contract from manifest
) -> dict:
    """Cell-by-cell: download -> analyze whole notebook -> migrate each cell -> execute -> fix.

    If `acceptance_contract_dict` is provided AND all cells passed (overall == PASS),
    runs the contract's pending_count_sql with consecutive-zero-window logic before
    finalizing the PASS verdict. See scripts/acceptance_contract.py.
    """

    # Task output goes in tasks/ subdirectory (dep outputs go in notebooks/)
    if task_key.startswith("dep_"):
        output_dir = f"{OUTPUT_BASE}/{job_name}/deps/{task_key}"
    else:
        output_dir = f"{OUTPUT_BASE}/{job_name}/tasks/{task_key}"
    log = NotebookLog(notebook_path, job_name, task_key)
    log.log(f"START: {notebook_path}")

    tmpdir = tempfile.mkdtemp(prefix="aidp_job_")
    total_tokens = 0
    _auto_installed_modules = set()  # track modules auto-installed to avoid retry loops

    try:
        # ── Download ──
        content = download_notebook(notebook_path)
        if not content:
            log.log("DOWNLOAD FAILED")
            return {"path": notebook_path, "task": task_key, "status": "DOWNLOAD_FAILED"}

        local_nb = os.path.join(tmpdir, "original.ipynb")
        with open(local_nb, 'wb') as f:
            f.write(content)

        nb, original_outputs, readable = parse_notebook(local_nb)
        cells = nb.get("cells", [])
        total_cells = len(cells)
        code_cells = sum(1 for c in cells if c.get("cell_type") == "code" and "".join(c.get("source", [])).strip())
        markdown_cells = sum(1 for c in cells if c.get("cell_type") == "markdown")
        raw_cells = sum(1 for c in cells if c.get("cell_type") == "raw")
        empty_code = total_cells - code_cells - markdown_cells - raw_cells
        log.log(f"Downloaded: {total_cells} cells ({code_cells} code, {markdown_cells} markdown, {raw_cells} raw, {empty_code} empty)")

        # ── Detect native Scala/SQL/R notebooks — AIDP supports Scala & SQL natively, no conversion needed ──
        _first_code = ""
        for _c in cells:
            if _c.get("cell_type") == "code":
                _first_code = "".join(_c.get("source", [])).strip()
                break
        # Check notebook metadata (kernelspec / language_info)
        _nb_meta = nb.get("metadata", {})
        _kernel_lang = _nb_meta.get("kernelspec", {}).get("language", "").lower()
        _lang_info = _nb_meta.get("language_info", {}).get("name", "").lower()
        # Check first cell marker: // = Scala, -- = SQL
        _marker_scala = _first_code.startswith("// Databricks notebook source")
        _marker_sql = _first_code.startswith("-- Databricks notebook source")
        is_native_scala = _marker_scala or _kernel_lang == "scala" or _lang_info == "scala"
        is_native_sql = _marker_sql or _kernel_lang == "sql" or _lang_info == "sql"
        if is_native_scala or is_native_sql:
            _native_lang = "Scala" if is_native_scala else "SQL"
            if is_native_scala:
                log.log(f"Native Scala notebook detected — cells migrated AS Scala via Opus (never ported to Python)")
            else:
                log.log(f"Native SQL notebook detected — cells preserved as-is (no Opus migration)")
            # Make the implicit default language explicit: a native Scala/SQL
            # notebook has BARE cells (Databricks omits the %scala/%sql magic on
            # default-language cells). Without the magic, the Python kernel runs
            # them as Python (e.g. `import java.time...` -> ModuleNotFoundError:
            # 'java'). Prefix bare code cells with %scala/%sql so they execute in
            # the right language. Cells already carrying a %magic are left as-is.
            _nmagic = "%scala" if is_native_scala else "%sql"
            _np = 0
            for _c in cells:
                if _c.get("cell_type") != "code":
                    continue
                _src = "".join(_c.get("source", [])) if isinstance(_c.get("source"), list) else (_c.get("source") or "")
                if _src.strip() and not _src.lstrip().startswith("%"):
                    # Keep nbformat's List[str] shape (not a bare str) so the
                    # AIDP fingerprint comparison stays consistent.
                    _c["source"] = (_nmagic + "\n" + _src).splitlines(keepends=True)
                    _np += 1
            if _np:
                log.log(f"Native {_native_lang}: made {_np} bare cell(s) explicit ({_nmagic})")

        # ── Step 0: Gather context for Opus ──
        catalog_context = load_catalog_snapshot()
        # Pass the parsed notebook (not the readable blob) so dependency scanning
        # is per-cell language-aware (strips the correct comment syntax per cell).
        deps = extract_notebook_dependencies(nb)
        downloaded_deps = {}
        # Resolve relative %run paths against the parent task notebook's
        # directory before downloading (mirrors ensure_migrated/process_job).
        # Without this, raw "./X" tokens reach download_notebook unchanged
        # and produce spurious 404s in logs.
        # Use parent's directory regardless of whether notebook_path is
        # absolute (/Workspace/...) or workspace-relative (GIT/...). dirname()
        # returns "" for a bare filename — in that case the fix simply doesn't
        # fire and behavior matches the original.
        _parent_dir = os.path.dirname(notebook_path)
        for dep in deps:
            # Treat as relative ONLY when it actually looks relative.
            # `Workspace/X` (no leading slash but absolute-looking) is treated
            # as absolute — matches the original code's behavior.
            _is_relative = dep.startswith("./") or dep.startswith("../") or (
                not dep.startswith("/") and not dep.startswith("Workspace/")
            )
            if _is_relative and _parent_dir:
                dep_abs = os.path.normpath(os.path.join(_parent_dir, dep))
            else:
                dep_abs = dep
            if not dep_abs.endswith(".ipynb"):
                dep_abs += ".ipynb"
            # Manifest-driven override: swap when naive resolution misses
            dep_abs = _resolve_relative_dep(notebook_path, dep_abs)
            # Strip /Workspace/ for the AIDP downloadFileMeta API
            dep_normalized = dep_abs.lstrip("/")
            if dep_normalized.startswith("Workspace/"):
                dep_normalized = dep_normalized[len("Workspace/"):]
            dep_content = download_notebook(dep_normalized)
            if dep_content:
                downloaded_deps[dep_normalized] = dep_content
        dependent_context = get_dependent_notebook_context(content.decode('utf-8', errors='replace'), downloaded_deps)
        log.log(f"Context: {len(deps)} deps loaded, {len(downloaded_deps)} found, catalog={len(catalog_context)} chars")

        # ── Step 1: Cell-by-cell analysis (static + cluster verification) ──
        bucket_mapping_ctx = get_bucket_mapping_context()
        from cell_analyzer import analyze_notebook_cells, render_cell_plan
        from cell_context import CellContext
        cell_context = CellContext()
        if DIRECT_EXECUTE:
            # Skip AI analysis — execute cells as-is (useful for WS resilience testing)
            cell_plans = [
                {"cell_index": i, "action": "execute", "description": "", "risks": [],
                 "changes_needed": [], "storage_paths": [], "table_refs": [], "fuse_risks": []}
                for i in range(len(cells))
            ]
            analysis = ""
            log.log("Analysis skipped (--direct-execute mode)")
        else:
            log.log("Analyzing cells...")
            cell_plans = await analyze_notebook_cells(
                cells, session, cell_context, catalog_context, bucket_mapping_ctx, log_fn=log.log)
            analysis = "\n".join(render_cell_plan(p) for p in cell_plans if p["action"] != "skip_empty")
            log.log(f"Analysis complete: {len(cell_plans)} cell plans")

        # ── Step 1b: Install missing Python packages upfront ──
        local_module_src_roots = []
        local_mirror_root_orig = ""
        local_mirror_root_dst = ""
        if run_all and not DIRECT_EXECUTE:
            all_imports = set()
            for plan in cell_plans:
                all_imports.update(plan.get("dependencies", []))
            if all_imports:
                try:
                    from cluster_lifecycle import ensure_requirements_installed
                    job_output_path = f"{OUTPUT_BASE}/{job_name}"
                    install_result = await ensure_requirements_installed(
                        session.cluster_id, session, list(all_imports),
                        job_output_path, timeout=600,
                        notebook_paths=[notebook_path],
                        migrated_base=MIGRATED_BASE)
                    installed = install_result.get("installed", [])
                    local_module_src_roots = install_result.get("src_roots", [])
                    local_mirror_root_orig = install_result.get("mirror_root_orig", "")
                    local_mirror_root_dst = install_result.get("mirror_root_dst", "")
                    if installed:
                        log.log(f"Installed {len(installed)} missing packages: {', '.join(installed)}")
                    if local_module_src_roots:
                        log.log(f"Local-module src roots (will be added to sys.path): {', '.join(local_module_src_roots)}")
                    if local_mirror_root_dst:
                        log.log(f"Local source MIRRORED: {local_mirror_root_orig} -> {local_mirror_root_dst}")
                except Exception as e:
                    log.log(f"Package install warning: {e}")

        # ── Step 2: Inject parameters + bootstrap (only if executing) ──
        if run_all:
            if parameters:
                params_code = f"import os, json; os.environ['AIDP_PARAMS'] = {repr(json.dumps(parameters))}"
                await session.execute(params_code, timeout=30)
            notebook_dir = os.path.dirname(notebook_path)
            # Local in-tree modules: prepend their src roots to sys.path so
            # `import system_config` / `from modules.config import …` resolve
            # without Opus having to add them manually each retry.
            local_syspath_block = ""
            if local_module_src_roots:
                local_syspath_block = (
                    "import sys as _sys\n"
                    + "".join(
                        f"if {root!r} not in _sys.path: _sys.path.insert(0, {root!r})\n"
                        for root in local_module_src_roots
                    )
                )
            bootstrap = (
                "from aidp_compat import dbutils, displayHTML, sql, translate_path, set_notebook_dir\n"
                # Set the notebook dir so dbutils.notebook.run("../relative") resolves correctly
                + f"set_notebook_dir({notebook_dir!r})\n"
                + local_syspath_block
                # (AIDP perf-config injection removed — we no longer set any
                # spark.conf during migration or in the artifact, per request.)
                # Object Storage CircuitBreaker + retry hardening — runtime-only.
                # NOT prepended to the migrated artifact (operator decides per-deploy
                # what concurrency profile to apply via AIDP_WAVE_SIZE env var).
                + AIDP_THROTTLE_HARDENING_CELL
            )
            await session.execute(bootstrap, timeout=30)
            # ── Write per-task manifest_params.json on the cluster ─────
            # The `oidlUtils` global is already wrapped by the cluster-session
            # bootstrap snippet (in job_migrate_from_workflow.py). That wrapper
            # reads from `manifest_params.json` on each `getParameter` call.
            # Here we write the current task's manifest parameters so the
            # wrapper picks them up. The wrapper survives cluster-session
            # recycles because the snippet is replayed on every fresh kernel,
            # and the file on /Workspace persists across kernels.
            #
            # The verification probe runs through the wrapper and prints the
            # verdict to the migration log so we can detect silent breakage.
            if parameters:
                mp_file = f"{OUTPUT_BASE}/{job_name}/manifest_params.json"
                write_mp_code = f"""
import json, os
_MP_FILE = {mp_file!r}
_params = {parameters!r}
os.makedirs(os.path.dirname(_MP_FILE), exist_ok=True)
with open(_MP_FILE, 'w') as _f:
    json.dump(_params, _f)
# VALIDATION-ONLY: publish params to spark.conf so %scala cells (native
# oidlUtils.parameters has no workflow-param context in an interactive run)
# can read them via spark.conf.get at exec time. The saved notebook is
# unchanged; this is a migration-time convenience, not a code change.
try:
    for _k, _v in _params.items():
        spark.conf.set('spark.aidp.param.' + str(_k), '' if _v is None else str(_v))
except Exception as _e:
    print('[AIDP-BRIDGE] spark.conf publish warning:', str(_e)[:120])
# Verification probe — confirm the wrapper picks up the new params
_probe_key = next(iter(_params)) if _params else None
if _probe_key:
    _probe = oidlUtils.parameters.getParameter(_probe_key, '__SENTINEL_NOT_BRIDGED__')
    _expected = _params.get(_probe_key)
    _wrapper_type = type(oidlUtils).__name__
    if _probe == _expected:
        print(f'[AIDP-BRIDGE] OK (manifest_params written, {{len(_params)}} params, wrapper={{_wrapper_type}}, probe-match on {{_probe_key!r}})')
    elif _probe == '__SENTINEL_NOT_BRIDGED__':
        print(f'[AIDP-BRIDGE] FAIL (wrapper={{_wrapper_type}}, probe sentinel returned — bootstrap snippet did not install wrapper)')
    else:
        print(f'[AIDP-BRIDGE] FAIL (wrapper={{_wrapper_type}}, probe returned unexpected: {{_probe!r}})')
else:
    print(f'[AIDP-BRIDGE] noop (no params)')
"""
                _r = await session.execute(write_mp_code, timeout=30)
                _bridge_stdout = format_outputs(_r.get("outputs", [])).strip()
                for _line in _bridge_stdout.splitlines():
                    if "[AIDP-BRIDGE]" in _line:
                        log.log(_line)
                        break
                else:
                    log.log(f"[AIDP-BRIDGE] WARNING: no verdict in manifest-params write: {_bridge_stdout[:200]!r}")
            # Runtime-only toPandas() safety — prevents driver OOM during execution.
            # NOT saved in the migrated notebook.
            await session.execute(AIDP_TOPANDAS_SAFETY, timeout=30)
            # Brief pause — AIDP kernel can stall on rapid back-to-back requests
            await asyncio.sleep(2)

        # ── Step 3: Cell-by-cell migrate (+ execute + fix if run_all) ──
        mode = "migrate+execute" if run_all else "migrate-only"
        log.log(f"Processing {total_cells} cells ({code_cells} code to {mode})...")
        cell_results = []
        executed_code = []
        migrated_cells = []
        cells_ok = 0
        cells_failed = 0
        skip_markdown = 0
        skip_raw = 0
        skip_empty = 0
        cells_fixed = 0
        consecutive_failures = 0
        monitor_decisions = []
        job_should_stop = False
        migration_notes = []
        # Data-recovery state (per-task):
        #   cells_data_substituted — passed via OK_DATA_SUBSTITUTED path
        #   cells_data_unavailable — failed after recovery exhausted dates
        #   cells_not_validated    — migrated code-only after task_validation_stop
        #   task_validation_stop   — once True, remaining cells skip execution
        cells_data_substituted = 0
        cells_data_unavailable = 0
        cells_not_validated = 0
        task_validation_stop = False

        # Track whether an unconditional notebook.exit() has been seen in a prior cell.
        # If yes, all subsequent cells are unreachable — copy them as-is, skip analysis/migration/execution.
        _seen_unconditional_exit = False

        for i, cell in enumerate(cells):
            cell_type = cell.get("cell_type", "unknown")
            source = "".join(cell.get("source", []))

            # ── Skip cells after an unconditional notebook.exit() ──
            # Either the analyzer flagged it (action=skip_after_exit) OR a prior cell triggered the flag.
            _plan_for_cell = cell_plans[i] if i < len(cell_plans) else {}
            if _seen_unconditional_exit or _plan_for_cell.get("action") == "skip_after_exit":
                migrated_cells.append(cell)
                cell_results.append({"cell": i, "status": "skipped", "reason": "unreachable — after unconditional notebook.exit()"})
                log.log(f"Cell {i}/{total_cells}: SKIPPED (unreachable after unconditional notebook.exit)")
                continue

            # Programmatic pre-processing before Opus (pandas S3 reads, internal path updates).
            # Scala cells get a SAFE subset (only path normalization in notebook.run calls).
            # Python cells get the full transformation pipeline.
            _cell_is_scala = source.lstrip().startswith("%scala")
            if cell_type == "code" and source.strip():
                if _cell_is_scala:
                    source = _preprocess_scala_cell_source(source, dep_path_map)
                else:
                    source = _preprocess_cell_source(source, dep_path_map)
                cell["source"] = [source]

            # ── Detect unconditional exit in THIS cell (set flag for next iterations) ──
            # Unconditional = top-level (zero indentation) notebook.exit() — not inside if/for/try.
            if cell_type == "code" and source.strip() and not _cell_is_scala:
                for _ln in source.splitlines():
                    _stripped = _ln.lstrip()
                    if _stripped.startswith("#"):
                        continue
                    if (_stripped.startswith("oidlUtils.notebook.exit(") or
                            _stripped.startswith("dbutils.notebook.exit(")):
                        if len(_ln) - len(_stripped) == 0:
                            _seen_unconditional_exit = True
                            break

            # Skip non-code cells with specific reason
            if cell_type == "markdown":
                migrated_cells.append(cell)
                cell_results.append({"cell": i, "status": "skipped", "reason": "markdown"})
                skip_markdown += 1
                continue
            elif cell_type == "raw":
                migrated_cells.append(cell)
                cell_results.append({"cell": i, "status": "skipped", "reason": "raw"})
                skip_raw += 1
                continue
            elif cell_type != "code" or not source.strip():
                migrated_cells.append(cell)
                cell_results.append({"cell": i, "status": "skipped", "reason": "empty"})
                skip_empty += 1
                continue

            # ── Gather cell-level context (plan + accumulated context) ──
            cell_plan = cell_plans[i] if i < len(cell_plans) else {}
            cell_plan_text = render_cell_plan(cell_plan) if cell_plan else ""
            accumulated_ctx = cell_context.render() if cell_context else ""
            # Local-module mirror context — included in every cell migrate / fix
            # so Opus only writes to the mirror copy, never the customer source.
            mirror_ctx = ""
            if local_mirror_root_dst:
                mirror_ctx = (
                    "\n=== LOCAL MODULE SOURCE MIRROR ===\n"
                    f"the source tree (READ-ONLY, NEVER MODIFY): {local_mirror_root_orig}\n"
                    f"Mirror copy (writable, ALL patches go here): {local_mirror_root_dst}\n"
                    "Rules:\n"
                    f"- sys.path is already configured to load local modules from {local_mirror_root_dst}.\n"
                    "- If you need to patch a local .py file (e.g. fix a syntax error, comment out\n"
                    "  Databricks-only code), rewrite the path so it points at the MIRROR copy.\n"
                    f"  Example: '{local_mirror_root_orig}/modules/utils/foo.py'\n"
                    f"       →  '{local_mirror_root_dst}/modules/utils/foo.py'\n"
                    f"- NEVER write to any path that starts with {local_mirror_root_orig}.\n"
                )
            # TODO: table names are same on AIDP (just default. prefix for 3-part names), full catalog not needed
            # Full catalog: cell_migrate_context = f"{cell_plan_text}\n\n{accumulated_ctx}\n\n{bucket_mapping_ctx}\n\n{catalog_context}"
            cell_migrate_context = f"{cell_plan_text}\n\n{accumulated_ctx}\n\n{bucket_mapping_ctx}\n\n{catalog_context[:2000]}{mirror_ctx}"

            # ── Check if this is a Slack/notification/external URL cell — preserve as Raw ──
            stripped_source = source.strip()
            _slack_markers = [
                "slack", "webhook", "send_slack", "slack_alert",
                "hooks.slack.com", "internal-host.example", "internal-gateway.example",
                # Slack SDK API method/import markers — catches `client.chat_postMessage(...)`,
                # `WebClient(...)`, `from slack_sdk import ...`, etc.
                "chat_postmessage", "slack_sdk", "webhookclient", "slackclient",
            ]
            source_lower = stripped_source.lower()
            _is_notification = any(m in source_lower for m in _slack_markers)
            # Slack ID syntax heuristic: channel IDs (C[A-Z0-9]{8,} passed as
            # channel=) or user mentions (@U[A-Z0-9]{8,}) are extremely
            # Slack-specific. Catches cells that import Slack via aliased names
            # we wouldn't otherwise match. Case-sensitive (Slack IDs are upper).
            if not _is_notification and re.search(
                    r"channel\s*=\s*['\"]C[A-Z0-9]{8,}|@U[A-Z0-9]{8,}",
                    stripped_source):
                _is_notification = True
            if _is_notification:
                raw_cell = copy.deepcopy(cell)
                raw_cell["cell_type"] = "raw"
                raw_cell["outputs"] = []
                migrated_cells.append(raw_cell)
                cells_ok += 1
                cell_results.append({"cell": i, "status": "ok", "code": source[:120], "note": "slack/notification cell -> raw"})
                log.log(f"Cell {i}/{total_cells}: OK (Slack/notification — preserved as Raw)")
                continue

            # ── Check if this is a %scala or %sql cell — keep as-is, AIDP supports both ──
            # Databricks notebooks use single-% magic (%scala, %sql)
            is_scala = stripped_source.startswith("%scala")
            is_sql = stripped_source.startswith("%sql")
            if is_sql:
                # ── Native SQL: deterministic transforms only, execute as-is ──
                # SQL cells are overwhelmingly table reads (%sql SELECT ... FROM
                # table), which run on AIDP unchanged. No Opus. Retry up to 2x
                # for transient failures (WS drops, kernel restarts).
                source = _apply_s3_translations(source)
                source = _rewrite_internal_paths(source, MIGRATED_BASE, notebook_path, dep_path_map)
                cell["source"] = [source]
                for _sql_attempt in range(2):
                    try:
                        result = await session.execute(source, timeout=14400)
                        out = format_outputs(result.get("outputs", []))
                        # AIDP surfaces %sql failures as stream text (status 'ok',
                        # no type=='error'), wrapped as "Command ID <uuid> failed
                        # with ... Failing command: ..." — scan output too.
                        has_error = bool(result.get("outputs")) and any(
                            o.get("type") == "error" for o in result.get("outputs", []))
                        if not has_error and out and (
                                "Failing command:" in out
                                or re.search(r"Command ID [0-9a-fA-F-]+ failed with", out)):
                            has_error = True
                        if not has_error and result.get("status") not in (None, "ok"):
                            has_error = True
                        if has_error:
                            if _sql_attempt < 1:
                                log.log(f"Cell {i}/{total_cells}: %sql error, retrying...")
                                await asyncio.sleep(2)
                                continue
                            log.log(f"Cell {i}/{total_cells}: FAIL (%sql — execution error)")
                            log.log(f"  Output: {out[:300]}")
                            cells_failed += 1
                            cell_results.append({"cell": i, "status": "fail", "code": source[:120], "note": "%sql execution error"})
                        else:
                            cells_ok += 1
                            cell_results.append({"cell": i, "status": "ok", "code": source[:120], "note": "%sql cell executed"})
                            log.log(f"Cell {i}/{total_cells}: OK (%sql — executed as-is{(' retry ' + str(_sql_attempt)) if _sql_attempt else ''})")
                        break
                    except Exception as e:
                        if _sql_attempt < 1:
                            log.log(f"Cell {i}/{total_cells}: %sql exception, retrying ({str(e)[:80]})")
                            await asyncio.sleep(5)
                            continue
                        log.log(f"Cell {i}/{total_cells}: FAIL (%sql — {str(e)[:100]})")
                        cells_failed += 1
                        cell_results.append({"cell": i, "status": "fail", "code": source[:120], "note": f"%sql error: {str(e)[:80]}"})
                        break
                migrated_cells.append(cell)
                continue

            if is_scala:
                # ── Native Scala: EXECUTE-FIRST, Opus (call_fix) only on error ──
                # Mirror the Python execute-verify-fix philosophy: apply the
                # deterministic transforms, run the cell AS-IS, and only hand it
                # to Opus when it actually errors. Do NOT migrate working Scala
                # up-front — that over-migrates cells that already run on AIDP
                # (the exact problem execute-first was introduced to avoid).
                # call_fix is Scala-aware: it fixes AS Scala and NEVER ports to
                # Python. The deterministic regex still handles literal s3a://.
                source = _apply_s3_translations(source)
                source = _rewrite_internal_paths(source, MIGRATED_BASE, notebook_path, dep_path_map)
                if not source.lstrip().startswith("%scala"):
                    source = "%scala\n" + source
                cell["source"] = [source]

                if not run_all:
                    # Code-only (dep) mode: deterministic transforms only — no
                    # execution and no Opus. The dep is executed + fixed when it
                    # is inlined into a task notebook (avoids double-execution).
                    cells_ok += 1
                    cell_results.append({"cell": i, "status": "migrated", "code": source[:2000], "note": "%scala deterministic transforms (code-only)"})
                    log.log(f"Cell {i}/{total_cells}: MIGRATED (%scala, code-only, no execution)")
                    migrated_cells.append(cell)
                    continue

                # ── notebook.run INLINE (treat like %run) ──────────────────
                # For each (oidlUtils|dbutils).notebook.run(...) call, inline the
                # child the same way %run does: set the child's Map params in the
                # kernel, execute+Opus-fix the child's cells AND save the fixed
                # child (via _inline_child_notebook), capturing the child's
                # notebook.exit value into spark.conf. Build an EXEC substitution
                # call -> spark.conf.get(<key>); the SAVED cell keeps notebook.run.
                # Scan UNCOMMENTED source only — a commented-out notebook.run
                # (// or # or -- dead code) must NOT be inlined. The full-call
                # text still matches in the active source for the exec swap.
                _active_src = "\n".join(
                    _l for _l in source.splitlines()
                    if not _l.lstrip().startswith(("//", "#", "--"))
                )
                _nbrun_subs = {}
                for _ci, _call in enumerate(_find_notebook_run_calls(_active_src)):
                    if not _call.get("path"):
                        continue
                    _rawp = _call["path"]
                    if not _rawp.endswith(".ipynb"):
                        _rawp += ".ipynb"
                    if not _rawp.startswith("/"):
                        _rawp = os.path.normpath(os.path.join(os.path.dirname(notebook_path), _rawp))
                    _child = None
                    if dep_path_map:
                        _vars = {_rawp, _rawp.replace(" ", "_"), _rawp.replace("_", " ")}
                        for _o, _mig in dep_path_map.items():
                            if _mig and any(_o == v or v.endswith("/" + _o) or _o.endswith("/" + v.lstrip("/")) for v in _vars):
                                _child = _mig
                                break
                    if not _child:
                        # Guard against double-MIGRATED_BASE: the path may already
                        # be migrated (e.g. _rewrite_internal_paths rewrote it).
                        if MIGRATED_BASE and _rawp.startswith(MIGRATED_BASE):
                            _child = _rawp
                        else:
                            _nn = normalize_nb_path(_rawp)
                            if not _nn.endswith(".ipynb"):
                                _nn += ".ipynb"
                            _child = f"{MIGRATED_BASE}/{_nn}"
                    if not _child.startswith("/"):
                        _child = f"/Workspace/{_child}"
                    _child = _child.replace(" ", "_")
                    _key = f"spark.aidp.nbrun.ret_{i}_{_ci}"
                    # Set the child's params (Map args) + init the capture key in-kernel.
                    _setp = '%scala\nspark.conf.set("' + _key + '", "")\n'
                    for _pk, _pv in _parse_run_map_args(_call["full"]):
                        _setp += 'try { spark.conf.set("spark.aidp.param.' + _pk + '", String.valueOf(' + _pv + ')) } catch { case _: Throwable => () }\n'
                    try:
                        await session.execute(_setp, timeout=30)
                    except Exception as _e:
                        log.log(f"  Cell {i}: notebook.run param-set warning: {str(_e)[:80]}")
                    log.log(f"  Cell {i}: notebook.run -> inlining child (like %run): {_child}")
                    try:
                        await _inline_child_notebook(
                            _child, session, log, job_name, analysis,
                            catalog_context, dependent_context, dep_path_map or {},
                            capture_conf_key=_key)
                    except Exception as _e:
                        log.log(f"  Cell {i}: notebook.run inline error: {str(_e)[:120]}")
                    _nbrun_subs[_call["full"]] = 'spark.conf.get("' + _key + '", "")'

                # Execute-first; Opus (call_fix) ONLY on a real execution error.
                _SCALA_RETRIES = 5
                current_code = source
                _resolved = False
                for _scala_attempt in range(_SCALA_RETRIES):
                    # EXEC-ONLY: inject --param values via spark.conf for %scala
                    # getParameter reads (validation only). current_code (saved
                    # cell) is left unchanged — correct for a real workflow.
                    exec_code = _scala_param_exec_rewrite(current_code)
                    # EXEC-ONLY: swap notebook.run(...) → the inlined child's
                    # captured return (spark.conf). Saved cell keeps notebook.run.
                    for _orig, _rep in _nbrun_subs.items():
                        exec_code = exec_code.replace(_orig, _rep)
                    # DATA-SAFETY (EXEC-ONLY): no %scala write may touch a real
                    # bucket. Redirect literal write/table targets to the tmp
                    # bucket, redirect reads of already-redirected paths, then the
                    # runtime guard wraps VARIABLE write destinations too.
                    exec_code = _apply_write_redirects(exec_code, source_op_hint=f"scala-cell-{i}")
                    exec_code = _apply_read_redirects(exec_code)
                    exec_code = _inject_write_guard(exec_code)
                    try:
                        result = await session.execute(exec_code, timeout=14400)
                    except Exception as e:
                        # Transient (WS drop / kernel restart): retry AS-IS — do
                        # NOT hand to Opus (it would rewrite working code).
                        log.log(f"Cell {i}/{total_cells}: %scala session exception, retrying as-is ({str(e)[:80]})")
                        await asyncio.sleep(3)
                        continue
                    out = format_outputs(result.get("outputs", []))
                    # AIDP surfaces %scala/%sql failures as STREAM text with
                    # status still 'ok' and NO type=='error' output — e.g.
                    #   "Command ID <uuid> failed with ... Failing command: ..."
                    # so a structured-output check ALONE marks failures as OK
                    # (validated on-cluster: compile errors, throws, undefined
                    # refs all return status='ok', types=[stream,...]). Scan the
                    # rendered output for AIDP's failure wrapper too.
                    has_error = bool(result.get("outputs")) and any(
                        o.get("type") == "error" for o in result.get("outputs", []))
                    if not has_error and out and (
                            "Failing command:" in out
                            or re.search(r"Command ID [0-9a-fA-F-]+ failed with", out)):
                        has_error = True
                    # Kernel/session-level errors that DO set a status (rare; AIDP
                    # normally returns 'ok' even for command failures).
                    if not has_error and result.get("status") not in (None, "ok"):
                        has_error = True
                    if has_error:
                        # Log the FULL output so the real exception is visible (the
                        # AIDP wrapper truncates at "Failing command:" in a 200-char
                        # preview, which hid root causes). Mirrors the Python loop.
                        for _ln in (out or "").split("\n")[:30]:
                            if _ln.strip():
                                log.log(f"  %scala OUTPUT: {_ln[:300]}")
                    if not has_error:
                        cells_ok += 1
                        if _scala_attempt > 0:
                            cells_fixed += 1
                            migration_notes.append(f"Cell {i}: Scala auto-fixed by Opus (attempt {_scala_attempt})")
                            log.log(f"Cell {i}/{total_cells}: OK (%scala — fixed by Opus, attempt {_scala_attempt})")
                        else:
                            log.log(f"Cell {i}/{total_cells}: OK (%scala — executed as-is)")
                        cell_results.append({"cell": i, "status": "ok", "code": current_code[:120], "note": "%scala executed" + (" (Opus-fixed)" if _scala_attempt else "")})
                        _resolved = True
                        break
                    # Real code/data error → hand to Opus (Scala-aware), keep %scala.
                    if _scala_attempt < _SCALA_RETRIES - 1:
                        log.log(f"Cell {i}/{total_cells}: %scala error (attempt {_scala_attempt+1}/{_SCALA_RETRIES}) → Opus fix: {out[:200]}")
                        try:
                            current_code = await call_fix(
                                current_code, _compact_output_for_llm(out) or out, [], _scala_attempt + 1,
                                session=session, log_fn=log.log, notebook_path=notebook_path)
                            if not current_code.lstrip().startswith("%scala"):
                                current_code = "%scala\n" + current_code
                            # OCI namespace is authoritative from the source/mapping —
                            # revert any Opus-invented namespace change (e.g.
                            # <DATALAKE_NAMESPACE> -> <WORKSPACE_NAMESPACE>).
                            current_code = _apply_namespace_from_mapping(current_code)
                        except Exception as _fe:
                            log.log(f"Cell {i}/{total_cells}: %scala call_fix error ({str(_fe)[:80]})")
                            cells_failed += 1
                            cell_results.append({"cell": i, "status": "fail", "code": current_code[:120], "note": "%scala fix error"})
                            _resolved = True
                            break
                    else:
                        log.log(f"Cell {i}/{total_cells}: FAIL (%scala — error after {_SCALA_RETRIES} attempts): {out[:200]}")
                        cells_failed += 1
                        cell_results.append({"cell": i, "status": "fail", "code": current_code[:120], "note": "%scala execution error after Opus fixes"})
                        _resolved = True
                if not _resolved:
                    # All attempts hit transient session exceptions.
                    log.log(f"Cell {i}/{total_cells}: FAIL (%scala — unresolved after {_SCALA_RETRIES} transient attempts)")
                    cells_failed += 1
                    cell_results.append({"cell": i, "status": "fail", "code": current_code[:120], "note": "%scala unresolved (transient)"})
                cell["source"] = [current_code]
                migrated_cells.append(cell)
                continue

            # ── Check if this is a %pip install cell — install via cluster libraries API ──
            _pip_match = re.match(r'^\s*(?:%pip|!pip)\s+install\s+(.+)', stripped_source)
            if _pip_match and run_all:
                _pip_args = _pip_match.group(1).strip()
                # Extract package names (skip flags like --quiet, -q, --upgrade, -U)
                _pip_pkgs = [p for p in _pip_args.split()
                             if not p.startswith("-") and not p.startswith("=")]
                # Strip version pins (e.g. mlflow==2.14.2 → mlflow)
                _pip_pkgs = [re.split(r'[>=<!\[]', p)[0] for p in _pip_pkgs if p]
                if _pip_pkgs:
                    log.log(f"Cell {i}/{total_cells}: %pip install — installing {', '.join(_pip_pkgs)} via cluster libraries API")
                    for _pkg in _pip_pkgs:
                        if _pkg not in _auto_installed_modules:
                            _auto_installed_modules.add(_pkg)
                            try:
                                from cluster_lifecycle import install_missing_package
                                job_output_path = f"{OUTPUT_BASE}/{job_name}"
                                await install_missing_package(
                                    session.cluster_id, session, _pkg,
                                    job_output_path, timeout=600)
                            except Exception as _e:
                                log.log(f"  [auto-install] {_pkg} failed: {_e}")
                # Comment out the original cell
                commented_cell = copy.deepcopy(cell)
                commented_cell["source"] = [f"# AIDP: installed via cluster libraries API\n# {stripped_source}\n"]
                commented_cell["outputs"] = []
                migrated_cells.append(commented_cell)
                cells_ok += 1
                cell_results.append({"cell": i, "status": "ok", "code": stripped_source[:120], "note": "%pip → cluster libraries API"})
                continue

            # ── Check if this is a %run or dbutils/oidlUtils.notebook.run cell ──
            is_run_cell = source.strip().startswith("%run")
            # Check uncommented lines only — commented-out notebook.run() is dead code
            _active_lines = [l.strip() for l in source.splitlines() if l.strip() and not l.strip().startswith("#")]
            _active_src = "\n".join(_active_lines)
            is_nb_run_cell = "dbutils.notebook.run(" in _active_src or "oidlUtils.notebook.run(" in _active_src or "notebook.run(" in _active_src

            if is_run_cell:
                # Extract the path from the %run line
                run_line = source.strip()[4:].strip()  # remove %run
                # Handle quoted paths (e.g. %run "/path/with spaces/Utils")
                if run_line.startswith('"') or run_line.startswith("'"):
                    q = run_line[0]
                    end = run_line.find(q, 1)
                    run_path = run_line[1:end] if end > 0 else run_line[1:].rstrip(q)
                else:
                    run_path = run_line.split()[0]  # first token is the path
                run_path = re.sub(r'\$\w+', '', run_path).strip()
                # Strip any residual quotes (double-escaped paths in source)
                run_path = run_path.strip('"').strip("'")
                if not run_path.endswith(".ipynb"):
                    run_path += ".ipynb"

                # Resolve relative paths relative to current notebook's directory.
                # Databricks treats ALL non-absolute %run paths as relative to the caller:
                #   %run ../foo, %run ./foo, %run SaveTable (bare name)
                if not run_path.startswith("/"):
                    current_dir = os.path.dirname(notebook_path)
                    run_path = os.path.normpath(os.path.join(current_dir, run_path))

                # Look up the migrated path (try exact, suffix, and space/underscore variants)
                migrated_run_path = None
                if dep_path_map:
                    run_path_variants = {run_path, run_path.replace(" ", "_"), run_path.replace("_", " ")}
                    for orig, mig in dep_path_map.items():
                        if not mig:
                            continue
                        for rp_variant in run_path_variants:
                            # Exact match or suffix match anchored at directory boundary
                            if orig == rp_variant or rp_variant.endswith("/" + orig) or orig.endswith("/" + rp_variant.lstrip("/")):
                                migrated_run_path = mig
                                break
                        if migrated_run_path:
                            break

                # Fall back: construct migrated path under MIGRATED_BASE (same as notebook.run fallback)
                if not migrated_run_path:
                    nb_norm = normalize_nb_path(run_path)
                    if not nb_norm.endswith(".ipynb"):
                        nb_norm += ".ipynb"
                    migrated_run_path = f"{MIGRATED_BASE}/{nb_norm}"
                child_path = migrated_run_path or run_path
                # Ensure /Workspace prefix — cluster filesystem requires it
                if not child_path.startswith("/Workspace/") and not child_path.startswith("/"):
                    child_path = f"/Workspace/{child_path}"
                # AIDP converts spaces to underscores in paths
                child_path = child_path.replace(" ", "_")

                if run_all:
                    # INLINE the child notebook: migrate + execute each child cell
                    # in the current session (which has the parent's namespace)
                    log.log(f"  Cell {i}: %run -> inlining child: {child_path}")
                    child_result = await _inline_child_notebook(
                        child_path, session, log, job_name,
                        analysis, catalog_context, dependent_context,
                        dep_path_map or {},
                    )
                    # Re-inject job parameters into kernel after %run
                    # Child notebooks (e.g. 00_dream_utils) may overwrite dbutils with a
                    # different instance that has empty widgets. This restores parameters
                    # from the AIDP_PARAMS env var into whatever dbutils is currently active.
                    # ALSO re-applies the oidlUtils.parameters.getParameter bridge in case a
                    # child notebook reset/replaced the native module attribute.
                    if parameters and run_all:
                        # Re-inject job parameters into dbutils after %run.
                        # Child notebooks (e.g. 00_dream_utils) may overwrite dbutils with a
                        # different instance that has empty widgets. The oidlUtils wrapper
                        # is installed by the cluster-session bootstrap snippet and reads
                        # from manifest_params.json on every call — it survives %run
                        # without needing to be re-installed here. We only need to refresh
                        # dbutils.widgets so any code that reads them sees the right values.
                        await session.execute("""
import os, json
_p = os.environ.get('AIDP_PARAMS')
if _p and 'dbutils' in dir():
    try:
        _params = json.loads(_p)
        if hasattr(dbutils, 'widgets') and hasattr(dbutils.widgets, '_values'):
            dbutils.widgets._values.update(_params)
        elif hasattr(dbutils, 'widgets') and hasattr(dbutils.widgets, '_data'):
            if isinstance(dbutils.widgets._data, dict):
                dbutils.widgets._data.update(_params)
    except Exception:
        pass
""", timeout=15)

                    # In the saved notebook, keep %run with the migrated path
                    migrated_code = f'%run {child_path}'
                    migrated_cell = copy.deepcopy(cell)
                    migrated_cell["source"] = [migrated_code]
                    migrated_cell["outputs"] = []
                    migrated_cells.append(migrated_cell)

                    if child_result.get("status") == "ok":
                        cells_ok += 1
                        cell_results.append({"cell": i, "status": "ok", "code": migrated_code, "child": child_path,
                                            "child_cells_ok": child_result.get("cells_ok", 0),
                                            "child_cells_fixed": child_result.get("cells_fixed", 0)})
                        log.log(f"Cell {i}/{total_cells}: OK (inlined {child_result.get('cells_ok', 0)} child cells, {child_result.get('cells_fixed', 0)} fixed)")
                        # %run shares parent namespace — notebook.exit() in child stops parent too
                        if "exit_value" in child_result:
                            log.log(f"Cell {i}/{total_cells}: child called notebook.exit() — stopping parent: {child_result['exit_value'][:100]}")
                            break
                    else:
                        cells_failed += 1
                        cell_results.append({"cell": i, "status": "error", "code": migrated_code, "child": child_path,
                                            "child_error": child_result.get("error", "")})
                        log.log(f"Cell {i}/{total_cells}: CHILD FAILED: {child_result.get('error', '')[:150]}")
                        # Stop parent - namespace is incomplete without the child
                        job_should_stop = True
                    continue
                else:
                    # Code-only: keep %run with migrated path
                    migrated_code = f'%run {child_path}'
                    migration_notes.append(f"Cell {i}: %run path updated to migrated location")
                    log.log(f"  Cell {i}: %run -> {migrated_code[:120]}")

            else:
                if DIRECT_EXECUTE:
                    # Debug/WS-test mode: execute exactly as-is, no transforms.
                    migrated_code = source
                elif is_native_sql:
                    # Native SQL: deterministic transforms only, NO Opus. SQL
                    # cells are overwhelmingly table reads (%sql SELECT ... FROM
                    # table / spark.sql), and table reads run on AIDP unchanged
                    # (same catalog identifiers). The only migration they need is
                    # path/%run rewriting, which the regex transforms cover. If a
                    # SQL cell does fail at execution, the fix loop (FIX_PROMPT,
                    # already SQL-aware) repairs it.
                    migrated_code = _apply_s3_translations(source)
                    migrated_code = _rewrite_internal_paths(migrated_code, MIGRATED_BASE, notebook_path, dep_path_map)
                    migrated_code = _fix_path_replace_idempotency(migrated_code)
                    if migrated_code != source:
                        migration_notes.append(f"Cell {i}: native SQL — deterministic path transforms (S3→OCI, %run); no Opus")
                elif is_native_scala:
                    # Native Scala: migrate THROUGH Opus, treated as Scala. The
                    # deterministic regex alone is not enough — it only matches
                    # literal "s3a://bucket/path" strings and misses non-literal
                    # paths (built from vars/concat/config), and an untranslated
                    # s3 read may SUCCEED silently (cluster reaches AWS) so the
                    # execute→fix loop never fires. Opus catches those cases at
                    # migrate time. Opus MUST keep this as Scala and never port it
                    # to Python (see the LANGUAGE directive below + FIX_PROMPT).
                    _scala_directive = (
                        "LANGUAGE: This is a SCALA cell (it begins with %scala, or runs on a "
                        "Scala-default notebook). Migrate it AS SCALA:\n"
                        "- Keep the %scala magic; emit valid Scala syntax only.\n"
                        "- NEVER rewrite or port this cell to Python — the output must be Scala.\n"
                        "- Translate Databricks/AWS APIs to their OCI equivalents IN SCALA: "
                        "s3a:// | s3:// paths -> oci://<bucket>@<namespace>/..., and the "
                        "dbutils.notebook.* / dbutils.fs.* renames per the documented rules "
                        "(oidlUtils.* / aidp_compat). Leave plain table reads as table reads.\n"
                        "- Any tool-added comment lines MUST use Scala // syntax (never #).\n\n"
                    )
                    migrate_resp, usage = await call_opus_with_tools(CELL_MIGRATE_PROMPT,
                        f"{_scala_directive}Migrate cell {i}/{total_cells} from {notebook_path}:\n\n{cell_migrate_context}\n\n```scala\n{source}\n```",
                        session=session, max_tokens=128000, log_fn=log.log)
                    total_tokens += usage["input"] + usage["output"]

                    migrated_code = _extract_code_from_response(migrate_resp)

                    # Deterministic post-Opus transforms (safety net), same as the
                    # Python branch. _add_missing_imports is already Scala-aware.
                    migrated_code = _apply_s3_translations(migrated_code)
                    migrated_code = _rewrite_internal_paths(migrated_code, MIGRATED_BASE, notebook_path, dep_path_map)
                    migrated_code = _fix_path_replace_idempotency(migrated_code)
                    migrated_code = _add_missing_imports(migrated_code)

                    _path_issues = _validate_migrated_run_paths(migrated_code, MIGRATED_BASE)
                    for _issue in _path_issues:
                        log.log(f"Cell {i}: WARN path-validator: {_issue}")
                        migration_notes.append(f"Cell {i}: path-validator: {_issue}")

                    if migrated_code != source:
                        migration_notes.append(f"Cell {i}: native Scala — migrated by Opus (Scala-preserving)")
                else:
                    # ── First-try optimization ───────────────────────────────
                    # Apply only the deterministic mechanical transforms (S3→OCI,
                    # %run path rewrite, idempotent prefix fix). If the resulting
                    # candidate contains no Databricks-specific markers, skip the
                    # upfront CELL_MIGRATE_PROMPT call entirely — the execute+
                    # verify+fix loop downstream will execute it as-is and only
                    # invoke FIX_PROMPT (call_fix) if it actually fails. This
                    # eliminates over-migration on cells that work as-is.
                    _candidate = _apply_s3_translations(source)
                    _candidate = _rewrite_internal_paths(_candidate, MIGRATED_BASE, notebook_path, dep_path_map)
                    _candidate = _fix_path_replace_idempotency(_candidate)

                    if not _has_databricks_markers(_candidate):
                        # No Databricks markers — try as-is on the cluster.
                        # call_fix in the execute+verify+fix loop is the safety
                        # net if the cell still fails for a reason we missed.
                        migrated_code = _add_missing_imports(_candidate)
                        log.log(f"Cell {i}/{total_cells}: AS_IS_CANDIDATE — no DB markers, will try as-is")
                        if migrated_code != source:
                            migration_notes.append(f"Cell {i}: deterministic transforms only (no Opus)")
                    else:
                        # ── Migrate this cell with Opus (with tool use for path exploration) ──
                        migrate_resp, usage = await call_opus_with_tools(CELL_MIGRATE_PROMPT,
                            f"Migrate cell {i}/{total_cells} from {notebook_path}:\n\n{cell_migrate_context}\n\n```python\n{source}\n```",
                            session=session, max_tokens=128000, log_fn=log.log)
                        total_tokens += usage["input"] + usage["output"]

                        # Clean response - extract code, strip explanation text
                        migrated_code = _extract_code_from_response(migrate_resp)

                        # Deterministic post-Opus transforms (S3→OCI, path fix, missing imports)
                        migrated_code = _apply_s3_translations(migrated_code)
                        migrated_code = _rewrite_internal_paths(migrated_code, MIGRATED_BASE, notebook_path, dep_path_map)
                        migrated_code = _fix_path_replace_idempotency(migrated_code)
                        migrated_code = _detect_table_to_path_regression(source, migrated_code)
                        migrated_code = _detect_path_returning_to_identifier_regression(source, migrated_code)
                        migrated_code = _add_missing_imports(migrated_code)

                        # Sanity-check the migrated cell for known path-mangling patterns
                        # (doubled MIGRATED_BASE, .ipynb<digit>.ipynb basename mangling,
                        # doubled .ipynb suffix). Pure logging; does not block the cell.
                        _path_issues = _validate_migrated_run_paths(migrated_code, MIGRATED_BASE)
                        for _issue in _path_issues:
                            log.log(f"Cell {i}: WARN path-validator: {_issue}")
                            migration_notes.append(f"Cell {i}: path-validator: {_issue}")

                        if migrated_code != source:
                            migration_notes.append(f"Cell {i}: migrated by Opus")

            # ── Rewrite notebook.run() paths to migrated locations ──
            if is_nb_run_cell and dep_path_map:
                # Search only uncommented lines for the active notebook.run() call
                _active_mig = "\n".join(l for l in migrated_code.splitlines()
                                        if l.strip() and not l.strip().startswith("#"))
                nb_run_match = re.search(r'(notebook\.run\s*\(\s*["\'])([^"\']+)(["\'])', _active_mig)
                if nb_run_match:
                    orig_run_path = nb_run_match.group(2)
                    migrated_run_path = None
                    run_path_variants = {orig_run_path, orig_run_path.replace(" ", "_"), orig_run_path.replace("_", " ")}
                    for orig, mig in dep_path_map.items():
                        if not mig:
                            continue
                        for rp_variant in run_path_variants:
                            # Exact match or suffix match anchored at directory boundary
                            if orig == rp_variant or rp_variant.endswith("/" + orig) or orig.endswith("/" + rp_variant.lstrip("/")):
                                migrated_run_path = mig
                                break
                        if migrated_run_path:
                            break
                    # Fall back: check under MIGRATED_BASE
                    if not migrated_run_path:
                        nb_norm = normalize_nb_path(orig_run_path)
                        migrated_run_path = f"{MIGRATED_BASE}/{nb_norm}"
                    if migrated_run_path and migrated_run_path != orig_run_path:
                        migrated_code = migrated_code.replace(orig_run_path, migrated_run_path, 1)
                        migration_notes.append(f"Cell {i}: notebook.run() path rewritten to {migrated_run_path}")
                        log.log(f"  Cell {i}: notebook.run() path -> {migrated_run_path[:120]}")

            migrated_cell = copy.deepcopy(cell)
            migrated_cell["source"] = [migrated_code]
            migrated_cell["outputs"] = []
            migrated_cells.append(migrated_cell)

            # ── Code-only mode (deps): skip execution, just save migrated code ──
            if not run_all:
                cell_results.append({"cell": i, "status": "migrated", "code": migrated_code[:2000]})
                cells_ok += 1
                log.log(f"Cell {i}/{total_cells}: MIGRATED (code-only, no execution)")
                # Static exit detection for migrate-only mode.
                # Execution mode catches this at runtime via NotebookExit exception.
                _unconditional_exit = False
                for _ln in migrated_code.splitlines():
                    _stripped = _ln.lstrip()
                    if _stripped.startswith("#"):
                        continue
                    if (_stripped.startswith("oidlUtils.notebook.exit(") or
                            _stripped.startswith("dbutils.notebook.exit(")):
                        if len(_ln) - len(_stripped) == 0:  # zero indentation = unconditional
                            _unconditional_exit = True
                            break
                if _unconditional_exit:
                    log.log(f"Cell {i}/{total_cells}: unconditional notebook.exit() — skipping {total_cells - i - 1} remaining cell(s)")
                    for j in range(i + 1, len(cells)):
                        migrated_cells.append(cells[j])
                    break
                continue

            # ── Task validation stop: code-only continuation ───────────
            # Set after a prior cell hit DATA_UNAVAILABLE_AT_MIGRATION. Subsequent
            # cells get their code migrated but NOT executed — saved notebook is
            # complete end-to-end; customer-runtime validates execution.
            if task_validation_stop:
                log.log(f"Cell {i}/{total_cells}: MIGRATED_NOT_VALIDATED — code-only (task validation halted earlier)")
                cell_results.append({"cell": i, "status": "MIGRATED_NOT_VALIDATED",
                                     "code": migrated_code[:1000]})
                cells_not_validated += 1
                _cell_history.append({
                    "index":          len(_cell_history),
                    "notebook_path":  notebook_path,
                    "cell_idx":       i,
                    "summary":        "MIGRATED_NOT_VALIDATED (data unavailable upstream)",
                    "final_code":     migrated_code[:3000],
                    "output_preview": "",
                    "status":         "migrated_not_validated",
                    "is_child":       False,
                    "last_note":      "",
                })
                continue

            # ── Execute + Verify + Fix loop (up to 10 attempts) ──
            MAX_CELL_RETRIES = 5
            current_code = migrated_code
            cell_passed = False
            orig_output = original_outputs[i] if i < len(original_outputs) else None
            _current_cell_notes.clear()
            # Per-cell data-recovery state. recovery_override is the EXEC-only
            # override block (date-substitution); recovery_attempted is set
            # the first time we try recovery, so we don't retry on every
            # attempt. original_failure_was_empty_data is set if attempt 0's
            # failure had an empty-data signature — used at max-retries to
            # decide between DATA_UNAVAILABLE_AT_MIGRATION (skip-and-continue)
            # vs FAILED (existing terminal-failure behavior).
            recovery_override = ""
            recovery_attempted = False
            original_failure_was_empty_data = False

            for attempt in range(MAX_CELL_RETRIES):
                # Compose the EXEC code: prepend the recovery override (if any)
                # to the in-memory current_code. The override is NEVER saved
                # to disk — only used for cluster execution.
                exec_code = (recovery_override + current_code) if recovery_override else current_code
                # ── Write-redirect (tool-only): never touch customer data ──
                # Detect writes/DML/DDL targets; record them in the job-wide
                # redirect map; substitute redirected destinations in EXEC
                # code only (current_code stays clean for save-back). Then
                # apply read-redirect for any target prior cells wrote to.
                exec_code = _apply_write_redirects(exec_code, source_op_hint=f"cell-{i}")
                exec_code = _inject_write_guard(exec_code)  # variable write dests → tmp
                exec_code = _apply_read_redirects(exec_code)
                # ── Execute ──
                try:
                    result = await session.execute(exec_code, timeout=14400)
                except Exception as exec_err:
                    log.log(f"Cell {i}: session error: {str(exec_err)[:100]}")
                    if session_pool:
                        try:
                            await session_pool.release(session)
                            session = await session_pool.acquire()
                            await session.execute(bootstrap, timeout=30)
                            await session.execute(AIDP_TOPANDAS_SAFETY, timeout=30)
                            if parameters:
                                await session.execute(params_code, timeout=30)
                            for prev_code in executed_code[-5:]:
                                try: await session.execute(prev_code, timeout=60)
                                except: pass
                            log.log(f"Cell {i}: reconnected, retrying...")
                            result = await session.execute(exec_code, timeout=14400)
                        except:
                            result = {"status": "error", "outputs": [{"type": "error", "ename": "SessionError", "evalue": str(exec_err)[:200]}]}
                    else:
                        result = {"status": "error", "outputs": [{"type": "error", "ename": "SessionError", "evalue": str(exec_err)[:200]}]}

                status = result.get("status", "error")
                raw_outputs = result.get("outputs", [])
                output = format_outputs(raw_outputs)

                # ── Cluster-down detection: reconnect WS + re-execute ──
                # When the AIDP Dataflow compute cluster is paused, the Python kernel
                # stays alive but Spark is unavailable. AIDP prints "Compute cluster X
                # is not running" in the cell output (status=ok, but Spark dead).
                # Fix: disconnect + reconnect the WS transport. This triggers AIDP to
                # resume the compute cluster. Then replay bootstrap + recent cells to
                # restore kernel state, and re-execute the current cell.
                if _CLUSTER_DOWN_RE.search(output or ""):
                    log.log(f"Cell {i}/{total_cells}: AIDP compute cluster down — reconnecting WS...")
                    await session.force_reconnect()
                    log.log(f"Cell {i}/{total_cells}: WS reconnected — replaying bootstrap...")
                    try:
                        await session.execute(bootstrap, timeout=60)
                        await session.execute(AIDP_TOPANDAS_SAFETY, timeout=30)
                        if parameters:
                            await session.execute(params_code, timeout=60)
                        for prev_code in executed_code[-5:]:
                            try:
                                await session.execute(prev_code, timeout=120)
                            except Exception:
                                pass
                    except Exception as replay_err:
                        log.log(f"Cell {i}/{total_cells}: bootstrap replay failed: {replay_err}")
                    log.log(f"Cell {i}/{total_cells}: retrying cell after reconnect...")
                    result = await session.execute(exec_code, timeout=14400)
                    raw_outputs = result.get("outputs", [])
                    output = format_outputs(raw_outputs)
                    status = result.get("status", "error")

                # Debug: log raw result if output is empty on error
                if status != "ok" and not output:
                    log.log(f"  DEBUG raw result keys: {list(result.keys())}")
                    log.log(f"  DEBUG raw outputs ({len(raw_outputs)}): {str(raw_outputs)[:500]}")
                    log.log(f"  DEBUG status: {status}")

                # ── Check for notebook.exit() — treat as successful early stop ──
                # Only match NotebookExit exception, not function call strings
                # like "dbutils.notebook.exit" which could appear in comments/logs.
                _nb_exit = False
                if status != "ok" and output:
                    _exit_patterns = ["NotebookExit"]
                    if any(p in output for p in _exit_patterns):
                        _nb_exit = True
                if _nb_exit:
                    exit_msg = output.strip().split("\n")[-1][:200]
                    log.log(f"Cell {i}/{total_cells}: notebook.exit() called — stopping execution: {exit_msg}")
                    cells_ok += 1
                    executed_code.append(current_code)
                    migrated_cells[i]["source"] = current_code.split("\n")
                    _cell_history.append({
                        "index": len(_cell_history), "notebook_path": notebook_path,
                        "cell_idx": i, "summary": f"notebook.exit(): {exit_msg}",
                        "final_code": current_code[:3000], "output_preview": output[:300],
                        "status": "ok", "is_child": False, "last_note": "",
                    })
                    # Preserve remaining cells as-is (not executed)
                    for j in range(i + 1, total_cells):
                        migrated_cells.append(cells[j])
                    break

                # ── Verify (4 checks) ──
                verification_issues = []

                if status != "ok":
                    verification_issues.append(f"Execution error: {output[:500]}")
                else:
                    # Check 1: Error patterns in stdout (exclude warnings).
                    # Each exception-name pattern requires a trailing ':' so it matches
                    # Python's actual traceback format (`ValueError: foo`) but NOT bare
                    # occurrences inside printed source code (e.g., a cell that prints
                    # `inspect.getsource(createTable)` where createTable contains a
                    # legitimate `raise ValueError(...)` line — this used to trip a
                    # false-positive VERIFY FAIL that exhausted call_fix's 5 attempts
                    # and failed otherwise-clean tasks).
                    error_patterns = ["Traceback", "Exception:", "FileNotFoundError:", "NameError:", "TypeError:", "ModuleNotFoundError:", "ImportError:", "AttributeError:", "KeyError:", "ValueError:"]
                    # Lines that are warnings, not errors
                    warning_patterns = ["WARN", "WARNING", "should be int, but was", "deprecated", "FutureWarning", "UserWarning", "DeprecationWarning"]
                    for pat in error_patterns:
                        if pat in (output or ""):
                            # Check it's not just in a warning line
                            is_warning = False
                            for line in (output or "").split("\n"):
                                if pat in line:
                                    if any(wp in line for wp in warning_patterns):
                                        is_warning = True
                                        break
                            if not is_warning:
                                verification_issues.append(f"Error pattern in output: '{pat}' found")
                                break

                    # Check 2: Validate DataFrame outputs (if cell creates/transforms DataFrames)
                    if any(kw in current_code for kw in [".read.", "spark.sql(", "spark.table(", ".filter(", ".groupBy(", ".join("]):
                        try:
                            check_result = await session.execute(
                                "import json; _vars = {k: str(type(v).__name__) for k,v in list(locals().items())[-10:] if hasattr(v, 'count') and 'DataFrame' in type(v).__name__}; print(json.dumps(_vars))",
                                timeout=15)
                            # If we got here without error, DataFrames are accessible
                        except:
                            pass

                    # Check 3a: OCI permission / infrastructure errors in output (unfixable by Opus)
                    _oci_fatal_patterns = [
                        "NotAuthorizedOrNotFound",
                        "User does not have required privileges",
                        "BucketNotFound",
                        "GeneratePar operation in LakeSharing",
                        "404, NotAuthorizedOrNotFound",
                    ]
                    for _ofp in _oci_fatal_patterns:
                        if _ofp in (output or ""):
                            verification_issues.append(
                                f"OCI PERMISSION ERROR (unfixable): '{_ofp}' — "
                                f"cluster lacks access to the target bucket/path. "
                                f"Fix IAM/bucket policies, do NOT retry with Opus.")
                            break

                    # Check 3b: Fetch Spark logs for silent errors
                    spark_errors = fetch_recent_spark_errors(session.cluster_id, minutes_back=1)
                    if spark_errors and "BucketNotFound" in spark_errors:
                        verification_issues.append(f"Spark log error: {spark_errors[:300]}")

                    # Check 4: Ask Opus to evaluate the output (skipped in --direct-execute mode)
                    if not DIRECT_EXECUTE and output and len(output.strip()) > 10:
                        orig_context = f"\n\nOriginal Databricks output for this cell:\n```\n{orig_output}\n```" if orig_output else "\n\n(No original output available for comparison)"
                        bucket_ctx = get_bucket_mapping_context()
                        eval_prompt = """You are verifying a migrated Spark cell's output on AIDP (Spark 3.5).

Reply with EXACTLY:
'PASS: <reason>' if the output looks correct, OR
'ISSUE: <description> | FIX: <specific suggestion for fixing the code>' if there is a real problem.

You have tools to verify paths and data availability. If the error involves a missing path or table,
use explore_path or suggest_oci_path to check if the data exists elsewhere (different namespace).
Use run_on_cluster to test fixes before recommending them.

AIDP CONTEXT:
- oidlUtils is a NATIVE AIDP module, pre-loaded in every kernel. Do NOT flag oidlUtils.notebook.exit(),
  oidlUtils.notebook.run(), or oidlUtils.parameters.* as wrong — these are the correct AIDP equivalents
  of dbutils.notebook.exit(), dbutils.notebook.run(), and dbutils.jobs.taskValues.

IMPORTANT - these are NOT issues (reply PASS):
- Spark warnings like "spark.sql.shuffle.partitions should be int, but was auto"
- Deprecation warnings, FutureWarnings, UserWarnings
- Minor formatting differences from original output
- Empty output when the cell is an assignment, import, or function definition (no output expected)
- Different timestamps, dates, or row counts (data may have changed between Databricks and AIDP)
- Missing original output to compare against
- Data count differences when the data exists but has different volume on AIDP
- "Compute cluster is not running" — transient infrastructure error, not a code bug

These ARE issues (reply ISSUE with FIX suggestion):
- Python exceptions (NameError, TypeError, FileNotFoundError, etc.)
- Tracebacks indicating code failure
- "Table not found" or "Path does not exist" errors (use tools to find correct path)
- Completely empty output when the original had substantial data output AND you verified the data should exist

EXECUTION CONTEXT: You are evaluating cell output in a sequential kernel. All previous cells have run.
The job execution history (last 100 cells) is shown above. If the error traces back to an upstream cell,
call get_cell_history to scan and fixup_cell to rewind — but verify idempotency first (replay re-executes
all cells from start_index forward). Use make_note to record migration concerns.
WHEN TO REWIND: If this is attempt 7+ and the root cause appears to be upstream, prefer fixup_cell."""
                        eval_resp, eval_usage = await call_opus_with_tools(
                            eval_prompt,
                                                        # TODO: table names are same on AIDP (just default. prefix), full catalog not needed
                            # Full catalog: f"...Catalog (available tables):\n{catalog_context}"
                            f"Cell code:\n```python\n{current_code}\n```\n\nFull execution output:\n```\n{_compact_output_for_llm(output)}\n```{orig_context}\n\n{bucket_ctx}\n\nNotebook context:\n{analysis[:2000]}\n\nCatalog (available tables):\n{catalog_context[:3000]}",
                            session=session, max_tokens=4000, max_tool_rounds=10, log_fn=log.log
                        )
                        total_tokens += eval_usage["input"] + eval_usage["output"]
                        eval_line = eval_resp.strip().split("\n")[0]
                        if eval_line.upper().startswith("ISSUE"):
                            verification_issues.append(f"Opus evaluation: {eval_line}")
                            # Extract FIX suggestion if present
                            if "FIX:" in eval_line:
                                eval_fix_suggestion = eval_line.split("FIX:", 1)[1].strip()
                            else:
                                eval_fix_suggestion = ""

                # ── If verified OK, accept the cell ──
                if not verification_issues:
                    cell_passed = True
                    consecutive_failures = 0
                    executed_code.append(current_code)
                    # Track any pip install commands in executed cell code
                    for pkg in extract_pip_packages(current_code):
                        _installed_packages.add(pkg)

                    # Inject execution output into notebook cell (unwrap AIDP JSON wrapper + dedup)
                    from context_tools import _unwrap_aidp_text
                    nb_outputs = []
                    seen_texts = set()
                    for ro in raw_outputs:
                        if ro.get("type") == "stream":
                            raw_text = ro.get("text", "")
                            clean_text = _unwrap_aidp_text(raw_text)
                            if clean_text and clean_text not in seen_texts:
                                seen_texts.add(clean_text)
                                nb_outputs.append({"output_type": "stream", "name": ro.get("name", "stdout"), "text": [clean_text]})
                        elif ro.get("type") == "execute_result":
                            data = ro.get("data", {})
                            if "text/plain" in data:
                                data = dict(data)
                                data["text/plain"] = _unwrap_aidp_text(data["text/plain"])
                            nb_outputs.append({"output_type": "execute_result", "data": data, "metadata": ro.get("metadata", {}), "execution_count": None})
                        elif ro.get("type") == "error":
                            # Strip ANSI codes from traceback for clean display
                            import re as _re
                            tb = ro.get("traceback", [])
                            clean_tb = [_re.sub(r'\x1b\[[0-9;]*m', '', line) for line in tb]
                            nb_outputs.append({"output_type": "error", "ename": ro.get("ename", ""), "evalue": ro.get("evalue", ""), "traceback": clean_tb})
                    migrated_cells[-1]["outputs"] = nb_outputs

                    if recovery_override:
                        # Cell passed with date-substitution override. Track
                        # separately from cells_fixed — this isn't a code
                        # fix, it's a test-only data substitution. The saved
                        # source is current_code (override exists ONLY in
                        # exec_code, never persisted). Defense-in-depth:
                        # strip any stray override block before saving.
                        cells_data_substituted += 1
                        migrated_cells[-1]["source"] = [_strip_data_recovery_block(current_code)]
                        migration_notes.append(
                            f"Cell {i}: OK_DATA_SUBSTITUTED — code validated on cluster "
                            f"using dates substituted from upstream table; "
                            f"original date filter saved unchanged"
                        )
                    elif attempt > 0:
                        cells_fixed += 1
                        migrated_cells[-1]["source"] = [current_code]
                        migration_notes.append(f"Cell {i}: auto-fixed (attempt {attempt})")

                    match_detail = None
                    if orig_output:
                        if " ".join(orig_output.strip().split()) == " ".join((output or "").strip().split()):
                            match_detail = "exact match"
                        else:
                            match_detail = "differs"

                    cell_results.append({
                        "cell": i, "status": ("OK_DATA_SUBSTITUTED" if recovery_override else "ok"),
                        "attempts": attempt + 1, "fixed": (attempt > 0 and not recovery_override),
                        "data_substituted": bool(recovery_override),
                        "code": current_code[:2000], "output": (output or "")[:3000],
                        "original_output": (orig_output[:1000] if orig_output else None),
                        "output_match": match_detail,
                    })
                    cells_ok += 1
                    if recovery_override:
                        log.log(f"Cell {i}/{total_cells}: OK_DATA_SUBSTITUTED (dates from upstream table; saved code unchanged)")
                    else:
                        fix_note = f" (fixed attempt {attempt})" if attempt > 0 else ""
                        log.log(f"Cell {i}/{total_cells}: OK{fix_note}")

                    # Update accumulated cell context
                    output_preview = _unwrap_aidp_text(output[:200]) if output else ""
                    code_preview = current_code.strip()[:80].replace('\n', ' ')
                    cell_context.add_cell(i, total_cells, code_preview, output_preview, "OK",
                        fixes=[f"attempt {attempt}"] if attempt > 0 else None)

                    # Append notes to fix log and record in history
                    if _current_cell_notes:
                        note_block = "\n".join(f"# NOTE: {n}" for n in _current_cell_notes)
                        if "# === AIDP MIGRATION FIX LOG ===" in current_code:
                            current_code = current_code.rstrip() + "\n" + note_block
                        else:
                            current_code = current_code.rstrip() + "\n# === AIDP MIGRATION FIX LOG ===\n" + note_block
                        migrated_cells[-1]["source"] = [current_code]

                    # Append to job-wide cell history
                    cell_summary = (cell_plan.get("description", "") or
                                    current_code.strip()[:100].replace("\n", " "))[:200]
                    _cell_history.append({
                        "index":          len(_cell_history),
                        "notebook_path":  notebook_path,
                        "cell_idx":       i,
                        "summary":        cell_summary,
                        "final_code":     current_code[:3000],
                        "output_preview": (output or "")[:300],
                        "status":         "ok",
                        "is_child":       False,
                        "last_note":      _current_cell_notes[-1] if _current_cell_notes else "",
                    })
                    _current_cell_notes.clear()
                    break

                # ── Verification failed - fix with Opus ──

                # Rate-limit / CircuitBreaker pre-check
                # ----------------------------------------
                # If the failure is OCI Object Storage 429 / SDK CircuitBreaker
                # OPEN, it is almost always TRANSIENT -- the cluster is fine,
                # the bucket is just rate-limiting. Burning a fix attempt on
                # this guarantees Opus rewrites code that was correct. Sleep
                # past the CB open-state window and retry without incrementing
                # ``attempt``. Capped to ``_MAX_RATE_LIMIT_RETRIES`` per cell.
                _MAX_RATE_LIMIT_RETRIES = 5
                if _detect_rate_limit is not None and not DIRECT_EXECUTE:
                    rl_attempts = locals().get("_rl_attempts", 0)
                    rl_directive = _detect_rate_limit(output or "", attempt=rl_attempts + 1)
                    if rl_directive.detected and rl_attempts < _MAX_RATE_LIMIT_RETRIES:
                        coord = _get_throttle_coord()
                        if coord is not None:
                            try:
                                snap = coord.record_cb_event(label=f"{job_name}/{notebook_path}")
                                log.log(
                                    f"  [throttle] CB event recorded: budget={snap.get('budget')} "
                                    f"in_flight={snap.get('in_flight')} "
                                    f"events_in_window={snap.get('cb_events_in_window')}"
                                )
                            except Exception as _exc:
                                log.log(f"  [throttle] failed to record CB event: {_exc}")
                        log.log(
                            f"Cell {i}/{total_cells}: {rl_directive.reason} detected "
                            f"(bucket={rl_directive.bucket or 'unknown'}) "
                            f"-- sleeping {rl_directive.backoff_sec}s and retrying "
                            f"(rl_retry {rl_attempts + 1}/{_MAX_RATE_LIMIT_RETRIES})"
                        )
                        await asyncio.sleep(rl_directive.backoff_sec)
                        _rl_attempts = rl_attempts + 1
                        continue  # retry same attempt without incrementing fix counter

                # Auto-install missing Python packages before handing to Opus (max 3 per notebook)
                _MAX_AUTO_INSTALLS = 3
                _mnf_match = re.search(r"No module named ['\"]?(\w+)['\"]?", output or "")
                if _mnf_match and len(_auto_installed_modules) < _MAX_AUTO_INSTALLS:
                    _missing_mod = _mnf_match.group(1)
                    if _missing_mod not in _auto_installed_modules:
                        _auto_installed_modules.add(_missing_mod)
                        try:
                            from cluster_lifecycle import install_missing_package
                            job_output_path = f"{OUTPUT_BASE}/{job_name}"
                            log.log(f"  [auto-install] Missing module '{_missing_mod}' — installing via cluster libraries API ({len(_auto_installed_modules)}/{_MAX_AUTO_INSTALLS})...")
                            _installed = await install_missing_package(
                                session.cluster_id, session, _missing_mod,
                                job_output_path, timeout=600,
                                notebook_paths=[notebook_path])
                            if _installed:
                                log.log(f"  [auto-install] {_missing_mod} installed — retrying cell")
                                continue  # retry same attempt without incrementing
                        except Exception as _install_err:
                            log.log(f"  [auto-install] Failed: {_install_err}")
                elif _mnf_match and len(_auto_installed_modules) >= _MAX_AUTO_INSTALLS:
                    _missing_mod = _mnf_match.group(1)
                    log.log(f"  [auto-install] Skipped '{_missing_mod}' — reached max auto-installs ({_MAX_AUTO_INSTALLS}). Add to cluster libraries manually.")

                consecutive_failures += 1
                issue_summary = "; ".join(verification_issues)
                # Classify the FIRST failure (attempt 0) as empty-data or
                # not. This tag drives the terminal-failure branch below —
                # empty-data → DATA_UNAVAILABLE_AT_MIGRATION (continue with
                # code-only), code error → FAILED (existing terminal-stop).
                if attempt == 0 and not original_failure_was_empty_data:
                    if _is_empty_data_failure(output or "", current_code):
                        original_failure_was_empty_data = True
                log.log(f"Cell {i}/{total_cells}: VERIFY FAIL (attempt {attempt+1}/{MAX_CELL_RETRIES}): {issue_summary[:300]}")
                if output:
                    # Log full output for debugging
                    for line in (output or "").split("\n")[:20]:
                        if line.strip():
                            log.log(f"  OUTPUT: {line[:200]}")

                # Circuit breaker for systemic issues
                if consecutive_failures >= CONSECUTIVE_FAIL_THRESHOLD and attempt == 0:
                    recent_errors = [cr.get("output", "") for cr in cell_results[-3:] if cr.get("status") == "error"]
                    recent_errors.append(issue_summary)
                    verdict, diagnosis = await monitor_analyze_failures(recent_errors, notebook_path)
                    monitor_decisions.append({"cell": i, "verdict": verdict, "diagnosis": diagnosis})
                    log.log(f"MONITOR: {verdict} - {diagnosis[:150]}")
                    if verdict == "systemic":
                        if session_pool and "unknown error" in (output or "").lower():
                            try:
                                await session_pool.release(session)
                                session = await session_pool.acquire()
                                await session.execute(bootstrap, timeout=30)
                                await session.execute(AIDP_TOPANDAS_SAFETY, timeout=30)
                                if parameters: await session.execute(params_code, timeout=30)
                                consecutive_failures = 0
                                log.log("Reconnected - retrying")
                                continue
                            except: pass
                        job_should_stop = True
                        cell_results.append({"cell": i, "status": "error", "code": current_code[:1000], "output": (output or "")[:2000], "monitor": diagnosis})
                        cells_failed += 1
                        break

                if DIRECT_EXECUTE:
                    # No AI fixing in direct-execute mode — just record the failure and move on
                    break

                # Early exit: OCI permission errors are unfixable by Opus — skip retries immediately
                _has_oci_perm_error = any("OCI PERMISSION ERROR" in v for v in verification_issues)
                if _has_oci_perm_error:
                    log.log(f"Cell {i}/{total_cells}: OCI permission error — cannot be fixed by migration tool. Check cluster IAM/bucket policies.")
                    cells_failed += 1
                    cell_results.append({"cell": i, "status": "error", "code": current_code[:1000],
                                         "output": (output or "")[:2000], "note": "OCI permission error"})
                    _cell_history.append({
                        "index": len(_cell_history), "notebook_path": notebook_path,
                        "cell_idx": i, "summary": "FAILED: OCI permission error (unfixable)",
                        "final_code": current_code[:3000], "output_preview": (output or "")[:300],
                        "status": "error", "is_child": False,
                        "last_note": issue_summary[:500],
                    })
                    _current_cell_notes.clear()
                    break

                # Early exit: if Opus confirmed table/data is missing via make_note,
                # retrying won't help — the table won't appear between attempts.
                _has_missing_data_note = any(
                    "TABLE_BLOCKED" in n or "MISSING" in n.upper() or "does not exist" in n.lower()
                    for n in _current_cell_notes
                )
                if _has_missing_data_note and attempt >= 1:
                    log.log(f"Cell {i}/{total_cells}: table/data confirmed missing — skipping retries")
                    cells_failed += 1
                    cell_results.append({"cell": i, "status": "error", "code": current_code[:1000],
                                         "output": (output or "")[:2000], "note": "missing data"})
                    _cell_history.append({
                        "index": len(_cell_history), "notebook_path": notebook_path,
                        "cell_idx": i, "summary": f"FAILED: missing table/data",
                        "final_code": current_code[:3000], "output_preview": (output or "")[:300],
                        "status": "error", "is_child": False,
                        "last_note": _current_cell_notes[-1] if _current_cell_notes else "",
                    })
                    _current_cell_notes.clear()
                    break

                # ── Data-recovery attempt (ONE-SHOT, READ-ONLY cells only) ─
                # If this is an empty-data failure and we haven't tried
                # recovery yet, ask Opus to identify the date variable and
                # the upstream table, query the table for actually available
                # dates, and build an EXEC-only override block. The override
                # is prepended to the cell on retry; the saved cell stays
                # byte-identical to the original.
                # READ_ONLY restriction: WRITE cells (saveAsTable, .write.*,
                # etc.) are NEVER given substituted dates — a write with
                # wrong dates would corrupt destinations.
                if (not recovery_attempted
                        and _is_empty_data_failure(output or "", current_code)
                        and not _is_write_cell(current_code)
                        and not DIRECT_EXECUTE):
                    recovery_attempted = True
                    log.log(f"Cell {i}/{total_cells}: empty-data failure detected — attempting data recovery")
                    try:
                        _override = await attempt_data_recovery(
                            current_code, output or "", session,
                            notebook_path=notebook_path, log_fn=log.log,
                        )
                    except Exception as _rec_err:
                        log.log(f"Cell {i}/{total_cells}: data recovery error: {str(_rec_err)[:200]}")
                        _override = None
                    if _override:
                        recovery_override = _override
                        log.log(f"Cell {i}/{total_cells}: recovery override built, retrying with substituted dates")
                        # Re-enter the loop — exec_code will be recomputed
                        # with the override prepended for execution. attempt
                        # increments naturally (Python for-loop semantics).
                        continue
                    else:
                        log.log(f"Cell {i}/{total_cells}: data recovery did not yield a viable override; falling through to call_fix")
                        # Fall through to call_fix below in case the cell ALSO
                        # has a code-side issue that Opus can fix.

                if attempt < MAX_CELL_RETRIES - 1:
                    try:
                        spark_logs = fetch_recent_spark_errors(session.cluster_id, minutes_back=2)
                        orig_context = f"\n\nOriginal Databricks output:\n```\n{orig_output[:2000]}\n```" if orig_output else ""
                        # Include bucket mapping context for path fixes
                        bucket_ctx = get_bucket_mapping_context()
                        # TODO: table names are same on AIDP (just default. prefix), full catalog not needed
                        # Full catalog: fix_context = f"{bucket_ctx}\n\n{catalog_context}\n\n..."
                        fix_context = f"{bucket_ctx}\n\n{catalog_context[:5000]}\n\n{dependent_context[:5000]}\n\n{analysis[:3000]}{orig_context}"
                        if spark_logs:
                            fix_context += f"\n\n{spark_logs}"
                        fix_context += f"\n\nVerification issues: {issue_summary}"
                        # Local-module mirror — Opus must patch the MIRROR, not the customer source
                        if local_mirror_root_dst:
                            fix_context += (
                                "\n\n=== LOCAL MODULE SOURCE MIRROR ===\n"
                                f"the source tree (READ-ONLY, NEVER MODIFY): {local_mirror_root_orig}\n"
                                f"Mirror copy (writable, ALL patches go here): {local_mirror_root_dst}\n"
                                f"- sys.path already loads local modules from {local_mirror_root_dst}.\n"
                                "- If you need to patch a local .py file (syntax fix, comment out Databricks-only code),\n"
                                f"  rewrite the path to point at the mirror, replacing the '{local_mirror_root_orig}' prefix with '{local_mirror_root_dst}'.\n"
                                f"- NEVER write to any path that starts with {local_mirror_root_orig}.\n"
                            )
                        # Include evaluator's fix suggestion
                        eval_fix = locals().get('eval_fix_suggestion', '')
                        if eval_fix:
                            fix_context += f"\n\nEvaluator suggested fix: {eval_fix}"
                        current_code = await call_fix(current_code, _compact_output_for_llm(output) or issue_summary, executed_code, attempt + 1, extra_context=fix_context, session=session, log_fn=log.log, notebook_path=notebook_path)
                        current_code = _fix_path_replace_idempotency(current_code)
                        current_code = _detect_table_to_path_regression(source, current_code)
                        current_code = _detect_path_returning_to_identifier_regression(source, current_code)
                        # OCI namespace authoritative from source/mapping — never let
                        # Opus change it (e.g. <DATALAKE_NAMESPACE> -> <WORKSPACE_NAMESPACE>).
                        current_code = _apply_namespace_from_mapping(current_code)
                        total_tokens += 500
                    except Exception as _fix_e:
                        # Narrowed from a bare `except` (which also swallowed
                        # KeyboardInterrupt / SystemExit). Surface what failed
                        # instead of silently breaking the retry loop.
                        log.log(f"    [fix] fix/transform step errored "
                                f"({type(_fix_e).__name__}: {_fix_e}); stopping retries for this cell")
                        break
                else:
                    # MAX_CELL_RETRIES exhausted. Branch on whether the
                    # original failure was a data issue vs a code error.
                    if original_failure_was_empty_data:
                        # Data-side failure (table/data unavailable for the
                        # specified dates). Code is correctly migrated; we
                        # just couldn't VALIDATE execution. Set
                        # task_validation_stop so subsequent cells in this
                        # task switch to code-only mode (saved notebook is
                        # complete; execution isn't re-attempted).
                        log.log(f"Cell {i}/{total_cells}: DATA_UNAVAILABLE_AT_MIGRATION after {MAX_CELL_RETRIES} attempts — switching task to code-only continuation for remaining cells")
                        cell_results.append({
                            "cell": i, "status": "DATA_UNAVAILABLE_AT_MIGRATION",
                            "attempts": MAX_CELL_RETRIES,
                            "code": current_code[:1000],
                            "output": (output or "")[:2000],
                            "recovery_attempted": recovery_attempted,
                        })
                        cells_data_unavailable += 1
                        task_validation_stop = True
                        migration_notes.append(
                            f"Cell {i}: DATA_UNAVAILABLE_AT_MIGRATION — upstream data not available for "
                            f"the cell's date filter; code is migrated but execution wasn't validated. "
                            f"Subsequent cells in this task are migrated code-only."
                        )
                        cell_summary = (cell_plan.get("description", "") or
                                        current_code.strip()[:100].replace("\n", " "))[:200]
                        _cell_history.append({
                            "index":          len(_cell_history),
                            "notebook_path":  notebook_path,
                            "cell_idx":       i,
                            "summary":        f"DATA_UNAVAILABLE: {cell_summary}",
                            "final_code":     current_code[:3000],
                            "output_preview": (output or "")[:300],
                            "status":         "data_unavailable",
                            "is_child":       False,
                            "last_note":      _current_cell_notes[-1] if _current_cell_notes else "",
                        })
                        _current_cell_notes.clear()
                    else:
                        log.log(f"Cell {i}/{total_cells}: FAILED after {MAX_CELL_RETRIES} attempts - FAILING JOB")
                        cell_results.append({"cell": i, "status": "error", "attempts": MAX_CELL_RETRIES, "code": current_code[:1000], "output": (output or "")[:2000]})
                        cells_failed += 1
                        job_should_stop = True  # fail the job after retries on a single cell
                        # Record failure in history
                        cell_summary = (cell_plan.get("description", "") or
                                        current_code.strip()[:100].replace("\n", " "))[:200]
                        _cell_history.append({
                            "index":          len(_cell_history),
                            "notebook_path":  notebook_path,
                            "cell_idx":       i,
                            "summary":        cell_summary,
                            "final_code":     current_code[:3000],
                            "output_preview": (output or "")[:300],
                            "status":         "error",
                            "is_child":       False,
                            "last_note":      _current_cell_notes[-1] if _current_cell_notes else "",
                        })
                        _current_cell_notes.clear()

            if job_should_stop:
                # Real code error: stop executing further cells, but still apply
                # deterministic code-only migration (S3→OCI paths, workspace path
                # rewrites, missing imports, regression detectors) to the remaining
                # cells so the saved artifact is fully translated rather than half
                # Databricks / half AIDP. Opus is NOT called on these cells —
                # verification is impossible after a hard failure, so we keep
                # things cheap/fast and rely on deterministic transforms only.
                # Operators can hand-polish anything tricky after data is restored.
                remaining = total_cells - i - 1
                log.log(f"JOB FAILED: stopping execution, code-only migrating {remaining} remaining cells (no Opus, no execution)")
                for j in range(i + 1, total_cells):
                    _orig_cell = cells[j]
                    _new_cell = dict(_orig_cell)
                    if _orig_cell.get("cell_type") == "code":
                        _src = "".join(_orig_cell.get("source", []))
                        if _src.strip():
                            try:
                                _migrated = _apply_s3_translations(_src)
                                _migrated = _rewrite_internal_paths(_migrated, MIGRATED_BASE, notebook_path, dep_path_map)
                                _migrated = _fix_path_replace_idempotency(_migrated)
                                _migrated = _detect_table_to_path_regression(_src, _migrated)
                                _migrated = _detect_path_returning_to_identifier_regression(_src, _migrated)
                                _migrated = _add_missing_imports(_migrated)
                                _new_cell["source"] = [_migrated]
                            except Exception as _e:
                                # On any transform error, keep the original cell as-is —
                                # safer than emitting partially-mangled code.
                                log.log(f"  [post-fail] Cell {j}: transform error: {str(_e)[:200]} — preserving as-is")
                    cell_results.append({"cell": j, "status": "blocked_code_only"})
                    migrated_cells.append(_new_cell)
                break

        cells_skipped = skip_markdown + skip_raw + skip_empty
        # Task outcome — precedence: FAIL > PARTIAL > PARTIAL_DATA_UNAVAILABLE
        # > PASS_DATA_SUBSTITUTED > PASS. The new data-flow outcomes are
        # distinct from the existing PASS/PARTIAL/FAIL so downstream tooling
        # can tell "code-validated end-to-end" from "validated with date
        # substitution" from "validated up to a data-availability cliff."
        if cells_ok == 0 and cells_data_substituted == 0:
            overall = "FAIL"
        elif cells_failed > 0:
            overall = "PARTIAL"
        elif cells_data_unavailable > 0 or cells_not_validated > 0:
            overall = "PARTIAL_DATA_UNAVAILABLE"
        elif cells_data_substituted > 0:
            overall = "PASS_DATA_SUBSTITUTED"
        else:
            overall = "PASS"

        # Cell count validation: migrated notebook should have >= original cells
        if len(migrated_cells) < total_cells:
            log.log(f"WARNING: migrated notebook has {len(migrated_cells)} cells, original has {total_cells} — possible cell loss")

        # ── Optional acceptance contract (post-cell-pass drain check) ──
        # Pattern adapted from prior internal pattern
        # aidp-batch-stream-acceptance skill. Runs only when (a) caller passed
        # a contract dict, (b) all cells passed, and (c) execution is live
        # (run_all=True). Maintains back-compat: absent contract = no-op.
        contract_result = None
        if acceptance_contract_dict and run_all and overall == "PASS":
            try:
                from acceptance_contract import (
                    AcceptanceContract, run_contract, ContractParseError,
                )
                contract = AcceptanceContract.from_dict(acceptance_contract_dict)
            except ContractParseError as _pe:
                log.log(f"[acceptance] contract parse error: {_pe} — skipping (treating as PASS)")
                contract = None
            except ImportError as _ie:
                log.log(f"[acceptance] module not available ({_ie}) — skipping")
                contract = None

            if contract is not None:
                # Adapter: run pending_count_sql via the same AIDPSession the
                # notebook used. Returns first column of first row as int.
                async def _probe_sql(sql: str) -> int:
                    _wrapper = (
                        "import json as _ac_json\n"
                        f"_ac_rows = spark.sql({sql!r}).collect()\n"
                        "if not _ac_rows: print(_ac_json.dumps({'count': 0}))\n"
                        "else: print(_ac_json.dumps({'count': int(list(_ac_rows[0].asDict().values())[0])}))\n"
                    )
                    _ac_res = await session.execute(_wrapper, timeout=120)
                    for _o in _ac_res.get("outputs", []):
                        if isinstance(_o, dict) and _o.get("text"):
                            for _line in _o["text"].splitlines():
                                _line = _line.strip()
                                if _line.startswith("{") and "count" in _line:
                                    import json as _jj
                                    return int(_jj.loads(_line)["count"])
                    raise RuntimeError("acceptance probe SQL returned no parseable output")

                log.log(f"[acceptance] contract starting for {task_key}")
                contract_result = await run_contract(contract, _probe_sql, log_fn=lambda m: log.log(f"[acceptance] {m}"))
                if not contract_result.passed:
                    # Demote PASS -> ACCEPTANCE_CONTRACT_VIOLATED
                    overall = "ACCEPTANCE_CONTRACT_VIOLATED"
                    log.log(f"[acceptance] {contract_result.reason}")
                    log.log(f"[acceptance] diagnostic: {contract_result.diagnostic}")
                else:
                    log.log(f"[acceptance] {contract_result.reason}")

        log.log(
            f"RESULT: {overall} | OK:{cells_ok} Fail:{cells_failed} "
            f"Skip:{cells_skipped} (md:{skip_markdown} raw:{skip_raw} empty:{skip_empty}) "
            f"Fix:{cells_fixed} "
            f"DataSub:{cells_data_substituted} DataUnavail:{cells_data_unavailable} "
            f"NotValidated:{cells_not_validated}"
            + (f" | acceptance: {contract_result.status} ({contract_result.attempts} attempts)" if contract_result else "")
        )

        # ── Build reports ──
        test_report = f"""# Test Report: {notebook_path}
## Job: {job_name} | Task: {task_key}
## Result: **{overall}**

| Metric | Count |
|--------|-------|
| Total cells | {total_cells} |
| Code cells executed | {cells_ok} OK, {cells_failed} failed |
| Auto-fixed | {cells_fixed} |
| Data-substituted (validated with dates from upstream table) | {cells_data_substituted} |
| Data-unavailable at migration | {cells_data_unavailable} |
| Migrated code-only (after data-unavailable) | {cells_not_validated} |
| Skipped: markdown | {skip_markdown} |
| Skipped: raw | {skip_raw} |
| Skipped: empty | {skip_empty} |
| Parameters | {json.dumps(parameters)} |
| Dependent notebooks | {len(deps)} ({len(downloaded_deps)} loaded) |
| Claude tokens | {total_tokens:,} |
| Date | {datetime.now().isoformat()} |

"""
        if monitor_decisions:
            test_report += "## Monitor Decisions\n"
            for md in monitor_decisions:
                test_report += f"- Cell {md['cell']}: **{md['verdict']}** - {md['diagnosis']}\n"
            test_report += "\n"

        # ── Write-redirect summary (tool-only writes that never touched production) ──
        _wr = get_write_redirect_summary()
        if _wr["tables"] or _wr["paths"]:
            test_report += "## Write Redirects (Tool-Only)\n"
            test_report += (
                "Every write/INSERT/UPDATE/DELETE/MERGE/CREATE/DROP during tool execution was "
                "redirected to a tool-owned destination so customer production data was never touched. "
                "The saved notebook KEEPS the original paths/tables — these redirects "
                "applied to cluster execution only.\n\n"
            )
            if _wr["tables"]:
                test_report += "**Tables redirected:**\n\n"
                test_report += "| Original | Redirected | Source operation |\n"
                test_report += "|---|---|---|\n"
                # Use the audit log for source-op info; dedupe by redirected target
                _seen = set()
                for entry in _wr["log"]:
                    if entry["kind"] != "table" or entry["redirected"] in _seen:
                        continue
                    _seen.add(entry["redirected"])
                    test_report += f"| `{entry['original']}` | `{entry['redirected']}` | {entry['op']} |\n"
                test_report += "\n"
            if _wr["paths"]:
                test_report += "**Paths redirected:**\n\n"
                test_report += "| Original | Redirected | Source operation |\n"
                test_report += "|---|---|---|\n"
                _seen = set()
                for entry in _wr["log"]:
                    if entry["kind"] != "path" or entry["redirected"] in _seen:
                        continue
                    _seen.add(entry["redirected"])
                    test_report += f"| `{entry['original']}` | `{entry['redirected']}` | {entry['op']} |\n"
                test_report += "\n"
            if _wr["collisions"]:
                test_report += "**Redirect collisions — review:**\n\n"
                for c in _wr["collisions"]:
                    test_report += f"- {c['kind']}: `{c['redirected']}` ← {c['originals']}. {c['note']}\n"
                test_report += "\n"

        test_report += "## Cell Results\n"
        for cr in cell_results:
            ci = cr["cell"]
            st = cr.get("status", "?")
            reason = cr.get("reason", "")

            if st == "skipped":
                test_report += f"\n### Cell {ci} - SKIPPED ({reason})\n"
                continue
            if st == "blocked":
                test_report += f"\n### Cell {ci} - BLOCKED\n"
                continue

            test_report += f"\n### Cell {ci} - {st.upper()}\n"
            if cr.get("note"):
                test_report += f"\n> **Note:** {cr['note']}\n"
            if cr.get("fixed"):
                test_report += f"*Auto-fixed on attempt {cr.get('attempts')}*\n"
            if cr.get("code"):
                test_report += f"\n```python\n{cr['code']}\n```\n"
            if cr.get("output"):
                out = cr['output']
                if len(out) > 1000:
                    out = out[:500] + f"\n\n... [{len(out) - 1000:,} chars truncated] ...\n\n" + out[-500:]
                test_report += f"\n**Output:**\n```\n{out}\n```\n"
            if cr.get("original_output"):
                test_report += f"\n**Original output:** `{cr['original_output'][:300]}`\n"
            if cr.get("output_match"):
                test_report += f"**Match:** {cr['output_match']}\n"
            if cr.get("monitor"):
                test_report += f"\n**Monitor:** {cr['monitor']}\n"

        # Save and upload artifacts
        try:
            analysis_path = os.path.join(tmpdir, "analysis_report.md")
            with open(analysis_path, 'w') as f:
                f.write(f"# Analysis: {notebook_path}\n\n{analysis}")

            migration_report_path = os.path.join(tmpdir, "migration_report.md")
            with open(migration_report_path, 'w') as f:
                notes = "\n".join(f"- {n}" for n in migration_notes)
                f.write(f"# Migration Report: {notebook_path}\n\n## Changes\n{notes}\n")

            test_report_path = os.path.join(tmpdir, "test_report.md")
            with open(test_report_path, 'w') as f:
                f.write(test_report)

            final_nb_path = os.path.join(tmpdir, "final.ipynb")
            final_nb = copy.deepcopy(nb)
            # (AIDP perf-config cell is intentionally NOT prepended to migrated
            # notebooks — we don't add any spark.conf configuration cell.)
            # Strip cell outputs before saving — execution outputs (e.g. Optuna trial
            # logs) can bloat the notebook to 100+ MB, causing multi-hour uploads via
            # base64 chunks over WebSocket.  The migrated notebook only needs correct code;
            # the customer will produce their own outputs when they run it.
            stripped_cells = []
            for cell in migrated_cells:
                cell = dict(cell)
                cell["outputs"] = []
                cell["execution_count"] = None
                # Strip internal fix-log comments — these are migration debug
                # artifacts and should not appear in the delivered notebook.
                # Also strip any AIDP_DATA_RECOVERY override block (defense-
                # in-depth: it should never reach migrated_cells[]["source"]
                # in the first place, but if a code path leaked it, drop it
                # here so the customer never sees test-only substitutions).
                if cell.get("cell_type") == "code":
                    src = "".join(cell.get("source", []))
                    src = src.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
                    src = _strip_data_recovery_block(src)
                    cell["source"] = [src]
                stripped_cells.append(cell)
            # Ensure aidp_compat import is in first code cell if dbutils is used anywhere
            stripped_cells = _ensure_dbutils_import(stripped_cells)
            # Inject inlined run_job_and_wait helper if any cell uses it (so the
            # migrated notebook is self-contained, no external staging-folder dependency).
            stripped_cells = _ensure_invoke_job_helper(stripped_cells)

            # Artifact-time sys.path cell — only added when local modules were
            # detected. Hardcodes the absolute mirror paths so the cloned/
            # deployed workflow can resolve `from modules.config import ...`
            # without any external setup. Mirror lives under the migration
            # output dir which the customer retains.
            prepend_cells = []  # no AIDP perf-config cell prepended
            if local_module_src_roots:
                syspath_src = (
                    "# AIDP local-module sys.path — added automatically by migration tool.\n"
                    "# Mirrors the customer's in-tree .py source so package-style imports\n"
                    "# (e.g. `from modules.config import system_config`) resolve when this\n"
                    "# notebook is cloned/executed.\n"
                    "import sys as _sys\n"
                    + "".join(
                        f"if {root!r} not in _sys.path: _sys.path.insert(0, {root!r})\n"
                        for root in local_module_src_roots
                    )
                )
                local_syspath_cell = {
                    "cell_type": "code",
                    "source": [syspath_src],
                    "outputs": [],
                    "metadata": {},
                    "execution_count": None,
                }
                prepend_cells.append(local_syspath_cell)
            final_nb["cells"] = prepend_cells + stripped_cells
            with open(final_nb_path, 'w') as f:
                json.dump(final_nb, f, indent=1)

            log_path = os.path.join(tmpdir, "execution_log.txt")
            with open(log_path, 'w') as f:
                f.write(log.text())

            log.log("Uploading artifacts...")

            # Save migrated notebook to MIGRATED_BASE so child deps can find it
            nb_norm = normalize_nb_path(notebook_path)
            if not nb_norm.endswith(".ipynb"):
                nb_norm += ".ipynb"
            migrated_nb_path = f"{MIGRATED_BASE}/{nb_norm}"

            # Cache notebook content in memory so _inline_child_notebook can read it
            # without relying on FUSE (os.path.exists can fail even seconds after upload).
            with open(final_nb_path, 'r') as f:
                _notebook_content_cache[migrated_nb_path] = f.read()

            # Upload to both the job output dir AND the migrated-notebooks dir
            upload_targets = [
                (final_nb_path, migrated_nb_path),  # migrated notebook for dep resolution
            ]

            UPLOAD_MAX_RETRIES = 5
            UPLOAD_DELAYS = [10, 20, 30, 45, 60]  # seconds between retries

            for upload_attempt in range(UPLOAD_MAX_RETRIES):
                try:
                    await session.execute(f"import os; os.makedirs('{output_dir}', exist_ok=True)", timeout=30)
                    await session.execute(f"import os; os.makedirs(os.path.dirname('{migrated_nb_path}'), exist_ok=True)", timeout=30)

                    for local_file, remote_name in [
                        (analysis_path, "analysis_report.md"),
                        (migration_report_path, "migration_report.md"),
                        (test_report_path, "test_report.md"),
                        (final_nb_path, "final.ipynb"),
                        (log_path, "execution_log.txt"),
                    ]:
                        with open(local_file, 'r') as f:
                            content_str = f.read()
                        b64 = base64.b64encode(content_str.encode('utf-8')).decode('ascii')
                        remote_path = f"{output_dir}/{remote_name}"

                        CHUNK = 45000
                        if len(b64) <= CHUNK:
                            await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
""", timeout=60)
                        else:
                            chunks = [b64[i:i+CHUNK] for i in range(0, len(b64), CHUNK)]
                            await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{chunks[0]}'))
""", timeout=60)
                            for chunk in chunks[1:]:
                                await session.execute(f"""
import base64, builtins
with builtins.open('{remote_path}', 'ab') as f:
    f.write(base64.b64decode('{chunk}'))
""", timeout=60)

                        log.log(f"  Uploaded: {remote_name} ({len(content_str):,} chars)")
                        if len(content_str) > 40000:
                            if not await verify_upload(session, remote_path, log_fn=log.log):
                                raise RuntimeError(f"FUSE verification failed: {remote_path}")

                    # Upload migrated notebook to MIGRATED_BASE for dep resolution
                    for local_file, remote_target in upload_targets:
                        with open(local_file, 'r') as f:
                            content_str = f.read()
                        b64 = base64.b64encode(content_str.encode('utf-8')).decode('ascii')
                        CHUNK = 45000
                        if len(b64) <= CHUNK:
                            await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_target}'), exist_ok=True)
with builtins.open('{remote_target}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
""", timeout=60)
                        else:
                            chunks = [b64[j:j+CHUNK] for j in range(0, len(b64), CHUNK)]
                            await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_target}'), exist_ok=True)
with builtins.open('{remote_target}', 'wb') as f:
    f.write(base64.b64decode('{chunks[0]}'))
""", timeout=60)
                            for chunk in chunks[1:]:
                                await session.execute(f"""
import base64, builtins
with builtins.open('{remote_target}', 'ab') as f:
    f.write(base64.b64decode('{chunk}'))
""", timeout=60)
                        log.log(f"  Uploaded migrated notebook to: {remote_target}")
                        if not await verify_upload(session, remote_target, log_fn=log.log):
                            raise RuntimeError(f"FUSE verification failed: {remote_target}")

                    log.log("Upload complete")
                    upload_ok = True
                    break  # success — exit retry loop

                except Exception as upload_err:
                    delay = UPLOAD_DELAYS[min(upload_attempt, len(UPLOAD_DELAYS) - 1)]
                    if upload_attempt < UPLOAD_MAX_RETRIES - 1:
                        log.log(f"Upload failed (attempt {upload_attempt + 1}/{UPLOAD_MAX_RETRIES}): {str(upload_err)[:200]} — retrying in {delay}s...")
                        await asyncio.sleep(delay)
                    else:
                        upload_ok = False
                        log.log(f"Upload FAILED after {UPLOAD_MAX_RETRIES} attempts: {str(upload_err)[:200]}")

        except Exception as outer_err:
            upload_ok = False
            log.log(f"Upload setup failed: {str(outer_err)[:200]}")

        # Fallback: upload to OCI Object Storage if workspace write failed
        if not upload_ok:
            log.log("Workspace upload failed — attempting OCI Object Storage backup...")
            try:
                ocs_files = [
                    (analysis_path, "analysis_report.md"),
                    (migration_report_path, "migration_report.md"),
                    (test_report_path, "test_report.md"),
                    (final_nb_path, "final.ipynb"),
                    (log_path, "execution_log.txt"),
                ]
                ocs_ok = fallback_upload_to_ocs(
                    ocs_files, output_dir, migrated_nb_path, log_fn=log.log)
                if ocs_ok:
                    log.log("OCI Object Storage backup succeeded")
                    upload_ok = True  # treat as success so registry gets updated
            except Exception as ocs_err:
                log.log(f"OCI Object Storage backup also failed: {str(ocs_err)[:200]}")

        # Update migration registry only if upload succeeded
        if upload_ok:
            try:
                await registry_update(session, notebook_path, migrated_nb_path, job_name, overall)
                log.log(f"  Registry updated: {registry_key(notebook_path)} -> {migrated_nb_path}")
            except Exception as reg_err:
                log.log(f"  Registry update failed (non-fatal): {str(reg_err)[:200]}")

        log.log(f"DONE: {overall}")
        return {
            "path": notebook_path, "task": task_key, "job": job_name,
            "status": overall, "ok": cells_ok, "failed": cells_failed,
            "skipped": cells_skipped, "fixed": cells_fixed,
            "skip_markdown": skip_markdown, "skip_raw": skip_raw, "skip_empty": skip_empty,
            "total_cells": total_cells, "code_cells": code_cells,
            "tokens": total_tokens, "job_should_stop": job_should_stop,
            "monitor_decisions": monitor_decisions,
            "cell_results": [cr for cr in cell_results if cr.get("note")],
        }

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Dependency Scanner & Recursive Migration ────────────────────────

# MIGRATED_BASE is set per-job in process_job(): {OUTPUT_BASE}/{job_name}/notebooks
MIGRATED_BASE = f"{OUTPUT_BASE}/notebooks"  # default, overridden per job
# Track what's been migrated: path -> migrated_path
_migration_cache: Dict[str, str] = {}

# Manifest-driven dep resolution override.
# Populated by job_migrate_from_workflow.py at startup from the validated manifest:
#   { parent_notebook_path : [ resolved_full_child_paths ] }
# Used as a fallback by _resolve_relative_dep when naive parent_dir+relative
# resolution produces a path that does not exist on the cluster (e.g., source
# notebook has a stale `%run ./X` whose actual file is in a sibling folder).
# Empty dict = override disabled (legacy behavior).
_DEP_RESOLUTION: Dict[str, List[str]] = {}


def set_dep_resolution(mapping: Dict[str, List[str]]) -> None:
    """Install a parent->[resolved_children] map from a validated manifest.
    Replaces any prior map. Pass {} to disable."""
    global _DEP_RESOLUTION
    _DEP_RESOLUTION = mapping or {}


def _resolve_relative_dep(parent_path: str, naive_resolved: str) -> str:
    """Return the manifest-listed path for `naive_resolved` if (a) the parent
    has a manifest entry, (b) `naive_resolved` is missing from the manifest's
    children for that parent, and (c) exactly one manifest child has the
    same basename. Otherwise return `naive_resolved` unchanged.

    The override only kicks in when the source notebook's `%run ./X` resolves
    to a path that doesn't match anything the manifest validated for that
    parent. Avoids touching the 95% happy-path."""
    if not _DEP_RESOLUTION:
        return naive_resolved
    children = _DEP_RESOLUTION.get(parent_path)
    if not children:
        return naive_resolved
    # Exact match — naive resolution is correct, no override
    if naive_resolved in children:
        return naive_resolved
    # Fall back to basename match against manifest children for this parent
    bn = os.path.basename(naive_resolved)
    matches = [c for c in children if os.path.basename(c) == bn]
    if len(matches) == 1:
        tprint(f"[manifest-resolved] {naive_resolved} -> {matches[0]} (parent={parent_path})")
        return matches[0]
    # 0 or 2+ matches — leave unchanged so existing failure paths trigger
    return naive_resolved

# ── Databricks job_id → AIDP job_key mapping (per-job) ────────────────
# Populated from manifest field "db_to_aidp_job_map" at job start.
# Used by _normalize_job_triggers to rewrite Databricks job-trigger calls
# (job_call/job_calling/call_job_internal/requests.post run-now) into the
# AIDP equivalent: run_job_and_wait("<aidp_uuid>", <converted_params>).
_db_to_aidp_job_map: Dict[str, str] = {}
# Set of int job_ids that were referenced in code but missing from the map.
# Surfaced in JOB_REPORT.md so the user can fill them in and re-run.
_unmapped_db_job_ids: set = set()
# In-memory cache of migrated notebook content: migrated_path -> notebook JSON string.
# Bypasses FUSE entirely for _inline_child_notebook — files written to /Workspace via
# builtins.open() can become invisible to os.path.exists() within seconds on AIDP's
# NFS/FUSE mount (verified: file uploaded and confirmed at 12:53:28, os.path.exists
# returned False at 12:53:52 — same kernel, same session, 24 seconds apart).
_notebook_content_cache: Dict[str, str] = {}

# ─── Migration Registry (cross-job skip) ─────────────────────────────
# Per-job registry file at {OUTPUT_BASE}/{job_name}/migration_registry.json.
# Each job writes only to its own file — no cross-job write conflicts.
# Lookup scans ALL job directories to find previously migrated notebooks.
# Key = normalized source notebook path, Value = {migrated_path, job_name, status, timestamp}

_registry_cache: Dict[str, Dict] = {}  # job_name -> registry dict (for writes)
_all_registries_loaded = False  # True once we've scanned all job dirs for lookup
_all_registries_merged: Dict[str, Dict] = {}  # merged view of all registries (for reads)


def _registry_path_for_job(job_name: str) -> str:
    """Return the registry file path for a given job."""
    return f"{OUTPUT_BASE}/{job_name}/migration_registry.json"


async def _load_one_registry(session: "AIDPSession", registry_path: str) -> Dict:
    """Load a single registry JSON file from cluster. Returns dict (empty if not found)."""
    try:
        result = await session.execute(f"""
import json, builtins, os, time
_p = '{registry_path}'
try:
    with builtins.open(_p) as f:
        _data = json.load(f)
    print(json.dumps(_data))
except FileNotFoundError:
    print('{{}}')
except Exception as _e:
    # Corrupt/partial JSON (e.g. interrupted save). DON'T return {{}} silently —
    # that would disable skip-migrated AND let save_registry overwrite the file
    # with a smaller dict, losing entries. Quarantine the corrupt file so it's
    # preserved and the original path is free for a clean rewrite.
    _q = _p + '.corrupt.' + str(int(time.time()))
    try:
        os.rename(_p, _q)
        print(json.dumps({{"__registry_corrupt__": _q, "error": str(_e)[:200]}}))
    except Exception as _e2:
        print(json.dumps({{"__registry_corrupt__": _p, "error": str(_e2)[:200]}}))
""", timeout=30)
        raw = format_outputs(result.get("outputs", []))
        from context_tools import _unwrap_aidp_text
        raw = _unwrap_aidp_text(raw)
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and "__registry_corrupt__" in parsed:
            tprint(f"  [registry] ERROR: corrupt registry quarantined -> "
                   f"{parsed['__registry_corrupt__']} ({parsed.get('error')}). "
                   f"Starting fresh; skip-migrated disabled this run, no data overwritten.")
            return {}
        return parsed
    except Exception as e:
        tprint(f"  [registry] WARNING: failed to read registry {registry_path}: {e!r}")
        return {}


async def _load_all_registries(session: "AIDPSession") -> Dict:
    """Scan all {OUTPUT_BASE}/*/migration_registry.json and merge into one dict.
    Called once per run on first registry_lookup."""
    global _all_registries_loaded, _all_registries_merged
    if _all_registries_loaded:
        return _all_registries_merged
    try:
        # List job directories under OUTPUT_BASE
        result = await session.execute(f"""
import os, json
base = '{OUTPUT_BASE}'
dirs = []
if os.path.isdir(base):
    for d in os.listdir(base):
        rp = os.path.join(base, d, 'migration_registry.json')
        if os.path.isfile(rp):
            dirs.append(rp)
print(json.dumps(dirs))
""", timeout=30)
        raw = format_outputs(result.get("outputs", []))
        from context_tools import _unwrap_aidp_text
        raw = _unwrap_aidp_text(raw)
        registry_files = json.loads(raw)
        tprint(f"  [registry] Found {len(registry_files)} registry file(s) across jobs")
    except Exception as e:
        tprint(f"  [registry] WARNING: failed to list registries: {e!r}")
        registry_files = []

    # Load all registry files in parallel
    regs = await asyncio.gather(*[_load_one_registry(session, rp) for rp in registry_files])

    # Merge with timestamp-aware conflict resolution (most recent wins)
    merged = {}
    for reg in regs:
        for path, entry in reg.items():
            existing = merged.get(path)
            if not existing or entry.get("timestamp", "") >= existing.get("timestamp", ""):
                merged[path] = entry

    _all_registries_merged = merged
    _all_registries_loaded = True
    return _all_registries_merged


async def load_registry(session: "AIDPSession", job_name: str) -> Dict:
    """Load the current job's registry (for writing). Cached per job_name."""
    if job_name in _registry_cache:
        return _registry_cache[job_name]
    rp = _registry_path_for_job(job_name)
    reg = await _load_one_registry(session, rp)
    _registry_cache[job_name] = reg
    return reg


async def save_registry(session: "AIDPSession", job_name: str, registry: Dict) -> None:
    """Write the current job's registry file."""
    _registry_cache[job_name] = registry
    rp = _registry_path_for_job(job_name)
    try:
        registry_json = json.dumps(registry, indent=1)
        b64 = base64.b64encode(registry_json.encode('utf-8')).decode('ascii')
        CHUNK = 45000
        if len(b64) <= CHUNK:
            await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{rp}'), exist_ok=True)
with builtins.open('{rp}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
""", timeout=30)
        else:
            chunks = [b64[i:i+CHUNK] for i in range(0, len(b64), CHUNK)]
            await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{rp}'), exist_ok=True)
with builtins.open('{rp}', 'wb') as f:
    f.write(base64.b64decode('{chunks[0]}'))
""", timeout=30)
            for chunk in chunks[1:]:
                await session.execute(f"""
import base64, builtins
with builtins.open('{rp}', 'ab') as f:
    f.write(base64.b64decode('{chunk}'))
""", timeout=30)
        tprint(f"  [registry] Saved {len(registry)} entries to {rp}")
    except Exception as e:
        tprint(f"  [registry] WARNING: failed to save registry: {e!r}")


async def registry_update(session: "AIDPSession", original_path: str,
                           migrated_path: str, job_name: str, status: str) -> None:
    """Add/update an entry in the current job's registry and persist it.

    `original_path` is the FULL original notebook path (e.g.
    "/Workspace/Users/<email>/foo.ipynb"). It is canonicalized via
    registry_key() before being stored so every entry has the same shape
    regardless of which job migrated it. Callers may also pass legacy
    JOB_ROOT-relative or normalize_nb_path-stripped forms; registry_key()
    re-expands them. Always writes — progress is recorded regardless of
    --skip-migrated."""
    key = registry_key(original_path)
    registry = await load_registry(session, job_name)
    registry[key] = {
        "migrated_path": migrated_path,
        "job_name": job_name,
        "status": status,
        "timestamp": datetime.now().isoformat(),
    }
    await save_registry(session, job_name, registry)
    # Also update the merged view so subsequent lookups in the same run see it
    _all_registries_merged[key] = registry[key]


async def registry_lookup(session: "AIDPSession", original_path: str) -> Optional[str]:
    """Check all job registries for a previously migrated notebook.

    `original_path` is canonicalized via registry_key() before lookup. To
    remain backward-compatible with older registries that stored entries
    under the legacy normalize_nb_path() form (JOB_ROOT-stripped, no
    /Workspace/ prefix), we fall back to that form if the canonical
    lookup misses.

    Returns the migrated_path if found and still VALID on cluster, else
    None. Only active when --skip-migrated is set."""
    if not SKIP_MIGRATED:
        return None
    merged = await _load_all_registries(session)
    # Try canonical full-path key first
    canonical = registry_key(original_path)
    entry = merged.get(canonical)
    if not entry:
        # Backward-compat: try the legacy JOB_ROOT-stripped form
        legacy = normalize_nb_path(original_path)
        entry = merged.get(legacy)
    if not entry:
        return None
    migrated_path = entry["migrated_path"]
    # Validate the file still exists and is valid on cluster
    try:
        check_result = await session.execute(f"""
import os, json
p = '{migrated_path}'
if os.path.exists(p):
    try:
        with open(p) as _f:
            nb = json.load(_f)
        if nb.get('cells') and len(nb['cells']) > 0:
            print('VALID')
        else:
            print('EMPTY')
    except:
        print('CORRUPT')
else:
    print('MISSING')
""", timeout=15)
        check_output = format_outputs(check_result.get("outputs", []))
        if "VALID" in check_output:
            return migrated_path
    except Exception:
        pass
    return None


def scan_for_run_deps(notebook_content: bytes) -> List[Tuple[str, Dict[str, str]]]:
    """Scan notebook for %run, dbutils.notebook.run, and oidlUtils.notebook.run dependencies.
    Returns list of (notebook_path, parameters_dict)."""
    try:
        nb = json.loads(notebook_content.decode('utf-8', errors='replace'))
    except:
        return []

    from context_tools import (notebook_default_language, detect_cell_language,
                               strip_comment_lines)
    _default_lang = notebook_default_language(nb)

    deps = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        raw_source = "".join(cell.get("source", []))
        # Filter out commented lines — commented-out %run / notebook.run() is dead
        # code. Use the cell's LANGUAGE-SPECIFIC comment syntax (Python/R '#',
        # Scala/Java '//', SQL '--') based on the notebook default + any per-cell
        # %magic, rather than blindly stripping every marker.
        _lang = detect_cell_language(raw_source, _default_lang)
        source = strip_comment_lines(raw_source, _lang)

        # %run /path $param1 $param2  (also matches relative: %run ../foo, %run ./foo)
        for match in re.finditer(r'%run\s+([^\n]+)', source):
            run_line = match.group(1).strip()
            if not run_line:
                continue
            # Handle quoted paths (e.g. %run "/path/with spaces/Utils")
            if run_line.startswith('"') or run_line.startswith("'"):
                q = run_line[0]
                end = run_line.find(q, 1)
                path = run_line[1:end] if end > 0 else run_line[1:].rstrip(q)
                rest = run_line[end+1:].strip() if end > 0 else ""
            else:
                parts = run_line.split()
                path = parts[0]
                rest = " ".join(parts[1:])
            # Extract $param references
            params = {}
            for p in rest.split():
                if p.startswith("$"):
                    params[p[1:]] = ""  # param name without $
            # Clean path
            path = re.sub(r'\$\w+', '', path).strip()
            if not path.endswith(".ipynb"):
                path += ".ipynb"
            deps.append((path, params))

        # dbutils.notebook.run("path", ...) or oidlUtils.notebook.run("path", ...)
        for match in re.finditer(r'(?:dbutils|oidlUtils)\.notebook\.run\s*\(\s*["\']([^"\']+)["\']', source):
            path = match.group(1)
            if not path.endswith(".ipynb"):
                path += ".ipynb"
            deps.append((path, {}))

    return deps


def normalize_nb_path(path: str) -> str:
    """Normalize a notebook path to workspace-relative form.
    Strips /Workspace/ prefix only — preserves the rest of the path as-is
    (Users/..., Repos/..., Git/..., or whatever subtree the customer uses).
    Replaces spaces with underscores for the *normalized* (key) form;
    actual file lookups via download_notebook() try both space and underscore
    variants, so the source filename can have either."""
    if path.startswith("/Workspace/"):
        path = path[len("/Workspace/"):]
    if path.startswith("/"):
        path = path[1:]
    # Normalized (canonical) form uses underscores. Lookups handle both.
    path = path.replace(" ", "_")
    # When migrating an exported job, collapse the export base so the migrated
    # tree mirrors the original user tree (Users/...) — and so a notebook
    # referenced as raw OR export-rooted normalizes to the SAME key (one cache
    # entry, one output location). No-op when EXPORT_BASE is unset.
    if EXPORT_BASE:
        eb = EXPORT_BASE.strip("/").replace(" ", "_")
        if eb and path.startswith(eb + "/"):
            path = path[len(eb) + 1:]
    return path


def registry_key(path: str) -> str:
    """Canonical registry-key form: FULL /Workspace/<full>/path.ipynb path.

    Unlike normalize_nb_path() which strips JOB_ROOT (producing different
    keys for the same notebook across jobs with different roots), this
    function preserves the full workspace path so every registry entry
    has one canonical, unambiguous key.

    Handles inputs in various shapes:
      - "/Workspace/Users/<email>/foo.ipynb"  → kept as-is
      - "Users/<email>/foo.ipynb"             → "/Workspace/Users/<email>/foo.ipynb"
      - "/Users/<email>/foo.ipynb"            → "/Workspace/Users/<email>/foo.ipynb"
      - JOB_ROOT-relative (e.g. ".bundle/x.ipynb") → re-prepended with
        "/Workspace/" + JOB_ROOT if JOB_ROOT is set; bare "/Workspace/x" otherwise.

    Spaces are normalised to underscores (AIDP convention — paths on the
    cluster use underscores even when the source filename has spaces).
    """
    if not path:
        return path
    p = path.strip().replace(" ", "_")
    # Collapse an export-base ref to its raw workspace form so a notebook
    # referenced as raw OR export-rooted shares one registry key. No-op when
    # EXPORT_BASE is unset.
    if EXPORT_BASE:
        eb = "/Workspace/" + EXPORT_BASE.strip("/").replace(" ", "_") + "/"
        if p.startswith(eb):
            p = "/Workspace/" + p[len(eb):]
    if p.startswith("/Workspace/"):
        return p
    # Strip leading slash so we can compose cleanly
    p = p.lstrip("/")
    # Workspace-relative path (e.g. "Users/alice@x.com/foo.ipynb") — just
    # add the /Workspace/ prefix
    if p.startswith("Users/"):
        return "/Workspace/" + p
    # Otherwise it might be JOB_ROOT-relative — re-attach JOB_ROOT if set
    # and the path doesn't already include it
    if JOB_ROOT:
        root = JOB_ROOT.strip("/").replace(" ", "_")
        if root and not p.startswith(root + "/") and p != root:
            return "/Workspace/" + root + "/" + p
    return "/Workspace/" + p


async def ensure_migrated(
    notebook_path: str,
    session: AIDPSession,
    depth: int = 0,
    job_name: str = "_deps",
) -> Optional[str]:
    """Recursively ensure a notebook and all its %run deps are migrated.
    Returns the migrated path, or None if migration failed.

    Flow:
    1. Check if already migrated (cache) → return cached path
    2. Download the notebook
    3. Scan for %run dependencies
    4. Recursively ensure_migrated for each dep (depth-first)
    5. Migrate this notebook (replacing %run paths with migrated paths)
    6. Test on cluster
    7. Save migrated version to MIGRATED_BASE
    8. Cache the result
    """
    normalized = normalize_nb_path(notebook_path)
    if not normalized.endswith(".ipynb"):
        normalized += ".ipynb"
    indent = "  " * depth

    # download_path: strip /Workspace/ only, keep JOB_ROOT (API needs full workspace-relative path)
    download_path = notebook_path.lstrip("/")
    if download_path.startswith("Workspace/"):
        download_path = download_path[len("Workspace/"):]
    if not download_path.endswith(".ipynb"):
        download_path += ".ipynb"
    # When migrating an exported job, ALWAYS source from under the export base —
    # never from the raw /Workspace/Users original a notebook may reference. The
    # exported copy is the migration's source of truth (and `normalized` has the
    # base already stripped). No-op when EXPORT_BASE is unset.
    if EXPORT_BASE:
        download_path = EXPORT_BASE.strip("/") + "/" + normalized

    # The migrated path is where the notebook will be saved
    migrated_path = f"{MIGRATED_BASE}/{normalized}"

    # Check cache
    if normalized in _migration_cache:
        tprint(f"{indent}[dep] Already migrated: {normalized}")
        return _migration_cache[normalized]

    # Check migration registry (cross-job) — only when skipping already-migrated.
    # With --no-skip-migrated we FORCE re-migration, so ignore prior results
    # (the in-memory _migration_cache above still dedups within this run).
    if SKIP_MIGRATED:
        registry_hit = await registry_lookup(session, notebook_path)
        if registry_hit:
            tprint(f"{indent}[dep] Found in migration registry: {normalized} -> {registry_hit}")
            _migration_cache[normalized] = registry_hit
            return registry_hit

    # Check if already migrated on workspace (brief lock for cluster I/O only)
    check_path = migrated_path
    try:
        # Brief lock: just the exists check on cluster
        check_result = await session.execute(f"""
import os, json
p = '{check_path}'
if os.path.exists(p):
    try:
        with open(p) as _f:
            nb = json.load(_f)
        if nb.get('cells') and len(nb['cells']) > 0:
            print('VALID')
        else:
            print('EMPTY')
    except:
        print('CORRUPT')
else:
    print('MISSING')
""", timeout=15)
        check_output = format_outputs(check_result.get("outputs", []))
        if "VALID" in check_output and SKIP_MIGRATED:
            tprint(f"{indent}[dep] Found existing migration: {check_path}")
            _migration_cache[normalized] = check_path
            return check_path
        elif "CORRUPT" in check_output or "EMPTY" in check_output:
            tprint(f"{indent}[dep] Found CORRUPT migration, will re-migrate: {check_path}")
            # Own try: a failed remove (perm / FUSE) must be surfaced, otherwise
            # the stale file persists and the next iteration loops on it.
            try:
                await session.execute(f"import os; os.remove('{check_path}')", timeout=10)
            except Exception as _rm_e:
                tprint(f"{indent}[dep] WARNING: could not remove corrupt migration "
                       f"{check_path}: {_rm_e!r} — may re-loop until cleared manually")
    except Exception as _chk_e:
        # Narrowed from a bare `except: pass` — log instead of swallowing.
        tprint(f"{indent}[dep] WARNING: existence/corruption check failed for "
               f"{check_path}: {_chk_e!r}")
    # Lock released here - Opus API calls below run WITHOUT holding the lock

    tprint(f"{indent}[dep] Migrating dependency: {normalized}")

    # Download (no lock needed - uses AIDP REST API, not cluster session)
    # Use download_path (full workspace-relative) not normalized (JOB_ROOT stripped)
    # Retry on transient failures (Bad Gateway, network blips) before giving up.
    content = None
    for _attempt in range(3):
        content = download_notebook(download_path)
        if content:
            break
        if _attempt < 2:
            tprint(f"{indent}[dep] download retry {_attempt + 1}/3 for {normalized}")
            time.sleep(2 * (_attempt + 1))
    if not content:
        tprint(f"{indent}[dep] CANNOT DOWNLOAD: {normalized} (tried: {download_path})")
        # Do NOT cache None — caching a failure here permanently blocks retries
        # by the backfill pass and by sibling tasks that reference the same dep.
        # Transient failures (Bad Gateway, throttling) must be retryable.
        return None

    # Scan for sub-dependencies
    sub_deps = scan_for_run_deps(content)
    # Resolve relative paths against this notebook's directory
    nb_dir = os.path.dirname(notebook_path) or os.path.dirname(normalized)
    resolved_sub_deps = []
    for dep_path, dep_params in sub_deps:
        if not dep_path.startswith("/"):
            dep_path = os.path.normpath(os.path.join(nb_dir, dep_path))
        # Manifest override: when naive resolution produces a path the manifest
        # didn't validate for this parent, swap it for the manifest's basename
        # match (if unique). No-op when the manifest is empty or the path is
        # already a known child of this parent.
        dep_path = _resolve_relative_dep(notebook_path, dep_path)
        resolved_sub_deps.append((dep_path, dep_params))
    if resolved_sub_deps:
        tprint(f"{indent}[dep] {normalized} has {len(resolved_sub_deps)} sub-deps: {[d[0] for d in resolved_sub_deps]}")

    # Deduplicate sub-deps
    seen = set()
    unique_sub_deps = []
    for dep_path, dep_params in resolved_sub_deps:
        if dep_path not in seen:
            seen.add(dep_path)
            unique_sub_deps.append((dep_path, dep_params))

    # Migrate sub-deps (parallel for code-only, with semaphore)
    dep_path_map = {}
    _dep_semaphore = getattr(ensure_migrated, '_semaphore', None)
    if _dep_semaphore is None:
        ensure_migrated._semaphore = asyncio.Semaphore(5)
        _dep_semaphore = ensure_migrated._semaphore

    async def _migrate_one_dep(dep_path, dep_params):
        async with _dep_semaphore:
            dep_normalized = normalize_nb_path(dep_path)
            return dep_path, dep_normalized, await ensure_migrated(
                dep_path, session, depth + 1, job_name)

    if len(unique_sub_deps) > 3:  # Parallel safe: callers use singleton, not session refs
        # Parallel: first batch-check which deps already exist (one cluster call),
        # then run Opus migrations in parallel for the ones that need it.
        tprint(f"{indent}[dep] Batch-checking {len(unique_sub_deps)} sub-deps...")

        # Single cluster call to check all paths at once
        paths_to_check = []
        for dp, _ in unique_sub_deps:
            n = normalize_nb_path(dp)
            if not n.endswith(".ipynb"):
                n += ".ipynb"
            paths_to_check.append((dp, n, f"{MIGRATED_BASE}/{n}"))

        check_code = "import os, json\nresults = {}\n"
        for _, _, check_path in paths_to_check:
            safe = check_path.replace("'", "\\'")
            check_code += f"try:\n  p = '{safe}'\n  if os.path.exists(p):\n    nb=json.load(open(p));results[p]='VALID' if nb.get('cells') else 'EMPTY'\n  else:\n    results[p]='MISSING'\nexcept:\n  results[p]='CORRUPT'\n"
        check_code += "print(json.dumps(results))"

        try:
            from context_tools import _unwrap_aidp_text
            # Use run_stateless (pool session) — avoids lock contention with health ping
            # and is more reliable at startup than the main session.
            check_result = await session.run_stateless(check_code, timeout=60)
            raw = format_outputs(check_result.get("outputs", []))
            check_output = _unwrap_aidp_text(raw)
            # If unwrap returned the AIDP JSON wrapper itself, extract first chunk value
            if check_output.startswith('[{"type"'):
                outputs = check_result.get("outputs", [])
                if outputs:
                    check_output = _unwrap_aidp_text(outputs[0].get("text", ""))
            exists_map = json.loads(check_output)
        except Exception as _bce:
            tprint(f"{indent}[dep] Batch-check failed: {_bce!r}")
            exists_map = {}

        # Split into already-done and need-migration
        needs_migration = []
        for dp, dp_norm, check_path in paths_to_check:
            status = exists_map.get(check_path, "MISSING")
            if status == "VALID":
                _migration_cache[dp_norm] = check_path
                dep_path_map[dp] = check_path
                dep_path_map[dp_norm] = check_path
            else:
                needs_migration.append((dp, None))

        skipped = len(unique_sub_deps) - len(needs_migration)
        tprint(f"{indent}[dep] {skipped} already exist, {len(needs_migration)} need migration (serial)...")

        for dp, dpar in needs_migration:
            try:
                dep_path, dep_normalized, migrated_dep = await _migrate_one_dep(dp, dpar)
                if migrated_dep:
                    dep_path_map[dep_path] = migrated_dep
                    dep_path_map[dep_normalized] = migrated_dep
                    if dep_path.endswith(".ipynb"):
                        dep_path_map[dep_path[:-6]] = migrated_dep
                    if not dep_path.startswith("/Workspace/"):
                        dep_path_map[f"/Workspace/{dep_normalized}"] = migrated_dep
            except Exception as e:
                tprint(f"{indent}[dep] Dep migration failed: {e}")
    else:
        # Serial for deeper levels or single deps
        for dep_path, dep_params in unique_sub_deps:
            dep_normalized = normalize_nb_path(dep_path)
            migrated_dep = await ensure_migrated(
                dep_path, session, depth + 1, job_name)
            if migrated_dep:
                dep_path_map[dep_path] = migrated_dep
                dep_path_map[dep_normalized] = migrated_dep
                if dep_path.endswith(".ipynb"):
                    dep_path_map[dep_path[:-6]] = migrated_dep
                if not dep_path.startswith("/Workspace/"):
                    dep_path_map[f"/Workspace/{dep_normalized}"] = migrated_dep

    # Now migrate this notebook - deps are code-only (no execution)
    # They'll be executed in the parent's namespace via oidlUtils.notebook.run()
    # Pass download_path (full workspace-relative) so process_notebook can
    # download via the AIDP API.  normalized has JOB_ROOT stripped which the
    # downloadFileMeta endpoint doesn't recognise.
    result = await process_notebook(
        download_path, session, job_name, f"dep_{os.path.basename(normalized)}",
        parameters={}, run_all=False, session_pool=None,
        dep_path_map=dep_path_map,
    )

    if result.get("status") in ("PASS", "PARTIAL"):
        _migration_cache[normalized] = migrated_path
        await registry_update(session, notebook_path, migrated_path, job_name, result["status"])
        tprint(f"{indent}[dep] Registry updated: {registry_key(notebook_path)}")
        tprint(f"{indent}[dep] Migrated OK (code-only): {normalized} -> {migrated_path}")
        return migrated_path
    else:
        tprint(f"{indent}[dep] MIGRATION FAILED: {normalized}")
        # Don't cache None — backfill pass / sibling tasks should be allowed
        # to retry. Re-running ensure_migrated() is idempotent.
        return None


async def backfill_missing_deps(
    session: AIDPSession,
    job_name: str,
    max_iterations: int = 4,
) -> dict:
    """Scan all migrated notebooks under MIGRATED_BASE for %run / notebook.run
    references that point at paths which are NOT yet migrated, then migrate
    them. Repeats until a fixed point (no new missing deps) or max_iterations.

    Catches transitive cross-user deps that ``ensure_migrated()`` may have
    skipped because of:
      - earlier transient download failures (Bad Gateway, throttling)
      - deps introduced only after a parent was rewritten by Opus
      - manifest run_deps that did not enumerate the full transitive tree

    Returns: {"scanned": int, "missing": int, "migrated": int, "failed": list}
    """
    from context_tools import _unwrap_aidp_text

    tprint(f"\n[backfill] Scanning migrated notebooks for unmigrated %run targets...")

    stats = {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}
    seen_attempted: set = set()

    for iteration in range(max_iterations):
        # 1) List every migrated notebook in MIGRATED_BASE
        list_code = (
            "import os, json\n"
            f"_root = '{MIGRATED_BASE}'\n"
            "_out = []\n"
            "for dp, _, fns in os.walk(_root):\n"
            "  for fn in fns:\n"
            "    if fn.endswith('.ipynb'):\n"
            "      _out.append(os.path.join(dp, fn))\n"
            "print(json.dumps(_out))"
        )
        try:
            list_result = await session.run_stateless(list_code, timeout=60)
            raw = format_outputs(list_result.get("outputs", []))
            list_output = _unwrap_aidp_text(raw)
            if list_output.startswith('[{"type"'):
                outs = list_result.get("outputs", [])
                if outs:
                    list_output = _unwrap_aidp_text(outs[0].get("text", ""))
            # AIDP sometimes DUPLICATES cell stdout ("[...]\n[...]"), so a plain
            # json.loads() raises "Extra data: line 2 column 1". Use raw_decode to
            # take the first complete JSON value and ignore any duplicated trailer.
            list_output = list_output.strip()
            try:
                migrated_files = json.loads(list_output)
            except json.JSONDecodeError:
                migrated_files, _idx = json.JSONDecoder().raw_decode(list_output)
        except Exception as e:
            tprint(f"[backfill] list failed: {e!r} — aborting backfill")
            return stats

        if not migrated_files:
            tprint(f"[backfill] No migrated notebooks under {MIGRATED_BASE}")
            return stats

        stats["scanned"] = len(migrated_files)

        # 2) For each migrated notebook, read it on-cluster and scan for %run targets
        # Use the same cluster (cheap, avoids re-downloading).
        scan_code = (
            "import os, json, re\n"
            "_files = " + json.dumps(migrated_files) + "\n"
            "_run_re = re.compile(r'%run\\s+([^\\n]+)')\n"
            "_nb_re  = re.compile(r'(?:dbutils|oidlUtils)\\.notebook\\.run\\s*\\(\\s*[\"\\']([^\"\\']+)[\"\\']')\n"
            "_results = []\n"
            "for _p in _files:\n"
            "  try:\n"
            "    with open(_p) as _f:\n"
            "      _nb = json.load(_f)\n"
            "  except Exception:\n"
            "    continue\n"
            "  for _c in _nb.get('cells', []):\n"
            "    if _c.get('cell_type') != 'code':\n"
            "      continue\n"
            "    _src = ''.join(_c.get('source', []))\n"
            "    _src = '\\n'.join(_l for _l in _src.splitlines() if _l.strip() and not _l.strip().startswith(('#', '//', '--')))\n"
            "    for _m in _run_re.finditer(_src):\n"
            "      _line = _m.group(1).strip()\n"
            "      if _line.startswith('\"') or _line.startswith(\"'\"):\n"
            "        _q = _line[0]; _e = _line.find(_q, 1)\n"
            "        _t = _line[1:_e] if _e > 0 else _line[1:].rstrip(_q)\n"
            "      else:\n"
            "        _t = _line.split()[0] if _line.split() else ''\n"
            "      _t = re.sub(r'\\$\\w+', '', _t).strip()\n"
            "      if _t:\n"
            "        _results.append((_p, _t))\n"
            "    for _m in _nb_re.finditer(_src):\n"
            "      _results.append((_p, _m.group(1)))\n"
            "print(json.dumps(_results))"
        )
        try:
            scan_result = await session.run_stateless(scan_code, timeout=120)
            raw = format_outputs(scan_result.get("outputs", []))
            scan_output = _unwrap_aidp_text(raw)
            if scan_output.startswith('[{"type"'):
                outs = scan_result.get("outputs", [])
                if outs:
                    scan_output = _unwrap_aidp_text(outs[0].get("text", ""))
            # AIDP may DUPLICATE cell stdout ("[...]\n[...]") → json.loads raises
            # "Extra data". Take the first complete JSON value via raw_decode.
            scan_output = scan_output.strip()
            try:
                run_refs = json.loads(scan_output)
            except json.JSONDecodeError:
                run_refs, _idx = json.JSONDecoder().raw_decode(scan_output)
        except Exception as e:
            tprint(f"[backfill] scan failed: {e!r} — aborting backfill")
            return stats

        # 3) Identify which targets are NOT under MIGRATED_BASE and not yet migrated
        # Target is "missing" if:
        #   - it's not already a path under MIGRATED_BASE, AND
        #   - the migrated equivalent (MIGRATED_BASE/<normalized>) doesn't exist
        missing_targets: list = []
        for parent_file, target in run_refs:
            # If reference is already a MIGRATED_BASE path, nothing to do
            if target.startswith(MIGRATED_BASE):
                continue
            # Skip relative paths (they're resolved by _fix_run, not real cross-user deps)
            if not target.startswith("/") and not target.startswith("Workspace/"):
                # Resolve relative to the parent's *original* dir is hard from migrated state;
                # _fix_run already converts these to absolute migrated paths during migration.
                # If a relative path leaked through unfixed, it implies a parent that wasn't
                # fully rewritten — surface it but don't try to migrate (no source path).
                continue

            normalized = normalize_nb_path(target)
            if not normalized.endswith(".ipynb"):
                normalized += ".ipynb"
            target_migrated = f"{MIGRATED_BASE}/{normalized}"

            if normalized in seen_attempted:
                continue
            missing_targets.append((parent_file, target, normalized, target_migrated))

        if not missing_targets:
            tprint(f"[backfill] iteration {iteration + 1}: fixed point reached, no missing deps")
            return stats

        # 4) Batch-check existence on cluster
        check_code = (
            "import os, json\n"
            "_paths = " + json.dumps([m[3] for m in missing_targets]) + "\n"
            "print(json.dumps({_p: os.path.exists(_p) for _p in _paths}))"
        )
        try:
            check_result = await session.run_stateless(check_code, timeout=60)
            raw = format_outputs(check_result.get("outputs", []))
            check_output = _unwrap_aidp_text(raw)
            if check_output.startswith('[{"type"'):
                outs = check_result.get("outputs", [])
                if outs:
                    check_output = _unwrap_aidp_text(outs[0].get("text", ""))
            exists_map = json.loads(check_output)
        except Exception as e:
            tprint(f"[backfill] exists-check failed: {e!r}")
            exists_map = {}

        truly_missing = [
            m for m in missing_targets
            if not exists_map.get(m[3], False)
        ]

        if not truly_missing:
            tprint(f"[backfill] iteration {iteration + 1}: all referenced deps already migrated")
            return stats

        stats["missing"] += len(truly_missing)
        tprint(f"[backfill] iteration {iteration + 1}: {len(truly_missing)} unmigrated dep(s) found")

        # 5) Migrate each missing dep through ensure_migrated()
        for parent_file, target, normalized, target_migrated in truly_missing:
            seen_attempted.add(normalized)
            tprint(f"[backfill]   migrating {target}  (referenced by {os.path.basename(parent_file)})")
            try:
                migrated = await ensure_migrated(target, session, depth=0, job_name=job_name)
                if migrated:
                    stats["migrated"] += 1
                    tprint(f"[backfill]   OK -> {migrated}")
                else:
                    stats["failed"].append(target)
                    tprint(f"[backfill]   FAILED: {target}")
            except Exception as e:
                stats["failed"].append(target)
                tprint(f"[backfill]   FAILED ({e!r}): {target}")

        # Loop again — newly-migrated deps may themselves reference more deps
        # which were also missed. Cap by max_iterations to avoid runaway.

    tprint(f"[backfill] hit max_iterations={max_iterations}, stopping")
    return stats


def _collect_missing_run_deps(deps_list: list) -> list:
    """Recursively collect external run_dep paths where exists=False."""
    result = []
    for rd in deps_list:
        if rd.get("location") == "external" and rd.get("exists") is False:
            result.append(rd["path"])
        result.extend(_collect_missing_run_deps(rd.get("nested_deps", [])))
    return result


def _flatten_manifest_run_deps(deps_list: list) -> list:
    """Recursively flatten manifest run_deps into a list of paths (depth-first).
    Includes nested deps so the full dependency tree is migrated."""
    result = []
    for rd in deps_list:
        path = rd.get("path")
        if path and rd.get("exists") is not False:
            # Depth-first: migrate nested deps before the parent
            result.extend(_flatten_manifest_run_deps(rd.get("nested_deps", [])))
            result.append(path)
    return result


# ─── Job Processing ──────────────────────────────────────────────────

async def process_job(job: dict, session: AIDPSession) -> dict:
    """Process one job, respecting DAG layer ordering.

    Acquires a wave-concurrency lease via ``ThrottleCoordinator`` before
    starting real work. The lease is process-level and file-locked, so
    multiple concurrent ``run_migration.sh`` invocations share a single
    wave budget (``AIDP_THROTTLE_BUDGET`` / ``AIDP_WAVE_SIZE``, default
    48). Soft-fails to no-op if the coordinator module is unavailable.
    """
    global MIGRATED_BASE, JOB_ROOT
    job_name = job["job_name"]

    # Acquire wave-concurrency lease BEFORE doing any per-job setup or
    # logging so blocked jobs accumulate quietly. Released on completion
    # via try/finally below.
    _wave_coord = _get_throttle_coord()
    _lease_id = None
    if _wave_coord is not None:
        try:
            _lease_id = _wave_coord.acquire(label=job_name)
        except Exception as _e:
            tprint(f"[throttle] lease acquire failed ({_e}) -- proceeding without wave cap")
            _lease_id = None
    try:
        return await _process_job_inner(job, session)
    finally:
        if _wave_coord is not None and _lease_id is not None:
            try:
                _wave_coord.release(_lease_id)
            except Exception:
                pass


async def _process_job_inner(job: dict, session: AIDPSession) -> dict:
    """Inner per-job logic. Wrapped by ``process_job`` for concurrency control."""
    global MIGRATED_BASE, JOB_ROOT
    job_name = job["job_name"]
    layers = job.get("execution_layers", [])
    tasks_by_key = {t["task_key"]: t for t in job["tasks"]}
    parameters = job.get("parameters", {})

    # Set MIGRATED_BASE and JOB_ROOT per job
    MIGRATED_BASE = f"{OUTPUT_BASE}/{job_name}/notebooks"
    JOB_ROOT = job.get("root", "")

    # Reset the tool-only write-redirect map for this job. The map is
    # shared across tasks within the job so cross-task reads of tool-
    # written tables/paths resolve to the redirected target — but a NEW
    # job starts with a clean slate (so two parallel migrations of
    # different jobs in the same Python process can't collide).
    clear_write_redirect_map()

    tprint(f"\n{'='*60}")
    tprint(f"JOB: {job_name} ({len(job['tasks'])} tasks, {len(layers)} layers)")
    tprint(f"Parameters: {json.dumps(parameters)}")
    tprint(f"Output: {OUTPUT_BASE}/{job_name}/")
    tprint(f"Notebooks: {MIGRATED_BASE}/")
    tprint(f"{'='*60}")

    # NOTE: the write-redirect sandbox schema is ensured PER TASK, right after
    # that task's cluster connect (see the connect block below) — not here.
    # Each task may run on a different cluster, and this point has no live
    # connection in the per-task-connect (no --cluster) path, so a create here
    # would silently no-op. _ensure_redirect_schema is idempotent per cluster.

    task_results = {}
    consecutive_nb_failures = 0
    task_counter = 0  # for numbered directory prefixes

    # Clear job-wide cell history and package tracking at start of each job
    _cell_history.clear()
    clear_installed_packages()

    # Register job manifest for get_job_parameters tool
    set_job_manifest(job)

    # Populate Databricks→AIDP job_id map from manifest. Keys are stringified
    # so callers can lookup with either int or str. Empty dict if absent.
    global _db_to_aidp_job_map, _unmapped_db_job_ids
    raw_map = job.get("db_to_aidp_job_map", {}) or {}
    _db_to_aidp_job_map = {str(k): str(v) for k, v in raw_map.items() if v}
    _unmapped_db_job_ids = set()
    if _db_to_aidp_job_map:
        tprint(f"  [job_map] loaded {len(_db_to_aidp_job_map)} Databricks→AIDP job mappings")

    # --start-task: skip tasks until we find the one matching START_TASK (substring match on task_key)
    _skipping = bool(START_TASK)

    _job_halted = None  # None = running; str = halt reason ("circuit_breaker")

    for layer_idx, layer in enumerate(layers):
        if _job_halted:
            for tk in layer:
                task_results[tk] = {"task": tk, "status": "BLOCKED", "blocked_by": _job_halted}
            continue

        tprint(f"\n  Layer {layer_idx}: {layer}")

        for task_key in layer:
            task_counter += 1

            # Resume from --start-task
            if _skipping:
                if START_TASK in task_key:
                    _skipping = False
                    tprint(f"  [--start-task] Resuming from {task_key} (matched '{START_TASK}')")
                else:
                    tprint(f"  [--start-task] Skipping {task_key}")
                    task_results[task_key] = {"task": task_key, "status": "SKIPPED"}
                    continue

            # --only-tasks: run only matching tasks
            if ONLY_TASKS and not any(t in task_key for t in ONLY_TASKS):
                tprint(f"  [--only-tasks] Skipping {task_key}")
                task_results[task_key] = {"task": task_key, "status": "SKIPPED"}
                continue

            task = tasks_by_key.get(task_key)
            if not task:
                task_results[task_key] = {"task": task_key, "status": "MISSING_TASK"}
                continue

            # Track current task for get_job_parameters tool
            set_current_task_key(task_key)

            # ── Cluster switching: connect or switch if task needs a different cluster ──
            task_cluster = (task.get("cluster_id")
                            or job.get("default_cluster")
                            or session.cluster_id)
            if not task_cluster:
                tprint(f"  ERROR {task_key}: no cluster_id in task/manifest and no --cluster default")
                task_results[task_key] = {"task": task_key, "status": "ERROR",
                                          "error": "No cluster_id available"}
                continue
            if session.cluster_id is None:
                # First task, no cluster connected yet (--cluster not specified)
                tprint(f"  Connecting to cluster {task_cluster[:12]}... for first task")
                from cluster_lifecycle import ensure_cluster_running, ensure_aidp_compat_installed
                await ensure_cluster_running(task_cluster)
                await ensure_aidp_compat_installed(task_cluster)
                await session.connect(cluster_id=task_cluster,
                                      session_name=f"aidp_mig_{job_name}_{task_key}")
                _bootstrap_snippets = [
                    "from aidp_compat import dbutils, displayHTML, sql, translate_path",
                    f"import os; os.makedirs('{OUTPUT_BASE}', exist_ok=True)",
                    build_oidlutils_bridge_snippet(OUTPUT_BASE, job_name),
                ]
                for _bs in _bootstrap_snippets:
                    await session.execute(_bs, timeout=30)
                    session.register_bootstrap(_bs)
                tprint(f"  Cluster {task_cluster[:12]}... ready.")
            elif task_cluster != session.cluster_id:
                tprint(f"  Switching cluster: {session.cluster_id[:12]}... -> {task_cluster[:12]}...")
                await session.switch_cluster(task_cluster,
                                             session_name=f"aidp_mig_{job_name}_{task_key}")
                tprint(f"  Cluster {task_cluster[:12]}... ready.")

            # Ensure the write-redirect sandbox schema on THIS task's (now live)
            # cluster before any redirected write runs. Idempotent per cluster —
            # runs once per distinct cluster the job touches. Must be here (live
            # connection), not job-level, since each task may use a different
            # cluster and the no-cluster path has no connection until now.
            await _ensure_redirect_schema(session)

            nb_path = task.get("resolved_path", task.get("notebook_path", ""))
            if task.get("resolution_status") == "NOT_FOUND":
                tprint(f"  SKIP {task_key}: notebook not found ({nb_path})")
                task_results[task_key] = {"path": nb_path, "task": task_key, "status": "NOT_FOUND"}
                continue

            # Check if dependencies passed
            blocked = False
            for dep in task.get("depends_on", []):
                dep_result = task_results.get(dep, {})
                if dep_result.get("status") in ("FAIL", "NOT_FOUND", "BLOCKED", "DOWNLOAD_FAILED"):
                    tprint(f"  BLOCKED {task_key}: dependency {dep} failed")
                    task_results[task_key] = {"path": nb_path, "task": task_key, "status": "BLOCKED", "blocked_by": dep}
                    blocked = True
                    break
            if blocked:
                continue

            # Check if any external %run deps are missing (from manifest run_deps)
            missing_deps = _collect_missing_run_deps(task.get("run_deps", []))
            if missing_deps:
                tprint(f"  SKIP {task_key}: {len(missing_deps)} external dep(s) missing: {missing_deps}")
                task_results[task_key] = {"path": nb_path, "task": task_key, "status": "SKIP_MISSING_DEPS",
                                          "missing_deps": missing_deps}
                continue

            # Merge task params with job params (job-level wins on conflict)
            merged_params = dict(task.get("base_parameters", {}))
            merged_params.update(parameters)
            set_current_task_params(merged_params)

            # Check if already migrated — filesystem check then registry
            nb_norm_check = normalize_nb_path(nb_path)
            if not nb_norm_check.endswith(".ipynb"):
                nb_norm_check += ".ipynb"

            # Filesystem check: does migrated notebook already exist in this job's output?
            if SKIP_MIGRATED:
                target_path = f"{MIGRATED_BASE}/{nb_norm_check}"
                try:
                    fs_check = await session.execute(f"""
import os, json
p = '{target_path}'
if os.path.exists(p) and os.path.getsize(p) > 0:
    with open(p) as f:
        nb = json.load(f)
    cells = nb.get('cells', [])
    print(f'VALID:{{len(cells)}}')
else:
    print('MISSING')
""", timeout=15)
                    fs_out = format_outputs(fs_check.get("outputs", []))
                    if fs_out.strip().startswith("VALID:"):
                        cell_count = fs_out.strip().split(":")[1]
                        tprint(f"  [{task_key}] SKIP: already exists in target ({cell_count} cells) -> {target_path}")
                        task_results[task_key] = {"path": nb_path, "task": task_key,
                                                  "status": "ALREADY_MIGRATED",
                                                  "migrated_path": target_path}
                        # Ensure skipped notebooks are recorded in registry
                        await registry_update(session, nb_path, target_path, job_name, "PASS")
                        continue
                except Exception:
                    pass  # fall through to registry check

                # Registry check: was it migrated by another job?
                registry_hit = await registry_lookup(session, nb_path)
                if registry_hit:
                    tprint(f"  [{task_key}] SKIP: already migrated by another job (registry) -> {registry_hit}")
                    task_results[task_key] = {"path": nb_path, "task": task_key,
                                              "status": "ALREADY_MIGRATED",
                                              "migrated_path": registry_hit}
                    # Ensure cross-job hits are recorded in this job's registry too
                    await registry_update(session, nb_path, registry_hit, job_name, "PASS")
                    continue

            # Step 1: Scan and migrate deps (depth-first, serial)
            tprint(f"  [{task_key}] Scanning for %run dependencies...")
            nb_content = download_notebook(nb_path)
            dep_path_map = {}
            if nb_content:
                run_deps = scan_for_run_deps(nb_content)
                # Resolve relative paths against the notebook's directory
                nb_dir = os.path.dirname(nb_path)
                resolved_deps = []
                for dep_path, dep_params in run_deps:
                    if not dep_path.startswith("/"):
                        dep_path = os.path.normpath(os.path.join(nb_dir, dep_path))
                    dep_path = _resolve_relative_dep(nb_path, dep_path)
                    resolved_deps.append((dep_path, dep_params))
                # Deduplicate
                seen = set()
                unique_deps = []
                for dep_path, dep_params in resolved_deps:
                    if dep_path not in seen:
                        seen.add(dep_path)
                        unique_deps.append((dep_path, dep_params))

                if unique_deps:
                    tprint(f"  [{task_key}] Found {len(unique_deps)} unique %run deps: {[d[0] for d in unique_deps]}")
                    for dep_path, dep_params in unique_deps:
                        migrated = await ensure_migrated(
                            dep_path, session, depth=1, job_name=job_name)
                        if migrated:
                            dep_path_map[dep_path] = migrated
                            dep_norm = normalize_nb_path(dep_path)
                            dep_path_map[dep_norm] = migrated
                            if dep_path.endswith(".ipynb"):
                                dep_path_map[dep_path[:-6]] = migrated
                            if not dep_path.startswith("/Workspace/"):
                                dep_path_map[f"/Workspace/{dep_norm}"] = migrated
                    tprint(f"  [{task_key}] Dep migration complete: {len(dep_path_map)} paths mapped")
                else:
                    tprint(f"  [{task_key}] No %run dependencies")

            # Use manifest run_deps to fill gaps — migrate any deps that scan_for_run_deps missed
            manifest_deps = _flatten_manifest_run_deps(task.get("run_deps", []))
            manifest_extra = 0
            for mdep_path in manifest_deps:
                # Normalize for comparison against what scan already found
                mdep_norm = normalize_nb_path(mdep_path)
                already_mapped = any(
                    mdep_path == k or mdep_norm == k or
                    mdep_path == v or mdep_norm == normalize_nb_path(v or "")
                    for k, v in dep_path_map.items()
                )
                if not already_mapped:
                    tprint(f"  [{task_key}] Manifest dep not found by scan — migrating: {mdep_path}")
                    migrated = await ensure_migrated(
                        mdep_path, session, depth=1, job_name=job_name)
                    if migrated:
                        dep_path_map[mdep_path] = migrated
                        dep_norm = normalize_nb_path(mdep_path)
                        dep_path_map[dep_norm] = migrated
                        if mdep_path.endswith(".ipynb"):
                            dep_path_map[mdep_path[:-6]] = migrated
                        if not mdep_path.startswith("/Workspace/"):
                            dep_path_map[f"/Workspace/{dep_norm}"] = migrated
                        manifest_extra += 1
            if manifest_extra:
                tprint(f"  [{task_key}] Manifest filled {manifest_extra} deps missed by scan")

            # Enrich dep_path_map with corrected paths from manifest (e.g. space <-> underscore)
            for rd in task.get("run_deps", []):
                orig = rd.get("original_path")
                corrected = rd.get("path")
                if orig and corrected and orig != corrected:
                    migrated = dep_path_map.get(corrected)
                    if migrated:
                        dep_path_map[orig] = migrated
                        tprint(f"  [{task_key}] Path correction from manifest: {orig} -> {corrected}")

            # Enrich dep_path_map with ALL transitive deps from _migration_cache.
            # ensure_migrated() recursively migrates sub-deps and caches them in
            # _migration_cache, but only returns the top-level migrated path.
            # Without this, _inline_child_notebook can't resolve nested %run paths
            # (e.g. <parameters_stub>.ipynb -> %run ./<shared_utils_notebook>) because they're
            # not direct deps of the task notebook.
            transitive_added = 0
            for cached_norm, cached_path in _migration_cache.items():
                if cached_path and cached_norm not in dep_path_map:
                    dep_path_map[cached_norm] = cached_path
                    # Also add /Workspace/ prefixed variant
                    if not cached_norm.startswith("/Workspace/"):
                        ws_key = f"/Workspace/{cached_norm}"
                        if ws_key not in dep_path_map:
                            dep_path_map[ws_key] = cached_path
                    # Also add without .ipynb
                    if cached_norm.endswith(".ipynb"):
                        bare = cached_norm[:-6]
                        if bare not in dep_path_map:
                            dep_path_map[bare] = cached_path
                    transitive_added += 1
            if transitive_added:
                tprint(f"  [{task_key}] Added {transitive_added} transitive deps from cache ({len(dep_path_map)} total paths)")

            # Step 2: Migrate this notebook with dep paths mapped
            numbered_key = f"{task_counter:02d}_{task_key}"
            # Optional acceptance contract: pulled from per-task manifest entry first,
            # falling back to job-level. Absent = no-op (back-compat).
            _ac_dict = task.get("acceptance_contract") or job.get("acceptance_contract")
            try:
                result = await process_notebook(
                    nb_path, session, job_name, numbered_key, merged_params,
                    run_all=True, session_pool=None, dep_path_map=dep_path_map,
                    acceptance_contract_dict=_ac_dict,
                )
            except Exception as e:
                tprint(f"  [{task_key}] ERROR: {e}")
                result = {"path": nb_path, "task": task_key, "status": "ERROR", "error": str(e)[:200]}

            task_results[task_key] = result

            if result.get("status") == "FAIL":
                consecutive_nb_failures += 1
            else:
                consecutive_nb_failures = 0

            if consecutive_nb_failures >= CONSECUTIVE_NB_FAIL_THRESHOLD:
                tprint(f"  CIRCUIT BREAKER: {consecutive_nb_failures} consecutive failures in {job_name}")
                _job_halted = "circuit_breaker"
                break  # stop current layer too — cascading failures

            if result.get("job_should_stop"):
                tprint(f"  [{task_key}] job_should_stop — cell exhausted retries, but migration continues for remaining tasks")
                # Don't block future layers — migration should attempt all notebooks.
                # Only circuit_breaker (consecutive FAIL notebooks) halts the job.

    # ── Backfill pass: migrate any transitive %run targets that slipped through ──
    # Catches cross-user deps that were missed because of transient download
    # failures, manifest gaps, or post-Opus-rewrite-introduced references.
    backfill_stats = {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}
    if not _job_halted:
        try:
            backfill_stats = await backfill_missing_deps(session, job_name)
            if backfill_stats["migrated"] or backfill_stats["failed"]:
                tprint(f"[backfill] summary: scanned={backfill_stats['scanned']} "
                       f"missing={backfill_stats['missing']} "
                       f"migrated={backfill_stats['migrated']} "
                       f"failed={len(backfill_stats['failed'])}")
        except Exception as _bf_err:
            tprint(f"[backfill] aborted: {_bf_err!r}")

    # Generate job report
    job_report = f"# Job Report: {job_name}\n\n"
    job_report += f"- Date: {datetime.now().isoformat()}\n"
    job_report += f"- Tasks: {len(job['tasks'])}\n"
    job_report += f"- Parameters: {json.dumps(parameters)}\n\n"
    job_report += "## Task Results\n\n"
    job_report += "| Task | Status | OK | Failed | Fixed |\n|------|--------|-----|--------|-------|\n"

    failed_notes = []  # collect error notes for summary
    for task in job["tasks"]:
        tk = task["task_key"]
        r = task_results.get(tk, {})
        st = r.get("status", "?")
        ok = r.get("ok", "-")
        fail = r.get("failed", "-")
        fix = r.get("fixed", "-")
        job_report += f"| {tk} | {st} | {ok} | {fail} | {fix} |\n"
        # Collect per-cell failure notes for this task
        for cr in r.get("cell_results", []):
            if cr.get("note") and cr.get("status") == "error":
                failed_notes.append(f"- **{tk}** cell {cr['cell']}: {cr['note']}")

    # Errors & warnings section (surfaces notes that would otherwise be silent)
    if failed_notes:
        job_report += "\n## Errors & Warnings\n\n"
        job_report += "The following cells failed with actionable notes:\n\n"
        job_report += "\n".join(failed_notes) + "\n"

    # Append unmapped Databricks job_ids (job-trigger calls without an AIDP UUID)
    if _unmapped_db_job_ids:
        job_report += "\n## Unmapped Databricks Job Triggers\n\n"
        job_report += "The following Databricks job_ids were referenced in code but missing\n"
        job_report += "from the manifest's `db_to_aidp_job_map`. Migrated cells stub them with\n"
        job_report += "`raise RuntimeError(...)` so failures are loud. Add the AIDP UUID for each\n"
        job_report += "to the manifest and re-run the affected tasks with `--start-task`.\n\n"
        job_report += "Manifest field shape:\n"
        job_report += "```json\n"
        job_report += '"db_to_aidp_job_map": {\n'
        for jid in sorted(_unmapped_db_job_ids):
            job_report += f'  "{jid}": "<aidp-job-uuid>",\n'
        job_report += "}\n"
        job_report += "```\n"

    # Append backfill summary (transitive cross-user deps caught after main pass)
    if backfill_stats.get("scanned"):
        job_report += "\n## Dependency Backfill\n\n"
        job_report += f"- Scanned migrated notebooks: {backfill_stats['scanned']}\n"
        job_report += f"- Unmigrated %run targets found: {backfill_stats['missing']}\n"
        job_report += f"- Migrated by backfill: {backfill_stats['migrated']}\n"
        if backfill_stats.get("failed"):
            job_report += f"- Failed: {len(backfill_stats['failed'])}\n"
            job_report += "\n```\n" + "\n".join(backfill_stats["failed"]) + "\n```\n"

    # Append required packages section
    packages = get_installed_packages()
    if packages:
        job_report += "\n## Required Packages\n\n"
        job_report += "The following packages were pip-installed during migration.\n"
        job_report += "Add these to your cluster libraries configuration to persist across restarts:\n\n"
        job_report += "```\n"
        job_report += "\n".join(packages)
        job_report += "\n```\n"

    # Upload job report
    job_report_path = f"{OUTPUT_BASE}/{job_name}/JOB_REPORT.md"
    pkg_path = f"{OUTPUT_BASE}/{job_name}/REQUIRED_PACKAGES.txt"
    try:
        b64 = base64.b64encode(job_report.encode('utf-8')).decode('ascii')
        await session.run_stateless(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{job_report_path}'), exist_ok=True)
with builtins.open('{job_report_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
""", timeout=60)

        # Upload REQUIRED_PACKAGES.txt if any packages were installed
        if packages:
            pkg_content = "# Required pip packages installed during AIDP migration\n"
            pkg_content += "# Add these to your cluster library configuration\n"
            pkg_content += f"# Generated: {datetime.now().isoformat()}\n"
            pkg_content += f"# Job: {job_name}\n\n"
            pkg_content += "\n".join(packages) + "\n"
            pkg_b64 = base64.b64encode(pkg_content.encode('utf-8')).decode('ascii')
            await session.run_stateless(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{pkg_path}'), exist_ok=True)
with builtins.open('{pkg_path}', 'wb') as f:
    f.write(base64.b64decode('{pkg_b64}'))
""", timeout=60)
            tprint(f"  [packages] Wrote {len(packages)} packages to {pkg_path}")

    except Exception as jr_err:
        tprint(f"  Job report workspace upload failed: {str(jr_err)[:200]} — trying OCI Object Storage...")
        try:
            import tempfile as _tmpmod
            with _tmpmod.TemporaryDirectory() as _jr_tmp:
                # Write JOB_REPORT.md locally
                jr_local = os.path.join(_jr_tmp, "JOB_REPORT.md")
                with open(jr_local, 'w') as f:
                    f.write(job_report)
                jr_key = job_report_path
                if jr_key.startswith("/Workspace/"):
                    jr_key = jr_key[len("/Workspace/"):]
                upload_to_object_storage(jr_local, jr_key, log_fn=tprint)

                # Write REQUIRED_PACKAGES.txt if any
                if packages:
                    pkg_local = os.path.join(_jr_tmp, "REQUIRED_PACKAGES.txt")
                    with open(pkg_local, 'w') as f:
                        pkg_content = "# Required pip packages installed during AIDP migration\n"
                        pkg_content += "# Add these to your cluster library configuration\n"
                        pkg_content += f"# Generated: {datetime.now().isoformat()}\n"
                        pkg_content += f"# Job: {job_name}\n\n"
                        pkg_content += "\n".join(packages) + "\n"
                        f.write(pkg_content)
                    pkg_key = pkg_path
                    if pkg_key.startswith("/Workspace/"):
                        pkg_key = pkg_key[len("/Workspace/"):]
                    upload_to_object_storage(pkg_local, pkg_key, log_fn=tprint)
        except Exception as ocs_jr_err:
            tprint(f"  Job report OCI backup also failed: {str(ocs_jr_err)[:200]}")

    _task_rs = [r for r in task_results.values() if isinstance(r, dict)]
    return {
        "job_name": job_name,
        "task_results": task_results,
        "total_ok":      sum(r.get("ok", 0) for r in _task_rs),
        "total_failed":  sum(r.get("failed", 0) for r in _task_rs),
        "total_fixed":   sum(r.get("fixed", 0) for r in _task_rs),
        # task-level non-cell failures: ERROR (exception), DOWNLOAD_FAILED, NOT_FOUND, BLOCKED
        # SKIP_MISSING_DEPS is a graceful skip, not an error — excluded from this count.
        "total_errored": sum(
            1 for r in _task_rs
            if r.get("status") in ("ERROR", "DOWNLOAD_FAILED", "NOT_FOUND",
                                   "BLOCKED", "MISSING_TASK")
        ),
        "total_skipped": sum(
            1 for r in _task_rs if r.get("status") == "SKIP_MISSING_DEPS"
        ),
    }


# ─── Main ────────────────────────────────────────────────────────────

async def main():
    global AIDP_BASE, DATALAKE_OCID, WORKSPACE_ID, OCI_PROFILE, OUTPUT_BASE, DOWNLOAD_META_URL, _SIGNER, START_TASK, ONLY_TASKS, SKIP_MIGRATED, DIRECT_EXECUTE
    parser = argparse.ArgumentParser(description="Job Migration Orchestrator")
    parser.add_argument("--parallel", type=int, default=20)
    parser.add_argument("--cluster", default=None,
                        help="AIDP cluster ID. If not specified, uses per-task cluster_id from manifest.")
    parser.add_argument("--jobs", help="Comma-separated job names to process (default: all)")
    parser.add_argument("--manifest", default=os.path.join(PROJECT_DIR, "reports", "job_manifest.json"))
    parser.add_argument("--bucket-mapping", default=None,
                        help="(Deprecated/ignored) S3→OCI bucket mapping is now supplied via "
                             "load_bucket_mapping(); this flag is kept only for backward "
                             "compatibility with older invocations.")
    # AIDP environment — override these for deployments using different defaults
    parser.add_argument("--aidp-base", default=AIDP_BASE,
                        help="AIDP REST endpoint base URL (default: %(default)s)")
    parser.add_argument("--datalake-ocid", default=DATALAKE_OCID, required=DATALAKE_OCID is None,
                        help="AIDP data lake OCID (required)")
    parser.add_argument("--workspace-id", default=WORKSPACE_ID, required=WORKSPACE_ID is None,
                        help="AIDP workspace UUID (required)")
    parser.add_argument("--oci-profile", default=OCI_PROFILE,
                        help="OCI config profile name in ~/.oci/config (default: %(default)s)")
    parser.add_argument("--output-base", default=OUTPUT_BASE, required=OUTPUT_BASE is None,
                        help="Workspace path for migrated notebooks and reports (required if no default compiled in)")
    parser.add_argument("--start-task", default="",
                        help="Skip all tasks before this task_key (substring match). "
                             "Useful for resuming a run that was interrupted mid-job.")
    parser.add_argument("--only-tasks", default="",
                        help="Comma-separated task names to run (substring match on task_key). "
                             "Only matching tasks are executed, all others are skipped. "
                             "Example: --only-tasks 'task_a,task_b'")
    _skip_grp = parser.add_mutually_exclusive_group()
    _skip_grp.add_argument("--skip-migrated", action="store_true", dest="skip_migrated", default=True,
                           help="Skip notebooks already migrated (default: enabled). "
                                "Works across jobs — if a notebook was migrated by any job, it is skipped.")
    _skip_grp.add_argument("--no-skip-migrated", action="store_false", dest="skip_migrated",
                           help="Force re-migration of all notebooks, even if already migrated.")
    parser.add_argument("--direct-execute", action="store_true",
                        help="Skip AI analysis/migration and execute notebook cells as-is. "
                             "Useful for testing WebSocket resilience without ANTHROPIC_API_KEY.")
    parser.add_argument("--wave-size", type=int, default=None,
                        help="Max concurrent migration processes per wave (cross-process via "
                             "ThrottleCoordinator). Drives the OCI Object Storage hardening "
                             "profile injected into each notebook's bootstrap: <=8 conservative, "
                             "9-200 balanced, >200 aggressive. Default: AIDP_WAVE_SIZE env "
                             "(48 if unset). Set to 0 to disable wave-concurrency capping.")
    args = parser.parse_args()

    # Wave size: explicit flag overrides env. Propagate to env so the
    # hardening cell injected into each notebook bootstrap picks the same
    # profile, and ThrottleCoordinator uses the same budget.
    if args.wave_size is not None:
        os.environ["AIDP_WAVE_SIZE"] = str(args.wave_size)
        os.environ["AIDP_THROTTLE_BUDGET"] = str(max(1, args.wave_size)) if args.wave_size > 0 else "999999"

    # Apply environment config — must happen before any code uses these globals
    DATALAKE_OCID   = args.datalake_ocid
    # Derive AIDP_BASE from the datalake OCID region unless explicitly overridden
    if args.aidp_base != AIDP_BASE:
        AIDP_BASE = args.aidp_base  # explicit --aidp-base wins
    else:
        from aidp_executor import get_region_from_ocid
        _region = get_region_from_ocid(DATALAKE_OCID)
        AIDP_BASE = f"https://aidp.{_region}.oci.oraclecloud.com/20240831"
    WORKSPACE_ID    = args.workspace_id
    OCI_PROFILE     = args.oci_profile
    # Strip trailing slash(es) so downstream f-strings like
    # f"{OUTPUT_BASE}/{job_name}/..." don't produce a doubled slash (//), which
    # leaks into migrated paths, the registry, task_values.json, and %run rewrites.
    # Guard None: --output-base is not required here (defaults to None).
    OUTPUT_BASE     = args.output_base.rstrip("/") if args.output_base else args.output_base

    # The cell-execution session (cluster_session → AIDPSession) is created with
    # only cluster_id and otherwise falls back to aidp_executor's module defaults.
    # Override them to this run's target so execution hits the right lake/
    # workspace/profile (session endpoint auto-derives from the lake region).
    import aidp_executor as _ae
    _ae.DEFAULT_LAKE_OCID = DATALAKE_OCID
    _ae.DEFAULT_WORKSPACE_ID = WORKSPACE_ID
    _ae.DEFAULT_OCI_PROFILE = OCI_PROFILE

    DOWNLOAD_META_URL = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
                         f"/actions/downloadFileMeta")
    _SIGNER = None  # reset so signer() picks up new OCI_PROFILE
    START_TASK = args.start_task.strip()
    ONLY_TASKS = [t.strip() for t in args.only_tasks.split(",") if t.strip()] if args.only_tasks else []
    SKIP_MIGRATED = args.skip_migrated
    DIRECT_EXECUTE = args.direct_execute
    # Propagate into agent_migrate (separate module with its own copies)
    import agent_migrate as _am
    _am.AIDP_BASE     = AIDP_BASE
    _am.DATALAKE_OCID = DATALAKE_OCID
    _am.WORKSPACE_ID  = WORKSPACE_ID
    _am.OCI_PROFILE   = OCI_PROFILE
    _am.DOWNLOAD_META_URL = DOWNLOAD_META_URL
    _am.UPLOAD_FOLDER_URL = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
                              f"/objects")
    _am.SIGNER = None  # reset cached signer

    # Load bucket mapping
    from context_tools import load_bucket_mapping
    mapping = load_bucket_mapping(args.bucket_mapping)
    tprint(f"Bucket mapping: {len(mapping)} S3 buckets loaded from {args.bucket_mapping}")

    # Load manifest
    with open(args.manifest) as f:
        manifest = json.load(f)

    jobs = manifest["jobs"]
    if args.jobs:
        selected = set(args.jobs.split(","))
        jobs = [j for j in jobs if j["job_name"] in selected]

    tprint(f"{'='*60}")
    tprint(f"Job Migration Orchestrator (serial)")
    tprint(f"{'='*60}")
    tprint(f"Jobs: {len(jobs)}")
    tprint(f"Cluster: {args.cluster or '(per-task from manifest)'}")
    tprint(f"Output: {OUTPUT_BASE}")
    tprint(f"Started: {datetime.now().isoformat()}")

    # Propagate AIDP config into cluster_lifecycle module
    import cluster_lifecycle as _cl
    _cl.AIDP_BASE = AIDP_BASE
    _cl.DATALAKE_OCID = DATALAKE_OCID
    _cl.WORKSPACE_ID = WORKSPACE_ID
    _cl.OCI_PROFILE = OCI_PROFILE
    _cl._SIGNER = None  # reset cached signer after profile change

    # Create singleton cluster session
    from cluster_session import cluster

    # Define bootstrap snippets (replayed on every fresh connect / cluster switch)
    _bootstrap_snippets = [
        "from aidp_compat import dbutils, displayHTML, sql, translate_path",
        f"import os; os.makedirs('{OUTPUT_BASE}', exist_ok=True)",
    ]

    async def _connect_and_bootstrap(cluster_id: str, job_name: str = "default"):
        """Connect to a cluster and run bootstrap snippets."""
        from cluster_lifecycle import ensure_cluster_running, ensure_aidp_compat_installed
        await ensure_cluster_running(cluster_id)
        await ensure_aidp_compat_installed(cluster_id)
        _session_name = f"aidp_mig_{job_name}"
        await cluster.connect(cluster_id=cluster_id, session_name=_session_name)
        for snippet in _bootstrap_snippets:
            await cluster.execute(snippet, timeout=30)
            cluster.register_bootstrap(snippet)
        tprint(f"Session ready on cluster {cluster_id[:12]}...")

    if args.cluster:
        # Explicit --cluster: connect immediately (original behavior)
        await _connect_and_bootstrap(args.cluster, jobs[0]['job_name'] if jobs else "default")

    # 'session' is now the singleton - all functions that take 'session' use this
    session = cluster

    # Process jobs one at a time
    job_results = []
    for job in jobs:
        try:
            result = await process_job(job, session)
            job_results.append(result)
        except Exception as e:
            print(f"  JOB ERROR: {job['job_name']}: {e}")
            job_results.append({"job_name": job["job_name"], "error": str(e)})

        # Singleton handles its own recycling every 5 min - no manual reconnect needed

    # Summary
    tprint(f"\n{'='*60}")
    tprint(f"MIGRATION COMPLETE")
    print(f"{'='*60}")
    for result in job_results:
        name = result.get("job_name", "?")
        if "error" in result:
            print(f"  {name}: ERROR - {result['error']}")
        else:
            ok      = result.get("total_ok", 0)
            fail    = result.get("total_failed", 0)
            fix     = result.get("total_fixed", 0)
            errored  = result.get("total_errored", 0)
            skipped  = result.get("total_skipped", 0)
            line = f"  {name}: Cells OK={ok} Failed={fail} Fixed={fix}"
            if errored:
                line += f" | Tasks errored={errored}"
            if skipped:
                line += f" | Tasks skipped={skipped}"
            print(line)

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
