#!/usr/bin/env python3
"""
Agent Migration Pipeline
=========================
Single-workflow, no-local-storage notebook migration.

For each notebook:
  1. DOWNLOAD from AIDP workspace (temporary)
  2. ANALYZE with the configured LLM - deep compatibility analysis
  3. MIGRATE with the configured LLM - produce migrated notebook + cell classifications
  4. TEST on AIDP cluster - execute READ_ONLY cells, test API connectivity,
     verify dependencies, compare outputs against originals
  5. FIX with Claude Opus 5 - fix any failures found in testing
  6. UPLOAD all artifacts to agent-migrated/ folder on AIDP workspace
  7. DELETE all local temporary files

Produces per notebook:
  - analysis_report.md     (Step 2)
  - migrated.ipynb         (Step 3)
  - migration_report.md    (Step 3)
  - test_report.md         (Step 4+5)
  - final.ipynb            (Step 5 - after fixes)

Usage:
    python3 agent_migrate.py --parallel 20
    python3 agent_migrate.py --start 0 --end 100 --parallel 20
"""

import llm_config  # central model/provider config
import anthropic
import asyncio
import contextvars
import json
import os
import sys
import re
import copy
import itertools
import time
import tempfile
import shutil
import argparse
import urllib.parse
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import oci
import requests as http_requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession, format_outputs, get_oci_signer
from notebook_oci_credentials import rewrite_oci_credential_reads

# ─── Configuration ────────────────────────────────────────────────────

# Generic — no hardcoded customer/AIDP-instance config. Set at runtime
# (job_migrate_from_workflow overrides these and recomputes the URLs). Profile
# defaults to "DEFAULT"; lake/workspace/cluster are required (no default).
AIDP_BASE = None
DATALAKE_OCID = None
WORKSPACE_ID = None
DOWNLOAD_META_URL = None  # recomputed once DATALAKE_OCID/WORKSPACE_ID are set
UPLOAD_FOLDER_URL = None
OCI_PROFILE = "DEFAULT"
DEFAULT_CLUSTER = None
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OUTPUT_FOLDER = "agent-migrated"

# ═══════════════════════════════════════════════════════════════════════
# FAIL-FAST INVESTIGATION CAPS (Fix 7z, 2026-07-29)
# ═══════════════════════════════════════════════════════════════════════
#
# WHY THESE EXIST
# ---------------
# Historical trigger: Example_Txn_Budget_Monitoring / 01_metric_deviation_tracking
# Cell 10 — Opus's fix-loop `run_on_cluster` fired
# `glob.glob('/Volumes/**/hourly_config.ini', recursive=True)`. The FUSE mount
# walk hung the kernel for 20 minutes (Python-side timeout), then the kernel
# remained wedged (AIDP does not expose `/kernels/{id}/interrupt`, HTTP 404)
# so all subsequent execute_requests queued indefinitely — the whole migration
# was effectively dead until manual restart.
#
# ROOT CAUSE
# ----------
# The fix-loop had NO structural cap on `run_on_cluster` calls, and the outer
# loop allowed up to 25 tool rounds × 10 fix attempts = 250 tool calls per
# cell. The `FIX_PROMPT` said "no more than 3 run_on_cluster calls per error"
# — but that was advisory only. Opus routinely violated it.
#
# EMPIRICAL BASELINE (from PR-32 / PR-34 ExampleVendor runs)
# ----------------------------------------------------
#   ~70% of fixable cells converge in 1 attempt
#   ~15%                            in 2 attempts
#   ~5%                             in 3 attempts
#   ~10% never converge (genuinely broken data — must be skipped)
#
# Beyond attempt 3, Opus mostly resubmits noise-variations of prior failures.
# 10 attempts was ~2× the useful ceiling; 25 tool rounds was ~5× the useful
# ceiling. The cost was token spend on doomed cells plus the runaway hang risk.
#
# DESIGN
# ------
# Hard-cap `run_on_cluster` at 3 per Opus call (matches the prompt rule, now
# structurally enforced). Tighten tool-round and fix-attempt budgets so the
# loop bounds are aligned with empirical usefulness — not aspirational
# investigation depth.
#
# See PR <TBD> for full rationale and the ExampleVendor log evidence.

# All four caps are env-overridable (PR-36 review): the 10→3 / 25→5 tightening
# is empirically grounded on ExampleVendor/Model_Mon runs, but a hard notebook that
# genuinely needs deeper investigation shouldn't require a code edit to
# unblock. Operators can restore the old behavior per-run, e.g.:
#   AIDP_MAX_FIX_ATTEMPTS=10 AIDP_MAX_TOOL_ROUNDS_FIX=25 python3 job_migrate_from_workflow.py ...

# Max `run_on_cluster` calls per Opus call (one fix attempt = one Opus call).
# After the cap, further `run_on_cluster` calls are refused by the tool with
# a clear next-step message ("submit_code or make_note, do not keep
# investigating"). Investigation phase ends here — commit to a fix.
MAX_RUN_ON_CLUSTER_PER_CALL = int(os.environ.get("AIDP_MAX_RUN_ON_CLUSTER_PER_CALL", "3"))

# Max tool-use rounds per Opus call for the FIX phase. Covers ALL tools
# combined (run_on_cluster + explore_path + describe_table + submit_code +
# make_note + etc.). With MAX_RUN_ON_CLUSTER_PER_CALL=3, five rounds is:
#   1 look at error + 3 investigations + 1 submit_code = 5. Sufficient.
# Was 25 in call_fix; burned tokens on exploration that never converged.
MAX_TOOL_ROUNDS_FIX = int(os.environ.get("AIDP_MAX_TOOL_ROUNDS_FIX", "5"))

# Max tool-use rounds per Opus call for the MIGRATE phase (pre-execution
# rewrite of Databricks APIs → AIDP). Was 10; migration cells rarely need
# more than 2-3 rounds since the rewrite rules are deterministic.
MAX_TOOL_ROUNDS_MIGRATE = int(os.environ.get("AIDP_MAX_TOOL_ROUNDS_MIGRATE", "5"))

# Max fix attempts per failing cell before it is marked FAIL/PARTIAL and
# the job moves on. Empirically ~85% of fixable cells resolve in ≤2 attempts
# and attempts beyond ~4 rarely help; 5 is a deliberate margin over that
# (a few real cells needed a 4th/5th round) while still capping worst-case
# wasted-token cost at ~25 tool calls / cell (down from 250).
#
# Overwritten in job_migrate.main() from cfg.runtime.fix_max_attempts (YAML
# is the source of truth for customer-tunable knobs). $AIDP_MAX_FIX_ATTEMPTS
# env var wins over both — kept as a per-run escape hatch.
MAX_FIX_ATTEMPTS = int(os.environ.get("AIDP_MAX_FIX_ATTEMPTS", "5"))

# Number of JOB EXECUTION HISTORY entries prepended to EVERY Opus request
# (migrate, eval, fix, recovery). Was 100 (~4-5k tokens per request once a job
# passes 100 cells). The model can page further back with get_cell_history /
# get_history_entry, which the history header already points at.
HISTORY_CONTEXT_ENTRIES = int(os.environ.get("AIDP_HISTORY_CONTEXT_ENTRIES", "25"))

# ─── Effort ladder knobs (opt-in; default = request unchanged) ────────────────
#
# The Opus-5 family runs adaptive thinking at the model's default effort when
# output_config is omitted (high on claude-opus-5, medium on claude-opus-5-5).
# Thinking is billed as output (5x the input rate), so it is the largest
# remaining cost lever. These knobs are OFF unless the env var is set: nothing
# is sent and every request stays byte-identical to today's.
#
# Why a ladder is safe here: execute+verify is a hard checker. A cheap first
# pass (migrate, or fix attempt 1) that misses fails verification exactly as it
# would today and escalates to the default effort on the next attempt; only the
# cells that converge on the first attempt pay less. Pin per route; never vary
# effort inside one call (it invalidates the messages cache).
#
# AIDP_EFFORT_FIX_FIRST_ATTEMPT does NOT keep the per-cell ceiling by itself: it
# turns the FIRST of the (MAX_FIX_ATTEMPTS - 1) fix calls into a reduced-effort
# one (default 5 attempts: 4 default-effort fixes -> 1 reduced + 3 default).
# Raise AIDP_MAX_FIX_ATTEMPTS / runtime.fix_max_attempts by one alongside it
# (6 keeps today's 4 default-effort fixes).
#
# A knob set to the model's own default is a no-op (identical to omitting it):
# `medium` changes nothing on claude-opus-5-5, whose default is medium; test
# `low` there, or name the model the replay runs on.
_EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")


def _effort_env(name: str):
    """Unset/empty → None (send nothing). Any other value must be a known level:
    a typo that silently ran at the model default would void the cost A/B the
    knob was set for, so it fails at startup instead."""
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return None
    if v not in _EFFORT_LEVELS:
        raise ValueError(f"{name}={v!r} is not a valid effort level; "
                         f"use one of {', '.join(_EFFORT_LEVELS)} or leave it unset")
    return v


EFFORT_MIGRATE = _effort_env("AIDP_EFFORT_MIGRATE")                       # CELL_MIGRATE_PROMPT calls
EFFORT_EVAL = _effort_env("AIDP_EFFORT_EVAL")                             # PASS/ISSUE verdict calls
EFFORT_FIX_FIRST_ATTEMPT = _effort_env("AIDP_EFFORT_FIX_FIRST_ATTEMPT")   # call_fix attempt 1 only


def validate_effort_env() -> None:
    """Re-check the effort knobs; raises ValueError on an unknown level. For
    entry points that do control-plane work before importing the migrator
    (job_migrate_from_workflow), so a typo fails before that work starts."""
    for name in ("AIDP_EFFORT_MIGRATE", "AIDP_EFFORT_EVAL", "AIDP_EFFORT_FIX_FIRST_ATTEMPT"):
        _effort_env(name)


# The effort the API applies when a request sends none (Claude API docs, read
# 2026-10-05): medium on claude-opus-5-5, high on the other effort-capable
# models listed. Never sent; it only lets the ledger and the log name the exact
# level a request ran at. A model not listed reports UNRESOLVED, not a guess.
MODEL_DEFAULT_EFFORT = {
    "claude-opus-5-5": "medium", "claude-opus-5": "high",
    "claude-sonnet-5-5": "high", "claude-sonnet-5": "high",
    "claude-fable-5-1": "high", "claude-fable-5": "high",
}
SUMMARY_EFFORT = "low"      # sent by both summary calls (_summarize_cell_code, summarize_notebook)


def effective_effort(sent=None) -> str:
    """The effort a request runs at: the level it sent, else the active model's
    default (UNRESOLVED when MODEL_DEFAULT_EFFORT does not list the model)."""
    if sent:
        return sent
    return MODEL_DEFAULT_EFFORT.get(getattr(llm_config, "MODEL", None), "UNRESOLVED")

# ═══════════════════════════════════════════════════════════════════════

# ─── OCI Auth ─────────────────────────────────────────────────────────

def get_signer():
    _, s = get_oci_signer(OCI_PROFILE)
    return s

SIGNER = None
def signer():
    global SIGNER
    if not SIGNER:
        SIGNER = get_signer()
    return SIGNER

# ─── AIDP Workspace Operations ───────────────────────────────────────

def download_notebook(notebook_path: str, local_path: str) -> bool:
    """Download a notebook from AIDP workspace to local temp file."""
    headers = {"Content-Type": "application/json", "path": notebook_path, "type": "NOTEBOOK"}
    resp = http_requests.post(DOWNLOAD_META_URL, auth=signer(), headers=headers, data="")
    resp.raise_for_status()
    par_url = resp.json().get("parUrl")
    if not par_url:
        return False
    resp = http_requests.get(par_url)
    resp.raise_for_status()
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return True


def create_folder(folder_path: str):
    """Create a folder on the AIDP workspace."""
    http_requests.post(UPLOAD_FOLDER_URL, auth=signer(),
        headers={"path": folder_path, "type": "FOLDER", "description": "Agent migration output"},
        data="{}")


def upload_to_workspace(session: AIDPSession, content: str, remote_path: str) -> bool:
    """Upload content to workspace via the executor (since direct upload API has issues)."""
    # We'll write files via the executor since /Workspace is writable
    # For large content, we base64 encode and decode on the cluster
    import base64
    b64 = base64.b64encode(content.encode('utf-8')).decode('ascii')

    # Split into chunks if too large for a single cell
    MAX_CHUNK = 50000  # Safe size for a single cell execution
    if len(b64) > MAX_CHUNK:
        # Write in chunks
        chunks = [b64[i:i+MAX_CHUNK] for i in range(0, len(b64), MAX_CHUNK)]
        # First chunk creates the file
        code = f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{chunks[0]}'))
print('chunk 0 written')
"""
        asyncio.get_event_loop().run_until_complete(session.execute(code, timeout=30))
        # Append remaining chunks
        for i, chunk in enumerate(chunks[1:], 1):
            code = f"""
import base64, builtins
with builtins.open('{remote_path}', 'ab') as f:
    f.write(base64.b64decode('{chunk}'))
print('chunk {i} written')
"""
            asyncio.get_event_loop().run_until_complete(session.execute(code, timeout=30))
        return True
    else:
        code = f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
print('written')
"""
        result = asyncio.get_event_loop().run_until_complete(session.execute(code, timeout=30))
        return result.get("status") == "ok"


# ─── Claude Opus Calls ───────────────────────────────────────────────

CLIENT = None
def claude():
    global CLIENT
    if not CLIENT:
        # Keep the SDK's transport-layer retries LOW (2, the default). Each SDK
        # retry can burn up to the per-call `timeout` (600s on the big tool-loop
        # calls) waiting on a hung connect, so a high value multiplies with the
        # outer retry loop into 20-min-per-attempt grinds during a sustained
        # outage. The patient, logged, capped-backoff retries are owned by
        # _create_message_with_529_retry (is_conn branch) — the client just
        # smooths over sub-second hiccups.
        CLIENT = llm_config.make_client(api_key=ANTHROPIC_API_KEY, max_retries=2)
    return CLIENT


# ─── fixup_cell Feature State ─────────────────────────────────────────────────

_cell_history: list = []          # job-wide flat list, cleared per job
_current_cell_notes: list = []    # notes for the current cell being processed
_replay_cell_fn = None            # registered from job_migrate.py (avoids circular import)
_repair_dependency_fn = None
_dependency_repair_receipt = contextvars.ContextVar("dependency_repair_receipt", default=0)
# Active notebook repairs are applied to their canonical source owner before
# caller retry; the existing finalizer still has to durably save that owner.
_fixup_source_receipt = contextvars.ContextVar("fixup_source_receipt", default=0)
_fixup_persistence_failures: list = []  # task-scoped, consumed by process_notebook
# Per-call attribution must not cross concurrently migrating dependency tasks.
# The event advances even when the task report deduplicates an identical message.
_fixup_persistence_event_var = contextvars.ContextVar(
    "fixup_persistence_event", default=(0, ""))


def _record_fixup_persistence_failure(message: str) -> None:
    """Retain unique task findings and identify each failing fixup call."""
    event_id, _ = _fixup_persistence_event_var.get()
    _fixup_persistence_event_var.set((event_id + 1, message))
    if message not in _fixup_persistence_failures:
        _fixup_persistence_failures.append(message)


def dependency_repair_receipt() -> int:
    """Successful durable repairs in this execution context, not concurrent tasks."""
    return _dependency_repair_receipt.get()


def _fixup_persistence_event() -> tuple[int, str]:
    return _fixup_persistence_event_var.get()


_current_task_key: str = ""       # set by process_job() before each task
_job_manifest: dict = {}          # set by job_migrate.py
_current_task_params: dict = {}   # merged params (task + job override), set per task
_current_compactor = None         # ContextCompactor for the active call_opus_with_tools call
_compactor_history: list = []     # all compactors created this session (for cross-call file retrieval)

# ─── Usage ledger (token accounting) ──────────────────────────────────────────
#
# `response.usage.input_tokens` is the UNCACHED remainder only. With prompt
# caching on, most billed input arrives as cache_read_input_tokens /
# cache_creation_input_tokens, and thinking is billed as output. The ledger
# records all meters per API request so reports show BILLED tokens (and an
# estimated cost) instead of a partial count. Route = which prompt drove the
# call (migrate / eval / eval_retry / fix / recovery / pass1_migrate / analysis /
# summary / compaction / other).
#
# Set AIDP_USAGE_LOG=/path/file.jsonl to also append one JSON line per request.
USAGE_LEDGER: list = []
_USAGE_LOG_PATH = os.environ.get("AIDP_USAGE_LOG", "")

# Attribution windows. The ledger is process-global, but sibling dependency
# notebooks migrate concurrently (asyncio.gather), so "records since my mark"
# would count a sibling's requests. Each record is tagged with the ids of the
# windows open in the calling task's context (a ContextVar: gather() gives
# every task its own copy), and a window counts only records carrying its id.
# Nested windows (job → notebook) both count a record. Records are made in
# coroutine context (never inside run_in_executor), so the tag is always set.
_USAGE_WINDOWS: contextvars.ContextVar = contextvars.ContextVar("aidp_usage_windows", default=())
_USAGE_WINDOW_IDS = itertools.count(1)

# First-party list prices, USD per million tokens (platform.claude.com pricing,
# read 2026-09-28). Estimate only — Bedrock/Vertex, negotiated rates and
# data-residency multipliers differ. The active model comes from llm_config
# (YAML `llm.model`); unknown ids fall back to the claude-opus-5-5 row.
USD_PER_MTOK_BY_MODEL = {
    "claude-opus-5-5": {"input": 4.0, "cache_read": 0.20, "cache_write_5m": 5.0,
                        "cache_write_1h": 8.0, "output": 20.0},
    "claude-opus-5":   {"input": 5.0, "cache_read": 0.50, "cache_write_5m": 6.25,
                        "cache_write_1h": 10.0, "output": 25.0},
    "claude-sonnet-5": {"input": 2.0, "cache_read": 0.20, "cache_write_5m": 2.5,
                        "cache_write_1h": 4.0, "output": 10.0},
    "claude-haiku-4-5": {"input": 1.0, "cache_read": 0.10, "cache_write_5m": 1.25,
                         "cache_write_1h": 2.0, "output": 5.0},
}
_FALLBACK_RATE_MODEL = "claude-opus-5-5"


def _active_model() -> str:
    m = getattr(llm_config, "MODEL", None)
    return m if isinstance(m, str) and m else _FALLBACK_RATE_MODEL


def _rates_for(model: str) -> dict:
    return USD_PER_MTOK_BY_MODEL.get(model, USD_PER_MTOK_BY_MODEL[_FALLBACK_RATE_MODEL])


def usage_rates_note() -> str:
    """One line describing the price table the est. USD figures used."""
    m = _active_model()
    r = _rates_for(m)
    known = m in USD_PER_MTOK_BY_MODEL
    return (f"Est. USD uses the first-party list price for {m if known else _FALLBACK_RATE_MODEL}"
            f"{'' if known else f' (fallback; {m} not in the rate table)'}: "
            f"input ${r['input']}, cache read ${r['cache_read']}, cache write ${r['cache_write_5m']}/5m "
            f"or ${r['cache_write_1h']}/1h, output ${r['output']} per MTok.")
_USAGE_KEYS = ("input", "cache_read", "cache_write", "cache_write_1h", "output", "thinking")


def _infer_route(system) -> str:
    """Classify a call by its system prompt (used when the caller passes no route)."""
    s = system if isinstance(system, str) else ""
    if s and (s == FIX_PROMPT or s == _FIX_PROMPT_TEMPLATE):
        return "fix"
    if s.startswith("You are migrating a single Databricks cell"):
        return "migrate"
    if s.startswith("You are verifying a migrated Spark cell"):
        return "eval"
    if s.startswith("You are diagnosing an EMPTY-DATA failure"):
        return "recovery"
    if s == MIGRATION_PROMPT:
        return "pass1_migrate"
    if s == ANALYSIS_PROMPT:
        return "analysis"
    return "other"


def _record_usage(route: str, usage, round_num: int = 1, log_fn=None, effort=None) -> dict:
    """Append one API request's token meters to USAGE_LEDGER and log them.
    `effort` = the effort the request sent (None: none sent). The record and the
    log line carry the exact level it ran at (effective_effort), so a run's
    effort is on record, not inferred from its environment.

    Tolerates partial usage objects (tests use SimpleNamespace stubs; older SDKs
    lack output_tokens_details) — every missing meter reads as 0."""
    def _g(obj, name):
        try:
            return int(getattr(obj, name, None) or 0)
        except (TypeError, ValueError):
            return 0
    cache_creation = getattr(usage, "cache_creation", None)
    rec = {
        "ts": round(time.time(), 3),
        "route": route,
        "round": round_num,
        "effort": effective_effort(effort),
        "input": _g(usage, "input_tokens"),
        "cache_read": _g(usage, "cache_read_input_tokens"),
        "cache_write": _g(usage, "cache_creation_input_tokens"),
        "cache_write_1h": _g(cache_creation, "ephemeral_1h_input_tokens"),
        "output": _g(usage, "output_tokens"),
        "thinking": _g(getattr(usage, "output_tokens_details", None), "thinking_tokens"),
        "windows": list(_USAGE_WINDOWS.get()),
    }
    USAGE_LEDGER.append(rec)
    msg = (f"[usage] route={route} round={round_num} in={rec['input']} "
           f"cache_read={rec['cache_read']} cache_write={rec['cache_write']} "
           f"out={rec['output']} thinking={rec['thinking']} effort={rec['effort']}")
    if log_fn:
        log_fn(msg)
    else:
        print(f"  {msg}")
    if _USAGE_LOG_PATH:
        try:
            with open(_USAGE_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
        except OSError as exc:
            print(f"  [usage] WARNING: could not append to {_USAGE_LOG_PATH}: {exc}")
    return rec


def open_usage_window() -> tuple:
    """Open an attribution window in the calling task's context; pass the mark
    to usage_delta, and to close_usage_window when the unit of work ends."""
    wid = next(_USAGE_WINDOW_IDS)
    _USAGE_WINDOWS.set(_USAGE_WINDOWS.get() + (wid,))
    return (len(USAGE_LEDGER), wid)


def close_usage_window(mark: tuple) -> None:
    """Stop tagging later records with this window (safe from any context)."""
    _USAGE_WINDOWS.set(tuple(w for w in _USAGE_WINDOWS.get() if w != mark[1]))


def usage_window_scope():
    """Snapshot the calling context's open windows. end_usage_window_scope(token)
    in a finally closes every window opened since, including one whose own close
    was skipped because the unit of work raised."""
    return _USAGE_WINDOWS.set(_USAGE_WINDOWS.get())


def end_usage_window_scope(token) -> None:
    _USAGE_WINDOWS.reset(token)


def _empty_usage() -> dict:
    d = {k: 0 for k in _USAGE_KEYS}
    d["requests"] = 0
    return d


def _finish_usage(tot: dict) -> dict:
    tot["billed_tokens"] = tot["input"] + tot["cache_read"] + tot["cache_write"] + tot["output"]
    cw_5m = max(tot["cache_write"] - tot["cache_write_1h"], 0)
    r = _rates_for(_active_model())
    tot["est_cost_usd"] = round(
        (tot["input"] * r["input"] + tot["cache_read"] * r["cache_read"]
         + cw_5m * r["cache_write_5m"] + tot["cache_write_1h"] * r["cache_write_1h"]
         + tot["output"] * r["output"]) / 1_000_000, 4)
    return tot


def usage_delta(mark: tuple) -> dict:
    """Aggregate the records made inside the window `mark` (from
    open_usage_window): totals, per-route split, billed_tokens and est_cost_usd."""
    start, wid = mark
    tot = _empty_usage()
    by_route: dict = {}
    for rec in USAGE_LEDGER[start:]:
        if wid not in rec.get("windows", ()):
            continue
        tot["requests"] += 1
        br = by_route.setdefault(rec["route"], _empty_usage())
        br["requests"] += 1
        for k in _USAGE_KEYS:
            tot[k] += rec.get(k, 0)
            br[k] += rec.get(k, 0)
    for br in by_route.values():
        _finish_usage(br)
    tot["by_route"] = by_route
    return _finish_usage(tot)


def combine_usage(usages) -> dict:
    """Sum usage_delta() dicts (e.g. one per migration process of a job).

    Meters and requests are added; billed_tokens is recomputed. est_cost_usd is
    the sum of the recorded estimates, so each part keeps the rates of the model
    it ran on."""
    tot = _empty_usage()
    by_route: dict = {}
    cost, route_cost = 0.0, {}
    for u in usages:
        tot["requests"] += u.get("requests", 0)
        cost += u.get("est_cost_usd", 0) or 0
        for k in _USAGE_KEYS:
            tot[k] += u.get(k, 0)
        for route, br in (u.get("by_route") or {}).items():
            acc = by_route.setdefault(route, _empty_usage())
            acc["requests"] += br.get("requests", 0)
            for k in _USAGE_KEYS:
                acc[k] += br.get(k, 0)
            route_cost[route] = route_cost.get(route, 0) + (br.get("est_cost_usd", 0) or 0)
    for route, br in by_route.items():
        _finish_usage(br)
        br["est_cost_usd"] = round(route_cost[route], 4)
    tot["by_route"] = by_route
    _finish_usage(tot)
    tot["est_cost_usd"] = round(cost, 4)
    return tot


def format_usage_line(u: dict) -> str:
    """One-line human summary of a usage_delta() dict."""
    return (f"requests={u.get('requests', 0)} uncached_in={u.get('input', 0):,} "
            f"cache_read={u.get('cache_read', 0):,} cache_write={u.get('cache_write', 0):,} "
            f"out={u.get('output', 0):,} (thinking {u.get('thinking', 0):,}) "
            f"est_cost≈${u.get('est_cost_usd', 0.0):.2f}")


def format_usage_table(u: dict) -> str:
    """Markdown table of the per-route split of a usage dict."""
    rows = ["| Route | Requests | Uncached in | Cache read | Cache write | Output | Thinking | Est. USD |",
            "|---|---|---|---|---|---|---|---|"]
    for route, br in sorted((u.get("by_route") or {}).items()):
        rows.append(f"| {route} | {br['requests']} | {br['input']:,} | {br['cache_read']:,} | "
                    f"{br['cache_write']:,} | {br['output']:,} | {br['thinking']:,} | {br['est_cost_usd']:.2f} |")
    return "\n".join(rows)


class FixupPersistenceError(RuntimeError):
    """A replay repair has no durable destination; more model retries cannot help."""


# ─── run_on_cluster kernel-global shadow guard ───────────────────────────────
#
# `run_on_cluster` executes Opus's diagnostic in the SAME PySpark kernel that
# runs the customer's migrated cells. If Opus's code rebinds a name AIDP's
# per-command prologue depends on (most notoriously `sc`), every SUBSEQUENT
# cell — including any recovery attempt — fails on the prologue call
# `sc.setJobGroup(<cmd-id>, ...)`. The kernel becomes UNRECOVERABLE without
# a restart (which loses ALL customer state).
#
# The prompt-side NAMESPACE HYGIENE block tells Opus not to use these names,
# but has been violated in real runs:
#   a pilot-customer AutoRefit 2026-07-26:   `import aidp_compat.s3_compat as sc`
#   the pilot deployment Model_Mon 2026-07-28: `sc = [c for c in df.columns if 'source' in c]`
# Prompt discipline is probabilistic — this AST scanner makes prevention
# deterministic. Runs BEFORE session.execute() so a bad diagnostic never
# reaches the kernel.
#
# Heal-after-execute was tried and FAILED: the AIDP prologue fires BEFORE
# the heal body, so on a poisoned kernel the heal itself can't execute.
# See git history 2026-07-28 for that dead end. PREVENTION IS THE ONLY FIX.
_RESERVED_KERNEL_GLOBALS = frozenset({"sc", "spark", "dbutils", "oidlUtils"})


def _names_bound_at_module_scope(tree: 'ast.AST') -> set:
    """Return the set of names that `tree` would bind at MODULE scope.

    Walks the body of a Module (or any block that acts like module top-level),
    collecting binding names from assignments, imports, defs, and loops.
    Skips names inside FunctionDef / AsyncFunctionDef / ClassDef bodies —
    those are function/class-local and don't leak to the kernel namespace.

    Returns a set of str. Callers filter against _RESERVED_KERNEL_GLOBALS.
    """
    import ast
    names: set = set()

    def _add_from_target(t):
        # A single assignment target may be a Name (simple), Tuple/List
        # (unpacking), or Starred (a, *b = ...). Recurse for Tuple/List/Starred.
        # Subscript / Attribute are NOT bindings of new names — skip them.
        if isinstance(t, ast.Name):
            names.add(t.id)
        elif isinstance(t, (ast.Tuple, ast.List)):
            for elt in t.elts:
                _add_from_target(elt)
        elif isinstance(t, ast.Starred):
            _add_from_target(t.value)
        # Subscript / Attribute intentionally ignored — those mutate an
        # existing object, they don't bind a new global.

    for node in tree.body:
        # a) Simple / annotated / augmented assignment
        if isinstance(node, ast.Assign):
            for t in node.targets:
                _add_from_target(t)
        elif isinstance(node, ast.AnnAssign):
            _add_from_target(node.target)
        elif isinstance(node, ast.AugAssign):
            # `sc += 1` requires an existing binding, but Python semantically
            # rebinds the name to the augmented value. For safety, treat as a
            # binding — a user writing `sc += 1` in Opus's code is a red flag.
            _add_from_target(node.target)

        # b) Imports (name and alias forms)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                bound = alias.asname or alias.name
                # For `import a.b` (no asname) the bound name is `a`.
                if not alias.asname and "." in bound:
                    bound = bound.split(".", 1)[0]
                # `from x import *` uses "*" — nothing we can pre-flag reliably;
                # skip so we don't over-block legitimate wildcard imports.
                if bound != "*":
                    names.add(bound)

        # c) Function / async function / class definitions bind the def name
        #    at MODULE scope; their bodies do NOT leak (that's the whole point
        #    of function-scoping). Add only the def's own name.
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)

        # d) for / while / with — the loop/with itself is at module level, so
        #    both the loop variable / with-target AND the body's bindings leak
        #    to the module namespace. Recurse into the body so a `sc = ...`
        #    inside a `for` body gets flagged (the Model_Mon 2026-07-28 shape).
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            _add_from_target(node.target)
            names.update(_names_bound_at_module_scope(ast.Module(body=node.body, type_ignores=[])))
            names.update(_names_bound_at_module_scope(ast.Module(body=node.orelse, type_ignores=[])))
        elif isinstance(node, ast.While):
            names.update(_names_bound_at_module_scope(ast.Module(body=node.body, type_ignores=[])))
            names.update(_names_bound_at_module_scope(ast.Module(body=node.orelse, type_ignores=[])))
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if item.optional_vars is not None:
                    _add_from_target(item.optional_vars)
            names.update(_names_bound_at_module_scope(ast.Module(body=node.body, type_ignores=[])))

        # e) try / if — recurse into their branches (module scope continues)
        elif isinstance(node, ast.Try):
            names.update(_names_bound_at_module_scope(node))  # walks node.body
            for handler in node.handlers:
                # `except Exception as e:` — `e` is bound in the handler.
                if handler.name:
                    names.add(handler.name)
                # Recurse into handler body — bindings inside the except leak
                names.update(_names_bound_at_module_scope(ast.Module(body=handler.body, type_ignores=[])))
            for h_body in (node.orelse, node.finalbody):
                names.update(_names_bound_at_module_scope(ast.Module(body=h_body, type_ignores=[])))
        elif isinstance(node, ast.If):
            names.update(_names_bound_at_module_scope(ast.Module(body=node.body, type_ignores=[])))
            names.update(_names_bound_at_module_scope(ast.Module(body=node.orelse, type_ignores=[])))

        # f) match / case (Python 3.10+): capture patterns like `case P as sc:`
        #    bind `sc` in the enclosing scope. Names inside patterns (e.g.
        #    `case [a, b]:`) also bind in the enclosing scope.
        elif isinstance(node, ast.Match):
            for case in node.cases:
                _collect_match_pattern_names(case.pattern, names)
                # Case body executes at match's scope — recurse for any bindings inside.
                names.update(_names_bound_at_module_scope(ast.Module(body=case.body, type_ignores=[])))

        # g) Walrus operator (ast.NamedExpr, 3.8+): can appear anywhere an
        #    expression can. At module scope, `if (sc := compute()):` or
        #    `[x for x in xs if (sc := f(x))]` both leak `sc` to module scope
        #    (walrus binds in the enclosing scope, not the containing
        #    comprehension). the reviewer's PR-35 review (2026-07-28) called this
        #    out — module-scope walrus was undetected by the earlier version.
        #    Scan all sub-expressions of the current top-level node, stopping
        #    at function/class/lambda boundaries (those are new scopes).
        names.update(_walrus_target_names_at_this_scope(node))

    return names


def _collect_match_pattern_names(pattern, out):
    """Recursively collect all `Name.id`s bound by a match `pattern` node.

    Handles: MatchAs (`case X as name` — the `as name` binding), MatchStar
    (`case [*rest]` — the `*rest` binding), MatchMapping (**rest), MatchOr
    (recurse into alternatives), MatchSequence / MatchClass (recurse into
    child patterns).
    """
    import ast
    if pattern is None:
        return
    if isinstance(pattern, ast.MatchAs):
        # `case X as name:` — name binds. Also `case name:` alone (MatchAs
        # with pattern=None and name set) captures.
        if pattern.name:
            out.add(pattern.name)
        _collect_match_pattern_names(pattern.pattern, out)
    elif isinstance(pattern, ast.MatchStar):
        if pattern.name:
            out.add(pattern.name)
    elif isinstance(pattern, ast.MatchMapping):
        if pattern.rest:
            out.add(pattern.rest)
        for sub in pattern.patterns:
            _collect_match_pattern_names(sub, out)
    elif isinstance(pattern, ast.MatchOr):
        for sub in pattern.patterns:
            _collect_match_pattern_names(sub, out)
    elif isinstance(pattern, ast.MatchSequence):
        for sub in pattern.patterns:
            _collect_match_pattern_names(sub, out)
    elif isinstance(pattern, ast.MatchClass):
        for sub in pattern.patterns:
            _collect_match_pattern_names(sub, out)
        for sub in pattern.kwd_patterns:
            _collect_match_pattern_names(sub, out)


def _walrus_target_names_at_this_scope(node):
    """Return the set of `NamedExpr.target.id`s in `node`'s subtree that
    are at THIS scope — i.e. NOT inside a nested function / class / lambda
    body (those are new scopes and don't leak walrus bindings out).

    Walrus binds in the enclosing scope, including from within
    comprehensions (that's the whole point of walrus in comps — leaking
    the loop-var out). So we walk everything except scope-creating nodes.
    """
    import ast
    out = set()
    # Simple traversal that stops at scope boundaries.
    stack = [node]
    while stack:
        cur = stack.pop()
        # Stop at scope-creating nodes — their walrus bindings are local.
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef,
                            ast.ClassDef, ast.Lambda)):
            continue
        if isinstance(cur, ast.NamedExpr) and isinstance(cur.target, ast.Name):
            out.add(cur.target.id)
        for child in ast.iter_child_nodes(cur):
            stack.append(child)
    return out


def _detect_kernel_global_shadow(code: str, *, reserved_globals=None):
    """AST-scan `code` for top-level bindings of reserved kernel globals.

    Returns None if `code` is safe. Returns a rejection message string if
    `code` would poison the kernel by rebinding `sc`, `spark`, `dbutils`, or
    `oidlUtils` at module scope. Customer execution supplies its platform-owned
    subset explicitly; the default diagnostic policy is unchanged.

    Never raises — a SyntaxError in Opus's code is left to AIDP to report
    (returning None here lets the normal execution path handle it).
    """
    import ast
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return None  # AIDP kernel will surface the syntax error
    bound = _names_bound_at_module_scope(tree)
    shadowed = sorted(bound & (_RESERVED_KERNEL_GLOBALS if reserved_globals is None
                               else frozenset(reserved_globals)))
    if not shadowed:
        return None
    if reserved_globals is not None:
        return (
            f"BLOCKED before execution: this cell rebinds AIDP kernel global(s) "
            f"{shadowed}. AIDP calls sc.setJobGroup(...) BEFORE every command; "
            "replacing sc with a helper function, import or other value prevents "
            "all subsequent commands from executing, including widget refresh. "
            "This candidate cell has NOT run; the guard left the kernel binding unchanged. "
            "Repair the customer code through the model. If the binding merely "
            "re-derives the platform SparkContext (for example sc = spark.sparkContext "
            "or sc = SparkContext.getOrCreate()), preserve that redundant assignment "
            "as a comment tagged Oracle tool modification and use the existing sc; "
            "keep its SparkContext consumers unchanged. Do not rename those consumers. "
            "For a conflicting customer helper/alias, give it a non-reserved name "
            "and update its references in this "
            "cell and dependent/parent notebooks. Preserve original lines with "
            "Oracle tool modification comments. Use notebook source and history "
            "tools to find consumers; repair_dependency/fixup_cell can repair "
            "earlier definitions and references. Do not remove functionality, "
            "reset sc, or fabricate defaults. Submit the repair for execution "
            "and verification. Resolve the collision before execution to avoid poisoning the kernel."
        )
    return (
        f"BLOCKED: your diagnostic code rebinds reserved kernel global(s) "
        f"{shadowed} at module scope. AIDP's per-command prologue calls "
        f"`sc.setJobGroup(<cmd-id>, ...)` — any rebinding here POISONS the "
        f"kernel for every subsequent cell in this task and downstream tasks. "
        f"NO recovery is possible short of a kernel restart (which loses ALL "
        f"customer state — imports, variables, cached DataFrames).\n"
        f"FIX: rename to a `_probe_`-prefixed name (e.g. `_probe_sc`, "
        f"`_sc_local`), OR wrap your diagnostic in a function so bindings "
        f"stay function-local:\n"
        f"    def _probe():\n"
        f"        {shadowed[0]} = ...   # local, doesn't leak\n"
        f"        print(...)\n"
        f"    _probe()\n"
        f"Then re-submit."
    )


# ─── run_on_cluster read-only guard ─────────────────────────────────────────
#
# `run_on_cluster` is a diagnostic tool, but it shares the same live Spark
# session as migrated-cell validation.  A diagnostic that calls INSERT (or a
# DataFrame writer) therefore mutates customer data outside the migrated cell's
# normal sandbox/redirect path.  This happened in ai_tool_DataProxy_O1_FT on
# 2026-07-29: three "inspect the error" probes executed the failing INSERT and
# spent ~7.5 minutes filtering/writing Delta files.
#
# Keep this guard deliberately scoped to run_on_cluster.  Migrated cells still
# go through the existing cell-text and writer-wrapper sandbox redirects.
_READ_ONLY_SQL_COMMANDS = frozenset({
    "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "VALUES", "TABLE",
    "REFRESH", "SET",
})
_MUTATING_SQL_COMMANDS = frozenset({
    "ALTER", "ANALYZE", "COMMENT", "COPY", "CREATE", "DELETE", "DROP",
    "GRANT", "INSERT", "LOAD", "MERGE", "MSCK", "OPTIMIZE", "REPLACE",
    "RESTORE", "REVOKE", "TRUNCATE", "UPDATE", "VACUUM",
})
_SPARK_WRITER_ATTRIBUTES = frozenset({
    "write", "writeStream", "writeTo",
})


def _static_string_value(node, bindings):
    """Best-effort static value for a SQL argument.

    Formatted values become ``{...}``, preserving the SQL command around them.
    Returning None means the value cannot be proven statically.
    """
    import ast
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return bindings.get(node.id)
    if isinstance(node, ast.JoinedStr):
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                parts.append("{...}")
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_string_value(node.left, bindings)
        right = _static_string_value(node.right, bindings)
        if left is not None and right is not None:
            return left + right
    # "SELECT ... {}".format(value) — formatting does not change the command.
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "format"):
        return _static_string_value(node.func.value, bindings)
    return None


def _sql_without_comments_or_literals(sql_text: str, strict: bool = False,
                                      keep_literals: bool = False):
    """Mask SQL comments and quoted literals while preserving SQL keywords.

    strict=True returns None instead of a masking when this lexer and Spark's
    could disagree about where a literal or comment ends: a backslash inside a
    quoted literal (Spark treats \\' as an escaped quote), a `/*` inside a block
    comment (Spark nests them), or text ending inside a literal or comment.
    keep_literals=True masks comments only, so literal edits stay visible. The
    result is always the same length as the input.
    """
    # This is a conservative lexer, not a full SQL parser.  It handles the
    # quoting/comment forms used by Spark SQL and makes mutation keywords inside
    # comments or string values invisible to the command classifier.
    out = []
    i = 0
    quote = None
    while i < len(sql_text):
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < len(sql_text) else ""
        if quote:
            if strict and ch == "\\":
                return None
            if ch == quote:
                if nxt == quote:       # SQL doubled quote: '' or ""
                    out.extend((ch, nxt) if keep_literals else (" ", " "))
                    i += 2
                    continue
                quote = None
            out.append(ch if keep_literals else " ")
            i += 1
            continue
        if ch in ("'", '"', "`"):
            quote = ch
            out.append(ch if keep_literals else " ")
            i += 1
            continue
        if ch == "-" and nxt == "-":
            while i < len(sql_text) and sql_text[i] not in "\r\n":
                out.append(" ")
                i += 1
            continue
        if ch == "/" and nxt == "*":
            out.extend((" ", " "))
            i += 2
            closed = False
            while i < len(sql_text):
                if sql_text[i:i + 2] == "*/":
                    out.extend((" ", " "))
                    i += 2
                    closed = True
                    break
                if strict and sql_text[i:i + 2] == "/*":
                    return None
                out.append(" ")
                i += 1
            if strict and not closed:
                return None
            continue
        out.append(ch)
        i += 1
    if strict and quote:
        return None
    return "".join(out)


def _sql_mutation_reason(sql_text: str):
    """Return a reason when SQL is mutating or cannot be classified read-only."""
    masked = _sql_without_comments_or_literals(sql_text)
    words = re.findall(r"[A-Za-z_]+", masked.upper())
    if not words:
        return "empty or unclassifiable Spark SQL"

    command = words[0]
    # EXPLAIN is explicitly safe even when the explained plan is an INSERT.
    if command == "EXPLAIN":
        return None
    if command in _MUTATING_SQL_COMMANDS:
        return f"mutating Spark SQL command {command}"
    if command == "WITH":
        # Spark permits WITH ... INSERT/MERGE/UPDATE/DELETE.  A CTE whose outer
        # command is SELECT contains none of these mutation commands.
        found = next((word for word in words if word in _MUTATING_SQL_COMMANDS), None)
        if found:
            return f"mutating Spark SQL command {found} after WITH"
        return None
    if command in _READ_ONLY_SQL_COMMANDS:
        return None
    return f"Spark SQL command {command} is not proven read-only"


# Partition-repair statements: unsupported AND unnecessary on AIDP (no partition
# registry, verified 2026-10-13). They are the only statements the top-level
# %sql repair loop lets the model change. Matched against comment/literal-masked
# statement text; a backticked table name is masked away and does not match.
_SQL_PARTITION_REPAIR_RE = re.compile(
    r"^\s*(?:MSCK\s+REPAIR\s+TABLE\s+(?P<msck>\w[\w.]*)(?:\s+(?:ADD|DROP|SYNC)\s+PARTITIONS)?"
    r"|ALTER\s+TABLE\s+(?P<alter>\w[\w.]*)\s+RECOVER\s+PARTITIONS)\s*$",
    re.IGNORECASE)
# Other statements that touch only catalog metadata or session state, never
# table data: cache control, USE, and session-local temp views.
_SQL_METADATA_ONLY_RE = re.compile(
    r"^\s*(?:(?:UN)?CACHE\s+(?:LAZY\s+)?TABLE\b"
    r"|CLEAR\s+CACHE\s*$"
    r"|USE\b"
    r"|CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW\b)",
    re.IGNORECASE)


def _sql_repair_block_reason(sql_cell: str):
    """Why a %sql cell must NOT be handed to the model for repair, or None.

    Top-level %sql cells execute without the write sandbox, so a model repair
    may only run when every statement is read-only or metadata-only. Fails
    closed: an empty or unclassifiable statement is a reason to block.
    """
    body = (sql_cell or "").lstrip()
    if body.startswith("%sql"):
        body = body[len("%sql"):]
    masked = _sql_without_comments_or_literals(body, strict=True)
    if masked is None:
        return ("SQL with backslash escapes or nested/unterminated literals or "
                "comments is not classified")
    statements = [s for s in masked.split(";") if s.strip()]
    if not statements:
        return "empty or unclassifiable Spark SQL"
    for statement in statements:
        if _SQL_PARTITION_REPAIR_RE.match(statement) or _SQL_METADATA_ONLY_RE.match(statement):
            continue
        reason = _sql_mutation_reason(statement)
        if reason:
            return reason
    return None


def _is_spark_sql_call(node):
    """Whether an AST Call is a standard Spark/PySpark SQL entry point."""
    import ast
    if not isinstance(node, ast.Call):
        return False
    if isinstance(node.func, ast.Attribute) and node.func.attr == "sql":
        return True
    # aidp_compat exposes `sql` alongside `spark`; guard that convenience form.
    return isinstance(node.func, ast.Name) and node.func.id == "sql"


def _detect_run_on_cluster_mutation(code: str):
    """Reject Spark mutations before run_on_cluster reaches the live kernel.

    Returns None for code proven free of mutating Spark SQL/DataFrame writer
    operations, otherwise an actionable BLOCKED message.  Unknown/dynamic SQL
    is denied because a hard read-only boundary must fail closed.
    """
    import ast
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return None  # preserve the normal kernel syntax-error path

    bindings = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value_node = node.value
            value = _static_string_value(value_node, bindings)
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    if value is None:
                        bindings.pop(target.id, None)
                    else:
                        bindings[target.id] = value

    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in _SPARK_WRITER_ATTRIBUTES:
            # ``df.write`` is a property and is used as
            # ``df.write.mode(...).save(...)``. A direct call ``obj.write(...)``
            # is therefore not a Spark writer; it is commonly
            # ``sys.stdout.write(...)`` or a local file handle. Keep bare
            # ``df.write`` access blocked, and keep writeStream/writeTo
            # unconditional because they do not collide with stdlib APIs.
            parent = parents.get(node)
            if (
                node.attr == "write"
                and isinstance(parent, ast.Call)
                and parent.func is node
            ):
                continue
            return (
                f"BLOCKED: run_on_cluster is strictly read-only; DataFrame "
                f"`.{node.attr}` writer operations are not allowed. This guard "
                f"only applies to diagnostics—normal migrated-cell sandbox "
                f"redirects are unchanged. Inspect the schema/plan with "
                f"`printSchema()`, `show()`, or `explain()` instead."
            )
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "getattr" and len(node.args) >= 2):
            attr_name = _static_string_value(node.args[1], bindings)
            if attr_name in _SPARK_WRITER_ATTRIBUTES:
                return (
                    f"BLOCKED: run_on_cluster is strictly read-only; dynamic "
                    f"access to DataFrame `.{attr_name}` is not allowed."
                )

        if not _is_spark_sql_call(node):
            continue
        if not node.args:
            return (
                "BLOCKED: run_on_cluster is strictly read-only; Spark SQL "
                "without a statically verifiable query is not allowed."
            )
        sql_text = _static_string_value(node.args[0], bindings)
        if sql_text is None:
            return (
                "BLOCKED: run_on_cluster is strictly read-only; this Spark SQL "
                "argument is dynamic and cannot be proven read-only. Use a "
                "literal SELECT/DESCRIBE/SHOW/EXPLAIN query instead."
            )
        reason = _sql_mutation_reason(sql_text)
        if reason:
            return (
                f"BLOCKED: run_on_cluster is strictly read-only ({reason}). "
                f"Do not execute writes to reproduce an error. Use `EXPLAIN "
                f"<statement>`, DESCRIBE, SELECT, or DataFrame `explain()` for "
                f"diagnosis. Normal migrated-cell sandbox redirects are unchanged."
            )
    return None


# ─── Pip Install Tracking ────────────────────────────────────────────────────

_installed_packages: set = set()  # package names installed via pip during migration, cleared per job
_table_lookup_cache: dict = {}   # table name -> result, avoids repeated lookups within a run
_path_explore_cache: dict = {}   # explored path -> result, avoids repeated explore_path calls

# Regex patterns for detecting pip install commands
_PIP_SHELL_RE = re.compile(
    r'(?:^|\n)\s*(?:!|%)?pip\s+install\s+(.+)', re.IGNORECASE)
_PIP_SUBPROCESS_RE = re.compile(
    r'subprocess\.(?:check_call|run|call)\s*\(\s*\[.*?["\']install["\']\s*,\s*(.+?)\]',
    re.IGNORECASE | re.DOTALL)

_PIP_FLAGS = {'-q', '--quiet', '-U', '--upgrade', '--no-cache-dir', '--user',
              '--force-reinstall', '--no-deps', '--pre', '--no-warn-script-location'}
_PIP_FLAGS_WITH_ARG = {'--index-url', '-i', '--extra-index-url', '--trusted-host',
                       '-t', '--target', '--prefix', '-c', '--constraint'}


def extract_pip_packages(code: str) -> list:
    """Extract package names from pip install commands in code.
    Returns list of package specifiers (e.g. ['pandas', 'scikit-learn==1.3.0'])."""
    packages = []

    # Shell-style: pip install pkg1 pkg2, %pip install pkg, !pip install pkg
    for m in _PIP_SHELL_RE.finditer(code):
        args = m.group(1).strip()
        # Stop at comment or line continuation
        args = args.split('#')[0].strip()
        packages.extend(_filter_pip_args(args.split()))

    # subprocess.check_call([sys.executable, "-m", "pip", "install", "pkg"])
    for m in _PIP_SUBPROCESS_RE.finditer(code):
        raw = m.group(1).strip()
        # Extract quoted strings from the list literal
        tokens = re.findall(r'["\']([^"\']+)["\']', raw)
        packages.extend(_filter_pip_args(tokens))

    return packages


def _filter_pip_args(tokens: list) -> list:
    """Filter out pip flags, keep only package specifiers."""
    result = []
    skip_next = False
    for tok in tokens:
        if skip_next:
            skip_next = False
            continue
        if tok in _PIP_FLAGS:
            continue
        if tok in _PIP_FLAGS_WITH_ARG:
            skip_next = True
            continue
        if tok.startswith('-'):
            continue
        # Skip sys.executable, python references
        if tok in ('sys.executable', 'python', 'python3'):
            continue
        if tok.strip():
            result.append(tok.strip())
    return result


def clear_installed_packages():
    """Clear the set of tracked pip-installed packages. Called per job."""
    _installed_packages.clear()
    _table_lookup_cache.clear()
    _path_explore_cache.clear()


def get_installed_packages() -> list:
    """Return sorted list of pip-installed packages tracked during this job."""
    return sorted(_installed_packages)


def register_replay_fn(fn):
    global _replay_cell_fn
    _replay_cell_fn = fn


def register_dependency_repair_fn(fn):
    global _repair_dependency_fn
    _repair_dependency_fn = fn


def set_current_task_key(task_key: str):
    """Set the current task key for get_job_parameters tool."""
    global _current_task_key
    _current_task_key = task_key


def set_job_manifest(manifest: dict):
    """Set the job manifest for get_job_parameters tool."""
    global _job_manifest
    _job_manifest = manifest


def set_current_task_params(params: dict):
    """Set the merged parameters for the current task (called from process_job)."""
    global _current_task_params
    _current_task_params = dict(params) if params else {}


async def _summarize_cell_code(code: str) -> str:
    """One-sentence description of what the code does (used in history summaries)."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: claude().messages.create(
            model=llm_config.MODEL,
            # Adaptive thinking is always on for the Opus-5 family and cannot be
            # disabled on claude-opus-5-5 (400). It counts against max_tokens, so
            # the former 80-token cap could be consumed by thinking alone;
            # content[0] was then a thinking block, `.text` raised, and the call
            # fell back to a code snippet (paid request, result discarded).
            # Effort `low` keeps the thinking minimal; 300 tokens leaves room for
            # it plus one sentence.
            max_tokens=300,
            timeout=30,
            output_config={"effort": SUMMARY_EFFORT},
            **llm_config.thinking_kwargs(),
            messages=[{"role": "user", "content":
                f"In one sentence, what does this Python/PySpark code do?\n\n```python\n{code[:1000]}\n```\n\nReply with just the sentence, no preamble."}]
        ))
        _record_usage("summary", resp.usage, effort=SUMMARY_EFFORT)
        text = " ".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()
        return text or code.strip()[:120].replace("\n", " ")
    except Exception:
        return code.strip()[:120].replace("\n", " ")


def _format_history_context() -> str:
    """Format the most recent HISTORY_CONTEXT_ENTRIES history entries as context for Opus."""
    if not _cell_history:
        return ""
    n = len(_cell_history)
    entries = _cell_history[-HISTORY_CONTEXT_ENTRIES:]
    lines = [
        f"=== JOB EXECUTION HISTORY (last {len(entries)} of {n} cells) ===",
        "Context: Cells execute sequentially across notebooks. Child notebooks inlined via %run "
        "execute in the parent's kernel namespace. Use get_cell_history/get_history_entry to inspect, "
        "fixup_cell(start_index, why) to rewind and replay, make_note to annotate this cell.",
        "",
    ]
    for e in entries:
        child_tag = "[child] " if e.get("is_child") else ""
        nb = e["notebook_path"]
        note_tag = f" | note: {e['last_note'][:60]}" if e.get("last_note") else ""
        repair_tag = (" | repair_dependency available (completed helper; save before retrying caller)"
                      if e.get("_task_key", "") == _current_task_key
                      and getattr(e.get("_child_replay_owner"), "completed_repair", None) else "")
        lines.append(
            f"[{e['index']}] {child_tag}{nb} cell {e['cell_idx']} | {e['status'].upper()} | "
            f"{e['summary'][:100]}{note_tag}{repair_tag}"
        )
    return "\n".join(lines)


ANALYSIS_PROMPT = """You are a Databricks-to-Oracle-AIDP migration analyst. Oracle AIDP runs open-source Apache Spark 3.5.0 (Scala 2.12.18, Java 17 GraalVM, Python 3.11). It does NOT include any Databricks proprietary runtime, libraries, or services.

## AIDP Environment (confirmed by testing):
- Pre-installed JARs: Delta Lake 3.2, Avro, OCI HDFS connector (BmcFilesystem)
- Installed JARs: whatever the exporter staged from the source job's library spec (plus the Hudi bundle); never assume a class resolves — verify on error
- Pre-installed Python: pandas 2.3.3, numpy 2.4.2, requests, oci, nbformat, ray 2.54, slack_sdk, boto3, delta, IPython
- Installed Python (via requirements.txt): matplotlib, scikit-learn, xgboost, seaborn, plotly, tqdm, etc.
- MLflow may NOT be pre-installed — install with pip install mlflow (no version pin) if needed
- Auth: OCI API key credentials fetched at cluster runtime from AIDP Credential Store via
  `aidputils.secrets.get(os.environ['AIDP_CRED_OCI_NAME'], key=...)`. Credential name and per-field key
  names come from required env vars (AIDP_CRED_OCI_NAME, AIDP_CRED_OCI_KEY_*) set by the
  migrator's kernel and saved-notebook bootstrap from cfg.aidp.oci_credentials.
  Never invent a credential name or field mapping; missing configuration must fail.
  Never a config file on the cluster.
  FORBIDDEN: oci.auth.signers.get_resource_principals_signer() — resource principal has known
  failure modes on AIDP and MUST NOT be used. Use API key + Signer pattern always.
- Filesystem: /Workspace/ is local, oci:// paths work via BmcFilesystem (configured with API key)
- SparkSession pre-initialized as `spark`, SparkContext as `sc`
- Libraries should be installed via cluster libraries section (not pip install at runtime)
- spark.jars is immutable at runtime
- aidp_compat installed as a cluster library (just import it, no sys.path needed)

## AIDP Compatibility Features (DO NOT flag these):
- display() - native on AIDP, Spark DataFrames ONLY. For other types, use the
  library's NATIVE display function or IPython's `Idisplay` (already in bootstrap):
    Spark DataFrame:    `df.display()`   -> `display(df)`           (AIDP native builtin, no import)
    Pandas DataFrame:   `df.display()`   -> `df` as last expression (Jupyter auto-render)
                                            or `Idisplay(df)`       (IPython.display, bootstrap-imported)
    matplotlib figure:  `fig.display()`  -> `plt.show()`            (matplotlib native)
    plotly figure:      `fig.display()`  -> `fig.show()`            (plotly native)
    PIL Image:          `img.display()`  -> `img.show()`            (PIL native)
    numpy / dict / etc: `obj.display()`  -> `print(obj)`            (Python native)
  Rule: NEVER pass non-Spark objects to AIDP's display(). Use the library's own
  display function for the type at hand.
  `aidp_compat` does NOT export `display` — writing `from aidp_compat import display`
  is a hard ImportError. Do not reach for it as an escape hatch.
  NEVER add a `DataFrame.display = _display_patch` monkey-patch (toPandas-based shims are slow).
- dbutils.fs - available via aidp_compat
- aidp_dbutils - the pilot deployment's existing shim
- %run magic - available on AIDP
- %sql magic - available on AIDP
- %scala magic - AIDP supports Scala
- oidlUtils - AIDP native
- Open Source Delta Lake and Delta Sharing
- Reading from Volume paths
- pandas, numpy - pre-installed

## INCOMPATIBLE FEATURES - Flag with severity:

### HIGH severity:
- Any Databricks internal API reference
- Any AWS Cloud references (S3 paths, boto3 for AWS-specific ops, Glue, Redshift)
- dbutils.notebook.getContext().notebookPath
- Reading and writing same path in single operation (works in Databricks, fails in OSS Spark)
- UDFs defined in one language used in another (AIDP has separate Spark sessions for Python & Scala)
- Databricks Connect, Databricks SDK
- Unity Catalog three-level namespace (catalog.schema.table)
- OPTIMIZE / ZORDER (Databricks-only)
- Databricks Feature Store, AutoML

### MEDIUM severity:
- MLflow Databricks-specific integrations
- spark.databricks.* configurations
- Photon engine references
- Structured Streaming Databricks-specific sources/sinks

### LOW severity:
- dbutils.widgets -> aidputils.widgets (Python and Scala) (comment out original, add replacement)
- Custom utility notebook references (available in workspace)
- get_glue_table_s3_location
- DBFS paths (work after Volume migration)

## FEATURE MAPPINGS (native aidputils.widgets is supported in Python and Scala; widget values are shared across languages, live-verified 2026-10-13):
- dbutils.notebook.exit : LEAVE AS-IS — tool renames deterministically (do NOT rename/comment/duplicate).
- dbutils.notebook.run  -> aidputils.notebook.run    [PYTHON]  /  oidlUtils.notebook.run    [%scala]
- dbutils.jobs.taskValues.set -> oidlUtils.parameters.setTaskValue    (CROSS-TASK — keep oidl in BOTH Python & Scala; aidputils has no taskValues API)
- dbutils.jobs.taskValues.get -> oidlUtils.parameters.getParameter     (CROSS-TASK — keep oidl)
- dbutils.widgets.get("name") -> aidputils.widgets.get("name")   [PYTHON and Scala]
  (native aidp widgets API — one call reads the notebook default / job-task param / notebook.run param / taskValue, and RAISES on a truly-unset widget like Databricks; do NOT add a compound read or a hardcoded default)
- dbutils.widgets.text/dropdown/combobox/multiselect(...) -> aidputils.widgets.*(...)  [PYTHON and Scala] — KEEP them (they register the widget so .get returns the default); getAll/remove/removeAll -> aidputils.widgets.*; Scala uses the same native widget APIs

## REPORT FORMAT:
1. **Compatibility Status**: GREEN / YELLOW / RED
2. **Notebook Description**: What does this notebook do (1-2 sentences)
3. **Notebooks Called**: All notebooks called via %run, dbutils.notebook.run, or APIs
4. **Jobs Called**: All jobs triggered via Databricks Job API
5. **Data Dependencies**: ALL tables, storage paths, volumes - mark READ or WRITE
6. **Incompatible Code**: Each issue with cell number, code snippet, severity, and migration action. Do NOT highlight markdown cells. Mask any sensitive tokens/passwords.
7. **External Dependencies**: imports, libraries, JARs, APIs that may not be available
8. **Migration Challenges**: Anything needing additional work beyond code changes
9. **Migration Summary Table**: | Notebook | Status | Issues | Effort (Low/Medium/High/Very High) |

COMMENT-AWARENESS: Only analyze ACTIVE (uncommented) code. Ignore patterns inside:
- Python: # line comments, triple-quoted strings (\"\"\"...\"\"\", '''...''')
- Scala: // line comments, /* ... */ block comments
- SQL: -- line comments
Commented-out code (e.g. "# import boto3", "# s3://old-bucket/data") is dead code from previous
migrations or developer cleanup — do NOT flag it as incompatible or include it in your report.

Be EXHAUSTIVE about ACTIVE code. Check every uncommented cell, import, path, and API call."""


MIGRATION_PROMPT = """You are migrating a Databricks notebook to Oracle AIDP (Spark 3.5.0, Python 3.11).

You will receive the notebook AND its analysis report. Produce a JSON object with:

1. "cells": Array of migrated cells, each with:
   - "cell_type": "code" | "markdown" | "raw"
   - "source": the migrated source code
   - "classification": "READ_ONLY" | "WRITE" | "NOTIFICATION" | "SKIP"

   Classification rules (be PRECISE):
   READ_ONLY: imports, reads, queries, transforms, display, function/class definitions, variable assignments, print, F.to_json(), spark.read.*, createOrReplaceTempView, .show(), .count()
   WRITE: df.write.*, saveAsTable, insertInto, CREATE TABLE (non-temp), DROP TABLE, DELETE, INSERT, os.remove, shutil.rmtree, put_object, delete_object
   NOTIFICATION: Slack SDK, email, webhook posts to notification services
   SKIP: empty, markdown, raw cells

2. "migration_notes": Array of strings describing each change

## Migration rules:
- First code cell must be:
  ```
  from aidp_compat import dbutils, translate_path
  ```
- %pip install: Comment out original with "# AIDP: install via cluster libraries", then add subprocess pip install so library is available during migration testing
- Convert %sh to subprocess.run
- Replace `from aidp_dbutils import _DBUtils` / `dbutils = _DBUtils(oidlUtils)` with the aidp_compat import
- dbutils.widgets.get("name") -> aidputils.widgets.get("name")   [PYTHON and Scala cells]. aidputils.widgets.get
  reads the widget default / job-task param / notebook.run param / taskValue from ONE native API and RAISES on a
  genuinely-unset widget (like Databricks), so the customer's try/except -> default works. Do NOT add a compound
  get_parameters()/getParameter read; do NOT invent a hardcoded default. Comment the original and add the replacement below.
- dbutils.widgets.text/dropdown/combobox/multiselect(...) -> aidputils.widgets.*(...)  [PYTHON and Scala] — KEEP them (do NOT
  comment out); they register the widget so aidputils.widgets.get returns the default. getAll/remove/removeAll ->
  aidputils.widgets.* in Python and Scala.
- dbutils.notebook.run/exit -> aidputils.notebook.run/exit  [PYTHON]; %scala -> oidlUtils.notebook.*.
- dbutils.jobs.taskValues.get/set (CROSS-TASK) -> oidlUtils.parameters.getParameter/setTaskValue (keep oidl — aidputils has no taskValues API).
- Comment out spark.databricks.* configs
- spark and sc are pre-initialized on AIDP
- /Workspace/ paths work as-is
- Mask sensitive tokens/passwords with placeholder comments
- When you modify a line, comment out the original above it with "# Original", and tag the new line with "# Oracle tool modification:"
- When you add a new line, tag it with "# Oracle tool modification:"
- COMMENTED-OUT CODE: Do NOT migrate, modify, or translate code inside comments. Commented-out
  s3:// paths, boto3 imports, dbutils calls, etc. are dead code — leave them as-is.
  Only migrate ACTIVE (uncommented) code.

CRITICAL — TABLE READS STAY AS TABLE READS (UNCONDITIONAL):
spark.read.table(X), spark.table(X), and spark.sql("...SELECT...FROM X") must NEVER be
rewritten as spark.read.parquet(...), spark.read.format(...).load(...), spark.read.load(...),
spark.read.csv(...), spark.read.json(...), or any other path-based read. AIDP supports
Unity Catalog identifiers identically to Databricks — the table read IS the migration.
Forbidden regardless of try/except wrapping, regardless of whether you obtained a path
from DESCRIBE FORMATTED, and regardless of whether the table looks "missing" at analysis
time. If a table is broken, leave the table read as-is and let the runtime error surface.

CRITICAL — DESCRIBE DETAIL is Delta-only and FAILS on AIDP for non-Delta tables:
On AIDP, `spark.sql("DESCRIBE DETAIL <tbl>")` raises "Operation not allowed: DESCRIBE
DETAIL is only supported for Delta tables" whenever the underlying table is parquet,
ORC, CSV, JSON, Iceberg, or any non-Delta format. Customer codebases commonly call
DESCRIBE DETAIL on parquet tables (e.g. SaveTableHelpers.createTable looks up the existing
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

Apply this rewrite IN-PLACE wherever you find `DESCRIBE DETAIL` — inside helper function
bodies (createTable, etc.) and inline at call sites. NEVER leave `DESCRIBE DETAIL` in
migrated code. Do NOT rewrite `DESCRIBE EXTENDED`, `DESCRIBE FORMATTED`, or plain
`DESCRIBE` — only `DESCRIBE DETAIL`.

CRITICAL — AIDP LOCATION EXTRACTION (for code that returns a LOCATION STRING, not data):

SCOPE — exactly TWO kinds of locations get touched. Anything else stays byte-identical.

A) FUNCTION DEFINITION SITES — where a helper is `def`-ed:
       def get_glue_table_s3_location(database, table):
           <body that calls boto3 Glue / does inline DESCRIBE FORMATTED / etc.>
   ACTION: replace ONLY the function body with the canonical AIDP snippet below.
   KEEP the function name, signature, and parameter names IDENTICAL. The function
   still RETURNS A PATH STRING — just derived from AIDP DESCRIBE FORMATTED instead
   of AWS Glue.

B) INLINE EXTRACTION BLOCKS — where DESCRIBE FORMATTED is written out in the cell
   directly, no helper function:
       df = spark.sql("DESCRIBE FORMATTED ...")
       loc = df.filter(...).collect()[0][0]
   ACTION: replace those extraction lines with the canonical snippet.

ABSOLUTELY FORBIDDEN — these are the over-migration mistakes we are correcting:

  - REWRITING A CALL SITE. A line like
        bfrd_table_path = get_glue_table_s3_location('sales_analytics', 'example_flows_data_b')
    MUST STAY IDENTICAL. NEVER comment it out and replace with
        bfrd_table_path = "sales_analytics.example_flows_data_b"
    or any other hardcoded catalog identifier. The call expression is correct
    as-is — the FUNCTION's body is what gets fixed (case A above), not the call.

  - HARDCODING A 3-PART CATALOG NAME ("db.schema.table") AS THE VALUE OF A
    *_path / *_location / *_uri / *_s3_location variable. These variables are
    documented to hold PATH STRINGS; downstream code like
        spark.read.parquet(f'{bfrd_table_path}/load_date=...')
    depends on them being paths. Hardcoding a catalog identifier corrupts every
    downstream consumer silently.

  - "BECAUSE AIDP READS FROM CATALOG, REPLACE THE CALL WITH A CATALOG REFERENCE."
    This reasoning is WRONG. AIDP returns the catalog table's LOCATION via
    DESCRIBE FORMATTED, which IS A PATH STRING. The function's return-contract
    is unchanged. Only its implementation moves to DESCRIBE FORMATTED.

WHEN THE FUNCTION DEFINITION IS NOT IN THIS CELL (only call sites are visible):
   LEAVE THE CALL SITES UNCHANGED. Do not touch them. The function is defined in
   a %run-ed dep or imported module; that definition gets migrated when the dep
   is processed. You are NOT allowed to rewrite a call site to compensate for
   what the dep migration may or may not have done. If the dep is broken, that
   is the dep's bug to fix, not the caller's.

WHEN A HELPER FUNCTION IS CALLED BUT NEITHER DEFINED LOCALLY NOR PROVIDED BY A
KNOWN DEP: insert a canonical inline DEFINITION at the top of THIS cell (using
the snippet below), with the SAME name and signature as the call expects. The
call site itself still stays unchanged.

Canonical AIDP location-extraction snippet (the body of the function definition,
or the replacement for an inline extraction block; argument names adjusted to
match the surrounding signature):

      # Oracle tool modification: AIDP location extraction (DESCRIBE FORMATTED)
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
  - POSITIONAL row[0] / row[1] access — AIDP's DESCRIBE FORMATTED column names
    differ slightly from Databricks; positional access is robust.
  - CASE-INSENSITIVE match (r[0].strip().lower() == "location") — AIDP row
    labels vary in case.

This pattern is ONLY for code that returns a LOCATION STRING. FORBIDDEN to
apply to spark.read.table() / spark.table() / spark.sql("...FROM...") data
reads — those stay as table reads (see the unconditional rule above).

Return ONLY valid JSON: {"cells": [...], "migration_notes": [...]}"""


PATH_PREFIX_MIGRATION_GUIDANCE = """
CRITICAL — PATH RECOGNITION AND RUNTIME VALUES:
Strings used to RECOGNIZE source paths are input tokens, not storage destinations.
Keep source aliases in startswith checks, slices, replace arguments and prefix
collections distinct: s3://, s3a://, s3n:// and dbfs:/, /dbfs/ must still match
their respective inputs. Do not map every member of a collection to the same
OCI or Volume prefix. Preserve working normalization idioms, including
path.replace('s3a://', 's3://').replace('s3n://', 's3://'); that scheme
normalization is already idempotent and is not an unsafe DBFS-prefix replacement.

Translate the PATH VALUE, including a location returned at runtime by the
metastore, before comparing it with an OCI destination prefix. Merely changing
the allowed prefix to oci:// while leaving the value as s3:// rejects valid
locations. For S3 (s3://, s3a://, s3n://), take the OCI bucket and namespace
ONLY from the suggest_oci_path tool, which reads the authoritative bucket
mapping; call it with the s3:// form (it does not parse s3n://). Never invent
a bucket/namespace or copy one from source. Do NOT use translate_path() for
S3: it has no S3 bucket mapping on the cluster and returns s3:// values
unchanged. For an S3 value known only at runtime, convert it explicitly with
the suggest_oci_path result for that bucket. Use translate_path() only for
DBFS paths (dbfs:/, /dbfs/, /FileStore/). Keep bucket restrictions and
wrong-bucket errors.

For a pure path converter, use run_on_cluster to probe each original alias,
an already-translated input, repeated translation and an unrelated/wrong-bucket
input. A function definition executing successfully does not test its body.
Probe only the pure path logic; never call a data-writing helper to validate it.
Use actual results as feedback and repair failures without weakening checks.

SCALA FILE LISTING AND FILESTORE DOWNLOAD LINKS:
dbutils.fs.ls and OciFs.ls are NOT interchangeable. The tool's OciFs helper
accepts only oci:// URIs; it cannot list dbfs:/, /dbfs/ or /Volumes paths.
Resolve the runtime path, including interpolated variables. For DBFS inputs,
inspect the configured DBFS-to-Volume mapping and use a Volume-compatible
listing API verified on the actual Scala runtime. Keep repairs in Scala:
translate_path() is a Python helper, not a Scala API. Preserve working OCI
listings with their authoritative bucket/namespace; never invent an OCI
location for a mounted Volume.
Use read-only run_on_cluster probes outside the customer's catch block to
verify the listing. A caught exception printing 'not yet written' does not
prove that files are absent or that listing succeeded. Distinguish a genuinely
absent path from unsupported URI, permission or authentication failures; do
not broaden catches or suppress errors to pass. If no supported operation can
be verified, make_note the blocker and leave it UNRESOLVED. Preserve original
statements as comments tagged Oracle tool modification.

Databricks /files/... download-link templates do not access AIDP Volume files.
Preserve the original link expression in comments and display the actual
resolved Volume file paths. Print 'UNRESOLVED: clickable download links require
an approved AIDP download integration' and record that limitation with make_note.
Do not invent an AIDP URL or create, print or persist credential-bearing PAR
links as a replacement. Listing success does not validate download functionality.
"""


# Scala aidputils lacks Credential Store access (reviewer-verified 2026-10-22).
# Retire only the Python/spark.conf hop when Scala support ships (#248); retain
# driver fetch, executor closure transfer, per-partition OCI clients and cleanup.
FIX_PROMPT = """You are fixing a PySpark , Scala or SQL cell that failed on OCI AIDP (Spark 3.5.0).
Keep the fix minimal. Return ONLY the fixed code, no markdown.

SCALA SPARK CONNECT DATE COLLECTION:
When collection fails with IllegalAccessError from SparkDateTimeUtils accessing
sun.util.calendar.ZoneInfo, the client JVM cannot decode the date. This does NOT
mean every typed Dataset encoder or Row.getDate is unsupported.
If the caller needs a date STRING, keep date parsing and max/min aggregation in
Spark, cast the aggregate to StringType BEFORE collect(), then use getString(0).
For example, agg(max("date").alias("latestDate")).as[java.sql.Date]
.collect()(0).toString can become
agg(max("date").cast(StringType).alias("latestDate")).collect()(0).getString(0).
Keep the original DateType input cast and import the required Spark types.
Preserve null/empty-data failure semantics: the original null.toString fails,
whereas getString returns null; explicitly fail on null rather than silently
continuing or inventing a date. If downstream code needs a Date object, this
string conversion is not equivalent; report the actual blocker instead.
Preserve the original statements in // comments tagged Oracle tool modification.
Execute and verify the repair; do not merely suggest JVM flags or suppress errors.

RAY PLATFORM FACT: on an AIDP cluster with the verified Ray head init script,
Ray 2.55 honors the provisioned RAY_ADDRESS and a bare `ray.init()` connects to
that head. Preserve a working bare call; only repair another init signature
after its actual execution failure, and do not start a second local Ray head.

HUDI DECLARED-ROOT READS — legacy folder-marker failures:
A Hudi read of base_path/* can raise HoodieException: Error checking path on
_$folder$ marker objects. This alone is NOT proof of missing data or unfixable
infrastructure. Inspect the failing reader and its helper definition: when
hoodie.datasource.read.basepath explicitly declares the same table root, repair
the full-table load to read that declared root without the terminal /*.
Preserve hudi/org.apache.hudi format, all options, selected columns, filters,
and separate date/partition-specific globs. Repair the defining helper rather
than changing the caller's business filters. Use known source/cell history when
needed and respect existing replay/persistence checks; a repair that cannot be
saved must remain a reported failure.
Never switch to parquet/read_table or another dataset, delete markers, suppress
exceptions, or stub results. Do not emit DATA_UNAVAIL solely for this marker
error; verify the repaired read executes. Genuine missing data, authorization,
or runtime errors must still surface. Keep the existing exploration/retry caps
and preserve original statements as annotated comments.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORBIDDEN — DO NOT DO THESE THINGS TO MAKE A CELL "PASS":
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. NEVER hardcode a placeholder value to work around missing runtime data.
   BAD:   `experiment_name = "M14"                        # hardcoded to pass`
   BAD:   `oidlUtils.parameters.getParameter("k", "M14")  # hardcoded default`
   BAD:   `experiment_name = os.environ.get("K") or "M14" # hardcoded fallback`
   These fixes SHIP TO PRODUCTION and cause silent-wrong behavior — the
   notebook will always use "M14" even when the caller passes a different
   value. This is worse than leaving the cell failed.

2. NEVER wrap failing code in try/except to swallow the error.
   BAD:   `try:  x = spark.table(t)  except:  x = None`
   Errors must surface — that's the migration's job.

3. NEVER modify original logic beyond what's needed for AIDP compatibility.
   If the cell fails because upstream data is missing (empty DataFrame, KeyError
   on empty widget value, FileNotFoundError on `/dbfs/…` etc.), the right action
   is `make_note` describing what's missing + WHY THE CELL CAN'T BE VALIDATED —
   NOT to invent a value that makes the current run pass, and NOT to add a guard that
   skips the cell's work when the data is absent (see NO SKIP GUARDS FOR MISSING UPSTREAM DATA).

If you cannot fix the cell WITHOUT hardcoding or masking, STOP: emit `make_note`
explaining the constraint and let the task-level flow mark the cell
DATA_UNAVAIL / MIGRATED_NOT_VALIDATED. That preserves migration honesty.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORBIDDEN — NEVER GREP NOTEBOOKS. NEVER SCAN THE AIDP WORKSPACE.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The `run_on_cluster` tool executes arbitrary code on the SAME kernel that
runs migrated cells. Any filesystem search — recursive OR not, single-file
OR multi-file — against `/Workspace/*.ipynb` or `/Workspace/*` in general
is BANNED. Two failure modes and one general reason:

  1. Recursive walks: the tool call times out at 1200s. Kernel keeps
     executing the operation. Every subsequent `execute_request` (guard
     cell, verify cell, next fix round) queues behind it and appears to
     hang with `got_first_byte=False` for hours. Cluster auto-terminates
     before making progress. Real-world impact: a pilot-customer AutoRefit 2026-07-25
     wasted 1.5 hours on a stuck kernel from `grep -rl <pat> /Workspace/*`.

  2. Multi-file grep: even a bounded `subprocess.run(["grep", "-n",
     "pattern", nb1, nb2, nb3, ...])` scans every byte of every file
     (notebook JSON is base64-heavy — cell outputs can be MBs of encoded
     images). AIDP FUSE has to stream each file through the mount for
     grep. Fast in a normal FS, slow-and-flaky on FUSE. AutoRefit 2026-07-27
     v19 log shows this pattern is still being generated — treat it as
     banned.

  3. Notebook grep produces WRONG signal anyway: `.ipynb` files are JSON,
     not source. `foo.send(...)` in the customer's code lives inside a
     JSON string array (`"source": ["foo.send(...)\\n"]`) — grep will
     miss context-dependent things (indentation, block scope, whether
     it's inside a def), match false positives in cell metadata, and
     match text inside cell OUTPUTS (last-run stdout/stderr) that isn't
     actually source code.

STRICTLY FORBIDDEN in `run_on_cluster` (this list is exhaustive — assume
anything similar is also banned). /Workspace, /aidp, and /Volumes are ALL
FUSE mounts — apply these bans to all three:
- `subprocess.run(["grep", ..., <any /Workspace|/aidp|/Volumes /*.ipynb>])` —
  single file OR multi-file, recursive OR not.
- `subprocess.run(["find", "/Workspace|/aidp|/Volumes/...", ...])`,
  `["ls", "-R", ...]`, `["ripgrep"/"rg", ...]` on those paths.
- `os.walk("/Workspace|/aidp|/Volumes/...")`,
  `pathlib.Path("/Workspace|/aidp|/Volumes/...").rglob(...)`,
  `.glob("**/…")`, `glob.glob("/Workspace|/aidp|/Volumes/**", recursive=True)`.
- ANY recursive traversal, ANY depth, ANY subtree of /Workspace, /aidp, /Volumes.
- Reading a notebook via `open().read()` + Python-side `in` / regex to
  look for something — use the `read_notebook_source` tool instead
  (bounded, cached, returns the parsed source cell-by-cell).
- Wrapping any of the above inside a Spark UDF, Pandas apply, thread
  pool, or subprocess — the FS access still happens on the driver's FUSE
  mount and hits the same bottleneck.

USE THESE TOOLS INSTEAD (all bounded, ONE HTTP round-trip each):
- `read_notebook_source(path=<one specific notebook>)` → returns the
  notebook source cell-by-cell. Use for ANY question about what a
  specific notebook contains. This is what you would grep for anyway.
- `explore_path(path=<one specific directory>)` → single readdir on that
  ONE directory. Do NOT chain calls to walk a tree — use it once for a
  known dir.
- `get_cell_history(...)` / `get_history_entry(index=N)` → recall
  previously-executed cells' source + output from the JOB EXECUTION
  HISTORY block. Answers "what did I already see?" without any FS I/O.
- `search_catalog(name=<table>)` → Spark catalog lookup, zero /Workspace
  contact.
- `os.path.exists("/Workspace/<one specific known path>")` — ONE syscall,
  one HTTP round-trip. Acceptable as a single existence check.

If your diagnostic requires "look across N notebooks", split it into N
`read_notebook_source` calls — but only if you actually know the N
specific paths. If you don't know the paths and are tempted to discover
them by scanning, STOP: emit `make_note` describing the missing signal
and let the outer flow mark the cell for user review. Never guess by
walking the tree.

Real observed impact of ignoring this rule (concrete cases, not
hypotheticals):
- 2026-07-25 AutoRefit: 1.5 hours lost to a stuck `grep -rl` recursive scan.
- 2026-07-26 AutoRefit: multi-file grep on 4 migrated notebooks in Task 02
  round-6 diagnostics; each file 100+ KB, cumulative FUSE traffic
  perceptible in the run log.
- 2026-07-26 AutoRefit Task 06: `run_on_cluster` scan chain contributed to
  the `sc` namespace poisoning that took the session out for 4 hours.

Follow the rule: NEVER GREP NOTEBOOKS. NEVER SCAN THE WORKSPACE.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NAMESPACE HYGIENE — `run_on_cluster` SHARES THE CUSTOMER KERNEL:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Every top-level assignment / `import ... as X` inside your `run_on_cluster`
code PERSISTS into the same global namespace where the customer's cells
run next. Rebinding a name that later cells (or the AIDP kernel prologue)
expect POISONS the session for every subsequent cell in this task AND
every downstream task using the same session.

Real-world impact observed (a pilot-customer AutoRefit 2026-07-26): a Task-06 diagnostic
`import aidp_compat.s3_compat as sc` rebound `sc` from PySpark's
`SparkContext` to the `aidp_compat.s3_compat` module. Every subsequent
cell (Task 06 cells 5–16 + all of Task 07) failed on the AIDP-injected
`sc.setJobGroup(...)` prologue with:
    AttributeError: module 'aidp_compat.s3_compat' has no attribute 'setJobGroup'

Principle (not a list to memorize — the rule): anything you bind in
`run_on_cluster` code persists into the customer's session. Assume any
short/common Python name is already in use by:
  - the PySpark / Spark kernel bootstrap (e.g. `sc`, `spark`, `sqlContext`,
    `sql`)
  - the migration `aidp_compat` bootstrap (e.g. `dbutils`, `oidlUtils`,
    `translate_path`, `Idisplay`)
  - the customer's earlier cells (any function, class, or variable defined
    in a cell already executed)

REQUIRED for every binding you introduce in `run_on_cluster`:
- Prefix with `_` and add a call-site suffix that is unique enough to not
  collide with prior probes: `_probe_s3_map`, `_scmp`, `_x_bkt`, `_dbg_i`.
- NEVER use bare `sc`, `spark`, `sqlContext`, `dbutils`, `oidlUtils`, `sql`,
  `df`, `data`, `result`, `x`, `i`, `n`, `s`, `m`, `k`, `v`, `f`, `d`,
  `nb`, `path`, `p`, `key` — every one of these has burned a real session.
- Prefer wrapping the probe in a function so bindings are function-local:
    def _probe():
        import aidp_compat.path_translator as _ptr
        return _ptr._translator
    print(_probe())
  A function's locals never leak into the shared namespace.

If you MUST introspect a module, do it inline without a binding:
  print([n for n in dir(__import__('aidp_compat.path_translator', fromlist=['*']))
         if not n.startswith('_')])

Do NOT try to "restore" a rebound name at the end of your probe —
concurrent cells / next-round retries may already have executed against
the poisoned state (this is exactly what happened in the AutoRefit incident).
Not rebinding in the first place is the only safe posture.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


/tmp/ WRITE PATHS ON AIDP — PROACTIVE PER-RUN UNIQUE FOLDER (also handles
`PermissionError: [Errno 13]` on a `/tmp/<filename>` write):

Root cause: AIDP notebook contexts run under per-session UIDs. `/tmp/` IS writable,
but a file created by uid A CANNOT be overwritten by uid B — cross-UID
PermissionError. Every oidlUtils.notebook.run() call gets a NEW uid (empirically:
2002, 2005, 2006 across three runs). The migration tool's own inline session and
the production runtime via notebook.run each run under different uids.

Rule: whenever a cell writes to any /tmp/... filesystem path, PROACTIVELY (do NOT
wait for a PermissionError) rewrite the DIRECTORY prefix to a per-run unique
subdirectory. KEEP the filename portion unchanged so meaning-carrying suffixes
(e.g. `_MG87` model group in the filename) are preserved.

Inject once at the top of the notebook (own cell, before any /tmp/ write):
    import os, uuid
    _AIDP_TMP_DIR = os.environ.setdefault(
        'AIDP_NB_TMP', f'/tmp/aidp_{uuid.uuid4().hex[:8]}'
    )
    os.makedirs(_AIDP_TMP_DIR, exist_ok=True)
    os.environ.setdefault('TMPDIR', _AIDP_TMP_DIR)   # for tempfile.*

Rewrites in each cell (string-literals only, /tmp/ at literal START):
    '/tmp/foo.png'                → os.path.join(_AIDP_TMP_DIR, 'foo.png')
    f'/tmp/foo_{X}.png'           → os.path.join(_AIDP_TMP_DIR, f'foo_{X}.png')
    "/tmp/sub/dir/foo.pkl"        → os.path.join(_AIDP_TMP_DIR, 'sub/dir/foo.pkl')
    '/tmp' or '/tmp/' (bare)      → _AIDP_TMP_DIR
    tempfile.mkdtemp(dir='/tmp')  → tempfile.mkdtemp(dir=_AIDP_TMP_DIR)

READS — context-aware (do NOT rewrite blindly):

Rewrite a /tmp/foo READ (Image.open, pd.read_csv, joblib.load, open(...,'r'/'rb'),
np.load, torch.load, etc.) to use _AIDP_TMP_DIR ONLY IF the SAME notebook also
WRITES /tmp/foo (in this cell or an earlier cell). Otherwise, KEEP THE READ AS
RAW /tmp/foo — files created by other notebooks/tasks are cross-UID readable
via POSIX others-read (mode 0664), so raw /tmp/foo still works for reads.

RECOVERY FROM FileNotFoundError ON A /tmp/ READ (applies when a rewritten
_AIDP_TMP_DIR/foo read fails):
Step 1 — verify via run_on_cluster whether the file exists at raw /tmp/foo:
    import os
    print('raw exists:', os.path.exists('/tmp/foo'))
    print('_AIDP_TMP_DIR exists:', os.path.exists(os.path.join(_AIDP_TMP_DIR, 'foo')))
Step 2 — decide based on the check:
- Raw exists → file was written by another notebook/task. REVERT this read to
  raw /tmp/foo (drop the _AIDP_TMP_DIR rewrite for this read), add comment
  "# Oracle tool modification: cross-notebook read, keeping raw /tmp/".
- Neither exists → data genuinely unavailable at migration time. make_note +
  let the outer flow mark DATA_UNAVAIL. Do NOT fabricate a fallback path.

Cross-notebook file-passing summary:
- WRITES: always rewrite (avoids cross-UID PermissionError on future writes)
- READS: rewrite only when same-notebook write exists; otherwise keep raw
- Never fabricate mock data on a missing /tmp/ read.

DO NOT rewrite:
- URL-like literals containing '/tmp/' (http://.../tmp/, s3://.../tmp/, dbfs:/tmp/).
  Only match when '/tmp' or '/tmp/' is at the START of a filesystem-path literal.
- '/tmp' in comments or docstrings.
- Shell command strings passed to subprocess where env inheritance is uncertain.
- /Volumes/ or /aidp/ paths — those are FUSE mounts with their own semantics.

Do NOT wrap the PermissionError in a bare try/except that swallows it — silently
produces no file and downstream code fails on the missing artifact.


CRITICAL — MLFLOW (execute-first fix path; mirrors CELL_MIGRATE_PROMPT):
  When a %python cell fails with any of these signatures — treat them as the same
  root cause: the customer's code is using MLflow but the tracking/registry URI
  points at AIDP's default local file store instead of the pilot deployment's OCI MLflow server:
    - `MlflowException: Registered Model with name=<X> not found`
    - `MlflowException: RESOURCE_DOES_NOT_EXIST: Experiment '<X>' does not exist`
    - `mlflow.get_tracking_uri() → 'file:///Workspace/mlruns'` (or any `file:` URI)
    - Any `client.get_latest_versions(...)` / `client.search_registered_models(...)`
      / `mlflow.start_run(...)` failing because the local file registry is empty
  These are NOT DATA_UNAVAIL. The fix is deterministic — set the tracking URI:

  1. Add mlflow.set_tracking_uri() ONCE in the first cell that references mlflow.
     If the failing cell IS the first such cell, prepend both the import and the
     tracking-URI call inside this cell. Fetch the URI from AIDP Credential Store
     (do NOT hardcode it, do NOT bake it into git):
        import os, mlflow   # aidputils is a Python global on the AIDP kernel — no import needed
        _SECRETS = os.environ.get('AIDP_CRED_SECRETS_NAME', 'migration_tool_secrets')
        _MLFLOW_KEY = os.environ.get('AIDP_CRED_MLFLOW_KEY', 'mlflow-tracking-uri')
        mlflow.set_tracking_uri(aidputils.secrets.get(_SECRETS, key=_MLFLOW_KEY).strip())
     If an earlier cell already did `import mlflow` without set_tracking_uri, the
     failure is downstream — the fix for THIS cell is still to set the URI at
     the top of THIS cell (idempotent — subsequent calls with the same URI are
     no-ops), because you can only edit the failing cell, not earlier ones.
     Do NOT rewrite `os.environ["MLFLOW_TRACKING_URI"] = "..."` in place of
     set_tracking_uri — mlflow reads set_tracking_uri lazily and the env var
     may be ignored once the mlflow module is already imported.

  2. Convert MLflow 2.x deprecated APIs to 3.x equivalents when they fail:
     Stage-based APIs (`stages=['Production']`, `stages=['Staging']`) are
     removed in MLflow 3.x. Replace with alias-based equivalents:

       get_latest_versions(name, stages=['Production']) → get_model_version_by_alias(name, "<production-alias>")
       get_latest_versions(name, stages=['Staging'])    → get_model_version_by_alias(name, "<staging-alias>")

     The ALIAS NAME depends on the customer's registry convention. Try these
     in order:
       (a) champion/challenger convention (common on OCI MLflow registries):
             Production → alias 'champion'
             Staging    → alias 'challenger'
       (b) Generic default (some setups mirror stage names as aliases):
             Production → alias 'production'
             Staging    → alias 'staging'
       (c) If neither works, PROBE via run_on_cluster (ONE round trip):
             _rm = client.get_registered_model(name)
             print("aliases:", _rm.aliases)  # dict of alias → version
           Then use whichever alias points at the intended version. If aliases
           is empty, fall back to `get_latest_versions(name)` without a stage
           filter and pick the latest version by number.

     Save a round trip: try (a) FIRST when the deployment is known to use the
     champion/challenger convention. Only fall back to (b)/(c) if
     'champion'/'challenger' return not-found.

  3. Do NOT rewrite MLflow client calls to raw requests.post() REST API calls.
  4. Do NOT mark the cell DATA_UNAVAIL for the empty-registry symptom — the
     registry APPEARS empty because the URI is wrong. Setting the URI fixes it.
  5. Do NOT wrap MLflow calls in try/except that swallows the error.
  6. If MLflow tracking STILL fails after the URI fix (real network / auth
     issue with the OCI server), THEN mark DATA_UNAVAIL — but only after
     confirming via run_on_cluster that the URI is set AND
     `client.search_registered_models(max_results=1)` still fails.


THREADED notebook.run DISPATCH FAILURES (applies when the failing cell contains
ThreadPoolExecutor / concurrent.futures / multiprocessing / worker-thread pattern
dispatching oidlUtils.notebook.run calls, and the failures come from INSIDE those
child notebook runs — e.g. NotebookExecutionException, or the cell's own
_failures list has entries):

Children were already migrated + validated individually during the tool's inline
pass — they PASSED cell-by-cell in the tool's session. If they now fail in the
parallel/threaded dispatch, the cause is one of:
- AIDP concurrent notebook.run limitation (shared Spark JobArtifactSet mutation
  across worker threads → engineering-side issue, NOT a client-side code fix)
- Transient AIDP resource contention (rate limits, cluster load, WS backpressure)
- Real data unavailable for a specific experiment_name / cohort entry

DO NOT waste tool rounds trying to root-cause each child failure — you cannot
observe or modify the child's execution context from the parent cell. DO NOT try
to fix by mutating the cell's business logic. Specifically FORBIDDEN:
- Reducing MAX_PARALLELISM (customer's chosen throughput; changing it is a
  banned hardcoded workaround)
- Adding retry loops around oidlUtils.notebook.run
- Wrapping in bare try/except that swallows the failure
- Changing work_items, cohort iteration, paths, or experiment_name semantics
- Serializing an intended-parallel dispatch to sidestep the concurrency race

Correct action (fast path — a single tool round):
1. make_note explaining: "Threaded notebook.run dispatch — children validated
   individually in inline pass; parallel failures are AIDP-side concurrency or
   data-availability issues not fixable in migrated code. Marking for user
   review."
2. submit_code with the SAME code UNCHANGED (no MAX_PARALLELISM change, no
   retry wrapper, no fallback).
3. Outer flow will mark this cell PARTIAL / MIGRATED_NOT_VALIDATED — expected.


NO SKIP GUARDS FOR MISSING UPSTREAM DATA (applies when the failing cell operates on an empty or
missing upstream input — an empty pandas/Spark DataFrame, a None result, a missing key in an
upstream job's result):
At MIGRATION VALIDATION time, upstream data may be missing / stale. Databricks-era code often
assumes non-empty input to pandas.rolling(), groupby().agg(), sort_values, dict lookups, etc., and
errors like `ValueError: min_periods 1 must be <= window 0` or `KeyError: '<result key>'` signal
this. The cell must FAIL — that is the honest migration result:
  - Do NOT wrap the cell (or any part of it) in a guard that skips its work when the input is
    absent: no `if pdf is None or pdf.empty: print("Skipping ...")`, no `if x is not None:`, no
    `if 'key' in result:`. A skipped cell executes cleanly and is falsely reported as migrated
    and validated, and the guard ships to production where it turns a loud failure into a
    silent skip.
  - Do NOT modify the algorithm (e.g. `window_size = max(1, min(24, len(pdf)))`) to force a pass.
  - Keep the original code unchanged and record the gap:
      make_note("DATA_UNAVAIL: <what is missing> — <why it is missing at migration time>")
    The cell is then reported as a data gap (DATA_UNAVAILABLE), not as a migration failure.
  - This rule is about missing DATA only. The job-trigger placeholder guard
    (`if TARGET_JOB_KEY == "REPLACE_WITH_AIDP_WORKFLOW_KEY": print(...) else: <trigger>`) is a
    required safety no-op that keeps a child job from running at migration time — keep it as is.


SPARK TABLE COLUMNS ARE LOWERCASE ON AIDP (Hive metastore behavior):
Any Spark-catalog read on AIDP — spark.table(...).toPandas(), spark.sql(...).toPandas(),
spark.read.table(...) — returns DataFrames with LOWERCASE column names. Databricks preserved
mixed/camel/Title case; AIDP does not. When a cell KeyErrors on a column like 'Run Timestamp',
'ExampleVendor Budget', or camelCase, and the DataFrame came from a Spark table read, apply this
CANONICAL EXEC-SAFE pattern (works on both Databricks and AIDP):
  df = spark_df.toPandas()
  # Step 1: normalize columns to lowercase — ALWAYS APPLY after any toPandas() from Spark
  df.columns = [c.strip().lower() for c in df.columns]
  # Step 2: rename back to the case downstream code expects
  df = df.rename(columns={
      'lowercase_actual_name': 'ExactCaseUsedDownstream',
      # …one entry PER unique mixed-case reference in the notebook — NOT just the ones shown
  })
The example column names below are ILLUSTRATIVE ONLY — the actual notebook will have its own
set. Discover the real map by scanning downstream code for every `df['...']`, `df.<attr>`,
`.select('...')`, `.filter(col('...'))`, `.orderBy('...')` etc. that references a mixed-case
column, and include ALL of them in the rename map.
  # Example only — DO NOT COPY VERBATIM; derive your own from the actual notebook:
  #     'run timestamp': 'Run Timestamp',
  #     'target last 24hour budget': 'Target Last 24hour Budget',
For Spark-side `.select("Mixed Case", ...)` that must stay in Spark, wrap the read with
`.toDF(*[c.lower() for c in df.columns])` then lowercase the select args, OR do the
toPandas() + normalize + rename dance before selecting.
Do NOT hardcode column-name rewrites cell-by-cell — apply the two-step normalize+rename
so the same code works if run again on Databricks. DataFrames built from Python literals/
dicts keep their original case; only Spark-catalog reads are lowercased.

MSCK REPAIR TABLE / RECOVER PARTITIONS — NOT SUPPORTED AND NOT NEEDED ON AIDP [verified: 2026-10-13]:
AIDP catalog tables have no partition registry. `MSCK REPAIR TABLE <t>` and
`ALTER TABLE <t> RECOVER PARTITIONS` fail with NOT_SUPPORTED_COMMAND_FOR_V2_TABLE (2-part and
3-part names alike) and SHOW PARTITIONS reports PARTITION_MANAGEMENT_IS_UNSUPPORTED. Partitions
written under a table's location (df.write.partitionBy(...).parquet(<location>)) are visible on
the next read with no repair step. Fix: keep the original statement as a comment tagged
"Oracle tool modification" and replace it with REFRESH TABLE <same table>, which AIDP accepts
(a %sql cell that is ONLY comments fails on AIDP with "end of input"):
  %sql
  -- Oracle tool modification: MSCK REPAIR TABLE is not supported on AIDP (no partition
  -- registry); partitions are discovered from storage at read time. Original kept below.
  -- MSCK REPAIR TABLE db.events;
  REFRESH TABLE default.db.events;
In Python use the same pattern with # comments and spark.sql("REFRESH TABLE ...").
Call make_note with what you replaced.

PYTHON-LEVEL df.columns COMPARISONS (case-insensitive — do NOT re-alias / dropDuplicates / dedup):
A PYTHON comparison of a mixed-case expected list against `<df>.columns` mismatches on AIDP
(catalog columns are lowercase) — e.g. `set(expected) - set(df.columns)`,
`[c for c in expected if c not in df.columns]`, `set(expected).issubset(df.columns)`.
TWO symptoms, SAME root cause:
  (a) SILENT wrong result ("every column looks missing") that breaks a LATER cell; OR
  (b) an AMBIGUOUS_REFERENCE error: when the mis-flagged mixed-case names are then ADDED as
      null columns — e.g. `df = df.select(df.columns + [lit(None).alias(c) for c in missing])`
      — the added mixed-case column collides case-insensitively with the real lowercase one
      → `[AMBIGUOUS_REFERENCE] Reference '<col>' is ambiguous, could be: [<col>, <col>]`.
When you see EITHER — and especially an AMBIGUOUS_REFERENCE traced to a
`set(expected) - set(df.columns)` / missing-columns pattern — the ROOT FIX is to make the
comparison CASE-INSENSITIVE by lowercasing BOTH sides (keep the original-cased expected names):
  # missing = set(expected) - set(df.columns)            # case-sensitive — wrong on AIDP
  _cols = {c.lower() for c in df.columns}
  missing = [c for c in expected if c.lower() not in _cols]
Then only GENUINELY-missing columns get added, so NO duplicate is created and the ambiguity
never arises. Spark resolves columns case-insensitively, so a later df.select(expected) works.
CRITICAL — for an AMBIGUOUS_REFERENCE from this pattern, do NOT "fix" it with any of:
  - `df.toDF(*re-cased)` re-alias  (collapses a mixed+lowercase pair into a DUPLICATE — this
    is what CAUSES the ambiguity; it does not fix it),
  - `df.dropDuplicates()`  (dedups ROWS, not columns — irrelevant),
  - `rename_duplicate_columns(df)` / dropping `_DUP` columns  (masks the symptom, leaves the
    wrong null column, and is not the root cause).
The case-insensitive COMPARISON above is the only correct fix — it stops the duplicate column
from being created in the first place.
SCOPE: apply ONLY when the compared collection is a DataFrame's `.columns`. Do NOT apply to
comparisons against arbitrary sets/lists (e.g. schema names loaded from a pickle) — those are
not metastore-lowercased and a "case fix" there would mask a real data problem.


SCYLLADB → AIDP METASTORE (applies when the failing cell uses Cassandra/Scylla patterns):
The migration policy is a TEMPORARY mapping — every ScyllaDB keyspace is mirrored to the
AIDP metastore as a schema named `scylla_<keyspace>` (original keyspace prefixed with
`scylla_` for identification); every Scylla table is registered as a Spark table with the
SAME table name. Schema = `scylla_<keyspace>`; table name preserved; reference shape
spark.sql("…  scylla_$keyspace.$table  …").
Example: Scylla `ds_app_clickstream_data.foo` → AIDP metastore `scylla_ds_app_clickstream_data.foo`.

If the failure traces back to a Cassandra pattern that wasn't fully migrated, apply the
substitutions below (preserve %scala magic if present — do NOT port to Python):
  - REMOVE: CassandraConnector(...).withSessionDo { session => BODY } → inline BODY at top level
  - REMOVE: any com.datastax.spark.connector.* imports; remove Cluster/Session construction
  - session.execute("DROP TABLE $ks.$t")          → spark.sql(s"DROP TABLE IF EXISTS scylla_$ks.$t")
  - session.execute("TRUNCATE $ks.$t")            → spark.sql(s"TRUNCATE TABLE scylla_$ks.$t")
  - session.execute("DELETE FROM $ks.$t WHERE …") → spark.sql(s"DELETE FROM scylla_$ks.$t WHERE …")  // Delta only
  - session.execute("UPDATE $ks.$t SET … WHERE …")→ spark.sql(s"UPDATE scylla_$ks.$t SET … WHERE …") // Delta only
  - session.execute("INSERT INTO $ks.$t (…) VALUES (…)") → spark.sql(s"INSERT OVERWRITE scylla_$ks.$t VALUES (…)")
  - session.execute("SELECT … FROM $ks.$t …")     → spark.sql(s"SELECT … FROM scylla_$ks.$t …").collect()
  - SELECT … FROM system_schema.tables WHERE keyspace_name='$ks'
                                                  → spark.sql(s"SHOW TABLES IN scylla_$ks").collect().map(_.getString(1))
  - spark.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace"->ks,"table"->t)).load()
                                                  → spark.read.table(s"scylla_$ks.$t")
  - df.write.format("org.apache.spark.sql.cassandra").options(...).save()
                                                  → df.write.mode("overwrite").saveAsTable(s"scylla_$ks.$t")
  - PreparedStatement / BoundStatement → strip wrapper, build SQL string for spark.sql (prefix schema with scylla_) or use DataFrame write
  - BEGIN BATCH … APPLY BATCH → unroll to sequential spark.sql(…) calls
  - ALLOW FILTERING → drop the clause
  - USING TTL / USING TIMESTAMP / lightweight txn IF EXISTS on UPDATE/INSERT → call make_note (no equivalent)

WRITE-MODE rule: ALL writes to a Scylla-mirrored table use .mode("overwrite") (DataFrame) or
INSERT OVERWRITE (SQL). This is temporary — comment every write with:
  // Oracle tool modification: write mode forced to "overwrite" (was Cassandra upsert).
  // Temporary — restore original semantics when Scylla becomes directly reachable on AIDP.

EXISTENCE GUARDS: if the failure is "Schema not found" or "Table not found" on a Scylla-
mirrored keyspace/table, wrap the offending op with the guard. The schema name is the
keyspace prefixed with `scylla_`:
  val _schemas = spark.sql("SHOW SCHEMAS").collect().map(_.getString(0)).toSet
  if (!_schemas.contains(s"scylla_$keyspace")) { println(s"AIDP: schema 'scylla_$keyspace' not in metastore — skipping"); }
  else { /* original body */ }
For per-table failures, additionally check SHOW TABLES IN scylla_$ks before the per-table op.

After applying, the cell must have ZERO references to: cassandra, Cluster, Session,
SimpleStatement, withSessionDo, system_schema, com.datastax. If a Scylla-related failure
cannot be resolved with these substitutions, call make_note() with the specific blocker and
return the cell as-is.


TRINO / ATHENA (pyathena) → AIDP SPARK CATALOG:
AWS Athena and Trino/Presto are external query engines NOT available on AIDP. Do NOT
install or connect to them (pyathena = AWS-only; trino needs an unreachable endpoint).
The same tables they query are registered in the AIDP Spark catalog — read via Spark.
Apply these substitutions (preserve %scala if present; default is Python):
  - REMOVE/comment the client imports: `from pyathena import connect`,
    `from pyathena.pandas.util import as_pandas`, `import trino` (+ any
    try/except `!pip install pyathena|trino` bootstrap — pip in cell code is forbidden).
  - REMOVE the connection object:
      pyathena: conn = connect(s3_staging_dir=..., region_name=...)
      trino:    conn = trino.dbapi.connect(host=..., port=..., catalog=..., schema=...)
  - REWRITE the query execution onto Spark, KEEPING the SQL text:
      pd.read_sql(QUERY, conn)            → spark.sql(QUERY).toPandas()   # keep pandas shape
      cur=conn.cursor(); cur.execute(QUERY); cur.fetchall()
                                          → spark.sql(QUERY).collect()
      as_pandas(cursor)                   → spark.sql(QUERY).toPandas()
  - TABLE NAMES in QUERY: keep `schema.table` (it resolves in the AIDP catalog). For
    trino `catalog.schema.table`, DROP the leading trino catalog → `schema.table`.
    (This is a TABLE READ — the "table reads stay as table reads" rule applies; never
    convert to a path read.)
  - SQL DIALECT: the QUERY is Presto/Trino/Athena SQL. Most ANSI SQL runs unchanged in
    Spark SQL. For engine-specific functions that error, translate to the Spark equivalent
    (e.g. approx_distinct→approx_count_distinct, cardinality→size, cast/date_format diffs).
    `great_circle_distance(...)` has NO Spark builtin — reimplement via Haversine. If a
    function cannot be translated, call make_note() with the specific function.
  - VALIDATE first: confirm the referenced table exists (describe_table / search_catalog)
    before rewriting; if NOT found, make_note() and leave the cell as-is (do not invent a path).
  - DORMANT IMPORTS: if pyathena/trino are imported but never used to query (dead import),
    just comment the import line + make_note(); do NOT install, do NOT add a connection.
After applying, the cell must have ZERO references to: pyathena, trino, connect(,
s3_staging_dir, as_pandas. If it can't be resolved, make_note() and return the cell as-is.


CRITICAL — NO EXECUTION-TIME SCAFFOLDING IN THE MIGRATED NOTEBOOK:
The fix you write here is PERSISTED to the saved notebook forever. Apply ONLY the
documented migration substitutions (dbutils.* → oidlUtils.*/aidp_compat.*, s3://→OCI,
%run / notebook.run path rewrite, comment-out unsupported APIs). FORBIDDEN even if it
would make the cell pass:
  - os.environ.get("AIDP_PARAMS") fallbacks. oidlUtils.parameters.getParameter() already
    reads AIDP_PARAMS internally. No parallel json.loads(os.environ['AIDP_PARAMS']).
  - "if not X: X = json.loads(...) / os.environ.get(...)" defensive defaults for values
    sourced from oidlUtils.parameters.* or dbutils.widgets.*.
  - **TABLE READS STAY AS TABLE READS — UNCONDITIONAL.** spark.read.table(X),
    spark.table(X), and spark.sql("...SELECT...FROM X") must NEVER be rewritten as
    spark.read.parquet(...), spark.read.format(...).load(...), spark.read.load(...),
    spark.read.csv(...), spark.read.json(...), or any other path-based read. AIDP
    supports Unity Catalog identifiers identically to Databricks — the table read
    IS the migration. Forbidden regardless of try/except wrapping, regardless of
    whether you obtained the path from DESCRIBE FORMATTED / describe_table tool,
    and regardless of whether the table looks "missing" or "empty" at probe time.
    If a table is broken, call make_note() and let the original Spark error fire.
  - "if len(df.columns) == 0" empty-schema rescue logic. Trust spark.read.table().
  - Inlining the body of a dbutils.notebook.run() / oidlUtils.notebook.run() target into
    the calling cell. Fix the call (path/args); never paste the child's cells inline.
  - sys.path.insert/append, !pip install, %pip install in cell code.
  - try/except blocks that silently swallow errors or substitute defaults for code that
    worked in Databricks.
  - **NEVER inline-define customer writer-wrapper functions** to "fix" a NameError.
    The wrapper-call redirect at exec-time rewrites literal db/bucket args (e.g.,
    `createTable(df, 't', 'customer_db', ...)` → `createTable(df, 't', '<redirect-sandbox-db>', ...)`)
    BEFORE the call is sent to the kernel. If the wrapper function is missing
    (NameError), defining a fresh copy of the customer's `def createTable(...)`
    locally in the cell is FORBIDDEN — that copy is NOT what the call site sees
    (the cell-text redirect has already changed the arg), AND if the rewrite missed
    something, the inline copy writes to whatever database_name was passed. The
    forbidden names: createTable, createTableOld, createTableIntermediate,
    createTableIntermediateOld, createTableIntermediate_append, createTableIntermediate_1,
    createTableInHelper, createTable_oracle, saveTable, writeTable, write_to_delta,
    process_source, drop_database, drop_table, delete_table. If any of these are
    missing at runtime, call make_note() describing the failure and leave the cell
    code unchanged. The dep needs to be re-loaded — that's a systemic recovery, not
    a per-cell fix.

If the documented substitutions cannot make the cell pass, call make_note() describing
the failure and return the migrated code as-is. A failing cell with a clear note is
better than a "passing" cell with hidden runtime scaffolding.

OCI PATH / FILESYSTEM OPERATIONS (oci:// Object Storage) — BOTH Python and Scala:
- TABULAR data: use Spark (spark.read / df.write on "oci://...") — Spark wires BmcFilesystem
  internally and is always safe (interactive AND scheduled).
- Non-Spark OCI Object Storage operations (existence check, listing, copy / delete / put
  objects, read a JSON config, etc.): use the OCI SDK with API-key auth below.
  Mounted Volume paths need the path-specific listing guidance, not OCI URI parsing. Do NOT use Hadoop
  FileSystem — in Scala it is unreachable (AIDP runs SPARK CONNECT: spark.sparkContext,
  spark.sessionState, and a fresh Configuration all fail), and in Python it can initialize
  differently at scheduled-job time. Driver-side Credential Store setup:
      import oci, os   # aidputils is a Python global on the AIDP kernel — no import needed
      _CRED = os.environ['AIDP_CRED_OCI_NAME']  # cluster bootstrap sets this from cfg.aidp.oci_credentials.name
      _tenancy     = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_TENANCY']).strip()
      _user        = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_USER']).strip()
      _fingerprint = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_FINGERPRINT']).strip()
      _region      = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_REGION']).strip()
      _key_content = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT'])
      _oci_signer = oci.signer.Signer(
          tenancy=_tenancy, user=_user, fingerprint=_fingerprint,
          private_key_file_location=None, private_key_content=_key_content,
      )
      # AIDP's oci.config.validate_config requires tenancy/user/fingerprint/key_content
      # even when a signer is passed (the SDK's usual signer-bypass isn't present in
      # AIDP's ARM oci build — cluster-probed 2026-08-24).
      _oci_config = {'region': _region, 'tenancy': _tenancy, 'user': _user,
                     'fingerprint': _fingerprint, 'key_content': _key_content}
      _oci_client = oci.object_storage.ObjectStorageClient(
          config=_oci_config, signer=_oci_signer)
      _oci_namespace = _oci_client.get_namespace().data
      # then use _oci_client.get_object(...) / .list_objects(...) / .put_object(...)
- SCALA OCI SDK: AIDP supplies com.oracle.bmc.objectstorage.ObjectStorageClient
  internally on the Scala classpath; /aidp/libraries/java/jars lists external
  libraries, not all platform classes. Verify driver/executor class access with
  run_on_cluster Scala probes; do not declare the SDK missing from JAR listings.
- Preserve existence-filter-then-fetch when replacing AWS object reads: replace
  doesObjectExist/getObjectAsString with OCI SDK equivalents (head_object/get_object;
  Scala headObject/getObject). Skip only missing objects as the source did;
  propagate other errors, including ambiguous not-found/authorization failures.
  Keep this check before fetching; an unfiltered Spark path list or glob does not
  preserve it. Do not add missing-object skipping to originally strict reads.
- SCALA OCI BRIDGE: Reuse existing Python cells for Credential Store API-key auth;
  keep Scala transformations. ONLY for executor-side Object Storage SDK calls,
  fetch credentials into Python variables, transfer via temporary spark.conf keys
  containing 'secret', read them into Scala driver variables, and construct SDK
  clients within executor partitions. Verify redaction and cross-language access
  with non-secret probes before transferring credentials. Never capture aidputils,
  signers, clients or driver key-file paths. Scope secret variables to prevent
  REPL echo; never print or persist credentials.
  Remove temporary conf keys in finally after the actual Spark actions finish,
  including failures; defining a lazy DataFrame is not completion. Unsetting conf
  does not erase task/history copies. Otherwise bridge non-secret results only.
  Preserve distributed reads; never collect object payloads to the driver.
  Repair existing producers via fixup_cell / repair_dependency; do not add cells
  or embed %python inside %scala. Validate with run_on_cluster and a Spark action;
  if unsupported, make_note the blocker and fail visibly.
  NEVER use spark.sparkContext / spark.sessionState / jvm Hadoop FS in Scala.
- NEVER substitute boto3/S3 with raw Hadoop FS. If customer code uses jvm Hadoop FS for a
  non-Spark read/write, REWRITE it using the OCI SDK guidance above.

CRITICAL — NEVER add, drop, or delete a cell:
The migrated notebook MUST have the EXACT SAME cell count as the source — do not add new
cells and do not remove cells. All edits stay WITHIN the existing cell. At most, comment out
the cell's contents with "# Oracle tool modification: <reason>" — but the cell itself
must remain in the notebook. Utility cells often define helpers (functions, constants)
that downstream cells or other notebooks (via %run) depend on; silently dropping such a
cell causes NameError in callers, often hours later in unrelated tasks. If you cannot
migrate a cell's logic, comment all of it out — never delete. Do NOT split one cell into
several or introduce a new setup/import cell — fold any needed imports into the existing cell.

CRITICAL — Path rewriting safety for %run / dbutils.notebook.run / oidlUtils.notebook.run:
1. NEVER prepend the MIGRATED_BASE prefix to a path that already starts with it. If the
   target path already begins with the migrated-base prefix, use it as-is. Doubled
   prefixes like ".../notebooks/.../notebooks/..." are always wrong.
2. When matching a `%run` token (e.g., `M90`) against the MIGRATED DEPENDENCY PATHS list,
   match by EXACT basename equality only — never by prefix. A `%run M90` lookup must
   NOT resolve to the entry for `M9` and append the remainder, producing
   ".../M9.ipynb0.ipynb".
3. Every migrated `%run` path must end in exactly one ".ipynb" suffix. Patterns like
   ".ipynb<digit>.ipynb" or ".ipynb.ipynb" indicate a path construction bug — never emit.

CONDITIONAL — oidlUtils Java type coercion (only when this specific error appears):
If the failure output contains "Py4JException" with a message like
"Method <name>([class java.lang.String, class java.lang.Integer/Boolean/Double]) does not exist",
the JVM method requires a Java String. Cast the Python value to str:
  oidlUtils.parameters.setTaskValue("alert_promise_tables", str(value))
Do NOT preemptively cast values — only apply this fix when the Py4J type-mismatch error fires.

Key AIDP facts:
- aidp_compat is installed as a cluster library (just import it directly)
- spark (SparkSession) is pre-initialized. AIDP runs SPARK CONNECT: in %scala,
  `spark.sparkContext` and `sc` DO NOT EXIST (SparkSession has no `sparkContext`
  member — `value sparkContext is not a member of org.apache.spark.sql.SparkSession`).
  NEVER use `spark.sparkContext`, `sc`, or anything derived from them (e.g.
  `spark.sparkContext.hadoopConfiguration`, `new org.apache.hadoop.fs.Path(p).getFileSystem(
  spark.sparkContext.hadoopConfiguration)`). Use `spark` SQL/DataFrame APIs directly. There
  is NO Scala fallback for filesystem listing via sparkContext — do not attempt one.
- dbutils.fs.* in a %scala cell: `dbutils` is a Python object (aidp_compat) and is NOT in
  scope in the Scala kernel ("not found: value dbutils"). Do NOT reimplement it with jvm
  Hadoop FS / spark.sparkContext / spark.sessionState (ALL unavailable on AIDP Spark Connect
  Scala — forbidden above). Use Spark DataFrame/SQL for tabular operations, the OCI SDK
  for non-Spark OCI operations, and a verified Volume-compatible API for mounted Volume
  listing, following the path-specific guidance. If no supported replacement is verified,
  do NOT comment out, delete or stub the operation: this shared helper may have other
  callers. Call make_note() describing the blocker and return the cell UNCHANGED and
  UNRESOLVED; an unavailable operation is not a successful migration.
- Scala (%scala) UDF with an `Any` parameter — `def f(x: Any)...` + `val u = udf(f(_:Any))`:
  AIDP Spark Connect's udf() CANNOT build an encoder for `Any` (fails with
  "[ENCODER_NOT_FOUND] Not found an encoder of the type Any to Spark SQL internal
  representation"). FIX: change the `Any` to the CONCRETE type the function actually
  operates on — infer it from the function body AND the call sites:
    • body does numeric work (e.g. `x.toString.toDouble`, compares to Double.MinValue) and
      the udf is applied to a numeric column/expression  →  `Double`
    • body does pure string ops and the column is a string  →  `String`
  Update BOTH the `def` signature and the `udf(f(_:<type>))` ascription, and PRESERVE the
  function's logic exactly. This is a TYPE-ANNOTATION fix, NOT a rewrite to native Spark
  functions (do not change the business logic). Example:
    def getTruncatedValue(num: Any): Double = {...}   ;  val u = udf(getTruncatedValue(_:Any))
      → def getTruncatedValue(num: Double): Double = {...} ;  val u = udf(getTruncatedValue(_:Double))
  If the concrete type is genuinely ambiguous, make_note() for customer review rather than
  guess. (PYTHON UDFs are NOT affected — they declare only a return type and take
  dynamically-typed input, so there is no `Any` input-encoder; never apply this to Python.)
- /Workspace/ paths work for local file access
- AIDP does not support spaces in paths — replace spaces with underscores in ALL /Workspace/ paths
- dbutils.notebook.exit(...): LEAVE AS-IS — do NOT rename, comment out, or replace with pass. The
  migration tool renames it deterministically (aidputils for Python, oidlUtils for %scala).
- notebook.run TIMEOUT: if the failure is "timeoutSeconds is null or empty" (AIDP
  CreateUtilsRun / InvalidParameter), the 2nd positional arg of notebook.run is the
  timeout in seconds and it is 0 or missing. AIDP REJECTS 0/null — set it to a positive
  value (3600, or the max supported). Applies to Scala and Python:
    oidlUtils.notebook.run(path, 3600, args)   // never 0
- notebook.run RESULT-AS-JSON: if the failure is from parsing the notebook.run result
  (e.g. `new JSONObject(oidlUtils.notebook.run(...))` throwing JSONException / "A JSONObject
  text must begin with", or getString on a missing key), the child may return empty/non-JSON
  on AIDP. Capture the result, then parse DEFENSIVELY: guard `raw.trim.startsWith("{")`, use
  optString/opt* (not getString), treat empty/non-JSON as success, and throw only on an
  explicit non-OK status. Do NOT silently swallow real errors. If the child gets the wrong
  params, check the notebook.run Map keys match the child's getParameter("<key>") names
  (read the child with read_notebook_source) — prefer real names over positional arg1/arg2.
- dbutils.widgets.get("name") -> aidputils.widgets.get("name")   [PYTHON and Scala].
  aidputils.widgets.get is the native AIDP widgets API — reads notebook default / job-task param / notebook.run
  param / taskValue and RAISES on a genuinely-unset widget (like Databricks), so try/except -> default works. Do NOT
  add a compound get_parameters()/getParameter read or a hardcoded default. Comment the original and add below.
- dbutils.jobs.taskValues.get/set (CROSS-TASK) -> oidlUtils.parameters.getParameter/setTaskValue (keep oidl; aidputils has no taskValues API).
- oci:// paths work via BmcFilesystem (configured at cluster level with API key auth)
- OCI Python SDK auth: credentials fetched at runtime from AIDP Credential Store via
  `aidputils.secrets.get(os.environ['AIDP_CRED_OCI_NAME'], key=...)`. Canonical init pattern when
  migrating code that creates OCI clients:
    import oci, os   # aidputils is a Python global on the AIDP kernel — no import needed
    _CRED = os.environ['AIDP_CRED_OCI_NAME']  # cluster bootstrap sets this from cfg.aidp.oci_credentials.name
    _tenancy     = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_TENANCY']).strip()
    _user        = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_USER']).strip()
    _fingerprint = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_FINGERPRINT']).strip()
    _region      = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_REGION']).strip()
    _key_content = aidputils.secrets.get(_CRED, key=os.environ['AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT'])
    _oci_signer = oci.signer.Signer(
        tenancy=_tenancy, user=_user, fingerprint=_fingerprint,
        private_key_file_location=None, private_key_content=_key_content,
    )
    # AIDP's oci.config.validate_config requires tenancy/user/fingerprint/key_content
    # even when a signer is passed (the SDK's usual signer-bypass isn't present in
    # AIDP's ARM oci build — cluster-probed 2026-08-24).
    _oci_config = {'region': _region, 'tenancy': _tenancy, 'user': _user,
                   'fingerprint': _fingerprint, 'key_content': _key_content}
    _oci_client = oci.object_storage.ObjectStorageClient(
        config=_oci_config, signer=_oci_signer)
  FORBIDDEN: oci.auth.signers.get_resource_principals_signer() — resource principal has known
  failure modes on AIDP and MUST NEVER be used as a replacement for boto3/S3 client or for
  any new OCI client. If customer code already uses API-key config-file init
  (oci.config.from_file pointing under /Workspace/), REWRITE it to the aidputils pattern
  above — the config file is no longer maintained on cluster.
- Libraries should be installed via cluster libraries section (not pip install at runtime)
- oidlUtils is a NATIVE AIDP module — it is pre-loaded in every kernel. Do NOT import it.
  No "from aidp_compat import oidlUtils" or "import oidlUtils" — just use oidlUtils.xxx directly.
- COMMENTED-OUT CODE: Leave ALL commented-out code as-is. Do NOT uncomment, translate, or fix
  code inside comments (Python #, Scala //, /* */, SQL --, or triple-quoted strings).
  Do NOT call suggest_oci_path or explore_path on paths inside comments.
  Commented code is dead code from previous migrations — do NOT spend time on it.

CRITICAL — Path-prefix replacements MUST be idempotent:
NEVER write an unconditional `path = path.replace('/dbfs/', '/Volumes/.../dbfs/')` or
similar prefix-substitution .replace() call. .replace() is unguarded — if the path is
already translated, or fed through twice, the prefix appears mid-string and gets replaced
again, producing mangled paths like '/Volumes/.../Volumes/.../dbfs/...'.

API REMINDER — `.startswith()` and `.replace()` are NOT interchangeable:
  str.startswith(prefix) -> bool      (ONLY tests; does NOT modify the string)
  str.replace(old, new)  -> str       (returns a NEW string)
NEVER write `s = s.startswith("/dbfs/", "/Volumes/...")` — that passes the second
argument as the `start` index (must be int) and returns a bool. Always use
.startswith INSIDE an `if`, then build the new string explicitly.

FORBIDDEN:
  path = path.replace('/dbfs/', '/Volumes/default/default/dbfs/')        # non-idempotent
  path = path.startswith('/dbfs/', '/Volumes/default/default/dbfs/')     # TypeError — wrong API

CORRECT (use ONE of these):
  # Option A — startswith guard, then build the new path explicitly:
  if path.startswith('/dbfs/'):
      path = '/Volumes/default/default/dbfs/' + path[len('/dbfs/'):]

  # Option B (PREFERRED) — translate_path() is idempotent and handles DBFS paths
  # (NOT s3:// or s3a://: it has no S3 bucket mapping — use suggest_oci_path):
  from aidp_compat import translate_path
  path = translate_path(path)

Same rule for /mnt/, s3://, dbfs://, /Workspace/ prefix translations.

CRITICAL - OCI Path Resolution (only for s3://, s3a://, oci:// paths):
- OCI bucket/namespace resolution ONLY applies when the code contains s3://, s3a://, or oci:// paths.
- All other paths (/FileStore/, /dbfs/, dbfs:/, /mnt/) are local DBFS paths — use translate_path()
  to convert to /Volumes/... paths. Do NOT explore them in OCI object storage.
- S3 paths (s3://bucket/...) must be translated to OCI paths (oci://oci-bucket@namespace/...)
- The S3 bucket name maps to an OCI bucket name, and each OCI bucket has a specific namespace
- Use the suggest_oci_path tool to find the correct OCI path for any S3 path
- Use the explore_path tool to verify a path exists BEFORE writing code that reads from it
- Use run_on_cluster to test data availability, check schemas, debug, or validate that a variable
  is defined (run_on_cluster runs in the MAIN kernel and shares all variables from prior cells)
- IMPORTANT: Many OCI buckets are in DIFFERENT namespaces (cross-tenancy). The namespace in the OCI path
  must match where the data actually lives, which may NOT be the AIDP workspace's default namespace.
- If a path doesn't exist, use suggest_oci_path first to find the correct bucket+namespace.
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
             bfrd_table_path = get_glue_table_s3_location('sales_analytics', 'example_flows_data_b')
         MUST STAY IDENTICAL. NEVER comment it out and replace with
             bfrd_table_path = "sales_analytics.example_flows_data_b"
         or any other hardcoded catalog identifier. The call expression is
         correct as-is — the FUNCTION's body is what gets fixed (case A above),
         not the call.

       - HARDCODING A 3-PART CATALOG NAME ("db.schema.table") as the VALUE of a
         *_path / *_location / *_uri / *_s3_location variable. These variables
         hold PATH STRINGS; downstream code like
             spark.read.parquet(f'{bfrd_table_path}/load_date=...')
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

     THIS PATTERN IS ONLY FOR PATH-STRING-RETURNING CODE. FORBIDDEN to apply to
     spark.read.table() / spark.table() / spark.sql("...FROM...") data reads —
     those stay as table reads (see the unconditional rule above).
  2. If search_catalog says a table does not exist, do NOT explore OCI paths for that data — direct
     bucket access will fail anyway. Use make_note to record the missing table and keep original code.
  3. Never hardcode oci:// paths as a replacement for table-based data access.
- SEARCH_CATALOG IS DEFINITIVE: If search_catalog returns no results, the table does NOT exist.
  Do NOT retry with name variations, different schemas, partial names, or explore_path.
  Tables match exactly or they don't exist. Record the missing table with make_note and move on.

CRITICAL - ROOT CAUSE IN PREVIOUS CELL:
If the error is caused by a PREVIOUS cell being incorrectly migrated (e.g. notebook.exit() was
commented out or replaced with pass, causing downstream cells to execute in unreachable code paths),
use fixup_cell to rewind to that cell and fix the root cause. Do NOT work around it in the current
cell — that creates fragile patches and wastes fix attempts on cascading failures.

CRITICAL - Do NOT:
- Generate code that calls dbutils.notebook.run() on non-migrated original Databricks paths
- Generate Slack webhook calls or external notification code (replace with a pass comment)
- REVERT previous fixes listed in the PREVIOUS FIXES section - those changes were validated and must be preserved

CRITICAL - DO NOT CHANGE CUSTOMER CODE LOGIC:
- Do NOT convert pandas to PySpark (e.g. pd.read_csv → spark.read.format('csv') is WRONG)
- Do NOT convert PySpark to pandas
- Do NOT change data processing logic, algorithms, or library choices
- Do NOT remove or stub code because you think a variable is "unused" or "dead code".
  You do NOT have full visibility into how variables are used across all cells and notebooks.
- Do NOT replace file reads (pickle.load, open, pd.read_csv, etc.) with hardcoded values.
  If the file path needs translation, translate the path and keep the read.
- Do NOT invent problems that don't exist. Do NOT add workarounds for hypothetical issues
  (e.g. "unmocking" libraries, clearing sys.modules, invalidating caches, writing to /tmp
  then copying to /Volumes for "FUSE consistency") unless the ACTUAL error requires it.
  FUSE issues are resolved on AIDP — write directly to /Volumes.
- Only fix the specific error — do not rewrite surrounding code
- Python/pandas version upgrades are OK (e.g. df.append → pd.concat for pandas 2.x)

CRITICAL - CODE QUALITY:
- No TODO comments, no placeholder code, no "implement later" stubs
  (EXCEPTION: unfixable Databricks infrastructure — see UNFIXABLE DATABRICKS INFRASTRUCTURE section above)
- No fallback/hack workarounds - fix the root cause
- No try/except that silently swallows errors without logging
- Every fix must be complete and production-ready
- When you modify a line, preserve the original above it in comments labelled
  "Original", and tag the change "Oracle tool modification:". Use comments valid
  for the cell's language: # for Python, // for Scala, -- for SQL.
- Tag added lines with "Oracle tool modification:" using the same comment syntax.

# FUSE consistency workarounds (safe_pickle_*, safe_write_parquet, etc.) were
# removed 2026-04-11 after FUSE stopped exhibiting the write-then-read delay
# bug on AIDP. If it resurfaces, prefer /tmp staging + shutil.copy2().

EXECUTION CONTEXT: Fixing a cell in a sequential kernel. The job execution history (most recent cells)
is shown above. If the root cause is in an earlier cell or child notebook, use get_cell_history
to identify it, then fixup_cell(start_index, why) to rewind and replay. Verify the replay sequence
is idempotent before calling fixup_cell (no double file writes, no duplicate external calls).
For a helper in an ALREADY COMPLETED dependency, use repair_dependency(index, fixed_code, why)
instead: it reopens that dependency through normal execution and durable save-back. Identify the
definition with get_history_entry, preserve its data source and date branches, and supply the
complete repaired cell. Do not repeatedly probe or redefine the helper only in the live kernel.
After a successful repair, submit the unchanged caller if no caller edit is needed.
An unsupported API argument (including secrets.get(scope=...)) is a code defect,
not DATA_UNAVAIL. A rejected repair_dependency call does not establish a missing
credential or data resource: report the repair rejection as an unresolved code
repair with make_note, without labelling it DATA_UNAVAIL. Only classify a secret
as unavailable after a correctly formed lookup actually fails to retrieve it.
Use make_note to annotate concerns about this cell.
WHEN TO REWIND: If this is attempt 7+ and the root cause appears to be upstream, prefer fixup_cell
over continuing to accumulate workarounds for this cell.

CRITICAL - HOW TO RETURN YOUR ANSWER:
You MUST call the submit_code tool with the complete fixed cell in its ORIGINAL
language (Python, Scala, or SQL), preserving any leading cell magic. Do NOT port
Scala or SQL to Python. No markdown, no prose, no explanations outside comments.
Do NOT add migration debug comments, fix-log annotations, or notes about cluster issues into the
submitted code. Use make_note for that. The delivered code must look like production code.

UNFIXABLE DATABRICKS INFRASTRUCTURE — stub immediately, do NOT keep retrying:
If the error is caused by Databricks-specific infrastructure with NO AIDP equivalent,
do NOT attempt another fix on the call itself. Instead:
1. Comment out the original code (preserve as reference)
2. Stub ALL return values so downstream cells get the variables they expect
3. print("STUBBED: <what> — original code preserved in comments above")
4. make_note("STUBBED: <what> — <why unfixable on AIDP>")

Detect by ANY of these signals:
- Direct Databricks REST API calls: requests.post/get to /api/2.0/jobs/*, /api/2.0/clusters/*
- Hardcoded Databricks job_id (large integer like 900000000000013) in any function call
- Wrapper functions that trigger Databricks jobs: run_job(), trigger_job(), submit_job(), etc.
- AWS Secrets Manager / boto3.client('secretsmanager')
- Databricks cluster policies / dbutils.secrets
- Databricks-specific webhooks or internal URLs

Example:
  # --- STUBBED: Databricks job trigger not available on AIDP ---
  # Original:
  # job_result = run_job({"run_name": model_name, "job_id": 900000000000013, ...})
  job_result = {"status": "STUBBED", "note": "Databricks job trigger not available on AIDP"}
  print("STUBBED: Databricks job trigger — original code in comments above")
If unsure what downstream cells need, use get_cell_history to check before stubbing.

KNOWN AIDP ERROR PATTERNS — fix these directly, don't investigate:
- "spark_catalog" doesn't exist / AnalysisException: catalog spark_catalog:
  Replace spark_catalog.x.y with x.y throughout the cell
- Table not found / AnalysisException on table lookup
  (TABLE_OR_VIEW_NOT_FOUND / [TABLE_OR_VIEW_NOT_FOUND] / "Table or view not found" /
   "cannot be found"): this is NOT a "fix these" pattern — it is a STOP/DATA_UNAVAIL
  condition. The ONLY edit ever permitted on a table reference is the catalog prefix:
  a 2-part name (schema.table) may become default.schema.table, and a 3-part name whose
  catalog does not exist on AIDP (e.g. `main`) may have ONLY its catalog replaced with
  `default`, schema and table UNCHANGED (main.example_team.t -> default.example_team.t).
  If, after that catalog-prefix adjustment, the table is STILL not found:
    * STOP. Do not "fix" it. The table's absence is a DATA gap, not a code bug.
    * Do NOT search for, describe_table, or point the read at a DIFFERENT table — even if a
      related-looking one EXISTS in the catalog (a *_backup / *_feature_backup / renamed /
      "migrated-equivalent" table, or a name you saw in a sibling notebook). Substituting a
      different table silently returns WRONG data and is FORBIDDEN.
    * make_note("DATA_UNAVAIL: <table> not found in AIDP catalog"), keep the ORIGINAL table
      reference exactly as written, submit best-effort/unchanged code, and move on.
  Changing schema.table to ANY other table to make the cell pass is a correctness violation,
  not a migration fix.
- ModuleNotFoundError / No module named:
  Do NOT run pip install via run_on_cluster — the cluster has no direct internet access (DNS fails).
  Missing packages are auto-installed via the AIDP cluster libraries API by the migration tool.
  If a module is still missing after auto-install, use make_note to record it and keep original code.
- NEVER run recursive filesystem scans via run_on_cluster:
  No glob.glob(..., recursive=True) on /Workspace, /aidp, or /Volumes — FUSE mounts, hangs forever.
  No os.walk('/Workspace/...'), os.walk('/aidp/...'), or os.walk('/Volumes/...') — same reason.
  Use read_notebook_source or explore_path for targeted lookups instead.
# (DISABLED 2026-04-11: FUSE issues resolved) FileNotFoundError on /Volumes — was FUSE cache bug, no longer applicable
- SparkOutOfMemoryError / OOM / driver memory exceeded:
  Try: df.persist(); df.count(); then proceed with the write. For very large DataFrames, use checkpoint.
- toPandas() OOM / driver memory exceeded on toPandas() / collect():
  Do NOT modify the original code. A runtime safety patch auto-limits large DataFrames during
  migration execution. If OOM still occurs, use make_note to record it and move on.
- When disabling an outbound operation that is side-effect only:
  Inspect its setup dependencies, including widget reads, URL construction, and headers,
  even if execution failed in that setup before reaching the network call. Comment out
  setup used exclusively by the disabled operation; preserve the original code with
  Oracle tool modification tags and report what was skipped.
  Keep any setup needed by remaining computation or downstream cells. Inspect notebook
  source/history with read_notebook_source or get_history_entry if usage is uncertain;
  do NOT assume the whole cell is disposable or disable data-producing computation.
  Classify a missing parameter as DATA_UNAVAIL only when the retained computation requires
  it. Do NOT fabricate a parameter value or emit DATA_UNAVAIL solely for unused setup
  belonging to the disabled operation.
- ConnectTimeout / ConnectionError on internal-gateway.example.com or ds-internal-gateway:
  Comment out original code (keep as reference), then:
  If side-effect only: add print("AIDP: Skipped — internal AWS endpoint not reachable")
  If it fetches data: use run_on_cluster + explore_path to find that data in OCI first
- "Table does not support deletes" / DELETE FROM / UPDATE / MERGE:
  Keep code as-is — do NOT rewrite. These are customer logic. Skip execution during migration
  (destructive operations can cause data loss). Validate syntax only.
- NoCredentialsError from boto3 / botocore:
  No AWS credentials on AIDP. Use describe_table/explore_path to reach the SAME dataset in the AIDP
  catalog or OCI Object Storage, then read from there. (This is S3/Glue access-migration of the same
  data — NOT a license to swap tables: a TABLE_OR_VIEW_NOT_FOUND is DATA_UNAVAIL, keep the original
  reference, never a backup/look-alike.) For Glue location lookups use inline
  spark.sql(f"DESCRIBE FORMATTED `{db}`.`{tbl}`").collect().
- AttributeError: 'MlflowClient' object has no attribute 'get_latest_versions':
  MLflow 3.x removed get_latest_versions() with stages. Replace with alias-based API:
  client.get_latest_versions(name, stages=['Production']) → client.get_model_version_by_alias(name, "production")
  client.get_latest_versions(name, stages=['Staging']) → client.get_model_version_by_alias(name, "staging")
- ModuleNotFoundError: No module named 'mlflow':
  MLflow may not be pre-installed. Install with: import subprocess, sys; subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow"])
  Do NOT pin a specific version.
- XGBoost 'gpu_hist' / Invalid Input 'gpu_hist':
  XGBoost 3.x removed 'gpu_hist'. Fix immediately — no investigation needed:
  Replace tree_method='gpu_hist' with tree_method='hist', device='cuda' everywhere in the cell.
  If GPU is not available on the cluster, use tree_method='hist' without device='cuda' (CPU fallback).
  If the cell defines a function, fix inside the function. If gpu_hist is in a dict/space, fix the dict.
  Use fixup_cell if gpu_hist appears in an earlier cell's function definition.
- SparkTrials / NameError: name 'SparkTrials' is not defined:
  SparkTrials is Databricks-only (distributed hyperopt). Fix immediately — no investigation needed:
  Replace SparkTrials(parallelism=N) with Trials(). Add "from hyperopt import Trials" if not imported.
  This runs hyperopt serially on the driver instead of distributed across Spark workers.
- aidp_dbutils / ModuleNotFoundError: No module named 'aidp_dbutils':
  Replace "from aidp_dbutils import _DBUtils" with "from aidp_compat import dbutils, translate_path".
  Remove the companion "dbutils = _DBUtils(...)" line — aidp_compat provides dbutils directly.
- ImportError: cannot import name 'AdamW' from 'transformers':
  transformers dropped the top-level AdamW. Fix immediately — replace "from transformers import AdamW" with
  "from torch.optim import AdamW" (keep any other symbols on the original import line).
- TypeError in pytorch-lifestream `to_pandas_record` (or ptls collate/feature-dict errors):
  DBX<->AIDP ptls version skew. Do NOT monkey-patch the library. make_note recommending the cluster pin
  pytorch-lifestream==0.6.0, then proceed.
- Path/glob read returns empty or errors on a wildcard path (".../dt=*/*.parquet"): AIDP's reader does not expand
  path wildcards. Point the read at the parent directory and keep any existing .dropDuplicates()/.filter().
- FileNotFoundError / "No such file or directory" writing a pandas file to /Volumes or /dbfs:
  The parent dir is not auto-created. Add os.makedirs(os.path.dirname(path), exist_ok=True) before the write.

═══════════════════════════════════════════════════════════════════════
DATA UNAVAILABILITY IS NOT A CODE BUG — THE FAIL-FAST DOCTRINE
═══════════════════════════════════════════════════════════════════════
When a file, table, path, or config is missing OR you don't know where it is:
1. Do NOT search for it. Recursive scans on FUSE mounts hang for HOURS.
2. Do NOT try alternative paths hoping to find it.
3. Do NOT retry the same command with slight variations.
4. Use `make_note("DATA_UNAVAIL: <resource> not found at <expected_path>")` then:
   (a) Submit best-effort code based on what you already know, OR
   (b) Comment the failing lines if the cell depends on missing data, OR
   (c) Submit the original code unchanged with a make_note explaining why.
5. Move on. Do not linger. The next cell may work.

The ONLY exploratory tool allowed is `explore_path(specific_path)` with a
KNOWN one-level directory. All recursive/wildcard traversal via `run_on_cluster`
is STRUCTURALLY BLOCKED — the tool WILL REFUSE.

INVESTIGATION BUDGET (STRUCTURAL — HARD LIMIT):
- Maximum <MAX_RUN_ON_CLUSTER_PER_CALL> `run_on_cluster` calls per Opus turn. The tool will refuse further calls.
- Maximum <MAX_TOOL_ROUNDS_FIX> total tool rounds per Opus turn (all tools combined).
- Maximum <MAX_FIX_ATTEMPTS> Opus fix-attempts per cell. After that, cell is marked FAIL/PARTIAL.
These are enforced by the migration tool — NOT advisory. If you exceed them,
you get REFUSED responses and lose the round. Commit to a fix EARLY.

If the error matches KNOWN AIDP ERROR PATTERNS above, fix it directly on
attempt 1 with ZERO investigation. Only investigate when the error is
genuinely unfamiliar.

NEVER run recursive filesystem scans on the cluster:
- Recursive traversal is BLOCKED on every absolute path EXCEPT /tmp/.
- No glob.glob(..., recursive=True), os.walk('/...'), .rglob(), .glob('**'),
  grep -r, find, or ls -R on any non-/tmp/ path — the tool will refuse.
- /tmp is allowed because pre-migration UUID-unique subfolders exist there
  and legitimate cells may need to walk to find them.
- If you need a notebook path, use read_notebook_source with the known path.
  If unknown, check the cell history or use explore_path on the specific
  directory — do NOT scan the workspace."""

# Fix 7z (2026-07-29): substitute the fail-fast cap values from the constants
# above so the prompt cannot drift out of sync with the enforced numbers.
# If MAX_RUN_ON_CLUSTER_PER_CALL / MAX_TOOL_ROUNDS_FIX / MAX_FIX_ATTEMPTS are
# tuned, the prompt automatically reflects the new values without a manual
# edit — silent-failure guard.
# The raw template is kept so _rebake_fix_prompt() can re-substitute after
# apply_cfg_to_modules changes MAX_FIX_ATTEMPTS from cfg.runtime (the caps
# below are baked at import time, BEFORE the YAML is loaded).
_FIX_PROMPT_TEMPLATE = FIX_PROMPT + PATH_PREFIX_MIGRATION_GUIDANCE


def _rebake_fix_prompt() -> None:
    """Re-substitute the cap placeholders from the CURRENT constant values.

    Phase 2: MLflow tracking URI is no longer a prompt placeholder — Opus
    fetches it at cell runtime via aidputils.secrets.get('migration_tool_secrets',
    key='mlflow-tracking-uri') per the MLflow section of the fix prompt.
    """
    global FIX_PROMPT
    FIX_PROMPT = (
        _FIX_PROMPT_TEMPLATE
        .replace("<MAX_RUN_ON_CLUSTER_PER_CALL>", str(MAX_RUN_ON_CLUSTER_PER_CALL))
        .replace("<MAX_TOOL_ROUNDS_FIX>", str(MAX_TOOL_ROUNDS_FIX))
        .replace("<MAX_FIX_ATTEMPTS>", str(MAX_FIX_ATTEMPTS))
    )


_rebake_fix_prompt()


def call_opus(system: str, user_content: str, max_tokens: int = 32000) -> Tuple[str, dict]:
    """Call Claude Opus 5 with streaming. Returns (response_text, usage)."""
    import random
    # Self-contained transient-retry for this SYNC streaming call (dep-migration
    # hot path). Mirrors _create_message_with_529_retry's classification without
    # touching it: retry an opaque 400 invalid_request_error / 529 overloaded /
    # connection blip (bounded), but let "prompt is too long" and any other error
    # propagate exactly as before (no regression — the happy path and non-transient
    # errors behave identically; only transient blips now retry instead of failing).
    _max_transient = 3
    _attempt = 0
    while True:
        text_parts = []
        try:
            with claude().messages.stream(
                model=llm_config.MODEL,
                max_tokens=max_tokens,
                timeout=600,  # 10 min timeout for the API call itself
                system=system,
                messages=[{"role": "user", "content": user_content}]
            ) as stream:
                for text in stream.text_stream:
                    text_parts.append(text)
            response = stream.get_final_message()
            full_text = "".join(text_parts)
            _record_usage(_infer_route(system), response.usage)
            return full_text, {"input": response.usage.input_tokens, "output": response.usage.output_tokens}
        except Exception as exc:
            _s = str(exc)
            _is_conn = isinstance(exc, (anthropic.APIConnectionError, anthropic.APITimeoutError))
            # NARROWED (review #49): only the bare opaque "Invalid request data"
            # server blip retries — not every 400/invalid_request (a real payload
            # bug must propagate immediately, not burn backoff sleeps).
            _is_transient = (
                _is_conn
                or "Error code: 529" in _s or "overloaded_error" in _s
                or ("Invalid request data" in _s
                    and "prompt is too long" not in _s
                    and "prompt_too_long" not in _s)
            )
            if _is_transient and _attempt < _max_transient:
                _attempt += 1
                _wait = (2 ** _attempt) + random.random()
                print(f"  [call_opus] transient Anthropic error — retry "
                      f"{_attempt}/{_max_transient} in {_wait:.1f}s: {_s[:120]}")
                time.sleep(_wait)
                continue
            raise


# ─── Tool Definitions for OCI Path Exploration ───────────────────────

OCI_PATH_TOOLS = [
    {
        "name": "explore_path",
        "description": "Explore an OCI, S3, or HDFS path on the AIDP cluster. Checks if the path exists, lists directory contents, or expands glob/wildcard patterns. Runs on the cluster using Spark's Hadoop filesystem (BmcFilesystem is configured at cluster level with API key auth). Use this to verify whether data is available before writing code that reads from it. IMPORTANT: Do NOT use this for /Workspace/, /Volumes/, or /tmp/ paths — those are local FUSE mounts, not OCI object storage. Only use for oci://, s3://, s3a://, or hdfs:// paths.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "The storage path to explore. Examples: 'oci://bucket@namespace/prefix/', 'oci://bucket@namespace/path/*.parquet', 's3://bucket/prefix/'"
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "suggest_oci_path",
        "description": "Given an S3, DBFS, or OCI path, look up the correct OCI path from the S3-to-OCI bucket mapping. Use this when you see an S3 path (s3://...) or a DBFS path (dbfs:/...) or an OCI path with the wrong namespace and need to find the correct OCI equivalent. Returns suggested oci:// paths with the right bucket name and namespace.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "The path to look up. Can be s3://, dbfs:/, /mnt/, or oci:// with potentially wrong namespace."
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "search_catalog",
        "description": "Check if a table exists in the AIDP catalog by exact name. Pass the exact table name (e.g. 'schema.table'). Returns EXISTS or NOT FOUND. Result is DEFINITIVE — if NOT FOUND, the table does NOT exist. Use this ONLY to verify the EXACT table the code already references (optionally with the default. catalog prefix). If it is NOT FOUND, that is DATA_UNAVAIL — do NOT search for a different/backup/look-alike/'migrated-equivalent' table to use instead, and do NOT describe_table any such alternative; substituting a different table is FORBIDDEN. Record the missing table with make_note and keep the original reference.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The table name or partial name to search for. Example: 'interactions', 'example_dues', 'allocation'"
                }
            },
            "required": ["query"],
            "additionalProperties": False
        }
    },
    {
        "name": "run_on_cluster",
        "description": "Execute strictly read-only Python/PySpark diagnostics on the AIDP cluster and return the output. Mutating Spark SQL (INSERT/CREATE/DROP/ALTER/MERGE/UPDATE/DELETE/etc.), dynamic SQL that cannot be proven read-only, and DataFrame write/writeStream/writeTo APIs are hard-blocked before execution. Use this to check table schemas (spark.sql('DESCRIBE table').show()), test data availability (spark.read.parquet('path').count()), inspect DataFrames, list files, EXPLAIN a write without executing it, or debug issues. The cluster has spark, sc, and aidp_compat available. Code must use print() to produce output. Keep code short and focused - this is for exploration, not for producing the final cell code.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Python/PySpark code to execute. Must use print() for output. Example: \"print(spark.sql('SHOW TABLES IN collection').collect()[:10])\""
                }
            },
            "required": ["code"],
            "additionalProperties": False
        }
    },
    {
        "name": "describe_table",
        "description": "Get the schema (columns, types) of a Spark table or view. Only call on actual Spark table names (schema.table or default.schema.table), NOT Python module names. If the table was not found by search_catalog, do NOT call describe_table on it.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name. Example: 'ops.example_dues_all', 'default.my_table'"
                }
            },
            "required": ["table_name"],
            "additionalProperties": False
        }
    },
    {
        "name": "list_schemas_and_tables",
        "description": "List available Spark schemas (databases) and optionally tables within a specific schema. Use this to discover what data is registered in the AIDP Spark catalog.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "schema": {
                    "type": ["string", "null"],
                    "description": "Specific schema to list tables from. Pass null to list all schemas."
                }
            },
            "required": ["schema"],
            "additionalProperties": False
        }
    },
    {
        "name": "submit_data_recovery_plan",
        "description": (
            "Submit a plan to substitute hardcoded dates in a cell with actual "
            "available dates from the upstream table. ONLY call this when a "
            "cell has failed with an empty-data signature (IndexError after "
            "indexed-access on a query, AnalysisException about missing path, "
            "etc.) and the failure is caused by hardcoded date filter values "
            "that the upstream table simply doesn't have data for. "
            "Your job is to IDENTIFY: (1) the upstream table being queried, "
            "(2) the column used to filter by date, and (3) the cell-scope "
            "variable(s) that hold the date filter values. Do NOT modify the "
            "cell's code. The framework will query the table for actually "
            "available dates and build the substitution override at execution "
            "time only — the saved cell stays byte-identical to the original. "
            "Use describe_table and run_on_cluster tools to confirm the table "
            "name and date column before submitting."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Fully-qualified table the cell queries, e.g. 'default.db.tbl' or 'corp_analytics.tbl'. Must be a real table verified via describe_table."
                },
                "date_column": {
                    "type": "string",
                    "description": "The column name in the table that holds the date the cell is filtering by, e.g. 'load_date', 'partition_date', 'decision_date'."
                },
                "overrides": {
                    "type": "array",
                    "description": "List of cell-scope variables whose values should be replaced with actual dates from the table.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "variable": {
                                "type": "string",
                                "description": "Name of the variable in the cell (e.g. 'uw_fct_date_list', 'start_date')."
                            },
                            "is_list": {
                                "type": "boolean",
                                "description": "True if the variable is a list of dates (e.g. uw_fct_date_list = ['2026-04-01', ...]); False if it's a single date string."
                            },
                            "max_dates": {
                                "type": "integer",
                                "description": "For is_list=True: how many recent dates to include in the substituted list (capped at 14)."
                            }
                        },
                        "required": ["variable", "is_list", "max_dates"],
                        "additionalProperties": False
                    }
                }
            },
            "required": ["table", "date_column", "overrides"],
            "additionalProperties": False
        }
    },
    {
        "name": "submit_code",
        "description": "Submit the complete migrated or fixed cell in its original language (Python, Scala, or SQL), preserving any leading cell magic. You MUST call this tool to return your answer. No markdown fences or prose outside code comments.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "The complete executable cell in its original language, including its cell magic if present. No markdown fences or prose outside code comments."
                }
            },
            "required": ["code"],
            "additionalProperties": False
        }
    },
    {
        "name": "read_notebook_source",
        "description": "Read the source code of a notebook from the AIDP workspace. Use this to understand dependent notebooks referenced via %run or dbutils.notebook.run - check function definitions, imports, variable assignments etc.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the notebook in the AIDP workspace. Example: 'Users/user3@example.com/Utils/Data_Utils.ipynb'"
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "inspect_package_source",
        "description": "Look up the source code of a function, class, or method from an installed Python package on the AIDP cluster. Use this to understand how a dependency works - e.g., how aidp_dbutils._DBUtils.run() is implemented, or what pyspark.sql.functions.sum does vs builtins.sum.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "module": {
                    "type": "string",
                    "description": "Python module path. Example: 'aidp_compat.path_translator', 'pyspark.sql.functions', 'aidp_dbutils'"
                },
                "name": {
                    "type": "string",
                    "description": "Function or class name. Example: 'translate_path', 'sum', '_DBUtils'"
                },
                "method": {
                    "type": ["string", "null"],
                    "description": "Method name if inspecting a class method. Pass null if not applicable. Example: 'run'"
                }
            },
            "required": ["module", "name", "method"],
            "additionalProperties": False
        }
    },
    {
        "name": "summarize_notebook",
        "description": "Get a concise summary of a notebook's purpose, key functions, imports, and variables it defines. Uses the configured model for summarization. Results are cached - calling this twice with the same path returns the cached summary instantly.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the notebook. Example: 'Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb'"
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "make_note",
        "description": (
            "Append a note about the current cell to the fix log and to cell history memory. "
            "Use this to record migration concerns, root cause observations, or important context "
            "you want to remember for this cell (e.g. 'This cell divides by X which will be 0 if "
            "upstream data is empty'). "
            "Notes are truncated to 500 characters. Multiple notes can be made; the last one is "
            "stored in the cell history entry. All notes are appended to the cell source's fix log comment."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "note": {
                    "type": "string",
                    "description": "Your note about this cell — migration concerns, root causes, or important observations. Max 500 chars."
                }
            },
            "required": ["note"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_cell_history",
        "description": (
            "Get a slice of the job-wide cell execution history. Each entry shows the index, "
            "notebook, cell number, a summary of what the cell does, its status (ok/error), "
            "and a short output preview. Use this to scan backwards and find which index "
            "caused the root-cause issue before calling fixup_cell."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "from_index": {
                    "type": ["integer", "null"],
                    "description": "Start index (inclusive). Use negative for tail: -20 = last 20. Pass null to start from beginning."
                },
                "to_index": {
                    "type": ["integer", "null"],
                    "description": "End index (exclusive). Pass null for end of history."
                }
            },
            "required": ["from_index", "to_index"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_history_entry",
        "description": "Get full details of a single history entry including its complete code. Use after get_cell_history identifies a suspicious entry.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "index": {"type": "integer", "description": "The history entry index."}
            },
            "required": ["index"],
            "additionalProperties": False
        }
    },
    {
        "name": "fixup_cell",
        "description": (
            "Rewind to a specific history index and replay all cells from that point to the current position. "
            "Each replayed cell goes through the full execute+verify+Opus-fix loop with your 'why' diagnosis "
            "injected as context at every fix attempt. Use after get_cell_history identifies the root cause index.\n\n"
            "IMPORTANT — IDEMPOTENCY REQUIREMENT: Replaying re-executes every cell from start_index forward. "
            "This is only safe if those cells are idempotent (re-running them does not cause double writes, "
            "duplicate Spark jobs, or corrupted state). Before calling fixup_cell, verify that the replay "
            "sequence does not include cells that write to files, tables, or external systems in a non-idempotent "
            "way. If the sequence has non-idempotent side effects, use get_history_entry to identify the cells "
            "and consider whether fixup_cell is the right approach, or whether you should fix the current cell "
            "directly instead.\n\n"
            "Optionally provide fixed_code for the target cell to replace its code before replay begins. "
            "For active child notebook cells, fixed_code updates pending child source; "
            "the normal child save-back must succeed before the repair is durable. "
            "Changed replays of completed children are rejected; use repair_dependency instead.\n\n"
            "NEVER use fixed_code to point a read at a DIFFERENT table than the original "
            "(spark.read.table/spark.table): the only table edit allowed is the `default.` catalog "
            "prefix — the schema.table must stay identical. Swapping in a backup/look-alike table to "
            "get past a TABLE_OR_VIEW_NOT_FOUND is a correctness violation; that case is DATA_UNAVAIL."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "start_index": {
                    "type": "integer",
                    "description": "The history index to start replay from (inclusive). All entries from here to end of history are replayed."
                },
                "why": {
                    "type": "string",
                    "description": "Your diagnosis of the root cause. Injected into the fix context for every replayed cell."
                },
                "fixed_code": {
                    "type": ["string", "null"],
                    "description": "Your fixed version of the target cell's code. Pass null if not providing a fix upfront. If provided, replaces the target cell's code before replay. Active child repairs update pending source for normal save-back; completed-child changes are rejected."
                }
            },
            "required": ["start_index", "why", "fixed_code"],
            "additionalProperties": False
        }
    },
    {
        "name": "repair_dependency",
        "description": (
            "Repair a helper defined in a completed child notebook of the CURRENT task. "
            "Use get_history_entry to identify its original definition, then supply that cell's "
            "complete repaired source. This reruns the entire dependency through normal "
            "execution, validation and durable save-back; it does not bypass replay ownership. "
            "Verify the dependency is safe to rerun (no non-idempotent side effects). "
            "Preserve original code in comments and tag changes # Oracle tool modification:. "
            "Do not change data sources or mask errors. After success, retry the current caller; "
            "if intervening parent cells materialized affected results, use fixup_cell to rebuild "
            "those cells first. Never repair only a live function with run_on_cluster."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "index": {"type": "integer", "description": "History index of the helper definition."},
                "fixed_code": {"type": "string", "description": "Complete repaired definition cell."},
                "why": {"type": "string", "description": "Evidence-backed diagnosis and rerun safety."},
            },
            "required": ["index", "fixed_code", "why"],
            "additionalProperties": False,
        },
    },
    {
        "name": "scan_sensitive_info",
        "description": "Scan a notebook file for hardcoded sensitive info: Databricks API tokens (dapi...), Slack tokens/webhooks, internal the pilot deployment endpoints (internal-gateway.example.com, ds-internal-gateway), and Databricks REST API calls. Returns list of matches with cell index, pattern type, and matched line. Use this when migrating cells that may have hardcoded credentials or internal AWS endpoint calls.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "notebook_path": {
                    "type": "string",
                    "description": "Absolute path to the notebook file on the cluster (e.g. /Workspace/Users/.../notebook.ipynb)"
                }
            },
            "required": ["notebook_path"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_job_parameters",
        "description": "Get job-level parameters for the current task from the job manifest. Use when dbutils.widgets.get(name) raises ValueError — the parameter value may be defined at the job level, not in the notebook itself.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "task_key": {
                    "type": ["string", "null"],
                    "description": "Task key to look up parameters for. Pass null to use the current task."
                }
            },
            "required": ["task_key"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_tool_output",
        "description": "Retrieve the full untruncated output of a previous tool call that was truncated to save context space. The filename is shown in the truncation notice (e.g. 'tool_003_run_on_cluster.txt'). Use this when you need details that were cut off. Only the exact bare filename from a truncation notice is accepted; directory paths are refused.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "filename": {
                    "type": "string",
                    "description": "Filename from the truncation notice, e.g. 'tool_003_run_on_cluster.txt'"
                }
            },
            "required": ["filename"],
            "additionalProperties": False
        }
    },
]


async def _handle_scan_sensitive_info(notebook_path: str, session, log_fn=None) -> str:
    """Scan a notebook for hardcoded sensitive info."""
    def _log(msg):
        if log_fn: log_fn(msg)

    import re as _re
    import json as _json

    _log(f"scan_sensitive_info: {notebook_path}")

    SENSITIVE_PATTERNS = [
        ("databricks_token", r'dapi[a-zA-Z0-9]{32,}'),
        ("slack_token", r'xox[bprs]-[0-9A-Za-z\-]+'),
        ("slack_webhook", r'hooks\.slack\.com/services/[A-Za-z0-9/]+'),
        ("examplecorp_internal", r'internal-gateway\.example\.com|ds-internal-gateway'),
        ("databricks_api", r'azuredatabricks\.net|api/2\.0/jobs'),
        # MLflow tracking URI with embedded credentials (user:password@host)
        # e.g. mlflow.set_tracking_uri("https://admin:secret@<mlflow-host>")
        ("mlflow_embedded_creds", r'mlflow\.set_tracking_uri\s*\([^)]*://[^:]+:[^@]+@'),
    ]

    # Read notebook via cluster — cap at 150 cells to stay within AIDP output limits
    code = f"""
import json, builtins
with builtins.open("{notebook_path}", "r") as f:
    nb = json.load(f)
cells = nb.get("cells", [])
sources = []
for i, cell in enumerate(cells):
    if cell.get("cell_type") == "code":
        src = "".join(cell.get("source", []))
        sources.append((i, src))
    if len(sources) >= 150:
        break
print(json.dumps(sources))
"""
    run = getattr(session, 'run_stateless', session.execute)
    result = await run(code, timeout=30)
    raw = _unwrap_aidp_text(format_outputs(result.get("outputs", [])))

    try:
        sources = _json.loads(raw)
    except Exception:
        return (
            f"Could not parse notebook sources (output may have been truncated by AIDP). "
            f"Raw output ({len(raw)} chars): {raw[:300]}"
        )

    matches = []
    for cell_idx, source in sources:
        for pattern_name, pattern in SENSITIVE_PATTERNS:
            for m in _re.finditer(pattern, source):
                line = source[max(0, m.start()-30):m.end()+30].replace('\n', ' ')
                matches.append({"cell_idx": cell_idx, "pattern": pattern_name, "match": line})

    if not matches:
        return "No sensitive info found."
    return _json.dumps(matches, indent=2)


async def _handle_get_job_parameters(log_fn=None) -> str:
    """Return the merged parameters for the current task.

    Parameters are pre-merged by process_job(): task-level base_parameters
    first, then job-level parameters override (job wins on conflict).
    """
    def _log(msg):
        if log_fn: log_fn(msg)

    import json as _json

    _log(f"get_job_parameters: task={_current_task_key!r}, params={bool(_current_task_params)}")

    if _current_task_params:
        return _json.dumps(_current_task_params, indent=2)
    return "No job parameters found for the current task."


def _handle_get_tool_output(filename: str, log_fn=None) -> str:
    """Read full tool output from disk.

    Searches the current compactor first, then previous compactors in reverse
    order (newest first). This allows the fix phase to retrieve files that were
    saved during the eval phase of the same cell.
    """
    def _log(msg):
        if log_fn: log_fn(msg)

    if not _compactor_history:
        return "No active compactor — tool output files not available."

    from context_compactor import NOT_FOUND_PREFIX, REFUSED_PREFIX, validate_output_filename

    _log(f"get_tool_output: {filename}")
    # The filename is model-supplied: accept only a bare tool_<NNN>_<tool>.txt
    # name, before any compactor touches the filesystem
    # (SEC-AIDP-SAMPLES-DBX-NEW-01).
    reason = validate_output_filename(filename)
    if reason is not None:
        _log(f"get_tool_output REFUSED {filename!r}: {reason}")
        return f"{REFUSED_PREFIX}: {reason}"

    # Search current first, then walk history in reverse order
    for compactor in reversed(_compactor_history):
        result = compactor.get_saved_output(filename)
        if result.startswith(REFUSED_PREFIX):
            # Policy refusal is final; do not keep probing older compactors.
            return result
        if not result.startswith(NOT_FOUND_PREFIX):
            return result

    # All compactors tried — return the last "not found" message (lists available files)
    return result


def _download_notebook_source(path: str):
    """Download a notebook from the AIDP workspace and return the parsed JSON dict.

    Returns the notebook dict on success, or None if not found.
    Raises on network/auth errors.

    Uses the module-level DOWNLOAD_META_URL + signer() — both configured at
    runtime for the active deployment (job_migrate_from_workflow overrides them).
    No hardcoded environment, so this targets whatever lake/workspace/profile
    the run is using (previously it always hit a fixed the pilot deployment/Mumbai workspace).
    """
    import requests as _requests

    for try_path in [path, path + ".ipynb" if not path.endswith(".ipynb") else path]:
        headers = {"Content-Type": "application/json", "path": try_path, "type": "NOTEBOOK"}
        resp = _requests.post(DOWNLOAD_META_URL, auth=signer(), headers=headers, data="")
        if resp.status_code == 200:
            par_url = resp.json().get("parUrl")
            if par_url:
                data = _requests.get(par_url)
                if data.status_code == 200:
                    return json.loads(data.content)
    return None


async def _handle_tool_call(tool_name: str, tool_input: dict, session=None, log_fn=None) -> str:
    """Execute a tool call and return the result string.
    If session is provided, can run cluster-based tools (async)."""
    from context_tools import explore_oci_path, suggest_oci_path, search_catalog

    _SKIP_MODULES = {
        "os", "sys", "json", "re", "io", "math", "time", "datetime",
        "collections", "functools", "itertools", "typing", "pathlib",
        "hashlib", "base64", "copy", "csv", "logging", "traceback",
        "subprocess", "tempfile", "shutil", "glob", "urllib", "http",
        "concurrent", "threading", "multiprocessing", "abc",
        "dataclasses", "enum", "struct", "pickle", "gzip", "zipfile",
        "contextlib", "pyspark", "numpy", "pandas", "scipy", "sklearn",
        "matplotlib", "seaborn", "plotly", "optuna", "xgboost",
        "lightgbm", "tensorflow", "torch", "keras", "pil", "cv2",
        "requests", "boto3", "botocore", "oci", "delta", "pytz",
        "dateutil", "aidp_compat", "oidlutils", "pprint", "operator",
        "decimal", "fractions", "warnings", "inspect", "ast", "textwrap",
        "string", "random", "secrets", "uuid", "socket", "ssl",
        "email", "html", "xml", "ctypes", "np", "pd", "spark",
    }

    if tool_name == "submit_code":
        # This is handled specially in call_opus_with_tools - should not reach here
        return "Code submitted"

    if tool_name == "submit_data_recovery_plan":
        # Also handled specially in call_opus_with_tools (early-return) — this
        # path is only reached if Opus calls the tool twice or in an unexpected
        # context. Return a harmless ack so the tool-use loop continues without
        # exception.
        return "Data recovery plan recorded."

    if tool_name == "explore_path":
        path = tool_input.get("path", "")
        if path in _path_explore_cache:
            return _path_explore_cache[path] + "\n(cached — already explored this path)"
        if session:
            result = await explore_oci_path(session, path)
            _path_explore_cache[path] = result
            return result
        else:
            return f"No cluster session available. Path: {path}"

    elif tool_name == "suggest_oci_path":
        path = tool_input.get("path", "")
        suggestions = suggest_oci_path(path)
        if suggestions:
            return "Suggested OCI paths:\n" + "\n".join(f"  {s}" for s in suggestions)
        else:
            return f"No mapping found for: {path}"

    elif tool_name == "search_catalog":
        query = tool_input.get("query", "")
        _q_first = query.split(".")[0].lower()
        if _q_first in _SKIP_MODULES:
            return f"'{query}' is a Python module, not a table. Skip."
        # Normalize cache key: lowercase + strip default. prefix — so
        # "exbridge_pipeline.FOO", "default.exbridge_pipeline.foo", "exbridge_pipeline.foo"
        # all hit the same cache entry.
        _cache_key = query.lower().strip()
        if _cache_key.startswith("default."):
            _cache_key = _cache_key[len("default."):]
        # Check run-level cache first — avoid repeated lookups for the same table
        if _cache_key in _table_lookup_cache:
            return _table_lookup_cache[_cache_key] + " (cached — already searched, do NOT retry)"
        result = search_catalog(query)
        # If offline catalog unavailable, try live cluster check
        if "No catalog available" in result and session:
            try:
                run = getattr(session, 'run_stateless', session.execute)
                _check_code = (
                    f"try:\n"
                    f"    cols = spark.sql('DESCRIBE TABLE {query}').collect()\n"
                    f"    print(f'Table EXISTS: {query} ({{len(cols)}} cols)')\n"
                    f"except Exception as e:\n"
                    f"    print(f'Table NOT FOUND: {query}')\n"
                )
                live_result = await run(_check_code, timeout=60)
                result = _extract_output(live_result)
            except Exception:
                result = f"Table NOT FOUND: '{query}' — catalog unavailable, cluster check failed"
        _table_lookup_cache[_cache_key] = result
        return result

    elif tool_name == "run_on_cluster":
        code = tool_input.get("code", "")
        _mutation_msg = _detect_run_on_cluster_mutation(code)
        if _mutation_msg:
            return _mutation_msg
        # ─── Fix 7a (2026-07-25): hard code guardrail against FUSE-recursive scans ─
        #
        # These patterns are known to hang for tens of minutes on AIDP's FUSE-mounted
        # /Workspace (each readdir → one HTTP roundtrip). When a scan runs longer
        # than the 1200s tool timeout AND the AIDP kernel-interrupt endpoint returns
        # 404 (which it does on Mumbai lake as of 2026-07-25), the kernel-side
        # subprocess KEEPS RUNNING even after we abandon the Python-side wait. All
        # subsequent execute_requests queue behind it and appear to hang for hours;
        # in the worst case the kernel is degraded until the cluster is restarted.
        #
        # Fix 5a (FIX_PROMPT guardrails) is advisory — Opus can violate. This is a
        # hard block: if the submitted code contains any of the patterns below, we
        # REFUSE to send it to the kernel and return a structured error to Opus.
        # Opus's next round sees the block reason and picks a bounded alternative.
        #
        # Real-world impact this prevents (a pilot-customer AutoRefit, 2026-07-25):
        #   Opus fix-loop submitted:
        #     subprocess.run("grep -rln 'APP_SOURCE_NAME' /Workspace/ 2>/dev/null | head -40",
        #                    shell=True, capture_output=True, text=True)
        #   That ran for 20+ minutes, tool timed out, kernel stayed busy on the grep
        #   for another 60+ minutes. Cluster kernel was degraded for the rest of the
        #   session — every subsequent operation was 5–30× slower than normal.
        #
        # SCOPE: we only block patterns that combine (a) a recursive traversal
        # primitive with (b) a target under /Workspace, /aidp, or /Volumes (all
        # FUSE mounts on AIDP — each readdir is an HTTP roundtrip).
        # Single-file, single-directory-listing, and local /tmp/ scans are allowed.
        # /Volumes added 2026-07-28 after a run_on_cluster diagnostic hung the
        # kernel for 20 minutes with glob.glob('/Volumes/**/...', recursive=True).
        #
        # False-positive avoidance: strip `#`-commented lines before matching so
        # a comment mentioning "grep -r /Workspace" doesn't block a legit cell.
        import re as _re_scan

        # Strip Python comment lines before pattern matching so a commented-out
        # forbidden call in a docstring or note doesn't produce a false BLOCKED.
        _active_code = "\n".join(
            ln for ln in (code or "").splitlines()
            if ln.strip() and not ln.strip().lstrip().startswith("#")
        )

        # Existing rules (kept — targeted at the specific forms we saw earlier).
        if _re_scan.search(r'glob\.glob\s*\([^)]*recursive\s*=\s*True', _active_code) and \
           _re_scan.search(r'["\']/(Workspace|aidp|Volumes)', _active_code):
            return ("BLOCKED: Recursive glob on /Workspace, /aidp, or /Volumes hangs indefinitely (FUSE mount). "
                    "Use read_notebook_source with the correct path, or explore_path for targeted lookups.")
        if _re_scan.search(r'os\.walk\s*\(\s*[\'"](/(Workspace|aidp|Volumes))', _active_code):
            return ("BLOCKED: os.walk on /Workspace, /aidp, or /Volumes hangs indefinitely (FUSE mount). "
                    "Use read_notebook_source with the correct path, or explore_path for targeted lookups.")

        # Fix 7a additions — subprocess-based recursive scans:
        #
        # These use a COARSE two-part match rather than trying to correlate flag
        # + target within one regex. The tighter regex approach failed on realistic
        # shell/argv forms because quoted patterns sit BETWEEN the flag and the
        # target (e.g. `grep -rln 'PATTERN' /Workspace/`), so a "no-quotes-between"
        # regex misses the exact case that hit us. Coarse two-part is more robust:
        #
        #   Part A: the code mentions the tool with a recursive flag
        #     (grep -r/-R/-rl/-rn, find, ls -R)
        #   Part B: the code mentions /Workspace or /aidp as a path
        #
        # Both TRUE anywhere in the submitted code → block. In principle this can
        # over-flag if someone submits code that mentions grep -r AND separately
        # mentions /Workspace in a totally unrelated context, but that combination
        # inside a single `run_on_cluster` call is not a normal use case — Opus
        # either does the walk or doesn't. Erring toward false-positive keeps
        # this simple + prevents kernel damage.
        _mentions_fuse_mount_path = bool(
            _re_scan.search(r'/(Workspace|aidp|Volumes)\b', _active_code)
        )

        # 1. `grep -r` / `-R` / `-rl` / `-rn` (any -r variant) — combined with
        #    a FUSE-mount path mention. Matches argv AND shell forms uniformly.
        if _mentions_fuse_mount_path and _re_scan.search(
            r'\bgrep\b[^\n]*?(?:-[rR]|["\']-[rR])[rRlLnN]*\b', _active_code,
        ):
            return ("BLOCKED: recursive grep (grep -r / -R / -rl / -rn) with a /Workspace, "
                    "/aidp, or /Volumes path mentioned anywhere in the submitted code — hangs "
                    "the kernel indefinitely (FUSE mount, each readdir = one HTTP roundtrip). "
                    "Use single-file grep — e.g. subprocess.run(['grep','-c','pattern', "
                    "'/Workspace/known/single/file.ipynb']) — or the `explore_path` tool with a "
                    "specific path, or `read_notebook_source` when you know the file. "
                    "See FIX_PROMPT rules for the full list of banned patterns.")

        # 2. `find` command combined with a FUSE-mount path mention.
        # (?!\s*\() excludes Python method-call syntax — `text.find("x")` /
        # `s.find(...)` is a bounded string operation, not the shell `find`
        # walker. Without it, any diagnostic using str.find() plus a FUSE
        # path literal was falsely BLOCKED (and the refusal still consumed
        # one of the MAX_RUN_ON_CLUSTER_PER_CALL budget slots).
        if _mentions_fuse_mount_path and _re_scan.search(r'\bfind\b(?!\s*\()', _active_code):
            return ("BLOCKED: `find` combined with a /Workspace, /aidp, or /Volumes path "
                    "mention — walks the FUSE tree same as `grep -r`, hangs the kernel. Use "
                    "`os.path.exists(specific_path)` for existence checks or `explore_path` "
                    "with a bounded directory.")

        # 3. `ls -R` combined with a FUSE-mount path mention.
        if _mentions_fuse_mount_path and _re_scan.search(r'\bls\b[^\n]*?-R[a-zA-Z]*\b', _active_code):
            return ("BLOCKED: `ls -R` combined with a /Workspace, /aidp, or /Volumes path "
                    "mention — same FUSE-recursive hang. Use `os.listdir(specific_dir)` for a "
                    "single directory or `explore_path` for a targeted lookup.")

        # 4. pathlib.Path(...).rglob(...) — recursive glob at pathlib level.
        #    Any use of .rglob is unsafe on FUSE regardless of target because the
        #    "**" semantics walks the entire tree. Guard even without an explicit
        #    /Workspace reference — Opus sometimes constructs the path via variable.
        if _re_scan.search(r'\.rglob\s*\(', _active_code):
            return ("BLOCKED: `.rglob()` recursively walks the filesystem — hangs on "
                    "FUSE-mounted /Workspace or /aidp. Use `Path(specific_dir).glob('*')` (single "
                    "level, NOT `**`) or `explore_path` for bounded traversal.")

        # 5. pathlib .glob('**/...') — recursive glob explicit form.
        if _re_scan.search(r"""\.glob\s*\(\s*["'][^"']*\*\*""", _active_code):
            return ("BLOCKED: `.glob('**/...')` recursively walks the filesystem — hangs on "
                    "FUSE-mounted /Workspace or /aidp. Use `.glob('*')` (single level) or "
                    "`explore_path` for bounded traversal.")

        # 6. `glob.glob('/Workspace/**', ...)` explicit-** form (complements the
        #    existing recursive=True check above, which requires the keyword arg).
        if _re_scan.search(r'''glob\.glob\s*\(\s*["'][^"']*/(Workspace|aidp|Volumes)[^"']*\*\*''', _active_code):
            return ("BLOCKED: `glob.glob('/Workspace|/aidp|/Volumes/**', ...)` recursively walks "
                    "the FUSE mount and hangs indefinitely. Use a bounded, single-level pattern or "
                    "`explore_path` with a specific directory.")

        # Fix 7z (2026-07-29): BLANKET fail-fast rule. Recursive traversal is
        # restricted to /tmp/ ONLY on the cluster. Every other absolute path is
        # either a FUSE mount (walks hang for hours) or a system dir (/etc, /opt,
        # /usr — irrelevant to migration). This is a SUPERSET of the specific
        # /Workspace|/aidp|/Volumes rules above — those give more targeted error
        # messages when they match; this catches everything else, including any
        # future mount AIDP adds without a code change here.
        #
        # /tmp/ is exempted because pre-migration UUID-unique subfolders are
        # created and subsequent cells legitimately need to walk to find them
        # (the subsequent cell doesn't know the exact UUID path). Local /tmp
        # is not FUSE — walks complete quickly.
        _recursive_primitive_pat = (
            r'(glob\.glob\s*\([^)]*recursive\s*=\s*True'
            r'|os\.walk\s*\('
            r'|\.rglob\s*\('
            r'|\.glob\s*\(\s*["\'][^"\']*\*\*'
            r'|\bgrep\b[^\n]*?-[rR]'
            r'|\bfind\b(?!\s*\()'   # shell find; excludes Python str.find(...) calls
            r'|\bls\b[^\n]*?-R'
            r')'
        )
        # Absolute path literal that is NOT under /tmp/ (or bare /tmp).
        # Negative lookahead: `/` followed by not-`tmp`-followed-by-quote-or-slash.
        # This intentionally over-matches paths like `/tmpfile` (which look
        # like /tmp but aren't) — blocking those is a safe over-approximation.
        _abs_path_not_tmp_pat = r'''["']/(?!tmp[/"'])[a-zA-Z][^"']*'''
        if _re_scan.search(_recursive_primitive_pat, _active_code) and \
           _re_scan.search(_abs_path_not_tmp_pat, _active_code):
            return (
                "BLOCKED: recursive traversal restricted to /tmp/ ONLY on the cluster. "
                "Every other absolute path is a FUSE mount (walks hang for hours) or "
                "system dir (irrelevant to migration). "
                "If you cannot find a data path: use "
                "`make_note(\"DATA_UNAVAIL: <resource>\")` and submit best-effort code "
                "OR skip the cell. Do NOT exploratory-scan — the tool will keep refusing."
            )

        # Track any pip install commands
        for pkg in extract_pip_packages(code):
            _installed_packages.add(pkg)

        # Reject shadowing of reserved kernel globals (sc / spark / dbutils /
        # oidlUtils) BEFORE the code reaches the kernel. If Opus rebinds `sc`
        # here, AIDP's per-command prologue `sc.setJobGroup(...)` fails and
        # the kernel is unrecoverable. Returning a rejection message to Opus
        # lets it retry with a `_probe_`-prefixed name — cheap and safe.
        _shadow_msg = _detect_kernel_global_shadow(code)
        if _shadow_msg:
            return _shadow_msg

        if session:
            try:
                code = rewrite_oci_credential_reads(code)
                result = await session.execute(code, timeout=1200)
                output = _extract_output(result)
                # Detect AIDP compute cluster down — surface clearly so Opus doesn't
                # try to "fix" the cell code. The main execution loop handles reconnect.
                import re as _re
                if _re.search(r"Compute cluster [a-z0-9.\-:]+ is not running", output or "", _re.IGNORECASE):
                    return (
                        "[INFRA] AIDP compute cluster is paused — this is NOT a code error. "
                        "The main cell execution loop will reconnect automatically. "
                        "Do not modify the cell code. Skip this tool call and proceed with submit_code."
                    )
                return output
            except Exception as e:
                return f"Execution error: {str(e)[:300]}"
        else:
            return "No cluster session available"

    elif tool_name == "describe_table":
        table_name = tool_input.get("table_name", "")
        _first_part = table_name.split(".")[0].lower()
        if _first_part in _SKIP_MODULES:
            return f"'{table_name}' is a Python module, not a Spark table. Skip."
        # Don't DESCRIBE a table we already know is missing
        _desc_key = table_name.lower().strip()
        if _desc_key.startswith("default."):
            _desc_key = _desc_key[len("default."):]
        _cached = _table_lookup_cache.get(_desc_key)
        if _cached and "NOT FOUND" in _cached:
            return f"'{table_name}' was already confirmed NOT FOUND. Do not retry."
        if session:
            try:
                run = getattr(session, 'run_stateless', session.execute)
                result = await run(
                    f"spark.sql('DESCRIBE TABLE {table_name}').show(100, truncate=False)",
                    timeout=60)
                return _extract_output(result)
            except Exception as e:
                return f"Error describing {table_name}: {str(e)[:300]}"
        else:
            return "No cluster session available"

    elif tool_name == "list_schemas_and_tables":
        schema = tool_input.get("schema") or ""
        _schema_cache_key = f"_schema_:{schema.lower()}"
        if _schema_cache_key in _table_lookup_cache:
            return _table_lookup_cache[_schema_cache_key] + " (cached — already listed, do NOT retry)"
        if session:
            try:
                run = getattr(session, 'run_stateless', session.execute)
                if schema:
                    result = await run(
                        f"for t in spark.sql('SHOW TABLES IN {schema}').collect()[:50]: print(f'{{t.tableName}}')",
                        timeout=60)
                else:
                    result = await run(
                        "for s in spark.sql('SHOW SCHEMAS').collect()[:50]: print(f'{s[0]}')",
                        timeout=60)
                _output = _extract_output(result)
                _table_lookup_cache[_schema_cache_key] = _output
                return _output
            except Exception as e:
                return f"Error listing: {str(e)[:300]}"
        else:
            return "No cluster session available"

    elif tool_name == "read_notebook_source":
        path = tool_input.get("path", "")
        try:
            nb = _download_notebook_source(path)
            if nb is None:
                return f"Notebook not found: {path}"
            code_parts = []
            for cell in nb.get("cells", []):
                if cell.get("cell_type") == "code":
                    src = "".join(cell.get("source", []))
                    if src.strip():
                        code_parts.append(src)
            full_src = "\n\n# ---\n\n".join(code_parts)
            return full_src[:50000] if len(full_src) > 50000 else full_src
        except Exception as e:
            return f"Error reading notebook: {str(e)[:300]}"

    elif tool_name == "inspect_package_source":
        module = tool_input.get("module", "")
        name = tool_input.get("name", "")
        method = tool_input.get("method", "")
        if session:
            try:
                if method:
                    code = f"import inspect; from {module} import {name}; print(inspect.getsource(getattr({name}, '{method}')))"
                else:
                    code = f"import inspect; from {module} import {name}; print(inspect.getsource({name}))"
                run = getattr(session, 'run_stateless', session.execute)
                result = await run(code, timeout=30)
                return _extract_output(result)
            except Exception as e:
                return f"Error inspecting {module}.{name}: {str(e)[:200]}"
        else:
            return "No cluster session available"

    elif tool_name == "summarize_notebook":
        path = tool_input.get("path", "")
        # Check cache first (stored in cell_context if available)
        try:
            nb = _download_notebook_source(path)
            if nb is None:
                return f"Notebook not found: {path}"
            code_parts = []
            for cell in nb.get("cells", []):
                if cell.get("cell_type") == "code":
                    src = "".join(cell.get("source", []))
                    if src.strip():
                        code_parts.append(src[:200])
            full_src = "\n---\n".join(code_parts)

            # Bullet summary. Thinking is always on for this model family and
            # counts against max_tokens; effort `low` keeps it minimal.
            summary_resp = claude().messages.create(
                model=llm_config.MODEL,
                max_tokens=1500,
                timeout=60,
                output_config={"effort": SUMMARY_EFFORT},
                **llm_config.thinking_kwargs(),
                messages=[{"role": "user", "content": f"Summarize this notebook in 3-5 bullet points. What does it do? What functions/variables does it define? What does it import?\n\n{full_src[:5000]}"}]
            )
            _record_usage("summary", summary_resp.usage, effort=SUMMARY_EFFORT)
            summary = " ".join(b.text for b in (summary_resp.content or [])
                               if getattr(b, "type", "") == "text").strip() or "No summary"
            return summary[:1000]
        except Exception as e:
            return f"Error summarizing notebook: {str(e)[:200]}"

    elif tool_name == "make_note":
        raw_note = tool_input.get("note", "").strip()[:500]
        _current_cell_notes.append(raw_note)
        if log_fn:
            log_fn(f"[note] {raw_note[:120]}")
        return f"Note recorded ({len(raw_note)} chars). Total notes this cell: {len(_current_cell_notes)}."

    elif tool_name == "get_cell_history":
        from_idx = tool_input.get("from_index") or 0
        to_idx   = tool_input.get("to_index") or None
        n = len(_cell_history)
        if from_idx < 0:
            from_idx = max(0, n + from_idx)
        if to_idx is not None and to_idx < 0:
            to_idx = max(0, n + to_idx)
        slice_ = _cell_history[from_idx:to_idx]
        lines = [f"Cell history (indices {from_idx}-{from_idx+len(slice_)-1} of {n} total):"]
        for e in slice_:
            child_tag = "[child] " if e.get("is_child") else ""
            nb = e["notebook_path"]
            lines.append(
                f"[{e['index']}] {child_tag}{nb} cell {e['cell_idx']} | {e['status'].upper()} | "
                f"{e['summary'][:120]} | out: {e['output_preview'][:80]}"
            )
        return "\n".join(lines)

    elif tool_name == "get_history_entry":
        idx = int(tool_input.get("index", 0))
        if idx < 0 or idx >= len(_cell_history):
            return f"ERROR: index {idx} out of range (history has {len(_cell_history)} entries)"
        e = _cell_history[idx]
        return (
            f"History entry [{idx}]\n"
            f"Notebook: {e['notebook_path']}\n"
            f"Cell: {e['cell_idx']} | is_child: {e.get('is_child', False)} | status: {e['status']}\n"
            f"Summary: {e['summary']}\n"
            f"Output preview: {e['output_preview']}\n"
            f"Code:\n```python\n{e.get('replay_code', e['final_code'])}\n```"
        )

    elif tool_name == "repair_dependency":
        idx = int(tool_input.get("index", -1))
        if not _repair_dependency_fn or not 0 <= idx < len(_cell_history):
            return "ERROR: dependency repair unavailable or history index invalid"
        entry = _cell_history[idx]
        if entry.get("_task_key", "") != _current_task_key:
            return "ERROR: cannot repair a dependency from a different task"
        result = await _repair_dependency_fn(
            entry, tool_input.get("fixed_code", ""), tool_input.get("why", ""), session, log_fn)
        if result.startswith("OK:"):
            _dependency_repair_receipt.set(_dependency_repair_receipt.get() + 1)
        return result

    elif tool_name == "fixup_cell":
        start_idx = int(tool_input.get("start_index", 0))
        why       = tool_input.get("why", "")
        fixed     = tool_input.get("fixed_code", None)

        n = len(_cell_history)
        if start_idx < 0 or start_idx >= n:
            return f"ERROR: start_index {start_idx} out of range (history has {n} entries). Use get_cell_history first."
        if not _replay_cell_fn:
            return "ERROR: replay function not registered."

        target = _cell_history[start_idx]
        old_entries = [dict(e) for e in _cell_history[start_idx:]]
        replay_entries = list(old_entries)

        # History is job-wide, but a parent notebook's mutable source objects are
        # valid only while that task is being assembled. Replaying an earlier
        # task after its notebook was uploaded used to mutate kernel state while
        # silently leaving the artifact unchanged. Reject the whole rewind before
        # executing anything when it crosses a task boundary.
        foreign_tasks = sorted({
            str(e.get("_task_key"))
            for e in replay_entries
            if e.get("_task_key") and e.get("_task_key") != _current_task_key
        })
        if foreign_tasks:
            message = (
                "fixup_cell cannot rewind across completed task boundaries: "
                f"current task={_current_task_key!r}, history task(s)={foreign_tasks}. "
                "Re-run the affected task with --start-task so its output notebook "
                "can be rebuilt and persisted."
            )
            _record_fixup_persistence_failure(message)
            if log_fn:
                log_fn(f"[fixup_cell] ERROR: {message}")
            return f"ERROR: {message}"

        # A caller redispatches its owned child history. Replaying those entries
        # separately duplicates work; names cannot identify repeated invocations.
        covered = {identity for entry in replay_entries
                   for identity in entry.get("_replay_owned_entries", ())}
        # Match the existing persistence comparison: supplying unchanged code
        # is a replay request, not an edit that would be lost by ownership filtering.
        fixed_changes_source = bool(fixed) and (
            fixed.rstrip() != target.get("replay_code", target["final_code"])
            .split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
        )
        if any(entry.get("_replay_entry_id") in covered
               and (entry.get("_force_persist") or (i == 0 and fixed_changes_source))
               for i, entry in enumerate(replay_entries)):
            message = (
                "The requested child repair belongs to a caller invocation "
                "included in this rewind. Apply and durably save the child repair "
                "through its source owner (repair_dependency for a completed child), "
                "then rewind the caller. History was not changed and nothing was replayed."
            )
            _record_fixup_persistence_failure(message)
            return f"ERROR: {message}"

        if log_fn:
            child_tag = "[child] " if target.get("is_child") else ""
            log_fn(f"[fixup_cell] Rewinding to index {start_idx}: "
                   f"{child_tag}{os.path.basename(target['notebook_path'])} cell {target['cell_idx']}: "
                   f"{target['summary'][:100]}")
            log_fn(f"[fixup_cell] Why: {why[:200]}")
            log_fn(f"[fixup_cell] Pruning {len(old_entries)} history entries, replaying through execute+verify+fix loop")

        # Prune history from start_index onward — replay will rebuild it
        del _cell_history[start_idx:]

        # Inject fixed_code into the target entry before replay
        if fixed:
            replay_entries[0] = {
                **replay_entries[0],
                # Keep the pre-injection source for replay safety checks. Once
                # replay_code is replaced, a first-attempt PASS otherwise has
                # no way to detect a table/path swap or a neutered operation.
                "_pre_fixup_replay_code": target.get("replay_code", target["final_code"]),
                "final_code": fixed[:3000],
                "replay_code": fixed,
                "_force_persist": (
                    bool(target.get("_force_persist"))
                    or fixed_changes_source
                ),
            }

        # Replay each entry through execute+verify+fix loop
        errors, change_lines = [], []
        persistence_pending = False
        source_applied = False
        for i, entry in enumerate(replay_entries):
            if entry.get("_replay_entry_id") in covered:
                continue
            result = await _replay_cell_fn(entry, why, session, log_fn)
            persistence_pending |= result.get("persistence_pending", False)
            source_applied |= result.get("source_applied", False)
            old_e = old_entries[i]
            new_status = result["status"]
            old_code = old_e.get("replay_code", old_e.get("final_code", ""))
            changed = (old_code != result["final_code"] or
                       old_e.get("status", "") != new_status)
            change_tag = " [CHANGED]" if changed else ""
            nb = os.path.basename(entry["notebook_path"])
            change_lines.append(
                f"[{start_idx+i}] {nb} cell {entry['cell_idx']}: "
                f"{old_e.get('status','?').upper()} -> {new_status.upper()}{change_tag}"
            )
            if result["status"] != "ok":
                errors.append(f"idx {start_idx+i} cell {entry['cell_idx']}: {result.get('output','')[:500]}")
            if result.get("kind") == "FIXUP_PERSISTENCE":
                # Preserve the untouched tail instead of executing dependents of
                # a repair that could not be applied/persisted.
                _cell_history.extend(old_entries[i + 1:])
                break

        new_n = len(_cell_history)
        changes_summary = "\n".join(change_lines[-15:])
        if errors:
            return (f"PARTIAL: Rewound to index {start_idx}, processed {len(change_lines)}/{len(replay_entries)} replay entries. "
                    f"History now has {new_n} entries.\n"
                    f"Issues: {'; '.join(errors[:3])}\n"
                    f"Changed cells:\n{changes_summary}")
        if source_applied:
            _fixup_source_receipt.set(_fixup_source_receipt.get() + 1)
        return (f"OK: Rewound to index {start_idx}, replayed {len(change_lines)} cells — all passed. "
                f"History now has {new_n} entries.\n"
                f"Changed cells:\n{changes_summary}\n"
                + ("Child repair applied; durable save-back pending.\n" if persistence_pending else "")
                + "Now submit your fix for the current cell.")

    elif tool_name == "scan_sensitive_info":
        notebook_path = tool_input.get("notebook_path", "")
        return await _handle_scan_sensitive_info(notebook_path, session, log_fn)

    elif tool_name == "get_job_parameters":
        return await _handle_get_job_parameters(log_fn)

    elif tool_name == "get_tool_output":
        filename = tool_input.get("filename", "")
        return _handle_get_tool_output(filename, log_fn)

    return f"Unknown tool: {tool_name}"


def _unwrap_aidp_text(raw_text: str) -> str:
    """AIDP wraps output in JSON: [{"type":"TEXT_PLAIN","value":"actual text"}].
    Handles concatenated arrays: [{"value":"a"}][{"value":"b"}].
    Extract the actual text value. Returns raw text if not wrapped."""
    if not raw_text:
        return ""

    # Try single JSON array first
    try:
        items = json.loads(raw_text, strict=False)
        if isinstance(items, list):
            parts = []
            for item in items:
                if isinstance(item, dict) and "value" in item:
                    parts.append(item["value"])
            if parts:
                return "".join(parts)
    except (json.JSONDecodeError, TypeError):
        pass

    # Handle concatenated JSON arrays: ][
    if "][" in raw_text:
        extracted = []
        for chunk in re.split(r'\]\s*\[', raw_text):
            chunk = chunk.strip()
            if not chunk.startswith("["):
                chunk = "[" + chunk
            if not chunk.endswith("]"):
                chunk = chunk + "]"
            try:
                items = json.loads(chunk, strict=False)
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict) and "value" in item:
                            extracted.append(item["value"])
            except (json.JSONDecodeError, TypeError):
                pass
        if extracted:
            # Deduplicate consecutive identical entries (AIDP sends twice)
            deduped = []
            for e in extracted:
                if not deduped or e != deduped[-1]:
                    deduped.append(e)
            return "".join(deduped)

    return raw_text  # Return as-is if not wrapped


def _extract_output(result: dict) -> str:
    """Extract text output from AIDP session execution result.
    Handles the AIDP JSON-wrapped format."""
    outputs = result.get("outputs", [])
    text = ""
    for o in outputs:
        if o.get("type") == "stream":
            raw = o.get("text", "")
            text += _unwrap_aidp_text(raw)
        elif o.get("type") == "execute_result":
            data = o.get("data", {})
            raw = data.get("text/plain", "")
            text += _unwrap_aidp_text(raw)
        elif o.get("type") == "error":
            text += f"ERROR: {o.get('ename', '')}: {o.get('evalue', '')}\n"
            for tb in o.get("traceback", [])[:3]:
                import re as _re
                clean = _re.sub(r'\x1b\[[0-9;]*m', '', str(tb))
                text += f"  {clean}\n"
    # Deduplicate (AIDP sometimes sends output twice)
    if text and len(text) > 20:
        half = len(text) // 2
        if text[:half] == text[half:]:
            text = text[:half]
    return text[:50000] if text else "(no output)"


async def _create_message_with_529_retry(loop, create_fn, log_fn=None,
                                          max_retries: int = 5,
                                          grammar_max_retries: int = 3,
                                          rate_limit_max_retries: int = 5,
                                          conn_max_retries: int = 6,
                                          invalid_data_max_retries: int = 3):
    """Run an Anthropic messages.create call with retries on transient errors.

    create_fn: a no-arg callable that synchronously invokes claude().messages.create(...)
               and returns the response object.
    log_fn:    optional callback(msg: str) for retry-progress logging.
    max_retries: 529-Overloaded retry ATTEMPTS after the initial call (default 5,
                so up to 6 total tries with backoff ~1s, 2s, 4s, 8s, 16s).
    grammar_max_retries: retry ATTEMPTS for "Grammar compilation timed out" (default 3).
    rate_limit_max_retries: retry ATTEMPTS for 429 rate_limit_error (default 5).
                Rate limits are minute-windowed so waits start at 30s not 1s.
    conn_max_retries: retry ATTEMPTS for APIConnectionError / APITimeoutError (default 6).
                These are transport-level blips (laptop wifi/DNS hiccup, dropped TLS
                mid-call) that the SDK client already retries internally; this outer
                layer is a second, more patient net (waits 5/10/20/40/80/120s) so a
                blip that outlasts the SDK's budget doesn't abort an entire notebook
                task mid-migration. The failure surfaces as a bare "Connection error."
                with no HTTP status, so match on the SDK exception type, not a string.

    Behavior:
    - On success, returns the response.
    - On Anthropic 529 ("Overloaded"), waits with exponential backoff + jitter and retries.
    - On APIConnectionError / APITimeoutError (transient transport failure), waits with
      exponential backoff (capped at 120s) and retries up to conn_max_retries.
    - On Anthropic 429 ("rate_limit_error", org input-token/minute cap): honors the
      response's `retry-after` header when present; else waits 30s * 2^attempt (30/60/120/240/480s).
      Without this handler, an org-cap 429 in the tool-loop was silently caught by the
      task-level except and the migration would move to the next task with no RESULT line
      — task looked completed but was actually abandoned mid-way. Retrying honors
      Anthropic's guidance and lets the migration recover instead of skipping cells.
    - On "Grammar compilation timed out" (a 400 invalid_request_error): the API timed out
      SERVER-SIDE compiling the tool-schema grammar — transient (the same schemas compile
      fine on a retry; verified: sibling tasks using the identical tool set succeed). Without
      a retry, ONE such timeout aborts an entire notebook task (0 cells migrated). Retry with
      backoff up to grammar_max_retries before giving up.
    - On any OTHER 400 invalid_request_error with a generic/opaque message — notably the
      bare "Invalid request data" (no field path, unlike a real payload bug which names the
      offending block) — treat it like the grammar timeout: a transient SERVER-SIDE
      validation blip. Retry with backoff up to invalid_data_max_retries, THEN raise.
      Bounded so a genuinely non-transient bad request still surfaces (just after a few
      retries). EXCLUDES "prompt is too long" (also a 400 invalid_request_error) so the
      caller's context-overflow emergency-trim still runs.
    - On "prompt is too long" / any remaining error, re-raises immediately so the caller's
      existing error-handling logic still runs.
    - If a retried error persists past its budget, raises RuntimeError chained from the last one.
    """
    import asyncio
    import random
    attempt_529 = 0
    attempt_grammar = 0
    attempt_429 = 0
    attempt_conn = 0
    attempt_invalid = 0
    while True:
        try:
            return await loop.run_in_executor(None, create_fn)
        except Exception as exc:
            exc_str = str(exc)
            # Match Anthropic's specific error markers (avoid matching incidental
            # substrings in user error messages). Anything not matched propagates so
            # the caller (e.g. context-overflow handler) can react appropriately.
            is_529 = ("Error code: 529" in exc_str) or ("overloaded_error" in exc_str)
            is_429 = ("Error code: 429" in exc_str) or ("rate_limit_error" in exc_str)
            is_grammar = "Grammar compilation timed out" in exc_str
            # Transport-level failure (no HTTP status): dropped connection / timeout.
            # Match on the SDK exception type — the message is a bare "Connection error."
            is_conn = isinstance(exc, (anthropic.APIConnectionError,
                                       anthropic.APITimeoutError))
            # Generic/opaque 400 invalid_request_error (e.g. the bare "Invalid request
            # data" with no field path) — treat as a transient SERVER-SIDE validation blip
            # and retry (bounded). MUST exclude "prompt is too long" (also a 400
            # invalid_request_error) so it still propagates to the caller's context-overflow
            # emergency-trim, and exclude the grammar-timeout case (its own branch above).
            # NARROWED (review #49): match ONLY the bare opaque "Invalid request
            # data" server-side blip (observed live to succeed on retry), NOT every
            # 400/invalid_request_error. A real payload bug names the offending
            # block (a different message) and must propagate immediately, not burn
            # 3 backoff sleeps. "prompt is too long" already won't match this, but
            # keep the guards defensively.
            is_invalid_data = (
                "Invalid request data" in exc_str
                and not is_grammar
                and "prompt is too long" not in exc_str
                and "prompt_too_long" not in exc_str
            )
            if is_529:
                if attempt_529 >= max_retries:
                    raise RuntimeError(
                        f"Anthropic 529 Overloaded persisted after {max_retries} retries. "
                        f"Last error: {exc}"
                    ) from exc
                wait = (2 ** attempt_529) + random.random()
                label = f"Anthropic 529 Overloaded — retry {attempt_529 + 1}/{max_retries} in {wait:.1f}s"
                attempt_529 += 1
            elif is_429:
                if attempt_429 >= rate_limit_max_retries:
                    raise RuntimeError(
                        f"Anthropic 429 rate_limit_error persisted after "
                        f"{rate_limit_max_retries} retries. Last error: {exc}"
                    ) from exc
                # Prefer server-provided retry-after when SDK surfaced it.
                retry_after = None
                try:
                    hdrs = getattr(getattr(exc, "response", None), "headers", None)
                    if hdrs is not None:
                        raw = hdrs.get("retry-after") or hdrs.get("Retry-After")
                        if raw is not None:
                            retry_after = float(raw)
                except Exception:
                    retry_after = None
                if retry_after and retry_after > 0:
                    wait = retry_after + random.random()
                else:
                    wait = 30.0 * (2 ** attempt_429) + random.random()
                label = (f"Anthropic 429 rate_limit_error — "
                         f"retry {attempt_429 + 1}/{rate_limit_max_retries} in {wait:.1f}s")
                attempt_429 += 1
            elif is_grammar:
                if attempt_grammar >= grammar_max_retries:
                    raise RuntimeError(
                        f"Anthropic 'Grammar compilation timed out' persisted after "
                        f"{grammar_max_retries} retries. Last error: {exc}"
                    ) from exc
                wait = (2 ** attempt_grammar) + random.random()
                label = (f"Anthropic Grammar compilation timed out (transient) — "
                         f"retry {attempt_grammar + 1}/{grammar_max_retries} in {wait:.1f}s")
                attempt_grammar += 1
            elif is_conn:
                if attempt_conn >= conn_max_retries:
                    raise RuntimeError(
                        f"Anthropic connection error persisted after {conn_max_retries} "
                        f"retries (transport-level; likely a network blip). Last error: {exc}"
                    ) from exc
                wait = min(120.0, 5.0 * (2 ** attempt_conn)) + random.random()
                label = (f"Anthropic connection error ({type(exc).__name__}) — "
                         f"retry {attempt_conn + 1}/{conn_max_retries} in {wait:.1f}s")
                attempt_conn += 1
            elif is_invalid_data:
                if attempt_invalid >= invalid_data_max_retries:
                    raise RuntimeError(
                        f"Anthropic 400 invalid_request_error persisted after "
                        f"{invalid_data_max_retries} retries — treating as non-transient "
                        f"(malformed request, not a server blip). Last error: {exc}"
                    ) from exc
                wait = (2 ** attempt_invalid) + random.random()
                label = (f"Anthropic 400 invalid_request_error (transient server blip?) — "
                         f"retry {attempt_invalid + 1}/{invalid_data_max_retries} in {wait:.1f}s")
                attempt_invalid += 1
            else:
                raise
            if log_fn:
                log_fn(label)
            else:
                print(f"  [tools] {label}")
            await asyncio.sleep(wait)


async def call_opus_with_tools(system: str, user_content: str, session=None,
                                # Fix 7z (2026-07-29): default lowered from 20 → 5 to
                                # align with MAX_TOOL_ROUNDS_FIX/MIGRATE. All known
                                # callers now pass an explicit value; the default
                                # protects against future callers that forget.
                                max_tokens: int = 128000, max_tool_rounds: int = 5,
                                log_fn=None, code_validator=None,
                                route: str = None, effort: str = None,
                                exclude_tools: Tuple[str, ...] = (),
                                require_end_turn: bool = False,
                                repair_caller: str = None) -> Tuple[str, dict]:
    """Call Claude Opus 5 with tool use for path exploration.
    Handles the tool-use loop: Opus can call tools, we execute them on cluster, send results back.
    Returns (final_text, usage_totals).

    log_fn: optional callback(msg: str) for logging tool calls. If None, prints to stdout.
    exclude_tools: tool names left out of this call's tool set. The output-check
    eval passes ("submit_code", "submit_data_recovery_plan"): a verdict never
    carries code or a plan, and either would otherwise come back as the reply
    text the verdict is parsed from.
    require_end_turn: return text only from a reply the model finished
    (stop_reason "end_turn"). A reply cut off at max_tokens, a refusal, or text
    next to a tool call that never got its result returns "" instead. The
    output-check eval passes True, so its verdict is never read from a reply
    the model did not finish.
    repair_caller: opt-in completed-dependency fix handoff. Retains this
    conversation, reserves three retrieval plus three submission turns, and raises
    UnresolvedRepairError if the normalized caller is unchanged without a saved
    dependency repair or a source-owner-applied fixup. The latter still requires
    the normal finalizer's durable save. Other routes retain their delivery contract.
    Async because tool handlers need to execute code on AIDP cluster."""
    import asyncio
    import uuid
    from context_compactor import ContextCompactor, strip_thinking_blocks

    def _log(msg):
        if log_fn:
            log_fn(msg)
        else:
            print(f"  [tools] {msg}")

    # Set up context compactor for this call. Store it as a module global so
    # _handle_get_tool_output can reference it. Each call_opus_with_tools
    # invocation gets its own compactor; sequential calls (eval then fix)
    # each set this, so only the most-recent call's files are directly
    # accessible via get_tool_output.
    call_id = str(uuid.uuid4())[:8]
    compactor = ContextCompactor(
        call_id=call_id, log_fn=_log,
        record_usage=lambda u: _record_usage("compaction", u, log_fn=_log))
    global _current_compactor, _compactor_history
    _current_compactor = compactor
    _compactor_history.append(compactor)

    # Prepend job execution history so Opus has context before reasoning
    history_ctx = _format_history_context()
    if history_ctx:
        user_content = history_ctx + "\n\n" + user_content

    messages = [{"role": "user", "content": user_content}]
    total_usage = {"input": 0, "output": 0}
    # Ledger route label (migrate / eval / fix / ...). Return shape of
    # total_usage is unchanged; the full meters go to USAGE_LEDGER.
    route = route or _infer_route(system)
    # Optional per-route effort (effort-ladder knobs above). Empty when unset so
    # the request is unchanged.
    _effort_kw = {"output_config": {"effort": effort}} if effort else {}

    # Anthropic prompt caching — CELL_MIGRATE_PROMPT is ~77k chars (~19k tokens),
    # FIX_PROMPT ~66k chars, OCI_PATH_TOOLS ~19k chars — all far above Opus 5's
    # 512-token cache minimum. Output is byte-identical to non-cached (the API
    # only reuses internal KV; no model behaviour change).
    #
    # Render order is tools -> system -> messages, so ONE breakpoint on the
    # system block caches tools + system together; the former per-tool marker
    # was redundant (and a 5-minute entry may not precede a 1-hour one).
    #
    # TTL 1h: cell execution between two Opus calls regularly exceeds 5 minutes
    # (CELL_EXECUTE_TIMEOUT is hours), which expired the 5-minute entry and
    # re-wrote the ~20k-token prefix at 1.25x on the next call. A 1-hour entry
    # costs 2x once and is refreshed for free by every read.
    #
    # Top-level `cache_control` on the request (see the create() calls below)
    # additionally caches the growing messages tail, so round r of a tool loop
    # reads rounds 1..r-1 at 0.1x instead of re-billing them at full price.
    _cached_system = [{
        "type": "text",
        "text": system,
        "cache_control": {"type": "ephemeral", "ttl": "1h"},
    }]
    # A narrowed tool set renders a different prefix; a call that narrows it
    # also sends its own system prompt, so no cache entry is shared anyway.
    _cached_tools = [t for t in OCI_PATH_TOOLS if t["name"] not in exclude_tools]
    _offered_tools = {t["name"] for t in _cached_tools}
    _offers_submit_code = "submit_code" in _offered_tools

    loop = asyncio.get_event_loop()

    def _record_delivery_response(reply, completed=None):
        # Preserve the conversation, but never execute investigation tools during
        # final delivery. Every tool_use must still receive exactly one result.
        # Thinking is stripped here: the delivery retry narrows the tool set, so
        # it would be stripped on send anyway, and a thinking-only reply would
        # then go out as an empty (rejected) assistant turn.
        kept = strip_thinking_blocks([{"role": "assistant", "content": reply.content}])[0]
        if kept["content"]:
            messages.append(kept)
        pending = [
            {"type": "tool_result", "tool_use_id": block.id,
             "content": (completed or {}).get(block.id,
                 "Final code was not accepted; return the complete corrected cell."),
             "is_error": block.id not in (completed or {})}
            for block in reply.content if block.type == "tool_use"
        ]
        if pending:
            messages.append({"role": "user", "content": pending})

    async def _retry_code_delivery():
        # Incident: exhausted exploration returned empty code, then the caller
        # silently kept the incompatible source. Retry delivery, not exploration.
        for attempt in range(1, 3):
            _log(f"Final-code delivery retry {attempt}/2 (no investigation tools)")
            messages.append({"role": "user", "content": (
                "Your previous response contained no usable final code. Using the "
                "existing investigation and test results, submit the complete "
                "corrected cell with submit_code. Do not investigate again or "
                "return explanations. Preserve the cell's language and business logic."
            )})
            # llm_config.MAX_TOKENS is only a FLOOR here — this loop's callers pass
            # large per-call budgets (128000/64000) for big migration outputs, so the
            # fixed MAX_TOKENS constant cannot lower the main request. (MAX_TOKENS is a
            # fixed internal constant, not a YAML knob — PR#146.) A per-model output cap
            # belongs in a future provider adapter, not here.
            reply = await _create_message_with_529_retry(loop, lambda: claude().messages.create(
                model=llm_config.MODEL, max_tokens=max(max_tokens, llm_config.MAX_TOKENS), timeout=600,
                # The tool set changes (submit_code only), which invalidates the
                # replayed thinking blocks; tool_use / tool_result stay paired.
                system=_cached_system, messages=strip_thinking_blocks(messages),
                tools=[t for t in OCI_PATH_TOOLS if t["name"] == "submit_code"],
                **llm_config.thinking_kwargs(),
                **_effort_kw,
            ), log_fn=_log)
            total_usage["input"] += reply.usage.input_tokens
            total_usage["output"] += reply.usage.output_tokens
            _record_usage(f"{route}_delivery", reply.usage, attempt, _log, effort=effort)
            candidates = [block.input.get("code", "") for block in reply.content
                          if block.type == "tool_use" and block.name == "submit_code"]
            candidates.append("\n".join(block.text for block in reply.content if block.type == "text"))
            for candidate in candidates:
                if isinstance(candidate, str):
                    candidate = rewrite_oci_credential_reads(candidate)
                    if code_validator(candidate):
                        return candidate, total_usage
            _log(f"Final-code delivery rejected: stop_reason={reply.stop_reason}; "
                 f"block_types={[block.type for block in reply.content]}")
            _record_delivery_response(reply)
        raise RuntimeError(
            "Conversion failed: no usable final code after 2 delivery retries; "
            "original source must not be marked migrated."
        )

    # The fix handoff shares this conversation and compactor. Three bounded
    # retrieval turns precede three submission turns; retrieving the last source
    # must never remove the model's opportunity to save its repair.
    retrieval_turns, submission_turns = 3, 3
    reserve_turns = retrieval_turns + submission_turns
    repair_receipt_before = dependency_repair_receipt()
    source_receipt_before = _fixup_source_receipt.get()
    repair_stage_start = None
    repair_feedback = (
        "Continue from the investigation and tool results above. Inspect the implicated "
        "helper with get_history_entry/get_tool_output, then use repair_dependency "
        "with its actual history index, full fixed_code and why. Wait for its OK. "
        "Rebuild affected intervening parent results with fixup_cell before submitting "
        "the caller; redefining a helper does not rebuild existing DataFrames. "
        "For an active parent repair, use fixup_cell with fixed_code and wait for its "
        "source-owner acknowledgement; normal final save must still succeed. "
        "run_on_cluster experiments and notes are not saved repairs. Preserve inputs, "
        "dates, source paths and business operations. If no safe repair is possible, "
        "make_note with the actual blocker. There are three retrieval turns followed "
        "by three repair/submission turns; an unchanged caller without acknowledged "
        "source-backed repair progress "
        "will be reported UNRESOLVED."
    )

    def _repair_delivery_feedback():
        progress = ("A completed dependency repair was saved; its caller still needs validation. "
                    if dependency_repair_receipt() != repair_receipt_before else
                    "An upstream repair was validated and applied to its notebook source owner; "
                    "final artifact save is still required. Validate the caller against rebuilt results. "
                    if _fixup_source_receipt.get() != source_receipt_before else
                    "The caller is still failing and no completed dependency repair was saved. ")
        return progress + repair_feedback

    def _repair_candidate(candidate):
        if repair_caller is None:
            return candidate
        normalized = _normalize_fix_response(candidate, repair_caller, log_fn)
        if (normalized.strip() == repair_caller.strip()
                and dependency_repair_receipt() == repair_receipt_before
                and _fixup_source_receipt.get() == source_receipt_before):
            return None
        # Compare the normalized candidate, but preserve the delivery contract.
        # call_fix normalizes the returned reply once. Normalizing the extracted
        # source again can discard valid Python such as `it = ...` as prose.
        return candidate

    def _continue_repair(reply, round_num, record_response=True):
        nonlocal repair_stage_start
        if repair_stage_start is None:
            repair_stage_start = round_num + 1
            _log("[fix] Unchanged caller without dependency save; continuing AI repair delivery")
        if record_response:
            _record_delivery_response(reply, completed_repairs)
        messages.append({"role": "user", "content": _repair_delivery_feedback()})

    # Fix 7z (2026-07-29): STRUCTURAL cap on run_on_cluster calls per Opus call.
    # The FIX_PROMPT rule "no more than 3 run_on_cluster calls per error" was
    # advisory only — Opus routinely violated it, leading to runaway
    # investigation and the ExampleVendor /Volumes hang. This counter (per
    # call_opus_with_tools invocation) enforces the cap tool-side. See the
    # MAX_RUN_ON_CLUSTER_PER_CALL constant block at the top of this file for
    # rationale.
    _run_on_cluster_count = 0

    normal_rounds = max_tool_rounds + 1  # preserve the existing investigation cap
    for round_num in range(normal_rounds + (reserve_turns if repair_caller is not None else 0)):
        if repair_caller is not None:
            if repair_stage_start is None and round_num == normal_rounds:
                repair_stage_start = round_num
                _log("[fix] Investigation cap reached; retaining conversation for bounded repair delivery")
                messages.append({"role": "user", "content": _repair_delivery_feedback()})
            if repair_stage_start is not None:
                stage = round_num - repair_stage_start
                if stage >= reserve_turns:
                    break
                if stage == retrieval_turns:
                    _log("[fix] Retrieval reserve exhausted; retaining repair, replay and submission tools")
                    # Retain repair + replay as well as caller submission. Existing
                    # investigation results remain visible, with thinking stripped
                    # because Anthropic binds it to the previous tool set.
                    delivery_names = {"repair_dependency", "fixup_cell", "submit_code", "make_note"}
                    _cached_tools = [t for t in _cached_tools if t["name"] in delivery_names]
                    _offered_tools = {t["name"] for t in _cached_tools}
                    messages = strip_thinking_blocks(messages)
                    messages.append({"role": "user", "content":
                        "Retrieval budget is exhausted. Three tool turns remain to save "
                        "the repair, rebuild affected results and submit the caller, "
                        "or record the actual blocker if source is insufficient. No further investigation."})
        # Apply context compaction before each API call
        messages = compactor.maybe_compact(messages, system)

        # Run the sync API call in a thread to not block the event loop
        try:
            response = await _create_message_with_529_retry(loop, lambda: claude().messages.create(
                model=llm_config.MODEL,
                max_tokens=max_tokens,
                timeout=600,
                system=_cached_system,
                messages=messages,
                tools=_cached_tools,
                **llm_config.thinking_kwargs(),
                cache_control={"type": "ephemeral"},  # automatic caching of the messages tail
                **_effort_kw,
            ), log_fn=_log)
        except Exception as _api_exc:
            _exc_str = str(_api_exc)
            if "prompt is too long" in _exc_str or "prompt_too_long" in _exc_str:
                _log(f"WARNING: Context overflow even after compaction. Falling back to emergency trim.")
                if len(messages) > 3:
                    # Same splice as Tier 2: the kept turns' thinking is bound
                    # to the dropped history and would be rejected.
                    messages = strip_thinking_blocks(messages[:1] + messages[-2:])
                    try:
                        response = await _create_message_with_529_retry(loop, lambda: claude().messages.create(
                            model=llm_config.MODEL,
                            max_tokens=max_tokens,
                            timeout=600,
                            system=_cached_system,
                            messages=messages,
                            tools=_cached_tools,
                            **llm_config.thinking_kwargs(),
                            cache_control={"type": "ephemeral"},
                            **_effort_kw,
                        ), log_fn=_log)
                    except Exception as _retry_exc:
                        raise RuntimeError(
                            f"Context overflow: prompt too long even after emergency trim "
                            f"({len(messages)} msgs). Aborting this cell attempt so the fix "
                            f"loop can retry with a fresh context. Original error: {_api_exc}"
                        ) from _retry_exc
                else:
                    raise RuntimeError(
                        f"Context overflow: prompt too long with only {len(messages)} messages. "
                        f"Cannot trim further. Aborting this cell attempt. Error: {_api_exc}"
                    ) from _api_exc
            else:
                raise

        total_usage["input"] += response.usage.input_tokens
        total_usage["output"] += response.usage.output_tokens
        # All four meters + thinking, every round (usage.input_tokens alone is
        # the uncached remainder — see USAGE_LEDGER).
        _record_usage(route, response.usage, round_num + 1, _log, effort=effort)

        # Log thinking blocks (Opus 5 returns empty thinking text unless display is requested)
        for block in response.content:
            if block.type == "thinking":
                thinking_text = block.thinking or ""
                # Log up to 2000 chars of thinking, line by line
                for line in thinking_text[:2000].split("\n"):
                    if line.strip():
                        _log(f"[thinking] {line.strip()}")
                if len(thinking_text) > 2000:
                    _log(f"[thinking] ... ({len(thinking_text)} chars total)")

        # Check if response has tool use
        has_tool_use = any(block.type == "tool_use" for block in response.content)

        # A model may submit the unchanged caller alongside the upstream patch.
        # Persist that patch before either final-code return path acknowledges it.
        repairs = [b for name in ("repair_dependency", "fixup_cell", "make_note")
                   for b in response.content
                   if b.type == "tool_use" and b.name == name
                   and (name == "repair_dependency" or repair_caller is not None)
                   and b.name in _offered_tools]
        completed_repairs = {}
        paired_repair_failed = False
        if repairs and "submit_code" in _offered_tools and any(b.type == "tool_use" and b.name == "submit_code"
                           for b in response.content):
            for block in repairs:
                fixup_event_before, _ = _fixup_persistence_event()
                result = await _handle_tool_call(
                    block.name, block.input, session=session, log_fn=log_fn)
                fixup_event_after, fixup_error = _fixup_persistence_event()
                if fixup_event_after != fixup_event_before:
                    raise FixupPersistenceError(fixup_error)
                # An invalid index (or another recoverable tool refusal) is
                # feedback for the model. It has not attempted a save and must
                # not be reported as a persistence failure. Defer the caller
                # submission so the tool result can guide the next turn.
                if block.name != "make_note" and not result.startswith("OK:"):
                    paired_repair_failed = True
                completed_repairs[block.id] = result
                _log(result)

        if code_validator is not None and not paired_repair_failed:
            submissions = [block.input.get("code", "") for block in response.content
                           if block.type == "tool_use" and block.name == "submit_code"]
            if submissions or not has_tool_use or response.stop_reason == "end_turn":
                candidates = submissions or ["\n".join(
                    block.text for block in response.content if block.type == "text")]
                for candidate in candidates:
                    if isinstance(candidate, str):
                        candidate = rewrite_oci_credential_reads(candidate)
                        if code_validator(candidate):
                            return candidate, total_usage
                _log(f"Final-code delivery rejected: stop_reason={response.stop_reason}; "
                     f"block_types={[block.type for block in response.content]}")
                _record_delivery_response(response)
                return await _retry_code_delivery()

        if not has_tool_use or (response.stop_reason == "end_turn" and not paired_repair_failed):
            # Extract text from response (skip thinking blocks)
            text_parts = [block.text for block in response.content if block.type == "text"]
            if round_num > 0:
                _log(f"Tool loop done after {round_num} round(s), {total_usage['input']+total_usage['output']} tokens")
            if require_end_turn and response.stop_reason != "end_turn":
                _log(f"Reply not finished (stop_reason={response.stop_reason}); its text is not returned")
                return "", total_usage
            candidate = _repair_candidate("\n".join(text_parts))
            if candidate is None:
                _continue_repair(response, round_num)
                continue
            return candidate, total_usage

        # Check if submit_code was called - extract code directly
        rejected_caller = paired_repair_failed
        for block in response.content:
            if block.type == "tool_use" and block.name == "submit_code" and _offers_submit_code:
                if paired_repair_failed:
                    break
                code = block.input.get("code", "")
                if code.strip():
                    _log(f"submit_code: {len(code)} chars")
                    if round_num > 0:
                        _log(f"Tool loop done after {round_num+1} round(s), {total_usage['input']+total_usage['output']} tokens")
                    candidate = _repair_candidate(rewrite_oci_credential_reads(code))
                    if candidate is None:
                        rejected_caller = True
                        break
                    return candidate, total_usage
        # A rejected caller may share a reply with source-retrieval tools.
        # Process their results below; rejecting submission must not discard
        # the investigation the model needs to deliver its dependency repair.

        # Check if submit_data_recovery_plan was called — return the plan as
        # a JSON-encoded string (caller will json.loads it). Parallel handling
        # to submit_code: a "submit" tool signals "Opus is done, here's the
        # answer."
        for block in response.content:
            if (not rejected_caller and block.type == "tool_use"
                    and block.name == "submit_data_recovery_plan"
                    and "submit_data_recovery_plan" in _offered_tools):
                plan = block.input
                _log(f"submit_data_recovery_plan: table={plan.get('table')!r} date_column={plan.get('date_column')!r} overrides={len(plan.get('overrides', []))}")
                if round_num > 0:
                    _log(f"Tool loop done after {round_num+1} round(s), {total_usage['input']+total_usage['output']} tokens")
                return json.dumps(plan), total_usage

        # Handle other tool calls
        messages.append({"role": "assistant", "content": response.content})

        # Process each tool use block (async - can execute on cluster)
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                if block.name not in _offered_tools:
                    # A tool this call left out (exclude_tools) is never run, and
                    # its input never becomes the reply.
                    tool_results.append({
                        "type": "tool_result", "tool_use_id": block.id, "is_error": True,
                        "content": f"Tool {block.name} is not available in this call."})
                    continue
                if block.name == "submit_code":
                    # Already handled above, but provide a result to continue the conversation
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": (("Caller submission rejected because a paired repair did not "
                                     "complete. Read its tool error, correct it and submit again."
                                     if paired_repair_failed else
                                     "Caller unchanged without a saved dependency repair; "
                                     "submission not accepted.") if rejected_caller else
                                    "Code submitted successfully"),
                        "is_error": rejected_caller,
                    })
                    continue

                tool_input_str = json.dumps(block.input)
                if len(tool_input_str) > 120:
                    tool_input_str = tool_input_str[:120] + "..."
                _log(f"Round {round_num+1}: {block.name}({tool_input_str})")

                # Fix 7z (2026-07-29): enforce MAX_RUN_ON_CLUSTER_PER_CALL BEFORE dispatch.
                # After the cap, further run_on_cluster calls are refused with a clear
                # next-step message. Opus's next round reads the refusal and pivots to
                # submit_code / make_note. See constant declaration for full rationale.
                if block.name == "run_on_cluster":
                    _run_on_cluster_count += 1
                    if _run_on_cluster_count > MAX_RUN_ON_CLUSTER_PER_CALL:
                        # Without submit_code (the eval) the next step is the
                        # reply itself; never point the model at an absent tool.
                        _next_step = (
                            "You MUST now do ONE of:\n"
                            "  (a) submit_code with your best fix based on what you already know\n"
                            "  (b) make_note('DATA_UNAVAIL: <resource>') + submit best-effort code\n"
                            "  (c) make_note('CANNOT_FIX: <why>') + submit the original code unchanged\n"
                        ) if _offers_submit_code else (
                            "Reply now with your final answer in the format your instructions "
                            "specify, based on what you already know (make_note first if a "
                            "resource is unavailable).\n"
                        )
                        _refusal = (
                            f"REFUSED: run_on_cluster budget exhausted "
                            f"(cap {MAX_RUN_ON_CLUSTER_PER_CALL} calls/Opus-turn, "
                            f"you attempted call #{_run_on_cluster_count}). "
                            "Investigation phase is over. " + _next_step +
                            "Do NOT keep investigating. This is a hard tool-side cap; further "
                            "run_on_cluster calls in this turn will also be refused."
                        )
                        _log(f"  -> REFUSED: run_on_cluster budget exhausted (call #{_run_on_cluster_count} > cap {MAX_RUN_ON_CLUSTER_PER_CALL})")
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": _refusal
                        })
                        continue

                import time as _time
                t0 = _time.time()
                fixup_event_before, _ = _fixup_persistence_event()
                result_str = (completed_repairs[block.id] if block.id in completed_repairs else
                              await _handle_tool_call(block.name, block.input, session=session, log_fn=log_fn))
                fixup_event_after, fixup_error = _fixup_persistence_event()
                if block.name in ("fixup_cell", "repair_dependency") and fixup_event_after != fixup_event_before:
                    # Only this tool call's failure stops its model turn. An
                    # earlier task finding still fails the final task report,
                    # but must not be attributed to a later successful repair.
                    raise FixupPersistenceError(fixup_error)
                elapsed = _time.time() - t0

                # Explicit retrieval must deliver the saved source, not another
                # truncated preview pointing to a new get_tool_output file.
                if block.name != "get_tool_output":
                    result_str = compactor.save_and_truncate(block.name, result_str)

                result_preview = result_str[:150].replace("\n", " ") if result_str else "(empty)"
                _log(f"  -> {result_preview} ({elapsed:.1f}s)")

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str,
                    **({"is_error": True} if block.id in completed_repairs
                       and block.name != "make_note"
                       and not completed_repairs[block.id].startswith("OK:") else {}),
                })

        messages.append({"role": "user", "content": tool_results})
        if rejected_caller:
            _continue_repair(response, round_num, record_response=False)

    if repair_caller is not None:
        raise UnresolvedRepairError(
            "UNRESOLVED: bounded repair delivery ended without a changed caller "
            "or caller submission after an acknowledged source-backed upstream repair. "
            "The failing caller was not re-executed.")

    if code_validator is not None:
        _log(f"WARNING: Exhausted {max_tool_rounds} tool rounds; requesting final code")
        return await _retry_code_delivery()

    # Non-code callers (evaluation / recovery plans) retain their text contract.
    # Exhausted tool rounds - force a final text response without tools
    _log(f"WARNING: Exhausted {max_tool_rounds} tool rounds, forcing final answer")

    # First check if the last response had any text. That response called a
    # tool, so its text was written before the tool's result: not a finished
    # answer under require_end_turn.
    text_parts = [block.text for block in response.content if block.type == "text"]
    if text_parts and any(t.strip() for t in text_parts) and not require_end_turn:
        return "\n".join(text_parts), total_usage

    # One more call WITHOUT tools to force a text answer. The loop already
    # appended the last turn and the results of its tool calls; appending them
    # again sent duplicate tool_use ids, which the API rejects. Ask for the
    # answer in a user turn of its own (the API merges it into the tool-results
    # turn).
    messages.append({"role": "user", "content": (
        "Tool round limit reached: produce your final answer now, without tools.")})

    # The Opus-5-family models (the configurable llm_config.MODEL default,
    # claude-opus-5 / claude-opus-5-5 — both validated 2026-09-23) require
    # thinking.type=adaptive (effort controls budget); the legacy
    # {"type":"enabled","budget_tokens":N} format returns 400 on this family.
    # Sampling params (temperature/top_p/top_k) are not supported on 4.7+ — omit them.
    _final_max = max(max_tokens, llm_config.MAX_TOKENS)
    final_response = await _create_message_with_529_retry(loop, lambda: claude().messages.create(
        model=llm_config.MODEL,
        max_tokens=_final_max,
        timeout=600,
        system=_cached_system,
        # No tools: the changed tool set invalidates replayed thinking blocks.
        messages=strip_thinking_blocks(messages),
        **llm_config.thinking_kwargs(),
        **_effort_kw,
        # No tools - forces text response
    ), log_fn=_log)
    total_usage["input"] += final_response.usage.input_tokens
    total_usage["output"] += final_response.usage.output_tokens
    _record_usage(f"{route}_final", final_response.usage, max_tool_rounds + 2, _log,
                  effort=effort)
    if require_end_turn and final_response.stop_reason != "end_turn":
        _log(f"Forced final reply not finished (stop_reason={final_response.stop_reason}); "
             f"its text is not returned")
        return "", total_usage
    text_parts = [block.text for block in final_response.content if block.type == "text"]
    return "\n".join(text_parts) if text_parts else "", total_usage


# %sql cells reach call_fix from the native-SQL execute-first loop. Without this
# rule the message told the model the cell was Python and to return only Python.
_SQL_FIX_RETURN_RULE = (
    "This is a SPARK SQL cell. You MUST call the submit_code tool to return the WHOLE cell, "
    "starting with %sql. Do NOT return code as text.\n"
    "The code field must contain ONLY valid Spark SQL — never convert the cell to Python. "
    "Never comment out or remove a statement to make the cell pass, and never add a "
    "statement that writes data. The only exception is a statement this prompt documents "
    "as unsupported AND unnecessary on AIDP (e.g. MSCK REPAIR TABLE): keep the original "
    "line as a SQL comment tagged '-- Oracle tool modification: <reason>' and replace it "
    "as documented — a comment-only %sql cell fails on AIDP.")


class UnresolvedRepairError(RuntimeError):
    """No changed caller or acknowledged source-backed upstream repair was delivered."""


def _normalize_fix_response(resp, original_code, log_fn=None):
    # Safety check: if tool rounds exhausted, Opus may return prose instead of code.
    # Detect PROSE explicitly (language-agnostic) rather than whitelisting Python
    # prefixes. A Python-only whitelist drops valid non-Python fixes — code
    # starting with "%scala", "val ", "object ", "SELECT ", etc. is not Python
    # and must not be discarded just because it isn't.
    # Rule: trust the response as code UNLESS it clearly opens like English prose
    # or is wrapped in a markdown fence (then extract the fenced code).
    stripped_resp = resp.strip() if resp else ""
    _PROSE_OPENERS = (
        "i ", "i'", "i’", "here", "to fix", "to resolve", "to make", "sure",
        "let me", "let's", "the error", "the issue", "the problem", "the cell",
        "the code", "this ", "it ", "we ", "you ", "based on", "looking at",
        "note:", "note that", "unfortunately", "apologies", "sorry", "first,",
        "the fix", "the root", "the failure", "okay", "ok,", "certainly",
    )
    # Positive code signal: an unambiguous code/magic opener can never be
    # English prose, so don't let a prose-opener match (e.g. a fix that
    # legitimately starts with "select"/"with") send valid code through the
    # extract-from-prose path. (Markdown-fenced responses still go through
    # extraction below.)
    _CODE_OPENERS = (
        "%scala", "%sql", "%python", "%pip", "%sh", "val ", "var ", "def ",
        "object ", "class ", "import ", "from ", "package ", "@", "select ",
        "with ", "create ", "insert ", "spark.", "print(",
    )
    _has_code_signal = stripped_resp.lower().startswith(_CODE_OPENERS)
    _looks_like_prose = (
        not stripped_resp
        or stripped_resp.startswith("```")
        or (stripped_resp.lower().startswith(_PROSE_OPENERS) and not _has_code_signal)
    )
    if _looks_like_prose:
        # Looks like prose (or markdown-fenced) — try extracting code from it
        extracted = _extract_code_from_response(resp)
        if extracted and extracted != resp:
            if log_fn:
                log_fn("  [fix] Answer was prose/fenced — extracted code from response")
            return rewrite_oci_credential_reads(extracted)
        # Still prose — return original code unchanged so it doesn't execute garbage
        if log_fn:
            log_fn(f"  [fix] WARNING: Answer is prose, not code — keeping original code")
        return original_code  # no executable candidate delivered
    return rewrite_oci_credential_reads(resp)


async def call_fix(code: str, error: str, context: List[str], attempt: int,
                   extra_context: str = "", session=None, log_fn=None,
                   notebook_path: str = "", inline_context: bool = False) -> str:
    """Call Claude Opus 5 to fix a cell. Uses full 1M context window.
    If session is provided, enables tool use for path exploration on cluster."""
    import asyncio
    # Earlier cells are NOT inlined by default (that dump grew with the cell index
    # and was the largest uncached block in every fix round). The model already gets
    # the JOB EXECUTION HISTORY block and can fetch any cell's final code with
    # get_history_entry(index) or the original with read_notebook_source.
    # inline_context=True keeps the full source: the child-repair path passes this
    # task's successful sources and relies on them being in the prompt (#201).
    ctx = ""
    if notebook_path:
        ctx += f"\n\nSource notebook path: {notebook_path}\n"
        ctx += f"(Use this path with read_notebook_source if you need to check the original cell content.)\n"
    if extra_context:
        ctx += f"\n\n{extra_context}\n"
    if context and inline_context:
        ctx += "\n\nALL previously executed cells in this session (in order):\n"
        for i, c in enumerate(context):
            ctx += f"### Cell {i} (executed OK):\n```python\n{c}\n```\n\n"
    elif context:
        ctx += (f"\n\n{len(context)} earlier cell(s) in this notebook executed OK in this session. "
                "Their source is not inlined: use get_history_entry(index) (indices are in the "
                "JOB EXECUTION HISTORY above) or read_notebook_source to inspect one when the "
                "error depends on an upstream definition.\n")

    is_sql_cell = code.lstrip().startswith("%sql")
    is_scala_cell = code.lstrip().startswith("%scala")
    code_lang = "sql" if is_sql_cell else "scala" if is_scala_cell else "python"
    if is_sql_cell:
        return_rule = _SQL_FIX_RETURN_RULE
    elif is_scala_cell:
        return_rule = (
            "You MUST call the submit_code tool to return the complete fixed Scala cell, "
            "starting with %scala. Preserve Scala; do NOT port to Python. "
            "Keep original statements in // comments with Oracle tool modification tags.")
    else:
        return_rule = (
            "You MUST call the submit_code tool to return your fixed Python code. "
            "Do NOT return code as text.\n"
            "The code field must contain ONLY valid executable Python.")
    fix_input = f"""Fix this cell (attempt {attempt}/{MAX_FIX_ATTEMPTS}).

The code:
```{code_lang}
{code}
```

The error:
```
{error[:5000]}
```
{ctx}

Diagnose the failing operation from its exception and runtime inputs first.
For type errors, inspect relevant schema/types before scanning data.
Separate code/API errors from data-quality findings; neither alone proves
required data is unavailable. Preserve the intended calculation; do not hide
errors or discard data to make execution pass. Use available tools
(explore_path, suggest_oci_path, search_catalog, run_on_cluster) to verify
relevant data/resource availability BEFORE changing data-access code; do not guess paths. Use DATA_UNAVAIL only
when evidence shows a required resource prevents validation.

{return_rule}"""

    if session:
        # Keep investigation and repair delivery in one conversation. Only this
        # task's completed dependency capability enables the bounded reserve;
        # ordinary fix calls retain their existing investigation cap.
        repairable = [e for e in _cell_history
                      if e.get("_task_key", "") == _current_task_key
                      and getattr(e.get("_child_replay_owner"), "completed_repair", None)]
        resp, _ = await call_opus_with_tools(
            FIX_PROMPT, fix_input, session=session,
            max_tokens=128000, max_tool_rounds=MAX_TOOL_ROUNDS_FIX, log_fn=log_fn,
            effort=(EFFORT_FIX_FIRST_ATTEMPT if attempt == 1 else None),
            repair_caller=code if repairable else None,
        )
        return _normalize_fix_response(resp, code, log_fn)
    else:
        loop = asyncio.get_event_loop()
        def _call_streaming():
            text_parts = []
            with claude().messages.stream(
                model=llm_config.MODEL,
                max_tokens=128000,
                timeout=1800,  # a 128k-token generation can exceed the SDK default; bound it explicitly
                system=FIX_PROMPT,
                messages=[{"role": "user", "content": fix_input}]
            ) as stream:
                for text in stream.text_stream:
                    text_parts.append(text)
            return "".join(text_parts)
        # Route the streaming fix through the SAME transient-retry wrapper as the
        # non-streaming calls (it runs _call_streaming in an executor internally),
        # so an opaque 400 "Invalid request data" / 529 / connection blip on the
        # fix path is retried instead of hard-failing the task.
        resp = await _create_message_with_529_retry(loop, _call_streaming, log_fn=log_fn)
        return rewrite_oci_credential_reads(_extract_code_from_response(resp))


# ── Data recovery (test-only date substitution) ──────────────────────
#
# When a cell fails with an empty-data signature, the framework calls
# attempt_data_recovery() to get a one-shot override block that substitutes
# the cell's date-filter variables with dates that ACTUALLY have data in the
# upstream table. The override is prepended to the cell's exec code ONLY;
# the saved cell stays byte-identical to the customer's original.
#
# Flow:
#   1. Opus inspects the cell + error, uses describe_table / run_on_cluster
#      to identify the table, date column, and cell-scope date variables.
#   2. Opus submits a plan via submit_data_recovery_plan (table, date_column,
#      list of variable overrides).
#   3. Framework queries the table for distinct recent dates.
#   4. Framework builds the override block (with begin/end markers) and
#      returns it. Caller prepends it to exec code, re-runs the cell.
#   5. If the query returns no rows: recovery fails (data truly unavailable);
#      caller falls through to the regular fix loop / DATA_UNAVAILABLE path.

DATA_RECOVERY_PROMPT = """You are diagnosing an EMPTY-DATA failure in a cell migrated to OCI AIDP (Spark 3.5.0, Python 3.11).

The cell's CODE is correct. The problem is that the cell hardcodes specific dates (in a list, a string, or a range) and the upstream table simply has no rows for those dates. Your job is NOT to modify the cell. Your job is to identify, exactly:

  1. The fully-qualified TABLE the cell is reading from (e.g. "default.db.tbl").
  2. The COLUMN in that table that the cell is filtering by date (e.g. "load_date", "partition_date").
  3. The cell-scope VARIABLES that hold the date filter values (e.g. "uw_fct_date_list", "start_date").

You have these tools:
  - describe_table: get schema of a Spark table (use to confirm the date column name and type).
  - run_on_cluster: execute PySpark on cluster (use to verify the table exists, the column exists, what the column's type/format is).
  - read_notebook_source: read the cell's notebook if needed for context.

When you have identified all three pieces, call the submit_data_recovery_plan tool with:
  - "table": the FQ table name (string).
  - "date_column": the column name to filter on (string).
  - "overrides": a list of {variable, is_list, max_dates} entries — one per cell-scope variable that holds date values.

Rules:
  - Do NOT modify the cell's code. The framework will inject a test-only override at execution time; the saved cell stays as-is.
  - Confirm the table actually exists via describe_table or run_on_cluster BEFORE submitting. If the table genuinely doesn't exist, don't submit — let the cell fail through the normal path.
  - is_list=true: variable is a list of date strings (e.g. `uw_fct_date_list = ['2026-04-01', '2026-04-02', ...]`). max_dates: how many recent available dates to include (3-14 typical).
  - is_list=false: variable is a single date string (e.g. `start_date = "2026-04-01"`). max_dates: always 1 in this case.
  - Date format: use whatever format the table's column uses (typically YYYY-MM-DD).
  - If multiple cell-scope variables need overrides (e.g. start_date AND end_date), include all of them in overrides.
  - If you cannot identify the table or date column with high confidence, do not submit. The cell will fall through to DATA_UNAVAILABLE handling, which is the correct outcome.

Return ONLY the tool call. No prose."""


async def attempt_data_recovery(
    cell_source: str,
    error_output: str,
    session,
    notebook_path: str = "",
    log_fn=None,
    max_dates_cap: int = 14,
) -> Optional[str]:
    """Identify date-substitution overrides for an empty-data cell failure and
    return an EXEC-only override block (BEGIN/END marker-bracketed) that the
    caller prepends to the cell's exec code. Returns None if recovery is not
    possible (Opus couldn't identify the table, table query returned no rows,
    plan was malformed, etc.).

    The returned block is a Python snippet of the form:

        # === AIDP_DATA_RECOVERY_OVERRIDE_BEGIN (test-only — not saved) ===
        # Source table: <fq>, date column: <col>, dates: <values from cluster>
        uw_fct_date_list = ['2024-12-08', '2024-12-07', ...]
        start_date = '2024-12-08'
        # === AIDP_DATA_RECOVERY_OVERRIDE_END ===

    The caller injects this BEFORE the cell's code, executes, and on success
    saves the cell's code WITHOUT this block (via _strip_data_recovery_block).
    """
    import json as _json

    def _log(msg):
        if log_fn:
            log_fn(msg)
        else:
            print(msg)

    if not session:
        _log("[data-recovery] no cluster session — skipping")
        return None

    # Build the Opus user message
    ctx = f"\n\nSource notebook path: {notebook_path}\n" if notebook_path else ""
    recovery_input = f"""A cell failed on the AIDP cluster with what looks like an empty-data signature (the code is correct, the data isn't there for the hardcoded dates).

The cell source:
```python
{cell_source[:6000]}
```

The error output:
```
{(error_output or "")[:3000]}
```
{ctx}

Identify the table, date column, and date-holding variables in this cell. Confirm the table exists. Then call submit_data_recovery_plan with your findings. If you cannot identify these with confidence, do not call the tool — the cell will be marked DATA_UNAVAILABLE which is the correct outcome."""

    # Call Opus with the recovery prompt. call_opus_with_tools returns the
    # plan as a JSON-encoded string when submit_data_recovery_plan is called.
    try:
        # Fix 7z (2026-07-29): capped at MAX_TOOL_ROUNDS_MIGRATE (was 10). Data
        # recovery has a narrow, deterministic scope (identify date column,
        # confirm table, propose substitution) — 5 rounds is enough.
        resp, _ = await call_opus_with_tools(
            DATA_RECOVERY_PROMPT, recovery_input,
            session=session, max_tokens=64000, max_tool_rounds=MAX_TOOL_ROUNDS_MIGRATE, log_fn=log_fn,
        )
    except Exception as e:
        _log(f"[data-recovery] Opus call failed: {str(e)[:200]}")
        return None

    # Parse the plan
    try:
        plan = _json.loads(resp)
    except (ValueError, TypeError):
        _log(f"[data-recovery] Opus returned non-JSON (likely chose not to submit a plan)")
        return None

    table = plan.get("table", "").strip()
    date_column = plan.get("date_column", "").strip()
    overrides = plan.get("overrides", [])
    if not table or not date_column or not overrides:
        _log(f"[data-recovery] plan missing required fields: {plan}")
        return None

    # Safety: identifier hygiene (prevent SQL injection via Opus). Allow only
    # alphanumerics, dots, underscores in table/column names.
    import re as _re
    _SAFE_IDENT = _re.compile(r"^[A-Za-z_][A-Za-z_0-9.]*$")
    if not _SAFE_IDENT.match(table):
        _log(f"[data-recovery] unsafe table identifier: {table!r} — aborting")
        return None
    if not _SAFE_IDENT.match(date_column):
        _log(f"[data-recovery] unsafe date_column identifier: {date_column!r} — aborting")
        return None

    # Determine how many distinct dates we need to fetch (cap across all overrides).
    requested_max = 1
    for ov in overrides:
        if not isinstance(ov, dict):
            _log(f"[data-recovery] malformed override: {ov!r}")
            return None
        var = ov.get("variable", "").strip()
        if not _SAFE_IDENT.match(var):
            _log(f"[data-recovery] unsafe variable identifier: {var!r}")
            return None
        n = int(ov.get("max_dates", 1) or 1)
        if n > requested_max:
            requested_max = min(n, max_dates_cap)

    # Query the cluster for the most recent distinct dates that exist in the
    # column. This is the GROUND TRUTH that drives the substitution.
    query_code = f"""
import json as _j
try:
    rows = spark.sql("SELECT DISTINCT `{date_column}` AS d FROM `{table.replace('.', '`.`')}` WHERE `{date_column}` IS NOT NULL ORDER BY `{date_column}` DESC LIMIT {requested_max}").collect()
    vals = [str(r[0]) for r in rows if r[0] is not None]
    print(_j.dumps({{"ok": True, "dates": vals}}))
except Exception as _e:
    print(_j.dumps({{"ok": False, "error": str(_e)[:300]}}))
""".strip()
    try:
        from aidp_executor import format_outputs
        result = await session.execute(query_code, timeout=120)
        raw = format_outputs(result.get("outputs", [])).strip()
        # Last line is the JSON print result
        json_line = raw.split("\n")[-1] if raw else ""
        probe = _json.loads(json_line)
    except Exception as e:
        _log(f"[data-recovery] cluster probe failed: {str(e)[:200]}")
        return None

    if not probe.get("ok"):
        _log(f"[data-recovery] cluster probe returned error: {probe.get('error', '?')}")
        return None

    available_dates = probe.get("dates", [])
    if not available_dates:
        _log(f"[data-recovery] table {table} has NO available dates in column {date_column} — recovery not possible")
        return None

    # Build the override block. Each variable gets its slice of available_dates.
    lines = [
        "# === AIDP_DATA_RECOVERY_OVERRIDE_BEGIN (test-only — not saved) ===",
        f"# Source table: {table}, date column: {date_column}",
        f"# Available dates (most recent first): {available_dates[:3]}{'...' if len(available_dates) > 3 else ''}",
    ]
    for ov in overrides:
        var = ov["variable"].strip()
        is_list = bool(ov.get("is_list", False))
        n = min(int(ov.get("max_dates", 1) or 1), len(available_dates))
        if n < 1:
            n = 1
        if is_list:
            slice_ = available_dates[:n]
            lines.append(f"{var} = {repr(slice_)}")
        else:
            lines.append(f"{var} = {repr(available_dates[0])}")
    lines.append("# === AIDP_DATA_RECOVERY_OVERRIDE_END ===")
    block = "\n".join(lines) + "\n"

    # Defensive: ensure the built block compiles as valid Python. Guards
    # against pathological edge cases (Opus returning a variable name that
    # is a Python reserved keyword like "class" or "for"; a date value with
    # a control character; etc.) that would otherwise turn the cell failure
    # from "empty data" into "SyntaxError in injected code" and confuse the
    # downstream fix loop.
    try:
        compile(block, "<aidp-data-recovery-override>", "exec")
    except SyntaxError as _se:
        _log(f"[data-recovery] override block failed compile check: {_se} — aborting recovery")
        return None

    _log(f"[data-recovery] built override: table={table} col={date_column} dates={available_dates[:3]} vars={[ov['variable'] for ov in overrides]}")
    return block


def _extract_code_from_response(resp: str) -> str:
    """Extract Python code from Opus response, stripping explanation text.
    Handles: markdown fences, preamble text before code, trailing explanations."""
    resp = resp.strip()

    # Strip markdown code fences
    if resp.startswith("```"):
        lines = resp.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        resp = "\n".join(lines).strip()

    # If the response contains code fences anywhere (Opus wrapped code in them)
    if "```python" in resp or "```\n" in resp:
        # Extract content between first ``` and last ```
        import re as _re
        m = _re.search(r'```(?:python)?\s*\n(.*?)```', resp, _re.DOTALL)
        if m:
            resp = m.group(1).strip()

    # If there's preamble text before actual Python code, try to strip it
    # Look for the first line that looks like Python code
    lines = resp.split("\n")
    code_start = 0

    # Patterns that indicate prose/explanation (NOT Python code)
    prose_starters = (
        "The ", "This ", "I ", "Note", "Here", "We ", "Since ", "Good", "Now ",
        "Let ", "First", "OK", "Alright", "Sure", "Based ", "After ", "Looking ",
        "To ", "In ", "However", "But ", "Also", "So ", "My ", "Actually",
    )

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue

        # Check if this line looks like Python code
        if any(stripped.startswith(kw) for kw in [
            "import ", "from ", "def ", "class ", "if ", "for ", "while ", "try:",
            "with ", "return ", "raise ", "yield ", "async ", "await ",
            "#", "spark.", "dbutils.", "display(", "sql(", "print(",
        ]) or ("=" in stripped and not stripped.endswith(".")
               and ":" not in stripped.split("=")[0]
               and not stripped[0].isdigit()
               and not any(stripped.startswith(p) for p in prose_starters)):
            code_start = i
            break

        # Prose indicators - skip these lines
        if stripped.startswith(prose_starters):
            continue
        if stripped.startswith("`"):
            continue
        # Numbered list items (1. 2. etc)
        if stripped[0].isdigit() and ("." in stripped[:3] or ")" in stripped[:3]):
            continue
        # Bullet points
        if stripped.startswith(("- ", "* ", "• ")):
            continue

        # Otherwise assume it's code
        code_start = i
        break

    if code_start > 0:
        resp = "\n".join(lines[code_start:]).strip()

    # Also strip trailing prose after the last line of code
    lines = resp.split("\n")
    code_end = len(lines)
    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if not stripped:
            continue
        if stripped.startswith(prose_starters) or (stripped[0].isdigit() and "." in stripped[:3]):
            code_end = i
            continue
        break
    if code_end < len(lines):
        resp = "\n".join(lines[:code_end]).strip()

    return resp


# ─── Notebook Processing ─────────────────────────────────────────────

def read_notebook(path: str) -> Tuple[dict, List[Optional[str]], str]:
    """Read notebook, extract outputs, build readable text."""
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        nb = json.load(f)

    outputs = []
    readable_parts = []
    for i, cell in enumerate(nb.get("cells", [])):
        cell_type = cell.get("cell_type", "unknown")
        source = "".join(cell.get("source", []))
        readable_parts.append(f"--- Cell {i} [{cell_type}] ---")
        readable_parts.append(source)
        readable_parts.append("")

        # Extract original output
        cell_outputs = cell.get("outputs", [])
        text_parts = []
        for out in cell_outputs:
            if out.get("output_type") == "stream":
                text_parts.append("".join(out.get("text", [])))
            elif out.get("output_type") in ("execute_result", "display_data"):
                data = out.get("data", {})
                if "text/plain" in data:
                    v = data["text/plain"]
                    text_parts.append("".join(v) if isinstance(v, list) else v)
        outputs.append("".join(text_parts) if text_parts else None)

    readable = "\n".join(readable_parts)
    if len(readable) > 80000:
        readable = readable[:80000] + "\n[... TRUNCATED ...]"
    return nb, outputs, readable


async def process_single_notebook(notebook_path: str, session: AIDPSession) -> dict:
    """Full pipeline for one notebook: download, analyze, migrate, test, fix, upload, cleanup."""
    nb_name = os.path.basename(notebook_path).replace(".ipynb", "")
    migrate_output = f"/Workspace/{OUTPUT_FOLDER}/{os.path.dirname(notebook_path)}/{nb_name}"

    print(f"\n{'='*60}")
    print(f"PROCESSING: {notebook_path}")
    print(f"{'='*60}")

    # Create temp dir for this notebook
    tmpdir = tempfile.mkdtemp(prefix=f"aidp_migrate_{nb_name}_")
    local_nb = os.path.join(tmpdir, "original.ipynb")
    total_tokens = 0

    try:
        # ── STEP 1: Download ──────────────────────────────────────
        print(f"  [1/6] Downloading...")
        try:
            download_notebook(notebook_path, local_nb)
        except Exception as e:
            print(f"  DOWNLOAD FAILED: {e}")
            return {"path": notebook_path, "status": "download_failed", "error": str(e)[:200]}

        nb, original_outputs, readable = read_notebook(local_nb)
        total_cells = len(nb.get("cells", []))
        outputs_with_data = sum(1 for o in original_outputs if o)
        print(f"  Downloaded: {total_cells} cells, {outputs_with_data} with saved outputs")

        # ── STEP 2: Analyze with Opus ─────────────────────────────
        print(f"  [2/6] Analyzing with {llm_config.MODEL}...")
        analysis_text, usage = call_opus(
            ANALYSIS_PROMPT,
            f"Analyze this notebook for Databricks-to-AIDP migration:\n\nPath: {notebook_path}\n\n```\n{readable}\n```"
        )
        total_tokens += usage["input"] + usage["output"]
        print(f"  Analysis: {usage['input']+usage['output']:,} tokens")

        # Save analysis report
        analysis_path = os.path.join(tmpdir, "analysis_report.md")
        with open(analysis_path, 'w') as f:
            f.write(f"# Analysis Report: {notebook_path}\n\n{analysis_text}")

        # ── STEP 3: Migrate with Opus ─────────────────────────────
        print(f"  [3/6] Migrating with {llm_config.MODEL}...")
        migrate_text, usage = call_opus(
            MIGRATION_PROMPT,
            f"Migrate this notebook. Path: {notebook_path}\n\nAnalysis:\n{analysis_text[:5000]}\n\nNotebook:\n```\n{readable}\n```"
        )
        total_tokens += usage["input"] + usage["output"]
        print(f"  Migration: {usage['input']+usage['output']:,} tokens")

        # Parse migrated cells
        cleaned = migrate_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines)
        start = cleaned.find("{")
        end = cleaned.rfind("}") + 1
        if start >= 0 and end > start:
            cleaned = cleaned[start:end]

        try:
            migrate_result = json.loads(cleaned)
            migrated_cells = migrate_result.get("cells", [])
            migration_notes = migrate_result.get("migration_notes", [])
        except json.JSONDecodeError:
            migrated_cells = [{"cell_type": c.get("cell_type", "code"),
                               "source": "".join(c.get("source", [])),
                               "classification": "READ_ONLY"} for c in nb.get("cells", [])]
            migration_notes = ["JSON parse failed - using original cells"]

        # Build migrated notebook
        migrated_nb = copy.deepcopy(nb)
        new_cells = []
        for mc in migrated_cells:
            cell = {"cell_type": mc.get("cell_type", "code"), "metadata": {},
                    "source": [mc.get("source", "")], "outputs": [], "execution_count": None}
            if mc.get("cell_type") == "markdown":
                cell.pop("outputs", None)
                cell.pop("execution_count", None)
            new_cells.append(cell)
        migrated_nb["cells"] = new_cells

        # Save migration report
        migration_report = f"# Migration Report: {notebook_path}\n\n## Notes\n"
        for note in migration_notes:
            migration_report += f"- {note}\n"

        migrated_path = os.path.join(tmpdir, "migrated.ipynb")
        with open(migrated_path, 'w') as f:
            json.dump(migrated_nb, f, indent=1)

        migration_report_path = os.path.join(tmpdir, "migration_report.md")
        with open(migration_report_path, 'w') as f:
            f.write(migration_report)

        # ── STEP 4: Test on cluster ───────────────────────────────
        print(f"  [4/6] Testing on cluster ({len(migrated_cells)} cells)...")

        # Bootstrap aidp_compat
        await session.execute("import sys; sys.path.insert(0, '/Workspace/migration-dependencies/python_libs/')", timeout=30)

        cell_results = []
        executed_code = []
        cells_ok = 0
        cells_failed = 0
        cells_skipped = 0
        cells_fixed = 0
        output_matches = 0
        output_diffs = 0

        for i, mc in enumerate(migrated_cells):
            classification = mc.get("classification", "SKIP")
            source = mc.get("source", "").strip()
            cell_type = mc.get("cell_type", "code")

            if cell_type != "code" or not source or classification in ("SKIP", "WRITE", "NOTIFICATION"):
                cell_results.append({"cell": i, "classification": classification, "status": "skipped"})
                cells_skipped += 1
                continue

            # Execute READ_ONLY cell
            current_code = source
            cell_passed = False

            for attempt in range(6):  # 0=first try, 1-2=Sonnet, 3-5=Opus
                result = await session.execute(current_code, timeout=120)
                status = result.get("status", "error")
                output = format_outputs(result.get("outputs", []))

                if status == "ok":
                    cell_passed = True
                    executed_code.append(current_code)

                    # Compare output
                    orig = original_outputs[i] if i < len(original_outputs) else None
                    match_detail = None
                    if orig:
                        orig_clean = " ".join(orig.strip().split())
                        actual_clean = " ".join((output or "").strip().split())
                        if orig_clean == actual_clean:
                            match_detail = "exact match"
                            output_matches += 1
                        else:
                            match_detail = "differs"
                            output_diffs += 1

                    if attempt > 0:
                        cells_fixed += 1
                        new_cells[i]["source"] = [current_code]

                    cell_results.append({
                        "cell": i, "classification": "READ_ONLY", "status": "ok",
                        "attempts": attempt + 1, "fixed": attempt > 0,
                        "code": current_code[:500],
                        "output": (output or "")[:1000],
                        "original_output": (orig[:500] if orig else None),
                        "output_match": match_detail,
                    })
                    cells_ok += 1
                    break
                else:
                    if attempt < 5:
                        print(f"    Cell {i} FAIL (attempt {attempt+1}), asking {llm_config.MODEL}...")
                        try:
                            current_code = call_fix(current_code, output, executed_code, attempt + 1)
                        except:
                            break
                    else:
                        cell_results.append({
                            "cell": i, "classification": "READ_ONLY", "status": "error",
                            "attempts": 6, "code": current_code[:500],
                            "output": (output or "")[:1000],
                        })
                        cells_failed += 1

        # ── STEP 5: Build test report ─────────────────────────────
        overall = "PASS" if cells_failed == 0 else "PARTIAL" if cells_ok > 0 else "FAIL"
        print(f"  [5/6] Result: {overall} | OK:{cells_ok} Failed:{cells_failed} Skipped:{cells_skipped} Fixed:{cells_fixed}")

        test_report = f"""# Test Report: {notebook_path}

## Summary
- **Date**: {datetime.now().isoformat()}
- **Result**: **{overall}**
- **Cells**: {cells_ok} OK, {cells_failed} failed, {cells_skipped} skipped, {cells_fixed} auto-fixed
- **Output comparison**: {output_matches} match, {output_diffs} differ
- **Total Claude tokens**: {total_tokens:,}

## Cell Results
"""
        for cr in cell_results:
            ci = cr["cell"]
            st = cr.get("status", "?")
            cls = cr.get("classification", "?")

            if st == "skipped":
                test_report += f"\n### Cell {ci} - SKIPPED ({cls})\n"
                continue

            test_report += f"\n### Cell {ci} - {st.upper()}"
            if cr.get("fixed"):
                test_report += f" (fixed, attempt {cr.get('attempts', '?')})"
            test_report += f"\n"

            if cr.get("code"):
                code_display = cr["code"] if len(cr["code"]) < 1000 else cr["code"][:1000] + "\n# ... truncated"
                test_report += f"\n```python\n{code_display}\n```\n"
            if cr.get("output"):
                out_display = cr["output"] if len(cr["output"]) < 1500 else cr["output"][:1500] + "\n... truncated"
                test_report += f"\n**Output:**\n```\n{out_display}\n```\n"
            if cr.get("original_output"):
                test_report += f"\n**Original output:** `{cr['original_output'][:200]}`\n"
                test_report += f"**Match:** {cr.get('output_match', 'N/A')}\n"

        test_report_path = os.path.join(tmpdir, "test_report.md")
        with open(test_report_path, 'w') as f:
            f.write(test_report)

        # Save final notebook (with fixes applied)
        final_path = os.path.join(tmpdir, "final.ipynb")
        with open(final_path, 'w') as f:
            json.dump(migrated_nb, f, indent=1)

        # ── STEP 6: Upload artifacts to agent-migrated/ ──────────
        print(f"  [6/6] Uploading artifacts to {OUTPUT_FOLDER}/...")

        # Create output folder structure via executor
        await session.execute(f"import os; os.makedirs('{migrate_output}', exist_ok=True)", timeout=30)

        artifacts = [
            (analysis_path, f"{migrate_output}/analysis_report.md"),
            (migrated_path, f"{migrate_output}/migrated.ipynb"),
            (migration_report_path, f"{migrate_output}/migration_report.md"),
            (test_report_path, f"{migrate_output}/test_report.md"),
            (final_path, f"{migrate_output}/final.ipynb"),
        ]

        for local_file, remote_path in artifacts:
            with open(local_file, 'r') as f:
                content = f.read()
            # Write via executor
            import base64
            b64 = base64.b64encode(content.encode('utf-8')).decode('ascii')
            if len(b64) < 50000:
                code = f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
"""
                await session.execute(code, timeout=60)
            else:
                # Skip very large files for now
                print(f"    Skipping upload of {os.path.basename(local_file)} (too large)")

        return {
            "path": notebook_path, "status": overall,
            "ok": cells_ok, "failed": cells_failed, "skipped": cells_skipped,
            "fixed": cells_fixed, "output_matches": output_matches,
            "output_diffs": output_diffs, "tokens": total_tokens,
        }

    finally:
        # ── STEP 7: Cleanup local temp files ──────────────────────
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Batch Processing ────────────────────────────────────────────────

async def run_batch(notebooks: List[str], cluster_id: str, parallel: int):
    """Process notebooks in parallel with separate sessions."""
    print(f"{'='*60}")
    print(f"Agent Migration Pipeline")
    print(f"{'='*60}")
    print(f"Notebooks: {len(notebooks)}")
    print(f"Parallel: {parallel}")
    print(f"Cluster: {cluster_id}")
    print(f"Output: /Workspace/{OUTPUT_FOLDER}/")
    print(f"Started: {datetime.now().isoformat()}")

    # Create output folder
    try:
        create_folder(OUTPUT_FOLDER)
    except:
        pass

    # Split into chunks
    chunks = [[] for _ in range(parallel)]
    for i, nb in enumerate(notebooks):
        chunks[i % parallel].append(nb)

    all_results = []

    async def worker(worker_id: int, chunk: List[str]):
        session = None
        results = []
        for i, nb_path in enumerate(chunk):
            # Reconnect every 10 notebooks
            if session is None or i % 10 == 0:
                if session:
                    try: await session.close()
                    except: pass
                session = AIDPSession(cluster_id=cluster_id)
                await session.connect()

            try:
                result = await process_single_notebook(nb_path, session)
                results.append(result)
            except Exception as e:
                print(f"  [Worker {worker_id}] Error: {nb_path}: {e}")
                results.append({"path": nb_path, "status": "error", "error": str(e)[:200]})
                try: await session.close()
                except: pass
                session = None

        if session:
            try: await session.close()
            except: pass
        return results

    tasks = [worker(i, chunk) for i, chunk in enumerate(chunks) if chunk]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    for cr in chunk_results:
        if isinstance(cr, list):
            all_results.extend(cr)

    # Summary
    total_ok = sum(r.get("ok", 0) for r in all_results if isinstance(r, dict))
    total_fail = sum(r.get("failed", 0) for r in all_results if isinstance(r, dict))
    total_fix = sum(r.get("fixed", 0) for r in all_results if isinstance(r, dict))
    total_tokens = sum(r.get("tokens", 0) for r in all_results if isinstance(r, dict))
    pass_count = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "PASS")

    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"  Notebooks: {len(all_results)} | PASS: {pass_count}")
    print(f"  Cells OK: {total_ok} | Failed: {total_fail} | Fixed: {total_fix}")
    print(f"  Tokens: {total_tokens:,}")
    print(f"  Output: /Workspace/{OUTPUT_FOLDER}/")
    print(f"{'='*60}")

    return all_results


async def main():
    global AIDP_BASE, DATALAKE_OCID, WORKSPACE_ID, OCI_PROFILE, DOWNLOAD_META_URL, UPLOAD_FOLDER_URL, SIGNER
    parser = argparse.ArgumentParser(description="Agent Migration Pipeline")
    parser.add_argument("--parallel", type=int, default=20)
    parser.add_argument("--cluster", required=True,
                        help="AIDP cluster ID (required — no default)")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--notebook", help="Single notebook path")
    # AIDP environment — override these for other-customer deployments
    parser.add_argument("--aidp-base", default=AIDP_BASE,
                        help="AIDP REST endpoint base URL (default: %(default)s)")
    parser.add_argument("--datalake-ocid", default=DATALAKE_OCID,
                        help="AIDP data lake OCID (default: %(default)s)")
    parser.add_argument("--workspace-id", default=WORKSPACE_ID,
                        help="AIDP workspace UUID (default: %(default)s)")
    parser.add_argument("--oci-profile", default=OCI_PROFILE,
                        help="OCI config profile name in ~/.oci/config (default: %(default)s)")
    parser.add_argument("--config", default=None,
                        help="aidp_config YAML path (highest precedence; else "
                             "$AIDP_CONFIG_PATH / ./aidp_migrator.yaml / ~).")
    args = parser.parse_args()

    # Standalone entrypoint: resolve the model from YAML (no code default).
    # --config has highest precedence, matching the documented loader contract.
    llm_config.ensure_configured(args.config)

    # Apply environment config — must happen before any code uses these globals
    AIDP_BASE       = args.aidp_base
    DATALAKE_OCID   = args.datalake_ocid
    WORKSPACE_ID    = args.workspace_id
    OCI_PROFILE     = args.oci_profile
    DOWNLOAD_META_URL = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
                         f"/actions/downloadFileMeta")
    UPLOAD_FOLDER_URL = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
                         f"/objects")
    SIGNER = None  # reset so signer() picks up new OCI_PROFILE

    if args.notebook:
        session = AIDPSession(cluster_id=args.cluster)
        await session.connect()
        try:
            result = await process_single_notebook(args.notebook, session)
            print(json.dumps(result, indent=2))
        finally:
            await session.close()
        return

    # Load notebook list
    report_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports", "notebook_list.json")
    with open(report_path) as f:
        notebooks = json.load(f)

    paths = [nb["path"] for nb in notebooks]
    paths = paths[args.start:args.end]

    await run_batch(paths, args.cluster, args.parallel)


if __name__ == "__main__":
    asyncio.run(main())
