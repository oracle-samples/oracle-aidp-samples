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

from __future__ import annotations

from typing import Optional
import asyncio
import os
import re
import sys
import time

import oci
import requests as http_requests
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:  # Package import in tests/tools; script import in AIDP notebooks.
    from .cluster_states import (  # type: ignore
        ACTIVE_CLUSTER_STATES,
        SHUTDOWN_CLUSTER_STATES,
        STARTABLE_CLUSTER_STATES,
        START_PROGRESS_CLUSTER_STATES,
        TERMINAL_CLUSTER_STATES,
    )
except ImportError:
    from cluster_states import (  # noqa: E402
        ACTIVE_CLUSTER_STATES,
        SHUTDOWN_CLUSTER_STATES,
        STARTABLE_CLUSTER_STATES,
        START_PROGRESS_CLUSTER_STATES,
        TERMINAL_CLUSTER_STATES,
    )

# ─── AIDP Environment (same as other scripts) ──────────────────────────

# Generic — no hardcoded customer/AIDP-instance config; set at runtime
# (job_migrate_from_workflow overrides these). Profile defaults to "DEFAULT".
AIDP_BASE = None
DATALAKE_OCID = None
WORKSPACE_ID = None
OCI_PROFILE = "DEFAULT"

# aidp_compat wheel — TARGET_WHEEL_PATH derives from aidp_compat/__init__.py's
# __version__ (single source of truth). Same version is used by aidp_compat/setup.py
# at build time, so the wheel filename produced by `python setup.py bdist_wheel`
# matches the path we expect on the cluster.
AIDP_COMPAT_DEPS_DIR = "/Workspace/migration-dependencies"


def _resolve_lib_dir(config_path: str = None) -> str:
    """Shared dependency repository — every migration artifact (baseline JARs
    AND wheels, incl. the aidp_compat wheel) is staged into and resolved from
    here. Single source of truth is the customer YAML `paths.lib_repository_dir`
    (deprecated aliases: `lib_fallback_dir`, then `jar_fallback_dir`) — the
    SAME value dbx_export resolves — so a customer-configured repository is
    honored by the Phase-2 aidp_compat prerequisite and baseline-library
    lookups too.

    Config entry points mirror the exporter: explicit ``config_path`` (a
    caller's --config), else $AIDP_CONFIG_PATH, else default discovery. When a
    config was EXPLICITLY given (arg or env), loader errors are surfaced — an
    explicit configuration error must never be swallowed into the default.
    Only with no discoverable YAML (older deployments / ad-hoc laptop tools)
    does the tool default apply. The requirements .txt files stay at
    AIDP_COMPAT_DEPS_DIR root; installable libraries live under the resolved
    directory."""
    try:
        from aidp_config import load_config
        paths = load_config(cli_config_path=config_path).paths
        configured = (
            getattr(paths, "lib_repository_dir", None)
            or getattr(paths, "lib_fallback_dir", None)
            or getattr(paths, "jar_fallback_dir", None)
        )
        if configured:
            return str(configured).rstrip("/")
    except Exception:
        if config_path or os.environ.get("AIDP_CONFIG_PATH"):
            raise
    return f"{AIDP_COMPAT_DEPS_DIR}/libs"


AIDP_LIB_DIR = _resolve_lib_dir()

# The pinned runtime triad (pandas==2.2.3, numpy==1.26.4, pyarrow==12.0.1) lives
# in this requirements file. It MUST be installed on every migration cluster —
# otherwise the cluster keeps the stock image's pandas 3.0.x / numpy 2.x, which
# breaks read_csv/.apply on string columns (ArrowStringArray dtype('O') error).
DEFAULT_REQUIREMENTS_PATH = f"{AIDP_COMPAT_DEPS_DIR}/default_requirements.txt"
TARGET_RUNTIME_REQUIREMENTS_PATH = f"{AIDP_COMPAT_DEPS_DIR}/target_runtime_requirements.txt"
CANONICAL_REQUIREMENTS_BASENAME_PREFIX = "resolved_requirements_"


def _is_canonical_requirements_path(path: str) -> bool:
    name = os.path.basename(str(path))
    return name.startswith(CANONICAL_REQUIREMENTS_BASENAME_PREFIX) and name.endswith(".txt")


def _canonical_requirements_path(cluster_id: str, job_output_path: str) -> str:
    """Return the cluster's resolved file, with a legacy fallback path."""
    matches = sorted({
        str(lib.get("path"))
        for lib in _attached_library_rows(cluster_id)
        if _is_canonical_requirements_path(lib.get("path", ""))
    })
    if len(matches) > 1:
        raise RuntimeError(
            f"cluster {cluster_id} has multiple resolved requirements files: {matches}"
        )
    if matches:
        return matches[0]
    return f"{job_output_path}/resolved_requirements_runtime.txt"


def _requirement_names(content: str) -> set:
    names = set()
    for raw in (content or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            names.add(canonicalize_name(Requirement(line).name))
        except InvalidRequirement:
            continue
    return names


def _append_missing_requirements(content: str, packages: list) -> tuple[str, list]:
    """Append only packages not already represented by a PEP 508 entry."""
    existing_names = _requirement_names(content)
    additions = [
        package for package in packages
        if canonicalize_name(package) not in existing_names
    ]
    if not additions:
        return content, []
    prefix = content.rstrip()
    suffix = "\n".join(sorted(set(additions))) + "\n"
    return ((prefix + "\n" if prefix else "") + suffix), sorted(set(additions))

# Read the target version from aidp_compat/__init__.py by direct file scan —
# same approach setup.py uses. This avoids importing the aidp_compat package
# (which pulls in pyspark/DBUtils/entity_crypto shims not needed for version lookup
# and may fail to import on a client machine without those deps).
def _read_aidp_compat_version() -> str:
    _init_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "aidp_compat", "__init__.py"
    )
    try:
        import re as _re
        with open(_init_path) as _f:
            # Anchor to line start so a commented `# __version__ = "..."` doesn't match.
            m = _re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', _f.read(), _re.MULTILINE)
        return m.group(1) if m else None
    except (OSError, AttributeError):
        return None


AIDP_COMPAT_VERSION = _read_aidp_compat_version()

TARGET_WHEEL_PATH = (
    f"{AIDP_LIB_DIR}/aidp_compat-{AIDP_COMPAT_VERSION}-py3-none-any.whl"
    if AIDP_COMPAT_VERSION else None
)
AIDP_COMPAT_PREFIX = f"{AIDP_LIB_DIR}/aidp_compat-"


def _is_aidp_compat_lib(path: str) -> bool:
    """Recognize any aidp_compat-*.whl on the cluster (used to detect whether
    aidp_compat is installed at all, regardless of version). Version drift
    (installed-version != TARGET_WHEEL_PATH) is intentionally NOT caught here —
    upgrade the wheel via scripts/deploy_aidp_compat.py when the version bumps."""
    return path.startswith(AIDP_COMPAT_PREFIX) and path.endswith(".whl")

# ─── OCI Signer ────────────────────────────────────────────────────────

_SIGNER = None


def _get_signer():
    """Shared OCI signer with the same auth precedence as the rest of the tool.

    Delegates to oci_auth.get_oci_signer (the canonical chain:
    AIDP Credential Store on-cluster → $OCI_CONFIG_FILE → ~/.oci/config,
    incl. security-token signers; also re-exported by aidp_executor).
    This module previously built its own
    API-key signer from ~/.oci/config only, which forced a materialized
    key file onto AIDP clusters solely for lifecycle REST calls while
    every other module already authenticated from the Credential Store
    (maintainer request, 2026-10-12). Lazy import keeps module load light.
    Callers reset the
    `_SIGNER` cache (`_cl._SIGNER = None`) after a profile change; that
    contract is unchanged.
    """
    global _SIGNER
    if _SIGNER is None:
        from oci_auth import get_oci_signer
        _, _SIGNER = get_oci_signer(OCI_PROFILE)
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
    """Return TARGET_WHEEL_PATH if that EXACT wheel exists in the workspace,
    else None. Target version comes from aidp_compat/__init__.py — the single
    source of truth. Used for the preflight prerequisite check and at install
    time. If the user has uploaded a different version, this will return None
    (caller raises PREREQUISITE with the expected path)."""
    if TARGET_WHEEL_PATH is None:
        return None
    for it in _list_workspace_objects(AIDP_LIB_DIR):
        if it.get("path") == TARGET_WHEEL_PATH:
            return TARGET_WHEEL_PATH
    return None


def _cluster_url(cluster_id: str) -> str:
    return (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}"
            f"/workspaces/{WORKSPACE_ID}/clusters/{cluster_id}")


def _libraries_url(cluster_id: str) -> str:
    return f"{_cluster_url(cluster_id)}/libraries"


# ─── Cluster State ─────────────────────────────────────────────────────


def get_cluster_state(cluster_id: str) -> str:
    """Return the cluster lifecycle state in canonical upper-case form."""
    signer = _get_signer()
    resp = http_requests.get(
        _cluster_url(cluster_id), auth=signer,
        headers={"Accept": "application/json"}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    state = data.get("lifecycleState") or data.get("state") or "UNKNOWN"
    return str(state).strip().upper()


def get_cluster_init_script_destinations(cluster_id: str) -> set[str]:
    """Read the scripts actually configured on the execution cluster."""
    signer = _get_signer()
    resp = http_requests.get(
        _cluster_url(cluster_id), auth=signer,
        headers={"Accept": "application/json"}, timeout=60,
    )
    resp.raise_for_status()
    runtime = resp.json().get("clusterRuntimeConfig") or {}
    return {
        item["destination"]
        for item in runtime.get("initScripts") or []
        if isinstance(item, dict) and isinstance(item.get("destination"), str)
    }


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


def get_cluster_idle_timeout(cluster_id: str):
    """Return the cluster's current autoTerminationMinutes (int), or None if it
    can't be read. Read-only. Used by the per-task idle-timeout bump/restore in
    job_migrate so a cluster doesn't idle-terminate mid-task during long
    setup/think gaps."""
    signer = _get_signer()
    try:
        resp = http_requests.get(
            _cluster_url(cluster_id), auth=signer,
            headers={"Accept": "application/json"}, timeout=60)
        resp.raise_for_status()
        val = resp.json().get("autoTerminationMinutes")
        return int(val) if val is not None else None
    except Exception as e:  # noqa: BLE001 — read is advisory; caller no-ops on None
        print(f"[idle-timeout] WARN: could not read autoTerminationMinutes on "
              f"{cluster_id[:12]}...: {type(e).__name__}: {e}", flush=True)
        return None


def set_cluster_idle_timeout(cluster_id: str, minutes: int, *, attempts: int = 3) -> bool:
    """Best-effort set of the cluster's idle auto-termination to ``minutes``.

    Uses ``PUT /clusters/{key}`` — verified 2026-09-22 to be honored on a
    RUNNING cluster, applied immediately with no restart. Echoes the cluster's
    current writable config (displayName/description/runtime/driver/worker) so
    the PUT changes ONLY autoTerminationMinutes; a minimal body can reset the
    other config sub-objects. Verifies by GET and retries up to ``attempts``.

    NEVER raises. Idle-timeout tuning is a cost/robustness mitigation, and
    failing a migration over it would cost more than a stale timeout (operator
    decision 2026-09-22). Returns True only when the new value is confirmed;
    otherwise logs loudly and returns False so the caller can carry on.
    """
    signer = _get_signer()
    url = _cluster_url(cluster_id)
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            cur = http_requests.get(
                url, auth=signer, headers={"Accept": "application/json"}, timeout=60)
            cur.raise_for_status()
            d = cur.json()
            body = {
                "displayName": d.get("displayName"),
                "description": d.get("description"),
                "clusterRuntimeConfig": d.get("clusterRuntimeConfig"),
                "driverConfig": d.get("driverConfig"),
                "workerConfig": d.get("workerConfig"),
                "autoTerminationMinutes": int(minutes),
            }
            put = http_requests.put(
                url, auth=signer, headers={"Content-Type": "application/json"},
                json=body, timeout=120)
            if put.status_code >= 400:
                raise RuntimeError(f"PUT HTTP {put.status_code}: {put.text[:200]}")
            chk = http_requests.get(
                url, auth=signer, headers={"Accept": "application/json"}, timeout=60)
            chk.raise_for_status()
            got = chk.json().get("autoTerminationMinutes")
            if got is not None and int(got) == int(minutes):
                return True
            raise RuntimeError(f"post-PUT verify mismatch: got {got!r}, want {minutes}")
        except Exception as e:  # noqa: BLE001 — best-effort; never fatal (see docstring)
            last_err = e
            if attempt < attempts:
                time.sleep(2 * attempt)
    # Cannot assert the live value: the failure may be a pre-PUT error (value
    # unchanged) OR a post-PUT verify failure (the PUT applied, value IS changed).
    # Say "unconfirmed", never "unchanged" — the caller records the original and
    # restores regardless, so a PUT that did apply is still cleaned up.
    print(f"[idle-timeout] ERROR: could not confirm autoTerminationMinutes={minutes} on "
          f"{cluster_id[:12]}... after {attempts} attempts: "
          f"{type(last_err).__name__}: {last_err} — current value unconfirmed, the PUT "
          f"may or may not have applied (not fatal)", flush=True)
    return False


def _get_state_after_start_conflict(cluster_id: str) -> str:
    """Read live state after start HTTP 409, or fail without resubmitting.

    A failed reconciliation read cannot prove whether AIDP accepted a start or
    is still completing a shutdown/library operation.  Retrying the start POST
    in that state risks a duplicate or conflicting lifecycle request, so both
    the initial reconciliation and every shutdown-settle poll fail loudly with
    the same contextual ``RuntimeError``.
    """
    try:
        return get_cluster_state(cluster_id)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot reconcile cluster {cluster_id} after start HTTP 409: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


_monotonic = time.monotonic
_MAX_START_REENTRIES = 3


class _ClusterStartDeadlineExceeded(RuntimeError):
    """Start reconciliation exhausted the readiness caller's deadline."""

    def __init__(
        self,
        cluster_id: str,
        attempts: int,
        last_state: str | None,
    ) -> None:
        self.attempts = attempts
        self.last_state = last_state or "UNKNOWN"
        super().__init__(
            f"Cluster {cluster_id} start reconciliation exceeded the caller "
            f"timeout after {attempts} start attempts; last reconciled live "
            f"state: {self.last_state}."
        )


def _sleep_with_start_deadline(
    seconds: float,
    deadline: float | None,
    cluster_id: str,
    attempts: int = 0,
    last_state: str | None = None,
) -> None:
    """Sleep within the caller's start deadline, or fail at that deadline."""
    if deadline is None:
        time.sleep(seconds)
        return
    remaining = deadline - _monotonic()
    if remaining <= 0:
        raise _ClusterStartDeadlineExceeded(
            cluster_id, attempts, last_state
        )
    sleep_for = min(float(seconds), remaining)
    time.sleep(sleep_for)
    if sleep_for < seconds:
        raise _ClusterStartDeadlineExceeded(
            cluster_id, attempts, last_state
        )


def start_cluster(cluster_id: str, deadline: float | None = None):
    """Start a stopped cluster with bounded HTTP-409 reconciliation.

    Returns ``ACTIVE`` when reconciliation proves the cluster is already
    active, a member of ``START_PROGRESS_CLUSTER_STATES`` when a start is
    already in progress, or ``START_REQUESTED`` after a start POST is accepted.
    Raises ``RuntimeError`` immediately when reconciliation cannot be read,
    yields a terminal or unrecognised state, or an HTTP response is
    unrecoverable.

    Fix 5d (2026-07-25): retry HTTP 409 with exponential backoff.
    ─────────────────────────────────────────────────────────────
    Observed failure mode (a pilot-customer AutoRefit 2026-07-25):
      1. Migration pre-flight installs a baseline JAR via PATCH /libraries
      2. AIDP returns INSTALL_ON_RESTART (terminal for stopped cluster —
         the JAR will attach at next boot).
      3. Migration immediately calls POST /actions/start.
      4. AIDP returns HTTP 409:
           "Unable to accept request due to an ongoing operation on the
            cluster … Please check AsyncOperations to find the in-progress
            operation for the resource."
      Root cause: even though the /libraries call returned a terminal STATUS
      (INSTALL_ON_RESTART), AIDP's ASYNC OPERATIONS QUEUE still holds a
      lock on the cluster for a brief window after the PATCH. That lock
      blocks any new cluster-lifecycle action (start / stop / patch libraries)
      until it clears. Empirically the window is 30–60 seconds; occasionally
      longer if the async operation is queued behind unrelated work.

    Fix: after HTTP 409, reconcile the live cluster state before deciding
    whether to retry. ACTIVE is success, an in-progress state is handed back
    to the caller's poll loop, and a terminal state fails immediately. A
    shutdown-in-progress state is polled until it becomes startable; no new
    start POST is issued while shutdown is still in progress. Only an
    unresolved/startable state uses the bounded 30/60/120/240/480-second retry
    ladder. Non-409 errors propagate immediately (fail-loud).
    """
    signer = _get_signer()
    url = f"{_cluster_url(cluster_id)}/actions/start"

    # Retry ladder. 5 attempts after the initial call.  Shutdown reconciliation
    # has one shared ~15-minute budget for the whole invocation; it is never
    # reset by a later POST/409 cycle. Async readiness callers pass their
    # absolute deadline so these synchronous waits cannot outlive the
    # configured ensure_cluster_running timeout. Direct callers that omit a
    # deadline may use the full retry and shutdown-settle budgets.
    _RETRY_WAITS = (30, 60, 120, 240, 480)
    shutdown_settle_timeout = sum(_RETRY_WAITS)
    shutdown_settle_elapsed = 0

    resp = None
    skip_retry_wait = False
    attempts_made = 0
    last_reconciled_state = None
    for attempt, wait_before_retry in enumerate((0,) + _RETRY_WAITS):
        if wait_before_retry and not skip_retry_wait:
            print(
                f"[cluster-lifecycle] start_cluster HTTP 409 (concurrent op) — "
                f"retry in {wait_before_retry}s "
                f"(attempt {attempt}/{len(_RETRY_WAITS)})",
                flush=True,
            )
            _sleep_with_start_deadline(
                wait_before_retry,
                deadline,
                cluster_id,
                attempts_made,
                last_reconciled_state,
            )
        skip_retry_wait = False
        request_timeout = 60
        if deadline is not None:
            remaining = deadline - _monotonic()
            if remaining <= 0:
                raise _ClusterStartDeadlineExceeded(
                    cluster_id, attempts_made, last_reconciled_state
                )
            request_timeout = max(0.1, min(60, remaining))
        resp = http_requests.post(
            url, auth=signer, json={},
            headers={"Content-Type": "application/json"},
            timeout=request_timeout)
        attempts_made += 1
        if resp.status_code != 409:
            # Success (2xx) OR non-409 error — no retry
            break
        # A concurrent operation may already have accepted the start. Reconcile
        # live state before retrying so the operation remains idempotent.
        current_state = _get_state_after_start_conflict(cluster_id)
        last_reconciled_state = current_state
        if current_state == "ACTIVE":
            print(
                f"[cluster-lifecycle] Cluster {cluster_id[:12]}... is already "
                "ACTIVE after start HTTP 409; treating start as successful.",
                flush=True,
            )
            return current_state
        if current_state in TERMINAL_CLUSTER_STATES:
            raise RuntimeError(
                f"Cannot start cluster {cluster_id}: live state after HTTP 409 "
                f"is terminal ({current_state})."
            )
        if current_state in START_PROGRESS_CLUSTER_STATES:
            print(
                f"[cluster-lifecycle] Cluster {cluster_id[:12]}... is already "
                f"{current_state} after start HTTP 409; waiting for ACTIVE "
                "instead of resubmitting start.",
                flush=True,
            )
            return current_state
        if current_state in SHUTDOWN_CLUSTER_STATES:
            poll_interval = 30
            print(
                f"[cluster-lifecycle] Cluster {cluster_id[:12]}... is "
                f"{current_state} after start HTTP 409; waiting for shutdown "
                "to finish before resubmitting start.",
                flush=True,
            )
            while (
                current_state in SHUTDOWN_CLUSTER_STATES
                and shutdown_settle_elapsed < shutdown_settle_timeout
            ):
                _sleep_with_start_deadline(
                    poll_interval,
                    deadline,
                    cluster_id,
                    attempts_made,
                    last_reconciled_state,
                )
                shutdown_settle_elapsed += poll_interval
                current_state = _get_state_after_start_conflict(cluster_id)
                last_reconciled_state = current_state
                print(
                    f"[cluster-lifecycle] Cluster {cluster_id[:12]}...: "
                    f"{current_state} ({shutdown_settle_elapsed}s / "
                    f"{shutdown_settle_timeout}s, settling)",
                    flush=True,
                )
            if current_state in SHUTDOWN_CLUSTER_STATES:
                raise RuntimeError(
                    f"Cluster {cluster_id} remained in shutdown state "
                    f"{current_state} for {shutdown_settle_elapsed}s after "
                    "start HTTP 409."
                )
            if current_state in ACTIVE_CLUSTER_STATES:
                return current_state
            if current_state in START_PROGRESS_CLUSTER_STATES:
                return current_state
            if current_state in TERMINAL_CLUSTER_STATES:
                raise RuntimeError(
                    f"Cannot start cluster {cluster_id}: shutdown after HTTP 409 "
                    f"settled to terminal state ({current_state})."
                )
            if current_state not in STARTABLE_CLUSTER_STATES:
                raise RuntimeError(
                    f"Cannot start cluster {cluster_id}: shutdown after HTTP 409 "
                    f"settled to unexpected state ({current_state})."
                )
            # The cluster is now proven startable. Re-submit immediately rather
            # than applying the generic 409 backoff after we already waited for
            # shutdown to settle.
            skip_retry_wait = True
        elif current_state not in STARTABLE_CLUSTER_STATES:
            raise RuntimeError(
                f"Cannot start cluster {cluster_id}: live state after HTTP 409 "
                f"is unrecognised ({current_state}); refusing to resubmit start."
            )
        # 409 — try again if attempts remain (loop will pick next wait)

    if resp is None or resp.status_code >= 400:
        raise RuntimeError(
            f"Failed to start cluster {cluster_id}: "
            f"HTTP {resp.status_code if resp is not None else '(no response)'} — "
            f"{(resp.text[:300] if resp is not None else '')}; "
            f"after {attempts_made} start attempts; "
            f"last reconciled live state: {last_reconciled_state}")
    print(f"[cluster-lifecycle] Start request sent for {cluster_id[:12]}...", flush=True)
    return "START_REQUESTED"


async def ensure_cluster_running(
    cluster_id: str,
    timeout: int = 1800,
    allow_failed_recovery: bool = False,
):
    """Ensure cluster is ACTIVE. Start if needed, poll until ready.

    Args:
        cluster_id: AIDP cluster UUID
        timeout: max seconds to wait (default 30 min)
        allow_failed_recovery: submit one start request when the initial state
            is FAILED. This is reserved for Phase-1 readiness; if that start
            fails, no second recovery start is submitted.

    Raises:
        RuntimeError if cluster doesn't become ACTIVE within timeout.
    """
    state = get_cluster_state(cluster_id)
    name = get_cluster_display_name(cluster_id)
    print(f"[cluster-lifecycle] Cluster {name} ({cluster_id[:12]}...): {state}", flush=True)

    poll_interval = 30  # seconds
    elapsed = 0.0
    started_at = _monotonic()
    deadline = started_at + max(0, timeout)
    # HTTP 202 means the control plane accepted the start request, but the
    # lifecycle read can briefly remain STOPPED/INACTIVE before STARTING is
    # observable. Do not submit a duplicate start during that propagation
    # window. Re-start only after actual progress was observed and the cluster
    # subsequently settled back to a startable state.
    accepted_start_in_flight = False
    start_progress_observed = state in START_PROGRESS_CLUSTER_STATES
    last_progress_state = state if start_progress_observed else None
    start_requests = 0
    start_regressions = 0
    failed_recovery_attempted = False

    def _timeout_error(
        last_state: str, attempts: int | None = None
    ) -> RuntimeError:
        attempt_count = start_requests if attempts is None else attempts
        return RuntimeError(
            f"Cluster {cluster_id} did not become ACTIVE within "
            f"{timeout} seconds after {attempt_count} start attempts "
            f"(last state: {last_state})."
        )

    async def _sleep_within_timeout() -> bool:
        nonlocal elapsed
        remaining = min(timeout - elapsed, deadline - _monotonic())
        if remaining <= 0:
            return False
        sleep_for = min(poll_interval, remaining)
        await asyncio.sleep(sleep_for)
        elapsed += sleep_for
        elapsed = max(elapsed, _monotonic() - started_at)
        return True

    async def _start_within_timeout(last_state: str):
        nonlocal elapsed, start_requests
        remaining = deadline - _monotonic()
        if remaining <= 0:
            raise _timeout_error(last_state)
        start_requests += 1
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(start_cluster, cluster_id, deadline),
                timeout=remaining,
            )
        except _ClusterStartDeadlineExceeded as exc:
            raise _timeout_error(exc.last_state, exc.attempts) from exc
        except asyncio.TimeoutError as exc:
            raise _timeout_error(last_state) from exc
        finally:
            elapsed = max(elapsed, _monotonic() - started_at)

    # A cluster can be mid-shutdown when we arrive — e.g. idle auto-termination
    # fired during a stall. STOPPING/TERMINATING are TRANSIENT: we cannot start a
    # cluster that is still stopping, and we must NOT treat it as a fatal
    # "unexpected state" (the old behaviour, which hard-failed the job). Wait for
    # it to reach a settled state, then fall through to the normal start path
    # below. Shares the `elapsed` budget with the poll loop so total wait is
    # still bounded by `timeout`.
    if state in SHUTDOWN_CLUSTER_STATES:
        print(f"[cluster-lifecycle] Cluster {name} is {state} (mid-shutdown) — "
              f"waiting for it to fully stop, then starting...", flush=True)
        while elapsed < timeout:
            if not await _sleep_within_timeout():
                break
            state = get_cluster_state(cluster_id)
            mins = int(elapsed) // 60
            secs = int(elapsed) % 60
            print(f"[cluster-lifecycle] {name}: {state} ({mins}m{secs:02d}s / {timeout // 60}m, settling)", flush=True)
            # A concurrent start or auto-recovery can move the cluster from a
            # shutdown state directly into recognized start progress. Hand it
            # to the normal readiness loop below; only truly unknown states
            # should fail the shutdown-settle guard.
            if state in (
                ACTIVE_CLUSTER_STATES
                | STARTABLE_CLUSTER_STATES
                | START_PROGRESS_CLUSTER_STATES
            ):
                break
            if state in TERMINAL_CLUSTER_STATES:
                raise RuntimeError(
                    f"Cluster {cluster_id} entered state '{state}' during shutdown — cannot proceed.")
            if state not in SHUTDOWN_CLUSTER_STATES:
                raise RuntimeError(
                    f"Cluster {cluster_id} entered unexpected state '{state}' "
                    "during shutdown — cannot proceed."
                )

        if state in SHUTDOWN_CLUSTER_STATES:
            raise _timeout_error(state)

    if state in ACTIVE_CLUSTER_STATES:
        return

    if state in STARTABLE_CLUSTER_STATES:
        # start_cluster contains bounded synchronous HTTP-409 backoff. Keep
        # that retry from blocking readiness work on unrelated clusters.
        start_result = await _start_within_timeout(state)
        if start_result in ACTIVE_CLUSTER_STATES:
            return
        if start_result in START_PROGRESS_CLUSTER_STATES:
            state = start_result
            accepted_start_in_flight = False
            start_progress_observed = True
            last_progress_state = start_result
        elif start_result == "START_REQUESTED":
            accepted_start_in_flight = True
            start_progress_observed = False
    elif state == "FAILED" and allow_failed_recovery:
        print(
            f"[cluster-lifecycle] Cluster {name} is FAILED; submitting one "
            "recovery start request.",
            flush=True,
        )
        failed_recovery_attempted = True
        start_result = await _start_within_timeout(state)
        if start_result in ACTIVE_CLUSTER_STATES:
            return
        if start_result in START_PROGRESS_CLUSTER_STATES:
            state = start_result
            start_progress_observed = True
            last_progress_state = start_result
        elif start_result == "START_REQUESTED":
            accepted_start_in_flight = True
            start_progress_observed = False
    elif state in START_PROGRESS_CLUSTER_STATES:
        print(f"[cluster-lifecycle] Cluster is {state}, waiting...", flush=True)
    else:
        raise RuntimeError(
            f"Cluster {cluster_id} is in unexpected state '{state}' — "
            f"cannot auto-start. Please check the AIDP console.")

    # Poll until ACTIVE (continues from `elapsed`, which the settle loop above
    # may already have advanced).
    while elapsed < timeout:
        if not await _sleep_within_timeout():
            break
        state = get_cluster_state(cluster_id)
        mins = int(elapsed) // 60
        secs = int(elapsed) % 60
        print(f"[cluster-lifecycle] {name}: {state} ({mins}m{secs:02d}s / {timeout // 60}m)", flush=True)

        if state in ACTIVE_CLUSTER_STATES:
            print(f"[cluster-lifecycle] Cluster {name} is ACTIVE.", flush=True)
            return
        if (
            state == "FAILED"
            and failed_recovery_attempted
            and accepted_start_in_flight
            and not start_progress_observed
        ):
            print(
                f"[cluster-lifecycle] Cluster {name} still reports FAILED "
                "after the accepted recovery start request; waiting for "
                "lifecycle progress without submitting another start.",
                flush=True,
            )
            continue
        if state in TERMINAL_CLUSTER_STATES:
            raise RuntimeError(
                f"Cluster {cluster_id} entered state '{state}' — cannot proceed.")

        if state in START_PROGRESS_CLUSTER_STATES:
            accepted_start_in_flight = False
            start_progress_observed = True
            last_progress_state = state
            continue

        if state in STARTABLE_CLUSTER_STATES:
            if failed_recovery_attempted and start_progress_observed:
                raise RuntimeError(
                    f"Cluster {cluster_id} regressed to {state} after the "
                    "single FAILED-state recovery start; refusing another "
                    "start request."
                )
            if accepted_start_in_flight and not start_progress_observed:
                print(
                    f"[cluster-lifecycle] Cluster {name} still reports {state} "
                    "after an accepted start request; waiting for lifecycle "
                    "progress instead of submitting a duplicate start.",
                    flush=True,
                )
                continue
            start_regressions += 1
            if start_regressions > _MAX_START_REENTRIES:
                raise RuntimeError(
                    f"Cluster {cluster_id} start regressed from "
                    f"{last_progress_state or 'progress'} to {state} "
                    f"{start_regressions} times after {start_requests} start "
                    "requests; refusing further starts."
                )
            print(
                f"[cluster-lifecycle] Cluster {name} settled back to {state}; "
                "re-entering the start path.",
                flush=True,
            )
            start_result = await _start_within_timeout(state)
            if start_result in ACTIVE_CLUSTER_STATES:
                return
            if start_result in START_PROGRESS_CLUSTER_STATES:
                state = start_result
                accepted_start_in_flight = False
                start_progress_observed = True
                last_progress_state = start_result
            elif start_result == "START_REQUESTED":
                accepted_start_in_flight = True
                start_progress_observed = False
            continue

        if state in SHUTDOWN_CLUSTER_STATES:
            continue

        raise RuntimeError(
            f"Cluster {cluster_id} entered unexpected state '{state}' while "
            "waiting for ACTIVE — cannot proceed."
        )

    raise _timeout_error(state)


# ─── Library Management ────────────────────────────────────────────────


def get_installed_libraries(cluster_id: str) -> list:
    """List libraries installed on the cluster."""
    signer = _get_signer()
    resp = http_requests.get(
        _libraries_url(cluster_id), auth=signer,
        headers={"Accept": "application/json"})
    resp.raise_for_status()
    return resp.json().get("items", [])


def _attached_library_rows(cluster_id: str) -> list:
    """Current registrations, excluding completed-deletion history.

    PR #90 review 5694086105: DELETED rows can coexist with live registrations
    for the same path. They must neither win last-row dictionaries nor keep
    polling/install gates busy. DELETING and all unknown states remain visible.
    Keep the raw listing API unchanged for diagnostics and other consumers.
    """
    return [row for row in get_installed_libraries(cluster_id)
            if str(row.get("status") or "UNKNOWN").upper() != "DELETED"]


def install_library(cluster_id: str, workspace_path: str):
    """Install a workspace file as a cluster library.

    AIDP can briefly return HTTP 409 after a cluster lifecycle or preceding
    library operation completes. Retry only that transient conflict; all other
    errors remain fail-fast.
    """
    signer = _get_signer()
    body = {
        "items": [{
            "operation": "INSTALL",
            "type": "WORKSPACE_FILE",
            "path": workspace_path,
        }]
    }
    retry_waits = (2, 4, 8, 16, 30)
    resp = None
    for attempt, wait_before_retry in enumerate((0,) + retry_waits):
        if wait_before_retry:
            print(
                f"[cluster-lifecycle] install_library HTTP 409 for "
                f"{cluster_id[:12]}...; retrying in {wait_before_retry}s "
                f"(attempt {attempt}/{len(retry_waits)})",
                flush=True,
            )
            time.sleep(wait_before_retry)
        resp = http_requests.patch(
            _libraries_url(cluster_id), auth=signer,
            json=body, headers={"Content-Type": "application/json"})
        if resp.status_code != 409:
            break
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Failed to install {workspace_path} on cluster {cluster_id}: "
            f"HTTP {resp.status_code} — {resp.text[:300]}")
    return resp.json()


async def install_library_async(cluster_id: str, workspace_path: str):
    """Async facade for the blocking library PATCH/retry implementation.

    AIDP permits only one library mutation at a time on a given cluster, so
    callers still await each install serially.  Moving the synchronous HTTP
    call and its 409 backoff sleeps to a worker thread prevents one cluster's
    retry window from blocking readiness work for every other cluster.
    """
    return await asyncio.to_thread(install_library, cluster_id, workspace_path)


async def ensure_aidp_compat_installed(cluster_id: str, timeout: int = 600):
    """Check if aidp_compat is installed on the cluster. Install if missing.

    Args:
        cluster_id: AIDP cluster UUID
        timeout: max seconds to wait for install (default 5 min)
    """
    libs = _attached_library_rows(cluster_id)
    matched_libs = [lib for lib in libs if _is_aidp_compat_lib(lib.get("path", ""))]
    # When multiple aidp_compat versions are listed (e.g. an old FAILED entry
    # alongside a new INSTALLED one after a manual upgrade), prefer the
    # INSTALLED/RESOLVED one so we don't clobber a working install.
    healthy = next((l for l in matched_libs if l.get("status", "") in ("INSTALLED", "RESOLVED")), None)
    pending = next((l for l in matched_libs if l.get("status", "") not in ("INSTALLED", "RESOLVED", "FAILED", "SKIPPED")), None)
    failed = next((l for l in matched_libs if l.get("status", "") in ("FAILED", "SKIPPED")), None)

    if healthy is not None:
        print(f"[cluster-lifecycle] aidp_compat already installed ({healthy.get('path','')}) on {cluster_id[:12]}...", flush=True)
        return False

    queued = next(
        (l for l in matched_libs if l.get("status", "") == "INSTALL_ON_RESTART"),
        None,
    )
    if queued is not None:
        print(
            f"[cluster-lifecycle] aidp_compat ({queued.get('path','')}) is "
            f"queued for restart on {cluster_id[:12]}...",
            flush=True,
        )
        return True

    # An install (or re-install) is needed — resolve the uploaded wheel
    # version-agnostically. If it isn't there, this is a missing PREREQUISITE.
    whl = resolve_aidp_compat_whl()
    if whl is None:
        raise RuntimeError(
            f"PREREQUISITE NOT MET: expected {TARGET_WHEEL_PATH} not found in "
            f"{AIDP_LIB_DIR} on this workspace. Build the wheel "
            f"(cd aidp_compat && python setup.py bdist_wheel) and upload the "
            f"aidp_compat-{AIDP_COMPAT_VERSION}-py3-none-any.whl to "
            f"{AIDP_LIB_DIR} before migrating, then retry.")

    if pending is not None:
        # PENDING / INSTALLING — let the poll loop wait for it
        print(f"[cluster-lifecycle] aidp_compat ({pending.get('path','')}) status is {pending.get('status','')}, waiting...", flush=True)
    elif failed is not None:
        print(f"[cluster-lifecycle] aidp_compat ({failed.get('path','')}) status is {failed.get('status','')}, re-installing from {whl}...", flush=True)
        await install_library_async(cluster_id, whl)
    else:
        print(f"[cluster-lifecycle] aidp_compat not found on {cluster_id[:12]}..., installing from {whl}...", flush=True)
        await install_library_async(cluster_id, whl)

    # Poll until installed
    poll_interval = 15
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        libs = _attached_library_rows(cluster_id)
        matched_libs = [lib for lib in libs if _is_aidp_compat_lib(lib.get("path", ""))]
        healthy = next((l for l in matched_libs if l.get("status", "") in ("INSTALLED", "RESOLVED")), None)
        pending = next((l for l in matched_libs if l.get("status", "") not in ("INSTALLED", "RESOLVED", "FAILED", "SKIPPED")), None)
        failed = next((l for l in matched_libs if l.get("status", "") in ("FAILED", "SKIPPED")), None)
        if healthy is not None:
            print(f"[cluster-lifecycle] aidp_compat installed successfully ({healthy.get('path','')}).", flush=True)
            return False
        queued = next(
            (l for l in matched_libs if l.get("status", "") == "INSTALL_ON_RESTART"),
            None,
        )
        if queued is not None:
            print(
                f"[cluster-lifecycle] aidp_compat queued for cluster restart "
                f"({queued.get('path','')}).",
                flush=True,
            )
            return True
        elif pending is not None:
            print(f"[cluster-lifecycle] aidp_compat ({pending.get('path','')}): {pending.get('status','')} ({elapsed}s / {timeout}s)", flush=True)
        elif failed is not None:
            msg = failed.get("stateMessage", "")
            raise RuntimeError(
                f"aidp_compat install FAILED on cluster {cluster_id} ({failed.get('path','')}): {msg}")
        else:
            # Library disappeared from list — re-install
            print(f"[cluster-lifecycle] aidp_compat disappeared, re-installing from {whl}...", flush=True)
            await install_library_async(cluster_id, whl)

    print(f"[cluster-lifecycle] WARNING: aidp_compat install did not confirm "
          f"within {timeout}s. Proceeding anyway — bootstrap import will verify.", flush=True)
    return False


# ─── Baseline JARs ────────────────────────────────────────────────────

# Generic Spark-side JARs the migration tool needs on every cluster.
# Not customer-specific — hudi is the standard bundle, scala-logging is a
# widely-used logging shim. Per-job JARs are handled separately by dbx_export
# at cluster-provision time (reads the source DBX job's cluster spec).
_BASELINE_JAR_BASENAMES = [
    "hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar",
    "scala-logging_2.12-3.9.5.jar",
]
BASELINE_JARS = [f"{AIDP_LIB_DIR}/{b}" for b in _BASELINE_JAR_BASENAMES]


def configure(config_path: str = None) -> None:
    """Re-resolve the dependency repository from a caller's explicit --config
    and recompute every constant derived from it. Callers that accept --config
    (job_migrate, job_migrate_from_workflow, install/deploy/setup scripts) MUST
    call this right where they propagate AIDP coordinates into this module —
    import-time resolution only sees $AIDP_CONFIG_PATH / default discovery, not
    a CLI path."""
    global AIDP_LIB_DIR, TARGET_WHEEL_PATH, AIDP_COMPAT_PREFIX, BASELINE_JARS
    AIDP_LIB_DIR = _resolve_lib_dir(config_path)
    TARGET_WHEEL_PATH = (
        f"{AIDP_LIB_DIR}/aidp_compat-{AIDP_COMPAT_VERSION}-py3-none-any.whl"
        if AIDP_COMPAT_VERSION else None
    )
    AIDP_COMPAT_PREFIX = f"{AIDP_LIB_DIR}/aidp_compat-"
    BASELINE_JARS = [f"{AIDP_LIB_DIR}/{b}" for b in _BASELINE_JAR_BASENAMES]


def _artifact_alias_dirs() -> set:
    """The exact directories between which same-basename aliasing is licensed.

    Derived from the CONFIGURED repository dir: the current lib dir plus its
    known legacy sibling (…/jars) when the configured dir is named …/libs —
    the verified relocation pair. Exotic configurations get NO legacy sibling
    and therefore no cross-directory aliasing (fail toward installing).
    """
    lib_dir = AIDP_LIB_DIR.rstrip("/")
    dirs = {lib_dir}
    parent, _, base = lib_dir.rpartition("/")
    if base == "libs" and parent:
        dirs.add(parent + "/jars")
    return dirs


def _find_artifact_by_basename(current: dict, artifact_path: str):
    """Same-basename fallback for versioned binary artifacts (.jar/.whl).

    The dependency repository moved /migration-dependencies/jars → /libs
    (PR #77 rename); clusters provisioned before the rename carry the SAME
    jar registered under the OLD path. Exact-path lookups then treat the jar
    as absent, re-install it under the new path, and the duplicate lands in
    INSTALL_ON_RESTART on an active cluster → loud false failure (incident:
    cluster c6364486 / workflow c68de7fd, 2026-09-09). Our jar/wheel
    filenames carry version(+hash) identity (e.g. …-0.15.0-3975ca4.jar), so
    basename equality is content-safe for these types. NEVER applied to
    requirements/text paths — distinct requirements.txt files legitimately
    share a basename.

    Returns (matched_path, value) or (None, None).
    """
    base = os.path.basename(artifact_path)
    if not base.endswith((".jar", ".whl")):
        return None, None
    # SCOPE: aliasing is licensed ONLY by the verified relocation incident —
    # the tool-owned dependency repository moving …/jars → …/libs. Filename
    # equality does NOT establish content identity for arbitrary workload
    # libraries (/team_a/model_udfs.jar ≠ /team_b/model_udfs.jar — PR #88
    # review P1, 2026-09-10), and the trusted set is EXACT directories, never
    # a parent prefix: deriving parent(AIDP_LIB_DIR) trusted the whole
    # workspace for a shallow configured dir like /Workspace/libs (review
    # round 4). The set is derived from the CONFIGURED lib dir so custom
    # paths.lib_repository_dir values keep both consumers in agreement —
    # keep in sync with dbx_export/aidp_client.artifact_alias_dirs().
    alias_dirs = _artifact_alias_dirs()
    if os.path.dirname(artifact_path.rstrip("/")) not in alias_dirs:
        return None, None
    # BEST-ranked across ALL same-basename rows (loaded > staged > transient
    # > rest) — never first-in-iteration-order: a FAILED row in one directory
    # beside an INSTALLED row in another must deterministically resolve to
    # the INSTALLED one (PR #88 review minor).
    rank = {"INSTALLED": 0, "RESOLVED": 0, "INSTALL_ON_RESTART": 1,
            "PENDING": 2, "RESOLVING": 2, "INSTALLING": 2}
    def _status_of(v):
        return str((v.get("status") if isinstance(v, dict) else v) or "")
    matches = [(path, value) for path, value in current.items()
               if path and path != artifact_path
               and os.path.dirname(path.rstrip("/")) in alias_dirs
               and os.path.basename(path) == base]
    if not matches:
        return None, None
    return min(matches, key=lambda pv: rank.get(_status_of(pv[1]), 3))


def _best_artifact_registration(current: dict, artifact_path: str):
    """Best (path, status) across ALL registrations of the same artifact.

    A cluster can carry the exact-path entry AND a same-basename entry from
    the other repository path simultaneously (the incident cluster held the
    legacy /jars entry INSTALLED next to the /libs duplicate staged
    INSTALL_ON_RESTART). What matters for readiness is the BEST state any
    registration reached — a loaded legacy entry satisfies the jar even while
    a duplicate sits staged. Precedence: loaded > staged > transient > rest.
    """
    candidates = []
    exact = current.get(artifact_path)
    if exact is not None:
        candidates.append((artifact_path, exact))
    legacy_path, legacy = _find_artifact_by_basename(current, artifact_path)
    if legacy is not None:
        candidates.append((legacy_path, legacy))
    if not candidates:
        return None, None
    rank = {"INSTALLED": 0, "RESOLVED": 0, "INSTALL_ON_RESTART": 1,
            "PENDING": 2, "RESOLVING": 2}
    def _status(v):
        return str((v.get("status") if isinstance(v, dict) else v) or "")
    return min(candidates, key=lambda c: rank.get(_status(c[1]), 3))


async def _poll_jar_terminal(cluster_id: str, jar_path: str, timeout: int) -> str:
    """Poll a JAR install to terminal. Unlike _poll_library_install (used for
    pip requirements files, where INSTALL_ON_RESTART never appears), a JAR PATCH
    install on a running cluster lands in INSTALL_ON_RESTART — which is a
    legitimate success terminal (file staged, will load on next cluster start).

    Returns: "loaded" (INSTALLED/RESOLVED), "queued" (INSTALL_ON_RESTART),
             "failed" (FAILED), or "timeout" (no terminal within timeout).
    """
    elapsed, poll = 0, 10
    while elapsed < timeout:
        await asyncio.sleep(poll)
        elapsed += poll
        for lib in _attached_library_rows(cluster_id):
            if lib.get("path") == jar_path:
                st = lib.get("status", "")
                if st in ("INSTALLED", "RESOLVED"):
                    return "loaded"
                if st == "INSTALL_ON_RESTART":
                    return "queued"
                if st == "FAILED":
                    return "failed"
                # PENDING / RESOLVING → keep polling
                break
    return "timeout"


def _log_baseline_jar_outcome(jar_path: str, result: str, cluster_was_active: bool):
    """Log the terminal outcome of a baseline JAR install, with a loud WARN if
    the JAR ended in INSTALL_ON_RESTART on an already-active cluster (Spark JVM
    classpath won't pick it up until the cluster restarts)."""
    basename = os.path.basename(jar_path)
    if result == "loaded":
        print(f"[cluster-lifecycle] baseline JAR {basename} loaded on classpath", flush=True)
        return
    # result == "queued" (INSTALL_ON_RESTART)
    if cluster_was_active:
        print(f"[cluster-lifecycle] WARNING: baseline JAR {basename} is "
              f"INSTALL_ON_RESTART on an already-active cluster. Spark JVM "
              f"classpath will NOT pick it up until the cluster is restarted. "
              f"Any code that imports hudi / scala-logging classes will fail "
              f"until then.", flush=True)
    else:
        print(f"[cluster-lifecycle] baseline JAR {basename} staged for next "
              f"cluster start (INSTALL_ON_RESTART) — will load automatically.",
              flush=True)


async def ensure_baseline_jars_installed(cluster_id: str, timeout: int = 600):
    """Ensure every BASELINE_JAR is attached to the cluster's library list.

    MUST run BEFORE ensure_cluster_running so that on a stopped cluster the
    JARs are queued (INSTALL_ON_RESTART) and get picked up during startup —
    landing on the JVM classpath. Installing after start leaves them in
    INSTALL_ON_RESTART indefinitely, requiring a manual cluster restart.

    Per-JAR behavior by current status on the cluster:
      - INSTALLED / RESOLVED / INSTALL_ON_RESTART → skip (already present)
      - PENDING / RESOLVING → wait for terminal (another install in flight)
      - FAILED or absent → install PATCH, then wait for terminal

    Terminal success states for a JAR install:
      - INSTALLED / RESOLVED → loaded on JVM classpath (cluster active + started
        after install, or cluster started fresh with the JAR queued)
      - INSTALL_ON_RESTART → staged; loads on next cluster start

    If a baseline JAR ends in INSTALL_ON_RESTART while the cluster was already
    ACTIVE, return True so the caller can restart the cluster before opening an
    execution session. Returning False means no restart is required.

    Fails loudly (RuntimeError) if a baseline JAR file is missing from the
    workspace — PREREQUISITE, not something to silently skip.
    """
    HEALTHY   = ("INSTALLED", "RESOLVED", "INSTALL_ON_RESTART")
    TRANSIENT = ("PENDING", "RESOLVING")

    # Snapshot state BEFORE any install so we can distinguish
    # "will load on start" (stopped) from "needs restart to load" (active).
    try:
        pre_state = get_cluster_state(cluster_id)
    except Exception:
        pre_state = "UNKNOWN"
    cluster_was_active = pre_state == "ACTIVE"
    restart_required = False

    current = {lib.get("path"): lib.get("status", "")
               for lib in _attached_library_rows(cluster_id)}

    for jar_path in BASELINE_JARS:
        basename = os.path.basename(jar_path)
        # Best registration across the exact path AND any same-basename entry
        # from the other repository path (…/jars vs …/libs rename): a loaded
        # legacy entry satisfies the jar — re-installing it under the new
        # path just duplicates into INSTALL_ON_RESTART, and a staged
        # duplicate must not demand a restart while a loaded entry exists.
        effective_path, status = _best_artifact_registration(current, jar_path)
        if effective_path is None:
            effective_path = jar_path
        elif effective_path != jar_path:
            print(f"[cluster-lifecycle] baseline JAR {basename} satisfied by "
                  f"existing registration at {effective_path} "
                  f"(status={status}) — not re-installing under "
                  f"{os.path.dirname(jar_path)}", flush=True)

        if status in HEALTHY:
            if status == "INSTALL_ON_RESTART" and cluster_was_active:
                restart_required = True
                _log_baseline_jar_outcome(effective_path, "queued", True)
            continue

        if status in TRANSIENT:
            # Another install is in flight — don't PATCH again; just wait.
            print(f"[cluster-lifecycle] baseline JAR {basename} in {status}, "
                  f"waiting for terminal...", flush=True)
            result = await _poll_jar_terminal(cluster_id, effective_path, timeout)
        else:
            # FAILED or absent → verify the source file exists, then install.
            parent = os.path.dirname(jar_path)
            listing = {it.get("path") for it in _list_workspace_objects(parent)}
            if jar_path not in listing:
                raise RuntimeError(
                    f"PREREQUISITE NOT MET: baseline JAR {basename} is not present "
                    f"at {parent} on this workspace. Upload it (e.g. via "
                    f"scripts/setup_migration_cluster.py) before migrating.")
            print(f"[cluster-lifecycle] baseline JAR {basename} not on "
                  f"{cluster_id[:12]}..., installing "
                  f"(pre-install cluster state: {pre_state})...", flush=True)
            await install_library_async(cluster_id, jar_path)
            result = await _poll_jar_terminal(cluster_id, jar_path, timeout)

        if result == "failed":
            raise RuntimeError(
                f"baseline JAR install FAILED on cluster {cluster_id}: {jar_path}")
        if result == "timeout":
            raise RuntimeError(
                f"baseline JAR install did not reach terminal within {timeout}s "
                f"on cluster {cluster_id}: {jar_path}")

        _log_baseline_jar_outcome(jar_path, result, cluster_was_active)
        if result == "queued" and cluster_was_active:
            restart_required = True

    return restart_required


async def _wait_for_cluster_state(
    cluster_id: str,
    desired_states: set[str],
    timeout: int = 1800,
    poll_interval: int = 20,
) -> str:
    """Wait for a cluster to enter one of the requested lifecycle states."""
    desired = {str(state).upper() for state in desired_states}
    elapsed = 0
    while elapsed <= timeout:
        state = get_cluster_state(cluster_id)
        if state in desired:
            return state
        if state in TERMINAL_CLUSTER_STATES:
            raise RuntimeError(
                f"Cluster {cluster_id} entered terminal state {state}."
            )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    raise RuntimeError(
        f"Cluster {cluster_id} did not reach {sorted(desired)} within "
        f"{timeout}s."
    )


async def restart_cluster_and_wait(cluster_id: str, timeout: int = 1800):
    """Stop an ACTIVE target cluster, start it again, and wait for ACTIVE.

    HTTP 409 is retried because a preceding library PATCH can leave a short
    asynchronous-operation lock even after the library status becomes
    INSTALL_ON_RESTART.
    """
    state = get_cluster_state(cluster_id)
    if state != "ACTIVE":
        await ensure_cluster_running(cluster_id, timeout=timeout)
        state = get_cluster_state(cluster_id)
    if state != "ACTIVE":
        raise RuntimeError(
            f"Cluster {cluster_id} is {state}; unable to perform required restart."
        )

    url = f"{_cluster_url(cluster_id)}/actions/stop"
    signer = _get_signer()
    response = None
    retry_waits = (0, 15, 30, 60, 60, 60)
    for attempt, wait_seconds in enumerate(retry_waits):
        if wait_seconds:
            print(
                f"[cluster-lifecycle] stop_cluster HTTP 409 (concurrent op) - "
                f"retry in {wait_seconds}s (attempt {attempt}/{len(retry_waits) - 1})",
                flush=True,
            )
            await asyncio.sleep(wait_seconds)
        response = http_requests.post(
            url,
            auth=signer,
            json={},
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        if response.status_code != 409:
            break

    if response is None or response.status_code not in (200, 201, 202, 204):
        raise RuntimeError(
            f"Failed to stop cluster {cluster_id}: HTTP "
            f"{response.status_code if response is not None else '(no response)'} - "
            f"{response.text[:300] if response is not None else ''}"
        )

    print(
        f"[cluster-lifecycle] Stop request sent for {cluster_id[:12]}...; "
        "waiting for INACTIVE.",
        flush=True,
    )
    await _wait_for_cluster_state(
        cluster_id, STARTABLE_CLUSTER_STATES, timeout=timeout
    )
    await ensure_cluster_running(cluster_id, timeout=timeout)


async def wait_for_attached_jars_to_settle(
    cluster_id: str, timeout: int = 600
) -> list[str]:
    """Wait for attached JAR operations and return paths queued for restart."""
    elapsed = 0
    poll_interval = 15
    transient_states = ("PENDING", "RESOLVING", "INSTALLING")
    while elapsed <= timeout:
        jars = [
            lib for lib in _attached_library_rows(cluster_id)
            if str(lib.get("path", "")).lower().endswith(".jar")
        ]
        # A FAILED/SKIPPED row whose SAME artifact (basename) is loaded under
        # another repository path is inert residue of the /jars→/libs rename
        # (e.g. a duplicate install that failed while the legacy copy serves
        # the classpath) — warn, don't raise: raising here wedged the cluster
        # forever, since the export path correctly refuses to re-PATCH a
        # basename-satisfied artifact (adversarial-verify finding, 2026-09-10).
        _all = {str(lib.get("path")): str(lib.get("status", "")) for lib in jars}
        failed = []
        for lib in jars:
            if str(lib.get("status", "")) not in ("FAILED", "SKIPPED"):
                continue
            path = str(lib.get("path"))
            _bp, _best = _best_artifact_registration(_all, path)
            _bs = str((_best.get("status") if isinstance(_best, dict) else _best) or "")
            if _bp != path and _bs in ("INSTALLED", "RESOLVED"):
                print(f"[cluster-lifecycle] WARN: {os.path.basename(path)} row is "
                      f"{lib.get('status')} at {path}, but the artifact is loaded "
                      f"via {_bp} — treating the failed row as inert (delete it "
                      "manually to silence this)", flush=True)
                continue
            failed.append((path, lib.get("status"), lib.get("stateMessage", "")))
        if failed:
            raise RuntimeError(
                f"Attached JAR installation failed on {cluster_id}: {failed}"
            )
        transient = [
            (lib.get("path"), lib.get("status"))
            for lib in jars
            if str(lib.get("status", "")) in transient_states
        ]
        if not transient:
            # Path-identity aware (PR #88 review, major 1): a staged
            # INSTALL_ON_RESTART row whose SAME artifact (basename) is already
            # INSTALLED/RESOLVED under another repository path (/jars vs /libs
            # rename) is an inert duplicate — the classpath already has the
            # jar, so it must NOT feed restart_required at the mainline entry
            # gate (ensure_cluster_ready_for_execution ORs this list into the
            # restart decision; the incident cluster demanded a restart
            # forever otherwise).
            current = {str(lib.get("path")): str(lib.get("status", ""))
                       for lib in jars}
            queued = []
            for lib in jars:
                if str(lib.get("status", "")) != "INSTALL_ON_RESTART":
                    continue
                path = str(lib.get("path"))
                _bp, _best = _best_artifact_registration(current, path)
                _best_status = str((_best.get("status") if isinstance(_best, dict) else _best) or "")
                if _bp != path and _best_status in ("INSTALLED", "RESOLVED"):
                    print(f"[cluster-lifecycle] staged {os.path.basename(path)} is an "
                          f"inert duplicate of loaded {_bp} — not counting toward "
                          "restart", flush=True)
                    continue
                queued.append(path)
            return queued
        if elapsed >= timeout:
            raise RuntimeError(
                f"Attached JAR installs did not settle within {timeout}s on "
                f"{cluster_id}: {transient}"
            )
        print(
            f"[cluster-lifecycle] Waiting for attached JAR installs on "
            f"{cluster_id[:12]}...: {transient}",
            flush=True,
        )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    raise RuntimeError(
        f"Attached JAR installs did not settle within {timeout}s on "
        f"{cluster_id}: {transient}"
    )


async def wait_for_runtime_requirements_to_settle(
    cluster_id: str, timeout: int = 600
) -> list[str]:
    """Wait for runtime requirements and return paths queued for restart."""
    elapsed = 0
    poll_interval = 15
    transient_states = ("PENDING", "RESOLVING", "INSTALLING")
    while elapsed <= timeout:
        requirements = [
            lib for lib in _attached_library_rows(cluster_id)
            if str(lib.get("path", "")) == TARGET_RUNTIME_REQUIREMENTS_PATH
            or _is_canonical_requirements_path(lib.get("path", ""))
        ]
        failed = [
            (lib.get("path"), lib.get("status"), lib.get("stateMessage", ""))
            for lib in requirements
            if str(lib.get("status", "")) in ("FAILED", "SKIPPED")
        ]
        if failed:
            raise RuntimeError(
                f"Runtime requirements installation failed on {cluster_id}: {failed}"
            )
        transient = [
            (lib.get("path"), lib.get("status"))
            for lib in requirements
            if str(lib.get("status", "")) in transient_states
        ]
        if not transient:
            return [
                str(lib.get("path"))
                for lib in requirements
                if str(lib.get("status", "")) == "INSTALL_ON_RESTART"
            ]
        if elapsed >= timeout:
            raise RuntimeError(
                f"Runtime requirements did not settle within {timeout}s on "
                f"{cluster_id}: {transient}"
            )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    raise RuntimeError(
        f"Runtime requirements did not settle within {timeout}s on "
        f"{cluster_id}: {transient}"
    )


async def verify_baseline_jars_loaded(
    cluster_id: str,
    timeout: int = 600,
    additional_paths: list[str] | None = None,
):
    """Require baseline and newly queued job JARs to be loaded."""
    required_paths = list(dict.fromkeys([
        *BASELINE_JARS,
        *(additional_paths or []),
    ]))
    elapsed = 0
    poll_interval = 15
    while elapsed <= timeout:
        current = {
            lib.get("path"): lib
            for lib in _attached_library_rows(cluster_id)
        }
        failures = []
        waiting = []
        for jar_path in required_paths:
            # Best registration across exact + same-basename paths: a loaded
            # legacy entry satisfies the jar even while a duplicate sits
            # staged INSTALL_ON_RESTART (see _best_artifact_registration).
            _bp, item = _best_artifact_registration(current, jar_path)
            status = str((item or {}).get("status", "MISSING"))
            if status in ("FAILED", "SKIPPED", "MISSING"):
                failures.append((jar_path, status, (item or {}).get("stateMessage", "")))
            elif status not in ("INSTALLED", "RESOLVED"):
                waiting.append((jar_path, status))

        if failures:
            raise RuntimeError(
                f"Required baseline JARs are unavailable on {cluster_id}: {failures}"
            )
        if not waiting:
            print(
                f"[cluster-lifecycle] All baseline JARs are loaded on "
                f"{cluster_id[:12]}...",
                flush=True,
            )
            return
        if elapsed >= timeout:
            raise RuntimeError(
                f"Baseline JARs did not become INSTALLED/RESOLVED within "
                f"{timeout}s on {cluster_id}: {waiting}"
            )
        print(
            f"[cluster-lifecycle] Waiting for baseline JARs to load on "
            f"{cluster_id[:12]}...: {waiting}",
            flush=True,
        )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    raise RuntimeError(
        f"Baseline JARs did not become INSTALLED/RESOLVED within "
        f"{timeout}s on {cluster_id}: {waiting}"
    )


async def ensure_cluster_ready_for_execution(
    cluster_id: str,
    cluster_timeout: int = 1800,
    library_timeout: int = 600,
    allow_fresh_cluster_restart: bool = False,
    additional_restart_paths: list[str] | None = None,
):
    """Stage baseline JARs, restart if required, and verify classpath readiness.

    Restarting the cluster that is executing this Python process would terminate
    the migration runner. AIDP exposes that cluster as COMPUTE_CLUSTER_KEY, so
    this condition fails fast with an actionable message instead.

    ``allow_fresh_cluster_restart`` is reserved for a caller that observed the
    target in CREATING/STARTING during the same operation. It permits the one
    restart needed to activate newly staged libraries without weakening the
    opt-in guard for clusters that were already active.
    """
    baseline_restart_required = await ensure_baseline_jars_installed(
        cluster_id, timeout=library_timeout
    )
    queued_jars = await wait_for_attached_jars_to_settle(
        cluster_id, timeout=library_timeout
    )
    queued_requirements = await wait_for_runtime_requirements_to_settle(
        cluster_id, timeout=library_timeout
    )
    pre_execution_state = get_cluster_state(cluster_id)
    # INSTALL_ON_RESTART on an already stopped cluster is expected: the next
    # ordinary start loads the libraries. Only an ACTIVE JVM needs a stop/start cycle.
    queued_restart_paths = list(dict.fromkeys([
        *queued_jars,
        *queued_requirements,
        *(additional_restart_paths or []),
    ]))
    restart_required = baseline_restart_required or (
        bool(queued_restart_paths) and pre_execution_state == "ACTIVE"
    )
    current_cluster = str(os.environ.get("COMPUTE_CLUSTER_KEY", "")).strip()
    if restart_required:
        if current_cluster and current_cluster == cluster_id:
            raise RuntimeError(
                f"One or more attached libraries on current control cluster "
                f"{cluster_id} are INSTALL_ON_RESTART. This workflow cannot "
                "restart its own "
                "cluster safely. Restart the cluster externally and rerun "
                "readiness, or execute readiness from a separate control cluster."
            )
        allow_shared_restart = str(
            os.environ.get("AIDP_ALLOW_SHARED_CLUSTER_RESTART", "")
        ).strip().lower() in {"1", "true", "yes"}
        if (
            pre_execution_state == "ACTIVE"
            and not allow_fresh_cluster_restart
            and not allow_shared_restart
        ):
            raise RuntimeError(
                f"Target cluster {cluster_id} is ACTIVE and attached libraries require "
                "a restart. Automatic restart is disabled to protect other active "
                "sessions. Confirm the cluster is not shared, then set "
                "AIDP_ALLOW_SHARED_CLUSTER_RESTART=1 and rerun readiness."
            )
        print(
            f"[cluster-lifecycle] Attached libraries require restart on target "
            f"cluster {cluster_id[:12]}...",
            flush=True,
        )
        await restart_cluster_and_wait(cluster_id, timeout=cluster_timeout)
        restarted = True
    else:
        await ensure_cluster_running(cluster_id, timeout=cluster_timeout)
        restarted = False

    await verify_baseline_jars_loaded(
        cluster_id,
        timeout=library_timeout,
        additional_paths=queued_restart_paths,
    )
    return restarted


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
        # Network down — a curated mapping is authoritative and used as-is; an
        # UNMAPPED name stays blocked (unreachability of the laptop-side check
        # says nothing about what cluster-side pip would fetch).
        if mapped is None:
            print(f"[cluster-lifecycle] WARNING: import '{import_name}' has no curated pip "
                  f"mapping and PyPI is unreachable for validation — NOT installed; add it "
                  f"to _IMPORT_TO_PIP or install it via the cluster libraries API", flush=True)
        _pypi_cache[lowered] = mapped
        return mapped

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

    # 3. No curated mapping — NEVER install an import name just because a PyPI
    #    package of that name exists (a squatted/unrelated package would be
    #    installed onto the customer's cluster). Report it for the operator to
    #    add to _IMPORT_TO_PIP or install via the cluster libraries API.
    if _pypi_exists(lowered):
        print(f"[cluster-lifecycle] WARNING: import '{import_name}' has no curated pip mapping; "
              f"a PyPI package of that name exists but will NOT be installed automatically — "
              f"add it to _IMPORT_TO_PIP or install it via the cluster libraries API", flush=True)
        _pypi_cache[lowered] = None
        return None

    # 4. Not on PyPI either — not installed; report rather than pass a name pip
    #    would fail on (the operator may own a private package of that name).
    print(f"[cluster-lifecycle] WARNING: no PyPI package found for import "
          f"'{import_name}' — not installed", flush=True)
    _pypi_cache[lowered] = None
    return None

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
                                  max_ancestor_depth: int = 6, *, strict: bool = False,
                                  excluded_roots: tuple = ()) -> tuple:
    """Walk parent directories of each notebook on the cluster looking for
    .py files / package dirs whose name matches a candidate import.

    Returns (matched_modules, src_roots, match_paths) where:
      matched_modules — set of candidate names found as local files/dirs
      src_roots       — set of directories to add to sys.path so the imports resolve
      match_paths     — list of actual filesystem paths of each match (used to
                        compute the mirror root)

    Exclude the migrated tree on fresh probes so a repaired mirror cannot
    become an additional source match under a shared workspace ancestor.

    ``strict`` distinguishes an unsuccessful scan from a verified empty scan.
    Legacy import-recovery callers retain their existing best-effort behavior.
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
                if strict and not roots_to_scan and p and p.count("/") == 2:
                    roots_to_scan.add(p)
                break
            roots_to_scan.add(p)
            p = os.path.dirname(p)
    if not roots_to_scan:
        if strict:
            raise RuntimeError("local-module scan has no bounded notebook directory")
        return set(), set(), []

    import json as _json
    excluded_roots = tuple(os.path.normpath(root) for root in excluded_roots if root)
    # Unique marker so the scan result is located by content, not by stdout
    # position. AIDP can DUPLICATE or INTERLEAVE stdout lines, so a bare
    # `print(json.dumps(hits))` parsed via `splitlines()[0]` picked up a fragment
    # and json.loads raised "Extra data" — a legitimate scan then looked like a
    # hard failure and blocked a valid pip install (slack_sdk, 2026-09-22). Same
    # The marker also keeps import-recovery scans independent of other stdout.
    _SCAN_SENTINEL = "__AIDP_LOCAL_SCAN__"
    code = (
        "import os, json\n"
        f"candidates = {_json.dumps(sorted(candidates))}\n"
        f"roots = {_json.dumps(sorted(roots_to_scan))}\n"
        f"excluded_roots = {_json.dumps(excluded_roots)}\n"
        "def _excluded(path):\n"
        "    return any(path == root or path.startswith(root + os.sep) for root in excluded_roots)\n"
        "hits = {}  # name -> list of matched paths\n"
        "cand_set = set(candidates)\n"
        "_seen_dirs = set()\n"
        "for root in roots:\n"
        "    if _excluded(root):\n"
        "        continue\n"
        "    if not os.path.isdir(root):\n"
        "        continue\n"
        "    for dirpath, dirnames, filenames in os.walk(root):\n"
        "        if dirpath in _seen_dirs:\n"
        "            dirnames[:] = []\n"
        "            continue\n"
        "        _seen_dirs.add(dirpath)\n"
        "        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d not in ('__pycache__', 'node_modules', '.venv', 'site-packages') and not _excluded(os.path.join(dirpath, d))]\n"
        "        for fn in filenames:\n"
        "            if fn.endswith('.py'):\n"
        "                stem = fn[:-3]\n"
        "                if stem in cand_set:\n"
        "                    hits.setdefault(stem, []).append(os.path.join(dirpath, fn))\n"
        "        for d in dirnames:\n"
        "            if d in cand_set:\n"
        "                hits.setdefault(d, []).append(os.path.join(dirpath, d) + '/')\n"
        f"print({_SCAN_SENTINEL!r} + json.dumps(hits))\n"
    )
    from aidp_executor import format_outputs
    try:
        # Timeout raised from 180s to 600s. Reason: os.walk() over
        # /Workspace/... FUSE mount does one HTTP roundtrip per readdir on
        # AIDP. Deep or wide workspace trees can legit take >3 min. The
        # previous 180s timeout was hit during ExampleVendor v3 migration and
        # triggered a fresh-session cascade (kernel-still-busy → ping fail
        # → recycle → verification fail → state loss). 600s gives realistic
        # legit walks room to finish; genuinely hung scans still surface,
        # just later.
        result = await session.execute(code, timeout=600)
    except Exception as e:
        if strict:
            raise RuntimeError(f"local-module scan failed: {e}") from e
        print(f"[cluster-lifecycle] local-module scan failed: {e}", flush=True)
        return set(), set(), []

    if strict and (result.get("status") != "ok" or any(
            o.get("type") == "error" or o.get("output_type") == "error"
            for o in result.get("outputs", []))):
        raise RuntimeError("local-module scan returned a kernel error")
    raw = format_outputs(result.get("outputs", []))
    if strict:
        from context_tools import _unwrap_aidp_text
        raw = _unwrap_aidp_text(raw)
    try:
        parsed = _json.loads(raw)
        if isinstance(parsed, list) and parsed and "value" in parsed[0]:
            raw = parsed[0]["value"]
    except Exception:
        pass
    # Locate the scan result by its sentinel, not by stdout position. AIDP can
    # duplicate/interleave stdout, so scan for the FIRST line carrying the marker
    # and `raw_decode` the JSON after it — that tolerates junk both before the
    # marker (interleaving) and after the object (a duplicated trailing copy),
    # which is exactly what broke the old `splitlines()[0]` parse with "Extra
    # data" and blocked a valid slack_sdk install (2026-09-22).
    hits = None
    found_marker = False
    parse_error = None
    for _line in raw.splitlines():
        _marker = _line.find(_SCAN_SENTINEL)
        if _marker == -1:
            continue
        found_marker = True
        try:
            hits, _ = _json.JSONDecoder().raw_decode(
                _line[_marker + len(_SCAN_SENTINEL):].strip())
        except ValueError as _e:
            parse_error = _e
            continue  # a duplicated/corrupt copy — try the next marker line
        break
    try:
        # No marker at all -> nothing to parse; a marker with unparseable JSON ->
        # surface that cause; a parsed non-mapping / bad-shape -> invalid. These
        # three stay distinct so the caller's diagnostics remain precise.
        if not found_marker or hits is None:
            raise ValueError(parse_error or "no local-module scan result")
        if strict and (not isinstance(hits, dict) or any(
                not isinstance(k, str) or not isinstance(v, list)
                or not all(isinstance(p, str) for p in v)
                for k, v in hits.items())):
            raise ValueError("invalid local-module scan result")
    except Exception as e:
        if strict:
            raise RuntimeError(f"local-module scan parse failed: {e}") from e
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


_STAGED_PYTHON_COPY_CODE = """\
import os as _os_stage, shutil as _shutil_stage, tempfile as _tempfile_stage

def _copy_python_file_staged(source, target, *, overwrite=False):
    # The migration job serializes its writers. Recheck after staging so a
    # repaired target encountered during this copy remains untouched.
    if not overwrite and _os_stage.path.lexists(target):
        return False
    _fd, _stage = _tempfile_stage.mkstemp(
        prefix='.' + _os_stage.path.basename(target) + '.',
        suffix='.aidp-copy-tmp', dir=_os_stage.path.dirname(target),
    )
    _os_stage.close(_fd)
    try:
        _shutil_stage.copy2(source, _stage)
        if not overwrite and _os_stage.path.lexists(target):
            return False
        _os_stage.replace(_stage, target)
        _stage = None
        return True
    finally:
        if _stage is not None:
            try:
                _os_stage.unlink(_stage)
            except FileNotFoundError:
                pass
"""


async def _mirror_source_tree(session, mirror_src: str, mirror_dst: str,
                              timeout: int = 600, refresh_mirror: bool = False,
                              excluded_roots: tuple = ()) -> bool:
    """Copy missing `.py` files into the mirror, preserving directory structure.

    Existing files are untouched unless refresh_mirror is explicitly true.
    Raises MirrorCopyError when execution or its result cannot confirm success.

    Why only .py and not the whole tree: when mirror_dst lives inside the
    migrated-notebooks tree (so the customer sees one coherent project
    layout), the destination may already contain migrated `.ipynb` files
    saved by Pass-1 dep migration. A full copytree (or rmtree-then-copytree)
    would clobber those. By copying only `.py` files, we never touch
    notebook artifacts. Hidden / cache dirs and migrated output roots are
    skipped on the source side, including when the destination is below src.
    """
    import json as _json
    code = (
        _STAGED_PYTHON_COPY_CODE
        + "import os, json\n"
        f"src = {_json.dumps(mirror_src)}\n"
        f"dst = {_json.dumps(mirror_dst)}\n"
        f"excluded_roots = {_json.dumps(excluded_roots)}\n"
        f"refresh_mirror = {refresh_mirror!r}\n"
        "result = {'src': src, 'dst': dst, 'ok': False, 'files_copied': 0, 'files_skipped': 0}\n"
        "excluded_roots = tuple(os.path.normpath(p) for p in excluded_roots)\n"
        "dst_norm = os.path.normpath(dst)\n"
        "def _excluded_source(path):\n"
        "    path = os.path.normpath(path)\n"
        "    return (path == dst_norm or path.startswith(dst_norm + os.sep)\n"
        "            or any(path == root or path.startswith(root + os.sep) for root in excluded_roots))\n"
        "try:\n"
        "    if not os.path.isdir(src):\n"
        "        result['error'] = f'source dir does not exist: {src}'\n"
        "    elif _excluded_source(src):\n"
        "        result['error'] = f'source dir is inside the migrated output: {src}'\n"
        "    else:\n"
        "        _SKIP_DIRS = {'__pycache__', 'node_modules', '.venv', 'site-packages'}\n"
        "        cnt = 0\n"
        "        skipped = 0\n"
        "        for _dp, _dns, _fns in os.walk(src):\n"
        "            _dns[:] = [d for d in _dns if not d.startswith('.') and d not in _SKIP_DIRS and not _excluded_source(os.path.join(_dp, d))]\n"
        "            _rel = os.path.relpath(_dp, src)\n"
        "            _tgt = dst if _rel == '.' else os.path.join(dst, _rel)\n"
        "            os.makedirs(_tgt, exist_ok=True)\n"
        "            for _fn in _fns:\n"
        "                if _fn.endswith('.py'):\n"
        "                    _target = os.path.join(_tgt, _fn)\n"
        "                    if not refresh_mirror and os.path.lexists(_target):\n"
        "                        skipped += 1\n"
        "                        continue\n"
        "                    if _copy_python_file_staged(os.path.join(_dp, _fn), _target, overwrite=refresh_mirror):\n"
        "                        cnt += 1\n"
        "                    else:\n"
        "                        skipped += 1\n"
        "        result['files_copied'] = cnt\n"
        "        result['files_skipped'] = skipped\n"
        "        result['ok'] = True\n"
        "except Exception as _e:\n"
        "    result['error'] = str(_e)[:300]\n"
        "print(json.dumps(result))\n"
    )
    from aidp_executor import format_outputs
    from context_tools import _unwrap_aidp_text
    try:
        res = await session.execute(code, timeout=timeout)
    except Exception as e:
        raise MirrorCopyError(f"Local-module mirror copy failed: {mirror_src} → {mirror_dst}: {e}") from e
    try:
        raw = _unwrap_aidp_text(format_outputs(res.get("outputs", [])))
    except Exception as e:
        raise MirrorCopyError(
            f"Local-module mirror result could not be decoded for {mirror_src} → {mirror_dst}: {e}"
        ) from e
    first_line = raw.strip().splitlines()[0] if raw.strip() else "{}"
    try:
        info = _json.loads(first_line)
    except Exception as e:
        raise MirrorCopyError(
            f"Local-module mirror result unreadable for {mirror_src} → {mirror_dst}: {first_line[:300]!r}"
        ) from e
    if isinstance(info, dict) and info.get("ok"):
        print(f"[cluster-lifecycle] Mirrored {mirror_src} → {mirror_dst} "
              f"({info.get('files_copied', 0)} .py copied, {info.get('files_skipped', 0)} existing skipped)",
              flush=True)
        return True
    error = info.get("error", "unknown") if isinstance(info, dict) else "invalid result shape"
    raise MirrorCopyError(f"Local-module mirror copy failed: {mirror_src} → {mirror_dst}: {error}")


class MirrorCopyError(RuntimeError):
    """A local-module mirror failed; execution must not use customer source."""


# ── Runtime version pin: install (cure) + verify (guard) ─────────────────────
# The AIDP stock cluster image ships pandas 3.0.x + numpy 2.x. On that combo,
# pandas backs string columns (and even the column Index) with ArrowStringArray;
# any read_csv / .apply / .astype(object) on a string column then dies with a
# cryptic `TypeError: Cannot interpret 'dtype('O')'` deep inside pandas — 100+
# cells into a migration, with no hint at the real cause.
#
# ROOT CAUSE (evidenced 2026-07-31): the target runtime requirements (which pin
# the triad) were never attached to the failing cluster, so it kept stock
# pandas 3.0.5. The cure is to install the target-safe runtime requirements;
# the guard verifies they actually took before the migration runs.
_PANDAS_MAX_MAJOR_EXCLUSIVE = 3  # pandas >= 3.0 triggers the ArrowString defect
_NUMPY_MAX_MAJOR_EXCLUSIVE = 2   # triad pins numpy==1.26.4; numpy >= 2.0 is part of the defect


def _ensure_target_runtime_requirements_file() -> None:
    """Materialize the target baseline for deployments predating this file.

    New installations create the workspace copy during setup. Existing
    deployments still have the repository source, so copy it lazily before the
    library API references the workspace path. Fail with a preflight-quality
    error when neither source is available or the workspace is not writable.
    """
    if os.path.isfile(TARGET_RUNTIME_REQUIREMENTS_PATH):
        return
    # Laptop runners have no /Workspace mount: the local check above is blind
    # there, and the copy below would write into the laptop filesystem (macOS:
    # read-only root → [Errno 30]; faced 2026-10-13 driving the EXBRIDGE for-each
    # migration from a laptop). The install step references the WORKSPACE
    # path, so an existing workspace copy is sufficient — verify via the API
    # before concluding the file is missing.
    _basename = os.path.basename(TARGET_RUNTIME_REQUIREMENTS_PATH)
    if any(
        (item.get("path") or "").rstrip("/").endswith("/" + _basename)
        or item.get("name") == _basename
        for item in _list_workspace_objects(
            os.path.dirname(TARGET_RUNTIME_REQUIREMENTS_PATH)
        )
    ):
        return
    configured = str(
        os.environ.get("AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE", "")
    ).strip()
    if not configured:
        try:
            from aidp_config import load_config
            configured = str(
                getattr(
                    load_config().paths,
                    "target_runtime_requirements_file",
                    "",
                )
                or ""
            ).strip()
        except Exception:
            # The repository fallback below remains valid when no customer
            # YAML can be discovered in an older deployment.
            configured = ""
    repository_source = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "target_runtime_requirements.txt",
    )
    source = next(
        (
            candidate for candidate in (configured, repository_source)
            if candidate
            and candidate != TARGET_RUNTIME_REQUIREMENTS_PATH
            and os.path.isfile(candidate)
        ),
        None,
    )
    if source is None:
        raise RuntimeError(
            "Target runtime requirements are missing. Expected "
            f"{TARGET_RUNTIME_REQUIREMENTS_PATH} or a readable repository/config "
            "source. Run scripts/setup_migration_cluster.py or set "
            "AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE."
        )
    try:
        os.makedirs(os.path.dirname(TARGET_RUNTIME_REQUIREMENTS_PATH), exist_ok=True)
        with open(source, "rb") as source_handle:
            content = source_handle.read()
        with open(TARGET_RUNTIME_REQUIREMENTS_PATH, "wb") as target_handle:
            target_handle.write(content)
    except OSError as exc:
        raise RuntimeError(
            f"Unable to materialize {TARGET_RUNTIME_REQUIREMENTS_PATH} from "
            f"{source}: {exc}. Run scripts/setup_migration_cluster.py on AIDP."
        ) from exc
    print(
        f"[cluster-lifecycle] materialized target runtime requirements: "
        f"{TARGET_RUNTIME_REQUIREMENTS_PATH}",
        flush=True,
    )


def _runtime_pin_violation(pandas_ver, numpy_ver=None):
    """Return a remediation message if the cluster's pandas (>=3) or numpy (>=2)
    is unsupported, else None. Pure/testable — no cluster I/O. Unparseable
    versions are ignored (never block on a reading we can't read)."""
    def _major(v):
        try:
            return int(str(v).strip().split(".")[0])
        except (ValueError, AttributeError, IndexError):
            return None
    problems = []
    pm = _major(pandas_ver)
    if pm is not None and pm >= _PANDAS_MAX_MAJOR_EXCLUSIVE:
        problems.append(f"pandas {pandas_ver} (>= {_PANDAS_MAX_MAJOR_EXCLUSIVE}.0)")
    nm = _major(numpy_ver)
    if numpy_ver is not None and nm is not None and nm >= _NUMPY_MAX_MAJOR_EXCLUSIVE:
        problems.append(f"numpy {numpy_ver} (>= {_NUMPY_MAX_MAJOR_EXCLUSIVE}.0)")
    if not problems:
        return None
    return (
        f"cluster has {' and '.join(problems)} — the pinned triad "
        f"compatibility constraints did NOT take effect. "
        f"read_csv / .apply on string columns will fail with "
        f"'TypeError: Cannot interpret dtype(O)' (ArrowStringArray). Remediation: "
        f"install {TARGET_RUNTIME_REQUIREMENTS_PATH} as a cluster library and restart."
    )


async def ensure_pinned_requirements_installed(cluster_id: str, timeout: int = 600):
    """Install the target-safe runtime baseline as a cluster library when no
    cluster-specific resolved requirements file exists.
    Idempotent — no-op if already attached and healthy. Returns True when the
    requirements are INSTALL_ON_RESTART so the readiness gate can safely
    restart the target cluster."""
    def _is_pin_lib(p):
        return str(p) == TARGET_RUNTIME_REQUIREMENTS_PATH
    libs = _attached_library_rows(cluster_id)
    canonical = [
        lib for lib in libs
        if _is_canonical_requirements_path(lib.get("path", ""))
    ]
    if len({lib.get("path") for lib in canonical}) > 1:
        raise RuntimeError(
            f"cluster {cluster_id} has multiple resolved requirements libraries: "
            f"{[lib.get('path') for lib in canonical]}"
        )
    failed = [
        lib for lib in canonical
        if lib.get("status", "") in ("FAILED", "SKIPPED")
    ]
    if failed:
        raise RuntimeError(
            f"resolved requirements install FAILED on {cluster_id}: "
            f"{[lib.get('stateMessage', '') for lib in failed]}"
        )
    healthy_canonical = [
        lib for lib in canonical
        if lib.get("status", "") in ("INSTALLED", "RESOLVED")
    ]
    queued_canonical = [
        lib for lib in canonical
        if lib.get("status", "") == "INSTALL_ON_RESTART"
    ]
    transient_canonical = [
        lib for lib in canonical
        if lib.get("status", "") in (
            "PENDING", "RESOLVING", "INSTALLING"
        )
    ]
    if healthy_canonical:
        print(
            f"[cluster-lifecycle] cluster-specific resolved requirements already "
            f"attached on {cluster_id[:12]}...; skipping separate baseline install",
            flush=True,
        )
        return False
    if queued_canonical:
        print(
            f"[cluster-lifecycle] resolved requirements require a cluster "
            f"restart on {cluster_id[:12]}...",
            flush=True,
        )
        return True
    if transient_canonical:
        canonical_path = transient_canonical[0].get("path")
        elapsed = 0
        while elapsed < timeout:
            await asyncio.sleep(15)
            elapsed += 15
            current = [
                lib for lib in _attached_library_rows(cluster_id)
                if lib.get("path") == canonical_path
            ]
            if any(lib.get("status") in ("INSTALLED", "RESOLVED") for lib in current):
                return False
            if any(lib.get("status") == "INSTALL_ON_RESTART" for lib in current):
                return True
            if any(lib.get("status") in ("FAILED", "SKIPPED") for lib in current):
                raise RuntimeError(
                    f"resolved requirements install FAILED on {cluster_id}: "
                    f"{[lib.get('stateMessage', '') for lib in current]}"
                )
        raise RuntimeError(
            f"resolved requirements did not become ready within {timeout}s on "
            f"{cluster_id}: {canonical_path}"
        )
    healthy = next((l for l in libs if _is_pin_lib(l.get("path", ""))
                    and l.get("status", "") in ("INSTALLED", "RESOLVED")), None)
    if healthy is not None:
        print(f"[cluster-lifecycle] target runtime requirements already installed on {cluster_id[:12]}...", flush=True)
        return False
    queued = next((l for l in libs if _is_pin_lib(l.get("path", ""))
                   and l.get("status", "") == "INSTALL_ON_RESTART"), None)
    if queued is not None:
        print(
            f"[cluster-lifecycle] target runtime requirements require a "
            f"cluster restart on {cluster_id[:12]}...",
            flush=True,
        )
        return True
    pending = next((l for l in libs if _is_pin_lib(l.get("path", ""))
                    and l.get("status", "") in (
                        "PENDING", "RESOLVING", "INSTALLING"
                    )), None)
    if pending is not None:
        elapsed = 0
        while elapsed < timeout:
            await asyncio.sleep(15)
            elapsed += 15
            mine = [
                lib for lib in _attached_library_rows(cluster_id)
                if _is_pin_lib(lib.get("path", ""))
            ]
            if any(lib.get("status") in ("INSTALLED", "RESOLVED") for lib in mine):
                return False
            if any(lib.get("status") == "INSTALL_ON_RESTART" for lib in mine):
                return True
            if any(lib.get("status") in ("FAILED", "SKIPPED") for lib in mine):
                raise RuntimeError(
                    f"target runtime requirements install FAILED on {cluster_id}: "
                    f"{[lib.get('stateMessage', '') for lib in mine]}"
                )
        raise RuntimeError(
            f"target runtime requirements did not become ready within {timeout}s "
            f"on {cluster_id}: {TARGET_RUNTIME_REQUIREMENTS_PATH}"
        )
    _ensure_target_runtime_requirements_file()
    print(f"[cluster-lifecycle] installing target runtime requirements ({TARGET_RUNTIME_REQUIREMENTS_PATH}) on {cluster_id[:12]}...", flush=True)
    await install_library_async(cluster_id, TARGET_RUNTIME_REQUIREMENTS_PATH)
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(15)
        elapsed += 15
        libs = _attached_library_rows(cluster_id)
        mine = [l for l in libs if _is_pin_lib(l.get("path", ""))]
        if any(l.get("status", "") in ("INSTALLED", "RESOLVED") for l in mine):
            print("[cluster-lifecycle] target runtime requirements installed.", flush=True)
            return False
        if any(l.get("status", "") == "INSTALL_ON_RESTART" for l in mine):
            print(
                "[cluster-lifecycle] target runtime requirements queued for "
                "cluster restart.",
                flush=True,
            )
            return True
        if any(l.get("status", "") in ("FAILED", "SKIPPED") for l in mine):
            raise RuntimeError(
                f"target runtime requirements install FAILED on {cluster_id} "
                f"({TARGET_RUNTIME_REQUIREMENTS_PATH}) — {[l.get('stateMessage','') for l in mine]}")
    raise RuntimeError(
        f"target runtime requirements did not confirm within {timeout}s on "
        f"{cluster_id}: {TARGET_RUNTIME_REQUIREMENTS_PATH}"
    )


async def verify_runtime_pins(cluster_id: str, session, timeout: int = 60):
    """GUARD: confirm the live cluster honors the pinned triad (pandas < 3.0,
    numpy < 2.0).

    Returns (status, message) where status is one of:
      "ok"            — versions honor the pin
      "violation"     — pandas>=3 or numpy>=2 (message = remediation); caller
                        should fail-fast unless AIDP_NO_STRICT_RUNTIME_PINS=1
      "undetermined"  — couldn't read versions (import failed / no marker); the
                        caller does not block, but this is logged distinctly (a
                        cluster where `import pandas` fails is itself broken)."""
    from aidp_executor import format_outputs
    code = (
        "import pandas as _p\n"
        "try:\n"
        "    import numpy as _n\n"
        "    _nv = _n.__version__\n"
        "except Exception:\n"
        "    _nv = '?'\n"
        "print('__AIDP_PINS__pandas=' + _p.__version__ + ',numpy=' + _nv + '__END__')\n"
    )
    result = await session.execute(code, timeout=timeout)
    output = format_outputs(result.get("outputs", []))
    m = re.search(r"__AIDP_PINS__pandas=(.+?),numpy=(.+?)__END__", output)
    if not m:
        print("[cluster-lifecycle][WARN] could not determine cluster pandas/numpy "
              "version (import failed?) — runtime-pin check inconclusive", flush=True)
        return ("undetermined", "")
    pv, nv = m.group(1).strip(), m.group(2).strip()
    msg = _runtime_pin_violation(pv, nv)
    if msg:
        return ("violation", msg)
    print(f"[cluster-lifecycle] runtime pins OK (pandas {pv}, numpy {nv})", flush=True)
    return ("ok", "")


class ImportProbeError(RuntimeError):
    """Dependency availability is unknown; stop execution before queuing more work."""


async def ensure_requirements_installed(
    cluster_id: str,
    session,
    import_names: list,
    job_output_path: str,
    timeout: int = 600,
    notebook_paths: list = None,
    migrated_base: str = "",
    export_base: str = "",
    refresh_mirror: bool = False,
    migrate_output_root: str = "",
) -> dict:
    """Check which imports are missing on the cluster and install via requirements.txt.

    Args:
        cluster_id: AIDP cluster UUID
        session: ClusterSession for running checks on cluster
        import_names: list of top-level import names from notebook analysis
        job_output_path: workspace path for this job (e.g. {MIGRATE_OUTPUT}/{job_name})
        timeout: max seconds for the combined import probe, and separately for library install
        notebook_paths: notebook paths whose source trees should be scanned for
            local modules (so we don't try to pip-install in-repo .py files).
        export_base: exported notebook root stripped from migrated notebook paths.
        refresh_mirror: explicitly replace existing mirrored `.py` files.
        migrate_output_root: configured root containing all migrated jobs; exclude
            it from source discovery so sibling jobs cannot become source matches.

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
    unresolved = f"availability of {', '.join(sorted(candidates))} is UNRESOLVED"
    try:
        result = await session.execute(check_code, timeout=timeout)
    except Exception as exc:
        raise ImportProbeError(
            f"Dependency import probe did not complete ({type(exc).__name__}: {exc}); "
            f"{unresolved}; execution stopped (probe budget {timeout}s)"
        ) from exc
    output = format_outputs(result.get("outputs", []))
    if result.get("status") != "ok":
        errors = [f"{item.get('ename')}: {item.get('evalue')}"
                  for item in result.get("outputs", []) if item.get("type") == "error"]
        detail = "; ".join(errors) or output[:500] or f"status {result.get('status')!r}"
        raise ImportProbeError(
            f"Dependency import probe failed ({detail}); {unresolved}; "
            f"execution stopped (probe budget {timeout}s)"
        )
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
    if not _m:
        raise ImportProbeError(
            "Dependency import probe did not return its completion marker; "
            f"{unresolved}; execution stopped (probe budget {timeout}s)"
        )
    payload = _m.group(1)

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
    # notebook's own source tree (e.g. system_config, base_policy_info_config).
    # We do NOT pip-install these. Copy their .py source into the migrated
    # notebook tree (or the legacy isolated location without migrated_base)
    # so fixes land in the mirror rather than the customer's original source.
    local_modules, src_roots = set(), set()
    mirror_root_orig = ""  # original source root that was mirrored
    mirror_root_dst = ""   # nonempty only when the mirror copy succeeds
    if notebook_paths:
        scan_exclusion_root = migrate_output_root or migrated_base
        local_modules, orig_src_roots, match_paths = await _discover_local_modules(
            session, notebook_paths, set(missing_imports),
            # The scan walks notebook ancestors. They may contain every job's
            # migrated tree, not just this job's migrated_base.
            excluded_roots=(scan_exclusion_root,) if scan_exclusion_root else (),
        )
        if local_modules:
            print(f"[cluster-lifecycle] Local modules (NOT pip-installable): {', '.join(sorted(local_modules))}", flush=True)
            missing_imports = [m for m in missing_imports if m not in local_modules]

            mirror_root_orig = _pick_mirror_root(match_paths)
            if mirror_root_orig:
                # Use the same export-base-relative layout as migrated notebooks,
                # so mirrored .py files sit beside their migrated .ipynb files.
                # For sources outside export_base, retain the workspace-relative
                # layout; callers without migrated_base keep the isolated layout.
                if migrated_base:
                    _rel = mirror_root_orig
                    _export_root = export_base.rstrip("/")
                    if _export_root and not _export_root.startswith("/Workspace/"):
                        _export_root = "/Workspace/" + _export_root.lstrip("/")
                    if _export_root and _rel.startswith(_export_root + "/"):
                        _rel = _rel[len(_export_root) + 1:]
                    elif _rel == _export_root:
                        _rel = ""
                    elif _rel.startswith("/Workspace/"):
                        _rel = _rel[len("/Workspace/"):]
                    else:
                        _rel = _rel.lstrip("/")
                    mirror_root_dst = f"{migrated_base.rstrip('/')}/{_rel}".rstrip("/")
                else:
                    mirror_root_dst = f"{job_output_path}/local_modules/{os.path.basename(mirror_root_orig)}"
                if not await _mirror_source_tree(
                    session, mirror_root_orig, mirror_root_dst,
                    refresh_mirror=refresh_mirror,
                    excluded_roots=(scan_exclusion_root,) if scan_exclusion_root else (),
                ):
                    raise MirrorCopyError(
                        f"Local-module mirror copy failed: {mirror_root_orig} → {mirror_root_dst}"
                    )
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

    # Resolve import names to validated PyPI package names
    pip_packages = []
    _seen_pkgs = set()
    skipped_unmapped = []
    for imp in missing_imports:
        pip_name = _resolve_pip_name(imp)
        if pip_name is None:
            # Curated-only rule: reported, never installed. One rejected import
            # must not disrupt the rest of the batch.
            skipped_unmapped.append(imp)
            continue
        if pip_name not in _seen_pkgs:
            _seen_pkgs.add(pip_name)
            pip_packages.append(pip_name)
    if skipped_unmapped:
        print(f"[cluster-lifecycle] NOT installing (no curated mapping): "
              f"{', '.join(sorted(skipped_unmapped))}", flush=True)

    if not pip_packages:
        return {"installed": [], "local_modules": sorted(local_modules),
                "src_roots": sorted(src_roots),
                "mirror_root_orig": mirror_root_orig, "mirror_root_dst": mirror_root_dst}

    print(f"[cluster-lifecycle] Missing packages: {', '.join(pip_packages)}", flush=True)

    req_workspace_path = _canonical_requirements_path(cluster_id, job_output_path)

    # Read existing job_requirements.txt to build cumulative set
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
    if existing_content.strip() == "EMPTY":
        existing_content = ""
    req_content, new_packages = _append_missing_requirements(
        existing_content, pip_packages
    )

    # Check if all missing packages are already in job_requirements.txt —
    # they were installed previously, no need to re-install
    if not new_packages:
        print(f"[cluster-lifecycle] All missing packages already in job_requirements.txt — skipping install", flush=True)
        return {"installed": [], "local_modules": sorted(local_modules),
                "src_roots": sorted(src_roots),
                "mirror_root_orig": mirror_root_orig, "mirror_root_dst": mirror_root_dst}

    # Upload via session (workspace is writable from cluster)
    upload_code = (
        f"import os\n"
        f"os.makedirs(os.path.dirname('{req_workspace_path}'), exist_ok=True)\n"
        f"with open('{req_workspace_path}', 'w') as f:\n"
        f"    f.write({repr(req_content)})\n"
        f"print('OK')\n"
    )
    await session.execute(upload_code, timeout=30)
    print(f"[cluster-lifecycle] Updated {req_workspace_path} ({len(new_packages)} new: {', '.join(new_packages)})", flush=True)

    # Install via cluster libraries API
    await install_library_async(cluster_id, req_workspace_path)
    print(f"[cluster-lifecycle] Install request sent for resolved requirements", flush=True)

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
    verify_distribution: bool = False,
) -> bool:
    """Install a single missing package discovered at runtime.

    Reads existing requirements.txt, appends the new package, re-uploads,
    and re-installs via the cluster libraries API.

    `module_name` may be a bare import name ("yaml") or a PEP 508 spec with a
    version pin ("mlflow==2.14.2"). When `verify_distribution` is true, the
    value came from an explicit pip command: preserve its distribution name and
    verify it through importlib metadata instead of assuming the distribution
    is importable under that name (for example, PyYAML imports as yaml). Either
    successful check still records the requirement line into
    job_requirements.txt so it survives a cluster restart.

    Returns True if install succeeded (or was safely skipped). Returns False
    (without trying pip) if the module is detected as a local .py file in the
    notebook's source tree.
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

    # Parse an optional version pin off the requested spec ("mlflow==2.14.2",
    # "pandas>=2.1"). module_name is usually a bare import name, but callers
    # may pass a full PEP 508 spec; a pinned request changes the skip rules.
    _req_name, _req_spec = module_name, None
    try:
        _parsed_req = Requirement(module_name)
        _req_name = _parsed_req.name
        if str(_parsed_req.specifier):
            _req_spec = _parsed_req.specifier
    except InvalidRequirement:
        pass  # bare/unparseable module name — treat as unpinned

    # Already-satisfied guard: the most accurate + cheapest "is it installed?"
    # check is on the kernel itself — this reflects real runtime availability,
    # covering BOTH cluster-libraries AND packages pre-baked into the Spark
    # runtime image (numpy, pandas, …). If satisfied, skip the (multi-minute)
    # re-install — but still persist the requirement line below so a cluster
    # restart re-installs it (the skip must never lose persistence).
    # Pinned specs: importability alone is NOT sufficient — the installed
    # distribution version must satisfy the pin (importlib.metadata.version);
    # a mismatch or any metadata failure falls through to the install path.
    # No-regression by construction: any probe failure, or a pip-name that is
    # not the import-name (e.g. scikit-learn→sklearn), yields MISSING and falls
    # straight through to the existing install path — same behavior as before.
    _skip_install = False
    try:
        from aidp_executor import format_outputs as _fmt
        from context_tools import _unwrap_aidp_text as _unwrap
        if _req_spec is None and not verify_distribution:
            _probe = await session.execute(
                "import importlib.util as _u\n"
                f"print('PRESENT' if _u.find_spec({module_name!r}) is not None else 'MISSING')\n",
                timeout=30,
            )
            _probe_out = _unwrap(_fmt(_probe.get("outputs", [])))
            if "PRESENT" in _probe_out and "MISSING" not in _probe_out:
                print(f"[cluster-lifecycle] {module_name} already importable on cluster — skipping install", flush=True)
                _skip_install = True
        else:
            _probe = await session.execute(
                "import importlib.metadata as _md\n"
                "try:\n"
                f"    print('VER=' + _md.version({_req_name!r}))\n"
                "except _md.PackageNotFoundError:\n"
                "    print('MISSING')\n",
                timeout=30,
            )
            _probe_out = _unwrap(_fmt(_probe.get("outputs", [])))
            _ver_m = re.search(r"VER=([^\s'\"]+)", _probe_out)
            if _ver_m and (
                _req_spec is None
                or _req_spec.contains(_ver_m.group(1), prereleases=True)
            ):
                print(f"[cluster-lifecycle] {_req_name} {_ver_m.group(1)} already satisfies "
                      f"{module_name!r} on cluster — skipping install", flush=True)
                _skip_install = True
            elif _ver_m:
                print(f"[cluster-lifecycle] {_req_name} {_ver_m.group(1)} does NOT satisfy "
                      f"{module_name!r} — installing pinned version", flush=True)
            else:
                print(f"[cluster-lifecycle] no installed metadata for {_req_name} — installing", flush=True)
    except Exception as _probe_err:  # noqa: BLE001 — probe must never block install
        print(f"[cluster-lifecycle] install-skip probe for {module_name} failed ({_probe_err}); proceeding to install", flush=True)

    # Explicit pip cells already supply a distribution name. Import recovery
    # supplies an import name and still needs the established import->pip map.
    pip_name = _req_name if verify_distribution else _resolve_pip_name(_req_name)
    if pip_name is None:
        # Curated-only rule: an unmapped import name is reported, never installed.
        print(f"[cluster-lifecycle] NOT installing '{_req_name}': no curated pip "
              f"mapping — add it to _IMPORT_TO_PIP or install it via the cluster "
              f"libraries API", flush=True)
        return False  # callers treat the return as a boolean (installed?)
    # Preserve the requested pin in the persisted requirement line so a later
    # cluster restart re-installs the same version, not just any version.
    pip_line = f"{pip_name}{_req_spec}" if _req_spec is not None else pip_name
    req_workspace_path = _canonical_requirements_path(cluster_id, job_output_path)

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

    if output.strip() == "EMPTY":
        output = ""
    req_content, additions = _append_missing_requirements(output, [pip_line])

    if not additions:
        print(f"[cluster-lifecycle] {pip_name} already in job_requirements.txt — "
              + ("skip confirmed" if _skip_install else "re-installing"), flush=True)
    else:
        print(f"[cluster-lifecycle] Adding {pip_line} to resolved requirements", flush=True)

    # Re-write the requirements file. This ALSO runs on the skip path when the
    # package wasn't recorded yet — an importable-but-unrecorded package must
    # still land in job_requirements.txt or it silently vanishes on cluster
    # restart. (No write needed on skip when it's already recorded.)
    if additions or not _skip_install:
        upload_code = (
            f"import os\n"
            f"os.makedirs(os.path.dirname('{req_workspace_path}'), exist_ok=True)\n"
            f"with open('{req_workspace_path}', 'w') as f:\n"
            f"    f.write({repr(req_content)})\n"
            f"print('OK')\n"
        )
        await session.execute(upload_code, timeout=30)

    if _skip_install:
        return True

    await install_library_async(cluster_id, req_workspace_path)
    print(f"[cluster-lifecycle] Re-install request sent for resolved requirements", flush=True)

    if not await _poll_library_install(cluster_id, req_workspace_path, timeout):
        return False

    # Terminal INSTALLED is necessary but not sufficient. Verify the requested
    # distribution or import on the live kernel; bounded re-probes absorb any
    # environment-visibility lag after INSTALLED.
    if verify_distribution:
        return await _verify_distribution_on_kernel(
            session, pip_name, _req_spec
        )
    return await _verify_import_on_kernel(session, _req_name)


async def _verify_distribution_on_kernel(
    session,
    distribution_name: str,
    specifier=None,
    attempts: int = 12,
    interval: int = 10,
) -> bool:
    """Verify an installed distribution without assuming its import name."""
    from aidp_executor import format_outputs as _fmt
    from context_tools import _unwrap_aidp_text as _unwrap

    probe = (
        "import importlib.metadata as _md\n"
        "try:\n"
        f"    print('VER=' + _md.version({distribution_name!r}))\n"
        "except _md.PackageNotFoundError:\n"
        "    print('MISSING')\n"
    )
    for attempt in range(1, attempts + 1):
        try:
            result = await session.execute(probe, timeout=30)
            output = _unwrap(_fmt(result.get("outputs", [])))
            match = re.search(r"VER=([^\s'\"]+)", output)
            if match and (
                specifier is None
                or specifier.contains(match.group(1), prereleases=True)
            ):
                print(
                    f"[cluster-lifecycle] {distribution_name} {match.group(1)}: "
                    f"distribution verified on kernel (attempt {attempt}).",
                    flush=True,
                )
                return True
        except Exception as exc:
            print(
                f"[cluster-lifecycle] distribution-verify probe error for "
                f"{distribution_name}: {exc}",
                flush=True,
            )
        if attempt < attempts:
            await asyncio.sleep(interval)
    constraint = f" satisfying {specifier}" if specifier is not None else ""
    print(
        f"[cluster-lifecycle] WARNING: {distribution_name} reached INSTALLED "
        f"but no distribution metadata{constraint} was visible after "
        f"{attempts * interval}s - reporting install failure.",
        flush=True,
    )
    return False


async def _verify_import_on_kernel(session, import_name: str,
                                   attempts: int = 12, interval: int = 10) -> bool:
    """True once `import_name` is genuinely importable on the kernel."""
    from aidp_executor import format_outputs as _fmt
    from context_tools import _unwrap_aidp_text as _unwrap
    probe = (
        "import importlib, importlib.util as _u\n"
        "importlib.invalidate_caches()\n"
        f"print('PRESENT' if _u.find_spec({import_name!r}) is not None else 'MISSING')\n"
    )
    for attempt in range(1, attempts + 1):
        try:
            r = await session.execute(probe, timeout=30)
            out = _unwrap(_fmt(r.get("outputs", [])))
            if "PRESENT" in out and "MISSING" not in out:
                print(f"[cluster-lifecycle] {import_name}: import verified on "
                      f"kernel (attempt {attempt}).", flush=True)
                return True
        except Exception as exc:  # probe error — keep trying within the budget
            print(f"[cluster-lifecycle] import-verify probe error for "
                  f"{import_name}: {exc}", flush=True)
        if attempt < attempts:
            await asyncio.sleep(interval)
    print(f"[cluster-lifecycle] WARNING: {import_name} reached INSTALLED but is "
          f"NOT importable on the kernel after {attempts * interval}s — "
          f"reporting install failure (no silent retry).", flush=True)
    return False


async def _poll_library_install(cluster_id: str, library_path: str, timeout: int = 600) -> bool:
    """Poll a just-triggered library (re)install to its true terminal state.

    Success is ONLY status == INSTALLED. RESOLVED means pip dependency
    resolution finished while installation is still in flight — treating it
    as success made the auto-install retry a cell ~9s before the package was
    importable (seaborn incident, run 80000000000011 / 2026-09-08: poll said
    "installed successfully" at 40s on RESOLVED; the import succeeded only
    minutes later). RESOLVED keeps polling. INSTALL_ON_RESTART and FAILED are
    loud failures — never retried into silently.
    """
    poll_interval = 10
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        libs = get_installed_libraries(cluster_id)
        for lib in libs:
            if lib.get("path") == library_path:
                status = lib.get("status", "")
                if status == "INSTALLED":
                    print(f"[cluster-lifecycle] {library_path}: terminal status "
                          f"INSTALLED after {elapsed}s.", flush=True)
                    return True
                elif status == "FAILED":
                    msg = lib.get("stateMessage", "")
                    print(f"[cluster-lifecycle] WARNING: {library_path} install "
                          f"FAILED: {msg}", flush=True)
                    return False
                elif status == "INSTALL_ON_RESTART":
                    print(f"[cluster-lifecycle] WARNING: {library_path} is "
                          f"INSTALL_ON_RESTART — a cluster restart is required "
                          f"before this library is usable; not retrying.", flush=True)
                    return False
                else:
                    # PENDING / RESOLVING / RESOLVED — still in flight.
                    print(f"[cluster-lifecycle] {library_path}: {status} "
                          f"({elapsed}s / {timeout}s)", flush=True)
                break

    print(f"[cluster-lifecycle] WARNING: {library_path} install did not reach "
          f"INSTALLED within {timeout}s.", flush=True)
    return False
