"""The actuator tick: context -> tier -> drain approvals -> plan -> gate -> act -> persist.

Approval loop: a quarantined action sits in ``{ops}.approvals`` as ``pending``; a human
approves it (scripts/approve.py); the next tick expires stale rows, checks each approved row before the gate
(``drain.approval_problem``: the kind must be one a quarantine rule holds, the row must be the one the gate
opened for a quarantine of that exact action, the approver must be a person, and a control-plane kind needs the
``NS_APPROVERS`` allowlist), re-evaluates the action through the same PolicyEngine (deny rules still deny; the
quarantine effect is lifted and recorded as an ``allow`` decision with reason ``APPROVED_BY:<name>``), claims
the row (``approved`` -> ``executing``), executes it through ``_act`` (dry-run aware), inserts the action row
and marks the approval ``executed`` / ``failed`` (or returns it to ``approved`` with a note when denied or
still planned-only). A dry-run tick reports what it would execute and leaves a control-plane approval untouched. Planning skips signatures that were executed this tick or that
already have an open (pending / approved) row, so the same action is never quarantined twice.

Runs OUTSIDE the cluster. ``io`` is an ``AidpIO`` (or a fake); ``store`` is anything with
``run_sql(sql) -> list[tuple]`` (an ``AidpIO`` in production, an in-memory fake in tests).

Sibling modules (tiers, gate, ledger) are imported lazily inside functions.

kv persistence: the ledger has no kv table, so the small per-workspace state the tier ladder and the
loop guard need (pending_tier, exhausted_ticks, sig_streak, last_known_budget, prev_tier) lives in a
``KVStore`` (default: a JSON file under ``NS_STATE_DIR`` or ``~/.nightshift``). The tick row in the
ledger stays the idempotent replay key.
"""
from __future__ import annotations

import calendar
import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from .config import Env
from .types import Action, Decision, Rule, TickContext

logger = logging.getLogger(__name__)

LOOP_GUARD_TICKS = 3
TICK_WINDOW_MINUTES = 30
PIPELINE_CLASSES = ("bronze", "silver", "gold", "optimize")
_FAILED_STATES = {"FAILED", "ERROR", "TIMED_OUT", "TIMEOUT", "CANCELED", "CANCELLED", "INTERNAL_ERROR"}
_RUNNING_STATES = {"RUNNING", "ACTIVE", "IN_PROGRESS", "PENDING", "QUEUED", "STARTING"}
# A run counts as finished only in a state known to be terminal. Anything else (a running state, an empty state, a
# value this list has not seen) counts as active, so a cluster is never read as idle on a state nobody recognised.
_DONE_STATES = _FAILED_STATES | {"SUCCEEDED", "SUCCESS", "SUCCESSFUL", "COMPLETED", "DONE", "FINISHED", "SKIPPED", "TERMINATED",
                                 "UPSTREAM_FAILED", "UPSTREAM_CANCELED", "KILLED", "ABORTED", "STOPPED"}

# Monthly envelopes (cost units of budget_ledger.est_cost / llm_ledger.est_cost). Overridable per
# workspace through kv['monthly_compute_budget'] / kv['monthly_llm_budget'] or the NS_* env vars.
DEFAULT_COMPUTE_BUDGET = float(os.environ.get("NS_MONTHLY_COMPUTE_BUDGET", "1000"))
DEFAULT_LLM_BUDGET = float(os.environ.get("NS_MONTHLY_LLM_BUDGET", "200"))


@dataclass
class TickReport:
    tick_id: str
    tier: str
    prev_tier: str | None
    actions: list[Action] = field(default_factory=list)
    decisions: list[Decision] = field(default_factory=list)
    approvals_opened: int = 0
    wake_consumed: int = 0
    dry_run: bool = True
    notes: list[str] = field(default_factory=list)
    outcomes: dict[str, Any] = field(default_factory=dict)  # action signature -> io result / skip reason
    approvals_executed: int = 0  # approved rows drained and executed this tick
    draft: dict[str, Any] | None = None  # the drafting pass (run_tick --draft): entries handed to the heartbeat and its summary

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["actions"] = [{**asdict(a), "signature": a.signature} for a in self.actions]
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)


# --------------------------------------------------------------------------- kv persistence
class KVStore(Protocol):
    def load(self) -> dict[str, Any]: ...
    def save(self, kv: dict[str, Any]) -> None: ...


class MemoryKV:
    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self.data: dict[str, Any] = dict(initial or {})

    def load(self) -> dict[str, Any]:
        return dict(self.data)

    def save(self, kv: dict[str, Any]) -> None:
        self.data = json.loads(json.dumps(kv, default=str))


class FileKV:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, Any]:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def save(self, kv: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(kv, indent=2, sort_keys=True, default=str), encoding="utf-8")
        tmp.replace(self.path)


def default_kv_store(env: Env) -> FileKV:
    root = Path(os.environ.get("NS_STATE_DIR") or (Path.home() / ".nightshift"))
    return FileKV(root / f"kv_{env.workspace_id[:8]}.json")


# --------------------------------------------------------------------------- helpers
def compute_tick_id(now: datetime, workspace: str) -> str:
    floored = now.replace(minute=(now.minute // TICK_WINDOW_MINUTES) * TICK_WINDOW_MINUTES, second=0, microsecond=0)
    return hashlib.sha256(f"{floored.isoformat()}|{workspace}".encode("utf-8")).hexdigest()


def _env_of(io: Any) -> Env:
    return getattr(io, "env", None) or Env()


def _row_get(row: Any, idx: int, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[idx]
    except (IndexError, TypeError):
        return default


def _f(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _sha16(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]


def derive_budget(row: Any, now: datetime, compute_budget: float, llm_budget: float) -> dict[str, Any]:
    """Turn a ``budget_summary_sql`` row (used_ocpu_hours, used_cost, llm_calls, llm_cost,
    last_budget_entry_at, last_llm_entry_at) into remaining / projected-overrun percentages.

    A ledger with no entry at all is reported as ``known=False`` (unknown, never "100 % remaining")."""
    used_cost = _f(_row_get(row, 1, "used_cost")) or 0.0
    llm_cost = _f(_row_get(row, 3, "llm_cost")) or 0.0
    last_entry = _row_get(row, 4, "last_budget_entry_at")
    if last_entry in (None, ""):
        return {"known": False, "remaining_pct": None, "projected_overrun_pct": None, "llm_remaining_pct": None}
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    days_elapsed = max((now - month_start).total_seconds() / 86400.0, 0.5)
    projected = used_cost / days_elapsed * days_in_month
    remaining = max(0.0, (1.0 - used_cost / compute_budget) * 100.0) if compute_budget > 0 else None
    overrun = max(0.0, (projected / compute_budget - 1.0) * 100.0) if compute_budget > 0 else None
    llm_remaining = max(0.0, (1.0 - llm_cost / llm_budget) * 100.0) if llm_budget > 0 else None
    r6 = lambda v: None if v is None else round(v, 6)  # noqa: E731
    return {"known": True, "remaining_pct": r6(remaining), "projected_overrun_pct": r6(overrun),
            "llm_remaining_pct": r6(llm_remaining), "used_cost": used_cost, "llm_cost": llm_cost}


def _pipeline_class(name: str) -> str | None:
    head = name.lower()
    for sep in ("_", ".", "-", " "):
        if sep in head:
            head = head.split(sep, 1)[0]
            break
    return head if head in PIPELINE_CLASSES else None


def _cluster_actual(c: dict) -> dict:
    """Read the cluster body as the Workbench REST API returns it: workerConfig.{minWorkerCount,maxWorkerCount,workerShape},
    autoTerminationMinutes, state. Legacy/guessed keys kept as fallbacks."""
    wc = c.get("workerConfig") or {}
    scale = c.get("autoScale") or c.get("autoscale") or {}
    return {
        "min_workers": wc.get("minWorkerCount", c.get("minWorkers", scale.get("minWorkers", c.get("numWorkers")))),
        "max_workers": wc.get("maxWorkerCount", c.get("maxWorkers", scale.get("maxWorkers", c.get("numWorkers")))),
        "auto_terminate_min": c.get("autoTerminationMinutes", c.get("autoStopInMinutes", c.get("autoTerminateMinutes"))),
        "shape": wc.get("workerShape") or c.get("workerShape") or c.get("workerNodeShape"),
        "state": c.get("state") or c.get("lifecycleState"),
    }


def action_from_json(action_json: Any) -> Action:
    """Rebuild an Action from approvals.action_json (a JSON string, or an already-parsed dict)."""
    body = json.loads(action_json) if isinstance(action_json, (str, bytes)) else action_json
    if not isinstance(body, dict) or not body.get("kind") or body.get("target") in (None, ""):
        raise ValueError("action_json needs kind and target")
    params = body.get("params") or {}
    if not isinstance(params, dict):
        raise ValueError("action_json.params must be an object")
    return Action(str(body["kind"]), str(body["target"]), params,
                  previous_value=body.get("previous_value"), reversible=bool(body.get("reversible", True)))


def run_status(run: dict) -> str:
    """The run's state, upper-cased. Job runs carry ``state`` as an object (``{"status": "RUNNING", ...}``) on the
    Workbench REST API and as a plain string in older or scripted rows; both are read (same rule as AidpIO.run_status)."""
    st = run.get("state")
    if isinstance(st, dict):
        return str(st.get("status") or st.get("lifecycleState") or "").upper()
    return str(st or run.get("lifecycleState") or run.get("status") or "").upper()


def _run_cluster_key(run: dict) -> str | None:
    for k in ("clusterKey", "clusterId", "cluster_id", "cluster_key"):
        if run.get(k):
            return str(run[k])
    cl = run.get("cluster")
    if isinstance(cl, dict) and (cl.get("key") or cl.get("id")):
        return str(cl.get("key") or cl.get("id"))
    return None


# --------------------------------------------------------------------------- 5.6 step 2: context
def build_context(io: Any, store: Any, now: datetime, kv: dict | None = None, notes: list[str] | None = None) -> TickContext:
    """One budget read (cached fallback, explicit unknown), cluster states, 24 h failures, active runs,
    unconsumed wake events."""
    from . import ledger

    env = _env_of(io)
    ops = env.ops
    notes = notes if notes is not None else []
    kv = dict(kv or {})
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    compute_budget = float(kv.get("monthly_compute_budget") or DEFAULT_COMPUTE_BUDGET)
    llm_budget = float(kv.get("monthly_llm_budget") or DEFAULT_LLM_BUDGET)

    remaining = overrun = llm = None
    source = "unknown"
    try:
        rows = store.run_sql(ledger.budget_summary_sql(ops, month_start.isoformat()))
        b = derive_budget(rows[0], now, compute_budget, llm_budget) if rows else {"known": False}
        if b["known"]:
            remaining, overrun, llm = b["remaining_pct"], b["projected_overrun_pct"], b["llm_remaining_pct"]
            source = "ledger"
            kv["last_known_budget"] = {"remaining_pct": remaining, "projected_overrun_pct": overrun,
                                       "llm_remaining_pct": llm, "as_of": now.isoformat()}
        else:
            notes.append("budget: ledger has no entries yet (not read as zero spend)")
    except Exception as e:  # noqa: BLE001
        notes.append(f"budget: ledger read failed ({type(e).__name__}: {e})")
    if source != "ledger":
        cached = kv.get("last_known_budget")
        if isinstance(cached, dict) and cached.get("remaining_pct") is not None:
            remaining, overrun, llm = (_f(cached.get("remaining_pct")), _f(cached.get("projected_overrun_pct")),
                                       _f(cached.get("llm_remaining_pct")))
            source = "cached"
            notes.append(f"budget: using cached value from {cached.get('as_of')}")
        else:
            source = "unknown"
            notes.append("budget: unknown (no ledger value, no cache) -> at most low_compute, never paused")

    cluster_states: dict[str, str] = {}
    managed = [str(k) for k in (kv.get("managed_clusters") or [env.cluster_id])]
    for key in managed:
        try:
            c = io.get_cluster(key)
            cluster_states[key] = str(c.get("state") or c.get("lifecycleState") or "UNKNOWN")
        except Exception as e:  # noqa: BLE001
            cluster_states[key] = "UNKNOWN"
            notes.append(f"cluster {key}: get_cluster failed ({type(e).__name__})")

    # active runs per managed cluster. A cluster has an entry only when the run list was read: the planner and the
    # gate (safety.active_runs) both treat a missing entry as unknown, and unknown is never idle.
    failures: list[dict] = []
    active_runs: dict[str, int] = {}
    try:
        runs = list(io.list_recent_runs(hours=24))
        # the recent list is one page of history; active runs are also asked for by status, so a run that started
        # before that page still counts (an io without the status query, such as a test fake, has only the page)
        ask_active = getattr(io, "list_active_runs", None)
        still_active = list(ask_active()) if callable(ask_active) else []
        counts = {k: 0 for k in managed}
        unrecognised: set[str] = set()
        seen: set[str] = set()
        for r in [*still_active, *runs]:
            state = run_status(r)
            if state in _FAILED_STATES:
                failures.append(r)
            elif state not in _DONE_STATES:
                rk = str(r.get("key") or r.get("id") or id(r))
                if rk in seen:
                    continue   # the same run from both lists
                seen.add(rk)
                if state not in _RUNNING_STATES:
                    unrecognised.add(state or "(empty)")
                ck = _run_cluster_key(r)
                for k in ([ck] if ck else managed):  # unknown cluster -> count against every managed one
                    counts[k] = counts.get(k, 0) + 1
        if unrecognised:
            notes.append(f"runs: state(s) {sorted(unrecognised)} not recognised; counted as active")
        active_runs = counts
    except Exception as e:  # noqa: BLE001
        active_runs = {}
        notes.append(f"runs: run list read failed ({type(e).__name__}); active runs unknown, "
                     "no cluster is treated as idle this tick")
    kv["active_runs"] = active_runs

    wake: list[dict] = []
    try:
        rows = store.run_sql(f"SELECT event_id, source, reason, pipeline, payload FROM {ops}.wake_events "
                             f"WHERE consumed_by_tick IS NULL ORDER BY created_at")
        for r in rows:
            wake.append({"event_id": _row_get(r, 0, "event_id"), "source": _row_get(r, 1, "source"),
                         "reason": _row_get(r, 2, "reason"), "pipeline": _row_get(r, 3, "pipeline"),
                         "payload": _row_get(r, 4, "payload")})
    except Exception as e:  # noqa: BLE001
        notes.append(f"wake_events: read failed ({type(e).__name__})")

    return TickContext(now_utc=now, budget_remaining_pct=remaining, projected_overrun_pct=overrun,
                       llm_remaining_pct=llm, budget_source=source, cluster_states=cluster_states,
                       recent_failures=failures, wake_events=wake, kv=kv)


# --------------------------------------------------------------------------- 5.6 step 4: plan
def plan_actions(ctx: TickContext, tier: str, policy_rows: list[dict], io: Any) -> list[Action]:
    """Desired (policy row for the tier) vs actual (cluster bodies, job bodies) -> diff list.

    Cluster worker bounds / auto-terminate are planned as ``cluster_update``; idle ACTIVE clusters at
    tiers whose policy allows only one worker are planned as ``cluster_stop``. Cadence and disable
    changes are planned with ``params.requires_schedule_body=True`` because the live job schedule body
    is unknown: they are gated and recorded but not executed until the body is confirmed."""
    from . import tiers

    actions: list[Action] = []
    budget_unknown = ctx.budget_source == "unknown"
    protected = set(ctx.kv.get("sla_protected_jobs") or [])
    cluster_policy = next((tiers.policy_for(tier, pc, policy_rows) for pc in PIPELINE_CLASSES
                           if tiers.policy_for(tier, pc, policy_rows)), None)

    for key, state in ctx.cluster_states.items():
        if not cluster_policy:
            break
        try:
            c = io.get_cluster(key)
        except Exception:  # noqa: BLE001
            continue
        actual = _cluster_actual(c)
        want = {k: cluster_policy.get(k) for k in ("min_workers", "max_workers", "auto_terminate_min")}
        want = {k: v for k, v in want.items() if v is not None}
        if want and any(actual.get(k) != v for k, v in want.items()):
            params = dict(want)
            if actual.get("shape"):
                params["shape"] = actual["shape"]
            actions.append(Action("cluster_update", key, params,
                                  previous_value={k: actual.get(k) for k in ("min_workers", "max_workers", "auto_terminate_min")},
                                  reversible=True))
        stop_idle = cluster_policy.get("stop_idle", cluster_policy.get("max_workers", 99) <= 1)
        idle = (ctx.kv.get("active_runs") or {}).get(key) == 0   # no entry = unknown, which is not idle
        if stop_idle and str(state).upper() == "ACTIVE" and idle:
            actions.append(Action("cluster_stop", key, {"reason": f"idle at tier {tier}"},
                                  previous_value={"state": state}, reversible=True))

    try:
        jobs = io.list_jobs()
    except Exception:  # noqa: BLE001
        jobs = []
    for j in jobs:
        name = str(j.get("name") or j.get("displayName") or "")
        key = str(j.get("key") or j.get("id") or name)
        pclass = _pipeline_class(name)
        pol = tiers.policy_for(tier, pclass, policy_rows) if pclass else None
        if not pol:
            continue
        sla = name in protected or key in protected or bool(j.get("sla_protected"))
        schedule = j.get("schedule") if isinstance(j.get("schedule"), dict) else {}
        current_cron = schedule.get("cron") or schedule.get("quartzCronExpression")
        enabled = pol.get("enabled", True)
        if enabled and pol.get("cron") and pol["cron"] != current_cron:
            actions.append(Action("job_update_schedule", key,
                                  {"cron": pol["cron"], "enabled": True, "sla_protected": sla, "requires_schedule_body": True},
                                  previous_value={"schedule": j.get("schedule")}, reversible=True))
        if not enabled and tier == "paused" and not budget_unknown:
            actions.append(Action("job_disable", key,
                                  {"enabled": False, "sla_protected": sla, "reason": "tier paused",
                                   "requires_schedule_body": True},
                                  previous_value={"schedule": j.get("schedule")}, reversible=True))
    return actions


# --------------------------------------------------------------------------- 5.6: the tick
HEARTBEAT_FILE = "/Workspace/Shared/nightshift/nightshift_heartbeat.py"   # what the scheduled job runs; the pass does not read it
DRAFT_PASS_TIMEOUT_S = 3600
REPO_ROOT = Path(__file__).resolve().parents[1]


def bundle_sources(repo_root: Path = REPO_ROOT) -> dict[str, str]:
    """The heartbeat and every package module as text, from the repo copy. The kernel cell writes them to a private
    temp dir and executes only from there, so nothing on the actuator path is imported or exec'd from the shared
    workspace folder (which anyone with write access to Shared/ could change between deploy and run)."""
    src: dict[str, str] = {}
    hb = repo_root / "jobs" / "nightshift_heartbeat.py"
    if hb.exists():
        src["nightshift_heartbeat.py"] = hb.read_text(encoding="utf-8")
    for f in sorted((repo_root / "nightshift").glob("*.py")):
        src[f"pkg/nightshift/{f.name}"] = f.read_text(encoding="utf-8")
    return src


def statement_sha(statement: Any) -> str:
    return hashlib.sha256(str(statement).strip().encode("utf-8")).hexdigest()


def select_draft_work(store: Any, ops: str, watched: list[dict], now: datetime, hours: int = 24,
                      cap: int = 3) -> tuple[list[dict], list[str]]:
    """Which watched entries the drafting pass hands to the heartbeat: those with no settled verdict in the window
    (to be graded) and those whose latest verdict is silent_wrong with no live replay_safe draft and room under the
    daily cap (to be drafted). The heartbeat re-applies every check before a model call; this only avoids starting
    a kernel run that would do nothing."""
    notes: list[str] = []
    entries = [w for w in watched if isinstance(w, dict) and w.get("statement") and w.get("table")]
    if not entries:
        return [], ["draft pass: no watched statements"]
    graded = {str(_row_get(r, 0, "statement_sha256")) for r in store.run_sql(ledger_mod().graded_statements_sql(ops, hours, now))}
    needing = {str(_row_get(r, 0, "statement_sha256")) for r in store.run_sql(ledger_mod().statements_needing_draft_sql(ops, hours, now, cap))}
    work: list[dict] = []
    for w in entries:
        sha = statement_sha(w["statement"])
        if sha not in graded:
            work.append(w)
            notes.append(f"draft pass: {w['table']} has no verdict in {hours} h; grading first")
        elif sha in needing:
            work.append(w)
            notes.append(f"draft pass: {w['table']} silently wrong with no live draft; drafting")
    if not work:
        notes.append("draft pass: nothing to draft (every silently wrong statement already has a live draft or is at its daily cap)")
    return work, notes


def ledger_mod():
    from . import ledger
    return ledger


def heartbeat_kernel_code(work: list[dict], sources: dict[str, str] | None = None, job_name: str = "nightshift_actuator",
                          reuse_hours: int = 24, attempts: int = 1, budget_s: int = DRAFT_PASS_TIMEOUT_S) -> str:
    """The cell the actuator runs in the kernel: the heartbeat, in draft-only mode on the pass's watch list, reusing a
    verdict the scheduled job produced inside `reuse_hours`. Same code path as the job, same gate. The heartbeat
    source, the package modules and the watch list all travel inside the cell; the kernel writes them to a private
    temp dir on the driver, imports the package from there (NS_PKG) and execs the heartbeat text it was given."""
    sources = bundle_sources() if sources is None else sources
    if "nightshift_heartbeat.py" not in sources:
        raise ValueError("the source bundle has no heartbeat")
    knobs = {"NS_FORCE_TICK": "1", "NS_DRAFT_ONLY": "1", "NS_REUSE_VERDICT_H": str(reuse_hours),
             "NS_DRAFT_ATTEMPTS": str(attempts), "NS_JOB_NAME": job_name, "NS_TASK_BUDGET_S": str(budget_s)}
    return (
        "import json, os, shutil, sys, tempfile\n"
        "sys.dont_write_bytecode = True\n"
        f"_src = json.loads({json.dumps(json.dumps(sources, sort_keys=True))})\n"
        "_dir = tempfile.mkdtemp(prefix='nightshift_actuator_')\n"
        "for _rel, _text in _src.items():\n"
        "    _path = os.path.join(_dir, *_rel.split('/'))\n"
        "    os.makedirs(os.path.dirname(_path), exist_ok=True)\n"
        "    open(_path, 'w', encoding='utf-8').write(_text)\n"
        "_watch = os.path.join(_dir, 'watch.json')\n"
        f"open(_watch, 'w', encoding='utf-8').write({json.dumps(json.dumps(work))})\n"
        f"os.environ.update({json.dumps(knobs)})\n"
        "os.environ['NS_PKG'] = os.path.join(_dir, 'pkg')\n"
        "os.environ['NS_WATCHED'] = _watch\n"
        "for _m in [k for k in sys.modules if k.split('.')[0] == 'nightshift']:\n"
        "    del sys.modules[_m]\n"
        "try:\n"
        "    exec(compile(_src['nightshift_heartbeat.py'], os.path.join(_dir, 'nightshift_heartbeat.py'), 'exec'))\n"
        "finally:\n"
        "    shutil.rmtree(_dir, ignore_errors=True)\n"
    )


def parse_heartbeat_output(result: Any) -> tuple[dict | None, list[dict]]:
    """The printed tick summary (the last line holding "tick_id") and the event lines, from a kernel result."""
    texts: list[str] = []
    if isinstance(result, dict):
        for o in result.get("outputs") or []:
            if isinstance(o, dict) and isinstance(o.get("text"), str):
                texts.append(o["text"])
        if isinstance(result.get("text"), str):
            texts.append(result["text"])
    summary, events = None, []
    for line in "\n".join(texts).splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except Exception:  # noqa: BLE001
            continue
        if "event" in obj:
            events.append(obj)
        elif "tick_id" in obj:
            summary = obj
    return summary, events


def run_draft_pass(io: Any, store: Any, ops: str, watched: list[dict], now: datetime, notes: list[str],
                   timeout_s: int = DRAFT_PASS_TIMEOUT_S) -> dict[str, Any]:
    """Grade what is ungraded and draft what is silently wrong, by running the heartbeat in the actuator's kernel
    session (the session that can call ai_generate). Returns a small report; details go to notes. The heartbeat gets
    a budget 300 s below the client deadline so it stops starting replays and writes its own tick row in time."""
    work, sel_notes = select_draft_work(store, ops, watched, now)
    notes.extend(sel_notes)
    report: dict[str, Any] = {"entries": len(work), "tables": [w["table"] for w in work]}
    if not work:
        return report
    sources = bundle_sources()
    report["sources"] = len(sources)
    t0 = time.time()
    result = io.run_code(heartbeat_kernel_code(work, sources, budget_s=max(600, timeout_s - 300)), timeout=timeout_s)
    summary, events = parse_heartbeat_output(result)
    report["seconds"] = round(time.time() - t0, 1)
    report["events"] = [e.get("event") for e in events]
    status = str(result.get("status") or "").lower() if isinstance(result, dict) else ""
    err = result.get("error") if isinstance(result, dict) else None
    report["kernel_status"] = status or None
    if status in ("pending", "timeout", "timed_out"):
        report["error"] = f"client deadline ({timeout_s} s) reached before the heartbeat finished; the kernel session is left open so it can finish and write its own tick row"
        notes.append("draft pass: " + report["error"])
        io.keep_session = True   # closing the session would kill the heartbeat mid-replay (AidpIO.close honours this)
    if err:
        msg = f"{err.get('ename')}: {str(err.get('evalue'))[:300]}" if isinstance(err, dict) else str(err)[:300]
        report["error"] = msg
        notes.append(f"draft pass: the heartbeat run raised {msg}")
    if summary is None:
        if "error" not in report:
            report["error"] = "the heartbeat run printed no summary line"
            notes.append("draft pass: the heartbeat run printed no summary line (status " + (status or "unknown") + ")")
        report["summary"] = None
        return report
    report["summary"] = {k: summary.get(k) for k in ("tick_id", "job", "sandbox", "sandbox_dropped", "result", "elapsed_s")}
    report["verdicts"] = summary.get("replay_verdicts")
    for v in summary.get("replay_verdicts") or []:
        if not isinstance(v, dict):
            continue
        d = v.get("drafting") or {}
        made = [a for a in d.get("attempts", []) if isinstance(a, dict)]
        opened = d.get("approval_opened")
        notes.append(f"draft pass: {v.get('table')} {v.get('verdict')}"
                     + (f"; {len(made)} draft(s), " + ", ".join(str(a.get('verdict') or a.get('validation') or a.get('error', ''))[:40] for a in made) if made else "")
                     + (f"; approval opened {d.get('attempts', [{}])[-1].get('approval_id')}" if opened else "")
                     + (f"; skipped: {d.get('skipped')}" if d.get("skipped") else ""))
    return report


def tick(io: Any, store: Any, now: datetime | None = None, dry_run: bool = True,
         rules: list[Rule] | None = None, policy_rows: list[dict] | None = None,
         kv_store: KVStore | None = None, draft: bool = False, watched: list[dict] | None = None) -> TickReport:
    from . import gate, ledger, tiers

    now = now or datetime.now(timezone.utc)
    env = _env_of(io)
    ops = env.ops
    if hasattr(io, "dry_run"):
        io.dry_run = dry_run
    kv_store = kv_store or default_kv_store(env)
    tick_id = compute_tick_id(now, env.workspace_id)
    notes: list[str] = []

    # idempotent replay: a recorded tick_id means this 30-minute window already ran
    try:
        if store.run_sql(f"SELECT tick_id FROM {ops}.ticks WHERE tick_id = '{tick_id}'"):
            if draft:
                # the pass does not depend on the window key (the heartbeat it launches salts its own tick id)
                notes.append("replay: tick already recorded for this window; drafting pass only")
                return TickReport(tick_id, "unchanged", None, dry_run=dry_run, notes=notes,
                                  draft=_safe_draft_pass(io, store, ops, watched or [], now, notes))
            return TickReport(tick_id, "unchanged", None, dry_run=dry_run,
                              notes=["replay: tick already recorded for this window; nothing done"])
    except Exception as e:  # noqa: BLE001
        notes.append(f"replay check failed ({type(e).__name__}); continuing")

    # 2. context (one budget read) + 3. tier with kv persistence
    kv = kv_store.load()
    prev_tier = kv.get("prev_tier")
    if prev_tier is None:
        try:
            rows = store.run_sql(f"SELECT compute_tier FROM {ops}.ticks ORDER BY started_at DESC LIMIT 1")
            prev_tier = _row_get(rows[0], 0, "compute_tier") if rows else None
        except Exception:  # noqa: BLE001
            prev_tier = None
    ctx = build_context(io, store, now, kv=kv, notes=notes)
    tier, kv_updates = tiers.compute_tier(ctx, prev_tier, ctx.kv)
    ctx.kv.update(kv_updates or {})
    if ctx.budget_source == "unknown" and tier == "paused":
        notes.append("tier: budget unknown while paused; resume lock holds, no disable actions are planned")
    llm_tier = tiers.raw_tier(ctx.llm_remaining_pct, None, ctx.budget_source)
    ctx.kv["tier"] = tier
    if prev_tier and prev_tier != tier:
        notes.append(f"tier edge: {prev_tier} -> {tier}")

    # gate plumbing (shared by the approval drain and the planned actions): decision row first, the
    # engine fails closed if the write fails
    decisions: list[Decision] = []
    decision_ids: dict[str, str] = {}
    approvals = denied = quarantined = 0

    def write_decision(decision: Decision, action: Action, provenance: str = "actuator",
                       principal: str | None = None) -> str:
        did = _sha16(tick_id, decision.action_signature, decision.decision, decision.rule_id)
        store.run_sql(ledger.insert_decision(ops, {
            "decision_id": did, "tick_id": tick_id, "action_signature": decision.action_signature,
            "action_kind": action.kind, "target": action.target,
            "args_sha256": hashlib.sha256(json.dumps(action.params, sort_keys=True, default=str).encode()).hexdigest(),
            "decision": decision.decision, "rule_id": decision.rule_id,
            "rules_evaluated": decision.evaluated_rules,
            "rules_triggered": [decision.rule_id] if decision.rule_id else [],
            "reason": decision.reason, "provenance": provenance, "principal": principal or env.config_profile,
            "latency_ms": decision.latency_ms,
        }, now=now))
        decision_ids[decision.action_signature] = did
        return did

    def audit_sink(decision: Decision, action: Action) -> None:
        # a decision written while re-checking a row a person approved is marked as such: only a decision from the
        # planning path can stand behind an approval (ledger.quarantine_trace_sql)
        rechecking = decision.action_signature in (ctx.kv.get("approved_signatures") or {})
        write_decision(decision, action, provenance=ledger.DRAIN_PROVENANCE if rechecking else "actuator")

    def decision_counter(rule_id: str, target: str, window_hours: int = 24) -> int:
        rows = store.run_sql(ledger.rate_count_sql(ops, rule_id, target, window_hours, now=now))
        return int(_row_get(rows[0], 0, "n", 0) or 0) if rows else 0

    engine = gate.PolicyEngine(rules if rules is not None else gate.default_rules(), audit_sink,
                               decision_counter=decision_counter)
    actions: list[Action] = []
    executed: list[Action] = []
    outcomes: dict[str, Any] = {}

    def act_and_record(a: Action, did: str | None) -> str:
        """6. act (dry-run aware through io) + the action row. Returns the recorded status."""
        try:
            if a.kind == "statement_repair":
                # applying a repair means recording it as an approved runbook for the statement's owner; this is a
                # ledger write, so it is real even in dry-run, and the agent never touches anyone's job code
                outcome = _apply_statement_repair(store, ops, a, ctx.kv.get("approved_signatures", {}).get(a.signature), now)
                status = "applied"
            else:
                outcome = _act(io, a, notes)
                status = "planned_only" if isinstance(outcome, dict) and outcome.get("skipped") else (
                    "dry_run" if dry_run else "applied")
        except Exception as e:  # noqa: BLE001
            outcome, status = {"error": f"{type(e).__name__}: {e}"}, "failed"
            notes.append(f"act failed: {a.kind} {a.target}: {outcome['error']}")
        outcomes[a.signature] = outcome
        store.run_sql(ledger.insert_action(ops, {
            "tick_id": tick_id, "action_signature": a.signature, "kind": a.kind, "target": a.target,
            "params": a.params, "previous_value": a.previous_value, "reversible": a.reversible,
            "decision_id": did, "status": status, "result": outcome,
            "applied_at": now if status == "applied" else None,
        }, now=now))
        if status in ("applied", "dry_run"):
            executed.append(a)
        return status

    # 3b. drain approvals: expire stale rows, then execute what a human approved
    #     through the same gate. open_sigs = signatures that already have a pending/approved row;
    #     executed_sigs = signatures executed from an approval this tick (planning skips both).
    approvals_executed = 0
    open_sigs: set[str] = set()
    executed_sigs: set[str] = set()
    try:
        store.run_sql(ledger.expire_approvals_sql(ops, now))
    except Exception as e:  # noqa: BLE001
        notes.append(f"approvals: expire failed ({type(e).__name__}: {e})")
    try:
        open_sigs = {str(_row_get(r, 1, "action_signature")) for r in store.run_sql(ledger.pending_approvals_sql(ops))}
    except Exception as e:  # noqa: BLE001
        notes.append(f"approvals: pending read failed ({type(e).__name__}: {e}); duplicates possible this tick")
    try:
        approved_rows = list(store.run_sql(ledger.approved_unexecuted_sql(ops, now)))
    except Exception as e:  # noqa: BLE001
        approved_rows = []
        notes.append(f"approvals: approved read failed ({type(e).__name__}: {e}); nothing drained")
    # the latest replay verdict per statement hash, for verify.replay_required (fails closed when this read fails)
    try:
        ctx.kv["replay_verdicts"] = {str(_row_get(r, 0, "statement_sha256")): {"verdict": _row_get(r, 1, "verdict"),
                                                                                 "age_hours": float(_row_get(r, 2, "age_hours") or 0.0)}
                                     for r in store.run_sql(ledger.latest_replay_verdicts_sql(ops, 24, now))}
    except Exception as e:  # noqa: BLE001
        ctx.kv["replay_verdicts"] = {}
        notes.append(f"replay verdicts: read failed ({type(e).__name__}: {e}); repairs will be denied this tick")
    approved_sigs: dict[str, str] = {}
    for r in approved_rows:
        aid = str(_row_get(r, 0, "approval_id"))
        sig = str(_row_get(r, 1, "action_signature"))
        by = str(_row_get(r, 3, "approved_by") or "unknown")
        try:
            a = action_from_json(_row_get(r, 2, "action_json"))
        except Exception as e:  # noqa: BLE001
            notes.append(f"approval {aid}: action_json unreadable ({type(e).__name__}: {e}); marked failed")
            store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note=f"BAD_ACTION_JSON: {e}", now=now))
            continue
        if a.signature != sig:
            notes.append(f"approval {aid}: action_json does not match action_signature; marked failed")
            store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note="SIGNATURE_MISMATCH", now=now))
            continue
        # the same checks the in-cluster drain applies before the gate: an approvable kind, a quarantine this gate
        # recorded for this exact action, a person as approver, and the NS_APPROVERS allowlist for control-plane kinds
        from .drain import approval_problem, claim_approval, new_claim  # local import: drain imports this module
        _allow = [x for x in os.environ.get("NS_APPROVERS", "").split(",") if x.strip()]
        blocked = approval_problem(store, ops, engine, a, aid, by, _allow or None, now=now)
        if blocked:
            rule, problem, terminal = blocked
            if terminal:
                # not the gate's own row: it never counts as an open approval, so it cannot hold up planning
                notes.append(f"approval {aid}: {rule}: {problem}; marked failed")
                store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note=f"{rule}: {problem}", now=now))
            else:
                open_sigs.add(sig)
                notes.append(f"approval {aid}: {rule}: {problem}; left approved, not executed")
                store.run_sql(ledger.mark_approval_sql(ops, aid, "approved", note=f"DENIED_ON_EXECUTE {rule}: {problem}", now=now))
            continue
        open_sigs.add(sig)
        approved_sigs[sig] = by
        ctx.kv["approved_signatures"] = dict(approved_sigs)
        d = engine.evaluate(a, ctx)
        if dry_run and a.kind != "statement_repair" and d.decision in ("quarantine", "allow"):
            # a dry-run tick shows what it would do and leaves the approval for a tick that applies: it writes no
            # allow-by-approval decision, takes no claim and does not mark the row (a repair is a ledger write and
            # is real in either mode)
            actions.append(a)
            decisions.append(d)
            notes.append(f"approval {aid}: dry run; {a.kind} on {a.target} would be executed (approved by {by}); left approved")
            continue
        if d.decision == "allow":
            # the gate no longer quarantines this action (the state it was held for has changed); the approval is
            # still what authorises it, so the decision row says so
            notes.append(f"approval {aid}: the gate allows {a.kind} on {a.target} without a quarantine now; executed under the approval by {by}")
        if d.decision == "quarantine":
            # the quarantine rule still fires; the human approval is what lifts it (deny rules still deny)
            d = Decision(sig, "allow", d.rule_id, f"APPROVED_BY:{by}", evaluated_rules=d.evaluated_rules,
                         latency_ms=d.latency_ms)
            try:
                write_decision(d, a, provenance="approval", principal=by)
            except Exception as e:  # noqa: BLE001
                d = Decision(sig, "deny", d.rule_id, "AUDIT_WRITE_FAILED", evaluated_rules=d.evaluated_rules)
                notes.append(f"approval {aid}: allow-by-approval row could not be written ({type(e).__name__})")
        actions.append(a)
        decisions.append(d)
        if d.decision != "allow":
            notes.append(f"approval {aid}: re-evaluation denied ({d.rule_id} {d.reason}); left approved, not executed")
            store.run_sql(ledger.mark_approval_sql(ops, aid, "approved", note=f"DENIED_ON_EXECUTE {d.rule_id}: {d.reason}", now=now))
            continue
        claimed, why = claim_approval(store, ops, aid, new_claim(tick_id, aid), now)
        if not claimed:
            notes.append(f"approval {aid}: {why}; not executed here")
            continue
        status = act_and_record(a, decision_ids.get(sig))
        if status == "failed":
            store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note=str(outcomes[sig].get("error")), now=now))
        elif status == "planned_only":
            notes.append(f"approval {aid}: {a.kind} on {a.target} still planned only; left approved")
            store.run_sql(ledger.mark_approval_sql(ops, aid, "approved", note=f"PLANNED_ONLY: {outcomes[sig].get('skipped')}", now=now))
        else:
            store.run_sql(ledger.mark_approval_sql(ops, aid, "executed", note=f"{status} by tick {tick_id[:12]}", now=now))
            approvals_executed += 1
            executed_sigs.add(sig)
            notes.append(f"approval {aid}: executed {a.kind} on {a.target} ({status}, approved by {by})")
    ctx.kv.pop("approved_signatures", None)

    # 4. plan
    policy_rows = policy_rows if policy_rows is not None else tiers.default_policy_rows()
    planned = plan_actions(ctx, tier, policy_rows, io)

    # skip what an approval already covers (executed this tick, or still pending/approved)
    candidates: list[Action] = []
    for a in planned:
        if a.signature in executed_sigs:
            notes.append(f"plan: {a.kind} on {a.target} executed from an approval this tick; not re-planned")
        elif a.signature in open_sigs:
            notes.append(f"plan: {a.kind} on {a.target} awaiting approval; not re-planned (sig {a.signature[:12]})")
        else:
            candidates.append(a)

    # loop guard: same signature three ticks in a row -> stop re-applying
    streak: dict[str, int] = {k: int(v) for k, v in (ctx.kv.get("sig_streak") or {}).items()}
    gated: list[Action] = []
    for a in candidates:
        if streak.get(a.signature, 0) >= LOOP_GUARD_TICKS - 1:
            notes.append(f"loop guard: {a.kind} on {a.target} planned {LOOP_GUARD_TICKS} ticks in a row; "
                         f"not re-applied, incident recommended (sig {a.signature[:12]})")
            continue
        gated.append(a)
    ctx.kv["sig_streak"] = {a.signature: streak.get(a.signature, 0) + 1 for a in candidates}
    actions.extend(gated)

    # 5. gate the planned actions: execute only allow; approvals for quarantine
    for a in gated:
        d = engine.evaluate(a, ctx)
        decisions.append(d)
        did = decision_ids.get(a.signature)
        if d.decision == "quarantine":
            quarantined += 1
            if a.signature in open_sigs:
                notes.append(f"quarantine: {a.kind} on {a.target} already has an open approval; not re-opened")
                continue
            store.run_sql(ledger.insert_approval(ops, {
                "tick_id": tick_id, "action_signature": a.signature, "decision_id": did,
                "action_json": {"kind": a.kind, "target": a.target, "params": a.params,
                                "previous_value": a.previous_value, "reversible": a.reversible},
                "note": f"{d.rule_id}: {d.reason}", "expires_at": now + timedelta(hours=24),
                "status": "pending",
            }, now=now))
            approvals += 1
            open_sigs.add(a.signature)
            continue
        if d.decision != "allow":
            denied += 1
            notes.append(f"denied: {a.kind} {a.target}: {d.rule_id} {d.reason}")
            continue
        act_and_record(a, did)

    # 7. persist: consume wake events, save kv, then the tick row LAST (idempotent replay key)
    wake_consumed = 0
    if ctx.wake_events:
        store.run_sql(ledger.consume_wake_events_sql(ops, tick_id, now=now))
        wake_consumed = len(ctx.wake_events)
    if not planned:
        empty = int(ctx.kv.get("empty_diffs", 0)) + 1
        ctx.kv["empty_diffs"] = empty
        if empty >= 3:
            ctx.kv["sleep_until"] = (now + timedelta(hours=min(2 * (empty - 2), 6))).isoformat()
    else:
        ctx.kv["empty_diffs"] = 0
        ctx.kv["sleep_until"] = None
    ctx.kv["prev_tier"] = tier
    ctx.kv["last_tick_id"] = tick_id
    kv_store.save(ctx.kv)
    store.run_sql(ledger.insert_tick(ops, {
        "tick_id": tick_id, "started_at": now, "finished_at": datetime.now(timezone.utc),
        "compute_tier": tier, "llm_tier": llm_tier,
        "remaining_pct": ctx.budget_remaining_pct, "projected_overrun_pct": ctx.projected_overrun_pct,
        "llm_remaining_pct": ctx.llm_remaining_pct, "budget_source": ctx.budget_source,
        "actions_proposed": len(planned), "actions_applied": len(executed),
        "actions_quarantined": quarantined, "actions_denied": denied, "dry_run": dry_run,
    }, now=now))
    # 8. drafting pass (run_tick --draft), after the tick row: grade ungraded watched statements and draft the silently
    #    wrong ones from this session's kernel, which can call ai_generate; the approvals drained above count as live
    #    drafts, and nothing in this tick waits on the pass (it can run for most of an hour)
    draft_report = _safe_draft_pass(io, store, ops, watched or [], now, notes) if draft else None
    return TickReport(tick_id, tier, prev_tier, actions, decisions, approvals, wake_consumed, dry_run, notes, outcomes,
                      approvals_executed=approvals_executed, draft=draft_report)


def _safe_draft_pass(io: Any, store: Any, ops: str, watched: list[dict], now: datetime, notes: list[str]) -> dict[str, Any]:
    try:
        return run_draft_pass(io, store, ops, watched, now, notes)
    except Exception as e:  # noqa: BLE001  (drafting is best effort; the tick is already recorded)
        notes.append(f"draft pass failed: {type(e).__name__}: {e}; the kernel session is left open in case the heartbeat is still running")
        try:
            io.keep_session = True
        except Exception:  # noqa: BLE001
            pass
        return {"error": f"{type(e).__name__}: {e}"}


def _apply_statement_repair(store: Any, ops: str, a: Action, approved_by: str | None, now: datetime | None) -> dict:
    from . import ledger  # the tick imports lazily too; keep the module import-light for the kernel
    """An approved draft becomes a runbook row keyed on the original statement's hash, so the next incident on that
    statement proposes it first; the draft row is marked approved. The runbook procedure is the drafted MERGE itself."""
    p = a.params
    draft_rows = list(store.run_sql(
        f"SELECT draft_sql, table_name, incident_key, statement_sha256, validation FROM {ops}.repair_drafts "
        f"WHERE draft_id = {ledger.lit(p.get('draft_id'))}"))
    if not draft_rows:
        raise ValueError(f"draft {p.get('draft_id')} not found in repair_drafts")
    r = draft_rows[0]
    draft_sql, table_name, incident_key = _row_get(r, 0, "draft_sql"), _row_get(r, 1, "table_name"), _row_get(r, 2, "incident_key")
    stmt_sha, validation = _row_get(r, 3, "statement_sha256"), _row_get(r, 4, "validation")
    # the approval binds to the draft TEXT (hash) and the draft row binds to the STATEMENT it was drafted for;
    # both must agree with the approved params before anything is written
    if hashlib.sha256(str(draft_sql).encode("utf-8")).hexdigest() != str(p.get("draft_sha256")):
        raise ValueError("draft text changed since approval (hash mismatch); not applied")
    if str(stmt_sha) != str(p.get("statement_sha256")) or str(table_name).lower() != str(p.get("table")).lower():
        raise ValueError("draft was graded for a different statement or table than the approval names; not applied")
    if str(validation) != "accepted":
        raise ValueError(f"draft validation is {validation!r}, not accepted; not applied")
    runbook_id = _sha16("repair", str(p.get("statement_sha256")), str(p.get("draft_sha256")))
    store.run_sql(ledger.upsert_runbook(ops, {
        "runbook_id": runbook_id, "fingerprint_key": str(p.get("statement_sha256")), "label": "STATEMENT_REPAIR",
        "title": f"Replay-safe replacement for the MERGE on {table_name}",
        "procedure": str(draft_sql), "source_incident_key": incident_key, "approved_by": approved_by,
        "approved_at": now, "successes": 1, "provenance": f"draft:{p.get('model')}",
    }, now=now))
    store.run_sql(ledger.update_draft_status_sql(ops, str(p.get("draft_id")), "approved", runbook_id=runbook_id, now=now,
                                                 only_from=("proposed", "graded")))
    return {"op": "runbook_upsert", "runbook_id": runbook_id, "table": table_name, "approved_by": approved_by}


def _act(io: Any, a: Action, notes: list[str]) -> Any:
    if a.params.get("requires_schedule_body"):
        notes.append(f"{a.kind} on {a.target} planned only: job schedule body unknown")
        return {"skipped": "requires_schedule_body"}
    if a.kind == "cluster_stop":
        return io.stop_cluster(a.target)
    if a.kind == "cluster_update":
        return io.update_cluster_workers(a.target, a.params.get("min_workers", 1), a.params.get("max_workers", 1),
                                         a.params.get("auto_terminate_min"))
    if a.kind in ("job_update_schedule", "job_disable"):
        body = dict(io.get_job(a.target))
        if a.kind == "job_update_schedule":
            body["schedule"] = {**(body.get("schedule") or {}), "cron": a.params["cron"]}
        else:
            body["schedule"] = {**(body.get("schedule") or {}), "pauseStatus": "PAUSED"}
        return io.update_job(a.target, body)
    if a.kind in ("notify", "noop"):
        return {"noted": True}
    raise ValueError(f"unknown action kind {a.kind}")
