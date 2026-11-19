"""run-until-green orchestration: trigger /jobRuns, poll, repair failed tasks only."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

from aws_aidp.aidp_client import AidpClient
from aws_aidp.state import RunStore

TERMINAL_RUN: set[str] = {
    "SUCCEEDED", "SUCCESS", "FAILED", "INTERNAL_ERROR", "BLOCKED",
    "CANCELED", "CANCELLED", "TIMED_OUT", "SKIPPED", "EXCLUDED",
    "UPSTREAM_CANCELED", "UPSTREAM_FAILED",
}
ACTIVE_RUN: set[str] = {
    "RUNNING", "PENDING", "WAITING", "QUEUED", "CANCELING", "PAUSED_MAINTENANCE",
}
GREEN: set[str] = {"SUCCEEDED", "SUCCESS"}
FAILED_TASK: set[str] = {"FAILED", "TIMED_OUT", "ERROR", "INTERNAL_ERROR"}


@dataclass
class RunOutcome:
    run_key: str
    final_status: str
    repair_count: int
    failed_tasks: list[str] = field(default_factory=list)
    elapsed_s: float = 0.0


def _task_states(run: dict) -> list[tuple[str, str, str]]:
    """Normalize current and legacy AIDP task-state response shapes."""
    values: list[tuple[str, str, str]] = []
    task_to_run = run.get("taskToTaskRunMap")
    summaries = run.get("taskRunSummaryMap")
    if isinstance(task_to_run, dict) and isinstance(summaries, dict):
        for task_key, task_run_key in task_to_run.items():
            summary = summaries.get(task_run_key)
            if not isinstance(summary, dict):
                continue
            state = summary.get("state") or {}
            if not isinstance(state, dict):
                state = {}
            values.append((
                str(task_key or "<unknown>"),
                str(state.get("status") or "UNKNOWN").upper(),
                str(state.get("stateMessage") or ""),
            ))
        if values:
            return values

    # Compatibility with early API fixtures that embedded state on each task.
    for task in run.get("tasks", []) or []:
        if not isinstance(task, dict):
            continue
        state = task.get("state") or {}
        if not isinstance(state, dict):
            state = {}
        values.append((
            str(task.get("taskKey") or "<unknown>"),
            str(state.get("status") or "UNKNOWN").upper(),
            str(state.get("stateMessage") or ""),
        ))
    return values


def _failed_task_keys(run: dict) -> list[str]:
    keys: list[str] = []
    for task_key, status, _message in _task_states(run):
        if status in FAILED_TASK and task_key != "<unknown>" and task_key not in keys:
            keys.append(task_key)
    return keys


def _emit(line: str, log: Callable[[str], None] | None) -> None:
    if log is not None:
        log(line)


def poll_until_terminal(
    client: AidpClient,
    run_key: str,
    *,
    poll_interval: float = 30.0,
    timeout: float = 3 * 3600,
    log: Callable[[str], None] | None = None,
    store: RunStore | None = None,
) -> dict:
    """Block until the run reaches a terminal state. Emits per-task transitions."""
    if poll_interval < 0:
        raise ValueError("poll_interval must be zero or greater")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")
    prev_task: dict[str, str] = {}
    prev_run: str | None = None
    start = time.monotonic()
    while True:
        run = client.get_job_run(run_key)
        run_status = str((run.get("state") or {}).get("status") or "UNKNOWN").upper()
        if run_status != prev_run:
            _emit(f"RUN: {prev_run or '-'} -> {run_status}", log)
            prev_run = run_status
        for tk, ts, state_message in _task_states(run):
            if prev_task.get(tk) != ts:
                msg = state_message.replace("\n", " ")[:120]
                _emit(f"TASK {tk:<24s} {prev_task.get(tk, '-'):<10s} -> {ts:<10s} {msg}", log)
                prev_task[tk] = ts
        if store is not None:
            store.append(run_key, {"event": "poll", "status": run_status, "tasks": prev_task.copy()})
        if run_status in TERMINAL_RUN:
            return run
        if run_status not in ACTIVE_RUN:
            raise RuntimeError(
                f"run {run_key} returned unknown AIDP status {run_status!r}"
            )
        if time.monotonic() - start > timeout:
            raise TimeoutError(f"run {run_key} timed out after {timeout}s (last status: {run_status})")
        time.sleep(poll_interval)


def run_until_green(
    client: AidpClient,
    job_key: str,
    *,
    parameters: list | dict | None = None,
    max_repairs: int = 3,
    poll_interval: float = 30.0,
    timeout: float = 3 * 3600,
    log: Callable[[str], None] | None = None,
    store: RunStore | None = None,
) -> RunOutcome:
    """Trigger a run, then auto-repair-and-poll until GREEN or `max_repairs` exhausted."""
    if max_repairs < 0:
        raise ValueError("max_repairs must be zero or greater")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")
    if poll_interval < 0:
        raise ValueError("poll_interval must be zero or greater")
    start = time.monotonic()
    deadline = start + timeout
    _emit(f"POST /jobRuns jobKey={job_key} parameters={parameters or []}", log)
    run_key = client.create_job_run(job_key, parameters=parameters)
    if store is None:
        store = RunStore(job_key)
    store.append(run_key, {"event": "created", "job_key": job_key, "parameters": parameters})
    _emit(f"created runKey={run_key}", log)

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(f"run {run_key} timed out after {timeout}s during creation")
    run = poll_until_terminal(
        client, run_key, poll_interval=poll_interval,
        timeout=remaining, log=log, store=store,
    )
    repair_count = 0
    while True:
        state = run.get("state") or {}
        status = str(state.get("status") or "UNKNOWN").upper() if isinstance(state, dict) else "UNKNOWN"
        if status in GREEN:
            outcome = RunOutcome(run_key=run_key, final_status=status, repair_count=repair_count,
                                  elapsed_s=time.monotonic() - start)
            store.append(run_key, {"event": "green", "repair_count": repair_count})
            _emit(f"GREEN after {repair_count} repair(s) in {outcome.elapsed_s:.0f}s", log)
            return outcome
        failed = _failed_task_keys(run)
        if not failed or repair_count >= max_repairs:
            outcome = RunOutcome(run_key=run_key, final_status=status, repair_count=repair_count,
                                  failed_tasks=failed, elapsed_s=time.monotonic() - start)
            store.append(run_key, {"event": "stop", "status": status, "failed": failed,
                                    "repair_count": repair_count})
            _emit(f"STOP status={status} failed={failed} repairs={repair_count}", log)
            return outcome
        repair_count += 1
        _emit(f"repair attempt {repair_count}/{max_repairs}: taskKeys={failed}", log)
        client.repair_job_run(run_key, failed)
        store.append(run_key, {"event": "repair", "attempt": repair_count, "task_keys": failed})
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"run {run_key} timed out after {timeout}s during repair {repair_count}"
            )
        run = poll_until_terminal(
            client, run_key, poll_interval=poll_interval, timeout=remaining,
            log=log, store=store,
        )
