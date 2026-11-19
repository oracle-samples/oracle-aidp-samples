"""Drive an AIDP job from the operator's machine: run, poll, fetch output.

Local instrumentation for work that executes inside AIDP. Everything here is
live-verified (2026-09-16) and replaces the ad-hoc shell it grew out of:

  * `POST /workspaces/{ws}/jobRuns` with `{"jobKey": ...}` answers 201 and a
    run key; `GET /jobRuns/{key}` carries `state.status`, whose documented
    vocabulary is PENDING, QUEUED, RUNNING, CANCELING, PAUSED_MAINTENANCE
    (still going) and SUCCESS, FAILED, INTERNAL_ERROR, BLOCKED, CANCELED,
    UPSTREAM_CANCELED, UPSTREAM_FAILED, SKIPPED, EXCLUDED, TIMED_OUT (ended).
    Every ended status must be in TERMINAL_STATES: one that is not gets
    polled to the end of the budget and reported STILL RUNNING.
  * a task run's OUTPUT is two calls, not one: list the task runs
    (`--sort-by` is REQUIRED -- omitting it fails "Invalid SortBy: null"),
    then fetch that task run's output, which for a NOTEBOOK_TASK arrives as
    an executed notebook whose cell outputs hold the script's stdout.
  * a notebook cell reports SystemExit as an error, so the generated driver
    turns the exit code into a verdict -- see provision_api.

`call` is injected, so the polling and parsing are unit-tested offline.
"""
from __future__ import annotations

import json
import re
import time
from typing import Callable

__all__ = ["TERMINAL_STATES", "ACTIVE_STATES", "SUCCESS_STATES",
           "JobRunCollision",
           "in_flight_runs", "run_job", "job_run_status",
           "fetch_task_output", "extract_notebook_text", "watch_job",
           "task_started", "cancel_run", "refresh_run", "COLD_START_SECONDS",
           "COLD_START_RESTARTS"]

TERMINAL_STATES = ("SUCCESS", "FAILED", "CANCELED", "TIMED_OUT",
                   "UPSTREAM_FAILED", "UPSTREAM_CANCELED", "BLOCKED",
                   "INTERNAL_ERROR", "SKIPPED", "EXCLUDED")
# Still going. UNKNOWN is a transport hiccup or an absent status field: not
# a verdict, and not a reason for the cold-start watchdog to act either.
ACTIVE_STATES = ("PENDING", "QUEUED", "RUNNING", "CANCELING",
                 "PAUSED_MAINTENANCE", "UNKNOWN")
SUCCESS_STATES = ("SUCCESS",)
# What a poll reports when the status GET itself failed. Deliberately in
# neither set above: it is not a state of the run, so it is never a verdict,
# never "still running", and never a reason for the watchdog to cancel.
UNREADABLE = "UNREADABLE"
# A status error that will read the same on every later poll: an expired
# session or a refused credential. Polling on would spend the whole budget
# (40 x 30 s by default) re-reading an answer known on the first call.
_PERMANENT_STATUS_ERROR = re.compile(
    r"session profile has expired|\b401\b|\b403\b|NotAuthenticated",
    re.IGNORECASE)
# The cold-start watchdog's defaults (see watch_job). Two minutes for the
# cluster to PICK UP a task, and several attempts, because a fresh cluster
# has been seen to ignore two runs in a row (live 2026-09-29).
COLD_START_SECONDS = 120.0
COLD_START_RESTARTS = 5


class JobRunCollision(RuntimeError):
    """A run of this job is already in flight. Named so the CLI reports it as
    a message rather than a traceback, and so it is never mistaken for the
    job having failed."""


def in_flight_runs(call: Callable[..., dict], *, workspace: str,
                   job_key: str) -> list[str]:
    """Keys of runs of `job_key` that have not ended.

    A run is finished when it carries an `endTime`; the envelope has no status
    field of its own (the status lives on the task runs), so absence of an end
    is the signal. A transport that cannot list runs must not block a run --
    this is a guard, not a gate -- so any failure here returns nothing.
    """
    try:
        payload = call("list_job_runs", workspace=workspace, job_key=job_key)
    except Exception:
        return []
    items = (payload.get("items") if isinstance(payload, dict) else None) or []
    return [str(i.get("key")) for i in items
            if not i.get("endTime") and i.get("key")]


def run_job(call: Callable[..., dict], *, workspace: str, job_key: str,
            parameters: dict[str, str] | None = None) -> str:
    """Start a run and return its key. Raises if the server returns none."""
    from .provision_api import build_job_run_body
    payload = call("run_job", workspace=workspace,
                   body=build_job_run_body(job_key, parameters))
    key = payload.get("key") or payload.get("id")
    if not key:
        raise RuntimeError(
            f"the run was accepted but carried no key: "
            f"{json.dumps(payload)[:200]}")
    return str(key)


def job_run_status(call: Callable[..., dict], *, workspace: str,
                   run_key: str) -> dict:
    """{status, message} for one run. An absent status reads UNKNOWN, never
    as success."""
    payload = call("get_job_run", workspace=workspace, key=run_key)
    state = payload.get("state") or {}
    return {"status": str(state.get("status") or payload.get("status")
                          or "UNKNOWN"),
            "message": str(state.get("stateMessage") or "")}


def task_started(call: Callable[..., dict], *, workspace: str,
                 run_key: str) -> bool:
    """Has the cluster actually PICKED UP this run's task?

    The distinction the job-run status cannot make. A wedged run and a
    healthy one both report `RUNNING` on the envelope; what separates them is
    one field on the task run:

        wedged  (live 2026-09-19): task run exists, `startTime` is null
        healthy (live 2026-09-19): task run carries a real `startTime`

    So `startTime` is the signal, not the status. A transport that cannot
    list task runs returns True -- unknown must never be read as wedged, or
    the watchdog would cancel healthy runs on a transport hiccup.
    """
    try:
        listed = call("list_task_runs", workspace=workspace, run_key=run_key)
    except Exception:
        return True
    items = (listed.get("items") if isinstance(listed, dict) else None) or []
    if not items:
        return False
    return any(i.get("startTime") for i in items)


def cancel_run(call: Callable[..., dict], *, workspace: str, run_key: str,
               poll_seconds: float = 5.0, max_polls: int = 12,
               sleep: Callable[[float], None] = time.sleep,
               on_cancel_error: Callable[[str], None] | None = None) -> str:
    """Cancel a run and poll it to a terminal state; returns the last state
    read, which the CALLER must check against TERMINAL_STATES.

    The cancel answers 202, which is acceptance and not completion, so the
    run is read back. This is polled to terminal rather than fired and
    forgotten because `maxConcurrentRuns: 1` means a resubmit while the old
    run still holds the slot is accepted and then silently discarded.

    A cancel that raises is not swallowed silently: the poll below is still
    the real answer (the run may already have ended), but the error text
    goes to `on_cancel_error`, because "the cancel never happened" -- the
    `aidp` CLI missing on an oci-only machine is the realistic case, it
    being the one job operation routed through that CLI -- must reach the
    report rather than read as a slow cancel.
    """
    try:
        call("cancel_job_run", workspace=workspace, run_key=run_key)
    except Exception as exc:
        if on_cancel_error:
            on_cancel_error(f"{type(exc).__name__}: {str(exc)[:200]}")
    status = "UNKNOWN"
    for _ in range(max_polls):
        try:
            status = job_run_status(call, workspace=workspace,
                                    run_key=run_key)["status"]
        except Exception:
            status = "UNKNOWN"
        if status in TERMINAL_STATES:
            return status
        sleep(poll_seconds)
    return status


def fetch_task_output(call: Callable[..., dict], *, workspace: str,
                      run_key: str) -> str:
    """The first task run's output text, or '' when there is none.

    Two calls, because a job run holds task runs and only a task run has
    output. A run that failed BEFORE launching a task has no task runs at
    all -- which is itself the diagnosis, and is returned as ''.
    """
    listed = call("list_task_runs", workspace=workspace, run_key=run_key)
    items = listed.get("items") or []
    if not items:
        return ""
    task_key = str(items[0].get("key") or items[0].get("taskRunKey") or "")
    if not task_key:
        return ""
    payload = call("fetch_task_output", workspace=workspace,
                   task_run_key=task_key)
    return extract_notebook_text(payload)


# The job runner's own call to the cluster failing, not the notebook: live
# 2026-09-29 a 76-minute copy ended FAILED on "TransientServiceError ...
# 'target_service': 'dataflowdp', 'status': 503 ... get_command_status".
_PLATFORM_TRANSIENT = re.compile(
    r"TransientServiceError|'status':\s*50[234]\b|ServiceUnavailable")
ERROR_TRACE_CHARS = 2000


def task_error_trace(call: Callable[..., dict], *, workspace: str,
                     run_key: str) -> str | None:
    """The first task run's `state.errorTrace`, or None.

    Only a task run read by key carries it: the job run's envelope says
    "Exception during execution of notebook" whatever happened, and the
    list items omit it. Never raises -- the verdict stands without it.
    """
    try:
        items = call("list_task_runs", workspace=workspace,
                     run_key=run_key).get("items") or []
        for item in items:
            key = str(item.get("key") or item.get("taskRunKey") or "")
            if not key:
                continue
            task = call("get_task_run", workspace=workspace, task_run_key=key)
            trace = ((task.get("state") or {}).get("errorTrace")
                     or task.get("errorTrace"))
            if trace:
                return str(trace)[:ERROR_TRACE_CHARS]
    except Exception:
        return None
    return None


def extract_notebook_text(payload: dict) -> str:
    """Every cell output of an executed notebook, concatenated.

    The envelope is `data: [{type: NOTEBOOK, value: "<nbformat json>"}]`, and
    a cell's text lives under `outputs[].text` or `outputs[].data['text/plain']`.
    Anything unparseable is returned as-is rather than swallowed: a failed
    run's only evidence must not be dropped on a shape surprise.
    """
    blocks = payload.get("data")
    if isinstance(blocks, str):
        return blocks
    text: list[str] = []
    for block in blocks or []:
        value = block.get("value") if isinstance(block, dict) else None
        if not value:
            continue
        try:
            notebook = json.loads(value)
        except (TypeError, ValueError):
            text.append(str(value))
            continue
        for cell in notebook.get("cells") or []:
            for out in cell.get("outputs") or []:
                chunk = (out.get("text")
                         or (out.get("data") or {}).get("text/plain") or "")
                if isinstance(chunk, list):
                    chunk = "".join(chunk)
                if chunk:
                    text.append(chunk)
                if out.get("ename"):
                    text.append(f'{out["ename"]}: {out.get("evalue")}')
    return "".join(text)


def watch_job(call: Callable[..., dict], *, workspace: str, job_key: str,
              parameters: dict[str, str] | None = None,
              poll_seconds: float = 30.0, max_polls: int = 40,
              on_poll: Callable[[str, int], None] | None = None,
              cold_start_seconds: float = COLD_START_SECONDS,
              cold_start_restarts: int = COLD_START_RESTARTS,
              on_restart: Callable[[str, str], None] | None = None,
              sleep: Callable[[float], None] = time.sleep,
              on_submit: Callable[[str], None] | None = None) -> dict:
    """Run a job, poll to a terminal state, and bring back its output.

    Returns {run_key, status, message, output, terminal, restarts, polls,
    unrecognised, status_unreadable, status_error}. `terminal: False` means the budget ran out with the job
    still going -- reported as running, never rounded to either verdict.
    `unrecognised: True` means the last status is in neither TERMINAL_STATES
    nor ACTIVE_STATES: a vocabulary this code does not know, which is not
    "still running" either, so the caller must not report it as such.

    A status GET that FAILS is a poll that read nothing, not the end of the
    watch: it reports UNREADABLE, counts against the budget, and the next
    poll tries again (an expired session or a 401/403 stops at once -- it
    will not clear). It used to raise straight out of here after the run
    had been submitted, so the caller wrote no record and the run key was
    lost. `status_unreadable: True` means the LAST poll read nothing, so
    the run's real state is unknown; `on_submit` hears every run key the
    moment it exists, so a caller can name it whatever happens next.

    THE COLD-START WATCHDOG. A cluster sometimes never picks up a job run --
    characteristically the FIRST run on a freshly created workspace. The run
    sits at `RUNNING` with its task unstarted, indefinitely: it does not fail,
    so nothing times out, and an operator watching a status field sees a job
    that is apparently working. Observed live 2026-09-19: the first run on a
    new workspace sat 9+ minutes untouched, and an identical run submitted
    after cancelling it succeeded in 90 seconds.

    So after `cold_start_seconds` with the task still unstarted, the run is
    cancelled and resubmitted, up to `cold_start_restarts` times. The budget
    is deliberately generous to measure only the pick-up, not the work: it
    checks whether the cluster TOOK the task, which is independent of how
    long the task then runs. Set `cold_start_restarts=0` to disable.

    ONE RESTART IS NOT ENOUGH. Live 2026-09-29, on a fresh workspace: the
    first run was cancelled at 65s as designed, and the resubmitted run
    wedged too -- 16 minutes at RUNNING, task PENDING, `startTime: null` --
    while the watch, its single restart spent, kept polling it as if it
    were working. A manual cancel and a third submission then succeeded in
    90 seconds. Hence the defaults: two minutes to pick up (the operators'
    rule of thumb) and several attempts. And when the attempts are spent
    and the task has STILL not started, the watch stops and says so
    (`cold_start_exhausted: True`) instead of spending the poll budget on a
    run the cluster is not going to take.

    The resubmit happens ONLY once the cancel is confirmed terminal. If the
    cancel raises or the run never leaves CANCELING within the cancel poll,
    the slot is still held; a resubmit would be accepted and discarded, and
    the watch would then describe the discarded run. So the original run
    is kept and watched, the failed attempt is recorded in `restarts` with
    `new_run: None`, and the result carries `cancel_unconfirmed: True` so
    the caller can say "cold start suspected; cancel unconfirmed" and exit
    non-zero instead of STILL RUNNING.
    """
    # A job created with `maxConcurrentRuns: 1` still ACCEPTS a second run
    # while the first is going -- and then never executes it: the run is
    # created, ends the instant it starts, and produces no task output. Polled
    # naively that reads as a terminal run with nothing in it, while the
    # console streams the OLD run's log, so the operator sees stale output and
    # concludes the fix they just deployed did not take. Refuse instead, and
    # name the run holding the slot.
    active = in_flight_runs(call, workspace=workspace, job_key=job_key)
    if active:
        raise JobRunCollision(
            f"job {job_key} already has {len(active)} run(s) in flight: "
            + ", ".join(active)
            + ". A job with maxConcurrentRuns=1 executes one run at a time, "
              "so a second run started now would not execute. Wait for it, "
              "or cancel it "
              "(`aidp workflow cancel-job-run <workspace> <run-key>`) -- and "
              "note that a notebook re-uploaded mid-run does NOT affect the "
              "run already going.")
    run_key = run_job(call, workspace=workspace, job_key=job_key,
                      parameters=parameters)
    if on_submit:
        on_submit(run_key)
    status, message = "UNKNOWN", ""
    status_error = None
    terminal = False
    restarts: list[dict] = []
    restarts_left = max(0, cold_start_restarts)
    exhausted: dict | None = None
    waited = 0.0
    attempt = 0
    polls_left = max_polls
    while polls_left > 0:
        sleep(poll_seconds)
        polls_left -= 1
        waited += poll_seconds
        attempt += 1
        try:
            state = job_run_status(call, workspace=workspace, run_key=run_key)
            status, message = state["status"], state["message"]
            status_error = None
        except Exception as exc:
            status_error = str(exc)[:300]
            status, message = UNREADABLE, status_error
        if on_poll:
            on_poll(status, attempt)
        if status_error is not None:
            if _PERMANENT_STATUS_ERROR.search(status_error):
                break
            continue
        if status in TERMINAL_STATES:
            terminal = True
            break
        # The watchdog acts only on a run KNOWN to be going: a status it
        # cannot classify is not a cold start, and cancelling it would turn
        # an unknown into a discarded run.
        # Not when the run in hand is one whose cancel already failed to
        # confirm: that is the cancel-unconfirmed path, reported as such.
        if (cold_start_restarts > 0 and not restarts_left
                and not (restarts and restarts[-1].get("new_run") is None)
                and status in ACTIVE_STATES
                and waited >= cold_start_seconds
                and not task_started(call, workspace=workspace,
                                     run_key=run_key)):
            # Every attempt is spent and this run has not been picked up
            # either. Watching it further only burns the budget on a run the
            # cluster is ignoring; stop and let the caller say so. It is
            # cancelled first: left alone it holds the job's only slot, and
            # the next `run` would be refused as a collision with a run that
            # is doing nothing.
            cancel_errors = []
            exhausted = {"run": run_key, "after_seconds": waited,
                         "cancel_state": cancel_run(
                             call, workspace=workspace, run_key=run_key,
                             sleep=sleep,
                             on_cancel_error=cancel_errors.append),
                         "cancel_error": (cancel_errors[0]
                                          if cancel_errors else None)}
            break
        if (restarts_left and status in ACTIVE_STATES
                and waited >= cold_start_seconds
                and not task_started(call, workspace=workspace,
                                     run_key=run_key)):
            # The cluster has not taken the task. Let this run go and submit
            # another -- polling the cancel to terminal first, because the
            # slot must be free or the resubmit is accepted and discarded.
            cancel_errors: list[str] = []
            ended = cancel_run(call, workspace=workspace, run_key=run_key,
                               sleep=sleep, on_cancel_error=cancel_errors.append)
            restarts_left -= 1
            after, waited = waited, 0.0
            if ended not in TERMINAL_STATES:
                # The slot is NOT free. Keep watching the run we have; the
                # attempt is on the record and the caller reports it.
                restarts.append({"abandoned_run": None, "cancel_state": ended,
                                 "cancel_error": (cancel_errors[0]
                                                  if cancel_errors else None),
                                 "new_run": None, "kept_run": run_key,
                                 "after_seconds": after})
                continue
            stale = run_key
            run_key = run_job(call, workspace=workspace, job_key=job_key,
                              parameters=parameters)
            if on_submit:
                on_submit(run_key)
            restarts.append({"abandoned_run": stale, "cancel_state": ended,
                             "cancel_error": (cancel_errors[0]
                                              if cancel_errors else None),
                             "new_run": run_key,
                             "after_seconds": after})
            # The budget the caller set describes how long to watch A RUN.
            # It was spent on a run that never started, so the replacement
            # gets it back rather than inheriting the leftovers and being
            # reported STILL RUNNING after a poll or two. Bounded overall by
            # cold_start_restarts, which is what caps the total wait.
            polls_left = max_polls
            if on_restart:
                on_restart(stale, run_key)
            status, message = "UNKNOWN", ""
    output = ""
    try:
        output = fetch_task_output(call, workspace=workspace, run_key=run_key)
    except Exception as exc:  # the verdict still stands without the log
        output = f"(output unavailable: {str(exc)[:200]})"
    # Why a run that ended badly ended: the fetched output is only the head
    # of a long notebook's log (~10,800 characters live), so the failure is
    # not in it.
    error_trace = (task_error_trace(call, workspace=workspace, run_key=run_key)
                   if terminal and status not in SUCCESS_STATES else None)
    return {"run_key": run_key, "status": status, "message": message,
            "output": output, "error_trace": error_trace,
            "platform_transient": bool(
                error_trace and _PLATFORM_TRANSIENT.search(error_trace)),
            "terminal": terminal, "restarts": restarts,
            "polls": attempt,
            "unrecognised": ((not terminal) and status not in ACTIVE_STATES
                             and status != UNREADABLE),
            "status_unreadable": status == UNREADABLE,
            "status_error": status_error,
            # None, or {run, after_seconds, cancel_state, cancel_error}: the
            # attempts ran out and this last run was never picked up either.
            "cold_start_exhausted": exhausted,
            # The state of the run being WATCHED, not of every attempt
            # ever made. An earlier attempt that could not confirm its
            # cancel is history the caller can read in `restarts`; if a
            # later one cancelled cleanly and resubmitted, the run in hand
            # is on a slot that was free.
            "cancel_unconfirmed": bool(restarts)
                                  and restarts[-1].get("new_run") is None,
            "ok": terminal and status in SUCCESS_STATES}


def refresh_run(call: Callable[..., dict], *, workspace: str, run_key: str,
                poll_seconds: float = 30.0, max_polls: int = 40,
                on_poll: Callable[[str, int], None] | None = None,
                sleep: Callable[[float], None] = time.sleep) -> dict:
    """Re-read a run that already exists -- NEVER submit, cancel or resubmit.

    For a local record that went stale: the poll budget ran out while the
    job went on (live 2026-09-29, S10 read STILL RUNNING and ended SUCCESS),
    or the run was started from the console and has no record at all. Polls
    the run to a terminal state within the budget, like watch_job, then
    brings back its output. The result has watch_job's shape, plus
    `job_key_seen` -- the job the run belongs to, per AIDP -- so a caller
    can refuse to file one job's run under another's record.
    """
    status, message, status_error = "UNKNOWN", "", None
    terminal, job_key_seen, attempt = False, None, 0
    for attempt in range(1, max(1, max_polls) + 1):
        if attempt > 1:
            sleep(poll_seconds)
        try:
            payload = call("get_job_run", workspace=workspace, key=run_key)
            state = payload.get("state") or {}
            status = str(state.get("status") or payload.get("status")
                         or "UNKNOWN")
            message = str(state.get("stateMessage") or "")
            job_key_seen = payload.get("jobKey") or job_key_seen
            status_error = None
        except Exception as exc:
            status_error = str(exc)[:300]
            status, message = UNREADABLE, status_error
        if on_poll:
            on_poll(status, attempt)
        if status_error is not None:
            if _PERMANENT_STATUS_ERROR.search(status_error):
                break
            continue
        if status in TERMINAL_STATES:
            terminal = True
            break
    output = ""
    try:
        output = fetch_task_output(call, workspace=workspace, run_key=run_key)
    except Exception as exc:
        output = f"(output unavailable: {str(exc)[:200]})"
    return {"run_key": run_key, "status": status, "message": message,
            "output": output, "terminal": terminal, "restarts": [],
            "polls": attempt,
            "unrecognised": ((not terminal) and status not in ACTIVE_STATES
                             and status != UNREADABLE),
            "status_unreadable": status == UNREADABLE,
            "status_error": status_error,
            "cold_start_exhausted": None, "cancel_unconfirmed": False,
            "ok": terminal and status in SUCCESS_STATES,
            "job_key_seen": job_key_seen}
