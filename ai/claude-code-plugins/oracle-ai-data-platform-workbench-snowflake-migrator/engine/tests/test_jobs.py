"""Driving an AIDP job from outside: run, poll, fetch the output.

Every shape here came from a live run, including the awkward ones: a task
run's output is an executed NOTEBOOK, and a run that dies before launching a
task has no task runs at all.
"""
import json

import pytest

from target import jobs

from target.jobs import (
    TERMINAL_STATES, extract_notebook_text, fetch_task_output, job_run_status,
    run_job, watch_job)


def _notebook_payload(*chunks, ename=None):
    nb = {"cells": [{"outputs": [{"text": list(chunks)}]
                     + ([{"ename": ename, "evalue": "boom"}] if ename else [])}]}
    return {"data": [{"type": "NOTEBOOK", "value": json.dumps(nb)}]}


class Fake:
    def __init__(self, *, states, output=None, task_runs=1):
        self.states = list(states)
        self.output = output if output is not None else _notebook_payload("ok\n")
        self.task_runs = task_runs
        self.ops: list[str] = []

    def __call__(self, operation, **kw):
        self.ops.append(operation)
        if operation == "run_job":
            return {"key": "run-1"}
        if operation == "get_job_run":
            state = self.states.pop(0) if self.states else "SUCCESS"
            return {"state": {"status": state, "stateMessage": f"in {state}"}}
        if operation == "list_task_runs":
            # A real `startTime`: the cluster took the task. Its absence is
            # what the cold-start watchdog reads as never-picked-up.
            return {"items": [{"key": f"task-{i}", "startTime": 1789854291517}
                              for i in range(self.task_runs)]}
        if operation == "fetch_task_output":
            return self.output
        raise AssertionError(operation)


def test_a_run_returns_its_key():
    assert run_job(Fake(states=[]), workspace="ws", job_key="j") == "run-1"


def test_a_run_with_no_key_raises_rather_than_returning_nothing():
    def call(operation, **kw):
        return {}

    with pytest.raises(RuntimeError, match="no key"):
        run_job(call, workspace="ws", job_key="j")


def test_an_absent_status_reads_unknown_not_success():
    def call(operation, **kw):
        return {}

    assert job_run_status(call, workspace="ws",
                          run_key="r")["status"] == "UNKNOWN"
    assert "UNKNOWN" not in TERMINAL_STATES


def test_watch_polls_until_a_terminal_state():
    fake = Fake(states=["PENDING", "RUNNING", "SUCCESS"])
    seen = []
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None,
                       on_poll=lambda status, n: seen.append(status))
    assert seen == ["PENDING", "RUNNING", "SUCCESS"]
    assert result["status"] == "SUCCESS"
    assert result["terminal"] is True and result["ok"] is True
    assert "ok" in result["output"]


def test_a_failure_is_terminal_but_not_ok():
    fake = Fake(states=["RUNNING", "FAILED"])
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None)
    assert result["terminal"] is True
    assert result["ok"] is False
    assert result["status"] == "FAILED"


def test_a_budget_that_runs_out_is_reported_as_still_running():
    # Neither verdict: the job is simply not finished, and saying otherwise
    # is the failure mode this whole plugin is built against.
    fake = Fake(states=["RUNNING"] * 10)
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       max_polls=2, sleep=lambda _s: None)
    assert result["terminal"] is False
    assert result["ok"] is False
    assert result["status"] == "RUNNING"


def test_the_output_comes_from_the_task_run_not_the_job_run():
    fake = Fake(states=["SUCCESS"])
    watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
              sleep=lambda _s: None)
    assert "list_task_runs" in fake.ops
    assert "fetch_task_output" in fake.ops


def test_a_run_with_no_task_runs_yields_no_output_rather_than_raising():
    # A job that fails BEFORE launching its task has no task runs; that is
    # itself the diagnosis.
    fake = Fake(states=["FAILED"], task_runs=0)
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None)
    assert result["output"] == ""
    assert result["status"] == "FAILED"


def test_an_unfetchable_output_does_not_lose_the_verdict():
    class NoOutput(Fake):
        def __call__(self, operation, **kw):
            if operation == "fetch_task_output":
                raise RuntimeError("404")
            return super().__call__(operation, **kw)

    result = watch_job(NoOutput(states=["SUCCESS"]), workspace="ws",
                       job_key="j", poll_seconds=0, sleep=lambda _s: None)
    assert result["ok"] is True
    assert "unavailable" in result["output"]


def test_notebook_text_includes_stdout_and_the_exception():
    text = extract_notebook_text(
        _notebook_payload("[discover] 11 schema(s)\n", ename="RuntimeError"))
    assert "11 schema(s)" in text
    assert "RuntimeError: boom" in text


def test_an_unparseable_output_is_returned_rather_than_swallowed():
    text = extract_notebook_text({"data": [{"value": "not json at all"}]})
    assert "not json at all" in text


def test_a_plain_string_payload_is_passed_through():
    assert extract_notebook_text({"data": "raw log"}) == "raw log"


def test_fetch_task_output_is_two_calls():
    fake = Fake(states=[])
    assert "ok" in fetch_task_output(fake, workspace="ws", run_key="r")
    assert fake.ops == ["list_task_runs", "fetch_task_output"]


def test_a_second_run_is_refused_while_one_is_in_flight():
    """maxConcurrentRuns=1 ACCEPTS a second run and then discards it: created,
    ended instantly, no task output. The console meanwhile streams the OLD
    run's log, so a freshly deployed fix looks like it never took."""
    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": [{"key": "older", "endTime": None},
                              {"key": "done", "endTime": 123}]}
        raise AssertionError(f"must not reach {op}")

    with pytest.raises(jobs.JobRunCollision) as exc:
        jobs.watch_job(call, workspace="ws", job_key="j", sleep=lambda s: None)
    msg = str(exc.value)
    assert "older" in msg and "done" not in msg
    assert "cancel-job-run" in msg


def test_a_finished_previous_run_does_not_block_the_next():
    started = {}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": [{"key": "done", "endTime": 123}]}
        if op == "run_job":
            started["yes"] = True
            return {"key": "new"}
        if op == "get_job_run":
            return {"status": "SUCCESS"}
        return {}

    res = jobs.watch_job(call, workspace="ws", job_key="j",
                         sleep=lambda s: None, max_polls=1)
    assert started.get("yes") is True
    assert res["ok"] is True


def test_a_transport_that_cannot_list_runs_is_a_guard_not_a_gate():
    """The guard must never be the reason a migration cannot run."""
    def call(op, **kw):
        if op == "list_job_runs":
            raise RuntimeError("not supported on this build")
        if op == "run_job":
            return {"key": "new"}
        if op == "get_job_run":
            return {"status": "SUCCESS"}
        return {}

    res = jobs.watch_job(call, workspace="ws", job_key="j",
                         sleep=lambda s: None, max_polls=1)
    assert res["ok"] is True


# --- the cold-start watchdog ------------------------------------------------
#
# A cluster sometimes never picks up a job run -- characteristically the first
# run on a newly created workspace. Live 2026-09-19: the run sat at RUNNING
# for 9+ minutes with `startTime: null` on its task run and never failed;
# resubmitting the identical job after a cancel succeeded in 90 seconds. The
# job-run status cannot see this, so the task run's `startTime` is the signal.


class ColdStart:
    """A cluster that ignores the first N runs, then behaves."""

    def __init__(self, *, ignore_runs=1):
        self.ignore_runs = ignore_runs
        self.submitted: list[str] = []
        self.cancelled: list[str] = []

    def _wedged(self, run_key):
        return self.submitted.index(run_key) < self.ignore_runs

    def __call__(self, operation, **kw):
        if operation == "list_job_runs":
            return {"items": []}
        if operation == "run_job":
            key = f"run-{len(self.submitted) + 1}"
            self.submitted.append(key)
            return {"key": key}
        if operation == "get_job_run":
            key = kw["key"]
            if key in self.cancelled:
                return {"state": {"status": "CANCELED"}}
            # A wedged run reports RUNNING forever -- it never fails, which
            # is exactly why nothing times out and the operator sees nothing.
            return {"state": {"status": "RUNNING" if self._wedged(key)
                              else "SUCCESS"}}
        if operation == "list_task_runs":
            started = None if self._wedged(kw["run_key"]) else 1789854291517
            return {"items": [{"key": "t1", "startTime": started}]}
        if operation == "cancel_job_run":
            self.cancelled.append(kw["run_key"])
            return {}
        if operation == "fetch_task_output":
            return _notebook_payload("ok\n")
        raise AssertionError(operation)


def test_a_run_the_cluster_never_picks_up_is_cancelled_and_resubmitted():
    fake = ColdStart(ignore_runs=1)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=10,
                    sleep=lambda s: None)
    assert fake.submitted == ["run-1", "run-2"]
    assert fake.cancelled == ["run-1"]
    assert res["ok"] is True
    # The result must point at the run that actually ran, not the first one.
    assert res["run_key"] == "run-2"


def test_the_restart_is_recorded_as_evidence_not_just_retried_quietly():
    """A reader comparing the report against the console has to be able to
    see that the output belongs to a different run key than was submitted."""
    fake = ColdStart(ignore_runs=1)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=10, sleep=lambda s: None)
    assert len(res["restarts"]) == 1
    entry = res["restarts"][0]
    assert entry["abandoned_run"] == "run-1"
    assert entry["new_run"] == "run-2"
    assert entry["cancel_state"] == "CANCELED"


def test_a_healthy_run_is_never_restarted_however_long_it_takes():
    """The budget measures PICK-UP, not work: a task that started and is
    still going is fine, and cancelling it would destroy real progress."""
    calls = {"n": 0}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": []}
        if op == "run_job":
            calls["n"] += 1
            return {"key": "run-1"}
        if op == "get_job_run":
            return {"state": {"status": "RUNNING"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": 1789854291517}]}
        if op == "fetch_task_output":
            return _notebook_payload("still going\n")
        raise AssertionError(op)

    res = watch_job(call, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=20, sleep=lambda s: None)
    assert calls["n"] == 1           # submitted once, never resubmitted
    assert res["terminal"] is False  # budget ran out; not a verdict
    assert res["restarts"] == []


def test_the_watchdog_gives_up_rather_than_restarting_forever():
    fake = ColdStart(ignore_runs=99)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, cold_start_restarts=2,
                    max_polls=20, sleep=lambda s: None)
    assert len(fake.submitted) == 3   # the original plus two restarts
    assert res["terminal"] is False   # never a verdict
    # ...and it STOPS on the last unstarted run instead of polling it for
    # the rest of the budget, cancelling it so it does not hold the slot.
    assert res["cold_start_exhausted"]["run"] == "run-3"
    assert res["cold_start_exhausted"]["cancel_state"] == "CANCELED"
    assert fake.cancelled == ["run-1", "run-2", "run-3"]
    assert res["polls"] < 20


def test_the_defaults_survive_a_cluster_that_ignores_two_runs():
    """Live 2026-09-29: a fresh cluster ignored the first run AND its one
    resubmission; the old default (one restart) then watched the second
    wedged run for 16 minutes. The third submission ran in 90 seconds."""
    fake = ColdStart(ignore_runs=2)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    max_polls=40, sleep=lambda s: None)
    assert res["ok"] is True
    assert res["run_key"] == "run-3"
    assert res["cold_start_exhausted"] is None


def test_the_default_pick_up_budget_is_two_minutes():
    fake = ColdStart(ignore_runs=1)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    max_polls=40, sleep=lambda s: None)
    assert res["restarts"][0]["after_seconds"] == 120


def test_the_watchdog_can_be_switched_off():
    fake = ColdStart(ignore_runs=99)
    watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
              cold_start_seconds=60, cold_start_restarts=0, max_polls=5,
              sleep=lambda s: None)
    assert fake.submitted == ["run-1"]
    assert fake.cancelled == []


def test_a_transport_that_cannot_list_task_runs_never_cancels_a_run():
    """Unknown must not read as wedged: on a transport hiccup the watchdog
    would otherwise kill healthy runs."""
    def call(op, **kw):
        if op == "list_task_runs":
            raise RuntimeError("not supported on this build")
        raise AssertionError(op)

    assert jobs.task_started(call, workspace="ws", run_key="r") is True


def test_a_run_with_no_task_runs_at_all_has_not_started():
    def call(op, **kw):
        return {"items": []}

    assert jobs.task_started(call, workspace="ws", run_key="r") is False


def test_a_cancel_is_polled_to_terminal_because_202_is_not_done():
    """The slot must be free before a resubmit: maxConcurrentRuns=1 accepts a
    second run while the first still holds it, and then discards it."""
    seen = {"n": 0}

    def call(op, **kw):
        if op == "cancel_job_run":
            return {}
        if op == "get_job_run":
            seen["n"] += 1
            return {"state": {"status": "RUNNING" if seen["n"] < 3
                              else "CANCELED"}}
        raise AssertionError(op)

    state = jobs.cancel_run(call, workspace="ws", run_key="r",
                            sleep=lambda s: None)
    assert state == "CANCELED"
    assert seen["n"] == 3


# --- the full State vocabulary ---------------------------------------------
#
# The API's State.status enum is PENDING, QUEUED, RUNNING, SKIPPED,
# INTERNAL_ERROR, BLOCKED, SUCCESS, FAILED, CANCELING, CANCELED,
# UPSTREAM_CANCELED, UPSTREAM_FAILED, EXCLUDED, TIMED_OUT, PAUSED_MAINTENANCE.
# TERMINAL_STATES used to omit INTERNAL_ERROR, SKIPPED, UPSTREAM_CANCELED and
# EXCLUDED, so a run that died that way -- the cluster failing to start is the
# realistic case -- was polled for the whole budget, cancelled and resubmitted
# once by the cold-start watchdog, and then reported STILL RUNNING with exit 0.

_API_STATES = {"PENDING", "QUEUED", "RUNNING", "SKIPPED", "INTERNAL_ERROR",
               "BLOCKED", "SUCCESS", "FAILED", "CANCELING", "CANCELED",
               "UPSTREAM_CANCELED", "UPSTREAM_FAILED", "EXCLUDED", "TIMED_OUT",
               "PAUSED_MAINTENANCE"}


def test_the_state_sets_cover_the_api_vocabulary_and_do_not_overlap():
    assert not set(TERMINAL_STATES) & set(jobs.ACTIVE_STATES)
    assert set(TERMINAL_STATES) | set(jobs.ACTIVE_STATES) >= _API_STATES
    assert "UNKNOWN" in jobs.ACTIVE_STATES, \
        "a transport hiccup must stay non-terminal, and must not restart"


@pytest.mark.parametrize("state", ["INTERNAL_ERROR", "SKIPPED",
                                   "UPSTREAM_CANCELED", "EXCLUDED"])
def test_a_dead_run_ends_the_watch_as_terminal_and_not_ok(state):
    fake = Fake(states=["PENDING", state])
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None)
    assert result["terminal"] is True
    assert result["ok"] is False
    assert result["status"] == state
    assert fake.ops.count("run_job") == 1
    assert "cancel_job_run" not in fake.ops


def test_a_dead_run_is_never_cancelled_and_resubmitted_by_the_watchdog():
    """The cluster failed to start: the run ended INTERNAL_ERROR within a
    minute and its task never got a startTime. That is a verdict, not a
    cold start."""
    counts = {"run_job": 0, "cancel_job_run": 0}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": []}
        if op in counts:
            counts[op] += 1
            return {"key": "run-%d" % counts["run_job"]}
        if op == "get_job_run":
            return {"state": {"status": "INTERNAL_ERROR"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": None}]}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)

    res = watch_job(call, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=6, sleep=lambda s: None)
    assert counts == {"run_job": 1, "cancel_job_run": 0}
    assert res["terminal"] is True and res["ok"] is False
    assert res["status"] == "INTERNAL_ERROR"


def test_an_unrecognised_status_is_flagged_and_never_restarted():
    """A status this code does not know is neither a verdict nor "still
    running", and the watchdog must not cancel a run it cannot classify."""
    counts = {"run_job": 0, "cancel_job_run": 0}

    def call(op, **kw):
        if op == "list_job_runs":
            return {"items": []}
        if op in counts:
            counts[op] += 1
            return {"key": "run-%d" % counts["run_job"]}
        if op == "get_job_run":
            return {"state": {"status": "SOME_FUTURE_STATE"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": None}]}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)

    res = watch_job(call, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=4, sleep=lambda s: None)
    assert res["terminal"] is False and res["ok"] is False
    assert res["unrecognised"] is True
    assert res["status"] == "SOME_FUTURE_STATE"
    assert counts == {"run_job": 1, "cancel_job_run": 0}


def test_a_spent_budget_on_a_running_job_is_not_unrecognised():
    fake = Fake(states=["RUNNING"] * 10)
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       max_polls=3, sleep=lambda _s: None)
    assert result["terminal"] is False
    assert result["unrecognised"] is False
    assert result["polls"] == 3, "the report says how much budget was spent"


@pytest.mark.parametrize("state", TERMINAL_STATES)
def test_every_terminal_state_ends_a_cancel_poll(state):
    seen = {"n": 0}

    def call(op, **kw):
        if op == "cancel_job_run":
            return {}
        if op == "get_job_run":
            seen["n"] += 1
            return {"state": {"status": state}}
        raise AssertionError(op)

    assert jobs.cancel_run(call, workspace="ws", run_key="r",
                           sleep=lambda s: None) == state
    assert seen["n"] == 1


# --- a cancel that never lands must not be followed by a resubmit ----------
#
# The watchdog used to call run_job whatever cancel_run came back with, and
# cancel_run swallowed every exception from the cancel itself. So on a machine
# with only the `oci` CLI (the cancel is the one job operation routed through
# `aidp`), or when the cancel sat in CANCELING past the poll, run-2 went into
# the slot run-1 still held, was accepted and discarded, and the report then
# described the discarded run -- as SUCCESS with no output if AIDP marks it so.


class CancelNeverLands(ColdStart):
    """`missing_cli`: cancel_job_run raises like a missing binary.
    `canceling`: the cancel is accepted and the run never leaves CANCELING."""

    def __init__(self, mode):
        super().__init__(ignore_runs=99)
        self.mode = mode
        self.cancel_attempts = 0

    def __call__(self, operation, **kw):
        if operation == "cancel_job_run":
            self.cancel_attempts += 1
            if self.mode == "missing_cli":
                raise FileNotFoundError(2, "aidp not found")
            self.cancelled.append(kw["run_key"])
            return {}
        if operation == "get_job_run" and self.mode == "canceling" \
                and kw["key"] in self.cancelled:
            return {"state": {"status": "CANCELING"}}
        return super().__call__(operation, **kw)


def test_a_cancel_that_raises_is_recorded_and_nothing_is_resubmitted():
    fake = CancelNeverLands("missing_cli")
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, cold_start_restarts=1,
                    max_polls=10, sleep=lambda s: None)
    assert fake.submitted == ["run-1"], "the slot was never freed"
    assert res["run_key"] == "run-1"
    assert res["terminal"] is False and res["ok"] is False
    assert res["cancel_unconfirmed"] is True
    entry = res["restarts"][0]
    assert entry["new_run"] is None and entry["kept_run"] == "run-1"
    assert "FileNotFoundError" in entry["cancel_error"]
    assert fake.cancel_attempts == 1, "the one restart was spent on it"


def test_a_cancel_stuck_in_canceling_is_not_followed_by_a_resubmit():
    fake = CancelNeverLands("canceling")
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=10, sleep=lambda s: None)
    assert fake.submitted == ["run-1"]
    entry = res["restarts"][0]
    assert entry["cancel_state"] == "CANCELING"
    assert entry["new_run"] is None
    assert res["cancel_unconfirmed"] is True


def test_a_confirmed_cancel_still_resubmits_and_is_not_flagged():
    fake = ColdStart(ignore_runs=1)
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=10, sleep=lambda s: None)
    assert fake.submitted == ["run-1", "run-2"]
    assert res["cancel_unconfirmed"] is False
    assert res["restarts"][0]["cancel_error"] is None


def test_cancel_run_reports_the_cancel_error_instead_of_swallowing_it():
    seen = []

    def call(op, **kw):
        if op == "cancel_job_run":
            raise FileNotFoundError(2, "aidp not found")
        if op == "get_job_run":
            return {"state": {"status": "RUNNING"}}
        raise AssertionError(op)

    state = jobs.cancel_run(call, workspace="ws", run_key="r", max_polls=2,
                            sleep=lambda s: None, on_cancel_error=seen.append)
    assert state == "RUNNING"
    assert len(seen) == 1 and "FileNotFoundError" in seen[0]
    assert "aidp not found" in seen[0]


# ------------------------------------------------ two watch_job edge cases
#
# Both were reported as "low confidence, confusing-but-not-crashing". Both
# are real.
#
# 1. `cancel_unconfirmed` was `any(new_run is None for r in restarts)` --
#    the whole restart HISTORY. A first attempt that cannot confirm its
#    cancel keeps the original run and records `new_run: None`; a second
#    attempt that cancels cleanly and resubmits records a real `new_run`.
#    The run being watched is then a properly submitted one on a free slot,
#    and the flag still said the cancel was unconfirmed -- so the CLI
#    printed "cold start suspected; cancel unconfirmed" and exited non-zero
#    about a run that was fine.
#
# 2. The poll budget was set once and never restored. After a restart
#    `waited` resets but `polls_left` does not, so the new run inherits
#    whatever the abandoned one left -- and STILL RUNNING can be reported
#    for a run that was barely watched.

class _FlakyCancel:
    """First cold start: the cancel never reaches a terminal state, so the
    original run is kept. Second: the cancel confirms and a new run goes in,
    which then succeeds."""

    def __init__(self):
        self.submitted: list[str] = []
        self.cancel_attempts = 0
        self.confirmed: set = set()

    def __call__(self, operation, **kw):
        if operation == "list_job_runs":
            return {"items": []}
        if operation == "run_job":
            key = f"run-{len(self.submitted) + 1}"
            self.submitted.append(key)
            return {"key": key}
        if operation == "get_job_run":
            key = kw["key"]
            if key in self.confirmed:
                return {"state": {"status": "CANCELED"}}
            # run-2 is the healthy resubmission.
            if key == "run-2":
                return {"state": {"status": "SUCCESS"}}
            return {"state": {"status": "RUNNING"}}
        if operation == "list_task_runs":
            started = 1789854291517 if kw["run_key"] == "run-2" else None
            return {"items": [{"key": "t1", "startTime": started}]}
        if operation == "cancel_job_run":
            self.cancel_attempts += 1
            if self.cancel_attempts >= 2:      # the second one confirms
                self.confirmed.add(kw["run_key"])
            return {}
        if operation == "fetch_task_output":
            return _notebook_payload("done\n")
        raise AssertionError(operation)


def test_cancel_unconfirmed_describes_the_run_being_watched():
    fake = _FlakyCancel()
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=30,
                    cold_start_restarts=2, sleep=lambda s: None)
    assert len(res["restarts"]) == 2, res["restarts"]
    assert res["restarts"][0]["new_run"] is None      # the failed cancel
    assert res["restarts"][1]["new_run"] == "run-2"   # the confirmed one
    assert res["cancel_unconfirmed"] is False, (
        "the run being watched was submitted onto a slot whose cancel WAS "
        "confirmed; the earlier failed attempt is history, not its state")


def test_an_unconfirmed_cancel_on_the_last_attempt_is_still_reported():
    """The flag must keep working where it belongs."""
    class NeverConfirms(_FlakyCancel):
        def __call__(self, operation, **kw):
            if operation == "cancel_job_run":
                return {}                     # never reaches terminal
            return super().__call__(operation, **kw)

    res = watch_job(NeverConfirms(), workspace="ws", job_key="j",
                    poll_seconds=30, cold_start_seconds=60, max_polls=12,
                    cold_start_restarts=1, sleep=lambda s: None)
    assert res["restarts"][-1]["new_run"] is None
    assert res["cancel_unconfirmed"] is True


class _SlowSecondRun:
    """run-1 is wedged; run-2 is picked up but needs three polls to finish.

    With the budget shared, run-2 is watched for whatever run-1 left and
    reported STILL RUNNING. With the budget restored it reaches SUCCESS.
    """

    def __init__(self):
        self.submitted: list[str] = []
        self.polls_of_run2 = 0

    def __call__(self, operation, **kw):
        if operation == "list_job_runs":
            return {"items": []}
        if operation == "run_job":
            key = f"run-{len(self.submitted) + 1}"
            self.submitted.append(key)
            return {"key": key}
        if operation == "get_job_run":
            key = kw["key"]
            if key == "run-2":
                self.polls_of_run2 += 1
                return {"state": {"status": "SUCCESS" if
                                  self.polls_of_run2 >= 3 else "RUNNING"}}
            return {"state": {"status": "CANCELED" if self.cancelled
                              else "RUNNING"}}
        if operation == "list_task_runs":
            started = 1789854291517 if kw["run_key"] == "run-2" else None
            return {"items": [{"key": "t1", "startTime": started}]}
        if operation == "cancel_job_run":
            self.cancelled = True
            return {}
        if operation == "fetch_task_output":
            return _notebook_payload("done\n")
        raise AssertionError(operation)

    cancelled = False


def test_the_poll_budget_is_restored_for_a_resubmitted_run():
    """The budget the caller set describes how long to watch A RUN. A run
    that replaces a wedged one gets that budget, not its leftovers.

    max_polls=3: two polls wedge run-1 and trigger the restart, leaving one.
    run-2 needs three. Sharing the budget reports STILL RUNNING about a run
    that was watched once.
    """
    fake = _SlowSecondRun()
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=3,
                    cold_start_restarts=1, sleep=lambda s: None)
    assert fake.submitted == ["run-1", "run-2"]
    assert res["run_key"] == "run-2"
    assert res["terminal"] is True, (
        "run-2 was picked up and finished; it was reported unfinished only "
        "because it inherited run-1's spent budget")
    assert res["ok"] is True


def test_the_restored_budget_is_still_bounded():
    """Restarts are capped, so the budget cannot be renewed forever."""
    fake = ColdStart(ignore_runs=99)          # never picks anything up
    res = watch_job(fake, workspace="ws", job_key="j", poll_seconds=30,
                    cold_start_seconds=60, max_polls=4,
                    cold_start_restarts=2, sleep=lambda s: None)
    assert res["terminal"] is False
    assert len(res["restarts"]) <= 2
    assert len(fake.submitted) <= 3           # original + 2 restarts


# --- a status poll that fails after the submit does not end the watch --------
#
# Found on review (contested; the double-load half was refuted -- the
# in-flight guard and 02's verified-skip prevent it -- and the evidence half
# kept). watch_job called job_run_status bare, so ONE failed GET after POST
# /jobRuns -- a 503, an expired session -- raised straight out of the watch:
# exit 1, no run_<job>.json, no RUN_<job>.md, the run key visible only in an
# echoed GET URI, and an expired session printed "nothing was sent ...
# re-run this stage" about a run that WAS sent and may still be copying.

class _FlakyStatus(Fake):
    """get_job_run raises on the polls listed in `fail_on` (1-based)."""

    def __init__(self, *, states, fail_on=(), error="503 Service Unavailable"):
        super().__init__(states=states)
        self.fail_on, self.error, self.polls = set(fail_on), error, 0

    def __call__(self, operation, **kw):
        if operation == "get_job_run":
            self.polls += 1
            if self.polls in self.fail_on or "*" in self.fail_on:
                self.ops.append(operation)
                raise RuntimeError(self.error)
        return super().__call__(operation, **kw)


def test_one_failed_status_poll_is_survived():
    fake = _FlakyStatus(states=["RUNNING", "SUCCESS"], fail_on={2})
    seen = []
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       sleep=lambda _s: None,
                       on_poll=lambda status, n: seen.append(status))
    assert seen == ["RUNNING", "UNREADABLE", "SUCCESS"]
    assert result["status"] == "SUCCESS" and result["ok"] is True
    assert result["run_key"] == "run-1"
    assert result["polls"] == 3, "the failed poll counts against the budget"


def test_a_status_that_is_never_readable_is_reported_with_the_run_key():
    fake = _FlakyStatus(states=[], fail_on={"*"})
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       max_polls=4, sleep=lambda _s: None)
    assert result["run_key"] == "run-1"
    assert result["terminal"] is False and result["ok"] is False
    assert result["status"] == "UNREADABLE"
    assert result["status_unreadable"] is True
    assert result["unrecognised"] is False, "unreadable is not a new state"
    assert "503" in result["status_error"]
    assert result["polls"] == 4
    assert "cancel_job_run" not in fake.ops, "unreadable is not a cold start"


def test_an_expired_session_stops_the_watch_at_once():
    """The answer cannot change within the run, so the budget is not spent
    on it -- 40 polls x 30 s of the same error would be twenty minutes."""
    fake = _FlakyStatus(states=[], fail_on={"*"},
                        error="get_job_run: the OCI CLI session profile has "
                              "expired")
    result = watch_job(fake, workspace="ws", job_key="j", poll_seconds=0,
                       max_polls=40, sleep=lambda _s: None)
    assert result["polls"] == 1
    assert result["status_unreadable"] is True


def test_every_submitted_run_key_is_announced():
    keys = []
    watch_job(Fake(states=["SUCCESS"]), workspace="ws", job_key="j",
              poll_seconds=0, sleep=lambda _s: None, on_submit=keys.append)
    assert keys == ["run-1"]
