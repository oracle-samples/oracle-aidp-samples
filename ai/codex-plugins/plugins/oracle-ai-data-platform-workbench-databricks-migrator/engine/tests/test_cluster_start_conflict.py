import asyncio
from types import SimpleNamespace

import pytest

from scripts import cluster_lifecycle as lifecycle
from scripts import cluster_states


def _response(status_code, text=""):
    return SimpleNamespace(status_code=status_code, text=text)


def test_lifecycle_consumes_shared_cluster_state_taxonomy():
    assert lifecycle.STARTABLE_CLUSTER_STATES is (
        cluster_states.STARTABLE_CLUSTER_STATES
    )
    assert lifecycle.TERMINAL_CLUSTER_STATES is (
        cluster_states.TERMINAL_CLUSTER_STATES
    )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"state": "starting"}, "STARTING"),
        ({"lifecycleState": " active "}, "ACTIVE"),
        ({"lifecycleState": "failed"}, "FAILED"),
        ({"lifecycleState": "restarting"}, "RESTARTING"),
        ({}, "UNKNOWN"),
    ],
)
def test_get_cluster_state_normalizes_api_state(monkeypatch, payload, expected):
    calls = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return payload

    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(
        lifecycle.http_requests,
        "get",
        lambda *args, **kwargs: calls.append((args, kwargs)) or Response(),
    )

    assert lifecycle.get_cluster_state("cluster-id") == expected
    assert calls[0][1]["timeout"] == 60


def test_start_cluster_treats_active_after_409_as_success(monkeypatch):
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _cluster_id: "ACTIVE")
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(
            409,
            "Unable to accept request due to an ongoing operation on the cluster",
        ),
    )

    lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1
    assert sleeps == []


def test_start_cluster_fails_loudly_when_initial_reconcile_get_fails(monkeypatch):
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(
        lifecycle,
        "get_cluster_state",
        lambda _cluster_id: (_ for _ in ()).throw(ConnectionError("GET unavailable")),
    )
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(RuntimeError, match="Cannot reconcile.*ConnectionError"):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1
    assert sleeps == []


def test_start_cluster_fails_loudly_when_settle_get_fails(monkeypatch):
    states = iter(["STOPPING", ConnectionError("settle GET unavailable")])
    posts = []
    sleeps = []

    def next_state(_cluster_id):
        value = next(states)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", next_state)
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(RuntimeError, match="Cannot reconcile.*ConnectionError"):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1
    assert sleeps == [30]


def test_start_cluster_does_not_resubmit_when_starting_after_409(monkeypatch):
    posts = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _cluster_id: "STARTING")
    monkeypatch.setattr(
        lifecycle.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected sleep")),
    )
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1


@pytest.mark.parametrize("state", ["UNKNOWN", "FOO"])
def test_start_cluster_refuses_unrecognised_state_after_409(monkeypatch, state):
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _cluster_id: state)
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(RuntimeError, match=rf"unrecognised \({state}\)"):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1
    assert sleeps == []


def test_start_cluster_retries_409_when_cluster_remains_stopped(monkeypatch):
    responses = iter([_response(409, "busy"), _response(202)])
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _cluster_id: "STOPPED")
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or next(responses),
    )

    lifecycle.start_cluster("cluster-id")

    assert len(posts) == 2
    assert sleeps == [30]


@pytest.mark.parametrize("shutdown_state", ["STOPPING", "TERMINATING"])
def test_start_cluster_waits_for_shutdown_before_resubmitting(
        monkeypatch, shutdown_state):
    responses = iter([_response(409, "busy"), _response(202)])
    states = iter([shutdown_state, shutdown_state, "STOPPED"])
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _cluster_id: next(states))
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or next(responses),
    )

    assert lifecycle.start_cluster("cluster-id") == "START_REQUESTED"

    assert len(posts) == 2
    assert sleeps == [30, 30]


@pytest.mark.parametrize("shutdown_state", ["STOPPING", "TERMINATING"])
def test_start_cluster_persistent_shutdown_does_not_resubmit(
        monkeypatch, shutdown_state):
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(
        lifecycle, "get_cluster_state", lambda _cluster_id: shutdown_state
    )
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(RuntimeError, match="remained in shutdown state"):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1
    assert sleeps == [30] * 31


def test_start_cluster_shutdown_settle_budget_is_shared_across_409_cycles(
        monkeypatch):
    posts = []
    sleeps = []
    # First 409 consumes the full settle budget and reaches STOPPED.  The
    # immediate resubmission receives another 409/STOPPING; the budget must not
    # reset for another 930 seconds.
    states = iter(["STOPPING"] + ["STOPPING"] * 30 + ["STOPPED", "STOPPING"])
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(RuntimeError, match="STOPPING for 930s"):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 2
    assert sleeps == [30] * 31


def test_start_cluster_refuses_unrecognised_state_after_shutdown_settles(
        monkeypatch):
    states = iter(["STOPPING", "FOO"])
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(RuntimeError, match=r"settled to unexpected state \(FOO\)"):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1
    assert sleeps == [30]


def test_start_cluster_exhaustion_reports_attempts_and_last_state(monkeypatch):
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: "STOPPED")
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(
        RuntimeError,
        match=r"after 6 start attempts; last reconciled live state: STOPPED",
    ):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 6
    assert sleeps == [30, 60, 120, 240, 480]


@pytest.mark.parametrize("state", ["FAILED", "DELETED", "DELETING"])
def test_start_cluster_fails_immediately_on_terminal_state_after_409(
        monkeypatch, state):
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _cluster_id: state)
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(
            409, "Unable to accept request due to an ongoing operation on the cluster"
        ),
    )

    with pytest.raises(RuntimeError, match=state):
        lifecycle.start_cluster("cluster-id")

    assert len(posts) == 1
    assert sleeps == []


def test_ensure_cluster_running_reconciles_409_starting_to_active(monkeypatch):
    states = iter(["STOPPED", "STARTING", "ACTIVE"])
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected sync sleep")),
    )
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(
            409, "Unable to accept request due to an ongoing operation on the cluster"
        ),
    )

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=60))

    assert len(posts) == 1
    assert sleeps == [30]


def test_ensure_cluster_running_restarts_when_progress_settles_to_stopped(
        monkeypatch):
    states = iter(["STOPPED", "UPDATING", "STOPPED", "STARTING", "ACTIVE"])
    responses = iter([_response(409, "library operation"), _response(202)])
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(
            AssertionError("unexpected synchronous backoff")
        ),
    )
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or next(responses),
    )

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=120))

    assert len(posts) == 2
    assert sleeps == [30, 30, 30]
    assert posts[0][1]["timeout"] <= 60
    assert posts[1][1]["timeout"] <= 60


def test_ensure_cluster_running_does_not_duplicate_accepted_start_before_progress(
        monkeypatch):
    """A 202-accepted start may remain STOPPED for one control-plane poll."""
    states = iter(["STOPPED", "STOPPED", "ACTIVE"])
    responses = iter([_response(202), _response(409, "duplicate start")])
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(
            AssertionError("unexpected synchronous backoff")
        ),
    )
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or next(responses),
    )

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=90))

    assert len(posts) == 1
    assert sleeps == [30, 30]


def test_ensure_cluster_running_restarts_after_accepted_start_makes_progress_then_stops(
        monkeypatch):
    states = iter(["STOPPED", "STARTING", "STOPPED", "ACTIVE"])
    responses = iter([_response(202), _response(202)])
    posts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or next(responses),
    )

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=120))

    assert len(posts) == 2
    assert sleeps == [30, 30, 30]


def test_ensure_cluster_running_bounds_start_retries_by_caller_timeout(
        monkeypatch):
    clock = [0.0]
    posts = []
    sync_sleeps = []

    def advance(seconds):
        sync_sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(lifecycle, "_monotonic", lambda: clock[0])
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: "STOPPED")
    monkeypatch.setattr(lifecycle.time, "sleep", advance)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )

    with pytest.raises(
        RuntimeError,
        match=r"after 2 start attempts \(last state: STOPPED\)",
    ):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=45))

    assert clock[0] == 45
    assert sync_sleeps == [30, 15]
    assert len(posts) == 2


@pytest.mark.parametrize("progress_state", ["STARTING", "UPDATING"])
def test_ensure_cluster_running_caps_progress_to_stopped_reentries(
        monkeypatch, progress_state):
    starts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: "STOPPED")

    def start(cluster_id, deadline=None):
        starts.append((cluster_id, deadline))
        return progress_state

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(lifecycle, "start_cluster", start)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    with pytest.raises(
        RuntimeError,
        match=(
            rf"start regressed from {progress_state} to STOPPED 4 times after "
            r"4 start requests; refusing further starts"
        ),
    ):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=600))

    assert len(starts) == 4
    assert sleeps == [30, 30, 30, 30]


@pytest.mark.parametrize("shutdown_state", ["STOPPING", "TERMINATING"])
def test_ensure_cluster_running_shutdown_timeout_is_not_unexpected_state(
        monkeypatch, shutdown_state):
    starts = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(
        lifecycle, "get_cluster_state", lambda _id: shutdown_state
    )
    monkeypatch.setattr(
        lifecycle,
        "start_cluster",
        lambda *args, **kwargs: starts.append((args, kwargs)),
    )

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    with pytest.raises(
        RuntimeError,
        match=rf"did not become ACTIVE.*last state: {shutdown_state}",
    ):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=60))

    assert starts == []


def test_ensure_cluster_running_shutdown_can_recover_into_start_progress(
        monkeypatch):
    """A concurrent start during shutdown must fall through to readiness."""
    states = iter(["STOPPING", "STARTING", "ACTIVE"])
    starts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle,
        "start_cluster",
        lambda *args, **kwargs: starts.append((args, kwargs)),
    )

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=60))

    assert starts == []
    assert sleeps == [30, 30]


def test_ensure_cluster_running_fails_fast_on_unknown_shutdown_settle(
        monkeypatch):
    states = iter(["STOPPING", "UNKNOWN"])
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    with pytest.raises(RuntimeError, match="unexpected state 'UNKNOWN'.*shutdown"):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=600))


@pytest.mark.parametrize("state", ["TERMINATED", "UNKNOWN", "PAUSED"])
def test_ensure_cluster_running_fails_fast_on_unrecognised_poll_state(
        monkeypatch, state):
    states = iter(["STOPPED", state])
    starts = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))

    def start(cluster_id, deadline=None):
        starts.append((cluster_id, deadline))
        return "START_REQUESTED"

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle, "start_cluster", start)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    with pytest.raises(
        RuntimeError,
        match=rf"unexpected state '{state}'.*waiting for ACTIVE",
    ):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=600))

    assert len(starts) == 1


def test_ensure_cluster_running_skips_poll_when_409_reconciles_active(monkeypatch):
    states = iter(["STOPPED", "ACTIVE"])
    posts = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected sync sleep")),
    )
    monkeypatch.setattr(
        lifecycle.http_requests,
        "post",
        lambda *args, **kwargs: posts.append((args, kwargs)) or _response(409, "busy"),
    )
    monkeypatch.setattr(
        lifecycle.asyncio,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected poll")),
    )

    asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=60))
    assert len(posts) == 1


def test_ensure_cluster_running_surfaces_failed_state_after_accepted_start(
        monkeypatch):
    states = iter(["STOPPED", "STARTING", "FAILED"])
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected sync sleep")),
    )
    monkeypatch.setattr(
        lifecycle.http_requests, "post", lambda *args, **kwargs: _response(409, "busy")
    )

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    with pytest.raises(RuntimeError, match="entered state 'FAILED'"):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=60))


def test_ensure_cluster_running_recovers_initial_failed_state_once(monkeypatch):
    states = iter(["FAILED", "STARTING", "ACTIVE"])
    starts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))

    def start(cluster_id, deadline=None):
        starts.append((cluster_id, deadline))
        return "START_REQUESTED"

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(lifecycle, "start_cluster", start)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    asyncio.run(
        lifecycle.ensure_cluster_running(
            "cluster-id",
            timeout=60,
            allow_failed_recovery=True,
        )
    )

    assert len(starts) == 1
    assert len(sleeps) == 2
    assert sum(sleeps) <= 60


def test_ensure_cluster_running_failed_recovery_fails_again_without_retry(
        monkeypatch):
    states = iter(["FAILED", "STARTING", "FAILED"])
    starts = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))

    def start(cluster_id, deadline=None):
        starts.append((cluster_id, deadline))
        return "START_REQUESTED"

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle, "start_cluster", start)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    with pytest.raises(RuntimeError, match="entered state 'FAILED'"):
        asyncio.run(
            lifecycle.ensure_cluster_running(
                "cluster-id",
                timeout=60,
                allow_failed_recovery=True,
            )
        )

    assert len(starts) == 1


def test_ensure_cluster_running_failed_recovery_times_out_without_retry(
        monkeypatch):
    clock = [0.0]
    starts = []
    sleeps = []
    monkeypatch.setattr(lifecycle, "_monotonic", lambda: clock[0])
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: "FAILED")

    def start(cluster_id, deadline=None):
        starts.append((cluster_id, deadline))
        return "START_REQUESTED"

    async def advance(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(lifecycle, "start_cluster", start)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", advance)

    with pytest.raises(
        RuntimeError,
        match=r"after 1 start attempts \(last state: FAILED\)",
    ):
        asyncio.run(
            lifecycle.ensure_cluster_running(
                "cluster-id",
                timeout=60,
                allow_failed_recovery=True,
            )
        )

    assert len(starts) == 1
    assert sum(sleeps) == 60


def test_ensure_cluster_running_failed_recovery_does_not_restart_after_regression(
        monkeypatch):
    states = iter(["FAILED", "STARTING", "STOPPED"])
    starts = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))

    def start(cluster_id, deadline=None):
        starts.append((cluster_id, deadline))
        return "START_REQUESTED"

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle, "start_cluster", start)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)

    with pytest.raises(RuntimeError, match="single FAILED-state recovery"):
        asyncio.run(
            lifecycle.ensure_cluster_running(
                "cluster-id",
                timeout=60,
                allow_failed_recovery=True,
            )
        )

    assert len(starts) == 1


def test_ensure_cluster_running_does_not_recover_failed_state_without_opt_in(
        monkeypatch):
    starts = []
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: "FAILED")
    monkeypatch.setattr(
        lifecycle,
        "start_cluster",
        lambda *args, **kwargs: starts.append((args, kwargs)),
    )

    with pytest.raises(RuntimeError, match="cannot auto-start"):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=60))

    assert starts == []


def test_ensure_cluster_running_zero_timeout_does_not_start(monkeypatch):
    states = iter(["STOPPED"])
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _id: "target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _id: next(states))
    monkeypatch.setattr(
        lifecycle.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected sync sleep")),
    )
    monkeypatch.setattr(
        lifecycle.http_requests, "post", lambda *args, **kwargs: _response(409, "busy")
    )

    with pytest.raises(RuntimeError, match="last state: STOPPED"):
        asyncio.run(lifecycle.ensure_cluster_running("cluster-id", timeout=0))
