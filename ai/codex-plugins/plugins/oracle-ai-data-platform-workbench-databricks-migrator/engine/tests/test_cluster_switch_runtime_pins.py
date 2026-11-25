"""Regression coverage for runtime pins across migration cluster changes."""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, Mock, patch

import pytest


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

import cluster_lifecycle
import phase1_cluster_readiness
from cluster_session import ClusterSession


def _isolated_session():
    instance = object.__new__(ClusterSession)
    instance._initialized = False
    ClusterSession.__init__(instance)
    instance._cluster_id = "old-cluster"
    instance._bootstrap_snippets = []
    instance.close = AsyncMock()
    instance.connect = AsyncMock()
    return instance


def test_switch_cluster_requires_ready_target_before_connecting():
    """A task cluster must pass the library/restart gate before connection."""
    session = _isolated_session()
    calls = []

    async def ready(lifecycle, cluster_id):
        calls.append(("ready", lifecycle, cluster_id))

    with patch.object(phase1_cluster_readiness, "prepare_cluster", ready):
        asyncio.run(session.switch_cluster("task-cluster", session_name="task"))

    assert calls == [
        ("ready", cluster_lifecycle, "task-cluster"),
    ]
    assert session._readiness_clusters == {"task-cluster"}
    session.close.assert_awaited_once()
    session.connect.assert_awaited_once_with(
        cluster_id="task-cluster", session_name="task"
    )


def test_switch_cluster_same_target_remains_a_no_op():
    """The additive pin fix must preserve the existing same-cluster fast path."""
    session = _isolated_session()
    session._cluster_id = "same-cluster"

    with patch.object(
        cluster_lifecycle,
        "ensure_cluster_ready_for_execution",
        new_callable=AsyncMock,
    ) as ready:
        asyncio.run(session.switch_cluster("same-cluster"))

    ready.assert_not_awaited()
    session.close.assert_not_awaited()
    session.connect.assert_not_awaited()


def test_switch_cluster_cache_hit_rechecks_liveness_before_connecting():
    session = _isolated_session()
    session._readiness_clusters.add("task-cluster")
    events = []

    async def ensure_running(cluster_id):
        events.append(("running", cluster_id))

    async def connect(**kwargs):
        events.append(("connect", kwargs["cluster_id"]))

    session.connect.side_effect = connect

    with (
        patch.object(
            phase1_cluster_readiness,
            "prepare_cluster",
            new_callable=AsyncMock,
        ) as ready,
        patch.object(
            cluster_lifecycle,
            "ensure_cluster_running",
            side_effect=ensure_running,
        ) as running,
    ):
        asyncio.run(session.switch_cluster("task-cluster"))

    ready.assert_not_awaited()
    running.assert_awaited_once_with("task-cluster")
    assert events == [
        ("running", "task-cluster"),
        ("connect", "task-cluster"),
    ]
    session.close.assert_awaited_once()
    session.connect.assert_awaited_once_with(cluster_id="task-cluster")


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("fail_at", [0, 1, 2])
@pytest.mark.parametrize("failure", [
    {"status": "error", "outputs": [{"text": "bootstrap rejected"}]},
    {"outputs": []}, None, "malformed reply",
    TimeoutError("bootstrap timed out"), RuntimeError("connection lost"),
])
def test_switch_bootstrap_failure_stops_and_never_registers_failure(
    cached, fail_at, failure, capsys
):
    """PR90 review 5175456750: switch replay must obey checked bootstrap policy."""
    session = _isolated_session()
    snippets = ["first()", "second()", "third()"]
    session._bootstrap_snippets = list(snippets)
    if cached:
        session.mark_cluster_ready("task-cluster")

    async def connect(**kwargs):
        session._bootstrap_snippets = []  # Real connect resets the new kernel's list.

    session.connect.side_effect = connect
    session.register_bootstrap = Mock(wraps=session.register_bootstrap)
    session.execute = AsyncMock(side_effect=[{"status": "ok"}] * fail_at + [failure])
    expected_type = type(failure) if isinstance(failure, Exception) else RuntimeError
    with (
        patch.object(phase1_cluster_readiness, "prepare_cluster", new_callable=AsyncMock),
        patch.object(cluster_lifecycle, "ensure_cluster_running", new_callable=AsyncMock),
        pytest.raises(expected_type) as error,
    ):
        asyncio.run(session.switch_cluster("task-cluster"))
    if isinstance(failure, Exception):
        assert error.value is failure
    else:
        assert f"bootstrap statement {fail_at + 1}/3 failed" in str(error.value)
    assert session.execute.await_count == fail_at + 1
    assert [(call.args[0], call.kwargs["timeout"]) for call in session.execute.await_args_list] == [
        (code, 60) for code in snippets[:fail_at + 1]
    ]
    assert [call.args[0] for call in session.register_bootstrap.call_args_list] == snippets[:fail_at]
    assert session._cluster_id is None
    assert session._bootstrap_snippets == snippets  # retain the original plan, not a partial prefix
    assert session.close.await_count == 2
    assert "Bootstrap replay complete" not in capsys.readouterr().out


@pytest.mark.parametrize("cached", [False, True])
def test_failed_switch_disposes_kernel_and_next_switch_replays_full_plan(cached):
    session = _isolated_session()
    # Exercise real close(), including the underlying executor and pool.
    session.close = ClusterSession.close.__get__(session)
    snippets = ["first()", "second()", "third()"]
    session._bootstrap_snippets = list(snippets)
    if cached:
        session.mark_cluster_ready("task-cluster")
    kernels, pools = [], []

    async def connect(cluster_id, **kwargs):
        session._cluster_id = cluster_id
        session._bootstrap_snippets = []
        session._session = AsyncMock()
        session._pool = AsyncMock()
        kernels.append(session._session)
        pools.append(session._pool)

    session.connect.side_effect = connect
    failure = RuntimeError("second statement rejected")
    session.execute = AsyncMock(side_effect=[{"status": "ok"}, failure])
    with (
        patch.object(phase1_cluster_readiness, "prepare_cluster", new_callable=AsyncMock),
        patch.object(cluster_lifecycle, "ensure_cluster_running", new_callable=AsyncMock),
    ):
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(session.switch_cluster("task-cluster"))
        assert caught.value is failure
        assert session._cluster_id is None
        assert session._session is None
        assert session._pool is None
        kernels[0].close.assert_awaited_once()
        pools[0].stop.assert_awaited_once()
        session.execute = AsyncMock(return_value={"status": "ok"})
        asyncio.run(session.switch_cluster("task-cluster"))
    assert len(kernels) == 2
    assert [call.args[0] for call in session.execute.await_args_list] == snippets
    assert session._bootstrap_snippets == snippets


@pytest.mark.parametrize("cached", [False, True])
def test_switch_bootstrap_success_preserves_order_and_individual_budgets(cached):
    session = _isolated_session()
    snippets = ["first()", "second()", "third()"]
    session._bootstrap_snippets = list(snippets)
    if cached:
        session.mark_cluster_ready("task-cluster")

    async def connect(**kwargs):
        session._bootstrap_snippets = []

    session.connect.side_effect = connect
    session.execute = AsyncMock(return_value={"status": "ok", "outputs": []})
    with (
        patch.object(phase1_cluster_readiness, "prepare_cluster", new_callable=AsyncMock),
        patch.object(cluster_lifecycle, "ensure_cluster_running", new_callable=AsyncMock),
    ):
        asyncio.run(session.switch_cluster("task-cluster"))
    assert [(call.args[0], call.kwargs["timeout"]) for call in session.execute.await_args_list] == [
        (code, 60) for code in snippets
    ]
    assert session._bootstrap_snippets == snippets
