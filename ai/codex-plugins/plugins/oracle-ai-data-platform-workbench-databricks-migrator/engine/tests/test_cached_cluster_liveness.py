"""PR #90 cache regression: exercise real lifecycle decisions at all consumers.

Only control-plane I/O and connection boundaries are replaced. These are local
simulations, not evidence that the corresponding branches ran on live AIDP.
"""

import asyncio
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cluster_lifecycle as lifecycle
from cluster_session import ClusterSession
import job_migrate
import job_migrate_from_workflow as runner
import phase1_cluster_readiness as readiness


class ConnectionReached(Exception):
    """Stop the real first-task path before executing any notebook code."""


def cached_target(monkeypatch, initial, fail_start=False):
    state = {"value": initial}
    events = []

    def get_state(cluster_id):
        events.append(("state", cluster_id, state["value"]))
        return state["value"]

    def start(cluster_id, deadline=None):
        assert deadline is not None
        assert state["value"] in {"STOPPED", "INACTIVE"}
        events.append(("start", cluster_id))
        if fail_start:
            raise RuntimeError("simulated start rejection")
        state["value"] = "STARTING"

    async def sleep(_):
        state["value"] = (
            "STOPPED" if state["value"] in {"STOPPING", "TERMINATING"}
            else "ACTIVE"
        )

    def unexpected(*args, **kwargs):
        raise AssertionError("cached target must not reinstall libraries or use network")

    monkeypatch.setattr(lifecycle, "get_cluster_state", get_state)
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _: "synthetic-target")
    monkeypatch.setattr(lifecycle, "start_cluster", start)
    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(readiness, "prepare_cluster", unexpected)
    for method in ("get", "post", "patch"):
        monkeypatch.setattr(lifecycle.http_requests, method, unexpected)
    return state, events


async def invoke_cached_consumer(monkeypatch, consumer, state, events):
    if consumer == "workflow_preflight":
        prepared = {"cluster-a"}
        result = await runner.prepare_migration_cluster_once(lifecycle, "cluster-a", prepared)
        assert result is False
        assert prepared == {"cluster-a"}
        assert state["value"] == "ACTIVE"
        events.append(("consumer-ready", "cluster-a"))
        return

    session = object.__new__(ClusterSession)
    session._initialized = False
    ClusterSession.__init__(session)
    session.mark_cluster_ready("cluster-a")
    session._cluster_id = "cluster-b" if consumer == "switch_cluster" else None

    async def connect(cluster_id, **kwargs):
        assert cluster_id == "cluster-a"
        assert state["value"] == "ACTIVE", "connected before cached cluster was running"
        events.append(("consumer-ready", cluster_id))
        raise ConnectionReached

    session.connect = connect
    if consumer == "switch_cluster":
        with pytest.raises(ConnectionReached):
            await session.switch_cluster("cluster-a")
        return

    # Isolate the real job preamble's in-memory bookkeeping; stop at connect.
    monkeypatch.setattr(job_migrate, "MIGRATE_OUTPUT", "/Workspace/test_output")
    for name in ("MIGRATED_BASE", "JOB_ROOT", "_db_to_aidp_job_map", "_unmapped_db_job_ids"):
        monkeypatch.setattr(job_migrate, name, getattr(job_migrate, name))
    monkeypatch.setattr(job_migrate, "START_TASK", None)
    monkeypatch.setattr(job_migrate, "ONLY_TASKS", [])
    monkeypatch.setattr(job_migrate, "_cell_history", {})
    monkeypatch.setattr(job_migrate, "_inlined_nb_run_children", set())
    for name in ("clear_write_redirect_map", "clear_installed_packages",
                 "set_job_manifest", "set_current_task_key"):
        monkeypatch.setattr(job_migrate, name, lambda *args: None)
    job = {
        "job_name": "cached-cluster-regression",
        "tasks": [{"task_key": "first", "cluster_id": "cluster-a"}],
        "execution_layers": [["first"]],
    }
    with pytest.raises(ConnectionReached):
        await job_migrate._process_job_inner(job, session)


@pytest.mark.parametrize("consumer", ["workflow_preflight", "switch_cluster", "first_task"])
@pytest.mark.parametrize("initial", [
    "ACTIVE", "STOPPED", "INACTIVE", "STOPPING", "TERMINATING",
    "CREATING", "STARTING", "UPDATING", "FAILED", "DELETED", "DELETING", "UNKNOWN",
])
def test_cached_consumers_require_live_active_cluster(monkeypatch, consumer, initial):
    state, events = cached_target(monkeypatch, initial)
    if initial in {"FAILED", "DELETED", "DELETING", "UNKNOWN"}:
        with pytest.raises(RuntimeError, match="unexpected state"):
            asyncio.run(invoke_cached_consumer(monkeypatch, consumer, state, events))
        assert not any(event[0] in {"start", "consumer-ready"} for event in events)
        return
    asyncio.run(invoke_cached_consumer(monkeypatch, consumer, state, events))
    expected_starts = int(initial in {"STOPPED", "INACTIVE", "STOPPING", "TERMINATING"})
    assert sum(event[0] == "start" for event in events) == expected_starts
    assert events[-1] == ("consumer-ready", "cluster-a")


@pytest.mark.parametrize("consumer", ["workflow_preflight", "switch_cluster", "first_task"])
def test_cached_start_failure_never_reaches_connection(monkeypatch, consumer):
    state, events = cached_target(monkeypatch, "STOPPED", fail_start=True)
    with pytest.raises(RuntimeError, match="simulated start rejection"):
        asyncio.run(invoke_cached_consumer(monkeypatch, consumer, state, events))
    assert not any(event[0] == "consumer-ready" for event in events)


def test_switch_a_to_b_to_idle_stopped_a(monkeypatch):
    state, events = cached_target(monkeypatch, "ACTIVE")
    session = object.__new__(ClusterSession)
    session._initialized = False
    ClusterSession.__init__(session)
    session._cluster_id = "cluster-a"
    session.mark_cluster_ready("cluster-a")

    async def prepare(_lifecycle, cluster_id):
        assert cluster_id == "cluster-b", "A must reuse library readiness"
        events.append(("prepare", cluster_id))

    async def connect(cluster_id, **kwargs):
        assert state["value"] == "ACTIVE"
        events.append(("connect", cluster_id))
        session._cluster_id = cluster_id

    monkeypatch.setattr(readiness, "prepare_cluster", prepare)
    session.connect = connect

    async def scenario():
        await session.switch_cluster("cluster-b")
        state["value"] = "STOPPED"  # A idle-terminates while B is executing.
        await session.switch_cluster("cluster-a")

    asyncio.run(scenario())
    assert [event for event in events if event[0] != "state"] == [
        ("prepare", "cluster-b"), ("connect", "cluster-b"),
        ("start", "cluster-a"), ("connect", "cluster-a"),
    ]
    assert session._readiness_clusters == {"cluster-a", "cluster-b"}
