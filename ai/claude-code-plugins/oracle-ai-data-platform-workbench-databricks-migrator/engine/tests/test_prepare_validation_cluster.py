import asyncio
from types import SimpleNamespace

from scripts import prepare_validation_cluster as validation_cluster
from scripts import cluster_states
from scripts.prepare_validation_cluster import (
    DEFAULT_COMPAT_LIBRARIES,
    failed_libraries,
    queued_jars,
    summarize_libraries,
)


def test_mlflow_setuptools_compat_wheel_is_a_default_cluster_library():
    assert DEFAULT_COMPAT_LIBRARIES == (
        "/Workspace/migration-dependencies/wheels/setuptools-80.10.2-py3-none-any.whl",
    )


def test_library_classification():
    items = [
        {"path": "/x/a.jar", "status": "INSTALL_ON_RESTART"},
        {"path": "/x/b.whl", "status": "INSTALLED"},
        {"path": "/x/c.txt", "status": "FAILED"},
    ]
    assert queued_jars(items) == ["/x/a.jar"]
    assert failed_libraries(items) == [items[2]]
    assert summarize_libraries(items)["/x/b.whl"] == "INSTALLED"


def test_validation_cluster_consumes_shared_cluster_state_taxonomy():
    assert validation_cluster.STARTABLE_CLUSTER_STATES is (
        cluster_states.STARTABLE_CLUSTER_STATES
    )
    assert validation_cluster.TERMINAL_CLUSTER_STATES is (
        cluster_states.TERMINAL_CLUSTER_STATES
    )


def test_stop_cluster_retries_conflict_until_accepted(monkeypatch):
    responses = iter([
        SimpleNamespace(status_code=409, text="busy"),
        SimpleNamespace(status_code=409, text="busy"),
        SimpleNamespace(status_code=202, text="accepted"),
    ])
    calls = []

    monkeypatch.setattr(
        validation_cluster.lifecycle, "_cluster_url", lambda _cluster: "https://cluster"
    )
    monkeypatch.setattr(
        validation_cluster.lifecycle, "_get_signer", lambda: object()
    )
    monkeypatch.setattr(
        validation_cluster.requests,
        "post",
        lambda *args, **kwargs: calls.append((args, kwargs)) or next(responses),
    )

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(validation_cluster.asyncio, "sleep", no_sleep)
    asyncio.run(validation_cluster._stop_cluster("cluster-id"))

    assert len(calls) == 3
