from types import SimpleNamespace

import pytest

from scripts import cluster_lifecycle as lifecycle
from dbx_export import aidp_client


def _response(status_code, text=""):
    return SimpleNamespace(
        status_code=status_code,
        text=text,
        json=lambda: {"status": "ok"},
    )


def test_install_library_retries_http_409(monkeypatch):
    responses = iter([_response(409, "busy"), _response(409, "busy"), _response(200)])
    sleeps = []
    calls = []

    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)

    def fake_patch(*args, **kwargs):
        calls.append((args, kwargs))
        return next(responses)

    monkeypatch.setattr(lifecycle.http_requests, "patch", fake_patch)

    result = lifecycle.install_library("cluster-id", "/Workspace/dependency.whl")

    assert result == {"status": "ok"}
    assert len(calls) == 3
    assert sleeps == [2, 4]


def test_install_library_does_not_retry_non_409(monkeypatch):
    calls = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.time, "sleep", lambda seconds: pytest.fail("slept"))

    def fake_patch(*args, **kwargs):
        calls.append((args, kwargs))
        return _response(500, "server error")

    monkeypatch.setattr(lifecycle.http_requests, "patch", fake_patch)

    with pytest.raises(RuntimeError, match="HTTP 500"):
        lifecycle.install_library("cluster-id", "/Workspace/dependency.whl")

    assert len(calls) == 1


def test_install_library_fails_after_409_retries_are_exhausted(monkeypatch):
    sleeps = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        lifecycle.http_requests,
        "patch",
        lambda *args, **kwargs: _response(409, "operation in progress"),
    )

    with pytest.raises(RuntimeError, match="HTTP 409"):
        lifecycle.install_library("cluster-id", "/Workspace/dependency.whl")

    assert sleeps == [2, 4, 8, 16, 30]


def test_export_client_library_attach_retries_http_409(monkeypatch):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace",
        export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    monkeypatch.setattr(
        client,
        "list_clusters",
        lambda: [{"key": "cluster", "displayName": "ai_tool_cluster"}],
    )
    monkeypatch.setattr(client, "list_cluster_libraries", lambda _key: [])
    responses = iter([_response(409, "busy"), _response(202)])
    sleeps = []
    monkeypatch.setattr(aidp_client.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        aidp_client.requests, "patch", lambda *args, **kwargs: next(responses)
    )

    client.install_cluster_libraries("cluster", [{"operation": "INSTALL"}])

    assert sleeps == [2]


def test_export_client_skips_library_already_installed(monkeypatch):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace",
        export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    jar_path = "/Workspace/export/jars/dependency.jar"
    monkeypatch.setattr(
        client,
        "list_clusters",
        lambda: [{"key": "cluster", "displayName": "ai_tool_cluster"}],
    )
    monkeypatch.setattr(
        client,
        "list_cluster_libraries",
        lambda _key: [{
            "path": jar_path,
            "status": "INSTALLED",
        }],
    )
    monkeypatch.setattr(
        aidp_client.requests,
        "patch",
        lambda *args, **kwargs: pytest.fail("already-installed JAR was patched"),
    )

    client.install_cluster_libraries(
        "cluster",
        [{"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": jar_path}],
    )


def test_export_client_healthy_duplicate_wins_over_old_failed_entry(monkeypatch):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace",
        export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    jar_path = "/Workspace/export/jars/dependency.jar"
    monkeypatch.setattr(
        client,
        "list_clusters",
        lambda: [{"key": "cluster", "displayName": "ai_tool_cluster"}],
    )
    monkeypatch.setattr(
        client,
        "list_cluster_libraries",
        lambda _key: [
            {"path": jar_path, "status": "INSTALLED"},
            {"path": jar_path, "status": "FAILED"},
        ],
    )
    monkeypatch.setattr(
        aidp_client.requests,
        "patch",
        lambda *args, **kwargs: pytest.fail("healthy duplicate was reinstalled"),
    )

    client.install_cluster_libraries(
        "cluster",
        [{"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": jar_path}],
    )


@pytest.mark.parametrize("stale_status", ["FAILED", "DELETED"])
def test_export_client_wait_uses_new_pending_row_over_stale_terminal(
    monkeypatch, stale_status,
):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace", export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    jar_path = "/Workspace/export/jars/dependency.jar"
    states = iter([
        [
            {"path": jar_path, "status": stale_status},
            {"path": jar_path, "status": "PENDING"},
        ],
        [
            {"path": jar_path, "status": stale_status},
            {"path": jar_path, "status": "INSTALLED"},
        ],
    ])
    monkeypatch.setattr(
        client, "list_cluster_libraries", lambda _key: next(states)
    )
    sleeps = []
    monkeypatch.setattr(aidp_client.time, "sleep", sleeps.append)

    status = client._wait_for_library_terminal_state(
        "cluster", {"path": jar_path}, poll_seconds=2
    )

    assert status == "INSTALLED"
    assert sleeps == [2]


@pytest.mark.parametrize("stale_status", ["FAILED", "DELETED"])
def test_export_client_wait_allows_late_new_row_after_stale_snapshots(
    monkeypatch, stale_status,
):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace", export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    jar_path = "/Workspace/export/jars/dependency.jar"
    states = iter([
        [{"path": jar_path, "status": stale_status}],
        [{"path": jar_path, "status": stale_status}],
        [
            {"path": jar_path, "status": stale_status},
            {"path": jar_path, "status": "PENDING"},
        ],
        [
            {"path": jar_path, "status": stale_status},
            {"path": jar_path, "status": "INSTALLED"},
        ],
    ])
    monkeypatch.setattr(
        client, "list_cluster_libraries", lambda _key: next(states)
    )
    sleeps = []
    monkeypatch.setattr(aidp_client.time, "sleep", sleeps.append)

    status = client._wait_for_library_terminal_state(
        "cluster", {"path": jar_path}, poll_seconds=2,
    )

    assert status == "INSTALLED"
    assert sleeps == [2, 2, 2]


def test_export_client_wait_still_raises_for_persistent_failed_state(monkeypatch):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace", export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    jar_path = "/Workspace/export/jars/dependency.jar"
    monkeypatch.setattr(
        client,
        "list_cluster_libraries",
        lambda _key: [{"path": jar_path, "status": "FAILED"}],
    )
    sleeps = []
    monkeypatch.setattr(aidp_client.time, "sleep", sleeps.append)

    with pytest.raises(aidp_client.AidpApiError, match="entered FAILED state"):
        client._wait_for_library_terminal_state(
            "cluster", {"path": jar_path}, poll_seconds=2,
        )

    assert sleeps == [2, 2]


def test_export_client_409_refresh_installs_only_remaining_library(monkeypatch):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace",
        export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    first_path = "/Workspace/export/jars/first.jar"
    second_path = "/Workspace/export/jars/second.jar"
    monkeypatch.setattr(
        client,
        "list_clusters",
        lambda: [{"key": "cluster", "displayName": "ai_tool_cluster"}],
    )
    library_states = iter([
        [],
        [{"type": "WORKSPACE_FILE", "path": first_path, "status": "INSTALLED"}],
        [{"type": "WORKSPACE_FILE", "path": first_path, "status": "INSTALLED"}],
        [{"type": "WORKSPACE_FILE", "path": first_path, "status": "INSTALLED"}],
        [{"type": "WORKSPACE_FILE", "path": second_path, "status": "INSTALLED"}],
    ])
    monkeypatch.setattr(
        client, "list_cluster_libraries", lambda _key: next(library_states)
    )
    responses = iter([_response(409, "conflict"), _response(202)])
    request_bodies = []
    monkeypatch.setattr(aidp_client.time, "sleep", lambda _seconds: None)

    def fake_patch(*args, **kwargs):
        request_bodies.append(kwargs["json"])
        return next(responses)

    monkeypatch.setattr(aidp_client.requests, "patch", fake_patch)

    client.install_cluster_libraries(
        "cluster",
        [
            {"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": first_path},
            {"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": second_path},
        ],
    )

    assert [
        [item["path"] for item in body["items"]]
        for body in request_bodies
    ] == [[first_path], [second_path]]


def test_export_client_installs_and_settles_each_library_sequentially(monkeypatch):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace",
        export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    paths = ["/Workspace/export/jars/one.jar", "/Workspace/export/jars/two.jar"]
    monkeypatch.setattr(
        client, "list_clusters",
        lambda: [{"key": "cluster", "displayName": "ai_tool_cluster"}],
    )
    states = iter([
        [],
        [{"path": paths[0], "status": "INSTALLED"}],
        [{"path": paths[0], "status": "INSTALLED"}],
        [{"path": paths[1], "status": "INSTALLED"}],
    ])
    monkeypatch.setattr(
        client, "list_cluster_libraries", lambda _key: next(states)
    )
    bodies = []
    monkeypatch.setattr(
        aidp_client.requests, "patch",
        lambda *args, **kwargs: (
            bodies.append(kwargs["json"]) or _response(202)
        ),
    )

    client.install_cluster_libraries("cluster", [
        {"operation": "INSTALL", "path": path} for path in paths
    ])

    assert [[item["path"] for item in body["items"]] for body in bodies] == [
        [paths[0]], [paths[1]],
    ]


def test_export_client_waits_for_existing_pending_item_before_next_patch(monkeypatch):
    config = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="workspace", export_output="/Workspace/export",
        prefix="ai_tool",
    )
    client = aidp_client.AidpClient(object(), config)
    first = "/Workspace/export/jars/pending.jar"
    second = "/Workspace/export/jars/next.jar"
    monkeypatch.setattr(
        client, "list_clusters",
        lambda: [{"key": "cluster", "displayName": "ai_tool_cluster"}],
    )
    states = iter([
        [{"path": first, "status": "PENDING"}],
        [{"path": first, "status": "PENDING"}],
        [{"path": first, "status": "INSTALLED"}],
        [{"path": first, "status": "INSTALLED"}],
        [{"path": second, "status": "INSTALLED"}],
    ])
    monkeypatch.setattr(
        client, "list_cluster_libraries", lambda _key: next(states)
    )
    sleeps = []
    monkeypatch.setattr(aidp_client.time, "sleep", sleeps.append)
    bodies = []
    monkeypatch.setattr(
        aidp_client.requests, "patch",
        lambda *args, **kwargs: (
            bodies.append(kwargs["json"]) or _response(202)
        ),
    )

    client.install_cluster_libraries("cluster", [
        {"operation": "INSTALL", "path": first},
        {"operation": "INSTALL", "path": second},
    ])

    assert sleeps == [2]
    assert [[item["path"] for item in body["items"]] for body in bodies] == [
        [second]
    ]
