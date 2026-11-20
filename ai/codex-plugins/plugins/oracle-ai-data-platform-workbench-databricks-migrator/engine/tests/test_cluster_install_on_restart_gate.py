"""Regression tests for the Phase 2 INSTALL_ON_RESTART lifecycle gate."""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, patch

import pytest


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

import cluster_lifecycle


@pytest.fixture(autouse=True)
def _no_runtime_requirements_unless_test_overrides(monkeypatch):
    monkeypatch.setattr(
        cluster_lifecycle,
        "wait_for_runtime_requirements_to_settle",
        AsyncMock(return_value=[]),
    )


def _jar_items(status: str) -> list[dict]:
    return [
        {"path": path, "status": status}
        for path in cluster_lifecycle.BASELINE_JARS
    ]


def test_active_cluster_with_queued_jar_requires_restart():
    with (
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="ACTIVE"),
        patch.object(
            cluster_lifecycle,
            "get_installed_libraries",
            return_value=_jar_items("INSTALL_ON_RESTART"),
        ),
    ):
        required = asyncio.run(
            cluster_lifecycle.ensure_baseline_jars_installed("target")
        )

    assert required is True


def test_compat_install_on_restart_returns_without_polling(monkeypatch):
    compat_path = cluster_lifecycle.TARGET_WHEEL_PATH
    monkeypatch.setattr(
        cluster_lifecycle,
        "get_installed_libraries",
        lambda _cluster_id: [
            {"path": compat_path, "status": "INSTALL_ON_RESTART"}
        ],
    )

    restart_required = asyncio.run(
        cluster_lifecycle.ensure_aidp_compat_installed("target", timeout=1)
    )

    assert restart_required is True


def test_attached_jar_gate_returns_job_specific_restart_paths():
    libraries = [
        *_jar_items("INSTALLED"),
        {
            "path": "/Workspace/job-specific.jar",
            "status": "INSTALL_ON_RESTART",
        },
    ]
    with patch.object(
        cluster_lifecycle,
        "get_installed_libraries",
        return_value=libraries,
    ):
        queued = asyncio.run(
            cluster_lifecycle.wait_for_attached_jars_to_settle("target")
        )

    assert queued == ["/Workspace/job-specific.jar"]


def test_attached_jar_gate_raises_when_timeout_is_not_poll_multiple():
    async def no_sleep(_seconds):
        return None

    with (
        patch.object(
            cluster_lifecycle,
            "get_installed_libraries",
            return_value=[{"path": "/Workspace/job.jar", "status": "INSTALLING"}],
        ),
        patch.object(cluster_lifecycle.asyncio, "sleep", no_sleep),
    ):
        with pytest.raises(RuntimeError, match="did not settle within 1s"):
            asyncio.run(
                cluster_lifecycle.wait_for_attached_jars_to_settle(
                    "target", timeout=1
                )
            )


def test_baseline_jar_gate_raises_when_timeout_is_not_poll_multiple():
    async def no_sleep(_seconds):
        return None

    libraries = [
        {"path": path, "status": "INSTALLING"}
        for path in cluster_lifecycle.BASELINE_JARS
    ]
    with (
        patch.object(
            cluster_lifecycle, "get_installed_libraries", return_value=libraries
        ),
        patch.object(cluster_lifecycle.asyncio, "sleep", no_sleep),
    ):
        with pytest.raises(RuntimeError, match="did not become INSTALLED/RESOLVED"):
            asyncio.run(
                cluster_lifecycle.verify_baseline_jars_loaded(
                    "target", timeout=1
                )
            )


def test_ready_gate_restarts_a_separate_target_and_verifies_jars():
    with (
        patch.dict(os.environ, {
            "COMPUTE_CLUSTER_KEY": "control",
            "AIDP_ALLOW_SHARED_CLUSTER_RESTART": "1",
        }),
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="ACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=True),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=["/Workspace/job.jar"]),
        ),
        patch.object(
            cluster_lifecycle,
            "restart_cluster_and_wait",
            new_callable=AsyncMock,
        ) as restart,
        patch.object(
            cluster_lifecycle,
            "ensure_cluster_running",
            new_callable=AsyncMock,
        ) as start,
        patch.object(
            cluster_lifecycle,
            "verify_baseline_jars_loaded",
            new_callable=AsyncMock,
        ) as verify,
    ):
        asyncio.run(
            cluster_lifecycle.ensure_cluster_ready_for_execution("target")
        )

    restart.assert_awaited_once_with("target", timeout=1800)
    start.assert_not_awaited()
    verify.assert_awaited_once_with(
        "target", timeout=600, additional_paths=["/Workspace/job.jar"]
    )


def test_ready_gate_refuses_to_restart_its_own_control_cluster():
    with (
        patch.dict(os.environ, {"COMPUTE_CLUSTER_KEY": "same-cluster"}),
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="ACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=True),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=["/Workspace/job.jar"]),
        ),
        patch.object(
            cluster_lifecycle,
            "restart_cluster_and_wait",
            new_callable=AsyncMock,
        ) as restart,
    ):
        with pytest.raises(RuntimeError, match="cannot restart its own cluster"):
            asyncio.run(
                cluster_lifecycle.ensure_cluster_ready_for_execution(
                    "same-cluster", allow_fresh_cluster_restart=True
                )
            )

    restart.assert_not_awaited()


def test_ready_gate_requires_opt_in_before_restarting_active_shared_cluster():
    clean_env = {
        key: value for key, value in os.environ.items()
        if key != "AIDP_ALLOW_SHARED_CLUSTER_RESTART"
    }
    with (
        patch.dict(os.environ, clean_env, clear=True),
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="ACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=True),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=["/Workspace/job.jar"]),
        ),
        patch.object(
            cluster_lifecycle,
            "restart_cluster_and_wait",
            new_callable=AsyncMock,
        ) as restart,
    ):
        with pytest.raises(RuntimeError, match="Automatic restart is disabled"):
            asyncio.run(
                cluster_lifecycle.ensure_cluster_ready_for_execution("target")
            )

    restart.assert_not_awaited()


def test_ready_gate_restarts_freshly_provisioned_target_without_shared_opt_in():
    clean_env = {
        key: value for key, value in os.environ.items()
        if key != "AIDP_ALLOW_SHARED_CLUSTER_RESTART"
    }
    clean_env["COMPUTE_CLUSTER_KEY"] = "control"
    with (
        patch.dict(os.environ, clean_env, clear=True),
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="ACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=True),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=["/Workspace/job.jar"]),
        ),
        patch.object(
            cluster_lifecycle,
            "restart_cluster_and_wait",
            new_callable=AsyncMock,
        ) as restart,
        patch.object(
            cluster_lifecycle,
            "verify_baseline_jars_loaded",
            new_callable=AsyncMock,
        ) as verify,
    ):
        asyncio.run(
            cluster_lifecycle.ensure_cluster_ready_for_execution(
                "target", allow_fresh_cluster_restart=True
            )
        )

    restart.assert_awaited_once_with("target", timeout=1800)
    verify.assert_awaited_once_with(
        "target", timeout=600, additional_paths=["/Workspace/job.jar"]
    )


def test_ready_gate_starts_without_restart_when_jars_are_not_queued():
    with (
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="INACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=False),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            cluster_lifecycle,
            "ensure_cluster_running",
            new_callable=AsyncMock,
        ) as start,
        patch.object(
            cluster_lifecycle,
            "verify_baseline_jars_loaded",
            new_callable=AsyncMock,
        ) as verify,
    ):
        asyncio.run(
            cluster_lifecycle.ensure_cluster_ready_for_execution("target")
        )

    start.assert_awaited_once_with("target", timeout=1800)
    verify.assert_awaited_once_with(
        "target", timeout=600, additional_paths=[]
    )


def test_job_specific_queued_jar_triggers_restart():
    with (
        patch.dict(os.environ, {
            "COMPUTE_CLUSTER_KEY": "control",
            "AIDP_ALLOW_SHARED_CLUSTER_RESTART": "1",
        }),
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="ACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=False),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=["/Workspace/job-specific.jar"]),
        ),
        patch.object(
            cluster_lifecycle,
            "restart_cluster_and_wait",
            new_callable=AsyncMock,
        ) as restart,
        patch.object(
            cluster_lifecycle,
            "verify_baseline_jars_loaded",
            new_callable=AsyncMock,
        ) as verify,
    ):
        asyncio.run(
            cluster_lifecycle.ensure_cluster_ready_for_execution("target")
        )

    restart.assert_awaited_once_with("target", timeout=1800)
    verify.assert_awaited_once_with(
        "target",
        timeout=600,
        additional_paths=["/Workspace/job-specific.jar"],
    )


def test_queued_runtime_requirements_trigger_restart():
    requirements = cluster_lifecycle.TARGET_RUNTIME_REQUIREMENTS_PATH
    with (
        patch.dict(os.environ, {
            "COMPUTE_CLUSTER_KEY": "control",
            "AIDP_ALLOW_SHARED_CLUSTER_RESTART": "1",
        }),
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="ACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=False),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_runtime_requirements_to_settle",
            AsyncMock(return_value=[requirements]),
        ),
        patch.object(
            cluster_lifecycle,
            "restart_cluster_and_wait",
            new_callable=AsyncMock,
        ) as restart,
        patch.object(
            cluster_lifecycle,
            "verify_baseline_jars_loaded",
            new_callable=AsyncMock,
        ) as verify,
    ):
        asyncio.run(cluster_lifecycle.ensure_cluster_ready_for_execution("target"))

    restart.assert_awaited_once_with("target", timeout=1800)
    verify.assert_awaited_once_with(
        "target", timeout=600, additional_paths=[requirements]
    )


def test_stopped_target_with_queued_jar_starts_without_extra_restart():
    with (
        patch.object(cluster_lifecycle, "get_cluster_state", return_value="INACTIVE"),
        patch.object(
            cluster_lifecycle,
            "ensure_baseline_jars_installed",
            AsyncMock(return_value=False),
        ),
        patch.object(
            cluster_lifecycle,
            "wait_for_attached_jars_to_settle",
            AsyncMock(return_value=["/Workspace/job-specific.jar"]),
        ),
        patch.object(
            cluster_lifecycle,
            "restart_cluster_and_wait",
            new_callable=AsyncMock,
        ) as restart,
        patch.object(
            cluster_lifecycle,
            "ensure_cluster_running",
            new_callable=AsyncMock,
        ) as start,
        patch.object(
            cluster_lifecycle,
            "verify_baseline_jars_loaded",
            new_callable=AsyncMock,
        ) as verify,
    ):
        asyncio.run(
            cluster_lifecycle.ensure_cluster_ready_for_execution("target")
        )

    restart.assert_not_awaited()
    start.assert_awaited_once_with("target", timeout=1800)
    verify.assert_awaited_once_with(
        "target",
        timeout=600,
        additional_paths=["/Workspace/job-specific.jar"],
    )
