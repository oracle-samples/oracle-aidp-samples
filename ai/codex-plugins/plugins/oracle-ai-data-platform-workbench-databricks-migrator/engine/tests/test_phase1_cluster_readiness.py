import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts import cluster_states
from scripts import phase1_cluster_readiness as readiness
from scripts import cluster_lifecycle
from scripts import aidp_executor


def test_collect_manifest_cluster_ids_is_unique_and_ordered():
    manifest = {
        "jobs": [
            {
                "tasks": [
                    {"task_key": "a", "cluster_id": "cluster-a"},
                    {"task_key": "b", "cluster": {"clusterKey": "cluster-b"}},
                    {"task_key": "c", "cluster_id": "cluster-a"},
                ],
                "default_cluster": "cluster-default",
            }
        ]
    }
    assert readiness.collect_manifest_cluster_ids(manifest, "control") == [
        "cluster-a",
        "cluster-b",
        "cluster-default",
    ]


def test_phase1_consumes_shared_state_taxonomy():
    assert readiness.TRANSIENT_CLUSTER_STATES is (
        cluster_states.TRANSIENT_CLUSTER_STATES
    )
    assert readiness.FAILED_CLUSTER_STATES is (
        cluster_states.TERMINAL_CLUSTER_STATES
    )


def test_collect_manifest_cluster_ids_uses_fallback_only_when_needed():
    assert readiness.collect_manifest_cluster_ids({"jobs": []}, "control") == [
        "control"
    ]


class FakeLifecycle:
    _best_artifact_registration = staticmethod(cluster_lifecycle._best_artifact_registration)

    def __init__(self, libraries, states=None):
        self.libraries = libraries
        self.calls = []
        self.first_mutation_library_states = []
        self.states = list(states or ["ACTIVE"])

    def get_cluster_display_name(self, cluster_id):
        return "target"

    def get_cluster_state(self, cluster_id):
        if len(self.states) > 1:
            return self.states.pop(0)
        return self.states[0]

    def get_installed_libraries(self, cluster_id):
        return list(self.libraries)

    async def ensure_pinned_requirements_installed(self, cluster_id, timeout):
        self.first_mutation_library_states.append(
            [item["status"] for item in self.libraries]
        )
        self.calls.append("pins")

    async def ensure_cluster_ready_for_execution(
        self,
        cluster_id,
        cluster_timeout,
        library_timeout,
        allow_fresh_cluster_restart=False,
        additional_restart_paths=None,
    ):
        self.calls.append((
            "baseline",
            allow_fresh_cluster_restart,
            list(additional_restart_paths or []),
        ))
        return False

    async def ensure_aidp_compat_installed(self, cluster_id, timeout):
        self.calls.append("compat")

    async def restart_cluster_and_wait(self, cluster_id, timeout):
        self.calls.append("restart")
        for item in self.libraries:
            if item["status"] == "INSTALL_ON_RESTART":
                item["status"] = "INSTALLED"


def test_prepare_cluster_job_wheel_restart_uses_real_opt_in_gate(monkeypatch):
    libraries = [
        {"path": "/Workspace/job.jar", "status": "INSTALLED"},
        {"path": "/Workspace/job.whl", "status": "INSTALL_ON_RESTART"},
    ]
    calls = []

    async def record(name, result=False):
        calls.append(name)
        return result

    async def restart(_cluster_id, timeout):
        calls.append(("restart", timeout))
        libraries[1]["status"] = "INSTALLED"

    monkeypatch.setenv("COMPUTE_CLUSTER_KEY", "control-id")
    monkeypatch.setenv("AIDP_ALLOW_SHARED_CLUSTER_RESTART", "1")
    monkeypatch.setattr(
        cluster_lifecycle, "get_cluster_display_name", lambda _cluster_id: "target"
    )
    monkeypatch.setattr(
        cluster_lifecycle, "get_cluster_state", lambda _cluster_id: "ACTIVE"
    )
    monkeypatch.setattr(
        cluster_lifecycle,
        "get_installed_libraries",
        lambda _cluster_id: list(libraries),
    )
    monkeypatch.setattr(
        cluster_lifecycle,
        "ensure_pinned_requirements_installed",
        lambda *_args, **_kwargs: record("pins"),
    )
    monkeypatch.setattr(
        cluster_lifecycle,
        "ensure_aidp_compat_installed",
        lambda *_args, **_kwargs: record("compat"),
    )
    monkeypatch.setattr(
        cluster_lifecycle,
        "ensure_baseline_jars_installed",
        lambda *_args, **_kwargs: record("jars"),
    )
    monkeypatch.setattr(
        cluster_lifecycle,
        "wait_for_attached_jars_to_settle",
        lambda *_args, **_kwargs: record("jar-wait", []),
    )
    monkeypatch.setattr(
        cluster_lifecycle,
        "wait_for_runtime_requirements_to_settle",
        lambda *_args, **_kwargs: record("requirements-wait", []),
    )
    monkeypatch.setattr(cluster_lifecycle, "restart_cluster_and_wait", restart)
    monkeypatch.setattr(
        cluster_lifecycle,
        "verify_baseline_jars_loaded",
        lambda *_args, **_kwargs: record("verify"),
    )

    result = asyncio.run(
        readiness.prepare_cluster(
            cluster_lifecycle,
            "target-id",
            cluster_timeout=10,
            library_timeout=10,
        )
    )

    assert result["status"] == "READY"
    assert result["restarted"] is True
    assert calls == [
        "pins",
        "compat",
        "jars",
        "jar-wait",
        "requirements-wait",
        ("restart", 10),
        "verify",
    ]
    assert {item["status"] for item in result["libraries_after"]} == {"INSTALLED"}


def test_prepare_cluster_refuses_to_restart_control_cluster(monkeypatch):
    lifecycle = FakeLifecycle(
        [{"path": "/Workspace/job.whl", "status": "INSTALL_ON_RESTART"}]
    )
    monkeypatch.setenv("COMPUTE_CLUSTER_KEY", "target-id")
    monkeypatch.setattr(cluster_lifecycle, "BASELINE_JARS", [])
    for name in ("get_cluster_display_name", "get_cluster_state", "get_installed_libraries",
                 "ensure_pinned_requirements_installed", "ensure_aidp_compat_installed",
                 "restart_cluster_and_wait"):
        monkeypatch.setattr(cluster_lifecycle, name, getattr(lifecycle, name))
    # Keep the real activation policy: a fake no-op activation only tested the
    # later changed-after-activation guard, not control-cluster protection.
    with pytest.raises(RuntimeError, match="cannot restart its own cluster"):
        asyncio.run(
            readiness.prepare_cluster(
                cluster_lifecycle, "target-id", cluster_timeout=10, library_timeout=10
            )
        )
    assert "restart" not in lifecycle.calls


def test_failed_library_blocks_phase1():
    lifecycle = FakeLifecycle(
        [
            {
                "path": "/Workspace/broken.jar",
                "status": "FAILED",
                "stateMessage": "bad artifact",
            }
        ]
    )
    with pytest.raises(RuntimeError, match="installation failed"):
        asyncio.run(
            readiness.wait_for_attached_libraries_to_settle(
                lifecycle, "target-id", timeout=0, poll_interval=0
            )
        )


def test_pre_mutation_wait_allows_terminal_failure_for_repair():
    lifecycle = FakeLifecycle(
        [{"path": "/Workspace/repairable.whl", "status": "FAILED"}]
    )

    queued = asyncio.run(
        readiness.wait_for_attached_libraries_to_settle(
            lifecycle,
            "target-id",
            timeout=0,
            poll_interval=0,
            fail_on_failed=False,
        )
    )

    assert queued == []


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("status", ["INSTALLED", "RESOLVED"])
def test_deleted_history_does_not_poison_full_readiness_or_report_reuse(status, reverse):
    path = "/Workspace/example/libs/example.whl"
    rows = [{"path": path, "status": status}, {"path": path, "status": "DELETED"}]
    lifecycle = FakeLifecycle(rows[::-1] if reverse else rows)

    result = asyncio.run(readiness.prepare_cluster(lifecycle, "target-id", library_timeout=0))

    assert result["status"] == "READY"
    assert result["restarted"] is False
    assert lifecycle.calls == ["pins", "compat", ("baseline", False, [])]
    assert result["libraries_after"] == [{"path": path, "status": status, "state_message": ""}]
    assert readiness.reusable_cluster_report_is_live(lifecycle, "target-id", result)[0]
    # Repeat the complete orchestration: old tombstones remain harmless.
    assert asyncio.run(readiness.prepare_cluster(lifecycle, "target-id", library_timeout=0))["status"] == "READY"


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("status", ["PENDING", "RESOLVING", "INSTALLING", "DELETING", "UNKNOWN"])
def test_deleted_history_never_hides_inflight_operations(status, reverse):
    path = "/Workspace/example/libs/example.whl"
    rows = [
        {"path": path, "status": "INSTALLED"},
        {"path": path, "status": "DELETED"},
        {"path": path, "status": status},
    ]
    lifecycle = FakeLifecycle(rows[::-1] if reverse else rows)
    with pytest.raises(RuntimeError, match="did not settle"):
        asyncio.run(readiness.prepare_cluster(lifecycle, "target-id", library_timeout=0))
    assert lifecycle.calls == []
    assert not readiness.reusable_cluster_report_is_live(lifecycle, "target-id", {})[0]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("status", ["FAILED", "SKIPPED"])
def test_deleted_history_does_not_hide_unrepaired_failure(status, reverse):
    path = "/Workspace/example/libs/example.whl"
    rows = [{"path": path, "status": status}, {"path": path, "status": "DELETED"}]
    lifecycle = FakeLifecycle(rows[::-1] if reverse else rows)
    with pytest.raises(RuntimeError, match="installation failed"):
        asyncio.run(readiness.prepare_cluster(lifecycle, "target-id", library_timeout=0))
    # Completed failures permit repair, but cannot pass the final gate unchanged.
    assert lifecycle.calls == ["pins", "compat", ("baseline", False, [])]


def test_deleted_only_is_absent_not_loaded_and_required_library_is_repaired():
    path = "/Workspace/example/libs/example.whl"
    lifecycle = FakeLifecycle([{"path": path, "status": "DELETED"}])
    report = {"libraries_after": [{"path": path, "status": "INSTALLED"}]}
    live, reason = readiness.reusable_cluster_report_is_live(lifecycle, "target-id", report)
    assert not live
    assert "missing" in reason
    assert readiness.summarize_libraries(lifecycle.libraries, lifecycle) == []

    async def repair(cluster_id, timeout):
        lifecycle.calls.append("pins")
        lifecycle.libraries.append({"path": path, "status": "INSTALLED"})

    lifecycle.ensure_pinned_requirements_installed = repair
    result = asyncio.run(readiness.prepare_cluster(lifecycle, "target-id", library_timeout=0))
    assert result["status"] == "READY"
    assert result["libraries_before"] == []
    assert result["libraries_after"][0]["status"] == "INSTALLED"


def test_loaded_binary_registration_satisfies_staged_duplicate():
    legacy_path = "/Workspace/migration-dependencies/jars/example-1.0.jar"
    current_path = "/Workspace/migration-dependencies/libs/example-1.0.jar"
    lifecycle = FakeLifecycle([
        {"path": legacy_path, "status": "INSTALLED"},
        {"path": current_path, "status": "INSTALL_ON_RESTART"},
    ])

    queued = asyncio.run(
        readiness.wait_for_attached_libraries_to_settle(
            lifecycle, "target-id", timeout=0, poll_interval=0
        )
    )
    loaded = asyncio.run(
        readiness.verify_all_attached_libraries_loaded(
            lifecycle, "target-id", timeout=0, poll_interval=0
        )
    )

    assert queued == []
    assert loaded == [{
        "path": legacy_path,
        "status": "INSTALLED",
        "state_message": "",
        "equivalent_paths": [legacy_path, current_path],
    }]


def test_text_library_registrations_remain_path_sensitive():
    lifecycle = FakeLifecycle([
        {
            "path": "/Workspace/job-a/requirements.txt",
            "status": "INSTALLED",
        },
        {
            "path": "/Workspace/job-b/requirements.txt",
            "status": "FAILED",
            "stateMessage": "dependency resolution failed",
        },
    ])

    with pytest.raises(RuntimeError, match="installation failed"):
        asyncio.run(
            readiness.wait_for_attached_libraries_to_settle(
                lifecycle, "target-id", timeout=0, poll_interval=0
            )
        )


def test_prepare_cluster_waits_until_creating_cluster_is_stable(monkeypatch):
    lifecycle = FakeLifecycle([], states=["CREATING", "CREATING", "ACTIVE"])

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(readiness.asyncio, "sleep", no_sleep)
    result = asyncio.run(
        readiness.prepare_cluster(
            lifecycle, "target-id", cluster_timeout=10, library_timeout=10
        )
    )

    assert result["status"] == "READY"
    assert result["state_before"] == "CREATING"
    assert result["state_after"] == "ACTIVE"
    assert lifecycle.calls == ["pins", "compat", ("baseline", True, [])]


def test_prepare_cluster_attempts_one_failed_state_recovery(monkeypatch):
    lifecycle = FakeLifecycle([], states=["FAILED"])

    async def recover(cluster_id, timeout, allow_failed_recovery=False):
        lifecycle.calls.append((
            "failed-recovery",
            cluster_id,
            timeout,
            allow_failed_recovery,
        ))
        lifecycle.states = ["ACTIVE"]

    monkeypatch.setattr(lifecycle, "ensure_cluster_running", recover, raising=False)

    result = asyncio.run(
        readiness.prepare_cluster(
            lifecycle,
            "target-id",
            cluster_timeout=45,
            library_timeout=10,
        )
    )

    assert result["status"] == "READY"
    assert result["state_before"] == "FAILED"
    assert result["state_after"] == "ACTIVE"
    assert lifecycle.calls == [
        ("failed-recovery", "target-id", 45, True),
        "pins",
        "compat",
        ("baseline", False, []),
    ]


def test_prepare_cluster_failed_recovery_stops_before_library_mutation(monkeypatch):
    lifecycle = FakeLifecycle([], states=["FAILED"])

    async def fail_recovery(cluster_id, timeout, allow_failed_recovery=False):
        lifecycle.calls.append((
            "failed-recovery",
            cluster_id,
            timeout,
            allow_failed_recovery,
        ))
        raise RuntimeError("recovery start failed")

    monkeypatch.setattr(
        lifecycle,
        "ensure_cluster_running",
        fail_recovery,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="recovery start failed"):
        asyncio.run(
            readiness.prepare_cluster(
                lifecycle,
                "target-id",
                cluster_timeout=45,
                library_timeout=10,
            )
        )

    assert lifecycle.calls == [
        ("failed-recovery", "target-id", 45, True),
    ]
    assert lifecycle.first_mutation_library_states == []


def test_prepare_cluster_waits_for_existing_library_operation(monkeypatch):
    lifecycle = FakeLifecycle(
        [{"path": "/Workspace/other.whl", "status": "INSTALLING"}]
    )
    snapshots = iter([
        [{"path": "/Workspace/other.whl", "status": "INSTALLING"}],
        [{"path": "/Workspace/other.whl", "status": "INSTALLING"}],
        [{"path": "/Workspace/other.whl", "status": "INSTALLED"}],
    ])
    sleeps = []

    def library_snapshot(_cluster_id):
        try:
            lifecycle.libraries = next(snapshots)
        except StopIteration:
            pass
        return list(lifecycle.libraries)

    async def no_sleep(_seconds):
        sleeps.append(_seconds)

    monkeypatch.setattr(lifecycle, "get_installed_libraries", library_snapshot)
    monkeypatch.setattr(readiness.asyncio, "sleep", no_sleep)

    result = asyncio.run(
        readiness.prepare_cluster(
            lifecycle, "target-id", cluster_timeout=10, library_timeout=30
        )
    )

    assert result["status"] == "READY"
    assert sleeps == [15]
    assert lifecycle.first_mutation_library_states == [["INSTALLED"]]
    assert lifecycle.calls == ["pins", "compat", ("baseline", False, [])]


def test_cluster_library_wait_fails_on_terminal_cluster_state():
    lifecycle = FakeLifecycle([], states=["FAILED"])
    with pytest.raises(RuntimeError, match="entered state FAILED"):
        asyncio.run(
            readiness.wait_for_cluster_library_mutation_state(
                lifecycle, "target-id", timeout=10, poll_interval=0
            )
        )


@pytest.mark.parametrize(
    "state",
    [
        "RESTARTING",
        "PROVISIONING",
        "INSTALLING_LIBRARIES",
        "RESTARTING_LIBRARIES",
    ],
)
def test_cluster_library_wait_accepts_shared_transient_states(
        monkeypatch, capsys, state):
    lifecycle = FakeLifecycle([], states=[state, "ACTIVE"])

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(readiness.asyncio, "sleep", no_sleep)
    assert asyncio.run(
        readiness.wait_for_cluster_library_mutation_state(
            lifecycle, "target-id", timeout=10, poll_interval=1
        )
    ) == "ACTIVE"
    assert "(0s / 10s)" in capsys.readouterr().out


def test_distinct_clusters_run_with_bounded_parallelism(monkeypatch):
    active = 0
    peak = 0

    async def fake_prepare(_lifecycle, cluster_id, **_kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01 if cluster_id != "cluster-c" else 0)
        active -= 1
        return {"cluster_id": cluster_id, "status": "READY"}

    monkeypatch.setattr(readiness, "prepare_cluster", fake_prepare)
    results, failures = asyncio.run(
        readiness.prepare_clusters_bounded(
            object(),
            ["cluster-a", "cluster-b", "cluster-c"],
            max_parallel=2,
            cluster_timeout=10,
            library_timeout=10,
        )
    )

    assert peak == 2
    assert failures == []
    assert [item["cluster_id"] for item in results] == [
        "cluster-a",
        "cluster-b",
        "cluster-c",
    ]


def test_readiness_report_requires_matching_key_and_fresh_timestamp(tmp_path):
    report_path = tmp_path / "readiness.json"
    generated_at = datetime.now(timezone.utc)
    report_path.write_text(json.dumps({
        "status": "READY",
        "readiness_key": "expected",
        "generated_at": generated_at.isoformat(),
        "clusters": [{"cluster_id": "cluster-a", "status": "READY"}],
    }))

    reusable = readiness.load_reusable_readiness_report(
        str(report_path),
        "expected",
        max_age_seconds=60,
        now=generated_at + timedelta(seconds=30),
    )
    assert set(reusable) == {"cluster-a"}

    with pytest.raises(RuntimeError, match="contract changed"):
        readiness.load_reusable_readiness_report(
            str(report_path), "different", now=generated_at
        )
    with pytest.raises(RuntimeError, match="outside the 10s reuse window"):
        readiness.load_reusable_readiness_report(
            str(report_path),
            "expected",
            max_age_seconds=10,
            now=generated_at + timedelta(seconds=11),
        )


def test_reusable_report_is_checked_against_live_cluster_state():
    lifecycle = FakeLifecycle(
        [{"path": "/Workspace/dependency.jar", "status": "INSTALLED"}]
    )
    report = {
        "libraries_after": [
            {"path": "/Workspace/dependency.jar", "status": "INSTALLED"}
        ]
    }
    assert readiness.reusable_cluster_report_is_live(
        lifecycle, "cluster-a", report
    )[0] is True

    lifecycle.libraries[0]["status"] = "INSTALLING"
    reusable, reason = readiness.reusable_cluster_report_is_live(
        lifecycle, "cluster-a", report
    )
    assert reusable is False
    assert "not settled/healthy" in reason


def test_max_parallel_clusters_uses_yaml_unless_cli_overrides():
    runtime = SimpleNamespace(phase1_max_parallel_clusters=4)
    assert readiness.resolve_max_parallel_clusters(None, runtime) == 4
    assert readiness.resolve_max_parallel_clusters(2, runtime) == 2
    with pytest.raises(ValueError, match="between 1 and 32"):
        readiness.resolve_max_parallel_clusters(0, runtime)
    with pytest.raises(ValueError, match="between 1 and 32"):
        readiness.resolve_max_parallel_clusters(33, runtime)


def test_ready_report_timestamp_is_recorded_after_cluster_work(
    monkeypatch, tmp_path
):
    started = datetime(2026, 9, 10, 0, 0, tzinfo=timezone.utc)
    completed = started + timedelta(minutes=45)

    class Clock(datetime):
        current = started

        @classmethod
        def now(cls, tz=None):
            return cls.current

    async def prepare_all(*_args, **_kwargs):
        Clock.current = completed
        return ([{"cluster_id": "cluster-a", "status": "READY"}], [])

    lifecycle = SimpleNamespace(
        AIDP_BASE="https://example.invalid",
        DATALAKE_OCID="lake",
        WORKSPACE_ID="workspace",
        TARGET_RUNTIME_REQUIREMENTS_PATH="/Workspace/requirements.txt",
        TARGET_WHEEL_PATH="/Workspace/compat.whl",
        BASELINE_JARS=[],
    )
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(phase1_max_parallel_clusters=3)
    )
    manifest = tmp_path / "manifest.json"
    output = tmp_path / "readiness.json"
    manifest.write_text(json.dumps({
        "jobs": [{"tasks": [{"cluster_id": "cluster-a"}]}]
    }))
    args = SimpleNamespace(
        manifest=str(manifest),
        fallback_cluster="",
        config="config.yaml",
        max_parallel_clusters=None,
        cluster_timeout=10,
        library_timeout=10,
        output=str(output),
    )

    monkeypatch.setattr(readiness, "datetime", Clock)
    monkeypatch.setattr(
        readiness, "configure_lifecycle", lambda _config: (cfg, lifecycle)
    )
    monkeypatch.setattr(readiness, "prepare_clusters_bounded", prepare_all)

    report = asyncio.run(readiness.run(args))

    assert report["started_at"] == started.isoformat()
    assert report["generated_at"] == completed.isoformat()
