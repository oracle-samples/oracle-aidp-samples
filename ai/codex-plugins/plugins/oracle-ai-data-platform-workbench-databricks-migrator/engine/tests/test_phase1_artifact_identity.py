"""PR #90 / #88 integration: identity must not hide another library's state.

Review: PR #90 comment 5617952770 (2026-09-10) requires the merged #88
identity helper, not workspace-wide basename grouping. Exercise the complete
readiness gate and the report handoff with real lifecycle policy; replace only
control-plane I/O and dependency staging. No AIDP calls are made.
"""

import asyncio
import os
from pathlib import Path
import sys

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cluster_lifecycle as lifecycle
from scripts import phase1_cluster_readiness as readiness


REPO = "/Workspace/migration-dependencies/libs"
LEGACY = "/Workspace/migration-dependencies/jars"
DIRECTORIES = [
    (REPO, LEGACY, REPO, True),
    (REPO, REPO, LEGACY, True),
    ("/Workspace/company/libs", "/Workspace/company/jars", "/Workspace/company/libs", True),
    ("/Workspace/libs", "/Workspace/jars", "/Workspace/libs", True),
    (REPO, "/Workspace/team-a", "/Workspace/team-b", False),
    (REPO, "/Workspace/team-a", REPO, False),
    (REPO, REPO, "/Workspace/team-b", False),
    (REPO, LEGACY, REPO + "2", False),
    (REPO, LEGACY, REPO + "/nested", False),
    (REPO, LEGACY + "/nested", REPO, False),
    ("/Workspace/libs", "/Workspace/team-a", "/Workspace/team-b", False),
    ("/Workspace/company/artifacts", "/Workspace/company/jars", "/Workspace/company/artifacts", False),
]
STATES = ["FAILED", "SKIPPED", "INSTALL_ON_RESTART", "PENDING", "RESOLVING", "INSTALLING", "UNKNOWN"]


def control_plane(monkeypatch, rows, repo=REPO):
    """Use real alias/restart/verification code, with no network or real waits."""
    events = []
    monkeypatch.setattr(lifecycle, "AIDP_LIB_DIR", repo)
    monkeypatch.setattr(lifecycle, "BASELINE_JARS", [])
    monkeypatch.setenv("COMPUTE_CLUSTER_KEY", "control")
    monkeypatch.delenv("AIDP_ALLOW_SHARED_CLUSTER_RESTART", raising=False)
    monkeypatch.setattr(lifecycle, "get_cluster_display_name", lambda _: "test-target")
    monkeypatch.setattr(lifecycle, "get_cluster_state", lambda _: "ACTIVE")
    monkeypatch.setattr(lifecycle, "get_installed_libraries", lambda _: [dict(row) for row in rows])

    async def stage_pins(*args, **kwargs):
        events.append("pins")

    async def stage_compat(*args, **kwargs):
        events.append("compat")

    async def restart(*args, **kwargs):
        events.append("restart")
        for row in rows:
            if row["status"] == "INSTALL_ON_RESTART":
                row["status"] = "INSTALLED"

    async def no_sleep(_):
        pass

    def no_network(*args, **kwargs):
        raise AssertionError("unexpected control-plane request")

    monkeypatch.setattr(lifecycle, "ensure_pinned_requirements_installed", stage_pins)
    monkeypatch.setattr(lifecycle, "ensure_aidp_compat_installed", stage_compat)
    monkeypatch.setattr(lifecycle, "restart_cluster_and_wait", restart)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    for method in ("get", "post", "patch"):
        monkeypatch.setattr(lifecycle.http_requests, method, no_network)
    return events


@pytest.mark.parametrize("repo,loaded_dir,other_dir,licensed", DIRECTORIES)
@pytest.mark.parametrize("extension", ["jar", "whl"])
@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("reverse", [False, True])
def test_complete_gate_preserves_alias_scope_and_operation_state(
    monkeypatch, repo, loaded_dir, other_dir, licensed, extension, state, reverse
):
    rows = [
        {"path": f"{loaded_dir}/example-1.0.{extension}", "status": "INSTALLED"},
        {"path": f"{other_dir}/example-1.0.{extension}", "status": state},
    ]
    if reverse:
        rows.reverse()
    events = control_plane(monkeypatch, rows, repo)
    settled = state in {"FAILED", "SKIPPED", "INSTALL_ON_RESTART"}
    if licensed and settled:
        result = asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
        assert result["status"] == "READY"
        assert result["restarted"] is False
        assert events == ["pins", "compat"]
    else:
        with pytest.raises(RuntimeError):
            asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
        assert "restart" not in events
        if not settled:
            # A healthy alias must never conceal an operation that still owns
            # the cluster mutation lock, even inside the trusted repository.
            assert events == []


@pytest.mark.parametrize("repo,loaded_dir,other_dir,licensed", DIRECTORIES)
@pytest.mark.parametrize("extension", ["jar", "whl", "txt"])
def test_report_reuse_uses_the_same_configured_alias_contract(
    monkeypatch, repo, loaded_dir, other_dir, licensed, extension
):
    loaded_path = f"{loaded_dir}/example-1.0.{extension}"
    requested_path = f"{other_dir}/example-1.0.{extension}"
    rows = [{"path": loaded_path, "status": "INSTALLED"}]
    control_plane(monkeypatch, rows, repo)
    report = {"libraries_after": [{"path": requested_path, "status": "INSTALLED"}]}
    ok, reason = readiness.reusable_cluster_report_is_live(lifecycle, "target", report)
    assert ok is (licensed and extension != "txt"), reason


@pytest.mark.parametrize("extension", ["jar", "whl"])
@pytest.mark.parametrize("state", STATES)
def test_report_reuse_never_hides_an_unrelated_unhealthy_library(monkeypatch, extension, state):
    a = f"/Workspace/team-a/example-1.0.{extension}"
    b = f"/Workspace/team-b/example-1.0.{extension}"
    rows = [{"path": a, "status": "INSTALLED"}, {"path": b, "status": state}]
    control_plane(monkeypatch, rows)
    ok, reason = readiness.reusable_cluster_report_is_live(
        lifecycle, "target", {"libraries_after": [{"path": a}]}
    )
    assert not ok
    assert "not settled/healthy" in reason


@pytest.mark.parametrize("state", ["PENDING", "RESOLVING", "INSTALLING", "UNKNOWN"])
@pytest.mark.parametrize("reverse", [False, True])
def test_same_path_inflight_row_cannot_be_hidden_by_loaded_row(monkeypatch, state, reverse):
    rows = [
        {"path": f"{REPO}/example-1.0.whl", "status": "INSTALLED"},
        {"path": f"{REPO}/example-1.0.whl", "status": state},
    ]
    if reverse:
        rows.reverse()
    events = control_plane(monkeypatch, rows)
    with pytest.raises(RuntimeError, match="did not settle"):
        asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
    assert events == []


def test_inflight_trusted_alias_settles_before_first_mutation(monkeypatch):
    rows = [
        {"path": f"{LEGACY}/example-1.0.whl", "status": "INSTALLED"},
        {"path": f"{REPO}/example-1.0.whl", "status": "INSTALLING"},
    ]
    events = control_plane(monkeypatch, rows)

    async def settle(_):
        assert events == []
        events.append("settled")
        rows[1]["status"] = "INSTALL_ON_RESTART"

    monkeypatch.setattr(lifecycle.asyncio, "sleep", settle)
    result = asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=15))
    assert result["status"] == "READY"
    assert events == ["settled", "pins", "compat"]


@pytest.mark.parametrize("opt_in", [False, True])
@pytest.mark.parametrize("is_control", [False, True])
def test_late_external_library_change_never_triggers_an_unplanned_restart(
    monkeypatch, opt_in, is_control
):
    rows = []
    events = control_plane(monkeypatch, rows)
    monkeypatch.setenv("COMPUTE_CLUSTER_KEY", "target" if is_control else "control")
    if opt_in:
        monkeypatch.setenv("AIDP_ALLOW_SHARED_CLUSTER_RESTART", "1")
    real_gate = lifecycle.ensure_cluster_ready_for_execution

    async def gate_then_external_change(*args, **kwargs):
        result = await real_gate(*args, **kwargs)
        rows.append({"path": "/Workspace/late-1.0.whl", "status": "INSTALL_ON_RESTART"})
        return result

    monkeypatch.setattr(lifecycle, "ensure_cluster_ready_for_execution", gate_then_external_change)
    with pytest.raises(RuntimeError, match="changed after activation"):
        asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
    assert "restart" not in events


@pytest.mark.parametrize("source", ["cli", "env", "cwd"])
@pytest.mark.parametrize("repo", [REPO, "/Workspace/company/libs", "/Workspace/libs"])
@pytest.mark.parametrize("trusted", [False, True])
def test_config_discovery_reaches_complete_gate_and_report_reuse(
    monkeypatch, tmp_path, source, repo, trusted
):
    """No fake config loader: CLI must beat env, which must beat cwd YAML."""
    for key in list(os.environ):
        if key.startswith("AIDP_"):
            monkeypatch.delenv(key)
    monkeypatch.chdir(tmp_path)
    fixture = Path(__file__).parent / "config" / "fixtures" / "minimal.yaml"
    config = yaml.safe_load(fixture.read_text())
    mapping = tmp_path / "mapping.json"
    mapping.write_text("s3_bucket,oci_bucket,oci_namespace\n")
    config["bucket_mapping"]["path"] = str(mapping)
    paths = {name: tmp_path / filename for name, filename in (
        ("cli", "explicit.yaml"), ("env", "environment.yaml"),
        ("cwd", "aidp_migrator.yaml"),
    )}
    for name, path in paths.items():
        config["paths"]["lib_repository_dir"] = repo if name == source else "/Workspace/decoy/libs"
        path.write_text(yaml.safe_dump(config))

    alias_dir = repo.rsplit("/", 1)[0] + "/jars" if trusted else "/Workspace/unrelated"
    loaded = f"{alias_dir}/example-1.0.whl"
    requested = f"{repo}/example-1.0.whl"
    rows = [{"path": loaded, "status": "INSTALLED"},
            {"path": requested, "status": "FAILED"}]
    events = control_plane(monkeypatch, rows)
    # configure_lifecycle mutates these globals; restore them after each cell.
    for name in ("AIDP_BASE", "DATALAKE_OCID", "WORKSPACE_ID", "OCI_PROFILE",
                 "_SIGNER", "TARGET_WHEEL_PATH", "AIDP_COMPAT_PREFIX"):
        monkeypatch.setattr(lifecycle, name, getattr(lifecycle, name))
    monkeypatch.setattr(lifecycle, "_BASELINE_JAR_BASENAMES", [])
    if source != "cwd":
        monkeypatch.setenv("AIDP_CONFIG_PATH", str(paths["env"]))
    _, configured = readiness.configure_lifecycle(str(paths["cli"]) if source == "cli" else None)
    assert configured is lifecycle
    assert lifecycle.AIDP_LIB_DIR == repo
    if trusted:
        result = asyncio.run(readiness.prepare_cluster(configured, "target", library_timeout=0))
        assert result["status"] == "READY"
        assert events == ["pins", "compat"]
    else:
        with pytest.raises(RuntimeError, match="installation failed"):
            asyncio.run(readiness.prepare_cluster(configured, "target", library_timeout=0))
    ok, _ = readiness.reusable_cluster_report_is_live(
        configured, "target", {"libraries_after": [{"path": requested}]}
    )
    assert ok is trusted


@pytest.mark.parametrize("repo", [REPO, "/Workspace/company/libs", "/Workspace/libs"])
@pytest.mark.parametrize("loaded_state", ["INSTALLED", "RESOLVED"])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("duplicate_state", ["INSTALL_ON_RESTART", "DELETED"])
def test_pr88_staged_baseline_alias_passes_real_baseline_and_whole_cluster_gates(
    monkeypatch, repo, loaded_state, reverse, duplicate_state
):
    """Review 5175456750: run BOTH policies, not a stubbed/empty baseline gate."""
    baseline = [f"{repo}/{name}" for name in lifecycle._BASELINE_JAR_BASENAMES]
    legacy = repo.rsplit("/", 1)[0] + "/jars"
    rows = [{"path": path, "status": loaded_state} for path in baseline[1:]]
    rows += [
        {"path": f"{legacy}/{lifecycle._BASELINE_JAR_BASENAMES[0]}", "status": loaded_state},
        {"path": baseline[0], "status": duplicate_state},
    ]
    if reverse:
        rows.reverse()
    events = control_plane(monkeypatch, rows, repo)
    monkeypatch.setattr(lifecycle, "BASELINE_JARS", baseline)
    # Baseline install, attached-JAR checks, activation decision, and final
    # baseline verification all use production code. Unexpected I/O fails.
    for _ in range(2):
        result = asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
        assert result["status"] == "READY"
        assert result["restarted"] is False
        assert all(item["status"] == loaded_state for item in result["libraries_after"])
    assert events == ["pins", "compat", "pins", "compat"]
    assert any(row["status"] == duplicate_state for row in rows)


@pytest.mark.parametrize("repo,loaded_dir,deleted_dir,licensed", DIRECTORIES)
@pytest.mark.parametrize("extension", ["jar", "whl", "txt"])
@pytest.mark.parametrize("reverse", [False, True])
def test_deleted_path_is_not_a_live_alias_or_report_requirement(
    monkeypatch, repo, loaded_dir, deleted_dir, licensed, extension, reverse
):
    loaded = f"{loaded_dir}/example-1.0.{extension}"
    deleted = f"{deleted_dir}/example-1.0.{extension}"
    rows = [{"path": loaded, "status": "INSTALLED"},
            {"path": deleted, "status": "DELETED"}]
    if reverse:
        rows.reverse()
    events = control_plane(monkeypatch, rows, repo)
    result = asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
    assert result["status"] == "READY"
    assert result["libraries_after"] == [{"path": loaded, "status": "INSTALLED", "state_message": ""}]
    assert events == ["pins", "compat"]
    # Removing an unrelated library is settled, but its tombstone must not
    # satisfy a report that still requires it. #88 licenses binary aliases only.
    ok, reason = readiness.reusable_cluster_report_is_live(
        lifecycle, "target", {"libraries_after": [{"path": deleted}]}
    )
    assert ok is (licensed and extension != "txt"), reason


@pytest.mark.parametrize("reverse", [False, True])
def test_deleted_same_path_baseline_passes_real_install_and_loaded_checks(monkeypatch, reverse):
    baseline = [f"{REPO}/{name}" for name in lifecycle._BASELINE_JAR_BASENAMES]
    rows = [{"path": path, "status": status} for path in baseline
            for status in ("INSTALLED", "DELETED")]
    if reverse:
        rows.reverse()
    events = control_plane(monkeypatch, rows)
    monkeypatch.setattr(lifecycle, "BASELINE_JARS", baseline)
    result = asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
    assert result["status"] == "READY"
    assert result["restarted"] is False
    assert events == ["pins", "compat"]
    assert {row["path"] for row in result["libraries_after"]} == set(baseline)


@pytest.mark.parametrize("opt_in", [False, True])
def test_control_cluster_real_restart_policy_blocks_even_with_opt_in(monkeypatch, opt_in):
    rows = [{"path": "/Workspace/example/job.whl", "status": "INSTALL_ON_RESTART"}]
    events = control_plane(monkeypatch, rows)
    monkeypatch.setenv("COMPUTE_CLUSTER_KEY", "target")
    if opt_in:
        monkeypatch.setenv("AIDP_ALLOW_SHARED_CLUSTER_RESTART", "1")
    with pytest.raises(RuntimeError, match="cannot restart its own cluster"):
        asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))
    assert events == ["pins", "compat"]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("initial", ["INSTALLED", "PENDING", "DELETED"])
def test_real_pins_and_compat_consumers_ignore_deleted_history(monkeypatch, reverse, initial):
    pins = lifecycle.TARGET_RUNTIME_REQUIREMENTS_PATH
    compat = lifecycle.TARGET_WHEEL_PATH
    rows = [{"path": path, "status": status} for path in (pins, compat)
            for status in ("DELETED", initial)]
    if reverse:
        rows.reverse()
    real_pins = lifecycle.ensure_pinned_requirements_installed
    real_compat = lifecycle.ensure_aidp_compat_installed
    events = control_plane(monkeypatch, rows)
    monkeypatch.setattr(lifecycle, "ensure_pinned_requirements_installed", real_pins)
    monkeypatch.setattr(lifecycle, "ensure_aidp_compat_installed", real_compat)
    monkeypatch.setattr(lifecycle, "resolve_aidp_compat_whl", lambda: compat)
    monkeypatch.setattr(lifecycle, "_ensure_target_runtime_requirements_file", lambda: None)

    async def install(cluster_id, path):
        assert all(row["status"] != "PENDING" for row in rows)
        events.append(("install", path))
        rows.append({"path": path, "status": "PENDING"})

    async def settle(_):
        for row in rows:
            if row["status"] == "PENDING":
                row["status"] = "INSTALLED"

    monkeypatch.setattr(lifecycle, "install_library_async", install)
    monkeypatch.setattr(lifecycle.asyncio, "sleep", settle)
    result = asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=30))
    assert result["status"] == "READY"
    assert all(row["status"] == "INSTALLED" for row in result["libraries_after"])
    assert events == ([("install", pins), ("install", compat)] if initial == "DELETED" else [])


@pytest.mark.parametrize("reverse", [False, True])
def test_deleted_old_requirements_do_not_create_false_multiple_file_conflict(monkeypatch, reverse):
    active = "/Workspace/example/resolved_requirements_current.txt"
    rows = [
        {"path": active, "status": "INSTALLED"},
        {"path": "/Workspace/example/resolved_requirements_old.txt", "status": "DELETED"},
    ]
    if reverse:
        rows.reverse()
    real_pins = lifecycle.ensure_pinned_requirements_installed
    events = control_plane(monkeypatch, rows)
    monkeypatch.setattr(lifecycle, "ensure_pinned_requirements_installed", real_pins)
    assert lifecycle._canonical_requirements_path("target", "/fallback") == active
    assert asyncio.run(readiness.prepare_cluster(lifecycle, "target", library_timeout=0))["status"] == "READY"
    assert events == ["compat"]
