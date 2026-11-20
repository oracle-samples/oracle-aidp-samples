"""Library-path identity: the /jars → /libs rename must not re-install JARs.

Incident (cluster c6364486 / workflow c68de7fd, 2026-09-09): the dependency
repository moved /Workspace/migration-dependencies/jars → …/libs (PR #77
rename). Clusters provisioned before the rename carry the SAME baseline JARs
registered under the OLD path. Exact-path lookups treated them as absent,
re-installed under the new path, the duplicate landed INSTALL_ON_RESTART on
the ACTIVE cluster, and readiness failed loudly for JARs that were already
loaded.

Contract: versioned binary artifacts (.jar/.whl — filenames carry
version(+hash) identity) are satisfied by a same-basename registration —
but ONLY within the tool-owned repository root (/Workspace/migration-dependencies/);
filename equality outside that root proves nothing (PR #88 review P1). requirements/text paths are NEVER basename-matched
(distinct requirements.txt files legitimately share a basename).

Covers all consumers: cluster_lifecycle.ensure_baseline_jars_installed,
verify_baseline_jars_loaded, wait_for_attached_jars_to_settle (the mainline
restart gate), and dbx_export AidpClient._library_satisfied_by (the export
install/wait decision).
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import cluster_lifecycle as cl
from dbx_export import aidp_client


def _run(coro):
    return asyncio.run(coro)


def _no_sleep(monkeypatch):
    async def fake_sleep(_):
        return None
    monkeypatch.setattr(cl.asyncio, "sleep", fake_sleep)


HUDI = cl._BASELINE_JAR_BASENAMES[0]
SCALA = cl._BASELINE_JAR_BASENAMES[1]
LEGACY_DIR = "/Workspace/migration-dependencies/jars"


def _cluster_env(monkeypatch, libs, state="ACTIVE"):
    installs = []
    monkeypatch.setattr(cl, "get_installed_libraries", lambda _cid: libs)
    monkeypatch.setattr(cl, "get_cluster_state", lambda _cid: state)
    monkeypatch.setattr(cl, "install_library", lambda _cid, p: installs.append(p))
    monkeypatch.setattr(
        cl, "_list_workspace_objects",
        lambda parent: [{"path": p} for p in cl.BASELINE_JARS])

    async def fake_poll(_cid, _path, _timeout):
        return "loaded"
    monkeypatch.setattr(cl, "_poll_jar_terminal", fake_poll)
    return installs


# ─── helper semantics ────────────────────────────────────────────────────────

def test_basename_match_only_for_jar_and_whl():
    current = {f"{LEGACY_DIR}/{HUDI}": "INSTALLED",
               "/Workspace/other/requirements.txt": "INSTALLED"}
    path, status = cl._find_artifact_by_basename(current, f"/Workspace/migration-dependencies/libs/{HUDI}")
    assert path == f"{LEGACY_DIR}/{HUDI}" and status == "INSTALLED"
    # requirements.txt must NEVER basename-match a different path
    path, status = cl._find_artifact_by_basename(
        current, "/Workspace/mine/requirements.txt")
    assert path is None and status is None


def test_best_registration_prefers_loaded_sibling_over_staged_exact():
    """Ranking within the licensed pair: exact /libs row staged
    INSTALL_ON_RESTART + legacy /jars row INSTALLED → the loaded sibling wins
    deterministically (PR #88 review minor, restated under the exact-dir
    scope of round 4 — only the jars↔libs pair is licensed)."""
    req = cl.BASELINE_JARS[0]
    current = {req: "INSTALL_ON_RESTART", f"{LEGACY_DIR}/{HUDI}": "INSTALLED"}
    path, status = cl._best_artifact_registration(current, req)
    assert (path, status) == (f"{LEGACY_DIR}/{HUDI}", "INSTALLED")
    # sibling dirs OUTSIDE the licensed pair are refused entirely
    current = {f"/Workspace/migration-dependencies/libs2/{HUDI}": "INSTALLED"}
    assert cl._find_artifact_by_basename(current, req) == (None, None)


def test_basename_match_refuses_outside_repo_root():
    """PR #88 review P1: filename equality does NOT establish content identity
    for arbitrary workload libraries — /team_a/model_udfs.jar must never
    satisfy /team_b/model_udfs.jar, and a customer-dir jar must never satisfy
    a repo-root request (nor vice versa)."""
    current = {"/Workspace/team_a/model_udfs.jar": "INSTALLED"}
    assert cl._find_artifact_by_basename(
        current, "/Workspace/team_b/model_udfs.jar") == (None, None)
    # customer-dir row must not satisfy a repo-root request either
    current = {f"/Workspace/team_a/{HUDI}": "INSTALLED"}
    assert cl._find_artifact_by_basename(current, cl.BASELINE_JARS[0]) == (None, None)
    # and a repo-root row must not satisfy a customer-dir request
    current = {f"{LEGACY_DIR}/{HUDI}": "INSTALLED"}
    assert cl._find_artifact_by_basename(
        current, f"/Workspace/team_b/{HUDI}") == (None, None)


# ─── ensure_baseline_jars_installed ──────────────────────────────────────────

def test_legacy_path_jar_satisfies_no_reinstall(monkeypatch):
    """The incident shape: jars INSTALLED under /jars, BASELINE_JARS under /libs."""
    _no_sleep(monkeypatch)
    libs = [{"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"},
            {"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"}]
    installs = _cluster_env(monkeypatch, libs)
    restart = _run(cl.ensure_baseline_jars_installed("c1"))
    assert installs == []          # no duplicate install PATCH
    assert restart is False        # already loaded — no restart needed


def test_legacy_staged_jar_requires_restart_but_no_reinstall(monkeypatch):
    _no_sleep(monkeypatch)
    libs = [{"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALL_ON_RESTART"},
            {"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"}]
    installs = _cluster_env(monkeypatch, libs, state="ACTIVE")
    restart = _run(cl.ensure_baseline_jars_installed("c1"))
    assert installs == []
    assert restart is True         # staged entry only loads on restart


def test_genuinely_missing_jar_still_installs(monkeypatch):
    _no_sleep(monkeypatch)
    libs = [{"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"}]  # hudi absent
    installs = _cluster_env(monkeypatch, libs)
    _run(cl.ensure_baseline_jars_installed("c1"))
    assert installs == [p for p in cl.BASELINE_JARS if HUDI in p]


def test_dual_registration_loaded_legacy_wins_no_restart(monkeypatch):
    """The incident cluster's exact state: legacy INSTALLED + /libs duplicate
    staged INSTALL_ON_RESTART. The loaded legacy entry satisfies the jar —
    no install, and NO restart demanded for the inert duplicate."""
    _no_sleep(monkeypatch)
    libs = [{"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"},
            {"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"},
            {"path": cl.BASELINE_JARS[0], "status": "INSTALL_ON_RESTART"},
            {"path": cl.BASELINE_JARS[1], "status": "INSTALL_ON_RESTART"}]
    installs = _cluster_env(monkeypatch, libs, state="ACTIVE")
    restart = _run(cl.ensure_baseline_jars_installed("c1"))
    assert installs == []
    assert restart is False


def test_mainline_gate_no_restart_on_incident_state(monkeypatch):
    """PR #88 review MAJOR 1: the incident cluster state must pass the gate
    every migration actually traverses — ensure_cluster_ready_for_execution —
    without demanding a restart. The staged /libs duplicates are inert
    (their artifacts are loaded via the legacy rows)."""
    _no_sleep(monkeypatch)
    libs = [{"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"},
            {"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"},
            {"path": cl.BASELINE_JARS[0], "status": "INSTALL_ON_RESTART"},
            {"path": cl.BASELINE_JARS[1], "status": "INSTALL_ON_RESTART"}]
    installs = _cluster_env(monkeypatch, libs, state="ACTIVE")
    restarts = []

    async def fake_restart(cid, timeout=0):
        restarts.append(cid)
    monkeypatch.setattr(cl, "restart_cluster_and_wait", fake_restart)

    async def fake_running(cid, timeout=0):
        return None
    monkeypatch.setattr(cl, "ensure_cluster_running", fake_running)

    async def fake_req_settle(cid, timeout=600):
        return []
    monkeypatch.setattr(cl, "wait_for_runtime_requirements_to_settle", fake_req_settle)

    _run(cl.ensure_cluster_ready_for_execution("c1"))
    assert restarts == []          # never restarted the ACTIVE cluster
    assert installs == []          # and never re-installed anything


def test_settle_wait_tolerates_inert_failed_duplicate(monkeypatch):
    """Adversarial-verify finding 1 (2026-09-10): exact /libs row FAILED while
    the legacy /jars twin is INSTALLED — the export path correctly refuses to
    re-PATCH, so the settle-wait raising on the FAILED row wedged the cluster
    forever. The failed duplicate is inert: warn, don't raise."""
    _no_sleep(monkeypatch)
    libs = [{"path": cl.BASELINE_JARS[0], "status": "FAILED",
             "stateMessage": "simulated"},
            {"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"}]
    monkeypatch.setattr(cl, "get_installed_libraries", lambda _cid: libs)
    queued = _run(cl.wait_for_attached_jars_to_settle("c1", timeout=0))
    assert queued == []  # no raise, nothing queued


def test_settle_wait_still_raises_on_genuine_failure(monkeypatch):
    _no_sleep(monkeypatch)
    libs = [{"path": cl.BASELINE_JARS[0], "status": "FAILED",
             "stateMessage": "real failure"}]
    monkeypatch.setattr(cl, "get_installed_libraries", lambda _cid: libs)
    with pytest.raises(RuntimeError, match="installation failed"):
        _run(cl.wait_for_attached_jars_to_settle("c1", timeout=0))


def test_settle_wait_keeps_genuinely_staged_jars(monkeypatch):
    """A staged jar with NO loaded same-basename twin must still be returned
    (the inert-duplicate exclusion must not hide real restart needs)."""
    _no_sleep(monkeypatch)
    libs = [{"path": cl.BASELINE_JARS[0], "status": "INSTALL_ON_RESTART"},
            {"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"}]
    monkeypatch.setattr(cl, "get_installed_libraries", lambda _cid: libs)
    queued = _run(cl.wait_for_attached_jars_to_settle("c1", timeout=0))
    assert queued == [cl.BASELINE_JARS[0]]


# ─── verify_baseline_jars_loaded ─────────────────────────────────────────────

def test_verify_passes_on_dual_registration(monkeypatch):
    """Loaded legacy entry + staged duplicate → verify must PASS (jar is on
    the classpath), not wait on the staged row until timeout."""
    _no_sleep(monkeypatch)
    libs = [{"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"},
            {"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"},
            {"path": cl.BASELINE_JARS[0], "status": "INSTALL_ON_RESTART"},
            {"path": cl.BASELINE_JARS[1], "status": "INSTALL_ON_RESTART"}]
    monkeypatch.setattr(cl, "get_installed_libraries", lambda _cid: libs)
    _run(cl.verify_baseline_jars_loaded("c1", timeout=0))  # returns, no raise


def test_verify_accepts_legacy_path_registration(monkeypatch):
    _no_sleep(monkeypatch)
    libs = [{"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"},
            {"path": f"{LEGACY_DIR}/{SCALA}", "status": "INSTALLED"}]
    monkeypatch.setattr(cl, "get_installed_libraries", lambda _cid: libs)
    _run(cl.verify_baseline_jars_loaded("c1", timeout=0))  # returns, no raise


def test_verify_still_fails_when_truly_absent(monkeypatch):
    _no_sleep(monkeypatch)
    monkeypatch.setattr(cl, "get_installed_libraries", lambda _cid: [])
    with pytest.raises(RuntimeError, match="unavailable"):
        _run(cl.verify_baseline_jars_loaded("c1", timeout=0))


# ─── dbx_export AidpClient._filter_installable_libraries ────────────────────

def _client(monkeypatch, installed):
    cfg = aidp_client.AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.example",
        workspace_id="ws",
        export_output="/Workspace/export", prefix="ai_tool")
    client = aidp_client.AidpClient(object(), cfg)
    monkeypatch.setattr(client, "list_cluster_libraries", lambda _k: installed)
    return client


def test_export_dedup_skips_same_jar_under_legacy_path(monkeypatch):
    client = _client(monkeypatch, [
        {"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"}])
    kind, _wait = client._library_satisfied_by(
        "c1", {"type": "WORKSPACE_FILE", "path": f"/Workspace/migration-dependencies/libs/{HUDI}"})
    assert kind == "basename"


def test_export_dedup_skips_same_wheel_within_repo_root(monkeypatch):
    """Wheel aliasing follows the same scope rule: satisfied by a same-name
    registration only WITHIN the repository root; an /old/ dir outside the
    root proves nothing (PR #88 review P1)."""
    inside = "/Workspace/migration-dependencies/jars/aidp_compat-0.5.3-py3-none-any.whl"
    req = {"type": "WORKSPACE_FILE",
           "path": "/Workspace/migration-dependencies/libs/aidp_compat-0.5.3-py3-none-any.whl"}
    client = _client(monkeypatch, [{"path": inside, "status": "INSTALLED"}])
    assert client._library_satisfied_by("c1", req) == ("basename", {"path": inside})
    client = _client(monkeypatch, [
        {"path": "/old/aidp_compat-0.5.3-py3-none-any.whl", "status": "INSTALLED"}])
    assert client._library_satisfied_by("c1", req) == (None, None)


def test_export_dedup_never_basename_matches_requirements(monkeypatch):
    """Distinct requirements.txt files share a basename — must still install."""
    client = _client(monkeypatch, [
        {"path": "/Workspace/other/requirements.txt", "status": "INSTALLED"}])
    assert client._library_satisfied_by(
        "c1", {"type": "WORKSPACE_FILE",
               "path": "/Workspace/mine/requirements.txt"}) == (None, None)


def test_export_dedup_reinstalls_over_failed_legacy_entry(monkeypatch):
    client = _client(monkeypatch, [
        {"path": f"{LEGACY_DIR}/{HUDI}", "status": "FAILED"}])
    items = [{"type": "WORKSPACE_FILE", "path": f"/Workspace/migration-dependencies/libs/{HUDI}"}]
    assert client._library_satisfied_by("c1", items[0]) == (None, None)


def test_export_waits_on_matched_registration_not_skipped_item(monkeypatch):
    """PR #88 review MAJOR 2: a basename-satisfied item must never be waited
    on by its OWN identity (which never appears — 300 polls, 600s stall,
    false 408). The wait target is the matched registration's path."""
    client = _client(monkeypatch, [
        {"path": f"{LEGACY_DIR}/{HUDI}", "status": "INSTALLED"}])
    kind, wait_item = client._library_satisfied_by(
        "c1", {"type": "WORKSPACE_FILE", "path": f"/Workspace/migration-dependencies/libs/{HUDI}"})
    assert kind == "basename"
    assert wait_item == {"path": f"{LEGACY_DIR}/{HUDI}"}

    # end-to-end through install_cluster_libraries: no PATCH, wait on match
    import dbx_export.aidp_client as ac_mod
    waited, patched = [], []
    client.list_clusters = lambda: [{"key": "c1", "displayName": "ai_tool_x"}]
    client._wait_for_library_terminal_state = (
        lambda _k, item, **kw: waited.append(item.get("path")))

    class _NoPatch:
        def __call__(self, *a, **k):
            patched.append(a); raise AssertionError("PATCH must not happen")
    orig_patch = ac_mod.requests.patch
    ac_mod.requests.patch = _NoPatch()
    try:
        client.install_cluster_libraries(
            "c1", [{"type": "WORKSPACE_FILE", "path": f"/Workspace/migration-dependencies/libs/{HUDI}"}])
    finally:
        ac_mod.requests.patch = orig_patch
    assert patched == []
    assert waited == [f"{LEGACY_DIR}/{HUDI}"]


def test_export_best_registration_resolves_stale_duplicate_rows(monkeypatch):
    """AIDP can retain a stale FAILED row beside a newer INSTALLED row for the
    SAME legacy path — the effective status must resolve to INSTALLED and
    satisfy the request, both row orders (multi-row ranking restated under
    the exact-dir scope of round 4)."""
    A = f"{LEGACY_DIR}/{HUDI}"
    REQ = f"/Workspace/migration-dependencies/libs/{HUDI}"
    for rows in ([{"path": A, "status": "FAILED"}, {"path": A, "status": "INSTALLED"}],
                 [{"path": A, "status": "INSTALLED"}, {"path": A, "status": "FAILED"}]):
        client = _client(monkeypatch, rows)
        kind, wait_item = client._library_satisfied_by(
            "c1", {"type": "WORKSPACE_FILE", "path": REQ})
        assert (kind, wait_item) == ("basename", {"path": A})


def test_alias_dirs_follow_custom_configuration(monkeypatch):
    """PR #88 review round 4 (param): a custom lib_repository_dir must (a)
    NOT broaden trust to its parent (shallow /Workspace/libs must not trust
    the whole workspace) and (b) keep BOTH consumers in agreement (custom
    …/company-dependencies/libs aliases its …/jars sibling in each)."""
    # (a) shallow config: /Workspace/libs → parent is /Workspace — the
    # unrelated-jar case must stay refused in the lifecycle consumer.
    monkeypatch.setattr(cl, "AIDP_LIB_DIR", "/Workspace/libs")
    current = {"/Workspace/team_a/model_udfs.jar": "INSTALLED"}
    assert cl._find_artifact_by_basename(
        current, "/Workspace/team_b/model_udfs.jar") == (None, None)
    assert cl._artifact_alias_dirs() == {"/Workspace/libs", "/Workspace/jars"}

    # (b) custom repository pair: both consumers alias jars↔libs siblings.
    monkeypatch.setattr(cl, "AIDP_LIB_DIR", "/Workspace/company-dependencies/libs")
    current = {"/Workspace/company-dependencies/jars/baseline-1.0.jar": "INSTALLED"}
    path, status = cl._find_artifact_by_basename(
        current, "/Workspace/company-dependencies/libs/baseline-1.0.jar")
    assert (path, status) == ("/Workspace/company-dependencies/jars/baseline-1.0.jar",
                              "INSTALLED")

    aidp_client.set_artifact_alias_repository("/Workspace/company-dependencies/libs")
    try:
        client = _client(monkeypatch, [
            {"path": "/Workspace/company-dependencies/jars/baseline-1.0.jar",
             "status": "INSTALLED"}])
        kind, wait = client._library_satisfied_by(
            "c1", {"type": "WORKSPACE_FILE",
                   "path": "/Workspace/company-dependencies/libs/baseline-1.0.jar"})
        assert (kind, wait) == ("basename", {
            "path": "/Workspace/company-dependencies/jars/baseline-1.0.jar"})
    finally:
        aidp_client._artifact_alias_dirs_configured = None


def test_export_refuses_alias_outside_repo_root(monkeypatch):
    """PR #88 review P1 (executed shape from the review): an unrelated
    same-named customer jar must NOT satisfy the request — 1 real PATCH,
    wait on the REQUESTED path."""
    client = _client(monkeypatch, [
        {"path": "/Workspace/team_a/model_udfs.jar", "status": "INSTALLED"}])
    assert client._library_satisfied_by(
        "c1", {"type": "WORKSPACE_FILE",
               "path": "/Workspace/team_b/model_udfs.jar"}) == (None, None)
