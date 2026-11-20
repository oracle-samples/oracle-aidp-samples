"""Migration-time cluster idle-timeout bump/restore.

Covers the two layers of the feature:
  A. cluster_lifecycle.get_cluster_idle_timeout / set_cluster_idle_timeout —
     the PUT-based runtime helpers (best-effort, verify-by-GET + retry, never
     fatal).
  B. job_migrate._bump/_revert/_revert_all_idle_timeouts — the per-cluster
     policy (bump only when current < target, restore recorded original,
     best-effort throughout).

Incident: clusters idle-terminated mid-task during long setup/think gaps,
surfacing as unrelated errors (2026-09). Fix raises each active cluster's
autoTermination to cfg.migration.cluster_idle_timeout_minutes (default 30) for
the migration and restores the original when done. PUT-on-running verified live
2026-09-22 (immediate, no restart).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cluster_lifecycle as lifecycle  # noqa: E402
import job_migrate  # noqa: E402


class _Resp:
    def __init__(self, json_data=None, status=200, text=""):
        self._json = json_data or {}
        self.status_code = status
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


# A cluster GET payload with the writable config sub-objects the PUT must echo.
CLUSTER_CFG = {
    "displayName": "c1",
    "description": None,
    "clusterRuntimeConfig": {"type": "SPARK", "sparkVersion": "3.5.0"},
    "driverConfig": {"driverShape": "amd.generic"},
    "workerConfig": {"workerShape": "amd.generic", "minWorkerCount": 1, "maxWorkerCount": 10},
    "autoTerminationMinutes": 15,
}


# ─── Part A: cluster_lifecycle helpers ──────────────────────────────────────

def test_get_idle_timeout_returns_int(monkeypatch):
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.http_requests, "get",
                        lambda *a, **k: _Resp({"autoTerminationMinutes": 30}))
    assert lifecycle.get_cluster_idle_timeout("cid") == 30


def test_get_idle_timeout_none_on_error_never_raises(monkeypatch):
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())

    def boom(*a, **k):
        raise RuntimeError("network")

    monkeypatch.setattr(lifecycle.http_requests, "get", boom)
    assert lifecycle.get_cluster_idle_timeout("cid") is None


def test_set_idle_timeout_success_echoes_config(monkeypatch):
    state = dict(CLUSTER_CFG)
    put_bodies = []
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.http_requests, "get", lambda *a, **k: _Resp(dict(state)))

    def fake_put(*a, **k):
        body = k.get("json")
        put_bodies.append(body)
        state["autoTerminationMinutes"] = body["autoTerminationMinutes"]
        return _Resp({}, status=202)

    monkeypatch.setattr(lifecycle.http_requests, "put", fake_put)
    assert lifecycle.set_cluster_idle_timeout("cid", 30) is True
    b = put_bodies[0]
    assert b["autoTerminationMinutes"] == 30
    # Only the timeout changes — the other writable config is echoed verbatim so
    # a PUT can't silently reset driver/worker/runtime config.
    assert b["workerConfig"] == CLUSTER_CFG["workerConfig"]
    assert b["driverConfig"] == CLUSTER_CFG["driverConfig"]
    assert b["clusterRuntimeConfig"] == CLUSTER_CFG["clusterRuntimeConfig"]


def test_set_idle_timeout_retries_then_succeeds(monkeypatch):
    state = dict(CLUSTER_CFG)
    attempts = {"put": 0}
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(lifecycle.http_requests, "get", lambda *a, **k: _Resp(dict(state)))

    def fake_put(*a, **k):
        attempts["put"] += 1
        if attempts["put"] == 1:
            return _Resp({}, status=500, text="transient")
        state["autoTerminationMinutes"] = k["json"]["autoTerminationMinutes"]
        return _Resp({}, status=202)

    monkeypatch.setattr(lifecycle.http_requests, "put", fake_put)
    assert lifecycle.set_cluster_idle_timeout("cid", 30, attempts=3) is True
    assert attempts["put"] == 2


def test_set_idle_timeout_best_effort_never_raises_returns_false(monkeypatch):
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(lifecycle.http_requests, "get", lambda *a, **k: _Resp(dict(CLUSTER_CFG)))
    monkeypatch.setattr(lifecycle.http_requests, "put",
                        lambda *a, **k: _Resp({}, status=500, text="down"))
    # Persistent failure: never raises, returns False so the caller carries on.
    assert lifecycle.set_cluster_idle_timeout("cid", 30, attempts=2) is False


def test_set_idle_timeout_verify_mismatch_returns_false(monkeypatch):
    # PUT returns OK but the value never actually changes → GET-verify mismatch.
    monkeypatch.setattr(lifecycle, "_get_signer", lambda: object())
    monkeypatch.setattr(lifecycle.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(lifecycle.http_requests, "get", lambda *a, **k: _Resp(dict(CLUSTER_CFG)))
    monkeypatch.setattr(lifecycle.http_requests, "put", lambda *a, **k: _Resp({}, status=202))
    assert lifecycle.set_cluster_idle_timeout("cid", 30, attempts=2) is False


# ─── Part B: job_migrate bump/revert policy ─────────────────────────────────

@pytest.fixture(autouse=True)
def _clean_idle_state():
    job_migrate._idle_timeout_originals.clear()
    job_migrate._idle_timeout_elevated.clear()
    job_migrate._MIGRATION_IDLE_TIMEOUT_MINUTES = 30
    yield
    job_migrate._idle_timeout_originals.clear()
    job_migrate._idle_timeout_elevated.clear()


def test_bump_only_when_current_below_target(monkeypatch):
    sets = []
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 15)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: sets.append((cid, m)) or True)
    job_migrate._bump_cluster_idle_timeout("A")
    assert sets == [("A", 30)]
    assert job_migrate._idle_timeout_originals["A"] == 15


def test_bump_skips_when_already_generous(monkeypatch):
    sets = []
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 60)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: sets.append((cid, m)) or True)
    job_migrate._bump_cluster_idle_timeout("A")
    assert sets == []  # 60 >= 30: never lower a longer custom timeout
    assert "A" not in job_migrate._idle_timeout_originals


def test_bump_skips_when_unreadable(monkeypatch):
    sets = []
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: None)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: sets.append(1) or True)
    job_migrate._bump_cluster_idle_timeout("A")
    assert sets == []
    assert "A" not in job_migrate._idle_timeout_originals


def test_bump_records_original_even_when_set_unconfirmed(monkeypatch):
    # set_cluster_idle_timeout can apply the PUT and THEN return False if the
    # verify-GET fails — the cluster is genuinely raised. The original MUST be
    # recorded so the finalizer can restore it; recording only on True would
    # strand the cluster elevated (P1). Restoring a PUT that never applied is a
    # harmless no-op, so record-before-set is safe in both cases.
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 15)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", lambda cid, m, **k: False)
    job_migrate._bump_cluster_idle_timeout("A")
    assert job_migrate._idle_timeout_originals.get("A") == 15


def test_unconfirmed_bump_is_restored_not_stranded(monkeypatch):
    # Full strand repro: PUT applies (cluster -> 30) but verify fails so the
    # setter returns False; the finalizer must still restore 30 -> 15.
    cluster_state = {"A": 15}

    def fake_set(cid, minutes, **k):
        cluster_state[cid] = minutes          # PUT applied...
        return False                          # ...but verify-GET failed → False

    # GET reflects the live cluster so the pre-restore ownership check sees the
    # raised value (30 == target) and proceeds to restore.
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: cluster_state[cid])
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", fake_set)
    job_migrate._bump_cluster_idle_timeout("A")
    assert cluster_state["A"] == 30                       # cluster is raised
    assert job_migrate._idle_timeout_originals.get("A") == 15  # original recorded

    # Finalizer restores despite the unconfirmed bump.
    def fake_restore(cid, minutes, **k):
        cluster_state[cid] = minutes
        return True

    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", fake_restore)
    job_migrate._revert_cluster_idle_timeout("A")
    assert cluster_state["A"] == 15                       # not stranded at 30
    assert "A" not in job_migrate._idle_timeout_originals


def test_bump_is_idempotent_per_cluster(monkeypatch):
    calls = {"get": 0}

    def _get(cid):
        calls["get"] += 1
        return 15

    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", _get)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", lambda cid, m, **k: True)
    job_migrate._bump_cluster_idle_timeout("A")
    job_migrate._bump_cluster_idle_timeout("A")  # already ELEVATED → skip (no GET)
    assert calls["get"] == 1


def test_bump_respects_configured_value(monkeypatch):
    sets = []
    job_migrate._MIGRATION_IDLE_TIMEOUT_MINUTES = 60
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: sets.append((cid, m)) or True)
    job_migrate._bump_cluster_idle_timeout("A")
    assert sets == [("A", 60)]  # 30 < configured 60 → raise to 60
    assert job_migrate._idle_timeout_originals["A"] == 30


def test_revisited_cluster_is_rebumped_after_restore_verify_failure(monkeypatch):
    """Finding ① (review 2026-09-22): A -> B -> A. A's mid-job restore applies the
    PUT but its verify fails (setter returns False), so A is actually back at 15
    yet still tracked for a retry. When the DAG returns to A, _bump MUST re-raise
    it — the old skip keyed on "tracked" (not "elevated") left the third task on A
    running unprotected at 15."""
    state = {"A": 15}
    sets = []

    def fake_set(cid, minutes, **k):
        sets.append((cid, minutes))
        state[cid] = minutes                 # the PUT always applies...
        return minutes != 15                 # ...but the restore-to-15 verify fails

    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: state[cid])
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", fake_set)

    # task 1 on A: raise 15 -> 30 (confirmed)
    job_migrate._bump_cluster_idle_timeout("A")
    assert state["A"] == 30 and "A" in job_migrate._idle_timeout_elevated

    # switch A -> B: restore A. PUT applies (A -> 15) but verify fails.
    assert job_migrate._revert_cluster_idle_timeout("A") is False
    assert state["A"] == 15
    assert "A" not in job_migrate._idle_timeout_elevated       # no longer believed elevated
    assert job_migrate._idle_timeout_originals.get("A") == 15  # retry still owed

    # task 3 returns to A: MUST re-raise (the bug left this unprotected at 15).
    sets.clear()
    job_migrate._bump_cluster_idle_timeout("A")
    assert state["A"] == 30, "revisited cluster was not re-protected"
    assert ("A", 30) in sets
    # the TRUE original (15) is preserved, not overwritten by the intermediate.
    assert job_migrate._idle_timeout_originals.get("A") == 15


def test_revert_all_returns_unrestored_clusters(monkeypatch):
    """Finding ③ (review 2026-09-22): a cluster whose restore cannot be confirmed
    is RETURNED by _revert_all so the job outcome can surface it as a cost
    residual (never fatal) rather than reporting an unqualified PASS over it."""
    job_migrate._idle_timeout_originals.update({"A": 15, "B": 20})

    # Both still at the target this run set → we own them → restore is attempted.
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: cid != "A")  # A never confirms; B is fine
    leftover = job_migrate._revert_all_idle_timeouts()
    assert leftover == ["A"]                                 # surfaced as unrestored
    assert "A" in job_migrate._idle_timeout_originals        # kept for a later/manual retry
    assert "B" not in job_migrate._idle_timeout_originals    # B confirmed → popped


def test_revert_does_not_clobber_an_externally_changed_timeout(monkeypatch):
    """Ownership guard (finding ②, review 2026-09-23): if the live timeout moved to
    a value THIS run did not set — an operator or an overlapping run changed it —
    _revert must NOT clobber it with the recorded original. It relinquishes the
    entry and leaves the newer value untouched (never a data/config loss)."""
    sets = []
    job_migrate._idle_timeout_originals["A"] = 15   # this run recorded 15, raised to 30
    # An operator has since set it to 60 — neither our target (30) nor the original (15).
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 60)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: sets.append((cid, m)) or True)
    assert job_migrate._revert_cluster_idle_timeout("A") is True   # relinquished, not a failure
    assert sets == []                                             # 60 was NOT overwritten
    assert "A" not in job_migrate._idle_timeout_originals         # entry relinquished
    assert "A" not in job_migrate._idle_timeout_elevated


def test_revert_does_not_restore_blind_when_live_value_unreadable(monkeypatch):
    """If the pre-restore read fails, ownership cannot be confirmed, so _revert must
    NOT restore blind (that is the clobber the guard prevents). The entry is kept
    for the job-end retry and reported as unrestored."""
    job_migrate._idle_timeout_originals["A"] = 15
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: None)
    called = []
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: called.append((cid, m)) or True)
    assert job_migrate._revert_cluster_idle_timeout("A") is False  # unrestored
    assert called == []                                           # never restored blind
    assert job_migrate._idle_timeout_originals.get("A") == 15     # kept for retry


def test_revisit_preserves_the_first_recorded_original(monkeypatch):
    """The record-once guard (finding ①): when a tracked cluster is revisited and
    reads a DIFFERENT still-low value, _bump must re-raise it WITHOUT overwriting
    the FIRST recorded original — otherwise the eventual restore returns the wrong
    baseline. (Distinguished from the sibling test where the revisit reads the same
    value: here the revisit reads 20 so the guard is actually exercised.)"""
    state = {"A": 15}
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: state["A"])
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: state.__setitem__("A", m) or True)
    # first visit: raise 15 -> 30, original recorded as 15
    job_migrate._bump_cluster_idle_timeout("A")
    assert job_migrate._idle_timeout_originals["A"] == 15 and state["A"] == 30
    # simulate switch-away (no longer believed elevated) with the restore owed,
    # and the cluster reading a DIFFERENT low value on the revisit.
    job_migrate._idle_timeout_elevated.discard("A")
    state["A"] = 20
    job_migrate._bump_cluster_idle_timeout("A")
    assert state["A"] == 30                                  # re-raised
    assert job_migrate._idle_timeout_originals["A"] == 15, "first original overwritten"


def test_process_job_surfaces_unrestored_residual_in_result(monkeypatch):
    """Finding ③ at the orchestration level: process_job's finally must attach
    idle_timeout_unrestored to the RETURNED result (mutating the pending return
    value) so the run summary can flag the cost residual — a green PASS must not
    hide a cluster left elevated."""
    import asyncio

    async def fake_inner(job, session, ledger=None, *, skip_task_dependency_scan=False):
        return {"job_name": "j1", "total_ok": 1, "total_failed": 0}

    monkeypatch.setattr(job_migrate, "_process_job_inner", fake_inner)
    monkeypatch.setattr(job_migrate, "_get_throttle_coord", lambda: None)
    # a cluster still at the target (we own it) whose restore never confirms →
    # left unrestored
    job_migrate._idle_timeout_originals["A"] = 15
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", lambda cid, m, **k: False)
    result = asyncio.run(job_migrate.process_job(
        {"job_name": "j1", "tasks": [], "execution_layers": []}, object()))
    assert result["idle_timeout_unrestored"] == ["A"]
    assert "A" in job_migrate._idle_timeout_originals        # kept for manual retry


def test_revert_restores_recorded_original(monkeypatch):
    sets = []
    job_migrate._idle_timeout_originals["A"] = 15
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)  # we own it
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: sets.append((cid, m)) or True)
    job_migrate._revert_cluster_idle_timeout("A")
    assert sets == [("A", 15)]
    assert "A" not in job_migrate._idle_timeout_originals


def test_revert_all_restores_each(monkeypatch):
    sets = []
    job_migrate._idle_timeout_originals.update({"A": 15, "B": 20})
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)  # we own them
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout",
                        lambda cid, m, **k: sets.append((cid, m)) or True)
    job_migrate._revert_all_idle_timeouts()
    assert sorted(sets) == [("A", 15), ("B", 20)]
    assert job_migrate._idle_timeout_originals == {}


def test_revert_keeps_entry_on_failure_for_retry(monkeypatch):
    job_migrate._idle_timeout_originals["A"] = 15
    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)  # we own it
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", lambda cid, m, **k: False)
    job_migrate._revert_cluster_idle_timeout("A")  # loud ERROR, never raises
    # KEPT (not popped) so the job-end _revert_all gets a final attempt — dropping
    # it would forfeit the retry and strand the cluster at the elevated timeout.
    assert job_migrate._idle_timeout_originals.get("A") == 15


def test_revert_keeps_entry_when_set_raises(monkeypatch):
    job_migrate._idle_timeout_originals["A"] = 15

    def boom(cid, m, **k):
        raise RuntimeError("network")

    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)  # we own it
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", boom)
    job_migrate._revert_cluster_idle_timeout("A")  # never raises
    assert job_migrate._idle_timeout_originals.get("A") == 15  # kept for retry


def test_bump_wired_at_common_point_and_offloaded():
    """Regression guard for the P1/async contract in the per-task loop.

    P1: the bump must fire at the COMMON active-cluster point so it also covers
    the pre-connected `--cluster` path (where neither the connect nor the switch
    branch runs). Async: the bump/revert must be off-loaded via asyncio.to_thread
    so a slow AIDP PUT can't block the event loop (mirrors install_library_async).
    A full loop-drive is disproportionate; end-to-end coverage is the live run.
    """
    import inspect
    src = inspect.getsource(job_migrate)
    # Exactly one bump invocation, at the common point, thread-off-loaded.
    assert src.count("asyncio.to_thread(_bump_cluster_idle_timeout, task_cluster)") == 1
    # No direct (event-loop-blocking) or per-branch bump call remains.
    assert "_bump_cluster_idle_timeout(task_cluster)" not in src
    # Switch-away revert and job-end revert-all are both off-loaded too.
    assert "asyncio.to_thread(_revert_cluster_idle_timeout, session.cluster_id)" in src
    assert "asyncio.to_thread(_revert_all_idle_timeouts)" in src


def test_revert_all_retries_kept_failure_then_pops_on_success(monkeypatch):
    # A revert that failed mid-run stays tracked; the job-end _revert_all retries
    # it and, on success, finally pops it.
    job_migrate._idle_timeout_originals["A"] = 15
    calls = {"n": 0}

    def flaky(cid, m, **k):
        calls["n"] += 1
        return calls["n"] >= 2  # first attempt fails, second succeeds

    monkeypatch.setattr(lifecycle, "get_cluster_idle_timeout", lambda cid: 30)  # we own it
    monkeypatch.setattr(lifecycle, "set_cluster_idle_timeout", flaky)
    job_migrate._revert_cluster_idle_timeout("A")          # fails → kept
    assert job_migrate._idle_timeout_originals.get("A") == 15
    job_migrate._revert_all_idle_timeouts()                # retries → succeeds → pops
    assert "A" not in job_migrate._idle_timeout_originals
    assert calls["n"] == 2
