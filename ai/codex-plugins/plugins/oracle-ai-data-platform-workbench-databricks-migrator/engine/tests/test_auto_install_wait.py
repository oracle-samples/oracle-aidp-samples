"""Auto-install must wait for TRUE install completion, then verify by use.

Incident (2026-09-08, run 80000000000011): `_poll_library_install` accepted
status RESOLVED as success — pip resolution had finished but installation was
still in flight. The auto-install declared "seaborn installed — retrying cell"
~9s before the package was importable, the retry loop exhausted, and the cell
failed even though the install landed minutes later. Contract now:

  - success ONLY on status == INSTALLED (RESOLVED keeps polling)
  - FAILED / INSTALL_ON_RESTART are loud failures
  - after INSTALLED, the module must be import-verified on the live kernel
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import cluster_lifecycle as cl


def _run(coro):
    return asyncio.run(coro)


def _no_sleep(monkeypatch):
    async def fake_sleep(_):
        return None
    monkeypatch.setattr(cl.asyncio, "sleep", fake_sleep)


def _lib_sequence(monkeypatch, path, statuses):
    """get_installed_libraries returns the next status per call (last repeats)."""
    calls = {"n": 0}

    def fake(cluster_id):
        i = min(calls["n"], len(statuses) - 1)
        calls["n"] += 1
        st = statuses[i]
        item = {"path": path, "status": st}
        if st == "FAILED":
            item["stateMessage"] = "simulated pip failure"
        return [item]

    monkeypatch.setattr(cl, "get_installed_libraries", fake)
    return calls


LIB = "/Workspace/x/resolved_requirements_test.txt"


# ─── _poll_library_install ────────────────────────────────────────────────────

def test_resolved_is_not_success_keeps_polling_until_installed(monkeypatch):
    """The seaborn incident: RESOLVED must NOT be accepted as success."""
    _no_sleep(monkeypatch)
    calls = _lib_sequence(monkeypatch, LIB,
                          ["PENDING", "RESOLVING", "RESOLVED", "RESOLVED", "INSTALLED"])
    ok = _run(cl._poll_library_install("c1", LIB, timeout=600))
    assert ok is True
    assert calls["n"] == 5  # polled THROUGH the RESOLVED states


def test_perpetual_resolved_times_out_as_failure(monkeypatch):
    _no_sleep(monkeypatch)
    _lib_sequence(monkeypatch, LIB, ["RESOLVED"])
    assert _run(cl._poll_library_install("c1", LIB, timeout=40)) is False


def test_failed_status_is_loud_failure(monkeypatch):
    _no_sleep(monkeypatch)
    _lib_sequence(monkeypatch, LIB, ["RESOLVING", "FAILED"])
    assert _run(cl._poll_library_install("c1", LIB, timeout=600)) is False


def test_install_on_restart_is_failure_not_retry(monkeypatch):
    _no_sleep(monkeypatch)
    _lib_sequence(monkeypatch, LIB, ["INSTALL_ON_RESTART"])
    assert _run(cl._poll_library_install("c1", LIB, timeout=600)) is False


def test_installed_is_success(monkeypatch):
    _no_sleep(monkeypatch)
    _lib_sequence(monkeypatch, LIB, ["RESOLVING", "INSTALLED"])
    assert _run(cl._poll_library_install("c1", LIB, timeout=600)) is True


# ─── _verify_import_on_kernel ────────────────────────────────────────────────

class _Session:
    """Kernel stub whose import probe flips to PRESENT after N calls."""

    def __init__(self, present_after=1):
        self.present_after = present_after
        self.calls = 0

    async def execute(self, code, timeout=None):
        self.calls += 1
        state = "PRESENT" if self.calls >= self.present_after else "MISSING"
        return {"outputs": [{"type": "stream", "text": state}]}


def test_verify_import_success_first_try(monkeypatch):
    _no_sleep(monkeypatch)
    s = _Session(present_after=1)
    assert _run(cl._verify_import_on_kernel(s, "seaborn")) is True
    assert s.calls == 1


def test_verify_import_absorbs_visibility_lag(monkeypatch):
    """INSTALLED but not yet importable: bounded re-probe must absorb the lag."""
    _no_sleep(monkeypatch)
    s = _Session(present_after=4)
    assert _run(cl._verify_import_on_kernel(s, "seaborn")) is True
    assert s.calls == 4


def test_verify_import_gives_up_loudly(monkeypatch):
    _no_sleep(monkeypatch)
    s = _Session(present_after=999)
    assert _run(cl._verify_import_on_kernel(s, "seaborn",
                                            attempts=3, interval=1)) is False
    assert s.calls == 3
