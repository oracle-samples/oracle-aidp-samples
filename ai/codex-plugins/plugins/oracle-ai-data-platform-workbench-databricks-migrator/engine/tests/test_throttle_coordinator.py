"""Tests for scripts/throttle_coordinator.py."""
from __future__ import annotations

import json
import os
import sys
import threading
import time

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from throttle_coordinator import ThrottleCoordinator


@pytest.fixture
def state_path(tmp_path):
    return str(tmp_path / "throttle.json")


def test_acquire_release_single(state_path):
    coord = ThrottleCoordinator(budget=2, state_path=state_path)
    lid = coord.acquire(label="job-a")
    snap = coord.snapshot()
    assert len(snap["in_flight"]) == 1
    assert snap["in_flight"][0]["label"] == "job-a"

    assert coord.release(lid) is True
    assert coord.snapshot()["in_flight"] == []


def test_release_unknown_returns_false(state_path):
    coord = ThrottleCoordinator(budget=2, state_path=state_path)
    assert coord.release("does-not-exist") is False


def test_lease_context_manager_releases(state_path):
    coord = ThrottleCoordinator(budget=1, state_path=state_path)
    with coord.lease(label="job-b"):
        assert len(coord.snapshot()["in_flight"]) == 1
    assert coord.snapshot()["in_flight"] == []


def test_acquire_blocks_until_slot_free(state_path):
    coord = ThrottleCoordinator(budget=1, state_path=state_path)
    lid = coord.acquire(label="job-a")

    acquired_at = []

    def waiter():
        lid2 = coord.acquire(label="job-b")
        acquired_at.append(time.monotonic())
        coord.release(lid2)

    t = threading.Thread(target=waiter)
    t.start()
    # Give the waiter a moment to enter its blocking loop
    time.sleep(1.0)
    assert acquired_at == []  # still blocked
    coord.release(lid)
    t.join(timeout=10)
    assert len(acquired_at) == 1  # got through after release


def test_acquire_timeout(state_path):
    coord = ThrottleCoordinator(budget=1, state_path=state_path)
    coord.acquire(label="hold")
    with pytest.raises(TimeoutError):
        coord.acquire(label="b", timeout_sec=1.5)


def test_record_cb_event_shrinks_budget(state_path):
    # Start with a larger original budget so we can observe the shrink
    coord = ThrottleCoordinator(budget=10, state_path=state_path, adaptive=True)
    # Acquire one lease so original_budget gets pinned in state
    lid = coord.acquire(label="seed")
    coord.release(lid)

    # 3 events within the window -> shrink to half
    coord.record_cb_event(label="x")
    coord.record_cb_event(label="x")
    snap = coord.record_cb_event(label="x")
    assert snap["budget"] < 10
    assert snap["budget"] >= 1
    assert snap["original_budget"] == 10


def test_record_cb_event_does_not_shrink_when_disabled(state_path):
    coord = ThrottleCoordinator(budget=10, state_path=state_path, adaptive=False)
    coord.acquire(label="seed")
    for _ in range(5):
        snap = coord.record_cb_event(label="x")
    assert snap["budget"] == 10


def test_state_file_round_trip(state_path):
    coord = ThrottleCoordinator(budget=4, state_path=state_path)
    lid = coord.acquire(label="job-x")
    # Inspect the on-disk file directly
    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)
    assert state["budget"] == 4
    assert any(l["lease_id"] == lid for l in state["in_flight"])
    coord.release(lid)


def test_stale_lease_reaped(state_path):
    """A lease for a non-existent PID should be reaped on the next acquire."""
    coord = ThrottleCoordinator(budget=1, state_path=state_path)
    # Plant a fake lease for a guaranteed-dead PID and a far-past timestamp.
    fake_state = {
        "in_flight": [
            {
                "pid": 0,  # invalid -- _pid_alive returns False
                "lease_id": "ghost",
                "acquired_at": 0.0,
                "label": "ghost",
            }
        ],
        "budget": 1,
        "cb_events": [],
        "last_cb_at": 0.0,
        "original_budget": 1,
    }
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(fake_state, f)
    # New acquire should clear the ghost and succeed in 1-budget mode.
    lid = coord.acquire(label="real", timeout_sec=5.0)
    snap = coord.snapshot()
    assert len(snap["in_flight"]) == 1
    assert snap["in_flight"][0]["lease_id"] == lid


def test_corrupt_state_file_recovers(state_path):
    with open(state_path, "w", encoding="utf-8") as f:
        f.write("{not valid json")
    coord = ThrottleCoordinator(budget=2, state_path=state_path)
    # Should not raise; coordinator starts fresh.
    lid = coord.acquire(label="x", timeout_sec=5.0)
    coord.release(lid)
