"""
Cross-Process Migration Throttle Coordinator
=============================================
A file-backed token bucket so N parallel migration processes (e.g. many
``run_migration.sh`` invocations) coordinate without a central server.

Why
---
The Databricks->AIDP migration toolkit is single-job-per-process. Scaling
to many jobs is done by launching processes in parallel. That works for
the orchestration layer but NOT for the underlying OCI Object Storage,
which has per-bucket request-rate limits. Without coordination, all many
processes try to run simultaneously, the cluster issues thousands of
parallel PutObject calls per second, the OCI Java SDK's CircuitBreaker
trips, and entire jobs fail with::

    BmcException: (-1, null, false) CircuitBreaker is OPEN ...

This coordinator gates job starts (and optionally cell executions) so the
in-flight count stays within a configured budget. It also tracks observed
429 / CircuitBreaker events and adaptively shrinks the budget.

Storage model
-------------
A single state file (default: ``$HOME/.aidp_throttle.json``) holds:

  * ``in_flight``: list of active leases ``[{pid, lease_id, acquired_at, label}]``
  * ``budget``: current max concurrent leases
  * ``cb_events``: list of recent (timestamp, label) tuples
  * ``last_cb_at``: timestamp of most recent CircuitBreaker observation

All reads/writes go through a portable file lock (``portalocker`` if
installed; ``fcntl`` on Linux/macOS as fallback; ``msvcrt`` on Windows).

Usage
-----
::

    from throttle_coordinator import ThrottleCoordinator

    coord = ThrottleCoordinator(budget=48)
    with coord.lease(label="ExampleJob"):
        run_migration_for_job(...)

The lease blocks until a slot is free. Stale leases (process gone) are
reaped automatically on every ``acquire``.

This module is dependency-free except for an optional ``portalocker``.
Pure-Python and import-safe.
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


DEFAULT_STATE_PATH = os.environ.get(
    "AIDP_THROTTLE_STATE",
    os.path.join(os.path.expanduser("~"), ".aidp_throttle.json"),
)

# How long without a heartbeat before a lease is considered stale and reaped.
DEFAULT_LEASE_TTL_SEC = 4 * 60 * 60  # 4h: cell timeout in job_migrate is 14400s

# How many CB events in the recent window before we shrink the budget.
CB_EVENT_WINDOW_SEC = 5 * 60
CB_SHRINK_AFTER = 3
CB_SHRINK_PCT = 0.5  # halve the budget on sustained CB pressure
CB_RECOVER_AFTER_SEC = 15 * 60  # restore budget if no CB events for 15 min

# Sleep range while polling for a free slot.
POLL_MIN_SEC = 0.5
POLL_MAX_SEC = 4.0


# -----------------------------------------------------------------------------
# Cross-platform file lock
# -----------------------------------------------------------------------------
class _FileLock:
    """Minimal exclusive file lock. Uses portalocker if available."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._fh = None
        # Lazily-resolved backend
        self._backend: Optional[str] = None

    def __enter__(self) -> "_FileLock":
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._fh = open(self.path + ".lock", "a+")
        try:
            import portalocker  # type: ignore
            portalocker.lock(self._fh, portalocker.LOCK_EX)
            self._backend = "portalocker"
        except ImportError:
            if sys.platform == "win32":
                import msvcrt  # type: ignore
                # Block until the lock is available.
                while True:
                    try:
                        msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
                        break
                    except OSError:
                        time.sleep(0.1)
                self._backend = "msvcrt"
            else:
                import fcntl  # type: ignore
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
                self._backend = "fcntl"
        return self

    def __exit__(self, *exc) -> None:
        try:
            if self._backend == "portalocker":
                import portalocker  # type: ignore
                portalocker.unlock(self._fh)
            elif self._backend == "msvcrt":
                import msvcrt  # type: ignore
                try:
                    self._fh.seek(0)
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            elif self._backend == "fcntl":
                import fcntl  # type: ignore
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._fh.close()
            except Exception:
                pass


def _pid_alive(pid: int) -> bool:
    """Best-effort check whether a process exists. False on permission denied."""
    if pid <= 0:
        return False
    try:
        if sys.platform == "win32":
            import ctypes
            PROCESS_QUERY_LIMITED = 0x1000
            h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED, False, pid)
            if not h:
                return False
            ctypes.windll.kernel32.CloseHandle(h)
            return True
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# -----------------------------------------------------------------------------
# State helpers
# -----------------------------------------------------------------------------
@dataclass
class _State:
    in_flight: List[Dict[str, Any]] = field(default_factory=list)
    budget: int = 48
    cb_events: List[Dict[str, Any]] = field(default_factory=list)
    last_cb_at: float = 0.0
    original_budget: int = 48

    def to_dict(self) -> Dict[str, Any]:
        return {
            "in_flight": self.in_flight,
            "budget": self.budget,
            "cb_events": self.cb_events,
            "last_cb_at": self.last_cb_at,
            "original_budget": self.original_budget,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any], default_budget: int) -> "_State":
        return cls(
            in_flight=list(d.get("in_flight", [])),
            budget=int(d.get("budget", default_budget)),
            cb_events=list(d.get("cb_events", [])),
            last_cb_at=float(d.get("last_cb_at", 0.0)),
            original_budget=int(d.get("original_budget", default_budget)),
        )


def _read_state(path: str, default_budget: int) -> _State:
    if not os.path.exists(path):
        return _State(budget=default_budget, original_budget=default_budget)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return _State.from_dict(json.load(f), default_budget)
    except (json.JSONDecodeError, OSError):
        # Corrupt file -- start fresh. Lock guarantees no concurrent writers.
        return _State(budget=default_budget, original_budget=default_budget)


def _write_state(path: str, state: _State) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state.to_dict(), f, indent=2, sort_keys=True)
    os.replace(tmp, path)


# -----------------------------------------------------------------------------
# Coordinator
# -----------------------------------------------------------------------------
class ThrottleCoordinator:
    """File-backed cross-process token-bucket coordinator.

    Args:
        budget: Initial maximum concurrent leases. For large-scale
            scale, calibrate from per-bucket budget * num_buckets / avg
            ops-per-job. Start at 32-48 and tune from observed CB rate.
        state_path: Path to the shared state JSON file. Default
            ``$AIDP_THROTTLE_STATE`` or ``~/.aidp_throttle.json``.
        lease_ttl_sec: Stale-lease reap threshold.
        adaptive: If True, observed CircuitBreaker events shrink the
            budget; quiet windows restore it. Default True.
    """

    def __init__(
        self,
        budget: int = 48,
        state_path: Optional[str] = None,
        lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC,
        adaptive: bool = True,
    ) -> None:
        if budget < 1:
            raise ValueError(f"budget must be >= 1, got {budget}")
        self.budget = budget
        self.state_path = state_path or DEFAULT_STATE_PATH
        self.lease_ttl_sec = lease_ttl_sec
        self.adaptive = adaptive
        self._lock = _FileLock(self.state_path)

    # -------------------------------------------------------------- Internal
    def _reap_and_adjust(self, state: _State, now: float) -> _State:
        """Drop stale leases, prune old CB events, restore budget if quiet."""
        state.in_flight = [
            l
            for l in state.in_flight
            if (now - float(l.get("acquired_at", 0))) < self.lease_ttl_sec
            and _pid_alive(int(l.get("pid", -1)))
        ]
        # Drop CB events older than the window
        cutoff = now - CB_EVENT_WINDOW_SEC
        state.cb_events = [e for e in state.cb_events if e.get("at", 0) > cutoff]

        if self.adaptive:
            # Restore budget if no CB activity for a while
            if (
                state.budget < state.original_budget
                and state.last_cb_at > 0
                and (now - state.last_cb_at) > CB_RECOVER_AFTER_SEC
            ):
                state.budget = state.original_budget
        return state

    # -------------------------------------------------------------- Public
    def acquire(self, label: str = "", timeout_sec: Optional[float] = None) -> str:
        """Block until a lease is available, then return a lease_id.

        Args:
            label: Free-form label (e.g. job name) for observability.
            timeout_sec: Optional max wait. None means wait forever.

        Returns:
            Lease ID string. Pass to :meth:`release`.

        Raises:
            TimeoutError: if ``timeout_sec`` elapses without acquisition.
        """
        deadline = (time.monotonic() + timeout_sec) if timeout_sec else None
        lease_id = uuid.uuid4().hex

        while True:
            now = time.time()
            with self._lock:
                state = _read_state(self.state_path, self.budget)
                # Initialise original_budget on first run
                if state.original_budget != self.budget and not state.in_flight:
                    state.original_budget = self.budget
                    state.budget = self.budget
                state = self._reap_and_adjust(state, now)

                if len(state.in_flight) < state.budget:
                    state.in_flight.append(
                        {
                            "pid": os.getpid(),
                            "lease_id": lease_id,
                            "acquired_at": now,
                            "label": label,
                        }
                    )
                    _write_state(self.state_path, state)
                    return lease_id

            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"could not acquire throttle lease within {timeout_sec}s"
                )
            time.sleep(random.uniform(POLL_MIN_SEC, POLL_MAX_SEC))

    def release(self, lease_id: str) -> bool:
        """Release a lease by id. Returns True if the lease was found."""
        now = time.time()
        with self._lock:
            state = _read_state(self.state_path, self.budget)
            before = len(state.in_flight)
            state.in_flight = [
                l for l in state.in_flight if l.get("lease_id") != lease_id
            ]
            state = self._reap_and_adjust(state, now)
            _write_state(self.state_path, state)
            return len(state.in_flight) != before

    @contextmanager
    def lease(self, label: str = "", timeout_sec: Optional[float] = None):
        """Context manager wrapping :meth:`acquire` / :meth:`release`."""
        lid = self.acquire(label=label, timeout_sec=timeout_sec)
        try:
            yield lid
        finally:
            self.release(lid)

    def record_cb_event(self, label: str = "") -> Dict[str, Any]:
        """Record an observed CircuitBreaker / 429 event.

        After ``CB_SHRINK_AFTER`` events within ``CB_EVENT_WINDOW_SEC``,
        the budget is shrunk by ``CB_SHRINK_PCT``. Quiet windows restore
        the original budget on subsequent ``acquire`` calls.

        Returns the post-update budget snapshot for logging.
        """
        now = time.time()
        with self._lock:
            state = _read_state(self.state_path, self.budget)
            state.cb_events.append({"at": now, "label": label})
            state.last_cb_at = now
            state = self._reap_and_adjust(state, now)
            if (
                self.adaptive
                and len(state.cb_events) >= CB_SHRINK_AFTER
                and state.budget > 1
            ):
                shrunk = max(1, int(state.budget * CB_SHRINK_PCT))
                if shrunk < state.budget:
                    state.budget = shrunk
            _write_state(self.state_path, state)
            return {
                "budget": state.budget,
                "original_budget": state.original_budget,
                "in_flight": len(state.in_flight),
                "cb_events_in_window": len(state.cb_events),
            }

    def snapshot(self) -> Dict[str, Any]:
        """Return current state without modification (for logging/CLI)."""
        now = time.time()
        with self._lock:
            state = _read_state(self.state_path, self.budget)
            state = self._reap_and_adjust(state, now)
            _write_state(self.state_path, state)
            return state.to_dict()


# -----------------------------------------------------------------------------
# CLI: useful for ops -- inspect, reset, simulate
# -----------------------------------------------------------------------------
def _main(argv: Optional[List[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="AIDP migration throttle coordinator")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_show = sub.add_parser("show", help="print current state")
    p_show.add_argument("--state", default=DEFAULT_STATE_PATH)
    p_show.add_argument("--budget", type=int, default=48)

    p_reset = sub.add_parser("reset", help="reset state file (clears all leases)")
    p_reset.add_argument("--state", default=DEFAULT_STATE_PATH)
    p_reset.add_argument("--budget", type=int, default=48)

    p_cb = sub.add_parser("record-cb", help="manually record a CB event")
    p_cb.add_argument("--state", default=DEFAULT_STATE_PATH)
    p_cb.add_argument("--budget", type=int, default=48)
    p_cb.add_argument("--label", default="manual")

    args = p.parse_args(argv)
    coord = ThrottleCoordinator(budget=args.budget, state_path=args.state)

    if args.cmd == "show":
        print(json.dumps(coord.snapshot(), indent=2, sort_keys=True))
        return 0
    if args.cmd == "reset":
        with coord._lock:
            _write_state(
                args.state,
                _State(budget=args.budget, original_budget=args.budget),
            )
        print(f"reset {args.state} budget={args.budget}")
        return 0
    if args.cmd == "record-cb":
        out = coord.record_cb_event(label=args.label)
        print(json.dumps(out, indent=2, sort_keys=True))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(_main())
