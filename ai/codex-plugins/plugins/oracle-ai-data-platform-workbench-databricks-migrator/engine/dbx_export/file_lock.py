"""Interprocess exclusive file lock shared across the toolkit.

Single home for the lock used by both the exporter's shape-limits store
(dbx_export.cluster_shape_mapping) and the migration throttle coordinator
(scripts/throttle_coordinator.py). Prefers portalocker when installed and
falls back to the platform primitive (msvcrt on Windows, fcntl elsewhere) —
there is deliberately NO no-op fallback: a lock that silently stops locking
loses exactly the concurrent-writer safety it exists to provide.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Optional


class FileLock:
    """Minimal exclusive file lock. Uses portalocker if available."""

    def __init__(self, path: str) -> None:
        self.path = str(path)
        self._fh = None
        # Lazily-resolved backend
        self._backend: Optional[str] = None

    def __enter__(self) -> "FileLock":
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
