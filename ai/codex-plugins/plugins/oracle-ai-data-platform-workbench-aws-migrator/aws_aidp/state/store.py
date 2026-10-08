"""On-disk run state at ~/Documents/aws-aidp-migrator/state/<job_key>/runs/.

JSONL append-only so a crashed/killed `aidp-migrate run` can be inspected.
"""
from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

try:  # POSIX file locking protects independent processes, not just threads.
    import fcntl
except ImportError:  # pragma: no cover - exercised only on non-POSIX platforms
    fcntl = None
try:  # Windows has an equivalent byte-range locking API in the standard library.
    import msvcrt
except ImportError:  # pragma: no cover - exercised only on non-Windows platforms
    msvcrt = None

DEFAULT_ROOT = Path.home() / "Documents" / "aws-aidp-migrator" / "state"
_LOCKS_GUARD = threading.Lock()
_PATH_LOCKS: dict[str, threading.Lock] = {}
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{number}" for number in range(1, 10)),
    *(f"LPT{number}" for number in range(1, 10)),
}


def _safe_key(value: str, label: str) -> str:
    if not isinstance(value, str) or not value or value in (".", ".."):
        raise ValueError(f"invalid {label}: {value!r}")
    if value != value.strip():
        raise ValueError(f"invalid {label}: leading or trailing whitespace is not allowed")
    if Path(value).is_absolute() or any(char in value for char in '/\\<>:"|?*'):
        raise ValueError(f"invalid {label}: filesystem delimiter characters are not allowed")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"invalid {label}: control characters are not allowed")
    if len(value.encode("utf-8")) > 200:
        raise ValueError(f"invalid {label}: value is too long")
    if value.split(".", 1)[0].upper() in _WINDOWS_RESERVED:
        raise ValueError(f"invalid {label}: reserved filesystem name")
    return value


def _thread_lock(path: Path) -> threading.Lock:
    key = str(path)
    with _LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.Lock())


@contextmanager
def _advisory_lock(fd: int, *, exclusive: bool):
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
    elif msvcrt is not None:  # pragma: no cover - requires Windows
        os.lseek(fd, 0, os.SEEK_SET)
        mode = msvcrt.LK_LOCK if exclusive else msvcrt.LK_RLCK
        msvcrt.locking(fd, mode, 1)
    try:
        yield
    finally:
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
        elif msvcrt is not None:  # pragma: no cover - requires Windows
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


class RunStore:
    def __init__(self, job_key: str, root: Path | str | None = None) -> None:
        self.root = (Path(root) if root is not None else DEFAULT_ROOT).expanduser()
        self.root.mkdir(parents=True, exist_ok=True)
        self.root = self.root.resolve()
        self.job_key = _safe_key(job_key, "job key")
        job_dir = self.root / self.job_key
        if job_dir.is_symlink():
            raise ValueError(f"invalid job key: state directory is a symlink: {job_dir}")
        job_dir.mkdir(exist_ok=True)
        run_dir = job_dir / "runs"
        if run_dir.is_symlink():
            raise ValueError(f"invalid job key: run directory is a symlink: {run_dir}")
        run_dir.mkdir(exist_ok=True)
        self.dir = run_dir.resolve()
        try:
            contained = os.path.commonpath((str(self.root), str(self.dir))) == str(self.root)
        except ValueError:
            contained = False
        if not contained:
            raise ValueError("state directory escapes the configured root")

    def _path(self, run_key: str) -> Path:
        path = self.dir / f"{_safe_key(run_key, 'run key')}.jsonl"
        if path.is_symlink():
            raise ValueError(f"invalid run key: state file is a symlink: {path}")
        resolved = path.resolve(strict=False)
        try:
            contained = os.path.commonpath((str(self.dir), str(resolved))) == str(self.dir)
        except ValueError:
            contained = False
        if not contained:
            raise ValueError("run history path escapes the configured state directory")
        return path

    def append(self, run_key: str, event: dict) -> None:
        if not isinstance(event, dict):
            raise ValueError("run history event must be a JSON object")
        record = {**event, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        payload = (json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        path = self._path(run_key)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        with _thread_lock(path):
            fd = os.open(path, flags, 0o600)
            try:
                with _advisory_lock(fd, exclusive=True):
                    view = memoryview(payload)
                    while view:
                        written = os.write(fd, view)
                        if written == 0:
                            raise OSError("could not append run history record")
                        view = view[written:]
                    os.fsync(fd)
            finally:
                os.close(fd)

    def history(self, run_key: str) -> list[dict]:
        p = self._path(run_key)
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        with _thread_lock(p):
            try:
                fd = os.open(p, flags)
            except FileNotFoundError:
                return []
            try:
                with _advisory_lock(fd, exclusive=False):
                    chunks = []
                    while True:
                        chunk = os.read(fd, 64 * 1024)
                        if not chunk:
                            break
                        chunks.append(chunk)
            finally:
                os.close(fd)
        try:
            text = b"".join(chunks).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"invalid run history {p}: file is not UTF-8") from exc

        events: list[dict] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"invalid run history {p} at line {line_number}: {exc.msg}"
                ) from exc
            if not isinstance(event, dict):
                raise ValueError(
                    f"invalid run history {p} at line {line_number}: event must be a JSON object"
                )
            events.append(event)
        return events
