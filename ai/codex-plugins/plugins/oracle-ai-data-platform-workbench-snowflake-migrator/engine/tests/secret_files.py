"""Credential files for tests: written owner-only, the way the plugin requires.

Every credential reaches the plugin as a `*_path` to a file that only its
owner can read (SEC-AIDP-SAMPLES-001). A test that writes one with
`write_text` gets the umask's 0644 on POSIX and is refused, so the fixtures
go through here. On Windows the mode is not checked (st_mode says nothing
about who can read a file) and `chmod` is a no-op for this purpose.
"""
from __future__ import annotations

import os
import pathlib
import tempfile

__all__ = ["write_secret", "temp_secret", "world_readable"]


def write_secret(path: pathlib.Path | str, text: str = "p") -> str:
    """Write `text` to `path`, mode 0600, and return the path as POSIX text
    (what a YAML config carries on every platform)."""
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.chmod(p, 0o600)
    return p.as_posix()


def temp_secret(text: str = "p") -> str:
    """An owner-only credential file somewhere temporary, for helpers that
    have no tmp_path (mkstemp creates 0600). The caller does not clean it
    up: it holds a test value, not a secret."""
    fd, path = tempfile.mkstemp(prefix="snowmig_test_secret_")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    return pathlib.Path(path).as_posix()


def world_readable(path: pathlib.Path | str) -> None:
    """Make a credential file readable by others, to test the refusal."""
    os.chmod(pathlib.Path(path), 0o644)
