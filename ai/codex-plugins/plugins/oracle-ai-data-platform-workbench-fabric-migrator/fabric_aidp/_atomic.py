"""Durable file writes. Temp file → fsync → atomic replace → fsync the directory.

An interrupted run must never leave a half-written artifact that a later `verify`
would read as complete.
"""
from __future__ import annotations

import errno
import json
import os
import uuid
from pathlib import Path

# Eight hex characters of uuid4, not all thirty-two. The temp name only has to
# be unique among the temp files alive in one directory at one instant, and the
# pid already separates processes, so eight hex is 2**32 within a single pid --
# far past what a write of one artifact needs.
#
# The length is the point. Measured 2026-09-29 on macOS, NAME_MAX 255, pid 543:
# a 224-character final filename that `Path.write_text` accepts directly
# produced a 266-character temp name under the full hex and failed with
# [Errno 63] File name too long. The same write succeeds here: 242 characters.
# Overhead on top of the final name went from 42 to 18 characters (44 to 20
# with a five-digit pid). Windows' default MAX_PATH of 260 makes the same gap
# bite on the whole path rather than the one component.
_TOKEN_HEX = 8


def _temp_path(path: Path) -> Path:
    return path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:_TOKEN_HEX]}.tmp")


def _temp_write_error(path: Path, temp: Path, cause: OSError) -> OSError:
    """Re-word a failure to create the temp file so it names the temp file.

    The write goes through a sibling that is longer than the final name, so a
    length limit the final path clears can still stop the write. The operating
    system reports that as ENAMETOOLONG on POSIX and, on Windows, as "the
    system cannot find the path specified" -- which sends the reader looking
    for a missing output directory that is not missing. Say what happened.
    """
    # The temp path goes in the exception's `filename`, which str() appends,
    # so naming it again here would print it twice -- and these are long names
    # by the time this fires.
    detail = (f"could not create the temporary file that writing "
              f"{path.name!r} goes through")
    if cause.errno in (errno.ENAMETOOLONG, errno.ENOENT) and path.parent.is_dir():
        detail += (
            f". Its directory exists, and the temporary name is "
            f"{len(str(temp))} characters against the final path's "
            f"{len(str(path))} -- {len(str(temp)) - len(str(path))} more -- so "
            f"a filename or path length limit that the final name clears was "
            f"exceeded by the temporary one. Shorten the output directory or "
            f"the asset name")
    return OSError(cause.errno, f"{cause.strerror or cause}: {detail}",
                   str(temp))


def write_text_atomic(path: Path, value: str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _temp_path(path)
    try:
        try:
            with temp.open("w", encoding="utf-8", newline="") as stream:
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as exc:
            raise _temp_write_error(path, temp, exc) from exc
        os.replace(temp, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                try:
                    os.fsync(directory_fd)
                except OSError:
                    # Some filesystems do not support directory fsync. The file
                    # itself is already durable and replaced.
                    pass
            finally:
                os.close(directory_fd)
    finally:
        try:
            temp.unlink()
        except OSError:
            # Any OSError, not just FileNotFoundError. A raise from a `finally`
            # displaces the exception already in flight, and on the one path
            # this module most needs to report clearly -- a temp name too long
            # to create -- `unlink` is too long to call either, so it raised
            # ENAMETOOLONG and threw away the explanation. Measured
            # 2026-09-29 with the old 32-hex token and a 224-character name:
            # the write surfaced as
            #   OSError: [Errno 63] File name too long
            # raised from `temp.unlink()` in this block, not from the write.
            # Failing to remove a temp file is never the more important of
            # two failures.
            pass


def write_json_atomic(path: Path, value: object) -> None:
    write_text_atomic(Path(path), json.dumps(value, indent=2, ensure_ascii=False))
