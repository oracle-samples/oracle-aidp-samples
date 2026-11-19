"""Safe path-segment validator used by artifact writers.

Bootstrap writes filesystem artifacts whose names come from
user-controlled inputs:

* ``ContentPackSpec.profile`` → ``profiles/<profile>.yaml``,
  ``evidence/<profile>/...``, diagnostic artifacts under
  ``.aidp/diagnostics/<run_id>/...`` (we also write
  ``<errorCode>__<vp-name>.json`` to that subtree).
* Pack ``columnAliases.<name>`` / ``semanticVariants.<name>`` keys →
  ``<vp-name>`` in diagnostic-artifact filenames.

Both surfaces accept arbitrary strings from YAML / JSON. Without
validation, a profile name of ``../../outside`` or a variation-point
name containing ``../`` would let a malformed (or malicious) bundle
turn bootstrap into an arbitrary relative-file-write primitive — the
``.resolve()`` call collapses the traversal into a real path outside
the workdir.

This module provides:

* :func:`validate_path_segment` — the segment-level allowlist check.
* :func:`assert_within_root` — defence-in-depth post-write check that
  the resolved target stays under the intended root.

Both raise :class:`UnsafePathSegmentError` on rejection so callers can
map the failure into AIDPF-coded diagnostics (the variation-phase
entry point catches and re-raises as a hard-fail before any I/O).
"""

from __future__ import annotations

import re
from pathlib import Path


# Allowlist regex: must start with alphanumeric (no leading ``.``, no leading
# ``-``); may contain alphanumeric, dot, hyphen, underscore. No path
# separators (``/`` or ``\``), no whitespace, no ``..``, no shell
# metacharacters.
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class UnsafePathSegmentError(ValueError):
    """Raised when a string intended for use as a filesystem path
    segment fails the safety allowlist.

    Carries the rejected value + the field it was rejected from so the
    caller can produce an actionable error message (e.g. "profile name
    'foo/bar' is unsafe — must match ^[A-Za-z0-9]...").
    """

    def __init__(self, *, value: str, field: str, reason: str) -> None:
        self.value = value
        self.field = field
        self.reason = reason
        super().__init__(
            f"unsafe path segment for {field}={value!r}: {reason}. "
            f"Allowed: alphanumeric start; alphanumeric / dot / hyphen / "
            f"underscore body. No separators, no whitespace, no '..'."
        )


def validate_path_segment(value: str, *, field: str) -> str:
    """Validate that ``value`` is safe to use as a single filesystem
    path segment.

    Args:
        value: the candidate segment.
        field: human-readable name of the source field (for the error
            message).

    Returns:
        ``value`` unchanged on success — allows fluent inline use
        (``segment = validate_path_segment(raw, field='profile')``).

    Raises:
        UnsafePathSegmentError: ``value`` is empty, contains a path
            separator, contains ``..``, or otherwise fails the
            allowlist regex.
    """
    if not isinstance(value, str):
        raise UnsafePathSegmentError(
            value=str(value),
            field=field,
            reason=f"expected str, got {type(value).__name__}",
        )
    if not value:
        raise UnsafePathSegmentError(
            value=value, field=field, reason="empty string"
        )
    # Explicit checks before regex for clearer error messages on the
    # common attack inputs.
    if "/" in value or "\\" in value:
        raise UnsafePathSegmentError(
            value=value, field=field, reason="contains path separator"
        )
    if ".." in value:
        raise UnsafePathSegmentError(
            value=value, field=field, reason="contains '..'"
        )
    if not _SAFE_SEGMENT_RE.match(value):
        raise UnsafePathSegmentError(
            value=value,
            field=field,
            reason="does not match ^[A-Za-z0-9][A-Za-z0-9._-]*$",
        )
    return value


def assert_within_root(target: Path, root: Path, *, field: str) -> None:
    """Defence-in-depth check that ``target.resolve()`` lives under
    ``root.resolve()``.

    Even with segment-level validation, this catches:

    * A bug in the segment validator (something slips through).
    * Future code that constructs a write target by joining
      multi-segment user input the segment validator wasn't applied to.
    * Symlink-traversal edge cases on platforms where ``Path.resolve``
      follows symlinks (rejecting any path that escapes the root after
      symlink resolution).

    Args:
        target: the candidate write path.
        root: the intended persistence root (typically ``workdir``).
        field: human-readable name of the field that drove the target
            path (for the error message).

    Raises:
        UnsafePathSegmentError: ``target.resolve()`` is not under
            ``root.resolve()``.
    """
    target_resolved = target.resolve()
    root_resolved = root.resolve()
    try:
        target_resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise UnsafePathSegmentError(
            value=str(target),
            field=field,
            reason=f"resolves outside intended root {root_resolved}",
        ) from exc


__all__ = [
    "UnsafePathSegmentError",
    "assert_within_root",
    "validate_path_segment",
]
