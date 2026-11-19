"""Runtime open() shim: transparently redirect /dbfs/* reads on FileNotFoundError.

Motivation
----------
The static source rewriter (job_migrate._apply_dbfs_translations) catches
`/dbfs/...` LITERALS in migrated source, but it can't see paths that only
materialize at runtime — e.g. dict values loaded from disk / model registry:

    schema_map = pickle.load(open("model.pkl", "rb"))
    # schema_map["col_a"] == "/dbfs/FileStore/schemas/col_a.pkl" at runtime
    pickle.load(open(schema_map[c], "rb"))
    #                     ^^^^^^^^^^^^^ static rewriter can't reach into this

That gap caused the Model_Mon Cell 44 stuck-3-hours issue (schema-inference
fallthrough on silent FileNotFoundError).

Approach
--------
- Monkey-patch builtins.open once at aidp_compat import time (idempotent).
- Behavior:
    * If path is str and starts with `/dbfs/` or `dbfs:` — try the ORIGINAL
      path first. On FileNotFoundError, retry with translate_path(path) IF
      that yields a different location.
    * Everything else — direct pass-through to the original open().
- Fallback semantics (try original first) preserve real error signals: if a
  path truly doesn't exist anywhere, the FileNotFoundError is raised with the
  original path in the message.
- Read AND write modes both fall back — a write to /dbfs/x when /dbfs is
  absent falls through to /Volumes/.../dbfs/x, matching the read path so
  downstream reads succeed at the same location.
- Non-str inputs (fd int, pathlib.Path, io.BytesIO) — pass-through, no logic.
- Exception scope: intentionally narrow to `FileNotFoundError` (POSIX errno
  2). Broader OSError variants (permission, is-a-directory, etc.) are NOT
  caught — those are real errors we don't want to mask with a silent fallback.

Scope
-----
- Catches: builtins.open, pickle.load(open(...)), json.load(open(...)),
  torch.load(open(...)), Image.open (uses builtins.open under the hood),
  numpy.load (str path → open), pandas.read_pickle (str path → open),
  and any other stdlib code path that resolves `open` to builtins.
- Does NOT catch: JVM-side file access (spark.read.parquet path resolution),
  C extensions that call POSIX open() directly (rare).

Idempotent — safe to call install() multiple times.
"""

from __future__ import annotations

import builtins as _builtins
from typing import Any

from aidp_compat.path_translator import translate_path


_ORIGINAL_OPEN = _builtins.open
# Defensive unwrap: if this module is re-executed (importlib.reload, or a
# second import after sys.modules manipulation -- happens in test suites and
# kernel restarts) while a prior shim instance is already installed,
# builtins.open IS that shim. Capturing it as "original" would make
# uninstall() "restore" a shim, leaving open() permanently wrapped. Unwrap
# via the marker: prefer the prior instance's saved original; fall back to
# io.open (== the true builtins.open in CPython) for prior shim versions
# that carried no back-reference.
if getattr(_ORIGINAL_OPEN, "__aidp_dbfs_shim__", False):
    import io as _io
    _ORIGINAL_OPEN = getattr(_ORIGINAL_OPEN, "__aidp_dbfs_original__", None) or _io.open


def _dbfs_aware_open(file, *args, **kwargs):
    """Drop-in replacement for builtins.open with /dbfs/* fallback.

    - str path starting with /dbfs/ or dbfs: → try original, on
      FileNotFoundError retry via translate_path
    - everything else → pass-through
    """
    # Only string paths are candidates for translation. pathlib.Path, int fd,
    # bytes, buffered readers, etc. bypass this entirely.
    if not isinstance(file, str):
        return _ORIGINAL_OPEN(file, *args, **kwargs)

    if not (file.startswith("/dbfs/") or file.startswith("dbfs:")):
        return _ORIGINAL_OPEN(file, *args, **kwargs)

    # Try the original path first. This preserves the caller's intent when
    # the file DOES exist at the raw location (e.g. cluster admin created
    # a compatibility symlink).
    try:
        return _ORIGINAL_OPEN(file, *args, **kwargs)
    except FileNotFoundError:
        translated = translate_path(file)
        if translated == file:
            # translate_path returned the same string — no fallback available,
            # re-raise the original error with the original path in the message.
            raise
        # Try the translated location. If THAT also fails, the caller sees a
        # FileNotFoundError with the translated path in the message — clear
        # signal that neither location has the file.
        return _ORIGINAL_OPEN(translated, *args, **kwargs)


# Marker attribute so install() is idempotent — multiple imports of aidp_compat
# (e.g. re-imports across %run children in the same kernel) don't wrap on wrap.
_dbfs_aware_open.__aidp_dbfs_shim__ = True  # type: ignore[attr-defined]
_dbfs_aware_open.__aidp_dbfs_original__ = _ORIGINAL_OPEN  # type: ignore[attr-defined]


def install() -> None:
    """Idempotently install the /dbfs-aware open() shim.

    Patches THREE locations because the AIDP Jupyter kernel binds `open` in
    the `__main__` module's globals at startup, which SHADOWS the builtin.
    Just replacing `builtins.open` is not enough — user code at the kernel's
    top level resolves `open` via `__main__.__dict__` first, so we must
    patch there too.

    - `builtins.open`         → safety net for stdlib code that resolves
                                 `open` from `__builtins__` (pickle.load
                                 internals, third-party libraries)
    - `__main__.__dict__[open]` → the KERNEL's shadowed reference; this is
                                   what user cells like `open(path)` see
    - Current-frame globals   → immediate effect when install() is called
                                 from within a running cell (before this
                                 function returns, code in the same cell
                                 that uses `open` picks up the shim)

    Called once from aidp_compat.__init__. Safe to call multiple times —
    all three patch points are re-applied unconditionally on every call.
    Re-application is intentional: a fresh notebook session may have reset
    __main__.open even when builtins.open was patched in a prior session,
    and the caller-frame patch has to happen from THIS call's frame.
    """
    _builtins.open = _dbfs_aware_open  # type: ignore[assignment]
    # __main__ patch — this is what AIDP kernel's user cells actually see
    import sys as _sys
    _main = _sys.modules.get("__main__")
    if _main is not None and hasattr(_main, "__dict__"):
        _main.__dict__["open"] = _dbfs_aware_open
    # Current-frame globals — install() may be called mid-cell; ensure the
    # rest of THIS cell also sees the shim. Uses frame introspection so
    # this works whether install() is called from the module-init path or
    # inline in a bootstrap cell.
    try:
        import sys as _sys2
        _caller_globals = _sys2._getframe(1).f_globals
        _caller_globals["open"] = _dbfs_aware_open
    except (ValueError, AttributeError):
        # Frame introspection unavailable (e.g. embedded interpreters);
        # __main__ patch above still covers the primary case.
        pass


def uninstall() -> None:
    """Restore the original open() in all patched locations. Test-only —
    production code should never need to remove the shim mid-kernel."""
    _builtins.open = _ORIGINAL_OPEN  # type: ignore[assignment]
    import sys as _sys
    _main = _sys.modules.get("__main__")
    if _main is not None and hasattr(_main, "__dict__"):
        if _main.__dict__.get("open") is _dbfs_aware_open:
            _main.__dict__["open"] = _ORIGINAL_OPEN
    try:
        _caller_globals = _sys._getframe(1).f_globals
        if _caller_globals.get("open") is _dbfs_aware_open:
            _caller_globals["open"] = _ORIGINAL_OPEN
    except (ValueError, AttributeError):
        pass
