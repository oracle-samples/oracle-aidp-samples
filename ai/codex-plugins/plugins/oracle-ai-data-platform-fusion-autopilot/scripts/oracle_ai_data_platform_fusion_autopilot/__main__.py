"""Self-bootstrapping entry point for the ``aidp-fusion-autopilot`` CLI.

Run as ``python -m oracle_ai_data_platform_fusion_autopilot`` (the ``bin/`` wrappers
do exactly this). The plugin ships its source in the Codex cache, but Codex
does not run an automatic Python install step, so this module provisions the third-party
dependencies **lazily, on first invocation**, then hands off to the Click CLI.


* **Run from source.** The package is imported straight from ``scripts/`` on the
  cache; only third-party deps need installing.
* **Stdlib-only until deps are ready.** Everything above the final ``cli`` import
  uses the standard library, so the preamble works before deps exist and on the
  bare interpreter.
* **Version floor.** Re-asserts Python >= 3.10 (the wrapper is the first guard;
  this covers direct ``python -m`` use). The code crashes on 3.9 (PEP 604
  ``X | None`` in Pydantic models), so we fail with a clear message instead.
* **Lazy ``pip install --target``** into a directory keyed by
  ``<version>-<req_sha8>-<cache_tag>-<sys.platform>-<machine>`` so a pin change,
  Python ABI change, or platform change each get a clean dir. The install runs
  through ``sys.executable`` (never bare ``pip``) so it matches the keyed ABI.
* **Atomic install.** Install into a temp dir, validate, ``os.replace`` promote,
  write the ``.complete`` marker last; a partial/failed install never poisons the
  keyed dir. A file lock makes concurrent first-calls safe.
* **Child-process visibility.** Deps go on both ``sys.path`` (this process) and
  ``os.environ["PYTHONPATH"]`` (so ``python -m build`` subprocesses find them).
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

# --- paths -----------------------------------------------------------------
# __main__.py lives at <root>/scripts/oracle_ai_data_platform_fusion_autopilot/
_PKG_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _PKG_DIR.parent
_REQUIREMENTS = _SCRIPTS_DIR / "requirements.txt"

_MIN_PYTHON = (3, 10)
_NO_AUTOINSTALL = os.environ.get("AIDP_FUSION_NO_AUTOINSTALL") == "1"


def _eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def _require_min_python() -> None:
    if sys.version_info < _MIN_PYTHON:
        have = ".".join(str(p) for p in sys.version_info[:3])
        _eprint(
            f"[aidp-fusion-autopilot] needs Python >= "
            f"{_MIN_PYTHON[0]}.{_MIN_PYTHON[1]} (found {have}). "
            "Re-run with a newer interpreter."
        )
        raise SystemExit(1)


def _slug(value: str) -> str:
    """Keep dir-name-safe chars only."""
    return "".join(c if (c.isalnum() or c in "._") else "-" for c in value)


def _deps_home() -> Path:
    home = (
        os.environ.get("AIDP_FUSION_HOME")
        or os.environ.get("PLUGIN_DATA")
        or os.path.join(os.path.expanduser("~"), ".aidp-fusion-autopilot")
    )
    return Path(home)


def _deps_key(version: str) -> str:
    req_sha8 = hashlib.sha256(_REQUIREMENTS.read_bytes()).hexdigest()[:8]
    cache_tag = sys.implementation.cache_tag or "py"
    parts = [version, req_sha8, cache_tag, sys.platform, platform.machine() or "unknown"]
    return _slug("-".join(parts))


def _manual_command(deps_dir: Path | None) -> str:
    if _REQUIREMENTS.exists() and deps_dir is not None:
        return (
            f'"{sys.executable}" -m pip install --target "{deps_dir}" '
            f'-r "{_REQUIREMENTS}"'
        )
    # Installed-wheel mode (no bundled requirements.txt): reinstall from the plugin
    # checkout. The package is not published on PyPI, so a bare distribution name
    # would install whatever someone registers under it.
    return f'"{sys.executable}" -m pip install "{_SCRIPTS_DIR.parent}"'


@contextlib.contextmanager
def _file_lock(lock_path: Path):
    """Cross-platform exclusive file lock (blocking)."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "w")
    try:
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    continue
        else:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(Exception):
            if os.name == "nt":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


def _validate_deps(target: Path) -> bool:
    """Confirm the sentinel dep imports AND actually resolves from ``target``.

    Done in a subprocess so a half-built dir never pollutes this process and so
    the check exercises the same ABI used at runtime. Crucially, ``PYTHONPATH``
    only *prepends* ``target`` — a bare ``import pydantic`` can still succeed from
    global/user site-packages when ``target`` is empty or partial. So we assert
    ``pydantic.__file__`` lives under ``target``; otherwise an empty dir would
    validate and we would write a poisoned ``.complete`` marker.
    """
    target = target.resolve()
    script = (
        "import sys, pathlib, pydantic\n"
        "f = pathlib.Path(pydantic.__file__).resolve()\n"
        "t = pathlib.Path(sys.argv[1]).resolve()\n"
        "sys.exit(0 if t in f.parents else 3)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script, str(target)],
        env={**os.environ, "PYTHONPATH": str(target)},
        capture_output=True,
    )
    return proc.returncode == 0


def _atomic_install(deps_dir: Path) -> None:
    """Install deps into ``deps_dir`` atomically. Raises on failure."""
    parent = deps_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = parent / f".tmp-{deps_dir.name}-{os.getpid()}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True)

    try:
        _eprint("[aidp-fusion-autopilot] first run — preparing dependencies (one-time)…")
        proc = subprocess.run(
            [
                sys.executable, "-m", "pip", "install",
                "--target", str(tmp_dir),
                "-r", str(_REQUIREMENTS),
            ],
        )
        if proc.returncode != 0:
            raise RuntimeError(f"pip install failed (rc={proc.returncode})")
        if not _validate_deps(tmp_dir):
            raise RuntimeError("dependency validation failed (sentinel import)")

        # Promote atomically. os.replace can't overwrite a non-empty dir, so
        # clear any pre-existing target first.
        if deps_dir.exists():
            shutil.rmtree(deps_dir, ignore_errors=True)
        os.replace(tmp_dir, deps_dir)
        # Marker written LAST so a crash before this point never looks complete.
        (deps_dir / ".complete").write_text(deps_dir.name, encoding="utf-8")
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _ensure_deps(deps_dir: Path) -> None:
    marker = deps_dir / ".complete"
    if marker.exists():
        return
    if _NO_AUTOINSTALL:
        return  # let the cli import fail and surface the manual command
    with _file_lock(deps_dir.parent / f".lock-{deps_dir.name}"):
        # Double-checked: another process may have completed while we waited.
        if marker.exists():
            return
        # Adopt an already-populated dir that lacks only the marker: either the
        # user ran the printed manual `pip install --target` fallback (which does
        # not write .complete), or a prior install promoted but crashed before
        # the marker. Validate first so we never adopt a partial dir.
        if deps_dir.exists() and _validate_deps(deps_dir):
            marker.write_text(deps_dir.name, encoding="utf-8")
            return
        _atomic_install(deps_dir)


def _activate_deps(deps_dir: Path) -> None:
    """Make ``deps_dir`` importable for this process AND child processes."""
    sys.path.insert(0, str(deps_dir))
    existing = os.environ.get("PYTHONPATH", "")
    prefix = os.pathsep.join([str(deps_dir), str(_SCRIPTS_DIR)])
    os.environ["PYTHONPATH"] = prefix + ((os.pathsep + existing) if existing else "")


def main() -> None:
    _require_min_python()

    from . import __version__  # dep-free (only sets __version__)

    # The lazy bootstrap is ONLY for run-from-source (plugin-cache) mode, where
    # scripts/requirements.txt ships alongside the package. In an installed wheel
    # the package is present WITHOUT that file and pip already installed the deps,
    # so we must not try to bootstrap (it would FileNotFoundError on the missing
    # requirements). Presence of requirements.txt is the signal for which mode.
    deps_dir: Path | None = None
    if _REQUIREMENTS.exists():
        deps_dir = _deps_home() / "pylib" / _deps_key(__version__)
        try:
            _ensure_deps(deps_dir)
        except Exception as exc:  # install failed — fail soft with manual command
            _eprint(f"[aidp-fusion-autopilot] dependency install failed: {exc}")
            _eprint(f"[aidp-fusion-autopilot] run manually: {_manual_command(deps_dir)}")
            raise SystemExit(1)
        _activate_deps(deps_dir)

    try:
        from .cli import main as cli_main
    except ModuleNotFoundError as exc:
        _eprint(f"[aidp-fusion-autopilot] missing dependency: {exc.name}")
        _eprint(f"[aidp-fusion-autopilot] run manually: {_manual_command(deps_dir)}")
        raise SystemExit(1)

    cli_main(prog_name="aidp-fusion-autopilot")


if __name__ == "__main__":
    main()
