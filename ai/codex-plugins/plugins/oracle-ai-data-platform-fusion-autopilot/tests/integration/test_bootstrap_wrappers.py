"""Integration tests for the POSIX bin/ wrapper (interpreter gating + PATH).

These use *stub* interpreters so they are fast and need no real dependency
install: each stub answers the wrapper's `-c` version probe with a fixed exit
code and, when invoked with `-m`, prints a marker instead of running the package.
That lets us observe exactly which interpreter the wrapper selected.

Windows (.cmd) resolution is not exercised here — see the plan's Testing
Strategy (treated as unverified until run on a real Windows host).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

if os.name == "nt":  # POSIX wrapper only
    pytest.skip("POSIX wrapper tests", allow_module_level=True)

REPO_ROOT = Path(__file__).resolve().parents[2]
WRAPPER = REPO_ROOT / "bin" / "aidp-fusion-autopilot"


def _make_stub(directory: Path, name: str, probe_rc: int, marker: str) -> None:
    """Write a fake interpreter.

    `<name> -c <code>` exits `probe_rc` (simulates the >=3.10 version probe).
    `<name> -m <module> ...` prints `marker` and exits 0 (simulates running).
    """
    directory.mkdir(parents=True, exist_ok=True)
    stub = directory / name
    stub.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "-c" ]; then exit %d; fi\n'
        'echo "%s"\n'
        "exit 0\n" % (probe_rc, marker)
    )
    stub.chmod(0o755)


# All six candidate names the POSIX wrapper probes, in order.
_CANDIDATES = ["python3", "python", "python3.13", "python3.12", "python3.11", "python3.10"]


def _run_wrapper(path_dirs, env_extra=None):
    env = {**os.environ, "PATH": os.pathsep.join(str(d) for d in path_dirs)}
    env.setdefault("AIDP_FUSION_NO_AUTOINSTALL", "1")
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [str(WRAPPER), "--version"],
        env=env,
        capture_output=True,
        text=True,
    )


def test_wrapper_rejects_when_no_interpreter_is_310(tmp_path):
    # Every candidate present but all report <3.10 -> wrapper must refuse.
    stubdir = tmp_path / "stubs"
    for name in _CANDIDATES:
        _make_stub(stubdir, name, probe_rc=1, marker="SHOULD-NOT-RUN")
    proc = _run_wrapper([stubdir, "/usr/bin", "/bin"])
    assert proc.returncode == 1
    assert "needs Python >= 3.10" in proc.stderr
    assert "SHOULD-NOT-RUN" not in proc.stdout  # never exec'd the code


def test_wrapper_skips_old_python_and_picks_a_newer_one(tmp_path):
    # python3 reports 3.9 (reject); python3.13 reports >=3.10 (select).
    # Also stub `python` as rejecting so a real /usr/bin/python >=3.10 can't be
    # selected ahead of python3.13 and make this test env-dependent.
    stubdir = tmp_path / "stubs"
    _make_stub(stubdir, "python3", probe_rc=1, marker="OLD-39")
    _make_stub(stubdir, "python", probe_rc=1, marker="OLD-PY")
    _make_stub(stubdir, "python3.13", probe_rc=0, marker="NEW-313")
    proc = _run_wrapper([stubdir, "/usr/bin", "/bin"])
    assert proc.returncode == 0, proc.stderr
    assert "NEW-313" in proc.stdout  # selected the >=3.10 interpreter
    assert "OLD-39" not in proc.stdout  # skipped the 3.9 one


def test_fresh_path_resolution_via_bin_dir(tmp_path):
    # bin/ on PATH, NO pip script dir, NO real python on PATH (stub only).
    # `aidp-fusion-autopilot` must resolve and run with no manual PATH edit.
    stubdir = tmp_path / "stubs"
    _make_stub(stubdir, "python3", probe_rc=0, marker="RAN")
    path_dirs = [REPO_ROOT / "bin", stubdir, "/usr/bin", "/bin"]
    env = {**os.environ, "PATH": os.pathsep.join(str(d) for d in path_dirs)}

    # `command` is a shell builtin; invoke through sh to resolve on PATH.
    which = subprocess.run(
        "command -v aidp-fusion-autopilot",
        env=env, capture_output=True, text=True, shell=True,
    )
    assert which.returncode == 0, "aidp-fusion-autopilot did not resolve on PATH"
    assert str(REPO_ROOT / "bin") in which.stdout

    ran = subprocess.run(
        "aidp-fusion-autopilot --version",
        env={**env, "AIDP_FUSION_NO_AUTOINSTALL": "1"},
        capture_output=True, text=True, shell=True,
    )
    assert ran.returncode == 0, ran.stderr
    assert "RAN" in ran.stdout
