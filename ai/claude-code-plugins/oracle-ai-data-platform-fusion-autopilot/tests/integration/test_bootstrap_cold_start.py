"""Real cold-start integration test for the self-bootstrapping entry point.

Unlike tests/unit/test_bootstrap_main.py (which mocks pip), this exercises the
ACTUAL path: `sys.executable -m pip install --target <key> -r scripts/requirements.txt`,
the `.complete` marker, and the second-run fast path. It downloads from PyPI, so
it is gated behind an env flag and skipped by default.

Run it with:  AIDP_FUSION_COLD_START=1 pytest tests/integration/test_bootstrap_cold_start.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("AIDP_FUSION_COLD_START") != "1",
    reason="real cold-start install hits PyPI; set AIDP_FUSION_COLD_START=1 to run",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"
PREPARING = "preparing dependencies"


def _invoke(home: Path):
    env = {
        **os.environ,
        "PYTHONPATH": str(SCRIPTS),
        "AIDP_FUSION_HOME": str(home),
    }
    env.pop("AIDP_FUSION_NO_AUTOINSTALL", None)
    return subprocess.run(
        [sys.executable, "-m", "oracle_ai_data_platform_fusion_autopilot", "--version"],
        env=env, capture_output=True, text=True,
    )


def test_cold_start_installs_then_fast_path(tmp_path):
    home = tmp_path / "home"

    # 1. Cold start: installs deps, prints the one-time preparing message, runs.
    first = _invoke(home)
    assert first.returncode == 0, first.stderr
    assert "0.1.0a0" in first.stdout
    assert PREPARING in first.stderr  # the install actually ran

    # 2. The keyed dir was populated with REAL deps + the marker (written last).
    keyed = list((home / "pylib").glob("0.1.0a0-*"))
    assert len(keyed) == 1, f"expected one keyed dir, got {keyed}"
    deps_dir = keyed[0]
    assert (deps_dir / ".complete").is_file()
    assert (deps_dir / "pydantic").is_dir()  # not just a stub
    assert (deps_dir / "oci").is_dir()
    assert (deps_dir / "build").is_dir()  # the runtime build dep is present

    # 3. Second run: fast path — no reinstall (no preparing message), still works.
    second = _invoke(home)
    assert second.returncode == 0, second.stderr
    assert "0.1.0a0" in second.stdout
    assert PREPARING not in second.stderr


def test_cold_start_child_process_sees_build(tmp_path):
    """After bootstrap, a `python -m build` child must resolve `build`.

    Proves the os.environ["PYTHONPATH"] propagation end-to-end against a real
    install (the unit test uses a dummy module; this uses the actual dep).
    """
    home = tmp_path / "home"
    assert _invoke(home).returncode == 0
    deps_dir = next((home / "pylib").glob("0.1.0a0-*"))

    # Simulate what the CLI does: deps dir on PYTHONPATH, then a child `-m build`.
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(deps_dir), str(SCRIPTS)])}
    child = subprocess.run(
        [sys.executable, "-m", "build", "--version"],
        env=env, capture_output=True, text=True,
    )
    assert child.returncode == 0, child.stderr
    assert "build" in (child.stdout + child.stderr).lower()
