"""Installed-wheel mode: `python -m oracle_ai_data_platform_fusion_autopilot` must work.

A pip-installed wheel contains the package (`__main__.py` included) but NOT the
sibling `scripts/requirements.txt`, and pip has already installed the deps. The
entry point must detect that mode and run the CLI directly instead of trying to
bootstrap (which would FileNotFoundError on the missing requirements file).

We simulate the wheel layout by copying just the package dir into a temp dir (so
there is no sibling requirements.txt) and running with the current interpreter.
That only models a wheel install when the interpreter *already has the deps* (as
pip would after a real wheel install), so this whole module skips when the
runtime deps are not importable here. The dep-free bypass logic is covered
unconditionally by tests/unit/test_bootstrap_main.py
(``test_main_bypasses_bootstrap_in_wheel_mode``).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

# Only meaningful when the interpreter has the runtime deps (the wheel premise).
for _mod in ("click", "pydantic", "dotenv", "rich"):
    pytest.importorskip(_mod, reason="installed-wheel simulation needs runtime deps present")

REPO_ROOT = Path(__file__).resolve().parents[2]
PKG = REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"


def test_installed_wheel_mode_runs_without_requirements_file(tmp_path):
    site = tmp_path / "site"
    site.mkdir()
    # Copy ONLY the package (real files, not a symlink, so .resolve() stays here
    # and the sibling requirements.txt is absent — i.e. wheel mode).
    shutil.copytree(
        PKG, site / "oracle_ai_data_platform_fusion_autopilot",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    assert not (site / "requirements.txt").exists()

    home = tmp_path / "home"
    env = {**os.environ, "PYTHONPATH": str(site), "AIDP_FUSION_HOME": str(home)}
    env.pop("AIDP_FUSION_NO_AUTOINSTALL", None)

    proc = subprocess.run(
        [sys.executable, "-m", "oracle_ai_data_platform_fusion_autopilot", "--version"],
        env=env, capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "0.1.0a0" in proc.stdout
    # No bootstrap should have run (deps came from the interpreter, not pylib).
    assert not (home / "pylib").exists()
