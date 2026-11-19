"""Installed-wheel smoke test.

Builds a wheel via `python -m build`, installs it into a fresh venv, and
exercises the v2 content-pack CLI verbs to confirm:

* The starter pack ships inside the wheel (per pyproject.toml's
  `tool.setuptools.package-data`).
* `pack.schema.json`, `dashboard.schema.json`, `node.schema.json` ship too.
* `aidp-fusion-autopilot content-pack list` finds the starter pack.
* `aidp-fusion-autopilot content-pack validate fusion-finance-starter` passes
  end-to-end (full validation pipeline).

Gated opt-in via env var ``AIDP_FUSION_AUTOPILOT_RUN_WHEEL_TEST=1`` because it
is slower than unit tests (~30 seconds, builds + creates a venv).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).resolve().parent.parent.parent

pytestmark = pytest.mark.skipif(
    os.environ.get("AIDP_FUSION_AUTOPILOT_RUN_WHEEL_TEST") != "1",
    reason="Set AIDP_FUSION_AUTOPILOT_RUN_WHEEL_TEST=1 to run (slow: builds wheel + venv).",
)


@pytest.fixture(scope="module")
def installed_venv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the wheel + install in a fresh venv. Returns the venv path."""
    build_dir = tmp_path_factory.mktemp("wheel-build")
    venv_dir = tmp_path_factory.mktemp("wheel-venv")

    # Build the wheel.
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(build_dir),
            str(PLUGIN_ROOT),
        ],
        check=True,
        capture_output=True,
    )

    # Find the built wheel.
    wheels = list(build_dir.glob("oracle_ai_data_platform_fusion_autopilot-*.whl"))
    assert len(wheels) == 1, f"expected one wheel, found: {wheels}"

    # Create venv + install.
    subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
    venv_python = venv_dir / "bin" / "python"
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "--quiet", str(wheels[0])],
        check=True,
        capture_output=True,
    )
    return venv_dir


def _run_in_venv(venv_dir: Path, *args: str) -> subprocess.CompletedProcess[str]:
    cli_path = venv_dir / "bin" / "aidp-fusion-autopilot"
    return subprocess.run([str(cli_path), *args], capture_output=True, text=True)


def test_wheel_ships_starter_pack(installed_venv: Path) -> None:
    """The installed wheel includes the starter pack files."""
    python = installed_venv / "bin" / "python"
    result = subprocess.run(
        [
            str(python),
            "-c",
            "import oracle_ai_data_platform_fusion_autopilot as p; "
            "from pathlib import Path; "
            "root = Path(p.__file__).parent / 'content_packs' / 'fusion-finance-starter'; "
            "print(root.exists(), (root / 'pack.yaml').exists())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "True True" in result.stdout, result.stdout


def test_wheel_ships_schema_artifacts(installed_venv: Path) -> None:
    """The installed wheel includes pack/node/dashboard.schema.json."""
    python = installed_venv / "bin" / "python"
    result = subprocess.run(
        [
            str(python),
            "-c",
            "import oracle_ai_data_platform_fusion_autopilot as p; "
            "from pathlib import Path; "
            "d = Path(p.__file__).parent; "
            "print((d / 'pack.schema.json').exists(), "
            "(d / 'node.schema.json').exists(), "
            "(d / 'dashboard.schema.json').exists())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "True True True" in result.stdout, result.stdout


def test_wheel_cli_content_pack_list(installed_venv: Path) -> None:
    result = _run_in_venv(installed_venv, "content-pack", "list", "--json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    names = {p["name"] for p in data["packs"]}
    assert "fusion-finance-starter" in names


def test_wheel_cli_content_pack_validate(installed_venv: Path) -> None:
    result = _run_in_venv(installed_venv, "content-pack", "validate", "fusion-finance-starter")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "validates clean" in result.stdout


def test_wheel_cli_content_pack_info(installed_venv: Path) -> None:
    result = _run_in_venv(
        installed_venv, "content-pack", "info", "fusion-finance-starter", "--json"
    )
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["id"] == "fusion-finance-starter"
    assert set(data["nodes"]["silver"]) == {"dim_supplier", "dim_account", "dim_calendar"}
