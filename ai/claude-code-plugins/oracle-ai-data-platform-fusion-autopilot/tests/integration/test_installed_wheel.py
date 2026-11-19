"""Installed-wheel smoke test.

Builds a wheel via `python -m build`, installs it into a fresh venv, and
exercises the v2 content-pack CLI verbs to confirm:

* The starter pack ships inside the wheel (per pyproject.toml's
  `tool.setuptools.package-data`).
* `pack.schema.json`, `dashboard.schema.json`, `node.schema.json` ship too.
* `aidp-fusion-autopilot content-pack list` finds the starter pack.
* `aidp-fusion-autopilot content-pack validate fusion-finance-starter` passes
  end-to-end (full validation pipeline).
* The `init` scaffold ships inside the wheel and `init` works for EVERY
  template from a clean `pip install` — the customer-found regression
  (``FileNotFoundError: examples directory not found``). The drift/packaging
  unit guard lives in ``tests/unit/test_init_scaffold_packaged.py``; this is
  the slow end-to-end half that actually builds + installs the wheel.

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

from oracle_ai_data_platform_fusion_autopilot.commands.init import TEMPLATES

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


@pytest.mark.parametrize("template", sorted(TEMPLATES))
def test_wheel_init_scaffolds_every_template(
    installed_venv: Path, tmp_path: Path, template: str
) -> None:
    """``init`` works for EVERY template from a clean ``pip install``.

    Regression for the customer-found bug: the repo-root ``examples/`` tree was
    not packaged, so ``init`` crashed with
    ``FileNotFoundError: examples directory not found``.

    Parametrizing over all templates is deliberate: ``minimal`` and
    ``full-finance`` resolve the TOP-LEVEL ``_scaffold/*.yaml`` files
    (``aidp.config.example.yaml``, ``full_finance.yaml``,
    ``minimal_gl_only.yaml``), which a nested-only ``_scaffold/**/*.yaml``
    package-data glob silently drops on some setuptools versions. The default
    template alone (``full-finance-starter``, nested) would not catch that.
    """
    cli_path = installed_venv / "bin" / "aidp-fusion-autopilot"
    proj = tmp_path / template
    proj.mkdir()
    result = subprocess.run(
        [str(cli_path), "init", "--template", template],
        cwd=str(proj),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"`init --template {template}` failed for a pip-installed customer:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert (proj / "bundle.yaml").is_file()
    assert (proj / "aidp.config.yaml").is_file()
