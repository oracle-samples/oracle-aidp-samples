"""Parity guard: scripts/requirements.txt must mirror pyproject [project.dependencies].

The CLI is provisioned from source (see plugin-install-hook feature): the lazy
bootstrap in ``__main__.py`` installs ``scripts/requirements.txt`` with
``pip install --target`` and also hashes that file to key the deps dir. If the
requirements file drifts from ``pyproject.toml``, a normal ``pip install`` of the
package and the bootstrap install would diverge. This test fails closed on drift.
"""

from __future__ import annotations

import sys
from pathlib import Path

if sys.version_info < (3, 11):  # pragma: no cover - tomllib is stdlib from 3.11
    import pytest

    pytest.skip(
        "tomllib requires Python 3.11+; parity check runs in CI on a newer interpreter",
        allow_module_level=True,
    )

import tomllib  # noqa: E402 - guarded above for <3.11

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"
REQUIREMENTS = REPO_ROOT / "scripts" / "requirements.txt"


def _normalize(spec: str) -> str:
    """Canonicalize a requirement spec for comparison (strip whitespace)."""
    return spec.replace(" ", "").strip()


def _pyproject_deps() -> set[str]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    return {_normalize(d) for d in data["project"]["dependencies"]}


def _requirements_deps() -> set[str]:
    out: set[str] = set()
    for raw in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        out.add(_normalize(line))
    return out


def test_requirements_file_exists() -> None:
    assert REQUIREMENTS.is_file(), f"missing {REQUIREMENTS}"


def test_requirements_match_pyproject() -> None:
    pyproject = _pyproject_deps()
    requirements = _requirements_deps()
    missing_from_requirements = pyproject - requirements
    extra_in_requirements = requirements - pyproject
    assert not missing_from_requirements and not extra_in_requirements, (
        "scripts/requirements.txt has drifted from pyproject.toml "
        "[project.dependencies].\n"
        f"  in pyproject but not requirements.txt: {sorted(missing_from_requirements)}\n"
        f"  in requirements.txt but not pyproject: {sorted(extra_in_requirements)}"
    )
