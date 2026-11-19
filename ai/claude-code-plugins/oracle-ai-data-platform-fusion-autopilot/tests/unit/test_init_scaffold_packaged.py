"""`init` scaffold must ship inside the package, not only in the repo.

Regression for the customer-found bug: ``pip install`` (non-editable) built a
wheel WITHOUT the repo-root ``examples/`` tree, so ``aidp-fusion-autopilot init``
— the first quickstart command — crashed with ``FileNotFoundError: examples
directory not found`` for every real customer. The editable-install test
suite never caught it because ``examples/`` is present on disk in a dev
checkout.

This is the fast, always-on guard:

* :class:`TestScaffoldShipsAndMatchesRepo` — every template ``init`` references
  resolves from package data (``_scaffold/``) AND is byte-identical to its repo
  ``examples/`` source, so the bundled copy can't silently drift.

The end-to-end customer path (build a real wheel, install it clean, run
``init`` for every template) lives in
``tests/integration/test_installed_wheel.py`` — it is slow and network-bound
(``pip install`` pulls the full dependency tree), so it is opt-in via
``AIDP_FUSION_AUTOPILOT_RUN_WHEEL_TEST=1`` like the other wheel-install tests.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.init import TEMPLATES

# Plugin root = two parents up from this test file
# (.../tests/unit/test_init_scaffold_packaged.py -> plugin root, where
# examples/ and .env.example live).
REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"


def _scaffold_relpaths() -> list[str]:
    """Every scaffold file ``init`` can copy: the two files per template plus
    the ``.env.example`` it always scaffolds."""
    rels: set[str] = {".env.example"}
    for bundle_src, config_src in TEMPLATES.values():
        rels.add(bundle_src)
        rels.add(config_src)
    return sorted(rels)


def _repo_source(relpath: str) -> Path:
    """The repo-side source of a scaffold file (the dev-fallback location)."""
    if relpath == ".env.example":
        return REPO_ROOT / ".env.example"
    return EXAMPLES / relpath


class TestScaffoldShipsAndMatchesRepo:
    @pytest.mark.parametrize("relpath", _scaffold_relpaths())
    def test_template_is_packaged(self, relpath: str) -> None:
        """The file is present in package data under ``_scaffold/`` — i.e. it
        ships in the wheel, so ``init`` works for a ``pip install`` customer."""
        res = resources.files(
            "oracle_ai_data_platform_fusion_autopilot"
        ).joinpath("_scaffold", relpath)
        assert res.is_file(), (
            f"{relpath!r} is referenced by init's TEMPLATES but is NOT shipped "
            f"in package data (_scaffold/). A pip-installed customer's `init` "
            f"would crash. Add it under _scaffold/ and to pyproject.toml's "
            f"[tool.setuptools.package-data]."
        )

    @pytest.mark.parametrize("relpath", _scaffold_relpaths())
    def test_packaged_matches_repo_source(self, relpath: str) -> None:
        """The packaged copy is byte-identical to the repo ``examples/`` /
        ``.env.example`` source, so the two can't drift unnoticed."""
        repo_src = _repo_source(relpath)
        assert repo_src.is_file(), (
            f"repo source {repo_src} for scaffold {relpath!r} is missing"
        )
        packaged = resources.files(
            "oracle_ai_data_platform_fusion_autopilot"
        ).joinpath("_scaffold", relpath)
        assert packaged.read_bytes() == repo_src.read_bytes(), (
            f"packaged _scaffold/{relpath} has drifted from examples/{relpath}. "
            f"Re-sync the _scaffold copy when you edit the repo example."
        )
