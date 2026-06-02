"""Hygiene checks for the docs/features tree (findings-sweep P5.2).

The dashboard auto-gen hook lives outside this repo (in the
``schuettc-claude-code-plugins/feature-workflow`` plugin) and currently
scans the ``docs/features/`` tree directly to decide which features are
"shipped." It does not filter by ``git ls-files``, so a contributor who
authors a ``shipped.md`` locally but forgets to commit it will see the
generated ``DASHBOARD.md`` claim the feature is shipped while the
checked-in source disagrees.

We can't unilaterally patch the upstream skill from inside a project
PR, but we *can* fail the test suite if a contributor leaves an
untracked ``shipped.md`` lying around — that's a deterministic local
guard against the same failure mode. See
``docs/features/findings-sweep/plan.md`` §P5.2 for context.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
FEATURES_DIR = REPO_ROOT / "docs" / "features"


def _git_tracked_files(root: Path) -> set[Path]:
    """Return the set of files tracked by git, as repo-relative paths."""
    try:
        out = subprocess.check_output(
            ["git", "-C", str(root), "ls-files"],
            text=True,
            stderr=subprocess.STDOUT,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        pytest.skip(f"git ls-files unavailable, skipping hygiene check: {exc}")
    return {Path(line.strip()) for line in out.splitlines() if line.strip()}


def test_no_untracked_shipped_md_files() -> None:
    """Every ``docs/features/<id>/shipped.md`` on disk must be git-tracked.

    Untracked ``shipped.md`` files lie to the dashboard generator and
    misrepresent shipped state to anyone running the workflow locally.
    Either commit the file or delete it.
    """
    if not FEATURES_DIR.exists():
        pytest.skip("docs/features/ does not exist; nothing to verify")

    on_disk = {
        p.relative_to(REPO_ROOT)
        for p in FEATURES_DIR.glob("*/shipped.md")
    }
    if not on_disk:
        return  # no shipped.md files at all → trivially passes

    tracked = _git_tracked_files(REPO_ROOT)
    untracked = sorted(str(p) for p in on_disk if p not in tracked)
    assert not untracked, (
        "Untracked docs/features/<id>/shipped.md files would mislead the "
        "dashboard generator into reporting an unshipped feature as shipped. "
        f"Either commit or delete: {untracked}"
    )


def test_dashboard_md_is_gitignored_or_tracked() -> None:
    """``DASHBOARD.md`` must be either tracked or explicitly gitignored.

    A floating untracked DASHBOARD.md is the same shape of failure as a
    floating ``shipped.md`` — it advertises a state to anyone running
    locally that doesn't match the checked-in source.
    """
    dash = FEATURES_DIR / "DASHBOARD.md"
    if not dash.exists():
        pytest.skip("docs/features/DASHBOARD.md does not exist locally")

    tracked = _git_tracked_files(REPO_ROOT)
    if dash.relative_to(REPO_ROOT) in tracked:
        return  # tracked is fine

    # Not tracked — must be explicitly gitignored.
    try:
        subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "check-ignore", str(dash)],
            stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError:
        pytest.fail(
            "docs/features/DASHBOARD.md is neither tracked nor gitignored. "
            "Add it to .gitignore (auto-generated artifact) or commit it."
        )
