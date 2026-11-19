#!/usr/bin/env python3
"""Sync shared Spark connector skills into Claude Code and Codex plugins."""

from __future__ import annotations

import filecmp
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SHARED_SKILLS = ROOT / "ai/shared-plugin-content/oracle-ai-data-platform-workbench-spark-connectors/skills"
TARGETS = [
    ROOT / "ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors/skills",
    ROOT / "ai/codex-plugins/plugins/oracle-ai-data-platform-workbench-spark-connectors/skills",
]


def copy_tree(src: Path, dst: Path) -> None:
    if not src.is_dir():
        raise SystemExit(f"missing shared skills directory: {src}")
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def assert_same(left: Path, right: Path) -> None:
    comparison = filecmp.dircmp(left, right)
    problems = comparison.left_only + comparison.right_only + comparison.diff_files + comparison.funny_files
    if problems:
        raise SystemExit(f"skill sync verification failed for {right}: {problems}")
    for name in comparison.common_dirs:
        assert_same(left / name, right / name)


def main() -> None:
    for target in TARGETS:
        copy_tree(SHARED_SKILLS, target)
        assert_same(SHARED_SKILLS, target)
        print(f"synced {target.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
