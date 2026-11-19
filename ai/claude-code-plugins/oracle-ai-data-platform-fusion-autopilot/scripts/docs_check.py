"""Lightweight documentation validation.

Checks:
1. Relative Markdown links in user-facing docs resolve on disk.
2. Every ``AIDPF-####`` code referenced in the repository is documented in
   ``docs/aidpf-error-codes.md``.

The script intentionally does not call the network or validate external URLs.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]

DOC_SOURCES = [
    ROOT / "README.md",
    ROOT / "workflow.md",
    ROOT / "docs",
    ROOT / "examples",
]

SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "node_modules",
}

LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
AIDPF_RE = re.compile(r"AIDPF-\d{4}")


def main() -> int:
    errors: list[str] = []
    errors.extend(check_markdown_links())
    errors.extend(check_aidpf_codes())

    if errors:
        print("docs-check failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("docs-check passed.")
    return 0


def markdown_files() -> list[Path]:
    files: list[Path] = []
    for source in DOC_SOURCES:
        if source.is_file() and source.suffix == ".md":
            files.append(source)
        elif source.is_dir():
            files.extend(source.rglob("*.md"))
    return sorted({p.resolve() for p in files})


def check_markdown_links() -> list[str]:
    errors: list[str] = []
    for md_file in markdown_files():
        text = md_file.read_text(encoding="utf-8")
        for match in LINK_RE.finditer(text):
            raw_target = match.group(1).strip()
            target = normalize_markdown_target(raw_target)
            if target is None:
                continue

            resolved = (md_file.parent / target).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                errors.append(
                    f"{rel(md_file)} links outside repository: {raw_target}"
                )
                continue

            if not resolved.exists():
                errors.append(
                    f"{rel(md_file)} has missing link target: {raw_target}"
                )
    return errors


def normalize_markdown_target(raw_target: str) -> Path | None:
    if not raw_target:
        return None

    target = raw_target
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1].strip()

    lowered = target.lower()
    if (
        lowered.startswith("http://")
        or lowered.startswith("https://")
        or lowered.startswith("mailto:")
        or lowered.startswith("app://")
        or lowered.startswith("codex://")
    ):
        return None

    if target.startswith("#"):
        return None

    target = target.split("#", 1)[0].split("?", 1)[0]
    target = unquote(target).strip()
    if not target:
        return None
    return Path(target)


def check_aidpf_codes() -> list[str]:
    registry = ROOT / "docs" / "aidpf-error-codes.md"
    if not registry.exists():
        return ["docs/aidpf-error-codes.md is missing"]

    documented = set(AIDPF_RE.findall(registry.read_text(encoding="utf-8")))
    referenced: set[str] = set()

    for path in repo_files():
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        referenced.update(AIDPF_RE.findall(text))

    missing = sorted(referenced - documented)
    if missing:
        return [
            "AIDPF codes referenced but not documented: " + ", ".join(missing)
        ]
    return []


def repo_files() -> list[Path]:
    files: list[Path] = []
    for path in ROOT.rglob("*"):
        if any(part in SKIP_DIRS for part in path.relative_to(ROOT).parts):
            continue
        if path.is_file():
            files.append(path)
    return files


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


if __name__ == "__main__":
    sys.exit(main())
