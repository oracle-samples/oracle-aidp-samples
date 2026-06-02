"""Smoke test: no broken internal markdown links in live docs.

Walks ``docs/*.md`` (non-recursive — feature plans / idea
sketches under ``docs/features/**`` are explicitly out of scope
because they contain illustrative, not-yet-real links per the
plan's Locked Decision 2) plus the top-level ``README.md``.

Extracts every inline markdown link via regex; for each
non-external target (anything not starting with ``http://``,
``https://``, ``mailto:``, or ``#``), resolves the path
relative to the file's directory and asserts it exists.

Caveats accepted at plan time:

- Reference-style links ``[text][ref]`` and autolinks
  ``<https://...>`` are silently missed (rare in this repo).
- URLs containing balanced parens (``[t](foo_(bar).md)``) are
  mis-captured by the simple regex; verified at plan time that
  no such targets exist in the live docs today.
- Image links ``![alt](src)`` extract the same way and
  resolve under the same path-relative-to-file rule —
  intentional; broken image refs get caught too.

The test scope is INTERNAL links only — external URLs aren't
validated (would require network and is brittle).
"""
from __future__ import annotations

import re
from pathlib import Path

# Inline markdown link: ``[text](target)`` or ``[text](target "title")``.
# ``target`` ends at whitespace or the closing paren.
_LINK_RE = re.compile(
    r"\[(?:[^\]]+)\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)"
)


def _is_external_or_anchor(target: str) -> bool:
    return target.startswith(("http://", "https://", "mailto:", "#"))


def test_no_broken_internal_markdown_links():
    repo_root = Path(__file__).resolve().parent.parent

    # Live docs only: non-recursive glob over docs/, plus README.
    md_files = sorted((repo_root / "docs").glob("*.md"))
    md_files.append(repo_root / "README.md")

    broken: list[str] = []
    for md in md_files:
        text = md.read_text(encoding="utf-8")
        for m in _LINK_RE.finditer(text):
            target = m.group("target")
            if _is_external_or_anchor(target):
                continue
            # Strip in-target anchor (e.g. ``foo.md#section``).
            target_path = target.split("#", 1)[0]
            if not target_path:
                continue
            resolved = (md.parent / target_path).resolve()
            if not resolved.exists():
                broken.append(
                    f"{md.relative_to(repo_root)}: [...]({target}) "
                    f"→ {resolved}"
                )

    assert not broken, (
        "broken internal markdown links:\n  " + "\n  ".join(broken)
    )
