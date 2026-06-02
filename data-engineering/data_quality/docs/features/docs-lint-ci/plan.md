# Plan: docs-lint-ci

## Goal

Pytest-runnable smoke test that walks `docs/*.md`
(non-recursive — live docs only; explicitly excludes
`docs/features/**` per Locked Decision 2) and `README.md`,
extracts every markdown link with a non-URL target, and asserts
the path resolves. Catches stale cross-references on PR review
(the kind that accumulate when files get renamed / moved).

## Locked Decisions

1. **Pure-stdlib implementation.** No dev-dep additions; just
   `re` for markdown-link extraction + `pathlib.Path.exists()`.
2. **Scope: internal links only, in LIVE docs only.**
   - External URLs (`http://`, `https://`, `mailto:`) are NOT
     validated — that requires network and is brittle.
   - In-file anchors (`#section`) are skipped.
   - **Files scanned**: `docs/*.md` (non-recursive) +
     `README.md`. **Excluded**: `docs/features/**` (codex R1
     MAJOR — those files are feature plans / idea sketches
     that contain illustrative, not-yet-real links).
3. **Single test file**: `tests/test_docs_links.py`.
4. **Auto-skip when target is a directory** —
   `[link](features/foo/)` is a valid reference pattern.
5. **Regex caveats** (codex R1 MAJOR — accepted):
   - Reference-style `[text][ref]` and autolinks
     `<https://...>` are silently missed. They're rare in
     this repo's live docs.
   - URLs containing balanced parens
     (`[text](foo_(bar).md)`) are mis-captured. We assert
     no such target exists in the current live docs (verified
     at plan time); future contributors get caught by review
     if they add one.
   - Image links `![alt](src)` extract the same way and
     resolve under the same path-relative-to-file rule —
     intentional; the test catches broken image refs too.
6. **Fix any broken links discovered on first run** as part of
   this PR (codex R1 BLOCKER — at least one broken link exists
   in `docs/programmatic_api.md` today).

## What Changes

### `tests/test_docs_links.py` (new)

```python
import re
from pathlib import Path

# Markdown link: [text](target) where target is not http://, https://,
# mailto:, or anchor (#).
_LINK_RE = re.compile(
    r"\[(?:[^\]]+)\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)",
)


def _is_external(target: str) -> bool:
    return target.startswith(("http://", "https://", "mailto:", "#"))


def test_no_broken_internal_markdown_links():
    repo_root = Path(__file__).resolve().parent.parent
    # Live docs only — feature plans / idea sketches under
    # docs/features/ are explicitly out of scope (Locked
    # Decision 2). Glob non-recursively to avoid them.
    md_files = list((repo_root / "docs").glob("*.md"))
    md_files.append(repo_root / "README.md")

    broken = []
    for md in md_files:
        text = md.read_text(encoding="utf-8")
        for m in _LINK_RE.finditer(text):
            target = m.group("target")
            if _is_external(target):
                continue
            # Strip in-target anchor (e.g. `foo.md#section`).
            target_path = target.split("#", 1)[0] or ""
            if not target_path:
                continue
            resolved = (md.parent / target_path).resolve()
            if not resolved.exists():
                broken.append(f"{md.relative_to(repo_root)}: {target}")

    assert not broken, "broken internal markdown links:\n  " + "\n  ".join(broken)
```

## Acceptance Criteria

- AC1: `tests/test_docs_links.py::test_no_broken_internal_markdown_links` exists and runs as part of `pytest tests/`.
- AC2: The test passes against the current state of `docs/` +
  `README.md` (no broken links today). If it doesn't, fix the
  broken links in the same PR before merging.
- AC3: External URLs and in-file anchors are skipped (the test
  doesn't fail on them).
- AC4: A regression added INSIDE this PR — a deliberately broken
  link added to a temp markdown file in the test, then removed
  before merge — confirms the test catches the failure mode.
  (We achieve this implicitly by deleting + verifying first.)

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Broken links exist today | If the test fails on first run, fix the links as part of this PR. |
| Markdown link regex misses edge cases (reference-style links, autolinks) | Acceptable — the test covers the dominant inline form which is what file-rename-rot affects. Reference-style links are rare in this repo. |
| Future file rename breaks links — but the test runs on the rename's PR | That's exactly the point. |

## Out-of-Band Reviews

- 1 adversarial plan review.
- 1 codex plan review → PASS.
- Then implement; 1 codex impl review.

## Effort

Small. ~30 LOC of Python.

## Plan Iteration Log

- v1: initial draft.
- v2: addressed codex plan-review round 1 — BLOCKER + 2
  MAJORs: scope narrowed to live docs (`docs/*.md` non-
  recursive + `README.md`); explicit exclusions documented;
  regex caveats spelled out; commitment to fix any broken
  links surfaced on first run.
