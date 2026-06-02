---
shipped: 2026-05-10
---

# Shipped: docs-lint-ci

## Summary

Pytest smoke test that walks `docs/*.md` (non-recursive) +
`README.md`, extracts every inline markdown link, and asserts
internal targets resolve. ~70 LOC pure-stdlib. Catches stale
cross-references on every test run — the kind that accumulate
silently when files get renamed or moved.

## Key Changes

- **`tests/test_docs_links.py`** (new): `_LINK_RE` extracts
  inline markdown links, skips external URLs + anchors,
  resolves internal targets relative to the file directory,
  collects broken targets and asserts the list is empty.
  Documented caveats in the docstring (reference-style links
  + autolinks + balanced-paren URLs are silently missed —
  rare in this repo's live docs and verified at plan time).
- **`docs/programmatic_api.md:474`**: stale link
  `../tests/manual/dashboard_charts.ipynb` → updated to
  `../tests/manual/local/dashboard_charts.ipynb` (the
  notebooks were moved to `tests/manual/local/` in an
  earlier reorg). The test caught it on first run.
- **`docs/CHANGELOG.md`**: Tests / CI entry.

## Files Changed

4 files; +132 / −1 lines.

## Plan PR

[#28](https://github.com/amitranjan-oracle/qualifire/pull/28).

## Review Cycles

Plan: 3 codex rounds.
- R1: BLOCKER (test would fail on current docs) + 2 MAJORs
  (regex edge cases; feature-plan files contain illustrative
  links).
- R2: MINOR — Goal section conflicted with scope description.
- R3 PASS.

Implementation: 1 codex round → PASS (clean).

## Local Test Results

- 1518 passed, 2 skipped (+1 new test).
- One broken link surfaced + fixed in the same PR (per Locked
  Decision 6).
