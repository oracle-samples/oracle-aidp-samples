---
id: docs-lint-ci
name: Docs — Broken Internal Markdown Link Smoke Test
type: Tech Debt
priority: P3
effort: Small
impact: Low
created: 2026-05-10
---

# Docs — Broken Internal Markdown Link Smoke Test

## Problem Statement

`docs/` is growing (CHANGELOG, configuration, architecture,
features, programmatic_api, backfill, notifications, etc.).
Cross-links between markdown files rot silently when files
are renamed or moved. No CI check exists today to catch a
broken `[link](docs/foo.md)` reference.

## Scope

Add a lightweight smoke test (Python; no external deps beyond
`re`) that walks every `.md` file under `docs/` and `README.md`,
extracts every `[text](path)` link with a non-URL target, and
asserts the path resolves. Run as part of the existing pytest
suite under `tests/test_docs_links.py`.

## Why It Matters

- Recent feature merges renamed `qualifire/core/backfill_eligibility.py`
  → deleted; `tests/test_skip_if_cached.py` →
  `tests/test_skip_recollection.py`. Docs cross-links pointing
  at old paths exist today (CHANGELOG entries cite old names
  for context, which is fine; what's NOT fine is forward-doc
  pointers that should resolve).
- Pre-merge catch of "this link points at a file that doesn't
  exist anymore" prevents a slow-drip of stale references.

## Why Deferred

Captured during `comprehensive-config-and-architecture-docs`
v2 scope cut. Adds value but is orthogonal to the core
documentation refresh.

## Affected Areas

- New `tests/test_docs_links.py`.
- Possibly an entry in `pyproject.toml` if any new dev
  dependency surfaces (none expected).
