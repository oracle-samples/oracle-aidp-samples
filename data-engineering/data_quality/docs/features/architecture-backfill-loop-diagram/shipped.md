---
shipped: 2026-05-11
---

# Shipped: architecture-backfill-loop-diagram

## Summary

New "## 2a. Backfill loop" section in `docs/architecture.md`
between the existing §2 (WAP lifecycle) and §3 (deferred
items). The section pairs an ASCII control-flow diagram with
prose answers to the four recurring operator questions about
the backfill driver. Also trims §3 to the one remaining
deferred backlog item now that `configuration-reference-audit`,
`pydantic-docstring-audit`, and `docs-lint-ci` have shipped.

## Key Changes

- **`docs/architecture.md`** — 140-line new section traces
  `qualifire backfill` →
  `run_backfill` (`qualifire/core/backfill.py:197`) →
  `_resolve_scopes` (`:628`) →
  `_WorkUnit` list →
  serial / `ThreadPoolExecutor` dispatch →
  per-anchor `_process_anchor` (`:350`):
  - Pass 1a: per-metric `_read_original_value` +
    `_read_severity_before`; failure aborts anchor.
  - Pass 1b: bulk tombstone via
    `build_prior_tombstone_rows` + single
    `storage.write_results` call, only when
    `soft_delete_prior=True`; failure aborts anchor.
  - Engine-once: `_run_anchor_once` (`:581`) →
    `engine.run()` exactly once per (scope, anchor);
    skipped entirely when `metric_names` is empty.
  - Pass 2: `_extract_metric_severity` per metric →
    `PartitionDiff(status=...)`.
  - Sample validators get `status="skipped"` diffs appended
    on TWO separate exit paths (engine-failure `except` and
    end-of-function happy path).
  - Sort by `seq_idx` → `BackfillReport`.
- Prose covers:
  - Why tombstones write BEFORE the engine.
  - Why parallel mode forces `notifiers={}`.
  - Why per-anchor reads anchor on `partition_ts`
    (post `backfill-severity-before-broken-readback`).
  - Why sample validators surface as `status="skipped"`.
- **`docs/architecture.md` §3** — trimmed from 5 deferred
  bullets to 1 (`configuration-reference-collectors-extension`).
- **`docs/CHANGELOG.md`** — Documentation entry.

## Files Changed

3 files; +160 / −24 lines (excluding plan.md and shipped.md).

## Plan PR

[#31](https://github.com/amitranjan-oracle/qualifire/pull/31).

## Review Cycles

Plan: 2 codex rounds.
- R1: 4 BLOCKERs — ACs not all diff-verifiable (AC5/AC6 were
  runtime/process gates); "~120 lines" budget had no
  tolerance; AC4 only removed this feature's bullet from §3
  while leaving 3 other already-shipped items stale; missing
  coverage of Pass 1a abort / Pass 1b conditional / sample-
  validator skip paths in the locked decisions.
- R2 PASS at v2.

Implementation: 2 codex rounds.
- R1: 1 BLOCKER — prose claim that "the end-of-function
  sample-validator block runs on both happy and engine-failure
  paths" was false; engine-failure path has its own separate
  sample-validator block at `backfill.py:516-528` before the
  early return at `:529`. Fixed by rewriting to state
  explicitly that there are TWO separate exit paths each with
  its own sample-validator-append step.
- R2 PASS.

## Local Test Results

- `tests/test_docs_links.py` passes — no broken internal
  links introduced.
- Section length: 140 LOC, at the upper edge of the
  90-140 budget locked in plan v2.
