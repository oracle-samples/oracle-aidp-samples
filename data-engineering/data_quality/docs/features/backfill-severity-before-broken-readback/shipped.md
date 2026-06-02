---
shipped: 2026-05-10
---

# Shipped: backfill-severity-before-broken-readback

## Summary

Fixes a 100%-incidence bug where `BackfillReport.partitions[*].severity_before`
was always `None`. `_read_severity_before` called
`read_validation_history(validation_name="")` which the storage
backends' exact-match filter never accepted. Knock-on:
`_classify_diff` over-reported `refreshed` when only severity
flipped between runs.

## Key Changes

- **`qualifire/core/backfill.py:_read_severity_before`**:
  - Signature: now takes `validation_names: list[str]` and
    `partition_ts: datetime`. Caller resolves both at the
    per-anchor site.
  - Reuses `storage.read_validations_at_partition` (the helper
    just shipped in skip-revalidation, PR #24) to scope the
    read to the exact backfill partition — fixes a separate
    correctness gap codex flagged in review (the previous
    helper returned the latest-overall row, not the partition-
    scoped one).
  - Aggregates max severity across multiple validators on the
    same metric (ERROR > WARNING > PASS). Symmetric with
    `_extract_metric_severity` post-fix so diff classification
    compares like-with-like.

- **New helper `_resolve_metric_to_validation_names(scope)`**:
  walks `scope.validations`, calls `_resolve_validation_base_name`
  per validator, builds `metric_name → [f"{base}.{metric}", ...]`.
  Validators without `expected_metrics()` (Pattern, Anomaly,
  SLO) contribute nothing — they don't flow through the
  per-metric backfill driver anyway.

- **`_extract_metric_severity` rewritten** (codex impl-review
  R1 MAJOR — without symmetric aggregation, severity_before
  could have been ERROR while severity_after was the first
  matching PASS, and the diff would falsely report `refreshed`).
  Now plucks metric_value once from collection_results and
  max-aggregates severity across all matching ValidationResults.

## Files Changed

5 files; +293 / −38 lines.

## Plan PR

[#27](https://github.com/amitranjan-oracle/qualifire/pull/27).

## Review Cycles

Plan: 1 adversarial + 2 codex rounds.
- Codex R1: 4 LOWs (confirming planner's claims) + 2 MAJORs
  (empty-data row metric_name=None contract; severity readback
  unanchored to backfill partition).
- Codex R2 PASS at v2.

Implementation: 2 codex rounds.
- Codex R1 (1 MAJOR + 1 LOW): asymmetric max-aggregation
  between `_read_severity_before` (aggregated) and
  `_extract_metric_severity` (first-row-wins) → diff
  classification could falsely report `refreshed`. Fixed by
  rewriting `_extract_metric_severity` to also max-aggregate.
  LOW: comment incorrectly cited `__lt__` for Python `max()`
  ordering — corrected to `__gt__`.
- Codex R2 PASS.

## Local Test Results

- 1517 passed, 2 skipped (+11 new tests in
  `tests/test_backfill_severity_before.py`).
- No regressions; full suite green.
