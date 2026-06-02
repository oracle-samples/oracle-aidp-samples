---
shipped: 2026-05-10
---

# Shipped: skip-recollection

## Summary

Replaces `skip_if_cached` with `skip_recollection` (clean break,
no alias) and removes the pre-pass's three layered restrictions:
the `cache_eligible(val_config)` validator-type whitelist, the
dimensional-collector early-`None` block, and the filtered-
collector runtime bypass. The new contract: data-presence — for
every metric the validator's `expected_metrics()` returns, every
persisted `(metric, dim)` combination must have an active non-
NULL row at the resolved partition_ts → replay synthetic
`CollectionResult`s with `metadata["from_cache"] = True`.
Otherwise fall through to the collector.

Filter scope is NOT in the natural key — a row persisted under
one filter scope replays against a different filter. Same trade-
off as `skip_revalidation`. Pattern / AnomalyDetection / SLO
continue to fall through the pre-pass via the `hasattr(val_config,
"expected_metrics")` guard. SLO carved out (datetime
`metric_value` round-trip would type-error in the validator; will
revisit as `slo-recency-skip-recollection` backlog if operators
ask).

## Key Changes

- **API + CLI rename** —
  `Qualifire.run_config(..., skip_if_cached=...)` →
  `skip_recollection=False` (and `run_config_parsed`, `backfill`,
  `validate`). CLI `--skip-if-cached` → `--skip-recollection`.
  `QualifireContext.skip_if_cached` → `skip_recollection`.
- **`qualifire/core/engine.py`** — `_try_skip_if_cached` →
  `_try_skip_recollection`. Body simplified:
  - Removed the `cache_eligible` import + gate.
  - Removed the dimensional-collector early-`None` block.
  - Removed the filtered-collector runtime bypass + the
    `effective_filter` kwarg on the pre-pass.
  - Added dimensional enumeration: when the collector has
    `dimensions` set, calls `storage.read_collection_dim_values_at_partition(...)`
    and point-looks up each `(metric, dim)`. Empty enumeration
    is treated as a cache miss, NOT vacuously satisfied (codex
    plan-review R1 MAJOR).
  - The `pre_composed_filter` block in `_collect` (engine.py:
    1209-1232) is **kept** — it's consumed by the collector
    dispatch below. Codex initially flagged for deletion but
    that was a misread; `pre_composed_filter` is load-bearing
    for filter composition. Comment updated to reflect that
    it's no longer consumed by the pre-pass.
- **`qualifire/core/backfill_eligibility.py`** — **deleted**.
  Single-purpose helper, no other consumers. Audited.
- **All 4 storage backends** (`SQLiteStorage`, `DeltaStorage`,
  `ExternalCatalogStorage`, `JDBCStorage`): new
  `read_collection_dim_values_at_partition(*, table_name,
  metric_name, partition_ts) -> list[str | None]`. Two-stage
  SQL with ROW_NUMBER per `dimension_value` ORDER BY
  `run_timestamp DESC, COALESCE(collected_at, run_timestamp)
  DESC` (codex impl-review R1 MAJOR — without the
  `collected_at` tiebreaker, tied timestamps would dedup
  non-deterministically). Post-dedup `is_active='true'` and
  `metric_value IS NOT NULL` filters.
- **Tests** —
  - `tests/test_skip_if_cached.py` → `tests/test_skip_recollection.py`,
    rewritten from scratch (9 tests covering T1-T6 + storage
    helper contract + parallel backfill plumbing + filter-scope
    replay trade-off pinned with control assertion).
  - `tests/test_collection/test_filters.py`: deleted 2 stale
    test classes (`TestSkipIfCachedRuntimeBypass`,
    `TestCacheBypassScope`) — they asserted the removed contract.
  - All other test references to `skip_if_cached` /
    `--skip-if-cached` updated to the new names.

- **Documentation** —
  - `docs/CHANGELOG.md`: Breaking entry covering the rename + the
    three restriction removals + filter-scope trade-off + SLO
    carve-out.
  - `docs/backfill_and_soft_delete.md`: rename references throughout.

## Files Changed

15 files; +446 / −410 lines (net +36; mostly the new
storage helper + tests minus the deleted module + stale tests).

## Plan PR

[#25](https://github.com/amitranjan-oracle/qualifire/pull/25).

## Review Cycles

Plan: 1 adversarial + 3 codex rounds.
- Codex R1 found 2 BLOCKERs (SLO datetime round-trip type-error
  + `pre_composed_filter` not actually dead code) + 1 MAJOR
  (empty dim enumeration semantics) + 1 MEDIUM (sample
  fall-through framing).
- Codex R2 found 1 BLOCKER (Locked Decision contradicted body
  on SLO scope) + 1 MEDIUM (AC5 framing).
- Codex R3 PASS at v4.

Implementation: 2 codex rounds.
- Codex R1 found 1 MAJOR (tied-timestamp dedup
  non-deterministic across all 4 backends) + 1 MEDIUM
  (missing-table error handling — engine wrap covers it; LOW
  reclassification accepted) + 2 LOWs (test under-evidenced;
  stale comment).
- Codex R2 PASS.

## Local Test Results

- 1506 passed, 2 skipped (was 1517 before this feature; net -11
  from removing cache_eligible whitelist tests + filter-bypass
  tests; +9 new tests in `test_skip_recollection.py`).
- No regressions; full suite green.
