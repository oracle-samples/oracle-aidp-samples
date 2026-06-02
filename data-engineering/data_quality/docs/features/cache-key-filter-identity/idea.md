---
id: cache-key-filter-identity
name: Cache-Key Filter Identity — Reach Filtered Collectors via skip_if_cached
type: Enhancement
priority: P2
effort: Medium
impact: Medium
created: 2026-05-09
---

# Cache-Key Filter Identity — Reach Filtered Collectors via skip_if_cached

> **Superseded by [`skip-recollection`](../skip-recollection/idea.md)
> (2026-05-09).** That broader feature redesigns
> `skip_if_cached` eligibility to be data-presence-driven for
> *all* validator types, drops the validator-type whitelist,
> adds dimension enumeration, AND folds in the entire scope of
> this idea (storage column for filter identity + filter-aware
> read path). When `skip-recollection` ships, this idea's work
> is fully covered. Keep this file as a historical pointer
> until the merge.

## Problem Statement

`Qualifire.run_config(..., skip_if_cached=True)` and
`Qualifire.backfill(..., skip_if_cached=True)` short-circuit
collection by reading the previously-persisted metric value from
the system table at the partition anchor instead of re-running
the collector. This optimisation is effective for unfiltered
collectors — a backfill replay of an unchanged partition skips
straight to the persisted value.

For filtered collectors (`AggregationCollectionConfig`,
`ProfilingCollectionConfig`, `MetricsCollectionConfig` with a
`filter:` set, or any of those on a `DatasetConfig` that carries
a dataset-level `filter:`), the cache short-circuit is currently
**runtime-bypassed for correctness**. The system-table cache row
carries no filter identity, so a row written under one filter
scope (e.g. before the `and-combine-collector-filter` feature
shipped, when the dataset filter was overridden by the collector
filter) could replay against a different scope after the
override → AND switch — silently mis-scoping the metric.

The runtime bypass keeps things correct, but means filtered
collectors **never benefit from** `skip_if_cached`. Every replay
re-aggregates against the cached / fresh-read source, even when
nothing has changed.

## Why It Matters

- **Backfill performance** — `qualifire.backfill(...)` is the
  main caller of `skip_if_cached=True`. Replaying a 90-day
  partition window with filtered collectors today re-aggregates
  every partition; with this feature, untouched partitions
  short-circuit.
- **Notebook iteration cost** — operators rehearsing demos with
  `skip_if_cached=True` and filtered configs see the same
  re-aggregation cost on every cell run.
- **Correctness invariant** — once cache lookup distinguishes
  filter scopes, the runtime bypass becomes redundant. Removing
  the bypass tightens the engine path back to the standard
  `_try_skip_if_cached → return synthetic CollectionResult` flow
  for *all* collector types uniformly.
- **Storage migration is not free** — adding a column to the
  system-table touches all four storage backends and requires
  thinking through pre-existing rows. This is feature-sized,
  not a sub-task — which is exactly why it was deferred from
  the `and-combine-collector-filter` parent feature.

## Who Benefits

- **Operators running multi-day backfills with filtered configs**
  — the most direct beneficiaries. Today's runtime bypass
  defeats the cache optimisation they explicitly opted in to.
- **AIDP demo / iteration loops** that combine
  `skip_if_cached=True` with the now-AND-combined filter
  semantics — currently paying a re-aggregation cost on every
  replay even when the underlying data hasn't moved.
- **Future cache-aware features** (e.g. dimension-aware cache
  enumeration, partition-anchored bulk replay) that depend on
  the cache lookup actually distinguishing scopes.

## Affected Areas

- `qualifire/storage/base.py` — `SystemTableStorage` Protocol
  signature for `read_collection_metric_at_partition` (new
  optional `effective_filter_hash` kwarg).
- `qualifire/storage/sqlite_storage.py`,
  `qualifire/storage/delta_storage.py`,
  `qualifire/storage/jdbc_storage.py`,
  `qualifire/storage/external_catalog.py` — all four backends
  declare the new column, populate it on write, match on it on
  read. Plus migration logic for tables provisioned before this
  column existed.
- `qualifire/storage/base.py:SYSTEM_TABLE_COLUMNS` /
  `COLUMN_DEFINITIONS` — new column entry.
- `qualifire/core/engine.py:_try_skip_if_cached` — drop the
  runtime bypass once the keyed cache is in place; pass the
  pre-composed effective filter through to the storage call.
- `qualifire/core/engine.py:_persist_data_rows` — populate the
  hash on collection-row writes.
- `tests/test_storage/*` — new tests for the keyed lookup path
  across all four backends.
- `docs/CHANGELOG.md` — entry under the next release's
  "Enhancements" section noting the cache-aware behaviour and
  the now-removed runtime bypass.

## Context

The runtime bypass was pinned by Codex round 2 of the
`and-combine-collector-filter` plan as the right placeholder
because:

1. The parent feature was already covering correctness (override
   → AND); cache optimisation is orthogonal.
2. Touching all four storage backends + migration is feature-
   sized, deserving of its own adversarial review cycle.
3. A one-line runtime bypass is auditable and reversible; a
   schema change interacts with running deployments and
   warrants its own iteration.

The current placeholder lives at
`qualifire/core/engine.py:_try_skip_if_cached` (the
`isinstance(collection, (Aggregation|Profiling|Metrics))` +
`if effective_filter is not None: return None` block). When
this feature ships, that block can be removed and replaced with
`storage.read_collection_metric_at_partition(...,
effective_filter_hash=hash(effective_filter))`.

The hashing function should be deterministic across runs, OS,
and Python versions — `hashlib.sha256(filter.encode()).hexdigest()[:16]`
or similar gives stability without collision risk for the
expected key cardinality. Plan-phase task.

## Open Questions for Planning

- **Hash function choice and length** — full SHA-256 hex, first
  16 chars, or a different stable hash? Trade-off: collision
  probability vs. column width / index cost.
- **Migration policy for existing rows** — NULL hash on legacy
  rows means they only match NULL-hash queries (i.e.
  unfiltered cases). Acceptable for forward execution; back-
  populating hashes for legacy filtered rows requires knowing
  their original filter, which the system table doesn't carry.
  Probably accept the legacy-rows-stay-stale trade-off and
  document it.
- **Whether to remove the runtime bypass entirely or keep it as
  a fail-safe** — once the keyed cache is reliable, the bypass
  becomes redundant. But leaving it as a defensive `if storage
  doesn't yet have the column → bypass` guard might be sensible
  for the first release after migration.
- **Backend coverage scope** — ship all four backends together,
  or land SQLite + Delta first and JDBC + external_catalog as a
  follow-up? The parent feature established that breaking-
  correctness fixes ship together with their docs; this is an
  optimisation, so a phased release is more defensible.
