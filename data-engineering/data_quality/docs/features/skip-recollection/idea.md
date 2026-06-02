---
id: skip-recollection
name: Skip-Recollection — Skip Collection When Metric Already in System Table
type: Enhancement
priority: P1
effort: Large
impact: High
created: 2026-05-09
---

# Skip-Recollection — Skip Collection When Metric Already in System Table

## Problem Statement

The collection stage of the qualifire pipeline runs the configured
collector (aggregation, profiling, metrics, sample, recency,
custom_query) and writes one row per
`(dataset_name, table_name, metric_name, partition_ts,
dimension_value)` natural key into the system table. Re-running
the same collection for the same key against unchanged source
data produces an identical metric value — wasted work.

This feature adds a `skip_recollection: bool = False` runtime flag
that says: **for each metric the collector would emit, look up
the natural key in the system table; if an active row with a
non-NULL `metric_value` already exists, replay it instead of
running the collector.**

The eligibility check is purely **data-presence**: the question
is "is there a row at this key?", not "what kind of validator
will consume this metric?" or "what kind of collector would
produce it?" The validator type, the collector type, the
filter / dimension shape — none of those are gates. The system
table either has the row or it doesn't.

A precursor exists today as `skip_if_cached: bool = False` (engine
pre-pass at `qualifire/core/engine.py:1379`) but with three
layered restrictions that make it unreachable on the demo YAMLs:

1. A hard-coded validator-type whitelist in
   `qualifire/core/backfill_eligibility.py:cache_eligible(val_config)`
   excludes SLO, Pattern, AnomalyDetection.
2. Dimensional collectors return `None` from the pre-pass
   (the engine doesn't enumerate dim values up front).
3. Filtered collectors are runtime-bypassed for filter-scope
   safety (post `and-combine-collector-filter`; see
   `cache-key-filter-identity` backlog).

All three restrictions ride on top of the same data-presence
question — and all three drop away once eligibility is
genuinely "row exists for this natural key, with the right
filter scope identity." The whitelist becomes redundant; the
dimensional gate becomes a forward enumeration; the filter
bypass becomes a key extension.

## Why It Matters

- **Retry cost.** A 20-minute collection that completes once
  and then fails downstream (validator exception, persistence
  outage, notifier failure) should not re-run for 20 minutes
  on retry. Today most validators leave that money on the
  table because the eligibility gate locks them out.
- **Backfill performance.** `qualifire.backfill(...)` is the
  main caller of `skip_if_cached=True`. Replays of partitions
  that already collected re-aggregate every row instead of
  reading the persisted value.
- **Operator mental model.** "If it's in the system table, use
  it" is intuitive and self-explanatory. Today's eligibility
  gates surprise operators repeatedly.
- **Naming clarity.** `skip_if_cached` is ambiguous (cached
  *what*? where?). `skip_recollection` names the user intent
  precisely — skip the collection step.

## Default

`skip_recollection = False` everywhere. Re-collect on every run
unless the operator opts in. Matches today's safe default for
`skip_if_cached`. Sibling flags (`skip_revalidation`,
`skip_renotification`) follow the same default-False contract.

## Who Benefits

- **Operators retrying after transient failures** anywhere
  downstream of collection (validator exception, notifier
  outage, system-table write hiccup).
- **Backfill drivers replaying historical partitions** that
  were already collected.
- **AIDP demo / iteration loops** rehearsing the validator
  pipeline without re-paying collection cost on every cell
  run.
- **CI / smoke harnesses** that assert reproducibility — replay
  the persisted run and observe identical pipeline behaviour.

## Affected Areas

### Public API rename — `skip_if_cached` → `skip_recollection`

- `qualifire.api.Qualifire.run_config(..., skip_recollection=False)`
- `qualifire.api.Qualifire.run_config_parsed(...)`
- `qualifire.api.Qualifire.backfill(...)`
- `qualifire.api.Qualifire.write_audit_publish(...)`
- `qualifire.cli` — `--skip-if-cached` → `--skip-recollection`
- `qualifire.core.context.QualifireContext.skip_if_cached`
  attribute → `skip_recollection`

Plan-phase decision: keep the old name as a deprecated alias
for one minor version vs. clean break with release-note
migration. The recent `and-combine-collector-filter` precedent
leans toward clean break.

### Engine — replace the eligibility gate with a presence check

- `qualifire/core/engine.py:_try_skip_if_cached` (rename to
  `_try_skip_recollection` for consistency with the new flag)
  — replace the layered gates with one rule: "for every metric
  the collector would emit, look up
  `(table_name, metric_name, partition_ts, dimension_value,
  effective_filter_hash)` in the system table; if every
  natural key has an active non-NULL row, return synthetic
  `CollectionResult` rows with `metadata['from_cache']=True`;
  otherwise fall through to the collector."
- `qualifire/core/backfill_eligibility.py:cache_eligible` —
  delete the function. The validator-type whitelist no longer
  exists. (Sample collectors fall through *organically*: their
  persisted row carries `metric_value=NULL` so the presence
  check fails. No special case needed.)
- Dimensional enumeration: when the collector has dimensions,
  enumerate the persisted `(dim_value, metric_name)` tuples at
  the partition anchor and require every combination to have
  an active row.
- Drop the runtime filter bypass added by
  `and-combine-collector-filter` once the filter-identity
  column is in place.

### Storage — system-table column for filter identity

Folds in the previously-captured `cache-key-filter-identity`
backlog item entirely. Touches all four storage backends:

- `qualifire/storage/sqlite_storage.py`
- `qualifire/storage/delta_storage.py`
- `qualifire/storage/jdbc_storage.py`
- `qualifire/storage/external_catalog.py`

Each backend declares the new column, populates it on write,
and matches on it during the read. Migration: existing rows
default to NULL hash → match only NULL-hash queries (the safe
"never replay this against a filtered config" default).

### Tests

- `tests/test_skip_if_cached.py` — rewrite to drive the new
  flag and the new presence-only eligibility. Per-collector
  regressions: aggregation (filtered + dimensional + plain),
  metrics, profiling, custom_query, recency, sample.
  Per-validator-type assertions are *side effects* of the
  collector regressions — operators don't pick "skip-eligible
  validators", they pick collectors and the data presence
  decides.
- `tests/test_collection/test_filters.py` — drop the runtime-
  bypass tests; replace with filter-aware short-circuit tests
  matching on the new column.

### Documentation

- `docs/CHANGELOG.md` — Breaking entry for the rename;
  Enhancements entry for the broader eligibility.
- `docs/programmatic_api.md` — flag rename, deprecation note
  (if applicable), examples.
- `docs/collectors/README.md` — "When does the engine skip
  collection?" subsection that names the data-presence rule
  and points at `skip_recollection`.

## Relationship to Other Features

- **Supersedes
  `docs/features/cache-key-filter-identity/idea.md`** in scope.
  The smaller idea covers a strict subset (the filter-identity
  column). Once this feature ships, the smaller one's scope is
  fully addressed; the smaller idea is marked superseded.
- **Builds on `and-combine-collector-filter`**. The runtime
  bypass that feature ships is the placeholder this feature
  removes.
- **Independent of `skip-revalidation` and
  `skip-renotification`.** Each is a standalone runtime flag
  governing a different pipeline stage with its own data-
  presence gate; they don't depend on each other for
  eligibility, schema, or correctness. They compose freely —
  any subset can be enabled — but each can ship in its own
  iteration without coupling.

## Why Deferred

The parent `and-combine-collector-filter` feature was scoped to
"AND-combine the dataset and collector filters." This feature
redesigns skip-recollection eligibility broadly — different
change vector, larger surface, deserving of its own adversarial
review cycle. Codex round 2 of the parent plan pinned the
runtime bypass as the right placeholder; this feature replaces
it with a principled presence-driven contract.

## Open Questions for Planning

- **Migration shape for the rename** — deprecation alias for
  one minor version vs. clean break.
- **Sample-collector visibility** — pattern / anomaly persist
  `metric_value=NULL` rows; the presence check fails for them
  organically. Should we surface a one-line DEBUG log
  ("collector emits non-cacheable metric_value; skipping
  presence check") so operators don't waste time wondering why
  `skip_recollection=True` has no speedup on sample-based
  configs?
- **Hash function for filter identity** — full SHA-256 hex,
  truncated, or a different stable hash? Column width / index
  cost vs. collision probability.
- **Dimension enumeration source** — backfill already does
  per-dim enumeration via its own driver; should the forward-
  execution pre-pass share that helper or have its own?
- **Backend rollout** — ship all four storage backends together
  vs. SQLite + Delta first and JDBC + external_catalog as a
  follow-up? The parent feature established that breaking-
  correctness fixes ship together; this is mostly an
  optimisation, so a phased release is more defensible than
  for the parent.
