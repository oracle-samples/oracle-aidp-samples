# Plan: skip-recollection

## Goal

Replace the existing `skip_if_cached` flag with `skip_recollection`
and remove its three layered restrictions (validator-type
whitelist, dimensional-collector null-return, filtered-collector
runtime bypass). The new contract: **for every metric the
collector would emit, look up the natural key in the system table;
if every expected combination has an active non-NULL row, replay
synthetic `CollectionResult`s instead of running the collector.**
No filter-identity column — same filter-scope semantics as
`skip_revalidation` (filter not in the natural key).

## Locked Decisions (user mandates)

1. **Clean break, no alias.** `skip_if_cached` removed entirely;
   `skip_recollection` is the only name. All entry points + CLI +
   `QualifireContext` rename. No deprecation alias.
2. **Drop all three layered restrictions:**
   - Delete `cache_eligible(val_config)` whitelist.
   - Remove the dimensional-collector early `None` return.
   - Remove the filtered-collector runtime bypass.
3. **No filter-identity column on storage.** Per user's call
   during planning sweep: "since we are checking on (dataset,
   table, metric, partition, dim), changing filter would still
   skip collection if metric is already present." Document the
   trade-off similar to `skip_revalidation`'s filter-scope
   limitation.
4. **All four backends ship together** — even though no schema
   change is needed (no new column), the dimensional-enumeration
   storage helper requires per-backend implementation.
5. **Default `False`.** Mirrors `skip_renotification` /
   `skip_revalidation`.
6. **Sample-based validators (Pattern, AnomalyDetection)** fall
   through because they lack `expected_metrics()`; the new pre-
   pass guards `if not hasattr(val_config, "expected_metrics"):
   return None` before any storage read (R1 MEDIUM correction —
   prior framing was "presence check returns NULL" but the gate
   actually fires earlier). Defense in depth: even if a future
   refactor adds `expected_metrics()` to these validators, their
   persisted sample row carries `metric_value=NULL` and
   `read_collection_metric_at_partition`'s
   `metric_value IS NOT NULL` filter still returns None.
7. **SLO is OUT OF SCOPE for this feature** (codex R1 BLOCKER):
   the original SLO concern was "replaying a cached `recency`
   row isn't a real freshness check." The deeper blocker is
   that the SLO collector emits a datetime as `metric_value`
   while storage's DOUBLE column round-trips as a different
   type — a cached replay would type-error in
   `SLOValidator.validate(...)` at the `now - recency_ts`
   subtraction. Don't add `expected_metrics()` to SLO; it
   continues to fall through the pre-pass via the existing
   `hasattr(val_config, "expected_metrics")` guard. Captured
   as `slo-recency-skip-recollection` follow-up backlog if
   operator demand arises.

## What Changes

### Public API rename — clean break

- `qualifire/api.py`:
  - `Qualifire.run_config(..., skip_if_cached=...)` →
    `skip_recollection=False`
  - `Qualifire.run_config_parsed(...)` — same
  - `Qualifire.backfill(...)` — same
  - `Qualifire.validate(...)` — same
- `qualifire/core/context.py`:
  - `self.skip_if_cached: bool = False` → `self.skip_recollection`
- `qualifire/cli.py`:
  - `--skip-if-cached` → `--skip-recollection` on `qualifire run`
    + `qualifire backfill`
- `qualifire/core/backfill.py`: rename `skip_if_cached` parameter
  in `run_backfill`, `_process_anchor`, `_run_anchor_once`, AND
  the per-anchor `ctx.skip_if_cached` setter (the same recurring
  serial+parallel pattern; codex-impl-review caught it twice
  already).
- `qualifire/core/config.py`: any docstring references to
  `skip_if_cached` updated.

### Engine — rewrite `_try_skip_if_cached`

Renamed to `_try_skip_recollection`. Body simplified:

- Gate on `self.context.skip_recollection`.
- Gate on `self.storage` not None.
- Gate on `ds_config.partition_ts` resolvable to an anchor (via
  the same `_resolve_run_level_partition_ts` helper used by
  `_try_skip_revalidation`).
- For each metric `val_config.expected_metrics()` returns:
  - If the collector has no dimensions: one point lookup at
    `(table, metric, partition_ts, dimension_value=None)`.
  - If the collector has dimensions: enumerate dim values
    persisted at this `(table, metric, partition_ts)` via a new
    `read_collection_dim_values_at_partition` storage helper,
    then point-lookup each `(table, metric, partition_ts, dim)`.
- If every expected `(metric, dim)` tuple has an active non-NULL
  row, synthesize and return `CollectionResult`s with
  `metadata["from_cache"] = True`. Otherwise return None.
- **Empty dim enumeration** (codex R1 MAJOR): when a dimensional
  collector's enumeration returns an empty list (no rows
  persisted yet for this metric at this partition — either a
  cold partition or a new dim that hasn't been collected
  before), the pre-pass returns `None` and the collector runs.
  Treats the empty case as "cache miss," NOT as "vacuously
  satisfied." Pin via test (T_empty_dim_falls_through). The
  remaining theoretical hole — a partition was previously
  collected with K dim values but the source now has K+1 —
  cannot be detected from the system table alone; documented
  as a known limitation of "data presence" semantics. Operator
  forces re-collection by setting `skip_recollection=False`
  for the run.

**Removed:**
- `from qualifire.core.backfill_eligibility import cache_eligible`
  + the gate. Delete `cache_eligible` and the entire
  `qualifire/core/backfill_eligibility.py` file (single-purpose
  helper; no other consumers).
- The dimensional-collector early-`None` block.
- The filtered-collector runtime bypass inside the pre-pass.

**KEPT** (codex R1 BLOCKER correction): the
`pre_composed_filter` precompute block in `_collect`
(engine.py:1220-1232) is **NOT** dead code — collectors
(Aggregation/Profiling/Metrics) consume it via
`filter_expr=pre_composed_filter` at engine.py:1262, 1278,
1286 to compose the dataset+collector filter. Initial v2
draft incorrectly slated it for deletion. It stays. Only the
internal-to-pre-pass bypass disappears.

The `effective_filter` kwarg on `_try_skip_if_cached` /
`_try_skip_recollection` is **removed** (no remaining
consumer once the bypass is gone — the pre-pass no longer
checks the filter at all).

### Storage — new dimensional enumeration helper

`read_collection_dim_values_at_partition(self, *, table_name: str,
metric_name: str, partition_ts) -> list[str | None]`

- Returns the distinct active dimension values persisted at the
  anchor for one `(table, metric)`. Pre-pass uses this to
  enumerate dimensions for collectors with `dimensions` set.
- All 4 backends. Same SQL idiom as
  `read_collection_metric_at_partition`:
  - Filter `record_type='collection'`, exact partition_ts,
    NULL-safe dim, dedup latest per natural key, post-dedup
    `is_active='true'` and `metric_value IS NOT NULL`.
  - Project `dimension_value` distinct.

### Config — no SLO change (codex R2 BLOCKER consistency fix)

SLO is OUT OF SCOPE per Locked Decision 7. Do **NOT** add
`expected_metrics()` to `SLOValidationConfig`. The pre-pass's
`hasattr(val_config, "expected_metrics")` guard handles the
fall-through. Pattern / AnomalyDetection / SLO all share the
"no `expected_metrics()` method" property → pre-pass returns
None → collector runs (defense-in-depth: their persisted shape
also wouldn't satisfy the `metric_value IS NOT NULL` filter,
but the `hasattr` guard fires first and is the load-bearing
check).

### Tests

- **Rename + rewrite** `tests/test_skip_if_cached.py` →
  `tests/test_skip_recollection.py` to:
  - Drive the new flag name.
  - Drop the validator-type-whitelist regressions (the whitelist
    is gone).
  - Add per-collector regressions matching the new contract:
    aggregation (with + without filter, with + without
    dimensions), profiling, metrics, custom_query, recency, sample.
  - Sample-based validator regression: Pattern /
    AnomalyDetection have no `expected_metrics()` method →
    pre-pass returns None on the `hasattr` guard → collector
    runs. (Plan v1 framed this as "metric_value=NULL" but
    sample collection rows are filtered out earlier than
    the persisted-shape claim — codex R1 MEDIUM.)
  - Filter-scope behavior: a row exists at
    `(table, metric, partition_ts, dim=None)` from a prior
    no-filter run; current run has a filter set; flag=True;
    pre-pass STILL replays (per Locked Decision 3 — filter not
    in the lookup key). Test pins the documented trade-off.
  - Dimensional enumeration: persist 3 dim values for a metric;
    flag=True; pre-pass returns 3 synthetic CollectionResults.
- **Delete** the runtime-filter-bypass tests in
  `tests/test_collection/test_filters.py` that assert
  "filtered-collector runs collection" — this is no longer the
  contract. Replace with: "with filter set + flag=True +
  matching system-table row, pre-pass replays."
- **Delete** `tests/test_backfill_eligibility.py` if it exists
  (single-purpose tests for the deleted helper).
- Existing tests' kwarg renames: `skip_if_cached=` → `skip_recollection=`
  in `tests/test_cli_backfill.py`, `tests/test_cli.py`, etc.

### Documentation

- `docs/programmatic_api.md` — rename + the new contract.
- `docs/CHANGELOG.md` — Breaking entry naming the rename + the
  three restriction removals.
- `docs/backfill_and_soft_delete.md` — rename references.
- `docs/collectors/README.md` — new "When does the engine skip
  collection?" subsection naming the data-presence rule.

## Acceptance Criteria

- AC1: `skip_if_cached` does not appear anywhere in
  `qualifire/`, `tests/` (config code, runtime code, tests, or
  current docs). Only allowed mentions: historical references in
  shipped feature plans under `docs/features/`. No deprecation
  alias.
- AC2: `skip_recollection: bool = False` plumbed through
  `Qualifire.run_config`, `run_config_parsed`, `backfill`,
  `validate`, plus `--skip-recollection` CLI flag, plus
  `QualifireContext.skip_recollection`.
- AC3: `qualifire/core/backfill_eligibility.py` is **deleted**.
  No remaining references to `cache_eligible` anywhere.
- AC4: Engine's `_try_skip_recollection` short-circuits when
  every expected `(metric, dim)` combination has an active
  non-NULL row, regardless of validator type or filter scope.
- AC5: Sample-based validators (Pattern, AnomalyDetection)
  AND SLO fall through via the
  `hasattr(val_config, "expected_metrics")` guard — they don't
  define the method, so the pre-pass returns `None` and the
  collector runs as today. (No new code on those config
  classes.)
- AC6: Dimensional collectors enumerate persisted dim values
  via a new `read_collection_dim_values_at_partition` helper on
  all 4 backends; pre-pass requires every `(metric, dim)`
  combination present.
- AC7: Filter-scope trade-off pinned: a row persisted under no
  filter is replayed against a current run with a filter set
  (filter not in natural key). Documented + tested.
- AC8: `tests/test_skip_recollection.py` covers per-collector
  regressions including the no-filter / dimensional /
  sample-collector / filter-scope-replay cases.
- AC9: Full test suite green. No regressions.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Filter-scope replay surprises operators (Locked Decision 3) | CHANGELOG Breaking entry + flag's docstring pin the contract; same trade-off as `skip_revalidation`. |
| Dimensional enumeration silently misses a dim value present in the source but missing from the system table → pre-pass falls through correctly | Existing presence check already requires every expected combination. The "miss" case → collector runs → no harm. |
| Storage helper performance: enumerating dim values per metric per anchor is a separate query | Acceptable: the enumeration is a single `SELECT DISTINCT dimension_value` per metric; cheap on indexed tables. Defer optimization to a bulk variant if real workloads show contention. |
| Parallel backfill dropping the renamed kwarg in one of the two call sites (recurring indent bug from prior features) | Explicit T6-style test pinning per-worker context value. |
| `cache_eligible` is referenced from a context I missed (doc, test, third-party hook) | Hard grep at impl time + AC3 enforces deletion. |

## Out-of-Band Reviews

- 2 adversarial plan reviews.
- 2 codex plan reviews → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Medium. Estimated:
- ~50 LOC engine pre-pass rewrite.
- (No SLO changes per Locked Decision 7.)
- ~120 LOC storage helper across 4 backends.
- ~15 LOC config / API / CLI / context renames.
- ~250 LOC tests (rewrite + add).
- ~15 LOC docs.

Plus deletions: `cache_eligible` helper file (~30 LOC) and the
filtered-collector runtime bypass + tests (~60 LOC).

## Plan Iteration Log

- v1: initial draft.
- v4: addressed codex plan-review round 2 —
  (a) BLOCKER: SLO `expected_metrics()` add was contradictory
  with v3's "out of scope" Locked Decision 7; rewrote the
  Config section to explicitly omit the SLO change. LOC budget
  updated.
  (b) MEDIUM: AC5 was still phrased in "metric_value=NULL"
  terms while the locked decision uses the `hasattr` framing;
  AC5 now matches.
- v3: addressed codex plan-review round 1 —
  (a) BLOCKER: SLO out of scope (datetime metric_value
  round-trip would type-error; skip-recollection backlog
  follow-up if operator demand);
  (b) BLOCKER: KEPT `pre_composed_filter` precompute (NOT
  dead — collectors consume it for filter composition);
  (c) MAJOR: empty dim enumeration explicitly = "cache miss,"
  not "vacuously satisfied" (cold partition / new dim cases);
  (d) MEDIUM: sample-based validators fall through via
  `hasattr` guard (corrected from the v1's "metric_value=NULL"
  framing).
- v2: addressed adversarial round 1 — corrected the
  Pattern/AnomalyDetection fall-through framing (the gate
  trips on missing `expected_metrics()`, not the metric_value
  filter); explicit SLO trade-off doc (replayed `recency` row
  isn't a real freshness check); cleanup of the dead
  `pre_composed_filter` precompute block in `_collect` and the
  `effective_filter` kwarg on the pre-pass.
