---
shipped: 2026-05-08
---

# Shipped: Metrics Backfill and Soft-Delete (Foundation)

## Summary

Foundation slice of the larger plan: ships the **`partition_step`
config field + H3 invariant**, the **`is_active` storage column +
write-site stamping**, and the **SQLite read-path rework** that makes
soft-delete actually hide deactivated rows from every consumer that
currently exists.

The user-facing primitives the plan also designed (`Qualifire.backfill`,
`Qualifire.deactivate_metric`, `skip_if_cached`, the matching CLI
subcommands, the engine backfill loop, the storage rewrite for the
three Spark-backed backends, the e2e test, and docs/examples) are
**out of scope** of this PR and tracked as follow-up work — see
"Follow-up scope" below.

The plan iterated through **6 codex review rounds** + **3 adversarial
self-passes** before reaching codex PASS at R6. The implementation
went through **2 codex impl-review rounds** (R1 FAIL → fixed → R2
CONDITIONAL PASS → should-fixes addressed).

## Key Changes

### S1 — `partition_step` config field + H3 invariant
- `DatasetConfig.partition_step: str | None` and
  `QualifireConfig.partition_step: str | None`
  (ISO 8601 duration: `P1D`, `PT1H`, `P1W`, `P2DT12H`).
- Cross-field validator at `QualifireConfig` level enforces the H3
  invariant: effective `partition_ts` ↔ effective `partition_step`
  (XOR rejected at config-load time, with the dataset name and a
  copy-paste fix in the error message).
- Run-level `partition_step` with no dataset using `partition_ts`
  is rejected as decorative cadence.
- New helpers `effective_partition_step(dataset, run)` and
  `effective_partition_ts(dataset, run)` for downstream callers.
- `Qualifire.validate(...)` and `Qualifire.validate_query(...)`
  accept `partition_step=...` and plumb it to `DatasetConfig`.

### S2 — `is_active` storage column + write-site stamping
- `is_active TEXT` column added to `SYSTEM_TABLE_COLUMNS`,
  `SYSTEM_TABLE_DDL`, `COLUMN_DEFINITIONS`, and the SQLite DDL.
- Defaults to `'true'` on every persisted row. Stored as TEXT
  (`'true'` / `'false'`) for portability across the four backends.
- `_ensure_columns()` migration auto-adds the column on next
  `initialize()` for SQLite (Delta / ExternalCatalog / JDBC are
  out of scope this PR).
- Engine row builders (`_build_validation_and_collection_rows`,
  `_engine_warning_row`, the notification row builder) stamp
  `is_active='true'` on every emitted row.
- `QualifireContext.backfill: bool` and `cached_metrics: dict`
  added — wired now, consumed by future S5/S8 work.

### S3 — SQLite read paths honour `is_active` (per H1/H2)
Six SQLite reads migrated to two-stage CTE: ROW_NUMBER per natural
key first → `COALESCE(is_active, 'true') = 'true'` filter →
`metric_value IS NOT NULL` filter → consumer's `record_type` filter.
- `read_metric_history` (non-bucketed): contract change from
  "up to N raw rows" to "up to N partition-distinct rows,
  soft-delete-aware". Documented behavior change in plan.
- `read_metric_history(step=...)` (bucketed): two-stage rework so a
  tombstone in a newer run-time bucket hides the active row in its
  old bucket.
- `read_metric_history_by_partition`: H2 ordering invariant
  (`run_timestamp DESC, collected_at DESC` — closes a future-
  `collected_at`-stale-row race).
- `read_validation_history`: per-natural-key dedup + is_active filter.
- `read_validation_history_bulk`: hard-enforced `limit=1`
  (notification suppression contract); raises `ValueError` for
  `limit > 1` since the multi-row tombstone-aware walk is out of
  scope this PR.
- `read_health_data`: H2 chain + `record_type_clause` (preserves
  the `include_collection=True` path that landed on `main` in
  parallel).
- `read_latest_run`: returns `None` when the latest row is a
  tombstone (H8).

### H6 — new `read_collection_metric_at_partition` storage method
- Declared on the `SystemTableStorage` Protocol.
- SQLite implementation lands in this PR; backs the future
  `skip_if_present` / `skip_if_cached` cache hits.
- Filter chain: `record_type='collection'` ∧ exact `partition_ts`
  ∧ NULL-safe `dimension_value`, ROW_NUMBER ORDER BY
  `run_timestamp DESC, collected_at DESC`, then `rn = 1` ∧
  `is_active` ∧ `metric_value IS NOT NULL`.

## Files Changed

> **Note (added 2026-05-10 via `backfill-followups-and-polish`
> Item 4):** Items marked **[PR #13]** below were originally
> claimed as shipped in this foundation PR but actually landed
> in the follow-up
> [PR #13 (`backfill-api-and-wap-target-mode`)](https://github.com/amitranjan-oracle/qualifire/pull/13).
> The foundation PR shipped only the scaffolding contracts; the
> consumers landed in the follow-up. Verified via
> `git log --diff-filter=A --follow -- <path>`.

- `qualifire/api.py` — `partition_step` kwarg on `validate` /
  `validate_query`; `_build_skip_if_cached_pre_pass` scaffold for
  future S8 wiring.
- `qualifire/core/config.py` — `partition_step` fields, validators,
  H3 invariant, `effective_partition_step` / `effective_partition_ts`
  helpers. **[PR #13]** `expected_metrics()` on threshold /
  historical / forecast validators (commit `9f05dc2`).
- `qualifire/core/context.py` — `backfill: bool`, `cached_metrics`
  fields.
- `qualifire/core/engine.py` — `is_active='true'` stamps in three
  row builders. **[PR #13]** `_tag_with_backfill` helper
  (renamed from the planned `_add_backfill_tag`; commit `76c53c1`).
- **[PR #13]** `qualifire/core/backfill_eligibility.py` (new) —
  `cache_eligible` predicate (commit `9f05dc2`).
- `qualifire/storage/base.py` — `is_active` column added; new
  `read_collection_metric_at_partition` Protocol method (this PR;
  commit `41f9bda`).
- `qualifire/storage/sqlite_storage.py` — H2/H6 read rewrites,
  new method.
- `qualifire/storage/delta_storage.py`,
  `external_catalog.py`, `jdbc_storage.py` — column added to shared
  schema, but SQL bodies not yet rewritten (follow-up).
- `tests/test_config_partition_step.py` (new, 11 cases)
- `tests/test_storage/test_soft_delete_sqlite.py` (new, 15 cases)
- `tests/test_api.py` — `validate_query(partition_ts=...,
  partition_step=...)` regression test.
- Existing test fixtures: `partition_step="P1D"` added to
  `DatasetConfig` literals using `partition_ts` (documented breaking
  change; ~6 test files).

## Testing

- **960 tests passing** locally before merge.
- New coverage:
  - 11 H3 invariant cases (inheritance, decorative-cadence rejection,
    invalid ISO duration, run-level-only forms).
  - 15 H2/H6 cases: NULL-tombstone-hides-stale-active, dimensional
    sibling isolation, legacy NULL-`is_active` row, bucketed cross-
    bucket suppression, `read_validation_history` /
    `_bulk` / `_latest_run` (H8), `read_collection_metric_at_partition`
    (H6 contract), and a future-`collected_at` regression.
- All four storage tests, engine routing, validate_df, signed_drift,
  partition_ts, columns_descriptions, threshold validation, e2e
  industries (untouched), and reporting/health regression tests
  pass.

## Notes

### Documented behavior change

`read_metric_history` (non-bucketed) now returns **up to N
partition-distinct rows** rather than **up to N raw rows**. Two rows
with the same `(table, metric, partition_ts, dimension)` collapse to
one (collection-preferred via the `CASE WHEN record_type='collection'`
tiebreaker). Verified via `git grep "read_metric_history\b"` that no
production caller depends on raw-row history — internal callers want
per-partition latest. Tests updated where they implicitly relied on
the old contract.

### `read_validation_history_bulk(limit > 1)` is unsupported

The current notification-suppression caller uses `limit=1`. Lifting
the limit cleanly under soft-delete requires a tombstone-aware
"cumulative-active" walk that's out of scope here. The API now
raises `ValueError` for `limit > 1` to prevent silent stale-active
leakage. If a future caller needs multi-row history, this is the
single place to extend.

### Reactivation contract

A backfill that re-collects a deactivated key intentionally **wins**:
the new active row carries a fresh `run_timestamp` and supersedes the
tombstone via `ROW_NUMBER ORDER BY run_timestamp DESC`. Operators
who want a deactivation to stick must run `deactivate-metric`
**after** any backfill that touches the range.

The `core/deactivate.py` helper that implements the H1 read-back-
and-bump for tombstone `run_timestamp` is **not** shipped here (S7
follow-up). Until it lands, operators have no API to write
tombstones; the engine and read paths are simply ready to honour
them.

### Parallel dashboard work preserved

The parallel `feat(dashboard): plot full per-partition history by
joining collection rows` and `fix(dashboard): validation always wins
over collection in history dedupe + scroll/redraw fixes` commits on
`main` are layered intact. The earlier `fd45c11` dashboard-tabs
commit on this feature branch became a no-op once main's newer
dashboard direction superseded its conflicting hunks; non-conflicting
parts of `fd45c11` would have stayed but the conflicting hunks were
the entire commit's overlap with main.

### CI / impl-review status

- 960 unit tests green locally.
- Plan went through 6 codex review rounds (R1-R6) — final R6 verdict
  PASS.
- Implementation went through 2 codex impl-review rounds (R1 FAIL →
  fixed → R2 CONDITIONAL PASS → should-fixes addressed including the
  `limit=1` runtime contract).

## Follow-up scope (NOT shipped here)

The plan also designed and validated the following surfaces, deferred
to follow-up commits / a continuation feature:

- **S2/S3 on Delta, ExternalCatalog, JDBC** — `is_active` column is
  in the shared schema, but these backends' read SQL bodies still
  ignore it and they don't implement
  `read_collection_metric_at_partition`. Until those land, soft-
  delete is **SQLite-only**.
- **S4** — `qualifire/core/selectors.py` (selector parser) and the
  `ScopedDataset` resolver.
- **S5** — backfill loop driver + `_run_backfill` + per-partition
  source selection (H2'); `expected_metrics()` resolver is shipped,
  the loop that consumes it is not.
- **S6** — `Qualifire.backfill` API + `BackfillReport` dataclasses.
- **S7** — `Qualifire.deactivate_metric` API +
  `qualifire/core/deactivate.py` (with the H1 read-back-and-bump
  helper).
- **S8** — `skip_if_cached` engine pre-pass; the API kwarg on
  `Qualifire.validate` exists and the cache-eligible predicate is
  shipped, but the engine doesn't yet consult
  `QualifireContext.cached_metrics` from collectors.
- **S9** — `qualifire backfill` / `qualifire deactivate-metric` /
  `--skip-if-cached` CLI subcommands.
- **S10** — end-to-end test composing all four primitives.
- **S11** — `docs/backfill_and_soft_delete.md` (architecture +
  JDBC pre-deploy `ALTER TABLE` SQL), `examples/backfill_quickstart.py`,
  and the README / `programmatic_api.md` / `configuration.md`
  cross-links.

These can ship in follow-up commits without re-opening the design
— the plan's H1-H8 invariants and the eleven step exit criteria
remain authoritative.

## Plan PR

[#10](https://github.com/amitranjan-oracle/qualifire/pull/10) —
`feat(metrics-backfill-and-soft-delete): plan + scaffolding`
