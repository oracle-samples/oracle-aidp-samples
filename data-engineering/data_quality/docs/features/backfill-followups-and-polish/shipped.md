---
shipped: 2026-05-10
---

# Shipped: Backfill Follow-ups and Polish

## Summary

Six follow-ups deferred from PR #13
(`backfill-api-and-wap-target-mode`) land together. Bundled
because Items 1, 2, 3, and 6 share the backfill driver
(`qualifire/core/backfill.py`) and would otherwise re-touch the
same lines across separate commits.

## Key Changes

### Item 1 — `Qualifire.backfill(parallelism=N)` + `--parallelism N`
- New `Qualifire.backfill(..., parallelism: int = 1)` (cap 64).
- Outer-anchor fan-out via `concurrent.futures.ThreadPoolExecutor`.
- Engine-once refactor: new `_run_anchor_once(...)` helper runs
  the engine **once** per `(scope, anchor)` instead of once per
  metric. Per-anchor write/notification counts drop in serial
  mode too — see CHANGELOG Compatibility note A.
- Two-pass anchor worker: Pass 1a per-metric pre-engine reads;
  Pass 1b builds all tombstone rows in memory and writes them
  in ONE bulk `storage.write_results(...)` call (atomic
  via SQLite `executemany` + `rollback` on exception); SINGLE
  engine.run(); Pass 2 builds per-metric diffs from the cached
  `EngineResult`.
- `parallelism > 1` forces `notifiers={}` for the inner engine
  call to avoid suppression-read races; `BackfillReport.notifications_suppressed`
  flags it. CLI emits a stderr advisory.
- Result ordering byte-for-byte identical to serial via
  `seq_idx`-based linearization.
- `KeyboardInterrupt` propagates; queued futures cancelled,
  in-flight workers complete, no partial report.
- SQLite: `check_same_thread=False` + `RLock` decorator on every
  public method. Spark backends untouched (their own
  thread-safety dominates).
- Delta + ExternalCatalog: per-call UUID-salted temp-view names
  in `read_validation_history_bulk` to avoid cross-worker
  collisions.

### Item 2 — `WAPConfig.partition_column` auto-detect
- New `_resolve_partition_anchors(ds, run_config)` SOLE helper
  returning `(effective_partition_column, effective_partition_ts_expr)`.
- Stricter `_BARE_IDENT_RE = ^[A-Za-z_][A-Za-z0-9_]*$` for
  auto-detect ONLY (existing `_SAFE_NAME_RE` validator on
  explicit-set unchanged).
- Threaded through `_validate_backfill_source` AND
  `_build_partition_dataset` (no legacy direct-read fallbacks).
- Honours run-level `QualifireConfig.partition_ts` inheritance.
- Does NOT mutate the user's `WAPConfig` instance.

### Item 3 — Multi-row `read_validation_history_bulk(limit > 1)`
- Two-stage CTE across SQLite, Delta, ExternalCatalog, JDBC:
  - Stage 1: rank per `(input_idx, partition_ts)` to pick latest
    version per partition.
  - Filter `is_active` between stages.
  - Stage 2: re-rank survivors per `input_idx`, apply `LIMIT`.
- Tombstone scope clarified: hides only the partition it
  tombstones, not the whole key. Aligns with singular path —
  see CHANGELOG Compatibility note B.
- Final `ORDER BY input_idx, kn` (deterministic; no
  run_timestamp ties).
- `input_idx` via Python `enumerate(keys)` (NOT
  `monotonically_increasing_id`).
- JDBC predicate parenthesized: `WHERE ({where_sql}) AND
  record_type = 'validation'` so AND/OR precedence applies the
  filter to the full OR group.
- JDBC fallback at `_read_jdbc` exception preserved (per-key
  reads at the requested limit).

### Item 4 — Doc-drift fix on metrics-backfill-and-soft-delete/shipped.md
- Three symbols claimed as PR #10 actually shipped in PR #13:
  `expected_metrics()`, `_tag_with_backfill` (renamed),
  `backfill_eligibility.py`. Marked `[PR #13]` with verifying
  commit SHAs.

### Item 5 — CLI `errored` lines route to stderr
- Non-`--json` summary printer: `if diff.status == "errored":
  print(line, file=sys.stderr)`. `--json` mode unchanged.

### Item 6 — Configurable `max_partitions`
- `Qualifire.backfill(..., max_partitions: int = 10000)`.
- `qualifire backfill --max-partitions N` CLI flag.
- `expand_partition_ts(..., max_partitions=N)` keyword.
- `max_partitions < 1` rejected.

## Files Changed

- `qualifire/api.py` — `parallelism`, `max_partitions` kwargs +
  validation.
- `qualifire/cli.py` — `--parallelism`, `--max-partitions`,
  suppression advisory, errored stderr routing.
- `qualifire/core/backfill.py` — full driver refactor:
  `_resolve_partition_anchors`, `_BARE_IDENT_RE`, `_WorkUnit`,
  `_PreEngine`, `_process_anchor`, `_run_anchor_once`,
  threadpool fan-out, two-pass worker.
- `qualifire/core/backfill_report.py` — `notifications_suppressed`
  field.
- `qualifire/core/deactivate.py` — new `build_prior_tombstone_rows`
  (build, no write).
- `qualifire/storage/sqlite_storage.py` — `_locked` decorator,
  `check_same_thread=False`, `executemany` + `rollback`,
  two-stage CTE, `limit < 1` rejection.
- `qualifire/storage/{delta,external_catalog,jdbc}_storage.py` —
  multi-row support + UUID-salted temp views (Spark) +
  parenthesized predicate (JDBC).
- `tests/test_backfill_max_partitions.py` (new, 4 cases).
- `tests/test_backfill_partition_column_autodetect.py` (new, 10 cases).
- `tests/test_backfill_parallelism.py` (new, 9 cases).
- `tests/test_storage/test_bulk_history_contract.py` (new,
  6 SQLite + 2 ExternalCatalog cases).
- `tests/test_storage/test_bulk_suppression.py` — added T3.7
  (JDBC fallback at limit=3) + parenthesization regression.
- `tests/test_cli_backfill.py` — added stderr-routing tests.
- `docs/CHANGELOG.md` — Enhancement entry + Compatibility notes
  A and B.
- `docs/backfill_and_soft_delete.md` — `--parallelism` /
  `--max-partitions` / suppression / SQLite caveat / Ctrl-C
  contract sections.
- `examples/backfill_quickstart.py` — comment for new kwargs.
- `docs/features/metrics-backfill-and-soft-delete/shipped.md` —
  Item 4 doc-drift fix.

## Testing

- **1466 tests passing** locally before merge (up from 1432 on
  `main` + 28 new tests + 6 added cases on existing files).
- New coverage:
  - 9 parallelism / engine-once / observability cases.
  - 10 partition_column auto-detect cases.
  - 4 max_partitions cases.
  - 6 SQLite + 2 ExternalCatalog bulk-history contract cases.
  - JDBC fallback at limit=3 + predicate-parens regression.
- All four storage backends pass the existing soft-delete /
  suppression / dedup tests.

## Review Cycles

### Plan
6 codex plan-review rounds (R1-R5 FAIL, R6 PASS). 2 adversarial
self-passes folded into v2 / v4. Final plan covers HS1 carve-outs
A/B as intentional behavior changes.

### Implementation
3 codex impl-review rounds:
- R1: 1 BLOCKER (per-metric tombstone partial-failure) + 1 HIGH
  (JDBC predicate parens) + 1 MEDIUM (cross-backend coverage) +
  1 LOW (legacy fallback) → fixed in `d01cdbb`.
- R2: 1 BLOCKER (write_results not actually atomic at the
  Python sqlite3 layer) + 1 MEDIUM (cross-backend on contract
  file) → fixed in `94e4733`.
- R3: PASS with 2 LOW polish (importorskip placement, exact
  ordered assertion) → fixed in `eb1230f`.

## Compatibility (Repeated for Reference)

### Carve-out A — Engine call count
With the engine-once refactor, `Qualifire.backfill(...)` invokes
the underlying engine **once per anchor** instead of once per
metric in scope. Per-anchor write/notification counts drop
proportionally. The per-metric `BackfillReport.partitions`
shape is unchanged; just the side-effect counts.

### Carve-out B — `read_validation_history_bulk(limit=1)`
Multi-row support brings the bulk path's tombstone semantics
into line with `read_validation_history`. A tombstone for
partition P now hides only P (not sibling partitions for the
same key). Affects the notification-suppression caller —
per-partition rather than per-key surfaces.

## Follow-ups Captured

- [`backfill-severity-before-broken-readback`](../backfill-severity-before-broken-readback/idea.md)
  — pre-existing `_read_severity_before(validation_name="")`
  bug surfaced by codex R4 (always returns None today). HS8
  scoped OUT of this feature; picks up in its own iteration.

## Plan PR

[#19](https://github.com/amitranjan-oracle/qualifire/pull/19) —
`feat(backfill-followups-and-polish): plan + impl`
