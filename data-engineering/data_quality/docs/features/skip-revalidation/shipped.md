---
shipped: 2026-05-10
---

# Shipped: skip-revalidation

## Summary

Runtime `skip_revalidation: bool = False` flag short-circuits the
validator dispatch when active validation rows already exist at
the dataset's resolved partition. Replays persisted verdicts as
`ValidationResult`s with `details["from_cache"] = True` instead
of re-running the validator's compute (Prophet fit, RandomForest
cross-validation, etc.). Faster retries / cheaper backfill
replays / reproducible CI snapshots.

## Key Changes

- **All 4 storage backends** (`SQLiteStorage`, `DeltaStorage`,
  `ExternalCatalogStorage`, `JDBCStorage`):
  `read_validations_at_partition(*, dataset_name,
  validation_name_prefix, partition_ts) -> list[dict]`. Two-stage
  SQL with ROW_NUMBER per `(validation_name, metric_name,
  dimension_value)` ORDER BY `run_timestamp DESC`; post-dedup
  `is_active='true'` filter. Prefix matches exact OR `prefix.%`
  with `ESCAPE '\\'` (codex impl-review R1 MAJOR — unescaped
  `_` in `LIKE` would cause cross-validation cross-matches when
  validation names contain underscores).
- **`qualifire/storage/base.py`**: protocol method added.
- **`qualifire/core/context.py`**: `skip_revalidation: bool =
  False` attribute.
- **`qualifire/api.py`**: `skip_revalidation` kwarg on
  `run_config`, `run_config_parsed`, `backfill`, `validate`
  (mirrors `skip_if_cached`'s actual surface — NOT on
  `validate_query` or `write_audit_publish`, which is consistent
  with the existing pattern). Sets `ctx.skip_revalidation`.
- **`qualifire/core/backfill.py`**: threaded through
  `run_backfill` → `_process_anchor` → `_run_anchor_once` (BOTH
  serial and parallel call sites).
- **`qualifire/cli.py`**: `--skip-revalidation` argparse flag on
  `run` and `backfill` subcommands.
- **`qualifire/core/engine.py:_validate`**: NEW pre-pass
  `_try_skip_revalidation(ds_config, val_config, base_name)`
  invoked BEFORE the validator branch. Returns `None` (fall
  through) when the flag is off, storage is missing, no anchor
  resolves, or no rows are found. Otherwise returns the
  rebuilt synthetic `ValidationResult`s.
  - `_row_to_validation_result(row, base_name)` helper performs
    explicit per-field rebuild: `Severity(row["validation_status"])`
    enum parse, JSON-decode `expected_value` and `details_json`,
    derive `validation_base_name` from current `val_config`
    (NOT in persisted row — the system table has no
    `validation_base_name` column).
- **Tests** — `tests/test_skip_revalidation.py`: 13 tests
  covering T1 (replay persisted row), T2 (default-False
  re-validates), T3 (no row → fall through), T4 (tombstoned
  row → fall through), T6 (parallel backfill plumbing), T8
  (`from_cache` marker), 3 row-rebuild helper tests, 3
  storage-helper contract tests including the LIKE-escape
  regression for underscored prefixes.
- **`tests/test_cli.py`**: extended fake `run_config_parsed`
  stub with `skip_revalidation=False`.
- **`docs/CHANGELOG.md`**: Enhancement entry.

## Files Changed

11 files; +538 / −7 lines.

## Plan PR

[#24](https://github.com/amitranjan-oracle/qualifire/pull/24).

## Review Cycles

Plan: 1 adversarial + 4 codex rounds. Codex R1 found 2 BLOCKERs
+ 2 MAJORs + 1 MEDIUM (singular helper signature, missing
synthetic-rebuild detail, anchor resolution algorithm,
sample-validator partition trail, plumbing scope mismatch with
`skip_if_cached`). R2 found 2 contradictions in v3 (singular
Locked Decision 6 vs plural Storage section; parameter name vs
SQL column name). R3 found Risks-table residual singular names.
R4 PASS at v5.

Implementation: 2 codex rounds. R1 found a MAJOR — unescaped
`_` in `LIKE prefix.%` patterns across all 4 backends causes
cross-validation row leakage when validation names contain
underscores (which `_SAFE_NAME_RE` allows). Fixed across all 4
backends with `\\` / `%` / `_` escaping + `ESCAPE '\\'` clause;
added regression test pinning the underscore case. R2 PASS.

## Local Test Results

- 1517 passed, 2 skipped (+13 new tests; up from 1504).
- No regressions; full suite green.
