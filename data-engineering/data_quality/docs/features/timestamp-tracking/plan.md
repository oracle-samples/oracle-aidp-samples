---
started: 2026-04-01
---

# Implementation Plan: Timestamp Tracking for Collection, Validation, and Notification

## Overview

Add three phase-level timestamp columns (`collected_at`, `validated_at`, `notified_at`) to the system table. Currently only `run_timestamp` exists (set once at dataset start), making it impossible to distinguish intra-day runs or observe pipeline phase timing.

The timestamps are captured at the engine level:
- `collected_at` — already on `CollectionResult.collected_at` (set by each collector); just needs to be persisted
- `validated_at` — captured right after `validator.validate()` returns
- `notified_at` — captured right after `notifier.send()` returns

## Implementation Steps

- [x] Step 1: Add columns to system table schema in `storage/base.py`
  - Add `collected_at`, `validated_at`, `notified_at` to `SYSTEM_TABLE_COLUMNS` list
  - Add three `TIMESTAMP` columns to `SYSTEM_TABLE_DDL`

- [x] Step 2: Update SQLite storage DDL in `storage/sqlite_storage.py`
  - Add three `TEXT` columns to the CREATE TABLE statement (SQLite uses TEXT for timestamps)

- [x] Step 3: Update Delta storage DDL in `storage/delta_storage.py`
  - Add three `TIMESTAMP` columns to the CREATE TABLE statement

- [x] Step 4: Add `validated_at` field to `ValidationResult` in `models.py`
  - `validated_at: datetime | None = None`

- [x] Step 5: Add `notified_at` field to `NotificationResult` in `models.py`
  - `notified_at: datetime | None = None`

- [x] Step 6: Capture `validated_at` in engine `_run_single_validation()`
  - After `validator.validate(collected)` returns, set `validated_at=datetime.now()` on each result

- [x] Step 7: Capture `notified_at` in engine `_send_grouped_notifications()`
  - After `notifier.send()` returns, set `nr.notified_at = datetime.now()` (NotificationResult already returned)

- [x] Step 8: Update `_persist_results()` to write the three timestamp columns
  - Validation rows: `collected_at` from earliest `CollectionResult.collected_at` in the dataset, `validated_at` from `vr.validated_at`
  - Collection rows: `collected_at` from `cr.collected_at`
  - Notification rows: `notified_at` from `nr.notified_at`
  - All three columns are NULL for record types where they don't apply

- [x] Step 9: Write tests
  - System table persistence includes the new timestamp columns
  - `validated_at` is populated on validation results
  - `notified_at` is populated on notification results
  - `collected_at` from CollectionResult is persisted
  - Existing tests continue to pass

- [x] Step 10: Update docs
  - Update `docs/configuration.md` system table section (if any) with new columns

## Technical Decisions

1. **Additive schema change**: New columns are NULLable. Existing system tables won't break — they'll have NULL for the new columns until the next run. No migration needed.

2. **Engine-level capture** (not collector/validator): Timestamps are captured in `_run_single_validation()` and `_send_grouped_notifications()` rather than pushing timestamp logic into each collector/validator. This keeps the interfaces clean.

3. **`collected_at` already exists**: `CollectionResult.collected_at` is already populated by every collector. We just need to persist it to the system table.

4. **No `on_empty_data` column**: This is captured in `run_timestamp` since it's a single value rather than a per-record concern.

## Files to Modify

| File | Change |
|------|--------|
| `qualifire/storage/base.py` | Add columns to `SYSTEM_TABLE_COLUMNS` and `SYSTEM_TABLE_DDL` |
| `qualifire/storage/sqlite_storage.py` | Add columns to SQLite DDL |
| `qualifire/storage/delta_storage.py` | Add columns to Delta DDL |
| `qualifire/core/models.py` | Add `validated_at` to ValidationResult, `notified_at` to NotificationResult |
| `qualifire/core/engine.py` | Capture timestamps in `_run_single_validation()`, `_send_grouped_notifications()`, `_persist_results()` |
| `tests/test_engine.py` | Persistence tests for new columns |

## Files That Should NOT Change

| File | Reason |
|------|--------|
| `qualifire/collection/*.py` | Collectors already set `collected_at` |
| `qualifire/validation/*.py` | Validators don't need to know about timestamps |
| `qualifire/notification/*.py` | Notifiers return results; engine adds timestamp |
| `qualifire/core/config.py` | No config changes needed |

## Testing Strategy

- Extend existing `TestEnginePersist` and `TestEngineCollectionPersistence` with timestamp assertions
- Verify SQLite storage round-trip with new columns
- Verify all three record types have correct timestamps

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Existing system tables missing new columns | Columns are NULLable; existing backends use `CREATE IF NOT EXISTS` which won't alter schema. Document that users should recreate or ALTER existing tables. |
| JDBC storage auto-creates tables on write | Spark JDBC writer uses schema from DataFrame; new columns will be included automatically on new tables. |
