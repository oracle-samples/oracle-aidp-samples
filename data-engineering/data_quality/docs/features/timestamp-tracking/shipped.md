---
shipped: 2026-04-01
---

# Shipped: Timestamp Tracking for Collection, Validation, and Notification

## Summary
Added three phase-level timestamp columns (`collected_at`, `validated_at`, `notified_at`) to the system table, enabling sub-daily run distinction and pipeline phase timing observability. Previously only `run_timestamp` existed (set once at dataset start).

## Key Changes
- Added `collected_at`, `validated_at`, `notified_at` columns to system table schema across all 4 backends (external_catalog, delta, sqlite, jdbc)
- Added `validated_at` field to `ValidationResult` model, captured after `validator.validate()` completes
- Added `notified_at` field to `NotificationResult` model, captured after `notifier.send()` completes
- `collected_at` (already on `CollectionResult`) now persisted to system table
- Each timestamp is NULL for inapplicable record types (e.g., validation rows have NULL `notified_at`)

## Files Changed
- `qualifire/storage/base.py` — `SYSTEM_TABLE_COLUMNS` and `SYSTEM_TABLE_DDL`
- `qualifire/storage/sqlite_storage.py` — SQLite DDL
- `qualifire/storage/delta_storage.py` — Delta DDL
- `qualifire/core/models.py` — `ValidationResult.validated_at`, `NotificationResult.notified_at`
- `qualifire/core/engine.py` — Timestamp capture in `_run_single_validation()`, `_send_grouped_notifications()`, `_persist_results()`
- `tests/test_engine.py` — 5 new tests

## Testing
- 390 total tests pass (5 new for this feature)
- Verified collected_at, validated_at, notified_at are persisted to SQLite system table
- Verified timestamps are NULL for inapplicable record types
- Verified validated_at is populated on all ValidationResult objects

## Notes
- Schema change is additive — existing system tables will have NULL for new columns until next run. Users with existing tables should ALTER or recreate them.
- No migration script provided; `CREATE TABLE IF NOT EXISTS` won't alter existing tables.
