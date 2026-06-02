---
shipped: 2026-04-01
---

# Shipped: Auto-Create System Table

## Summary
All 4 storage backends now auto-create the system table on first run and migrate existing tables by adding any missing columns. No manual setup required.

## Key Changes
- Added shared `COLUMN_DEFINITIONS` mapping in `storage/base.py` for column name → (Spark type, SQLite type)
- SQLiteStorage: Added `_ensure_columns()` using `PRAGMA table_info` + `ALTER TABLE ADD COLUMN`
- ExternalCatalogStorage: Added `_ensure_columns()` using `DESCRIBE TABLE` + `ALTER TABLE ADD COLUMNS`
- DeltaStorage: Added `_ensure_columns()` using `DESCRIBE TABLE` + `ALTER TABLE ADD COLUMNS`
- JDBCStorage: Added `_ensure_table()` that creates the table via empty DataFrame write if it doesn't exist
- Graceful fallback: all migration methods catch and log errors if ALTER TABLE isn't supported

## Files Changed
- `qualifire/storage/base.py` — added `COLUMN_DEFINITIONS` mapping
- `qualifire/storage/sqlite_storage.py` — added `_ensure_columns()`
- `qualifire/storage/external_catalog.py` — added `_ensure_columns()`
- `qualifire/storage/delta_storage.py` — added `_ensure_columns()`
- `qualifire/storage/jdbc_storage.py` — added `_ensure_table()`
- `tests/test_storage/test_sqlite.py` — 3 new tests (migration, idempotent, write-after-migration)
- `tests/test_storage/test_spark_storages.py` — updated 3 tests for new initialize behavior

## Testing
- 422 tests pass
- New SQLite tests verify: old-schema → new-schema migration, idempotent initialize, write-after-migration
- Spark storage tests updated to account for additional SQL calls from `_ensure_columns()`

## Notes
- External catalogs that don't support ALTER TABLE will log a warning but not fail
- JDBC table auto-creation depends on the database allowing Spark JDBC writer to create tables
- `COLUMN_DEFINITIONS` is the single source of truth for column types across all backends
