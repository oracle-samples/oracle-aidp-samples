---
started: 2026-04-01
---

# Implementation Plan: Auto-Create System Table

## Overview
Ensure the system table is automatically created with the correct schema on first run across all backends, and that existing tables are migrated to add any missing columns.

## Implementation Steps
- [ ] Step 1: Add `_ensure_columns()` to SQLiteStorage to ALTER TABLE ADD COLUMN for missing columns
- [ ] Step 2: Add `_ensure_columns()` to ExternalCatalogStorage and DeltaStorage (Spark SQL ALTER TABLE)
- [ ] Step 3: Add auto-create DDL to JDBCStorage `initialize()` (currently only checks connectivity)
- [ ] Step 4: Add a shared `COLUMN_DEFINITIONS` mapping in base.py for column name → type
- [ ] Step 5: Add tests for schema migration and JDBC auto-create
- [ ] Step 6: Run full test suite

## Technical Decisions
- `_ensure_columns()` called from `initialize()` after CREATE TABLE IF NOT EXISTS
- SQLite: `ALTER TABLE ADD COLUMN` for each missing column
- Spark SQL (External Catalog, Delta): `ALTER TABLE ADD COLUMNS` for missing columns
- JDBC: Uses Spark DataFrame write with `mode("append")` which auto-creates if DB allows; fallback to explicit DDL
- Column definitions shared from base.py to avoid drift

## Testing Strategy
- SQLite: Test migration from old schema (missing columns) to new schema
- Test that `initialize()` is idempotent (calling twice is safe)
- Test that write works after migration

## Risks & Mitigations
- Some external catalogs may not support ALTER TABLE — log warning but don't fail
- JDBC databases vary in DDL support — use Spark JDBC writer auto-create as primary strategy
