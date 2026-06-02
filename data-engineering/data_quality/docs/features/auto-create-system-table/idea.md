---
id: auto-create-system-table
name: Auto-Create System Table
type: Feature
priority: P0
effort: Small
impact: High
created: 2026-04-01
blocked_by: system-table-collected-data
---

# Auto-Create System Table

## Problem Statement
When the system table does not yet exist — such as on a first run or in a fresh environment — the pipeline has no way to persist collection and validation results. If this scenario is not handled, the system will fail at runtime when attempting to write results.

The behavior needs to differ by backend:

- **External catalog** (e.g., Oracle, Postgres): The system table should be created with the correct schema and empty content if it doesn't already exist.
- **SQLite**: The system table must be created before any collection or validation results are saved, since there is no pre-existing database to fall back on.

Without automatic table creation, users would need to manually set up the system table before their first data quality run — a poor onboarding experience and a common source of setup errors.

## Affected Areas
- storage/persistence (table existence check and DDL for both SQLite and external catalogs)
- pipeline/orchestration (ensure table exists before writing results)
- configuration (backend-specific creation logic)
