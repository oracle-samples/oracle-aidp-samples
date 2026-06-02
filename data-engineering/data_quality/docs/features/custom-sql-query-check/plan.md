---
started: 2026-04-01
---

# Implementation Plan: Custom SQL Query Check

## Overview

Add a dataset-level `query` field to `DatasetConfig` as an alternative to `table`. When set, the engine materializes the query result as a Spark temp view (or in-memory SQLite table for Pandas) and passes that view name to all downstream collectors. This reuses the entire existing pipeline with minimal code changes — the same pattern WAP already uses with its staging table.

Key insight: The existing `custom_query` **collection type** runs SQL within a single validation block. This feature adds a **dataset-level query source** so all validation types (threshold, drift, trend, shape, SLO) can operate on the query results.

## Implementation Steps

- [x] Step 1: Add `query`, `dimensions`, `measures` fields to `DatasetConfig` in `config.py`
  - Add `query: str | None = None`
  - Add `dimensions: list[str] | None = None`
  - Add `measures: list[str] | None = None`
  - Update `_require_table_or_wap` validator to accept `query` as a third option
  - Validate mutual exclusivity: exactly one of `table`, `wap`, or `query` must be set
  - Rename validator to `_require_source` for clarity

- [x] Step 2: Add `_materialize_query()` method to `QualifireEngine` in `engine.py`
  - Render the SQL query through Jinja context (`self.context.render()`)
  - Execute via `self.backend.execute_sql(rendered_sql)`
  - Register result as temp view: `_qf_{dataset_name}_{run_id[:8]}`
  - Return the temp view name
  - On failure, raise with the rendered SQL in the error message

- [x] Step 3: Add `_validate_query_schema()` method to `QualifireEngine`
  - If `dimensions` or `measures` are declared, check the temp view's columns
  - For each declared column missing from the result, produce an ERROR-severity `ValidationResult`
  - Return the list of schema validation results (empty if all columns present or no declarations)

- [x] Step 4: Update `_run_dataset()` to handle query-based datasets
  - At the top, if `ds_config.query` is set:
    1. Call `_materialize_query()` to get the temp view name
    2. Call `_validate_query_schema()` — if errors, short-circuit (skip validations)
    3. Create a modified `ds_config` with `table=temp_view_name` for downstream use
    4. If `filter` is set alongside `query`, log a warning and ignore it
  - In `finally` block, drop the temp view if one was created
  - Ensure `ds_result.table` is set to the dataset name (not the temp view) for reporting
  - Caching: the existing `_cache_table()` already works with any table name, including temp views

- [x] Step 5: Update `_collect()` to resolve the effective table name
  - When `ds_config.query` is set and `ds_config.table` is the temp view, the current `table = ds_config.table or ""` already works
  - Store original query in `details_json` for persistence (truncated to 2000 chars)

- [x] Step 6: Update `_persist_results()` for query-based datasets
  - Ensure `table_name` in persisted rows uses the dataset name, not the temp view name
  - Add query text (truncated) to `details_json` for traceability

- [x] Step 7: Add `validate_query()` method to `Qualifire` in `api.py`
  - Follows same pattern as `validate()` but accepts `query` instead of `table`
  - Accepts optional `dimensions`, `measures`, `cache`, `cache_storage_level`
  - Builds a `DatasetConfig(query=sql, ...)` and delegates to the engine

- [x] Step 8: Write config validation tests in `tests/test_config.py`
  - `query` alone is valid (no `table` or `wap` needed)
  - `query` + `table` raises ValueError (mutual exclusivity)
  - `query` + `wap` raises ValueError
  - `dimensions` and `measures` are optional lists
  - Existing `table`-only and `wap`-only configs still pass

- [x] Step 9: Write engine tests in `tests/test_engine.py`
  - Query materialization creates temp view and passes it to collectors
  - Temp view is cleaned up after validations (even on error)
  - Schema validation catches missing dimension/measure columns
  - Schema validation passes when declarations match
  - Query execution error produces ERROR DatasetResult
  - `filter` alongside `query` is ignored with warning
  - Caching works with query-based datasets
  - Parallel validations work on query-based datasets

- [x] Step 10: Write API tests in `tests/test_api.py`
  - `validate_query()` creates correct config and runs engine
  - Works with all builder methods (threshold_check, drift_check, etc.)

- [x] Step 11: Add YAML example and update docs
  - New example: `docs/examples/custom_sql_query_check.yaml`
  - Update `docs/configuration.md` with `query`, `dimensions`, `measures` reference

## Technical Decisions

1. **Temp view approach** (not physical table): Temp views are cheap, automatically scoped to the Spark session, and don't require DDL permissions. Same approach WAP uses.

2. **Dataset-level query** (not new validation type): Adding `query` to `DatasetConfig` is far simpler than creating a new validation type. It leverages all existing collectors and validators without modification.

3. **Mutual exclusivity with table/wap**: A dataset is sourced from exactly one of: a table, a WAP pipeline, or a query. This prevents ambiguity.

4. **Dimension/measure validation is optional**: Teams that just want to run a query without declaring schema can do so. Schema declarations add an extra safety net.

5. **Temp view naming**: `_qf_{dataset_name}_{run_id[:8]}` avoids collisions even with parallel runs while being debuggable.

## Files to Modify

| File | Change |
|------|--------|
| `qualifire/core/config.py` | Add `query`, `dimensions`, `measures` to `DatasetConfig`; update validator |
| `qualifire/core/engine.py` | Add `_materialize_query()`, `_validate_query_schema()`; update `_run_dataset()`, `_persist_results()` |
| `qualifire/api.py` | Add `validate_query()` method |
| `tests/test_config.py` | Config validation tests |
| `tests/test_engine.py` | Engine materialization and lifecycle tests |
| `tests/test_api.py` | API integration tests |
| `docs/configuration.md` | Document new fields |
| `docs/examples/` | New example YAML |

## Files That Should NOT Change

| File | Reason |
|------|--------|
| `qualifire/collection/*.py` | All collectors already work with any table name |
| `qualifire/validation/*.py` | All validators consume `CollectionResult` — decoupled |
| `qualifire/backends/*.py` | `execute_sql()` already supports arbitrary SQL |
| `qualifire/notification/*.py` | Notifications consume `DatasetResult` — decoupled |
| `qualifire/storage/*.py` | Storage persists `ValidationResult` — decoupled |

## Testing Strategy

- **Unit tests**: Config model validation (mutual exclusivity, optional fields, defaults)
- **Unit tests**: Engine temp view lifecycle (create, use, cleanup on success and error)
- **Unit tests**: Schema validation (missing columns, matching columns, no declarations)
- **Integration tests**: Full pipeline with query-based dataset + threshold validation
- **Integration tests**: Full pipeline with query-based dataset + drift validation
- **Mock infrastructure**: Extend `_make_engine()` helper and `MockDataFrame` from `tests/conftest.py`

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Complex queries cause OOM when materialized | Document best practices (filter, limit scope). Cache controls help. |
| Confusion between dataset `query` and collection `custom_query` | Clear docs distinguishing: dataset `query` replaces the table; collection `custom_query` runs SQL within a validation block |
| Temp view name leaks into system table / notifications | Engine maps temp view back to dataset name for all external outputs |
| Pandas backend temp view compatibility | Pandas backend uses SQLite; register result as SQLite table. Test both backends. |
