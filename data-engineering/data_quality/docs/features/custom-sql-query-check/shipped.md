---
shipped: 2026-04-01
---

# Shipped: Custom SQL Query Check

## Summary
Added a dataset-level `query` field to `DatasetConfig` as an alternative to `table`, enabling validation of metrics from multi-table joins, CTEs, and complex SQL. The query result is materialized as a temp view and all existing validators operate on it unchanged.

## Key Changes
- `DatasetConfig` now accepts `query`, `dimensions`, and `measures` fields (mutually exclusive with `table`/`wap`)
- Engine materializes query results as temp views with automatic lifecycle management (create, validate, cleanup)
- Optional schema validation: declared dimension/measure columns are checked against query result before running validations
- Query text (truncated) is persisted to system table for traceability
- New `validate_query()` programmatic API method
- Security hardening: SQL statement validation (SELECT-only), Jinja2 SandboxedEnvironment, dataset name validation, dimension/measure identifier validation

## Files Changed
- `qualifire/core/config.py` — DatasetConfig fields, validators
- `qualifire/core/engine.py` — `_materialize_query()`, `_validate_query_schema()`, `_drop_temp_view()`, `_validate_readonly_sql()`
- `qualifire/core/context.py` — Switched to SandboxedEnvironment
- `qualifire/api.py` — `validate_query()` method
- `tests/conftest.py` — MockDataFrame enhancements
- `tests/test_config.py` — Config validation tests
- `tests/test_engine.py` — Query lifecycle, schema, persistence, security tests
- `tests/test_api.py` — API integration tests
- `docs/configuration.md` — Query source documentation
- `docs/examples/custom_sql_query_check.yaml` — YAML example

## Testing
- 381 total tests pass (20 new tests for this feature)
- Config validation: mutual exclusivity, dimensions/measures, name safety
- Engine: materialization, schema validation, persistence, cleanup, error handling, parallel validations
- Security: DDL rejection, multi-statement rejection, SSTI prevention, identifier injection prevention
- API: validate_query() with all parameter combinations

## Notes
- Phase 2 (programmatic `validate_query()` builder, `max_rows` safety limit, query cost estimation) is deferred
- Pandas backend temp view path uses `register_table()` / `drop_table()` — works but no dedicated integration test
- Collection results from anomaly detection (DataFrames) are not persisted, as per existing behavior (only numeric values are saved)
