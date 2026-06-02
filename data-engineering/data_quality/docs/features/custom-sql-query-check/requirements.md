# Custom SQL Query Check -- Requirements

**Feature**: Custom SQL Query Check
**Priority**: P1 | **Effort**: Medium | **Impact**: High
**Date**: 2026-04-01

---

## 1. Problem Statement

| Dimension | Detail |
|-----------|--------|
| **Who** | Data engineers and analysts validating cross-table business metrics |
| **What** | No way to run a user-defined SQL query (joins, CTEs, subqueries) and pipe its results through the full validation pipeline |
| **Why** | Teams need to validate derived metrics (revenue = orders JOIN products), referential integrity, cross-dataset consistency, and complex business rules that cannot be expressed against a single table |
| **How now** | Users either skip these checks entirely or write ad-hoc scripts outside qualifire, losing notification/history/WAP integration |

## 2. Architecture Context (What Exists Today)

Before defining requirements, here is how the current system works -- this constrains the design:

**Dataset model**: Every dataset requires a `table` (or a `wap` block). The `table` is passed to collectors, which build SQL against it.

**Collection layer**: Five collector types exist (`aggregation`, `profiling`, `metrics`, `sample`, `custom_query`). Each collector receives a `table` name and a `Backend` instance, runs SQL, and returns `list[CollectionResult]`. The `custom_query` collector already accepts arbitrary SQL and an optional `dimension` column.

**Validation layer**: Five validators (`slo`, `threshold`, `drift`, `trend`, `shape`) consume `list[CollectionResult]` and produce `list[ValidationResult]`. Validators are completely decoupled from how data was collected.

**Engine flow**: For each dataset: cache table (optional) -> for each validation: collect -> validate -> aggregate results -> notify -> persist.

**Key observation**: The existing `custom_query` collection type already handles arbitrary SQL within a single validation block. What is missing is the ability to use a custom SQL query **as the dataset source itself** -- replacing `table` -- so that all validation types (threshold, drift, trend, shape) can operate on the query results without each needing its own embedded SQL.

## 3. Proposed Solution -- Dataset-Level Query Source

Introduce an optional `query` field on `DatasetConfig` as an alternative to `table`. When `query` is specified, the engine materializes the query result as a temp view and uses that view name as the effective `table` for all downstream collectors and validators.

This approach:
- Reuses the entire existing pipeline (collectors, validators, notifications, persistence)
- Requires minimal code changes (config model + engine materialization)
- Supports all existing validation types on the query results
- Follows the established pattern from WAP (which also creates a staging table/view)

### YAML Config Shape

```yaml
datasets:
  - name: "cross_table_revenue"
    query: |
      SELECT
        o.region,
        SUM(o.quantity * p.price) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS order_count,
        AVG(o.quantity * p.price) AS avg_order_value
      FROM catalog.schema.orders o
      JOIN catalog.schema.products p ON o.product_id = p.product_id
      WHERE o.order_date = '{{ ds }}'
      GROUP BY o.region
    dimensions:
      - region
    measures:
      - total_revenue
      - order_count
      - avg_order_value
    validations:
      - type: "threshold"
        collection:
          type: "aggregation"
          expressions:
            - "SUM(total_revenue) AS global_revenue"
            - "COUNT(*) AS region_count"
        rules:
          - metric: "global_revenue"
            thresholds:
              warning: { min: 100000 }
              error: { min: 50000 }
```

### How It Works (Engine Changes)

1. Engine sees `query` instead of `table` on the dataset
2. Engine renders the query via Jinja context
3. Engine executes the query via `backend.execute_sql()`
4. Engine registers the result as a temp view (e.g., `_qf_<dataset_name>_<run_id_short>`)
5. All collectors receive this temp view name as their `table` argument
6. After all validations complete, the temp view is dropped

## 4. User Stories

### US-1: Basic Custom SQL Query Check (Must)

```
As a data engineer
I want to define a SQL query as my dataset source instead of a table name
So that I can validate metrics derived from joins, CTEs, or subqueries

Acceptance Criteria:
- Given a dataset config with `query` instead of `table`,
  When the engine runs,
  Then the query is executed and its results are used as the dataset for all validations
- Given a query with Jinja variables ({{ ds }}, {{ today }}, custom context),
  When the engine renders the query,
  Then all variables are substituted before execution
- The query result must be usable by all existing collection types
  (aggregation, profiling, metrics, custom_query, sample)
- The query result must be usable by all existing validation types
  (threshold, drift, trend, shape)

Priority: Must | Value: 9 | Effort: M
```

### US-2: Dimension and Measure Declarations (Should)

```
As a data engineer
I want to declare which columns are dimensions and which are measures
So that the system can validate schema correctness and provide clearer
reporting/documentation

Acceptance Criteria:
- Given `dimensions` and `measures` lists on the dataset config,
  When the query executes,
  Then the engine validates that all declared columns exist in the result
- Given a missing declared column,
  When the query result is inspected,
  Then an ERROR-severity validation result is produced before any other checks run
- Dimension/measure declarations are optional -- omitting them skips
  the schema validation step

Priority: Should | Value: 5 | Effort: S
```

### US-3: Query Result Caching (Must)

```
As a data engineer running multiple validations on a complex join query
I want the query result to be cached for the duration of the validation run
So that the expensive query is not re-executed for each validation block

Acceptance Criteria:
- Given a dataset with `query` and `cache: true`,
  When the engine runs,
  Then the query result DataFrame is cached before validations and unpersisted after
- Given a dataset with `query` and `cache: false` (or omitted),
  When the engine runs,
  Then the query result is registered as a temp view but not cached
- Caching must respect the existing `cache_storage_level` setting

Priority: Must | Value: 7 | Effort: S
```

### US-4: Programmatic API Support (Should)

```
As a developer using the qualifire Python API
I want a builder method to create a custom SQL query dataset
So that I can run cross-table validations without YAML

Acceptance Criteria:
- Given the programmatic API (Qualifire class),
  When I call a method like `qf.validate_query(sql="...", validations=[...])`,
  Then the query-based dataset flow executes the same as YAML-driven
- The API must support all the same parameters: query, dimensions, measures,
  cache, validations

Priority: Should | Value: 6 | Effort: S
```

### US-5: Error Reporting with Query Context (Must)

```
As a data engineer debugging a failed validation
I want error messages to include the query source (not just a temp view name)
So that I can trace failures back to the originating SQL

Acceptance Criteria:
- Given a validation failure on a query-based dataset,
  When the result is persisted to the system table,
  Then the `table_name` field contains a meaningful identifier (dataset name),
  and the `details_json` includes the original query text (truncated if >2000 chars)
- Given a query execution failure (syntax error, missing table, timeout),
  When the engine catches the error,
  Then it produces an ERROR-severity DatasetResult with a clear message
  including the rendered SQL

Priority: Must | Value: 6 | Effort: S
```

## 5. Edge Cases and Error Handling

### Query Execution Failures

| Scenario | Expected Behavior |
|----------|-------------------|
| SQL syntax error | ERROR result with rendered SQL in message; other datasets continue |
| Referenced table does not exist | ERROR result; other datasets continue |
| Query returns zero rows | Proceed with empty result set; collectors handle empty data per their existing logic (aggregation returns nulls, sample returns empty, etc.) |
| Query timeout / resource exhaustion | ERROR result with original exception message; engine does not retry |
| Query returns extremely large result set | No automatic limit -- user is responsible for query scope. Document this risk. |

### Schema and Type Issues

| Scenario | Expected Behavior |
|----------|-------------------|
| Declared dimension/measure column missing from result | ERROR before running any validations |
| Column type incompatible with validator (e.g., string where numeric expected) | Existing validator error handling applies (ThresholdValidator raises on non-numeric) |
| Query returns duplicate column names | Backend-dependent behavior (Spark will suffix with `_1`); document that aliases should be unique |
| Query result column names contain special characters | Pass through as-is; metric names in rules must match exactly |

### Interaction with Existing Features

| Scenario | Expected Behavior |
|----------|-------------------|
| `query` + `table` both specified | Config validation error: must use one or the other |
| `query` + `wap` both specified | Config validation error: WAP requires a table target; query is a source, not a publish target |
| `query` + `filter` on dataset | `filter` is ignored (the query itself should contain all filtering); emit a WARNING log |
| `query` + `cache: true` | Supported -- cache the materialized query result |
| `query` with SLO validation | Supported but unusual -- SLO uses RecencyCollector which operates on the table; the temp view would be the target. Document that SLO recency on a query-based dataset checks recency of the query result, not the source tables. |
| Parallel validation on query dataset | Supported -- temp view is readable by multiple threads |

### Security and Safety

| Scenario | Expected Behavior |
|----------|-------------------|
| Query contains DDL/DML (DROP, INSERT, UPDATE) | No enforcement at the qualifire level -- backend permissions govern this. Document that queries should be SELECT-only. |
| Query with Jinja injection risk | Existing Jinja2 StrictUndefined mode prevents undefined variable expansion. No additional sanitization needed for the templating layer. |
| Temp view name collision | Use sufficiently unique name: `_qf_{dataset_name}_{run_id[:8]}` |

## 6. Dependencies and Risks

### Dependencies

| Dependency | Type | Impact |
|------------|------|--------|
| `Backend.execute_sql()` | Existing | Already supports arbitrary SQL -- no changes needed |
| Spark temp view registration | Existing | `df.createOrReplaceTempView()` -- well-established pattern |
| Pandas backend compatibility | Existing | PandasBackend must support `execute_sql()` that returns a DataFrame queryable by other collectors. Currently uses SQLite in-memory; the temp view pattern must translate to a registered table in that context. |
| Jinja context rendering | Existing | `QualifireContext.render()` already handles arbitrary template strings |

### Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Complex queries cause OOM when materialized | Medium | High | Document best practices (add filters, limit result size). Cache controls provide some guardrail. Consider an optional `max_rows` config. |
| Users confuse dataset-level `query` with collection-level `custom_query` | High | Low | Clear documentation distinguishing the two. Dataset `query` replaces the table; collection `custom_query` runs SQL within a validation block against a table. |
| Temp view name leaks into system table / notifications | Low | Low | Engine maps temp view name back to dataset name for all external-facing outputs |
| Pandas backend does not support temp view pattern | Medium | Medium | Test both backends. For Pandas, consider registering the result DataFrame as a named table in the in-memory SQLite database. |
| Query-based datasets cannot use WAP | Low | Low | Document this clearly. WAP is a write-then-validate pattern; query datasets are read-only analysis. These are complementary, not overlapping. |

## 7. Scope for Medium Effort Implementation

### Phase 1 -- In Scope (this release)

1. **Config model changes**
   - Add `query: str | None = None` to `DatasetConfig`
   - Add optional `dimensions: list[str]` and `measures: list[str]` to `DatasetConfig`
   - Update `_require_table_or_wap` validator to accept `table`, `wap`, or `query`
   - Validate mutual exclusivity: `query` cannot coexist with `table` or `wap`

2. **Engine changes**
   - New `_materialize_query()` method: render Jinja, execute SQL, register temp view
   - Update `_run_dataset()` to call `_materialize_query()` when `query` is set, then proceed with the temp view as the effective table
   - Ensure temp view cleanup in `finally` block
   - Optional dimension/measure column validation before running collectors
   - If `filter` is set alongside `query`, log a warning and ignore the filter

3. **Caching support**
   - Reuse existing `_cache_table()` / `_uncache()` with the temp view name
   - The existing cache-before-validate, uncache-after pattern applies unchanged

4. **Error handling**
   - Query execution errors -> ERROR DatasetResult with rendered SQL in details
   - Schema mismatch (missing dimension/measure columns) -> ERROR before validations

5. **Tests**
   - Unit tests: config validation (query vs table vs wap mutual exclusivity)
   - Unit tests: engine materialization and temp view lifecycle
   - Unit tests: dimension/measure validation
   - Integration test: query-based dataset with threshold validation
   - Integration test: query-based dataset with drift validation

6. **Documentation**
   - YAML config reference update with `query` field
   - Example YAML file (`docs/examples/custom_sql_query_check.yaml`)
   - Update `docs/configuration.md` with the new dataset source option

### Phase 2 -- Out of Scope (future)

- Programmatic API builder method (`validate_query()`) -- straightforward follow-up
- `max_rows` safety limit on query results
- Query cost estimation / dry-run mode
- Query result schema caching for faster config validation
- Multi-query datasets (multiple queries composing one logical dataset)
- Query-based datasets with WAP integration (write query results to a target)

## 8. Implementation Guidance

### Files to Modify

| File | Change |
|------|--------|
| `qualifire/core/config.py` | Add `query`, `dimensions`, `measures` to `DatasetConfig`; update validator |
| `qualifire/core/engine.py` | Add `_materialize_query()`; update `_run_dataset()` entry point |
| `qualifire/core/engine.py` | Update `_persist_results()` to use dataset name (not temp view) for `table_name` |
| `tests/test_config.py` | Config validation tests for new fields |
| `tests/test_engine.py` | Engine tests for query materialization and cleanup |
| `docs/configuration.md` | Document `query`, `dimensions`, `measures` fields |
| `docs/examples/` | New example YAML |

### Files That Should NOT Need Changes

| File | Reason |
|------|--------|
| `qualifire/collection/*.py` | All collectors already work with any table name |
| `qualifire/validation/*.py` | All validators consume `CollectionResult` -- decoupled from source |
| `qualifire/backends/*.py` | `execute_sql()` already handles arbitrary SQL |
| `qualifire/notification/*.py` | Notifications consume `DatasetResult` -- decoupled from source |
| `qualifire/storage/*.py` | Storage persists `ValidationResult` -- decoupled from source |

### Estimated Effort Breakdown

| Task | Effort |
|------|--------|
| Config model changes + validation | 2 hours |
| Engine materialization + cleanup | 3 hours |
| Dimension/measure column validation | 1 hour |
| Error handling and edge cases | 2 hours |
| Unit tests | 3 hours |
| Integration tests | 2 hours |
| Documentation and examples | 2 hours |
| **Total** | **~15 hours (Medium)** |

## 9. Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Adoption | 20% of configs use `query` within 3 months of release | System table query on `collection_type` |
| Error rate | Query-based datasets fail at the same rate as table-based ones (excluding query authoring errors) | System table analysis |
| Performance | Query materialization adds <5s overhead vs. equivalent manual temp view creation | Benchmark tests |
| Support tickets | No increase in support volume attributable to confusion between `query` and `custom_query` | Ticket tracking |

## 10. Latent Bug Found During Analysis

The engine at `qualifire/core/engine.py` line 334 passes `dimension=dimension` to `CustomQueryCollector`, and the collector does accept it (line 36 of `custom_query.py`). However, the `CustomQueryCollectionConfig` in `config.py` (line 106-111) declares a `dimension` field, and the engine reads it via `getattr(collection, "dimension", None)` at line 294. This is wired correctly.

No bug found -- the code is consistent. The `CustomQueryCollector.__init__` does accept `dimension` as a keyword argument (verified on second read).
