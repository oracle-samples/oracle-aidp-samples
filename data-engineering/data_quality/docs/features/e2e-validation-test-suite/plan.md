---
started: 2026-04-01
---

# Implementation Plan: End-to-End Validation Test Suite

## Overview
Build a comprehensive E2E test that generates realistic data with 30+ daily partitions and weekly seasonality, loads it into a PandasBackend, runs all 5 validation types through the full engine pipeline, persists results to SQLite, and verifies legitimate pass/fail outcomes with explanations.

## Implementation Steps
- [x] Step 1: Create data generator — 45 days of daily sales data with weekday/weekend seasonality, trend, and intentional anomalies
- [x] Step 2: Create dimension/fact tables for multi-table custom SQL testing
- [x] Step 3: Build E2E test class with SLO validation (pass + fail scenarios)
- [x] Step 4: Add threshold validation (pass + fail scenarios)
- [x] Step 5: Add drift/historical validation (requires seeding storage with past runs)
- [x] Step 6: Add trend/forecast validation (requires 30+ days of history in storage)
- [x] Step 7: Add anomaly/shape validation (pass with normal data, fail with injected anomalies)
- [x] Step 8: Add query-based dataset test with dimensions and measures
- [x] Step 9: Verify system table persistence (collection + validation rows)
- [x] Step 10: Run full suite — 27 E2E tests pass, 458 total pass

## Technical Decisions
- Use BOTH PandasBackend AND SparkBackend (parametrized tests)
- Use SQLiteStorage(":memory:") for persistence
- Generate data programmatically with numpy (deterministic seed)
- Seed historical data in SQLite for drift/trend validators
- Anomaly test: inject outlier records into one period to trigger legitimate failure
- No gaming: thresholds set to match genuine data patterns

## Data Design
- **sales_daily**: 45 days x ~200 rows/day. Weekday avg=200, weekend avg=100. Gradual 5% uptrend.
- **sales_anomaly**: Same as sales_daily but current period has 50% corrupted values (legitimate anomalies).
- **products** (dimension table): 10 products with categories for JOIN tests.
- **Timestamps**: Recent data (hours ago) for SLO pass, stale data (days ago) for SLO fail.

## Testing Strategy
- Each validation type has at least one PASS and one FAIL test
- All outcomes are driven by data patterns, not threshold gaming
- Results include explanations verifiable against generated data
- System table persistence verified after each run

## Risks & Mitigations
- Prophet may not be installed → skip trend tests with `pytest.importorskip("prophet")`
- sklearn/shap may not be installed → skip anomaly tests with `pytest.importorskip`
- Pandas SQL limitations → use simple SQL compatible with pandasql
