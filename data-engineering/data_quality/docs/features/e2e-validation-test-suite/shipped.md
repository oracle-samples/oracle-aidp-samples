---
shipped: 2026-04-01
---

# Shipped: End-to-End Validation Test Suite

## Summary
Comprehensive E2E test suite covering all 5 validation types (SLO, threshold, drift, trend, anomaly) with both PandasBackend and SparkBackend. Uses realistic generated data (45 days, weekday/weekend seasonality, uptrend) with legitimate pass/fail outcomes driven by data patterns. SQLite system table for persistence.

## Key Changes
- 27 E2E tests across 7 test classes
- Parametrized on both Pandas and Spark backends (24 dual-backend + 3 Pandas-only)
- Data generator: 45 days x ~200 rows/day with 0.5% daily growth, weekday/weekend seasonality
- Products dimension table for multi-table JOIN tests
- Prophet trend tests included (with history seeding)
- Isolation Forest anomaly tests with normal and corrupted data
- System table persistence verification (collection + validation rows)
- Multi-validation pipeline test (SLO + threshold combined)
- E2EPandasBackend wrapper for Spark-compatible `.collect()` on pandas results

## Files Changed
- `tests/test_e2e.py` — new, 500+ line E2E test suite

## Testing
- 27 E2E tests: 27 passed, 0 failed
- Full suite: 458 passed, 0 failed
- All 5 validation types have at least 1 pass and 1 fail scenario
- Both PandasBackend and SparkBackend exercised
- SQLite persistence verified with row-level assertions

## Test Coverage Summary

| Validation | Tests | Backends | Pass/Fail |
|-----------|-------|----------|-----------|
| SLO | 2 | Pandas + Spark | 1 pass, 1 fail |
| Threshold | 2 | Pandas + Spark | 1 pass, 1 fail |
| Drift | 3 | Pandas + Spark | 2 pass, 1 fail |
| Trend (Prophet) | 2 | Pandas + Spark | 1 pass, 1 fail |
| Anomaly (IsoForest) | 2 | Pandas | 1 pass, 1 fail |
| Query + JOIN | 1 | Pandas | 1 pass |
| Persistence | 2 | Pandas + Spark | 2 pass |
| Multi-validation | 1 | Pandas + Spark | 1 pass |

## Notes
- Anomaly tests run on PandasBackend only (SamplerCollector uses pandas `.query()` filter syntax)
- Query/JOIN tests run on PandasBackend only (pandasql handles JOIN materialization)
- Prophet and scikit-learn are now installed as dependencies
- All outcomes are data-driven with no threshold gaming
