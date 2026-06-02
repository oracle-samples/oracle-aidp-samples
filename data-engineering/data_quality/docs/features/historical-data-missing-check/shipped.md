---
shipped: 2026-04-01
---

# Shipped: Historical Data Missing Check

## Summary
Added configurable `on_missing_history` option to all history-dependent validators (drift, trend, shape) so users can control behavior when historical data is insufficient for comparison. Default `"ignore"` mode uses whatever data is available and proceeds with validation. Modes `"warn"` and `"error"` surface the issue explicitly. All cold-start results are tagged with `cold_start: true` in details for downstream tracking.

## Key Changes
- Added `on_missing_history: Literal["ignore", "warn", "error"]` to `HistoricalCompareConfig`, `ForecastModelConfig`, and `AnomalyModelConfig`
- HistoricalValidator uses `on_missing_history` to control severity when zero past values exist
- ForecastValidator lowers Prophet minimum from 10 to 2 points in `"ignore"` mode, enabling forecasting with sparse data
- IsolationForestValidator separates empty-current-data (governed by `on_empty_data`) from no-past-data (governed by `on_missing_history`)
- Engine passes `on_missing_history` from config to IsolationForestValidator
- All cold-start results include `details={"cold_start": True}` for programmatic tracking

## Files Changed
- `qualifire/core/config.py`
- `qualifire/validation/historical.py`
- `qualifire/validation/forecast.py`
- `qualifire/validation/isolation_forest.py`
- `qualifire/core/engine.py`
- `tests/test_validation/test_historical.py`
- `tests/test_validation/test_forecast.py`
- `tests/test_validation/test_isolation_forest.py`
- `docs/trend_check.md`
- `docs/shape_check.md`
- `docs/validators.md`

## Testing
- 385 tests pass (4 new tests added, 7 existing tests updated)
- Tests cover all three `on_missing_history` modes for all three validators
- Tests verify `cold_start: true` tagging in details
- Tests verify `"ignore"` mode best-effort behavior (partial data usage)
- Security review passed with no blocking issues

## Notes
- Default is `"ignore"` for full backward compatibility — no existing configs need changes
- The `on_missing_history` config is separate from the existing `missing_strategy` on drift checks (which controls fill behavior for partial data)
- Prophet minimum of 2 in ignore mode means predictions with very few points may be less accurate — this is by design since users explicitly opt in
