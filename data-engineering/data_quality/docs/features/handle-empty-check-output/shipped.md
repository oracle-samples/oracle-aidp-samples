---
shipped: 2026-04-01
---

# Shipped: Handle Empty Check Output

## Summary
Added configurable `on_empty_data` field to all validation configs so users control what happens when a check has no data to validate. Default is `"warning"` — surfaces the issue without breaking pipelines.

## Key Changes
- Added `on_empty_data: Literal["pass", "warning", "error"]` to all 5 validation config classes (SLO, threshold, drift, trend, shape)
- Added `on_empty_data` parameter to `Validator` base class with `_empty_data_result()` helper
- Engine-level empty-output detection in `_run_single_validation` — catches validators that return empty result lists
- Isolation forest, forecast, and historical validators now use configurable severity instead of hardcoded PASS for insufficient data
- 15 new tests covering all `on_empty_data` values, engine-level detection, and config parsing

## Files Changed
- `qualifire/core/config.py`
- `qualifire/core/engine.py`
- `qualifire/validation/base.py`
- `qualifire/validation/isolation_forest.py`
- `qualifire/validation/forecast.py`
- `qualifire/validation/historical.py`
- `qualifire/validation/slo.py`
- `qualifire/validation/threshold.py`
- `tests/test_config.py`
- `tests/test_engine.py`
- `tests/test_validation/test_forecast.py`
- `tests/test_validation/test_historical.py`
- `tests/test_validation/test_isolation_forest.py`

## Testing
- 376 tests pass (15 new, 0 failures)
- Tests cover all three `on_empty_data` values for each affected validator
- Engine-level empty-output detection tested with mock validators
- Config parsing and YAML round-trip tested
- Security review passed (0 critical/high/medium issues)

## Notes
- Default `"warning"` is a behavior change from the previous implicit PASS — non-breaking since only ERROR triggers `QualifireValidationError`
- Users who want the old behavior can set `on_empty_data: "pass"`
