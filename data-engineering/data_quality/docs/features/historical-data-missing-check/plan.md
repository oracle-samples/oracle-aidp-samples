---
started: 2026-04-01
---

# Implementation Plan: Historical Data Missing Check

## Overview
Add a configurable `on_missing_history` option to all history-dependent validators (drift, trend, shape) so users can choose what happens when historical data is insufficient. The default `"ignore"` mode uses whatever data is available within the lookup window and proceeds with validation — only skipping when comparison is truly impossible. Modes `"warn"` and `"error"` surface the issue explicitly. All cold-start results are tagged with `"cold_start": true` in details for downstream tracking.

## Implementation Steps
- [x] Step 1: Add `on_missing_history` field to config models (`HistoricalCompareConfig`, `ForecastModelConfig`, `AnomalyModelConfig`)
- [x] Step 2: Update `HistoricalValidator` to use `on_missing_history` from rule config, tag cold-start results with `cold_start: true`
- [x] Step 3: Update `ForecastValidator` to use `on_missing_history`, lower Prophet minimum to 2 in ignore mode, tag cold-start results
- [x] Step 4: Update `IsolationForestValidator` to accept and use `on_missing_history` parameter, tag cold-start results
- [x] Step 5: Update `QualifireEngine._validate()` to pass `on_missing_history` to `IsolationForestValidator`
- [x] Step 6: Add tests for on_missing_history="warn" and "error" on all three validators
- [x] Step 7: Add tests for cold_start details tag and "ignore" mode best-effort behavior
- [x] Step 8: Update check documentation (shape_check.md, trend_check.md) with new config option

## Technical Decisions

### Config option: `on_missing_history`
- `"ignore"` (default): Use whatever historical data is available within the lookup window. Proceed with validation using available data. Only skip validation when comparison is truly impossible (0 past values for drift, <2 points for trend, 0 past periods for shape). When skipped, return PASS with `cold_start: true` tag.
- `"warn"`: When historical data is insufficient for validation, return WARNING.
- `"error"`: When historical data is insufficient for validation, return ERROR.

### Config placement
- **Drift**: `on_missing_history` in `HistoricalCompareConfig` (per-rule, under `compare:`)
- **Trend**: `on_missing_history` in `ForecastModelConfig` (per-rule, under `model:`)
- **Shape**: `on_missing_history` in `AnomalyModelConfig` (per-validation, under `model:`)

### Behavioral changes per validator
- **Drift**: Already uses available values with `missing_strategy: "ignore"`. The new `on_missing_history` controls the severity when there are truly 0 past values. `missing_strategy` continues to control fill behavior for partial data.
- **Trend (Prophet)**: With `on_missing_history: "ignore"`, lower Prophet minimum from 10 to 2 points. Attempt forecasting with whatever data exists. With `"warn"`/`"error"`, surface insufficient history at the original 10-point threshold.
- **Shape**: With `on_missing_history: "ignore"`, use whatever past periods are available. Only skip when 0 past periods exist.

### Cold-start tagging
- All results where validation was skipped or had insufficient history include `details={"cold_start": True}`.
- This applies regardless of the `on_missing_history` setting, enabling programmatic tracking.

## Files to Modify

| File | Change |
|------|--------|
| `qualifire/core/config.py` | Add `on_missing_history` to 3 config models |
| `qualifire/validation/historical.py` | Use config for severity + tag details |
| `qualifire/validation/forecast.py` | Use config for severity, lower Prophet min in ignore mode + tag details |
| `qualifire/validation/isolation_forest.py` | Accept param, use for severity + tag details |
| `qualifire/core/engine.py` | Pass `on_missing_history` to IsolationForestValidator |
| `tests/test_validation/test_historical.py` | Add on_missing_history tests |
| `tests/test_validation/test_forecast.py` | Add on_missing_history tests |
| `tests/test_validation/test_isolation_forest.py` | Add on_missing_history tests |
| `docs/shape_check.md` | Document new option |
| `docs/trend_check.md` | Document new option |

## Testing Strategy
- Unit tests for each validator with `on_missing_history="warn"` and `"error"` verifying correct severity
- Unit tests verifying `cold_start: true` appears in details for all cold-start scenarios
- Unit tests for "ignore" mode verifying validators proceed with partial data
- Existing tests remain unchanged (default "ignore" preserves current PASS behavior)

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking existing configs | Default is "ignore" — identical to current behavior |
| Prophet with few data points gives poor predictions | Users opt in to this; "ignore" is explicit intent to accept lower accuracy |
| Missing the cold_start tag in edge cases | Test all code paths (0 data, partial data, sufficient data) |
