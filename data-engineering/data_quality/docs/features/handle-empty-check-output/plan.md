---
started: 2026-04-01
---

# Implementation Plan: Handle Empty Check Output

## Overview

When a check runs but produces no output (empty query results, empty validator results), the system silently treats it as PASS. This creates false confidence that validations succeeded when no validation actually occurred. We add a configurable `on_empty_data` field so users decide what severity to use when a check has no data to validate. Default: `"warning"`.

## Implementation Steps

- [x] Step 1: Add `on_empty_data` field to all five validation config classes in `config.py`
  - Add `on_empty_data: Literal["pass", "warning", "error"] = "warning"` to each of: SLOValidationConfig, ThresholdValidationConfig, HistoricalValidationConfig, ForecastValidationConfig, AnomalyDetectionValidationConfig
  - Uses a string literal that maps to Severity enum at runtime

- [x] Step 2: Add `on_empty_data` to `Validator` base class and wire through engine
  - Add `on_empty_data: Severity = Severity.WARNING` param to `Validator.__init__`
  - Add a helper method `Validator._empty_result(name, type, message)` that builds a ValidationResult using `self.on_empty_data` severity
  - In `engine.py _validate()`, pass `on_empty_data` to each validator constructor (map string → Severity)

- [x] Step 3: Add engine-level empty-output detection in `_run_single_validation`
  - After `_validate()` returns, if `validation_results` is empty, create a ValidationResult with severity from `val_config.on_empty_data` and message "Check produced no validation output"
  - Catches all validators that return empty lists (e.g., SLO validator when no "recency" metric found)

- [x] Step 4: Update validators to use `self.on_empty_data` instead of hardcoded PASS
  - `isolation_forest.py:119` — empty current data or no past data → use `self.on_empty_data`
  - `forecast.py:111` — insufficient history (<10 points) → use `self.on_empty_data`
  - `historical.py:117` — no historical data → use `self.on_empty_data`

- [x] Step 5: Update existing tests for the new behavior
  - Update isolation_forest tests: `test_empty_current_returns_pass` → expect WARNING (new default)
  - Update forecast/historical tests if they test the insufficient-data paths
  - Ensure existing passing tests still pass (threshold, SLO handle missing metrics via ERROR already)

- [x] Step 6: Add new tests
  - Test engine-level empty-output detection (validator returns empty list → WARNING result)
  - Test `on_empty_data: "pass"` preserves old behavior
  - Test `on_empty_data: "error"` escalates to ERROR
  - Test config parsing with on_empty_data field

## YAML Example

```yaml
validations:
  - type: "shape"
    on_empty_data: "warning"  # default — surfaces insufficient data as WARNING
    collection:
      type: "sample"
      n_records: 10000

  - type: "threshold"
    on_empty_data: "error"    # strict — fail if no data to validate
    collection:
      type: "aggregation"
      expressions: ["COUNT(*) AS row_count"]
    rules:
      - metric: row_count
        thresholds:
          error: { min: 1 }
```

## Technical Decisions

- **Configurable per-validation**: Each check can have its own `on_empty_data` behavior since different checks may have different expectations about data availability.
- **Default WARNING**: Non-breaking — only ERROR triggers `QualifireValidationError`. Users get visibility without pipeline failures. Users who want silence can set `"pass"`.
- **Engine + validator dual check**: Engine catches empty result lists; validators use `on_empty_data` for their own insufficient-data early returns. Belt and suspenders.
- **No shared base config class**: Adding `on_empty_data` to each config individually (5 fields) is simpler than introducing a mixin, and matches the existing pattern of repeated fields (`notify`, `suppress_repeat_alerts`).

## Testing Strategy

- Unit tests for each modified validator with all three `on_empty_data` values
- Unit test for engine empty-output detection
- Config parsing tests for the new field
- Verify backward compatibility: default `"warning"` is the only behavior change

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Default WARNING is a behavior change | WARNING is non-breaking (no QualifireValidationError). Only affects notifications if `on_warning` channels configured |
| Five config classes to update | Mechanical change, same field on each — low risk |
