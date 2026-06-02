---
shipped: 2026-05-10
---

# Shipped: configuration-reference-audit

## Summary

Audit + extend `docs/configuration.md` from 665 → ~1100 lines.
Adds field-reference tables (name / type / default / accepted
forms), worked YAML examples, and common-pitfalls subsections
for the 10 highest-priority Pydantic config surfaces +
nested sub-types. Cross-links to the just-shipped class
docstrings from `pydantic-docstring-audit` as primary source.

## Key Changes

- **`docs/configuration.md`** — 10 scope items audited:
  - `QualifireConfig` (root, new tables)
  - `DatasetConfig`
  - `SLOValidationConfig` (+ `RecencyCollectionConfig` summary)
  - `ThresholdValidationConfig` (+ `ThresholdRuleConfig` +
    `ThresholdLevels` + `ThresholdBounds` shared vocabulary)
  - `HistoricalValidationConfig` (drift, + `HistoricalRuleConfig`
    + `HistoricalCompareConfig` + `HistoricalThresholds`)
  - `ForecastValidationConfig` (+ `ForecastRuleConfig` +
    `ForecastModelConfig`)
  - `AnomalyDetectionValidationConfig` (+ `AnomalyModelConfig`)
  - `PatternValidationConfig` (+ `PatternModelConfig` +
    `PatternThresholdConfig`)
  - `AggregationCollectionConfig`
  - `SampleCollectionConfig` (+ `SampleHistoryConfig`)
- Coverage footer naming what's NOT covered (deferred to
  `configuration-reference-collectors-extension`).
- **`docs/collectors/aggregation.md`** + **`docs/collectors/README.md`**:
  fixed stale `list[str]` `expressions:` examples and the
  "Missing `AS <name>`" pitfall — the dict form is the only
  accepted shape; the list form was rejected at config load
  months ago.
- **`qualifire/core/config.py:DatasetConfig`**: corrected the
  class docstring's "mutually exclusive — pick one per
  dataset" claim. `table` + `df` actually coexist (df supplies
  data, table supplies identity); query and wap remain
  mutually exclusive with everything.
- **`docs/CHANGELOG.md`**: Documentation entry.

## Files Changed

5 files; +517 / −22 lines.

## Plan PR

[#30](https://github.com/amitranjan-oracle/qualifire/pull/30).

## Review Cycles

Plan: 3 codex rounds.
- R1: scope-gap blocker — root config + sub-Pydantic types not
  named.
- R2: section-count + notification-channel naming consistency
  fixes.
- R3 PASS at v3.

Implementation: 4 codex rounds, **18 factual errors caught
total** before merge.
- R1: 13 errors — wrong field-required claims; non-existent
  aliases / fabricated defaults; mis-attributed nesting
  (drift_breakdown_by_slice on validation vs model);
  fabricated SLO threshold key constraints; safe-name
  validator omissions; missing PatternModelConfig constraint
  notes.
- R2: 5 more — `partition_step` documented as validator
  history cadence (actually backfill range cadence; validators
  read per-rule); `n_estimators` constraint off (`>= 1` not
  `>= 10`); required `step` fields shown as having defaults
  in YAML examples; collectors docs still teaching rejected
  list-form; `DatasetConfig` docstring still saying mutually
  exclusive.
- R3: 1 leftover — `AS <name>` pitfall language in
  collectors/aggregation.md inconsistent with dict-key
  contract.
- R4 PASS.

## Local Test Results

- 1518 passed, 2 skipped (docs-only; no behavior change).
- docs-lint test (just shipped) confirms all new internal
  links resolve.
