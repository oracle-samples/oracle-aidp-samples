---
shipped: 2026-04-21
---

# Shipped: Pattern Check Validation (Random Forest Two-Sample)

## Summary

Added a new `pattern` validation type — a Random Forest two-sample
classifier that detects multivariate drift by training a model to
separate past rows (label 0) from current rows (label 1) and reporting
cross-validated AUC as the drift signal. Fills the gap between `drift`
(single-metric, scalar z-score) and `shape` (per-row isolation-forest
outliers) with a batch-level multivariate signal: "does the overall
pattern of this run differ from past runs?" Optional SHAP explanations
name the top features driving the separation.

## Key Changes

- New `PatternValidationConfig` / `PatternModelConfig` /
  `PatternThresholdConfig` pydantic configs with `Literal["pattern"]`
  discriminator; wired into the `ValidationConfig` union.
- New `PatternCheckValidator` at `qualifire/validation/pattern_check.py`,
  fed by `SampleCollectionConfig` via the engine's collect→validate
  pipeline. Uses `StratifiedKFold` + `cross_val_score(..., scoring="roc_auc",
  error_score="raise")` with a finite-value guard.
- Extracted `qualifire/validation/_encoding.py` — shared column encoder
  used by both `pattern` and `shape`. Rejects duplicate column labels,
  handles pandas nullable `boolean` correctly, normalizes tz-aware
  datetimes to UTC epoch seconds, and maps `NaT` → `0` instead of
  `iinfo(int64).min`.
- `qualifire/api.py::pattern_check` programmatic helper with explicit
  `past_dates` / `history_filters` cross-validation.
- `qualifire/core/engine.py`: dataset-scoped thread-safe `_SampleCache`
  with per-key singleflight locking so shape + pattern on the same
  `SampleCollectionConfig` see **identical rows** — Spark's
  `sample_records` uses unseeded `ORDER BY RAND()`, so without this
  cache two sample-based validators would disagree on what the sample
  even contains. Prime runs serially before fan-out; fallback on prime
  failure still serializes concurrent misses and writes back.
- Leakage guard: `exclude_columns` drops partition/date/ID columns
  before training; intersection of past↔current feature sets handled
  explicitly with a warning when columns diverge.
- SHAP explanations are optional (require the `anomaly` extra). Import
  failures are soft (result returned without `top_contributing_features`);
  runtime SHAP errors surface in `details["explanation_error"]`.

## Files Changed

- `qualifire/validation/pattern_check.py` (new)
- `qualifire/validation/_encoding.py` (new, extracted)
- `qualifire/validation/isolation_forest.py` (switched to shared encoder)
- `qualifire/api.py`
- `qualifire/core/config.py`
- `qualifire/core/engine.py`
- `docs/pattern_check.md` (new)
- `docs/validators.md`, `docs/configuration.md`, `docs/programmatic_api.md`, `README.md`
- `tests/test_validation/test_pattern_check.py` (new, 29 tests)
- `tests/test_validation/test_encoding.py` (new, 8 regression tests)
- `tests/test_e2e.py` (pattern scenarios + sample-cache regressions)
- `tests/test_cli.py`

## Testing

- `pytest -q` → 651/651 pass.
- 29 unit tests in `test_pattern_check.py` cover config validation
  (threshold keys, AUC bounds, inverted thresholds, n_estimators,
  max_depth), the CV floor at `min(class) >= cv_folds`, leakage
  controls, and SHAP soft/hard failure modes.
- 8 encoder regression tests in `test_encoding.py` lock in the
  pathological-input fixes from round 3 (duplicate columns, nullable
  boolean with `pd.NA`, `NaT`, tz-aware datetimes, empty frame,
  all-null numeric).
- `test_shape_and_pattern_share_sample_collection` — shape + pattern
  on identical `SampleCollectionConfig` call `_collect_sample` exactly
  once.
- `test_sample_cache_fallback_shares_single_collection_after_prime_failure`
  — forces the prime attempt to raise and asserts the fallback runs
  exactly once for shape + pattern under `validation_parallelism=4`.

Five rounds of `/codex:adversarial-review --wait` ran end-to-end; every
actionable finding landed as its own commit on the branch before the
next round.

## Notes

- Sklearn and SHAP stay optional — SHAP only hard-required when
  `model.explain=True`. The validator degrades gracefully when SHAP
  is missing (returns AUC without `top_contributing_features` and
  logs a warning).
- `docs/pattern_check.md` documents the algorithm, configuration,
  leakage control, and how to read AUC + SHAP output. The CV-floor
  guidance is now `min(class) >= cv_folds` (sklearn's hard minimum for
  stratified k-fold), with a note that users should aim for several
  multiples of `cv_folds` per class for stable AUC.
- PR: https://github.com/amitranjan-oracle/qualifire/pull/3 (base `main`).
