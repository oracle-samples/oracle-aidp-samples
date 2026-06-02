# Pattern Check

**Builder**: `pattern_check()` | **YAML type**: `pattern`

*Implementation: Random Forest two-sample classifier + SHAP explainability*

The pattern check answers a batch-level question: **"does the overall multivariate shape of the current run differ from past runs?"** It trains a Random Forest classifier to tell current rows (label `1`) apart from past rows (label `0`). When the cross-validated AUC is meaningfully above 0.5, the classifier has found a way to separate the two — i.e., the distribution has drifted.

Unlike `shape` (which flags individual anomalous rows) and `drift` (which compares a single metric against its history), `pattern` captures multivariate structural shifts that would not show up in a per-column summary.

## When to use `pattern` vs `shape` vs `drift`

| Question | Validator |
|---|---|
| "Is today's metric N% off from the historical mean?" | `drift` |
| "Which rows in today's run look anomalous against history?" | `shape` |
| "Is today's run drawn from a different distribution than past runs?" | `pattern` |

`pattern` and `shape` share the same `sample` collector; you can configure both on the same dataset and they'll share the sample without duplication.

## How It Works

### 1. Sampling

Current and historical data are sampled using the same `sample` collector as `shape`:

```python
qf.pattern_check(
    n_records=10000,          # records per period
    past_dates=3,             # number of historical periods
    step="P7D",                # interval between periods
    slice_column="sale_date",
    slice_value="{{ ds }}",
)
```

This produces 4 samples: current + 3 past periods.

### 2. Leakage Control (required)

Before encoding, every column listed in `model.exclude_columns` is dropped from both current and past slices. **This step is load-bearing.**

Any column that was used to *define* which rows are "current" vs "past" — partition columns, date columns, ingestion timestamps, surrogate IDs — will let the classifier separate runs trivially even when the actual business distribution is unchanged. Without exclusions, you'll see AUC ≈ 1.0 on every run.

Minimum-viable exclude list for a date-partitioned table:

```yaml
model:
  exclude_columns: ["sale_date", "updated_at", "sale_id"]
```

### 3. Schema Change Detection

Before combining samples, Qualifire intersects the column sets across current and all past periods. Columns present in only one side are logged. Set `alert_on_schema_change: true` to emit a WARNING on schema differences in addition to the AUC result.

### 4. Column Encoding

Shared with `shape`:

| Column Type | Encoding |
|---|---|
| Numeric (int, float) | Pass-through, median imputation for nulls |
| Boolean | 0/1 integer |
| Timestamp / datetime | Unix timestamp (seconds) |
| Categorical ≤ 20 unique values | One-hot encoding |
| Categorical > 20 unique values | Label encoding (integer) |
| Complex (array, map, struct) | Stringify + label-encode, or drop if `drop_complex=true` |

Keeping the encoding identical between `shape` and `pattern` means when both alert on the same run, their top-feature attributions refer to the same feature matrix.

### 5. Classifier + Cross-Validation

```
RandomForestClassifier(
    n_estimators=200,
    max_depth=8,
    class_weight="balanced",
    random_state=42,
    n_jobs=1,
)
```

Scored with `StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=42)` and `cross_val_score(..., scoring="roc_auc")`. Reported `actual_value` is the **mean CV AUC** across folds. Per-fold scores and the std-dev are attached to `details`.

Cross-validation is intentional: reporting train-set AUC would always score 1.0 on a tree-based model with enough depth and overstate drift.

### 6. Tiny-Sample Guard

If either class has fewer than `cv_folds` rows — i.e. not enough samples for sklearn to build one stratified fold per class — the validator refuses to train and returns `_empty_data_result` with a message naming the counts. This surfaces as your `on_empty_data` severity rather than as a noisy CV failure. (Note: sklearn *will* run at exactly `cv_folds` per class, but results at that floor have very high variance; you probably want meaningfully more.)

### 7. Severity

```
auc >= error_threshold   → ERROR
auc >= warning_threshold → WARNING
else                     → PASS
```

Default thresholds: `warning=0.65`, `error=0.80`. The config schema rejects:

- Unknown threshold keys (`warning: { aux: 0.65 }`)
- AUC values outside `[0.5, 1.0]`
- Inverted thresholds (`warning.auc > error.auc`)

…at `validate-config` time, not at run time.

### 8. SHAP Explainability

When `explain: true` and `severity != PASS`, a fresh Random Forest is fit on the full `(X, y)` and passed to `shap.TreeExplainer`. Mean absolute SHAP values over the positive class name the top 5 features driving the separation:

```python
result.details["top_contributing_features"]
# [
#     {"feature": "region_EU", "importance": 0.284},
#     {"feature": "amount", "importance": 0.112},
#     {"feature": "category_A", "importance": 0.067},
# ]
```

SHAP is optional: if it's missing or raises, the validator logs a warning and returns the AUC result **without** the `top_contributing_features` key. It never crashes the run.

### Reading the importance distribution

The shape of the top-features list tells you something about the drift:

- **One dominant feature** (e.g. `amount: 0.49`, every other feature `< 0.01`) is the **healthy clean-signal pattern**. The classifier found one column that cleanly separates today from history (e.g. an `amount` distribution shift) and didn't need any other feature. Low importances on the rest aren't a SHAP failure — they're SHAP correctly reporting "these other columns weren't useful." Don't ignore the dominant feature because the rest look "weak."
- **Multiple features with similar importances** typically indicates a multivariate / co-varying shift — the classifier couldn't separate today from history with any one column, so it leaned on several. Investigation effort scales with the number of features above ~0.05.
- **Top feature is a partition / ID / timestamp column** (e.g. `sale_date_2026-05-07`, `sale_id`, `updated_at`) is **leakage, not drift**. The classifier learned to read the row's identity rather than its content. Add the column to `exclude_columns` and re-run. The engine auto-excludes `slice_column`; operators must add per-row IDs and ingestion timestamps themselves. See [`docs/collectors/sample.md`](../collectors/sample.md) common pitfalls.
- **All features have ≈ 0.0 importance** with high AUC is rare and usually means the classifier separated by chance on a tiny sample (`n_records` too low) or that SHAP attribution failed on this dataset shape — re-run with `n_records ≥ 1000`.

## Configuration

### Programmatic

```python
qf.pattern_check(
    n_records=10000,
    past_dates=3,
    step="P7D",
    slice_column="sale_date",
    slice_value="{{ ds }}",
    # Optional: pass explicit past slice predicates instead of past_dates/step.
    # When provided, its length defines the number of past slices.
    history_filters=None,
    thresholds={
        "warning": {"auc": 0.65},
        "error":   {"auc": 0.80},
    },
    # Model knobs (all forwarded into PatternModelConfig):
    n_estimators=200,
    max_depth=8,
    cv_folds=5,
    class_weight="balanced",
    explain=True,
    drop_complex=False,
    alert_on_schema_change=False,
    exclude_columns=["sale_date", "updated_at", "sale_id"],
    on_missing_history="ignore",
)
```

### YAML

```yaml
- type: "pattern"
  collection:
    type: "sample"
    n_records: 10000
    slice_column: "sale_date"
    slice_value: "{{ ds }}"
    history:
      past_dates: 3
      step: "P7D"
      # Optional: explicit per-slice filters. If set, its length MUST
      # match past_dates — config-load will reject the mismatch.
      # filters:
      #   - "sale_date = '2026-04-01'"
      #   - "sale_date = '2026-04-08'"
      #   - "sale_date = '2026-04-15'"
  model:
    n_estimators: 200
    max_depth: 8
    cv_folds: 5
    class_weight: "balanced"
    explain: true
    drop_complex: false
    alert_on_schema_change: false
    on_missing_history: "ignore"
    exclude_columns:
      - "sale_date"
      - "updated_at"
      - "sale_id"
  thresholds:
    warning: { auc: 0.65 }
    error:   { auc: 0.80 }
```

## Cold Start & Missing History

When no historical data is available, behavior follows `model.on_missing_history`:

| Value | Behavior |
|---|---|
| `"ignore"` (default) | Returns PASS with `details.cold_start=true` |
| `"warn"` | Returns WARNING when no past data is available |
| `"error"` | Returns ERROR when no past data is available |

## Empty Current

When the current sample is empty, the validator returns an empty-data result whose severity follows `on_empty_data` (`pass` / `warning` / `error`).

## Details Payload

A successful run produces the following keys in `ValidationResult.details`:

| Key | Type | Meaning |
|---|---|---|
| `auc` | float | Mean cross-validated AUC |
| `auc_std` | float | Std-dev of per-fold AUC |
| `auc_folds` | list[float] | Per-fold AUC scores |
| `n_current` | int | Rows with label=1 after exclusions |
| `n_past` | int | Rows with label=0 after exclusions |
| `n_features` | int | Width of the encoded matrix |
| `cv_folds` | int | Number of CV folds used |
| `top_contributing_features` | list | SHAP top-5 (only when severity != PASS and SHAP succeeded) |
| `value_drift_explainer` | list | Per-feature value-shift summaries parallel to `top_contributing_features` (when `explain_value_drift=True`) |
| `value_drift_explainer_truncated` | bool | Set when the parallel list contains placeholdered entries due to the 16 KB payload cap |
| `value_drift_explainer_mapping_errors` | int | Count of features whose name was missing from the encoder mapping (defensive, normally absent) |
| `value_drift_explainer_error` | str | Set on explainer failure; original verdict is unaffected |

All scalars are cast to Python builtins before persistence so `details_json` round-trips through JSON cleanly.

## Value Drift Explainer

When `explain_value_drift=True` (default) and SHAP names top
contributing features, the validator augments each feature with a
per-kind summary of *how* its values shifted between the current
sample and the union of past slices:

```python
"value_drift_explainer": [
    {
        "feature": "amount",
        "source_column": "amount",
        "kind": "numeric",
        "current": {"count": 1000, "null_pct": 0.02, "mean": 120.5, "p99": 500.0, ...},
        "past":    {"count": 3000, "null_pct": 0.01, "mean": 80.0,  "p99": 300.0, ...},
        "delta":   {"mean_pct": 0.506, "p99_pct": 0.667, "null_pct_abs": 0.01},
        "summary": "amount; p50 100 vs 70; mean +51%; p99 +67%",
    },
    {
        "feature": "channel_mobile",
        "source_column": "channel",
        "kind": "onehot",
        "category": "mobile",
        "is_null_bin": false,
        "current": {"count": 1000, "rate": 0.60},
        "past":    {"count": 3000, "rate": 0.30},
        "delta":   {"rate_pp": 0.30},
        "summary": "channel='mobile' rate 60.0% vs 30.0% (+30.0pp)",
    },
]
```

The list is parallel-length / parallel-order with
`top_contributing_features` so a reader can `zip(...)` the two.
Per-kind shape:

| kind | scalar(s) reported |
|------|---------------------|
| numeric | count, null_pct, mean, std, p25/p50/p75/p95/p99, min, max |
| boolean | count, null_pct, true_rate |
| datetime | count, null_pct, min (ISO), max (ISO) |
| onehot | count, rate (`category` and `is_null_bin` identify the bin) |
| label_encoded | count, null_pct, top-5 category mix `[{value, rate}]` |
| unknown | count, null_pct (only) |
| truncated | placeholder when the entry was dropped to fit the 16 KB payload cap |

Notifier rendering: the body shows the top-3 entries inline as
`→ {summary}` lines under the `• {feature}` SHAP bullet. Set
`explain_value_drift=False` on the validator (or
`model.explain_value_drift: false` in YAML) to skip this stage.

The interactive HTML dashboard (`qf.interactive_dashboard(...)`)
renders the same block via a click-to-expand row detail panel —
see [Dashboard Detail Panel](#dashboard-detail-panel) below.

## Dashboard Detail Panel

The interactive HTML dashboard (`qf.interactive_dashboard(...)`) shows
each pattern row's `details` block in a click-to-expand panel beneath
the per-partition history table. Pressing the `▸` toggle in the
first column reveals the SHAP top features parallel-zipped with the
`value_drift_explainer` summaries (no Python round-trip required —
the dashboard parses the persisted `details_json` directly into the
embedded SNAPSHOT). The panel collapses on a second click.

### Shared dashboards / PII

The detail panel surfaces `value_drift_explainer` category values
**verbatim** from the source column. The dashboard does NOT redact
those fields — doing so would mask the very drift the explainer is
meant to surface. For shared dashboards where the source column may
contain PII (email, phone, payment-card values, etc.), control
disclosure at the source:

- Set `model.explain_value_drift: false` to disable the block for the
  affected validation.
- Add the column to `model.exclude_columns` so SHAP can't pick it as
  a top contributor in the first place.
- Filter or hash the column upstream of qualifire (e.g. via the
  dataset `query:` or a CTE) so the sample never sees raw PII.

The keyword denylist (`password`, `secret`, `token`, `api_key`,
`credential`) redacts those fields if they ever appear at non-
explainer paths in `details_json`. It is **not** a primary privacy
guard for the explainer block.

## Tuning Tips

- **Class imbalance** — `class_weight="balanced"` (default) rescales at training time; AUC is class-balance invariant.
- **Small samples** — lower `cv_folds` to 2 or 3 when per-period row counts are small. The tiny-sample guard refuses to train when `min(class) < cv_folds` (sklearn's hard floor for stratified k-fold); aim for several multiples of `cv_folds` per class for stable AUC.
- **Noisy alerts** — lower `max_depth` (e.g., 5) to regularize the forest and raise the PASS/WARNING threshold.
- **Leakage surprises** — if AUC is >0.95 every run, the first suspect is an un-excluded partition/date column. Add it to `exclude_columns`.

## See also

- [`shape.md`](shape.md) — Isolation Forest per-row anomaly detection.
- [`forecast.md`](forecast.md) — Prophet forecasting.
- [`../programmatic_api.md`](../programmatic_api.md) — general
  `qf.validate(...)` shape.
- [`../notifications.md`](../notifications.md) — notify routing.
- [`README.md`](README.md) — index of validator types.
