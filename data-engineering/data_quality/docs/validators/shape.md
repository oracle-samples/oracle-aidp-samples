# Shape Check

**Builder**: `shape_check()` | **YAML type**: `shape`

*Implementation: Isolation Forest + SHAP explainability*

The shape check uses Isolation Forest to discover "unknown unknowns" — systemic data quality issues that you can't predict with hand-written rules. It compares the multi-dimensional shape (distribution) of current data against historical periods, detecting schema shifts, distribution changes, encoding errors, and upstream pipeline corruption without requiring predefined rules.

## Why Isolation Forest?

Traditional rule-based validation covers **known unknowns** — issues you can anticipate (null rates, row counts, value ranges). But you can't write rules for everything, especially across thousands of columns.

Isolation Forest operates on the full dataset and captures **multi-column correlations**. Combined with SHAP, it tells you not just that something is wrong, but **which columns** are driving the anomaly.

## How It Works

### 1. Sampling

Current and historical data are sampled to keep compute tractable:

```python
qf.shape_check(
    n_records=10000,          # records per period
    past_dates=3,             # number of historical periods
    step="P7D",                # interval between periods
    slice_column="date",
    slice_value="{{ ds }}",
)
```

This produces 4 samples: current + 3 past periods.

### 2. Schema Change Detection

Before combining samples, Qualifire compares column sets:
- **Common columns** → used for analysis
- **New columns** → logged, not used
- **Dropped columns** → logged, not used

Set `alert_on_schema_change: true` to get a WARNING on schema differences.

### 3. Column Encoding

All columns are encoded to numeric features:

| Column Type | Encoding |
|---|---|
| Numeric (int, float, decimal) | Pass-through, median imputation for nulls |
| Boolean | 0/1 integer |
| Timestamp / datetime | Unix timestamp (seconds) |
| Categorical ≤ 20 unique values | One-hot encoding |
| Categorical > 20 unique values | Label encoding (integer) |
| Complex types (array, map, struct) | Flatten first element, or drop if `drop_complex=true` |

### 4. Model Fitting

An Isolation Forest is fit on **all** combined data (current + past):

```
IsolationForest(n_estimators=100, contamination="auto", random_state=42)
```

- `n_estimators`: Number of isolation trees. More trees = more stable scores.
- `contamination`: Expected proportion of outliers. `"auto"` lets sklearn determine the threshold.

### 5. Scoring

Only **current period** records are scored. The anomaly ratio is:

```
anomaly_ratio = anomalous_records_in_current / total_records_in_current
```

### 6. Severity

```
anomaly_ratio >= error_threshold  → ERROR
anomaly_ratio >= warning_threshold → WARNING
else → PASS
```

Default thresholds: warning = 0.6, error = 0.8.

### 7. SHAP Explainability

When anomalies are detected and `explain: true`, SHAP TreeExplainer computes per-feature importance across all anomalous records. The top 5 contributing features are reported:

```python
result.details["top_contributing_features"]
# [
#     {"feature": "order_amount", "importance": 0.342},
#     {"feature": "customer_region_US", "importance": 0.128},
#     {"feature": "delivery_days_timestamp", "importance": 0.089},
# ]
```

## Configuration

### Programmatic

```python
qf.shape_check(
    n_records=10000,
    past_dates=3,
    step="P7D",
    slice_column="date",
    slice_value="{{ ds }}",
    # Model parameters
    n_estimators=100,
    contamination="auto",
    explain=True,
    drop_complex=False,
    alert_on_schema_change=False,
)
```

### YAML

```yaml
- type: "shape"
  collection:
    type: "sample"
    n_records: 10000
    slice_column: "date"
    slice_value: "{{ ds }}"
    history:
      past_dates: 3
      step: "P7D"
  model:
    n_estimators: 100
    contamination: "auto"
    explain: true
    drop_complex: false
    alert_on_schema_change: false
  thresholds:
    warning: { anomaly_score: 0.6 }
    error: { anomaly_score: 0.8 }
```

## Cold Start & Missing History

When no historical data is available for comparison, behavior is controlled by `on_missing_history`:

| Value | Behavior |
|---|---|
| `"ignore"` (default) | Returns PASS with `cold_start: true` in details |
| `"warn"` | Returns WARNING when no past data is available |
| `"error"` | Returns ERROR when no past data is available |

```yaml
model:
  on_missing_history: "warn"  # surface cold-start as WARNING
```

## Tuning Tips

- **Large tables**: Increase `n_records` for better representation, but compute time grows.
- **Noisy data**: Increase `contamination` (e.g., 0.1) to be more tolerant of outliers.
- **Seasonal patterns**: Use more `past_dates` and a matching `step` (e.g., 4 past dates with 7D step covers a month of weekly data).
- **Complex types**: Set `drop_complex: true` if nested columns add noise without value.
- **Schema evolution**: Set `alert_on_schema_change: true` during migrations to detect unexpected column changes.

## Visualization

```python
from qualifire.reporting import plot_anomaly_shap

# After running anomaly detection
for ds in result.datasets:
    for vr in ds.validation_results:
        if vr.validation_type == "shape" and vr.details:
            fig = plot_anomaly_shap(vr.details)
            fig.show()
```

## Value Drift Explainer

When `explain_value_drift=True` (default) and SHAP populates
`top_contributing_features`, the validator augments each feature
with a per-kind value-shift summary describing *how* the column's
distribution changed between the current sample and the union of
past slices. The list is parallel-length / parallel-order with
`top_contributing_features` so a reader can `zip(...)` the two:

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

See [`pattern.md` § Value Drift Explainer](pattern.md#value-drift-explainer)
for the per-kind stat shape and the full kind matrix
(`numeric` | `boolean` | `datetime` | `onehot` | `label_encoded`
| `unknown` | `truncated`).

```python
from qualifire.reporting.plots import plot_value_drift

for ds in result.datasets:
    for vr in ds.validation_results:
        if vr.validation_type == "shape":
            explainer = (vr.details or {}).get("value_drift_explainer") or []
            if explainer:
                plot_value_drift(explainer).show()
```

Set `explain_value_drift=False` on the validator (or
`model.explain_value_drift: false` in YAML) to skip the explainer.

The interactive HTML dashboard (`qf.interactive_dashboard(...)`)
renders the same block via a click-to-expand row detail panel —
see [Dashboard Detail Panel](#dashboard-detail-panel) below.

## Dashboard Detail Panel

The interactive HTML dashboard surfaces each shape row's `details`
in a click-to-expand panel beneath the per-partition history table.
Pressing the `▸` toggle reveals the SHAP top features parallel-zipped
with the `value_drift_explainer` summaries plus the anomaly ratio and
sample sizes.

### Shared dashboards / PII

The detail panel surfaces `value_drift_explainer` category values
**verbatim** from the source column. The dashboard does NOT redact
those fields — doing so would mask the drift the explainer is meant
to surface. For shared dashboards where the source column may carry
PII (email, phone, payment-card values, etc.), control disclosure at
the source:

- Set `model.explain_value_drift: false` to disable the block for
  the affected validation.
- Add the column to `model.exclude_columns` so the Isolation Forest
  doesn't include it as a top SHAP contributor.
- Filter or hash the column upstream of qualifire (e.g. via the
  dataset `query:` or a CTE) so the sample never sees raw PII.

The keyword denylist (`password`, `secret`, `token`, `api_key`,
`credential`) redacts those fields at non-explainer paths in
`details_json`. It is **not** a primary privacy guard for the
explainer block.

## See also

- [`pattern.md`](pattern.md) — RandomForest two-sample classifier
  for batch-level drift.
- [`forecast.md`](forecast.md) — Prophet forecasting.
- [`../programmatic_api.md`](../programmatic_api.md) — general
  `qf.validate(...)` shape.
- [`../notifications.md`](../notifications.md) — notify routing.
- [`README.md`](README.md) — index of validator types.
