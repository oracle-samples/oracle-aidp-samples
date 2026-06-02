# Trend Check (Forecast)

**Builder**: `trend_check()` | **YAML type**: `trend`

*Implementation: Facebook Prophet time-series forecasting*

The trend check uses Facebook's Prophet to build adaptive thresholds
that account for trends, seasonality, and growth patterns.

> **Partition anchor required.** Forecast reads history via
> [`read_metric_history_by_partition`](../../qualifire/storage/base.py)
> — anchor − k·step lookups against the dataset's `partition_ts`.
> Both `DatasetConfig.partition_ts` and `rule.model.step` must be set;
> otherwise the validator surfaces a structured ERROR (no
> `run_timestamp` fallback). See
> [partition_anchoring.md](partition_anchoring.md) for the full
> contract.

## Why Forecasting?

Static thresholds break when data has natural patterns:
- Weekday vs weekend traffic differences
- Monthly billing cycles
- Seasonal sales trends
- Gradual growth over time

Prophet models these patterns and generates **prediction bands** — if the current value falls outside the band, it's anomalous relative to the historical pattern.

## How It Works

1. **Read history**: Reads past `history_count` values from the system
   table at partition anchors `cr.partition_ts − k·step` for
   `k=1..history_count` (default: 90 data points). Prophet's `ds`
   axis is the partition timestamp, not `run_timestamp` — backfills
   and notebook re-runs that cluster many writes at one wall-clock
   moment don't pollute the fit.
2. **Fit model**: Prophet decomposes the time series into trend +
   seasonality + noise.
3. **Predict**: Generates prediction for the next time period with
   confidence intervals.
4. **Validate**: Checks if current value falls within the warning
   band (80%) and error band (95%).

### Two-Band Approach

```
                    ┌─── Error band (95%) ────┐
                    │  ┌─ Warning band (80%) ─┐│
                    │  │                      ││
 ──────────────────────┼──── predicted ────────┼──────────
                    │  │                      ││
                    │  └──────────────────────┘│
                    └──────────────────────────┘
```

- **Inside warning band** → PASS
- **Outside warning band, inside error band** → WARNING
- **Outside error band** → ERROR

## Prerequisites

```bash
pip install 'qualifire[forecast]'
```

Requires a **system table** with at least 10 historical data points for the metric.

## Configuration

### Programmatic

A column-ref ``partition_ts`` like ``"event_dt"`` requires
``dimensions=[…]`` so the SELECT can group by it — otherwise
``event_dt`` selected beside ``AVG(...)`` is invalid SQL. The
example below uses per-day forecast on ``event_dt``; for
single-anchor forecasts see the literal pattern in
[`partition_anchoring.md`](partition_anchoring.md).

```python
result = qf.validate(
    table="catalog.schema.sales",
    partition_ts="event_dt",            # required — anchors history reads
    dimensions=["event_dt"],            # required so GROUP BY captures the column ref
    validations=[
        qf.trend_check(
            metric="avg_sales",
            aggregation="AVG(sales_amount)",
            history_count=90,
            step="P1D",                 # required — partition cadence
            # Prophet hyperparameters
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10.0,
            seasonality_mode="additive",
        ),
    ],
)
```

### YAML

```yaml
- type: "trend"
  collection:
    type: "aggregation"
    expressions:
      - "AVG(sales_amount) AS avg_sales"
  rules:
    - metric: "avg_sales"
      model:
        history_count: 90
        step: "P1D"
        changepoint_prior_scale: 0.05
        seasonality_prior_scale: 10.0
        seasonality_mode: "additive"
        interval_width:
          warning: 0.80
          error: 0.95
  notify:
    warning: ["email"]
    error: ["email", "slack"]
```

## Hyperparameters

| Parameter | Default | Effect |
|---|---|---|
| `history_count` | 90 | Number of historical data points. More data = better seasonality detection, but slower fit |
| `step` | **required** | Partition cadence (ISO 8601, e.g. `P1D`, `PT1H`). No default — every rule must declare it. |
| `changepoint_prior_scale` | 0.05 | **Trend flexibility**. Higher values (0.1–0.5) make the model more reactive to recent changes. Lower values produce smoother trends |
| `seasonality_prior_scale` | 10.0 | **Seasonal amplitude**. Lower values dampen seasonal effects |
| `seasonality_mode` | "additive" | Use `"multiplicative"` when seasonal amplitude grows proportionally with the trend |
| `interval_width.warning` | 0.80 | Width of the warning prediction band |
| `interval_width.error` | 0.95 | Width of the error prediction band |
| `on_missing_history` | "ignore" | Behavior when insufficient history. `"ignore"`: use available data (min 2 points). `"warn"`: WARNING if < 10 points. `"error"`: ERROR if < 10 points |

### Tuning Guide

**Volatile data** (e.g., real-time event counts):
```yaml
changepoint_prior_scale: 0.2   # more reactive
interval_width:
  warning: 0.70                # tighter warning band
  error: 0.90                  # tighter error band
```

**Stable data with strong seasonality** (e.g., daily revenue):
```yaml
changepoint_prior_scale: 0.01  # smooth trend
seasonality_prior_scale: 15.0  # preserve seasonal patterns
```

**Rapidly growing data** (e.g., user signups):
```yaml
changepoint_prior_scale: 0.1
seasonality_mode: "multiplicative"  # seasonal swings scale with growth
```

## Cold Start & Missing History

When there aren't enough historical data points, behavior is controlled by `on_missing_history`:

| Value | Behavior |
|---|---|
| `"ignore"` (default) | Use whatever data is available. Lowers minimum from 10 to 2 points for forecasting. With < 2 points, returns PASS with `cold_start: true` in details |
| `"warn"` | Returns WARNING if < 10 points are available |
| `"error"` | Returns ERROR if < 10 points are available |

All cold-start results include `details={"cold_start": True}` for downstream tracking regardless of the setting.

```yaml
rules:
  - metric: "avg_sales"
    model:
      on_missing_history: "warn"  # surface cold-start as WARNING
```

The metric continues to be collected and stored. Once enough history accumulates, forecasting activates automatically.

## Visualization

```python
from qualifire.reporting import plot_forecast

# Read partition-anchored history that the forecast validator used.
history = storage.read_metric_history_by_partition(
    table_name="catalog.schema.sales",
    metric_name="avg_sales",
    anchor_ts=cr.partition_ts,          # the run's anchor
    count=90,
    step="P1D",
)
vr = result.datasets[0].validation_results[0]  # forecast result

fig = plot_forecast(
    history=history,
    forecast_details=vr.expected_value,
    current_value=vr.actual_value,
    metric_name="avg_sales",
)
fig.show()
```

## Combining with Other Validators

Forecast works best alongside other validators:

```python
validations=[
    qf.threshold_check(...),      # Hard floor/ceiling (absolute safety net)
    qf.trend_check(...),       # Dynamic thresholds (adapts to patterns)
    qf.drift_check(...),     # Week-over-week comparison
]
```

The threshold check catches catastrophic failures (row count = 0).
The forecast catches subtle shifts that a static threshold would
miss.

## See also

- [`partition_anchoring.md`](partition_anchoring.md) — partition_ts /
  step contract.
- [`drift.md`](drift.md) — historical comparison validator.
- [`shape.md`](shape.md) — Isolation Forest anomaly detection.
- [`../programmatic_api.md`](../programmatic_api.md) — general
  `qf.validate(...)` shape.
- [`../notifications.md`](../notifications.md) — notify routing.
- [`README.md`](README.md) — index of validator types.

## Dashboard Detail Panel

The interactive HTML dashboard's click-to-expand row panel renders
trend rows' `observed` vs `predicted` plus the prediction band
`[yhat_lower, yhat_upper]`. Press the `▸` toggle in the first column
of the per-partition history table.
