# Threshold Check

**Builder**: `threshold_check()` | **YAML type**: `threshold`

*Implementation: bare-number / `{min, max}` bounds on aggregated
metrics*

A threshold check answers: **is this metric inside the bounds we
expect right now?** Unlike drift / forecast, the bounds are
*hand-set* — no history is consulted — so it's the right tool for
"row count must be at least 1000" or "null rate must be below 5 %".

## How it works

1. The collection step runs the configured aggregation (or any other
   collector — profiling, recency, sample) and produces one or more
   `CollectionResult` rows with `metric_name` + `metric_value`.
2. The validator iterates rules; for each rule it picks the matching
   metric and compares `metric_value` against `error` thresholds
   (then `warning` thresholds if `error` doesn't fire).
3. Severity is the first level whose bounds were violated.

## Threshold form

`thresholds` is a dict with optional `warning` and `error` keys.
Each key takes a sub-dict of `{measure: bound}` pairs. **Two shapes**
of bound are accepted on every measure:

| Shape | Meaning |
|---|---|
| `25` (bare number) | Symmetric absolute bound — `value > 25` or `value < -25` violates. Compatible with the early Qualifire releases. |
| `{min: -10, max: 25}` | Asymmetric signed bounds — `value < min` or `value > max` violates. New form; pick this when the measure is signed and the two sides matter independently. |
| `{min: 100}` or `{max: 5}` | One-sided bound. The other side is unbounded. |

Measures available on `threshold_check`:

| Measure | What gets compared |
|---|---|
| `min` / `max` | `metric_value` directly |
| (any positive number) | Same as `max` for back-compat |

> Threshold checks compare the metric's literal value, so the rich
> "deviation" / "z_score" / "rate_of_change" measures live on
> [`drift_check`](drift.md), not here.

## Configuration

### Programmatic

```python
qf.validate(
    table="catalog.retail.sales",
    validations=[
        qf.threshold_check(
            aggregations={
                "row_count": "COUNT(*)",
                "null_pct":  "100.0 * SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) / COUNT(*)",
                "avg_amount": "AVG(amount)",
            },
            rules=[
                # Row count must clear 1000 — bare number rejected, use {min}.
                {"metric": "row_count",
                 "thresholds": {"error": {"min": 1000}}},
                # Null rate two-tier; both bounds use {max}.
                {"metric": "null_pct",
                 "thresholds": {"warning": {"max": 5.0},
                                "error":   {"max": 10.0}}},
                # Asymmetric range — avg_amount may dip 10 % below baseline
                # but mustn't double.
                {"metric": "avg_amount",
                 "thresholds": {"warning": {"min": 45.0, "max": 95.0},
                                "error":   {"min": 30.0, "max": 150.0}}},
            ],
        ),
    ],
)
```

### YAML

```yaml
- type: "threshold"
  name: "row_count_floor"
  description: "Daily row count must clear 1 000."
  collection:
    type: "aggregation"
    expressions:
      - "COUNT(*) AS row_count"
  rules:
    - metric: "row_count"
      thresholds:
        error: {min: 1000}
    - metric: "null_pct"
      thresholds:
        warning: {max: 5.0}
        error:   {max: 10.0}
  notify:
    error: ["pager"]
```

## Result payload

Each rule produces one `ValidationResult`. Notable fields:

- `metric_value` — observed value.
- `expected_value` — JSON-serialised thresholds dict that was
  checked (the `{warning: {...}, error: {...}}` shape under
  `thresholds`).
- `message` — human-readable summary naming which bound was
  violated (e.g. `"row_count = 42 < min=1000 (error)"`).
- `severity` — first level whose bounds were violated; `PASS`
  when all bounds are within range.

## Notes

- **Thresholds run before any notification.** Use the `notify` block
  to route warnings to email and errors to pager. See
  [notifications.md](../notifications.md).
- **The aggregation collector accepts dimensions.** Provide
  `dimensions=["region"]` to get one `ValidationResult` per region
  (each carrying its own `dimension_value`).
- **Threshold checks don't require `partition_ts`.** Drift and
  forecast do — see [partition_anchoring.md](partition_anchoring.md).

## See also

- [`drift.md`](drift.md) — historical comparison (signed measures,
  thresholds against a moving baseline).
- [`slo.md`](slo.md) — duration-based thresholds for freshness.
- [`../programmatic_api.md`](../programmatic_api.md) — general
  `qf.validate(...)` shape.
- [`README.md`](README.md) — index of validator types.

## Dashboard Detail Panel

The interactive HTML dashboard's click-to-expand row panel renders
threshold rows' `actual` value plus the configured `warning` /
`error` `{min, max}` bounds. Press the `▸` toggle in the first
column of the per-partition history table.
