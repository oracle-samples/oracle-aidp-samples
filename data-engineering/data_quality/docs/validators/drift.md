# Drift Check (Historical Comparison)

**Builder**: `drift_check()` | **YAML type**: `drift`

*Implementation: signed comparison of the current value against past
partition-anchored values from the system table*

A drift check answers: **how does today's value look against the
recent past?** The validator pulls the last `past_values` partitions
of a metric from the system table, computes one or more *measures*
against the current value, and applies signed `{min, max}` thresholds.

> **Partition anchor required.** Drift reads history via
> [`read_metric_history_by_partition`](../../qualifire/storage/base.py)
> — anchor − k·step lookups against the current run's
> `cr.partition_ts`. Both `DatasetConfig.partition_ts` and
> `rule.compare.step` must be set; otherwise the validator surfaces a
> structured ERROR (no `run_timestamp` fallback). See
> [partition_anchoring.md](partition_anchoring.md) for the full
> contract.

## Measures

| Measure | Formula | Sign | Notes |
|---|---|---|---|
| `deviation_pct` | `(current − mean(past)) / mean(past) × 100` | signed | Percent change. Negative = current below baseline. |
| `deviation_abs` | `current − mean(past)` | signed | Same as above, in raw units. |
| `z_score` | `(current − mean(past)) / stddev(past)` | signed | Stable for noisy series; needs ≥ 2 past values to compute stddev. |
| `rate_of_change_pct` | `(current − last) / last × 100` | signed | Step-over-step change vs. the **immediate prior** partition only. |
| `rate_of_change_abs` | `current − last` | signed | Same, in raw units. |

All measures are *signed*: a 25 % drop and a 25 % surge produce
`deviation_pct = -25` and `deviation_pct = +25` respectively.

## Threshold form

Same two shapes as [`threshold_check`](threshold.md#threshold-form):

| Shape | Meaning |
|---|---|
| `25` (bare number) | Symmetric absolute bound — `|measure| > 25` violates. Back-compat. |
| `{min: -10, max: 25}` | Asymmetric signed bounds — `measure < min` or `measure > max` violates. The new form. |

A measure entry can omit one side (`{min: 100}`, `{max: 5}`) — that
side is then unbounded.

## How it works

1. Resolve the run's anchor: `cr.partition_ts` is the dataset's
   partition expression rendered against the run context.
2. Read history via `read_metric_history_by_partition(anchor_ts,
   count=past_values, step=compare.step, ...)` — that returns the
   metric values seeded at `anchor − k·step` for k=1..past_values.
3. Compute every requested measure against `mean(past_values)`
   (`deviation_*`, `z_score`) or the immediate prior partition
   (`rate_of_change_*`).
4. Pick the highest severity tier whose bounds fired (`error` →
   `warning` → `pass`).

## Missing-history strategies

`compare.missing_strategy` controls the per-rule behaviour when the
read returns fewer than `past_values` rows:

| Strategy | Behaviour |
|---|---|
| `ignore` (default) | Use whatever history is available; fall to cold-start if zero. |
| `substitute` | Replace missing slots with the mean of available slots. |
| `error` | Surface ERROR explicitly when fewer than requested. |

`compare.on_missing_history` controls cold-start (zero history) — same
shape as forecast: `ignore` (PASS), `warn` (WARNING), `error`
(ERROR). All three carry `details.cold_start = True` so dashboards
can highlight first-run partitions.

## Configuration

### Programmatic

A column-ref ``partition_ts`` like ``"event_dt"`` requires
``dimensions=[…]`` so the SELECT can group by it — otherwise
``event_dt`` selected beside ``AVG(amount)`` is invalid SQL. The
example below uses dimensional drift (per-day ``avg_amount``); for
non-dimensional / single-anchor drift see the literal-anchor pattern
under [`partition_anchoring.md`](partition_anchoring.md).

```python
qf.validate(
    table="catalog.retail.sales",
    partition_ts="event_dt",
    dimensions=["event_dt"],     # required so GROUP BY captures the column ref
    validations=[
        qf.drift_check(
            aggregations={"avg_amount": "AVG(amount)"},
            rules=[{
                "metric": "avg_amount",
                "compare": {
                    "past_values": 4,
                    "step":        "P7D",
                    # Cold-start handling — surface as warning until 4
                    # weeks of history exist.
                    "on_missing_history": "warn",
                },
                "thresholds": {
                    # Asymmetric bounds: tolerate a 50 % surge but only
                    # a 25 % dip, plus a 30 % weekly rate-of-change cap.
                    "warning": {
                        "deviation_pct":      {"min": -25, "max": 50},
                        "rate_of_change_pct": {"min": -30, "max": 30},
                    },
                    "error": {
                        "deviation_pct":      {"min": -50, "max": 100},
                    },
                },
            }],
        ),
    ],
)
```

### YAML

```yaml
- type: "drift"
  collection:
    type: "aggregation"
    expressions:
      - "AVG(amount) AS avg_amount"
  rules:
    - metric: "avg_amount"
      compare:
        past_values: 4
        step: "P7D"
        missing_strategy: "ignore"
        on_missing_history: "warn"
      thresholds:
        warning:
          deviation_pct: {min: -25, max: 50}
          rate_of_change_pct: {min: -30, max: 30}
        error:
          deviation_pct: {min: -50, max: 100}
  notify:
    warning: ["email"]
    error: ["pager"]
```

## Result payload

For each rule:

- `metric_value` — current observed value.
- `expected_value` — JSON-serialised thresholds dict that was
  checked (the `{warning: {...}, error: {...}}` shape under
  `thresholds`).
- `message` — human-readable summary including the firing
  measure name and the bound that was violated. Operators inspect
  this in the dashboard / notification text.
- `details.cold_start` — `True` when no history was found (per
  `on_missing_history`); set on every empty-history result.
- `details.past_values` — the raw past values consulted (post
  missing-strategy fill).
- `details.mean_past`, `details.stddev` — summary stats over
  `past_values` (used by `deviation_*` and `z_score`).
- `details.deviation_pct`, `details.deviation_abs`,
  `details.z_score`, `details.rate_of_change_pct`,
  `details.rate_of_change_abs` — every computed measure value,
  regardless of which one fired. Useful for charts that want to
  plot the full signal even when none of the bounds tripped.

## Common pitfalls

- **Drift fires every run after a renamed dimension.** Drift keys on
  `(table_name, metric_name, dimension_value)`. Renaming a dimension
  resets history — use `substitute` until ramp-up completes, or
  rebuild history through a backfill.
- **`step` doesn't match the actual partition cadence.** A daily
  table with `step="P7D"` looks back 7 days for one row instead of
  the prior day's row — usually wrong. Match `step` to the
  partition cadence.
- **Mismatch between `partition_ts` and the seed timestamps.**
  `partition_ts` must lex-sort identical to the persisted
  `partition_ts` strings; `2024-05-01T00:00:00.000000` and
  `2024-05-01T00:00:00` are different keys to the storage layer.
  See [partition_anchoring.md](partition_anchoring.md#timestamp-format).

## See also

- [`partition_anchoring.md`](partition_anchoring.md) — partition_ts /
  step contract.
- [`threshold.md`](threshold.md) — static bounds (no history needed).
- [`forecast.md`](forecast.md) — Prophet-based adaptive bands.
- [`../programmatic_api.md`](../programmatic_api.md) — general
  `qf.validate(...)` shape.
- [`README.md`](README.md) — index of validator types.

## Dashboard Detail Panel

The interactive HTML dashboard (`qf.interactive_dashboard(...)`)
renders drift rows' `details` in a click-to-expand panel showing
`current` vs `mean_past` plus the signed measures
(`deviation_pct`, `z_score`, `rate_of_change_pct`, …). Press the
`▸` toggle in the first column of the per-partition history table.
