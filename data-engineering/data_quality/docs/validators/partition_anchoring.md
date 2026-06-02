# Partition anchoring

All four history-backed validators (drift, forecast, shape, pattern)
look up past data via *partition-anchored reads*: given a current
run's `partition_ts` and a per-rule `step`, the storage layer or
sample collector reaches back at `partition_ts − k·step` for
`k=1..count`. This document is the cross-cutting reference for that
contract.

## The two pieces operators must set

| Piece | Where | What it expresses |
|---|---|---|
| `DatasetConfig.partition_ts` | run-config / `qf.validate(partition_ts=...)` | The dataset's partition timestamp **expression** — Jinja-rendered, then injected by aggregation collectors as `<expr> AS qf_partition_ts` into the SELECT. Examples: `'2026-04-28'` (literal), `event_dt` (column), `CAST(updated_at AS DATE)` (expression), `'{{ ds }}'` (Jinja). For shape / pattern the expression must Jinja-render to a literal datetime / date — column refs aren't enough because the sampler operates on a single anchor. |
| `rule.compare.step` (drift) / `rule.model.step` (forecast) / `history.step` (shape / pattern) | per-rule | The partition **cadence**. **ISO 8601 only**: `P1D`, `P7D`, `PT1H`, `P2DT12H`. Legacy compact (`"1D"`, `"7D"`) is rejected at config-load time. |

Both pieces are **mandatory** for drift / forecast / shape / pattern.

* **Drift / forecast** raise a structured ERROR
  (`details.missing_partition_anchor = True`) at validation time
  when `cr.partition_ts` is missing.
* **Shape / pattern** raise `QualifireConfigError` at
  collection time when `partition_ts_expr` is missing or doesn't
  Jinja-render to a literal, OR when `step` is missing on the
  history block. Both have an escape hatch via
  `SampleHistoryConfig.filters` (explicit per-slice predicates) —
  set `filters=[…]` and the sampler bypasses the anchor math
  entirely (but `step` is still required for reproducibility, even
  though it isn't consulted in that path).
* **Pydantic** rejects rule configs missing `step` at config-load
  time — the field is required and non-nullable on
  `HistoricalCompareConfig`, `ForecastModelConfig`, and
  `SampleHistoryConfig`.

## Lookback resolution

For the current run with `cr.partition_ts = T` and `compare.step = S`:

```
anchor_k  = T − k·S          for k = 1..past_values
history   = read_metric_history_by_partition(
                anchor_ts=T,
                count=past_values,
                step=S,
                table_name=…, metric_name=…, dimension_value=…,
            )
```

Storage backends serialize each `anchor_k` to ISO-8601 and execute a
SELECT with `partition_ts IN (...)` plus dedupe-by-partition (prefer
`record_type='collection'` over `'validation'`, then most-recent
write).

## Timestamp format

Persisted `partition_ts` strings are written by the engine via
`datetime.isoformat()` — *no* microseconds when the wall clock has
none, *with* microseconds otherwise. Partition lookups serialize
their anchors the same way.

> **Pin one format.** If your seed timestamps come from a custom
> migration script that uses `isoformat(timespec="microseconds")`
> while production writes use the default form, the IN-clause
> comparison silently misses every seed (`"2024-05-01T00:00:00"` ≠
> `"2024-05-01T00:00:00.000000"`).

## Cold-start behaviour

When the partition-anchored read returns zero rows — fresh
deployment, just-renamed dimension, or a deliberately empty table —
each validator picks behaviour from `compare.on_missing_history`
(drift) / `model.on_missing_history` (forecast):

| Value | Severity | Detail |
|---|---|---|
| `ignore` (default) | PASS | Includes `details.cold_start = True`. |
| `warn` | WARNING | Same flag. |
| `error` | ERROR | Same flag. |

`details.cold_start` always rides on the result so dashboards can
visually distinguish "first-run" from "everything is fine".

## Sampler-anchored history (shape / pattern)

Shape (`shape_check`) and pattern (`pattern_check`) don't read from
the system table — they sample raw rows from the source table for
each historical slice. The anchor math is `slice_value − k·step`,
implemented inside `qualifire/collection/sampler.py:SamplerCollector`:

1. Render the operator-supplied `slice_value` (Jinja template) and
   parse the result as an ISO date / datetime. The pair
   `slice_column = '<slice_value>'` becomes the current-slice
   predicate.
2. For each `i` in `1..past_dates`, compute
   `past_value_i = slice_value − i · step` and emit
   `slice_column = '<past_value_i>'` for the past-slice predicate. No
   Jinja re-rendering is involved — the shift is mechanical.

`slice_value` may or may not equal the dataset's `partition_ts` —
the sampler doesn't assume they match. For non-date partitions
(e.g. version IDs), use `SampleHistoryConfig.filters` (explicit
per-slice predicates); that path bypasses anchor math entirely but
still requires `step` so the rule remains reproducible.

## Diagnostic recipes

### "Drift returns ERROR with `missing_partition_anchor: True`."

Either:

- `DatasetConfig.partition_ts` isn't set (or the Jinja expression
  rendered to empty) — set it on the dataset or pass
  `partition_ts="..."` to `qf.validate(...)`.
- `rule.compare.step` isn't set — add `step: "P7D"` (or whatever
  cadence applies).

### "Drift always returns `cold_start: True`, but data exists."

The anchors don't lex-equal the seeded `partition_ts`. Run:

```python
storage.read_metric_history_by_partition(
    table_name=..., metric_name=..., anchor_ts=cr.partition_ts,
    count=4, step="P7D", dimension_value=None,
)
```

If empty, dump a recent row from the system table and compare the
two `partition_ts` strings byte-for-byte. The mismatch is almost
always microseconds, timezone offset, or a Jinja expression that
resolved to a different format than the seeds (e.g.
`'{{ ds }}'` → `'2026-05-07'` vs.
`'2026-05-07T00:00:00'`).

### "Forecast errors with `Insufficient history (cold start)`."

Same root causes as the drift case, plus: forecast asks for at least
2 past values when `on_missing_history: ignore` and 10 when
`warn`/`error`. If you're seeing < 2 partitions of seeded history,
you're genuinely cold-starting; let the run accumulate history or
backfill via the metrics-backfill flow.

## See also

- [`drift.md`](drift.md), [`forecast.md`](forecast.md),
  [`shape.md`](shape.md), [`pattern.md`](pattern.md) — per-validator
  pages.
- [`../configuration.md`](../configuration.md) — dataset and rule
  config reference.
- [`../programmatic_api.md`](../programmatic_api.md) —
  `partition_ts=` parameter on the public Python API.
- `qualifire/storage/base.py` — `read_metric_history_by_partition`
  contract.
- `qualifire/core/duration.py:partition_lookback_anchors` —
  reference implementation of the anchor formula.
