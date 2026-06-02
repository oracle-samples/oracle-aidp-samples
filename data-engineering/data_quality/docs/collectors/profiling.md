# Profiling Collector

Type-aware column profiling in a single SQL pass. Emits one row per
(column, statistic, dimension combo). The richest collector in the
catalog — useful for general-purpose data quality monitoring without
hand-writing per-column thresholds.

## Configuration

```yaml
collection:
  type: profiling
  columns: ["amount", "currency", "merchant_id"]
  stats: ["null_count", "mean", "stddev", "min", "max", "distinct_count"]
  exclude_stats: []                   # blacklist over stats
  filter: "status = 'completed'"       # optional
  top_k: 10                            # for categorical/string columns
  quantiles: [0.5, 0.95, 0.99]         # for numeric columns
  dimensions: ["region"]               # optional
  column_profiles:
    amount:
      stats: ["mean", "stddev", "min", "max"]   # per-column override
      exclude_stats: []
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"profiling"` | yes | discriminator |
| `columns` | `list[str]` | yes | columns to profile |
| `stats` | `list[str]` | no | base stats to compute (default: all) |
| `exclude_stats` | `list[str]` | no | drop these from `stats` |
| `filter` | `str` | no | AND-combined with `DatasetConfig.filter` (see [filter precedence](../validators/validator_collector_matrix.md#filter-precedence)) |
| `top_k` | `int` | no | top-K most-frequent values (categorical) |
| `quantiles` | `list[float]` | no | quantile fractions to compute (numeric) |
| `dimensions` | `list[str]` | no | one row per (col, stat, dim) |
| `column_profiles` | `dict[str, ProfileColumn]` | no | per-column stat list override |

### Available stats

- **Numeric**: `null_count`, `null_pct`, `min`, `max`, `mean`,
  `stddev`, `distinct_count`, `quantiles` (controlled by
  `quantiles=`).
- **String / categorical**: `null_count`, `null_pct`,
  `distinct_count`, `top_k` (controlled by `top_k=`),
  `min_length`, `max_length`, `avg_length`.
- **Boolean**: `null_count`, `true_count`, `false_count`,
  `null_pct`.
- **Timestamp / date**: `null_count`, `min`, `max`, `distinct_count`.

The collector inspects the column type at execution time and skips
stats that don't apply (e.g. asking for `mean` on a string column
won't crash — it's silently dropped).

## Programmatic shape

```python
from qualifire.core.config import ProfilingCollectionConfig, ProfileColumn

ProfilingCollectionConfig(
    columns=["amount", "currency"],
    stats=["null_count", "mean", "stddev"],
    quantiles=[0.5, 0.95],
    column_profiles={
        "currency": ProfileColumn(stats=["distinct_count", "top_k"]),
    },
)
```

## What it computes

A single SELECT pass over the (filtered) table:

```sql
SELECT
  region,
  COUNT(*) AS qf_total,
  COUNT(amount) AS qf_amount_non_null,
  AVG(amount) AS qf_amount_mean,
  STDDEV(amount) AS qf_amount_stddev,
  ...
FROM <table>
WHERE status = 'completed'
GROUP BY region
```

`top_k` and `quantiles` use a second pass when needed (Spark uses
`approx_percentile`; SQLite falls back to a custom UDF).

## What lands in the system table

| metric_name | metric_value | details_json |
|-------------|--------------|--------------|
| `amount.null_count` | 12 | `{"col":"amount","stat":"null_count"}` |
| `amount.mean` | 91.42 | `{"col":"amount","stat":"mean"}` |
| `amount.quantile_0.95` | 250.0 | `{"col":"amount","stat":"quantile","q":0.95}` |
| `currency.top_k` | NULL (categorical) | `{"col":"currency","stat":"top_k","values":[{"value":"USD","count":47000}, ...]}` |
| `currency.distinct_count` | 12 | `{"col":"currency","stat":"distinct_count"}` |

The `metric_name` form is `<column>.<stat>` (or `<column>.quantile_<q>`).
That's the key validators look up in their threshold rules.

## Validators that consume it

- **threshold** — `metric: "amount.null_count"`, `thresholds: {error: {max: 0}}`
- **drift** — same metric_name lookup against history
- **forecast** — typically less useful for profiling output (high
  variance per-stat); use it sparingly

## Common pitfalls

- **`top_k` payload size** — `details_json` for a high-cardinality
  column can hit MB-range when `top_k=100`. Storage backends with
  text-column size limits (some JDBC) reject. Cap at 20 unless you
  have a specific reason.
- **`distinct_count` cost** — exact distinct counts are expensive on
  Spark for billion-row tables. Use `approx_distinct_count` via a
  custom expression in `aggregation` if you need cheaper.
- **Quantiles on small samples** — quantiles on partitions with fewer
  than 100 rows produce noisy values; threshold rules on these are
  almost always flaky.

## Output of dependent validators

```
[WARNING] drift_check.amount.mean: amount.mean = 142.5 deviates 28.4% from past mean
  - current: 142.5 | past mean: 110.0
  - deviation_pct: 28.4
```

For top_k drift (when supported), the message names which categories
swung:

```
[WARNING] drift_check.currency.top_k: currency mix changed (top category USD: 67% → 45%)
```
