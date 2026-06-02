# Aggregation Collector

Runs one SQL `SELECT <exprs> FROM <table> WHERE <filter>` (with optional
`GROUP BY` on dimensions) and emits one `CollectionResult` per
expression × dimension combination.

This is the workhorse — most threshold / drift / forecast checks are
backed by it.

## Configuration

```yaml
collection:
  type: aggregation
  expressions:
    row_count: "COUNT(*)"
    avg_amount: "AVG(amount)"
    null_pct: "SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)"
  filter: "status = 'completed'"   # optional; AND-combined with dataset filter
  dimensions: ["region", "product"]  # optional
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"aggregation"` | yes | discriminator |
| `expressions` | `dict[str, str]` | yes | `{metric_name: sql_expression}` — key is the metric name, value is the SQL aggregate. The old `list[str]` form with `AS <name>` is rejected at config load. |
| `filter` | `str` | no | SQL WHERE fragment; AND-combined with `DatasetConfig.filter` (see [filter precedence](../validators/validator_collector_matrix.md#filter-precedence)) |
| `dimensions` | `list[str]` | no | columns added to `GROUP BY`; produces one row per combination |

## Programmatic shape

```python
from qualifire.core.config import AggregationCollectionConfig

AggregationCollectionConfig(
    expressions={"row_count": "COUNT(*)", "avg_amount": "AVG(amount)"},
    filter="status = 'completed'",
    dimensions=["region"],
)
```

## What it computes

```sql
SELECT
  region,                                 -- one row per dim combo
  '{{ partition_ts }}' AS qf_partition_ts, -- injected by collector
  COUNT(*) AS row_count,
  AVG(amount) AS avg_amount
FROM <table>
WHERE status = 'completed'
GROUP BY region
```

Each non-key column in the result becomes a separate
`CollectionResult` row with `metric_name=<column>` and
`metric_value=<value>`.

When `partition_ts` resolves to a SQL expression (column ref or
function call) instead of a literal, the collector injects
`<expr> AS qf_partition_ts` into the SELECT so each output row carries
its own anchor — important for late-arriving rows where the partition
isn't a single value.

## What lands in the system table

For the example above with two regions:

| metric_name | metric_value | dimension_value | partition_ts | collection_type |
|-------------|--------------|-----------------|--------------|-----------------|
| row_count   | 47213        | `{"region":"us"}` | 2026-04-28T00:00:00 | aggregation |
| avg_amount  | 91.42        | `{"region":"us"}` | 2026-04-28T00:00:00 | aggregation |
| row_count   | 23410        | `{"region":"uk"}` | 2026-04-28T00:00:00 | aggregation |
| avg_amount  | 88.10        | `{"region":"uk"}` | 2026-04-28T00:00:00 | aggregation |

## Validators that consume it

- **threshold** — checks `metric_value` against bounds
- **drift** — compares `metric_value` to past partitions
- **trend** (forecast) — fits Prophet on the time series
- *not* shape / pattern (those use `sample`)

## Common pitfalls

- **Passing a `list[str]` of `"<expr> AS <name>"`** — the
  list-with-`AS` form (pre-2026-05 shape) is rejected at config
  load. Use the dict form: `{metric_name: sql_expression}`. The
  metric name comes from the dict key; the value is the bare
  SQL aggregate. The `AS qf_partition_ts` alias that the
  collector synthesises internally (see § "What it computes"
  below) is a separate injection; operators don't write it.
- **`partition_ts` mismatch** — when one validation uses a column ref
  and another uses a literal on the same dataset, history reads on
  the literal-side validator can't find the column-side rows. Pick
  one strategy per dataset.
- **Dimension cardinality** — high-cardinality dimensions (>1k unique
  values) blow up the result set and make per-segment thresholds
  noisy. Consider bucketing in the SQL expression instead.
- **NULL dimensions** — a NULL dimension value persists as SQL NULL
  (not `_default`). History reads are NULL-safe across SQLite (`IS`),
  Spark (`<=>`), and JDBC backends.

## Output of dependent validators

When backed by aggregation, a threshold validation result reads:

```
[ERROR] threshold_check.row_count: row_count = 5 (violates error threshold {"min": 1000})
  - dimension: {"region":"uk"}
  - actual: 5
  - threshold: {"min": 1000}
```

Drift adds the per-row context:

```
[WARNING] drift_check.avg_amount: avg_amount = 142.5 deviates 28.4% from past mean
  - current: 142.5 | past mean: 110.0
  - deviation_pct: 28.4
  - z_score: 2.1
```
