# Metrics Collector

A thin wrapper over `aggregation` for the common case where you want to
declare KPIs as a `name → SQL expression` mapping rather than write
`AS <name>` inline. Functionally equivalent for downstream validators
(drift, trend, threshold consume the same row shape).

## When to use it instead of `aggregation`

- You want a flat `name: expr` map for readability in YAML.
- You're declaring 3+ metrics and the inline `AS` clutter starts to
  hurt diff legibility.
- A team has a curated KPI catalog they want to mirror in config.

For one-off counts or pure technical metrics, `aggregation` is fine.

## Configuration

```yaml
collection:
  type: metrics
  metrics:
    revenue: "SUM(amount)"
    avg_order_value: "AVG(amount)"
    active_users: "COUNT(DISTINCT user_id)"
  filter: "status = 'completed'"      # optional
  dimensions: ["region"]              # optional
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"metrics"` | yes | discriminator |
| `metrics` | `dict[str, str]` | yes | name → SQL expression |
| `filter` | `str` | no | AND-combined with `DatasetConfig.filter` (see [filter precedence](../validators/validator_collector_matrix.md#filter-precedence)) |
| `dimensions` | `list[str]` | no | one row per combination |

## Programmatic shape

```python
from qualifire.core.config import MetricsCollectionConfig

MetricsCollectionConfig(
    metrics={
        "revenue": "SUM(amount)",
        "avg_order_value": "AVG(amount)",
    },
    filter="status = 'completed'",
)
```

## What it computes

Equivalent SQL to `aggregation` with `expressions=["SUM(amount) AS revenue", ...]`:

```sql
SELECT region, SUM(amount) AS revenue, AVG(amount) AS avg_order_value
FROM <table>
WHERE status = 'completed'
GROUP BY region
```

## What lands in the system table

Same shape as aggregation — one row per (metric, dimension combo).

## Validators that consume it

- threshold, drift, forecast — same as aggregation.

## Common pitfalls

- **Aliases collide** — two metrics with the same name silently
  overwrite each other in the dict. Pydantic catches duplicate keys
  at load time only when YAML duplicates the key textually; if both
  paths resolve to the same name, the second wins.
- **Reserved Spark / JDBC keywords as metric names** — `time`,
  `date`, etc. quote them in `metric_name` when downstream readers
  query the system table.
