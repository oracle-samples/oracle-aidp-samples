# Recency Collector

Special-purpose collector for SLO checks: returns "how stale is the
data" as a duration. Three strategies cover the common shapes.

## Configuration

```yaml
recency:
  strategy: max_column          # max_column | delta_log | metadata | custom_sql
  column: updated_at             # required for max_column
```

Or for a Delta table:

```yaml
recency:
  strategy: delta_log
```

Or with custom SQL:

```yaml
recency:
  strategy: custom_sql
  sql: "SELECT MAX(refreshed_at) FROM {{ table }}"
```

## Strategies

| Strategy | What it computes | Best for |
|----------|------------------|----------|
| `max_column` | `SELECT MAX(<column>) FROM <table> WHERE <filter>` | tables with an explicit `updated_at` / `event_ts` |
| `delta_log` | latest commit timestamp from the Delta transaction log | Delta tables (Spark/Databricks) |
| `metadata` | catalog last-modified metadata | Hive metastore / Unity Catalog |
| `custom_sql` | result of operator-supplied SQL — must return a single timestamp | streaming feeds with bespoke watermark tables |

## Programmatic shape

```python
from qualifire.core.config import RecencyCollectionConfig

RecencyCollectionConfig(
    strategy="max_column",
    column="updated_at",
)
```

Or via the public builder:

```python
qf.slo_check(column="updated_at", warning="PT4H", error="PT8H")
```

## What lands in the system table

A single `metric_name="recency_age"` row with `metric_value` as the
age in seconds. The validator (SLO) compares against the configured
duration thresholds.

## Validator that consumes it

- **SLO** — only consumer; the recency collector is hard-wired into
  `SLOValidationConfig`.

## Common pitfalls

- **`max_column` on partitioned tables without filter** — full scan.
  Set `DatasetConfig.filter` to scope to recent partitions.
- **`delta_log` on non-Delta storage** — fails at runtime with a
  clear error. Use `max_column` instead.
- **`custom_sql` returns NULL** — the validator interprets NULL as
  "no data ever" which surfaces as an ERROR. If your SQL might
  legitimately return NULL (empty source), wrap with COALESCE to a
  sentinel old timestamp.

## Output of dependent SLO validator

```
[ERROR] slo_check: Data is 9h 42m stale (threshold: 8h)
  - data age: 34920s (9.7 hours)
  - threshold: PT8H
```
