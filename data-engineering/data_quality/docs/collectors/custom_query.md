# Custom Query Collector

Run an arbitrary SQL query that returns a single row. Each non-key
column in that row becomes a `CollectionResult` with
`metric_name=<column>` and `metric_value=<value>`. Use this when:

- The metric needs CTEs, JOINs, or window functions that don't fit the
  flat aggregation shape.
- You're computing a derived KPI that mixes multiple tables.
- You want to expose a precomputed view as Qualifire metrics.

## Configuration

```yaml
collection:
  type: custom_query
  sql: |
    WITH valid AS (
      SELECT * FROM {{ table }} WHERE status = 'completed'
    )
    SELECT
      COUNT(*)             AS row_count,
      AVG(amount)          AS avg_amount,
      SUM(amount * fx_rate) / SUM(quantity) AS revenue_per_unit
    FROM valid
    JOIN currency_dim USING (currency)
  dimensions: ["region"]   # optional — adds to GROUP BY behaviour
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"custom_query"` | yes | discriminator |
| `sql` | `str` | yes | Jinja-rendered before execution; access `{{ table }}`, `{{ ds }}`, etc. |
| `dimensions` | `list[str]` | no | columns the query is grouped by; the engine uses them to fan out result rows |

There is **no** collection-level `filter`. The query owns its own
`WHERE`. Setting `filter` raises a config-load error with a migration
hint (you'd be filtering an already-aggregated single row, which
silently empties the result).

## Programmatic shape

```python
from qualifire.core.config import CustomQueryCollectionConfig

CustomQueryCollectionConfig(
    sql="SELECT COUNT(*) AS row_count, AVG(amount) AS avg_amount FROM {{ table }} WHERE ds = '{{ ds }}'",
)
```

## Common patterns

### Roll-up + per-region in the same dataset

Two separate validations — one with the rolled-up SUM, another
per-region:

```yaml
- type: threshold
  name: revenue_total_check
  collection:
    type: custom_query
    sql: |
      SELECT SUM(amount) AS total_revenue
      FROM {{ table }}
      WHERE ds = '{{ ds }}'
  rules:
    - metric: total_revenue
      thresholds: { error: { min: 1000000 } }

- type: threshold
  name: revenue_per_region_check
  collection:
    type: custom_query
    sql: |
      SELECT region, SUM(amount) AS region_revenue
      FROM {{ table }}
      WHERE ds = '{{ ds }}'
      GROUP BY region
    dimensions: ["region"]
  rules:
    - metric: region_revenue
      thresholds: { warning: { min: 50000 } }
```

This avoids the trap of putting a `SUM(...)` rule in `validation` on
top of an already-rolled-up query (the SUM rolls up nothing when the
input is one row per region).

### Time-windowed comparison

```yaml
collection:
  type: custom_query
  sql: |
    SELECT
      AVG(CASE WHEN ds = '{{ ds }}' THEN amount END) AS today_avg,
      AVG(CASE WHEN ds = '{{ macros.ds_add(ds, -7) }}' THEN amount END) AS week_ago_avg
    FROM {{ table }}
    WHERE ds IN ('{{ ds }}', '{{ macros.ds_add(ds, -7) }}')
```

### Windowed top-K

```yaml
collection:
  type: custom_query
  sql: |
    WITH ranked AS (
      SELECT customer_id, SUM(amount) AS spend,
             ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rn
      FROM {{ table }}
      WHERE ds = '{{ ds }}'
      GROUP BY customer_id
    )
    SELECT MAX(spend) AS top_customer_spend,
           MIN(spend) FILTER (WHERE rn <= 100) AS top_100_floor
    FROM ranked
```

## What lands in the system table

The query above produces:

| metric_name | metric_value |
|-------------|--------------|
| top_customer_spend | 12450.00 |
| top_100_floor | 1820.00 |

Each scalar column becomes one row.

## Validators that consume it

- **threshold** — most common, one rule per output column
- **drift** — column over multiple partitions
- **trend** (forecast) — column as a time series
- **custom_query_check** — a thin convenience wrapper that pairs the
  custom query with threshold rules in one validation block

## Common pitfalls

- **More than one row** — the collector takes the first row and
  warns. If your query intentionally returns multiple, declare
  `dimensions` and the engine will treat each row as a dimensioned
  result.
- **Column type mismatch** — non-numeric output columns persist as
  `metric_value=None` (text values are rejected by the storage
  schema). Cast in SQL: `CAST(my_text AS DOUBLE)`.
- **Heavy queries on every run** — custom query is the most expensive
  collector because it can do anything. Pair it with the engine's
  cache (`cache: true` on the dataset) when multiple validations
  share the same query.
