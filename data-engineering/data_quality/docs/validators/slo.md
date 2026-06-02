# SLO Check

**Builder**: `slo_check()` | **YAML type**: `slo`

*Implementation: recency / freshness against duration thresholds*

An SLO check answers a single question: **how stale is this dataset's
data?** The validator picks the most recent timestamp the dataset
exposes (its *recency timestamp*), computes `now − recency`, and
compares the delta against `warning` / `error` durations.

## How it works

1. **Resolve recency** — run one of four strategies (table below) to
   find the dataset's most recent timestamp.
2. **Compute freshness** — `delta = datetime.now() − recency_ts`.
3. **Compare** — `delta > error_duration` → ERROR;
   `delta > warning_duration` → WARNING; else PASS.

The result row carries:

- `metric_value` — freshness in seconds (numeric, sortable).
- `actual_value_text` — ISO-8601 duration string
  (`P1DT2H30M15S`) for human display.
- `expected_value` — `{warning: ISO_DURATION, error: ISO_DURATION}`,
  JSON-serialised, for dashboards that want to render the threshold
  alongside the observed value.
- `details.recency_timestamp` — the recency moment that drove the
  comparison.
- `details.delta_seconds` — same as `metric_value`, retained for
  back-compat with consumers that read out of `details`.

## Strategies

`slo_check` knows four ways to find the dataset's recency timestamp.
Pick one that matches how your dataset records time — they're
interchangeable as far as the comparison logic is concerned.

| Strategy | What it runs | Required args | When to use |
|---|---|---|---|
| `max_column` (default) | `SELECT MAX(<column>) FROM <table>` (with optional `WHERE <filter>`) | `column=` | The most common case. The table has a `last_updated_at` / `event_ts` / `created_at` column that records when each row was written or observed. The validator reads the global maximum; downstream filters in `validate(filter_expr=...)` are honoured. |
| `delta_log` | `DESCRIBE HISTORY <table>` and reads the most recent operation timestamp from the Delta history. | (none) | Delta tables where you want freshness *of the table itself* — last `INSERT` / `UPDATE` / `MERGE` operation — rather than freshness of the data inside. Use this when the table has no per-row timestamp column. |
| `metadata` | Backend's `get_table_metadata(table)["last_modified"]`. | (none) | Catalog-level metadata when there is no row-level timestamp column AND the table isn't Delta. Backend support varies — Spark + most catalogs surface `last_modified`; pandas backends often do not. |
| `custom_sql` | Executes the user-supplied `sql=` (Jinja-rendered) and reads the first column of the first row as the recency. | `sql=` | Anything else — joins, sub-queries, time spent in queue, `MAX(updated_at)` over a partition filter, the most recent record passing a quality predicate, etc. |

If the chosen strategy returns NULL (e.g. no rows match the filter,
or the column is empty), the collector raises and the engine
converts that into a structured ERROR validation result —
freshness can't be reasoned about without an anchor.

## Configuration

### Programmatic

```python
# 1. max_column — the typical case
qf.validate(
    table="catalog.retail.sales",
    validations=[
        qf.slo_check(
            name="sales_freshness",
            description="sales table must refresh hourly",
            column="updated_at",
            strategy="max_column",
            warning="PT1H",
            error="PT4H",
        ),
    ],
)

# 2. delta_log — freshness of the table operation, not the data
qf.validate(
    table="catalog.retail.sales",
    validations=[
        qf.slo_check(
            strategy="delta_log",
            warning="PT2H",
            error="PT12H",
        ),
    ],
)

# 3. custom_sql — anything else
qf.validate(
    table="catalog.retail.sales",
    validations=[
        qf.slo_check(
            strategy="custom_sql",
            sql=(
                "SELECT MAX(updated_at) "
                "FROM {{ table }} "
                "WHERE region = '{{ task.region }}'"
            ),
            warning="PT4H",
            error="PT24H",
        ),
    ],
)
```

### YAML

```yaml
- type: "slo"
  name: "sales_freshness"
  description: "Sales table must refresh hourly."
  recency:
    strategy: "max_column"        # or "delta_log" / "metadata" / "custom_sql"
    column: "updated_at"          # required for max_column
    sql: null                     # required for custom_sql
  thresholds:
    warning: "PT1H"               # ISO 8601 duration (required form)
    error:   "PT4H"
  notify:
    error: ["pager"]
```

## Threshold form

`thresholds` is a dict with optional `warning` and `error` keys. Each
value is an ISO 8601 duration string:

- `P7D` (7 days), `PT1H` (1 hour), `P1W` (1 week),
  `P2DT12H` (2 days + 12 hours), `P1W2D` (9 days — durations sum).

The legacy compact form (`7D`, `1H`, `1D4H`) is **rejected** at
config-load time. Months and years (`PnM` / `PnY`) are also rejected —
calendar arithmetic isn't supported; use weeks/days instead.

If only `warning` is set, the validator only emits PASS or WARNING.
If only `error` is set, only PASS or ERROR. Setting both gives the
typical three-tier ladder (PASS → WARNING → ERROR).

## Routing

`notify={'warning': ['email'], 'error': ['pager']}` (programmatic) or
the equivalent `notify:` block (YAML) routes the result through the
configured channels — same shape as every other validator. Per-severity
routing means you can keep WARNING out of the on-call rotation.

## Common pitfalls

- **`column` doesn't have a clock timestamp**: `max_column` reads
  whatever you give it. If the column stores a date-only string
  ("2026-04-22"), freshness is measured to midnight of that date.
- **The filter excludes recent rows**: `validate(filter_expr=...)`
  applies before the `MAX`. If the filter cuts off recent data, the
  recency timestamp will look stale even though fresh data exists in
  the unfiltered table. Move the filter into a `custom_sql` query if
  you need richer logic.
- **Time zone mismatch**: the validator uses `datetime.now()`
  (naive, local). If your stored timestamps are UTC ISO strings and
  the operator runs in a non-UTC timezone, the freshness number
  will be off by the timezone offset. Normalize at the source —
  store UTC, or convert in the SQL.

## See also

- [`../programmatic_api.md`](../programmatic_api.md) — general
  `qf.validate(...)` shape.
- [`../notifications.md`](../notifications.md) — notify routing.
- [`README.md`](README.md) — index of validator types.

## Dashboard Detail Panel

The interactive HTML dashboard's click-to-expand row panel renders
SLO rows' `data age` and the configured `threshold`. Press the `▸`
toggle in the first column of the per-partition history table.
