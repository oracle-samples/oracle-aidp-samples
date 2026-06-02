# Column Profiling

Qualifire's profiling engine computes column-level statistics in a **single pass** over the DataFrame — all stats for all columns are batched into one `df.agg()` call. Top-K frequency counts use a lightweight separate `groupBy` per string column.

## How It Works

```
┌──────────────────────────────────────────────────────────┐
│ DataFrame                                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐    │
│  │ numeric  │  │  string  │  │ boolean  │  │ ts/date│    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └───┬────┘    │
│       │              │              │             │      │
│       ▼              ▼              ▼             ▼      │
│  NumericAnalyzer TextAnalyzer BooleanAnalyzer Timestamp  │
│       │              │              │             │      │
│       └──────────────┴──────────────┴─────────────┘      │
│                         │                                │
│              ONE df.agg(*all_exprs)    ← Pass 1          │
│                         │                                │
│              groupBy(col).count()      ← Pass 2 (top_k)  │
└──────────────────────────────────────────────────────────┘
```

1. **Classify columns** by Spark DataType or pandas dtype
2. **Collect expressions** from applicable analyzers for each column
3. **Execute one `agg()`** with all expressions combined
4. **Top-K** (optional, per string column): lightweight `groupBy().count().limit(k)`

## Stats by Column Type

### Numeric (int, float, decimal)

| Stat | Spark Function | Description |
|---|---|---|
| `count` | `COUNT(1)` | Total row count |
| `null_count` | `SUM(WHEN IS NULL)` | Null values |
| `null_pct` | computed | Null percentage |
| `min` | `MIN(col)` | Minimum value |
| `max` | `MAX(col)` | Maximum value |
| `mean` | `MEAN(col)` | Average |
| `stddev` | `STDDEV(col)` | Standard deviation |
| `variance` | `VARIANCE(col)` | Variance |
| `skewness` | `SKEWNESS(col)` | Distribution skew |
| `kurtosis` | `KURTOSIS(col)` | Distribution kurtosis |
| `approx_distinct` | `APPROX_COUNT_DISTINCT(col)` | Approximate cardinality |
| `p25`, `p50`, `p75` | `PERCENTILE_APPROX(col, [...])` | Approximate quantiles |

### Text (string)

| Stat | Description |
|---|---|
| `count`, `null_count`, `null_pct` | Same as numeric |
| `min_length` | Shortest string length |
| `max_length` | Longest string length |
| `avg_length` | Average string length |
| `approx_distinct` | Approximate cardinality |
| `empty_count` | Count of empty strings (`""`) |
| `empty_pct` | Empty string percentage |
| `top_k` | Most frequent values with counts (separate pass) |

### Boolean

| Stat | Description |
|---|---|
| `count`, `null_count`, `null_pct` | Same as above |
| `true_count` | Count of `TRUE` values |
| `false_count` | Count of `FALSE` values |
| `true_pct` | Percentage of `TRUE` among non-null |

### Timestamp / Date

| Stat | Description |
|---|---|
| `count`, `null_count`, `null_pct` | Same as above |
| `min` | Earliest timestamp |
| `max` | Latest timestamp |
| `approx_distinct` | Approximate distinct timestamps |

## Configuration

### All defaults (profile everything)

```yaml
- type: "threshold"
  collection:
    type: "profiling"
  rules:
    - metric: "amount.null_pct"
      thresholds:
        error: { max: 5.0 }
```

### Select specific columns

```yaml
collection:
  type: "profiling"
  columns: ["amount", "name", "created_at"]
```

### Disable expensive stats globally

```yaml
collection:
  type: "profiling"
  exclude_stats: ["top_k", "skewness", "kurtosis"]
  top_k: 0                             # also disables top_k (alternative)
```

### Include only specific stats

```yaml
collection:
  type: "profiling"
  stats: ["count", "null_count", "null_pct", "approx_distinct"]
```

### Per-column overrides

```yaml
collection:
  type: "profiling"
  exclude_stats: ["top_k"]             # global: no top_k anywhere

  column_profiles:
    user_id:
      stats: ["count", "null_count", "approx_distinct"]   # only these
    email:
      exclude_stats: ["empty_count", "empty_pct"]          # exclude these
    amount:                                                 # inherits global
```

### Custom quantiles

```yaml
collection:
  type: "profiling"
  quantiles: [0.10, 0.25, 0.50, 0.75, 0.90, 0.99]
```

Produces stats named `p10`, `p25`, `p50`, `p75`, `p90`, `p99`.

## Precedence Rules

When both global and per-column settings exist:

| Priority | Setting | Effect |
|---|---|---|
| 1 (highest) | Per-column `stats` | Only those stats for that column |
| 2 | Per-column `exclude_stats` | Those stats skipped for that column |
| 3 | Global `stats` | Only those stats for all columns |
| 4 | Global `exclude_stats` | Those stats skipped everywhere |
| 5 (default) | No filters | All stats computed |

Excluded stats are filtered at **expression-building time** — they are never added to the `agg()` call, so they consume zero compute.

## Programmatic API

```python
from qualifire.collection.profiling import ProfileEngine, NumericAnalyzer, TextAnalyzer
from qualifire.collection.profiling.engine import ColumnProfileOverrides

# Default: all stats
engine = ProfileEngine()
result = engine.profile(df, table_name="sales")

# Exclude globally
result = engine.profile(df, exclude_stats=["top_k", "skewness"])

# Per-column overrides
result = engine.profile(
    df,
    column_overrides={
        "user_id": ColumnProfileOverrides(stats=["count", "approx_distinct"]),
        "notes": ColumnProfileOverrides(exclude_stats=["top_k"]),
    },
)

# Custom analyzers
engine = ProfileEngine(analyzers=[
    NumericAnalyzer(quantiles=[0.10, 0.50, 0.90]),
    TextAnalyzer(top_k=5),
])
result = engine.profile(df, columns=["amount", "name"])

# Access results
for col_name, profile in result.columns.items():
    print(f"{col_name} ({profile.column_type}):")
    for stat, value in profile.stats.items():
        print(f"  {stat}: {value}")
```

## Custom Analyzers

Extend profiling with custom stats by subclassing `StatAnalyzer`:

```python
from qualifire.collection.profiling.analyzers import StatAnalyzer

class RangeAnalyzer(StatAnalyzer):
    @property
    def name(self):
        return "range"

    @property
    def applicable_types(self):
        return {"numeric"}

    def spark_expressions(self, col_name):
        from pyspark.sql import functions as F
        c = F.col(f"`{col_name}`")
        return [("range", F.max(c) - F.min(c))]

    def pandas_compute(self, series, col_name):
        non_null = series.dropna()
        return {"range": non_null.max() - non_null.min() if len(non_null) > 0 else None}

# Use it
engine = ProfileEngine(analyzers=[RangeAnalyzer()])
result = engine.profile(df)
```

## Performance

- **Single pass**: All built-in stats (except top_k) run in one `df.agg()`. For 50 columns with all stats enabled, this is ~600 expressions in a single Spark action.
- **Approximate functions**: `approx_count_distinct()` and `percentile_approx()` are used instead of exact equivalents for better performance on large datasets.
- **Column filtering**: Use `columns` to profile only the columns you care about, reducing the expression count proportionally.
- **Stat filtering**: Excluded stats never enter the `agg()` call — zero overhead.

### When to disable top_k

Top-K is the only stat that requires a **separate pass** over the DataFrame. While a single `df.agg()` handles all other stats in one Spark job, top_k executes `df.groupBy(col).count().orderBy(desc).limit(k)` **per string column**. On a table with many string columns, this means many additional Spark jobs — each triggering a full shuffle.

**Disable top_k when:**

- **Wide tables with many string columns**: A table with 50 string columns means 50 extra `groupBy` jobs. This can easily dominate your profiling runtime.
- **Large datasets where shuffle is expensive**: Each `groupBy().count()` triggers a full shuffle. On billion-row tables, this is costly even for a single column.
- **High-cardinality string columns**: Top-K on a column with millions of unique values (UUIDs, free text) produces a large intermediate group table before limiting. The result is rarely useful.
- **Latency-sensitive pipelines**: If profiling runs in your critical path, removing the extra passes keeps runtime predictable.
- **When you only need null/distinct stats**: If you're just checking data completeness (`null_pct`, `approx_distinct`), top_k adds no value.

**How to disable:**

```yaml
# Globally (all string columns)
collection:
  type: "profiling"
  top_k: 0                                # or: exclude_stats: ["top_k"]

# Per-column (keep top_k for some, disable for others)
collection:
  type: "profiling"
  column_profiles:
    notes:
      exclude_stats: ["top_k"]            # disable for this high-cardinality column
    status:                                # status keeps top_k (useful for enum-like columns)
```

> **Spark 4.0+ note:** Spark 4.0 introduces `approx_top_k()` — an approximate
> aggregate function that can run inside `df.agg()` like any other stat. When
> AIDP upgrades to Spark 4.0+, the TextAnalyzer can be updated to use
> `approx_top_k(col, k)` instead of `groupBy().count()`, eliminating the
> separate pass entirely and making profiling truly single-pass for all stats.
