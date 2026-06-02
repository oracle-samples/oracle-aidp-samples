# Collectors

A **collector** turns rows in your source table into the metric stream
that validators check against. Every validation owns one collector.
The collector runs against the backend (Spark / pandas / JDBC),
returns a list of `CollectionResult` rows, and the engine persists
them to the system table so future runs can read history.

This folder documents each collector in depth: what it computes, the
exact YAML / Python shape, what gets written to the system table, and
the failure modes you'll trip over in production.

## Quick chooser

| Goal | Collector | Page |
|------|-----------|------|
| One or more SQL aggregates over the partition | `aggregation` | [aggregation.md](aggregation.md) |
| Named KPIs as a `name → expression` map | `metrics` | [metrics.md](metrics.md) |
| Type-aware column profiling (counts, mean / stddev, top-K, quantiles) | `profiling` | [profiling.md](profiling.md) |
| Random row sample for shape / pattern anomaly detection | `sample` | [sample.md](sample.md) |
| Arbitrary single-row SQL returning columns as metrics | `custom_query` | [custom_query.md](custom_query.md) |
| Freshness signal (max(updated_at), Delta log lookup, custom SQL) | `recency` | [recency.md](recency.md) |

## Shared concepts

These apply to every collector — read once, then the per-page docs
focus on the collector-specific bits.

### `partition_ts`

Set on `DatasetConfig` (or globally on `QualifireConfig`). It is the
identity stamp on every row the collector writes. The system table's
identity contract is `(partition_ts, dimension_value, metric_name)`,
resolved latest-by-`collected_at`.

`partition_ts` is the lookback anchor for **drift** and **forecast**
validators (which read aggregated history from the system table):
without it those checks surface a structured ERROR with
`details.missing_partition_anchor=true`. **Shape** and **pattern**
do *not* anchor on `partition_ts` — they sample raw rows from the
source table per slice using `slice_column` + `slice_value` on the
sample collection (see [sample.md](sample.md)). `partition_ts` is
still useful to set on the dataset so the sample-collection's
`metric_name="sample"` row carries a stable identity in the system
table.

```yaml
datasets:
  - name: sales_daily
    table: catalog.schema.sales
    partition_ts: "'{{ ds }}'"   # Jinja-rendered to a literal datetime
```

`partition_ts` accepts:
- `'2026-04-28'` — single-quoted ISO date literal
- `'{{ ds }}'` — single-quoted Jinja that renders to ISO date
- `event_dt` — bare column reference (per-row partition_ts via
  `qf_partition_ts AS event_dt` injection — only for aggregation /
  metrics / profiling, not the sampler)

See [`docs/validators/partition_anchoring.md`](../validators/partition_anchoring.md)
for the anchor math used by drift / forecast.

### `filter` (dataset-level)

`DatasetConfig.filter` is a SQL fragment AND-applied to every
collector run for that dataset. Use it to scope ad-hoc one-time runs
or to pin per-day partitions across a multi-validation dataset:

```yaml
datasets:
  - name: sales
    table: catalog.schema.sales
    filter: "region = 'us'"     # applies to every validation below
    partition_ts: "'{{ ds }}'"
    validations: [...]
```

When a per-collector `filter:` is *also* set on a validation,
the two predicates AND-combine. See
[filter precedence](../validators/validator_collector_matrix.md#filter-precedence)
for the composition rule, the rendered-empty Jinja contract,
and the migration shape from the pre-2026-05-09 override
behaviour.

For sample collections (shape / pattern), the dataset filter
auto-AND-combines with the sampler's per-slice predicate.

### `dimensions`

Every aggregation-shape collector accepts `dimensions: [col1, col2]`.
Output rows fan out into one per dimension combination, with
`dimension_value` set to the canonical JSON form (lowercase keys,
sorted). Validators consume each dimension as an independent series
when looking up history.

```yaml
collection:
  type: aggregation
  expressions: {revenue: "SUM(amount)"}
  dimensions: ["region", "product"]
```

A single SQL pass with `GROUP BY` produces all rows — the engine
does **not** issue one query per dimension combination.

### `filter` (collection-level, where supported)

Aggregation / metrics / profiling each carry an optional
`collection.filter` that is **AND-combined** with the dataset
filter for that one validation. Use it when one validation needs
a tighter scope than the rest of the dataset; the dataset-level
guard (soft-delete exclusions, PII / tenant predicates) still
applies. See [filter precedence](../validators/validator_collector_matrix.md#filter-precedence)
for the composition contract.

```yaml
filter: "region = 'us'"             # dataset
validations:
  - type: threshold
    collection:
      type: aggregation
      # Effective scope: (region = 'us') AND (status = 'active')
      filter: "status = 'active'"
      expressions: { active_count: "COUNT(*)" }
```

Sample collections do **not** have a collection-level filter —
they use only the dataset filter, AND-combined with the auto-generated
slice predicate.

### What lands in the system table

Every collector emits rows of shape:

| Column | Meaning |
|--------|---------|
| `metric_name` | `"revenue"`, `"row_count"`, `"sample"` (sentinel for sample collector) |
| `metric_value` | numeric value (NULL for `sample` rows) |
| `dimension_value` | canonical JSON of dim cols, NULL for non-dimensioned |
| `collection_type` | `"aggregation"` / `"metrics"` / `"profiling"` / `"sample"` / etc. |
| `partition_ts` | dataset-level or per-row anchor |
| `collected_at` | `datetime.now()` at write time — used for tiebreakers |
| `collector_name` | the validation name (so two validations sharing a sample stay traceable) |
| `details_json` | collector-specific extra (e.g. quantiles for profiling) |

Drift / forecast / anomaly validators read this back filtered by
`partition_ts ≤ current.partition_ts` and ordered descending.
