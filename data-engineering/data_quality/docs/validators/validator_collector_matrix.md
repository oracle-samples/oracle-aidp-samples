# Validator × Collector Compatibility Matrix

Which collectors can each validator consume? Most validators are
collector-agnostic (they read `metric_name` / `metric_value` rows),
but a few are wired to a specific collector and won't work with
others.

## Matrix

| Validator → / Collector ↓ | threshold | drift | trend (forecast) | shape | pattern | slo | custom_query_check |
|---------------------------|-----------|-------|------------------|-------|---------|-----|--------------------|
| **aggregation** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **metrics** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **profiling** | ✅ | ✅ | ⚠️ noisy | ❌ | ❌ | ❌ | ❌ |
| **custom_query** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| **sample** | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ |
| **recency** | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ |

Legend:
- ✅ — supported, documented combination
- ⚠️ noisy — works but produces unstable signal (per-stat variance is
  high; threshold rules tend to flap)
- ❌ — config-load rejects the pairing

## Working pairs — minimal examples

### threshold + aggregation

```yaml
- type: threshold
  collection:
    type: aggregation
    expressions: ["COUNT(*) AS row_count", "AVG(amount) AS avg_amount"]
  rules:
    - metric: row_count
      thresholds: { error: { min: 1000 } }
    - metric: avg_amount
      thresholds: { warning: { min: 50, max: 500 } }
```

### threshold + metrics

```yaml
- type: threshold
  collection:
    type: metrics
    metrics:
      revenue: "SUM(amount)"
      avg_order: "AVG(amount)"
  rules:
    - metric: revenue
      thresholds: { error: { min: 1000000 } }
```

### threshold + profiling

```yaml
- type: threshold
  collection:
    type: profiling
    columns: ["amount", "currency"]
    stats: ["null_count", "distinct_count"]
  rules:
    - metric: amount.null_count
      thresholds: { error: { max: 0 } }
    - metric: currency.distinct_count
      thresholds: { warning: { min: 5 } }
```

### threshold + custom_query

```yaml
- type: threshold
  collection:
    type: custom_query
    sql: |
      SELECT
        SUM(amount) AS total_revenue,
        SUM(amount) FILTER (WHERE refunded) / SUM(amount) AS refund_rate
      FROM {{ table }}
      WHERE ds = '{{ ds }}'
  rules:
    - metric: total_revenue
      thresholds: { error: { min: 1000000 } }
    - metric: refund_rate
      thresholds: { warning: { max: 0.05 } }
```

### drift + aggregation

```yaml
- type: drift
  collection:
    type: aggregation
    expressions: ["AVG(amount) AS avg_amount", "COUNT(*) AS row_count"]
  rules:
    - metric: avg_amount
      compare: { past_values: 4, step: P7D }
      thresholds:
        warning: { deviation_pct: { min: -25, max: 25 } }
        error:   { deviation_pct: { min: -50, max: 50 } }
```

### drift + custom_query

```yaml
- type: drift
  collection:
    type: custom_query
    sql: "SELECT SUM(amount) AS total FROM {{ table }} WHERE ds = '{{ ds }}'"
  rules:
    - metric: total
      compare: { past_values: 7, step: P1D }
      thresholds:
        warning: { rate_of_change_pct: { min: -30, max: 30 } }
```

### trend + aggregation

```yaml
- type: trend
  collection:
    type: aggregation
    expressions: ["SUM(amount) AS daily_revenue"]
  rules:
    - metric: daily_revenue
      model:
        history_count: 90
        step: P1D
        seasonality_mode: multiplicative
        interval_width: { warning: 0.80, error: 0.95 }
```

### shape + sample

```yaml
- type: shape
  collection:
    type: sample
    n_records: 10000
    slice_column: sale_date
    slice_value: "{{ ds }}"
    history: { past_dates: 3, step: P7D }
  model:
    n_estimators: 100
    contamination: auto
    explain: true
  thresholds:
    warning: { anomaly_score: 0.6 }
    error: { anomaly_score: 0.8 }
```

### pattern + sample

```yaml
- type: pattern
  collection:
    type: sample
    n_records: 10000
    slice_column: sale_date
    slice_value: "{{ ds }}"
    history: { past_dates: 3, step: P7D }
  model:
    n_estimators: 200
    cv_folds: 5
    explain: true
    exclude_columns: ["sale_date", "updated_at", "sale_id"]
  thresholds:
    warning: { auc: 0.65 }
    error: { auc: 0.80 }
```

### slo + recency

```yaml
- type: slo
  recency:
    strategy: max_column
    column: updated_at
  thresholds:
    warning: PT4H
    error: PT8H
```

### custom_query_check (convenience wrapper)

```yaml
- type: custom_query_check
  collection:
    type: custom_query
    sql: |
      SELECT COUNT(*) AS row_count
      FROM {{ table }}
      WHERE ds = '{{ ds }}'
  rules:
    - metric: row_count
      thresholds: { error: { min: 1000 } }
```

Functionally equivalent to `threshold + custom_query`; the wrapper
exists so YAML reads "I'm running a custom query check" instead of
"I'm running threshold-on-custom-query".

## Why some pairings are forbidden

- **shape / pattern require `sample`** — they consume raw rows, not
  aggregated metrics. Pairing them with aggregation / metrics gives
  the model a single one-row "current" and N one-row "past" inputs,
  which IF / RF can't separate.
- **slo requires `recency`** — slo's contract is "duration since data
  last updated"; aggregation can't produce a duration.
- **profiling + trend** — works but rarely useful: per-stat values
  (null_count, distinct_count) have high day-over-day variance,
  Prophet's smoothing fights you. Use threshold rules instead.

## What happens when you pair them anyway

Config-load rejects the combination at validation time with:

```
QualifireConfigError: Validator 'shape' requires collection type 'sample',
  got 'aggregation'. shape consumes raw rows for Isolation Forest scoring;
  aggregated metrics produce a single-row input that can't be classified.
```

---

## Filter precedence

When **both** a dataset-level `filter:` and a collector-level
`filter:` are set, the engine **AND-combines** them — both
predicates apply. Pre-2026-05-09 the dataset filter was
silently dropped (override semantics); see the migration section
in `docs/CHANGELOG.md` for the breaking-change details.

The rule applies uniformly across `aggregation`, `profiling`,
`metrics`, and `sample` collectors. `custom_query` rejects
`filter:` at config load — embed the predicate inside the `sql`
field's `WHERE` clause instead. `recency` has no
collector-level filter; the dataset filter applies as-is.

### Worked example

```yaml
datasets:
  - name: sales_us_completed
    table: catalog.schema.sales
    filter: "region = 'US'"          # broad guard: tenant / soft-delete / PII
    validations:
      - type: threshold
        collection:
          type: aggregation
          expressions:
            row_count: "COUNT(*)"
          filter: "status = 'completed'"   # narrow: this check only
        rules:
          - metric: row_count
            thresholds:
              warning: { min: 1000 }
              error:   { min: 100 }
```

Effective scope at runtime:
`(region = 'US') AND (status = 'completed')`.

### Composition contract

* The engine renders each filter's Jinja templates
  **independently** before composing. A collector filter like
  `filter: "{{ optional_predicate }}"` that resolves to `""` at
  runtime is treated as `None` (no filter) by the helper, so
  the combined string is always well-formed SQL — never
  `(region = 'US') AND ()`.
* Each operand is wrapped in its own pair of parentheses
  (`(<dataset>) AND (<collector>)`). This preserves SQL
  operator precedence when either side contains a top-level
  `OR` clause; without parentheses,
  `a OR b AND c OR d` would be parsed as
  `a OR (b AND c) OR d`, changing the truth value.
* Already-parenthesised operands keep their inner parens; you
  may see double-parens like `((a OR b)) AND (c)` in the
  rendered SQL. Engines treat this as a no-op.
* Empty / whitespace-only YAML values for `filter:` coerce to
  `None` at config load, so a `filter: ""` line is identical
  to omitting the field.

### Authoritative definition

`qualifire/collection/_filters.py:and_combine_filters` is the
single source of truth for the composition rule. Both the
engine's three filtered collector call sites (in
`QualifireEngine._compose_collector_filter`) and the sampler's
per-slice predicate composition use the same helper.

### Migration shape

If you previously relied on the override semantic — the
collector filter replacing the dataset filter — you have one of
two migration paths:

1. **Embrace the AND** — usually what you actually wanted.
   Thresholds may need re-tuning against the now-narrower row
   scope. No YAML changes required.
2. **Move the broader filter into the collector** — drop
   `DatasetConfig.filter` for that dataset and put the broader
   predicate inside each validator's collector-level `filter:`.
   When several validators share the dataset filter and only
   one needs the override behaviour, this means duplicating the
   predicate across the others.

See `docs/CHANGELOG.md` for the canonical breaking-change
notice.
