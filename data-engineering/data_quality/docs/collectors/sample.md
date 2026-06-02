# Sample Collector

Pulls N random rows from the current partition and from each historical
partition, stacks them into a single labeled DataFrame, and hands the
result to a row-level model (Isolation Forest for `shape`, Random
Forest two-sample for `pattern`).

The only collector that emits raw rows rather than aggregated metrics.

## Configuration

```yaml
collection:
  type: sample
  n_records: 10000
  slice_column: "sale_date"          # the partition column
  slice_value: "{{ ds }}"            # value at the current slice (Jinja-rendered)
  history:
    past_dates: 3
    step: P7D                         # required even when filters override
    # filters: [...]                  # optional verbatim per-slice predicates
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"sample"` | yes | discriminator |
| `n_records` | `int` | no (default 10000) | rows per slice; total = `n_records × (1 + past_dates)` |
| `slice_column` | `str` | no\* | partition column name; pairs with `slice_value` |
| `slice_value` | `str` | no\* | Jinja-renderable value (e.g. `"{{ ds }}"`); parses as ISO date / datetime |
| `history.past_dates` | `int` | no (default 3) | number of historical slices |
| `history.step` | `str` | yes | ISO 8601 duration (`"P7D"`, `"PT1H"`, `"P1W2D"`) |
| `history.filters` | `list[str]` | no | explicit per-slice WHERE predicates (verbatim mode) |

\* `slice_column` and `slice_value` must be set together. If both are
omitted you must supply `history.filters`. The Pydantic validator
rejects mismatched cases at config load.

## Slicing model

Two paths.

### Structured (recommended for date partitions)

`slice_column = '<rendered slice_value>'` for the current slice, and
`slice_column = '<slice_value − k·step>'` for past slice k. The
sampler:

1. Renders `slice_value` via Jinja, then parses it as an ISO date /
   datetime to derive an anchor.
2. For k in 1..past_dates: `past_anchor = anchor − k·step`. If the
   anchor is at midnight (typical day-grain), emits an ISO date
   string; otherwise emits the full ISO timestamp.
3. AND-combines with `DatasetConfig.filter` so dataset scope
   (`region = 'us'`) applies to every slice unchanged.

```yaml
filter: "region = 'us'"      # dataset
collection:
  type: sample
  slice_column: sale_date
  slice_value: "{{ ds }}"
  history: { past_dates: 3, step: P7D }
```

When `ds = '2026-04-28'`, the sampler issues:

| Slice | Predicate |
|-------|-----------|
| current | `(sale_date = '2026-04-28') AND (region = 'us')` |
| past_1 | `(sale_date = '2026-04-21') AND (region = 'us')` |
| past_2 | `(sale_date = '2026-04-14') AND (region = 'us')` |
| past_3 | `(sale_date = '2026-04-07') AND (region = 'us')` |

### Verbatim (escape hatch for non-date partitions)

```yaml
collection:
  type: sample
  history:
    past_dates: 3
    step: P7D     # required for rule identity, not consulted at runtime
    filters:
      - "model_version = 'v3'"
      - "model_version = 'v2'"
      - "model_version = 'v1'"
```

The sampler runs the supplied filters exactly. `DatasetConfig.filter`
does **not** auto-AND-combine in this mode — the caller owns the full
WHERE.

The current slice in this mode has no per-slice predicate; pair with
`DatasetConfig.filter` if you need to scope it.

## Programmatic shape

```python
from qualifire.core.config import SampleCollectionConfig, SampleHistoryConfig

SampleCollectionConfig(
    n_records=10000,
    slice_column="sale_date",
    slice_value="{{ ds }}",
    history=SampleHistoryConfig(past_dates=3, step="P7D"),
)
```

Or via the public builder:

```python
qf.shape_check(
    n_records=10000,
    past_dates=3,
    step="P7D",
    slice_column="sale_date",
    slice_value="{{ ds }}",
)
```

## What lands in the system table

The collector emits **one** `metric_name="sample"` row per run.
`metric_value` is NULL — the actual DataFrames are not persisted
(too large; would dominate storage). The validator processes them
in-memory and writes its own `details_json` with AUC / anomaly_ratio /
SHAP top features.

## Validators that consume it

- **shape** (Isolation Forest)
- **pattern** (Random Forest two-sample classifier)

Both use the same `SampleCollectionConfig`, so two validations on the
same dataset can share rows via the engine's sample cache (`shape +
pattern` = 1 SQL pull, not 2).

## Common pitfalls

- **`slice_value` doesn't parse as ISO** — `"v42"`, `"yesterday"`,
  random strings raise `MissingPartitionAnchorError` because the
  sampler can't compute anchors. Use ISO dates (`"2026-04-28"`),
  ISO datetimes (`"2026-04-28T12:00:00"`), or `history.filters` for
  non-temporal partitions.
- **Date Jinja in `DatasetConfig.filter`** — the dataset filter
  AND-combines unchanged with each slice predicate (current and
  past). If your dataset filter contains a temporal Jinja template
  (e.g. `event_dt >= '{{ ds }}'`), it renders to *today* for every
  slice — so past slices become
  `event_dt = '<past_date>' AND event_dt >= '<today>'`. Depending
  on how empty the result is, the classifier either trains on
  empty / single-class data (raises) or sees a wildly imbalanced
  set and produces a misleading score. Either way the result is
  unreliable. Keep `DatasetConfig.filter` to non-temporal
  qualifiers (region / status / tenant). The per-slice temporal
  predicate belongs on the sample collection via `slice_column` +
  `slice_value`.
- **Timestamp partition column with date `slice_value`** — if
  `sale_ts` is a TIMESTAMP and `slice_value="2026-04-28"`, the
  predicate `sale_ts = '2026-04-28'` only matches rows at exactly
  midnight. Either use a `sale_date` column (denormalize) or write
  `history.filters` with explicit `sale_ts >= '...' AND sale_ts < '...'`
  ranges.
- **`step` mismatch with stored history** — the sampler's anchor math
  is partition-deterministic. If `step="P7D"` but the partitions
  actually arrive every 5 days, three of the four past slices return
  zero rows, the classifier sees imbalanced classes, and reports
  AUC ≈ 0.5 regardless of true drift. Tools to detect: log the
  per-slice row count.
- **Constant-per-slice and per-row-unique columns leak class
  identity** — both `shape` (Isolation Forest) and `pattern`
  (Random Forest) leak in the same way. Two leakage modes worth
  explicit naming because they're easy to miss:
  - **Slice column** (e.g. `sale_date`). After one-hot encoding,
    `sale_date` becomes one feature per unique value
    (`sale_date_2026-05-07`, `sale_date_2026-05-06`, …). Today's
    rows have `1` in the today-feature; every past row has `0`.
    Both validators split on it trivially → AUC ≈ 1 / 100% of
    current-period rows flagged regardless of true distribution
    drift. **The engine auto-excludes the
    `slice_column` for both shape and pattern** — operators do not
    need to repeat it in `exclude_columns`.
  - **Per-row identifiers** (e.g. `sale_id`, `event_id`,
    `transaction_id`). These have a unique value per row, so the
    encoder turns them into label-encoded integers that the trees
    use as a row-discriminator. The IF / RF reads the leak
    instead of any real signal. Operators **must** add these to
    `exclude_columns` — the engine doesn't know which columns are
    row-IDs.
  - **Ingestion timestamps** (`updated_at`, `created_at`) — same
    story. Different rows have different timestamps; trees split
    on micro-second differences as a row marker. Add to
    `exclude_columns`.

  Symptom in SHAP top-features: a column named after the partition
  (e.g. `sale_date_2026-05-07`) or a per-row-unique field shows
  up with very high importance. That's leakage, not drift.

## Output of dependent validators

Pattern (with SHAP):

```
[ERROR] pattern_check: Pattern AUC: 0.91 (+/-0.03)
  - top contributing features:
      • amount (0.4200)
      • channel_mobile (0.1800)
      • merchant_id (0.0910)
  - AUC: 0.910 ± 0.030
  - sample sizes: current=500, past=1500
```

Shape:

```
[ERROR] shape_check: Anomaly ratio 0.83 (current run anomalous)
  - top contributing features:
      • amount (0.510)
      • day_of_week (0.180)
  - anomaly ratio: 0.830
```

When SHAP isn't installed (`qualifire[anomaly]` extra not present):

```
[ERROR] pattern_check: Pattern AUC: 0.91 (+/-0.03)
  - explanation unavailable: shap not installed: ...
```
