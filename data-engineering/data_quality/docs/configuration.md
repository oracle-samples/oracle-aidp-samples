# Configuration Reference

Qualifire configs are YAML or JSON files validated by Pydantic at load time. Use `qualifire validate-config --config path.yaml` to check syntax.

## Top-Level Fields

```yaml
owner: "team-name"                          # Required. Team/owner identifier.
bu: "business-unit"                         # Required. Business unit.
system_table: "catalog.schema.qf_history"   # Fully-qualified system table name.
system_table_backend: "external_catalog"    # "external_catalog" | "delta" | "sqlite" | "jdbc"
jdbc:                                       # Required only when system_table_backend == "jdbc"; see below.
  url: "jdbc:postgresql://host:5432/db"
  user: "qf_user"
  password: "secret://prod_db/password"      # Pair with Qualifire(secret_resolver=...); see docs/jinja_rendering.md#secrets
dataset_parallelism: 1                              # Parallel dataset execution (default: 1 = sequential)
context:                                    # Extra Jinja variables (optional)
  my_param: "value"

notifications:                              # Notification channel definitions (optional)
  email: { ... }
  slack: { ... }

datasets:                                   # Required. List of datasets to validate.
  - name: "..."
    ...
```

> **Note on `backend:`** — earlier versions of Qualifire accepted a
> top-level `backend: "spark"` field in YAML. That field never drove
> runtime backend selection (it was a label) and has been removed.
> Pass the runtime backend directly to `Qualifire.from_config(...)`
> or `Qualifire(backend=...)`. YAMLs that still carry `backend:`
> raise `QualifireConfigError` at load time with a migration message.

### `QualifireConfig` field reference

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `owner` | `str` | required | Team / owner identifier; stamped on every row. |
| `bu` | `str` | required | Business unit; stamped on every row. |
| `system_table` | `str` | required | Fully-qualified system table name. For SQLite, accepts a file path or `:memory:`. |
| `system_table_backend` | `Literal["external_catalog","delta","sqlite","jdbc"]` | `"external_catalog"` | One of the 4 backends. |
| `jdbc` | `JDBCConfig \| dict \| None` | `None` | JDBC connection block; required when `system_table_backend="jdbc"`. See [`## JDBC System Table`](#jdbc-system-table). |
| `dataset_parallelism` | `int` | `1` | Parallel dataset execution count. |
| `notifications` | `dict[str, NotificationChannelConfig]` | `{}` | Named channels referenced by per-validation `notify`. See [`## Notification Channels`](#notification-channels). |
| `engine_notify` | `NotifyConfig \| None` | `None` | Routing for engine-level events (persistence failures, suppression-read failures, WAP cleanup leaks). |
| `partition_ts` | `str \| None` | `None` | Run-level default `partition_ts`. Datasets without their own value inherit this. Accepts column refs, literals, Jinja, SQL functions. |
| `partition_step` | `str \| None` | `None` | Run-level default ISO-8601 duration (`P7D`, `PT1H`). Used by **`qualifire backfill`** range expansion (`--partition START..END`); history-backed validators read their cadence from their OWN rule (`compare.step` / `model.step` / `history.step`), not from this field. |
| `context` | `dict[str, str]` | `{}` (empty dict via `Field(default_factory=dict)`) | Extra Jinja variables merged into every render. |
| `datasets` | `list[DatasetConfig]` | required | One per table / query / DataFrame / WAP block. |

**Common pitfalls**:

- Confusing run-level `partition_step` (backfill range
  cadence) with per-rule `compare.step` / `model.step` /
  `history.step` (validator history cadence). The validators
  read their own step; this run-level field is for backfill
  range expansion only.
- Setting `system_table_backend: "jdbc"` without a `jdbc:` block.
  `qualifire validate-config` catches this; runtime would raise
  with a clearer message than a deep stack trace.
- Mixing `system_table` paths and bare names within a single
  pipeline (e.g. `Qualifire(system_table="/tmp/qf.db")` plus
  `QualifireConfig(system_table="qf")`) triggers a
  `QualifireReinitWarning` on every run. Set both to the same
  value, or pass `warn_on_reinit=False` to silence intentionally.

## Constructing from YAML

The recommended way to wire a runtime backend to a YAML config is
`Qualifire.from_config(...)`:

```python
from qualifire.api import Qualifire
from qualifire.backends.spark_backend import SparkBackend

qf = Qualifire.from_config('configs/quality.yaml', backend=SparkBackend(spark))
qf.run_config('configs/quality.yaml', context={'ds': '2026-05-09'})
```

`from_config` lifts `owner`, `bu`, `system_table`,
`system_table_backend`, and `jdbc` from the YAML onto the
instance. Per-config fields (`notifications`, `engine_notify`,
`partition_ts`, `partition_step`, `dataset_parallelism`,
`context`, `datasets`) stay attached to each run's config.

To inject a runtime notifier instance (e.g. a test capture)
that wins over YAML's `notifications:` block on every call,
pass it via `notifiers={}`:

```python
qf = Qualifire.from_config(
    'configs/quality.yaml',
    backend=SparkBackend(spark),
    notifiers={'inbox': captured_notifier},
)
```

The `notifiers={}` mapping always wins — at construction and on
every subsequent `run_config` / `backfill` / `validate` call.

## JDBC System Table

Setting `system_table_backend: jdbc` stores validation history in any
JDBC-reachable database (Oracle, PostgreSQL, MySQL, ...) via Spark's
JDBC DataFrame reader/writer. The `jdbc:` block is required at config
load time — omitting it fails preflight with a pointer to add the
block.

```yaml
system_table: "qf_history"                  # Table name inside the JDBC database.
system_table_backend: "jdbc"
jdbc:
  url: "jdbc:postgresql://db.internal:5432/qualifire"   # Required.
  user: "qf_user"                                        # Optional (also acceptable inside properties).
  password: "secret://prod_db/password"                  # Optional; resolved by Qualifire(secret_resolver=...). Plain ${ENV_VAR} interpolation is NOT supported — see docs/jinja_rendering.md#secrets.
  driver: "org.postgresql.Driver"                        # Optional; normally auto-detected from URL.
  properties:                                            # Optional; passed through to Spark's JDBC writer.
    fetchsize: "1000"
    batchsize: "500"
```

**Notes:**

- `user`, `password`, and `driver` are merged into `properties` before
  being handed to Spark. Any value already present in `properties`
  wins, so you can put everything in `properties` if you prefer.
- A `from_secret: "name"` directive on `jdbc:` populates `url` /
  `user` / `password` / `driver` from a single resolver-backed
  dict; mutually exclusive with literal field values. See
  [secrets](jinja_rendering.md#secrets).
- Spark attempts to auto-create the table on first write; if the
  database denies that, pre-create the table with the system-table
  schema (see `qualifire.storage.base.SYSTEM_TABLE_COLUMNS`).
- Reporting (`qualifire report`) requires a SparkSession — JDBC reads
  go through Spark. Use `sqlite` if you need Spark-less reporting.

## External Catalog System Table (AIDP-only)

Operators using `system_table_backend: external_catalog` get
hardened initialise + write semantics against an **AIDP Master
Catalog external connection** mounted over Oracle ALH / ADW / ATP.
The backend is now AIDP-specific — it forces the writer's data
source to `aidataplatform`, which only exists in AIDP-shipped
Spark distributions. Non-AIDP deployments (Unity Catalog,
Iceberg REST, governed Hive, plain Hive metastore) should use
`system_table_backend: delta` or `jdbc` instead.

### Eager table create via the AIDP connector

`initialize()` creates the system table via
`empty_df.write.format("aidataplatform").mode("ignore")
.saveAsTable("<catalog>.<schema>.<table>")` on first run. The
explicit `format("aidataplatform")` is load-bearing — it forces
Spark to route the create through the AIDP connector instead of
falling through to Spark's session-default source (Delta on AIDP
Workbench), which would land the table in OCI Object Storage
instead of the underlying Oracle Autonomous Database.

The writer chain carries **no `USING` override, no `LOCATION`,
no `TBLPROPERTIES`** beyond the format pin — the AIDP connector
decides physical layout against the Master Catalog mapping.

The create is gated behind a `DESCRIBE TABLE` probe so
insert-only operators (DBA pre-created the table, qualifire
identity has only INSERT privilege) don't hit a create failure
on a table that already exists. A narrow race window between
the probe and the create surfaces as
`TableAlreadyExistsException` and is treated as success.

### Fail-loud on missing schema

`CREATE SCHEMA IF NOT EXISTS` is **not** issued. Operators provision
catalog schemas out-of-band on every supported external catalog;
qualifire's previous best-effort schema creation was decorative.
Missing schema surfaces as a structured `QualifireSystemTableError`
during `initialize()` with one of four classified remediation hints:

| Bucket | Cause | Operator action |
|---|---|---|
| Namespace not found | DBA hasn't created the catalog schema | Ask DBA to create the schema once; re-run |
| Read-only catalog | Catalog is read-only mode | Switch to a writable catalog or pre-create on a mirror |
| Privilege denied | Qualifire identity lacks CREATE on the schema | Ask DBA to pre-create the table; INSERT-only is sufficient |
| Connector-rejected | `aidataplatform` data source isn't available or rejected the write | Verify the connector jar is on the Spark classpath and the catalog is registered in AIDP Master Catalogs over an Oracle Autonomous Database; for non-AIDP deployments switch to `delta` or `jdbc` backend |

Classifier order: namespace → read-only → privilege →
connector-rejected. Dual-phrase errors (Glue / Ranger style
"DATABASE NOT FOUND" + "INSUFFICIENT PRIVILEGES") route to the
upstream cause first.

### Skip object-storage staging

Every system-table write carries
`.option("skip.oos.staging", "true")` automatically. AIDP /
Oracle 23ai catalogs honour this option to bypass the object-
storage staging hop for in-database writes; non-AIDP catalogs
silently ignore unknown writer options. Operators don't need to
configure anything — the option is hard-coded in the
`external_catalog` backend.

### `insertInto` over `saveAsTable`

Writes use the DataFrame writer's `insertInto(catalog.schema.table)`
not `saveAsTable(...)`. `insertInto` is schema-strict (rejects
column-shape drift at write time) and doesn't update table
metadata, so insert-only credentials work. Schema-strict is the
right behaviour for the system table — silent column drift would
corrupt drift / forecast / pattern history.

## Notification Channels

### Email (SMTP)

```yaml
notifications:
  email:
    type: email
    smtp_host: "smtp.company.com"
    smtp_port: 587                  # default: 587
    smtp_user: "user"               # optional
    smtp_password: "pass"           # optional
    use_tls: true                   # default: true
    sender: "qualifire@company.com" # optional, defaults to smtp_user
    recipients:
      - "team@company.com"
      - "oncall@company.com"
```

### Slack (Webhook)

```yaml
notifications:
  slack:
    type: slack
    webhook_url: "https://hooks.slack.com/services/T.../B.../xxx"
```

### Generic Webhook

```yaml
notifications:
  pagerduty:
    type: webhook
    url: "https://events.pagerduty.com/v2/enqueue"
    method: POST               # POST (default) or PUT
    headers:
      Authorization: "Token token=xxx"
```

## Dataset Config

A dataset requires exactly **one** source: `table`, `query`, or `wap`.

```yaml
datasets:
  - name: "sales_daily"                    # Required. Unique identifier.
    description: "Daily sales fact table." # Optional. Persisted on every system-table row produced by this dataset.
    table: "catalog.schema.sales"           # Table source (mutually exclusive with query/wap).
    filter: "date = '{{ ds }}'"             # Optional. Jinja-templated WHERE clause.
    partition_ts: "'{{ ds }}'"              # Required for drift / forecast / shape / pattern
                                            # (anchor − k·step history reads). Literal
                                            # (`'2026-04-28'`) or Jinja (`'{{ ds }}'`) wins for
                                            # single-anchor runs; column refs (e.g. `event_dt`)
                                            # are supported only when paired with
                                            # `dimensions: [event_dt]` so SELECT can GROUP BY.
                                            # See validators/partition_anchoring.md.
    cache: false                            # Cache table for faster multi-validation reads (default: false)
    cache_storage_level: "MEMORY_AND_DISK"  # Spark StorageLevel (default: MEMORY_AND_DISK)
    validation_parallelism: 1                 # Parallel collect+validate pipelines (default: 1)
    validations:                            # List of validation configs.
      - type: "slo"
        name: "sales_freshness"             # Optional. Stable identity for system-table grouping.
        description: "Sales must refresh hourly."   # Optional. Persisted alongside dataset_description.
        ...
      - type: "threshold"
        ...
```

> **`partition_ts`** is the anchor used by drift and forecast
> validators to read history. Both the dataset-level expression and
> each rule's `step` (cadence) are required when using those
> validators — see
> [validators/partition_anchoring.md](validators/partition_anchoring.md).
>
> **`description`** fields on the dataset and on each validation are
> persisted as `dataset_description` / `validation_description`
> columns on every system-table row, so dashboards can surface them
> without an extra metadata lookup.

### `DatasetConfig` field reference

Cross-link: see the class docstring in
`qualifire/core/config.py:DatasetConfig` for the operator-mental-model intro.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `name` | `str \| None` | `None` (defaults to `table` for single-table datasets) | Logical identity for system-table persistence + history. Required for `query` / `df` / `wap` (history would otherwise collapse unrelated workloads onto the same key). |
| `description` | `str \| None` | `None` | Operator-facing prose; persisted as `dataset_description` on every system-table row. |
| `table` | `str \| None` | `None` | Fully-qualified table or view name. Can be combined with `df` (df supplies the data; table supplies the logical identity). Mutually exclusive with `query` and `wap`. |
| `query` | `str \| None` | `None` | A SELECT (possibly Jinja-templated). Materialised as a temp view. Mutually exclusive with `table` / `df` / `wap`. |
| `df` | `Any` | `None` | In-memory DataFrame (programmatic-only; not in YAML). Can be combined with `table` (see above); mutually exclusive with `query` / `wap`. |
| `wap` | `WAPConfig \| None` | `None` | WAP lifecycle block (see [`## WAP (Write-Audit-Publish)`](#wap-write-audit-publish)). Mutually exclusive with `table` / `query` / `df`. |
| `filter` | `str \| None` | `None` | Dataset-level WHERE clause (Jinja-templated). AND-combined with any per-collector filter at the engine boundary. |
| `dimensions` | `list[str] \| None` | `None` | Column names to GROUP BY (one collected row per dim value). Each entry must match the SQL identifier regex (validator). Conventional pairing: when `partition_ts` is a column ref, list it in `dimensions` so the collector's SELECT can GROUP BY it. |
| `measures` | `list[str] \| None` | `None` | Declared measure columns (validated against result schema in `query` mode). |
| `partition_ts` | `str \| None` | `None` (falls back to run-level `QualifireConfig.partition_ts`) | First-class partition identity. Accepts column refs (`event_dt`), Jinja literals (`'{{ ds }}'`), or SQL functions (`CURRENT_TIMESTAMP`). Drives `anchor − k·step` history reads. |
| `partition_step` | `str \| None` | `None` (falls back to run-level) | Dataset-level ISO-8601 duration for **backfill range expansion** (`qualifire backfill --partition START..END`). Per-rule cadence for history-backed validators lives on the rule itself (`compare.step` / `model.step` / `history.step`). |
| `cache` | `bool` | `False` | Cache the materialised DataFrame for faster multi-validation reads. |
| `cache_storage_level` | `str` | `"MEMORY_AND_DISK"` | Spark `StorageLevel` name when `cache=True`. |
| `validation_parallelism` | `int` | `1` | Parallel collect+validate pipelines within this dataset. |
| `validations` | `list[ValidationConfig]` | `[]` | One or more validation configs (Threshold / SLO / Drift / Trend / Shape / Pattern). |
| `redacted_columns` | `list[str]` | `[]` | Source columns whose names get replaced with `"<redacted>"` in SHAP / drift / schema-change emissions. See [`docs/CHANGELOG.md`](CHANGELOG.md) § webhook-payload-redaction. |
| `allowlist_columns` | `list[str] \| None` | `None` | When set, columns NOT in this list are also redacted (intersected with the instance-level allowlist). |

**Common pitfalls**:

- Setting `table:` and `query:` together — rejected at
  config load. (`table:` + `df:` IS allowed: df supplies the
  data, table supplies the identity.)
- Using a column-ref `partition_ts` (e.g. `partition_ts:
  event_dt`) without listing it in `dimensions:` — the
  collector's SELECT can't GROUP BY a column it didn't
  ask for. Not a Pydantic error; a runtime SQL error.
- Forgetting `name:` on a `query:` or `wap:` dataset — defaults
  collapse unrelated workloads onto the same system-table key.
- Mistaking `filter:` for a per-collector filter — both apply
  via AND-combine; if you set the same predicate in both
  places, you'll filter twice.

### Query Source (Custom SQL)

Use `query` instead of `table` to validate metrics from joins, CTEs, or subqueries. The query result is materialized as a temp view and all validations run against it.

```yaml
datasets:
  - name: "cross_table_revenue"
    query: |
      SELECT
        o.region,
        SUM(o.quantity * p.price) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS order_count
      FROM catalog.schema.orders o
      JOIN catalog.schema.products p ON o.product_id = p.product_id
      WHERE o.order_date = '{{ ds }}'
      GROUP BY o.region
    dimensions:                            # Optional. Declared dimension columns.
      - region
    measures:                              # Optional. Declared measure columns.
      - total_revenue
      - order_count
    cache: true                            # Cache query result (recommended for multiple validations)
    validations:
      - type: "threshold"
        collection:
          type: "aggregation"
          expressions:
            global_revenue: "SUM(total_revenue)"
        rules:
          - metric: "global_revenue"
            thresholds:
              error: { min: 50000 }
```

**Notes:**
- `query` and `table` are mutually exclusive (also mutually exclusive with `wap`)
- `query` supports Jinja2 templating (`{{ ds }}`, `{{ today }}`, custom context)
- `dimensions` and `measures` are optional — when declared, the engine validates that all listed columns exist in the query result before running any validations
- `filter` is ignored for query datasets (include filtering in the query itself)
- All existing collection types (aggregation, profiling, metrics, sample, custom_query) and validation types (threshold, drift, trend, shape, pattern, SLO) work with query datasets

## Validation Types

### SLO Check

```yaml
- type: "slo"
  recency:
    strategy: "max_column"      # max_column | delta_log | metadata | custom_sql
    column: "updated_at"        # Required for max_column
    sql: "SELECT ..."           # Required for custom_sql
  thresholds:
    warning: "PT4H"               # Duration: 4H, 2D, 1W, 30M, 1D4H
    error: "PT8H"
  notify:
    on_success: ["email"]             # optional — notify on success (default: disabled)
    warning: ["email"]
    error: ["email", "slack"]
```

> Duplicate-alert suppression is a runtime flag now (`--skip-renotification` /
> `Qualifire.run_config(skip_renotification=True)`), not a per-validation YAML
> field. See `notifications.md` for details.

#### `SLOValidationConfig` field reference

Cross-link: `qualifire/core/config.py:SLOValidationConfig`.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["slo"]` | `"slo"` | The discriminator. |
| `name` | `str \| None` | `None` (engine generates `slo.<dataset>` if unset) | Stable validation identifier. When set, must match `[a-zA-Z0-9_.\-]+`. |
| `description` | `str \| None` | `None` | Operator-facing prose; persisted on every row. |
| `recency` | `RecencyCollectionConfig` | required | How recency is computed (see below). |
| `thresholds` | `dict[str, str]` | required | Conventional keys: `warning` and `error`. Values are ISO-8601 duration strings (`PT4H`, `P1D`). The Pydantic type is `dict[str, str]` with no per-key validator — extra keys load silently; the validator at runtime only reads `warning` / `error`. |
| `on_empty_data` | `Literal["pass","warning","error"]` | `"warning"` | Severity emitted when the source has no rows. |
| `notify` | `NotifyConfig` | `NotifyConfig()` | Per-validation routing (see [`## Notification Channels`](#notification-channels)). |

**`recency.strategy`** options:

| Strategy | Required field | What it does |
|----------|----------------|--------------|
| `max_column` | `column: str` | `recency = MAX(<column>)`. Most common. |
| `custom_sql` | `sql: str` | Arbitrary SQL returning a single TIMESTAMP. |
| `delta_log` | (none) | Reads Delta transaction-log commit timestamp. |
| `metadata` | (none) | Reads file-system / catalog metadata. |

**Common pitfalls**:

- Wrong threshold form — values must be ISO-8601 duration
  STRINGS (`"PT4H"`), not Python `timedelta` objects.
- SLO is OUT of scope for `skip_recollection` —
  replaying a cached `recency` value would defeat the
  freshness contract. The runtime flag's
  `hasattr(val_config, "expected_metrics")` guard skips SLO
  automatically; no operator action needed.
- `max_column` requires a TIMESTAMP-typed column; passing a
  DATE column produces a delta in days rather than seconds.

### Threshold Check

```yaml
- type: "threshold"
  collection:
    type: "aggregation"         # aggregation | profiling | metrics | custom_query
    expressions:
      avg_sales: "AVG(sales_amount)"
      row_count: "COUNT(*)"
      null_pct: "SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)"
  rules:
    - metric: "avg_sales"
      thresholds:
        warning: { min: 100 }
        error: { min: 50, max: 10000 }
    - metric: "null_pct"
      thresholds:
        warning: { max: 5.0 }
        error: { max: 10.0 }
  notify:
    error: ["email", "slack"]
```

**Threshold operators**: `min`, `max`, `eq`, `neq`, `gt`, `lt`, `gte`, `lte`

#### `ThresholdValidationConfig` field reference

Cross-link: `qualifire/core/config.py:ThresholdValidationConfig`.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["threshold"]` | `"threshold"` | Discriminator. |
| `name` | `str \| None` | `None` | Validator identity. When set, must match `[a-zA-Z0-9_.\-]+`. |
| `description` | `str \| None` | `None` | Operator-facing prose. |
| `collection` | `CollectionConfig` | required | Aggregation / Profiling / Metrics / CustomQuery / Sample. |
| `rules` | `list[ThresholdRuleConfig]` | required | One rule per metric. |
| `on_empty_data` | `Literal["pass","warning","error"]` | `"warning"` | Severity on empty source. |
| `notify` | `NotifyConfig` | `NotifyConfig()` | Per-validation routing. |
| `expected_value_compact` | `bool` | `True` | When True, persisted `expected_value` only carries the bounds the operator set; unset operator slots are pruned. |

#### `ThresholdRuleConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `metric` | `str` | required | Name of a metric emitted by the collector. |
| `thresholds` | `ThresholdLevels` | required | The level vocabulary below. |

#### `ThresholdLevels` + `ThresholdBounds` vocabulary (shared)

`ThresholdLevels` has two optional keys — `warning` and `error`
— each of type `ThresholdBounds`. `ThresholdBounds` accepts:

| Operator | Type | Meaning |
|----------|------|---------|
| `min` | `float \| None` | metric value `>= min` (fail otherwise) |
| `max` | `float \| None` | metric value `<= max` |
| `eq` | `float \| None` | metric value `== eq` |
| `neq` | `float \| None` | metric value `!= neq` |
| `gt` | `float \| None` | strict `>` |
| `lt` | `float \| None` | strict `<` |
| `gte` | `float \| None` | metric value `>= gte` (same operator semantics as `min`; independent field — both can coexist) |
| `lte` | `float \| None` | metric value `<= lte` (same operator semantics as `max`; independent field — both can coexist) |

Combining operators within a single level (`{min: 50, max: 100}`)
defines a range. All operators are `AND`-ed.

**Common pitfalls**:

- Setting an `error` level less strict than `warning` —
  e.g. `warning: {min: 100}, error: {min: 50}` silently
  evaluates correctly but is logically inverted; expect
  warnings to fire constantly without errors ever escalating.
- Using a bare number (`error: 100`) — not supported by
  `ThresholdBounds`; use `error: {min: 100}` instead.
- Forgetting that `expected_value_compact=True` (the default)
  prunes the persisted `expected_value` to only the operators
  you set — readers comparing today's row to a year-old row
  may see different shapes if you tightened the rule.

### Historical Comparison

```yaml
- type: "drift"
  collection:
    type: "aggregation"
    expressions:
      avg_sales: "AVG(sales_amount)"
  rules:
    - metric: "avg_sales"
      compare:
        past_values: 3              # Number of historical values to compare
        step: "P7D"                  # Interval between values
        missing_strategy: "ignore"  # ignore | substitute | error
      thresholds:
        warning: { deviation_pct: 20 }
        error: { deviation_pct: 50 }
  notify:
    warning: ["email"]
```

**Comparison modes**: `deviation_pct` (% from mean), `deviation_abs` (absolute), `z_score`, `rate_of_change_pct`, `rate_of_change_abs`

#### `HistoricalValidationConfig` field reference

Cross-link: `qualifire/core/config.py:HistoricalValidationConfig`.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["drift"]` | `"drift"` | Discriminator. |
| `name` | `str \| None` | `None` | Validator identity. When set, must match `[a-zA-Z0-9_.\-]+`. |
| `description` | `str \| None` | `None` | Operator-facing prose. |
| `collection` | `CollectionConfig` | required | Same as Threshold. |
| `rules` | `list[HistoricalRuleConfig]` | required | One rule per metric. |
| `on_empty_data` | `Literal["pass","warning","error"]` | `"warning"` | Severity on empty source. |
| `notify` | `NotifyConfig` | `NotifyConfig()` | Per-validation routing. |

#### `HistoricalRuleConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `metric` | `str` | required | Name of a metric emitted by the collector. |
| `compare` | `HistoricalCompareConfig` | required | History lookup + sparse-history policy (below). |
| `thresholds` | `HistoricalThresholds` | required | Per-measure bounds. |

#### `HistoricalCompareConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `past_values` | `int` | `3` | Number of past partitions to look back. |
| `step` | `str` | required | ISO-8601 cadence (`P7D`, `PT1H`). Drives `anchor − k·step`. |
| `missing_strategy` | `Literal["ignore","substitute","error"]` | `"ignore"` | What to do when SOME of the requested past partitions are absent. |
| `on_missing_history` | `Literal["ignore","warn","error"]` | `"ignore"` | What to do when NO history exists at all (cold start). |

#### `HistoricalThresholds`

Two top-level keys, `warning` and `error`, each
`dict[str, dict[str, float]] | None`. Each per-measure key
inside is one of `deviation_pct`, `z_score`,
`rate_of_change_pct`, `deviation_abs`, `rate_of_change_abs`.
Each per-measure value is a `{min, max}` dict (either bound
optional) — signed bounds:
`error: {deviation_pct: {min: -50, max: 100}}` fires when
value falls outside the range; useful when a 50% drop should
fire but a 100% spike is fine, or vice versa.

**Common pitfalls**:

- Omitting `compare.step` — required for the partition-anchored
  history read. Loading fails at config parse with a clear
  message.
- Confusing `missing_strategy` (some-past-missing) with
  `on_missing_history` (no-history-at-all). The first fires
  per-rule when a few past partitions are absent; the second
  is the cold-start policy on first-ever-run.
- Using `deviation_pct` with metric values that can naturally
  be 0 — division by zero falls back per Pydantic; check the
  emitted `deviation_pct` value before tuning the threshold.

### Forecast (Prophet)

```yaml
- type: "trend"
  collection:
    type: "aggregation"
    expressions:
      avg_sales: "AVG(sales_amount)"
  rules:
    - metric: "avg_sales"
      model:
        history_count: 90                # default: 90
        step: "P1D"                       # required (no default)
        changepoint_prior_scale: 0.05    # default: 0.05
        seasonality_prior_scale: 10.0    # default: 10.0
        seasonality_mode: "additive"     # additive | multiplicative
        interval_width:
          warning: 0.80                  # default: 0.80
          error: 0.95                    # default: 0.95
  notify:
    warning: ["email"]
    error: ["email", "slack"]
```

#### `ForecastValidationConfig` field reference

Cross-link: `qualifire/core/config.py:ForecastValidationConfig`.
Requires the `[forecast]` extra (Prophet).

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["trend"]` | `"trend"` | Discriminator. |
| `name` | `str \| None` | `None` | Validator identity. When set, must match `[a-zA-Z0-9_.\-]+`. |
| `description` | `str \| None` | `None` | Operator-facing prose. |
| `collection` | `CollectionConfig` | required | Same as Threshold. |
| `rules` | `list[ForecastRuleConfig]` | required | One rule per metric. |
| `on_empty_data` | `Literal["pass","warning","error"]` | `"warning"` | Severity on empty source. |
| `notify` | `NotifyConfig` | `NotifyConfig()` | Per-validation routing. |

#### `ForecastRuleConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `metric` | `str` | required | Name of a metric emitted by the collector. |
| `model` | `ForecastModelConfig` | required | Prophet model config (below). |

#### `ForecastModelConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `history_count` | `int` | `90` | Past partitions fed to Prophet for fit. |
| `step` | `str` | required | ISO-8601 cadence anchoring the history. |
| `changepoint_prior_scale` | `float` | `0.05` | Prophet hyperparameter. |
| `seasonality_prior_scale` | `float` | `10.0` | Prophet hyperparameter. |
| `seasonality_mode` | `Literal["additive","multiplicative"]` | `"additive"` | Prophet seasonality form. |
| `interval_width` | `ForecastIntervalWidth` | `ForecastIntervalWidth()` | `warning` / `error` confidence interval widths. |
| `on_missing_history` | `Literal["ignore","warn","error"]` | `"ignore"` | Cold-start policy. |

**Common pitfalls**:

- Setting `history_count` low (e.g. 7) — Prophet needs enough
  history to learn seasonality; below ~30 partitions the
  forecast band is wildly wide and the validator emits
  near-PASS even on real anomalies.
- Using `seasonality_mode: "multiplicative"` on a metric that
  crosses zero — Prophet hard-fails on negative values in
  multiplicative mode. Use additive or filter zeros upstream.

### Anomaly Detection (Isolation Forest + SHAP)

```yaml
- type: "shape"
  collection:
    type: "sample"
    n_records: 10000                     # default: 10000
    slice_column: "date"                    # column to slice on
    slice_value: "{{ ds }}"            # value at the current slice
    history:
      past_dates: 3                      # default: 3
      step: "P7D"                         # required (no default)
  model:
    n_estimators: 100                    # default: 100
    contamination: "auto"                # default: "auto"
    explain: true                        # default: true (SHAP)
    drop_complex: false                  # default: false
    alert_on_schema_change: false        # default: false
  thresholds:
    warning: { anomaly_score: 0.6 }
    error: { anomaly_score: 0.8 }
  notify:
    error: ["email", "slack"]
```

#### `AnomalyDetectionValidationConfig` field reference

Cross-link: `qualifire/core/config.py:AnomalyDetectionValidationConfig`.
Requires the `[anomaly]` extra (sklearn + SHAP).

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["shape"]` | `"shape"` | Discriminator. |
| `name` | `str \| None` | `None` | Validator identity. When set, must match `[a-zA-Z0-9_.\-]+`. |
| `description` | `str \| None` | `None` | Operator-facing prose. |
| `collection` | `SampleCollectionConfig` | required | Sample collector (see below). |
| `model` | `AnomalyModelConfig` | `AnomalyModelConfig()` | IF model knobs. |
| `thresholds` | `AnomalyThresholdConfig` | `AnomalyThresholdConfig()` | `warning` / `error` levels on `anomaly_score`. |
| `on_empty_data` | `Literal["pass","warning","error"]` | `"warning"` | Severity on empty source. |
| `notify` | `NotifyConfig` | `NotifyConfig()` | Per-validation routing. |

#### `AnomalyModelConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `n_estimators` | `int` | `100` | IF tree count. |
| `contamination` | `str \| float` | `"auto"` | IF contamination fraction. |
| `explain` | `bool` | `True` | Compute SHAP top-contributing-features. |
| `explain_value_drift` | `bool` | `True` | Emit per-feature drift entries (numeric / onehot / boolean shapes). |
| `drift_breakdown_by_slice` | `bool` | `False` | Adds per-past-slice deltas to `value_drift_explainer` (top-3 entries with kind in {numeric, onehot, boolean}). See [`docs/CHANGELOG.md`](CHANGELOG.md) § drift-explainer-per-slice-breakdown. |
| `drop_complex` | `bool` | `False` | Drop columns sklearn can't encode (lists, structs). |
| `alert_on_schema_change` | `bool` | `False` | Emit a `.schema` sub-row when new / dropped / inconsistent columns appear between current and past. |
| `exclude_columns` | `list[str]` | `[]` | Columns dropped before fitting (typically per-row IDs + ingestion timestamps to prevent trivial separation). |
| `on_missing_history` | `Literal["ignore","warn","error"]` | `"ignore"` | Cold-start policy. |

**Common pitfalls**:

- Skipping `model.exclude_columns` for tables with high-cardinality
  per-row IDs (`order_id`, `event_id`) — the IF trivially splits
  on them and flags 100% as anomalous.
- `model.contamination="auto"` (the default) can produce noisy
  scores on small samples; for `n_records < 5000` consider
  `contamination=0.05` (5%).

### Pattern Check (Random Forest two-sample + SHAP)

```yaml
- type: "pattern"
  collection:
    type: "sample"
    n_records: 10000                     # default: 10000
    slice_column: "sale_date"               # column to slice on
    slice_value: "{{ ds }}"            # value at the current slice
    history:
      past_dates: 3                      # default: 3
      step: "P7D"                         # required (no default)
      # Optional: explicit per-slice filters. When set, the length
      # MUST match past_dates — config-load rejects a mismatch.
      # filters:
      #   - "sale_date = '2026-04-01'"
      #   - "sale_date = '2026-04-08'"
      #   - "sale_date = '2026-04-15'"
  model:
    n_estimators: 200                    # default: 200
    max_depth: 8                         # default: 8 (null = unlimited)
    class_weight: "balanced"             # balanced | balanced_subsample | null
    random_state: 42                     # default: 42
    cv_folds: 5                          # default: 5; accepted range [2, 10]
    explain: true                        # default: true (SHAP)
    drop_complex: false                  # default: false
    alert_on_schema_change: false        # default: false
    on_missing_history: "ignore"         # ignore | warn | error
    # Leakage-control knob. Drop partition / date / ingestion-timestamp
    # / ID columns BEFORE encoding — otherwise the classifier learns
    # to separate runs trivially (AUC ~ 1.0) even when the business
    # distribution is unchanged.
    exclude_columns:
      - "sale_date"
      - "updated_at"
      - "sale_id"
  thresholds:
    warning: { auc: 0.65 }               # default if omitted
    error:   { auc: 0.80 }               # default if omitted
  notify:
    error: ["email", "slack"]
```

#### `PatternValidationConfig` field reference

Cross-link: `qualifire/core/config.py:PatternValidationConfig`.
Requires the `[anomaly]` extra (sklearn + SHAP).

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["pattern"]` | `"pattern"` | Discriminator. |
| `name` | `str \| None` | `None` | Validator identity. When set, must match `[a-zA-Z0-9_.\-]+`. |
| `description` | `str \| None` | `None` | Operator-facing prose. |
| `collection` | `SampleCollectionConfig` | required | Sample collector. |
| `model` | `PatternModelConfig` | `PatternModelConfig()` | Random Forest knobs (below). |
| `thresholds` | `PatternThresholdConfig` | `PatternThresholdConfig()` | `warning` / `error` on AUC. |
| `on_empty_data` | `Literal["pass","warning","error"]` | `"warning"` | Severity on empty source. |
| `notify` | `NotifyConfig` | `NotifyConfig()` | Per-validation routing. |

#### `PatternModelConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `n_estimators` | `int` | `200` | RF tree count. Validator-constrained to `>= 1`. |
| `max_depth` | `int \| None` | `8` | RF tree depth. Validator-constrained to `>= 1` when set. |
| `class_weight` | `Literal["balanced","balanced_subsample"] \| None` | `"balanced"` | RF class weighting. |
| `random_state` | `int` | `42` | RF random seed. |
| `cv_folds` | `int` | `5` | Cross-validation folds. Validator-constrained to `>= 2`. |
| `explain` | `bool` | `True` | Compute SHAP top-contributing-features. |
| `explain_value_drift` | `bool` | `True` | Emit per-feature drift entries. |
| `drift_breakdown_by_slice` | `bool` | `False` | Adds per-past-slice deltas to `value_drift_explainer` (top-3 entries with kind in {numeric, onehot, boolean}). See [`docs/CHANGELOG.md`](CHANGELOG.md) § drift-explainer-per-slice-breakdown. |
| `drop_complex` | `bool` | `False` | Drop columns sklearn can't encode. |
| `alert_on_schema_change` | `bool` | `False` | Emit `.schema` sub-row on column changes. |
| `exclude_columns` | `list[str]` | `[]` | **Load-bearing leakage control** — list per-row IDs + ingestion-timestamp columns the classifier could trivially split on. Validator-constrained: each entry must match the SQL identifier regex. |
| `on_missing_history` | `Literal["ignore","warn","error"]` | `"ignore"` | Cold-start policy. |

#### `PatternThresholdConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `warning` | `dict[str, float] \| None` | `None` (validator dispatch substitutes `0.65`) | Only `{auc: <float>}` is accepted. |
| `error` | `dict[str, float] \| None` | `None` (validator dispatch substitutes `0.80`) | Only `{auc: <float>}` is accepted. |

AUC near 0.5 means current and past are indistinguishable
(good); near 1.0 means clearly separable (drift). Default
thresholds work for most cases.

**Common pitfalls**:

- Skipping `model.exclude_columns` — the most common pattern
  config bug. A partition / date / ingestion-timestamp / ID
  column in the feature matrix produces trivial AUC = 1.0
  on every run regardless of true drift. Always list these.
- Pattern needs ENOUGH samples to fit a Random Forest with
  `cv_folds=5` — for `n_records < 1000` consider reducing
  `cv_folds` to 3.
- Setting `n_estimators` very low (< 50) — high variance per
  fold; AUC can flap PASS/WARN between runs on the same data.

## Collection Types

| Type | Fields | Description |
|---|---|---|
| `aggregation` | `expressions`, `filter`, `dimensions` | Mapping of `metric_name: SQL expression` (e.g., `avg_sales: "AVG(sales_amount)"`) |
| `profiling` | `columns`, `stats`, `exclude_stats`, `filter`, `top_k`, `quantiles`, `column_profiles`, `dimensions` | Type-aware column profiling (single-pass). See [Profiling](#profiling-collection) |
| `metrics` | `metrics`, `filter`, `dimensions` | Named KPI metrics as `name: SQL expression` mapping |
| `sample` | `n_records`, `slice_column`, `slice_value`, `history` | Random N records for anomaly detection |
| `custom_query` | `sql`, `dimensions` | Arbitrary SQL. Single-row when `dimensions` is unset; one row per dim combo when `dimensions` is set. Put any filtering inside `sql`'s `WHERE`. |

### `AggregationCollectionConfig` field reference

Cross-link: `qualifire/core/config.py:AggregationCollectionConfig`.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["aggregation"]` | `"aggregation"` | Discriminator. |
| `expressions` | `dict[str, str]` | required | `{metric_name: sql_expression}`. Each value is a SQL aggregate like `COUNT(*)` or `AVG(amount)`. Validator-constrained: must be non-empty. |
| `filter` | `str \| None` | `None` | Optional WHERE; AND-combined with the dataset-level filter at the engine boundary. Jinja-templated. Empty / whitespace-only values coerce to `None`. |
| `dimensions` | `list[str] \| None` | `None` | GROUP BY columns; one row emitted per `(metric, dim_value)` combination. Each entry must match the SQL identifier regex. |

**Common pitfalls**:

- Missing `AS <name>` in `expressions` — the old list form
  required it; the dict form (current) does NOT (the key is
  the metric name).
- Setting both dataset-level `filter:` AND collector-level
  `filter:` with overlapping predicates — both apply via
  AND-combine, so you'll filter twice. Pick one place.
- Whitespace-only filter values silently coerce to `None`;
  no error, but easy to miss when debugging "why isn't my
  filter applied."

### `SampleCollectionConfig` field reference

Cross-link: `qualifire/core/config.py:SampleCollectionConfig`.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["sample"]` | `"sample"` | Discriminator. |
| `n_records` | `int` | `10000` | Per-slice sample size (current AND each past slice). |
| `slice_column` | `str \| None` | `None` | Partition column. Validator-constrained to a SQL identifier shape (`[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*`). Required if `slice_value` is set; both go together. |
| `slice_value` | `str \| None` | `None` | Jinja-rendered current-slice value (e.g. `"{{ ds }}"`). |
| `history` | `SampleHistoryConfig \| None` | `None` | Nested history block (below). |

#### `SampleHistoryConfig`

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `past_dates` | `int` | `3` | Number of past slices (not rows per slice). |
| `step` | `str` | required | ISO-8601 cadence between slices (`P7D`, `P1D`). |
| `filters` | `list[str] \| None` | `None` | Escape hatch: explicit per-slice WHERE clauses verbatim. Use when partitions aren't date-shaped (e.g. version IDs). |

**Common pitfalls**:

- `past_dates: 3` does NOT mean 3000 rows — it means 3 past
  slices, each at `n_records` rows.
- Setting `slice_column` without `slice_value` (or vice versa)
  — both go together or neither.
- Pattern / shape validators need consistent samples across
  slices; if the source table's row count varies wildly,
  `n_records` should be smaller than the smallest expected
  per-slice count.

### `ProfilingCollectionConfig` field reference

Cross-link: `qualifire/core/config.py:ProfilingCollectionConfig`.
Deep usage examples (Stats by column type, Global stat
filtering, Per-column overrides, Precedence rules) live in
[`## Profiling Collection`](#profiling-collection) below; this
table is the compact field reference.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["profiling"]` | `"profiling"` | Discriminator. |
| `columns` | `list[str] \| None` | `None` | Subset of columns to profile. `None` = profile every column the backend exposes. |
| `stats` | `list[str] \| None` | `None` | Global include filter. When set, ONLY these stats are computed (and `exclude_stats` is ignored). |
| `exclude_stats` | `list[str] \| None` | `None` | Global exclude filter. Computed when `stats` is unset. |
| `filter` | `str \| None` | `None` | Optional WHERE; AND-combined with the dataset-level filter at the engine boundary. Empty / whitespace-only values coerce to `None`. |
| `top_k` | `int` | `10` | Top-K for string columns. Set to `0` to disable top-K entirely (skips the per-string `groupBy` pass). |
| `quantiles` | `list[float] \| None` | `None` | Quantile fractions in `[0, 1]`. `None` defaults to `[0.25, 0.50, 0.75]`. |
| `column_profiles` | `dict[str, ColumnProfileOverride] \| None` | `None` | Per-column overrides; see sub-table below. |
| `dimensions` | `list[str] \| None` | `None` | Accepted at config-load (validated as identifiers) but currently NOT consumed by `ProfilingCollector` — the engine does not pass dimensions to the profiler (engine.py:1266-1282). Setting this field is a silent no-op today. |

#### `ColumnProfileOverride`

Cross-link: `qualifire/core/config.py:ColumnProfileOverride`.

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `stats` | `list[str] \| None` | `None` | Per-column include list. Overrides the global `stats` AND `exclude_stats` for the matched column. |
| `exclude_stats` | `list[str] \| None` | `None` | Per-column exclude list. Applies only when the per-column `stats` is unset. |

**Common pitfalls**:

- Setting `stats` and `exclude_stats` at the same level —
  `stats` always wins. The `exclude_stats` is then silently
  ignored at that scope (per-column or global).
- `top_k: 0` disables the entire per-string-column `groupBy`
  pass; that's the right knob if you only want numeric /
  scalar stats. Setting `exclude_stats: ["top_k"]` does the
  same thing more verbosely.
- `quantiles: []` falls back to the default
  `[0.25, 0.50, 0.75]` — same as `quantiles: None` — because
  `NumericAnalyzer` uses `self.quantiles or [...]`
  (`qualifire/collection/profiling/analyzers.py:71`). There is
  no "disable quantiles" knob via `quantiles`; suppress them
  via `exclude_stats: ["quantiles"]` (or `["p25", "p50",
  "p75"]` for the unpacked names) instead.
- Setting `dimensions:` on a profiling collection is silently
  ignored — the engine does not forward the field to
  `ProfilingCollector`. If you need per-segment profiling
  today, run one validation per segment with a `filter:`
  pinning the dim value.

### `MetricsCollectionConfig` field reference

Cross-link: `qualifire/core/config.py:MetricsCollectionConfig`.

The Metrics collector is named-KPI shaped: a `metrics`
dict from operator-chosen names to SQL expressions, just
like `Aggregation.expressions` but oriented around business
KPIs rather than statistical aggregates. Wire format and
runtime semantics overlap heavily with Aggregation —
choose Metrics when the operator-facing list reads as KPIs
(`monthly_revenue`, `daily_active_users`), and Aggregation
when it reads as raw stats (`row_count`, `null_pct`).

```yaml
- type: "threshold"
  collection:
    type: "metrics"
    metrics:
      monthly_revenue: "SUM(amount)"
      avg_basket_size: "AVG(items_per_order)"
    filter: "status = 'completed'"
    dimensions: ["region"]
  rules:
    - metric: "monthly_revenue"
      thresholds:
        error: { min: 100000 }
```

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["metrics"]` | `"metrics"` | Discriminator. |
| `metrics` | `dict[str, str]` | required | `{metric_name: sql_expression}`. Required, must be a dict (no list form). |
| `filter` | `str \| None` | `None` | Optional WHERE; AND-combined with the dataset-level filter at the engine boundary. Empty / whitespace-only values coerce to `None`. |
| `dimensions` | `list[str] \| None` | `None` | GROUP BY columns; one row emitted per `(metric, dim_value)` combination. Each entry must match the SQL identifier regex. |

**Common pitfalls**:

- Choosing Metrics vs Aggregation arbitrarily — they share
  the same wire shape (dict of name → SQL). The distinction
  is reader-facing (KPI naming) and a `metric_name`
  collision between two validations on the same dataset is
  the operator's responsibility to avoid; the engine does
  not enforce uniqueness across validations.
- Putting non-aggregate SQL in the value (e.g.
  `monthly_revenue: "amount"` instead of `"SUM(amount)"`) —
  legal at config-load, but produces one row per source
  row at execution. Most validators (threshold, drift)
  assume aggregate-shaped output.
- Same dataset-vs-collector filter compounding as
  Aggregation — both predicates AND-combine, so setting
  both with overlapping clauses double-filters.

### `CustomQueryCollectionConfig` field reference

Cross-link: `qualifire/core/config.py:CustomQueryCollectionConfig`.

The custom-query collector wraps an arbitrary SQL string.
When `dimensions` is unset, the collector takes the FIRST
row of the result and maps each non-key column to one
`CollectionResult.metric_name` / `metric_value` pair
(`qualifire/collection/custom_query.py:88-107`). When
`dimensions` is set, the collector iterates every returned
row and emits one `CollectionResult` per (metric_column,
dimension_combo) tuple (`custom_query.py:109-145`); your
`sql` is responsible for the `GROUP BY`. Use this collector
when neither Aggregation nor Metrics fits the shape (e.g.
cross-table joins, window functions, dialect-specific
syntax).

```yaml
- type: "threshold"
  collection:
    type: "custom_query"
    sql: |
      SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT order_id) AS distinct_orders,
        AVG(amount) AS avg_amount
      FROM orders
      WHERE status = 'completed'
        AND event_dt = '{{ ds }}'
  rules:
    - metric: "row_count"
      thresholds:
        error: { min: 1 }
```

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `type` | `Literal["custom_query"]` | `"custom_query"` | Discriminator. |
| `sql` | `str` | required | Arbitrary aggregated SQL. The collector renders Jinja, then expects a single-row result; each non-key column becomes a metric. |
| `dimensions` | `list[str] \| None` | `None` | Names of columns the operator's `sql` already selects (and presumably already `GROUP BY`s). The engine adds NO `GROUP BY` itself for `custom_query`. The collector reads each row, treats matching column values as the `dimension_value`, and routes the remaining columns as `(metric, dim_value)` pairs (`qualifire/collection/custom_query.py:109-145`). Entries must match the SQL identifier regex. |

**Common pitfalls**:

- **`filter:` is silently ignored.** The
  `CustomQueryCollectionConfig` model has no `filter` field;
  Pydantic's default `extra="ignore"` drops the key without
  warning. There is no engine-side wrapper that adds an
  outer `WHERE` for `custom_query` (it would be a silent
  no-op anyway because the SQL already returns aggregated
  rows). Put any predicate INSIDE `sql`'s WHERE.
- Multi-row results WITHOUT `dimensions:` set — the
  collector takes only the first row of the result and
  silently drops the rest. Either flatten the query with
  an outer aggregation, OR set `dimensions:` to the
  GROUP BY column list so the collector emits one
  metric row per dim combo.
- Forgetting `qf_partition_ts` injection — unlike
  Aggregation, the custom-query collector does NOT auto-
  inject `<expr> AS qf_partition_ts`. If you want
  per-row partition anchoring (column-ref `partition_ts`),
  select the column explicitly: `SELECT event_dt AS
  qf_partition_ts, ...`.

### `RecencyCollectionConfig` field reference

Cross-link: `qualifire/core/config.py:RecencyCollectionConfig`.

`RecencyCollectionConfig` is the freshness-clock spec used
INSIDE `SLOValidationConfig.recency` (see [SLO
Check](#slo-check) above). It is NOT a standalone
collector type — `RecencyCollectionConfig` has no `type`
discriminator field and is not a member of the
`CollectionConfig` discriminated union
(`qualifire/core/config.py:530-536`). The only place to
mount it today is the SLO validator's `recency:` block.
This subsection documents the field shape; the operator
example lives in [SLO Check](#slo-check).

| Field | Type | Default | Accepted forms |
|-------|------|---------|----------------|
| `strategy` | `Literal["max_column", "delta_log", "metadata", "custom_sql"]` | required | The freshness-extraction mode. |
| `column` | `str \| None` | `None` | Required when `strategy="max_column"`; the recency value is `MAX(<column>)`. Hard-fails at config-load with `"'column' is required for max_column strategy"` if missing (`_validate_strategy_fields`, config.py:219-225). The validator does NOT cross-reject `column` when paired with another strategy — extra fields load silently. |
| `sql` | `str \| None` | `None` | Required when `strategy="custom_sql"`; the recency value is the result of `<sql>` (must return a single TIMESTAMP). Hard-fails at config-load with `"'sql' is required for custom_sql strategy"` if missing. The validator does NOT cross-reject `sql` when paired with another strategy — extra fields load silently. |

**Common pitfalls**:

- Mismatched `strategy` / required-field pairing — only
  `max_column` requires `column`, only `custom_sql`
  requires `sql`. Setting `column` with `strategy:
  "custom_sql"` (or vice versa) loads silently and the
  extra field is ignored; runtime then surfaces the wrong
  recency value with no obvious failure.
- `max_column` on a DATE-typed column — produces a delta
  measured in days when downstream consumers expect
  seconds. Use a TIMESTAMP column or wrap with
  `CAST(<col> AS TIMESTAMP)` via the `custom_sql`
  strategy.
- `delta_log` and `metadata` strategies depend on the
  backend exposing the relevant metadata. `delta_log`
  requires a Delta-aware backend with transaction-log
  access; `metadata` reads `last_modified` from
  `backend.get_table_metadata`, which Spark and pandas
  backends do not currently expose
  (`qualifire/backends/spark_backend.py`,
  `qualifire/backends/pandas_backend.py`). Spot-check
  with a manual run on your target backend before
  relying on these in production.

## Coverage of this reference

This reference covers the 14 audited config surfaces:
`QualifireConfig`, `DatasetConfig`, `ThresholdValidationConfig`,
`HistoricalValidationConfig` (drift),
`ForecastValidationConfig`, `PatternValidationConfig`,
`AnomalyDetectionValidationConfig`, `SLOValidationConfig`,
`AggregationCollectionConfig`, `SampleCollectionConfig`,
`ProfilingCollectionConfig`, `MetricsCollectionConfig`,
`CustomQueryCollectionConfig`, `RecencyCollectionConfig`,
plus their nested sub-types (rules / compare / model /
recency / history / column-profile blocks). Notification
channels (Email / Slack / Webhook / Console) are in
[`## Notification Channels`](#notification-channels) above.

The class docstrings in `qualifire/core/config.py` are the
primary source for these field tables (audited as the
[`pydantic-docstring-audit`](features/pydantic-docstring-audit/)
feature). Future field additions update both code + this
reference in the same PR per AC.

### Dimension Slicing

Use `dimensions` to GROUP BY one or more columns and produce per-segment results.

```yaml
# Single dimension
collection:
  type: "aggregation"
  expressions:
    total: "SUM(amount)"
  dimensions: ["region"]

# Multiple dimensions
collection:
  type: "aggregation"
  expressions:
    total: "SUM(amount)"
  dimensions: ["region", "category"]
```

The `dimension_value` in the system table is a JSON string with alphabetically sorted keys:
- Single: `{"region": "US"}`
- Multiple: `{"category": "electronics", "region": "US"}`

## Profiling Collection

Type-aware, single-pass profiling. All column stats are batched into **one `df.agg()` call**. Top-K uses a lightweight separate `groupBy` per string column (skipped entirely when disabled).

### Stats by column type

| Type | Stats |
|---|---|
| **Numeric** (int, float, decimal) | count, null_count, null_pct, min, max, mean, stddev, variance, skewness, kurtosis, approx_distinct, p25, p50, p75 |
| **Text** (string) | count, null_count, null_pct, min_length, max_length, avg_length, approx_distinct, empty_count, empty_pct, top_k |
| **Boolean** | count, null_count, null_pct, true_count, false_count, true_pct |
| **Timestamp** (timestamp, date) | count, null_count, null_pct, min, max, approx_distinct |

### Basic usage

```yaml
- type: "threshold"
  collection:
    type: "profiling"
    columns: ["amount", "name"]     # null = all columns
    top_k: 10                        # 0 to disable (default: 10)
    quantiles: [0.25, 0.50, 0.75]   # default percentiles
  rules:
    - metric: "amount.null_pct"
      thresholds:
        error: { max: 5.0 }
    - metric: "name.approx_distinct"
      thresholds:
        warning: { min: 100 }
```

### Global stat filtering

```yaml
collection:
  type: "profiling"
  # Include only these stats (all columns)
  stats: ["count", "null_count", "null_pct", "approx_distinct"]

  # Or exclude specific stats (all columns)
  exclude_stats: ["top_k", "skewness", "kurtosis"]
```

When `stats` (include list) is set, `exclude_stats` is ignored.

### Per-column overrides

Override stats for specific columns. Per-column settings take precedence over global settings.

```yaml
collection:
  type: "profiling"
  exclude_stats: ["top_k"]        # global: disable top_k everywhere

  column_profiles:
    user_id:
      stats: ["count", "null_count", "approx_distinct"]  # only these for user_id
    notes:
      exclude_stats: ["top_k", "avg_length"]              # exclude just these for notes
    # amount: no override — uses global defaults (top_k excluded)
```

### Precedence rules

1. Per-column `stats` (include list) — only those stats computed for that column
2. Per-column `exclude_stats` — those stats skipped for that column
3. Global `stats` (include list) — only those stats globally
4. Global `exclude_stats` — those stats skipped everywhere
5. Default — all stats computed

Excluded stats are filtered at **expression-building time** — they never execute in the `agg()` call.

## Parallelism & Caching

### Dataset Parallelism

Run multiple datasets concurrently using thread pools. Spark operations release the GIL during JVM calls, so threads overlap effectively.

```yaml
dataset_parallelism: 4                              # top-level: 4 datasets at a time (default: 1)

datasets:
  - name: "sales"
    table: "catalog.schema.sales"
    ...
  - name: "inventory"
    table: "catalog.schema.inventory"
    ...
  # Both run concurrently (up to 4 at a time)
```

### Pipeline Parallelism

Run multiple collect+validate pipelines concurrently within a single dataset. Each pipeline's collect and validate steps are sequential (validate depends on collect output), but independent pipelines overlap.

```yaml
datasets:
  - name: "sales"
    table: "catalog.schema.sales"
    validation_parallelism: 3                 # 3 pipelines at a time (default: 1)
    validations:
      - type: "slo"           # ─┐
        ...                   #  ├── all 3 run concurrently
      - type: "threshold"     #  │
        ...                   #  │
      - type: "profiling"     # ─┘
        ...
```

### Dataset Caching

Cache a dataset's DataFrame before running validations. All pipelines (including parallel ones) read from the cached data instead of re-scanning the table.

```yaml
datasets:
  - name: "sales"
    table: "catalog.schema.sales"
    cache: true                             # default: false
    cache_storage_level: "MEMORY_AND_DISK"  # default: MEMORY_AND_DISK
    validation_parallelism: 3
    validations:
      - ...  # all read from cache
```

**Storage levels**: `MEMORY_ONLY`, `MEMORY_AND_DISK` (default), `DISK_ONLY`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `OFF_HEAP`. These map to Spark's `pyspark.StorageLevel`.

Caching happens **before** any pipelines start and unpersists **after** all complete.

For WAP with `cache: true`, the staging data is cached as a temp view instead of a physical table — see [WAP](#wap-write-audit-publish).

## WAP (Write-Audit-Publish)

```yaml
datasets:
  - name: "sales_wap"
    wap:
      sql: "SELECT * FROM raw.sales WHERE date = '{{ ds }}'"  # SELECT only — staging is managed internally
      target_table: "catalog.schema.sales_prod"
      write_options:
        mode: "overwrite"
        partitionBy: ["date"]
    validations:
      - type: "threshold"
        collection:
          type: "aggregation"
          expressions:
            row_count: "COUNT(*)"
        rules:
          - metric: "row_count"
            thresholds:
              error: { min: 100 }
```

### WAP with Caching

When `cache: true` is set on a WAP dataset, the staging data is cached as a Spark temp view instead of a physical table. Validations query the cached view. On publish, data is inserted from the view into the target table; on rollback, the view is dropped and the cache is unpersisted.

```yaml
datasets:
  - name: "sales_wap"
    cache: true
    cache_storage_level: "MEMORY_ONLY"
    wap:
      sql: "SELECT * FROM raw.sales WHERE date = '{{ ds }}'"
      target_table: "catalog.schema.sales_prod"
    validations:
      - ...  # run against cached temp view
```

## Jinja Variables

All string fields (`filter`, `expressions`, `sql`) support Jinja2 templating.

**Built-in**: `today`, `yesterday`, `now`, `run_id`, `ds`, `ds_nodash`

**Filters**: `date_add(n)`, `date_format(fmt)`

**AIDP**: `job.*`, `task.*`, `hub.*`, `workspace.*` (passed via `context`)

```yaml
filter: "date = '{{ today | date_add(-1) }}'"
```
