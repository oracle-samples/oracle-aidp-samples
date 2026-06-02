# Write-Audit-Publish (WAP) Pattern

WAP is a data quality gate that validates data **before** it reaches production tables. If validation fails, the data is rolled back — no bad data is published.

## How It Works

```
┌─────────┐     ┌──────────┐     ┌───────────┐
│  WRITE  │ ──> │  AUDIT   │ ──> │  PUBLISH  │
│ staging │     │ validate │     │  target   │
└─────────┘     └──────────┘     └───────────┘
                     │
                     │ ERROR?
                     ▼
               ┌───────────┐
               │ ROLLBACK  │
               │ drop stg  │
               └───────────┘
```

1. **WRITE**: Data lands in a temporary staging table
2. **AUDIT**: Validations run against the staging table
3. **PUBLISH** (PASS/WARNING): Data moves from staging to target, staging is dropped
4. **ROLLBACK** (ERROR): Staging table is dropped, `QualifireValidationError` is raised

## DataFrame-Driven WAP

Use when you have a Spark/Pandas DataFrame to write:

```python
from qualifire import Qualifire, QualifireValidationError
from qualifire.backends.spark_backend import SparkBackend

qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="catalog.schema.qf_history",
    owner="team", bu="finance",
)

try:
    result = qf.write_audit_publish(
        df=my_dataframe,
        target_table="catalog.schema.sales_prod",
        write_options={
            "mode": "overwrite",
            "partitionBy": ["date"],
        },
        validations=[
            qf.threshold_check(
                aggregations={"row_count": "COUNT(*)", "null_pct": "..."},
                rules=[
                    {"metric": "row_count", "thresholds": {"error": {"min": 100}}},
                    {"metric": "null_pct", "thresholds": {"error": {"max": 10.0}}},
                ],
            ),
        ],
        name="sales_wap",
    )
    print("Published successfully:", result.overall_severity.value)
except QualifireValidationError as e:
    print("Publication blocked:", e)
    # Staging table already dropped
```

### write_options

Supports all Spark DataFrame write options:

| Option | Description |
|---|---|
| `mode` | `"append"` (default) or `"overwrite"` |
| `partitionBy` | List of partition columns |
| Any Spark option | Passed through to `df.write.option(key, value)` |

## SQL-Driven WAP

Provide a SELECT query — Qualifire wraps it as `CREATE TABLE <staging> AS <your_select>` and manages the staging table internally:

```python
result = qf.write_audit_publish(
    target_table="catalog.schema.sales_prod",
    sql="""
        SELECT * FROM raw_catalog.schema.sales
        WHERE date = '{{ ds }}'
    """,
    validations=[
        qf.threshold_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
        ),
    ],
    context={"ds": "2024-06-15"},
)
```

## YAML Config WAP

```yaml
datasets:
  - name: "sales_wap"
    wap:
      sql: "SELECT * FROM raw.sales WHERE date = '{{ ds }}'"
      target_table: "catalog.schema.sales_prod"
      write_options:
        mode: "overwrite"
        partitionBy: ["date"]
    validations:
      - type: "threshold"
        collection:
          type: "aggregation"
          expressions: ["COUNT(*) AS row_count"]
        rules:
          - metric: "row_count"
            thresholds:
              error: { min: 100 }
        notify:
          error: ["email", "slack"]
```

## Staging Table Naming

The staging table is always auto-generated from the `target_table`:

```
catalog.schema._qf_staging_sales_a1b2c3d4
```

The suffix is a random 8-character hex string to avoid collisions.

## Behavior by Severity

| Severity | Action |
|---|---|
| **PASS** | Publish to target, drop staging |
| **WARNING** | Publish to target, drop staging, send notifications |
| **ERROR** | Drop staging (rollback), send notifications, raise `QualifireValidationError` |

## Combining with Other Validators

WAP works with any validation type:

```python
result = qf.write_audit_publish(
    df=my_df,
    target_table="...",
    validations=[
        qf.threshold_check(...),       # row count, null rates
        qf.slo_check(...),             # freshness
        qf.drift_check(...),      # comparison with past
        qf.shape_check(...),         # ML anomaly detection
    ],
)
```

If **any** validation returns ERROR, the entire batch is rolled back.

## Caching

When `cache=True`, the staging data is cached as a Spark temp view instead of a physical staging table. This avoids writing and reading a physical table, and enables fast repeated reads when multiple validations run against the same staging data.

```python
result = qf.write_audit_publish(
    df=my_df,
    target_table="catalog.schema.sales_prod",
    validations=[...],
    cache=True,
    cache_storage_level="MEMORY_ONLY",  # default: MEMORY_AND_DISK
)
```

```yaml
datasets:
  - name: "sales_wap"
    cache: true
    cache_storage_level: "MEMORY_ONLY"
    validation_parallelism: 3                  # combine with parallel validations
    wap:
      sql: "SELECT * FROM raw.sales"
      target_table: "catalog.schema.sales_prod"
    validations:
      - ...  # all read from cached temp view
```

**Lifecycle with caching:**
1. WRITE: Execute SELECT → cache result → register as temp view
2. AUDIT: Validations query the cached temp view (no disk I/O)
3. PUBLISH/ROLLBACK: Insert from view into target → unpersist cache → drop view
