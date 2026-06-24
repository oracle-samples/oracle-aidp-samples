---
description: Read and write Apache Iceberg tables backed by OCI Object Storage from an AIDP notebook. Use when the user mentions Iceberg, Apache Iceberg, time travel, snapshots, schema evolution, partition evolution, or wants ACID transactions on data lake files. Uses the Iceberg Hadoop catalog on `oci://` — auth is implicit via the workspace IAM identity.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-iceberg` — Apache Iceberg on OCI Object Storage

Manage Iceberg tables (ACID, time travel, schema evolution, partition pruning) backed by OCI Object Storage as the warehouse. The Iceberg Hadoop catalog stores all metadata in the same bucket as data — no external metastore.

## When to use
- Iceberg tables on OCI Object Storage.
- Mentioned: "Iceberg", "time travel", "snapshots", "schema evolution".

## When NOT to use
- For raw CSV/Parquet/JSON files in `oci://` (no transactions / time-travel) → [`aidp-object-storage`](../aidp-object-storage/SKILL.md).
- For Iceberg tables on AWS / Azure → adapt this skill's catalog config; the Hadoop catalog is portable but the bucket URI changes.

## One-time catalog registration

```python
OCI_NAMESPACE = "<namespace>"
BUCKET_NAME   = "<bucket>"
WAREHOUSE     = f"oci://{BUCKET_NAME}@{OCI_NAMESPACE}/iceberg-warehouse"
CATALOG_NAME  = "oci_catalog"

spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}",            "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}.type",       "hadoop")
spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}.warehouse",  WAREHOUSE)
```

After this, all SQL referring to `oci_catalog.<db>.<table>` is Iceberg-managed.

## Create database + table

```python
DB    = "demo_db"
TABLE = "employees"
FQN   = f"{CATALOG_NAME}.{DB}.{TABLE}"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DB}")
spark.sql(f"""
    CREATE TABLE {FQN} (
        employee_id   INT,
        employee_name STRING,
        salary        DOUBLE,
        department    STRING,
        hire_date     DATE
    )
    USING iceberg
    PARTITIONED BY (department)
""")
```

## Insert (each call is one ACID transaction = one snapshot)

```python
import pandas as pd
from datetime import date

pdf = pd.DataFrame([
    (101, "John Doe",       75000.0, "Engineering", date(2022, 1, 15)),
    (102, "Jane Smith",     85000.0, "Sales",       date(2021, 3, 20)),
], columns=["employee_id", "employee_name", "salary", "department", "hire_date"])

spark.createDataFrame(pdf).writeTo(FQN).append()
```

## Schema evolution (no rewrite)

```python
spark.sql(f"ALTER TABLE {FQN} ADD COLUMN location STRING")
# Old rows show NULL for the new column; no errors.
```

## Time travel

```python
snaps = spark.sql(f"""
    SELECT snapshot_id, committed_at, operation
    FROM {FQN}.snapshots
    ORDER BY committed_at
""").collect()
first = snaps[0].snapshot_id
spark.sql(f"SELECT * FROM {FQN} VERSION AS OF {first}").show()
```

## Inspect physical files

```python
spark.sql(f"""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM {FQN}.files
""").show(truncate=False)
```

## Gotchas
- **Auth is implicit** — same as [`aidp-object-storage`](../aidp-object-storage/SKILL.md). The workspace IAM identity reads/writes Object Storage. No keys.
- **Hadoop catalog stores metadata IN the bucket.** Snapshots, schema versions, manifests all sit under `<warehouse>/<db>/<table>/metadata/`. There is no Hive metastore, no Glue, no JDBC catalog.
- **`USING iceberg` is required** in CREATE TABLE; otherwise Spark uses the default V1 file source and you lose ACID.
- **Time-travel requires the snapshot ID** — keeping a long retention helps. Iceberg expires snapshots based on table properties (`history.expire.max-snapshot-age-ms`); set this if long-term time travel matters.
- **Partition pruning kicks in** for queries with predicates on the partition column (`department` in the example). Without that predicate Iceberg still reads all files but in parallel.

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Ingest_into_iceberg_hadoop_catalog_oci_native.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Ingest_into_iceberg_hadoop_catalog_oci_native.ipynb)
- Apache Iceberg docs: <https://iceberg.apache.org/docs/latest/spark-getting-started/>
