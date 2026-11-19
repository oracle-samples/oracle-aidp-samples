---
name: aidp-azuresql
description: Read or write Azure SQL Database from an AIDP notebook through the AIDP `aidataplatform` Spark format handler. Use when the user mentions Azure SQL, Azure SQL Database, AZURE_SQLSERVER, or a database.windows.net endpoint. Auth is SQL username/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-azuresql` — Azure SQL Database via AIDP `aidataplatform`

Use the dedicated Azure SQL connector type, `AZURE_SQLSERVER`. It supports ingestion reads and writes, external-catalog access, `catalog.id`, and SQL pushdown.

## When to use

- Read or write Azure SQL Database from an AIDP notebook.
- Mentioned: "Azure SQL", "Azure SQL Database", or `database.windows.net`.

## When NOT to use

- For self-managed Microsoft SQL Server → [`aidp-sqlserver`](../aidp-sqlserver/SKILL.md).
- For Azure Data Lake Storage → [`aidp-azure-adls`](../aidp-azure-adls/SKILL.md).

## Ingestion read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="AZURE_SQLSERVER",
    host=os.environ["AZURE_SQL_HOST"],
    port=int(os.environ.get("AZURE_SQL_PORT", "1433")),
    database_name=os.environ["AZURE_SQL_DATABASE"],
    user=os.environ["AZURE_SQL_USER"],
    password=os.environ["AZURE_SQL_PASSWORD"],
    schema=os.environ.get("AZURE_SQL_SCHEMA", "dbo"),
    table=os.environ["AZURE_SQL_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Ingestion write

`CREATE`, `APPEND`, `OVERWRITE`, and `MERGE` are supported. `write.merge.keys` is required for `MERGE`.

```python
write_opts = aidataplatform_options(
    type="AZURE_SQLSERVER",
    host=os.environ["AZURE_SQL_HOST"],
    port=int(os.environ.get("AZURE_SQL_PORT", "1433")),
    database_name=os.environ["AZURE_SQL_DATABASE"],
    user=os.environ["AZURE_SQL_USER"],
    password=os.environ["AZURE_SQL_PASSWORD"],
    schema=os.environ.get("AZURE_SQL_SCHEMA", "dbo"),
    table=os.environ["AZURE_SQL_TARGET_TABLE"],
    extra={"write.mode": "CREATE"},
)
df.write.format(AIDP_FORMAT).options(**write_opts).save()
```

## External catalog and `catalog.id`

Create an Azure SQL external catalog in **Master Catalogs** first. Use a three-part name for catalog reads and writes, or `catalog.id` to reuse its saved connection.

```python
catalog_df = spark.table("<CATALOG_NAME>.<SCHEMA>.<TABLE_NAME>")
catalog_df.show(5)

catalog_id_df = (spark.read.format(AIDP_FORMAT)
    .option("catalog.id", "<CATALOG_ID>")
    .option("schema", "<SCHEMA>")
    .option("table", "<TABLE_NAME>")
    .load())

catalog_id_df.write.format(AIDP_FORMAT) \
    .option("catalog.id", "<CATALOG_ID>") \
    .option("schema", "<SCHEMA>") \
    .option("table", "<TARGET_TABLE_NAME>") \
    .option("write.mode", "APPEND") \
    .save()
```

## Pushdown SQL

```python
pushdown_df = (spark.read.format(AIDP_FORMAT)
    .options(**opts)
    .option("pushdown.sql", "SELECT TOP 10 * FROM <SCHEMA>.<TABLE_NAME>")
    .load())
pushdown_df.show(5)
```

## Gotchas

- Use `AZURE_SQLSERVER`, not `SQLSERVER`, for Azure SQL Database.
- Azure SQL normally uses port `1433`; pass the fully-qualified `*.database.windows.net` host.
- `schema` is usually `dbo`; `database.name` is the Azure SQL database.
- AIDP needs egress to the Azure SQL endpoint. Configure Azure firewall rules for the AIDP network path.

## References

- Official sample: [AzureSQL notebook](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors/AzureSQL.ipynb)
