---
name: aidp-sqlserver
description: Read or write Microsoft SQL Server from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions SQL Server, MSSQL, or has a TDS host/port. Auth is host/port + database + user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-sqlserver` — Microsoft SQL Server via AIDP `aidataplatform`

## When to use
- Read or write a Microsoft SQL Server from an AIDP notebook.
- Mentioned: "SQL Server", "MSSQL", "TDS".

## When NOT to use
- For Postgres → [`aidp-postgresql`](../aidp-postgresql/SKILL.md).
- For MySQL → [`aidp-mysql`](../aidp-mysql/SKILL.md).
- For Azure SQL Database → [`aidp-azuresql`](../aidp-azuresql/SKILL.md).

## Read
```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="SQLSERVER",
    host=os.environ["MSSQL_HOST"],
    port=int(os.environ.get("MSSQL_PORT", "1433")),
    user=os.environ["MSSQL_USER"],
    password=os.environ["MSSQL_PASSWORD"],
    schema=os.environ.get("MSSQL_SCHEMA", "dbo"),
    table=os.environ["MSSQL_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Write
```python
opts = aidataplatform_options(
    type="SQLSERVER",
    host=os.environ["MSSQL_HOST"],
    port=int(os.environ.get("MSSQL_PORT", "1433")),
    database_name=os.environ["MSSQL_DATABASE"],   # required on write
    user=os.environ["MSSQL_USER"],
    password=os.environ["MSSQL_PASSWORD"],
    schema=os.environ.get("MSSQL_SCHEMA", "dbo"),
    table=os.environ["MSSQL_TARGET_TABLE"],
    extra={"write.mode": "CREATE"},
)
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Gotchas
- **`database.name` is required on write but not strictly on read.** If a read fails with "object not found", supply `database_name=` too — some SQL Server installs require it for the connector to disambiguate.
- **Schema vs database**: SQL Server has both. `schema` here is the SQL-Server schema (typically `dbo`); `database.name` is the catalog (e.g. `master`, `AdventureWorks`).
- **TDS port 1433** by default. Azure SQL Database exposes port 1433 over public TLS; AIDP must reach it via its egress route.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb)
