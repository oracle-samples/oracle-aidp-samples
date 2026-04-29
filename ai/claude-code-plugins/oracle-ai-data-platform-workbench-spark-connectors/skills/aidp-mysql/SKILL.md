---
description: Read or write MySQL or OCI MySQL HeatWave from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions MySQL, HeatWave, MySQL Database Service, MDS, or has a MySQL host/port. Auth is host/port + user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-mysql` — MySQL / OCI MySQL HeatWave via AIDP `aidataplatform`

Covers both **on-prem / generic MySQL** (`type=MYSQL`) and **OCI MySQL HeatWave** (`type=MYSQL_HEATWAVE`). The option shape is identical; only the `type` differs. For most workloads pick `MYSQL` and the connector will route correctly. Choose `MYSQL_HEATWAVE` to get HeatWave-aware optimizations (the AIDP connector pushes down compatible ops to the HeatWave accelerator).

## When to use
- Read or write MySQL or OCI HeatWave from an AIDP notebook.
- Mentioned: "MySQL", "HeatWave", "MDS", "MySQL Database Service".

## When NOT to use
- For Postgres → [`aidp-postgresql`](../aidp-postgresql/SKILL.md).
- For SQL Server → [`aidp-sqlserver`](../aidp-sqlserver/SKILL.md).

## Read
```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type=os.environ.get("MYSQL_TYPE", "MYSQL"),  # or "MYSQL_HEATWAVE"
    host=os.environ["MYSQL_HOST"],
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"],
    schema=os.environ["MYSQL_SCHEMA"],   # MySQL schema = database name
    table=os.environ["MYSQL_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Write
```python
opts = aidataplatform_options(
    type="MYSQL",
    host=os.environ["MYSQL_HOST"],
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"],
    schema=os.environ["MYSQL_SCHEMA"],
    table=os.environ["MYSQL_TARGET_TABLE"],
    extra={"write.mode": "CREATE"},
)
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Gotchas
- **`schema` = MySQL database name.** MySQL conflates "schema" and "database"; pass the database name as `schema`.
- **HeatWave routing** — pass `type=MYSQL_HEATWAVE` to enable HeatWave-aware predicate pushdown. The MySQL DB Service still works under `type=MYSQL`; you only get HeatWave acceleration with the explicit type.
- **Default port 3306** for MySQL / HeatWave; OCI HeatWave Lakehouse exposes port 33060 (XAPI) — use `MYSQL_PORT=33060` for that case.
- **Network reachability** — same constraint as PostgreSQL: must be reachable from the AIDP cluster's VCN.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample (MYSQL): [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb)
- Official sample (MYSQL_HEATWAVE): [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
