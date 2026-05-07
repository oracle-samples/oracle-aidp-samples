---
description: Read or write an Oracle Database (Compute / Base DB / on-prem / Oracle 19c, 21c, 23ai, 26ai non-Autonomous) from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions Oracle Database, generic Oracle DB, on-prem Oracle, plain Oracle JDBC, port 1521, non-Autonomous Oracle. Read-write. Auth is host/port + database name + user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-oracle-db` — Generic Oracle Database via AIDP `aidataplatform`

For non-Autonomous Oracle DBs — Oracle on Compute, Base DB, on-prem, customer-managed Oracle 19c/21c/23ai/26ai. Auth is plain user/password over TCP 1521.

## When to use
- User wants to read or write a non-Autonomous Oracle Database from an AIDP notebook.
- User mentions: "Oracle Database", "generic Oracle DB", "on-prem Oracle", "Oracle 19c", "Oracle 21c", "Oracle 23ai non-Autonomous", "Base DB", "Oracle on Compute".
- User has a host/port + plain user/password (no wallet, no IAM DB-Token).

## When NOT to use
- For Autonomous DB family (ALH/ADW/ATP) → [`aidp-alh`](../aidp-alh/SKILL.md). Autonomous always uses TCPS + wallet (or IAM DB-Token) — `aidp-oracle-db` won't work.
- For Exadata Cloud Service → [`aidp-exacs`](../aidp-exacs/SKILL.md). ExaCS has its own NNE pattern.
- For PeopleSoft / Siebel — those run on Oracle DB but have their own dedicated skills with the right schema defaults.

## Prerequisites in the AIDP notebook
1. Helpers on `sys.path` (run `aidp-connectors-bootstrap` first).
2. Network: cluster must reach the Oracle DB host on the listener port (typically 1521). For private DBs in customer VCNs, VCN peering is required.
3. Env vars / OCI Vault secrets:
   - `ORADB_HOST`, `ORADB_PORT` (typically `1521`)
   - `ORADB_DATABASE_NAME` (Oracle SID or service name)
   - `ORADB_USER`, `ORADB_PASSWORD`
   - `ORADB_SCHEMA`, `ORADB_TABLE`

## Read (inline options)

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="ORACLE_DB",
    host=os.environ["ORADB_HOST"],
    port=int(os.environ.get("ORADB_PORT", "1521")),
    database_name=os.environ["ORADB_DATABASE_NAME"],
    user=os.environ["ORADB_USER"],
    password=os.environ["ORADB_PASSWORD"],
    schema=os.environ["ORADB_SCHEMA"],
    table=os.environ["ORADB_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Write (inline options)

```python
opts = aidataplatform_options(
    type="ORACLE_DB",
    host=os.environ["ORADB_HOST"],
    port=int(os.environ.get("ORADB_PORT", "1521")),
    database_name=os.environ["ORADB_DATABASE_NAME"],
    user=os.environ["ORADB_USER"],
    password=os.environ["ORADB_PASSWORD"],
    schema=os.environ["ORADB_SCHEMA"],
    table=os.environ["ORADB_TARGET_TABLE"],
    extra={"write.mode": "APPEND"},   # CREATE | APPEND | OVERWRITE
)
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Read via existing external catalog (`catalog.id`)

If your AIDP workspace already has the Oracle DB registered as an external catalog, drop the host/port/credentials entirely and reference the catalog by id:

```python
opts = aidataplatform_options(
    type="ORACLE_DB",
    schema=os.environ["ORADB_SCHEMA"],
    table=os.environ["ORADB_TABLE"],
    extra={"catalog.id": os.environ["ORADB_CATALOG_ID"]},
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

Or use three-part naming via `spark.table()`:

```python
df = spark.table(f"{os.environ['ORADB_CATALOG_ID']}.{os.environ['ORADB_SCHEMA']}.{os.environ['ORADB_TABLE']}")
df.show(10)
df.write.mode("overwrite").saveAsTable(
    f"{os.environ['ORADB_CATALOG_ID']}.{os.environ['ORADB_SCHEMA']}.{os.environ['ORADB_TARGET_TABLE']}"
)
```

## Pushdown SQL

```python
opts = aidataplatform_options(
    type="ORACLE_DB",
    host=os.environ["ORADB_HOST"],
    port=int(os.environ.get("ORADB_PORT", "1521")),
    database_name=os.environ["ORADB_DATABASE_NAME"],
    user=os.environ["ORADB_USER"],
    password=os.environ["ORADB_PASSWORD"],
    extra={
        "pushdown.sql": (
            "SELECT department_id, COUNT(*) AS headcount, SUM(salary) AS total "
            "FROM HR.EMPLOYEES "
            "WHERE hire_date >= DATE '2024-01-01' "
            "GROUP BY department_id"
        ),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show()
```

## Gotchas
- **`database.name` is the Oracle SID or service name**, not the schema. Easy mix-up.
- **Plain user/password only.** Wallets and IAM DB-Tokens are Autonomous-DB-only — for those, use `aidp-alh`.
- **Network reachability** is the most common failure. From the cluster: `socket.create_connection((host, 1521), timeout=8)`. Failure = network problem (NSG / route table / DNS), not auth.
- **Write modes** — `CREATE` (fail if exists), `APPEND`, `OVERWRITE`. Use `MERGE` only via `pushdown.sql` (`MERGE INTO ... WHEN MATCHED ...`) or three-part `saveAsTable` with `write.merge.keys`.
- **NLS settings.** Default Oracle dates can come back with timezone surprises. Set `extra={"oracle.jdbc.timezoneAsRegion": "false"}` if you see TZ drift.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Oracle_Database.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Oracle_Database.ipynb)
