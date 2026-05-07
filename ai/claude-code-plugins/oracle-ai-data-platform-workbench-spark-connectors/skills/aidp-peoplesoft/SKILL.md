---
description: Read from Oracle PeopleSoft into a Spark DataFrame in an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions PeopleSoft, PSFT, HCM, FSCM, Campus Solutions, or has a PeopleSoft host/port. Auth is host/port + database name + user/password. Read-only.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-peoplesoft` — Oracle PeopleSoft via AIDP `aidataplatform`

## When to use
- User wants to ingest PeopleSoft data (HCM, FSCM, Campus Solutions, ELM, CRM) into a Spark DataFrame from an AIDP notebook.
- User mentions: "PeopleSoft", "PSFT", "PS HCM", "Campus Solutions".

## When NOT to use
- For Oracle Autonomous DB family (ALH/ADW/ATP) → [`aidp-alh`](../aidp-alh/SKILL.md).
- For generic Oracle DB on Compute / Base DB / on-prem → [`aidp-oracle-db`](../aidp-oracle-db/SKILL.md).
- For Oracle Siebel → [`aidp-siebel`](../aidp-siebel/SKILL.md).

## Prerequisites in the AIDP notebook
1. Helpers on `sys.path` (run `aidp-connectors-bootstrap` first).
2. Env vars / OCI Vault secrets:
   - `PSFT_HOST` (PeopleSoft DB host)
   - `PSFT_PORT` (typically `1521` for the underlying Oracle DB)
   - `PSFT_DATABASE_NAME` (Oracle SID / service)
   - `PSFT_USER`, `PSFT_PASSWORD`
   - `PSFT_SCHEMA` (typically `SYSADM`)
   - `PSFT_TABLE` (PeopleSoft record table, e.g. `PS_JOB`, `PS_VOUCHER`)

## Read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="ORACLE_PEOPLESOFT",
    host=os.environ["PSFT_HOST"],
    port=int(os.environ["PSFT_PORT"]),
    database_name=os.environ["PSFT_DATABASE_NAME"],
    user=os.environ["PSFT_USER"],
    password=os.environ["PSFT_PASSWORD"],
    schema=os.environ.get("PSFT_SCHEMA", "SYSADM"),
    table=os.environ["PSFT_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Pushdown SQL

Push a complete source query at the database — connector skips schema/table option building and runs the SQL verbatim. Useful for joins, filters, derived columns where you don't want Spark to fetch the full table.

```python
opts = aidataplatform_options(
    type="ORACLE_PEOPLESOFT",
    host=os.environ["PSFT_HOST"],
    port=int(os.environ["PSFT_PORT"]),
    database_name=os.environ["PSFT_DATABASE_NAME"],
    user=os.environ["PSFT_USER"],
    password=os.environ["PSFT_PASSWORD"],
    extra={
        "pushdown.sql": (
            "SELECT EMPLID, EFFDT, DEPTID, JOBCODE "
            "FROM SYSADM.PS_JOB "
            "WHERE EFF_STATUS = 'A' AND EFFDT >= DATE '2025-01-01'"
        ),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Gotchas
- **Connector is read-only.** PeopleSoft is treated as a source-of-truth system; writes back must go through PeopleSoft's own application APIs, not Spark.
- **Underlying Oracle DB.** Most PeopleSoft installs run on Oracle DB; the connector hits the DB directly. Network reachability rules from `aidp-oracle-db` apply — cluster pod CIDR needs L3 path to the PeopleSoft DB host.
- **`SYSADM` schema** is the standard PeopleSoft owner. Ensure the connector user has `SELECT` privs on the PS_* tables you need — PeopleSoft tables are not granted to PUBLIC by default.
- **Read row count** before pulling — PeopleSoft fact tables can have hundreds of millions of rows. Always include a `WHERE` filter via `pushdown.sql` for production.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Only_Ingestion_Connectors/Oracle_PeopleSoft.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors/Oracle_PeopleSoft.ipynb)
