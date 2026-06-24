---
description: Read from Oracle Siebel CRM into a Spark DataFrame in an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions Siebel, Siebel CRM, S_CONTACT, S_ORG_EXT, or has a Siebel host/port. Auth is host/port + database name + user/password. Read-only.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-siebel` — Oracle Siebel CRM via AIDP `aidataplatform`

## When to use
- User wants to ingest Siebel CRM data (contacts, accounts, opportunities, service requests) into a Spark DataFrame from an AIDP notebook.
- User mentions: "Siebel", "Siebel CRM", "S_CONTACT", "S_ORG_EXT", "S_OPTY", Siebel base tables.

## When NOT to use
- For Oracle Autonomous DB family (ALH/ADW/ATP) → [`aidp-alh`](../aidp-alh/SKILL.md).
- For Oracle PeopleSoft → [`aidp-peoplesoft`](../aidp-peoplesoft/SKILL.md).
- For generic Oracle DB → [`aidp-oracle-db`](../aidp-oracle-db/SKILL.md).

## Prerequisites in the AIDP notebook
1. Helpers on `sys.path` (run `aidp-connectors-bootstrap` first).
2. Env vars / OCI Vault secrets:
   - `SIEBEL_HOST`, `SIEBEL_PORT` (typically `1521`)
   - `SIEBEL_DATABASE_NAME` (Oracle SID / service)
   - `SIEBEL_USER`, `SIEBEL_PASSWORD`
   - `SIEBEL_SCHEMA` (typically `SIEBEL`)
   - `SIEBEL_TABLE` (a Siebel base table, e.g. `S_CONTACT`, `S_ORG_EXT`)

## Read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="ORACLE_SIEBEL",
    host=os.environ["SIEBEL_HOST"],
    port=int(os.environ["SIEBEL_PORT"]),
    database_name=os.environ["SIEBEL_DATABASE_NAME"],
    user=os.environ["SIEBEL_USER"],
    password=os.environ["SIEBEL_PASSWORD"],
    schema=os.environ.get("SIEBEL_SCHEMA", "SIEBEL"),
    table=os.environ["SIEBEL_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Pushdown SQL

Use `pushdown.sql` to run a complete source query — push joins, filters, and aggregations to the Siebel DB instead of pulling whole base tables into Spark.

```python
opts = aidataplatform_options(
    type="ORACLE_SIEBEL",
    host=os.environ["SIEBEL_HOST"],
    port=int(os.environ["SIEBEL_PORT"]),
    database_name=os.environ["SIEBEL_DATABASE_NAME"],
    user=os.environ["SIEBEL_USER"],
    password=os.environ["SIEBEL_PASSWORD"],
    extra={
        "pushdown.sql": (
            "SELECT C.ROW_ID, C.LAST_NAME, C.FST_NAME, O.NAME AS ACCOUNT "
            "FROM SIEBEL.S_CONTACT C "
            "JOIN SIEBEL.S_ORG_EXT O ON C.PR_HELD_POSTN_ID = O.ROW_ID "
            "WHERE C.STATUS_CD = 'Active'"
        ),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Gotchas
- **Connector is read-only.** Siebel data should be written back through Siebel's EAI/REST channels, not Spark. The connector is intentionally one-way.
- **Underlying Oracle DB.** Siebel runs on Oracle DB; network reachability rules from `aidp-oracle-db` apply.
- **`SIEBEL` schema owner.** Standard Siebel install owns all base tables (`S_*`) under the `SIEBEL` schema. The connector user needs `SELECT` privs.
- **Soft-delete columns.** Siebel uses `ROW_ID` keys and `LAST_UPD` for incremental ingest. Filter with `WHERE LAST_UPD > :since` via `pushdown.sql` for delta loads.
- **Audit columns.** `CREATED`, `CREATED_BY`, `LAST_UPD`, `LAST_UPD_BY` are populated by triggers on every row — useful for change tracking.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Only_Ingestion_Connectors/Oracle_Siebel.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors/Oracle_Siebel.ipynb)
