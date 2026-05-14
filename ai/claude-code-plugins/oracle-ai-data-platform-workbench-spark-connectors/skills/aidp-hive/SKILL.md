---
description: Read or write Apache Hive from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions Hive, HiveServer2, HS2, HCatalog, or has a Hive metastore host/port. Auth is host/port + user/password. Read-write.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-hive` — Apache Hive via AIDP `aidataplatform`

## When to use
- User wants to read or write a Hive table from an AIDP notebook.
- User mentions: "Hive", "HiveServer2", "HS2", "HCatalog", "Hive metastore", a Hive database/table name.

## When NOT to use
- For Oracle Big Data Service (BDS) HiveServer2 with **Kerberos** auth via Spark JDBC → use a custom skill (we removed `aidp-bds-hive` from v0.4 scope; revisit if your customer needs Kerberos specifically). The current `aidp-hive` skill covers non-Kerberos Hive (LDAP / SASL-PLAIN / NoAuth) via the `aidataplatform` format handler.
- For Iceberg-on-Hive-style metadata where data lives on `oci://` → [`aidp-iceberg`](../aidp-iceberg/SKILL.md).

## Prerequisites in the AIDP notebook
1. Helpers on `sys.path` (run `aidp-connectors-bootstrap` first).
2. Network: cluster must reach the HS2 host on the configured port (typically 10000). If your Hive lives in a customer VCN, ensure VCN peering is in place — the cluster pod CIDR has no implicit route to user VCNs.
3. Env vars / OCI Vault secrets:
   - `HIVE_HOST` (HiveServer2 hostname)
   - `HIVE_PORT` (typically `10000`)
   - `HIVE_USER`, `HIVE_PASSWORD`
   - `HIVE_SCHEMA` (Hive database name)
   - `HIVE_TABLE` (Hive table name)

## Read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="HIVE",
    host=os.environ["HIVE_HOST"],
    port=int(os.environ.get("HIVE_PORT", "10000")),
    user=os.environ["HIVE_USER"],
    password=os.environ["HIVE_PASSWORD"],
    schema=os.environ["HIVE_SCHEMA"],
    table=os.environ["HIVE_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Write

```python
opts = aidataplatform_options(
    type="HIVE",
    host=os.environ["HIVE_HOST"],
    port=int(os.environ.get("HIVE_PORT", "10000")),
    user=os.environ["HIVE_USER"],
    password=os.environ["HIVE_PASSWORD"],
    schema=os.environ["HIVE_SCHEMA"],
    table=os.environ["HIVE_TARGET_TABLE"],
    extra={"write.mode": "APPEND"},   # CREATE | APPEND | OVERWRITE
)
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Pushdown SQL

```python
opts = aidataplatform_options(
    type="HIVE",
    host=os.environ["HIVE_HOST"],
    port=int(os.environ.get("HIVE_PORT", "10000")),
    user=os.environ["HIVE_USER"],
    password=os.environ["HIVE_PASSWORD"],
    extra={
        "pushdown.sql": (
            "SELECT customer_id, SUM(amount) AS total "
            "FROM sales_db.transactions "
            "WHERE event_dt >= '2025-01-01' "
            "GROUP BY customer_id"
        ),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Gotchas
- **No `database.name` option** — Hive uses `schema` directly to identify the Hive database (e.g. `default`, `sales_db`). Don't pass `database_name`.
- **Network reachability is the most common failure.** Smoke-test from the cluster: `socket.create_connection((host, port), timeout=8)`. If this fails, it's a network problem, not an auth problem — VCN peering / NSG / DNS, in that order.
- **No Kerberos on this skill.** This connector handler uses LDAP / SASL-PLAIN / NoAuth. If your Hive cluster only accepts Kerberos, you need a Spark-native JDBC path with a keytab (out of scope for v0.5 — file an issue).
- **Write modes** — `CREATE` (fail if exists), `APPEND`, `OVERWRITE`. Default is `CREATE`.
- **Partitioned tables.** When writing to a partitioned Hive table, `OVERWRITE` overwrites all partitions touched by the DataFrame's distinct partition keys; existing partitions not touched are preserved.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors/Hive.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors/Hive.ipynb)
