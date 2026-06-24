---
description: Read or write Oracle Exadata Cloud Service (ExaCS) from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions ExaCS, Exadata, Exadata Cloud, RAC SCAN listener, or has a private-subnet Oracle DB. Prefer the official `ORACLE_EXADATA` connector sample with catalog.id, pushdown.sql, and connector write modes.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-exacs` — Oracle Exadata Cloud Service via AIDP `aidataplatform`

## When to use
- User wants to read or write an ExaCS PDB from an AIDP notebook.
- User mentions: "ExaCS", "Exadata Cloud", "RAC SCAN listener", "private-subnet Oracle DB".

## When NOT to use
- For Oracle AI Lakehouse / ADW / ATP (Autonomous DB family) → use [`aidp-alh`](../aidp-alh/SKILL.md).

## Critical infrastructure prerequisites

ExaCS sits in a customer private subnet; AIDP runs in its own VCN. Two pieces must be in place before any JDBC will work — neither is configurable from a notebook:

1. **VCN routing AIDP→ExaCS** (PE/RCE through PAGW). Workspaces created without private connectivity can't reach customer DBs.
2. **Workspace `scanDetails`** — required for RAC clusters. Without it, the SCAN listener's redirect to a node-local IP (`10.x.x.x`) is unreachable from the executor → `ORA-17820`. Configure once via the AIDP workspace REST API or console:

   ```json
   networkConfigurationDetails: {
     "subnetId": "ocid1.subnet.oc1.iad.aaaa...",
     "scanDetails": [
       {"fqdn": "<scan-host>.clientsubnet.dns.oraclevcn.com", "port": "1521"}
     ]
   }
   ```

   This activates **PE-ARCH 3c (RCE with SCAN Proxy)** — the PAGW translates RAC node-redirect IPs to Class-E NAT addresses so the JDBC redirect lands on the right tunnel.

Smoke-test connectivity from a notebook before chasing JDBC errors:

```python
import socket, ipaddress
ip = socket.gethostbyname(SCAN_HOST)            # should be Class-E (255.x) or RFC-1918 — never public
with socket.create_connection((SCAN_HOST, 1521), timeout=15): pass
```

## Read (inline connector options)

Use the official AIDP Exadata connector first:

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="ORACLE_EXADATA",
    host=os.environ["EXACS_HOST"],
    port=int(os.environ.get("EXACS_PORT", "1521")),
    database_name=os.environ["EXACS_DATABASE_NAME"],
    user=os.environ["EXACS_USER"],
    password=os.environ["EXACS_PASSWORD"],
    schema=os.environ["EXACS_SCHEMA"],
    table=os.environ["EXACS_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show()
```

## Write

```python
opts["table"] = os.environ["EXACS_TARGET_TABLE"]
opts["write.mode"] = os.environ.get("EXACS_WRITE_MODE", "APPEND")  # CREATE | APPEND | OVERWRITE | MERGE
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Use `catalog.id`

```python
catalog_opts = aidataplatform_options(
    type="ORACLE_EXADATA",
    schema=os.environ["EXACS_SCHEMA"],
    table=os.environ["EXACS_TABLE"],
    extra={"catalog.id": os.environ["EXACS_CATALOG_ID"]},
)
df = spark.read.format(AIDP_FORMAT).options(**catalog_opts).load()
```

## Pushdown

```python
pushdown_opts = aidataplatform_options(
    type="ORACLE_EXADATA",
    host=os.environ["EXACS_HOST"],
    port=int(os.environ.get("EXACS_PORT", "1521")),
    database_name=os.environ["EXACS_DATABASE_NAME"],
    user=os.environ["EXACS_USER"],
    password=os.environ["EXACS_PASSWORD"],
    extra={
        "pushdown.sql": "SELECT * FROM HR.EMPLOYEES WHERE DEPARTMENT_ID = 10",
    },
)
df = spark.read.format(AIDP_FORMAT).options(**pushdown_opts).load()
```

## Gotchas

- **`scanDetails` is the #1 cause of ORA-17820.** If the JDBC URL points at the SCAN listener on a RAC cluster but the workspace doesn't have the SCAN FQDN registered, the redirect fails silently. See "Critical infrastructure prerequisites" above.
- **Port 1521 is often encrypted by server-side NNE.** The `ORACLE_EXADATA` connector is the preferred path; fall back to raw JDBC only for diagnostics that the built-in connector cannot express.
- **No IMDS access from AIDP notebooks** — Instance Principal / Resource Principal flows that work elsewhere on OCI compute fail here. Use API Key + inline PEM if you need OCI SDK calls.
- If you need wallet+TCPS or IAM DB-Token to an Autonomous DB, use the [`aidp-alh`](../aidp-alh/SKILL.md) skill instead.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official Exadata sample: [data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Oracle_Exadata.ipynb](../../../../../data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Oracle_Exadata.ipynb)
- AIDP private endpoint design (PE-ARCH 1a–3c, SCAN Proxy): see workspace memory `oci_private_endpoint_design.md`.
