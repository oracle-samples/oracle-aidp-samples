---
description: Connect from an AIDP notebook to Oracle Exadata Cloud Service (ExaCS) via Spark JDBC. Use when the user mentions ExaCS, Exadata, Exadata Cloud, RAC SCAN listener, or has a private-subnet Oracle DB. Auth is plain user/password on TCP 1521 with server-enforced AES256 Native Network Encryption (live-validated against Oracle 23ai). Wallet TCPS and IAM DB-Token are not supported by AIDP notebooks for ExaCS.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-exacs` — Oracle Exadata Cloud Service via Spark JDBC

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

## Auth: plain user/password on TCP 1521 + server-enforced NNE (live-validated)

This is the **only supported auth path** for ExaCS from AIDP notebooks. Wallet/TCPS and IAM DB-Token are not workable in the AIDP notebook environment for ExaCS clusters and have been intentionally removed from this skill.

ExaCS deployments commonly enforce **Oracle Native Network Encryption (NNE)** server-side:
`SQLNET.ENCRYPTION_SERVER=REQUIRED` + `SQLNET.ENCRYPTION_TYPES_SERVER=AES256`. The Oracle thin
driver complies automatically — no client-side `oracle.net.encryption_*` options needed. The
encrypted session can be verified after connect via `v$session_connect_info.network_service_banner`
(see snippet below).

```python
import os
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_password,
)

url = build_oracle_jdbc_url(
    host=os.environ["EXACS_HOST"],            # SCAN FQDN or host
    port=int(os.environ.get("EXACS_PORT", "1521")),
    service_name=os.environ["EXACS_SERVICE_NAME"],
    use_tcps=False,                           # plain TCP — encryption is via NNE, not TLS
)
opts = spark_jdbc_options_password(
    url=url,
    user=os.environ["EXACS_USER"],
    password=os.environ["EXACS_PASSWORD"],
)
df = (spark.read.format("jdbc").options(**opts)
        .option("dbtable", os.environ["EXACS_TABLE_FOR_TEST"]).load())
df.show(5)
```

**SYS / SYSDBA logon** (admin scripts only; not for app workloads):

```python
opts["oracle.jdbc.internal_logon"] = "sysdba"
```

**Verify in-transit encryption is active** (run once per environment):

```python
enc_q = ("SELECT network_service_banner FROM v$session_connect_info "
         "WHERE sid = SYS_CONTEXT('USERENV','SID')")
banners = (spark.read.format("jdbc").options(**opts)
             .option("query", enc_q).load().collect())
for r in banners:
    print(r[0])
# Expect lines including:
#   AES256 Encryption service adapter for Linux: Version ...
#   Crypto-checksumming service for Linux: Version ...
```

## Gotchas

- **`scanDetails` is the #1 cause of ORA-17820.** If the JDBC URL points at the SCAN listener on a RAC cluster but the workspace doesn't have the SCAN FQDN registered, the redirect fails silently. See "Critical infrastructure prerequisites" above.
- **Port 1521 is NOT plain unencrypted.** With NNE configured server-side, port-1521 TCP is AES256 encrypted on the wire. Confirm via `v$session_connect_info` after the first connect — don't assume.
- **No IMDS access from AIDP notebooks** — Instance Principal / Resource Principal flows that work elsewhere on OCI compute fail here. Use API Key + inline PEM if you need OCI SDK calls.
- **No wallet/TCPS or IAM DB-Token paths for ExaCS** in this skill. Customer ExaCS deployments do not commonly expose TCPS listeners and AIDP notebooks do not support IAM DB-Token against ExaCS. The AIDP notebook environment connects via Native Network Encryption on plain TCP 1521. If you need wallet+TCPS or IAM DB-Token to an Autonomous DB, use the [`aidp-alh`](../aidp-alh/SKILL.md) skill instead.
- **Driver class name** — `oracle.jdbc.OracleDriver` is canonical. The legacy alias `oracle.jdbc.driver.OracleDriver` also works but is deprecated.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py](../../scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py)
- Live-validated reference notebook (the source of the Option A pattern): [`exacs_intransit_encryption_demo.ipynb`](https://github.com/ahmedawan-oracle/oracle-ai-data-platform-workbench-spark-connectors) on workspace `exacs-private-test` — proves AES256 NNE end-to-end on a customer ExaCS cluster running Oracle 23ai.
- AIDP private endpoint design (PE-ARCH 1a–3c, SCAN Proxy): see workspace memory `oci_private_endpoint_design.md`.
