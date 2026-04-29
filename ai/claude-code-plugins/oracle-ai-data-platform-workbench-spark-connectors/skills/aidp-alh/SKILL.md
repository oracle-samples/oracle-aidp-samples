---
description: Connect from an AIDP notebook to Oracle AI Lakehouse (ALH), Autonomous Data Warehouse (ADW), or Autonomous Transaction Processing (ATP) via Spark JDBC. Use when the user mentions ALH, AI Lakehouse, ADW, ATP, Autonomous Database, or wants to query a 26ai-backed Oracle Autonomous DB from Spark. Covers wallet (mTLS), IAM DB-Token (with on-executor refresh for long jobs), and API Key auth paths.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-alh` — Oracle AI Lakehouse / ADW / ATP via Spark JDBC

This skill covers the **entire Oracle Autonomous Database family** — ALH, ADW, ATP — because they are all Oracle 26ai under the hood. Same JDBC driver (`oracle.jdbc.OracleDriver`), same URL pattern (`jdbc:oracle:thin:@tcps://...:1522/...`), same TNS service-name shape (`_high`/`_medium`/`_low`), same wallet flow, same IAM DB-Token flow.

If the user names ATP or ADW specifically, just use this skill — substitute the env-var prefix (`ATP_*` / `ADW_*`) for `ALH_*` and proceed identically.

## When to use
- User wants to read or write an ALH, ADW, or ATP table from an AIDP notebook.
- User mentions: "ALH", "AI Lakehouse", "ADW", "Autonomous Data Warehouse", "ATP", "Autonomous Transaction Processing", "Autonomous Database", "26ai lakehouse", "lakehouse external catalog".
- User has an Autonomous DB wallet, an Autonomous DB connection string, or an externally-cataloged Autonomous DB in AIDP.

## When NOT to use
- For ExaCS / on-prem Oracle → use [`aidp-exacs`](../aidp-exacs/SKILL.md).

## Prerequisites in the AIDP notebook
1. The Oracle JDBC driver (`ojdbc11.jar` or `ojdbc8.jar`) must be on the cluster classpath. AIDP `tpcds` cluster ships it; if not, attach via Cluster → Libraries.
2. Add the helper package to `sys.path` (one-time, paste at the top of your notebook):
   ```python
   import sys
   sys.path.insert(0, "/Workspace/Shared/oracle_ai_data_platform_connectors/scripts")
   ```
   (Adjust the path to wherever you've uploaded this plugin's `scripts/` dir, or pip-install the package once it's published.)
3. Have one of: an ALH wallet ZIP (Option A), DB-token compartment OCID + API key (Option B), or admin OCI config (Option C).

**Background:** ALH is plain Oracle 26ai under the hood. The `oracle.jdbc.OracleDriver`, the URL pattern (`jdbc:oracle:thin:@tcps://...:1522/...`), and the TNS service-name shape (`_high`/`_medium`/`_low`) all transfer 1:1 from ATP. If you've used ATP from AIDP before, this looks identical.

## Auth: pick one

### Option A — Wallet (mTLS, recommended default)

```python
import os
from oracle_ai_data_platform_connectors.auth import write_wallet_to_tmp
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_wallet,
)

# Write wallet to /tmp (NEVER /Workspace — FUSE breaks the JDBC driver's reads)
tns_admin = write_wallet_to_tmp(
    wallet="/path/to/alh-wallet.zip",      # or pass bytes from OCI Vault
    target_dir="/tmp/wallet/alh",
)

url = build_oracle_jdbc_url(
    tns_alias=os.environ["ALH_TNS_SERVICE"],   # e.g. "alh_high"
    tns_admin=tns_admin,
)

opts = spark_jdbc_options_wallet(
    url=url,
    user=os.environ["ALH_USER"],
    password=os.environ["ALH_PASSWORD"],
)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "MY_SCHEMA.MY_TABLE").load()
df.show(5)
```

### Option B — IAM DB-Token (validated against ATP via IMFA IoT, 3/3 successful)

```python
import os
from oracle_ai_data_platform_connectors.auth import generate_db_token
from oracle_ai_data_platform_connectors.auth.dbtoken import refresh_on_executors
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_dbtoken,
)

token_dir = generate_db_token(
    compartment_ocid=os.environ["ALH_COMPARTMENT_OCID"],
    target_dir="/tmp/dbcred_alh",
)

url = build_oracle_jdbc_url(
    tns_alias=os.environ["ALH_TNS_SERVICE"],
    tns_admin=os.environ.get("ALH_WALLET_PATH", "/tmp/wallet/alh"),
)
opts = spark_jdbc_options_dbtoken(url=url, token_dir=token_dir)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "MY_TABLE").load()

# For long-running jobs (>25 min), wrap your partition logic with refresh_on_executors:
refresh = refresh_on_executors(spark, os.environ["ALH_COMPARTMENT_OCID"], "/tmp/dbcred_alh")
result = df.rdd.mapPartitions(lambda part: refresh(part)).toDF(df.schema)
```

### Option C — API Key + inline OCI config (catalog-sync side only)

The catalog-sync metadata harvest from ALH into the AIDP external catalog uses OCI control-plane APIs, not JDBC. For that side use:

```python
from oracle_ai_data_platform_connectors.auth import from_inline_pem
config = from_inline_pem(
    user_ocid=os.environ["OCI_USER_OCID"],
    tenancy_ocid=os.environ["OCI_TENANCY_OCID"],
    fingerprint=os.environ["OCI_FINGERPRINT"],
    private_key_pem=os.environ["OCI_PRIVATE_KEY_PEM"],
    region=os.environ["OCI_REGION"],
)
# pass `config` to oci.<service>Client(config=config) — never write the PEM to disk
```

## Run the query

Standard Spark JDBC. For large reads, partition the read:

```python
df = (
    spark.read.format("jdbc").options(**opts)
        .option("dbtable", "(SELECT id, x, y FROM big_table) t")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "10000000")
        .option("numPartitions", "16")
        .load()
)
```

For writeback (overwrite only — external catalogs don't support DDL):

```python
(df.write.format("jdbc").options(**opts)
   .option("dbtable", "MY_SCHEMA.TARGET_TABLE")
   .mode("overwrite")
   .save())
```

## Gotchas
- **Wallet must live under `/tmp`.** `/Workspace` is FUSE-mounted; the JDBC driver process can't read it reliably (`Errno 107`).
- **`os.chmod` is a no-op on FUSE.** The helper uses `os.open(..., O_WRONLY|O_CREAT, 0o666)` so the JDBC driver process (different UID) can read the wallet/token.
- **`oracle.jdbc.timezoneAsRegion=false`** — set in helper defaults; avoids the well-known TZ region warning.
- **External catalog refresh** uses `CREATE EXTERNAL CATALOG ... OPTIONS('wallet.content'=base64, 'user.name'=..., 'password'=..., 'wallet.password'=...)`. Same DB credentials as Option A.
- **TNS service name** — confirm via the wallet's `tnsnames.ora`. Not always `alh_high`.
- **Instance Principal / Resource Principal** are blocked in AIDP notebooks today (IMDS unreachable, RP tokens not provided). Don't try `InstancePrincipalsSecurityTokenSigner()`.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py](../../scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py)
- Wallet helper: [scripts/oracle_ai_data_platform_connectors/auth/wallet.py](../../scripts/oracle_ai_data_platform_connectors/auth/wallet.py)
- AIDP notebook auth limits: `Claude context/AIDP/AIDP Context/AIDP/aidp-notebook-authentication.md`
