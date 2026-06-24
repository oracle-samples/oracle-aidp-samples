---
description: Connect from an AIDP notebook to Oracle AI Lakehouse (ALH), Autonomous Data Warehouse (ADW), or Autonomous Transaction Processing (ATP). Prefer the AIDP `aidataplatform` Spark format handler for wallet/password and catalog.id paths (`ORACLE_ALH` / `ORACLE_ATP`), but use raw Spark JDBC for IAM DB-token because the AIDP connector does not support DB-token auth yet.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-alh` — Oracle AI Lakehouse / ADW / ATP via AIDP `aidataplatform`

This skill covers the **entire Oracle Autonomous Database family** using the official AIDP ingestion connectors. Use `ORACLE_ALH` for Oracle AI Lakehouse / ADW-style connections and `ORACLE_ATP` when the user specifically wants ATP.

If the user names ATP or ADW specifically, just use this skill — substitute the env-var prefix (`ATP_*` / `ADW_*`) for `ALH_*` and proceed identically.

## When to use
- User wants to read or write an ALH, ADW, or ATP table from an AIDP notebook.
- User mentions: "ALH", "AI Lakehouse", "ADW", "Autonomous Data Warehouse", "ATP", "Autonomous Transaction Processing", "Autonomous Database", "26ai lakehouse", "lakehouse external catalog".
- User has wallet content/path, a TNS alias, DB username/password, or an existing AIDP external catalog id.

## When NOT to use
- For ExaCS → use [`aidp-exacs`](../aidp-exacs/SKILL.md).
- For non-Autonomous Oracle Database → use [`aidp-oracle-db`](../aidp-oracle-db/SKILL.md).

## Prerequisites in the AIDP notebook
1. AIDP cluster with the built-in `aidataplatform` Spark format handler.
2. One of: base64 wallet content, wallet zip at a Workspace/Volume path, an existing external catalog id, or IAM DB-token requirements for the raw JDBC exception path.
3. Env vars / OCI Vault secrets:
   - `ALH_WALLET_CONTENT` or `ALH_WALLET_PATH`
   - `ALH_TNS`, `ALH_USER`, `ALH_PASSWORD`
   - `ALH_SCHEMA`, `ALH_TABLE`
   - optional `ALH_CATALOG_ID`

## Read (inline connector options)

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type=os.environ.get("ALH_CONNECTOR_TYPE", "ORACLE_ALH"),  # use ORACLE_ATP for ATP
    user=os.environ["ALH_USER"],
    password=os.environ["ALH_PASSWORD"],
    schema=os.environ["ALH_SCHEMA"],
    table=os.environ["ALH_TABLE"],
    extra={
        "wallet.content": os.environ["ALH_WALLET_CONTENT"],
        "tns": os.environ["ALH_TNS"],
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show()
```

## Read using `wallet.path`

```python
opts = aidataplatform_options(
    type="ORACLE_ALH",
    user=os.environ["ALH_USER"],
    password=os.environ["ALH_PASSWORD"],
    schema=os.environ["ALH_SCHEMA"],
    table=os.environ["ALH_TABLE"],
    extra={
        "wallet.path": os.environ["ALH_WALLET_PATH"],  # /Workspace/... or /Volumes/...
        "tns": os.environ["ALH_TNS"],
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
```

## Write (inline connector options)

```python
opts["table"] = os.environ["ALH_TARGET_TABLE"]
opts["write.mode"] = os.environ.get("ALH_WRITE_MODE", "APPEND")  # CREATE | APPEND | OVERWRITE | MERGE
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Use an existing external catalog (`catalog.id`)

```python
catalog_opts = aidataplatform_options(
    type="ORACLE_ALH",
    schema=os.environ["ALH_SCHEMA"],
    table=os.environ["ALH_TABLE"],
    extra={"catalog.id": os.environ["ALH_CATALOG_ID"]},
)
df = spark.read.format(AIDP_FORMAT).options(**catalog_opts).load()

df.write.format(AIDP_FORMAT).options(
    **{
        "catalog.id": os.environ["ALH_CATALOG_ID"],
        "schema": os.environ["ALH_SCHEMA"],
        "table": os.environ["ALH_TARGET_TABLE"],
        "write.mode": "APPEND",
    }
).save()
```

For external-catalog table access, three-part names are also supported:

```python
df = spark.table(f"{os.environ['ALH_CATALOG_ID']}.{os.environ['ALH_SCHEMA']}.{os.environ['ALH_TABLE']}")
df.write.mode("append").insertInto(
    f"{os.environ['ALH_CATALOG_ID']}.{os.environ['ALH_SCHEMA']}.{os.environ['ALH_TARGET_TABLE']}"
)
```

## Pushdown

```python
pushdown_opts = aidataplatform_options(
    type="ORACLE_ALH",
    user=os.environ["ALH_USER"],
    password=os.environ["ALH_PASSWORD"],
    extra={
        "wallet.content": os.environ["ALH_WALLET_CONTENT"],
        "tns": os.environ["ALH_TNS"],
        "pushdown.sql": "SELECT * FROM HR.EMPLOYEES WHERE DEPARTMENT_ID = 10",
    },
)
df = spark.read.format(AIDP_FORMAT).options(**pushdown_opts).load()
```

## IAM DB-token exception (raw Spark JDBC)

AIDP's built-in `aidataplatform` connector does **not** support IAM DB-token auth yet. If the user explicitly needs DB-token, use raw Spark JDBC with the helper package:

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
    tns_alias=os.environ["ALH_TNS"],
    tns_admin=os.environ["ALH_WALLET_DIR"],  # extracted wallet directory under /tmp
)
opts = spark_jdbc_options_dbtoken(url=url, token_dir=token_dir)

df = (
    spark.read.format("jdbc")
    .options(**opts)
    .option("dbtable", f"{os.environ['ALH_SCHEMA']}.{os.environ['ALH_TABLE']}")
    .load()
)
df.show()

# For long-running jobs, refresh the DB token on executors before partition work.
refresh = refresh_on_executors(spark, os.environ["ALH_COMPARTMENT_OCID"], token_dir)
```

## Gotchas
- Prefer `catalog.id` when the connection already exists in AIDP; it keeps host, wallet, and credentials out of generated notebooks.
- For source-side filtering, prefer `pushdown.sql` for precise Oracle SQL semantics, or DataFrame `.select(...).filter(...).limit(...)` when automatic pushdown is enough.
- Use `wallet.content` or `wallet.path` with the built-in connector; do not materialize wallets into `/tmp` unless you are intentionally falling back to raw Spark JDBC.
- IAM DB-token is the main intentional raw JDBC fallback because `aidataplatform` does not support it yet.
- `ORACLE_ALH` and `ORACLE_ATP` are separate connector type literals. Use `ORACLE_ATP` when adapting the ATP sample exactly.
- Instance Principal / Resource Principal are blocked in AIDP notebooks today (IMDS unreachable, RP tokens not provided). Do not try `InstancePrincipalsSecurityTokenSigner()`.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official ALH sample: [data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Autonomous_AI_Lakehouse.ipynb](../../../../../data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Autonomous_AI_Lakehouse.ipynb)
- Official ATP sample: [data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Autonomous_Transaction_Processing.ipynb](../../../../../data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors/Autonomous_Transaction_Processing.ipynb)
- AIDP notebook auth limits: `Claude context/AIDP/AIDP Context/AIDP/aidp-notebook-authentication.md`
