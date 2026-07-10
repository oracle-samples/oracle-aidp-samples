---
name: aidp-snowflake
description: Read Snowflake from an AIDP notebook through the AIDP `aidataplatform` Spark format handler. Use when the user mentions Snowflake, Snowflake warehouse, Basic authentication, KeyPair authentication, private.key.file, or private.key.content. Snowflake writes are not supported in AIDP 4.0.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-snowflake` — read Snowflake via AIDP `aidataplatform`

Use the built-in AIDP Snowflake connector (`type=SNOWFLAKE`). It supports Basic and KeyPair authentication, direct ingestion reads, external-catalog reads, and SQL pushdown. **Do not generate write code for Snowflake in the 4.0 release.**

## When to use

- Read from a Snowflake warehouse in an AIDP notebook.
- Mentioned: "Snowflake", "warehouse", "private.key.file", or "KeyPair".

## When NOT to use

- For a generic JDBC-only source that has no dedicated connector → [`aidp-jdbc-custom`](../aidp-jdbc-custom/SKILL.md).
- For a Snowflake write request, explain that the 4.0 AIDP Snowflake connector is read-only.

## Basic authentication read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="SNOWFLAKE",
    host=os.environ["SNOWFLAKE_HOST"],
    port=443,
    database_name=os.environ["SNOWFLAKE_DATABASE"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
    table=os.environ["SNOWFLAKE_TABLE"],
    extra={
        "authentication.method": "Basic",
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE", ""),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Key-pair authentication read

Set one of `private.key.file` or `private.key.content`. Use `private.key.pass.phrase` only for an encrypted key.

```python
opts = aidataplatform_options(
    type="SNOWFLAKE",
    host=os.environ["SNOWFLAKE_HOST"],
    port=443,
    database_name=os.environ["SNOWFLAKE_DATABASE"],
    user=os.environ["SNOWFLAKE_USER"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
    table=os.environ["SNOWFLAKE_TABLE"],
    extra={
        "authentication.method": "KeyPair",
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE", ""),
        "private.key.file": os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        "private.key.pass.phrase": os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", ""),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

To supply the key inline, replace `private.key.file` with `private.key.content`; never hard-code a private key in a notebook.

## External-catalog read

Create the Snowflake external catalog in **Master Catalogs** first. You can then use a three-part table name or reuse its connection with `catalog.id`.

```python
catalog_df = spark.table("<CATALOG_NAME>.<SCHEMA>.<TABLE_NAME>")
catalog_df.show(5)

catalog_id_df = (spark.read.format(AIDP_FORMAT)
    .option("catalog.id", "<CATALOG_ID>")
    .option("schema", "<SCHEMA>")
    .option("table", "<TABLE_NAME>")
    .load())
catalog_id_df.show(5)
```

## Pushdown SQL

Use `pushdown.sql` to execute a source query in Snowflake.

```python
pushdown_df = (spark.read.format(AIDP_FORMAT)
    .options(**opts)
    .option("pushdown.sql", "SELECT * FROM <SCHEMA>.<TABLE_NAME> LIMIT 10")
    .load())
pushdown_df.show(5)
```

## Gotchas

- **Read-only in 4.0.** Do not use `df.write`, `saveAsTable`, `insertInto`, or `write.mode` with Snowflake.
- **Authentication values are case-sensitive.** Use `Basic` or `KeyPair`.
- **Warehouse is required.** Set `warehouse` for both authentication modes; `role` is optional.
- **Network reachability.** The AIDP cluster needs outbound HTTPS access to the Snowflake account endpoint.

## References

- Official sample: [Snowflake notebook](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors/Snowflake.ipynb)
