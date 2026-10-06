---
name: aidp-db2
description: Read or write IBM Db2 from an AIDP notebook through the AIDP `aidataplatform` Spark format handler. Use when the user mentions DB2, IBM Db2, LUW, or `type=DB2`. Auth is host/port + database name + user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-db2` — IBM Db2 via AIDP `aidataplatform`

Use the built-in AIDP Db2 connector (`type=DB2`) for ingestion reads, writes, and SQL pushdown. External-catalog support is not included in the 4.1 release.

## When to use

- Read from or write to an IBM Db2 database from an AIDP notebook.
- Mentioned: "DB2", "Db2", "IBM Db2", or `type=DB2`.

## When NOT to use

- For a database without a dedicated AIDP connector → [`aidp-jdbc-custom`](../aidp-jdbc-custom/SKILL.md).
- For a Db2 external-catalog request; that capability is not available in 4.1.

## Ingestion read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="DB2",
    host=os.environ["DB2_HOST"],
    port=int(os.environ.get("DB2_PORT", "50000")),
    database_name=os.environ["DB2_DATABASE"],
    user=os.environ["DB2_USER"],
    password=os.environ["DB2_PASSWORD"],
    schema=os.environ["DB2_SCHEMA"],
    table=os.environ["DB2_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Ingestion write

`CREATE`, `APPEND`, `OVERWRITE`, and `MERGE` are supported. `write.merge.keys` is required for `MERGE`.

```python
write_opts = aidataplatform_options(
    type="DB2",
    host=os.environ["DB2_HOST"],
    port=int(os.environ.get("DB2_PORT", "50000")),
    database_name=os.environ["DB2_DATABASE"],
    user=os.environ["DB2_USER"],
    password=os.environ["DB2_PASSWORD"],
    schema=os.environ["DB2_SCHEMA"],
    table=os.environ["DB2_TARGET_TABLE"],
    extra={"write.mode": "CREATE"},
)
df.write.format(AIDP_FORMAT).options(**write_opts).save()
```

## Pushdown SQL

```python
pushdown_df = (spark.read.format(AIDP_FORMAT)
    .options(**opts)
    .option("pushdown.sql", "SELECT * FROM <SCHEMA>.<TABLE_NAME> FETCH FIRST 10 ROWS ONLY")
    .load())
pushdown_df.show(5)
```

## Gotchas

- Use `DB2`, not the generic JDBC connector type.
- `database.name` is required for Db2.
- Db2 external-catalog access is not included in the 4.1 release.

## References

- Official sample: [Db2 notebook](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors/DB2.ipynb)
