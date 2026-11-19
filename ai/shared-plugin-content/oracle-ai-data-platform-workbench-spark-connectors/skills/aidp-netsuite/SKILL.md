---
name: aidp-netsuite
description: Read NetSuite SuiteAnalytics Connect data from an AIDP notebook through the AIDP `aidataplatform` Spark format handler. Use when the user mentions NetSuite, SuiteAnalytics Connect, ns.account.id, ns.role.id, or ns.access.token. Supports username/password or OAuth access-token authentication. Read-only.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-netsuite` — NetSuite SuiteAnalytics Connect via AIDP `aidataplatform`

Use the built-in `NETSUITE` connector for read-only SuiteAnalytics Connect ingestion. It supports either username/password authentication or a fresh OAuth 2.0 access token.

## When to use

- Read NetSuite data through SuiteAnalytics Connect from an AIDP notebook.
- Mentioned: "NetSuite", "SuiteAnalytics Connect", `ns.account.id`, `ns.role.id`, or `ns.access.token`.

## When NOT to use

- For NetSuite RESTlets or SuiteTalk REST APIs, use a REST-specific integration rather than this JDBC-style connector.
- For NetSuite writes, explain that the AIDP 4.0 NetSuite connector is read-only.

## Username and password read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="NETSUITE",
    host=os.environ["NETSUITE_HOST"],
    port=int(os.environ["NETSUITE_PORT"]),
    user=os.environ["NETSUITE_USER"],
    password=os.environ["NETSUITE_PASSWORD"],
    schema=os.environ["NETSUITE_SCHEMA"],
    table=os.environ["NETSUITE_TABLE"],
    extra={
        "ns.account.id": os.environ["NETSUITE_ACCOUNT_ID"],
        "ns.role.id": os.environ["NETSUITE_ROLE_ID"],
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## OAuth access-token read

Use `ns.access.token` instead of `user.name` and `password`. Keep the access token in an environment variable or secret manager; never hard-code it in a notebook.

```python
token_opts = aidataplatform_options(
    type="NETSUITE",
    host=os.environ["NETSUITE_HOST"],
    port=int(os.environ["NETSUITE_PORT"]),
    schema=os.environ["NETSUITE_SCHEMA"],
    table=os.environ["NETSUITE_TABLE"],
    extra={
        "ns.account.id": os.environ["NETSUITE_ACCOUNT_ID"],
        "ns.role.id": os.environ["NETSUITE_ROLE_ID"],
        "ns.access.token": os.environ["NETSUITE_ACCESS_TOKEN"],
    },
)
df = spark.read.format(AIDP_FORMAT).options(**token_opts).load()
df.show(10)
```

## Pushdown SQL

```python
pushdown_df = (spark.read.format(AIDP_FORMAT)
    .options(**token_opts)
    .option("pushdown.sql", "SELECT * FROM <SCHEMA>.<TABLE_NAME> WHERE <COLUMN_NAME> = '<VALUE>'")
    .load())
pushdown_df.show(10)
```

## Gotchas

- **Read-only in 4.0.** Do not generate `df.write`, `saveAsTable`, `insertInto`, or `write.mode` examples.
- **Choose one authentication mode.** Use either `user.name`/`password` or `ns.access.token`, not both.
- **Token freshness matters.** Refresh `ns.access.token` before it expires.
- **SuiteAnalytics Connect access is required.** The account and role must be entitled to the schemas and tables requested.

## References

- Official sample: [NetSuite notebook](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors/NetSuite.ipynb)
