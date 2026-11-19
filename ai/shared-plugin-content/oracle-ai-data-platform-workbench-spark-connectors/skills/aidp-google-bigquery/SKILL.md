---
name: aidp-google-bigquery
description: Read Google BigQuery tables and views from AIDP notebooks using the built-in GOOGLE_BIGQUERY connector, service-account authentication, SQL pushdown, catalog.id, or external-catalog names. Use for BigQuery or GBQ requests in AIDP 5.1 and later.
allowed-tools: Read, Write, Edit, Bash
---

# Google BigQuery via AIDP

Use the built-in `aidataplatform` format with `type=GOOGLE_BIGQUERY` and the Progress DataDirect driver supplied by AIDP. Ingestion and external-catalog access are read-only; do not generate writes, MERGE, or write-mode examples for GBQ.

## Service-account connection and direct read

The user needs a Google service account allowed to query the selected project and read its datasets/tables, plus compute network access to Google BigQuery and OAuth endpoints. Store the full service-account JSON in an AIDP secret. Use the service account's matching email.

If the plugin helper package is already available in the notebook, compose the shared option builder as below. Otherwise generate the equivalent Spark `.option(...)` calls directly, or use `aidp-connectors-bootstrap` when the user wants helper setup. Built-in connector reads do not require a helper upload.

```python
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

key_json = aidputils.secrets.get("<SECRET_NAME>", "<SECRET_KEY>")
opts = aidataplatform_options(
    type="GOOGLE_BIGQUERY",
    host="https://www.googleapis.com/bigquery/v2",
    port=443,
    schema="<DATASET>",
    table="<TABLE_OR_VIEW>",
    extra={
        "project.id": "<GCP_PROJECT_ID>",
        "oauth.type": "service_account",
        "oauth.service.acct.email": "<SERVICE_ACCOUNT_EMAIL>",
        "oauth.pvt.key": key_json,
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).option("row.limit", "10").load()
df.show()
```

`schema` is a BigQuery dataset. Use `project.id` for the GCP project. `port` defaults to `443` when omitted.

## Pushdown and parallel reads

For a complete BigQuery SQL query, use fully qualified identifiers enclosed in BigQuery backticks:

```python
query_df = (
    spark.read.format(AIDP_FORMAT)
    .options(**opts)
    .option("pushdown.sql", "SELECT * FROM `<GCP_PROJECT_ID>.<DATASET>.<TABLE_OR_VIEW>` LIMIT 10")
    .load()
)
query_df.show()
```

For table reads, `pushdown.columns` selects comma-separated columns and `pushdown.filter` supplies a source SQL predicate. `row.limit` bounds the result size. Numeric range partitioning uses `partition.column`, `partition.num`, `partition.lower`, and `partition.upper`; choose a numeric column and bounds from the source data. The public notebook includes a complete partitioned-read example.

## External-catalog access

Use an existing Google BigQuery external catalog created in **Master Catalogs** with service-account authentication. Reuse its stored connection with the catalog identifier; do not add inline credentials:

```python
catalog_df = (
    spark.read.format("aidataplatform")
    .option("catalog.id", "<CATALOG_ID>")
    .option("schema", "<DATASET>")
    .option("table", "<TABLE_OR_VIEW>")
    .option("row.limit", "10")
    .load()
)
catalog_df.show()
```

Three-part names use the catalog **name**, dataset, and table/view:

```python
external_df = spark.table("`<CATALOG_NAME>`.`<DATASET>`.`<TABLE_OR_VIEW>`")
external_df.limit(10).show()
spark.sql("SELECT * FROM `<CATALOG_NAME>`.`<DATASET>`.`<TABLE_OR_VIEW>` LIMIT 10").show()
```

## Authentication constraints

- Use the typed options `oauth.type=service_account`, `oauth.service.acct.email`, and `oauth.pvt.key`. Pass the full JSON as `oauth.pvt.key`, not only a PEM key or a key-file path.
- Keep credentials in secrets; do not print the JSON or the connection-options dictionary.
- Do not substitute username/password, numeric OAuth values, raw driver property aliases, or `connection.properties`-prefixed OAuth keys.
- Use the built-in connector rather than installing another BigQuery JDBC driver or routing to the generic JDBC skill.

## Reference

- [Google BigQuery notebook and connector option reference](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors/Google_BigQuery.ipynb)
