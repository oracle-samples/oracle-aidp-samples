---
description: Read or write Snowflake from an AIDP notebook via Spark using the Snowflake Spark connector. Use when the user mentions Snowflake, Snowflake warehouse, sfUrl, sfUser, or wants to migrate from Snowflake. Auth is sfUser + sfPassword over the Snowflake Spark connector (`net.snowflake.spark.snowflake`).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-snowflake` — Snowflake via the Snowflake Spark connector

Bridge AIDP Spark to Snowflake using the official Snowflake Spark connector. Useful for migration off Snowflake or for cross-warehouse joins where Snowflake holds the source of truth.

## When to use
- Reading or writing a Snowflake warehouse from AIDP.
- Mentioned: "Snowflake", "sfUrl", "sfWarehouse".

## When NOT to use
- For a generic JDBC-only DB (no Spark connector available) → [`aidp-jdbc-custom`](../aidp-jdbc-custom/SKILL.md).

## Cluster prerequisite — install the connector JARs

The Snowflake Spark connector is **not** in the AIDP cluster image by default. Two ways to get it in.

### Option A — Runtime-load (recommended; no cluster restart) ★ live-validated

The plugin's `add_spark_connector_at_runtime` helper downloads + registers both JARs in the running Spark session. Live-validated on the AIDP `tpcds` cluster (Spark 3.5.0) — see `tests/live-results/row20.json`.

```python
from oracle_ai_data_platform_connectors.jdbc import (
    add_spark_connector_at_runtime, download_jdbc_jar,
)

jars = [
    download_jdbc_jar(
        maven_url="https://repo1.maven.org/maven2/net/snowflake/"
                  "spark-snowflake_2.12/3.1.1/spark-snowflake_2.12-3.1.1.jar",
        target_path="/tmp/spark-snowflake_2.12-3.1.1.jar"),
    download_jdbc_jar(
        maven_url="https://repo1.maven.org/maven2/net/snowflake/"
                  "snowflake-jdbc/3.19.0/snowflake-jdbc-3.19.0.jar",
        target_path="/tmp/snowflake-jdbc-3.19.0.jar"),
]
add_spark_connector_at_runtime(
    spark,
    jar_paths=jars,
    verify_classes=[
        "net.snowflake.spark.snowflake.DefaultSource",
        "net.snowflake.client.jdbc.SnowflakeDriver",
    ],
    register_jdbc_driver_class="net.snowflake.client.jdbc.SnowflakeDriver",
)
```

The helper does three things in one call: builds a `URLClassLoader` covering both JARs and sets it as the thread context CL (so Spark's `ServiceLoader` finds the `snowflake` format), registers the JDBC driver with `DriverManager` (for any code path that goes through `getConnection`), and calls `SparkContext.addJar` on each JAR (so executors fetch them — required because Snowflake reads partition across executors). All without a kernel restart.

### Option B — Cluster Library tab (durable, requires admin)

Upload both JARs to a Volume and attach via the cluster Library tab. Persists across cluster restarts. Requires admin access. After restart, skip the runtime-load helper.

Pin the versions — newer Snowflake connector / JDBC may not be compatible with the cluster's Spark version. The pair tested on Spark 3.5.0 / Scala 2.12 is `spark-snowflake_2.12-3.1.1` + `snowflake-jdbc-3.19.0`.

## Read

```python
import os

snowflake_options = {
    "sfUrl":       os.environ["SNOW_URL"],         # e.g. xy12345.us-east-1.snowflakecomputing.com
    "sfUser":      os.environ["SNOW_USER"],
    "sfPassword":  os.environ["SNOW_PASSWORD"],
    "sfDatabase":  os.environ.get("SNOW_DATABASE", "DATAFLOW"),
    "sfSchema":    os.environ.get("SNOW_SCHEMA",   "DF_SCHEMA"),
    "sfWarehouse": os.environ.get("SNOW_WAREHOUSE", "COMPUTE_WH"),
}

df = (spark.read
      .format("snowflake")
      .options(**snowflake_options)
      .option("dbtable", os.environ["SNOW_TABLE"])
      .load())
df.show(5)
```

## Write

```python
(df.write
   .format("snowflake")
   .options(**snowflake_options)
   .option("dbtable", os.environ["SNOW_TARGET_TABLE"])
   .mode("overwrite")
   .save())
```

## Gotchas
- **No Spark JDBC fallback in this skill.** The Snowflake JDBC alone (no Spark connector) doesn't push down predicates and is much slower. Use the Spark connector.
- **Network reachability** — Snowflake is public over TLS; the AIDP cluster needs egress. If your cluster is in a strict NSG, allow outbound HTTPS to `*.snowflakecomputing.com`.
- **Auth** — only password auth shown here. Snowflake key-pair auth (RSA) and OAuth are also supported by the connector but require additional `pem_private_key` / `sfAuthenticator` options not covered in this skill.
- **`dbtable` is the simplest spec.** For complex pushdown use `query` instead — `option("query", "SELECT ... FROM ... WHERE ...")` runs the query in Snowflake and only ships the result.
- **Case sensitivity** — Snowflake folds unquoted names to UPPERCASE. If a Spark write fails with "table not found" on a lowercase target, quote the name in `dbtable`.

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Connect_Using_Custom_JDBC_Driver.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Connect_Using_Custom_JDBC_Driver.ipynb)
- Snowflake Spark connector docs: <https://docs.snowflake.com/en/user-guide/spark-connector>
