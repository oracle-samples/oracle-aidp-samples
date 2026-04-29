---
description: Connect to ANY database that has a JDBC driver from an AIDP notebook using Spark's native `format("jdbc")`. Use when the user mentions a DB without a dedicated AIDP connector — SQLite, ClickHouse, DuckDB, generic JDBC URL — or wants to use a custom JDBC driver they uploaded. Auth is driver-specific.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-jdbc-custom` — Generic JDBC escape hatch

The catch-all skill for any DB with a JDBC driver. Skips the AIDP `aidataplatform` format and uses native Spark JDBC. Useful for DBs like SQLite, ClickHouse, DuckDB, IBM DB2, SAP HANA, or any niche driver the user has uploaded.

## When to use
- The DB doesn't have a dedicated `aidp-*` skill in this plugin.
- User has a `.jar` JDBC driver they want to use.
- Mentioned: "custom JDBC", "JDBC driver", "any JDBC".

## When NOT to use
- For Postgres / MySQL / SQL Server / Oracle → use the dedicated skill. The `aidataplatform` format gives the connector pushdown and connection pooling that this skill doesn't.
- For Snowflake → [`aidp-snowflake`](../aidp-snowflake/SKILL.md). The Spark connector is much better than raw JDBC.

## Two ways to load a non-bundled JDBC driver

### Option A — Runtime-load (recommended; no cluster restart) ★ live-validated

The plugin ships a helper that loads a JDBC JAR into a running Spark session via Java's URLClassLoader + DriverManager. It works without admin access and without restarting the kernel. Live-validated on the AIDP `tpcds` cluster (Spark 3.5.0) — see `tests/live-results/row24.json`.

```python
import os
from oracle_ai_data_platform_connectors.jdbc import (
    add_jdbc_jar_at_runtime, download_jdbc_jar,
)

# Download once (Maven Central is reachable from AIDP clusters)
jar = download_jdbc_jar(
    maven_url="https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.46.0.0/sqlite-jdbc-3.46.0.0.jar",
    target_path="/tmp/sqlite-jdbc-3.46.0.0.jar",
)

# Register with the running Spark session
add_jdbc_jar_at_runtime(spark, jar_path=jar, driver_class="org.sqlite.JDBC")

# Now standard Spark JDBC works
df = (spark.read.format("jdbc")
      .option("url",      "jdbc:sqlite::memory:")
      .option("driver",   "org.sqlite.JDBC")
      .option("dbtable",  "(SELECT 1 AS c1, 2 AS c2, 3 AS c3)")
      .option("fetchsize","1000")
      .load())
df.show()
```

The helper is implemented at [scripts/oracle_ai_data_platform_connectors/jdbc/runtime_load.py](../../scripts/oracle_ai_data_platform_connectors/jdbc/runtime_load.py) — internally it builds a URLClassLoader rooted at the existing thread context loader, calls `DriverManager.registerDriver`, and sets the JVM thread context class loader so Spark's `Utils.classForName` resolves the driver class.

### Option B — Cluster Library tab (durable, requires admin)

For frequently-used drivers, upload the JAR to a Volume and attach via the cluster Library tab. This persists across cluster restarts. Requires cluster admin access. After attach + restart:

```python
# Driver class is now on the system classpath; no runtime trick needed.
df = (spark.read.format("jdbc")
      .option("url", JDBC_URL).option("driver", DRIVER)
      .option("dbtable", TABLE).load())
```

## Generic template

```python
df = (spark.read.format("jdbc")
      .option("url",      "jdbc:<vendor>://<host>:<port>/<db>")
      .option("driver",   "<full.class.Name>")
      .option("user",     os.environ["CUST_DB_USER"])
      .option("password", os.environ["CUST_DB_PASSWORD"])
      .option("dbtable",  os.environ["CUST_DB_TABLE"])
      .option("fetchsize", "10000")
      .load())
```

## Common driver classes

| DB | Driver class | URL prefix |
|---|---|---|
| SQLite | `org.sqlite.JDBC` | `jdbc:sqlite:` |
| ClickHouse | `com.clickhouse.jdbc.ClickHouseDriver` | `jdbc:clickhouse://` |
| DuckDB | `org.duckdb.DuckDBDriver` | `jdbc:duckdb:` |
| IBM DB2 | `com.ibm.db2.jcc.DB2Driver` | `jdbc:db2://` |
| SAP HANA | `com.sap.db.jdbc.Driver` | `jdbc:sap://` |
| Vertica | `com.vertica.jdbc.Driver` | `jdbc:vertica://` |

## Gotchas
- **No predicate pushdown beyond what Spark JDBC infers.** This skill is the escape hatch, not the optimized path.
- **`dbtable` accepts a subquery** — wrap in parens to filter at the source: `option("dbtable", "(SELECT * FROM big_table WHERE date > '2025-01-01') t")`.
- **`fetchsize=10000`** is a good default; smaller values create driver chatter, larger values risk OOM on the executor.
- **Partitioning** — for parallel reads, use `option("partitionColumn", ...).option("lowerBound", ...).option("upperBound", ...).option("numPartitions", N)`. Without these the read is single-partition and serial.
- **Driver JAR mismatch** — symptom is `ClassNotFoundException: <driver class>`. Re-check that the JAR is attached to the running cluster (not just uploaded to a Volume).

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Connect_Using_Custom_JDBC_Driver.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Connect_Using_Custom_JDBC_Driver.ipynb)
- Spark JDBC docs: <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>
