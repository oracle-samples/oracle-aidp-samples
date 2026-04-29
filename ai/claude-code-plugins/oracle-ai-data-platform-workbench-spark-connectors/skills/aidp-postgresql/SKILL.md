---
description: Read or write PostgreSQL from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions PostgreSQL, Postgres, "psql", or has a Postgres host/port to connect to. HTTP-style auth — host/port + user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-postgresql` — PostgreSQL via AIDP `aidataplatform`

## When to use
- User wants to read or write a PostgreSQL database from an AIDP notebook.
- Mentioned: "PostgreSQL", "Postgres", "psql".

## When NOT to use
- For MySQL / HeatWave → [`aidp-mysql`](../aidp-mysql/SKILL.md).
- For SQL Server → [`aidp-sqlserver`](../aidp-sqlserver/SKILL.md).
- For arbitrary JDBC-only DBs → [`aidp-jdbc-custom`](../aidp-jdbc-custom/SKILL.md).

## Read

### Option A: Spark native JDBC (recommended for SSL/Neon/RDS/most production)
The AIDP `aidataplatform` format with `type=POSTGRESQL` silently ignores SSL options — Postgres rejects with `[PostgreSQL]connection is insecure (try using sslmode=require)`. **For SSL-required Postgres targets (Neon, RDS, Aiven, most production deployments) use Spark native JDBC with URL-embedded `sslmode=require`.** The cluster has no `org.postgresql.Driver` pre-installed; runtime-load it the same way `aidp-jdbc-custom` does.

```python
import os, urllib.request
from py4j.java_gateway import java_import

JAR_PATH = "/tmp/postgresql-42.7.4.jar"
if not os.path.exists(JAR_PATH):
    urllib.request.urlretrieve(
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar",
        JAR_PATH,
    )

# Register driver on driver JVM
gw = spark._sc._gateway
url = spark._jvm.java.io.File(JAR_PATH).toURI().toURL()
arr = gw.new_array(spark._jvm.java.net.URL, 1); arr[0] = url
ucl = spark._jvm.java.net.URLClassLoader(arr, spark._jvm.java.lang.ClassLoader.getSystemClassLoader())
spark._jvm.java.lang.Thread.currentThread().setContextClassLoader(ucl)
DriverCls = spark._jvm.java.lang.Class.forName("org.postgresql.Driver", True, ucl)
spark._jvm.java.sql.DriverManager.registerDriver(DriverCls.newInstance())
spark._jsc.addJar(JAR_PATH)  # distribute to executors

# Now read — note sslmode=require URL-embedded
JDBC_URL = (
    f"jdbc:postgresql://{os.environ['PG_HOST']}:{os.environ.get('PG_PORT','5432')}"
    f"/{os.environ['PG_DB']}?sslmode=require"
)
df = (spark.read.format("jdbc")
      .option("url", JDBC_URL)
      .option("driver", "org.postgresql.Driver")
      .option("user", os.environ["PG_USER"])
      .option("password", os.environ["PG_PASSWORD"])
      .option("dbtable", f"{os.environ.get('PG_SCHEMA','public')}.{os.environ['PG_TABLE']}")
      .load())
df.show(5)
```

### Option B: AIDP `aidataplatform` format (only for non-SSL Postgres — rare)
Use this only if your Postgres explicitly accepts non-TLS connections (most managed Postgres services don't).

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="POSTGRESQL",
    host=os.environ["PG_HOST"],
    port=int(os.environ.get("PG_PORT", "5432")),
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    schema=os.environ.get("PG_SCHEMA", "public"),
    table=os.environ["PG_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Write
```python
opts = aidataplatform_options(
    type="POSTGRESQL",
    host=os.environ["PG_HOST"],
    port=int(os.environ.get("PG_PORT", "5432")),
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    schema=os.environ.get("PG_SCHEMA", "public"),
    table=os.environ["PG_TARGET_TABLE"],
    extra={"write.mode": "CREATE"},   # CREATE | APPEND | OVERWRITE
)
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Gotchas
- **SSL** — AIDP `aidataplatform` POSTGRESQL handler silently ignores SSL options (`ssl`, `sslmode`, `jdbc.ssl.enabled`, `encrypt`). For any production / managed Postgres (Neon, RDS, Aiven, etc.) use Option A (Spark native JDBC) with URL-embedded `sslmode=require`. Verified live 2026-04-27 against Neon serverless 17.8.
- **No bundled driver** — the cluster does NOT have `org.postgresql.Driver` pre-installed for native Spark JDBC. Use the runtime-load pattern in Option A (download the jar from Maven Central inside the cluster and register via `URLClassLoader` + `DriverManager.registerDriver` + `spark._jsc.addJar`). The aidataplatform format has its own bundled driver and works without runtime-load.
- **Network reachability** — Postgres must be reachable from the AIDP cluster's NAT egress IP. Public-internet endpoints (Neon, Supabase, RDS public) work via the cluster's NAT path. Self-hosted Postgres in user-managed VCNs typically does NOT work — the cluster's pod CIDR has no route to user VCNs without explicit VCN peering. Smoke-test with a Python socket: `socket.create_connection((host, 5432), timeout=8)`.
- **`schema`** is the Postgres logical schema (e.g. `public`), not the database name. The database name is a separate `PG_DB` env var that goes into the JDBC URL.
- **Write modes** — `CREATE` (fail if exists), `APPEND`, `OVERWRITE`. Default is `CREATE`.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb)
