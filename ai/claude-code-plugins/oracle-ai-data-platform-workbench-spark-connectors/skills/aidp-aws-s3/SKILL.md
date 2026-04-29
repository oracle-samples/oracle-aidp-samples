---
description: Read and write AWS S3 (`s3a://`) from an AIDP notebook. Use when the user mentions S3, AWS S3 bucket, s3a, or has AWS access keys. Auth is access key + secret key via the Hadoop S3A connector. boto3 is also available for non-Spark management operations (list, copy).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-aws-s3` — AWS S3 via the S3A connector

Read or write `s3a://<bucket>/<key>` paths from AIDP Spark using AWS access keys. Optional `boto3` path for management operations (list, copy, head).

## When to use
- AIDP needs to consume or land data in AWS S3.
- Mentioned: "S3", "s3a", "AWS bucket".

## When NOT to use
- For OCI Object Storage → [`aidp-object-storage`](../aidp-object-storage/SKILL.md).
- For Azure ADLS Gen2 → [`aidp-azure-adls`](../aidp-azure-adls/SKILL.md).

## Cluster prerequisite — runtime-load BOTH `hadoop-aws` and `aws-java-sdk-bundle`

The AIDP `tpcds` cluster does NOT have `org.apache.hadoop.fs.s3a.S3AFileSystem` pre-installed (verified live 2026-04-27). Both `hadoop-aws-<ver>.jar` (~1 MB) AND `aws-java-sdk-bundle-<ver>.jar` (~280 MB) must be runtime-loaded. **Match `hadoop-aws` to the cluster's exact Hadoop version** (`spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()` — typically `3.3.4` for Spark 3.5.0). Mismatch produces `NoSuchMethodError` deep in `org.apache.hadoop.fs.s3a`.

Beyond the standard runtime-load + DriverManager pattern, S3A also requires telling Hadoop's Configuration which classloader to use — Hadoop's `FileSystem.get()` uses `Configuration.getClassLoader()`, not the JVM thread context loader.

## Spark read (S3A, runtime-loaded driver)

```python
import os, urllib.request
from py4j.java_gateway import java_import

# 1. Confirm cluster's Hadoop version + match hadoop-aws jar
HADOOP_VER = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
print("hadoop:", HADOOP_VER)  # e.g. 3.3.4

JARS = {
    f"/tmp/hadoop-aws-{HADOOP_VER}.jar":
        f"https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/{HADOOP_VER}/hadoop-aws-{HADOOP_VER}.jar",
    "/tmp/aws-java-sdk-bundle-1.12.262.jar":
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
}
for path, url in JARS.items():
    if not os.path.exists(path):
        urllib.request.urlretrieve(url, path)

# 2. Build URLClassLoader covering BOTH jars + set on Hadoop Configuration
gw = spark._sc._gateway
URLArr = gw.new_array(spark._jvm.java.net.URL, len(JARS))
for i, p in enumerate(JARS):
    URLArr[i] = spark._jvm.java.io.File(p).toURI().toURL()
sysCL = spark._jvm.java.lang.ClassLoader.getSystemClassLoader()
ucl = spark._jvm.java.net.URLClassLoader(URLArr, sysCL)

hconf = spark._jsc.hadoopConfiguration()
hconf.setClassLoader(ucl)  # CRITICAL — Hadoop FileSystem lookup uses this, not the thread context

# 3. Configure S3A credentials + endpoint
hconf.set("fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
hconf.set("fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
hconf.set("fs.s3a.endpoint", "s3.amazonaws.com")  # or s3.<region>.amazonaws.com
hconf.set("fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 4. Distribute the jars to executors (driver-only registration won't work for cluster reads)
for p in JARS:
    spark._jsc.addJar(p)

# 5. Read — works for csv/json/parquet/delta
df = spark.read.option("header", "true").csv(
    f"s3a://{os.environ['S3_BUCKET']}/{os.environ['S3_FILE']}"
)
df.show()
```

**Live-validated 2026-04-27**: 2 rows from `s3a://test-data-sep3-2025/csv/sample.csv` via this pattern.

## boto3 fallback (management ops, not data plane)

```python
import boto3, os

s3 = boto3.client(
    "s3",
    aws_access_key_id     = os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key = os.environ["S3_SECRET_KEY"],
    region_name           = os.environ.get("S3_REGION", "us-east-1"),
)

resp = s3.list_objects_v2(Bucket=os.environ["S3_BUCKET"], Prefix="")
for obj in resp.get("Contents", []):
    print(obj["Key"])
```

## Gotchas
- **Use `s3a://` (the Hadoop driver), not `s3://` or `s3n://`.** The latter two are deprecated and may not be present in the cluster.
- **`aws-java-sdk-bundle` version drift** — pin to the version `hadoop-aws` was built against. Lab clusters often need this jar installed; the symptom of mismatch is `NoSuchMethodError` deep in `org.apache.hadoop.fs.s3a` when listing/reading.
- **`Configuration.setClassLoader` is required after runtime-load** — Hadoop's `FileSystem.get()` calls `Configuration.getClassByName()` which uses the Configuration's classloader (not the JVM thread context). Without `hconf.setClassLoader(ucl)`, you get `ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found` even though you just registered the jar.
- **Secrets in env vars only.** Never hard-code keys in notebooks. Source from `.env`/OCI Vault.
- **Region** — `boto3.client('s3', region_name=...)` is required for non-default regions; for the Spark path the bucket region is auto-discovered, but you may need `fs.s3a.endpoint=s3.<region>.amazonaws.com` for non-us-east-1 if listings fail.
- **`boto3` is NOT pre-installed on AIDP cluster** and PyPI mirror is typically unreachable. For management ops (list, copy, head), drive from local rather than cluster.
- **Egress cost & latency** — S3 reads from AIDP cross-cloud. For heavy ETL, copy to OCI Object Storage once and read locally.

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Ingest_from_Multi_Cloud.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Ingest_from_Multi_Cloud.ipynb)
- Hadoop AWS docs: <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/>
