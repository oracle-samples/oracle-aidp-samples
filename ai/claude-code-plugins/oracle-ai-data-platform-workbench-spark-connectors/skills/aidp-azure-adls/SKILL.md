---
description: Read and write Azure Data Lake Storage Gen2 (`abfss://`) from an AIDP notebook. Use when the user mentions ADLS, Azure Data Lake, abfss, or wants to ingest from a multi-cloud Azure source. Auth is OAuth client-credentials (Service Principal client_id + secret + tenant).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-azure-adls` — Azure ADLS Gen2 via OAuth client-credentials

Read or write `abfss://<container>@<storage_account>.dfs.core.windows.net/...` paths from AIDP Spark using a Service Principal.

## When to use
- AIDP needs to consume or land data in Azure ADLS Gen2.
- Mentioned: "ADLS", "abfss", "Azure Data Lake".

## When NOT to use
- For OCI Object Storage → [`aidp-object-storage`](../aidp-object-storage/SKILL.md).
- For AWS S3 → [`aidp-aws-s3`](../aidp-aws-s3/SKILL.md).

## One-time auth setup

Configure the Spark Hadoop connector with Service-Principal credentials. Do this once per session/job:

```python
import os

storage_account = os.environ["ADLS_STORAGE_ACCOUNT"]   # account name only, no .dfs...
client_id       = os.environ["ADLS_CLIENT_ID"]          # SP application (client) id
client_secret   = os.environ["ADLS_CLIENT_SECRET"]      # SP secret value
tenant          = os.environ["ADLS_TENANT"]             # Azure AD tenant id (GUID)

base = f"fs.azure.account"
host = f"{storage_account}.dfs.core.windows.net"

spark.conf.set(f"{base}.auth.type.{host}",                 "OAuth")
spark.conf.set(f"{base}.oauth.provider.type.{host}",       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"{base}.oauth2.client.id.{host}",          client_id)
spark.conf.set(f"{base}.oauth2.client.secret.{host}",      client_secret)
spark.conf.set(f"{base}.oauth2.client.endpoint.{host}",    f"https://login.microsoftonline.com/{tenant}/oauth2/token")
```

## Read

```python
container = os.environ["ADLS_CONTAINER"]
data_file = os.environ["ADLS_DATA_FILE"]   # e.g. "data/2025/january/orders.csv"

df = (spark.read
      .format("csv")
      .option("header", True)
      .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/{data_file}"))
df.show()
```

## Write (e.g. land into a managed Delta table)

```python
(df.write
   .mode("overwrite")
   .format("delta")
   .saveAsTable("default.default.data_from_adls"))
```

## Gotchas
- **Service Principal must have RBAC on the storage account.** Assign `Storage Blob Data Contributor` (or Reader for read-only) on the container or the account.
- **Hierarchical Namespace must be enabled** on the storage account for `abfss://` to work (ADLS Gen2 = HNS-on storage account).
- **Secrets in env vars** — never hard-code in notebooks. Source from a `.env` file gitignored, or from OCI Vault via `oracle_ai_data_platform_connectors.auth.secrets.get(name)`.
- **Endpoint URL** — `login.microsoftonline.com/<tenant>/oauth2/token` is the v1 endpoint and is what the `ClientCredsTokenProvider` expects. Don't use the v2 endpoint here.
- **`abfss://` not `abfs://`** — always use the TLS variant.

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Ingest_from_Multi_Cloud.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Ingest_from_Multi_Cloud.ipynb)
- Hadoop Azure docs: <https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html>
