# aidp_compat â€” Supported Operations

Version: **0.5.3** (wheel: `aidp_compat-0.5.3-py3-none-any.whl`)
OCI auth: **API key** via `/Workspace/<oci-config-workspace-path>` (DEFAULT profile).
Override with env vars: `OCI_CONFIG_FILE` / `OCI_CONFIG_PROFILE`.

> Resource principal auth is NOT used (known failure modes on AIDP; forbidden by policy).

---

## 1. `dbutils.fs.*` â€” File System Operations

Import:

```python
from aidp_compat import dbutils
```

All `dbutils.fs.*` methods support **two path types**:
- **OCI Object Storage** â€” `oci://<bucket>@<namespace>/<key>` (uses OCI Python SDK with API key)
- **Local filesystem** â€” `/Workspace/...`, `/Volumes/...`, `/tmp/...`, etc. (uses native `os`/`shutil`)

`dbfs:/...`, `/mnt/...`, `s3://...` paths are auto-translated to `oci://` via the mount config first.

| Method | Signature | OCI | Local | Recurse | Notes |
|---|---|:---:|:---:|:---:|---|
| `ls` | `dbutils.fs.ls(path) -> List[FileInfo]` | âœ… | âœ… | n/a | Returns FileInfo (path, name, size, modificationTime, isDir, isFile). Subdirectories appear as `isDir=True`. |
| `cp` | `dbutils.fs.cp(src, dst, recurse=False) -> bool` | âœ… | âœ… | âœ… | All 4 directions: ociâ†”local, localâ†”local, ociâ†”oci. `recurse=True` walks a "directory" (prefix). |
| `mv` | `dbutils.fs.mv(src, dst, recurse=False) -> bool` | âœ… | âœ… | âœ… | Cross-scheme = copy then delete src. |
| `rm` | `dbutils.fs.rm(path, recurse=False) -> bool` | âœ… | âœ… | âœ… | Recurse for prefix removal. |
| `mkdirs` | `dbutils.fs.mkdirs(path) -> bool` | âœ… | âœ… | n/a | OCI: writes a zero-byte placeholder ending in `/` (Object Storage is flat). Local: `os.makedirs`. |
| `head` | `dbutils.fs.head(path, max_bytes=65536) -> str` | âœ… | âœ… | n/a | OCI: uses Range request for partial download. Returns UTF-8 string. |
| `put` | `dbutils.fs.put(path, contents, overwrite=False) -> bool` | âœ… | âœ… | n/a | Raises `FileExistsError` if `overwrite=False` and target exists. |
| `mount` | `dbutils.fs.mount(source, mountPoint, ...) -> bool` | n/a | n/a | n/a | Path mapping only (not a real FUSE mount). Stored in memory + env vars. |
| `unmount` | `dbutils.fs.unmount(mountPoint) -> bool` | n/a | n/a | n/a | Removes a path mapping. |
| `mounts` | `dbutils.fs.mounts() -> List[MountInfo]` | n/a | n/a | n/a | Lists current mappings. |
| `refreshMounts` | `dbutils.fs.refreshMounts() -> bool` | n/a | n/a | n/a | Reloads `AIDP_MOUNT_CONFIG` json. |
| `updateMount` | `dbutils.fs.updateMount(source, mountPoint, ...) -> bool` | n/a | n/a | n/a | Alias for `mount`. |
| `help` | `dbutils.fs.help([method]) -> None` | n/a | n/a | n/a | Prints help text. |

**Implementation note**: All file-content ops use OCI Python SDK directly (no `jvm.org.apache.hadoop.fs`). Works in both interactive notebooks AND scheduled workflows. `copy_object` waits for OCI's async work-request to COMPLETE (up to 300s timeout).

---

## 2. Other `dbutils.*` Namespaces

| Namespace | Status | Notes |
|---|---|---|
| `dbutils.fs.*` | âœ… Full | See table above. |
| `dbutils.widgets.*` | âœ… | Backed by `oidlUtils.parameters` on AIDP. Use `oidlUtils.parameters.getParameter("name", "default")` for new code. |
| `dbutils.secrets.*` | âœ… | **Vault-only by default**: OCI Vault (API key auth), requires `AIDP_VAULT_OCID` env var + secret named `<scope>/<key>`. Plaintext `AIDP_SECRET_<SCOPE>_<KEY>` env vars and the JSON file named by `AIDP_SECRETS_FILE` are consulted only with `AIDP_ALLOW_PLAINTEXT_SECRETS=1` (file must be `0600` on POSIX; one `insecure plaintext secrets mode active` log line, never values). `AIDP_SECRET_SCOPES=<scope,scope>` restricts `get`/`list`/`listScopes`; list operations return names only. |
| `dbutils.notebook.*` | âœ… | `notebook.run` and `notebook.exit` go to `oidlUtils.notebook.*`. Use `oidlUtils.notebook.run(path, timeout=3600)` for new code (timeout=0 is rejected by AIDP). The compat `notebook.run` passes every non-magic cell through the mandatory sandbox gate (section 8) before `exec`. |
| `dbutils.library.*` | âš ï¸ | No-op shim. Cluster libraries must be installed via cluster libraries API, not at runtime. |
| `dbutils.credentials.*` | âš ï¸ | Stub only. Use API key auth directly. `assumeRole` not supported. |
| `dbutils.data.*` | âš ï¸ | Stub. Use pandas `describe()` instead. |
| `dbutils.jobs.*` | âœ… | `dbutils.jobs.taskValues.get/set` works. |

---

## 3. Top-Level Helpers

```python
from aidp_compat import displayHTML, display, sql, translate_path, set_notebook_dir
```

| Function | Purpose | Example |
|---|---|---|
| `display(df)` | Pretty-print Spark/Pandas DataFrame in notebook | `display(spark.read.table("default.x.y"))` |
| `displayHTML(html)` | Render HTML in notebook | `displayHTML("<b>title</b>")` |
| `sql(query)` | Run a SQL query, return DataFrame | `df = sql("SELECT * FROM default.x.y LIMIT 10")` |
| `translate_path(p)` | Idempotent `/dbfs/FileStore/...` â†’ `/Volumes/default/default/dbfs/FileStore/...` translation. NO-OP on already-translated paths. | `local = translate_path("/dbfs/FileStore/foo.csv")` |
| `set_notebook_dir(path)` | Set the dir used by `dbutils.notebook.run("../relative")` to resolve relative paths | Called automatically by the migration tool's bootstrap. |

---

## 4. `aidp_compat.safe_io` â€” Spark-Safe I/O Helpers

For situations where naive `df.write.parquet(...)` / `pickle.dump(...)` can corrupt or lose data on AIDP. Import individually:

```python
from aidp_compat import safe_pickle_dump, safe_pickle_load, safe_write_parquet, ...
```

| Function | Purpose |
|---|---|
| `safe_pickle_dump(obj, path)` / `safe_pickle_load(path)` | Atomic pickle write/read (handles FUSE write-then-read consistency) |
| `safe_write_parquet(df, path, mode="overwrite", partitionBy=None)` | DataFrame write with overwrite-safety (clears stale `_temporary` dirs) |
| `safe_save_as_table(df, table_name, mode="overwrite", ...)` | Like saveAsTable but with retry + cleanup |
| `safe_read_modify_write_parquet(...)` | Read + transform + write back without leaving the original in inconsistent state |
| `safe_write_parquet_coalesced(df, path, num_files=1, ...)` | Write N target files (use for small-result optimization) |
| `safe_save_as_table_coalesced(...)` | Same coalesce + saveAsTable |
| `safe_pandas_to_csv(pdf, path, ...)` | Pandas â†’ CSV with `/Volumes` FUSE retry |
| `safe_materialize(df)` / `safe_unpersist(df)` | Force-cache + cleanup helpers |
| `safe_read_file(path)` | Read text file with FUSE retry |
| `load_saved_model_from_volumes(path)` | Read model artifacts from `/Volumes/...` (handles FUSE delays) |
| `safe_joblib_dump(obj, path)` / `safe_joblib_load(path)` | joblib with FUSE-safe semantics |

All `safe_io` helpers are **pure Python / PySpark** â€” no JVM Hadoop FS dependency.

---

## 5. `aidp_compat.s3_compat` â€” S3 â†’ OCI Routing

For code that still references S3 buckets (use only if migrating boto3 code minimally):

```python
from aidp_compat.s3_compat import read_s3_object, write_s3_object
data = read_s3_object("source-prod-bucket", "path/to/key")  # auto-routes to OCI
write_s3_object("source-prod-bucket", "path/to/key", data)
```

S3-to-OCI bucket mapping comes from `reports/s3_to_oci_bucket_mapping.csv`. Uses OCI SDK with API key auth.

---

## 6. `aidp_compat.glue_compat` â€” AWS Glue Replacement

```python
from aidp_compat import get_glue_table_s3_location
location = get_glue_table_s3_location("database_name", "table_name")
# Internally calls: spark.sql(f"DESCRIBE FORMATTED `{db}`.`{tbl}`")
```

Use as a drop-in replacement for `boto3.client('glue').get_table(...)['Table']['StorageDescriptor']['Location']`.

---

## 7. `aidp_compat.oci_throttle` â€” Object Storage Tuning

For bulk migrations or high-concurrency object-storage workloads:

```python
from aidp_compat.oci_throttle import tune_for_parallel_migration
tune_for_parallel_migration(spark, concurrent_jobs=48, verbose=True)
```

Applies CircuitBreaker + retry tuning to mitigate OCI 429 bursts. Profiles: conservative â‰¤8, balanced 9-200, aggressive >200.

---

## 8. `aidp_compat.notebook_policy` — Mandatory Sandbox Gate

Runtime enforcement at the execution sink. `dbutils.notebook.run(...)`, `dbutils.fs.rm/mv/cp/put` and every `safe_io` write helper refuse to run until a sandbox is declared; undeclared = `PermissionError` naming the missing variables (fail closed).

| Variable | Meaning |
|---|---|
| `AIDP_SANDBOX_CATALOG` / `AIDP_SANDBOX_SCHEMA` | Only `catalog.schema` that `saveAsTable` / `insertInto` / `CREATE TABLE` / `INSERT` / `DROP` / `MERGE` / `DELETE` may target (1-part names count as outside) |
| `AIDP_SANDBOX_PREFIX` | `oci://bucket@ns/path/` that path writes, `dbutils.fs` deletes/moves and absolute `open()` must stay under; comma list to add a `/Volumes/...` staging prefix |
| `AIDP_SANDBOX_ALLOW_NETWORK` | `1` permits `socket` / `requests` / `urllib` / `http.client` / `paramiko` imports (default off) |
| `AIDP_NOTEBOOK_POLICY_ALLOW` | Comma list of rule ids accepted as a reviewed exception (logged as `allowed_exception`) |
| `AIDP_NOTEBOOK_POLICY_LOG` | Optional JSONL file mirroring the in-process policy log |

Or declare programmatically: `from aidp_compat import SandboxPolicy, set_sandbox_policy; set_sandbox_policy(SandboxPolicy(catalog="default", schema="<sandbox>", prefix="oci://<bucket>@<ns>/<sandbox>/"))`. `job_migrate.py` does this on every cluster connect using the write-redirect schema/bucket.

Before each non-magic cell runs it is `ast.parse`d and refused (`PolicyViolation`, a `PermissionError` with notebook path, cell index, rule id, target and remediation) on:

| Rule | Refuses |
|---|---|
| `NBP-IMPORT-PROCESS` | `import subprocess` / `ctypes` / `pty` |
| `NBP-IMPORT-NETWORK` | `import socket` / `requests` / `urllib` / `http.client` / `paramiko` (unless `AIDP_SANDBOX_ALLOW_NETWORK=1`) |
| `NBP-OS-ENVIRON` / `NBP-OS-PROCESS` | `os.environ` / `os.getenv` / `os.putenv`; `os.system` / `os.popen` / `os.exec*` / `os.spawn*` |
| `NBP-SHUTIL-RMTREE` | `shutil.rmtree` |
| `NBP-BUILTIN-EXEC` | `eval` / `exec` / `compile` / `__import__` |
| `NBP-OPEN-PATH` / `NBP-OPEN-DYNAMIC` | `open()` on an absolute literal path outside the prefix; `open()` for writing with a non-literal path |
| `NBP-DBFS-PATH` / `NBP-DBFS-DYNAMIC` | `*.fs.rm/mv/cp/put` whose literal target is outside the prefix; non-literal target |
| `NBP-TABLE-TARGET` / `NBP-TABLE-DYNAMIC` | `.write...saveAsTable/insertInto` outside `catalog.schema`; non-literal name |
| `NBP-SQL-TARGET` / `NBP-SQL-DYNAMIC` | `spark.sql` / `sql` DDL/DML (and `LOCATION`) outside the sandbox; f-string / concatenated DDL/DML |
| `NBP-PATH-WRITE` / `NBP-PATH-WRITE-DYNAMIC` | `.write...parquet/csv/json/orc/text/save` outside the prefix; non-literal path |
| `NBP-RUNTIME-PATH` / `NBP-RUNTIME-TABLE` | Runtime assertion inside `dbutils.fs.*` / `safe_io` write helpers |

`CREATE [OR REPLACE] [GLOBAL] TEMP VIEW` and plain `SELECT` are not writes. Every refusal and allowed exception is appended to the policy log (`get_policy_log()`, `policy_log_markdown()`); `job_migrate.py` pulls it into each task's test report as **Notebook Policy Log**.

---

## 9. What NOT to Use in Migrated Notebooks

âŒ **Direct JVM Hadoop FS calls** â€” `spark._jvm.org.apache.hadoop.fs.FileSystem.get(...)`, `fs.open(...)`, `fs.exists(...)`. These FAIL in scheduled workflow runs even though they work interactively.
**Use instead**: `dbutils.fs.*` (above) or OCI Python SDK directly.

âŒ **`oci.auth.signers.get_resource_principals_signer()`** â€” Known failure modes on AIDP.
**Use instead**: API key auth (see top of this doc).

âŒ **`boto3` for S3** â€” AWS SDK not configured on AIDP.
**Use instead**: `aidp_compat.s3_compat` for routing, or OCI SDK directly.

âŒ **`dbutils.notebook.run(path, 0, ...)`** â€” AIDP rejects `timeout=0`.
**Use instead**: `oidlUtils.notebook.run(path, timeout=3600, ...)`.

---

## 10. Quick Smoke Test

After installing the wheel, run this in a notebook to verify install:

```python
import os
from aidp_compat import dbutils

OCI_BASE = "oci://oracle_ai_automation@<WORKSPACE_NAMESPACE>/aidp_compat_smoke"
# Declare the sandbox first: dbutils.fs writes refuse without it (section 8).
os.environ.setdefault("AIDP_SANDBOX_CATALOG", "default")
os.environ.setdefault("AIDP_SANDBOX_SCHEMA", "<sandbox_schema>")
os.environ.setdefault("AIDP_SANDBOX_PREFIX", OCI_BASE + "/")
dbutils.fs.put(f"{OCI_BASE}/hi.txt", "hello", overwrite=True)
print(dbutils.fs.head(f"{OCI_BASE}/hi.txt", 100))
dbutils.fs.rm(f"{OCI_BASE}/", recurse=True)
print("âœ“ smoke test passed")
```

---

## 11. Changelog (recent)

| Version | Date | Highlights |
|---|---|---|
| unreleased | 2026-10 | `notebook_policy` mandatory sandbox gate on `notebook.run` + runtime assertions in `fs`/`safe_io` writers; `secrets` Vault-only by default (plaintext opt-in, `0600` file check, scope allowlist) |
| **0.5.3** | 2026-05-20 | `cp/mv` OCI â†’ OCI now waits for async `copy_object` work-request to COMPLETE |
| 0.5.2 | 2026-05-20 | `cp` OCI â†’ OCI passes required `destination_region` to `copy_object` |
| 0.5.1 | 2026-05-20 | All `dbutils.fs.*` methods rewritten to use OCI Python SDK (API key) â€” works in workflow; `s3_compat`, `secrets` use API key auth; resource principal removed |
| 0.5.0 | (prior) | JVM Hadoop FS based; resource principal auth |
