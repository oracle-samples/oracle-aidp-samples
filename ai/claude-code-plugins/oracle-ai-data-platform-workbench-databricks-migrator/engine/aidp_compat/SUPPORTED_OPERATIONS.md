# aidp_compat — Supported Operations

Version: **0.5.0** (wheel: `aidp_compat-0.5.0-py3-none-any.whl`)
OCI auth: **API key** via `/Workspace/<oci-config-workspace-path>` (DEFAULT profile).
Override with env vars: `OCI_CONFIG_FILE` / `OCI_CONFIG_PROFILE`.

> Resource principal auth is NOT used (known failure modes on AIDP; forbidden by policy).

---

## 1. `dbutils.fs.*` — File System Operations

Import:

```python
from aidp_compat import dbutils
```

All `dbutils.fs.*` methods support **two path types**:
- **OCI Object Storage** — `oci://<bucket>@<namespace>/<key>` (uses OCI Python SDK with API key)
- **Local filesystem** — `/Workspace/...`, `/Volumes/...`, `/tmp/...`, etc. (uses native `os`/`shutil`)

`dbfs:/...`, `/mnt/...`, `s3://...` paths are auto-translated to `oci://` via the mount config first.

| Method | Signature | OCI | Local | Recurse | Notes |
|---|---|:---:|:---:|:---:|---|
| `ls` | `dbutils.fs.ls(path) -> List[FileInfo]` | ✅ | ✅ | n/a | Returns FileInfo (path, name, size, modificationTime, isDir, isFile). Subdirectories appear as `isDir=True`. |
| `cp` | `dbutils.fs.cp(src, dst, recurse=False) -> bool` | ✅ | ✅ | ✅ | All 4 directions: oci↔local, local↔local, oci↔oci. `recurse=True` walks a "directory" (prefix). |
| `mv` | `dbutils.fs.mv(src, dst, recurse=False) -> bool` | ✅ | ✅ | ✅ | Cross-scheme = copy then delete src. |
| `rm` | `dbutils.fs.rm(path, recurse=False) -> bool` | ✅ | ✅ | ✅ | Recurse for prefix removal. |
| `mkdirs` | `dbutils.fs.mkdirs(path) -> bool` | ✅ | ✅ | n/a | OCI: writes a zero-byte placeholder ending in `/` (Object Storage is flat). Local: `os.makedirs`. |
| `head` | `dbutils.fs.head(path, max_bytes=65536) -> str` | ✅ | ✅ | n/a | OCI: uses Range request for partial download. Returns UTF-8 string. |
| `put` | `dbutils.fs.put(path, contents, overwrite=False) -> bool` | ✅ | ✅ | n/a | Raises `FileExistsError` if `overwrite=False` and target exists. |
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
| `dbutils.fs.*` | ✅ Full | See table above. |
| `dbutils.widgets.*` | ✅ | Backed by `oidlUtils.parameters` on AIDP. Use `oidlUtils.parameters.getParameter("name", "default")` for new code. |
| `dbutils.secrets.*` | ✅ | **Vault-only by default**: OCI Vault (API key auth), requires `AIDP_VAULT_OCID` env var + secret named `<scope>/<key>`. Plaintext `AIDP_SECRET_<SCOPE>_<KEY>` env vars and the JSON file named by `AIDP_SECRETS_FILE` are consulted only with `AIDP_ALLOW_PLAINTEXT_SECRETS=1` (file must be `0600` on POSIX; one `insecure plaintext secrets mode active` log line, never values). `AIDP_SECRET_SCOPES=<scope,scope>` and `AIDP_SECRET_KEYS=<scope/key,scope/*,...>` restrict `get`/`list`/`listScopes` (read per call; `none` = nothing allowed; a refusal is logged as `NBP-RUNTIME-SECRET`); `job_migrate.py` derives both from the `dbutils.secrets.get(scope, key)` literals of the planned notebooks and declares them in the cluster bootstrap (an operator-provided value wins). List operations return names only. |
| `dbutils.notebook.*` | ✅ | `notebook.run` and `notebook.exit` go to `oidlUtils.notebook.*`. Use `oidlUtils.notebook.run(path, timeout=3600)` for new code (timeout=0 is rejected by AIDP). The compat `notebook.run` passes every non-magic cell through the mandatory sandbox gate (section 8) before `exec`. |
| `dbutils.library.*` | ⚠️ | No-op shim. Cluster libraries must be installed via cluster libraries API, not at runtime. |
| `dbutils.credentials.*` | ⚠️ | Stub only. Use API key auth directly. `assumeRole` not supported. |
| `dbutils.data.*` | ⚠️ | Stub. Use pandas `describe()` instead. |
| `dbutils.jobs.*` | ✅ | `dbutils.jobs.taskValues.get/set` works. |

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
| `translate_path(p)` | Idempotent `/dbfs/FileStore/...` → `/Volumes/default/default/dbfs/FileStore/...` translation. NO-OP on already-translated paths. | `local = translate_path("/dbfs/FileStore/foo.csv")` |
| `set_notebook_dir(path)` | Set the dir used by `dbutils.notebook.run("../relative")` to resolve relative paths | Called automatically by the migration tool's bootstrap. |

---

## 4. `aidp_compat.safe_io` — Spark-Safe I/O Helpers

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
| `safe_pandas_to_csv(pdf, path, ...)` | Pandas → CSV with `/Volumes` FUSE retry |
| `safe_materialize(df)` / `safe_unpersist(df)` | Force-cache + cleanup helpers |
| `safe_read_file(path)` | Read text file with FUSE retry |
| `load_saved_model_from_volumes(path)` | Read model artifacts from `/Volumes/...` (handles FUSE delays) |
| `safe_joblib_dump(obj, path)` / `safe_joblib_load(path)` | joblib with FUSE-safe semantics |

All `safe_io` helpers are **pure Python / PySpark** — no JVM Hadoop FS dependency.

---

## 5. `aidp_compat.s3_compat` — S3 → OCI Routing

For code that still references S3 buckets (use only if migrating boto3 code minimally):

```python
from aidp_compat.s3_compat import read_s3_object, write_s3_object
data = read_s3_object("<source_bucket>", "path/to/key")  # auto-routes to OCI
write_s3_object("<source_bucket>", "path/to/key", data)
```

S3-to-OCI bucket mapping comes from `reports/s3_to_oci_bucket_mapping.csv`. Uses OCI SDK with API key auth.

---

## 6. `aidp_compat.glue_compat` — AWS Glue Replacement

```python
from aidp_compat import get_glue_table_s3_location
location = get_glue_table_s3_location("database_name", "table_name")
# Internally calls: spark.sql(f"DESCRIBE FORMATTED `{db}`.`{tbl}`")
```

Use as a drop-in replacement for `boto3.client('glue').get_table(...)['Table']['StorageDescriptor']['Location']`.

---

## 7. `aidp_compat.oci_throttle` — Object Storage Tuning

For bulk migrations or high-concurrency object-storage workloads:

```python
from aidp_compat.oci_throttle import tune_for_parallel_migration
tune_for_parallel_migration(spark, concurrent_jobs=48, verbose=True)
```

Applies CircuitBreaker + retry tuning to mitigate OCI 429 bursts. Profiles: conservative ≤8, balanced 9-200, aggressive >200.

---

## 8. `aidp_compat.notebook_policy` — Mandatory Sandbox Gate

Runtime enforcement at the execution sink. `dbutils.notebook.run(...)`, `dbutils.fs.rm/mv/cp/put/mkdirs` and every `safe_io` write helper refuse to run until a sandbox is declared; undeclared = `PermissionError` naming the missing variables (fail closed).

| Variable | Meaning |
|---|---|
| `AIDP_SANDBOX_CATALOG` / `AIDP_SANDBOX_SCHEMA` | Only `catalog.schema` that `saveAsTable` / `insertInto` / `writeTo` / `CREATE TABLE` / `INSERT` / `DROP` / `MERGE` / `DELETE` may target (1-part names count as outside) |
| `AIDP_SANDBOX_PREFIX` | Comma list of prefixes that path writes, `dbutils.fs` deletes/moves/mkdirs, `os`/`shutil`/`pathlib` file operations and absolute `open()` must stay under. The first entry is the object-storage prefix (`oci://bucket@ns/path/`); the rest are staging areas. Paths are canonicalised first; a `..` segment is always outside, and on POSIX a symlink inside a prefix pointing outside is outside |
| `AIDP_SANDBOX_ALLOW_NETWORK` | `1` permits `socket` / `requests` / `urllib` / `urllib3` / `httpx` / `aiohttp` / `http.client` / `paramiko` / `ftplib` / `smtplib` / ... imports (default off) |
| `AIDP_NOTEBOOK_POLICY_ALLOW` | Comma list of rule ids accepted as a reviewed exception (logged as `allowed_exception`) |
| `AIDP_NOTEBOOK_POLICY_LOG` | Optional JSONL file mirroring the in-process policy log |
| `AIDP_SECRET_SCOPES` / `AIDP_SECRET_KEYS` | `dbutils.secrets` allowlist (scopes; `scope/key` or `scope/*`), see `dbutils.secrets.*` above; `none` = nothing allowed |

Or declare programmatically: `from aidp_compat import SandboxPolicy, set_sandbox_policy; set_sandbox_policy(SandboxPolicy(catalog="default", schema="<sandbox>", prefix="oci://<bucket>@<ns>/<sandbox>/", extra_prefixes=("/Volumes/default/default/dbfs/",)))`.

**Declared once, frozen for the kernel.** The first policy resolved (explicit object, or the environment on first use) is a one-shot snapshot: later `AIDP_SANDBOX_*` changes are ignored by every assertion, and `set_sandbox_policy` with a different policy (or `None`) is refused as `NBP-POLICY-TAMPER`. Re-running the same declaration (bootstrap replay on reconnect) is a no-op.

**What `job_migrate.py` declares** on every cluster connect and per task (`build_sandbox_policy_snippet`; an operator-provided variable wins and replaces the whole list):

| Prefix | Why |
|---|---|
| `oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/` | the write-redirect bucket; every OCI write the migrator produces lands here |
| `/Volumes/default/default/dbfs/` | the `/dbfs/` and `dbfs:/` translation target the migration prompt rewrites paths to |
| `/tmp/` | local-disk staging the prompt prescribes for torch / h5py / sqlite before copying to `/Volumes` |
| `<output-base>/` | the tool's own output directory (`task_values.json`, `manifest_params.json`, reports) |

Together with catalog `default` / schema `<oci_backup_bucket>_overwrite` and the planned `dbutils.secrets` allowlist (section `dbutils.secrets.*`). Everything else on `/Volumes`, `/Workspace` or in other buckets stays refused.

Before each non-magic cell runs it is `ast.parse`d and refused (`PolicyViolation`, a `PermissionError` with notebook path, cell index, rule id, target and remediation) on:

| Rule | Refuses |
|---|---|
| `NBP-IMPORT-PROCESS` | `import subprocess` / `ctypes` / `pty` / `multiprocessing` |
| `NBP-IMPORT-NETWORK` | `import socket` / `requests` / `urllib` / `urllib3` / `httpx` / `aiohttp` / `http.client` / `paramiko` / `ftplib` / `smtplib` / `pycurl` / `telnetlib` / `poplib` / `imaplib` / `websocket(s)` / `grpc` / `xmlrpc` (unless `AIDP_SANDBOX_ALLOW_NETWORK=1`) |
| `NBP-OS-ENVIRON` / `NBP-OS-PROCESS` | `os.environ` / `os.getenv` / `os.putenv` / `from os import *` (also through an alias, `import os as o`); `os.system` / `os.popen` / `os.exec*` / `os.spawn*` / `os.fork` / `asyncio.create_subprocess_*` |
| `NBP-SHUTIL-RMTREE` | `shutil.rmtree` |
| `NBP-BUILTIN-EXEC` | `eval` / `exec` / `compile` / `__import__` |
| `NBP-INDIRECT` | `import importlib` / `builtins`, `sys.modules`, `getattr()` / `vars()` on `os` / `sys` / `builtins` / the compat shims |
| `NBP-POLICY-TAMPER` | `import aidp_compat[.x]`, `from aidp_compat[.fs/.safe_io/.secrets/.notebook] import` of the policy API / shim classes / private names, anything from `aidp_compat.notebook_policy`, attribute assignment / `del` / `setattr` on `os` / `sys` / `builtins` / `shutil` / `importlib` / `aidp_compat` / `dbutils` / `spark`, private `dbutils._x` attributes; at runtime, redeclaring or clearing the frozen policy |
| `NBP-OPEN-PATH` / `NBP-OPEN-DYNAMIC` | `open()` and `Path(...).read_text/read_bytes/write_text/write_bytes/open` on an absolute path outside the prefix; any mode with a path that cannot be resolved statically |
| `NBP-FS-PATH` / `NBP-FS-DYNAMIC` | `os.remove/unlink/rmdir/removedirs/rename/renames/replace/truncate/mkdir/makedirs/link/symlink`, `shutil.move/copy/copy2/copyfile/copytree` (destination), `Path(...).unlink/rmdir/mkdir/touch/rename/replace/symlink_to` outside the prefix; unresolvable path |
| `NBP-DBFS-PATH` / `NBP-DBFS-DYNAMIC` | `*.fs.rm/mv/cp/put/mkdirs` (also through `fs = dbutils.fs`) whose target is outside the prefix; unresolvable target |
| `NBP-TABLE-TARGET` / `NBP-TABLE-DYNAMIC` | `.write...saveAsTable/insertInto`, `.writeTo(...)` outside `catalog.schema`; unresolvable name |
| `NBP-SQL-TARGET` / `NBP-SQL-DYNAMIC` | `spark.sql` / `sql` DDL/DML (and `LOCATION '..'` / `LOCATION ".."` / `OPTIONS (path ...)`) outside the sandbox; a statement that cannot be resolved statically (f-string / concatenation whose literal head is DDL/DML, a variable, a call) |
| `NBP-PATH-WRITE` / `NBP-PATH-WRITE-DYNAMIC` | `.write...parquet/csv/json/orc/text/save`, `.write...option("path", ..)` / `.options(path=..)` outside the prefix; unresolvable path |
| `NBP-RUNTIME-PATH` / `NBP-RUNTIME-TABLE` / `NBP-RUNTIME-SECRET` | Runtime assertion inside `dbutils.fs.*` / `safe_io` write helpers / `dbutils.secrets` |

**Static resolution.** A target that is a plain name bound exactly once (in this or an earlier cell of the run) to a string literal is checked as that literal; `import x as y`, `from m import n as y` and `y = dbutils.fs` aliases are followed across cells. Anything else that is not a literal is a `*-DYNAMIC` refusal unless its rule id is in `AIDP_NOTEBOOK_POLICY_ALLOW`.

**Scope of the gate.** It is a static check of the forms listed above, not a Python sandbox: the runtime assertions are the enforcement layer for every write that goes through the compat helpers (`dbutils.fs.*`, `safe_io`, `dbutils.secrets`), and Spark writes are covered by the AST rules plus the migrator's write-redirect interceptors. Not covered: direct OCI SDK calls (`import oci` is allowed because migrated notebooks use the API-key init pattern), `spark.read` of production data (reads are intended), and native Spark writers reached through objects the gate cannot see (a `DataFrameWriter` stored in a variable). `CREATE [OR REPLACE] [GLOBAL] TEMP VIEW` and plain `SELECT` are not writes. `dbutils.fs.mount(...)` keeps `extra_configs` in memory only (never in the process environment). Every refusal, allowed exception and sandbox declaration is appended to the policy log (`get_policy_log()`, `policy_log_markdown()`); `job_migrate.py` pulls it into each task's test report as **Notebook Policy Log**.

---

## 9. What NOT to Use in Migrated Notebooks

❌ **Direct JVM Hadoop FS calls** — `spark._jvm.org.apache.hadoop.fs.FileSystem.get(...)`, `fs.open(...)`, `fs.exists(...)`. These FAIL in scheduled workflow runs even though they work interactively.
**Use instead**: `dbutils.fs.*` (above) or OCI Python SDK directly.

❌ **`oci.auth.signers.get_resource_principals_signer()`** — Known failure modes on AIDP.
**Use instead**: API key auth (see top of this doc).

❌ **`boto3` for S3** — AWS SDK not configured on AIDP.
**Use instead**: `aidp_compat.s3_compat` for routing, or OCI SDK directly.

❌ **`dbutils.notebook.run(path, 0, ...)`** — AIDP rejects `timeout=0`.
**Use instead**: `oidlUtils.notebook.run(path, timeout=3600, ...)`.

---

## 10. Quick Smoke Test

After installing the wheel, run this in a notebook to verify install:

```python
import os
from aidp_compat import dbutils

OCI_BASE = "oci://<oci_backup_bucket>@<WORKSPACE_NAMESPACE>/aidp_compat_smoke"
# Declare the sandbox first: dbutils.fs writes refuse without it (section 8).
os.environ.setdefault("AIDP_SANDBOX_CATALOG", "default")
os.environ.setdefault("AIDP_SANDBOX_SCHEMA", "<sandbox_schema>")
os.environ.setdefault("AIDP_SANDBOX_PREFIX", OCI_BASE + "/")
dbutils.fs.put(f"{OCI_BASE}/hi.txt", "hello", overwrite=True)
print(dbutils.fs.head(f"{OCI_BASE}/hi.txt", 100))
dbutils.fs.rm(f"{OCI_BASE}/", recurse=True)
print("✓ smoke test passed")
```

---

## 11. Changelog (recent)

| Version | Date | Highlights |
|---|---|---|
| unreleased | 2026-10 | `notebook_policy` mandatory sandbox gate on `notebook.run` + runtime assertions in `fs`/`safe_io` writers; `secrets` Vault-only by default (plaintext opt-in, `0600` file check, scope allowlist) |
| **0.5.3** | 2026-05-20 | `cp/mv` OCI → OCI now waits for async `copy_object` work-request to COMPLETE |
| 0.5.2 | 2026-05-20 | `cp` OCI → OCI passes required `destination_region` to `copy_object` |
| 0.5.1 | 2026-05-20 | All `dbutils.fs.*` methods rewritten to use OCI Python SDK (API key) — works in workflow; `s3_compat`, `secrets` use API key auth; resource principal removed |
| 0.5.0 | (prior) | JVM Hadoop FS based; resource principal auth |
