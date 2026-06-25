# Databricks → AIDP Gotchas

15 platform differences the migrator handles, with the exact recipe for each. The migrator applies most automatically; this reference is here so Claude can route failed-cell scenarios to the correct fix and so the user can review which fixes are being applied.

---

## Gotcha #1 — AWS-Secrets-Manager-backed decrypt UDFs

**What breaks:** notebooks reference a UDF backed by AWS Secrets Manager. AIDP has no equivalent.

**Fix recipe:**
- If the UDF is decorative (its output isn't used downstream), comment out the call.
- If output is used, replace with a passthrough stub via `synthetic_overrides.toml`:
  ```toml
  [[mock_udf]]
  name = "<legacy_secret_udf>"
  strategy = "passthrough"
  return_type = "string"
  ```
- The migrator's Pass-2 detects the UDF + injects the stub if registered.

---

## Gotcha #2 — `/Volumes` FUSE write-then-read consistency delay

**What breaks:** code does `joblib.dump(...)` to a Volume path, then immediately `joblib.load(...)` the same path → `FileNotFoundError`.

**Fix recipe:**
- Wrap with `aidp_compat.safe_*` helpers if present.
- Or insert a `time.sleep(5)` between write and read (rough — adjust based on file size).
- For streaming checkpoints, ensure `checkpointLocation` is in a directory the streaming task is solely responsible for.

---

## Gotcha #3 — `from pyspark.sql.functions import *` shadows `builtins.sum`

**What breaks:** `total = sum(my_list)` after the wildcard import dispatches to `pyspark.sql.functions.sum` which expects a Column. Cells crash with `TypeError: Column not iterable` or similar.

**Fix recipe:**
- Migrator rewrites Python `sum(...)` to `builtins.sum(...)` when it detects a wildcard import upstream.
- Alternative: change the import to `import pyspark.sql.functions as F` and qualify every use as `F.col`, `F.sum`, etc.

---

## Gotcha #4 — No outbound internet from cluster (`pip install` fails)

**What breaks:** cell calls `!pip install <pkg>` — fails because the cluster has no outbound HTTPS.

**Fix recipe:**
- Replace `!pip install` cells with `# AIDP: installed via cluster libraries API` (migrator does this automatically).
- Library must be pre-installed on the cluster via the AIDP cluster-libraries REST API BEFORE migration.
- Common missing packages from Databricks-DBR-bundled lists: `omegaconf`, `mlflow`, certain `hyperopt` versions.

---

## Gotcha #5 — Per-statement DDL discarded on WebSocket session close

**What breaks:** running `CREATE TABLE` in one `nb_execute_code` call, then `SHOW TABLES` in another — the table doesn't appear. The DDL never persisted.

**Fix recipe:**
- Batch all `CREATE SCHEMA` / `CREATE TABLE` into ONE WS execute call.
- The catalog migrator's `migrate_catalog.py` does this automatically via `--chunk-size 25`.
- For ad-hoc DDL, wrap in a single multi-statement Python cell:
  ```python
  for stmt in [...DDL list...]:
      spark.sql(stmt)
  ```

---

## Gotcha #6 — `CREATE SCHEMA … COMMENT '<text with colon>'` silently nukes the schema

**What breaks:** `CREATE SCHEMA <s> COMMENT 'team:platform'` — schema is NOT created, no error returned. `SHOW SCHEMAS` doesn't list it.

**Fix recipe:**
- Migrator's catalog rewriter (rule #14) ALWAYS strips the COMMENT clause from CREATE SCHEMA.
- For descriptions, set them post-create via `ALTER SCHEMA <s> SET DBPROPERTIES ('description' = '<text>')` — that path doesn't have the colon bug.

---

## Gotcha #7 — `%run Imports/` with trailing slash isn't resolved

**What breaks:** `%run //Workspace/.../Imports/` (Databricks-permissive trailing slash) — AIDP returns "notebook not found".

**Fix recipe:**
- Migrator strips trailing slash automatically during the line-magic rewrite.
- Same gotcha for `dbutils.notebook.run("./helpers/")` — strip the trailing slash.

---

## Gotcha #8 — Session tokens (`AIDP_SESSION`) expire ~1 hr mid-run

**What breaks:** a long-running migration (>1 hr) under an `oci session authenticate` profile dies mid-cell with `Security Token expired`.

**Fix recipe:**
- Use an `api_key` profile for unattended migrations — they don't expire.
- For interactive work, `oci session refresh --profile <profile>` periodically.
- The migrator's executor tracks session age and warns at 50 min remaining.

---

## Gotcha #9 — `omegaconf` (and other DBR-bundled packages) missing on AIDP

**What breaks:** `from omegaconf import ListConfig` → `ModuleNotFoundError`. Databricks Runtime bundles many small-but-not-PyPI-default packages; AIDP's runtime doesn't include all of them.

**Fix recipe:**
- Install via cluster libraries API:
  ```bash
  curl -X POST <AIDP_BASE>/.../clusters/<id>/libraries \
    -d '{"libraries":[{"pypi":{"package":"omegaconf"}}]}'
  ```
- Cluster restart required for some libs. Verify with `pip list` on the cluster after restart.
- For one-off cells that use a missing package, the migrator wraps in try/except and inline-stubs the missing functions.

---

## Gotcha #10 — `<param_lookup_helper>` undefined when `<parameters_stub>.ipynb` reduces to a stub

**What breaks:** Pass-1 dep migration reduces a parameter-bootstrap notebook to a stub (e.g. just imports). Downstream cells call `<param_lookup_helper>(...)` → `NameError`.

**Fix recipe:**
- Migrator inlines a `<param_lookup_helper>` helper at the cell USE site (not at import time) when it detects the call without a corresponding definition.
- Manual fix: prepend a code cell that defines `<param_lookup_helper>` from `dbutils.widgets.get` semantics.

---

## Gotcha #11 — `joblib.load` on missing pickle file

**What breaks:** `joblib.load("/Volumes/.../missing.pkl")` — file genuinely doesn't exist (artifact wasn't replicated).

**Fix recipe:**
- Wrap in try/except, set the var to an empty default so downstream cells continue:
  ```python
  try:
      cfg = joblib.load("/Volumes/.../config.pkl")
  except FileNotFoundError:
      cfg = {}  # placeholder
  ```
- Long-term: replicate the source artifact to AIDP Object Storage and update the path.

---

## Gotcha #12 — `context_json["tags"]["taskKey"]` KeyError

**What breaks:** `dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()` on AIDP doesn't include a `tags` key (Databricks always does). Direct access raises KeyError.

**Fix recipe:**
- Migrator rewrites to:
  ```python
  context_json.get("tags", {}).get("taskKey", "unknown_task")
  ```

---

## Gotcha #13 — `%scala` cells using `session.execute` (Cassandra/Scylla)

**What breaks:** Scala cells that reference `com.datastax.oss.driver.api.core.CqlSession` — driver not installed on the AIDP cluster.

**Fix recipe:**
- Migrator replaces the cell body with a print stub:
  ```scala
  // AIDP: skipped — Scylla/Cassandra driver not available
  print("AIDP: skipped Cassandra call")
  ```
- For real migration: stand up an OCI-NoSQL or alternative store and re-author the call.

---

## Gotcha #14 — `<source-bucket>` not in OCI bucket mapping

**What breaks:** notebook reads `s3://<source-bucket>/...` but bucket-mapping config doesn't know `<source-bucket>`. Migrator surfaces "S3 bucket X not found in OCI bucket mapping. Known buckets: [...]".

**Fix recipe:**
- Add the bucket → namespace entry to `bucket_mapping.json` (see [`aidp-bucket-mapping`](../skills/aidp-bucket-mapping/SKILL.md)).
- OR explicitly mark the path "out of scope" by wrapping reads in try/except for tests.

---

## Gotcha #15 — `<base_table_var>` NameError when `<parameters_stub>.ipynb` stub doesn't define it

**What breaks:** same family as Gotcha #10. The source parameter notebook defines `<base_table_var>`, but the migrator's stub-reduction dropped that line.

**Fix recipe:**
- Migrator detects the cell-level NameError + inlines:
  ```python
  <base_table_var> = locals().get('<base_table_var>', '<sandbox_schema>.<base_table>')
  ```
- Manual fix: prepend a cell that defines all expected globals before the consuming cell.

---

## Quick router — error message → gotcha

| Error message contains | Likely gotcha |
|---|---|
| `Column not iterable` / `Column object is not callable` after wildcard import | #3 |
| `ModuleNotFoundError: No module named '<pkg>'` | #4 / #9 |
| `Security Token expired` / `KeyError: 'tenancy'` | #8 |
| `TABLE_OR_VIEW_NOT_FOUND` after a fresh CREATE | #5 |
| `Schema not found` after `CREATE SCHEMA` succeeded | #6 |
| `notebook not found` for a `%run` target | #7 |
| `KeyError: 'tags'` | #12 |
| `FileNotFoundError: /Volumes/...` immediately after a write | #2 |
| `FileNotFoundError: /Volumes/.../*.pkl` | #11 |
| `NameError: name '<var>' is not defined` in a parameter context | #10 / #15 |
| `S3 bucket "<name>" not found in OCI bucket mapping` | #14 |
| `object datastax is not a member of package com` | #13 |
| `<legacy_secret_udf> is not defined` / `AWS Secrets Manager` | #1 |
