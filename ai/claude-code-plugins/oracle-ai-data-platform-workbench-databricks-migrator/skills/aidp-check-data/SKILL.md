---
name: aidp-check-data
description: Pre-migration data-availability scan. Reads every notebook in a migration manifest, extracts every spark.read.table / spark.read.parquet / saveAsTable reference, and probes whether each target schema/table/path exists on the AIDP cluster BEFORE you spend Pass-2 cluster time. Use after aidp-build-dag and before aidp-migrate-job, especially the first time you migrate against a target environment.
---

# `aidp-check-data` — pre-migration data-availability scan

Pass-2 of the migrator is expensive (live cluster time + Claude-with-tool-use tokens per cell). Running this scan first catches the "no source table" and "wrong bucket" failure modes in seconds instead of hours.

## When to use

- After [`aidp-build-dag`](../aidp-build-dag/SKILL.md), before [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md).
- After [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) (verify schemas + tables actually landed).
- Any time the user wonders "is the data ready".

## Invocation

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py \
  --root "<databricks-workspace-path>" \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --oci-profile <profile>
```

Or for the workflow-shape input (matches [`aidp-build-dag`](../aidp-build-dag/SKILL.md)'s workflow path):

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability_for_workflow.py \
  --job-id <databricks-job-id> \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --oci-profile <profile>
```

## What it does

1. Walks every notebook in the manifest.
2. Extracts every reference to:
   - `spark.read.table("...")` / `spark.table("...")`
   - `spark.read.parquet/csv/json/delta("...")`
   - `.saveAsTable("...")` (target — wrote-to)
   - 3-part name references in `%sql` / `spark.sql(...)` strings
3. For each unique reference, opens a Spark session on the cluster and runs a probe:
   - tables → `DESCRIBE TABLE <fq>` (and `SHOW TABLES IN <schema>` to differentiate "schema missing" from "table missing")
   - paths → `dbutils.fs.ls(path)` via the migrator's helper
4. Emits a report with three columns:
   - **OK** — table/path exists, accessible
   - **MISSING** — does not exist on the cluster
   - **EMPTY** — exists but has 0 rows / 0 files (often a sign that the catalog migration succeeded but data wasn't replicated)

## How to read the output

Sample shape:
```
== check_data_availability_for_workflow report ==
TABLES
   OK      <catalog>.<schema>.<table_a>             1234567 rows
   MISSING <catalog>.<schema>.<table_b>             -- DESCRIBE failed: SCHEMA_OR_TABLE_NOT_FOUND
   EMPTY   <catalog>.<schema>.<table_c>             0 rows

PATHS
   OK      oci://<bucket>@<ns>/path/to/file         52 objects
   MISSING oci://<bucket>@<ns>/missing/path         -- listObjects 404
```

**MISSING** rows → Pass-2 will definitely fail at those cells. Options:
- Run [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) if the underlying *schema* is missing.
- Configure [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) if `s3://` → `oci://` rewrites haven't been done.
- Mark the table as "out of scope" in the manifest and migrate the consuming notebook with a stub upstream.

**EMPTY** rows → Pass-2 may pass (no error) but produce empty downstream tables. This is the silent failure mode. Decide whether to:
- Backfill the source.
- Use the synthetic-data path (if your team has one).
- Accept and document.

## Reusing the bucket-mapping config

If the manifest references `s3://` paths, the scanner also consults `<migrator-repo>/config/oci_bucket_tenancy_mapping.json` (or whatever your bucket mapping helper resolves) to translate before probing. If the mapping is missing the bucket, the scanner reports a clear `S3 bucket X not found in OCI bucket mapping`. Fix via [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) and re-run.

## Performance + cost

- Each table probe is a small `DESCRIBE` — sub-second on a warm cluster.
- Each path probe is a `listObjects` against OCI Object Storage — also fast.
- Total scan time scales linearly with unique references; expect <2 min for a workflow with 50 notebooks.
- No Claude tokens spent — this is pure REST + Spark.

## Gotchas

- **2-part vs 3-part name resolution** — if the source code uses `schema.table` (no catalog), the scanner resolves against the cluster's current catalog (`default`). If the user expects a non-default catalog, surface that mismatch.
- **Cluster must be Active.** A `Stopped` cluster will make every probe fail with a connection error — instruct the user to start the cluster first.
- **CTEs and computed names**: the regex extractor catches static names, not names built at runtime via f-strings. False negatives are possible — review notebooks that consume dynamic table names manually.

## After this

If everything is OK: proceed to [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md).
If anything is MISSING: resolve via [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) or [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) and re-run this skill.
