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

Workflow-based (the normal case — matches [`aidp-build-dag`](../aidp-build-dag/SKILL.md)'s workflow path):

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability_for_workflow.py \
  --config <customer>.yaml \
  --job-key <workflow-key> \
  --cluster <CLUSTER_ID> \
  [--manifest reports/<MyJob>_manifest.json] \
  [--only-tasks <substrings>] \
  [--output reports/<MyJob>_data_check.json] \
  [--bucket-mapping <path>]
```

`--job-key` is the **AIDP** job key (UUID); `--manifest` skips the API fetch when you already built one. `--config` supplies the AIDP coordinates (`--lake-ocid` / `--workspace-id` / `--oci-profile` are per-run overrides).

Or path-rooted, for a workspace folder:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py \
  --root "<workspace-root-path>" \
  --cluster <CLUSTER_ID> \
  [--bucket-mapping <path>] \
  [--lake-ocid <LAKE_OCID> --workspace-id <WORKSPACE_UUID> --oci-profile <profile>]
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
4. Emits a report with these classes:
   - **EXISTS** — table/path exists, accessible (row/item count printed)
   - **MISSING** (tables) / **NOT FOUND** (paths) — does not exist on the cluster
   - **ERROR** — the probe itself failed (access error, bad reference); investigate, don't assume present

   There is **no `EMPTY` class** — an existing-but-empty table reports as `EXISTS (0 rows)`; judge emptiness from the printed count.

## How to read the output

Sample shape:
```
Storage Paths (2)
  EXISTS (52 items)  oci://<bucket>@<ns>/path/to/file (used by: <task_a>)
  NOT FOUND          oci://<bucket>@<ns>/missing/path
                     Suggestions: oci://<bucket>@<ns>/other/path

Spark Tables (3)
  EXISTS (1234567 rows)  <schema>.<table_a>
  MISSING <schema>.<table_b>
  ERROR   <schema>.<table_c>: <probe error>

RESULT: ALL DATA AVAILABLE - ready to migrate     ← only when nothing is missing
RESULT: <n> data sources missing - check suggestions above
```

**MISSING / NOT FOUND** rows → Pass-2 will definitely fail at those cells. Options:
- Run [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) if the underlying *schema* is missing.
- Configure [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) if `s3://` → `oci://` rewrites haven't been done.
- Mark the table as "out of scope" in the manifest and migrate the consuming notebook with a stub upstream.

**ERROR** rows → the probe failed (permissions, malformed reference, cluster hiccup). Root-cause before migrating — an ERROR is not evidence the data exists.

**EXISTS (0 rows)** → Pass-2 may pass (no error) but produce empty downstream tables. This is the silent failure mode. Decide whether to:
- Backfill the source.
- Use the synthetic-data path (if your team has one).
- Accept and document.

## Reusing the bucket-mapping config

If the manifest references `s3://` paths, the scanner consults the S3→OCI bucket mapping CSV (`cfg.bucket_mapping.path`, default `config/bucket_map.csv`; per-run override `--bucket-mapping <path>`) to translate before probing. If the mapping is missing the bucket, the path cannot be translated and probes as `NOT FOUND` (with suggestions when a fuzzy match exists). Fix via [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) and re-run.

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

If `RESULT: ALL DATA AVAILABLE`: proceed to [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md).
If anything is MISSING / NOT FOUND: resolve via [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) or [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) and re-run this skill.
