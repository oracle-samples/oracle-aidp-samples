---
name: aidp-bucket-mapping
description: Configure the s3:// → oci:// bucket / namespace mapping the migrator uses to rewrite paths during notebook + catalog migration. Use when (a) the user has external tables / files at s3:// paths that need to land on OCI Object Storage, OR (b) check_data_availability reports s3:// paths as NOT FOUND because the bucket isn't mapped, OR (c) the DDL rewriter logs an R02_BUCKET_UNMAPPED warning.
---

# `aidp-bucket-mapping` — wire up `s3://` ↔ `oci://`

Several places in the migrator look up `s3://<bucket>/<path>` and rewrite to `oci://<bucket>@<namespace>/<path>`:
- `aidp-migrate-catalog` rewrites external-table `LOCATION` clauses.
- `aidp-migrate-job` Pass-1 rewrites `spark.read.parquet("s3://...")` literals in notebook cells.
- `aidp-check-data` probes paths via the same translation.

All consult the same bucket-mapping config.

## When to use

- Setting up the migrator on a new workstation / new tenancy combo.
- Any time a tool reports an unmapped S3 bucket (an `R02_BUCKET_UNMAPPED` warning from the DDL rewriter, or a data-check probe that can't translate an `s3://` path).
- After provisioning a new OCI Object Storage bucket that mirrors a Databricks-side S3 bucket.

## The config file

The migrator loads bucket mappings via the `load_bucket_mapping()` helper (`engine/scripts/context_tools.py`). The customer supplies a **3-column CSV** — NOT JSON:

```csv
s3_bucket,oci_bucket,oci_namespace
<s3-bucket-name>,<oci-bucket-name>,<oci-namespace>
<another-s3-bucket>,<another-oci-bucket>,<oci-namespace>
```

| Column | Meaning |
|---|---|
| `s3_bucket` | Source S3 bucket name (no `s3://` prefix). |
| `oci_bucket` | Target OCI Object Storage bucket. |
| `oci_namespace` | OCI tenancy namespace (NOT the DataLake namespace — these can differ). |

The path comes from `bucket_mapping.path` in the customer YAML (`--config`); `--bucket-mapping <path>` is the per-run override, and the legacy fallback is `config/bucket_map.csv` in the engine. `migrate_catalog.py` takes the same CSV via `--bucket-map <path>`. Save the file to a path the user controls (gitignored — it contains tenancy-specific identifiers).

## Building the mapping for a new tenancy

If the user is doing this for the first time:

1. **List the source S3 buckets referenced in the Databricks workspace.** Quick way:
   ```bash
   # On the migrator repo:
   grep -roE 's3://[a-z0-9.-]+' <source-databricks-checkout>/ | sort -u
   ```
   Or use the migrator's prep helper if available.

2. **For each, identify the target OCI bucket + namespace.** Either:
   - The user provisions matching OCI buckets (recommended for big migrations — preserves bucket names).
   - The user routes everything into a single shared OCI bucket with prefix isolation.

3. **Find the OCI namespace.** Each OCI tenancy has ONE namespace per region — find it via:
   ```bash
   oci os ns get --profile <profile>
   # returns: {"data": "<your-namespace>"}
   ```

4. **Write the CSV** and point `bucket_mapping.path` in the customer YAML at it (the engine's legacy fallback location is `config/bucket_map.csv`). Make sure it's gitignored.

5. **Test the mapping** by re-running [`aidp-check-data`](../aidp-check-data/SKILL.md) with `--bucket-mapping <path>`. Any `MISSING` entries with `s3://...` paths now resolve to `oci://...` and probe correctly.

## What actually happens on an unknown bucket (verified against the code)

Behavior contract — unknown buckets are **surfaced, not rejected**:
- `migrate_catalog.py` does NOT reject an unknown bucket: the rewriter leaves the original `s3://` LOCATION **unchanged** in the emitted DDL and records an `R02_BUCKET_UNMAPPED` warning in the rule-application audit (visible in `--out-report` / `--dry-run` output). Review every such warning — a replayed table pointing at `s3://` will fail at first read on AIDP.
- `job_migrate.py` Pass-1/2 surfaces the bucket mapping to the model's fix context so it can translate paths or ask; an untranslatable path fails at execute time, loudly.
- `check_data_availability*` can't translate the path, so the probe reports it `NOT FOUND` (with fuzzy suggestions when a near-match exists).

If you see `R02_BUCKET_UNMAPPED` warnings but the replay continues, the DDL kept the untranslated `s3://` path — confirm the consumer notebook either no longer reads that path OR add the mapping and re-run.

## Common mistakes

| Mistake | Fix |
|---|---|
| Confusing DataLake namespace vs OCI tenancy namespace | These can differ. `oci_namespace` in the mapping is the TENANCY namespace (`oci os ns get`), not the DataLake's internal namespace. |
| Hardcoding bucket names that include `s3://` prefix | Don't include `s3://` — just the bucket name. |
| Forgetting to pass `--bucket-mapping <path>` to subsequent invocations | The path is per-run, not persisted. Add it to your `aidp-migrate-job` / `aidp-migrate-catalog` invocations. |
| Listing buckets the user doesn't actually have read access to | The mapping resolves the name; access errors surface at first read. Don't pre-mock buckets the user can't touch. |

## After this

- Re-run [`aidp-check-data`](../aidp-check-data/SKILL.md) — any `MISSING` rows that were due to bucket-map issues should now be `OK`.
- Proceed to [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) / [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md).
