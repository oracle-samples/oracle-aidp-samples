---
description: Guided catalog migration flow. Extract Databricks Unity Catalog / HMS metadata, preview the 18-rule DDL rewrite, then batched replay on AIDP. Asks before destructive replay step.
---

# `/migrate-catalog` — guided catalog migration

Walk the user through extracting Unity Catalog / HMS metadata from Databricks, previewing the rewritten DDL, and replaying it on AIDP.

## Workflow

1. **Confirm prereqs** — invoke [`aidp-migrator-bootstrap`](../skills/aidp-migrator-bootstrap/SKILL.md). Especially check `DATABRICKS_HOST` + `DATABRICKS_TOKEN` are set (catalog extract needs them).
2. **Confirm scope** — ask which catalogs / schemas the user wants to migrate. Default to "everything in this catalog" but accept a filter list.
3. **Confirm bucket mapping** — if any external tables have `s3://` locations, the bucket-map config must exist. Route to [`aidp-bucket-mapping`](../skills/aidp-bucket-mapping/SKILL.md) if missing.
4. **Stage 1: extract** — `extract_catalog_databricks.py` → `reports/catalog_pack.json`. Show the table count.
5. **Stage 2 dry-run: rewrite preview** — `migrate_catalog.py --dry-run`. Surface:
   - Total CREATE SCHEMA statements
   - Total CREATE TABLE statements
   - Any rejections (MV / streaming / unsupported)
   - Any bucket-map misses
   Ask the user "ready to replay on the cluster?"
6. **Stage 2 replay** — `migrate_catalog.py` (no `--dry-run`). Surface per-chunk status.
7. **Verify** — for each migrated schema, run `SHOW TABLES IN default.<schema>` and surface the count. Compare to extract.

## Args

$ARGUMENTS

If `$ARGUMENTS` provides `<catalog>` or `<catalog>:<schema>` filters, use them; else ask.

## Checkpoints

```
[Phase 1/4] Extracted N catalogs, M schemas, K tables → reports/catalog_pack.json
[Phase 2/4] Dry-run: would create 23 schemas + 412 tables. Rejected: 3 MVs, 1 streaming table.
            About to run live DDL replay — proceed? (y/N)
[Phase 3/4] Replayed in 4 chunks of 25 statements each. All chunks committed.
[Phase 4/4] Verify: SHOW TABLES across each schema. Discrepancies: 0
```

## When to stop

- User aborts at the dry-run checkpoint.
- Bucket-map is missing buckets — fix first via [`aidp-bucket-mapping`](../skills/aidp-bucket-mapping/SKILL.md).
- Dry-run shows >10% rejected (MVs / streaming) — likely a structural mismatch; review with the user before proceeding.

## After this

- Verify with [`aidp-check-data`](../skills/aidp-check-data/SKILL.md) — schemas + tables should now resolve.
- Proceed to [`/migrate-job`](./migrate-job.md) for the notebook layer.
