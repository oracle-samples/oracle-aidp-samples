---
name: migrate-catalog-flow
description: Guided catalog migration flow. Extracts Databricks Unity Catalog / HMS metadata, previews the 18-rule DDL rewrite, then batched replay on AIDP. Asks before the destructive replay step. Use when the user wants to migrate the catalog layer (schemas + tables) with checkpoints, rather than running aidp-migrate-catalog directly.
---

# `migrate-catalog-flow` — guided catalog migration

Walk the user through extracting Unity Catalog / HMS metadata from Databricks, previewing the rewritten DDL, and replaying it on AIDP.

## When to use

- User says "migrate the catalog", "port the schemas", "migrate Unity Catalog DDL".
- User wants the guided extract → dry-run → confirm → replay flow with stop points.
- BEFORE [`migrate-job-flow`](../migrate-job-flow/SKILL.md) — schemas + tables must exist on AIDP first so notebook reads have targets.

For a one-shot non-interactive run, prefer [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) directly.

## Workflow

1. **Confirm prereqs** — invoke [`aidp-migrator-bootstrap`](../aidp-migrator-bootstrap/SKILL.md). Especially check `DATABRICKS_HOST` + `DATABRICKS_TOKEN` are set (catalog extract needs them).
2. **Confirm scope** — ask which catalogs / schemas the user wants to migrate. Default to "everything in this catalog" but accept a filter list.
3. **Confirm bucket mapping** — if any external tables have `s3://` locations, the bucket-map config must exist. Route to [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) if missing.
4. **Stage 1: extract** — `extract_catalog_databricks.py` → `reports/catalog_pack.json`. Show the table count.
5. **Stage 2 dry-run: rewrite preview** — `migrate_catalog.py --dry-run`. Surface:
   - Total CREATE SCHEMA statements
   - Total CREATE TABLE statements
   - Any rejections (materialized views, streaming, unsupported)
   - Any bucket-map misses

   Ask the user "ready to replay on the cluster?"
6. **Stage 2 replay** — `migrate_catalog.py` (no `--dry-run`). Surface per-chunk status.
7. **Verify** — for each migrated schema, run `SHOW TABLES IN default.<schema>` and surface the count. Compare to extract.

## Args

If the user supplied `<catalog>` or `<catalog>:<schema>` filters in their prompt, use them; else ask in step 2.

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
- Bucket-map is missing buckets — fix first via [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md).
- Dry-run shows >10% rejected (MVs / streaming) — likely a structural mismatch; review with the user before proceeding.

## After this

- Verify with [`aidp-check-data`](../aidp-check-data/SKILL.md) — schemas + tables should now resolve.
- Proceed to [`migrate-job-flow`](../migrate-job-flow/SKILL.md) for the notebook layer.
