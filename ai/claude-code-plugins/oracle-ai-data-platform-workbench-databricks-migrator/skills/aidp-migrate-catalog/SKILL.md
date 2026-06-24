---
name: aidp-migrate-catalog
description: Migrate Databricks Unity Catalog / HMS schemas + tables onto AIDP. Two-stage extract→rewrite→replay pipeline. The rewriter applies 18 DDL rules (3-part→2-part flatten, s3://→oci:// via bucket-map, source-format preserved (Delta stays Delta) with delta.* property scrub, MV/streaming rejection, CREATE SCHEMA COMMENT-colon strip). Batched single-WebSocket DDL replay works around AIDP's per-statement-discard quirk. Use BEFORE aidp-migrate-job — schemas must exist before notebook reads.
---

# `aidp-migrate-catalog` — Unity Catalog / HMS → AIDP DDL migration

The notebook migration (Pass-2) reads tables. If the schemas + tables don't exist on AIDP first, every read fails. This skill ports the metadata layer.

## When to use

- BEFORE [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) for the first time on a new target environment.
- After the user adds new schemas/tables to the source Databricks workspace.
- After a [`aidp-check-data`](../aidp-check-data/SKILL.md) run reports many `MISSING` entries.

## Two-stage pipeline

```
┌─────────────────────────────────┐    ┌─────────────────────────────────┐
│ Stage 1: extract_catalog_       │    │ Stage 2: migrate_catalog.py     │
│           databricks.py         │ → │  reads catalog_pack.json,        │
│  REST against Unity Catalog API │    │  applies 18 DDL rewrite rules,  │
│  reports/catalog_pack.json      │    │  batches into ONE WS execute    │
│                                 │    │  on the AIDP cluster            │
└─────────────────────────────────┘    └─────────────────────────────────┘
```

## Stage 1 — extract from Databricks

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/extract_catalog_databricks.py \
  --catalogs "<catalog_a>,<catalog_b>" \
  --schemas-only "<catalog_a>:<schema_1>,<catalog_a>:<schema_2>" \
  --out reports/catalog_pack.json
```

| Flag | Purpose |
|---|---|
| `--catalogs` | Comma-separated catalog names to extract. Use the source catalog names (e.g. the Unity Catalog you migrate from). |
| `--schemas-only` | Optional filter — only extract these specific schemas. Format: `<catalog>:<schema>,<catalog>:<schema>`. Without it, ALL schemas in each catalog are extracted. |
| `--out` | Output JSON path. The next stage reads this. |

Env required:
- `DATABRICKS_HOST` — `https://<workspace>.cloud.databricks.com`
- `DATABRICKS_TOKEN` — PAT with workspace + catalog read.

The extractor uses Unity Catalog's REST API (`/api/2.1/unity-catalog/tables` with `include_delta_metadata=true`) and includes exponential backoff for rate limits. Expect 1-5 min per catalog depending on table count.

### Pack JSON shape

```json
{
  "catalogs": [
    {
      "name": "<source_catalog>",
      "schemas": [
        {
          "name": "<schema>",
          "tables": [
            {
              "name": "<table>",
              "table_type": "MANAGED",
              "data_source_format": "DELTA",
              "storage_location": "s3://<bucket>/<path>",
              "columns": [{"name": "id", "type_text": "long", "nullable": true}, ...],
              "properties": {"delta.minReaderVersion": "2", ...},
              "comment": null,
              "partition_columns": ["date"]
            }
          ]
        }
      ]
    }
  ]
}
```

## Stage 2 — rewrite + replay on AIDP

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/migrate_catalog.py \
  --pack reports/catalog_pack.json \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --output-base <output-workspace-path> \
  --oci-profile <profile>
```

Useful flags:
| Flag | Purpose |
|---|---|
| `--dry-run` | Print the rewritten DDL but do NOT execute. Use first time to inspect. |
| `--chunk-size N` | Statements per WS execute batch (default 25). Reduce if you hit timeout. |
| `--catalogs <list>` | Migrate only these catalogs from the pack. |
| `--schemas <list>` | Migrate only these schemas. Format `<catalog>:<schema>`. |

## What the 18 DDL rewrite rules do

Full details in [references/ddl-rewrite-rules.md](../../references/ddl-rewrite-rules.md). Quick summary:

| Rule | Input → Output |
|---|---|
| 3-part name flatten | `<src_cat>.<schema>.<table>` → `<schema>.<table>` (AIDP defaults to single-catalog `default`) |
| `s3://` → `oci://` rewrite | `s3://<bucket>/<path>` → `oci://<bucket>@<namespace>/<path>` via bucket-map |
| Source format preserved | Default `--target-using` is None → Delta stays Delta. AIDP supports Delta natively. Pass `--target-using parquet` to deliberately convert. |
| `delta.*` property strip | All `delta.minReaderVersion`, `delta.minWriterVersion`, etc. dropped |
| `spark.sql.*` property strip | Reserved cluster-level configs, can't be set in DDL |
| `pipelines.*` property strip | DLT-specific, not applicable |
| `view.query.*` property strip | UC-specific view properties |
| `unity.*` property strip | UC-specific |
| MV rejection | `MATERIALIZED VIEW` not supported on AIDP — DDL skipped + flagged |
| Streaming table rejection | `STREAMING TABLE` not supported — DDL skipped + flagged |
| External-table `LOCATION` rewrite | Rewritten via the same `s3://`→`oci://` bucket-map |
| `CREATE SCHEMA … COMMENT '<text>'` colon strip | AIDP silently nukes a schema if its COMMENT contains a `:` — strip the COMMENT clause entirely. |
| etc. (full list in references) | |

## The batched-DDL workaround (important)

AIDP's Spark WS execute has a quirk: per-statement DDL is discarded if the session closes between statements. Workaround: batch all `CREATE SCHEMA` + `CREATE TABLE` (in dependency order) into ONE WS execute call. `migrate_catalog.py` does this automatically via `--chunk-size`.

This means if a single statement in a chunk fails, the WHOLE chunk's reported status may say success even though some tables aren't created. The migrator therefore probes existence post-execute for every statement and reports per-statement results.

## Verify after running

After the migration, verify schemas + tables actually landed via [`aidp-check-data`](../aidp-check-data/SKILL.md). Specifically:

```python
spark.sql("SHOW SCHEMAS IN default").show(100, False)
spark.sql("SHOW TABLES IN default.<schema>").show(100, False)
spark.sql("DESCRIBE TABLE default.<schema>.<table>").show(100, False)
```

`DESCRIBE TABLE` confirms (a) the table exists and (b) the column types are what you expect.

## What this skill does NOT migrate

- **Row-level data.** This is metadata only. To get rows into AIDP, use a separate copy (CTAS from external location, or `oci os copy`).
- **Permissions / grants.** UC grants don't map to AIDP's permission model. Re-apply manually.
- **Stored procedures / user-defined functions.** UC functions / SQL UDFs need separate porting.
- **Views.** Simple views work via the same rewriter. Complex views with cross-catalog refs need manual review.
- **Materialized views / streaming tables.** Rejected by the rewriter — flagged in the output for human re-implementation.

## After this

- [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) if any tables still have `s3://` locations the bucket-map didn't cover.
- [`aidp-check-data`](../aidp-check-data/SKILL.md) to verify everything landed.
- Then [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) for the notebook layer.
