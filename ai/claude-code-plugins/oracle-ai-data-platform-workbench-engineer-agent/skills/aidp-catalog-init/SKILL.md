---
name: aidp-catalog-init
description: One-time AIDP catalog discovery that writes a cached, version-controllable .aidp/catalog.md grounding file (tables, columns, FK/join hints, value dictionaries). Use when the user says "/aidp-catalog-init", asks to "map/discover my lakehouse", set up data discovery, or before answering data questions when no catalog cache exists. Re-run with --refresh when the schema changes.
---

# `aidp-catalog-init` — build the catalog grounding file

Walk the AIDP catalog tree and generate `.aidp/catalog.md` — the cached, user-editable grounding file that
makes subsequent NL-to-SQL fast and accurate (mirrors Astronomer's `warehouse-init` → `.astro/warehouse.md`).
Discovery is **pure control-plane** — **no SQL, no compute** (except optional `--with-counts`, which uses
the bundled SQL helper). Self-contained: **no aidp MCP required**.

## When to use
- First-time setup, or `--refresh` after schema changes.

## Engine — official `aidp` CLI (control-plane, no compute)
Preferred engine is the official Oracle `aidp` CLI; `oci raw-request` is the fallback when the CLI isn't
installed. Both hit the same data-plane REST API with the same auth — see
[references/aidp-cli-map.md](../../references/aidp-cli-map.md) for the full skill→command map and
[references/oci-raw-request.md](../../references/oci-raw-request.md) for base URL + auth ladder + conventions.

**CLI (preferred):**
```bash
# 1. catalogs
aidp catalog list --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>
# 2. schemas in a catalog
aidp schema list --catalog-key <cat> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>
# 3. tables in a schema (schema-key is the dotted <cat.schema>)
aidp schema list-tables --catalog-key <cat> --schema-key <cat.schema> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>
# single catalog/schema/table: aidp catalog get · aidp schema get · aidp schema get-table
```

**Fallback (no CLI installed) — `oci raw-request`** (LIVE-VERIFIED `20240831` / `dataLakes` /
`--profile DEFAULT` — see [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md)):
```bash
B="https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>"
oci raw-request --http-method GET --target-uri "$B/catalogs" --profile DEFAULT
oci raw-request --http-method GET --target-uri "$B/schemas?catalogKey=<cat>" --profile DEFAULT
oci raw-request --http-method GET --target-uri "$B/tables?catalogKey=<cat>&schemaKey=<cat.schema>" --profile DEFAULT
```

- **Single table / columns** — `aidp schema get-table` (or the REST `tables?…` list, which returns columns,
  types, and properties); filter to the one
  table client-side by its key (no dedicated single-table param confirmed — see no-mcp-rest-map.md).
- Per-endpoint params are **required**: a bare path returns `400 InvalidParameter: query param X must not
  be null`, which names the missing param.
- On `401/403`/"Security Token", follow the auth ladder (refresh `AIDP_SESSION`, retry with
  `--auth security_token`) in oci-raw-request.md.

## Process
1. **Walk the tree (no compute):** `aidp catalog list` → for each, `aidp schema list --catalog-key` → for
   each, `aidp schema list-tables --catalog-key --schema-key` (columns, types, properties) — or the REST
   fallback above. For large catalogs, fan out one subagent per catalog to parallelize discovery.
2. **Capture grounding hints (this is what raises NL-SQL accuracy):**
   - **FK/join hints** — infer likely join keys from naming (`*_sk`, `*_id`, shared column names) and any
     declared keys in the table properties. Record them so the agent doesn't guess joins later.
   - **Value dictionaries** — for low-cardinality categorical columns, note canonical values/format
     (prevents wrong WHERE literals like "California" vs "CA"). Pull distinct values only when cheap
     (`--with-counts` path), or mark TODO.
   - **Large-table flags** — flag big fact tables ("always filter by date").
3. **Enrich from the codebase** if present (existing notebooks, SQL files, CLAUDE.md) for descriptions.
4. **Write `.aidp/catalog.md`** with sections: *Quick Reference* (concept→table), *Catalogs → schemas →
   tables* (columns, types, join keys, flags), *Value dictionaries*, *Gotchas*. Preserve user edits + HTML
   comments on `--refresh`; flag removed tables with `<!-- REMOVED -->`.
5. **Summarize** to the user (N catalogs / schemas / tables, large tables flagged) and suggest next steps
   (`aidp-semantic-model` for metrics, `aidp-analyzing-data` to ask questions).

## Options
- `--refresh` — regenerate, preserving user edits and Quick-Reference rows.
- `--catalog <name>` — limit to one catalog.
- `--with-counts` — also fetch row counts / distinct values via the bundled SQL helper (uses the cluster,
  off by default — it costs compute and needs a running cluster):

  ```bash
  python "$PLUGIN_DIR/scripts/aidp_sql.py" --region <r> --datalake <DATALAKE_OCID> --workspace <ws> --cluster <key> \
    --code "spark.sql('SELECT COUNT(*) AS n FROM <cat>.<schema>.<table>').show()"
  ```

  Returns JSON with `status` / `outputs` / `spark_job_ids`; mints a UPST from the api_key DEFAULT profile and
  auto-creates a scratch notebook (no AIDP_SESSION required). See
  [references/oci-raw-request.md](../../references/oci-raw-request.md) for the control-plane side.

## Output format (`.aidp/catalog.md`)
```markdown
# AIDP catalog — generated <date> (edit freely)
## Quick Reference
| Concept | Table | Key |
|---|---|---|
| customers | default.default.customer | c_customer_sk |
## <catalog> → <schema>
#### <table>   (rows: <n if --with-counts>; LARGE if big)
| Column | Type | Notes (PK/FK/join) |
## Value dictionaries
## Gotchas
```

## Notes
- Resolve `<region>` / `<DATALAKE_OCID>` / `<workspace>` explicitly — catalog calls are scoped to the
  DataLake; the SQL helper is scoped to a workspace + cluster.
- `.aidp/` is git-ignored — it's a per-project cache, not shipped with the plugin.
- **Auto-Populate Catalog Extractor (bulk auto-cataloging from Object Storage) has a REST surface** at
  `…/dataLakes/<OCID>/extractors` (NOT `/metadataExtractors`, which 404s — an earlier note probed the wrong
  path). **LIVE-VERIFIED 2026-06-12:** `GET …/20240831/dataLakes/<OCID>/extractors` → **200** `{"items":[]}`.
  Surface: `GET/POST/DELETE /extractors`, `GET /extractors/<key>/extractedEntities`,
  `GET /extractors/<key>/extractedTables/<name>`, `POST /extractors/<key>/actions/manageExtractedEntities`
  (accept/reject/import), lifecycle `ACCEPTED→IN_PROGRESS→SUCCEEDED/FAILED/IN_REVIEW`. This complements (does
  not replace) the discovery walk above and `aidp-ingest-file-to-table`. Probe the create/manage write paths
  live (need an Object Storage source) before relying on them.
- The aidp MCP is an **optional accelerator** — if one is configured you may use `list_catalogs` /
  `list_schemas` / `list_tables` / `get_table` instead of the raw calls, but it is not required.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) — skill → official `aidp` CLI command map (primary engine)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) · [references/semantic-model.md](../../references/semantic-model.md)
