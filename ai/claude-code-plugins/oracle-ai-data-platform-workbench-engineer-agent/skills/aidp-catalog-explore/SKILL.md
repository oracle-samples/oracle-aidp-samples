---
name: aidp-catalog-explore
description: Browse the AIDP data catalog live — list catalogs, schemas, tables, and volumes, inspect a table's columns/properties, and resolve a human name to its catalog key. Use when the user asks "what catalogs/schemas/tables exist", "describe table X", "what columns does Y have", or needs a catalog/schema/table key for another operation.
---

# `aidp-catalog-explore` — live catalog browsing

Browse and inspect the AIDP catalog directly via the control-plane — no SQL, no compute, no MCP required.
Complements `aidp-catalog-init`: use this for ad-hoc, live lookups; use `catalog-init` to build the cached
grounding file.

## When to use
- "What catalogs/schemas/tables are there?" · "describe / show columns of <table>" · "find tables about X"
- You need a catalog/schema/table **key** to feed another skill.

## Engine — official `aidp` CLI (control-plane, read-only)
Preferred engine is the official Oracle `aidp` CLI; `oci raw-request` is the fallback when the CLI isn't
installed. Both hit the same data-plane REST API with the same auth — see
[references/aidp-cli-map.md](../../references/aidp-cli-map.md) for the full command map and
[references/oci-raw-request.md](../../references/oci-raw-request.md) for base URL + auth ladder.

| Goal | CLI (preferred) | REST fallback (verified) |
|---|---|---|
| List catalogs | `aidp catalog list` | `GET /catalogs` |
| Describe a catalog | `aidp catalog get --catalog-key <cat>` | (in list output) |
| List schemas in a catalog | `aidp schema list --catalog-key <cat>` | `GET /schemas?catalogKey=<cat>` |
| List tables in a schema | `aidp schema list-tables --catalog-key <cat> --schema-key <cat.schema>` | `GET /tables?catalogKey=<cat>&schemaKey=<cat.schema>` |
| Describe a single table | `aidp schema get-table --catalog-key <cat> --schema-key <cat.schema> --table-key <t>` | `GET /tables?…` then filter by table key client-side |
| List volumes | `aidp volume list --catalog-key <cat>` | `GET /volumes?catalogKey=<cat>` (param shape TBD — if `400`, the error names the required param) |

All CLI calls take `--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`.

```bash
# CLI (preferred): list tables in cat.schema
aidp schema list-tables --catalog-key default --schema-key default.default \
  --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1

# Fallback (no CLI installed): oci raw-request, --profile DEFAULT (api_key)
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/tables?catalogKey=default&schemaKey=default.default" \
  --profile DEFAULT
```

On `401/403/"Security Token"` follow the auth ladder. Per-endpoint REST params are **required** — a bare
path returns `400 InvalidParameter: query param X must not be null`, which names the missing param. REST
output is JSON: `{ "data": …, "headers": …, "status": <int> }`.

## Patterns
- **Browse down the tree:** `aidp catalog list` → `aidp schema list --catalog-key <cat>` →
  `aidp schema list-tables --catalog-key <cat> --schema-key <cat.schema>` → `aidp schema get-table` (or the
  REST fallback `GET /catalogs` → `GET /schemas?catalogKey=` → `GET /tables?…` → filter for the table).
- **Describe a table:** list tables for the schema, then filter the response by table key; present columns,
  types, properties, and any declared keys from that table object.
- **Find by concept:** check `.aidp/catalog.md` Quick Reference first (instant); fall back to scanning
  `/tables` names + table objects for the concept.
- **Resolve name→key:** downstream skills need keys, not display names — use these list calls to translate.

## Notes
- Workspace/DataLake-scoped — confirm the region + DataLake OCID first.
- Prefer the cached `.aidp/catalog.md` for repeated lookups; this skill is for live/uncached inspection or
  when the cache may be stale (then suggest `aidp-catalog-init --refresh`).
- If an `aidp` MCP is configured, its `list_catalogs` / `list_schemas` / `list_tables` / `get_table` /
  `list_volumes` tools are an optional accelerator — not required.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) — skill → official `aidp` CLI command map (primary engine)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md)
