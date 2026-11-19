---
name: aidp-table-management
description: Control-plane lifecycle for AIDP catalog objects — create/update/delete/refresh tables, views, schemas, and catalogs, and register an EXTERNAL catalog/connection (ALH/ADW/object-storage) as a persistent catalog object. Use when the user wants to create or drop a table/view/schema/catalog via the control plane (not a SELECT), register an external data source as a catalog, refresh catalog metadata, or test a connection. For SQL-native CREATE/ALTER/DROP use aidp-sql-ddl; for file→table ingestion use aidp-ingest-file-to-table; for browsing use aidp-catalog-explore.
---

# `aidp-table-management` — catalog/schema/table/view lifecycle + external-catalog registration

Manage the *metadata objects* of the AIDP catalog through the control plane. Two equivalent paths for table
DDL — the **SQL-native** path (`aidp-sql-ddl`, `CREATE/ALTER/DROP …`) and this **control-plane** path
(CLI/REST `create-table`/`create-view`/`create catalog`) — plus the one thing only the control plane does:
**registering an external catalog/connection** as a persistent object.

> **Engine precedence:** official **`aidp` CLI** `catalog`/`schema` groups (preferred) → **`oci raw-request`**
> REST fallback (see [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md)). Use `--auth api_key
> --profile DEFAULT`. **Live-verified 2026-06-10:** `GET …/catalogs` → 200, `…/schemas?catalogKey=<key>` → 200;
> `…/tables?catalogKey=&schemaKey=` and `…/views?…` → 400 InvalidParameter (routes exist, need the real
> **schema key**, not the name).

## When to use
- Create/update/delete/refresh a **table**, **view**, **schema** (namespace), or **catalog**.
- **Register an external catalog/connection** (a persistent EXTERNAL CATALOG object). Test a connection.
- NOT a SELECT (→ `aidp-analyzing-data`); NOT SQL DDL on an existing lakehouse (→ `aidp-sql-ddl`);
  NOT a one-off file load (→ `aidp-ingest-file-to-table`); NOT transient per-notebook source reads
  (→ the `…-spark-connectors` plugin — that is a different thing from a registered catalog).

## Quick reference (CLI verb · REST · body)
| Object | CLI (preferred) | REST fallback | Create body (SDK `Create*Details`, camelCase) |
|---|---|---|---|
| Table | `aidp schema create-table\|update-table\|delete-table\|refresh-table` | `POST/PUT/DELETE …/tables?catalogKey=&schemaKey=` | `{displayName, catalogKey, schemaKey, description, tableType (MANAGED\|EXTERNAL), managedTableDefinition\|externalTableDefinition, tableFields[], partitionKeys[], tableProperties[]}` |
| View | `aidp schema create-view\|update-view\|delete-view` | `POST/PUT/DELETE …/views` | `{displayName, catalogKey, schemaKey, description, viewText, viewProperties[], viewFields[]}` |
| Schema | `aidp schema create\|update\|delete\|refresh` (namespace) | `POST/PUT/DELETE …/schemas` | `{displayName, catalogName, description, properties}` |
| Catalog | `aidp catalog create\|update\|delete\|refresh` | `POST/PUT/DELETE …/catalogs` | `{displayName, description, catalogType, sourceType, properties, connectionDetails}` |
| Test conn. | `aidp catalog test-connection` | `POST …/catalogs/actions/testConnection` | connection config |

SQL-native equivalents (often simpler) live in **`aidp-sql-ddl`**: `CREATE TABLE … USING DELTA` / CTAS,
`CREATE VIEW … AS SELECT`, `CREATE SCHEMA`, `DROP …`. Use whichever the user prefers; the control-plane verbs
also set object metadata/properties that SQL doesn't.

## Register an external catalog/connection
A **registered catalog** is a persistent object (`catalogType` + `sourceType` + `connectionDetails`), distinct
from the transient per-notebook source reads handled by the connectors plugin. Flow:
1. `aidp catalog create --display-name … --catalog-type … --source-type … --body @connection.json`
   (or `POST …/catalogs` with `CreateCatalogDetails`). **Confirm the exact `connectionDetails` shape with
   `aidp help catalog create` / the SDK `CreateConnectionDetails` model and a live test before production** —
   do not hand-fabricate connection fields.
2. `aidp catalog test-connection` to validate, then `aidp catalog refresh` to pull metadata.
3. Spark-SQL alternative for object-storage-backed tables: `CREATE EXTERNAL TABLE … LOCATION 'oci://…'`
   (via `aidp-sql-ddl`); Iceberg/Delta-on-`oci://` reads → `…-spark-connectors` (`aidp-iceberg`, `aidp-object-storage`).

## Guardrails
- **Mutation gate:** create/update/delete/refresh change state — show the body, confirm first (especially
  `delete-table`/`delete catalog`/drop-schema-CASCADE), and persist to `.aidp/payloads/`
  ([references/payloads.md]($HOME/.aidp/references/payloads.md)).
- Resolve `catalogKey`/`schemaKey` (real keys, not names) via `aidp-catalog-explore` first — the REST
  `tables`/`views` routes 400 without valid keys.
- Grants on these objects (catalog/schema/table/view) → `aidp-roles-access`.
- **Auto-Populate Catalog Extractor (bulk auto-cataloging) has a REST surface** at `…/dataLakes/<OCID>/extractors`
  (NOT `/metadataExtractors`, which 404s). **LIVE-VERIFIED 2026-06-12:** `GET …/extractors` → **200**. See
  `aidp-catalog-init` for the full surface (entities/extractedTables/manageExtractedEntities + lifecycle);
  complements `aidp-catalog-init` discovery + `aidp-ingest-file-to-table`.

## References
- [aidp-sql-ddl](../aidp-sql-ddl/SKILL.md) (SQL-native DDL) · [aidp-catalog-explore](../aidp-catalog-explore/SKILL.md) (resolve keys) · [aidp-ingest-file-to-table](../aidp-ingest-file-to-table/SKILL.md) (file load) · [aidp-roles-access](../aidp-roles-access/SKILL.md) (grants)
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) · [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md) · [references/payloads.md]($HOME/.aidp/references/payloads.md)
