---
name: aidp-ingest-file-to-table
description: Load a data file (CSV/JSON/Parquet/etc.) into a managed AIDP Delta table. Use when the user wants to ingest a file into a table, create a table from a file, or land raw data in the lakehouse. Supports the 1-step path and the 3-step upload→infer→create path. Control-plane via the official `aidp` CLI.
---

# `aidp-ingest-file-to-table` — file → managed Delta table

Land a file into a managed AIDP table, either in one call or via the staged 3-step flow when you need to
review/adjust the inferred schema. This is a **control-plane** flow on the DataLake `schema`/`tables`
resource. **Primary engine: the official Oracle `aidp` CLI** (same REST API + auth); `oci raw-request` is
the fallback when the CLI isn't installed.

## When to use
- "Load this CSV/JSON into a table", "create a table from <file>", "ingest <file> into the lakehouse".

## CLI (preferred)
Per [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md): `schema generate-temp-file-upload-target`
→ `schema infer` / `infer-with-preview` → `schema create-data-table` / `create-table` (also
`schema retrieve-par`). All commands take `--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`.
```bash
# 3-step (control): stage → infer → create
aidp schema generate-temp-file-upload-target --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1   # returns upload target / PAR (also: retrieve-par)
aidp schema infer-with-preview              --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1   # review columns/types/preview (or: infer)
aidp schema create-data-table --body-file .aidp/payloads/create-data-table-<name>.json \
  --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1                                            # or: create-table
```
**Mutating ops (`create-data-table`/`create-table`, upload):** persist the body to `.aidp/payloads/`
and confirm with the user before running (see [references/payloads.md]($HOME/.aidp/references/payloads.md)).

**Fallback (no CLI)** — same REST + auth via `oci raw-request` against
`…/20240831/dataLakes/<OCID>/…` (auth ladder in [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md)):
`POST /tables/actions/uploadDataFile` (multipart/binary may need PAR upload — see `aidp-volumes`),
`POST /tables/actions/inferSchema`, `POST /tables/actions/createTable` (with `catalogKey`, `schemaKey`,
table name, finalized columns, source format, load options), verify `GET /tables?catalogKey=<cat>&schemaKey=<cat.schema>`.

> **Verify-first (no-fabrication):** the upload/infer/create action shapes are **UNVERIFIED** in this env
> (not yet in `references/rest-endpoint-map.md`). Confirm with a live probe (start with a
> `GET /tables?catalogKey=…&schemaKey=…` 200 against the target schema) before any write; record results.

> **Live-verified 2026-06-10 on de-agent (CSV → `de_ingest_test`, 3 rows) — correction:** the
> `uploadDataFile` / `inferSchema` / `createTable` action names above are **WRONG**. The working flow is the
> `schema`-resource 3-step: (1) `generate-temp-file-upload-target` returns a PAR + `ociFilePath`; (2) PUT the
> file bytes to the PAR (HTTP 200); (3) `infer-with-preview` — its `location` MUST be the `ociFilePath` OCI
> URI, **not** the `uploadKey` (passing `uploadKey` → 400); (4) `create-data-table` returns **202 + a
> `datalake-async-operation-key`** (poll to `SUCCEEDED`).
> **`create-data-table` is HEADERLESS/POSITIONAL:** `header=true` is ignored at create, so `tableFields` must
> use the reader column names `_c0`/`_c1`/`_c2…` — naming them `id`/`name`/`amt` fails the async op with
> `UNRESOLVED_COLUMN`. Rename afterward via `ALTER TABLE … RENAME COLUMN`.

## Workflow
1. Confirm the source file location (workspace path or volume) and the target `catalog.schema.table`
   (create the schema first if needed).
2. **1-step (simple):** `aidp schema create-table` referencing the source file, format, and options —
   fastest when the schema infers cleanly.
3. **3-step (control):** `generate-temp-file-upload-target` → `infer-with-preview` (review columns/types
   with the user; fix types/headers/delimiters) → `create-data-table` with the finalized columns.
4. Async: table creation may return `202` with an async-operation key — poll until terminal (async
   convention in `references/oci-raw-request.md`; track via `aidp-observability`).
5. Verify with `aidp schema list-tables` / `GET /tables?…`; report the fully-qualified table name and row/column summary.

## Gotchas (documented limits, no workaround)
- **Delimited files: comma only** — auto-populate "Doesn't support delimiters other than comma" (platform
  reference §42 Known Issues #15). Pre-convert tab/pipe/semicolon-delimited files to CSV before ingest.
- **No multi-line JSON for external tables** — "Can't create external tables with multi-line JSON" (platform
  reference §42 Known Issues #12). Use newline-delimited JSON (one record per line) for external tables.

## Notes
- Big files: prefer landing into a volume / object storage and loading from there; mind cluster memory.
- For continuous/streaming or external-source ingestion, use the spark-connectors plugin + `aidp-federate`,
  not this skill (this is file→table).
- Clean up temporary tables created during validation.

## References
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) · [references/payloads.md]($HOME/.aidp/references/payloads.md) · [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md)
- pairs with `aidp-workspace-files`, `aidp-volumes`, `aidp-profiling-tables`
