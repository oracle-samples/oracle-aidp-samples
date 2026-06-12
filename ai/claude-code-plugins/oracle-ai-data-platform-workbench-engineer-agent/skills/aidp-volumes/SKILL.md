---
name: aidp-volumes
description: Work with AIDP volumes — list volumes, browse files inside a volume, upload/download via the PAR flow, and create directories. Use when the user mentions volumes, needs to stage large/binary files, or move data in/out of a volume (distinct from the workspace filesystem). Control-plane via the official `aidp` CLI.
---

# `aidp-volumes` — volume files & PAR transfers

Manage AIDP volumes and their contents — the staging area for larger/binary data, distinct from the
workspace filesystem. **Primary engine: the official Oracle `aidp` CLI** `volume` group (same REST API +
auth); `oci raw-request` is the fallback when the CLI isn't installed.

## When to use
- "List volumes / what's in volume X", "upload/download a file to/from a volume", "make a directory in a
  volume", staging large or binary files.

## CLI (preferred)
Per [references/aidp-cli-map.md](../../references/aidp-cli-map.md): `volume list | get | create | list-files |
make-dir | upload-file[-with-par] | download-file[-with-par] | delete-file | delete-dir`. All take
`--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`.
```bash
# LIVE-VERIFIED: list needs BOTH catalogKey AND schemaKey
aidp volume list --catalog-key <cat> --schema-key <cat.schema> \
  --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1
aidp volume get <key>        --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1   # one volume
aidp volume list-files <key> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1   # browse files
aidp volume upload-file-with-par|download-file-with-par <key> ...                                                  # PAR transfer (binary-safe)
aidp volume make-dir|delete-file|delete-dir <key> ...
```
**Mutating ops (`create`/`make-dir`/`upload-file[-with-par]`/`delete-file`/`delete-dir`):** persist the body
to `.aidp/payloads/` and confirm with the user before running (see [references/payloads.md](../../references/payloads.md)).

**Fallback (no CLI)** — same REST + auth via `oci raw-request` against
`…/20240831/dataLakes/<DATALAKE_OCID>/…` (auth ladder in [references/oci-raw-request.md](../../references/oci-raw-request.md)):
`GET /volumes?catalogKey=<cat>&schemaKey=<cat.schema>` (both params required; a bare path / single param
returns **400 InvalidParameter** — the error names the missing key) · `GET /volumes/<key>` · files under
`GET /volumes/<key>/files` · PAR upload/download via `POST /volumes/<key>/actions/…` then `PUT`/`GET` the
bytes to the returned PAR URL · `POST /volumes/<key>/actions/makeDir`.

> **Verify-first (no-fabrication):** the volumes routes are **UNVERIFIED** here — `GET /volumes?catalogKey=…`
> alone returned **400** (also needs `schemaKey`). Confirm the exact params with a live `GET`/`volume list`
> before any write/upload; record the working shape in `references/rest-endpoint-map.md`.
>
> **Live-verified 2026-06-10 on de-agent — correction:** the route **EXISTS** (not removed). A bare
> `GET …/volumes` returns **400 InvalidParameter** naming both missing keys — *"query param schemaKey must not
> be null; query param catalogKey must not be null"* — so listing volumes requires **both** `catalogKey` and
> `schemaKey` query params.

## Workflow
1. **Verify:** `aidp volume list --catalog-key <cat> --schema-key <cat.schema>` (read back the required-param
   shape from the 400 if it errors).
2. **Browse:** list volumes → `list-files <key>`; `get <key>` for one volume's details.
3. **Upload:** `upload-file-with-par`; **download:** `download-file-with-par` (PAR bytes move directly to/from
   Object Storage, not through the JSON API).
4. **Dirs:** `make-dir`.
5. Confirm before destructive/overwrite operations.

## Volume-object lifecycle (the volume itself, not its files)
Beyond file ops, the volume **object** has its own CRUD: `aidp volume create | update | delete`
(REST `POST /volumes` · `PUT /volumes/<key>` · `DELETE /volumes/<key>`), plus directory ops
`make-dir | update-dir | delete-dir`. Create body carries `displayName`, `catalogKey`, `schemaKey` (+
type/location) — confirm the exact fields with `aidp help volume create` / a live read before writing
(routes need real catalog/schema keys, like `list`). Grants on a volume → `aidp-roles-access` (volume row).
Persist create/update/delete bodies to `.aidp/payloads/` and confirm first.

## Notes
- **PAR transfers are byte-accurate** — they carry binary fine (wheels/jars/parquet) as well as text.
- **Async (202):** PAR-provisioning or dir actions may return `202` with an operation key — poll the
  async-ops endpoint until terminal (see `aidp-observability` / `oci-raw-request.md`).
- Volumes are the right home for big staging files before `aidp-ingest-file-to-table`.
- Workspace/catalog-scoped — confirm the workspace and catalog before listing.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) · [references/payloads.md](../../references/payloads.md) · [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) · [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md) · pairs with `aidp-ingest-file-to-table`, `aidp-workspace-files`
