---
name: aidp-workspace-files
description: Manage files and notebooks in the AIDP workspace filesystem — list, upload, download, read, create, move, rename, delete files/folders/notebooks. Use when the user wants to put a script/notebook/config into the workspace, read or move workspace files, or organize Shared/ folders.
---

# `aidp-workspace-files` — workspace filesystem & notebook CRUD

Manage the AIDP workspace filesystem and Jupyter contents. **Primary engine: the official Oracle `aidp` CLI**
`workspace-object` group (same REST API + auth); `oci raw-request` against the Notebook contents API is the
fallback when the CLI isn't installed.

## When to use
- "Upload this script/notebook to the workspace", "list/read/move/rename/delete a workspace file",
  "organize the Shared folder".

## CLI (preferred)
Per [references/aidp-cli-map.md](../../references/aidp-cli-map.md): `workspace-object create | get | head |
list | update | copy | move | rename | delete | upload-with-par | download-with-par`. All take
`--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>` (workspace-scoped — confirm `<WS>`).
```bash
aidp workspace-object list   --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1   # browse a dir
aidp workspace-object get    --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1   # read a file/notebook (head = metadata only)
aidp workspace-object create --body-file .aidp/payloads/create-load-py.json \
  --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1                              # create/overwrite
aidp workspace-object move|rename|copy|delete ...                                                                  # reorganize
aidp workspace-object upload-with-par|download-with-par ...                                                        # PAR transfer (binary-safe)
```
**Mutating ops (`create`/`update`/`move`/`rename`/`delete`/`upload-with-par`):** persist the body to
`.aidp/payloads/` and confirm with the user before running (see [references/payloads.md](../../references/payloads.md)).

**Fallback (no CLI)** — same REST + auth via the workspace-scoped Jupyter **Notebook contents API** with
`oci raw-request` (auth ladder in [references/oci-raw-request.md](../../references/oci-raw-request.md)):
Base `…/20240831/dataLakes/<DATALAKE_OCID>/workspaces/<WS>/notebook/api/contents/<urlencoded-path>` —
`GET <dir>` list · `GET <file>?content=1` (`type=notebook` for `.ipynb`) read · `PUT <path>` create/overwrite
(`type`/`format`/`content` body) · `POST <dir>` create-untitled / rename-move (`{"path":…}`) · `DELETE <path>`.
Jupyter shapes: file `{"type":"file","format":"text","content":"…"}`; notebook `{"type":"notebook","content":<ipynb-json>}`;
dir `{"type":"directory"}`.

> **Live-verified 2026-06-10 on de-agent — correction:** bare `api_key` `oci raw-request` against the
> `…/notebook/api/contents/<path>` API does **not** reliably do file CRUD on `20240831` instances — directory
> `GET` → 500 InternalError; `PUT`-create / `GET` / `DELETE` of a path → 404 NotAuthorizedOrNotFound (route is
> reachable — structured JSON errors — but the HTTP contents CRUD does not succeed). Treat the contents HTTP
> REST path as **not-working pending fix / AIDP_SESSION re-test**. For reliable file/notebook CRUD, route ops
> through the WebSocket Jupyter helper (`scripts/aidp_sql.py`, used by `aidp-notebooks` — auto-creates notebooks,
> lists/deletes at `/Workspace/Shared`), PAR-based `upload-with-par` / `download-with-par`, or the `nb_*` MCP tools.

## Workflow
1. Confirm the workspace (`<WS>`) and DataLake OCID; never trust a local `.env`.
2. **List/inspect:** `aidp workspace-object list` (then `head` for metadata, `get` to read).
3. **Put a file/notebook:** `aidp workspace-object create` (persist body to `.aidp/payloads/`, confirm first).
4. **Move/rename/copy:** `aidp workspace-object move|rename|copy`.
5. **Delete:** `aidp workspace-object delete` — do a `list` first and confirm before deleting anything you didn't just create.

## Notes
- **Verify-first (no-fabrication):** confirm the path with a live `list`/`GET` of a known dir before
  mutating; record the working shape. List before any delete.
- **Binary uploads** (wheels/jars/images/parquet): use `upload-with-par` / `download-with-par` (or a
  **volume + PAR** via `aidp-volumes`) — the JSON contents API is text/notebook only.
- Never upload secrets/`.env`.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) · [references/payloads.md](../../references/payloads.md) · [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) · pairs with `aidp-notebooks`, `aidp-volumes`
