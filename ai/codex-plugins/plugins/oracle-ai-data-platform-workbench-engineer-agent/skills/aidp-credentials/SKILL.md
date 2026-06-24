---
name: aidp-credentials
description: Manage the AIDP credential store (secrets) — list, get, create, update, delete credentials used by AIDP workflows. Use when the user wants to store/rotate a secret centrally instead of embedding it, or manage connection credentials. Primary engine is the official `aidp` CLI (`aidp credentials …`); the same Preview REST API via `oci raw-request` is the no-CLI fallback. Verify the endpoint live before relying on it.
---

# `aidp-credentials` — credential store (Preview)

Manage centrally-stored AIDP credentials/secrets.

**CLI (preferred):** `aidp credentials <command> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`
- `aidp credentials list | get | create | update | delete`

**Fallback (no CLI):** same `credentialStore` REST API via `oci raw-request` (identical endpoint + auth;
see `references/oci-raw-request.md`).

> **Preview + verify-first (no-fabrication):** `credentialStore` is **Preview** and the route exists, but
> its **GET/response shape is TBD**. Confirm the working path (default `20240831`/`dataLakes`) with a live
> `aidp credentials list` (or `GET …/credentials`) before asserting success or doing writes; record it in
> `references/rest-endpoint-map.md`. Treat the path as **UNVERIFIED** until a live 2xx returns.

## When to use
- "Store/rotate a secret in AIDP", "manage connection credentials", "stop embedding this secret in code".

## Workflow
1. **Verify first:** `aidp credentials list` (CLI) — or a `GET …/credentials` (REST fallback) — returns
   2xx; record the version/prefix.
2. Read/create/update as asked. **Never print secret values**; pass secret material in the request body
   only, never echo it back. Confirm before delete/rotate.
3. Handle async 202 + etag/if-match per the shared conventions.

**Mutating ops** (`create`, `update`/rotate, `delete`): persist the body to `.aidp/payloads/` and confirm
first ([references/payloads.md]($HOME/.aidp/references/payloads.md)).

### Create body — `CreateDataLakeCredentialDetails`
CLI: `aidp credentials create <DATALAKE_OCID> --body <JSON>` (CLI README "credentials create"). Top-level
envelope (SDK `create_data_lake_credential_details.py:51-63`):

| Field (wire) | Req | Notes |
|---|---|---|
| `displayName` | ✅ | start with a letter; letters/digits/`_` only — no secrets in the name |
| `credentialDescription` | – | purpose summary |
| `type` | ✅ | discriminator — `SECRET_TOKEN` \| `VAULT_REFERENCE` \| `SERVICE_ACCOUNT` (`…:18-26`) |
| `credentialDetails` | ✅ | nested object whose `credentialType` must match `type` (`credential_details.py:52-73`) |

`credentialDetails` shape per type (subclass models + CLI README "credentials create"):

| `credentialType` | Fields (wire) | Source |
|---|---|---|
| `SECRET_TOKEN` | `secretTokenPair`: array of `{secretKey, secretValue}` | `secret_token_credential_details.py:38-41`, `secret_pair.py:35-38` |
| `VAULT_REFERENCE` | `secretId` (OCID of an external Vault secret) | `vault_reference_credential_details.py:38-41` |
| `SERVICE_ACCOUNT` | `userId`, `fingerprint`, `tenancy`, `region`, `isReadOnly`, `privateKey` | `service_account_credential_details.py:63-71` |

Example (`SECRET_TOKEN`) — **persist to `.aidp/payloads/create-<name>-credential.json` and confirm first;
the `secretValue` is the only secret material — pass it in the body, never echo it back:**
```json
{
  "displayName": "github_pat",
  "credentialDescription": "GitHub PAT for workspace git",
  "type": "SECRET_TOKEN",
  "credentialDetails": {
    "credentialType": "SECRET_TOKEN",
    "secretTokenPair": [ { "secretKey": "token", "secretValue": "<PAT>" } ]
  }
}
```
> Field **names** are confirmed (SDK `attribute_map` + CLI README). The full create **round-trip** is
> **verify-first**: `…/credentials` GET returned 400 here (Preview, list-shape TBD —
> `references/rest-endpoint-map.md`), so confirm a 2xx before relying on the POST.

### Fallback (no CLI) — REST endpoints (**lake-scoped**, Preview)
**Live-probed 2026-06-10:** `GET …/dataLakes/<ocid>/credentials` → **400** (route exists, list-shape TBD —
needs a param/body); `…/workspaces/<ws>/credentials` → **404** (so credentials are **lake-scoped, not
workspace-scoped**).
- `GET  /dataLakes/<ocid>/credentials` — list (400 until the required param/shape is supplied — verify live)
- `POST /dataLakes/<ocid>/credentials` — create
- `GET|PUT|DELETE /dataLakes/<ocid>/credentials/{key}` — get / update / delete

Base URL: `https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<dataLakeOcid>/…`

## Guardrails
- Secrets never go into logs, the transcript, or committed files.
- Destructive ops (delete/rotate) require explicit confirmation.

## References
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) · [references/payloads.md]($HOME/.aidp/references/payloads.md) · [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md)
