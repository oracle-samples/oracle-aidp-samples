---
name: aidp-user-settings
description: Manage AIDP user settings / preferences — list, get, create, update, delete a user's settings for the DataLake. Use when the user wants to view or change their AIDP workbench preferences/settings, or manage stored user-setting entries. Self-contained — official `aidp user-setting` CLI preferred, `oci raw-request` fallback.
---

# `aidp-user-settings` — user settings & preferences

Read and manage AIDP user-setting entries. Self-contained: no MCP / `ai-data-engineer-agent` required.
Engine precedence per `references/aidp-cli-map.md` — prefer the official `aidp` CLI, else `oci raw-request`.

## When to use
- "Show / change my AIDP settings or preferences", "list/get/update/delete a user setting".

## Engine (CLI-preferred)
- `aidp user-setting list <instance-id> --auth api_key --profile DEFAULT`
- `aidp user-setting get|create|update|delete <instance-id> … ` (see `aidp help user-setting` for the
  key/body of each; create/update take a `--body` JSON).
- **Fallback (`oci raw-request`):** `…/20240831/dataLakes/<OCID>/userSettings` — **live-verified 200**
  (lake-scoped, 2026-06-10); `GET …/userSettings/{key}`, `POST`/`PUT`/`DELETE` for CRUD (confirm the
  create/update body via `aidp help user-setting` / a live read before writing).

## Create body — `CreateUserSettingDetails`
CLI: `aidp user-setting create <DATALAKE_OCID> --body <JSON>` (CLI README "user-setting create").
Envelope (SDK `create_user_setting_details.py:34-44`):

| Field (wire) | Req | Notes |
|---|---|---|
| `name` | ✅ | user-friendly setting name |
| `isDefault` | ✅ | mark this the default for its type |
| `data` | ✅ | nested `SettingData`; discriminator `type` ∈ `IAM_USER_CREDENTIAL` \| `GIT_ACCOUNT` \| `OAUTH` (`setting_data.py:18-26`) |

`data` variants (subclass models + CLI README "user-setting create"):

| `type` | Fields (wire) | Source |
|---|---|---|
| `GIT_ACCOUNT` | `entityType` (`PERSONAL_ACCESS_TOKEN`), `providerName` (`GITHUB`\|`BITBUCKET`\|`GITLAB`\|`OCI_DEVOPS`), `username`, `personalAccessToken` | `git_account_user_setting.py:77-83`, enums `…:18-34` |
| `IAM_USER_CREDENTIAL` | `userId`, `tenancy`, `region`, `fingerprint`, `privateApiKey` | `iam_user_credential_user_setting.py:57-64` |

Example (`GIT_ACCOUNT`) — **persist to `.aidp/payloads/create-<name>-user-setting.json` and confirm first;
`personalAccessToken`/`privateApiKey` are secret material — pass in the body, never echo back:**
```json
{
  "name": "my_github",
  "isDefault": true,
  "data": {
    "type": "GIT_ACCOUNT",
    "entityType": "PERSONAL_ACCESS_TOKEN",
    "providerName": "GITHUB",
    "username": "<user>",
    "personalAccessToken": "<PAT>"
  }
}
```
> Field **names** are confirmed (SDK `attribute_map` + CLI README). `…/userSettings` GET is live-200
> (lake-scoped, 2026-06-10); still re-read after create to confirm the round-trip.

## Workflow
1. `list`/`get` to show current settings.
2. For create/update/delete, show the change and apply it; re-read to confirm.

## Guardrails
- Scope to the caller's own settings; don't change another principal's settings without explicit instruction.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) · [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md)
