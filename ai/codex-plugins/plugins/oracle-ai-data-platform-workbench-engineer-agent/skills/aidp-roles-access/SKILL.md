---
name: aidp-roles-access
description: Manage AIDP roles and access — list roles, view permissions, create roles, add/remove members, and grant/revoke per-resource permissions on catalogs, schemas, tables, views, volumes, workspaces, workspace objects, and clusters. Also covers column masking/classification (restricted views + ontology sensitivity — no masking REST API exists). Use when the user asks about roles/RBAC, who can access what, granting/revoking access on any resource, adding someone to a role, or masking/classifying columns. Primary engine is the official `aidp` CLI; the same REST API via `oci raw-request` is the no-CLI fallback.
---

# `aidp-roles-access` — roles, permissions, access (RBAC)

Inspect and manage AIDP RBAC.

**CLI (preferred):** `aidp role <command> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`
- Roles: `aidp role list | get | create | update | delete | add-member | remove-member | list-permissions`
- Per-resource grants: `aidp <catalog|cluster|volume|schema|workspace|workspace-object> list-permissions | manage-permission`

**Fallback (no CLI):** same Role REST API via `oci raw-request` (identical endpoint + auth). Permission
**writes** (workspace/cluster/volume grants) can also use the gated MCP admin tools as an **optional
accelerator** when configured.

> **Verify-first + least privilege:** bind to the caller's identity; never escalate beyond what they have.
> Confirm the working path with a live `aidp role list` (or `GET /roles`) before any write.
> Auth + base URL: [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md).

## When to use
- "List roles / who has access", "create a role", "add/remove a member", "grant/revoke access to a
  workspace/cluster/volume".

## Read & role CRUD (CLI preferred)
```bash
# List roles (smoke test) — CLI
aidp role list --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1

# Add a member to a role — CLI
aidp role add-member --instance-id <DATALAKE_OCID> --role-key <ROLE_KEY> \
  --auth api_key --profile DEFAULT --region us-ashburn-1 \
  --principals '["ocid1.user.oc1..xxxx"]'
```

**Mutating ops** (`create`, `update`, `delete`, `add-member`, `remove-member`, `manage-permission`):
persist the body to `.aidp/payloads/` and confirm first ([references/payloads.md]($HOME/.aidp/references/payloads.md)).

### Fallback (no CLI) — REST via `oci raw-request`
Base: `https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/…`

- **List roles** — `GET /roles` — ✅ **LIVE-VERIFIED 200** (api_key DEFAULT profile, `20240831`/`dataLakes`).
- **Inspect a role** — `GET /roles/{k}`, `GET /roles/{k}/permissions`.
- **Create / update / delete** — `POST /roles`, `PUT /roles/{k}`, `DELETE /roles/{k}` (send `if-match: <etag>` on PUT/DELETE).
- **Membership** — `POST /roles/{k}/actions/addMember` · `POST /roles/{k}/actions/removeMember`
  (body e.g. `{"principals":["ocid1.user.oc1..xxxx"]}`).

```bash
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/roles" \
  --profile DEFAULT
```

On `401`/`403`/"Security Token", follow the auth ladder in `oci-raw-request.md` (refresh `AIDP_SESSION`).

## Per-resource permission grants (full matrix)
Role CRUD + membership scope *who is in a role*; per-resource grants scope *what a principal can do to one
object*. Every resource type exposes a `list-permissions` + `manage-permission` pair (CLI preferred; REST
`…/<resource>/<key>/permissions` + `…/actions/managePermission` fallback). Grant body is consistently
`{"principals":[…], "permission":"<enum>", "action":"GRANT"|"REVOKE"}` — confirm the exact `permission` enum
for each resource via `aidp help <resource> manage-permission` / a live read before writing.

| Resource | CLI verbs | REST |
|---|---|---|
| Catalog | `aidp catalog list-permissions \| manage-permission` | `…/catalogs/<key>/permissions` |
| Schema | `aidp schema list-permissions \| manage-permission` | `…/schemas/<key>/permissions` |
| **Table** | `aidp schema list-table-permissions \| manage-table-permission <TABLE-KEY>` | `…/tables/<key>/permissions` |
| **View** | `aidp schema list-view-permissions \| manage-view-permission <VIEW-KEY>` | `…/views/<key>/permissions` |
| Volume | `aidp volume list-permissions \| manage-permission` | `…/volumes/<key>/permissions` |
| Workspace | `aidp workspace list-permissions \| manage-permission` (+ `list-create-permissions \| manage-create-permission`) | `…/workspaces/<key>/permissions` |
| Workspace object | `aidp workspace-object list-permissions \| manage-permission` | `…/workspaceObjects/<key>/permissions` |
| Cluster | (no GA CLI verb) | `…/clusters/<key>/permissions` |
| **Job/Workflow** | `aidp workflow list-job-permissions <ws> <JOB-KEY>` / `manage-job-permission <ws> <JOB-KEY> --body` | `…/workspaces/{ws}/jobs/{key}/permissions` |
| Knowledge Base | `assign\|manage\|revoke` KB permission (`aidp-knowledge-bases`) | `…/knowledgeBases/<key>/permissions` |

> **Job/Workflow body shape differs from the generic grant.** The CLI README + SDK confirm
> `manage-job-permission` does **not** take `{principals,permission,action}`. Its grant body is
> `AssignJobPermissionDetails` = `{"assignees":{"type":"USER|ROLE|GROUP","targets":[…]},"permissions":["READ"|"USE"|"MANAGE"|"ADMIN"]}`
> (`permissions` is a list aligned 1:1 with `assignees.targets`); the manage wrapper is
> `{"assignJobPermissionDetails":{…},"revokeJobPermissionDetails":{…}}`. Enum names are confirmed-citable
> (SDK `assign_job_permission_details.py` lines 18-30 / `permission_assignees.py` lines 18-26;
> CLI README `workflow manage-job-permission`, README lines 7315-7377); still confirm the live enum with
> `aidp help workflow manage-job-permission` or a `list-job-permissions` read before writing.

**Optional accelerator:** when a gated `aidp` MCP is configured (`AIDP_MCP_ENABLE_ADMIN_TOOLS=true` + MCP
restart), `manage_workspace_permission` / `manage_cluster_permission` / `manage_volume_permission` /
`manage_create_workspace_permission` wrap the same writes (`details_json`
`{"principals":[…],"permission":"WRITE","action":"GRANT"}`). Not required — REST verbs above are the source of
truth; or apply the grant in the console.

## Column masking & classification (honest scope — no data-plane API found)
**There is no programmatic masking/classification REST API in the tested tenancy** — `GET …/maskingPolicies`,
`/dataClassifications`, `/columnMaskingPolicies`, `/tags` all returned **404** (probed 2026-06-10; recorded in
`references/rest-endpoint-map.md`). Do **not** fabricate one. What actually exists:
- **Restricted / redacting views** — the practical column-level control today: `CREATE VIEW` exposing only
  permitted columns (or `CASE`/hashing to redact), then grant on the view, not the base table (`aidp-sql-ddl`
  + the View row above). This is the recommended pattern when asked to "mask a column".
- **Ontology-driven sensitivity** — the **Ontologies** feature tags terms (`av:isSensitive` / `av:requiresRole`)
  for governance, but it is UI-driven with no confirmed REST surface here (see the Ontologies note in
`aidp-semantic-model`).
If the user needs policy-based dynamic masking, surface that it's UI/ontology-governed today and offer the
restricted-view pattern as the API-driven equivalent — don't claim a masking endpoint.

## Workflow
1. Read current state first (`GET /roles` + the relevant role's `/permissions`).
2. Show the exact grant/revoke (principal, permission, target) and **confirm** before applying.
3. Apply via REST (role CRUD/membership), or the gated admin tool if it's available; re-read to confirm.

## Guardrails
- Access changes are sensitive — confirm every grant/revoke; never broaden beyond the user's request.
- Don't propose IAM/permission changes that aren't explicitly asked for.

## References
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) · [references/payloads.md]($HOME/.aidp/references/payloads.md) · [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md)
