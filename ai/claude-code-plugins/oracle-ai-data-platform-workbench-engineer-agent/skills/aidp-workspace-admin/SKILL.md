---
name: aidp-workspace-admin
description: Provision and inspect AIDP DataLake instances and workspaces, including private-network workspaces attached to a customer VCN/subnet. Use when the user wants to create/list/get a workspace or DataLake instance, set up a new (e.g. private) AIDP environment, or replicate a customer setup. Create/delete are guarded — confirm before any provisioning.
---

# `aidp-workspace-admin` — instance & workspace provisioning

Provision and inspect AIDP DataLake instances and workspaces via the AIDP control-plane. Covers the
IMFA-style private-workspace setup (workspace attached to a customer VCN/subnet via advanced fields). No MCP
server and no `ai-data-engineer-agent` repo are required.

## When to use
- Create / list / get a **workspace** or **DataLake instance**.
- Stand up a new AIDP environment (incl. private-network workspace).

## Engine — official `aidp` CLI (control-plane)
Preferred engine is the official Oracle `aidp` CLI; `oci raw-request` is the fallback when the CLI isn't
installed. Both hit the same data-plane REST API with the same auth — see
[references/aidp-cli-map.md](../../references/aidp-cli-map.md) for the command map and
[references/oci-raw-request.md](../../references/oci-raw-request.md) for the base URL, auth ladder,
async/pagination/etag conventions, and the no-fabrication gate.

| Op | CLI (preferred) | REST fallback |
|---|---|---|
| List workspaces | `aidp workspace list` | `GET /workspaces` |
| Get one workspace | `aidp workspace get --workspace-key <ws>` | `GET /workspaces/<ws>` |
| Create / update / delete | `aidp workspace create\|update\|delete` | `POST\|PUT\|DELETE /workspaces[/<ws>]` |
| Git folder | `aidp workspace create-git-folder` | `…/gitRepositories…` (Preview) |

All CLI calls take `--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`.
**DataLake *instance* creation is OCI control-plane (heavier, IAM-governed, billable) — there is no
data-plane `aidp` CLI command for it; use the `oci ai-data-platform` CLI shown below and gate it.**

```
REST base: https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/…
Auth: --profile DEFAULT (api_key) → on 401/403 fall back to --auth security_token --profile AIDP_SESSION
```

## Read operations (safe)
List and inspect workspaces — **LIVE-VERIFIED 200** on `20240831/dataLakes` (`GET /workspaces` is the
control-plane sanity call):

```bash
# CLI (preferred): list workspaces
aidp workspace list --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1
# get one workspace: aidp workspace get --workspace-key <WS> …

# Fallback (no CLI installed): oci raw-request
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/workspaces" \
  --profile DEFAULT
```

## Create a workspace
Prefer `aidp workspace create` with the body fields; the REST fallback is `POST /workspaces` with the same
JSON body. For a **private-network** workspace (attached to a customer VCN/subnet), include the networking
fields (e.g. `isPrivateNetworkEnabled`, subnet/NSG OCIDs, `freeformTags`/`definedTags`, `vectorDbId`). Build
the payload **with the user's real OCIDs** — never invent them; ask for the subnet/VCN OCIDs. Persist the
body to `.aidp/payloads/` and confirm first ([references/payloads.md](../../references/payloads.md)).

```bash
# CLI (preferred) — show the resolved body first, then create on confirmation
aidp workspace create --display-name "…" --workspace-type "…" \
  --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1
  # private-net: add --is-private-network-enabled true --subnet-id ocid1.subnet… (see `aidp help workspace`)

# Fallback (no CLI installed)
oci raw-request --http-method POST \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/workspaces" \
  --request-body '{"displayName":"…","workspaceType":"…","isPrivateNetworkEnabled":true,"subnetId":"ocid1.subnet…"}' \
  --request-headers '{"content-type":"application/json"}' \
  --profile DEFAULT
```

The exact request body fields are UNVERIFIED in this env — confirm against a live `201`/`400` before
asserting them as required, per the no-fabrication gate.

## Create / delete a DataLake instance (guarded — OCI control-plane `oci ai-data-platform`)
Instance lifecycle is an **OCI control-plane** resource (IAM-governed, async via work-requests, billable;
delete is destructive). It is **not** a data-plane `aidp`/`/dataLakes` call — use the OCI CLI
`oci ai-data-platform` family. **Live-verified 2026-06-10** (this command provisioned `aidp_agent_e2e` →
ACTIVE):
```bash
# CREATE — show the resolved args + confirm first (returns a work-request; poll to ACTIVE)
oci ai-data-platform ai-data-platform create \
  --compartment-id <COMPARTMENT_OCID> \
  --display-name "my_aidp" \
  --ai-data-platform-type prod \
  --default-workspace-name "my_ws" \
  --profile DEFAULT --region us-ashburn-1

# inspect / poll
oci ai-data-platform ai-data-platform get --ai-data-platform-id <OCID> --profile DEFAULT --region us-ashburn-1

# DELETE — destructive, confirm first (removes cluster + workspaces + all)
oci ai-data-platform ai-data-platform delete --ai-data-platform-id <OCID> --profile DEFAULT --region us-ashburn-1
```
- **Compartment:** ask the user which compartment — never auto-derive it silently.
- **IAM preflight (evidence-based, do not invent policy text):** instance create/delete is authorized by OCI
  IAM in the **target compartment**. If create returns `NotAuthorized`/404, the calling principal lacks
  manage rights on the AI Data Platform resource-family there — have the tenancy admin confirm/add the
  required policy (per OCI AIDP docs); don't assert a specific policy statement yourself.
- Async: create/delete return a **work request** — poll `oci ai-data-platform work-request get` (or list
  work-requests) until terminal, then confirm with `… get`.

## Guardrails
- **Destructive/provisioning actions are gated:** show the resolved payload, confirm with the user, and
  never create or delete on a shared/live environment without an explicit go-ahead.
- Networking itself (creating the VCN/subnet/NAT/Service-GW, ACL whitelists) is **out of scope** — that's
  OCI tooling. This skill only *attaches* a workspace to existing network resources.
- Never print OCIDs back in logs beyond what's needed; never hardcode them into committed files.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) — skill → official `aidp` CLI command map (primary engine)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) — base URL, auth ladder, conventions
- [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md) — live-verified endpoint status
- [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) — full no-MCP REST coverage
