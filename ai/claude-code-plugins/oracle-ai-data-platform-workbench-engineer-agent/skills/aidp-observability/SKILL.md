---
name: aidp-observability
description: Surface AIDP operational state — recently accessed resources and long-running async operations (status, completion, waiting). Use when the user asks "what happened recently", "what's running", "is that operation done yet", or needs to track/await an async AIDP operation (e.g. provisioning, large commits). Runs over the official `aidp` CLI.
---

# `aidp-observability` — activity & async operations

Show what's happening in the AIDP workspace: recent job runs and the state of long-running operations.
**Primary engine: the official Oracle `aidp` CLI** (`async-operations` + `workflow list-recent-job-runs`);
`oci raw-request` is the fallback when the CLI isn't installed. Read-only — no payload persistence needed.

## When to use
- "What did I touch recently / what's running?" · "is operation X done?" · waiting on a provisioning or
  long commit to finish.

## CLI (preferred)
Per [references/aidp-cli-map.md](../../references/aidp-cli-map.md): `async-operations list | get` and
`workflow list-recent-job-runs`. All take `--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`
(workspace-scoped — confirm the workspace).
```bash
# In-flight ops — a resource-type / filter param is REQUIRED
aidp async-operations list --resource-type <type> \
  --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1
aidp async-operations get <key> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1   # status of one
# Recent job runs
aidp workflow list-recent-job-runs --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1
```

**Fallback (no CLI)** — same REST + auth via `oci raw-request` against
`…/20240831/dataLakes/<DATALAKE_OCID>/…` (auth ladder in [references/oci-raw-request.md](../../references/oci-raw-request.md)):
```bash
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/asyncOperations?<param>=<value>" \
  --profile DEFAULT
```
`GET …/asyncOperations?<param>=<value>` (a query param is **required** — a bare path returns
`400 InvalidParameter: query param X must not be null`; the error names the missing param, e.g. a
resource-type / `workspaceKey` / `status`; also accepts `sortBy=timeCreated`) · `GET …/asyncOperations/<key>`
for one. **`GET …/recentActivities` returned 404 in testing (2026-06-10)** — prefer the CLI
`workflow list-recent-job-runs` for "recent activity"; treat `recentActivities` as not provisioned unless a
live read says otherwise.

> **Live-verified 2026-06-10 on de-agent — correction:** the required filter is specifically
> `status=` **or** `resourceType=`. A bare `GET …/asyncOperations` returns
> `400 MissingParameter: "ResourceType or Status filter need to be specified"`. Confirmed 200s:
> `?status=SUCCEEDED` (returned real `CREATE_CLUSTER`/`CREATE_WORKSPACE` records), `?resourceType=CLUSTER`,
> and get-one `…/asyncOperations/<key>`. `…/recentActivities` returned
> `404 NotAuthorizedOrNotFound` (not provisioned here) — use `?status=`/`?resourceType=` or
> `aidp workflow list-recent-job-runs` instead.

## Workflow
- **In-flight ops:** `aidp async-operations list --resource-type <type>` → table of key, type, state, started.
- **Status of one:** `aidp async-operations get <key>` → report the operation's current `state`.
- **Block until done:** poll `aidp async-operations get <key>` on an interval (back off; clamp total wait)
  until a terminal state, then report it. Use this after provisioning (`aidp-workspace-admin`), bundle
  deploys (`aidp-bundle`), or other `202`-async actions — those return an operation key in the
  `datalake-async-operation-key` header or a body field (see the Async note in the reference).
- **Recent activity:** `aidp workflow list-recent-job-runs` → present resource, action, time.

## Notes
- Workspace-scoped — confirm the workspace.
- `async-operations list` needs a **resource-type / filter param** (shape-TBD in the live map) — probe,
  read the 400, supply the named param. Don't present it as confirmed until a live 2xx is recorded
  (no-fabrication gate).
- For Spark job/stage/cluster-log diagnostics use `aidp-spark-debugging`; this skill is platform-level
  operation tracking, not Spark internals.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) · [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) · [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md)
