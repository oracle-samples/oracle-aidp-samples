---
name: aidp-rest
description: "AIDP control-plane REST client primitives: OCI signing, workspace/cluster discovery, notebook upload via contents API, Job + JobRun lifecycle, taskRun output fetch + marker parsing. Use when a skill or tool needs to talk to a live AIDP cluster — uploading code, dispatching jobs, polling state, retrieving executed notebook output. NOT for AIDP CLI flows (use the `.aidp/cli.js` wrapper) or for in-cluster code (the AIDP runtime injects spark/aidputils as globals)."
---

# aidp-rest — AIDP control-plane REST primitives

> **Canonical location**: the Python client lives in the plugin source at `scripts/oracle_ai_data_platform_fusion_autopilot/dispatch/rest_client.py`. The skill's `client.py` is a path-resolving re-export shim — it adds the checkout's `scripts/` directory to `sys.path` and re-exports the canonical names. Edits to the client land in the plugin first; this skill documents the contract and the empirically-confirmed gotchas.

Reusable Python client for the AIDP `/aiDataPlatforms/<id>/workspaces/<key>/...` REST surface. Encapsulates OCI request signing (both API-key and session-token profiles), the empirically-confirmed request shapes, and the gotchas that aren't in Oracle's swagger.

## When to use

- Building a skill that dispatches a notebook to a live cluster.
- Diagnosing a job run — fetching its executed notebook, parsing output markers, finding the failed task.
- Resolving workspace / cluster display names to UUIDs.
- Starting a STOPPED cluster + waiting for ACTIVE before submitting work.

**Do NOT use** for:
- In-cluster code that runs inside an AIDP notebook session — `spark`, `aidputils`, `oidlUtils` are runtime globals; talk to the AIDP control plane via those, not via REST.
- The `.aidp/cli.js` CLI flows defined in the workspace `CLAUDE.md` — those handle agent flows, not workspace data jobs.

## Prerequisites

1. **OCI CLI authed** — `~/.oci/config` with at least a `DEFAULT` profile pointing at a user with AIDP read/write on the target tenancy.
2. **Coordinates known**:
   - `region` (e.g. `us-ashburn-1`)
   - `aiDataPlatformId` (OCID; the regional AIDP instance)
   - `workspaceKey` (UUID — discoverable via `list_workspaces()` + `find_workspace_by_name(...)`)
   - `clusterKey` (UUID — discoverable via `list_clusters(workspaceKey)` + `find_cluster_by_name(...)`)

## Quickstart

```python
from client import AidpRestClient

client = AidpRestClient(
    region="us-ashburn-1",
    aidp_id="<ocid1.datalake.oc1.{region}.{tenancy-specific}>",
    workspace_key="<workspace-uuid>",
)

# 1. Verify cluster is healthy
client.verify_cluster_active(cluster_key)

# 2. Upload a notebook (built locally as an nbformat-4 dict)
client.upload_notebook("/Workspace/Shared/my-skill/run.ipynb", notebook_dict)

# 3. Create a job + submit a run
job_key = client.create_notebook_job(
    name="my-skill-run", description="...",
    notebook_path="/Workspace/Shared/my-skill/run.ipynb",
    cluster_key=cluster_key, cluster_name="fusion_autopilot_dev",
    task_key="main",
)
run_key = client.submit_run(job_key)

# 4. Poll to terminal state — returns a RunResult dataclass with .status + .raw
result = client.poll_run(run_key, timeout_s=2700, interval_s=20)
if result.status != "SUCCESS":
    handle_failure(result.raw)

# 5. Fetch the executed notebook
task_run_key = client.resolve_task_run_key(result.raw, "main")
executed_nb_json = client.fetch_output(task_run_key)

# 6. Parse a stdout marker the notebook printed
marker = client.parse_marker(json.loads(executed_nb_json),
                             begin="AIDP_LIVE_TEST_RESULT_BEGIN",
                             end="AIDP_LIVE_TEST_RESULT_END")
```

## Endpoint reference (regional data-plane host)

```
https://datalake.<region>.oci.oraclecloud.com/<apiVersion>/aiDataPlatforms/<aiDataPlatformId>/workspaces/<workspaceKey>/...
```

`apiVersion` = `20260430`. Live-validated 2026-05-17 / 2026-05-21 against the `<aidp-instance>` AIDP instance + `playground` workspace.

| Operation | HTTP + path | Notes |
|---|---|---|
| List workspaces | `GET /workspaces` | Response shape: `{items:[{key, displayName, type, ...}]}` — name only in `displayName`, not `name` |
| Get workspace | `GET /workspaces/{key}` | Single-resource shape (no `items` wrapper) |
| List clusters | `GET /workspaces/{wk}/clusters` | `clusters[].state` is one of `ACTIVE/STOPPED/FAILED/CREATING` (NOT `lifecycleState`) |
| Get cluster | `GET /workspaces/{wk}/clusters/{ck}` | Used by `verify_cluster_active` and `wait_cluster_active` poll loops |
| Start cluster | `POST /workspaces/{wk}/clusters/{ck}/actions/start` | Body `{}` required (empty body → 400). Returns 202 + cluster object. Poll `state` for `STOPPED → CREATING → ACTIVE` (~3-5 min). |
| Upload content | `PUT /workspaces/{wk}/notebook/api/contents/{urlencoded_path}` | Body: `{name, path, type:'notebook', format:'json', content:<nbformat dict>}`. PUT is create-or-update (no separate POST step). Server strips leading `/` in returned `path`. |
| Read content | `GET /workspaces/{wk}/notebook/api/contents/{urlencoded_path}?type=notebook&content=1` | The `?type=notebook&content=1` query is **required** — other forms return 500. |
| Create job | `POST /workspaces/{wk}/jobs` | See "Job-create body" below — `path: "jobs"` + `maxConcurrentRuns: 1` are **required client-side** even though swagger lists only `name` as required. Omitting trips 500 + CircuitBreaker (~15min cooldown). |
| List jobs | `GET /workspaces/{wk}/jobs` | |
| Submit run | `POST /workspaces/{wk}/jobRuns` | Body: `{jobKey, parameters:[], queue:{isEnabled:false}}` |
| Poll run | `GET /workspaces/{wk}/jobRuns/{key}` | `state.status` enum: `PENDING/RUNNING/SUCCESS/FAILED/CANCELED/TIMED_OUT` |
| Fetch task output | `POST /workspaces/{wk}/taskRuns/{trk}/actions/fetchOutput` | Body: `{outputKey: ""}` (empty string — `"main"` returns misleading 404). Notebook lands in `data[0].value` (NOT `data[0].content`). |

## Job-create body (the empirically-correct shape)

```json
{
  "name": "<unique>",
  "description": "...",
  "path": "jobs",
  "maxConcurrentRuns": 1,
  "jobClusters": [{"clusterKey": "<UUID>", "clusterName": "<name>", "newCluster": null}],
  "parameters": [],
  "tasks": [
    {
      "type": "NOTEBOOK_TASK",
      "taskKey": "<task-local id>",
      "runIf": "ALL_SUCCESS",
      "notebookPath": "/Workspace/Shared/<dir>/<file>.ipynb",
      "cluster": {"clusterKey": "<UUID>", "clusterName": "<name>", "newCluster": null},
      "parameters": [],
      "dependsOn": [],
      "maxRetries": 0
    }
  ]
}
```

- `jobClusters[]` and `tasks[].cluster` are **redundant mirrors** — both carry the same `{clusterKey, clusterName, newCluster:null}` with the cluster's real UUID. Both are required by the working reference job shape.
- Leave `source` field on tasks **null/omitted** — don't set `"WORKSPACE"`.

## Gotchas (the ones that bit us)

1. **CircuitBreaker on malformed POST /jobs** — a single 500 response trips a 15-min cooldown on the workspace's workflow service. Validate the body client-side before sending; never retry-loop on `InternalError`.
2. **Notebook path encoding** — `urllib.parse.quote(path, safe="")` so slashes become `%2F`. Path-style, not query-style.
3. **GET on a notebook needs `?type=notebook&content=1`** — bare GET returns 500.
4. **Directory listing returns 500** for everything I tried — don't.
5. **`taskRunKey` lives in `taskToTaskRunMap[<taskKey>]`** — research doc was silent. For single-task jobs, `next(iter(run["taskRunSummaryMap"]))` also works.
6. **`fetchOutput` `outputKey: ""`** — empty string, NOT `"main"`. The 404 it returns for wrong values misleadingly cites the `taskRunKey`, not the bad `outputKey`.
7. **Output payload at `data[0].value`** — NOT `data[0].content`. The executed notebook is a JSON string at `.value`.
8. **`oidlUtils.notebook.exit(...)` is unreliable on `fusion_autopilot_dev`** — module not installed there. Use the stdout-marker pattern instead: notebook prints `<MARKER_BEGIN> <json> <MARKER_END>`, REST caller walks `cells[*].outputs[*]` for it.
9. **Poll loops must tolerate `ReadTimeout`** — the polling endpoint occasionally times out on a single request. Retry after a short sleep; don't fail the whole dispatch.
10. **Plain `requests` returns raw API body** — no `{status, data}` envelope (unlike `oci raw-request` from the CLI which wraps it). Top-level keys are the resource fields directly.

## Source of truth

The full empirical research log lives at `dev/RESEARCH_aidp_rest_api_probe_results.md` (Phases 1-4 confirmed against `<aidp-instance>` / `playground` / `fusion_autopilot_dev`, 2026-05-17). When this skill's behavior diverges from that doc, the doc updates. When the doc diverges from Oracle's swagger, the doc wins because it was verified against live infrastructure.
