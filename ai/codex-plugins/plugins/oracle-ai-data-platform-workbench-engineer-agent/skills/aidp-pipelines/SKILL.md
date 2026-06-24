---
name: aidp-pipelines
description: Author, schedule, run, and monitor AIDP Jobs (task DAGs of notebooks/python with cron). Use when the user wants to build a pipeline, create/update/run a Job, schedule a recurring run, check a run's status, read a task's output, or cancel a run.
---

# `aidp-pipelines` — AIDP Jobs (build, schedule, run, monitor)

Author and operate AIDP Jobs — task DAGs over notebooks/python with optional cron — and watch their runs.
**Engine precedence** (see [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md)): prefer the
official **`aidp workflow …`** CLI when installed; fall back to **`oci raw-request`** otherwise. Both hit the
same REST API with the same auth — no MCP / `ai-data-engineer-agent` repo required. **Persist every mutation
body to `.aidp/payloads/` and confirm before running** (see [references/payloads.md]($HOME/.aidp/references/payloads.md)).

> **Auth + base URL:** CLI flags `--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`;
> REST base `https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/…`. On 401/403 follow
> the auth ladder (`oci session refresh --profile AIDP_SESSION`) in `references/oci-raw-request.md`.

## Commands (CLI preferred · REST fallback)
- **Author:** `aidp workflow list-jobs` · `create-job` · `get-job` · `update-job` · `delete-job`
  (REST: `GET|POST|PUT|DELETE /workspaces/{ws}/jobs[/{key}]`).
- **Run/monitor:** `aidp workflow create-job-run` · `get-job-run` · `list-job-runs` · `list-recent-job-runs` ·
  `list-task-runs` · `get-task-run` · `fetch-output` / `export-task-run-output` · `cancel-job-run[s]` ·
  `repair-job-run` (REST: `POST …/jobs/{key}/actions/run`, `GET …/jobRuns/{runId}`, task-run output).

> **Listing reliability (LIVE-LESSON 2026-06-12):** job lists are **large and paginated** — a real workspace
> can hold **100+ jobs** (live: `playground` returned 100 on the first page). Always **paginate** (`limit`
> + the `opc-next-page` header) and **never conclude "no jobs" from a single call**. First confirm the call
> returned **HTTP 2xx with a JSON body** — a CLI/auth/network error (or a shell-quoting bug) whose output is
> parsed as an empty list is a silent false-negative that turns 100+ jobs into "0". Jobs are workspace-scoped
> and their `path`/`notebookPath` are rooted at `/Workspace/...`. See the reliability conventions in
> [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md).

## When to use
- "Build a pipeline / job", "run it daily", "trigger job X", "why/what did run Y do", "cancel run Z".

## Endpoints (`oci raw-request`, control-plane)
Author: `GET /workspaces/{ws}/jobs` (list) · `POST /workspaces/{ws}/jobs` (create) ·
`GET|PUT|DELETE /workspaces/{ws}/jobs/{key}` (read / fetch-modify-put update / delete).
Run/monitor: `POST /workspaces/{ws}/jobs/{key}/actions/run` (trigger) ·
`GET …/jobs/{key}/jobRuns` and `GET …/jobRuns/{runId}` (run status + task-to-task-run mapping) ·
task runs + output (`taskRuns` / task-run output under the run) for executed code, stdout, notebook cells.

> REST run/monitor sub-shapes (`jobRuns`, per-task output field names) are **probe-first** per
> [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) — a bare path returns a `400` naming
> the missing param. Don't present a sub-path as confirmed until a live 2xx; verify before destructive ops.

## Authoring a job — TWO-STEP create→update (LIVE-VERIFIED 2026-06-10)
**A job is created with a name-only body, then a second `update-job` call adds clusters + tasks.** A single
POST that inlines `tasks`/`displayName` is **rejected** (`400 Invalid resource name` / `Invalid Task type`).
Confirmed live on `aidp_skilltest` and via the official SDK sample `workflow_notebook_job_sample.py`.

- **Step 1 — create (name-only):** `POST …/workspaces/<ws>/jobs` with
  `{"name":"etl_daily.job","path":"/Workspace/Shared","maxConcurrentRuns":1}` → **201**, returns the job key.
  (Use `name` — a resource name, not `displayName`; names allow letters/`_`/`.` and must start with a letter.)
- **Step 2 — update (add clusters + tasks):** `PUT …/workspaces/<ws>/jobs/<key>` with
  `jobClusters` + `tasks`:
  ```json
  { "jobClusters": [ { "clusterKey": "<CLUSTER_KEY>", "clusterName": "<CLUSTER_NAME>" } ],
    "tasks": [
      { "type": "NOTEBOOK_TASK", "taskKey": "extract", "runIf": "ALL_SUCCESS",
        "cluster": { "clusterKey": "<CLUSTER_KEY>", "clusterName": "<CLUSTER_NAME>" },
        "source": "WORKSPACE", "notebookPath": "/Workspace/Shared/extract.ipynb", "dependsOn": [] },
      { "type": "NOTEBOOK_TASK", "taskKey": "load", "cluster": { "clusterKey": "<CLUSTER_KEY>", "clusterName": "<CLUSTER_NAME>" },
        "source": "WORKSPACE", "notebookPath": "/Workspace/Shared/load.ipynb", "dependsOn": ["extract"] } ] }
  ```
- **Discriminator is `type`** (`NOTEBOOK_TASK`/`PYTHON_TASK`/…), **not `taskType`**. **`dependsOn` defines the DAG.**
- **`clusterName` pitfall:** the per-task `cluster.clusterName` must be the real cluster **name**, not a UUID
  (a UUID → `WORKFLOW_EXECUTION_0049 Cluster not found`); pair it with `clusterKey`.
- Schedule: add a cron expression in the update body for recurring runs. Persist bodies to `.aidp/payloads/`.
- The official `aidp workflow create-job` / `update-job` CLI wraps these two steps.

> **Live-verified 2026-06-10 on de-agent — correction:** the Step-2 `PUT …/jobs/<key>` is a **full replace, not a
> merge** — the update body MUST re-send `name` + `path` + `maxConcurrentRuns` **alongside** `jobClusters`/`tasks`.
> Omitting `name` returns `400 InvalidParameter` ("name must not be null").

## Run & monitor
1. `POST …/jobs/{key}/actions/run` → returns a run; `GET …/jobRuns/{runId}` for status + task-to-task-run mapping.
2. `GET …/jobRuns` (filter by job key) for history; drill into a run for per-task status/type/duration.
3. Read a task run's output (executed code + stdout / notebook cells) for results & debugging.
4. To stop a running job, `POST` the run's cancel action.

> **Live-verified 2026-06-10 on de-agent — corrections (full create→run→SUCCESS→delete lifecycle):**
> - **Run trigger:** `POST …/jobs/{key}/actions/run` returns **404** on the `20240831` instance. The WORKING
>   trigger is `POST …/workspaces/{ws}/jobRuns` with body `{"jobKey":"<key>"}` → **201**. Treat `POST …/jobRuns
>   {jobKey}` as the verified trigger; `actions/run` is a probe only.
> - **Task-run output:** read via `GET …/taskRuns/{key}` (200, has `outputKey`) +
>   `POST …/taskRuns/{key}/actions/fetchOutput` (200, NOTEBOOK payload). Job-executed notebooks may persist
>   **empty cells**, so confirm results from task `state` / `stateMessage` (e.g. "Successfully executed
>   notebook…"), not from the fetched cell output.

## Recipe — run a notebook as a job, end-to-end (the official AI-skill demo flow)
This is how AIDP runs a notebook workload (job-based; the CLI/SDK does **not** execute cells interactively —
for that use `$HOME/.aidp/aidp_sql.py`). Mirrors the official Codex demo:
1. **Preconditions:** the notebook exists (`aidp-notebooks`) and the target cluster is ACTIVE (`aidp-cluster-ops`).
2. **Create the job (two-step, per "Authoring a job" above)** — persist bodies, confirm, then:
   ```bash
   # Step 1: create name-only -> returns job key
   aidp workflow create-job --instance-id <OCID> --auth api_key --profile DEFAULT \
     --body '{"name":"WeatherSummary.job","path":"/Workspace/WeatherDemo","maxConcurrentRuns":1}'
   # Step 2: update -> add jobClusters + the NOTEBOOK_TASK (type, cluster{clusterKey,clusterName}, source, notebookPath)
   aidp workflow update-job --instance-id <OCID> --auth api_key --profile DEFAULT --job-key <JOB_KEY> \
     --body '{"jobClusters":[{"clusterKey":"<CK>","clusterName":"<CNAME>"}],"tasks":[{"type":"NOTEBOOK_TASK","taskKey":"summary","cluster":{"clusterKey":"<CK>","clusterName":"<CNAME>"},"source":"WORKSPACE","notebookPath":"/Workspace/WeatherDemo/WeatherSummary.ipynb","dependsOn":[]}]}'
   ```
3. **Start the run:** `aidp workflow create-job-run --instance-id <OCID> … --body '{"jobKey":"<JOB_KEY>"}'` → returns a run key.
4. **Poll to terminal:** loop `aidp workflow get-job-run … <RUN_KEY>` every few seconds until
   `SUCCESS`/`FAILED`/`CANCELED` (PENDING → RUNNING → terminal).
5. **Fetch output + summarize:** on SUCCESS, `aidp workflow fetch-output` / `export-task-run-output` for the
   task run, and summarize the report the notebook produced (not just the run status). On FAILED → `aidp-spark-debugging`.

## Task types, retries, streaming, repair & parameters (platform-ref §10–11)
**Task `type`s** beyond `NOTEBOOK_TASK`/`PYTHON_TASK`: **If/Else** (conditional branching on a condition),
**Nested Job** (embed another job's tasks as one node), **JAR** (Scala/Java — JDK/Scala/Spark version must
match the cluster runtime). Tasks can depend on success **or failure** of a parent; tasks sharing a parent
run in parallel.

**Retry policy** (per task): `retryCount` (max attempts), `retryInterval` (wait between), `retryOnTimeout`
(retry if the task exceeds its time limit). **Streaming task:** mark the notebook/python task *Streaming* —
disables execution timeout + task dependencies, runs continuously until stopped, auto-restarts at monthly
maintenance; set **Max Concurrent Runs = 1**. Scheduling min frequency is **30 min**. Run statuses include
`SKIPPED` (prior run still active) and `TIMED_OUT`.

**Repair a failed run** (rerun only the failed/selected tasks, don't re-run the whole DAG):
```bash
oci raw-request --http-method POST --profile DEFAULT \
  --target-uri "https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/jobRuns/<RUN_KEY>/actions/repair" \
  --request-body '{ "...RepairJobRunDetails: tasks to rerun + optional Key/Value or JSON params..." }'
```
(SDK `workflow.repair_job_run`; `{{job.repair_count}}` increments per repair — confirm the body live before use.)

**Parameterization** — precedence **Job Run > Task > Job** (runtime overrides task, task overrides job; job
params are **immutable during task execution**). Reference **system parameters** with `{{…}}` in task
configs/paths: `{{job.id}}`,`{{job.name}}`,`{{job.run_id}}`,`{{job.repair_count}}`,`{{job.parameters.[name]}}`,
`{{job.trigger.type}}`,`{{job.trigger.file_arrival.location}}` (file-arrival trigger),`{{task.name}}`,
`{{task.run_id}}`,`{{task.execution_count}}`,`{{tasks.[name].result_state}}` (success/failed/skipped/…),
`{{tasks.[name].error_code}}`,`{{workspace.id}}`,`{{hub.region}}`. For passing computed values notebook→notebook
inside a task, see `oidlUtils.notebook.run/exit` in `aidp-notebooks`.

## Workflow
1. Confirm the notebook(s)/python file(s) exist (`aidp-notebooks` / `aidp-workspace-files`) and the cluster.
2. Build the task DAG (deps, schedule); show the user the JSON job spec **before** creating.
3. `POST` to create, trigger a test run, poll the run to terminal, read task output.
4. On failure, route to `aidp-spark-debugging` (logs + Spark UI) with the failing task run.
5. Clean up test jobs (`DELETE …/jobs/{key}`) when validating.

## Interactive SQL (only if a task needs a quick check)
For ad-hoc Spark-SQL outside a job, use the bundled helper — no MCP required:
```bash
python "$HOME/.aidp/aidp_sql.py" --region <r> --datalake <DATALAKE_OCID> --workspace <WS> \
  --cluster <key> --code "spark.sql('SELECT 1').show()"
```
It mints a UPST from the api_key DEFAULT profile and returns JSON (status/outputs/spark_job_ids).
Production pipeline steps belong in a notebook/python task driven by the Job, not this helper.

## References
- [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md)
- Pairs with `aidp-notebooks`, `aidp-workspace-files`, `aidp-spark-debugging`, `aidp-data-quality`.
