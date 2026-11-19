# REST engine map — `oci raw-request` (control-plane fallback)

> **Engine precedence:** the **preferred** control-plane engine is the official `aidp` CLI
> ([`aidp-cli-map.md`](aidp-cli-map.md)). `oci raw-request` (this doc) is the **self-contained fallback** —
> same `aidp.<region>` REST API + auth, used when the CLI isn't installed or doesn't expose the op.

This map backs the fallback path: every discovery / catalog / cluster / job / governance skill runs
via `oci raw-request` (auth + base URL in [`oci-raw-request.md`](oci-raw-request.md)) — **no MCP and no
`ai-data-engineer-agent` repo required**. Interactive **Spark-SQL** runs via the bundled
[`$HOME/.aidp/aidp_sql.py`](../aidp_sql.py) helper (see below). An `aidp` MCP, if one happens to be
configured, is an **optional accelerator** only — never assumed.

> **LIVE-VERIFIED 2026-06-09** (tenancy `<TENANCY>`, us-ashburn-1, `oci raw-request --profile DEFAULT`,
> `20240831/dataLakes`). Endpoints below returned the noted status with the api_key DEFAULT profile and
> **no MCP** in the path.

## Skill → REST endpoint (verified ✅ / shape-TBD ⚠️)
Base: `https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<dataLakeOcid>/…`
(The "MCP tool" column names the optional-accelerator equivalent, for reference only.)

| Skill | REST endpoint (primary engine) | MCP equivalent (optional) |
|---|---|---|
| workspace-admin / overview | ✅ `GET /workspaces` | `list_workspaces` |
| catalog-init / explore | ✅ `GET /catalogs` | `list_catalogs` |
| catalog-init / explore | ✅ `GET /schemas?catalogKey=<cat>` | `list_schemas(catalog)` |
| catalog-init / explore | ✅ `GET /tables?catalogKey=<cat>&schemaKey=<cat.schema>` | `list_tables(cat,schema)` |
| catalog-explore | ⚠️ list via `tables?…` then filter by key client-side (single-table filter param TBD) | `get_table(key)` |
| catalog-init / extractor | ✅ `GET /extractors` (200, 2026-06-12; auto-populate catalog from Object Storage — NOT `/metadataExtractors`. + `/extractors/<key>/extractedEntities`, `/extractedTables/<name>`, `actions/manageExtractedEntities`) | (REST only) |
| cluster-ops | ✅ `GET /workspaces/<ws>/clusters` (or `GET /clusters`) | `list_clusters` |
| cluster-ops | ⚠️ `GET /workspaces/<ws>/clusters/<key>` (probe) | `get_cluster_status` |
| cluster-ops | ✅ `POST /workspaces/<ws>/clusters/<key>/actions/start\|stop\|restart` with body `{}` → `202` (LIVE-VERIFIED; no body → `400 "request body must not be null"`; re-start while STARTING → `409`). Use the cluster's home workspace. | `start/stop/restart` |
| pipelines | ✅ `GET /workspaces/<ws>/jobs` (**paginated: 100+/page — follow `opc-next-page`; confirm 2xx+JSON before concluding "empty"**) | `list_jobs` |
| spark-debugging | ✅ `GET https://gateway.aidp.<region>…/sparkui/<clusterKey>/api/v1/applications/…` (200, 2026-06-12; jobs/stages/`taskSummary` quantiles/executors/sql — control-plane alt to kernel-side `uiWebUrl`) | (REST only) |
| pipelines | ⚠️ `POST /workspaces/<ws>/jobs/<key>/actions/run`; `GET …/jobRuns…` (probe shapes) | `run_job` / `get_job_run` / `list_task_runs` / `get_task_run_output` |
| roles-access | ✅ `GET /roles` | `list_roles` |
| data-sharing | ✅ `GET /shares`, `GET /recipients` | (REST only) |
| models-catalog | ✅ `GET /models?modelType=<GENERATIVE_AI|BASE|EMBEDDING|LLM>` | (REST only) |
| volumes | ⚠️ `GET /volumes?catalogKey=…` (400 — param shape TBD) | `list_volumes` |
| observability | ⚠️ `GET /asyncOperations?…` (param shape TBD) | `list_async_operations` |
| workspace-files | ⚠️ workspace contents REST (probe; binary needs SDK/PAR) | `list_files` / `upload_file` |

> Per-endpoint params (`catalogKey`, `schemaKey`, …) are required — a bare path returns
> `400 InvalidParameter: query param X must not be null`, which tells you the missing param.

## Interactive Spark SQL: the bundled `$HOME/.aidp/aidp_sql.py` helper
Spark-SQL cells run on a Spark kernel over **WebSocket** (Jupyter v5.3) — a protocol plain `oci
raw-request` (HTTP) can't speak. The plugin ships its own helper for this, so no MCP is needed:

```
python "$HOME/.aidp/aidp_sql.py" \
  --region <r> --datalake <DATALAKE_OCID> --workspace <ws> \
  --cluster <cluster-key> --code "spark.sql('SELECT 1').show()" \
  [--profile DEFAULT] [--session-profile AIDP_SESSION] [--timeout 180]
```

**LIVE-VERIFIED on tpcds:** the helper mints a short-lived UPST from the api_key DEFAULT profile, auto-creates
a scratch notebook, runs the cell, and returns JSON (`{status, execution_count, outputs, spark_job_ids,
error}`). No `AIDP_SESSION` required; `--session-profile` is optional (use it only if you'd rather supply a
pre-minted `oci session authenticate` token). It depends only on public packages (`oci`, `requests`,
`websocket-client`, `cryptography`) — it does **not** import `aidp_agent` or need the
`ai-data-engineer-agent` repo. Use a `SELECT 1` cell as the trivial smoke test.

Skills that use the helper: `aidp-analyzing-data`, `aidp-profiling-tables`, `aidp-data-quality`,
`aidp-ai-sql`, `aidp-verified-queries` (validation), `aidp-federate`.

### Secondary option: job-based SQL (async, not interactive)
For batch/long-running SQL, or when WebSocket egress is unavailable, run SQL as a Job instead of the
interactive helper:
1. Write a notebook that runs the SQL and writes its result to a volume / object storage
   (`POST` workspace contents, or upload via PAR).
2. Create a Job for it: `POST /workspaces/<ws>/jobs` (NOTEBOOK_TASK), then
   `POST …/jobs/<key>/actions/run`.
3. Poll the run (`GET …/jobRuns/<id>`), then read the task output / the result file.
This is pure `oci raw-request` but **async and heavier** than `$HOME/.aidp/aidp_sql.py`. Prefer the helper
for interactive work.

## Bottom line
- **Control plane** (discovery, catalog, clusters, jobs, roles, sharing, models, governance) → primary
  engine is `oci raw-request`. Works today, self-contained, no MCP.
- **Interactive Spark-SQL** → bundled `$HOME/.aidp/aidp_sql.py` helper (live-verified). Job-based path is the
  async secondary option.
- An `aidp` MCP is an optional accelerator only — never required, never assumed.
