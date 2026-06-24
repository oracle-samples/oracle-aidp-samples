---
name: aidp-spark-debugging
description: Diagnose slow or failed AIDP Spark work using cluster logs, metrics, and the Spark UI REST API. Use when a job/query is slow or failed, the user asks "why did this fail / why is it slow", or you need stage/task timings, skew, shuffle/spill, executor, or SQL-execution details. Lightweight triage — for deep performance tuning (skew/spill/shuffle/joins/AQE/Delta) use the `aidp-spark-optimization` skill.
---

# `aidp-spark-debugging` — logs + metrics + Spark UI triage

Ground failure/slowness diagnosis in real execution data (never guess). Two engines, no MCP required:
- **Cluster logs & metrics** → `oci raw-request` POST actions on the cluster (control-plane).
- **Spark UI job/stage/task/SQL detail** → `$HOME/.aidp/aidp_sql.py` runs a kernel-side cell that hits the
  Spark **REST API** (`spark.sparkContext.uiWebUrl` + `/api/v1/applications/...`). The Spark UI is only
  reachable from inside the running kernel, so the helper is the no-MCP path for it.

## When to use
- A Spark job/query is slow or failed; "why did run X fail / why is it slow"; need stage/task/skew/shuffle detail.

## Engines

### 1. Logs & metrics — `oci raw-request` (control-plane)
Base + auth ladder in [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md). Verified on
`20240831/dataLakes`. `<WS>` = `…/dataLakes/<OCID>/workspaces/<workspace>`.

```bash
# Search logs (logContentTypeContains: driver | executor | events; ISO ms timestamps; ≤24h window)
oci raw-request --http-method POST \
  --target-uri "https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/clusters/<KEY>/actions/searchLogs" \
  --request-body '{"timeBegin":"2026-06-09T00:00:00.000Z","timeEnd":"2026-06-09T01:00:00.000Z","logContentTypeContains":"executor","messageContains":"OutOfMemory"}' \
  --request-headers '{"content-type":"application/json"}' --profile DEFAULT
# also: …/actions/downloadLogs (same body) for a downloadable archive.

# Metrics over a range (POST summarizeMetricsData)
oci raw-request --http-method POST \
  --target-uri "https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/clusters/<KEY>/actions/summarizeMetricsData" \
  --request-body '{"metricName":"MemoryUtilization","timeBegin":"…Z","timeEnd":"…Z","interval":"1m","aggregationType":"MEAN"}' \
  --request-headers '{"content-type":"application/json"}' --profile DEFAULT
```
Optional log filters: `logLevel`, `subjectContains`, `eventType`, `thread`, `executionContextId`,
`advancedFilter`. Metric names: `CpuUtilization`, `MemoryUtilization`, `GcCpuUtilization`, `JvmHeapUsed`,
`DiskReadBytes`/`DiskWriteBytes`, `NetworkReceiveBytes`/`NetworkTransmitBytes`, `ActiveTasks`,
`TotalFailedTasks`/`TotalCompletedTasks`/`TotalTasks`, `shuffleTotalBytesRead`/`TotalShuffleWriteBytes`.
`aggregationType`: `MEAN|SUM|MAX|MIN`. Confirm the exact action path against the cluster on first use
(probe), per the no-fabrication gate in `oci-raw-request.md`.

### 2. Spark UI detail — `$HOME/.aidp/aidp_sql.py` (kernel-side Spark REST)
Run a cell on the target cluster that reads the live Spark REST API. The helper mints a UPST from the
api_key DEFAULT profile (no MCP, no AIDP_SESSION):
```bash
python "$HOME/.aidp/aidp_sql.py" \
  --region <region> --datalake <OCID> --workspace <ws> --cluster <KEY> \
  --code "import json,ssl,urllib.request as u; ctx=ssl._create_unverified_context(); base=spark.sparkContext.uiWebUrl+'/api/v1'; opn=lambda p: json.load(u.urlopen(p,context=ctx)); apps=opn(base+'/applications'); app=apps[0]['id']; print(json.dumps(opn(base+'/applications/'+app+'/jobs'), default=str))"
```
> **SSL note (LIVE-VERIFIED 2026-06-10):** the cluster's Spark UI is HTTPS with a self-signed cert, so a bare
> `urlopen` raises `SSLCertVerificationError`. Pass an unverified context (`ssl._create_unverified_context()`,
> as above) — this is kernel-internal traffic to the same cluster, not an external call.
>
> **Control-plane alternative (LIVE-VERIFIED 2026-06-12) — no kernel cell needed:** the same Spark UI REST is
> proxied through the AIDP gateway at
> `https://gateway.aidp.<region>.oci.oraclecloud.com/sparkui/<clusterKey>/api/v1/applications[/<app>/{jobs,stages,stages/<id>/<attempt>/taskSummary,executors,sql,storage/rdd,environment}]`,
> reachable with the same `oci raw-request --profile DEFAULT` signing. `GET …/sparkui/<clusterKey>/api/v1/applications`
> → **200** (returns the running app + version). Use this when you want stage/task/SQL metrics without
> executing a cell (e.g. the cluster has no free kernel); the kernel-side `uiWebUrl` path above remains the
> default. Verified live: per-stage `taskSummary` quantiles (`[p50,p75,p95,p100]`) expose task skew directly.
>
> **Finished job runs are not retained on the tested cluster (no Spark History Server exposed) — LIVE-VERIFIED 2026-06-12:** the gateway
> lists only the LIVE cluster app and **404s** on a completed run's app; the Spark UI is live-only with bounded
> in-memory retention (`spark.ui.retainedJobs/Stages`). So a finished Job's stage/task/SQL detail isn't
> retrievable after the fact (job/task **durations** and cluster **metrics** via `summarizeMetricsData` still
> are — see §1). To make a Job's full Spark-UI metrics fetchable *later*, append a **snapshot cell** as the
> job's last task that captures the live REST and persists it to a Volume / Object Storage:
> ```python
> import json, ssl, urllib.request as u
> ctx=ssl._create_unverified_context(); base=spark.sparkContext.uiWebUrl+'/api/v1'
> app=json.load(u.urlopen(base+'/applications',context=ctx))[0]['id']
> get=lambda p: json.load(u.urlopen(f"{base}/applications/{app}{p}",context=ctx))
> snap={'executors':get('/allexecutors'),'jobs':get('/jobs'),'stages':get('/stages'),'sql':get('/sql')}
> spark.createDataFrame([(json.dumps(snap),)],['m']).coalesce(1).write.mode('overwrite').json('oci://<bkt>@<ns>/spark_metrics/<run>.json')
> ```
> Read it back anytime with `spark.read.json(...)`. (Or enable Spark event-log persistence / a History Server
> at the cluster level if your tenancy exposes one.)
Swap the final REST sub-path to drill in (all under `/applications/<app>/…`):
`/jobs` · `/jobs/<id>` · `/stages` · `/stages/<id>/<attempt>` · `/stages/<id>/<attempt>/taskSummary`
(p0–p100 quantiles) · `/stages/<id>/<attempt>/taskList` · `/allexecutors` · `/sql` · `/sql/<execId>` ·
`/storage/rdd` · `/environment`. Returns JSON `{status, outputs, spark_job_ids, error}` — parse the
`outputs` text. See [$HOME/.aidp/aidp_sql.py]($HOME/.aidp/aidp_sql.py).

## Triage workflow
1. **Scope:** a Job run (start with `aidp-pipelines` task-run output) or an interactive query?
2. **Find the work:** kernel-side Spark REST `/applications/<app>/jobs` (+ `?status=failed`) / `/sql` →
   the failing/slow id; then `/jobs/<id>` / `/sql/<execId>` for the plan + stage ids.
3. **Stage/task detail:** `/stages/<id>/<attempt>` (shuffle, spills, GC); `taskSummary` (p0–p100 — skew if
   max ≫ median); `taskList` for outliers.
4. **Executors / cache:** `/allexecutors` (memory, GC); `/storage/rdd` for caching.
5. **Errors:** `searchLogs` (driver/executor/events) for the exception; `summarizeMetricsData` for resource
   pressure (use ms-precision ISO timestamps; ≤24h window).
6. **Report** the root-cause signal (skew / spill / OOM / wrong cluster / data error) with the evidence, and
   a concrete next step.

## Scope note
This is triage. For deep query rewrites/tuning (wide aggregations, skewed joins, driver-side loops), defer
to the upstream **spark-performance-optimization** / **spark-ui-investigation** skills — don't duplicate
them here.

## References
- [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) · [$HOME/.aidp/aidp_sql.py]($HOME/.aidp/aidp_sql.py) · pairs with `aidp-pipelines`, `aidp-cluster-ops`
