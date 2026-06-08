# AIDP Monitoring — Metrics Reference

What each metric on the AIDP dashboards (Grafana **and** the native OCI Management Dashboard) means,
its unit, and **at what level it is measured** (driver / executor / cluster / job / agent). Definitions
are taken from the AIDP source — `datahub/` (`datahub-dp`, `datahub-ui`, `aidp-utils`, `workflow`) and
`datalake-connectivity/` — and cited to `file:line` where found. Where a metric is **not** defined in
those repos (emitted by an external runtime), that is stated explicitly rather than guessed.

> **Levels are cluster-aggregated.** The Spark/compute values are published by the underlying **OCI
> Data Flow Spark runtime** (not by AIDP code) and the dashboards query them aggregated per compute
> cluster — i.e. "**across driver and executors**" (AIDP's own UI wording). Per-executor breakdown is
> possible but not wired in by default — see [§5](#5-driver-vs-executor).

---

## Overview — three metric families

| Family | Section | Namespace | Produced by | `resourceId` is… | AIDP-instance dimension |
|---|---|---|---|---|---|
| **Spark / compute** | Data Platform | `oracle_aidataplatform` (current) / `oracle_datalake` (legacy) | OCI **Data Flow Spark runtime** (external; AIDP only reads them back) | the **compute-cluster** OCID | `datalakeId` |
| **Jobs** | User Application | `oracle_aidataplatform` | AIDP **workflow service** (`postMetricData`) | = `datalakeId` = **AIDP instance** OCID | `resourceId` (= `datalakeId`) |
| **GenAI / Agent** | AI Platform | `oracle_aidataplatform` | AIDP **agent runtime** (OpenTelemetry → OCI) | the **compute-cluster** OCID | *none* — only compute cluster |

This is why the dashboard's **AIDP Instance** filter scopes **Data Platform** (`datalakeId`) and **User
Application** (`resourceId`) but **not AI Platform** — GenAI metrics carry no AIDP-instance OCID, only
the compute cluster (`computeClusterId` / `computeClusterName`).

---

## 1. Data Platform — Spark / compute metrics

These are **OCI Data Flow data-plane metrics**, emitted **per executor and per driver**
(`executorId = driver, 1, 2, …n` — [Data Flow Metrics](https://docs.oracle.com/en-us/iaas/data-flow/using/metrics.htm)).
The **Spark dashboard is per executor/driver** (`executorId = driver, 1, 2…`). OCI MD groups by
`(datalakeId, resourceName, executorId)` and labels series by `executorId`; **Grafana groups by
`executorId` only** (its plugin v6.5.4 can't do multi-dimension `groupBy`), with the AIDP/cluster set via
the filters. Authoritative units (Data Flow docs):
CPU / Memory / GC / FileSystem = **Percent**; Disk / Network = **Sum of bytes** over the interval (the
dashboard applies `rate()` to show Bytes/s). `JvmHeapUsed`, tasks and shuffle are AIDP extensions not in
the Data Flow doc — units inferred. Note: `TotalCompletedTasks`/`TotalFailedTasks` are emitted **driver-only**.

| Metric | Meaning | Unit | Level |
|---|---|---|---|
| `CpuUtilization` | CPU activity — busy vs idle, % of total | Percent | Cluster (driver+executors) |
| `MemoryUtilization` | Memory in use, % of pages used over time | Percent | Cluster |
| `FileSystemUtilization` | File-system utilization across driver & executors | Percent | Cluster |
| `GcCpuUtilization` | Garbage-collector CPU utilization | Percent | Cluster |
| `JvmHeapUsed` | Total JVM heap used across driver & executors | Bytes | Cluster |
| `DiskReadBytes` | Disk read activity | Bytes/s | Cluster |
| `DiskWriteBytes` | Disk write throughput | Bytes/s | Cluster |
| `NetworkReceiveBytes` | Network bytes received across driver & executors | Bytes | Cluster |
| `NetworkTransmitBytes` | Network bytes transmitted across driver & executors | Bytes | Cluster |
| `ActiveTasks` | Active Spark tasks across driver & executors | Count | Cluster |
| `TotalCompletedTasks` | Tasks completed | Count | Cluster |
| `TotalFailedTasks` | Tasks failed | Count | Cluster |
| `TotalTasks` | Total tasks | Count | Cluster |
| `shuffleTotalBytesRead` | Total shuffle read bytes | Bytes | Cluster |
| `TotalShuffleWriteBytes` | Total shuffle write bytes | Bytes | Cluster |

*Source:* UI mapping `datahub-ui/.../compute/ts/clustermetrics.tsx:62-76`; descriptions
`.../nls/root/cluster.messages.js:465-485`; backend read/MQL builder
`datahub-dp/.../modelconverter/WrapperModelConverter.java:178-225` (`METRIC_QUERY_FORMAT = "%s[%s]{%s}.%s()"`,
filtered by `resourceId` = compute-cluster OCID, optional `agentNode`); spec
`.../datahub-dp-spec/.../wrapper/definitions.cond.yaml:299-318`.

---

## 2. User Application — job metrics

Emitted by the workflow service to `oracle_aidataplatform`. Both carry a **`status`** dimension
(`SUCCESS` / `FAILED` / …), used by the "by status" panels. The AIDP instance is in `resourceId`.

| Metric | Meaning | Unit | Level |
|---|---|---|---|
| `JobRunDuration` | Job run wall-clock = `endTime − startTime` | Milliseconds | Job |
| `WorkspaceRunDuration` | The **same** duration value at the workspace/instance scope | Milliseconds | Workspace |

**Job Run vs Workspace Run.** `emitPublicMonitoringData` (`JobRunPostExecutionProcessor.java:483-505`)
emits **both metrics with the identical `duration`** (`endTime − startTime`); `JobRunDuration` is
dimensioned by the **job** (`jobName`/`jobId`/`status`) while `WorkspaceRunDuration` represents that run
at the **workspace/instance** scope (`resourceId`/`resourceName` = instance OCID, intended `workspaceId`).
Because the value is identical and `JobRunDuration` carries the richer per-job breakdown, the **dashboards
keep only `JobRunDuration`** (Run Duration, Duration by Status, Run count by Status).

*Source:* names `WhitelistedMetrics.java:5-6`; computed & emitted
`JobRunPostExecutionProcessor.java:422,490-504`; emit path `JobServiceUtil.java:380-386` →
`PublicMonitoringEmitter.java:72-150` (`postMetricData`); namespace from
`workflow-service-api/config/base.conf:7`. Duration is in **ms** (`JobRunPostExecutionProcessor.java:422`).

---

## 3. AI Platform — GenAI / agent metrics

Created as OpenTelemetry instruments in `aidp-utils` and exported to OCI by the agent-runtime OTel
pipeline. Two sub-groups: **service-meter** metrics (sessions/endpoint/tokens, read back by name in
`oci_metric_names.py`) and **tool counters** (created in `aidp-utils/.../tools/*.py`). The dashboard
groups them by `computeClusterName`.

| Metric (dashboard name) | Meaning | Unit | Level | In repo? |
|---|---|---|---|---|
| `total_tokens` | Total tokens (input + output). No literal `total_tokens` metric — maps to `gen_ai_api.tokens.total` / sum of the two below | Tokens | Agent | ⚠ indirect (`llm_usage_metrics.py:66-69`) |
| `total_tokens_input_to_llm` | Cumulative input tokens to the LLM | Tokens | Agent | ✅ `oci_metric_names.py:6`; `session_service.py:371-382` |
| `total_tokens_output_from_llm` | Cumulative output tokens from the LLM | Tokens | Agent | ✅ `oci_metric_names.py:7`; `session_service.py:385-396` |
| `gen_ai_api_success_counter` | Successful GenAI inference calls (OTel `gen_ai_api.requests.success.total`) | Count | Agent | ✅ `generative_ai_inference_v2_client.py:188,241` |
| `prompt_tool_success_counter` | PromptTool success count | Count | Agent / tool | ✅ `aidp-utils/.../tools/prompt.py:122` |
| `rag_tool_success_counter` | RAGTool success count | Count | Agent / tool | ✅ `.../tools/rag.py:35` |
| `sql_tool_success_counter` | SQLTool success count | Count | Agent / tool | ✅ `.../tools/sql.py:158` |
| `sql_tool_failure_counter` | SQLTool failure count | Count | Agent / tool | ✅ `.../tools/sql.py:159` |
| `sql_tool_connection_pool_success_counter` | SQL connection-pool acquire success | Count | Agent / tool | ✅ `.../tools/sql.py:161,227` |
| `sql_tool_connection_pool_failure_counter` | SQL connection-pool acquire failure | Count | Agent / tool | ✅ `.../tools/sql.py:162,205` |
| `total_sessions` | Distinct chat sessions (count of grouped `sessionId`) | Count | Agent | ✅ `oci_metric_names.py:5`; `session_service.py:350-362` |
| `sessions_duration` | Session duration | ms *(inferred)* | Agent | ✅ `oci_metric_names.py:10` |
| `request_count` | Per-endpoint request count | Count | Agent | ✅ `oci_metric_names.py:9` |
| `endpoint_latency` | Agent endpoint latency (mean/min/max/p50/p95) | ms *(inferred)* | Agent | ✅ `oci_metric_names.py:8`; `session_service.py:426-480` |
| `llm.requests` | LLM request count | Count *(inferred)* | Agent | ❌ not in repos — see §6 |
| `llm.tokens.cached` | Cached (prompt-cache) tokens | Tokens *(inferred)* | Agent | ❌ not in repos — see §6 |
| `llm.tokens.reasoning` | Reasoning tokens | Tokens *(inferred)* | Agent | ❌ not in repos — see §6 |
| `gen_ai.client.operation.duration` | GenAI client operation duration | **seconds** (OTel) | Agent | ❌ not in repos — OTel semconv, see §6 |

> **OTel naming nuance:** the counters actually *created* in `aidp-utils` are dotted
> (`gen_ai_api.tokens.total`, `gen_ai_api.requests.success.total`, …). How `PROMPT_TOOL_SUCCESS_COUNTER`
> etc. surface in OCI depends on the OTel exporter's name mapping (not in these repos).

---

## 4. Dimensions

| Dimension | Meaning |
|---|---|
| `datalakeId` | **AIDP instance OCID.** Present on Spark/compute metrics; for jobs it is the value placed into `resourceId`. (`PublicMonitoringEmitter.java:111`, `oci_monitoring_client_util.py:51`) |
| `resourceId` | The OCI resource the metric belongs to. **Jobs:** = `datalakeId` = AIDP instance OCID. **Spark/compute & GenAI:** = the **compute-cluster** OCID used as the MQL filter. (`WrapperModelConverter.java:220`, `MetricsConstants.java:31`) |
| `resourceName` | Human-readable resource (cluster) name. For `WorkspaceRunDuration` it is set to the instance OCID. (`MetricsConstants.java:32`) |
| `clusterKey` | Workspace-object key of the compute cluster (resolved to its OCID). (`cluster/parameters.cond.yaml:4`) |
| `computeClusterId` | DataFlow compute-cluster OCID; group-by for token metrics & replica counting. (`session_service.py:370`) |
| `computeClusterKey` | Internal CP-side key for the compute cluster. (`ClusterService.java`) |
| `computeClusterName` | Display name of the compute cluster. (`ClusterService.java:570`) |
| `executorId` | Spark-runtime label identifying the executor (or driver). **Set by the Data Flow runtime, not by AIDP** — present on the raw series, used only by AIDP's UI. See §5. |
| `agentNode` | Identifies an **AI-compute replica**; optional filter / group-by for AI-compute metrics. (`WrapperModelConverter.java:51,218`; `ClusterModelConverter.java:134`) |
| `agentFlowKey` | Agent-flow identifier; primary GenAI filter dimension. (`session_service.py:343`) |
| `deploymentId` | Agent-flow deployment key; group-by for token metrics. (`session_service.py:370`) |
| `sessionId` | Chat-session identifier; group-by for session/token metrics. (`session_service.py:359`) |
| `jobId` | Job key. (`MetricsConstants.java:28`) |
| `jobName` | Job display name. (`MetricsConstants.java:30`) |
| `status` | Job/run lifecycle status (SUCCESS/FAILED/…). (`MetricsConstants.java:33`) |
| `tenantId` | Customer tenancy OCID (job dimension). (`MetricsConstants.java:26`) |
| `pid`, `userTenantId` | **Not metric dimensions** in these repos (`pid` = OS process id in tooling; `userTenantId` = CP request-header end-user tenancy for authz). |

`compartmentId` (and `availabilityDomain`) are always-emitted common dimensions on the public path
(`OCIMonitoringDimensions.java:9-21`).

---

## 5. Driver vs Executor

**Are the Spark metrics driver-level or executor-level? — Neither, as wired by AIDP: they are consumed
as a single compute-cluster aggregate.**

Evidence:
- The only Spark-metrics query builder (`WrapperModelConverter.java:178-225`) builds MQL filtered by
  **`resourceId` = compute-cluster OCID** (optional `agentNode`) and ends in an aggregation predicate
  (`.mean()`/`.sum()`/`.p80()`). There is **no `executorId` filter anywhere** in either repo.
- AIDP's own UI labels these values "**across driver and executors**" (`cluster.messages.js:469-485`).
- `executorId` appears **only in the UI**, never in any emitter/MQL/config — and AIDP doesn't publish
  these metrics at all; the **OCI Data Flow Spark runtime** does (the `CpuUtilization`-family names
  don't exist in any AIDP `*.conf`/`*.yaml` metric config).
- Replicas (for AI compute) are identified by **`agentNode`**, not `executorId`
  (`ClusterModelConverter.java:134`: `…{computeClusterId="%s"}.groupBy(agentNode).count()`).

**Is `executorId="0"` the driver?** Not confirmable from these repos — AIDP never assigns it. By Spark
convention `executorId == "driver"` is the driver and `0,1,2,…` are executors, but that mapping is owned
by the Data Flow runtime that publishes the metric.

**How to get executor-level metrics.** The `executorId` dimension **is already present on the raw series**
in OCI Monitoring (confirmed live: `CpuUtilization` carries `executorId`). Because these dashboards query
OCI Monitoring directly (not AIDP's `summarizeMetricsData`, which exposes no `executorId` parameter), you
can break out per executor by grouping on it, e.g.:

```
CpuUtilization[1m]{resourceId = "<computeClusterOcid>"}.grouping(executorId).mean()
```

(analogous to how AIDP groups AI-compute replicas by `agentNode`). The current dashboards group by
`resourceName` (cluster), which aggregates across executors — switch the grouping/add an `Executor`
breakdown panel if per-executor detail is needed. Confirm the exact `executorId` semantics
(`"driver"` vs numeric) against the live published series.

---

## 6. Referenced by the dashboard but **not defined** in the AIDP repos

These appear as OCI metric names in the tenancy but have **no definition in `datahub/` or
`datalake-connectivity/`** — they are emitted by an OpenTelemetry GenAI **semantic-convention**
instrumentation library inside the agent-runtime image (outside these repos). Meanings below are
**inferred from the OTel GenAI semantic conventions / the metric name**, not from AIDP code:

| Metric | Inferred meaning (OTel GenAI semconv) |
|---|---|
| `llm.requests` | Number of LLM requests |
| `llm.tokens.cached` | Tokens served from the prompt cache |
| `llm.tokens.reasoning` | Reasoning/thinking tokens consumed |
| `gen_ai.client.operation.duration` | Duration of a GenAI client operation — OTel histogram, unit **seconds** ([OTel GenAI metrics](https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/)) |
| `total_tokens` *(literal)* | No literal metric; equals `gen_ai_api.tokens.total` = input + output tokens |

To document these authoritatively, inspect the agent-runtime image's OTel instrumentation (the
GenAI semconv exporter), which is not part of these two repositories.

---

## Source map

- **Spark read path:** `datahub-dp/api-handler/datahub-dp-api/.../modelconverter/WrapperModelConverter.java`, `ClusterModelConverter.java`; `.../service/WrapperService.java`, `ClusterService.java`; spec `.../datahub-dp-spec/.../wrapper/definitions.cond.yaml`.
- **Spark UI/labels:** `datahub-ui/.../compute/ts/clustermetrics.tsx`, `.../nls/root/cluster.messages.js`.
- **Job emit:** `datahub-dp/workflow/.../job/sdk/JobRunPostExecutionProcessor.java`, `.../job/util/JobServiceUtil.java`, `.../job/telemetry/emitter/impl/PublicMonitoringEmitter.java`, `.../job/telemetry/util/MetricsConstants.java`, `.../model/WhitelistedMetrics.java`.
- **GenAI emit (OTel):** `aidp-utils/src/aidputils/agents/toolkit/llm_usage_metrics.py`, `.../auth/client/generative_ai_inference_v2_client.py`, `.../tools/{prompt,rag,sql}.py`, `.../observability/{metrics_util,service_metrics_util}.py`.
- **GenAI read path + names:** `datahub-dp/aidp-dp-agent/agentservice/metrics/oci_metric_names.py`, `.../utils/oci_monitoring_client_util.py`, `.../service/session_service.py`.
- **Prometheus→OCI plumbing:** `datahub-dp/workflow/.../job/emitter/PrometheusToOCIEmitter.java`, `MetricItem.java`, `OCIMonitoringDimensions.java`.
