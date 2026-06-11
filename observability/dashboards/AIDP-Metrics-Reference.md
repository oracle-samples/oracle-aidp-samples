# AIDP Monitoring — Metrics Reference

What each metric on the AIDP dashboards (Grafana **and** the native OCI Management Dashboard) means,
its unit, and **at what level it is measured** (driver / executor / cluster / job / agent). Meanings
reflect the metric semantics, the public [OCI Data Flow Metrics](https://docs.oracle.com/en-us/iaas/data-flow/using/metrics.htm)
documentation, and the [OpenTelemetry GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/).

---

## Overview — three metric families

| Family | Section | Namespace | Source | `resourceId` is… | AIDP-instance dimension |
|---|---|---|---|---|---|
| **Spark / compute** | Data Platform | `oracle_aidataplatform` (current) / `oracle_datalake` (legacy) | OCI Data Flow Spark runtime | the **compute-cluster** OCID | `datalakeId` |
| **Jobs** | User Application | `oracle_aidataplatform` | AIDP workflow service | = `datalakeId` = **AIDP instance** OCID | `resourceId` (= `datalakeId`) |
| **GenAI / Agent** | AI Platform | `oracle_aidataplatform` | AIDP agent runtime (OpenTelemetry → OCI Monitoring) | the **compute-cluster** OCID | *none* — only compute cluster |

This is why the dashboards' **AIDP Instance** filter scopes **Data Platform** (`datalakeId`) and **User
Application** (`resourceId`) but **not AI Platform** — GenAI metrics carry no AIDP-instance OCID, only
the compute cluster (`computeClusterId` / `computeClusterName` / `computeClusterKey`).

---

## 1. Data Platform — Spark / compute metrics

These are **OCI Data Flow data-plane metrics**, emitted **per executor and per driver**
(`executorId = driver, 1, 2, …n` — see [Data Flow Metrics](https://docs.oracle.com/en-us/iaas/data-flow/using/metrics.htm)).
The dashboards present them per executor/driver: the Grafana Spark dashboard groups by `executorId`; the
OCI MD Spark dashboard groups by `(datalakeId, resourceName, executorId)`. Units (per the Data Flow docs):
CPU / Memory / GC / FileSystem = **Percent**; Disk / Network = **Sum of bytes** over the interval (the
dashboards apply `rate()` to show Bytes/s). `JvmHeapUsed`, tasks and shuffle are AIDP extensions beyond
the documented Data Flow set — units inferred. `TotalCompletedTasks` / `TotalFailedTasks` are emitted
**driver-only**.

| Metric | Meaning | Unit | Level |
|---|---|---|---|
| `CpuUtilization` | CPU activity — busy vs idle, % of total | Percent | Per executor/driver |
| `MemoryUtilization` | Memory in use, % of pages used over time | Percent | Per executor/driver |
| `FileSystemUtilization` | File-system utilization | Percent | Per executor/driver |
| `GcCpuUtilization` | Garbage-collector CPU utilization | Percent | Per executor/driver |
| `JvmHeapUsed` | JVM heap used | Bytes | Per executor/driver |
| `DiskReadBytes` | Disk read activity over the interval | Bytes | Per executor/driver |
| `DiskWriteBytes` | Disk write activity over the interval | Bytes | Per executor/driver |
| `NetworkReceiveBytes` | Network bytes received over the interval | Bytes | Per executor/driver |
| `NetworkTransmitBytes` | Network bytes transmitted over the interval | Bytes | Per executor/driver |
| `ActiveTasks` | Active Spark tasks | Count | Per executor/driver |
| `TotalCompletedTasks` | Tasks completed | Count | Driver |
| `TotalFailedTasks` | Tasks failed | Count | Driver |
| `TotalTasks` | Total tasks | Count | Per executor/driver |
| `shuffleTotalBytesRead` | Total shuffle read bytes | Bytes | Per executor/driver |
| `TotalShuffleWriteBytes` | Total shuffle write bytes | Bytes | Per executor/driver |

---

## 2. User Application — job metrics

Published to `oracle_aidataplatform` by the AIDP workflow service. Both carry a **`status`** dimension
(`SUCCESS` / `FAILED` / …), used by the "by status" panels. The AIDP instance is in `resourceId`.

| Metric | Meaning | Unit | Level |
|---|---|---|---|
| `JobRunDuration` | Job run wall-clock = `endTime − startTime` | Milliseconds | Job |
| `WorkspaceRunDuration` | The **same** duration value at the workspace/instance scope | Milliseconds | Workspace |

**Job Run vs Workspace Run.** Both metrics record the **identical** run duration (`endTime − startTime`);
`JobRunDuration` is dimensioned by the **job** (`jobName` / `jobId` / `status`), while
`WorkspaceRunDuration` represents that run at the **workspace / instance** scope (`resourceId` =
instance OCID). Because the value is identical and `JobRunDuration` carries the richer per-job breakdown,
the **dashboards keep only `JobRunDuration`** (Run Duration, Duration by Status, Run count by Status).

---

## 3. AI Platform — GenAI / agent metrics

Emitted by the AIDP agent runtime as OpenTelemetry instruments and exported to OCI Monitoring. They are
dimensioned by compute cluster (`computeClusterName` / `computeClusterId` / `computeClusterKey`) and by
`agentFlowKey` / `deploymentId` / `sessionId`. The dashboards label them by compute (name) with a Compute
Key dropdown to disambiguate, and (Grafana) an Agent Flow filter.

| Metric (dashboard name) | Meaning | Unit | Level |
|---|---|---|---|
| `total_tokens` | Total tokens (input + output) | Tokens | Agent |
| `total_tokens_input_to_llm` | Cumulative input tokens to the LLM | Tokens | Agent |
| `total_tokens_output_from_llm` | Cumulative output tokens from the LLM | Tokens | Agent |
| `total_tokens_input_to_agent` | Cumulative input tokens to the agent | Tokens | Agent |
| `total_tokens_output_from_agent` | Cumulative output tokens from the agent | Tokens | Agent |
| `gen_ai_api_success_counter` | Successful GenAI inference calls | Count | Agent |
| `gen_ai_api_failure_counter` | Failed GenAI inference calls | Count | Agent |
| `prompt_tool_success_counter` | PromptTool success count | Count | Agent / tool |
| `rag_tool_success_counter` / `rag_tool_failure_counter` | RAGTool success / failure count | Count | Agent / tool |
| `sql_tool_success_counter` / `sql_tool_failure_counter` | SQLTool success / failure count | Count | Agent / tool |
| `sql_tool_connection_pool_success_counter` / `…_failure_counter` | SQL connection-pool acquire success / failure | Count | Agent / tool |
| `total_sessions` | Distinct chat sessions | Count | Agent |
| `sessions_duration` | Session duration | ms *(inferred)* | Agent |
| `request_count` | Per-endpoint request count | Count | Agent |
| `endpoint_latency` | Agent endpoint latency (mean / min / max / p50 / p95) | ms *(inferred)* | Agent |
| `llm.requests` | LLM request count | Count *(inferred)* | Agent |
| `llm.tokens.cached` | Cached (prompt-cache) tokens | Tokens *(inferred)* | Agent |
| `llm.tokens.reasoning` | Reasoning tokens | Tokens *(inferred)* | Agent |
| `gen_ai.client.operation.duration` | GenAI client operation duration — OTel histogram | **seconds** | Agent |

> `llm.requests`, `llm.tokens.cached`, `llm.tokens.reasoning` and `gen_ai.client.operation.duration`
> follow the OpenTelemetry GenAI semantic conventions (emitted by the runtime's instrumentation); their
> meanings/units above are taken from that spec. `total_tokens` is the sum of the input + output token
> counters. Items marked *(inferred)* are not unit-annotated at the source.

> **Region / version availability.** Not every AI-Platform metric is emitted by every runtime version or in
> every region. In particular the **agent-scope token counters** (`total_tokens_input_to_agent`,
> `total_tokens_output_from_agent`) and some **`*_failure_counter`** series (e.g.
> `sql_tool_connection_pool_failure_counter`) may be absent in a given tenancy/region. The dashboards chart
> the full set for completeness; an absent metric renders **no data** rather than an error.

---

## 4. Dimensions

| Dimension | Meaning |
|---|---|
| `datalakeId` | **AIDP instance OCID.** Present on Spark/compute metrics; for jobs the instance OCID is in `resourceId`. |
| `resourceId` | **Jobs:** = `datalakeId` = AIDP instance OCID. **Spark/compute & GenAI:** the compute-cluster OCID. |
| `resourceName` | Human-readable compute/cluster name (not guaranteed unique). |
| `clusterKey` | Unique key of a Spark compute cluster (use to disambiguate same-named clusters). |
| `computeClusterId` | Compute-cluster OCID (AI compute). |
| `computeClusterKey` | Unique key of an AI compute (use to disambiguate same-named computes). |
| `computeClusterName` | Display name of the AI compute (not guaranteed unique). |
| `executorId` | Spark executor identifier — `driver` for the driver, `1, 2, …n` for executors. |
| `agentNode` | Identifies an AI-compute replica. |
| `agentFlowKey` | Agent-flow identifier; primary GenAI filter dimension. |
| `deploymentId` | Agent-flow deployment key. |
| `sessionId` | Chat-session identifier. |
| `jobId` | Unique job identifier (use to disambiguate same-named jobs). |
| `jobName` | Job display name (not guaranteed unique). |
| `status` | Job/run lifecycle status (`SUCCESS` / `FAILED` / …). |
| `tenantId` | Customer tenancy OCID. |

`compartmentId` (and `availabilityDomain`) are common dimensions on the published metrics.

---

## 5. Driver vs executor

The Spark/compute metrics are **OCI Data Flow data-plane metrics** and carry an **`executorId`**
dimension: per the [Data Flow Metrics docs](https://docs.oracle.com/en-us/iaas/data-flow/using/metrics.htm),
`executorId = driver` is the Spark **driver** and `1, 2, …n` are the **executors**. So the data is
available per executor and per driver (verified live — `CpuUtilization` and the other compute metrics
carry `executorId`).

The Spark dashboards group by `executorId` (Grafana) / `(datalakeId, resourceName, executorId)` (OCI MD)
to show **one line per executor/driver**. To break out per executor in your own queries, group on it,
e.g.:

```
CpuUtilization[1m]{resourceId = "<computeClusterOcid>"}.grouping(executorId).mean()
```

`TotalCompletedTasks` / `TotalFailedTasks` are emitted by the driver only, so they appear under
`executorId = driver`.

---

## 6. Metrics following the OpenTelemetry GenAI semantic conventions

`llm.requests`, `llm.tokens.cached`, `llm.tokens.reasoning`, and `gen_ai.client.operation.duration` are
emitted by the agent runtime's OpenTelemetry instrumentation and follow the
[OTel GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/).
`gen_ai.client.operation.duration` is a histogram in **seconds**; the `llm.tokens.*` are token counts;
`llm.requests` is a request count. (`total_tokens` is not a single emitted metric — it is the sum of the
input + output token counters.)
