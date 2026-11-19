# AIDP operational notes (agent reference)

How to *run* the optimization loop on AIDP: collect Spark metrics, inspect workflows, change configs safely. These are AIDP platform behaviors (shareable with AIDP customers); no internal source details.

## Collecting Spark UI metrics

The gateway-routed tools (`spark_list_jobs`, `spark_get_stage`, `spark_get_sql_query`, …) often return **404** on a cluster. The reliable path is **`spark_query_via_kernel`**, which queries the Spark REST API from inside a **live kernel** (driver `localhost:4040`).

1. Need an active kernel: `nb_list_sessions` (state `idle`/`busy`). Idle sessions whose kernel was reaped return 404 — start a fresh session (`nb_create_session` on an ACTIVE cluster) if needed.
2. Resolve the app id: `spark_query_via_kernel(kernel_id, session_id, "")` (empty path → application list). The app id also prints from `spark.sparkContext.applicationId`.
3. Then: `{appId}/sql`, `{appId}/sql/{id}`, `{appId}/jobs/{id}`, `{appId}/stages/{id}/0`, `{appId}/stages/{id}/0/taskSummary?quantiles=0,0.25,0.5,0.75,1`, `{appId}/stages/{id}/0/taskTable?start=0&length=20`, `{appId}/allexecutors`, `{appId}/environment`.
4. **Collect soon after the run** — Spark UI history dies with the application; auto-stop/restart loses it.

Compute (infra) metrics: `get_cluster_metrics` (CpuUtilization, MemoryUtilization, JvmHeapUsed, shuffle/disk/network, ActiveTasks…) over a time range — per-executor series; not lost on stop.

## Inspecting workflows / jobs (poor-vs-optimised runs)

- `list_jobs` → `get_job`; `list_job_runs(job_key)` → `get_job_run` (notebook path, cluster, task→run map, durations, lifecycle).
- `get_job_run` → `list_task_runs` → `get_task_run` (status, code location, params, error trace) → **`get_task_run_output`** (the executed source **and** output, including Spark job info). Large outputs spill to a file — read in chunks.
- This is how you measure a workflow before/after: run, fetch output + Spark UI metrics on its cluster, optimise, re-run, compare.

## Changing configs safely (one SparkSession per cluster)

- A cluster has **one shared SparkSession**. A notebook `spark.conf.set(key, val)` **leaks to every other notebook/job on that cluster**. After a benchmark, **revert**: `spark.conf.unset(key)` or restore the prior value (capture `spark.conf.get(key)` first).
- `[notebook]` (runtime SQL, `spark.sql.*`) take effect immediately. `[cluster-create]`/`[non-modifiable]` (`spark.memory.*`, `spark.io.compression.*`, `spark.serializer`, `spark.executor.cores`/memory) **raise `AnalysisException [CANNOT_MODIFY_CONFIG]`** when set from a notebook (verified live, Spark 3.5.0) — set them in the cluster's **Spark Advanced Configurations** at create time (the `sparkAdvancedConfigurations` field, exposed by the Workbench UI and the SDK `create_cluster` payload) and **restart** to change them on a live cluster; read effective values with `spark.sparkContext.getConf().get(...)` or `spark_get_environment` (`spark.conf.get` may itself throw for a core conf that isn't explicitly set). `[non-modifiable]` (`spark.executor.cores` = 2×OCPU) → change via OCPU tier / worker count. See `config-matrix.md`, `cluster-sizing.md`.
- A cluster **restart resets all configs** to the cluster definition — a clean way to roll back a session's `spark.conf.set` changes.

## Rules

- **Never call `spark.stop()`** in an AIDP notebook — it kills the kernel's Spark context and breaks all later cells; the session must be restarted.
- Prefer `oci://` paths over `compute:///` for durable I/O across runs.
- For optimization tests use **`ephemeral_01`** or **`dataquality`** compute. Isolate a benchmark (own cluster or quiet window) so other notebooks' jobs and config changes don't contaminate metrics.
- Prove equivalence before A/B (same input/rows/columns/result); separate AQE-skipped stages; compare p50, not just wall time (`diagnosis.md`).
