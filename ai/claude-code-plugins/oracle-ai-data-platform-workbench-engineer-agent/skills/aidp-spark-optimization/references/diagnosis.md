# Diagnosis — find & measure the opportunity from the Spark UI

Core idea: **never guess.** Read stage metrics + task quantiles + the SQL plan, map the dominant symptom to a technique, then re-measure after the change. Compare the *same* metrics on *logically identical* runs.

On AIDP, collect metrics via `spark_query_via_kernel` against a **live kernel** (the gateway `spark_*` tools often 404). Spark UI history dies with the application — **collect soon after the run**, before auto-stop/restart. See `aidp-notes.md`.

## Investigation sequence

```
spark_list_sql_queries → spark_get_sql_query   (which query, plan, duration, jobs)
spark_list_jobs        → spark_get_job          (job → stageIds)
spark_list_stages      → spark_get_stage         (shuffle, spill, I/O, GC per stage)
spark_get_task_summary → spark_get_task_list     (skew: p0/p25/p50/p75/p100)
spark_list_executors                              (GC, memory, shuffle per executor)
spark_get_environment                             (confirm a config actually took effect)
```
Kernel fallback paths: `""` (app id) → `{appId}/sql/{id}` → `{appId}/jobs/{id}` → `{appId}/stages/{id}/0` → `{appId}/stages/{id}/0/taskSummary?quantiles=0,0.25,0.5,0.75,1` → `{appId}/stages/{id}/0/taskTable?start=0&length=20` → `{appId}/allexecutors` → `{appId}/environment`.

## Symptom → likely cause → reference

| Symptom in the UI | Likely cause | Go to |
|---|---|---|
| One stage dominates wall time; a **straggler task** runs far past the median (task `p100/p50 > 2x`, >5x severe) | Skew | `02-joins.md`, `07-aqe.md` |
| `SortMergeJoin` where one side is small | Missed broadcast | `02-joins.md` |
| High `memoryBytesSpilled`/`diskBytesSpilled`; high `jvmGcTime` | Under-memory / too-few partitions | `04-memory-and-spill.md`, `01-partitioning.md` |
| Lost/replaced executors mid-stage | OOM (often SHJ build / skew) | `02-joins.md`, `04-memory-and-spill.md` |
| Thousands of tiny tasks; huge `numTasks` for tiny data | Too many partitions / driver-loop fan-out | `01-partitioning.md`, `06-caching-materialization.md` |
| Scan stage huge; input is 10k+ files | Small-file problem | `03-file-layout-io.md` |
| CPU-bound; `p50≈p100`; hash-aggregate build dominates; tiny result cardinality | Wide-codegen overhead | `05-codegen.md` |
| Same DataFrame recomputed across jobs | Missing cache / re-materialization | `06-caching-materialization.md` |
| Output is thousands of small files | No AQE coalesce / over-partitioned write | `07-aqe.md`, `03-file-layout-io.md` |

## Metrics that matter

**Stage** (`spark_get_stage`, `detailed=true`): `executorRunTime` (total CPU work), `executorCpuTime`, `jvmGcTime`, `shuffleReadBytes`/`shuffleWriteBytes` (data movement), `memoryBytesSpilled`/`diskBytesSpilled` (memory pressure), `inputBytes`/`outputBytes`, `numTasks`.

**Task quantiles** (`spark_get_task_summary`) — the primary skew detector:
- **Skew ratio = `executorRunTime` p100 / p50.** >2x actionable, >5x severe.
- Compare against per-task data: if p100 runs 3x longer but processes only ~5% more data, it is a true **straggler** (skew), not just more work.
- `jvmGcTime` high at p100 = memory pressure on the hot partition; check `shuffleReadRecords`/`shuffleReadBytes` at p100 vs p50 — a few tasks reading far more records = a hot key.
- **Where in the UI:** Stages tab → the slow stage → **Summary Metrics for Tasks** quantiles (Min/25th/Median/75th/Max). `Max` Duration ≫ `Median` Duration is the straggler signature; the **Event Timeline** shows the long task bars while the rest finished early. The Executors tab confirms whether the straggler is one hot partition (skew) vs one slow executor (a sick node).

**Executor** (`spark_list_executors`): `totalGCTime`, `memoryUsed`/`maxMemory`, `peakJVMHeapMemory`, `peakOnHeapExecutionMemory`, `totalShuffleRead/Write`. Peak execution memory << allocated ⇒ memory wasted / `memory.fraction` too low (see `cluster-sizing.md`).

**SQL** (`spark_get_sql_query`): physical plan confirms join type (`SortMergeJoin` vs `BroadcastHashJoin`), `Exchange` (shuffle), `WholeStageCodegen`, scan `Batched` flag; per-node row/byte metrics.

## Reading rules (avoid these traps)

- **Stage duration = stage start→end timestamps**, NOT summed task time. Cumulative task metrics (e.g. "time in aggregation build") are summed across tasks and can exceed wall-clock.
- **Separate AQE-skipped stages** — do not count skipped stages as time spent.
- **Compare p50, not just wall time** — p50 strips skew noise and exposes per-task cost. If p50≈p100, it's uniform CPU overhead, not skew.
- **Prove equivalence before A/B** — same input, rows, columns, result count. Otherwise you're not benchmarking the change.
- **If scan + shuffle stayed flat but CPU dropped**, the win was execution-path (e.g. codegen), not I/O.
- **Verify a config took effect** — read it back (`spark.sparkContext.getConf().get(...)` or `spark_get_environment`). A `[cluster-create]` config set from a notebook **raises `CANNOT_MODIFY_CONFIG`** (it errors, not silently) — it must be set in the cluster config + restart (see `config-matrix.md`).

## The measurement loop
Detect → diagnose (plan + metrics) → propose (pick technique) → apply (code/config; mind `[notebook]` vs `[cluster-create]`) → re-run same cluster+data → compare same metrics (wall time, shuffle, spill, skew ratio, output files).
