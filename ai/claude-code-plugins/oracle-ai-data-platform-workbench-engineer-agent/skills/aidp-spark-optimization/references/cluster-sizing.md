# Cluster right-sizing (AIDP compute)

Goal: pick worker count + OCPU + RAM so the workload runs with **enough parallelism** and **no spill**, without over-provisioning. Two modes: size from a workload's shape (a priori), or re-size from an observed run (preferred — evidence-based).

## AIDP compute model (the facts that drive sizing)

- **One executor per worker** (AIDP abstracts multi-executor). "executor" ≈ "worker".
- **`spark.executor.cores` = 2 × OCPU** on AMD/Intel → **tasks per worker = 2 × OCPU**. `[non-modifiable]` — derived, not exposed.
  - ARM is cheaper per OCPU but runs **1 task/OCPU** (not 2). To match AMD/Intel task throughput you need ~2× the OCPU, so the headline ARM discount shrinks (a ~60%-cheaper ARM nets only ~20% effective saving for compute-parallel work). *(Observed on prior AIDP shapes — verify the task/OCPU ratio and the per-core RAM ceilings against current shapes.)*
- **Spark executor memory ≈ 0.74–0.84 × worker RAM** (the rest is JVM/overhead + PySpark + off-heap; varies with overhead config — live-measured `24275m` on a 32 GB worker ≈ 0.74×, ~0.84× in another engagement). Use ~0.8× as a planning estimate, then confirm from the Environment tab.
- **Execution+storage region = `spark.memory.fraction` (default 0.6) × executor memory ≈ 0.6 × 0.84 ≈ 0.5 × worker RAM.** 300 MB is reserved and untouchable.
- **Sizing is coarse** — OCPU and RAM come from fixed tiers (≈ powers of two), and you cannot set arbitrary RAM, arbitrary RAM-per-core, or cores-per-OCPU. Expect to over- or under-provision; the free knob is **worker count**. (Practical ceiling observed: up to 16 OCPU you can pair ~16 GB/core; above 16 OCPU, ~8 GB/core max.)
- Scaling is roughly linear for shuffle/CPU-bound batch work: in one field engagement, 2× resources ≈ half runtime, ½ resources ≈ 2× runtime.

## Sizing for parallelism

```
tasks_per_worker = 2 × OCPU            # AMD/Intel
workers          = ceil(target_tasks / tasks_per_worker)
```
- **Splittable input** (Parquet/ORC, uncompressed/ bzip2): `target_tasks ≈ input_bytes / spark.sql.files.maxPartitionBytes (128 MB)`. A single file can be read by many tasks.
- **Unsplittable input** (whole-file gzip CSV/JSON): **1 file = 1 task**. `target_tasks ≥ number_of_files` for full concurrency. If workers can't run all files at once, files process in waves.
- For shuffle-heavy stages, also set `spark.sql.shuffle.partitions` ≈ 2–3× total cores (see `01-partitioning.md`), independent of worker count.

## Sizing for memory (avoid spill)

```
exec_mem_per_worker  ≈ spark.memory.fraction × 0.84 × worker_RAM     # default fraction 0.6
exec_mem_per_task    ≈ exec_mem_per_worker / tasks_per_worker
need_per_task        ≈ peak execution memory one task requires
```
- **gzip rule of thumb:** decompressing+processing one gzip file needs **execution memory ≈ 4 × compressed file size** (empirical; uncompressed input needs less because it doesn't expand on read).
- Ensure `exec_mem_per_task ≥ need_per_task`. Two levers:
  1. **Raise `spark.memory.fraction`** to **0.7–0.75 (cap 0.8)** `[cluster-create — restart]` *only when there are no user data structures and little/no caching* (storage can't preempt execution, so leave headroom if you cache). **Cap at 0.8.** 0.9 eats the JVM-overhead/off-heap buffer and risks OOM — treat it as historical/exceptional (a pure no-user-structure ingest with measured headroom), not a recommendation. Raising the fraction reclaimed wasted RAM and **eliminated ~1.4 TB of spill** in a 2 TB ingest with no extra hardware.
  2. **Pick a larger RAM tier** (coarse) or **add workers** (more tasks in flight, same per-task budget).
- Spill is a silent multiplier — eliminating it gave ~15–20% on a 200 GB job and ~3× on a 2 TB job. Validate: Spark UI `Spill (Memory)/(Disk)` should be ~0 and `peakExecutionMemory` should sit under the per-task budget.

## Re-size from an observed run (preferred)

Read the Spark UI after a representative run (`spark_list_executors` + `spark_get_stage`):

| Observation | Meaning | Action |
|---|---|---|
| `Spill (Disk/Memory)` > 0 | Execution memory too small | Raise `memory.fraction` (if no user structures) or larger RAM tier |
| `peakExecutionMemory` ≪ allocated execution memory | RAM wasted | Lower RAM tier, or you have room to raise `memory.fraction` for fewer/larger files |
| Cores idle; few long tasks | Under-parallelized | More partitions / more workers / split input |
| `numTasks` ≫ data justifies; tiny tasks | Over-parallelized / small data | Fewer partitions, `coalesce`, smaller cluster |
| Task `p100/p50 > 2x` | Skew (don't just add hardware) | Fix skew first (`02-joins.md`, `07-aqe.md`) — more nodes won't help a single hot task |
| Long `jvmGcTime` on big workers | Mega-executor GC | Prefer more, smaller workers over few huge ones |

## Procedure

1. Run once on a modest cluster; collect peak execution memory, spill, task count, skew, wall time.
2. **Fix skew/anti-patterns before scaling** — hardware can't fix a straggler or a redundant shuffle.
3. Size parallelism (workers × 2×OCPU ≥ target tasks; gzip ⇒ ≥ #files).
4. Size memory (per-task budget ≥ need; raise `memory.fraction` to reclaim waste before buying RAM).
5. Re-run, confirm no spill and even task distribution. Trade extra RAM for latency only when the SLA needs it.

Configs referenced here: `spark.memory.fraction` `[cluster-create]`, `spark.executor.cores` `[non-modifiable]`, `spark.sql.shuffle.partitions`/`maxPartitionBytes` `[notebook]`. See `config-matrix.md`.

## Worked example: 2 TB gzip CSV → Delta

Tying parallelism + memory + tier together for the canonical ingest (1000 gzip files @ ~2 GB):

1. **Parallelism** — gzip is unsplittable ⇒ `target_tasks ≥ #files = 1000`. With 4-OCPU workers (`2×OCPU = 8` tasks each): `workers ≥ ceil(1000/8) = 125`; with 16-OCPU workers (32 tasks each): `≥ 32`.
2. **Per-task memory** — gzip rule ≈ `4 × file size` = `4 × 2 GB = 8 GB`/task.
3. **Check the budget** — on a 64 GB worker, execution ≈ `memory.fraction(0.6) × ~0.8 × 64 / 8 ≈ 4 GB/task` < 8 GB needed → **spill**. Fixes: raise `memory.fraction` to 0.75 (`[cluster-create]`, → ~5 GB/task) and/or move to a 128 GB tier (→ ~10 GB/task), or run fewer tasks/worker.
4. **Codec/write** — zstd for output (`spark.sql.parquet.compression.codec` `[notebook]`) + zstd shuffle/spill (`spark.io.compression.codec` `[cluster-create]`); turn on Delta `optimizeWrite` so the write lands few well-sized files (`08-delta-lake.md`).
5. **Re-run** — confirm Spill = 0 and even task durations; scale worker count to the SLA.

(The 2 TB field ingest hit ~8 min with no spill once memory and codec were sized this way — `case-studies.md` F9.)
