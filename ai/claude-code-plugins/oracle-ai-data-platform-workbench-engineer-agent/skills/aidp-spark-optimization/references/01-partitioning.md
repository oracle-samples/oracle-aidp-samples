# Partitioning & parallelism (Spark 3.5.0)

Routes here when: weak parallelism (cores idle), too many tiny/empty partitions, thousands of micro-tasks on small data, a `repartition`/`coalesce` decision, or `spark.sql.shuffle.partitions` tuning. **Core idea:** Spark runs exactly **one task per partition** — partition count is your unit of parallelism *and* your unit of overhead. Too few = idle cores + huge tasks (spill/OOM); too many = scheduler/metadata overhead that can dwarf the data. Tune partition count to the data, not by reflex.

Default reference (OSS 3.5.0): `spark.sql.shuffle.partitions=200`, `spark.sql.files.maxPartitionBytes=128MB`, `spark.sql.files.openCostInBytes=4MB`. AQE (`spark.sql.adaptive.enabled=true`) coalesces post-shuffle partitions by default. See `config-matrix.md` for the full table; `07-aqe.md` for AQE coalescing detail.

---

## Partition fundamentals (1 task per partition; empty partitions still cost; default ≈ cores)

**What:** A DataFrame/RDD is a set of partitions; Spark launches one task per partition per stage. The number of partitions — not the number of rows — sets how many tasks run.
**Why it matters:** Empty and near-empty partitions are NOT free: each still schedules a task (serialize/deserialize, launch, metadata, result collection). On small data a bloated partition count turns into pure scheduling overhead — tiny bytes, enormous task count. The default partition count of a *manually/locally created* DataFrame ≈ the number of cores available (e.g. 12 on a 12-vCPU laptop; on the worker it is the configured cores per worker). On AIDP, `spark.executor.cores = 2 x OCPU` ([non-modifiable]), so a 16-core-per-worker setup produces 16+ partitions per locally-built DataFrame before any union.
**Patterns (use when):**
- Small lookup/metadata DataFrames built in code (one row at a time, `range`, `parallelize`, `createDataFrame`) — they inherit core-count partitioning regardless of size.
- A stage shows thousands of tasks but trivial total input bytes.
- Tasks complete in milliseconds and the stage is dominated by scheduling, not compute.
**Anti-patterns (avoid when):** Large genuinely-parallel reads — there you *want* one task per ~128MB; do not collapse them (see `repartition` vs `coalesce` below and `03-file-layout-io.md`).
**Apply:**
```python
# before (the smell): union 100 single-row DataFrames in a driver loop.
# Each single-row df has ~#cores partitions → final df = 100 x #cores partitions
# (mostly empty). On a 16-core worker that is ~1600+; in one field engagement it
# was ~30,000 partitions, and every metadata .head() scanned them all.
meta_df = spark.createDataFrame([first_row])
for r in next_rows:                       # 100 iterations
    meta_df = meta_df.union(spark.createDataFrame([r]))

# after (the fix): build a single list of Row objects, create ONE DataFrame.
# Partition count drops to the default (≈ #cores, e.g. 12) with no empties.
from pyspark.sql import Row
row_list = [Row(**r) for r in all_rows]   # accumulate in driver memory (tiny)
meta_df = spark.createDataFrame(row_list)
```
**Evidence:** Field engagement — a metadata DataFrame built by 100 single-row `union`s carried ~1200 partitions locally (mostly empty, the rest one row each) and ~30,000 partitions on the cluster; each `.head()` on it took ~3s locally / ~30s on-cluster. Collapsing to a single `createDataFrame(row_list)` dropped the per-lookup time from ~3s to <0.1s. Combined with caching + folding two actions into one (below), the overall pipeline runtime fell **~67%** (≈6h → ≈2h). See the driver-loop fan-out section next.
**Detect in Spark UI:** Stage with task count >> any sane bytes/128MB; tasks with ~0 input rows; total stage input KB-scale but hundreds/thousands of tasks. Check the DataFrame directly with `df.rdd.getNumPartitions()`.

---

## Too-many-small-partitions → driver-loop metadata fan-out

**What:** A small DataFrame carrying far more partitions than rows, read repeatedly (often inside a driver-side loop with `.head()`/`.collect()`/`.count()` per iteration). Each action re-scans every partition — including the empties.
**Why it matters:** Cost scales with *partitions x actions*, not data size. With 30,000 partitions and one `.head()` per table across 400 tables, that is millions of trivial tasks. The data is tiny but the driver and scheduler are saturated launching and reaping empty tasks. This is the partition-count tax compounding with a per-iteration action.
**Patterns (use when):**
- A driver loop iterates over a small "catalog"/"metadata" DataFrame and calls an action each pass.
- Multiple actions read the same unchanging small DataFrame.
- The DataFrame is the product of repeated `union`/`repartition` upstream.
**Anti-patterns (avoid when):** The DataFrame genuinely changes each iteration (then you cannot cache it as-is), or it is read exactly once (caching/repartition overhead won't pay back).
**Apply:**
```python
# before (the smell): bloated-partition df, two actions per table, in a loop.
for t in tables:
    name = meta_df.where(meta_df.t == t).select("t_name").head()       # action 1
    part = meta_df.where(meta_df.t == t).select("t_partition").head()  # action 2

# after (the fix): right-size to 1 partition + cache once + fold actions.
# repartition(1) is justified HERE: the data is tiny, reused many times, and a
# single small task beats fanning out across empty partitions.
meta_df = meta_df.repartition(1).cache()   # materialize once
meta_df.count()                            # trigger the cache fill
for t in tables:
    row = meta_df.where(meta_df.t == t).select("t_name", "t_partition").head()  # 1 action
```
  Config: no key — this is a code/layout fix. (`repartition(1)` here is a *deliberate* small-DataFrame collapse, not a general rule.)
**Evidence:** Field engagement (same case as above): caching the unchanging metadata DataFrame, repartitioning it to 1, and folding two `.head()` calls into one saved ~6 min per table; with 400 tables this was the bulk of the ~67% pipeline reduction (≈6h → ≈2h). Cross-ref `06-caching-materialization.md` for the caching + fold-redundant-actions mechanics, and `02-joins.md`/`05-codegen.md` for the other two findings in that engagement.
**Detect in Spark UI:** Many short jobs each triggered by a `head`/`collect`/`count`; the SQL tab shows the same scan repeated; per-stage task count >> rows. Cross-ref `diagnosis.md` (repeated-job pattern).

---

## `spark.sql.shuffle.partitions` (post-shuffle parallelism)

**What:** Number of partitions produced by a shuffle (join, `groupBy`, `distinct`, window, `repartition` without an explicit N). Default **200**.
**Why it matters:** This is the parallelism of *every* shuffle stage. 200 is a fixed guess that fits neither a tiny dataset (200 mostly-empty partitions = overhead + small output files) nor a multi-TB shuffle (each of 200 partitions is huge → spill/OOM, weak parallelism). Rule of thumb for a **large** shuffle: set it to **~2-3x total cluster cores** so every core gets a few waves of work, then size each partition near ~128-200MB (total shuffle bytes / target ≈ partition count). Crossing **2001** partitions switches Spark to compressed map-status tracking (more memory-efficient at high counts).
**Patterns (use when):** A large shuffle stage has too few partitions (giant tasks, spill) — raise it; or a small workload sprays 200 tiny partitions — lower it (or rely on AQE coalescing).
**Anti-patterns (avoid when):** With AQE on (default), **prefer leaving it high-ish and letting AQE coalesce down** rather than hard-pinning a low value. Hard-coding `shuffle.partitions` to a small number does **not** disable AQE, but AQE only coalesces *down* from the count you set — so a low value leaves little to coalesce and can re-introduce skew/small-file problems. Do not micro-tune it per query when AQE already adapts.
**Apply:**
```python
# before: default 200 on a multi-TB shuffle → each partition is enormous, spills.
# (no setting → 200)

# after: size to the shuffle. ~2-3x total cores for a large shuffle.
# e.g. 13 workers x 8 cores = 104 cores → ~256-312 partitions.
spark.conf.set("spark.sql.shuffle.partitions", 256)   # [notebook]
# ... run the shuffle-heavy query ...
spark.conf.unset("spark.sql.shuffle.partitions")       # revert: shared SparkSession leaks!
```
  Config: `spark.sql.shuffle.partitions` = default `200` → suggested `~2-3x total cores` for large shuffles  [notebook]
  Interplay: keep `spark.sql.adaptive.coalescePartitions.enabled=true` [notebook] so AQE merges down over-provisioned partitions toward `advisoryPartitionSizeInBytes` (64MB default). Set a high `shuffle.partitions` as the *ceiling* and let AQE find the right count. See `07-aqe.md`.
**Evidence:** General OSS guidance (Spark 3.5.0 docs + field practice): 200 is a starting default, not a target; aim ~128-200MB per shuffle partition. (Memory-driven sizing math lives in `04-memory-and-spill.md`.)
**Detect in Spark UI:** SQL/Stages tab — a shuffle stage with exactly 200 tasks regardless of data; or 200 tasks where a few are huge (spill) and most are empty. Task `p100/p50` skew points to `02-joins.md`/`07-aqe.md` instead.

> Shared-cluster warning: there is ONE SparkSession per AIDP cluster. A notebook `spark.conf.set("spark.sql.shuffle.partitions", ...)` **leaks to every other notebook** on that cluster. Always revert with `spark.conf.unset(...)` (or restore the prior value). A cluster restart resets everything to the cluster config. See `aidp-notes.md`.

---

## `repartition` vs `coalesce` vs `REBALANCE`

**What:** Three ways to change partition count. `repartition(N[, cols])` = **full shuffle** to exactly N (or hash-partitioned by cols); can increase *or* decrease, always balanced. `coalesce(N)` = **merge adjacent partitions, no shuffle**; can only *decrease*; cheap but can leave skewed/uneven partitions and reduce upstream parallelism. SQL hint `/*+ REBALANCE(col) */` = AQE-aware rebalance that splits skewed partitions and coalesces small ones to ~advisory size.
**Why it matters:** `repartition` buys balance by paying a full shuffle (write + network + read, often an extra stage). `coalesce` is free but only narrows and can starve the *producing* stage of parallelism (if you `coalesce(1)` before a wide transform, that transform runs single-threaded). `REBALANCE` is the "right-size output files without manually picking N" option when AQE is on.
**Patterns (use when):**
| Goal | Use | Cost |
|---|---|---|
| Increase parallelism / fix skew across partitions | `repartition(N)` or `repartition(N, key)` | full shuffle |
| Shrink partition count *after* a filter that emptied many (no rebalance needed) | `coalesce(N)` | no shuffle |
| Reduce output file count after a wide stage without re-shuffling that stage | `coalesce(N)` | no shuffle |
| Even out output file sizes / fix mild skew, AQE on | `/*+ REBALANCE(col) */` | AQE shuffle, skew-split aware |
**Anti-patterns (avoid when):**
- `coalesce(N)` to a *small* N *before* an expensive transform → that transform loses parallelism. Put the narrowing *after* the heavy work, or use `repartition` if you need the parallelism first.
- `repartition` "to be safe" with no measured reason — it is a full shuffle every time (see next section).
- `repartition(1)` on large data to get one output file — forces all data through one task → OOM/spill. Use `coalesce`/`REBALANCE` + file-sizing instead (`03-file-layout-io.md`).
**Apply:**
```python
# before (the smell): coalesce(1) before a heavy join → join runs on 1 task.
df.coalesce(1).join(big, "k").write...        # serialized, slow

# after (the fix): do the heavy work in parallel, narrow at the very end.
joined = df.join(big, "k")                     # full parallelism
joined.coalesce(8).write...                    # merge to 8 files, no extra shuffle

# or, AQE-on, to right-size files without picking N and with skew handling:
spark.sql("SELECT /*+ REBALANCE(k) */ * FROM joined")  # [notebook] hint
```
  Config: behaviour governed by AQE — `spark.sql.adaptive.enabled=true` + `spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB` drive `REBALANCE` target size  [notebook]. `repartition`/`coalesce` are DataFrame ops (no config).
**Evidence:** General OSS semantics (Spark 3.5.0). See the avoid-unnecessary-repartition case below for the measured shuffle penalty.
**Detect in Spark UI:** An `Exchange (rebalance/hashpartitioning)` node + an extra stage = a `repartition`/`REBALANCE` shuffle. A stage that suddenly drops to a few tasks after a `coalesce` = lost parallelism if it precedes heavy work.

---

## Avoid unnecessary `repartition` (it forces a shuffle)

**What:** Calling `repartition(N)` without a measured need. Every `repartition` is a **full shuffle**: an extra Exchange, an extra stage, and a write→network→read of the whole dataset.
**Why it matters:** The shuffle can cost more than the work it was meant to help. A "round number" `repartition` on an already well-partitioned read just adds a stage and re-distributes data for no benefit — sometimes converting a single efficient stage into two and ballooning wall time.
**Patterns (use when):** You have a *specific* reason: spreading skewed data, increasing parallelism before a heavy transform, or hash-co-locating by join key. Otherwise don't.
**Anti-patterns (avoid when):**
- Defensive/"just in case" `repartition` after a read that already parallelized correctly.
- Repartitioning to a count below your natural read parallelism (you *lose* tasks AND pay the shuffle).
**Apply:**
```python
# before (the smell): repartition(24) on a 270GB gzipped-CSV read.
# The read already had good per-file task parallelism; repartition added a full
# shuffle, created a 2nd stage, and the job took >45 min.
df = spark.read.csv(path).repartition(24)
df.write.format("delta").mode("overwrite").save(out)

# after (the fix): drop the repartition; let the read's natural partitioning stand.
df = spark.read.csv(path)
df.write.format("delta").mode("overwrite").save(out)   # ~18 min, single stage
```
  Config: none — remove the call. (If you truly need balance, prefer `/*+ REBALANCE */` with AQE so the shuffle is skew-aware and file-size-aware.)
**Evidence:** Field reproduction (2TB gzipped-CSV → Delta ingest): on 270GB of input, `repartition(24)` introduced a shuffle and a second stage and pushed the job to **>45 min**; removing it ran the same ingest in **18m 25s** (~2.5x faster) — and that 18-min run was the *unoptimized* baseline that still had memory spill. The shuffle alone was the dominant penalty. (Spill, sizing, and gzip-memory math are in `04-memory-and-spill.md` / `cluster-sizing.md`.)
**Detect in Spark UI:** An unexpected `Exchange` + extra stage right after a file scan; shuffle read/write bytes ≈ full dataset size with no join/aggregation to justify it.

---

## Read parallelism: `maxPartitionBytes` / `openCostInBytes`

**What:** Controls how the file scan packs files/splits into read partitions (= tasks). `spark.sql.files.maxPartitionBytes` (128MB) caps bytes per read partition; `spark.sql.files.openCostInBytes` (4MB) is the estimated cost of *opening* a file, used to decide whether to pack many small files into one task.
**Why it matters:** These set read-side parallelism *before* any shuffle. Default ~128MB/task is right for most data. For **large, splittable** files you may raise `maxPartitionBytes` so one task processes more (fewer tasks, fewer/larger output files — useful when small executors otherwise emit tiny files). For **many small files**, `openCostInBytes` makes the planner bin-pack them so you don't get one task per tiny file (task explosion). They only help where the format is splittable.
**Patterns (use when):** Tune up `maxPartitionBytes` to coarsen tasks on big splittable inputs; lean on `openCostInBytes` to coalesce small-file reads. Full small-file treatment is in `03-file-layout-io.md`.
**Anti-patterns (avoid when):** Raising `maxPartitionBytes` on already-large tasks that spill (you make spill worse); expecting these to help **unsplittable** inputs (gzip CSV ignores them — see next).
**Apply:**
```python
# before: 128MB/task default produced many small output files from small executors.
# after: coarsen read tasks so each does more work → fewer, larger output files.
spark.conf.set("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)  # [notebook]
spark.conf.set("spark.sql.files.openCostInBytes",   1 * 1024 * 1024)    # lower packs MORE small files/task [notebook]
# ... read + write ...
spark.conf.unset("spark.sql.files.maxPartitionBytes")   # revert (shared session)
spark.conf.unset("spark.sql.files.openCostInBytes")
```
  Config: `spark.sql.files.maxPartitionBytes` = default `128MB` → raise for big splittable reads  [notebook]
  Config: `spark.sql.files.openCostInBytes` = default `4MB` → *lower* (e.g. 1MB) packs more small files per task; raising spreads them across more tasks. Advanced — `maxPartitionBytes` is the primary coarsening lever  [notebook]
**Evidence:** Field reproduction note: "reading uncompressed files can benefit from multiple tasks (a single file can be split), but small per-file volume produces small output files — tune one task to operate on >128MB via `maxPartitionBytes` and `openCostInBytes`." Cross-ref `03-file-layout-io.md` for the small-file problem and output file sizing.
**Detect in Spark UI:** Scan stage task count ≈ file count (small-file explosion) or input/task far below 128MB; conversely, a few tasks each reading >>128MB with spill.

---

## gzip unsplittability → 1 task per file (parallelism ceiling)

**What:** Whole-file gzip (e.g. `.csv.gz`) is **not splittable** — Spark cannot break one gzip file across tasks. Read parallelism is therefore capped at **one task per file**, regardless of `maxPartitionBytes`.
**Why it matters:** Your parallelism is pinned to file *count*, not data *size* or cluster cores. To process N gzip files concurrently you need ≥ N task slots. Idle cores cannot help a job bottlenecked on too-few-large gzip files; and each task must hold/decompress a whole file (memory pressure — gzip-CSV peak execution memory ran ~4x the file size in the field; see `04-memory-and-spill.md`). This drives cluster sizing.
**Patterns (use when):** Ingesting gzipped CSV/JSON/text. Size the cluster to file count: to process P files in parallel you need P task slots = (workers x cores). On AIDP `cores = 2 x OCPU`.
**Anti-patterns (avoid when):** Do NOT `repartition` to "increase parallelism" on gzip — the read is still 1 task/file *and* you pay a shuffle (see avoid-unnecessary-repartition above). Do NOT add a separate decompress step (extra hop: write to object storage, read back) — Spark decompresses inline; let it, if the files are read by this app only.
**Apply:**
```python
# Don't fight unsplittability with repartition. Size the cluster to file count.
# Peak load = 100 gzip files in parallel → need >=100 task slots.
# With 4 OCPU workers (= 8 cores each): ceil(100 / 8) = 13 workers.
df = spark.read.csv("oci://bucket/path/*.csv.gz", header=True)
# parallelism = number of .gz files, capped by available task slots; no repartition.
df.write.format("delta").mode("overwrite").save(out)
```
  Config: no read-parallelism knob helps gzip whole-file splits (`maxPartitionBytes` is ignored across a single gzip). The lever is **cluster size** (worker count x cores) — see `cluster-sizing.md`.
**Evidence:** Field reproduction (2TB gzipped CSV → Delta): "gzip files can't be split across Spark tasks; to process 100 gzip files in parallel we'd need 100 tasks." A 13-node, 4-OCPU (8-core) cluster gave 104 slots to cover a ~100-file peak. Note: gzip *within* a Parquet file is fine — Parquet row groups (≈128MB) stay splittable, so a gzip/zstd-compressed Parquet still parallelizes. Compression-codec choice is in `03-file-layout-io.md`.
**Detect in Spark UI:** Scan stage task count == number of `.gz` files (not data/128MB); cores sitting idle while a few long tasks run; long single-task durations on a compressed-text source.

---

## JDBC source: parallel reads

**What:** `spark.read.format("jdbc")` (and the AIDP `aidataplatform`/JDBC connectors) runs as a **single task** unless you partition it — one connection pulls the whole table.
**Why it matters:** a single-threaded source read is a hard parallelism ceiling no matter how big the cluster is. Splitting the read into `numPartitions` range queries on a `partitionColumn` parallelizes it; a small `fetchsize` (driver default) also throttles throughput.
**Patterns (use when):** a numeric/evenly-distributed `partitionColumn` exists; push a `WHERE` via the `query`/`dbtable` option to read less.
**Anti-patterns (avoid when):** skewed `partitionColumn` (one task gets most rows); too many `numPartitions` overloads the source DB (open connections); no natural numeric split column.
**Apply:**
```python
(spark.read.format("jdbc")
   .option("url", url).option("dbtable", "schema.t")
   .option("partitionColumn","id").option("lowerBound",0).option("upperBound",10_000_000)
   .option("numPartitions",16)        # 16 parallel range queries
   .option("fetchsize",10000).load())
```
Config: these are **read options**, not Spark confs. See the AIDP connector skills for source-specific setup.
**Detect in Spark UI:** the JDBC scan stage runs **1 task** (no partitioning) while cores idle.

## Quick decisions

- Tiny DataFrame, many partitions/empties, read repeatedly → build via one `createDataFrame(list)`, then `repartition(1).cache()`. (`06-caching-materialization.md`)
- Large shuffle, too few/too many partitions → set `spark.sql.shuffle.partitions` ~2-3x cores [notebook], keep AQE coalescing on. (`07-aqe.md`)
- Need balance/more parallelism → `repartition` (paid shuffle, do it for a reason). Need to shrink files only → `coalesce`. AQE on + want skew/file-size-aware → `/*+ REBALANCE */`.
- About to `repartition` "to be safe" → don't; measure first (field: `repartition(24)` on 270GB = 18→45 min).
- Many small files / coarsen read tasks → `maxPartitionBytes` + `openCostInBytes` [notebook] (`03-file-layout-io.md`).
- gzip ingest slow / cores idle → it's 1 task/file; resize the cluster, don't repartition (`cluster-sizing.md`).
