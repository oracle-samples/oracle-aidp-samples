# Memory model & spill (Apache Spark 3.5.0)

Routes here: Spark UI shows **Spill (Memory)** / **Spill (Disk)**, lost executors, OOM, or high `jvmGcTime`; a stage runs slower than its input size implies; gzip/CSV ingest under-uses RAM.
**Core idea:** execution memory is a *fraction of a fraction* of the worker. Spill is execution running out of that fraction and paging to disk — a silent runtime multiplier. The biggest lever is `spark.memory.fraction` (give execution more of the heap), but on AIDP it is **[cluster-create]** — a notebook `spark.conf.set` on it raises `AnalysisException [CANNOT_MODIFY_CONFIG]` (set it in the cluster config + restart).
**Sizing math (how much RAM per task / cluster shape) lives in `cluster-sizing.md` — cross-referenced below, not duplicated here.**

---

## How worker RAM becomes execution memory

Each layer shrinks what execution actually gets. Walk it top-down for a 64 GB worker:

```
worker RAM (e.g. 64 GB)
  → spark.executor.memory ≈ 0.84 × worker        (rest = overhead, PySpark proc, off-heap)
      → 300 MB RESERVED                            (Spark internal; cannot be touched/changed)
      → (heap − 300MB) split by spark.memory.fraction:
          ├─ Unified region M = memory.fraction (0.6)  → EXECUTION + STORAGE
          │     └─ within M, storageFraction (0.5) is the eviction-protected
          │        storage floor; the rest floats between execution & storage
          └─ (1 − memory.fraction) = 0.4              → USER memory
                (your objects: broadcast vars, UDF state, hashmaps, arrays)
```

Net effect for execution: `~0.84 (exec mem) × 0.6 (fraction) ≈ 0.5` of worker RAM. **On a 64 GB worker, peak execution memory caps near ~30 GB** — exactly what a field engagement observed (peak execution 30.3 GB on a 64 GB worker) even though 64 GB was allocated. The "missing" ~34 GB sat in overhead + the 40% user region that the job never used.

**Unified model (Spark 1.6+):** execution and storage share region M. **Execution can preempt storage** (evict cached blocks to disk to make room); **storage cannot preempt execution.** So if a job both caches and computes heavily, raising `memory.fraction` to near-1.0 starves nothing on the execution side but can get cached blocks evicted — keep headroom (see anti-pattern below).

| Region | Config | OSS 3.5.0 default | Where to set on AIDP |
|---|---|---|---|
| Reserved | (fixed) | 300 MB | non-modifiable (Spark internal) |
| Unified M (exec+storage) | `spark.memory.fraction` | 0.6 | **[cluster-create]** (restart) |
| Storage floor within M | `spark.memory.storageFraction` | 0.5 | **[cluster-create]** (restart) |
| User region | `1 − spark.memory.fraction` | 0.4 | derived from above **[cluster-create]** |
| Heap total | `spark.executor.memory` | 1g (AIDP ≈ 0.84 × worker) | [cluster-create] (worker size) |
| Off-heap | `spark.memory.offHeap.enabled` / `.size` | false / 0 | **[cluster-create]** (restart) |
| Cores/tasks per exec | `spark.executor.cores` | 1 (AIDP = 2 × OCPU) | **[non-modifiable]** |

> WARN: `spark.memory.*`, `spark.memory.offHeap.*`, and `spark.serializer` are startup/cluster configs. A notebook `spark.conf.set("spark.memory.fraction", 0.75)` **raises `AnalysisException [CANNOT_MODIFY_CONFIG]`** (verified live, Spark 3.5.0) — the value is read at executor JVM launch and cannot change at session time. Read effective values with the Spark UI **Environment** tab or `spark.sparkContext.getConf().get(...)` (plain `spark.conf.get` can itself throw for a core conf not explicitly set), and set it in the cluster's Spark config at creation + restart. See `config-matrix.md`.
> WARN: shared cluster = one SparkSession. Even for notebook-settable configs, a `spark.conf.set` leaks to other notebooks; revert with `spark.conf.unset`. Restart resets everything to cluster config.

---

## Spill: detection & impact

**What:** when execution needs more than its share of M, Spark serializes data and writes it to local disk ("spill"), then reads it back. Correctness is preserved; speed is not.

**Why it matters:** spilled bytes are written *and* re-read; with serialization/compression overhead the disk volume can dwarf the in-memory footprint. Tungsten's in-memory layout is far more compact than the spilled form — Spark can hold much more data in memory than the same data costs once spilled. Killing spill removes that round-trip entirely.

**Detect in Spark UI:** Stage detail → **Spill (Memory)** and **Spill (Disk)** columns (aggregate + per-task in the task table). Per-executor: compare **Peak Execution Memory** against the heap — if peak is far *below* the heap yet you see spill, the cap is `memory.fraction`, not RAM. Cross-ref `diagnosis.md` for reading task quantiles.

**Patterns (use when):**
- Spill (Memory)/(Disk) non-zero on a wide stage (shuffle, sort, aggregate, gzip-CSV ingest).
- Peak Execution Memory plateaus well under the heap while spill is large → the `0.6` fraction is the ceiling, not physical RAM.
- Job is purely read→transform→write with **no user data structures** (no large broadcast vars, no UDF-side collections) → the 40% user region is dead weight.

**Anti-patterns (avoid when):**
- Heavy caching/`persist` in the same job → execution preempts storage; pushing `memory.fraction` to 0.9 leaves storage almost no protected floor and cached blocks get evicted to disk. Keep headroom (e.g. stay ≤0.7) and/or set a higher `storageFraction`.
- Real user-side allocations (big broadcast tables, Python objects, large UDF state) → they live in the 40% user region; shrinking it (high `memory.fraction`) risks user-side OOM.
- Sub-second / small jobs → spill round-trip is noise; don't tune.

---

## Raise `spark.memory.fraction` to eliminate spill

**What:** give execution a bigger slice of the heap by raising `memory.fraction` above 0.6 — viable precisely when the 40% user region is unused.

**Why it matters:** execution memory ≈ `0.84 × fraction × worker`. At 0.6 on a 128 GB worker only ~63 GB reaches execution+storage; at 0.8 it climbs to ~86 GB. If peak execution demand sits between those, the bump converts spill into in-memory work. (Going past ~0.8 starts eating the JVM-overhead/off-heap buffer and risks OOM — keep 0.8 as the cap.)

**Apply:**
```python
# before (the smell): notebook tries to fix spill at session time -> AnalysisException
spark.conf.set("spark.memory.fraction", 0.75)  # raises [CANNOT_MODIFY_CONFIG]; read at JVM launch

# after (the fix): set it in the cluster's Spark config at creation, then restart.
#   Cluster Spark config:
#     spark.memory.fraction 0.75
#   Then re-run the SAME workload and re-check Spill (Memory)/(Disk).
# In the notebook you only VERIFY it took effect (getConf, not spark.conf.get):
assert spark.sparkContext.getConf().get("spark.memory.fraction") == "0.75"   # else cluster config wasn't applied
```
Config: `spark.memory.fraction` = default `0.6` → suggested `0.7–0.75` (general-purpose; field default that "generally worked"); **cap `0.8`** even for a pure no-user-structure ingest; `0.65` to shave a small residual spill. **`0.9` is aggressive** — it shrinks the JVM-overhead/off-heap buffer and can cause OOM, so use it only with measured headroom **[cluster-create]**

**Evidence:**
- Field engagement (gzip-CSV ingest, 64 GB workers): at `fraction=0.6`, peak execution capped ~30 GB, **Spill (Memory) 915.5 GiB / Spill (Disk) 494.8 GiB** cluster-wide, wall 18m25s. Re-ingest on 128 GB workers with `fraction=0.9`: **no spill**, peak execution 88 GB, wall 15m35s → **~15–20% faster** on ~200 GB-class input. (0.9 was used for that *pure no-user-structure* ingest; for general use prefer 0.7–0.75 / cap 0.8 — 0.9 trims the overhead buffer and can OOM.)
- Same lever, larger data: avoiding spill on **2 TB** gzip ingest gave **~3×** improvement (limited memory-per-core control means you over-provision RAM; for latency-sensitive jobs that trade is worth it).
- Another field pipeline: `memory.fraction 0.6 → 0.7` ("give more memory for execution and storage") was part of the config set that took a 56 min baseline under a 40 min runtime; **0.7 generally worked**, more aggressive values did not reliably help.

> `spark.memory.fraction` can be set at compute creation but **cannot be changed afterwards** on a live cluster. `spark.executor.cores` (= 2 × OCPU) **cannot be altered**.

**Detect in Spark UI:** Spill columns drop to "-" after the change; Peak Execution Memory rises toward (but stays under) the new execution ceiling. Cross-ref `diagnosis.md`.

---

## gzip empirical rule: execution memory ≈ 4× compressed file size

**What:** for gzipped-CSV ingest, plan execution memory at roughly **4× the compressed file size per file in flight** (gzip is unsplittable → one file = one task; see `03-file-layout-io.md`).

**Why it matters:** gzip is unsplittable, so each task owns a whole file and must hold its *uncompressed* expansion. Field measurement: an executor running 8 tasks on 2.7 GB files each hit ~88 GB peak execution ≈ 11 GB/file ≈ **4× the 2.7 GB compressed size**. Uncompressed input needs less execution memory (no expansion) and *can* be split across tasks.

**Apply (planning, not a config):**
```
per-task execution mem  ≈ 4 × compressed_file_size       # gzip CSV
per-executor execution  ≈ 4 × compressed_file_size × tasks_per_executor   (tasks = spark.executor.cores)
# Worked example: 2 GB gzip files → ~8 GB/task → ~66 GB peak/executor at 8 tasks
#   (validated: 100×2GB run hit 66.4 GB peak execution, no spill).
```
Use this to choose worker RAM and `memory.fraction` so the per-executor execution ceiling covers the estimate. **The cluster-shaping arithmetic (workers × OCPU × RAM, tasks-per-executor) is in `cluster-sizing.md`.**

**Anti-pattern:** applying 4× to already-uncompressed CSV/Parquet — the multiplier is a gzip-expansion artifact; uncompressed sources need less and are splittable. The exact factor varies with file content; treat it as a ballpark.

**Evidence:** field engagement — 2.7 GB gzip → ~88 GB exec/executor (8 tasks); predicted ~66 GB for 2 GB files, measured **66.4 GB** with no spill.

---

## Avoid unnecessary `repartition` (forces shuffle → spill)

**What:** a defensive `repartition(N)` "to be safe" triggers a full shuffle, which materializes shuffle data and can itself spill — adding stages and memory pressure rather than relieving it.

**Why it matters:** `repartition` is a wide transformation (full shuffle); `coalesce` (decrease only) is narrow (no shuffle). An unmotivated repartition turns a one-stage scan into a two-stage shuffle job. (Partition-count strategy proper lives in `01-partitioning.md`; here the point is its *memory/spill* cost.)

**Apply:**
```python
# before (the smell): blind repartition before a simple ingest
df = spark.read.csv(path).repartition(24)          # full shuffle, extra stage
df.write.format("delta").save(out)

# after (the fix): drop it; let the read parallelism stand. coalesce only to MERGE down.
df = spark.read.csv(path)
df.write.format("delta").save(out)                 # no shuffle, no extra stage
```
Config: none — this is a code change. (To size output files instead, use AQE coalescing / `maxPartitionBytes` — see `01-partitioning.md`, `03-file-layout-io.md`.)

**Evidence:** field engagement on 270 GB gzip ingest — adding `repartition(24)` caused a shuffle, **2 stages, and >45 min**; removing it ran in **18m25s** *even while spilling* (the spill-bound baseline still beat the repartitioned version). The shuffle cost outweighed the spill.

**Detect in Spark UI:** an extra `Exchange` node in the SQL plan / a second shuffle stage with its own Spill columns where the logic implies a single pass.

---

## Off-heap memory

**What:** move part of execution/storage off the JVM heap (`spark.memory.offHeap.enabled=true` + a non-zero `spark.memory.offHeap.size`). Off-heap (C/native) memory is not garbage-collected by the JVM, reducing GC pressure on large allocations.

**Why it matters:** for very large in-memory footprints, on-heap data drives long GC pauses; off-heap sidesteps the collector. Tungsten can use off-heap for its compact binary format.

**Patterns (use when):** large heaps with high `jvmGcTime` despite no spill; you want to add execution capacity without enlarging the GC-managed heap.
**Anti-patterns (avoid when):** small/short jobs (added complexity, no payoff); chasing spill that `memory.fraction` already fixes — prefer the simpler lever first.

Config: `spark.memory.offHeap.enabled` = default `false` → `true`; `spark.memory.offHeap.size` = default `0` → e.g. `8g` **[cluster-create]** (both read at JVM launch; notebook `spark.conf.set` raises CANNOT_MODIFY_CONFIG — restart required).

**Detect in Spark UI:** Executors tab **On Heap / Off Heap Storage Memory**; high `jvmGcTime` (Stage task metrics) trending down after enabling. Cross-ref `diagnosis.md`.

---

## Kryo serialization

**What:** switch the serializer from the default Java serializer to Kryo (`spark.serializer=org.apache.spark.serializer.KryoSerializer`). Kryo produces faster, more compact serialized bytes.

**Why it matters:** shuffle data, spilled blocks, and serialized cached partitions are all serialized — Kryo is roughly an order of magnitude faster/tighter than Java serialization, so it shrinks spill-disk volume, shuffle write, and cache size. Complements (does not replace) raising `memory.fraction`.

**Patterns (use when):** shuffle- or spill-heavy stages; serialized caching (`MEMORY_AND_DISK_SER`); any RDD-level work with custom objects.
**Anti-patterns (avoid when):** purely DataFrame/SQL pipelines already using Tungsten's binary encoders see less benefit (Catalyst doesn't route through the generic serializer for its internal rows); the win is largest for RDD/custom-object paths. Custom classes may need `registerKryoClasses` to avoid storing full class names.

Config: `spark.serializer` = default `org.apache.spark.serializer.JavaSerializer` → `org.apache.spark.serializer.KryoSerializer` **[cluster-create]** (startup config; notebook `spark.conf.set` raises CANNOT_MODIFY_CONFIG — restart required). Tune `spark.kryoserializer.buffer.max` if you hit buffer-overflow errors.

**Detect in Spark UI:** lower Shuffle Write / Spill (Disk) byte volumes after the switch; serialized-cache RDDs smaller in the Storage tab.

---

## GC pressure (a spill-adjacent symptom)

**What:** high JVM garbage-collection time inflates task duration without doing useful work; often co-travels with under-memory and large on-heap footprints.

**Why it matters:** GC time is counted inside task time; a stage can look "slow" purely from collector pauses. Over-large executors and heavy on-heap object churn are the usual causes.

**Mitigate:**
- Don't over-size a single executor — mega-executors mean long GC; prefer more, smaller executors (sizing: `cluster-sizing.md`).
- Enable off-heap (above) and/or Kryo serialized caching to cut on-heap churn.
- Reduce shuffle/spill volume in the first place (joins → `02-joins.md`; AQE coalescing → `01-partitioning.md`).

Config: none notebook-settable here; the levers are sizing + off-heap + serializer, all **[cluster-create]**.

**Detect in Spark UI:** Stage task summary **GC Time** (`jvmGcTime`) as a large fraction of task duration; Executors tab GC Time column. Cross-ref `diagnosis.md`.

---

## Quick decision order for spill / memory

1. Confirm the symptom: Spill (Memory)/(Disk) > 0 and/or Peak Execution Memory capped below the heap (`diagnosis.md`).
2. **No user data structures?** Raise `spark.memory.fraction` (0.6 → 0.7–0.75, cap 0.8) **[cluster-create + restart]**; 0.9 risks OOM (it eats the overhead buffer). Verify via Environment tab — a notebook `spark.conf.set` raises CANNOT_MODIFY_CONFIG.
3. **Caching in the same job?** Keep headroom (≤0.7) and/or raise `storageFraction`; remember execution preempts storage, not vice-versa.
4. gzip ingest? Size execution at **~4× compressed file size per task** (`cluster-sizing.md` for cluster shape).
5. Remove unmotivated `repartition` — its shuffle can spill more than it saves.
6. Still GC-bound or huge footprint? Enable off-heap and/or Kryo (both **[cluster-create]**); favor more, smaller executors.
