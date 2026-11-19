# Caching, materialization & redundant work

Routes here when: the same DataFrame is recomputed across jobs; a driver loop calls `collect()`/`head()`/`count()` repeatedly; intermediates are written out only to be re-read; a `MERGE` on a huge table is the bottleneck. Core idea: **stop doing work twice** — cache multi-use data with the right layout, keep intermediates lazy, and don't use Spark as a key-value store in a driver loop.

## Cache / persist a reused DataFrame

**What:** Materialize a DataFrame in memory (and/or disk) so repeated actions reuse it instead of recomputing the lineage.
**Why:** A DataFrame consumed by ≥2 actions/branches is recomputed each time; caching pays back the recompute. Big for iterative/multi-branch DAGs.
**Patterns (use when):** same DataFrame used by ≥2 actions; iterative loops over the same base data; an expensive scan/join reused downstream.
**Anti-patterns (avoid when):** single-use DataFrame (wastes memory, evicts useful blocks); data too big for memory → eviction/spill churn; forgetting `unpersist()` (leak); upstream changed (stale cache).
**Apply:**
```python
from pyspark.storagelevel import StorageLevel
hot = enrich(base).persist(StorageLevel.MEMORY_AND_DISK)
hot.count()          # materialize once
... # multiple actions/branches read `hot`
hot.unpersist()      # release when done
```
Config: `spark.sql.inMemoryColumnarStorage.compressed`=true, `...batchSize`=10000 `[notebook]`. Large objects: `MEMORY_ONLY_SER` + Kryo (`[cluster-create]`, see `04-memory-and-spill.md`).
**Detect in Spark UI:** the same scan/shuffle stages re-running across jobs; Storage tab shows cached blocks once cache is added.

## Optimize the layout of a cached table (repartition on join key before caching)

**What:** If a large cached table is later joined on a key, `repartition(key)` **before** caching so the join doesn't reshuffle it.
**Why:** Caching does **not** avoid shuffle — a join still hash-partitions both sides on the key. If the big side is already partitioned on the key, only the small side moves.
**Patterns:** a large (100s of GB) cached table repeatedly joined on the same key.
**Anti-patterns:** key with extreme skew (repartition concentrates a hot key — handle skew first, `02-joins.md`); table not actually reused.
**Apply:**
```python
big = big.repartition("join_key").persist(StorageLevel.MEMORY_AND_DISK); big.count()
out = medium.join(big, "join_key")     # big side not reshuffled
```
**Evidence:** field engagement, ~400GB cached side — 5 min → 3 min (−40%).

## Avoid unnecessary materialization (keep intermediates lazy)

**What:** Don't write/checkpoint intermediate results you only consume once — let Spark keep them as a lazy plan ("view") and materialize **only the final output**.
**Why:** A Spark query is jobs that can run in parallel; forcing intermediate materialization serializes them and adds I/O + storage. Lazy intermediates preserve parallelism and cut I/O.
**Patterns:** multi-stage pipeline writing temp tables between steps that are read once.
**Anti-patterns:** an intermediate genuinely reused many times (then **cache** it instead — different from writing to storage); needing a checkpoint to truncate a pathological lineage.
**Apply:** chain transformations; write once at the end. Cache (don't write) if reused.
**Evidence:** field pipeline 28 min → 22 min, SLA 30 min.

## Iterative union + driver loop → build once, single action

**What:** Replace `reduce(unionByName, [one-row DFs])` + repeated `filter().collect()[0]` in a driver loop with a single `createDataFrame(Row list)`, `repartition(1).cache()`, and one action per lookup — or a plain Python dict for tiny metadata.
**Why:** (1) iterative `unionByName` builds deep lineage for a tiny table; (2) each `filter().collect()` is a separate action that replans + re-executes; (3) a manually-created DataFrame gets ~#cores partitions, so a 100-row union has ~100×cores partitions → every lookup fans out into many tiny tasks. The metadata lookup, not the data movement, dominates.
**Patterns:** a tiny metadata/lookup table; a driver loop filtering it repeatedly; multiple actions per iteration; `collect()[0]` as a lookup primitive.
**Anti-patterns:** the lookup table must participate in distributed Spark joins (keep it a DataFrame); it's genuinely large (don't collect to driver).
**Apply:**
```python
# before: union chain + 2 actions/iteration
meta = reduce(lambda a,b: a.unionByName(b), one_row_dfs)
for t in tables:
    src  = meta.filter(meta.name==t).collect()[0][0]
    part = meta.filter(meta.name==t).collect()[0][2]

# after: build once, single partition + cache, one action — or a dict
from pyspark.sql import Row
meta = spark.createDataFrame([Row(name=n, path=p, part=c) for ...]).repartition(1).cache(); meta.count()
lut  = {r["name"]: (r["path"], r["part"]) for r in meta.collect()}   # tiny → leave Spark out of the loop
for t in tables:
    src, part = lut[t]
```
Config: none new — `[notebook]` code change.
**Evidence:** AIDP reproduction, 20-table loop 2m33s → 1m02s (~2.5x); metadata lookup **80 tasks → 1 task** per action. Field: ~67% pipeline reduction (≈6h → 2h) at 400 tables / 30k partitions.
**Detect in Spark UI:** many small repeated jobs named `collect`/`head`/`take`; a tiny result (~KB) launching dozens of tasks (`01-partitioning.md`, `diagnosis.md`).

## Prefer JOIN over MERGE for full reloads / large upserts

**What:** When `MERGE INTO` on a large table is the bottleneck, rewrite the upsert as a `full_outer` join + a `CASE WHEN` column select, and write with **dynamic partition overwrite** (only touched partitions).
**Why:** `MERGE` can rewrite far more than necessary; a join + dynamic overwrite touches only the affected partitions and parallelizes cleanly.
**Patterns:** partitioned target; incremental load where the changed partition set is known/bounded; `MERGE` exceeding its SLA.
**Anti-patterns:** true row-level CDC into an unpartitioned table where MERGE's matching semantics are required; tiny deltas where MERGE is already cheap.
**Apply:**
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")   # [notebook]
joined = src.alias("inc").join(
    base.filter(f"event_hr IN ({hrs})").alias("base"), keys, "full_outer")
upsert = joined.select(*[F.expr(
    f"CASE WHEN inc.{k} IS NULL THEN base.{c} ELSE inc.{c} END AS {c}") for c in cols])
upsert.write.partitionBy("event_hr").mode("overwrite").format("delta").save(path)
```
Config: `spark.sql.sources.partitionOverwriteMode` = STATIC → `dynamic` `[notebook]` (see `03-file-layout-io.md`).
**Evidence:** field engagement, 5 h+ (killed at SLA) → 26 min.

## Related
- `UNION` of the same table → array + explode (single scan): `02-joins.md`.
- Tiny-data task fan-out & partition counts: `01-partitioning.md`.
- Cache memory vs execution memory (storage can't preempt execution): `04-memory-and-spill.md`.
