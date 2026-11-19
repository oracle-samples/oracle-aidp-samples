# Joins

Routes here when: a join is the slow stage; the plan shows `SortMergeJoin` (SMJ) where one side is small; task `p100/p50 > 2x` on a join stage (skew); executors lost / OOM during a join; "200GB SMJ post-Exchange with a 200MB side" smell; a big dimension joined un-filtered; repeated joins on the same key.

**Core idea:** joins cost shuffle. Kill the shuffle on the small side (**broadcast**), shrink the small side until it broadcasts (**semi-join pre-filter**), skip the sort when both sides are hash-able (**SHJ**), or break up a hot key when no side is small (**AQE skew-join / salting**). Always confirm the *result* is unchanged before comparing runtimes.

Defaults (OSS Spark 3.5.0): `autoBroadcastJoinThreshold=10MB`, `preferSortMergeJoin=true`, AQE on, `skewJoin.enabled=true` (factor `5.0`, threshold `256MB`), CBO off. Join-strategy priority when multiple are legal: **BROADCAST > MERGE (SMJ) > SHUFFLE_HASH (SHJ) > SHUFFLE_REPLICATE_NL**.

---

## Broadcast hash join (autoBroadcastJoinThreshold)

**What:** ship the small side to every executor so each fact partition joins locally — no shuffle, no Exchange, no sort.
**Why it matters:** a shuffle join repartitions *both* sides by key (the dominant cost). Broadcast eliminates the shuffle and the sort on the build side, and is immune to key skew because every partition joins independently against a local copy. The default 10MB threshold is conservative — on a large cluster the small side is often far bigger than 10MB yet trivially broadcastable.

**Patterns (use when):**
- Plan shows `SortMergeJoin` with a small/medium side (the "200GB SMJ post-Exchange with a 200MB side" smell — a 200GB fact pointlessly re-shuffled to merge a 200MB dimension).
- Executors are large. Field engagement: 128GB executors + 64GB driver → comfortably broadcast ~1GB; raising threshold 10MB→1gb cut pipeline runtime 56→48 min.
- The small side fits in **driver** memory (driver collects it first), then in each **executor**'s memory (one copy per executor).

**Anti-patterns (avoid when):**
- **Full-outer equi-join** — cannot use `BroadcastHashJoin` (no "stream one side" semantics for a full outer); raising the threshold won't convert it, the plan stays SMJ. (A broadcast nested-loop join exists for some non-equi cases, but it is not the hash-join optimization you want.)
- Build side too big → driver collects it then OOMs, or each executor OOMs holding a copy. Setting the threshold to a huge value (e.g. 4GB) sharply raises driver memory pressure; pair with `spark.driver.maxResultSize` and verify it actually broadcasts.
- Right side of a left-outer / left side of a right-outer (the "outer" side can't be the broadcast side).

**Apply:**
```python
# before (the smell): 200GB fact re-shuffled to SMJ a 200MB dim
big_fact.join(small_dim, "key")           # plan: SortMergeJoin -> two Exchanges

# after: let the small side broadcast (no shuffle on either side)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1gb")  # default 10mb
big_fact.join(small_dim, "key")           # plan: BroadcastHashJoin

# or force it per-join without changing the session default:
from pyspark.sql.functions import broadcast
big_fact.join(broadcast(small_dim), "key")
```
| Config | Default | Suggested | Where |
|---|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | size to RAM: ~`1gb` on 128GB executors (`-1` disables) | [notebook] |
| `spark.driver.maxResultSize` | `1g` | raise if broadcasting >~1GB | [cluster-create] |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | = `autoBroadcastJoinThreshold` | runtime SMJ→broadcast cap (see `07-aqe.md`) | [notebook] |

**Evidence:** field engagement — threshold 10MB→1gb gave 56→48 min ("shuffle-less joins"); broadcast worked **comfortably up to ~1GB** on 128GB executors / 64GB driver. AIDP reproduction — once the dim is filtered to 13.4MB it auto-broadcasts and the write stage drops 68.2s→45.6s.
**Detect in Spark UI:** SQL tab shows `SortMergeJoin` with an `Exchange` under each child; the small child's `data size` is far under your executor memory. After the fix it becomes `BroadcastHashJoin` + `BroadcastExchange`. Cross-ref `diagnosis.md`.

---

## Join strategy hints (surgically override the planner)

**What:** force a specific join strategy on one join without changing a session conf. Hints: **`BROADCAST`** (aliases `BROADCASTJOIN`, `MAPJOIN`), **`MERGE`** (aliases `SHUFFLE_MERGE`, `MERGEJOIN` → SMJ), **`SHUFFLE_HASH`** (→ SHJ), **`SHUFFLE_REPLICATE_NL`** (shuffle-replicate nested loop).
**Why it matters:** a `BROADCAST` hint **bypasses `autoBroadcastJoinThreshold` entirely** — you can broadcast a side larger than the threshold for *this one join* without raising the global conf (which, on a shared cluster, leaks to every other notebook — see the warning at the bottom). It's the precise alternative to bumping the session default. Priority when both sides carry conflicting hints: **`BROADCAST` > `MERGE` > `SHUFFLE_HASH` > `SHUFFLE_REPLICATE_NL`**; with the same hint on both sides Spark picks the build side by join type + relation size.
**Anti-patterns (avoid when):** the hinted strategy is illegal for the join type — e.g. `BROADCAST` on the big side of a **full-outer** equi-join is silently ignored (no guarantee Spark honors any hint); a `BROADCAST` on a side that doesn't fit memory will OOM (the hint removes the size guardrail).
**Apply:**
```python
from pyspark.sql.functions import broadcast
big_fact.join(broadcast(small_dim), "key")          # DataFrame API: force broadcast, ignores threshold
big_fact.join(small_dim.hint("broadcast"), "key")   # equivalent via .hint(...)
a.join(b.hint("shuffle_hash"), "key")               # force SHJ (skip the SMJ sort)
# SQL: SELECT /*+ BROADCAST(d) */ * FROM fact f JOIN dim d ON f.key = d.key
```
**Detect in Spark UI:** the chosen node (`BroadcastHashJoin`/`ShuffledHashJoin`/`SortMergeJoin`) reflects the hint; if the plan ignored your hint, the strategy was illegal for that join type.

---

## Shuffle-Hash Join vs Sort-Merge Join (preferSortMergeJoin)

**What:** SHJ builds an in-memory hash map of the (shuffled) build side and probes it; SMJ shuffles **and sorts** both sides then merges. Both still shuffle by key — SHJ just skips the sort.
**Why it matters:** sorting both sides is pure overhead when a hash join would do. With `preferSortMergeJoin=true` (default), Spark prefers SMJ even when SHJ is legal; flipping it to `false` lets the planner choose SHJ and skip the sort. **Caveat:** SHJ's build side must fit in the executor's partition memory — the hash map **cannot spill**. If a partition is too big (skewed key), the map OOMs and the container is killed; a replacement container takes ~3–4 min to come up *and* the lost work must be recomputed. So SHJ trades sort-time for OOM-risk under skew.

**Patterns (use when):**
- Both sides too big to broadcast but the per-partition build side fits in memory; you're paying for a sort you don't need.
- Keys are reasonably even (no single partition dominates).

**Anti-patterns (avoid when):**
- **Skewed key** — a hot partition's hash map won't fit and can't spill → executor OOM → container loss. This is exactly the failure mode seen in the field (SHJ on a 4GB/1.2GB dimension caused executor eviction via OOM during streaming micro-batches). Under skew, prefer broadcast (after a semi-join pre-filter) or AQE skew-join, not SHJ.
- A small side is broadcastable — broadcast beats both SHJ and SMJ.

**Apply:**
```python
# before: both sides shuffled AND sorted, then merged
a.join(b, "key")                          # plan: SortMergeJoin (Sort under each side)

# after: skip the sort when SHJ is legal and build side fits in memory
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
a.join(b, "key")                          # plan may now be ShuffledHashJoin
```
| Config | Default | Suggested | Where |
|---|---|---|---|
| `spark.sql.join.preferSortMergeJoin` | `true` (`.internal`/advanced) | `false` when SHJ legal and no skew | [notebook] |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | `0` | raise to let AQE pick SHJ at runtime | [notebook] |

**Evidence:** field engagement — `preferSortMergeJoin=false` moved the pipeline 44→43 min (one step in a 56→40 min sequence). Modest on its own; the big wins were broadcast + skew handling.
**Detect in Spark UI:** SMJ node shows a `Sort` under each side. If keys are even and sides aren't broadcastable, that sort is the candidate to remove. Watch for lost executors / OOM in the join stage if you enable SHJ — that means a partition's build side didn't fit (go to skew handling).

---

## Skew detection (p100/p50)

**What:** find the join stage where a few tasks run far longer than the median because their key(s) hold disproportionately more rows.
**Why it matters:** a stage finishes only when its **slowest** task finishes. One hot partition serializes the whole stage on a straggler while everyone else idles. Skew is a *task-time* problem, not a data-volume problem — the giveaway is high p100/p50 runtime with little difference in per-task data read.

**How to read it (cross-ref `diagnosis.md`, `07-aqe.md`):**
- Pull task quantiles: `executorRunTime` at `quantiles=0,0.25,0.5,0.75,1`.
- **Skew ratio = p100 / p50.** `> 2x` is actionable; `> 5x` is severe.
- Confirm it's *skew*, not just *more data*: compare per-task `shuffleReadRecords`/`inputSize` across quantiles. If the slow task read ~the same volume as the median but took 3x longer → true skew. If the slow task read 2x the data and took 2x longer → not skew (normalize time-per-record before concluding).

**Field example (streaming SMJ):** median task = 7 min reading 961K records / 53MB shuffle; p75 and p100 = 20–21 min while reading only ~5% more data — a clear straggler, not volume. More than half the tasks finished under 7 min but the stage waited 21 min on the hot ones.
**AIDP reproduction:** naive left-join on a skewed `user_id` → write-stage skew ratio **3.0x** (p50 4,277ms vs p100 12,724ms); after the fix **1.3x**.

**Detect in Spark UI:** Stages tab → the join/write stage → Task Metrics → Summary quantiles. `Max` Duration ≫ `Median`. Also check `jvmGcTime` at p100 (high = memory pressure on the hot partition). The SQL tab confirms the join type feeding the skewed stage.

---

## Semi-join pre-filter (shrink the dimension until it broadcasts)

**What:** before joining, reduce the big dimension to only the keys actually present in this fact batch — by collecting the fact's distinct keys and semi-joining the dimension against them. Often shrinks the dimension enough to broadcast, which then also eliminates skew.
**Why it matters:** the most reliable skew fix when no side is naturally broadcastable. Not every dimension key appears in a given fact (esp. a streaming micro-batch — inactive users, test accounts, keys for other time windows). `fact.select(key).distinct()` is cheap (small fact, parquet stats, distinct is not skew-vulnerable). Use **`"semi"`** (LEFT SEMI) — it returns dimension columns only, filtered by existence, no duplication from the fact side. The filtered dimension is small enough to broadcast → the real join becomes a `BroadcastHashJoin`, which has no shuffle and is immune to hot keys.

**Patterns (use when):**
- A big dimension is joined where many of its rows match no fact row.
- Skewed SMJ/SHJ join, no side currently broadcastable, and AQE skew-join is insufficient.

**Anti-patterns (avoid when):**
- **Full-outer join** — you need *all* rows from both sides, so you cannot filter the dimension upfront. The semi-join pre-filter does not apply. (Inner / left-outer with the fact on the left are fine — the join semantics are preserved because you only dropped dimension rows that no fact row references.)
- The dimension is already small (just broadcast it) or already mostly-matching (filtering buys little).

**Apply:**
```python
# before: full 4GB / 1.2GB dimensions shuffled into a skewed SortMergeJoin
enriched = fact.join(dim, "user_id", "left_outer")

# after: distinct fact keys -> semi-join the dim down -> it now broadcasts
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200mb")  # default 10mb
fact_keys    = fact.select("user_id").distinct()                 # cheap; not skew-vulnerable
filtered_dim = dim.join(fact_keys, "user_id", "semi")            # LEFT SEMI: dim cols only
enriched     = fact.join(filtered_dim, "user_id", "left_outer")  # AQE -> BroadcastHashJoin
```
| Config | Default | Suggested | Where |
|---|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | `200mb` (size to filtered dim) | [notebook] |

**Evidence:** field engagement (streaming) — dim **4GB → 68MB** and **1.2GB → 200MB** after the semi-join filter (same scan), both then broadcast; stage **max 21 min → 3.5 min**, micro-batch avg **13–14 min → 7–8 min** (SLA was 11 min). AIDP reproduction — dim through the join **128MB → 13.4MB** (−90%), wall **71s → 59.5s** (−16%), skew **3.0x → 1.3x** (−57%).
**Detect in Spark UI:** before — the dimension scan + `Exchange` carries GBs into an SMJ. After — the filtered dimension shows a `BroadcastExchange` of MBs, and the main join node is `BroadcastHashJoin`. The fact-side distinct adds a small extra stage (e.g. a 6.1MB shuffle) — cheap relative to the eliminated dimension shuffle.

---

## Salting (manual, extreme skew)

**What:** spread one hot key across N synthetic sub-keys — append a random salt to the fact key, and cross-join (explode) the dimension across the same salt range — so the hot partition is split into N partitions.
**Why it matters:** the fallback when **no side is broadcastable** *and* AQE skew-join doesn't catch the skew (the hot partition slips under `skewedPartitionFactor`/`skewedPartitionThresholdInBytes`, or you need deterministic partitioning). Trades a small-side blow-up (×N) and an explicit shuffle for parallelizing the single hot key.

**Patterns (use when):** extreme single-key skew that broadcast can't solve and AQE misses. Prefer broadcast-after-semi-join and AQE skew-join *first* — salting is the last resort (more code, the ×N dimension blow-up, an extra shuffle).
**Anti-patterns (avoid when):** AQE skew-join already handles it (it's automatic — see `07-aqe.md`); or the dimension is large (×N explode becomes expensive). Pick `salt_buckets` no larger than needed to bring the hot partition down to ~median size.

**Apply:**
```python
import pyspark.sql.functions as F

salt_buckets = 10
fact_salted  = fact.withColumn("salt", (F.rand() * salt_buckets).cast("int"))
dim_exploded = dim.crossJoin(spark.range(salt_buckets).withColumnRenamed("id", "salt"))
enriched     = fact_salted.join(dim_exploded, ["key", "salt"], "left_outer").drop("salt")
```
No new config — code-only.
**Detect in Spark UI:** before — one task at p100 with ≫ median `shuffleReadRecords`. After — that work is spread across `salt_buckets` tasks of similar size; skew ratio drops toward 1.x.

---

## Bucketing (repeated joins on the same key)

**What:** pre-shuffle and pre-sort a table by the join key at write time (`bucketBy(N, key).sortBy(key)`). Later joins on that key read co-located buckets and skip the shuffle+sort entirely.
**Why it matters:** amortizes the shuffle cost across many downstream joins — you pay it once at write. Only pays off when the **same key** is joined **repeatedly**; for a one-off join the bucketing write costs more than it saves.

**Patterns (use when):** a table is joined on the same key in many jobs/queries; both sides can be bucketed with the **same bucket count**.
**Anti-patterns (avoid when):** one-off join; bucket counts differ between the two sides (mismatch disables the optimization and Spark re-shuffles anyway); the join key changes across queries.

**Apply:**
```python
# write once, bucketed + sorted on the join key (managed table)
df.write.bucketBy(256, "key").sortBy("key").saveAsTable("fact_bucketed")
# later joins on "key" between two same-bucket-count tables skip shuffle+sort
```
| Config | Default | Note | Where |
|---|---|---|---|
| `spark.sql.sources.bucketing.enabled` | `true` | leave on; matching bucket counts required | [notebook] |

**Evidence:** general OSS technique (no field number in these sources). Validate with a before/after plan: the shuffle `Exchange` should disappear from the join.
**Detect in Spark UI:** after — the join stage has no `Exchange`/`Sort` for the bucketed key; reads are bucket-local.

---

## CBO (cost-based optimizer / join reorder)

**What:** use table/column statistics to reorder multi-join queries and pick join strategies by estimated cost.
**Why it matters:** can improve multi-way joins — but it's **off by default** and needs fresh `ANALYZE TABLE ... COMPUTE STATISTICS` data; **stale stats make it harmful** (it reorders based on wrong sizes). In the field engagement, `spark.sql.cbo.enabled=true` was in the "didn't help / needs further exploration" bucket. Treat as experimental; AQE (runtime, stats-free) usually subsumes the benefit.

**Apply (only with fresh stats):**
```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
# requires: ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS
```
| Config | Default | Where |
|---|---|---|
| `spark.sql.cbo.enabled` | `false` | [notebook] |
| `spark.sql.cbo.joinReorder.enabled` | `false` | [notebook] |

**Detect in Spark UI:** compare plans with/without; only adopt if it measurably improves *and* stats are kept fresh.

---

## AQE skew handling + threshold interplay

**What:** with AQE on (default 3.5.0), Spark detects skewed shuffle partitions at runtime and splits each into sub-partitions, and can convert SMJ→broadcast/SHJ when it observes the real side size. Mostly automatic. Full mechanics + configs in **`07-aqe.md`** — summarized here only as it pertains to joins.
**Why it matters:** often fixes skew with zero code change. But it has a sharp limitation worth knowing before you reach for salting/semi-join.

**The threshold interplay (the trap):**
- A partition is "skewed" only if it is **both** `> skewedPartitionFactor` (×5.0) the median **and** `> skewedPartitionThresholdInBytes` (256MB).
- **`skewedPartitionThresholdInBytes` must be ≥ `advisoryPartitionSizeInBytes`** to take effect. Lowering the skew threshold to catch a smaller hot partition forces lowering the advisory size too — which then **coalesces into many tiny partitions → a small-file problem** on write. In the field, fact tasks already processed <60MB, so lowering the skew threshold wasn't viable.
- Even when AQE splits a hot partition, **part of a single hot key still lands in one task** (you can't split below one key) — so AQE alone may leave residual skew. That's the cue to switch to **semi-join pre-filter + broadcast** (broadcast has no shuffle, so no skew at all) or **salting**.

**Apply:** AQE skew-join is on by default; verify before adding complexity.
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")           # default true
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")  # default true
# tune only if measured; mind the interplay above:
# skewJoin.skewedPartitionThresholdInBytes (256MB) >= adaptive.advisoryPartitionSizeInBytes (64MB)
```
| Config | Default | Note | Where |
|---|---|---|---|
| `spark.sql.adaptive.skewJoin.enabled` | `true` | per-partition split | [notebook] |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5.0` | skewed if > 5× median … | [notebook] |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256MB` | … **and** > this; must be ≥ advisory size | [notebook] |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | lowering it to chase small skew makes small files | [notebook] |

**Evidence:** mild skew below these thresholds slips through AQE → escalate to salting / semi-join (field decision). See the semi-join section for the numbers that resulted.
**Detect in Spark UI:** AQE-rewritten plan shows `AQEShuffleRead` with `skewed=true` splits. If the plan shows no split yet p100/p50 stays high, the hot partition slipped under the thresholds — go to semi-join/salting.

---

## UNION-of-same-table → array + explode (avoid double read + double join)

**What:** when the same table is read and joined twice only to UNION two near-identical projections (e.g. one branch selects `id_1`, the other selects `id_2` when non-null), replace it with a single join that builds an **array** of the wanted ids, then `explode` the array.
**Why it matters:** the UNION pattern reads table A twice and joins A↔B twice — two scans + two shuffles. The rewrite scans and joins **once**; `explode` is a **narrow transformation** (no shuffle), and the array holds at most 2 elements, so it's nearly free. This is a join optimization because the expensive part being eliminated is a duplicate shuffle-join. (Note: `UNION` semantics differ between DataFrame and Spark SQL — verify row counts after the rewrite.)

**Apply:**
```python
import pyspark.sql.functions as F

# before: read+join A↔B twice, then UNION (double scan, double shuffle-join)
b1 = a.join(b, "join_key").select("join_key", F.col("id_1").alias("id"))
b2 = a.join(b, "join_key").where(F.col("id_2").isNotNull()) \
       .select("join_key", F.col("id_2").alias("id"))
result = b1.unionByName(b2)

# after: join once, build an array (id_1, plus id_2 if non-null), then explode
joined = a.join(b, "join_key")
arr = F.when(F.col("id_2").isNotNull(), F.array("id_1", "id_2")) \
        .otherwise(F.array("id_1"))
result = joined.select("join_key", F.explode(arr).alias("id"))   # explode = narrow, no shuffle
```
No new config (one small extra dimension ~50MB in the same engagement was kept broadcastable by raising the threshold — see broadcast section).
**Evidence:** field engagement — **67% runtime reduction, 15 min → 5 min**, by removing the duplicate read + duplicate shuffle-join.
**Detect in Spark UI:** before — the same table scan and the same join `Exchange` appear twice in the SQL plan, feeding a `Union`. After — a single scan/join and an `Generate`/`explode` node (no `Exchange`).

---

## Salted / two-stage aggregation (groupBy skew)

**What:** AQE skew-join split does **not** fix a skewed `groupBy` (one hot group key → one straggler reduce task). Fix it with a **two-stage salted aggregation**: salt the key → partial aggregate → strip salt → final aggregate.
**Why it matters:** spreads a hot key across `N` salt buckets so the partial aggregation parallelizes; the final stage re-combines the (now small) per-bucket partials. This is the recipe the AQE and skew sections redirect here for.
**Patterns (use when):** `groupBy(hotKey).agg(...)` where one/few keys dominate, with **distributive/associative** aggregates (`sum`, `count`, `min`, `max`).
**Anti-patterns (avoid when):** holistic/order-sensitive aggregates (exact `percentile`/`median`, `collect_list`) — two-stage recombination is invalid; for `avg`, carry `sum`+`count` and divide at the end; no real skew (adds a shuffle for nothing).
**Apply:**
```python
import pyspark.sql.functions as F
N = 16
partial = (df.withColumn("_salt", (F.rand()*N).cast("int"))
             .groupBy("key","_salt").agg(F.sum("amt").alias("p")))   # stage 1: partial, parallel
result  = partial.groupBy("key").agg(F.sum("p").alias("amt"))        # stage 2: combine partials
```
**Detect in Spark UI:** the aggregate's reduce stage shows `p100/p50 > 2x` with one task reading far more shuffle records (`diagnosis.md`).

## Broadcast variables vs broadcast joins

**What:** `spark.sparkContext.broadcast(obj)` ships a **read-only Python/JVM object** (lookup dict, model, config) to each executor **once** — distinct from a *broadcast join* (which broadcasts a DataFrame).
**Why it matters:** closing over a large driver object inside a UDF/transform re-serializes it **with every task**; `broadcast()` sends it once per executor (tasks > ~20 KiB are worth this).
**Patterns:** a non-DataFrame lookup map/model used in a UDF / `mapPartitions`.
**Anti-patterns:** the lookup is a DataFrame → use a broadcast *join* / semi-join (above), not a broadcast variable; the object is too big for executor memory.
**Apply:**
```python
b = spark.sparkContext.broadcast(lookup_dict)
df.withColumn("v", my_udf(F.col("k")))   # my_udf reads b.value, NOT the driver-side dict
```

### Cross-references
- Skew measurement & quantiles → `diagnosis.md`
- AQE coalescing, skew-join internals, runtime SMJ→broadcast → `07-aqe.md`
- `shuffle.partitions`, advisory size, small-partition/small-file effects → `01-partitioning.md`
- Exact defaults + where-settable for every key above → `config-matrix.md`

> Shared-cluster warning: there is **one SparkSession per cluster**. A `spark.conf.set` from a notebook (e.g. `autoBroadcastJoinThreshold`, `preferSortMergeJoin`) **leaks to other notebooks** on the same cluster — revert it explicitly with `spark.conf.unset(...)` or restore the prior value when done. `[cluster-create]`/`[non-modifiable]` keys can't be changed from a notebook at all (a `spark.conf.set` raises `AnalysisException [CANNOT_MODIFY_CONFIG]`); a restart resets everything to cluster config.
