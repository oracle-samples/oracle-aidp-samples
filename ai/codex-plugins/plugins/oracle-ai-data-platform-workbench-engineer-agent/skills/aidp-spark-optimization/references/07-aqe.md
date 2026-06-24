# 07 — Adaptive Query Execution (AQE)

Routes here: you want Spark to **re-plan from real runtime statistics** instead of estimates — right-size shuffle output partitions, flip a join to broadcast/shuffle-hash once the real size is known, or split a hot skewed partition mid-stage. Also routes here when someone "turned AQE on" expecting magic and skew/small-file problems remain.

**Core idea:** AQE re-optimizes a query *after each shuffle* using the actual map-output sizes (not the optimizer's pre-run estimates). It does three things — (1) **coalesce** post-shuffle partitions to a target size, (2) **switch join strategy** (SMJ→broadcast or SMJ→shuffle-hash) at runtime, (3) **split skewed join partitions**. It is on by default in Spark 3.5.0 (since 3.2.0). It is a *re-planner*, not a fix for every problem: it handles **join** skew (the skew-join split), not plain `groupBy` aggregation skew, and its knobs interact — shrinking one to catch small skew can manufacture a small-file problem (see the interplay caveat below).

All AQE configs are `spark.sql.*` runtime SQL confs → **[notebook]**-settable via `spark.conf.set(...)`. None require a cluster restart. On a shared cluster there is ONE SparkSession: a `spark.conf.set` here **leaks to other notebooks** — revert with `spark.conf.unset(key)` (or restore the prior value) when done. See `aidp-notes.md`.

---

## AQE umbrella — confirm it's actually on

**What:** The master switch and its sub-features. With AQE on, the Spark UI SQL plan shows an `AdaptiveSparkPlan` root and `isFinalPlan=true` once the runtime re-plan settles.

**Why it matters:** Every technique below is a no-op if `spark.sql.adaptive.enabled=false`. It is `true` by default in 3.5.0, but a cluster Spark config or an inherited session conf can have disabled it. Also: **hard-coding `spark.sql.shuffle.partitions` does not disable AQE, but it sets the *starting* partition count that AQE then coalesces down** — so set it high (2–3× total cores) and let coalescing right-size, rather than hand-tuning a single number (see `01-partitioning.md`).

**Patterns (use when):** always verify on first inspection of any slow shuffle-heavy job.

**Anti-patterns (avoid when):** don't disable AQE to "make plans deterministic" — you lose runtime coalescing, the runtime broadcast switch, and skew split all at once.

**Apply:**
```python
# verify (read-back, don't assume the default)
print(spark.conf.get("spark.sql.adaptive.enabled"))            # expect "true"
print(spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))  # expect "true"
print(spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))   # expect "true"

# if a cluster/session disabled it:
spark.conf.set("spark.sql.adaptive.enabled", "true")
```
Config: `spark.sql.adaptive.enabled` = default `true` → keep `true` [notebook]

**Detect in Spark UI:** SQL tab → plan node starts with `AdaptiveSparkPlan`. If absent, AQE is off. Look for `CustomShuffleReader`/`AQEShuffleRead` nodes (coalesce/skew-split evidence) and a join node whose strategy differs from `explain(True)`'s pre-run plan (that's a runtime switch). Cross-ref `diagnosis.md`.

---

## Coalesce post-shuffle partitions (right-size output files)

**What:** After a shuffle, AQE merges adjacent small post-shuffle partitions up toward a target size so each reduce task has meaningful work, rather than 200 tiny tasks (the static `shuffle.partitions` default) or thousands of tiny output files.

**Why it matters:** Two distinct wins. (1) **Parallelism/scheduling:** avoids thousands of near-empty tasks each paying fixed scheduling overhead. (2) **Output file sizing:** one reduce partition → roughly one output file, so the target size directly controls output file size. The default behaviour is biased toward parallelism, which can leave files smaller than ideal — see the parallelismFirst tradeoff.

The key tradeoff is `coalescePartitions.parallelismFirst`:
- **`true` (default):** AQE coalesces toward `minPartitionSize` (1MB) first to maximize the number of partitions/parallelism, only loosely honoring the advisory target. Good for compute throughput, but tends to produce **more, smaller output files**.
- **`false`:** AQE honors `advisoryPartitionSizeInBytes` as the real target → **fewer, well-sized output files**. Use this when the stage *writes output* and small files would hurt downstream reads (cross-ref `03-file-layout-io.md`).

**Patterns (use when):**
- The job writes a partitioned Delta/Parquet output and you see many sub-128MB files.
- A large shuffle is followed by a write, and you want output files near 128MB.
- You raised `shuffle.partitions` for parallelism and now have too many tiny output files — let coalescing merge them back.

**Anti-patterns (avoid when):**
- Don't lower `advisoryPartitionSizeInBytes` far below 64MB to chase parallelism — you recreate the small-file problem and (per the interplay caveat below) you can't lower it for skew either without shrinking files.
- Setting `parallelismFirst=false` on a compute-bound stage that does *not* write output buys little and can reduce parallelism.

**Apply:**
```python
# before (the smell): big shuffle → write produces hundreds of small files,
# default parallelismFirst=true keeps partitions small (~1MB floor)

# after (the fix): make AQE honor a real target so output files are well-sized
spark.conf.set("spark.sql.shuffle.partitions", "768")            # high start; AQE coalesces down
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")  # raise 64MB -> 128MB
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
# write proceeds normally; AQE coalesces to ~128MB partitions -> ~128MB output files
```
Config:
| key | default | suggested | where |
|---|---|---|---|
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | keep `true` | [notebook] |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | `128MB` for well-sized output files | [notebook] |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | `false` to honor the advisory target (fewer/larger files) | [notebook] |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | leave unless deliberately tuning the floor | [notebook] |

**Evidence:** Field engagement — raising `shuffle.partitions` to 768 alone barely moved the needle (47m30s). Adding `advisoryPartitionSizeInBytes=128mb` + `parallelismFirst=false` produced **384 well-sized output files** (so fewer `.par` files generated downstream) and brought the stage to **44m20s**. The motivation was output-file quality as much as wall time.

**Detect in Spark UI:** SQL plan shows `AQEShuffleRead coalesced`; the write stage's output-file count and per-file size (or the partition count after coalescing) tell you whether the target is being honored. If you see a low partition count but tiny files, `parallelismFirst` is still `true`.

**Targeting only the write — the `REBALANCE` hint.** `parallelismFirst`/`advisoryPartitionSizeInBytes` are **query-global** — lowering the advisory size to fix output files also affects every other shuffle in the query. To right-size *only the final write* without touching the rest of the plan, add a `REBALANCE` hint, which inserts a balanced shuffle sized to the advisory target right before the write: `df.hint("rebalance").write...` (or SQL `/*+ REBALANCE(col) */`). AQE also **splits skewed rebalance partitions** automatically (`spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled`, default `true`; merge floor `rebalancePartitionsSmallPartitionFactor=0.2`), so a hot key in the write doesn't produce one giant file. This is the preferred lever for "fix the output file sizes" when the rest of the query is fine (cross-ref `08-delta-lake.md`, `03-file-layout-io.md`).

---

## SMJ → BroadcastHashJoin at runtime

**What:** When a join's actual (post-filter, post-shuffle) build-side size is observed to be under the broadcast threshold, AQE rewrites the planned SortMergeJoin into a BroadcastHashJoin — eliminating the shuffle+sort of both sides — and uses a local shuffle reader to avoid re-fetching shuffle blocks across the network.

**Why it matters:** Broadcast joins do no exchange and are **immune to hot keys** — every partition joins against the broadcast copy independently, so a skewed key can't create a straggler. The static optimizer can only estimate sizes pre-run and often plans SMJ for a side that is actually small after filters; AQE corrects this once the real size is known. This is the single highest-leverage AQE behaviour for the classic "200GB SMJ with a 200MB small side" smell (cross-ref `02-joins.md`).

**Patterns (use when):**
- A join's small side is *estimated* large but is small after upstream filters/aggregations.
- You applied a semi-join pre-filter that shrank a dimension to broadcastable size and want AQE to pick up the broadcast automatically.

**Anti-patterns (avoid when):**
- Full-outer equi-joins **cannot** use BroadcastHashJoin — AQE will not switch them; reduce the side another way (cross-ref `02-joins.md`).
- Raising the adaptive threshold so high the build side OOMs the driver/executors — the broadcast copy lives on the driver then ships to every executor.

**Apply:**
```python
# the adaptive switch reuses the *static* broadcast threshold, so raise that:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200mb")  # default 10MB
# AQE inherits it via spark.sql.adaptive.autoBroadcastJoinThreshold (defaults to the static value)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")  # default true; keeps it local

# Now a SortMergeJoin whose real build side < 200MB is rewritten to BroadcastHashJoin at runtime.
```
Config:
| key | default | suggested | where |
|---|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | raise to fit the real small side (e.g. `200mb`); size against driver+executor mem | [notebook] |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | `= autoBroadcastJoinThreshold` | usually leave (inherits static) | [notebook] |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | keep `true` | [notebook] |

**Evidence:** AIDP reproduction (4-core cluster, Spark 3.5.0). A 1GB fact left-joined to a 128MB dim planned as SortMergeJoin (71s, task skew 3.0×). A semi-join pre-filter shrank the dim to **13.4MB**; with `autoBroadcastJoinThreshold=200mb`, AQE converted the outer join to **BroadcastHashJoin** at runtime → **59.5s (-16%)**, task skew dropped to **1.3×**, write-stage executor time 68.2s→45.6s. The broadcast removed the user_id repartition that was creating the straggler.

**Detect in Spark UI:** `explain(True)` shows `SortMergeJoin`, but the runtime SQL plan shows `BroadcastHashJoin` + `BroadcastExchange` + `AQEShuffleRead local`. That delta = the runtime switch fired. Cross-ref `02-joins.md`, `diagnosis.md`.

---

## SMJ → ShuffleHashJoin at runtime (skip the sort)

**What:** When neither side is broadcastable but one side's shuffle partitions are small enough to build an in-memory hash map, AQE can pick ShuffleHashJoin (SHJ) over SortMergeJoin — skipping the sort of both sides.

**Why it matters:** SMJ pays to sort both sides before merging. If a build side fits in memory per partition, SHJ avoids that sort entirely. Spark prefers SMJ by default (`spark.sql.join.preferSortMergeJoin=true`); AQE's runtime variant is gated by `maxShuffledHashJoinLocalMapThreshold`, which is **`0` by default (disabled)** — raise it (to the per-partition build-size bytes you'll tolerate) to let AQE consider SHJ.

**Patterns (use when):** large-to-large join where one side, after shuffle, has partitions small enough to hash-build in memory, and the sort is a visible cost.

**Anti-patterns (avoid when):**
- The build-side partition does **not** fit in memory → SHJ builds a hash map that can't spill cleanly, risking OOM and a lost container (which costs minutes to replace, plus recomputation). When in doubt prefer SMJ or broadcast. Cross-ref `02-joins.md`.
- Don't combine an aggressive `maxShuffledHashJoinLocalMapThreshold` with too-few partitions — bigger per-partition build maps.

**Apply:**
```python
# before: large-large join sorts both sides (SortMergeJoin)
# after: allow AQE to choose SHJ when a build side fits in memory per partition
spark.conf.set("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", str(64 * 1024 * 1024))  # 0 (off) -> e.g. 64MB
# (A blunter, query-wide alternative is spark.sql.join.preferSortMergeJoin=false -- see 02-joins.md;
#  the AQE threshold is more surgical because it's size-gated at runtime.)
```
Config: `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` = default `0` (disabled) → raise to a per-partition build-size budget (e.g. `64m`) to enable [notebook]
Related: `spark.sql.join.preferSortMergeJoin` = `true` → `false` is the query-wide hammer (`02-joins.md`) [notebook]

**Evidence:** Field engagement — disabling SMJ preference (`preferSortMergeJoin=false`) on a large-large join shaved the pipeline from 44m to **43m05s** (modest; the sort was not the dominant cost there). Treat SHJ as a small, situational win, not a headline lever.

**Detect in Spark UI:** runtime SQL plan shows `ShuffledHashJoin` where `explain(True)` showed `SortMergeJoin`; the `Sort` nodes on the join inputs disappear.

---

## Skew-join handling (split the hot partition)

**What:** AQE detects skewed shuffle partitions in a join (both **sort-merge and shuffled-hash** joins) and splits each hot partition into several sub-partitions (replicating the matching partition on the other side), so the straggler's work is spread across multiple tasks.

**Why it matters:** Skew means one task processes far more data than the median; the whole stage waits on that straggler, turning parallel work serial. A partition is treated as skewed only when it is **both** larger than `skewedPartitionFactor` × the median partition **and** larger than `skewedPartitionThresholdInBytes`. Splitting it restores parallelism without changing query logic.

**CRITICAL — works for JOINS, not aggregations.** AQE skew split only applies to join partitions. A skewed `groupBy`/aggregation is **not** handled by this mechanism — for aggregation skew you must restructure (e.g. salt the group key / two-stage aggregate), see `02-joins.md`. Do not expect `skewJoin.enabled` to fix a skewed aggregate.

**Patterns (use when):** a join stage shows task `p100/p50 > 2×` (severe at `> 5×`), high `jvmGcTime`/spill concentrated in a few tasks, and the hot partitions are genuinely large (≥ the byte threshold).

**Anti-patterns (avoid when):**
- **Mild skew below the thresholds slips through.** If the fact's shuffle partitions are small (e.g. each reduce task reads <60MB), the hot partition never exceeds the 256MB threshold, so AQE skips it — and you *can't* simply lower the threshold (see interplay below). Fall back to semi-join pre-filter / salting (`02-joins.md`).
- Aggregation skew (covered above).
- Broadcast already eliminated the shuffle — then there's no skewed partition to split (broadcast joins are immune to hot keys), so skew handling is irrelevant.

**Apply:**
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")                         # default true
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5.0")            # >5x median ...
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")# ... AND >256MB
```
Config:
| key | default | note | where |
|---|---|---|---|
| `spark.sql.adaptive.skewJoin.enabled` | `true` | keep `true` | [notebook] |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5.0` | partition skewed if > factor × median AND above the size threshold | [notebook] |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256MB` | partition skewed only if also > this | [notebook] |
| `spark.sql.adaptive.forceOptimizeSkewedJoin` | `false` (since 3.3) | force the skew split even when it requires an **extra shuffle** | [notebook] |

**`forceOptimizeSkewedJoin` (the "split anyway" knob):** normally AQE only splits a skewed join partition when a shuffle is *already* present to piggyback on; if the cheaper plan has no shuffle, AQE leaves the skew alone rather than add one. Setting `forceOptimizeSkewedJoin=true` tells AQE to split the hot partition **even if it must introduce an extra shuffle** to do so — worth it when the straggler costs more than the added exchange. It does **not** lower the skew thresholds, so it does not help *below-threshold* skew (that still needs semi-join pre-filter / salting — see `02-joins.md`). Use it only when you've confirmed a genuine straggler that AQE is declining to split for shuffle-cost reasons.

**Evidence:** Field engagement — a streaming pipeline joined a fact against 4GB and 1.2GB dimensions; some microbatches lost executors to OOM. AQE skew split was *considered but not chosen first*: fact shuffle reads were small (<60MB/task) so the hot partitions never reached the threshold, and lowering the threshold was blocked by the interplay caveat. The chosen fix (semi-join pre-filter → broadcast) is in `02-joins.md`: dims dropped to 68MB and 200MB, microbatch 13–14min → **7–8min**.

**Detect in Spark UI:** join-stage task summary with `p100/p50 > 2×` on `executorRunTime` and per-task shuffle-read size. When skew split fires, the plan shows `AQEShuffleRead skewed` and the post-split task count rises. Cross-ref `diagnosis.md`, `02-joins.md`.

---

## The interplay caveat: skew threshold vs advisory size (read before tuning either)

**The constraint:** `skewJoin.skewedPartitionThresholdInBytes` must be **≥** `coalescePartitions.advisoryPartitionSizeInBytes` to take effect. AQE coalesces shuffle partitions toward the advisory size *first*; a partition can only be a skew-split candidate if it exceeds the skew threshold, and the threshold is effectively floored by the advisory size. So the skew threshold a partition is measured against is never smaller than the partitions AQE just created.

**Why this bites you:** When skew is *mild* — the hot partition is, say, 100MB while the median is small — you might want to lower `skewedPartitionThresholdInBytes` below 256MB so AQE catches it. But to make that take effect you must **also lower `advisoryPartitionSizeInBytes`** below it. Shrinking the advisory size makes AQE coalesce into many **small partitions → many small output files** (cross-ref `03-file-layout-io.md`) — and to fix *that* you'd add an extra shuffle (a `REBALANCE`/repartition) on write, adding runtime. So:

> **You cannot shrink the advisory size to catch tiny skew without manufacturing a small-file problem.** Below the thresholds, AQE skew handling is the wrong tool.

**What to do instead (AQE insufficient → fall back):**
- **Below-threshold join skew** → semi-join pre-filter the big side down to broadcastable size, then let AQE switch to BroadcastHashJoin (broadcast is hot-key-immune). See `02-joins.md`.
- **A few extreme hot keys AQE can't broadcast away** → manual salting (append a random bucket to the hot key, explode the small side over the salt range, join on `[key, salt]`). See `02-joins.md`.
- **Aggregation skew** → AQE skew split does not apply; salt the group key / two-stage aggregate. See `02-joins.md`.

```python
# WRONG: trying to catch a 100MB skewed partition by lowering only the threshold
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64mb")  # no effect if advisory is 128MB
# To "fix" it you'd also drop advisory below 64MB -> small files -> extra write shuffle. Don't.

# RIGHT for mild/below-threshold skew: reduce the side instead (semi-join -> broadcast)
filtered_dim = dim.join(fact.select("key").distinct(), "key", "semi")  # see 02-joins.md
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200mb")
enriched = fact.join(filtered_dim, "key", "left_outer")  # AQE -> BroadcastHashJoin, no skewed partition exists
```

**Detect in Spark UI:** task skew is visible (`p100/p50 > 2×`) but the plan shows **no** `AQEShuffleRead skewed` node despite `skewJoin.enabled=true` → the hot partition is below threshold; don't lower the threshold, switch to the pre-filter/salting path. Cross-ref `02-joins.md`, `03-file-layout-io.md`, `diagnosis.md`.

---

## When AQE is enough vs not — quick decision

| Situation | AQE handles it? | Action |
|---|---|---|
| Small side actually broadcastable at runtime | Yes — SMJ→BroadcastHashJoin | raise `autoBroadcastJoinThreshold` |
| Too many small post-shuffle partitions / small output files | Yes — coalesce | `parallelismFirst=false`, `advisoryPartitionSizeInBytes=128mb` |
| Large-large join, build side fits per partition | Yes — SMJ→SHJ | raise `maxShuffledHashJoinLocalMapThreshold` |
| Join skew, hot partition ≥ 256MB and > 5× median | Yes — skew split | keep `skewJoin.enabled=true` |
| Join skew, hot partition **below** threshold | **No** | semi-join pre-filter + broadcast (`02-joins.md`) |
| A few extreme hot keys, can't broadcast | **No** | salting (`02-joins.md`) |
| **Aggregation** (groupBy) skew | **No** | salt group key / two-stage aggregate (`02-joins.md`) |
| Full-outer equi-join, big side | **No** (no BroadcastHashJoin) | reduce a side another way (`02-joins.md`) |

See `config-matrix.md` for the consolidated key/default/where-settable table, and `quick-reference.md` for the impact-ranked checklist.
