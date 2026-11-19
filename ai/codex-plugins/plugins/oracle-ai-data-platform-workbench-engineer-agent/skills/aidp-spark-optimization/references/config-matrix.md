# Config matrix — Spark 3.5.0 (+ OSS Delta), with where-to-set on AIDP

Defaults are open-source Apache Spark 3.5.0. **Where-to-set** is the operative column for AIDP.

## Where-to-set legend

- **[notebook]** — runtime SQL conf; `spark.conf.set(...)` takes effect this session. All `spark.sql.*`.
- **[cluster-create]** — startup/cluster config; read at executor/JVM launch. Set it in the cluster's Spark config; **changing it on a live cluster needs a restart**. A notebook `spark.conf.set` on these **raises `AnalysisException [CANNOT_MODIFY_CONFIG]`** (verified live on Spark 3.5.0 — it errors, it is *not* silently ignored). Set them in the cluster's **Spark Advanced Configurations** at create time (the cluster `sparkAdvancedConfigurations` field). Read effective values with `spark.sparkContext.getConf().get(...)` or `spark_get_environment`.
- **[non-modifiable]** — not exposed by AIDP / derived. e.g. `spark.executor.cores` = 2 × OCPU.
- **[Delta]** — open-source Delta Lake conf; `spark.conf.set` works (notebook), governs the Delta write/OPTIMIZE op.

> **Shared-cluster caveat (one SparkSession per cluster):** a `[notebook]` `spark.conf.set` **leaks to every other notebook/job on that cluster**. Revert it explicitly when done (`spark.conf.unset(key)` or restore the previous value). A cluster restart resets everything to the cluster config. See `aidp-notes.md`.

## Joins & broadcast

| Config | Default | Where | Effect / when to change |
|---|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | [notebook] | Raise (e.g. 200mb–1gb) so a small/filtered side broadcasts → no shuffle. Cost: driver/executor memory. `-1` disables. |
| `spark.sql.join.preferSortMergeJoin` | true | [notebook] (advanced/internal) | `false` lets Spark pick Shuffle-Hash (skips SMJ sort) when viable. Risk: SHJ build OOM. |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | = autoBroadcastJoinThreshold | [notebook] | AQE runtime SMJ→broadcast threshold. |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | 0 | [notebook] | Raise to let AQE pick SHJ at runtime. |
| `spark.sql.cbo.enabled` / `...joinReorder.enabled` | false / false | [notebook] | Opt-in; needs fresh `ANALYZE TABLE` stats; harmful if stale. |
| `spark.sql.sources.bucketing.enabled` | true | [notebook] | Bucketing for repeated same-key joins. |
| `spark.driver.maxResultSize` | 1g | [cluster-create] | Guards `collect()`/broadcast build on driver. |

## AQE (all [notebook]; `spark.sql.adaptive.enabled` = true by default)

| Config | Default | Effect |
|---|---|---|
| `spark.sql.adaptive.enabled` | true | Master switch; re-optimizes from runtime stats. |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Merge small post-shuffle partitions. |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | Target partition/output size; raise to ~128mb for fewer, well-sized output files. |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | true | Set **false** to honor the advisory size (fewer small files) over max parallelism. |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1MB | Floor for coalesced partition size. |
| `spark.sql.adaptive.skewJoin.enabled` | true | Split skewed join partitions at runtime. |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5.0 | Skewed if > 5× median AND > threshold. |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256MB | Must be ≥ advisory size to take effect (interplay — see `07-aqe.md`). |
| `spark.sql.adaptive.forceOptimizeSkewedJoin` | false (since 3.3) | Force the skew split even when it adds an extra shuffle. Doesn't lower thresholds. |
| `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` | true | Split skewed `REBALANCE` partitions so a hot key doesn't make one giant output file. |
| `spark.sql.adaptive.localShuffleReader.enabled` | true | Local shuffle read after broadcast conversion. |

## Partitioning & parallelism

| Config | Default | Where | Effect |
|---|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | [notebook] | Set ~2–3× total cores for large shuffles; let AQE coalesce. |
| `spark.sql.files.maxPartitionBytes` | 128MB | [notebook] | Bytes packed per read task. Raise to read >128MB/task. |
| `spark.sql.files.openCostInBytes` | 4MB | [notebook] (advanced/internal) | Per-file open cost in bin-packing; high with many small files inflates read cost. |
| `spark.sql.files.minPartitionNum` | = leafNodeDefaultParallelism | [notebook] | Floor on read partitions. |
| `spark.default.parallelism` | total cores | [cluster-create] | RDD-API default parallelism. |

## File layout, compression, I/O

| Config | Default | Where | Effect |
|---|---|---|---|
| `spark.sql.sources.partitionOverwriteMode` | STATIC | [notebook] | `DYNAMIC` overwrites only partitions present in the DataFrame (not the whole table). |
| `spark.sql.parquet.compression.codec` | snappy | [notebook] | `zstd` = gzip-tight + snappy-fast; smaller output. |
| `spark.sql.parquet.filterPushdown` | true | [notebook] | Row-group skipping via predicate pushdown. |
| `spark.sql.parquet.columnarReaderBatchSize` | 4096 | [notebook] | Vectorized read batch. |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | true | [notebook] | Prunes fact partitions from filtered dim (star schema). |
| `spark.io.compression.codec` | lz4 | **[cluster-create]** | Shuffle/spill/broadcast codec; `zstd` cuts spill-disk volume. |
| `spark.io.compression.zstd.level` | 1 | **[cluster-create]** | Higher = tighter, more CPU. |
| `spark.io.compression.zstd.bufferSize` | 32k | **[cluster-create]** | Larger (e.g. 1024kb) = faster zstd. |
| `spark.shuffle.compress` / `spark.shuffle.spill.compress` | true / true | [cluster-create] | Keep on. |

## Memory & resources

| Config | Default | Where | Effect |
|---|---|---|---|
| `spark.memory.fraction` | 0.6 | **[cluster-create]** | Exec+storage share of (heap−300MB). Raise to **0.7–0.75 (cap 0.8)** to kill spill when no user structures/caching — **0.9 is aggressive**: it shrinks the JVM-overhead/off-heap buffer and risks OOM. **Notebook `spark.conf.set` raises CANNOT_MODIFY_CONFIG.** |
| `spark.memory.storageFraction` | 0.5 | **[cluster-create]** | Eviction-protected storage sub-region. |
| `spark.memory.offHeap.enabled` / `.size` | false / 0 | **[cluster-create]** | Off-heap execution memory. |
| `spark.executor.memory` | 1g (AIDP derives ≈0.74–0.84× worker RAM; live: 24275m on a 32GB worker ≈ 0.74×) | **[cluster-create]** | Set via worker RAM tier. |
| `spark.executor.memoryOverhead` | derived | [cluster-create] | JVM/PySpark overhead. |
| `spark.executor.cores` | 1 (AIDP = 2 × OCPU) | **[non-modifiable]** | Tasks per worker; change via OCPU tier. |
| `spark.serializer` | JavaSerializer | **[cluster-create]** | Set `KryoSerializer` for faster/compacter shuffle+cache. |

## Codegen (all [notebook]; advanced/internal — tune per-job, with caution)

| Config | Default | Effect |
|---|---|---|
| `spark.sql.codegen.wholeStage` | true | Keep on globally. Do not disable for the whole app. |
| `spark.sql.codegen.maxFields` | 100 | Above this, whole-stage codegen falls back. **Lower it** (e.g. 50) for a CPU-bound very-wide aggregation that is slower with codegen on. See `05-codegen.md`. |

## Delta Lake 3.2.0 (OSS) — table properties + OPTIMIZE/write confs

Full feature treatment (usage / limits / patterns / anti-patterns) is in `08-delta-lake.md`. Defaults below are OSS Delta **3.2.0** (validated against the v3.2.0 source). `spark.databricks.delta.*` are session confs (`[notebook]`-settable; many are `.internal`/advanced); `delta.*` are table properties.

| Config / property | OSS 3.2.0 default | Effect |
|---|---|---|
| `spark.databricks.delta.optimize.maxFileSize` | **1 GiB** | OPTIMIZE target output file size. Decks set it lower (e.g. 256–512MB) for smaller files. `.internal` |
| `spark.databricks.delta.optimize.minFileSize` | **1 GiB** | Files below this are compaction candidates (keep min ≤ max). `.internal` |
| `spark.databricks.delta.optimize.maxThreads` | **15** | OPTIMIZE bin-compaction parallelism. `.internal` |
| `delta.autoOptimize.optimizeWrite` (tbl) / `spark.databricks.delta.optimizeWrite.enabled` | off (unset) | Pre-write shuffle → fewer, well-sized files; small runtime cost. |
| `spark.databricks.delta.optimizeWrite.binSize` | **512 MiB** | Target per-partition size for optimized writes. `.internal` |
| `delta.autoOptimize.autoCompact` (tbl) / `spark.databricks.delta.autoCompact.enabled` | off (unset) | Post-write compaction of small files. |
| `spark.databricks.delta.autoCompact.maxFileSize` / `minNumFiles` | 128MB / 50 | Auto-compact target size; min small files to trigger. `.internal` |
| `delta.dataSkippingNumIndexedCols` (tbl) | **32** | First-N columns with min/max stats for file skipping (`-1` = all). |
| `delta.enableDeletionVectors` (tbl) | **false** | Merge-on-read deletes/updates (GA for SCAN/DELETE/UPDATE/MERGE in 3.2.0). |
| `delta.enableChangeDataFeed` (tbl) | **false** | Change Data Feed (CDF). |
| `delta.deletedFileRetentionDuration` (tbl) | **7 days** | VACUUM tombstone retention — don't shrink (breaks time-travel/concurrent readers). |
| `delta.logRetentionDuration` (tbl) | **30 days** | Transaction-log retention. |
| `spark.databricks.delta.retentionDurationCheck.enabled` | **true** | Guards VACUUM against a dangerously short RETAIN. |

Notes (OSS Delta 3.2.0, source-validated):
- `OPTIMIZE` (compaction/bin-packing), `ZORDER`, `VACUUM`, optimized writes, auto-compaction, deletion vectors, CDF, and **liquid clustering (`CLUSTER BY`, GA in 3.2.0; 2–4 cols — OPTIMIZE clustering needs ≥2; not compatible with partitioning or ZORDER)** are all open-source. See `08-delta-lake.md`.
- **`delta.targetFileSize` is NOT in OSS Delta 3.2.0** (Databricks-only) — control OPTIMIZE output size with `spark.databricks.delta.optimize.maxFileSize`.
- Databricks-only (NOT in OSS): Photon, predictive optimization, Delta disk cache, `delta.targetFileSize` / `tuneFileSizesForRewrites`.

## Oracle / JDBC source + sink (see `09-oracle-database.md`)

These are DataFrame **reader/writer options** (`.option(...)`), not session confs — they take effect per read/write, no cluster restart. Marked **[read-option]** / **[write-option]**. (AIDP's external-catalog ADW reader uses dotted names: `partition.column`, `partition.lower/upper/num`, `fetch.size`, `pushdown.sql`.)

| Option | Default | Where | Effect |
|---|---|---|---|
| `fetchsize` (read) | **10** (Oracle JDBC driver!) / 1000 (AIDP ADW) | [read-option] | Rows per round-trip. Raise hard (10k–100k); too high OOMs on wide rows. Biggest read lever. |
| `partitionColumn` + `lowerBound`+`upperBound`+`numPartitions` | none → 1 task | [read-option] | Parallel range read; one numeric/date column. `numPartitions` = concurrent DB connections. |
| `batchsize` (write) | 1000 | [write-option] | Rows per `executeBatch`. Raise (5k–50k) to cut round-trips. Conventional-path JDBC still logs fully. |
| `truncate` (write, overwrite) | false | [write-option] | `true` overwrites without DROP+CREATE (keeps table/indexes/grants). |
| `numPartitions` (write) | source-dependent | [write-option] | = concurrent writer connections; bound to DB capacity, not Spark cores. |

> Writes to **ADW/ATP** use the managed bulk path (`DBMS_CLOUD.COPY_DATA`, set-based). Writes to **classic/Exadata** Oracle are **conventional-path batched JDBC `INSERT … VALUES`** (fully logged) — redo is a DB-side (NOLOGGING + direct-path) decision, not a Spark conf. See the redo field case in `09-oracle-database.md`.

## Storage Partition Join (V2 sources — eliminate the join shuffle) [notebook]

**Opt-in in 3.5.0** — the master switch defaults to `false` (it became on-by-default only in Spark 4.0).

| Config | Default (3.5.0) | Effect |
|---|---|---|
| `spark.sql.sources.v2.bucketing.enabled` | **false** | Use a V2 source's reported partitioning to skip the join `Exchange`. Set `true` to enable SPJ. |
| `spark.sql.sources.v2.bucketing.pushPartValues.enabled` | **false** | Also enable to co-partition when one side misses partition values. |
| `spark.sql.requireAllClusterKeysForCoPartition` | true (internal) | Set `false` to allow join-key ⊂ partition-key. |
| `spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled` | false | Skew handling for SPJ (non-full-outer). |

(The `allowJoinKeysSubsetOfPartitionKeys` / `allowCompatibleTransforms` / `bucketing.shuffle` SPJ knobs are **Spark 4.0+ — not in 3.5.0**.)

## Rules
- Tune `[notebook]` configs per job; **revert on shared clusters**.
- `[cluster-create]` changes (memory, io.compression, serializer) require setting at cluster creation and a restart; confirm via `spark_get_environment`.
- `[non-modifiable]` (`executor.cores`) → change the OCPU tier / worker count instead (`cluster-sizing.md`).
- Modifiability verified live (Spark 3.5.0): a `[notebook]` key (`spark.sql.shuffle.partitions`, `autoBroadcastJoinThreshold`) changes via `spark.conf.set`; a `[cluster-create]`/`[non-modifiable]` key (`spark.memory.fraction`, `spark.io.compression.codec`, `spark.executor.cores`) **raises `AnalysisException [CANNOT_MODIFY_CONFIG]`** — set it in the cluster config + restart. **End-to-end verified:** a cluster created with `sparkAdvancedConfigurations={"spark.memory.fraction":"0.75"}` reported `0.75` to the running Spark (default is 0.6) — the cluster-create path works while the notebook path errors.
