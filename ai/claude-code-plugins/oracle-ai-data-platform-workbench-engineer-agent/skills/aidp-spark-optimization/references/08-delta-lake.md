# Delta Lake 3.2.0 (OSS) — features & optimizations

Routes here: a Delta table is slow to read/write, fragmenting into small files, doing full-file rewrites on DELETE/UPDATE/MERGE, scanning too much, or you're choosing a layout (partition vs ZORDER vs CLUSTER BY). **Core idea:** Delta optimizations are about **file count/size**, **data layout for skipping**, and **avoiding rewrites** — each has a cost, and several must be paired (OPTIMIZE needs VACUUM; clustering needs stats).

Scope: **open-source Delta Lake 3.2.0 on Spark 3.5.0** (validated against the v3.2.0 source). Availability tags: `[3.2.0]` GA in OSS 3.2.0 · `[PREVIEW]` experimental in 3.2.0 · `[>3.2]` added later · `[DBX-ONLY]` Databricks-proprietary. `spark.databricks.delta.*` is **legacy naming honored by OSS** (not Databricks-only); many are `.internal` advanced knobs. Defaults + where-to-set: `config-matrix.md`. Docs: https://docs.delta.io/latest/optimizations-oss.html

## The recipe: optimized writes + zstd(output/shuffle/spill) + bin size

Field-proven combo for **fewer, well-sized files at a small, bounded write cost**:

1. **Optimized writes** — writer emits well-sized files via one pre-write shuffle (no post-hoc OPTIMIZE):
   ```python
   df.write.format("delta").option("optimizeWrite","true").save(path)   # [notebook] per-write
   ```
2. **zstd for OUTPUT parquet** `[notebook]` — tight like gzip, fast like snappy:
   ```python
   spark.conf.set("spark.sql.parquet.compression.codec", "zstd")        # default snappy
   ```
3. **zstd for SHUFFLE + SPILL** `[cluster-create]` — set in the cluster Spark config + restart (a notebook `spark.conf.set` on `spark.io.compression.codec` raises `CANNOT_MODIFY_CONFIG`):
   ```
   spark.io.compression.codec  zstd      # default lz4 (covers shuffle, spill, broadcast)
   spark.io.compression.zstd.level 3     # default 1
   spark.io.compression.zstd.bufferSize 1024kb   # default 32k
   ```
4. **Bin size / target file size** — set `spark.databricks.delta.optimizeWrite.binSize` (default 512 MiB) and `spark.databricks.delta.optimize.maxFileSize` to ~128–512 MB (the splittable-parquet sweet spot).

**Tradeoffs:** the optimized-write shuffle adds write latency (you buy cheaper reads). **Output codec and shuffle/spill codec are independent knobs** — output is `[notebook]`, shuffle/spill is `[cluster-create]`; don't conflate. zstd costs a little more CPU than lz4/snappy but is net-faster here.
**Evidence (field, OSS Spark 3.5 / Delta):** output→zstd: write 30m13s→13m37s, 953→944 GB; 2 TB ingest output codec: zstd 8m22s/1889 GB vs gzip 10m47s/1906 GB vs snappy 15m7s/~3 TB; shuffle/spill→zstd(lvl3): spill-disk **1051 GB → 138 GB**. (See `case-studies.md`, `03-file-layout-io.md`, `04-memory-and-spill.md`.)

## Getting well-sized files: 4 approaches (compare before you pick)

When a write produces many small files, there are four ways to end up with well-sized files. **Key axis: PREVENT small files at write time (optimized writes / AQE coalesce) vs COMPACT them after (OPTIMIZE / auto-compaction).** Post-write compaction leaves the small files already written (plus stale copies) — so it does **not** cut the write-time request burst and it needs VACUUM; write-time approaches cut the files actually written.

| Approach | Runs | Prevent vs compact | Extra shuffle | Separate OPTIMIZE job | Needs VACUUM | Cuts files *written* (↓ OCI 429 risk) | Z-order | Main cost |
|---|---|---|---|---|---|---|---|---|
| **OPTIMIZE** | after load (separate statement) | compact | no | **yes** | **yes** | no — small files still written; OPTIMIZE writes more | only with `ZORDER BY` | extra job + VACUUM overhead |
| **Auto-compaction** (`autoCompact`) | synchronously after each write | compact | no | no | **yes** | no — the write still bursts small files | **no** (bin-packing only) | adds write tail-latency + VACUUM |
| **Optimized writes** (`optimizeWrite`) | during the write | **prevent** | **yes** (pre-write) | no | no *compaction* tombstones | **yes** — well-sized files written directly | no | write latency — tune `binSize` |
| **AQE coalesce** (`advisoryPartitionSizeInBytes` + `parallelismFirst=false`) | during the write, on the existing shuffle | **prevent** | **no** (rides the existing shuffle) | no | no | yes — fewer output files, **only if the query already shuffles** | no | configs are session/query-global (see caveat) |

**Corrections to common belief:**
- **Z-order is opt-in, and auto-compaction does NOT z-order.** OPTIMIZE bin-packs by default; you get z-order only with explicit `OPTIMIZE … ZORDER BY`. Auto-compaction is bin-packing only. For an ordered layout use ZORDER or liquid clustering.
- **Optimized writes don't remove VACUUM entirely** — they remove the *compaction-driven* stale files; you still VACUUM normal UPDATE/DELETE/MERGE/overwrite tombstones.

**OCI Object Storage 429 (the write-burst angle):** OCI Object Storage rate-limits requests and can return **HTTP 429 (Too Many Requests)** under a heavy small-file write burst (many PUTs) — and OPTIMIZE/auto-compaction make it *worse* (they write yet more files). Optimized writes and AQE coalescing cut the number of files written → fewer requests → lower 429 risk. This is the strongest reason to **prevent** small files at write time rather than compact after.

**AQE coalescing caveats (why it isn't a silver bullet):**
- It coalesces **shuffle** partitions — a pure scan→write with **no shuffle** has nothing to coalesce, so it simply doesn't apply.
- `advisoryPartitionSizeInBytes` + `parallelismFirst=false` are **session/query-global** → they affect *every* shuffle stage, which can slow earlier stages of a multi-stage job. To size **only the final write**, use a targeted `/*+ REBALANCE(cols) */` hint at the write instead of lowering parallelism globally.

**Decision:** prefer **optimized writes** (or **AQE coalesce / `REBALANCE`** when the job already shuffles and you want no extra shuffle) to *prevent* small files; fall back to **auto-compaction** (no separate job, but synchronous write cost) or a scheduled **OPTIMIZE + VACUUM** when you can't change the write path. Details + configs for each are below and in `config-matrix.md`.

## File sizing & compaction

### OPTIMIZE (compaction / bin-packing) `[3.2.0]`
**What:** coalesces small files into fewer, evenly-sized files. Idempotent.
```sql
OPTIMIZE t;                          -- whole table
OPTIMIZE t WHERE date >= '2024-01-01';   -- scope to recent partitions
```
```python
from delta.tables import DeltaTable
DeltaTable.forPath(spark, path).optimize().where("date='2024-01-01'").executeCompaction()
```
Confs (`.internal`): `optimize.maxFileSize` (default **1 GiB**, target output), `optimize.minFileSize` (default **1 GiB**, files below are candidates — keep min ≤ max), `optimize.maxThreads` (default **15**), `optimize.repartition.enabled` (true → `repartition(1)` for many tiny files).
**⚠ Pairs with VACUUM (mandatory):** OPTIMIZE **rewrites into new files and leaves the old ones** (so readers/time-travel keep working) → storage *grows* until VACUUM deletes the unreferenced files past the retention window. **OPTIMIZE without VACUUM = permanent storage bloat.**
**Patterns:** append/streaming-sink/over-partitioned tables read repeatedly; compact only recent partitions with `WHERE`.
**Anti-patterns:** table read once (cost never amortized); latency-critical sinks; `minFileSize ≥ maxFileSize` (re-packs everything — field gotcha); forgetting VACUUM.

### Optimized writes (`optimizeWrite`) `[3.2.0]` (since 3.1.0)
**What:** pre-write shuffle rebalances data so each partition writes fewer, well-sized files — prevents the small-file problem *at write time*.
Confs (precedence high→low): writer option `optimizeWrite` > `spark.databricks.delta.optimizeWrite.enabled` (off) > table prop `delta.autoOptimize.optimizeWrite` (off). `optimizeWrite.binSize` default **512 MiB**; `numShuffleBlocks` 50M; `maxShufflePartitions` 2000.
**Patterns:** replaces manual `coalesce(n)`/`repartition(n)`; partitioned sinks.
**Anti-patterns/tradeoff:** the pre-write **shuffle adds latency + spill cost** — not for ultra-low-latency writes or already-large outputs.

### Auto compaction (`autoCompact`) `[3.2.0]` (since 3.1.0)
**What:** after a write commits, synchronously runs a small OPTIMIZE on dirs with too many small files. Targets ~**128 MB** (smaller than manual OPTIMIZE's 1 GiB).
Confs: `spark.databricks.delta.autoCompact.enabled` (off) / table prop `delta.autoOptimize.autoCompact`; `autoCompact.maxFileSize` 128MB; `autoCompact.minNumFiles` **50**; `minFileSize` ~64MB.
**Patterns:** frequent small/incremental (streaming) writes where you don't want a separate OPTIMIZE job.
**Anti-patterns/tradeoff:** adds tail latency to every write; prefer a scheduled OPTIMIZE-to-1GiB for batch tables.

### File-size tuning `[3.2.0]` / `[DBX-ONLY]`
OSS lever is `spark.databricks.delta.optimize.maxFileSize` (1 GiB). **`delta.targetFileSize` and `delta.tuneFileSizesForRewrites` do NOT exist in OSS Delta 3.2.0 (Databricks-only).** Larger files = fewer-file scans but heavier rewrites + coarser MERGE; smaller files help MERGE but reintroduce small-file overhead.

## Data layout for skipping

### Data skipping + column stats `[3.2.0]`
**What:** Delta auto-collects per-file min/max on the **first `delta.dataSkippingNumIndexedCols` (default 32)** columns; queries skip non-matching files for free.
**Patterns:** put high-selectivity filter columns in the first 32 (reorder schema via `ALTER COLUMN`), pair with ZORDER/clustering.
**Anti-patterns:** long string/binary columns early → stats bloat the `_delta_log` with little skip benefit (lower the count or use `delta.dataSkippingStatsColumns`). Skipping is only as good as the layout.

### Z-ORDER `[3.2.0]` (since 2.0.0)
**What:** multi-dimensional clustering co-locates related rows so min/max stats are tight → prunes far more files for high-cardinality, non-partition filter columns.
```sql
OPTIMIZE events WHERE date='2024-11-18' ZORDER BY (eventType);
```
**Limitations/anti-patterns:** **NOT idempotent** (re-clusters + pays IO every run); balances by *tuple count* not bytes (task skew); only on stats-collected cols; low-cardinality cols buy little; >~4 cols dilutes. Cannot ZORDER on partition columns.

### Liquid clustering (`CLUSTER BY`) `[3.2.0]` — **GA in OSS 3.2.0**
**What:** one evolvable clustering layout that replaces partitioning + ZORDER; data is rewritten **incrementally** (ZCube-based in 3.2.0, less write amplification).
**Version note:** preview in 3.1.0 (needed `enableClusteringTablePreview`); in **3.2.0 the preview flag is removed → fully GA**. SQL `CREATE TABLE … CLUSTER BY` + the `DeltaTable.clusterBy()` API + `ALTER TABLE … CLUSTER BY (…)` / `CLUSTER BY NONE` are in 3.2.0. (`OPTIMIZE FULL` is a later, 3.3 addition.)
```sql
CREATE TABLE t (id INT, c STRING) USING DELTA CLUSTER BY (id, c);  -- 2-4 cols (OPTIMIZE clustering needs >=2)
OPTIMIZE t;   -- incremental clustering
```
**Limitations:** **2–4 clustering columns** — max 4, and **OPTIMIZE clustering needs ≥2** (a single `CLUSTER BY (col)` table is created/inserted fine, but `OPTIMIZE` errors `Cannot do Hilbert clustering by zero or one column` — live-verified on 3.2.0); stats-eligible cols only (first 32); **incompatible with partitioning AND with ZORDER**; needs writer protocol v7 + reader v1 (+ `clustering`/`domainMetadata` table features) — protocol can't be downgraded (older writers locked out; any Delta client can still read).
**Patterns:** high-cardinality filters, skewed/evolving access patterns, fast-growing tables — pick this instead of partitioning by a high-cardinality column or ZORDER.
**Anti-patterns:** you need a physical `partitionBy` layout for an external tool; clients that can't meet protocol v7.

### Partitioning vs clustering `[3.2.0]`
`PARTITIONED BY (col)` = one directory per value → great for **low/medium-cardinality** filter columns (date/hour/region). **High-cardinality partitioning hurts**: partitioning by `user_id`/uuid/second-timestamp explodes into millions of tiny dirs/files (small-file tax) and at write time every task writes every partition (`tasks × partitions` files). For high-cardinality skipping use **ZORDER or CLUSTER BY**, not partitioning.
```python
df.repartition("event_hr").write.partitionBy("event_hr").format("delta").save(path)  # control files/partition
```
Also: `spark.sql.sources.partitionOverwriteMode` defaults **STATIC** (overwrites the whole table) — set **DYNAMIC** for partition-scoped reloads (see `03-file-layout-io.md`).

### Generated columns `[3.2.0]`
Derive a low-cardinality partition column from a raw column (e.g. `DATE GENERATED ALWAYS AS (CAST(ts AS DATE))`), then partition on the derived column while filtering on the raw one (generated partition filters prune partitions). Scalar deterministic expressions only; expression is locked at creation; raises the protocol.

## Avoiding rewrites (DML / merge-on-read)

### Deletion vectors (merge-on-read) `[3.2.0]`
**What:** soft-delete — mark rows removed in a sidecar bitmap instead of rewriting whole Parquet files; deletions applied at read. **Off by default** (`delta.enableDeletionVectors=false`; opt in). GA per-op by 3.2.0: SCAN (2.3+), DELETE (2.4+), UPDATE/MERGE (3.1+). 3.2.0 dropped DV broadcast + added predicate pushdown → ~2× read perf on DV tables.
```sql
ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors'=true);
REORG TABLE t APPLY (PURGE);   -- materialize/remove DVs (idempotent)
```
**Patterns:** frequent point DELETE/UPDATE/MERGE on large tables (GDPR deletes, CDC merges with few changed rows/file).
**Anti-patterns/tradeoff:** read path pays a DV-apply cost; raises reader+writer protocol (older clients locked out; 3.0+ can drop the feature); soft-deleted data lingers until VACUUM (privacy/storage).

### MERGE performance `[3.2.0]`
**Tips:** (1) **Add partition/clustering predicates to the `ON` clause** — MERGE scans the *whole* target by default. (2) Compact small files first. (3) Deletion vectors avoid full-file rewrites for matched rows. (4) Pre-dedupe the source (duplicate source keys fail the merge). (5) `spark.databricks.delta.merge.repartitionBeforeWrite.enabled` (false) → fewer small files on partitioned targets. (6) Balance `spark.sql.shuffle.partitions` (MERGE shuffles several times — too high = small-file explosion).
**Anti-pattern:** no `ON` predicate on a huge target = full scan + conflict-prone. (See JOIN-over-MERGE for full reloads: `06-caching-materialization.md`.)

### Change Data Feed (CDF) `[3.2.0]` (since 2.0; off by default)
**What:** records row-level inserts/updates/deletes for downstream incremental consumption. `ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed=true)`; read via `table_changes('t', startV, endV)` or `.option("readChangeFeed","true")` (batch **and** streaming in 3.2.0). `_change_type` ∈ insert/update_preimage/update_postimage/delete.
**Anti-patterns/tradeoff:** extra `_change_data` storage + write cost; only captures changes **after** enablement (no backfill).

## Schema & maintenance

### Schema evolution `[3.2.0]`
`mergeSchema` (append-time add columns), `overwriteSchema` (rewrite on overwrite), `spark.databricks.delta.schema.autoMerge.enabled` (false; also covers MERGE auto-evolve). Anti-pattern: leaving autoMerge always-on → silent schema drift; type changes need a full rewrite.

### Column mapping `[3.2.0]`
`delta.columnMapping.mode` = `none` (default) / `name` / `id` → RENAME/DROP become metadata-only (no rewrite). Raises writer protocol; **can't be turned off once enabled**; DROP doesn't delete underlying data; interacts with CDF/streaming on non-additive changes.

### VACUUM `[3.2.0]`
**What:** physically deletes data files unreferenced by the log AND older than retention. Does **not** delete log files.
```sql
VACUUM t;                    -- default 7-day (168h) retention
VACUUM t DRY RUN;            -- preview
VACUUM t USING INVENTORY inv -- 3.2: skip full dir listing on huge tables
```
Confs: table prop `delta.deletedFileRetentionDuration` (**7 days**); `spark.databricks.delta.retentionDurationCheck.enabled` (**true**, guard); `vacuum.parallelDelete.enabled` (false).
**⚠ Data-loss/time-travel risk:** shrinking `RETAIN` below 7 days (and disabling the check) can destroy time travel and **delete files an in-flight reader still needs** → corrupt that query. Only shrink if no transaction runs longer than the new window.

### Checkpoints & log retention `[3.2.0]`
Delta periodically writes a Parquet checkpoint (`delta.checkpointInterval`, default **10** commits) to collapse the JSON log; old log JSONs are cleaned past `delta.logRetentionDuration` (**30 days**). **Two independent retention windows:** VACUUM governs *data* files (`deletedFileRetentionDuration`, 7d); log retention governs *log* files (30d). Long log retention enables deeper time travel but grows `_delta_log`.

## Availability quick-table (OSS 3.2.0)

| Feature | Status |
|---|---|
| OPTIMIZE/compaction, ZORDER, VACUUM, data skipping, schema evolution, column mapping, generated columns, CDF, partitioning | `[3.2.0]` GA |
| Optimized writes, auto compaction, deletion vectors (SCAN/DELETE/UPDATE/MERGE) | `[3.2.0]` GA |
| **Liquid clustering (`CLUSTER BY`)** | `[3.2.0]` GA (preview flag removed) |
| Row tracking; UniForm (Iceberg + **Hudi new in 3.2**) | `[3.2.0]` |
| Type widening; in-commit timestamps | `[PREVIEW]` in 3.2.0 — don't use in prod |
| `OPTIMIZE FULL`, Python `clusterBy()` builder extras, identity columns | `[>3.2]` (3.3+) |
| `delta.targetFileSize`/`tuneFileSizesForRewrites`, Photon, predictive optimization, Delta disk cache, `delta.parquet.compression.codec` | `[DBX-ONLY]` |

## Quick decisions
- Small files on read? → OPTIMIZE recent partitions, **then schedule VACUUM**; or turn on optimized writes / auto-compaction to prevent them.
- Filter on a high-cardinality non-partition column? → ZORDER (simple) or CLUSTER BY (evolvable, GA in 3.2.0) — never partition by it.
- Frequent point deletes/updates/merges? → enable deletion vectors; add ON-clause predicates; `REORG … PURGE` + VACUUM to reclaim.
- Writing many partitions and getting tiny files? → `optimizeWrite` + zstd + bin size (the recipe above).
- Reclaiming storage after compaction/DML? → VACUUM (respect the 7-day floor).
