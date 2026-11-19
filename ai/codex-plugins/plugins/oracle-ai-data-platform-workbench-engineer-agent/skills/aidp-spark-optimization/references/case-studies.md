# Case studies — real before/after evidence

Use these numbers when proposing a technique (sets expectations) and as templates for the measurement loop. Two sources: **AIDP reproductions** (4-core `ephemeral_01`, Spark 3.5.0, full Spark UI evidence) and **field engagements** (large clusters). Numbers are workload-specific — they show direction and magnitude, not guarantees.

## AIDP reproductions (4-core cluster, Spark 3.5.0)

| # | Optimization | Before → After | Key signal | Ref |
|---|---|---|---|---|
| R1 | **Skewed join → semi-join pre-filter + broadcast** (1GB fact ⋈ 128MB dim on skewed key) | 71s → 59.5s (−16%) | SortMergeJoin → BroadcastHashJoin; **task skew 3.0x → 1.3x**; dim through join 128MB → 13.4MB | `02-joins.md` |
| R2 | **Iterative union + driver loop → Row-list + cache + single action** (20-table bronze→silver loop) | 2m33s → 1m02s (~2.5x) | metadata lookup **80 tasks → 1 task** per action; 408KB result no longer fans out | `06-caching-materialization.md`,`01-partitioning.md` |
| R3 | **Wide-aggregation codegen → lower `maxFields`** (118 agg fields, 12 groups, 10.9M rows) | 26.56s → 18.76s (1.42x) | same scan/shuffle; executor CPU 94.7s → 67.0s; **p50 task 6055ms → 4252ms** | `05-codegen.md` |
| R4 | **Delta small files → OPTIMIZE (compaction)** (50 appends; Delta 3.2.0) | full-scan read **8.37s → 0.50s (~16×)** | active files **200 → 1**; physical files **206 → 6 only after VACUUM** (OPTIMIZE leaves stale files) | `08-delta-lake.md` |
| R5 | **Delta optimized writes** (`optimizeWrite`) | **40 files → 1** in one write pass | prevents the small-file problem at write time (no separate OPTIMIZE) | `08-delta-lake.md` |
| R6 | **Delta deletion vectors** on a 1-row DELETE | full-file rewrite → **none** | DV off: parquet 5→6 (file rewritten); DV on: 5→5 + a `.bin` deletion vector | `08-delta-lake.md` |

## Field engagements (large clusters)

| # | Optimization | Before → After | Note | Ref |
|---|---|---|---|---|
| F1 | **UNION of same table → array + explode** | 15 min → 5 min (~67%) | single scan + narrow explode instead of double read+join | `02-joins.md` |
| F2 | **Repartition cached table on join key before caching** | 5 min → 3 min (−40%) | large cached side no longer reshuffled on join | `06-caching-materialization.md` |
| F3 | **Avoid unnecessary materialization** (intermediates as views) | 28 min → 22 min | enables job parallelism; less I/O; SLA 30 min | `06-caching-materialization.md` |
| F4 | **Skewed streaming join → semi-join pre-filter + broadcast** | stage max 21 → 3.5 min; micro-batch 13–14 → 7–8 min | dims 4GB→68MB, 1.2GB→200MB; SLA 11 min | `02-joins.md` |
| F5 | **Codegen on wide aggregation → lower `maxFields`** | 7.9 h → ~23 min | HashAggregate cumulative 9691h → <500h; sibling pipeline with >100 fields was already fast (codegen auto-off) | `05-codegen.md` |
| F6 | **MERGE → full-outer JOIN + dynamic partition overwrite** | 5 h+ (killed) → 26 min | `partitionOverwriteMode=DYNAMIC`; rewrite upsert as join | `06-caching-materialization.md`,`03-file-layout-io.md` |
| F7 | **zstd output compression** (vs gzip) | 30m13s → 13m37s | output 953GB → 944GB; runtime less than half | `03-file-layout-io.md` |
| F10 | **Oracle/Exadata write: conventional row-style INSERT → DB-side disable redo-shipping ("ARS") + NOLOGGING + direct-path** | 6h+/up to ~20TB redo → ~2h/~4.3TB redo | disabling ARS cleared the archive stall (`log file switch (archiving needed)` gone); `redo log space requests` 36.3M → ~1,400 | `09-oracle-database.md` |
| F11 | **ADW read: raise `fetch.size`** (8.75M narrow rows) | 33s → 11s (~67%) | `fetch.size` 1000→1,000,000 (+2 partitions); fetch size beat partition count | `09-oracle-database.md` |

### F8 — Config-tuning ladder (large media/streaming pipeline, 24× 16-OCPU/128GB workers)

Cumulative effect of stacking configs on one pipeline:

| Change | Runtime |
|---|---|
| Baseline (post-compaction) | 56 min |
| `+ autoBroadcastJoinThreshold = 1gb` | 48 min |
| `+ shuffle.partitions = 768` (≈2–3× cores) + AQE coalesce (`advisory=128mb`, `parallelismFirst=false`) | 44 min |
| `+ preferSortMergeJoin = false` (Shuffle-Hash) | 43 min |
| `+ io.compression.codec=zstd` (level 3, buffer 1024kb) `[cluster-create]` | 40 min |
| `+ memory.fraction=0.7` `[cluster-create]`, `parquet.compression.codec=zstd` | <40 min |

Spill (disk) dropped 1051.7GB → 137.9GB from the zstd shuffle codec. With 2× resources ≈ 30 min; ½ resources ≈ 70 min (≈ linear).

### F9 — Memory right-sizing + compression (2TB gzip-CSV → Delta ingest)

| Step | Result |
|---|---|
| 13× (4-OCPU/64GB), `memory.fraction=0.6` | 18m25s, **915GB mem + 494GB disk spill** |
| → 128GB workers, `memory.fraction=0.9` `[cluster-create]` | 15m35s, **no spill** (peak exec 88GB) |
| → 200GB peak (4 partitions) + zstd | 6m14s, no spill (peak exec 66GB ≈ 4× file size) |
| → 2TB (10× volume), scaled cluster | 8m15s (no degradation) |

Codec at 2TB: zstd 8m22s / 1889GB · gzip 10m47s / 1906GB · snappy 15m7s / 3TB.

### F10 — Oracle/Exadata redo-log saturation (multi-TB Gold load)

A PySpark pipeline wrote a multi-TB Gold layer into an Exadata (Oracle 23ai) database. With **no bulk-load path in use** (the managed `DBMS_CLOUD` bulk writer is the ADW/ATP path), the load ran as conventional-path, fully-logged `INSERT … VALUES`.

| | Before | After |
|---|---|---|
| End-to-end | 6h+ with restarts | ~2h, no restarts |
| Redo for the load | up to ~20 TB | ~4.3 TB |
| `redo log space requests` (AWR) | 36,278,585 | 1,398 |
| Top wait | `log file switch (archiving needed)` ~24% DB time, ~85s/wait | gone (now network-ingest bound) |

Root cause: redo generated faster than the DB could archive (archiving ~30× slower than redo fills). **Fix = DB-side**: target tables/PDB set to `NOLOGGING` **and** the managed redo-shipping/backup service (ARS = Oracle Autonomous Recovery Service, confirmed by an Exadata SME) disabled — with the load made **direct-path** (conventional DML logs regardless of `NOLOGGING`). **Tradeoffs:** `NOLOGGING` blocks aren't media-recoverable (keep post-load RMAN backups); reduced recovery guarantees → acceptable only on a dedicated load DB; if the DB hosts other critical/compliant data, use a **separate** DB for AIDP writes. Spark-side levers (batchsize, bounded parallelism) reduce but don't remove conventional-path redo — the real lever is the write path. See `09-oracle-database.md`.

## Reading the evidence

- The **mechanism** transfers; the **magnitude** depends on data shape, skew, and cluster. Re-measure.
- "What didn't help" in the field (verify per workload, don't apply blindly): `cbo.enabled=true` with stale stats, larger `columnarReaderBatchSize`, custom DMA buffer sizes, blanket `coalesce`, caching the entire source table.
- Counterintuitive: a wide-aggregation pipeline with **more** fields can be **faster** because it crosses `codegen.maxFields` and falls back to interpreted execution (F5, R3).
