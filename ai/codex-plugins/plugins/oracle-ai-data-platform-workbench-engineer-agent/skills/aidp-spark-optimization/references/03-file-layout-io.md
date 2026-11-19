# File layout & I/O (Apache Spark 3.5.0 + OSS Delta Lake)

Symptoms that route here: a job spends minutes *before any compute* (file listing/opening), thousands of tiny tasks/files, a slow scan that reads far more "cost" than data, one gigantic file getting no parallelism (1 task), output is bloated, or an overwrite wiped partitions it shouldn't have.

**Core idea:** I/O cost = *data bytes* + *per-file open overhead* + *(de)compression CPU*. Aim for ~128MB units everywhere — input partitions, output files, compacted files. Too many small files explodes open-overhead and task count; too few huge unsplittable files kills parallelism. Pick a codec that is tight *and* keeps data splittable downstream (zstd-in-parquet).

All `spark.sql.*` here are runtime-settable from a notebook **[notebook]**. The shuffle/spill codec (`spark.io.compression.*`) is **[cluster-create]** — see `04-memory-and-spill.md`. On a shared cluster a notebook `spark.conf.set` LEAKS to other notebooks on the one SparkSession — revert with `spark.conf.unset(...)` or restore the prior value (`aidp-notes.md`).

---

## The small-file problem (listing + opening overhead)

**What:** Spark charges a fixed *open cost* per file when planning a scan, modelled as `spark.sql.files.openCostInBytes` (default 4MB). Reading N files costs roughly `data_bytes + N * 4MB`, independent of how small each file is.
**Why it matters:** with enough files, open overhead dwarfs the actual data and inflates task count (each ~128MB read partition becomes many tiny ones), which pressures the driver (listing, task scheduling) and the scheduler. This is an I/O + scheduling tax, not a compute one. There is also a **write-side** cost on object storage: a small-file write burst is many PUT requests, and **OCI Object Storage can return HTTP 429 (Too Many Requests)** under that rate. So prefer to *prevent* small files at write time — the four approaches (optimized writes / AQE coalesce *prevent*; OPTIMIZE / auto-compaction *compact after* and need VACUUM) are compared in `08-delta-lake.md`.

**Patterns (use when):**
- Source is a directory of 10k+ (or 100k+) small files; the scan stage's "input size" is small but wall time is large and dominated by task count / first-stage latency.
- A streaming sink or a high-cardinality `partitionBy` has been appending file-per-microbatch / file-per-task for a long time.
- Field engagement: a peak (3x-records) workload missed its **90-min SLA, running 2h+**, because the source had **261K files**. 400GB across 261K files → read cost ≈ `400GB + (261K × 4MB) = 400GB + ~1TB` — **opening cost was ~2.5x the actual data**. The same 400GB across **800 files** → `400GB + (800 × 4MB) = 400GB + 3.2GB` (open cost negligible).

**Anti-patterns (avoid when):**
- A few large files already (~128MB+ each): open cost is already negligible — tune nothing here; look at partitioning/joins instead.
- "Fixing" small files by cranking `maxPartitionBytes` so high that each task reads many GB and spills — pair file-size tuning with the memory math in `04-memory-and-spill.md`.

**Apply:**
```python
# before (the smell): reading a directory of hundreds of thousands of small files
df = spark.read.parquet("oci://bucket/raw/")   # 400GB across 261K files -> +~1TB open cost

# after (the fix): two complementary moves
# 1) Compact the SOURCE so future reads see ~128-512MB files (see "Compaction" below).
# 2) For one-off reads of an unfixable source, let a task absorb more than one
#    small file by raising the per-task budget so the open cost is amortized:
spark.conf.set("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)  # 256MB/task
# openCostInBytes also acts as the minimum "spacing" used when packing files into a
# partition; lowering it makes Spark pack more small files per task (less open-cost padding).
spark.conf.set("spark.sql.files.openCostInBytes", 1 * 1024 * 1024)      # 1MB (from 4MB)
df = spark.read.parquet("oci://bucket/raw/")
```
Config:
| key | default | suggested | tag |
|---|---|---|---|
| `spark.sql.files.maxPartitionBytes` | 128MB | keep 128MB (field-confirmed good default); raise to 256MB+ to make a task read more than one small file | [notebook] |
| `spark.sql.files.openCostInBytes` | 4MB (`.internal`) | lower (e.g. 1MB) to pack more small files per task; advanced — tune with care | [notebook] |

**Evidence:** field engagement — compacting the 261K-file source (33 min OPTIMIZE) brought a 2h+ pipeline under the 90-min SLA (post-compaction baseline 56 min, then <40 min after further tuning).
**Detect in Spark UI:** scan stage with huge *task count* but small *input size*; long gap before the first stage starts (driver listing). Cross-ref `diagnosis.md` and `01-partitioning.md` (tiny-partition fan-out).

---

## Compaction (OSS Delta OPTIMIZE / bin-packing)

**What:** rewrite many small files into fewer well-sized files. In OSS Delta Lake this is `DeltaTable.forPath(...).optimize().executeCompaction()` (or SQL `OPTIMIZE <table>`), which bin-packs files and is idempotent (a compacted table is a no-op on re-run).
**Why it matters:** it permanently removes the open-cost tax for every future reader, not just the current job. Trades one-time compaction CPU/IO for repeated read speedups.

**Patterns (use when):**
- You own the table being read repeatedly and it has accumulated small files (append-heavy / streaming sink / over-partitioned writes).
- You can spend a one-time compaction window; OPTIMIZE supports a `WHERE partition_predicate` to compact only recent/affected partitions.

**Anti-patterns (avoid when):**
- The table is read once and discarded — compaction cost won't be amortized.
- You set `minFileSize >= maxFileSize`. **Gotcha (field revelation):** keep the *min* size strictly **lower** than the intended *max* size, otherwise almost every file looks "eligible" (too small) and you re-pack far more than intended. min picks files smaller than this; max is the target output size.

**Apply:**
```python
from delta.tables import DeltaTable

# Pick files smaller than 128MB, rewrite toward 512MB targets. min < max is mandatory.
spark.conf.set("spark.databricks.delta.optimize.minFileSize", 128 * 1024 * 1024)   # 128MB: compact files below this
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 512 * 1024 * 1024)   # 512MB: target output size
spark.conf.set("parquet.block.size", 256 * 1024 * 1024)                            # 256MB row groups
spark.conf.set("spark.databricks.delta.optimize.maxThreads", "32")                 # rewrite parallelism (OSS default 15; raise toward cluster cores)

compaction_df = (
    DeltaTable
    .forPath(spark, spark.sql(f"DESCRIBE DETAIL {table_name}").head().location)
    .optimize()
    # .where("event_hr >= '...'")   # optional: compact only recent partitions
    .executeCompaction()
)
```
Note: the `spark.databricks.delta.optimize.*` keys are honored by **OSS Delta Lake** (the `databricks` prefix is historical) and are [notebook] Delta confs governing the OPTIMIZE write op. SQL form: `OPTIMIZE <table> [WHERE <part_pred>]`; target size via `spark.databricks.delta.optimize.maxFileSize` (OSS default **1 GiB**; the deck set it smaller for more files). Note `delta.targetFileSize` is Databricks-only — NOT in OSS Delta 3.2.0. **Full Delta treatment + correct 3.2.0 defaults: `08-delta-lake.md`. And remember: OPTIMIZE leaves the old files until VACUUM removes them past retention.**

Config:
| key | role | suggested | tag |
|---|---|---|---|
| `spark.databricks.delta.optimize.minFileSize` | compact files smaller than this | 128MB (must be `< maxFileSize`) | [notebook] (Delta) |
| `spark.databricks.delta.optimize.maxFileSize` | target output file size | 512MB | [notebook] (Delta) |
| `spark.databricks.delta.optimize.maxThreads` | rewrite parallelism | size to cluster | [notebook] (Delta) |
| `parquet.block.size` | row-group size in output | 256MB | [notebook] |

**Evidence:** field engagement — compaction step **33 min**; it let the rest of the pipeline run at **56 min baseline → <40 min** after tuning, comfortably under the 90-min SLA (≈17 min headroom even counting the 33-min compaction).
**Detect in Spark UI:** see the small-file detection above. Best fix is upstream: have the *writer* produce ~128MB files (AQE coalescing — `01-partitioning.md`, `07-aqe.md`) so compaction isn't needed.

---

## Delta data skipping + ZORDER + VACUUM (OSS Delta Lake)

**What:** OSS Delta keeps per-file min/max stats on the first `dataSkippingNumIndexedCols` columns (default 32) and skips files whose stats can't match a predicate. `ZORDER BY (cols)` multi-dimensionally clusters data so those stats are tight on the chosen columns. `VACUUM` removes files no longer referenced by the log.
**Why it matters:** data skipping turns a full scan into a partial scan with no code change — it's free at read time once the layout/stats exist. ZORDER makes skipping effective for *non-partition* filter columns.

**Patterns (use when):**
- Queries filter on a high-cardinality column that is not (and shouldn't be) a partition column — ZORDER on it so file-level min/max prune most files.
- 2-4 frequently-filtered columns; ZORDER co-locates them.
- VACUUM after compaction/ZORDER to reclaim the now-orphaned small files.

**Anti-patterns (avoid when):**
- ZORDER on a low-cardinality column (few distinct values) — clustering buys almost nothing.
- More than ~2-4 ZORDER columns — effectiveness dilutes across dimensions.
- Re-running ZORDER expecting idempotence — ZORDER is **NOT idempotent** (re-clusters each run; costs IO).
- Stats columns are long strings — they bloat the log; reorder the schema or lower `dataSkippingNumIndexedCols` so stats land on useful columns.
- `VACUUM ... RETAIN` below the default 7 days — breaks time-travel and can delete files an in-flight concurrent reader/transaction still needs.

**Apply:**
```python
# Cluster by the columns you filter on (not partition columns); then reclaim orphans.
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (customer_id, event_type)")
spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")   # 168h = 7d default; do NOT go lower casually
```
Config:
| key | default | note | tag |
|---|---|---|---|
| `spark.databricks.delta.optimize.*` | (see Compaction) | ZORDER reuses OPTIMIZE plumbing | [notebook] (Delta) |
| `dataSkippingNumIndexedCols` (table prop / conf) | 32 | first N cols get min/max stats; reorder schema to put filter cols early | [notebook] (Delta) |

OSS note: ZORDER is open-source, and **liquid clustering (`CLUSTER BY`) is also GA in OSS Delta 3.2.0** (max 4 cols; not compatible with partitioning or ZORDER — see `08-delta-lake.md`). "Column Statistics in delta tables" was on the field team's *further-exploration* list (not yet proven on these engagements) — treat ZORDER+stats as available but validate impact per workload.
**Detect in Spark UI:** scan reads far more files/bytes than the predicate selectivity implies; check the SQL node's "number of files read" vs total. Predicate not pushed → see next section.

---

## Compression codecs: zstd vs gzip vs snappy

**What:** choice of codec for **output parquet** (`spark.sql.parquet.compression.codec`). Three operating points: gzip (tight, slow CPU), snappy (fast CPU, loose ratio), **zstd** (tight like gzip, fast like snappy — best of both, the field default).
**Why it matters:** the codec drives output size (storage + downstream read bytes) *and* (de)compression CPU. zstd wins on both axes vs the two extremes for these workloads.

**Patterns (use when):**
- Writing parquet/Delta output and you want smaller files without paying gzip's CPU — default to zstd.
- Output size or downstream read cost matters (it usually does).

**Anti-patterns (avoid when):**
- Confusing the *output* codec with the *shuffle/spill* codec. `spark.sql.parquet.compression.codec` [notebook] controls files on disk; `spark.io.compression.codec` (shuffle & spill, default lz4) is **[cluster-create]** and needs a restart — cover that in `04-memory-and-spill.md`. They are independent knobs.
- Assuming gzip-compressed *CSV* behaves like gzip-in-parquet — it does not (see splittability below).

**Apply:**
```python
# before (the smell): default snappy output, or gzip output paying heavy CPU
df.write.mode("overwrite").format("delta").partitionBy("event_hr").save(target_path)

# after (the fix): zstd parquet output
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")   # default is snappy
df.write.mode("overwrite").format("delta").partitionBy("event_hr").save(target_path)
```
Config: `spark.sql.parquet.compression.codec` = default `snappy` → suggested `zstd`  [notebook].
(For the shuffle/spill side: `spark.io.compression.codec` = default `lz4` → `zstd`, with `spark.io.compression.zstd.level=3`, `zstd.bufferSize=1024kb` — all **[cluster-create]**, see `04-memory-and-spill.md`.)

**Evidence:**
- Field engagement: switching output to zstd cut a write step **30m13s → 13m37s (≈half)** while *also* shrinking output **953GB (gzip) → 944GB (zstd)** — faster *and* smaller.
- Field (2TB gzipped-CSV ingest into Delta): output parquet codec comparison — **zstd 8m22s / 1889GB**, **gzip 10m47s / 1906GB**, **snappy 15m7s / 3TB**. zstd is fastest and tightest; snappy's loose ratio nearly doubles storage.
**Detect in Spark UI:** large output "bytes written"; slow write stage with high CPU (gzip) — cross-check codec in the Environment tab.

---

## gzip unsplittability — the file-layout angle

Whole-file gzip (`.csv.gz`) can't be split across tasks (**one gzip file = one task**). The *parallelism ceiling* this creates and the "size the cluster to file count" response are the **canonical treatment in `01-partitioning.md`** (cross-ref `cluster-sizing.md`). Here is only the file-layout takeaway:

- **gzip *in parquet* IS splittable.** Parquet is row groups (~128MB), so a gzip- or zstd-compressed *parquet* file parallelizes downstream at the row-group boundary. Unsplittability is a problem for gzipped *CSV/text*, not parquet.
- **So when you must ingest unsplittable gzipped CSV, write the output as (zstd) parquet/Delta** — it stays splittable for every downstream reader, turning a one-task-per-file source into a normally-parallel dataset. Field (2TB ingest): zstd-parquet output stayed well under its 15-min ETA (**8m15s**) and read fine from an ADW external table (19c).
- For splittable input, `spark.sql.files.maxPartitionBytes` (128MB→256MB) widens a task; for many small files, `openCostInBytes` bin-packs them (see the small-file section above).

**Detect in Spark UI:** scan stage with task count == file count and idle cores while a few long tasks run. For the parallelism-ceiling diagnosis + cluster sizing, go to `01-partitioning.md`.

---

## partitionOverwriteMode: STATIC → DYNAMIC

**What:** when overwriting a partitioned table, `spark.sql.sources.partitionOverwriteMode` defaults to **STATIC**, which replaces the **entire** table (truncate + load effect). **DYNAMIC** overwrites only the partitions present in the DataFrame being written and leaves the rest intact.
**Why it matters:** STATIC on a partitioned reload silently deletes partitions you didn't intend to touch (data loss / re-load of everything). DYNAMIC is the correct mode for partial/partition-scoped reloads and full-reload-as-join patterns — with no performance penalty (field-confirmed).

**Patterns (use when):**
- A pipeline rebuilds only a few partitions (e.g. the current hours) but writes `mode("overwrite")` to a table with many partitions.
- The JOIN-over-MERGE full-reload pattern (`06-caching-materialization.md`) that writes a recomputed partition set back over the base table.

**Anti-patterns (avoid when):**
- You genuinely intend to replace the whole table — STATIC is correct then.
- You forget to set it on a shared session — it leaks; set it right before the write and unset after, or pin it per write.

**Apply:**
```python
# before (the smell): overwrite a partitioned table -> STATIC default WIPES all other partitions
upsert_df.write.partitionBy("event_hr").mode("overwrite").format("delta").save(base_path)

# after (the fix): only the partitions present in upsert_df are replaced; others untouched
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
upsert_df.write.partitionBy("event_hr").mode("overwrite").format("delta").save(base_path)
```
Config: `spark.sql.sources.partitionOverwriteMode` = default `STATIC` → suggested `DYNAMIC` for partition-scoped reloads  [notebook].

**Evidence:** field — in the 2TB ingest the use case overwrites only **4 partitions**, not 240; DYNAMIC restricts the write to present partitions and **performance did not degrade**. Used together with the JOIN-over-MERGE rewrite (5h+ killed → ~26 min).
**Detect in Spark UI:** not a UI metric — a correctness/semantics check. Verify the table's partition list before/after, or read the write plan.

---

## Predicate pushdown, column pruning, partition pruning

**What:** push filters and column selection down to the file reader so Spark reads fewer bytes. Parquet/ORC support **predicate pushdown** (skip row groups via min/max) and **column pruning** (read only selected columns). **Static partition pruning** drops whole partition directories when a `WHERE` hits a partition column; **dynamic partition pruning (DPP)** prunes the partitioned fact at runtime using keys from a joined dimension (star schema).
**Why it matters:** all of these reduce I/O for free (no shuffle, no extra stage). Column pruning and static pruning are "always good"; DPP avoids scanning the whole fact in fact/dim joins.

**Patterns (use when):**
- `select()` only the columns you need, as early as possible (auto for parquet/orc).
- Filter on a partition column → static pruning; filter through a join on a partitioned fact → DPP (on by default).
- Filterable predicate on a parquet/orc column → pushdown skips row groups.

**Anti-patterns (avoid when):**
- CSV/JSON sources — they don't support predicate pushdown (no row-group stats). Convert to parquet/Delta to get it.
- A `CAST` or UDF wrapped around the filtered column **blocks pushdown** — filter on the raw column, push the cast to the other side / a literal.
- Reading `*` then dropping columns later — the read already paid for them; prune at read time.

**Apply:**
```python
# before (the smell): read everything, cast inside the predicate (blocks pushdown), filter late
df = spark.read.parquet("oci://bucket/fact/")
df = df.filter(F.col("event_date").cast("string") == "2026-06-01")   # CAST blocks pushdown
out = df.select("id", "amount", "event_date", "extra1", "extra2")

# after (the fix): prune columns at read, filter on the raw (partition) column -> pushdown + pruning
df = (spark.read.parquet("oci://bucket/fact/")
        .select("id", "amount", "event_date")                # column pruning
        .filter(F.col("event_date") == "2026-06-01"))        # static partition pruning + pushdown
```
Config:
| key | default | note | tag |
|---|---|---|---|
| `spark.sql.parquet.filterPushdown` | true | leave on; pushdown for parquet | [notebook] |
| `spark.sql.orc.filterPushdown` | true | ORC pushdown | [notebook] |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | true | DPP for star-schema joins on a partitioned fact | [notebook] |

**Evidence:** general OSS behavior; reinforced by the field theme "scans are costly too" — reducing what a join/aggregation reads (filter + select before the shuffle) was part of the runtime wins (e.g. UNION-of-same-table 15→5 min by reading/joining once instead of twice).
**Detect in Spark UI:** the scan node shows `PushedFilters: []` (empty) when pushdown is blocked, or reads all columns when only a few are used; `explain(True)` shows the filter sitting *above* the scan instead of pushed into it. Cross-ref `diagnosis.md`.

---

## partitionBy cardinality (avoid partition explosion)

**What:** `partitionBy(col)` creates one directory per distinct value of `col`. High-cardinality columns explode the table into millions of tiny directories/files — re-creating the small-file problem on the write side.
**Why it matters:** good partitioning enables pruning; bad (high-cardinality) partitioning produces an unmanageable number of tiny files (open-cost tax on every future read) and a file-per-task explosion at write time.

**Patterns (use when):**
- Partition on a *low/medium-cardinality* column you filter on (date, hour, region) — e.g. `event_hr` (4 partitions/hour in the field case) enables static pruning with a sane file count.
- Before `partitionBy`, `repartition(partition_cols)` so each partition is written by few tasks (not one file per task per partition).

**Anti-patterns (avoid when):**
- `partitionBy` on a high-cardinality column (user_id, uuid, timestamp-to-the-second) — partition explosion. Use ZORDER for skipping on high-cardinality columns instead of partitioning by them.
- Writing a partitioned table without repartitioning first — every task writes into every partition → `tasks × partitions` tiny files.

**Apply:**
```python
# before (the smell): partition by a high-cardinality column -> millions of tiny dirs/files
df.write.partitionBy("user_id").format("delta").save(path)

# after (the fix): partition by a low-cardinality filter column; repartition first to control file count
(df.repartition("event_hr")                       # 1 writer group per partition value
   .write.partitionBy("event_hr")                 # low-cardinality, prunable
   .format("delta").save(path))
# Need fast filtering on user_id too? Don't partition by it -- ZORDER BY (user_id) for data skipping.
```
Config: no single conf — a layout decision. Pair with AQE coalescing (`spark.sql.adaptive.advisoryPartitionSizeInBytes` 64MB → 128MB, `coalescePartitions.parallelismFirst=false`) so writers emit ~128MB files (`01-partitioning.md`, `07-aqe.md`).

**Evidence:** field — the 2TB ingest deliberately overwrote **4 partitions/hour** (low cardinality) rather than per-finer keys; field revelation "128MB is a good default for `spark.sql.files.maxPartitionBytes`" and the practice of tuning AQE coalescing to emit fewer/larger output files (384 files → less PAR generation) are the write-side counterpart to avoiding partition explosion.
**Detect in Spark UI:** write stage emits an enormous number of output files / a deeply nested partition tree; subsequent reads hit the small-file symptom. Cross-ref `01-partitioning.md`.
