# 09 — Oracle Database (ADW / ATP / Exadata) interaction

Routes here when a Spark job **reads from or writes to an Oracle database** — Autonomous Data Warehouse (ADW), Autonomous Transaction Processing (ATP), or a classic/Exadata Oracle DB — and the source/sink is the bottleneck: a single-task JDBC read, a slow multi-TB write, redo-log pressure on the target DB, or unsupported complex (array/map/struct) types.

**Core idea:** the database is a *shared, stateful* system on the other side of a network. Two levers dominate: **(1) parallelism + round-trip size** (partition the read/write across tasks; fetch/insert in big batches, not row-by-row), and **(2) how the write lands in the DB** (bulk/direct-path load vs row-by-row conventional INSERTs — this decides redo volume and runtime). Unlike object-storage I/O, you can also overload the DB: too much parallelism or pushdown turns *your* speedup into *its* outage.

> **One fact to anchor everything below:** the write path differs by **target type**, and conflating them causes wrong diagnoses.
> - **Bulk/staged path — the AIDP managed ADW / ATP writer:** Spark writes the DataFrame to object storage as CSV (one file per partition, in parallel), then the DB bulk-loads it via **`DBMS_CLOUD.COPY_DATA`** (append / LOB / identity-column cases go through an external table + `INSERT … SELECT`). Set-based, not row-by-row. **`DBMS_CLOUD` ships built-in on Autonomous DB (ADW/ATP); the AIDP managed bulk writer targets those.** (It *can* be manually installed on a non-autonomous 19c+ DB, but that's a DBA setup the Spark writer doesn't rely on — see the classic/Exadata path next.)
> - **Conventional JDBC path — classic / on-prem / Exadata Oracle:** there is **no bulk-staging path**; rows land via **conventional-path batched `INSERT … VALUES`** (`PreparedStatement.addBatch()`/`executeBatch()`, one connection per partition). Every row is fully logged (redo + undo) — this is where the multi-TB redo-saturation problems come from. The same DataFrame that bulk-loads efficiently into ADW will, against Exadata, stream row-batches that the DB must fully log.

---

## Reading from Oracle — parallelize the read, size the fetch

**What:** a `format("jdbc")` / external-catalog read of an Oracle table. By default Spark reads it through **one task on one connection** — a single thread pulling the whole table — unless you tell it how to split the work.

**Why it matters:** two independent costs. (1) **Parallelism:** with no partitioning, one executor core does all the work while the rest idle — the read can't go faster than one connection. (2) **Round-trips:** Oracle's JDBC driver default `fetchsize` is **10 rows per round-trip** — catastrophic over a network for millions of rows (the read is latency-bound, not bandwidth-bound). AIDP's ADW reader defaults `fetch.size` to 1000, better but still low for narrow rows.

**Patterns (use when):**
- The source table is large and the read stage is a single long task (check the Spark UI: one task, full duration).
- The table has a reasonably uniform **numeric or date** column to range-partition on (an indexed PK/created-date is ideal).

**Anti-patterns (avoid when):**
- **No good partition column** — a skewed `partitionColumn` makes "hot" range-tasks slow (the partition ranges are equal-width, not equal-count). Pick a more uniform column, or split with a UNION of pre-filtered DataFrames.
- **Too many partitions** — each partition opens its **own** connection to the DB. A high `numPartitions`/`partition.num` hammers the database (connection storms, parsing, I/O) and can degrade it for everyone else. Match it to what the DB can absorb, not just to Spark's core count.
- **`fetchsize` too high** — large fetch buffers per task can OOM the executor on **wide rows** (CLOB/large VARCHAR2). Raise it deliberately, watch executor memory.

**Apply:**
```python
# Parallel, range-partitioned read (generic JDBC). lowerBound/upperBound define the range
# that is split into numPartitions equal-width slices; rows outside still come back (edge predicates).
df = (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "SCHEMA.BIG_FACT")        # or a pushdown subquery: "(SELECT ... ) t"
        .option("partitionColumn", "ID")             # numeric or date, ideally indexed & uniform
        .option("lowerBound", "1").option("upperBound", "100000000")
        .option("numPartitions", "16")               # read partitions; up to this many *concurrent* DB connections (bounded by free task slots)
        .option("fetchsize", "10000")                # default 10 (Oracle driver!) -> raise hard
        .load())

# AIDP external-catalog / aidataplatform ADW reader uses the dotted equivalents:
#   .option("partition.column","ID").option("partition.lower","1").option("partition.upper","1e8")
#   .option("partition.num","16").option("fetch.size","100000")
```

**Pushdown — let the database do the filtering:** Spark pushes **column projection, predicates/filters, aggregates, limit, and Top-N** down to Oracle (via the JDBC dialect / V2 scan), so only the needed rows/columns cross the wire. This is free and large — always project and filter *before* the data leaves the DB. You can also push an explicit query with `pushdown.sql` (AIDP) / a `dbtable` subquery. **Caveat:** a query that spans the Oracle table **and** object storage can't be pushed down; and pushing *everything* down to ADW can starve the DB's non-Spark users — push the heavy filter, not the whole pipeline.

**Evidence (AIDP ADW reader, field/blog reproduction):** 8.75M rows. 1 partition + default `fetch.size` = **33s**; 2 partitions + default = 24s; 1 partition + `fetch.size=1,000,000` = 17s; **2 partitions + large fetch = 11s (~67% faster)**. Fetch size moved the needle more than partition count for this narrow-row table.

**Detect in Spark UI:** the JDBC scan stage has **one task** (no partitioning) or a few long-running tasks (skewed `partitionColumn`); `inputBytes` arrives slowly with low CPU (round-trip-bound → raise fetch size). Cross-ref `01-partitioning.md` (JDBC parallel reads), `diagnosis.md`.

---

## Writing to Oracle — bulk/direct-path beats row-by-row, and redo is the hidden cost

**What:** landing a Spark DataFrame in an Oracle table. The single biggest performance and stability factor is **how the rows are inserted**, because it determines how much **redo** (and undo) the database must generate and archive.

**Why it matters — conventional vs direct-path inserts:**
- **Conventional-path** inserts (`INSERT … VALUES`, even batched via `executeBatch`) go through the buffer cache and generate **full redo + undo** for every row. This is what Spark's built-in JDBC writer does. For a multi-TB load this can generate *more redo than the data itself* and stall on the DB's redo/archive subsystem (see the field case below).
- **Direct-path** inserts (`INSERT /*+ APPEND_VALUES */ … VALUES` for array binds, or `INSERT /*+ APPEND */ … SELECT` from an external table / staging) write **above the high-water mark**, bypassing the buffer cache, and — **only if the table/tablespace is in `NOLOGGING` *and* the database is not in `FORCE LOGGING`** — generate minimal redo. This is the efficient path for big loads. ⚠️ **`FORCE LOGGING` overrides `NOLOGGING`** and is commonly enabled on an Exadata that has a **Data Guard standby** (to keep the standby consistent) — there, even direct-path logs in full and `NOLOGGING` is silently ignored; the DBA must confirm `FORCE_LOGGING=NO` for it to help.

> **Subtle, review-critical Oracle fact:** the plain `/*+ APPEND */` hint only triggers direct-path for `INSERT … SELECT`. On `INSERT … VALUES` it is **silently ignored** — you need `/*+ APPEND_VALUES */` (11gR2+) for direct-path array inserts. So a JDBC batched `INSERT /*+ APPEND */ … VALUES (?,?)` is still conventional-path and fully logged. This is exactly the classic/Exadata write path's situation: enabling an "append-hint" write option there does **not** make the load direct-path and does **not** reduce redo. Don't assume an append-hint option helped; verify against the actual SQL and the table's logging mode.

> **Can you do NOLOGGING + direct-path *from Spark*? Mostly no — and there's a parallelism catch (important).** Spark's built-in JDBC writer only emits **conventional-path** `INSERT … VALUES` and gives you no way to inject `/*+ APPEND_VALUES */`, so out of the box it **cannot** do direct-path. Even if you hand-roll it (custom `foreachPartition` + Oracle array binds with `APPEND_VALUES`), **Oracle direct-path insert takes an exclusive lock on the target table/segment** — so N concurrent writers into the *same* table **serialize** (effectively single-threaded), which throws away Spark's `numPartitions` parallelism. (Conventional-path inserts take *no* such lock — that's precisely *why* parallel Spark JDBC writers work at all, at the cost of full redo.) **You don't get "parallel direct-path" out of many client connections.** The ways to get **both** parallelism and direct-path:
> - **Bulk/staged (best, ADW/ATP):** Spark writes the CSVs to object storage **in parallel**, then the DB runs **one** set-based direct-path load (`DBMS_CLOUD.COPY_DATA` / external table + `INSERT /*+ APPEND */ … SELECT`), which the DB itself can parallelize internally (parallel DML). Spark parallelism is in the CSV write; direct-path efficiency is in the single DB-side load. No segment-lock contention.
> - **Partitioned target (classic/Exadata):** direct-path can run in parallel only if each writer targets a **distinct partition** (partition-level locking). Same-partition writers still serialize.
> - **Otherwise:** accept the trade — conventional-path = parallel writers + full redo; direct-path = low redo but serialized. This is *why* the field fix below was made **on the DB** (the load path + NOLOGGING), not by flipping a Spark write option.

**Patterns (use when):**
- **Large loads to ADW/ATP → the bulk path is automatic and is what you want.** The Autonomous-DB writer stages CSV to object storage (parallel, one file per partition) and bulk-loads via `DBMS_CLOUD.COPY_DATA` (+ external table + `INSERT … SELECT` for append) — set-based, can be direct-path, and scales with the **ADW ECPU count** (4 ECPU loaded ~2× faster than 1 ECPU in the reproduction). This sidesteps per-row redo entirely. Tune the Spark side by not over-partitioning (fewer, well-sized CSVs — see below).
- **Loads to classic/Exadata → you are on conventional JDBC; there is no bulk path.** You cannot make Spark bulk-load a non-Autonomous Oracle. The redo cost is then a **DB-side** decision (NOLOGGING + true direct-path), not a Spark knob — see the field case below. From Spark, the only levers are **`batchsize`** (more rows per round-trip) and **bounded parallelism** (fewer concurrent logging sessions).
- **Generic JDBC writes → raise `batchsize`** so each round-trip ships many rows (`PreparedStatement.executeBatch`), and **bound `numPartitions`** to the DB's tolerance (each partition = one writer connection). `truncate=true` (on overwrite) avoids a DROP+CREATE that loses storage params/indexes.

**Anti-patterns (avoid when):**
- **Row-by-row writes to a LOGGING table at TB scale** — the redo/archive subsystem becomes the bottleneck regardless of how fast Spark is (field case below). Reach for the bulk path or DB-side direct-path + NOLOGGING.
- **Over-parallelizing the write** — too many partitions = too many concurrent INSERT sessions contending on the same segments/indexes and multiplying redo-allocation latch waits. Also: **too many output partitions = too many CSV files** on the bulk path, slowing the object-storage `.par` generation step. Use **AQE coalescing** (`parallelismFirst=false`, advisory ~128MB) to write fewer, well-sized files (cross-ref `07-aqe.md`, `03-file-layout-io.md`).
- **MERGE/upsert via `saveAsTable`** — not supported directly; stage to a temp table then run a DB-side `MERGE` (AIDP supports `write.mode=MERGE` + `write.merge.keys`; or `saveAsTable` a staging table and `MERGE` through `oracledb`).

**Apply:**
```python
# Generic JDBC write: big batches, bounded parallelism, truncate-on-overwrite
(df.repartition(8)                                   # 8 writer connections (bound to DB tolerance)
   .write.format("jdbc")
   .option("url", jdbc_url).option("dbtable", "SCHEMA.TARGET")
   .option("batchsize", "10000")                     # rows per round-trip (default 1000)
   .option("truncate", "true")                       # overwrite without DROP+CREATE
   .mode("overwrite").save())

# Prefer the managed bulk path for large ADW/ATP loads. Write through the EXTERNAL CATALOG
# (3-part name) -- AIDP stages one CSV per partition to object storage, then bulk-loads via
# DBMS_CLOUD.COPY_DATA. (The aidataplatform read format is read-only; writes go via the catalog.)
df.write.mode("append").saveAsTable("ADW_CATALOG.SCHEMA.TARGET")
# upsert into ADW (staging + DB-side MERGE under the hood):
# (df.write.option("write.mode","MERGE").option("write.merge.keys","id")
#    .insertInto("ADW_CATALOG.SCHEMA.TARGET"))
```
| Option | Default | Suggested | Notes |
|---|---|---|---|
| `batchsize` (write) | `1000` | `5000`–`50000` | rows per `executeBatch`; bigger = fewer round-trips |
| `numPartitions` / `repartition(n)` | source-dependent | bound to DB capacity | *up to* N concurrent writer connections (bounded by task slots) |
| `truncate` (overwrite) | `false` | `true` | keeps table/indexes; avoids DROP+CREATE — applies when the new schema matches (else Spark recreates) |
| `fetchsize` (read) | **10 (Oracle driver)** / 1000 (AIDP) | `10000`–`100000` | watch executor memory on wide rows |
| `partition.column`/`lower`/`upper`/`num` (read) | none → 1 task | set for parallel read | one numeric/date column |

**Detect in Spark UI / DB:** Spark side — the write stage is one or few long tasks (raise parallelism within DB tolerance) or thousands of tiny output files (coalesce). DB side — see redo/archive symptoms below.

---

## Field case: redo-log saturation writing a multi-TB Gold layer to Exadata

**Symptom (DB-side, surfaced as a Spark SLA miss):** a PySpark pipeline wrote a multi-TB Gold layer into an Exadata (Oracle 23ai) database. Because the Exadata target had **no bulk-load path in use** (the managed `DBMS_CLOUD` bulk writer is the ADW/ATP path), the load ran as **conventional-path, row-style `INSERT … VALUES`** — fully logged. Runs took **6+ hours with restarts and generated up to ~20 TB of redo for the data load**. The database was **archive-bound**: redo logs *filled* in ~2 minutes but took **~60 minutes to archive** ("archiving a redo log takes ~30× longer than filling it"), so sessions stalled waiting for a log to free up.

**How to recognize it in an Oracle AWR report (the diagnosis):**
- **`log file switch (archiving needed)`** dominating Top Timed Foreground Events (here ~24% of DB time, ~85s *per wait*) — sessions blocked because no redo log is free to switch into.
- **`redo log space requests`** and **`redo log space wait time`** enormous (here 36.3M requests / 23.3M centiseconds) — the signature of redo generated faster than it can be archived.
- High **`latch: redo allocation` / `latch: redo copy`**, foreground **wait class "Configuration"** large.
- Top objects by physical writes = the target Gold tables (confirms the load is the redo source).

**Fix (DB-side, owned by the DBA — not a Spark config), two levers applied together:**
1. **Disable the managed redo-shipping / backup service (called "ARS" on the engagement).** This is the change that **most directly sped up the Exadata writes**: the service was shipping/archiving redo far slower than the load generated it, so every redo log stayed locked until shipped and sessions stalled on `log file switch (archiving needed)` — the **dominant wait (~24% of DB time)**. With it disabled, redo logs free up immediately, the load stops waiting on archiving, and the runtime collapses. (**ARS = Oracle Autonomous Recovery Service** — the OCI-managed backup/recovery service that ships redo off the database; confirmed by an Exadata SME.)
2. **Set the target tables / PDB to `NOLOGGING`** to cut the *volume* of redo generated (≈20 TB → ~4.3 TB here). For `NOLOGGING` to actually reduce redo the inserts must be **direct-path** — conventional-path DML logs in full regardless of the table's logging mode, so the load path itself must be direct-path/bulk. The engagement also verified the DB was **not in `FORCE LOGGING`** (`V$DATABASE.FORCE_LOGGING=NO`, PDB `FORCE_NOLOGGING=YES`) — without that, `FORCE LOGGING` (often on for a Data Guard standby) would override `NOLOGGING` and the direct-path load would still log in full.

So: **disabling ARS removed the archive bottleneck (the big runtime win); NOLOGGING + direct-path reduced how much redo was produced in the first place.** Both are DB-side; neither is a Spark knob.

**Result:** end-to-end **6h+ → ~2 hours, no restarts**; redo **up to ~20 TB → ~4.3 TB** for the same data; **`redo log space requests` 36.3M → ~1,400** and the `log file switch (archiving needed)` waits disappeared from the top events (the bottleneck shifted to network ingest — the desired state). Spark-side repartition and batch size were reviewed and deemed fine; the lever was the DB write/logging path.

**Tradeoffs (state these to the customer before recommending it):**
- **`NOLOGGING` sacrifices media recoverability** for the affected blocks: data loaded after the last backup is **not recoverable** from archived redo on a media failure (recovery marks those blocks as logically corrupt). Mitigate by taking an **RMAN backup immediately after each load** and keeping it (the engagement kept long-term RMAN backups, 1-year retention).
- **Disabling the managed recovery/backup service reduces the DB's recovery guarantees** broadly — acceptable on a **dedicated load/perf-testing database**, risky on a production DB with other workloads.
- **If the database also hosts other critical / compliance-bound datasets, provision a *separate* database for the AIDP writes** so disabling redo/recovery features for the bulk load doesn't weaken protection of the regulated data. Keep ARCHIVELOG mode where compliance requires it.

**The Spark-side takeaway:** you usually can't (and shouldn't) tune the DB's redo from Spark, but you control the **write path**. Prefer the **bulk/staged load** (object storage → `DBMS_CLOUD`/external table → direct-path `INSERT … SELECT`) over row-by-row JDBC for large volumes; it generates far less redo per row and is set-based. When forced onto JDBC, large `batchsize` + bounded parallelism reduces (but does not eliminate) the redo pressure. Surface the redo/NOLOGGING decision to the DBA early — it's a recovery-policy choice, not a knob.

---

## Complex types (array / map / struct) in ADW — JSON-as-VARCHAR2

**What:** the **Spark JDBC dialect has no type mapping for Spark's `array`/`map`/`struct`** onto Oracle columns, so writing such columns directly fails or is lossy. (Oracle *does* have object/collection types — VARRAY, nested table — and a native `JSON` type on 19c+/23ai, but the Spark JDBC writer won't target them.)

**Why it matters:** AIDP/Spark pipelines that produce nested columns (e.g. from JSON ingestion, `collect_list`, structs) need a representation Oracle can store and query.

**Pattern (the documented workaround):**
```python
import pyspark.sql.functions as F
# 1. serialize each complex column to a JSON string before writing
out = df.withColumn("tags_json",   F.to_json("tags"))      \
        .withColumn("addr_json",   F.to_json("address"))   # array / struct / map -> JSON string
#    write tags_json / addr_json as VARCHAR2 columns (drop the original nested columns)
```
```sql
-- 2. in ADW, unnest on read with JSON_TABLE + TREAT(... AS JSON) / dot-notation:
SELECT t.customer_id, j.tag
FROM   target t,
       JSON_TABLE(TREAT(t.tags_json AS JSON), '$[*]' COLUMNS (tag VARCHAR2(100) PATH '$')) j;
-- scalar access: TREAT(t.addr_json AS JSON).city.string()
```

**Anti-patterns / limitations:**
- **VARCHAR2 length limit** — a large nested column can exceed VARCHAR2 max (4000 bytes, or 32767 with `MAX_STRING_SIZE=EXTENDED`); very large JSON needs **CLOB** (its own read/write cost) or, on 19c+/23ai, a **native `JSON` column** (more efficient querying, but still written from Spark as a string and cast). Size the column to the data.
- The capability is **evolving** — no native array/struct round-trip yet; treat JSON-as-VARCHAR2 as the current bridge, not a permanent contract. No published performance numbers for this path — measure JSON parse cost (`JSON_TABLE`) if it's in a hot query.

---

## Storage Partition Join (SPJ) — skip the shuffle for a V2 Oracle source (advanced)

**What:** when a join's data comes from a **DataSource V2** connector (the AIDP external-catalog Oracle reader is V2) that **reports its partitioning**, Spark can co-partition the join and **eliminate the shuffle Exchange** entirely — the V2 analogue of bucketing. **Off by default in 3.5.0** — opt in with `spark.sql.sources.v2.bucketing.enabled=true` (it became on-by-default only in Spark 4.0).

**Why it matters:** removes the most expensive part of a join (the shuffle) when both sides are already partitioned compatibly by the source.

**Patterns (use when):** joining two large V2-sourced tables on their reported partition keys; you see a shuffle `Exchange` that the source's partitioning could make unnecessary.
**Anti-patterns (avoid when):** the join keys don't match the partition keys (set `spark.sql.requireAllClusterKeysForCoPartition=false` only if the connector supports partial matches); a small side that would just broadcast — broadcast still beats SPJ.

| Config | Default (3.5.0) | Note |
|---|---|---|
| `spark.sql.sources.v2.bucketing.enabled` | **`false`** | master switch for SPJ — **must opt in** in 3.5.0 |
| `spark.sql.sources.v2.bucketing.pushPartValues.enabled` | **`false`** | also enable to co-partition when one side misses partition values |
| `spark.sql.requireAllClusterKeysForCoPartition` | `true` (internal) | set `false` to allow join-key ⊂ partition-key |
| `spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled` | `false` | skew handling for SPJ (non-full-outer) |

**Detect in Spark UI:** if SPJ fires, the join has **no `Exchange`** node before it. (Newer `allowJoinKeysSubsetOfPartitionKeys`/`allowCompatibleTransforms`/`bucketing.shuffle` knobs are **Spark 4.0+ — not in 3.5.0**; don't reference them for our runtime.)

---

### Cross-references
- JDBC parallel **reads** (`partitionColumn`/`lowerBound`/`upperBound`/`numPartitions`) and partition sizing → `01-partitioning.md`
- AQE coalescing for **well-sized output files / CSV count** on the bulk write path → `07-aqe.md`, `03-file-layout-io.md`
- Broadcast vs shuffle joins (a pushed-down/filtered Oracle dimension may become broadcastable) → `02-joins.md`
- Exact config keys, defaults, and where-settable → `config-matrix.md`; symptom lookup → `quick-reference.md`

> **DB-as-shared-resource warning:** every `numPartitions`/`partition.num` you add is another concurrent connection and another parallel load on the Oracle DB. Spark parallelism that helps *you* can degrade the DB for *other* users — size it to the database's capacity (ECPUs, sessions, redo throughput), and push down filters so less data and fewer rows ever cross the wire.
