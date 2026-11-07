# Streaming from an Iceberg source table into ADW

Use a native Apache Iceberg table in AIDP as the **source** of a Spark Structured Streaming
pipeline: one stream writes events into the table, another reads it with
`spark.readStream.format("iceberg")` and upserts every micro-batch into an Oracle Autonomous
Data Warehouse (ADW) table with MERGE. ADW also reads the Iceberg table in place, through an
external table, so both the raw events and the merged state can be queried from ADW.

The sample then changes the source table while both streams run -- updates, deletes,
overwrites, MERGE, schema changes, compaction and snapshot expiry -- and records what each change
does to the streams, which read options help, and how to work around what they cannot do.

| Notebook | Role |
|---|---|
| [`01_producer_rate_to_iceberg.ipynb`](01_producer_rate_to_iceberg.ipynb) | Generates order events with Spark's `rate` source and appends them to an Iceberg table. |
| [`02_consumer_iceberg_to_adw.ipynb`](02_consumer_iceberg_to_adw.ipynb) | Reads the table as a stream and upserts each micro-batch into ADW; creates the ADW external table over the Iceberg table. |
| [`03_source_table_changes.ipynb`](03_source_table_changes.ipynb) | Changes the source table while 1 and 2 run, and shows the effect of each change. |

```
 notebook 1                          notebook 2                                   ADW
 rate source --> Iceberg table --> readStream.format("iceberg") --> foreachBatch --> ORDERS (MERGE)
                 order_events                                          |          ORDERS_STG, STREAM_BATCH_LOG
                 (oci:// bucket) <------ ADW external table ORDER_EVENTS_ICEBERG (reads it in place)
                       ^
                       | notebook 3: UPDATE, DELETE, INSERT OVERWRITE, MERGE, ALTER TABLE, compaction, expiry
```

Everything here was run on AIDP Workbench, Spark 3.5.0, with the Iceberg runtime AIDP ships
(`iceberg-spark-runtime-3.5_2.12-1.8.x-oci`), against an ADW on Oracle AI Database 26ai. Several
Iceberg streaming features documented for newer Iceberg releases behave differently on this
version; they are listed under [Iceberg 1.8 vs. the current docs](#iceberg-18-vs-the-current-docs).

---

## Results at a glance

What one change to the source table does to a stream reading it:

| Change to the source | Snapshot | Default read options | Option that keeps the stream running | What the stream then misses | Way around it |
|---|---|---|---|---|---|
| `INSERT`, `DataFrameWriterV2.append`, streaming writes | `append` | read | -- | nothing | -- |
| `MERGE` with only `WHEN NOT MATCHED THEN INSERT` | `append` | read | -- | nothing | the way to append idempotently |
| Row-level `UPDATE` (copy-on-write or merge-on-read) | `overwrite` | **query fails** | `streaming-skip-overwrite-snapshots` | the updated rows | change events, or catch-up from the changelog |
| Row-level `DELETE`, copy-on-write | `overwrite` | **query fails** | `streaming-skip-overwrite-snapshots` -- not the delete option | the deletes | delete events, or catch-up |
| `DELETE` of whole files (e.g. a partition), or merge-on-read `DELETE` | `delete` | **query fails** | `streaming-skip-delete-snapshots` | the deletes | delete events, or catch-up |
| `INSERT OVERWRITE`, `overwritePartitions()` | `overwrite` | **query fails** | `streaming-skip-overwrite-snapshots` | the removed rows **and the new rows** | catch-up |
| `MERGE` that updates | `overwrite` | **query fails** | `streaming-skip-overwrite-snapshots` | updated and inserted rows | catch-up |
| `ADD COLUMN`, `RENAME COLUMN`, `DROP COLUMN` | none | read; a running query keeps its old schema until restarted | -- | the new column's values until the restart | restart the query |
| Column type change | -- | AIDP rejects it | -- | -- | new column, or rebuild the table |
| `CREATE OR REPLACE TABLE ... AS SELECT`, `rollback_to_snapshot` | parentless / earlier | **query fails** (NPE on `parentId()`) | none | -- | new checkpoint + `stream-from-timestamp` |
| `rewrite_data_files`, `rewrite_manifests` | `replace` | skipped, no duplicates | -- | nothing | -- |
| `expire_snapshots` past a query's position | -- | **restart fails**; a *new* query may read nothing | none | -- | expire behind the slowest reader; bootstrap new readers |

The consumer in notebook 2 sets both skip options so that it survives notebook 3, and notebook 3
shows how to apply the skipped changes to ADW. After all of notebook 3's changes, ADW and the
Iceberg table matched: 4,970 orders on each side, none missing, none extra, none with a
different event id, region, status or amount (`AdwSink.compare_with_source`).

---

## One-time setup

### 1. Cluster

- An AIDP cluster (Spark 3.5). We used 2 OCPU / 32 GB nodes with 1 to 10 workers.
- **python-oracledb** as a cluster library on driver and executors, e.g. `oracledb` in a
  `requirements.txt` added to the cluster's libraries. It is not part of the base image; without
  it notebook 2 fails with `ModuleNotFoundError: No module named 'oracledb'`.

### 2. A Volume for checkpoints

Streaming checkpoints and the notebooks' stop files live on a Volume. Create a folder in a
bucket, for example `iceberg-checkpoints/` in a bucket of your choice, then mount it in the
`default` schema of the `default` catalog:

```sql
create external volume if not exists default.default.iceberg_checkpoints
location 'oci://<bucket>@<namespace>/iceberg-checkpoints'
```

On AIDP, the same statement written with an upper-case `LOCATION` and a trailing `/` failed with
"Location is missing for External Volume"; the form above worked.

The Iceberg table itself goes to `oci://<bucket>@<namespace>/iceberg-streaming/warehouse/...`
(`WAREHOUSE` in the notebooks). The AIDP workspace needs read and write access to that bucket.

### 3. The ADW schema and its Object Storage credential

Run [`sql/adw_setup.sql`](sql/adw_setup.sql): part 1 as `ADMIN` creates the user
`ICEBERG_STREAM` with the few grants it needs; part 2 as `ICEBERG_STREAM` creates the
`DBMS_CLOUD` credential `OCI_ICEBERG_CRED` that lets ADW read the bucket. The notebooks create the
tables themselves (`CREATE TABLE IF NOT EXISTS`, Oracle AI Database 23ai or later).

### 4. The ADW wallet on a Volume

ADW with mutual TLS needs its wallet. Unzip the wallet into a folder on a Volume that only the
pipeline's users can read, for example `/Volumes/default/default/adw_wallets/<wallet-folder>/`
(`WALLET_DIR` in the notebooks). The driver reads it from there and passes the `ewallet.pem` and
connect descriptor to the executors, which cannot read `/Volumes` paths.

### 5. Store the ADW secrets

In the AIDP Credential Store, create a credential of type **Secret Token** named
`adw_iceberg_stream` (`CREDENTIAL_NAME`) with four keys:

| Key | Value |
|---|---|
| `user` | `ICEBERG_STREAM` |
| `password` | its password |
| `wallet_password` | the wallet password, needed for `ewallet.pem` |
| `tns` | a TNS alias from the wallet's `tnsnames.ora`, e.g. `<db>_low` |

The notebooks read them with `aidputils.secrets.get(name=..., key=...)` and never print them.

### 6. Upload the files

Copy this folder to `/Workspace/streaming-from-iceberg-source` (`SAMPLE_DIR`): the three
notebooks plus `adw_sink.py` and `stream_control.py`. In each notebook, set `WAREHOUSE` and
`WALLET_DIR`; keep the shared configuration identical in all three.

---

## Run it

1. Open notebook 1 and run its cells up to and including **Start the stream**. That cell keeps
   running while the producer runs.
2. Open notebook 2 in another tab and run its cells up to and including **Start the stream**.
3. Work through notebook 3, section by section. Its last section stops both streams, catches up
   the skipped changes and compares ADW with the source.
4. Once the streams have stopped, the stream cells return; then run the remaining cells of
   notebooks 2 and 1 -- the checks, and the optional clean-up. While a stream runs, the cells
   after it cannot run (see the caveats).

Each stream stops by itself after `RUN_MINUTES`, or when notebook 3 calls
`stream_control.request_stop(CONTROL_DIR, "producer" | "consumer")`.

---

## Configuration reference

All observations come from this sample's runs on AIDP; numbers are from notebook 3 unless noted.

### Read options (`spark.readStream.format("iceberg").option(...)`)

| Option | Default | What it does | Observed |
|---|---|---|---|
| `streaming-skip-overwrite-snapshots` | `false` | Skip `overwrite` snapshots instead of failing. | Needed for row-level UPDATE/DELETE (copy-on-write), MERGE that updates, INSERT OVERWRITE. The skipped snapshot's **added rows are skipped too**: 0 of 3 `REPLACED` rows reached the probe after an `INSERT OVERWRITE`. |
| `streaming-skip-delete-snapshots` | `false` | Skip `delete` snapshots instead of failing. | Needed for a partition `DELETE` and for merge-on-read deletes. Does **not** cover a row-level copy-on-write delete, which is an `overwrite`. |
| `stream-from-timestamp` | oldest snapshot | Where a **new** query starts, epoch milliseconds: the first snapshot committed at or after it. | Correct for new queries. **Changing it on a restart from an existing checkpoint skipped the 50 rows committed while the query was down**; see [the caveats](#iceberg-18-vs-the-current-docs). |
| `streaming-max-files-per-micro-batch` | unlimited | Cap the files per micro-batch. | 1 gave 16 batches of 300 rows; 5 on 12 files gave 125, 125, 50. |
| `streaming-max-rows-per-micro-batch` | unlimited | Cap the rows per micro-batch, whole files only. | 700 with 300-row files: batches of 600. **100 -- smaller than one file: 0 rows, no error, the query never advances.** |
| `async-micro-batch-planning-enabled` | -- | In the current docs. | Not in this Iceberg build. |

```python
events = (spark.readStream.format("iceberg")
          .option("streaming-skip-overwrite-snapshots", "true")
          .option("streaming-skip-delete-snapshots", "true")
          .option("streaming-max-files-per-micro-batch", "200")
          .load("default.iceberg_streaming_demo.order_events"))   # also: .table(...), or .load("oci://...") by path
```

All three ways of naming the source worked: `format("iceberg").load("<catalog>.<schema>.<table>")`,
`readStream.table(...)` and `format("iceberg").load("oci://...table path")`.

**Triggers.** `processingTime` works as expected. `Trigger.AvailableNow` without rate limits
read the whole backlog in one batch. **With** a rate limit it processed one batch and ended; a
second `AvailableNow` run from the same checkpoint made no progress, while a `processingTime`
trigger on that checkpoint picked up the remaining 275 of 300 rows -- so nothing was lost, but
do not combine `AvailableNow` with rate limits on this version. `Trigger.Once` with a rate limit
processed one limited batch per run.

### Write options (`writeStream.format("iceberg").option(...)`)

| Option / setting | What it does | Observed |
|---|---|---|
| `outputMode("append")` / `("complete")` | Append each micro-batch, or replace the table each batch. | append used throughout |
| `trigger(processingTime=...)` | One Iceberg commit per trigger. | With **10 seconds**, a single append from another notebook could not commit for **16 minutes**: every attempt lost the race for the next metadata version. With **1 minute** -- the minimum the Iceberg docs recommend -- other writers committed normally. |
| `checkpointLocation` | Spark's progress for this query. | A Volume path worked; restarts resumed exactly (50 rows committed while down, then 20 more). |
| `fanout-enabled` | Write rows of all partitions without sorting each micro-batch by partition. | Recommended by the Iceberg docs for partitioned targets; writes into a 4-partition table also worked without it. |
| `snapshot-property.<key>` | Adds `<key>` to every commit's snapshot summary. | Used to tell the producer's commits from other writers' (`summary['app']`). |
| `toTable(...)` vs `.option("path", ...).start()` | Table name, or a path for directory-based tables. | `toTable` with the 3-part name. |

### Table properties of the source table

| Property | Value | Why |
|---|---|---|
| `write.update.mode`, `write.delete.mode`, `write.merge.mode` | `copy-on-write` | ADW external tables fail on delete files (ORA-29913, "Delete files not supported"), and `create_changelog_view` does not support them ("Delete files are currently not supported in changelog scans"). |
| `write.metadata.delete-after-commit.enabled` + `write.metadata.previous-versions-max` | `true`, `100` | Each streaming commit writes a `vN.metadata.json`; with cleanup and a limit of 3, 8 commits left 4 files instead of 9. |
| `commit.retry.num-retries` | `10` | More retries for writers that lose a commit race (default 4). |

---

## The ADW side

### Upserting micro-batches: staging table + one MERGE

`adw_sink.AdwSink.upsert_batch` is the `foreachBatch` function:

1. Skip the batch if `STREAM_BATCH_LOG` records it for this query (Spark replays the last batch
   after a restart if it was not marked complete). The log key is the query name, the query id
   Spark keeps in the checkpoint's `metadata` file, and the batch id -- so a query started on a
   new checkpoint, whose batch ids start again at 0, is never mistaken for an earlier one.
2. Keep the newest event per order (no duplicate keys for the MERGE, ORA-30926).
3. Executors insert the rows into a **permanent** staging table with python-oracledb array
   inserts, in parallel.
4. The driver runs **one** set-based MERGE into `ORDERS`, logs the batch and clears its staging
   rows, in a single transaction.

The MERGE applies an event only if its `event_id` is newer than the one the row reflects, deletes
the order for `op = 'D'`, and inserts new orders:

```sql
MERGE INTO orders t
USING (SELECT ... FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.order_id ORDER BY s.event_id DESC) rn
                        FROM orders_stg s WHERE s.batch_id = :batch_id) WHERE rn = 1) s
ON (t.order_id = s.order_id)
WHEN MATCHED THEN UPDATE SET ... WHERE s.event_id > t.last_event_id
                         DELETE WHERE s.op = 'D'
WHEN NOT MATCHED THEN INSERT (...) VALUES (...) WHERE s.op <> 'D'
```

Why not the built-in `aidataplatform` connector? It has a MERGE write mode, but every call
creates and drops a staging table, and by default stages the rows through Object Storage. Loading
the same 2,000 rows into the permanent staging table took **13.2 s** through the connector
(APPEND, `skip.oos.staging=true`, `wallet.path` pointing at the wallet **zip**) and **2.2 s** with
the executor inserts used here. Per micro-batch, this sample's path issues no DDL: two short
driver connections and one connection per partition.

Notes:

- Executors cannot read `/Volumes` paths, so Spark JDBC with `TNS_ADMIN=/Volumes/...` fails on
  them ("The Network Adapter could not establish the connection"). The driver therefore
  broadcasts the PEM wallet, the connect descriptor and the ADW password. Each insert task
  writes the wallet to a new private temporary folder and deletes it as soon as the connection
  is open. Anyone who can run code on the cluster's executors could still read the broadcast:
  run the pipeline on a cluster whose users may see these credentials.
- A task Spark re-runs after it committed stages its rows twice; the MERGE keeps one row per
  order, so the result is unchanged.

### Reading the Iceberg table in place

`AdwSink.create_iceberg_external_table` calls `DBMS_CLOUD.CREATE_EXTERNAL_TABLE` with ADW's
Hadoop catalog support (`iceberg_catalog_type: hadoop`, the warehouse folder, and
`<schema>.<table>`), which is why the table sits at `<warehouse>/<schema>/<table>`. Observed:

- Every query reads the **current snapshot**, through `version-hint.text`: new commits appear
  without a refresh -- also with old metadata files being cleaned up.
- Compaction, snapshot expiry and copy-on-write updates and deletes are fine.
- Merge-on-read delete files make every query fail with ORA-29913 ("Delete files not
  supported"), and also make re-creating the external table fail. `rewrite_data_files` with
  `rewrite-all` alone left the delete file referenced; adding `remove-dangling-deletes` removed
  it and ADW could read the table again.
- The column list is fixed at creation. After `ADD COLUMN` the external table kept working without
  the new column; re-creating it (`replace=True`) added it.
- AIDP's managed Iceberg tables live in an AIDP-created bucket under `managed/<schema>.db/<uuid>`;
  this sample gives the table an explicit `LOCATION` in your own bucket so that ADW can find it
  through the Hadoop layout and read it with your credential.

### Catching up skipped changes

`resync_snapshot` in notebook 3 handles any skipped `overwrite` or `delete` snapshot:
`create_changelog_view` between the snapshot's parent and the snapshot lists the rows it
removed and added; their orders are re-read from the current table and written to ADW with a
MERGE; orders with no events left are deleted. The MERGE overwrites an ADW row unless its
`last_event_id` is above that order's **watermark** -- the highest event id of the order that
the changelog and the current table contain. A row above it was updated by an event that
arrived later, and stays. Two rules:

- **Resync only after the consumer has passed the snapshot.** Otherwise the consumer can still
  deliver older appends of those orders afterwards -- in one test it re-inserted 3 orders that a
  partition `DELETE` had removed. `resync_snapshot` waits for the consumer's checkpoint first.
- **Resync before expiring the snapshot.** The changelog needs the snapshot and its parent.

---

## AIDP caveats and workarounds

| Behaviour | Workaround |
|---|---|
| While a stream started from a notebook runs, other cells of **that** notebook wait ~95 s and fail with "Command execution failed on compute cluster." Other notebooks cannot see the query (separate Spark sessions, one Spark application). | One notebook per long-running stream; run the stream inside one blocking cell (`stream_control.run_until_stopped`). |
| Closing or deleting the notebook session does **not** stop its stream; one kept committing for an hour. The kernel itself also outlives the session: edited helper modules stay loaded until the kernel restarts (or `importlib.reload`). | Always stop with `query.stop()` -- `run_until_stopped` does, on a stop file or a time limit. Last resort: `stream_control.list_cluster_streams(spark)` and `stop_cluster_stream(spark, run_id)`, which interrupts the query's driver thread. `sparkContext.cancelJobGroup(run_id)` only stopped queries that had a batch running at that moment (2 of 6). |
| Stopping a query in the middle of a micro-batch cancels that batch's job, and AIDP marks the cell failed ("Streaming query failed: JobFailed ... cancelled job group"); nothing is lost, the batch runs again on restart. | `run_until_stopped` waits up to two minutes for the running batch to finish before `stop()`; in our runs no batch was cancelled. A trigger starting in between can still be. |
| Concurrent writers to one native Iceberg table race for the next `vN.metadata.json`; losers fail with `CommitFailedException: Version N already exists` once retries run out. A 10-second streaming writer starved another writer for 16 minutes. | Trigger at 1 minute or more; raise `commit.retry.num-retries`; retry writes on that error (notebook 3's `committed`). See [Concurrent Writes to Native Iceberg Tables](../transformation/iceberg/Iceberg_Concurrent_Writes.ipynb). |
| `RENAME COLUMN` or `DROP COLUMN` on the table a streaming writer writes to stops that writer: "Field status not found in source schema", also after a restart, until its code changes. `ADD COLUMN` is fine: the writer leaves the new column `NULL`. | Change the writer's code together with the schema; try such changes on a copy first (notebook 3, section 6b). |
| Every column type change is rejected, even Iceberg-legal widenings: "405 ... Changing Column Data Type operation is not supported for this data format". | Add a new column with the new type and migrate; or `CREATE OR REPLACE TABLE ... AS SELECT` and restart readers with a new checkpoint and `stream-from-timestamp`. |
| `DROP TABLE` -- even `PURGE` -- leaves the files of a table with its own `LOCATION`; `CREATE TABLE` at that location then attaches to the old metadata, or fails with "Existing Iceberg schema at location ... does not match incoming schema" (it listed the partition column last, so even the identical DDL mismatched). | Delete the folder too; notebook 1's clean-up does it with the Hadoop `FileSystem` API. |
| A **new** query on a table whose old snapshots were compacted and expired reads nothing: it starts at the oldest snapshot, a `replace`, and reads only files that snapshot added. 0 of 60 rows. | Bootstrap: load the table once in batch at a snapshot, then stream with `stream-from-timestamp` just after it (notebook 3, section 7b). |
| A query restarted after the snapshot in its checkpoint was expired fails: "Cannot load current offset at snapshot ..., the snapshot was expired or removed". | Expire only behind the oldest reader's position: `stream_control.committed_source_offset(checkpoint)` reads it. |

---

## Iceberg 1.8 vs. the current docs

The [Iceberg Spark Structured Streaming docs](https://iceberg.apache.org/docs/latest/spark-structured-streaming/)
describe the newest release. On AIDP's 1.8.x build:

| Docs (latest) | On AIDP, Iceberg 1.8.x | Upstream change |
|---|---|---|
| `streaming-max-rows-per-micro-batch` is a soft limit: a batch always includes the next file. | Hard limit: a file larger than the limit stops the query silently. | apache/iceberg#12988, in 1.10 |
| Rate limits split `Trigger.AvailableNow` into several batches. | One batch, then the query ends; re-running makes no progress. | apache/iceberg#13824, in 1.11 |
| Rate limits are ignored by `Trigger.Once`. | Honoured: one limited batch per run. | -- |
| `async-micro-batch-planning-enabled`. | Not available. | 1.11 (Spark 3.5 backport, apache/iceberg#15992) |
| -- | A restart with a later `stream-from-timestamp` skips snapshots committed while the query was down. | apache/iceberg#18348, on `main` after 1.12.0 |

---

## Files

| File | Contents |
|---|---|
| `01_producer_rate_to_iceberg.ipynb`, `02_consumer_iceberg_to_adw.ipynb`, `03_source_table_changes.ipynb` | The three notebooks, outputs cleared. |
| `adw_sink.py` | `AdwConnection` (Credential Store + wallet), `AdwSink`: table DDL, `upsert_batch`, `resync_orders`, `create_iceberg_external_table`, `compare_with_source`. |
| `stream_control.py` | `run_until_stopped`, `request_stop`, `committed_source_offset`, `list_cluster_streams`, `stop_cluster_stream`. |
| `sql/adw_setup.sql` | ADW user, grants, Object Storage credential, and queries over both ADW tables. |
| `tests/test_helpers.py` | Offline tests for the two modules. |

## Tests

```bash
cd data-engineering/streaming-from-iceberg-source
python3 -m pytest -q tests
```

The tests need no Spark and no database: they cover identifier validation, TNS descriptor and
Hadoop-catalog path parsing, the generated MERGE and INSERT statements, the executor insert loop
(with a fake driver), the stop logic of `run_until_stopped`, and checkpoint parsing.

## Clean up

- Notebook 2, last cell: drops `ORDERS`, `ORDERS_STG`, `STREAM_BATCH_LOG` and the external table.
- Notebook 1, last cell: drops the Iceberg table, deletes its folder and the checkpoints.
- Notebook 3, last cell: drops its scratch tables.
- Then, if you no longer need them: the Credential Store entry, the wallet folder, the
  `ICEBERG_STREAM` user and its `OCI_ICEBERG_CRED` credential in ADW.

## References

- [Iceberg: Spark Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/) and [Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Autonomous Database: Query Apache Iceberg Tables](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/query-external-data-apache-iceberg.html)
- Related samples: [Native Apache Iceberg Table](../../getting-started/Native_Iceberg_Table.ipynb), [Concurrent Writes to Native Iceberg Tables](../transformation/iceberg/Iceberg_Concurrent_Writes.ipynb), [Iceberg Time Travel, Rollback, and Retention](../transformation/iceberg/iceberg_time_travel_rollback_retention.ipynb), [ADW External Table on Delta UniForm](../adw-ext-table-on-uniform/README.md), [Streaming Metrics to OCI Monitoring](../streaming-metrics-to-oci-monitoring/README.md)
