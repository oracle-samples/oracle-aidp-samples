# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Upsert Spark Structured Streaming micro-batches into an Oracle Autonomous Database (ADW) table.

The source rows are order events (one row per change to an order). The ADW table ``ORDERS``
keeps the latest state of each order. ``AdwSink.upsert_batch`` is the ``foreachBatch``
function; for every micro-batch it:

1. Skips the batch if ``STREAM_BATCH_LOG`` already records it for this query. Spark replays the
   last micro-batch after a restart if it was not marked complete in the checkpoint. A query is
   identified by its name and by the id Spark keeps in its checkpoint; its staged rows carry that
   id too, so two queries never touch each other's rows.
2. Keeps only the newest event per order, so the MERGE never sees two rows for one key
   (ORA-30926) and fewer rows travel to ADW.
3. Has the executors insert those rows into a permanent staging table, in parallel, with
   python-oracledb array inserts. Executors cannot read ``/Volumes`` paths on AIDP, so the
   driver reads the wallet and broadcasts it to them.
4. Runs one set-based MERGE from staging into ``ORDERS``, records the batch in
   ``STREAM_BATCH_LOG`` and removes the staged rows, all in one transaction.

A delete event (``op = 'D'``) does not remove the order's row: it marks it ``is_deleted = 'Y'``
and keeps its ``last_event_id``. Such a tombstone lets every MERGE reject an older event for the
order, so neither a late event nor a resync can bring a deleted order back. Query live orders
with ``WHERE is_deleted = 'N'``.

Why not the built-in ``aidataplatform`` connector with ``write.mode=MERGE``? It works, but it
creates and drops a staging table on every call (and by default stages the rows through Object
Storage). In our test, loading the same 2,000 rows into a permanent staging table took 13.2 s
with the connector (APPEND mode, ``skip.oos.staging=true``) and 2.2 s with the executor
inserts used here.

``resync_orders`` repairs orders whose source rows changed in snapshots the stream skipped
(UPDATE, DELETE, MERGE, INSERT OVERWRITE). ``create_iceberg_external_table`` lets ADW query
the Iceberg source table in place.

Trust model: the ADW password and the PEM wallet travel to the executors in a broadcast
variable, and each insert task writes the wallet to a private temporary folder that it deletes
right after connecting. Run this on a cluster whose users may see those credentials.

Requirements: python-oracledb on the cluster (driver and executors) and Oracle AI Database
23ai or later for ``CREATE TABLE IF NOT EXISTS``.
"""

from __future__ import annotations

import base64
import json
import os
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional, Tuple

_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")

# Columns of a source event, as the producer notebook writes them.
EVENT_COLUMNS = ("event_id", "order_id", "region", "status", "amount", "op", "event_ts")
# Column order of the staging table, and of the rows the executors insert. watermark_id is only
# set by a resync (see resync_orders); the stream leaves it NULL.
STAGING_COLUMNS = ("query_id", "batch_id") + EVENT_COLUMNS + ("watermark_id",)
# query_id of the rows that resyncs write to the batch log.
RESYNC_QUERY_ID = "resync"


def identifier(name: str) -> str:
    """Validate an unquoted Oracle identifier and return it in upper case."""
    if not _IDENTIFIER.match(name):
        raise ValueError(f"not a simple Oracle identifier: {name!r}")
    return name.upper()


def tns_descriptor(tnsnames_text: str, alias: str) -> str:
    """Return the connect descriptor for ``alias`` from the text of a tnsnames.ora file.

    The executors have no tnsnames.ora, so they connect with the full descriptor instead.
    """
    m = re.search(rf"^\s*{re.escape(alias)}\s*=\s*\(", tnsnames_text, re.IGNORECASE | re.MULTILINE)
    if not m:
        raise ValueError(f"alias {alias!r} not found in tnsnames.ora")
    start = m.end() - 1
    depth = 0
    for i in range(start, len(tnsnames_text)):
        ch = tnsnames_text[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return re.sub(r"\s+", "", tnsnames_text[start:i + 1])
    raise ValueError(f"unbalanced parentheses in the descriptor for {alias!r}")


def hadoop_catalog_coordinates(table_location: str, region: str) -> Tuple[str, str]:
    """Split an Iceberg table location into ADW's Hadoop catalog settings.

    ``oci://bucket@namespace/path/to/warehouse/<database>/<table>`` becomes
    ``("https://objectstorage.<region>.oraclecloud.com/n/<namespace>/b/<bucket>/o/path/to/warehouse",
    "<database>.<table>")``. The table must sit two folder levels below the warehouse, which is
    how the producer notebook lays it out.
    """
    m = re.fullmatch(r"oci://([^@/]+)@([^/]+)/(.+)", table_location.rstrip("/"))
    if not m:
        raise ValueError(f"expected oci://bucket@namespace/path, got {table_location!r}")
    bucket, namespace, path = m.groups()
    parts = path.split("/")
    if len(parts) < 3:
        raise ValueError("the table location needs at least <warehouse>/<database>/<table>")
    for part in parts:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", part):
            raise ValueError(f"unsupported character in path segment {part!r}")
    warehouse = (f"https://objectstorage.{region}.oraclecloud.com/n/{namespace}/b/{bucket}/o/"
                 + "/".join(parts[:-2]))
    return warehouse, f"{parts[-2]}.{parts[-1]}"


def checkpoint_query_id(checkpoint_location: str) -> str:
    """The id Spark stores in a query's checkpoint: the same across restarts, new for a new checkpoint."""
    with open(os.path.join(checkpoint_location, "metadata")) as f:
        return json.loads(f.read())["id"]


@dataclass(frozen=True)
class AdwConnection:
    """How to reach the ADW: the credentials, plus the unzipped wallet on a Volume."""

    user: str
    password: str
    tns_alias: str
    wallet_dir: str
    wallet_password: str

    def __repr__(self) -> str:      # keep secrets out of notebook output and logs
        return f"AdwConnection(user={self.user!r}, tns_alias={self.tns_alias!r}, wallet_dir={self.wallet_dir!r})"

    @classmethod
    def from_credential_store(cls, secrets, credential_name: str, wallet_dir: str) -> "AdwConnection":
        """Read the keys user, password, wallet_password and tns from a Credential Store entry.

        ``secrets`` is ``aidputils.secrets``. The values are never printed.
        """
        values = {}
        for key in ("user", "password", "wallet_password", "tns"):
            value = secrets.get(name=credential_name, key=key)
            if not value:
                raise RuntimeError(f"credential {credential_name!r} has no value for key {key!r}")
            # Names may carry stray whitespace from copy and paste; passwords are used exactly as stored.
            values[key] = value.strip() if key in ("user", "tns") else value
        return cls(values["user"], values["password"], values["tns"], wallet_dir, values["wallet_password"])

    def connect(self):
        """Open a connection from the driver (python-oracledb thin mode, mTLS wallet)."""
        import oracledb
        return oracledb.connect(user=self.user, password=self.password, dsn=self.tns_alias,
                                config_dir=self.wallet_dir, wallet_location=self.wallet_dir,
                                wallet_password=self.wallet_password)

    def executor_settings(self) -> dict:
        """What an executor needs to connect: the descriptor and the PEM wallet, read on the driver."""
        with open(os.path.join(self.wallet_dir, "tnsnames.ora")) as f:
            descriptor = tns_descriptor(f.read(), self.tns_alias)
        with open(os.path.join(self.wallet_dir, "ewallet.pem"), "rb") as f:
            wallet_pem = base64.b64encode(f.read()).decode()
        return {"user": self.user, "password": self.password, "wallet_password": self.wallet_password,
                "descriptor": descriptor, "wallet_pem": wallet_pem}


def connect_from_executor(settings: dict):
    """Connect from an executor. The wallet exists on disk only while the connection is opened:
    python-oracledb reads it during connect, so the private folder is deleted right after."""
    import oracledb
    folder = tempfile.mkdtemp(prefix="adw_wallet_")          # mode 0700, unique per call
    try:
        fd = os.open(os.path.join(folder, "ewallet.pem"), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(base64.b64decode(settings["wallet_pem"]))
        return oracledb.connect(user=settings["user"], password=settings["password"], dsn=settings["descriptor"],
                                wallet_location=folder, wallet_password=settings["wallet_password"])
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def insert_partition(rows: Iterable, settings: dict, insert_sql: str, array_size: int) -> Iterator[int]:
    """Executor side: insert one Spark partition into the staging table and commit.

    A task that fails before the commit leaves nothing behind. If Spark re-runs a task that had
    already committed, its rows are staged twice; the MERGE keeps one row per order, so the
    result is the same.
    """
    conn = connect_from_executor(settings)
    count = 0
    try:
        cursor = conn.cursor()
        buffer: List[tuple] = []
        for row in rows:
            buffer.append(tuple(row))
            if len(buffer) >= array_size:
                cursor.executemany(insert_sql, buffer)
                count += len(buffer)
                buffer = []
        if buffer:
            cursor.executemany(insert_sql, buffer)
            count += len(buffer)
        conn.commit()
    finally:
        conn.close()
    yield count


class AdwSink:
    """Keeps an ADW table of latest order states in step with a stream of order events.

    ``checkpoint_location`` is the checkpoint of the streaming query that calls ``upsert_batch``;
    it is needed to tell that query's batches apart from those of an earlier query with the same
    name. Leave it out for a sink that only runs resyncs and queries.

    A query on a **new** checkpoint replays the source table from its oldest snapshot, while the
    ADW table still holds the state the old query built: rows that skipped snapshots later
    deleted would come back. So the first batch of a new checkpoint fails if STREAM_BATCH_LOG
    has batches of another checkpoint under the same query name. To start over, either drop the
    ADW tables (notebook 2's clean-up), or bootstrap: ``reload()`` ADW from one snapshot, then
    start the new query with ``stream-from-timestamp`` just after that snapshot and
    ``allow_new_checkpoint=True`` (README, "Starting over on a new checkpoint").
    """

    def __init__(self, spark, adw: AdwConnection, query_name: str, checkpoint_location: Optional[str] = None,
                 target: str = "ORDERS", staging: str = "ORDERS_STG", batch_log: str = "STREAM_BATCH_LOG",
                 array_size: int = 1000, allow_new_checkpoint: bool = False):
        self.spark = spark
        self.adw = adw
        self.query_name = query_name
        self.checkpoint_location = checkpoint_location
        self.target = identifier(target)
        self.staging = identifier(staging)
        self.batch_log = identifier(batch_log)
        self.array_size = array_size
        self.allow_new_checkpoint = allow_new_checkpoint
        self._executor_settings = None   # broadcast once, on first use
        self._query_id = None            # read from the checkpoint on the first batch
        # The executors import this module to run insert_partition, so ship it to them.
        spark.sparkContext.addPyFile(os.path.abspath(__file__))
        placeholders = ", ".join(f":{i + 1}" for i in range(len(STAGING_COLUMNS)))
        self.insert_sql = f"INSERT INTO {self.staging} ({', '.join(STAGING_COLUMNS)}) VALUES ({placeholders})"
        # The stream applies an event only if it is newer than the one ADW already reflects. Strictly
        # newer: an event whose row was corrected in place (same event_id) and then resynced must not
        # be overwritten by the original version of that event arriving late.
        self.stream_merge_sql = self._merge_sql("s.event_id > t.last_event_id")
        # A resync replaces what ADW has for an order, unless ADW already reflects an event newer
        # than every event of that order the resync looked at (its per-order watermark).
        self.resync_merge_sql = self._merge_sql("t.last_event_id <= s.watermark_id")

    # ---- set-up -------------------------------------------------------------------------------

    def ensure_tables(self) -> None:
        """Create the target, staging and batch log tables if they do not exist yet."""
        ddl = [
            f"""CREATE TABLE IF NOT EXISTS {self.target} (
                    order_id       NUMBER(19)   PRIMARY KEY,
                    region         VARCHAR2(16),
                    status         VARCHAR2(16),
                    amount         NUMBER(12,2),
                    last_event_id  NUMBER(19)   NOT NULL,
                    last_event_ts  TIMESTAMP,
                    is_deleted     VARCHAR2(1)  DEFAULT 'N' NOT NULL,
                    updated_at     TIMESTAMP    DEFAULT SYSTIMESTAMP)""",
            f"""CREATE TABLE IF NOT EXISTS {self.staging} (
                    query_id      VARCHAR2(36) NOT NULL,
                    batch_id      NUMBER(19) NOT NULL,
                    event_id      NUMBER(19) NOT NULL,
                    order_id      NUMBER(19) NOT NULL,
                    region        VARCHAR2(16),
                    status        VARCHAR2(16),
                    amount        NUMBER(12,2),
                    op            VARCHAR2(1),
                    event_ts      TIMESTAMP,
                    watermark_id  NUMBER(19))""",
            f"CREATE INDEX IF NOT EXISTS {self.staging}_BATCH_IX ON {self.staging} (query_id, batch_id)",
            f"""CREATE TABLE IF NOT EXISTS {self.batch_log} (
                    query_name    VARCHAR2(128) NOT NULL,
                    query_id      VARCHAR2(36)  NOT NULL,
                    batch_id      NUMBER(19)    NOT NULL,
                    rows_in       NUMBER        NOT NULL,
                    rows_merged   NUMBER        NOT NULL,
                    min_event_id  NUMBER(19),
                    max_event_id  NUMBER(19),
                    applied_at    TIMESTAMP     DEFAULT SYSTIMESTAMP,
                    CONSTRAINT {self.batch_log}_PK PRIMARY KEY (query_name, query_id, batch_id))""",
        ]
        with self.adw.connect() as conn:
            cursor = conn.cursor()
            for statement in ddl:
                cursor.execute(statement)
            # Tables made by an older version of this sample lack columns this version needs.
            for table, column in ((self.target, "IS_DELETED"), (self.staging, "QUERY_ID"),
                                  (self.staging, "WATERMARK_ID"), (self.batch_log, "QUERY_ID")):
                cursor.execute("SELECT COUNT(*) FROM user_tab_columns WHERE table_name = :t AND column_name = :c",
                               t=table, c=column)
                if not cursor.fetchone()[0]:
                    raise RuntimeError(f"{table} has no column {column}: it was created by an older version of "
                                       f"this sample. Drop {self.target}, {self.staging} and {self.batch_log} "
                                       "(notebook 2's clean-up cell) and run again.")

    def create_iceberg_external_table(self, table_name: str, credential_name: str, table_location: str,
                                      region: str, replace: bool = False) -> bool:
        """Create an ADW external table over the Iceberg table, through ADW's Hadoop catalog support.

        ADW resolves the current snapshot from the table's version-hint.text on every query, so
        new snapshots show up without a refresh. The column list is fixed when the external table
        is created: after a schema change, call again with ``replace=True``. Returns False if the
        table already existed and ``replace`` is False.
        """
        name = identifier(table_name)
        warehouse, table_path = hadoop_catalog_coordinates(table_location, region)
        fmt = json.dumps({"access_protocol": {"protocol_type": "iceberg", "protocol_config": {
            "iceberg_catalog_type": "hadoop", "iceberg_warehouse": warehouse, "iceberg_table_path": table_path}}})
        with self.adw.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_tables WHERE table_name = :t", t=name)
            if cursor.fetchone()[0]:
                if not replace:
                    return False
                cursor.execute(f"DROP TABLE {name}")
            cursor.execute("""BEGIN
                                DBMS_CLOUD.CREATE_EXTERNAL_TABLE(table_name => :t, credential_name => :c, format => :f);
                              END;""", t=name, c=identifier(credential_name), f=fmt)
        return True

    # ---- streaming ----------------------------------------------------------------------------

    def upsert_batch(self, batch_df, batch_id: int) -> None:
        """``foreachBatch`` function: apply one micro-batch of order events to the ADW table."""
        from pyspark.sql import functions as F

        query_id = self._stream_query_id()
        batch_df = batch_df.select(*EVENT_COLUMNS).persist()
        try:
            stats = batch_df.agg(F.count(F.lit(1)).alias("n"), F.min("event_id").alias("lo"),
                                 F.max("event_id").alias("hi")).first()
            if stats.n == 0:
                return
            started = time.monotonic()
            with self.adw.connect() as conn:
                cursor = conn.cursor()
                if self._already_applied(cursor, query_id, batch_id):
                    print(f"batch {batch_id}: already applied in ADW, skipped")
                    return
                # Remove rows a failed attempt of this batch may have staged.
                self._clear_staging(cursor, query_id, batch_id)
                conn.commit()
            newest = self._newest_per_order(batch_df).withColumn("watermark_id", F.lit(None).cast("bigint"))
            staged = self._stage(newest, query_id, batch_id)
            with self.adw.connect() as conn:
                cursor = conn.cursor()
                cursor.execute(self.stream_merge_sql, query_id=query_id, batch_id=batch_id)
                merged = cursor.rowcount
                self._log_batch(cursor, self.query_name, query_id, batch_id, stats.n, merged, stats.lo, stats.hi)
                self._clear_staging(cursor, query_id, batch_id)
                conn.commit()
            print(f"batch {batch_id}: {stats.n} events, {staged} orders staged, {merged} rows merged, "
                  f"event_id {stats.lo}..{stats.hi}, {time.monotonic() - started:.1f}s")
        finally:
            batch_df.unpersist()

    def reload(self, snapshot_df, label: str) -> int:
        """Replace the whole ADW table with the newest surviving event per order of one snapshot.

        ``snapshot_df`` is the source table read at one snapshot, e.g. ``VERSION AS OF <id>``.
        Use it to bootstrap ADW before starting a query on a new checkpoint from just after that
        snapshot. The stream must not be running. Returns the number of orders loaded.
        """
        from pyspark.sql import functions as F

        newest = self._newest_per_order(snapshot_df.select(*EVENT_COLUMNS)).withColumn(
            "watermark_id", F.lit(None).cast("bigint"))
        reload_id = -int(time.time() * 1000)
        staged = self._stage(newest, RESYNC_QUERY_ID, reload_id)
        with self.adw.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {self.target}")
            # Into an empty table, the stream's MERGE inserts every order -- deleted ones as tombstones.
            cursor.execute(self.stream_merge_sql, query_id=RESYNC_QUERY_ID, batch_id=reload_id)
            loaded = cursor.rowcount
            self._log_batch(cursor, f"reload:{label}"[:128], RESYNC_QUERY_ID, reload_id, staged, loaded, None, None)
            self._clear_staging(cursor, RESYNC_QUERY_ID, reload_id)
            conn.commit()
        print(f"reload {label!r}: {loaded} orders loaded")
        return loaded

    def resync_applied(self, label: str) -> bool:
        """True if a resync with this label has already been applied."""
        rows = self.fetch(f"SELECT COUNT(*) FROM {self.batch_log} WHERE query_name = :q", q=f"resync:{label}"[:128])
        return rows[0][0] > 0

    def resync_orders(self, truth_df, watermarks_df, label: str) -> int:
        """Make ADW match the source table for some orders, for changes the stream skipped.

        ``watermarks_df`` has one row per affected order: ``order_id`` and ``watermark_id``, the
        highest event_id of that order the caller looked at -- the events the skipped snapshot
        removed or added, and the order's newest surviving event. ``truth_df`` holds the newest
        surviving event of each affected order that still has one. Affected orders without a row
        in ``truth_df`` are marked deleted in ADW.

        An ADW row whose ``last_event_id`` is above its order's watermark reflects an event newer
        than anything the resync saw (the stream applied it meanwhile) and is left alone. ``label``
        names the resync in STREAM_BATCH_LOG; re-running a label is a no-op. Returns the number
        of ADW rows merged.

        Call it only once the stream has processed every append committed up to the skipped
        snapshot. Before that, the stream may still deliver older appends of the affected orders,
        and they would land after the resync -- for example re-inserting orders that the snapshot
        deleted.
        """
        from pyspark.sql import functions as F

        log_name = f"resync:{label}"[:128]
        if self.resync_applied(label):
            print(f"resync {label!r} was already applied, skipped")
            return 0
        resync_id = -int(time.time() * 1000)   # negative ids never collide with stream batch ids
        truth = truth_df.select(*EVENT_COLUMNS)
        # One row per affected order: its newest surviving event, or a tombstone (op 'D') if none is left.
        joined = (watermarks_df.select("order_id", F.col("watermark_id").cast("bigint"))
                  .join(truth, "order_id", "left")
                  .withColumn("gone", F.col("event_id").isNull()))
        rows = joined.select(F.coalesce("event_id", "watermark_id").alias("event_id"), "order_id", "region", "status",
                             "amount", F.when(F.col("gone"), F.lit("D")).otherwise(F.col("op")).alias("op"),
                             "event_ts", "watermark_id")
        counts = joined.agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("gone").cast("int")).alias("gone"),
                            F.max("watermark_id").alias("hi")).first()
        staged = self._stage(rows, RESYNC_QUERY_ID, resync_id)
        with self.adw.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(self.resync_merge_sql, query_id=RESYNC_QUERY_ID, batch_id=resync_id)
            merged = cursor.rowcount
            self._log_batch(cursor, log_name, RESYNC_QUERY_ID, resync_id, staged, merged, None, counts.hi)
            self._clear_staging(cursor, RESYNC_QUERY_ID, resync_id)
            conn.commit()
        print(f"resync {label!r}: {counts.n} orders checked, {counts.gone} without events left, "
              f"{merged} ADW rows merged")
        return merged

    # ---- queries ------------------------------------------------------------------------------

    def fetch(self, sql: str, **binds) -> List[tuple]:
        """Run a query on ADW and return all rows (for the verification cells)."""
        with self.adw.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, **binds)
            return cursor.fetchall()

    def compare_with_source(self, external_table: str) -> dict:
        """Compare the ADW table with the source, entirely inside ADW.

        The latest event per order is computed from the Iceberg external table (the source, read
        in place) and joined with the live rows of the target table. It compares the
        event id, region, status and amount; not the event timestamp, which the two tables store
        with different time zone handling. Run it once the consumer has caught up; while events
        are in flight, recent orders show as different.
        """
        sql = f"""
            WITH latest AS (
                SELECT order_id, region, status, amount, event_id
                FROM (SELECT e.order_id, e.region, e.status, e.amount, e.event_id, e.op,
                             ROW_NUMBER() OVER (PARTITION BY e.order_id ORDER BY e.event_id DESC) AS rn
                      FROM {identifier(external_table)} e)
                WHERE rn = 1 AND op <> 'D')
            SELECT COUNT(l.order_id),
                   COUNT(o.order_id),
                   NVL(SUM(CASE WHEN o.order_id IS NULL THEN 1 END), 0),
                   NVL(SUM(CASE WHEN l.order_id IS NULL THEN 1 END), 0),
                   NVL(SUM(CASE WHEN l.order_id IS NOT NULL AND o.order_id IS NOT NULL
                                 AND (o.last_event_id <> l.event_id
                                      OR DECODE(o.region, l.region, 0, 1) = 1
                                      OR DECODE(o.status, l.status, 0, 1) = 1
                                      OR DECODE(o.amount, l.amount, 0, 1) = 1) THEN 1 END), 0)
            FROM latest l FULL OUTER JOIN (SELECT * FROM {self.target} WHERE is_deleted = 'N') o
                 ON o.order_id = l.order_id"""
        row = self.fetch(sql)[0]
        keys = ("orders_in_source", "orders_in_adw", "missing_in_adw", "extra_in_adw", "different")
        return dict(zip(keys, (int(v) for v in row)))

    # ---- internals ----------------------------------------------------------------------------

    def _merge_sql(self, matched_condition: str) -> str:
        return f"""
            MERGE INTO {self.target} t
            USING (
                SELECT order_id, region, status, amount, op, event_id, event_ts, watermark_id
                FROM (
                    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.order_id ORDER BY s.event_id DESC) AS rn
                    FROM {self.staging} s
                    WHERE s.query_id = :query_id AND s.batch_id = :batch_id
                )
                WHERE rn = 1
            ) s
            ON (t.order_id = s.order_id)
            WHEN MATCHED THEN UPDATE SET
                t.region = s.region, t.status = s.status, t.amount = s.amount,
                t.last_event_id = s.event_id, t.last_event_ts = s.event_ts,
                t.is_deleted = CASE WHEN s.op = 'D' THEN 'Y' ELSE 'N' END, t.updated_at = SYSTIMESTAMP
                WHERE {matched_condition}
            WHEN NOT MATCHED THEN INSERT (order_id, region, status, amount, last_event_id, last_event_ts, is_deleted)
                VALUES (s.order_id, s.region, s.status, s.amount, s.event_id, s.event_ts,
                        CASE WHEN s.op = 'D' THEN 'Y' ELSE 'N' END)"""

    def _stream_query_id(self) -> str:
        if self._query_id is None:
            if not self.checkpoint_location:
                raise ValueError("upsert_batch needs the streaming query's checkpoint_location")
            query_id = checkpoint_query_id(self.checkpoint_location)
            if not self.allow_new_checkpoint:
                with self.adw.connect() as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {self.batch_log} WHERE query_name = :q AND query_id <> :i",
                                   q=self.query_name, i=query_id)
                    if cursor.fetchone()[0]:
                        raise RuntimeError(
                            f"{self.batch_log} has batches of {self.query_name!r} from another checkpoint. A query "
                            "on a new checkpoint replays the source table into ADW state built by the old one, "
                            "which can bring back rows that skipped snapshots deleted. Start from empty ADW "
                            "tables (notebook 2's clean-up), or pass allow_new_checkpoint=True if ADW already "
                            "holds the right state.")
            self._query_id = query_id
        return self._query_id

    @staticmethod
    def _newest_per_order(events_df):
        from pyspark.sql import Window, functions as F
        newest_first = Window.partitionBy("order_id").orderBy(F.col("event_id").desc())
        return events_df.withColumn("_rn", F.row_number().over(newest_first)).where("_rn = 1").drop("_rn")

    def _stage(self, rows_df, query_id: str, batch_id: int) -> int:
        """Insert rows into the staging table from the executors; returns the row count."""
        from pyspark.sql import functions as F
        rows = rows_df.select(F.lit(query_id).alias("query_id"), F.lit(batch_id).cast("bigint").alias("batch_id"),
                              *STAGING_COLUMNS[2:])
        if self._executor_settings is None:
            # The password and wallet travel to the executors as a broadcast variable.
            self._executor_settings = self.spark.sparkContext.broadcast(self.adw.executor_settings())
        settings, sql, size = self._executor_settings, self.insert_sql, self.array_size
        return sum(rows.rdd.mapPartitions(lambda part: insert_partition(part, settings.value, sql, size)).collect())

    def _clear_staging(self, cursor, query_id: str, batch_id: int) -> None:
        cursor.execute(f"DELETE FROM {self.staging} WHERE query_id = :i AND batch_id = :b", i=query_id, b=batch_id)

    def _already_applied(self, cursor, query_id: str, batch_id: int) -> bool:
        cursor.execute(f"""SELECT COUNT(*) FROM {self.batch_log}
                           WHERE query_name = :q AND query_id = :i AND batch_id = :b""",
                       q=self.query_name, i=query_id, b=batch_id)
        return cursor.fetchone()[0] > 0

    def _log_batch(self, cursor, query_name: str, query_id: str, batch_id: int, rows_in: int, merged: int,
                   lo: Optional[int], hi: Optional[int]) -> None:
        cursor.execute(f"""INSERT INTO {self.batch_log}
                             (query_name, query_id, batch_id, rows_in, rows_merged, min_event_id, max_event_id)
                           VALUES (:q, :i, :b, :n, :m, :lo, :hi)""",
                       q=query_name, i=query_id, b=batch_id, n=rows_in, m=merged, lo=lo, hi=hi)
