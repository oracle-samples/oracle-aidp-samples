"""JDBC system table storage backend (Spark-coupled).

REQUIRES `SparkBackend` (or another `Backend` exposing a
`SparkSession` via `.spark`). The implementation drives
`spark.read.format("jdbc")` and `spark.write.format("jdbc")`,
not a generic Python JDBC client. PandasBackend cannot use this
backend; the `Qualifire._init_storage` constructor raises a
`ValueError` with migration text when invoked against a
non-Spark backend.

Suitable external databases: Oracle, PostgreSQL, MySQL, DB2.
SQL Server is intentionally out of scope (see
`_bounded_history_read` notes). For pandas-only deployments,
use `system_table_backend="sqlite"` (dev) or wait for a future
non-Spark JDBC path (not in this PR).
"""

from __future__ import annotations

import functools
import json
import logging
from contextlib import contextmanager
from typing import Any, Callable

from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS

logger = logging.getLogger(__name__)

# Sentinel for "argument not supplied" vs. `None` (match SQL NULL).
_UNSET = object()

from qualifire.storage._predicates import jdbc_dim_predicate


def _scrubbed(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator: catch any non-RuntimeError exception leaving a
    JDBCStorage public method and re-raise via ``self._scrub`` so
    raw Spark/JDBC exception strings (which after secret resolution
    may carry credential-bearing connection URLs) never escape into
    engine logs or persisted system-table rows. Already-scrubbed
    ``RuntimeError`` instances pass through unchanged.

    Codex impl-review round 3 finding 1.
    """

    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except RuntimeError:
            raise
        except Exception as e:
            raise self._scrub(e) from None

    return wrapped


class JDBCStorage:
    """System table stored in an external database via JDBC.

    Uses Spark's JDBC DataFrame writer for inserts and JDBC reads
    for queries. The target table must already exist (or will be
    auto-created by Spark on first write if the DB permits).

    Args:
        spark: SparkSession instance.
        table: Table name in the JDBC database.
        jdbc_url: JDBC connection URL (e.g., "jdbc:oracle:thin:@host:1521/service").
        connection_properties: Dict with "user", "password", and optional driver/other props.
    """

    def __init__(
        self,
        spark: Any,
        table: str,
        jdbc_url: str,
        connection_properties: dict[str, str] | None = None,
    ):
        self._spark = spark
        self._table = table
        self._jdbc_url = jdbc_url
        self._props = connection_properties or {}
        # Pre-computed sub-protocol-only form for any log line / error
        # message. Resolved JDBC URLs may carry user / password / token
        # inline (`jdbc:...//user:pwd@host`); the bare URL must not
        # appear in INFO/DEBUG/ERROR records or persisted state. See
        # ``qualifire/notification/_redact.py:redact_jdbc_url``.
        from qualifire.notification._redact import redact_jdbc_url

        self._safe_url = redact_jdbc_url(jdbc_url)

    def initialize(self) -> None:
        """Verify connectivity and ensure the table exists with the correct schema.

        Raises on connectivity or table-creation failure so a broken
        backend can't be silently accepted by ``_init_storage``. A run
        that proceeds past initialize() is guaranteed to be able to
        read and, if the table was auto-created, write to it.

        If the table does not exist, it is created by writing an empty
        DataFrame with the expected schema via Spark JDBC (letting the
        DB auto-create).
        """
        logger.info("Initializing JDBC storage: %s at %s", self._table, self._safe_url)
        # Dialect-aware probe. ``SELECT 1 AS ping`` is ANSI-portable on
        # PostgreSQL/MySQL/SQLite/SQL Server/DB2, but Oracle rejects a
        # bare SELECT without a FROM clause — it requires ``FROM DUAL``.
        # We advertise Oracle support in the JDBC block, so pick the
        # right form based on the URL prefix. If we ever add more
        # FROM-required dialects (e.g. Ingres), extend this branch.
        probe_sql = self._connectivity_probe_sql()
        try:
            self._read_jdbc(probe_sql).collect()
        except Exception as e:
            raise RuntimeError(
                f"JDBC connectivity check failed for {self._safe_url}: "
                f"{type(e).__name__}. Verify the URL, driver jar on the "
                "Spark classpath, and user/password in the 'jdbc:' block."
            ) from None
        self._ensure_table()

    def _connectivity_probe_sql(self) -> str:
        """Return a driver-appropriate connectivity probe subquery.

        Oracle rejects ``SELECT 1`` without a FROM clause; use
        ``DUAL``, Oracle's conventional single-row pseudo-table.
        Other dialects accept the bare form. Keyed off the URL prefix
        so we don't need the driver jar on the classpath to decide.
        """
        url_lower = self._jdbc_url.lower()
        if url_lower.startswith("jdbc:oracle:"):
            return "(SELECT 1 FROM DUAL) tmp"
        return "(SELECT 1 AS ping) tmp"

    def _ensure_table(self) -> None:
        """Create the table if it doesn't exist by writing an empty DataFrame.

        Raises if creation fails — the run cannot silently proceed
        with a backend that will swallow writes later.

        Why timestamps are stored as STRING, not TIMESTAMP
        ---------------------------------------------------
        ``write_results`` receives already-ISO-8601 timestamp strings
        from the engine (see ``datetime.now().isoformat()`` call
        sites). If we declared these columns as TIMESTAMP the JDBC
        driver would have to implicitly cast the bind parameter on
        insert, which works on lenient drivers (PostgreSQL, MySQL)
        and fails or mis-stores on stricter ones (older Oracle
        thin, SQL Server with ``datetime2``). Storing as STRING
        keeps the declared type honest about what is actually
        written, makes the write path driver-agnostic, and lets
        range predicates use lexicographic comparison which is
        semantically correct for ISO-8601. The SQLite backend
        already uses TEXT for the same columns for the same
        reason.
        """
        # Check if the table exists by reading 0 rows. The returned
        # DataFrame's schema is what Spark's JDBC reader inferred
        # from the remote column types — validate it against the
        # columns we'll actually insert/select against later.
        #
        # Spark's JDBC reader is lazy: ``_read_jdbc()`` returns a
        # DataFrame without actually executing the subquery — the
        # remote parse (and any "relation does not exist" error)
        # fires only on the first action. If we only wrapped the
        # read call in ``try/except``, a truly missing table would
        # slip past the guard and blow up at
        # ``existing_df.schema.fields`` below, short-circuiting the
        # "fall through to create path" logic. Touch ``.schema``
        # inside the guard to force remote parsing, so the missing-
        # table case is caught here and we actually create the
        # table. (Same pattern as the FETCH FIRST / LIMIT fallback
        # in ``_bounded_history_read``.)
        existing_fields: list[Any] | None = None
        try:
            existing_df = self._read_jdbc(
                f"(SELECT * FROM {self._table} WHERE 1=0) tmp"
            )
            existing_fields = list(existing_df.schema.fields)
        except Exception:
            # Treat any read failure as "table doesn't exist"; fall
            # through to the create path. Don't conflate this with
            # the schema-mismatch RuntimeError below.
            logger.info(
                "JDBC table %s not found; creating via empty DataFrame write",
                self._table,
            )

        if existing_fields is not None:
            existing_by_name = {f.name: f for f in existing_fields}
            missing = [c for c in SYSTEM_TABLE_COLUMNS if c not in existing_by_name]
            if missing:
                raise RuntimeError(
                    f"JDBC table '{self._table}' at {self._safe_url} is missing "
                    f"required column(s): {', '.join(missing)}. Either drop "
                    "the table so qualifire can recreate it with the correct "
                    "schema, or add the columns with types matching the STRING/"
                    "DOUBLE contract documented on _ensure_table()."
                )
            # Validate column *types*, not just names. ``write_results``
            # sends every non-DOUBLE column as a ``str(...)`` bind
            # parameter (including the ``*_timestamp``/``*_at`` fields,
            # which are ISO-8601 strings by contract). If a DBA
            # pre-created the table with native TIMESTAMP/DATE columns,
            # initialize() would have passed on name alone but the
            # first real write would fail on strict drivers (Oracle,
            # SQL Server) or mis-store on lenient ones (PostgreSQL,
            # MySQL). Reject at init time so the failure is loud,
            # immediate, and actionable instead of mid-run.
            from pyspark.sql.types import (
                ByteType,
                DecimalType,
                DoubleType,
                FloatType,
                IntegerType,
                LongType,
                ShortType,
                StringType,
            )
            # Numeric-compatible types for ``metric_value``. ``DOUBLE``
            # is the canonical write type, but any numeric column the
            # JDBC reader would upcast to DOUBLE is equally safe to
            # insert into (Spark narrows on the write side).
            _NUMERIC_TYPES = (
                DoubleType, FloatType, DecimalType,
                IntegerType, LongType, ShortType, ByteType,
            )
            # String-compatible types for every other column. Some JDBC
            # drivers surface VARCHAR/CHAR as vendor-specific subclasses
            # of StringType (e.g. VarcharType exists on newer Spark
            # versions as a subclass); isinstance() handles subclasses
            # correctly, so we don't need to enumerate them.
            type_errors: list[str] = []
            for col_name in SYSTEM_TABLE_COLUMNS:
                field = existing_by_name[col_name]
                dt = field.dataType
                if col_name == "metric_value":
                    if not isinstance(dt, _NUMERIC_TYPES):
                        type_errors.append(
                            f"  - {col_name}: expected a numeric type "
                            f"(DOUBLE/FLOAT/DECIMAL/INT/LONG), got "
                            f"{dt.simpleString()}"
                        )
                else:
                    # Every other column is written via ``str(value)``.
                    # The driver must accept a string bind parameter
                    # against this column — i.e. the inferred Spark
                    # type must be string-compatible. TIMESTAMP/DATE
                    # native types are specifically rejected: ISO-8601
                    # string writes against them are driver-dependent
                    # and silently lossy on the lenient ones.
                    if not isinstance(dt, StringType):
                        type_errors.append(
                            f"  - {col_name}: expected STRING/VARCHAR/TEXT, "
                            f"got {dt.simpleString()}. (Qualifire writes "
                            "ISO-8601 strings to timestamp columns by "
                            "design; see the docstring on _ensure_table.)"
                        )
            if type_errors:
                raise RuntimeError(
                    f"JDBC table '{self._table}' at {self._safe_url} has "
                    "incompatible column type(s):\n"
                    + "\n".join(type_errors)
                    + "\n\nEither drop the table so qualifire can recreate "
                    "it with the correct schema, or alter the columns to "
                    "match the STRING/DOUBLE contract documented on "
                    "_ensure_table()."
                )
            logger.info("JDBC table %s exists with compatible schema", self._table)
            return

        try:
            from pyspark.sql.types import (
                DoubleType,
                StringType,
                StructField,
                StructType,
            )

            fields = []
            for col_name in SYSTEM_TABLE_COLUMNS:
                spark_type_str = COLUMN_DEFINITIONS[col_name][0]
                # STRING and TIMESTAMP columns both land as StringType
                # on the JDBC side — see docstring for rationale.
                if spark_type_str == "DOUBLE":
                    field_type = DoubleType()
                else:
                    field_type = StringType()
                fields.append(StructField(col_name, field_type, True))

            schema = StructType(fields)
            empty_df = self._spark.createDataFrame([], schema)
            empty_df.write.jdbc(
                url=self._jdbc_url,
                table=self._table,
                mode="append",
                properties=self._props,
            )
        except Exception as e:
            raise RuntimeError(
                f"Could not create JDBC table '{self._table}' at "
                f"{self._safe_url}: {type(e).__name__}. Verify the user "
                "has CREATE TABLE permission, or create the table "
                "out-of-band and re-run."
            ) from None

    def _scrub(self, exc: Exception) -> RuntimeError:
        """Wrap a Spark/JDBC exception so its message no longer
        embeds the connection URL (which after secret resolution
        carries credentials). Spark surfaces the URL in plenty of
        nested error strings — we strip the literal `self._jdbc_url`
        and keep the exception type for diagnostic value.
        """
        original = str(exc)
        scrubbed = original.replace(self._jdbc_url, self._safe_url)
        return RuntimeError(
            f"{type(exc).__name__} on {self._safe_url} table "
            f"'{self._table}': {scrubbed}"
        )

    @contextmanager
    def _scrubbing(self):
        """Context manager that catches any exception arising inside
        a JDBC read/write operation and re-raises through ``_scrub``.

        Used on every public read method to keep raw Spark/JDBC
        exception strings (which may embed the connection URL with
        credentials) out of engine logs and persisted system-table
        rows. See Codex impl-review round 3 finding 1.
        """
        try:
            yield
        except RuntimeError:
            # Already scrubbed (e.g. by ``_ensure_table``'s explicit
            # raises which use ``self._safe_url`` directly). Don't
            # re-wrap.
            raise
        except Exception as e:
            raise self._scrub(e) from None

    def write_results(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from pyspark.sql import Row

        spark_rows = []
        for row in rows:
            cleaned = {}
            for col in SYSTEM_TABLE_COLUMNS:
                v = row.get(col)
                if col == "is_active" and v is None:
                    v = "true"
                if col == "metric_value" and v is not None:
                    cleaned[col] = float(v)
                elif col == "details_json" and isinstance(v, dict):
                    cleaned[col] = json.dumps(v)
                elif col == "expected_value" and isinstance(v, dict):
                    cleaned[col] = json.dumps(v)
                else:
                    cleaned[col] = str(v) if v is not None else None
            spark_rows.append(Row(**cleaned))

        try:
            df = self._spark.createDataFrame(spark_rows)
            df.write.jdbc(
                url=self._jdbc_url,
                table=self._table,
                mode="append",
                properties=self._props,
            )
        except Exception as e:
            # Engine catches and persists ``str(exc)`` into the
            # system table's ``qualifire.persistence`` warning row.
            # Any URL-bearing exception text would land in a
            # durable, queryable column. Scrub before re-raising.
            raise self._scrub(e) from None
        logger.debug("Wrote %d rows to JDBC table %s", len(rows), self._table)

    # Multiplier applied to ``limit * step_seconds`` when computing a
    # lower-bound cutoff for the bucketed read path. History that
    # straddles gaps (weekends, holidays, deploy freezes) needs extra
    # window beyond the nominal "limit buckets × step". 3× is enough
    # to tolerate ~2/3 of windows being empty before the validator
    # would fall under ``past_values`` anyway. Tune if a dataset
    # routinely has sparser coverage.
    _BUCKETED_READ_SAFETY_MULTIPLIER = 3

    @_scrubbed
    def read_metric_history(
        self,
        table_name: str,
        metric_name: str,
        limit: int = 90,
        step: str | None = None,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read past values for a metric. H2-aware soft-delete.

        JDBC databases vary widely in their SQL dialect — Oracle wants
        ``FETCH FIRST``, MySQL wants ``LIMIT``, and integer-division
        syntax for bucketing (``DIV`` vs ``/`` vs ``FLOOR``) differs
        even more. Rather than paper over that with database-specific
        SQL, this backend does bucketing and ordering Spark-side on
        the DataFrame returned by the JDBC reader: the DB only sees
        portable predicate filters, and the window logic runs inside
        Spark. See ``SQLiteStorage.read_metric_history`` for semantics.

        H2 contract: filter ``is_active = 'true'``
        AFTER the per-partition dedup ROW_NUMBER. ``metric_value IS
        NOT NULL`` is also applied AFTER the dedup so a tombstone
        (NULL value, ``is_active='false'``) wins ROW_NUMBER for its
        partition_ts and is then excluded — it doesn't let a stale
        active row underneath surface.
        """
        from datetime import timedelta

        from qualifire.core.duration import parse_step_seconds

        step_seconds = parse_step_seconds(step)
        t_sql = table_name.replace("'", "''")
        m_sql = metric_name.replace("'", "''")
        dim_predicate = jdbc_dim_predicate("dimension_value", dimension_value)
        # H2-strict: `metric_value IS NOT NULL` is NOT pushed to the
        # DB anymore; it is applied Spark-side post-dedup so a
        # tombstone wins ROW_NUMBER for its partition_ts and is then
        # filtered out, hiding any stale active row underneath.
        base_where = (
            f"table_name = '{t_sql}' "
            f"AND metric_name = '{m_sql}' "
            f"AND {dim_predicate} "
            f"AND run_timestamp IS NOT NULL"
        )

        if step_seconds is None:
            df = self._bounded_history_read(base_where, limit)
        else:
            df = self._bucketed_history_read(base_where, limit, step_seconds)

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        precedence = (
            F.when(F.col("record_type") == F.lit("collection"), F.lit(0))
            .otherwise(F.lit(1))
        )
        # Spark-side H2 filter: is_active = 'true'
        # AND metric_value IS NOT NULL AFTER dedup. Pre-existing-
        # tables-without-column tolerated via COALESCE.
        is_active_active = (
            F.col("is_active") == F.lit("true")
        )
        if step_seconds is None:
            # Non-bucketed: dedup per partition_ts first (latest
            # run_timestamp wins, validation > collection on tie),
            # THEN filter is_active + metric_value, then take top N.
            partition_window = Window.partitionBy("partition_ts").orderBy(
                F.col("run_timestamp").desc(),
                precedence.asc(),
            )
            deduped = (
                df.withColumn("_rn_part", F.row_number().over(partition_window))
                .filter(F.col("_rn_part") == 1)
                .drop("_rn_part")
                .filter(is_active_active)
                .filter(F.col("metric_value").isNotNull())
            )
            ordered = deduped.orderBy(
                precedence.asc(), F.col("run_timestamp").desc()
            ).limit(int(limit))
            return [r.asDict() for r in ordered.collect()]

        # Cast to ``timestamp`` before ``unix_timestamp`` so the
        # engine's ``str(datetime.now())`` output (space separator +
        # microseconds, sometimes ``T`` separator from the context
        # layer) parses cleanly. The no-format overload of
        # ``unix_timestamp`` uses Spark's default pattern
        # ``yyyy-MM-dd HH:mm:ss``, which in CORRECTED timeParserPolicy
        # (the Spark 3+ default) either raises or returns NULL on the
        # trailing ``.ffffff`` — silently zeroing out every JDBC
        # bucketed history read. ``cast("timestamp")`` uses the
        # Catalyst ISO-8601 parser, which handles both separators and
        # optional fractional seconds. See the regression test in
        # ``test_spark_storages.py``.
        ts_col = F.col("run_timestamp").cast("timestamp")
        bucket = (
            F.floor(
                F.unix_timestamp(ts_col).cast("long")
                / F.lit(int(step_seconds))
            )
        ).cast("long")
        # Bucketed (H2 two-stage): first dedup per partition_ts, then
        # filter is_active and metric_value, then bucket the survivors.
        partition_window = Window.partitionBy("partition_ts").orderBy(
            F.col("run_timestamp").desc(),
            precedence.asc(),
        )
        survivors = (
            df.withColumn("_rn_part", F.row_number().over(partition_window))
            .filter(F.col("_rn_part") == 1)
            .drop("_rn_part")
            .filter(is_active_active)
            .filter(F.col("metric_value").isNotNull())
        )
        with_bucket = survivors.withColumn("_bucket", bucket).filter(
            F.col("_bucket").isNotNull()
        )
        window = Window.partitionBy("_bucket").orderBy(
            precedence.asc(), F.col("run_timestamp").desc()
        )
        picked = (
            with_bucket.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_bucket", "_rn")
            .orderBy(F.col("run_timestamp").desc())
            .limit(int(limit))
        )
        return [r.asDict() for r in picked.collect()]

    @_scrubbed
    def read_metric_history_by_partition(
        self,
        table_name: str,
        metric_name: str,
        anchor_ts: Any,
        count: int,
        step: str,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Partition-anchored history read (JDBC).

        Pushes the selective predicates (table/metric/dim/partition_ts IN ...)
        down to the database; does the dedupe (one row per partition_ts,
        preferring collection rows) Spark-side, mirroring the rest of this
        backend's "DB filters, Spark windowizes" pattern.

        ``dimension_value=None`` matches rows persisted with SQL NULL.
        """
        from qualifire.core.duration import partition_lookback_anchors

        if count <= 0:
            return []
        anchors = partition_lookback_anchors(anchor_ts, count, step)
        # ANSI ``TIMESTAMP '...'`` literal with a *space* separator,
        # not the ISO ``T`` form. JDBC pushdown delivers this string
        # to the target dialect's parser; Oracle's default
        # ``NLS_TIMESTAMP_FORMAT`` does not accept ``T`` between date
        # and time, so a bare string IN list silently matches zero
        # rows and every drift / forecast / anomaly run cold-starts
        # forever. The ANSI ``TIMESTAMP '...'`` literal works
        # uniformly across Oracle / Postgres / MySQL / DB2.
        anchor_strs = [
            a.isoformat(sep=" ").replace("'", "''") for a in anchors
        ]
        in_list = ", ".join(f"TIMESTAMP '{s}'" for s in anchor_strs)
        t_sql = table_name.replace("'", "''")
        m_sql = metric_name.replace("'", "''")
        dim_predicate = jdbc_dim_predicate("dimension_value", dimension_value)
        subquery = f"""
            (SELECT metric_name, metric_value, run_timestamp, validation_status,
                    record_type, collector_name, dimension_value, partition_ts,
                    collected_at, is_active
             FROM {self._table}
             WHERE table_name = '{t_sql}'
               AND metric_name = '{m_sql}'
               AND {dim_predicate}
               AND partition_ts IN ({in_list})
            ) hist
        """
        df = self._read_jdbc(subquery)

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # H2 contract: dedup per partition_ts FIRST (latest run_timestamp
        # wins, validation > collection on tie), THEN filter is_active +
        # metric_value, so a tombstone wins ROW_NUMBER and excludes any
        # stale active row underneath it.
        precedence = (
            F.when(F.col("record_type") == F.lit("collection"), F.lit(0))
            .otherwise(F.lit(1))
        )
        latest_ts = F.coalesce(F.col("collected_at"), F.col("run_timestamp"))
        window = Window.partitionBy("partition_ts").orderBy(
            F.col("run_timestamp").desc(),
            precedence.asc(),
            latest_ts.desc(),
        )
        is_active_active = (
            F.col("is_active") == F.lit("true")
        )
        picked = (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .filter(is_active_active)
            .filter(F.col("metric_value").isNotNull())
            .orderBy(F.col("partition_ts").desc())
        )
        return [r.asDict() for r in picked.collect()]

    @_scrubbed
    def read_collection_metric_at_partition(
        self,
        table_name: str,
        metric_name: str,
        anchor_ts: Any,
        dimension_value: str | None = None,
    ) -> dict[str, Any] | None:
        """H6: latest active collection row at exactly ``anchor_ts``.

        Signature matches the SystemTableStorage Protocol.
        """
        from datetime import datetime
        t_sql = table_name.replace("'", "''")
        m_sql = metric_name.replace("'", "''")
        if isinstance(anchor_ts, datetime):
            pt = anchor_ts.isoformat()
        else:
            pt = str(anchor_ts)
        pt_sql = pt.replace("'", "''")
        dim_predicate = jdbc_dim_predicate("dimension_value", dimension_value)
        subquery = (
            f"(SELECT * FROM {self._table} "
            f"WHERE table_name = '{t_sql}' "
            f"AND metric_name = '{m_sql}' "
            f"AND record_type = 'collection' "
            f"AND partition_ts = '{pt_sql}' "
            f"AND {dim_predicate}) tmp"
        )
        df = self._read_jdbc(subquery)

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        window = Window.partitionBy(
            "table_name", "metric_name", "dimension_value", "partition_ts",
        ).orderBy(
            F.col("run_timestamp").desc(),
            F.col("collected_at").desc(),
        )
        is_active_active = (
            F.col("is_active") == F.lit("true")
        )
        picked = (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .filter(is_active_active)
            .filter(F.col("metric_value").isNotNull())
            .limit(1)
        )
        rows = picked.collect()
        return rows[0].asDict() if rows else None

    @_scrubbed
    def read_collection_dim_values_at_partition(
        self,
        *,
        table_name: str,
        metric_name: str,
        partition_ts: Any,
    ) -> list[str | None]:
        """Distinct active dim values for one ``(table, metric)`` at
        the partition. Backs ``skip_recollection`` dimensional
        enumeration."""
        from datetime import datetime
        if isinstance(partition_ts, datetime):
            pt = partition_ts.isoformat()
        else:
            pt = str(partition_ts)
        pt_sql = pt.replace("'", "''")
        t_sql = table_name.replace("'", "''")
        m_sql = metric_name.replace("'", "''")
        subquery = (
            f"(SELECT * FROM {self._table} "
            f"WHERE record_type = 'collection' "
            f"AND table_name = '{t_sql}' "
            f"AND metric_name = '{m_sql}' "
            f"AND partition_ts = '{pt_sql}') tmp"
        )
        df = self._read_jdbc(subquery)

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # Codex impl-review R1 MAJOR: tiebreaker on ``collected_at``.
        window = Window.partitionBy("dimension_value").orderBy(
            F.col("run_timestamp").desc(),
            F.coalesce(F.col("collected_at"), F.col("run_timestamp")).desc(),
        )
        is_active_active = (
            F.col("is_active") == F.lit("true")
        )
        picked = (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .filter(is_active_active)
            .filter(F.col("metric_value").isNotNull())
            .select("dimension_value")
            .distinct()
            .orderBy("dimension_value")
        )
        return [r["dimension_value"] for r in picked.collect()]

    @_scrubbed
    def read_validations_at_partition(
        self,
        *,
        dataset_name: str,
        validation_name_prefix: str,
        partition_ts: Any,
    ) -> list[dict[str, Any]]:
        """All active validation rows at a partition for one dataset+
        validation prefix. Backs ``skip_revalidation``."""
        from datetime import datetime
        if isinstance(partition_ts, datetime):
            pt = partition_ts.isoformat()
        else:
            pt = str(partition_ts)
        pt_sql = pt.replace("'", "''")
        ds_sql = dataset_name.replace("'", "''")
        prefix_sql = validation_name_prefix.replace("'", "''")
        # codex impl-review R1 MAJOR: escape SQL LIKE metacharacters.
        like_escaped = (
            prefix_sql
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        like_pattern = f"{like_escaped}.%"
        subquery = (
            f"(SELECT * FROM {self._table} "
            f"WHERE record_type = 'validation' "
            f"AND dataset_name = '{ds_sql}' "
            f"AND (validation_name = '{prefix_sql}' "
            f"OR validation_name LIKE '{like_pattern}' ESCAPE '\\') "
            f"AND partition_ts = '{pt_sql}') tmp"
        )
        df = self._read_jdbc(subquery)

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        window = Window.partitionBy(
            "validation_name", "metric_name", "dimension_value",
        ).orderBy(F.col("run_timestamp").desc())
        is_active_active = (
            F.col("is_active") == F.lit("true")
        )
        picked = (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .filter(is_active_active)
            .orderBy("validation_name", "metric_name", "dimension_value")
        )
        return [r.asDict() for r in picked.collect()]

    def _bounded_history_read(self, where_clause: str, limit: int) -> Any:
        """Return a DataFrame of at most ``limit`` rows for the
        metric-history read, with ORDER BY + LIMIT pushed into the
        JDBC subquery so the DB doesn't ship the entire history
        table to Spark.

        Tries ANSI SQL ``FETCH FIRST N ROWS ONLY`` first (Oracle,
        PostgreSQL 10+, DB2). Falls back to ``LIMIT N`` for
        MySQL/SQLite/older-PostgreSQL dialects. As a last resort
        (e.g. a dialect we don't know about), returns the unbounded
        subquery and lets the caller limit Spark-side — not ideal,
        but at least it still works.

        SQL Server note: SQL Server does not accept either form inside
        a derived table (it wants ``TOP N`` or ``OFFSET ... FETCH
        NEXT ... ROWS ONLY``, and rejects ``ORDER BY`` in a subquery
        without one of those). Spark JDBC also requires the predicate
        to be expressible as a subquery for pushdown, so SQL Server
        support would need a dedicated code path. It is intentionally
        out of scope for this reader; operators on SQL Server should
        use one of the other supported dialects, or file an issue.
        """
        # Dedupe rule (consistent across backends + dashboard JS):
        # latest run_timestamp wins; on a tie, validation wins so the
        # surviving row carries the verdict rather than its paired
        # collection row. CASE WHEN is ANSI SQL and portable across
        # every JDBC dialect this library targets.
        projection = (
            "SELECT metric_name, metric_value, run_timestamp, validation_status, "
            "record_type, collector_name, dimension_value, partition_ts, is_active "
            f"FROM {self._table} "
            f"WHERE {where_clause} "
            "ORDER BY "
            "run_timestamp DESC, "
            "CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END ASC"
        )
        lim = int(limit)
        attempts = [
            f"({projection} FETCH FIRST {lim} ROWS ONLY) tmp",
            f"({projection} LIMIT {lim}) tmp",
        ]
        # ``self._read_jdbc`` returns a lazy DataFrame: the SQL is
        # only parsed by the remote engine when an action runs (e.g.
        # ``collect``/``count``/``schema``). If we just return the
        # first attempt, a MySQL/SQLite/older-PostgreSQL dialect that
        # rejects ``FETCH FIRST`` will not fail here — it'll fail at
        # the caller's ``.collect()``, long past the try/except, so
        # the ``LIMIT`` fallback is never reached and every
        # non-bucketed history read crashes. Force a lightweight
        # action here (``.schema`` triggers query planning remotely
        # for a subquery-table read) so the dialect rejection
        # surfaces inside the loop and we actually fall back.
        last_err: Exception | None = None
        for subquery in attempts:
            try:
                df = self._read_jdbc(subquery)
                _ = df.schema  # force remote parse
                return df
            except Exception as e:
                last_err = e
                continue
        # Spark/JDBC exception messages can embed connection details
        # (URL, sometimes credentials inside SQLState text). Log only
        # the exception type — diagnostic value comes from the type
        # name plus the dialect-rejection context.
        logger.warning(
            "JDBC dialect rejected both FETCH FIRST and LIMIT pushdown (%s); "
            "falling back to unbounded subquery + Spark-side limit. This "
            "will scan the full history for this metric.",
            type(last_err).__name__ if last_err else "<no-exception>",
        )
        return self._read_jdbc(f"({projection}) tmp")

    def _bucketed_history_read(
        self, where_clause: str, limit: int, step_seconds: int
    ) -> Any:
        """Fetch the rows needed to form the last ``limit`` buckets,
        anchored to the newest matching row rather than wall-clock.

        Why two queries (fast path)
        ---------------------------
        We need a bounded remote read (else the DB ships the whole
        history), but the pushdown cutoff has to be relative to the
        newest *actual* row for that ``(table_name, metric_name)``.
        Using ``now() - limit*step`` fails after long idle periods:
        the real baselines are older than ``now - window`` and drop
        off, which silently flips the validator into cold-start mode
        even though valid history exists. So we first ask the DB
        for MAX(run_timestamp) under the same predicates, then
        pull rows from ``(max - limit*step*safety)`` upward.

        Correctness fallback for sparse history
        ---------------------------------------
        The ``3×`` safety window only tolerates ~2/3 empty buckets in
        the targeted range. For sparser schedules (monthly datasets
        with weekly ``step``, datasets inside long deploy freezes,
        etc.), the heuristic can return fewer than ``limit`` rows —
        which necessarily produces fewer than ``limit`` distinct
        buckets downstream, silently dropping older valid history.
        To match the SQLite/Delta/Catalog contract of "bucket over
        the full matching history", we count the bounded result and,
        if it has fewer rows than the requested ``limit``, re-read
        without the cutoff. The bounded read stays the fast path;
        the fallback only fires when it was provably insufficient.

        If MAX comes back empty (truly no matching history), fall
        back to an empty DataFrame without a second query.
        """
        max_subquery = (
            f"(SELECT MAX(run_timestamp) AS max_ts "
            f"FROM {self._table} WHERE {where_clause}) tmp"
        )
        max_rows = self._read_jdbc(max_subquery).collect()
        if not max_rows or max_rows[0]["max_ts"] is None:
            # No data at all — return an empty frame cheaply without
            # firing the second query.
            empty_subquery = (
                f"(SELECT metric_name, metric_value, run_timestamp, "
                f"validation_status, record_type, collector_name, "
                f"dimension_value, partition_ts, is_active FROM {self._table} "
                f"WHERE {where_clause} AND 1=0) tmp"
            )
            return self._read_jdbc(empty_subquery)

        max_ts = max_rows[0]["max_ts"]
        # ``max_ts`` may come back as a Python datetime (if the DB
        # column happens to be TIMESTAMP despite our STRING contract
        # — possible on DBA-created tables) or a string. Normalize.
        if hasattr(max_ts, "isoformat"):
            newest_iso = max_ts.isoformat()
        else:
            newest_iso = str(max_ts)

        # Parse newest and subtract the safety window to get cutoff.
        # Tolerate minor ISO variants with/without timezone offsets
        # by stripping a trailing 'Z' if present (``fromisoformat``
        # accepts '+00:00' but not 'Z' on Python < 3.11 consistently).
        from datetime import datetime, timedelta

        parseable = newest_iso.rstrip("Z") if newest_iso.endswith("Z") else newest_iso
        try:
            newest_dt = datetime.fromisoformat(parseable)
        except ValueError:
            # If we can't parse the timestamp we have no principled
            # way to pick a cutoff — fall back to an unbounded read
            # so we don't silently drop valid history.
            logger.warning(
                "JDBC MAX(run_timestamp)='%s' could not be parsed as ISO; "
                "falling back to unbounded bucketed read.", newest_iso
            )
            return self._unbounded_history_read(where_clause)

        window_seconds = int(limit) * int(step_seconds) * self._BUCKETED_READ_SAFETY_MULTIPLIER
        cutoff_dt = newest_dt - timedelta(seconds=window_seconds)
        cutoff_iso = cutoff_dt.isoformat()
        cutoff_sql = cutoff_iso.replace("'", "''")
        bounded_subquery = (
            f"(SELECT metric_name, metric_value, run_timestamp, validation_status, "
            f"record_type, collector_name, dimension_value, partition_ts, is_active "
            f"FROM {self._table} "
            f"WHERE {where_clause} "
            f"AND run_timestamp >= '{cutoff_sql}') tmp"
        )
        bounded_df = self._read_jdbc(bounded_subquery)

        # Sparse-history safety: after the bounded read, count the
        # *distinct buckets* we could form from it — not raw rows.
        # Raw row count can be misleading: many runs clustered inside
        # one or two recent buckets (e.g. a metric computed 50× in
        # the last day of a 7D step) easily exceed ``limit`` on row
        # count while the window collapses to too few distinct
        # buckets after the Spark-side ``ROW_NUMBER`` step. That
        # would silently hand the validator a truncated baseline.
        #
        # The distinct-bucket count is computed *in Spark* (see
        # ``_count_distinct_buckets``), using the same
        # ``unix_timestamp(...)/step_seconds`` expression that the
        # caller will apply downstream. A prior iteration counted
        # buckets Python-side via ``datetime.fromisoformat().timestamp()``
        # — but ``datetime.timestamp()`` on a naive datetime uses the
        # Python process timezone, while Spark's ``unix_timestamp()``
        # uses the Spark session timezone. When those disagree, this
        # check could claim sufficient buckets while Spark would
        # actually collapse them into fewer. Using Spark for both
        # makes the fallback decision exactly match the downstream
        # bucketing, regardless of the process/session TZ config.
        try:
            distinct_bucket_count = self._count_distinct_buckets(
                bounded_df, step_seconds
            )
            bounded_row_count = int(bounded_df.count())
        except Exception:
            # If we can't even count, let the caller's bucketing
            # fail loudly rather than silently swallow. Prefer
            # shipping with the bounded result than crashing the
            # fast path.
            logger.debug(
                "JDBC bucketed read: could not introspect bounded rows; "
                "skipping sparse-history fallback."
            )
            return bounded_df

        if distinct_bucket_count < int(limit):
            logger.info(
                "JDBC bucketed read (step=%ds, cutoff=%s) yielded %d distinct "
                "buckets from %d rows (< requested limit=%d); falling back to "
                "unbounded scan to avoid silently truncating the baseline on "
                "sparse or burst-clustered history.",
                step_seconds, cutoff_iso, distinct_bucket_count,
                bounded_row_count, limit,
            )
            return self._unbounded_history_read(where_clause)

        return bounded_df

    def _count_distinct_buckets(self, bounded_df: Any, step_seconds: int) -> int:
        """Count distinct buckets in ``bounded_df`` using the same
        Spark expression that drives downstream bucketing.

        Factored from ``_bucketed_history_read`` so both paths share
        one basis (Spark's ``unix_timestamp()`` / Spark session TZ),
        and so tests can exercise the sparse-history fallback without
        a live ``SparkContext`` by monkeypatching this method.

        Uses the same ``cast("timestamp")`` dance as
        ``_bucketed_history_read`` to handle engine-written ISO
        strings with microseconds.
        """
        from pyspark.sql import functions as F

        expr = F.floor(
            F.unix_timestamp(
                F.col("run_timestamp").cast("timestamp")
            ).cast("long")
            / F.lit(int(step_seconds))
        ).cast("long")
        return int(
            bounded_df
            .select(expr.alias("_b"))
            .filter(F.col("_b").isNotNull())
            .distinct()
            .count()
        )

    def _unbounded_history_read(self, where_clause: str) -> Any:
        """Full matching history without a time cutoff.

        Used as the correctness fallback when the bounded bucketed
        read returned too few rows, and as the unparseable-MAX
        escape hatch. Matches the SQLite/Delta/Catalog contract of
        scanning the full matching history and forming buckets from
        whatever is there.
        """
        return self._read_jdbc(
            f"(SELECT metric_name, metric_value, run_timestamp, "
            f"validation_status, record_type, collector_name, "
            f"dimension_value, partition_ts, is_active FROM {self._table} "
            f"WHERE {where_clause}) tmp"
        )

    @_scrubbed
    def read_latest_run(self, dataset_name: str) -> dict[str, Any] | None:
        """H8: tombstone latest → None.

        If the row with the newest ``run_timestamp`` for the dataset
        is a tombstone (``is_active='false'``), return None — no
        fallback to the next-most-recent active row.
        """
        d_sql = dataset_name.replace("'", "''")
        query = (
            f"(SELECT * FROM {self._table} "
            f"WHERE dataset_name = '{d_sql}' "
            f"ORDER BY run_timestamp DESC "
            f"FETCH FIRST 1 ROWS ONLY) tmp"
        )
        try:
            rows = self._read_jdbc(query).collect()
        except Exception:
            query_alt = (
                f"(SELECT * FROM {self._table} "
                f"WHERE dataset_name = '{d_sql}' "
                f"ORDER BY run_timestamp DESC) tmp"
            )
            rows = self._read_jdbc(query_alt).limit(1).collect()
        if not rows:
            return None
        d = rows[0].asDict()
        if str(d.get("is_active") or "true").lower() != "true":
            return None
        return d

    @_scrubbed
    def read_validation_history(
        self,
        dataset_name: str,
        validation_name: str,
        limit: int = 10,
        metric_name: Any = _UNSET,
        dimension_value: Any = _UNSET,
    ) -> list[dict[str, Any]]:
        # Same quote-escaping discipline as ``read_latest_run`` and
        # ``read_metric_history``: Spark's JDBC subquery interface
        # doesn't parameterize, so apostrophes in user-supplied
        # names either break the query or (worse) alter the
        # predicate. Harden every name.
        d_sql = dataset_name.replace("'", "''")
        v_sql = validation_name.replace("'", "''")
        clauses = [f"dataset_name = '{d_sql}'", f"validation_name = '{v_sql}'"]
        if metric_name is not _UNSET:
            if metric_name is None:
                clauses.append("metric_name IS NULL")
            else:
                m_sql = str(metric_name).replace("'", "''")
                clauses.append(f"metric_name = '{m_sql}'")
        if dimension_value is not _UNSET:
            clauses.append(jdbc_dim_predicate("dimension_value", dimension_value))
        where_sql = " AND ".join(clauses)
        # Pull a wider window to allow Spark-side per-(metric,
        # dimension, partition_ts) dedup before the soft-delete
        # filter; the H2 contract requires filtering is_active AFTER
        # ROW_NUMBER, not before. Cap the remote scan at limit*4 as
        # a reasonable budget — sufficient for correctness when up
        # to 75% of the rows in the candidate window are tombstones.
        # If the bound is too tight in practice we can revisit.
        scan_limit = max(int(limit) * 4, 50)
        query = (
            f"(SELECT validation_name, validation_type, validation_status, "
            f"validation_message, run_timestamp, "
            f"metric_name, dimension_value, partition_ts, is_active "
            f"FROM {self._table} "
            f"WHERE {where_sql} "
            f"ORDER BY run_timestamp DESC "
            f"FETCH FIRST {scan_limit} ROWS ONLY) tmp"
        )
        try:
            df = self._read_jdbc(query)
            _ = df.schema  # force remote parse
        except Exception:
            query_alt = (
                f"(SELECT validation_name, validation_type, validation_status, "
                f"validation_message, run_timestamp, "
                f"metric_name, dimension_value, partition_ts, is_active "
                f"FROM {self._table} "
                f"WHERE {where_sql} "
                f"ORDER BY run_timestamp DESC) tmp"
            )
            df = self._read_jdbc(query_alt).limit(scan_limit)

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # Dedup per (metric_name, dimension_value, partition_ts)
        # first; then filter is_active post-dedup so a tombstone hides
        # historical rows correctly (H2).
        window = Window.partitionBy(
            "metric_name", "dimension_value", "partition_ts"
        ).orderBy(F.col("run_timestamp").desc())
        is_active_active = (
            F.col("is_active") == F.lit("true")
        )
        deduped = (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .filter(is_active_active)
            .orderBy(F.col("run_timestamp").desc())
            .limit(int(limit))
            .drop("partition_ts", "is_active")
        )
        rows = deduped.collect()
        return [r.asDict() for r in rows]

    @_scrubbed
    def read_validation_history_bulk(
        self,
        keys: list[Any],
        limit: int = 1,
    ) -> dict[Any, list[dict[str, Any]]]:
        """One-JDBC-round-trip bulk read.

        Strategy: filter remote rows down with a server-side
        ``(dataset_name, validation_name) IN (...)`` predicate
        (portable across Oracle, PostgreSQL, MySQL, DB2 — these
        dialects all accept row-value `IN` lists for non-NULL
        scalars). Bring the filtered rows into Spark in a single
        JDBC read, then apply the NULL-safe metric/dimension
        matching plus per-key ``LIMIT 1`` Spark-side. This is
        bounded by the union of distinct (dataset, validation)
        pairs across all keys — typically equal to the validation
        count per run.

        Per-key looped reads (the previous shape) issued N
        round-trips even for SELECT-1-row-each queries, which
        dominated wall time on remote databases. The single-read
        form is the P4.3 contract.

        Item 3 (backfill-followups-and-polish): multi-row support
        via two-stage Python-side ranking after the bulk JDBC pull.
        Per-key, per-partition latest version → drop tombstones →
        re-rank per key → ``LIMIT``. Aligned with
        ``read_validation_history`` partition-aware semantics.
        """
        if limit < 1:
            raise ValueError(
                f"read_validation_history_bulk: limit must be >= 1; "
                f"got {limit}"
            )
        if not keys:
            return {}

        # De-dup (dataset_name, validation_name) tuples — they're
        # the only fields safe to push into a server-side IN-list
        # without dialect-fragile NULL handling. Metric / dim
        # filtering happens Spark-side after the read.
        ds_vn_pairs = sorted({(k.dataset_name, k.validation_name) for k in keys})

        # Build the server-side WHERE clause as an OR of
        # (dataset_name = ? AND validation_name = ?) clauses. This
        # is portable across every dialect we support; row-value IN
        # is not (Oracle accepts, MySQL accepts, SQL Server doesn't).
        # Quote-escape every literal — names from user configs can
        # contain apostrophes.
        clauses = []
        for ds, vn in ds_vn_pairs:
            ds_sql = ds.replace("'", "''")
            vn_sql = vn.replace("'", "''")
            clauses.append(f"(dataset_name = '{ds_sql}' AND validation_name = '{vn_sql}')")
        where_sql = " OR ".join(clauses) if clauses else "1=0"

        # Codex impl-review R1 HIGH: parenthesize the OR group so
        # ``record_type = 'validation'`` applies to every dataset/
        # validation pair, not just the last one (SQL precedence:
        # AND binds tighter than OR).
        query = (
            f"(SELECT dataset_name, validation_name, validation_type, "
            f"validation_status, validation_message, run_timestamp, "
            f"validated_at, run_id, partition_ts, "
            f"metric_name, dimension_value, is_active "
            f"FROM {self._table} "
            f"WHERE ({where_sql}) AND record_type = 'validation') tmp"
        )

        # Pull the filtered remote rows into a Spark DataFrame.
        try:
            df = self._read_jdbc(query)
            collected = df.collect()
        except Exception as e:
            # Spark/JDBC exception strings can embed the connection
            # URL and sometimes credentials. Persist type name only.
            logger.warning(
                "JDBC bulk read failed (%s); falling back to per-key reads",
                type(e).__name__,
            )
            result: dict[Any, list[dict[str, Any]]] = {}
            for k in keys:
                result[k] = self.read_validation_history(
                    dataset_name=k.dataset_name,
                    validation_name=k.validation_name,
                    limit=limit,
                    metric_name=k.metric_name,
                    dimension_value=k.dimension_value,
                )
            return result

        # Index returned rows by (dataset, validation) for cheap
        # client-side dispatch.
        from collections import defaultdict
        by_pair: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in collected:
            d = row.asDict()
            by_pair[(d["dataset_name"], d["validation_name"])].append(d)

        result: dict[Any, list[dict[str, Any]]] = {k: [] for k in keys}
        # Tiebreak helper: produce a sort key matching the SQL
        # ORDER BY (run_timestamp, validated_at, run_id) DESC.
        # Empty strings sort first under reverse=True (i.e. last —
        # last-resort tiebreak when fields are missing).
        def _sort_key(r):
            return (
                r.get("run_timestamp") or "",
                r.get("validated_at") or "",
                r.get("run_id") or "",
            )

        for k in keys:
            candidates = by_pair.get((k.dataset_name, k.validation_name), [])
            # NULL-safe matching on metric and dimension.
            wanted_dim = k.dimension_value
            matched = []
            for row in candidates:
                if row.get("metric_name") != k.metric_name:
                    continue
                if row.get("dimension_value") != wanted_dim:
                    continue
                matched.append(row)
            if not matched:
                continue

            # Stage 1: per partition_ts, pick latest version. A
            # tombstoned partition contributes its tombstone here;
            # the active filter in Stage 2 then excludes it.
            from collections import defaultdict
            by_partition: dict[Any, list[dict[str, Any]]] = defaultdict(list)
            for row in matched:
                by_partition[row.get("partition_ts")].append(row)
            stage1: list[dict[str, Any]] = []
            for _pt, rows_for_pt in by_partition.items():
                rows_for_pt.sort(key=_sort_key, reverse=True)
                stage1.append(rows_for_pt[0])

            # Stage 2: drop tombstones, re-rank, apply LIMIT.
            survivors = [
                r for r in stage1
                if str(r.get("is_active") or "true").lower() == "true"
            ]
            survivors.sort(key=_sort_key, reverse=True)
            top_n = survivors[: int(limit)]

            # Strip non-projected columns the singular call doesn't
            # expose so callers don't depend on internal fields.
            result[k] = [
                {
                    kk: vv for kk, vv in r.items()
                    if kk not in ("is_active", "partition_ts", "run_id",
                                  "validated_at")
                }
                for r in top_n
            ]
        return result

    @_scrubbed
    def read_health_data(
        self, days: int = 30, *, include_collection: bool = False,
    ) -> list[dict[str, Any]]:
        """Read recent validation rows for health reporting.

        Dialect portability: instead of relying on DB-specific date
        arithmetic (``DATEADD`` vs ``DATE_SUB`` vs ``INTERVAL``),
        compute the cutoff ISO timestamp in Python and compare as a
        string — ``run_timestamp`` is already stored as ISO-8601
        text by ``write_results``, so string comparison is
        lexicographically correct.

        Timezone basis
        --------------
        The engine writes ``run_timestamp`` via naive
        ``datetime.now().isoformat()`` (no ``+HH:MM`` offset, no
        ``Z``). Comparing against a ``datetime.now(timezone.utc)``
        cutoff would be wrong twice over: the cutoff string carries
        a ``+00:00`` offset that the stored strings lack (breaking
        lexicographic comparison), and the actual wall-clock
        reference differs by the process timezone offset. Match the
        engine's basis exactly by using naive ``datetime.now()`` so
        the last-N-days filter means what it says on the operator's
        machine.
        """
        from datetime import datetime, timedelta

        cutoff = (datetime.now() - timedelta(days=int(days))).isoformat()
        cutoff_sql = cutoff.replace("'", "''")
        record_type_clause = (
            "record_type IN ('validation', 'collection')"
            if include_collection
            else "record_type = 'validation'"
        )
        # Dedupe per natural key (dataset, validation_name, metric,
        # partition_ts, dim) to match SQLite/Delta/external_catalog.
        # Latest run_timestamp wins; on tie, validation > collection.
        query = (
            f"(WITH ranked AS (SELECT run_id, owner, bu, dataset_name, table_name, "
            f"validation_type, validation_status, "
            f"run_timestamp, validation_name, "
            f"metric_name, metric_value, "
            f"partition_ts, dimension_value, "
            f"expected_value, actual_value_text, "
            f"validation_message, record_type, is_active, "
            f"dataset_description, validation_description, details_json, "
            f"ROW_NUMBER() OVER ("
            f"  PARTITION BY dataset_name, validation_name, "
            f"               metric_name, partition_ts, dimension_value "
            f"  ORDER BY run_timestamp DESC, "
            f"           CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END"
            f") AS _rn "
            f"FROM {self._table} "
            f"WHERE run_timestamp >= '{cutoff_sql}') "
            f"SELECT run_id, owner, bu, dataset_name, table_name, "
            f"validation_type, validation_status, "
            f"run_timestamp, validation_name, "
            f"metric_name, metric_value, "
            f"partition_ts, dimension_value, "
            f"expected_value, actual_value_text, "
            f"validation_message, record_type, "
            f"dataset_description, validation_description, details_json "
            f"FROM ranked "
            f"WHERE _rn = 1 "
            f"AND is_active = 'true' "
            f"AND {record_type_clause} "
            f"ORDER BY run_timestamp DESC) tmp"
        )
        rows = self._read_jdbc(query).collect()
        return [r.asDict() for r in rows]

    def _read_jdbc(self, query: str) -> Any:
        """Read from JDBC using Spark."""
        return self._spark.read.jdbc(
            url=self._jdbc_url,
            table=query,
            properties=self._props,
        )
