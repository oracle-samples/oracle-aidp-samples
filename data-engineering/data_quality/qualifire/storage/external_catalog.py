"""External catalog system table storage (AIDP default, insert-only)."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from qualifire.core.exceptions import QualifireSystemTableError
from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS

# Strict per-segment unquoted-identifier check. ``self._table`` is
# interpolated into raw SQL (``CREATE SCHEMA IF NOT EXISTS …``) and
# into the ``saveAsTable(…)`` argument; without per-segment
# validation, a YAML typo like ``"cat. .tbl"`` or a hostile value
# like ``"default LOCATION '/tmp/x'.tbl"`` would land directly in
# the catalog's DDL parser. Spark's standard unquoted identifiers
# are letter / underscore lead, then letter / digit / underscore.
# Operators with non-standard column names (spaces, hyphens,
# reserved keywords) need backtick-quoted identifiers, which this
# storage layer does not synthesize — switch to a different system
# table name.
_IDENT_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

logger = logging.getLogger(__name__)

# Sentinel for "argument not supplied" vs. `None` (match SQL NULL).
_UNSET = object()


# ---------------------------------------------------------------------------
# Initialize-time error classifiers
# ---------------------------------------------------------------------------
# Substring matching on short tokens (especially vendor codes like
# ``ORA-01031``) risks false-positives when SQL echo / nested cause
# text contains the literal characters in an unrelated context.
# Using compiled regex with ``\b`` word boundaries (or run-of-non-
# alphanumeric for hyphenated codes) so the codes only match when
# they're actually error codes, not embedded payload data.

# Genuine permission-denied phrases. Bare tokens like ``DENIED``,
# ``INSUFFICIENT``, ``ACL`` would over-match transient resource errors
# and gateway failures, so we only match explicit privilege phrases or
# Oracle's ORA-01031 (insufficient privileges) error code.
_PERMISSION_PATTERN = re.compile(
    r"\b(?:"
    r"PERMISSION\s+DENIED"
    r"|ACCESS\s+DENIED"
    r"|ACCESS\s+CONTROL"
    r"|NOT\s+AUTHORIZED"
    r"|INSUFFICIENT\s+PRIVILEGE[S]?"
    r"|INSUFFICIENT\s+GRANT[S]?"
    r"|ORA-01031"
    r")\b",
    re.IGNORECASE,
)

# Read-only catalog phrases.
_READ_ONLY_PATTERN = re.compile(
    r"\b(?:"
    r"READ[-\s_]?ONLY"
    r"|WRITE\s+NOT\s+ALLOWED"
    r"|MODIFICATION\s+NOT\s+ALLOWED"
    r"|TABLE[-\s_]+IS[-\s_]+READ[-\s_]?ONLY"
    r")\b",
    re.IGNORECASE,
)

# Namespace / schema / database missing.
_NAMESPACE_NOT_FOUND_PATTERN = re.compile(
    r"\b(?:"
    r"SCHEMA[-\s_]?NOT[-\s_]?FOUND"
    r"|NAMESPACE[-\s_]?NOT[-\s_]?FOUND"
    r"|DATABASE[-\s_]?NOT[-\s_]?FOUND"
    r"|ORA-00959"     # Oracle: tablespace doesn't exist
    r"|ORA-04043"     # Oracle: object doesn't exist
    r")\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Write-time options applied to every system-table write
# ---------------------------------------------------------------------------
# AIDP / Oracle 23ai catalogs honour ``skip.oos.staging=true`` to bypass
# the object-storage staging hop for in-database writes. The system
# table is always small, always direct-to-database, never a candidate
# for OOS staging — so set the option unconditionally on every write.
# Non-AIDP catalogs silently ignore unknown writer options, so this is
# safe to set regardless of the underlying catalog.
#
# Source: ``datalake-connectivity`` —
# ``connectivity/spark-connectors/.../util/DataAccessParams.scala:47``
# (``val SKIP_OOS_STAGING = "skip.oos.staging"``) +
# ``builders/WriteOperationConfigBuilder.scala:290`` (per-write option
# read from the writer's options dict).
_SYSTEM_TABLE_WRITE_OPTIONS: dict[str, str] = {
    "skip.oos.staging": "true",
}

from qualifire.storage._predicates import spark_dim_predicate


class ExternalCatalogStorage:
    """System table stored in an external catalog (AIDP default).

    Uses Spark SQL for insert-only operations. External catalogs do not
    support UPDATE or DELETE — this is strictly append-only.

    Args:
        spark: SparkSession instance.
        table: Fully-qualified 3-part table name (catalog.schema.table).
    """

    def __init__(self, spark: Any, table: str):
        self._spark = spark
        self._table = table

    def initialize(self) -> None:
        """Ensure the system table exists; eagerly via the V2-native
        ``DataFrameWriterV2.create()`` primitive on an empty
        DataFrame.

        Three-tier contract — schema = fail loud, table = fail loud,
        column-drift = best-effort:

        * **Namespace** is NOT created by qualifire. ``CREATE SCHEMA
          IF NOT EXISTS`` is no longer issued — operators on every
          supported external catalog (AIDP, Unity Catalog, Iceberg
          REST, governed Hive) provision schemas out-of-band via
          DBA tooling. Missing namespace surfaces during table
          creation below as ``QualifireSystemTableError`` with a
          structured "ask your DBA" remediation hint.
        * **Table** is created eagerly, here, via
          ``empty_df.write.format("aidataplatform").mode("ignore")
          .saveAsTable(self._table)`` — but only after a
          ``DESCRIBE TABLE`` probe confirms it's missing. The
          probe-then-create sequence is required so insert-only
          operators (DBA pre-created the table, qualifire identity
          has only INSERT privilege) don't hit a create failure on
          a table that already exists.
        * **Column drift** stays best-effort
          (``_ensure_columns()``). Catalogs that reject DESCRIBE
          on existing tables (some governed Iceberg modes, REST
          catalogs without a schema endpoint) must continue to
          work for the unchanged-column-set case.

        Why explicit ``format("aidataplatform")`` instead of
        relying on Spark catalog routing:

        - Raw ``spark.sql("CREATE TABLE catalog.schema.table
          (...)")`` falls through to Spark's v1 default handler —
          on AIDP Workbench the session-default source is Delta,
          so the system table lands in Object Storage rather than
          in the underlying ADW.
        - ``empty_df.write.mode("ignore").saveAsTable(...)`` (no
          explicit format) lowers in V2 to
          ``CreateTableAsSelect(..., ignoreIfExists = true)`` —
          a data-write plan with zero rows; AIDP's connector
          short-circuits the zero-row data-write branch and never
          creates the table.
        - ``empty_df.writeTo(...).create()`` requires the
          catalog plugin to intercept CREATE TABLE DDL. AIDP's
          external-catalog feature wires the catalog plugin for
          **read** paths only — DDL routes through Spark's
          session-default source (Delta again).
        - ``empty_df.write.format("aidataplatform").mode("ignore")
          .saveAsTable(self._table)`` is the working path: forces
          the writer's data source to be the AIDP connector,
          which honors the catalog portion of the 3-part name and
          issues native Oracle ``CREATE TABLE`` against the
          underlying Autonomous Database.

        This makes the backend AIDP-only (``aidataplatform`` data
        source must be on the Spark classpath). Non-AIDP
        deployments should use the ``delta`` or ``jdbc`` backend.

        Raises:
            ValueError: if ``self._table`` is not a 3-part
                ``catalog.schema.table`` identifier or any segment
                fails the Spark unquoted-identifier regex.
            QualifireSystemTableError: if the eager table creation
                is required and fails — distinguished into four
                buckets (namespace-not-found / read-only catalog /
                privilege denied / catalog-rejected) with structured
                remediation hints. A ``TableAlreadyExistsException``
                raised in the DESCRIBE→create race window is
                **not** an error — treated as success.
        """
        parts = self._table.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"system_table={self._table!r} is not a 3-part "
                f"identifier (got {len(parts)} dot-separated "
                "segments). The external_catalog backend requires "
                "the standard 'catalog.schema.table' shape. For "
                "session-default-namespace or non-qualified table "
                "names, switch to a different "
                "system_table_backend ('delta', 'sqlite', or "
                "'jdbc')."
            )
        for segment in parts:
            if not _IDENT_SEGMENT_RE.fullmatch(segment):
                raise ValueError(
                    f"system_table={self._table!r} contains a "
                    f"malformed identifier segment {segment!r}. "
                    "Each dot-separated segment must match "
                    "[A-Za-z_][A-Za-z0-9_]* (Spark unquoted "
                    "identifier shape). Catches empty segments, "
                    "whitespace, embedded operators / keywords, "
                    "and other shapes that would inject into the "
                    "raw DDL."
                )
        namespace = ".".join(parts[:-1])

        # Probe-then-create: try DESCRIBE TABLE first; only call
        # saveAsTable if the probe says the table is missing.
        # Inverted detection — match permission-denied phrases (small,
        # well-defined set) and fall through to create for everything
        # else. ``saveAsTable(mode="ignore")`` is itself idempotent,
        # so a false "missing" diagnosis is recoverable; a false
        # "exists" would skip creation on a genuinely missing table.
        if self._table_exists_via_describe():
            logger.info(
                "System table %s exists; skipping eager creation",
                self._table,
            )
        else:
            self._create_system_table(namespace)

        self._ensure_columns()

    def _table_exists_via_describe(self) -> bool:
        """Best-effort existence probe via ``DESCRIBE TABLE``.

        Returns ``True`` when the probe confirms the table exists
        OR when DESCRIBE fails with an explicit privilege phrase
        (operator has restricted DESCRIBE on an existing table —
        common in governed catalogs).

        Returns ``False`` when DESCRIBE fails for any other reason
        (Oracle ``ORA-00942``, Spark ``TABLE_OR_VIEW_NOT_FOUND``,
        REST DSV2 ``UNSUPPORTED_OPERATION``, ambiguous
        ``AnalysisException``, transient connectivity errors).
        """
        try:
            self._spark.sql(f"DESCRIBE TABLE {self._table}").collect()
            return True
        except Exception as e:
            msg = str(e)
            if _PERMISSION_PATTERN.search(msg):
                logger.info(
                    "DESCRIBE TABLE %s denied (likely permission-restricted "
                    "but table exists); skipping CREATE: %s",
                    self._table, e,
                )
                return True
            logger.info(
                "DESCRIBE TABLE %s did not confirm existence (%s); "
                "attempting eager saveAsTable creation",
                self._table, e,
            )
            return False

    def _create_system_table(self, namespace: str) -> None:
        """Create the system table via the AIDP ``aidataplatform``
        data source explicitly
        (``empty_df.write.format("aidataplatform").mode("ignore")
        .saveAsTable(self._table)``).

        Why explicit ``format("aidataplatform")`` over the catalog-
        plugin route: the AIDP "external catalog mounted over ADW"
        feature wires a 3-part-name → catalog-connection mapping
        into Spark's catalog plugin layer for **read** paths
        (resolves table names through the connector, query
        push-down works). But CREATE TABLE DDL — both raw
        ``spark.sql("CREATE TABLE ...")`` and the V2 ``writeTo
        (...).create()`` primitive — observed to fall through to
        Spark's session-default source (Delta on AIDP Workbench)
        and land in Object Storage, never reaching the underlying
        Autonomous Database. Forcing the writer's format to
        ``aidataplatform`` bypasses Spark's catalog routing
        entirely and goes straight through the connector, which
        issues native Oracle ``CREATE TABLE`` against ADW for the
        3-part name's resolved catalog connection.

        Side-effect: this storage backend is now explicitly
        AIDP-only — incompatible with Unity Catalog, Iceberg REST,
        or governed Hive (which were never actually exercised
        anyway; the backend has always been named for AIDP). Those
        deployments should use the ``delta`` or ``jdbc`` backend
        instead.

        ``mode("ignore")`` is the ``saveAsTable`` analog of
        ``IF NOT EXISTS`` — no-op if the table already exists
        (covers the race window between the DESCRIBE probe and
        the write).

        Raises ``QualifireSystemTableError`` on failure, classified
        into one of four buckets so operators can route the fix
        directly. Classifier order matters — namespace and read-only
        win over privilege when dual-phrase errors (Glue / Ranger
        style) include both, since those are the upstream causes.
        """
        col_defs = ", ".join(
            f"{name} {COLUMN_DEFINITIONS[name][0]}"
            for name in SYSTEM_TABLE_COLUMNS
        )
        logger.info("Eagerly creating system table: %s", self._table)
        schema = self._system_table_spark_schema()
        empty_df = self._spark.createDataFrame([], schema)
        try:
            (
                empty_df.write
                .format("aidataplatform")
                .mode("ignore")
                .saveAsTable(self._table)
            )
        except Exception as e:
            msg = str(e)
            exc_name = type(e).__name__

            # 0. Race window — table appeared between the DESCRIBE
            # probe (which said missing) and this create. Treat as
            # success; the next ``write_results`` will find the
            # table and proceed. ``mode("ignore")`` should swallow
            # this internally, but some connector implementations
            # still raise — handle both shapes.
            if (
                "TABLE_ALREADY_EXISTS" in msg.upper()
                or "ALREADYEXISTS" in exc_name.upper()
                or "ALREADY EXISTS" in msg.upper()
            ):
                logger.info(
                    "System table %s appeared between DESCRIBE probe "
                    "and saveAsTable; treating as success",
                    self._table,
                )
                return

            # 1. Namespace doesn't exist — highest priority. Dual-
            # phrase errors (e.g. "DATABASE NOT FOUND" alongside
            # "INSUFFICIENT PRIVILEGES" from Glue/Ranger) route to
            # the upstream cause.
            if _NAMESPACE_NOT_FOUND_PATTERN.search(msg):
                raise QualifireSystemTableError(
                    f"Namespace {namespace!r} does not exist. "
                    f"Qualifire does not create catalog schemas — "
                    f"ask your DBA to create {namespace!r} once and "
                    f"re-run."
                ) from e

            # 2. Catalog is read-only.
            if _READ_ONLY_PATTERN.search(msg):
                raise QualifireSystemTableError(
                    f"System table {self._table!r} does not exist "
                    f"and the catalog is read-only. Switch the run "
                    f"to a writable catalog or ask your DBA to "
                    f"pre-create the table on a writable mirror. "
                    f"Required schema: ({col_defs})"
                ) from e

            # 3. Privilege / permission denied during create.
            if _PERMISSION_PATTERN.search(msg):
                raise QualifireSystemTableError(
                    f"System table {self._table!r} does not exist "
                    f"and qualifire lacks CREATE privilege on "
                    f"{namespace!r}. Ask your DBA to pre-create the "
                    f"table — qualifire only needs INSERT privilege "
                    f"from then on. Required schema: ({col_defs})"
                ) from e

            # 4. Connector-rejected / generic catch-all. Includes
            # the case where the ``aidataplatform`` data source
            # isn't registered (you're on a non-AIDP Spark build —
            # switch to ``system_table_backend="delta"`` or
            # ``"jdbc"``).
            raise QualifireSystemTableError(
                f"Eager AIDP saveAsTable on {self._table!r} failed: "
                f"{e}. Verify the ``aidataplatform`` data source is "
                f"available on the Spark classpath and the "
                f"{namespace!r} catalog is registered in AIDP Master "
                f"Catalogs over an Oracle Autonomous Database. For "
                f"non-AIDP deployments switch system_table_backend "
                f"to 'delta' or 'jdbc'."
            ) from e

    def _ensure_columns(self) -> None:
        """Add any missing columns to an existing table via ALTER TABLE.

        Best-effort: an unsupported DESCRIBE on this catalog or a
        transient connection error falls through to a no-op. Note
        the eager ``format("aidataplatform").saveAsTable(...)`` in
        ``_create_system_table`` has already run for the fresh-
        install case, so a DESCRIBE failure here is rarely "table
        doesn't exist yet" — it's more likely an auth / network /
        catalog-capability issue.
        The DESCRIBE failure is logged at WARNING so a real
        connectivity error is visible in default operator logs.
        The next ``write_results`` will surface the underlying
        problem directly if it isn't a table-not-found case.
        """
        try:
            desc_rows = self._spark.sql(f"DESCRIBE TABLE {self._table}").collect()
            existing = {row[0] for row in desc_rows}
        except Exception as e:
            logger.warning(
                "DESCRIBE TABLE %s skipped (table may not exist yet "
                "on a fresh install, or DESCRIBE may be unsupported "
                "by this catalog, or the connection may be broken): "
                "%s. Column-migration is skipped this run; if the "
                "next write_results also fails the underlying error "
                "is the real cause.",
                self._table, e,
            )
            return

        missing = [
            (col, COLUMN_DEFINITIONS[col][0])
            for col in SYSTEM_TABLE_COLUMNS
            if col not in existing
        ]
        if not missing:
            return

        cols_sql = ", ".join(f"{name} {spark_type}" for name, spark_type in missing)
        try:
            self._spark.sql(f"ALTER TABLE {self._table} ADD COLUMNS ({cols_sql})")
            logger.info("Added %d missing column(s) to %s", len(missing), self._table)
        except Exception as e:
            logger.warning(
                "Could not add missing columns to %s (ALTER TABLE may not be supported): %s",
                self._table, e,
            )

    def _system_table_spark_schema(self) -> Any:
        """Build a Spark ``StructType`` from ``COLUMN_DEFINITIONS``.

        Without an explicit schema, ``createDataFrame`` infers types
        from the first row. A row where an optional field is ``None``
        for the entire batch (typical for ``notification_*`` columns
        on collection-only writes) raises ``cannot determine type
        for None`` from the inference path. Worse, when inference
        does succeed, fresh tables created by the first
        ``saveAsTable`` carry STRING for fields that
        ``COLUMN_DEFINITIONS`` declares TIMESTAMP — drift from the
        documented system-table contract.

        Passing this StructType explicitly fixes both: every row's
        type is fixed, every fresh table inherits the declared
        shape, and the schema source of truth stays
        ``COLUMN_DEFINITIONS``. ISO-format timestamp strings coerce
        to TIMESTAMP at write time the same way Spark already does
        for ``DeltaStorage`` against pre-typed tables.
        """
        from pyspark.sql.types import (
            DoubleType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        type_map = {
            "STRING": StringType(),
            "DOUBLE": DoubleType(),
            "TIMESTAMP": TimestampType(),
        }
        fields = []
        for col in SYSTEM_TABLE_COLUMNS:
            generic = COLUMN_DEFINITIONS[col][0]
            spark_type = type_map.get(generic, StringType())
            fields.append(StructField(col, spark_type, nullable=True))
        return StructType(fields)

    def write_results(self, rows: list[dict[str, Any]]) -> None:
        """Append rows to the system table via DataFrame writer's
        ``insertInto``.

        Two important shape choices, both confirmed against AIDP's
        documented append pattern:

        1. ``insertInto`` over ``saveAsTable``. The eager
           ``format("aidataplatform").saveAsTable(...)`` in
           ``initialize()`` guarantees the target table exists,
           so no auto-create is needed at write time. Using
           ``insertInto`` is
           schema-strict (rejects column-shape drift at write time
           — the right behaviour for the system table; silent
           column addition / mismatch would corrupt history) and
           doesn't update table metadata, so insert-only credentials
           work.

        2. ``.options(**_SYSTEM_TABLE_WRITE_OPTIONS)`` carries
           ``skip.oos.staging=true`` on every write. AIDP / Oracle
           23ai catalogs honour this option to bypass the
           object-storage staging hop for in-database writes;
           non-AIDP catalogs silently ignore unknown writer options,
           so this is safe regardless of the underlying catalog.

        ``insertInto`` is positional, not name-based — the row's
        column order must match the table's. Both the CREATE DDL
        and this DataFrame are built from ``SYSTEM_TABLE_COLUMNS``
        order via ``_system_table_spark_schema()``, so positional
        alignment is a side-effect of the single source of truth.

        An explicit ``StructType`` (built from ``COLUMN_DEFINITIONS``)
        is passed to ``createDataFrame`` so that all-None optional
        columns don't crash schema inference.
        """
        if not rows:
            return
        from datetime import datetime

        from pyspark.sql import Row

        # Columns declared TIMESTAMP need real datetimes (not ISO
        # strings) when paired with a TimestampType StructField, so
        # parse during the cleaning pass.
        ts_cols = {
            col for col in SYSTEM_TABLE_COLUMNS
            if COLUMN_DEFINITIONS[col][0] == "TIMESTAMP"
        }

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
                elif col in ts_cols and isinstance(v, str):
                    # System-table infrastructure — a malformed
                    # timestamp is a producer / data-contract bug.
                    # Coercing to NULL would silently corrupt
                    # history (drift / forecast lookbacks key on
                    # ``run_timestamp`` / ``partition_ts``); raise
                    # so the bad batch fails loud instead.
                    try:
                        cleaned[col] = datetime.fromisoformat(v)
                    except ValueError as e:
                        raise ValueError(
                            f"system-table column {col!r} got "
                            f"non-ISO timestamp {v!r}: {e}. The "
                            "engine writes datetime.isoformat() values "
                            "for these fields — a non-ISO value "
                            "indicates a producer bug. Inspect the "
                            "calling collector / validator."
                        ) from e
                elif col in ts_cols and isinstance(v, datetime):
                    cleaned[col] = v
                else:
                    cleaned[col] = str(v) if v is not None else None
            spark_rows.append(Row(**cleaned))

        schema = self._system_table_spark_schema()
        df = self._spark.createDataFrame(spark_rows, schema=schema)
        logger.debug("Writing %d rows to system table", len(rows))
        (
            df.write.mode("append")
            .options(**_SYSTEM_TABLE_WRITE_OPTIONS)
            .insertInto(self._table)
        )

    def read_metric_history(
        self,
        table_name: str,
        metric_name: str,
        limit: int = 90,
        step: str | None = None,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read past values for a metric, filtered to a segment.

        H2-aware: dedup by ``partition_ts`` first; ``is_active`` and
        ``metric_value IS NOT NULL`` filters apply AFTER the dedup so
        a tombstone in a newer run-time bucket hides the active row in
        any earlier bucket.
        """
        from qualifire.core.duration import parse_step_seconds

        step_seconds = parse_step_seconds(step)
        table_name_sql = table_name.replace("'", "''")
        metric_name_sql = metric_name.replace("'", "''")
        dim_predicate = spark_dim_predicate("dimension_value", dimension_value)
        if step_seconds is None:
            sql = f"""
                WITH dedup AS (
                    SELECT metric_name, metric_value, run_timestamp, validation_status,
                           record_type, collector_name, dimension_value, partition_ts,
                           is_active,
                           ROW_NUMBER() OVER (
                               PARTITION BY partition_ts
                               ORDER BY
                                   run_timestamp DESC,
                                   CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END
                           ) AS _rn
                    FROM {self._table}
                    WHERE table_name = '{table_name_sql}'
                      AND metric_name = '{metric_name_sql}'
                      AND {dim_predicate}
                      AND run_timestamp IS NOT NULL
                )
                SELECT metric_name, metric_value, run_timestamp, validation_status,
                       record_type, collector_name, dimension_value, partition_ts
                FROM dedup
                WHERE _rn = 1
                  AND is_active = 'true'
                  AND metric_value IS NOT NULL
                ORDER BY run_timestamp DESC
                LIMIT {int(limit)}
            """
            rows = self._spark.sql(sql).collect()
            return [r.asDict() for r in rows]

        sql = f"""
            WITH dedup AS (
                SELECT metric_name, metric_value, run_timestamp, validation_status,
                       record_type, collector_name, dimension_value, partition_ts,
                       is_active,
                       ROW_NUMBER() OVER (
                           PARTITION BY partition_ts
                           ORDER BY
                               run_timestamp DESC,
                               CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END
                       ) AS _rn_part
                FROM {self._table}
                WHERE table_name = '{table_name_sql}'
                  AND metric_name = '{metric_name_sql}'
                  AND {dim_predicate}
                  AND run_timestamp IS NOT NULL
                  AND unix_timestamp(run_timestamp) IS NOT NULL
            ),
            survivors AS (
                SELECT metric_name, metric_value, run_timestamp, validation_status,
                       record_type, collector_name, dimension_value, partition_ts
                FROM dedup
                WHERE _rn_part = 1
                  AND is_active = 'true'
                  AND metric_value IS NOT NULL
            ),
            bucketed AS (
                SELECT metric_name, metric_value, run_timestamp, validation_status,
                       record_type, collector_name, dimension_value, partition_ts,
                       ROW_NUMBER() OVER (
                           PARTITION BY CAST(unix_timestamp(run_timestamp) AS BIGINT) DIV {int(step_seconds)}
                           ORDER BY
                               run_timestamp DESC,
                               CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END
                       ) AS _rn
                FROM survivors
            )
            SELECT metric_name, metric_value, run_timestamp, validation_status,
                   record_type, collector_name, dimension_value, partition_ts
            FROM bucketed
            WHERE _rn = 1
            ORDER BY run_timestamp DESC
            LIMIT {int(limit)}
        """
        rows = self._spark.sql(sql).collect()
        return [r.asDict() for r in rows]

    def read_metric_history_by_partition(
        self,
        table_name: str,
        metric_name: str,
        anchor_ts: Any,
        count: int,
        step: str,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Partition-anchored history read. H2-aware."""
        from qualifire.core.duration import partition_lookback_anchors

        if count <= 0:
            return []
        anchors = partition_lookback_anchors(anchor_ts, count, step)
        # ANSI ``TIMESTAMP '...'`` literal with a *space* separator,
        # not the ISO ``T`` form. ADW / Oracle external catalogs
        # silently fail to match a TIMESTAMP column against a bare
        # string with the ``T`` separator (NLS_TIMESTAMP_FORMAT
        # default is ``'YYYY-MM-DD HH24:MI:SS.FF'``); the IN clause
        # would return zero rows and every drift / forecast /
        # anomaly run would cold-start in perpetuity. The sibling
        # ``read_collection_metric_at_partition`` already uses this
        # ANSI form — keeping the two read paths consistent.
        anchor_strs = [
            a.isoformat(sep=" ").replace("'", "''") for a in anchors
        ]
        in_list = ", ".join(f"TIMESTAMP '{s}'" for s in anchor_strs)
        table_name_sql = table_name.replace("'", "''")
        metric_name_sql = metric_name.replace("'", "''")
        dim_predicate = spark_dim_predicate("dimension_value", dimension_value)
        sql = f"""
            WITH ranked AS (
                SELECT metric_name, metric_value, run_timestamp, validation_status,
                       record_type, collector_name, dimension_value, partition_ts,
                       collected_at, is_active,
                       ROW_NUMBER() OVER (
                           PARTITION BY partition_ts
                           ORDER BY
                               run_timestamp DESC,
                               CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END,
                               COALESCE(collected_at, run_timestamp) DESC
                       ) AS _rn
                FROM {self._table}
                WHERE table_name = '{table_name_sql}'
                  AND metric_name = '{metric_name_sql}'
                  AND {dim_predicate}
                  AND partition_ts IN ({in_list})
            )
            SELECT metric_name, metric_value, run_timestamp, validation_status,
                   record_type, collector_name, dimension_value, partition_ts
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND metric_value IS NOT NULL
            ORDER BY partition_ts DESC
        """
        rows = self._spark.sql(sql).collect()
        return [r.asDict() for r in rows]

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
        dim_predicate = spark_dim_predicate("dimension_value", dimension_value)
        sql = f"""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY table_name, metric_name, dimension_value, partition_ts
                           ORDER BY run_timestamp DESC, collected_at DESC
                       ) AS _rn
                FROM {self._table}
                WHERE table_name = '{t_sql}'
                  AND metric_name = '{m_sql}'
                  AND record_type = 'collection'
                  AND partition_ts = TIMESTAMP '{pt_sql}'
                  AND {dim_predicate}
            )
            SELECT *
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND metric_value IS NOT NULL
            LIMIT 1
        """
        rows = self._spark.sql(sql).collect()
        return rows[0].asDict() if rows else None

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
        # Codex impl-review R1 MAJOR: tiebreaker on ``collected_at``.
        sql = f"""
            WITH ranked AS (
                SELECT dimension_value, run_timestamp, collected_at,
                       is_active, metric_value,
                       ROW_NUMBER() OVER (
                           PARTITION BY dimension_value
                           ORDER BY run_timestamp DESC,
                                    COALESCE(collected_at, run_timestamp) DESC
                       ) AS _rn
                FROM {self._table}
                WHERE record_type = 'collection'
                  AND table_name = '{t_sql}'
                  AND metric_name = '{m_sql}'
                  AND partition_ts = TIMESTAMP '{pt_sql}'
            )
            SELECT DISTINCT dimension_value
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND metric_value IS NOT NULL
            ORDER BY dimension_value
        """
        rows = self._spark.sql(sql).collect()
        return [r["dimension_value"] for r in rows]

    def read_validations_at_partition(
        self,
        *,
        dataset_name: str,
        validation_name_prefix: str,
        partition_ts: Any,
    ) -> list[dict[str, Any]]:
        """All active validation rows at a partition for one dataset+
        validation prefix. Backs ``skip_revalidation``.
        """
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
        sql = f"""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY validation_name, metric_name,
                                        dimension_value
                           ORDER BY run_timestamp DESC
                       ) AS _rn
                FROM {self._table}
                WHERE record_type = 'validation'
                  AND dataset_name = '{ds_sql}'
                  AND (validation_name = '{prefix_sql}'
                       OR validation_name LIKE '{like_pattern}'
                           ESCAPE '\\')
                  AND partition_ts = TIMESTAMP '{pt_sql}'
            )
            SELECT *
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
            ORDER BY validation_name, metric_name, dimension_value
        """
        rows = self._spark.sql(sql).collect()
        return [r.asDict() for r in rows]

    def read_latest_run(self, dataset_name: str) -> dict[str, Any] | None:
        """H8: tombstone latest → None."""
        ds_sql = dataset_name.replace("'", "''")
        sql = f"""
            SELECT *
            FROM {self._table}
            WHERE dataset_name = '{ds_sql}'
            ORDER BY run_timestamp DESC
            LIMIT 1
        """
        rows = self._spark.sql(sql).collect()
        if not rows:
            return None
        d = rows[0].asDict()
        if str(d.get("is_active") or "true").lower() != "true":
            return None
        return d

    def read_validation_history(
        self,
        dataset_name: str,
        validation_name: str,
        limit: int = 10,
        metric_name: Any = _UNSET,
        dimension_value: Any = _UNSET,
    ) -> list[dict[str, Any]]:
        """H2-aware validation history read."""
        ds_sql = dataset_name.replace("'", "''")
        v_sql = validation_name.replace("'", "''")
        clauses = [
            f"dataset_name = '{ds_sql}'",
            f"validation_name = '{v_sql}'",
        ]
        if metric_name is not _UNSET:
            if metric_name is None:
                clauses.append("metric_name IS NULL")
            else:
                m_sql = str(metric_name).replace("'", "''")
                clauses.append(f"metric_name = '{m_sql}'")
        if dimension_value is not _UNSET:
            clauses.append(spark_dim_predicate("dimension_value", dimension_value))
        sql = f"""
            WITH ranked AS (
                SELECT validation_name, validation_type, validation_status,
                       validation_message, run_timestamp,
                       metric_name, dimension_value, is_active,
                       ROW_NUMBER() OVER (
                           PARTITION BY metric_name, dimension_value, partition_ts
                           ORDER BY run_timestamp DESC
                       ) AS _rn
                FROM {self._table}
                WHERE {' AND '.join(clauses)}
            )
            SELECT validation_name, validation_type, validation_status,
                   validation_message, run_timestamp,
                   metric_name, dimension_value
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
            ORDER BY run_timestamp DESC
            LIMIT {int(limit)}
        """
        rows = self._spark.sql(sql).collect()
        return [r.asDict() for r in rows]

    def read_validation_history_bulk(
        self,
        keys: list[Any],
        limit: int = 1,
    ) -> dict[Any, list[dict[str, Any]]]:
        """Bulk read with multi-row support (Item 3,
        backfill-followups-and-polish).

        Two-stage CTE: per ``(input_idx, partition_ts)`` pick latest
        version, drop tombstones, re-rank per ``input_idx``, apply
        ``LIMIT``. Aligned with ``read_validation_history``
        partition-aware semantics. UUID-salted temp-view name to
        avoid cross-worker collisions under parallel backfill.
        """
        import uuid
        if limit < 1:
            raise ValueError(
                f"read_validation_history_bulk: limit must be >= 1; "
                f"got {limit}"
            )
        if not keys:
            return {}
        spark = self._spark
        rows = [
            (
                idx,
                k.dataset_name,
                k.validation_name,
                k.metric_name,
                k.dimension_value,
            )
            for idx, k in enumerate(keys)
        ]
        keys_view = f"_qf_bulk_keys_{uuid.uuid4().hex}"
        try:
            schema = "_idx LONG, _ds STRING, _vn STRING, _mn STRING, _dv STRING"
            keys_df = spark.createDataFrame(rows, schema=schema)
            keys_df.createOrReplaceTempView(keys_view)
            sql = f"""
                WITH per_partition AS (
                    SELECT
                        k._idx AS _idx,
                        h.validation_name, h.validation_type, h.validation_status,
                        h.validation_message, h.run_timestamp,
                        h.metric_name, h.dimension_value, h.is_active,
                        h.partition_ts, h.run_id, h.validated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY k._idx, h.partition_ts
                            ORDER BY h.run_timestamp DESC,
                                     h.validated_at DESC,
                                     h.run_id DESC
                        ) AS _pn
                    FROM {keys_view} k
                    JOIN {self._table} h
                      ON h.dataset_name = k._ds
                     AND h.validation_name = k._vn
                     AND (h.metric_name <=> k._mn)
                     AND (h.dimension_value <=> k._dv)
                     AND h.record_type = 'validation'
                ),
                active_per_partition AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY _idx
                            ORDER BY run_timestamp DESC,
                                     validated_at DESC,
                                     run_id DESC
                        ) AS _kn
                    FROM per_partition
                    WHERE _pn = 1
                      AND is_active = 'true'
                )
                SELECT _idx, validation_name, validation_type,
                       validation_status, validation_message,
                       run_timestamp, metric_name, dimension_value
                FROM active_per_partition
                WHERE _kn <= {int(limit)}
                ORDER BY _idx, _kn
            """
            collected = spark.sql(sql).collect()
        finally:
            try:
                spark.sql(f"DROP VIEW IF EXISTS `{keys_view}`")
            except Exception:
                pass
        result: dict[Any, list[dict[str, Any]]] = {k: [] for k in keys}
        for row in collected:
            d = row.asDict()
            idx = d.pop("_idx")
            result[keys[idx]].append(d)
        return result

    def read_health_data(
        self, days: int = 30, *, include_collection: bool = False,
    ) -> list[dict[str, Any]]:
        """H2-aware: dedup per natural key; ``is_active`` filter post-dedup."""
        record_type_clause = (
            "record_type IN ('validation', 'collection')"
            if include_collection
            else "record_type = 'validation'"
        )
        sql = f"""
            WITH ranked AS (
                SELECT run_id, owner, bu, dataset_name, table_name,
                       validation_type, validation_status,
                       run_timestamp, validation_name,
                       metric_name, metric_value,
                       partition_ts, dimension_value,
                       expected_value, actual_value_text,
                       validation_message, record_type, is_active,
                       dataset_description, validation_description, details_json,
                       ROW_NUMBER() OVER (
                           PARTITION BY dataset_name, validation_name,
                                        metric_name, partition_ts, dimension_value
                           ORDER BY
                               run_timestamp DESC,
                               CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END
                       ) AS _rn
                FROM {self._table}
                WHERE run_timestamp >= date_sub(current_timestamp(), {days})
            )
            SELECT run_id, owner, bu, dataset_name, table_name,
                   validation_type, validation_status,
                   run_timestamp, validation_name,
                   metric_name, metric_value,
                   partition_ts, dimension_value,
                   expected_value, actual_value_text,
                   validation_message, record_type,
                   dataset_description, validation_description, details_json
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND {record_type_clause}
            ORDER BY run_timestamp DESC
        """
        rows = self._spark.sql(sql).collect()
        return [r.asDict() for r in rows]
