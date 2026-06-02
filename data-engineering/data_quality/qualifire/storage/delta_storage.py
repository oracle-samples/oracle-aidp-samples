"""Delta Lake system table storage with programmatic compact()."""

from __future__ import annotations

import json
import logging
from typing import Any

# Sentinel for "argument not supplied" vs. `None` (match SQL NULL).
_UNSET = object()

from qualifire.storage._predicates import spark_dim_predicate

from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS

logger = logging.getLogger(__name__)


class DeltaStorage:
    """System table stored as a Delta table.

    Supports append-only writes plus a programmatic compact() method
    to optimize small file accumulation.

    Args:
        spark: SparkSession instance.
        table: Fully-qualified table name.
    """

    def __init__(self, spark: Any, table: str):
        self._spark = spark
        self._table = table

    def initialize(self) -> None:
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                run_id STRING,
                run_timestamp TIMESTAMP,
                owner STRING,
                bu STRING,
                dataset_name STRING,
                table_name STRING,
                metric_name STRING,
                metric_value DOUBLE,
                collection_type STRING,
                validation_name STRING,
                validation_type STRING,
                validation_status STRING,
                validation_message STRING,
                notification_channel STRING,
                notification_status STRING,
                details_json STRING,
                record_type STRING,
                collector_name STRING,
                dimension_value STRING,
                collected_at TIMESTAMP,
                validated_at TIMESTAMP,
                notified_at TIMESTAMP,
                partition_ts TIMESTAMP,
                expected_value STRING,
                actual_value_text STRING,
                dataset_description STRING,
                validation_description STRING
            ) USING DELTA
        """
        logger.info("Initializing Delta system table: %s", self._table)
        self._spark.sql(ddl)
        self._ensure_columns()

    def _ensure_columns(self) -> None:
        """Add any missing columns to an existing Delta table."""
        try:
            desc_rows = self._spark.sql(f"DESCRIBE TABLE {self._table}").collect()
            existing = {row[0] for row in desc_rows}
        except Exception:
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
            logger.warning("Could not add missing columns to %s: %s", self._table, e)

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

        df = self._spark.createDataFrame(spark_rows)
        df.write.mode("append").saveAsTable(self._table)

    def read_metric_history(
        self,
        table_name: str,
        metric_name: str,
        limit: int = 90,
        step: str | None = None,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read past values for a metric, filtered to a segment.

        H2-aware: dedup by ``partition_ts`` first (latest
        ``run_timestamp`` wins, validation > collection on tie); then
        filter ``is_active = 'true'`` AFTER the dedup so a tombstone
        in a newer run-time bucket hides the active row in any
        earlier bucket. Mirrors :meth:`SQLiteStorage.read_metric_history`.
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

        # Bucketed read — two-stage. Stage 1: per-partition_ts dedup
        # (soft-delete tombstones win the inner ROW_NUMBER and get
        # filtered out before bucketing). Stage 2: bucket-of-run-time
        # dedup over the survivors.
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
        """Partition-anchored history read (Delta/Spark SQL).

        H2-aware: filter ``metric_value IS NOT NULL`` and
        ``is_active = 'true'`` AFTER the
        ``ROW_NUMBER`` step so a tombstone wins the per-partition
        rank and the consumer sees no row for the deactivated key.
        """
        from qualifire.core.duration import partition_lookback_anchors

        if count <= 0:
            return []
        anchors = partition_lookback_anchors(anchor_ts, count, step)
        anchor_strs = [a.isoformat().replace("'", "''") for a in anchors]
        in_list = ", ".join(f"'{s}'" for s in anchor_strs)
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

        Filter chain: ``record_type='collection'`` ∧ exact
        ``partition_ts`` ∧ NULL-safe ``dimension_value``,
        ROW_NUMBER ORDER BY ``run_timestamp DESC, collected_at DESC``,
        then ``rn = 1`` ∧ ``is_active = 'true'`` ∧
        ``metric_value IS NOT NULL``. Returns None when no eligible
        row exists. Signature matches the SystemTableStorage Protocol.
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
        # codex impl-review R1 MAJOR: escape SQL LIKE metacharacters
        # (`_`, `%`) so a prefix containing an underscore doesn't
        # cross-match other validation names.
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
        """H8: tombstone latest → None.

        If the row with the newest ``run_timestamp`` for the dataset
        is a tombstone (``is_active='false'``), return None — no
        fallback to the next-most-recent active row.
        """
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
        """H2-aware: dedup per (metric, dimension, partition_ts) first;
        ``is_active`` filter post-dedup."""
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
        partition-aware semantics.

        Notes:
        - Temp-view name uses ``uuid.uuid4().hex`` salt to avoid
          collisions when multiple workers prefetch the same keys
          concurrently (codex R2 HIGH).
        - try/finally drops the view even on worker failure.
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
        # UUID salt avoids cross-worker collisions on
        # ``createOrReplaceTempView`` under parallel backfill.
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
        """H2-aware: dedup per natural key; filter ``is_active`` AFTER
        the rank. Tombstones in newer run-time buckets hide active
        rows in older buckets.
        """
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

    def compact(self, num_files: int = 1) -> None:
        """Compact small files in the Delta table.

        Args:
            num_files: Target number of files after compaction.
        """
        logger.info("Compacting Delta table %s to %d files", self._table, num_files)
        self._spark.sql(
            f"OPTIMIZE {self._table}"
        )
