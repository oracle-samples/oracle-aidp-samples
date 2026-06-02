"""SQLite system table storage for local dev/test."""

from __future__ import annotations

import functools
import json
import logging
import sqlite3
from typing import Any

from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS

logger = logging.getLogger(__name__)


# Sentinel for "argument not supplied" — distinct from `None` (which
# means "match SQL NULL"). Avoids ambiguity in
# `read_validation_history(metric_name=None)`.
_UNSET = object()


def _locked(method):
    """Decorator: serialize calls to a SQLiteStorage method through
    ``self._lock``. Item 1 (backfill-followups-and-polish): under
    ``Qualifire.backfill(parallelism > 1)`` worker threads share
    one storage instance; the underlying connection has
    ``check_same_thread=False`` and we hold the lock for the
    duration of every public read/write to avoid concurrent-access
    corruption. RLock allows cross-method calls without deadlock.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapper


class SQLiteStorage:
    """System table stored in a local SQLite database.

    Useful for development, testing, and single-machine usage.

    Args:
        db_path: Path to SQLite database file. Use ":memory:" for in-memory.
        table: Table name within the database.
    """

    def __init__(self, db_path: str = ":memory:", table: str = "qualifire_history"):
        self._db_path = db_path
        self._table = table
        self._conn: sqlite3.Connection | None = None
        # Item 1 (backfill-followups-and-polish): under
        # ``Qualifire.backfill(parallelism > 1)`` worker threads
        # share this storage instance. SQLite's default Python
        # binding restricts a connection to its creator thread; we
        # opt out via ``check_same_thread=False`` and serialize all
        # method calls behind ``_lock`` so concurrent reads/writes
        # don't corrupt the connection. Production parallelism wins
        # live in Spark-backed backends; SQLite serializes safely.
        import threading
        # RLock so internal method-to-method calls don't deadlock
        # (e.g. read_validation_history_bulk → read_validation_history
        # in the JDBC fallback path mirror; SQLite has no fallback
        # but RLock is the safer default).
        self._lock = threading.RLock()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self._db_path, check_same_thread=False,
            )
            self._conn.row_factory = sqlite3.Row
        return self._conn


    @_locked
    def initialize(self) -> None:
        conn = self._get_conn()
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                run_id TEXT,
                run_timestamp TEXT,
                owner TEXT,
                bu TEXT,
                dataset_name TEXT,
                table_name TEXT,
                metric_name TEXT,
                metric_value REAL,
                collection_type TEXT,
                validation_name TEXT,
                validation_type TEXT,
                validation_status TEXT,
                validation_message TEXT,
                notification_channel TEXT,
                notification_status TEXT,
                details_json TEXT,
                record_type TEXT,
                collector_name TEXT,
                dimension_value TEXT,
                collected_at TEXT,
                validated_at TEXT,
                notified_at TEXT,
                partition_ts TEXT,
                expected_value TEXT,
                actual_value_text TEXT,
                dataset_description TEXT,
                validation_description TEXT,
                is_active TEXT
            )
        """
        conn.execute(ddl)
        conn.commit()
        self._ensure_columns()

    def _ensure_columns(self) -> None:
        """Add any missing columns to an existing table (schema migration)."""
        conn = self._get_conn()
        cursor = conn.execute(f"PRAGMA table_info({self._table})")
        existing = {row[1] for row in cursor.fetchall()}

        for col_name in SYSTEM_TABLE_COLUMNS:
            if col_name not in existing:
                _, sqlite_type = COLUMN_DEFINITIONS[col_name]
                logger.info("Adding missing column '%s' to %s", col_name, self._table)
                conn.execute(
                    f"ALTER TABLE {self._table} ADD COLUMN {col_name} {sqlite_type}"
                )
        conn.commit()

    @_locked
    def write_results(self, rows: list[dict[str, Any]]) -> None:
        """Atomic bulk insert: all rows commit together or none.

        Uses ``executemany`` (single SQLite call, all-or-nothing) and
        an explicit rollback on exception so a partially-prepared
        transaction never bleeds into the next call.
        """
        if not rows:
            return
        conn = self._get_conn()
        placeholders = ", ".join("?" for _ in SYSTEM_TABLE_COLUMNS)
        cols = ", ".join(SYSTEM_TABLE_COLUMNS)
        sql = f"INSERT INTO {self._table} ({cols}) VALUES ({placeholders})"
        all_values: list[list[Any]] = []
        for row in rows:
            values = []
            for col in SYSTEM_TABLE_COLUMNS:
                v = row.get(col)
                if col == "is_active" and v is None:
                    # Writers may omit ``is_active`` for ordinary
                    # (non-tombstone) rows; default to 'true' so the
                    # strict read-time filter ``is_active = 'true'``
                    # never has to deal with NULL.
                    v = "true"
                if col == "details_json" and isinstance(v, dict):
                    values.append(json.dumps(v))
                elif col == "expected_value" and isinstance(v, dict):
                    values.append(json.dumps(v))
                elif col == "metric_value" and v is not None:
                    values.append(float(v))
                else:
                    values.append(str(v) if v is not None else None)
            all_values.append(values)
        try:
            conn.executemany(sql, all_values)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    @_locked
    def read_metric_history(
        self,
        table_name: str,
        metric_name: str,
        limit: int = 90,
        step: str | None = None,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read past values for a metric, filtered to a specific segment.

        ``dimension_value`` filters per-segment. ``None`` matches
        rows persisted with SQL NULL (non-dimensioned datasets);
        any other value matches that exact dimension. Equality is
        NULL-safe via SQLite's ``IS`` operator.

        When ``step`` is given, rows are bucketed into fixed-width
        intervals of that duration (relative to the UNIX epoch) and
        at most one row is returned per bucket — the collection row
        if present, otherwise the newest validation row.

        Rows with a null ``run_timestamp`` are always excluded.
        """
        # H2-compliant rewrite (plan: metrics-backfill-and-soft-delete S3).
        # Two-stage CTE: dedup per (partition_ts, dimension) for soft-delete
        # first; then optional bucket dedup. Filtering metric_value or
        # is_active before the inner ROW_NUMBER would let a stale active
        # row surface underneath a newer NULL tombstone.
        from qualifire.core.duration import parse_step_seconds

        conn = self._get_conn()
        step_seconds = parse_step_seconds(step)
        if step_seconds is None:
            # Dedupe rule (consistent with the dashboard JS): pick the
            # row with the LATEST ``run_timestamp`` per natural key.
            # When two rows tie on run_timestamp (the typical "validator
            # emits vrow + crow at the same instant" pattern),
            # validation wins so the surviving row carries the verdict
            # rather than its own paired collection row. A recollection
            # at T2 > T1 thus correctly shadows an earlier verdict at
            # T1 — the underlying value was rewritten and the verdict
            # is now stale.
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
                    WHERE table_name = ?
                      AND metric_name = ?
                      AND dimension_value IS ?
                      AND run_timestamp IS NOT NULL
                )
                SELECT metric_name, metric_value, run_timestamp, validation_status,
                       record_type, collector_name, dimension_value, partition_ts
                FROM dedup
                WHERE _rn = 1
                  AND is_active = 'true'
                  AND metric_value IS NOT NULL
                ORDER BY run_timestamp DESC
                LIMIT ?
            """
            cursor = conn.execute(
                sql, (table_name, metric_name, dimension_value, limit)
            )
            return [dict(r) for r in cursor.fetchall()]

        # Bucketed read — two-stage. Stage 1: per-partition_ts dedup
        # (latest run_timestamp wins, validation tie-break, soft-delete
        # tombstones win the inner ROW_NUMBER and get filtered out
        # downstream). Stage 2: bucket-of-run-time dedup over the
        # survivors using the same dedupe rule.
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
                WHERE table_name = ?
                  AND metric_name = ?
                  AND dimension_value IS ?
                  AND run_timestamp IS NOT NULL
                  AND strftime('%s', run_timestamp) IS NOT NULL
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
                           PARTITION BY CAST(strftime('%s', run_timestamp) AS INTEGER) / ?
                           ORDER BY
                               run_timestamp DESC,
                               CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END
                       ) AS _rn
                FROM survivors
            )
            SELECT metric_name, metric_value, run_timestamp, validation_status,
                   record_type, collector_name, dimension_value
            FROM bucketed
            WHERE _rn = 1
            ORDER BY run_timestamp DESC
            LIMIT ?
        """
        cursor = conn.execute(
            sql, (table_name, metric_name, dimension_value, step_seconds, limit)
        )
        return [dict(r) for r in cursor.fetchall()]

    @_locked
    def read_metric_history_by_partition(
        self,
        table_name: str,
        metric_name: str,
        anchor_ts: Any,
        count: int,
        step: str,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Partition-anchored history read. See
        :class:`qualifire.storage.base.SystemTableStorage` for semantics.

        ``dimension_value=None`` matches rows persisted with SQL NULL.
        """
        from qualifire.core.duration import partition_lookback_anchors

        if count <= 0:
            return []
        anchors = partition_lookback_anchors(anchor_ts, count, step)
        # Persisted as ISO strings; lexicographic == chronological for ISO-8601.
        anchor_strs = [a.isoformat() for a in anchors]
        placeholders = ", ".join("?" for _ in anchor_strs)

        # The identity for a metric cell is
        # ``(partition_ts, dimension_value, metric_name)``. When the
        # same identity has multiple rows (a partition rerun, or a
        # collection row + later validation row), the contract is
        # "latest write wins" — pick the most recent ``collected_at``
        # for collection rows; fall back to ``run_timestamp`` if
        # ``collected_at`` is null. Collection rows take precedence
        # over validation rows for the metric_value column.
        #
        # ROW_NUMBER inside a CTE picks the survivor deterministically
        # — the previous Python-side ``seen.add(partition_ts)`` dedupe
        # was non-deterministic when two rows shared a partition_ts
        # AND the same record_type, because ORDER BY couldn't
        # discriminate within the tie.
        # H2-compliant: filter metric_value IS NOT NULL and is_active
        # AFTER the row-number step so a NULL tombstone wins ROW_NUMBER
        # and the consumer sees no row for the deactivated key.
        #
        # Dedupe rule (consistent with the dashboard JS): latest
        # ``run_timestamp`` wins; on a tie, validation wins so the
        # surviving row carries the verdict rather than its own
        # paired collection row. ``collected_at`` is a final
        # tie-breaker for the rare same-record_type, same-run_timestamp
        # case (e.g. two collection rows at the same instant).
        conn = self._get_conn()
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
                WHERE table_name = ?
                  AND metric_name = ?
                  AND dimension_value IS ?
                  AND partition_ts IN ({placeholders})
            )
            SELECT metric_name, metric_value, run_timestamp, validation_status,
                   record_type, collector_name, dimension_value, partition_ts
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND metric_value IS NOT NULL
            ORDER BY partition_ts DESC
        """
        cursor = conn.execute(
            sql, (table_name, metric_name, dimension_value, *anchor_strs)
        )
        return [dict(r) for r in cursor.fetchall()]

    @_locked
    def read_latest_run(self, dataset_name: str) -> dict[str, Any] | None:
        # Plan H2 / H8: latest-run row honours soft-delete. If the
        # newest row for the dataset is a tombstone, return None.
        conn = self._get_conn()
        sql = f"""
            SELECT *
            FROM {self._table}
            WHERE dataset_name = ?
            ORDER BY run_timestamp DESC
            LIMIT 1
        """
        cursor = conn.execute(sql, (dataset_name,))
        row = cursor.fetchone()
        if row is None:
            return None
        d = dict(row)
        if str(d.get("is_active") or "true").lower() != "true":
            return None
        return d

    @_locked
    def read_validation_history(
        self,
        dataset_name: str,
        validation_name: str,
        limit: int = 10,
        metric_name: Any = _UNSET,
        dimension_value: Any = _UNSET,
    ) -> list[dict[str, Any]]:
        """Read past validation rows, optionally filtered by metric+dim.

        Two-arg call (no `metric_name`/`dimension_value`) preserves
        the historical wildcard semantics. Pass `metric_name=` /
        `dimension_value=` to filter strictly. NULL handling:
          - `metric_name`: explicit ``IS NULL`` when caller passes
            ``None`` AS A KEYWORD (not as the unset default — see
            sentinel below).
          - `dimension_value`: NULL-safe equality via SQLite's ``IS``.
        """
        conn = self._get_conn()
        clauses = ["dataset_name = ?", "validation_name = ?"]
        params: list[Any] = [dataset_name, validation_name]

        # Sentinel-aware filtering. _UNSET => skip predicate;
        # otherwise NULL-safe equality (``IS`` on SQLite).
        if metric_name is not _UNSET:
            clauses.append("metric_name IS ?")
            params.append(metric_name)
        if dimension_value is not _UNSET:
            clauses.append("dimension_value IS ?")
            params.append(dimension_value)
        params.append(limit)

        # H2: dedup per (metric, dimension, partition_ts) first, then
        # filter is_active AFTER ROW_NUMBER so a tombstone hides
        # historical rows correctly.
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
            LIMIT ?
        """
        cursor = conn.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]

    @_locked
    def read_validation_history_bulk(
        self,
        keys: list[Any],  # list[ValidationKey]
        limit: int = 1,
    ) -> dict[Any, list[dict[str, Any]]]:
        """Bulk read suppression history for many keys in one query.

        Builds one CTE of input keys and joins against the system
        table with NULL-safe predicates. Returns a dict keyed on the
        input ValidationKey objects so callers iterate without a
        second lookup.
        """
        if not keys:
            return {}
        if limit < 1:
            raise ValueError(
                f"read_validation_history_bulk: limit must be >= 1; "
                f"got {limit}"
            )
        # Item 3 (backfill-followups-and-polish, HS1 carve-out B):
        # multi-row support via a two-stage CTE. Stage 1 picks the
        # latest version per (input_idx, partition_ts) — partition-
        # scoped tombstones hide their partition only. Stage 2
        # re-ranks survivors per input_idx and applies the row cap.
        # Aligns with the singular ``read_validation_history``
        # partition-aware semantics; supersedes the prior partition-
        # blind ``limit=1`` shape.
        conn = self._get_conn()

        # Build a VALUES list as a CTE. Each input key contributes
        # one row of (idx, dataset_name, validation_name, metric_name,
        # dimension_value). idx is used to associate result rows back
        # to the input ValidationKey by position; SQLite has no native
        # row constructor for IN-tuple semantics with NULL safety, so
        # we JOIN against the CTE using explicit IS NULL OR = ?.
        rows_sql_parts = []
        params: list[Any] = []
        for idx, k in enumerate(keys):
            rows_sql_parts.append("(?, ?, ?, ?, ?)")
            params.extend([
                idx,
                k.dataset_name,
                k.validation_name,
                k.metric_name,
                k.dimension_value,
            ])
        values_clause = ", ".join(rows_sql_parts)

        # Two-stage CTE:
        #   per_partition: latest version per (input_idx, partition_ts)
        #   active_per_partition: drop tombstones, re-rank per input_idx
        sql = f"""
            WITH input_keys(_idx, _ds, _vn, _mn, _dv) AS (
                VALUES {values_clause}
            ),
            per_partition AS (
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
                FROM input_keys k
                JOIN {self._table} h
                  ON h.dataset_name = k._ds
                 AND h.validation_name = k._vn
                 AND h.metric_name IS k._mn
                 AND h.dimension_value IS k._dv
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
            SELECT * FROM active_per_partition
            WHERE _kn <= ?
            ORDER BY _idx, _kn
        """
        cursor = conn.execute(sql, params + [limit])
        result: dict[Any, list[dict[str, Any]]] = {k: [] for k in keys}
        for row in cursor.fetchall():
            d = dict(row)
            idx = d.pop("_idx")
            d.pop("_pn", None)
            d.pop("_kn", None)
            result[keys[idx]].append(d)
        return result

    @_locked
    def read_health_data(
        self, days: int = 30, *, include_collection: bool = False,
    ) -> list[dict[str, Any]]:
        """Return validation rows from the last ``days`` days for dashboards.

        Selects the columns dashboards typically need (basic identity +
        partition_ts + numeric metric + descriptions). Extending past the
        original 5-column shape is intentional — the static `health_report`
        ignores extra keys, while the interactive dashboard and chart
        notebooks consume them.

        Args:
            days: Time window in days (filters on ``run_timestamp``).
            include_collection: When True, also returns ``record_type =
                'collection'`` rows. Collection rows carry the metric
                values that history-backed validators (drift / forecast /
                shape / pattern) read for past partitions but produce no
                validation record themselves. Including them lets the
                interactive dashboard plot a metric's full per-partition
                history under its today-only validation entry, instead
                of showing a single point. Default False preserves the
                long-standing validation-only contract for the static
                health report.
        """
        # H2: dedup per natural key first, then is_active filter, then
        # the consumer's record_type filter — so a tombstone wins
        # ROW_NUMBER and the consumer sees a clean exclusion.
        conn = self._get_conn()
        record_type_clause = (
            "record_type IN ('validation', 'collection')"
            if include_collection
            else "record_type = 'validation'"
        )
        # ``details_json`` is projected so dashboard / health-report
        # consumers can detect rows tagged
        # ``qualifire_internal_failure=True`` (Phase 2 of
        # external-catalog-system-table-hardening). Without it the
        # health reporter's ``internal_error_count`` always sees
        # None and falls back to "all errors are data findings",
        # defeating the suppression-distinction contract.
        sql = f"""
            WITH ranked AS (
                SELECT run_id, owner, bu, dataset_name, table_name,
                       validation_type, validation_status,
                       run_timestamp, validation_name,
                       metric_name, metric_value,
                       partition_ts, dimension_value,
                       expected_value, actual_value_text,
                       validation_message,
                       dataset_description, validation_description,
                       record_type, is_active, details_json,
                       ROW_NUMBER() OVER (
                           PARTITION BY dataset_name, validation_name,
                                        metric_name, partition_ts, dimension_value
                           ORDER BY run_timestamp DESC
                       ) AS _rn
                FROM {self._table}
                WHERE run_timestamp >= datetime('now', ?)
            )
            SELECT run_id, owner, bu, dataset_name, table_name,
                   validation_type, validation_status,
                   run_timestamp, validation_name,
                   metric_name, metric_value,
                   partition_ts, dimension_value,
                   expected_value, actual_value_text,
                   validation_message, record_type,
                   dataset_description, validation_description,
                   details_json
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND {record_type_clause}
            ORDER BY run_timestamp DESC
        """
        cursor = conn.execute(sql, (f"-{days} days",))
        return [dict(r) for r in cursor.fetchall()]

    @_locked
    def read_collection_metric_at_partition(
        self,
        table_name: str,
        metric_name: str,
        anchor_ts: Any,
        dimension_value: str | None = None,
    ) -> dict[str, Any] | None:
        """Latest active collection cell for one (table, metric, partition, dim).

        Plan H6: backs ``skip_if_present`` and ``skip_recollection`` cache
        hits. Filter chain: ``record_type='collection'`` ∧ exact
        partition_ts ∧ NULL-safe dim, ROW_NUMBER ORDER BY
        run_timestamp DESC, collected_at DESC, then ``rn = 1``,
        ``is_active``, ``metric_value IS NOT NULL``.
        """
        from datetime import datetime as _dt

        conn = self._get_conn()
        anchor_str = (
            anchor_ts.isoformat() if isinstance(anchor_ts, _dt) else str(anchor_ts)
        )
        sql = f"""
            WITH ranked AS (
                SELECT metric_name, metric_value, run_timestamp, collected_at,
                       partition_ts, dimension_value, collector_name,
                       expected_value, actual_value_text,
                       record_type, is_active,
                       ROW_NUMBER() OVER (
                           PARTITION BY table_name, metric_name, partition_ts,
                                        dimension_value
                           ORDER BY run_timestamp DESC,
                                    COALESCE(collected_at, run_timestamp) DESC
                       ) AS _rn
                FROM {self._table}
                WHERE record_type = 'collection'
                  AND table_name = ?
                  AND metric_name = ?
                  AND partition_ts = ?
                  AND dimension_value IS ?
            )
            SELECT metric_name, metric_value, run_timestamp, partition_ts,
                   dimension_value, collector_name, collected_at,
                   expected_value, actual_value_text
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND metric_value IS NOT NULL
            LIMIT 1
        """
        cursor = conn.execute(
            sql, (table_name, metric_name, anchor_str, dimension_value)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    @_locked
    def read_collection_dim_values_at_partition(
        self,
        *,
        table_name: str,
        metric_name: str,
        partition_ts: Any,
    ) -> list[str | None]:
        """Distinct active dim values for one ``(table, metric)`` at
        the partition. Backs ``skip_recollection`` dimensional
        enumeration. Empty list = no active rows persisted yet."""
        from datetime import datetime as _dt

        conn = self._get_conn()
        anchor_str = (
            partition_ts.isoformat()
            if isinstance(partition_ts, _dt) else str(partition_ts)
        )
        # Codex impl-review R1 MAJOR: dedup must include
        # ``collected_at DESC`` as a tiebreaker so a tombstone /
        # NULL-value row sharing the same ``run_timestamp`` ranks
        # deterministically. Same shape as
        # ``read_collection_metric_at_partition``.
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
                  AND table_name = ?
                  AND metric_name = ?
                  AND partition_ts = ?
            )
            SELECT DISTINCT dimension_value
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
              AND metric_value IS NOT NULL
            ORDER BY dimension_value
        """
        cursor = conn.execute(sql, (table_name, metric_name, anchor_str))
        return [r[0] for r in cursor.fetchall()]

    @_locked
    def read_validations_at_partition(
        self,
        *,
        dataset_name: str,
        validation_name_prefix: str,
        partition_ts: Any,
    ) -> list[dict[str, Any]]:
        """All active validation rows at a partition for one dataset+
        validation prefix. Backs ``skip_revalidation``.

        Returns rows whose ``validation_name`` matches the prefix
        exactly OR starts with ``prefix + "."`` (catches Pattern's
        ``.schema`` sub-emission). Latest dedup per natural key
        ``(metric_name, dimension_value)`` via ROW_NUMBER ORDER BY
        ``run_timestamp DESC``. Filters ``record_type='validation'``
        and ``is_active='true'`` post-dedup so a tombstone hides
        an older live row at the same key.
        """
        from datetime import datetime as _dt

        conn = self._get_conn()
        anchor_str = (
            partition_ts.isoformat()
            if isinstance(partition_ts, _dt) else str(partition_ts)
        )
        # SQL LIKE: ``_`` is a single-character wildcard. Validation
        # names allow underscores (`_SAFE_NAME_RE`), so a prefix like
        # ``qa_1`` would match ``qaa1.schema`` without escaping
        # (codex impl-review R1 MAJOR). Escape ``_`` and ``%`` and
        # use the ANSI ESCAPE clause; ``\\`` is the escape char.
        escaped_prefix = (
            validation_name_prefix
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        like_prefix = f"{escaped_prefix}.%"
        sql = f"""
            WITH ranked AS (
                SELECT run_id, run_timestamp, dataset_name, table_name,
                       validation_name, validation_type, validation_status,
                       validation_message, metric_name, metric_value,
                       partition_ts, dimension_value,
                       expected_value, actual_value_text,
                       record_type, is_active, details_json,
                       ROW_NUMBER() OVER (
                           PARTITION BY validation_name, metric_name,
                                        dimension_value
                           ORDER BY run_timestamp DESC
                       ) AS _rn
                FROM {self._table}
                WHERE record_type = 'validation'
                  AND dataset_name = ?
                  AND (validation_name = ?
                       OR validation_name LIKE ? ESCAPE '\\')
                  AND partition_ts = ?
            )
            SELECT run_id, run_timestamp, dataset_name, table_name,
                   validation_name, validation_type, validation_status,
                   validation_message, metric_name, metric_value,
                   partition_ts, dimension_value,
                   expected_value, actual_value_text,
                   details_json
            FROM ranked
            WHERE _rn = 1
              AND is_active = 'true'
            ORDER BY validation_name, metric_name, dimension_value
        """
        cursor = conn.execute(sql, (
            dataset_name, validation_name_prefix, like_prefix, anchor_str,
        ))
        return [dict(r) for r in cursor.fetchall()]

    @_locked
    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
