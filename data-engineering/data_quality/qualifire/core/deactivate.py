"""Operator-facing deactivate-metric helper.

Implements the H1 read-back-and-bump pattern: read the latest active
row for the matched ``(dataset, metric, dim, partition)`` tuple, then
write a tombstone INSERT (``is_active='false'``) with a fresh
``run_timestamp`` strictly greater than the latest active row's
``run_timestamp``. The H2 read paths exclude tombstoned natural keys
because the tombstone wins ``ROW_NUMBER`` per natural key and the
post-dedup ``is_active = 'true'`` filter elides it.

Idempotent retry: a second call with the same arguments finds no
matching active rows (the tombstone superseded them) and returns 0.
No exception. Operators may call ``deactivate_metric`` repeatedly to
maintain a desired state.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Minimum bump to satisfy H1's "newer run_timestamp" invariant when
# the latest active row's timestamp equals or exceeds ``now()``. 1ms
# is enough to keep ROW_NUMBER ordering deterministic across backends
# (SQLite, Delta, ExternalCatalog, JDBC) which all preserve ISO-8601
# string ordering at millisecond granularity.
_TIMESTAMP_BUMP = timedelta(milliseconds=1)


def deactivate_metric(
    storage: Any,
    *,
    dataset_name: str,
    metric_name: str,
    dimension_value: str | None = None,
    partition_ts: datetime | str | None = None,
    note: str | None = None,
    owner: str = "",
    bu: str = "",
) -> int:
    """Tombstone every active row matching the filter.

    Args:
        storage: Concrete storage backend (must expose ``read_*``
            methods for the relevant key shape and a ``write_results``
            method for INSERTs).
        dataset_name: Identifies the dataset whose metric to
            deactivate.
        metric_name: Logical metric name.
        dimension_value: Optional encoded dimension string. ``None``
            (default) matches rows persisted with SQL NULL — i.e.
            non-dimensioned metrics. To deactivate a specific
            dimension pass its encoded form (see
            :func:`qualifire.core.models.encode_dimension_value`).
        partition_ts: ``None`` (default) tombstones every active
            partition for the matched (dataset, metric, dim) tuple.
            A specific timestamp / ISO string narrows to that
            partition only.
        note: Free-form string captured in the tombstone row's
            ``details_json.deactivated_by``. Useful for ops tickets
            ("OPS-12345") or human-readable rationale.
        owner / bu: Carried onto the tombstone row's metadata
            columns. Defaults to empty strings; the dashboard uses
            them only for display.

    Returns:
        Number of tombstone INSERTs written (== number of active
        rows matched).

    Raises:
        Backend-native column-not-found / table-missing errors when
        invoked against a legacy system table that pre-dates the
        ``is_active`` column (loud-fail, per D9).
    """
    table_name = _resolve_table_name(storage, dataset_name)
    rows_to_match = _read_active_rows_for_key(
        storage,
        dataset_name=dataset_name,
        table_name=table_name,
        metric_name=metric_name,
        dimension_value=dimension_value,
        partition_ts=partition_ts,
    )
    if not rows_to_match:
        logger.debug(
            "deactivate_metric: no active rows matched "
            "(dataset=%r, metric=%r, dim=%r, partition_ts=%r)",
            dataset_name, metric_name, dimension_value, partition_ts,
        )
        return 0

    # Build tombstone INSERTs. Each row's run_timestamp is bumped to
    # max(now, latest.run_timestamp + 1ms) so H1's "newer" invariant
    # is satisfied even for clock-skew or microsecond-collision
    # scenarios. Latest run_timestamp is read from the matched row.
    now = datetime.now()
    deactivated_at_iso = now.isoformat()
    tombstones: list[dict[str, Any]] = []
    for row in rows_to_match:
        latest_ts = _parse_run_timestamp(row.get("run_timestamp"))
        if latest_ts is not None and latest_ts >= now:
            tomb_ts = (latest_ts + _TIMESTAMP_BUMP).isoformat()
        else:
            tomb_ts = deactivated_at_iso
        details: dict[str, Any] = {
            "deactivated_at": deactivated_at_iso,
        }
        if note is not None:
            details["deactivated_by"] = note
        tombstones.append(_build_tombstone(
            row=row,
            dataset_name=dataset_name,
            table_name=table_name,
            metric_name=metric_name,
            dimension_value=row.get("dimension_value", dimension_value),
            run_timestamp=tomb_ts,
            details=details,
            owner=owner,
            bu=bu,
        ))

    storage.write_results(tombstones)
    logger.info(
        "deactivate_metric: wrote %d tombstone(s) for "
        "(dataset=%r, metric=%r, dim=%r, partition_ts=%r)",
        len(tombstones), dataset_name, metric_name,
        dimension_value, partition_ts,
    )
    return len(tombstones)


def build_prior_tombstone_rows(
    storage: Any,
    *,
    dataset_name: str,
    metric_name: str,
    partition_ts: datetime | str | None = None,
    dimension_value: str | None = None,
    note: str | None = None,
    owner: str = "",
    bu: str = "",
) -> list[dict[str, Any]]:
    """Item 1 (backfill-followups-and-polish): build (do NOT write)
    tombstone rows for a single metric/partition.

    Used by the per-anchor backfill driver to batch tombstones for
    multiple metrics into one ``storage.write_results(...)`` call,
    so a partial-failure scenario (one metric's tombstone build
    works, another's read fails) doesn't leave the system table in
    a mixed state. The single bulk write is atomic on SQLite (one
    transaction); on Spark backends the bulk INSERT is also a
    single Spark write whose all-or-nothing semantics dominate
    the per-row failure path.

    Mirrors :func:`deactivate_metric` exactly except for the final
    storage write — that lands in the caller.
    """
    table_name = _resolve_table_name(storage, dataset_name)
    rows_to_match = _read_active_rows_for_key(
        storage,
        dataset_name=dataset_name,
        table_name=table_name,
        metric_name=metric_name,
        dimension_value=dimension_value,
        partition_ts=partition_ts,
    )
    if not rows_to_match:
        return []
    now = datetime.now()
    deactivated_at_iso = now.isoformat()
    tombstones: list[dict[str, Any]] = []
    for row in rows_to_match:
        latest_ts = _parse_run_timestamp(row.get("run_timestamp"))
        if latest_ts is not None and latest_ts >= now:
            tomb_ts = (latest_ts + _TIMESTAMP_BUMP).isoformat()
        else:
            tomb_ts = deactivated_at_iso
        details: dict[str, Any] = {"deactivated_at": deactivated_at_iso}
        if note is not None:
            details["deactivated_by"] = note
        tombstones.append(_build_tombstone(
            row=row,
            dataset_name=dataset_name,
            table_name=table_name,
            metric_name=metric_name,
            dimension_value=row.get("dimension_value", dimension_value),
            run_timestamp=tomb_ts,
            details=details,
            owner=owner,
            bu=bu,
        ))
    return tombstones


def _read_active_rows_for_key(
    storage: Any,
    *,
    dataset_name: str,
    table_name: str,
    metric_name: str,
    dimension_value: str | None,
    partition_ts: datetime | str | None,
) -> list[dict[str, Any]]:
    """Read the active row(s) matching the tombstone target.

    When ``partition_ts`` is supplied, uses
    :meth:`SystemTableStorage.read_collection_metric_at_partition`
    (H6) for the exact-partition path. When ``partition_ts is None``,
    we have no per-backend "all-partitions" reader to use — fall back
    to :meth:`read_metric_history` with the dataset's table_name (we
    walk a generous limit to cover historic partitions; soft-delete
    semantics on each backend mean we only see active partition_ts
    values).

    The returned rows are projection dicts containing at least
    ``partition_ts`` and ``run_timestamp`` — what
    :func:`_build_tombstone` needs to emit a paired tombstone INSERT.
    """
    if not table_name:
        # No active rows for this dataset → idempotent no-op.
        return []
    if partition_ts is not None:
        row = storage.read_collection_metric_at_partition(
            table_name=table_name,
            metric_name=metric_name,
            anchor_ts=partition_ts,
            dimension_value=dimension_value,
        )
        return [row] if row else []

    # No partition_ts: walk the full active history. read_metric_history
    # with a high limit covers the typical dataset retention; backends
    # that have more rows than this are an operator-supplied edge case
    # and can be addressed by passing partition_ts explicitly.
    rows = storage.read_metric_history(
        table_name=table_name,
        metric_name=metric_name,
        limit=10000,
        dimension_value=dimension_value,
    )
    # Filter to collection rows only — we tombstone collection rows,
    # not the validation rows derived from them. Validators re-read
    # the storage chain on next run and pick up the tombstone via H2.
    return [r for r in rows if r.get("record_type") == "collection"]


def _resolve_table_name(storage: Any, dataset_name: str) -> str:
    """Best-effort resolution of ``table_name`` for the dataset.

    Backends store ``(dataset_name, table_name)`` separately because
    a single dataset can describe multiple tables. ``deactivate_metric``
    operates at the dataset level — we look up the latest row for
    ``dataset_name`` and use its ``table_name``. Returns empty string
    if the dataset has no rows (caller's read will then return [] and
    the deactivate is a 0-row no-op).
    """
    latest = storage.read_latest_run(dataset_name)
    if latest is None:
        return ""
    return latest.get("table_name", "") or ""


def _parse_run_timestamp(raw: Any) -> datetime | None:
    """Parse the storage layer's ``run_timestamp`` representation
    back to a Python datetime.

    Backends return either a Python datetime (Spark TIMESTAMP path)
    or an ISO-8601 string (SQLite, JDBC). Returns None when the
    value is unparseable — callers fall back to ``now()`` which is
    sufficient because H1 only requires a strictly-newer timestamp,
    not exact bumping from the latest.
    """
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    raw_str = str(raw)
    # Tolerate trailing 'Z' (Spark sometimes emits with offset).
    if raw_str.endswith("Z"):
        raw_str = raw_str[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw_str)
    except ValueError:
        return None


def _build_tombstone(
    *,
    row: dict[str, Any],
    dataset_name: str,
    table_name: str,
    metric_name: str,
    dimension_value: str | None,
    run_timestamp: str,
    details: dict[str, Any],
    owner: str,
    bu: str,
) -> dict[str, Any]:
    """Assemble a single tombstone row.

    The tombstone shares the row's natural-key fields
    (``dataset_name``, ``table_name``, ``metric_name``,
    ``dimension_value``, ``partition_ts``) so H2's ROW_NUMBER PARTITION
    BY produces the dedup pair. ``is_active='false'`` and
    ``metric_value=None`` mark it as a tombstone — read paths exclude
    rows whose latest version per natural key has either marker.

    ``dataset_name`` / ``table_name`` / ``metric_name`` /
    ``dimension_value`` are passed in explicitly (rather than read off
    ``row``) because the H6 read projection is intentionally narrow
    and may not include all natural-key columns. The caller is the
    authoritative source for the key.
    """
    return {
        "run_id": str(uuid.uuid4()),
        "run_timestamp": run_timestamp,
        "owner": owner,
        "bu": bu,
        "dataset_name": dataset_name,
        "table_name": table_name,
        "metric_name": metric_name,
        "metric_value": None,
        "collection_type": row.get("collection_type"),
        "validation_name": None,
        "validation_type": None,
        "validation_status": None,
        "validation_message": None,
        "notification_channel": None,
        "notification_status": None,
        "details_json": json.dumps(details),
        "record_type": "collection",
        "collector_name": row.get("collector_name"),
        "dimension_value": dimension_value,
        "collected_at": run_timestamp,
        "validated_at": None,
        "notified_at": None,
        "partition_ts": row.get("partition_ts"),
        "expected_value": None,
        "actual_value_text": None,
        "dataset_description": None,
        "validation_description": None,
        "is_active": "false",
    }
