"""Abstract system table storage protocol.

Read-side semantics for `dimension_value`:
  - Non-dimensioned rows persist with SQL ``NULL``; dimensioned
    rows persist with a JSON-encoded segment dict
    (e.g. ``'{"region":"us"}'``). Read predicates that filter by
    `dimension_value` use NULL-safe equality:
    ``IS`` on SQLite, ``<=>`` on Spark, parameterized ``IS NULL`` /
    ``=`` on JDBC. There is no ``"_default"`` sentinel.
  - `metric_name` accepts NULL (some validation rows have no metric)
    and uses the same NULL-safe equality form.
"""

from __future__ import annotations

from datetime import datetime  # noqa: F401  (used in Protocol method signature)
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from qualifire.core.models import ValidationKey


SYSTEM_TABLE_COLUMNS = [
    "run_id",
    "run_timestamp",
    "owner",
    "bu",
    "dataset_name",
    "table_name",
    "metric_name",
    "metric_value",
    "collection_type",
    "validation_name",
    "validation_type",
    "validation_status",
    "validation_message",
    "notification_channel",
    "notification_status",
    "details_json",
    "record_type",
    "collector_name",
    "dimension_value",
    "collected_at",
    "validated_at",
    "notified_at",
    # Resolved partition timestamp (ISO-8601 string). Stored alongside
    # `run_timestamp`: run_timestamp = when the run executed, partition_ts =
    # which logical partition (date/hour/etc.) the row describes. Used as
    # the anchor for partition-anchored historical reads.
    "partition_ts",
    # JSON-serialized expected bounds (e.g. {"warning": {"min": -25, "max": 25}}).
    # Surfaced as a column rather than buried in details_json so dashboards can
    # filter/render thresholds without parsing JSON every read.
    "expected_value",
    # Human-readable form of the actual value. Numeric metric_value still lives
    # in `metric_value`; this column carries text representations like
    # "P1DT2H30M" (SLO freshness) or "0.5%" — anything dashboards want to
    # render verbatim. Optional.
    "actual_value_text",
    # Optional human descriptions of the dataset / validation, captured at
    # config time and stamped on every row produced by them. Empower the
    # dashboard to surface intent without consulting the YAML.
    "dataset_description",
    "validation_description",
    # Soft-delete marker. TEXT column storing 'true' / 'false' for
    # cross-backend portability. Default 'true' on every persisted
    # row. Read paths exclude rows whose latest version per natural
    # key has is_active='false' — see plan H1, H2.
    "is_active",
]

SYSTEM_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
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
    validation_description STRING,
    is_active STRING
)
"""


# Column name -> (Spark/generic SQL type, SQLite type)
COLUMN_DEFINITIONS: dict[str, tuple[str, str]] = {
    "run_id": ("STRING", "TEXT"),
    "run_timestamp": ("TIMESTAMP", "TEXT"),
    "owner": ("STRING", "TEXT"),
    "bu": ("STRING", "TEXT"),
    "dataset_name": ("STRING", "TEXT"),
    "table_name": ("STRING", "TEXT"),
    "metric_name": ("STRING", "TEXT"),
    "metric_value": ("DOUBLE", "REAL"),
    "collection_type": ("STRING", "TEXT"),
    "validation_name": ("STRING", "TEXT"),
    "validation_type": ("STRING", "TEXT"),
    "validation_status": ("STRING", "TEXT"),
    "validation_message": ("STRING", "TEXT"),
    "notification_channel": ("STRING", "TEXT"),
    "notification_status": ("STRING", "TEXT"),
    "details_json": ("STRING", "TEXT"),
    "record_type": ("STRING", "TEXT"),
    "collector_name": ("STRING", "TEXT"),
    "dimension_value": ("STRING", "TEXT"),
    "collected_at": ("TIMESTAMP", "TEXT"),
    "validated_at": ("TIMESTAMP", "TEXT"),
    "notified_at": ("TIMESTAMP", "TEXT"),
    "partition_ts": ("TIMESTAMP", "TEXT"),
    "expected_value": ("STRING", "TEXT"),
    "actual_value_text": ("STRING", "TEXT"),
    "dataset_description": ("STRING", "TEXT"),
    "validation_description": ("STRING", "TEXT"),
    "is_active": ("STRING", "TEXT"),
}


@runtime_checkable
class SystemTableStorage(Protocol):
    """Protocol for system table storage backends.

    All implementations are append-only (insert + select).
    """

    def initialize(self) -> None:
        """Create the system table if it doesn't exist."""
        ...

    def write_results(self, rows: list[dict[str, Any]]) -> None:
        """Append result rows to the system table."""
        ...

    def read_metric_history(
        self,
        table_name: str,
        metric_name: str,
        limit: int = 90,
        step: str | None = None,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read historical values for a metric, ordered by run_timestamp desc.

        ``dimension_value`` filters per-segment history. ``None``
        matches non-dimensioned rows (persisted as SQL NULL); pass a
        JSON-encoded segment string to match a specific segment.
        Backends use NULL-safe equality so the same ``None`` matches
        a NULL row uniformly across SQLite / Spark / JDBC.
        """
        ...

    def read_latest_run(self, dataset_name: str) -> dict[str, Any] | None:
        """Read the most recent run result for a dataset."""
        ...

    def read_validation_history(
        self,
        dataset_name: str,
        validation_name: str,
        limit: int = 10,
        metric_name: str | None = None,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read past validation results for deduplication.

        Two-arg form (`dataset_name`, `validation_name` only) is
        backwards compatible: matches every row with those two
        fields regardless of metric/dim. Pass `metric_name=` and
        `dimension_value=` explicitly to filter strictly.
        """
        ...

    def read_validation_history_bulk(
        self,
        keys: list["ValidationKey"],
        limit: int = 1,
    ) -> dict["ValidationKey", list[dict[str, Any]]]:
        """Bulk-fetch suppression history for many keys in one round-trip.

        Used by the engine's notification path to pre-fetch
        suppression state before any current-run write happens
        (avoids self-suppression on resequenced persistence).
        """
        ...

    def read_health_data(
        self, days: int = 30, *, include_collection: bool = False,
    ) -> list[dict[str, Any]]:
        """Read recent validation rows for health reporting.

        Returns rows with: ``run_id``, ``owner``, ``bu``,
        ``dataset_name``, ``table_name``, ``validation_type``,
        ``validation_status``, ``run_timestamp``, ``validation_name``,
        ``metric_name``, ``metric_value``, ``partition_ts``,
        ``dimension_value``, ``expected_value``, ``actual_value_text``,
        ``validation_message``, ``record_type``,
        ``dataset_description``, ``validation_description``,
        ``details_json`` (optional — every concrete backend in tree
        already projects it; the interactive dashboard's
        click-to-expand row detail panel consumes it as a parsed
        dict). Filtered to ``record_type='validation'`` within the
        last ``days`` days.

        Args:
            days: time window in days.
            include_collection: when True, also return
                ``record_type='collection'`` rows. Used by the
                interactive dashboard to plot the full per-partition
                history of metrics that history-backed validators
                (drift / forecast / shape / pattern) reference but
                only validate on the current partition.
        """
        ...

    def read_metric_history_by_partition(
        self,
        table_name: str,
        metric_name: str,
        anchor_ts: "datetime",
        count: int,
        step: str,
        dimension_value: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read partition-anchored historical values for a metric.

        Returns the rows whose ``partition_ts`` is exactly
        ``anchor_ts − k·step`` for k = 1, 2, …, ``count``.
        Ordered most-recent-first.

        Example: anchor=2026-04-28, count=3, step="P7D" returns rows
        for partition_ts ∈ {2026-04-21, 2026-04-14, 2026-04-07}.

        ``step`` is ISO 8601 only (``P7D``, ``PT1H``, ``P2DT12H``).
        Months / years (``PnM`` / ``PnY``) are rejected — variable
        widths break the anchor math. Legacy compact (``"7D"``,
        ``"1H"``) is rejected at parse time.

        ``dimension_value`` filters per-segment via NULL-safe
        equality (``IS`` / ``<=>`` / parameterized per backend).
        Pass ``None`` to match non-dimensioned rows.

        Rows missing partition_ts are excluded — partition-anchored
        reads make no sense without an anchor on each row.
        """
        ...

    def read_collection_metric_at_partition(
        self,
        table_name: str,
        metric_name: str,
        anchor_ts: "datetime",
        dimension_value: str | None = None,
    ) -> dict[str, Any] | None:
        """Read the latest active collection cell for a partition.

        Used by the backfill loop's ``skip_if_present`` pre-check and
        by ``Qualifire.validate(skip_recollection=True)`` to short-circuit
        re-collection when an active row already exists for the
        ``(table_name, metric_name, partition_ts=anchor_ts,
        dimension_value)`` natural key.

        Filter chain (per plan H2 / H6): ``record_type='collection'``
        ∧ exact ``partition_ts`` ∧ NULL-safe ``dimension_value``,
        ROW_NUMBER ORDER BY ``run_timestamp DESC, collected_at DESC``,
        then ``rn = 1`` ∧ ``is_active = 'true'`` ∧
        ``metric_value IS NOT NULL``.

        Returns the surviving row as a dict (with ``metric_name``,
        ``metric_value``, ``partition_ts``, ``dimension_value``,
        ``collector_name``, ``collected_at``, ``expected_value``,
        ``actual_value_text``) or ``None``. ``read_validation_history_bulk``
        is unrelated — it remains the notification-suppression API.
        """
        ...

    def read_collection_dim_values_at_partition(
        self,
        *,
        table_name: str,
        metric_name: str,
        partition_ts: "datetime",
    ) -> list[str | None]:
        """Read distinct active dimension values at a partition for
        one ``(table, metric)``. Backs the dimensional branch of
        the ``skip_recollection`` pre-pass — the engine enumerates
        which dim values were persisted last run, then point-looks
        up each. Returns the distinct dim values (including
        ``None`` for non-dimensioned rows). Empty list = cache
        miss; the engine falls through to the collector.
        """
        ...

    def read_validations_at_partition(
        self,
        *,
        dataset_name: str,
        validation_name_prefix: str,
        partition_ts: "datetime",
    ) -> list[dict[str, Any]]:
        """Read all active validation rows for a single dataset+
        validation prefix at a specific partition. Backs the
        ``skip_revalidation`` runtime flag's pre-pass.

        Filter chain: ``record_type='validation'`` AND exact
        ``partition_ts`` AND ``dataset_name`` match AND
        (``validation_name = prefix`` OR ``validation_name LIKE
        prefix + '.%'``). Latest dedup per natural key
        ``(metric_name, dimension_value)`` via ROW_NUMBER ORDER BY
        ``run_timestamp DESC``, post-dedup filter
        ``is_active = 'true'`` so a tombstone
        hides an older live row at the same key.

        Returns a list of row dicts (zero or more); empty list
        means "validator should re-run."
        """
        ...
