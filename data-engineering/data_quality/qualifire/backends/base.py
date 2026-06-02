"""Abstract backend protocol for data access."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Backend(Protocol):
    """Protocol that all data backends must implement.

    PySpark is the default; Pandas is the secondary option.
    """

    def execute_sql(self, sql: str) -> Any:
        """Execute a SQL statement and return result as a DataFrame."""
        ...

    def get_table_metadata(self, table: str) -> dict[str, Any]:
        """Return table metadata: columns, types, partitions, etc."""
        ...

    def get_column_profile(
        self, table: str, column: str, filter_expr: str | None = None
    ) -> dict[str, Any]:
        """Return column-level statistics."""
        ...

    def get_aggregations(
        self, table: str, expressions: list[str], filter_expr: str | None = None
    ) -> dict[str, Any]:
        """Execute aggregation expressions and return name->value mapping."""
        ...

    def sample_records(
        self, table: str, n: int, filter_expr: str | None = None
    ) -> Any:
        """Return a sample of N records as a DataFrame."""
        ...

    def get_delta_history(self, table: str) -> Any | None:
        """Return Delta Lake table history, or None if not a Delta table."""
        ...

    def write_df(self, df: Any, table: str, options: dict[str, Any] | None = None) -> None:
        """Write a DataFrame to a table."""
        ...

    def drop_table(self, table: str) -> None:
        """Drop a permanent table if it exists."""
        ...

    def table_exists(self, table: str) -> bool:
        """Check if a table exists."""
        ...

    def register_table(self, name: str, df: Any) -> None:
        """Register a DataFrame as a temp/named view that subsequent
        ``execute_sql`` calls can reference by name. Distinct from
        ``write_df`` (which writes a permanent table). On Spark this
        wraps ``df.createOrReplaceTempView``; on Pandas it stores
        the frame in the backend's in-memory table dict.
        """
        ...

    def drop_temp_view(self, name: str) -> None:
        """Drop a registered temp view. Distinct from ``drop_table``,
        which drops permanent tables. On Spark this issues
        ``DROP VIEW IF EXISTS``; on Pandas it removes the entry
        from the backend's table dict.
        """
        ...
