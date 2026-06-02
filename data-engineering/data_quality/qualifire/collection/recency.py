"""Recency / freshness collector for SLO checks."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from qualifire.backends.base import Backend
from qualifire.collection.base import Collector
from qualifire.core.context import QualifireContext
from qualifire.core.models import CollectionResult


class RecencyCollector(Collector):
    """Collects the most recent timestamp for a table via configurable strategy.

    Strategies:
        max_column: SELECT MAX(column) FROM table
        delta_log: Read last operation timestamp from Delta history
        metadata: Read table metadata for last modified time
        custom_sql: Execute user-provided SQL returning a single timestamp
    """

    def __init__(
        self,
        strategy: str,
        column: str | None = None,
        sql: str | None = None,
        filter_expr: str | None = None,
    ):
        self.strategy = strategy
        self.column = column
        self.sql = sql
        self.filter_expr = filter_expr

    def collect(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        **kwargs: Any,
    ) -> list[CollectionResult]:
        filter_rendered = context.render(self.filter_expr) if self.filter_expr else None
        ts = self._get_recency(backend, table, filter_rendered, context)
        return [
            CollectionResult(
                metric_name="recency",
                metric_value=ts,
                collected_at=datetime.now(),
                metadata={"strategy": self.strategy, "table": table},
            )
        ]

    def _get_recency(
        self,
        backend: Backend,
        table: str,
        filter_expr: str | None,
        context: QualifireContext,
    ) -> datetime:
        if self.strategy == "max_column":
            where = f" WHERE {filter_expr}" if filter_expr else ""
            sql = f"SELECT MAX(`{self.column}`) AS max_ts FROM {table}{where}"
            row = backend.execute_sql(sql).collect()[0]
            val = row[0]
            if val is None:
                raise ValueError(f"No data found for MAX({self.column}) in {table}")
            if isinstance(val, str):
                return datetime.fromisoformat(val)
            return val

        elif self.strategy == "delta_log":
            history = backend.get_delta_history(table)
            if history is None:
                raise ValueError(f"Table {table} has no Delta history")
            row = history.orderBy("timestamp", ascending=False).limit(1).collect()[0]
            return row["timestamp"]

        elif self.strategy == "metadata":
            meta = backend.get_table_metadata(table)
            last_modified = meta.get("last_modified") or meta.get("lastModified")
            if not last_modified:
                raise ValueError(f"No last_modified metadata for {table}")
            if isinstance(last_modified, str):
                return datetime.fromisoformat(last_modified)
            return last_modified

        elif self.strategy == "custom_sql":
            rendered = context.render(self.sql)
            row = backend.execute_sql(rendered).collect()[0]
            val = row[0]
            if val is None:
                raise ValueError(f"Custom SQL returned NULL for recency of {table}")
            if isinstance(val, str):
                return datetime.fromisoformat(val)
            return val

        else:
            raise ValueError(f"Unknown recency strategy: {self.strategy}")
