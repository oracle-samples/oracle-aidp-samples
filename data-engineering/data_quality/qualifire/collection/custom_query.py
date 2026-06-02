"""Custom SQL/expression collector."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from qualifire.backends.base import Backend
from qualifire.collection._partition import (
    PARTITION_ALIAS as _PARTITION_ALIAS,
    coerce_partition_ts as _coerce_partition_ts,
)
from qualifire.collection.base import Collector
from qualifire.core.context import QualifireContext
from qualifire.core.models import CollectionResult


class CustomQueryCollector(Collector):
    """Collects metrics via arbitrary user-provided SQL.

    The SQL should return a single row with named columns. Each column
    becomes a separate CollectionResult.

    When ``dimensions`` is specified, the query is expected to return
    multiple rows. The named dimension columns are used as the dimension
    value and remaining columns become metrics, one CollectionResult
    per (metric, dimension_combo).

    partition_ts handling
    ---------------------
    Two ways to stamp ``cr.partition_ts``:

    1. **Per-row (preferred for dimensional SQL):** include a
       ``qf_partition_ts`` column in your SELECT. The collector pops
       that column out of the row and stamps the parsed value on each
       result. Per-row values are useful when the custom SQL aggregates
       across multiple partitions in one query.
    2. **Constant (default):** when ``partition_ts_expr`` is supplied
       on the dataset and the SELECT does NOT carry a
       ``qf_partition_ts`` column, the collector renders the
       expression once and stamps the same value on every result.
       Suitable for SQL that's pinned to a single run anchor (e.g.
       a Jinja-templated WHERE clause that filters to ``{{ ds }}``).

    Both work for dimensional and non-dimensional queries.

    Args:
        sql: SQL query template (supports Jinja2 variables).
        dimensions: Optional column names to treat as dimension keys.
        partition_ts_expr: Dataset's partition_ts expression (post-Jinja).
            Used as the constant fallback when the SELECT doesn't
            include a ``qf_partition_ts`` column.
    """

    def __init__(
        self,
        sql: str,
        dimensions: list[str] | None = None,
        partition_ts_expr: str | None = None,
    ):
        self.sql = sql
        self.dimensions = dimensions
        self.partition_ts_expr = partition_ts_expr

    def collect(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        **kwargs: Any,
    ) -> list[CollectionResult]:
        rendered_sql = context.render(self.sql, extra={"table": table})
        df = backend.execute_sql(rendered_sql)
        rows = df.collect()
        now = datetime.now()

        # Constant-fallback partition_ts: render the dataset's
        # ``partition_ts`` expression once. Per-row values from the
        # ``qf_partition_ts`` column win when present.
        constant_partition_ts = _coerce_partition_ts(
            context.render(self.partition_ts_expr)
            if self.partition_ts_expr else None
        )

        if not rows:
            return []

        if self.dimensions:
            return self._collect_with_dimensions(
                rows, table, now, constant_partition_ts,
            )

        row = rows[0].asDict() if hasattr(rows[0], "asDict") else dict(rows[0])
        partition_ts = (
            _coerce_partition_ts(row.pop(_PARTITION_ALIAS, None))
            or constant_partition_ts
        )
        return [
            CollectionResult(
                metric_name=key,
                metric_value=value,
                collected_at=now,
                metadata={"table": table, "collection_type": "custom_query"},
                partition_ts=partition_ts,
            )
            for key, value in row.items()
        ]

    def _collect_with_dimensions(
        self,
        rows: list,
        table: str,
        now: datetime,
        constant_partition_ts: datetime | None,
    ) -> list[CollectionResult]:
        """Produce one CollectionResult per (metric, dimension_combo)."""
        from qualifire.core.models import encode_dimension_value

        dim_set = set(self.dimensions)
        results: list[CollectionResult] = []
        for row in rows:
            row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)
            partition_ts = (
                _coerce_partition_ts(row_dict.pop(_PARTITION_ALIAS, None))
                or constant_partition_ts
            )
            dim_val = encode_dimension_value(
                {d: row_dict.get(d, "") for d in self.dimensions}
            )
            for key, value in row_dict.items():
                if key in dim_set:
                    continue
                results.append(
                    CollectionResult(
                        metric_name=key,
                        metric_value=value,
                        collected_at=now,
                        metadata={
                            "table": table,
                            "collection_type": "custom_query",
                            "dimensions": self.dimensions,
                        },
                        dimension_value=dim_val,
                        partition_ts=partition_ts,
                    )
                )
        return results
