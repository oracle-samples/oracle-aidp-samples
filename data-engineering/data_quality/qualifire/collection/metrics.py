"""Named KPI metrics collector."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from qualifire.backends.base import Backend
from qualifire.collection._filters import render_filter_once
from qualifire.collection._partition import (
    PARTITION_ALIAS as _PARTITION_ALIAS,
    coerce_partition_ts as _coerce_partition_ts,
)
from qualifire.collection.base import Collector
from qualifire.core.context import QualifireContext
from qualifire.core.models import CollectionResult


class MetricsCollector(Collector):
    """Collects named KPI metrics via SQL expressions.

    Args:
        metrics: Mapping of metric name -> SQL expression.
        filter_expr: Optional WHERE clause filter.
        dimensions: Optional columns to GROUP BY, producing per-dimension results.
        partition_ts_expr: Dataset's partition_ts expression (post-Jinja).
            When set, injected as ``(<expr>) AS qf_partition_ts`` into
            the SELECT and stamped on each :class:`CollectionResult`.
            Required for drift / forecast rules that build on this
            collector — the validators surface a structured ERROR
            otherwise.
    """

    def __init__(
        self,
        metrics: dict[str, str],
        filter_expr: str | None = None,
        dimensions: list[str] | None = None,
        partition_ts_expr: str | None = None,
    ):
        self.metrics = metrics
        self.filter_expr = filter_expr
        self.dimensions = dimensions
        self.partition_ts_expr = partition_ts_expr

    def collect(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        **kwargs: Any,
    ) -> list[CollectionResult]:
        # Build SQL expressions as "expr AS name"
        expressions = [
            f"({context.render(expr)}) AS {name}"
            for name, expr in self.metrics.items()
        ]
        filter_rendered = render_filter_once(self.filter_expr, context)
        partition_rendered = (
            context.render(self.partition_ts_expr)
            if self.partition_ts_expr else None
        )
        now = datetime.now()

        if self.dimensions:
            return self._collect_with_dimensions(
                backend, table, expressions, filter_rendered,
                partition_rendered, now,
            )

        # Non-dimensional path: inject the partition_ts expression as
        # an extra "metric" so backend.get_aggregations runs them in a
        # single SELECT. Pop the synthetic column out of each row.
        if partition_rendered:
            expressions = [
                *expressions,
                f"({partition_rendered}) AS {_PARTITION_ALIAS}",
            ]
        row = backend.get_aggregations(table, expressions, filter_rendered)
        partition_ts = _coerce_partition_ts(row.pop(_PARTITION_ALIAS, None))

        return [
            CollectionResult(
                metric_name=name,
                metric_value=row[name],
                collected_at=now,
                metadata={"table": table, "collection_type": "metrics"},
                partition_ts=partition_ts,
            )
            for name in self.metrics
        ]

    def _collect_with_dimensions(
        self,
        backend: Backend,
        table: str,
        expressions: list[str],
        filter_rendered: str | None,
        partition_rendered: str | None,
        now: datetime,
    ) -> list[CollectionResult]:
        """Collect with GROUP BY dimensions, producing one result per group."""
        from qualifire.core.models import encode_dimension_value

        dim_cols = [f"`{d}`" for d in self.dimensions]
        select_cols = list(dim_cols) + list(expressions)
        if partition_rendered:
            select_cols.append(f"({partition_rendered}) AS {_PARTITION_ALIAS}")
        select_clause = ", ".join(select_cols)
        group_clause = ", ".join(dim_cols)
        sql = f"SELECT {select_clause} FROM {table}"
        if filter_rendered:
            sql += f" WHERE {filter_rendered}"
        sql += f" GROUP BY {group_clause}"

        df = backend.execute_sql(sql)
        rows = df.collect() if hasattr(df, "collect") else df.to_dict("records")

        results: list[CollectionResult] = []
        for row in rows:
            row_dict = row.asDict() if hasattr(row, "asDict") else row
            partition_ts = _coerce_partition_ts(
                row_dict.pop(_PARTITION_ALIAS, None)
            )
            dim_val = encode_dimension_value(
                {d: row_dict.get(d, "") for d in self.dimensions}
            )
            for name in self.metrics:
                results.append(
                    CollectionResult(
                        metric_name=name,
                        metric_value=row_dict.get(name),
                        collected_at=now,
                        metadata={
                            "table": table,
                            "collection_type": "metrics",
                            "dimensions": self.dimensions,
                        },
                        dimension_value=dim_val,
                        partition_ts=partition_ts,
                    )
                )
        return results
