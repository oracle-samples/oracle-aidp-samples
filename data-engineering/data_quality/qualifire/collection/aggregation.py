"""Aggregation collector for user-defined SQL expressions."""

from __future__ import annotations

import json
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


# ``_render_filter_once`` was originally defined here as a
# module-level helper shared by the three filtered collectors.
# Lifted to ``qualifire.collection._filters`` as
# ``render_filter_once`` so siblings (metrics, profiler) import
# from a single canonical location instead of from this module.
# Local re-export preserves the existing test that asserts
# ``aggregation._render_filter_once`` round-trips correctly.
_render_filter_once = render_filter_once


class AggregationCollector(Collector):
    """Collects aggregated metrics via user-defined SQL expressions.

    Sub-feature A (renamed shape): ``expressions`` is a
    ``dict[str, str]`` of ``{metric_name: sql_expression}``. The
    collector wraps each expression as ``(<sql>) AS <name>`` for the
    backend's SELECT; the metric name comes from the dict key, not
    from a fragile ``AS <name>`` suffix lexer.

    Returns one CollectionResult per dict entry.

    When ``dimensions`` is specified, the query includes a GROUP BY on
    those columns and produces one CollectionResult per
    (metric_name, dimension_combo).

    When ``partition_ts_expr`` is supplied (a SQL expression, post-Jinja
    rendering by the engine), it's added as ``<expr> AS qf_partition_ts``
    to the SELECT and stamped onto each result. For non-dimensional
    aggregations the expression must be constant (e.g. ``'2026-05-06'``,
    ``CURRENT_TIMESTAMP``); column refs require GROUP BY participation
    or Spark will reject the query.
    """

    def __init__(
        self,
        expressions: dict[str, str],
        filter_expr: str | None = None,
        dimensions: list[str] | None = None,
        partition_ts_expr: str | None = None,
    ):
        self.expressions = dict(expressions)
        self.filter_expr = filter_expr
        self.dimensions = dimensions
        self.partition_ts_expr = partition_ts_expr

    def _build_select_exprs(self, context: QualifireContext) -> list[str]:
        """Render each ``{name: expr}`` pair into ``(<rendered>) AS <name>``.

        The wrapping parentheses match the ``Qualifire.threshold_check``
        builder's existing output, so collector behaviour is unchanged
        from the operator's view.
        """
        rendered: list[str] = []
        for name, expr in self.expressions.items():
            rendered.append(f"({context.render(expr)}) AS {name}")
        return rendered

    def collect(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        **kwargs: Any,
    ) -> list[CollectionResult]:
        rendered_exprs = self._build_select_exprs(context)
        filter_rendered = render_filter_once(self.filter_expr, context)
        partition_rendered = (
            context.render(self.partition_ts_expr) if self.partition_ts_expr else None
        )
        now = datetime.now()

        if self.dimensions:
            return self._collect_with_dimensions(
                backend, table, rendered_exprs, filter_rendered, partition_rendered, now,
            )

        # Non-dimensional aggregation. Inject the partition_ts expression as an
        # extra "aggregation" — backend.get_aggregations runs them all in one
        # SELECT — and pop the synthetic column out of the per-metric result.
        if partition_rendered:
            rendered_exprs = [*rendered_exprs, f"({partition_rendered}) AS {_PARTITION_ALIAS}"]
        row = backend.get_aggregations(table, rendered_exprs, filter_rendered)

        partition_ts = _coerce_partition_ts(row.pop(_PARTITION_ALIAS, None))

        results: list[CollectionResult] = []
        for key, value in row.items():
            results.append(
                CollectionResult(
                    metric_name=key,
                    metric_value=value,
                    collected_at=now,
                    metadata={"table": table, "collection_type": "aggregation"},
                    partition_ts=partition_ts,
                )
            )
        return results

    def _collect_with_dimensions(
        self,
        backend: Backend,
        table: str,
        rendered_exprs: list[str],
        filter_rendered: str | None,
        partition_rendered: str | None,
        now: datetime,
    ) -> list[CollectionResult]:
        """Collect with GROUP BY dimensions, producing one result per group."""
        dim_cols = [f"`{d}`" for d in self.dimensions]
        select_cols = list(dim_cols) + list(rendered_exprs)
        # Dimensional aggregations: partition_ts can reference dimension cols
        # (already in GROUP BY) or be constant. Column refs outside GROUP BY
        # would cause Spark to reject the query — propagate the error verbatim.
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
        dim_set = set(self.dimensions)

        from qualifire.core.models import encode_dimension_value

        results: list[CollectionResult] = []
        for row in rows:
            row_dict = row.asDict() if hasattr(row, "asDict") else row
            partition_ts = _coerce_partition_ts(row_dict.pop(_PARTITION_ALIAS, None))
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
                            "collection_type": "aggregation",
                            "dimensions": self.dimensions,
                        },
                        dimension_value=dim_val,
                        partition_ts=partition_ts,
                    )
                )
        return results
