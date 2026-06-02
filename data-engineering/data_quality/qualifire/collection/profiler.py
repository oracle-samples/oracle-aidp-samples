"""Column profiling collector using the single-pass profiling engine."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from qualifire.backends.base import Backend
from qualifire.collection._filters import render_filter_once
from qualifire.collection.base import Collector
from qualifire.collection.profiling import ProfileEngine, StatAnalyzer
from qualifire.collection.profiling.engine import ColumnProfileOverrides
from qualifire.core.context import QualifireContext
from qualifire.core.models import CollectionResult


class ProfilingCollector(Collector):
    """Collects column-level statistics via the single-pass ProfileEngine.

    Type-aware: applies NumericAnalyzer, TextAnalyzer, BooleanAnalyzer,
    or TimestampAnalyzer based on each column's data type. All stats are
    computed in ONE `df.agg()` call (except top-K which needs a groupBy).

    Supports granular stat control:
        - `stats`: global include filter (only these stats are computed)
        - `exclude_stats`: global exclude filter (these stats are skipped)
        - `column_profiles`: per-column overrides with their own include/exclude

    Returns one CollectionResult per stat per column.

    Args:
        columns: Subset of columns to profile (None = all).
        stats: Global include filter (None = all stats).
        exclude_stats: Global exclude filter.
        column_profiles: Per-column stat overrides {col: {stats: [...], exclude_stats: [...]}}.
        filter_expr: Optional WHERE clause filter.
        analyzers: Custom list of StatAnalyzer instances (None = defaults).
        top_k: Number of most frequent values for string columns (0 to disable).
        quantiles: Percentile probabilities for numeric columns.
    """

    def __init__(
        self,
        columns: list[str] | None = None,
        stats: list[str] | None = None,
        exclude_stats: list[str] | None = None,
        column_profiles: dict[str, dict[str, Any]] | None = None,
        filter_expr: str | None = None,
        analyzers: list[StatAnalyzer] | None = None,
        top_k: int = 10,
        quantiles: list[float] | None = None,
        partition_ts_expr: str | None = None,
    ):
        self.columns = columns
        self.stats = stats
        self.exclude_stats = exclude_stats
        self.column_profiles = column_profiles
        self.filter_expr = filter_expr
        self.top_k = top_k
        self.quantiles = quantiles
        self._custom_analyzers = analyzers
        self.partition_ts_expr = partition_ts_expr

    def _build_analyzers(self) -> list[StatAnalyzer]:
        if self._custom_analyzers is not None:
            return self._custom_analyzers
        from qualifire.collection.profiling.analyzers import (
            BooleanAnalyzer,
            NumericAnalyzer,
            TextAnalyzer,
            TimestampAnalyzer,
        )
        return [
            NumericAnalyzer(quantiles=self.quantiles or [0.25, 0.50, 0.75]),
            TextAnalyzer(top_k=self.top_k),
            BooleanAnalyzer(),
            TimestampAnalyzer(),
        ]

    def _build_column_overrides(self) -> dict[str, ColumnProfileOverrides] | None:
        if not self.column_profiles:
            return None
        overrides = {}
        for col_name, cfg in self.column_profiles.items():
            if isinstance(cfg, dict):
                overrides[col_name] = ColumnProfileOverrides(
                    stats=cfg.get("stats"),
                    exclude_stats=cfg.get("exclude_stats"),
                )
            elif hasattr(cfg, "stats"):
                overrides[col_name] = ColumnProfileOverrides(
                    stats=cfg.stats,
                    exclude_stats=cfg.exclude_stats,
                )
        return overrides

    def collect(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        **kwargs: Any,
    ) -> list[CollectionResult]:
        from qualifire.collection._partition import coerce_partition_ts

        filter_rendered = render_filter_once(self.filter_expr, context)
        now = datetime.now()

        # Profiling stamps a constant ``partition_ts`` (the dataset's
        # resolved literal) on every per-column / per-stat result.
        # Profiling is inherently a single-shot scan over the filtered
        # frame, so per-row partition_ts variation isn't meaningful;
        # one anchor matches the dataset's run identity.
        partition_ts = coerce_partition_ts(
            context.render(self.partition_ts_expr)
            if self.partition_ts_expr else None
        )

        # Get the DataFrame
        if filter_rendered:
            df = backend.execute_sql(f"SELECT * FROM {table} WHERE {filter_rendered}")
        else:
            df = backend.execute_sql(f"SELECT * FROM {table}")

        # Run single-pass profiling
        engine = ProfileEngine(analyzers=self._build_analyzers())
        profile_result = engine.profile(
            df=df,
            table_name=table,
            columns=self.columns,
            stats=self.stats,
            exclude_stats=self.exclude_stats,
            column_overrides=self._build_column_overrides(),
        )

        # Convert to CollectionResults
        results: list[CollectionResult] = []
        for col_name, col_profile in profile_result.columns.items():
            for stat_name, stat_value in col_profile.stats.items():
                results.append(
                    CollectionResult(
                        metric_name=f"{col_name}.{stat_name}",
                        metric_value=stat_value,
                        collected_at=now,
                        metadata={
                            "column": col_name,
                            "stat": stat_name,
                            "column_type": col_profile.column_type,
                            "table": table,
                            "collection_type": "profiling",
                        },
                        partition_ts=partition_ts,
                    )
                )

        return results
