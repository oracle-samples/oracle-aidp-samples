"""Composable column-type analyzers for single-pass profiling.

Each analyzer declares the column types it applies to and provides
Spark Column expressions that will be batched into one `df.agg()` call.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnStat:
    """A single stat definition for a column."""

    name: str
    expr_builder: Any  # Callable(col_name) -> pyspark.sql.Column


class StatAnalyzer(ABC):
    """Base class for type-aware column analyzers.

    Subclass this to add custom profiling stats. The engine collects
    expressions from all applicable analyzers and runs them in one pass.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Analyzer identifier."""
        ...

    @property
    @abstractmethod
    def applicable_types(self) -> set[str]:
        """Set of type categories: 'numeric', 'string', 'boolean', 'timestamp'."""
        ...

    @abstractmethod
    def spark_expressions(self, col_name: str) -> list[tuple[str, Any]]:
        """Return (stat_name, spark_Column) pairs for a single column.

        These get aliased as `{col_name}__{stat_name}` and batched into
        one `df.agg()` call across ALL columns.
        """
        ...

    @abstractmethod
    def pandas_compute(self, series: Any, col_name: str) -> dict[str, Any]:
        """Compute stats from a pandas Series. Return {stat_name: value}."""
        ...

    def needs_separate_pass(self) -> list[str]:
        """Return stat names that require a separate query (e.g. top_k).

        Override in subclasses that need groupBy or other multi-row ops.
        """
        return []


class NumericAnalyzer(StatAnalyzer):
    """Profiling stats for numeric columns (int, float, decimal).

    Stats: count, null_count, null_pct, min, max, mean, stddev, variance,
    skewness, kurtosis, approx_distinct, quantiles (p25, p50, p75).
    """

    def __init__(self, quantiles: list[float] | None = None):
        self.quantiles = quantiles or [0.25, 0.50, 0.75]

    @property
    def name(self) -> str:
        return "numeric"

    @property
    def applicable_types(self) -> set[str]:
        return {"numeric"}

    def spark_expressions(self, col_name: str) -> list[tuple[str, Any]]:
        from pyspark.sql import functions as F

        c = F.col(f"`{col_name}`")
        is_null = F.when(c.isNull(), 1).otherwise(0)
        total = F.count(F.lit(1))

        exprs = [
            ("count", F.count(F.lit(1))),
            ("null_count", F.sum(is_null)),
            ("null_pct", F.round(F.sum(is_null) * 100.0 / total, 4)),
            ("min", F.min(c)),
            ("max", F.max(c)),
            ("mean", F.mean(c.cast("double"))),
            ("stddev", F.stddev(c.cast("double"))),
            ("variance", F.variance(c.cast("double"))),
            ("skewness", F.skewness(c.cast("double"))),
            ("kurtosis", F.kurtosis(c.cast("double"))),
            ("approx_distinct", F.approx_count_distinct(c)),
            ("quantiles", F.percentile_approx(c.cast("double"), self.quantiles)),
        ]
        return exprs

    def pandas_compute(self, series: Any, col_name: str) -> dict[str, Any]:
        import numpy as np

        total = len(series)
        null_count = int(series.isna().sum())
        non_null = series.dropna()
        quantile_values = {}
        for q in self.quantiles:
            quantile_values[f"p{int(q * 100)}"] = float(non_null.quantile(q)) if len(non_null) > 0 else None

        return {
            "count": total,
            "null_count": null_count,
            "null_pct": round(null_count * 100.0 / max(total, 1), 4),
            "min": non_null.min() if len(non_null) > 0 else None,
            "max": non_null.max() if len(non_null) > 0 else None,
            "mean": float(non_null.mean()) if len(non_null) > 0 else None,
            "stddev": float(non_null.std()) if len(non_null) > 0 else None,
            "variance": float(non_null.var()) if len(non_null) > 0 else None,
            "skewness": float(non_null.skew()) if len(non_null) > 2 else None,
            "kurtosis": float(non_null.kurtosis()) if len(non_null) > 3 else None,
            "approx_distinct": int(series.nunique()),
            "quantiles": quantile_values,
        }


class TextAnalyzer(StatAnalyzer):
    """Profiling stats for string/text columns.

    Stats: count, null_count, null_pct, min_length, max_length, avg_length,
    approx_distinct, empty_count, empty_pct. Top-K requires a separate pass
    on Spark 3.x (groupBy per column). Spark 4.0+ adds approx_top_k() which
    can be used inside agg() for true single-pass — see needs_separate_pass().
    """

    def __init__(self, top_k: int = 10):
        self.top_k = top_k

    @property
    def name(self) -> str:
        return "text"

    @property
    def applicable_types(self) -> set[str]:
        return {"string"}

    def spark_expressions(self, col_name: str) -> list[tuple[str, Any]]:
        from pyspark.sql import functions as F

        c = F.col(f"`{col_name}`")
        is_null = F.when(c.isNull(), 1).otherwise(0)
        is_empty = F.when(c == "", 1).otherwise(0)
        total = F.count(F.lit(1))
        str_len = F.length(c)

        return [
            ("count", F.count(F.lit(1))),
            ("null_count", F.sum(is_null)),
            ("null_pct", F.round(F.sum(is_null) * 100.0 / total, 4)),
            ("min_length", F.min(str_len)),
            ("max_length", F.max(str_len)),
            ("avg_length", F.round(F.mean(str_len.cast("double")), 2)),
            ("approx_distinct", F.approx_count_distinct(c)),
            ("empty_count", F.sum(is_empty)),
            ("empty_pct", F.round(F.sum(is_empty) * 100.0 / total, 4)),
        ]

    def pandas_compute(self, series: Any, col_name: str) -> dict[str, Any]:
        total = len(series)
        null_count = int(series.isna().sum())
        non_null = series.dropna().astype(str)
        lengths = non_null.str.len()
        empty_count = int((non_null == "").sum())

        return {
            "count": total,
            "null_count": null_count,
            "null_pct": round(null_count * 100.0 / max(total, 1), 4),
            "min_length": int(lengths.min()) if len(lengths) > 0 else None,
            "max_length": int(lengths.max()) if len(lengths) > 0 else None,
            "avg_length": round(float(lengths.mean()), 2) if len(lengths) > 0 else None,
            "approx_distinct": int(series.nunique()),
            "empty_count": empty_count,
            "empty_pct": round(empty_count * 100.0 / max(total, 1), 4),
        }

    def needs_separate_pass(self) -> list[str]:
        return ["top_k"] if self.top_k > 0 else []


class BooleanAnalyzer(StatAnalyzer):
    """Profiling stats for boolean columns.

    Stats: count, null_count, null_pct, true_count, false_count, true_pct.
    """

    @property
    def name(self) -> str:
        return "boolean"

    @property
    def applicable_types(self) -> set[str]:
        return {"boolean"}

    def spark_expressions(self, col_name: str) -> list[tuple[str, Any]]:
        from pyspark.sql import functions as F

        c = F.col(f"`{col_name}`")
        is_null = F.when(c.isNull(), 1).otherwise(0)
        total = F.count(F.lit(1))
        true_count = F.sum(c.cast("int"))

        return [
            ("count", F.count(F.lit(1))),
            ("null_count", F.sum(is_null)),
            ("null_pct", F.round(F.sum(is_null) * 100.0 / total, 4)),
            ("true_count", true_count),
            ("false_count", total - F.sum(is_null) - true_count),
            ("true_pct", F.round(true_count * 100.0 / (total - F.sum(is_null)), 4)),
        ]

    def pandas_compute(self, series: Any, col_name: str) -> dict[str, Any]:
        total = len(series)
        null_count = int(series.isna().sum())
        non_null = series.dropna()
        true_count = int(non_null.sum())
        false_count = len(non_null) - true_count

        return {
            "count": total,
            "null_count": null_count,
            "null_pct": round(null_count * 100.0 / max(total, 1), 4),
            "true_count": true_count,
            "false_count": false_count,
            "true_pct": round(true_count * 100.0 / max(len(non_null), 1), 4),
        }


class TimestampAnalyzer(StatAnalyzer):
    """Profiling stats for timestamp/date columns.

    Stats: count, null_count, null_pct, min, max, approx_distinct.
    """

    @property
    def name(self) -> str:
        return "timestamp"

    @property
    def applicable_types(self) -> set[str]:
        return {"timestamp"}

    def spark_expressions(self, col_name: str) -> list[tuple[str, Any]]:
        from pyspark.sql import functions as F

        c = F.col(f"`{col_name}`")
        is_null = F.when(c.isNull(), 1).otherwise(0)
        total = F.count(F.lit(1))

        return [
            ("count", F.count(F.lit(1))),
            ("null_count", F.sum(is_null)),
            ("null_pct", F.round(F.sum(is_null) * 100.0 / total, 4)),
            ("min", F.min(c)),
            ("max", F.max(c)),
            ("approx_distinct", F.approx_count_distinct(c)),
        ]

    def pandas_compute(self, series: Any, col_name: str) -> dict[str, Any]:
        total = len(series)
        null_count = int(series.isna().sum())
        non_null = series.dropna()

        return {
            "count": total,
            "null_count": null_count,
            "null_pct": round(null_count * 100.0 / max(total, 1), 4),
            "min": non_null.min() if len(non_null) > 0 else None,
            "max": non_null.max() if len(non_null) > 0 else None,
            "approx_distinct": int(series.nunique()),
        }


# Default analyzer set
DEFAULT_ANALYZERS: list[StatAnalyzer] = [
    NumericAnalyzer(),
    TextAnalyzer(),
    BooleanAnalyzer(),
    TimestampAnalyzer(),
]
