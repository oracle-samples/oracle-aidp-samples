"""Single-pass profiling engine for Spark and Pandas DataFrames.

Batches all column stats into ONE `df.agg()` call for Spark, or one
vectorized pass for Pandas. Top-K for string columns uses a lightweight
separate `groupBy().count()` per column.

Supports granular stat control:
    - Global include/exclude filters
    - Per-column include/exclude overrides
    - Stats excluded at the expression level (never computed)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from qualifire.collection.profiling.analyzers import (
    DEFAULT_ANALYZERS,
    StatAnalyzer,
    TextAnalyzer,
)

logger = logging.getLogger(__name__)

# Type category mapping for Spark DataTypes
_SPARK_TYPE_MAP: dict[str, str] = {
    "ByteType": "numeric",
    "ShortType": "numeric",
    "IntegerType": "numeric",
    "LongType": "numeric",
    "FloatType": "numeric",
    "DoubleType": "numeric",
    "DecimalType": "numeric",
    "StringType": "string",
    "BooleanType": "boolean",
    "TimestampType": "timestamp",
    "TimestampNTZType": "timestamp",
    "DateType": "timestamp",
}


@dataclass
class ColumnProfileOverrides:
    """Per-column stat include/exclude overrides.

    Resolution order:
        1. If column has `stats` set, only those stats are included
        2. Else if column has `exclude_stats`, those are excluded from the global set
        3. Else global stats/exclude_stats apply
    """

    stats: list[str] | None = None
    exclude_stats: list[str] | None = None


@dataclass
class ColumnProfile:
    """Profile results for a single column."""

    column: str
    column_type: str  # "numeric", "string", "boolean", "timestamp", "unsupported"
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProfileResult:
    """Aggregated profile results for a table."""

    table: str
    row_count: int
    columns: dict[str, ColumnProfile] = field(default_factory=dict)

    def to_flat_dict(self) -> dict[str, Any]:
        """Flatten to {col.stat: value} format for CollectionResult compatibility."""
        flat: dict[str, Any] = {"_row_count": self.row_count}
        for col_name, profile in self.columns.items():
            for stat_name, value in profile.stats.items():
                flat[f"{col_name}.{stat_name}"] = value
        return flat


class ProfileEngine:
    """Single-pass profiling engine supporting Spark and Pandas DataFrames.

    Supports granular control over which stats are computed:

        # All stats for all columns (default)
        engine.profile(df)

        # Exclude stats globally
        engine.profile(df, exclude_stats=["top_k", "skewness", "kurtosis"])

        # Include only specific stats globally
        engine.profile(df, stats=["count", "null_count", "null_pct"])

        # Per-column overrides
        engine.profile(df, column_overrides={
            "user_id": ColumnProfileOverrides(stats=["count", "approx_distinct"]),
            "notes": ColumnProfileOverrides(exclude_stats=["top_k"]),
        })

    Args:
        analyzers: List of StatAnalyzer instances. Defaults to all built-in.
    """

    def __init__(self, analyzers: list[StatAnalyzer] | None = None):
        self.analyzers = analyzers or list(DEFAULT_ANALYZERS)

    def profile(
        self,
        df: Any,
        table_name: str = "",
        columns: list[str] | None = None,
        stats: list[str] | None = None,
        exclude_stats: list[str] | None = None,
        column_overrides: dict[str, ColumnProfileOverrides] | None = None,
        filter_expr: str | None = None,
    ) -> ProfileResult:
        """Profile a DataFrame in a single pass.

        Args:
            df: Spark or Pandas DataFrame.
            table_name: Table name for metadata.
            columns: Subset of columns to profile (None = all).
            stats: Global include filter — only these stats are computed (None = all).
            exclude_stats: Global exclude filter — these stats are skipped.
            column_overrides: Per-column include/exclude overrides.
            filter_expr: Optional filter to apply before profiling.

        Returns:
            ProfileResult with per-column stats.
        """
        if self._is_spark_df(df):
            return self._profile_spark(
                df, table_name, columns, stats, exclude_stats, column_overrides, filter_expr
            )
        else:
            return self._profile_pandas(
                df, table_name, columns, stats, exclude_stats, column_overrides, filter_expr
            )

    def _should_include_stat(
        self,
        stat_name: str,
        col_name: str,
        global_stats: list[str] | None,
        global_exclude: list[str] | None,
        column_overrides: dict[str, ColumnProfileOverrides] | None,
    ) -> bool:
        """Determine if a stat should be computed for a column.

        Per-column overrides take precedence over global settings.
        """
        override = (column_overrides or {}).get(col_name)

        if override and override.stats is not None:
            # Column has explicit include list — only those stats
            return stat_name in override.stats

        if override and override.exclude_stats:
            # Column has exclude list — check it first
            if stat_name in override.exclude_stats:
                return False

        # Fall through to global filters
        if global_stats is not None:
            return stat_name in global_stats

        if global_exclude and stat_name in global_exclude:
            return False

        return True

    # ------------------------------------------------------------------
    # Spark implementation
    # ------------------------------------------------------------------

    def _profile_spark(
        self,
        df: Any,
        table_name: str,
        columns: list[str] | None,
        stats: list[str] | None,
        exclude_stats: list[str] | None,
        column_overrides: dict[str, ColumnProfileOverrides] | None,
        filter_expr: str | None,
    ) -> ProfileResult:
        from pyspark.sql import functions as F

        if filter_expr:
            df = df.filter(filter_expr)

        schema = df.schema
        col_types = self._classify_spark_columns(schema, columns)

        # --- Pass 1: Single agg() with only INCLUDED expressions ---
        agg_exprs = []
        col_stat_keys: list[tuple[str, str]] = []

        agg_exprs.append(F.count(F.lit(1)).alias("__row_count__"))

        for col_name, type_category in col_types.items():
            for analyzer in self._analyzers_for_type(type_category):
                for stat_name, expr in analyzer.spark_expressions(col_name):
                    # "quantiles" stat unpacks to p25/p50/p75 — check those names
                    if stat_name == "quantiles":
                        numeric = self._numeric_analyzer()
                        q_names = [f"p{int(q * 100)}" for q in numeric.quantiles] if numeric else []
                        if not any(
                            self._should_include_stat(qn, col_name, stats, exclude_stats, column_overrides)
                            for qn in q_names
                        ):
                            continue
                    elif not self._should_include_stat(
                        stat_name, col_name, stats, exclude_stats, column_overrides
                    ):
                        continue

                    alias = f"{col_name}__{stat_name}"
                    agg_exprs.append(expr.alias(alias))
                    col_stat_keys.append((col_name, stat_name))

        logger.debug(
            "Profiling %d columns with %d expressions in single pass",
            len(col_types), len(agg_exprs) - 1,
        )
        row = df.agg(*agg_exprs).collect()[0].asDict()

        # Parse results
        row_count = row["__row_count__"]
        result = ProfileResult(table=table_name, row_count=row_count)

        for col_name, type_category in col_types.items():
            result.columns[col_name] = ColumnProfile(column=col_name, column_type=type_category)

        for col_name, stat_name in col_stat_keys:
            alias = f"{col_name}__{stat_name}"
            value = row.get(alias)
            if stat_name == "quantiles" and isinstance(value, (list, tuple)):
                numeric = self._numeric_analyzer()
                if numeric:
                    for q, v in zip(numeric.quantiles, value):
                        label = f"p{int(q * 100)}"
                        if self._should_include_stat(
                            label, col_name, stats, exclude_stats, column_overrides
                        ):
                            result.columns[col_name].stats[label] = v
            else:
                result.columns[col_name].stats[stat_name] = value

        # --- Pass 2 (lightweight): Top-K for string columns (only if included) ---
        for col_name, type_category in col_types.items():
            if type_category != "string":
                continue
            if not self._should_include_stat("top_k", col_name, stats, exclude_stats, column_overrides):
                continue
            text_analyzer = self._text_analyzer()
            if text_analyzer and text_analyzer.top_k > 0:
                top_k = (
                    df.groupBy(F.col(f"`{col_name}`"))
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(text_analyzer.top_k)
                    .collect()
                )
                result.columns[col_name].stats["top_k"] = [
                    {"value": r[0], "count": r[1]} for r in top_k
                ]

        return result

    # ------------------------------------------------------------------
    # Pandas implementation
    # ------------------------------------------------------------------

    def _profile_pandas(
        self,
        df: Any,
        table_name: str,
        columns: list[str] | None,
        stats: list[str] | None,
        exclude_stats: list[str] | None,
        column_overrides: dict[str, ColumnProfileOverrides] | None,
        filter_expr: str | None,
    ) -> ProfileResult:
        if filter_expr:
            df = df.query(filter_expr)

        col_types = self._classify_pandas_columns(df, columns)
        row_count = len(df)
        result = ProfileResult(table=table_name, row_count=row_count)

        for col_name, type_category in col_types.items():
            profile = ColumnProfile(column=col_name, column_type=type_category)
            series = df[col_name]

            for analyzer in self._analyzers_for_type(type_category):
                computed = analyzer.pandas_compute(series, col_name)
                # Unpack quantiles dict
                if "quantiles" in computed and isinstance(computed["quantiles"], dict):
                    for label, value in computed["quantiles"].items():
                        if self._should_include_stat(
                            label, col_name, stats, exclude_stats, column_overrides
                        ):
                            profile.stats[label] = value
                    del computed["quantiles"]
                for k, v in computed.items():
                    if self._should_include_stat(
                        k, col_name, stats, exclude_stats, column_overrides
                    ):
                        profile.stats[k] = v

            # Top-K for string columns
            if type_category == "string" and self._should_include_stat(
                "top_k", col_name, stats, exclude_stats, column_overrides
            ):
                text_analyzer = self._text_analyzer()
                if text_analyzer and text_analyzer.top_k > 0:
                    value_counts = series.dropna().value_counts().head(text_analyzer.top_k)
                    profile.stats["top_k"] = [
                        {"value": val, "count": int(cnt)}
                        for val, cnt in value_counts.items()
                    ]

            result.columns[col_name] = profile

        return result

    # ------------------------------------------------------------------
    # Type classification
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_spark_columns(
        schema: Any, columns: list[str] | None
    ) -> dict[str, str]:
        col_types: dict[str, str] = {}
        for f in schema.fields:
            if columns and f.name not in columns:
                continue
            type_name = type(f.dataType).__name__
            if type_name.startswith("Decimal"):
                type_name = "DecimalType"
            category = _SPARK_TYPE_MAP.get(type_name, "unsupported")
            if category != "unsupported":
                col_types[f.name] = category
            else:
                logger.debug("Skipping unsupported column type: %s (%s)", f.name, type_name)
        return col_types

    @staticmethod
    def _classify_pandas_columns(
        df: Any, columns: list[str] | None
    ) -> dict[str, str]:
        import pandas as pd

        col_types: dict[str, str] = {}
        for col in df.columns:
            if columns and col not in columns:
                continue
            dtype = df[col].dtype
            if pd.api.types.is_bool_dtype(dtype):
                col_types[col] = "boolean"
            elif pd.api.types.is_numeric_dtype(dtype):
                col_types[col] = "numeric"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                col_types[col] = "timestamp"
            elif pd.api.types.is_object_dtype(dtype):
                non_null = df[col].dropna()
                if len(non_null) > 0 and all(isinstance(v, bool) for v in non_null):
                    col_types[col] = "boolean"
                else:
                    col_types[col] = "string"
            elif pd.api.types.is_string_dtype(dtype):
                col_types[col] = "string"
            else:
                logger.debug("Skipping unsupported column type: %s (%s)", col, dtype)
        return col_types

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _analyzers_for_type(self, type_category: str) -> list[StatAnalyzer]:
        return [a for a in self.analyzers if type_category in a.applicable_types]

    def _text_analyzer(self) -> TextAnalyzer | None:
        for a in self.analyzers:
            if isinstance(a, TextAnalyzer):
                return a
        return None

    def _numeric_analyzer(self) -> Any:
        from qualifire.collection.profiling.analyzers import NumericAnalyzer

        for a in self.analyzers:
            if isinstance(a, NumericAnalyzer):
                return a
        return None

    @staticmethod
    def _is_spark_df(df: Any) -> bool:
        return hasattr(df, "schema") and hasattr(df, "rdd")
