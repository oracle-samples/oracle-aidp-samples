"""PySpark backend implementation."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class SparkBackend:
    """Backend wrapping a PySpark SparkSession.

    Args:
        spark: Active SparkSession instance.
    """

    def __init__(self, spark: Any):
        self._spark = spark
        # Force-disable Arrow-backed pandas paths. pandas 3.0.x +
        # pyarrow 24+ (the AIDP cluster's stack) has a regression in
        # ``ArrowExtensionArray.to_numpy(dtype=...)`` that surfaces as
        # ``TypeError: Cannot interpret 'dtype('O')' as a data type``
        # the moment a sample/profile/pattern/forecast collector
        # touches a pandas DataFrame with arrow-backed columns. The
        # bug is upstream — qualifire can't encode its way around
        # it because even ``Series.__repr__`` triggers the broken
        # path.
        #
        # Two switches are needed:
        #
        # 1. ``spark.sql.execution.arrow.pyspark.enabled=false`` —
        #    stops ``DataFrame.toPandas()`` from returning arrow-
        #    backed extension dtypes.
        # 2. ``pd.options.future.infer_string=False`` — stops
        #    pandas 3.0+ from creating arrow-backed string columns
        #    when constructing a DataFrame from plain Python objects
        #    (forecast / drift load history rows via
        #    ``pd.DataFrame(list_of_dicts)`` and would otherwise
        #    still hit the same bug even with Spark's arrow off).
        #
        # The cost is a slower toPandas() fallback to plain numpy
        # dtypes, which is fine for the small samples qualifire
        # collects (typically ≤ 300 rows). Setting unconditionally
        # rather than gating on a config knob keeps the surface
        # minimal — operators on a working pandas can re-enable
        # by setting these options back after constructing the
        # backend.
        try:
            spark.conf.set(
                "spark.sql.execution.arrow.pyspark.enabled", "false"
            )
            logger.info(
                "SparkBackend disabled spark.sql.execution.arrow.pyspark.enabled "
                "(pandas 3.0 ArrowExtensionArray bug workaround)"
            )
        except Exception as e:
            logger.debug(
                "Could not set arrow.pyspark.enabled=false on backend init: %s", e
            )

        # Pandas may not be imported on a slim spark-only backend
        # install — guard the option flip behind an import-error
        # tolerance so the backend still constructs.
        try:
            import pandas as _pd

            try:
                _pd.set_option("future.infer_string", False)
                logger.info(
                    "SparkBackend set pandas future.infer_string=False "
                    "(pandas 3.0 arrow string bug workaround)"
                )
            except Exception as e:
                # Pandas < 2.1 doesn't have this option; pandas
                # 2.1+ accepts it. KeyError on unknown option in
                # very old pandas — log + continue.
                logger.debug(
                    "pandas.set_option(future.infer_string) skipped: %s", e
                )
        except ImportError:
            logger.debug(
                "pandas not importable from SparkBackend.__init__; "
                "skipping future.infer_string flip"
            )

    @property
    def spark(self) -> Any:
        return self._spark

    def execute_sql(self, sql: str) -> Any:
        logger.debug("Executing SQL: %s", sql[:200])
        return self._spark.sql(sql)

    def get_table_metadata(self, table: str) -> dict[str, Any]:
        columns = []
        for f in self._spark.table(table).schema.fields:
            columns.append({
                "name": f.name,
                "type": str(f.dataType),
                "nullable": f.nullable,
            })
        # Try to get partitioning info
        partitions: list[str] = []
        try:
            detail = self._spark.sql(f"DESCRIBE DETAIL {table}").collect()
            if detail:
                row = detail[0].asDict()
                partitions = row.get("partitionColumns", []) or []
        except Exception:
            pass
        return {"table": table, "columns": columns, "partitions": partitions}

    def get_column_profile(
        self, table: str, column: str, filter_expr: str | None = None
    ) -> dict[str, Any]:
        where = f" WHERE {filter_expr}" if filter_expr else ""
        sql = f"""
            SELECT
                COUNT(1) AS total_count,
                COUNT(`{column}`) AS non_null_count,
                COUNT(1) - COUNT(`{column}`) AS null_count,
                ROUND((COUNT(1) - COUNT(`{column}`)) * 100.0 / NULLIF(COUNT(1), 0), 4) AS null_pct,
                MIN(`{column}`) AS min_value,
                MAX(`{column}`) AS max_value,
                AVG(CAST(`{column}` AS DOUBLE)) AS mean_value,
                STDDEV(CAST(`{column}` AS DOUBLE)) AS stddev_value,
                APPROX_COUNT_DISTINCT(`{column}`) AS approx_distinct
            FROM {table}{where}
        """
        row = self._spark.sql(sql).collect()[0].asDict()
        return row

    def get_aggregations(
        self, table: str, expressions: list[str], filter_expr: str | None = None
    ) -> dict[str, Any]:
        select_clause = ", ".join(expressions)
        where = f" WHERE {filter_expr}" if filter_expr else ""
        sql = f"SELECT {select_clause} FROM {table}{where}"
        row = self._spark.sql(sql).collect()[0].asDict()
        return row

    def sample_records(
        self, table: str, n: int, filter_expr: str | None = None
    ) -> Any:
        where = f" WHERE {filter_expr}" if filter_expr else ""
        sql = f"SELECT * FROM {table}{where} ORDER BY RAND() LIMIT {n}"
        return self._spark.sql(sql)

    def get_delta_history(self, table: str) -> Any | None:
        try:
            return self._spark.sql(f"DESCRIBE HISTORY {table}")
        except Exception:
            return None

    def write_df(self, df: Any, table: str, options: dict[str, Any] | None = None) -> None:
        options = options or {}
        mode = options.pop("mode", "append")
        partition_by = options.pop("partitionBy", None)

        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)
        for k, v in options.items():
            writer = writer.option(k, v)
        writer.saveAsTable(table)

    def drop_table(self, table: str) -> None:
        self._spark.sql(f"DROP TABLE IF EXISTS {table}")

    def register_table(self, name: str, df: Any) -> None:
        """Register ``df`` as a Spark temp view named ``name``."""
        df.createOrReplaceTempView(name)

    def drop_temp_view(self, name: str) -> None:
        """Drop a Spark temp view (NOT a permanent table). Backticks
        guard view names that contain SQL-special characters.

        Routed through ``execute_sql`` so test backends that mock
        the SQL channel see the call.
        """
        self.execute_sql(f"DROP VIEW IF EXISTS `{name}`")

    def table_exists(self, table: str) -> bool:
        try:
            self._spark.table(table)
            return True
        except Exception:
            return False
