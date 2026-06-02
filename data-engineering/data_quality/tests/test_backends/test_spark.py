"""Tests for SparkBackend with mocked SparkSession."""

from unittest.mock import MagicMock

import pytest

from tests.conftest import MockDataFrame, MockRow
from qualifire.backends.spark_backend import SparkBackend


class TestSparkBackend:
    def test_execute_sql(self, mock_spark):
        mock_spark.sql.return_value = MockDataFrame([{"count": 42}])
        backend = SparkBackend(mock_spark)
        result = backend.execute_sql("SELECT COUNT(*) AS count FROM t")
        mock_spark.sql.assert_called_once()
        rows = result.collect()
        assert rows[0]["count"] == 42

    def test_get_aggregations(self, mock_spark):
        mock_spark.sql.return_value = MockDataFrame([{"avg_sales": 150.0, "row_count": 1000}])
        backend = SparkBackend(mock_spark)
        result = backend.get_aggregations(
            "db.table", ["AVG(sales) AS avg_sales", "COUNT(*) AS row_count"]
        )
        assert result["avg_sales"] == 150.0
        assert result["row_count"] == 1000

    def test_table_exists_true(self, mock_spark):
        backend = SparkBackend(mock_spark)
        assert backend.table_exists("db.table") is True

    def test_table_exists_false(self, mock_spark):
        mock_spark.table.side_effect = Exception("Table not found")
        backend = SparkBackend(mock_spark)
        assert backend.table_exists("db.missing") is False

    def test_drop_table(self, mock_spark):
        backend = SparkBackend(mock_spark)
        backend.drop_table("db.table")
        mock_spark.sql.assert_called_with("DROP TABLE IF EXISTS db.table")

    def test_get_delta_history_returns_none_on_error(self, mock_spark):
        mock_spark.sql.side_effect = Exception("Not a Delta table")
        backend = SparkBackend(mock_spark)
        assert backend.get_delta_history("non_delta_table") is None

    def test_init_disables_arrow_pyspark_conversion(self, mock_spark):
        """Backend init must turn off Arrow-pandas conversion.

        Workaround for the pandas 3.0.x + pyarrow 24+ regression
        in ``ArrowExtensionArray.to_numpy(dtype=...)`` (the
        AIDP cluster's Python stack). Arrow-backed string /
        Float64 columns produced by ``DataFrame.toPandas()``
        otherwise propagate dtype('O') errors into pattern_check
        and isolation_forest's encode path even after the
        encoder fix landed.

        Pinning here so a future refactor doesn't accidentally
        drop the ``conf.set`` call and silently re-break sample
        validators on impacted clusters.
        """
        SparkBackend(mock_spark)
        mock_spark.conf.set.assert_called_with(
            "spark.sql.execution.arrow.pyspark.enabled", "false"
        )

    def test_init_disables_pandas_future_infer_string(self, mock_spark):
        """Backend init must also flip ``pandas.future.infer_string``
        off. Spark's arrow-pyspark switch only governs ``toPandas()``
        — pandas 3.0+ still constructs arrow-backed string columns
        when a plain ``pd.DataFrame(list_of_dicts)`` runs (forecast /
        drift load history rows that way). Both switches are needed
        to fully steer clear of the pandas 3.0 dtype('O') bug.
        """
        import pandas as pd
        original = pd.get_option("future.infer_string") if "future.infer_string" in (o.key for o in pd._config.config._registered_options.values()) else None
        try:
            SparkBackend(mock_spark)
            assert pd.get_option("future.infer_string") is False
        finally:
            if original is not None:
                pd.set_option("future.infer_string", original)

    def test_init_tolerates_locked_spark_conf(self):
        """If ``conf.set`` raises (locked SparkSession, mock backend
        without a ``conf`` attribute, etc.) the backend must still
        construct — losing the workaround is preferable to crashing
        every Qualifire instantiation."""
        broken_spark = MagicMock()
        broken_spark.conf.set.side_effect = RuntimeError("conf is locked")
        # No exception expected.
        backend = SparkBackend(broken_spark)
        assert backend.spark is broken_spark
