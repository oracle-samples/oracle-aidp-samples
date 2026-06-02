"""Tests for SparkBackend using a real local SparkSession."""

import pytest
from pyspark.sql import SparkSession

from qualifire.backends.spark_backend import SparkBackend


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture
def backend(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW test_sales AS "
              "SELECT * FROM VALUES "
              "(1, 10.0, 'A'), (2, 20.0, 'B'), (3, NULL, 'A'), "
              "(4, 40.0, 'B'), (5, 50.0, 'C') "
              "AS (id, amount, category)")
    return SparkBackend(spark)


class TestSparkBackendReal:
    def test_execute_sql(self, backend):
        df = backend.execute_sql("SELECT COUNT(*) AS cnt FROM test_sales")
        assert df.collect()[0]["cnt"] == 5

    def test_spark_property(self, backend):
        assert backend.spark is not None

    def test_get_table_metadata(self, backend):
        meta = backend.get_table_metadata("test_sales")
        assert meta["table"] == "test_sales"
        col_names = {c["name"] for c in meta["columns"]}
        assert col_names == {"id", "amount", "category"}

    def test_get_column_profile(self, backend):
        profile = backend.get_column_profile("test_sales", "amount")
        assert profile["total_count"] == 5
        assert profile["null_count"] == 1
        assert profile["non_null_count"] == 4
        assert profile["min_value"] == 10.0
        assert profile["max_value"] == 50.0

    def test_get_column_profile_with_filter(self, backend):
        profile = backend.get_column_profile("test_sales", "amount", "category = 'A'")
        assert profile["total_count"] == 2

    def test_get_aggregations(self, backend):
        result = backend.get_aggregations(
            "test_sales",
            ["AVG(amount) AS avg_amt", "COUNT(*) AS cnt"],
        )
        assert result["cnt"] == 5
        assert result["avg_amt"] == 30.0

    def test_get_aggregations_with_filter(self, backend):
        result = backend.get_aggregations(
            "test_sales",
            ["COUNT(*) AS cnt"],
            "category = 'B'",
        )
        assert result["cnt"] == 2

    def test_sample_records(self, backend):
        df = backend.sample_records("test_sales", 3)
        rows = df.collect()
        assert len(rows) == 3

    def test_get_delta_history_returns_none_for_view(self, backend):
        assert backend.get_delta_history("test_sales") is None

    def test_table_exists(self, backend):
        assert backend.table_exists("test_sales") is True
        assert backend.table_exists("nonexistent_table_xyz") is False

    def test_write_df_and_drop(self, backend, spark):
        df = spark.sql("SELECT 1 AS x, 2 AS y")
        backend.write_df(df, "test_write_tmp", {"mode": "overwrite"})
        assert backend.table_exists("test_write_tmp")
        result = backend.execute_sql("SELECT * FROM test_write_tmp").collect()
        assert len(result) == 1
        backend.drop_table("test_write_tmp")

    def test_write_df_with_partition(self, backend, spark):
        df = spark.sql("SELECT 1 AS id, 'A' AS part UNION ALL SELECT 2, 'B'")
        backend.write_df(df, "test_part_tmp", {"mode": "overwrite", "partitionBy": ["part"]})
        assert backend.table_exists("test_part_tmp")
        backend.drop_table("test_part_tmp")
