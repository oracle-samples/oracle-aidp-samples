import pytest
from pyspark.sql import SparkSession, Row
from pyspark.testing import assertDataFrameEqual

from utils.merge_generator import get_merged_df


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").appName("merge_generator_tests").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def base_df(spark):
    base_data = [
        ("Alice", 30, 20, "North"),
        ("Bob", 25, 20, "South"),
        ("Charlie", 30, 10, "West"),
        ("David", 30, 10, "West"),
        ("Eve", 30, 20, "North")
    ]
    base_columns = ["name", "department_id", "category_id", "region"]
    return spark.createDataFrame(base_data, base_columns)


def test_merge_with_updates_and_inserts(spark, base_df):
    # Incremental data with updates and new insert
    incremental_data = [
        ("David", 25, 10, "West"),  # Update in department_id (belonging to partition category_id=10 and region='West')
        ("Eve", 25, 20, "North"),   # Update in department_id (belonging to partition category_id=20 and region='North')
        ("Fatima", 25, 20, "West")  # A new insert (belonging to partition category_id=20 and region='West')
    ]
    incremental_columns = ["name", "department_id", "category_id", "region"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(
        base_df,
        incremental_df,
        primary_keys=["name", "region"],
        partition_columns=["category_id", "region"]
    )

    expected_rows = [
        Row(name='David', department_id=25, category_id=10, region='West'),
        Row(name='Charlie', department_id=30, category_id=10, region='West'),
        Row(name='Eve', department_id=25, category_id=20, region='North'),
        Row(name='Alice', department_id=30, category_id=20, region='North'),
        Row(name='Fatima', department_id=25, category_id=20, region='West')
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))