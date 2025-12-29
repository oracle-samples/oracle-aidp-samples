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


def test_merge_with_deletions_allowing_additional_columns(spark, base_df):
    incremental_data = [
        ("Alice", 25, 20, "North", "update"),    # Update in department_id (belonging to partition category_id=10 and region='West')
        ("Fatima", 25, 20, "West", "insert"),    # A new insert (belonging to partition category_id=20 and region='West')
        ("Charlie", None, 10, "West", "delete")  # A new insert (belonging to partition category_id=10 and region='West')
    ]
    incremental_columns = ["name", "department_id", "category_id", "region", "status"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name", "region"], partition_columns = ["category_id", "region"], deletion_condition = "incremental.status = 'delete'")

    expected_rows = [
        Row(name='David', department_id=30, category_id=10, region='West', status=None),
        Row(name='Eve', department_id=30, category_id=20, region='North', status=None),
        Row(name='Alice', department_id=25, category_id=20, region='North', status='update'),
        Row(name='Fatima', department_id=25, category_id=20, region='West', status='insert')
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))


def test_merge_with_deletions_disallowing_additional_columns(spark, base_df):
    incremental_data = [
        ("Alice", 25, 20, "North", "update"),    # Update in department_id (belonging to partition category_id=10 and region='West')
        ("Fatima", 25, 20, "West", "insert"),    # A new insert (belonging to partition category_id=20 and region='West')
        ("Charlie", None, 10, "West", "delete")  # A new insert (belonging to partition category_id=10 and region='West')
    ]
    incremental_columns = ["name", "department_id", "category_id", "region", "status"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name", "region"], partition_columns = ["category_id", "region"], deletion_condition = "incremental.status = 'delete'", allow_extra_columns = False)

    expected_rows = [
        Row(name='David', department_id=30, category_id=10, region='West'),
        Row(name='Eve', department_id=30, category_id=20, region='North'),
        Row(name='Alice', department_id=25, category_id=20, region='North'),
        Row(name='Fatima', department_id=25, category_id=20, region='West')
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))


def test_merge_with_denying_specific_columns(spark, base_df):
    incremental_data = [
        ("Alice", 25, 20, "North", "update"),    # Update in department_id (belonging to partition category_id=10 and region='West')
        ("Fatima", 25, 20, "West", "insert"),    # A new insert (belonging to partition category_id=20 and region='West')
        ("Charlie", None, 10, "West", "delete")  # A new insert (belonging to partition category_id=10 and region='West')
    ]
    incremental_columns = ["name", "department_id", "category_id", "region", "status"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name", "region"], partition_columns = ["category_id", "region"], deletion_condition = "incremental.status = 'delete'", extra_columns_deny_list = ["status"])

    expected_rows = [
        Row(name='David', department_id=30, category_id=10, region='West'),
        Row(name='Eve', department_id=30, category_id=20, region='North'),
        Row(name='Alice', department_id=25, category_id=20, region='North'),
        Row(name='Fatima', department_id=25, category_id=20, region='West')
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))


def test_merge_with_columns_missing_in_incremental(spark, base_df):
    incremental_data = [
        ("David", 10, "West"),
        ("Eve", 20, "North"),
        ("Fatima", 20, "West")
    ]
    incremental_columns = ["name", "category_id", "region"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name"], partition_columns = ["category_id", "region"])

    expected_rows = [
        Row(name='David', category_id=10, region='West', department_id=None),
        Row(name='Charlie', category_id=10, region='West', department_id=30),
        Row(name='Eve', category_id=20, region='North', department_id=None),
        Row(name='Alice', category_id=20, region='North', department_id=30),
        Row(name='Fatima', category_id=20, region='West', department_id=None)
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))


def test_merge_by_carrying_values_from_base(spark, base_df):
    incremental_data = [
        ("David", 10, "West"),
        ("Eve", 20, "North"),
        ("Fatima", 20, "West")
    ]
    incremental_columns = ["name", "category_id", "region"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name"], partition_columns = ["category_id", "region"], carry_base_values_when_column_missing_list = ["department_id"])

    expected_rows = [
        Row(name='David', category_id=10, region='West', department_id=30),
        Row(name='Charlie', category_id=10, region='West', department_id=30),
        Row(name='Eve', category_id=20, region='North', department_id=30),
        Row(name='Alice', category_id=20, region='North', department_id=30),
        Row(name='Fatima', category_id=20, region='West', department_id=None)
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))


def test_merge_with_specific_columns_to_update(spark, base_df):
    incremental_data = [
        ("David", 25, 20, "West"),  # Update in department_id (belonging to partition category_id=10 and region='West')
        ("Fatima", 25, 20, "West")  # A new insert (belonging to partition category_id=20 and region='West')
    ]
    incremental_columns = ["name", "department_id", "category_id", "region"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name"], partition_columns = ["region"], column_update_allow_list=["department_id"])

    expected_rows = [
        Row(name='Charlie', department_id=30, category_id=10, region='West'),
        Row(name='David', department_id=25, category_id=10, region='West'),
        Row(name='Fatima', department_id=25, category_id=20, region='West')
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))


def test_merge_with_specific_columns_not_to_update(spark, base_df):
    incremental_data = [
        ("David", 25, 10, "West"),  # Update in department_id (belonging to partition category_id=10 and region='West')
        ("Fatima", 25, 20, "West")  # A new insert (belonging to partition category_id=20 and region='West')
    ]
    incremental_columns = ["name", "department_id", "category_id", "region"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name", "region"], partition_columns = ["category_id", "region"], column_update_deny_list=["department_id"])
    merged_df.show()

    from pyspark.sql import Row
    expected_rows = [
        Row(name='Charlie', department_id=30, category_id=10, region='West'),
        Row(name='David', department_id=30, category_id=10, region='West'),
        Row(name='Fatima', department_id=25, category_id=20, region='West')
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))


def test_merge_without_supplying_partitions(spark, base_df):
    incremental_data = [
        ("David", 25, 10, "West"),  # Update in an existing row
        ("Fatima", 25, 20, "West")  # A new insert
    ]
    incremental_columns = ["name", "department_id", "category_id", "region"]
    incremental_df = spark.createDataFrame(incremental_data, incremental_columns)

    merged_df = get_merged_df(base_df, incremental_df, primary_keys = ["name", "region"])

    expected_rows = [
        Row(name='Alice', department_id=30, category_id=20, region='North'),
        Row(name='Bob', department_id=25, category_id=20, region='South'),
        Row(name='Charlie', department_id=30, category_id=10, region='West'),
        Row(name='David', department_id=25, category_id=10, region='West'),
        Row(name='Eve', department_id=30, category_id=20, region='North'),
        Row(name='Fatima', department_id=25, category_id=20, region='West')
    ]

    assertDataFrameEqual(merged_df, spark.createDataFrame(expected_rows))
