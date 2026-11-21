"""Local Spark checks; opt in with AIDP_RUN_LOCAL_SPARK_TESTS=1.

Requires Java and permission to bind localhost sockets. No remote writes.
"""

import os
import uuid
from unittest.mock import Mock

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("AIDP_RUN_LOCAL_SPARK_TESTS") != "1",
    reason="Set AIDP_RUN_LOCAL_SPARK_TESTS=1 to start a local Spark JVM",
)
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from test_latest_partition_compat import ORIGINAL_HELPER, load_rewritten


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]")
               .appName("latest-partition-metadata-test")
               .config("spark.ui.enabled", "false")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.driver.host", "127.0.0.1")
               .config("spark.sql.shuffle.partitions", "1")
               .getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def partition_table(spark, tmp_path):
    names = []

    def create(rows, datatype="string"):
        name = "metadata_events_" + uuid.uuid4().hex
        schema = f"value int, partitioning_timestamp {datatype}"
        spark.createDataFrame(rows, schema).write.partitionBy("partitioning_timestamp").option(
            "path", str(tmp_path / name)).saveAsTable(name)
        names.append(name)
        return name

    yield create
    for name in names:
        spark.sql(f"DROP TABLE `{name}`")


@pytest.mark.parametrize("partitions,datatype", [([10, 20, 20, None], "long"),
                                               (["p10", "p20", "p20", None], "string")])
@pytest.mark.parametrize("quote", ["'", '"'])
def test_partitioned_parquet_readback_with_redirect(spark, partition_table, partitions, datatype, quote):
    name = partition_table(list(enumerate(partitions)), datatype)
    source = ORIGINAL_HELPER.replace('= "{timestamp}"', f'= {quote}{{timestamp}}{quote}')
    helper = load_rewritten(spark, source)
    resolver = Mock(return_value="default." + name)
    helper.__globals__["_aidp_rr_resolve"] = resolver
    config_before = spark.conf.get("spark.sql.parquet.enableVectorizedReader")
    group = uuid.uuid4().hex
    spark.sparkContext.setJobGroup(group, "latest partition row-presence probe")
    result = helper("events").cache()
    # A one-row probe preserves max-over-rows semantics even for zero-row files.
    # It must not aggregate the entire source table or inspect older populated groups.
    assert len(spark.sparkContext.statusTracker().getJobIdsForGroup(group)) == 1
    assert spark.conf.get("spark.sql.parquet.enableVectorizedReader") == config_before
    try:
        assert result.count() == 2
        assert sorted(r.value for r in result.collect()) == [1, 2]
        assert result.columns == ["value"]
        resolver.assert_called_once_with("intermediate_tables.events")
    finally:
        result.unpersist()
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)


@pytest.mark.parametrize("rows", [[], [(1, None)]])
def test_empty_or_null_partitions_return_empty_dataframe(spark, partition_table, rows):
    name = partition_table(rows)
    result = load_rewritten(spark)(name, database_name="default")
    assert result.count() == 0
    assert result.columns == ["value"]


@pytest.mark.parametrize("quote", ["'", '"'])
def test_explicit_timestamp_on_spark(spark, partition_table, quote):
    name = partition_table([(1, "10"), (2, "20")])
    source = ORIGINAL_HELPER.replace('= "{timestamp}"', f'= {quote}{{timestamp}}{quote}')
    result = load_rewritten(spark, source)(name, database_name="default", timestamp=10)
    assert [r.value for r in result.collect()] == [1]


def test_encoded_partition_value_and_string_order(spark, partition_table):
    name = partition_table([(1, "9"), (2, "10"), (3, "a/b=c%")])
    result = load_rewritten(spark)(name, database_name="default")
    assert [r.value for r in result.collect()] == [3]


def test_incomplete_partition_with_hidden_task_file_is_not_selected(spark, partition_table):
    name = partition_table([(1, "10")])
    location = next(r[1] for r in spark.sql(f"DESCRIBE TABLE EXTENDED `{name}`").collect()
                    if r[0] == "Location")
    path = spark._jvm.org.apache.hadoop.fs.Path(
        location + "/partitioning_timestamp=20/_temporary/task/part-000.parquet")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    fs.mkdirs(path.getParent())
    fs.create(path).close()
    result = load_rewritten(spark)(name, database_name="default")
    assert [r.value for r in result.collect()] == [1]


@pytest.mark.parametrize("latest_file,older", [("part-000", True), ("part-000", False),
                                               ("part-000.bin", True)])
def test_visible_parquet_files_do_not_require_a_suffix(spark, tmp_path, latest_file, older):
    location = tmp_path / uuid.uuid4().hex
    if older:
        spark.createDataFrame([(10,)], "value int").write.parquet(str(location / "partitioning_timestamp=10"))
    latest = location / "partitioning_timestamp=20"
    spark.createDataFrame([(20,)], "value int").write.parquet(str(latest))
    next(latest.glob("part-*.parquet")).rename(latest / latest_file)
    name = "suffix_events_" + uuid.uuid4().hex
    spark.sql(f"CREATE TABLE `{name}` (value INT, partitioning_timestamp STRING) USING PARQUET "
              f"PARTITIONED BY (partitioning_timestamp) LOCATION '{location}'")
    try:
        spark.sql(f"MSCK REPAIR TABLE `{name}`")
        namespace = {"spark": spark}
        exec(ORIGINAL_HELPER, namespace)
        baseline = namespace["readIntermediateTable"](name, database_name="default")
        result = load_rewritten(spark)(name, database_name="default")
        assert [r.value for r in baseline.collect()] == [20]
        assert result.collect() == baseline.collect()
        assert result.columns == ["value"]
    finally:
        spark.sql(f"DROP TABLE `{name}`")


def test_corrupt_extensionless_latest_file_raises_instead_of_returning_older_data(spark, tmp_path):
    location = tmp_path / uuid.uuid4().hex
    spark.createDataFrame([(10,)], "value int").write.parquet(str(location / "partitioning_timestamp=10"))
    latest = location / "partitioning_timestamp=20"
    latest.mkdir()
    (latest / "part-000").write_bytes(b"not a parquet file")
    name = "corrupt_events_" + uuid.uuid4().hex
    spark.sql(f"CREATE TABLE `{name}` (value INT, partitioning_timestamp STRING) USING PARQUET "
              f"PARTITIONED BY (partitioning_timestamp) LOCATION '{location}'")
    try:
        spark.sql(f"MSCK REPAIR TABLE `{name}`")
        namespace = {"spark": spark}
        exec(ORIGINAL_HELPER, namespace)
        for helper in (namespace["readIntermediateTable"], load_rewritten(spark)):
            with pytest.raises(Exception, match="(?i)parquet|footer"):
                helper(name, database_name="default").collect()
    finally:
        spark.sql(f"DROP TABLE `{name}`")


@pytest.mark.parametrize("datatype", ["int", "bigint", "string"])
@pytest.mark.parametrize("empty_aliases", [(), ("01",), ("01", "1")])
def test_all_directories_with_the_maximum_typed_value_are_read(spark, tmp_path, datatype, empty_aliases):
    location = tmp_path / uuid.uuid4().hex
    for directory, value in [("01", 42), ("1", 99), ("0", 0)]:
        rows = [] if directory in empty_aliases else [(value,)]
        spark.createDataFrame(rows, "value int").write.parquet(
            str(location / ("partitioning_timestamp=" + directory)))
    name = "equivalent_events_" + uuid.uuid4().hex
    spark.sql(f"CREATE TABLE `{name}` (value INT, partitioning_timestamp {datatype}) USING PARQUET "
              f"PARTITIONED BY (partitioning_timestamp) LOCATION '{location}'")
    try:
        spark.sql(f"MSCK REPAIR TABLE `{name}`")
        namespace = {"spark": spark}
        exec(ORIGINAL_HELPER, namespace)
        baseline = namespace["readIntermediateTable"](name, database_name="default")
        result = load_rewritten(spark)(name, database_name="default")
        expected = ([0] if len(empty_aliases) == 2 else
                    [99] if datatype == "string" or empty_aliases else [42, 99])
        assert sorted(r.value for r in baseline.collect()) == expected
        assert sorted(r.value for r in result.collect()) == expected
        assert result.columns == ["value"]
    finally:
        spark.sql(f"DROP TABLE `{name}`")


@pytest.mark.parametrize("older", [True, False])
@pytest.mark.parametrize("datatype", ["string", "int"])
def test_zero_row_latest_parquet_preserves_maximum_over_rows(spark, tmp_path, older, datatype):
    location = tmp_path / uuid.uuid4().hex
    if older:
        spark.createDataFrame([(42,)], "value int").write.parquet(str(location / "partitioning_timestamp=10"))
    spark.createDataFrame([], "value int").write.parquet(str(location / "partitioning_timestamp=20"))
    name = "empty_file_events_" + uuid.uuid4().hex
    spark.sql(f"CREATE TABLE `{name}` (value INT, partitioning_timestamp {datatype}) USING PARQUET "
              f"PARTITIONED BY (partitioning_timestamp) LOCATION '{location}'")
    try:
        spark.sql(f"MSCK REPAIR TABLE `{name}`")
        namespace = {"spark": spark}
        exec(ORIGINAL_HELPER, namespace)
        expected = namespace["readIntermediateTable"](name, database_name="default").collect()
        result = load_rewritten(spark)(name, database_name="default")
        assert result.collect() == expected
        assert [r.value for r in expected] == ([42] if older else [])
    finally:
        spark.sql(f"DROP TABLE `{name}`")
