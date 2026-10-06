"""infa_compat.align_to_table: a write's columns take the target table's
types. On AIDP a Delta MERGE refused a Sequence Generator's BIGINT for a
NUMBER(10,0) key, and three instances of one target appending to a shared
reject table failed on the same mismatch (2026-09-29)."""
from decimal import Decimal

from tests.conftest_spark import spark  # noqa: F401  (fixture; skips without pyspark)

from infa_compat import align_to_table


def test_columns_take_the_table_types_and_the_rest_is_untouched(spark):  # noqa: F811
    spark.createDataFrame([(Decimal("1"), "a")], "CUST_SK decimal(10,0), NAME string") \
        .createOrReplaceTempView("t_align")
    df = spark.createDataFrame([(7, "b", 1.5)], "CUST_SK bigint, NAME string, EXTRA double")
    out = align_to_table(spark, df, "t_align")
    assert dict(out.dtypes) == {"CUST_SK": "decimal(10,0)", "NAME": "string", "EXTRA": "double"}
    assert out.collect()[0]["CUST_SK"] == Decimal("7")


def test_a_missing_table_leaves_the_frame_alone(spark):  # noqa: F811
    df = spark.createDataFrame([(7,)], "CUST_SK bigint")
    assert align_to_table(spark, df, "no_such_table_here") is df
