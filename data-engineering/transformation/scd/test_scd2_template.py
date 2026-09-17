"""Tests for the SCD2 Jinja template in slowly_changing_dimension_template.ipynb.

The notebook is the source of truth: the templates and `run_scd2_merge` are lifted out
of its own cell rather than duplicated here, so these tests fail if the notebook drifts.

What each group covers:

  1. Rendering      -- the emitted SQL uses NULL-safe equality, and the key column is
                       a parameter rather than a hardcoded `customer_id`.
  2. Spark semantics -- the *rendered predicate text* evaluated by a real Spark session.
                       This is the actual bug: `NULL != 'x'` is NULL, which is falsy in
                       both `WHEN MATCHED AND` and `WHERE`, so a NULL -> value change is
                       expired by neither statement and vanishes with no error.
  3. Guard          -- staging uniqueness is enforced before either statement runs.
  4. End-to-end     -- the full expire-then-insert pair against Delta. Marked `delta`
                       because it needs delta-spark and its jars.

Run:
    pip install -r requirements.txt
    pytest test_scd2_template.py -v
    pytest test_scd2_template.py -v -m "not delta"    # skip the Delta end-to-end
"""
import json
import os
import re

import pytest

NOTEBOOK = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "slowly_changing_dimension_template.ipynb"
)

BASE_KW = dict(
    source_table="staging_customer",
    target_table="scd_test.dim_customer",
    scd_keys="target.customer_id = source.customer_id",
    key_column="customer_id",
    tracked_columns=["name", "email", "status"],
    insert_columns=["customer_id", "name", "email", "status"],
)


# ----------------------------------------------------------------------------------
# Load the templates out of the notebook itself
# ----------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def notebook_ns():
    """Exec the notebook cell that defines the templates; return its namespace."""
    with open(NOTEBOOK, encoding="utf-8") as fh:
        nb = json.load(fh)
    src = next(
        (
            "".join(c["source"])
            for c in nb["cells"]
            if c["cell_type"] == "code" and "scd2_template_update" in "".join(c["source"])
        ),
        None,
    )
    assert src, "no cell in the notebook defines scd2_template_update"
    ns = {}
    exec(compile(src, "<scd-template-cell>", "exec"), ns)
    for name in ("scd2_template_update", "scd2_template_insert", "run_scd2_merge"):
        assert name in ns, "notebook cell no longer defines %s" % name
    return ns


@pytest.fixture
def rendered(notebook_ns):
    return (
        notebook_ns["scd2_template_update"].render(**BASE_KW),
        notebook_ns["scd2_template_insert"].render(**BASE_KW),
    )


# ----------------------------------------------------------------------------------
# 1. Rendering
# ----------------------------------------------------------------------------------
def test_change_detection_is_null_safe(rendered):
    """Both statements compare with `<=>`, never a bare `!=`.

    `!=` is what made NULL -> value transitions disappear; this is the regression guard.
    """
    update_sql, insert_sql = rendered
    for label, sql in (("expire", update_sql), ("insert", insert_sql)):
        for col in BASE_KW["tracked_columns"]:
            assert "NOT (target.%s <=> source.%s)" % (col, col) in sql, (
                "%s step does not NULL-safely compare %s" % (label, col)
            )
        assert "!=" not in sql, "%s step still contains a bare != comparison" % label


def test_key_column_is_parameterised(notebook_ns):
    """The "no current row" test uses key_column, not a hardcoded customer_id.

    Only the *predicate* is checked. `customer_id` may still legitimately appear via
    insert_columns, so a blanket substring check would be wrong.
    """
    kw = dict(
        BASE_KW,
        key_column="product_id",
        scd_keys="target.product_id = source.product_id",
        insert_columns=["product_id", "name"],
    )
    insert_sql = notebook_ns["scd2_template_insert"].render(**kw)
    assert "target.product_id IS NULL" in insert_sql
    assert "target.customer_id IS NULL" not in insert_sql, (
        "the no-current-row predicate is still hardcoded to customer_id"
    )


def test_every_tracked_column_is_compared(rendered):
    """No tracked column is silently dropped from either predicate."""
    update_sql, insert_sql = rendered
    for sql in (update_sql, insert_sql):
        assert sql.count("<=>") == len(BASE_KW["tracked_columns"])


# ----------------------------------------------------------------------------------
# 2. The predicate's semantics, in a real Spark session
# ----------------------------------------------------------------------------------
def _delta_available():
    try:
        import delta  # noqa: F401
    except ImportError:
        return False
    return True


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One session for the whole module.

    Spark allows a single active SparkSession per JVM, so `getOrCreate()` in a later test
    returns this one. Delta's extensions are therefore configured here, up front, when
    delta-spark is installed -- otherwise the Delta end-to-end test would silently
    inherit a session without them and fail with a Py4JJavaError depending on test order.
    """
    pytest.importorskip("pyspark", reason="pyspark not installed")
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("scd2_template_tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.warehouse.dir",
            str(tmp_path_factory.mktemp("warehouse")),
        )
    )
    if _delta_available():
        import delta

        builder = (
            builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _predicate_for(sql, col):
    """Pull the rendered `NOT (target.c <=> source.c)` text out of the emitted SQL."""
    m = re.search(r"NOT \(target\.%s <=> source\.%s\)" % (col, col), sql)
    assert m, "predicate for %s not found in rendered SQL" % col
    return m.group(0)


@pytest.mark.parametrize(
    "target_val,source_val,expect_change",
    [
        ("'a@x.com'", "'a@x.com'", False),   # unchanged      -> no-op
        ("'a@x.com'", "'b@x.com'", True),    # value -> value -> expire + insert
        ("NULL", "'b@x.com'", True),         # NULL  -> value -> THE BUG under !=
        ("'a@x.com'", "NULL", True),         # value -> NULL  -> also dropped under !=
        ("NULL", "NULL", False),             # both NULL      -> no-op
    ],
)
def test_rendered_predicate_semantics_in_spark(
    spark, rendered, target_val, source_val, expect_change
):
    """Evaluate the notebook's own rendered predicate in Spark's three-valued logic."""
    predicate = _predicate_for(rendered[0], "email")
    got = spark.sql(
        "SELECT %s AS changed FROM (SELECT %s AS email) target, (SELECT %s AS email) source"
        % (predicate, target_val, source_val)
    ).collect()[0]["changed"]
    assert got is expect_change or got == expect_change, (
        "%s vs %s: expected changed=%s, Spark said %r" % (target_val, source_val, expect_change, got)
    )


@pytest.mark.parametrize("target_val,source_val", [("NULL", "'b@x.com'"), ("'a@x.com'", "NULL")])
def test_neq_predicate_yields_null_and_is_therefore_falsy(spark, target_val, source_val):
    """Documents the bug the `<=>` form fixes.

    `target.email != source.email` evaluates to NULL when either side is NULL. NULL is
    falsy in `WHEN MATCHED AND` and in `WHERE`, so the row is neither expired nor
    re-inserted -- the change is lost with no error raised.
    """
    got = spark.sql(
        "SELECT (target.email != source.email) AS changed "
        "FROM (SELECT %s AS email) target, (SELECT %s AS email) source"
        % (target_val, source_val)
    ).collect()[0]["changed"]
    assert got is None, "expected NULL from the != form, got %r" % got


# ----------------------------------------------------------------------------------
# 3. The staging-uniqueness guard
# ----------------------------------------------------------------------------------
class _FakeDF:
    def __init__(self, rows):
        self._rows = rows

    def count(self):
        return len(self._rows)

    def limit(self, n):
        return _FakeDF(self._rows[:n])

    def collect(self):
        return self._rows


class _FakeSpark:
    """Answers only the duplicate probe; records everything else as executed."""

    def __init__(self, duplicate_keys):
        self._dupes = [(k,) for k in duplicate_keys]
        self.executed = []

    def sql(self, query):
        if "HAVING count(*) > 1" in query:
            return _FakeDF(self._dupes)
        self.executed.append(query)
        return _FakeDF([])


def test_guard_allows_unique_staging(notebook_ns):
    notebook_ns["spark"] = _FakeSpark(duplicate_keys=[])
    notebook_ns["run_scd2_merge"](**BASE_KW)
    assert len(notebook_ns["spark"].executed) == 2, "expected the expire + insert pair"


def test_guard_blocks_duplicate_staging_before_executing_anything(notebook_ns):
    """Duplicates must stop the run, not half-apply it.

    For a key that already has a current row the expire MERGE would abort; for a key with
    no current row nothing aborts and the dimension silently gains two current rows. The
    guard has to fire before either statement runs.
    """
    notebook_ns["spark"] = _FakeSpark(duplicate_keys=[3, 7])
    with pytest.raises(ValueError) as excinfo:
        notebook_ns["run_scd2_merge"](**BASE_KW)
    assert notebook_ns["spark"].executed == [], "statements ran despite duplicate keys"
    assert "3" in str(excinfo.value) and "7" in str(excinfo.value), (
        "the error should name the offending keys: %s" % excinfo.value
    )


# ----------------------------------------------------------------------------------
# 4. End-to-end against Delta
# ----------------------------------------------------------------------------------
@pytest.mark.delta
def test_expire_then_insert_end_to_end(notebook_ns, spark):
    """The four SCD2 paths, executed against Delta. Dana is the NULL -> value case.

    Uses the shared session so Delta's extensions are present regardless of test order.
    Skipped when delta-spark is not installed.
    """
    pytest.importorskip("delta", reason="delta-spark not installed")
    from datetime import datetime

    from pyspark.sql.types import (BooleanType, DateType, IntegerType, StringType,
                                   StructField, StructType)

    try:
        target_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("status", StringType(), True),
            StructField("effective_start_date", DateType(), True),
            StructField("effective_end_date", DateType(), True),
            StructField("current_flag", BooleanType(), True),
        ])
        spark.sql("CREATE SCHEMA IF NOT EXISTS scd_test")
        spark.sql("DROP TABLE IF EXISTS scd_test.dim_customer")
        spark.createDataFrame([
            (1, "Alice", "alice@example.com", "active", datetime(2025, 1, 1), None, True),
            (2, "Bob", "bob@example.com", "active", datetime(2025, 1, 1), None, True),
            (4, "Dana", None, "active", datetime(2025, 1, 1), None, True),
        ], target_schema).write.format("delta").mode("overwrite").saveAsTable(
            "scd_test.dim_customer"
        )

        source_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("status", StringType(), True),
        ])
        spark.createDataFrame([
            (1, "Alice", "alice@example.com", "inactive"),   # tracked column changed
            (2, "Bob", "bob@example.com", "active"),         # identical -> no-op
            (3, "Charlie", "charlie@example.com", "active"), # no current row -> insert
            (4, "Dana", "dana@example.com", "active"),       # NULL -> value
        ], source_schema).createOrReplaceTempView("staging_customer")

        notebook_ns["spark"] = spark
        notebook_ns["run_scd2_merge"](**BASE_KW)

        rows = spark.sql(
            "SELECT customer_id, current_flag FROM scd_test.dim_customer"
        ).collect()
        expired = sorted(r["customer_id"] for r in rows if not r["current_flag"])
        current = sorted(r["customer_id"] for r in rows if r["current_flag"])

        assert expired == [1, 4], "expected Alice and Dana expired, got %s" % expired
        assert current == [1, 2, 3, 4], "every key should have exactly one current row: %s" % current

        # Bob was untouched: still his original start date, one row only.
        bob = [r for r in rows if r["customer_id"] == 2]
        assert len(bob) == 1 and bob[0]["current_flag"], "Bob should not have been rewritten"

        # Re-running is a no-op.
        before = spark.table("scd_test.dim_customer").count()
        notebook_ns["run_scd2_merge"](**BASE_KW)
        assert spark.table("scd_test.dim_customer").count() == before, "second run was not a no-op"
    finally:
        # The session is session-scoped and shared; only clean up what this test made.
        spark.sql("DROP TABLE IF EXISTS scd_test.dim_customer")
        spark.sql("DROP SCHEMA IF EXISTS scd_test")
