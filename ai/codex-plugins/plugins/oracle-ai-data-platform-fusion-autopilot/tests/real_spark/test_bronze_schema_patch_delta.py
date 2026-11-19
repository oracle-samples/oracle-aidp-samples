"""Delta-capable real-Spark leg for bronze schemaPatches (design §5d).

REQUIRED (import failure = test failure) when ``AIDPF_REQUIRE_DELTA_TESTS=1``
(the ``make test-delta`` gate); skips loudly otherwise. Covers the claims a
fake-Spark unit test cannot: the all-nullable landed baseline, patched-vs-
unpatched landed StructType equality (incl. metadata), the §5a guard on
REAL permissive casts, and failed-overwrite atomicity (the prior table
version stays readable).

Run via ``make test-delta`` (isolated venv, cluster-matched pins pyspark
3.5 / delta-spark 3.2, Java 11/17 required).
"""

from __future__ import annotations

import os

import pytest

_REQUIRED = os.environ.get("AIDPF_REQUIRE_DELTA_TESTS") == "1"

if _REQUIRED:
    import delta  # noqa: F401 — hard failure when the gate demands the leg
    import pyspark  # noqa: F401
else:  # pragma: no cover — local convenience
    pyspark = pytest.importorskip(
        "pyspark",
        reason="Delta leg: run `make test-delta` (needs Java + Python<=3.12)",
    )
    delta = pytest.importorskip(
        "delta",
        reason="Delta leg: run `make test-delta` (delta-spark pin missing)",
    )


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("delta-warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("besp-delta-leg")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    # AIDPF_DELTA_LEG_SHARED_SESSION=1 runs the leg against an externally
    # provided Delta-capable session (e.g. an AIDP cluster notebook, the
    # pins' target stack) — stopping THAT session would kill its host.
    if os.environ.get("AIDPF_DELTA_LEG_SHARED_SESSION") != "1":
        session.stop()


def _base_df(spark):
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("Id", LongType(), False),
        StructField("MaterialCost", LongType(), True,
                    {"comment": "load-bearing"}),
        StructField("ItemType", StringType(), True),
    ])
    return spark.createDataFrame(
        [(1, 100, "A"), (2, 0, "B"), (3, None, "C")], schema
    )


def test_all_nullable_landed_baseline(spark) -> None:
    """The live-verified fact the whole design rests on: Delta's
    ``saveAsTable`` lands ALL columns nullable — even a non-nullable
    DataFrame column — for patched and unpatched writes alike."""
    df = _base_df(spark)
    assert df.schema["Id"].nullable is False  # in-flight non-nullable
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("baseline_probe")
    landed = spark.table("baseline_probe").schema
    assert all(f.nullable for f in landed.fields)


def test_patched_landing_equals_unpatched_landing(spark) -> None:
    """NFR-2: cast-back + withMetadata + the single overwrite lands a
    schema EQUAL to an unpatched landing of the same data — names, types,
    metadata, and the shared all-nullable baseline."""
    from pyspark.sql import functions as F

    df = _base_df(spark)
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("unpatched_landing")

    # Simulate the adapter's patched path on the same data: the column
    # arrives as bigint (the patch type), is cast back to bigint's declared
    # type... use decimal(38,0) as the declared type per the live case.
    declared = "decimal(38,0)"
    unpatched_decl = df.withColumn(
        "MaterialCost", F.col("MaterialCost").cast(declared)
    ).withMetadata("MaterialCost", {"comment": "load-bearing"})
    unpatched_decl.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("unpatched_declared")

    patched = (
        df.withColumnRenamed("MaterialCost", "__patch_raw_0")
        .withColumn("MaterialCost", F.col("__patch_raw_0").cast(declared))
        .drop("__patch_raw_0")
        .withMetadata("MaterialCost", {"comment": "load-bearing"})
        .select("Id", "MaterialCost", "ItemType")
    )
    patched.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("patched_landing")

    landed_patched = spark.table("patched_landing").schema
    landed_unpatched = spark.table("unpatched_declared").schema
    assert landed_patched == landed_unpatched
    mc = landed_patched["MaterialCost"]
    assert mc.dataType.simpleString() == declared
    assert dict(mc.metadata) == {"comment": "load-bearing"}


def test_guard_counters_catch_real_permissive_casts(spark) -> None:
    """§5a on REAL casts: overflow → null, fractional double → rounding,
    malformed string → null — each yields a non-zero guard counter; the
    verified bigint→decimal(38,0) pair yields zeros."""
    from pyspark.sql import functions as F

    def counters(df, raw, col, patch_type):
        row = df.agg(
            F.sum(
                F.when(F.col(raw).isNotNull() & F.col(col).isNull(), 1)
                .otherwise(0)
            ).alias("nulled"),
            F.sum(
                F.when(~F.col(col).cast(patch_type).eqNullSafe(F.col(raw)), 1)
                .otherwise(0)
            ).alias("rt"),
        ).collect()[0]
        return (row["nulled"] or 0, row["rt"] or 0)

    # Clean live pair: bigint values → decimal(38,0).
    clean = spark.createDataFrame([(100,), (0,), (None,)], "raw: bigint") \
        .withColumn("val", F.col("raw").cast("decimal(38,0)"))
    assert counters(clean, "raw", "val", "bigint") == (0, 0)

    # Overflow: bigint too wide for decimal(5,0) → silently NULL.
    overflow = spark.createDataFrame([(12345678901,)], "raw: bigint") \
        .withColumn("val", F.col("raw").cast("decimal(5,0)"))
    nulled, _ = counters(overflow, "raw", "val", "bigint")
    assert nulled == 1

    # Fractional double → decimal(38,0) rounds → round-trip mismatch.
    frac = spark.createDataFrame([(1.7,)], "raw: double") \
        .withColumn("val", F.col("raw").cast("decimal(38,0)"))
    _, rt = counters(frac, "raw", "val", "double")
    assert rt == 1

    # Malformed string → int → silently NULL.
    bad = spark.createDataFrame([("abc",)], "raw: string") \
        .withColumn("val", F.col("raw").cast("int"))
    nulled, _ = counters(bad, "raw", "val", "string")
    assert nulled == 1


def test_failed_overwrite_leaves_prior_version_readable(spark) -> None:
    """Round-4 finding 1 (atomicity): a patched landing whose overwrite
    fails mid-job leaves the prior target's data AND schema readable —
    Delta's overwrite is one transaction. The failing v2 write carries a
    MATERIALLY DIFFERENT schema (extra column, changed type, metadata) so
    a schema-mutating atomicity regression cannot hide behind a same-shape
    overwrite, and survival is asserted as exact v1 StructType equality
    plus the original rows."""
    from pyspark.sql import functions as F

    v1 = spark.createDataFrame([(1, "a"), (2, "b")], "Id: bigint, V: string")
    v1.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("atomic_probe")
    v1_schema = spark.table("atomic_probe").schema          # captured BEFORE
    v1_rows = sorted(
        spark.table("atomic_probe").collect(), key=lambda r: r["Id"]
    )

    poisoned = (
        spark.createDataFrame(
            [(3, 1.5, "x", "n"), (4, 2.5, None, "n")],
            "Id: bigint, NewCol: double, V: string, Extra: string",
        )
        .withColumn(
            "V",
            F.when(F.col("V").isNull(),
                   F.raise_error("poisoned row — injected mid-write failure"))
            .otherwise(F.col("V")),
        )
        .withMetadata("Extra", {"note": "schema-changing v2"})
    )
    assert [f.name for f in poisoned.schema.fields] != [
        f.name for f in v1_schema.fields
    ]
    with pytest.raises(Exception):
        poisoned.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable("atomic_probe")

    survivor = spark.table("atomic_probe")
    assert survivor.schema == v1_schema                      # exact StructType
    assert sorted(survivor.collect(), key=lambda r: r["Id"]) == v1_rows
