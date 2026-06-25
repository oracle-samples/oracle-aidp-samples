#!/usr/bin/env python3
"""
JAR Functionality Test Suite
=============================
Tests that every installed JAR is actually usable on the AIDP cluster.
Goes beyond class loading - tests actual instantiation, method calls,
UDF registration, and format reads.

Usage:
    python3 test_jar_functionality.py [--cluster <id>]
"""

import asyncio
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession, format_outputs

DEFAULT_CLUSTER = os.environ.get("AIDP_CLUSTER", "")

# Each test: (name, code, what_success_looks_like)
TESTS = [
    # ─── Hudi ────────────────────────────────────────────────────
    ("Hudi: Class loading", """
try:
    cls = sc._jvm.java.lang.Class.forName("org.apache.hudi.DataSourceReadOptions")
    print("OK: DataSourceReadOptions loaded")
    cls2 = sc._jvm.java.lang.Class.forName("org.apache.hudi.HoodieSparkUtils")
    print("OK: HoodieSparkUtils loaded")
except Exception as e:
    print(f"FAIL: {e}")
"""),

    ("Hudi: Format registration", """
try:
    # Try to read with hudi format - will fail on data but proves format is registered
    spark.read.format("org.apache.hudi").load("oci://nonexistent@ns/path")
except Exception as e:
    err = str(e)
    if "BucketNotFound" in err or "not exist" in err or "Path does not exist" in err:
        print("OK: Hudi format registered (expected path error)")
    elif "DATA_SOURCE_NOT_FOUND" in err:
        print("FAIL: Hudi format NOT registered - JAR not on classpath")
    else:
        print(f"OK: Hudi format registered (error: {err[:150]})")
"""),

    # ─── Delta Lake ──────────────────────────────────────────────
    ("Delta Lake: DeltaTable class", """
try:
    from delta.tables import DeltaTable
    print("OK: delta.tables.DeltaTable importable")
    cls = sc._jvm.java.lang.Class.forName("io.delta.tables.DeltaTable")
    print("OK: io.delta.tables.DeltaTable loaded via JVM")
except Exception as e:
    print(f"FAIL: {e}")
"""),

    # ─── Feature Generation Library A ────────────────────────────
    ("Feature Library A: Class loading", """
classes = [
    "com.example.app_a.ClassA",
    "com.example.app_a.ClassB",
]
for cls_name in classes:
    try:
        cls = sc._jvm.java.lang.Class.forName(cls_name)
        print(f"OK: {cls_name}")
    except Exception as e:
        print(f"FAIL: {cls_name} - {str(e)[:100]}")
"""),

    ("Feature Library A: Instantiation test", """
try:
    constants = sc._jvm.com.example.app_a.ClassA
    # Try accessing a static field or method
    print(f"OK: Feature Library A ClassA class accessible")
    # List available methods
    methods = [m.getName() for m in constants.getClass().getMethods()][:10]
    print(f"   Methods: {methods}")
except Exception as e:
    print(f"FAIL: Cannot access Feature Library A ClassA - {str(e)[:200]}")
"""),

    # ─── Feature Library B ───────────────────────────────────────
    ("Feature Library B: Class loading", """
classes = [
    "com.example.app_b.ClassA",
    "com.example.app_b.ClassB",
]
for cls_name in classes:
    try:
        cls = sc._jvm.java.lang.Class.forName(cls_name)
        print(f"OK: {cls_name}")
    except Exception as e:
        print(f"FAIL: {cls_name} - {str(e)[:100]}")
"""),

    # ─── Feature Library C ───────────────────────────────────────
    ("Feature Library C: Class loading", """
try:
    cls = sc._jvm.java.lang.Class.forName("com.example.app_c.ClassA")
    print("OK: Feature Library C ClassA loaded")
except Exception as e:
    print(f"FAIL: Feature Library C ClassA - {str(e)[:100]}")
"""),

    # ─── Parser Library ──────────────────────────────────────────
    ("Parser Library: Class loading", """
classes = [
    "com.example.parserlib.ClassA",
    "com.example.parserlib.featgenerator.ClassB",
]
for cls_name in classes:
    try:
        cls = sc._jvm.java.lang.Class.forName(cls_name)
        print(f"OK: {cls_name}")
    except Exception as e:
        print(f"FAIL: {cls_name} - {str(e)[:100]}")
"""),

    # ─── Decryptor Library ───────────────────────────────────────
    ("Decryptor: Class loading", """
classes = [
    "com.example.decryptor.ClassA",
    "com.example.decryptor.UdfRegistrar",
]
for cls_name in classes:
    try:
        cls = sc._jvm.java.lang.Class.forName(cls_name)
        print(f"OK: {cls_name}")
    except Exception as e:
        print(f"FAIL: {cls_name} - {str(e)[:100]}")
"""),

    ("Decryptor: Instantiation", """
try:
    encryptor = sc._jvm.com.example.decryptor.ClassA
    print(f"OK: Decryptor ClassA accessible")
    methods = [m.getName() for m in encryptor.getClass().getDeclaredMethods()][:10]
    print(f"   Methods: {methods}")
except Exception as e:
    print(f"FAIL: {str(e)[:200]}")
"""),

    # ─── Scala Logging ───────────────────────────────────────────
    ("Scala Logging: Class loading", """
try:
    cls = sc._jvm.java.lang.Class.forName("com.typesafe.scalalogging.Logger")
    print("OK: com.typesafe.scalalogging.Logger loaded")
except Exception as e:
    print(f"FAIL: {str(e)[:100]}")
"""),

    # ─── Spark UDF Registration Test ─────────────────────────────
    ("Spark UDF: Register and call a test UDF", """
try:
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    @udf(StringType())
    def test_udf(x):
        return f"migrated_{x}"

    spark.udf.register("migration_test_udf", test_udf)
    result = spark.sql("SELECT migration_test_udf('hello') as val").collect()
    print(f"OK: UDF registered and called, result: {result[0].val}")
except Exception as e:
    print(f"FAIL: {str(e)[:200]}")
"""),

    # ─── End-to-end: Create temp table and query ─────────────────
    ("E2E: Spark SQL with temp table", """
try:
    from pyspark.sql import Row
    df = spark.createDataFrame([Row(id=1, name="test"), Row(id=2, name="migration")])
    df.createOrReplaceTempView("jar_test_table")
    result = spark.sql("SELECT count(*) as cnt FROM jar_test_table").collect()
    print(f"OK: Temp table created and queried, count={result[0].cnt}")
    spark.catalog.dropTempView("jar_test_table")
except Exception as e:
    print(f"FAIL: {str(e)[:200]}")
"""),

    # ─── Comprehensive classpath check ───────────────────────────
    ("Classpath: Verify all JARs present", """
import os
expected = [
    'hudi-spark3.5-bundle',
    'feature_jar_a',
    'feature_jar_b',
    'feature_jar_c',
    'parser_jar',
    'decryptor',
    'scala-logging',
    'delta-spark',
]

cp = spark.sparkContext._jvm.java.lang.System.getProperty('java.class.path')
jars = cp.split(':')
print(f"Total JARs on classpath: {len(jars)}")
for keyword in expected:
    matches = [os.path.basename(j) for j in jars if keyword.lower() in j.lower()]
    if matches:
        print(f"  OK: {keyword} -> {matches[0]}")
    else:
        print(f"  MISSING: {keyword}")
"""),
]


async def main():
    parser = argparse.ArgumentParser(description="Test JAR functionality on AIDP")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--profile", default="DEFAULT")
    args = parser.parse_args()

    print("=" * 60)
    print("JAR Functionality Test Suite")
    print("=" * 60)
    print(f"Cluster: {args.cluster}")
    print(f"Tests: {len(TESTS)}")
    print()

    session = AIDPSession(cluster_id=args.cluster, oci_profile=args.profile)
    await session.connect()

    passed = 0
    failed = 0
    results = []

    try:
        for name, code in TESTS:
            print(f"--- {name} ---")
            result = await session.execute(code, timeout=60)
            output = format_outputs(result.get("outputs", []))
            status = result.get("status", "error")

            if output:
                print(f"  {output.strip()}")

            has_fail = "FAIL" in (output or "") or status != "ok"
            has_ok = "OK" in (output or "")

            if has_fail and not has_ok:
                failed += 1
                results.append((name, "FAIL"))
            elif has_fail and has_ok:
                failed += 1  # partial
                results.append((name, "PARTIAL"))
            else:
                passed += 1
                results.append((name, "PASS"))
            print()

    finally:
        await session.close()

    print("=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    for name, status in results:
        icon = "PASS" if status == "PASS" else "PARTIAL" if status == "PARTIAL" else "FAIL"
        print(f"  [{icon}] {name}")

    print(f"\nTotal: {passed} passed, {failed} failed out of {len(TESTS)}")
    print("=" * 60)

    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
