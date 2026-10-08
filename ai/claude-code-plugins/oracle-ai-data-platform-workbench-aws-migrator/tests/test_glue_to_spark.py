"""Tests for the Glue→Spark translator. Runs standalone (no pytest needed):

    python3 -m tests.test_glue_to_spark

Also pytest-discoverable (functions named test_*).
"""
from __future__ import annotations

from aws_aidp.translate.glue_to_spark import translate, _s3_to_oci


def test_s3_to_oci():
    assert _s3_to_oci("s3://bkt/a/b/", "ns") == "oci://bkt@ns/a/b/"
    assert _s3_to_oci("s3a://bkt/k", "ns") == "oci://bkt@ns/k"
    assert _s3_to_oci("s3://bkt", "ns") == "oci://bkt@ns"
    assert _s3_to_oci("not-a-uri", "ns") == "not-a-uri"


def test_from_catalog_becomes_spark_table():
    src = ('x = glueContext.create_dynamic_frame.from_catalog('
           'database="db", table_name="t", transformation_ctx="x")')
    r = translate(src, oci_namespace="ns")
    assert 'spark.table("`db`.`t`")' in r.translated_sql
    assert "create_dynamic_frame" not in r.translated_sql


def test_write_from_options_becomes_df_write():
    src = ('glueContext.write_dynamic_frame.from_options(frame=out, connection_type="s3", '
           'connection_options={"path": "s3://b/curated/"}, format="parquet")')
    r = translate(src, oci_namespace="ns")
    assert 'out.write.format("parquet").save("oci://b@ns/curated/")' in r.translated_sql
    assert '.mode("overwrite")' not in r.translated_sql
    assert any(f.rule == "write_disposition" for f in r.findings)


def test_glue_imports_and_job_lifecycle_removed():
    src = ("from awsglue.job import Job\n"
           "job = Job(glueContext)\n"
           "job.init(a, b)\n"
           "job.commit()\n")
    r = translate(src, oci_namespace="ns")
    # no awsglue import or Job lifecycle line executes — each is commented out
    for line in r.translated_sql.splitlines():
        if any(s in line for s in ("awsglue", "job.init", "job.commit", "= Job(")):
            assert line.lstrip().startswith("#"), line


def test_applymapping_is_flagged_not_dropped():
    src = 'm = ApplyMapping.apply(frame=x, mappings=[("a","string","a","string")])'
    r = translate(src, oci_namespace="ns")
    assert r.flags >= 1
    assert "ApplyMapping.apply" in r.translated_sql  # left in place for manual rewrite
    assert any(f.rule == "transform_applymapping" for f in r.findings)


def test_todf_and_fromdf_collapse():
    src = ('df = frame.toDF()\n'
           'out = DynamicFrame.fromDF(df, glueContext, "out")\n')
    r = translate(src, oci_namespace="ns")
    # A generic .toDF() may belong to a real DynamicFrame; keeping it is safer
    # than deleting a method call without data-flow proof.
    assert "frame.toDF()" in r.translated_sql
    assert "DynamicFrame.fromDF" not in r.translated_sql
    assert "out = df" in r.translated_sql


def test_inline_fromdf_in_write_resolves_to_df():
    """Regression (found live on a real S3 Glue script): an inline
    frame=DynamicFrame.fromDF(df, ...) inside write_dynamic_frame.from_options
    must resolve to `df.write...`, not the literal `DynamicFrame.write...`."""
    src = ('glueContext.write_dynamic_frame.from_options('
           'frame=DynamicFrame.fromDF(df, glueContext, "out"), '
           'connection_type="s3", connection_options={"path": "s3://b/curated/"}, '
           'format="parquet")')
    r = translate(src, oci_namespace="ns")
    assert 'df.write.format("parquet").save("oci://b@ns/curated/")' in r.translated_sql
    assert "DynamicFrame.write" not in r.translated_sql
    assert "DynamicFrame.fromDF" not in r.translated_sql


def test_clean_job_has_no_flags():
    """A job with no DynamicFrame-only transforms should fully auto-translate."""
    src = ("import sys\n"
           "from awsglue.utils import getResolvedOptions\n"
           "from awsglue.context import GlueContext\n"
           "args = getResolvedOptions(sys.argv, ['JOB_NAME'])\n"
           "glueContext = GlueContext(sc)\n"
           "spark = glueContext.spark_session\n"
           'pay = glueContext.create_dynamic_frame.from_catalog(database="c", table_name="p")\n'
           "df = pay.toDF()\n"
           'df.write.mode("overwrite").parquet("s3://b/agg/")\n')
    r = translate(src, oci_namespace="ns")
    assert r.flags == 0, [str(f) for f in r.findings]
    assert not r.needs_manual_review
    assert 'spark.table("`c`.`p`")' in r.translated_sql
    assert 'oci://b@ns/agg/' in r.translated_sql


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"  ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")


if __name__ == "__main__":
    _run_all()
