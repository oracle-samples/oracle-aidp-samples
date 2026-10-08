"""GlueContext-only methods must never reach PASS.

The translator rewrites the GlueContext receiver to a SparkSession, so any
GlueContext-only method left in the output resolves against SparkSession at
runtime and raises AttributeError on the job's first executable line -- while a
status-only verdict still reads PASS.

The residual-API check listed three names (getSource, getSink,
create_data_frame), so streaming, purge, transition and transaction jobs
translated silently. These are the jobs most likely to be destructive, which is
the worst place to be quiet.
"""
from __future__ import annotations

import unittest

from aws_aidp.translate.glue_to_spark import translate as glue_translate


PREAMBLE = (
    "import sys\n"
    "from awsglue.context import GlueContext\n"
    "from awsglue.job import Job\n"
    "from pyspark.context import SparkContext\n"
    "sc = SparkContext()\n"
    "glueContext = GlueContext(sc)\n"
    "job = Job(glueContext)\n"
    "job.init('n', {})\n"
)

GLUECONTEXT_ONLY_BODIES = {
    "forEachBatch": "glueContext.forEachBatch(frame, batch_function=f, options={})\n",
    "purge_table": "glueContext.purge_table('db', 'tbl', options={})\n",
    "purge_s3_path": "glueContext.purge_s3_path('oci://b@ns/k', options={})\n",
    "transition_table": "glueContext.transition_table('db', 't', transition_to='GLACIER')\n",
    "transition_s3_path": "glueContext.transition_s3_path('oci://b@ns/k', 'GLACIER')\n",
    "start_transaction": "txid = glueContext.start_transaction(read_only=False)\n",
    "commit_transaction": "glueContext.commit_transaction(txid)\n",
    "cancel_transaction": "glueContext.cancel_transaction(txid)\n",
    "write_from_options": (
        "glueContext.write_from_options(frame_or_dfc=dyf, connection_type='x')\n"
    ),
    "create_data_frame_from_catalog": (
        "df = glueContext.create_data_frame_from_catalog(database='d', table_name='t')\n"
    ),
    "create_data_frame_from_options": (
        "df = glueContext.create_data_frame_from_options(connection_type='s3x')\n"
    ),
    "extract_jdbc_conf": "conf = glueContext.extract_jdbc_conf('my-conn')\n",
    "add_ingestion_time_columns": (
        "out = glueContext.add_ingestion_time_columns(df, 'hour')\n"
    ),
    "getSink": "sink = glueContext.getSink(path='p')\n",
    "getSource": "src = glueContext.getSource(connection_type='s3x', paths=['p'])\n",
    "getSinkWithFormat": "s = glueContext.getSinkWithFormat(connection_type='x')\n",
    "getSampleStreamingDynamicFrame": (
        "s = glueContext.getSampleStreamingDynamicFrame(frame, options={})\n"
    ),
}


class GlueContextOnlyMethodTests(unittest.TestCase):
    def _flags(self, body: str) -> list[str]:
        result = glue_translate(PREAMBLE + body, oci_namespace="ns")
        return [f.rule for f in result.findings if f.severity == "flag"]

    def test_every_gluecontext_only_method_is_flagged(self):
        for name, body in GLUECONTEXT_ONLY_BODIES.items():
            with self.subTest(method=name):
                self.assertIn(
                    "residual_glue_api",
                    self._flags(body),
                    f"{name} reached PASS but raises AttributeError on a SparkSession",
                )

    def test_a_translatable_job_stays_clean(self):
        """The control: a job the translator fully handles must not be flagged,
        so the check above is not simply flagging every Glue job."""
        body = (
            "df = glueContext.create_dynamic_frame.from_catalog("
            "database='d', table_name='t').toDF()\n"
        )
        self.assertNotIn("residual_glue_api", self._flags(body))

    def test_native_spark_foreachbatch_is_not_flagged(self):
        """Structured Streaming's DataStreamWriter.foreachBatch is real Spark and
        must not be confused with GlueContext.forEachBatch."""
        result = glue_translate(
            "q = df.writeStream.foreachBatch(fn).start()\n", oci_namespace="ns"
        )
        self.assertNotIn(
            "residual_glue_api",
            [f.rule for f in result.findings if f.severity == "flag"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
