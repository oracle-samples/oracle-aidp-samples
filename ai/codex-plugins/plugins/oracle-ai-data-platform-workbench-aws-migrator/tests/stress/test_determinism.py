from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from aws_aidp.migrate import migrate
from aws_aidp.translate.athena_to_spark_sql import translate as athena_translate
from aws_aidp.translate.glue_to_spark import translate as glue_translate
from tests.stress.helpers import athena_asset, glue_asset, plan_with


class DeterminismTests(unittest.TestCase):
    def test_translators_repeat_exactly(self):
        sql = "SELECT array_agg(x), cardinality(a) FROM t"
        py = 'df.write.parquet("s3://bucket/path")\n'
        sql_results = [athena_translate(sql) for _ in range(20)]
        py_results = [glue_translate(py, oci_namespace="ns") for _ in range(20)]
        self.assertEqual(len({r.translated_sql for r in sql_results}), 1)
        self.assertEqual(len({tuple(map(str, r.findings)) for r in sql_results}), 1)
        self.assertEqual(len({r.translated_sql for r in py_results}), 1)
        self.assertEqual(len({tuple(map(str, r.findings)) for r in py_results}), 1)

    def test_artifact_hashes_repeat_across_output_directories(self):
        plan = plan_with(
            athena_asset(query="SELECT cardinality(a) FROM t"),
            glue_asset(script='print("s3://bucket/key")'),
        )
        hashes = []
        with tempfile.TemporaryDirectory() as tmp:
            for i in range(10):
                out = Path(tmp) / str(i)
                report = migrate(plan, out_dir=out, demo=True)
                artifact_hashes = []
                for result in report["results"]:
                    path = out / result["output_path"]
                    artifact_hashes.append(hashlib.sha256(path.read_bytes()).hexdigest())
                hashes.append(tuple(artifact_hashes))
        self.assertEqual(len(set(hashes)), 1)


if __name__ == "__main__":
    unittest.main()
