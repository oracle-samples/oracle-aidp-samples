from __future__ import annotations

import ast
import random
import string
import sys
import types
import unittest
from unittest.mock import patch

from aws_aidp.translate import glue_to_spark
from aws_aidp.translate.glue_to_spark import translate
from tests.stress.helpers import load_fixture


class GlueCorpusTests(unittest.TestCase):
    def test_clean_source_skips_expensive_glue_rules(self):
        source = (
            "from pyspark.sql.types import MapType, StringType\n"
            "schema = MapType(StringType(), StringType())\n"
        ) * 1_000

        def unexpected_rule(*_args):
            self.fail("clean source should not enter the Glue migration rule pipeline")

        with patch.object(glue_to_spark, "_RULES", [unexpected_rule]):
            result = glue_to_spark.translate(source, oci_namespace="ns")

        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.findings, [])

    def test_clean_source_fast_path_still_validates_namespace(self):
        result = translate("value = 1\n", oci_namespace="INVALID")
        self.assertTrue(any(f.rule == "oci_namespace_invalid" for f in result.findings))

    def test_clean_source_fast_path_still_flags_placeholder_oci_paths(self):
        result = translate('target = "oci://bucket@<your-oci-namespace>/key"\n')
        self.assertTrue(any(f.rule == "oci_namespace_missing" for f in result.findings))

    def test_clean_source_fast_path_accepts_real_oci_namespace(self):
        result = translate('target = "oci://bucket@realnamespace/key"\n')
        self.assertFalse(any(f.rule == "oci_namespace_missing" for f in result.findings))

    def test_invalid_unmarked_source_is_validated_before_fast_path(self):
        result = translate("ordinary_call(\n", oci_namespace="ns")
        self.assertTrue(any(f.rule == "source_python_invalid" for f in result.findings))

    def test_every_glue_rule_family_enters_migration_pipeline(self):
        representatives = [
            "from awsglue.context import GlueContext\n",
            "ctx = GlueContext(sc)\n",
            "sc = SparkContext()\n",
            "spark = ctx.spark_session\n",
            "job = Job(ctx)\n",
            "args = getResolvedOptions(argv, names)\n",
            "frame = ctx.create_dynamic_frame.from_catalog()\n",
            "ctx.write_dynamic_frame.from_options()\n",
            "frame = ctx.create_dynamic_frame_from_catalog()\n",
            "ctx.write_dynamic_frame_from_options()\n",
            "frame = DynamicFrame.fromDF(df, ctx, 'frame')\n",
            "source = ctx.getSource()\n",
            "sink = ctx.getSink()\n",
            "frame = ctx.create_data_frame()\n",
            'df.write.parquet("s3://bucket/key")\n',
            *(f"result = {name}.apply(frame=frame)\n" for name, _ in glue_to_spark._FLAG_TRANSFORMS),
            "result = ApplyMapping . apply(frame=frame)\n",
            "result = (Map .\n    apply(frame=frame))\n",
            "result = ApplyMapping \\\n    . apply(frame=frame)\n",
            "result = (ApplyMapping\n    # comment\n    . apply(frame=frame))\n",
            "result = frame.apply_mapping(mappings=[])\n",
            "result = frame.drop_fields(paths=['obsolete'])\n",
            "result = frame.resolveChoice(specs=[])\n",
            "result = frame.rename_field('old', 'new')\n",
            "result = frame.filter(f=lambda row: True)\n",
            "read(transformation_ctx='source')\n",
        ]
        for source in representatives:
            with self.subTest(source=source):
                self.assertTrue(glue_to_spark._requires_glue_migration(source))

    def test_transform_with_python_lexical_gaps_is_flagged(self):
        sources = [
            "result = ApplyMapping \\\n    . apply(frame=frame)\n",
            "result = (ApplyMapping\n    # comment\n    . apply(frame=frame))\n",
        ]
        for source in sources:
            with self.subTest(source=source):
                ast.parse(source)
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "transform_applymapping"
                    for finding in result.findings
                ))

    def test_preexisting_spark_self_assignment_is_preserved(self):
        source = "spark = spark\n"
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)

    def test_all_fixture_outputs_remain_valid_python(self):
        jobs = load_fixture()["sources"]["glue"]["items"]["jobs"]
        for job in jobs:
            with self.subTest(job=job["name"]):
                result = translate(job["script"], oci_namespace="ns")
                ast.parse(result.translated_sql)

    def test_representative_pass_script_executes_with_aidp_contract_stub(self):
        writes = []

        class Writer:
            def mode(self, value):
                self.mode_value = value
                return self

            def parquet(self, path):
                writes.append((self.mode_value, path))

        class Frame:
            write = Writer()

            def toDF(self):
                return self

        class Spark:
            sparkContext = object()

            def table(self, name):
                self.table_name = name
                return Frame()

        spark = Spark()

        class Builder:
            @staticmethod
            def getOrCreate():
                return spark

        class SparkSession:
            builder = Builder()

        class Parameters:
            @staticmethod
            def getParameter(name, default):
                return {"JOB_NAME": "stress"}.get(name, default)

        source = (
            "import sys\n"
            "from awsglue.utils import getResolvedOptions\n"
            "from pyspark.context import SparkContext\n"
            "from awsglue.context import GlueContext\n"
            "args = getResolvedOptions(sys.argv, ['JOB_NAME'])\n"
            "sc = SparkContext()\n"
            "ctx = GlueContext(sc)\n"
            'frame = ctx.create_dynamic_frame.from_catalog(database="db", table_name="table")\n'
            "df = frame.toDF()\n"
            'df.write.mode("overwrite").parquet("s3://bucket/out")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.flags, 0, [str(f) for f in result.findings])

        pyspark = types.ModuleType("pyspark")
        sql = types.ModuleType("pyspark.sql")
        context = types.ModuleType("pyspark.context")
        sql.SparkSession = SparkSession
        context.SparkContext = object
        modules = {"pyspark": pyspark, "pyspark.sql": sql, "pyspark.context": context}
        globals_dict = {"oidlUtils": types.SimpleNamespace(parameters=Parameters())}
        with patch.dict(sys.modules, modules):
            exec(compile(result.translated_sql, "<translated>", "exec"), globals_dict)

        self.assertEqual(spark.table_name, "`db`.`table`")
        self.assertEqual(writes, [("overwrite", "oci://bucket@ns/out")])

    def test_resolved_options_use_documented_required_parameter_contract(self):
        source = "args = getResolvedOptions(sys.argv, ['JOB_NAME'])"
        result = translate(source, oci_namespace="ns")
        self.assertIn("oidlUtils.parameters.getParameter(name, None)", result.translated_sql)
        self.assertNotIn("widgets.get", result.translated_sql)

        class MissingParameters:
            @staticmethod
            def getParameter(name, default):
                return default

        namespace = {
            "oidlUtils": types.SimpleNamespace(parameters=MissingParameters()),
            "sys": types.SimpleNamespace(argv=[]),
        }
        with self.assertRaisesRegex(ValueError, "missing required AIDP job parameter: JOB_NAME"):
            exec(result.translated_sql, namespace)

    def test_s3_uri_in_comment_is_unchanged(self):
        source = "# input is s3://bucket/key\nprint('ok')"
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.changes, 0)

    def test_multiple_input_paths_are_preserved(self):
        source = (
            'x = gc.create_dynamic_frame.from_options(connection_type="s3", '
            'connection_options={"paths":["s3://a/x","s3://b/y"]}, format="json")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn("oci://a@ns/x", result.translated_sql)
        self.assertIn("oci://b@ns/y", result.translated_sql)
        self.assertEqual(result.flags, 0)

    def test_mixed_input_schemes_are_left_unchanged(self):
        source = (
            'x = gc.create_dynamic_frame.from_options(connection_type="s3", '
            'connection_options={"paths":["s3://a/x","file:///tmp/y"]}, format="json")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "read_paths_unhandled" for f in result.findings))

    def test_partition_keys_are_preserved(self):
        source = (
            'gc.write_dynamic_frame.from_options(frame=df, connection_type="s3", '
            'connection_options={"path":"s3://b/x", "partitionKeys":["day","region"]}, '
            'format="parquet")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn('.partitionBy("day", "region")', result.translated_sql)
        self.assertTrue(any(f.rule == "write_disposition" for f in result.findings))
        self.assertNotIn('.mode("overwrite")', result.translated_sql)

    def test_invalid_partition_keys_are_left_unchanged(self):
        source = (
            'gc.write_dynamic_frame.from_options(frame=df, connection_type="s3", '
            'connection_options={"path":"s3://b/x", "partitionKeys":"day"}, '
            'format="parquet")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "partition_keys_unhandled" for f in result.findings))

    def test_complex_write_frame_expression_is_preserved(self):
        source = (
            'gc.write_dynamic_frame.from_options(frame=make_df(a, nested(b, c)), '
            'connection_type="s3", connection_options={"path":"s3://b/x"}, '
            'format="parquet")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn("make_df(a, nested(b, c)).write", result.translated_sql)
        ast.parse(result.translated_sql)

    def test_multiple_write_paths_are_not_silently_reduced(self):
        source = (
            'gc.write_dynamic_frame.from_options(frame=df, connection_type="s3", '
            'connection_options={"paths":["s3://a/x","s3://b/y"]}, format="json")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "write_paths_unhandled" for f in result.findings))

    def test_generic_todf_is_not_deleted_without_dataflow_proof(self):
        source = "result = custom_object.toDF()"
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)

    def test_unrelated_s3_string_is_unchanged_and_forces_review(self):
        source = 'print("documentation: s3://bucket/key")'
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "s3_path_unhandled" for f in result.findings))

    def test_normal_raw_and_fstring_s3_literals_force_review(self):
        sources = [
            'path = "s3://bucket/key"\n',
            'path = r"s3a://bucket/key"\n',
            'path = f"s3://{bucket}/key"\n',
            'path = F"S3A://{bucket}/key"\n',
            'path = "s3" "://bucket/key"\n',
        ]
        for source in sources:
            with self.subTest(source=source):
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "s3_path_unhandled"
                    for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_method_style_dynamicframe_transforms_force_review(self):
        cases = {
            'frame.apply_mapping(mappings=[])': "transform_applymapping",
            'frame.drop_fields(paths=["obsolete"])': "transform_dropfields",
            'frame.resolveChoice(specs=[])': "transform_resolvechoice",
            'frame.rename_field("old", "new")': "transform_renamefield",
            'frame.filter(f=lambda row: True)': "transform_filter",
            'frame.filter(lambda row: row["active"])': "transform_filter",
            'frame.select_fields(paths=["id"])': "transform_selectfields",
            'frame.split_fields(paths=["id"], name1="a", name2="b")':
                "transform_splitfields",
            'frame.unbox(path="payload", format="json")': "transform_unbox",
            'frame.relationalize(root_table_name="root", staging_path=tmp)':
                "transform_relationalize",
            'frame.map(f=lambda row: row)': "transform_map",
            'frame.join(paths1=["id"], paths2=["id"], frame2=other)':
                "transform_join",
        }
        for expression, expected_rule in cases.items():
            with self.subTest(expression=expression):
                source = (
                    'frame = ctx.create_dynamic_frame.from_catalog('
                    'database="raw", table_name="claims")\n'
                    f"out = {expression}\n"
                )
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == expected_rule for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_remaining_dynamicframe_only_methods_force_review(self):
        cases = {
            "frame.drop_null_fields()": "transform_dropnullfields",
            'frame.split_rows({"id": {">": 10}}, "high", "low")':
                "transform_splitrows",
            "frame.mergeDynamicFrame(other, primary_keys=['id'])":
                "transform_mergedynamicframe",
            "frame.spigot('/tmp/sample', {'topk': 10})": "transform_spigot",
            "frame.unnest()": "transform_unnest",
            "frame.unnest_ddb_json()": "transform_unnestddbjson",
            "frame.simplify_ddb_json()": "transform_simplifyddbjson",
            "frame.errorsAsDynamicFrame()": "transform_errorsasdynamicframe",
            "frame.assertErrorThreshold()": "transform_asserterrorthreshold",
            "frame.errorsCount()": "transform_errorscount",
            "frame.stageErrorsCount()": "transform_stageerrorscount",
            "frame.getNumPartitions()": "transform_getnumpartitions",
            "frame.recomputeSchema()": "transform_recomputeschema",
            "frame.schema()": "transform_schema",
            "frame.union(other)": "transform_union",
            "frame.write(connection_type='jdbc')": "transform_write",
            "frame.toDF([])": "transform_todfoptions",
            "frame.toDF(options=[])": "transform_todfoptions",
            "frame.show(num_rows=5)": "transform_showoptions",
        }
        for expression, expected_rule in cases.items():
            with self.subTest(expression=expression):
                source = (
                    'frame = ctx.create_dynamic_frame.from_catalog('
                    'database="raw", table_name="claims")\n'
                    f"out = {expression}\n"
                )
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == expected_rule for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_dataframe_compatible_dynamicframe_methods_pass_through(self):
        source = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="raw", table_name="claims")\n'
            "frame.count()\n"
            "frame.show()\n"
            "frame.show(5)\n"
            "frame.printSchema()\n"
            "coalesced = frame.coalesce(2)\n"
            "repartitioned = coalesced.repartition(3)\n"
            "df = repartitioned.toDF()\n"
        )
        result = translate(source, oci_namespace="ns")
        self.assertFalse(any(
            finding.severity == "flag" for finding in result.findings
        ), [str(finding) for finding in result.findings])

    def test_compatible_partition_methods_preserve_dynamicframe_lineage(self):
        source = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="raw", table_name="claims")\n'
            "partitioned = frame.coalesce(2).repartition(3)\n"
            "out = partitioned.unnest()\n"
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(
            finding.rule == "transform_unnest" for finding in result.findings
        ))

    def test_dynamicframe_lineage_uses_reaching_assignments(self):
        positives = [
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t")\n'
                'frame = frame.drop_fields(paths=["x"])\n'
                'out = frame.select_fields(paths=["id"])\n'
            ),
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t")\n'
                'alias = frame\n'
                'out = alias.drop_fields(paths=["x"])\n'
            ),
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t")\n'
                'out = frame.drop_fields(paths=["x"])\n'
                'frame = None\n'
            ),
            (
                'frame = None\n'
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t")\n'
                'out = frame.drop_fields(paths=["x"])\n'
            ),
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t")\n'
                'def unrelated():\n'
                '    frame = None\n'
                'out = frame.drop_fields(paths=["x"])\n'
            ),
        ]
        for source in positives:
            with self.subTest(source=source):
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "transform_dropfields"
                    for finding in result.findings
                ), [str(finding) for finding in result.findings])

        source = 'frame = None\nout = frame.drop_fields(paths=["x"])\n'
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.flags, 0)

    def test_match_flow_tracks_guards_bindings_and_branch_joins(self):
        exhaustive = (
            'match source_kind:\n'
            '    case "first":\n'
            '        frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="a")\n'
            '    case _:\n'
            '        frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="b")\n'
            'out = frame.drop_fields(paths=["x"])\n'
        )
        subject_and_guard = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t")\n'
            'match frame.drop_fields(paths=["subject"]):\n'
            '    case value if frame.select_fields(paths=["guard"]):\n'
            '        pass\n'
        )
        pattern_shadow = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t")\n'
            'match candidate:\n'
            '    case frame:\n'
            '        out = frame.drop_fields(paths=["x"])\n'
        )

        if sys.version_info < (3, 10):
            for source in (exhaustive, subject_and_guard, pattern_shadow):
                with self.subTest(source=source):
                    result = translate(source, oci_namespace="ns")
                    self.assertEqual(result.translated_sql, source)
                    self.assertTrue(any(
                        finding.rule == "source_python_invalid"
                        for finding in result.findings
                    ), [str(finding) for finding in result.findings])
            return

        self.assertTrue(any(
            finding.rule == "transform_dropfields"
            for finding in translate(exhaustive, oci_namespace="ns").findings
        ))
        rules = {
            finding.rule
            for finding in translate(subject_and_guard, oci_namespace="ns").findings
        }
        self.assertIn("transform_dropfields", rules)
        self.assertIn("transform_selectfields", rules)
        # A match pattern rebinds `frame` to an unknown value, but drop_fields is
        # DynamicFrame-only, so the Glue script flags it without provenance.
        self.assertTrue(any(
            finding.rule == "transform_dropfields"
            for finding in translate(pattern_shadow, oci_namespace="ns").findings
        ))

    def test_dynamicframe_only_methods_in_nested_scopes_cannot_pass(self):
        # A DynamicFrame-only method has no DataFrame counterpart, so in a Glue
        # script it must be flagged even when lineage cannot prove the receiver
        # (function parameters, shadowed locals).
        sources = [
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t")\n'
                'def inspect(frame):\n'
                '    return frame.drop_fields(paths=["x"])\n'
            ),
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t")\n'
                'def inspect():\n'
                '    out = frame.drop_fields(paths=["x"])\n'
                '    frame = None\n'
            ),
        ]
        for source in sources:
            with self.subTest(source=source):
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "transform_dropfields"
                    for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_free_variable_lineage_reaches_function_lambda_and_class_scopes(self):
        suffixes = [
            'def inspect():\n    return frame.drop_fields(paths=["x"])\n',
            'inspect = lambda: frame.drop_fields(paths=["x"])\n',
            'class Pipeline:\n    output = frame.drop_fields(paths=["x"])\n',
        ]
        prefix = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t")\n'
        )
        for suffix in suffixes:
            with self.subTest(suffix=suffix):
                result = translate(prefix + suffix, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "transform_dropfields"
                    for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_method_names_on_non_dynamicframe_receivers_do_not_false_flag(self):
        sources = [
            "out = repository.apply_mapping(mappings=[])\n",
            "out = repository.drop_null_fields()\n",
            "out = repository.errorsAsDynamicFrame()\n",
            "out = repository.getNumPartitions()\n",
            "out = repository.schema()\n",
            "out = repository.toDF([])\n",
            "out = repository.show(num_rows=5)\n",
            "out = repository.union(other)\n",
            "out = repository.write(connection_type='jdbc')\n",
            "out = df.filter(df.active)\n",
            "out = df.filter(condition='active')\n",
        ]
        for source in sources:
            with self.subTest(source=source):
                result = translate(source, oci_namespace="ns")
                self.assertEqual(result.translated_sql, source)
                self.assertEqual(result.flags, 0, [
                    str(finding) for finding in result.findings
                ])

    def test_method_transform_survives_catalog_rewrite_but_cannot_pass(self):
        source = (
            'out = ctx.create_dynamic_frame.from_catalog('
            'database="raw-zone", table_name="claims-2026").apply_mapping('
            'mappings=[])\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn(
            'spark.table("`raw-zone`.`claims-2026`").apply_mapping',
            result.translated_sql,
        )
        self.assertTrue(any(
            finding.rule == "transform_applymapping" for finding in result.findings
        ))

    def test_catalog_identifier_parts_are_individually_quoted(self):
        source = (
            'out = ctx.create_dynamic_frame.from_catalog('
            'database="raw-zone", table_name="claims-2026")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(
            result.translated_sql,
            'out = spark.table("`raw-zone`.`claims-2026`")\n',
        )
        ast.parse(result.translated_sql)

    def test_catalog_identifier_backticks_are_escaped(self):
        source = (
            'out = ctx.create_dynamic_frame.from_catalog('
            'database="raw`zone", table_name="claims`2026")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn(
            'spark.table("`raw``zone`.`claims``2026`")',
            result.translated_sql,
        )

    def test_writing_job_with_bookmark_context_forces_review(self):
        writes = [
            'frame.toDF().write.mode("append").parquet("s3://bucket/out")',
            'frame.toDF().write.mode("overwrite").saveAsTable("curated.claims")',
            'ctx.write_dynamic_frame.from_options('
            'frame=frame, connection_type="s3", '
            'connection_options={"path": "s3://bucket/out"})',
            'frame.write(connection_type="jdbc", '
            'connection_options={"url": "jdbc:test"})',
            'spark.sql("INSERT OVERWRITE TABLE curated.claims '
            'SELECT * FROM source")',
        ]
        for write in writes:
            with self.subTest(write=write):
                source = (
                    'frame = ctx.create_dynamic_frame.from_catalog('
                    'database="raw", table_name="claims", '
                    'transformation_ctx="claims_source")\n'
                    f"{write}\n"
                )
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_bookmark_writer_lineage_uses_reaching_assignments(self):
        sources = [
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t", transformation_ctx="src")\n'
                'frame = frame.drop_fields(paths=["x"])\n'
                'frame.toDF().write.mode("append").saveAsTable("x")\n'
            ),
            (
                'frame = ctx.create_dynamic_frame.from_catalog('
                'database="d", table_name="t", transformation_ctx="src")\n'
                'df = frame.toDF()\n'
                'df.write.mode("append").saveAsTable("x")\n'
                'df = None\n'
            ),
        ]
        for source in sources:
            with self.subTest(source=source):
                result = translate(source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_stored_writer_lineage_and_resets(self):
        prefix = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="src")\n'
            'df = frame.toDF()\n'
        )
        writes = [
            'writer = df.write.mode("append")\nwriter.saveAsTable("x")\n',
            'stream = df.writeStream.format("memory")\nstream.start()\n',
            'v2 = df.writeTo("x").using("parquet")\nv2.append()\n',
            'df = df.filter(df.id > 0)\n'
            'writer = df.write\nwriter.saveAsTable("x")\ndf = None\n',
            # An untyped receiver still counts: after a bookmarked read, a writer
            # chain is output unless proven otherwise (fail closed).
            'rows = df.collect()\nrows.write.parquet("/tmp/report")\n',
        ]
        for write in writes:
            with self.subTest(write=write):
                result = translate(prefix + write, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

        non_outputs = [
            'writer = df.write.mode("append")\n'
            'writer = None\nwriter.saveAsTable("x")\n',
        ]
        for non_output in non_outputs:
            with self.subTest(non_output=non_output):
                result = translate(prefix + non_output, oci_namespace="ns")
                self.assertFalse(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_grouped_aggregation_preserves_dataframe_writer_lineage(self):
        source = (
            'pay = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="pay")\n'
            'df = pay.toDF()\n'
            'grouped = df.groupBy("product_line")\n'
            'pivoted = grouped.pivot("year")\n'
            'agg = pivoted.agg({"amount": "sum"}).withColumnRenamed('
            '"sum(amount)", "total")\n'
            'agg.write.mode("overwrite").parquet("/tmp/output")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(
            finding.rule == "job_bookmark" for finding in result.findings
        ), [str(finding) for finding in result.findings])

    def test_inline_bookmark_source_precedes_chained_output(self):
        source = (
            'ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="src")'
            '.toDF().write.mode("append").saveAsTable("x")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(
            finding.rule == "job_bookmark" for finding in result.findings
        ), [str(finding) for finding in result.findings])

    def test_bookmark_spark_sql_write_classification(self):
        writes = [
            'query = "INSERT INTO curated.claims SELECT * FROM source"\n'
            'spark.sql(query)\n',
            'target = "curated.claims"\n'
            'spark.sql(f"INSERT INTO {target} SELECT * FROM source")\n',
            'spark.sql("-- migration output\\nINSERT OVERWRITE TABLE curated.claims '
            'SELECT * FROM source")\n',
            'spark.sql(sqlQuery="INSERT INTO curated.claims SELECT * FROM source")\n',
            'query = build_sql()\nspark.sql(query)\n',
            'verb = "INSERT"\nspark.sql(f"{verb} INTO curated.claims '
            'SELECT * FROM source")\n',
        ]
        prefix = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="src")\n'
        )
        for write in writes:
            with self.subTest(write=write):
                result = translate(prefix + write, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

        reads = [
            'spark.sql("SELECT * FROM source")\n',
            'query = "-- read only\\nSELECT * FROM source"\nspark.sql(query)\n',
            'spark.sql(sqlQuery=f"SELECT * FROM {table}")\n',
        ]
        for read in reads:
            with self.subTest(read=read):
                result = translate(prefix + read, oci_namespace="ns")
                self.assertFalse(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_bookmark_events_in_function_scopes_ignore_definition_order(self):
        source = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="src")\n'
        )
        output = (
            'def emit():\n'
            '    spark.sql("INSERT INTO curated.claims SELECT * FROM source")\n'
        )
        for script in (source + output, output + source):
            with self.subTest(script=script):
                result = translate(script, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_writeframe_requires_glue_sink_or_glue_only_spelling(self):
        prefix = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="src")\n'
        )
        false_positive = translate(
            prefix + "report.writeFrame()\n", oci_namespace="ns",
        )
        self.assertFalse(any(
            finding.rule == "job_bookmark" for finding in false_positive.findings
        ))

        actual_sink = translate(
            prefix
            + 'sink = ctx.getSink(connection_type="s3")\n'
            + 'sink.writeFrame(frame)\n',
            oci_namespace="ns",
        )
        self.assertTrue(any(
            finding.rule == "job_bookmark" for finding in actual_sink.findings
        ))

        # getSink and write_dynamic_frame exist only on GlueContext, so these
        # spellings identify an output even on an untyped receiver.
        glue_only_outputs = [
            'sink = report.getSink(connection_type="s3")\nsink.writeFrame(frame)\n',
            'report.getSink(connection_type="s3").writeFrame(frame)\n',
            'report.write_dynamic_frame.from_options(frame=frame)\n',
        ]
        for output in glue_only_outputs:
            with self.subTest(output=output):
                result = translate(prefix + output, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_bookmark_flag_policy_requires_stateful_context_and_output(self):
        read_only = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="raw", table_name="claims", transformation_ctx="source")\n'
        )
        empty_context_write = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="raw", table_name="claims", transformation_ctx="")\n'
            'frame.toDF().write.mode("append").saveAsTable("curated.claims")\n'
        )
        diagnostic_write = read_only + 'sys.stdout.write("read complete")\n'
        custom_context = (
            "source = custom_read(transformation_ctx='trace')\n"
            "report.write.parquet('/tmp/report')\n"
        )
        unrelated_writer = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="raw", table_name="claims", transformation_ctx="source")\n'
            "report.write.parquet('/tmp/report')\n"
        )
        custom_result = translate(custom_context, oci_namespace="ns")
        self.assertEqual(custom_result.translated_sql, custom_context)
        self.assertEqual(custom_result.flags, 0)
        # A bookmarked read plus a writer chain on an untyped receiver is
        # reviewed: the receiver may be the frame routed through a helper.
        self.assertTrue(any(
            finding.rule == "job_bookmark"
            for finding in translate(unrelated_writer, oci_namespace="ns").findings
        ))
        for source in (
            read_only,
            empty_context_write,
            diagnostic_write,
            custom_context,
        ):
            with self.subTest(source=source):
                result = translate(source, oci_namespace="ns")
                self.assertFalse(any(
                    finding.rule == "job_bookmark" for finding in result.findings
                ))

    def test_recognized_spark_io_path_is_rewritten(self):
        source = 'df.write.mode("append").parquet("s3://bucket/key")'
        result = translate(source, oci_namespace="ns")
        self.assertIn('parquet("oci://bucket@ns/key")', result.translated_sql)
        self.assertFalse(result.needs_manual_review)

    def test_non_s3_connection_is_flagged(self):
        source = (
            'x = gc.create_dynamic_frame.from_options(connection_type="jdbc", '
            'connection_options={"url":"jdbc:test"}, format="jdbc")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(result.needs_manual_review)
        self.assertEqual(result.translated_sql, source)

    def test_unknown_spark_provider_is_flagged(self):
        source = (
            'x = gc.create_dynamic_frame.from_options(connection_type="s3", '
            'connection_options={"path":"s3://a/x"}, format="xml")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(f.rule == "read_format_unverified" for f in result.findings))

    def test_unhandled_format_options_are_flagged(self):
        source = (
            'x = gc.create_dynamic_frame.from_options(connection_type="s3", '
            'connection_options={"paths":["s3://a/x"]}, format="csv", '
            'format_options={"withHeader": True})'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(result.needs_manual_review)
        self.assertTrue(any(f.rule == "read_options_unhandled" for f in result.findings))

    def test_catalog_predicate_is_flagged(self):
        source = (
            'x = gc.create_dynamic_frame.from_catalog(database="d", table_name="t", '
            'push_down_predicate="day >= 1")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(f.rule == "catalog_options_unhandled" for f in result.findings))

    def test_connection_options_are_flagged(self):
        source = (
            'x = gc.create_dynamic_frame.from_options(connection_type="s3", '
            'connection_options={"paths":["s3://a/x"], "recurse": True}, '
            'format="json")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(f.rule == "connection_options_unhandled" for f in result.findings))

    def test_unknown_top_level_read_argument_is_flagged(self):
        source = (
            'x = gc.create_dynamic_frame.from_options(connection_type="s3", '
            'connection_options={"path":"s3://a/x"}, format="json", mystery=True)'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(f.rule == "read_arguments_unhandled" for f in result.findings))

    def test_glue_call_inside_comment_is_unchanged(self):
        source = (
            '# x = gc.create_dynamic_frame.from_catalog(database="d", table_name="t")\n'
            'print("ok")'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)

    def test_glue_syntax_inside_multiline_string_is_unchanged(self):
        source = (
            'example = """from awsglue.context import GlueContext\n'
            'ctx = GlueContext(sc)\n'
            'job.init(args)\n'
            'x = ctx.create_dynamic_frame.from_catalog(database="d", table_name="t")\n'
            '"""'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)

    def test_multiline_job_lifecycle_removal_preserves_syntax(self):
        source = (
            'job = Job(\n    glueContext\n)\n'
            'job.init(\n    args["JOB_NAME"],\n    args\n)\n'
            'job.commit(\n)\nprint("done")\n'
        )
        result = translate(source, oci_namespace="ns")
        ast.parse(result.translated_sql)
        active = [
            line for line in result.translated_sql.splitlines()
            if not line.lstrip().startswith("#")
        ]
        self.assertFalse(any("Job(" in line or "job.init(" in line for line in active))

    def test_non_glue_commit_method_is_never_removed(self):
        source = "connection.commit()"
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)

    def test_reassigned_job_variable_lifecycle_is_left_for_review(self):
        source = (
            "job = Job(ctx)\n"
            "job = database_connection\n"
            "job.commit()\n"
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn("job.commit()", result.translated_sql)
        self.assertTrue(result.needs_manual_review)

    def test_indented_import_and_lifecycle_removal_preserve_nonempty_suite(self):
        source = (
            "def configure(ctx, args):\n"
            "    from awsglue.job import Job\n"
            "    job = Job(ctx)\n"
            "    job.init(\n"
            "        args,\n"
            "    )\n"
            "    job.commit()\n"
            "    return 7\n"
        )
        result = translate(source, oci_namespace="ns")
        ast.parse(result.translated_sql)
        namespace = {}
        exec(result.translated_sql, namespace)
        self.assertEqual(namespace["configure"](None, {}), 7)

    def test_bootstrap_is_inserted_after_docstring_and_future_import(self):
        source = (
            '"""module docs"""\n'
            "from __future__ import annotations\n"
            "from awsglue.context import GlueContext\n"
            "ctx = GlueContext(sc)\n"
        )
        result = translate(source, oci_namespace="ns")
        ast.parse(result.translated_sql)
        self.assertLess(
            result.translated_sql.index("from __future__ import annotations"),
            result.translated_sql.index("from pyspark.sql import SparkSession"),
        )

    def test_context_rewrite_does_not_consume_chained_expression(self):
        source = "ctx = GlueContext(sc).spark_session"
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "residual_glue_api" for f in result.findings))

    def test_function_local_spark_assignment_does_not_suppress_module_bootstrap(self):
        source = (
            "from awsglue.context import GlueContext\n"
            "def unrelated():\n"
            "    spark = object()\n"
            "ctx = GlueContext(sc)\n"
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn("SparkSession.builder.getOrCreate()", result.translated_sql)
        ast.parse(result.translated_sql)

    def test_aliased_dynamicframe_transform_is_flagged(self):
        source = (
            "from awsglue.transforms import ApplyMapping as AM\n"
            "out = AM.apply(frame=df, mappings=[])\n"
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(f.rule == "transform_applymapping" for f in result.findings))

    def test_aliased_glue_parameter_api_cannot_false_pass(self):
        source = (
            "from awsglue.utils import getResolvedOptions as resolve\n"
            "args = resolve(sys.argv, ['JOB_NAME'])\n"
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(f.rule == "glue_alias_unhandled" for f in result.findings))

    def test_placeholder_namespace_forces_review(self):
        source = 'df.write.parquet("s3://bucket/key")'
        result = translate(source)
        self.assertTrue(any(f.rule == "oci_namespace_missing" for f in result.findings))

    def test_invalid_namespace_cannot_be_injected_into_generated_python(self):
        source = 'df = spark.read.parquet("s3://bucket/path")\n'
        result = translate(source, oci_namespace='bad\"; raise RuntimeError() #')
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "oci_namespace_invalid" for f in result.findings))
        self.assertTrue(any(f.rule == "s3_path_unhandled" for f in result.findings))

    def test_dynamic_catalog_identifiers_are_left_for_review(self):
        source = "x = gc.create_dynamic_frame.from_catalog(database=db, table_name=table)"
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "read_from_catalog" for f in result.findings))

    def test_invalid_source_is_unchanged_and_flagged(self):
        source = 'print("unterminated)\n# s3://bucket/key'
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "source_python_invalid" for f in result.findings))

    def test_dynamicframe_fromdf_preserves_nested_first_argument(self):
        source = 'out = DynamicFrame.fromDF(make_df(a, b), glueContext, "out")'
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.translated_sql, "out = make_df(a, b)")

    def test_translation_is_idempotent(self):
        source = (
            "from awsglue.context import GlueContext\n"
            "ctx = GlueContext(sc)\n"
            'x = ctx.create_dynamic_frame.from_catalog(database="d", table_name="t")\n'
            'x.toDF().write.parquet("s3://b/x")\n'
        )
        once = translate(source, oci_namespace="ns").translated_sql
        twice = translate(once, oci_namespace="ns").translated_sql
        self.assertEqual(twice, once)

    def test_random_text_never_crashes(self):
        rng = random.Random(9911)
        alphabet = string.ascii_letters + string.digits + " ()[]{}'\"-,./:_\n\t"
        for _ in range(1_000):
            source = "".join(rng.choice(alphabet) for _ in range(rng.randrange(0, 300)))
            result = translate(source, oci_namespace="ns")
            self.assertIsInstance(result.translated_sql, str)

    GLUE_HEADER = (
        "from awsglue.context import GlueContext\n"
        "ctx = GlueContext(sc)\n"
    )

    def test_method_form_glue_context_api_translates_like_attribute_form(self):
        header = self.GLUE_HEADER + (
            'frame = ctx.create_dynamic_frame.from_catalog(database="d", table_name="t")\n'
        )
        pairs = [
            (
                'out = ctx.create_dynamic_frame.from_catalog(database="raw", table_name="claims")\n',
                'out = ctx.create_dynamic_frame_from_catalog(database="raw", table_name="claims")\n',
            ),
            (
                'out = ctx.create_dynamic_frame.from_options(connection_type="s3", '
                'connection_options={"paths": ["s3://b/in/"]}, format="parquet")\n',
                'out = ctx.create_dynamic_frame_from_options(connection_type="s3", '
                'connection_options={"paths": ["s3://b/in/"]}, format="parquet")\n',
            ),
            (
                'ctx.write_dynamic_frame.from_options(frame=frame, connection_type="s3", '
                'connection_options={"path": "s3://b/out/"}, format="parquet")\n',
                'ctx.write_dynamic_frame_from_options(frame=frame, connection_type="s3", '
                'connection_options={"path": "s3://b/out/"}, format="parquet")\n',
            ),
        ]
        for attribute_form, method_form in pairs:
            with self.subTest(method_form=method_form):
                expected = translate(header + attribute_form, oci_namespace="ns")
                actual = translate(header + method_form, oci_namespace="ns")
                self.assertEqual(actual.translated_sql, expected.translated_sql)
                self.assertEqual(
                    [str(finding) for finding in actual.findings],
                    [str(finding) for finding in expected.findings],
                )
                self.assertNotIn("_dynamic_frame_from_", actual.translated_sql)

    def test_unrewritten_method_form_glue_context_api_cannot_pass(self):
        sources = [
            'frame = ctx.create_dynamic_frame_from_rdd(rdd, "frame")\n',
            'ctx.write_dynamic_frame_from_catalog(frame=frame, database="curated", table_name="claims")\n',
            'ctx.write_dynamic_frame_from_jdbc_conf(frame=frame, catalog_connection="conn", connection_options={})\n',
        ]
        for source in sources:
            with self.subTest(source=source):
                result = translate(self.GLUE_HEADER + source, oci_namespace="ns")
                self.assertIn(source.split("(")[0].split(" = ")[-1].split(".")[-1], result.translated_sql)
                self.assertTrue(any(
                    finding.rule == "residual_glue_api" for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_method_form_bookmark_write_forces_review(self):
        source = self.GLUE_HEADER + (
            'frame = ctx.create_dynamic_frame_from_catalog('
            'database="raw", table_name="events", transformation_ctx="src")\n'
            'ctx.write_dynamic_frame_from_options(frame=frame, connection_type="s3", '
            'connection_options={"path": "s3://b/out/"}, format="parquet", '
            'transformation_ctx="sink")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(
            finding.rule == "job_bookmark" for finding in result.findings
        ), [str(finding) for finding in result.findings])

    def test_dynamicframe_only_methods_are_flagged_without_lineage_proof(self):
        cases = {
            'def clean(frame):\n    return frame.apply_mapping(mappings=[])\n':
                "transform_applymapping",
            'def clean(frame):\n    return frame.filter(f=lambda row: row["ok"])\n':
                "transform_filter",
            'def load():\n    return ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t")\n\nout = load().resolveChoice(specs=[])\n':
                "transform_resolvechoice",
            'frames = {"a": ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t")}\nout = frames["a"].drop_fields(paths=["x"])\n':
                "transform_dropfields",
            'holder.frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t")\nout = holder.frame.rename_field("a", "b")\n':
                "transform_renamefield",
        }
        for source, expected_rule in cases.items():
            with self.subTest(expected_rule=expected_rule):
                result = translate(self.GLUE_HEADER + source, oci_namespace="ns")
                self.assertTrue(any(
                    finding.rule == expected_rule for finding in result.findings
                ), [str(finding) for finding in result.findings])

    def test_ambiguous_method_names_still_require_dynamicframe_proof(self):
        source = self.GLUE_HEADER + (
            'frame = ctx.create_dynamic_frame.from_catalog(database="d", table_name="t")\n'
            'df = frame.toDF()\n'
            'def summarize(df):\n'
            '    return df.filter(df.active).join(other, "id").union(other).show(5)\n'
            'names = ", ".join(map(str, [1, 2]))\n'
            'parts = df.rdd.getNumPartitions()\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertEqual(result.flags, 0, [str(finding) for finding in result.findings])

    def test_bookmark_read_inside_helper_with_module_level_write_forces_review(self):
        source = self.GLUE_HEADER + (
            'def load():\n'
            '    return ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="src")\n\n'
            'load().toDF().write.mode("append").parquet("s3://b/out/")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(
            finding.rule == "job_bookmark" for finding in result.findings
        ), [str(finding) for finding in result.findings])

    @unittest.skipIf(sys.version_info < (3, 11), "except* requires Python 3.11")
    def test_bookmark_write_inside_try_star_forces_review(self):
        source = self.GLUE_HEADER + (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="d", table_name="t", transformation_ctx="src")\n'
            'try:\n'
            '    frame.toDF().write.mode("append").parquet("s3://b/out/")\n'
            'except* ValueError:\n'
            '    pass\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertTrue(any(
            finding.rule == "job_bookmark" for finding in result.findings
        ), [str(finding) for finding in result.findings])

    def test_s3_uri_in_docstrings_does_not_force_review(self):
        source = (
            '"""Nightly load.\n\nReads s3://bucket/raw/ and publishes curated data."""\n'
            + self.GLUE_HEADER
            + 'def run():\n'
            '    """Writes s3://bucket/curated/ output."""\n'
            '    return 1\n'
            'frame = ctx.create_dynamic_frame.from_catalog(database="d", table_name="t")\n'
            'frame.toDF().write.parquet("s3://bucket/out/")\n'
        )
        result = translate(source, oci_namespace="ns")
        self.assertIn("s3://bucket/raw/", result.translated_sql)
        self.assertIn('parquet("oci://bucket@ns/out/")', result.translated_sql)
        self.assertEqual(result.flags, 0, [str(finding) for finding in result.findings])


if __name__ == "__main__":
    unittest.main()
