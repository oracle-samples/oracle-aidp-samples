"""Focused tests for run_on_cluster's hard read-only boundary."""

from __future__ import annotations

import ast
import pathlib
import re
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
AGENT_MIGRATE = REPO / "scripts" / "agent_migrate.py"


def _extract_guard():
    src = AGENT_MIGRATE.read_text(encoding="utf-8")
    tree = ast.parse(src)
    wanted_funcs = {
        "_static_string_value",
        "_sql_without_comments_or_literals",
        "_sql_mutation_reason",
        "_is_spark_sql_call",
        "_detect_run_on_cluster_mutation",
    }
    wanted_assigns = {
        "_READ_ONLY_SQL_COMMANDS",
        "_MUTATING_SQL_COMMANDS",
        "_SPARK_WRITER_ATTRIBUTES",
    }
    picked = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted_funcs:
            picked.append(node)
        elif isinstance(node, ast.Assign):
            if any(
                isinstance(target, ast.Name) and target.id in wanted_assigns
                for target in node.targets
            ):
                picked.append(node)
    module = ast.Module(body=picked, type_ignores=[])
    ns = {"ast": ast, "re": re, "frozenset": frozenset}
    exec(compile(module, str(AGENT_MIGRATE), "exec"), ns)
    return ns["_detect_run_on_cluster_mutation"]


detect = _extract_guard()


class TestMutatingSparkSqlBlocked(unittest.TestCase):

    def assert_blocked(self, code, expected=None):
        message = detect(code)
        self.assertIsNotNone(message, code)
        self.assertIn("BLOCKED", message)
        self.assertIn("read-only", message)
        if expected:
            self.assertIn(expected, message)

    def test_attachment_insert_variable_is_blocked(self):
        self.assert_blocked(
            '''
fl_version = 177
query = f"""
    INSERT INTO example_feature_backup.loan_offer_o1_v{fl_version}
    SELECT * FROM loan_offer_table
"""
spark.sql(query)
''',
            "INSERT",
        )

    def test_direct_ddl_and_dml_are_blocked(self):
        for command in (
            "CREATE TABLE t (id INT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN x INT",
            "MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET *",
            "UPDATE t SET id=1",
            "DELETE FROM t",
            "TRUNCATE TABLE t",
        ):
            with self.subTest(command=command):
                self.assert_blocked(f"spark.sql({command!r})")

    def test_with_insert_is_blocked(self):
        self.assert_blocked(
            'spark.sql("WITH rows AS (SELECT * FROM s) INSERT INTO t SELECT * FROM rows")',
            "INSERT",
        )

    def test_dynamic_sql_fails_closed(self):
        self.assert_blocked("spark.sql(make_query())", "dynamic")

    def test_aidp_sql_alias_is_guarded(self):
        self.assert_blocked('sql("INSERT INTO t VALUES (1)")', "INSERT")


class TestDataFrameWritersBlocked(unittest.TestCase):

    def assert_blocked(self, code, expected=None):
        message = detect(code)
        self.assertIsNotNone(message, code)
        self.assertIn("BLOCKED", message)
        if expected:
            self.assertIn(expected, message)

    def test_batch_writer_chains(self):
        for code in (
            "df.write.mode('overwrite').saveAsTable('t')",
            "df.write.format('delta').save('/tmp/t')",
            "df.write.parquet('/tmp/t')",
            "df.write.insertInto('t')",
        ):
            with self.subTest(code=code):
                self.assert_blocked(code, ".write")

    def test_bare_dataframe_writer_access_is_blocked(self):
        self.assert_blocked("writer = df.write", ".write")

    def test_stream_and_v2_writers(self):
        self.assert_blocked("df.writeStream.format('delta').start('/tmp/t')", ".writeStream")
        self.assert_blocked("df.writeTo('t').append()", ".writeTo")

    def test_dynamic_writer_access(self):
        self.assert_blocked("getattr(df, 'write').parquet('/tmp/t')", ".write")


class TestReadOnlyDiagnosticsAllowed(unittest.TestCase):

    def assert_allowed(self, code):
        self.assertIsNone(detect(code), code)

    def test_read_only_sql(self):
        for command in (
            "SELECT * FROM t LIMIT 1",
            "SHOW TABLES",
            "DESCRIBE TABLE t",
            "WITH rows AS (SELECT * FROM t) SELECT * FROM rows",
            "VALUES (1), (2)",
        ):
            with self.subTest(command=command):
                self.assert_allowed(f"spark.sql({command!r}).show()")

    def test_explain_insert_is_allowed(self):
        self.assert_allowed('spark.sql("EXPLAIN INSERT INTO t SELECT * FROM s").show(False)')

    def test_mutation_words_in_comments_and_literals_do_not_block(self):
        self.assert_allowed(
            '''spark.sql("""
-- INSERT INTO t
SELECT 'DROP TABLE t' AS diagnostic_text
""").show()'''
        )

    def test_dataframe_reads_and_explain_are_allowed(self):
        self.assert_allowed(
            "df = spark.read.parquet('/tmp/t')\n"
            "df.printSchema()\n"
            "df.explain(True)\n"
            "print(df.count())"
        )

    def test_non_spark_write_methods_are_allowed(self):
        self.assert_allowed("sys.stdout.write(str(1))")
        self.assert_allowed(
            "fh = open('/tmp/x', 'w')\n"
            "fh.write(data)"
        )

    def test_refresh_and_set_are_allowed_but_cache_is_not(self):
        self.assert_allowed('spark.sql("REFRESH TABLE db.t")')
        self.assert_allowed('spark.sql("SET spark.sql.shuffle.partitions=8")')
        message = detect('spark.sql("CACHE TABLE db.t")')
        self.assertIsNotNone(message)
        self.assertIn("CACHE", message)

    def test_syntax_error_keeps_normal_path(self):
        self.assert_allowed("spark.sql(")


if __name__ == "__main__":
    unittest.main()
