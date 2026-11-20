"""Focused tests for the read-only run_on_cluster mutation guard."""

import ast
import pathlib
import re
import unittest


SOURCE = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "agent_migrate.py"


def load_guard():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    wanted_functions = {
        "_static_string_value",
        "_sql_without_comments_or_literals",
        "_sql_mutation_reason",
        "_is_spark_sql_call",
        "_detect_run_on_cluster_mutation",
    }
    wanted_assignments = {
        "_READ_ONLY_SQL_COMMANDS",
        "_MUTATING_SQL_COMMANDS",
        "_SPARK_WRITER_ATTRIBUTES",
    }
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            nodes.append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(
                isinstance(target, ast.Name)
                and target.id in wanted_assignments
                for target in targets
            ):
                nodes.append(node)
    namespace = {"re": re, "frozenset": frozenset}
    exec(compile(ast.Module(nodes, type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace["_detect_run_on_cluster_mutation"]


GUARD = load_guard()


class MutationGuardTests(unittest.TestCase):
    def test_blocks_exact_insert_probe_from_incident(self):
        code = '''
query = f"""INSERT INTO example_feature_backup.loan_offer_o1_v{fl_version}
SELECT * FROM loan_offer_table"""
spark.sql(query)
'''
        self.assertIn("strictly read-only", GUARD(code))

    def test_blocks_dataframe_writer(self):
        self.assertIsNotNone(GUARD('df.write.mode("overwrite").saveAsTable("db.t")'))

    def test_allows_regular_write_method_calls(self):
        self.assertIsNone(GUARD("sys.stdout.write(str(1))"))
        self.assertIsNone(GUARD("fh.write(data)"))

    def test_allows_read_only_sql(self):
        self.assertIsNone(GUARD('spark.sql("SELECT * FROM db.t LIMIT 1").show()'))

    def test_allows_describe_and_schema_probe(self):
        self.assertIsNone(GUARD('spark.sql("DESCRIBE TABLE db.t").show()'))


if __name__ == "__main__":
    unittest.main()
