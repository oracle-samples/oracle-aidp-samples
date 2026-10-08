"""Output-validation gates for the Athena → Spark SQL translator.

The translator derives its verdict from whether a rewrite rule matched.  That
makes ``flags == 0`` mean "no rule matched", not "this is runnable Spark SQL",
so anything the rule set does not recognise is reported as a clean PASS.

These tests pin the five gates that close that hole.  Each gate has both a
"must flag" case (the bug) and a "must not flag" case, because over-flagging
is its own failure: a review queue full of false alarms trains people to
ignore it.

Every "must flag" case below was executed against Spark 3.5.9 and observed to
fail or return a wrong value.
"""
from __future__ import annotations

import unittest

from aws_aidp.translate.athena_to_spark_sql import translate


def flags(sql: str) -> set[str]:
    return {f.rule for f in translate(sql).findings if f.severity == "flag"}


class MultiStatementGateTests(unittest.TestCase):
    """Gate A — Spark executes one statement per call."""

    def test_two_statements_are_flagged(self):
        # Spark 3.5.9: PARSE_SYNTAX_ERROR ... extra input 'SELECT'
        self.assertIn("multi_statement", flags("SELECT 1; SELECT 2"))

    def test_trailing_semicolon_is_not_flagged(self):
        self.assertNotIn("multi_statement", flags("SELECT 1;"))

    def test_semicolon_inside_a_literal_is_not_a_statement_break(self):
        self.assertNotIn("multi_statement", flags("SELECT 'a;b' AS s FROM t"))

    def test_semicolon_inside_a_comment_is_not_a_statement_break(self):
        self.assertNotIn("multi_statement", flags("-- one; two\nSELECT 1"))


class StatementShapeGateTests(unittest.TestCase):
    """Gate B — the output has to look like a SQL statement at all."""

    def test_garbage_input_is_flagged(self):
        # Spark 3.5.9: PARSE_SYNTAX_ERROR at or near '!'
        self.assertIn("statement_not_recognized", flags("!!!! not sql at all ((("))

    def test_unterminated_string_literal_is_flagged(self):
        # Spark 3.5.9: PARSE_SYNTAX_ERROR at or near '''
        self.assertIn("statement_unbalanced", flags("SELECT 'abc FROM t"))

    def test_unbalanced_parentheses_are_flagged(self):
        self.assertIn("statement_unbalanced", flags("SELECT (a + b FROM t"))

    def test_closing_parenthesis_before_any_opening_is_flagged(self):
        # Counts match (1 and 1) but the order is impossible.
        # Spark 3.5.9 rejects this; a count-only check would wave it through.
        self.assertIn("statement_unbalanced", flags("SELECT )("))

    def test_misordered_parentheses_with_equal_counts_are_flagged(self):
        self.assertIn("statement_unbalanced", flags("SELECT a) FROM (t"))

    def test_balanced_nested_parentheses_are_not_flagged(self):
        self.assertNotIn("statement_unbalanced", flags(
            "SELECT COALESCE((a + b), (c * d)) FROM t WHERE x IN (1, 2)"
        ))

    def test_parenthesis_inside_a_literal_does_not_unbalance(self):
        self.assertNotIn("statement_unbalanced", flags("SELECT ')(' AS s FROM t"))

    def test_statement_ending_in_a_dangling_operator_is_flagged(self):
        """A statement cannot end on a binary operator or conjunction.

        Unlike clause adjacency, this is decidable without a parser: Spark
        3.5.9 rejects every one of these, and no valid statement ends this
        way.
        """
        for sql in ("SELECT a FROM t WHERE a =",
                    "SELECT a FROM t WHERE a AND",
                    "SELECT a FROM t WHERE a OR",
                    "SELECT a FROM t WHERE a >",
                    "SELECT a +",
                    "SELECT a FROM t WHERE a LIKE"):
            with self.subTest(sql=sql):
                self.assertIn("statement_incomplete", flags(sql))

    def test_statement_ending_in_a_comma_or_dangling_by_clause_is_flagged(self):
        """Two more endings that cannot terminate a statement.

        Both verified rejected by Spark 3.5.9.  A two-word BY clause is
        unambiguous -- unlike a lone trailing keyword, it cannot be reread as
        a table alias.
        """
        for sql in ("SELECT a,",
                    "SELECT a FROM t ORDER BY",
                    "SELECT a FROM t GROUP BY",
                    "SELECT a FROM t SORT BY",
                    "SELECT a FROM t CLUSTER BY",
                    "SELECT a FROM t DISTRIBUTE BY"):
            with self.subTest(sql=sql):
                self.assertIn("statement_incomplete", flags(sql))

    def test_trailing_keyword_read_as_an_alias_is_not_flagged(self):
        """Spark rereads a lone trailing keyword as an alias, so these parse.

        `SELECT a FROM` is `SELECT a AS from`; `SELECT a FROM t WHERE` is
        `... FROM t AS where`.  All verified VALID on Spark 3.5.9, so a
        "statement ends on a clause keyword" rule would be unsound.
        """
        for sql in ("SELECT a FROM",
                    "SELECT a FROM t WHERE",
                    "SELECT a FROM t LIMIT",
                    "SELECT from",
                    "SELECT group",
                    "SELECT a, FROM t"):
            with self.subTest(sql=sql):
                self.assertNotIn("statement_incomplete", flags(sql))

    def test_non_reserved_keyword_used_as_a_column_is_not_flagged(self):
        """Spark 3.5's default parser reserves none of these words.

        `SELECT * FROM t WHERE group = 1` is valid Spark -- `group` is an
        ordinary column name -- so no clause heuristic may assume a keyword
        in that position starts a clause.
        """
        for sql in ("SELECT * FROM t WHERE group = 1",
                    "SELECT * FROM t WHERE limit = 1",
                    "SELECT * FROM t WHERE window = 1",
                    "SELECT * FROM t WHERE order = 1",
                    "SELECT group, order FROM t",
                    "SELECT * FROM t WHERE select = 1",
                    "SELECT * FROM t WHERE from = 1"):
            with self.subTest(sql=sql):
                self.assertNotIn("statement_incomplete", flags(sql))

    def test_well_formed_clause_sequences_are_not_flagged(self):
        for sql in ("SELECT * FROM t",
                    "SELECT DISTINCT a FROM t",
                    "SELECT a FROM t WHERE b = 1 GROUP BY a HAVING COUNT(*) > 1 "
                    "ORDER BY a LIMIT 5",
                    "SELECT a FROM t UNION SELECT b FROM u",
                    "SELECT a FROM t UNION ALL SELECT b FROM u",
                    "SELECT COUNT(*) FROM t",
                    "WITH c AS (SELECT 1 AS a) SELECT a FROM c",
                    "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u)",
                    "SELECT * FROM t ORDER BY a DESC LIMIT 10",
                    # A projection made only of a literal or a quoted
                    # identifier must not read as an empty clause body.
                    "SELECT 'say \"hi\"' FROM t",
                    "SELECT `id` FROM `acme-db`.`claims`",
                    "-- \"comment\"\nSELECT 'x' /* \"block\" */ FROM t",
                    "SELECT * FROM VALUES (1, 2) AS v(a, b)"):
            with self.subTest(sql=sql):
                self.assertNotIn("statement_incomplete", flags(sql))

    def test_ordinary_select_is_not_flagged(self):
        found = flags("SELECT a, b FROM t WHERE a > 1")
        self.assertNotIn("statement_not_recognized", found)
        self.assertNotIn("statement_unbalanced", found)

    def test_parenthesised_query_is_recognised(self):
        """A set operation may parenthesise its operands.

        Valid on Spark 3.5.9, but the leading-token check found no word
        before the "(" and reported the statement unrecognised.
        """
        for sql in ("(SELECT a FROM t) UNION ALL (SELECT a FROM u)",
                    "(SELECT a FROM t) UNION (SELECT a FROM u)",
                    "((SELECT a FROM t))",
                    "  ( SELECT a FROM t ) EXCEPT ( SELECT a FROM u )"):
            with self.subTest(sql=sql):
                self.assertNotIn("statement_not_recognized", flags(sql))

    def test_leading_comment_before_select_is_not_flagged(self):
        found = flags("-- a comment\n/* another */\nSELECT 1")
        self.assertNotIn("statement_not_recognized", found)

    def test_non_select_statement_keywords_are_accepted(self):
        for sql in ("SHOW PARTITIONS foo",
                    "MSCK REPAIR TABLE foo",
                    "WITH c AS (SELECT 1) SELECT * FROM c",
                    "INSERT INTO t SELECT 1"):
            with self.subTest(sql=sql):
                self.assertNotIn("statement_not_recognized", flags(sql))


class PrestoOnlySyntaxGateTests(unittest.TestCase):
    """Gate C — grammar Spark's parser rejects outright."""

    CASES = {
        "with_recursive":
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT * FROM r",
        "fetch_first":
            "SELECT * FROM t ORDER BY x FETCH FIRST 5 ROWS WITH TIES",
        "at_time_zone":
            "SELECT ts AT TIME ZONE 'UTC' FROM t",
        "tablesample":
            "SELECT * FROM t TABLESAMPLE BERNOULLI(10)",
        "quantified_comparison":
            "SELECT * FROM t WHERE x > ANY (SELECT y FROM u)",
        "cast_as_json":
            "SELECT CAST(s AS JSON) FROM t",
        "cast_as_row":
            "SELECT CAST(r AS ROW(a INT, b VARCHAR)) FROM t",
        "prepare":
            "PREPARE p FROM SELECT * FROM t WHERE a = 1",
        "unload":
            "UNLOAD (SELECT * FROM t) TO 's3://b/k/' WITH (format='PARQUET')",
        "ctas_with_properties":
            "CREATE TABLE foo WITH (format='PARQUET') AS SELECT * FROM t",
    }

    def test_each_presto_only_construct_is_flagged(self):
        for label, sql in self.CASES.items():
            with self.subTest(construct=label):
                self.assertIn("presto_only_syntax", flags(sql))

    def test_ordinary_spark_compatible_query_is_not_flagged(self):
        self.assertNotIn("presto_only_syntax", flags(
            "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY a LIMIT 10"
        ))

    def test_spark_supported_lookalikes_are_not_flagged(self):
        # Spark 3.5.9 accepts all of these; flagging them would be noise.
        for sql in ("SELECT * FROM t LIMIT ALL",
                    "SELECT a FROM t GROUP BY GROUPING SETS ((a))",
                    "SELECT a FROM t UNION DISTINCT SELECT a FROM u",
                    "SELECT * FROM t WHERE a IS DISTINCT FROM b"):
            with self.subTest(sql=sql):
                self.assertNotIn("presto_only_syntax", flags(sql))


class UnknownFunctionGateTests(unittest.TestCase):
    """Gate D — the function has to exist in Spark 3.5."""

    def test_presto_hashing_idiom_is_flagged(self):
        # Spark 3.5.9: UNRESOLVED_ROUTINE `to_hex`
        self.assertIn("unknown_function", flags(
            "SELECT to_hex(sha256(to_utf8(email))) FROM t"
        ))

    def test_each_unknown_presto_function_is_flagged(self):
        for fn, sql in {
            "geometric_mean": "SELECT geometric_mean(x) FROM t",
            "with_timezone": "SELECT with_timezone(ts, 'UTC') FROM t",
            "at_timezone": "SELECT at_timezone(ts, 'UTC') FROM t",
            "truncate": "SELECT truncate(1.239, 2) FROM t",
            "bitwise_and": "SELECT bitwise_and(a, b) FROM t",
            "infinity": "SELECT infinity() FROM t",
            "codepoint": "SELECT codepoint(s) FROM t",
            "normalize": "SELECT normalize(s) FROM t",
            "json_size": "SELECT json_size(j, '$.a') FROM t",
        }.items():
            with self.subTest(function=fn):
                self.assertIn("unknown_function", flags(sql))

    def test_spark_builtins_are_not_flagged(self):
        self.assertNotIn("unknown_function", flags(
            "SELECT count(*), sum(x), date_format(ts, 'yyyy-MM-dd'), "
            "element_at(arr, 1), regexp_like(s, 'a') FROM t"
        ))

    def test_try_cast_is_not_flagged(self):
        # Valid Spark syntax but parser-level, so absent from SHOW FUNCTIONS.
        self.assertNotIn("unknown_function", flags("SELECT try_cast(s AS INT) FROM t"))

    def test_table_alias_column_list_is_not_mistaken_for_a_call(self):
        # `AS t(signal)` scans as a call to `t(` — observed while measuring the demo.
        self.assertNotIn("unknown_function", flags(
            "SELECT signal FROM claims CROSS JOIN UNNEST(fraud_signals) AS t(signal)"
        ))

    def test_ddl_column_list_is_not_mistaken_for_a_call(self):
        # `CREATE TABLE foo (a INT)` parses cleanly on Spark 3.5.9; the table
        # name sits in front of a column list, not an argument list.
        for sql in ("CREATE TABLE foo (a INT)",
                    "CREATE EXTERNAL TABLE bar (a INT, b STRING) STORED AS PARQUET",
                    "CREATE TABLE IF NOT EXISTS foo (a INT)",
                    "CREATE TABLE warehouse.foo (a INT)",
                    "CREATE OR REPLACE VIEW v (a) AS SELECT 1"):
            with self.subTest(sql=sql):
                self.assertNotIn("unknown_function", flags(sql))

    def test_ddl_clause_keyword_lists_are_not_mistaken_for_calls(self):
        # All of these parse cleanly on Spark 3.5.9; the parenthesis belongs to
        # a DDL clause keyword, not to a function call.
        for sql in ("ALTER TABLE foo ADD COLUMNS (a INT)",
                    "ALTER TABLE foo REPLACE COLUMNS (a INT)",
                    "ALTER TABLE foo ADD PARTITION (dt = '2026-01-01')",
                    "ALTER TABLE foo DROP PARTITION (dt = '2026-01-01')",
                    "CREATE TABLE t2 (a INT) USING parquet OPTIONS (path 'x')",
                    "CREATE TABLE t3 (a INT) TBLPROPERTIES ('k' = 'v')",
                    "CREATE TABLE t4 (a INT) PARTITIONED BY (dt STRING)",
                    "INSERT INTO t PARTITION (dt = '2026-01-01') SELECT 1"):
            with self.subTest(sql=sql):
                self.assertNotIn("unknown_function", flags(sql))

    def test_insert_column_list_is_not_mistaken_for_a_call(self):
        for sql in ("INSERT INTO target (id) SELECT 1",
                    "INSERT INTO warehouse.target (id, name) SELECT 1, 'x'"):
            with self.subTest(sql=sql):
                self.assertNotIn("unknown_function", flags(sql))

    def test_sql_keywords_followed_by_a_paren_are_not_calls(self):
        """Keywords may legally precede "(" without being function calls.

        All valid on Spark 3.5.9.  Reported from review: GROUPING SETS and
        CASE were clean before the gates were added, so these are
        regressions the gate introduced.
        """
        for sql in ("SELECT a, b FROM t GROUP BY GROUPING SETS ((a), (b))",
                    "SELECT CASE WHEN x > 0 THEN (a) ELSE (b) END FROM t",
                    "SELECT a FROM t GROUP BY CUBE (a, b)",
                    "SELECT a FROM t GROUP BY ROLLUP (a, b)",
                    "SELECT s FROM t WHERE s LIKE 'a!_b' ESCAPE '!'"):
            with self.subTest(sql=sql):
                self.assertNotIn("unknown_function", flags(sql))

    def test_table_alias_without_as_is_not_mistaken_for_a_call(self):
        """`AS` is optional before an alias with a column list."""
        for sql in ("SELECT x FROM (VALUES (1)) t(x)",
                    "SELECT tag FROM claims CROSS JOIN UNNEST(arr) u(tag)",
                    "SELECT x FROM (SELECT 1 AS x) sub(x)"):
            with self.subTest(sql=sql):
                self.assertNotIn("unknown_function", flags(sql))

    def test_cte_name_is_not_mistaken_for_a_call(self):
        self.assertNotIn("unknown_function", flags(
            "WITH recent(id) AS (SELECT id FROM t) SELECT * FROM recent"
        ))

    def test_keywords_that_precede_parens_are_not_flagged(self):
        self.assertNotIn("unknown_function", flags(
            "SELECT if(x > 0, 'y', 'n'), array(1, 2), map('k', 'v'), "
            "struct(a, b), cast(x AS INT) FROM t WHERE a IN (1, 2)"
        ))

    def test_function_name_inside_a_literal_or_comment_is_not_flagged(self):
        self.assertNotIn("unknown_function", flags(
            "-- geometric_mean(x)\nSELECT 'to_hex(y)' AS s FROM t"
        ))


class ResidualS3PathGateTests(unittest.TestCase):
    """Gate E — a migrated query must not still point at AWS."""

    def test_ddl_location_keeps_s3_path_and_is_flagged(self):
        self.assertIn("s3_path_unhandled", flags(
            "CREATE EXTERNAL TABLE foo (a INT) STORED AS PARQUET "
            "LOCATION 's3://acme-raw-data/claims/'"
        ))

    def test_s3_literal_in_a_predicate_is_flagged(self):
        self.assertIn("s3_path_unhandled", flags(
            "SELECT * FROM t WHERE src = 's3://acme-raw-data/claims/'"
        ))

    def test_s3a_and_s3n_schemes_are_flagged(self):
        for uri in ("s3a://b/k/", "s3n://b/k/"):
            with self.subTest(uri=uri):
                self.assertIn("s3_path_unhandled", flags(
                    f"SELECT * FROM t WHERE src = '{uri}'"
                ))

    def test_oci_path_is_not_flagged(self):
        self.assertNotIn("s3_path_unhandled", flags(
            "SELECT * FROM t WHERE src = 'oci://bucket@ns/claims/'"
        ))

    def test_query_with_no_paths_is_not_flagged(self):
        self.assertNotIn("s3_path_unhandled", flags("SELECT a FROM t"))


class CleanQueryRegressionTests(unittest.TestCase):
    """The gates must not disturb queries that already translate cleanly."""

    def test_representative_clean_queries_raise_no_gate_flags(self):
        gate_rules = {
            "multi_statement", "statement_not_recognized", "statement_unbalanced",
            "presto_only_syntax", "unknown_function", "s3_path_unhandled",
        }
        for sql in (
            "SELECT customer_id, date_format(order_date, '%Y-%m-%d') AS day, "
            "SUM(premium_amount) AS revenue FROM acme_curated.policy_orders "
            "WHERE order_date >= DATE '2026-01-01' GROUP BY 1, 2",
            "SELECT c.claim_id, signal FROM acme_curated.claims c "
            "CROSS JOIN UNNEST(c.fraud_signals) AS t(signal)",
            "SELECT approx_distinct(policy_id) FROM acme_curated.policies",
        ):
            with self.subTest(sql=sql[:48]):
                self.assertEqual(flags(sql) & gate_rules, set())


if __name__ == "__main__":
    unittest.main(verbosity=2)
