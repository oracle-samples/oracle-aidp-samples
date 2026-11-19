from __future__ import annotations

import importlib.util
import os
import unittest

from aws_aidp.translate.athena_to_spark_sql import translate
from aws_aidp.translate.glue_to_spark import translate as translate_glue
from tests.stress.helpers import load_fixture


HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None


@unittest.skipUnless(HAS_PYSPARK, "install pyspark to enable Spark parser checks")
class SparkRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[1]").appName(
            "aws-aidp-stress"
        ).getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_runtime_version_matches_pin_when_configured(self):
        expected = os.environ.get("AIDP_SPARK_VERSION")
        if not expected:
            self.skipTest("set AIDP_SPARK_VERSION to enforce the target runtime")
        self.assertTrue(self.spark.version.startswith(expected))

    def test_every_zero_flag_fixture_query_parses(self):
        parser = self.spark._jsparkSession.sessionState().sqlParser()
        queries = load_fixture()["sources"]["athena"]["items"]["named_queries"]
        for query in queries:
            result = translate(query["query"])
            if result.flags:
                continue
            with self.subTest(query=query["name"]):
                parser.parsePlan(result.translated_sql)

    def test_checked_in_spark_builtin_list_matches_this_runtime(self):
        """The generated allowlist must match the engine it claims to describe.

        If it drifts, the unknown_function gate starts inventing or missing
        findings.  Regenerate with scripts/generate_spark_builtins.py.
        """
        import re as _re

        from aws_aidp.translate.spark_builtins import (
            SPARK_BUILTINS, SPARK_KEYWORDS, SPARK_VERSION)

        identifier = _re.compile(r"[a-z_][a-z0-9_]*")
        live = {
            row[0].split(".")[-1].lower()
            for row in self.spark.sql("SHOW FUNCTIONS").collect()
        }
        live = {name for name in live if identifier.fullmatch(name)}
        # CI installs pyspark~=3.5.0, so the patch level floats; the function
        # registry is stable within a minor release.
        self.assertEqual(
            SPARK_VERSION.rsplit(".", 1)[0], self.spark.version.rsplit(".", 1)[0],
            "spark_builtins.py was generated from a different Spark minor release",
        )
        claimed_but_absent = sorted(SPARK_BUILTINS - live)
        present_but_unlisted = sorted(live - SPARK_BUILTINS)
        # Names we claim exist but Spark lacks are the dangerous direction:
        # the gate would wave through a function that fails at runtime.
        self.assertEqual(claimed_but_absent, [],
                         "spark_builtins.py lists functions this Spark lacks; "
                         "rerun scripts/generate_spark_builtins.py")
        # The reverse only causes over-flagging, but still means the list is stale.
        self.assertEqual(present_but_unlisted, [],
                         "this Spark has functions the list is missing; "
                         "rerun scripts/generate_spark_builtins.py")

        # The keyword list guards against false "unknown function" flags on
        # `GROUPING SETS (…)`, `CASE … THEN (…)` and friends, so it has to
        # track the engine too.
        from scripts.generate_spark_builtins import collect_keywords

        self.assertEqual(
            SPARK_KEYWORDS, frozenset(collect_keywords(self.spark)),
            "spark_builtins.py keyword list is stale; "
            "rerun scripts/generate_spark_builtins.py",
        )

    def test_presto_only_syntax_is_genuinely_unparseable_by_spark(self):
        """Everything gate C rejects must actually fail Spark's parser.

        This is the guard against over-flagging: if Spark ever accepts one of
        these, the denylist entry is wrong and should be removed.
        """
        from aws_aidp.translate.athena_to_spark_sql import _PRESTO_ONLY_SYNTAX

        parser = self.spark._jsparkSession.sessionState().sqlParser()
        cases = {
            "WITH RECURSIVE": "WITH RECURSIVE r(n) AS (SELECT 1) SELECT * FROM r",
            "FETCH FIRST/NEXT": "SELECT * FROM t ORDER BY x FETCH FIRST 5 ROWS WITH TIES",
            "AT TIME ZONE": "SELECT ts AT TIME ZONE 'UTC' FROM t",
            "TABLESAMPLE BERNOULLI/SYSTEM": "SELECT * FROM t TABLESAMPLE BERNOULLI(10)",
            "quantified comparison": "SELECT * FROM t WHERE x > ANY (SELECT y FROM u)",
            "CAST AS JSON": "SELECT CAST(s AS JSON) FROM t",
            "CAST AS ROW": "SELECT CAST(r AS ROW(a INT)) FROM t",
            "PREPARE/EXECUTE/DEALLOCATE": "PREPARE p FROM SELECT * FROM t",
            "UNLOAD": "UNLOAD (SELECT * FROM t) TO 's3://b/k/' WITH (format='PARQUET')",
            "CTAS WITH (properties)":
                "CREATE TABLE foo WITH (format='PARQUET') AS SELECT * FROM t",
        }
        # every denylist entry must be exercised here
        self.assertEqual({label for label, _, _ in _PRESTO_ONLY_SYNTAX}, set(cases))

        for label, sql in cases.items():
            with self.subTest(construct=label):
                self.assertTrue(translate(sql).flags, "gate C did not flag it")
                with self.assertRaises(Exception):
                    parser.parsePlan(sql)

    def test_functions_flagged_as_unknown_are_genuinely_absent_from_spark(self):
        """Gate D must only flag names this engine really lacks."""
        live = {
            row[0].split(".")[-1].lower()
            for row in self.spark.sql("SHOW FUNCTIONS").collect()
        }
        cases = [
            "SELECT to_hex(sha256(to_utf8(email))) FROM t",
            "SELECT geometric_mean(x) FROM t",
            "SELECT with_timezone(ts, 'UTC') FROM t",
            "SELECT bitwise_and(a, b) FROM t",
            "SELECT codepoint(s) FROM t",
            "SELECT json_size(j, '$.a') FROM t",
            "SELECT regexp_split(code, ',') FROM t",
        ]
        for sql in cases:
            result = translate(sql)
            flagged = [
                f.detail.split("'")[1]
                for f in result.findings if f.rule == "unknown_function"
            ]
            with self.subTest(sql=sql):
                self.assertTrue(flagged, "gate D did not flag anything")
                for name in flagged:
                    self.assertNotIn(name, live,
                                     f"{name} exists in Spark; gate D is over-flagging")

    def test_date_format_with_literal_letters_executes(self):
        """A translated format with literal letters must actually run.

        The previous Trino-style '' escaping parsed but died at evaluation
        with "Unknown pattern letter: T", and the shipped test asserted that
        broken string, so nothing caught it.  This executes the output.
        """
        self.spark.sql(
            "CREATE OR REPLACE TEMP VIEW fmt_t AS "
            "SELECT timestamp'2024-01-03 10:20:30' AS ts"
        )
        for source, expected in [
            ("SELECT date_format(ts, '%Y-%m-%dT%H:%i:%sZ') FROM fmt_t",
             "2024-01-03T10:20:30Z"),
            ("SELECT date_format(ts, 'Day %d of %M') FROM fmt_t",
             "Day 03 of January"),
            ("SELECT date_format(ts, '%Y-%m-%d') FROM fmt_t",
             "2024-01-03"),
        ]:
            result = translate(source)
            with self.subTest(source=source):
                self.assertEqual(result.flags, 0)
                self.assertEqual(
                    self.spark.sql(result.translated_sql).collect()[0][0], expected)

    def test_flagged_semantic_gaps_really_do_differ_on_spark(self):
        """The new flags must describe a real difference, not a guess."""
        self.spark.sql(
            "CREATE OR REPLACE TEMP VIEW gap_t AS SELECT 7 AS a, 2 AS b, "
            "date'2024-01-03' AS d")
        # greatest: Spark skips NULLs, Athena propagates them
        self.assertEqual(self.spark.sql("SELECT greatest(1, NULL)").collect()[0][0], 1)
        # integer division: Spark returns a double, Athena truncates to 3
        self.assertEqual(self.spark.sql("SELECT 7/2").collect()[0][0], 3.5)
        # EXTRACT(DOW): Spark counts from Sunday, Athena (ISO) from Monday
        self.assertEqual(
            self.spark.sql("SELECT EXTRACT(DOW FROM date'2024-01-03')").collect()[0][0], 4)
        # doubled quote: Spark concatenates and drops the apostrophe
        self.assertEqual(self.spark.sql("SELECT 'it''s'").collect()[0][0], "its")
        # and each of those inputs is flagged by the translator
        for sql in ("SELECT greatest(1, NULL) FROM gap_t",
                    "SELECT 7/2 FROM gap_t",
                    "SELECT EXTRACT(DOW FROM d) FROM gap_t",
                    "SELECT 'it''s' FROM gap_t"):
            with self.subTest(sql=sql):
                self.assertTrue(translate(sql).flags)

    def test_glue_catalog_identifier_with_hyphens_parses_per_part(self):
        source = (
            'frame = ctx.create_dynamic_frame.from_catalog('
            'database="raw-zone", table_name="claims-2026")\n'
        )
        result = translate_glue(source, oci_namespace="ns")
        self.assertIn('spark.table("`raw-zone`.`claims-2026`")', result.translated_sql)
        parser = self.spark._jsparkSession.sessionState().sqlParser()
        identifiers = parser.parseMultipartIdentifier("`raw-zone`.`claims-2026`")
        self.assertEqual(
            [identifiers.apply(index) for index in range(identifiers.size())],
            ["raw-zone", "claims-2026"],
        )

    def test_supported_scalar_rewrites_execute(self):
        cases = [
            (
                "SELECT JSON_EXTRACT_SCALAR('{\"score\":7}', '$.score') AS value",
                "7",
            ),
            (
                "SELECT date_format(to_timestamp('2026-09-09 12:00:00'), '%Y-%m-%d') AS value",
                "2026-09-09",
            ),
            (
                "SELECT strpos('alphabet', 'pha') AS value",
                3,
            ),
            (
                # Character classes keep this a zero-flag case; backslash escapes
                # are covered by test_regex_escape_hazard_is_observable_in_spark.
                "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)', 0) AS value",
                "100-200",
            ),
        ]
        for source, expected in cases:
            with self.subTest(source=source):
                result = translate(source)
                self.assertEqual(result.flags, 0, [str(f) for f in result.findings])
                actual = self.spark.sql(result.translated_sql).first()["value"]
                self.assertEqual(actual, expected)

        zip_result = translate(
            "SELECT zip(array(1, 2), array('a', 'b')) AS value"
        )
        self.assertEqual(zip_result.flags, 0, [str(f) for f in zip_result.findings])
        self.assertEqual(
            len(self.spark.sql(zip_result.translated_sql).first()["value"]),
            2,
        )

    def test_regex_escape_hazard_is_observable_in_spark(self):
        # Athena reads '(\d+)' as the regex \d+; Spark's default parser consumes
        # the backslash first, so the same literal no longer matches digits.
        result = translate(r"SELECT regexp_extract('100-200', '(\d+)-(\d+)', 1) AS value")
        self.assertEqual(
            [finding.rule for finding in result.findings],
            ["regex_escape_sequence"],
        )
        self.assertNotEqual(
            self.spark.sql(result.translated_sql).first()["value"],
            "100",
        )

    def test_double_quoted_identifier_rewrite_parses(self):
        result = translate('SELECT "id" FROM "acme-db"."claims"')
        self.assertEqual(result.translated_sql, "SELECT `id` FROM `acme-db`.`claims`")
        self.assertEqual(result.flags, 0)
        parser = self.spark._jsparkSession.sessionState().sqlParser()
        parser.parsePlan(result.translated_sql)

    def test_flagged_string_dialect_differences_are_observable_in_spark(self):
        split_result = translate("SELECT split('a,b', ',')[1] AS value")
        self.assertEqual(
            [finding.rule for finding in split_result.findings],
            ["split_subscript_index"],
        )
        self.assertEqual(
            self.spark.sql(split_result.translated_sql).first()["value"],
            "b",
        )

        regexp_result = translate(
            "SELECT regexp_extract('100-200', '([0-9]+)-([0-9]+)') AS value"
        )
        self.assertEqual(
            [finding.rule for finding in regexp_result.findings],
            ["regexp_extract_default_group"],
        )
        self.assertEqual(
            self.spark.sql(regexp_result.translated_sql).first()["value"],
            "100",
        )

        cast_result = translate("SELECT CAST(1 AS VARCHAR) AS value")
        self.assertEqual(
            [finding.rule for finding in cast_result.findings],
            ["cast_varchar"],
        )
        parser = self.spark._jsparkSession.sessionState().sqlParser()
        with self.assertRaisesRegex(Exception, "DATATYPE_MISSING_SIZE"):
            parser.parsePlan(cast_result.translated_sql)

    def test_cardinality_null_contract_under_ansi_mode(self):
        result = translate("SELECT cardinality(CAST(NULL AS ARRAY<INT>)) AS value")
        self.assertTrue(any(
            finding.rule == "cardinality_null_semantics"
            for finding in result.findings
        ))
        previous = self.spark.conf.get("spark.sql.ansi.enabled")
        try:
            self.spark.conf.set("spark.sql.ansi.enabled", "true")
            self.assertIsNone(self.spark.sql(result.translated_sql).first()["value"])
            non_null = translate("SELECT cardinality(array(1, 2)) AS value")
            self.assertTrue(any(
                finding.rule == "cardinality_null_semantics"
                for finding in non_null.findings
            ))
            self.assertEqual(
                self.spark.sql(non_null.translated_sql).first()["value"],
                2,
            )
        finally:
            self.spark.conf.set("spark.sql.ansi.enabled", previous)

    def test_array_agg_null_difference_is_visible(self):
        source = (
            "SELECT array_agg(value) AS values FROM VALUES "
            "(CAST(1 AS INT)), (CAST(NULL AS INT)), (CAST(2 AS INT)) AS t(value)"
        )
        result = translate(source)
        self.assertTrue(any(
            finding.rule == "array_agg_semantics" for finding in result.findings
        ))
        self.assertEqual(self.spark.sql(result.translated_sql).first()["values"], [1, 2])

    def test_pinned_compatible_constructs_execute(self):
        cast_result = translate("SELECT try_cast('not-a-number' AS DOUBLE) AS value")
        self.assertEqual(cast_result.flags, 0)
        self.assertIsNone(self.spark.sql(cast_result.translated_sql).first()["value"])

        aggregate_result = translate(
            "SELECT max_by(value, ordering) AS max_value, "
            "min_by(value, ordering) AS min_value "
            "FROM VALUES ('low', 1), ('high', 2) AS t(value, ordering)"
        )
        self.assertEqual(aggregate_result.flags, 0)
        row = self.spark.sql(aggregate_result.translated_sql).first()
        self.assertEqual((row["max_value"], row["min_value"]), ("high", "low"))

    def test_simple_unnest_rewrite_executes(self):
        source = (
            "SELECT value FROM (SELECT array(1, 2, 3) AS values) t "
            "CROSS JOIN UNNEST(values) AS u(value)"
        )
        result = translate(source)
        self.assertEqual(result.flags, 0)
        rows = [row["value"] for row in self.spark.sql(result.translated_sql).collect()]
        self.assertEqual(rows, [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
