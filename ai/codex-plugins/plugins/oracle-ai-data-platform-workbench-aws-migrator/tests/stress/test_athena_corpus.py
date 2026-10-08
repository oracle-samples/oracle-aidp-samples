from __future__ import annotations

import random
import string
import unittest

from aws_aidp.translate.athena_to_spark_sql import translate


class AthenaCorpusTests(unittest.TestCase):
    def test_safe_renames_and_semantic_review_flags_are_case_insensitive(self):
        result = translate(
            "SELECT ARRAY_AGG(x), Approx_Distinct(y), CARDINALITY(z), ZIP(a,b) FROM t"
        )
        self.assertIn("ARRAY_AGG(x)", result.translated_sql)
        self.assertIn("approx_count_distinct(y)", result.translated_sql)
        self.assertIn("CARDINALITY(z)", result.translated_sql)
        self.assertIn("arrays_zip(a,b)", result.translated_sql)
        self.assertEqual(
            {f.rule for f in result.findings if f.severity == "flag"},
            {"array_agg_semantics", "cardinality_null_semantics"},
        )

    def test_simple_array_agg_is_not_false_pass_due_to_null_semantics(self):
        source = "SELECT array_agg(x) FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.changes, 0)
        self.assertTrue(any(f.rule == "array_agg_semantics" for f in result.findings))

    def test_cardinality_is_not_false_pass_due_to_spark_session_config(self):
        source = "SELECT cardinality(values) FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.changes, 0)
        self.assertTrue(any(
            f.rule == "cardinality_null_semantics" for f in result.findings
        ))

    def test_function_names_in_comments_and_literals_are_unchanged(self):
        source = (
            "-- array_agg(x), cardinality(y)\n"
            "SELECT 'zip(a,b)', \"array_agg(x)\", `date_format(ts, '%Y')` "
            "/* JSON_EXTRACT(j, '$.x'), date_diff('day', a, b) */"
        )
        result = translate(source)
        # Only the identifier quoting changes; nothing inside comments,
        # literals, or identifiers is treated as a function call.
        self.assertEqual(
            result.translated_sql,
            "-- array_agg(x), cardinality(y)\n"
            "SELECT 'zip(a,b)', `array_agg(x)`, `date_format(ts, '%Y')` "
            "/* JSON_EXTRACT(j, '$.x'), date_diff('day', a, b) */",
        )
        self.assertEqual((result.changes, result.flags), (1, 0))

    def test_ordered_array_agg_is_flagged_and_left_unchanged(self):
        source = "SELECT array_agg(x ORDER BY ts) FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "array_agg_ordered" for f in result.findings))

    def test_distinct_array_agg_is_flagged_and_left_unchanged(self):
        source = "SELECT array_agg(DISTINCT x) FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "array_agg_distinct" for f in result.findings))

    def test_distinct_ordered_array_agg_gets_both_specific_flags(self):
        source = "SELECT array_agg(DISTINCT x ORDER BY ts) FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(
            {f.rule for f in result.findings},
            {"array_agg_distinct", "array_agg_ordered"},
        )

    def test_malformed_array_agg_is_flagged_without_rewrite(self):
        for source in ("SELECT array_agg()", "SELECT array_agg(x, y)", "SELECT array_agg(x"):
            with self.subTest(source=source):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertTrue(result.needs_manual_review)

    def test_date_format_with_nested_comma_expression_is_rewritten(self):
        source = "SELECT date_format(coalesce(ts, current_timestamp), '%Y-%m-%d') FROM t"
        result = translate(source)
        self.assertEqual(
            result.translated_sql,
            "SELECT date_format(coalesce(ts, current_timestamp), 'yyyy-MM-dd') FROM t",
        )
        self.assertEqual((result.changes, result.flags), (1, 0))

    def test_date_format_all_supported_tokens_translate_exactly(self):
        source = (
            "SELECT date_format(ts, "
            "'%Y %y %m %c %d %e %H %k %h %I %l %i %s %S %p %j %a %W %b %M %f %T %r %%')"
        )
        result = translate(source)
        self.assertEqual(
            result.translated_sql,
            "SELECT date_format(ts, "
            "'yyyy yy MM M dd d HH H hh hh h mm ss ss a DDD EEE EEEE MMM MMMM "
            "SSSSSS HH:mm:ss hh:mm:ss a %')",
        )
        self.assertEqual((result.changes, result.flags), (1, 0))

    def test_date_format_literal_letters_are_quoted_for_spark(self):
        # This previously expected Trino-style '' doubling, which Spark reads
        # as adjacent literals and rejects with "Unknown pattern letter: T".
        # Backslash escaping is what Spark's parser wants; verified on 3.5.9
        # that the string below evaluates to 2024-01-03T10:20:30Z.
        result = translate("SELECT date_format(ts, '%Y-%m-%dT%H:%i:%sZ')")
        self.assertEqual(
            result.translated_sql,
            r"SELECT date_format(ts, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')",
        )
        self.assertEqual(result.flags, 0)

    def test_multiple_and_nested_date_formats_rewrite_in_one_pass(self):
        source = (
            "SELECT date_format(date_format(ts, '%Y'), '%m'), "
            "date_format(ts, '%d') FROM t"
        )
        result = translate(source)
        self.assertEqual(
            result.translated_sql,
            "SELECT date_format(date_format(ts, 'yyyy'), 'MM'), "
            "date_format(ts, 'dd') FROM t",
        )
        self.assertEqual((result.changes, result.flags), (3, 0))

    def test_unsafe_date_formats_are_flagged_and_not_partially_rewritten(self):
        cases = [
            "SELECT date_format(ts, '%Y-%q')",
            "SELECT date_format(ts, '%w')",
            "SELECT date_format(ts, format_column)",
            "SELECT date_format(ts, `%Y`)",
            "SELECT date_format(ts, 'yyyy-MM-dd')",
            "SELECT date_format(ts, '%Y'",
        ]
        for source in cases:
            with self.subTest(source=source):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertTrue(result.needs_manual_review)

    def test_multi_column_unnest_is_flagged(self):
        source = "SELECT k,v FROM t CROSS JOIN UNNEST(m) AS u(k,v)"
        result = translate(source)
        self.assertTrue(result.needs_manual_review)
        self.assertEqual(result.translated_sql, source)

    def test_nested_unnest_expression_is_structurally_rewritten(self):
        source = (
            "SELECT x FROM t CROSS JOIN "
            "UNNEST(transform(filter(a, x -> x > 0), x -> coalesce(x, 0))) AS u(x)"
        )
        result = translate(source)
        self.assertIn(
            "LATERAL VIEW explode(transform(filter(a, x -> x > 0), "
            "x -> coalesce(x, 0))) u AS x",
            result.translated_sql,
        )
        self.assertTrue(any(f.rule == "higher_order_lambda" for f in result.findings))

    def test_multiple_simple_unnests_are_rewritten_in_one_pass(self):
        source = (
            "SELECT x,y FROM t CROSS JOIN UNNEST(a) AS u(x) "
            "CROSS JOIN UNNEST(split(b, ',')) AS v(y)"
        )
        result = translate(source)
        self.assertEqual(result.translated_sql.count("LATERAL VIEW explode"), 2)
        self.assertNotIn("UNNEST", result.translated_sql)
        self.assertEqual((result.changes, result.flags), (2, 0))

    def test_unsupported_unnest_forms_never_false_pass(self):
        cases = [
            "SELECT k,v FROM t CROSS JOIN UNNEST(m) AS u(k,v)",
            "SELECT x,y FROM t CROSS JOIN UNNEST(a,b) AS u(x,y)",
            "SELECT x FROM t LEFT JOIN UNNEST(a) AS u(x) ON TRUE",
            "SELECT x FROM t CROSS JOIN UNNEST(a)",
            'SELECT x FROM t CROSS JOIN UNNEST(a) AS "u"(x)',
            "SELECT x FROM t CROSS JOIN UNNEST(a AS u(x)",
        ]
        for source in cases:
            with self.subTest(source=source):
                result = translate(source)
                self.assertTrue(any(f.rule == "unnest_unhandled" for f in result.findings))

    def test_ordinality_gets_one_specific_flag(self):
        source = "SELECT x,i FROM t CROSS JOIN UNNEST(a) WITH ORDINALITY AS u(x,i)"
        result = translate(source)
        self.assertEqual(result.flags, 1)
        self.assertEqual(result.findings[0].rule, "unnest_with_ordinality")

    def test_non_scalar_json_extract_is_flagged_without_rewrite(self):
        source = "SELECT JSON_EXTRACT(payload, '$.object') FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertTrue(any(f.rule == "json_extract" for f in result.findings))

    def test_scalar_json_extract_only_rewrites_static_simple_paths(self):
        source = "SELECT JSON_EXTRACT_SCALAR(payload, '$.items[0].name') FROM t"
        result = translate(source)
        self.assertEqual(
            result.translated_sql,
            "SELECT get_json_object(payload, '$.items[0].name') FROM t",
        )
        self.assertEqual((result.changes, result.flags), (1, 0))

    def test_complex_dynamic_or_malformed_scalar_json_path_is_flagged(self):
        cases = [
            "SELECT JSON_EXTRACT_SCALAR(payload, '$.*') FROM t",
            "SELECT JSON_EXTRACT_SCALAR(payload, '$[\"quoted-key\"]') FROM t",
            "SELECT JSON_EXTRACT_SCALAR(payload, path_column) FROM t",
            "SELECT JSON_EXTRACT_SCALAR(payload) FROM t",
            "SELECT JSON_EXTRACT_SCALAR(payload, '$.x' FROM t",
        ]
        for source in cases:
            with self.subTest(source=source):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertTrue(any(
                    f.rule == "json_extract_scalar_unhandled" for f in result.findings
                ))

    def test_three_argument_date_diff_is_flagged(self):
        result = translate("SELECT date_diff('day', started, ended) FROM t")
        self.assertTrue(any(f.rule == "date_diff" for f in result.findings))

    def test_athena_date_add_is_flagged_but_spark_shape_is_allowed(self):
        athena = translate("SELECT date_add('day', 2, ts) FROM t")
        spark = translate("SELECT date_add(d, 2) FROM t")
        self.assertTrue(any(f.rule == "date_add" for f in athena.findings))
        self.assertEqual((spark.translated_sql, spark.flags), ("SELECT date_add(d, 2) FROM t", 0))

    def test_athena_datetime_functions_are_flagged(self):
        for function in (
            "date_parse", "parse_datetime", "format_datetime",
            "from_iso8601_timestamp", "to_iso8601", "last_day_of_month",
        ):
            with self.subTest(function=function):
                result = translate(f"SELECT {function}(x) FROM t")
                self.assertTrue(any(f.rule == "athena_datetime" for f in result.findings))

    def test_map_aggregates_are_flagged(self):
        for function in ("map_agg", "multimap_agg"):
            with self.subTest(function=function):
                result = translate(f"SELECT {function}(k, v) FROM t")
                self.assertTrue(any(f.rule == "map_agg" for f in result.findings))

    def test_other_unverified_athena_constructs_are_flagged(self):
        cases = {
            "arbitrary": "SELECT arbitrary(x) FROM t",
            "higher_order_lambda": "SELECT reduce(a, 0, (s, x) -> s + x, s -> s) FROM t",
            "regexp_like": "SELECT regexp_like(name, '^A\\\\d+$') FROM t",
        }
        for expected_rule, source in cases.items():
            with self.subTest(expected_rule=expected_rule):
                result = translate(source)
                self.assertTrue(any(f.rule == expected_rule for f in result.findings))

    def test_split_subscript_index_base_difference_gets_exactly_one_flag(self):
        source = "SELECT split(csv, ',')[1] FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.flags, 1)
        self.assertEqual(result.findings[0].rule, "split_subscript_index")

    def test_split_regex_metachar_delimiters_each_get_exactly_one_flag(self):
        for delimiter in ("|", ".", "\\"):
            source = f"SELECT split(value, '{delimiter}') FROM t"
            with self.subTest(delimiter=delimiter):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertEqual(result.flags, 1)
                self.assertEqual(result.findings[0].rule, "split_regex_delimiter")

    def test_nonliteral_split_delimiters_get_one_query_level_flag(self):
        cases = (
            "SELECT split(value, delimiter_column) FROM t",
            "SELECT split(value, coalesce(delimiter_column, ','), 3) FROM t",
            "SELECT split(a, delimiter_a), split(b, delimiter_b) FROM t",
        )
        for source in cases:
            with self.subTest(source=source):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertEqual(result.flags, 1)
                self.assertEqual(
                    [finding.rule for finding in result.findings],
                    ["split_dynamic_delimiter"],
                )

    def test_static_non_regex_split_delimiters_remain_zero_flag(self):
        source = "SELECT split(csv, ','), split(tags, ';', 3) FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual((result.changes, result.flags), (0, 0))

    def test_two_argument_regexp_extract_gets_exactly_one_flag(self):
        source = "SELECT regexp_extract(value, '([0-9]+)') FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.flags, 1)
        self.assertEqual(result.findings[0].rule, "regexp_extract_default_group")

    def test_cast_as_varchar_gets_exactly_one_flag(self):
        for target_type in ("VARCHAR", "VARCHAR(255)"):
            source = f"SELECT CAST(value AS {target_type}) FROM t"
            with self.subTest(target_type=target_type):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertEqual(result.flags, 1)
                self.assertEqual(result.findings[0].rule, "cast_varchar")

    def test_two_argument_strpos_is_safely_rewritten_to_instr(self):
        source = "SELECT strpos(value, 'needle'), STRPOS(other, key) FROM t"
        result = translate(source)
        self.assertEqual(
            result.translated_sql,
            "SELECT instr(value, 'needle'), instr(other, key) FROM t",
        )
        self.assertEqual((result.changes, result.flags), (1, 0))

    def test_three_argument_strpos_gets_exactly_one_flag(self):
        source = "SELECT strpos(value, 'needle', 2) FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual(result.flags, 1)
        self.assertEqual(result.findings[0].rule, "strpos_unhandled")

    def test_new_string_rules_shield_comments_literals_and_identifiers(self):
        source = (
            "-- split(csv, '|')[1], regexp_extract(x, '(x)'), CAST(x AS VARCHAR)\n"
            "SELECT 'strpos(x, y)', `split(x, '.')`, \"regexp_extract(x, '(x)')\""
        )
        result = translate(source)
        # The double-quoted identifier is converted to Spark's backtick form;
        # its content must still never be parsed as a function call.
        self.assertEqual(
            result.translated_sql,
            "-- split(csv, '|')[1], regexp_extract(x, '(x)'), CAST(x AS VARCHAR)\n"
            "SELECT 'strpos(x, y)', `split(x, '.')`, `regexp_extract(x, '(x)')`",
        )
        self.assertEqual((result.changes, result.flags), (1, 0))

    def test_new_string_rules_are_idempotent(self):
        source = "SELECT strpos(value, 'needle') FROM t"
        once = translate(source).translated_sql
        twice = translate(once).translated_sql
        self.assertEqual(twice, once)

    def test_pinned_spark_compatible_constructs_remain_unchanged(self):
        source = (
            "SELECT try_cast(raw AS DECIMAL(12, 2)), max_by(x, ts), min_by(x, ts) FROM t"
        )
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual((result.changes, result.flags), (0, 0))

    def test_malformed_pinned_spark_constructs_are_flagged(self):
        cases = [
            ("try_cast_unhandled", "SELECT try_cast(raw)"),
            ("try_cast_unhandled", "SELECT try_cast(raw AS JSON)"),
            ("max_by_unhandled", "SELECT max_by(x)"),
            ("min_by_unhandled", "SELECT min_by(x, y, z)"),
        ]
        for expected_rule, source in cases:
            with self.subTest(expected_rule=expected_rule):
                result = translate(source)
                self.assertTrue(any(f.rule == expected_rule for f in result.findings))

    def test_simple_intervals_are_allowed_and_complex_intervals_are_flagged(self):
        simple = translate("SELECT current_date + INTERVAL '30' DAY")
        self.assertEqual(simple.flags, 0)
        for source in (
            "SELECT INTERVAL '1-2' YEAR TO MONTH",
            "SELECT INTERVAL '1' DAY * retention_days",
        ):
            with self.subTest(source=source):
                result = translate(source)
                self.assertTrue(any(f.rule == "complex_interval" for f in result.findings))

    def test_wrong_arity_rewrites_are_flagged_and_left_unchanged(self):
        for source in (
            "SELECT APPROX_DISTINCT()",
            "SELECT APPROX_DISTINCT(x, 0.1, 3)",
            "SELECT cardinality(a, b)",
            "SELECT zip(a)",
            "SELECT zip()",
        ):
            with self.subTest(source=source):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertTrue(result.needs_manual_review)

    def test_optional_approx_error_and_multi_array_zip_translate(self):
        source = "SELECT APPROX_DISTINCT(x, 0.1), zip(a, b, c) FROM t"
        result = translate(source)
        self.assertEqual(
            result.translated_sql,
            "SELECT approx_count_distinct(x, 0.1), arrays_zip(a, b, c) FROM t",
        )
        self.assertEqual((result.changes, result.flags), (2, 0))

    def test_multiple_histograms_get_one_query_level_review_flag(self):
        result = translate("SELECT histogram(x), HISTOGRAM(y) FROM t")
        self.assertEqual(result.changes, 0)
        self.assertEqual([f.rule for f in result.findings], ["histogram"])

    def test_supported_translation_is_idempotent(self):
        source = (
            "SELECT array_agg(x), cardinality(tags), zip(a,b), "
            "date_format(coalesce(ts, now()), '%Y-%m-%d') FROM t "
            "CROSS JOIN UNNEST(tags) AS u(x)"
        )
        once = translate(source).translated_sql
        twice = translate(once).translated_sql
        self.assertEqual(twice, once)

    def test_findings_and_output_are_deterministic(self):
        source = (
            "SELECT array_agg(DISTINCT x ORDER BY ts), date_diff('day', a, b), "
            "JSON_EXTRACT_SCALAR(j, '$.*') FROM t"
        )
        first = translate(source)
        for _ in range(20):
            result = translate(source)
            self.assertEqual(result.translated_sql, first.translated_sql)
            self.assertEqual(result.findings, first.findings)

    def test_random_text_never_crashes(self):
        rng = random.Random(42017)
        alphabet = string.ascii_letters + string.digits + " ()[]{}'\"`-,/*%_\n\t"
        for _ in range(1_000):
            source = "".join(rng.choice(alphabet) for _ in range(rng.randrange(0, 300)))
            result = translate(source)
            self.assertIsInstance(result.translated_sql, str)

    def test_double_quoted_identifiers_are_rewritten_to_backticks(self):
        source = 'SELECT "id", "amount" FROM "acme-db"."claims" WHERE "status" = \'OPEN\''
        result = translate(source)
        self.assertEqual(
            result.translated_sql,
            "SELECT `id`, `amount` FROM `acme-db`.`claims` WHERE `status` = 'OPEN'",
        )
        self.assertEqual((result.changes, result.flags), (1, 0))
        self.assertEqual(result.findings[0].rule, "double_quoted_identifier")

    def test_double_quoted_identifier_escapes_are_preserved(self):
        result = translate('SELECT "we""ird", "back`tick" FROM t')
        self.assertEqual(result.translated_sql, "SELECT `we\"ird`, `back``tick` FROM t")
        self.assertEqual((result.changes, result.flags), (1, 0))

    def test_double_quotes_inside_literals_and_comments_are_untouched(self):
        source = "-- \"comment\"\nSELECT 'say \"hi\"' /* \"block\" */ FROM t"
        result = translate(source)
        self.assertEqual(result.translated_sql, source)
        self.assertEqual((result.changes, result.flags), (0, 0))

    def test_regex_backslash_escapes_get_exactly_one_flag(self):
        cases = [
            r"SELECT regexp_replace(code, '\d+', '') FROM t",
            r"SELECT regexp_extract(code, '([A-Z]+)-(\d+)', 2) FROM t",
            r"SELECT regexp_extract_all(code, '\w+') FROM t",
            # `regexp_split` is not a Spark 3.5 built-in (Spark spells it
            # `split`), so the output-validation gate correctly adds a second,
            # unrelated finding here.  The escape rule must still fire once.
            r"SELECT regexp_split(code, '\s+') FROM t",
        ]
        for source in cases:
            with self.subTest(source=source):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                escape_flags = [f for f in result.findings
                                if f.rule == "regex_escape_sequence"]
                self.assertEqual(len(escape_flags), 1,
                                 [str(f) for f in result.findings])
                self.assertEqual(
                    {f.rule for f in result.findings} - {"regex_escape_sequence"},
                    {"unknown_function"} if "regexp_split" in source else set(),
                    [str(f) for f in result.findings],
                )

    def test_regex_without_backslash_is_not_flagged_for_escapes(self):
        result = translate("SELECT regexp_replace(code, '[0-9]+', ''), regexp_extract(code, '([A-Z]+)', 1) FROM t")
        self.assertEqual((result.changes, result.flags), (0, 0))

    def test_array_contains_and_levenshtein_are_safely_renamed(self):
        result = translate("SELECT contains(tags, 'x'), levenshtein_distance(a, b) FROM t")
        self.assertEqual(
            result.translated_sql,
            "SELECT array_contains(tags, 'x'), levenshtein(a, b) FROM t",
        )
        self.assertEqual((result.changes, result.flags), (2, 0))

    def test_spark_incompatible_athena_functions_are_flagged(self):
        cases = {
            "from_unixtime": "SELECT from_unixtime(ts) FROM t",
            "to_unixtime": "SELECT to_unixtime(ts) FROM t",
            "from_iso8601_date": "SELECT from_iso8601_date(d) FROM t",
            "try_expression": "SELECT TRY(1 / x) FROM t",
            "url_extract": "SELECT url_extract_host(u), url_extract_parameter(u, 'q') FROM t",
            "json_parse": "SELECT json_parse(payload) FROM t",
            "day_of_week": "SELECT day_of_week(d), dow(d) FROM t",
        }
        for expected_rule, source in cases.items():
            with self.subTest(expected_rule=expected_rule):
                result = translate(source)
                self.assertEqual(result.translated_sql, source)
                self.assertEqual(result.flags, 1, [str(f) for f in result.findings])
                self.assertEqual(result.findings[0].rule, expected_rule)

    def test_try_cast_is_not_mistaken_for_try_expression(self):
        result = translate("SELECT TRY_CAST(x AS INT) FROM t")
        self.assertEqual((result.changes, result.flags), (0, 0))


if __name__ == "__main__":
    unittest.main()
