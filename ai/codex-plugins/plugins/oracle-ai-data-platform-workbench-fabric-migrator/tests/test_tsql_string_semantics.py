"""T-SQL string semantics that Spark reads differently, and returns a value.

Each of these was reproduced LIVE on an AIDP cluster (Spark 3.5.0,
spark.sql.ansi.enabled=false) as `SELECT <expr> AS v`, translated with
`translate(sql, kind="query")`. Before the fix every one graded PASS -- or
failed only at run time -- and the comments quote both answers:

  S1  'it''s' was left as written; Spark reads two adjacent literals and
      concatenates them, so the apostrophe vanished
  S2  'C:\\new\\table' was left as written; Spark reads a backslash in a
      literal as an escape, so it returned C:<newline>ew<tab>able
  S3  '5' + 3 became concat('5', 3), which is '53'; T-SQL converts the
      string and adds, and returns 8
  S4  LEFT(..) + RIGHT(..) and UPPER(..) + LOWER(..) were left as `+`, which
      Spark evaluates as double addition: NULL, silently
  S5  'abc' = 'abc   ' is true in T-SQL (ANSI padding) and false on Spark,
      and nothing said so
  S6  EOMONTH, REPLICATE and DATENAME passed through unflagged -- PASS --
      and failed on the cluster with UNRESOLVED_ROUTINE
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql, kind="query"):
    return tsql.translate(sql, kind=kind)


def _rules(result):
    return [f.rule for f in result.findings]


class DoubledQuoteTests(unittest.TestCase):
    """S1. Before: `SELECT 'it''s' AS v` came back unchanged at flags=0.

        SQL Server: it's
        Spark 3.5.0 on 'it''s': its   (two literals, 'it' 's', concatenated)
        Spark 3.5.0 on 'it\\'s': it's
    """

    def test_a_doubled_quote_becomes_a_backslash_escape(self):
        result = _t("SELECT 'it''s' AS v")
        # SQL Server: it's
        self.assertEqual(result.translated_sql, r"SELECT 'it\'s' AS v")
        self.assertIn("SQ16_QUOTE_ESCAPE", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_quotes_at_both_ends_of_the_body(self):
        # SQL Server: 'x'
        out = _t("SELECT '''x''' AS v").translated_sql
        self.assertEqual(out, r"SELECT '\'x\'' AS v")

    def test_a_literal_that_is_only_an_apostrophe(self):
        # SQL Server: '
        out = _t("SELECT '''' AS v").translated_sql
        self.assertEqual(out, r"SELECT '\'' AS v")

    def test_the_empty_literal_is_not_a_doubled_quote(self):
        # SQL Server: '' (empty string). `''` here is open + close, not an
        # escaped quote, and must stay as written.
        result = _t("SELECT '' AS v")
        self.assertEqual(result.translated_sql, "SELECT '' AS v")
        self.assertNotIn("SQ16_QUOTE_ESCAPE", _rules(result))

    def test_a_literal_without_quotes_is_untouched_and_not_reported(self):
        result = _t("SELECT 'abc' AS v")
        self.assertEqual(result.translated_sql, "SELECT 'abc' AS v")
        self.assertEqual(_rules(result), [])

    def test_composes_with_the_unicode_prefix(self):
        # SQL Server: it's
        out = _t("SELECT N'it''s' AS v").translated_sql
        self.assertEqual(out, r"SELECT 'it\'s' AS v")

    def test_composes_with_string_concatenation(self):
        # SQL Server: it's Bob. SQ40 builds concat(...) from the T-SQL text,
        # and the literal inside it is respelled once, afterwards.
        out = _t("SELECT 'it''s ' + name AS v FROM t").translated_sql
        self.assertEqual(out, r"SELECT concat('it\'s ', name) AS v FROM t")

    def test_an_apostrophe_in_a_bracketed_identifier_is_not_a_literal(self):
        out = _t("SELECT [it''s] AS v FROM t").translated_sql
        self.assertEqual(out, "SELECT `it''s` AS v FROM t")

    def test_an_apostrophe_in_a_comment_is_not_a_literal(self):
        sql = "SELECT 1 AS v -- it''s a comment"
        self.assertEqual(_t(sql).translated_sql, sql)

    def test_an_unterminated_literal_is_left_exactly_as_written(self):
        result = _t("SELECT 'it''s AS v")
        self.assertEqual(result.translated_sql, "SELECT 'it''s AS v")
        self.assertIn("SQ01_UNTERMINATED_QUOTE", _rules(result))
        self.assertNotIn("SQ16_QUOTE_ESCAPE", _rules(result))

    def test_the_respelling_rule_runs_last(self):
        # Every rule above it scans with the T-SQL lexer, which reads `\'`
        # as a closing quote; run after this one, it would see an
        # unterminated literal and blank the rest of the object.
        self.assertIs(tsql.RULES[-1], tsql.rule_spark_string_literals)


class BackslashTests(unittest.TestCase):
    r"""S2. Before: `SELECT 'C:\new\table' AS v` came back unchanged at
    flags=0.

        SQL Server: C:\new\table        (T-SQL has no backslash escapes)
        Spark 3.5.0 on 'C:\new\table':   C:<newline>ew<tab>able
        Spark 3.5.0 on 'C:\\new\\table': C:\new\table
    """

    def test_every_backslash_is_doubled(self):
        result = _t(r"SELECT 'C:\new\table' AS v")
        # SQL Server: C:\new\table
        self.assertEqual(result.translated_sql, r"SELECT 'C:\\new\\table' AS v")
        self.assertIn("SQ17_BACKSLASH_ESCAPE", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_a_trailing_backslash(self):
        # SQL Server: a\ -- the quote after it closes the literal in T-SQL.
        # Undoubled, Spark would read `\'` as an escaped quote and run on.
        out = _t(r"SELECT 'a\' AS v").translated_sql
        self.assertEqual(out, r"SELECT 'a\\' AS v")

    def test_a_backslash_before_a_doubled_quote(self):
        # SQL Server: \'x -- backslashes doubled FIRST, so the one the quote
        # escape introduces is not doubled with them.
        result = _t(r"SELECT '\''x' AS v")
        self.assertEqual(result.translated_sql, r"SELECT '\\\'x' AS v")
        self.assertIn("SQ16_QUOTE_ESCAPE", _rules(result))
        self.assertIn("SQ17_BACKSLASH_ESCAPE", _rules(result))

    def test_composes_with_the_unicode_prefix(self):
        # SQL Server: C:\temp
        out = _t(r"SELECT N'C:\temp' AS v").translated_sql
        self.assertEqual(out, r"SELECT 'C:\\temp' AS v")

    def test_a_literal_built_by_another_rule_is_not_doubled_twice(self):
        # SQL Server: Agent \Bob. SQ40 copies the literal into concat(...);
        # it is respelled once, at the end, not once per rule that moved it.
        out = _t(r"SELECT 'Agent \' + name AS v FROM t").translated_sql
        self.assertEqual(out, r"SELECT concat('Agent \\', name) AS v FROM t")

    def test_a_backslash_in_a_bracketed_identifier_is_not_a_literal(self):
        out = _t(r"SELECT [a\b] AS v FROM t").translated_sql
        self.assertEqual(out, r"SELECT `a\b` AS v FROM t")

    def test_a_backslash_in_a_comment_is_untouched(self):
        sql = "SELECT 1 AS v -- C:\\new"
        self.assertEqual(_t(sql).translated_sql, sql)


class LikeBackslashTests(unittest.TestCase):
    r"""S2 meets SQ90. A LIKE pattern is a literal first and a pattern second,
    and on Spark both levels read `\` as an escape: the parser, and then
    LIKE's default escape character. T-SQL reads it at neither level.
    """

    def test_a_plain_like_pattern_matches_one_backslash(self):
        # SQL Server: 'a\bc' LIKE 'a\b%' is true (`\` is a plain character).
        # Spark's LIKE must receive `a\\b%` (escaped backslash), so the SQL
        # literal carries four.
        result = _t(r"SELECT * FROM t WHERE c LIKE 'a\b%'")
        self.assertEqual(result.translated_sql,
                         r"SELECT * FROM t WHERE c LIKE 'a\\\\b%'")
        self.assertIn("SQ90_LIKE_BACKSLASH", _rules(result))
        self.assertIn("SQ17_BACKSLASH_ESCAPE", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_a_like_with_an_escape_clause_keeps_its_pattern(self):
        # With ESCAPE '!' the backslash is a plain character in both
        # dialects' LIKE, so only the literal level is doubled.
        out = _t(r"SELECT * FROM t WHERE c LIKE 'a\!%' ESCAPE '!'").translated_sql
        self.assertEqual(out, r"SELECT * FROM t WHERE c LIKE 'a\\!%' ESCAPE '!'")

    def test_an_escape_clause_naming_the_backslash(self):
        # SQL Server: ESCAPE '\' makes `\%` a literal percent; Spark's LIKE
        # reads it the same way, so the pattern is not re-escaped.
        out = _t(r"SELECT * FROM t WHERE c LIKE '5\%' ESCAPE '\'").translated_sql
        self.assertEqual(out, r"SELECT * FROM t WHERE c LIKE '5\\%' ESCAPE '\\'")

    def test_the_rlike_regex_escapes_a_backslash_exactly_once(self):
        # SQL Server: 'a\' LIKE '[a]\' is true. The regex needs `\\` (an
        # escaped backslash), and the literal holding it needs `\\\\`.
        # Double-escaped would be eight, and would match two backslashes.
        out = _t(r"SELECT * FROM t WHERE c LIKE '[a]\'").translated_sql
        self.assertEqual(out, r"SELECT * FROM t WHERE c RLIKE '^[a]\\\\$'")

    def test_the_rlike_regex_escaped_dot_keeps_one_backslash(self):
        # Regex `^[0-9]\.txt$`, written in Spark SQL as `\\.`: SQ17 is the
        # one place the literal-level doubling happens.
        out = _t("SELECT * FROM t WHERE c LIKE '[0-9].txt'").translated_sql
        self.assertEqual(out, r"SELECT * FROM t WHERE c RLIKE '^[0-9]\\.txt$'")

    def test_the_finding_quotes_the_spark_literal_as_emitted(self):
        detail = next(f.detail for f in
                      _t("SELECT * FROM t WHERE c LIKE '[0-9].txt'").findings
                      if f.rule == "SQ90_LIKE_CHARACTER_CLASS")
        self.assertIn(r"RLIKE '^[0-9]\\.txt$'", detail)


class StringPlusNumberTests(unittest.TestCase):
    """S3. Before: `SELECT '5' + 3 AS v` -> `SELECT concat('5', 3) AS v`.

        SQL Server: 8     (int outranks varchar; '5' is converted, then added)
        Spark 3.5.0 on concat('5', 3): 53

    Flagged, not rewritten: no Spark spelling is exact for every string (see
    SQ42's detail), and a REVIEW is better than a different wrong answer.
    """

    def test_a_string_plus_an_integer_is_not_concatenated(self):
        result = _t("SELECT '5' + 3 AS v")
        # SQL Server: 8
        self.assertEqual(result.translated_sql, "SELECT '5' + 3 AS v")
        self.assertIn("SQ42_STRING_PLUS_NUMBER", _rules(result))
        self.assertNotIn("SQ40_STRING_CONCAT", _rules(result))
        self.assertTrue(result.needs_manual_review)

    def test_the_number_on_the_left(self):
        # SQL Server: 8
        result = _t("SELECT 3 + '5' AS v")
        self.assertEqual(result.translated_sql, "SELECT 3 + '5' AS v")
        self.assertIn("SQ42_STRING_PLUS_NUMBER", _rules(result))

    def test_a_decimal_literal(self):
        # SQL Server: 6.5 (the string converts to numeric)
        result = _t("SELECT '5' + 1.5 AS v")
        self.assertEqual(result.translated_sql, "SELECT '5' + 1.5 AS v")
        self.assertIn("SQ42_STRING_PLUS_NUMBER", _rules(result))

    def test_a_cast_to_a_numeric_type(self):
        # SQL Server: 8
        result = _t("SELECT '5' + CAST(c AS INT) AS v FROM t")
        self.assertIn("'5' + CAST(c AS INT)", result.translated_sql)
        self.assertIn("SQ42_STRING_PLUS_NUMBER", _rules(result))

    def test_a_convert_to_a_numeric_type(self):
        result = _t("SELECT CONVERT(decimal(10, 2), c) + '5' AS v FROM t")
        self.assertIn("SQ42_STRING_PLUS_NUMBER", _rules(result))
        self.assertNotIn("SQ40_STRING_CONCAT", _rules(result))

    def test_anywhere_in_a_longer_run(self):
        # SQL Server: ('1' + '2') + 3 = '12' + 3 = 15 -- still addition.
        result = _t("SELECT '1' + '2' + 3 AS v")
        self.assertIn("SQ42_STRING_PLUS_NUMBER", _rules(result))
        self.assertNotIn("concat(", result.translated_sql)

    def test_a_cast_to_a_string_type_is_still_concatenation(self):
        # SQL Server: a5
        # (SQ61 then wraps the CAST in its length-preserving substring.)
        result = _t("SELECT 'a' + CAST(5 AS varchar(10)) AS v")
        self.assertTrue(result.translated_sql.startswith("SELECT concat('a', "))
        self.assertNotIn("SQ42_STRING_PLUS_NUMBER", _rules(result))

    def test_a_number_inside_an_identifier_is_not_a_number(self):
        out = _t("SELECT 'a' + c3 AS v FROM t").translated_sql
        self.assertEqual(out, "SELECT concat('a', c3) AS v FROM t")

    def test_the_finding_says_why(self):
        detail = next(f.detail for f in _t("SELECT '5' + 3 AS v").findings
                      if f.rule == "SQ42_STRING_PLUS_NUMBER")
        self.assertIn("8", detail)
        self.assertIn("CAST", detail)


class StringFunctionConcatTests(unittest.TestCase):
    """S4. Before: both of these came back unchanged at flags=0.

        LEFT('abcdef', 2) + RIGHT('abcdef', 2)
            SQL Server: abef      Spark 3.5.0 as written: NULL
        UPPER('ab') + LOWER('CD')
            SQL Server: ABcd      Spark 3.5.0 as written: NULL

    SQ40 recognised a string literal and nothing else, so a `+` between two
    string-returning calls was read as arithmetic on unknown types.
    """

    def test_left_plus_right(self):
        result = _t("SELECT LEFT('abcdef', 2) + RIGHT('abcdef', 2) AS v")
        # SQL Server: abef
        self.assertEqual(result.translated_sql,
                         "SELECT concat(LEFT('abcdef', 2), RIGHT('abcdef', 2)) AS v")
        self.assertIn("SQ40_STRING_CONCAT", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_upper_plus_lower(self):
        # SQL Server: ABcd
        out = _t("SELECT UPPER('ab') + LOWER('CD') AS v").translated_sql
        self.assertEqual(out, "SELECT concat(UPPER('ab'), LOWER('CD')) AS v")

    def test_every_listed_function_is_a_string_term(self):
        for call in ("LEFT(a, 1)", "RIGHT(a, 1)", "UPPER(a)", "LOWER(a)",
                     "LTRIM(a)", "RTRIM(a)", "TRIM(a)", "REPLACE(a, b, c)",
                     "FORMAT(d, f)", "STUFF(a, 1, 2, b)", "REVERSE(a)",
                     "QUOTENAME(a)", "CHAR(65)", "NCHAR(65)",
                     "CONCAT_WS(s, a, b)", "SUBSTRING(a, 2, 3)"):
            with self.subTest(call=call):
                result = _t(f"SELECT {call} + b AS v FROM t")
                self.assertIn("SQ40_STRING_CONCAT", _rules(result))
                self.assertTrue(result.translated_sql.startswith("SELECT concat("))

    def test_a_function_rewritten_earlier_is_still_a_string_term(self):
        # CONCAT -> concat_ws (SQ31) and SUBSTRING(s, 0, n) -> substring
        # (SQ22) both run before SQ40 and hand it their Spark spelling.
        self.assertEqual(_t("SELECT CONCAT(a, b) + c AS v FROM t").translated_sql,
                         "SELECT concat(concat_ws('', a, b), c) AS v FROM t")
        self.assertEqual(_t("SELECT SUBSTRING(s, 0, 3) + t AS v FROM x").translated_sql,
                         "SELECT concat(substring(s, 1, 2), t) AS v FROM x")

    def test_a_cast_to_a_string_type(self):
        for sql in ("SELECT CAST(a AS nvarchar(10)) + b AS v FROM t",
                    "SELECT CONVERT(varchar(10), a) + b AS v FROM t",
                    "SELECT CAST(a AS char(3)) + b AS v FROM t"):
            with self.subTest(sql=sql):
                self.assertIn("SQ40_STRING_CONCAT", _rules(_t(sql)))

    def test_isnull_and_coalesce_of_a_string_literal(self):
        # SQL Server: ISNULL(NULL, '') + 'x' is 'x'.
        self.assertEqual(_t("SELECT ISNULL(a, '') + b AS v FROM t").translated_sql,
                         "SELECT concat(coalesce(a, ''), b) AS v FROM t")
        self.assertIn("SQ40_STRING_CONCAT",
                      _rules(_t("SELECT COALESCE(a, 'n/a') + b AS v FROM t")))

    def test_coalesce_of_a_number_is_not_a_string(self):
        result = _t("SELECT COALESCE(a, 0) + b AS v FROM t")
        self.assertEqual(result.translated_sql,
                         "SELECT COALESCE(a, 0) + b AS v FROM t")
        self.assertEqual(_rules(result), [])

    def test_two_bare_columns_are_still_left_alone(self):
        # Types unknown: this is addition as often as it is concatenation.
        result = _t("SELECT a + b AS v FROM t")
        self.assertEqual(result.translated_sql, "SELECT a + b AS v FROM t")
        self.assertEqual(_rules(result), [])

    def test_a_non_string_function_is_not_a_string_term(self):
        result = _t("SELECT ABS(a) + b AS v FROM t")
        self.assertEqual(result.translated_sql, "SELECT ABS(a) + b AS v FROM t")

    def test_a_qualified_user_function_is_not_a_string_term(self):
        # `dbo.left` is somebody's scalar UDF; its type is not known here.
        out = _t("SELECT dbo.left(a) + b AS v FROM x").translated_sql
        self.assertNotIn("concat(", out)

    def test_a_string_function_plus_a_number_is_addition(self):
        # SQL Server: LEFT('12', 2) + 1 is 13 (S3 again).
        result = _t("SELECT LEFT('12', 2) + 1 AS v")
        self.assertEqual(result.translated_sql, "SELECT LEFT('12', 2) + 1 AS v")
        self.assertIn("SQ42_STRING_PLUS_NUMBER", _rules(result))

    def test_nested_runs_are_both_converted(self):
        # SQL Server: UPPER('a' + 'b') + LOWER('C') is ABc.
        out = _t("SELECT UPPER('a' + 'b') + LOWER('C') AS v").translated_sql
        self.assertEqual(out, "SELECT concat(UPPER(concat('a', 'b')), LOWER('C')) AS v")

    def test_a_higher_precedence_neighbour_is_refused(self):
        # `x * UPPER(a) + b` is `(x * UPPER(a)) + b`; wrapping UPPER(a) + b
        # in concat would regroup it as x * concat(...).
        for sql in ("SELECT x * UPPER(a) + b AS v FROM t",
                    "SELECT UPPER(a) + b * x AS v FROM t",
                    "SELECT x - 'a' + b AS v FROM t"):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertNotIn("concat(", result.translated_sql)
                self.assertIn("SQ41_CONCAT_PRECEDENCE", _rules(result))

    def test_a_same_level_operator_after_the_run_keeps_its_grouping(self):
        # `'a' + b - c` is `('a' + b) - c`; concat(...) - c is that grouping.
        out = _t("SELECT 'a' + b - c AS v FROM t").translated_sql
        self.assertEqual(out, "SELECT concat('a', b) - c AS v FROM t")


class TrailingSpaceComparisonTests(unittest.TestCase):
    """S5. Before: every predicate below came back unchanged at flags=0.

        'abc' = 'abc   '    SQL Server: true   Spark 3.5.0: false

    Flagged, not rewritten, and the literal case only -- see the rule's
    docstring for why a column-side detector would be noise.
    """

    def _flagged(self, sql):
        return "SQ18_TRAILING_SPACE_COMPARE" in _rules(_t(sql))

    def test_the_measured_comparison_is_flagged(self):
        # SQL Server: 1 (true); Spark: 0
        result = _t("SELECT IIF('abc' = 'abc   ', 1, 0) AS v")
        self.assertIn("SQ18_TRAILING_SPACE_COMPARE", _rules(result))
        self.assertTrue(result.needs_manual_review)
        # A flag: the SQL itself is not changed beyond SQ83's IIF.
        self.assertEqual(result.translated_sql,
                         "SELECT if('abc' = 'abc   ', 1, 0) AS v")

    def test_every_comparing_position(self):
        for sql in ("SELECT * FROM t WHERE c = 'abc '",
                    "SELECT * FROM t WHERE 'abc ' <> c",
                    "SELECT * FROM t WHERE a = 1 AND c != 'x '",
                    "SELECT * FROM t WHERE LEFT(c, 3) = 'ab '",
                    "SELECT * FROM a JOIN b ON a.k = b.k AND a.c = 'x '",
                    "SELECT CASE WHEN c = 'a ' THEN 1 END AS v FROM t",
                    "SELECT * FROM t GROUP BY c HAVING c = 'a '",
                    # A simple CASE compares with the same padding.
                    "SELECT CASE c WHEN 'a ' THEN 1 END AS v FROM t"):
            with self.subTest(sql=sql):
                self.assertTrue(self._flagged(sql))

    def test_an_assignment_or_alias_is_not_a_comparison(self):
        # `SET c = 'x '` stores the spaces; `SELECT label = 'x '` is T-SQL's
        # alias syntax. Neither compares, in either dialect's reading of it.
        for sql in ("UPDATE t SET c = 'x ' WHERE d = 'y'",
                    "UPDATE t SET a = 1, c = 'x '",
                    "SELECT label = 'x ' FROM t",
                    "SELECT a, label = 'x ' FROM t"):
            with self.subTest(sql=sql):
                self.assertFalse(self._flagged(sql))

    def test_a_literal_without_a_trailing_space_is_not_flagged(self):
        for sql in ("SELECT * FROM t WHERE c = 'abc'",
                    "SELECT * FROM t WHERE c = ' abc'",
                    "SELECT 'x ' AS v"):
            with self.subTest(sql=sql):
                self.assertFalse(self._flagged(sql))

    def test_ordering_comparisons_are_out_of_scope(self):
        # Documented limitation: only = / <> / != and simple CASE.
        self.assertFalse(self._flagged("SELECT * FROM t WHERE c <= 'a '"))

    def test_the_finding_names_the_literal_and_the_difference(self):
        detail = next(f.detail for f in
                      _t("SELECT * FROM t WHERE c = 'abc '").findings
                      if f.rule == "SQ18_TRAILING_SPACE_COMPARE")
        self.assertIn("'abc '", detail)
        self.assertIn("trailing", detail)


class MissingFunctionTests(unittest.TestCase):
    """S6. Before: each call below came back unchanged at flags=0, graded
    PASS, and Spark 3.5.0 on AIDP rejected it with UNRESOLVED_ROUTINE.
    """

    def test_eomonth_one_argument(self):
        # SQL Server: EOMONTH('2024-02-10') is 2024-02-29
        result = _t("SELECT EOMONTH(d) AS v FROM t")
        self.assertEqual(result.translated_sql, "SELECT last_day(d) AS v FROM t")
        self.assertIn("SQ30_EOMONTH", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_eomonth_with_a_month_offset(self):
        # SQL Server: EOMONTH('2024-01-31', 1) is 2024-02-29
        out = _t("SELECT EOMONTH('2024-01-31', 1) AS v").translated_sql
        self.assertEqual(out, "SELECT last_day(add_months('2024-01-31', 1)) AS v")

    def test_eomonth_nested(self):
        out = _t("SELECT EOMONTH(EOMONTH(d), -1) AS v FROM t").translated_sql
        self.assertEqual(out, "SELECT last_day(add_months(last_day(d), -1)) AS v FROM t")

    def test_eomonth_with_no_arguments_is_refused(self):
        result = _t("SELECT EOMONTH() AS v")
        self.assertEqual(result.translated_sql, "SELECT EOMONTH() AS v")
        self.assertIn("SQ30_EOMONTH_ARITY", _rules(result))

    def test_replicate_with_a_literal_count(self):
        # SQL Server: ababab
        result = _t("SELECT REPLICATE('ab', 3) AS v")
        self.assertEqual(result.translated_sql, "SELECT repeat('ab', 3) AS v")
        self.assertIn("SQ30_REPLICATE", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_replicate_with_a_computed_count_is_guarded(self):
        # SQL Server: REPLICATE('ab', -1) is NULL; Spark's repeat gives ''.
        out = _t("SELECT REPLICATE(s, n) AS v FROM t").translated_sql
        self.assertEqual(out, "SELECT repeat(s, if(n < 0, NULL, n)) AS v FROM t")

    def test_replicate_with_a_negative_literal_is_guarded(self):
        # SQL Server: NULL
        out = _t("SELECT REPLICATE('ab', -1) AS v").translated_sql
        self.assertEqual(out, "SELECT repeat('ab', if((-1) < 0, NULL, -1)) AS v")

    def test_the_zero_padding_idiom_still_concatenates(self):
        # SQL Server: REPLICATE('0', 5 - LEN('42')) + '42' is 00042. The
        # guard sits inside repeat(...) so SQ40 still reads a string term.
        out = _t("SELECT REPLICATE('0', 5 - n) + c AS v FROM t").translated_sql
        self.assertEqual(
            out, "SELECT concat(repeat('0', if((5 - n) < 0, NULL, 5 - n)), c) AS v FROM t")

    def test_datename_month(self):
        # SQL Server (us_english): DATENAME(month, '2024-02-10') is February
        result = _t("SELECT DATENAME(month, d) AS v FROM t")
        self.assertEqual(result.translated_sql,
                         "SELECT date_format(d, 'MMMM') AS v FROM t")
        self.assertIn("SQ30_DATENAME", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_datename_weekday_and_its_abbreviations(self):
        # SQL Server (us_english): DATENAME(weekday, '2024-02-10') is Saturday
        for part in ("weekday", "dw", "w", "WEEKDAY"):
            with self.subTest(part=part):
                out = _t(f"SELECT DATENAME({part}, d) AS v FROM t").translated_sql
                self.assertEqual(out, "SELECT date_format(d, 'EEEE') AS v FROM t")

    def test_datename_month_abbreviations(self):
        for part in ("mm", "m", "[month]"):
            with self.subTest(part=part):
                out = _t(f"SELECT DATENAME({part}, d) AS v FROM t").translated_sql
                self.assertEqual(out, "SELECT date_format(d, 'MMMM') AS v FROM t")

    def test_datename_says_it_assumes_us_english(self):
        detail = next(f.detail for f in
                      _t("SELECT DATENAME(month, d) AS v FROM t").findings
                      if f.rule == "SQ30_DATENAME")
        self.assertIn("us_english", detail)

    def test_datename_of_another_part_is_refused(self):
        # SQL Server: DATENAME(year, d) is '2024' -- a number as text.
        for part in ("year", "day", "quarter", "hour"):
            with self.subTest(part=part):
                result = _t(f"SELECT DATENAME({part}, d) AS v FROM t")
                self.assertEqual(result.translated_sql,
                                 f"SELECT DATENAME({part}, d) AS v FROM t")
                self.assertIn("SQ30_DATENAME_PART", _rules(result))
                self.assertTrue(result.needs_manual_review)

    def test_datename_concatenates_as_a_string(self):
        # SQL Server: DATENAME(dw, d) + ', ' + x is 'Saturday, x'
        out = _t("SELECT DATENAME(dw, d) + x AS v FROM t").translated_sql
        self.assertEqual(out, "SELECT concat(date_format(d, 'EEEE'), x) AS v FROM t")

    def test_a_qualified_udf_of_the_same_name_is_untouched(self):
        out = _t("SELECT dbo.EOMONTH(d) AS v FROM x").translated_sql
        self.assertNotIn("last_day", out)


class SecondTranslateTests(unittest.TestCase):
    """Declared, like DeclaredNonIdempotencyTests: SQ16/SQ17 respell for
    Spark, and a second translate reads that Spark text as T-SQL. Nothing in
    the pipeline translates twice."""

    def test_a_second_translate_doubles_the_backslashes_again(self):
        one = _t(r"SELECT 'C:\x' AS v").translated_sql
        self.assertEqual(one, r"SELECT 'C:\\x' AS v")
        self.assertEqual(_t(one).translated_sql, r"SELECT 'C:\\\\x' AS v")


if __name__ == "__main__":
    unittest.main()
