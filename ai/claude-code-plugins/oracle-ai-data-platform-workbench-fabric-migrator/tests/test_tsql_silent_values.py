"""T-SQL defects that run fine on Spark and return the wrong answer.

The hardest class to catch: every one of these translated without an error,
graded PASS with flags=0, and produced a different value from the T-SQL it
came from. Each expectation below was measured on the tree before the fix,
and the measured "before" is quoted next to the case it belongs to. The Spark
numbers quoted in the comments were measured on Spark 4.2.0.

  D1  CONCAT(a, b) passed through, and Spark's concat returns NULL where
      T-SQL's CONCAT treats a NULL argument as an empty string
  D2  LOG(value, base) passed through, and Spark's LOG is LOG(base, value):
      log(10, 100) is 2.0 and log(100, 10) is 0.5, so the two are not
      interchangeable and every logarithm came out a different number
  D3  CAST(c AS varchar(n)) became CAST(c AS STRING), and Spark does not
      truncate in a cast, so a value T-SQL would have cut to n characters
      survived whole
  D4  a bare DECIMAL/NUMERIC passed through: T-SQL's default is DECIMAL(18,0)
      and Spark's is DECIMAL(10,0), so a 14-digit value became NULL
  D5  LIKE '[A-Z]%' passed through, and Spark's LIKE reads the class as
      literal characters, so the predicate matched nothing
  D6  SUBSTRING(s, 0, n) passed through; T-SQL's start of 0 consumes one slot
      before the string, so the result was one character too long
  D7  GO was deleted, merging two statements into one, and a TOP from the
      first batch became a LIMIT on the second
  D8  SQ11 read the column reference dbo.t.c as a three-part table name and
      emitted default.dbo_t.c, which resolves to nothing
  T6  rule_top ended with a document-wide, unmasked, unconditional
      whitespace collapse that ran on every object, TOP or no TOP, and
      rewrote the contents of string literals and comments
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql, **kw):
    kw.setdefault("kind", "warehouse_ddl")
    kw.setdefault("item", "W")
    return tsql.translate(sql, **kw)


def _rules(result):
    return [f.rule for f in result.findings]


class ConcatNullTests(unittest.TestCase):
    """D1. Before: `SELECT CONCAT(a, b) FROM dbo.t` came back with CONCAT
    untouched and flags=0.

    Measured on Spark 4.2.0:
        concat('a', NULL)             -> NULL
        concat_ws('', 'a', NULL)      -> 'a'
        concat_ws('', 'a', NULL, 'b') -> 'ab'
    T-SQL's CONCAT treats a NULL argument as an empty string, so
    `concat_ws('', ...)` is the exact reproduction and `concat` is not.
    """

    def test_concat_becomes_concat_ws_with_an_empty_separator(self):
        result = _t("SELECT CONCAT(a, b) FROM dbo.t")
        self.assertIn("concat_ws('', a, b)", result.translated_sql)
        self.assertNotIn("CONCAT(", result.translated_sql)
        self.assertIn("SQ31_CONCAT", _rules(result))

    def test_three_arguments(self):
        out = _t("SELECT CONCAT(a, b, c) FROM dbo.t").translated_sql
        self.assertIn("concat_ws('', a, b, c)", out)

    def test_argument_text_is_preserved_verbatim(self):
        out = _t("SELECT CONCAT(a, 'x, y', f(b)) FROM dbo.t").translated_sql
        self.assertIn("concat_ws('', a, 'x, y', f(b))", out)

    def test_lower_case_is_the_same_function(self):
        out = _t("SELECT concat(a, b) FROM dbo.t").translated_sql
        self.assertIn("concat_ws('', a, b)", out)

    def test_a_nested_concat_is_rewritten_too(self):
        out = _t("SELECT CONCAT(a, CONCAT(b, c)) FROM dbo.t").translated_sql
        self.assertEqual(out.count("concat_ws"), 2)
        self.assertNotIn("CONCAT(", out)

    def test_concat_ws_is_left_alone(self):
        # T-SQL's own CONCAT_WS skips NULLs, and so does Spark's, so the two
        # already agree and there is nothing to change.
        out = _t("SELECT CONCAT_WS('-', a, b) FROM dbo.t").translated_sql
        self.assertIn("CONCAT_WS('-', a, b)", out)

    def test_one_argument_is_flagged_not_rewritten(self):
        result = _t("SELECT CONCAT(a) FROM dbo.t")
        self.assertIn("SQ31_CONCAT_ARITY", _rules(result))
        self.assertIn("CONCAT(a)", result.translated_sql)

    def test_inside_a_literal_is_untouched(self):
        out = _t("SELECT 'CONCAT(a, b)' FROM dbo.t").translated_sql
        self.assertIn("'CONCAT(a, b)'", out)

    def test_the_plus_rule_still_emits_null_propagating_concat(self):
        # SQ40 converts T-SQL's `+` and `+` DOES propagate NULL, so its output
        # must stay `concat`. If this rule ran after SQ40 it would rewrite
        # SQ40's own output and change `'a' + NULL` from NULL to 'a'.
        out = _t("SELECT 'a' + b FROM dbo.t").translated_sql
        self.assertIn("concat('a', b)", out)
        self.assertNotIn("concat_ws", out)

    def test_the_finding_says_why_concat_is_wrong(self):
        result = _t("SELECT CONCAT(a, b) FROM dbo.t")
        detail = next(f.detail for f in result.findings if f.rule == "SQ31_CONCAT")
        self.assertIn("NULL", detail)
        self.assertIn("concat_ws", detail)


class LogArgumentOrderTests(unittest.TestCase):
    """D2. Before: `SELECT LOG(x, 10) FROM dbo.t` came back with LOG untouched
    and flags=0, returning a different number.

    T-SQL is LOG(value, base); Spark is LOG(base, value). Measured on Spark
    4.2.0: log(10, 100) -> 2.0 and log(100, 10) -> 0.5, so the two argument
    orders are not interchangeable.
    """

    def test_two_arguments_swap(self):
        result = _t("SELECT LOG(x, 10) FROM dbo.t")
        self.assertIn("log(10, x)", result.translated_sql)
        self.assertIn("SQ32_LOG", _rules(result))

    def test_one_argument_is_the_natural_log_in_both_and_is_untouched(self):
        result = _t("SELECT LOG(x) FROM dbo.t")
        self.assertIn("LOG(x)", result.translated_sql)
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME"])

    def test_log10_is_the_same_function_in_both_and_is_untouched(self):
        result = _t("SELECT LOG10(x) FROM dbo.t")
        self.assertIn("LOG10(x)", result.translated_sql)
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME"])

    def test_expression_arguments_are_preserved_verbatim(self):
        out = _t("SELECT LOG(a + b, 2) FROM dbo.t").translated_sql
        self.assertIn("log(2, a + b)", out)

    def test_two_calls_are_both_swapped(self):
        out = _t("SELECT LOG(x, 10), LOG(y, 2) FROM dbo.t").translated_sql
        self.assertIn("log(10, x)", out)
        self.assertIn("log(2, y)", out)

    def test_a_nested_log_is_swapped_too(self):
        # The inner call is rewritten through the argument, not on a second
        # pass: a second pass over this rule's own output would swap the
        # operands back, because a swap cannot tell its input from its output.
        out = _t("SELECT LOG(LOG(x, 2), 10) FROM dbo.t").translated_sql
        self.assertIn("log(10, log(2, x))", out)

    def test_one_run_is_stable_and_swaps_exactly_once(self):
        out = _t("SELECT LOG(x, 10) FROM dbo.t").translated_sql
        self.assertIn("log(10, x)", out)
        self.assertNotIn("log(x, 10)", out)

    def test_three_arguments_are_flagged_not_rewritten(self):
        result = _t("SELECT LOG(x, 10, 2) FROM dbo.t")
        self.assertIn("SQ32_LOG_ARITY", _rules(result))
        self.assertIn("LOG(x, 10, 2)", result.translated_sql)

    def test_inside_a_literal_is_untouched(self):
        out = _t("SELECT 'LOG(x, 10)' FROM dbo.t").translated_sql
        self.assertIn("'LOG(x, 10)'", out)

    def test_a_word_ending_in_log_is_not_the_function(self):
        out = _t("SELECT catalog(x, 10) FROM dbo.t").translated_sql
        self.assertIn("catalog(x, 10)", out)

    def test_the_finding_names_both_orders(self):
        result = _t("SELECT LOG(x, 10) FROM dbo.t")
        detail = next(f.detail for f in result.findings if f.rule == "SQ32_LOG")
        self.assertIn("LOG(value, base)", detail)
        self.assertIn("log(base, value)", detail)


class CastLengthTests(unittest.TestCase):
    """D3. Before: `SELECT CAST(c AS varchar(20)) FROM dbo.t` came back as
    `CAST(c AS STRING)` at flags=0, and the truncation was gone.

    Measured on Spark 4.2.0: `CAST('abcdefghijklmnopqrstuvwxyz' AS varchar(20))`
    is *accepted* and returns all 26 characters -- Spark does not truncate in
    a cast -- while `substring(cast(c as string), 1, 10)` returns 'abcdefghij'.
    T-SQL truncates to the declared length.
    """

    def test_varchar_n_truncates(self):
        result = _t("SELECT CAST(c AS varchar(20)) FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "SELECT substring(CAST(c AS STRING), 1, 20) FROM default.W.t")
        self.assertIn("SQ61_CAST_LENGTH", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_nvarchar_n_truncates(self):
        out = _t("SELECT CAST(c AS nvarchar(50)) FROM dbo.t").translated_sql
        self.assertIn("substring(CAST(c AS STRING), 1, 50)", out)

    def test_char_n_truncates_and_flags_the_padding_it_does_not_reproduce(self):
        result = _t("SELECT CAST(c AS char(10)) FROM dbo.t")
        self.assertIn("substring(CAST(c AS STRING), 1, 10)",
                      result.translated_sql)
        self.assertIn("SQ61_CHAR_PADDING", _rules(result))
        self.assertTrue(result.needs_manual_review)

    def test_nchar_n_is_the_same_decision_as_char_n(self):
        result = _t("SELECT CAST(c AS nchar(2)) FROM dbo.t")
        self.assertIn("substring(CAST(c AS STRING), 1, 2)", result.translated_sql)
        self.assertIn("SQ61_CHAR_PADDING", _rules(result))

    def test_varchar_does_not_flag_padding(self):
        self.assertNotIn("SQ61_CHAR_PADDING",
                         _rules(_t("SELECT CAST(c AS varchar(20)) FROM dbo.t")))

    def test_varchar_max_has_no_length_to_apply(self):
        result = _t("SELECT CAST(c AS varchar(max)) FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT CAST(c AS STRING) FROM default.W.t")
        self.assertNotIn("SQ61_CAST_LENGTH", _rules(result))

    def test_try_cast_keeps_its_null_on_failure_semantics(self):
        out = _t("SELECT TRY_CAST(c AS varchar(20)) FROM dbo.t").translated_sql
        self.assertIn("substring(try_cast(c AS STRING), 1, 20)", out)
        self.assertNotIn("CAST(", out)

    def test_convert_truncates_the_same_way(self):
        out = _t("SELECT CONVERT(varchar(20), c) FROM dbo.t").translated_sql
        self.assertEqual(
            out, "SELECT substring(CAST(c AS STRING), 1, 20) FROM default.W.t")

    def test_try_convert_truncates_the_same_way(self):
        out = _t("SELECT TRY_CONVERT(nvarchar(50), c) FROM dbo.t").translated_sql
        self.assertIn("substring(try_cast(c AS STRING), 1, 50)", out)

    def test_the_cast_and_convert_paths_still_agree(self):
        convert = _t("SELECT CONVERT(varchar(20), c) FROM dbo.t").translated_sql
        cast = _t("SELECT CAST(c AS varchar(20)) FROM dbo.t").translated_sql
        self.assertEqual(convert, cast)

    def test_the_expression_is_preserved_verbatim(self):
        out = _t("SELECT CAST(a + b AS varchar(5)) FROM dbo.t").translated_sql
        self.assertIn("substring(CAST(a + b AS STRING), 1, 5)", out)

    def test_a_nested_cast_truncates_at_both_levels(self):
        out = _t("SELECT CAST(CAST(x AS varchar(10)) AS varchar(3))").translated_sql
        self.assertEqual(
            out,
            "SELECT substring(CAST(substring(CAST(x AS STRING), 1, 10) "
            "AS STRING), 1, 3)")

    def test_a_column_definition_keeps_STRING(self):
        # A column is a different decision and is deliberately untouched:
        # T-SQL raises on an over-long INSERT rather than truncating, so
        # STRING loses a constraint, not a value. See the report.
        result = _t("CREATE TABLE dbo.t (c varchar(20))")
        self.assertIn("c STRING", result.translated_sql)
        self.assertNotIn("SQ61_CAST_LENGTH", _rules(result))

    def test_a_non_string_cast_is_untouched(self):
        result = _t("SELECT CAST(c AS int) FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT CAST(c AS int) FROM default.W.t")

    def test_inside_a_literal_is_untouched(self):
        out = _t("SELECT 'CAST(c AS varchar(20))' FROM dbo.t").translated_sql
        self.assertIn("'CAST(c AS varchar(20))'", out)

    def test_the_finding_quotes_what_was_measured(self):
        detail = next(f.detail for f in
                      _t("SELECT CAST(c AS varchar(20))").findings
                      if f.rule == "SQ61_CAST_LENGTH")
        self.assertIn("does not truncate", detail)
        self.assertIn("20", detail)


class BareDecimalTests(unittest.TestCase):
    """D4. Before: `SELECT CAST(c AS DECIMAL)` and `CREATE TABLE dbo.t (c
    DECIMAL)` both passed through untouched at flags=0.

    T-SQL's bare DECIMAL/NUMERIC is DECIMAL(18,0); Spark's is DECIMAL(10,0).
    Measured on Spark 4.2.0: `cast(12345678901234 as decimal)` -> None, and
    `cast(12345678901234 as decimal(18,0))` -> the value. Data loss, not a
    precision nicety.
    """

    def test_a_bare_decimal_cast_gets_t_sqls_precision(self):
        result = _t("SELECT CAST(c AS DECIMAL) FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT CAST(c AS DECIMAL(18,0)) FROM default.W.t")
        self.assertIn("SQ62_DECIMAL_DEFAULT", _rules(result))

    def test_a_bare_numeric_cast_is_the_same(self):
        result = _t("SELECT CAST(c AS NUMERIC) FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT CAST(c AS DECIMAL(18,0)) FROM default.W.t")
        self.assertIn("SQ62_DECIMAL_DEFAULT", _rules(result))

    def test_an_explicit_precision_is_left_exactly_alone(self):
        result = _t("SELECT CAST(c AS decimal(10,2)) FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT CAST(c AS decimal(10,2)) FROM default.W.t")
        self.assertEqual([f for f in result.findings
                          if f.rule.startswith(("SQ60", "SQ62"))], [])

    def test_numeric_with_a_precision_still_becomes_decimal(self):
        out = _t("SELECT CAST(c AS NUMERIC(10,2)) FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT CAST(c AS DECIMAL(10,2)) FROM default.W.t")

    def test_a_bare_decimal_column(self):
        result = _t("CREATE TABLE dbo.t (c DECIMAL)")
        self.assertIn("c DECIMAL(18,0)", result.translated_sql)
        self.assertIn("SQ62_DECIMAL_DEFAULT", _rules(result))

    def test_a_bare_numeric_column(self):
        result = _t("CREATE TABLE dbo.t (c NUMERIC)")
        self.assertIn("c DECIMAL(18,0)", result.translated_sql)

    def test_a_column_with_an_explicit_precision_raises_nothing(self):
        result = _t("CREATE TABLE dbo.t (c decimal(18,2))")
        self.assertIn("c decimal(18,2)", result.translated_sql)
        self.assertEqual([f for f in result.findings
                          if f.rule.startswith(("SQ60", "SQ62"))], [])

    def test_convert_resolves_it_through_the_same_table(self):
        out = _t("SELECT CONVERT(DECIMAL, c) FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT CAST(c AS DECIMAL(18,0)) FROM default.W.t")

    def test_try_convert_resolves_it_through_the_same_table(self):
        out = _t("SELECT TRY_CONVERT(NUMERIC, c) FROM dbo.t").translated_sql
        self.assertEqual(out,
                         "SELECT try_cast(c AS DECIMAL(18,0)) FROM default.W.t")

    def test_inside_a_literal_is_untouched(self):
        out = _t("SELECT 'CAST(c AS DECIMAL)' FROM dbo.t").translated_sql
        self.assertIn("'CAST(c AS DECIMAL)'", out)

    def test_the_finding_names_both_defaults(self):
        detail = next(f.detail for f in _t("SELECT CAST(c AS DECIMAL)").findings
                      if f.rule == "SQ62_DECIMAL_DEFAULT")
        self.assertIn("18", detail)
        self.assertIn("10", detail)
        self.assertIn("NULL", detail)


class LikeCharacterClassTests(unittest.TestCase):
    """D5. Before: every one of these passed through unchanged at flags=0, and
    Spark's LIKE reads `[A-Z]` as four literal characters, so the predicate
    matched nothing where T-SQL matched.

    Measured on Spark 4.2.0, against what T-SQL returns:

        'Alpha' like '[A-Z]%'        -> False   (T-SQL: true)
        'Alpha' like '[^0-9]%'       -> False   (T-SQL: true)
        'a5bc9' like 'a[0-9]_c%'     -> False   (T-SQL: true)
        'Alpha' rlike '^[A-Z].*$'    -> True
        'Alpha' rlike '^[^0-9].*$'   -> True
        'a5bc9' rlike '^a[0-9].c.*$' -> True
    """

    def test_a_range_class(self):
        result = _t("SELECT * FROM dbo.t WHERE c LIKE '[A-Z]%'")
        self.assertIn("c RLIKE '^[A-Z].*$'", result.translated_sql)
        self.assertIn("SQ90_LIKE_CHARACTER_CLASS", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_a_negated_class(self):
        out = _t("SELECT * FROM dbo.t WHERE c LIKE '[^0-9]%'").translated_sql
        self.assertIn("c RLIKE '^[^0-9].*$'", out)

    def test_the_measured_three_argument_shape(self):
        out = _t("SELECT * FROM dbo.t WHERE c LIKE 'a[0-9]_c%'").translated_sql
        self.assertIn("c RLIKE '^a[0-9].c.*$'", out)

    def test_a_plain_pattern_is_left_as_like(self):
        # Correct already, and cheaper: LIKE is a prefix/suffix match Spark
        # can push down, RLIKE is a regex engine per row.
        result = _t("SELECT * FROM dbo.t WHERE c LIKE 'abc%'")
        self.assertIn("c LIKE 'abc%'", result.translated_sql)
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME"])

    def test_not_like_becomes_not_rlike(self):
        out = _t("SELECT * FROM dbo.t WHERE c NOT LIKE '[A-Z]%'").translated_sql
        self.assertIn("c NOT RLIKE '^[A-Z].*$'", out)

    def test_an_underscore_becomes_a_single_character_wildcard(self):
        out = _t("SELECT * FROM dbo.t WHERE c LIKE '[ab]_'").translated_sql
        self.assertIn("RLIKE '^[ab].$'", out)

    def test_a_regex_metacharacter_in_literal_text_is_escaped(self):
        # `.` outside a class is a literal dot in LIKE and "any character" in
        # a regex. The emitted SQL literal doubles the backslash because
        # Spark's parser processes backslash escapes in a string literal.
        out = _t(r"SELECT * FROM dbo.t WHERE c LIKE '[0-9].txt'").translated_sql
        self.assertIn(r"RLIKE '^[0-9]\\.txt$'", out)

    def test_every_metacharacter_listed_in_the_defect(self):
        out = _t("SELECT * FROM dbo.t WHERE c LIKE '[a].*+?(){}|^$'").translated_sql
        self.assertIn(r"RLIKE '^[a]\\.\\*\\+\\?\\(\\)\\{\\}\\|\\^\\$$'", out)

    def test_a_backslash_in_literal_text_survives_as_one_backslash(self):
        out = _t(r"SELECT * FROM dbo.t WHERE c LIKE '[a]\b'").translated_sql
        # regex `\\b` (an escaped backslash then b), written in SQL as `\\\\b`
        self.assertIn(r"RLIKE '^[a]\\\\b$'", out)

    def test_a_bracket_inside_a_class_is_escaped_for_the_regex(self):
        # T-SQL spells a literal '[' as the one-character set `[[]`; Java
        # reads a bare '[' inside a class as a nested class.
        out = _t("SELECT * FROM dbo.t WHERE c LIKE '[[]%'").translated_sql
        self.assertIn(r"RLIKE '^[\\[].*$'", out)

    def test_a_closing_bracket_first_in_the_set_is_a_literal(self):
        out = _t("SELECT * FROM dbo.t WHERE c LIKE '[]]%'").translated_sql
        self.assertIn(r"RLIKE '^[\\]].*$'", out)

    def test_a_percent_inside_a_class_stays_literal(self):
        out = _t("SELECT * FROM dbo.t WHERE c LIKE '[%]a'").translated_sql
        self.assertIn("RLIKE '^[%]a$'", out)

    def test_a_quote_in_the_pattern_is_re_escaped(self):
        # Spark spelling, not T-SQL's: `'^[A-Z]''s$'` is two adjacent
        # literals on Spark, which concatenates them to `^[A-Z]s$` and drops
        # the apostrophe from the regex (SQ16).
        out = _t("SELECT * FROM dbo.t WHERE c LIKE '[A-Z]''s'").translated_sql
        self.assertIn(r"RLIKE '^[A-Z]\'s$'", out)

    def test_an_escape_clause_is_refused_and_says_so(self):
        result = _t("SELECT * FROM dbo.t WHERE c LIKE '[A-Z]!%' ESCAPE '!'")
        self.assertIn("SQ90_LIKE_ESCAPE", _rules(result))
        self.assertIn("LIKE '[A-Z]!%' ESCAPE '!'", result.translated_sql)
        self.assertTrue(result.needs_manual_review)

    def test_an_escape_clause_with_no_class_is_left_alone(self):
        # Spark's LIKE supports ESCAPE, so there is nothing wrong with it.
        result = _t("SELECT * FROM dbo.t WHERE c LIKE 'a!%b' ESCAPE '!'")
        self.assertIn("LIKE 'a!%b' ESCAPE '!'", result.translated_sql)
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME"])

    def test_an_unclosed_class_is_refused(self):
        result = _t("SELECT * FROM dbo.t WHERE c LIKE 'a[bc'")
        self.assertIn("SQ90_LIKE_PATTERN", _rules(result))
        self.assertIn("LIKE 'a[bc'", result.translated_sql)

    def test_an_empty_class_is_refused(self):
        result = _t("SELECT * FROM dbo.t WHERE c LIKE 'a[]b'")
        self.assertIn("SQ90_LIKE_PATTERN", _rules(result))

    def test_a_non_literal_pattern_cannot_be_inspected_and_is_left_alone(self):
        result = _t("SELECT * FROM dbo.t WHERE c LIKE other")
        self.assertIn("c LIKE other", result.translated_sql)
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME"])

    def test_two_predicates_are_both_converted(self):
        out = _t("SELECT * FROM dbo.t WHERE a LIKE '[A-Z]%' "
                 "AND b LIKE '[0-9]%'").translated_sql
        self.assertIn("a RLIKE '^[A-Z].*$'", out)
        self.assertIn("b RLIKE '^[0-9].*$'", out)

    def test_like_inside_a_quoted_identifier_is_not_the_keyword(self):
        result = _t("SELECT [like] FROM dbo.t")
        self.assertIn("`like`", result.translated_sql)
        self.assertNotIn("SQ90_LIKE_CHARACTER_CLASS", _rules(result))

    def test_like_inside_a_literal_is_untouched(self):
        out = _t("SELECT 'c LIKE [A-Z]' FROM dbo.t").translated_sql
        self.assertIn("'c LIKE [A-Z]'", out)

    def test_the_finding_warns_about_collation(self):
        detail = next(f.detail for f in
                      _t("SELECT * FROM dbo.t WHERE c LIKE '[A-Z]%'").findings
                      if f.rule == "SQ90_LIKE_CHARACTER_CLASS")
        self.assertIn("collation", detail)
        self.assertIn("RLIKE", detail)


class SubstringFromZeroTests(unittest.TestCase):
    """D6. Before: `SELECT SUBSTRING(s, 0, CHARINDEX('-', s)) FROM dbo.t` came
    back as `SUBSTRING(s, 0, locate('-', s))` at flags=0, one character too
    long.

    T-SQL's SUBSTRING with a start of 0 consumes one slot before the string,
    so the length counts from position 0. Measured on 'a-b-c' with
    CHARINDEX('-') = 2: T-SQL SUBSTRING(s,0,2) -> 'a', Spark substring(s,0,2)
    -> 'a-', and Spark substring(s, 1, 2-1) -> 'a'.
    """

    def test_the_measured_idiom(self):
        result = _t("SELECT SUBSTRING(s, 0, CHARINDEX('-', s)) FROM dbo.t")
        self.assertIn("substring(s, 1, (locate('-', s)) - 1)",
                      result.translated_sql)
        self.assertIn("SQ22_SUBSTRING_ZERO", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_a_literal_length_is_worked_out_rather_than_left_as_arithmetic(self):
        out = _t("SELECT SUBSTRING(s, 0, 2) FROM dbo.t").translated_sql
        self.assertIn("substring(s, 1, 1)", out)

    def test_a_start_of_one_is_identical_in_both_and_untouched(self):
        result = _t("SELECT SUBSTRING(s, 1, 5) FROM dbo.t")
        self.assertIn("SUBSTRING(s, 1, 5)", result.translated_sql)
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME"])

    def test_a_computed_start_is_left_alone(self):
        # Not decidable from the text: it is 0-or-less that differs, and
        # `locate(...) + 1` is never 0. See the report.
        result = _t("SELECT SUBSTRING(s, CHARINDEX('-', s) + 1, 5) FROM dbo.t")
        self.assertNotIn("SQ22_SUBSTRING_ZERO", _rules(result))

    def test_a_negative_start_is_refused_not_guessed(self):
        result = _t("SELECT SUBSTRING(s, -2, 5) FROM dbo.t")
        self.assertIn("SQ22_SUBSTRING_START", _rules(result))
        self.assertIn("SUBSTRING(s, -2, 5)", result.translated_sql)
        self.assertTrue(result.needs_manual_review)

    def test_an_expression_argument_is_preserved_verbatim(self):
        out = _t("SELECT SUBSTRING(a + b, 0, f(x)) FROM dbo.t").translated_sql
        self.assertIn("substring(a + b, 1, (f(x)) - 1)", out)

    def test_a_nested_substring_is_reached(self):
        out = _t("SELECT SUBSTRING(SUBSTRING(s, 0, 4), 0, 2)").translated_sql
        self.assertEqual(out, "SELECT substring(substring(s, 1, 3), 1, 1)")

    def test_a_two_argument_substring_is_not_the_t_sql_form(self):
        result = _t("SELECT SUBSTRING(s, 2) FROM dbo.t")
        self.assertIn("SUBSTRING(s, 2)", result.translated_sql)
        self.assertNotIn("SQ22_SUBSTRING_ZERO", _rules(result))

    def test_inside_a_literal_is_untouched(self):
        out = _t("SELECT 'SUBSTRING(s, 0, 2)' FROM dbo.t").translated_sql
        self.assertIn("'SUBSTRING(s, 0, 2)'", out)

    def test_the_finding_quotes_the_measurement(self):
        detail = next(f.detail for f in _t("SELECT SUBSTRING(s, 0, 2)").findings
                      if f.rule == "SQ22_SUBSTRING_ZERO")
        self.assertIn("one character", detail)
        self.assertIn("'a-'", detail)


class GoBatchSplitTests(unittest.TestCase):
    """D7. Before, measured:

        SELECT TOP 5 * FROM dbo.a
        GO
        SELECT * FROM dbo.b
        -> SELECT * FROM default.W.a
           SELECT * FROM default.W.b LIMIT 5

    at flags=0. Two statements ran together with no separator, and the row cap
    that belonged to the first landed on the second. `GO` was deleted outright,
    and deleting a statement separator is not the same as removing a keyword
    Spark does not know.
    """

    def test_the_reproduction(self):
        result = _t("SELECT TOP 5 * FROM dbo.a\nGO\nSELECT * FROM dbo.b")
        self.assertEqual(
            result.translated_sql,
            "SELECT * FROM default.W.a LIMIT 5;\nSELECT * FROM default.W.b")
        self.assertIn("SQ02_GO_BATCH", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_the_limit_stays_on_its_own_statement_when_the_second_has_a_top(self):
        out = _t("SELECT TOP 5 * FROM dbo.a\nGO\n"
                 "SELECT TOP 9 * FROM dbo.b").translated_sql
        self.assertEqual(
            out,
            "SELECT * FROM default.W.a LIMIT 5;\n"
            "SELECT * FROM default.W.b LIMIT 9")

    def test_a_statement_already_terminated_does_not_get_a_second_semicolon(self):
        out = _t("SELECT 1;\nGO\nSELECT 2").translated_sql
        self.assertEqual(out, "SELECT 1;\nSELECT 2")

    def test_a_trailing_go_separates_nothing_and_adds_nothing(self):
        out = _t("SELECT 1\nGO\n").translated_sql
        self.assertEqual(out, "SELECT 1\n")

    def test_a_leading_go_terminates_nothing(self):
        out = _t("GO\nSELECT 1").translated_sql
        self.assertEqual(out, "SELECT 1")

    def test_two_go_lines_in_a_row_add_one_separator(self):
        out = _t("SELECT 1\nGO\nGO\nSELECT 2").translated_sql
        self.assertEqual(out, "SELECT 1;\nSELECT 2")

    def test_three_batches(self):
        out = _t("SELECT 1\nGO\nSELECT 2\nGO\nSELECT 3").translated_sql
        self.assertEqual(out, "SELECT 1;\nSELECT 2;\nSELECT 3")

    def test_the_word_go_inside_an_identifier_is_still_left_alone(self):
        result = _t("SELECT [go_live_date] FROM dbo.[GO_LOG]")
        self.assertNotIn("SQ02_GO_BATCH", _rules(result))

    def test_the_finding_says_it_separates_rather_than_deletes(self):
        detail = next(f.detail for f in
                      _t("SELECT 1\nGO\nSELECT 2").findings
                      if f.rule == "SQ02_GO_BATCH")
        self.assertIn(";", detail)
        self.assertIn("separator", detail)


class QualifiedColumnTests(unittest.TestCase):
    """D8. Before, measured:

        SELECT dbo.t.c FROM dbo.t
          -> SELECT default.dbo_t.c FROM default.W.t      flags=0

    The FROM clause is right and the column reference is not: SQ11 read
    `dbo.t.c` as `database.schema.object` and built a table name out of a
    column reference, so `default.dbo_t.c` resolves to nothing.

    `a.b.c` is genuinely ambiguous in T-SQL -- `database.schema.object` in an
    object position, `schema.table.column` anywhere else -- so position is
    what decides it here, exactly as it already decides `timestamp` between
    rowversion and a datetime.
    """

    def test_the_reproduction(self):
        result = _t("SELECT dbo.t.c FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT t.c FROM default.W.t")
        self.assertIn("SQ15_QUALIFIED_COLUMN", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_the_table_reading_survives_in_an_object_position(self):
        result = _t("SELECT * FROM AcmeDW.dbo.claim")
        self.assertEqual(result.translated_sql,
                         "SELECT * FROM default.AcmeDW.claim")
        self.assertIn("SQ11_THREE_PART_NAME", _rules(result))

    def test_a_create_target_is_still_a_table(self):
        out = _t("CREATE TABLE AcmeDW.dbo.claim (c INT)").translated_sql
        self.assertIn("CREATE TABLE default.AcmeDW.claim", out)

    def test_a_join_predicate_is_a_column_reference(self):
        out = _t("SELECT * FROM dbo.t JOIN dbo.u ON dbo.t.c = dbo.u.d").translated_sql
        self.assertEqual(
            out,
            "SELECT * FROM default.W.t JOIN default.W.u ON t.c = u.d")

    def test_a_where_clause_is_a_column_reference(self):
        out = _t("SELECT 1 FROM dbo.t WHERE dbo.t.c = 2").translated_sql
        self.assertIn("WHERE t.c = 2", out)

    def test_the_table_part_keeps_the_quoting_it_needs(self):
        out = _t("SELECT dbo.[my table].c FROM dbo.[my table]").translated_sql
        self.assertEqual(out, "SELECT `my table`.c FROM default.W.`my table`")

    def test_a_trim_from_is_not_an_object_position(self):
        # The same guard the two-part rule needs: the FROM belongs to TRIM.
        out = _t("SELECT TRIM(BOTH ' ' FROM dbo.t.name) FROM dbo.t").translated_sql
        self.assertIn("TRIM(BOTH ' ' FROM t.name)", out)

    def test_four_parts_are_still_a_linked_server(self):
        result = _t("SELECT * FROM a.b.c.d")
        self.assertIn("SQ11_LINKED_SERVER", _rules(result))
        self.assertIn("a.b.c.d", result.translated_sql)

    def test_a_three_part_function_call_is_refused_not_guessed(self):
        result = _t("SELECT db.dbo.fn(x) FROM dbo.t")
        self.assertIn("SQ15_QUALIFIED_CALL", _rules(result))
        self.assertIn("db.dbo.fn(x)", result.translated_sql)
        self.assertTrue(result.needs_manual_review)

    def test_inside_a_literal_is_untouched(self):
        out = _t("SELECT 'dbo.t.c' FROM dbo.t").translated_sql
        self.assertIn("'dbo.t.c'", out)

    def test_the_finding_names_both_readings(self):
        detail = next(f.detail for f in _t("SELECT dbo.t.c FROM dbo.t").findings
                      if f.rule == "SQ15_QUALIFIED_COLUMN")
        self.assertIn("column", detail)
        self.assertIn("t.c", detail)


# Enough SQL to reach every registered rule at least once, so the sweep below
# is a statement about the ruleset rather than about one construct.
_RULE_SWEEP_CORPUS = (
    "SELECT CONCAT(a, b) FROM dbo.t",
    "SELECT LOG(x, 10) FROM dbo.t",
    "SELECT LOG10(x), SUBSTRING(s, 1, 5) FROM dbo.t",
    "SELECT CAST(c AS varchar(20)) FROM dbo.t",
    "SELECT CAST(c AS DECIMAL) FROM dbo.t",
    "SELECT * FROM dbo.t WHERE c LIKE '[A-Z]%'",
    "SELECT SUBSTRING(s, 0, CHARINDEX('-', s)) FROM dbo.t",
    "SELECT TOP 5 * FROM dbo.a\nGO\nSELECT * FROM dbo.b",
    "SELECT dbo.t.c FROM dbo.t",
    "SELECT 'a' + b FROM dbo.t",
    "SELECT DATEDIFF(day, a, b), ISNULL(x, GETDATE()) FROM dbo.t",
    "SELECT CONVERT(nvarchar(50), c), TRY_CONVERT(int, d) FROM dbo.t",
    "SELECT DATEADD(day, 1, d), IIF(a = 1, 'x', 'y') FROM dbo.t",
    "CREATE TABLE dbo.t ([c] [nvarchar](50) NOT NULL, amount MONEY,"
    " id BIGINT IDENTITY(1,1), CONSTRAINT pk PRIMARY KEY (id)) ON [PRIMARY]",
    "CREATE OR ALTER VIEW dbo.v WITH SCHEMABINDING AS SELECT 1 AS x",
    "ALTER TABLE dbo.t ADD CONSTRAINT pk PRIMARY KEY (id);",
    "SELECT a INTO dbo.u FROM dbo.t",
    "SELECT N'x' FROM dbo.t",
    "SELECT [it's], [a]]b] FROM dbo.t",
)


def _apply_rule(rule, sql, findings=None):
    """One rule, with the arguments `translate` would give it."""
    extra = {}
    if rule in tsql._CATALOG_AWARE:
        extra["catalog"] = "default"
    if rule in tsql._ITEM_AWARE:
        extra["item"] = "W"
    return rule(sql, [] if findings is None else findings, **extra)


class FixedPointLandmineTests(unittest.TestCase):
    """F-1 follow-up. `rule_log`'s rewrite is its own inverse, measured:

        SELECT LOG(x, 10) -> log(10, x) -> log(x, 10) -> log(10, x) ...

    It alternates forever. That is harmless where it stands -- the rule
    recurses into its own arguments and is applied exactly once -- and fatal
    the moment anyone routes it through `_to_fixed_point`, which re-runs a
    rule until the text stops changing: it would never converge, burn the
    8-pass budget, and then report SQ03_NESTING_TOO_DEEP for a one-level
    expression. That is the previous batch's F2 defect reached from the other
    direction.

    These tests exist to make that landmine impossible to step on without a
    test going red.
    """

    def test_the_self_inverse_set_is_declared_and_registered(self):
        declared = tsql._SELF_INVERSE_RULES
        self.assertIn(tsql.rule_log, declared)
        for rule in declared:
            self.assertIn(rule, tsql.RULES,
                          f"{rule.__name__} is declared self-inverse but is "
                          f"not registered, so the set has rotted")

    def test_no_self_inverse_rule_is_routed_through_the_fixed_point(self):
        seen = []
        original = tsql._to_fixed_point

        def spy(*args, **kwargs):
            seen.append(True)
            return original(*args, **kwargs)

        tsql._to_fixed_point = spy
        try:
            # Positive control first: without it a spy that never fires would
            # make the real assertion below vacuous. rule_concat DOES use the
            # fixed point, through _rewrite_call.
            _apply_rule(tsql.rule_concat, "SELECT CONCAT(a, b) FROM dbo.t")
            self.assertTrue(seen, "the spy never fired, so this test proves "
                                  "nothing about the rules it checks")
            for rule in tsql._SELF_INVERSE_RULES:
                for sql in _RULE_SWEEP_CORPUS:
                    del seen[:]
                    _apply_rule(rule, sql)
                    self.assertEqual(
                        seen, [],
                        f"{rule.__name__} reached _to_fixed_point on {sql!r}. "
                        f"Its rewrite is its own inverse, so the loop will "
                        f"alternate until the pass budget burns and then "
                        f"report SQ03_NESTING_TOO_DEEP for a flat expression")
        finally:
            tsql._to_fixed_point = original

    def test_every_rule_not_declared_self_inverse_is_idempotent(self):
        for rule in tsql.RULES:
            if rule in tsql._SELF_INVERSE_RULES:
                continue
            for sql in _RULE_SWEEP_CORPUS:
                with self.subTest(rule=rule.__name__, sql=sql[:40]):
                    once = _apply_rule(rule, sql)
                    self.assertEqual(
                        once, _apply_rule(rule, once),
                        f"{rule.__name__} changed its own output. Either it "
                        f"belongs in _SELF_INVERSE_RULES with the argument "
                        f"written down, or it is a bug")


class DeclaredNonIdempotencyTests(unittest.TestCase):
    """The non-idempotency this batch introduced, pinned so it is a known
    asserted property rather than a surprise.

    Nothing in the pipeline translates twice, and these are the only two
    places where doing so would change a VALUE rather than a spelling.
    """

    def test_rule_log_alternates(self):
        one = tsql.rule_log("SELECT LOG(x, 10)", [])
        two = tsql.rule_log(one, [])
        three = tsql.rule_log(two, [])
        self.assertEqual(one, "SELECT log(10, x)")
        self.assertEqual(two, "SELECT log(x, 10)")
        self.assertEqual(three, one)

    def test_a_second_whole_translate_swaps_log_back(self):
        one = _t("SELECT LOG(x, 10) FROM dbo.t").translated_sql
        two = _t(one).translated_sql
        self.assertEqual(one, "SELECT log(10, x) FROM default.W.t")
        self.assertEqual(two, "SELECT log(x, 10) FROM default.W.t")

    def test_a_second_whole_translate_re_reads_SQ40s_concat_as_a_CONCAT_call(self):
        # `+` propagates NULL and CONCAT does not, so `concat` and
        # `concat_ws('', ...)` are the right targets for the two -- they are
        # different functions on purpose. Only a SECOND run conflates them,
        # by reading SQ40's output as if the user had written CONCAT.
        one = _t("SELECT 'a' + b FROM dbo.t").translated_sql
        two = _t(one).translated_sql
        self.assertEqual(one, "SELECT concat('a', b) FROM default.W.t")
        self.assertEqual(two, "SELECT concat_ws('', 'a', b) FROM default.W.t")

    def test_a_concat_the_user_wrote_is_stable_on_a_second_run(self):
        one = _t("SELECT CONCAT(a, b) FROM dbo.t").translated_sql
        self.assertEqual(one, _t(one).translated_sql)

    def test_every_other_fix_in_this_batch_survives_a_second_translate(self):
        for sql in ("SELECT CAST(c AS varchar(20)) FROM dbo.t",
                    "SELECT CAST(c AS DECIMAL) FROM dbo.t",
                    "SELECT * FROM dbo.t WHERE c LIKE '[A-Z]%'",
                    "SELECT SUBSTRING(s, 0, 4) FROM dbo.t",
                    "SELECT TOP 5 * FROM dbo.a\nGO\nSELECT * FROM dbo.b",
                    "SELECT dbo.t.c FROM dbo.t"):
            with self.subTest(sql=sql[:40]):
                one = _t(sql).translated_sql
                self.assertEqual(one, _t(one).translated_sql)


class SelectIntoPerStatementTests(unittest.TestCase):
    """F-2. `rule_select_into` built the CTAS from the WHOLE document, so in a
    multi-statement file the created table was populated from the wrong query.
    Measured before the fix, and identical at 4371cee so it is pre-existing:

        SELECT * FROM dbo.b;
        SELECT a INTO dbo.u FROM dbo.t
        -> CREATE TABLE default.W.u AS SELECT * FROM default.W.b;
           SELECT a FROM default.W.t
           flags=0   PASS

    `dbo.u` is created from `dbo.b` instead of `dbo.t`, and the statement that
    actually had the INTO has silently lost its target. A table with the wrong
    contents, graded PASS.

    D7 makes this reachable from every `GO`-separated export, and it also
    supplies the fix: `_statement_bounds` already finds the `;`-delimited
    statement an offset belongs to.
    """

    def test_the_reproduction(self):
        result = _t("SELECT * FROM dbo.b;\nSELECT a INTO dbo.u FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "SELECT * FROM default.W.b;\n"
            "CREATE TABLE default.W.u AS SELECT a FROM default.W.t")
        self.assertIn("SQ51_SELECT_INTO", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_the_same_thing_across_a_go(self):
        out = _t("SELECT * FROM dbo.b\nGO\n"
                 "SELECT a INTO dbo.u FROM dbo.t").translated_sql
        self.assertEqual(
            out,
            "SELECT * FROM default.W.b;\n"
            "CREATE TABLE default.W.u AS SELECT a FROM default.W.t")

    def test_the_into_in_the_first_statement_leaves_the_second_alone(self):
        out = _t("SELECT a INTO dbo.u FROM dbo.t;\n"
                 "SELECT * FROM dbo.b").translated_sql
        self.assertEqual(
            out,
            "CREATE TABLE default.W.u AS SELECT a FROM default.W.t;\n"
            "SELECT * FROM default.W.b")

    def test_the_single_statement_case_is_exactly_as_before(self):
        result = _t("SELECT a INTO dbo.u FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "CREATE TABLE default.W.u AS SELECT a FROM default.W.t")
        self.assertEqual(result.flags, 0)

    def test_two_statements_each_with_an_into(self):
        result = _t("SELECT a INTO dbo.u FROM dbo.t;\n"
                    "SELECT b INTO dbo.v FROM dbo.w")
        self.assertEqual(
            result.translated_sql,
            "CREATE TABLE default.W.u AS SELECT a FROM default.W.t;\n"
            "CREATE TABLE default.W.v AS SELECT b FROM default.W.w")
        self.assertEqual(_rules(result).count("SQ51_SELECT_INTO"), 2)

    def test_a_subquery_into_is_still_refused_and_no_longer_abandons_the_file(self):
        result = _t("SELECT a INTO dbo.u FROM dbo.t;\n"
                    "SELECT * FROM (SELECT c INTO x FROM dbo.w) y")
        self.assertIn("SQ51_INTO_SUBQUERY", _rules(result))
        self.assertIn("CREATE TABLE default.W.u AS SELECT a FROM default.W.t",
                      result.translated_sql)

    def test_two_depth_zero_intos_in_one_statement_are_refused_not_crashed(self):
        # Not valid T-SQL, but the pattern can see it, and wrapping the same
        # statement twice would raise on overlapping replacements.
        result = _t("SELECT a INTO dbo.u FROM dbo.t INTO dbo.v")
        self.assertIn("SQ51_INTO_TWICE", _rules(result))

    def test_the_finding_names_the_statement_it_wrapped(self):
        detail = next(f.detail for f in
                      _t("SELECT * FROM dbo.b;\nSELECT a INTO dbo.u FROM dbo.t")
                      .findings if f.rule == "SQ51_SELECT_INTO")
        self.assertIn("default.W.u", detail)
        self.assertIn("default.W.t", detail)


class GoWithARepeatCountTests(unittest.TestCase):
    """F-3. `_GO_RE` requires GO alone on its line, so T-SQL's batch-repeat
    form never matched it. Measured before:

        SELECT 1
        GO 5
        SELECT 2
        -> unchanged, findings=[], flags=0, PASS

    `GO 5` reaches the output verbatim and Spark's parser stops at it: loud at
    run time, silent at grading, which is the one thing this module must not
    do. `GO n` means "run the batch n times", which is not expressible in
    Spark SQL, so it is flagged rather than invented.
    """

    def test_a_repeat_count_is_flagged(self):
        result = _t("SELECT 1\nGO 5\nSELECT 2")
        self.assertIn("SQ02_GO_COUNT", _rules(result))
        self.assertTrue(result.needs_manual_review)

    def test_it_is_left_exactly_as_written(self):
        sql = "SELECT 1\nGO 5\nSELECT 2"
        self.assertEqual(_t(sql).translated_sql, sql)

    def test_a_trailing_semicolon_form(self):
        self.assertIn("SQ02_GO_COUNT", _rules(_t("SELECT 1\nGO 3;\nSELECT 2")))

    def test_lower_case_is_the_same_directive_in_both_forms(self):
        # `go` is what sqlcmd and SSMS accept too. The plain form was
        # case-SENSITIVE before this commit, so a lower-case `go` alone on a
        # line passed straight through with no finding -- the same silent
        # hole, reached by a different spelling.
        self.assertIn("SQ02_GO_COUNT", _rules(_t("SELECT 1\ngo 2\nSELECT 2")))
        plain = _t("SELECT 1\ngo\nSELECT 2")
        self.assertEqual(plain.translated_sql, "SELECT 1;\nSELECT 2")
        self.assertIn("SQ02_GO_BATCH", _rules(plain))

    def test_a_plain_go_is_still_split_and_not_flagged(self):
        result = _t("SELECT 1\nGO\nSELECT 2")
        self.assertEqual(result.translated_sql, "SELECT 1;\nSELECT 2")
        self.assertNotIn("SQ02_GO_COUNT", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_both_forms_in_one_file(self):
        result = _t("SELECT 1\nGO\nSELECT 2\nGO 4\nSELECT 3")
        self.assertIn("SQ02_GO_BATCH", _rules(result))
        self.assertIn("SQ02_GO_COUNT", _rules(result))
        self.assertIn("SELECT 1;", result.translated_sql)
        self.assertIn("GO 4", result.translated_sql)

    def test_a_repeat_count_inside_a_literal_is_not_a_batch_directive(self):
        result = _t("SELECT 'GO 5' AS x FROM dbo.t")
        self.assertNotIn("SQ02_GO_COUNT", _rules(result))

    def test_the_finding_names_the_count_and_why_it_cannot_be_written(self):
        detail = next(f.detail for f in _t("SELECT 1\nGO 5\nSELECT 2").findings
                      if f.rule == "SQ02_GO_COUNT")
        self.assertIn("5", detail)
        self.assertIn("times", detail)


class TopWhitespaceCleanupTests(unittest.TestCase):
    """T6. `rule_top` finished with

        re.sub(r"(\\bSELECT(?:\\s+DISTINCT)?)\\s{2,}", r"\\1 ", cleaned,
               flags=re.I)

    document-wide, on the RAW text, unconditionally -- so it ran on every
    object translated, including the ones with no TOP anywhere. Measured
    before the fix, each with findings=[]:

      SELECT 'SELECT   me' AS a FROM dbo.t
        -> SELECT 'SELECT me' AS a FROM dbo.t
      -- SELECT   a\\nSELECT b FROM dbo.t
        -> -- SELECT a\\nSELECT b FROM dbo.t
      SELECT\\n    a,\\n    b\\nFROM dbo.t
        -> SELECT a,\\n    b\\nFROM dbo.t

    The first is a user's string value rewritten with nothing reported. The
    third marks essentially every multi-line warehouse object in an estate
    as changed by a rule that did nothing for it.
    """

    def _top(self, sql):
        findings = []
        return tsql.rule_top(sql, findings), findings

    def test_a_literal_holding_the_word_select_is_byte_identical(self):
        sql = "SELECT 'SELECT   me' AS a FROM dbo.t"
        self.assertEqual(self._top(sql), (sql, []))

    def test_a_comment_holding_the_word_select_is_byte_identical(self):
        sql = "-- SELECT   a\nSELECT b FROM dbo.t"
        self.assertEqual(self._top(sql), (sql, []))

    def test_a_multi_line_select_keeps_its_layout(self):
        sql = "SELECT\n    a,\n    b\nFROM dbo.t"
        self.assertEqual(self._top(sql), (sql, []))

    def test_a_literal_survives_a_top_in_the_same_statement(self):
        """The cleanup has to be local to the TOP, not merely conditional on
        one existing somewhere in the document."""
        out, findings = self._top("SELECT TOP 5 'SELECT   me' FROM dbo.t")
        self.assertEqual(out, "SELECT 'SELECT   me' FROM dbo.t LIMIT 5")
        self.assertEqual([f.rule for f in findings], ["SQ50_TOP"])

    def test_a_later_statement_is_not_reflowed_by_an_earlier_top(self):
        out, _ = self._top("SELECT TOP 5 a FROM dbo.t;\n"
                           "SELECT\n   b\nFROM dbo.u")
        self.assertEqual(out, "SELECT a FROM dbo.t LIMIT 5;\n"
                              "SELECT\n   b\nFROM dbo.u")

    def test_the_double_space_the_removal_leaves_is_still_closed(self):
        out, _ = self._top("SELECT TOP 5 a FROM dbo.t")
        self.assertEqual(out, "SELECT a FROM dbo.t LIMIT 5")

    def test_distinct_keeps_its_one_space(self):
        out, _ = self._top("SELECT DISTINCT TOP 5 a FROM dbo.t")
        self.assertEqual(out, "SELECT DISTINCT a FROM dbo.t LIMIT 5")

    def test_a_top_on_its_own_line_takes_the_line_with_it(self):
        out, _ = self._top("SELECT\n  TOP 5\n  a\nFROM dbo.t")
        self.assertEqual(out, "SELECT\n  a\nFROM dbo.t LIMIT 5")

    def test_a_refused_top_changes_no_whitespace_at_all(self):
        """A flagged TOP is not a replacement, so nothing may be reflowed."""
        sql = "SELECT  TOP 5 PERCENT  a FROM dbo.t"
        out, findings = self._top(sql)
        self.assertEqual(out, sql)
        self.assertEqual([f.rule for f in findings], ["SQ50_TOP_UNSUPPORTED"])


if __name__ == "__main__":
    unittest.main()
