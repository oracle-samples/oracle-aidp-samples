import ast
import unittest

from fabric_aidp.translate import m_runtime
from fabric_aidp.translate.m_expr import (_FRAME_FUNCTIONS, Untranslatable,
                                          decode_escapes, translate_expression)


def _t(text, scope=None, frame="df", helpers=None):
    """`frame` is the DataFrame variable a type-dispatching helper reads.
    Pass frame=None to assert a function refuses without one."""
    return translate_expression(text, scope, frame=frame,
                                helpers=set() if helpers is None else helpers)


class LiteralAndColumnTests(unittest.TestCase):
    def test_column_reference(self):
        self.assertEqual(_t("each [Year]"), "F.col('Year')")

    def test_column_name_with_a_space(self):
        self.assertEqual(_t("each [Order Date]"), "F.col('Order Date')")

    def test_string_literal(self):
        self.assertEqual(_t('each "hi"'), "F.lit('hi')")

    def test_doubled_quote_in_a_string_literal(self):
        self.assertEqual(_t('each "say ""hi"""'), 'F.lit(\'say "hi"\')')

    def test_booleans_and_null(self):
        self.assertEqual(_t("each true"), "F.lit(True)")
        self.assertEqual(_t("each null"), "F.lit(None)")

    def test_bare_expression_without_the_each_keyword(self):
        self.assertEqual(_t("[Year]"), "F.col('Year')")


class QuotedIdentifierTests(unittest.TestCase):
    """`#"..."` is M's syntax for a name, not part of the name.

    It used to be carried into the column reference verbatim, so
    `[#"order-id"]` became `F.col('#"order-id"')` -- a column no frame has,
    in a step graded a rewrite. The form is *mandatory* in M for any name
    that is not a plain identifier, so the columns it broke were exactly the
    ones that need it: every hyphen, every space, every `#`.
    """

    def test_a_hyphenated_name_loses_the_quoting_syntax(self):
        self.assertEqual(_t('each [#"order-id"]'), "F.col('order-id')")

    def test_a_name_with_a_space(self):
        self.assertEqual(_t('each [#"line total"]'), "F.col('line total')")

    def test_a_doubled_quote_is_one_quote_in_the_name(self):
        # M's escape for `"` inside a quoted identifier, the same as in a
        # string literal: `#"a""b"` names the column `a"b`.
        self.assertEqual(_t('each [#"a""b"]'), 'F.col(\'a"b\')')

    def test_a_quoted_name_may_contain_a_closing_bracket(self):
        # The token pattern has to know about `#"..."` for this: a general
        # "anything but `]`" branch ends the token in the middle of the name.
        self.assertEqual(_t('each [#"data]1"]'), "F.col('data]1')")

    def test_whitespace_inside_the_quotes_is_part_of_the_name(self):
        # Stripping applies between the brackets and the name, never inside
        # the quotes -- `#"  a  "` is a column called "  a  ".
        self.assertEqual(_t('each [#"  a  "]'), "F.col('  a  ')")

    def test_an_m_escape_inside_a_quoted_name_is_decoded(self):
        self.assertEqual(_t('each [#"a#(tab)b"]'), "F.col('a\\tb')")

    def test_an_unknown_escape_in_a_name_refuses_rather_than_passing_through(self):
        with self.assertRaises(Untranslatable):
            _t('each [#"a#(zzz)b"]')

    def test_quoted_names_survive_a_compound_expression(self):
        self.assertEqual(
            _t('each [plain] + [#"order-id"]'),
            "((F.col('plain')) + (F.col('order-id')))")

    def test_a_generalized_identifier_is_still_taken_verbatim(self):
        # No quoting syntax, so nothing to strip and no escapes to decode.
        self.assertEqual(_t("each [Order Date]"), "F.col('Order Date')")

    def test_an_empty_field_access_is_refused(self):
        # `[]` is M's empty record; `F.col('')` names no column at all.
        with self.assertRaises(Untranslatable):
            _t("each []")


class OperatorTests(unittest.TestCase):
    def test_ampersand_is_concat_not_addition(self):
        # M's & is string concatenation. Emitting + here is the exact class of
        # silent wrong answer the T-SQL rule set already guards against.
        self.assertEqual(_t("each [a] & [b]"), "F.concat(F.col('a'), F.col('b'))")

    def test_concat_uses_concat_not_concat_ws(self):
        # concat_ws drops nulls and yields a short string; M yields null.
        self.assertNotIn("concat_ws", _t("each [a] & [b]"))

    def test_arithmetic_precedence_is_preserved(self):
        result = _t("each [a] + [b] * 2")
        self.assertEqual(result, "((F.col('a')) + (((F.col('b')) * (F.lit(2)))))")

    def test_equality_is_null_safe(self):
        # M: null = null is true. Spark ==: NULL, and filter() drops the row.
        self.assertEqual(_t("each [a] = [b]"), "F.col('a').eqNullSafe(F.col('b'))")

    def test_not_equal_keeps_null_rows(self):
        # M: null <> "x" is true, so a "does not equal" filter keeps nulls.
        self.assertEqual(_t('each [a] <> "x"'), "(~F.col('a').eqNullSafe(F.lit('x')))")

    def test_comparison_to_null_becomes_isnull(self):
        self.assertEqual(_t("each [a] = null"), "F.col('a').isNull()")

    def test_inequality_to_null_becomes_isnotnull(self):
        self.assertEqual(_t("each [a] <> null"), "F.col('a').isNotNull()")

    def test_null_on_the_left_is_handled_like_the_right(self):
        self.assertEqual(_t("each null = [a]"), "F.col('a').isNull()")
        self.assertEqual(_t("each null <> [a]"), "F.col('a').isNotNull()")

    def test_ordering_comparisons_still_propagate_null(self):
        # Only = and <> are two-valued in M; < > <= >= return null.
        self.assertEqual(_t("each [a] > null"), "((F.col('a')) > (F.lit(None)))")

    def test_null_comparison_shapes_are_two_valued_like_m(self):
        """Issue #1 called this wrong. It is not; these pin why.

        M's = and <> are two-valued: null = null is true and null <> "x" is
        true. Only the ordering operators propagate null, and they are left
        alone below.
        """
        cases = {
            "each [x] <> null": "F.col('x').isNotNull()",
            "each [x] = null": "F.col('x').isNull()",
            "each null <> [x]": "F.col('x').isNotNull()",
            # A parenthesised null is not the null literal's source text, so
            # _equality falls through to eqNullSafe instead of isNotNull. It
            # still means the same rows: `x <=> NULL` is true exactly when x
            # is null and is never itself null, so its negation is
            # isNotNull() spelled differently.
            "each [x] <> (null)": "(~F.col('x').eqNullSafe((F.lit(None))))",
            "each null = null": "F.lit(True)",
            "each null <> null": "F.lit(False)",
            "each [x] > null": "((F.col('x')) > (F.lit(None)))",
            "each [x] >= null": "((F.col('x')) >= (F.lit(None)))",
        }
        for source, want in cases.items():
            with self.subTest(source=source):
                self.assertEqual(_t(source), want)

    def test_a_chained_comparison_is_refused(self):
        """M groups chained comparisons right to left; this parser groups them
        left to right, so the two disagree.

        `each [a] = null = null` is M's `[a] = (null = null)`, i.e. `[a] = true`.
        Grouped left to right it is `([a] = null) = null`, which emitted
        `F.col('a').isNull().isNull()` -- always false, under a PASS. M's
        published grammar is right-recursive at both comparison levels, but
        there is no Power BI here to confirm the implementation follows it.
        Unverifiable semantics is a reason to refuse, not to re-group on a
        reading that might be wrong.
        """
        for source in ("each [a] = null = null",
                       "each [a] = [b] = [c]",
                       "each [x] = null = true",
                       "each [a] <> null <> null",
                       "each [a] < [b] = null",
                       "each [a] < [b] < [c]"):
            with self.subTest(source=source):
                with self.assertRaisesRegex(Untranslatable, "chained comparison"):
                    _t(source)

    def test_one_comparison_per_level_still_translates(self):
        """The boundary: parentheses and and/or each start a fresh level, so
        only a bare chain of two operators at one level is refused."""
        cases = {
            # Parenthesised: the inner = is a level down, so neither level chains.
            "each ([a] = [b]) = [c]":
                "((F.col('a').eqNullSafe(F.col('b')))).eqNullSafe(F.col('c'))",
            "each [a] = ([b] = [c])":
                "F.col('a').eqNullSafe((F.col('b').eqNullSafe(F.col('c'))))",
            "each [x] <> null": "F.col('x').isNotNull()",
            "each [a] = [b]": "F.col('a').eqNullSafe(F.col('b'))",
            "each [a] < [b]": "((F.col('a')) < (F.col('b')))",
            "each [x] >= 3": "((F.col('x')) >= (F.lit(3)))",
            "each [a] = 1 and [b] = 2":
                "((F.col('a').eqNullSafe(F.lit(1))) & "
                "(F.col('b').eqNullSafe(F.lit(2))))",
            # The real corpus shape, which must keep working.
            'each [Points] <> null and [Points] <> ""':
                "((F.col('Points').isNotNull()) & "
                "((~F.col('Points').eqNullSafe(F.lit('')))))",
        }
        for source, want in cases.items():
            with self.subTest(source=source):
                self.assertEqual(_t(source), want)

    def test_an_operand_containing_a_null_is_not_treated_as_the_null(self):
        """Issue #1's likeliest reading was that `null in (left, right)` could
        match a substring of a longer operand. It cannot: that is a tuple, so
        the test is whole-operand equality. Both operands below contain
        `F.lit(None)` inside them and neither is mistaken for it.
        """
        self.assertEqual(
            _t("each (if [a] then null else [b]) <> null"),
            "((F.when(F.col('a'), F.lit(None)).otherwise(F.col('b'))))"
            ".isNotNull()")
        # M's & yields null when either side is null, so this is true.
        self.assertEqual(
            _t("each [a] & null = null"),
            "(F.concat(F.col('a'), F.lit(None))).isNull()")

    def test_and_or_become_bitwise_operators(self):
        self.assertIn("&", _t("each [a] > 1 and [b] > 2"))
        self.assertIn("|", _t("each [a] > 1 or [b] > 2"))

    def test_not_becomes_tilde(self):
        self.assertIn("~", _t("each not [a]"))

    def test_unary_minus(self):
        self.assertEqual(_t("each [Year] * -1"), "((F.col('Year')) * ((-(F.lit(1)))))")

    def test_parentheses_override_precedence(self):
        # Verbose but exact: the multiplication must wrap the addition, not the
        # other way round. Compare with test_arithmetic_precedence_is_preserved.
        self.assertEqual(_t("each ([a] + [b]) * 2"),
                         "(((((F.col('a')) + (F.col('b'))))) * (F.lit(2)))")


class IfTests(unittest.TestCase):
    def test_if_becomes_when_otherwise(self):
        self.assertEqual(
            _t('each if [a] > 1 then "big" else "small"'),
            "F.when(((F.col('a')) > (F.lit(1))), F.lit('big')).otherwise(F.lit('small'))")

    def test_nested_if_in_the_else_branch(self):
        self.assertIn("otherwise(F.when(", _t("each if [a] then 1 else if [b] then 2 else 3"))


class FunctionTests(unittest.TestCase):
    def test_date_year(self):
        self.assertEqual(_t("each Date.Year([d])"), "F.year(F.col('d'))")

    def test_text_from_goes_through_the_helper(self):
        helpers = set()
        self.assertEqual(
            translate_expression("each Text.From([n])", frame="df",
                                 helpers=helpers),
            "_m_text(df, F.col('n'))")
        self.assertEqual(helpers, {"_m_text"})
        # The shared _t helper's default frame reaches the same answer.
        self.assertEqual(_t("each Text.From([a])"), "_m_text(df, F.col('a'))")

    def test_text_from_without_a_frame_refuses(self):
        # A scalar binding has no DataFrame to read a type from. Guessing
        # would put back the cast this whole change removes.
        with self.assertRaises(Untranslatable):
            translate_expression("each Text.From([n])", helpers=set())

    def test_number_from_goes_through_the_helper(self):
        # It used to be a bare `.cast('double')` in _FUNCTIONS, the table for
        # translations that do not depend on the argument's type. On a
        # timestamp that cast answers epoch seconds where M answers an OLE
        # automation serial -- eight orders of magnitude out, and it looks
        # like a number. Measured on Spark 4.2.0.
        helpers = set()
        self.assertEqual(
            translate_expression("each Number.From([n])", frame="df",
                                 helpers=helpers),
            "_m_number(df, F.col('n'))")
        self.assertEqual(helpers, {"_m_number"})

    def test_number_from_without_a_frame_refuses(self):
        with self.assertRaises(Untranslatable):
            translate_expression("each Number.From([n])", helpers=set())

    def test_nested_calls(self):
        # Date.AddMonths now goes through the type-dispatching helper (Task 7),
        # so the emission shape moved; Date.Year still wraps it inline.
        self.assertEqual(_t("each Date.Year(Date.AddMonths([d], 3))"),
                         "F.year(_m_add_months(df, F.col('d'), F.lit(3)))")

    def test_text_start_requires_a_literal_length(self):
        self.assertEqual(_t("each Text.Start([a], 3)"),
                         "F.substring(F.col('a'), 1, 3)")

    def test_text_start_with_a_computed_length_is_refused(self):
        # PySpark's substring wants a Python int; a Column silently fails at runtime.
        with self.assertRaises(Untranslatable):
            _t("each Text.Start([a], [n])")

    def test_day_monday_is_the_integer_one(self):
        self.assertEqual(_t("each Day.Monday"), "F.lit(1)")

    def test_date_totext_needs_a_literal_format(self):
        self.assertIn("date_format", _t('each Date.ToText([d], "yyyy-MM")'))
        with self.assertRaises(Untranslatable):
            _t("each Date.ToText([d], [fmt])")

    def test_start_of_week_is_monday_only(self):
        self.assertEqual(_t("each Date.StartOfWeek([d], Day.Monday)"),
                         "_m_start_of_week(df, F.col('d'), monday=True)")
        with self.assertRaises(Untranslatable):
            _t("each Date.StartOfWeek([d], Day.Thursday)")

    def test_wrong_arity_is_refused(self):
        with self.assertRaises(Untranslatable):
            _t("each Date.AddDays([d])")

    def test_number_round_is_bankers_rounding(self):
        # Measured on Spark 4.2.0: F.round(2.5, 0) is 3.0 and F.bround(2.5, 0)
        # is 2.0. M's Number.Round default is RoundingMode.ToEven.
        self.assertEqual(_t("each Number.Round([x])"),
                         "F.bround(F.col('x'), 0)")
        self.assertEqual(_t("each Number.Round([x], 2)"),
                         "F.bround(F.col('x'), 2)")

    def test_round_up_keeps_the_digit_count(self):
        # F.ceil dropped the 2 entirely. Scaling through decimal rather than
        # multiplying the double: 0.07 * 100 is 7.000000000000001, so the
        # naive form answers 0.08 for Number.RoundUp(0.07, 2).
        self.assertEqual(
            _t("each Number.RoundUp([x], 2)"),
            "(F.ceil((F.col('x')).cast('decimal(38,18)') * F.lit(100))"
            " / F.lit(100)).cast('double')")
        self.assertEqual(
            _t("each Number.RoundDown([x], 1)"),
            "(F.floor((F.col('x')).cast('decimal(38,18)') * F.lit(10))"
            " / F.lit(10)).cast('double')")

    def test_round_up_without_digits_is_plain_ceil(self):
        self.assertEqual(_t("each Number.RoundUp([x])"),
                         "F.ceil(F.col('x'))")

    def test_extra_arguments_are_refused_not_dropped(self):
        # Text.Upper(s, culture) silently lost the culture.
        with self.assertRaises(Untranslatable):
            _t('each Text.Upper([s], "en-US")')
        # Number.Round(x, n, RoundingMode.Up) silently lost the mode.
        with self.assertRaises(Untranslatable):
            _t("each Number.Round([x], 2, 3)")

    def test_too_few_arguments_still_refused(self):
        with self.assertRaises(Untranslatable):
            _t("each Date.AddDays([d])")

    def test_text_trim_removes_all_whitespace(self):
        # Measured: F.trim('  x\t ') is 'x\t' -- the tab survives. M trims it.
        self.assertEqual(_t("each Text.Trim([s])"),
                         r"F.regexp_replace(F.col('s'), r'^\s+|\s+$', '')")

    def test_text_trim_with_characters_uses_btrim(self):
        # Measured: btrim('00x00', '0') is 'x'.
        self.assertEqual(_t('each Text.Trim([s], "0")'),
                         "F.btrim(F.col('s'), F.lit('0'))")

    def test_list_functions_refuse_instead_of_returning_the_argument(self):
        # List.Sum([xs]) used to emit "(F.col('xs'))" -- the column itself.
        for source in ("each List.Sum([xs])", "each List.Max([xs])",
                       "each List.Min([xs])"):
            with self.subTest(source=source):
                with self.assertRaises(Untranslatable):
                    _t(source)

    def test_date_totext_accepts_formats_both_dialects_read_alike(self):
        self.assertEqual(
            _t('each Date.ToText([d], "dd/MM/yyyy")'),
            "F.date_format(F.col('d'), 'dd/MM/yyyy')")
        self.assertEqual(
            _t('each Date.ToText([d], "yyyy-MM-dd HH:mm:ss")'),
            "F.date_format(F.col('d'), 'yyyy-MM-dd HH:mm:ss')")

    def test_date_totext_refuses_dotnet_only_formats(self):
        for pattern in ("d",        # a .NET *standard* format: short date
                        "M",        # month/day pattern, not the month number
                        "MM/dd/yyyy tt",   # tt: Spark raises at run time
                        "fff",      # fractional seconds: Spark raises
                        "ddd",      # .NET day name, Spark zero-padded day
                        "dddd", "K", "zzz", "MMMMM"):
            with self.subTest(pattern=pattern):
                with self.assertRaises(Untranslatable):
                    _t('each Date.ToText([d], "%s")' % pattern)

    def test_date_totext_refuses_a_culture(self):
        # The record form the Power Query editor writes.
        with self.assertRaises(Untranslatable):
            _t('each Date.ToText([d], [Format="MMM/yy", Culture="pt-BR"])')

    def test_start_of_week_defaults_to_sunday(self):
        # 2026-09-25 is a Friday; M's default firstDayOfWeek is Day.Sunday,
        # the same default this module's Date.DayOfWeek already uses. Task 7
        # moved the emission behind the type-dispatching helper; the default
        # under test is still Sunday.
        self.assertEqual(_t("each Date.StartOfWeek([d])"),
                         "_m_start_of_week(df, F.col('d'))")

    def test_start_of_week_monday_still_supported(self):
        self.assertEqual(
            _t("each Date.StartOfWeek([d], Day.Monday)"),
            "_m_start_of_week(df, F.col('d'), monday=True)")

    def test_start_of_week_refuses_other_days(self):
        with self.assertRaises(Untranslatable):
            _t("each Date.StartOfWeek([d], Day.Wednesday)")

    def test_date_functions_go_through_the_helper(self):
        cases = {
            "each Date.AddDays([d], 1)":
                "_m_add_days(df, F.col('d'), F.lit(1))",
            "each Date.AddMonths([d], 2)":
                "_m_add_months(df, F.col('d'), F.lit(2))",
            "each Date.AddYears([d], 2)":
                "_m_add_months(df, F.col('d'), (F.lit(2)) * 12)",
            "each Date.StartOfMonth([d])":
                "_m_start_of_month(df, F.col('d'))",
            "each Date.EndOfMonth([d])":
                "_m_end_of_month(df, F.col('d'))",
            "each Date.StartOfYear([d])":
                "_m_start_of_year(df, F.col('d'))",
            "each Date.EndOfYear([d])":
                "_m_end_of_year(df, F.col('d'))",
            "each Date.StartOfWeek([d])":
                "_m_start_of_week(df, F.col('d'))",
            "each Date.StartOfWeek([d], Day.Monday)":
                "_m_start_of_week(df, F.col('d'), monday=True)",
        }
        for source, want in cases.items():
            with self.subTest(source=source):
                helpers = set()
                self.assertEqual(
                    translate_expression(source, frame="df", helpers=helpers),
                    want)
                self.assertTrue(helpers)

    def test_type_independent_date_functions_stay_inline(self):
        # year/month/quarter return an int whatever the input type, so they
        # need no frame and no helper.
        helpers = set()
        self.assertEqual(
            translate_expression("each Date.Year([d])", frame="df",
                                 helpers=helpers),
            "F.year(F.col('d'))")
        self.assertEqual(helpers, set())

    def test_date_arithmetic_without_a_frame_refuses(self):
        with self.assertRaises(Untranslatable):
            translate_expression("each Date.AddDays([d], 1)", helpers=set())


class EscapeTests(unittest.TestCase):
    def test_m_escapes_are_decoded_in_literals(self):
        self.assertEqual(_t('each "a#(tab)b"'), "F.lit('a\\tb')")
        self.assertEqual(_t('each "#(cr,lf)"'), "F.lit('\\r\\n')")
        self.assertEqual(_t('each "#(#)"'), "F.lit('#')")
        self.assertEqual(_t('each "#(0041)"'), "F.lit('A')")

    def test_unknown_escape_is_refused_not_passed_through(self):
        with self.assertRaises(Untranslatable):
            _t('each "#(bogus)"')

    def test_text_without_escapes_is_untouched(self):
        self.assertEqual(_t('each "plain"'), "F.lit('plain')")

    def test_decode_escapes_boundary_values(self):
        # chr() raises a raw ValueError above 0x10FFFF, which is not
        # Untranslatable and would crash translate_query outright rather than
        # naming and blocking a single step.
        good = {
            "a#(tab)b": "a\tb",
            "#(cr,lf)": "\r\n",
            "#(#)": "#",
            "#(0041)": "A",
            "#(0001F600)": "\U0001F600",
            "#(0010FFFF)": "\U0010FFFF",
            "plain": "plain",
            "a#b": "a#b",
            "#(#)(": "#(",
            "": "",
            "#": "#",
        }
        for source, expected in good.items():
            with self.subTest(source=source):
                self.assertEqual(decode_escapes(source), expected)

    def test_decode_escapes_refuses_malformed_input(self):
        # #(FFFFFFFF) and #(11000000) are out of Unicode's range; a#(tab is an
        # unterminated escape -- #( always starts one in M, so passing it
        # through would emit a string M could never produce.
        for source in ("#(FFFFFFFF)", "#(11000000)", "a#(tab",
                       "#(bogus)", "#()", "#(00041)"):
            with self.subTest(source=source):
                with self.assertRaises(Untranslatable):
                    decode_escapes(source)


class ScopeTests(unittest.TestCase):
    def test_unbound_identifier_is_refused(self):
        with self.assertRaises(Untranslatable):
            _t("each currentDate")

    def test_identifier_in_scope_is_substituted(self):
        self.assertEqual(_t("each Date.Year(currentDate)",
                            {"currentDate": "F.lit('2026-01-01')"}),
                         "F.year(F.lit('2026-01-01'))")

    def test_scope_does_not_leak_into_column_names(self):
        self.assertEqual(_t("each [currentDate]", {"currentDate": "F.lit(1)"}),
                         "F.col('currentDate')")


class FrameFunctionsTests(unittest.TestCase):
    def test_every_entry_emits_the_name_it_registers(self):
        """Nothing else ties a _FRAME_FUNCTIONS entry's *emitted* call to the
        helper name it registers -- m_expr.py wrote "_m_text" twice in the
        Text.From entry, once in the format string and once as the returned
        helper name. If those two copies ever diverge, the generated file
        calls a function its own prelude never defines: `ast.parse` still
        accepts it (a call to an undefined name is syntactically valid), so
        this is a NameError on the cluster, not a translation-time refusal.

        A registered name that is not in m_runtime.HELPERS is the same bug
        from the other side: `m_runtime.prelude_for` would raise an uncaught
        KeyError trying to build the prelude for it. Checking membership here
        catches both.
        """
        args = ["F.col('a')", "F.lit(1)", "F.lit(2)"]
        for name, handler in _FRAME_FUNCTIONS.items():
            with self.subTest(name=name):
                source, helper = handler(args, "df")
                self.assertIn(helper, m_runtime.HELPERS)
                called = source.split("(", 1)[0]
                self.assertEqual(called, helper)
                # Exercise the other half of the mechanism: prelude_for must
                # not raise for a name this table actually registers.
                m_runtime.prelude_for({helper})


class RefusalTests(unittest.TestCase):
    def test_nested_each_is_refused(self):
        with self.assertRaises(Untranslatable):
            _t("each List.Transform({1, 2}, each _ + 1)")

    def test_let_inside_each_is_refused(self):
        with self.assertRaises(Untranslatable):
            _t("each let x = 1 in x")

    def test_unknown_function_is_named_in_the_error(self):
        with self.assertRaises(Untranslatable) as caught:
            _t("each ExtractComponentFn([a])")
        self.assertIn("ExtractComponentFn", str(caught.exception))

    def test_empty_expression_is_refused(self):
        with self.assertRaises(Untranslatable):
            _t("each ")


class OutputCompilesTests(unittest.TestCase):
    CASES = [
        "each [a]", "each [a] & [b]", "each [a] + [b] * 2",
        'each if [a] > 1 then "x" else "y"', "each Date.Year([d])",
        "each Text.Start(Text.From([id]), 3)", "each [a] <> null",
        "each not ([a] > 1 and [b] < 2)", "each Date.DayOfWeek([d], Day.Monday)",
    ]

    def test_every_translation_is_valid_python(self):
        for case in self.CASES:
            with self.subTest(case=case):
                # Through _t, so the type-dispatching family has a frame.
                ast.parse(_t(case), mode="eval")


if __name__ == "__main__":
    unittest.main()
