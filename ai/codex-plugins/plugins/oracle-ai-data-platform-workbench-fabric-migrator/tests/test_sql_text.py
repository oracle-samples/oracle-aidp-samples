import unittest

from fabric_aidp.translate.sql_text import (
    apply_replacements, mask_literals, masked, quoted_identifier_spans,
    string_literal_spans, strip_sql_comments, unterminated_span,
)


class MaskedTests(unittest.TestCase):
    def test_masked_blanks_comments_and_literal_bodies(self):
        out = masked("SELECT 'abc' -- note\nFROM t")
        self.assertNotIn("abc", out)
        self.assertNotIn("note", out)
        self.assertIn("FROM t", out)

    def test_masked_preserves_length_and_offsets(self):
        sql = "SELECT 'abc' -- note\nFROM t"
        out = masked(sql)
        self.assertEqual(len(out), len(sql))
        self.assertEqual(out.index("FROM"), sql.index("FROM"))

    def test_masked_keeps_quote_characters(self):
        # The body is blanked but the delimiters survive, so a rule can still
        # see where a literal begins and ends.
        self.assertEqual(masked("SELECT 'ab'"), "SELECT '  '")

    def test_masked_empty_literal_is_unchanged(self):
        self.assertEqual(masked("SELECT ''"), "SELECT ''")

    def test_masked_keeps_newlines(self):
        self.assertEqual(masked("SELECT 1\n/* x\ny */\nFROM t").count("\n"), 3)


class QuotedIdentifierMaskingTests(unittest.TestCase):
    """T-SQL allows any character inside a `[bracket identifier]`, apostrophe
    included, and this module's own rules then emit the Spark spelling of the
    same thing, `` `it's` ``. Neither pass knew either form, so the `'` in
    `SELECT [it's], c FROM dbo.t` opened a literal that never closed: the
    statement came back unrewritten with NO finding, so it graded PASS with
    `dbo.t` left meaning a different table in Spark."""

    def test_an_apostrophe_in_a_bracket_identifier_opens_no_literal(self):
        sql = "SELECT [it's], c FROM dbo.t"
        self.assertEqual(masked(sql), sql)

    def test_an_apostrophe_in_a_backtick_identifier_opens_no_literal(self):
        sql = "SELECT `it's`, c FROM dbo.t"
        self.assertEqual(masked(sql), sql)

    def test_a_comment_marker_inside_a_bracket_identifier_is_not_a_comment(self):
        sql = "SELECT [a -- b], c FROM t"
        self.assertEqual(strip_sql_comments(sql), sql)
        self.assertEqual(masked(sql), sql)

    def test_a_block_comment_marker_ends_the_identifier_search(self):
        # This used to assert the whole span survived as a name. It no longer
        # does, deliberately: a `/*` inside the body means the `]` found was
        # most likely a comment's, and taking it emitted an identifier built
        # out of comment text with no finding (F1). A name that really
        # contains `/*` is legal and never written; a stray `[` is not.
        sql = "SELECT [a /* b], c FROM t"
        self.assertEqual(unterminated_span(sql), ("identifier", 7))
        self.assertEqual(len(masked(sql)), len(sql))

    def test_a_doubled_bracket_is_an_escaped_bracket(self):
        # T-SQL spells a literal `]` inside a bracket identifier as `]]`.
        sql = "SELECT [a]]b], 'x' FROM t"
        self.assertEqual(masked(sql), "SELECT [a]]b], ' ' FROM t")

    def test_a_literal_after_a_bracket_identifier_is_still_masked(self):
        self.assertEqual(masked("SELECT [it's], 'secret' FROM t"),
                         "SELECT [it's], '      ' FROM t")

    def test_a_bracket_inside_a_literal_is_not_an_identifier(self):
        # The literal wins: `'[--'` is text, and the `--` inside it stays.
        self.assertEqual(masked("SELECT '[--' FROM t"), "SELECT '   ' FROM t")

    def test_an_unterminated_bracket_does_not_swallow_the_statement(self):
        # Declining is the point: consuming the rest of the text would blind
        # every rule to it, which is the failure this pass exists to avoid.
        self.assertEqual(masked("SELECT [a FROM t"), "SELECT [a FROM t")

    def test_an_unterminated_bracket_still_lets_a_later_comment_go(self):
        self.assertEqual(masked("SELECT [a -- x\nFROM t"),
                         "SELECT [a     \nFROM t")

    def test_length_and_newlines_are_preserved(self):
        sql = "SELECT [it's],\n  'lit' -- note\nFROM t"
        out = masked(sql)
        self.assertEqual(len(out), len(sql))
        self.assertEqual(out.count("\n"), sql.count("\n"))
        self.assertEqual(out.index("FROM"), sql.index("FROM"))


class StringLiteralSpanTests(unittest.TestCase):
    def test_spans_point_at_the_delimiters(self):
        sql = "SELECT 'ab', x"
        spans = string_literal_spans(sql)
        self.assertEqual(spans, [(7, 11)])
        self.assertEqual(sql[7:11], "'ab'")

    def test_a_doubled_quote_does_not_end_the_literal(self):
        sql = "SELECT 'it''s'"
        self.assertEqual(string_literal_spans(sql), [(7, 14)])

    def test_a_quoted_identifier_holds_no_literal(self):
        self.assertEqual(string_literal_spans("SELECT [it's] FROM t"), [])
        self.assertEqual(string_literal_spans("SELECT `it's` FROM t"), [])

    def test_an_unterminated_literal_runs_to_the_end(self):
        sql = "SELECT 'abc"
        self.assertEqual(string_literal_spans(sql), [(7, len(sql))])


class DoubleQuotedIdentifierMaskingTests(unittest.TestCase):
    """The T-SQL reading of `"..."`, which callers now ask for explicitly:
    it is an identifier, because T-SQL's default is QUOTED_IDENTIFIER ON and
    that is what every Fabric export is generated under. It used to be masked
    as a string literal in every dialect, which blanked the name and let Spark
    read the column's name as its value."""

    def _masked(self, sql):
        return masked(sql, quoted_identifier=True)

    def test_a_double_quoted_name_is_an_identifier_not_a_literal(self):
        self.assertEqual(
            string_literal_spans('SELECT "a" FROM t', quoted_identifier=True),
            [])
        self.assertEqual(
            quoted_identifier_spans('SELECT "a" FROM t', quoted_identifier=True),
            [(7, 10)])

    def test_its_body_is_not_blanked(self):
        sql = 'SELECT "my col" FROM t'
        self.assertEqual(self._masked(sql), sql)

    def test_a_doubled_quote_is_an_escape_inside_it(self):
        self.assertEqual(
            quoted_identifier_spans('SELECT "a""b" FROM t',
                                    quoted_identifier=True),
            [(7, 13)])

    def test_an_apostrophe_inside_it_opens_no_literal(self):
        sql = 'SELECT "it\'s", c FROM dbo.t'
        self.assertEqual(self._masked(sql), sql)

    def test_a_double_quote_inside_a_literal_is_still_text(self):
        self.assertEqual(self._masked("SELECT '\"a\"' FROM t"),
                         "SELECT '   ' FROM t")

    def test_a_comment_marker_inside_it_is_not_a_comment(self):
        sql = 'SELECT "a -- b", c FROM t'
        self.assertEqual(self._masked(sql), sql)

    def test_an_unterminated_double_quote_is_an_open_identifier(self):
        self.assertEqual(
            unterminated_span('SELECT "a FROM t', quoted_identifier=True),
            ("identifier", 7))


class MaskingInvariantTests(unittest.TestCase):
    """The contract every rule depends on: an offset found in a masked copy is
    valid in the original. Locked over adversarial inputs because
    `strip_sql_comments` is now span-based rather than a character loop, and a
    length or newline drift would silently misplace every rewrite."""

    ADVERSARIAL = (
        "SELECT [a FROM t", "SELECT '[a]' FROM t", "SELECT [a 'b'] FROM t",
        "SELECT [a]]", "SELECT 1 -- [x\nFROM t", "SELECT 1 /* [x */ FROM t",
        "SELECT [a\nb] FROM t", "SELECT 1\r\nFROM t", "SELECT a] FROM t",
        "SELECT [[[]]] FROM t", "", "SELECT 'a''' ", "SELECT 1 /* x",
        "SELECT `a``b`", "-- only a comment", "/*", "'", "[", "`", "]]",
        "SELECT 'a\nb' FROM t",
        # Added with G1: `"` is an identifier quote now, so the same
        # adversarial shapes have to hold for it too.
        'SELECT "a FROM t', 'SELECT "a""b" FROM t', 'SELECT "a\nb" FROM t',
        '"', '""', 'SELECT \'"a\' FROM t', 'SELECT "a -- b" FROM t',
    )

    def test_length_and_line_structure_survive_every_pass(self):
        for sql in self.ADVERSARIAL:
            for masking_pass in (strip_sql_comments, mask_literals, masked):
                with self.subTest(sql=sql, pass_=masking_pass.__name__):
                    out = masking_pass(sql)
                    self.assertEqual(len(out), len(sql))
                    self.assertEqual(out.count("\n"), sql.count("\n"))
                    self.assertEqual(out.count("\r"), sql.count("\r"))

    def test_the_predicate_never_raises_on_them(self):
        for sql in self.ADVERSARIAL:
            with self.subTest(sql=sql):
                found = unterminated_span(sql)
                self.assertTrue(found is None or sql[found[1]] in "'\"[`/")

    def test_nested_brackets_follow_the_escape_rule(self):
        # `[[[]]]` is `[` + `[[` + `]]` + `]`: the identifier `[[]`, closed.
        self.assertIsNone(unterminated_span("SELECT [[[]]] FROM t"))
        self.assertEqual(masked("SELECT [[[]]] FROM t"), "SELECT [[[]]] FROM t")


class UnterminatedSpanTests(unittest.TestCase):
    """Masking that reaches end of input still inside a quote, a bracket or a
    block comment silently swallows the rest of the object: every rule reads
    it as literal or comment text, rewrites nothing and raises nothing, so it
    grades PASS. Measured before this predicate existed:

        SELECT 'abc FROM dbo.t   -> SELECT 'abc FROM dbo.t   findings=[]
        SELECT [a]]              -> SELECT [a]]              findings=[]
        SELECT 1 /* x FROM dbo.t -> SELECT 1 /* x FROM dbo.t  findings=[]

    This is a read-only predicate; the masking passes keep their signatures
    and their length-preserving contract.
    """

    def test_well_formed_sql_has_nothing_open(self):
        for sql in ("SELECT 'abc' FROM t",
                    "SELECT 'it''s ok' FROM t",
                    "SELECT [it's], c FROM t",
                    "SELECT [a]]b] FROM t",
                    "SELECT `a``b` FROM t",
                    "SELECT 1 /* note */ FROM t",
                    "SELECT 1 -- note\nFROM t"):
            with self.subTest(sql=sql):
                self.assertIsNone(unterminated_span(sql))

    def test_an_unclosed_literal_is_reported_at_its_opening_quote(self):
        self.assertEqual(unterminated_span("SELECT 'abc FROM dbo.t"),
                         ("literal", 7))

    def test_an_unclosed_bracket_is_reported(self):
        # `[a]]` is `[` + the escape `]]`, so the identifier never closes.
        self.assertEqual(unterminated_span("SELECT [a]]"), ("identifier", 7))

    def test_an_unclosed_backtick_is_reported(self):
        self.assertEqual(unterminated_span("SELECT `a FROM dbo.t"),
                         ("identifier", 7))

    def test_an_unclosed_block_comment_is_reported(self):
        self.assertEqual(unterminated_span("SELECT 1 /* x FROM dbo.t"),
                         ("comment", 9))

    def test_a_line_comment_needs_no_terminator(self):
        # An apostrophe in prose is the false positive that matters most:
        # real SQL is full of `-- don't do this`.
        self.assertIsNone(unterminated_span("SELECT 1 -- don't FROM dbo.t"))
        self.assertIsNone(unterminated_span("SELECT 1 -- a [b\nFROM t"))

    def test_an_opener_inside_a_literal_is_text(self):
        self.assertIsNone(unterminated_span("SELECT 'a [b' FROM t"))
        self.assertIsNone(unterminated_span("SELECT 'a /* b' FROM t"))

    def test_an_opener_inside_a_bracket_identifier_is_text(self):
        self.assertIsNone(unterminated_span("SELECT [a 'b] FROM t"))

    def test_a_block_comment_marker_ends_the_identifier_instead(self):
        # See _NOT_IN_AN_IDENTIFIER: the `]` after a `/*` is treated as the
        # comment's, not the name's, so the `[` is an open identifier.
        self.assertEqual(unterminated_span("SELECT [a /* b] FROM t"),
                         ("identifier", 7))

    def test_a_marker_inside_a_comment_is_text(self):
        self.assertIsNone(unterminated_span("SELECT 1 /* it's [a */ FROM t"))

    def test_the_earliest_open_construct_wins(self):
        kind, offset = unterminated_span("SELECT 'a, [b FROM t")
        self.assertEqual((kind, offset), ("literal", 7))

    def test_degenerate_inputs(self):
        for sql in ("", "SELECT 1", None, 7):
            with self.subTest(sql=sql):
                self.assertIsNone(unterminated_span(sql))

    def test_offsets_are_valid_in_the_original(self):
        sql = "SELECT a,\n       'abc FROM dbo.t"
        kind, offset = unterminated_span(sql)
        self.assertEqual(kind, "literal")
        self.assertEqual(sql[offset], "'")


class DialectArgumentTests(unittest.TestCase):
    """`"..."` means different things in the two dialects this tool touches,
    so the masking passes take the dialect rather than picking one globally.

    The default is `quoted_identifier=False`, the **Spark** reading, chosen
    deliberately: `sql_text` is a shared utility and Spark is this tool's
    target dialect, so a caller that forgets the argument gets the
    conservative behaviour. Masking a literal's contents can only *suppress*
    a rewrite; exposing an identifier's body can *corrupt* one -- which is
    exactly what happened when the notebook SQL-cell path inherited T-SQL's
    reading and rewrote a table name inside a user's string.
    """

    SQL = 'SELECT "my col" FROM t'

    def test_the_default_reads_a_double_quote_as_a_string_literal(self):
        self.assertEqual(masked(self.SQL), 'SELECT "      " FROM t')
        self.assertEqual(string_literal_spans(self.SQL), [(7, 15)])
        self.assertEqual(quoted_identifier_spans(self.SQL), [])

    def test_the_tsql_reading_is_opt_in(self):
        self.assertEqual(masked(self.SQL, quoted_identifier=True), self.SQL)
        self.assertEqual(
            string_literal_spans(self.SQL, quoted_identifier=True), [])
        self.assertEqual(
            quoted_identifier_spans(self.SQL, quoted_identifier=True),
            [(7, 15)])

    def test_single_quotes_are_literals_in_both_readings(self):
        for flag in (False, True):
            with self.subTest(quoted_identifier=flag):
                self.assertEqual(masked("SELECT 'ab' FROM t",
                                        quoted_identifier=flag),
                                 "SELECT '  ' FROM t")

    def test_brackets_and_backticks_are_identifiers_in_both_readings(self):
        for flag in (False, True):
            with self.subTest(quoted_identifier=flag):
                sql = "SELECT [it's], `b'c` FROM t"
                self.assertEqual(masked(sql, quoted_identifier=flag), sql)

    def test_comments_follow_the_dialect_too(self):
        # Under the Spark reading a `--` inside `"..."` is inside a literal;
        # under T-SQL's it is inside a name. Either way it is not a comment.
        sql = 'SELECT "a -- b" FROM t'
        self.assertEqual(strip_sql_comments(sql), sql)
        self.assertEqual(strip_sql_comments(sql, quoted_identifier=True), sql)

    def test_an_unterminated_double_quote_is_named_per_dialect(self):
        self.assertEqual(unterminated_span('SELECT "a'), ("literal", 7))
        self.assertEqual(unterminated_span('SELECT "a', quoted_identifier=True),
                         ("identifier", 7))

    def test_the_invariant_holds_under_both_readings(self):
        for sql in MaskingInvariantTests.ADVERSARIAL:
            for flag in (False, True):
                for masking_pass in (strip_sql_comments, mask_literals, masked):
                    with self.subTest(sql=sql, quoted_identifier=flag,
                                      pass_=masking_pass.__name__):
                        out = masking_pass(sql, quoted_identifier=flag)
                        self.assertEqual(len(out), len(sql))
                        self.assertEqual(out.count("\n"), sql.count("\n"))
                        self.assertEqual(out.count("\r"), sql.count("\r"))


class ApplyReplacementsTests(unittest.TestCase):
    def test_single_replacement(self):
        self.assertEqual(apply_replacements("abcdef", [(2, 4, "XY")]), "abXYef")

    def test_multiple_replacements_do_not_shift_each_other(self):
        out = apply_replacements("aaa bbb ccc", [(0, 3, "1"), (4, 7, "22"), (8, 11, "333")])
        self.assertEqual(out, "1 22 333")

    def test_replacements_may_be_supplied_out_of_order(self):
        out = apply_replacements("aaa bbb", [(4, 7, "Z"), (0, 3, "Y")])
        self.assertEqual(out, "Y Z")

    def test_empty_list_is_a_no_op(self):
        self.assertEqual(apply_replacements("abc", []), "abc")

    def test_insertion_with_equal_offsets(self):
        self.assertEqual(apply_replacements("ac", [(1, 1, "b")]), "abc")

    def test_overlapping_replacements_raise(self):
        with self.assertRaises(ValueError):
            apply_replacements("abcdef", [(0, 3, "x"), (2, 5, "y")])


class MovedHelperTests(unittest.TestCase):
    def test_strip_sql_comments_still_preserves_literals(self):
        self.assertIn("-- inside", strip_sql_comments("SELECT '-- inside'"))

    def test_mask_literals_still_blanks_bodies(self):
        self.assertNotIn("inside", mask_literals("SELECT 'inside'"))

    def test_warehouse_module_still_exposes_both(self):
        from fabric_aidp.inventory import warehouse
        self.assertIn("-- inside", warehouse.strip_sql_comments("SELECT '-- inside'"))
        self.assertNotIn("inside", warehouse.mask_literals("SELECT 'inside'"))

    def test_warehouse_classification_is_unaffected(self):
        from fabric_aidp.inventory.warehouse import classify_sql_object
        self.assertEqual(classify_sql_object("CREATE TABLE dbo.t (id INT)"),
                         ("table", "dbo", "t"))
        self.assertEqual(classify_sql_object("EXEC('CREATE TABLE x')"), ("other", "", ""))



from fabric_aidp.translate.sql_text import split_call_arguments


class SplitCallArgumentsTests(unittest.TestCase):
    def _args(self, sql):
        view = masked(sql)
        result = split_call_arguments(view, view.index("("))
        if result is None:
            return None
        spans, _close = result
        return [sql[a:b].strip() for a, b in spans]

    def test_three_arguments(self):
        self.assertEqual(self._args("F(a, b, c)"), ["a", "b", "c"])

    def test_nested_call_is_one_argument(self):
        self.assertEqual(self._args("F(G(a, b), c)"), ["G(a, b)", "c"])

    def test_comma_inside_a_literal_does_not_split(self):
        self.assertEqual(self._args("F('a,b', c)"), ["'a,b'", "c"])

    def test_single_argument(self):
        self.assertEqual(self._args("F(a)"), ["a"])

    def test_empty_call(self):
        self.assertEqual(self._args("F()"), [""])

    def test_unterminated_call_returns_none(self):
        self.assertIsNone(self._args("F(a, b"))

    def test_close_index_points_at_the_closing_paren(self):
        sql = "F(a, b)"
        view = masked(sql)
        _spans, close = split_call_arguments(view, view.index("("))
        self.assertEqual(sql[close], ")")


if __name__ == "__main__":
    unittest.main()
