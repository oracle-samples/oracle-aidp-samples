r"""T-SQL defects that crash the translator or emit SQL that cannot run.

Every expectation here was measured on the tree before the fix, and the
measured "before" is quoted next to the case it belongs to.

  D1  the maskers did not know `[bracket identifiers]`, so one apostrophe in a
      column name opened a string literal that never closed and silently
      disabled every downstream rule -- with no finding raised, so it graded
      PASS with an unqualified table name
  D2  `N'unicode'` literals reached the output verbatim (a Spark syntax error)
      or were mangled into `Nconcat('a', N)'b'`, which is not SQL at all
  D3  CONVERT emitted T-SQL type names Spark rejects: CAST(c AS nvarchar(50))
  E1  SQ10 did not know the `]]` escape the maskers were taught in D1, so
      `[a]]b]` came out as `` `a`]b] ``, which fails on Spark
  E2  SQ50 read the word TOP inside a quoted identifier as the keyword, so a
      correctly translated statement was pushed to REVIEW for a column name
  F1  masking that ran off the end of the input still inside a quote, a
      bracket or a block comment swallowed the rest of the object: nothing
      rewritten, nothing flagged, graded PASS with the table unqualified
  G1  `"my col"` is an identifier in T-SQL and a string literal in Spark, so
      the object returned the column's name instead of its value -- PASS,
      zero findings, wrong data
  F1  an unterminated `[` reached forward into a comment for its closer and
      emitted an identifier built out of comment text, unflagged
  F2  a zero-width refusal was applied as a no-op edit every pass, so the
      8-pass budget burned and a depth-one expression reported deep nesting
  E-a a column name containing a control-flow word tripped the procedural
      gate, so one identifier disabled every rule in the file
  E-b the constraint rule cut IDENTITY/UNIQUE out of column *names*, emitting
      the empty backquoted identifier E1 had just added a refusal for
  E-c the bare-CAST path passed any type outside SQ60's table through
      unflagged, so D3 had fixed only half the surface
  INTO the bare `INTO` of `SELECT ... INTO t` was not an object keyword, so
      the table being created was left unqualified while the source was not
  TRY every TRY_CONVERT passed through verbatim, and it is unrunnable on
      Spark whatever its type argument
  COL a column declared with a T-SQL type outside SQ60's tables passed
      through verbatim, and the DDL cannot run
  TS  a `timestamp` COLUMN is T-SQL's rowversion, not a datetime, and became
      a Spark TIMESTAMP: a silently different schema
  T1  `\bCREATE\s+TABLE\b[^(]*\(` ran past `AS SELECT` and took a CTAS's
      first `(` for a column list, which disarmed the constraint rule for
      the whole rest of the document
  T5  only the FIRST CREATE TABLE body in a document was cleaned, and inline
      CHECK / REFERENCES / COLLATE were never cleaned at all
  T2  nine T-SQL control-flow and hint constructs came back unchanged with
      findings=[] while the README claimed "any T-SQL control flow" is
      flagged; every one of the nine is rejected by Spark 4.2.0
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql):
    return tsql.translate(sql, kind="warehouse_ddl", item="W")


class BracketIdentifierMaskingTests(unittest.TestCase):
    """D1. Measured before the fix:

        SELECT [it's], c FROM dbo.t -> SELECT [it's], c FROM dbo.t  findings: []
    """

    def test_apostrophe_in_a_bracket_identifier_still_rewrites_the_statement(self):
        result = _t("SELECT [it's], c FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT `it's`, c FROM default.W.t")
        rules = {f.rule for f in result.findings}
        self.assertIn("SQ10_BRACKET_IDENT", rules)
        self.assertIn("SQ11_TWO_PART_NAME", rules)

    def test_the_table_reference_is_never_silently_left_unqualified(self):
        # The defect's real damage: no finding at all, so `verify` graded the
        # object PASS while `dbo.t` names a different table in Spark.
        result = _t("SELECT [it's], c FROM dbo.t")
        self.assertNotIn("dbo.t", result.translated_sql)
        self.assertNotEqual(result.findings, [])

    def test_mixed_identifiers_both_convert(self):
        # Measured before: SELECT `TOP secret`, [it's] FROM dbo.t -- the first
        # identifier converted, the second did not, and the table was lost.
        out = _t("SELECT [TOP secret], [it's] FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `TOP secret`, `it's` FROM default.W.t")

    def test_a_comment_marker_inside_a_bracket_identifier_is_not_a_comment(self):
        out = _t("SELECT [a--b] FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `a--b` FROM default.W.t")

    def test_a_quote_inside_a_bracket_identifier_does_not_hide_a_later_literal(self):
        out = _t("SELECT [it's] + 'x' FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT concat(`it's`, 'x') FROM default.W.t")


class UnicodeLiteralTests(unittest.TestCase):
    """D2. Measured before the fix:

        SELECT N'x' FROM dbo.t        -> SELECT N'x' FROM default.W.t
        SELECT N'a' + N'b' FROM dbo.t -> SELECT Nconcat('a', N)'b' FROM default.W.t

    Both fail on Spark 4.2.0: it has no N-prefixed literal, and the second
    output is not SQL at all.
    """

    def test_single_unicode_literal_loses_its_prefix(self):
        result = _t("SELECT N'x' FROM dbo.t")
        self.assertEqual(result.translated_sql, "SELECT 'x' FROM default.W.t")
        self.assertIn("SQ13_UNICODE_LITERAL", {f.rule for f in result.findings})

    def test_concatenated_unicode_literals_are_not_mangled(self):
        out = _t("SELECT N'a' + N'b' FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT concat('a', 'b') FROM default.W.t")

    def test_an_n_that_is_not_a_literal_prefix_is_left_alone(self):
        out = _t("SELECT N, n FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT N, n FROM default.W.t")

    def test_the_letter_n_inside_a_literal_is_left_alone(self):
        # The N stays. The doubled quotes are respelled for Spark (SQ16),
        # which would otherwise read three adjacent literals and return Nx.
        out = _t("SELECT 'N''x''' FROM dbo.t").translated_sql
        self.assertEqual(out, r"SELECT 'N\'x\'' FROM default.W.t")

    def test_an_n_ending_an_identifier_is_not_a_prefix(self):
        out = _t("SELECT colN'x' FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT colN'x' FROM default.W.t")


class ConvertTypeNameTests(unittest.TestCase):
    """D3. Measured before the fix:

        SELECT CONVERT(nvarchar(50), c) FROM dbo.t
          -> SELECT CAST(c AS nvarchar(50)) FROM default.W.t   (fails on Spark)

    while `CAST(c AS varchar(20))` -> `CAST(c AS STRING)` already ran, and
    `CONVERT(int, c)` -> `CAST(c AS int)` was already correct.
    """

    def test_nvarchar_becomes_a_spark_type(self):
        # Expectation updated by the tsql-silent-values batch: the type name
        # is still mapped -- which is what this test is about -- but the
        # length is no longer dropped. SQ61 wraps the cast so the value is
        # truncated the way T-SQL truncates it; see test_tsql_silent_values.
        result = _t("SELECT CONVERT(nvarchar(50), c) FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "SELECT substring(CAST(c AS STRING), 1, 50) FROM default.W.t")
        self.assertNotIn("nvarchar", result.translated_sql)

    def test_it_agrees_with_the_bare_cast_path(self):
        # SQ60_TYPE maps varchar(20) -> STRING for a bare CAST; the CONVERT
        # path must not invent a second answer for the same type.
        convert = _t("SELECT CONVERT(varchar(20), c) FROM dbo.t").translated_sql
        cast = _t("SELECT CAST(c AS varchar(20)) FROM dbo.t").translated_sql
        self.assertEqual(convert, cast)

    def test_types_spark_already_accepts_pass_through(self):
        out = _t("SELECT CONVERT(int, c) FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT CAST(c AS int) FROM default.W.t")

    def test_the_mapped_type_names(self):
        # The length-carrying string types moved to SQ61's truncating form
        # in the tsql-silent-values batch and are asserted there.
        for tsql_type, spark_type in (("ntext", "STRING"),
                                      ("text", "STRING"),
                                      ("datetime", "TIMESTAMP"),
                                      ("datetime2(7)", "TIMESTAMP"),
                                      ("smalldatetime", "TIMESTAMP"),
                                      ("bit", "BOOLEAN"),
                                      ("uniqueidentifier", "STRING"),
                                      ("money", "DECIMAL(19,4)"),
                                      ("smallmoney", "DECIMAL(19,4)"),
                                      ("decimal(10,2)", "decimal(10,2)"),
                                      ("numeric(10,2)", "DECIMAL(10,2)")):
            with self.subTest(tsql_type=tsql_type):
                out = _t(f"SELECT CONVERT({tsql_type}, c) FROM dbo.t").translated_sql
                self.assertEqual(out, f"SELECT CAST(c AS {spark_type}) FROM default.W.t")

    def test_a_lossy_mapping_is_flagged_as_well_as_applied(self):
        # MONEY's scale maps but its rounding does not, which is why SQ60
        # refuses to touch it. Emitting `CAST(x AS MONEY)` is not an option
        # here -- Spark rejects it -- so the rewrite is applied and the loss
        # is flagged, the same call SQ71/SQ73 make when they remove a clause.
        result = _t("SELECT CONVERT(money, c) FROM dbo.t")
        self.assertIn("DECIMAL(19,4)", result.translated_sql)
        self.assertTrue(result.needs_manual_review)

    def test_an_unmapped_type_is_refused_rather_than_passed_through(self):
        sql = "SELECT CONVERT(xml, c) FROM dbo.t"
        result = _t(sql)
        self.assertIn("CONVERT(xml, c)", result.translated_sql)
        self.assertIn("SQ81_CONVERT_TYPE", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)

    def test_a_type_name_that_is_not_a_type_at_all_is_refused(self):
        result = _t("SELECT CONVERT(some_udt, c) FROM dbo.t")
        self.assertIn("CONVERT(some_udt, c)", result.translated_sql)
        self.assertIn("SQ81_CONVERT_TYPE", {f.rule for f in result.findings})

    def test_no_t_sql_type_name_survives_into_the_output(self):
        for tsql_type in ("nvarchar(50)", "ntext", "bit", "datetime",
                          "uniqueidentifier", "money"):
            with self.subTest(tsql_type=tsql_type):
                out = _t(f"SELECT CONVERT({tsql_type}, c)").translated_sql
                self.assertNotIn(tsql_type.split("(")[0], out.casefold())


class EscapedBracketTests(unittest.TestCase):
    """E1. Measured before the fix:

        SELECT [a]]b] FROM dbo.t -> SELECT `a`]b] FROM default.W.t

    and that output fails on Spark. In T-SQL `[a]]b]` is the single identifier
    `a]b` -- `]]` is how a `]` is spelled inside brackets. D1 taught both
    masking passes that rule; SQ10's own pattern still cut at the first `]`.
    """

    def test_an_escaped_bracket_stays_inside_one_identifier(self):
        result = _t("SELECT [a]]b] FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT `a]b` FROM default.W.t")
        self.assertIn("SQ10_BRACKET_IDENT", {f.rule for f in result.findings})

    def test_no_stray_bracket_survives(self):
        out = _t("SELECT [a]]b] FROM dbo.t").translated_sql
        self.assertNotIn("]", out.replace("`a]b`", ""))

    def test_a_trailing_escaped_bracket(self):
        out = _t("SELECT [a]]] FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `a]` FROM default.W.t")

    def test_two_escapes_in_one_name(self):
        out = _t("SELECT [a]]b]]c] FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `a]b]c` FROM default.W.t")

    def test_a_backtick_and_an_escaped_bracket_together(self):
        # `]` needs no escaping in a Spark backquoted identifier; a backtick
        # does, by doubling, which SQ10 already did.
        out = _t("SELECT [we`ird]]x] FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `we``ird]x` FROM default.W.t")

    def test_two_adjacent_identifiers_are_not_joined(self):
        out = _t("SELECT [a], [b] FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `a`, `b` FROM default.W.t")

    def test_an_escaped_bracket_in_a_type_position_still_unwraps(self):
        # The type path reads the same unescaped name.
        out = _t("CREATE TABLE dbo.t ([a]]b] [int] NOT NULL)").translated_sql
        self.assertIn("`a]b` INT NOT NULL", out.replace("int", "INT"))

    def test_an_empty_bracket_identifier_is_refused(self):
        # T-SQL rejects `[]` itself, so this input is already malformed. The
        # point is that it must not be turned into an empty backquoted
        # identifier, which Spark cannot resolve, with no finding raised.
        result = _t("SELECT [] FROM dbo.t")
        self.assertIn("[]", result.translated_sql)
        self.assertIn("SQ10_BRACKET_EMPTY", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)


class TopInsideAnIdentifierTests(unittest.TestCase):
    """E2. Measured before the fix:

        SELECT [TOP secret], [it's] FROM dbo.t
          -> SELECT `TOP secret`, `it's` FROM default.W.t
             [SQ10_BRACKET_IDENT x2, SQ11_TWO_PART_NAME, SQ50_TOP_NON_LITERAL]

    The translation is right; the flag is not. `masked()` blanks literal
    bodies but deliberately keeps identifier bodies visible, so `_TOP_RE` read
    the column name as the keyword and a clean statement needed a human for
    nothing.
    """

    def test_a_column_named_top_raises_no_top_finding(self):
        result = _t("SELECT [TOP secret], [it's] FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT `TOP secret`, `it's` FROM default.W.t")
        self.assertNotIn("SQ50_TOP_NON_LITERAL",
                         {f.rule for f in result.findings})

    def test_such_a_statement_needs_no_review(self):
        result = _t("SELECT [TOP secret] FROM dbo.t")
        self.assertEqual(result.flags, 0, [str(f) for f in result.findings])
        self.assertFalse(result.needs_manual_review)

    def test_a_backticked_identifier_is_covered_too(self):
        # SQ10 has already produced the backtick form by the time SQ50 runs,
        # and an input may arrive written that way.
        result = _t("SELECT `TOP secret` FROM dbo.t")
        self.assertNotIn("SQ50_TOP_NON_LITERAL",
                         {f.rule for f in result.findings})

    def test_a_real_top_is_still_rewritten(self):
        result = _t("SELECT TOP 10 [TOP secret] FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "SELECT `TOP secret` FROM default.W.t LIMIT 10")
        self.assertIn("SQ50_TOP", {f.rule for f in result.findings})

    def test_a_real_non_literal_top_is_still_flagged(self):
        result = _t("SELECT TOP @n [TOP secret] FROM dbo.t")
        self.assertIn("SQ50_TOP_NON_LITERAL", {f.rule for f in result.findings})


class UnterminatedConstructTests(unittest.TestCase):
    """F1. Measured before the fix:

        SELECT 'abc FROM dbo.t   -> SELECT 'abc FROM dbo.t
                                    findings=[]  needs_manual_review=False
        SELECT [a]]              -> SELECT [a]]
                                    findings=[]  needs_manual_review=False
        SELECT 1 /* x FROM dbo.t -> SELECT 1 /* x FROM dbo.t
                                    findings=[]  needs_manual_review=False

    D1's shape reached through malformed input: a masking state that silently
    disables every rule. In the first case `dbo.t` sits inside what the masker
    reasonably reads as a string, so SQ11 never sees it and the user ships an
    unqualified table name believing the file was checked. The SQL is left
    exactly as written -- guessing where the quote belongs would be invention.
    """

    def test_an_unterminated_literal_is_flagged(self):
        result = _t("SELECT 'abc FROM dbo.t")
        self.assertIn("SQ01_UNTERMINATED_QUOTE", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)

    def test_an_unterminated_literal_is_not_repaired(self):
        sql = "SELECT 'abc FROM dbo.t"
        self.assertEqual(_t(sql).translated_sql, sql)

    def test_the_detail_names_the_construct_and_the_offset(self):
        detail = next(f.detail for f in _t("SELECT 'abc FROM dbo.t").findings
                      if f.rule == "SQ01_UNTERMINATED_QUOTE")
        self.assertIn("offset 7", detail)
        self.assertIn("line 1", detail)

    def test_an_unterminated_bracket_is_flagged(self):
        result = _t("SELECT [a]]")
        self.assertIn("SQ01_UNTERMINATED_IDENTIFIER",
                      {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)
        self.assertEqual(result.translated_sql, "SELECT [a]]")

    def test_an_unterminated_backtick_is_flagged(self):
        result = _t("SELECT `a FROM dbo.t")
        self.assertIn("SQ01_UNTERMINATED_IDENTIFIER",
                      {f.rule for f in result.findings})

    def test_an_unterminated_block_comment_is_flagged(self):
        # The third door onto the same silent PASS: everything after `/*` is
        # blanked, so SQ11 never saw dbo.t either.
        result = _t("SELECT 1 /* x FROM dbo.t")
        self.assertIn("SQ01_UNTERMINATED_COMMENT",
                      {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)

    def test_well_formed_sql_raises_no_such_flag(self):
        for sql in ("SELECT 'abc' FROM dbo.t",
                    "SELECT 'it''s ok' FROM dbo.t",
                    "SELECT [it's], c FROM dbo.t",
                    "SELECT [a]]b] FROM dbo.t",
                    "SELECT 1 -- don't FROM dbo.t",
                    "SELECT 1 /* note */ FROM dbo.t"):
            with self.subTest(sql=sql):
                rules = {f.rule for f in _t(sql).findings}
                self.assertEqual([r for r in rules if r.startswith("SQ01_")], [])

    def test_sound_statements_before_the_break_are_still_translated(self):
        # The flag is about the file; it does not throw away the rewrites that
        # are sound. A truncated export is exactly this shape.
        result = _t("SELECT a FROM dbo.t; SELECT 'abc FROM dbo.u")
        self.assertIn("default.W.t", result.translated_sql)
        self.assertIn("SQ01_UNTERMINATED_QUOTE", {f.rule for f in result.findings})


class DoubleQuotedIdentifierTests(unittest.TestCase):
    """G1. T-SQL's default is QUOTED_IDENTIFIER ON -- what DacFx and Fabric
    emit under -- so `"my col"` is an identifier. Spark reads it as a string
    literal. Measured on Spark 4.2.0 against `select 42 as `my col``:

        SELECT "my col" FROM t -> 'my col'   <- Spark: the literal text
        SELECT `my col` FROM t -> 42         <- what T-SQL returns

    so an object selecting a double-quoted column silently returned the
    column's NAME instead of its VALUE. Measured here before the fix:

        SELECT "my col" FROM t  -> SELECT "my col" FROM t     findings=[]
        SELECT c FROM "dbo"."t" -> SELECT c FROM "dbo"."t"    findings=[]

    The dialect decision is recorded in sql_text's docstring: `"..."` is a
    quoted identifier, and a file that says otherwise is flagged rather than
    modelled.
    """

    def test_a_double_quoted_column_becomes_a_backticked_identifier(self):
        result = _t('SELECT "my col" FROM t')
        self.assertEqual(result.translated_sql, "SELECT `my col` FROM t")
        self.assertIn("SQ10_DQUOTE_IDENT", {f.rule for f in result.findings})

    def test_the_value_not_the_name(self):
        # The whole point: no double quote survives into the output, because
        # what survives returns the column name as a string.
        self.assertNotIn('"', _t('SELECT "my col" FROM t').translated_sql)

    def test_a_double_quoted_two_part_name_is_qualified(self):
        result = _t('SELECT c FROM "dbo"."t"')
        self.assertEqual(result.translated_sql, "SELECT c FROM default.W.t")
        rules = {f.rule for f in result.findings}
        self.assertIn("SQ10_DQUOTE_IDENT", rules)
        self.assertIn("SQ11_TWO_PART_NAME", rules)

    def test_a_doubled_quote_is_an_escaped_quote(self):
        # T-SQL spells an embedded `"` by doubling it, the same rule as `]]`.
        out = _t('SELECT "a""b" FROM dbo.t').translated_sql
        self.assertEqual(out, 'SELECT `a"b` FROM default.W.t')

    def test_a_keyword_inside_a_double_quoted_name_is_not_the_keyword(self):
        result = _t('SELECT "TOP secret" FROM dbo.t')
        self.assertEqual(result.translated_sql,
                         "SELECT `TOP secret` FROM default.W.t")
        self.assertEqual(result.flags, 0, [str(f) for f in result.findings])

    def test_an_unterminated_double_quote_is_an_identifier_now(self):
        rules = {f.rule for f in _t('SELECT "a').findings}
        self.assertIn("SQ01_UNTERMINATED_IDENTIFIER", rules)
        self.assertNotIn("SQ01_UNTERMINATED_QUOTE", rules)

    def test_an_empty_double_quoted_name_is_refused(self):
        result = _t('SELECT "" FROM dbo.t')
        self.assertIn('""', result.translated_sql)
        self.assertIn("SQ10_DQUOTE_EMPTY", {f.rule for f in result.findings})

    def test_a_type_name_in_double_quotes_unwraps_like_a_bracketed_one(self):
        out = _t('CREATE TABLE dbo.t (c "int" NOT NULL)').translated_sql
        self.assertEqual(out, "CREATE TABLE default.W.t (c int NOT NULL)")

    def test_single_quoted_literals_are_untouched(self):
        out = _t("SELECT 'my col' FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT 'my col' FROM default.W.t")


class QuotedIdentifierOffTests(unittest.TestCase):
    """G1's exception. `SET QUOTED_IDENTIFIER OFF` flips the meaning of `"` for
    the whole file, and then `"x"` really is a literal. That is rare and it is
    a mode switch, so it is flagged and those spans are left exactly as
    written -- modelling the mode mid-file, or tracking it across batches,
    would mean guessing, and a wrong guess here changes values silently.
    """

    SQL = 'SET QUOTED_IDENTIFIER OFF;\nSELECT "abc" AS x FROM dbo.t'

    def test_the_mode_switch_is_flagged(self):
        result = _t(self.SQL)
        self.assertIn("SQ14_QUOTED_IDENTIFIER_OFF",
                      {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)

    def test_the_detail_names_the_statement_and_the_decision(self):
        detail = next(f.detail for f in _t(self.SQL).findings
                      if f.rule == "SQ14_QUOTED_IDENTIFIER_OFF")
        self.assertIn("SET QUOTED_IDENTIFIER OFF", detail)
        self.assertIn("left", detail)

    def test_the_double_quoted_spans_are_left_alone(self):
        result = _t(self.SQL)
        self.assertIn('"abc"', result.translated_sql)
        self.assertNotIn("SQ10_DQUOTE_IDENT", {f.rule for f in result.findings})

    def test_bracketed_identifiers_in_such_a_file_still_convert(self):
        # The mode says nothing about `[...]`; only `"` changes meaning.
        out = _t('SET QUOTED_IDENTIFIER OFF;\nSELECT [a b] FROM dbo.t').translated_sql
        self.assertIn("`a b`", out)

    def test_quoted_identifier_on_is_not_flagged(self):
        result = _t('SET QUOTED_IDENTIFIER ON;\nSELECT "a b" FROM dbo.t')
        self.assertNotIn("SQ14_QUOTED_IDENTIFIER_OFF",
                         {f.rule for f in result.findings})
        self.assertIn("`a b`", result.translated_sql)


class BracketReachingIntoACommentTests(unittest.TestCase):
    """F1, a regression from D1 and E1 together. `_quoted_identifier_end` does
    a raw search for the closer; D1 moved the identifier branch ahead of the
    comment branch; E1 deleted `_BRACKET_IDENT_RE`, whose `[^\\]\\r\\n]+` was
    what had stopped SQ10 acting on a span like this. So an unterminated `[`
    reaches forward into a comment for its closer. Measured before the fix:

        SELECT [a\\n-- ] closes here\\nFROM dbo.t
          -> SELECT `a\\n-- ` closes here\\nFROM default.W.t   flags=0  PASS
        SELECT [a /* ] */ FROM dbo.t
          -> SELECT `a /* ` */ FROM default.W.t               flags=0  PASS

    Both emit a plausible-looking identifier built out of comment text, and
    the second leaves a dangling `*/` that Spark rejects -- with no finding,
    so both graded PASS. My justification for deleting the regex, "an
    unterminated `[` is not a span at all", held only while no `]` appeared
    later anywhere; a comment is exactly where a later `]` lives.
    """

    def test_a_bracket_reaching_across_a_line_is_refused(self):
        result = _t("SELECT [a\n-- ] closes here\nFROM dbo.t")
        self.assertIn("SQ01_UNTERMINATED_IDENTIFIER",
                      {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)
        self.assertNotIn("`", result.translated_sql)

    def test_a_bracket_reaching_into_a_block_comment_is_refused(self):
        result = _t("SELECT [a /* ] */ FROM dbo.t")
        self.assertIn("SQ01_UNTERMINATED_IDENTIFIER",
                      {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)
        self.assertNotIn("`", result.translated_sql)

    def test_the_rest_of_the_statement_is_still_read(self):
        # Refusing the span is what lets the real SQL be seen: `dbo.t` is
        # outside the bogus identifier and must still be qualified.
        out = _t("SELECT [a\n-- ] closes here\nFROM dbo.t").translated_sql
        self.assertIn("default.W.t", out)

    def test_a_single_line_dash_comment_marker_is_still_an_identifier(self):
        # Measured as correct and runnable: T-SQL's own lexer reads `[` before
        # `--`, so `[a -- ]` is the name `a -- ` and `here` is an alias. Spark
        # accepts the backquoted form, so this one is left working.
        out = _t("SELECT [a -- ] here FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `a -- ` here FROM default.W.t")

    def test_an_ordinary_double_dash_in_a_name_still_converts(self):
        out = _t("SELECT [a--b] FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT `a--b` FROM default.W.t")

    def test_a_multi_line_backtick_identifier_is_refused_too(self):
        result = _t("SELECT `a\n-- ` closes here\nFROM dbo.t")
        self.assertIn("SQ01_UNTERMINATED_IDENTIFIER",
                      {f.rule for f in result.findings})

    def test_a_multi_line_double_quoted_identifier_is_refused_too(self):
        result = _t('SELECT "a\n-- " closes here\nFROM dbo.t')
        self.assertIn("SQ01_UNTERMINATED_IDENTIFIER",
                      {f.rule for f in result.findings})


class ZeroWidthRefusalTests(unittest.TestCase):
    """F2. `_collect_string_concat` returns a zero-width item for its SQ41
    refusal. `_to_fixed_point` applied it as a no-op edit, so the text never
    changed, the same item came back every pass, the 8-pass budget burned, and
    the post-loop check reported deep nesting for a flat expression. Measured
    before the fix, on a depth-ONE expression:

        SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END + ' ' + [Region]
        FROM dbo.t
          -> 8x SQ41_CONCAT_KEYWORD + SQ03_NESTING_TOO_DEEP   flags=9

    D4 reused the helper without noticing that one of its four callers hands
    in zero-width items raw, where the other three strip them first.
    """

    SQL = ("SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END + ' ' + [Region] "
           "FROM dbo.t")

    def test_a_flat_expression_does_not_report_deep_nesting(self):
        rules = [f.rule for f in _t(self.SQL).findings]
        self.assertNotIn("SQ03_NESTING_TOO_DEEP", rules)

    def test_the_refusal_is_reported_once_not_once_per_pass(self):
        rules = [f.rule for f in _t(self.SQL).findings]
        self.assertEqual(rules.count("SQ41_CONCAT_KEYWORD"), 1)

    def test_the_flag_count_is_the_one_real_problem(self):
        self.assertEqual(_t(self.SQL).flags, 1)

    def test_the_statement_is_still_translated_around_it(self):
        out = _t(self.SQL).translated_sql
        self.assertIn("`Region`", out)
        self.assertIn("default.W.t", out)

    def test_a_genuine_concat_next_to_it_still_converts(self):
        out = _t("SELECT CASE WHEN x = 1 THEN 'a' END + ' ' + c, 'x' + y "
                 "FROM dbo.t").translated_sql
        self.assertIn("concat('x', y)", out)

    def test_deep_nesting_is_still_reported(self):
        # The budget check has to keep working; only the false trigger goes.
        deep = "SELECT " + "ISNULL(a, " * 10 + "GETDATE()" + ")" * 10
        self.assertIn("SQ03_NESTING_TOO_DEEP",
                      {f.rule for f in _t(deep).findings})


class KeywordInsideAnIdentifierTests(unittest.TestCase):
    """E-a. E2's bug with the whole translator as blast radius.
    `_CONTROL_FLOW_RE` scans the masked view, where identifier bodies are
    deliberately visible -- the naming rules need to read them -- so a column
    name containing a control-flow word tripped the procedural gate, and
    `translate()` short-circuits on that and returns the T-SQL verbatim.
    Measured before the fix:

        SELECT [exec time], c FROM dbo.t       -> unchanged, SQ01_PROCEDURAL
        SELECT [cursor position], c FROM dbo.t -> unchanged, SQ01_PROCEDURAL
        SELECT [#units], c FROM dbo.t          -> unchanged, SQ01_PROCEDURAL

    `[Exec Time]` and `[#Units]` are realistic BI column names, and one of
    them disabled every rule in the file.
    """

    def test_a_column_named_exec_time_does_not_look_procedural(self):
        result = _t("SELECT [exec time], c FROM dbo.t")
        self.assertNotIn("SQ01_PROCEDURAL", {f.rule for f in result.findings})
        self.assertEqual(result.translated_sql,
                         "SELECT `exec time`, c FROM default.W.t")

    def test_the_other_measured_names(self):
        for name, expected in (("cursor position", "`cursor position`"),
                               ("#units", "`#units`"),
                               ("declare @x", "`declare @x`"),
                               ("while loop", "`while loop`"),
                               ("begin try", "`begin try`")):
            with self.subTest(name=name):
                result = _t(f"SELECT [{name}], c FROM dbo.t")
                self.assertNotIn("SQ01_PROCEDURAL",
                                 {f.rule for f in result.findings})
                self.assertEqual(
                    result.translated_sql,
                    f"SELECT {expected}, c FROM default.W.t")

    def test_the_double_quoted_spelling_too(self):
        result = _t('SELECT "exec time", c FROM dbo.t')
        self.assertNotIn("SQ01_PROCEDURAL", {f.rule for f in result.findings})
        self.assertIn("`exec time`", result.translated_sql)

    def test_a_real_procedural_construct_is_still_flagged(self):
        for sql in ("DECLARE @n INT; SELECT @n",
                    "EXEC dbo.sp_load",
                    # `SELECT * INTO #tmp` was here and moved to
                    # ControlFlowCoverageTests: a temp table is no longer
                    # control flow, it is SQ51_TEMP_TABLE by name.
                    "WHILE 1 = 1 BEGIN SELECT 1 END",
                    "SET @n = 1"):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertIn("SQ01_PROCEDURAL",
                              {f.rule for f in result.findings})
                self.assertEqual(result.translated_sql, sql)

    def test_a_control_word_inside_a_string_literal_is_still_ignored(self):
        result = _t("SELECT 'exec this' AS note FROM dbo.t")
        self.assertNotIn("SQ01_PROCEDURAL", {f.rule for f in result.findings})
        self.assertIn("default.W.t", result.translated_sql)


class ConstraintScanInsideAnIdentifierTests(unittest.TestCase):
    """E-b. `rule_constraints` locates IDENTITY, inline PRIMARY KEY/UNIQUE and
    DEFAULT on the masked view, where identifier bodies are visible, and then
    cuts at those offsets -- so it read the word inside the backticks SQ10 had
    just produced and deleted it. Measured before the fix:

        CREATE TABLE dbo.t ([identity] [int] NOT NULL)
          -> CREATE TABLE default.W.t (`` int NOT NULL)
        CREATE TABLE dbo.t ([identity no] [int] NOT NULL)
          -> CREATE TABLE default.W.t (`no` int NOT NULL)
        CREATE TABLE dbo.t ([my unique id] [int] NOT NULL)
          -> CREATE TABLE default.W.t (`my id` int NOT NULL)

    The first emits the empty backquoted identifier that E1 added
    SQ10_BRACKET_EMPTY to refuse -- "cannot resolve", by that rule's own
    docstring -- two rules after refusing it, and with a `rewrite` finding
    calling it a success. The second and third rename the user's column.
    """

    def test_a_column_named_identity_keeps_its_name(self):
        result = _t("CREATE TABLE dbo.t ([identity] [int] NOT NULL)")
        self.assertEqual(result.translated_sql,
                         "CREATE TABLE default.W.t (`identity` int NOT NULL)")
        self.assertNotIn("SQ70_IDENTITY", {f.rule for f in result.findings})

    def test_no_empty_backquoted_identifier_is_ever_emitted(self):
        out = _t("CREATE TABLE dbo.t ([identity] [int] NOT NULL)").translated_sql
        self.assertNotIn("``", out)

    def test_a_column_whose_name_starts_with_identity(self):
        out = _t("CREATE TABLE dbo.t ([identity no] [int] NOT NULL)").translated_sql
        # The space also earns SQ77's column mapping; the body is what this pins.
        self.assertTrue(out.startswith("CREATE TABLE default.W.t (`identity no` int NOT NULL)"), out)

    def test_a_column_whose_name_contains_unique(self):
        result = _t("CREATE TABLE dbo.t ([my unique id] [int] NOT NULL)")
        self.assertTrue(result.translated_sql.startswith(
            "CREATE TABLE default.W.t (`my unique id` int NOT NULL)"), result.translated_sql)
        self.assertNotIn("SQ70_INLINE_CONSTRAINT",
                         {f.rule for f in result.findings})

    def test_a_column_whose_name_contains_default_raises_no_flag(self):
        result = _t("CREATE TABLE dbo.t ([default value] [int] NOT NULL)")
        self.assertNotIn("SQ70_DEFAULT", {f.rule for f in result.findings})
        self.assertIn("`default value` int", result.translated_sql)

    def test_the_real_constructs_are_still_removed_and_flagged(self):
        result = _t("CREATE TABLE dbo.t (id INT IDENTITY(1,1) NOT NULL, "
                    "amt INT DEFAULT 0, CONSTRAINT pk PRIMARY KEY (id))")
        rules = {f.rule for f in result.findings}
        self.assertIn("SQ70_IDENTITY", rules)
        self.assertIn("SQ70_DEFAULT", rules)
        self.assertIn("SQ70_TABLE_CONSTRAINT", rules)
        self.assertNotIn("IDENTITY", result.translated_sql)
        self.assertNotIn("PRIMARY KEY", result.translated_sql)
        self.assertIn("amt INT DEFAULT 0", result.translated_sql)

    def test_a_real_inline_unique_is_still_removed(self):
        result = _t("CREATE TABLE dbo.t (code INT UNIQUE)")
        self.assertIn("SQ70_INLINE_CONSTRAINT", {f.rule for f in result.findings})
        self.assertNotIn("UNIQUE", result.translated_sql)


class BareCastTypeTests(unittest.TestCase):
    """E-c. D3 fixed the CONVERT path; the bare-CAST path still passed any
    type name outside SQ60's own table straight through, with no finding.
    Measured before the fix:

        SELECT CAST(c AS rowversion) FROM dbo.t   -> unchanged, flags=0, PASS
        SELECT CAST(c AS sysname) FROM dbo.t      -> unchanged, flags=0, PASS
        SELECT CAST(c AS dbo.MyAlias) FROM dbo.t  -> unchanged, flags=0, PASS
        SELECT CAST(c AS money) FROM dbo.t        -> left as MONEY + flag,
            while CONVERT(money, c) mapped it to DECIMAL(19,4) + flag

    The two paths now resolve a cast's target type through the same
    `_spark_convert_type`, so they cannot give different answers.
    """

    def test_an_unknown_type_in_a_cast_is_flagged(self):
        result = _t("SELECT CAST(c AS rowversion) FROM dbo.t")
        self.assertIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)
        self.assertIn("CAST(c AS rowversion)", result.translated_sql)

    def test_the_other_measured_unknowns(self):
        for written in ("sysname", "dbo.MyAlias", "geography_x"):
            with self.subTest(written=written):
                result = _t(f"SELECT CAST(c AS {written}) FROM dbo.t")
                self.assertIn("SQ60_TYPE_UNKNOWN",
                              {f.rule for f in result.findings})

    def test_the_detail_names_the_type(self):
        detail = next(f.detail for f in _t("SELECT CAST(c AS rowversion)").findings
                      if f.rule == "SQ60_TYPE_UNKNOWN")
        self.assertIn("rowversion", detail)

    def test_money_in_a_cast_now_agrees_with_convert(self):
        cast = _t("SELECT CAST(c AS money) FROM dbo.t").translated_sql
        convert = _t("SELECT CONVERT(money, c) FROM dbo.t").translated_sql
        self.assertEqual(cast, convert)
        self.assertIn("DECIMAL(19,4)", cast)

    def test_uniqueidentifier_in_a_cast_now_agrees_with_convert(self):
        cast = _t("SELECT CAST(c AS uniqueidentifier) FROM dbo.t").translated_sql
        convert = _t("SELECT CONVERT(uniqueidentifier, c) FROM dbo.t").translated_sql
        self.assertEqual(cast, convert)

    def test_a_lossy_cast_mapping_still_needs_review(self):
        self.assertTrue(_t("SELECT CAST(c AS money) FROM dbo.t").needs_manual_review)

    def test_a_type_with_no_spark_equivalent_keeps_its_explanation(self):
        # TIME and XML have no Spark type at all, so both paths refuse; SQ60
        # keeps its specific advice rather than a generic "unknown".
        for written, rule in (("time", "SQ60_TIME"), ("xml", "SQ60_XML")):
            with self.subTest(written=written):
                result = _t(f"SELECT CAST(c AS {written}) FROM dbo.t")
                self.assertIn(rule, {f.rule for f in result.findings})
                self.assertIn(f"CAST(c AS {written})", result.translated_sql)

    def test_the_mapped_types_are_unchanged(self):
        # varchar(20)/nvarchar(50) moved to SQ61's truncating form in the
        # tsql-silent-values batch and are asserted there.
        for written, expected in (("datetime", "TIMESTAMP"),
                                  ("bit", "BOOLEAN"),
                                  ("numeric(10,2)", "DECIMAL(10,2)")):
            with self.subTest(written=written):
                out = _t(f"SELECT CAST(c AS {written}) FROM dbo.t").translated_sql
                self.assertEqual(out, f"SELECT CAST(c AS {expected}) FROM default.W.t")

    def test_types_spark_already_accepts_are_left_exactly_alone(self):
        for written in ("int", "bigint", "date", "decimal(10,2)", "STRING"):
            with self.subTest(written=written):
                result = _t(f"SELECT CAST(c AS {written}) FROM dbo.t")
                self.assertEqual(result.translated_sql,
                                 f"SELECT CAST(c AS {written}) FROM default.W.t")
                self.assertEqual(
                    [f for f in result.findings if f.rule.startswith("SQ60")], [])

    def test_a_column_alias_in_a_subquery_is_not_read_as_a_type(self):
        # The reason this is anchored on `CAST(` and not on `AS <word>)`.
        result = _t("SELECT * FROM (SELECT c AS name) t")
        self.assertEqual(result.translated_sql, "SELECT * FROM (SELECT c AS name) t")
        self.assertEqual([f for f in result.findings if f.rule.startswith("SQ60")], [])

    def test_a_nested_cast_resolves_both_levels(self):
        # The inner varchar(10) also truncates now (SQ61); the nesting,
        # which is what this test is about, is unchanged.
        out = _t("SELECT CAST(CAST(x AS varchar(10)) AS int) FROM dbo.t").translated_sql
        self.assertEqual(
            out,
            "SELECT CAST(substring(CAST(x AS STRING), 1, 10) AS int) "
            "FROM default.W.t")

    def test_a_column_definition_is_untouched_by_this(self):
        # MONEY in a *column* keeps its name on purpose: that decision is
        # about a stored column's rounding and is argued separately.
        result = _t("CREATE TABLE dbo.t (amount MONEY)")
        self.assertIn("amount MONEY", result.translated_sql)
        self.assertIn("SQ60_MONEY", {f.rule for f in result.findings})


class SelectIntoTargetTests(unittest.TestCase):
    """INTO. `_OBJECT_KEYWORDS` listed `INSERT INTO` and `MERGE INTO` but not
    the bare `INTO` of `SELECT ... INTO`, so SQ11 never saw the table being
    created. Measured before the fix:

        SELECT a INTO dbo.u FROM dbo.t
          -> CREATE TABLE dbo.u AS SELECT a FROM default.W.t   flags=0  PASS

    The source qualified, the target not: the statement reads the right table
    and creates the new one somewhere else, with nothing flagged.
    """

    def test_the_created_table_is_qualified_too(self):
        result = _t("SELECT a INTO dbo.u FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "CREATE TABLE default.W.u AS SELECT a FROM default.W.t")
        self.assertEqual(result.flags, 0, [str(f) for f in result.findings])

    def test_both_names_are_reported(self):
        rules = [f.rule for f in _t("SELECT a INTO dbo.u FROM dbo.t").findings]
        self.assertEqual(rules.count("SQ11_TWO_PART_NAME"), 2)

    def test_insert_into_still_works_and_is_not_matched_twice(self):
        result = _t("INSERT INTO dbo.u SELECT a FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "INSERT INTO default.W.u SELECT a FROM default.W.t")
        rules = [f.rule for f in result.findings]
        self.assertEqual(rules.count("SQ11_TWO_PART_NAME"), 2)

    def test_merge_into_still_works(self):
        out = _t("MERGE INTO dbo.u AS tgt USING dbo.t AS src ON 1=1").translated_sql
        self.assertIn("MERGE INTO default.W.u", out)

    def test_a_temp_table_target_is_not_turned_into_a_ctas(self):
        # `#tmp` used to trip the procedural gate, which refused the whole
        # object. It is now SQ51_TEMP_TABLE by name and the rest of the
        # statement is still translated -- but no CTAS is built, because it
        # would create a permanent table from a session-scoped one.
        result = _t("SELECT a INTO #tmp FROM dbo.t")
        self.assertIn("SQ51_TEMP_TABLE", {f.rule for f in result.findings})
        self.assertNotIn("SQ51_SELECT_INTO", {f.rule for f in result.findings})
        self.assertEqual(result.translated_sql,
                         "SELECT a INTO #tmp FROM default.W.t")

    def test_a_variable_target_is_not_treated_as_a_table(self):
        result = _t("SELECT a INTO @tbl FROM dbo.t")
        self.assertNotIn("default.W.@tbl", result.translated_sql)

    def test_a_three_part_into_target_still_resolves(self):
        out = _t("SELECT a INTO AcmeDW.dbo.u FROM dbo.t").translated_sql
        self.assertIn("default.AcmeDW.u", out)


class TryConvertTests(unittest.TestCase):
    """TRY_CONVERT. `_CONVERT_RE` is `\\bCONVERT\\s*\\(`, and `\\b` does not
    match inside `TRY_CONVERT` because the `_` is a word character -- the same
    miss E-c found for TRY_CAST. So every TRY_CONVERT passed through verbatim.
    Measured before the fix:

        SELECT TRY_CONVERT(nvarchar(50), c) FROM dbo.t -> unchanged, flags=0
        SELECT TRY_CONVERT(int, c) FROM dbo.t          -> unchanged, flags=0
        SELECT TRY_CONVERT(varchar, d, 120) FROM dbo.t -> unchanged, flags=0

    and on Spark 4.2.0 `select TRY_CONVERT(nvarchar(50), 'x')` fails with
    UNRESOLVED_ROUTINE -- whatever its type argument, so the silence is the
    bug independently of the mapping.

    The target is `try_cast`, not `CAST`: TRY_CONVERT returns NULL where
    CONVERT raises, and Spark's `try_cast` has exactly that semantics
    (`try_cast('abc' AS int)` -> NULL, `cast('abc' AS int)` -> CAST_INVALID_INPUT
    under ANSI). Mapping it to CAST would turn a NULL into a failed query.
    """

    def test_a_try_convert_becomes_a_try_cast(self):
        # try_cast rather than CAST is the point here; the length now
        # survives too (SQ61, tsql-silent-values).
        result = _t("SELECT TRY_CONVERT(nvarchar(50), c) FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "SELECT substring(try_cast(c AS STRING), 1, 50) FROM default.W.t")
        self.assertNotIn("CAST(", result.translated_sql)

    def test_it_is_not_mapped_to_a_plain_cast(self):
        out = _t("SELECT TRY_CONVERT(int, c) FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT try_cast(c AS int) FROM default.W.t")
        self.assertNotIn("CAST(", out)

    def test_the_finding_says_why_not_cast(self):
        detail = next(f.detail for f in
                      _t("SELECT TRY_CONVERT(int, c)").findings
                      if f.rule == "SQ84_TRY_CONVERT")
        self.assertIn("NULL", detail)

    def test_the_type_table_is_the_one_convert_uses(self):
        for written, expected in (("datetime", "TIMESTAMP"),
                                  ("bit", "BOOLEAN"),
                                  ("numeric(10,2)", "DECIMAL(10,2)"),
                                  ("int", "int")):
            with self.subTest(written=written):
                out = _t(f"SELECT TRY_CONVERT({written}, c) FROM dbo.t").translated_sql
                self.assertEqual(
                    out, f"SELECT try_cast(c AS {expected}) FROM default.W.t")

    def test_a_lossy_type_is_mapped_and_flagged_like_convert(self):
        result = _t("SELECT TRY_CONVERT(money, c) FROM dbo.t")
        self.assertIn("try_cast(c AS DECIMAL(19,4))", result.translated_sql)
        self.assertIn("SQ84_TRY_CONVERT_TYPE_LOSS",
                      {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)

    def test_an_unmapped_type_is_refused(self):
        result = _t("SELECT TRY_CONVERT(xml, c) FROM dbo.t")
        self.assertIn("TRY_CONVERT(xml, c)", result.translated_sql)
        self.assertIn("SQ84_TRY_CONVERT_TYPE", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)

    def test_a_style_code_is_refused_the_way_convert_refuses_it(self):
        result = _t("SELECT TRY_CONVERT(varchar, d, 120) FROM dbo.t")
        self.assertIn("TRY_CONVERT(varchar, d, 120)", result.translated_sql)
        rule, detail = next((f.rule, f.detail) for f in result.findings
                            if "STYLE" in f.rule)
        self.assertEqual(rule, "SQ84_TRY_CONVERT_STYLE")
        convert_detail = next(f.detail for f in
                              _t("SELECT CONVERT(varchar, d, 120)").findings
                              if f.rule == "SQ81_CONVERT_STYLE")
        self.assertEqual(detail.replace("TRY_CONVERT", "CONVERT"), convert_detail)

    def test_a_nested_try_convert_is_reached(self):
        out = _t("SELECT TRY_CONVERT(int, TRY_CONVERT(varchar, x))").translated_sql
        self.assertEqual(out, "SELECT try_cast(try_cast(x AS STRING) AS int)")

    def test_plain_convert_is_unchanged(self):
        out = _t("SELECT CONVERT(nvarchar(50), c) FROM dbo.t").translated_sql
        self.assertEqual(
            out, "SELECT substring(CAST(c AS STRING), 1, 50) FROM default.W.t")

    def test_no_try_convert_survives_into_the_output(self):
        for written in ("nvarchar(50)", "int", "datetime", "money"):
            with self.subTest(written=written):
                out = _t(f"SELECT TRY_CONVERT({written}, c)").translated_sql
                self.assertNotIn("TRY_CONVERT", out)


class UnmappableColumnTypeTests(unittest.TestCase):
    """The column-definition half of E-c. `_COLUMN_TYPE_RE` is built from
    SQ60's own name tables, so a T-SQL type outside them matched nothing,
    raised nothing, and the DDL passed through. Measured before the fix:

        CREATE TABLE dbo.t (c rowversion) -> unchanged, flags=0, PASS
        CREATE TABLE dbo.t (c sysname)    -> unchanged, flags=0, PASS

    and on Spark 4.2.0 `create table t (c rowversion) using parquet` fails.

    No type parsing: the names we cannot map are enumerated, and they are
    looked for in the *type* position of a column definition -- the same
    positional pattern the known names already use.

    What makes that safe is POSITION, not E-a's blanking of identifier bodies.
    I expected the blanking to be the guard and measured that it is not: the
    first attempt added these names to `_KNOWN_TYPE_NAMES` so SQ10 would
    unwrap the DacFx `[rowversion]` type spelling, and SQ10's unwrapping is
    position-blind, so a column *named* `[rowversion]` lost its quoting --
    `` `rowversion` INT `` became `rowversion INT`. The type slot accepts the
    backquoted wrapper instead, and the name slot is never a match.
    """

    def test_an_unmappable_column_type_is_flagged(self):
        result = _t("CREATE TABLE dbo.t (c rowversion)")
        self.assertIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)
        self.assertIn("c rowversion", result.translated_sql)

    def test_sysname_too(self):
        result = _t("CREATE TABLE dbo.t (c sysname)")
        self.assertIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})

    def test_the_detail_names_the_type_and_the_column(self):
        detail = next(f.detail for f in _t("CREATE TABLE dbo.t (c rowversion)").findings
                      if f.rule == "SQ60_TYPE_UNKNOWN")
        self.assertIn("rowversion", detail)
        self.assertIn("c", detail)

    def test_the_dacfx_bracketed_spelling_is_reached(self):
        # DacFx writes the type bracketed: `[c] [rowversion] NOT NULL`. SQ10
        # unwraps a bracketed *type* name to a bare word, which is what lets
        # this rule see it -- so the unmappable names have to be known types.
        result = _t("CREATE TABLE dbo.t ([c] [rowversion] NOT NULL)")
        self.assertIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})

    def test_a_column_NAMED_rowversion_draws_no_flag(self):
        # The verification that matters: E-a blanks identifier bodies before a
        # keyword scan, so the name is invisible here.
        for name in ("[rowversion]", "`rowversion`", '"rowversion"'):
            with self.subTest(name=name):
                result = _t(f"CREATE TABLE dbo.t ({name} INT NOT NULL)")
                self.assertNotIn("SQ60_TYPE_UNKNOWN",
                                 {f.rule for f in result.findings})
                self.assertIn("`rowversion` INT", result.translated_sql)

    def test_an_unquoted_column_named_rowversion_draws_no_flag_either(self):
        # Better than the worst case: the name position is not the type
        # position, so even an unquoted `rowversion INT` is safe.
        result = _t("CREATE TABLE dbo.t (rowversion INT NOT NULL)")
        self.assertNotIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})

    def test_the_word_outside_a_create_table_is_ignored(self):
        for sql in ("SELECT rowversion FROM dbo.t",
                    "SELECT c AS sysname FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertNotIn("SQ60_TYPE_UNKNOWN",
                                 {f.rule for f in _t(sql).findings})

    def test_a_cast_to_the_same_type_is_still_flagged(self):
        # E-c's half, unchanged: one id for one situation in both positions.
        self.assertIn("SQ60_TYPE_UNKNOWN",
                      {f.rule for f in _t("SELECT CAST(c AS rowversion)").findings})

    def test_mappable_columns_are_untouched(self):
        result = _t("CREATE TABLE dbo.t (a NVARCHAR(50), b BIT, c DATETIME2(7))")
        self.assertNotIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})
        self.assertIn("a STRING", result.translated_sql)
        self.assertIn("b BOOLEAN", result.translated_sql)
        self.assertIn("c TIMESTAMP", result.translated_sql)

    def test_a_flagged_type_keeps_its_own_message(self):
        # MONEY and friends are enumerated as unmappable too, but they have
        # specific advice and it is not replaced by a generic refusal.
        rules = {f.rule for f in _t("CREATE TABLE dbo.t (amount MONEY)").findings}
        self.assertIn("SQ60_MONEY", rules)
        self.assertNotIn("SQ60_TYPE_UNKNOWN", rules)


class TSqlTimestampColumnTests(unittest.TestCase):
    """T-SQL's `timestamp` *column* type is a documented synonym for
    `rowversion`: an 8-byte binary row version, not a datetime. We passed it
    through as Spark's TIMESTAMP, so the migrated table got a datetime column
    where the source had a row version. Measured before the fix:

        CREATE TABLE dbo.t (c timestamp)              -> unchanged, flags=0
        CREATE TABLE dbo.t ([c] [timestamp] NOT NULL) -> (`c` timestamp ...),
                                                         flags=0

    Worse than the unrunnable cases this batch is named for: unrunnable DDL
    fails loudly on the cluster, this succeeds with a different schema.

    The ambiguity is only in the CAST position, where the word is also a
    legitimate Spark type and the Spark meaning is the likelier intent in a
    migrated file. T-SQL admits no second reading in a column definition, so
    the two positions differ on purpose -- no whole-file dialect guess needed.
    """

    def test_a_timestamp_column_is_flagged(self):
        result = _t("CREATE TABLE dbo.t (c timestamp)")
        self.assertIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})
        self.assertTrue(result.needs_manual_review)
        self.assertIn("c timestamp", result.translated_sql)

    def test_the_detail_says_it_is_rowversion_not_a_datetime(self):
        detail = next(f.detail for f in _t("CREATE TABLE dbo.t (c timestamp)").findings
                      if f.rule == "SQ60_TYPE_UNKNOWN")
        self.assertIn("rowversion", detail.casefold())
        self.assertIn("datetime", detail.casefold())

    def test_the_dacfx_bracketed_spelling_is_flagged(self):
        result = _t("CREATE TABLE dbo.t ([c] [timestamp] NOT NULL)")
        self.assertIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})

    def test_a_cast_to_timestamp_is_left_alone(self):
        # Deliberately different from the column position; see the rule.
        result = _t("SELECT CAST(c AS timestamp) FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT CAST(c AS timestamp) FROM default.W.t")
        self.assertEqual([f for f in result.findings if f.rule.startswith("SQ60")], [])

    def test_a_convert_to_timestamp_is_left_alone_too(self):
        out = _t("SELECT CONVERT(timestamp, c) FROM dbo.t").translated_sql
        self.assertEqual(out, "SELECT CAST(c AS timestamp) FROM default.W.t")

    def test_a_column_NAMED_timestamp_draws_no_flag(self):
        # Same false-positive check that paid off for [rowversion]: run it,
        # do not assume it.
        result = _t("CREATE TABLE dbo.t ([timestamp] INT NOT NULL)")
        self.assertNotIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})
        self.assertIn("INT NOT NULL", result.translated_sql)

    def test_a_column_named_timestamp_in_the_other_spellings(self):
        for name in ('"timestamp"', "`timestamp`", "[my timestamp]"):
            with self.subTest(name=name):
                result = _t(f"CREATE TABLE dbo.t ({name} INT NOT NULL)")
                self.assertNotIn("SQ60_TYPE_UNKNOWN",
                                 {f.rule for f in result.findings})

    def test_datetime2_is_still_mapped(self):
        result = _t("CREATE TABLE dbo.t (c DATETIME2(7))")
        self.assertIn("c TIMESTAMP", result.translated_sql)
        self.assertNotIn("SQ60_TYPE_UNKNOWN", {f.rule for f in result.findings})

    def test_rowversion_and_sysname_still_behave(self):
        for written in ("rowversion", "sysname"):
            with self.subTest(written=written):
                self.assertIn(
                    "SQ60_TYPE_UNKNOWN",
                    {f.rule for f in _t(f"CREATE TABLE dbo.t (c {written})").findings})
                self.assertIn(
                    "SQ60_TYPE_UNKNOWN",
                    {f.rule for f in _t(f"SELECT CAST(c AS {written})").findings})


class CtasColumnListTests(unittest.TestCase):
    r"""T1. `_CREATE_TABLE_BODY_RE` was `\bCREATE\s+TABLE\b[^(]*\(`, and
    `[^(]*` ran straight past `AS SELECT ... WHERE` to the first `(`
    anywhere after CREATE TABLE. Measured before the fix, both statements
    in one document:

        CREATE TABLE dbo.t AS SELECT a FROM dbo.u WHERE (a > 0);
        CREATE TABLE dbo.v (id INT IDENTITY(1,1) PRIMARY KEY);
          -> ... CREATE TABLE default.W.v (id INT IDENTITY(1,1) PRIMARY KEY);
             findings: SQ11_TWO_PART_NAME x3 and nothing else

    `(a > 0)` was read as the column list, nothing in it matched, so
    `changed` stayed False and rule_constraints returned before it ever saw
    the genuine body. Spark 4.2.0 rejects the surviving statement with
    PARSE_SYNTAX_ERROR at 'IDENTITY' (measured, `USING parquet`).
    """

    SQL = ("CREATE TABLE dbo.t AS SELECT a FROM dbo.u WHERE (a > 0);\n"
           "CREATE TABLE dbo.v (id INT IDENTITY(1,1) PRIMARY KEY);")

    def test_the_real_column_list_after_a_ctas_is_still_cleaned(self):
        result = _t(self.SQL)
        self.assertNotIn("IDENTITY", result.translated_sql)
        self.assertNotIn("PRIMARY KEY", result.translated_sql)
        rules = {f.rule for f in result.findings}
        self.assertIn("SQ70_IDENTITY", rules)
        self.assertIn("SQ70_INLINE_CONSTRAINT", rules)

    def test_the_ctas_select_is_left_exactly_as_it_was(self):
        out = _t(self.SQL).translated_sql
        self.assertIn("AS SELECT a FROM default.W.u WHERE (a > 0);", out)

    def test_a_ctas_on_its_own_is_not_a_column_list(self):
        """Nothing in `(a > 0)` is a column, so nothing may be reported for
        it -- and the body must come back byte-identical."""
        sql = "CREATE TABLE dbo.t AS SELECT a FROM dbo.u WHERE (a > 0)"
        result = _t(sql)
        self.assertEqual([f.rule for f in result.findings
                          if f.rule.startswith("SQ70")], [])
        self.assertTrue(result.translated_sql.endswith("WHERE (a > 0)"),
                        result.translated_sql)


class EveryCreateTableBodyTests(unittest.TestCase):
    """T5, first half. `_CREATE_TABLE_BODY_RE.search` is one body per
    document. Measured before the fix, two identical tables in one file:

        CREATE TABLE dbo.a (id INT IDENTITY(1,1) PRIMARY KEY, n VARCHAR(10));
        CREATE TABLE dbo.b (id INT IDENTITY(1,1) PRIMARY KEY, n VARCHAR(10));
          -> CREATE TABLE default.W.a (id INT, n STRING);
             CREATE TABLE default.W.b (id INT IDENTITY(1,1) PRIMARY KEY,
                                       n STRING);
             findings: SQ60_TYPE x2, SQ70_INLINE_CONSTRAINT, SQ70_IDENTITY

    `dbo.b` kept the whole clause with no finding of its own, so the four
    that did fire read as if the file had been cleaned.
    """

    SQL = ("CREATE TABLE dbo.a (id INT IDENTITY(1,1) PRIMARY KEY, n VARCHAR(10));\n"
           "CREATE TABLE dbo.b (id INT IDENTITY(1,1) PRIMARY KEY, n VARCHAR(10));")

    def test_the_second_table_is_cleaned_too(self):
        out = _t(self.SQL).translated_sql
        self.assertEqual(
            out,
            "CREATE TABLE default.W.a (id INT, n STRING);\n"
            "CREATE TABLE default.W.b (id INT, n STRING);")

    def test_the_second_table_gets_its_own_findings(self):
        rules = [f.rule for f in _t(self.SQL).findings]
        self.assertEqual(rules.count("SQ70_IDENTITY"), 2)
        self.assertEqual(rules.count("SQ70_INLINE_CONSTRAINT"), 2)

    def test_the_nullability_rule_reaches_the_second_body_as_well(self):
        """Same regex, same `search`: SQ72 stopped at the first body too."""
        out = _t("CREATE TABLE dbo.a (x INT NULL);\n"
                 "CREATE TABLE dbo.b (y INT NULL);").translated_sql
        self.assertEqual(out, "CREATE TABLE default.W.a (x INT);\n"
                              "CREATE TABLE default.W.b (y INT);")


class InlineColumnModifierTests(unittest.TestCase):
    """T5, second half. `_INLINE_PK_RE`/`_IDENTITY_RE` covered PRIMARY KEY,
    UNIQUE and IDENTITY and nothing else. Measured before the fix:

        CREATE TABLE dbo.a (
          id INT NOT NULL CHECK (id > 0),
          fk INT REFERENCES dbo.b(id),
          n VARCHAR(10) COLLATE Latin1_General_CI_AS
        );
          -> all three modifiers survive; findings: [SQ11, SQ60_TYPE]

    Each on its own with `USING parquet`, on both Sparks. On 4.2.0 CHECK and
    REFERENCES are UNSUPPORTED_FEATURE.TABLE_OPERATION ("does not support
    CONSTRAINT") and `COLLATE Latin1_General_CI_AS` is
    COLLATION_INVALID_NAME. **On 3.5.0, which is what the AIDP cluster
    runs, all three are PARSE_SYNTAX_ERROR** -- collations arrived in Spark
    4.0, so on the target no COLLATE clause parses at all, `UTF8_LCASE`
    included. Rejected either way, which is why the rule removes them; the
    versions differ only in how loudly.
    """

    SQL = ("CREATE TABLE dbo.a (\n"
           "  id INT NOT NULL CHECK (id > 0),\n"
           "  fk INT REFERENCES dbo.b(id),\n"
           "  n VARCHAR(10) COLLATE Latin1_General_CI_AS\n"
           ");")

    def test_all_three_are_removed(self):
        out = _t(self.SQL).translated_sql
        for word in ("CHECK", "REFERENCES", "COLLATE"):
            with self.subTest(word=word):
                self.assertNotIn(word, out)

    def test_each_one_has_its_own_finding(self):
        rules = {f.rule for f in _t(self.SQL).findings}
        self.assertIn("SQ70_INLINE_CHECK", rules)
        self.assertIn("SQ70_INLINE_REFERENCES", rules)
        self.assertIn("SQ70_COLLATE", rules)

    def test_each_finding_quotes_what_it_removed(self):
        by_rule = {f.rule: f.detail for f in _t(self.SQL).findings}
        self.assertIn("'CHECK (id > 0)'", by_rule["SQ70_INLINE_CHECK"])
        self.assertIn("'REFERENCES dbo.b(id)'",
                      by_rule["SQ70_INLINE_REFERENCES"])
        self.assertIn("'COLLATE Latin1_General_CI_AS'",
                      by_rule["SQ70_COLLATE"])

    def test_collate_says_the_comparison_changes(self):
        detail = next(f.detail for f in _t(self.SQL).findings
                      if f.rule == "SQ70_COLLATE")
        self.assertIn("SENSITIVE", detail)

    def test_the_columns_themselves_survive(self):
        out = _t(self.SQL).translated_sql
        self.assertIn("id INT NOT NULL", out)
        self.assertIn("fk INT", out)
        self.assertIn("n STRING", out)

    def test_a_check_with_nested_parens_is_cut_whole(self):
        r"""`CHECK (a IN (1, 2))` nests; a `\([^)]*\)` tail would stop at the
        inner `)` and leave a stray one in the column list."""
        out = _t("CREATE TABLE dbo.a (id INT CHECK (id IN (1, 2)), n INT)")
        self.assertEqual(out.translated_sql,
                         "CREATE TABLE default.W.a (id INT, n INT)")

    def test_a_column_named_check_or_collate_keeps_its_name(self):
        """The same trap E-b closed for IDENTITY: these patterns read a
        blanked view, so a bracketed name must not be cut."""
        for name in ("[check value]", "[collate order]", "[references list]"):
            with self.subTest(name=name):
                result = _t(f"CREATE TABLE dbo.t ({name} [int] NOT NULL)")
                self.assertIn(name.strip("[]"), result.translated_sql)
                self.assertEqual(
                    [f.rule for f in result.findings
                     if f.rule in ("SQ70_INLINE_CHECK", "SQ70_COLLATE",
                                   "SQ70_INLINE_REFERENCES")], [])


class ControlFlowCoverageTests(unittest.TestCase):
    """T2. `_CONTROL_FLOW_RE` knew DECLARE @, SET @, BEGIN TRY, BEGIN CATCH,
    WHILE, GOTO, CURSOR, EXEC, EXECUTE and #temp, and the README said the
    tool flags "any T-SQL control flow". Measured before the fix, each of
    these came back unchanged with no procedural finding:

      IF EXISTS (SELECT 1 FROM dbo.t) SELECT 1   []
      BEGIN\n SELECT 1\nEND                      []
      SELECT 1\nRETURN                           []
      THROW 50000, 'x', 1                        []
      RAISERROR('x', 16, 1)                      []
      WAITFOR DELAY '00:00:05'                   []
      BEGIN TRANSACTION\nSELECT 1\nCOMMIT         []
      SELECT a FROM dbo.t INNER HASH JOIN ...    [SQ11 only]
      SELECT * FROM dbo.t CROSS APPLY dbo.f(...) [SQ11 only]

    Spark 4.2.0 rejects all nine. Seven are refused whole (they are
    statements, and the object is not a query); the join hint and APPLY are
    flagged in place, because only one clause of an otherwise ordinary
    SELECT is at fault.
    """

    REFUSED = (
        "IF EXISTS (SELECT 1 FROM dbo.t) SELECT 1",
        "BEGIN\n SELECT 1\nEND",
        "SELECT 1\nRETURN",
        "THROW 50000, 'x', 1",
        "RAISERROR('x', 16, 1)",
        "WAITFOR DELAY '00:00:05'",
        "BEGIN TRANSACTION\nSELECT 1\nCOMMIT",
    )

    def test_each_refused_construct_is_flagged_whole(self):
        for sql in self.REFUSED:
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertEqual([f.rule for f in result.findings],
                                 ["SQ01_PROCEDURAL"])
                self.assertEqual(result.translated_sql, sql)

    def test_return_is_refused_because_spark_reads_it_as_an_alias(self):
        """The one that is not a syntax error. Measured on Spark 4.2.0:
        `SELECT 1\nRETURN` is ACCEPTED and comes back as one column NAMED
        `RETURN` holding 1 -- a procedural keyword silently became a column
        name, which is worse than a rejection."""
        result = _t("SELECT 1\nRETURN")
        self.assertEqual([f.rule for f in result.findings], ["SQ01_PROCEDURAL"])

    def test_a_join_hint_is_flagged_not_refused(self):
        result = _t("SELECT a FROM dbo.t INNER HASH JOIN dbo.u ON t.a=u.a")
        rules = [f.rule for f in result.findings]
        self.assertIn("SQ80_JOIN_HINT", rules)
        self.assertNotIn("SQ01_PROCEDURAL", rules)
        self.assertIn("default.W.t", result.translated_sql)

    def test_a_merge_join_hint_is_not_reported_as_a_merge_statement(self):
        r"""`\bMERGE\s+` matched `MERGE JOIN` and told the operator about
        OUTPUT and WHEN NOT MATCHED BY SOURCE, neither of which is in the
        SQL."""
        rules = [f.rule for f in
                 _t("SELECT a FROM dbo.t INNER MERGE JOIN dbo.u ON t.a=u.a").findings]
        self.assertIn("SQ80_JOIN_HINT", rules)
        self.assertNotIn("SQ80_MERGE", rules)

    def test_a_real_merge_statement_still_reports_sq80_merge(self):
        rules = [f.rule for f in _t(
            "MERGE INTO dbo.t USING dbo.u ON 1=1 "
            "WHEN MATCHED THEN UPDATE SET a=1").findings]
        self.assertIn("SQ80_MERGE", rules)

    def test_apply_is_flagged_not_refused(self):
        for keyword in ("CROSS", "OUTER"):
            with self.subTest(keyword=keyword):
                rules = [f.rule for f in _t(
                    f"SELECT * FROM dbo.t {keyword} APPLY dbo.f(t.a) x").findings]
                self.assertIn("SQ80_APPLY", rules)
                self.assertNotIn("SQ01_PROCEDURAL", rules)

    def test_the_existing_hint_and_output_behaviour_is_unchanged(self):
        for sql, rule in (
                ("SELECT a FROM dbo.t WITH (NOLOCK)", "SQ80_HINT"),
                ("SELECT a FROM dbo.t OPTION (MAXDOP 1)", "SQ80_HINT"),
                ("DELETE FROM dbo.t OUTPUT deleted.a", "SQ80_OUTPUT"),
                ("EXEC sp_rename 'a', 'b'", "SQ01_PROCEDURAL")):
            with self.subTest(sql=sql):
                self.assertIn(rule, [f.rule for f in _t(sql).findings])

    def test_a_hash_inside_a_bracketed_identifier_is_still_not_a_temp_table(self):
        result = _t("SELECT [a#b] FROM dbo.t")
        self.assertEqual([f.rule for f in result.findings],
                         ["SQ10_BRACKET_IDENT", "SQ11_TWO_PART_NAME"])

    def test_a_control_flow_word_inside_an_identifier_is_still_a_column(self):
        """E-a, extended to the words this batch added."""
        result = _t("SELECT [exec time], [if any], [begin date], "
                    "[return code], [commit id] FROM dbo.t")
        self.assertNotIn("SQ01_PROCEDURAL", {f.rule for f in result.findings})

    def test_an_unbracketed_column_whose_name_starts_with_one_is_fine(self):
        result = _t("CREATE TABLE dbo.t (returns_no INT, commitment INT, "
                    "beginning INT, ifx INT)")
        self.assertNotIn("SQ01_PROCEDURAL", {f.rule for f in result.findings})

    def test_spark_s_own_if_exists_is_not_tsql_control_flow(self):
        """`DROP TABLE IF EXISTS` and `CREATE TABLE IF NOT EXISTS` are valid
        Spark SQL, which is why the IF branch is anchored to a line start."""
        for sql in ("DROP TABLE IF EXISTS dbo.t",
                    "CREATE TABLE IF NOT EXISTS dbo.t (a INT)"):
            with self.subTest(sql=sql):
                self.assertNotIn("SQ01_PROCEDURAL",
                                 {f.rule for f in _t(sql).findings})

    def test_a_clean_object_is_still_clean(self):
        for sql in ("CREATE TABLE dbo.t (id INT)",
                    "CREATE VIEW dbo.v AS SELECT 1",
                    "SELECT 1",
                    "SELECT 'DECLARE @x' AS note"):
            with self.subTest(sql=sql):
                self.assertNotIn("SQ01_PROCEDURAL",
                                 {f.rule for f in _t(sql).findings})

    def test_the_sq01_detail_lists_what_is_detected_and_says_it_is_a_list(self):
        detail = _t("THROW 50000, 'x', 1").findings[0].detail
        for word in ("IF", "RETURN", "THROW", "RAISERROR", "WAITFOR",
                     "COMMIT", "ROLLBACK"):
            with self.subTest(word=word):
                self.assertIn(word, detail)
        self.assertIn("reported clean", detail)


class TempTableTests(unittest.TestCase):
    r"""T8. `SQ51_TEMP_TABLE` was unreachable. `_CONTROL_FLOW_RE` matched
    `#\w+` and `translate()` returns on the procedural gate before RULES
    runs at all, so the branch in `rule_select_into` that carried the
    specific message could not fire. Measured before the fix:

      SELECT a INTO #tmp FROM dbo.t  -> ['SQ01_PROCEDURAL'], unchanged
      SELECT a INTO ##g   FROM dbo.t -> ['SQ01_PROCEDURAL'], unchanged
      _CONTROL_FLOW_RE.search("SELECT a INTO #tmp FROM dbo.t") -> truthy

    Made reachable rather than deleted: the statement around a temp table
    is an ordinary SELECT worth translating, and the operator needs to be
    told which construct to change and what to change it to. Spark 4.2.0
    rejects every spelling with PARSE_SYNTAX_ERROR at '#' -- measured for
    `SELECT ... INTO #tmp`, `INTO ##g`, `FROM #tmp`, `INSERT INTO #tmp` and
    `DROP TABLE #tmp` -- and accepts `CREATE OR REPLACE TEMP VIEW`, which
    is the replacement the finding names.
    """

    def test_a_select_into_a_temp_table_is_named_not_generic(self):
        result = _t("SELECT a INTO #tmp FROM dbo.t")
        rules = [f.rule for f in result.findings]
        self.assertIn("SQ51_TEMP_TABLE", rules)
        self.assertNotIn("SQ01_PROCEDURAL", rules)

    def test_the_finding_names_the_table_and_the_replacement(self):
        detail = next(f.detail for f in _t("SELECT a INTO #tmp FROM dbo.t").findings
                      if f.rule == "SQ51_TEMP_TABLE")
        self.assertIn("#tmp", detail)
        self.assertIn("TEMP VIEW", detail)

    def test_a_global_temp_table_too(self):
        detail = next(f.detail for f in _t("SELECT a INTO ##g FROM dbo.t").findings
                      if f.rule == "SQ51_TEMP_TABLE")
        self.assertIn("##g", detail)

    def test_every_other_use_of_a_temp_table_is_reached(self):
        for sql in ("SELECT a FROM #tmp",
                    "INSERT INTO #tmp SELECT a FROM dbo.t",
                    "DROP TABLE #tmp"):
            with self.subTest(sql=sql):
                self.assertIn("SQ51_TEMP_TABLE",
                              {f.rule for f in _t(sql).findings})

    def test_the_rest_of_the_statement_is_still_translated(self):
        """The whole point of not refusing it whole."""
        self.assertEqual(_t("SELECT a INTO #tmp FROM dbo.t").translated_sql,
                         "SELECT a INTO #tmp FROM default.W.t")

    def test_one_finding_per_object_not_per_occurrence(self):
        rules = [f.rule for f in
                 _t("SELECT a FROM #s JOIN #s AS x ON 1=1 JOIN #t ON 1=1").findings]
        self.assertEqual(rules.count("SQ51_TEMP_TABLE"), 1)

    def test_it_names_every_distinct_table_it_found(self):
        detail = next(
            f.detail for f in
            _t("SELECT a FROM #s JOIN #s AS x ON 1=1 JOIN #t ON 1=1").findings
            if f.rule == "SQ51_TEMP_TABLE")
        self.assertIn("#s", detail)
        self.assertIn("#t", detail)

    def test_a_hash_in_an_identifier_or_a_literal_is_not_a_temp_table(self):
        for sql in ("SELECT [a#b] FROM dbo.t", "SELECT 'a#b' FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertNotIn("SQ51_TEMP_TABLE",
                                 {f.rule for f in _t(sql).findings})

    def test_real_control_flow_beside_a_temp_table_still_refuses_whole(self):
        """SQ01 is not weakened: the gate still fires on everything else it
        ever fired on, and an object with both is still refused."""
        result = _t("DECLARE @x INT; SELECT a INTO #tmp FROM dbo.t")
        self.assertEqual([f.rule for f in result.findings], ["SQ01_PROCEDURAL"])

    def test_a_non_temp_select_into_still_becomes_a_ctas(self):
        result = _t("SELECT a INTO dbo.u FROM dbo.t")
        self.assertIn("SQ51_SELECT_INTO", {f.rule for f in result.findings})
        self.assertIn("CREATE TABLE default.W.u AS", result.translated_sql)


if __name__ == "__main__":
    unittest.main()
