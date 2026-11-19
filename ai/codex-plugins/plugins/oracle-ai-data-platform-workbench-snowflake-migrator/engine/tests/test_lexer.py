"""The SQL scanner: literals and identifiers are inviolable.

Every bug this file guards against has the same shape -- a regex or a str.split
that treated the inside of a string literal as if it were code. That is how a
comment stripper eats a real apostrophe, how a statement splitter cuts a
statement in half, and how a construct detector reports QUALIFY in a customer
name.
"""
import pytest

from snowflake_source.dialect import lexer


# ---------------------------------------------------------------- segments

def test_plain_code_is_one_code_segment():
    assert lexer.segments("select 1") == [("code", "select 1")]


def test_string_literal_is_its_own_segment():
    assert lexer.segments("select 'a'") == [
        ("code", "select "), ("string", "'a'")]


def test_doubled_quote_inside_literal_does_not_end_it():
    # 'it''s' is ONE literal, not two literals with code between them.
    assert lexer.segments("select 'it''s'") == [
        ("code", "select "), ("string", "'it''s'")]


def test_backslash_escape_inside_literal_does_not_end_it():
    assert lexer.segments(r"select 'a\'b'") == [
        ("code", "select "), ("string", r"'a\'b'")]


def test_quoted_identifier_is_its_own_segment():
    assert lexer.segments('select "col"') == [
        ("code", "select "), ("ident", '"col"')]


def test_doubled_double_quote_inside_identifier_does_not_end_it():
    assert lexer.segments('select "we""ird"') == [
        ("code", "select "), ("ident", '"we""ird"')]


def test_dollar_quoted_body_is_a_string():
    assert lexer.segments("select $$a ' ; b$$") == [
        ("code", "select "), ("string", "$$a ' ; b$$")]


def test_line_and_block_comments_are_comment_segments():
    assert lexer.segments("select 1 -- hi\nfrom t") == [
        ("code", "select 1 "), ("comment", "-- hi"), ("code", "\nfrom t")]
    assert lexer.segments("select /* hi */ 1") == [
        ("code", "select "), ("comment", "/* hi */"), ("code", " 1")]


def test_unterminated_literal_is_reported_not_guessed():
    with pytest.raises(lexer.UnterminatedLiteral):
        lexer.segments("select 'oops")


def test_unterminated_block_comment_is_reported():
    with pytest.raises(lexer.UnterminatedLiteral):
        lexer.segments("select /* oops")


# ------------------------------------------------------------ code_only

def test_code_only_blanks_literals_but_keeps_length_and_shape():
    # Literals are replaced by a same-length blank so offsets survive, and a
    # regex run over the result cannot match text that was inside a literal.
    out = lexer.code_only("select 'QUALIFY' from t")
    assert "QUALIFY" not in out
    assert out.startswith("select ") and out.rstrip().endswith("from t")


def test_code_only_hides_construct_words_inside_identifiers():
    assert "QUALIFY" not in lexer.code_only('select "QUALIFY" from t')


# -------------------------------------------------------- strip_comments

def test_strip_comments_removes_comments():
    assert lexer.strip_comments("select 1 -- c\nfrom t").split() == [
        "select", "1", "from", "t"]


def test_strip_comments_leaves_a_comment_marker_inside_a_literal_alone():
    # THE BUG: a naive `--[^\n]*` sub eats the rest of the line here.
    sql = "select 'a -- not a comment' x, 2 y"
    assert lexer.strip_comments(sql) == sql


def test_strip_comments_leaves_a_block_marker_inside_a_literal_alone():
    sql = "select '/* not a comment */' x"
    assert lexer.strip_comments(sql) == sql


# ------------------------------------------------------ split_statements

def test_split_statements_splits_on_top_level_semicolons():
    assert lexer.split_statements("select 1; select 2") == ["select 1", "select 2"]


def test_split_statements_ignores_a_semicolon_inside_a_literal():
    # THE BUG: str.split(";") turns this one statement into two, the second of
    # which begins with the word DROP.
    sql = "select 'a;drop table t' as note"
    assert lexer.split_statements(sql) == [sql]


def test_split_statements_ignores_a_semicolon_inside_a_quoted_identifier():
    sql = 'select 1 as "a;b"'
    assert lexer.split_statements(sql) == [sql]


def test_split_statements_ignores_a_semicolon_inside_a_comment():
    # One statement, not two. The comment stays attached to it -- the claim
    # under test is that the `;` inside it did not split.
    out = lexer.split_statements("select 1 -- ;drop\n")
    assert len(out) == 1
    assert out[0].startswith("select 1")
    assert lexer.leading_verb(out[0]) == "SELECT"


def test_split_statements_drops_empty_fragments():
    assert lexer.split_statements("select 1;;  ;") == ["select 1"]


# ---------------------------------------------------------- leading_verb

def test_leading_verb_skips_comments_and_parens():
    assert lexer.leading_verb("/* c */ (select 1)") == "SELECT"
    assert lexer.leading_verb("-- c\n  with x as (select 1) select * from x") == "WITH"


def test_leading_verb_of_a_literal_only_statement_is_none():
    assert lexer.leading_verb("'just a string'") is None
    assert lexer.leading_verb("   ") is None


def test_leading_verb_does_not_read_a_verb_out_of_a_literal():
    assert lexer.leading_verb("'drop table t'") is None


# --------------------------------------------------------- cte_body_verb

@pytest.mark.parametrize("sql", [
    "with x as (select 1) select * from x",
    "with a as (select 1), b as (select 2) select * from a, b",
    "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) "
    "SELECT * FROM r",
    'with "Weird Name" as (select 1) select 1',
    "with x (a, b) as (select 1, 2) select a from x",
    "with x as (select 1) /* c */ select * from x",
    "with x /* c */ as (select 1) -- c\n select * from x",
    "with x as (select (1)) select ((1)) from x",
])
def test_cte_body_verb_finds_the_select_after_the_cte_list(sql):
    assert lexer.cte_body_verb(sql) == "SELECT"


@pytest.mark.parametrize("sql, verb", [
    ("with x as (select 1 a) insert into t select a from x", "INSERT"),
    ("with x as (select 1) delete from t", "DELETE"),
    ("with x as (select 1) update t set a = 1", "UPDATE"),
    ("with x as (select 1) merge into t using x on 1=1 when matched then delete",
     "MERGE"),
    ("WITH src AS (SELECT * FROM s) INSERT INTO d.s.t SELECT * FROM src", "INSERT"),
])
def test_cte_body_verb_reports_the_write_after_the_cte_list(sql, verb):
    assert lexer.cte_body_verb(sql) == verb


def test_cte_body_verb_does_not_read_the_verb_out_of_a_literal():
    # `insert` and `;` inside the CTE body are data, and so is a quoted
    # identifier. The body verb is the first keyword at depth 0 after the list.
    assert lexer.cte_body_verb(
        "with x as (select 'insert; delete' as w) select w from x") == "SELECT"
    assert lexer.cte_body_verb(
        'with x as (select 1 as "insert") select * from x') == "SELECT"
    assert lexer.cte_body_verb(
        "with x as (select $$ insert $$ as w) select w from x") == "SELECT"


def test_cte_body_verb_is_none_when_no_keyword_follows_the_cte_list():
    # A parenthesised body is a legitimate read the walker does not follow.
    # None means "could not identify", never "harmless": the guard refuses.
    assert lexer.cte_body_verb("with x as (select 1) (select * from x)") is None
    assert lexer.cte_body_verb("with x as (select 1)") is None
    assert lexer.cte_body_verb("with x as (select 1) 'text'") is None
    assert lexer.cte_body_verb('with x as (select 1) "q"') is None


def test_cte_body_verb_of_a_statement_that_is_not_a_cte_is_none():
    assert lexer.cte_body_verb("select 1") is None
    assert lexer.cte_body_verb("insert into t values (1)") is None
    assert lexer.cte_body_verb("") is None


# ----------------------------------------------------------- quote_ident

def test_quote_ident_wraps_and_doubles_embedded_double_quotes():
    assert lexer.quote_ident("plain") == '"plain"'
    # THE BUG: f'"{name}"' produces "we"ird" -- broken SQL, and the tail of the
    # name lands outside the quotes where it is parsed as code.
    assert lexer.quote_ident('we"ird') == '"we""ird"'


def test_quote_ident_refuses_empty_and_non_string():
    with pytest.raises(ValueError):
        lexer.quote_ident("")
    with pytest.raises(ValueError):
        lexer.quote_ident(None)


def test_qualify_joins_quoted_parts():
    assert lexer.qualify("DB", "SC", "T") == '"DB"."SC"."T"'
    assert lexer.qualify('D"B', "SC") == '"D""B"."SC"'


# ------------------------------------------------------------ like_literal

def test_like_literal_escapes_wildcards():
    # THE BUG: `LIKE 'ORDER_ITEMS'` matches ORDERxITEMS too -- `_` is a
    # single-character wildcard. An existence probe built this way is unsound.
    assert lexer.like_literal("ORDER_ITEMS") == r"ORDER\_ITEMS"
    assert lexer.like_literal("a%b") == r"a\%b"
    assert lexer.like_literal(r"back\slash") == r"back\\slash"


def test_like_literal_escapes_single_quotes():
    assert lexer.like_literal("O'Brien") == "O''Brien"


# -------------------------------------------------------------- sub_code

def test_sub_code_rewrites_code():
    out, n = lexer.sub_code(r"\bIFF\s*\(", "IF(", "select IFF(a,1,2)")
    assert out == "select IF(a,1,2)"
    assert n == 1


def test_sub_code_leaves_a_match_inside_a_literal_alone():
    # THE BUG: a bare re.sub rewrites the contents of a string literal, which
    # silently changes DATA, not just SQL.
    sql = "select 'IFF(' as label, IFF(a,1,2) as v"
    out, n = lexer.sub_code(r"\bIFF\s*\(", "IF(", sql)
    assert out == "select 'IFF(' as label, IF(a,1,2) as v"
    assert n == 1


def test_sub_code_leaves_a_match_inside_a_quoted_identifier_alone():
    sql = 'select 1 as "IFF("'
    assert lexer.sub_code(r"\bIFF\s*\(", "IF(", sql) == (sql, 0)


def test_sub_code_leaves_a_match_inside_a_comment_alone():
    sql = "-- IFF(\nselect 1"
    assert lexer.sub_code(r"\bIFF\s*\(", "IF(", sql) == (sql, 0)


def test_sub_code_accepts_a_callable_replacement():
    out, n = lexer.sub_code(r"DATEADD\((\w+)", lambda m: f"date_add_{m.group(1)}(",
                            "select DATEADD(day, 1, d)")
    assert out == "select date_add_day(, 1, d)"
    assert n == 1


def test_sub_code_refuses_to_rewrite_a_span_that_crosses_a_literal():
    # `IFF'x'(` would match `IFF\s*\(` against the blanked mask. Rewriting it
    # would delete the literal, so the span is skipped instead.
    sql = "select IFF'x'("
    assert lexer.sub_code(r"\bIFF\s*\(", "IF(", sql) == (sql, 0)


def test_find_code_returns_only_matches_in_code():
    sql = "select 'QUALIFY' , QUALIFY x"
    found = lexer.find_code(r"\bQUALIFY\b", sql)
    assert len(found) == 1
    assert sql[found[0][0]:found[0][1]] == "QUALIFY"


def test_sub_code_anchor_group_allows_a_literal_operand_in_the_span():
    # LISTAGG(x, ',') legitimately CONTAINS a literal, and the replacement
    # reproduces it. Only the keyword has to be in code, so the rule anchors
    # on the keyword rather than on the whole span.
    sql = "select LISTAGG(a, ',') from t"
    out, n = lexer.sub_code(r"(?P<kw>\bLISTAGG\s*\()\s*(\w+)\s*,\s*('[^']*')\s*\)",
                            lambda m: f"concat_ws({m.group(3)}, collect_list({m.group(2)}))",
                            sql, anchor_group="kw")
    assert out == "select concat_ws(',', collect_list(a)) from t"
    assert n == 1


def test_sub_code_anchor_group_still_rejects_a_keyword_inside_a_literal():
    sql = "select 'LISTAGG(a, \\',\\')' as label"
    out, n = lexer.sub_code(r"(?P<kw>\bLISTAGG\s*\()", "X(", sql, anchor_group="kw")
    assert (out, n) == (sql, 0)


def test_sub_code_anchor_group_accepts_a_literal_at_the_span_start():
    # 'x'::int -- the operand is a literal, the `::` operator is code.
    sql = "select 'x'::int"
    out, n = lexer.sub_code(r"('[^']*'|\w+)\s*(?P<op>::)\s*(\w+)",
                            lambda m: f"CAST({m.group(1)} AS {m.group(3)})",
                            sql, anchor_group="op")
    assert out == "select CAST('x' AS int)"
    assert n == 1


# ------------------------------------------------------- backtick identifiers

def test_backtick_quoted_identifier_is_its_own_segment():
    # The translator emits Spark backtick identifiers and the DDL generator
    # re-lexes the emitted statement to hand the catalog API its body. While a
    # backtick was code, a `"` inside it opened a phantom identifier and the
    # re-lex raised -- and the catalog API would have received an empty body.
    assert lexer.segments('select `a"b` from t') == [
        ("code", "select "), ("ident", '`a"b`'), ("code", " from t")]


def test_doubled_backtick_inside_identifier_does_not_end_it():
    assert lexer.segments("select `we``ird`") == [
        ("code", "select "), ("ident", "`we``ird`")]


def test_unterminated_backtick_identifier_is_reported():
    with pytest.raises(lexer.UnterminatedLiteral, match="identifier"):
        lexer.segments("select `oops from t")


# ----------------------------------------------------------- sql_literal

@pytest.mark.parametrize("value", ["o'brien", "a\\", "x\\'); drop table t --",
                                   "\\\\'", ""])
def test_sql_literal_stays_one_string_segment(value):
    sql = f"select '{lexer.sql_literal(value)}' X"
    assert [k for k, _ in lexer.segments(sql)] == ["code", "string", "code"]


def test_sql_literal_escapes_the_backslash_before_the_quote():
    assert lexer.sql_literal("a\\'b") == "a\\\\''b"
