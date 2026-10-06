"""Snowflake `//` line comments are comments, not code.

Reproduced through the real entry points. Snowflake accepts
two line-comment markers, `--` and `//`; the lexer knew only `--`. So in

    // don't touch

the apostrophe opened a string literal. With nothing to close it the scan
raised UnterminatedLiteral -- `assess` exited 1 printing only "unterminated
string starting at offset 43", naming no object and writing no artifact,
over a task body whose only fault was a comment. With a second apostrophe
further on, the phantom literal swallowed the code between them:

  * a view with QUALIFY between two such comments was planned can_migrate
    and stamped R42 "portable" -- the one construct that had to block it was
    inside the phantom string -- and failed at deploy with a bare 500
  * a view with ONE such comment was refused as "unparseable_sql:
    unterminated string"
  * `select 1 // it's\\n; drop table t` read as one SELECT to the read-only
    guard (no engine path sends such SQL, but the guard is the guarantee)
  * a `//` comment mentioning `insert into X` was counted as a census
    writes= edge

`//` is Snowflake-only: Spark SQL has no such comment, so a view body that
carries one would fail to parse on the target. The translator rewrites each
`//` comment to `--` (T21_SLASH_COMMENT), which is the same comment.
"""
import pytest

from plan.build import _view_verdict
from snowflake_source import conn
from snowflake_source.dialect import lexer
from snowflake_source.dialect.translate import translate_sql
from snowflake_source.extract.census import written_tables
from target.ddl import build_create_view


def _view(ddl):
    return {"source_identifier": "DB.S.V", "object_type": "VIEW",
            "source_database": "DB", "source_schema": "S",
            "view_ddl_get_ddl": ddl, "source_metadata": {}, "columns": [],
            "compatibility_status": "supported"}


# ---------------------------------------------------------------- lexer

def test_a_slash_comment_is_a_comment_segment():
    assert lexer.segments("select 1 // don't\nfrom t") == [
        ("code", "select 1 "), ("comment", "// don't"), ("code", "\nfrom t")]


def test_a_slash_comment_at_the_end_of_input_runs_to_the_end():
    assert lexer.segments("select 1 // it's") == [
        ("code", "select 1 "), ("comment", "// it's")]


def test_a_slash_inside_a_literal_or_another_comment_is_not_a_comment():
    assert lexer.segments("select 'http://x' // c") == [
        ("code", "select "), ("string", "'http://x'"), ("code", " "),
        ("comment", "// c")]
    assert [k for k, _ in lexer.segments("select 1 -- a // b\n")] == [
        "code", "comment", "code"]


def test_single_slash_division_is_still_code():
    assert lexer.segments("select a / b from t") == [
        ("code", "select a / b from t")]


def test_the_read_only_guard_sees_the_statement_after_a_slash_comment():
    with pytest.raises(conn.SourceWriteRefused, match="DROP"):
        conn.assert_read_only("select 1 // it's\n; drop table t")
    conn.assert_read_only("select 1 // it's fine\n")


# ------------------------------------------------------------ pipe / task

def test_a_task_body_with_a_slash_comment_apostrophe_is_readable():
    assert written_tables("insert into T select 1 // don't", "DB", "S") == [
        "DB.S.T"]


def test_a_write_verb_inside_a_slash_comment_is_not_a_write():
    assert written_tables(
        "select 1 // later: insert into AUDIT_LOG\n", "DB", "S") == []


# ---------------------------------------------------------------- views

QUALIFY_BETWEEN = (
    "create view V as select * from DB.S.T // the customer's latest row\n"
    "qualify row_number() over (partition by id order by ts desc) = 1\n"
    "// don't remove this\n")


def test_qualify_between_two_slash_comments_is_detected():
    ok, category, reason = _view_verdict(_view(QUALIFY_BETWEEN))
    assert ok is False
    assert category == "snowflake_only_sql"
    assert "QUALIFY" in reason


def test_qualify_between_two_slash_comments_blocks_the_ddl():
    res = build_create_view(_view(QUALIFY_BETWEEN), "lake.db_s.v")
    assert res.blocked is True and "QUALIFY" in res.blocked_reason


def test_one_slash_comment_with_an_apostrophe_is_not_unparseable():
    ddl = "create view V as select a // today's rows\nfrom DB.S.T"
    assert _view_verdict(_view(ddl)) == (True, "", "")


def test_the_emitted_view_carries_no_slash_comment():
    ddl = "create view V as select a // today's rows\nfrom DB.S.T"
    res = build_create_view(_view(ddl), "lake.db_s.v")
    assert res.blocked is False, res.blocked_reason
    assert "//" not in res.sql
    assert "-- today's rows\nfrom DB.S.T" in res.sql, res.sql
    assert "T21_SLASH_COMMENT" in [r.rule_id for r in res.rules_applied]


def test_translation_rewrites_only_the_comment_marker():
    r = translate_sql("select 'a//b' as x // note\nfrom t -- keep // this\n")
    assert r.sql == "select 'a//b' as x -- note\nfrom t -- keep // this\n"
    assert [a["rule_id"] for a in r.applied] == ["T21_SLASH_COMMENT"]
    assert not r.unsupported
