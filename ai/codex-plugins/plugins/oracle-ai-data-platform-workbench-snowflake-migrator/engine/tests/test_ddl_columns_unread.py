"""A table whose columns could not be READ is not a privilege problem.

The DDL half. When the
INFORMATION_SCHEMA.COLUMNS read for a schema fails, every table in it has no
columns. build_create_table blocked each one with "table has no columns
visible to this role (Delta-shared or insufficient privilege)" -- a guess
that sends the operator after grants when the real cause was, say, a
statement timeout. The extractor now records the failure on the record
(`columns_read: "failed"`, `columns_read_error: <text>`), and the block
reason quotes it. Without that field the old wording stands: an empty,
successfully read column list is what a share or a missing grant looks like.
"""
from target.ddl import build_create_table


def _rec(**over):
    r = {"source_identifier": "DB.S.T", "object_type": "TABLE",
         "source_metadata": {}, "columns": [],
         "compatibility_status": "supported"}
    r.update(over)
    return r


def test_a_failed_columns_read_is_quoted_not_guessed():
    res = build_create_table(_rec(
        compatibility_status="unassessed", columns_read="failed",
        columns_read_error="SQL execution canceled: statement timeout"),
        "lake.db_s.t")
    assert res.blocked is True
    assert "statement timeout" in res.blocked_reason, res.blocked_reason
    assert "privilege" not in res.blocked_reason


def test_an_empty_list_that_was_read_keeps_the_visibility_reason():
    res = build_create_table(_rec(columns_read="ok"), "lake.db_s.t")
    assert res.blocked is True
    assert "visible to this role" in res.blocked_reason
