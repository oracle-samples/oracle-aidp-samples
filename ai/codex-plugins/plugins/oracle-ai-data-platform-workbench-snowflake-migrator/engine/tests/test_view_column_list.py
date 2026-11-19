"""A view's header column list names its columns; it must not be dropped.

Reproduced with build_ddl_payload and deploy_catalog. GET_DDL
emits a view with its column list:

    create or replace view V_CUST_TOTALS(
        CUSTOMER,
        TOTAL
    ) as select CUST_ID, SUM(AMT) from ORDERS group by 1;

extract_view_body returned only the text after the header AS, and
build_create_view emitted `CREATE VIEW ... AS select CUST_ID, SUM(AMT) ...`
with no list. The target view's columns were CUST_ID and SUM(AMT), while
expected_columns and the catalog API's viewFields said CUSTOMER and TOTAL
(INFORMATION_SCHEMA reads the header). A downstream `select CUSTOMER` broke;
deploy reported the mismatch only after creating the view, and CREATE ... IF
NOT EXISTS never corrects it. Views whose list repeats the body's own names
lost nothing, which is why the live estate never showed it.

The list is now carried, and a list that cannot be read blocks the view
rather than being guessed at. It first went into the SQL as a view column
list, CREATE VIEW <fqn> (c1, c2) AS <body>; live on AIDP such a view is
created and then cannot be read (test_view_column_list_live.py), so the SQL
now names them the way the catalog API's viewText always did:
`CREATE VIEW <fqn> AS SELECT * FROM (<body>) AS named_columns(c1, c2)`.
"""
import pytest

from plan.build import _view_verdict
from snowflake_source.dialect.views import (
    extract_view_body, extract_view_columns,
)
from target.ddl import build_create_view, build_ddl_payload

RENAMING = ("create or replace view V_CUST_TOTALS(\n\tCUSTOMER,\n\tTOTAL\n) as "
            "select CUST_ID, SUM(AMT) from DB.S.ORDERS group by 1;")


def _view(ddl, name="V_CUST_TOTALS"):
    return {"source_identifier": f"DB.S.{name}", "object_type": "VIEW",
            "source_database": "DB", "source_schema": "S",
            "view_ddl_get_ddl": ddl, "source_metadata": {},
            "compatibility_status": "supported",
            "columns": [
                {"COLUMN_NAME": "CUSTOMER", "target_type": "STRING",
                 "ORDINAL_POSITION": 1},
                {"COLUMN_NAME": "TOTAL", "target_type": "DECIMAL(38,0)",
                 "ORDINAL_POSITION": 2}]}


def test_the_header_column_list_is_read():
    assert extract_view_columns(RENAMING) == ["CUSTOMER", "TOTAL"]


def test_a_view_without_a_list_has_none():
    assert extract_view_columns("create view V as select a from t") == []
    # A parenthesised body is not a column list.
    assert extract_view_columns("create view V as (select a from t)") == []


def test_quoted_names_comments_and_policies_are_handled():
    ddl = ('create or replace secure view if not exists DB.S."v x"(\n'
           '\t"Customer Id" COMMENT \'the, customer\',\n'
           '\tTOTAL WITH MASKING POLICY p WITH TAG (t = \'a,b\'),\n'
           '\tlower_case\n) comment=\'x (y)\' as select 1, 2, 3')
    assert extract_view_columns(ddl) == ["Customer Id", "TOTAL", "LOWER_CASE"]


def test_an_unreadable_list_blocks_the_view_in_plan_and_ddl():
    ddl = "create view V( , A) as select 1"
    with pytest.raises(ValueError, match="column list"):
        extract_view_body(ddl)
    ok, category, reason = _view_verdict(_view(ddl))
    assert (ok, category) == (False, "unparseable_sql")
    assert "column list" in reason
    assert build_create_view(_view(ddl), "lake.db_s.v").blocked is True


def test_the_create_view_names_the_columns():
    res = build_create_view(_view(RENAMING), "lake.db_s.v_cust_totals")
    assert res.blocked is False, res.blocked_reason
    # Aliased in the body, not a view column list (unreadable on AIDP).
    assert res.sql.startswith(
        "CREATE VIEW IF NOT EXISTS `lake`.`db_s`.`v_cust_totals` "
        "AS SELECT * FROM (\n"), res.sql
    assert res.sql.endswith(
        ") AS named_columns(`CUSTOMER`, `TOTAL`)"), res.sql


def test_sql_and_view_text_both_expose_the_header_names():
    inv = {"inventory": [_view(RENAMING)]}
    plan = {"target_names": {"DB.S.V_CUST_TOTALS": "lake.db_s.v_cust_totals"},
            "waves": [["DB.S.V_CUST_TOTALS"]],
            "clone_targets": ["DB.S.V_CUST_TOTALS"]}
    (stmt,) = build_ddl_payload(inv, plan)["statements"]
    assert "named_columns(`CUSTOMER`, `TOTAL`)" in stmt["sql"]
    assert stmt["view_text"] == (
        "SELECT * FROM (\nselect CUST_ID, SUM(AMT) from DB.S.ORDERS "
        "group by 1\n) AS named_columns(`CUSTOMER`, `TOTAL`)"), stmt["view_text"]
    assert [c["name"] for c in stmt["expected_columns"]] == [
        "CUSTOMER", "TOTAL"]


def test_generated_view_sql_with_a_list_still_yields_its_body():
    """The generated SQL carries no header list any more; its body is the
    aliased query, which still starts from the source body."""
    res = build_create_view(_view(RENAMING), "lake.db_s.v_cust_totals")
    assert extract_view_body(res.sql).startswith(
        "SELECT * FROM (\nselect CUST_ID")
    assert extract_view_columns(res.sql) == []


def test_an_older_plan_with_a_header_list_still_yields_named_view_text():
    """A ddl_plan.json written before the change still carries the header
    list; its catalog viewText is the same aliased query."""
    from target.ddl import _view_text
    old = ("CREATE VIEW IF NOT EXISTS `lake`.`db_s`.`v_cust_totals` "
           "(`CUSTOMER`, `TOTAL`) AS\nselect CUST_ID, SUM(AMT) from "
           "DB.S.ORDERS group by 1")
    assert _view_text(old) == (
        "SELECT * FROM (\nselect CUST_ID, SUM(AMT) from DB.S.ORDERS "
        "group by 1\n) AS named_columns(`CUSTOMER`, `TOTAL`)")


def test_the_emitted_sql_and_view_text_parse_as_spark():
    sqlglot = pytest.importorskip("sqlglot")
    inv = {"inventory": [_view(RENAMING)]}
    plan = {"target_names": {"DB.S.V_CUST_TOTALS": "lake.db_s.v_cust_totals"},
            "waves": [["DB.S.V_CUST_TOTALS"]],
            "clone_targets": ["DB.S.V_CUST_TOTALS"]}
    (stmt,) = build_ddl_payload(inv, plan)["statements"]
    assert sqlglot.parse_one(stmt["sql"], dialect="spark")
    assert sqlglot.parse_one(stmt["view_text"], dialect="spark")
