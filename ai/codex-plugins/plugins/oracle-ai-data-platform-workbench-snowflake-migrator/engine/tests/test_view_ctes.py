"""A CTE name is not a table, even when a table in the view's schema has it.

Reproduced with the real build_create_view. The positional
rewrite qualifies every bare name after FROM or JOIN that matches a mapped
object in the view's own schema -- which is right for `from ORDERS`, and
wrong when ORDERS is the view's own CTE. The dbt-compiled idiom

    with ORDERS as (select * from ORDERS where STATUS = 'OPEN')
    select count(*) from ORDERS

became `... select count(*) from lake.db_sales.orders`: the view created,
returned the UNFILTERED base table, and DDL_PLAN said R41 "rewrote object
references: ORDERS -> lake.db_sales.orders" as though that were correct.
Source names are upper-case and target names lower-case, so the rewrite
fires in the default configuration. A CTE whose name matches no table
(`recent`) got a false R42 "unresolved reference ... create only once those
objects exist" warning instead.

A CTE name is in scope from the end of its own body to the end of the
statement (or of the parenthesis the WITH sits in); inside its own body a
non-recursive CTE's name still means the table, so that one IS rewritten.
"""
from snowflake_source.dialect import lexer
from target.ddl import build_create_view


def _view_rec(sql, ident="DB.SALES.V1"):
    db, schema, _ = ident.split(".")
    return {"source_identifier": ident, "object_type": "VIEW",
            "source_database": db, "source_schema": schema,
            "source_metadata": {}, "view_ddl_get_ddl": sql, "columns": []}


_MAP = {"DB.SALES.V1": "lake.db_sales.v1",
        "DB.SALES.ORDERS": "lake.db_sales.orders",
        "DB.SALES.STG_ORDERS": "lake.db_sales.stg_orders"}


def _rules(res):
    return {r.rule_id: r.detail for r in res.rules_applied}


def test_a_cte_named_like_a_table_is_not_rewritten_to_the_table():
    res = build_create_view(_view_rec(
        "create view V1 as WITH ORDERS AS (SELECT * FROM STG_ORDERS "
        "WHERE NOT IS_DELETED) SELECT COUNT(*) FROM ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert not res.blocked, res.blocked_reason
    assert "FROM lake.db_sales.stg_orders" in res.sql, res.sql
    assert res.sql.endswith("SELECT COUNT(*) FROM ORDERS"), res.sql
    assert _rules(res)["R41_VIEW_REFS_REWRITTEN"] == (
        "rewrote object references: STG_ORDERS -> lake.db_sales.stg_orders")


def test_a_self_shadowing_cte_body_still_reads_the_table():
    # Inside its own (non-recursive) body the name is the table; after the
    # body it is the CTE. Only the inner reference is the table's.
    res = build_create_view(_view_rec(
        "create view V1 as with ORDERS as (select * from ORDERS where "
        "STATUS = 'OPEN') select count(*) as N from ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert "(select * from lake.db_sales.orders where" in res.sql, res.sql
    assert res.sql.endswith("select count(*) as N from ORDERS"), res.sql


def test_a_join_to_a_cte_keeps_the_ctes_filter():
    res = build_create_view(_view_rec(
        "create view V1 as with STG_ORDERS as (select * from STG_ORDERS "
        "where active), o as (select 1 as A) "
        "select * from o join STG_ORDERS s on o.A = s.A"),
        "lake.db_sales.v1", _MAP)
    assert "join STG_ORDERS s" in res.sql, res.sql
    assert "(select * from lake.db_sales.stg_orders where active)" in res.sql


def test_a_cte_matching_no_table_is_not_an_unresolved_reference():
    res = build_create_view(_view_rec(
        "create view V1 as with recent as (select * from ORDERS) "
        "select * from recent"),
        "lake.db_sales.v1", _MAP)
    assert "R42_VIEW_REFS_UNRESOLVED" not in _rules(res), res.rules_applied
    assert not [w for w in res.warnings if "unqualified" in w.lower()]
    assert "from lake.db_sales.orders" in res.sql


def test_a_later_cte_sees_an_earlier_one():
    res = build_create_view(_view_rec(
        "create view V1 as with a as (select 1 as X), "
        "b as (select * from a) select * from b"),
        "lake.db_sales.v1", _MAP)
    assert "R42_VIEW_REFS_UNRESOLVED" not in _rules(res), res.rules_applied


def test_a_nested_with_is_scoped_to_its_parenthesis():
    # Outside the subquery the name is free again: it is the table.
    res = build_create_view(_view_rec(
        "create view V1 as select * from (with ORDERS as (select 1 as A) "
        "select * from ORDERS) q join ORDERS o on q.A = o.A"),
        "lake.db_sales.v1", _MAP)
    assert "select * from ORDERS) q" in res.sql, res.sql
    assert "join lake.db_sales.orders o" in res.sql, res.sql


def test_a_quoted_cte_name_is_exact_case():
    # "orders" and ORDERS are different names in Snowflake, so a quoted
    # lower-case CTE does not shadow an unquoted reference.
    res = build_create_view(_view_rec(
        'create view V1 as with "orders" as (select 1 as A) '
        'select * from "orders" join ORDERS o on 1 = 1'),
        "lake.db_sales.v1", _MAP)
    assert "from `orders`" in res.sql, res.sql
    assert "join lake.db_sales.orders o" in res.sql, res.sql


def test_cte_scopes_reports_names_and_where_they_apply():
    sql = "with recursive r (n) as (select 1) select * from r"
    (name, start, end), = lexer.cte_scopes(sql)
    assert name == "R"
    assert start <= sql.index("(select 1)"), "recursive: visible in its body"
    assert end == len(sql)


def test_with_that_is_not_a_cte_list_declares_nothing():
    assert lexer.cte_scopes(
        "select cast(a as timestamp with time zone) from t") == []
