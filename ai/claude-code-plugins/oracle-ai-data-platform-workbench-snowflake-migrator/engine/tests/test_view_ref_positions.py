"""Where a view body names a table -- and where a FROM is not a FROM clause.

The view-reference passes read any `FROM|JOIN <a>.<b>` as a table reference.
That matched `EXTRACT(YEAR FROM o.ORDER_DATE)`, `TRIM(BOTH ' ' FROM c.NAME)`
and `a IS DISTINCT FROM c.STATUS` -- columns, which the lineage scanner has
always ignored -- so ddl_plan.json warned that ordinary views referenced
objects "not part of this migration" and that "The CREATE VIEW will fail".
Nothing had established that.

Two more gaps in the same passes:
  * a quoted in-migration reference, `"COMMERCE"."Orders"`, was looked up
    upper-cased (ORDERS) against a key stored upper-cased, while the lookup
    for a quoted part used its exact case -- so it never matched, and was
    blamed on the migration's scope;
  * nothing read past the comma of a FROM list: in `from ORDERS o,
    CUSTOMERS c` only ORDERS was qualified, CUSTOMERS stayed bare with no
    warning, and S10 now runs that SQL -- it fails, or binds to a same-named
    table in the session's schema.

And the unresolved-reference rule shared its id, R44, with the column-list
rule; it is R45_VIEW_REFS_UNRESOLVED now.
"""
import pytest

from target.ddl import build_create_view

DB = "TEST_DB"
NAME_MAP = {
    f"{DB}.COMMERCE.ORDERS": "cat.test_db_commerce.orders",
    f"{DB}.COMMERCE.Orders": "cat.test_db_commerce.orders_quoted",
    f"{DB}.COMMERCE.CUSTOMERS": "cat.test_db_commerce.customers",
    f"{DB}.SALES.ORDERS": "cat.test_db_sales.orders",
    f"{DB}.SALES.CUSTOMERS": "cat.test_db_sales.customers",
}


def _build(body, schema="SALES", name_map=NAME_MAP):
    record = {"source_identifier": f"{DB}.{schema}.V", "object_type": "VIEW",
              "source_database": DB, "source_schema": schema,
              "view_ddl_get_ddl": f"create or replace view V as {body};",
              "columns": []}
    return build_create_view(record, f"cat.test_db_{schema.lower()}.v",
                             name_map)


def _ids(res):
    return [r.rule_id for r in res.rules_applied]


def _unresolved_warnings(res):
    return [w for w in res.warnings
            if "unresolved" in w.lower() or "unqualified" in w.lower()]


@pytest.mark.parametrize("expr", [
    "EXTRACT(YEAR FROM o.ORDER_DATE)",
    "EXTRACT(YEAR FROM ORDER_DATE)",
    "TRIM(BOTH ' ' FROM o.NOTE)",
    "TRIM(LEADING FROM NOTE)",
    "SUBSTRING(o.NOTE FROM 2 FOR 3)",
    "o.STATUS IS DISTINCT FROM o.PRIOR_STATUS",
    "o.STATUS IS NOT DISTINCT FROM PRIOR_STATUS",
])
def test_a_from_inside_an_expression_is_not_a_table_reference(expr):
    res = _build(f"select {expr} as X from ORDERS o")
    assert "from cat.test_db_sales.orders o" in res.sql
    assert not _unresolved_warnings(res), res.warnings
    assert "R42_VIEW_REFS_UNRESOLVED" not in _ids(res)
    assert "R45_VIEW_REFS_UNRESOLVED" not in _ids(res)
    assert "R44_VIEW_REFS_UNRESOLVED" not in _ids(res)
    # The column is left exactly as written.
    assert expr.split("(")[-1].split()[-1].rstrip(")") in res.sql


def test_a_quoted_in_migration_reference_is_qualified():
    res = _build('select * from "COMMERCE"."Orders" x')
    assert "cat.test_db_commerce.orders_quoted x" in res.sql
    assert not _unresolved_warnings(res), res.warnings
    assert "R41_VIEW_REFS_REWRITTEN" in _ids(res)


def test_an_unquoted_reference_does_not_match_a_quoted_mixed_case_one():
    # Unquoted COMMERCE.orders folds to ORDERS, not to "Orders".
    res = _build("select * from COMMERCE.orders x")
    assert "cat.test_db_commerce.orders x" in res.sql


def test_every_item_of_a_comma_from_list_is_qualified():
    res = _build("select o.ID, c.NAME from ORDERS o, CUSTOMERS c "
                 "where c.ID = o.CUST_ID")
    assert "from cat.test_db_sales.orders o, cat.test_db_sales.customers c" \
        in res.sql, res.sql
    assert not _unresolved_warnings(res), res.warnings


def test_a_two_part_item_after_a_comma_is_qualified():
    res = _build("select 1 from ORDERS o, COMMERCE.CUSTOMERS c")
    assert "cat.test_db_commerce.customers c" in res.sql, res.sql


def test_an_unmapped_comma_item_is_named_not_silently_left():
    name_map = {k: v for k, v in NAME_MAP.items()
                if not k.endswith("SALES.CUSTOMERS")}
    res = _build("select 1 from ORDERS o, CUSTOMERS c", name_map=name_map)
    assert "from cat.test_db_sales.orders o, CUSTOMERS c" in res.sql
    assert "R42_VIEW_REFS_UNRESOLVED" in _ids(res)
    assert "R45_VIEW_REFS_UNRESOLVED" in _ids(res)
    assert any("CUSTOMERS" in w for w in _unresolved_warnings(res)), \
        res.warnings


def test_a_subquery_in_a_from_list_does_not_hide_the_next_item():
    res = _build("select 1 from (select ID from ORDERS) s, CUSTOMERS c")
    assert "from cat.test_db_sales.orders)" in res.sql, res.sql
    assert "cat.test_db_sales.customers c" in res.sql, res.sql


def test_a_table_function_after_a_comma_is_not_a_table():
    res = _build("select 1 from ORDERS o, LATERAL FLATTEN(input => o.TAGS) f")
    assert not any("FLATTEN" in w or "LATERAL" in w
                   for w in _unresolved_warnings(res)), res.warnings


def test_the_unresolved_rule_no_longer_shares_r44_with_the_column_list():
    res = _build("select * from COMMERCE.NOT_MIGRATING")
    assert "R45_VIEW_REFS_UNRESOLVED" in _ids(res)
    assert "R44_VIEW_REFS_UNRESOLVED" not in _ids(res)
