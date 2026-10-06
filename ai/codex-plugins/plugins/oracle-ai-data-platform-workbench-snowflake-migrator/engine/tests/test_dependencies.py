"""Dependency extraction: ACCOUNT_USAGE preferred, parsed DDL as fallback."""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.dependencies import (
    extract_dependencies, parse_view_references,
)

# The real view from the test estate: 4 fully-qualified names, 3 LEFT JOINs.
REAL_VIEW = """
create or replace view ACME_ORDER_360_VW(ORDER_ID, ITEM_COUNT) as
  SELECT o.ORDER_ID, COUNT(i.ORDER_ITEM_ID) AS ITEM_COUNT
  FROM SNOWMIG_TESTDB.PUBLIC.ORDER_DIMENSIONS o
  LEFT JOIN SNOWMIG_TESTDB.PUBLIC.CUSTOMER_DIMENSIONS c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
  LEFT JOIN SNOWMIG_TESTDB.PUBLIC.STORE_DIMENSIONS s
    ON o.STORE_ID = s.STORE_ID
  LEFT JOIN SNOWMIG_TESTDB.PUBLIC.ORDER_ITEMS_FACT i
    ON o.ORDER_ID = i.ORDER_ID
  GROUP BY o.ORDER_ID;
"""


def test_parses_all_four_qualified_references():
    got = parse_view_references(REAL_VIEW, default_db="D", default_schema="S")
    assert got == [
        "SNOWMIG_TESTDB.PUBLIC.CUSTOMER_DIMENSIONS",
        "SNOWMIG_TESTDB.PUBLIC.ORDER_DIMENSIONS",
        "SNOWMIG_TESTDB.PUBLIC.ORDER_ITEMS_FACT",
        "SNOWMIG_TESTDB.PUBLIC.STORE_DIMENSIONS",
    ]


def test_bare_name_qualified_with_defaults():
    assert parse_view_references("select * from orders",
                                 default_db="D", default_schema="S") == ["D.S.ORDERS"]


def test_two_part_name_qualified_with_default_db():
    assert parse_view_references("select * from sales.orders",
                                 default_db="D", default_schema="S") == ["D.SALES.ORDERS"]


def test_subquery_after_from_is_not_a_reference():
    got = parse_view_references("select * from (select 1) t",
                                default_db="D", default_schema="S")
    assert got == []


def test_duplicate_references_deduplicated():
    sql = "select * from D.S.A join D.S.A b on 1=1"
    assert parse_view_references(sql, default_db="D", default_schema="S") == ["D.S.A"]


def test_account_usage_is_preferred_when_readable():
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from D.S.T"},
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"}]}
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "D.S.V", "REFERENCED": "D.S.T",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    out = extract_dependencies(run, inv)
    assert out["source_used"] == "account_usage"
    assert out["edges"] == [{"from": "D.S.V", "to": "D.S.T",
                             "kind": "VIEW->TABLE", "source": "account_usage"}]


def test_falls_back_to_parsed_ddl_when_account_usage_denied():
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from D.S.T"},
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "object_dependencies" in sql.lower():
                raise RuntimeError("Object does not exist or not authorized")
            return super().__call__(sql, params)

    out = extract_dependencies(Denied({}), inv)
    assert out["source_used"] == "parsed_ddl"
    assert out["edges"][0]["source"] == "parsed_ddl"
    assert "not authorized" in out["coverage_note"]


def test_fallback_keeps_a_reference_outside_the_inventory_as_a_marked_edge():
    # A reference we never inventoried cannot be planned. It used to be
    # dropped, which hid it from the plan altogether; it is now an edge the
    # planner can refuse the view on, marked so it is never mistaken for a
    # node to wave (compute_waves ignores an edge whose end is not a node).
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "select * from OTHER.X.Y"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    out = extract_dependencies(Denied({}), inv)
    assert out["edges"] == [{"from": "D.S.V", "to": "OTHER.X.Y",
                             "kind": "VIEW->OBJECT", "source": "parsed_ddl",
                             "outside_inventory": True}]
    assert any("OTHER.X.Y" in n for n in out["unresolved_references"])


def test_tables_produce_no_edges_in_fallback_mode():
    inv = {"inventory": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                          "source_database": "D", "source_schema": "S"}]}

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    assert extract_dependencies(Denied({}), inv)["edges"] == []


# --- quoted, case-sensitive identifiers ------------------------------------
#
# Snowflake keeps a quoted identifier's case and treats it as case-SENSITIVE,
# so a view created as "SalesView" over "Orders" is inventoried as
# DB.S.SalesView / DB.S.Orders, and that exact spelling is the plan's node.
# An edge whose endpoints are spelled any other way is silently dropped by
# the wave computation, and the view is then created before its base table.

def _mixed_case_inventory():
    return {"inventory": [
        {"source_identifier": "DB.S.Orders", "object_type": "TABLE",
         "source_database": "DB", "source_schema": "S",
         "row_count_exact": 1000},
        {"source_identifier": "DB.S.SalesView", "object_type": "VIEW",
         "source_database": "DB", "source_schema": "S",
         "view_ddl_get_ddl": 'create view "SalesView" as '
                             'select * from "DB"."S"."Orders"'},
        {"source_identifier": "DB.S.PLAIN", "object_type": "TABLE",
         "source_database": "DB", "source_schema": "S",
         "row_count_exact": 10},
        {"source_identifier": "DB.S.PLAIN_V", "object_type": "VIEW",
         "source_database": "DB", "source_schema": "S",
         "view_ddl_get_ddl": "create view PLAIN_V as select * from DB.S.PLAIN"}]}


def test_account_usage_keeps_edges_for_quoted_mixed_case_objects():
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "DB.S.SalesView", "REFERENCED": "DB.S.Orders",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
        {"REFERENCING": "DB.S.PLAIN_V", "REFERENCED": "DB.S.PLAIN",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    out = extract_dependencies(run, _mixed_case_inventory())
    assert out["source_used"] == "account_usage"
    assert sorted((e["from"], e["to"]) for e in out["edges"]) == [
        ("DB.S.PLAIN_V", "DB.S.PLAIN"), ("DB.S.SalesView", "DB.S.Orders")]


def test_parsed_ddl_fallback_emits_exact_inventory_identifiers():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            raise RuntimeError("not authorized")

    out = extract_dependencies(Denied({}), _mixed_case_inventory())
    assert out["source_used"] == "parsed_ddl"
    assert sorted((e["from"], e["to"]) for e in out["edges"]) == [
        ("DB.S.PLAIN_V", "DB.S.PLAIN"), ("DB.S.SalesView", "DB.S.Orders")], \
        "endpoints are the inventory's exact spelling, not an upper-cased copy"
    assert out["unresolved_references"] == []


def test_a_mixed_case_view_lands_in_a_later_wave_than_its_base_table():
    # End to end through the planner: the edge has to survive into the waves.
    from plan.build import build_plan
    inv = _mixed_case_inventory()
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "DB.S.SalesView", "REFERENCED": "DB.S.Orders",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    plan = build_plan(inv, extract_dependencies(run, inv))
    wave_of = {n: i for i, wave in enumerate(plan["waves"]) for n in wave}
    assert wave_of["DB.S.Orders"] < wave_of["DB.S.SalesView"], plan["waves"]


# --- readable but not (yet) populated ---------------------------------------
#
# OBJECT_DEPENDENCIES lags DDL by up to ~3 hours. A view created or altered
# shortly before the run -- the normal state of a freshly prepared estate --
# has no row there yet, and the query succeeds with nothing in it. That must
# not be presented as authoritative lineage: the DDL already captured for
# the view is parsed instead, and the provenance says what happened.

def _chain_inventory():
    return {"inventory": [
        {"source_identifier": "DB.S.T_BASE", "object_type": "TABLE",
         "source_database": "DB", "source_schema": "S"},
        {"source_identifier": "DB.S.V_LEVEL1", "object_type": "VIEW",
         "source_database": "DB", "source_schema": "S",
         "view_ddl_get_ddl": "create view V_LEVEL1 as select * from DB.S.T_BASE"},
        {"source_identifier": "DB.S.A_TOP", "object_type": "VIEW",
         "source_database": "DB", "source_schema": "S",
         "view_ddl_get_ddl": "create view A_TOP as select * from DB.S.V_LEVEL1"}]}


def test_readable_but_empty_account_usage_parses_view_ddl():
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "create view V as select * from D.S.T"},
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"}]}
    out = extract_dependencies(FakeSql({"object_dependencies": []}), inv)
    assert out["source_used"] == "account_usage_empty"
    assert out["edges"] == [{"from": "D.S.V", "to": "D.S.T",
                             "kind": "VIEW->OBJECT", "source": "parsed_ddl"}]
    assert out["views_without_account_usage_edge"] == ["D.S.V"]
    note = out["coverage_note"].lower()
    assert "lag" in note and "not authoritative" in note
    assert out["warning"] and "no account_usage lineage edge" in out["warning"].lower()


def test_partial_account_usage_merges_only_uncovered_views():
    from plan.waves import compute_waves
    inv = _chain_inventory()
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "DB.S.V_LEVEL1", "REFERENCED": "DB.S.T_BASE",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    out = extract_dependencies(run, inv)
    assert out["source_used"] == "account_usage+parsed_ddl"
    by_from = {e["from"]: e for e in out["edges"]}
    assert len(out["edges"]) == 2
    assert by_from["DB.S.V_LEVEL1"]["source"] == "account_usage"
    assert by_from["DB.S.A_TOP"]["source"] == "parsed_ddl"
    assert by_from["DB.S.A_TOP"]["to"] == "DB.S.V_LEVEL1"
    assert out["views_without_account_usage_edge"] == ["DB.S.A_TOP"]
    nodes = [r["source_identifier"] for r in inv["inventory"]]
    assert compute_waves(nodes, out["edges"])["waves"] == [
        ["DB.S.T_BASE"], ["DB.S.V_LEVEL1"], ["DB.S.A_TOP"]]


def test_fully_covered_account_usage_is_unchanged():
    inv = _chain_inventory()
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "DB.S.V_LEVEL1", "REFERENCED": "DB.S.T_BASE",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
        {"REFERENCING": "DB.S.A_TOP", "REFERENCED": "DB.S.V_LEVEL1",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "VIEW"}]})
    out = extract_dependencies(run, inv)
    assert out["source_used"] == "account_usage"
    assert "authoritative" in out["coverage_note"]
    assert all(e["source"] == "account_usage" for e in out["edges"])
    assert not out.get("views_without_account_usage_edge")
    assert out["warning"] is None


def test_readable_empty_with_tables_only_stays_account_usage():
    # No views, so there is nothing to lag and nothing to parse.
    inv = {"inventory": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                          "source_database": "D", "source_schema": "S"}]}
    out = extract_dependencies(FakeSql({"object_dependencies": []}), inv)
    assert out["source_used"] == "account_usage"
    assert out["edges"] == []
    assert out["warning"] is None


def test_a_view_with_no_edge_from_either_source_is_named_in_the_warning():
    # The manifest path carries no view SQL, so there is nothing to parse:
    # the view is ordered by size only, and the warning has to say so rather
    # than let the plan imply it was ordered.
    inv = {"inventory": [
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": None, "view_text_show": None},
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"}]}
    out = extract_dependencies(FakeSql({"object_dependencies": []}), inv)
    assert out["source_used"] == "account_usage_empty"
    assert out["edges"] == []
    assert "D.S.V" in out["warning"]
    assert "size only" in out["warning"]
