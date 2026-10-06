"""Parsed-DDL lineage reads the code, and every relation of a FROM list.

Reproduced with the real extract_dependencies / build_plan.
The parsed_ddl path is what runs when OBJECT_DEPENDENCIES is denied (a
silent fallback) and for every view ACCOUNT_USAGE has not caught up with
yet. It ran `\\b(?:FROM|JOIN)\\s+<ident>` over the raw DDL, so:

  * it captured only the FIRST relation after FROM. A view over
    `ORDERS o, Z_CUST c` had no edge to the view Z_CUST, landed in the same
    wave and was emitted BEFORE it -- while PLANNED_OBJECTS asserted "views
    follow their base tables"
  * it matched inside comments, string literals and the GET_DDL
    `COMMENT='...'` header. A comment saying "from V_B" on V_A, with V_B
    really reading V_A, fabricated a V_A <-> V_B cycle, and both valid views
    were dropped from the DDL as "not emitted: dependency cycle"

The scan now runs over lexer.code_only() and reads names from the raw text
at those positions (as census.written_tables does), and keeps consuming
`, <relation> [alias]` after a FROM target until a keyword. A bare name
that is one of the view's own CTEs is the CTE, not a table.
"""
from fake_sql import FakeSql
from plan.build import build_plan
from snowflake_source.extract.dependencies import (
    extract_dependencies, parse_view_references,
)


def _refs(sql):
    return parse_view_references(sql, default_db="DB", default_schema="S")


class _Denied(FakeSql):
    def __call__(self, sql, params=None):
        if "object_dependencies" in sql.lower():
            raise RuntimeError("Object does not exist or not authorized")
        return super().__call__(sql, params)


def _table(name, rows=10):
    return {"source_identifier": f"DB.S.{name}", "object_type": "TABLE",
            "source_database": "DB", "source_schema": "S",
            "row_count_exact": rows}


def _view(name, ddl):
    return {"source_identifier": f"DB.S.{name}", "object_type": "VIEW",
            "source_database": "DB", "source_schema": "S",
            "view_ddl_get_ddl": ddl}


# ------------------------------------------------------------- FROM lists

def test_every_comma_joined_relation_is_a_reference():
    assert _refs("select * from A a, B b") == ["DB.S.A", "DB.S.B"]


def test_comma_list_with_as_aliases_qualified_and_quoted_names():
    assert _refs('select * from X.Y.A as a, S2.B, "Mixed" m where 1 = 1') == [
        "DB.S.MIXED", "DB.S2.B", "X.Y.A"]


def test_a_subquery_in_a_from_list_does_not_end_the_list():
    assert _refs("select * from (select 1 as n) q, B where q.n = 1") == [
        "DB.S.B"]


def test_the_list_ends_at_a_keyword():
    assert _refs("select * from A where x in (1, 2) group by a, b") == [
        "DB.S.A"]


# ------------------------------------------------- not code, not a reference

def test_a_from_inside_a_comment_is_not_a_reference():
    assert _refs("select * from A -- copied from B\n") == ["DB.S.A"]
    assert _refs("select * /* from B */ from A // from C\n") == ["DB.S.A"]


def test_a_from_inside_a_string_literal_is_not_a_reference():
    assert _refs("select 'from B' as note from A") == ["DB.S.A"]


def test_the_get_ddl_comment_header_is_not_a_reference():
    ddl = ("create or replace view V comment='rolled up from V_B nightly' "
           "as select * from A;")
    assert _refs(ddl) == ["DB.S.A"]


def test_a_from_that_is_not_a_from_clause_names_no_table():
    # `IS DISTINCT FROM` and `EXTRACT(part FROM expr)` take an expression.
    assert _refs("select extract(year from ORDER_TS) as y, "
                 "a is distinct from B as d from A") == ["DB.S.A"]


def test_a_table_function_in_a_from_list_is_not_a_table():
    assert _refs("select * from A, table(flatten(input => A.V)) f") == [
        "DB.S.A"]


def test_a_cte_name_is_not_a_table_reference():
    assert _refs("with recent as (select * from A) select * from recent") == [
        "DB.S.A"]


# ------------------------------------------------------- through the plan

def test_a_comma_joined_view_is_waved_after_the_view_it_reads():
    inv = {"inventory": [
        _table("ORDERS", 1000), _table("CUST", 10),
        _view("Z_CUST", "create view Z_CUST as select * from DB.S.CUST"),
        _view("A_ORDER_CUST", "create view A_ORDER_CUST as select * "
              "from DB.S.ORDERS o, DB.S.Z_CUST c where o.C = c.C")]}
    deps = extract_dependencies(_Denied({}), inv)
    assert {("DB.S.A_ORDER_CUST", "DB.S.Z_CUST"),
            ("DB.S.A_ORDER_CUST", "DB.S.ORDERS")} <= {
        (e["from"], e["to"]) for e in deps["edges"]}
    plan = build_plan(inv, deps)
    wave_of = {n: i for i, wave in enumerate(plan["waves"]) for n in wave}
    assert wave_of["DB.S.Z_CUST"] < wave_of["DB.S.A_ORDER_CUST"], plan["waves"]


def test_a_comment_does_not_fabricate_a_cycle():
    inv = {"inventory": [
        _table("T"),
        _view("V_A", "create view V_A comment='feeds V_B; not from V_B' as "
              "select * from DB.S.T -- joined from V_B later\n"),
        _view("V_B", "create view V_B as select * from DB.S.V_A")]}
    deps = extract_dependencies(_Denied({}), inv)
    assert ("DB.S.V_A", "DB.S.V_B") not in {
        (e["from"], e["to"]) for e in deps["edges"]}
    plan = build_plan(inv, deps)
    assert plan["cycles"] == [], plan["cycles"]
    wave_of = {n: i for i, wave in enumerate(plan["waves"]) for n in wave}
    assert wave_of["DB.S.V_A"] < wave_of["DB.S.V_B"], plan["waves"]
