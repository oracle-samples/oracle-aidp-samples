"""A view's reference to an object outside the inventory is kept, not dropped.

Reproduced with the real extractor. Migrations run one database at a time, so a view in D that
joins OTHERDB.S.FACTS is an expected shape. Both lineage sources kept only
edges whose two ends were inventory ids: ACCOUNT_USAGE dropped the
V_MIX -> OTHERDB.S.FACTS row and hard-coded `unresolved_references: []`.
V_MIX kept its in-scope edge to D.S.T, so it was "ordered"; the plan put it
in wave 2 under "Dependencies land before their dependents, so views follow
their base tables", never mentioned OTHERDB, and the create failed with a
bare 500 because the target has no FACTS.

This is the extractor's half: the outside reference is an
edge, marked `outside_inventory`, from both sources, and is named in
`unresolved_references`. The planner's half -- refusing the view as
dependency_not_migrated -- reads exactly that edge. Only a VIEW's outside
references become edges: a table's (a sequence behind a column DEFAULT) is
not resolved by its CREATE TABLE, and the DEFAULT itself is already
reported as not carried.
"""
from fake_sql import FakeSql
from plan.build import build_plan
from snowflake_source.extract.dependencies import (extract_dependencies,
                                                   parse_view_references)


def _inv():
    return {"inventory": [
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"},
        {"source_identifier": "D.S.V_MIX", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "create view V_MIX as select * from D.S.T t "
                             "join OTHERDB.S.FACTS f on t.K = f.K"}]}


def _au_rows():
    return {"object_dependencies": [
        {"REFERENCING": "D.S.V_MIX", "REFERENCED": "D.S.T",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
        {"REFERENCING": "D.S.V_MIX", "REFERENCED": "OTHERDB.S.FACTS",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
        # Neither end in the inventory: somebody else's lineage.
        {"REFERENCING": "X.Y.V", "REFERENCED": "X.Y.T",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
        # A table's reference out of scope (a sequence behind a DEFAULT).
        {"REFERENCING": "D.S.T", "REFERENCED": "D.S.SEQ_ID",
         "REFERENCING_TYPE": "TABLE", "REFERENCED_TYPE": "SEQUENCE"}]}


class _Denied(FakeSql):
    def __call__(self, sql, params=None):
        if "object_dependencies" in sql.lower():
            raise RuntimeError("Object does not exist or not authorized")
        return super().__call__(sql, params)


def _pairs(deps):
    return {(e["from"], e["to"]) for e in deps["edges"]}


def test_account_usage_keeps_the_outside_reference_as_an_edge():
    deps = extract_dependencies(FakeSql(_au_rows()), _inv())
    assert deps["source_used"] == "account_usage"
    assert _pairs(deps) == {("D.S.V_MIX", "D.S.T"),
                            ("D.S.V_MIX", "OTHERDB.S.FACTS")}
    outside = [e for e in deps["edges"] if e.get("outside_inventory")]
    assert outside == [{"from": "D.S.V_MIX", "to": "OTHERDB.S.FACTS",
                        "kind": "VIEW->TABLE", "source": "account_usage",
                        "outside_inventory": True}]
    assert deps["unresolved_references"] == ["OTHERDB.S.FACTS"]


def test_an_in_scope_edge_is_not_marked_outside():
    deps = extract_dependencies(FakeSql(_au_rows()), _inv())
    inside = next(e for e in deps["edges"] if e["to"] == "D.S.T")
    assert "outside_inventory" not in inside


def test_parsed_ddl_keeps_the_outside_reference_as_an_edge():
    deps = extract_dependencies(_Denied({}), _inv())
    assert deps["source_used"] == "parsed_ddl"
    assert _pairs(deps) == {("D.S.V_MIX", "D.S.T"),
                            ("D.S.V_MIX", "OTHERDB.S.FACTS")}
    (outside,) = [e for e in deps["edges"] if e.get("outside_inventory")]
    assert outside["source"] == "parsed_ddl"
    assert deps["unresolved_references"] == ["OTHERDB.S.FACTS"]


def test_a_lagged_view_parsed_from_ddl_keeps_its_outside_reference_too():
    # ACCOUNT_USAGE readable but has no row for V_MIX yet.
    deps = extract_dependencies(FakeSql({"object_dependencies": []}), _inv())
    assert deps["source_used"] == "account_usage_empty"
    assert ("D.S.V_MIX", "OTHERDB.S.FACTS") in _pairs(deps)
    assert deps["unresolved_references"] == ["OTHERDB.S.FACTS"]


def test_a_from_that_names_no_object_makes_no_outside_edge():
    # An outside edge now refuses a view, so a word after FROM that is not
    # a relation name must not become one.
    inv = {"inventory": [{
        "source_identifier": "D.S.V", "object_type": "VIEW",
        "source_database": "D", "source_schema": "S",
        "view_ddl_get_ddl": "create view V as select extract(year from TS) "
                            "from identifier('D.S.T'), table(flatten(x))"}]}
    deps = extract_dependencies(_Denied({}), inv)
    assert deps["edges"] == [] and deps["unresolved_references"] == []


def test_nth_value_from_first_or_last_is_not_a_from_clause():
    # Snowflake's
    # `NTH_VALUE(x, n) FROM FIRST|LAST OVER (...)` was read as a FROM clause,
    # so parse_view_references returned DB.S.FIRST beside the real DB.S.T.
    # Once an outside reference is kept as an edge, that name -- an
    # object that does not exist -- refuses a valid view as
    # dependency_not_migrated whenever lineage comes from parsed DDL.
    for direction in ("first", "LAST"):
        ddl = (f"create view V as select nth_value(a, 2) from {direction} "
               "ignore nulls over (order by b) from T")
        assert parse_view_references(ddl, default_db="D",
                                     default_schema="S") == ["D.S.T"]
    inv = {"inventory": [
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "source_database": "D", "source_schema": "S"},
        {"source_identifier": "D.S.V", "object_type": "VIEW",
         "source_database": "D", "source_schema": "S",
         "view_ddl_get_ddl": "create view V as select nth_value(a, 2) "
                             "from first over (order by b) from T"}]}
    deps = extract_dependencies(_Denied({}), inv)
    assert _pairs(deps) == {("D.S.V", "D.S.T")}
    assert deps["unresolved_references"] == []


def test_a_table_named_first_after_another_call_is_still_read():
    # The guard is narrow: only the `)` closing an NTH_VALUE call makes
    # `FROM FIRST` a window modifier. A relation really named FIRST, after
    # any other call's `)`, is still a reference.
    refs = parse_view_references(
        "create view V as select count(*) from first",
        default_db="D", default_schema="S")
    assert refs == ["D.S.FIRST"]
    refs = parse_view_references(
        "create view V as select nth_value(a, 2) over (order by b) from LAST",
        default_db="D", default_schema="S")
    assert refs == ["D.S.LAST"]


def test_the_plan_still_builds_over_an_outside_edge():
    inv = _inv()
    plan = build_plan(inv, extract_dependencies(FakeSql(_au_rows()), inv))
    assert "D.S.T" in plan["clone_targets"]
