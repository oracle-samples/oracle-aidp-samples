"""A view that reads an object outside the assessed scope is not migrating.

Migrations run one database at a time, so a view in D that joins
OTHERDB.S.FACTS is ordinary. Both lineage sources used to keep only edges
whose two ends were inventory ids, so a mixed view -- `from D.S.T join
OTHERDB.S.FACTS` -- kept its in-scope edge, was "ordered", sat in wave 2
under "Dependencies land before their dependents, so views follow their
base tables", and never mentioned OTHERDB. DDL kept the qualified reference
verbatim, and the create then failed with a bare 500.

The extractor now keeps the outside reference as an edge (another change).
This is the planner's side: an edge whose `to` is not an inventory
source_identifier is a dependency on something that is not migrating, so
the view goes to cannot_migrate as dependency_not_migrated, naming the
outside object and saying it is outside the assessed scope, and the cascade
carries on from there.
"""
from plan.build import build_plan
from report.render import render_planned_objects
from test_plan_build import rec


def _inv():
    return {"inventory": [
        rec("D.S.T"),
        rec("D.S.V_MIX", kind="VIEW",
            ddl="create view V_MIX as select t.a from D.S.T t "
                "join OTHERDB.S.FACTS f on t.a = f.a"),
        rec("D.S.V_TOP", kind="VIEW",
            ddl="create view V_TOP as select a from D.S.V_MIX"),
        rec("D.S.V_OK", kind="VIEW",
            ddl="create view V_OK as select a from D.S.T")]}


_EDGES = [{"from": "D.S.V_MIX", "to": "D.S.T"},
          {"from": "D.S.V_MIX", "to": "OTHERDB.S.FACTS"},
          {"from": "D.S.V_TOP", "to": "D.S.V_MIX"},
          {"from": "D.S.V_OK", "to": "D.S.T"}]


def test_a_view_reading_an_outside_object_cannot_migrate():
    plan = build_plan(_inv(), {"edges": _EDGES, "source_used": "account_usage"})
    can = {c["source_identifier"] for c in plan["can_migrate"]}
    assert can == {"D.S.T", "D.S.V_OK"}
    mix = next(c for c in plan["cannot_migrate"]
               if c["source_identifier"] == "D.S.V_MIX")
    assert mix["category"] == "dependency_not_migrated"
    assert "OTHERDB.S.FACTS" in mix["reason"]
    assert "outside the assessed scope" in mix["reason"]


def test_the_cascade_carries_on_from_the_refused_view():
    plan = build_plan(_inv(), {"edges": _EDGES})
    top = next(c for c in plan["cannot_migrate"]
               if c["source_identifier"] == "D.S.V_TOP")
    assert top["category"] == "dependency_not_migrated"
    assert "depends on D.S.V_MIX" in top["reason"]
    assert all("D.S.V_MIX" not in w and "D.S.V_TOP" not in w
               for w in plan["waves"])


def test_planned_objects_names_the_outside_object():
    md = render_planned_objects(build_plan(_inv(), {"edges": _EDGES}))
    assert "OTHERDB.S.FACTS" in md
    waves = md.split("## Order of creation", 1)[1]
    assert "`D.S.V_MIX` →" not in waves


def test_an_edge_to_an_excluded_inventory_object_is_still_a_restriction():
    # Inside the inventory but restricted is the existing cascade, not
    # "outside the assessed scope".
    plan = build_plan(_inv(), {"edges": _EDGES[:1] + _EDGES[3:]},
                      restrictions={"exclude_objects": ["D.S.T"]})
    ok = next(c for c in plan["cannot_migrate"]
              if c["source_identifier"] == "D.S.V_OK")
    assert "which is excluded" in ok["reason"]
    assert "outside the assessed scope" not in ok["reason"]
