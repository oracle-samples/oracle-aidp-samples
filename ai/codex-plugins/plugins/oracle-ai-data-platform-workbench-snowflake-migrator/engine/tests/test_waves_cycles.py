"""A cycle is its strongly connected component, not everything left over.

compute_waves returned `cycles=[sorted(node_set - placed)]`: every node
Kahn's algorithm could not place, as ONE cycle. So a view that only reads a
cycle member was listed under "Dependency cycles", and its DDL reason said
"dependency cycle with D.S.A_VW, D.S.B_VW" although it is in no cycle; and
two unrelated cycles merged into one (`[['A','B','X','Y']]`), so each
member's reason named objects from the other cycle. Holding all of them
back is right -- none can be ordered -- but the labels were wrong.

Now the stuck set is split into strongly connected components: those of
size > 1 are the cycles, and every other stuck node is reported as blocked
behind the cycle(s) it depends on.
"""
from plan.build import build_plan
from plan.waves import compute_waves
from report.render import render_planned_objects
from target.ddl import build_ddl_payload
from test_plan_build import rec


def e(dependent, dependency):
    return {"from": dependent, "to": dependency}


def test_a_node_downstream_of_a_cycle_is_not_in_it():
    out = compute_waves(["A", "B", "C"], [e("A", "B"), e("B", "A"), e("C", "A")])
    assert out["waves"] == []
    assert out["cycles"] == [["A", "B"]]
    assert out["blocked_behind_cycle"] == {"C": ["A", "B"]}


def test_two_independent_cycles_stay_two():
    out = compute_waves(["A", "B", "X", "Y", "OK"],
                        [e("A", "B"), e("B", "A"), e("X", "Y"), e("Y", "X")])
    assert out["waves"] == [["OK"]]
    assert out["cycles"] == [["A", "B"], ["X", "Y"]]
    assert out["blocked_behind_cycle"] == {}


def test_a_chain_behind_a_cycle_names_the_cycle_it_waits_on():
    out = compute_waves(["A", "B", "C", "D"],
                        [e("A", "B"), e("B", "A"), e("C", "A"), e("D", "C")])
    assert out["cycles"] == [["A", "B"]]
    assert out["blocked_behind_cycle"] == {"C": ["A", "B"], "D": ["A", "B"]}


def _views(*names_and_bases):
    return [rec(f"D.S.{n}", kind="VIEW",
                ddl=f"create view {n} as select a from D.S.{b}")
            for n, b in names_and_bases]


def _plan():
    inv = {"inventory": _views(("A_VW", "B_VW"), ("B_VW", "A_VW"),
                               ("C_VW", "A_VW"), ("X_VW", "Y_VW"),
                               ("Y_VW", "X_VW"))}
    edges = [e("D.S.A_VW", "D.S.B_VW"), e("D.S.B_VW", "D.S.A_VW"),
             e("D.S.C_VW", "D.S.A_VW"), e("D.S.X_VW", "D.S.Y_VW"),
             e("D.S.Y_VW", "D.S.X_VW")]
    return inv, build_plan(inv, {"edges": edges})


def test_the_plan_carries_the_cycles_and_what_waits_behind_them():
    _, plan = _plan()
    assert plan["cycles"] == [["D.S.A_VW", "D.S.B_VW"], ["D.S.X_VW", "D.S.Y_VW"]]
    assert plan["blocked_behind_cycle"] == {"D.S.C_VW": ["D.S.A_VW", "D.S.B_VW"]}


def test_ddl_reasons_name_only_the_objects_of_that_cycle():
    inv, plan = _plan()
    reasons = {b["source_identifier"]: b["reason"]
               for b in build_ddl_payload(inv, plan)["blocked"]}
    assert set(reasons) == {"D.S.A_VW", "D.S.B_VW", "D.S.C_VW",
                            "D.S.X_VW", "D.S.Y_VW"}
    assert "dependency cycle with D.S.B_VW;" in reasons["D.S.A_VW"]
    assert "X_VW" not in reasons["D.S.A_VW"] and "C_VW" not in reasons["D.S.A_VW"]
    assert "dependency cycle with" not in reasons["D.S.C_VW"]
    assert "not in a cycle" in reasons["D.S.C_VW"]
    assert "D.S.A_VW, D.S.B_VW" in reasons["D.S.C_VW"]


def test_planned_objects_lists_cycles_apart_from_what_waits_behind_them():
    _, plan = _plan()
    md = render_planned_objects(plan)
    section = md.split("## Dependency cycles", 1)[1].split("\n## ", 1)[0]
    cycle_lines = [l for l in section.splitlines() if l.startswith("- `")]
    assert cycle_lines[:2] == ["- `D.S.A_VW`, `D.S.B_VW`",
                               "- `D.S.X_VW`, `D.S.Y_VW`"]
    assert "Blocked behind a cycle" in section
    behind = section.split("Blocked behind a cycle", 1)[1]
    assert "`D.S.C_VW`" in behind and "`D.S.C_VW`" not in "\n".join(cycle_lines[:2])
