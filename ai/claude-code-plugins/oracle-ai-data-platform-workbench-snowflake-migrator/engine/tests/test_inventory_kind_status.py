"""INVENTORY.md's Compatibility column must agree with the planner.

Live 2026-09-25: a dynamic table read `TABLE ... supported` in INVENTORY.md
while PLANNED_OBJECTS.md, from the same inventory, refused it as
`unsupported_object`. `compatibility_status` only ever described the column
types; the object kind was decided later, in plan/build.py, and the one
report most readers open first never heard about it. A column that says
"supported" for something the tool will refuse is a promise the tool breaks.
"""
import pytest

from plan.build import build_plan, object_kind_block
from report.render import render_inventory


def _rec(name, kind="TABLE", **flags):
    rec = {"source_identifier": f"D.S.{name}", "object_type": kind,
           "source_database": "D", "source_schema": "S",
           "identifier_case_form": "UPPER_UNQUOTED",
           "compatibility_status": "supported", "blocked_reasons": [],
           "row_count_exact": 1, "row_count_source": "show_metadata",
           "columns": [{"COLUMN_NAME": "A", "DATA_TYPE": "NUMBER",
                        "ORDINAL_POSITION": 1, "target_type": "DECIMAL(38,0)"}],
           "source_metadata": dict(flags)}
    if kind == "VIEW":
        rec["view_ddl_get_ddl"] = "create view V as select a from D.S.T"
    return rec


def _inv(*records):
    return {"probed_at": "2026-09-25T00:00:00+00:00",
            "session": {"A": "ACCT", "R": "AWS_US_EAST_1", "ROLE": "R"},
            "databases_in_scope": ["D"], "object_count": len(records),
            "counts_by_type": {}, "identifier_case_collisions": {},
            "extraction_notes": [], "row_count_mode": "metadata",
            "inventory": list(records)}


def _cell(md, ident):
    line = next(l for l in md.splitlines() if l.startswith(f"| `{ident}`"))
    return line.rstrip(" |").rsplit("|", 1)[-1].strip()


KIND_FLAGS = [
    ("TABLE", "is_dynamic", "dynamic table"),
    ("TABLE", "is_external", "external table"),
    ("TABLE", "is_iceberg", "Iceberg table"),
    ("TABLE", "is_event", "event table"),
    ("TABLE", "is_hybrid", "hybrid table"),
    ("VIEW", "is_secure", "secure view"),
    ("VIEW", "is_materialized", "materialized view"),
]


@pytest.mark.parametrize("kind,flag,label", KIND_FLAGS)
def test_a_kind_the_planner_refuses_is_not_shown_as_supported(kind, flag, label):
    rec = _rec("X", kind=kind, **{flag: "Y"})
    cell = _cell(render_inventory(_inv(rec)), "D.S.X")
    assert cell != "supported", cell
    assert label in cell, cell


@pytest.mark.parametrize("kind,flag,label", KIND_FLAGS)
def test_the_inventory_and_the_plan_say_the_same_thing(kind, flag, label):
    """The property that broke: one inventory, two reports, two answers.

    A dynamic table and a materialized view now migrate as a table
    snapshot, and the inventory says `table snapshot (...)`; an external or
    Iceberg table is refused as `register in place (...)`, not `blocked`. So
    refused must be exactly "neither supported nor a snapshot"."""
    rec = _rec("X", kind=kind, **{flag: "true"})
    plan = build_plan(_inv(rec), {"edges": []})
    refused = {c["source_identifier"] for c in plan["cannot_migrate"]}
    cell = _cell(render_inventory(_inv(rec)), "D.S.X")
    copied = cell == "supported" or cell.startswith("table snapshot")
    assert ("D.S.X" in refused) == (not copied), cell


def test_a_plain_table_is_still_supported():
    rec = _rec("T", is_dynamic="N", is_external="N", is_iceberg="N")
    assert _cell(render_inventory(_inv(rec)), "D.S.T") == "supported"
    assert object_kind_block(rec) is None


def test_a_flag_on_the_wrong_object_type_does_not_block():
    """is_secure is a view property; a table carrying it is not refused by the
    planner, so the inventory must not refuse it either."""
    rec = _rec("T", is_secure="Y")
    assert _cell(render_inventory(_inv(rec)), "D.S.T") == "supported"
    plan = build_plan(_inv(rec), {"edges": []})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == ["D.S.T"]


def test_a_type_block_still_reads_blocked():
    rec = {**_rec("T"), "compatibility_status": "blocked",
           "blocked_reasons": ["P: VARIANT"]}
    assert _cell(render_inventory(_inv(rec)), "D.S.T").startswith("blocked")


def test_a_kind_block_is_explained_once_under_the_table():
    md = render_inventory(_inv(_rec("A", is_dynamic="Y"),
                               _rec("B", is_hybrid="Y")))
    tail = md[md.index("| `D.S.B`"):]
    assert tail.lower().count("object kind") == 1, tail[:600]
    assert "PLANNED_OBJECTS.md" in tail


def test_no_explanation_when_nothing_is_kind_blocked():
    md = render_inventory(_inv(_rec("A")))
    assert "object kind" not in md.lower()


def test_the_label_is_the_planners_own():
    """One table of kinds, read by both reports. A second copy in the
    renderer is the drift this test exists to stop."""
    label, reason = object_kind_block(_rec("X", is_hybrid="Y"))
    assert label == "hybrid table"
    plan = build_plan(_inv(_rec("X", is_hybrid="Y")), {"edges": []})
    assert plan["cannot_migrate"][0]["reason"] == reason
