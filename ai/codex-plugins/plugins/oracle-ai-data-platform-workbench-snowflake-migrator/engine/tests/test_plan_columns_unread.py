"""A table whose columns were never read is not a table that clones cleanly.

When the INFORMATION_SCHEMA.COLUMNS read for a schema failed (a statement
timeout, 000630, in the repro) the extractor noted it and returned no
columns, and every report downstream took the empty list at face value:
INVENTORY.md said `supported` with Cols 0, PLANNED_OBJECTS.md planned it
("0 cannot move"), SUMMARY.md rated it LOW with "All column types mapped, no
properties dropped; structure clones cleanly", and only DDL_PLAN.md refused
it -- blaming a privilege problem for what was a timeout. Four reports, one
table, four different stories, and the one that was right sent the operator
to fix grants.

The extractor now marks such a record `compatibility_status: "unassessed"`,
`columns_read: "failed"` and the error text in `columns_read_error`. These
tests hold the planner and the reports to that record, using synthetic
records carrying exactly those fields.
"""
from plan.build import build_plan
from plan.status import assess_risk
from report.render import (render_inventory, render_planned_objects,
                           render_summary)

ERROR = ("000630 (57014): Statement reached its statement or warehouse "
         "timeout of 30 second(s) and was canceled.")


def _unread(ident="MYDB.PUBLIC.ORDERS"):
    db, schema, _ = ident.split(".")
    return {"source_identifier": ident, "object_type": "TABLE",
            "source_database": db, "source_schema": schema,
            "compatibility_status": "unassessed", "blocked_reasons": [],
            "columns": [], "columns_read": "failed",
            "columns_read_error": ERROR,
            "row_count_exact": 5, "source_metadata": {"bytes": 50},
            "identifier_case_form": "UPPER"}


def _read(ident="MYDB.PUBLIC.CUSTOMERS"):
    db, schema, _ = ident.split(".")
    return {"source_identifier": ident, "object_type": "TABLE",
            "source_database": db, "source_schema": schema,
            "compatibility_status": "supported", "blocked_reasons": [],
            "columns": [{"name": "ID", "source_type": "NUMBER(38,0)"}],
            "columns_read": "ok",
            "row_count_exact": 3, "source_metadata": {"bytes": 30},
            "identifier_case_form": "UPPER"}


def _inv():
    return {"inventory": [_unread(), _read()]}


def test_a_table_whose_columns_were_not_read_cannot_migrate():
    plan = build_plan(_inv(), {"edges": []})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == [
        "MYDB.PUBLIC.CUSTOMERS"]
    entry = next(c for c in plan["cannot_migrate"]
                 if c["source_identifier"] == "MYDB.PUBLIC.ORDERS")
    assert entry["category"] == "columns_unread"
    # The reason is the error the read got, not a guess at a privilege.
    assert "000630" in entry["reason"]
    assert "timeout" in entry["reason"]
    assert "MYDB.PUBLIC.ORDERS" not in plan["clone_targets"]


def test_the_planned_objects_report_titles_the_category():
    md = render_planned_objects(build_plan(_inv(), {"edges": []}))
    assert "(`columns_unread`) — 1" in md
    assert "000630" in md
    assert "**1** cannot move" in md


def test_the_inventory_does_not_call_it_supported():
    md = render_inventory(_inv())
    row = next(l for l in md.splitlines() if "`MYDB.PUBLIC.ORDERS`" in l)
    assert "not assessed (columns unread)" in row
    assert "supported" not in row


def test_the_summary_never_says_it_clones_cleanly():
    plan = build_plan(_inv(), {"edges": []})
    md = render_summary(plan, _inv(), None, None)
    row = next(l for l in md.splitlines() if "`MYDB.PUBLIC.ORDERS`" in l)
    assert "clones cleanly" not in row
    assert "| LOW |" not in row
    assert "BLOCKED" in row


def test_assess_risk_never_rates_an_unread_object_low():
    # Defence for a caller that hands assess_risk the inventory record
    # itself rather than the plan entry: no facts is not good facts.
    level, note = assess_risk(_unread())
    assert level != "LOW"
    assert "clones cleanly" not in note
    assert "000630" in note
