"""The architecture options are ALWAYS presented, not an opt-in stage."""
import json

import pytest

from plan.data_movement import (
    OPTIONS, architecture_decision, capability_matrix,
)
from report.render import render_planned_objects, render_summary

PLAN = {
    "bronze_mapping": "database -> Standard Catalog",
    "target_names": {"D.S.T": "D.S.T"},
    "can_migrate": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                     "target": "D.S.T", "rows": 1, "columns": 1}],
    "cannot_migrate": [], "silver_gold_jobs": [],
    "catalogs_to_create": ["D"], "schemas_to_create": [["D", "S"]],
    "waves": [["D.S.T"]], "cycles": [],
    "summary": {"objects_inventoried": 1, "can_migrate": 1, "cannot_migrate": 0,
                "tables": 1, "views": 0, "catalogs": 1, "schemas": 1,
                "silver_gold_jobs": 0, "cannot_by_category": {}},
}
INV = {"session": {"A": "ACC", "R": "REG", "ROLE": "R", "V": "1"},
       "databases_in_scope": ["D"]}


# --- every option must be actionable by a future MVP ---------------------

def test_every_option_says_what_building_it_requires():
    for o in OPTIONS:
        assert o["implementation_notes"], o["id"]
        assert len(" ".join(o["implementation_notes"])) > 60, o["id"]


def test_every_option_declares_what_it_handles():
    from plan.data_movement import CUSTOMER_DEFINED_ID
    for o in OPTIONS:
        assert set(o["handles"]) <= {"historic_bulk", "ongoing_incremental",
                                     "read_without_copy", "cutover"}
        if o["id"] != CUSTOMER_DEFINED_ID:
            assert o["handles"], o["id"]


def test_capability_matrix_covers_every_option_and_capability():
    m = capability_matrix()
    assert set(m["options"]) == {o["id"] for o in OPTIONS}
    for cap in ("historic_bulk", "ongoing_incremental", "read_without_copy"):
        assert any(cap in caps for caps in m["options"].values()), cap
    assert m["note"]


# --- the decision state --------------------------------------------------

def test_no_choice_returns_every_option_and_says_so():
    d = architecture_decision(None)
    assert d["decided"] is False
    assert len(d["options"]) == len(OPTIONS)
    assert "no architecture has been chosen" in d["statement"].lower()


def test_a_recorded_choice_is_surfaced_with_its_outstanding_unknowns():
    choice = {"option_id": "A1_UNLOAD_OBJECT_STORAGE", "chosen_by": "navid",
              "rationale": "same-region unload", "executed": False}
    d = architecture_decision(choice)
    assert d["decided"] is True
    assert d["chosen"]["id"] == "A1_UNLOAD_OBJECT_STORAGE"
    assert d["unknowns_outstanding"]
    assert d["options"], "the alternatives stay visible even after a choice"


def test_an_unknown_recorded_choice_does_not_silently_pass():
    d = architecture_decision({"option_id": "A9_NOPE"})
    assert d["decided"] is False
    assert "A9_NOPE" in d["statement"]


# --- always rendered -----------------------------------------------------

def test_planned_objects_always_carries_the_architecture_options():
    md = render_planned_objects(PLAN)
    assert "Data-movement architecture" in md
    for o in OPTIONS:
        assert o["id"] in md, o["id"]
    assert "no architecture has been chosen" in md.lower()


def test_summary_always_carries_the_architecture_options():
    md = render_summary(PLAN, INV, None, None)
    for o in OPTIONS:
        assert o["id"] in md, o["id"]


def test_options_appear_even_with_no_destination_and_no_deployment():
    md = render_summary(PLAN, INV, None, None)
    assert "not supplied" in md.lower()
    assert "A2_FEDERATE_EXTERNAL_CATALOG" in md


def test_a_recorded_choice_is_shown_in_both_reports():
    plan = dict(PLAN, architecture_choice={
        "option_id": "A4_ICEBERG_INTEROP", "chosen_by": "navid",
        "rationale": "estate is already Iceberg", "executed": False})
    for md in (render_planned_objects(plan), render_summary(plan, INV, None, None)):
        assert "A4_ICEBERG_INTEROP" in md
        assert "estate is already Iceberg" in md
        assert "not executed" in md.lower() or "executes nothing" in md.lower()


def test_reports_never_claim_a_transfer_happened():
    plan = dict(PLAN, architecture_choice={
        "option_id": "A1_UNLOAD_OBJECT_STORAGE", "chosen_by": "x",
        "rationale": "y", "executed": False})
    for md in (render_planned_objects(plan), render_summary(plan, INV, None, None)):
        low = md.lower()
        assert "moves no bytes" in low or "no data" in low
