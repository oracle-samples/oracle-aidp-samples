"""The customer may want an architecture we did not list, or none yet."""
import pytest

from plan.data_movement import (
    CUSTOMER_DEFINED_ID, OPTIONS, architecture_decision, execute_transfer,
    record_choice,
)


def test_a_customer_defined_option_exists_and_is_last():
    assert OPTIONS[-1]["id"] == CUSTOMER_DEFINED_ID
    assert "customer" in OPTIONS[-1]["name"].lower()


def test_it_makes_no_claim_about_moving_bytes():
    o = next(o for o in OPTIONS if o["id"] == CUSTOMER_DEFINED_ID)
    assert o["moves_bytes"] is None, "unknown until described, not assumed"
    assert o["catalog_type"] == "TBD"
    assert o["handles"] == []


def test_only_the_customer_defined_option_may_leave_moves_bytes_unknown():
    for o in OPTIONS:
        if o["id"] != CUSTOMER_DEFINED_ID:
            assert o["moves_bytes"] in (True, False), o["id"]


# --- deferring is a valid answer -----------------------------------------

def test_choosing_it_with_no_detail_is_a_deferral_not_an_error():
    rec = record_choice(CUSTOMER_DEFINED_ID, chosen_by="navid",
                        rationale="architecture not decided yet")
    assert rec["deferred"] is True
    assert rec["custom_architecture"] is None
    assert rec["executed"] is False


def test_a_deferral_still_needs_a_rationale():
    with pytest.raises(ValueError, match="rationale"):
        record_choice(CUSTOMER_DEFINED_ID, chosen_by="x", rationale="")


def test_the_decision_reports_deferred_distinctly_from_undecided():
    never_asked = architecture_decision(None)
    deferred = architecture_decision(record_choice(
        CUSTOMER_DEFINED_ID, chosen_by="navid", rationale="later"))
    assert never_asked["decided"] is False and never_asked["deferred"] is False
    assert deferred["decided"] is False and deferred["deferred"] is True
    assert "deliberately deferred" in deferred["statement"].lower()


def test_all_options_stay_visible_while_deferred():
    d = architecture_decision(record_choice(
        CUSTOMER_DEFINED_ID, chosen_by="x", rationale="later"))
    assert len(d["options"]) == len(OPTIONS)


# --- a customer architecture that is not in our list --------------------

def test_a_custom_architecture_is_recorded_verbatim():
    rec = record_choice(
        CUSTOMER_DEFINED_ID, chosen_by="navid",
        rationale="their platform team already has a pattern",
        custom_architecture={
            "name": "Kafka CDC into Iceberg on OCI, dbt on AIDP",
            "description": "Debezium off Snowflake streams into OCI Streaming, "
                           "compacted to Iceberg, transformed by dbt on AIDP."})
    assert rec["deferred"] is False
    assert rec["custom_architecture"]["name"].startswith("Kafka CDC")
    assert "Debezium" in rec["custom_architecture"]["description"]


def test_a_custom_architecture_needs_a_name_and_a_description():
    for bad in ({"name": "", "description": "d"}, {"name": "n", "description": ""},
                {"name": "n"}, {"description": "d"}):
        with pytest.raises(ValueError, match="name and a description"):
            record_choice(CUSTOMER_DEFINED_ID, chosen_by="x", rationale="y",
                          custom_architecture=bad)


def test_a_custom_architecture_is_not_forced_into_a_listed_option():
    d = architecture_decision(record_choice(
        CUSTOMER_DEFINED_ID, chosen_by="x", rationale="y",
        custom_architecture={"name": "Their own thing", "description": "how it works"}))
    assert d["decided"] is True
    assert d["chosen"]["id"] == CUSTOMER_DEFINED_ID
    assert d["chosen"]["custom_architecture"]["name"] == "Their own thing"
    assert d["chosen"].get("mapped_to") is None, "never silently mapped to A1-A5"


def test_the_plugin_makes_no_assessment_of_a_custom_architecture():
    d = architecture_decision(record_choice(
        CUSTOMER_DEFINED_ID, chosen_by="x", rationale="y",
        custom_architecture={"name": "N", "description": "D"}))
    low = d["statement"].lower()
    assert "not assess" in low or "no assessment" in low
    assert d["unknowns_outstanding"], "unknowns are unknown, so say everything"


def test_custom_detail_on_a_listed_option_is_rejected():
    # A custom architecture belongs to the customer-defined option, not bolted
    # onto A1 where it would look like an assessed variant of it.
    with pytest.raises(ValueError, match=CUSTOMER_DEFINED_ID):
        record_choice("A1_UNLOAD_OBJECT_STORAGE", chosen_by="x", rationale="y",
                      custom_architecture={"name": "N", "description": "D"})


# --- the boundary holds --------------------------------------------------

def test_execution_is_refused_for_the_customer_defined_option_too():
    with pytest.raises(NotImplementedError, match=CUSTOMER_DEFINED_ID):
        execute_transfer(CUSTOMER_DEFINED_ID)


def test_refusal_for_a_custom_architecture_says_it_is_undescribed_to_us():
    with pytest.raises(NotImplementedError) as exc:
        execute_transfer(CUSTOMER_DEFINED_ID)
    assert "describe" in str(exc.value).lower()
