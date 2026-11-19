"""Data-movement options. MVP presents them; it executes nothing."""
import pytest

from plan.data_movement import (
    CUSTOMER_DEFINED_ID, MAINTENANCE_TRAPS, OPTIONS, NotImplementedInMvp,
    execute_transfer, options_for, record_choice,
)
from report.render import architecture_section


def test_at_least_three_options_offered():
    assert len(OPTIONS) >= 3


def test_every_option_is_fully_described():
    for o in OPTIONS:
        assert o["id"] and o["name"]
        assert o["catalog_type"] in ("INTERNAL", "EXTERNAL", "BOTH", "TBD")
        assert o["etl"] and o["moves_bytes"] in (True, False, None)
        assert o["pros"] and o["cons"]
        assert o["unknowns"], "every option must state what is still unverified"
        assert o["status"] == "proposal_only"


def test_the_option_space_covers_the_named_mechanisms():
    blob = " ".join(
        f'{o["name"]} {o["etl"]} {" ".join(o["pros"])} {" ".join(o["cons"])}'
        for o in OPTIONS).lower()
    for mechanism in ("object storage", "fivetran", "external", "internal",
                      "iceberg", "interconnect"):
        assert mechanism in blob, mechanism


def test_internal_and_external_catalog_options_both_exist():
    kinds = {o["catalog_type"] for o in OPTIONS}
    assert "INTERNAL" in kinds and "EXTERNAL" in kinds


def test_at_least_one_option_moves_no_bytes():
    assert any(o["moves_bytes"] is False for o in OPTIONS)


def test_a_customer_defined_slot_exists_so_none_of_the_above_is_answerable():
    from plan.data_movement import CUSTOMER_DEFINED_ID
    assert any(o["id"] == CUSTOMER_DEFINED_ID for o in OPTIONS)


def test_options_for_historic_and_ongoing_are_distinguished():
    historic = options_for("historic")
    ongoing = options_for("ongoing")
    assert historic and ongoing
    assert {o["id"] for o in historic} != {o["id"] for o in ongoing}


def test_unknown_phase_is_rejected():
    with pytest.raises(ValueError, match="unknown phase"):
        options_for("someday")


# --- choice recording -----------------------------------------------------

def test_recording_a_choice_captures_it_without_acting():
    rec = record_choice("A1_UNLOAD_OBJECT_STORAGE", chosen_by="navid",
                        rationale="same-region unload avoids egress")
    assert rec["option_id"] == "A1_UNLOAD_OBJECT_STORAGE"
    assert rec["executed"] is False
    assert "navid" in rec["chosen_by"]
    assert rec["next_step"]


def test_recording_an_unknown_option_is_rejected():
    with pytest.raises(ValueError, match="unknown option"):
        record_choice("A9_TELEPORT", chosen_by="x", rationale="y")


def test_a_choice_requires_a_rationale():
    # The choice drives cost and wall-clock; an unexplained one is not a decision.
    with pytest.raises(ValueError, match="rationale"):
        record_choice("A1_UNLOAD_OBJECT_STORAGE", chosen_by="x", rationale="")


# --- the hard boundary ----------------------------------------------------

def test_execution_is_refused_for_every_option():
    for o in OPTIONS:
        with pytest.raises(NotImplementedInMvp) as exc:
            execute_transfer(o["id"])
        assert o["id"] in str(exc.value)


def test_the_refusal_says_what_would_have_to_be_settled_first():
    with pytest.raises(NotImplementedInMvp, match="region"):
        execute_transfer("A1_UNLOAD_OBJECT_STORAGE")


# ==========================================================================
# Maintenance ownership is an architecture consequence, not a footnote.
#
# Whether the customer inherits OPTIMIZE/VACUUM depends entirely on which
# option they pick: federating leaves the data in Snowflake, which keeps
# maintaining it; landing Delta tables transfers the obligation on day one.
# Presenting the options without saying so hides a real operating cost.
# ==========================================================================

def test_every_option_states_who_owns_maintenance():
    for o in OPTIONS:
        assert o.get("maintenance_ownership"), f'{o["id"]} does not say'


def test_federating_leaves_maintenance_with_snowflake():
    a2 = next(o for o in OPTIONS if o["id"] == "A2_FEDERATE_EXTERNAL_CATALOG")
    owner = a2["maintenance_ownership"]
    assert owner["owner"] == "snowflake"
    # None of the Delta traps bite data that never became a Delta table.
    assert owner["traps_apply"] == []


def test_landing_delta_tables_transfers_every_trap():
    a1 = next(o for o in OPTIONS if o["id"] == "A1_UNLOAD_OBJECT_STORAGE")
    owner = a1["maintenance_ownership"]
    assert owner["owner"] == "customer"
    assert set(owner["traps_apply"]) == {t["id"] for t in MAINTENANCE_TRAPS}


def test_streaming_ingestion_is_flagged_as_the_worst_churn_case():
    a3 = next(o for o in OPTIONS if o["id"] == "A3_REDIRECT_INGESTION")
    assert a3["maintenance_ownership"]["owner"] == "customer"
    assert "churn" in a3["maintenance_ownership"]["note"].lower() \
        or "small file" in a3["maintenance_ownership"]["note"].lower()


def test_hybrid_means_two_regimes_at_once():
    a5 = next(o for o in OPTIONS if o["id"] == "A5_HYBRID_WAVES")
    assert a5["maintenance_ownership"]["owner"] == "both"


def test_a_customer_defined_architecture_makes_no_maintenance_claim():
    a6 = next(o for o in OPTIONS if o["id"] == CUSTOMER_DEFINED_ID)
    owner = a6["maintenance_ownership"]
    assert owner["owner"] is None, "unknown until they describe it"
    assert owner["traps_apply"] is None


def test_only_the_customer_defined_option_may_leave_ownership_unknown():
    unknown = [o["id"] for o in OPTIONS
               if o["maintenance_ownership"]["owner"] is None]
    assert unknown == [CUSTOMER_DEFINED_ID]


def test_the_three_traps_are_declared_with_ids_and_consequences():
    assert len(MAINTENANCE_TRAPS) == 3
    for trap in MAINTENANCE_TRAPS:
        assert trap["id"] and trap["trap"] and trap["consequence"]
    text = " ".join(t["trap"] + t["consequence"] for t in MAINTENANCE_TRAPS).lower()
    assert "time travel" in text          # VACUUM bounds it
    assert "storage" in text              # OPTIMIZE grows it until VACUUM
    assert "schedul" in text              # nothing runs itself


def test_the_architecture_section_always_carries_the_traps():
    # The options are always presented; the maintenance consequence must
    # travel with them rather than living in a reference file.
    section = architecture_section({})   # no choice recorded
    text = "\n".join(section) if isinstance(section, list) else str(section)
    low = text.lower()
    assert "maintenance" in low
    for trap in MAINTENANCE_TRAPS:
        assert trap["trap"].split(".")[0].lower()[:25] in low


def test_the_rendered_table_shows_the_real_owner_per_option():
    """Guards the projection, not just the source data.

    `architecture_decision()` projects a fixed field set, and the first version
    of this feature dropped `maintenance_ownership` on the way through -- so
    every row rendered "*unknown*" while the unit tests, which read OPTIONS
    directly, all passed.
    """
    text = "\n".join(architecture_section({}))
    row = [ln for ln in text.split("\n")
           if "A2_FEDERATE_EXTERNAL_CATALOG" in ln and ln.startswith("|")][0]
    assert "Snowflake" in row, f"A2 must show Snowflake as owner: {row}"
    row = [ln for ln in text.split("\n")
           if "A1_UNLOAD_OBJECT_STORAGE" in ln and ln.startswith("|")][0]
    assert "you" in row, f"A1 must show the customer as owner: {row}"
    # And nothing but A6 may render as unknown.
    unknown_rows = [ln for ln in text.split("\n")
                    if ln.startswith("| **A") and "*unknown*" in ln
                    and "Maintenance" not in ln]
    assert all("A6_CUSTOMER_DEFINED" in ln for ln in unknown_rows), unknown_rows


def test_the_rendered_per_option_note_is_not_empty():
    text = "\n".join(architecture_section({}))
    section = text.split("### What each choice does to maintenance")[1]
    for line in section.split("### Three maintenance points to plan for")[0].strip().split("\n"):
        if line.startswith("- **A"):
            body = line.split("—", 1)[1]
            assert len(body.strip(" .()")) > 40, f"empty note: {line}"


# --- the CLI's own surfaces scope the "no bytes" claim ----------------------
#
# The CHANGELOG's "every surface now says the control plane
# copies no data; snowmig_02_copy_schema moves rows" fix covered the docs and
# skills, and missed the CLI itself: the snowmig.py module docstring (which
# IS the root --help) still said the plugin "moves no bytes and implements
# no transfer", and cmd_data_options hard-coded the same note into
# data_options.json. So one data-options run wrote a JSON saying there is
# no transfer path next to a DATA_MOVEMENT_OPTIONS.md naming the one there
# is. --help also sent STANDARD-catalog structure to `notebook`, whose
# upload is refused, instead of the structure job at S10. The surface test
# scans docs, not engine .py, so nothing caught it.

def _flat(text):
    return " ".join(text.split())


def test_root_help_does_not_claim_the_plugin_moves_no_bytes():
    import snowmig
    text = _flat(snowmig.build_parser().format_help())
    assert "moves no bytes and implements no transfer" not in text
    assert "snowmig_02_copy_schema" in text
    assert "snowmig_01_structure" in text, "STANDARD structure is S10's job"
    assert "by the `notebook` script" not in text


def test_data_options_json_names_the_implemented_copy_path(tmp_path, capsys):
    import json
    import snowmig
    assert snowmig.main(["data-options", "--out-dir", str(tmp_path)]) == 0
    payload = json.loads((tmp_path / "data_options.json").read_text(
        encoding="utf-8"))
    assert "moves no bytes and implements no transfer" not in payload["note"]
    assert "snowmig_02_copy_schema" in payload["note"]
    md = (tmp_path / "DATA_MOVEMENT_OPTIONS.md").read_text(encoding="utf-8")
    assert "snowmig_02_copy_schema" in md, "the two artifacts agree"
    assert "none implemented" not in capsys.readouterr().out


def test_the_json_note_is_the_markdowns_own_sentence(tmp_path):
    """One constant for both artifacts. The CLI and the renderer were each
    fixed to stop saying "implements no transfer", in two lanes, in two
    different wordings -- which is the drift the shared constant exists to
    stop."""
    import json
    import snowmig
    from report.render import DATA_OPTIONS_NOTE
    assert snowmig.main(["data-options", "--out-dir", str(tmp_path)]) == 0
    payload = json.loads((tmp_path / "data_options.json").read_text(
        encoding="utf-8"))
    assert payload["note"] == DATA_OPTIONS_NOTE
