"""Per-object migration status and risk. Pure."""
import pytest

from plan.status import (
    MIGRATION_STATUS, RISK_LEVELS, assess_risk, migration_status,
)


def can(ident="D.S.T", kind="TABLE", rows=10, warnings=(), omitted=()):
    return {"source_identifier": ident, "object_type": kind, "rows": rows,
            "warnings": list(warnings), "omitted_properties": list(omitted)}


# --- migration status vocabulary -----------------------------------------

def test_status_vocabulary_is_fixed():
    assert MIGRATION_STATUS == ("NOT_YET_DONE", "IN_PROGRESS", "SHALLOW_CLONE",
                                "DATA_CLONE", "DONE", "BLOCKED")


def test_planned_but_not_deployed_is_not_yet_done():
    assert migration_status("D.S.T", deployed=None) == "NOT_YET_DONE"


def test_verified_structure_is_shallow_clone():
    dep = {"dry_run": False, "verified_targets": ["D.S.T"], "failed_targets": []}
    assert migration_status("D.S.T", deployed=dep) == "SHALLOW_CLONE"


def test_attempted_but_unverified_is_in_progress():
    dep = {"dry_run": False, "attempted_targets": ["D.S.T"],
           "verified_targets": [], "failed_targets": []}
    assert migration_status("D.S.T", deployed=dep) == "IN_PROGRESS"


def test_a_failed_create_is_blocked_not_in_progress():
    # This test once pinned IN_PROGRESS, which is how SUMMARY.md came to
    # show a burned-name failure as work under way. See
    # test_status_failed_deploy.py.
    dep = {"dry_run": False, "attempted_targets": ["D.S.T"],
           "verified_targets": [], "failed_targets": ["D.S.T"]}
    assert migration_status("D.S.T", deployed=dep) == "BLOCKED"


def test_dry_run_never_reports_progress():
    dep = {"dry_run": True, "verified_targets": ["D.S.T"], "failed_targets": []}
    assert migration_status("D.S.T", deployed=dep) == "NOT_YET_DONE"


def test_blocked_object_is_blocked():
    assert migration_status("D.S.T", deployed=None, blocked=True) == "BLOCKED"


def test_data_clone_and_done_are_never_produced_by_this_mvp():
    # The plugin moves no data, so no code path may claim otherwise.
    dep = {"dry_run": False, "verified_targets": ["D.S.T"], "failed_targets": [],
           "rows_copied": {"D.S.T": 100}}
    assert migration_status("D.S.T", deployed=dep) == "SHALLOW_CLONE"


# --- risk ----------------------------------------------------------------

def test_risk_levels_are_fixed():
    assert RISK_LEVELS == ("LOW", "MEDIUM", "HIGH")


def test_plain_mapped_table_is_low_risk():
    level, note = assess_risk(can())
    assert level == "LOW"
    assert note


def test_blocked_object_is_high_risk_with_the_reason():
    level, note = assess_risk(
        {"source_identifier": "D.S.J", "object_type": "TABLE",
         "category": "unmapped_type", "reason": "PAYLOAD: VARIANT"},
        blocked=True)
    assert level == "HIGH"
    assert "VARIANT" in note


def test_view_is_high_risk_for_untranslated_sql():
    # Raised from MEDIUM: an untranslated view creates successfully and then
    # returns wrong numbers, which is worse than a loud failure.
    level, note = assess_risk({"object_type": "VIEW"})
    assert level == "HIGH"
    assert "translation" in note

def test_dropped_properties_raise_risk_to_medium():
    level, note = assess_risk(can(omitted=["cluster_by=(C)"]))
    assert level == "MEDIUM"
    assert "cluster_by" in note


def test_large_table_is_flagged_for_the_later_data_phase():
    level, note = assess_risk(can(rows=500_000_000))
    assert level == "MEDIUM"
    assert "row" in note.lower()


def test_timezone_warning_raises_risk():
    level, note = assess_risk(can(warnings=["TS: TIMESTAMP_LTZ timezone semantics"]))
    assert level == "MEDIUM"
    assert "timezone" in note.lower()


def test_highest_applicable_risk_wins():
    level, _ = assess_risk(can(kind="VIEW", rows=999_999_999), blocked=True)
    assert level == "HIGH"


def test_note_is_always_a_sentence_not_empty():
    for obj in (can(), can(kind="VIEW"), can(rows=10**9)):
        _, note = assess_risk(obj)
        assert note and len(note) > 15


# --------------------------------------------------------------------------
# A structure mismatch must not read as a clone.
# --------------------------------------------------------------------------

def test_a_mismatched_object_is_blocked_not_shallow_clone():
    # The object exists in AIDP but is not the one we planned, and IF NOT
    # EXISTS means we left it alone. Reporting SHALLOW_CLONE would claim we
    # cloned someone else's table.
    deployed = {"dry_run": False, "attempted_targets": ["A"],
                "verified_targets": [], "mismatched_targets": ["A"],
                "unverified_structure_targets": [], "failed_targets": []}
    assert migration_status("A", deployed=deployed) == "BLOCKED"


def test_an_unverified_structure_is_in_progress_not_shallow_clone():
    deployed = {"dry_run": False, "attempted_targets": ["A"],
                "verified_targets": [], "mismatched_targets": [],
                "unverified_structure_targets": ["A"], "failed_targets": []}
    assert migration_status("A", deployed=deployed) == "IN_PROGRESS"


def test_verified_is_still_shallow_clone():
    deployed = {"dry_run": False, "attempted_targets": ["A"],
                "verified_targets": ["A"], "mismatched_targets": [],
                "unverified_structure_targets": [], "failed_targets": []}
    assert migration_status("A", deployed=deployed) == "SHALLOW_CLONE"


def test_an_untranslated_view_is_high_risk_not_medium():
    # An untranslated view CREATES SUCCESSFULLY and then returns wrong
    # numbers. That is worse than an object that fails loudly.
    level, note = assess_risk({"object_type": "VIEW"})
    assert level == "HIGH"
    assert "wrong" in note.lower() or "verify" in note.lower()


def test_risk_is_never_downgraded():
    # A view is HIGH because a mistranslation creates successfully and then
    # returns wrong numbers. A column warning on the same view is a lesser
    # fact and must not pull it down to MEDIUM.
    level, note = assess_risk({"object_type": "VIEW", "warnings": [
        "STATUS: declared length 20 is not enforced by Delta; recorded only"]})
    assert level == "HIGH"
    assert "1 column warning" in note
    level, _ = assess_risk(can(kind="VIEW", warnings=["TS: timezone semantics"],
                               omitted=["is_secure=false"]))
    assert level == "HIGH"
    level, _ = assess_risk({"object_type": "VIEW", "deferred_properties": [
        {"property": "cluster_by", "value": "(A)", "aidp_equivalent": "x"}]})
    assert level == "HIGH"


def test_a_deferred_maintenance_setting_raises_risk_and_names_itself():
    # A clustering key that does not arrive is a performance regression on the
    # biggest tables. It must not read as LOW.
    level, note = assess_risk({
        "object_type": "TABLE",
        "deferred_properties": [{"property": "cluster_by",
                                 "value": "(ORDER_DATE)",
                                 "aidp_equivalent": "CLUSTER BY / ZORDER"}]})
    assert level == "MEDIUM"
    assert "cluster_by" in note


def test_a_kind_warning_raises_risk_to_medium_and_carries_the_sentence():
    # A TRANSIENT/TEMPORARY table planned as a permanent Delta table is a
    # caveat about the object, not about a column, and the row must carry
    # the sentence rather than a count.
    warning = ("TRANSIENT table in Snowflake (no Fail-safe, short Time Travel); "
               "it is planned as a permanent Delta table, so confirm it is "
               "meant to persist")
    level, note = assess_risk({"object_type": "TABLE", "kind_warning": warning})
    assert level == "MEDIUM"
    assert warning in note
    assert "column warning" not in note
    assert "clones cleanly" not in note


def test_a_kind_warning_never_lowers_a_view_from_high():
    level, _ = assess_risk({"object_type": "VIEW", "kind_warning": "TEMPORARY ..."})
    assert level == "HIGH"


def test_a_missing_or_empty_kind_warning_leaves_risk_alone():
    assert assess_risk(can())[0] == "LOW"
    assert assess_risk(dict(can(), kind_warning=None))[0] == "LOW"
