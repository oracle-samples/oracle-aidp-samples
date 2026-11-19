"""The migration summary table: name, rows, risk, migration status."""
from report.render import render_summary

PLAN = {
    "bronze_mapping": "Snowflake database -> AIDP Standard Catalog",
    "target_names": {"D.PUBLIC.ORDERS": "D.PUBLIC.ORDERS",
                     "D.PUBLIC.V": "D.PUBLIC.V"},
    "can_migrate": [
        {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "target": "D.PUBLIC.ORDERS", "rows": 100, "columns": 41},
        {"source_identifier": "D.PUBLIC.V", "object_type": "VIEW",
         "target": "D.PUBLIC.V", "rows": 100, "columns": 17}],
    "cannot_migrate": [
        {"source_identifier": "D.PUBLIC.J", "object_type": "TABLE",
         "category": "unmapped_type", "reason": "PAYLOAD: VARIANT"}],
    "silver_gold_jobs": [
        {"name": "silver_D_PUBLIC", "layer": "SILVER", "reads_from": "D.PUBLIC",
         "enabled": False, "trigger": "MANUAL_NEVER_TRIGGERED",
         "body_status": "placeholder"}],
    "catalogs_to_create": ["D"],
    "schemas_to_create": [["D", "PUBLIC"]],
    "summary": {"objects_inventoried": 3, "can_migrate": 2, "cannot_migrate": 1,
                "tables": 1, "views": 1, "catalogs": 1, "schemas": 1,
                "silver_gold_jobs": 1, "cannot_by_category": {"unmapped_type": 1}},
}
INV = {"session": {"A": "TESTACCT01", "R": "AWS_US_EAST_2", "ROLE": "ACCOUNTADMIN",
                   "V": "10.32.102"},
       "databases_in_scope": ["D"]}
DEPLOYED = {"dry_run": False, "attempted_targets": ["D.PUBLIC.ORDERS", "D.PUBLIC.V"],
            "verified_targets": ["D.PUBLIC.ORDERS"], "failed_targets": ["D.PUBLIC.V"],
            "catalog_in_scope": "D"}


# --- source -> destination ------------------------------------------------

def test_source_and_destination_are_stated_briefly():
    md = render_summary(PLAN, INV, None, None)
    assert "TESTACCT01" in md and "AWS_US_EAST_2" in md
    assert "->" in md or "→" in md


def test_destination_shows_the_target_when_supplied():
    md = render_summary(PLAN, INV, DEPLOYED,
                        {"datalake_ocid": "ocid1.aidataplatform.oc1.iad.a",
                         "workspace": "ws-1", "cluster_id": "cl-1", "catalog": "D"})
    assert "ocid1.aidataplatform.oc1.iad.a" in md
    assert "cl-1" in md


def test_destination_says_not_supplied_when_absent():
    md = render_summary(PLAN, INV, None, None)
    assert "not supplied" in md.lower()


# --- the table ------------------------------------------------------------

def test_table_has_the_four_required_columns():
    md = render_summary(PLAN, INV, None, None)
    header = next(l for l in md.splitlines() if l.startswith("| Object"))
    for col in ("Object", "Rows", "Risk", "Migration status"):
        assert col in header, header


def test_row_counts_shown_per_object():
    md = render_summary(PLAN, INV, None, None)
    assert "| 100 " in md


def test_status_is_not_yet_done_before_any_deploy():
    md = render_summary(PLAN, INV, None, None)
    assert "NOT_YET_DONE" in md


def test_status_becomes_shallow_clone_once_verified():
    md = render_summary(PLAN, INV, DEPLOYED, None)
    orders = next(l for l in md.splitlines() if "D.PUBLIC.ORDERS" in l)
    assert "SHALLOW_CLONE" in orders


def test_unverified_object_shows_in_progress():
    # Exists, columns never compared. (This used D.PUBLIC.V from DEPLOYED's
    # failed_targets, pinning a FAILED create as IN_PROGRESS.)
    dep = dict(DEPLOYED, failed_targets=[],
               unverified_structure_targets=["D.PUBLIC.V"])
    md = render_summary(PLAN, INV, dep, None)
    view = next(l for l in md.splitlines() if "`D.PUBLIC.V`" in l)
    assert "IN_PROGRESS" in view


def test_failed_object_shows_blocked():
    md = render_summary(PLAN, INV, DEPLOYED, None)
    view = next(l for l in md.splitlines() if "`D.PUBLIC.V`" in l)
    assert "BLOCKED" in view and "Deploy failed" in view


def test_blocked_object_shows_blocked_and_high_risk():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "D.PUBLIC.J" in l)
    assert "BLOCKED" in row and "HIGH" in row


def test_view_carries_high_risk():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "`D.PUBLIC.V`" in l)
    assert "HIGH" in row


def test_views_and_tables_are_both_in_the_table():
    md = render_summary(PLAN, INV, None, None)
    assert "TABLE" in md and "VIEW" in md


def test_risk_column_reflects_deferred_properties_and_timezone_warnings():
    # The facts the plan entry carries (see plan.build) must reach the Risk
    # column: a clustered table with a timezone caveat is MEDIUM, and the
    # note names both.
    # A pass-through guard only: the fixture hand-crafts the facts, so this
    # passes against a tree where build.py never produced them. The wiring
    # is pinned by test_plan_build::
    # test_can_migrate_entries_carry_the_risk_bearing_facts and test_demo::
    # test_the_demo_summary_rates_the_clustered_table_medium.
    orders = dict(PLAN["can_migrate"][0],
                  warnings=["CREATED_AT: TIMESTAMP_NTZ -> TIMESTAMP: "
                            "TIMEZONE SEMANTICS DIFFER"],
                  deferred_properties=[{"property": "cluster_by",
                                        "value": "LINEAR(ORDER_DATE)",
                                        "aidp_equivalent": "CLUSTER BY / ZORDER"}],
                  omitted_properties=[])
    plan = dict(PLAN, can_migrate=[orders, PLAN["can_migrate"][1]])
    md = render_summary(plan, INV, None, None)
    row = next(l for l in md.splitlines() if "`D.PUBLIC.ORDERS`" in l)
    assert "| MEDIUM |" in row
    assert "cluster_by=LINEAR(ORDER_DATE)" in row
    assert "timezone" in row.lower()
    rollup = next(l for l in md.splitlines() if l.startswith("By risk:"))
    assert "**MEDIUM**" in rollup


def test_view_stays_high_when_it_carries_column_warnings():
    view = dict(PLAN["can_migrate"][1], warnings=[
        "CUSTOMER: declared length 39 is not enforced by Delta; recorded only"])
    plan = dict(PLAN, can_migrate=[PLAN["can_migrate"][0], view])
    md = render_summary(plan, INV, None, None)
    row = next(l for l in md.splitlines() if "`D.PUBLIC.V`" in l)
    assert "| HIGH |" in row, row


# --- jobs -----------------------------------------------------------------

def test_jobs_appear_in_the_same_summary_format():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "silver_D_PUBLIC" in l)
    assert "JOB" in row
    assert "NOT_YET_DONE" in row


def test_job_risk_notes_it_is_a_placeholder():
    md = render_summary(PLAN, INV, None, None)
    row = next(l for l in md.splitlines() if "silver_D_PUBLIC" in l)
    assert "placeholder" in row.lower() or "never triggered" in row.lower()


# --- the no-data guarantee ------------------------------------------------

def test_summary_does_not_claim_data_status_and_points_to_reconcile():
    # The summary reads only the control-plane deploy result. Whether rows
    # were copied is the in-AIDP reconcile job's report; the summary once
    # said "the plugin ... moves no data" while snowmig_02_copy_schema
    # INSERT-SELECTs every row.
    md = render_summary(PLAN, INV, DEPLOYED, None)
    low = md.lower()
    assert "moves no data" not in low and "copies no data" not in low
    assert "reconcil" in low and "MIGRATION_REPORT.md" in md
    assert "snowmig_02_copy_schema" in md
    assert "DATA_CLONE" in md, "the vocabulary is shown so the gap is visible"
    assert "says nothing about rows" in low


def test_status_counts_rolled_up():
    md = render_summary(PLAN, INV, DEPLOYED, None)
    assert "SHALLOW_CLONE" in md and "BLOCKED" in md


# --- smoke report ---------------------------------------------------------

def test_smoke_report_shows_both_ends_and_the_verdict():
    from report.render import render_smoke
    md = render_smoke({
        "ok": True,
        "source": {"reachable": True, "user": "U", "role": "R", "account": "A",
                   "region": "REG",
                   "checks": [{"name": "list databases", "ok": True,
                               "detail": "1 database(s) visible"}]},
        "destination": {"skipped": False, "catalog": "D", "cluster_id": "cl",
                        "checks": [{"name": "read target catalog", "ok": True,
                                    "detail": "2 schema(s)"}],
                        "write_verified": True,
                        "write_note": "verified: created D.probe. remove it",
                        "left_behind": ["D.probe"]}})
    assert "PASS" in md
    assert "list databases" in md and "read target catalog" in md
    assert "D.probe" in md
    assert "DROP" in md, "must say why it cannot clean up"


def test_smoke_report_states_when_destination_was_skipped():
    from report.render import render_smoke
    md = render_smoke({"ok": True,
                       "source": {"reachable": True, "checks": []},
                       "destination": {"skipped": True,
                                       "reason": "coordinates not supplied"}})
    assert "Skipped" in md and "not supplied" in md


def test_summary_makes_no_destination_claim_when_none_is_supplied():
    md = render_summary(PLAN, INV, None, None)
    assert "none is assumed" in md.lower()
    assert "derived from the OCID" not in md, "no OCID was given to derive from"
    assert md.count("*not supplied*") >= 5, "every destination field, not just some"


# --------------------------------------------------------------------------
# A missing row count must carry its reason.
# --------------------------------------------------------------------------

def test_a_dash_in_the_rows_column_is_explained():
    # `-` for a view that was not counted looked identical to `-` for a job,
    # which has no rows at all. Different facts must not render the same.
    inv = {"row_count_mode": "metadata",
           "inventory": [
               {"source_identifier": "D.S.T", "object_type": "TABLE",
                "row_count_exact": 5, "row_count_source": "show_metadata"},
               {"source_identifier": "D.S.V", "object_type": "VIEW",
                "row_count_exact": None, "row_count_source": "not_counted",
                "row_count_note": "not counted: counting a view executes it"}]}
    md = render_summary({"can_migrate": [
        {"source_identifier": "D.S.T", "object_type": "TABLE", "rows": 5,
         "target": "C.S.T"},
        {"source_identifier": "D.S.V", "object_type": "VIEW", "rows": None,
         "target": "C.S.V"}]}, inv, None, {})
    assert "Row counts" in md
    assert "metadata" in md
    assert "executes it" in md or "executing" in md


def test_a_count_error_is_named_in_the_summary():
    inv = {"row_count_mode": "exact",
           "inventory": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                          "row_count_exact": None, "row_count_source": "error",
                          "row_count_note": "No active warehouse selected"}]}
    md = render_summary({"can_migrate": [
        {"source_identifier": "D.S.T", "object_type": "TABLE", "rows": None,
         "target": "C.S.T"}]}, inv, None, {})
    assert "No active warehouse selected" in md
    assert "D.S.T" in md


def test_smoke_report_header_is_partial_when_destination_skipped():
    from report.render import render_smoke
    md = render_smoke({"ok": True, "complete": False, "verdict": "PARTIAL",
                       "source": {"reachable": True, "checks": []},
                       "destination": {"skipped": True,
                                       "reason": "coordinates not supplied"}})
    assert "PARTIAL" in "\n".join(md.splitlines()[:3])
    assert "Verdict: **PASS**" not in md
    assert "not a pass" in md.lower()


def test_smoke_report_never_says_pass_for_a_legacy_skipped_result():
    # smoke.json written before the verdict key existed: ok True + skipped.
    from report.render import render_smoke
    md = render_smoke({"ok": True,
                       "source": {"reachable": True, "checks": []},
                       "destination": {"skipped": True,
                                       "reason": "coordinates not supplied"}})
    assert "Verdict: **PASS**" not in md
    assert "PARTIAL" in md


def test_a_transient_table_is_scored_medium_with_the_reason_in_its_row():
    # plan.build records that a TRANSIENT/TEMPORARY table is planned as a
    # permanent Delta table. SUMMARY.md once scored it LOW with "structure
    # clones cleanly", which is the opposite of a caveat.
    scratch = dict(PLAN["can_migrate"][0], source_identifier="D.PUBLIC.SCRATCH",
                   target="D.PUBLIC.SCRATCH",
                   kind_warning="TRANSIENT table in Snowflake (no Fail-safe, "
                                "short Time Travel); it is planned as a "
                                "permanent Delta table, so confirm it is meant "
                                "to persist")
    plan = dict(PLAN, can_migrate=[scratch, PLAN["can_migrate"][1]])
    md = render_summary(plan, INV, None, None)
    row = next(l for l in md.splitlines() if "`D.PUBLIC.SCRATCH`" in l)
    assert "| MEDIUM |" in row, row
    assert "TRANSIENT" in row and "permanent" in row
    assert "column warning" not in row
    rollup = next(l for l in md.splitlines() if l.startswith("By risk:"))
    assert "**MEDIUM**" in rollup
