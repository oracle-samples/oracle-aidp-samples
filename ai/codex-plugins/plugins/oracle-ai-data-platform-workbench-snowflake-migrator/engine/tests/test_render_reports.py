"""The two headline reports: what is planned to move, and what got created."""
from snowflake_source.extract.census import KINDS
from report.render import (
    render_census, render_ddl_plan, render_inventory, render_maintenance,
    render_compute, render_planned_objects, render_soft_clone_summary,
)

PLAN = {
    "built_at": "2026-09-09T00:00:00+00:00",
    "bronze_catalog_prefix": None,
    "bronze_mapping": "Snowflake database -> AIDP Standard Catalog, schema -> schema",
    "waves": [["D.PUBLIC.ORDERS"], ["D.PUBLIC.ORDERS_VW"]],
    "cycles": [],
    "target_names": {"D.PUBLIC.ORDERS": "D.PUBLIC.ORDERS",
                     "D.PUBLIC.ORDERS_VW": "D.PUBLIC.ORDERS_VW"},
    "clone_targets": ["D.PUBLIC.ORDERS", "D.PUBLIC.ORDERS_VW"],
    "can_migrate": [
        {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "target": "D.PUBLIC.ORDERS", "rows": 100, "columns": 41},
        {"source_identifier": "D.PUBLIC.ORDERS_VW", "object_type": "VIEW",
         "target": "D.PUBLIC.ORDERS_VW", "rows": 100, "columns": 17}],
    "cannot_migrate": [
        {"source_identifier": "D.PUBLIC.J", "object_type": "TABLE",
         "category": "unmapped_type", "reason": "PAYLOAD: VARIANT semi-structured"},
        {"source_identifier": "D.PUBLIC.V2", "object_type": "VIEW",
         "category": "snowflake_only_sql", "reason": "uses QUALIFY (no Spark equiv)"},
        {"source_identifier": "D2.S.X", "object_type": "TABLE",
         "category": "restriction", "reason": "database D2 excluded (restriction: exclude_databases)"}],
    "restrictions_applied": {"exclude_databases": ["D2"]},
    "catalogs_to_create": ["D"],
    "schemas_to_create": [["D", "PUBLIC"]],
    "silver_gold_jobs": [
        {"name": "silver_D_PUBLIC", "layer": "SILVER", "enabled": False,
         "trigger": "MANUAL_NEVER_TRIGGERED", "reads_from": "D.PUBLIC",
         "body_status": "placeholder"},
        {"name": "gold_D_PUBLIC", "layer": "GOLD", "enabled": False,
         "trigger": "MANUAL_NEVER_TRIGGERED", "reads_from": "D.PUBLIC",
         "body_status": "placeholder"}],
    "dependency_source": "account_usage",
    "dependency_coverage_note": "authoritative",
    "summary": {"objects_inventoried": 5, "can_migrate": 2, "cannot_migrate": 3,
                "tables": 1, "views": 1, "catalogs": 1, "schemas": 1,
                "silver_gold_jobs": 2,
                "cannot_by_category": {"unmapped_type": 1, "snowflake_only_sql": 1,
                                       "restriction": 1}},
}


# --- "what are the objects planned to move" ------------------------------

def test_planned_report_leads_with_the_headline_counts():
    md = render_planned_objects(PLAN)
    assert "2" in md and "3" in md
    assert "planned to move" in md.lower()


def test_can_migrate_table_lists_object_type_and_target():
    md = render_planned_objects(PLAN)
    assert "D.PUBLIC.ORDERS" in md and "TABLE" in md
    assert "D.PUBLIC.ORDERS_VW" in md and "VIEW" in md


def test_cannot_migrate_gives_a_brief_reason_per_object():
    md = render_planned_objects(PLAN)
    assert "VARIANT" in md
    assert "QUALIFY" in md
    assert "database D2 excluded" in md


def test_cannot_migrate_is_grouped_by_category():
    md = render_planned_objects(PLAN)
    for cat in ("unmapped_type", "snowflake_only_sql", "restriction"):
        assert cat in md


def test_restrictions_in_force_are_shown():
    md = render_planned_objects(PLAN)
    assert "exclude_databases" in md


def test_bronze_mapping_is_stated_explicitly():
    md = render_planned_objects(PLAN)
    assert "Standard Catalog" in md


def test_silver_gold_jobs_shown_as_created_but_not_triggered():
    md = render_planned_objects(PLAN)
    assert "silver_D_PUBLIC" in md
    assert "not triggered" in md.lower() or "never triggered" in md.lower()


def test_catalogs_the_user_must_precreate_are_called_out():
    md = render_planned_objects(PLAN)
    assert "catalogs_to_create" in md.lower() or "Catalogs" in md


def test_waves_shown_with_views_last():
    md = render_planned_objects(PLAN)
    assert md.index("Wave 1") < md.index("Wave 2")


# --- "what has been created in the soft clone" ---------------------------

def test_soft_clone_summary_dry_run_says_nothing_created():
    md = render_soft_clone_summary(PLAN, {"dry_run": True, "statement_count": 2,
                                          "executed": 0, "verified": 0,
                                          "failed": [], "chunk_errors": [],
                                          "blocked_count": 0,
                                          "catalog_in_scope": "D",
                                          "out_of_scope_count": 0,
                                          "out_of_scope_catalogs": []})
    assert "DRY RUN" in md.upper()
    assert "nothing was created" in md.lower()


def test_soft_clone_summary_reports_verified_not_executed():
    md = render_soft_clone_summary(PLAN, {
        "dry_run": False, "statement_count": 2, "executed": 2, "verified": 1,
        "failed": [{"target_fqn": "D.PUBLIC.ORDERS_VW",
                    "reason": "not present after its chunk reported completion"}],
        "chunk_errors": [], "blocked_count": 0, "catalog_in_scope": "D",
        "out_of_scope_count": 0, "out_of_scope_catalogs": []})
    assert "verified" in md.lower()
    assert "1/2" in md or "1 of 2" in md
    assert "D.PUBLIC.ORDERS_VW" in md


def test_soft_clone_summary_says_the_tables_are_empty():
    md = render_soft_clone_summary(PLAN, {
        "dry_run": False, "statement_count": 2, "executed": 2, "verified": 2,
        "failed": [], "chunk_errors": [], "blocked_count": 0,
        "catalog_in_scope": "D", "out_of_scope_count": 0,
        "out_of_scope_catalogs": []})
    assert "empty" in md.lower() and "no data" in md.lower()


def test_soft_clone_summary_flags_out_of_scope_catalogs():
    md = render_soft_clone_summary(PLAN, {
        "dry_run": False, "statement_count": 1, "executed": 1, "verified": 1,
        "failed": [], "chunk_errors": [], "blocked_count": 0,
        "catalog_in_scope": "D", "out_of_scope_count": 4,
        "out_of_scope_catalogs": ["D2", "D3"]})
    assert "D2" in md and "D3" in md
    assert "separate" in md.lower() or "not deployed" in md.lower()


# --- compute proposal -----------------------------------------------------

def test_compute_report_lists_warehouses_and_proposals():
    md = render_compute({
        "warehouse_count": 1, "max_concurrent_clusters": 2,
        "total_source_nodes": 4, "observed_credits_total": 600.0,
        "credits_basis": "observed", "cost_model": None,
        "cost_note": "No cost model: a credit price was not supplied.",
        "proposals": [{"name": "W", "source_size": "Medium", "source_nodes": 4,
                       "worker_count": 2, "autoscale_max_workers": 4,
                       "worker_ocpus": 8, "worker_shape_family": "VM.Standard.E5.Flex",
                       "shape_confirmation_required": True, "notes": "confirm shape"}],
        "blocked": [{"name": "ODD", "reason": "unrecognised size 'Nano'"}]})
    assert "W" in md and "Medium" in md
    assert "VM.Standard.E5.Flex" in md
    assert "confirm" in md.lower()
    assert "ODD" in md and "Nano" in md
    assert "credit price" in md.lower()


# --------------------------------------------------------------------------
# Mismatch and unverified structure must be visible, not folded into
# "verified" or "failed".
# --------------------------------------------------------------------------

def test_soft_clone_summary_reports_a_structure_mismatch_prominently():
    res = {"dry_run": False, "statement_count": 2, "executed": 2, "verified": 1,
           "catalog_in_scope": "CAT",
           "failed": [], "chunk_errors": [],
           "mismatches": [{"source_identifier": "D.S.T", "target_fqn": "CAT.S.T",
                           "reason": "position 1: planned ID DECIMAL(38,0), "
                                     "found ID STRING"}],
           "unverified_structure": []}
    md = render_soft_clone_summary({"can_migrate": []}, res)
    assert "Structure differs" in md
    assert "CAT.S.T" in md
    assert "not been cloned" in md.lower() or "left as found" in md.lower()


def test_soft_clone_summary_reports_unverified_structure_separately():
    res = {"dry_run": False, "statement_count": 1, "executed": 1, "verified": 0,
           "catalog_in_scope": "CAT", "failed": [], "chunk_errors": [],
           "mismatches": [],
           "unverified_structure": [
               {"source_identifier": "D.S.T", "target_fqn": "CAT.S.T",
                "reason": "exists, but its structure could not be read"}]}
    md = render_soft_clone_summary({"can_migrate": []}, res)
    assert "Structure not verified" in md
    assert "CAT.S.T" in md


def test_verified_wording_says_structure_not_just_existence():
    res = {"dry_run": False, "statement_count": 1, "executed": 1, "verified": 1,
           "catalog_in_scope": "CAT", "failed": [], "chunk_errors": [],
           "mismatches": [], "unverified_structure": []}
    md = render_soft_clone_summary({"can_migrate": []}, res)
    assert "column" in md.lower()


def test_ddl_plan_reports_deferred_maintenance_settings():
    plan = {"statements": [
        {"source_identifier": "DB.SC.T", "object_type": "TABLE",
         "target_fqn": "CAT.SC.T", "sql": "CREATE TABLE ...",
         "rules_applied": [], "warnings": [], "omitted_properties": [],
         "deferred_properties": [
             {"property": "cluster_by", "value": "(ORDER_DATE, STORE_ID)",
              "aidp_equivalent": "Delta liquid clustering (`CLUSTER BY`) or "
                                 "`OPTIMIZE … ZORDER BY`"}]}],
        "blocked": []}
    md = render_ddl_plan(plan)
    assert "Maintenance and layout" in md
    assert "cluster_by" in md
    assert "(ORDER_DATE, STORE_ID)" in md
    assert "ZORDER" in md
    # And it must say plainly that nothing was applied.
    assert "not applied" in md.lower()


# --------------------------------------------------------------------------
# The maintenance report (M2).
# --------------------------------------------------------------------------

def _maint(**over):
    base = {
        "probed_at": "2026-09-10T00:00:00+00:00", "history_days": 30,
        "table_parameters_probed": False,
        "retention": {"account": {"data_retention_time_in_days": 1,
                                  "max_data_extension_time_in_days": 14,
                                  "set_at": "default"},
                      "databases": {"DB": 1}, "schemas": {"DB.PUBLIC": 1}},
        "account_usage": {"readable": True, "note": "summarised over 30 day(s)"},
        "tables": [], "objects_with_signals": 0,
        "no_equivalent": [{"capability": "Fail-safe", "snowflake": "7 days",
                           "impact": "No AIDP equivalent"}],
        "unreadable": [],
    }
    base.update(over)
    return base


def _mt(**over):
    base = {"source_identifier": "DB.PUBLIC.ORDERS", "cluster_by": "(ORDER_DATE)",
            "clustered": True, "automatic_clustering": True,
            "change_tracking": False, "search_optimization": False,
            "search_optimization_bytes": None, "retention_days": 7,
            "retention_set_at": "table", "retention_inherited_value": 1,
            "rows": 4_000_000, "bytes": 10**9,
            "reclustering": {"measured": True, "events": 12, "credits": 34.5,
                             "bytes_reclustered": 1, "rows_reclustered": 1},
            "dml_churn": {"measured": True, "rows_added": 10, "rows_removed": 5,
                          "rows_updated": 5, "rows_rewritten": 10, "windows": 30},
            "signals": [{"signal": "clustering key in use", "detail": "d",
                         "aidp_equivalent": "liquid clustering",
                         "aidp_requires": "a scheduled job"}]}
    base.update(over)
    return base


def test_maintenance_report_lists_tables_with_signals():
    md = render_maintenance(_maint(tables=[_mt()], objects_with_signals=1))
    assert "DB.PUBLIC.ORDERS" in md
    assert "clustering key in use" in md
    assert "scheduled job" in md


def test_maintenance_report_states_nothing_was_applied():
    md = render_maintenance(_maint(tables=[_mt()])).lower()
    assert "applies nothing" in md, "the report must say it changed nothing"
    # It used to say "none applied"; ddl now carries the table settings, and
    # the report says which path does and which does not.
    assert "carried into the create table by `ddl`" in md
    assert "applied by nobody" in md
    assert "proposes no cadence" in md


def test_unmeasured_history_is_never_shown_as_zero():
    md = render_maintenance(_maint(
        account_usage={"readable": False, "note": "Insufficient privileges"},
        tables=[_mt(reclustering={"measured": False, "events": None,
                                  "credits": None, "bytes_reclustered": None,
                                  "rows_reclustered": None})]))
    assert "not measured" in md.lower()
    assert "Insufficient privileges" in md
    # The credits column must not read as a real zero.
    assert "| 0 " not in md


def test_capabilities_with_no_equivalent_are_named():
    md = render_maintenance(_maint())
    assert "Fail-safe" in md
    assert "No equivalent" in md or "no equivalent" in md


def test_a_clean_estate_says_so_rather_than_rendering_an_empty_table():
    md = render_maintenance(_maint(tables=[_mt(
        clustered=False, cluster_by="", automatic_clustering=False,
        retention_set_at="inherited", signals=[])], objects_with_signals=0))
    assert "no maintenance" in md.lower() or "nothing" in md.lower()


# --------------------------------------------------------------------------
# The census scope statement must reach the reports that state coverage.
# "7 of 7 objects can move" was true of what was looked at.
# --------------------------------------------------------------------------

_CENSUS = {
    "total": 3, "by_kind": {"PROCEDURE": 2, "TASK": 1},
    "by_language": {"SQL": 1, "JAVASCRIPT": 1}, "by_effort": {"HIGH": 1},
    "kinds": {"PROCEDURE": {"count": 2, "readable": True, "note": "2 found"},
              "TASK": {"count": 1, "readable": True, "note": "1 found"}},
    "objects": [
        {"kind": "PROCEDURE", "source_identifier": "DB.SC.SP_LOAD",
         "detail": "(A VARCHAR)", "migratable": False, "reason": "code",
         "language": "JAVASCRIPT", "effort": "HIGH", "aidp_path": "rewrite"},
        {"kind": "TASK", "source_identifier": "DB.SC.T_NIGHTLY",
         "detail": "state=started", "migratable": False,
         # The real reason from KINDS, so this tests the shipped text.
         "reason": next(k["reason"] for k in KINDS if k["kind"] == "TASK"),
         "language": None, "effort": None, "aidp_path": None}],
    "unreadable": [],
    "scope_statement": "**3 object(s) in this estate cannot be migrated by "
                       "this plugin**: 2 procedure(s), 1 task(s).",
}


def test_planned_objects_carries_the_scope_statement():
    plan = dict(PLAN)
    plan["census"] = _CENSUS
    md = render_planned_objects(plan)
    assert "cannot be migrated by this plugin" in md
    assert "procedure(s)" in md


def test_planned_objects_without_a_census_says_scope_was_not_examined():
    # Silence is the bug being fixed: if the census did not run, the coverage
    # claim must not read as if the whole estate was examined.
    md = render_planned_objects(dict(PLAN))
    assert "tables and views" in md.lower()


def test_the_census_report_lists_objects_with_their_effort():
    md = render_census(_CENSUS)
    assert "DB.SC.SP_LOAD" in md
    assert "JAVASCRIPT" in md
    assert "HIGH" in md
    assert "DB.SC.T_NIGHTLY" in md


def test_the_census_report_says_nothing_is_migratable():
    md = render_census(_CENSUS).lower()
    assert "cannot" in md or "not migrat" in md
    assert "no procedure or udf equivalent is generated" in md


def test_a_task_gets_the_cutover_warning():
    md = render_census(_CENSUS)
    assert "stops being populated" in md or "stops being" in md


# --------------------------------------------------------------------------
# SECURITY.md: a policy object that exists while POLICY_REFERENCES (up to
# ~2 h stale) shows no attachment must not be rendered as an all-clear.
# --------------------------------------------------------------------------

def _security(**over):
    base = {"statement": "s", "exposures": [], "exposure_count": 0,
            "secure_views": [], "unreadable": [],
            "policy_references_readable": True,
            "policies": {"masking": {"count": 1, "readable": True, "note": ""},
                         "row_access": {"count": 0, "readable": True, "note": ""},
                         "tags": {"count": 0, "readable": True, "note": ""}},
            "grants": {"measured": False}}
    base.update(over)
    return base


def test_security_report_flags_a_defined_but_unattached_policy():
    from report.render import render_security
    md = render_security(_security(policies_defined_without_attachment=1))
    assert "Defined is not the same as attached" not in md, \
        "that sentence tells the reader to disregard the only contradicting signal"
    assert "not seen attached" in md and "all-clear" in md


def test_security_report_keeps_the_plain_sentence_when_attachments_are_accounted_for():
    from report.render import render_security
    md = render_security(_security(policies_defined_without_attachment=0))
    assert "Defined is not the same as attached" in md
    assert "all-clear" not in md


def test_planned_objects_with_an_empty_census_carries_the_visibility_caveat():
    # The scope statement travels into PLANNED_OBJECTS.md and SUMMARY.md, so
    # the "whole estate" claim would travel with it.
    from fake_sql import FakeSql
    from snowflake_source.extract.census import build_census
    empty = {"information_schema.procedures": [], "information_schema.functions": [],
             "information_schema.sequences": [], "information_schema.stages": [],
             "information_schema.file_formats": [], "information_schema.pipes": [],
             "show tasks": [], "show streams": [], "show materialized views": [],
             "show dynamic tables": [], "show alerts": [], "show secrets": [],
             "show network rules": [], "show streamlits": [],
             "show notebooks": [], "show services": [], "show shares": [],
             "show roles": [], "show network policies": [],
             "show applications": [], "show compute pools": [],
             "show replication groups": []}
    plan = dict(PLAN)
    plan["census"] = build_census(FakeSql(empty), ["DB"], role="R")
    md = render_planned_objects(plan)
    assert "whole estate" not in md.lower()
    assert "visible" in md.lower()


def test_dependency_not_migrated_has_a_title_not_a_raw_key():
    plan = dict(PLAN)
    plan["cannot_migrate"] = PLAN["cannot_migrate"] + [
        {"source_identifier": "D.PUBLIC.J_VW", "object_type": "VIEW",
         "category": "dependency_not_migrated",
         "reason": "depends on D.PUBLIC.J, which is blocked (unmapped_type)"}]
    md = render_planned_objects(plan)
    assert "### Depends on an object that is not migrating" in md
    assert "depends on D.PUBLIC.J, which is blocked" in md


# plan.json may carry `target_catalog_note` (how the target catalog comes to
# exist: a container at S4, structure at S10). It is the approval artifact's
# business to show it, and an older plan.json without it must still render.
# --------------------------------------------------------------------------

def test_planned_objects_renders_the_target_catalog_note_when_present():
    note = ("The target catalog is created as a CONTAINER at S4 by "
            "`catalog --catalog-type standard --execute`; its schemas and "
            "tables are created at S10 on AIDP compute.")
    md = render_planned_objects(dict(PLAN, target_catalog_note=note))
    assert note in md
    structure = md.split("## Target structure to exist first", 1)[1]
    assert note in structure.split("\n## ", 1)[0], \
        "the note belongs with the target-structure section"


def test_planned_objects_without_the_note_still_renders():
    plan = {k: v for k, v in PLAN.items() if k != "target_catalog_note"}
    md = render_planned_objects(plan)
    assert "Target structure to exist first" in md
    assert "None" not in md.split("## Target structure to exist first", 1)[1].split("\n## ", 1)[0]


# --------------------------------------------------------------------------
# plan.json carries `table_kind_warnings` (TRANSIENT/TEMPORARY tables planned
# as permanent Delta tables). The sign-off artifact must show them; a warning
# nobody renders is not a warning.
# --------------------------------------------------------------------------

_KIND_WARNING = {"source_identifier": "D.PUBLIC.SCRATCH", "kind": "TRANSIENT",
                 "warning": "TRANSIENT table in Snowflake (no Fail-safe, short "
                            "Time Travel); it is planned as a permanent Delta "
                            "table, so confirm it is meant to persist"}


def test_planned_objects_lists_tables_planned_as_something_else():
    md = render_planned_objects(dict(PLAN, table_kind_warnings=[_KIND_WARNING]))
    assert "## Planned, but not as what they were" in md
    section = md.split("## Planned, but not as what they were", 1)[1].split("\n## ", 1)[0]
    assert "`D.PUBLIC.SCRATCH`" in section and "TRANSIENT" in section
    assert "permanent" in section


def test_planned_objects_has_no_kind_section_when_nothing_changes_kind():
    assert "Planned, but not as what they were" not in render_planned_objects(PLAN)
    assert "Planned, but not as what they were" not in render_planned_objects(
        dict(PLAN, table_kind_warnings=[]))


# --------------------------------------------------------------------------
# "Dependencies land before their dependents, so views follow their base
# tables" is only true of views that HAVE an edge. A view with no edge from
# either source sorts by size and can land before its base table; the plan
# must say so instead of asserting an order it does not have.
# --------------------------------------------------------------------------

def _order_section(md):
    return md.split("## Order of creation", 1)[1].split("\n## ", 1)[0]


def test_order_of_creation_names_views_with_no_dependency_edge():
    warning = ("1 view(s) have no ACCOUNT_USAGE lineage edge (the view lags "
               "DDL by up to ~3 h); their DDL was parsed instead. 1 still have "
               "no edge from either source and are ordered by size only: "
               "D.PUBLIC.ORDERS_VW. Re-run `deps` after the lag before relying "
               "on the wave order.")
    md = render_planned_objects(dict(
        PLAN, dependency_source="account_usage_empty",
        views_without_dependency_edge=["D.PUBLIC.ORDERS_VW"],
        dependency_warning=warning, dependency_edge_count=0))
    section = _order_section(md)
    assert "so views follow their base tables" not in md
    assert "`D.PUBLIC.ORDERS_VW`" in section
    assert "size only" in section
    assert "NOT guaranteed" in section
    assert warning in md


def test_order_of_creation_flags_unordered_views_even_without_a_producer_warning():
    # ACCOUNT_USAGE denied -> parsed_ddl with warning None, and a view whose
    # only reference lies outside the inventory still has no edge.
    md = render_planned_objects(dict(
        PLAN, dependency_source="parsed_ddl",
        views_without_dependency_edge=["D.PUBLIC.ORDERS_VW"],
        dependency_warning=None))
    assert "so views follow their base tables" not in md
    assert "`D.PUBLIC.ORDERS_VW`" in _order_section(md)
    assert "None" not in _order_section(md)


def test_order_of_creation_keeps_the_plain_sentence_when_every_view_has_an_edge():
    assert "so views follow their base tables" in render_planned_objects(PLAN)
    assert "so views follow their base tables" in render_planned_objects(
        dict(PLAN, views_without_dependency_edge=[], dependency_warning=None))


# --------------------------------------------------------------------------
# "## Target structure to exist first" must not tell the reader, in three
# consecutive lines, to create catalog `d`, that `d` is the EXTERNAL pointer
# and not the target, and that the clone creates `d.public`. It also must
# name the schemas the structure job creates: since S10 takes the approved
# target_fqn as it stands, that is the plan's `catalog.schema`, not the
# source schema (these two tests once pinned `public` / "Older deploy path
# only", which described the job before that fix; test_target_catalog_note.py
# runs the job itself).
# --------------------------------------------------------------------------

from plan.build import _target_catalog_note  # noqa: E402

_DEFAULT_NOTE = _target_catalog_note(["d"], None, "db_schema")
_PREFIX_NOTE = _target_catalog_note(["lake"], "lake", "db_schema")


def _structure_section(md):
    return md.split("## Target structure to exist first", 1)[1].split("\n## ", 1)[0]


def test_default_mode_target_structure_does_not_ask_for_the_mirrored_catalog():
    md = render_planned_objects(dict(PLAN, target_catalog_note=_DEFAULT_NOTE,
                                     catalogs_to_create=["d"],
                                     schemas_to_create=[["d", "public"]]))
    section = _structure_section(md)
    assert "create these" not in section, section
    assert "Catalogs" in section, "the word test :94 pins must survive"
    assert "not a catalog to create" in section
    assert _DEFAULT_NOTE in section
    names = next(l for l in section.splitlines()
                 if l.startswith("Schemas the plan names"))
    assert "`d.public`" in names
    assert "`plan --bronze-catalog-prefix" in names
    assert "Older" not in section
    assert "Schemas the clone will create" not in section


def test_prefix_mode_target_structure_names_the_catalog_to_create_and_the_planned_schema():
    md = render_planned_objects(dict(PLAN, bronze_catalog_prefix="lake",
                                     target_catalog_note=_PREFIX_NOTE,
                                     catalogs_to_create=["lake"],
                                     schemas_to_create=[["lake", "d_public"]]))
    section = _structure_section(md)
    assert "create these" in section and "`lake`" in section
    assert _PREFIX_NOTE in section
    structure_line = next(l for l in section.splitlines()
                          if l.startswith("Schemas the structure job"))
    assert "`lake.d_public`" in structure_line
    assert "Older" not in section
    assert "Schemas the clone will create" not in section


def test_a_plan_without_the_note_keeps_the_legacy_two_lines():
    plan = {k: v for k, v in PLAN.items() if k != "target_catalog_note"}
    section = _structure_section(render_planned_objects(plan))
    assert "create these" in section
    assert "Schemas the clone will create: `D.PUBLIC`" in section


# --------------------------------------------------------------------------
# SECURITY.md: the policy-object table, tag attachments and grant classes.
# I3 again -- a kind nobody asked for and a kind the role cannot see are two
# different cells, and neither of them is a zero.
# --------------------------------------------------------------------------

def _all_kinds(**over):
    base = {"masking": {"count": 0, "readable": True, "note": ""},
            "row_access": {"count": 0, "readable": True, "note": ""},
            "aggregation": {"count": 0, "readable": True, "note": ""},
            "projection": {"count": 0, "readable": True, "note": ""},
            "tags": {"count": 0, "readable": True, "note": ""}}
    base.update(over)
    return base


def test_security_report_lists_aggregation_and_projection_policy_objects():
    from report.render import render_security
    md = render_security(_security(policies=_all_kinds(
        projection={"count": 2, "readable": True, "note": ""})))
    assert "| Aggregation |" in md
    assert "| Projection | 2" in md


def test_security_report_shows_a_denied_kind_as_not_visible_not_zero():
    from report.render import render_security
    md = render_security(_security(policies=_all_kinds(
        aggregation={"count": None, "readable": False, "note": "denied"})))
    assert "not visible to this role" in md
    assert "| Aggregation | 0" not in md


def test_security_report_says_when_a_kind_was_never_enumerated():
    # _security()'s payload predates aggregation/projection enumeration, so
    # the report must say those kinds were never asked for rather than
    # leaving the reader to assume they were covered.
    from report.render import render_security
    md = render_security(_security())
    assert "not enumerated" in md
    assert "never asked for" in md


def test_security_report_lists_tag_attachments():
    from report.render import render_security
    md = render_security(_security(tag_references={
        "measured": True, "count": 1, "out_of_scope": 0, "carried_over": False,
        "source": "SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES (lags up to ~2 "
                  "hours behind DDL)",
        "attachments": [{"object": "DB.SC.CUSTOMERS", "column": "EMAIL",
                         "domain": "COLUMN", "tag": "DB.SC.PII",
                         "value": "HIGH",
                         "consequence": "the tag does not travel",
                         "aidp_path": "ontology sensitivity classification"}]}))
    assert "Tag attachments" in md
    assert "DB.SC.PII" in md and "DB.SC.CUSTOMERS" in md
    assert "lags up to ~2 hours" in md


def test_security_report_never_reports_zero_tag_attachments_when_unreadable():
    from report.render import render_security
    md = render_security(_security(tag_references={
        "measured": False, "count": None, "attachments": [],
        "out_of_scope": 0, "carried_over": False,
        "source": "SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES",
        "note": "Insufficient privileges"}))
    assert "Tag attachments" in md
    assert "Not measured" in md
    assert "0 tag attachment" not in md


def test_security_report_names_the_grant_classes_it_covered():
    from report.render import render_security
    md = render_security(_security(grants={
        "measured": True, "by_object": {}, "carried_over": False,
        "classes_requested": ["TABLE", "VIEW", "SCHEMA", "WAREHOUSE"],
        "by_class": {"SCHEMA": {"grants": 2, "roles": ["LOADER"],
                                "objects": 1}},
        "out_of_scope": 0, "note": "0 in-scope object(s)"}))
    assert "SCHEMA" in md and "WAREHOUSE" in md
    assert "No grant is replayed" in md


# --- the deploy report says what the deploy result records ---------------
# catalog_deploy records a NOT NULL the catalog API cannot carry and a
# COMMENT the target dropped; the summary used to print neither, so the
# operator read "verified" and nothing else.

_DEPLOYED = {"dry_run": False, "statement_count": 1, "executed": 1, "verified": 1,
             "failed": [], "chunk_errors": [], "blocked_count": 0,
             "catalog_in_scope": "D", "out_of_scope_count": 0,
             "out_of_scope_catalogs": []}


def test_soft_clone_summary_names_the_properties_the_api_cannot_carry():
    md = render_soft_clone_summary(PLAN, {
        **_DEPLOYED,
        "properties_not_applied": [{
            "source_identifier": "D.PUBLIC.ORDERS", "target_fqn": "d.public.orders",
            "property": "NOT NULL", "columns": ["ORDER_ID"],
            "reason": "ORDER_ID are NOT NULL in the reviewed plan and are "
                      "created NULLABLE here: the field entry has no nullability key"}]})
    assert "## Properties this transport cannot carry" in md
    assert "`d.public.orders` — NOT NULL:" in md and "R21" in md


def test_soft_clone_summary_names_a_dropped_description():
    md = render_soft_clone_summary(PLAN, {
        **_DEPLOYED,
        "description_drift": [{
            "source_identifier": "D.PUBLIC.ORDERS", "target_fqn": "d.public.orders",
            "reason": "created with the planned columns, but table COMMENT: "
                      "planned 'orders' found ''."}]})
    assert "## Descriptions not found on the target" in md
    assert "`d.public.orders`" in md and "table COMMENT" in md


def test_soft_clone_summary_is_silent_when_nothing_was_dropped():
    md = render_soft_clone_summary(PLAN, dict(_DEPLOYED))
    assert "cannot carry" not in md and "Descriptions not found" not in md


# ----------------------------- the inventory headline agrees with the run
#
# Live 2026-09-23, two databases in scope and one of them wholly unreadable:
#
#     role `SNOWMIG_LIMITED`
#     Databases in scope: SNOWMIG_COVERAGE, SNOWMIG_COV_B
#     **16 objects** - TABLE 8 - VIEW 8
#
# Both lines mislead in the same direction as the census bug. The role line
# named an authority that, without --only-primary-role, also held
# ACCOUNTADMIN. The scope line listed two databases beside a count drawn from
# one. The refusal was disclosed thirty-five lines lower, under Extraction
# notes, where a reader who has taken the headline does not go.

def _inv(**over):
    base = {
        "probed_at": "2026-09-23T00:00:00Z",
        "session": {"A": "ACC", "R": "REG", "ROLE": "LIMITED"},
        "databases_in_scope": ["DB_A", "DB_B"],
        "object_count": 8,
        "counts_by_type": {"TABLE": 8},
        "inventory": [],
        "extraction_notes": [],
    }
    base.update(over)
    return base


def test_a_database_that_could_not_be_read_is_marked_in_the_scope_line():
    md = render_inventory(_inv(extraction_notes=[
        "database DB_B: 002043 (02000): SQL compilation error"]))
    scope = next(l for l in md.splitlines() if l.startswith("Databases in scope"))
    assert "DB_B (**NOT READ**)" in scope, scope
    assert "DB_A (**NOT READ**)" not in scope


def test_the_count_says_it_does_not_cover_the_refused_database():
    md = render_inventory(_inv(extraction_notes=[
        "database DB_B: 002043 (02000): SQL compilation error"]))
    assert "could not be read at all" in md
    assert "privilege result, not an empty database" in md


def test_a_fully_readable_estate_gets_no_such_warning():
    md = render_inventory(_inv())
    assert "NOT READ" not in md
    assert "could not be read at all" not in md


def test_an_object_level_note_is_not_mistaken_for_a_refused_database():
    """The extractor writes both kinds of note into the same list."""
    md = render_inventory(_inv(extraction_notes=[
        "DB_A.PUBLIC.V1: GET_DDL failed: 002043 (02000)"]))
    assert "NOT READ" not in md


def test_the_role_line_names_the_secondary_roles_that_served_the_reads():
    md = render_inventory(_inv(session={
        "A": "ACC", "R": "REG", "ROLE": "LIMITED",
        "SECONDARY_ROLES": '{"roles":"ACCOUNTADMIN","value":"ALL"}'}))
    role_line = next(l for l in md.splitlines() if l.startswith("Probed"))
    assert "ACCOUNTADMIN" in role_line, role_line
    assert "secondary" in role_line.lower()


def test_no_secondary_roles_leaves_the_role_line_as_it_was():
    md = render_inventory(_inv(session={
        "A": "ACC", "R": "REG", "ROLE": "LIMITED",
        "SECONDARY_ROLES": '{"roles":"","value":""}'}))
    role_line = next(l for l in md.splitlines() if l.startswith("Probed"))
    assert "secondary" not in role_line.lower()
    assert "`LIMITED`" in role_line
