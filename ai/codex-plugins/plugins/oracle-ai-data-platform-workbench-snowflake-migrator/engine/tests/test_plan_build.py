"""plan.json assembly: bronze mirror, can/cannot with reasons, restrictions."""
import pytest

from plan.build import TargetCollision, build_plan

PORTABLE_VIEW = ("create view V as select a from D.PUBLIC.T")
SNOWFLAKE_VIEW = ("create view V2 as select * from D.PUBLIC.T "
                  "qualify row_number() over (order by a) = 1")


def rec(ident, kind="TABLE", rows=10, status="supported", blocked=(), ddl=None):
    db, schema, name = ident.split(".")
    r = {"source_identifier": ident, "object_type": kind,
         "source_database": db, "source_schema": schema,
         "compatibility_status": status, "blocked_reasons": list(blocked),
         "row_count_exact": rows, "source_metadata": {"bytes": rows * 10}}
    if kind == "VIEW":
        r["view_ddl_get_ddl"] = ddl or PORTABLE_VIEW
    return r


# --- bronze mirror --------------------------------------------------------

def test_bronze_target_mirrors_the_source_three_part_name():
    plan = build_plan({"inventory": [rec("MYDB.SALES.ORDERS")]}, {"edges": []})
    # AIDP folds identifiers, so the PLANNED target is the folded name.
    assert plan["target_names"]["MYDB.SALES.ORDERS"] == "mydb.sales.orders"


def test_catalogs_and_schemas_to_create_are_derived():
    inv = {"inventory": [rec("D1.S1.A"), rec("D1.S2.B"), rec("D2.S1.C")]}
    plan = build_plan(inv, {"edges": []})
    assert plan["catalogs_to_create"] == ["d1", "d2"]
    assert plan["schemas_to_create"] == [["d1", "s1"], ["d1", "s2"], ["d2", "s1"]]


def test_catalog_prefix_mode_recorded():
    plan = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []},
                      bronze_catalog_prefix="bronze")
    assert plan["target_names"]["D.S.T"] == "bronze.d_s.t"
    assert plan["bronze_catalog_prefix"] == "bronze"


# --- views are in scope now -----------------------------------------------

def test_portable_view_can_migrate():
    inv = {"inventory": [rec("D.PUBLIC.T"), rec("D.PUBLIC.V", kind="VIEW")]}
    plan = build_plan(inv, {"edges": [{"from": "D.PUBLIC.V", "to": "D.PUBLIC.T"}]})
    can = {c["source_identifier"] for c in plan["can_migrate"]}
    assert can == {"D.PUBLIC.T", "D.PUBLIC.V"}
    assert "D.PUBLIC.V" in plan["clone_targets"], "views are cloned now"


def test_view_with_snowflake_only_sql_cannot_migrate_with_the_construct_named():
    inv = {"inventory": [rec("D.PUBLIC.V2", kind="VIEW", ddl=SNOWFLAKE_VIEW)]}
    plan = build_plan(inv, {"edges": []})
    cannot = plan["cannot_migrate"]
    assert [c["source_identifier"] for c in cannot] == ["D.PUBLIC.V2"]
    assert "QUALIFY" in cannot[0]["reason"]
    assert cannot[0]["category"] == "snowflake_only_sql"


def test_views_ordered_after_their_base_tables():
    inv = {"inventory": [rec("D.PUBLIC.T"), rec("D.PUBLIC.V", kind="VIEW")]}
    plan = build_plan(inv, {"edges": [{"from": "D.PUBLIC.V", "to": "D.PUBLIC.T"}]})
    assert plan["waves"] == [["D.PUBLIC.T"], ["D.PUBLIC.V"]]


# --- can / cannot with reasons -------------------------------------------

def test_unmapped_column_type_cannot_migrate():
    inv = {"inventory": [rec("D.S.J", status="blocked",
                             blocked=["PAYLOAD: VARIANT semi-structured"])]}
    plan = build_plan(inv, {"edges": []})
    c = plan["cannot_migrate"][0]
    assert c["category"] == "unmapped_type"
    assert "VARIANT" in c["reason"]


def test_can_migrate_entries_carry_the_target_and_type():
    plan = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []})
    c = plan["can_migrate"][0]
    assert c["target"] == "d.s.t" and c["object_type"] == "TABLE"


def test_can_migrate_entries_carry_the_risk_bearing_facts():
    # SUMMARY.md scores risk from the plan entry alone. The column warnings
    # and the maintenance settings ddl will defer must therefore travel on
    # it, or a clustered table with a timezone caveat reads LOW.
    r = rec("D.S.T")
    r["warnings"] = ["TS: TIMESTAMP_NTZ -> TIMESTAMP: TIMEZONE SEMANTICS DIFFER"]
    r["source_metadata"].update({"cluster_by": "LINEAR(ORDER_DATE)",
                                 "change_tracking": "ON", "is_secure": "false",
                                 "owner": "SYSADMIN", "retention_time": "7"})
    plan = build_plan({"inventory": [r]}, {"edges": []})
    c = plan["can_migrate"][0]
    assert c["warnings"] == r["warnings"]
    # ddl carries change tracking and a 7-day retention into the CREATE
    # TABLE now; the clustering key names a column this record does not
    # have, so it is the one that stays a deferred decision.
    assert [d["property"] for d in c["deferred_properties"]] == ["cluster_by"]
    assert all(d["aidp_equivalent"] for d in c["deferred_properties"])
    assert c["omitted_properties"] == [], "is_secure=false is unset, owner is informational"


def test_a_view_carries_no_maintenance_facts_because_ddl_reports_none_for_it():
    # SHOW VIEWS reports change_tracking too and catalog.py records it, but
    # ddl scans source_metadata only in build_create_table; build_create_view
    # reads is_secure/is_materialized alone. SUMMARY.md and DDL_PLAN.md must
    # name the same settings, so a view's plan entry carries none -- or
    # SUMMARY.md says "change_tracking=ON not applied" for a view whose DDL
    # plan says nothing of the kind.
    v = rec("D.PUBLIC.V", kind="VIEW")
    v["source_metadata"].update({"change_tracking": "ON", "retention_time": "1"})
    plan = build_plan({"inventory": [rec("D.PUBLIC.T"), v]}, {"edges": []})
    c = next(c for c in plan["can_migrate"] if c["source_identifier"] == "D.PUBLIC.V")
    assert c["deferred_properties"] == [] and c["omitted_properties"] == []


def test_a_plain_record_still_carries_empty_facts():
    c = build_plan({"inventory": [rec("D.S.T")]}, {"edges": []})["can_migrate"][0]
    assert c["warnings"] == [] and c["deferred_properties"] == []
    assert c["omitted_properties"] == []


def test_every_object_appears_in_exactly_one_of_can_or_cannot():
    inv = {"inventory": [rec("D.S.OK"), rec("D.S.BAD", status="blocked",
                                            blocked=["x: VARIANT"]),
                         rec("D.S.V2", kind="VIEW", ddl=SNOWFLAKE_VIEW)]}
    plan = build_plan(inv, {"edges": []})
    ids = ([c["source_identifier"] for c in plan["can_migrate"]]
           + [c["source_identifier"] for c in plan["cannot_migrate"]])
    assert sorted(ids) == ["D.S.BAD", "D.S.OK", "D.S.V2"]
    assert len(ids) == len(set(ids))


# --- restrictions ---------------------------------------------------------

def test_restriction_exclusions_appear_in_cannot_migrate():
    inv = {"inventory": [rec("D1.S.A"), rec("D2.S.B")]}
    plan = build_plan(inv, {"edges": []},
                      restrictions={"exclude_databases": ["D2"]})
    c = next(c for c in plan["cannot_migrate"] if c["source_identifier"] == "D2.S.B")
    assert c["category"] == "restriction"
    assert "D2 excluded" in c["reason"]
    assert plan["restrictions_applied"] == {"exclude_databases": ["D2"]}


def test_restricted_objects_are_not_in_waves_or_clone_targets():
    inv = {"inventory": [rec("D1.S.A"), rec("D2.S.B")]}
    plan = build_plan(inv, {"edges": []},
                      restrictions={"exclude_databases": ["D2"]})
    assert plan["waves"] == [["D1.S.A"]]
    assert plan["clone_targets"] == ["D1.S.A"]


# --- silver / gold jobs ---------------------------------------------------

def test_silver_and_gold_jobs_created_per_schema_and_never_triggered():
    inv = {"inventory": [rec("D.S1.A"), rec("D.S2.B")]}
    plan = build_plan(inv, {"edges": []})
    jobs = plan["silver_gold_jobs"]
    assert len(jobs) == 4
    assert all(j["enabled"] is False for j in jobs)
    assert all(j["trigger"] == "MANUAL_NEVER_TRIGGERED" for j in jobs)


def test_no_jobs_for_schemas_with_nothing_migratable():
    inv = {"inventory": [rec("D.S.BAD", status="blocked", blocked=["x: VARIANT"])]}
    plan = build_plan(inv, {"edges": []})
    assert plan["silver_gold_jobs"] == []


# --- summary + safety -----------------------------------------------------

def test_summary_counts_match_the_lists():
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V", kind="VIEW"),
                         rec("D.S.BAD", status="blocked", blocked=["x: VARIANT"])]}
    plan = build_plan(inv, {"edges": []})
    s = plan["summary"]
    assert s["can_migrate"] == 2 and s["cannot_migrate"] == 1
    assert s["tables"] == 1 and s["views"] == 1
    assert s["catalogs"] == 1 and s["schemas"] == 1


def test_target_collision_still_halts():
    inv = {"inventory": [rec("D.S.T"), rec("D.s.T")]}
    with pytest.raises(TargetCollision) as exc:
        build_plan(inv, {"edges": []})
    # The HALT names its one in-tool remedy in the form the JSON file takes:
    # the twin to defer, spelled exactly, double-quoted.
    msg = str(exc.value)
    assert "exclude_objects" in msg
    assert '\\"D\\".\\"s\\".\\"T\\"' in msg


def test_dependency_provenance_carried():
    plan = build_plan({"inventory": [rec("D.S.T")]},
                      {"edges": [], "source_used": "parsed_ddl",
                       "coverage_note": "view edges ONLY"})
    assert plan["dependency_source"] == "parsed_ddl"
    assert "ONLY" in plan["dependency_coverage_note"]


# --- one bad view must not abort the plan -----------------------------------

def test_a_view_with_an_escaped_quote_in_a_cast_does_not_abort_the_plan():
    bad = "create view V_BAD as select 'don\\'t'::string as w, a from D.PUBLIC.T"
    inv = {"inventory": [rec("D.PUBLIC.T"),
                         rec("D.PUBLIC.V_BAD", kind="VIEW", ddl=bad)]}
    plan = build_plan(inv, {"edges": []})
    can = {c["source_identifier"] for c in plan["can_migrate"]}
    assert "D.PUBLIC.V_BAD" in can, plan["cannot_migrate"]


def test_collision_resolved_by_excluding_the_quoted_twin():
    # The HALT above has exactly one in-tool remedy: name the twin to defer
    # with its exact, double-quoted spelling. The other twin is then planned.
    inv = {"inventory": [rec("D.S.T"), rec("D.S.t")]}
    plan = build_plan(inv, {"edges": []},
                      restrictions={"exclude_objects": ['"D"."S"."t"']})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == ["D.S.T"]
    assert [(c["source_identifier"], c["category"])
            for c in plan["cannot_migrate"]] == [("D.S.t", "restriction")]


def test_unknown_count_under_a_cap_lands_in_cannot_migrate_with_the_reason():
    # Views carry no count under the default --row-counts metadata; a cap
    # that cannot be evaluated excludes rather than silently admitting.
    uncounted = rec("D.S.T")
    uncounted["row_count_exact"] = None
    inv = {"inventory": [uncounted, rec("D.S.SMALL", rows=3)]}
    plan = build_plan(inv, {"edges": []}, restrictions={"max_rows": 10})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == ["D.S.SMALL"]
    c = plan["cannot_migrate"][0]
    assert c["category"] == "restriction"
    assert "cannot be evaluated" in c["reason"] and "max_rows" in c["reason"]


# --- dependency cascade ---------------------------------------------------
#
# A view whose base object is not migrating cannot migrate either. Before,
# only the planned ids reached compute_waves, so the edge to a blocked or
# excluded base was dropped, the view had indegree 0, sorted FIRST in wave 1
# (rows=None -> size 0) and its CREATE VIEW was emitted over a table that
# will never exist -- while the report said "views follow their base tables".

def _edge(view, base):
    return {"from": view, "to": base}


def test_view_over_a_blocked_table_cannot_migrate_and_is_not_waved_or_emitted():
    from target.ddl import build_ddl_payload
    inv = {"inventory": [rec("D.S.T", status="blocked", blocked=["P: VARIANT"]),
                         rec("D.S.V", kind="VIEW", ddl="create view V as select a from D.S.T")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V", "D.S.T")]})
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    assert cannot["D.S.V"]["category"] == "dependency_not_migrated"
    assert "depends on D.S.T, which is blocked" in cannot["D.S.V"]["reason"]
    assert cannot["D.S.V"]["object_type"] == "VIEW"
    assert plan["can_migrate"] == [] and plan["waves"] == []
    assert plan["clone_targets"] == []
    assert plan["summary"]["can_migrate"] == 0
    assert plan["summary"]["cannot_migrate"] == 2
    assert plan["summary"]["cannot_by_category"]["dependency_not_migrated"] == 1
    assert build_ddl_payload(inv, plan)["statements"] == []


def test_view_over_a_restricted_table_cannot_migrate():
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V", kind="VIEW",
                                           ddl="create view V as select a from D.S.T")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V", "D.S.T")]},
                      restrictions={"exclude_objects": ["D.S.T"]})
    cats = {c["source_identifier"]: c["category"] for c in plan["cannot_migrate"]}
    assert cats == {"D.S.T": "restriction", "D.S.V": "dependency_not_migrated"}
    v = next(c for c in plan["cannot_migrate"] if c["source_identifier"] == "D.S.V")
    assert "depends on D.S.T, which is excluded" in v["reason"]


def test_dependency_exclusion_cascades_through_a_view_chain():
    inv = {"inventory": [rec("D.S.T", status="blocked", blocked=["P: VARIANT"]),
                         rec("D.S.V1", kind="VIEW", ddl="create view V1 as select a from D.S.T"),
                         rec("D.S.V2", kind="VIEW", ddl="create view V2 as select a from D.S.V1"),
                         rec("D.S.OK")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V1", "D.S.T"),
                                      _edge("D.S.V2", "D.S.V1")]})
    cats = {c["source_identifier"]: c["category"] for c in plan["cannot_migrate"]}
    assert cats["D.S.V1"] == cats["D.S.V2"] == "dependency_not_migrated"
    v2 = next(c for c in plan["cannot_migrate"] if c["source_identifier"] == "D.S.V2")
    assert "depends on D.S.V1, which is blocked" in v2["reason"]
    assert plan["waves"] == [["D.S.OK"]]


def test_dependency_exclusion_through_a_diamond_lists_each_view_once():
    inv = {"inventory": [rec("D.S.T", status="blocked", blocked=["P: VARIANT"]),
                         rec("D.S.V1", kind="VIEW", ddl="create view V1 as select a from D.S.T"),
                         rec("D.S.V2", kind="VIEW", ddl="create view V2 as select a from D.S.T"),
                         rec("D.S.V3", kind="VIEW",
                             ddl="create view V3 as select a from D.S.V1 join D.S.V2 on 1=1")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V1", "D.S.T"), _edge("D.S.V2", "D.S.T"),
                                      _edge("D.S.V3", "D.S.V1"), _edge("D.S.V3", "D.S.V2")]})
    ids = ([c["source_identifier"] for c in plan["can_migrate"]]
           + [c["source_identifier"] for c in plan["cannot_migrate"]])
    assert sorted(ids) == ["D.S.T", "D.S.V1", "D.S.V2", "D.S.V3"]
    assert len(ids) == len(set(ids)), "every object in exactly one list, once"
    cats = {c["source_identifier"]: c["category"] for c in plan["cannot_migrate"]}
    assert cats == {"D.S.T": "unmapped_type", "D.S.V1": "dependency_not_migrated",
                    "D.S.V2": "dependency_not_migrated",
                    "D.S.V3": "dependency_not_migrated"}


def test_views_whose_bases_all_migrate_are_unaffected_by_the_cascade():
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V", kind="VIEW",
                                           ddl="create view V as select a from D.S.T")]}
    plan = build_plan(inv, {"edges": [_edge("D.S.V", "D.S.T")]})
    assert plan["waves"] == [["D.S.T"], ["D.S.V"]]
    assert plan["cannot_migrate"] == []


# --- the plan says whose catalog name its Target column carries -----------
#
# The in-AIDP structure job creates the plan's target_fqn and refuses a
# --target-catalog that is not the plan's catalog; that catalog part is the
# source database mirrored (or a prefix), so the plan states which it is.

def test_plan_carries_a_target_catalog_note_for_the_default_mirror():
    plan = build_plan({"inventory": [rec("MYDB.SALES.ORDERS")]}, {"edges": []})
    note = plan["target_catalog_note"]
    assert "01_create_structure" in note
    assert "--target-catalog" in note
    assert "mydb" in note, "names the mirrored catalog the column carries"
    assert "source database" in note.lower()
    assert "external" in note.lower(), "warns that the source-named catalog is the pointer"


def test_target_catalog_note_names_the_prefix_when_one_was_given():
    plan = build_plan({"inventory": [rec("MYDB.SALES.ORDERS")]}, {"edges": []},
                      bronze_catalog_prefix="lake", bronze_schema_style="db")
    note = plan["target_catalog_note"]
    assert "'lake'" in note and "--bronze-catalog-prefix" in note
    assert "'db'" in note, "names the schema style the column follows"
    assert "01_create_structure" in note and "--target-catalog" in note


def test_target_catalog_note_is_present_even_when_nothing_migrates():
    inv = {"inventory": [rec("D.S.BAD", status="blocked", blocked=["x: VARIANT"])]}
    plan = build_plan(inv, {"edges": []})
    assert plan["target_catalog_note"] and "01_create_structure" in plan["target_catalog_note"]


# --- SHOW TABLES kind flags -----------------------------------------------
#
# SHOW TABLES lists dynamic, external, Iceberg, event and hybrid tables next
# to standard ones, and the extractor keeps the is_* flags. build_plan never
# read them: a dynamic table was planned as a plain Delta copy while
# CENSUS.md said it never migrates, and the others were flattened silently.

KIND_FLAGS = ("is_dynamic", "is_external", "is_iceberg", "is_event", "is_hybrid")
# A dynamic table migrates as a table snapshot (test_plan_snapshots.py): its
# contents are readable. The other kinds are still refused.
REFUSED_KIND_FLAGS = tuple(f for f in KIND_FLAGS if f != "is_dynamic")


def _flagged(ident, flag, value):
    r = rec(ident)
    r["source_metadata"][flag] = value
    return r


@pytest.mark.parametrize("flag", REFUSED_KIND_FLAGS)
@pytest.mark.parametrize("value", ["Y", "true", "TRUE"])
def test_a_flagged_table_kind_cannot_migrate_with_a_specific_reason(flag, value):
    inv = {"inventory": [_flagged("D.S.T", flag, value), rec("D.S.PLAIN")]}
    plan = build_plan(inv, {"edges": []})
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    assert set(cannot) == {"D.S.T"}
    # External and Iceberg tables are not refused outright any more: their
    # files are registered in place over OCI Object Storage (C4.3).
    assert cannot["D.S.T"]["category"] == (
        "register_in_place" if flag in ("is_external", "is_iceberg")
        else "unsupported_object")
    assert cannot["D.S.T"]["object_type"] == "TABLE"
    assert flag.removeprefix("is_") in cannot["D.S.T"]["reason"].lower()
    assert [c["source_identifier"] for c in plan["can_migrate"]] == ["D.S.PLAIN"]
    assert plan["summary"]["can_migrate"] == 1 and plan["summary"]["tables"] == 1
    assert "D.S.T" not in plan["clone_targets"]
    assert all("D.S.T" not in wave for wave in plan["waves"])


@pytest.mark.parametrize("flag", KIND_FLAGS)
@pytest.mark.parametrize("value", ["N", "false", "", None])
def test_an_unset_kind_flag_leaves_the_table_migratable(flag, value):
    plan = build_plan({"inventory": [_flagged("D.S.T", flag, value)]}, {"edges": []})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == ["D.S.T"]


def test_a_dynamic_table_is_planned_as_a_snapshot_and_says_so():
    """It was refused as "a snapshot that never refreshes". It is now that
    snapshot, carried as a table, with the refresh decided and named."""
    plan = build_plan({"inventory": [_flagged("D.S.DT", "is_dynamic", "Y")]},
                      {"edges": []})
    entry = plan["can_migrate"][0]
    assert entry["snapshot_of"] == "dynamic table"
    assert "snapshot" in entry["kind_warning"]
    assert entry["refresh"]["verdict"].startswith("refresh NOT generated")


@pytest.mark.parametrize("kind", ["TRANSIENT", "TEMPORARY", "transient"])
def test_transient_and_temporary_tables_migrate_with_a_warning(kind):
    r = rec("D.S.T")
    r["source_metadata"]["kind"] = kind
    plan = build_plan({"inventory": [r, rec("D.S.PLAIN")]}, {"edges": []})
    assert {c["source_identifier"] for c in plan["can_migrate"]} == {"D.S.PLAIN", "D.S.T"}
    warnings = plan["table_kind_warnings"]
    assert [w["source_identifier"] for w in warnings] == ["D.S.T"]
    assert warnings[0]["kind"] == kind.upper()
    assert kind.upper() in warnings[0]["warning"]
    assert "permanent" in warnings[0]["warning"].lower()
    # The same sentence travels on the can entry, where SUMMARY.md scores
    # it; the plan-level list is what PLANNED_OBJECTS.md renders.
    entries = {c["source_identifier"]: c for c in plan["can_migrate"]}
    assert entries["D.S.T"]["kind_warning"] == warnings[0]["warning"]
    assert entries["D.S.PLAIN"]["kind_warning"] is None
    assert entries["D.S.T"]["warnings"] == [], "column warnings stay separate"


def test_a_plain_table_carries_no_kind_warning():
    r = rec("D.S.T")
    r["source_metadata"]["kind"] = "TABLE"
    plan = build_plan({"inventory": [r]}, {"edges": []})
    assert plan["table_kind_warnings"] == []


# --- the plan says which views it could NOT order -------------------------
#
# "Views follow their base tables" holds only for views with an edge. A view
# with no edge from ACCOUNT_USAGE or parsed DDL sorts by size and can land in
# wave 1 ahead of its base; the plan must carry that list, derived from the
# planned views against the edges, so every provenance is covered alike.

def _unordered_view_inventory():
    # A view that reads no relation at all. (This used to read OTHERDB.S.T,
    # an out-of-inventory reference that the extractor now keeps as a marked
    # edge -- see test_outside_references.py -- so it is no longer edgeless.)
    view = rec("D.S.V", kind="VIEW", ddl="create view V as select 1 as a")
    view["row_count_exact"] = None
    return {"inventory": [rec("D.S.T", rows=1000), view]}


def test_plan_names_views_with_no_edge_when_account_usage_was_empty():
    from fake_sql import FakeSql
    from snowflake_source.extract.dependencies import extract_dependencies
    inv = _unordered_view_inventory()
    plan = build_plan(inv, extract_dependencies(FakeSql({"object_dependencies": []}), inv))
    assert plan["dependency_source"] == "account_usage_empty"
    assert plan["views_without_dependency_edge"] == ["D.S.V"]
    assert plan["dependency_warning"] and "D.S.V" in plan["dependency_warning"]
    assert plan["dependency_edge_count"] == 0


def test_plan_names_views_with_no_edge_when_account_usage_was_denied():
    # parsed_ddl with warning None: the producer had nothing to warn about,
    # but the view still has no edge and is still ordered by size only.
    from snowflake_source.extract.dependencies import extract_dependencies

    def denied(sql, params=None):
        raise RuntimeError("insufficient privileges")

    inv = _unordered_view_inventory()
    plan = build_plan(inv, extract_dependencies(denied, inv))
    assert plan["dependency_source"] == "parsed_ddl"
    assert plan["views_without_dependency_edge"] == ["D.S.V"]
    assert plan["dependency_warning"] is None
    assert plan["dependency_edge_count"] == 0


def test_plan_lists_no_unordered_views_when_account_usage_covers_them_all():
    from fake_sql import FakeSql
    from snowflake_source.extract.dependencies import extract_dependencies
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V", kind="VIEW",
                                           ddl="create view V as select a from D.S.T")]}
    run = FakeSql({"object_dependencies": [
        {"REFERENCING": "D.S.V", "REFERENCED": "D.S.T",
         "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"}]})
    plan = build_plan(inv, extract_dependencies(run, inv))
    assert plan["dependency_source"] == "account_usage"
    assert plan["views_without_dependency_edge"] == []
    assert plan["dependency_warning"] is None
    assert plan["dependency_edge_count"] == 1
    assert plan["waves"] == [["D.S.T"], ["D.S.V"]]


def test_unordered_views_are_listed_only_if_they_are_planned():
    # A view that cannot migrate is not "unordered"; it is not in the order.
    inv = {"inventory": [rec("D.S.T"), rec("D.S.V2", kind="VIEW", ddl=SNOWFLAKE_VIEW)]}
    plan = build_plan(inv, {"edges": []})
    assert plan["views_without_dependency_edge"] == []


# --- a target name the destination refuses --------------------------------
#
# Live 2026-09-22: `"Mixed Case Table"` folded to `mixed case table`, was
# planned, had DDL generated for it, and was attempted -- AIDP returned
# 400 `Invalid name ... Only lower-case characters, numbers and underscores
# are allowed`, and the name was burned for the rest of the run. It belongs
# with every other object that cannot land: refused at plan time.

def test_a_target_name_the_destination_refuses_is_not_planned():
    plan = build_plan({"inventory": [rec("D.EDGE.Mixed Case Table")]},
                      {"edges": []})
    assert plan["can_migrate"] == []
    entry = plan["cannot_migrate"][0]
    assert entry["category"] == "unacceptable_target_name"
    assert "mixed case table" in entry["reason"]
    assert "lower-case" in entry["reason"]


def test_the_refusal_says_the_plugin_will_not_rename_it():
    plan = build_plan({"inventory": [rec("D.EDGE.Mixed Case Table")]},
                      {"edges": []})
    reason = plan["cannot_migrate"][0]["reason"].lower()
    assert "rename" in reason
    assert "different table" in reason


def test_a_view_whose_name_has_a_space_is_refused_too():
    plan = build_plan({"inventory": [rec("D.EDGE.V Quoted", kind="VIEW")]},
                      {"edges": []})
    assert plan["can_migrate"] == []
    assert plan["cannot_migrate"][0]["category"] == "unacceptable_target_name"


def test_a_schema_name_with_a_space_refuses_its_objects():
    plan = build_plan({"inventory": [rec("D.MY SCHEMA.T")]}, {"edges": []})
    assert plan["cannot_migrate"][0]["category"] == "unacceptable_target_name"
    assert "my schema" in plan["cannot_migrate"][0]["reason"]


def test_an_ordinary_name_still_plans():
    plan = build_plan({"inventory": [rec("D.EDGE.ORDERS")]}, {"edges": []})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == [
        "D.EDGE.ORDERS"]
