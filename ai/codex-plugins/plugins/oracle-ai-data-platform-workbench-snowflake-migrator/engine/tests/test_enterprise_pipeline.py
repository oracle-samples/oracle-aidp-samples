"""The whole offline pipeline over the ENTERPRISE estate, through the CLI.

`assess -> deps -> maintenance -> security -> plan -> ddl -> summary`, the
same commands an operator runs, with only the Snowflake transport swapped
for `enterprise_run_sql` (engine/emulation/snowflake_fake.py). Two things
are checked, and they are different things:

  1. every KIND gets exactly the verdict or path it should -- a hybrid
     table is refused as a hybrid table, not as a type problem; a failover
     group is counted, not unasked;
  2. no report contradicts another. Each report is right on its own terms
     and they are read by different people: SUMMARY.md said ORDERS was a
     clean clone while MAINTENANCE.md said its point lookups would regress,
     and said CUSTOMERS was LOW while SECURITY.md said its e-mail column
     arrives unmasked. A reader of one report was told the opposite of a
     reader of the other.

Everything here is offline and emulated: no Snowflake, no AIDP.
"""
import json
import re

import pytest

import snowmig
from emulation.snowflake_fake import ENTERPRISE_DB, enterprise_run_sql

# What each inventoried object must become. `(list, category, words the
# reason must contain)`. The materialized view migrates as a table snapshot
# with a generated refresh job, so it is planned `can`.
EXPECTED = {
    "SNOWENT.SALES.ORDERS": ("can", None, None),
    "SNOWENT.SALES.CUSTOMERS": ("can", None, None),
    "SNOWENT.SALES.CUSTOMER_360_SV": ("cannot", "unsupported_object",
                                      "secure view"),
    "SNOWENT.SALES.ORDER_TOTALS_MV": ("can", None, None),
    # Not copied: registered over OCI Object Storage once the files move.
    "SNOWENT.LAKE.EXT_CLICKS": ("cannot", "register_in_place",
                                "external table"),
    "SNOWENT.LAKE.EXT_PARTNER_FEED": ("cannot", "register_in_place",
                                      "external table"),
    "SNOWENT.LAKE.ICE_EVENTS": ("cannot", "register_in_place",
                                "Iceberg table"),
    "SNOWENT.LAKE.ICE_GLUE_ORDERS": ("cannot", "register_in_place",
                                     "Iceberg table"),
    "SNOWENT.OPS.HYB_SESSIONS": ("cannot", "unsupported_object",
                                 "hybrid (Unistore) table"),
    "SNOWENT.OPS.APP_EVENTS": ("cannot", "unsupported_object",
                               "event table"),
}

# The census: every non-table kind the estate holds, each counted once.
EXPECTED_CENSUS = {
    "FUNCTION": 1, "EXTERNAL_FUNCTION": 1, "STAGE": 1, "FILE_FORMAT": 1,
    "STREAM": 1, "MATERIALIZED_VIEW": 1, "SERVICE": 1, "COMPUTE_POOL": 1,
    "APPLICATION": 1, "SHARE": 2, "REPLICATION_GROUP": 1,
    "FAILOVER_GROUP": 1,
}

STAGES = (["assess", "--database", ENTERPRISE_DB], ["deps"], ["maintenance"],
          ["security"], ["plan"], ["ddl"], ["external-registration"],
          ["share-plan"], ["summary"])


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    out = tmp_path_factory.mktemp("enterprise")
    mp = pytest.MonkeyPatch()
    # Module-scoped, so the suite's autouse publish guard is re-applied here.
    mp.setenv("SNOWMIG_NO_STAGE_PUBLISH", "1")
    mp.setattr(snowmig, "_run_sql_from_args", lambda args: enterprise_run_sql)
    # Never a developer's real config: the run is the emulation and nothing
    # else.
    mp.setattr(snowmig, "_config_path", lambda args, **k: None)
    mp.chdir(out)
    codes = {}
    try:
        for argv in STAGES:
            codes[argv[0]] = snowmig.main(argv + ["--out-dir", str(out)])
    finally:
        mp.undo()

    def read(name):
        text = (out / name).read_text(encoding="utf-8")
        return json.loads(text) if name.endswith(".json") else text
    return {"out": out, "codes": codes, "read": read}


def _rows(md: str, first_cell: str) -> dict[str, list[str]]:
    """Markdown table rows keyed by their first cell (backticks stripped)."""
    rows = {}
    for line in md.splitlines():
        if not line.startswith("| `"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        rows[cells[0].strip("`")] = cells
    return rows


def test_every_stage_exits_zero(run):
    assert run["codes"] == {argv[0]: 0 for argv in STAGES}


# ------------------------------------------------- 1. the right verdicts

@pytest.mark.parametrize("ident", sorted(EXPECTED))
def test_each_object_gets_exactly_its_verdict(run, ident):
    plan = run["read"]("plan.json")
    can = {c["source_identifier"] for c in plan["can_migrate"]}
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    where, category, words = EXPECTED[ident]
    if where == "can":
        assert ident in can and ident not in cannot
        return
    assert ident not in can
    assert cannot[ident]["category"] == category, cannot[ident]
    assert words in cannot[ident]["reason"], cannot[ident]["reason"]


def test_the_plan_accounts_for_every_object_exactly_once(run):
    inv = run["read"]("inventory.json")
    plan = run["read"]("plan.json")
    can = [c["source_identifier"] for c in plan["can_migrate"]]
    cannot = [c["source_identifier"] for c in plan["cannot_migrate"]]
    assert sorted(can + cannot) == sorted(
        r["source_identifier"] for r in inv["inventory"])
    assert not set(can) & set(cannot)
    assert set(can + cannot) == set(EXPECTED), "EXPECTED must name every object"


def test_the_census_counts_every_kind_the_estate_holds(run):
    census = run["read"]("inventory.json")["census"]
    assert census["unreadable"] == []
    assert {k: v for k, v in census["by_kind"].items() if v} == EXPECTED_CENSUS


def test_replication_and_failover_groups_are_read_once_and_told_apart(run):
    # SHOW REPLICATION GROUPS lists both kinds; reading SHOW FAILOVER GROUPS
    # as well would count the failover group twice.
    objects = {o["source_identifier"]: o
               for o in run["read"]("inventory.json")["census"]["objects"]}
    fg, rg = objects["ENT_FG"], objects["ENT_RG"]
    assert fg["kind"] == "FAILOVER_GROUP" and rg["kind"] == "REPLICATION_GROUP"
    assert "allowed_accounts=EMUORG.EMU_ENT_DR" in fg["detail"]
    assert fg["reason"] != rg["reason"]
    assert "failover" in fg["reason"].lower()
    census_md = run["read"]("CENSUS.md")
    assert "ENT_FG" in census_md and "ENT_RG" in census_md


def test_the_inbound_and_outbound_shares_get_opposite_verdicts(run):
    shares = {o["source_identifier"]: o
              for o in run["read"]("inventory.json")["census"]["objects"]
              if o["kind"] == "SHARE"}
    assert shares["ENT_PARTNER_SHARE"]["detail"].startswith("OUTBOUND to ")
    assert shares["WEATHER_SHARE"]["detail"].startswith("INBOUND")
    assert shares["WEATHER_SHARE"]["reason"] != shares["ENT_PARTNER_SHARE"]["reason"]


# -------------------------------------------- 2. no report contradicts another

def test_inventory_compatibility_agrees_with_the_plan(run):
    plan = run["read"]("plan.json")
    rows = _rows(run["read"]("INVENTORY.md"), "Object")
    kinds = {"secure view", "materialized view", "external table",
             "Iceberg table", "hybrid table", "event table"}
    for c in plan["can_migrate"]:
        want = (f'table snapshot ({c["snapshot_of"]})' if c.get("snapshot_of")
                else "supported")
        assert rows[c["source_identifier"]][-1] == want, c
    for c in plan["cannot_migrate"]:
        cell = rows[c["source_identifier"]][-1]
        verb = ("register in place" if c["category"] == "register_in_place"
                else "blocked")
        label = re.fullmatch(verb + r" \((.+)\)", cell)
        assert label and label.group(1) in kinds, (c["source_identifier"], cell)
        assert label.group(1).split()[0].lower() in c["reason"].lower()


def test_ddl_covers_the_planned_objects_and_nothing_else(run):
    plan = run["read"]("plan.json")
    ddl = run["read"]("ddl_plan.json")
    can = {c["source_identifier"] for c in plan["can_migrate"]}
    emitted = {s["source_identifier"] for s in ddl["statements"]}
    blocked = {b["source_identifier"] for b in ddl["blocked"]}
    assert emitted | blocked == can
    assert not (emitted | blocked) & {c["source_identifier"]
                                      for c in plan["cannot_migrate"]}


def test_external_registration_covers_exactly_the_register_in_place_set(run):
    plan = run["read"]("plan.json")
    reg = run["read"]("external_registration.json")
    planned = {c["source_identifier"] for c in plan["cannot_migrate"]
               if c["category"] == "register_in_place"}
    assert {t["source_identifier"] for t in reg["tables"]} == planned
    assert reg["executed"] is False and reg["not_in_inventory"] == []
    # Each registration lands on the plan's own target name.
    names = plan["target_names"]
    for t in reg["tables"]:
        assert t["target"] == names[t["source_identifier"]]


def test_the_share_plan_agrees_with_the_plan_and_security(run):
    # Every shared object's status is derived from plan.json and
    # security.json, and a table SECURITY.md says is protected is never in
    # a step that would publish it.
    plan = run["read"]("plan.json")
    sec = run["read"]("security.json")
    sp = run["read"]("share_plan.json")
    can = {c["source_identifier"]: c for c in plan["can_migrate"]}
    exposed = {e["object"] for e in sec["exposures"]}
    share = next(s for s in sp["shares"] if s["name"] == "ENT_PARTNER_SHARE")
    for o in share["objects"]:
        ident = o["source_identifier"]
        if ident in exposed:
            assert o["status"] == "hold_exposure", o
        if ident not in can:
            assert o["status"] in ("no_target", "register_first"), o
        else:
            assert o["target"] == can[ident]["target"]
    published = {st["subject"] for st in share["steps"]
                 if st["command"] == "manage-data-asset"}
    assert not published & {can[i]["target"] for i in exposed if i in can}
    assert len(share["recipients"]) == 2


def test_summary_marks_every_refused_object_blocked(run):
    plan = run["read"]("plan.json")
    rows = _rows(run["read"]("SUMMARY.md"), "Object")
    for c in plan["cannot_migrate"]:
        assert rows[c["source_identifier"]][4] == "BLOCKED"
    for c in plan["can_migrate"]:
        assert rows[c["source_identifier"]][4] != "BLOCKED"


def test_search_optimization_is_named_wherever_orders_is_scored(run):
    # MAINTENANCE.md: "Search Optimization Service enabled ... no equivalent;
    # point-lookup performance on high-cardinality columns will regress".
    # The DDL plan and the summary scored ORDERS without it.
    maint = run["read"]("maintenance.json")
    orders = next(t for t in maint["tables"]
                  if t["source_identifier"] == "SNOWENT.SALES.ORDERS")
    assert any("Search Optimization" in s["signal"] for s in orders["signals"])
    stmt = next(s for s in run["read"]("ddl_plan.json")["statements"]
                if s["source_identifier"] == "SNOWENT.SALES.ORDERS")
    assert "search_optimization=ON" in stmt["omitted_properties"]
    entry = next(c for c in run["read"]("plan.json")["can_migrate"]
                 if c["source_identifier"] == "SNOWENT.SALES.ORDERS")
    assert "search_optimization=ON" in entry["omitted_properties"]
    assert "search_optimization=ON" in _rows(
        run["read"]("SUMMARY.md"), "Object")["SNOWENT.SALES.ORDERS"][5]


def test_a_protected_object_is_not_scored_clean_by_the_summary(run):
    # SECURITY.md: CUSTOMERS.EMAIL arrives UNMASKED and ORDERS loses its
    # row filter. The summary must not rate either one as a clean clone.
    sec = run["read"]("security.json")
    exposed = {e["object"] for e in sec["exposures"]}
    assert exposed == {"SNOWENT.SALES.CUSTOMERS", "SNOWENT.SALES.ORDERS"}
    rows = _rows(run["read"]("SUMMARY.md"), "Object")
    customers = rows["SNOWENT.SALES.CUSTOMERS"]
    assert customers[3] == "HIGH", customers
    assert "MASK_EMAIL" in customers[5] and "SECURITY.md" in customers[5]
    orders = rows["SNOWENT.SALES.ORDERS"]
    assert orders[3] == "HIGH" and "RAP_REGION" in orders[5]


def test_planned_objects_counts_match_the_plan(run):
    plan = run["read"]("plan.json")
    md = run["read"]("PLANNED_OBJECTS.md")
    s = plan["summary"]
    assert (f'Planned **{s["can_migrate"]}** of {s["objects_inventoried"]} '
            f'inventoried objects') in md
    assert f'**{s["cannot_migrate"]}** cannot move' in md


def test_the_kind_verdict_does_not_depend_on_the_type_switch():
    # Under the strict mapping (`--mapping-defaults off`, semi-structured
    # blocked) an event table's OBJECT columns and an external table's VALUE
    # VARIANT made the plan file both under `unmapped_type`, telling the
    # operator to fix a TYPE. `--semi-structured string` would then "fix" it
    # and reveal the real refusal, the object kind, one run later.
    from plan.build import build_plan
    from snowflake_source.extract.catalog import build_inventory
    from snowflake_source.extract.dependencies import extract_dependencies
    inv = build_inventory(enterprise_run_sql, [ENTERPRISE_DB],
                          semi_structured="block")
    plan = build_plan(inv, extract_dependencies(enterprise_run_sql, inv))
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    for ident, category, words in (
            ("SNOWENT.OPS.APP_EVENTS", "unsupported_object", "event table"),
            ("SNOWENT.LAKE.EXT_CLICKS", "register_in_place", "external table")):
        assert cannot[ident]["category"] == category, cannot[ident]
        assert words in cannot[ident]["reason"]
