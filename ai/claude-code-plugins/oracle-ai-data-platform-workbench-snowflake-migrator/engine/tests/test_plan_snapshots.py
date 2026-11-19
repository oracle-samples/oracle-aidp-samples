"""A dynamic table and a materialized view migrate as a table snapshot.

They were refused outright (`unsupported_object`): "a copy would be a
snapshot that never refreshes". That was true of the copy and wrong about
the conclusion. Their CURRENT CONTENTS are readable -- a qualified pushdown
SELECT over any Snowflake relation returns rows (live 2026-09-29: ~8.5 s per
statement, and the copy stage reads every table that way) -- so refusing
them left the estate's most-queried rollups with nothing on the target at
all, where a snapshot plus a refresh job leaves them with their data and a
way to keep it current.

What the plan now says, per object, and nothing more:

  * it migrates as a TABLE (`object_type` TABLE, `snapshot_of` naming what
    it was), created and copied like any other table;
  * `refresh generated` when its defining query translates exactly and
    every object it reads is migrating -- `snowmig jobs` then writes the
    refresh notebook; otherwise `refresh NOT generated: <why>`, naming the
    construct or the missing object. The table still migrates either way;
    only the refresh is withheld;
  * the source cadence (a dynamic table's TARGET_LAG) is recorded as the
    INTENDED one. Nothing is scheduled: the generated job is MANUAL.

A dynamic table's query comes from the census (SHOW DYNAMIC TABLES `text`,
kept as source_facts since the census change); a materialized view's from
the same census row or, failing that, the view text the inventory keeps.
The live shapes (2026-09-29) are used with fake names.
"""
import pytest

from plan.build import build_plan, object_kind_block, snapshot_kind
from report.render import render_inventory, render_planned_objects
from target.ddl import build_ddl_payload
from target.provisioning import plan_copy_schemas

DT_TEXT = ("CREATE OR REPLACE DYNAMIC TABLE DB.CORE.DT_ORDER_ROLLUP "
           "lag = '1 day' refresh_mode = 'AUTO' initialize = 'ON_CREATE' "
           "warehouse = WH_X AS SELECT CUSTOMER_ID, COUNT(*) AS N FROM "
           "DB.CORE.ORDERS GROUP BY CUSTOMER_ID")
MV_TEXT = ("CREATE OR REPLACE MATERIALIZED VIEW MV_ORDER_TOTALS AS\n"
           "SELECT CUSTOMER_ID, SUM(AMOUNT) AS TOTAL FROM ORDERS "
           "GROUP BY CUSTOMER_ID")


def _rec(ident, kind="TABLE", status="supported", **flags):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": kind,
            "source_database": db, "source_schema": schema,
            "identifier_case_form": "UPPER_UNQUOTED",
            "compatibility_status": status,
            "blocked_reasons": ["X: VARIANT"] if status == "blocked" else [],
            "row_count_exact": 1, "row_count_source": "show_metadata",
            "columns": [{"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "NUMBER",
                         "ORDINAL_POSITION": 1, "IS_NULLABLE": "YES",
                         "target_type": "DECIMAL(38,0)"}],
            "source_metadata": dict(flags)}


def _dt(ident="DB.CORE.DT_ORDER_ROLLUP", **flags):
    return _rec(ident, is_dynamic="Y", **flags)


def _mv(ident="DB.CORE.MV_ORDER_TOTALS", ddl=MV_TEXT, **flags):
    r = _rec(ident, kind="VIEW", is_materialized="true", **flags)
    r["view_text_show"] = ddl
    return r


def _census(*objects):
    return {"objects": list(objects), "kinds": {}, "total": len(objects)}


def _dt_census(ident="DB.CORE.DT_ORDER_ROLLUP", text=DT_TEXT, lag="1 day"):
    return {"kind": "DYNAMIC_TABLE", "source_identifier": ident,
            "migratable": False, "reason": "", "detail": f"target_lag={lag}",
            "source_facts": {"target_lag": lag, "refresh_mode": "INCREMENTAL",
                             "text": text, "warehouse": "WH_X"}}


def _inv(*records, census=None):
    inv = {"probed_at": "2026-09-29T00:00:00+00:00",
           "session": {"A": "ACCT", "R": "REGION", "ROLE": "R"},
           "databases_in_scope": ["DB"], "object_count": len(records),
           "counts_by_type": {}, "identifier_case_collisions": {},
           "extraction_notes": [], "row_count_mode": "metadata",
           "inventory": list(records)}
    if census is not None:
        inv["census"] = census
    return inv


def _plan(*records, census=None, edges=(), **kw):
    return build_plan(_inv(*records, census=census),
                      {"edges": list(edges)}, **kw)


def _can(plan):
    return {c["source_identifier"]: c for c in plan["can_migrate"]}


ORDERS = "DB.CORE.ORDERS"


# ----------------------------------------------------- migrates as a table

def test_a_dynamic_table_migrates_as_a_table_snapshot_with_its_refresh():
    plan = _plan(_rec(ORDERS), _dt(), census=_census(_dt_census()))
    entry = _can(plan)["DB.CORE.DT_ORDER_ROLLUP"]
    assert entry["object_type"] == "TABLE"
    assert entry["source_object_type"] == "TABLE"
    assert entry["snapshot_of"] == "dynamic table"
    refresh = entry["refresh"]
    assert refresh["generated"] is True
    assert refresh["verdict"] == "refresh generated"
    assert refresh["cadence"] == {"source": "TARGET_LAG", "value": "1 day"}
    assert refresh["reads"] == ["db.core.orders"]
    assert not plan["cannot_migrate"]
    assert plan["summary"]["tables"] == 2
    assert "DB.CORE.DT_ORDER_ROLLUP" in plan["clone_targets"]


def test_a_materialized_view_migrates_as_a_table_not_a_view():
    plan = _plan(_rec(ORDERS), _mv())
    entry = _can(plan)["DB.CORE.MV_ORDER_TOTALS"]
    assert entry["object_type"] == "TABLE", "created and copied as a table"
    assert entry["source_object_type"] == "VIEW"
    assert entry["snapshot_of"] == "materialized view"
    assert entry["refresh"]["generated"] is True
    # Snowflake maintained it on every base-table change: there is no lag
    # to carry, and the plan must not invent one.
    assert entry["refresh"]["cadence"] is None
    assert "no cadence" in entry["refresh"]["cadence_note"].lower()
    assert plan["summary"]["tables"] == 2 and plan["summary"]["views"] == 0


def test_the_snapshot_says_what_it_is_in_the_risk_sentence():
    """kind_warning is what SUMMARY.md scores; a snapshot that no longer
    refreshes itself is a behaviour change, not a clean clone."""
    plan = _plan(_rec(ORDERS), _dt(), census=_census(_dt_census()))
    warning = _can(plan)["DB.CORE.DT_ORDER_ROLLUP"]["kind_warning"]
    assert "snapshot" in warning and "dynamic table" in warning
    assert "not scheduled" in warning.lower() or "manual" in warning.lower()


# ------------------------------------------------ refresh NOT generated

def test_an_untranslatable_query_keeps_the_snapshot_and_withholds_the_refresh():
    text = DT_TEXT.replace("GROUP BY CUSTOMER_ID",
                           "QUALIFY ROW_NUMBER() OVER (ORDER BY N) = 1")
    plan = _plan(_rec(ORDERS), _dt(), census=_census(_dt_census(text=text)))
    refresh = _can(plan)["DB.CORE.DT_ORDER_ROLLUP"]["refresh"]
    assert refresh["generated"] is False
    assert refresh["verdict"].startswith("refresh NOT generated: ")
    assert "QUALIFY" in refresh["verdict"]


def test_a_query_over_a_table_that_is_not_migrating_withholds_the_refresh():
    """The snapshot does not need ORDERS to exist -- its rows are read from
    the dynamic table itself -- so it is not cascaded out. Its refresh
    does, so the verdict names ORDERS."""
    plan = _plan(_rec(ORDERS, status="blocked"), _dt(),
                 census=_census(_dt_census()),
                 edges=[{"from": "DB.CORE.DT_ORDER_ROLLUP", "to": ORDERS}])
    can = _can(plan)
    assert "DB.CORE.DT_ORDER_ROLLUP" in can, "the snapshot still migrates"
    refresh = can["DB.CORE.DT_ORDER_ROLLUP"]["refresh"]
    assert refresh["generated"] is False
    assert "DB.CORE.ORDERS" in refresh["verdict"]
    assert "not migrating" in refresh["verdict"]


def test_an_excluded_source_withholds_the_refresh_too():
    plan = _plan(_rec(ORDERS), _dt(), census=_census(_dt_census()),
                 restrictions={"exclude_objects": [ORDERS]})
    refresh = _can(plan)["DB.CORE.DT_ORDER_ROLLUP"]["refresh"]
    assert refresh["generated"] is False
    assert "ORDERS" in refresh["verdict"]


def test_no_captured_query_is_said_not_guessed():
    """assess --no-census, or a role that cannot see SHOW DYNAMIC TABLES:
    the table is in SHOW TABLES, its query is nowhere."""
    plan = _plan(_rec(ORDERS), _dt())
    refresh = _can(plan)["DB.CORE.DT_ORDER_ROLLUP"]["refresh"]
    assert refresh["generated"] is False
    assert "not captured" in refresh["verdict"]
    assert refresh["cadence"] is None


def test_a_census_row_without_the_query_names_the_flag_not_the_grants():
    """A default assess: the census saw the dynamic table (its lag is read)
    but dropped the body without --capture-definitions. The role is fine."""
    census = _census({"kind": "DYNAMIC_TABLE",
                      "source_identifier": "DB.CORE.DT_ORDER_ROLLUP",
                      "source_facts": {"target_lag": "1 day"}})
    plan = _plan(_rec(ORDERS), _dt(), census=census)
    refresh = _can(plan)["DB.CORE.DT_ORDER_ROLLUP"]["refresh"]
    assert refresh["generated"] is False
    assert "SHOW DYNAMIC TABLES" not in refresh["verdict"]


def test_a_materialized_view_query_is_found_on_the_census_row_first():
    census = _census({"kind": "MATERIALIZED_VIEW",
                      "source_identifier": "DB.CORE.MV_ORDER_TOTALS",
                      "source_facts": {"text": MV_TEXT}})
    plan = _plan(_rec(ORDERS), _mv(ddl=None), census=census)
    assert _can(plan)["DB.CORE.MV_ORDER_TOTALS"]["refresh"]["generated"]


# -------------------------------------------- what still does not migrate

def test_a_secure_materialized_view_is_still_refused_as_secure():
    plan = _plan(_rec(ORDERS), _mv(is_secure="true"))
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    assert "secure view" in cannot["DB.CORE.MV_ORDER_TOTALS"]["reason"].lower()


def test_an_iceberg_dynamic_table_is_still_refused_as_iceberg():
    plan = _plan(_dt(is_iceberg="Y"), census=_census(_dt_census()))
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    assert "iceberg" in cannot["DB.CORE.DT_ORDER_ROLLUP"]["reason"].lower()


def test_a_type_blocked_dynamic_table_is_still_refused_for_its_types():
    plan = _plan(_dt(status="blocked"), census=_census(_dt_census()))
    assert plan["cannot_migrate"][0]["category"] == "unmapped_type"


def test_snapshot_kind_is_none_for_a_plain_table_and_view():
    assert snapshot_kind(_rec(ORDERS)) is None
    assert snapshot_kind(_rec("DB.CORE.V", kind="VIEW")) is None
    assert snapshot_kind(_dt()) == "dynamic table"
    assert snapshot_kind(_mv()) == "materialized view"
    # The kind still has no plain-Delta equivalent; the snapshot is what the
    # plan does about it.
    assert object_kind_block(_dt())[0] == "dynamic table"


def test_a_view_over_a_snapshot_migrates_after_it():
    view = _rec("DB.CORE.V_TOP", kind="VIEW")
    view["view_ddl_get_ddl"] = ("create view V_TOP as select * from "
                                "DB.CORE.DT_ORDER_ROLLUP")
    plan = _plan(_rec(ORDERS), _dt(), view, census=_census(_dt_census()),
                 edges=[{"from": "DB.CORE.V_TOP",
                         "to": "DB.CORE.DT_ORDER_ROLLUP"}])
    assert "DB.CORE.V_TOP" in _can(plan)
    order = [i for wave in plan["waves"] for i in wave]
    assert order.index("DB.CORE.DT_ORDER_ROLLUP") < order.index("DB.CORE.V_TOP")


# ------------------------------------------------ ddl and copy follow it

def test_ddl_emits_a_create_table_for_a_materialized_view_snapshot():
    inv = _inv(_rec(ORDERS), _mv())
    plan = build_plan(inv, {"edges": []})
    payload = build_ddl_payload(inv, plan)
    stmt = next(s for s in payload["statements"]
                if s["source_identifier"] == "DB.CORE.MV_ORDER_TOTALS")
    assert stmt["object_type"] == "TABLE"
    assert stmt["sql"].startswith("CREATE TABLE IF NOT EXISTS")
    assert stmt["snapshot_of"] == "materialized view"
    assert "view_text" not in stmt
    assert not payload["blocked"]
    assert plan_copy_schemas(payload) == ["CORE"], "its rows are copied"


# ------------------------------------------------------------- the reports

def _cell(md, ident):
    line = next(l for l in md.splitlines() if l.startswith(f"| `{ident}`"))
    return line.rstrip(" |").rsplit("|", 1)[-1].strip()


@pytest.mark.parametrize("record,label", [(_dt(), "dynamic table"),
                                          (_mv(), "materialized view")])
def test_inventory_says_table_snapshot_not_blocked_and_not_supported(record,
                                                                     label):
    md = render_inventory(_inv(record))
    cell = _cell(md, record["source_identifier"])
    assert cell == f"table snapshot ({label})", cell
    assert "refresh" in md[md.index("table snapshot ("):].lower()
    assert "PLANNED_OBJECTS.md" in md


def test_planned_objects_lists_each_snapshot_with_its_refresh_verdict():
    text = DT_TEXT.replace("GROUP BY CUSTOMER_ID",
                           "QUALIFY ROW_NUMBER() OVER (ORDER BY N) = 1")
    plan = _plan(_rec(ORDERS), _dt(), _mv(),
                 census=_census(_dt_census(text=text)))
    md = render_planned_objects(plan)
    head = "## Planned as table snapshots"
    section = md[md.index(head):].split("\n## ", 1)[0]
    assert "`DB.CORE.MV_ORDER_TOTALS` (materialized view)" in section
    assert "refresh generated" in section
    assert "refresh NOT generated: " in section and "QUALIFY" in section
    assert "TARGET_LAG 1 day" in section
    assert "snowmig jobs" in section
