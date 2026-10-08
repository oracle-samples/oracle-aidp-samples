"""`plan --secure-views as-view`: a secure view planned as a plain view, loudly.

A secure view is refused by default, and stays refused: its definition is
hidden, the optimizer may not push predicates through it, and whatever
row-visibility logic its body holds keys off Snowflake roles. None of that
exists on AIDP. But refusal leaves the operator with every consumer of the
view broken at cutover, and many secure views exist only because Snowflake
SHARES require them (a share can carry a view only if it is secure), not
because anything in them is secret.

So the operator may opt in, per run, to plan them as plain views. The
opt-in may never be quiet: every report that can place the view says, in
words, that it is not secure any more -- PLANNED_OBJECTS.md before the
object list, the DDL statement's warnings and rule, and SUMMARY.md at HIGH.
What it does NOT do: bypass the translator (a secure view with
Snowflake-only SQL is still refused for that SQL), or turn a secure
MATERIALIZED view into anything (a materialized view is still refused).
"""
import copy
import json

import pytest

import snowmig
from emulation.snowflake_fake import ENTERPRISE_DB, enterprise_run_sql
from plan.build import SECURE_VIEW_MODES, build_plan
from report.render import render_planned_objects
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.dependencies import extract_dependencies
from target.ddl import build_ddl_payload

SV = "SNOWENT.SALES.CUSTOMER_360_SV"


@pytest.fixture(scope="module")
def estate():
    inv = build_inventory(enterprise_run_sql, [ENTERPRISE_DB])
    return inv, extract_dependencies(enterprise_run_sql, inv)


def _plan(estate, **kw):
    inv, deps = estate
    return build_plan(copy.deepcopy(inv), deps, **kw)


def test_the_default_is_unchanged_refused(estate):
    plan = _plan(estate)
    cannot = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    assert cannot[SV]["category"] == "unsupported_object"
    assert "secure view" in cannot[SV]["reason"]
    assert plan["secure_views_mode"] == "refuse"
    assert plan["secure_views_as_views"] == []


def test_the_modes_are_exactly_two():
    assert SECURE_VIEW_MODES == ("refuse", "as-view")
    with pytest.raises(ValueError, match="secure_views"):
        build_plan({"inventory": []}, {"edges": []}, secure_views="yes")


def test_as_view_plans_it_with_the_security_warning_attached(estate):
    plan = _plan(estate, secure_views="as-view")
    entry = next(c for c in plan["can_migrate"] if c["source_identifier"] == SV)
    warning = entry["kind_warning"]
    assert warning.startswith("SECURITY WARNING")
    assert "definition is visible" in warning
    assert "row" in warning
    assert plan["secure_views_as_views"] == [SV]
    assert plan["secure_views_mode"] == "as-view"


def test_as_view_does_not_bypass_the_translator():
    rec = {"source_identifier": "D.S.SV", "object_type": "VIEW",
           "source_database": "D", "source_schema": "S",
           "compatibility_status": "supported", "columns": [],
           "source_metadata": {"is_secure": "true"},
           "view_ddl_get_ddl": ("create or replace secure view D.S.SV as "
                                "select a from D.S.T qualify row_number() "
                                "over (order by a) = 1")}
    plan = build_plan({"inventory": [rec]}, {"edges": []},
                      secure_views="as-view")
    assert plan["cannot_migrate"][0]["category"] == "snowflake_only_sql"
    assert plan["secure_views_as_views"] == []


def test_a_secure_materialized_view_is_still_refused():
    rec = {"source_identifier": "D.S.MV", "object_type": "VIEW",
           "source_database": "D", "source_schema": "S",
           "compatibility_status": "supported", "columns": [],
           "source_metadata": {"is_secure": "true", "is_materialized": "true"},
           "view_ddl_get_ddl": "create secure materialized view D.S.MV as select 1 as A"}
    plan = build_plan({"inventory": [rec]}, {"edges": []},
                      secure_views="as-view")
    assert plan["cannot_migrate"][0]["category"] == "unsupported_object"
    assert "materialized" in plan["cannot_migrate"][0]["reason"]


def test_a_role_aware_body_says_so_in_the_warning():
    # A secure view is often the row filter itself: CURRENT_ROLE() in its
    # body keys off a Snowflake role that does not exist on AIDP.
    rec = {"source_identifier": "D.S.SV", "object_type": "VIEW",
           "source_database": "D", "source_schema": "S",
           "compatibility_status": "supported", "columns": [],
           "source_metadata": {"is_secure": "true"},
           "view_ddl_get_ddl": ("create secure view D.S.SV as select a from "
                                "D.S.T where current_role() = 'ANALYST'")}
    plan = build_plan({"inventory": [rec]}, {"edges": []},
                      secure_views="as-view")
    warning = plan["can_migrate"][0]["kind_warning"]
    assert "CURRENT_ROLE" in warning


def test_ddl_emits_a_plain_view_and_names_what_it_dropped(estate):
    inv, _ = estate
    plan = _plan(estate, secure_views="as-view")
    ddl = build_ddl_payload(inv, plan)
    stmt = next(s for s in ddl["statements"] if s["source_identifier"] == SV)
    assert stmt["sql"].upper().startswith("CREATE VIEW")
    assert "SECURE" not in stmt["sql"].upper()
    assert "R60_SECURE_VIEW_AS_PLAIN" in [r["rule_id"] for r in stmt["rules_applied"]]
    assert any(w.startswith("SECURITY WARNING") for w in stmt["warnings"])
    assert SV not in {b["source_identifier"] for b in ddl["blocked"]}


def test_ddl_refuses_it_by_default_as_it_always_has(estate):
    inv, _ = estate
    plan = _plan(estate)
    ddl = build_ddl_payload(inv, plan)
    assert SV not in {s["source_identifier"] for s in ddl["statements"]}


def test_planned_objects_puts_the_warning_before_the_object_list(estate):
    md = render_planned_objects(_plan(estate, secure_views="as-view"))
    warn = md.index("## SECURITY WARNING")
    assert warn < md.index("## Can migrate")
    section = md[warn:md.index("## Can migrate")]
    assert f"`{SV}`" in section and "--secure-views as-view" in section


def test_planned_objects_is_silent_about_it_by_default(estate):
    assert "SECURITY WARNING" not in render_planned_objects(_plan(estate))


def test_the_cli_flag_reaches_the_plan_and_the_summary(tmp_path, monkeypatch):
    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: enterprise_run_sql)
    monkeypatch.setattr(snowmig, "_config_path", lambda args, **k: None)
    monkeypatch.chdir(tmp_path)
    for argv in (["assess", "--database", ENTERPRISE_DB], ["deps"], ["security"],
                 ["plan", "--secure-views", "as-view"], ["ddl"], ["summary"]):
        assert snowmig.main(argv + ["--out-dir", str(tmp_path)]) == 0, argv
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["secure_views_as_views"] == [SV]
    summary = (tmp_path / "SUMMARY.md").read_text(encoding="utf-8")
    row = next(l for l in summary.splitlines() if l.startswith(f"| `{SV}`"))
    cells = [c.strip() for c in row.strip("|").split("|")]
    assert cells[3] == "HIGH" and cells[4] != "BLOCKED"
    assert "SECURITY WARNING" in cells[5]
    # And SECURITY.md's own finding reaches the same row.
    assert "SECURE (definition and row-visibility guarantees)" in cells[5]


def test_the_cli_rejects_an_unknown_mode(tmp_path, capsys):
    with pytest.raises(SystemExit):
        snowmig.main(["plan", "--secure-views", "maybe", "--out-dir", str(tmp_path)])
