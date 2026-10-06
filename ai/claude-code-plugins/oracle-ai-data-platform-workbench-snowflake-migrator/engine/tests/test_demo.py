"""Dev mode: the full pipeline against the built-in emulation.

The demo is production code over fake transports, so these tests double as an
integration test of the whole pipeline — extractors, planner, DDL, smoke,
catalog registration and deploy in one pass, offline.
"""
import json

import pytest

from emulation.runbook import (
    DEMO_EXTERNAL_CATALOG, DEMO_STANDARD_CATALOG, run_demo)
from emulation.snowflake_fake import demo_run_sql
from report.stages import build_stage_board

_EXPECTED_ARTIFACTS = (
    "emulation.json", "inventory.json", "INVENTORY.md", "CENSUS.md",
    "dependencies.json", "maintenance.json", "MAINTENANCE.md",
    "security.json", "SECURITY.md", "warehouses.json", "compute.json",
    "COMPUTE_PROPOSAL.md", "data_options.json", "DATA_MOVEMENT_OPTIONS.md",
    "plan.json", "PLANNED_OBJECTS.md", "ddl_plan.json", "DDL_PLAN.md",
    "smoke.json", "SMOKE_TEST.md", "provision_result.json", "PROVISION.md",
    "catalog_result.json", "CATALOG.md",
    "PREFLIGHT.md", "deploy_result.json", "SOFT_CLONE_SUMMARY.md",
    "NOTEBOOK.md", "SUMMARY.md", "STAGES.md", "DEMO.md",
    f"snowmig_shallow_clone_{DEMO_STANDARD_CATALOG}.ipynb",
)


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    out = tmp_path_factory.mktemp("demo")
    result = run_demo(out)
    return out, result


def test_the_demo_writes_every_production_artifact(demo):
    out, _ = demo
    missing = [n for n in _EXPECTED_ARTIFACTS if not (out / n).exists()]
    assert missing == []


def test_the_demo_is_unmistakably_marked_as_emulated(demo):
    out, _ = demo
    marker = json.loads((out / "emulation.json").read_text(encoding="utf-8"))
    assert marker["emulated"] is True
    text = (out / "DEMO.md").read_text(encoding="utf-8")
    assert "EMULATED" in text.splitlines()[0], \
        "the banner must be the first thing a reader sees"
    assert "No Snowflake account and no AIDP DataLake were contacted" in text


def test_the_demo_estate_teaches_the_blocking_lessons(demo):
    out, _ = demo
    plan = json.loads((out / "plan.json").read_text(encoding="utf-8"))
    cannot = {r["source_identifier"]: r["category"]
              for r in plan["cannot_migrate"]}
    assert cannot["SNOWDEMO.SALES.EVENTS_RAW"] == "unmapped_type"
    assert cannot["SNOWDEMO.ANALYTICS.TOP_CUSTOMERS_VW"] == "snowflake_only_sql"
    assert cannot["SNOWDEMO.ANALYTICS.CUSTOMER_360_VW"] == "unsupported_object"
    assert plan["summary"]["can_migrate"] == 4


def test_the_demo_deploy_demonstrates_the_live_learned_behaviours(demo):
    out, result = demo
    deployed = json.loads((out / "deploy_result.json").read_text(encoding="utf-8"))
    assert deployed["catalog_type"] == "INTERNAL"
    assert sorted(deployed["verified_targets"]) == [
        "SNOWDEMO.SALES.CUSTOMERS", "SNOWDEMO.SALES.ORDERS"]
    assert deployed["poisoned_names"] == [
        f"{DEMO_STANDARD_CATALOG}.sales.legacy_audit"]
    assert deployed["derived_type_drift_targets"] == [
        "SNOWDEMO.ANALYTICS.ORDER_SUMMARY_VW"]
    drift = deployed["derived_type_drift"][0]["reason"].lower()
    assert "narrow" in drift
    assert any("refused" in line.lower() for line in result["narrative"]), \
        "the EXTERNAL refusal must be part of the story"


def test_the_demo_security_and_census_lessons_fire(demo):
    out, _ = demo
    sec = json.loads((out / "security.json").read_text(encoding="utf-8"))
    assert sec["exposure_count"] == 1
    assert sec["exposures"][0]["object"] == "SNOWDEMO.SALES.CUSTOMERS"
    assert len(sec["secure_views"]) == 1
    inv = json.loads((out / "inventory.json").read_text(encoding="utf-8"))
    kinds = inv["census"]["by_kind"]
    assert kinds.get("TASK") == 1 and kinds.get("STREAM") == 1


def test_the_demo_smoke_passes_and_the_probe_cleans_up(demo):
    out, _ = demo
    smoke = json.loads((out / "smoke.json").read_text(encoding="utf-8"))
    assert smoke["ok"] is True
    assert smoke["destination"]["write_verified"] is True
    assert smoke["destination"]["left_behind"] == []
    assert smoke["destination"]["catalog_type"] == "INTERNAL"


def test_the_demo_registers_and_verifies_the_external_catalog(demo):
    out, _ = demo
    cat = json.loads((out / "catalog_result.json").read_text(encoding="utf-8"))
    assert cat["catalog"] == DEMO_EXTERNAL_CATALOG
    assert cat["action"] == "created"
    assert cat["verified"] is True


def test_the_stage_board_reads_the_demo_run_as_complete(demo):
    out, _ = demo
    board = build_stage_board(out)
    # Every stage the demo drives has run. `preflight` is deliberately not
    # one of them: it confirms a real connection config with a real user,
    # which an emulated run has nothing to say about. Nor is `run`: it
    # starts an in-AIDP job, and the demo emulates no job runtime.
    not_run = [s["stage"] for s in board["stages"]
               if s["status"] == "NOT_RUN"]
    # Nor are the in-AIDP workflows or the token roll-up: an emulation runs
    # no AIDP job and no agent session. (`ingest` is satisfied by `assess`.)
    # `structure-workflow` is NOT satisfied by the demo's `deploy`: that
    # deploy has a failed object on purpose, and a twin that failed does
    # not stand in for the other path.
    assert not_run == ["preflight", "discover-workflow", "structure-workflow",
                       "copy-workflow", "reconcile-workflow", "publish",
                       "tokens", "teardown"], not_run
    row = next(s for s in board["stages"] if s["stage"] == "structure-workflow")
    assert row["found"].startswith("not satisfied: `deploy`")
    assert board["next_stage"] is None, \
        "an optional stage must not be proposed as next"


def test_the_demo_inventory_labels_its_row_counts_as_metadata(demo):
    # The default mode is metadata: SHOW estimates for tables, nothing for
    # views. INVENTORY.md is the per-object sign-off artifact, and it once
    # called those numbers exact and printed ERROR for every view.
    out, _ = demo
    inv = json.loads((out / "inventory.json").read_text(encoding="utf-8"))
    assert inv["row_count_mode"] == "metadata"
    md = (out / "INVENTORY.md").read_text(encoding="utf-8")
    assert "Rows (exact)" not in md
    assert "ERROR" not in md
    assert "metadata" in md
    view = next(l for l in md.splitlines()
                if "`SNOWDEMO.ANALYTICS.ORDER_SUMMARY_VW`" in l)
    assert "not counted" in view
    summary = (out / "SUMMARY.md").read_text(encoding="utf-8")
    assert "show `-`" not in summary, \
        "SUMMARY.md must not describe a blank INVENTORY.md does not print"


def test_the_demo_summary_rates_the_clustered_table_medium(demo):
    # ORDERS carries cluster_by / change_tracking / retention_time and a
    # TIMESTAMP_NTZ downgrade; DDL_PLAN.md lists all four. SUMMARY.md once
    # rated it LOW with "no properties dropped", because only the object type
    # and the row count reached the risk assessment.
    out, _ = demo
    md = (out / "SUMMARY.md").read_text(encoding="utf-8")
    orders = next(l for l in md.splitlines() if "`SNOWDEMO.SALES.ORDERS`" in l)
    assert "| MEDIUM |" in orders, orders
    # The clustering key, change tracking and retention are now CARRIED into
    # the CREATE TABLE, so SUMMARY.md no longer lists them as not applied --
    # and DDL_PLAN.md shows where they went.
    assert "not applied" not in orders, orders
    ddl_md = (out / "DDL_PLAN.md").read_text(encoding="utf-8")
    assert "CLUSTER BY (ORDER_DATE)" in ddl_md
    view = next(l for l in md.splitlines()
                if "`SNOWDEMO.ANALYTICS.ORDER_SUMMARY_VW`" in l)
    assert "| HIGH |" in view, "a view's column warnings must not pull it down"
    rollup = next(l for l in md.splitlines() if l.startswith("By risk:"))
    assert "**MEDIUM**" in rollup


def test_the_stage_board_deploy_row_adds_up(demo):
    # deploy_result.json: 4 statements, 2 verified, 1 failed (poisoned name),
    # 1 created with derived type drift. The row once read "verified 2/4,
    # 1 not verified", which does not sum, and the drift was invisible.
    out, _ = demo
    board = build_stage_board(out)
    deploy = next(s for s in board["stages"] if s["stage"] == "deploy")
    assert "verified 2/4" in deploy["found"]
    assert "1 failed" in deploy["found"]
    assert "1 created with derived type drift" in deploy["found"]
    assert deploy["attention"] is True
    assert "deploy" in board["needs_attention"]


def test_the_emulated_snowflake_refuses_a_question_it_cannot_answer():
    # A fake that improvises is how an emulation starts lying.
    with pytest.raises(ValueError, match="no answer"):
        demo_run_sql("select * from somewhere.else")


def test_the_demo_narrative_is_printable_and_nonempty(demo):
    _, result = demo
    assert len(result["narrative"]) >= 10
    assert all(isinstance(line, str) and line for line in result["narrative"])


def test_the_demo_notebook_note_routes_to_the_verified_path(demo):
    # `notebook --upload` is refused; the demo must not tell the reader that
    # production uploads through it.
    out, _ = demo
    text = (out / "NOTEBOOK.md").read_text(encoding="utf-8")
    assert "`notebook --upload` places" not in text
    assert "run --job snowmig_01_structure" in text
