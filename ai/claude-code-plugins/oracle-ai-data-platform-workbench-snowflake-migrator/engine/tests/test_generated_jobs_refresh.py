"""A refresh notebook and a job spec for every snapshot the plan can refresh.

The plan now carries a dynamic table or a materialized view as a table
snapshot, and says `refresh generated` when its defining query translates.
That verdict was a promise nothing kept: no notebook existed. This is the
generator behind it.

For each snapshot with `refresh generated`:

  * a notebook whose one statement is `INSERT OVERWRITE TABLE <target>
    <defining query>` -- the query translated by the view translator `ddl`
    uses, every reference rewritten to the migrated table. Delta supports a
    full atomic overwrite (probe 2026-09-29: MERGE, time travel and the
    other writes this relies on all run on AIDP Spark 3.5.0 / Delta 3.1.0);
  * a job spec that is MANUAL and says so: the source cadence (a dynamic
    table's TARGET_LAG) is recorded as the INTENDED one, and where it maps
    to a Quartz cron (the schedule shape the AIDP job API documents) that
    cron is written down as a PAUSED proposal -- never sent.

`refresh NOT generated` produces no notebook, and the verdict travels into
the output. A generator that disagrees with the plan (the inventory changed
after `plan`) says so and writes nothing for that object.
"""
import json

import pytest

from plan.build import build_plan
from target.generated_jobs import (build_generated_jobs, job_body,
                                   lag_schedule)
from test_plan_snapshots import (DT_TEXT, ORDERS, _census, _dt, _dt_census,
                                 _inv, _mv, _rec)


def _generate(*records, census=None, edges=(), **kw):
    inv = _inv(*records, census=census)
    plan = build_plan(inv, {"edges": list(edges)}, **kw)
    if census is not None:
        plan["census"] = census          # as cmd_plan carries it
    return build_generated_jobs(plan, inv), plan


def _job(out, source):
    return next(j for j in out["jobs"] if j["source_identifier"] == source)


def _code(notebook):
    return "".join("".join(c["source"]) for c in notebook["cells"]
                   if c["cell_type"] == "code")


DT = "DB.CORE.DT_ORDER_ROLLUP"
MV = "DB.CORE.MV_ORDER_TOTALS"


# ----------------------------------------------------------- the notebook

def test_a_refreshable_dynamic_table_gets_an_insert_overwrite_notebook():
    out, plan = _generate(_rec(ORDERS), _dt(), census=_census(_dt_census()))
    job = _job(out, DT)
    notebook = out["notebooks"][job["tasks"][0]["notebook"]]
    code = _code(notebook)
    assert "INSERT OVERWRITE TABLE `db`.`core`.`dt_order_rollup`" in code
    # The query reads the MIGRATED table, not the Snowflake one.
    assert "FROM db.core.orders" in code
    assert "DB.CORE.ORDERS" not in code
    assert "spark.sql(" in code
    assert job["refresh_sql"].startswith("INSERT OVERWRITE TABLE ")


def test_the_notebook_says_what_it_does_and_what_it_does_not():
    out, _ = _generate(_rec(ORDERS), _dt(), census=_census(_dt_census()))
    job = _job(out, DT)
    notebook = out["notebooks"][job["tasks"][0]["notebook"]]
    md = "".join("".join(c["source"]) for c in notebook["cells"]
                 if c["cell_type"] == "markdown")
    assert DT in md and "db.core.dt_order_rollup" in md
    assert "full" in md.lower() and "overwrite" in md.lower()
    assert "incremental" in md.lower(), \
        "Snowflake's INCREMENTAL refresh mode is not what this does"
    assert "not scheduled" in md.lower()


# ------------------------------------------------------------- the job

def test_the_job_is_manual_and_records_the_lag_as_intended():
    out, _ = _generate(_rec(ORDERS), _dt(), census=_census(_dt_census()))
    job = _job(out, DT)
    assert job["trigger"] == "MANUAL"
    assert job["schedule_applied"] is False
    assert job["intended_cadence"] == {"source": "TARGET_LAG",
                                       "value": "1 day"}
    proposal = job["proposed_schedule"]
    assert proposal["quartzCronExpression"] == "0 0 0 * * ?"
    assert proposal["pauseStatus"] == "PAUSED"
    assert "not applied" in job["note"].lower()
    assert job["kind"] == "refresh" and job["snapshot_of"] == "dynamic table"


def test_a_materialized_view_has_no_cadence_to_propose():
    out, _ = _generate(_rec(ORDERS), _mv())
    job = _job(out, MV)
    assert job["intended_cadence"] is None
    assert job["proposed_schedule"] is None
    assert "no cadence" in job["cadence_note"].lower()


def test_the_job_body_is_unscheduled_and_runs_the_notebook():
    out, _ = _generate(_rec(ORDERS), _dt(), census=_census(_dt_census()))
    body = job_body(_job(out, DT), notebook_folder="f/generated_jobs",
                    cluster_key="CLUSTER_KEY")
    assert "schedule" not in body, "the schedule is recorded, never sent"
    assert body["maxConcurrentRuns"] == 1
    (task,) = body["tasks"]
    assert task["type"] == "NOTEBOOK_TASK"
    assert task["notebookPath"].startswith("f/generated_jobs/")
    assert task["cluster"] == {"clusterKey": "CLUSTER_KEY"}
    assert task["runIf"] == "ALL_SUCCESS" and task["source"] == "WORKSPACE"
    assert "dependsOn" not in task


def test_a_snapshot_over_another_snapshot_runs_after_it():
    rollup2 = _dt("DB.CORE.DT_TOP")
    text = ("CREATE DYNAMIC TABLE DB.CORE.DT_TOP target_lag = DOWNSTREAM "
            "warehouse = WH_X AS SELECT CUSTOMER_ID FROM "
            "DB.CORE.DT_ORDER_ROLLUP WHERE N > 1")
    census = _census(_dt_census(),
                     _dt_census("DB.CORE.DT_TOP", text=text,
                                lag="DOWNSTREAM"))
    out, _ = _generate(_rec(ORDERS), _dt(), rollup2, census=census)
    top = _job(out, "DB.CORE.DT_TOP")
    assert top["run_after"] == [_job(out, DT)["name"]]
    assert top["proposed_schedule"] is None
    assert "downstream" in top["cadence_note"].lower()


# ---------------------------------------------------------- not generated

def test_refresh_not_generated_writes_no_notebook_and_keeps_the_verdict():
    text = DT_TEXT.replace("GROUP BY CUSTOMER_ID",
                           "QUALIFY ROW_NUMBER() OVER (ORDER BY N) = 1")
    out, _ = _generate(_rec(ORDERS), _dt(),
                       census=_census(_dt_census(text=text)))
    assert out["jobs"] == [] and out["notebooks"] == {}
    (skipped,) = out["not_generated"]
    assert skipped["source_identifier"] == DT
    assert skipped["verdict"].startswith("refresh NOT generated: ")
    assert "QUALIFY" in skipped["verdict"]


def test_the_generator_refuses_what_the_plan_did_not_promise():
    """An inventory edited after `plan` must not produce a notebook the plan
    never approved -- nor lose one silently."""
    inv = _inv(_rec(ORDERS), _mv())
    plan = build_plan(inv, {"edges": []})
    assert plan["table_snapshots"][0]["generated"] is True
    inv["inventory"][1]["view_text_show"] = (
        "create materialized view MV_ORDER_TOTALS as select * from "
        "DB.CORE.ORDERS qualify row_number() over (order by 1) = 1")
    out = build_generated_jobs(plan, inv)
    assert out["jobs"] == []
    (skipped,) = out["not_generated"]
    assert "re-run `plan`" in skipped["verdict"]


def test_a_plan_without_snapshots_generates_nothing_and_says_why():
    out = build_generated_jobs({"can_migrate": []}, {"inventory": []})
    assert out["jobs"] == [] and out["not_generated"] == []
    assert out["summary"]["refresh_jobs"] == 0


# ------------------------------------------------------------- lag -> cron

@pytest.mark.parametrize("lag,cron", [
    ("1 day", "0 0 0 * * ?"),
    ("1 days", "0 0 0 * * ?"),
    ("24 hours", "0 0 0 * * ?"),
    ("2 hours", "0 0 0/2 * * ?"),
    ("5 minutes", "0 0/5 * * * ?"),
    ("60 minutes", "0 0 * * * ?"),
    ("30 seconds", "0/30 * * * * ?"),
])
def test_a_lag_that_divides_the_clock_maps_to_a_quartz_cron(lag, cron):
    assert lag_schedule(lag)["quartzCronExpression"] == cron


@pytest.mark.parametrize("lag", ["7 minutes", "5 hours", "3 days",
                                 "DOWNSTREAM", "soon", ""])
def test_a_lag_with_no_exact_cron_proposes_none_and_says_why(lag):
    proposal = lag_schedule(lag)
    assert proposal["quartzCronExpression"] is None
    assert proposal["reason"]


def test_the_refresh_sql_parses_as_spark():
    sqlglot = pytest.importorskip("sqlglot", reason="dev-only parse check")
    out, _ = _generate(_rec(ORDERS), _dt(), _mv(),
                       census=_census(_dt_census()))
    for job in out["jobs"]:
        assert sqlglot.parse_one(job["refresh_sql"], dialect="spark")


def test_the_output_is_json_serialisable():
    out, _ = _generate(_rec(ORDERS), _dt(), census=_census(_dt_census()))
    json.dumps(out)


@pytest.mark.parametrize("query,needle", [
    ("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))", "table function"),
    ("SELECT $1 AS A FROM @landing/f.csv", "stage"),
])
def test_a_defining_query_over_a_stage_or_table_function_is_not_refreshed(
        query, needle):
    """The relation check sees tables and views only. A query that reads a
    stage or a table function has nothing on AIDP to refresh from, so the
    refresh is NOT generated, whatever the translator made of the text."""
    from target.generated_jobs import refresh_query

    res = refresh_query(
        "CREATE MATERIALIZED VIEW MV AS " + query,
        source_identifier="DB.CORE.MV", source_database="DB",
        source_schema="CORE", target_fqn="c.core.mv", name_map={})
    assert res["generated"] is False, res["select"]
    assert needle in res["reason"], res["reason"]
