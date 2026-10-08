"""Snowflake tasks become generated AIDP job specs, one per task graph.

A task is a scheduler: a SQL body on a SCHEDULE, or after its PREDECESSORS.
The census has always said that a task populating a migrated table means
that table stops being populated at cutover, and PLANNED_OBJECTS.md names
the table. What nobody produced was the thing to put in the task's place.

Live shapes (SHOW TASKS, 2026-09-29, fake names): `schedule` is
`60 MINUTE`, `1440 MINUTE` or `USING CRON <expr> <tz>`, `predecessors` a
JSON array in a string, and the two live bodies are
`INSERT INTO STAGING_EVENTS (EVENT_ID, PAYLOAD) SELECT 2, 'task ran'` and a
fully qualified `INSERT INTO <db>.R3.CUSTOMERS SELECT 99, 'x'`. An
unqualified name resolves in the TASK's own schema (live-verified
2026-09-25, see test_census_writes.py).

What is generated, and what it says it is:

  * one job per task graph, its tasks in dependency order (`depends_on`,
    sent as the documented `dependsOn`), created MANUAL. The SCHEDULE is
    parsed and recorded; where it maps exactly onto a Quartz cron it is a
    PAUSED proposal, never sent. A WHEN condition is recorded and NOT
    carried: the generated job runs unconditionally when triggered;
  * a SQL DML body over migrated tables -- INSERT [OVERWRITE] ... SELECT /
    VALUES, DELETE ... WHERE, TRUNCATE -- translated by the view translator
    with every name rewritten to its target;
  * anything else -- CALL, Snowflake Scripting, MERGE, UPDATE, a body over a
    stream or a table that is not migrating, a Snowflake-only construct -- a
    STUB notebook that raises when run, and a verdict naming why. A stub
    that succeeded would read as the task having run.
"""
import json

import pytest

from plan.build import build_plan
from target.generated_jobs import (build_generated_jobs, job_body,
                                   schedule_proposal)
from test_plan_snapshots import _census, _inv, _rec


def _task(name, definition, *, schema="CORE", schedule=None,
          predecessors="[]", condition=None, state="suspended",
          overlap="false", relations=None):
    facts = {"schedule": schedule, "predecessors": [] if predecessors == "[]"
             else predecessors, "condition": condition, "warehouse": "WH_X",
             "state": state, "definition": definition,
             "allow_overlapping_execution": overlap}
    if relations is not None:
        facts["task_relations"] = relations
    return {"kind": "TASK", "source_identifier": f"DB.{schema}.{name}",
            "migratable": False, "reason": "", "detail": "",
            "source_facts": facts}


def _stream(name, table):
    return {"kind": "STREAM", "source_identifier": f"DB.CORE.{name}",
            "source_facts": {"table_name": table, "mode": "DEFAULT"}}


def _generate(tables, *objects):
    census = _census(*objects)
    inv = _inv(*[_rec(t) for t in tables], census=census)
    plan = build_plan(inv, {"edges": []})
    plan["census"] = census
    return build_generated_jobs(plan, inv)


def _graph(out, root):
    return next(j for j in out["jobs"] if j["kind"] == "task_graph"
                and j["root"] == root)


def _code(out, task):
    nb = out["notebooks"][task["notebook"]]
    return "".join("".join(c["source"]) for c in nb["cells"]
                   if c["cell_type"] == "code")


STAGING = "DB.CORE.STAGING_EVENTS"
LIVE_BODY = ("INSERT INTO STAGING_EVENTS (EVENT_ID, PAYLOAD) "
             "SELECT 2, 'task ran'")


# ------------------------------------------------------ translated bodies

def test_the_live_root_task_becomes_a_manual_job_with_its_body_translated():
    out = _generate([STAGING], _task("TSK_REFRESH_GOLD", LIVE_BODY,
                                     schedule="60 MINUTE"))
    job = _graph(out, "DB.CORE.TSK_REFRESH_GOLD")
    (task,) = job["tasks"]
    assert task["generated"] is True
    assert task["sql"] == ("INSERT INTO `db`.`core`.`staging_events` "
                           "(EVENT_ID, PAYLOAD)\nSELECT 2, 'task ran'")
    assert f"SQL = {task['sql']!r}" in _code(out, task), "held as data"
    assert job["trigger"] == "MANUAL" and job["schedule_applied"] is False
    assert job["intended_schedule"] == {
        "source": "SCHEDULE", "value": "60 MINUTE",
        "interval": {"count": 60, "unit": "MINUTE"}}
    assert job["proposed_schedule"]["quartzCronExpression"] == "0 0 * * * ?"
    assert job["proposed_schedule"]["pauseStatus"] == "PAUSED"
    assert "clock" in job["proposed_schedule_note"].lower(), \
        "a Snowflake interval runs from resume time, not on the clock"
    assert job["source_state"] == "suspended"
    assert job["warehouse"] == "WH_X"


def test_a_fully_qualified_write_is_rewritten_to_its_target():
    out = _generate(["DB.R3.CUSTOMERS"], _task(
        "TSK_SLASH", "INSERT INTO DB.R3.CUSTOMERS SELECT 99, 'x'",
        schema="R3", schedule="1440 MINUTE"))
    job = _graph(out, "DB.R3.TSK_SLASH")
    assert job["tasks"][0]["sql"] == ("INSERT INTO `db`.`r3`.`customers`\n"
                                      "SELECT 99, 'x'")
    assert job["proposed_schedule"]["quartzCronExpression"] == "0 0 0 * * ?"


def test_insert_overwrite_select_from_a_migrated_table():
    out = _generate([STAGING, "DB.CORE.ORDERS"], _task(
        "T", "insert overwrite into STAGING_EVENTS select ORDER_ID, "
             "IFF(AMOUNT > 0, 'y', 'n') from ORDERS", schedule="5 MINUTE"))
    sql = _graph(out, "DB.CORE.T")["tasks"][0]["sql"]
    assert sql.startswith("INSERT OVERWRITE TABLE `db`.`core`.`staging_events`")
    assert "FROM db.core.orders" in sql.replace("from", "FROM")
    assert "IFF" not in sql.upper().replace("IF(", ""), "dialect-translated"


def test_delete_and_truncate_are_translated():
    out = _generate([STAGING], _task("T1", "DELETE FROM STAGING_EVENTS "
                                           "WHERE EVENT_ID < 5"),
                    _task("T2", "TRUNCATE TABLE STAGING_EVENTS"))
    assert _graph(out, "DB.CORE.T1")["tasks"][0]["sql"] == (
        "DELETE FROM `db`.`core`.`staging_events` WHERE EVENT_ID < 5")
    t2 = _graph(out, "DB.CORE.T2")["tasks"][0]
    # Carried as a DELETE: the effect is the same, and Delta keeps history.
    assert t2["sql"] == "DELETE FROM `db`.`core`.`staging_events`"
    assert "TRUNCATE" in t2["verdict"]


# ------------------------------------------------------------- task graph

def test_predecessors_become_one_job_in_dependency_order():
    objs = [_task("R", LIVE_BODY, schedule="USING CRON 0 9 * * * UTC"),
            _task("A", LIVE_BODY, predecessors=["DB.CORE.R"]),
            _task("B", LIVE_BODY, predecessors=["DB.CORE.R"]),
            _task("C", LIVE_BODY, predecessors=["DB.CORE.A", "DB.CORE.B"])]
    out = _generate([STAGING], *objs)
    graphs = [j for j in out["jobs"] if j["kind"] == "task_graph"]
    assert len(graphs) == 1
    job = graphs[0]
    keys = [t["source_identifier"] for t in job["tasks"]]
    assert keys == ["DB.CORE.R", "DB.CORE.A", "DB.CORE.B", "DB.CORE.C"]
    by = {t["source_identifier"]: t for t in job["tasks"]}
    assert by["DB.CORE.C"]["depends_on"] == [by["DB.CORE.A"]["task_key"],
                                             by["DB.CORE.B"]["task_key"]]
    body = job_body(job, notebook_folder="x", cluster_key="K")
    last = body["tasks"][-1]
    assert last["dependsOn"] == [{"taskKey": by["DB.CORE.A"]["task_key"]},
                                 {"taskKey": by["DB.CORE.B"]["task_key"]}]
    assert "schedule" not in body


def test_a_predecessor_outside_the_census_is_said_not_guessed():
    out = _generate([STAGING], _task("CHILD", LIVE_BODY,
                                     predecessors=["OTHERDB.S.ROOT"]))
    job = _graph(out, "DB.CORE.CHILD")
    assert any("OTHERDB.S.ROOT" in n for n in job["notes"])


def test_an_unreadable_predecessor_list_is_said_not_read_as_a_root():
    obj = _task("CHILD", LIVE_BODY)
    obj["source_facts"].pop("predecessors")
    obj["source_facts"]["predecessors_unread"] = "[DB.CORE.R"
    out = _generate([STAGING], obj)
    job = _graph(out, "DB.CORE.CHILD")
    assert any("could not be read" in n for n in job["notes"])


# ------------------------------------------------------------------- stubs

@pytest.mark.parametrize("body,needle", [
    ("CALL DB.CORE.REFRESH_GOLD()", "DB.CORE.REFRESH_GOLD"),
    ("BEGIN INSERT INTO STAGING_EVENTS SELECT 1, 'a'; "
     "INSERT INTO STAGING_EVENTS SELECT 2, 'b'; END", "Scripting"),
    ("EXECUTE IMMEDIATE 'select 1'", "EXECUTE IMMEDIATE"),
    ("MERGE INTO STAGING_EVENTS t USING STAGING_EVENTS s ON t.EVENT_ID = "
     "s.EVENT_ID WHEN MATCHED THEN UPDATE SET PAYLOAD = s.PAYLOAD", "MERGE"),
    ("UPDATE STAGING_EVENTS SET PAYLOAD = 'x'", "UPDATE"),
    ("INSERT INTO STAGING_EVENTS SELECT * FROM STAGING_EVENTS "
     "QUALIFY ROW_NUMBER() OVER (ORDER BY EVENT_ID) = 1", "QUALIFY"),
    ("INSERT INTO NOT_MIGRATING SELECT 1", "DB.CORE.NOT_MIGRATING"),
    ("DELETE FROM STAGING_EVENTS WHERE EVENT_ID IN (SELECT 1)", "subquery"),
])
def test_an_untranslatable_body_is_a_stub_that_fails_when_run(body, needle):
    out = _generate([STAGING], _task("T", body))
    (task,) = _graph(out, "DB.CORE.T")["tasks"]
    assert task["generated"] is False
    assert task["verdict"].startswith("stub: ")
    assert needle in task["verdict"], task["verdict"]
    code = _code(out, task)
    assert "raise RuntimeError(" in code
    assert "spark.sql(" not in code


def test_a_body_over_a_stream_names_the_stream_and_the_nearest_equivalent():
    out = _generate([STAGING, "DB.CORE.ORDERS"],
                    _stream("STR_ORDERS", "DB.CORE.ORDERS"),
                    _task("T", "INSERT INTO STAGING_EVENTS SELECT ORDER_ID, "
                               "'x' FROM STR_ORDERS",
                          condition="SYSTEM$STREAM_HAS_DATA('STR_ORDERS')"))
    job = _graph(out, "DB.CORE.T")
    verdict = job["tasks"][0]["verdict"]
    assert "stream" in verdict.lower() and "STR_ORDERS" in verdict
    assert "table_changes" in verdict and "DB.CORE.ORDERS" in verdict
    assert job["condition"] == "SYSTEM$STREAM_HAS_DATA('STR_ORDERS')"
    assert any("condition" in n.lower() and "not carried" in n.lower()
               for n in job["notes"])


def test_overlap_and_a_finalizer_are_recorded_not_carried():
    out = _generate([STAGING], _task(
        "T", LIVE_BODY, overlap="true",
        relations='{"Predecessors":[],"FinalizerTask":"DB.CORE.FIN"}'))
    job = _graph(out, "DB.CORE.T")
    assert job["max_concurrent_runs"] == 1
    notes = " ".join(job["notes"])
    assert "overlapping" in notes.lower()
    assert "DB.CORE.FIN" in notes


# -------------------------------------------------------------- schedules

@pytest.mark.parametrize("schedule,cron,tz", [
    ("USING CRON 0 9 * * MON-FRI America/Chicago", "0 0 9 ? * MON-FRI",
     "America/Chicago"),
    ("USING CRON 0 9 * * 1-5 UTC", "0 0 9 ? * 2-6", "UTC"),
    ("USING CRON 0 0 1 * * UTC", "0 0 0 1 * ?", "UTC"),
    ("USING CRON */15 * * * * Europe/London", "0 0/15 * * * ?",
     "Europe/London"),
    ("USING CRON 30 2 * * 0,6 UTC", "0 30 2 ? * 1,7", "UTC"),
    ("30 MINUTE", "0 0/30 * * * ?", "UTC"),
])
def test_a_schedule_maps_to_a_paused_quartz_proposal(schedule, cron, tz):
    got = schedule_proposal(schedule)
    assert got["quartzCronExpression"] == cron
    assert got["timezoneId"] == tz
    assert got["pauseStatus"] == "PAUSED"


@pytest.mark.parametrize("schedule", [
    "USING CRON 0 9 1 * 1 UTC",       # day-of-month AND day-of-week
    "USING CRON 0 9 L * * UTC",       # last-day forms are not converted
    "USING CRON 0 9 * * 1 ",          # no time zone
    "7 MINUTE",
    "every day",
])
def test_a_schedule_with_no_exact_quartz_form_proposes_none(schedule):
    got = schedule_proposal(schedule)
    assert got["quartzCronExpression"] is None
    assert got["reason"]


def test_a_child_task_has_no_schedule_of_its_own():
    out = _generate([STAGING], _task("R", LIVE_BODY, schedule="60 MINUTE"),
                    _task("A", LIVE_BODY, predecessors=["DB.CORE.R"]))
    job = _graph(out, "DB.CORE.R")
    assert job["intended_schedule"]["value"] == "60 MINUTE"


# ------------------------------------------------------------------ totals

def test_no_census_means_no_task_jobs_and_says_so():
    inv = _inv(_rec(STAGING))
    out = build_generated_jobs(build_plan(inv, {"edges": []}), inv)
    assert out["summary"]["task_jobs"] == 0
    assert "census" in out["summary"]["tasks_note"].lower()


def test_translated_task_sql_parses_as_spark():
    sqlglot = pytest.importorskip("sqlglot", reason="dev-only parse check")
    out = _generate([STAGING, "DB.CORE.ORDERS"],
                    _task("T1", LIVE_BODY),
                    _task("T2", "DELETE FROM STAGING_EVENTS WHERE EVENT_ID < 5"),
                    _task("T3", "insert overwrite into STAGING_EVENTS "
                                "select ORDER_ID, 'x' from ORDERS"))
    for job in out["jobs"]:
        for task in job["tasks"]:
            if task.get("sql"):
                assert sqlglot.parse_one(task["sql"], dialect="spark")
    json.dumps(out)


# ------------------------------------ sources the relation check cannot see

@pytest.mark.parametrize("body,needle", [
    # The commonest task body there is: load a staged file. A stage is not
    # a relation the translator can see, so this used to come back
    # "translated" with `@stage` and `$1` carried verbatim into Spark SQL.
    ("INSERT INTO STAGING_EVENTS SELECT $1, $2 FROM @stage/file.csv",
     "stage"),
    ("INSERT INTO STAGING_EVENTS SELECT t.$1, t.$2 FROM @~/f.csv t", "stage"),
    ("INSERT INTO STAGING_EVENTS SELECT $1, $2 FROM @%STAGING_EVENTS",
     "stage"),
    # A table function: RESULT_SCAN reads the previous query's result set,
    # which only exists inside the Snowflake session that ran it.
    ("INSERT INTO STAGING_EVENTS SELECT * FROM "
     "TABLE(RESULT_SCAN(LAST_QUERY_ID()))", "table function"),
    ("insert into STAGING_EVENTS select * from table ( "
     "INFORMATION_SCHEMA.TASK_HISTORY())", "table function"),
    # A session variable is set in the Snowflake session; Spark has none.
    ("DELETE FROM STAGING_EVENTS WHERE EVENT_ID < $cutoff",
     "session variable"),
    ("INSERT INTO STAGING_EVENTS SELECT $cutoff, 'x'", "session variable"),
])
def test_a_body_reading_a_stage_or_table_function_is_a_stub(body, needle):
    out = _generate([STAGING], _task("T", body))
    (task,) = _graph(out, "DB.CORE.T")["tasks"]
    assert task["generated"] is False, task["sql"]
    assert task["verdict"].startswith("stub: ")
    assert needle in task["verdict"], task["verdict"]
    assert "raise RuntimeError(" in _code(out, task)


def test_a_stage_or_dollar_inside_a_literal_is_not_a_stage_read():
    """The check runs over code only: '@' or '$1' inside a string literal
    or a quoted identifier is data, and does not make a body a stub."""
    out = _generate([STAGING], _task(
        "T", "INSERT INTO STAGING_EVENTS SELECT 2, 'mail@x $1 table(y)'"))
    (task,) = _graph(out, "DB.CORE.T")["tasks"]
    assert task["generated"] is True, task["verdict"]


def test_a_stage_load_that_stops_is_not_reported_as_taken_over():
    """GENERATED_JOBS.md joins the plan's loads that stop at cutover to the
    task that would take each over. A stage load must read as a stub
    there, never as a translated task (I3)."""
    from report.render import render_generated_jobs

    task = _task("LOAD", "INSERT INTO STAGING_EVENTS SELECT $1, $2 "
                         "FROM @landing/events.csv")
    task["writes"] = [STAGING]
    census = _census(task)
    inv = _inv(_rec(STAGING), census=census)
    plan = build_plan(inv, {"edges": []})
    plan["census"] = census
    out = build_generated_jobs(plan, inv)
    md = render_generated_jobs(out, plan)
    section = md[md.index("## Loads that stop at cutover"):]
    line = next(l for l in section.splitlines() if STAGING in l)
    assert "STUB" in line and "(translated" not in line, line


def test_a_translated_body_carries_the_translator_warnings():
    """A body no dialect rule changed is carried verbatim: functions outside
    the rule table (CURRENT_WAREHOUSE, SYSDATE, PARSE_JSON ...) are not
    checked and may not exist in Spark 3.5. That is what the view
    translator warns, and the refresh notebooks already say it; a task's
    verdict and notebook now say it too, rather than a bare 'translated'."""
    out = _generate([STAGING], _task(
        "T", "INSERT INTO STAGING_EVENTS SELECT 1, CURRENT_WAREHOUSE()"))
    (task,) = _graph(out, "DB.CORE.T")["tasks"]
    assert task["generated"] is True
    assert "carried over unchanged" in task["verdict"], task["verdict"]
    assert any("carried over unchanged" in w for w in task["warnings"])
    nb = out["notebooks"][task["notebook"]]
    md = "".join("".join(c["source"]) for c in nb["cells"]
                 if c["cell_type"] == "markdown")
    assert "carried over unchanged" in md


def test_a_dialect_translated_body_says_so_in_its_verdict():
    out = _generate([STAGING, "DB.CORE.ORDERS"], _task(
        "T", "insert into STAGING_EVENTS select ORDER_ID, "
             "IFF(AMOUNT > 0, 'y', 'n') from ORDERS"))
    (task,) = _graph(out, "DB.CORE.T")["tasks"]
    assert task["generated"] is True
    assert "dialect-translated" in task["verdict"], task["verdict"]
