"""The census keeps what job generation needs, from rows it already reads.

A task, a dynamic table, a materialized view and a stream were each read --
SHOW TASKS / SHOW DYNAMIC TABLES / SHOW MATERIALIZED VIEWS / SHOW STREAMS --
and then reduced to a name and one `detail` string. Everything a generated
AIDP job would need was on the row and was thrown away: the task's SCHEDULE
and PREDECESSORS (its place in a task graph), its WHEN condition, warehouse,
state and body; the dynamic table's TARGET_LAG, REFRESH_MODE and defining
query; the materialized view's query; the stream's base table and mode.
`--capture-definitions` kept a body only as an opt-in, under one key, with
no schedule beside it.

Row shapes are the live ones (SHOW output captured 2026-09-29 from a real
trial account; names replaced with fakes): `predecessors` is a JSON array
held in a STRING, `schedule` is `60 MINUTE` or `USING CRON <expr> <tz>`,
`condition` is null when the task has none, and a stream's `table_name` is
fully qualified.

Nothing new is asked of Snowflake. The facts come from the rows the census
already reads, so the read-only guard sees exactly the statements it saw
before -- and every one of them is a SHOW or a SELECT.
"""
import json

import pytest

from fake_sql import FakeSql
from snowflake_source.extract.census import build_census
from test_census_coverage import _responses


# ----------------------------------------------------- live row shapes

def _task_row(**over):
    row = {
        "created_on": "2026-09-22 12:34:01.408000-07:00",
        "name": "TSK_REFRESH_GOLD", "id": "00000000-0000-0000-0000-000000000001",
        "database_name": "DB", "schema_name": "CORE", "owner": "OWNER_ROLE",
        "comment": "", "warehouse": "WH_X", "schedule": "60 MINUTE",
        "predecessors": "[]", "state": "suspended",
        "definition": "INSERT INTO STAGING_EVENTS (EVENT_ID, PAYLOAD) "
                      "SELECT 2, 'task ran'",
        "condition": None, "allow_overlapping_execution": "false",
        "error_integration": "null", "last_committed_on": None,
        "last_suspended_on": None, "owner_role_type": "ROLE", "config": None,
        "task_relations": "{\"Predecessors\":[]}",
        "last_suspended_reason": None, "success_integration": "null",
        "scheduling_mode": None, "target_completion_interval": None,
        "execute_as_user": None, "overlap_policy": "NO_OVERLAP",
        "created_by_user": "USER_X"}
    row.update(over)
    return row


def _dynamic_row(**over):
    row = {
        "name": "DT_ORDER_ROLLUP", "database_name": "DB", "schema_name": "CORE",
        "cluster_by": "", "rows": "493", "bytes": "17920",
        "owner": "OWNER_ROLE", "target_lag": "1 day",
        "refresh_mode": "INCREMENTAL", "refresh_mode_reason": None,
        "warehouse": "WH_X", "comment": "",
        "text": "CREATE OR REPLACE DYNAMIC TABLE DB.CORE.DT_ORDER_ROLLUP "
                "lag = '1 day' refresh_mode = 'AUTO' initialize = "
                "'ON_CREATE' warehouse = WH_X AS SELECT CUSTOMER_ID, "
                "COUNT(*) AS N FROM DB.CORE.ORDERS GROUP BY CUSTOMER_ID",
        "scheduling_state": "ACTIVE", "is_iceberg": "false",
        "configured_refresh_mode": "AUTO"}
    row.update(over)
    return row


def _mv_row(**over):
    row = {
        "name": "MV_ORDER_TOTALS", "database_name": "DB", "schema_name": "CORE",
        "rows": "493", "source_table_name": "ORDERS", "invalid": "false",
        "invalid_reason": None, "behind_by": "0s", "is_secure": "false",
        "text": "CREATE OR REPLACE MATERIALIZED VIEW MV_ORDER_TOTALS AS\n"
                "SELECT CUSTOMER_ID, SUM(AMOUNT) AS TOTAL FROM ORDERS "
                "GROUP BY CUSTOMER_ID"}
    row.update(over)
    return row


def _stream_row(**over):
    row = {
        "name": "STR_ORDERS", "database_name": "DB", "schema_name": "CORE",
        "owner": "OWNER_ROLE", "comment": "",
        "table_name": "DB.CORE.ORDERS", "source_type": "Table",
        "base_tables": "DB.CORE.ORDERS", "type": "DELTA", "stale": "false",
        "mode": "DEFAULT", "stale_after": "2026-10-06 12:33:51.715000-07:00",
        "invalid_reason": "N/A"}
    row.update(over)
    return row


def _census(bodies: bool = True, **rows):
    """A census over `rows`; `bodies` is --capture-definitions, which keeps
    task bodies and dynamic-table / materialized-view queries."""
    fake = FakeSql(_responses(**rows))
    return build_census(fake, ["DB"], include_definitions=bodies), fake


def _one(census, kind):
    return next(o for o in census["objects"] if o["kind"] == kind)


# ------------------------------------------------------------------ tasks

def test_a_task_keeps_its_schedule_graph_place_and_body():
    census, _ = _census(**{"show tasks": [_task_row()]})
    facts = _one(census, "TASK")["source_facts"]
    assert facts["schedule"] == "60 MINUTE"
    assert facts["predecessors"] == [], "an empty JSON array is no parents"
    assert facts["condition"] is None, "null on the row is 'no WHEN clause'"
    assert facts["warehouse"] == "WH_X"
    assert facts["state"] == "suspended"
    assert facts["definition"].startswith("INSERT INTO STAGING_EVENTS")
    assert facts["allow_overlapping_execution"] == "false"


def test_a_body_is_kept_only_with_capture_definitions():
    """A task body or a snapshot query can carry literals (a COPY INTO's
    CREDENTIALS, an EXECUTE IMMEDIATE's password), and the census lands in
    files `provision` uploads. Without the opt-in the body is not kept, the
    record says so, and the scheduling facts a job needs still are."""
    census, _ = _census(False, **{"show tasks": [_task_row()],
                                  "show dynamic tables": [_dynamic_row()],
                                  "show materialized views": [_mv_row()]})
    task = _one(census, "TASK")
    assert "definition" not in task["source_facts"]
    assert task["source_facts"]["body_captured"] is False
    assert task["source_facts"]["schedule"] == "60 MINUTE"
    for kind in ("DYNAMIC_TABLE", "MATERIALIZED_VIEW"):
        facts = _one(census, kind)["source_facts"]
        assert "text" not in facts and facts["body_captured"] is False, kind
    kept, _ = _census(True, **{"show tasks": [_task_row()]})
    assert _one(kept, "TASK")["source_facts"]["definition"].startswith(
        "INSERT INTO STAGING_EVENTS")


@pytest.mark.parametrize("raw,expected", [
    ('["DB.CORE.TSK_ROOT"]', ["DB.CORE.TSK_ROOT"]),
    # Snowflake quotes each part when the name needs it; a quoted part keeps
    # its case, an unquoted one folds.
    (json.dumps(['"DB"."CORE"."tsk_lower"']), ["DB.CORE.tsk_lower"]),
    ('[\n  "DB.CORE.A",\n  "DB.CORE.B"\n]', ["DB.CORE.A", "DB.CORE.B"]),
    # A bare name resolves in the task's own schema, as the body's does.
    ('["TSK_ROOT"]', ["DB.CORE.TSK_ROOT"]),
    ('["OTHER.TSK_ROOT"]', ["DB.OTHER.TSK_ROOT"]),
    (["DB.CORE.TSK_ROOT"], ["DB.CORE.TSK_ROOT"]),   # a driver that decodes it
])
def test_predecessors_become_fully_qualified_task_names(raw, expected):
    census, _ = _census(**{"show tasks": [_task_row(
        name="TSK_CHILD", schedule=None, predecessors=raw)]})
    assert _one(census, "TASK")["source_facts"]["predecessors"] == expected


def test_unreadable_predecessors_are_kept_raw_and_named_not_dropped():
    """A graph edge that cannot be read is not "no parents": reading it as
    none would schedule a child task as a root."""
    census, _ = _census(**{"show tasks": [_task_row(
        name="TSK_CHILD", predecessors="[DB.CORE.TSK_ROOT")]})
    facts = _one(census, "TASK")["source_facts"]
    assert "predecessors" not in facts
    assert facts["predecessors_unread"] == "[DB.CORE.TSK_ROOT"
    assert any("TSK_CHILD" in n and "predecessors" in n
               for n in census["unreadable"])


def test_a_cron_schedule_and_a_condition_are_kept_verbatim():
    census, _ = _census(**{"show tasks": [_task_row(
        schedule="USING CRON 0 9 * * MON-FRI America/Chicago",
        condition="SYSTEM$STREAM_HAS_DATA('STR_ORDERS')")]})
    facts = _one(census, "TASK")["source_facts"]
    assert facts["schedule"] == "USING CRON 0 9 * * MON-FRI America/Chicago"
    assert facts["condition"] == "SYSTEM$STREAM_HAS_DATA('STR_ORDERS')"


def test_a_field_the_row_does_not_carry_is_absent_not_invented():
    """An older account's SHOW TASKS may lack a column. Absent stays absent:
    a None here would read as "no schedule" or "no condition"."""
    row = _task_row()
    for key in ("condition", "allow_overlapping_execution", "task_relations"):
        row.pop(key)
    census, _ = _census(**{"show tasks": [row]})
    facts = _one(census, "TASK")["source_facts"]
    assert "condition" not in facts
    assert "allow_overlapping_execution" not in facts


# ------------------------------------- dynamic tables, MVs and streams

def test_a_dynamic_table_keeps_its_lag_refresh_mode_and_query():
    census, _ = _census(**{"show dynamic tables": [_dynamic_row()]})
    facts = _one(census, "DYNAMIC_TABLE")["source_facts"]
    assert facts["target_lag"] == "1 day"
    assert facts["refresh_mode"] == "INCREMENTAL"
    assert facts["warehouse"] == "WH_X"
    assert "AS SELECT CUSTOMER_ID" in facts["text"]
    assert facts["scheduling_state"] == "ACTIVE"


def test_a_materialized_view_keeps_its_query():
    census, _ = _census(**{"show materialized views": [_mv_row()]})
    mv = _one(census, "MATERIALIZED_VIEW")
    assert mv["source_identifier"] == "DB.CORE.MV_ORDER_TOTALS"
    assert mv["source_facts"]["text"].startswith(
        "CREATE OR REPLACE MATERIALIZED VIEW")
    assert mv["source_facts"]["invalid"] == "false"


def test_a_stream_keeps_its_base_table_and_mode():
    census, _ = _census(**{"show streams": [_stream_row()]})
    facts = _one(census, "STREAM")["source_facts"]
    assert facts["table_name"] == "DB.CORE.ORDERS"
    assert facts["mode"] == "DEFAULT"
    assert facts["source_type"] == "Table"


def test_a_kind_generation_does_not_use_carries_no_facts():
    census, _ = _census(**{"show alerts": [
        {"name": "A", "schema_name": "S", "state": "started",
         "condition": "select 1"}]})
    assert "source_facts" not in _one(census, "ALERT")


# ------------------------------------------------------------- read-only

def test_no_new_statement_is_issued_and_every_one_is_a_read():
    """The facts ride on rows already read. Same statements as a census of
    the same estate with nothing in it, and all of them SHOW or SELECT."""
    _, empty = _census()
    _, full = _census(**{"show tasks": [_task_row()],
                         "show dynamic tables": [_dynamic_row()],
                         "show materialized views": [_mv_row()],
                         "show streams": [_stream_row()]})
    assert full.calls == empty.calls
    for sql in full.calls:
        assert sql.strip().split()[0].lower() in ("show", "select"), sql
