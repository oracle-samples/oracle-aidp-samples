"""Recorder: shape_rows on a saved Spark UI sample payload, and the generated cell's properties.
Local only; no Spark, no live calls."""
from __future__ import annotations

import ast
import json
from types import SimpleNamespace

import pytest

from nightshift import ledger as L
from nightshift.recorder import FLIGHT_COLUMNS, _ns_parse_ts, recorder_cell, shape_rows, summary_from_output

from helpers import FrameSink, stub_spark_ui

# Trimmed from a real Spark 3.5 /api/v1 response shape (stages?withSummaries=true, sql, jobs, executors).
SAMPLE = {
    "app_id": "app-20261101221403-0007",
    "recorded_at": "2026-11-01T22:20:00+00:00",
    "jobs": [
        {"jobId": 12, "name": "saveAsTable at <stdin>:3", "status": "SUCCEEDED", "stageIds": [30, 31]},
        {"jobId": 13, "name": "count at <stdin>:9", "status": "FAILED", "stageIds": [32]},
    ],
    "stages": [
        {
            "status": "COMPLETE", "stageId": 30, "attemptId": 0, "numTasks": 8, "numActiveTasks": 0,
            "numCompleteTasks": 8, "numFailedTasks": 0, "numKilledTasks": 0,
            "executorRunTime": 5120, "executorCpuTime": 4100000000,
            "submissionTime": "2026-11-01T22:14:10.512GMT", "firstTaskLaunchedTime": "2026-11-01T22:14:10.600GMT",
            "completionTime": "2026-11-01T22:14:13.012GMT",
            "inputBytes": 1048576, "inputRecords": 1000, "outputBytes": 524288, "outputRecords": 1000,
            "shuffleReadBytes": 0, "shuffleReadRecords": 0, "shuffleWriteBytes": 65536, "shuffleWriteRecords": 1000,
            "memoryBytesSpilled": 0, "diskBytesSpilled": 0,
            "name": "Exchange hashpartitioning(order_id#12, 200)", "details": "org.apache.spark.sql...",
            "taskMetricsDistributions": {"quantiles": [0.0, 0.25, 0.5, 0.75, 1.0],
                                         "duration": [400.0, 520.0, 610.0, 700.0, 1200.0],
                                         "shuffleReadMetrics": {"readBytes": [0.0, 0.0, 0.0, 0.0, 0.0]}},
        },
        {
            "status": "COMPLETE", "stageId": 31, "attemptId": 0, "numTasks": 200, "numFailedTasks": 0,
            "submissionTime": "2026-11-01T22:14:13.100GMT", "completionTime": "2026-11-01T22:14:19.900GMT",
            "shuffleReadBytes": 65536, "shuffleWriteBytes": 0, "memoryBytesSpilled": 2048, "diskBytesSpilled": 1024,
            "inputBytes": 0, "outputBytes": 700000, "name": "MergeIntoCommand write",
        },
        {
            "status": "FAILED", "stageId": 32, "attemptId": 1, "numTasks": 4, "numFailedTasks": 4,
            "submissionTime": "2026-11-01T22:15:00.000GMT",
            "failureReason": "Job aborted due to stage failure: ExecutorLostFailure (executor 3 exited caused by OOM)",
            "name": "count at <stdin>:9", "shuffleReadBytes": 9000000, "shuffleWriteBytes": 0,
        },
    ],
    "sql": [
        {
            "id": 7, "status": "COMPLETED", "description": "MERGE INTO default.nightshift_demo.orders_silver",
            "planDescription": "== Physical Plan ==\nExecute MergeIntoCommand ...", "submissionTime": "2026-11-01T22:14:10.000GMT",
            "duration": 9800, "runningJobIds": [], "successJobIds": [12], "failedJobIds": [],
            "nodes": [{"nodeId": 1, "nodeName": "Exchange", "metrics": [{"name": "shuffle bytes written", "value": "64.0 KiB"}]}],
        },
        {
            "id": 8, "status": "FAILED", "description": "count at <stdin>:9", "duration": 61000,
            "runningJobIds": [], "successJobIds": [], "failedJobIds": [13], "nodes": [],
        },
    ],
    "executors": [
        {"id": "driver", "failedTasks": 0, "maxMemory": 4000000000},
        {"id": "1", "failedTasks": 4, "maxMemory": 8000000000},
    ],
}


def test_shape_rows_one_per_stage_and_sql():
    rows = shape_rows(SAMPLE, "orders_silver", "run-001")
    assert len(rows) == 5
    assert [r["kind"] for r in rows] == ["stage", "stage", "stage", "sql", "sql"]


def test_shape_rows_columns_match_ledger_contract():
    for r in shape_rows(SAMPLE, "j", "r"):
        assert tuple(r.keys()) == FLIGHT_COLUMNS


def test_shape_rows_stage_metrics_and_duration():
    rows = {r["entity_id"]: r for r in shape_rows(SAMPLE, "orders_silver", "run-001")}
    s30 = rows["30.0"]
    assert s30["duration_ms"] == 2500
    assert s30["num_tasks"] == 8 and s30["failed_tasks"] == 0
    assert s30["input_bytes"] == 1048576 and s30["output_bytes"] == 524288
    assert s30["shuffle_write_bytes"] == 65536
    assert s30["job_name"] == "orders_silver" and s30["run_id"] == "run-001"
    assert s30["app_id"] == SAMPLE["app_id"]
    details = json.loads(s30["details_json"])
    assert details["job_id"] == 12
    assert details["task_duration_quantiles_ms"][-1] == 1200.0
    assert details["executors"] == {"count": 2, "failed_tasks": 4, "max_memory_bytes": 12000000000}


def test_shape_rows_spill_is_memory_plus_disk():
    rows = {r["entity_id"]: r for r in shape_rows(SAMPLE, "j", "r")}
    assert rows["31.0"]["spill_bytes"] == 3072


def test_shape_rows_failed_stage_without_completion_time():
    rows = {r["entity_id"]: r for r in shape_rows(SAMPLE, "j", "r")}
    s32 = rows["32.1"]
    assert s32["status"] == "FAILED" and s32["duration_ms"] is None and s32["failed_tasks"] == 4
    assert "OOM" in json.loads(s32["details_json"])["failure_reason"]


def test_shape_rows_sql_rows():
    rows = [r for r in shape_rows(SAMPLE, "j", "r") if r["kind"] == "sql"]
    ok, bad = rows
    assert ok["entity_id"] == "7" and ok["duration_ms"] == 9800 and ok["failed_tasks"] == 0
    assert json.loads(ok["details_json"])["node_metrics"] == {"Exchange:shuffle bytes written": "64.0 KiB"}
    assert bad["status"] == "FAILED" and bad["failed_tasks"] == 1


def test_shape_rows_empty_payload():
    assert shape_rows({"app_id": "x"}, "j", "r") == []


@pytest.mark.parametrize("value,expected", [
    ("2026-11-01T22:14:10.512GMT", 1793571250512),
    ("2026-11-01T22:14:10GMT", 1793571250000),
    (1700000000000, 1700000000000),
    (None, None),
    ("garbage", None),
])
def test_parse_spark_ui_timestamp(value, expected):
    assert _ns_parse_ts(value) == expected


def test_recorder_cell_is_valid_python_and_self_contained():
    src = recorder_cell("default.nightshift_ops", "orders_silver", "run-1", max_stages=50)
    ast.parse(src)  # syntax
    assert "def shape_rows(" in src and "def _ns_parse_ts(" in src  # helpers embedded
    assert "/api/v1/applications" in src
    assert "uiWebUrl" in src and "localhost" in src
    assert 'saveAsTable(f"{ops}.flight_recorder")' in src
    assert "_ns_flight_recorder('default.nightshift_ops', 'orders_silver', 'run-1', 50)" in src
    assert "import nightshift" not in src and "from nightshift" not in src


def test_recorder_cell_accepts_expressions_for_notebook_placeholders():
    src = recorder_cell("default.nightshift_ops", "x", "y", job_name_expr="JOB_NAME", run_id_expr="RUN_ID")
    ast.parse(src)
    assert "_ns_flight_recorder('default.nightshift_ops', JOB_NAME, RUN_ID, 200)" in src


def test_rendered_notebook_cell_matches_generator():
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "notebooks" / "flight_recorder_cell.py"
    text = path.read_text(encoding="utf-8")
    ast.parse(text)
    assert '"{{job.name}}"' in text and '"{{job.run_id}}"' in text and '"manual"' in text
    body = recorder_cell("default.nightshift_ops", "x", "y", job_name_expr="JOB_NAME", run_id_expr="RUN_ID")
    assert body.strip() in text


def test_summary_from_output_picks_last_json_line():
    result = {"outputs": [
        {"output_type": "stream", "text": "noise\n"},
        {"output_type": "stream", "text": '{"ok": true, "rows": 5}\n'},
    ]}
    assert summary_from_output(result) == {"ok": True, "rows": 5}
    assert summary_from_output({"outputs": []}) is None


# ----------------------------------------------------------------------------- the cell, run against a stand-in UI
def _run_cell(monkeypatch, payload):
    """Execute the generated cell the way the heartbeat does; returns its namespace and what it wrote."""
    stub_spark_ui(monkeypatch, payload)
    sink = FrameSink()
    spark = SimpleNamespace(sparkContext=SimpleNamespace(uiWebUrl="http://driver:4040"), createDataFrame=sink.createDataFrame)
    namespace = {"spark": spark, "__name__": "__fr__"}
    exec(compile(recorder_cell("default.nightshift_ops", "orders_silver", "run-1"), "flight_recorder_cell", "exec"), namespace)
    return namespace, sink


def test_the_cell_writes_every_column_of_the_table(monkeypatch, capsys):
    _, sink = _run_cell(monkeypatch, SAMPLE)

    (table, columns, rows), = sink.saved
    assert table == "default.nightshift_ops.flight_recorder"
    assert columns == [c for c, _ in L.COLUMNS["flight_recorder"]]
    assert all(len(row) == len(columns) for row in rows)
    spark_type = {"STRING": "StringType", "TIMESTAMP": "TimestampType", "BIGINT": "LongType", "INT": "IntegerType"}
    assert sink.types == [[spark_type[t] for _, t in L.COLUMNS["flight_recorder"]]]


def test_a_recorded_row_carries_the_ledgers_record_id_and_a_created_at(monkeypatch, capsys):
    _, sink = _run_cell(monkeypatch, SAMPLE)

    (_, columns, rows), = sink.saved
    records = [dict(zip(columns, row)) for row in rows]
    assert records
    for r in records:
        assert r["record_id"] == L._sha16(r["run_id"], r["kind"], r["entity_id"])
        assert r["created_at"] is not None and r["created_at"] == r["recorded_at"]
    assert len({r["record_id"] for r in records}) == len(records)


def test_the_cell_leaves_its_summary_for_the_code_that_ran_it(monkeypatch, capsys):
    namespace, sink = _run_cell(monkeypatch, SAMPLE)

    assert namespace["_ns_summary"]["ok"] is True
    assert namespace["_ns_summary"]["rows"] == len(sink.saved[0][2])


def test_an_unreachable_spark_ui_is_in_the_summary_and_nothing_is_written(monkeypatch, capsys):
    namespace, sink = _run_cell(monkeypatch, None)

    assert namespace["_ns_summary"]["ok"] is False
    assert "no applications from Spark UI" in namespace["_ns_summary"]["error"]
    assert sink.saved == []
