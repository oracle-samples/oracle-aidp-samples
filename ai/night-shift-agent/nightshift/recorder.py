"""Flight recorder: a notebook cell that reads the Spark UI REST API inside the kernel and appends
stage/SQL evidence to ``{ops}.flight_recorder``.

``shape_rows`` is a pure function (stdlib only) so it can be unit-tested here and is embedded
verbatim (via ``inspect.getsource``) into the generated cell, so the kernel and the tests run the
same code.

flight_recorder columns (ledger contract):
  recorded_at TIMESTAMP, job_name, run_id, app_id, kind ('stage'|'sql'), entity_id, name, status,
  duration_ms BIGINT, num_tasks INT, failed_tasks INT, shuffle_read_bytes, shuffle_write_bytes,
  spill_bytes, input_bytes, output_bytes BIGINT, details_json STRING
"""
from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from typing import Any

FLIGHT_COLUMNS = (
    "recorded_at", "job_name", "run_id", "app_id", "kind", "entity_id", "name", "status",
    "duration_ms", "num_tasks", "failed_tasks", "shuffle_read_bytes", "shuffle_write_bytes",
    "spill_bytes", "input_bytes", "output_bytes", "details_json",
)


# --------------------------------------------------------------------------- pure helpers (embedded)
def _ns_parse_ts(value):
    """Spark UI timestamps look like '2025-03-01T22:14:03.512GMT'. Returns epoch ms or None."""
    import datetime as _dt

    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    for suffix in ("GMT", "UTC", "Z"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            d = _dt.datetime.strptime(s, fmt).replace(tzinfo=_dt.timezone.utc)
            return int(d.timestamp() * 1000)
        except ValueError:
            continue
    return None


def _ns_int(value, default=0):
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def shape_rows(payload_dicts, job_name, run_id):
    """Shape Spark UI REST payloads into flight_recorder rows (one per stage, one per SQL execution).

    ``payload_dicts``: {"app_id": str, "stages": [...], "sql": [...], "jobs": [...], "executors": [...],
    "recorded_at": iso str (optional)}. Only stdlib is used so this can run inside the kernel."""
    import datetime as _dt
    import json as _json

    recorded_at = payload_dicts.get("recorded_at") or _dt.datetime.now(_dt.timezone.utc).isoformat()
    app_id = payload_dicts.get("app_id") or ""
    stage_to_job = {}
    for j in payload_dicts.get("jobs") or []:
        for sid in j.get("stageIds") or []:
            stage_to_job.setdefault(sid, j.get("jobId"))
    executors = payload_dicts.get("executors") or []
    exec_summary = {
        "count": len(executors),
        "failed_tasks": sum(_ns_int(e.get("failedTasks")) for e in executors),
        "max_memory_bytes": sum(_ns_int(e.get("maxMemory")) for e in executors),
    }
    rows = []
    for s in payload_dicts.get("stages") or []:
        sub = _ns_parse_ts(s.get("submissionTime") or s.get("firstTaskLaunchedTime"))
        done = _ns_parse_ts(s.get("completionTime"))
        duration = (done - sub) if (sub is not None and done is not None) else None
        summ = s.get("taskMetricsDistributions") or {}
        details = {
            "attempt_id": s.get("attemptId"),
            "job_id": stage_to_job.get(s.get("stageId")),
            "executor_run_time_ms": s.get("executorRunTime"),
            "executor_cpu_time_ns": s.get("executorCpuTime"),
            "num_active_tasks": s.get("numActiveTasks"),
            "num_complete_tasks": s.get("numCompleteTasks"),
            "num_killed_tasks": s.get("numKilledTasks"),
            "records_read": s.get("inputRecords"),
            "records_written": s.get("outputRecords"),
            "shuffle_read_records": s.get("shuffleReadRecords"),
            "shuffle_write_records": s.get("shuffleWriteRecords"),
            "task_duration_quantiles_ms": summ.get("duration"),
            "task_shuffle_read_quantiles": (summ.get("shuffleReadMetrics") or {}).get("readBytes"),
            "failure_reason": s.get("failureReason"),
            "description": s.get("description"),
            "details": (s.get("details") or "")[:2000],
            "executors": exec_summary,
        }
        rows.append({
            "recorded_at": recorded_at,
            "job_name": job_name,
            "run_id": run_id,
            "app_id": app_id,
            "kind": "stage",
            "entity_id": f"{s.get('stageId')}.{s.get('attemptId', 0)}",
            "name": (s.get("name") or "")[:500],
            "status": s.get("status") or "UNKNOWN",
            "duration_ms": duration,
            "num_tasks": _ns_int(s.get("numTasks")),
            "failed_tasks": _ns_int(s.get("numFailedTasks")),
            "shuffle_read_bytes": _ns_int(s.get("shuffleReadBytes")),
            "shuffle_write_bytes": _ns_int(s.get("shuffleWriteBytes")),
            "spill_bytes": _ns_int(s.get("memoryBytesSpilled")) + _ns_int(s.get("diskBytesSpilled")),
            "input_bytes": _ns_int(s.get("inputBytes")),
            "output_bytes": _ns_int(s.get("outputBytes")),
            "details_json": _json.dumps(details, default=str),
        })
    for q in payload_dicts.get("sql") or []:
        metrics = {}
        for node in q.get("nodes") or []:
            for m in node.get("metrics") or []:
                metrics[f"{node.get('nodeName')}:{m.get('name')}"] = m.get("value")
        details = {
            "running_job_ids": q.get("runningJobIds"),
            "success_job_ids": q.get("successJobIds"),
            "failed_job_ids": q.get("failedJobIds"),
            "plan": (q.get("planDescription") or "")[:4000],
            "node_metrics": dict(list(metrics.items())[:200]),
            "executors": exec_summary,
        }
        failed = len(q.get("failedJobIds") or [])
        rows.append({
            "recorded_at": recorded_at,
            "job_name": job_name,
            "run_id": run_id,
            "app_id": app_id,
            "kind": "sql",
            "entity_id": str(q.get("id")),
            "name": (q.get("description") or "")[:500],
            "status": q.get("status") or "UNKNOWN",
            "duration_ms": _ns_int(q.get("duration"), None),
            "num_tasks": 0,
            "failed_tasks": failed,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 0,
            "spill_bytes": 0,
            "input_bytes": 0,
            "output_bytes": 0,
            "details_json": _json.dumps(details, default=str),
        })
    return rows


# --------------------------------------------------------------------------- cell generator
_CELL_TEMPLATE = '''# Night Shift flight recorder (generated by nightshift.recorder.recorder_cell)
import json, os, ssl, urllib.request, datetime as _dt

{helpers}

def _ns_flight_recorder(ops, job_name, run_id, max_stages):
    summary = {{"ok": False, "ops": ops, "job_name": job_name, "run_id": run_id}}
    try:
        # the Spark UI is served by this driver (spark.ui address, same host or cluster-internal) with a certificate
        # that is not issued for that address; verification is off for THIS client only, which reads stage and SQL
        # metadata from the driver it runs on and sends nothing. Never reuse this context for an external host.
        ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE

        def get(path, default=None):
            try:
                with urllib.request.urlopen(base + path, context=ctx, timeout=30) as r:
                    return json.loads(r.read())
            except Exception as e:  # noqa: BLE001
                summary.setdefault("warnings", []).append(f"{{path}}: {{e}}")
                return default

        base = None
        try:
            base = spark.sparkContext.uiWebUrl
        except Exception:  # noqa: BLE001
            base = None
        if not base:
            for key in ("SPARK_UI_URL", "SPARK_PUBLIC_DNS"):
                if os.environ.get(key):
                    base = os.environ[key]; break
        if not base:
            try:
                port = spark.conf.get("spark.ui.port", "4040")
            except Exception:  # noqa: BLE001
                port = "4040"
            base = f"http://localhost:{{port}}"
        if not base.startswith("http"):
            base = "http://" + base
        base = base.rstrip("/")
        summary["spark_ui"] = base

        apps = get("/api/v1/applications", default=[]) or []
        if not apps:
            raise RuntimeError("no applications from Spark UI")
        app_id = apps[0]["id"]
        prefix = f"/api/v1/applications/{{app_id}}"
        jobs = get(prefix + "/jobs", default=[]) or []
        stages = get(prefix + "/stages?withSummaries=true", default=None)
        if stages is None:
            stages = get(prefix + "/stages", default=[]) or []
        stages = sorted(stages, key=lambda s: (s.get("stageId", 0), s.get("attemptId", 0)), reverse=True)[:max_stages]
        sql = get(prefix + "/sql?length=200", default=[]) or []
        executors = get(prefix + "/executors", default=[]) or []
        recorded_at = _dt.datetime.now(_dt.timezone.utc)
        rows = shape_rows({{"app_id": app_id, "jobs": jobs, "stages": stages, "sql": sql, "executors": executors,
                           "recorded_at": recorded_at.isoformat()}}, job_name, run_id)
        summary.update({{"app_id": app_id, "stages": len(stages), "sql": len(sql), "rows": len(rows)}})
        if rows:
            from pyspark.sql import types as T
            schema = T.StructType([
                T.StructField("recorded_at", T.TimestampType()), T.StructField("job_name", T.StringType()),
                T.StructField("run_id", T.StringType()), T.StructField("app_id", T.StringType()),
                T.StructField("kind", T.StringType()), T.StructField("entity_id", T.StringType()),
                T.StructField("name", T.StringType()), T.StructField("status", T.StringType()),
                T.StructField("duration_ms", T.LongType()), T.StructField("num_tasks", T.IntegerType()),
                T.StructField("failed_tasks", T.IntegerType()), T.StructField("shuffle_read_bytes", T.LongType()),
                T.StructField("shuffle_write_bytes", T.LongType()), T.StructField("spill_bytes", T.LongType()),
                T.StructField("input_bytes", T.LongType()), T.StructField("output_bytes", T.LongType()),
                T.StructField("details_json", T.StringType()),
            ])
            data = []
            for r in rows:
                r = dict(r); r["recorded_at"] = recorded_at
                data.append(tuple(r[c] for c in {columns!r}))
            spark.createDataFrame(data, schema).write.mode("append").saveAsTable(f"{{ops}}.flight_recorder")
            summary["table"] = f"{{ops}}.flight_recorder"
        summary["ok"] = True
    except Exception as e:  # noqa: BLE001 - the recorder must never fail the job
        summary["error"] = f"{{type(e).__name__}}: {{e}}"
    print(json.dumps(summary, default=str))
    return summary

_ns_flight_recorder({ops!r}, {job_name}, {run_id}, {max_stages})
'''


def recorder_cell(
    ops: str,
    job_name: str,
    run_id: str,
    max_stages: int = 200,
    *,
    job_name_expr: str | None = None,
    run_id_expr: str | None = None,
) -> str:
    """Return Python source for the flight-recorder cell. ``job_name``/``run_id`` are embedded as
    literals unless ``*_expr`` overrides supply raw Python expressions (used by the notebook file)."""
    helpers = "\n\n".join(inspect.getsource(f) for f in (_ns_parse_ts, _ns_int, shape_rows))
    return _CELL_TEMPLATE.format(
        helpers=helpers,
        columns=FLIGHT_COLUMNS,
        ops=ops,
        job_name=job_name_expr or repr(job_name),
        run_id=run_id_expr or repr(run_id),
        max_stages=int(max_stages),
    )


def summary_from_output(result: dict) -> dict[str, Any] | None:
    """Pull the printed JSON summary out of an ``nb_execute_code`` result (last JSON object line)."""
    text = "".join(str(o.get("text", "")) for o in result.get("outputs", []) if o.get("output_type") == "stream")
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
