"""Fakes and constants shared by the actuator, drain, repair and draft-pass tests, plus the duckdb translation the
ledger tests use. Plain module (not a conftest) so the test files import the names they need explicitly; pytest puts
this directory on sys.path because it has no __init__.py. No live platform calls anywhere."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone

from nightshift import draft as D
from nightshift import tiers
from nightshift.actuator import MemoryKV, TickReport, build_context, plan_actions, tick
from nightshift.config import Env
from nightshift.types import Action

OPS = Env().ops
# Late in the month so the burn-rate projection does not flag an overrun for the sample spend.
T0 = datetime(2026, 11, 28, 3, 7, 0, tzinfo=timezone.utc)
LAST_ENTRY = "2026-11-27 22:00:00"
# budget_summary_sql row: used_ocpu_hours, used_cost, llm_calls, llm_cost, last_budget_entry_at, last_llm_entry_at
ROW_NORMAL = (10.0, 550.0, 100, 40.0, LAST_ENTRY, LAST_ENTRY)        # 45 % compute, 80 % llm -> normal
ROW_LOW = (20.0, 800.0, 100, 40.0, LAST_ENTRY, LAST_ENTRY)           # 20 % -> low_compute
ROW_EMPTY_LEDGER = (0.0, 0.0, 0, 0.0, None, None)                    # no entries -> unknown


# ----------------------------------------------------------------------------- actuator fakes
class FakeIO:
    """Scripted AidpIO stand-in. Records every call; mutating calls honour dry_run like the real one."""

    def __init__(self, clusters: dict[str, dict] | None = None, jobs: list[dict] | None = None,
                 runs: list[dict] | None = None, dry_run: bool = True) -> None:
        self.env = Env()
        self.dry_run = dry_run
        self.clusters = clusters if clusters is not None else {self.env.cluster_id: {
            "key": self.env.cluster_id, "displayName": "DemoCluster", "state": "ACTIVE",
            "minWorkers": 2, "maxWorkers": 10, "autoStopInMinutes": 60, "workerShape": "amd.generic",
        }}
        self.jobs = jobs or []
        self.runs = runs or []
        self.calls: list[tuple] = []

    def get_cluster(self, key):
        self.calls.append(("get_cluster", key))
        return dict(self.clusters[key])

    def list_jobs(self):
        self.calls.append(("list_jobs",))
        return list(self.jobs)

    def get_job(self, key):
        self.calls.append(("get_job", key))
        return next(j for j in self.jobs if j["key"] == key)

    def list_recent_runs(self, hours=24):
        self.calls.append(("list_recent_runs", hours))
        return list(self.runs)

    def _mut(self, name, *args):
        self.calls.append((name, *args, {"dry_run": self.dry_run}))
        return {"dry_run": True, "op": name} if self.dry_run else {"ok": True, "op": name}

    def update_cluster_workers(self, key, min_w, max_w, auto=None):
        return self._mut("update_cluster_workers", key, min_w, max_w, auto)

    def stop_cluster(self, key):
        return self._mut("stop_cluster", key)

    def update_job(self, key, body):
        return self._mut("update_job", key, body)

    def request(self, method, path, body=None, params=None):
        return self._mut("request", method, path, body)

    def mutations(self, name=None):
        return [c for c in self.calls if c[0] in ("update_cluster_workers", "stop_cluster", "update_job", "request")
                and (name is None or c[0] == name)]


class FakeStore:
    """Records every SQL string; answers the actuator's reads from scripted data."""

    def __init__(self, budget_rows: list | None = None, budget_error: Exception | None = None,
                 wake_rows: list | None = None, replayed: bool = False, fail_on: str | None = None) -> None:
        self.budget_rows = budget_rows if budget_rows is not None else [ROW_NORMAL]
        self.budget_error = budget_error
        self.wake_rows = wake_rows or []
        self.replayed = replayed
        self.fail_on = fail_on
        self.sql: list[str] = []

    def run_sql(self, sql: str, timeout: int = 300) -> list[tuple]:
        self.sql.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"write failed: {self.fail_on}")
        if f"FROM {OPS}.ticks WHERE tick_id" in sql:
            return [("seen",)] if self.replayed else []
        if "used_ocpu_hours" in sql:
            if self.budget_error:
                raise self.budget_error
            return list(self.budget_rows)
        if sql.startswith("SELECT") and f"FROM {OPS}.wake_events WHERE consumed_by_tick IS NULL" in sql:
            return list(self.wake_rows)
        if "COUNT(*) AS n" in sql:
            return [(0,)]
        return []

    def idx(self, needle: str) -> int:
        return next(i for i, s in enumerate(self.sql) if needle in s)

    def count(self, needle: str) -> int:
        return sum(needle in s for s in self.sql)


def run(io: FakeIO, store: FakeStore, kv: MemoryKV | None = None, **kw) -> TickReport:
    return tick(io, store, now=kw.pop("now", T0), kv_store=kv or MemoryKV(), **kw)


def _ctx(io: FakeIO, row=ROW_NORMAL, kv=None):
    return build_context(io, FakeStore(budget_rows=[row]), T0, kv=kv)


# ----------------------------------------------------------------------------- approval loop
class ApprovalStore(FakeStore):
    """FakeStore that also answers the approvals reads with scripted rows."""

    def __init__(self, pending_rows: list | None = None, approved_rows: list | None = None, traced: bool = True,
                 claim_lost: bool = False, bound: bool = True, same_id: int = 1, holders: int = 1, **kw) -> None:
        super().__init__(**kw)
        self.pending_rows = pending_rows or []
        self.approved_rows = approved_rows or []
        self.bound = bound              # False: the row names no gate decision (written straight into the table)
        self.same_id = same_id          # rows carrying the approval id (more than one = a copied id)
        self.traced = traced            # False: the decision the row names is not a quarantine of its action
        self.holders = holders          # approval rows naming that decision (more than one = a copied row)
        self.claim_lost = claim_lost    # True: another drainer's claim is on the row when it is read back
        self.claims: dict[str, str] = {}

    def run_sql(self, sql: str, timeout: int = 300) -> list[tuple]:
        if sql.startswith(f"SELECT decision_id, action_signature, tick_id FROM {OPS}.approvals WHERE approval_id"):
            self.sql.append(sql)
            return [("did-q" if self.bound else None, None, "tick-q")] * self.same_id
        if "COUNT(*) AS holders" in sql:
            self.sql.append(sql)
            return [(self.holders,)]
        if "COUNT(*) AS quarantined" in sql:
            self.sql.append(sql)
            if self.fail_on and self.fail_on in sql:
                raise RuntimeError("trace read failed")
            return [(1 if self.traced else 0,)]
        if sql.startswith(f"UPDATE {OPS}.approvals SET status = 'executing'"):
            self.sql.append(sql)
            m = re.search(r"claim:([0-9a-f]+)'", sql)
            aid = re.search(r"WHERE approval_id = '([^']+)'", sql)
            self.claims[aid.group(1)] = m.group(1)
            return []
        if sql.startswith(f"SELECT status, note FROM {OPS}.approvals WHERE approval_id"):
            self.sql.append(sql)
            aid = re.search(r"WHERE approval_id = '([^']+)'", sql).group(1)
            held = "0" * 16 if self.claim_lost else self.claims.get(aid, "")
            return [("executing", f"budget.big_resize: x | executing 2026-11-28T03:07:00Z claim:{held}")]
        if sql.startswith("SELECT") and f"FROM {OPS}.approvals" in sql:
            self.sql.append(sql)
            if "status = 'approved'" in sql:
                return list(self.approved_rows)
            if "status = 'pending'" in sql:
                return list(self.pending_rows)
            return []
        return super().run_sql(sql, timeout)


def _quarantined_action(io: FakeIO) -> Action:
    """The low_compute cluster resize (10 -> 3 workers) that budget.big_resize quarantines."""
    acts = plan_actions(_ctx(io, ROW_LOW), "low_compute", tiers.default_policy_rows(), io)
    return next(a for a in acts if a.kind == "cluster_update")


def _action_json(a: Action) -> str:
    return json.dumps({"kind": a.kind, "target": a.target, "params": a.params,
                       "previous_value": a.previous_value, "reversible": a.reversible}, sort_keys=True)


def _approved_row(a: Action, by: str = "reviewer", aid: str = "ap-1", expires=None):
    return (aid, a.signature, _action_json(a), by, expires)


# ----------------------------------------------------------------------------- statement repair
TABLE = "demo.silver.sensor_reading"
UPSERT = (f"MERGE INTO {TABLE} t USING demo.bronze.sensor_reading_raw s ON t.sensor_id = s.sensor_id "
          "WHEN MATCHED AND s.ingest_ts >= t.ingest_ts THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
DSHA = hashlib.sha256(UPSERT.encode()).hexdigest()
SSHA = "s" * 64


class RepairStore(ApprovalStore):
    """ApprovalStore that also answers the replay-verdict and repair-draft reads the drain makes."""

    def __init__(self, verdict_rows, draft_row=None, **kw):
        super().__init__(**kw)
        self.verdict_rows = verdict_rows
        self.draft_row = draft_row or (UPSERT, TABLE, "inc1", SSHA, "accepted")

    def run_sql(self, sql, timeout=300):
        if f"FROM {OPS}.replay_verdicts" in sql and "ROW_NUMBER()" in sql:
            self.sql.append(sql)
            return list(self.verdict_rows)
        if f"FROM {OPS}.repair_drafts WHERE draft_id" in sql:
            self.sql.append(sql)
            return [self.draft_row]
        return super().run_sql(sql, timeout)


def _repair():
    return Action("statement_repair", TABLE, D.draft_action_params(TABLE, SSHA, "draft1", DSHA, "xai.grok-4.7"))


# ----------------------------------------------------------------------------- duckdb stand-in for Spark
def _to_duckdb(sql: str) -> str:
    """Translate the ledger's Spark SQL + Delta DDL into duckdb SQL (translation documented in test_ledger.py)."""
    sql = sql.replace(" USING DELTA", "")
    sql = re.sub(r"\nPARTITIONED BY \([^)]*\)", "", sql)
    sql = re.sub(r"\nTBLPROPERTIES \([^)]*\)", "", sql)
    sql = re.sub(r"\bSTRING\b", "VARCHAR", sql)
    sql = sql.replace("current_timestamp()", "current_timestamp")
    # Spark string escapes (\\ and \') -> duckdb standard strings (literal backslash, doubled quote)
    return sql.replace("\\\\", "\x00").replace("\\'", "''").replace("\x00", "\\")


class _DuckCon:
    """duckdb connection that translates every statement with _to_duckdb (the ledger emits Spark SQL)."""
    def __init__(self, con): self._con = con
    def execute(self, sql, *a, **k): return self._con.execute(_to_duckdb(sql), *a, **k)
    def close(self): self._con.close()


# ----------------------------------------------------------------------------- the real ledger SQL, in duckdb
class DuckStore:
    """The actuator's store over real ledger tables in duckdb: every decision, approval, action and tick row the tick
    writes is really there, and every read (pending, approved, the approval binding, the claim, the rate count) is the
    real statement. Only the budget read is scripted, like the other fakes. ``sql`` keeps the statements that ran."""

    def __init__(self, budget_row=ROW_LOW) -> None:
        import duckdb

        from nightshift import ledger

        self.con = duckdb.connect()
        self.con.create_function("unix_timestamp", lambda ts: ts.timestamp(), ["TIMESTAMP"], "DOUBLE")
        for stmt in ledger.ddl_statements(OPS):
            self.con.execute(self._t(stmt))
        self.budget_row = budget_row
        self.sql: list[str] = []

    @staticmethod
    def _t(sql: str) -> str:
        # duckdb has no ``default`` catalog: the ledger lives in a single-level schema here
        return _to_duckdb(sql).replace(OPS + ".", "nightshift_ops.").replace(f"SCHEMA IF NOT EXISTS {OPS}", "SCHEMA IF NOT EXISTS nightshift_ops")

    def run_sql(self, sql: str, timeout: int = 300) -> list[tuple]:
        self.sql.append(sql)
        if "used_ocpu_hours" in sql:
            return [self.budget_row]
        cur = self.con.execute(self._t(sql))
        return [tuple(r) for r in cur.fetchall()] if cur.description else []

    def rows(self, sql: str) -> list[tuple]:
        return self.con.execute(sql.replace("{ops}", "nightshift_ops")).fetchall()

    def forge(self, approval_id: str, action: Action, by: str = "reviewer", decision_id: str | None = None,
              status: str = "approved", when: datetime = T0, tick_id: str = "forged") -> None:
        """What a principal with INSERT on approvals can do: a row written straight into the table."""
        ts = when.replace(tzinfo=None)
        self.con.execute(
            "INSERT INTO nightshift_ops.approvals (approval_id, tick_id, action_signature, action_json, decision_id, status, "
            "approved_by, approved_at, requested_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [approval_id, tick_id, action.signature, _action_json(action), decision_id, status, by, ts, ts, ts])


# ----------------------------------------------------------------------------- flight recorder stand-ins
class _StubType:
    def __init__(self, *args) -> None:
        self.args = args


class _StubField:
    def __init__(self, name, dtype) -> None:
        self.name, self.dtype = name, dtype


class _StubStruct:
    def __init__(self, fields) -> None:
        self.fields = fields

    @property
    def names(self):
        return [f.name for f in self.fields]


def stub_spark_ui(monkeypatch, payload: dict | None) -> None:
    """What the flight-recorder cell needs outside a cluster: ``pyspark.sql.types`` and a Spark UI that answers from
    ``payload`` (keys app_id, jobs, stages, sql, executors). ``payload=None`` is a Spark UI nobody answers."""
    import io
    import sys
    import types
    import urllib.error
    import urllib.request

    spark_types = types.ModuleType("pyspark.sql.types")
    spark_types.StructType, spark_types.StructField = _StubStruct, _StubField
    for name in ("StringType", "TimestampType", "LongType", "IntegerType"):
        setattr(spark_types, name, type(name, (_StubType,), {}))
    spark_sql = types.ModuleType("pyspark.sql")
    spark_sql.types = spark_types
    pyspark = types.ModuleType("pyspark")
    pyspark.sql = spark_sql
    for name, mod in (("pyspark", pyspark), ("pyspark.sql", spark_sql), ("pyspark.sql.types", spark_types)):
        monkeypatch.setitem(sys.modules, name, mod)

    def urlopen(url, context=None, timeout=None):
        if payload is None:
            raise urllib.error.URLError("connection refused")
        path = url.split("/api/v1/applications", 1)[1]
        body = ([{"id": payload["app_id"]}] if path == "" else
                payload["jobs"] if path.endswith("/jobs") else
                payload["stages"] if "/stages" in path else
                payload["sql"] if "/sql" in path else
                payload["executors"])
        return io.BytesIO(json.dumps(body).encode("utf-8"))

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)


class FrameSink:
    """Records ``spark.createDataFrame(data, schema).write.mode("append").saveAsTable(name)``."""

    def __init__(self) -> None:
        self.saved: list[tuple[str, list[str], list[tuple]]] = []   # (table, column names, rows)
        self.types: list[list[str]] = []                             # per saved frame: the Spark type name of each column

    def createDataFrame(self, data, schema):
        sink = self

        class _Writer:
            def mode(self, _mode):
                return self

            def saveAsTable(self, name):
                sink.saved.append((name, schema.names, list(data)))
                sink.types.append([type(f.dtype).__name__ for f in schema.fields])

        class _Frame:
            write = _Writer()

        return _Frame()
