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

    def __init__(self, pending_rows: list | None = None, approved_rows: list | None = None, **kw) -> None:
        super().__init__(**kw)
        self.pending_rows = pending_rows or []
        self.approved_rows = approved_rows or []

    def run_sql(self, sql: str, timeout: int = 300) -> list[tuple]:
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
