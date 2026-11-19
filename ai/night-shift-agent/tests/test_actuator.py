"""Actuator tick with FakeIO + in-memory FakeStore (real tiers/gate/ledger modules), plus AidpIO unit
tests with a fake client. No live platform calls."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from nightshift import actuator, tiers
from nightshift import ledger as L
from nightshift.actuator import MemoryKV, TickReport, action_from_json, build_context, compute_tick_id, derive_budget, plan_actions
from nightshift.aidp_io import AidpIO, parse_sql_result
from nightshift.config import Env
from nightshift.types import Action, Rule

from helpers import (LAST_ENTRY, OPS, ROW_EMPTY_LEDGER, ROW_LOW, ROW_NORMAL, T0, ApprovalStore, FakeIO, FakeStore, _action_json, _approved_row, _ctx,
                     _quarantined_action, run)


# ----------------------------------------------------------------------------- tick behaviour
def test_dry_run_decisions_before_actions_and_tick_row_last():
    io, store = FakeIO(), FakeStore()
    report = run(io, store)
    assert isinstance(report, TickReport) and report.dry_run is True
    assert report.tier == "normal"
    assert [a.kind for a in report.actions] == ["cluster_update"]
    assert report.decisions[0].decision == "allow"
    assert store.idx(f"INSERT INTO {OPS}.policy_decisions") < store.idx(f"INSERT INTO {OPS}.actions")
    assert store.sql[-1].startswith(f"INSERT INTO {OPS}.ticks")
    assert store.count(f"INSERT INTO {OPS}.ticks") == 1
    assert io.mutations() == [("update_cluster_workers", io.env.cluster_id, 1, 6, 30, {"dry_run": True})]
    assert report.outcomes[report.actions[0].signature]["dry_run"] is True
    assert report.actions[0].previous_value == {"min_workers": 2, "max_workers": 10, "auto_terminate_min": 60}
    assert "'dry_run'" not in store.sql[-1] and "TRUE" in store.sql[-1]  # ticks.dry_run = TRUE


def test_apply_mode_disables_dry_run_on_io():
    io = FakeIO()
    report = run(io, FakeStore(), dry_run=False)
    assert report.dry_run is False and io.dry_run is False
    assert io.mutations() == [("update_cluster_workers", io.env.cluster_id, 1, 6, 30, {"dry_run": False})]


def test_quarantine_opens_approval_and_skips_action():
    io, store = FakeIO(), FakeStore(budget_rows=[ROW_LOW])  # low_compute: 10 -> 3 max workers = x0.3
    report = run(io, store)
    assert report.tier == "low_compute"
    assert report.approvals_opened == 1
    d = report.decisions[0]
    assert d.decision == "quarantine" and d.rule_id == "budget.big_resize"
    assert store.count(f"INSERT INTO {OPS}.approvals") == 1
    assert store.count(f"INSERT INTO {OPS}.actions") == 0
    assert io.mutations() == []
    assert store.idx(f"INSERT INTO {OPS}.policy_decisions") < store.idx(f"INSERT INTO {OPS}.approvals")
    assert store.idx(f"INSERT INTO {OPS}.approvals") < store.idx(f"INSERT INTO {OPS}.ticks")


def test_first_deny_wins_over_quarantine():
    io, store = FakeIO(), FakeStore(budget_rows=[ROW_LOW])
    rules = [Rule("budget.big_resize", 500, {"kind": "cluster_update"}, "quarantine"),
             Rule("protect.cluster", 5, {"kind": "cluster_update", "target": io.env.cluster_id}, "deny")]
    report = run(io, store, rules=rules)
    assert report.decisions[0].decision == "deny" and report.decisions[0].rule_id == "protect.cluster"
    assert report.approvals_opened == 0 and store.count(f"INSERT INTO {OPS}.actions") == 0
    assert any(n.startswith("denied:") for n in report.notes)


def test_loop_guard_stops_third_identical_tick():
    io, kv = FakeIO(), MemoryKV()
    r1 = run(io, FakeStore(), kv)
    r2 = run(io, FakeStore(), kv, now=T0 + timedelta(minutes=30))
    r3 = run(io, FakeStore(), kv, now=T0 + timedelta(minutes=60))
    assert len(r1.actions) == 1 and len(r2.actions) == 1
    assert r1.actions[0].signature == r2.actions[0].signature
    assert r3.actions == []
    assert any("loop guard" in n for n in r3.notes)
    assert len({r1.tick_id, r2.tick_id, r3.tick_id}) == 3
    assert kv.data["sig_streak"][r1.actions[0].signature] == 3


def test_unknown_budget_never_plans_paused_action():
    io = FakeIO(jobs=[{"key": "j1", "name": "bronze_orders", "schedule": None}])
    kv = MemoryKV({"prev_tier": "paused"})  # resume lock keeps us paused while budget is unknown
    store = FakeStore(budget_rows=[ROW_EMPTY_LEDGER])
    report = run(io, store, kv)
    assert report.tier == "paused"
    assert not [a for a in report.actions if a.kind == "job_disable"]
    assert any("budget: unknown" in n for n in report.notes)
    assert any("resume lock holds" in n for n in report.notes)
    assert "'unknown'" in store.sql[-1]


def test_paused_tier_plans_disable_and_stop():
    io = FakeIO(jobs=[{"key": "j1", "name": "bronze_orders", "schedule": None}])
    report = run(io, FakeStore(), MemoryKV({"prev_tier": "paused"}))
    assert report.tier == "paused"
    kinds = {a.kind for a in report.actions}
    assert {"cluster_update", "cluster_stop", "job_disable"} <= kinds
    by_kind = {d.action_signature: d for d in report.decisions}
    disable = next(a for a in report.actions if a.kind == "job_disable")
    assert by_kind[disable.signature].decision == "allow"           # budget.disable_pipeline allows at paused
    assert report.outcomes[disable.signature] == {"skipped": "requires_schedule_body"}
    assert io.mutations("stop_cluster") == [("stop_cluster", io.env.cluster_id, {"dry_run": True})]
    assert report.approvals_opened == 1                              # 10 -> 1 workers is a big resize


def test_build_context_unknown_then_cached_budget():
    io = FakeIO()
    ctx = build_context(io, FakeStore(budget_rows=[ROW_EMPTY_LEDGER]), T0)
    assert ctx.budget_source == "unknown" and ctx.budget_remaining_pct is None
    kv = {"last_known_budget": {"remaining_pct": 22.0, "projected_overrun_pct": 5.0, "llm_remaining_pct": 50.0,
                                "as_of": "2026-11-28T02:30:00+00:00"}}
    ctx2 = build_context(io, FakeStore(budget_error=RuntimeError("kernel down")), T0, kv=kv)
    assert ctx2.budget_source == "cached" and ctx2.budget_remaining_pct == 22.0
    assert ctx2.cluster_states == {io.env.cluster_id: "ACTIVE"}
    assert ctx2.kv["active_runs"] == {io.env.cluster_id: 0}


def test_cached_budget_survives_across_ticks_via_kv():
    io, kv = FakeIO(), MemoryKV()
    run(io, FakeStore(), kv)
    store2 = FakeStore(budget_error=RuntimeError("down"))
    report = run(io, store2, kv, now=T0 + timedelta(minutes=30))
    assert "'cached'" in store2.sql[-1]
    assert report.tier == "normal"


def test_replay_same_window_is_idempotent():
    io, store = FakeIO(), FakeStore(replayed=True)
    report = run(io, store)
    assert report.actions == [] and any("replay" in n for n in report.notes)
    assert len(store.sql) == 1 and store.count("INSERT") == 0
    assert io.calls == []


def test_tick_id_floors_to_30_minutes():
    ws = "ws-1"
    a = compute_tick_id(datetime(2026, 11, 2, 3, 7, tzinfo=timezone.utc), ws)
    b = compute_tick_id(datetime(2026, 11, 2, 3, 29, 59, tzinfo=timezone.utc), ws)
    c = compute_tick_id(datetime(2026, 11, 2, 3, 30, tzinfo=timezone.utc), ws)
    assert a == b != c and len(a) == 64
    assert compute_tick_id(T0, "ws-2") != a


def test_wake_events_consumed_before_tick_row():
    store = FakeStore(wake_rows=[("w1", "monitor", "job failed", "bronze", "{}"), ("w2", "monitor", "drift", None, "{}")])
    report = run(FakeIO(), store)
    assert report.wake_consumed == 2
    assert store.idx(f"UPDATE {OPS}.wake_events SET consumed_by_tick") < store.idx(f"INSERT INTO {OPS}.ticks")


def test_active_runs_block_cluster_stop():
    io = FakeIO(runs=[{"key": "r1", "state": "RUNNING"}, {"key": "r0", "state": "FAILED"}])
    ctx = build_context(io, FakeStore(), T0)
    assert ctx.kv["active_runs"] == {io.env.cluster_id: 1}
    assert [r["key"] for r in ctx.recent_failures] == ["r0"]
    # planner never proposes a stop while runs are active...
    report = run(io, FakeStore(), MemoryKV({"prev_tier": "paused"}))
    assert "cluster_stop" not in [a.kind for a in report.actions]
    assert io.mutations("stop_cluster") == []
    # ...and the gate's safety.active_runs rule denies one anyway (defence in depth)
    from nightshift import gate
    from nightshift.types import Action
    ctx.kv["tier"] = "paused"
    engine = gate.PolicyEngine(gate.default_rules(), lambda d, a: None)
    d = engine.evaluate(Action("cluster_stop", io.env.cluster_id, {"reason": "idle"}), ctx)
    assert d.decision == "deny" and d.rule_id == "safety.active_runs"


def test_fail_closed_when_decision_row_cannot_be_written():
    io, store = FakeIO(), FakeStore(fail_on=f"INSERT INTO {OPS}.policy_decisions")
    report = run(io, store)
    assert report.decisions[0].decision == "deny" and report.decisions[0].reason == "AUDIT_WRITE_FAILED"
    assert io.mutations() == []
    assert store.count(f"INSERT INTO {OPS}.actions") == 0
    assert store.sql[-1].startswith(f"INSERT INTO {OPS}.ticks")


# ----------------------------------------------------------------------------- planning
def test_plan_actions_cluster_diff_and_no_diff():
    io = FakeIO()
    rows = tiers.default_policy_rows()
    acts = plan_actions(_ctx(io), "normal", rows, io)
    assert [a.kind for a in acts] == ["cluster_update"]
    assert acts[0].params == {"min_workers": 1, "max_workers": 6, "auto_terminate_min": 30, "shape": "amd.generic"}
    io.clusters[io.env.cluster_id].update({"minWorkers": 1, "maxWorkers": 6, "autoStopInMinutes": 30})
    assert plan_actions(_ctx(io), "normal", rows, io) == []


def test_plan_actions_stops_idle_cluster_at_critical_only():
    io = FakeIO()
    rows = tiers.default_policy_rows()
    assert "cluster_stop" not in [a.kind for a in plan_actions(_ctx(io, ROW_LOW), "low_compute", rows, io)]
    acts = plan_actions(_ctx(io, ROW_LOW), "critical", rows, io)
    stop = next(a for a in acts if a.kind == "cluster_stop")
    assert stop.previous_value == {"state": "ACTIVE"} and stop.reversible is True


def test_cadence_change_marked_requires_schedule_body_and_not_executed():
    io = FakeIO(jobs=[{"key": "j1", "name": "bronze_orders", "schedule": None},
                      {"key": "j2", "name": "adhoc_thing", "schedule": None}])  # no pipeline class -> ignored
    acts = plan_actions(_ctx(io), "normal", tiers.default_policy_rows(), io)
    sched = [a for a in acts if a.kind == "job_update_schedule"]
    assert len(sched) == 1 and sched[0].target == "j1"
    assert sched[0].params == {"cron": "0 * * * *", "enabled": True, "sla_protected": False, "requires_schedule_body": True}
    report = run(io, FakeStore())
    assert any("schedule body unknown" in n for n in report.notes)
    assert io.mutations("update_job") == []
    assert "'planned_only'" in next(s for s in report_sql(report, io) if "job_update_schedule" in s)


def report_sql(report: TickReport, io: FakeIO) -> list[str]:
    store = FakeStore()
    run(io, store)
    return [s for s in store.sql if s.startswith(f"INSERT INTO {OPS}.actions")]


def test_sla_protected_job_schedule_change_is_denied():
    io = FakeIO(jobs=[{"key": "j1", "name": "gold_kpis", "schedule": None}])
    report = run(io, FakeStore(), MemoryKV({"sla_protected_jobs": ["gold_kpis"]}))
    d = next(d for d, a in zip(report.decisions, report.actions) if a.kind == "job_update_schedule")
    assert d.decision == "deny" and d.rule_id == "protect.sla"


def test_derive_budget():
    b = derive_budget(ROW_NORMAL, T0, 1000.0, 200.0)
    assert b["known"] and b["remaining_pct"] == 45.0 and b["llm_remaining_pct"] == 80.0
    assert b["projected_overrun_pct"] == 0.0
    early = derive_budget(ROW_NORMAL, datetime(2026, 11, 2, 12, tzinfo=timezone.utc), 1000.0, 200.0)
    assert early["projected_overrun_pct"] > 100.0  # 550 spent in 1.5 days projects far past 1000
    assert derive_budget(ROW_EMPTY_LEDGER, T0, 1000.0, 200.0)["known"] is False
    assert derive_budget({"used_cost": 1200.0, "llm_cost": 0.0, "last_budget_entry_at": LAST_ENTRY}, T0, 1000.0, 200.0)["remaining_pct"] == 0.0


def test_report_to_json_roundtrip():
    report = run(FakeIO(), FakeStore())
    d = json.loads(report.to_json())
    assert d["tick_id"] == report.tick_id and d["actions"][0]["signature"] == report.actions[0].signature


def test_file_kv_roundtrip(tmp_path):
    kv = actuator.FileKV(tmp_path / "state" / "kv.json")
    assert kv.load() == {}
    kv.save({"prev_tier": "normal", "sig_streak": {"abc": 2}})
    assert kv.load() == {"prev_tier": "normal", "sig_streak": {"abc": 2}}


# ----------------------------------------------------------------------------- AidpIO with a fake client

def _echo_mark(code: str) -> str:
    """Return the per-call marker the real run_sql embedded in the submitted code (nonce included)."""
    import re as _re
    m = _re.search(r"__NS_ROWS__[0-9a-f]{8}", code)
    return m.group(0) if m else "__NS_ROWS__"


class FakeClient:
    def __init__(self, metrics: Any = None, sql_rows: Any = None) -> None:
        self.calls: list[tuple] = []
        self.metrics = metrics if metrics is not None else {"results": []}
        self.sql_rows = sql_rows if sql_rows is not None else [["a", 1], ["b", 2]]
        self.sessions_created = 0

    def summarize_metrics(self, key, **body):
        self.calls.append(("summarize_metrics", key, body))
        return self.metrics

    def update_job(self, key, **body):
        self.calls.append(("update_job", key, body))
        return {"key": key}

    def update_cluster(self, key, **body):
        self.calls.append(("update_cluster", key, body))
        return {"key": key}

    def stop_cluster(self, key):
        self.calls.append(("stop_cluster", key))
        return {}

    def nb_create_session(self, path, name, cluster_id):
        self.sessions_created += 1
        self.calls.append(("nb_create_session", path, name, cluster_id))
        return {"id": "sess-1", "kernel": {"id": "kern-1"}}

    def nb_execute_code(self, kernel_id, session_id, code, timeout=120):
        self.calls.append(("nb_execute_code", kernel_id, session_id, timeout))
        assert "spark.sql(" in code
        return {"status": "ok", "outputs": [
            {"output_type": "spark_event", "text": "[{\"id\": 3}]"},
            {"output_type": "stream", "text": _echo_mark(code) + json.dumps(self.sql_rows) + _echo_mark(code) + chr(10)},
        ]}

    def nb_delete_session(self, session_id):
        self.calls.append(("nb_delete_session", session_id))

    def list_recent_job_runs(self, **kw):
        now = datetime.now(timezone.utc)
        return {"items": [
            {"key": "old", "state": "FAILED", "timeStarted": (now - timedelta(hours=40)).isoformat()},
            {"key": "new", "state": "FAILED", "timeStarted": (now - timedelta(hours=2)).isoformat()},
            {"key": "undated", "state": "SUCCEEDED"},
        ]}


def test_summarize_metrics_empty_results_is_none_not_zero():
    fc = FakeClient(metrics={"results": []})
    io = AidpIO(Env(), client=fc)
    assert io.summarize_metrics("c1", "CpuUtilization", "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z") is None
    assert fc.calls[0][2] == {"metricName": "CpuUtilization", "timeBegin": "2026-11-01T00:00:00Z",
                              "timeEnd": "2026-11-01T01:00:00Z", "aggregationType": "MEAN", "interval": "5m"}
    assert AidpIO.metric_mean(None) is None


def test_summarize_metrics_returns_results_and_mean():
    fc = FakeClient(metrics={"results": [
        {"name": "CpuUtilization", "aggregatedDataPoints": [{"timestamp": "t", "value": 10.0}, {"timestamp": "t", "value": 30.0}]},
        {"name": "CpuUtilization", "aggregatedDataPoints": [{"timestamp": "t", "value": 20.0}]},
    ]})
    io = AidpIO(Env(), client=fc)
    res = io.summarize_metrics("c1", "MemoryUtilization", "b", "e", aggregation="MAX", interval="1h")
    assert len(res) == 2 and AidpIO.metric_mean(res) == 20.0


@pytest.mark.parametrize("kwargs", [
    {"aggregation": "AVG"}, {"aggregation": "COUNT"}, {"interval": "30s"}, {"interval": "1w"},
])
def test_summarize_metrics_invalid_enum_raises_before_any_call(kwargs):
    fc = FakeClient()
    io = AidpIO(Env(), client=fc)
    with pytest.raises(ValueError):
        io.summarize_metrics("c1", "CpuUtilization", "b", "e", **kwargs)
    assert fc.calls == []


def test_summarize_metrics_unknown_metric_name_raises_before_call():
    fc = FakeClient()
    with pytest.raises(ValueError):
        AidpIO(Env(), client=fc).summarize_metrics("c1", "BogusMetric", "b", "e")
    assert fc.calls == []


def test_mutations_honour_dry_run_by_default():
    fc = FakeClient()
    io = AidpIO(Env(), client=fc)
    assert io.dry_run is True
    assert io.update_job("j", {"name": "x"})["dry_run"] is True
    assert io.stop_cluster("c")["dry_run"] is True
    assert io.update_cluster_workers("c", 1, 3, 15)["dry_run"] is True
    assert fc.calls == []
    io.dry_run = False
    io.stop_cluster("c")
    io.update_cluster_workers("c", 1, 3, 15)
    assert fc.calls[0] == ("stop_cluster", "c")
    assert fc.calls[1][2] == {"workerConfig": {"minWorkerCount": 1, "maxWorkerCount": 3}, "autoTerminationMinutes": 15}


def test_run_sql_single_lazy_session_and_close():
    fc = FakeClient(sql_rows=[["2026-11-02T03:00:00", 1.5, None]])
    env = Env()
    io = AidpIO(env, client=fc)
    assert fc.sessions_created == 0
    rows = io.run_sql("SELECT 1")
    rows2 = io.run_sql("SELECT 2", timeout=10)
    assert rows == [("2026-11-02T03:00:00", 1.5, None)] and rows2 == rows
    assert fc.sessions_created == 1
    assert fc.calls[0] == ("nb_create_session", env.probe_notebook, "_nightshift_runner.ipynb", env.cluster_id)
    assert fc.calls[2][3] == 10
    io.close()
    assert fc.calls[-1] == ("nb_delete_session", "sess-1")
    io.close()  # idempotent
    assert fc.calls[-1] == ("nb_delete_session", "sess-1")


def test_parse_sql_result_errors_and_empty():
    with pytest.raises(RuntimeError):
        parse_sql_result({"error": {"ename": "AnalysisException", "evalue": "table not found"}, "outputs": []})
    # no marker = the statement is not confirmed to have completed, whatever status the kernel reports
    with pytest.raises(RuntimeError, match="not confirmed"):
        parse_sql_result({"status": "ok", "outputs": []})
    with pytest.raises(RuntimeError):
        parse_sql_result({"status": "error", "outputs": []})
    with pytest.raises(RuntimeError, match="not confirmed"):
        parse_sql_result({"outputs": [{"output_type": "stream", "text": "some other output\n"}]})
    # a write prints the marker around an empty list
    done = {"status": "ok", "outputs": [{"output_type": "stream", "text": "__M__[]__M__\n"}]}
    assert parse_sql_result(done, "__M__") == []


def test_list_recent_runs_window_filter_and_failures():
    io = AidpIO(Env(), client=FakeClient())
    keys = [r["key"] for r in io.list_recent_runs(hours=24)]
    assert keys == ["new", "undated"]
    assert [r["key"] for r in io.recent_failures()] == ["new"]


class RunsClient:
    """Job-runs endpoint as the Workbench serves it: one page of history, and a status filter."""

    def __init__(self, history, active=None, fail_status=None):
        self.history, self.active, self.fail_status, self.calls = history, active or {}, fail_status, []

    def list_job_runs(self, **kw):
        self.calls.append(kw)
        if "status" in kw:
            if kw["status"] == self.fail_status:
                raise RuntimeError("jobRuns 429")
            return {"items": list(self.active.get(kw["status"], []))}
        return self.history


def _ms(hours_ago: float) -> int:
    return int((datetime.now(timezone.utc) - timedelta(hours=hours_ago)).timestamp() * 1000)


def test_a_run_still_active_is_never_dropped_for_being_old():
    """The window filter applies to finished runs only: a run that started 30 hours ago and is still running stays."""
    hist = {"items": [{"key": "old-done", "state": {"status": "SUCCESS"}, "startTime": _ms(30)},
                      {"key": "old-running", "state": {"status": "RUNNING"}, "startTime": _ms(30)},
                      {"key": "old-odd", "state": {"status": "DISPATCHING"}, "timeCreated": _ms(40)},
                      {"key": "new-done", "state": {"status": "SUCCESS"}, "startTime": _ms(1)}]}
    io = AidpIO(Env(), client=RunsClient(hist))
    assert [r["key"] for r in io.list_recent_runs(hours=24)] == ["old-running", "old-odd", "new-done"]
    assert io.client.calls[0] == {"limit": 100}


@pytest.mark.parametrize("payload", [None, {"code": "TooManyRequests", "message": "slow down"}, {"jobRuns": []}, {"items": None}, "oops", 7])
def test_an_unrecognised_run_list_response_is_an_error_not_an_empty_list(payload):
    class Bad:
        def list_job_runs(self, **kw):
            return payload

        def list_recent_job_runs(self, **kw):
            return payload

    io = AidpIO(Env(), client=Bad())
    with pytest.raises(RuntimeError, match="not recognised"):
        io.list_recent_runs()
    with pytest.raises(RuntimeError, match="not recognised"):
        io.list_active_runs()
    # and through the tick: unknown, so nothing is stopped or resized
    io.dry_run = False
    notes: list[str] = []
    ctx = build_context(_ClusterIO(io), FakeStore(), T0, notes=notes)
    assert ctx.kv["active_runs"] == {} and any("active runs unknown" in n for n in notes)


class _ClusterIO:
    """AidpIO's run reads over a scripted cluster body (the tick needs get_cluster too)."""

    def __init__(self, io):
        self._io, self.env = io, io.env

    def get_cluster(self, key):
        return {"key": key, "state": "ACTIVE", "minWorkers": 2, "maxWorkers": 10, "workerShape": "amd.generic"}

    def list_recent_runs(self, hours=24):
        return self._io.list_recent_runs(hours)

    def list_active_runs(self):
        return self._io.list_active_runs()


def test_active_runs_are_asked_for_by_status_so_a_page_of_history_cannot_hide_one():
    """The newest page of history holds only finished runs; a run that started before that page is still running.
    The status query finds it, the cluster is busy, and a failed status query means unknown."""
    hist = {"items": [{"key": f"done-{i}", "state": {"status": "SUCCESS"}, "startTime": _ms(1)} for i in range(100)]}
    long_run = {"key": "long", "state": {"status": "RUNNING"}, "startTime": _ms(72)}
    io = AidpIO(Env(), client=RunsClient(hist, active={"RUNNING": [long_run], "BLOCKED": [{"key": "b1", "state": {"status": "BLOCKED"}}]}))
    assert [r["key"] for r in io.list_active_runs()] == ["long", "b1"]
    assert [c.get("status") for c in io.client.calls] == ["RUNNING", "PENDING", "QUEUED", "BLOCKED"]
    ctx = build_context(_ClusterIO(io), FakeStore(), T0)
    assert ctx.kv["active_runs"] == {io.env.cluster_id: 2}

    # the same run in both lists is counted once
    both = AidpIO(Env(), client=RunsClient({"items": [long_run]}, active={"RUNNING": [long_run]}))
    assert build_context(_ClusterIO(both), FakeStore(), T0).kv["active_runs"] == {both.env.cluster_id: 1}

    broken = AidpIO(Env(), client=RunsClient(hist, fail_status="QUEUED"))
    notes: list[str] = []
    assert build_context(_ClusterIO(broken), FakeStore(), T0, notes=notes).kv["active_runs"] == {}
    assert any("active runs unknown" in n for n in notes)


def test_a_run_on_a_cluster_given_as_a_number_counts_against_that_cluster():
    io = FakeIO(runs=[{"key": "r1", "state": {"status": "RUNNING"}, "cluster": {"id": 12345}}],
                clusters={"12345": {"key": "12345", "state": "ACTIVE", "minWorkers": 1, "maxWorkers": 2}})
    ctx = build_context(io, FakeStore(), T0, kv={"managed_clusters": ["12345"]})
    assert ctx.kv["active_runs"] == {"12345": 1}


# ----------------------------------------------------------------------------- approval loop
def test_action_from_json_roundtrip_and_validation():
    a = _quarantined_action(FakeIO())
    b = action_from_json(_action_json(a))
    assert b == a and b.signature == a.signature
    assert action_from_json({"kind": "cluster_stop", "target": "c1"}) == Action("cluster_stop", "c1", {})
    for bad in ("{}", json.dumps({"kind": "x"}), json.dumps({"kind": "x", "target": "t", "params": [1]}), "not json"):
        with pytest.raises(ValueError):
            action_from_json(bad)


def test_tick_drains_approved_row_and_executes_through_gate():
    io = FakeIO()
    a = _quarantined_action(io)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, store, dry_run=False)
    assert report.tier == "low_compute"
    assert report.approvals_executed == 1 and report.approvals_opened == 0
    # executed through _act with the approved params, recorded as an action row
    assert io.mutations() == [("update_cluster_workers", io.env.cluster_id, 1, 3, 15, {"dry_run": False})]
    assert report.outcomes[a.signature] == {"ok": True, "op": "update_cluster_workers"}
    assert store.count(f"INSERT INTO {OPS}.actions") == 1 and "'applied'" in store.sql[store.idx(f"INSERT INTO {OPS}.actions")]
    # the quarantine row is still written, then the allow-by-approval row the action points at
    decision_rows = [s for s in store.sql if s.startswith(f"INSERT INTO {OPS}.policy_decisions")]
    assert len(decision_rows) == 2
    assert "'quarantine'" in decision_rows[0] and "'allow'" in decision_rows[1]
    assert "'APPROVED_BY:reviewer'" in decision_rows[1] and "'approval'" in decision_rows[1] and "'reviewer'" in decision_rows[1]
    assert report.decisions[0].decision == "allow" and report.decisions[0].reason == "APPROVED_BY:reviewer"
    assert report.decisions[0].rule_id == "budget.big_resize"
    # approval marked executed, before the tick row; no new approval for the same signature
    mark = store.idx("SET status = 'executed'")
    assert "approval_id = 'ap-1'" in store.sql[mark] and mark < store.idx(f"INSERT INTO {OPS}.ticks")
    assert store.count(f"INSERT INTO {OPS}.approvals") == 0
    assert any("executed from an approval this tick; not re-planned" in n for n in report.notes)
    assert [x.signature for x in report.actions] == [a.signature]
    # order: expire -> pending read -> approved read -> drain
    assert store.idx("SET status = 'expired'") < store.idx("status = 'pending' ORDER BY") < store.idx("status = 'approved' AND")
    assert "approved_signatures" not in json.dumps(report.to_dict())


def test_tick_drain_in_apply_mode_marks_applied():
    io = FakeIO()
    a = _quarantined_action(io)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 1
    assert io.mutations()[0][-1] == {"dry_run": False}
    assert "'applied'" in store.sql[store.idx(f"INSERT INTO {OPS}.actions")]
    assert "applied by tick" in store.sql[store.idx("SET status = 'executed'")]
    assert ", 1, 0, 0," in store.sql[-1]  # ticks row: actions_applied 1, quarantined 0, denied 0


def test_denied_reevaluation_leaves_approval_approved_with_note():
    io = FakeIO(runs=[{"key": "r1", "state": "RUNNING"}])  # active run -> safety.active_runs denies cluster_update
    a = _quarantined_action(io)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, store)
    assert report.approvals_executed == 0
    assert io.mutations() == []
    assert store.count(f"INSERT INTO {OPS}.actions") == 0
    assert store.count("SET status = 'executed'") == 0 and store.count("SET status = 'failed'") == 0
    note_sql = store.sql[store.idx("DENIED_ON_EXECUTE")]
    assert note_sql.startswith(f"UPDATE {OPS}.approvals SET status = 'approved'") and "safety.active_runs" in note_sql
    assert report.decisions[0].decision == "deny" and report.decisions[0].rule_id == "safety.active_runs"
    assert any("left approved, not executed" in n for n in report.notes)
    # the planner wanted the same resize but it is covered by the open approval: no new approval row
    assert store.count(f"INSERT INTO {OPS}.approvals") == 0
    assert any("awaiting approval" in n for n in report.notes)


def test_planning_skips_signature_with_pending_approval():
    io = FakeIO()
    a = _quarantined_action(io)
    pending = [("ap-0", a.signature, _action_json(a), T0 - timedelta(hours=1), T0 + timedelta(hours=23), "budget.big_resize: BIG_RESIZE")]
    store = ApprovalStore(budget_rows=[ROW_LOW], pending_rows=pending)
    report = run(io, store)
    assert report.tier == "low_compute"
    assert report.actions == [] and report.decisions == []
    assert report.approvals_opened == 0 and store.count(f"INSERT INTO {OPS}.approvals") == 0
    assert store.count(f"INSERT INTO {OPS}.policy_decisions") == 0
    assert any("awaiting approval; not re-planned" in n for n in report.notes)
    assert io.mutations() == []
    # without the pending row the same tick quarantines and opens the approval (control)
    control = ApprovalStore(budget_rows=[ROW_LOW])
    assert run(FakeIO(), control).approvals_opened == 1


def test_pending_signature_does_not_feed_loop_guard_streak():
    io, kv = FakeIO(), MemoryKV()
    a = _quarantined_action(io)
    pending = [("ap-0", a.signature, _action_json(a), T0, T0 + timedelta(hours=24), None)]
    run(io, ApprovalStore(budget_rows=[ROW_LOW], pending_rows=pending), kv)
    assert a.signature not in kv.data.get("sig_streak", {})


def test_expire_runs_before_drain_and_uses_tick_clock():
    store = ApprovalStore()
    run(FakeIO(), store)
    sql = store.sql[store.idx("SET status = 'expired'")]
    assert sql == L.expire_approvals_sql(OPS, T0)
    assert "expires_at <= TIMESTAMP '2026-11-28 03:07:00.000000'" in sql
    assert store.idx("SET status = 'expired'") < store.idx("status = 'approved' AND")
    assert store.idx("status = 'approved' AND") < store.idx(f"INSERT INTO {OPS}.policy_decisions")


def test_drain_marks_failed_on_bad_json_and_signature_mismatch():
    io = FakeIO()
    a = _quarantined_action(io)
    tampered = Action(a.kind, a.target, {**a.params, "max_workers": 24}, a.previous_value, a.reversible)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[
        ("ap-bad", "sig-bad", "{not json", "reviewer", None),
        ("ap-mismatch", a.signature, _action_json(tampered), "reviewer", None),  # action_json edited after quarantine
    ])
    report = run(io, store)
    assert report.approvals_executed == 0 and io.mutations() == []
    failed = [s for s in store.sql if "SET status = 'failed'" in s]
    assert len(failed) == 2
    assert "approval_id = 'ap-bad'" in failed[0] and "BAD_ACTION_JSON" in failed[0]
    assert "approval_id = 'ap-mismatch'" in failed[1] and "SIGNATURE_MISMATCH" in failed[1]
    drained = [s for s in store.sql[:store.idx("SET status = 'failed'") + 2] if s.startswith(f"INSERT INTO {OPS}.policy_decisions")]
    assert drained == []  # neither row reached the gate
    # a row marked failed is not an open approval: the planner proposes the resize itself and the gate quarantines it
    assert report.approvals_opened == 1 and not any("awaiting approval" in n for n in report.notes)


def test_drain_marks_failed_when_act_raises():
    io = FakeIO()
    a = _quarantined_action(io)

    def boom(key, min_w, max_w, auto=None):
        raise RuntimeError("PUT 500")

    io.update_cluster_workers = boom
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 0
    assert "'failed'" in store.sql[store.idx(f"INSERT INTO {OPS}.actions")]
    sql = store.sql[store.idx("SET status = 'failed'")]
    assert "approval_id = 'ap-1'" in sql and "RuntimeError: PUT 500" in sql
    assert report.outcomes[a.signature] == {"error": "RuntimeError: PUT 500"}


def test_drain_planned_only_action_stays_approved():
    io = FakeIO(jobs=[{"key": "j1", "name": "bronze_orders", "schedule": None}])
    a = Action("job_disable", "j1", {"enabled": False, "sla_protected": False, "reason": "tier paused",
                                     "requires_schedule_body": True}, previous_value={"schedule": None})
    store = ApprovalStore(approved_rows=[_approved_row(a, aid="ap-dis")])
    report = run(io, store, dry_run=False)  # tier normal: budget.disable_pipeline quarantines job_disable -> lifted by approval
    assert report.decisions[0].decision == "allow" and report.decisions[0].reason == "APPROVED_BY:reviewer"
    assert report.outcomes[a.signature] == {"skipped": "requires_schedule_body"}
    assert report.approvals_executed == 0 and io.mutations("update_job") == []
    sql = store.sql[store.idx("PLANNED_ONLY")]
    assert sql.startswith(f"UPDATE {OPS}.approvals SET status = 'approved'") and "approval_id = 'ap-dis'" in sql


def test_drain_accepts_dict_rows_and_parsed_action_json(monkeypatch):
    monkeypatch.setenv("NS_APPROVERS", "ops-lead")
    io = FakeIO()
    a = _quarantined_action(io)
    row = {"approval_id": "ap-d", "action_signature": a.signature, "action_json": json.loads(_action_json(a)),
           "approved_by": "ops-lead", "expires_at": None}
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[row])
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 1
    assert "'APPROVED_BY:ops-lead'" in next(s for s in store.sql if "'allow'" in s and s.startswith(f"INSERT INTO {OPS}.policy_decisions"))


def test_approvals_read_failure_is_noted_not_fatal():
    store = ApprovalStore(fail_on="SET status = 'expired'")
    report = run(FakeIO(), store)
    assert any(n.startswith("approvals: expire failed") for n in report.notes)
    assert store.sql[-1].startswith(f"INSERT INTO {OPS}.ticks")

    class Broken(ApprovalStore):
        def run_sql(self, sql, timeout=300):
            if sql.startswith("SELECT") and f"FROM {OPS}.approvals" in sql:
                self.sql.append(sql)
                raise RuntimeError("kernel down")
            return super().run_sql(sql, timeout)

    store = Broken(budget_rows=[ROW_LOW])
    report = run(FakeIO(), store)
    assert any("pending read failed" in n for n in report.notes) and any("approved read failed" in n for n in report.notes)
    assert report.approvals_opened == 1  # degraded: cannot see open rows, so the quarantine opens one


def test_report_json_carries_approvals_executed():
    io = FakeIO()
    a = _quarantined_action(io)
    report = run(io, ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)]), dry_run=False)
    d = json.loads(report.to_json())
    assert d["approvals_executed"] == 1 and d["approvals_opened"] == 0
    assert TickReport("t", "normal", None).approvals_executed == 0


# ----------------------------------------------------------------------------- scripts/approve.py (SQL only, no platform)
def _load_approve_script():
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "scripts" / "approve.py"
    spec = importlib.util.spec_from_file_location("ns_approve_script", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_approve_script_print_only_and_list_formatting(capsys):
    mod = _load_approve_script()
    assert mod.main(["--print-only", "approve", "ap-1", "--by", "reviewer", "--note", "resize ok"]) == 0
    out = capsys.readouterr().out
    assert out.startswith(f"UPDATE {OPS}.approvals SET status = 'approved', approved_by = 'reviewer'")
    assert f"profile={Env().config_profile}; resize ok" in out and "status = 'pending'" in out
    assert mod.main(["--print-only", "reject", "ap-1", "--by", "reviewer"]) == 0
    assert "SET status = 'rejected'" in capsys.readouterr().out
    assert mod.main(["--print-only", "list"]) == 0
    assert capsys.readouterr().out.strip() == L.pending_approvals_sql(OPS)

    a = _quarantined_action(FakeIO())
    rows = [("ap-1", a.signature, _action_json(a), T0, T0 + timedelta(hours=24), "budget.big_resize: BIG_RESIZE: 10 -> 3 (x0.30)")]
    table = mod.format_pending(rows)
    lines = table.splitlines()
    assert lines[0].startswith("id") and "kind" in lines[0] and "expires_at" in lines[0]
    assert lines[1].startswith("ap-1") and "cluster_update" in lines[1] and '"max_workers": 3' in lines[1]
    assert "2026-11-29 03:07:00" in lines[1]
    assert "(no pending approvals)" in mod.format_pending([])


def test_approve_script_runs_against_injected_store(capsys):
    mod = _load_approve_script()

    class Io:
        def __init__(self):
            self.sql = []
            self.closed = False

        def run_sql(self, sql, timeout=300):
            self.sql.append(sql)
            if sql.startswith("SELECT approval_id, status"):
                return [("ap-1", "approved", "reviewer", T0, "budget.big_resize | approved by reviewer: profile=DEFAULT")]
            return []

        def close(self):
            self.closed = True

    io = Io()
    assert mod.main(["approve", "ap-1", "--by", "reviewer"], io=io) == 0
    assert io.sql[0].startswith(f"UPDATE {OPS}.approvals SET status = 'approved'") and io.closed
    out = capsys.readouterr().out
    assert '"status": "approved"' in out and "next actuator tick" in out
    io2 = Io()
    io2.run_sql = lambda sql, timeout=300: [("ap-1", "executed", "reviewer", T0, "")] if sql.startswith("SELECT") else []
    assert mod.main(["reject", "ap-1", "--by", "reviewer"], io=io2) == 1  # not pending any more

def test_actuator_refuses_service_or_unlisted_approver(monkeypatch):
    """Gate parity with the in-cluster drain: a row approved by a service identity, or by a name not on NS_APPROVERS,
    is left approved with DENIED_ON_EXECUTE approver.identity and nothing is executed."""
    io = FakeIO()
    a = _quarantined_action(io)
    for by, approvers in (("dataflow", ""), ("reviewer", "priya,lee")):
        monkeypatch.setenv("NS_APPROVERS", approvers)
        store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a, by=by)])
        report = run(io, store)
        assert report.approvals_executed == 0
        assert any("DENIED_ON_EXECUTE approver.identity" in q for q in store.sql), by
        assert not any("executed" in q and "SET status = 'executed'" in q for q in store.sql), by


# ----------------------------------------------------------------------------- forged or unprovable approvals
def _forged_row(kind: str, target: str, params: dict | None = None, by: str = "mallory", aid: str = "ap-forged"):
    """An approval row written straight into the table (status approved), the way a principal with INSERT could."""
    a = Action(kind, target, params or {})
    return a, (aid, a.signature, _action_json(a), by, None)


@pytest.mark.parametrize("kind,target,params", [
    ("job_run", "victim-job-key", {"body": {}}),
    ("cluster_stop", "cluster-test", {"reason": "forged"}),
    ("job_update_schedule", "victim-job-key", {"cron": "0 * * * *"}),
    ("drop_everything", "cluster-test", {}),
])
def test_forged_approval_of_a_kind_no_quarantine_holds_is_never_executed(kind, target, params, monkeypatch):
    """An approval lifts a quarantine and nothing else. A row carrying a kind no quarantine rule can hold was not
    opened by the gate: it is marked failed and no control-plane call is made, in apply mode, even for a listed approver."""
    monkeypatch.setenv("NS_APPROVERS", "mallory")
    io = FakeIO(jobs=[{"key": "victim-job-key", "name": "victim", "schedule": {"cron": "0 0 * * *"}}])
    _, row = _forged_row(kind, target, params)
    store = ApprovalStore(budget_rows=[ROW_NORMAL], approved_rows=[row], traced=False)
    report = run(io, store, dry_run=False)
    # the tick's own plan may still resize the managed cluster; nothing the forged row asked for is called
    assert report.approvals_executed == 0
    assert [c[0] for c in io.mutations() if c[0] != "update_cluster_workers"] == []
    failed = store.sql[store.idx("SET status = 'failed'")]
    assert "approval.kind: KIND_NOT_APPROVABLE" in failed
    assert store.count("SET status = 'executing'") == 0 and store.count("SET status = 'executed'") == 0
    assert any("KIND_NOT_APPROVABLE" in n for n in report.notes)


def test_forged_approval_of_an_approvable_kind_fails_without_a_quarantine_decision(monkeypatch):
    """A cluster_update row inserted with a spoofed, listed approver name still needs the gate's own quarantine
    decision for that exact action; without one it is marked failed and nothing is called."""
    monkeypatch.setenv("NS_APPROVERS", "reviewer")
    io = FakeIO()
    _, row = _forged_row("cluster_update", io.env.cluster_id, {"min_workers": 1, "max_workers": 24}, by="reviewer")
    store = ApprovalStore(budget_rows=[ROW_NORMAL], approved_rows=[row], traced=False)
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 0
    assert not any(c[0] == "update_cluster_workers" and c[3] == 24 for c in io.mutations())   # the forged 24 workers
    assert "approval.trace: NO_QUARANTINE_DECISION" in store.sql[store.idx("SET status = 'failed'")]
    assert store.count("COUNT(*) AS quarantined") == 1


@pytest.mark.parametrize("kw,reason", [
    ({"bound": False}, "NO_DECISION_ID"),            # written straight into the table: names no gate decision
    ({"same_id": 2}, "DUPLICATE_APPROVAL_ID"),       # a second row with a real row's id
    ({"same_id": 0}, "DUPLICATE_APPROVAL_ID"),       # the id read back matches no row
    ({"holders": 2}, "DECISION_SHARED"),             # a copy of a real row, naming the same quarantine decision
])
def test_an_approval_row_that_is_not_the_one_the_gate_opened_is_never_executed(kw, reason):
    """The approval is bound to the row the gate opened, not to the action: a copy of a real approval (same action,
    same decision, a listed approver's name) is marked failed and nothing is called, and it does not block planning."""
    io = FakeIO()
    a = _quarantined_action(io)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)], **kw)
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 0 and io.mutations() == []
    assert f"approval.trace: {reason}" in store.sql[store.idx("SET status = 'failed'")]
    assert store.count("SET status = 'executing'") == 0
    assert report.approvals_opened == 1   # the failed row is not an open approval; the gate quarantines the plan itself


def test_dry_run_tick_leaves_a_control_plane_approval_for_a_tick_that_applies():
    """A dry-run tick shows what it would execute. It must not consume the person's approval: no allow-by-approval
    decision, no claim, no status change, no action row; the next applying tick executes it."""
    io = FakeIO()
    a = _quarantined_action(io)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, store)                                     # dry_run=True is the default
    assert report.approvals_executed == 0 and io.mutations() == []
    assert any("dry run" in n and "would be executed" in n and "left approved" in n for n in report.notes)
    assert store.count(f"UPDATE {OPS}.approvals SET status = 'executing'") == 0
    assert store.count("SET status = 'executed'") == 0 and store.count(f"INSERT INTO {OPS}.actions") == 0
    assert not any("APPROVED_BY" in s for s in store.sql)
    assert report.approvals_opened == 0 and any("awaiting approval" in n for n in report.notes)   # still open, not re-planned


def test_control_plane_approval_needs_the_approver_allowlist(monkeypatch):
    """Without NS_APPROVERS a control-plane approval is not executed, whoever the row names; it stays approved."""
    monkeypatch.delenv("NS_APPROVERS", raising=False)
    io = FakeIO()
    a = _quarantined_action(io)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 0 and io.mutations() == []
    assert "DENIED_ON_EXECUTE approver.allowlist" in store.sql[store.idx("DENIED_ON_EXECUTE")]
    assert store.count("SET status = 'executing'") == 0


def test_quarantine_trace_read_failure_fails_closed():
    io = FakeIO()
    a = _quarantined_action(io)
    store = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)], fail_on="COUNT(*) AS quarantined")
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 0 and store.count("SET status = 'executing'") == 0
    assert "DENIED_ON_EXECUTE approval.trace" in store.sql[store.idx("DENIED_ON_EXECUTE")]


def test_only_the_claim_holder_executes_an_approval():
    """Two drainers can reach the same approved row. The row is moved to executing before anything is called, and a
    drainer that reads back another drainer's claim does nothing."""
    io = FakeIO()
    a = _quarantined_action(io)
    won = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, won, dry_run=False)
    assert report.approvals_executed == 1 and len(io.mutations("update_cluster_workers")) == 1
    assert won.idx("SET status = 'executing'") < won.idx(f"INSERT INTO {OPS}.actions") < won.idx("SET status = 'executed'")
    assert "claim:" in won.sql[won.idx("SET status = 'executing'")] and "AND status = 'approved'" in won.sql[won.idx("SET status = 'executing'")]

    io2 = FakeIO()
    lost = ApprovalStore(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)], claim_lost=True)
    report = run(io2, lost, dry_run=False)
    assert report.approvals_executed == 0 and io2.mutations() == []
    assert lost.count(f"INSERT INTO {OPS}.actions") == 0 and lost.count("SET status = 'executed'") == 0
    assert any("claimed by another drainer" in n for n in report.notes)


def test_a_claim_that_cannot_be_read_back_is_handed_back_not_left_stuck():
    """The claim UPDATE ran but the read-back fails: the drainer does not act, and it returns the row to approved
    (only if the claim on it is its own) so the next tick can take it."""
    class NoReadBack(ApprovalStore):
        def run_sql(self, sql, timeout=300):
            if sql.startswith(f"SELECT status, note FROM {OPS}.approvals WHERE approval_id"):
                self.sql.append(sql)
                raise RuntimeError("kernel restarted")
            return super().run_sql(sql, timeout)

    io = FakeIO()
    a = _quarantined_action(io)
    store = NoReadBack(budget_rows=[ROW_LOW], approved_rows=[_approved_row(a)])
    report = run(io, store, dry_run=False)
    assert report.approvals_executed == 0 and io.mutations() == []
    assert store.count(f"SELECT status, note FROM {OPS}.approvals") == 2            # read twice before giving up
    release = store.sql[store.idx("claim released")]
    claim = store.claims["ap-1"]
    assert release.startswith(f"UPDATE {OPS}.approvals SET status = 'approved'")
    assert f"status = 'executing' AND note LIKE '%claim:{claim}'" in release
    assert any("claim read-back failed" in n for n in report.notes) and not any("another drainer" in n for n in report.notes)


# ----------------------------------------------------------------------------- busy clusters are never read as idle
@pytest.mark.parametrize("run_row", [
    {"key": "r1", "state": {"status": "RUNNING", "stateMessage": "In run"}},        # the Workbench REST shape
    {"key": "r1", "state": {"status": "PENDING"}},
    {"key": "r1", "state": "RUNNING"},                                              # plain string
    {"key": "r1", "lifecycleState": "IN_PROGRESS"},
    {"key": "r1", "state": {"status": "DISPATCHING"}},                              # a state this code has never seen
    {"key": "r1"},                                                                  # no state at all
])
def test_a_running_job_run_in_any_shape_keeps_the_cluster_busy(run_row):
    """The run state arrives as an object on the REST API. Stringifying it once read a busy cluster as idle; the
    state is now read from the object, and anything not known to be finished counts as active."""
    io = FakeIO(runs=[run_row, {"key": "r0", "state": {"status": "SUCCESS"}}, {"key": "rf", "state": {"status": "FAILED"}}])
    ctx = build_context(io, FakeStore(), T0)
    assert ctx.kv["active_runs"] == {io.env.cluster_id: 1}
    assert [r["key"] for r in ctx.recent_failures] == ["rf"]
    report = run(io, FakeStore(), MemoryKV({"prev_tier": "paused"}), dry_run=False)
    assert "cluster_stop" not in [a.kind for a in report.actions] and io.mutations("stop_cluster") == []
    from nightshift import gate
    ctx.kv["tier"] = "paused"
    d = gate.PolicyEngine(gate.default_rules(), lambda d, a: None).evaluate(Action("cluster_stop", io.env.cluster_id, {}), ctx)
    assert (d.decision, d.rule_id) == ("deny", "safety.active_runs") and d.reason.startswith("ACTIVE_RUNS: 1")


def test_finished_runs_leave_the_cluster_idle():
    io = FakeIO(runs=[{"key": "r0", "state": {"status": "SUCCESS"}}, {"key": "r1", "state": {"status": "SKIPPED"}},
                      {"key": "r2", "state": "TIMED_OUT"}])
    ctx = build_context(io, FakeStore(), T0)
    assert ctx.kv["active_runs"] == {io.env.cluster_id: 0}


def test_failed_run_list_read_means_unknown_and_nothing_is_stopped_or_resized():
    """When the run list cannot be read the tick does not know whether the cluster is busy: no count is recorded, the
    planner proposes no stop, and the gate denies stop and resize alike."""
    class NoRuns(FakeIO):
        def list_recent_runs(self, hours=24):
            raise RuntimeError("jobRuns 503")

    io = NoRuns()
    notes: list[str] = []
    ctx = build_context(io, FakeStore(), T0, notes=notes)
    assert ctx.kv["active_runs"] == {} and any("active runs unknown" in n for n in notes)
    report = run(io, FakeStore(budget_rows=[ROW_LOW]), MemoryKV({"prev_tier": "paused"}), dry_run=False)
    assert "cluster_stop" not in [a.kind for a in report.actions]
    assert io.mutations() == []
    assert report.decisions and all(d.decision == "deny" and d.rule_id == "safety.active_runs"
                                    and d.reason.startswith("ACTIVE_RUNS_UNKNOWN") for d in report.decisions)

