"""Table-driven tests for nightshift.gate (PolicyEngine + default rules). No live calls."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from nightshift.config import Env
from nightshift.gate import (
    RULE_CHECKS,
    PolicyEngine,
    cron_min_interval_minutes,
    default_rules,
    register_check,
    selector_matches,
)
from nightshift.types import Action, Decision, Rule, TickContext

NOW = datetime(2026, 11, 3, 3, 0, tzinfo=timezone.utc)


def ctx(tier: str = "normal", **kv) -> TickContext:
    # the tick records a run count per managed cluster once it has read the run list; c1 is idle unless a case says otherwise
    kv.setdefault("active_runs", {"c1": 0})
    return TickContext(
        now_utc=NOW, budget_remaining_pct=50.0, projected_overrun_pct=0.0, llm_remaining_pct=50.0,
        budget_source="ledger", kv={"tier": tier, **kv},
    )


class Sink:
    """Collects audited (decision, action) pairs; optionally raises."""

    def __init__(self, fail: bool = False) -> None:
        self.rows: list[tuple[Decision, Action]] = []
        self.fail = fail

    def __call__(self, decision: Decision, action: Action) -> None:
        if self.fail:
            raise RuntimeError("ledger unreachable")
        self.rows.append((decision, action))


def engine(counter=None, rules=None, sink=None) -> tuple[PolicyEngine, Sink]:
    sink = sink or Sink()
    return PolicyEngine(rules if rules is not None else default_rules(), sink, counter), sink


def zero_counter(rule_id: str, target: str, window_hours: int) -> int:
    return 0


def resize(target="c1", new=4, old=4, shape=None, min_w=1, **extra) -> Action:
    params = {"min_workers": min_w, "max_workers": new, **extra}
    if shape is not None:
        params["shape"] = shape
    return Action("cluster_update", target, params, previous_value={"max_workers": old, "min_workers": min_w})


def schedule(target="job1", cron="0 */3 * * *", **extra) -> Action:
    return Action("job_update_schedule", target, {"cron": cron, **extra})


# ---------------------------------------------------------------- cron parsing

@pytest.mark.parametrize(
    "cron, interval",
    [
        ("* * * * *", 1),
        ("*/5 * * * *", 5),
        ("*/15 * * * *", 15),
        ("*/30 * * * *", 30),
        ("0,30 * * * *", 30),
        ("0,45 * * * *", 15),  # wraps: 45 -> next hour 00 is 15 min
        ("0 * * * *", 60),
        ("0 */3 * * *", 180),
        ("0 */6 * * *", 360),
        ("0 2 * * *", 1440),
        ("0 3 * * 0", 1440),
        ("30 1,13 * * *", 720),
        ("0 9-17 * * 1-5", 60),
        ("15 */2 * * *", 120),
        ("0 0-23/4 * * *", 240),
    ],
)
def test_cron_min_interval(cron, interval):
    assert cron_min_interval_minutes(cron) == interval


@pytest.mark.parametrize("cron", ["0 * * *", "60 * * * *", "0 24 * * *", "a b c d e", "*/0 * * * *", ""])
def test_cron_invalid_raises(cron):
    with pytest.raises(ValueError):
        cron_min_interval_minutes(cron)


# ---------------------------------------------------------------- selector

@pytest.mark.parametrize(
    "selector, action, expected",
    [
        ({}, Action("noop", "x"), True),
        ({"kind": "cluster_update"}, resize(), True),
        ({"kind": "cluster_stop"}, resize(), False),
        ({"kind": ["cluster_stop", "cluster_update"]}, resize(), True),
        ({"target": "c1"}, resize(target="c1"), True),
        ({"shape": "VM.Standard.E4.Flex"}, resize(shape="VM.Standard.E4.Flex"), True),  # params key
        ({"shape": "VM.Standard.E4.Flex"}, resize(), False),  # missing param
        ({"reversible": True}, resize(), True),
        ({"kind": "cluster_update", "target": "other"}, resize(target="c1"), False),
    ],
)
def test_selector_matches(selector, action, expected):
    assert selector_matches(selector, action) is expected


# ---------------------------------------------------------------- default rules: table

def test_default_rules_cover_report_table():
    rules = default_rules()
    ids = [r.rule_id for r in rules]
    assert ids == [
        "validate.cron", "validate.cluster_bounds", "protect.sla", "protect.self", "safety.active_runs",
        "verify.replay_required", "tier.repair_quarantine",
        "budget.big_resize", "budget.disable_pipeline", "rate.resizes_daily", "rate.cron_edits_hourly",
    ]
    assert all(r.rule_id in RULE_CHECKS for r in rules)
    assert all(r.enabled for r in rules)
    prio = {r.rule_id: r.priority for r in rules}
    assert prio["validate.cron"] == 100 < prio["protect.sla"] == 200 < prio["safety.active_runs"] == 300
    assert prio["budget.big_resize"] == 500 < prio["rate.resizes_daily"] == 600


@pytest.mark.parametrize(
    "name, action, context, expected, rule_id",
    [
        # validate.cron
        ("cron ok 3h", schedule(cron="0 */3 * * *"), ctx(), "allow", None),
        ("cron ok 30m", schedule(cron="*/30 * * * *"), ctx(), "allow", None),
        ("cron too frequent", schedule(cron="*/15 * * * *"), ctx(), "deny", "validate.cron"),
        ("cron every minute", schedule(cron="* * * * *"), ctx(), "deny", "validate.cron"),
        ("cron malformed", schedule(cron="0 * *"), ctx(), "deny", "validate.cron"),
        ("cron missing", Action("job_update_schedule", "job1", {}), ctx(), "deny", "validate.cron"),
        # validate.cluster_bounds
        ("bounds ok", resize(new=6, old=4, min_w=2), ctx(), "allow", None),
        ("bounds min 0", resize(new=4, old=4, min_w=0), ctx(), "deny", "validate.cluster_bounds"),
        ("bounds min > max", resize(new=2, old=2, min_w=5), ctx(), "deny", "validate.cluster_bounds"),
        ("bounds over cap", resize(new=25, old=24), ctx(), "deny", "validate.cluster_bounds"),
        ("bounds at cap", resize(new=24, old=24), ctx(), "allow", None),
        ("shape allowed", resize(shape="VM.Standard.E4.Flex"), ctx(), "allow", None),
        ("shape gpu", resize(shape="VM.GPU.A10.1"), ctx(), "deny", "validate.cluster_bounds"),
        ("shape unknown", resize(shape="BM.Weird.1"), ctx(), "deny", "validate.cluster_bounds"),
        # protect.sla
        ("sla cadence change", schedule(sla_protected=True), ctx("critical"), "deny", "protect.sla"),
        ("sla not protected", schedule(sla_protected=False), ctx("critical"), "allow", None),
        ("sla paused keeps enabled", schedule(sla_protected=True, enabled=True), ctx("paused"), "allow", None),
        ("sla paused disables", schedule(sla_protected=True, enabled=False), ctx("paused"), "deny", "protect.sla"),
        ("sla disable at paused",
         Action("job_disable", "gold_kpis", {"sla_protected": True}), ctx("paused"), "deny", "protect.sla"),
        # protect.self
        ("self steward job", Action("job_disable", "steward_tick", {}), ctx("paused"), "deny", "protect.self"),
        ("self steward cluster", Action("cluster_stop", "steward-cluster", {}), ctx(), "deny", "protect.self"),
        ("self ops table",
         Action("statement_repair", "job9", {"table": f"{Env().ops_schema}.ticks"}), ctx(), "deny", "protect.self"),
        ("self ops table qualified",
         Action("statement_repair", "job9", {"table": f"{Env().ops}.policy_rules"}), ctx(), "deny", "protect.self"),
        ("self other table", Action("notify", "job9", {"table": "bronze.orders"}), ctx(), "allow", None),
        ("self noop", Action("noop", "anything"), ctx(), "allow", None),
        # safety.active_runs
        ("active runs stop", Action("cluster_stop", "c1"), ctx(active_runs={"c1": 2}), "deny", "safety.active_runs"),
        ("active runs update", resize(target="c1"), ctx(active_runs={"c1": 1}), "deny", "safety.active_runs"),
        ("active runs zero", Action("cluster_stop", "c1"), ctx(active_runs={"c1": 0}), "allow", None),
        # unknown is never idle: no entry for the target, no count at all, or a count that is not a number
        ("active runs other cluster", Action("cluster_stop", "c1"), ctx(active_runs={"c2": 3}), "deny", "safety.active_runs"),
        ("active runs no kv", Action("cluster_stop", "c1"), ctx(active_runs=None), "deny", "safety.active_runs"),
        ("active runs none", Action("cluster_stop", "c1"), ctx(active_runs={"c1": None}), "deny", "safety.active_runs"),
        ("active runs unknown update", resize(target="c1"), ctx(active_runs={}), "deny", "safety.active_runs"),
        # budget.big_resize
        ("resize 2x exactly", resize(new=8, old=4), ctx(), "allow", None),
        ("resize over 2x", resize(new=9, old=4), ctx(), "quarantine", "budget.big_resize"),
        ("resize half exactly", resize(new=4, old=8), ctx(), "allow", None),
        ("resize under half", resize(new=3, old=8), ctx(), "quarantine", "budget.big_resize"),
        ("resize explicit old/new params",
         Action("cluster_update", "c1", {"max_workers_old": 2, "max_workers_new": 10}), ctx(),
         "quarantine", "budget.big_resize"),
        ("resize unknown old", Action("cluster_update", "c1", {"max_workers": 10}), ctx(), "allow", None),
        # budget.disable_pipeline
        ("disable at normal", Action("job_disable", "bronze_x", {}), ctx("normal"), "quarantine",
         "budget.disable_pipeline"),
        ("disable at critical", Action("job_disable", "bronze_x", {}), ctx("critical"), "quarantine",
         "budget.disable_pipeline"),
        ("disable at paused", Action("job_disable", "bronze_x", {}), ctx("paused"), "allow", None),
        # a known kind no rule objects to is allowed; a kind the engine does not know is denied before any rule runs
        ("notify default allow", Action("notify", "oncall", {"text": "hi"}), ctx(), "allow", None),
        ("job_run unknown kind", Action("job_run", "silver_y", {}), ctx(), "deny", "gate.known_kinds"),
        ("job_repair unknown kind", Action("job_repair", "job9", {"table": "bronze.orders"}), ctx(), "deny", "gate.known_kinds"),
        ("made-up unknown kind", Action("drop_everything", "c1", {}), ctx(), "deny", "gate.known_kinds"),
    ],
)
def test_default_rules_paths(name, action, context, expected, rule_id):
    eng, sink = engine(counter=zero_counter)
    d = eng.evaluate(action, context)
    assert (d.decision, d.rule_id) == (expected, rule_id), name
    assert d.action_signature == action.signature
    assert len(sink.rows) == 1 and sink.rows[0][0] is d


# ---------------------------------------------------------------- rate limits

@pytest.mark.parametrize(
    "prior, expected, rule_id",
    [(0, "allow", None), (2, "allow", None), (3, "deny", "rate.resizes_daily"), (7, "deny", "rate.resizes_daily")],
)
def test_rate_resizes_daily_threshold(prior, expected, rule_id):
    calls = []

    def counter(rid, target, window):
        calls.append((rid, target, window))
        return prior

    eng, _ = engine(counter=counter)
    d = eng.evaluate(resize(new=5, old=4), ctx())
    assert (d.decision, d.rule_id) == (expected, rule_id)
    assert ("rate.resizes_daily", "c1", 24) in calls


@pytest.mark.parametrize(
    "prior, expected, rule_id",
    [(9, "allow", None), (10, "deny", "rate.cron_edits_hourly"), (11, "deny", "rate.cron_edits_hourly")],
)
def test_rate_cron_edits_hourly_threshold(prior, expected, rule_id):
    calls = []

    def counter(rid, target, window):
        calls.append((rid, target, window))
        return prior

    eng, _ = engine(counter=counter)
    d = eng.evaluate(schedule(), ctx())
    assert (d.decision, d.rule_id) == (expected, rule_id)
    assert ("rate.cron_edits_hourly", "job1", 1) in calls


@pytest.mark.parametrize("action", [resize(new=5, old=4), schedule()])
def test_rate_rules_fail_closed_without_counter(action):
    eng, sink = engine(counter=None)
    d = eng.evaluate(action, ctx())
    assert d.decision == "deny" and d.reason == "DB_UNAVAILABLE"
    assert d.rule_id.startswith("rate.")
    assert len(sink.rows) == 1


def test_no_counter_does_not_affect_non_rate_actions():
    eng, _ = engine(counter=None)
    assert eng.evaluate(Action("notify", "oncall"), ctx()).decision == "allow"


# ---------------------------------------------------------------- ordering, stickiness, audit

def test_first_deny_wins_stops_evaluation():
    eng, _ = engine(counter=zero_counter)
    # cron invalid (100) and sla protected (200): the lower priority rule wins and later ones are not evaluated
    d = eng.evaluate(schedule(cron="*/5 * * * *", sla_protected=True), ctx("critical"))
    assert (d.decision, d.rule_id) == ("deny", "validate.cron")
    assert d.evaluated_rules == ["validate.cron"]


def test_priority_sort_not_list_order():
    sink = Sink()
    rules = [
        Rule("late", 900, {}, "deny"),
        Rule("early", 10, {}, "deny"),
    ]
    d = PolicyEngine(rules, sink).evaluate(Action("noop", "x"), ctx())
    assert d.rule_id == "early" and d.evaluated_rules == ["early"]


def test_disabled_rules_are_skipped():
    rules = [Rule("blocker", 1, {}, "deny", enabled=False)]
    eng, _ = engine(rules=rules)
    d = eng.evaluate(Action("noop", "x"), ctx())
    assert d.decision == "allow" and d.evaluated_rules == []


def test_quarantine_is_sticky_through_allows():
    eng, _ = engine(counter=zero_counter)
    d = eng.evaluate(resize(new=9, old=4), ctx())
    assert (d.decision, d.rule_id) == ("quarantine", "budget.big_resize")
    # evaluation continued past the quarantine to the rate rule
    assert d.evaluated_rules == ["validate.cluster_bounds", "protect.self", "safety.active_runs",
                                 "budget.big_resize", "rate.resizes_daily"]


def test_quarantine_then_later_deny_becomes_deny():
    eng, _ = engine(counter=lambda rid, t, w: 99)
    d = eng.evaluate(resize(new=9, old=4), ctx())
    assert (d.decision, d.rule_id) == ("deny", "rate.resizes_daily")
    assert "budget.big_resize" in d.evaluated_rules


def test_first_quarantine_reason_kept():
    rules = [
        Rule("q1", 10, {}, "quarantine"),
        Rule("q2", 20, {}, "quarantine"),
    ]
    eng, _ = engine(rules=rules)
    d = eng.evaluate(Action("noop", "x"), ctx())
    assert (d.decision, d.rule_id) == ("quarantine", "q1")
    assert d.evaluated_rules == ["q1", "q2"]


def test_selector_only_rule_triggers_on_match():
    rules = [Rule("freeze.notify", 50, {"kind": "notify"}, "deny")]
    eng, _ = engine(rules=rules)
    assert eng.evaluate(Action("notify", "x"), ctx()).decision == "deny"
    assert eng.evaluate(Action("noop", "x"), ctx()).decision == "allow"


def test_allow_effect_rule_is_informational():
    rules = [Rule("mark", 1, {}, "allow"), Rule("block", 2, {}, "deny")]
    eng, _ = engine(rules=rules)
    d = eng.evaluate(Action("noop", "x"), ctx())
    assert d.decision == "deny" and d.evaluated_rules == ["mark", "block"]


def test_audit_failure_fails_closed():
    eng, sink = engine(counter=zero_counter, sink=Sink(fail=True))
    d = eng.evaluate(Action("notify", "oncall"), ctx())
    assert d.decision == "deny" and d.reason == "AUDIT_WRITE_FAILED"
    assert sink.rows == []
    assert d.action_signature == Action("notify", "oncall").signature


def test_audit_written_before_return_with_metadata():
    eng, sink = engine(counter=zero_counter)
    d = eng.evaluate(resize(new=5, old=4), ctx())
    assert sink.rows[0][0] is d and sink.rows[0][1].target == "c1"
    assert d.latency_ms >= 0.0
    assert d.evaluated_rules[0] == "validate.cluster_bounds"


def test_check_exception_fails_closed():
    @register_check("test.boom")
    def boom(rule, action, c, counter):
        raise KeyError("missing column")

    try:
        rules = [Rule("test.boom", 1, {}, "quarantine")]
        eng, _ = engine(rules=rules)
        d = eng.evaluate(Action("noop", "x"), ctx())
        assert d.decision == "deny" and d.rule_id == "test.boom" and d.reason.startswith("CHECK_ERROR")
    finally:
        RULE_CHECKS.pop("test.boom", None)


def test_rule_params_override_defaults():
    rules = [Rule("validate.cron", 100, {"kind": "job_update_schedule"}, "deny", {"min_minutes": 120})]
    eng, _ = engine(rules=rules)
    assert eng.evaluate(schedule(cron="0 * * * *"), ctx()).decision == "deny"
    assert eng.evaluate(schedule(cron="0 */2 * * *"), ctx()).decision == "allow"


def test_cluster_bounds_fills_min_from_previous_value():
    a = Action("cluster_update", "c1", {"max_workers": 2}, previous_value={"min_workers": 4, "max_workers": 6})
    eng, _ = engine(counter=zero_counter)
    d = eng.evaluate(a, ctx())
    assert (d.decision, d.rule_id) == ("deny", "validate.cluster_bounds")


def test_unknown_kind_is_denied_before_any_rule_and_still_audited():
    eng, sink = engine(counter=zero_counter)
    d = eng.evaluate(Action("job_run", "victim-job-key", {}), ctx())
    assert (d.decision, d.rule_id, d.reason) == ("deny", "gate.known_kinds", "UNKNOWN_KIND: job_run")
    assert d.evaluated_rules == [] and len(sink.rows) == 1 and sink.rows[0][0] is d


def test_approvable_kinds_are_the_kinds_a_quarantine_rule_holds():
    eng, _ = engine(counter=zero_counter)
    assert eng.approvable_kinds() == {"statement_repair", "cluster_update", "job_disable"}
    assert PolicyEngine([Rule("deny.all", 1, {}, "deny")], lambda d, a: None).approvable_kinds() == frozenset()


def test_rate_limit_counts_applied_changes_from_the_ledger(ops_db):
    """End to end against the ledger SQL. The quota is spent by applied actions whose allow decision ran the rate
    rule (an allow records rule_id NULL, so the rule is found in rules_evaluated): three applied small resizes of one
    cluster are allowed, the fourth is denied, a denied attempt and a dry-run allow do not consume quota, and another
    cluster has its own count."""
    from nightshift import ledger as L

    from helpers import _to_duckdb

    ops, con = ops_db
    seq = iter(range(100))
    mode = {"status": "applied"}

    def sink(d: Decision, a: Action) -> None:
        tick = f"t{next(seq)}"
        con.execute(L.insert_decision(ops, {
            "tick_id": tick, "action_signature": d.action_signature, "action_kind": a.kind, "target": a.target,
            "decision": d.decision, "rule_id": d.rule_id, "rules_evaluated": d.evaluated_rules, "reason": d.reason}, NOW))
        if d.decision == "allow":   # what the tick does next: act, then the action row pointing at the decision
            con.execute(L.insert_action(ops, {
                "tick_id": tick, "action_signature": d.action_signature, "kind": a.kind, "target": a.target, "params": a.params,
                "decision_id": L._sha16(tick, d.action_signature, d.decision, d.rule_id), "status": mode["status"],
                "applied_at": NOW if mode["status"] == "applied" else None}, NOW))

    def counter(rule_id: str, target: str, window_hours: int) -> int:
        return int(con.execute(_to_duckdb(L.rate_count_sql(ops, rule_id, target, window_hours, NOW))).fetchone()[0])

    eng = PolicyEngine(default_rules(), sink, counter)
    mode["status"] = "dry_run"
    previews = [eng.evaluate(resize(new=n, old=4), ctx()) for n in (5, 6, 7, 8, 5, 6)]
    assert [d.decision for d in previews] == ["allow"] * 6 and counter("rate.resizes_daily", "c1", 24) == 0   # previews are free
    mode["status"] = "applied"
    seen = [eng.evaluate(resize(new=n, old=4), ctx()) for n in (5, 6, 7, 8, 5)]
    assert [(d.decision, d.rule_id) for d in seen[:3]] == [("allow", None)] * 3
    assert all("rate.resizes_daily" in d.evaluated_rules for d in seen[:3])
    assert [(d.decision, d.rule_id) for d in seen[3:]] == [("deny", "rate.resizes_daily")] * 2
    assert seen[3].reason.startswith("RATE_LIMIT: 3 prior in 24h")
    assert counter("rate.resizes_daily", "c1", 24) == 3 and counter("rate.resizes_daily", "c2", 24) == 0
    other = eng.evaluate(resize(target="c2", new=5, old=4), ctx(active_runs={"c2": 0}))
    assert other.decision == "allow"


def test_a_quarantine_rule_without_a_kind_selector_makes_every_known_kind_approvable():
    eng = PolicyEngine([Rule("hold.everything", 1, {}, "quarantine")], lambda d, a: None)
    assert eng.approvable_kinds() == eng.known_kinds


def test_the_self_protection_rule_follows_ns_ops_and_ignores_case(monkeypatch):
    """One ledger variable: the rule guards the schema NS_OPS names, the same one the job and the actuator write, and
    a table name is matched the way Spark resolves it (without case)."""
    from nightshift import gate
    monkeypatch.setenv("NS_OPS", "lake.night_ops")
    assert (Env().ops, Env().catalog, Env().ops_schema) == ("lake.night_ops", "lake", "night_ops")
    eng = gate.PolicyEngine(gate.default_rules(), lambda d, a: None, decision_counter=lambda *a: 0)
    for table in ("lake.night_ops.approvals", "LAKE.Night_Ops.policy_decisions", "night_ops.ticks"):
        d = eng.evaluate(Action("statement_repair", "job9", {"table": table}), ctx())
        assert (d.decision, d.rule_id) == ("deny", "protect.self"), table
    # the default ledger is not this instance's ledger any more, so the rule no longer names it
    d = eng.evaluate(Action("statement_repair", "job9", {"table": "default.nightshift_ops.ticks"}), ctx())
    assert d.rule_id != "protect.self"
    monkeypatch.delenv("NS_OPS")
    assert Env().ops == "default.nightshift_ops"
    for bad in ("a.b.c", "lake.night ops", "x; DROP", ""):
        with pytest.raises(ValueError, match="NS_OPS"):
            Env(ops=bad)
