"""Policy gate: ``Rule`` rows in priority order, first deny wins, sticky quarantine, fail-closed audit.

The rule table is ``default_rules()`` in this module (code, reviewed like code). Both runners build their engine
from it; the ``policy_rules`` ledger table is reserved for a later version and nothing reads it.

Fail closed throughout: an action kind the engine does not know is denied (``UNKNOWN_KIND``), a failed decision
write denies, and a rate rule with no counter denies with ``DB_UNAVAILABLE``.
"""
from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import Any

from nightshift.config import JOB_CRON_MIN_MINUTES, Env, _env
from nightshift.types import Action, Decision, Rule, TickContext

Counter = Callable[[str, str, int], int]
"""``counter(rule_id, target, window_hours)`` -> prior ALLOW decisions in the window whose evaluation ran that rule
for that target (an allow records ``rule_id`` NULL or the lifted quarantine rule, so the rate rule is found in
``rules_evaluated``)."""
CheckFn = Callable[[Rule, Action, TickContext, Counter | None], tuple[bool, str]]
"""``check(rule, action, ctx, counter)`` -> ``(triggered, reason)``."""

RULE_CHECKS: dict[str, CheckFn] = {}

# Worker shapes a cluster_update may set: NS_SHAPE_ALLOWLIST, comma-separated, as the Workbench reports workerShape
# ("amd.generic" is the generic worker shape; the VM.Standard.* names are kept so an explicit shape can be allowed).
DEFAULT_SHAPE_ALLOWLIST = tuple(
    s.strip()
    for s in _env("NS_SHAPE_ALLOWLIST", "amd.generic,VM.Standard.E4.Flex,VM.Standard.E5.Flex,VM.Standard3.Flex").split(",")
    if s.strip()
)
DEFAULT_WORKER_CAP = 24
_MISSING = object()

# The action kinds the engine will evaluate at all. Anything else is denied before a rule runs: a kind nobody wrote
# a rule for must never fall through to the default allow (a forged or mistyped action would otherwise execute).
KNOWN_KINDS: frozenset[str] = frozenset(
    {"cluster_stop", "cluster_update", "job_update_schedule", "job_disable", "statement_repair", "notify", "noop"}
)
UNKNOWN_KIND_RULE = "gate.known_kinds"


def register_check(rule_id: str) -> Callable[[CheckFn], CheckFn]:
    """Decorator: register ``fn`` as the check for ``rule_id``."""

    def deco(fn: CheckFn) -> CheckFn:
        RULE_CHECKS[rule_id] = fn
        return fn

    return deco


def selector_matches(selector: dict[str, Any], action: Action) -> bool:
    """Every selector key must equal the Action attribute or ``action.params`` key; list values mean "in"."""
    for key, wanted in selector.items():
        actual = getattr(action, key, _MISSING)
        if actual is _MISSING:
            actual = action.params.get(key, _MISSING)
        if actual is _MISSING:
            return False
        if isinstance(wanted, (list, tuple, set, frozenset)):
            if actual not in wanted:
                return False
        elif actual != wanted:
            return False
    return True


class PolicyEngine:
    """Evaluate an Action against rule rows and audit the decision before returning it."""

    def __init__(
        self,
        rules: list[Rule],
        audit_sink: Callable[[Decision, Action], None],
        decision_counter: Counter | None = None,
        known_kinds: Iterable[str] = KNOWN_KINDS,
    ) -> None:
        self.rules = list(rules)
        self.audit_sink = audit_sink
        self.decision_counter = decision_counter
        self.known_kinds = frozenset(known_kinds)

    def approvable_kinds(self) -> frozenset[str]:
        """Kinds a quarantine rule can hold for a person. An approval lifts a quarantine and nothing else, so an
        approval row carrying any other kind was not opened by this gate and is never executed."""
        kinds: set[str] = set()
        for rule in self.rules:
            if not rule.enabled or rule.effect != "quarantine":
                continue
            wanted = rule.selector.get("kind")
            if wanted is None:
                kinds.update(self.known_kinds)   # a quarantine rule with no kind selector can hold any known kind
            elif isinstance(wanted, str):
                kinds.add(wanted)
            elif isinstance(wanted, (list, tuple, set, frozenset)):
                kinds.update(str(k) for k in wanted)
        return frozenset(kinds)

    def evaluate(self, action: Action, ctx: TickContext) -> Decision:
        """Priority-ordered evaluation: unknown kind denies, first deny wins, quarantine sticks, a known kind no
        rule objects to is allowed; audit before return."""
        t0 = time.perf_counter()
        evaluated: list[str] = []
        quarantine: tuple[str, str] | None = None
        decision: Decision | None = None

        if action.kind not in self.known_kinds:
            decision = Decision(action.signature, "deny", UNKNOWN_KIND_RULE, f"UNKNOWN_KIND: {action.kind}")

        for rule in sorted((r for r in self.rules if r.enabled and decision is None), key=lambda r: r.priority):
            if not selector_matches(rule.selector, action):
                continue
            evaluated.append(rule.rule_id)
            check = RULE_CHECKS.get(rule.rule_id)
            if check is None:
                triggered, reason = True, f"{rule.rule_id}: selector matched"
            else:
                try:
                    triggered, reason = check(rule, action, ctx, self.decision_counter)
                except Exception as exc:  # fail closed on a broken check
                    decision = Decision(action.signature, "deny", rule.rule_id, f"CHECK_ERROR: {exc!r}")
                    break
            if not triggered:
                continue
            if rule.effect == "deny":
                decision = Decision(action.signature, "deny", rule.rule_id, reason)
                break
            if rule.effect == "quarantine" and quarantine is None:
                quarantine = (rule.rule_id, reason)
            # effect == "allow": informational marker only; evaluation continues.

        if decision is None:
            if quarantine is not None:
                decision = Decision(action.signature, "quarantine", quarantine[0], quarantine[1])
            else:
                decision = Decision(action.signature, "allow", None, "DEFAULT_ALLOW")
        decision.evaluated_rules = evaluated
        decision.latency_ms = (time.perf_counter() - t0) * 1000.0

        try:
            self.audit_sink(decision, action)
        except Exception:  # fail closed: an unrecorded decision is not an allow
            return Decision(
                action.signature, "deny", decision.rule_id, "AUDIT_WRITE_FAILED",
                evaluated_rules=evaluated, latency_ms=(time.perf_counter() - t0) * 1000.0,
            )
        return decision


# ---------------------------------------------------------------- helpers

def _tier(ctx: TickContext) -> str | None:
    return ctx.kv.get("tier")


def _expand_cron_field(field: str, lo: int, hi: int) -> set[int]:
    """Expand one cron field (``*``, ``*/N``, ``a-b``, ``a-b/N``, ``a,b,c``) into a set of values."""
    values: set[int] = set()
    for part in field.split(","):
        step = 1
        has_step = "/" in part
        if has_step:
            part, step_s = part.split("/", 1)
            step = int(step_s)
            if step < 1:
                raise ValueError("step must be >= 1")
        if part == "*":
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(part)
            if has_step:
                end = hi  # "5/15" style: from 5 every 15
        if not (lo <= start <= end <= hi):
            raise ValueError(f"field {field!r} out of range {lo}-{hi}")
        values.update(range(start, end + 1, step))
    if not values:
        raise ValueError(f"empty field {field!r}")
    return values


def cron_min_interval_minutes(cron: str) -> int:
    """Minimum gap in minutes between two firings of a 5-field cron within one day (day fields ignored).

    A schedule that fires at most once per day returns 1440. Raises ValueError on malformed input.
    """
    fields = cron.split()
    if len(fields) != 5:
        raise ValueError("cron must have 5 fields")
    minutes = _expand_cron_field(fields[0], 0, 59)
    hours = _expand_cron_field(fields[1], 0, 23)
    _expand_cron_field(fields[2], 1, 31)
    _expand_cron_field(fields[3], 1, 12)
    _expand_cron_field(fields[4], 0, 7)
    times = sorted(h * 60 + m for h in hours for m in minutes)
    if len(times) == 1:
        return 1440
    gaps = [b - a for a, b in zip(times, times[1:])]
    gaps.append(times[0] + 1440 - times[-1])
    return min(gaps)


def _num(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- checks

@register_check("validate.cron")
def check_cron(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny a schedule whose minimum interval is under the platform minimum, or that does not parse."""
    min_minutes = int(rule.params.get("min_minutes", JOB_CRON_MIN_MINUTES))
    cron = action.params.get("cron")
    if not isinstance(cron, str) or not cron.strip():
        return True, "CRON_MISSING"
    try:
        interval = cron_min_interval_minutes(cron)
    except (ValueError, TypeError) as exc:
        return True, f"CRON_INVALID: {exc}"
    if interval < min_minutes:
        return True, f"CRON_TOO_FREQUENT: {interval} min < {min_minutes} min minimum"
    return False, "ok"


@register_check("validate.cluster_bounds")
def check_cluster_bounds(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny unless 1 <= min <= max <= cap, shape allowlisted and not a GPU shape."""
    cap = int(rule.params.get("worker_cap", DEFAULT_WORKER_CAP))
    allow = tuple(rule.params.get("shapes", DEFAULT_SHAPE_ALLOWLIST))
    prev = action.previous_value or {}
    p = action.params
    if "min_workers" in p or "max_workers" in p:
        min_w = _num(p.get("min_workers", prev.get("min_workers")))
        max_w = _num(p.get("max_workers", prev.get("max_workers")))
        if min_w is None:
            min_w = 1.0
        if max_w is None:
            max_w = min_w
        if not (1 <= min_w <= max_w <= cap):
            return True, f"CLUSTER_BOUNDS: need 1 <= min({min_w:g}) <= max({max_w:g}) <= {cap}"
    shape = p.get("shape")
    if shape is not None:
        if "gpu" in str(shape).lower():
            return True, f"GPU_SHAPE: {shape}"
        if shape not in allow:
            return True, f"SHAPE_NOT_ALLOWED: {shape}"
    return False, "ok"


@register_check("protect.sla")
def check_sla(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny cadence/disable changes on sla_protected pipelines unless tier is paused and the action keeps them enabled."""
    if not action.params.get("sla_protected"):
        return False, "ok"
    keeps_enabled = bool(action.params.get("enabled", action.kind != "job_disable"))
    if _tier(ctx) == "paused" and keeps_enabled:
        return False, "ok"
    return True, "SLA_PROTECTED"


@register_check("protect.self")
def check_self(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny anything aimed at the steward itself or the ops schema tables."""
    prefixes = tuple(rule.params.get("target_prefixes", ("steward",)))
    if action.target.startswith(prefixes):
        return True, f"SELF_PROTECT: target {action.target}"
    env = Env()
    table_prefixes = tuple(rule.params.get("table_prefixes", (env.ops_schema, env.ops)))
    table = action.params.get("table")
    if isinstance(table, str) and table.startswith(table_prefixes):
        return True, f"SELF_PROTECT: table {table}"
    return False, "ok"


@register_check("safety.active_runs")
def check_active_runs(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny cluster_stop/cluster_update while runs are active on that cluster, and when the count is unknown: the
    tick writes an entry per managed cluster only after it read the run list, so a missing entry (the read failed, or
    the target is not a managed cluster) fails closed."""
    counts = ctx.kv.get("active_runs")
    active = counts.get(action.target) if isinstance(counts, dict) else None
    if isinstance(active, bool) or not isinstance(active, int):
        return True, f"ACTIVE_RUNS_UNKNOWN: no run count for {action.target}"
    if active > 0:
        return True, f"ACTIVE_RUNS: {active} on {action.target}"
    return False, "ok"


@register_check("budget.big_resize")
def check_big_resize(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Quarantine a max_workers change by more than 2x up or 0.5x down."""
    hi = float(rule.params.get("max_ratio", 2.0))
    lo = float(rule.params.get("min_ratio", 0.5))
    new = _num(action.params.get("max_workers_new", action.params.get("max_workers")))
    old = _num(action.params.get("max_workers_old", (action.previous_value or {}).get("max_workers")))
    if new is None or old is None or old <= 0:
        return False, "ok"
    ratio = new / old
    if ratio > hi or ratio < lo:
        return True, f"BIG_RESIZE: {old:g} -> {new:g} (x{ratio:.2f})"
    return False, "ok"


@register_check("budget.disable_pipeline")
def check_disable_pipeline(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Quarantine any job_disable unless the tier is paused."""
    if _tier(ctx) == "paused":
        return False, "ok"
    return True, f"DISABLE_AT_TIER: {_tier(ctx)}"


def _rate_check(rule: Rule, action: Action, counter: Counter | None) -> tuple[bool, str]:
    if counter is None:
        return True, "DB_UNAVAILABLE"
    window = int(rule.params.get("window_hours", 24))
    max_allowed = int(rule.params.get("max_allowed", 3))
    prior = int(counter(rule.rule_id, action.target, window))
    if prior >= max_allowed:
        return True, f"RATE_LIMIT: {prior} prior in {window}h >= {max_allowed}"
    return False, "ok"


@register_check("rate.resizes_daily")
def check_resizes_daily(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny the (max_allowed+1)th cluster_update in the window; fail closed without a counter."""
    return _rate_check(rule, action, counter)


@register_check("rate.cron_edits_hourly")
def check_cron_edits_hourly(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny the (max_allowed+1)th job_update_schedule in the window; fail closed without a counter."""
    return _rate_check(rule, action, counter)


@register_check("verify.replay_required")
def check_replay_required(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Deny a statement_repair unless the tick loaded a fresh replay_safe verdict for the DRAFT it proposes.
    The tick puts the latest verdicts in ``ctx.kv['replay_verdicts']`` as {statement_sha256: {verdict, age_hours}};
    the draft's own hash is what must be proven, not the original statement's. Fails closed when absent or stale."""
    verdicts = ctx.kv.get("replay_verdicts") or {}
    draft_sha = str(action.params.get("draft_sha256") or "")
    max_age = float(rule.params.get("max_age_hours", 24))
    v = verdicts.get(draft_sha)
    if not draft_sha or not v:
        return True, "NO_REPLAY_VERDICT: the proposed statement has not been replay-graded"
    if str(v.get("verdict")) != "replay_safe":
        return True, f"REPLAY_VERDICT_{str(v.get('verdict')).upper()}: not replay_safe"
    age = float(v.get("age_hours", 1e9))
    if age > max_age:
        return True, f"REPLAY_VERDICT_STALE: {age:.1f}h old > {max_age:g}h"
    return False, "ok"


@register_check("tier.repair_quarantine")
def check_repair_quarantine(rule: Rule, action: Action, ctx: TickContext, counter: Counter | None) -> tuple[bool, str]:
    """Every statement_repair is a Tier 2 change: it always waits for a person (earned autonomy comes later)."""
    return True, f"REPAIR_TIER2: {action.params.get('table', action.target)} draft {str(action.params.get('draft_id', ''))[:12]}"


def default_rules() -> list[Rule]:
    """The default policy table as Rule rows."""
    return [
        Rule("validate.cron", 100, {"kind": "job_update_schedule"}, "deny",
             {"min_minutes": JOB_CRON_MIN_MINUTES}),
        Rule("validate.cluster_bounds", 100, {"kind": "cluster_update"}, "deny",
             {"worker_cap": DEFAULT_WORKER_CAP, "shapes": list(DEFAULT_SHAPE_ALLOWLIST)}),
        Rule("protect.sla", 200, {"kind": ["job_update_schedule", "job_disable"]}, "deny"),
        Rule("protect.self", 200, {}, "deny", {"target_prefixes": ["steward"]}),
        Rule("safety.active_runs", 300, {"kind": ["cluster_stop", "cluster_update"]}, "deny"),
        Rule("verify.replay_required", 300, {"kind": "statement_repair"}, "deny", {"max_age_hours": 24}),
        Rule("tier.repair_quarantine", 500, {"kind": "statement_repair"}, "quarantine"),
        Rule("budget.big_resize", 500, {"kind": "cluster_update"}, "quarantine",
             {"max_ratio": 2.0, "min_ratio": 0.5}),
        Rule("budget.disable_pipeline", 500, {"kind": "job_disable"}, "quarantine"),
        Rule("rate.resizes_daily", 600, {"kind": "cluster_update"}, "deny",
             {"window_hours": 24, "max_allowed": 3}),
        Rule("rate.cron_edits_hourly", 600, {"kind": "job_update_schedule"}, "deny",
             {"window_hours": 1, "max_allowed": 10}),
    ]


__all__ = [
    "PolicyEngine", "RULE_CHECKS", "register_check", "selector_matches", "cron_min_interval_minutes",
    "default_rules", "DEFAULT_SHAPE_ALLOWLIST", "DEFAULT_WORKER_CAP", "KNOWN_KINDS", "UNKNOWN_KIND_RULE",
]
