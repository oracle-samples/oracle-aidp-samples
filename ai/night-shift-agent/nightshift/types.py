"""Shared dataclasses used across the package."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

Tier = Literal["high", "normal", "low_compute", "critical", "paused"]
DecisionKind = Literal["allow", "deny", "quarantine"]
ActionKind = Literal[
    "cluster_stop", "cluster_update", "job_update_schedule", "job_disable", "job_run", "job_repair",
    "statement_repair",   # a drafted replacement for a watched production statement; applied as a runbook, never to code
    "notify", "noop",
]


@dataclass
class TickContext:
    now_utc: datetime
    budget_remaining_pct: float | None
    projected_overrun_pct: float | None
    llm_remaining_pct: float | None
    budget_source: Literal["ledger", "cached", "unknown"]
    cluster_states: dict[str, str] = field(default_factory=dict)
    recent_failures: list[dict[str, Any]] = field(default_factory=list)
    wake_events: list[dict[str, Any]] = field(default_factory=list)
    kv: dict[str, Any] = field(default_factory=dict)


@dataclass
class Action:
    kind: ActionKind
    target: str
    params: dict[str, Any] = field(default_factory=dict)
    previous_value: dict[str, Any] | None = None
    reversible: bool = True

    @property
    def signature(self) -> str:
        payload = f"{self.kind}|{self.target}|{json.dumps(self.params, sort_keys=True, default=str)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class Decision:
    action_signature: str
    decision: DecisionKind
    rule_id: str | None
    reason: str
    evaluated_rules: list[str] = field(default_factory=list)
    latency_ms: float = 0.0


@dataclass
class Rule:
    rule_id: str
    priority: int
    selector: dict[str, Any]
    effect: DecisionKind
    params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
