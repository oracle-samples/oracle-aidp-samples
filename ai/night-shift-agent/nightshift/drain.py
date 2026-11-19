"""Drain of human-approved actions, shared by the in-cluster heartbeat and the external actuator.

An approval row a person set to ``approved`` is re-evaluated through the policy gate (the quarantine it was opened
under still fires; the recorded approval is what lifts it, deny rules still deny, and ``verify.replay_required`` needs
a verdict fresher than 24 h), then executed, and the row is marked executed or failed. The heartbeat passes
``kinds=("statement_repair",)``: a repair is a ledger write (the draft becomes a runbook row), so the loop closes inside
the platform without the laptop. Control-plane kinds (cluster_*) need the signer only the actuator has; the heartbeat
leaves them ``approved`` and reports them under ``left``.

Before the gate runs, every approved row is checked (``approval_problem``), because the row itself is only data in a
table and ``approved_by`` is an unauthenticated string:

* the kind must be one a quarantine rule can hold (``PolicyEngine.approvable_kinds``); an approval lifts a quarantine
  and nothing else, so a row carrying ``cluster_stop`` or any other kind is marked failed;
* the row must be the one the gate opened: exactly one row carries its ``approval_id``, it names a ``decision_id``,
  that decision is a quarantine of the same action the gate wrote while PLANNING, in the approval's own tick and in
  the last 48 hours, and no other approval row names that decision. A row inserted straight into ``approvals`` names
  no such decision; a copy of a real row shares its decision, and then neither the copy nor the original is
  executed. The quarantine the gate writes again each time it re-checks an approved row (provenance ``drain``) is
  never accepted, so it cannot be named by a later row. These rows are marked failed. One case is left to the
  grants: a quarantine whose approval row could not be written (the insert failed) is a planning decision no row
  names, and a row inserted for it would pass this check;
* an empty ``approved_by``, a service identity (the task principal, the heartbeat or actuator names, anything starting
  with ``nightshift_``) or, when ``approvers`` is given, a name not on that list leaves the row ``approved`` with
  ``DENIED_ON_EXECUTE approver.identity``;
* a control-plane kind (``CONTROL_PLANE_KINDS``) is executed only when an approver allowlist is configured
  (``NS_APPROVERS``); without one the row stays ``approved`` with ``DENIED_ON_EXECUTE approver.allowlist``.

None of this replaces the grants: INSERT on ``<ops>.approvals`` and ``<ops>.policy_decisions`` belongs to the agent's
principal alone and UPDATE on ``<ops>.approvals`` to the approvers role (platform roles). The Workbench approvals
notebook writes ``current_user()`` under the reviewer's own session, so the Delta history of the table records who ran
the update.

Two drainers can reach the same row (the heartbeat and the actuator both drain repairs; two actuators can overlap), so
the one that acts first claims the row (``approved`` -> ``executing``, ``claim_approval``) and only the claim holder
executes. A row left ``executing`` by a drainer that died is never retried; it expires and a person decides again.

``store.run_sql(sql)`` returns rows (tuples or Row objects) for a SELECT and may return anything for a write.
``write_decision(decision, action, provenance=..., principal=...)`` writes the decision row first; if it raises, the
action is denied (fail closed).
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Callable, Iterable

from . import ledger
from .actuator import _apply_statement_repair, _row_get, _sha16, action_from_json
from .types import Decision

LEDGER_KINDS: tuple[str, ...] = ("statement_repair",)
CONTROL_PLANE_KINDS: frozenset[str] = frozenset({"cluster_stop", "cluster_update", "job_update_schedule", "job_disable"})
SERVICE_IDENTITIES: frozenset[str] = frozenset({"", "unknown", "dataflow", "nightshift_heartbeat", "nightshift_actuator"})


def approver_problem(approved_by: str | None, approvers: Iterable[str] | None = None) -> str | None:
    """Why this approver may not lift a quarantine, or None when the name is acceptable."""
    name = (approved_by or "").strip()
    if name.lower() in SERVICE_IDENTITIES or name.lower().startswith("nightshift_"):
        return "approved_by missing or a service identity"
    allow = [a.strip().lower() for a in (approvers or []) if a and a.strip()]
    if allow and name.lower() not in allow:
        return "approver not on the NS_APPROVERS list"
    return None


def _count(rows: Any, name: str) -> int:
    rows = list(rows)
    return int(_row_get(rows[0], 0, name, 0) or 0) if rows else 0


def approval_problem(store: Any, ops: str, engine: Any, action: Any, approval_id: str, approved_by: str | None,
                     approvers: Iterable[str] | None = None, now: datetime | None = None) -> tuple[str, str, bool] | None:
    """Why an approved row may not be executed, as (rule, reason, terminal), or None when it may go to the gate.
    ``terminal`` rows can never become valid and are marked failed; the others stay approved and are retried.
    The checks that prove the row is the gate's own run first, so a row that is not is terminal whatever name it
    carries, and never lingers as an open approval."""
    approvable = engine.approvable_kinds()
    if action.kind not in approvable:
        return ("approval.kind", f"KIND_NOT_APPROVABLE: no quarantine rule holds {action.kind!r} for approval", True)
    try:
        named = list(store.run_sql(ledger.approval_binding_sql(ops, approval_id)))
        if len(named) != 1:
            return ("approval.trace", f"DUPLICATE_APPROVAL_ID: {len(named)} rows carry this approval id; none is executed", True)
        decision_id, opened_in = _row_get(named[0], 0, "decision_id"), _row_get(named[0], 2, "tick_id")
        if decision_id in (None, "") or opened_in in (None, ""):
            return ("approval.trace", "NO_DECISION_ID: the row names no gate decision, so the gate did not open it", True)
        trace = ledger.quarantine_trace_sql(ops, str(decision_id), action.signature, str(opened_in), now=now)
        if _count(store.run_sql(trace), "quarantined") < 1:
            return ("approval.trace", "NO_QUARANTINE_DECISION: the decision this row names is not the quarantine the gate "
                                      "wrote for this action when it opened an approval (same tick, last 48 hours)", True)
        holders = _count(store.run_sql(ledger.decision_holders_sql(ops, str(decision_id))), "holders")
        if holders != 1:
            return ("approval.trace", f"DECISION_SHARED: {holders} approval rows name this decision (a copied row); "
                                      "none is executed", True)
    except Exception as e:  # noqa: BLE001  (fail closed: no proof the gate opened this approval)
        return ("approval.trace", f"quarantine decision read failed ({_err(e, 80)})", False)
    problem = approver_problem(approved_by, approvers)
    if problem:
        return ("approver.identity", problem, False)
    allow = [a for a in (approvers or []) if a and str(a).strip()]
    if action.kind in CONTROL_PLANE_KINDS and not allow:
        return ("approver.allowlist", "NS_APPROVERS is not set; control-plane approvals run only for listed approvers", False)
    return None


def new_claim(tick_id: str, approval_id: str) -> str:
    return _sha16(tick_id, approval_id, os.getpid(), time.time_ns())


def claim_approval(store: Any, ops: str, approval_id: str, claim: str, now: datetime) -> tuple[bool, str]:
    """approved -> executing for this drainer. Returns (True, "") only when the row read back carries this claim;
    otherwise (False, reason). When the read-back itself fails the claim is handed back (executing -> approved, only
    if it is this drainer's), so an approval is never left stuck by a read error."""
    try:
        store.run_sql(ledger.claim_approval_sql(ops, approval_id, claim, now))
    except Exception as e:  # noqa: BLE001  (a concurrent-update conflict means another drainer took it)
        return False, f"claim not taken ({_err(e, 80)})"
    rows, last = None, None
    for _ in range(2):
        try:
            rows = list(store.run_sql(ledger.approval_claim_sql(ops, approval_id)))
            break
        except Exception as e:  # noqa: BLE001
            last = e
    if rows is None:
        try:
            store.run_sql(ledger.release_claim_sql(ops, approval_id, claim, now))
        except Exception:  # noqa: BLE001  (the row then expires; it is never executed twice)
            pass
        return False, f"claim read-back failed ({_err(last, 80)}); the claim was handed back and the row is retried next tick"
    mine = any(str(_row_get(r, 0, "status") or "") == "executing"
               and str(_row_get(r, 1, "note") or "").endswith(ledger.APPROVAL_CLAIM_TAG + claim) for r in rows)
    return (True, "") if mine else (False, "claimed by another drainer")


def _err(e: BaseException, n: int = 160) -> str:
    first = str(e).splitlines()[0] if str(e) else ""
    return f"{type(e).__name__}: {first[:n]}"


def drain_approvals(store: Any, ops: str, ctx: Any, engine: Any, write_decision: Callable[..., Any], tick_id: str,
                    now: datetime, log: Callable[..., Any] | None = None, kinds: Iterable[str] = LEDGER_KINDS,
                    verdict_hours: int = 24, approvers: Iterable[str] | None = None) -> dict:
    """Expire stale rows, then execute every approved action of the given kinds through the gate. Returns a report:
    ``expired`` (ok or the error), ``executed`` [{approval_id, runbook_id, approved_by}], ``left`` [{approval_id, kind,
    reason}] for rows that stay approved, ``errors`` [str] for rows marked failed or reads that failed."""
    kinds = tuple(kinds)
    emit = log or (lambda *a, **k: None)
    out: dict[str, Any] = {"expired": None, "executed": [], "left": [], "errors": []}
    try:
        store.run_sql(ledger.expire_approvals_sql(ops, now))
        out["expired"] = "ok"
    except Exception as e:  # noqa: BLE001
        out["expired"] = _err(e, 120)
    try:
        approved_rows = list(store.run_sql(ledger.approved_unexecuted_sql(ops, now)))
    except Exception as e:  # noqa: BLE001
        out["errors"].append(f"approved read failed: {_err(e, 120)}")
        return out
    if not approved_rows:
        return out
    try:
        ctx.kv["replay_verdicts"] = {
            str(_row_get(r, 0, "statement_sha256")): {"verdict": _row_get(r, 1, "verdict"),
                                                     "age_hours": float(_row_get(r, 2, "age_hours") or 0.0)}
            for r in store.run_sql(ledger.latest_replay_verdicts_sql(ops, verdict_hours, now))}
    except Exception as e:  # noqa: BLE001  (fails closed: verify.replay_required denies without a verdict)
        ctx.kv["replay_verdicts"] = {}
        out["errors"].append(f"replay verdicts read failed: {_err(e, 120)}")
    try:
        for r in approved_rows:
            aid = str(_row_get(r, 0, "approval_id"))
            sig = str(_row_get(r, 1, "action_signature"))
            approved_by = str(_row_get(r, 3, "approved_by") or "unknown")
            try:
                a = action_from_json(_row_get(r, 2, "action_json"))
            except Exception as e:  # noqa: BLE001
                store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note=f"BAD_ACTION_JSON: {_err(e, 80)}", now=now))
                out["errors"].append(f"{aid}: action_json unreadable")
                continue
            if a.signature != sig:
                store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note="SIGNATURE_MISMATCH", now=now))
                out["errors"].append(f"{aid}: signature mismatch")
                continue
            if a.kind not in kinds:
                out["left"].append({"approval_id": aid, "kind": a.kind, "reason": "not drained here (control-plane action; the external actuator executes it)"})
                continue
            blocked = approval_problem(store, ops, engine, a, aid, approved_by, approvers, now=now)
            if blocked:
                rule, problem, terminal = blocked
                if terminal:
                    store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note=f"{rule}: {problem}", now=now))
                    out["errors"].append(f"{aid}: {rule}: {problem}")
                else:
                    store.run_sql(ledger.mark_approval_sql(ops, aid, "approved", note=f"DENIED_ON_EXECUTE {rule}: {problem}", now=now))
                    out["left"].append({"approval_id": aid, "kind": a.kind, "reason": f"{rule}: {problem}"})
                emit("approval_denied_on_execute", approval_id=aid, rule=rule, reason=problem)
                continue
            ctx.kv["approved_signatures"] = {sig: approved_by}
            d = engine.evaluate(a, ctx)
            if d.decision == "quarantine":
                d = Decision(sig, "allow", d.rule_id, f"APPROVED_BY:{approved_by}", evaluated_rules=d.evaluated_rules,
                             latency_ms=d.latency_ms)
                try:
                    write_decision(d, a, provenance="approval", principal=approved_by)
                except Exception as e:  # noqa: BLE001
                    d = Decision(sig, "deny", d.rule_id, f"AUDIT_WRITE_FAILED: {_err(e, 80)}", evaluated_rules=d.evaluated_rules)
            if d.decision != "allow":
                store.run_sql(ledger.mark_approval_sql(ops, aid, "approved", note=f"DENIED_ON_EXECUTE {d.rule_id}: {d.reason}", now=now))
                out["left"].append({"approval_id": aid, "kind": a.kind, "reason": f"{d.rule_id}: {d.reason}"})
                emit("approval_denied_on_execute", approval_id=aid, rule=d.rule_id, reason=d.reason)
                continue
            claimed, why = claim_approval(store, ops, aid, new_claim(tick_id, aid), now)
            if not claimed:
                out["left"].append({"approval_id": aid, "kind": a.kind, "reason": why})
                continue
            try:
                outcome = _apply_statement_repair(store, ops, a, approved_by, now)
                status = "applied"
            except Exception as e:  # noqa: BLE001
                outcome, status = {"error": _err(e, 200)}, "failed"
            store.run_sql(ledger.insert_action(ops, {
                "tick_id": tick_id, "action_signature": sig, "kind": a.kind, "target": a.target, "params": a.params,
                "previous_value": None, "reversible": True, "decision_id": _sha16(tick_id, sig, d.decision, d.rule_id),
                "status": status, "result": outcome, "applied_at": now if status == "applied" else None}, now=now))
            if status == "failed":
                store.run_sql(ledger.mark_approval_sql(ops, aid, "failed", note=str(outcome.get("error")), now=now))
                out["errors"].append(f"{aid}: {outcome.get('error')}")
                emit("approval_failed", approval_id=aid, error=outcome.get("error"))
            else:
                store.run_sql(ledger.mark_approval_sql(ops, aid, "executed", note=f"applied by tick {tick_id[:12]}", now=now))
                out["executed"].append({"approval_id": aid, "runbook_id": outcome.get("runbook_id"), "approved_by": approved_by})
                emit("approval_executed", approval_id=aid, runbook_id=outcome.get("runbook_id"), approved_by=approved_by)
    finally:
        ctx.kv.pop("approved_signatures", None)
    return out


__all__ = ["LEDGER_KINDS", "CONTROL_PLANE_KINDS", "SERVICE_IDENTITIES", "approver_problem", "approval_problem",
           "claim_approval", "new_claim", "drain_approvals"]
