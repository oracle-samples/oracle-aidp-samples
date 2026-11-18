"""Drain of human-approved actions, shared by the in-cluster heartbeat and the external actuator.

An approval row a person set to ``approved`` is re-evaluated through the policy gate (the quarantine it was opened
under still fires; the recorded approval is what lifts it, deny rules still deny, and ``verify.replay_required`` needs
a verdict fresher than 24 h), then executed, and the row is marked executed or failed. The heartbeat passes
``kinds=("statement_repair",)``: a repair is a ledger write (the draft becomes a runbook row), so the loop closes inside
the platform without the laptop. Control-plane kinds (cluster_*) need the signer only the actuator has; the heartbeat
leaves them ``approved`` and reports them under ``left``.

Who approved is checked before the gate runs: an empty ``approved_by``, a service identity (the task principal, the
heartbeat or actuator names, anything starting with ``nightshift_``) or, when ``approvers`` is given, a name not on that
list leaves the row ``approved`` with ``DENIED_ON_EXECUTE approver.identity`` and nothing is applied. The column itself
is only as trustworthy as the UPDATE grant on ``<ops>.approvals``; in production that grant belongs to the approvers
role alone (platform roles), and the Workbench approvals notebook writes ``current_user()`` under the reviewer's own
session, so the Delta history of the table records who ran the update.

``store.run_sql(sql)`` returns rows (tuples or Row objects) for a SELECT and may return anything for a write.
``write_decision(decision, action, provenance=..., principal=...)`` writes the decision row first; if it raises, the
action is denied (fail closed).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Iterable

from . import ledger
from .actuator import _apply_statement_repair, _row_get, _sha16, action_from_json
from .types import Decision

LEDGER_KINDS: tuple[str, ...] = ("statement_repair",)
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
            problem = approver_problem(approved_by, approvers)
            if problem:
                store.run_sql(ledger.mark_approval_sql(ops, aid, "approved", note=f"DENIED_ON_EXECUTE approver.identity: {problem}", now=now))
                out["left"].append({"approval_id": aid, "kind": a.kind, "reason": f"approver.identity: {problem}"})
                emit("approval_denied_on_execute", approval_id=aid, rule="approver.identity", reason=problem)
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


__all__ = ["LEDGER_KINDS", "SERVICE_IDENTITIES", "approver_problem", "drain_approvals"]
