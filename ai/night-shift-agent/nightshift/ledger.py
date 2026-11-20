"""DDL for the nightshift_ops ledger + Spark SQL string builders (INSERT / UPDATE / MERGE).

State tables, approvals and the morning-review views live here. Everything is a pure string
builder: no Spark, no network. Values are rendered with ``lit()``.

Idempotency keys: wake_events.event_id, ticks.tick_id, policy_decisions.decision_id,
approvals.approval_id, actions.action_id (derived from tick_id + action_signature),
incidents.incident_key (= sha256(fingerprint_key|pipeline)[:16]), runbooks.runbook_id,
flight_recorder.record_id, charter_history.charter_sha256, agent_registry.agent_id,
budget_ledger.entry_id, llm_ledger.entry_id, policy_rules.rule_id.

INSERT builders use ``INSERT INTO t (...) SELECT ... WHERE NOT EXISTS (key)`` so a retried
tick never double-writes; upserts use ``MERGE INTO``. Append-only tables carry
``TBLPROPERTIES ('delta.appendOnly' = 'true')`` so they stay time-travelable for forensics.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

NL = "\n"
OPS_DEFAULT = "default.nightshift_ops"

APPEND_ONLY: frozenset[str] = frozenset(
    {"policy_decisions", "actions", "ticks", "flight_recorder", "charter_history", "replay_verdicts"}
)
PARTITIONED: dict[str, str] = {"budget_ledger": "window_day", "llm_ledger": "window_day"}

# table -> [(column, spark type)], in dependency order (tables that are referenced come first).
COLUMNS: dict[str, list[tuple[str, str]]] = {
    "policy_rules": [
        ("rule_id", "STRING"), ("priority", "INT"), ("selector", "STRING"), ("effect", "STRING"),
        ("params", "STRING"), ("enabled", "BOOLEAN"), ("description", "STRING"),
        ("updated_by", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ],
    "charter_history": [
        ("charter_sha256", "STRING"), ("version", "INT"), ("previous_sha256", "STRING"),
        ("content", "STRING"), ("change_source", "STRING"), ("approved_by", "STRING"),
        ("created_at", "TIMESTAMP"),
    ],
    "agent_registry": [
        ("agent_id", "STRING"), ("owner", "STRING"), ("role_key", "STRING"),
        ("allowed_schemas", "STRING"), ("write_tier", "STRING"), ("charter_sha256", "STRING"),
        ("prompt_sha256", "STRING"), ("card_json", "STRING"), ("card_sha256", "STRING"),
        ("status", "STRING"), ("quotas", "STRING"), ("tables_touched", "STRING"),
        ("denial_rate", "DOUBLE"), ("last_run_at", "TIMESTAMP"),
        ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ],
    "wake_events": [
        ("event_id", "STRING"), ("source", "STRING"), ("reason", "STRING"), ("pipeline", "STRING"),
        ("payload_hash", "STRING"), ("payload", "STRING"), ("consumed_by_tick", "STRING"),
        ("consumed_at", "TIMESTAMP"), ("created_at", "TIMESTAMP"),
    ],
    "ticks": [
        ("tick_id", "STRING"), ("started_at", "TIMESTAMP"), ("finished_at", "TIMESTAMP"),
        ("compute_tier", "STRING"), ("llm_tier", "STRING"), ("remaining_pct", "DOUBLE"),
        ("projected_overrun_pct", "DOUBLE"), ("llm_remaining_pct", "DOUBLE"),
        ("budget_source", "STRING"), ("actions_proposed", "INT"), ("actions_applied", "INT"),
        ("actions_quarantined", "INT"), ("actions_denied", "INT"), ("dry_run", "BOOLEAN"),
        ("created_at", "TIMESTAMP"),
    ],
    "budget_ledger": [
        ("entry_id", "STRING"), ("tick_id", "STRING"), ("job_key", "STRING"), ("run_key", "STRING"),
        ("cluster_key", "STRING"), ("category", "STRING"), ("ocpu_hours", "DOUBLE"),
        ("est_cost", "DOUBLE"), ("window_hour", "STRING"), ("window_day", "STRING"),
        ("created_at", "TIMESTAMP"),
    ],
    "llm_ledger": [
        ("entry_id", "STRING"), ("tick_id", "STRING"), ("run_key", "STRING"), ("model", "STRING"),
        ("calls", "BIGINT"), ("prompt_tokens", "BIGINT"), ("completion_tokens", "BIGINT"),
        ("est_cost", "DOUBLE"), ("outcome", "STRING"), ("window_hour", "STRING"),
        ("window_day", "STRING"), ("created_at", "TIMESTAMP"),
    ],
    "policy_decisions": [
        ("decision_id", "STRING"), ("tick_id", "STRING"), ("action_signature", "STRING"),
        ("action_kind", "STRING"), ("target", "STRING"), ("args_sha256", "STRING"),
        ("decision", "STRING"), ("rule_id", "STRING"), ("rules_evaluated", "STRING"),
        ("rules_triggered", "STRING"), ("reason", "STRING"), ("provenance", "STRING"),
        ("principal", "STRING"), ("latency_ms", "DOUBLE"), ("created_at", "TIMESTAMP"),
    ],
    "approvals": [
        ("approval_id", "STRING"), ("tick_id", "STRING"), ("action_signature", "STRING"),
        ("action_json", "STRING"), ("decision_id", "STRING"), ("incident_key", "STRING"),
        ("requested_at", "TIMESTAMP"), ("expires_at", "TIMESTAMP"), ("approved_by", "STRING"),
        ("approved_at", "TIMESTAMP"), ("status", "STRING"), ("note", "STRING"),
        ("created_at", "TIMESTAMP"),
    ],
    "incidents": [
        ("incident_key", "STRING"), ("fingerprint_key", "STRING"), ("pipeline", "STRING"),
        ("label", "STRING"), ("confidence", "DOUBLE"), ("sample_error", "STRING"),
        ("root_cause", "STRING"), ("fix", "STRING"), ("runbook_id", "STRING"), ("verdict", "STRING"),
        ("approver", "STRING"), ("occurrences", "INT"), ("first_seen", "TIMESTAMP"),
        ("last_seen", "TIMESTAMP"), ("last_run_key", "STRING"), ("provenance", "STRING"),
        ("status", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ],
    "runbooks": [
        ("runbook_id", "STRING"), ("fingerprint_key", "STRING"), ("label", "STRING"),
        ("title", "STRING"), ("procedure", "STRING"), ("source_incident_key", "STRING"),
        ("approved_by", "STRING"), ("approved_at", "TIMESTAMP"), ("successes", "INT"),
        ("failures", "INT"), ("provenance", "STRING"), ("enabled", "BOOLEAN"),
        ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ],
    "actions": [
        ("action_id", "STRING"), ("tick_id", "STRING"), ("action_signature", "STRING"),
        ("kind", "STRING"), ("target", "STRING"), ("params", "STRING"), ("previous_value", "STRING"),
        ("reversible", "BOOLEAN"), ("decision_id", "STRING"), ("status", "STRING"),
        ("result", "STRING"), ("applied_at", "TIMESTAMP"), ("created_at", "TIMESTAMP"),
    ],
    "flight_recorder": [
        ("record_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("job_name", "STRING"), ("run_id", "STRING"),
        ("app_id", "STRING"), ("kind", "STRING"), ("entity_id", "STRING"), ("name", "STRING"), ("status", "STRING"),
        ("duration_ms", "BIGINT"), ("num_tasks", "INT"), ("failed_tasks", "INT"), ("shuffle_read_bytes", "BIGINT"),
        ("shuffle_write_bytes", "BIGINT"), ("spill_bytes", "BIGINT"), ("input_bytes", "BIGINT"), ("output_bytes", "BIGINT"),
        ("details_json", "STRING"), ("created_at", "TIMESTAMP"),
    ],
    # one row per model-drafted replacement statement: what was asked, what came back, whether it passed validation and replay
    "repair_drafts": [
        ("draft_id", "STRING"), ("tick_id", "STRING"), ("incident_key", "STRING"), ("table_name", "STRING"),
        ("statement_sha256", "STRING"), ("model", "STRING"), ("attempt", "INT"), ("prompt_sha256", "STRING"),
        ("draft_sql", "STRING"), ("draft_sha256", "STRING"), ("validation", "STRING"), ("verdict_id", "STRING"),
        ("verdict", "STRING"), ("diff_rows", "BIGINT"), ("status", "STRING"), ("approval_id", "STRING"),
        ("runbook_id", "STRING"), ("provenance", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP"),
    ],
    # one row per hazard-replay grading of a statement against a production table (the "replay before approval" evidence)
    "replay_verdicts": [
        ("verdict_id", "STRING"), ("tick_id", "STRING"), ("table_name", "STRING"), ("statement_sha256", "STRING"),
        ("candidate", "STRING"), ("source_version", "BIGINT"), ("verdict", "STRING"), ("passes_snapshot", "BOOLEAN"),
        ("passes_run_twice", "BOOLEAN"), ("hazards_json", "STRING"), ("diffs_json", "STRING"), ("diff_rows", "BIGINT"),
        ("statements_run", "INT"), ("elapsed_s", "DOUBLE"), ("error", "STRING"), ("provenance", "STRING"),
        ("created_at", "TIMESTAMP"),
    ],
}

TABLE_ORDER: tuple[str, ...] = tuple(COLUMNS)
IDEMPOTENCY_KEY: dict[str, str] = {
    "policy_rules": "rule_id", "charter_history": "charter_sha256", "agent_registry": "agent_id",
    "wake_events": "event_id", "ticks": "tick_id", "budget_ledger": "entry_id", "llm_ledger": "entry_id",
    "policy_decisions": "decision_id", "approvals": "approval_id", "incidents": "incident_key",
    "runbooks": "runbook_id", "actions": "action_id", "flight_recorder": "record_id", "replay_verdicts": "verdict_id",
    "repair_drafts": "draft_id",
}


def _create_table(table: str) -> str:
    cols = ",\n".join(f"  {name} {typ}" for name, typ in COLUMNS[table])
    sql = f"CREATE TABLE IF NOT EXISTS {{ops}}.{table} (\n{cols}\n) USING DELTA"
    if table in PARTITIONED:
        sql += f"\nPARTITIONED BY ({PARTITIONED[table]})"
    if table in APPEND_ONLY:
        sql += "\nTBLPROPERTIES ('delta.appendOnly' = 'true')"
    return sql


# table -> CREATE TABLE IF NOT EXISTS ... with a literal {ops} placeholder.
DDL: dict[str, str] = {table: _create_table(table) for table in TABLE_ORDER}


def ddl_statements(ops: str = OPS_DEFAULT) -> list[str]:
    """CREATE SCHEMA first, then every CREATE TABLE in dependency order, rendered for ``ops``."""
    return [f"CREATE SCHEMA IF NOT EXISTS {ops}"] + [DDL[t].replace("{ops}", ops) for t in TABLE_ORDER]


def render_ddl_file(ops: str = OPS_DEFAULT) -> str:
    """The text of sql/ddl_ops.sql. Generated, never hand-edited (test_ledger checks equality). The header holds no
    semicolon, so splitting the file on ';' yields exactly the statements (ddl_statements() gives them directly)."""
    header = (
        "-- Night Shift ops ledger DDL (Spark SQL, Delta). GENERATED by nightshift.ledger.render_ddl_file()\n"
        "-- Do not edit by hand. Regenerate with: python -m nightshift.ledger [catalog.schema]\n"
        f"-- ops schema: {ops}\n\n"
    )
    return header + ";\n\n".join(ddl_statements(ops)) + ";\n"


def write_ddl_file(ops: str = OPS_DEFAULT, path: str | None = None) -> str:
    """Rewrite sql/ddl_ops.sql (or ``path``) for ``ops``; returns the path written."""
    from pathlib import Path

    target = Path(path) if path else Path(__file__).resolve().parents[1] / "sql" / "ddl_ops.sql"
    target.write_text(render_ddl_file(ops), encoding="utf-8", newline="\n")
    return str(target)


# --- literals ------------------------------------------------------------------------------

def _escape_str(value: str) -> str:
    # Spark SQL string literals process backslash escapes (escapedStringLiterals=false): the backslash is doubled and
    # the quote is backslash-escaped. A doubled quote ('') is NOT an escape in Spark: 'it''s' is two adjacent literals
    # and reads back as "its". Control chars are kept.
    return value.replace("\\", "\\\\").replace("'", "\\'")


def lit(value: Any) -> str:
    """Render a Python value as a Spark SQL literal.

    None -> NULL; bool -> TRUE/FALSE; int -> digits; float -> repr (nan/inf -> NULL);
    datetime -> TIMESTAMP '...' (tz-aware converted to UTC, naive taken as UTC);
    date -> DATE '...'; dict/list/tuple -> JSON string literal (sorted keys); str -> quoted/escaped.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "NULL"
        return f'CAST({value!r} AS DOUBLE)'
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (dict, list, tuple)):
        return f"'{_escape_str(json.dumps(value, sort_keys=True, default=str, ensure_ascii=False))}'"
    if isinstance(value, bytes):
        return f"X'{value.hex()}'"
    return f"'{_escape_str(str(value))}'"


def _now(now: datetime | None) -> datetime:
    return now if now is not None else datetime.now(timezone.utc)


def _sha16(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]


def incident_key(fingerprint_key: str, pipeline: str) -> str:
    """incidents.incident_key = sha256(fingerprint_key|pipeline)[:16]."""
    return _sha16(fingerprint_key, pipeline)


def _row_for(table: str, row: Mapping[str, Any]) -> list[tuple[str, Any]]:
    cols = [c for c, _ in COLUMNS[table]]
    unknown = set(row) - set(cols)
    if unknown:
        raise ValueError(f"{table}: unknown columns {sorted(unknown)}")
    return [(c, row.get(c)) for c in cols]


def typed_lit(value: Any, spark_type: str) -> str:
    """lit() plus an explicit CAST for integer literals bound for BIGINT or DOUBLE columns: Delta's schema
    enforcement rejects an INT literal into a BIGINT column (DELTA_FAILED_TO_MERGE_FIELDS)."""
    t = (spark_type or "").upper()
    if isinstance(value, bool) or value is None:
        return lit(value)
    if isinstance(value, int) and t in ("BIGINT", "LONG"):
        return f"CAST({value} AS BIGINT)"
    if isinstance(value, int) and t == "DOUBLE":
        return f"CAST({value} AS DOUBLE)"
    return lit(value)


def _idempotent_insert(ops: str, table: str, row: Mapping[str, Any]) -> str:
    """INSERT ... SELECT ... WHERE NOT EXISTS (same key), valid Spark SQL and duckdb."""
    key = IDEMPOTENCY_KEY[table]
    if row.get(key) in (None, ""):
        raise ValueError(f"{table}.{key} is required")
    pairs = _row_for(table, row)
    types = dict(COLUMNS[table])
    cols = ", ".join(c for c, _ in pairs)
    vals = ", ".join(typed_lit(v, types.get(c, "")) for c, v in pairs)
    return (
        f"INSERT INTO {ops}.{table} ({cols})\n"
        f"SELECT {vals}\n"
        f"WHERE NOT EXISTS (SELECT 1 FROM {ops}.{table} WHERE {key} = {lit(row[key])})"
    )


def _with_defaults(row: Mapping[str, Any], defaults: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for k, v in defaults.items():
        if out.get(k) is None:
            out[k] = v
    return out


# --- builders ------------------------------------------------------------------------------

def insert_wake_event(ops: str, event: Mapping[str, Any], now: datetime | None = None) -> str:
    """Idempotent on event_id (derived from source|reason|payload_hash when absent)."""
    ts = _now(now)
    payload = event.get("payload")
    payload_json = json.dumps(payload, sort_keys=True, default=str) if isinstance(payload, (dict, list)) else payload
    payload_hash = event.get("payload_hash") or (
        hashlib.sha256((payload_json or "").encode("utf-8")).hexdigest()[:16] if payload_json else None
    )
    row = _with_defaults(
        {**event, "payload": payload_json, "payload_hash": payload_hash},
        {"event_id": _sha16(event.get("source"), event.get("reason"), payload_hash), "created_at": ts},
    )
    return _idempotent_insert(ops, "wake_events", row)


def consume_wake_events_sql(ops: str, tick_id: str, now: datetime | None = None) -> str:
    """Claim every unconsumed wake event for this tick. Re-running is a no-op (WHERE ... IS NULL)."""
    if not tick_id:
        raise ValueError("tick_id is required")
    return (
        f"UPDATE {ops}.wake_events SET consumed_by_tick = {lit(tick_id)}, consumed_at = {lit(_now(now))}\n"
        f"WHERE consumed_by_tick IS NULL"
    )


def insert_tick(ops: str, tick: Mapping[str, Any], now: datetime | None = None) -> str:
    row = _with_defaults(tick, {"started_at": _now(now), "created_at": _now(now), "dry_run": False})
    return _idempotent_insert(ops, "ticks", row)


def insert_replay_verdict(ops: str, verdict: Mapping[str, Any], now: datetime | None = None) -> str:
    """Replay-verdict row. verdict_id defaults to sha16(tick_id|table_name|statement_sha256|candidate); the
    hazards/diffs dicts are stored as JSON."""
    v = dict(verdict)
    for k in ("hazards_json", "diffs_json"):
        if isinstance(v.get(k), (list, tuple, dict)):
            v[k] = json.dumps(v[k], sort_keys=True, default=str)
    row = _with_defaults(v, {
        "verdict_id": _sha16(v.get("tick_id"), v.get("table_name"), v.get("statement_sha256"), v.get("candidate")),
        "provenance": "heartbeat", "created_at": _now(now),
    })
    return _idempotent_insert(ops, "replay_verdicts", row)


def insert_repair_draft(ops: str, draft: Mapping[str, Any], now: datetime | None = None) -> str:
    """Repair-draft row, one per model call. draft_id defaults to
    sha16(tick_id|attempt|statement_sha256|draft_sha256|model) so an identical draft text produced on a later tick
    gets its own row (own verdict, own timestamp) and the per-day cap counts real calls; status defaults to the
    validation outcome ('rejected' when validation failed, else 'graded')."""
    d = dict(draft)
    row = _with_defaults(d, {
        "draft_id": _sha16(d.get("tick_id"), d.get("attempt"), d.get("statement_sha256"), d.get("draft_sha256"), d.get("model")),
        "status": "rejected" if str(d.get("validation", "")).startswith("rejected") else "graded",
        "provenance": "heartbeat", "created_at": _now(now), "updated_at": _now(now),
    })
    return _idempotent_insert(ops, "repair_drafts", row)


def update_draft_status_sql(ops: str, draft_id: str, status: str, approval_id: str | None = None,
                            runbook_id: str | None = None, now: datetime | None = None,
                            only_from: tuple[str, ...] | None = None) -> str:
    """Move a draft row to ``status``; with ``only_from`` the update applies only when the row is still in one of
    those states, so a re-proposal can never rewrite a row a person already approved or rejected."""
    sets = [f"status = {lit(status)}", f"updated_at = {lit(_now(now))}"]
    if approval_id is not None:
        sets.append(f"approval_id = {lit(approval_id)}")
    if runbook_id is not None:
        sets.append(f"runbook_id = {lit(runbook_id)}")
    guard = f" AND status IN ({', '.join(lit(s) for s in only_from)})" if only_from else ""
    return f"UPDATE {ops}.repair_drafts SET {', '.join(sets)} WHERE draft_id = {lit(draft_id)}{guard}"


def drafts_today_sql(ops: str, statement_sha256: str, now: datetime | None = None, runner: str | None = None) -> str:
    """Three counts for a statement: n = model replies in the trailing 24 h (a reply the parser rejected counts, a
    call that failed before any reply and a claim row do not), safe = replay_safe drafts that still carry a live
    approval (pending and unexpired, approved, or executed), failed_recent = calls that failed inside the last hour
    by this runner when `runner` is given (the runner backs off instead of hammering a model service that fails for
    it; another runner's failures neither back it off nor eat the cap it needs). A replay_safe draft the gate denied, a person rejected, or that lapsed does not suppress redrafting."""
    end = _now(now)
    return (
        f"SELECT SUM(CASE WHEN d.validation <> 'claim' AND d.validation NOT LIKE 'rejected: ai_generate failed%'" + NL +
        f"                AND d.created_at >= {lit(end)} - INTERVAL 24 HOURS THEN 1 ELSE 0 END) AS n,\n"
        "       SUM(CASE WHEN d.verdict = 'replay_safe' AND a.status IN ('pending', 'approved', 'executing', 'executed')\n"
        f"                 AND (a.status <> 'pending' OR a.expires_at IS NULL OR a.expires_at > {lit(end)})\n"
        "                THEN 1 ELSE 0 END) AS safe,\n"
        f"       SUM(CASE WHEN d.validation LIKE 'rejected: ai_generate failed%' AND d.created_at >= {lit(end)} - INTERVAL 60 MINUTES\n"
        + (f"                 AND d.provenance = {lit(runner)}\n" if runner else "")
        + "                THEN 1 ELSE 0 END) AS failed_recent\n"
        f"FROM {ops}.repair_drafts d LEFT JOIN {ops}.approvals a ON a.approval_id = d.approval_id\n"
        f"WHERE d.statement_sha256 = {lit(statement_sha256)}"
    )


CLAIM_LEASE_MINUTES = 60


def claim_id_for(statement_sha256: str) -> str:
    """One lease row per statement (validation 'claim'); the holder is the run whose nonce is in tick_id."""
    return _sha16("claim", statement_sha256)


def claim_take_over_sql(ops: str, claim_id: str, run_nonce: str, runner: str, now: datetime | None = None) -> str:
    """Re-arm a lease that is released ('done') or stale (still 'drafting' but untouched for CLAIM_LEASE_MINUTES, the
    holder having been killed). A fresh lease held by another run is left alone."""
    end = _now(now)
    return (f"UPDATE {ops}.repair_drafts SET tick_id = {lit(run_nonce)}, provenance = {lit(runner)}, status = 'drafting', "
            f"updated_at = {lit(end)} WHERE draft_id = {lit(claim_id)} AND validation = 'claim' "
            f"AND (status <> 'drafting' OR updated_at < {lit(end)} - INTERVAL {CLAIM_LEASE_MINUTES} MINUTES)")


def claim_owner_sql(ops: str, claim_id: str) -> str:
    """Who holds a drafting lease: run nonce (tick_id column), provenance, status, updated_at."""
    return f"SELECT tick_id, provenance, status, updated_at FROM {ops}.repair_drafts WHERE draft_id = {lit(claim_id)}"


def claim_release_sql(ops: str, claim_id: str, run_nonce: str, now: datetime | None = None) -> str:
    """Release the lease, only if this run still holds it."""
    return (f"UPDATE {ops}.repair_drafts SET status = 'done', updated_at = {lit(_now(now))} "
            f"WHERE draft_id = {lit(claim_id)} AND tick_id = {lit(run_nonce)} AND status = 'drafting'")


def decided_draft_sql(ops: str, draft_sha256: str) -> str:
    """Approvals already opened for this exact draft text (any tick): a pending, approved, executed or rejected row
    means the heartbeat must not open another one. action_json carries draft_sha256 in its params."""
    pattern = '%"draft_sha256": "' + str(draft_sha256) + '"%'   # lit() handles escaping; the kernel may be Python < 3.12
    return (
        f"SELECT approval_id, status FROM {ops}.approvals\n"
        f"WHERE action_json LIKE {lit(pattern)}\n"
        "AND status IN ('pending', 'approved', 'executing', 'executed', 'rejected') LIMIT 1"
    )


def latest_verdict_for_statement_sql(ops: str, statement_sha256: str, hours: float = 24, now: datetime | None = None) -> str:
    """The newest verdict row for one statement hash inside the window: verdict_id, verdict, diffs_json, hazards_json,
    elapsed_s, created_at, error. The heartbeat reuses it as the base verdict when NS_REUSE_VERDICT_H is set (the actuator's
    drafting pass), so a statement graded by the scheduled job an hour ago is not replayed again just to draft."""
    end = _now(now)
    return (
        f"SELECT verdict_id, verdict, diffs_json, hazards_json, elapsed_s, created_at, error FROM {ops}.replay_verdicts" + NL +
        f"WHERE statement_sha256 = {lit(statement_sha256)} AND created_at >= {lit(end)} - INTERVAL {int(round(hours * 60))} MINUTES" + NL +
        "ORDER BY created_at DESC, verdict_id DESC LIMIT 1"
    )


def graded_statements_sql(ops: str, hours: int = 24, now: datetime | None = None) -> str:
    """Statement hashes whose LATEST verdict inside the window is settled (replay_safe, silent_wrong, snapshot_wrong
    or insufficient_coverage),
    plus those whose newest row is a crash younger than two hours (regrading a crash every pass would burn the pass;
    an older crash is regraded). Same "latest row" rule as statements_needing_draft_sql and the heartbeat's reuse."""
    latest = latest_replay_verdicts_sql(ops, hours, now)
    return (
        "SELECT statement_sha256 FROM (" + NL + latest + NL + ") g" + NL +
        "WHERE verdict IN ('replay_safe', 'silent_wrong', 'snapshot_wrong', 'insufficient_coverage') OR (verdict = 'crash' AND age_hours < 2.0)"
    )


def statements_needing_draft_sql(ops: str, hours: int = 24, now: datetime | None = None, cap: int = 3) -> str:
    """Statement hashes whose latest verdict in the window is silent_wrong, that have no replay_safe draft with a live
    approval (pending and unexpired, approved, or executed), and fewer than `cap` drafts in the trailing 24 h. The
    actuator's drafting pass hands these to the heartbeat, which applies the same checks again before any model call."""
    end = _now(now)
    latest = latest_replay_verdicts_sql(ops, hours, now)
    return (
        f"SELECT v.statement_sha256 FROM (" + NL + latest + NL + ") v" + NL +
        "WHERE v.verdict = 'silent_wrong'" + NL +
        f"AND NOT EXISTS (SELECT 1 FROM {ops}.repair_drafts d LEFT JOIN {ops}.approvals a ON a.approval_id = d.approval_id" + NL +
        "                WHERE d.statement_sha256 = v.statement_sha256" + NL +
        "                AND ((d.verdict = 'replay_safe' AND a.status IN ('pending', 'approved', 'executing', 'executed')" + NL +
        f"                      AND (a.status <> 'pending' OR a.expires_at IS NULL OR a.expires_at > {lit(end)}))" + NL +
        f"                     OR (d.validation = 'claim' AND d.status = 'drafting' AND d.updated_at >= {lit(end)} - INTERVAL {CLAIM_LEASE_MINUTES} MINUTES)))" + NL +
        f"AND (SELECT COUNT(*) FROM {ops}.repair_drafts d2 WHERE d2.statement_sha256 = v.statement_sha256" + NL +
        "     AND d2.validation <> 'claim' AND d2.validation NOT LIKE 'rejected: ai_generate failed%'" + NL +
        f"     AND d2.created_at >= {lit(end)} - INTERVAL 24 HOURS) < {int(cap)}"
    )


def latest_replay_verdicts_sql(ops: str, hours: int = 24, now: datetime | None = None) -> str:
    """Latest verdict per statement hash in the trailing window: statement_sha256, verdict, age_hours. Loaded into
    ctx.kv['replay_verdicts'] by the tick so verify.replay_required can check a proposed draft."""
    end = _now(now)
    return (
        "SELECT statement_sha256, verdict, age_hours FROM (\n"
        f"  SELECT statement_sha256, verdict,\n"
        f"         (unix_timestamp({lit(end)}) - unix_timestamp(created_at)) / 3600.0 AS age_hours,\n"
        f"         ROW_NUMBER() OVER (PARTITION BY statement_sha256 ORDER BY created_at DESC, verdict_id DESC) AS rn\n"
        f"  FROM {ops}.replay_verdicts WHERE created_at >= {lit(end)} - INTERVAL {int(hours)} HOURS\n"
        ") WHERE rn = 1"
    )


def insert_decision(ops: str, decision: Mapping[str, Any], now: datetime | None = None) -> str:
    """Policy decision row. decision_id defaults to sha16(tick_id|action_signature|decision|rule_id)."""
    d = dict(decision)
    for k in ("rules_evaluated", "rules_triggered"):
        if isinstance(d.get(k), (list, tuple, dict)):
            d[k] = json.dumps(d[k], sort_keys=True, default=str)
    row = _with_defaults(d, {
        "decision_id": _sha16(d.get("tick_id"), d.get("action_signature"), d.get("decision"), d.get("rule_id")),
        "created_at": _now(now),
    })
    return _idempotent_insert(ops, "policy_decisions", row)


def insert_action(ops: str, action: Mapping[str, Any], now: datetime | None = None) -> str:
    """Action row. action_id defaults to sha16(tick_id|action_signature); params/previous_value -> JSON."""
    a = dict(action)
    for k in ("params", "previous_value"):
        if isinstance(a.get(k), (dict, list)):
            a[k] = json.dumps(a[k], sort_keys=True, default=str)
    row = _with_defaults(a, {
        "action_id": _sha16(a.get("tick_id"), a.get("action_signature")),
        "status": "planned", "reversible": True, "created_at": _now(now),
    })
    return _idempotent_insert(ops, "actions", row)


def insert_approval(ops: str, approval: Mapping[str, Any], now: datetime | None = None) -> str:
    """Approval request. approval_id defaults to sha16(tick_id|action_signature); status pending."""
    a = dict(approval)
    if isinstance(a.get("action_json"), (dict, list)):
        a["action_json"] = json.dumps(a["action_json"], sort_keys=True, default=str)
    row = _with_defaults(a, {
        "approval_id": _sha16(a.get("tick_id"), a.get("action_signature")),
        "status": "pending", "requested_at": _now(now), "created_at": _now(now),
    })
    return _idempotent_insert(ops, "approvals", row)


# --- approvals lifecycle -----------------------------------------------------------
# pending -> approved | rejected | expired ; approved -> executing -> executed | failed | approved ; open rows expire.
# Every builder is a plain UPDATE guarded by the current status, so a retried statement is a no-op and
# a terminal row (executed / failed / rejected / expired) is never rewritten. ``executing`` is the drain's claim:
# one drainer moves the row there (claim_approval_sql), reads it back, and only the holder of the claim acts.

APPROVAL_STATUSES: tuple[str, ...] = ("pending", "approved", "executing", "rejected", "executed", "failed", "expired")
_APPROVAL_OPEN = ("pending", "approved", "executing")
APPROVAL_CLAIM_TAG = "claim:"
# Column order of the SELECT builders below (the actuator reads rows positionally).
PENDING_APPROVAL_COLUMNS: tuple[str, ...] = ("approval_id", "action_signature", "action_json", "requested_at", "expires_at", "note")
APPROVED_APPROVAL_COLUMNS: tuple[str, ...] = ("approval_id", "action_signature", "action_json", "approved_by", "expires_at")


def _note_append_expr(suffix: str) -> str:
    """SQL expression that appends ``suffix`` to approvals.note (``' | '``-separated, NULL-safe)."""
    return f"CASE WHEN note IS NULL THEN {lit(suffix)} ELSE CONCAT(note, {lit(' | ' + suffix)}) END"


def _decide_approval_sql(ops: str, approval_id: str, status: str, approved_by: str, note: str | None, now: datetime | None) -> str:
    if not approval_id:
        raise ValueError("approval_id is required")
    if not approved_by:
        raise ValueError("approved_by is required")
    sets = [f"status = {lit(status)}", f"approved_by = {lit(approved_by)}", f"approved_at = {lit(_now(now))}"]
    if note is not None:
        sets.append(f"note = {_note_append_expr(f'{status} by {approved_by}: {note}')}")
    return (
        f"UPDATE {ops}.approvals SET {', '.join(sets)}\n"
        f"WHERE approval_id = {lit(approval_id)} AND status = 'pending'"
    )


def approve_sql(ops: str, approval_id: str, approved_by: str, note: str | None = None, now: datetime | None = None) -> str:
    """pending -> approved. Records approved_by / approved_at; ``note`` (if given) is appended to the
    existing note so the quarantine reason written by the gate is kept."""
    return _decide_approval_sql(ops, approval_id, "approved", approved_by, note, now)


def reject_sql(ops: str, approval_id: str, approved_by: str, note: str | None = None, now: datetime | None = None) -> str:
    """pending -> rejected (approved_by / approved_at record who said no and when)."""
    return _decide_approval_sql(ops, approval_id, "rejected", approved_by, note, now)


def pending_approvals_sql(ops: str) -> str:
    """Pending rows, oldest first. Columns: PENDING_APPROVAL_COLUMNS."""
    return (
        f"SELECT {', '.join(PENDING_APPROVAL_COLUMNS)} FROM {ops}.approvals\n"
        f"WHERE status = 'pending' ORDER BY requested_at"
    )


def approved_unexecuted_sql(ops: str, now: datetime | None = None) -> str:
    """Approved rows the next tick must execute: status approved and not past expires_at.
    Columns: APPROVED_APPROVAL_COLUMNS."""
    return (
        f"SELECT {', '.join(APPROVED_APPROVAL_COLUMNS)} FROM {ops}.approvals\n"
        f"WHERE status = 'approved' AND (expires_at IS NULL OR expires_at > {lit(_now(now))})\n"
        f"ORDER BY approved_at"
    )


def claim_approval_sql(ops: str, approval_id: str, claim: str, now: datetime | None = None) -> str:
    """approved -> executing, stamped with the drainer's claim. The WHERE keeps it to rows still ``approved``, so of
    two drainers that reach the same row only one UPDATE matches; each then reads the row back (approval_claim_sql)
    and acts only if the claim it finds is its own. A row left ``executing`` by a drainer that died is never picked
    up again (approved_unexecuted_sql reads ``approved`` only); it expires with the other open rows."""
    if not approval_id or not claim:
        raise ValueError("approval_id and claim are required")
    ts = _now(now)
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    suffix = f"executing {ts.strftime('%Y-%m-%dT%H:%M:%SZ')} {APPROVAL_CLAIM_TAG}{claim}"
    return (
        f"UPDATE {ops}.approvals SET status = 'executing', note = {_note_append_expr(suffix)}\n"
        f"WHERE approval_id = {lit(approval_id)} AND status = 'approved'"
    )


def approval_claim_sql(ops: str, approval_id: str) -> str:
    """Read-back for claim_approval_sql. Columns: status, note."""
    return f"SELECT status, note FROM {ops}.approvals WHERE approval_id = {lit(approval_id)}"


# Provenance of a decision the gate writes while it PLANS an action (the tick that may open an approval for it), as
# opposed to ``DRAIN_PROVENANCE``: the re-check of a row a person already approved, in any mode. Only a planning
# decision can stand behind an approval.
OPENING_PROVENANCES: tuple[str, ...] = ("actuator", "heartbeat")
DRAIN_PROVENANCE = "drain"


def approval_binding_sql(ops: str, approval_id: str) -> str:
    """Every row carrying this approval id, with the gate decision it names. Columns: decision_id, action_signature,
    tick_id. The drain executes an approval only when this returns exactly one row (Delta enforces no key, so a copied
    id is possible) and that row names a decision."""
    return f"SELECT decision_id, action_signature, tick_id FROM {ops}.approvals WHERE approval_id = {lit(approval_id)}"


def quarantine_trace_sql(ops: str, decision_id: str, action_signature: str, tick_id: str, hours: int = 48,
                         now: datetime | None = None) -> str:
    """Is the decision this approval names the one the gate wrote when it opened it: a quarantine of this exact action
    (kind, target and params are in the signature), in the approval's own tick, written by the planning path, inside
    the trailing window? The gate quarantines the same action again every time it re-checks an approved row; those
    decisions carry ``DRAIN_PROVENANCE`` and another tick, and vouch for nothing. An approval lives 24 hours; 48
    leaves room for a late drain."""
    if hours <= 0:
        raise ValueError("hours must be positive")
    end = lit(now) if now is not None else "current_timestamp()"
    return (
        f"SELECT COUNT(*) AS quarantined FROM {ops}.policy_decisions\n"
        f"WHERE decision_id = {lit(decision_id)} AND action_signature = {lit(action_signature)} AND decision = 'quarantine'\n"
        f"  AND tick_id = {lit(tick_id)} AND provenance IN ({', '.join(lit(p) for p in OPENING_PROVENANCES)})\n"
        f"  AND created_at >= {end} - INTERVAL {int(hours)} HOURS"
    )


def decision_holders_sql(ops: str, decision_id: str) -> str:
    """How many approval rows name this decision. The gate opens one approval per quarantine decision, so more than
    one means a row was copied; none of them is executed."""
    return f"SELECT COUNT(*) AS holders FROM {ops}.approvals WHERE decision_id = {lit(decision_id)}"


def release_claim_sql(ops: str, approval_id: str, claim: str, now: datetime | None = None) -> str:
    """executing -> approved, only for the drainer whose claim is the last thing in the note (used when the claim
    could not be confirmed, so the approval is not left stuck in ``executing``)."""
    if not approval_id or not claim:
        raise ValueError("approval_id and claim are required")
    ts = _now(now)
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    suffix = f"claim released {ts.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    return (
        f"UPDATE {ops}.approvals SET status = 'approved', note = {_note_append_expr(suffix)}\n"
        f"WHERE approval_id = {lit(approval_id)} AND status = 'executing' AND note LIKE {lit('%' + APPROVAL_CLAIM_TAG + claim)}"
    )


def mark_approval_sql(ops: str, approval_id: str, status: str, note: str | None = None, now: datetime | None = None,
                      claim: str | None = None) -> str:
    """A drainer's write on an approval row: ``executed`` / ``failed`` / ``expired``, or ``approved`` as a note-only
    update (the row stays approved and is retried next tick). The appended note carries the status and the tick clock,
    since the table has no executed_at column.

    Which rows it may touch depends on whether the drainer holds the row:

    * without ``claim`` (every write made BEFORE a claim: a denial, a row that is not the gate's own) it matches
      ``approved`` rows only. It never matches ``executing``: a drainer that read the row before another one claimed
      it would otherwise put that row back to ``approved`` while its action is being applied, and it would be
      claimed and applied a second time. It never matches ``pending`` either: a drainer only ever reads approved rows;
    * with ``claim`` (every write made AFTER this drainer's claim: the outcome, or handing a planned-only action back)
      it matches the ``executing`` row whose note ends with this drainer's own claim, and nothing else."""
    if not approval_id:
        raise ValueError("approval_id is required")
    if status not in ("approved", "executed", "failed", "expired"):
        raise ValueError(f"mark_approval_sql: status must be approved/executed/failed/expired, got {status!r}")
    ts = _now(now)
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    suffix = f"{status} {ts.strftime('%Y-%m-%dT%H:%M:%SZ')}" + (f": {note}" if note else "")
    if claim:
        held = f"status = 'executing' AND note LIKE {lit('%' + APPROVAL_CLAIM_TAG + claim)}"
    else:
        held = "status = 'approved'"
    return (
        f"UPDATE {ops}.approvals SET status = {lit(status)}, note = {_note_append_expr(suffix)}\n"
        f"WHERE approval_id = {lit(approval_id)} AND {held}"
    )


def expire_approvals_sql(ops: str, now: datetime | None = None) -> str:
    """Pending and approved rows past expires_at -> expired. A row left ``executing`` (a drainer that died after its
    claim) expires one hour later than that, so a sweep by another drainer never expires a row whose action is being
    applied at that moment. Rows without an expires_at never expire."""
    end = lit(_now(now))
    return (
        f"UPDATE {ops}.approvals SET status = 'expired'\n"
        f"WHERE expires_at IS NOT NULL AND ((status IN ('pending', 'approved') AND expires_at <= {end})\n"
        f"   OR (status = 'executing' AND expires_at <= {end} - INTERVAL 1 HOURS))"
    )


def _merge(ops: str, table: str, row: Mapping[str, Any], key: str, update_set: Mapping[str, str]) -> str:
    pairs = _row_for(table, row)
    src = ", ".join(f"{lit(v)} AS {c}" for c, v in pairs)
    cols = ", ".join(c for c, _ in pairs)
    ins = ", ".join(f"s.{c}" for c, _ in pairs)
    upd = ",\n  ".join(f"{c} = {expr}" for c, expr in update_set.items())
    return (
        f"MERGE INTO {ops}.{table} AS t\n"
        f"USING (SELECT {src}) AS s\n"
        f"ON t.{key} = s.{key}\n"
        f"WHEN MATCHED THEN UPDATE SET\n  {upd}\n"
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({ins})"
    )


def upsert_incident(ops: str, incident: Mapping[str, Any], now: datetime | None = None) -> str:
    """MERGE on incident_key (= fingerprint_key + pipeline); matched rows bump occurrences and last_seen.

    Diagnosis fields (root_cause, fix, runbook_id, verdict, approver) are only overwritten when the
    incoming row carries a non-null value, so a re-sighting never erases an approved diagnosis.
    """
    ts = _now(now)
    inc = dict(incident)
    if not inc.get("fingerprint_key") or not inc.get("pipeline"):
        raise ValueError("incidents need fingerprint_key and pipeline")
    row = _with_defaults(inc, {
        "incident_key": incident_key(inc["fingerprint_key"], inc["pipeline"]),
        "occurrences": 1, "first_seen": ts, "last_seen": ts, "status": "open",
        "provenance": "log_derived", "created_at": ts, "updated_at": ts,
    })
    keep_if_null = ("root_cause", "fix", "runbook_id", "verdict", "approver", "status")
    update_set = {
        "occurrences": "t.occurrences + 1",
        "last_seen": "s.last_seen",
        "last_run_key": "s.last_run_key",
        "label": "s.label",
        "confidence": "s.confidence",
        "sample_error": "s.sample_error",
        **{c: f"COALESCE(s.{c}, t.{c})" for c in keep_if_null},
        "updated_at": "s.updated_at",
    }
    return _merge(ops, "incidents", row, "incident_key", update_set)


def upsert_runbook(ops: str, runbook: Mapping[str, Any], now: datetime | None = None) -> str:
    """MERGE on runbook_id; counters (successes/failures) are kept, content/approval refreshed."""
    ts = _now(now)
    rb = dict(runbook)
    if not rb.get("runbook_id"):
        raise ValueError("runbooks need runbook_id")
    row = _with_defaults(rb, {
        "successes": 0, "failures": 0, "enabled": True, "provenance": "approved_incident",
        "created_at": ts, "updated_at": ts,
    })
    update_set = {
        "title": "s.title", "procedure": "s.procedure", "label": "s.label",
        "fingerprint_key": "s.fingerprint_key", "source_incident_key": "COALESCE(s.source_incident_key, t.source_incident_key)",
        "approved_by": "COALESCE(s.approved_by, t.approved_by)",
        "approved_at": "COALESCE(s.approved_at, t.approved_at)",
        "successes": "t.successes + s.successes", "failures": "t.failures + s.failures",
        "enabled": "s.enabled", "provenance": "s.provenance", "updated_at": "s.updated_at",
    }
    return _merge(ops, "runbooks", row, "runbook_id", update_set)


def insert_flight_rows(ops: str, rows: Iterable[Mapping[str, Any]], now: datetime | None = None) -> str:
    """One INSERT with a batched VALUES list. Columns follow recorder.FLIGHT_COLUMNS; record_id defaults to sha16(run_id|kind|entity_id)."""
    ts = _now(now)
    rendered = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("details_json"), (dict, list)):
            d["details_json"] = json.dumps(d["details_json"], sort_keys=True, default=str)
        d = _with_defaults(d, {
            "record_id": _sha16(d.get("run_id"), d.get("kind"), d.get("entity_id")),
            "recorded_at": ts, "created_at": ts,
        })
        pairs = _row_for("flight_recorder", d)
        rendered.append("(" + ", ".join(lit(v) for _, v in pairs) + ")")
    if not rendered:
        raise ValueError("insert_flight_rows: no rows")
    cols = ", ".join(c for c, _ in COLUMNS["flight_recorder"])
    return f"INSERT INTO {ops}.flight_recorder ({cols}) VALUES\n" + ",\n".join(rendered)


def _ts_literal(iso: str) -> str:
    dt = datetime.fromisoformat(iso)
    return lit(dt)


def budget_summary_sql(ops: str, month_start_iso: str) -> str:
    """Inputs for the tier computation since month start: used OCPU hours / cost, LLM calls / cost,
    latest ledger entry (for budget_source freshness). The remaining pct is computed in tiers.py."""
    ts = _ts_literal(month_start_iso)
    return (
        "SELECT\n"
        f"  (SELECT COALESCE(SUM(ocpu_hours), 0.0) FROM {ops}.budget_ledger WHERE created_at >= {ts}) AS used_ocpu_hours,\n"
        f"  (SELECT COALESCE(SUM(est_cost), 0.0) FROM {ops}.budget_ledger WHERE created_at >= {ts}) AS used_cost,\n"
        f"  (SELECT COALESCE(SUM(calls), 0) FROM {ops}.llm_ledger WHERE created_at >= {ts}) AS llm_calls,\n"
        f"  (SELECT COALESCE(SUM(est_cost), 0.0) FROM {ops}.llm_ledger WHERE created_at >= {ts}) AS llm_cost,\n"
        f"  (SELECT MAX(created_at) FROM {ops}.budget_ledger) AS last_budget_entry_at,\n"
        f"  (SELECT MAX(created_at) FROM {ops}.llm_ledger) AS last_llm_entry_at"
    )


def rate_count_sql(ops: str, rule_id: str, target: str | None, window_hours: int,
                   now: datetime | None = None) -> str:
    """Count the actions actually APPLIED in the trailing window whose allow decision ran this rule (and target).
    The quota is spent by changes, not by evaluations: a dry-run tick writes the same allow decision and an action row
    with status ``dry_run``, and neither counts. The rule is looked up in ``rules_evaluated``, the JSON list every
    decision row records, because an allow never carries the rate rule in ``rule_id`` (that column is NULL for a plain
    allow and the lifted quarantine rule for an approved one). Callers pass ``now`` (the tick's UTC clock): the ledger
    writes UTC wall-clock literals, and comparing them with ``current_timestamp()`` would shift the window by the
    session's time-zone offset."""
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    end = lit(now) if now is not None else "current_timestamp()"
    sql = (
        f"SELECT COUNT(*) AS n FROM {ops}.actions a JOIN {ops}.policy_decisions d ON d.decision_id = a.decision_id\n"
        f"WHERE a.status = 'applied' AND d.decision = 'allow' AND instr(d.rules_evaluated, {lit(json.dumps(str(rule_id)))}) > 0\n"
        f"  AND a.applied_at >= {end} - INTERVAL {int(window_hours)} HOURS"
    )
    if target is not None:
        sql += f"\n  AND d.target = {lit(target)}"
    return sql

if __name__ == "__main__":  # python -m nightshift.ledger [catalog.schema]
    import sys

    print("written:", write_ddl_file(sys.argv[1] if len(sys.argv) > 1 else OPS_DEFAULT))

