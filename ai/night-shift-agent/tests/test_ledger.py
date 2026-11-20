"""Tests for nightshift.ledger. Local only; duckdb stands in for Spark where syntax allows.

DuckDB translation used by the smoke test (helpers._to_duckdb; the ops_db fixture lives in conftest.py):
  * drop ``USING DELTA``
  * drop ``PARTITIONED BY (...)`` (Delta physical layout, no duckdb equivalent)
  * drop ``TBLPROPERTIES (...)`` (delta.appendOnly is a Delta table property)
  * map ``STRING`` -> ``VARCHAR`` (TIMESTAMP / INT / BIGINT / DOUBLE / BOOLEAN exist in both)
  * ``current_timestamp()`` -> ``current_timestamp`` (duckdb takes it without parentheses)
  * the ops schema is single-level (``nightshift_ops``) because duckdb has no ``default`` catalog;
    live Spark uses ``default.nightshift_ops``.
Everything else (INSERT ... SELECT ... WHERE NOT EXISTS, UPDATE, MERGE INTO, INTERVAL n HOURS,
TIMESTAMP '...' literals) runs unchanged on both engines. Backslash doubling inside string literals is
Spark semantics; duckdb keeps both backslashes, which the smoke test tolerates.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from nightshift import ledger as L

from helpers import _to_duckdb

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TABLES = [
    "wake_events", "ticks", "budget_ledger", "llm_ledger", "policy_rules", "policy_decisions", "approvals",
    "incidents", "runbooks", "actions", "flight_recorder", "charter_history", "agent_registry", "replay_verdicts", "repair_drafts",
]
APPEND_ONLY = {"policy_decisions", "actions", "ticks", "flight_recorder", "charter_history", "replay_verdicts"}
NOW = datetime(2026, 11, 1, 3, 0, 0)


# --- DDL -----------------------------------------------------------------------------------

def test_ddl_statement_count_and_order():
    stmts = L.ddl_statements("default.nightshift_ops")
    assert len(stmts) == 1 + 15
    assert stmts[0] == "CREATE SCHEMA IF NOT EXISTS default.nightshift_ops"
    tables = [re.match(r"CREATE TABLE IF NOT EXISTS default\.nightshift_ops\.(\w+)", s).group(1) for s in stmts[1:]]
    assert sorted(tables) == sorted(EXPECTED_TABLES)
    assert tables == list(L.TABLE_ORDER)
    # dependency order: things referenced by tick_id / decision_id / incident_key come first
    assert tables.index("ticks") < tables.index("policy_decisions") < tables.index("actions")
    assert tables.index("policy_decisions") < tables.index("approvals")
    assert tables.index("incidents") < tables.index("runbooks")
    assert tables.index("policy_rules") < tables.index("policy_decisions")
    assert "{ops}" not in "".join(stmts)


def test_ddl_dict_has_placeholder_and_delta():
    assert set(L.DDL) == set(EXPECTED_TABLES)
    for table, sql in L.DDL.items():
        assert sql.startswith(f"CREATE TABLE IF NOT EXISTS {{ops}}.{table} (")
        assert "USING DELTA" in sql


def test_append_only_properties_on_the_right_tables():
    for table, sql in L.DDL.items():
        has_prop = "TBLPROPERTIES ('delta.appendOnly' = 'true')" in sql
        assert has_prop == (table in APPEND_ONLY), table
    assert L.APPEND_ONLY == APPEND_ONLY


def test_partitioning_and_required_columns():
    for table, cols in L.COLUMNS.items():
        names = [c for c, _ in cols]
        assert "created_at" in names, table
        assert dict(cols)["created_at"] == "TIMESTAMP"
        assert L.IDEMPOTENCY_KEY[table] in names, table
        assert len(names) == len(set(names)), table
    assert "PARTITIONED BY (window_day)" in L.DDL["budget_ledger"]
    assert "PARTITIONED BY (window_day)" in L.DDL["llm_ledger"]
    assert "PARTITIONED BY" not in L.DDL["ticks"]
    assert ("incident_key", "STRING") in L.COLUMNS["incidents"]
    assert ("consumed_by_tick", "STRING") in L.COLUMNS["wake_events"]
    assert ("action_signature", "STRING") in L.COLUMNS["actions"]
    assert ("action_signature", "STRING") in L.COLUMNS["policy_decisions"]


def test_sql_file_matches_generated_text():
    path = ROOT / "sql" / "ddl_ops.sql"
    on_disk = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert on_disk == L.render_ddl_file("default.nightshift_ops"), (
        "sql/ddl_ops.sql drifted; regenerate from nightshift.ledger.render_ddl_file()")
    assert "default.nightshift_ops.flight_recorder" in on_disk
    assert on_disk.count("CREATE TABLE IF NOT EXISTS") == 15


# --- lit() ---------------------------------------------------------------------------------

def test_lit_escaping_and_types():
    assert L.lit(None) == "NULL"
    assert L.lit(True) == "TRUE" and L.lit(False) == "FALSE"
    assert L.lit(42) == "42"
    assert L.lit(1.5) == "CAST(1.5 AS DOUBLE)"
    assert L.lit(float("nan")) == "NULL"
    assert L.lit("it's") == "'it\\'s'"          # Spark: \' is the quote escape; '' would read back as "its"
    assert L.lit("a\\b") == "'a\\\\b'"
    assert L.lit(datetime(2026, 11, 1, 3, 4, 5)) == "TIMESTAMP '2026-11-01 03:04:05.000000'"
    assert L.lit(datetime(2026, 11, 1, 3, 4, 5, tzinfo=timezone.utc)) == "TIMESTAMP '2026-11-01 03:04:05.000000'"
    assert L.lit(date(2026, 11, 1)) == "DATE '2026-11-01'"
    d = L.lit({"b": 1, "a": "x'y"})
    assert d.startswith("'") and d.endswith("'")
    assert json.loads(d[1:-1].replace("\\'", "'")) == {"a": "x'y", "b": 1}
    assert d.index('"a"') < d.index('"b"')  # sorted keys
    assert L.lit(["x", 1]) == "'[\"x\", 1]'"


# --- builders ------------------------------------------------------------------------------

def test_insert_wake_event_idempotent_shape():
    sql = L.insert_wake_event("o.s", {"source": "jobs", "reason": "FAILED", "pipeline": "p", "payload": {"k": 1}}, NOW)
    assert sql.startswith("INSERT INTO o.s.wake_events (event_id, source, reason, pipeline, payload_hash, payload,")
    assert "WHERE NOT EXISTS (SELECT 1 FROM o.s.wake_events WHERE event_id = '" in sql
    assert "'{\"k\": 1}'" in sql
    # same event -> same text (same event_id)
    assert sql == L.insert_wake_event("o.s", {"source": "jobs", "reason": "FAILED", "pipeline": "p", "payload": {"k": 1}}, NOW)


def test_consume_wake_events_sql_shape():
    sql = L.consume_wake_events_sql("o.s", "tick-1", NOW)
    assert sql.startswith("UPDATE o.s.wake_events SET consumed_by_tick = 'tick-1'")
    assert sql.rstrip().endswith("WHERE consumed_by_tick IS NULL")
    assert sql.count("WHERE") == 1
    with pytest.raises(ValueError):
        L.consume_wake_events_sql("o.s", "")


def test_insert_builders_require_keys_and_reject_unknown_columns():
    with pytest.raises(ValueError):
        L.insert_tick("o.s", {"compute_tier": "normal"}, NOW)  # no tick_id
    with pytest.raises(ValueError):
        L.insert_tick("o.s", {"tick_id": "t", "bogus": 1}, NOW)
    with pytest.raises(ValueError):
        L.upsert_incident("o.s", {"pipeline": "p"}, NOW)
    with pytest.raises(ValueError):
        L.insert_flight_rows("o.s", [], NOW)


def test_decision_and_action_json_fields():
    sql = L.insert_decision("o.s", {"tick_id": "t", "action_signature": "s", "decision": "deny", "rule_id": "r1",
                                    "rules_evaluated": ["r0", "r1"], "reason": "no"}, NOW)
    assert "'[\"r0\", \"r1\"]'" in sql
    assert "WHERE NOT EXISTS (SELECT 1 FROM o.s.policy_decisions WHERE decision_id = '" in sql
    sql = L.insert_action("o.s", {"tick_id": "t", "action_signature": "s", "kind": "job_run", "target": "j",
                                  "params": {"x": 1}, "previous_value": None}, NOW)
    assert "'{\"x\": 1}'" in sql and "'planned'" in sql and "TRUE" in sql
    sql = L.insert_approval("o.s", {"tick_id": "t", "action_signature": "s", "action_json": {"kind": "job_repair"}}, NOW)
    assert "'pending'" in sql and "o.s.approvals" in sql


def test_upsert_incident_merge_text():
    sql = L.upsert_incident("o.s", {"fingerprint_key": "abcd", "pipeline": "sales", "label": "TIMEOUT",
                                    "confidence": 0.9, "sample_error": "x"}, NOW)
    assert sql.startswith("MERGE INTO o.s.incidents AS t")
    assert "ON t.incident_key = s.incident_key" in sql
    assert "occurrences = t.occurrences + 1" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql and "WHEN NOT MATCHED THEN INSERT" in sql
    assert f"'{L.incident_key('abcd', 'sales')}' AS incident_key" in sql
    assert "root_cause = COALESCE(s.root_cause, t.root_cause)" in sql  # re-sighting never erases a diagnosis
    assert L.incident_key("abcd", "sales") != L.incident_key("abcd", "returns")


def test_upsert_runbook_and_rate_and_budget_sql():
    sql = L.upsert_runbook("o.s", {"runbook_id": "rb", "title": "t", "procedure": "p", "label": "SKEW"}, NOW)
    assert "ON t.runbook_id = s.runbook_id" in sql
    sql = L.rate_count_sql("o.s", "rl.cluster_resize", "cluster-1", 24)
    assert "decision = 'allow'" in sql and "INTERVAL 24 HOURS" in sql and "target = 'cluster-1'" in sql
    assert "a.status = 'applied'" in sql and "JOIN o.s.policy_decisions d ON d.decision_id = a.decision_id" in sql
    assert "current_timestamp()" in sql
    assert "current_timestamp()" not in L.rate_count_sql("o.s", "rl.cluster_resize", "cluster-1", 24, NOW)   # the tick's own clock
    assert "target" not in L.rate_count_sql("o.s", "r", None, 1)
    with pytest.raises(ValueError):
        L.rate_count_sql("o.s", "r", None, 0)
    sql = L.budget_summary_sql("o.s", "2026-11-01")
    assert "TIMESTAMP '2026-11-01 00:00:00.000000'" in sql
    for alias in ("used_ocpu_hours", "used_cost", "llm_calls", "llm_cost", "last_budget_entry_at"):
        assert alias in sql


def test_insert_flight_rows_batched_values():
    sql = L.insert_flight_rows("o.s", [{"run_id": "r", "kind": "stage", "entity_id": f"{i}.0"} for i in range(3)], NOW)
    assert sql.startswith("INSERT INTO o.s.flight_recorder (record_id, recorded_at, job_name, run_id,")
    assert sql.count("\n(") == 3 and sql.count("VALUES") == 1


# --- duckdb smoke test (translation documented in the module docstring) ------------------------

def test_duckdb_smoke(ops_db):
    ops, con = ops_db
    tables = {r[0] for r in con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{ops}'").fetchall()}
    assert tables == set(EXPECTED_TABLES)

    ev = {"source": "jobs", "reason": "FAILED", "pipeline": "sales_hourly", "payload": {"run": "r1", "note": "it's"}}
    con.execute(L.insert_wake_event(ops, ev, NOW))
    con.execute(L.insert_wake_event(ops, ev, NOW))  # retry -> no duplicate
    assert con.execute(f"SELECT COUNT(*) FROM {ops}.wake_events").fetchone()[0] == 1
    con.execute(L.consume_wake_events_sql(ops, "t1", NOW))
    con.execute(L.consume_wake_events_sql(ops, "t2", NOW))  # already claimed: stays t1
    assert con.execute(f"SELECT consumed_by_tick FROM {ops}.wake_events").fetchone()[0] == "t1"

    con.execute(L.insert_tick(ops, {"tick_id": "t1", "compute_tier": "normal", "llm_tier": "normal",
                                    "remaining_pct": 55.0, "budget_source": "ledger", "actions_proposed": 1}, NOW))
    con.execute(L.insert_tick(ops, {"tick_id": "t1", "compute_tier": "normal"}, NOW))
    assert con.execute(f"SELECT COUNT(*) FROM {ops}.ticks").fetchone()[0] == 1

    # an allow carries no rule_id (the gate records None for a plain allow); the rate rule is in rules_evaluated
    dec = {"tick_id": "t1", "action_signature": "sig1", "action_kind": "job_run", "target": "job1",
           "decision": "allow", "rule_id": None, "rules_evaluated": ["deny.gold", "rl.repairs_per_job"],
           "reason": "ok", "latency_ms": 1.5}
    con.execute(L.insert_decision(ops, dec, NOW))
    con.execute(L.insert_decision(ops, {**dec, "decision": "deny", "rule_id": "rl.repairs_per_job", "reason": "over quota"}, NOW))
    con.execute(L.insert_decision(ops, {**dec, "tick_id": "t2", "rules_evaluated": ["deny.gold", "rl.repairs_per_job_weekly"]}, NOW))
    con.execute(L.insert_action(ops, {"tick_id": "t1", "action_signature": "sig1", "kind": "job_run",
                                      "target": "job1", "params": {"a": 1}, "previous_value": {"a": 0},
                                      "decision_id": L._sha16("t1", "sig1", "allow", None), "status": "applied", "applied_at": NOW}, NOW))
    # a dry-run action behind the other allow decision: recorded, never counted
    con.execute(L.insert_action(ops, {"tick_id": "t2", "action_signature": "sig1", "kind": "job_run", "target": "job1",
                                      "decision_id": L._sha16("t2", "sig1", "allow", None), "status": "dry_run"}, NOW))
    con.execute(L.insert_approval(ops, {"tick_id": "t1", "action_signature": "sig2",
                                        "action_json": {"kind": "job_repair", "target": "job2"}}, NOW))
    assert con.execute(f"SELECT status FROM {ops}.approvals").fetchone()[0] == "pending"

    inc = {"fingerprint_key": "abcd1234abcd1234", "pipeline": "sales_hourly", "label": "SCHEMA_MISMATCH",
           "confidence": 0.9, "sample_error": "cannot resolve 'x'", "last_run_key": "r1"}
    con.execute(L.upsert_incident(ops, inc, NOW))
    con.execute(L.upsert_incident(ops, {**inc, "last_run_key": "r2", "root_cause": "upstream rename"}, NOW))
    con.execute(L.upsert_incident(ops, {**inc, "last_run_key": "r3"}, NOW))  # root_cause must survive
    row = con.execute(f"SELECT occurrences, last_run_key, root_cause FROM {ops}.incidents").fetchone()
    assert row == (3, "r3", "upstream rename")

    con.execute(L.upsert_runbook(ops, {"runbook_id": "rb1", "title": "map renamed columns", "procedure": "...",
                                       "label": "SCHEMA_MISMATCH", "fingerprint_key": inc["fingerprint_key"]}, NOW))
    con.execute(L.upsert_runbook(ops, {"runbook_id": "rb1", "title": "map renamed columns v2", "procedure": "...",
                                       "label": "SCHEMA_MISMATCH", "successes": 1}, NOW))
    assert con.execute(f"SELECT title, successes FROM {ops}.runbooks").fetchone() == ("map renamed columns v2", 1)

    con.execute(L.insert_flight_rows(ops, [
        {"run_id": "r1", "kind": "stage", "entity_id": "7.0", "num_tasks": 200, "spill_bytes": 15_000_000_000,
         "duration_ms": 2_280_000},
        {"run_id": "r1", "kind": "sql", "entity_id": "3", "details_json": {"plan": "Exchange hashpartitioning"}},
    ], NOW))
    assert con.execute(f"SELECT COUNT(*) FROM {ops}.flight_recorder").fetchone()[0] == 2

    budget = con.execute(_to_duckdb(L.budget_summary_sql(ops, "2026-11-01"))).fetchone()
    assert budget[:4] == (0.0, 0.0, 0, 0.0)
    # only the applied action behind an allow that ran this rule counts toward the quota
    assert con.execute(_to_duckdb(L.rate_count_sql(ops, "rl.repairs_per_job", "job1", 24, NOW))).fetchone()[0] == 1
    # pass now= explicitly: the fixture rows are stamped NOW, so wall-clock windows expire a day later
    assert con.execute(_to_duckdb(L.rate_count_sql(ops, "rl.repairs_per_job", None, 24, NOW))).fetchone()[0] == 1


# --- approvals lifecycle ----------------------------------------------------------------------------

LATER = datetime(2026, 11, 2, 7, 0, 0)


def test_approve_and_reject_sql_shapes():
    sql = L.approve_sql("o.s", "ap1", "reviewer", note="profile=DEFAULT; ok at 07:00", now=LATER)
    assert sql.startswith("UPDATE o.s.approvals SET status = 'approved', approved_by = 'reviewer', approved_at = TIMESTAMP '2026-11-02 07:00:00.000000'")
    assert "WHERE approval_id = 'ap1' AND status = 'pending'" in sql
    assert "CASE WHEN note IS NULL THEN 'approved by reviewer: profile=DEFAULT; ok at 07:00'" in sql
    assert "CONCAT(note, ' | approved by reviewer: profile=DEFAULT; ok at 07:00')" in sql
    no_note = L.approve_sql("o.s", "ap1", "reviewer", now=LATER)
    assert "note" not in no_note.split("WHERE")[0]            # note untouched when none is given
    rej = L.reject_sql("o.s", "ap1", "reviewer", note="it's fine", now=LATER)
    assert "SET status = 'rejected'" in rej and "rejected by reviewer: it\\'s fine" in rej
    for bad in ({"approval_id": ""}, {"approved_by": ""}):
        with pytest.raises(ValueError):
            L.approve_sql("o.s", bad.get("approval_id", "ap1"), bad.get("approved_by", "x"))


def test_pending_and_approved_select_shapes():
    sql = L.pending_approvals_sql("o.s")
    assert sql.startswith("SELECT approval_id, action_signature, action_json, requested_at, expires_at, note FROM o.s.approvals")
    assert "WHERE status = 'pending' ORDER BY requested_at" in sql
    sql = L.approved_unexecuted_sql("o.s", LATER)
    assert sql.startswith("SELECT approval_id, action_signature, action_json, approved_by, expires_at FROM o.s.approvals")
    assert "status = 'approved' AND (expires_at IS NULL OR expires_at > TIMESTAMP '2026-11-02 07:00:00.000000')" in sql
    assert L.PENDING_APPROVAL_COLUMNS[:3] == L.APPROVED_APPROVAL_COLUMNS[:3]


def test_mark_and_expire_sql_shapes():
    sql = L.mark_approval_sql("o.s", "ap1", "executed", note="dry_run by tick abc", now=LATER)
    assert sql.startswith("UPDATE o.s.approvals SET status = 'executed', note = CASE WHEN note IS NULL THEN 'executed 2026-11-02T07:00:00Z: dry_run by tick abc'")
    assert sql.endswith("WHERE approval_id = 'ap1' AND status = 'approved'")           # before a claim: approved rows only
    held = L.mark_approval_sql("o.s", "ap1", "executed", note="applied", now=LATER, claim="abcd1234")
    assert held.endswith("WHERE approval_id = 'ap1' AND status = 'executing' AND note LIKE '%claim:abcd1234'")   # after it: the holder's row only
    for status in ("approved", "failed", "expired"):
        assert "'executing'" not in L.mark_approval_sql("o.s", "ap1", status, note="x", now=LATER)   # never without the claim
    assert "'failed 2026-11-02T07:00:00Z: boom'" in L.mark_approval_sql("o.s", "ap1", "failed", note="boom", now=LATER)
    assert "'expired 2026-11-02T07:00:00Z'" in L.mark_approval_sql("o.s", "ap1", "expired", now=LATER)
    assert "SET status = 'approved'" in L.mark_approval_sql("o.s", "ap1", "approved", note="DENIED_ON_EXECUTE", now=LATER)
    with pytest.raises(ValueError):
        L.mark_approval_sql("o.s", "ap1", "rejected")
    with pytest.raises(ValueError):
        L.mark_approval_sql("o.s", "", "executed")
    sql = L.expire_approvals_sql("o.s", LATER)
    assert sql == ("UPDATE o.s.approvals SET status = 'expired'\n"
                   "WHERE expires_at IS NOT NULL AND ((status IN ('pending', 'approved') AND expires_at <= TIMESTAMP '2026-11-02 07:00:00.000000')\n"
                   "   OR (status = 'executing' AND expires_at <= TIMESTAMP '2026-11-02 07:00:00.000000' - INTERVAL 1 HOURS))")


def test_duckdb_approval_lifecycle(ops_db):
    """pending -> approved -> executed; a second pending row expires; terminal rows are never rewritten."""
    ops, con = ops_db
    body = {"kind": "cluster_update", "target": "c1", "params": {"max_workers": 3}}
    for sig, expires in (("sigA", datetime(2026, 11, 3, 3, 0)), ("sigB", datetime(2026, 11, 2, 5, 0)), ("sigC", None)):
        con.execute(L.insert_approval(ops, {"tick_id": "t1", "action_signature": sig, "action_json": body,
                                            "expires_at": expires, "note": "budget.big_resize: BIG_RESIZE"}, NOW))
    ids = dict(con.execute(f"SELECT action_signature, approval_id FROM {ops}.approvals").fetchall())

    con.execute(L.approve_sql(ops, ids["sigA"], "reviewer", note="profile=DEFAULT", now=LATER))
    con.execute(L.approve_sql(ops, ids["sigA"], "mallory", note="again", now=LATER))  # not pending any more: no-op
    row = con.execute(f"SELECT status, approved_by, note FROM {ops}.approvals WHERE approval_id = '{ids['sigA']}'").fetchone()
    assert row == ("approved", "reviewer", "budget.big_resize: BIG_RESIZE | approved by reviewer: profile=DEFAULT")

    con.execute(L.expire_approvals_sql(ops, LATER))  # 07:00 > sigB's 05:00 expiry; sigA (tomorrow) and sigC (no expiry) stay
    status = dict(con.execute(f"SELECT action_signature, status FROM {ops}.approvals").fetchall())
    assert status == {"sigA": "approved", "sigB": "expired", "sigC": "pending"}

    drained = con.execute(L.approved_unexecuted_sql(ops, LATER)).fetchall()
    assert [r[0] for r in drained] == [ids["sigA"]] and drained[0][3] == "reviewer"
    pending = con.execute(L.pending_approvals_sql(ops)).fetchall()
    assert [r[1] for r in pending] == ["sigC"] and len(pending[0]) == len(L.PENDING_APPROVAL_COLUMNS)

    con.execute(L.mark_approval_sql(ops, ids["sigA"], "executed", note="dry_run by tick t2", now=LATER))
    con.execute(L.mark_approval_sql(ops, ids["sigA"], "failed", note="late", now=LATER))  # terminal: no-op
    row = con.execute(f"SELECT status, note FROM {ops}.approvals WHERE approval_id = '{ids['sigA']}'").fetchone()
    assert row[0] == "executed" and row[1].endswith(" | executed 2026-11-02T07:00:00Z: dry_run by tick t2")
    assert con.execute(L.approved_unexecuted_sql(ops, LATER)).fetchall() == []

    con.execute(L.reject_sql(ops, ids["sigC"], "reviewer", now=LATER))
    assert con.execute(f"SELECT status, approved_by FROM {ops}.approvals WHERE approval_id = '{ids['sigC']}'").fetchone() == ("rejected", "reviewer")
    assert con.execute(L.pending_approvals_sql(ops)).fetchall() == []


def test_approval_binding_trace_and_claim_against_the_ledger(ops_db):
    """The reads the approval drain depends on, against real tables: an approval is bound to the decision row it
    names, that decision vouches for it only as a quarantine of the same action inside the window, a copied row shows
    up as a second holder of the decision, and of two claims on one approved row only the first takes."""
    ops, con = ops_db
    base = {"tick_id": "t1", "action_signature": "sig1", "action_kind": "cluster_update", "target": "c1",
            "rules_evaluated": ["budget.big_resize"], "reason": "BIG_RESIZE", "provenance": "actuator"}
    con.execute(L.insert_decision(ops, {**base, "decision": "quarantine", "rule_id": "budget.big_resize"}, NOW))
    con.execute(L.insert_decision(ops, {**base, "tick_id": "t2", "action_signature": "sig2", "decision": "allow", "rule_id": None}, NOW))
    did = L._sha16("t1", "sig1", "quarantine", "budget.big_resize")
    did_allow = L._sha16("t2", "sig2", "allow", None)

    # the gate quarantines the same action again when it re-checks the approved row on a later tick
    con.execute(L.insert_decision(ops, {**base, "tick_id": "t3", "decision": "quarantine", "rule_id": "budget.big_resize",
                                        "provenance": L.DRAIN_PROVENANCE}, NOW))
    did_recheck = L._sha16("t3", "sig1", "quarantine", "budget.big_resize")

    def traced(decision_id, sig, tick, now=LATER):
        return con.execute(_to_duckdb(L.quarantine_trace_sql(ops, decision_id, sig, tick, now=now))).fetchone()[0]

    assert traced(did, "sig1", "t1") == 1
    assert traced(did, "sig-other", "t1") == 0                           # the decision is for another action
    assert traced(did, "sig1", "t9") == 0                                # the approval row is from another tick
    assert traced(did_recheck, "sig1", "t3") == 0                        # a re-check of an approved row opens nothing
    assert traced(did_allow, "sig2", "t2") == 0                          # an allow is not a quarantine
    assert traced("no-such-decision", "sig1", "t1") == 0
    assert traced(did, "sig1", "t1", datetime(2026, 11, 20, tzinfo=timezone.utc)) == 0   # weeks later it vouches for nothing
    with pytest.raises(ValueError):
        L.quarantine_trace_sql(ops, did, "sig1", "t1", hours=0)

    con.execute(L.insert_approval(ops, {"approval_id": "ap1", "tick_id": "t1", "action_signature": "sig1", "decision_id": did,
                                        "action_json": {"kind": "cluster_update", "target": "c1"}, "note": "budget.big_resize"}, NOW))
    assert con.execute(L.approval_binding_sql(ops, "ap1")).fetchall() == [(did, "sig1", "t1")]
    assert con.execute(L.approval_binding_sql(ops, "nope")).fetchall() == []
    assert con.execute(L.decision_holders_sql(ops, did)).fetchone()[0] == 1
    # a copy of the row under another id names the same decision: two holders
    con.execute(L.insert_approval(ops, {"approval_id": "ap-copy", "tick_id": "x", "action_signature": "sig1", "decision_id": did,
                                        "action_json": {"kind": "cluster_update", "target": "c1"}, "status": "approved"}, NOW))
    assert con.execute(L.decision_holders_sql(ops, did)).fetchone()[0] == 2
    con.execute(f"DELETE FROM {ops}.approvals WHERE approval_id = 'ap-copy'")

    con.execute(L.approve_sql(ops, "ap1", "reviewer", now=LATER))
    con.execute(L.claim_approval_sql(ops, "ap1", "aaaa", LATER))
    con.execute(L.claim_approval_sql(ops, "ap1", "bbbb", LATER))          # the row is no longer approved: no match
    status, note = con.execute(L.approval_claim_sql(ops, "ap1")).fetchone()
    assert status == "executing" and note.endswith("claim:aaaa") and "bbbb" not in note
    assert con.execute(_to_duckdb(L.approved_unexecuted_sql(ops, LATER))).fetchall() == []   # not picked up again
    con.execute(L.release_claim_sql(ops, "ap1", "bbbb", LATER))          # not the holder: nothing changes
    assert con.execute(f"SELECT status FROM {ops}.approvals").fetchone()[0] == "executing"
    con.execute(L.release_claim_sql(ops, "ap1", "aaaa", LATER))          # the holder hands it back
    status, note = con.execute(L.approval_claim_sql(ops, "ap1")).fetchone()
    assert status == "approved" and note.endswith("claim released 2026-11-02T07:00:00Z")
    con.execute(L.claim_approval_sql(ops, "ap1", "cccc", LATER))
    con.execute(L.mark_approval_sql(ops, "ap1", "executed", note="applied", now=LATER, claim="cccc"))
    assert con.execute(f"SELECT status FROM {ops}.approvals").fetchone()[0] == "executed"
    with pytest.raises(ValueError):
        L.claim_approval_sql(ops, "ap1", "")


def test_an_executing_row_expires_an_hour_after_the_others(ops_db):
    """A sweep must not expire a row whose action is being applied right now: pending and approved rows expire at
    expires_at, a row left executing only an hour after it."""
    ops, con = ops_db
    for aid, status in (("p", "pending"), ("a", "approved"), ("e", "executing")):
        con.execute(L.insert_approval(ops, {"approval_id": aid, "tick_id": "t", "action_signature": "s" + aid, "status": status,
                                            "action_json": {"kind": "cluster_update", "target": "c1"},
                                            "expires_at": datetime(2026, 11, 2, 6, 30)}, NOW))
    con.execute(_to_duckdb(L.expire_approvals_sql(ops, LATER)))                       # 07:00: 30 minutes past expiry
    assert dict(con.execute(f"SELECT approval_id, status FROM {ops}.approvals").fetchall()) == {"p": "expired", "a": "expired", "e": "executing"}
    con.execute(_to_duckdb(L.expire_approvals_sql(ops, datetime(2026, 11, 2, 7, 31))))  # more than an hour past it
    assert con.execute(f"SELECT status FROM {ops}.approvals WHERE approval_id = 'e'").fetchone()[0] == "expired"


def test_a_stale_drainer_cannot_reopen_or_overwrite_a_claimed_approval(ops_db):
    """Two drainers read the same approved row. One claims it and is applying the action. The other, still working
    from its earlier read, gets a deny from the gate and writes its note-only 'approved' mark, or decides the row is
    not the gate's and writes 'failed'. Neither write may touch the claimed row: put back to approved it would be
    claimed and applied a second time. Then the sequence from the review, with the production SQL: first claim, stale
    denial, second claim."""
    ops, con = ops_db
    con.execute(L.insert_approval(ops, {"approval_id": "ap1", "tick_id": "t1", "action_signature": "sig1", "decision_id": "d1",
                                        "action_json": {"kind": "cluster_update", "target": "c1"}, "note": "budget.big_resize"}, NOW))
    con.execute(L.approve_sql(ops, "ap1", "reviewer", now=LATER))

    def row():
        return con.execute(f"SELECT status, note FROM {ops}.approvals").fetchone()

    con.execute(L.claim_approval_sql(ops, "ap1", "first", LATER))                       # drainer A holds the row
    for stale in (L.mark_approval_sql(ops, "ap1", "approved", note="DENIED_ON_EXECUTE safety.active_runs", now=LATER),
                  L.mark_approval_sql(ops, "ap1", "failed", note="approval.trace: DUPLICATE_APPROVAL_ID", now=LATER),
                  L.mark_approval_sql(ops, "ap1", "executed", note="applied by tick x", now=LATER),
                  L.mark_approval_sql(ops, "ap1", "expired", now=LATER)):
        con.execute(stale)                                                             # drainer B, from its stale read
        status, note = row()
        assert status == "executing" and note.endswith("claim:first") and "DENIED" not in note and "DUPLICATE" not in note
    con.execute(L.claim_approval_sql(ops, "ap1", "second", LATER))                      # so a second claim finds nothing to take
    assert row()[1].endswith("claim:first")
    assert con.execute(_to_duckdb(L.approved_unexecuted_sql(ops, LATER))).fetchall() == []

    # another drainer's claim does not let it write the outcome either; only the holder's does
    con.execute(L.mark_approval_sql(ops, "ap1", "executed", note="applied", now=LATER, claim="second"))
    assert row()[0] == "executing"
    con.execute(L.mark_approval_sql(ops, "ap1", "executed", note="applied by tick t2", now=LATER, claim="first"))
    status, note = row()
    assert status == "executed" and note.endswith("applied by tick t2")

    # a write made before any claim never touches a pending row (a drainer only reads approved ones)
    con.execute(L.insert_approval(ops, {"approval_id": "ap2", "tick_id": "t1", "action_signature": "sig2",
                                        "action_json": {"kind": "cluster_update", "target": "c1"}}, NOW))
    con.execute(L.mark_approval_sql(ops, "ap2", "failed", note="approval.trace: DUPLICATE_APPROVAL_ID", now=LATER))
    assert con.execute(f"SELECT status FROM {ops}.approvals WHERE approval_id = 'ap2'").fetchone()[0] == "pending"
