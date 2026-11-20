"""Drafted repairs: prompt, extraction, validation (stub Spark parser), the two gate rules, the ledger rows, and the
drain that turns an approved statement_repair into a runbook."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from nightshift import draft as D
from nightshift import gate, ledger
from nightshift.types import Action, TickContext
from nightshift.watch import checked_watch, walk_tree

NOW = datetime(2026, 11, 3, 3, 0, 0, tzinfo=timezone.utc)
GOOD = ("MERGE INTO demo.silver.sensor_reading t USING demo.bronze.sensor_reading_raw s "
        "ON t.sensor_id = s.sensor_id AND t.event_ts = s.event_ts WHEN NOT MATCHED THEN INSERT *")
UPSERT = ("MERGE INTO demo.silver.sensor_reading t USING demo.bronze.sensor_reading_raw s "
          "ON t.sensor_id = s.sensor_id AND t.event_ts = s.event_ts WHEN MATCHED AND s.ingest_ts >= t.ingest_ts THEN UPDATE SET * "
          "WHEN NOT MATCHED THEN INSERT *")
VALUES_DRAFT = ("MERGE INTO demo.silver.sensor_reading t USING demo.bronze.sensor_reading_raw s "
                "ON t.sensor_id = s.sensor_id WHEN MATCHED AND s.ingest_ts >= t.ingest_ts THEN UPDATE SET value = COALESCE(s.value, t.value) "
                "WHEN NOT MATCHED THEN INSERT (sensor_id, value, ingest_ts) VALUES (s.sensor_id, COALESCE(s.value, CAST(0 AS DOUBLE)), s.ingest_ts)")


# --------------------------------------------------------------------------------------------- prompt + extraction
def test_prompt_contains_facts_and_forces_the_reply_shape():
    p = D.build_prompt(GOOD, "demo.silver.sensor_reading", "demo.bronze.sensor_reading_raw",
                       ["sensor_id", "event_ts"], "ingest_ts", [("sensor_id", "string"), ("ingest_ts", "timestamp")],
                       ["revision"], {"revision": 50000}, attempt=0)
    assert "demo.silver.sensor_reading" in p and "Ordering column: ingest_ts" in p
    assert "revision" in p and "50000" in p and "<sql>" in p and "nothing else" in p
    assert len(p) <= D.PROMPT_MAX_CHARS
    assert D.build_prompt(GOOD, "t", "s", ["k"], "ts", [], [], None, attempt=1) != D.build_prompt(GOOD, "t", "s", ["k"], "ts", [], [], None, attempt=0)


def test_ai_generate_sql_escapes_and_refuses_openai():
    sql = D.ai_generate_sql("xai.grok-4.7", "it's a 'test'\\n")
    assert sql.startswith("SELECT ai_generate('xai.grok-4.7', '") and "it\\'s" in sql and "\\\\n" in sql
    with pytest.raises(ValueError, match="is refused: the draft step does not use openai"):   # a rule, not a missing entry
        D.ai_generate_sql("openai.gpt-5.4", "x")
    with pytest.raises(ValueError):
        D.ai_generate_sql(None, "x")


@pytest.mark.parametrize("raw, expected", [
    ("```sql\n" + UPSERT + ";\n```", UPSERT),
    ("Here is the fix:\n\n" + UPSERT + "\n\nThis keeps the newest row.", UPSERT),
    (UPSERT.replace(" USING ", "\n  USING ") + ";", UPSERT),
    (UPSERT + " This keeps the newest row.", UPSERT),                                  # same-line prose after INSERT *
    (VALUES_DRAFT + " This keeps the newest row.", VALUES_DRAFT),                      # balanced VALUES with calls inside
    (VALUES_DRAFT.replace(" WHEN NOT MATCHED", " WHEN NOT MATCHED") , VALUES_DRAFT),
    ("I cannot help with that.", None),
    ("", None), (None, None),
    # prose that names the statement before giving it: the statement starts where a MERGE INTO <table> ... USING does
    ("Here is the corrected MERGE INTO statement:\n" + UPSERT, UPSERT),
    ("I rewrote the MERGE INTO statement so the newest row wins.\n\n" + UPSERT + ";", UPSERT),
    ("The MERGE INTO statement using a guard on the ordering column:\n" + UPSERT, UPSERT),
    ("Fixed: " + UPSERT, UPSERT),
])
def test_extract_sql(raw, expected):
    assert D.extract_sql(raw) == expected


# --------------------------------------------------------------------------------------------- validation (stub parser)
class _Node:
    def __init__(self, tree): self._t = tree
    def toJSON(self): return json.dumps(self._t)


class _StubSpark:
    """A stand-in for the Spark parser: a tiny MERGE grammar good enough to produce the node classes walk_tree checks."""
    def __init__(self, extra_nodes=()): self.extra = list(extra_nodes)

    def _plan(self, stmt):
        s = stmt.lower()
        nodes = [{"class": "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable"},
                 {"class": "org...UnresolvedRelation"}, {"class": "org...UnresolvedRelation"},
                 {"class": "org...EqualTo"}, {"class": "org...UnresolvedAttribute"}, {"class": "org...InsertStarAction"}]
        if "when matched" in s:
            nodes += [{"class": "org...GreaterThanOrEqual"}, {"class": "org...UpdateStarAction"}]
        if "reflect(" in s:
            nodes.append({"class": "org...UnresolvedFunction", "nameParts": "[reflect]"})
        if "(select" in s:
            nodes = [n for n in nodes if "UnresolvedRelation" not in n["class"]] + [{"class": "org...UnresolvedRelation"}, {"class": "org...Project"}]
        return _Node(nodes + self.extra)

    class _J:
        def __init__(self, outer): self.o = outer
        def sessionState(self): return self
        def sqlParser(self): return self
        def parsePlan(self, stmt): return self.o._plan(stmt)

    @property
    def _jsparkSession(self): return _StubSpark._J(self)


def test_walk_tree_allowlist():
    seen, bad = walk_tree([{"class": "a.MergeIntoTable"}, {"class": "a.UnresolvedFunction", "nameParts": "[coalesce]"},
                           {"class": "a.UnresolvedFunction", "nameParts": ["reflect"]}, {"class": "a.ScalarSubquery"}])
    assert seen[0] == "MergeIntoTable" and bad == ["function reflect", "ScalarSubquery"]


def test_checked_watch_accepts_good_and_rejects_hostile():
    sp = _StubSpark()
    w = {"table": "demo.silver.sensor_reading", "ts": "ingest_ts", "source": "demo.bronze.sensor_reading_raw"}
    t, ts, keys, a = checked_watch(sp, {**w, "statement": GOOD})
    assert keys == ["sensor_id", "event_ts"] and a.template.startswith("MERGE INTO {target}")
    t, ts, keys, a = checked_watch(sp, {**w, "statement": UPSERT})
    assert a.has_ordering_guard
    for bad in [GOOD.replace("t.sensor_id = s.sensor_id", "reflect('x','y') = s.sensor_id"),
                GOOD.replace("USING demo.bronze.sensor_reading_raw s", "USING (SELECT * FROM x) s"),
                GOOD + "; DROP TABLE x", GOOD.replace("INTO demo.silver.sensor_reading", "INTO other.table"),
                GOOD.replace("USING demo.bronze.sensor_reading_raw", "USING delta.`/tmp/x`"),
                GOOD.replace("USING demo.bronze.sensor_reading_raw", "USING somewhere.else")]:
        with pytest.raises(ValueError):
            checked_watch(sp, {**w, "statement": bad})


# --------------------------------------------------------------------------------------------- gate rules
def _ctx(verdicts):
    return TickContext(now_utc=NOW, budget_remaining_pct=80.0, projected_overrun_pct=0.0, llm_remaining_pct=50.0,
                       budget_source="ledger", kv={"tier": "high", "replay_verdicts": verdicts})


def _repair(draft_sha="d" * 64):
    return Action("statement_repair", "demo.silver.sensor_reading",
                  D.draft_action_params("demo.silver.sensor_reading", "s" * 64, "draft1", draft_sha, "xai.grok-4.7"))


def test_repair_without_verdict_is_denied_and_with_verdict_quarantined():
    audit = []
    eng = gate.PolicyEngine(gate.default_rules(), lambda d, a: audit.append(d), decision_counter=lambda *a: 0)
    d = eng.evaluate(_repair(), _ctx({}))
    assert d.decision == "deny" and d.rule_id == "verify.replay_required" and "NO_REPLAY_VERDICT" in d.reason
    d = eng.evaluate(_repair(), _ctx({"d" * 64: {"verdict": "silent_wrong", "age_hours": 0.1}}))
    assert d.decision == "deny" and "SILENT_WRONG" in d.reason
    d = eng.evaluate(_repair(), _ctx({"d" * 64: {"verdict": "replay_safe", "age_hours": 30.0}}))
    assert d.decision == "deny" and "STALE" in d.reason
    d = eng.evaluate(_repair(), _ctx({"d" * 64: {"verdict": "replay_safe", "age_hours": 0.5}}))
    assert d.decision == "quarantine" and d.rule_id == "tier.repair_quarantine"
    # the verdict must be for the draft's own hash, not the original statement's
    d = eng.evaluate(_repair(draft_sha="e" * 64), _ctx({"d" * 64: {"verdict": "replay_safe", "age_hours": 0.5}}))
    assert d.decision == "deny"
    assert len(audit) == 5


# --------------------------------------------------------------------------------------------- ledger rows
def test_repair_draft_rows_and_lookups():
    ops = "o.s"
    sql = ledger.insert_repair_draft(ops, {"statement_sha256": "s1", "draft_sha256": "d1", "model": "m", "table_name": "t",
                                           "draft_sql": "MERGE INTO t", "validation": "accepted", "verdict": "replay_safe",
                                           "diff_rows": 0, "attempt": 0}, NOW)
    assert "INSERT INTO o.s.repair_drafts" in sql and "'graded'" in sql and "CAST(0 AS BIGINT)" in sql
    rej = ledger.insert_repair_draft(ops, {"statement_sha256": "s1", "draft_sha256": "d2", "model": "m", "table_name": "t",
                                           "draft_sql": "nope", "validation": "rejected: no MERGE", "attempt": 1}, NOW)
    assert "'rejected'" in rej
    # one row per model call: the default draft_id carries tick and attempt, so the same text on a later tick is a new row
    same_text_later = ledger.insert_repair_draft(ops, {"tick_id": "t2", "statement_sha256": "s1", "draft_sha256": "d1", "model": "m",
                                                       "table_name": "t", "draft_sql": "MERGE INTO t", "validation": "accepted",
                                                       "verdict": "silent_wrong", "diff_rows": 3, "attempt": 0}, NOW)
    assert same_text_later.split("WHERE draft_id = ")[1] != sql.split("WHERE draft_id = ")[1]
    up = ledger.update_draft_status_sql(ops, "draft1", "approved", runbook_id="rb1", now=NOW)
    assert up.startswith("UPDATE o.s.repair_drafts SET status = 'approved'") and "runbook_id = 'rb1'" in up and up.endswith("'draft1'")
    guarded = ledger.update_draft_status_sql(ops, "draft1", "proposed", approval_id="ap1", now=NOW, only_from=("graded",))
    assert guarded.endswith("WHERE draft_id = 'draft1' AND status IN ('graded')")
    today = ledger.drafts_today_sql(ops, "s1", NOW)
    assert "FROM o.s.repair_drafts d LEFT JOIN o.s.approvals a ON a.approval_id = d.approval_id" in today
    assert "d.statement_sha256 = 's1'" in today and "INTERVAL 24 HOURS" in today and "AS failed_recent" in today
    decided = ledger.decided_draft_sql(ops, "d" * 64)
    assert decided.startswith("SELECT approval_id, status FROM o.s.approvals") and '"draft_sha256": "' + "d" * 64 in decided
    assert "'rejected'" in decided and "'expired'" not in decided
    latest = ledger.latest_replay_verdicts_sql(ops, 24, NOW)
    assert "ROW_NUMBER() OVER (PARTITION BY statement_sha256 ORDER BY created_at DESC, verdict_id DESC)" in latest and "age_hours" in latest
    assert "repair_drafts" in ledger.TABLE_ORDER and ledger.IDEMPOTENCY_KEY["repair_drafts"] == "draft_id"


# --------------------------------------------------------------------------------------------- drain: approved repair -> runbook
class _Store:
    def __init__(self, draft_sql, statement_sha="s" * 64, table="demo.silver.sensor_reading", validation="accepted"):
        self.sql = []; self.row = (draft_sql, table, "inc1", statement_sha, validation)

    def run_sql(self, sql):
        self.sql.append(sql)
        if "FROM o.s.repair_drafts WHERE draft_id" in sql:
            return [self.row]
        return []


def test_apply_statement_repair_writes_runbook_and_marks_draft():
    from nightshift.actuator import _apply_statement_repair
    sha = D.sha(UPSERT)
    a = Action("statement_repair", "demo.silver.sensor_reading",
               D.draft_action_params("demo.silver.sensor_reading", "s" * 64, "draft1", sha, "xai.grok-4.7"))
    st = _Store(UPSERT)
    out = _apply_statement_repair(st, "o.s", a, "reviewer", NOW)
    assert out["op"] == "runbook_upsert" and out["approved_by"] == "reviewer"
    joined = "\n".join(st.sql)
    assert "MERGE INTO o.s.runbooks" in joined and "STATEMENT_REPAIR" in joined
    assert "UPDATE o.s.repair_drafts SET status = 'approved'" in joined and "AND status IN ('proposed', 'graded')" in joined
    # refused: text changed since approval, graded for another statement, another table, or never accepted by the validator
    for bad in [_Store(UPSERT + " "), _Store(UPSERT, statement_sha="z" * 64), _Store(UPSERT, table="other.silver.t"),
                _Store(UPSERT, validation="rejected: no MERGE")]:
        with pytest.raises(ValueError):
            _apply_statement_repair(bad, "o.s", a, "reviewer", NOW)
        assert not any("runbooks" in s for s in bad.sql)


def test_draft_check_requires_a_declared_source():
    sp = _StubSpark()
    w = {"table": "demo.silver.sensor_reading", "ts": "ingest_ts", "statement": UPSERT}
    checked_watch(sp, w)                                   # a watch entry may leave the source implicit
    with pytest.raises(ValueError):
        checked_watch(sp, w, require_source=True)          # a model draft may not
    with pytest.raises(ValueError):
        checked_watch(sp, "not an object")
    t, ts, keys, a = checked_watch(sp, {**w, "source": "demo.bronze.sensor_reading_raw"}, require_source=True)
    assert a.source == "demo.bronze.sensor_reading_raw"


def test_a_merge_with_a_delete_branch_is_not_graded(monkeypatch):
    """The watched table is the statement's own output, so it holds no row the DELETE branch would remove; replay
    would pass without ever running it. Such a statement (watched or drafted) is refused with the reason."""
    from nightshift import watch

    def tree(_spark, stmt):
        return {"class": "x.MergeIntoTable", "children": [{"class": "x.UnresolvedRelation"}, {"class": "x.UnresolvedRelation"},
                                                          {"class": "x.DeleteAction"}, {"class": "x.InsertStarAction"}]}

    monkeypatch.setattr(watch, "parse_tree", tree)
    stmt = "MERGE INTO c.s.orders t USING c.b.orders s ON t.id = s.id WHEN MATCHED AND s.status = 'CANCELLED' THEN DELETE WHEN NOT MATCHED THEN INSERT *"
    with pytest.raises(ValueError, match="DELETE branch is not graded"):
        watch.checked_watch(None, {"table": "c.s.orders", "ts": "updated_at", "statement": stmt})
    assert "DeleteAction" not in watch.ALLOWED_NODES
