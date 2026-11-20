"""The scheduled job itself (jobs/nightshift_heartbeat.py), run end to end without a platform.

The file is executed the way the task runtime executes it, with a stand-in ``spark``. Ledger statements go to a real
duckdb copy of the ops schema, so leases, drafts, decisions, verdict read-backs and approvals behave as they do in
Delta; statements against the watched table and the sandbox are answered by the stand-in (row counts, coverage counts
and per-check diffs), and ``ai_generate`` returns a scripted reply. Only the platform parser is stubbed
(``nightshift.watch.parse_tree``); every other validation of the watch list and of the model's reply is the real one.

What this pins: every model call of a run is made before its first replay; a replay-safe draft opens exactly one
approval behind a quarantine decision; a verdict whose check exercised no row is insufficient_coverage and opens
nothing; a table over NS_REPLAY_MAX_ROWS is reported, not replayed; draft counts are read again under the lease.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from nightshift import draft as D
from nightshift import ledger as L
from nightshift import replay as R

from helpers import FrameSink, _to_duckdb, stub_spark_ui

ROOT = Path(__file__).resolve().parents[1]
OPS = "nightshift_ops"
SILVER, BRONZE = "default.nightshift_demo.orders_silver", "default.nightshift_demo.orders_bronze"
RETURNS, RETURNS_SRC = "default.nightshift_demo.returns_silver", "default.nightshift_demo.returns_bronze"
COLS = [("order_id", "int"), ("status", "string"), ("amount", "double"), ("updated_at", "timestamp")]
INSERT_ONLY = ("MERGE INTO {t} t USING {s} s ON t.order_id = s.order_id WHEN NOT MATCHED THEN INSERT "
               "(order_id, status, amount, updated_at) VALUES (s.order_id, s.status, s.amount, s.updated_at)")
GUARDED = ("MERGE INTO {t} t USING {s} s ON t.order_id = s.order_id WHEN MATCHED AND s.updated_at > t.updated_at THEN "
           "UPDATE SET t.status = s.status, t.amount = s.amount, t.updated_at = s.updated_at WHEN NOT MATCHED THEN INSERT "
           "(order_id, status, amount, updated_at) VALUES (s.order_id, s.status, s.amount, s.updated_at)")


class Row(tuple):
    """Enough of a Spark Row: positional, by name, asDict()."""

    def __new__(cls, **kw):
        self = super().__new__(cls, kw.values())
        self._names = list(kw)
        return self

    def asDict(self):
        return dict(zip(self._names, self))

    def __getitem__(self, k):
        return super().__getitem__(self._names.index(k)) if isinstance(k, str) else super().__getitem__(k)


class _NamedRow(tuple):
    """A row whose column names need not be identifiers (a coverage count named `revision:<column>`)."""
    def __new__(cls, data):
        self = super().__new__(cls, data.values())
        self._data = dict(data)
        return self

    def asDict(self):
        return dict(self._data)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return list(self._rows)


class FakeSpark:
    """Routes each statement: the model, the watched tables, the sandbox plan, or the duckdb ledger."""

    def __init__(self, con, n_rows=1000, coverage=None, table_rows=None, reply=None, clock=None, replay_s=0.0, fail_count_at=0):
        self.con = con
        self.n_rows = n_rows
        self.table_rows = table_rows or {}
        self.coverage = coverage                 # overrides for the coverage statement's counts
        self.reply = reply                       # the statement the model returns ({t}/{s} filled); default: the guarded upsert
        self.clock, self.replay_s = clock, replay_s   # a replay advances the stand-in clock by replay_s seconds
        self.fail_count_at, self.counts = fail_count_at, 0   # the nth row count of a watched table raises
        self.events: list[tuple] = []            # ("model", table) / ("replay", sandbox target) in the order they happen
        self.statements: list[str] = []
        self.last_merge = ""                     # the statement text of the plan in flight decides its diffs

    def sql(self, q: str):
        self.statements.append(q)
        if q.startswith("SELECT ai_generate("):
            table = RETURNS if "returns_silver" in q else SILVER
            self.events.append(("model", table))
            src = RETURNS_SRC if table == RETURNS else BRONZE
            return _Result([("Here is the fix:\n```sql\n" + (self.reply or GUARDED).format(t=table, s=src) + ";\n```\nIt keeps the newest row.",)])
        if q.startswith("SHOW SCHEMAS"):
            return _Result([])
        if q.startswith("DESCRIBE DETAIL"):
            return _Result([Row(format="delta")])
        if q.startswith("DESCRIBE HISTORY"):
            return _Result([Row(version=7), Row(version=6)])
        if q.startswith("DESCRIBE "):
            return _Result(COLS)
        m = re.match(r"SELECT COUNT\(\*\) FROM (default\.nightshift_demo\.\w+)$", q)
        if m:
            self.counts += 1
            if self.counts == self.fail_count_at:
                raise RuntimeError("AnalysisException: table is being rewritten")
            return _Result([(self.table_rows.get(m.group(1), self.n_rows),)])
        if "nightshift_sbx" in q or q.startswith("DROP VIEW IF EXISTS ns_batch_"):
            return self._plan(q)
        cur = self.con.execute(_to_duckdb(q))
        return _Result([tuple(r) for r in cur.fetchall()] if cur.description else [])

    def _plan(self, q: str):
        if q.startswith("CREATE SCHEMA IF NOT EXISTS"):
            self.events.append(("replay", q.split()[-1]))
            self.last_merge = ""
            if self.clock is not None:
                self.clock["t"] += self.replay_s
        elif q.startswith("MERGE INTO"):
            self.last_merge = q
        elif q.startswith("SELECT COUNT(*) AS n_rows"):
            names = re.findall(r" AS `?([\w:]+)`?", q)
            default = lambda n: self.n_rows if n == "n_rows" else 0 if n.startswith("revision:") else 50   # noqa: E731
            return _Result([_NamedRow({n: (self.coverage or {}).get(n, default(n)) for n in names})])
        elif "AS diff_rows" in q:
            # an insert-only statement keeps the stale row under the revision hazard; a guarded upsert is clean
            wrong = "WHEN MATCHED" not in self.last_merge and "_tgt_revision" in q
            return _Result([(220 if wrong else 0,)])
        return _Result([])


@pytest.fixture
def ledger_db():
    con = duckdb.connect()
    con.create_function("unix_timestamp", lambda ts: ts.timestamp(), ["TIMESTAMP"], "DOUBLE")
    for stmt in L.ddl_statements(OPS):
        con.execute(_to_duckdb(stmt))
    yield con
    con.close()


def _watch(tmp_path, entries):
    path = tmp_path / "watched_merges.json"
    path.write_text(json.dumps(entries), encoding="utf-8")
    return path


def _entry(table, source, statement=INSERT_ONLY, **kw):
    return {"name": f"insert-if-absent on {table.split('.')[-1]}", "table": table, "ts": "updated_at", "key": ["order_id"],
            "source": source, "statement": statement.format(t=table, s=source), **kw}


def _seed_verdict(con, statement, verdict="silent_wrong", table=SILVER, minutes_ago=20, elapsed=30.0, error=None):
    """A verdict the scheduled job wrote on an earlier tick, so this run drafts from it without replaying first."""
    wrong = verdict == "silent_wrong"
    con.execute(_to_duckdb(L.insert_replay_verdict(OPS, {
        "tick_id": "earlier-tick", "table_name": table, "statement_sha256": D.sha(statement), "candidate": "production",
        "verdict": verdict, "passes_snapshot": True, "passes_run_twice": True,
        "hazards_json": {**{h: True for h in R.DELIVERY_HAZARDS}, "revision": not wrong},
        "diffs_json": {"baseline": 0, "run_twice": 0, **{h: 0 for h in R.DELIVERY_HAZARDS}, "revision": 220 if wrong else 0},
        "diff_rows": 220 if wrong else 0, "statements_run": 80, "elapsed_s": elapsed, "error": error},
        datetime.now(timezone.utc) - timedelta(minutes=minutes_ago))))


def _run_heartbeat(monkeypatch, capsys, spark, watched: Path, **env):
    def parse_tree(_spark, stmt):  # the one platform call checked_watch makes; shape of a plain two-table MERGE
        return {"class": "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable",
                "children": [{"class": "x.UnresolvedRelation"}, {"class": "x.UnresolvedRelation"}]}

    monkeypatch.setattr("nightshift.watch.parse_tree", parse_tree)
    for k, v in {"NS_PKG": str(ROOT), "NS_OPS": OPS, "NS_WATCHED": str(watched), "NS_DRAFT_ONLY": "1", **env}.items():
        monkeypatch.setenv(k, v)
    for k in ("NS_FORCE_TICK", "NS_SBX", "NS_REUSE_VERDICT_H", "NS_DRAFT_ATTEMPTS", "NS_TASK_BUDGET_S", "NS_JOB_NAME"):
        if k not in env:
            monkeypatch.delenv(k, raising=False)
    src = (ROOT / "jobs" / "nightshift_heartbeat.py").read_text(encoding="utf-8")
    capsys.readouterr()
    exec(compile(src, "nightshift_heartbeat.py", "exec"), {"spark": spark, "__name__": "__heartbeat__"})
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    return lines[-1], [e for e in lines if "event" in e]


def _approvals(con):
    return con.execute(f"SELECT approval_id, status, action_signature, action_json FROM {OPS}.approvals").fetchall()


def test_every_model_call_precedes_the_first_replay_and_each_safe_draft_opens_one_approval(ledger_db, tmp_path, monkeypatch, capsys):
    """Two statements carry a silently-wrong verdict from an earlier tick and a third has never been graded. The run
    asks the model for both drafts, then replays both drafts, then replays the ungraded statement."""
    a, b, c = _entry(SILVER, BRONZE), _entry(RETURNS, RETURNS_SRC), _entry(SILVER, BRONZE, statement=GUARDED, draft=False)
    _seed_verdict(ledger_db, a["statement"])
    _seed_verdict(ledger_db, b["statement"], table=RETURNS)
    spark = FakeSpark(ledger_db)
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a, b, c]))

    kinds = [k for k, _ in spark.events]
    assert kinds == ["model", "model", "replay", "replay", "replay"]            # no model call after a replay
    assert [t for k, t in spark.events if k == "model"] == [SILVER, RETURNS]
    names = [e["event"] for e in events]
    assert names.index("draft_validated") < names.index("draft_graded")
    assert names.count("draft_received") == 2 and names.count("draft_graded") == 2 and names.count("approval_opened") == 2
    assert max(i for i, n in enumerate(names) if n == "draft_received") < min(i for i, n in enumerate(names) if n == "draft_graded")

    by_table = {(v["table"], v["verdict"]): v for v in summary["replay_verdicts"]}
    assert set(by_table) == {(SILVER, "silent_wrong"), (RETURNS, "silent_wrong"), (SILVER, "replay_safe")}
    for key in ((SILVER, "silent_wrong"), (RETURNS, "silent_wrong")):
        drafting = by_table[key]["drafting"]
        assert drafting["approval_opened"] and [x["verdict"] for x in drafting["attempts"]] == ["replay_safe"]
        assert drafting["attempts"][0]["gate"] == {"decision": "quarantine", "rule": "tier.repair_quarantine",
                                                   "reason": drafting["attempts"][0]["gate"]["reason"]}
    assert by_table[(SILVER, "replay_safe")]["coverage"]["revision"] == 50

    # the ledger: two pending approvals, each behind a quarantine decision for the same action, both leases released
    approvals = _approvals(ledger_db)
    assert len(approvals) == 2 and {r[1] for r in approvals} == {"pending"}
    for aid, _, sig, action_json in approvals:
        assert json.loads(action_json)["kind"] == "statement_repair"
        (did, bound_sig, opened_in), = ledger_db.execute(L.approval_binding_sql(OPS, aid)).fetchall()
        assert bound_sig == sig and ledger_db.execute(_to_duckdb(L.quarantine_trace_sql(OPS, did, sig, opened_in))).fetchone()[0] == 1
        assert ledger_db.execute(L.decision_holders_sql(OPS, did)).fetchone()[0] == 1
    leases = ledger_db.execute(f"SELECT status FROM {OPS}.repair_drafts WHERE validation = 'claim'").fetchall()
    assert leases == [("done",), ("done",)]
    assert ledger_db.execute(f"SELECT COUNT(*) FROM {OPS}.replay_verdicts WHERE verdict = 'replay_safe'").fetchone()[0] == 3
    assert summary["sandbox_dropped"] is True and summary["result"] == "tick recorded"
    assert ledger_db.execute(f"SELECT COUNT(*) FROM {OPS}.ticks").fetchone()[0] == 1


def test_a_passing_replay_with_an_unexercised_check_is_insufficient_coverage_and_opens_nothing(ledger_db, tmp_path, monkeypatch, capsys):
    """Three rows: the plan uses three deliveries, and here the coverage statement reports that the revision check
    touched no row. The draft passes every diff, but it is not replay_safe and no approval is opened."""
    a = _entry(SILVER, BRONZE)
    _seed_verdict(ledger_db, a["statement"])
    spark = FakeSpark(ledger_db, n_rows=3, coverage={"revision": 0, "late": 1, "duplicate": 1, "retried": 1, "run_twice": 1, "out_of_order": 3})
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]))
    assert any("NTILE(3) OVER" in q for q in spark.statements) and not any("NTILE(4) OVER" in q for q in spark.statements)
    attempt = summary["replay_verdicts"][0]["drafting"]["attempts"][0]
    assert attempt["verdict"] == "insufficient_coverage" and summary["replay_verdicts"][0]["drafting"]["approval_opened"] is False
    assert _approvals(ledger_db) == []
    row = ledger_db.execute(f"SELECT verdict, error FROM {OPS}.replay_verdicts WHERE candidate LIKE 'draft:%'").fetchone()
    assert row[0] == "insufficient_coverage" and "no row exercised revision" in row[1] and "3 row(s)" in row[1]


def test_a_table_over_the_row_cap_is_reported_and_not_replayed(ledger_db, tmp_path, monkeypatch, capsys):
    a = _entry(SILVER, BRONZE)
    spark = FakeSpark(ledger_db, n_rows=5_000_001)
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]), NS_REPLAY_MAX_ROWS="5000000")
    assert spark.events == []                                                      # nothing staged, nothing asked
    assert summary["replay_verdicts"] == [{"table": SILVER, "verdict": "skipped",
                                           "error": "5000001 rows is over NS_REPLAY_MAX_ROWS (5000000); not replayed"}]
    assert [e["event"] for e in events] == ["replay_skipped"]
    assert ledger_db.execute(f"SELECT COUNT(*) FROM {OPS}.replay_verdicts").fetchone()[0] == 0


def test_draft_counts_are_read_again_under_the_lease(ledger_db, tmp_path, monkeypatch, capsys):
    """Another runner finishes a replay-safe draft (with a live approval) between this run's first count and its
    claim. The count taken under the lease sees it: no model call is made and the lease is released."""
    a = _entry(SILVER, BRONZE)
    sha = D.sha(a["statement"])
    _seed_verdict(ledger_db, a["statement"])
    spark = FakeSpark(ledger_db)
    real_sql = spark.sql
    seen = {"counts": 0}

    def sql(q):
        if q.startswith("SELECT SUM(CASE WHEN d.validation <> 'claim'"):
            seen["counts"] += 1
            if seen["counts"] == 2:   # the read under the lease: the other runner's draft and approval are there now
                when = datetime.now(timezone.utc)
                spark.con.execute(_to_duckdb(L.insert_approval(OPS, {"approval_id": "other-appr", "tick_id": "other", "action_signature": "sig-other",
                                                                      "action_json": {"kind": "statement_repair", "target": SILVER},
                                                                      "expires_at": when + timedelta(hours=24)}, when)))
                spark.con.execute(_to_duckdb(L.insert_repair_draft(OPS, {
                    "draft_id": "other-draft", "tick_id": "other", "table_name": SILVER, "statement_sha256": sha, "model": "m", "attempt": 0,
                    "prompt_sha256": "p", "draft_sql": "MERGE ...", "draft_sha256": "d" * 64, "validation": "accepted", "verdict": "replay_safe",
                    "diff_rows": 0, "approval_id": "other-appr", "provenance": "nightshift_actuator"}, when)))
        return real_sql(q)

    spark.sql = sql
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]))
    assert seen["counts"] == 2 and spark.events == []
    drafting = summary["replay_verdicts"][0]["drafting"]
    assert drafting["attempts"] == [] and "already exists" in drafting["skipped"]
    assert ledger_db.execute(f"SELECT status FROM {OPS}.repair_drafts WHERE validation = 'claim'").fetchall() == [("done",)]
    assert [r[0] for r in _approvals(ledger_db)] == ["other-appr"]


def test_interleaved_mode_still_drafts_and_proves_in_one_pass(ledger_db, tmp_path, monkeypatch, capsys):
    """NS_DRAFT_BEFORE_REPLAY=0 keeps the older order (model call, replay, next statement) for instances where a
    model call after Spark work is not a problem."""
    a, b = _entry(SILVER, BRONZE), _entry(RETURNS, RETURNS_SRC)
    _seed_verdict(ledger_db, a["statement"])
    _seed_verdict(ledger_db, b["statement"], table=RETURNS)
    spark = FakeSpark(ledger_db)
    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a, b]), NS_DRAFT_BEFORE_REPLAY="0")
    assert [k for k, _ in spark.events] == ["model", "replay", "model", "replay"]
    assert len(_approvals(ledger_db)) == 2


def _open_one_approval(ledger_db, tmp_path, monkeypatch, capsys):
    a = _entry(SILVER, BRONZE)
    _seed_verdict(ledger_db, a["statement"])
    watched = _watch(tmp_path, [a])
    _run_heartbeat(monkeypatch, capsys, FakeSpark(ledger_db), watched)
    (aid, status, sig, _), = _approvals(ledger_db)
    assert status == "pending"
    return a, watched, aid, sig


def test_the_loop_closes_in_the_job_an_approved_repair_becomes_a_runbook_on_the_next_run(ledger_db, tmp_path, monkeypatch, capsys):
    """Run 1 drafts, proves and opens the approval. A person approves the row. Run 2 drains it: gate first, claim,
    runbook row with the approver's name, approval executed. Nothing but ledger writes."""
    a, watched, aid, sig = _open_one_approval(ledger_db, tmp_path, monkeypatch, capsys)
    ledger_db.execute(_to_duckdb(L.approve_sql(OPS, aid, "reviewer", now=datetime.now(timezone.utc))))
    summary, events = _run_heartbeat(monkeypatch, capsys, FakeSpark(ledger_db), watched, NS_FORCE_TICK="1", NS_APPROVERS="reviewer")
    drained = summary["approvals_drained"]
    assert [e["approval_id"] for e in drained["executed"]] == [aid] and drained["errors"] == [] and drained["left"] == []
    assert "approval_executed" in [e["event"] for e in events]
    (status, note), = ledger_db.execute(f"SELECT status, note FROM {OPS}.approvals").fetchall()
    assert status == "executed" and "claim:" in note
    (label, by, procedure), = ledger_db.execute(f"SELECT label, approved_by, procedure FROM {OPS}.runbooks").fetchall()
    assert label == "STATEMENT_REPAIR" and by == "reviewer" and procedure.startswith("MERGE INTO default.nightshift_demo.orders_silver")
    assert "WHEN MATCHED AND s.updated_at > t.updated_at" in procedure


@pytest.mark.parametrize("approver,allowlist,left_reason", [("reviewer", "priya,lee", "not on the NS_APPROVERS list"),
                                                             ("nightshift_heartbeat", "", "missing or a service identity")])
def test_the_job_applies_the_approver_checks_before_it_writes_a_runbook(ledger_db, tmp_path, monkeypatch, capsys, approver, allowlist, left_reason):
    a, watched, aid, sig = _open_one_approval(ledger_db, tmp_path, monkeypatch, capsys)
    ledger_db.execute(_to_duckdb(L.approve_sql(OPS, aid, approver, now=datetime.now(timezone.utc))))
    summary, _ = _run_heartbeat(monkeypatch, capsys, FakeSpark(ledger_db), watched, NS_FORCE_TICK="1", NS_APPROVERS=allowlist)
    drained = summary["approvals_drained"]
    assert drained["executed"] == [] and left_reason in drained["left"][0]["reason"]
    assert ledger_db.execute(f"SELECT COUNT(*) FROM {OPS}.runbooks").fetchone()[0] == 0
    assert ledger_db.execute(f"SELECT status FROM {OPS}.approvals").fetchall() == [("approved",)]


def test_a_copy_of_a_pending_repair_approval_writes_no_runbook_in_the_job(ledger_db, tmp_path, monkeypatch, capsys):
    """A second row for the same repair, inserted as already approved (with the real row's decision id): the job
    marks it failed and writes nothing; the row the gate opened is still pending."""
    a, watched, aid, sig = _open_one_approval(ledger_db, tmp_path, monkeypatch, capsys)
    (did, action_json, opened_in), = ledger_db.execute(f"SELECT decision_id, action_json, tick_id FROM {OPS}.approvals").fetchall()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    ledger_db.execute(f"INSERT INTO {OPS}.approvals (approval_id, tick_id, action_signature, action_json, decision_id, status, approved_by, "
                      "approved_at, requested_at, created_at) VALUES ('copy', ?, ?, ?, ?, 'approved', 'mallory', ?, ?, ?)",
                      [opened_in, sig, action_json, did, now, now, now])
    summary, _ = _run_heartbeat(monkeypatch, capsys, FakeSpark(ledger_db), watched, NS_FORCE_TICK="1")
    drained = summary["approvals_drained"]
    assert drained["executed"] == [] and "DECISION_SHARED" in drained["errors"][0]
    assert ledger_db.execute(f"SELECT COUNT(*) FROM {OPS}.runbooks").fetchone()[0] == 0
    assert dict(ledger_db.execute(f"SELECT approval_id, status FROM {OPS}.approvals").fetchall()) == {aid: "pending", "copy": "failed"}


# ----------------------------------------------------------------------------- drafting: what is asked for, and when not
STILL_WRONG = ("MERGE INTO {t} t USING {s} s ON t.order_id = s.order_id WHEN NOT MATCHED AND s.order_id IS NOT NULL THEN INSERT "
               "(order_id, status, amount, updated_at) VALUES (s.order_id, s.status, s.amount, s.updated_at)")


def _drafts(con):
    return con.execute(f"SELECT validation, status, verdict, prompt_sha256 FROM {OPS}.repair_drafts WHERE validation <> 'claim' "
                       "ORDER BY created_at").fetchall()


def _leases(con):
    return [r[0] for r in con.execute(f"SELECT status FROM {OPS}.repair_drafts WHERE validation = 'claim'").fetchall()]


def test_the_row_cap_also_covers_a_draft_no_model_call_for_a_table_too_large_to_replay(ledger_db, tmp_path, monkeypatch, capsys):
    """The base verdict is reused from an earlier tick; the table has since grown past the cap. Nothing is asked."""
    a = _entry(SILVER, BRONZE)
    _seed_verdict(ledger_db, a["statement"])
    spark = FakeSpark(ledger_db, n_rows=5_000_001)
    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]), NS_REPLAY_MAX_ROWS="5000000")
    assert spark.events == [] and _drafts(ledger_db) == []
    assert "over NS_REPLAY_MAX_ROWS (5000000)" in summary["replay_verdicts"][0]["drafting"]["skipped"]


def test_a_one_row_table_is_not_drafted_for(ledger_db, tmp_path, monkeypatch, capsys):
    """No draft can be proven on a table where a hazard has nothing to act on; asking would spend the daily cap."""
    a = _entry(SILVER, BRONZE)
    _seed_verdict(ledger_db, a["statement"])
    spark = FakeSpark(ledger_db, n_rows=1)
    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]))
    assert spark.events == [] and "no draft can be proven" in summary["replay_verdicts"][0]["drafting"]["skipped"]


def test_no_draft_is_asked_for_when_its_replay_would_not_fit_after_the_queued_ones(ledger_db, tmp_path, monkeypatch, capsys):
    """Three silently-wrong statements whose replays take 400 s each, a task of 1100 s. Two drafts fit; the third is
    not asked for (a reply that cannot be replayed is a model call spent for nothing), and both that were asked are replayed."""
    third, third_src = "default.nightshift_demo.refunds_silver", "default.nightshift_demo.refunds_bronze"
    entries = [_entry(SILVER, BRONZE), _entry(RETURNS, RETURNS_SRC), _entry(third, third_src)]
    for e in entries:
        _seed_verdict(ledger_db, e["statement"], table=e["table"], elapsed=400.0)
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr("time.time", lambda: clock["t"])
    spark = FakeSpark(ledger_db, clock=clock, replay_s=400.0)
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, entries), NS_TASK_BUDGET_S="1100")
    assert [k for k, _ in spark.events] == ["model", "model", "replay", "replay"]
    skipped = summary["replay_verdicts"][2]["drafting"]["skipped"]
    assert "800s already promised to queued replays" in skipped
    assert [d[1] for d in _drafts(ledger_db)] == ["proposed", "proposed"]        # both asked drafts were replayed and proposed
    assert "draft_replay_skipped" not in [e["event"] for e in events] and _leases(ledger_db) == ["done", "done", "done"]


def test_the_prompt_changes_from_tick_to_tick_until_the_daily_cap(ledger_db, tmp_path, monkeypatch, capsys):
    """One model call per tick in the default mode. Each uses the next wording (it follows the drafts already made
    today), and the fourth tick asks nothing: three replies in 24 hours is the cap."""
    a = _entry(SILVER, BRONZE)
    _seed_verdict(ledger_db, a["statement"])
    watched = _watch(tmp_path, [a])
    calls = []
    for _ in range(4):
        spark = FakeSpark(ledger_db, reply=STILL_WRONG)
        summary, _ = _run_heartbeat(monkeypatch, capsys, spark, watched, NS_FORCE_TICK="1")
        calls.append([k for k, _ in spark.events])
    assert calls == [["model", "replay"]] * 3 + [[]]
    rows = _drafts(ledger_db)
    assert [(r[0], r[2]) for r in rows] == [("accepted", "silent_wrong")] * 3 and len({r[3] for r in rows}) == 3
    assert "cap 3" in summary["replay_verdicts"][0]["drafting"]["skipped"]


def test_an_accepted_reply_is_recorded_even_when_its_replay_cannot_start(ledger_db, tmp_path, monkeypatch, capsys):
    """The row count before the draft's replay fails. The reply is still a row (accepted, never replayed), so the
    daily cap counts it; the lease is released; nothing is proposed."""
    a = _entry(SILVER, BRONZE)
    _seed_verdict(ledger_db, a["statement"])
    spark = FakeSpark(ledger_db, fail_count_at=2)      # 1st count: before asking; 2nd: before the replay
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]))
    assert [k for k, _ in spark.events] == ["model"]
    assert [r[:3] for r in _drafts(ledger_db)] == [("accepted", "validated", None)]
    attempt = summary["replay_verdicts"][0]["drafting"]["attempts"][0]
    assert attempt["error"].startswith("row count failed") and _leases(ledger_db) == ["done"] and _approvals(ledger_db) == []
    assert "draft_proof_failed" in [e["event"] for e in events]


def test_a_statement_listed_twice_is_asked_for_once(ledger_db, tmp_path, monkeypatch, capsys):
    a = _entry(SILVER, BRONZE)
    _seed_verdict(ledger_db, a["statement"])
    spark = FakeSpark(ledger_db)
    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a, dict(a)]))
    assert [k for k, _ in spark.events] == ["model", "replay"] and len(_approvals(ledger_db)) == 1
    assert "listed more than once" in summary["replay_verdicts"][1]["drafting"]["skipped"]


def test_with_verdict_reuse_off_a_run_drafts_right_after_its_own_replay(ledger_db, tmp_path, monkeypatch, capsys):
    """Deferring the draft to the next tick only works when that tick reuses this tick's verdict. With
    NS_REUSE_VERDICT_H=0 nothing would ever be drafted, so the run drafts after its replay instead."""
    a = _entry(SILVER, BRONZE)
    spark = FakeSpark(ledger_db)
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]), NS_REUSE_VERDICT_H="0")
    assert [k for k, _ in spark.events] == ["replay", "model", "replay"]
    assert summary["replay_verdicts"][0]["drafting"]["approval_opened"] and "drafting_deferred" not in [e["event"] for e in events]


def test_a_reused_insufficient_coverage_verdict_keeps_its_explanation(ledger_db, tmp_path, monkeypatch, capsys):
    a = _entry(SILVER, BRONZE)
    why = "insufficient coverage: no row exercised late, out_of_order (1 row(s) in the table)"
    _seed_verdict(ledger_db, a["statement"], verdict="insufficient_coverage", error=why)
    spark = FakeSpark(ledger_db, n_rows=1)
    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]))
    entry = summary["replay_verdicts"][0]
    assert spark.events == [] and entry["verdict"] == "insufficient_coverage" and entry["error"] == why and "drafting" not in entry


# ----------------------------------------------------------------------------- housekeeping: the stale-sandbox sweep
class SchemaListingSpark(FakeSpark):
    """FakeSpark whose catalog lists the given schemas, so the sweep has something to choose from."""

    def __init__(self, con, schemas, **kw):
        super().__init__(con, **kw)
        self.schemas = schemas

    def sql(self, q: str):
        if q.startswith("SHOW SCHEMAS"):
            self.statements.append(q)
            return _Result([(s,) for s in self.schemas])
        return super().sql(q)


def test_the_sandbox_sweep_drops_only_stale_schemas_named_the_way_a_run_names_them(ledger_db, tmp_path, monkeypatch, capsys):
    """A run's sandbox is <base>_<tick8>_<start minute>. Other schemas that merely share the base (a team's
    date-stamped schema, a date range) are never dropped, and neither is a run that is still inside its window."""
    minute = int(datetime.now(timezone.utc).timestamp()) // 60
    stale, fresh = f"nightshift_sbx_0a1b2c3d_{minute - 600}", f"nightshift_sbx_0a1b2c3d_{minute - 10}"
    foreign = ["nightshift_sbx_reports_20250131_2", "nightshift_sbx_archive_deadbeef_7",
               "nightshift_sbx_20250131_2", "nightshift_sbx_20250101_20250131"]
    spark = SchemaListingSpark(ledger_db, [stale, fresh, *foreign])

    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, []))

    assert summary["sandbox_swept"] == [stale]
    dropped = " ".join(q for q in spark.statements if q.startswith("DROP SCHEMA"))
    assert stale in dropped
    assert not [name for name in [fresh, *foreign] if name in dropped]


# ----------------------------------------------------------------------------- the drain never costs the tick
def test_the_tick_is_still_graded_and_recorded_when_the_approval_drain_raises(ledger_db, tmp_path, monkeypatch, capsys):
    def broken_drain(*args, **kwargs):
        raise RuntimeError("approvals table unavailable")

    monkeypatch.setattr("nightshift.drain.drain_approvals", broken_drain)
    a = _entry(SILVER, BRONZE)

    summary, _ = _run_heartbeat(monkeypatch, capsys, FakeSpark(ledger_db), _watch(tmp_path, [a]))

    assert summary["approvals_drained"]["errors"] == ["drain failed: RuntimeError: approvals table unavailable"]
    assert summary["approvals_drained"]["executed"] == []
    assert [v["table"] for v in summary["replay_verdicts"]] == [SILVER]
    assert ledger_db.execute(f"SELECT COUNT(*) FROM {OPS}.ticks").fetchone()[0] == 1


# ----------------------------------------------------------------------------- evidence: the flight recorder's own report
UI_SAMPLE = {"app_id": "app-1", "jobs": [], "sql": [], "executors": [],
             "stages": [{"stageId": 1, "attemptId": 0, "status": "COMPLETE", "numTasks": 1, "name": "count at <stdin>:1"}]}


def test_the_tick_says_why_when_the_flight_recorder_recorded_nothing(ledger_db, tmp_path, monkeypatch, capsys):
    """The generated cell catches its own errors and prints them; the tick summary repeats what the cell reported."""
    stub_spark_ui(monkeypatch, None)

    summary, _ = _run_heartbeat(monkeypatch, capsys, FakeSpark(ledger_db), _watch(tmp_path, []), NS_DRAFT_ONLY="0")

    assert summary["flight_recorder"].startswith("skipped: ")
    assert "no applications from Spark UI" in summary["flight_recorder"]


def test_the_tick_says_ok_when_the_flight_recorder_wrote_its_rows(ledger_db, tmp_path, monkeypatch, capsys):
    stub_spark_ui(monkeypatch, UI_SAMPLE)
    spark, sink = FakeSpark(ledger_db), FrameSink()
    spark.sparkContext = SimpleNamespace(uiWebUrl="http://driver:4040")
    spark.createDataFrame = sink.createDataFrame

    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, []), NS_DRAFT_ONLY="0")

    assert summary["flight_recorder"] == "ok"
    assert len(sink.saved) == 1 and len(sink.saved[0][2]) == 1


# ---------------------------------------------------------------------------------------------------------------------
# review round 3
# ---------------------------------------------------------------------------------------------------------------------
VERSIONED = ("MERGE INTO {t} t USING {s} s ON t.order_id = s.order_id AND t.updated_at = s.updated_at WHEN NOT MATCHED THEN INSERT "
             "(order_id, status, amount, updated_at) VALUES (s.order_id, s.status, s.amount, s.updated_at)")


def test_an_ordering_column_in_the_key_is_rejected_before_any_replay_or_model_call(ledger_db, tmp_path, monkeypatch, capsys):
    """The keys come from the statement's ON equalities in the first entry and from `key` in the second. Neither is
    replayed, drafted for or written to the ledger as a verdict: with the ordering column in the key the revision
    check would have passed without exercising anything."""
    from_on = {"table": SILVER, "ts": "updated_at", "source": BRONZE, "statement": VERSIONED.format(t=SILVER, s=BRONZE)}
    declared = _entry(SILVER, BRONZE, key=["order_id", "updated_at"])
    _seed_verdict(ledger_db, declared["statement"])
    spark = FakeSpark(ledger_db)
    summary, events = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [from_on, declared]))
    assert [(v["verdict"], v["table"]) for v in summary["replay_verdicts"]] == [("rejected", SILVER)] * 2
    assert all("ordering column cannot be a key" in v["error"] for v in summary["replay_verdicts"])
    assert spark.events == [] and _approvals(ledger_db) == []
    assert ledger_db.execute(f"SELECT COUNT(*) FROM {OPS}.replay_verdicts WHERE tick_id <> 'earlier-tick'").fetchone()[0] == 0
    # the same statement with the business key declared is graded
    graded = {**from_on, "key": ["order_id"], "draft": False}
    summary, _ = _run_heartbeat(monkeypatch, capsys, FakeSpark(ledger_db), _watch(tmp_path, [graded]), NS_FORCE_TICK="1")
    assert summary["replay_verdicts"][0]["verdict"] == "silent_wrong"


def test_a_table_with_a_column_a_correction_cannot_change_is_insufficient_coverage_by_name(ledger_db, tmp_path, monkeypatch, capsys):
    """An array column is passed through the corrections unchanged, so an UPDATE that forgets it would look right. The
    plan reports a zero count under the column's name; the verdict is not replay_safe and the ledger row says why."""
    monkeypatch.setitem(globals(), "COLS", [*COLS, ("tags", "array<string>")])
    a = _entry(SILVER, BRONZE, statement=GUARDED, draft=False)
    spark = FakeSpark(ledger_db)
    summary, _ = _run_heartbeat(monkeypatch, capsys, spark, _watch(tmp_path, [a]))
    v = summary["replay_verdicts"][0]
    assert v["verdict"] == "insufficient_coverage" and v["coverage"]["revision:tags"] == 0 and v["coverage"]["revision"] == 50
    assert any("0 AS `revision:tags`" in q for q in spark.statements)
    row = ledger_db.execute(f"SELECT verdict, error FROM {OPS}.replay_verdicts").fetchone()
    assert row[0] == "insufficient_coverage" and "a correction cannot change tags" in row[1] and "no row exercised" not in row[1]
