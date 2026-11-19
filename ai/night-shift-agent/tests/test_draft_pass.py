"""The actuator's drafting pass (run_tick --draft): the ledger queries that pick the work, the selection against a
watch list, the kernel cell it runs (the heartbeat file in draft-only mode with verdict reuse), the parsing of the
heartbeat's printed lines, and the plumbing through tick(). The heartbeat itself is not imported here (it needs Spark)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from nightshift import actuator as A
from nightshift import ledger as L

from helpers import OPS, ROW_NORMAL, FakeIO, FakeStore, run

NOW = datetime(2026, 11, 3, 19, 0, tzinfo=timezone.utc)
W1 = {"name": "a", "table": "default.demo.a_silver", "ts": "updated_at", "source": "default.demo.a_bronze",
      "statement": "MERGE INTO default.demo.a_silver t USING default.demo.a_bronze s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"}
W2 = {"name": "b", "table": "default.demo.b_silver", "ts": "updated_at", "source": "default.demo.b_bronze",
      "statement": "MERGE INTO default.demo.b_silver t USING default.demo.b_bronze s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"}
W3 = {"name": "c", "table": "default.demo.c_silver", "ts": "updated_at", "source": "default.demo.c_bronze",
      "statement": "MERGE INTO default.demo.c_silver t USING default.demo.c_bronze s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"}
SHA = {k: A.statement_sha(w["statement"]) for k, w in (("a", W1), ("b", W2), ("c", W3))}


# ---------------------------------------------------------------- ledger queries (shape + duckdb)
def test_query_shapes():
    q = L.latest_verdict_for_statement_sql(OPS, "s" * 64, 24, NOW)
    assert "INTERVAL 1440 MINUTES" in q and "ORDER BY created_at DESC, verdict_id DESC LIMIT 1" in q and ("s" * 64) in q
    q = L.graded_statements_sql(OPS, 24, NOW)
    assert "ROW_NUMBER() OVER (PARTITION BY statement_sha256" in q and q.rstrip().endswith("WHERE verdict IN ('replay_safe', 'silent_wrong', 'snapshot_wrong', 'insufficient_coverage') OR (verdict = 'crash' AND age_hours < 2.0)")
    q = L.statements_needing_draft_sql(OPS, 24, NOW, cap=3)
    assert "v.verdict = 'silent_wrong'" in q and "NOT EXISTS" in q and "< 3" in q and "a.status IN ('pending', 'approved', 'executing', 'executed')" in q
    assert "d.validation = 'claim' AND d.status = 'drafting' AND d.updated_at >=" in q and "NOT LIKE 'rejected: ai_generate failed%'" in q
    q = L.drafts_today_sql(OPS, "s" * 64, NOW)
    assert "AS failed_recent" in q and "INTERVAL 60 MINUTES" in q and "d.validation <> 'claim'" in q
    assert not q.rstrip().endswith("INTERVAL 24 HOURS")           # the live-approval count is not time-bounded
    cid = L.claim_id_for("s" * 64)
    assert len(cid) == 16 and L.claim_owner_sql(OPS, cid) == f"SELECT tick_id, provenance, status, updated_at FROM {OPS}.repair_drafts WHERE draft_id = '{cid}'"
    q = L.claim_take_over_sql(OPS, cid, "nonce1", "nightshift_actuator", NOW)
    assert "SET tick_id = 'nonce1', provenance = 'nightshift_actuator', status = 'drafting'" in q and "(status <> 'drafting' OR updated_at <" in q
    q = L.claim_release_sql(OPS, cid, "nonce1", NOW)
    assert q.startswith(f"UPDATE {OPS}.repair_drafts SET status = 'done'") and "tick_id = 'nonce1' AND status = 'drafting'" in q


def _verdict(ops, con, sha, verdict, when, table="t", candidate="c"):
    con.execute(L.insert_replay_verdict(ops, {
        "tick_id": "tick" + sha[:4] + candidate, "table_name": table, "statement_sha256": sha, "candidate": candidate,
        "verdict": verdict, "passes_snapshot": True, "passes_run_twice": True,
        "hazards_json": {"late": True, "revision": verdict == "replay_safe"}, "diffs_json": {"baseline": 0, "revision": 0 if verdict == "replay_safe" else 5},
        "diff_rows": 0 if verdict == "replay_safe" else 5, "statements_run": 10, "elapsed_s": 300.0}, now=when))


def _draft(ops, con, sha, draft_sha, verdict, approval_id, when, validation="accepted", status=None):
    row = {"tick_id": "t" + draft_sha[:4], "table_name": "t", "statement_sha256": sha, "model": "m", "attempt": 0,
           "prompt_sha256": "p" * 64, "draft_sql": "MERGE ...", "draft_sha256": draft_sha, "validation": validation,
           "verdict": verdict, "diff_rows": 0, "approval_id": approval_id, "provenance": "test"}
    if status:
        row["status"] = status
    con.execute(L.insert_repair_draft(ops, row, now=when))


def test_duckdb_needing_draft_and_graded(ops_db):
    ops, con = ops_db
    # Spark's unix_timestamp(ts) is not a duckdb function; register the equivalent for the age arithmetic
    con._con.create_function("unix_timestamp", lambda ts: ts.timestamp(), ["TIMESTAMP"], "DOUBLE")
    a, b, c = SHA["a"], SHA["b"], SHA["c"]
    # a: silently wrong, no drafts            -> needs a draft
    # b: silently wrong, replay_safe draft with a pending unexpired approval -> covered
    # c: silently wrong but three drafts today -> capped
    # d: replay_safe                           -> graded, nothing to do
    _verdict(ops, con, a, "silent_wrong", NOW - timedelta(hours=1))
    _verdict(ops, con, b, "silent_wrong", NOW - timedelta(hours=2))
    _draft(ops, con, b, "d" * 64, "replay_safe", "appr1", NOW - timedelta(hours=1))
    con.execute(L.insert_approval(ops, {"approval_id": "appr1", "tick_id": "t", "action_signature": "sig1",
                                        "action_json": {"kind": "statement_repair"}, "note": "n",
                                        "expires_at": NOW + timedelta(hours=20), "status": "pending"}, now=NOW - timedelta(hours=1)))
    _verdict(ops, con, c, "silent_wrong", NOW - timedelta(hours=3))
    for i in range(3):
        _draft(ops, con, c, f"{i}" * 64, "silent_wrong", None, NOW - timedelta(hours=2, minutes=i))
    _verdict(ops, con, "e" * 64, "replay_safe", NOW - timedelta(minutes=30))
    # f: silently wrong with three FAILED model calls today -> failed calls do not eat the cap, still needs a draft
    f = "f" * 64
    _verdict(ops, con, f, "silent_wrong", NOW - timedelta(hours=1))
    for i in range(3):
        _draft(ops, con, f, f"a{i}" * 32, None, None, NOW - timedelta(hours=3, minutes=i), validation="rejected: ai_generate failed: boom")
    # g: silently wrong with a live lease (another runner drafting right now) -> not needing; h: same but the lease is stale
    g, h = "9" * 64, "8" * 64
    _verdict(ops, con, g, "silent_wrong", NOW - timedelta(hours=1))
    _draft(ops, con, g, "c1" * 32, None, None, NOW - timedelta(minutes=10), validation="claim", status="drafting")
    _verdict(ops, con, h, "silent_wrong", NOW - timedelta(hours=1))
    _draft(ops, con, h, "c2" * 32, None, None, NOW - timedelta(hours=2), validation="claim", status="drafting")
    # i: an older silent_wrong verdict but the LATEST row is a three-hour-old crash -> neither graded nor needing (regraded)
    # j: latest row is a crash from 30 minutes ago -> counted as graded (not retried every pass), not needing
    # k: latest row snapshot_wrong -> graded, never needing
    i_, j_, k_ = "7" * 64, "6" * 64, "5" * 64
    _verdict(ops, con, i_, "silent_wrong", NOW - timedelta(hours=5))
    _verdict(ops, con, i_, "crash", NOW - timedelta(hours=3), candidate="again")
    _verdict(ops, con, j_, "crash", NOW - timedelta(minutes=30))
    _verdict(ops, con, k_, "snapshot_wrong", NOW - timedelta(minutes=30))
    needing = {r[0] for r in con.execute(L.statements_needing_draft_sql(ops, 24, NOW)).fetchall()}
    assert needing == {a, f, h}
    graded = {r[0] for r in con.execute(L.graded_statements_sql(ops, 24, NOW)).fetchall()}
    assert graded == {a, b, c, "e" * 64, f, g, h, j_, k_}
    # the live-approval block does not age out: an executed repair from two days ago still covers its statement
    m = "4" * 64
    _verdict(ops, con, m, "silent_wrong", NOW - timedelta(hours=1))
    _draft(ops, con, m, "d2" * 32, "replay_safe", "appr2", NOW - timedelta(days=2))
    con.execute(L.insert_approval(ops, {"approval_id": "appr2", "tick_id": "t", "action_signature": "sig2",
                                        "action_json": {"kind": "statement_repair"}, "note": "n",
                                        "expires_at": NOW - timedelta(days=1), "status": "executed"}, now=NOW - timedelta(days=2)))
    assert con.execute(L.drafts_today_sql(ops, m, NOW)).fetchall()[0][:2] == (0, 1)
    assert m not in {r[0] for r in con.execute(L.statements_needing_draft_sql(ops, 24, NOW)).fetchall()}
    # the lease: insert-if-absent, take-over only when released or stale, release only by the holder
    cid = L.claim_id_for(a)
    def lease_row(**kw):
        return {"draft_id": cid, "tick_id": kw["nonce"], "table_name": "t", "statement_sha256": a, "model": "m", "attempt": -1,
                "prompt_sha256": "", "draft_sql": "", "draft_sha256": cid, "validation": "claim", "status": "drafting", "provenance": kw["who"]}
    con.execute(L.insert_repair_draft(ops, lease_row(nonce="n1", who="job"), now=NOW - timedelta(minutes=5)))
    con.execute(L.insert_repair_draft(ops, lease_row(nonce="n2", who="actuator"), now=NOW))      # no-op: the lease exists
    con.execute(L.claim_take_over_sql(ops, cid, "n2", "actuator", NOW))                           # fresh lease: left alone
    assert con.execute(L.claim_owner_sql(ops, cid)).fetchall()[0][:3] == ("n1", "job", "drafting")
    con.execute(L.claim_release_sql(ops, cid, "n2", NOW))                                        # not the holder: no-op
    assert con.execute(L.claim_owner_sql(ops, cid)).fetchall()[0][2] == "drafting"
    con.execute(L.claim_release_sql(ops, cid, "n1", NOW))
    assert con.execute(L.claim_owner_sql(ops, cid)).fetchall()[0][2] == "done"
    con.execute(L.claim_take_over_sql(ops, cid, "n2", "actuator", NOW))                           # released: re-armed
    assert con.execute(L.claim_owner_sql(ops, cid)).fetchall()[0][:3] == ("n2", "actuator", "drafting")
    con.execute(L.claim_take_over_sql(ops, cid, "n3", "job", NOW + timedelta(minutes=61)))        # stale: re-armed by the next run
    assert con.execute(L.claim_owner_sql(ops, cid)).fetchall()[0][:3] == ("n3", "job", "drafting")
    n, safe, failed_recent = con.execute(L.drafts_today_sql(ops, f, NOW)).fetchall()[0]
    assert (n, safe, failed_recent) == (0, 0, 0)
    _draft(ops, con, f, "a9" * 32, None, None, NOW - timedelta(minutes=20), validation="rejected: ai_generate failed: boom")
    assert con.execute(L.drafts_today_sql(ops, f, NOW)).fetchall()[0][2] == 1
    assert con.execute(L.drafts_today_sql(ops, f, NOW, runner="test")).fetchall()[0][2] == 1      # my own failure backs me off
    assert con.execute(L.drafts_today_sql(ops, f, NOW, runner="nightshift_actuator")).fetchall()[0][2] == 0   # the job's failure does not block the actuator
    row = con.execute(L.latest_verdict_for_statement_sql(ops, a, 24, NOW)).fetchall()
    assert len(row) == 1 and row[0][1] == "silent_wrong" and json.loads(row[0][2])["revision"] == 5
    # a verdict older than the window is not reused
    assert con.execute(L.latest_verdict_for_statement_sql(ops, a, 0.5, NOW)).fetchall() == []


# ---------------------------------------------------------------- selection against the watch list
class DraftStore(FakeStore):
    def __init__(self, graded=(), needing=(), **kw):
        super().__init__(**kw)
        self.graded, self.needing = list(graded), list(needing)

    def run_sql(self, sql, timeout=300):
        if ") g" in sql and "WHERE verdict IN ('replay_safe', 'silent_wrong', 'snapshot_wrong', 'insufficient_coverage')" in sql:
            self.sql.append(sql)
            return [(s,) for s in self.graded]
        if "v.verdict = 'silent_wrong'" in sql:
            self.sql.append(sql)
            return [(s,) for s in self.needing]
        return super().run_sql(sql, timeout)


def test_select_draft_work_grades_unseen_and_drafts_silent_wrong():
    store = DraftStore(graded=[SHA["a"], SHA["b"]], needing=[SHA["a"]])
    work, notes = A.select_draft_work(store, OPS, [W1, W2, W3, "junk", {"table": "x"}], NOW)
    assert [w["table"] for w in work] == [W1["table"], W3["table"]]
    assert any("silently wrong" in n for n in notes) and any("no verdict" in n for n in notes)
    work, notes = A.select_draft_work(DraftStore(graded=[SHA["a"]], needing=[]), OPS, [W1], NOW)
    assert work == [] and notes[-1].startswith("draft pass: nothing to draft")
    assert A.select_draft_work(store, OPS, [], NOW) == ([], ["draft pass: no watched statements"])


# ---------------------------------------------------------------- the kernel cell and its output
def test_heartbeat_kernel_code_bundles_sources_sets_knobs_and_execs_from_a_private_dir():
    sources = {"nightshift_heartbeat.py": "print('hb')", "pkg/nightshift/__init__.py": "", "pkg/nightshift/ledger.py": "X = 1"}
    code = A.heartbeat_kernel_code([W1], sources)
    assert '"NS_DRAFT_ONLY": "1"' in code and '"NS_REUSE_VERDICT_H": "24"' in code and '"NS_FORCE_TICK": "1"' in code
    assert "os.environ['NS_WATCHED'] = _watch" in code and "os.environ['NS_PKG'] = os.path.join(_dir, 'pkg')" in code
    assert json.dumps(json.dumps([W1])) in code and json.dumps(json.dumps(sources, sort_keys=True)) in code
    assert "/Workspace/Shared" not in code                             # nothing is read from the shared folder
    assert "exec(compile(_src['nightshift_heartbeat.py']" in code and "shutil.rmtree(_dir, ignore_errors=True)" in code
    assert "sys.dont_write_bytecode = True" in code and "del sys.modules[_m]" in code
    compile(code, "cell", "exec")   # it is valid Python
    with pytest.raises(ValueError):
        A.heartbeat_kernel_code([W1], {"pkg/nightshift/x.py": ""})


def test_bundle_sources_has_heartbeat_and_every_package_module(tmp_path):
    (tmp_path / "jobs").mkdir(); (tmp_path / "nightshift").mkdir()
    (tmp_path / "jobs" / "nightshift_heartbeat.py").write_text("print(1)", encoding="utf-8")
    (tmp_path / "nightshift" / "a.py").write_text("x = 1", encoding="utf-8")
    assert A.bundle_sources(tmp_path) == {"nightshift_heartbeat.py": "print(1)", "pkg/nightshift/a.py": "x = 1"}
    real = A.bundle_sources()
    assert "nightshift_heartbeat.py" in real and "pkg/nightshift/actuator.py" in real and len(real) >= 15
    assert 'os.environ.get("NS_PKG"' in real["nightshift_heartbeat.py"]


def test_parse_heartbeat_output_finds_summary_and_events():
    res = {"outputs": [{"output_type": "stream", "text": '{"event": "replaying", "table": "t"}\nnoise\n'},
                       {"output_type": "stream", "text": '{"event": "draft_graded", "verdict": "replay_safe"}\n{"tick_id": "abc", "result": "tick recorded"}\n'}]}
    summary, events = A.parse_heartbeat_output(res)
    assert summary == {"tick_id": "abc", "result": "tick recorded"} and [e["event"] for e in events] == ["replaying", "draft_graded"]
    assert A.parse_heartbeat_output({"outputs": []}) == (None, [])
    assert A.parse_heartbeat_output("garbage") == (None, [])


# ---------------------------------------------------------------- the pass through tick()
class KernelIO(FakeIO):
    """FakeIO with the two things the pass uses: client.nb_save_file and run_code."""

    def __init__(self, output: dict, **kw):
        super().__init__(**kw)
        self.output = output
        self.saved: list[tuple[str, str]] = []
        self.codes: list[tuple[str, int]] = []
        io = self

        class _C:
            def nb_save_file(self, path, content):
                io.saved.append((path, content))
                return {"path": path}
        self.client = _C()

    def run_code(self, code, timeout=300):
        self.codes.append((code, timeout))
        return self.output


SUMMARY = {"tick_id": "deadbeef", "job": "nightshift_actuator", "sandbox": "default.nightshift_sbx_dead_1", "sandbox_dropped": True,
           "result": "tick recorded", "elapsed_s": 700.0,
           "replay_verdicts": [{"table": W1["table"], "verdict": "silent_wrong",
                                "drafting": {"attempts": [{"attempt": 0, "verdict": "replay_safe", "approval_id": "ap12"}], "approval_opened": True}}]}


def test_tick_draft_pass_runs_heartbeat_on_selected_entries_and_reports():
    out = {"outputs": [{"output_type": "stream", "text": '{"event": "verdict_reused"}\n' + json.dumps(SUMMARY) + "\n"}]}
    io = KernelIO(out)
    store = DraftStore(graded=[SHA["a"], SHA["b"]], needing=[SHA["a"]], budget_rows=[ROW_NORMAL])
    rep = run(io, store, draft=True, watched=[W1, W2])
    assert io.saved == []                                               # nothing is written to the shared workspace
    assert len(io.codes) == 1 and io.codes[0][1] == A.DRAFT_PASS_TIMEOUT_S and "NS_DRAFT_ONLY" in io.codes[0][0]
    assert f'"NS_TASK_BUDGET_S": "{A.DRAFT_PASS_TIMEOUT_S - 300}"' in io.codes[0][0]
    assert json.dumps(json.dumps([W1])) in io.codes[0][0] and W2["table"] not in io.codes[0][0]   # only the selected entry
    assert rep.draft["sources"] >= 15 and rep.draft["kernel_status"] is None and "error" not in rep.draft
    assert rep.draft["entries"] == 1 and rep.draft["summary"]["tick_id"] == "deadbeef" and rep.draft["events"] == ["verdict_reused"]
    assert any("approval opened ap12" in n for n in rep.notes)
    # the tick row is written before the pass starts (the pass can run for most of an hour)
    assert store.idx(f"INSERT INTO {OPS}.ticks") < store.idx(") g")


def test_tick_draft_pass_does_nothing_when_nothing_to_draft_or_when_off():
    io = KernelIO({"outputs": []})
    store = DraftStore(graded=[SHA["a"]], needing=[], budget_rows=[ROW_NORMAL])
    rep = run(io, store, draft=True, watched=[W1])
    assert io.codes == [] and rep.draft == {"entries": 0, "tables": []}
    rep = run(KernelIO({"outputs": []}), DraftStore(graded=[], needing=[SHA["a"]], budget_rows=[ROW_NORMAL]), draft=False, watched=[W1])
    assert rep.draft is None


def test_tick_draft_pass_failure_is_a_note_not_a_crash():
    class Boom(KernelIO):
        def run_code(self, code, timeout=300):
            raise RuntimeError("kernel gone")
    io = Boom({"outputs": []})
    store = DraftStore(graded=[], needing=[], budget_rows=[ROW_NORMAL])
    rep = run(io, store, draft=True, watched=[W1])
    assert rep.draft["error"].startswith("RuntimeError") and any("draft pass failed" in n for n in rep.notes)
    assert store.count(f"INSERT INTO {OPS}.ticks") == 1


def test_tick_draft_pass_without_summary_is_noted():
    io = KernelIO({"outputs": [{"output_type": "stream", "text": "nothing useful\n"}]})
    store = DraftStore(graded=[], needing=[], budget_rows=[ROW_NORMAL])
    rep = run(io, store, draft=True, watched=[W1])
    assert rep.draft["summary"] is None and rep.draft["error"].startswith("the heartbeat run printed no summary") and any("no summary line" in n for n in rep.notes)


def test_tick_draft_pass_reports_client_deadline_and_kernel_errors():
    io = KernelIO({"status": "pending", "outputs": [{"output_type": "stream", "text": '{"event": "replaying"}\n'}], "error": None})
    rep = run(io, DraftStore(graded=[], needing=[], budget_rows=[ROW_NORMAL]), draft=True, watched=[W1])
    assert rep.draft["kernel_status"] == "pending" and "client deadline" in rep.draft["error"] and rep.draft["events"] == ["replaying"]
    assert io.keep_session is True                                   # run_tick's close() must not kill the running heartbeat
    io = KernelIO({"status": "error", "outputs": [], "error": {"ename": "ImportError", "evalue": "no module named nightshift.ledger"}})
    rep = run(io, DraftStore(graded=[], needing=[], budget_rows=[ROW_NORMAL]), draft=True, watched=[W1])
    assert rep.draft["error"] == "ImportError: no module named nightshift.ledger" and any("raised ImportError" in n for n in rep.notes)


def test_tick_draft_pass_runs_even_when_the_window_already_ticked():
    out = {"outputs": [{"output_type": "stream", "text": json.dumps(SUMMARY) + "\n"}]}
    io = KernelIO(out)
    store = DraftStore(graded=[], needing=[], budget_rows=[ROW_NORMAL], replayed=True)
    rep = run(io, store, draft=True, watched=[W1])
    assert rep.tier == "unchanged" and rep.draft["entries"] == 1 and len(io.codes) == 1
    assert not any(s.startswith(f"INSERT INTO {OPS}.ticks") for s in store.sql)   # no second tick row for the window


def test_tick_draft_pass_runs_after_the_tick_row():
    out = {"outputs": [{"output_type": "stream", "text": json.dumps(SUMMARY) + "\n"}]}
    io = KernelIO(out)
    store = DraftStore(graded=[], needing=[], budget_rows=[ROW_NORMAL])
    run(io, store, draft=True, watched=[W1])
    tick_at = store.idx(f"INSERT INTO {OPS}.ticks")
    select_at = store.idx(") g")
    assert select_at > tick_at
