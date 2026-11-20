# Night Shift heartbeat - an Oracle AI Data Platform scheduled job (Python task on the workspace cluster, every 30 minutes).
#
# This is the in-cluster half of the agent. Each tick it:
#   0. derives an idempotent tick id from the 30-minute window and exits if that window already ran; sweeps sandbox
#      schemas left behind by a run the platform killed (older than SBX_STALE_HOURS);
#   1. reads the budget ledger and computes the compute tier (hysteresis against the previous tick);
#   2. counts the unconsumed wake events (the external actuator is the one that claims and acts on them);
#   3. records flight-recorder evidence from this cluster's own Spark UI;
#   4. re-grades every watched production statement with the hazard-replay harness on a SHALLOW CLONE
#      (late, duplicate, out of order, retried, revision) in a sandbox schema private to this run, and writes a
#      replay_verdicts row that is read back before it is trusted; a check that exercised no row makes the verdict
#      insufficient_coverage, never replay_safe, and a table over NS_REPLAY_MAX_ROWS is reported, not replayed;
#   5. for a statement that is silently wrong, asks the model for ONE replacement statement, validates it through the
#      platform parser allowlist (pinned to the declared target AND source), replays it the same way, records the
#      draft and its verdict, and, only if it is replay_safe, passes a statement_repair action through the policy
#      gate, which quarantines it to a human; a draft text a person already decided on is never re-proposed. Every
#      model call of a run is made before its first replay (NS_DRAFT_BEFORE_REPLAY, on by default);
#   6. drops the run's sandbox schema and writes the tick row, also when a step failed (the error is in the summary).
# Trust boundary: the package and the watch list are read from a workspace folder and model output is untrusted text;
# nothing is executed before nightshift.watch.checked_watch accepts it, and then only inside the sandbox harness.
# Control-plane reads and writes (job runs, cluster bodies, approvals execution) are not possible from inside a task
# (a task kernel has no control-plane signer); the external actuator does those against the same ledger.
# Nothing here changes the control plane; applying an approved repair means writing a runbook row.
try:
    spark  # noqa: F821  (provided by the task runtime)
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

PKG = os.environ.get("NS_PKG", "/Workspace/Shared/nightshift/pkg")   # the actuator's pass points this at a private copy it sent itself
if PKG not in sys.path:
    sys.path.insert(0, PKG)
from nightshift import draft as D  # noqa: E402
from nightshift import gate, ledger, replay, tiers  # noqa: E402
from nightshift.recorder import recorder_cell  # noqa: E402
from nightshift.drain import drain_approvals  # noqa: E402
from nightshift.types import Action, TickContext  # noqa: E402
from nightshift.watch import checked_watch, require_delta, table_columns  # noqa: E402

OPS = os.environ.get("NS_OPS", "default.nightshift_ops")        # ledger location (catalog.schema); the DDL and the approvals notebook use the same value
SBX_BASE = os.environ.get("NS_SBX", "default.nightshift_sbx")  # per-run schema = <base>_<tick8>_<start minute>
WATCHED = os.environ.get("NS_WATCHED", "/Workspace/Shared/nightshift/watched_merges.json")  # [{"table","statement","ts","source"?,"key"?,"draft"?}]
DAILY_BUDGET_EST = 20.0                                         # placeholder units; a shape x hours estimate until a metering source is wired in
HAZARDS = list(replay.HAZARDS)
JOB_NAME = os.environ.get("NS_JOB_NAME", "nightshift_heartbeat")
DRAFT_ATTEMPTS = int(os.environ.get("NS_DRAFT_ATTEMPTS", "2"))
FORCE_TICK = os.environ.get("NS_FORCE_TICK") == "1"             # a manual re-run inside the same half hour ignores the idempotent window (its tick id is salted)
TASK_BUDGET_S = int(os.environ.get("NS_TASK_BUDGET_S", "1800"))  # the job's timeout; a replay that cannot finish inside it is not started
DRAFT_ONLY = os.environ.get("NS_DRAFT_ONLY") == "1"               # the actuator's drafting pass: no flight recorder, just grade/reuse + draft
REUSE_VERDICT_H = float(os.environ.get("NS_REUSE_VERDICT_H", "1") or 0)  # >0: reuse a settled verdict this recent (hours) instead of replaying again; the scheduled job reuses the last tick's
DRAFT_BEFORE_REPLAY = os.environ.get("NS_DRAFT_BEFORE_REPLAY", "1") == "1"  # model calls only before any replay in the run; on some instances ai_generate fails inside a task after heavy Spark work. It works by drafting from the previous tick's verdict, so it needs NS_REUSE_VERDICT_H > 0; with 0 a run drafts right after its own replay
MAX_REPLAY_ROWS = int(os.environ.get("NS_REPLAY_MAX_ROWS", "2000000"))        # a replay stages the whole table through one global sort and re-applies the statement once per delivery; a larger table is reported as skipped
TASK_MARGIN_S = 150
SBX_STALE_HOURS = 3
SBX_SWEEP_MAX_DAYS = 30                                           # the sweep leaves anything older alone: a number that far back is a date stamp, not a start minute
sbx_catalog, sbx_schema = SBX_BASE.split(".", 1) if "." in SBX_BASE else ("default", SBX_BASE)
SBX_NAME_RE = re.compile(rf"^{re.escape(sbx_schema)}_[0-9a-f]{{8}}_(\d+)$")  # exactly the name a run gives its sandbox, built from the configured base so the sweep follows NS_SBX
ORDERABLE = ("timestamp", "date", "number")


def rows(sql):
    return [tuple(r) for r in spark.sql(sql).collect()]


def run(sql):
    spark.sql(sql)


def utcnow():
    return datetime.now(timezone.utc)


def sha16(*parts):
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]


def log(event, **kw):
    """One JSON line per step, flushed, so the job output or a notebook cell shows the tick's progress as it runs."""
    print(json.dumps({"event": event, **kw}, default=str), flush=True)


def err_text(e, n=200):
    first = str(e).splitlines()[0] if str(e) else ""
    return f"{type(e).__name__}: {first[:n]}"


now = utcnow()
floored = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
tick_id = hashlib.sha256(f"{floored.isoformat()}|{'measure|' + now.isoformat() if FORCE_TICK else 'heartbeat'}".encode()).hexdigest()
RUN_NONCE = tick_id[:16] + "-" + hashlib.sha256(f"{now.isoformat()}|{os.getpid()}|{time.time_ns()}".encode()).hexdigest()[:8]
summary = {"tick_id": tick_id[:16], "window": floored.isoformat(), "job": JOB_NAME, "run_nonce": RUN_NONCE}

# 0. idempotent window key: a second run inside the same half hour does nothing. The rest of the tick sits under
# `if not ALREADY:` rather than behind sys.exit(): the task runtime treats SystemExit as a failed run.
ALREADY = False
if not FORCE_TICK and rows(f"SELECT 1 FROM {OPS}.ticks WHERE tick_id = '{tick_id}' LIMIT 1"):
    summary["result"] = "window already recorded; nothing done"
    print(json.dumps(summary))
    ALREADY = True
if not ALREADY:

    t_start = time.time()
    start_minute = int(now.timestamp()) // 60
    RUN_SBX = f"{sbx_catalog}.{sbx_schema}_{tick_id[:8]}_{start_minute}"
    summary["sandbox"] = RUN_SBX


    def time_left():
        return TASK_BUDGET_S - (time.time() - t_start)


    # 0b. sweep sandbox schemas a killed run left behind (a Python finally never runs when the platform kills the task);
    # the start minute is in the schema name, so only schemas older than SBX_STALE_HOURS are touched. The sweep drops
    # with CASCADE, so a name has to be exactly a run's and its minute a recent one: a team schema that only shares
    # the base (<base>_reports_20250131_2, <base>_20250101_20250131) is not this job's to drop
    swept = []
    try:
        for r in rows(f"SHOW SCHEMAS IN {sbx_catalog} LIKE '{sbx_schema}*'"):
            name = str(r[0])
            m = SBX_NAME_RE.match(name)
            if m and SBX_STALE_HOURS * 60 <= start_minute - int(m.group(1)) <= SBX_SWEEP_MAX_DAYS * 24 * 60:
                run(f"DROP SCHEMA IF EXISTS {sbx_catalog}.{name} CASCADE")
                swept.append(name)
    except Exception as e:  # noqa: BLE001  (housekeeping; never blocks the tick)
        summary["sandbox_sweep_error"] = err_text(e, 120)
    summary["sandbox_swept"] = swept

    # 1. budget -> tier (previous tick's tier read back from the ledger for hysteresis)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    b = rows(ledger.budget_summary_sql(OPS, month_start))
    used_cost = float(b[0][1] or 0.0) if b else 0.0
    last_entry = b[0][4] if b else None
    days = max(1, now.day)
    remaining_pct = max(0.0, 100.0 * (1.0 - used_cost / (DAILY_BUDGET_EST * days))) if last_entry is not None else None
    source = "ledger" if last_entry is not None else "unknown"
    prev = rows(f"SELECT compute_tier FROM {OPS}.ticks ORDER BY started_at DESC LIMIT 1")
    prev_tier = prev[0][0] if prev else None
    ctx = TickContext(now_utc=now, budget_remaining_pct=remaining_pct, projected_overrun_pct=0.0, llm_remaining_pct=None,
                      budget_source=source, kv={"tier": None})
    tier, kv_updates = tiers.compute_tier(ctx, prev_tier, {})
    ctx.kv["tier"] = tier
    summary.update({"tier": tier, "prev_tier": prev_tier, "remaining_pct": remaining_pct, "budget_source": source})

    # 2. wake events are only counted here: the external actuator claims them when it plans on them
    try:
        summary["wake_events_pending"] = int(rows(f"SELECT COUNT(*) FROM {OPS}.wake_events WHERE consumed_by_tick IS NULL")[0][0])
    except Exception as e:  # noqa: BLE001
        summary["wake_events_pending"] = f"unknown: {err_text(e, 80)}"

    # 3. flight recorder from this cluster's Spark UI (the generated cell prints its own summary)
    if DRAFT_ONLY:
        summary["flight_recorder"] = "skipped: draft-only run"
    else:
        try:
            fr_ns = {"spark": spark, "__name__": "__fr__"}
            exec(compile(recorder_cell(OPS, JOB_NAME, tick_id[:12], max_stages=100), "flight_recorder_cell", "exec"), fr_ns)
            # the cell catches its own errors and prints them, so "it ran" is not "it recorded": report what it reported
            fr = fr_ns.get("_ns_summary") or {}
            summary["flight_recorder"] = "ok" if fr.get("ok") else f"skipped: {str(fr.get('error') or 'the cell left no summary')[:120]}"
        except Exception as e:  # noqa: BLE001  (evidence is best effort; the tick must still be recorded)
            summary["flight_recorder"] = f"skipped: {err_text(e, 120)}"


    # 4. replay-verify the watched statements
    def execute_plan(plan):
        """Run a spark_sql_plan: work statements in order, cleanup always; returns the verdict dict."""
        work = [s for s in plan if replay.parse_plan_tag(s)[0] != "cleanup"]
        cleanup = [s for s in plan if replay.parse_plan_tag(s)[0] == "cleanup"]
        v = {"passes_snapshot": False, "passes_run_twice": False, "hazards": {}, "diff_rows": 0, "verdict": "crash",
             "diffs": {}, "error": None, "source_version": None, "statements_run": 0, "elapsed_s": None,
             "coverage": None, "uncovered": []}
        t0 = time.time()
        try:
            for st in work:
                stage, name, sql = replay.parse_plan_tag(st)
                res = spark.sql(sql)
                v["statements_run"] += 1
                if stage == "pin" and name == "history":
                    hist = res.collect()
                    if hist:
                        v["source_version"] = max(int(r["version"]) for r in hist)
                elif stage == "coverage":
                    got = res.collect()
                    v["coverage"] = replay.coverage_from_row(got[0] if got else None)
                elif stage == "diff":
                    v["diffs"][name] = int(res.collect()[0][0])
            # one rule for the local harness and this plan: replay_safe needs every check to pass AND to have
            # exercised at least one row; a pass with a vacuous check is insufficient_coverage
            v.update(replay.settle_verdict(v["diffs"], HAZARDS, v["coverage"]))
            if v["verdict"] == replay.VERDICT_INSUFFICIENT:
                # a check that exercised no row, or a column the corrections could not change (reported by name)
                fixed = [u[len(replay.UNREVISED_PREFIX):] for u in v["uncovered"] if u.startswith(replay.UNREVISED_PREFIX)]
                idle = [u for u in v["uncovered"] if not u.startswith(replay.UNREVISED_PREFIX)]
                why = []
                if idle:
                    why.append(f"no row exercised {', '.join(idle)} ({(v['coverage'] or {}).get('n_rows', 0)} row(s) in the table)")
                if fixed:
                    why.append(f"a correction cannot change {', '.join(fixed)} (array, map, struct or binary), "
                               "so an UPDATE that forgets it is not caught")
                v["error"] = "insufficient coverage: " + "; ".join(why)
        except Exception as e:  # noqa: BLE001
            v["error"] = err_text(e, 160)
        finally:
            for st in cleanup:
                try:
                    spark.sql(replay.parse_plan_tag(st)[2])
                except Exception as e:  # noqa: BLE001
                    v.setdefault("cleanup_errors", []).append(str(e)[:120])
            v["elapsed_s"] = round(time.time() - t0, 1)
        return v


    def reuse_verdict(statement_sha):
        """The latest verdict for this statement inside REUSE_VERDICT_H hours, rebuilt into the shape grade() returns, or
        None. Only a settled verdict is reused (replay_safe, silent_wrong, snapshot_wrong, insufficient_coverage, the last
        with the explanation the ledger holds); a crash is regraded."""
        if REUSE_VERDICT_H <= 0:
            return None
        r = rows(ledger.latest_verdict_for_statement_sql(OPS, statement_sha, REUSE_VERDICT_H, utcnow()))
        if not r or str(r[0][1]) not in ("replay_safe", "silent_wrong", "snapshot_wrong", "insufficient_coverage"):
            return None
        verdict_id, verdict, diffs_json, hazards_json, elapsed_s, created_at = r[0][:6]
        recorded_error = r[0][6] if len(r[0]) > 6 else None
        diffs = json.loads(diffs_json) if isinstance(diffs_json, str) else dict(diffs_json or {})
        hazards = json.loads(hazards_json) if isinstance(hazards_json, str) else dict(hazards_json or {})
        if not isinstance(diffs, dict) or not isinstance(hazards, dict):
            return None
        v = {"passes_snapshot": diffs.get("baseline", 1) == 0, "passes_run_twice": diffs.get("run_twice", 1) == 0,
             "hazards": {h: bool(ok) for h, ok in hazards.items()}, "diff_rows": int(sum(int(x) for x in diffs.values())),
             "verdict": str(verdict), "diffs": {k: int(x) for k, x in diffs.items()},
             "error": recorded_error if str(verdict) == "insufficient_coverage" else None, "source_version": None,
             "statements_run": 0, "elapsed_s": float(elapsed_s or 0.0), "ledger": f"reused:{verdict_id}", "reused_from": str(created_at)}
        return v, str(verdict_id)


    def table_rows(table):
        return int(rows(f"SELECT COUNT(*) FROM {table}")[0][0])


    def grade(table, ts_col, keys, template, cols, candidate, statement_sha, n_rows=None):
        """Replay one statement, write its replay_verdicts row and read it back; returns (verdict dict, verdict_id).
        v['ledger'] is 'ok' only when the row that was read back carries the verdict that was computed. The row count
        sizes the deliveries (a table smaller than the batch count gets one row per delivery, never an empty one)."""
        n_rows = table_rows(table) if n_rows is None else n_rows
        plan = replay.spark_sql_plan(table, RUN_SBX, template, keys, ts_col, HAZARDS, replay.HazardConfig(), columns=cols,
                                     n_rows=n_rows)
        v = execute_plan(plan)
        verdict_id = sha16(tick_id, table.lower(), statement_sha, candidate, ts_col, ",".join(sorted(keys)))
        v["ledger"] = "not_written"
        try:
            run(ledger.insert_replay_verdict(OPS, {
                "verdict_id": verdict_id, "tick_id": tick_id, "table_name": table, "statement_sha256": statement_sha,
                "candidate": candidate, "source_version": v["source_version"], "verdict": v["verdict"],
                "passes_snapshot": v["passes_snapshot"], "passes_run_twice": v["passes_run_twice"], "hazards_json": v["hazards"],
                "diffs_json": v["diffs"], "diff_rows": v["diff_rows"], "statements_run": v["statements_run"],
                "elapsed_s": float(v["elapsed_s"] or 0.0), "error": v["error"], "provenance": JOB_NAME}, utcnow()))
            back = rows(f"SELECT verdict FROM {OPS}.replay_verdicts WHERE verdict_id = '{verdict_id}'")
            v["ledger"] = "ok" if back and str(back[0][0]) == v["verdict"] else "mismatch"
        except Exception as e:  # noqa: BLE001
            v["ledger"] = f"write_failed: {err_text(e, 120)}"
        return v, verdict_id


    # 5. drafting: gate plumbing shared with the external actuator (decision row first, fail closed)
    def write_decision(decision, action, provenance="heartbeat", principal=None):
        did = sha16(tick_id, decision.action_signature, decision.decision, decision.rule_id)
        run(ledger.insert_decision(OPS, {
            "decision_id": did, "tick_id": tick_id, "action_signature": decision.action_signature,
            "action_kind": action.kind, "target": action.target,
            "args_sha256": hashlib.sha256(json.dumps(action.params, sort_keys=True, default=str).encode()).hexdigest(),
            "decision": decision.decision, "rule_id": decision.rule_id, "rules_evaluated": decision.evaluated_rules,
            "rules_triggered": [decision.rule_id] if decision.rule_id else [], "reason": decision.reason,
            "provenance": provenance, "principal": principal or JOB_NAME, "latency_ms": decision.latency_ms}, now=utcnow()))
        return did


    def decision_counter(rule_id, target, window_hours=24):
        r = rows(ledger.rate_count_sql(OPS, rule_id, target, window_hours, now=utcnow()))
        return int(r[0][0] or 0) if r else 0


    def audit_sink(d, a):
        # a decision written while re-checking a row a person approved is marked as such: only a decision from the
        # planning path (a draft just proven) can stand behind an approval (ledger.quarantine_trace_sql)
        rechecking = d.action_signature in (ctx.kv.get("approved_signatures") or {})
        write_decision(d, a, provenance=ledger.DRAIN_PROVENANCE if rechecking else "heartbeat")


    engine = gate.PolicyEngine(gate.default_rules(), audit_sink, decision_counter=decision_counter)


    # 3c. approvals a person decided since the last tick, job side. A statement repair is a ledger write
    # (the draft becomes a runbook row), so the heartbeat drains those itself and the loop closes inside the platform:
    # draft -> approval in the Workbench notebook -> next tick. Control-plane actions (cluster_*) need the signer only the
    # external actuator has and are left for it. The gate re-evaluates every approved action; the verdict must be fresh.
    class _LedgerStore:
        run_sql = staticmethod(rows)


    APPROVERS = [a for a in os.environ.get("NS_APPROVERS", "").split(",") if a.strip()]   # optional allowlist of approver identities
    try:
        summary["approvals_drained"] = drain_approvals(_LedgerStore(), OPS, ctx, engine, write_decision, tick_id, utcnow(), log=log, approvers=APPROVERS)
    except Exception as e:  # noqa: BLE001  (the drain is one step of the tick: the statements are still graded and the tick row written)
        summary["approvals_drained"] = {"expired": None, "executed": [], "left": [], "errors": [f"drain failed: {err_text(e, 120)}"]}
        log("approval_drain_failed", error=err_text(e, 120))


    def record_draft(row, when=None):
        run(ledger.insert_repair_draft(OPS, row, when or utcnow()))


    DEFERRED_PROOFS = []        # replays of validated drafts, run after the last model call of the run (see draft_repairs)
    DRAFTING_NOW = set()        # statements this run already drafts for (a watch list can name one statement twice)
    RESERVED = {"s": 0.0}       # replay seconds promised to the queued proofs; no draft is asked for beyond what still fits


    def _draft_counts(statement_sha):
        today = rows(ledger.drafts_today_sql(OPS, statement_sha, utcnow(), runner=JOB_NAME))
        return (int(today[0][0] or 0), int(today[0][1] or 0), int(today[0][2] or 0)) if today else (0, 0, 0)


    def _draft_skip(counts):
        """Why this statement is not drafted for now, or None."""
        _, n_safe, n_failed_recent = counts
        if n_safe:
            return "a replay_safe draft with a live approval already exists for this statement today"
        if n_failed_recent:
            return f"{n_failed_recent} model call(s) for this statement failed in the last hour; backing off"
        return None


    def _size_problem(n_rows):
        """Why a draft for a table of this size could not be proven, or None. The same row cap as the base replay, and
        the floor under which a hazard has nothing to act on (such a draft could only ever grade insufficient_coverage)."""
        if n_rows > MAX_REPLAY_ROWS:
            return f"{n_rows} rows is over NS_REPLAY_MAX_ROWS ({MAX_REPLAY_ROWS}); a draft would not be replayed"
        if n_rows < 2:
            return f"the table has {n_rows} row(s); no draft can be proven replay_safe on it"
        return None


    def _release_lease(claim_id):
        try:
            run(ledger.claim_release_sql(OPS, claim_id, RUN_NONCE, utcnow()))
        except Exception as e:  # noqa: BLE001  (a stale lease is re-armable after CLAIM_LEASE_MINUTES)
            log("claim_release_failed", claim_id=claim_id, error=err_text(e, 120))


    def draft_repairs(w, table, ts_col, keys, cols, statement, statement_sha, base_verdict, source, defer_replay=False):
        """Ask the model for a replacement, validate, replay, record; open an approval only for a replay_safe draft.
        With defer_replay the model call and its validation happen now and the replay is queued in DEFERRED_PROOFS,
        which the tick runs after the last model call of the run; the lease is held until that replay is done."""
        out = {"attempts": [], "approval_opened": False}

        def skipped(reason):
            out["skipped"] = reason
            log("drafting_skipped", table=table, reason=reason)
            return out

        model = D.DRAFT_MODEL_BY_TIER.get(tier)
        if not model:
            out["skipped"] = f"no drafting model at tier {tier}"
            return out
        if statement_sha in DRAFTING_NOW:
            return skipped("this run is already drafting for this statement (it is listed more than once)")
        skip = _draft_skip(_draft_counts(statement_sha)) or _size_problem(table_rows(table))
        if skip:
            return skipped(skip)
        # one runner per statement at a time: a lease row (insert-if-absent, re-armed only when released or stale) whose
        # tick_id column carries the holder's run nonce. The scheduled job, the actuator's drafting pass and a second copy
        # of either can otherwise all reach this point for the same statement; the lease covers the model call AND the
        # draft's replay.
        claim_id = ledger.claim_id_for(statement_sha)
        run(ledger.insert_repair_draft(OPS, {
            "draft_id": claim_id, "tick_id": RUN_NONCE, "incident_key": w.get("incident_key"), "table_name": table,
            "statement_sha256": statement_sha, "model": model, "attempt": -1, "prompt_sha256": "", "draft_sql": "",
            "draft_sha256": claim_id, "validation": "claim", "status": "drafting", "provenance": JOB_NAME}, utcnow()))
        release_here = True   # the release is conditional on this run's nonce, so it is a no-op when the lease is not ours
        try:
            run(ledger.claim_take_over_sql(OPS, claim_id, RUN_NONCE, JOB_NAME, utcnow()))
            owner = rows(ledger.claim_owner_sql(OPS, claim_id))
            held_by, holder, status = (str(owner[0][0]), str(owner[0][1]), str(owner[0][2])) if owner else ("", "unknown", "")
            if held_by != RUN_NONCE or status != "drafting":
                return skipped(f"another run ({holder}, {status} since {owner[0][3] if owner else '?'}) holds this statement's drafting lease")
            DRAFTING_NOW.add(statement_sha)
            # counted again now that the lease is held: the counts read above can be stale by a whole draft when
            # another runner finished one between that read and this claim, and the daily cap is enforced on these
            counts = _draft_counts(statement_sha)
            skip = _draft_skip(counts)
            if skip:
                return skipped(skip)
            n_today = counts[0]
            job = {"w": w, "table": table, "ts_col": ts_col, "keys": keys, "cols": cols, "statement": statement,
                   "statement_sha": statement_sha, "base_verdict": base_verdict, "source": source, "model": model, "n_today": n_today}
            failed = [h for h, ok in base_verdict["hazards"].items() if not ok]
            log("drafting", table=table, model=model, failed_hazards=failed, drafts_today=n_today)
            if not defer_replay:
                # model call, replay, next model call: a later attempt can follow a failed replay inside the same run
                for attempt in range(DRAFT_ATTEMPTS):
                    if _attempt_blocked(job, n_today, out):
                        break
                    asked = _ask(job, out, attempt)
                    if asked is not None and _prove(job, out, asked):
                        break
                return out
            # model call(s) now, replay later: stop at the first reply the parser accepts
            asked = None
            for attempt in range(DRAFT_ATTEMPTS):
                if _attempt_blocked(job, n_today, out):
                    break
                asked = _ask(job, out, attempt)
                if asked is not None:
                    break
            if asked is None:
                return out
            cost = float(base_verdict.get("elapsed_s") or 0.0)
            RESERVED["s"] += cost

            def finish(asked=asked, cost=cost):
                RESERVED["s"] -= cost
                try:
                    _prove(job, out, asked)
                except Exception as e:  # noqa: BLE001
                    out["error"] = err_text(e)
                    log("draft_proof_failed", table=table, draft_id=asked["draft_id"], error=out["error"])
                finally:
                    _release_lease(claim_id)

            DEFERRED_PROOFS.append(finish)
            release_here = False   # finish() releases the lease after the replay
            return out
        finally:
            if release_here:
                _release_lease(claim_id)


    def _attempt_blocked(job, n_today, out):
        """True (and out['skipped'] set) when no further model call may be made for this statement in this run: the
        daily cap is reached, or the draft's replay would not fit in what is left of the task once the replays already
        queued have run (a draft that cannot be replayed is a model call spent for nothing)."""
        base_cost = float(job["base_verdict"].get("elapsed_s") or 0.0)
        reason = None
        if n_today + len(out["attempts"]) >= D.MAX_DRAFTS_PER_STATEMENT_PER_DAY:
            reason = f"{n_today + len(out['attempts'])} drafts today (cap {D.MAX_DRAFTS_PER_STATEMENT_PER_DAY})"
        elif time_left() < RESERVED["s"] + base_cost + TASK_MARGIN_S:
            reason = (f"task budget: {time_left():.0f}s left, {RESERVED['s']:.0f}s already promised to queued replays, "
                      f"a replay takes about {base_cost:.0f}s")
        if reason:
            out["skipped"] = reason
            log("drafting_skipped", table=job["table"], reason=reason)
        return reason is not None


    def _ask(job, out, attempt):
        """One model call for one statement. The reply is untrusted text: it is recorded, reduced to one statement
        and put through the parser allowlist. Returns the validated draft for _prove, or None when the call failed
        or the reply was rejected (the attempt is recorded either way). No replay happens here."""
        w, table, ts_col, keys, cols = job["w"], job["table"], job["ts_col"], job["keys"], job["cols"]
        statement, statement_sha, base_verdict, source, model = (job["statement"], job["statement_sha"], job["base_verdict"],
                                                                 job["source"], job["model"])
        failed = [h for h, ok in base_verdict["hazards"].items() if not ok]
        rec = {"attempt": attempt, "model": model}
        # the prompt's wording rotates with the drafts already made today, not with the position in this run: a run that
        # asks once per tick would otherwise send the identical prompt every time
        prompt = D.build_prompt(statement, table, source, keys, ts_col, cols, failed, base_verdict["diffs"], job["n_today"] + attempt)
        row = {"tick_id": tick_id, "incident_key": w.get("incident_key"), "table_name": table, "statement_sha256": statement_sha,
               "model": model, "attempt": attempt, "prompt_sha256": D.sha(prompt), "provenance": JOB_NAME}
        t0 = time.time()
        try:
            raw = rows(D.ai_generate_sql(model, prompt))[0][0]
        except Exception as e:  # noqa: BLE001  (recorded as a failed call: it does not count toward the daily cap, but it backs this runner off for an hour)
            detail = " | ".join(l.strip() for l in str(e).splitlines() if l.strip())  # the cause is rarely on line one
            msg = f"ai_generate failed: {type(e).__name__}: {detail[:600]}"
            row.update({"draft_sql": msg, "draft_sha256": D.sha(msg), "validation": f"rejected: {msg}"[:200]})
            row["draft_id"] = sha16(tick_id, attempt, statement_sha, row["draft_sha256"], model)
            record_draft(row)
            log("draft_failed", table=table, attempt=attempt, model=model, error=msg[:300])
            rec["error"] = msg
            out["attempts"].append(rec)
            return None
        rec["llm_s"] = round(time.time() - t0, 1)
        draft_sql = D.extract_sql(raw)
        draft_sha = D.sha(draft_sql or str(raw))
        draft_id = sha16(tick_id, attempt, statement_sha, draft_sha, model)
        row.update({"draft_id": draft_id, "draft_sql": draft_sql or str(raw)[:4000], "draft_sha256": draft_sha})
        rec["draft_id"] = draft_id
        log("draft_received", table=table, attempt=attempt, model=model, llm_s=rec["llm_s"], draft_id=draft_id,
            draft_sql=(draft_sql or str(raw))[:400])
        if not draft_sql:
            row["validation"] = "rejected: no MERGE statement in the reply"
            record_draft(row)
            rec["validation"] = row["validation"]
            out["attempts"].append(rec)
            return None
        try:
            dt, dts, dkeys, da = checked_watch(spark, {"table": table, "ts": ts_col, "source": source, "key": keys,
                                                       "statement": draft_sql}, require_source=True)
            row["validation"] = "accepted"
        except Exception as e:  # noqa: BLE001
            row["validation"] = f"rejected: {str(e)[:160]}"
            log("draft_rejected", draft_id=draft_id, validation=row["validation"])
            record_draft(row)
            rec["validation"] = row["validation"]
            out["attempts"].append(rec)
            return None
        log("draft_validated", draft_id=draft_id, validation="accepted", replaying=HAZARDS)
        return {"rec": rec, "row": row, "draft_sha": draft_sha, "draft_id": draft_id, "template": da.template, "attempt": attempt}


    def _prove(job, out, asked):
        """Replay a validated draft under the same hazards, record it with its verdict, and open an approval when it
        is replay_safe. Returns True when no further attempt should be made for this statement in this run."""
        w, table, ts_col, keys, cols = job["w"], job["table"], job["ts_col"], job["keys"], job["cols"]
        statement_sha, model = job["statement_sha"], job["model"]
        rec, row, draft_sha, draft_id, attempt = asked["rec"], asked["row"], asked["draft_sha"], asked["draft_id"], asked["attempt"]
        base_cost = float(job["base_verdict"].get("elapsed_s") or 0.0)

        def unreplayed(key, reason):
            # the reply is kept as a row (accepted by the parser, never replayed: status 'validated', no verdict); it
            # counts toward today's cap and a later run drafts again
            row["status"] = "validated"
            record_draft(row)
            rec.update({"validation": "accepted", key: reason})
            out["attempts"].append(rec)
            log("draft_replay_skipped" if key == "replay_skipped" else "draft_proof_failed", table=table, draft_id=draft_id, reason=reason)
            return True

        try:
            n_rows = table_rows(table)
        except Exception as e:  # noqa: BLE001
            return unreplayed("error", f"row count failed: {err_text(e)}")
        problem = _size_problem(n_rows)
        if not problem and time_left() < base_cost + TASK_MARGIN_S:
            problem = f"task budget: {time_left():.0f}s left, a replay takes about {base_cost:.0f}s"
        if problem:
            return unreplayed("replay_skipped", problem)
        try:
            v, vid = grade(table, ts_col, keys, asked["template"], cols, f"draft:{model}:{attempt}", draft_sha, n_rows=n_rows)
        except Exception as e:  # noqa: BLE001  (the accepted reply must not be lost, or the daily cap would never engage)
            return unreplayed("error", f"replay failed: {err_text(e)}")
        row.update({"verdict_id": vid, "verdict": v["verdict"], "diff_rows": v["diff_rows"]})
        record_draft(row)
        log("draft_graded", draft_id=draft_id, verdict=v["verdict"], diffs=v["diffs"], replay_s=v["elapsed_s"], ledger=v["ledger"])
        rec.update({"validation": "accepted", "verdict": v["verdict"], "diffs": v["diffs"], "replay_s": v["elapsed_s"],
                    "ledger": v["ledger"]})
        out["attempts"].append(rec)
        if v["verdict"] != "replay_safe":
            return False
        if v["ledger"] != "ok":
            rec["approval_skipped"] = f"verdict row not confirmed in the ledger ({v['ledger']}); no approval opened"
            return False
        decided = rows(ledger.decided_draft_sql(OPS, draft_sha))
        if decided:
            rec["approval_skipped"] = f"this draft text already has approval {decided[0][0]} ({decided[0][1]})"
            log("approval_skipped", draft_id=draft_id, reason=rec["approval_skipped"])
            return True
        # a replay-safe draft becomes a statement_repair action; the gate quarantines it to a person. The verdict the
        # gate sees is the one read back from the ledger, not the in-memory dict.
        ctx.kv["replay_verdicts"] = {draft_sha: {"verdict": "replay_safe", "age_hours": 0.0}}
        action = Action("statement_repair", table, D.draft_action_params(table, statement_sha, draft_id, draft_sha, model), reversible=True)
        d = engine.evaluate(action, ctx)
        rec["gate"] = {"decision": d.decision, "rule": d.rule_id, "reason": d.reason}
        if d.decision != "quarantine":
            return False
        approval_id = sha16(tick_id, action.signature)
        # the approval names the quarantine decision the gate just wrote for this action; the drain executes an approved
        # row only when it is the one row that names such a decision (a row written straight into the table names none)
        run(ledger.insert_approval(OPS, {
            "approval_id": approval_id, "tick_id": tick_id, "action_signature": action.signature,
            "decision_id": sha16(tick_id, d.action_signature, d.decision, d.rule_id),
            "action_json": {"kind": action.kind, "target": action.target, "params": action.params, "previous_value": None, "reversible": True},
            "incident_key": w.get("incident_key"), "note": f"{d.rule_id}: {d.reason} | replay_safe draft {draft_id} replaces statement {statement_sha[:12]}",
            "expires_at": utcnow() + timedelta(hours=24), "status": "pending"}, now=utcnow()))
        run(ledger.update_draft_status_sql(OPS, draft_id, "proposed", approval_id=approval_id, now=utcnow(), only_from=("graded",)))
        out["approval_opened"] = True
        rec["approval_id"] = approval_id
        log("approval_opened", approval_id=approval_id, draft_id=draft_id, rule=d.rule_id, reason=d.reason)
        return True


    verdicts = []
    failure = None
    try:
        try:
            with open(WATCHED, encoding="utf-8") as f:
                watched = json.load(f)
        except Exception as e:  # noqa: BLE001
            watched = []
            summary["watched_merges"] = f"not read: {type(e).__name__}"
        if not isinstance(watched, list):
            summary["watched_merges"] = f"ignored: top level is {type(watched).__name__}, expected a list"
            watched = []

        prepared = []
        for w in watched:
            if not isinstance(w, dict):
                verdicts.append({"table": None, "verdict": "rejected", "error": f"entry is {type(w).__name__}, not an object"})
                continue
            try:
                table, ts_col, keys, a = checked_watch(spark, w)
                # the watched name must be fully qualified so the comparison below is exact (a 2-part name would be
                # resolved against the session's current catalog and could alias the ledger or a sandbox schema)
                parts = table.lower().split(".")
                if len(parts) != 3:
                    raise ValueError("watched table must be written as catalog.schema.table")
                if ".".join(parts[:2]) == OPS.lower() or (parts[0] == sbx_catalog.lower() and parts[1].startswith(sbx_schema.lower())):
                    raise ValueError("the agent's own ledger and sandbox schemas are never watched")
                require_delta(spark, table)
                cols = table_columns(spark, table)
                types = dict(cols)
                if ts_col not in types or any(k not in types for k in keys):
                    raise ValueError("ts/key columns not found in the table")
                if replay._spark_type_family(types[ts_col]) not in ORDERABLE:
                    raise ValueError(f"ordering column {ts_col!r} has type {types[ts_col]!r}; it must be timestamp, date or numeric")
                source = w.get("source") or a.source
            except Exception as e:  # noqa: BLE001  (a bad watch entry is recorded, never executed)
                verdicts.append({"table": w.get("table"), "verdict": "rejected", "error": err_text(e)})
                continue
            statement = str(w["statement"]).strip()
            prepared.append({"w": w, "table": table, "ts_col": ts_col, "keys": keys, "a": a, "cols": cols, "source": source,
                             "statement": statement, "sha": D.sha(statement)})

        def _entry(v):
            return {"table": None, "verdict": v["verdict"], "diffs": v["diffs"], "elapsed_s": v["elapsed_s"], "error": v["error"], "ledger": v["ledger"],
                    "coverage": v.get("coverage")}

        # pass 1: statements with a reusable settled verdict draft FIRST, so every model call in this run happens before
        # any replay work (on some instances ai_generate fails inside a task once heavy Spark work has run). With
        # NS_DRAFT_BEFORE_REPLAY the drafts' own replays are queued and run after the last statement has been asked.
        pending_grade = []
        for it in prepared:
            w, table, statement_sha = it["w"], it["table"], it["sha"]
            try:
                reused = reuse_verdict(statement_sha)
            except Exception as e:  # noqa: BLE001
                verdicts.append({"table": table, "verdict": "crash", "error": err_text(e)})
                continue
            if not reused:
                pending_grade.append(it)
                continue
            v, vid = reused
            log("verdict_reused", table=table, statement_sha=statement_sha[:12], verdict=v["verdict"], diffs=v["diffs"],
                verdict_id=vid, graded_at=v["reused_from"])
            entry = _entry(v); entry["table"] = table
            if v["verdict"] == "silent_wrong" and w.get("draft", True):
                try:
                    entry["drafting"] = draft_repairs(w, table, it["ts_col"], it["keys"], it["cols"], it["statement"], statement_sha, v, it["source"],
                                                      defer_replay=DRAFT_BEFORE_REPLAY)
                except Exception as e:  # noqa: BLE001
                    entry["drafting"] = {"attempts": [], "approval_opened": False, "error": err_text(e)}
            verdicts.append(entry)

        # every model call of this run has been made; the validated drafts are replayed now (each finisher records its
        # own failure in the statement's drafting entry and releases the statement's lease)
        for finish in DEFERRED_PROOFS:
            finish()

        # pass 2: live replays; a silent_wrong verdict found here is drafted on the NEXT tick (pass 1), not after the replay
        for it in pending_grade:
            w, table, statement_sha = it["w"], it["table"], it["sha"]
            if time_left() < 300:
                verdicts.append({"table": table, "verdict": "skipped", "error": f"task budget: {time_left():.0f}s left"})
                continue
            try:
                n_rows = table_rows(table)
                if n_rows > MAX_REPLAY_ROWS:
                    reason = f"{n_rows} rows is over NS_REPLAY_MAX_ROWS ({MAX_REPLAY_ROWS}); not replayed"
                    log("replay_skipped", table=table, statement_sha=statement_sha[:12], reason=reason)
                    verdicts.append({"table": table, "verdict": "skipped", "error": reason})
                    continue
                log("replaying", table=table, candidate=w.get("name", "production"), statement_sha=statement_sha[:12], hazards=HAZARDS,
                    rows=n_rows)
                v, vid = grade(table, it["ts_col"], it["keys"], it["a"].template, it["cols"], w.get("name", "production"), statement_sha,
                               n_rows=n_rows)
                log("statement_graded", table=table, verdict=v["verdict"], diffs=v["diffs"], elapsed_s=v["elapsed_s"], ledger=v["ledger"])
                entry = _entry(v); entry["table"] = table
                if v["verdict"] == "silent_wrong" and w.get("draft", True):
                    if DRAFT_BEFORE_REPLAY and REUSE_VERDICT_H > 0:
                        reason = "model calls run before any replay in a tick; the next tick drafts from this verdict"
                        entry["drafting"] = {"attempts": [], "approval_opened": False, "deferred": reason}
                        log("drafting_deferred", table=table, verdict_id=vid, reason=reason)
                    else:
                        try:
                            entry["drafting"] = draft_repairs(w, table, it["ts_col"], it["keys"], it["cols"], it["statement"], statement_sha, v, it["source"])
                        except Exception as e:  # noqa: BLE001
                            entry["drafting"] = {"attempts": [], "approval_opened": False, "error": err_text(e)}
            except Exception as e:  # noqa: BLE001  (a grading failure is a crash verdict; the tick still lands)
                entry = {"table": table, "verdict": "crash", "error": err_text(e)}
            verdicts.append(entry)
    except Exception as e:  # noqa: BLE001
        failure = e
    summary["replay_verdicts"] = verdicts

    # 6. the run's sandbox goes, then the tick row (the in-cluster tick plans no control-plane actions)
    try:
        run(f"DROP SCHEMA IF EXISTS {RUN_SBX} CASCADE")
        summary["sandbox_dropped"] = True
    except Exception as e:  # noqa: BLE001
        summary["sandbox_dropped"] = f"failed: {err_text(e, 120)}"
    opened = sum(1 for e in verdicts if (e.get("drafting") or {}).get("approval_opened"))
    run(ledger.insert_tick(OPS, {
        "tick_id": tick_id, "started_at": now, "finished_at": utcnow(), "compute_tier": tier,
        "llm_tier": tier, "remaining_pct": remaining_pct, "projected_overrun_pct": 0.0, "llm_remaining_pct": None,
        "budget_source": source, "actions_proposed": opened, "actions_applied": 0, "actions_quarantined": opened,
        "actions_denied": 0, "dry_run": True}, now))
    summary["elapsed_s"] = round(time.time() - t_start, 1)
    summary["result"] = "tick recorded" if failure is None else f"tick recorded with errors: {err_text(failure)}"
    print(json.dumps(summary, default=str))
    if failure is not None:
        raise failure
