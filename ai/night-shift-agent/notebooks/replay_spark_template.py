# Night Shift hazard-replay cell for an Oracle AI Data Platform notebook kernel.
#
# Self-contained: no nightshift import. PLAN is the statement list emitted by
# nightshift.replay.spark_sql_plan(...). Render it with nightshift.replay.render_spark_cell(plan)
# (replaces the plan token on the PLAN line with a literal list) or paste the list by hand.
#
# Each statement starts with a tag line "-- ns:<stage>:<name>". Stages run in order inside a
# try block; every "cleanup" statement runs in the finally block, so the sandbox clone, the
# staging table, the target tables and the batch view never outlive this cell, even when the
# repair crashes. The cell ends by printing one verdict dict: passes_snapshot, passes_run_twice,
# hazards {name: bool}, diff_rows and verdict (replay_safe | silent_wrong | snapshot_wrong |
# insufficient_coverage | crash), plus diffs, coverage (rows each check exercised), uncovered,
# source_version, statements_run, elapsed_s and error details. A statement that passes every check
# while a check exercised no row is insufficient_coverage, never replay_safe. So is one graded on a
# table with a column the corrections cannot change (array, map, struct, binary): the coverage row
# carries a zero count named "revision:<column>" and it is listed in uncovered. Same coverage rule
# as nightshift.replay.settle_verdict, with one difference: this cell grades the hazards its PLAN
# contains, while the scheduled job requires all five (a plan with fewer is silent_wrong there).
import json
import time

PLAN = __NS_PLAN__  # list[str]; filled by render_spark_cell or pasted by hand

TAG_PREFIX = "-- ns:"
HAZARD_NAMES = ("late", "duplicate", "out_of_order", "retried", "revision")


def _parse(statement):
    first, _, rest = statement.partition("\n")
    if first.startswith(TAG_PREFIX):
        stage, _, name = first[len(TAG_PREFIX):].partition(":")
        return stage.strip(), name.strip(), rest
    return "work", "untagged", statement


work = [s for s in PLAN if _parse(s)[0] != "cleanup"]
cleanup = [s for s in PLAN if _parse(s)[0] == "cleanup"]

verdict = {
    "passes_snapshot": False,
    "passes_run_twice": False,
    "hazards": {},
    "diff_rows": 0,
    "verdict": "crash",
    "diffs": {},
    "error": None,
    "source_version": None,
    "statements_run": 0,
    "elapsed_s": None,
    "coverage": None,
    "uncovered": [],
}
t0 = time.time()
try:
    for statement in work:
        stage, name, sql = _parse(statement)
        result = spark.sql(sql)  # noqa: F821  (spark is provided by the notebook kernel)
        verdict["statements_run"] += 1
        if stage == "pin" and name == "history":
            rows = result.collect()  # DESCRIBE HISTORY must stay a standalone statement
            if rows:
                verdict["source_version"] = max(int(r["version"]) for r in rows)
        elif stage == "coverage":
            got = result.collect()
            row = got[0] if got else None
            data = {} if row is None else (row.asDict() if hasattr(row, "asDict") else dict(row))
            verdict["coverage"] = {str(k): int(v or 0) for k, v in data.items()}
        elif stage == "diff":
            verdict["diffs"][name] = int(result.collect()[0][0])

    diffs = verdict["diffs"]
    verdict["passes_snapshot"] = diffs.get("baseline", 1) == 0
    verdict["passes_run_twice"] = diffs.get("run_twice", 1) == 0
    verdict["hazards"] = {h: diffs[h] == 0 for h in HAZARD_NAMES if h in diffs}
    verdict["diff_rows"] = int(sum(diffs.values()))
    if not verdict["passes_snapshot"]:
        verdict["verdict"] = "snapshot_wrong"
    elif verdict["passes_run_twice"] and all(verdict["hazards"].values()):
        covered = verdict["coverage"] or {}
        verdict["uncovered"] = [c for c in ("run_twice", *verdict["hazards"]) if int(covered.get(c) or 0) < 1]
        if "revision" in verdict["hazards"]:
            verdict["uncovered"] += [k for k, n in covered.items() if k.startswith("revision:") and int(n or 0) < 1]
        verdict["verdict"] = "insufficient_coverage" if verdict["uncovered"] else "replay_safe"
    else:
        verdict["verdict"] = "silent_wrong"
except Exception as exc:  # the candidate (or the plan) crashed; keep the evidence, still clean up
    verdict["verdict"] = "crash"
    verdict["error"] = type(exc).__name__
    verdict["error_message"] = str(exc)[:500]
    verdict["failed_statement"] = verdict["statements_run"]
finally:
    for statement in cleanup:
        stage, name, sql = _parse(statement)
        try:
            spark.sql(sql)  # noqa: F821
        except Exception as exc:  # report, never mask the verdict
            verdict.setdefault("cleanup_errors", []).append({name: f"{type(exc).__name__}: {str(exc)[:200]}"})
    verdict["elapsed_s"] = round(time.time() - t0, 2)

print(json.dumps(verdict, default=str, indent=2))
