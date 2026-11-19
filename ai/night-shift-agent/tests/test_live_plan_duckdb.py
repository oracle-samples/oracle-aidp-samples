"""The LIVE plan (nightshift.replay.spark_sql_plan), executed.

The other plan tests check the generated statements as text. Here the plan is run, statement by statement, in duckdb,
the way the heartbeat's execute_plan runs it in Spark: staging with NTILE / ROW_NUMBER / COUNT(*) OVER (), the sampling
flags, every delivery view and every MERGE, the corrections table, the stale re-delivery, run-twice, the EXCEPT ALL diffs
and the coverage counts. The verdict comes from the same settle_verdict rule.

duckdb is not Spark, so each statement goes through a small, explicit translation (``_translate``): Delta syntax is
dropped, SHALLOW CLONE becomes a copy, ``struct(*)`` becomes the row itself, ``pmod`` and ``date_add`` are mapped, and
the target alias is removed from the left of a MERGE assignment (duckdb does not take it there). Nothing else is
changed: the predicates, the window functions and the statements under test are the plan's own. What this does not
prove is Spark's own parser and Delta's MERGE; the first tick on an instance does that.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb
import pytest

from nightshift import replay as R
from nightshift.merge_adapt import adapt_merge

ROOT = Path(__file__).resolve().parents[1]
PROD, SBX = "prod.orders_silver", "sbx"
COLUMNS = [("order_id", "int"), ("customer_id", "string"), ("region", "string"), ("status", "string"), ("amount", "double"),
           ("quantity", "int"), ("order_date", "date"), ("updated_at", "timestamp")]
NAMES = ", ".join(c for c, _ in COLUMNS)
INSERT = (f"WHEN NOT MATCHED THEN INSERT ({NAMES}) VALUES (" + ", ".join(f"s.{c}" for c, _ in COLUMNS) + ")")
SET_ALL = ", ".join(f"t.{c} = s.{c}" for c, _ in COLUMNS if c != "order_id")
HEAD = "MERGE INTO {target} t USING {batch} s ON t.order_id = s.order_id "
INSERT_ONLY = HEAD + INSERT
GUARDED = HEAD + f"WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET {SET_ALL} " + INSERT
BLIND = HEAD + f"WHEN MATCHED THEN UPDATE SET {SET_ALL} " + INSERT
PARTIAL = HEAD + "WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET t.customer_id = s.customer_id, t.updated_at = s.updated_at " + INSERT
APPEND = "INSERT INTO {target} SELECT * FROM {batch}"


def _translate(sql: str) -> str | None:
    """One plan statement as duckdb SQL, or None for a statement with no duckdb counterpart (DESCRIBE HISTORY)."""
    if sql.startswith("DESCRIBE HISTORY"):
        return None
    sql = sql.replace(" USING DELTA", "")
    sql = re.sub(r"CREATE TABLE (\S+) SHALLOW CLONE (\S+)( VERSION AS OF \d+)?", r"CREATE TABLE \1 AS SELECT * FROM \2", sql)
    # struct(*) -> the row as a struct: name the source and select it whole
    sql = re.sub(r"struct\(\*\) AS ns_row,(.*?)FROM (\S+)\n\)", r"ns_src AS ns_row,\1FROM \2 ns_src\n)", sql, flags=re.S)
    sql = sql.replace("(SELECT struct(*) AS ns_row FROM delivered)", "(SELECT delivered AS ns_row FROM delivered)")
    sql = re.sub(r"date_add\((\w+), 1\)", r"(\1 + 1)", sql)
    if sql.startswith("MERGE INTO"):  # duckdb takes no target alias on the left of an assignment
        sql = re.sub(r"UPDATE SET (.*?)(?= WHEN NOT MATCHED|$)",
                     lambda m: "UPDATE SET " + re.sub(r"(?<![\w.])t\.(\w+)\s*=", r"\1 =", m.group(1)), sql, flags=re.S)
    return sql


def _execute(con, plan) -> dict:
    """The heartbeat's execute_plan, against duckdb: work in order, cleanup always, verdict from settle_verdict."""
    diffs, coverage, hazards_run = {}, None, []
    try:
        for st in plan:
            stage, name, sql = R.parse_plan_tag(st)
            if stage == "cleanup":
                continue
            q = _translate(sql)
            if q is None:
                continue
            cur = con.execute(q)
            if stage == "coverage":
                row = cur.fetchone()
                coverage = R.coverage_from_row(dict(zip([d[0] for d in cur.description], row)))
            elif stage == "diff":
                diffs[name] = int(cur.fetchone()[0])
                if name not in ("baseline", "run_twice"):
                    hazards_run.append(name)
    finally:
        for st in plan:
            stage, _, sql = R.parse_plan_tag(st)
            if stage == "cleanup":
                con.execute(_translate(sql))
    return {**R.settle_verdict(diffs, hazards_run, coverage), "diffs": diffs}


def _orders(n: int) -> list[tuple]:
    return [(i, f"C{i % 7:04d}", ["north", "south", "east"][i % 3], ["NEW", "PAID"][i % 2], 10.0 + i, 1 + i % 5,
             f"2026-10-{1 + i % 28:02d}", f"2026-11-01 23:{i // 60 % 60:02d}:{i % 60:02d}") for i in range(1, n + 1)]


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE SCHEMA prod")
    c.execute("CREATE MACRO pmod(a, b) AS ((a % b) + b) % b")
    c.execute("SET default_null_order = 'nulls_first_on_asc_last_on_desc'")   # Spark's ordering of NULLs
    c.execute("CREATE TABLE prod.orders_silver (order_id INT, customer_id VARCHAR, region VARCHAR, status VARCHAR, amount DOUBLE, "
              "quantity INT, order_date DATE, updated_at TIMESTAMP)")
    yield c
    c.close()


def _grade(con, template: str, n_rows: int, hazards=tuple(R.HAZARDS), rows=None) -> dict:
    con.execute("DELETE FROM prod.orders_silver")
    data = rows if rows is not None else _orders(n_rows)
    if data:
        con.executemany("INSERT INTO prod.orders_silver VALUES (?, ?, ?, ?, ?, ?, ?, ?)", data)
    n = con.execute("SELECT COUNT(*) FROM prod.orders_silver").fetchone()[0]
    plan = R.spark_sql_plan(PROD, SBX, template, ["order_id"], "updated_at", list(hazards), R.HazardConfig(), columns=COLUMNS, n_rows=n)
    out = _execute(con, plan)
    left = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'sbx'").fetchall()
    assert left == []                                   # the plan cleaned up everything it created
    return out


def test_the_shipped_watch_entries_grade_as_the_readme_says(con):
    """jobs/watched_merges.json over a table with the synthetic job's columns: the insert-if-absent entry is silently
    wrong on the revision hazard only (so it is drafted for), the guarded upsert is replay_safe."""
    watched = json.loads((ROOT / "jobs" / "watched_merges.json").read_text(encoding="utf-8"))
    first, second = (adapt_merge(w["statement"]).template for w in watched[:2])
    v1, v2 = _grade(con, first, 300), _grade(con, second, 300)
    assert v1["verdict"] == "silent_wrong" and [h for h, ok in v1["hazards"].items() if not ok] == ["revision"]
    assert v1["diffs"]["revision"] == 2 * v1["coverage"]["revision"] == 60    # per corrected key: the stale row kept, the correction missing
    assert v2["verdict"] == "replay_safe" and v2["diff_rows"] == 0 and v2["uncovered"] == []
    assert set(json.loads(json.dumps(v2["coverage"]))) == {"n_rows", "run_twice", *R.HAZARDS} and v2["coverage"]["n_rows"] == 300


def test_a_blind_upsert_passes_every_delivery_hazard_and_fails_only_the_stale_row_after_its_correction(con):
    """The false PASS the review found, on the live plan: without the stale re-delivery this statement graded
    replay_safe on all five hazards."""
    v = _grade(con, BLIND, 300)
    assert v["passes_snapshot"] and v["passes_run_twice"]
    assert {h: v["hazards"][h] for h in R.DELIVERY_HAZARDS} == {h: True for h in R.DELIVERY_HAZARDS}
    assert v["hazards"]["revision"] is False and v["verdict"] == "silent_wrong"
    assert v["diffs"]["revision"] == 2 * v["coverage"]["revision"]            # every correction overwritten by its stale row
    # the same plan without that last delivery is the old revision hazard, which this statement passes
    plan = R.spark_sql_plan(PROD, SBX, BLIND, ["order_id"], "updated_at", ["revision"], R.HazardConfig(), columns=COLUMNS, n_rows=300)
    stale = [i for i, s in enumerate(plan) if R.parse_plan_tag(s)[1] in ("revision_batch6_view", "revision_batch6_apply")]
    assert len(stale) == 2
    old = _execute(con, [s for i, s in enumerate(plan) if i not in stale])
    assert old["diffs"]["revision"] == 0


def test_an_update_that_forgets_columns_and_a_plain_append_are_caught(con):
    partial = _grade(con, PARTIAL, 300)
    assert partial["verdict"] == "silent_wrong" and [h for h, ok in partial["hazards"].items() if not ok] == ["revision"]
    append = _grade(con, APPEND, 300)
    assert append["verdict"] == "silent_wrong" and not append["passes_run_twice"]
    assert not append["hazards"]["duplicate"] and not append["hazards"]["retried"] and not append["hazards"]["revision"]


@pytest.mark.parametrize("n", list(range(2, 13)) + [39, 40, 41, 60])
def test_small_tables_exercise_every_check_and_still_tell_right_from_wrong(con, n):
    """From two rows up every hazard has a row to act on (the row count sizes the deliveries and each sampling flag
    takes the first row when its modulus would miss): the guarded upsert is replay_safe, and the statements that
    mishandle duplicates or corrections are caught on a table this small."""
    good = _grade(con, GUARDED, n)
    assert good["verdict"] == "replay_safe" and good["uncovered"] == [], good["coverage"]
    assert all(good["coverage"][c] >= 1 for c in ("run_twice", *R.HAZARDS))
    assert _grade(con, APPEND, n)["verdict"] == "silent_wrong"
    assert _grade(con, INSERT_ONLY, n)["hazards"]["revision"] is False
    assert _grade(con, BLIND, n)["hazards"]["revision"] is False


@pytest.mark.parametrize("n,uncovered", [(0, ["run_twice", "late", "duplicate", "out_of_order", "retried", "revision"]),
                                         (1, ["late", "out_of_order"])])
def test_a_table_with_nothing_to_move_is_insufficient_coverage_on_the_live_plan(con, n, uncovered):
    v = _grade(con, GUARDED, n)
    assert v["verdict"] == "insufficient_coverage" and v["uncovered"] == uncovered and v["diff_rows"] == 0
    assert _grade(con, APPEND, n)["verdict"] != "replay_safe"


def test_a_row_without_an_ordering_value_is_not_the_one_corrected(con):
    """Three rows, the first with no ordering value. It sorts first, so the small-table rule would pick it; a
    correction cannot be built from it, so the revision check reports nothing exercised rather than a vacuous pass."""
    rows = _orders(3)
    rows[0] = rows[0][:7] + (None,)
    v = _grade(con, GUARDED, 3, rows=rows)
    assert v["coverage"]["revision"] == 0 and v["uncovered"] == ["revision"] and v["verdict"] == "insufficient_coverage"


def test_one_hazard_at_a_time_matches_the_full_plan(con):
    full = _grade(con, INSERT_ONLY, 120)
    for h in R.HAZARDS:
        alone = _grade(con, INSERT_ONLY, 120, hazards=(h,))
        assert alone["diffs"][h] == full["diffs"][h] and alone["coverage"][h] == full["coverage"][h]
