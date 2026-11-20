"""Local tests for nightshift.replay (pandas + duckdb only; no live calls).

Where SQL differs from Spark SQL it is stated: the sql_apply tests use the duckdb dialect
(QUALIFY, INSERT ... ON CONFLICT) as the local stand-in for a Delta MERGE repair.
"""
from __future__ import annotations

import math
import re

import numpy as np
import pandas as pd
import pytest

from nightshift import replay as R

KEYS = ["k"]
TS = "ts"


# --------------------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------------------
def _events(n: int = 60) -> pd.DataFrame:
    """Unique-key event stream: naive append IS the right answer for a single ordered run."""
    ts = pd.Timestamp("2026-11-01T00:00:00") + pd.to_timedelta(np.arange(n), unit="min")
    return pd.DataFrame({"k": np.arange(n), "v": [f"e{i}" for i in range(n)], "ts": ts})


def _versioned(n_keys: int = 20, versions: int = 3) -> pd.DataFrame:
    """Each key gets several versions with increasing ts; truth = latest version per key."""
    rows = []
    rng = np.random.default_rng(7)
    for v in range(versions):
        for k in range(n_keys):
            rows.append({"k": k, "v": f"k{k}v{v}", "amt": float(rng.integers(1, 100)),
                         "ts": pd.Timestamp("2026-11-01") + pd.Timedelta(hours=v * 24 + int(rng.integers(0, 12)))})
    return pd.DataFrame(rows)


def _sorted_rows(frames) -> pd.DataFrame:
    cat = pd.concat(list(frames), ignore_index=True)
    return cat.sort_values(list(cat.columns), kind="mergesort").reset_index(drop=True)


@pytest.fixture
def events() -> pd.DataFrame:
    return _events()


@pytest.fixture
def versioned() -> pd.DataFrame:
    return _versioned()


@pytest.fixture
def cfg() -> R.HazardConfig:
    return R.HazardConfig()


# --------------------------------------------------------------------------------------
# HazardConfig + schedules
# --------------------------------------------------------------------------------------
def test_hazard_config_defaults_and_validation():
    c = R.HazardConfig()
    assert (c.seed, c.late_fraction, c.dup_fraction, c.shuffle, c.retry_batches, c.n_batches) == (
        20261102, 0.1, 0.1, True, 1, 4)
    with pytest.raises(ValueError):
        R.HazardConfig(n_batches=0)
    with pytest.raises(ValueError):
        R.HazardConfig(late_fraction=1.5)


def test_baseline_is_ordered_and_split(events, cfg):
    sched = R.make_hazard_batches(events, KEYS, TS, cfg)
    assert set(sched) == set(R.SCHEDULES)
    base = sched["baseline"]
    assert len(base) == cfg.n_batches
    cat = pd.concat(base, ignore_index=True)
    assert cat[TS].is_monotonic_increasing
    assert R.diff(cat, events) == 0


def test_late_preserves_multiset_but_moves_rows(events, cfg):
    sched = R.make_hazard_batches(events, KEYS, TS, cfg)
    assert R.diff(pd.concat(sched["late"]), events) == 0
    assert len(sched["late"]) == cfg.n_batches
    moved = sum(R.diff(a, b) for a, b in zip(sched["late"], sched["baseline"])) // 2
    assert moved == round(cfg.late_fraction * len(pd.concat(sched["baseline"][:-1])))
    # late rows land in strictly later batches, so some batch is no longer ts-ordered
    assert not pd.concat(sched["late"], ignore_index=True)[TS].is_monotonic_increasing


def test_duplicate_repeats_rows_only(events, cfg):
    sched = R.make_hazard_batches(events, KEYS, TS, cfg)
    cat = pd.concat(sched["duplicate"], ignore_index=True)
    extra = len(cat) - len(events)
    assert extra == round(cfg.dup_fraction * len(events))
    # the unique rows are exactly the baseline rows
    assert R.diff(cat.drop_duplicates(), events) == 0
    assert len(sched["duplicate"]) in (cfg.n_batches, cfg.n_batches + 1)


def test_out_of_order_permutes_batches_and_preserves_multiset(events, cfg):
    sched = R.make_hazard_batches(events, KEYS, TS, cfg)
    assert len(sched["out_of_order"]) == cfg.n_batches
    assert R.diff(pd.concat(sched["out_of_order"]), events) == 0
    perm = R._batch_permutation(cfg.n_batches, cfg)
    assert sorted(perm) == list(range(cfg.n_batches)) and perm != list(range(cfg.n_batches))
    for delivered, i in zip(sched["out_of_order"], perm):
        assert R.diff(delivered, sched["baseline"][i]) == 0
    # shuffle=False is a deterministic reversal, never vacuous
    assert R._batch_permutation(4, R.HazardConfig(shuffle=False)) == [3, 2, 1, 0]


def test_retried_delivers_a_batch_twice_consecutively(events, cfg):
    sched = R.make_hazard_batches(events, KEYS, TS, cfg)
    retried = sched["retried"]
    assert len(retried) == cfg.n_batches + cfg.retry_batches
    repeats = [i for i in range(len(retried) - 1) if R.diff(retried[i], retried[i + 1]) == 0 and len(retried[i]) > 0]
    assert len(repeats) == cfg.retry_batches
    assert R.diff(pd.concat(retried).drop_duplicates(), events) == 0


def test_schedules_are_deterministic_per_seed(events):
    a = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(seed=1))
    b = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(seed=1))
    c = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(seed=2))
    for name in R.SCHEDULES:
        assert len(a[name]) == len(b[name])
        assert all(R.diff(x, y) == 0 for x, y in zip(a[name], b[name]))
    assert any(R.diff(x, y) != 0 for x, y in zip(a["late"], c["late"]))


def test_make_hazard_batches_rejects_missing_columns(events, cfg):
    with pytest.raises(KeyError):
        R.make_hazard_batches(events, ["nope"], TS, cfg)


# --------------------------------------------------------------------------------------
# recompute / diff / run_schedule
# --------------------------------------------------------------------------------------
def test_recompute_keeps_latest_row_per_key(versioned):
    truth = R.recompute(versioned, KEYS, TS)
    assert len(truth) == versioned["k"].nunique()
    assert truth["v"].str.endswith("v2").all()
    assert R.recompute(versioned.iloc[0:0], KEYS, TS).empty


def test_diff_is_symmetric_and_ignores_order_and_dtype_drift():
    a = pd.DataFrame({"k": [1, 2, 3], "v": [1.0, 2.0, 3.0], "ts": pd.to_datetime(["2026-01-01"] * 3)})
    b = a.iloc[::-1].reset_index(drop=True)
    b["k"] = b["k"].astype("Int64")
    b["v"] = b["v"].astype("int64")
    b["ts"] = b["ts"].astype("datetime64[us]")
    b = b[["ts", "v", "k"]]
    assert R.diff(a, b) == 0
    c = pd.concat([a, a.iloc[[0]]], ignore_index=True)  # one extra duplicate row
    assert R.diff(a, c) == 1 and R.diff(c, a) == 1
    d = a.copy(); d.loc[0, "v"] = 99.0  # one changed row = 2 differing rows (one each side)
    assert R.diff(a, d) == 2
    assert R.diff(a, a.drop(columns=["v"])) == 6  # schema mismatch: every row differs
    assert R.diff(None, a) == 3


def test_run_schedule_naive_vs_merge(versioned, cfg):
    sched = R.make_hazard_batches(versioned, KEYS, TS, cfg)
    appended = R.run_schedule(R.naive_append, sched["baseline"])
    merged = R.run_schedule(R.merge_on_keys(KEYS, TS), sched["baseline"])
    assert len(appended) == len(versioned)
    assert R.diff(merged, R.recompute(versioned, KEYS, TS)) == 0
    with pytest.raises(TypeError):
        R.run_schedule(lambda s, b: None, sched["baseline"])


# --------------------------------------------------------------------------------------
# verdicts
# --------------------------------------------------------------------------------------
def test_naive_append_is_silent_wrong_on_duplicate_and_retried(events, cfg):
    v = R.replay_verdict(R.naive_append, events, KEYS, TS, cfg)
    assert v["passes_snapshot"] is True          # a single ordered run looks perfect
    assert v["passes_run_twice"] is False
    assert v["hazards"] == {"late": True, "duplicate": False, "out_of_order": True, "retried": False, "revision": False}
    assert v["verdict"] == "silent_wrong"
    assert v["diff_rows"] > 0 and v["error"] is None


def test_merge_on_keys_is_replay_safe(versioned, cfg):
    v = R.replay_verdict(R.merge_on_keys(KEYS, TS), versioned, KEYS, TS, cfg)
    assert v["verdict"] == "replay_safe"
    assert v["passes_snapshot"] and v["passes_run_twice"] and all(v["hazards"].values())
    assert v["diff_rows"] == 0


def test_naive_append_on_versioned_data_is_snapshot_wrong_not_silent(versioned, cfg):
    v = R.replay_verdict(R.naive_append, versioned, KEYS, TS, cfg)
    assert v["passes_snapshot"] is False
    assert v["verdict"] == "snapshot_wrong"


def test_planted_dedup_within_batch_fails_retried(events, cfg):
    v = R.replay_verdict(R.dedup_within_batch_append(KEYS, TS), events, KEYS, TS, cfg)
    assert v["passes_snapshot"] is True
    assert v["hazards"]["late"] is True and v["hazards"]["out_of_order"] is True
    assert v["hazards"]["retried"] is False
    assert v["hazards"]["duplicate"] is False
    assert v["verdict"] == "silent_wrong"


def test_crash_path_captures_exception_type(events, cfg):
    def boom(state, batch):
        raise ValueError("schema drift")

    v = R.replay_verdict(boom, events, KEYS, TS, cfg)
    assert v["verdict"] == "crash"
    assert v["error"] == "ValueError"
    assert v["error_message"] == "schema drift"
    assert v["failed_stage"] == "baseline"
    assert v["hazards"] == {h: False for h in R.HAZARDS}


def test_crash_in_hazard_stage_is_recorded(events, cfg):
    calls = {"n": 0}

    def flaky(state, batch):
        calls["n"] += 1
        if calls["n"] > cfg.n_batches + 1:  # survives baseline + run-twice, dies in first hazard
            raise RuntimeError("late batch exploded")
        return R.naive_append(state, batch)

    v = R.replay_verdict(flaky, events, KEYS, TS, cfg)
    assert v["verdict"] == "crash" and v["error"] == "RuntimeError"
    assert v["passes_snapshot"] is True and v["failed_stage"] == "late"


def test_custom_recompute_fn_is_used(events, cfg):
    def wrong_truth(df, key_cols, ts_col):
        return df.iloc[0:0]

    v = R.replay_verdict(R.naive_append, events, KEYS, TS, cfg, recompute_fn=wrong_truth)
    assert v["passes_snapshot"] is False and v["verdict"] == "snapshot_wrong"


# --------------------------------------------------------------------------------------
# grade_many
# --------------------------------------------------------------------------------------
def test_grade_many_shape_and_silent_wrong_rate(events, cfg):
    def boom(state, batch):
        raise KeyError("k")

    table = R.grade_many({"naive": R.naive_append, "merge": R.merge_on_keys(KEYS, TS), "boom": boom},
                         events, KEYS, TS, cfg)
    assert isinstance(table, R.GradeTable)
    assert list(table.columns) == R.GRADE_COLUMNS
    assert table.shape == (3, len(R.GRADE_COLUMNS))
    by = table.set_index("candidate")
    assert by.loc["naive", "verdict"] == "silent_wrong"
    assert by.loc["merge", "verdict"] == "replay_safe"
    assert by.loc["boom", "verdict"] == "crash" and by.loc["boom", "error"] == "KeyError"
    # two reached the card (passed snapshot, no crash); one of them is silently wrong
    assert table.silent_wrong_rate == pytest.approx(0.5)
    assert R.silent_wrong_rate(table) == pytest.approx(0.5)
    assert math.isnan(R.silent_wrong_rate(table[table["verdict"] == "crash"]))


# --------------------------------------------------------------------------------------
# sql_apply (duckdb dialect; local stand-in for a Delta MERGE)
# --------------------------------------------------------------------------------------
CORRECT_QUALIFY_SQL = """
WITH all_rows AS (SELECT * FROM target UNION ALL SELECT * FROM batch)
SELECT * FROM all_rows
QUALIFY row_number() OVER (PARTITION BY k ORDER BY ts DESC) = 1
"""

CORRECT_ON_CONFLICT_SQL = """
INSERT INTO target
SELECT * FROM batch QUALIFY row_number() OVER (PARTITION BY k ORDER BY ts DESC) = 1
ON CONFLICT (k) DO UPDATE SET v = excluded.v, amt = excluded.amt, ts = excluded.ts
WHERE excluded.ts >= target.ts
"""

INCORRECT_PLAIN_INSERT_SQL = "INSERT INTO target SELECT * FROM batch"

ANTI_JOIN_INSERT_ONLY_SQL = """
INSERT INTO target
SELECT b.* FROM batch b ANTI JOIN target t USING (k)
"""


def test_sql_apply_correct_qualify_merge_is_replay_safe(versioned, cfg):
    v = R.replay_verdict(R.sql_apply(CORRECT_QUALIFY_SQL, KEYS), versioned, KEYS, TS, cfg)
    assert v["verdict"] == "replay_safe", v


def test_sql_apply_correct_on_conflict_is_replay_safe(versioned, cfg):
    fn = R.sql_apply(CORRECT_ON_CONFLICT_SQL, KEYS, declare_primary_key=True)
    v = R.replay_verdict(fn, versioned, KEYS, TS, cfg)
    assert v["verdict"] == "replay_safe", v


def test_sql_apply_incorrect_plain_insert_is_silent_wrong(events, cfg):
    v = R.replay_verdict(R.sql_apply(INCORRECT_PLAIN_INSERT_SQL, KEYS), events, KEYS, TS, cfg)
    assert v["passes_snapshot"] is True
    assert v["hazards"]["duplicate"] is False and v["hazards"]["retried"] is False
    assert v["verdict"] == "silent_wrong"


def test_sql_apply_anti_join_insert_only_misses_updates(versioned, events, cfg):
    fn = R.sql_apply(ANTI_JOIN_INSERT_ONLY_SQL, KEYS)
    # fine for an insert-only event stream under every delivery hazard (dedups across batches)...
    v = R.replay_verdict(fn, events, KEYS, TS, cfg)
    assert all(v["hazards"][h] for h in R.DELIVERY_HAZARDS) and v["passes_run_twice"]
    # ...yet silently wrong once a correction arrives: the stale row is kept (revision hazard)
    assert v["hazards"]["revision"] is False and v["verdict"] == "silent_wrong"
    # ...but keeps the FIRST version of a key, so it fails the recompute on versioned data
    assert R.replay_verdict(fn, versioned, KEYS, TS, cfg)["verdict"] == "snapshot_wrong"


def test_sql_apply_syntax_error_is_a_crash(events, cfg):
    v = R.replay_verdict(R.sql_apply("INSERT INTO target SELEC * FROM batch", KEYS), events, KEYS, TS, cfg)
    assert v["verdict"] == "crash"
    assert v["error"].endswith(("Exception", "Error"))  # duckdb ParserException
    with pytest.raises(ValueError):
        R.sql_apply("   ", KEYS)


def test_sql_apply_grade_many_mixed(versioned, cfg):
    table = R.grade_many({
        "qualify_merge": R.sql_apply(CORRECT_QUALIFY_SQL, KEYS),
        "plain_insert": R.sql_apply(INCORRECT_PLAIN_INSERT_SQL, KEYS),
        "pandas_merge": R.merge_on_keys(KEYS, TS),
    }, versioned, KEYS, TS, cfg)
    verdicts = dict(zip(table["candidate"], table["verdict"]))
    assert verdicts == {"qualify_merge": "replay_safe", "plain_insert": "snapshot_wrong", "pandas_merge": "replay_safe"}
    assert table.silent_wrong_rate == 0.0


# --------------------------------------------------------------------------------------
# spark_sql_plan (pure string generation)
# --------------------------------------------------------------------------------------
TEMPLATE = (
    "MERGE INTO {target} t USING {batch} b ON t.k = b.k "
    "WHEN MATCHED AND b.ts >= t.ts THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
)


COLUMNS = [("k", "bigint"), ("v", "string"), ("amt", "double"), ("ts", "timestamp")]


def _plan(hazards=R.HAZARDS, **kw):
    kw.setdefault("columns", COLUMNS)
    return R.spark_sql_plan("default.bank.core_load", "default.ops_sandbox", TEMPLATE, ["k"], "ts",
                            list(hazards), **kw)


def test_spark_plan_stage_ordering_and_cleanup_last():
    plan = _plan()
    stages = [R.parse_plan_tag(s)[0] for s in plan]
    order = ["setup", "pin", "stage", "coverage", "target", "repair", "twice", "diff", "cleanup"]
    assert [s for s in order if s in stages] == order
    # each stage is one contiguous block, in that order
    ranks = [order.index(s) for s in stages]
    assert ranks == sorted(ranks)
    assert stages[-1] == "cleanup"
    tail = [R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[0] == "cleanup"]
    assert all(t.startswith(("DROP TABLE IF EXISTS", "DROP VIEW IF EXISTS")) for t in tail)


def test_spark_plan_uses_only_live_verified_clone_features():
    joined = "\n".join(_plan(pin_version=412))
    assert "CREATE TABLE default.ops_sandbox.core_load_sbx SHALLOW CLONE default.bank.core_load VERSION AS OF 412" in joined
    assert "DEEP CLONE" not in joined.upper().replace("SHALLOW CLONE", "")
    history = [R.parse_plan_tag(s)[2] for s in _plan() if "DESCRIBE HISTORY" in s]
    assert history == ["DESCRIBE HISTORY default.bank.core_load"]  # standalone, never a subquery
    assert joined.count("DESCRIBE HISTORY") == 1
    # without pin_version the clone pins the current version
    assert "SHALLOW CLONE default.bank.core_load\n" in "\n".join(_plan()) + "\n"


def test_spark_plan_placeholders_filled_and_repair_counts():
    cfg = R.HazardConfig(n_batches=4, retry_batches=1)
    plan = _plan(cfg=cfg)
    applies = [s for s in plan if R.parse_plan_tag(s)[1].endswith("_apply") or R.parse_plan_tag(s) [0:2] == ("twice", "apply")]
    for s in applies:
        assert "{target}" not in s and "{batch}" not in s
        assert "MERGE INTO default.ops_sandbox.core_load_tgt_" in s
        assert "USING ns_batch_core_load b" in s
    # deliveries: baseline 4, late 4, duplicate 5 (trailing re-delivery), out_of_order 4, retried 5,
    # revision 6 (baseline + corrections + the superseded rows again), run-twice 1
    assert len(applies) == 4 + 4 + 5 + 4 + 5 + 6 + 1
    views = [s for s in plan if "CREATE OR REPLACE TEMP VIEW ns_batch_core_load" in s]
    assert len(views) == len(applies)
    # the run-twice target is a SHALLOW CLONE of the finished baseline target
    assert any("core_load_tgt_run_twice SHALLOW CLONE default.ops_sandbox.core_load_tgt_baseline" in s for s in plan)


def test_spark_plan_hazard_predicates_match_local_schedule_logic():
    cfg = R.HazardConfig(n_batches=4)
    plan = _plan(cfg=cfg)
    ooo_views = [R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[1].startswith("out_of_order") and "VIEW" in s]
    delivered = [int(re.search(r"ns_batch = (\d+)", v).group(1)) for v in ooo_views]
    assert delivered == [i + 1 for i in R._batch_permutation(4, cfg)]
    retried_views = [R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[1].startswith("retried") and "VIEW" in s]
    retried_batches = [int(re.search(r"ns_batch = (\d+)", v).group(1)) for v in retried_views]
    expected = []
    for i in range(4):
        expected.append(i + 1)
        if i in R._retry_indices(4, cfg):
            expected.append(i + 1)
    assert retried_batches == expected
    late_view = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[1] == "late_batch2_view")
    assert "(ns_batch = 2 AND NOT ns_late) OR (ns_batch = 1 AND ns_late)" in late_view


def test_spark_plan_diff_queries_recompute_cte_and_except_all_both_ways():
    plan = _plan(hazards=["late"])
    diffs = {R.parse_plan_tag(s)[1]: R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[0] == "diff"}
    assert set(diffs) == {"baseline", "late", "run_twice"}
    for name in ("baseline", "late"):
        q = diffs[name]
        assert q.startswith("WITH truth AS")
        assert "ROW_NUMBER() OVER (PARTITION BY ns_row.k ORDER BY ns_row.ts DESC)" in q
        assert q.count("EXCEPT ALL") == 2
        assert f"SELECT * FROM default.ops_sandbox.core_load_tgt_{name} EXCEPT ALL SELECT * FROM truth" in q
        assert f"SELECT * FROM truth EXCEPT ALL SELECT * FROM default.ops_sandbox.core_load_tgt_{name}" in q
    assert diffs["run_twice"].count("EXCEPT ALL") == 2 and "truth" not in diffs["run_twice"]


def test_spark_plan_cleanup_drops_every_created_object():
    plan = _plan()
    created = set()
    for s in plan:
        m = re.search(r"CREATE (?:OR REPLACE )?(?:TABLE|TEMP VIEW|SCHEMA IF NOT EXISTS) (\S+)", R.parse_plan_tag(s)[2])
        if m and "SCHEMA" not in m.group(0):
            created.add(m.group(1))
    dropped = {re.search(r"DROP (?:TABLE|VIEW) IF EXISTS (\S+)", R.parse_plan_tag(s)[2]).group(1)
               for s in plan if R.parse_plan_tag(s)[0] == "cleanup"}
    assert created == dropped
    # stale leftovers from a dead kernel are dropped up front too
    stale = {re.search(r"IF EXISTS (\S+)", R.parse_plan_tag(s)[2]).group(1)
             for s in plan if R.parse_plan_tag(s)[1].startswith("stale_")}
    assert stale == dropped


def test_spark_plan_validation_errors():
    with pytest.raises(ValueError):
        _plan(hazards=["late", "eclipse"])
    with pytest.raises(ValueError):
        R.spark_sql_plan("p.t", "s", "MERGE INTO {target} USING src", ["k"], "ts", ["late"])
    with pytest.raises(ValueError):
        R.spark_sql_plan("p.t", "s", TEMPLATE, [], "ts", ["late"])


# --------------------------------------------------------------------------------------
# notebook template: self-contained, cleans up in finally, prints a verdict
# --------------------------------------------------------------------------------------
class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _StubSpark:
    """Records statements; DESCRIBE HISTORY returns versions, diff queries return a count,
    and an optional statement substring triggers a failure."""

    def __init__(self, diff_count=0, fail_on: str | None = None, coverage: dict | None = {}):  # noqa: B006
        self.statements: list[str] = []
        self.diff_count = diff_count
        self.fail_on = fail_on
        self.coverage = coverage   # per-check counts the coverage statement returns (7 unless named); None = no row

    def sql(self, q: str):
        self.statements.append(q)
        if self.fail_on and self.fail_on in q:
            raise RuntimeError("AnalysisException: boom")
        if q.startswith("DESCRIBE HISTORY"):
            return _Rows([{"version": 412}, {"version": 411}])
        if "AS diff_rows" in q:
            return _Rows([(self.diff_count,)])
        if q.startswith("SELECT COUNT(*) AS n_rows"):
            names = re.findall(r" AS `?([\w:]+)`?", q)
            return _Rows([] if self.coverage is None else [{n: self.coverage.get(n, 0 if n.startswith("revision:") else 7) for n in names}])
        return _Rows([])


def _run_template(plan, spark, capsys):
    cell = R.render_spark_cell(plan)
    assert "nightshift" not in re.sub(r"#.*", "", cell)  # no nightshift import in the kernel cell
    exec(compile(cell, "replay_spark_template.py", "exec"), {"spark": spark})
    out = capsys.readouterr().out
    import json
    return json.loads(out[out.index("{"):])


def test_notebook_template_runs_plan_and_prints_replay_safe(capsys):
    plan = _plan()
    spark = _StubSpark(diff_count=0)
    verdict = _run_template(plan, spark, capsys)
    assert verdict["verdict"] == "replay_safe"
    assert verdict["passes_snapshot"] and verdict["passes_run_twice"]
    assert verdict["hazards"] == {h: True for h in R.HAZARDS}
    assert verdict["source_version"] == 412
    assert verdict["diff_rows"] == 0 and verdict["error"] is None
    # every statement ran, tags stripped, cleanup last
    assert len(spark.statements) == len(plan)
    assert not any(s.startswith("-- ns:") for s in spark.statements)
    n_cleanup = sum(1 for s in plan if R.parse_plan_tag(s)[0] == "cleanup")
    assert all(s.startswith("DROP") for s in spark.statements[-n_cleanup:])


def test_notebook_template_crash_still_drops_sandbox(capsys):
    plan = _plan()
    spark = _StubSpark(fail_on="MERGE INTO")
    verdict = _run_template(plan, spark, capsys)
    assert verdict["verdict"] == "crash" and verdict["error"] == "RuntimeError"
    n_cleanup = sum(1 for s in plan if R.parse_plan_tag(s)[0] == "cleanup")
    assert all(s.startswith("DROP") for s in spark.statements[-n_cleanup:])
    assert len(spark.statements) < len(plan)  # work stopped at the crash


def test_notebook_template_silent_wrong_when_hazard_diff_nonzero(capsys):
    plan = _plan(hazards=["retried"])
    spark = _StubSpark(diff_count=3)
    # baseline diff must be zero for silent_wrong; make the stub answer 0 for baseline only
    orig = spark.sql

    def sql(q):
        r = orig(q)
        if "AS diff_rows" in q and "core_load_tgt_baseline EXCEPT ALL SELECT * FROM truth" in q:
            return _Rows([(0,)])
        return r

    spark.sql = sql
    verdict = _run_template(plan, spark, capsys)
    assert verdict["passes_snapshot"] is True
    assert verdict["hazards"] == {"retried": False}
    assert verdict["verdict"] == "silent_wrong"
    assert verdict["diff_rows"] == 6  # retried 3 + run_twice 3


@pytest.mark.parametrize("coverage,uncovered", [({"revision": 0}, ["revision"]), ({"run_twice": 0, "late": 0}, ["run_twice", "late"]),
                                                (None, ["run_twice", *R.HAZARDS])])
def test_notebook_template_reports_insufficient_coverage_instead_of_replay_safe(capsys, coverage, uncovered):
    """Every diff is zero, but a check exercised no row (or the coverage statement returned nothing): the cell must
    not print replay_safe. Same rule as nightshift.replay.settle_verdict, restated in the self-contained cell."""
    verdict = _run_template(_plan(), _StubSpark(diff_count=0, coverage=coverage), capsys)
    assert verdict["verdict"] == "insufficient_coverage" and verdict["uncovered"] == uncovered
    assert verdict["passes_snapshot"] and verdict["passes_run_twice"] and verdict["diff_rows"] == 0


def test_notebook_template_names_a_column_the_corrections_cannot_change(capsys):
    """Every diff is zero and every check exercised rows, but the table has an array column the corrections pass
    through: the cell reports it by name and does not print replay_safe (same rule as settle_verdict)."""
    columns = [("k", "bigint"), ("tags", "array<string>"), ("v", "string"), ("ts", "timestamp")]
    plan = R.spark_sql_plan("default.bank.core_load", "default.ops_sandbox", TEMPLATE, ["k"], "ts", list(R.HAZARDS), columns=columns)
    verdict = _run_template(plan, _StubSpark(diff_count=0), capsys)
    assert verdict["verdict"] == "insufficient_coverage" and verdict["uncovered"] == ["revision:tags"]
    assert verdict["coverage"]["revision:tags"] == 0 and verdict["diff_rows"] == 0
    plain = R.spark_sql_plan("default.bank.core_load", "default.ops_sandbox", TEMPLATE, ["k"], "ts", list(R.HAZARDS),
                             columns=[c for c in columns if c[0] != "tags"])
    assert _run_template(plain, _StubSpark(diff_count=0), capsys)["verdict"] == "replay_safe"
