"""The revision hazard: same key, later ordering value, one changed payload column (a correction)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from nightshift import replay as R

KEYS = ["k"]
TS = "ts"


def _events(n: int = 60) -> pd.DataFrame:
    ts = pd.Timestamp("2026-11-01T00:00:00") + pd.to_timedelta(np.arange(n), unit="min")
    return pd.DataFrame({"k": np.arange(n), "v": [f"e{i}" for i in range(n)], "amt": np.arange(n, dtype=float), "ts": ts})


@pytest.fixture
def events() -> pd.DataFrame:
    return _events()


# --------------------------------------------------------------------------------------
# schedule construction
# --------------------------------------------------------------------------------------
def test_revision_batch_moves_ts_later_and_changes_one_payload_column(events):
    cfg = R.HazardConfig(rev_fraction=0.1)
    sched = R.make_hazard_batches(events, KEYS, TS, cfg)
    rev = sched["revision"]
    assert len(rev) == cfg.n_batches + 1                       # baseline deliveries + corrections
    assert all(R.diff(a, b) == 0 for a, b in zip(rev[:-1], sched["baseline"]))
    corr = rev[-1]
    assert len(corr) == 6                                       # 10 percent of 60
    orig = events.set_index("k").loc[corr["k"]]
    assert (corr[TS].values > orig[TS].values).all()           # later than the row it corrects
    assert (corr["v"].values != orig["v"].values).all()        # first non-key, non-ts column changed
    assert (corr["amt"].values == orig["amt"].values).all()    # only one payload column changed
    assert set(corr["k"]) <= set(events["k"])                  # no new keys


def test_revision_is_deterministic_and_respects_revise_col_and_fraction(events):
    a = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(revise_col="amt"))["revision"][-1]
    b = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(revise_col="amt"))["revision"][-1]
    assert R.diff(a, b) == 0
    orig = events.set_index("k").loc[a["k"]]
    assert (a["amt"].values == orig["amt"].values + 1).all() and (a["v"].values == orig["v"].values).all()
    none = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(rev_fraction=0.0))["revision"]
    assert len(none) == R.HazardConfig().n_batches               # no corrections batch when disabled


def test_pick_revise_col_rules():
    cols = ["k", "ts", "v", "amt"]
    assert R.pick_revise_col(cols, ["k"], "ts") == "v"
    assert R.pick_revise_col(cols, ["k"], "ts", "amt") == "amt"
    assert R.pick_revise_col(["k", "ts"], ["k"], "ts") is None
    with pytest.raises(ValueError):
        R.pick_revise_col(cols, ["k"], "ts", "k")
    with pytest.raises(KeyError):
        R.pick_revise_col(cols, ["k"], "ts", "nope")


def test_keys_and_ts_only_table_still_gets_a_revision(events):
    slim = events[["k", "ts"]]
    rev = R.make_hazard_batches(slim, KEYS, TS, R.HazardConfig())["revision"][-1]
    assert len(rev) > 0 and (rev[TS] > events.set_index("k").loc[rev["k"], TS].values).all()


# --------------------------------------------------------------------------------------
# verdicts: the hazard separates upsert from insert-if-absent
# --------------------------------------------------------------------------------------
def test_upsert_merge_passes_revision_but_insert_if_absent_fails_it(events):
    cfg = R.HazardConfig()
    up = R.replay_verdict(R.merge_on_keys(KEYS, TS), events, KEYS, TS, cfg)
    assert up["verdict"] == "replay_safe" and up["hazards"]["revision"] is True

    ins = R.replay_verdict(R.insert_if_absent(KEYS), events, KEYS, TS, cfg)
    assert ins["passes_snapshot"] and ins["passes_run_twice"]
    assert {h: ins["hazards"][h] for h in R.DELIVERY_HAZARDS} == {h: True for h in R.DELIVERY_HAZARDS}
    assert ins["hazards"]["revision"] is False
    assert ins["verdict"] == "silent_wrong"
    assert ins["diffs"]["revision"] == 12                        # 6 stale rows kept + 6 corrections missing


def test_naive_append_fails_revision_by_keeping_both_rows(events):
    v = R.replay_verdict(R.naive_append, events, KEYS, TS, R.HazardConfig())
    assert v["hazards"]["revision"] is False and v["diffs"]["revision"] == 6  # the stale rows are extra


def test_grade_table_has_revision_column(events):
    table = R.grade_many({"ins": R.insert_if_absent(KEYS), "up": R.merge_on_keys(KEYS, TS)}, events, KEYS, TS)
    assert "revision" in table.columns
    assert bool(table.set_index("candidate").loc["ins", "revision"]) is False
    assert table.silent_wrong_rate == 0.5


# --------------------------------------------------------------------------------------
# live plan
# --------------------------------------------------------------------------------------
TEMPLATE = "MERGE INTO {target} t USING {batch} b ON t.k = b.k WHEN NOT MATCHED THEN INSERT *"
COLUMNS = [("k", "bigint"), ("v", "string"), ("amt", "double"), ("ts", "timestamp"), ("flag", "boolean")]


def _plan(**kw):
    kw.setdefault("columns", COLUMNS)
    return R.spark_sql_plan("default.bank.core_load", "sbx", TEMPLATE, ["k"], "ts", ["revision"], **kw)


def test_spark_plan_revision_builds_corrections_table_and_delivery():
    plan = _plan()
    tags = [R.parse_plan_tag(s)[:2] for s in plan]
    assert ("stage", "revisions") in tags
    rev_sql = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[1] == "revisions")
    assert "CREATE TABLE sbx.core_load_rev USING DELTA AS" in rev_sql
    assert "ts + INTERVAL 1 HOUR AS ts" in rev_sql                  # ordering column moved later
    assert "concat(v, '_rev') AS v" in rev_sql                      # first mutable payload column changed
    assert "\n       amt,\n" in rev_sql and "flag" in rev_sql       # the others pass through by name
    assert "WHERE ns_rev" in rev_sql
    # four baseline deliveries plus the corrections delivery, each a view + an apply
    rev_views = [s for s in plan if R.parse_plan_tag(s)[0] == "repair" and R.parse_plan_tag(s)[1].startswith("revision_") and "VIEW" in s]
    assert len(rev_views) == R.HazardConfig().n_batches + 1
    assert "SELECT * FROM sbx.core_load_rev" in rev_views[-1]
    # revision truth is the recompute over staging plus the corrections
    diff_sql = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[0] == "diff" and R.parse_plan_tag(s)[1] == "revision")
    assert "UNION ALL" in diff_sql.split("truth AS")[0] and "sbx.core_load_rev" in diff_sql
    # staging carries the flag; cleanup drops the corrections table
    stage_sql = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[1] == "ntile")
    assert "AS ns_rev" in stage_sql
    assert any("DROP TABLE IF EXISTS sbx.core_load_rev" in s for s in plan if R.parse_plan_tag(s)[0] == "cleanup")


def test_spark_plan_revision_type_rules_and_errors():
    assert R.spark_shift_expr("d", "date") == "date_add(d, 1)"
    assert R.spark_shift_expr("n", "bigint") == "n + 1"
    assert R.spark_mutate_expr("b", "boolean") == "NOT b"
    assert R.spark_mutate_expr("x", "array<string>") is None
    assert R.pick_spark_revise_col([("k", "bigint"), ("ts", "timestamp"), ("x", "array<string>"), ("n", "int")], ["k"], "ts") == ("n", "int")
    assert R.pick_spark_revise_col([("k", "bigint"), ("ts", "timestamp")], ["k"], "ts") is None
    with pytest.raises(TypeError):
        R.pick_spark_revise_col([("k", "bigint"), ("ts", "timestamp"), ("x", "array<string>")], ["k"], "ts", "x")
    with pytest.raises(ValueError):
        _plan(columns=None)
    with pytest.raises(KeyError):
        _plan(columns=[("k", "bigint"), ("v", "string")])
    with pytest.raises(TypeError):
        R.spark_sql_plan("p.t", "s", TEMPLATE, ["k"], "ts", ["revision"], columns=[("k", "bigint"), ("ts", "string")])


def test_spark_plan_without_revision_has_no_corrections_table():
    plan = R.spark_sql_plan("default.bank.core_load", "sbx", TEMPLATE, ["k"], "ts", ["late"])
    assert not any("core_load_rev" in s for s in plan)
