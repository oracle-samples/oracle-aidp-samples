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
def test_revision_batch_moves_ts_later_and_changes_every_payload_column(events):
    cfg = R.HazardConfig(rev_fraction=0.1)
    sched = R.make_hazard_batches(events, KEYS, TS, cfg)
    rev = sched["revision"]
    assert len(rev) == cfg.n_batches + 2                       # baseline deliveries + corrections + stale re-delivery
    assert all(R.diff(a, b) == 0 for a, b in zip(rev[:-2], sched["baseline"]))
    corr, stale = rev[-2], rev[-1]
    # the last delivery is the superseded originals of exactly the corrected keys, unchanged
    assert list(stale["k"]) == list(corr["k"]) and R.diff(stale, events[events["k"].isin(corr["k"])]) == 0
    assert len(corr) == 6                                       # 10 percent of 60
    orig = events.set_index("k").loc[corr["k"]]
    assert (corr[TS].values > orig[TS].values).all()           # later than the row it corrects
    assert (corr["v"].values != orig["v"].values).all()        # every non-key, non-ordering column changes,
    assert (corr["amt"].values != orig["amt"].values).all()    # so an UPDATE that forgets a column is caught
    only_v = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(revise_col="v"))["revision"][-2]
    assert (only_v["amt"].values == events.set_index("k").loc[only_v["k"], "amt"].values).all()   # revise_col narrows it to one
    assert set(corr["k"]) <= set(events["k"])                  # no new keys


def test_revision_is_deterministic_and_respects_revise_col_and_fraction(events):
    a = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(revise_col="amt"))["revision"][-2]
    b = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig(revise_col="amt"))["revision"][-2]
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
    rev = R.make_hazard_batches(slim, KEYS, TS, R.HazardConfig())["revision"][-2]
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
    # the 6 stale rows stay beside their corrections, and the 6 superseded rows delivered again are appended too
    assert v["hazards"]["revision"] is False and v["diffs"]["revision"] == 12


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
    assert rev_sql.startswith("CREATE TABLE sbx.core_load_rev USING DELTA AS\nSELECT k,\n")   # the key passes through
    for changed in ("concat(v, '_rev') AS v", "amt + 1 AS amt", "NOT flag AS flag"):   # every payload column changes
        assert changed in rev_sql
    one = next(R.parse_plan_tag(s)[2] for s in _plan(cfg=R.HazardConfig(revise_col="amt")) if R.parse_plan_tag(s)[1] == "revisions")
    assert "amt + 1 AS amt" in one and "\n       v,\n" in one and one.rstrip().split("\n")[-2].strip() == "flag"   # revise_col: that one only
    odd = R.spark_sql_plan("p.t", "s", TEMPLATE, ["k"], "ts", ["revision"], columns=[("k", "bigint"), ("tags", "array<string>"), ("ts", "timestamp")])
    assert "\n       tags,\n" in next(R.parse_plan_tag(s)[2] for s in odd if R.parse_plan_tag(s)[1] == "revisions")   # unchangeable type: passed through
    assert "WHERE ns_rev" in rev_sql
    # four baseline deliveries, the corrections delivery, then the superseded originals again; each a view + an apply
    rev_views = [s for s in plan if R.parse_plan_tag(s)[0] == "repair" and R.parse_plan_tag(s)[1].startswith("revision_") and "VIEW" in s]
    assert len(rev_views) == R.HazardConfig().n_batches + 2
    assert "SELECT * FROM sbx.core_load_rev" in rev_views[-2]
    assert rev_views[-1].endswith("SELECT ns_row.* FROM sbx.core_load_stage WHERE ns_rev")
    # revision truth is the recompute over staging plus the corrections
    diff_sql = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[0] == "diff" and R.parse_plan_tag(s)[1] == "revision")
    assert "UNION ALL" in diff_sql.split("truth AS")[0] and "sbx.core_load_rev" in diff_sql
    # staging carries the flag; cleanup drops the corrections table
    stage_sql = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[1] == "ntile")
    assert "AS ns_rev" in stage_sql
    assert "AND ns_row.ts IS NOT NULL) AS ns_rev" in stage_sql      # a row with no ordering value is never revised
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


# --------------------------------------------------------------------------------------
# a stale row after its correction: the case a blind last-write-wins upsert gets wrong
# --------------------------------------------------------------------------------------
def test_blind_last_write_wins_passes_every_delivery_hazard_and_fails_revision(events):
    """An unguarded upsert (WHEN MATCHED THEN UPDATE SET *) keeps whatever arrives last. On key-unique data that is
    right under late, duplicate, out-of-order and retried delivery and right for the corrections themselves; it is
    wrong when the superseded row arrives after its correction. That delivery is what makes it silent_wrong."""
    v = R.replay_verdict(R.last_write_wins(KEYS), events, KEYS, TS, R.HazardConfig())
    assert v["passes_snapshot"] and v["passes_run_twice"]
    assert {h: v["hazards"][h] for h in R.DELIVERY_HAZARDS} == {h: True for h in R.DELIVERY_HAZARDS}
    assert v["hazards"]["revision"] is False and v["verdict"] == "silent_wrong"
    assert v["diffs"]["revision"] == 12                          # 6 corrections overwritten by 6 stale rows

    # without the stale re-delivery the same statement would have passed: corrections alone cannot tell it apart
    sched = R.make_hazard_batches(events, KEYS, TS, R.HazardConfig())
    truth = R.recompute(pd.concat(sched["revision"], ignore_index=True), KEYS, TS)
    assert R.diff(R.run_schedule(R.last_write_wins(KEYS), sched["revision"][:-1]), truth) == 0
    assert R.diff(R.run_schedule(R.last_write_wins(KEYS), sched["revision"]), truth) == 12


def test_guarded_upsert_survives_the_stale_redelivery_in_sql(events):
    """The same pair as SQL through the duckdb adapter: the ordering guard is what makes the upsert replay-safe."""
    cols = "k, v, amt, ts"
    blind = (f"DELETE FROM target WHERE k IN (SELECT k FROM batch); INSERT INTO target SELECT {cols} FROM batch")
    guarded = ("DELETE FROM target WHERE EXISTS (SELECT 1 FROM batch b WHERE b.k = target.k AND b.ts > target.ts); "
               f"INSERT INTO target SELECT {cols} FROM batch b WHERE NOT EXISTS (SELECT 1 FROM target t WHERE t.k = b.k)")
    vb = R.replay_verdict(R.sql_apply(blind, KEYS), events, KEYS, TS, R.HazardConfig())
    vg = R.replay_verdict(R.sql_apply(guarded, KEYS), events, KEYS, TS, R.HazardConfig())
    assert vb["verdict"] == "silent_wrong" and [h for h, ok in vb["hazards"].items() if not ok] == ["revision"]
    assert vg["verdict"] == "replay_safe" and vg["uncovered"] == []


# --------------------------------------------------------------------------------------
# small tables: a check that exercises no row proves nothing
# --------------------------------------------------------------------------------------
@pytest.mark.parametrize("n", [2, 3, 5, 7])
def test_small_frames_have_no_empty_delivery_and_every_check_exercises_a_row(n):
    df = _events(n)
    cfg = R.HazardConfig()
    sched = R.make_hazard_batches(df, KEYS, TS, cfg)
    assert len(sched["baseline"]) == min(cfg.n_batches, n) and all(len(b) for b in sched["baseline"])
    cov = R.hazard_coverage(sched)
    assert set(cov) == {"run_twice", *R.HAZARDS} and all(c >= 1 for c in cov.values()), cov
    # broken duplicate handling is caught on a table this small, and the correct reference is still replay_safe
    bad = R.replay_verdict(R.dedup_within_batch_append(KEYS, TS), df, KEYS, TS, cfg)
    assert bad["verdict"] == "silent_wrong" and (not bad["hazards"]["duplicate"] or not bad["hazards"]["retried"])
    assert R.replay_verdict(R.naive_append, df, KEYS, TS, cfg)["verdict"] == "silent_wrong"
    good = R.replay_verdict(R.merge_on_keys(KEYS, TS), df, KEYS, TS, cfg)
    assert good["verdict"] == "replay_safe" and good["uncovered"] == []


@pytest.mark.parametrize("n,uncovered", [(0, ["run_twice", "late", "duplicate", "out_of_order", "retried", "revision"]),
                                         (1, ["late", "out_of_order"])])
def test_a_table_too_small_to_exercise_a_hazard_is_insufficient_coverage_not_replay_safe(n, uncovered):
    """One row cannot arrive late or out of order, and an empty table exercises nothing: even the correct reference
    is reported as insufficient_coverage, and so is the broken one that would otherwise have slipped through."""
    df = _events(n)
    for fn in (R.merge_on_keys(KEYS, TS), R.insert_if_absent(KEYS)) if n == 0 else (R.merge_on_keys(KEYS, TS),):
        v = R.replay_verdict(fn, df, KEYS, TS, R.HazardConfig())
        assert v["verdict"] == "insufficient_coverage" and v["uncovered"] == uncovered
        assert v["diff_rows"] == 0 and all(v["hazards"].values())


def test_a_disabled_hazard_is_reported_as_uncovered(events):
    v = R.replay_verdict(R.merge_on_keys(KEYS, TS), events, KEYS, TS, R.HazardConfig(rev_fraction=0.0, retry_batches=0))
    assert v["verdict"] == "insufficient_coverage" and v["uncovered"] == ["retried", "revision"]


def test_settle_verdict_rules():
    ok = {"baseline": 0, "run_twice": 0, **{h: 0 for h in R.HAZARDS}}
    cov = {"n_rows": 9, "run_twice": 2, **{h: 1 for h in R.HAZARDS}}
    assert R.settle_verdict(ok, R.HAZARDS, cov)["verdict"] == "replay_safe"
    assert R.settle_verdict(ok, R.HAZARDS, {**cov, "revision": 0})["uncovered"] == ["revision"]
    assert R.settle_verdict(ok, R.HAZARDS, {**cov, "revision": 0})["verdict"] == "insufficient_coverage"
    assert R.settle_verdict(ok, R.HAZARDS, None)["verdict"] == "insufficient_coverage"          # not measured = not proven
    assert R.settle_verdict({**ok, "late": 3}, R.HAZARDS, cov)["verdict"] == "silent_wrong"
    assert R.settle_verdict({**ok, "late": 3}, R.HAZARDS, None)["verdict"] == "silent_wrong"   # a failure stands on its own
    assert R.settle_verdict({**ok, "baseline": 1}, R.HAZARDS, cov)["verdict"] == "snapshot_wrong"
    missing = {k: v for k, v in ok.items() if k != "revision"}
    assert R.settle_verdict(missing, R.HAZARDS, cov)["verdict"] == "silent_wrong"               # a hazard with no diff did not pass
    assert R.coverage_from_row(None) == {} and R.coverage_from_row({"n_rows": 3, "late": None}) == {"n_rows": 3, "late": 0}


def test_spark_plan_small_table_flags_batches_and_coverage_statement():
    hz = list(R.HAZARDS)
    plan = R.spark_sql_plan("default.bank.core_load", "sbx", TEMPLATE, ["k"], "ts", hz, columns=COLUMNS, n_rows=3)
    stage_sql = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[1] == "ntile")
    assert "NTILE(3) OVER" in stage_sql and "COUNT(*) OVER () AS ns_total" in stage_sql      # 3 rows -> 3 deliveries, none empty
    # each sampling flag also takes the first row when the table is too small for the 1-in-10 modulus to hit
    for offset in ("", " + 1", " + 2"):
        assert f"(pmod(ns_slot{offset}, 10) = 0 OR (ns_total < 40 AND ns_slot = 1))" in stage_sql
    assert "AND ns_batch < 3) AS ns_late" in stage_sql
    base_views = [s for s in plan if R.parse_plan_tag(s)[1].startswith("baseline_") and "VIEW" in s]
    assert len(base_views) == 3
    twice_view = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[:2] == ("twice", "view"))
    assert twice_view.endswith("WHERE ns_batch = 3")
    cov_sql = next(R.parse_plan_tag(s)[2] for s in plan if R.parse_plan_tag(s)[0] == "coverage")
    assert cov_sql.startswith("SELECT COUNT(*) AS n_rows,") and cov_sql.endswith("FROM sbx.core_load_stage")
    for name in ("run_twice", *R.HAZARDS):
        assert f" AS {name}" in cov_sql
    assert "SUM(CASE WHEN ns_batch = 3 THEN 1 ELSE 0 END) AS run_twice" in cov_sql
    # without a row count the plan keeps cfg.n_batches; the coverage statement still tells the caller what ran
    assert "NTILE(4) OVER" in "\n".join(R.spark_sql_plan("default.bank.core_load", "sbx", TEMPLATE, ["k"], "ts", hz, columns=COLUMNS))
    only_late = R.spark_sql_plan("default.bank.core_load", "sbx", TEMPLATE, ["k"], "ts", ["late"])
    cov_late = next(R.parse_plan_tag(s)[2] for s in only_late if R.parse_plan_tag(s)[0] == "coverage")
    assert " AS late" in cov_late and " AS revision" not in cov_late and " AS retried" not in cov_late


# --------------------------------------------------------------------------------------
# corrections change every column; rows that cannot carry a later ordering value are not revised
# --------------------------------------------------------------------------------------
def test_an_update_that_forgets_a_column_fails_the_revision_hazard(events):
    """A guarded upsert whose UPDATE lists only some columns kept passing while a correction changed one column.
    Every payload column changes now, so the forgotten one shows."""
    insert = "INSERT INTO target SELECT k, v, amt, ts FROM batch b WHERE NOT EXISTS (SELECT 1 FROM target t WHERE t.k = b.k)"
    partial = "UPDATE target SET v = b.v, ts = b.ts FROM batch b WHERE b.k = target.k AND b.ts > target.ts; " + insert
    full = "UPDATE target SET v = b.v, amt = b.amt, ts = b.ts FROM batch b WHERE b.k = target.k AND b.ts > target.ts; " + insert
    vp = R.replay_verdict(R.sql_apply(partial, KEYS), events, KEYS, TS, R.HazardConfig())
    vf = R.replay_verdict(R.sql_apply(full, KEYS), events, KEYS, TS, R.HazardConfig())
    assert vp["verdict"] == "silent_wrong" and [h for h, ok in vp["hazards"].items() if not ok] == ["revision"]
    assert vf["verdict"] == "replay_safe"


@pytest.mark.parametrize("make_ts", [
    lambda n: pd.Series([pd.NaT] * n, dtype="datetime64[ns]"),          # no ordering value at all
    lambda n: pd.Series(np.arange(n, dtype=float) * 16 + 1e17),         # floats so large that +1 changes nothing
])
def test_ordering_values_that_cannot_move_later_leave_revision_uncovered(make_ts):
    """A correction is 'same key, later ordering value'. Where no row can be given a later value the revision check
    has nothing to act on: it is reported as uncovered, and even last-write-wins is not called replay_safe."""
    df = _events(60)
    df["ts"] = make_ts(60).values
    cov = R.hazard_coverage(R.make_hazard_batches(df, KEYS, TS, R.HazardConfig()))
    assert cov["revision"] == 0
    for fn in (R.last_write_wins(KEYS), R.merge_on_keys(KEYS, TS)):
        v = R.replay_verdict(fn, df, KEYS, TS, R.HazardConfig())
        assert v["verdict"] == "insufficient_coverage" and v["uncovered"] == ["revision"]


def test_rows_without_an_ordering_value_are_never_the_ones_revised():
    df = _events(60)
    df.loc[df["k"] % 2 == 0, "ts"] = pd.NaT
    sched = R.make_hazard_batches(df, KEYS, TS, R.HazardConfig(rev_fraction=0.5))
    corr, stale = sched["revision"][-2], sched["revision"][-1]
    assert len(corr) == 15 and corr[TS].notna().all() and (corr["k"] % 2 == 1).all() and stale[TS].notna().all()
    assert R.replay_verdict(R.merge_on_keys(KEYS, TS), df[df[TS].notna()], KEYS, TS, R.HazardConfig())["verdict"] == "replay_safe"


def test_uncovered_checks_are_reported_with_a_failing_verdict_too():
    one = _events(1)
    v = R.replay_verdict(R.insert_if_absent(KEYS), one, KEYS, TS, R.HazardConfig())
    assert v["verdict"] == "silent_wrong" and v["uncovered"] == ["late", "out_of_order"]   # wrong, and unprovable here
