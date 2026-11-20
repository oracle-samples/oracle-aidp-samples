"""Table-driven tests for nightshift.tiers (pure functions, no live calls)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from nightshift.config import JOB_CRON_MIN_MINUTES, MODEL_LADDER, TIERS
from nightshift.gate import cron_min_interval_minutes
from nightshift.tiers import (
    DEFAULT_THRESHOLDS,
    PIPELINE_CLASSES,
    compute_tier,
    default_policy_rows,
    policy_for,
    raw_tier,
    worse,
)
from nightshift.types import TickContext

NOW = datetime(2026, 11, 3, 3, 0, tzinfo=timezone.utc)


def ctx(remaining=80.0, overrun=0.0, llm=80.0, source="ledger") -> TickContext:
    return TickContext(
        now_utc=NOW,
        budget_remaining_pct=remaining,
        projected_overrun_pct=overrun,
        llm_remaining_pct=llm,
        budget_source=source,
    )


def run_sequence(readings, prev=None, kv=None, thresholds=DEFAULT_THRESHOLDS):
    """Feed successive readings through compute_tier, merging kv updates like the actuator would."""
    kv = dict(kv or {})
    out = []
    for reading in readings:
        c = reading if isinstance(reading, TickContext) else ctx(**reading)
        prev, updates = compute_tier(c, prev, kv, thresholds)
        kv.update(updates)
        out.append(prev)
    return out, kv


# ---------------------------------------------------------------- raw_tier ladder

@pytest.mark.parametrize(
    "remaining, overrun, source, expected",
    [
        (None, None, "unknown", "low_compute"),  # unknown budget, no cache
        (None, None, "ledger", "low_compute"),  # ledger read gave nothing
        (0.0, None, "unknown", "low_compute"),  # unknown source never pauses
        (95.0, 0.0, "ledger", "high"),
        (60.01, None, "ledger", "high"),
        (60.0, 0.0, "ledger", "normal"),  # boundary: 60 is not "more than 60"
        (95.0, 5.0, "ledger", "normal"),  # projected overrun blocks high
        (45.0, 0.0, "cached", "normal"),
        (30.0, 0.0, "ledger", "normal"),  # boundary: 30 is not below 30
        (29.9, 0.0, "ledger", "low_compute"),
        (95.0, 10.1, "ledger", "low_compute"),  # overrun > 10 forces low_compute
        (95.0, 10.0, "ledger", "normal"),  # overrun exactly 10 does not
        (10.0, 0.0, "ledger", "low_compute"),  # boundary: 10 is not below 10
        (9.9, 0.0, "ledger", "critical"),
        (95.0, 25.1, "ledger", "critical"),  # overrun > 25 forces critical
        (0.0, 0.0, "ledger", "paused"),
        (-3.0, 0.0, "ledger", "paused"),
    ],
)
def test_raw_tier_ladder(remaining, overrun, source, expected):
    assert raw_tier(remaining, overrun, source) == expected


def test_raw_tier_custom_thresholds():
    t = dict(DEFAULT_THRESHOLDS, high_pct=80.0, normal_pct=50.0)
    assert raw_tier(70.0, 0.0, "ledger", t) == "normal"
    assert raw_tier(49.0, 0.0, "ledger", t) == "low_compute"


@pytest.mark.parametrize(
    "a, b, expected",
    [("high", "normal", "normal"), ("normal", "high", "normal"), ("low_compute", "critical", "critical"),
     ("paused", "high", "paused"), ("normal", "normal", "normal")],
)
def test_worse(a, b, expected):
    assert worse(a, b) == expected


# ---------------------------------------------------------------- compute_tier: LLM worse-wins

@pytest.mark.parametrize(
    "remaining, llm, expected",
    [
        (80.0, 80.0, "high"),
        (80.0, 45.0, "normal"),  # LLM budget drags compute-high down
        (80.0, 20.0, "low_compute"),
        (80.0, 5.0, "critical"),
        (20.0, 80.0, "low_compute"),  # compute drags LLM-high down
        (80.0, None, "low_compute"),  # unknown LLM budget is conservative
    ],
)
def test_llm_budget_worse_wins_first_tick(remaining, llm, expected):
    tier, _ = compute_tier(ctx(remaining=remaining, llm=llm), None, {})
    assert tier == expected


def test_first_tick_adopts_candidate_without_hysteresis():
    tier, updates = compute_tier(ctx(remaining=45.0), None, {})
    assert tier == "normal"
    assert updates["pending_tier"] is None and updates["pending_ticks"] == 0


# ---------------------------------------------------------------- hysteresis, both directions

@pytest.mark.parametrize(
    "start, readings, expected",
    [
        # downgrade high -> normal needs two ticks
        ("high", [dict(remaining=45.0)] * 3, ["high", "normal", "normal"]),
        # upgrade normal -> high needs two ticks
        ("normal", [dict(remaining=90.0)] * 3, ["normal", "high", "high"]),
        # low_compute -> critical
        ("low_compute", [dict(remaining=5.0)] * 2, ["low_compute", "critical"]),
        # critical -> low_compute (recovery)
        ("critical", [dict(remaining=20.0)] * 2, ["critical", "low_compute"]),
        # normal -> low_compute via overrun
        ("normal", [dict(remaining=70.0, overrun=15.0)] * 2, ["normal", "low_compute"]),
        # a one-tick blip does not flip the tier
        ("high", [dict(remaining=45.0), dict(remaining=90.0), dict(remaining=45.0)], ["high", "high", "high"]),
        # pending resets when the candidate changes mid-way
        ("high", [dict(remaining=45.0), dict(remaining=20.0), dict(remaining=20.0)], ["high", "high", "low_compute"]),
    ],
)
def test_hysteresis_sequences(start, readings, expected):
    tiers, _ = run_sequence(readings, prev=start)
    assert tiers == expected


def test_hysteresis_kv_bookkeeping():
    tier, upd = compute_tier(ctx(remaining=45.0), "high", {})
    assert tier == "high" and upd["pending_tier"] == "normal" and upd["pending_ticks"] == 1
    tier, upd = compute_tier(ctx(remaining=45.0), "high", upd)
    assert tier == "normal" and upd["pending_tier"] is None and upd["pending_ticks"] == 0


def test_hysteresis_three_ticks_when_configured():
    t = dict(DEFAULT_THRESHOLDS, hysteresis_ticks=3)
    tiers, _ = run_sequence([dict(remaining=45.0)] * 4, prev="high", thresholds=t)
    assert tiers == ["high", "high", "normal", "normal"]


# ---------------------------------------------------------------- grace before paused

def test_grace_two_zero_ticks_then_paused():
    tiers, kv = run_sequence([dict(remaining=0.0)] * 3, prev="critical")
    assert tiers == ["critical", "paused", "paused"]
    assert kv["exhausted_ticks"] == 3
    assert kv["exhausted_since"] == NOW.isoformat()


def test_grace_from_normal_holds_then_pauses():
    # hysteresis and grace both satisfied on the second zero tick
    tiers, _ = run_sequence([dict(remaining=0.0)] * 2, prev="normal")
    assert tiers == ["normal", "paused"]


def test_grace_first_tick_zero_is_critical_not_paused():
    tier, upd = compute_tier(ctx(remaining=0.0), None, {})
    assert tier == "critical"
    assert upd["exhausted_ticks"] == 1 and upd["exhausted_since"] == NOW.isoformat()


def test_grace_clears_when_condition_lifts():
    _, kv = run_sequence([dict(remaining=0.0), dict(remaining=50.0)], prev="critical")
    assert kv["exhausted_ticks"] == 0 and kv["exhausted_since"] is None


def test_grace_blip_does_not_pause():
    tiers, _ = run_sequence([dict(remaining=0.0), dict(remaining=5.0), dict(remaining=0.0)], prev="critical")
    assert "paused" not in tiers


def test_grace_longer_than_hysteresis():
    t = dict(DEFAULT_THRESHOLDS, grace_ticks=3)
    tiers, _ = run_sequence([dict(remaining=0.0)] * 4, prev="normal", thresholds=t)
    assert tiers == ["normal", "critical", "paused", "paused"]


def test_llm_zero_also_pauses_after_grace():
    tiers, _ = run_sequence([dict(remaining=80.0, llm=0.0)] * 2, prev="critical")
    assert tiers == ["critical", "paused"]


# ---------------------------------------------------------------- unknown budget never paused

@pytest.mark.parametrize("source", ["unknown", "ledger", "cached"])
def test_unknown_budget_never_paused(source):
    readings = [dict(remaining=None, llm=None, source=source)] * 5
    tiers, kv = run_sequence(readings, prev="high")
    assert tiers == ["high", "low_compute", "low_compute", "low_compute", "low_compute"]
    assert kv["exhausted_ticks"] == 0


def test_unknown_source_with_zero_reading_never_paused():
    tiers, _ = run_sequence([dict(remaining=0.0, llm=0.0, source="unknown")] * 4, prev="normal")
    assert "paused" not in tiers and tiers[-1] == "low_compute"


def test_unknown_budget_while_paused_stays_paused():
    tiers, _ = run_sequence([dict(remaining=None, llm=None, source="unknown")] * 3, prev="paused")
    assert tiers == ["paused"] * 3


# ---------------------------------------------------------------- leaving paused needs approval

def test_leaving_paused_requires_approval():
    tiers, kv = run_sequence([dict(remaining=90.0)] * 4, prev="paused")
    assert tiers == ["paused"] * 4
    assert kv["pending_tier"] == "high" and kv["pending_ticks"] == 4


def test_leaving_paused_with_approval_after_hysteresis():
    tiers, kv = run_sequence([dict(remaining=90.0)] * 2, prev="paused", kv={"resume_approved": True})
    assert tiers == ["paused", "high"]
    assert kv["resume_approved"] is False  # approval consumed
    assert kv["pending_tier"] is None


def test_approval_arriving_later_flips_immediately_once_hysteresis_met():
    tiers, kv = run_sequence([dict(remaining=50.0)] * 3, prev="paused")
    assert tiers == ["paused"] * 3
    kv["resume_approved"] = True
    tier, upd = compute_tier(ctx(remaining=50.0), "paused", kv)
    assert tier == "normal" and upd["resume_approved"] is False


def test_approval_not_consumed_while_still_exhausted():
    tier, upd = compute_tier(ctx(remaining=0.0), "paused", {"resume_approved": True, "exhausted_ticks": 5})
    assert tier == "paused"
    assert "resume_approved" not in upd


# ---------------------------------------------------------------- determinism

def test_compute_tier_is_pure_and_deterministic():
    kv = {"pending_tier": "normal", "pending_ticks": 1}
    snapshot = dict(kv)
    a = compute_tier(ctx(remaining=45.0), "high", kv)
    b = compute_tier(ctx(remaining=45.0), "high", kv)
    assert a == b and kv == snapshot


# ---------------------------------------------------------------- policy rows

def test_default_policy_rows_cover_every_tier_and_class():
    rows = default_policy_rows()
    assert len(rows) == len(TIERS) * len(PIPELINE_CLASSES)
    keys = {(r["tier"], r["pipeline_class"]) for r in rows}
    assert keys == {(t, c) for t in TIERS for c in PIPELINE_CLASSES}
    expected_keys = {"tier", "pipeline_class", "cron", "min_workers", "max_workers", "auto_terminate_min",
                     "ai_model", "sample_pct", "backfill_concurrency", "enabled"}
    assert all(set(r) == expected_keys for r in rows)


def test_default_policy_rows_models_from_ladder_never_openai():
    for row in default_policy_rows():
        assert row["ai_model"] == MODEL_LADDER[row["tier"]]
        assert not (row["ai_model"] or "").startswith("openai")


def test_default_policy_rows_cron_respects_platform_minimum():
    for row in default_policy_rows():
        assert cron_min_interval_minutes(row["cron"]) >= JOB_CRON_MIN_MINUTES, row


@pytest.mark.parametrize(
    "tier, cls, cron_interval, enabled, min_w, max_w, auto_term, sample, backfill",
    [
        ("high", "bronze", 60, True, 2, 10, 60, 100, 4),
        ("high", "optimize", 1440, True, 2, 10, 60, 100, 4),
        ("normal", "silver", 1440, True, 1, 6, 30, 100, 2),
        ("low_compute", "bronze", 180, True, 1, 3, 15, 20, 1),
        ("low_compute", "optimize", 1440, False, 1, 3, 15, 20, 1),
        ("critical", "bronze", 360, True, 1, 1, 5, 0, 0),
        ("critical", "gold", 1440, True, 1, 1, 5, 0, 0),
        ("paused", "bronze", 60, False, 1, 1, 5, 0, 0),
        ("paused", "gold", 1440, True, 1, 1, 5, 0, 0),
    ],
)
def test_policy_for_matches_report_table(tier, cls, cron_interval, enabled, min_w, max_w, auto_term, sample, backfill):
    row = policy_for(tier, cls, default_policy_rows())
    assert row is not None
    assert cron_min_interval_minutes(row["cron"]) == cron_interval
    assert row["enabled"] is enabled
    assert (row["min_workers"], row["max_workers"], row["auto_terminate_min"]) == (min_w, max_w, auto_term)
    assert (row["sample_pct"], row["backfill_concurrency"]) == (sample, backfill)
    assert 1 <= row["min_workers"] <= row["max_workers"] <= 24


def test_policy_for_missing_returns_none():
    assert policy_for("high", "platinum", default_policy_rows()) is None
    assert policy_for("nope", "gold", default_policy_rows()) is None
    assert policy_for("high", "gold", []) is None


def test_paused_disables_all_but_gold():
    rows = default_policy_rows()
    assert policy_for("paused", "gold", rows)["enabled"] is True
    for cls in ("bronze", "silver", "optimize"):
        assert policy_for("paused", cls, rows)["enabled"] is False
