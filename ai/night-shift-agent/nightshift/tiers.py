"""Budget -> tier ladder with two-tick hysteresis and two-tick grace before paused.

Pure functions only: no I/O, no clock reads. ``compute_tier`` returns the kv updates it wants persisted.
"""
from __future__ import annotations

from typing import Any

from nightshift.config import JOB_CRON_MIN_MINUTES, MODEL_LADDER, TIERS
from nightshift.types import TickContext

DEFAULT_THRESHOLDS: dict[str, float | int] = dict(
    high_pct=60.0,
    normal_pct=30.0,
    low_pct=10.0,
    overrun_low_pct=10.0,
    overrun_critical_pct=25.0,
    grace_ticks=2,
    hysteresis_ticks=2,
)

SEVERITY: dict[str, int] = {tier: rank for rank, tier in enumerate(TIERS)}
"""Severity order: high < normal < low_compute < critical < paused."""

PIPELINE_CLASSES = ("bronze", "silver", "gold", "optimize")

# kv keys owned by this module
KV_PENDING_TIER = "pending_tier"
KV_PENDING_TICKS = "pending_ticks"
KV_EXHAUSTED_SINCE = "exhausted_since"
KV_EXHAUSTED_TICKS = "exhausted_ticks"
KV_RESUME_APPROVED = "resume_approved"


def worse(a: str, b: str) -> str:
    """Return the more severe of two tiers."""
    return a if SEVERITY[a] >= SEVERITY[b] else b


def raw_tier(
    remaining_pct: float | None,
    projected_overrun_pct: float | None,
    budget_source: str,
    thresholds: dict[str, Any] = DEFAULT_THRESHOLDS,
) -> str:
    """Map one budget reading to a tier with no memory.

    Unknown remaining (None; the no-cache case has ``budget_source == 'unknown'``) is ``low_compute``:
    an unknown budget is conservative but never pauses. ``remaining <= 0`` returns ``paused`` as a
    *candidate*; ``compute_tier`` applies the grace period before anything is actually paused.
    """
    if remaining_pct is None or budget_source == "unknown":
        return "low_compute"
    overrun = projected_overrun_pct or 0.0
    if remaining_pct <= 0:
        return "paused"
    if remaining_pct < thresholds["low_pct"] or overrun > thresholds["overrun_critical_pct"]:
        return "critical"
    if remaining_pct < thresholds["normal_pct"] or overrun > thresholds["overrun_low_pct"]:
        return "low_compute"
    if remaining_pct > thresholds["high_pct"] and overrun <= 0:
        return "high"
    return "normal"


def compute_tier(
    ctx: TickContext,
    prev_tier: str | None,
    kv: dict[str, Any],
    thresholds: dict[str, Any] = DEFAULT_THRESHOLDS,
) -> tuple[str, dict[str, Any]]:
    """Combine compute and LLM budgets (worse wins), then apply hysteresis, grace and the resume lock.

    * Hysteresis: a change away from ``prev_tier`` in either direction needs ``hysteresis_ticks``
      consecutive ticks of the same raw tier (``kv['pending_tier']`` / ``kv['pending_ticks']``).
    * Grace: ``paused`` additionally needs ``grace_ticks`` consecutive zero-remaining ticks
      (``kv['exhausted_since']`` / ``kv['exhausted_ticks']``); until then the tier is ``critical``.
    * Resume lock: leaving ``paused`` requires ``kv['resume_approved']`` truthy; it is consumed on exit.
    * First tick (``prev_tier is None``) adopts the candidate directly (grace still applies).

    Returns ``(tier, kv_updates)``; the caller merges ``kv_updates`` into its kv store.
    """
    grace_ticks = int(thresholds["grace_ticks"])
    hysteresis_ticks = int(thresholds["hysteresis_ticks"])

    compute_raw = raw_tier(ctx.budget_remaining_pct, ctx.projected_overrun_pct, ctx.budget_source, thresholds)
    llm_raw = raw_tier(ctx.llm_remaining_pct, None, ctx.budget_source, thresholds)
    candidate = worse(compute_raw, llm_raw)

    updates: dict[str, Any] = {}

    # Grace bookkeeping: consecutive ticks at zero remaining.
    if candidate == "paused":
        exhausted_ticks = int(kv.get(KV_EXHAUSTED_TICKS) or 0) + 1
        exhausted_since = kv.get(KV_EXHAUSTED_SINCE) or ctx.now_utc.isoformat()
    else:
        exhausted_ticks = 0
        exhausted_since = None
    updates[KV_EXHAUSTED_TICKS] = exhausted_ticks
    updates[KV_EXHAUSTED_SINCE] = exhausted_since

    # Hysteresis bookkeeping.
    pending_tier: str | None = kv.get(KV_PENDING_TIER)
    pending_ticks = int(kv.get(KV_PENDING_TICKS) or 0)
    if prev_tier is None:
        tier = candidate
    elif candidate == prev_tier:
        tier = prev_tier
        pending_tier, pending_ticks = None, 0
    else:
        pending_ticks = pending_ticks + 1 if pending_tier == candidate else 1
        pending_tier = candidate
        tier = candidate if pending_ticks >= hysteresis_ticks else prev_tier

    # Grace gate (on entry only): zero remaining is at least critical, paused only after the grace.
    if tier == "paused" and prev_tier != "paused" and exhausted_ticks < grace_ticks:
        tier = "critical"

    # Resume lock: nothing leaves paused without an approval.
    if prev_tier == "paused" and tier != "paused":
        if kv.get(KV_RESUME_APPROVED):
            updates[KV_RESUME_APPROVED] = False
        else:
            tier = "paused"

    if tier == candidate:
        pending_tier, pending_ticks = None, 0
    updates[KV_PENDING_TIER] = pending_tier
    updates[KV_PENDING_TICKS] = pending_ticks
    return tier, updates


def policy_for(tier: str, pipeline_class: str, policy_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the policy row for ``(tier, pipeline_class)`` or None."""
    for row in policy_rows:
        if row.get("tier") == tier and row.get("pipeline_class") == pipeline_class:
            return row
    return None


# Cron strings. All respect the platform minimum (JOB_CRON_MIN_MINUTES = 30).
CRON_HOURLY = "0 * * * *"
CRON_EVERY_3H = "0 */3 * * *"
CRON_EVERY_6H = "0 */6 * * *"
CRON_NIGHTLY = "0 2 * * *"
CRON_WEEKLY = "0 3 * * 0"
assert JOB_CRON_MIN_MINUTES <= 60, "hourly cadence would violate the platform cron minimum"

# tier -> (min_workers, max_workers, auto_terminate_min, sample_pct, backfill_concurrency)
_CLUSTER_PROFILE: dict[str, tuple[int, int, int, int, int]] = {
    "high": (2, 10, 60, 100, 4),
    "normal": (1, 6, 30, 100, 2),
    "low_compute": (1, 3, 15, 20, 1),
    "critical": (1, 1, 5, 0, 0),
    "paused": (1, 1, 5, 0, 0),
}

# tier -> class -> (cron, enabled). "skipped" rows keep their cron but are disabled.
_CADENCE: dict[str, dict[str, tuple[str, bool]]] = {
    "high": {
        "bronze": (CRON_HOURLY, True), "silver": (CRON_NIGHTLY, True),
        "gold": (CRON_NIGHTLY, True), "optimize": (CRON_WEEKLY, True),
    },
    "normal": {
        "bronze": (CRON_HOURLY, True), "silver": (CRON_NIGHTLY, True),
        "gold": (CRON_NIGHTLY, True), "optimize": (CRON_WEEKLY, True),
    },
    "low_compute": {
        "bronze": (CRON_EVERY_3H, True), "silver": (CRON_NIGHTLY, True),
        "gold": (CRON_NIGHTLY, True), "optimize": (CRON_WEEKLY, False),
    },
    "critical": {
        "bronze": (CRON_EVERY_6H, True), "silver": (CRON_NIGHTLY, True),
        "gold": (CRON_NIGHTLY, True), "optimize": (CRON_WEEKLY, False),
    },
    "paused": {
        "bronze": (CRON_HOURLY, False), "silver": (CRON_NIGHTLY, False),
        "gold": (CRON_NIGHTLY, True), "optimize": (CRON_WEEKLY, False),
    },
}


def default_policy_rows() -> list[dict[str, Any]]:
    """Encode the tier ladder as rows for every (tier, pipeline_class); models from ``MODEL_LADDER``."""
    rows: list[dict[str, Any]] = []
    for tier in TIERS:
        min_w, max_w, auto_term, sample_pct, backfill = _CLUSTER_PROFILE[tier]
        for pipeline_class in PIPELINE_CLASSES:
            cron, enabled = _CADENCE[tier][pipeline_class]
            rows.append(
                dict(
                    tier=tier,
                    pipeline_class=pipeline_class,
                    cron=cron,
                    min_workers=min_w,
                    max_workers=max_w,
                    auto_terminate_min=auto_term,
                    ai_model=MODEL_LADDER[tier],
                    sample_pct=sample_pct,
                    backfill_concurrency=backfill,
                    enabled=enabled,
                )
            )
    return rows


__all__ = [
    "DEFAULT_THRESHOLDS", "SEVERITY", "PIPELINE_CLASSES", "worse", "raw_tier", "compute_tier",
    "policy_for", "default_policy_rows",
]
