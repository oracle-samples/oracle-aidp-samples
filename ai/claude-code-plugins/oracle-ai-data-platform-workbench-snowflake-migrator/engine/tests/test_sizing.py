"""Warehouse -> Spark cluster proposal. Pure."""
import pytest

from sizing.warehouse_map import (
    CREDITS_PER_HOUR, NODES_PER_SIZE, propose_all, propose_cluster,
)


def wh(name="W", size="X-Small", maxc=1, credits=None, days=30):
    return {"name": name, "size": size, "max_cluster_count": maxc,
            "min_cluster_count": 1, "observed_credits": credits,
            "observed_days": days, "auto_suspend_seconds": 600, "state": "STARTED"}


def test_xsmall_is_one_node():
    p = propose_cluster(wh(size="X-Small"))
    assert p["source_nodes"] == 1
    assert p["worker_count"] >= 1


def test_each_size_step_doubles_nodes():
    assert NODES_PER_SIZE["Small"] == 2 * NODES_PER_SIZE["X-Small"]
    assert NODES_PER_SIZE["4X-Large"] == 128


def test_credits_per_hour_doubles_too():
    assert CREDITS_PER_HOUR["X-Small"] == 1
    assert CREDITS_PER_HOUR["4X-Large"] == 128


def test_worker_count_tracks_warehouse_size():
    small = propose_cluster(wh(size="Small"))["worker_count"]
    large = propose_cluster(wh(size="Large"))["worker_count"]
    assert large > small


def test_multi_cluster_warehouse_raises_autoscale_max():
    single = propose_cluster(wh(size="Medium", maxc=1))
    multi = propose_cluster(wh(size="Medium", maxc=4))
    assert multi["autoscale_max_workers"] == 4 * single["autoscale_max_workers"]


def test_shape_family_is_flagged_as_needing_confirmation():
    # Inventing an exact OCI shape SKU would be a fabrication.
    p = propose_cluster(wh())
    assert p["shape_confirmation_required"] is True
    assert "confirm" in p["notes"].lower()


def test_unknown_size_is_blocked_not_guessed():
    p = propose_cluster(wh(size="Nano"))
    assert p["blocked"] is True
    assert "Nano" in p["reason"]


def test_credits_reported_when_observed():
    out = propose_all([wh(size="Medium", credits=600.0, days=30)])
    assert out["observed_credits_total"] == 600.0
    assert out["credits_basis"] == "observed"


def test_credits_estimated_from_size_when_not_observed():
    out = propose_all([wh(size="Medium", credits=None)])
    assert out["credits_basis"] == "declared_size_only"
    assert out["observed_credits_total"] is None


def test_no_cost_without_an_explicit_credit_price():
    # Snowflake credit price varies by edition and region; guessing it would
    # produce a number that looks authoritative and is not.
    out = propose_all([wh(size="Medium", credits=600.0)])
    assert out["cost_model"] is None
    assert "credit price" in out["cost_note"].lower()


def test_cost_computed_when_a_price_is_supplied():
    out = propose_all([wh(size="Medium", credits=600.0, days=30)],
                      credit_price_usd=3.0)
    assert out["cost_model"]["snowflake_monthly_usd"] == pytest.approx(1800.0)
    assert out["cost_model"]["credit_price_usd"] == 3.0


def test_totals_and_concurrency_ceiling():
    out = propose_all([wh("A", "Small", maxc=2), wh("B", "Large", maxc=3)])
    assert out["warehouse_count"] == 2
    assert out["max_concurrent_clusters"] == 5
    assert out["total_source_nodes"] == NODES_PER_SIZE["Small"] + NODES_PER_SIZE["Large"]


def test_blocked_warehouses_listed_separately():
    out = propose_all([wh("A", "Small"), wh("B", "Nano")])
    assert [b["name"] for b in out["blocked"]] == ["B"]
    assert len(out["proposals"]) == 1
