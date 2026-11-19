"""Snowflake warehouse -> AIDP Spark cluster proposal. Pure, zero I/O.

Snowflake warehouse sizing is a clean doubling series: X-Small is 1 node and 1
credit/hour, and every step doubles both. That much is documented and safe to
encode.

What is NOT encoded is an exact OCI compute shape SKU. Shape availability varies
by tenancy and region, so every proposal carries
`shape_confirmation_required: True` and names a shape FAMILY rather than
asserting a SKU that may not exist in the customer's region.

Cost is only computed when the caller supplies a credit price. Snowflake credit
prices vary by edition and region; defaulting one would produce a number that
looks authoritative and is not.
"""
from __future__ import annotations

import re

__all__ = ["CREDITS_PER_HOUR", "NODES_PER_SIZE", "cluster_base_name",
           "cluster_names", "propose_cluster", "propose_all"]

_WH_SUFFIX = re.compile(r"[_\-]?(WH|WAREHOUSE)$", re.IGNORECASE)


def cluster_base_name(warehouse_name: str) -> str:
    """The name a new cluster takes: the warehouse's base name, without a
    trailing _WH / _WAREHOUSE, in AIDP's safe charset ([a-z0-9_])."""
    from target.naming import translate_name
    base = _WH_SUFFIX.sub("", str(warehouse_name).strip()) or str(warehouse_name)
    return translate_name(base, kind="cluster").name


def cluster_names(warehouse_names, *, reserved=()) -> dict[str, str]:
    """{warehouse: cluster name}, one DISTINCT name per warehouse.

    The base name (COMPUTE_WH -> compute) folds suffix and case, so two
    warehouses can land on one name -- COMPUTE_WH and COMPUTE -- and would
    then share one cluster. A base name shared by two warehouses, or equal
    to a `reserved` name (the migration cluster's), falls back to the full
    translated warehouse name (compute_wh); one that still collides gets a
    numbered suffix. Assigned in sorted order, so the input order never
    decides which warehouse keeps which name.
    """
    from target.naming import translate_name
    names = sorted({str(w).strip() for w in warehouse_names
                    if str(w or "").strip()})
    base = {w: cluster_base_name(w) for w in names}
    counts: dict[str, int] = {}
    for n in base.values():
        counts[n] = counts.get(n, 0) + 1
    used = set(reserved)
    out = {}
    for w in names:
        name = base[w]
        if counts[name] > 1 or name in used:
            name = translate_name(w, kind="cluster").name
        candidate, n = name, 2
        while candidate in used:
            candidate, n = f"{name}_{n}", n + 1
        used.add(candidate)
        out[w] = candidate
    return out

# Documented Snowflake series: each size doubles.
NODES_PER_SIZE = {
    "X-Small": 1, "Small": 2, "Medium": 4, "Large": 8, "X-Large": 16,
    "2X-Large": 32, "3X-Large": 64, "4X-Large": 128, "5X-Large": 256,
    "6X-Large": 512,
}
CREDITS_PER_HOUR = dict(NODES_PER_SIZE)

# A Snowflake "server" is ~8 vCPU / 16 GB. Spark workers are provisioned larger,
# so a worker absorbs several Snowflake nodes' worth of compute.
_VCPU_PER_SOURCE_NODE = 8
_MEM_GB_PER_SOURCE_NODE = 16
_VCPU_PER_WORKER = 16
_SHAPE_FAMILY = "VM.Standard.E5.Flex (or the tenancy's current standard flex family)"


def propose_cluster(warehouse: dict, *, mode: str = "new",
                    existing_cluster_id: str | None = None,
                    cluster_name: str | None = None) -> dict:
    """Propose a standard-sized Spark cluster for one warehouse.

    mode `new` proposes creating a cluster named from the warehouse's base
    name; `existing` points the warehouse at `existing_cluster_id` and
    proposes creating nothing -- the sizing is then advisory only.
    """
    size = warehouse.get("size")
    if size not in NODES_PER_SIZE:
        return {"name": warehouse.get("name"), "blocked": True,
                "reason": f"unrecognised Snowflake warehouse size {size!r}; "
                          "refusing to guess a compute equivalent"}

    nodes = NODES_PER_SIZE[size]
    vcpu = nodes * _VCPU_PER_SOURCE_NODE
    workers = max(1, vcpu // _VCPU_PER_WORKER)
    max_clusters = max(1, int(warehouse.get("max_cluster_count") or 1))

    existing = mode == "existing"
    base = cluster_base_name(warehouse.get("name") or "")
    name = cluster_name or base
    action_note = (
        f" Uses the EXISTING cluster {existing_cluster_id}; it is not resized, "
        f"so the sizing above is advisory." if existing else
        f" Proposed as a NEW cluster named `{name}`"
        + (f" (its base name `{base}` would collide with another "
           f"warehouse's or the migration cluster's, so it keeps more of "
           f"its own name)." if name != base else "."))
    return {
        "name": warehouse.get("name"),
        "blocked": False,
        "cluster_action": "use_existing" if existing else "create",
        "target_cluster": existing_cluster_id if existing else name,
        "source_size": size,
        "source_nodes": nodes,
        "source_vcpu_equivalent": vcpu,
        "source_memory_gb_equivalent": nodes * _MEM_GB_PER_SOURCE_NODE,
        "driver_shape_family": _SHAPE_FAMILY,
        "worker_shape_family": _SHAPE_FAMILY,
        "worker_ocpus": _VCPU_PER_WORKER // 2,   # OCPU = 2 vCPU on x86 flex shapes
        "worker_count": workers,
        "autoscale_min_workers": workers,
        "autoscale_max_workers": workers * max_clusters,
        "auto_suspend_seconds": warehouse.get("auto_suspend_seconds"),
        "shape_confirmation_required": True,
        "notes": (
            f"{size} = {nodes} Snowflake node(s) ~= {vcpu} vCPU. Proposed "
            f"{workers} worker(s); autoscale max {workers * max_clusters} to cover "
            f"max_cluster_count={max_clusters}. Confirm the shape family and OCPU "
            "availability against the target tenancy and region before use."
            + action_note),
    }


def propose_all(warehouses: list[dict], *,
                credit_price_usd: float | None = None, mode: str = "new",
                existing_cluster_id: str | None = None,
                reserved=("migration_assets",)) -> dict:
    proposals, blocked = [], []
    # Distinct names across the whole set, so N proposals are N clusters;
    # `reserved` is the migration cluster's default name.
    names = cluster_names([wh.get("name") or "" for wh in warehouses],
                          reserved=reserved)
    for wh in warehouses:
        p = propose_cluster(wh, mode=mode,
                            existing_cluster_id=existing_cluster_id,
                            cluster_name=names.get(
                                str(wh.get("name") or "").strip()))
        (blocked if p.get("blocked") else proposals).append(p)

    observed = [w.get("observed_credits") for w in warehouses
                if w.get("observed_credits") is not None]
    credits_total = sum(observed) if observed else None
    days = next((w.get("observed_days") for w in warehouses
                 if w.get("observed_credits") is not None), None)

    cost_model, cost_note = None, ""
    if credit_price_usd is None:
        cost_note = ("No cost model: a credit price was not supplied. Snowflake "
                     "credit prices vary by edition and region, so one is never "
                     "assumed. Pass --credit-price to compute spend.")
    elif credits_total is None:
        cost_note = ("No cost model: credit consumption was not observable "
                     "(ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY unavailable), so "
                     "there is nothing to price.")
    else:
        per_day = credits_total / days if days else credits_total
        cost_model = {
            "credit_price_usd": credit_price_usd,
            "observed_credits": credits_total,
            "observed_days": days,
            "snowflake_monthly_usd": round(per_day * 30 * credit_price_usd, 2),
            "snowflake_annual_usd": round(per_day * 365 * credit_price_usd, 2),
            "basis": "observed WAREHOUSE_METERING_HISTORY",
            "aidp_comparison": (
                "AIDP cost depends on the confirmed cluster shapes and their "
                "running hours, which cannot be derived from Snowflake metering "
                "alone. Price the proposed shapes against the tenancy rate card "
                "once the shape family is confirmed."),
        }

    return {
        "warehouse_count": len(warehouses),
        "max_concurrent_clusters": sum(
            max(1, int(w.get("max_cluster_count") or 1)) for w in warehouses),
        "total_source_nodes": sum(
            NODES_PER_SIZE.get(w.get("size"), 0) for w in warehouses),
        "observed_credits_total": credits_total,
        "credits_basis": "observed" if observed else "declared_size_only",
        "cost_model": cost_model,
        "cost_note": cost_note,
        "proposals": proposals,
        "blocked": blocked,
        "cluster_mode": mode,
        "existing_cluster_id": existing_cluster_id,
        "clusters_to_create": (0 if mode == "existing" else len(
            {p["target_cluster"] for p in proposals})),
    }
