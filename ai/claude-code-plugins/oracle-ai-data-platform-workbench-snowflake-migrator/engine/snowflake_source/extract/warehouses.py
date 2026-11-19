"""Warehouse inventory. Read-only.

`SHOW WAREHOUSES` needs no special privilege and gives size, scaling policy and
auto-suspend -- but it returns only the warehouses the current role holds a
privilege on (USAGE, MONITOR, OPERATE or OWNERSHIP), so an empty list says
something about the role before it says anything about the account, and the
result records that. Actual consumed credits live in
`SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`, which needs a grant a
customer may refuse -- so metering is optional and its absence is recorded, never
silently treated as zero consumption.
"""
from __future__ import annotations

import datetime
from typing import Callable

__all__ = ["extract_warehouses", "SIZE_ORDER"]

# Snowflake warehouse sizes, smallest first. Each step doubles compute.
SIZE_ORDER = ("X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large",
              "3X-Large", "4X-Large", "5X-Large", "6X-Large")


def _size_rank(size: str) -> int:
    try:
        return SIZE_ORDER.index(size)
    except ValueError:
        return -1


def _int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def extract_warehouses(run_sql: Callable[..., list[dict]], *,
                       metering_days: int = 30) -> dict:
    rows = run_sql("show warehouses")

    metering: dict[str, dict] = {}
    metering_source, metering_note = "unavailable", ""
    try:
        for m in run_sql(
                "select warehouse_name as WAREHOUSE_NAME, "
                "       sum(credits_used) as CREDITS, "
                f"      {metering_days} as DAYS "
                "from snowflake.account_usage.warehouse_metering_history "
                f"where start_time >= dateadd(day, -{metering_days}, current_date) "
                "group by warehouse_name"):
            metering[m["WAREHOUSE_NAME"]] = m
        metering_source = "account_usage"
    except Exception as exc:
        metering_note = (
            f"WAREHOUSE_METERING_HISTORY unavailable ({exc}); credit consumption is "
            "unknown. Sizing below is derived from declared warehouse size only, "
            "not from observed usage.")

    warehouses = []
    for r in rows:
        name = r["name"]
        m = metering.get(name)
        warehouses.append({
            "name": name,
            "size": r.get("size"),
            "size_rank": _size_rank(r.get("size")),
            "state": r.get("state"),
            "type": r.get("type"),
            "min_cluster_count": _int(r.get("min_cluster_count"), 1),
            "max_cluster_count": _int(r.get("max_cluster_count"), 1),
            "auto_suspend_seconds": _int(r.get("auto_suspend")),
            "auto_resume": str(r.get("auto_resume", "")).lower() == "true",
            "owner": r.get("owner"),
            "comment": r.get("comment"),
            "observed_credits": (float(m["CREDITS"]) if m and m.get("CREDITS")
                                 is not None else None),
            "observed_days": (_int(m.get("DAYS")) if m else None),
        })
    warehouses.sort(key=lambda w: (-w["size_rank"], w["name"]))

    visibility_note = ""
    if not warehouses:
        visibility_note = (
            "0 warehouses visible to the current role: SHOW WAREHOUSES lists "
            "only warehouses the role holds USAGE, MONITOR, OPERATE or "
            "OWNERSHIP on, so the compute proposal covers only what this role "
            "can see. Re-run with a role that can see every warehouse before "
            "sizing the target.")

    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "warehouse_count": len(warehouses),
        "max_concurrent_clusters": sum(w["max_cluster_count"] for w in warehouses),
        "metering_source": metering_source,
        "metering_note": metering_note,
        "visibility_note": visibility_note,
        "warehouses": warehouses,
    }
