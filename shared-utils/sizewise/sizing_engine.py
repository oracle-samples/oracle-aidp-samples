"""
SizeWise — AIDP capacity sizing engine.

Turns customer business inputs (data volume, sources, users, concurrency, SLA,
growth, workload mix, ...) into a defensible AIDP cluster topology + cost, for
three scenarios (Conservative / Balanced / Cost-Optimized).

Implements the frozen sizing-model spec v1.0. The Balanced scenario is
calibrated to reproduce the accepted Entel v20 reference (~$62k/mo compute+
services) within -0.07%. All tunables live in SizingConfig / config.yaml; the
compute cost math is delegated to pricing.py (mirrors Oracle AIDPU rates).

Entry point:
    result = size(SizingInput(...), SizingConfig.load())

CLI:
    python3 sizing_engine.py                 # runs the Entel calibration case
    python3 sizing_engine.py inputs.json     # sizes from a JSON inputs file
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

from pricing import AIDPPricing, AIDPCluster, DAYS_PER_MONTH

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_VERSION = "1.0"
SCENARIO_KEYS = ["conservative", "balanced", "cost_optimized"]
SCENARIO_NAMES = {"conservative": "Conservative", "balanced": "Balanced",
                  "cost_optimized": "Cost-Optimized"}


# --------------------------------------------------------------------------- #
# small numeric helpers
# --------------------------------------------------------------------------- #
def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def round_up_pow2(x):
    x = max(1, int(math.ceil(x)))
    return 1 << (x - 1).bit_length()


def _num(v, default=None):
    """Coerce to float; blank/None -> default."""
    if v is None:
        return default
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _boolish(v, default=False):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on", "y")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class SizingConfig:
    aidpu_usd: float = 0.001
    object_storage_rate: float = 0.0255
    archive_rate: float = 0.0026
    platform_min_aidpu: float = 230.0
    days_per_month: int = 30

    # topology
    users_per_team: int = 15
    concurrency_default_frac: float = 0.65
    users_per_shared_cluster: int = 60
    hybrid_teams_per_cluster: int = 6
    max_exec_per_cluster: int = 48

    # data-scan sizing (v1.1: realistic per-executor scan capacity; concurrency is the primary driver)
    tb_per_executor_scan: float = 2.0
    base_tb_per_executor: float = 2.0   # deprecated alias
    headroom_months: int = 6
    catalog_tb_per_source: float = 0.5

    # concurrency sizing
    default_query_runtime_sec: float = 45.0
    default_queries_per_user_hr: float = 20.0

    # memory floor
    hot_data_fraction: float = 0.10
    mem_overcommit: float = 3.0

    # executor / driver shape
    exec_mem_per_ocpu: int = 8
    exec_mem_min: int = 32
    exec_mem_max: int = 256
    driver_ocpu: int = 8
    driver_mem_gb: int = 128
    min_executors_floor: int = 2
    util_cap: float = 0.95
    min_business_hours: float = 8

    # DMC
    dmc_base_ocpu: int = 16
    dmc_tb_per_ocpu: float = 1.3
    dmc_mem_ratio: int = 16
    dmc_ocpu_min: int = 16
    dmc_ocpu_max: int = 96
    ha_dmc_count: int = 2

    # ingest / ETL
    etl_share_threshold: float = 0.25
    ingest_gb_per_exec: float = 60
    etl_min_exec_cap: int = 2
    etl_util: float = 0.55
    etl_hours_per_day: float = 8

    # GPU
    ml_users_per_gpu: int = 20
    max_gpus: int = 8
    gpu_host_ocpu: int = 16
    gpu_host_mem: int = 256
    ml_hours_per_day: float = 8

    # dev
    dev_max_exec: int = 4
    dev_hours_per_day: float = 8
    dev_util: float = 0.5

    # fixed services
    atp_monthly: float = 300.0
    networking_monthly: float = 300.0
    data_transfer_monthly: float = 100.0

    # envelope guardrails
    envelope_user_low: float = 120
    envelope_user_high: float = 320
    envelope_tb_low: float = 650
    envelope_tb_high: float = 850

    # calibration anchor
    anchor_monthly: float = 61999.60
    anchor_cost_per_user: float = 230.0

    sla_tiers: dict = field(default_factory=lambda: {
        "relaxed":          {"exec_per_concurrent_user": 1.5, "slots_per_query": 0.5, "target_util": 0.95, "util_target": 0.60, "exec_ocpu": 8,  "exec_mem_gb": 48},
        "standard":         {"exec_per_concurrent_user": 2.4, "slots_per_query": 1.0, "target_util": 0.92, "util_target": 0.50, "exec_ocpu": 8,  "exec_mem_gb": 64},
        "strict":           {"exec_per_concurrent_user": 3.5, "slots_per_query": 2.0, "target_util": 0.80, "util_target": 0.42, "exec_ocpu": 8,  "exec_mem_gb": 96},
        "mission_critical": {"exec_per_concurrent_user": 5.0, "slots_per_query": 4.0, "target_util": 0.65, "util_target": 0.35, "exec_ocpu": 16, "exec_mem_gb": 128},
    })

    scenarios: dict = field(default_factory=lambda: {
        "conservative":   {"cluster_proc": "AMD", "util_mult": 0.80, "min_frac": 1.00, "headroom_mult": 1.25, "tbpe_mult": 0.75, "hours_mult": 1.00, "dmc_ocpu_override": 48, "dmc_mem_override": 768, "force_ha": True,  "include_dev": True,  "ingest_trigger_gb": 300, "etl_batch_window": False, "storage_proj_mult": 1.5, "services_mult": 2.0},
        "balanced":       {"cluster_proc": "ARM", "util_mult": 1.00, "min_frac": 0.83, "headroom_mult": 1.00, "tbpe_mult": 1.00, "hours_mult": 1.00, "dmc_ocpu_override": 32, "dmc_mem_override": 512, "force_ha": None,  "include_dev": None,  "ingest_trigger_gb": 200, "etl_batch_window": False, "storage_proj_mult": 1.0, "services_mult": 1.0},
        "cost_optimized": {"cluster_proc": "ARM", "util_mult": 1.30, "min_frac": 0.50, "headroom_mult": 0.90, "tbpe_mult": 1.30, "hours_mult": 0.83, "dmc_ocpu_override": 24, "dmc_mem_override": 384, "force_ha": False, "include_dev": False, "ingest_trigger_gb": 600, "etl_batch_window": True,  "storage_proj_mult": 0.5, "services_mult": 1.0},
    })

    @classmethod
    def load(cls, path=None):
        """Load defaults, overriding with config.yaml if present/available."""
        cfg = cls()
        path = path or os.path.join(HERE, "config.yaml")
        if not os.path.exists(path):
            return cfg
        try:
            import yaml
        except ImportError:
            return cfg  # PyYAML not installed -> baked-in defaults
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        for k, v in data.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg


# --------------------------------------------------------------------------- #
# Inputs
# --------------------------------------------------------------------------- #
@dataclass
class SizingInput:
    customer_name: str = "Customer"
    total_source_data_tb: float = 82.0
    num_data_sources: int = 6
    per_source_volume_tb: str = ""          # comma list, optional
    num_users: int = 255
    num_concurrent_users: float | None = None
    sla_tier: str = "standard"
    monthly_growth_tb: float | None = None
    annual_growth_pct: float = 110.0
    daily_ingest_gb: float | None = None
    workload_mix: str = "90/10/0"           # interactive/etl/ml percentages
    avg_query_runtime_sec: float = 45.0
    queries_per_active_user_per_hr: float = 20.0
    business_hours_per_day: float = 12.0
    num_teams: int | None = None
    users_per_team: int = 15
    isolation_model: str = "per_team"       # per_team | shared_pool | hybrid
    processor_preference: str = "auto"      # auto | arm | amd | intel
    need_dev_env: bool = True
    need_ha: bool = False
    include_storage_cost: bool = True
    object_storage_hot_pct: float = 100.0
    data_retention_months: float = 12.0
    region: str = "us-ashburn-1"
    billing_model: str = "universal_credits"

    @classmethod
    def from_dict(cls, d: dict) -> "SizingInput":
        g = d.get
        return cls(
            customer_name=(g("customer_name") or "Customer").strip() or "Customer",
            total_source_data_tb=_num(g("total_source_data_tb"), 82.0),
            num_data_sources=int(_num(g("num_data_sources"), 6) or 6),
            per_source_volume_tb=(g("per_source_volume_tb") or "").strip(),
            num_users=int(_num(g("num_users"), 255) or 255),
            num_concurrent_users=_num(g("num_concurrent_users"), None),
            sla_tier=(g("sla_tier") or "standard").strip().lower(),
            monthly_growth_tb=_num(g("monthly_growth_tb"), None),
            annual_growth_pct=_num(g("annual_growth_pct"), 110.0),
            daily_ingest_gb=_num(g("daily_ingest_gb"), None),
            workload_mix=(g("workload_mix") or "90/10/0").strip(),
            avg_query_runtime_sec=_num(g("avg_query_runtime_sec"), 45.0),
            queries_per_active_user_per_hr=_num(g("queries_per_active_user_per_hr"), 20.0),
            business_hours_per_day=_num(g("business_hours_per_day"), 12.0),
            num_teams=(int(_num(g("num_teams"))) if _num(g("num_teams")) else None),
            users_per_team=int(_num(g("users_per_team"), 15) or 15),
            isolation_model=(g("isolation_model") or "per_team").strip().lower(),
            processor_preference=(g("processor_preference") or "auto").strip().lower(),
            need_dev_env=_boolish(g("need_dev_env"), True),
            need_ha=_boolish(g("need_ha"), False),
            include_storage_cost=_boolish(g("include_storage_cost"), True),
            object_storage_hot_pct=_num(g("object_storage_hot_pct"), 100.0),
            data_retention_months=_num(g("data_retention_months"), 12.0),
            region=(g("region") or "us-ashburn-1").strip(),
            billing_model=(g("billing_model") or "universal_credits").strip().lower(),
        )


def _normalize_mix(raw: str):
    """'90/10/0' -> {'interactive':0.9,'etl_batch':0.1,'ml_gpu':0.0} (sums to 1)."""
    parts = [p for p in str(raw).replace("%", "").replace(",", "/").split("/") if p.strip() != ""]
    vals = []
    for p in parts[:3]:
        try:
            vals.append(max(0.0, float(p)))
        except ValueError:
            vals.append(0.0)
    while len(vals) < 3:
        vals.append(0.0)
    total = sum(vals)
    if total <= 0:
        return {"interactive": 1.0, "etl_batch": 0.0, "ml_gpu": 0.0}
    return {"interactive": vals[0] / total, "etl_batch": vals[1] / total, "ml_gpu": vals[2] / total}


def _parse_per_source(raw: str, total_tb: float, n: int):
    vals = []
    for p in str(raw).split(","):
        x = _num(p, None)
        if x is not None:
            vals.append(x)
    if not vals:
        n = max(1, n)
        return [total_tb / n] * n
    return vals


# --------------------------------------------------------------------------- #
# Cluster spec builders (conform to the output data contract)
# --------------------------------------------------------------------------- #
def _spec_from_cluster(role, name, count, cl: AIDPCluster, always_on=False,
                       rationale="", gpu_per_executor=0):
    return {
        "role": role,
        "name": name,
        "count": count,
        "processor": ("NVIDIA_GPU" if gpu_per_executor else cl.processor),
        "driver_ocpu": cl.driver_ocpu,
        "driver_memory_gb": cl.driver_memory,
        "executor_ocpu": cl.executor_ocpu,
        "executor_memory_gb": cl.executor_memory,
        "gpu_per_executor": gpu_per_executor,
        "min_executors": cl.min_executors,
        "max_executors": cl.max_executors,
        "avg_utilization": round(cl.avg_utilization, 4),
        "avg_executors": round(cl.avg_executors, 2),
        "hours_per_day": cl.hours_per_day,
        "always_on": always_on,
        "node_driver_usd_hr": round(cl.driver_cost_per_hour, 4),
        "node_executor_usd_hr": round(cl.executor_cost_per_hour, 4),
        "cluster_usd_hr": round(cl.total_cost_per_hour, 4),
        "cluster_usd_month": round(cl.monthly_cost, 2),
        "role_usd_month": round(cl.monthly_cost * count, 2),
        "billed_floor_applied": False,
        "sizing_rationale": rationale,
        "peak_ocpu_each": cl.max_total_ocpu,
        "peak_memory_gb_each": cl.max_total_memory,
        "peak_gpu_each": cl.max_total_gpu,
    }


# --------------------------------------------------------------------------- #
# Per-scenario sizing
# --------------------------------------------------------------------------- #
def _size_scenario(inp: SizingInput, cfg: SizingConfig, scen_key: str, d: dict) -> dict:
    scen = cfg.scenarios[scen_key]
    sla = cfg.sla_tiers.get(inp.sla_tier, cfg.sla_tiers["standard"])
    clusters = []
    warnings = []

    total_tb = d["total_source_data_tb"]
    teams = d["teams"]
    concurrent = d["concurrent"]
    tb_per_team = d["tb_per_team"]
    num_prod = d["num_prod_clusters"]
    mix = d["mix"]

    proc = _resolve_processor(inp.processor_preference, scen)

    # ---- 2.2 production cluster size: hybrid = max(concurrency, data-scan, memory) ----
    exec_ocpu = int(sla["exec_ocpu"])
    exec_mem = int(sla["exec_mem_gb"])
    data_per_cluster = total_tb / num_prod if num_prod else total_tb

    # (a) CONCURRENCY (primary): calibrated executors-per-concurrent-user, floored by Little's Law
    #     throughput. Entel: 10 concurrent/cluster x 2.4 = 24 executors (the anchor).
    conc_per_cluster = concurrent / num_prod if num_prod else concurrent
    exec_conc_calibrated = math.ceil(conc_per_cluster * sla.get("exec_per_concurrent_user", 2.4))
    arrival_qps = concurrent * inp.queries_per_active_user_per_hr / 3600.0
    inflight_total = math.ceil(arrival_qps * inp.avg_query_runtime_sec)
    inflight_pc = math.ceil(inflight_total / num_prod) if num_prod else inflight_total
    exec_littles = math.ceil(inflight_pc * sla["slots_per_query"] / sla["target_util"])
    exec_from_conc = max(exec_conc_calibrated, exec_littles)

    # (b) DATA-SCAN parallelism (realistic TB/executor of active scan, scenario-adjusted)
    tbpe = cfg.tb_per_executor_scan * scen["tbpe_mult"]
    exec_from_data = math.ceil(data_per_cluster / tbpe) if tbpe > 0 else cfg.min_executors_floor

    # (c) MEMORY working-set floor: per-cluster hot data must fit executor RAM
    cluster_hot_tb = (total_tb * cfg.hot_data_fraction) / num_prod if num_prod else 0
    req_mem_gb = cluster_hot_tb * 1024 / cfg.mem_overcommit
    exec_from_mem = math.ceil(req_mem_gb / exec_mem) if exec_mem else cfg.min_executors_floor

    raw_max = max(exec_from_conc, exec_from_data, exec_from_mem, cfg.min_executors_floor)
    max_exec = math.ceil(raw_max * scen["headroom_mult"])
    max_exec = int(clamp(max_exec, 2, cfg.max_exec_per_cluster))
    min_exec = max(cfg.min_executors_floor, round(max_exec * scen["min_frac"]))
    effective_ha = inp.need_ha if scen["force_ha"] is None else scen["force_ha"]
    if effective_ha:
        min_exec = max_exec

    avg_util = min(cfg.util_cap, sla["util_target"] * scen["util_mult"])
    hours = clamp(inp.business_hours_per_day * scen["hours_mult"], cfg.min_business_hours, 24)

    bind_reason = _argmax_label(
        [("data-bound", exec_from_data), ("concurrency-bound", exec_from_conc),
         ("memory-floored", exec_from_mem)])
    rationale = (f"{bind_reason}: max_exec={max_exec} "
                 f"(data={exec_from_data}, concurrency={exec_from_conc}, memory={exec_from_mem}; "
                 f"headroom x{scen['headroom_mult']})")

    prod = AIDPCluster(cfg.driver_ocpu, cfg.driver_mem_gb, exec_ocpu, exec_mem,
                       min_exec, max_exec, proc, hours, avg_util)
    clusters.append(_spec_from_cluster(
        "production", "Production Cluster", num_prod, prod, rationale=rationale))

    # ---- 2.3 Default Master Catalog ----
    catalog_scale = d["catalog_scale"]
    dmc_ocpu = clamp(round_up_pow2(cfg.dmc_base_ocpu + catalog_scale / cfg.dmc_tb_per_ocpu),
                     cfg.dmc_ocpu_min, cfg.dmc_ocpu_max)
    dmc_ocpu = int(scen["dmc_ocpu_override"] or dmc_ocpu)
    dmc_mem = int(scen["dmc_mem_override"] or dmc_ocpu * cfg.dmc_mem_ratio)
    dmc_node_hr = AIDPPricing.node_usd_per_hour(dmc_ocpu, dmc_mem, "AMD")
    floor_hr = cfg.platform_min_aidpu * cfg.aidpu_usd
    dmc_billed_hr = max(floor_hr, dmc_node_hr)
    dmc_floor_applied = dmc_node_hr < floor_hr
    if dmc_floor_applied:
        warnings.append(f"DMC below platform minimum -> billed at {cfg.platform_min_aidpu:.0f} AIDPU/hr")
    dmc_count = cfg.ha_dmc_count if effective_ha else 1
    dmc_month = dmc_billed_hr * 24 * cfg.days_per_month
    clusters.insert(0, {
        "role": "default_master_catalog", "name": "Default Master Catalog",
        "count": dmc_count, "processor": "AMD",
        "driver_ocpu": dmc_ocpu, "driver_memory_gb": dmc_mem,
        "executor_ocpu": 0, "executor_memory_gb": 0, "gpu_per_executor": 0,
        "min_executors": 0, "max_executors": 0, "avg_utilization": 1.0, "avg_executors": 0,
        "hours_per_day": 24, "always_on": True,
        "node_driver_usd_hr": round(dmc_billed_hr, 4), "node_executor_usd_hr": 0.0,
        "cluster_usd_hr": round(dmc_billed_hr, 4),
        "cluster_usd_month": round(dmc_month, 2),
        "role_usd_month": round(dmc_month * dmc_count, 2),
        "billed_floor_applied": dmc_floor_applied,
        "sizing_rationale": f"catalog scale {catalog_scale:.0f} TB-equiv; fixed AMD 24/7"
                            + (" (HA x2)" if dmc_count > 1 else ""),
        "peak_ocpu_each": dmc_ocpu, "peak_memory_gb_each": dmc_mem, "peak_gpu_each": 0,
    })

    # ---- 2.4 optional ingest/ETL cluster ----
    if d["daily_ingest"] >= scen["ingest_trigger_gb"] or mix["etl_batch"] >= cfg.etl_share_threshold:
        etl_max = max(cfg.etl_min_exec_cap, math.ceil(d["daily_ingest"] / cfg.ingest_gb_per_exec))
        etl_max = int(clamp(etl_max, 2, cfg.max_exec_per_cluster))
        etl_min = max(2, round(etl_max * 0.5))
        etl_hours = cfg.etl_hours_per_day if scen["etl_batch_window"] else inp.business_hours_per_day
        etl_hours = clamp(etl_hours, cfg.min_business_hours, 24)
        etl = AIDPCluster(cfg.driver_ocpu, cfg.driver_mem_gb, 8, 64, etl_min, etl_max,
                          proc, etl_hours, cfg.etl_util)
        clusters.append(_spec_from_cluster(
            "ingest_etl", "Ingest / ETL Cluster", 1, etl,
            rationale=f"{d['daily_ingest']:.0f} GB/day ingest; {etl_max} exec @ {cfg.ingest_gb_per_exec:.0f} GB/exec"))

    # ---- 2.6 optional GPU/ML cluster ----
    total_gpu = 0
    if mix["ml_gpu"] > 0:
        gpu_count = int(clamp(math.ceil(inp.num_users * mix["ml_gpu"] / cfg.ml_users_per_gpu),
                              1, cfg.max_gpus))
        total_gpu = gpu_count
        gpu_hr = AIDPPricing.node_usd_per_hour(cfg.gpu_host_ocpu, cfg.gpu_host_mem, "Intel", gpu=gpu_count)
        gpu_month = gpu_hr * cfg.ml_hours_per_day * cfg.days_per_month
        clusters.append({
            "role": "gpu", "name": "GPU / ML Cluster", "count": 1, "processor": "NVIDIA_GPU",
            "driver_ocpu": cfg.gpu_host_ocpu, "driver_memory_gb": cfg.gpu_host_mem,
            "executor_ocpu": 0, "executor_memory_gb": 0, "gpu_per_executor": gpu_count,
            "min_executors": 0, "max_executors": 0, "avg_utilization": 1.0, "avg_executors": 0,
            "hours_per_day": cfg.ml_hours_per_day, "always_on": False,
            "node_driver_usd_hr": round(gpu_hr, 4), "node_executor_usd_hr": 0.0,
            "cluster_usd_hr": round(gpu_hr, 4),
            "cluster_usd_month": round(gpu_month, 2), "role_usd_month": round(gpu_month, 2),
            "billed_floor_applied": False,
            "sizing_rationale": f"{gpu_count} GPU @ 4110 AIDPU/GPU/hr — dominates cost",
            "peak_ocpu_each": cfg.gpu_host_ocpu, "peak_memory_gb_each": cfg.gpu_host_mem,
            "peak_gpu_each": gpu_count,
        })

    # ---- 2.7 optional dev/test cluster ----
    include_dev = inp.need_dev_env if scen["include_dev"] is None else scen["include_dev"]
    if include_dev:
        dev = AIDPCluster(4, 32, 4, 32, 1, cfg.dev_max_exec, "ARM",
                          cfg.dev_hours_per_day, cfg.dev_util)
        clusters.append(_spec_from_cluster(
            "dev", "Dev / Test Cluster", 1, dev, rationale="shared non-prod, ARM, short hours"))

    # ---- 2.8 storage + additional services ----
    services = []
    storage_month = 0.0
    if inp.include_storage_cost:
        avg_stored_tb = total_tb + d["monthly_growth"] * (inp.data_retention_months / 2.0) * scen["storage_proj_mult"]
        hot_gb = avg_stored_tb * 1024 * (inp.object_storage_hot_pct / 100.0)
        arch_gb = avg_stored_tb * 1024 * (1 - inp.object_storage_hot_pct / 100.0)
        storage_month = hot_gb * cfg.object_storage_rate + arch_gb * cfg.archive_rate
        services.append({"name": "Object Storage",
                         "detail": f"{avg_stored_tb:,.0f} TB avg-over-retention "
                                   f"({inp.object_storage_hot_pct:.0f}% hot)",
                         "usd_month": round(storage_month, 2)})

    atp = cfg.atp_monthly * scen["services_mult"]
    net = cfg.networking_monthly * scen["services_mult"]
    xfer = cfg.data_transfer_monthly * scen["services_mult"]
    services.append({"name": "Autonomous Database", "detail": "Data anonymizer / metadata",
                     "usd_month": round(atp, 2)})
    services.append({"name": "Networking", "detail": "VPN / FastConnect", "usd_month": round(net, 2)})
    services.append({"name": "Data Transfer", "detail": f"Egress ({inp.region})", "usd_month": round(xfer, 2)})
    services_month = atp + net + xfer

    # ---- 2.9 totals + envelope guards ----
    compute_month = sum(c["role_usd_month"] for c in clusters)
    total_month = compute_month + storage_month + services_month
    cost_per_user = total_month / inp.num_users if inp.num_users else 0.0
    cost_per_tb = compute_month / total_tb if total_tb else 0.0

    peak_ocpu = sum(c["peak_ocpu_each"] * c["count"] for c in clusters)
    peak_mem = sum(c["peak_memory_gb_each"] * c["count"] for c in clusters)

    flags = []
    if cost_per_user < cfg.envelope_user_low:
        flags.append(f"UNDER_SIZED: ${cost_per_user:.0f}/user < ${cfg.envelope_user_low:.0f}")
    if cost_per_user > cfg.envelope_user_high:
        flags.append(f"OVER_PROVISIONED: ${cost_per_user:.0f}/user > ${cfg.envelope_user_high:.0f}")
    if not flags:
        flags = ["OK"]

    # v20-comparable core (DMC + production + fixed services; excludes storage/ingest/dev/gpu)
    core_month = (sum(c["role_usd_month"] for c in clusters
                      if c["role"] in ("default_master_catalog", "production"))
                  + services_month)

    return {
        "scenario": scen_key,
        "scenario_name": SCENARIO_NAMES[scen_key],
        "is_recommended": scen_key == "balanced",
        "num_prod_clusters": num_prod,
        "num_teams": teams,
        "concurrent_users_used": int(concurrent),
        "inflight_queries_total": inflight_total,
        "sla_tier": inp.sla_tier,
        "knobs": scen,
        "clusters": clusters,
        "services": services,
        "compute_usd_month": round(compute_month, 2),
        "storage_usd_month": round(storage_month, 2),
        "services_usd_month": round(services_month, 2),
        "total_usd_month": round(total_month, 2),
        "total_usd_year": round(total_month * 12, 2),
        "core_usd_month": round(core_month, 2),
        "cost_per_user_month": round(cost_per_user, 2),
        "cost_per_tb_month": round(cost_per_tb, 2),
        "total_ocpu": int(peak_ocpu),
        "total_gpu": int(total_gpu),
        "total_memory_gb": int(peak_mem),
        "processor": proc,
        "avg_utilization": round(avg_util, 3),
        "hours_per_day": hours,
        "envelope_flags": flags,
        "warnings": warnings,
    }


def _resolve_processor(pref, scen):
    pref = (pref or "auto").lower()
    if pref in ("arm", "amd", "intel"):
        return pref.upper() if pref != "intel" else "Intel"
    return scen["cluster_proc"]


def _argmax_label(pairs):
    return max(pairs, key=lambda p: p[1])[0]


# --------------------------------------------------------------------------- #
# Top-level entry point
# --------------------------------------------------------------------------- #
def size(inp: SizingInput, cfg: SizingConfig | None = None) -> dict:
    cfg = cfg or SizingConfig.load()

    # ---- 2.0 normalization / derived intermediates ----
    concurrent = inp.num_concurrent_users or round(inp.num_users * cfg.concurrency_default_frac)
    teams = inp.num_teams or max(1, round(inp.num_users / max(1, inp.users_per_team)))
    monthly_growth = (inp.monthly_growth_tb
                      if inp.monthly_growth_tb is not None
                      else inp.total_source_data_tb * inp.annual_growth_pct / 100.0 / 12.0)
    daily_ingest = (inp.daily_ingest_gb
                    if inp.daily_ingest_gb is not None
                    else monthly_growth * 1024 / 30.0)
    per_source = _parse_per_source(inp.per_source_volume_tb, inp.total_source_data_tb, inp.num_data_sources)
    mix = _normalize_mix(inp.workload_mix)

    input_warnings = []
    if str(inp.per_source_volume_tb).strip():
        ps_sum = sum(per_source)
        if abs(ps_sum - inp.total_source_data_tb) > max(1.0, 0.05 * inp.total_source_data_tb):
            input_warnings.append(
                f"Per-source volumes sum to {ps_sum:,.1f} TB but total source data is "
                f"{inp.total_source_data_tb:,.1f} TB. Sizing uses the total; per-source is informational only.")

    tb_per_team = inp.total_source_data_tb / max(1, teams)
    catalog_scale = inp.total_source_data_tb + inp.num_data_sources * cfg.catalog_tb_per_source

    # ---- 2.1 cluster count: topology first, then a realistic data guardrail ----
    if inp.isolation_model == "shared_pool":
        base_count = max(1, math.ceil(concurrent / cfg.users_per_shared_cluster))
    elif inp.isolation_model == "hybrid":
        base_count = max(1, math.ceil(teams / cfg.hybrid_teams_per_cluster))
    else:  # per_team
        base_count = teams
    # Add clusters only if the fleet genuinely needs more executors than one cluster can hold,
    # by scan parallelism OR hot-memory working set (realistic capacities — not stored-volume/0.2).
    sla = cfg.sla_tiers.get(inp.sla_tier, cfg.sla_tiers["standard"])
    scan_exec_total = math.ceil(inp.total_source_data_tb / cfg.tb_per_executor_scan)
    hot_mem_total_gb = inp.total_source_data_tb * cfg.hot_data_fraction * 1024 / cfg.mem_overcommit
    mem_exec_total = math.ceil(hot_mem_total_gb / sla["exec_mem_gb"])
    fleet_exec_demand = max(scan_exec_total, mem_exec_total)
    data_floor = math.ceil(fleet_exec_demand / cfg.max_exec_per_cluster)
    num_prod_clusters = max(base_count, data_floor, 1)

    derived = {
        "total_source_data_tb": inp.total_source_data_tb,
        "concurrent": concurrent,
        "teams": teams,
        "monthly_growth": round(monthly_growth, 3),
        "daily_ingest": round(daily_ingest, 1),
        "per_source": [round(x, 2) for x in per_source],
        "max_source_tb": round(max(per_source), 2),
        "mix": {k: round(v, 3) for k, v in mix.items()},
        "tb_per_team": round(tb_per_team, 3),
        "conc_per_team": round(concurrent / max(1, teams), 2),
        "fwd_data_tb": round(inp.total_source_data_tb + monthly_growth * cfg.headroom_months, 1),
        "catalog_scale": round(catalog_scale, 1),
        "num_prod_clusters": num_prod_clusters,
        "input_warnings": input_warnings,
    }

    scenarios = {k: _size_scenario(inp, cfg, k, derived) for k in SCENARIO_KEYS}

    bal = scenarios["balanced"]
    delta_pct = ((bal["core_usd_month"] - cfg.anchor_monthly) / cfg.anchor_monthly * 100.0
                 if cfg.anchor_monthly else 0.0)
    calibration = {
        "anchor_monthly": cfg.anchor_monthly,
        "anchor_cost_per_user": cfg.anchor_cost_per_user,
        "balanced_core_monthly": bal["core_usd_month"],
        "balanced_total_monthly": bal["total_usd_month"],
        "delta_pct": round(delta_pct, 2),
        "note": ("Balanced core (DMC + production + fixed services, storage/ingest/dev "
                 "excluded) vs the Entel v20 anchor. Storage/ingest/dev are added on top "
                 "when their inputs are enabled."),
    }

    return {
        "customer_name": inp.customer_name,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "region": inp.region,
        "billing_model": inp.billing_model,
        "inputs_echo": asdict(inp),
        "derived": derived,
        "scenarios": scenarios,
        "recommended_scenario": "balanced",
        "calibration": calibration,
        "pricing_rates": {
            "ARM": AIDPPricing.ARM_OCPU, "AMD": AIDPPricing.AMD_OCPU,
            "Intel": AIDPPricing.INTEL_OCPU, "GPU": AIDPPricing.NVIDIA_GPU,
            "memory": AIDPPricing.MEMORY_GB, "aidpu_usd": AIDPPricing.AIDPU_TO_USD,
            "object_storage": cfg.object_storage_rate, "platform_min_aidpu": cfg.platform_min_aidpu,
        },
        "config_version": CONFIG_VERSION,
    }


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _cli(argv):
    cfg = SizingConfig.load()
    if len(argv) > 1:
        with open(argv[1], "r", encoding="utf-8") as fh:
            inp = SizingInput.from_dict(json.load(fh))
        label = argv[1]
    else:
        inp = SizingInput()  # Entel calibration defaults
        label = "Entel calibration defaults"

    result = size(inp, cfg)
    cal = result["calibration"]
    print(f"\nSizeWise — {result['customer_name']}  ({label})")
    print("=" * 72)
    for k in SCENARIO_KEYS:
        s = result["scenarios"][k]
        star = "  <-- recommended" if s["is_recommended"] else ""
        print(f"  {s['scenario_name']:<15} total ${s['total_usd_month']:>12,.2f}/mo "
              f"| ${s['cost_per_user_month']:>7,.2f}/user | {s['num_prod_clusters']:>3} prod clusters "
              f"| {','.join(s['envelope_flags'])}{star}")
    print("-" * 72)
    print(f"  Calibration: Balanced core ${cal['balanced_core_monthly']:,.2f}/mo vs "
          f"anchor ${cal['anchor_monthly']:,.2f}  => {cal['delta_pct']:+.2f}%")
    within = abs(cal["delta_pct"]) <= 15
    print(f"  Calibration self-test (|delta| <= 15%): {'PASS' if within else 'FAIL'}")
    print()
    return 0 if within else 1


if __name__ == "__main__":
    sys.exit(_cli(sys.argv))
