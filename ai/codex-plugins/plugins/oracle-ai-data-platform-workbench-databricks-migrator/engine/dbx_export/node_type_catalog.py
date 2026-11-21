"""Databricks node_type_id → AIDP shape mapping.

Preserves architecture family (ARM stays ARM, Intel stays Intel, etc.) instead
of collapsing every DBX type onto amd.generic. Preserves vCPU and memory exactly
when the DBX type is in the catalog; falls back to nearest-higher on unknown
types via family-suffix parsing.

OCPU formula (per OCI billing model — matches the pilot deployment's compute-usage report):
  - ARM   shapes: OCPU = vCPU              (1:1)
  - AMD   shapes: OCPU = ceil(vCPU / 2)    (Databricks vCPU is a hyperthread; OCI OCPU is a physical core)
  - Intel shapes: OCPU = ceil(vCPU / 2)    (same as AMD)
  - GPU   shapes: OCPU = ceil(vCPU / 2)    (per-GPU billing dominates)

AIDP shape families (empirically verified against the ap-mumbai-1 workspace's
several hundred clusters; fleet probe):
  - "amd.generic"  — 510 uses
  - "arm.generic"  — 96 uses
  - "gpu.a10"      — 20 uses  (NVIDIA A10; only GPU family GA today)
  - "intel.generic" — ASSUMED. No Intel clusters in the workspace to verify;
                      matches naming pattern. Runtime fallback to amd.generic
                      if AIDP rejects at create time (see cluster_converter +
                      dbx_export/__main__ create path).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ─── AIDP shape names ────────────────────────────────────────────────

AIDP_SHAPE_AMD   = "amd.generic"
AIDP_SHAPE_ARM   = "arm.generic"
AIDP_SHAPE_INTEL = "intel.generic"   # unverified; runtime fallback to AMD on rejection
AIDP_SHAPE_GPU   = "gpu.a10"         # only GPU family GA on AIDP today

ARCH_TO_AIDP_SHAPE: Dict[str, str] = {
    "ARM":   AIDP_SHAPE_ARM,
    "AMD":   AIDP_SHAPE_AMD,
    "Intel": AIDP_SHAPE_INTEL,
    "GPU":   AIDP_SHAPE_GPU,
}


# ─── DBX node type catalog ───────────────────────────────────────────
#
# Source: the pilot deployment compute usage report (a large sample of one-minute snapshots) — every DBX AWS instance type observed on the pilot deployment's
# Databricks fleet plus a few not-yet-observed variants for completeness.
#
# Each entry:  {"arch": "ARM|AMD|Intel|GPU", "vcpu": int, "mem_gb": int,
#               ["gpus": int, "gpu_model": str], ["ambiguous": True]}
#
# - "arch"       is the family for AIDP shape mapping.
# - "vcpu"       is the DBX-reported vCPU count (hyperthread count on x86;
#                physical-core count on ARM Graviton).
# - "mem_gb"     is the DBX-reported memory in GB.
# - "gpus"       (GPU only) — physical GPU count on the instance.
# - "gpu_model"  (GPU only) — NVIDIA GPU generation (T4 / A10G / L4).
# - "ambiguous"  (fleet-pool only) — set True on Databricks fleet pools
#                (r-fleet / rd-fleet / m-fleet) where Intel-vs-AMD is NOT
#                identifiable from the node_type_id alone. Per user guidance
#                these map to AMD as a fallback, with a WARN in the log.

DBX_NODE_CATALOG: Dict[str, Dict[str, Any]] = {

    # ═══ ARM (Graviton) — OCPU = vCPU ═══════════════════════════════
    # r*g* / m*g* / c*g* families + Databricks *g-fleet pools
    "rg-fleet.xlarge":  {"arch": "ARM", "vcpu": 4,  "mem_gb": 32},
    "rg-fleet.2xlarge": {"arch": "ARM", "vcpu": 8,  "mem_gb": 64},
    "rg-fleet.4xlarge": {"arch": "ARM", "vcpu": 16, "mem_gb": 128},
    "rg-fleet.8xlarge": {"arch": "ARM", "vcpu": 32, "mem_gb": 256},
    "rg-fleet.12xlarge":{"arch": "ARM", "vcpu": 48, "mem_gb": 384},
    "rg-fleet.16xlarge":{"arch": "ARM", "vcpu": 64, "mem_gb": 512},
    "mg-fleet.xlarge":  {"arch": "ARM", "vcpu": 4,  "mem_gb": 16},
    "mg-fleet.2xlarge": {"arch": "ARM", "vcpu": 8,  "mem_gb": 32},
    "mg-fleet.4xlarge": {"arch": "ARM", "vcpu": 16, "mem_gb": 64},
    "mg-fleet.8xlarge": {"arch": "ARM", "vcpu": 32, "mem_gb": 128},
    "rgd-fleet.xlarge": {"arch": "ARM", "vcpu": 4,  "mem_gb": 32},
    "cg-fleet.xlarge":  {"arch": "ARM", "vcpu": 4,  "mem_gb": 8},
    "r6g.large":        {"arch": "ARM", "vcpu": 2,  "mem_gb": 16},
    "r6g.xlarge":       {"arch": "ARM", "vcpu": 4,  "mem_gb": 32},
    "r6g.2xlarge":      {"arch": "ARM", "vcpu": 8,  "mem_gb": 64},
    "r6g.4xlarge":      {"arch": "ARM", "vcpu": 16, "mem_gb": 128},
    "r6g.8xlarge":      {"arch": "ARM", "vcpu": 32, "mem_gb": 256},
    "r6gd.xlarge":      {"arch": "ARM", "vcpu": 4,  "mem_gb": 32},
    "r6gd.4xlarge":     {"arch": "ARM", "vcpu": 16, "mem_gb": 128},
    "r7gd.4xlarge":     {"arch": "ARM", "vcpu": 16, "mem_gb": 128},
    "r8g.xlarge":       {"arch": "ARM", "vcpu": 4,  "mem_gb": 32},
    "r8g.4xlarge":      {"arch": "ARM", "vcpu": 16, "mem_gb": 128},

    # ═══ Intel — OCPU = ceil(vCPU/2) ═════════════════════════════════
    "i3.xlarge":        {"arch": "Intel", "vcpu": 4,  "mem_gb": 30.5},
    "i3.2xlarge":       {"arch": "Intel", "vcpu": 8,  "mem_gb": 61},
    "i3.4xlarge":       {"arch": "Intel", "vcpu": 16, "mem_gb": 122},
    "i3.8xlarge":       {"arch": "Intel", "vcpu": 32, "mem_gb": 244},
    "i3en.xlarge":      {"arch": "Intel", "vcpu": 4,  "mem_gb": 32},
    "i3en.2xlarge":     {"arch": "Intel", "vcpu": 8,  "mem_gb": 64},
    "m5.large":         {"arch": "Intel", "vcpu": 2,  "mem_gb": 8},
    "m5.xlarge":        {"arch": "Intel", "vcpu": 4,  "mem_gb": 16},
    "m5.2xlarge":       {"arch": "Intel", "vcpu": 8,  "mem_gb": 32},
    "m5.4xlarge":       {"arch": "Intel", "vcpu": 16, "mem_gb": 64},
    "m5.8xlarge":       {"arch": "Intel", "vcpu": 32, "mem_gb": 128},
    "m5d.xlarge":       {"arch": "Intel", "vcpu": 4,  "mem_gb": 16},
    "m5d.2xlarge":      {"arch": "Intel", "vcpu": 8,  "mem_gb": 32},
    "m5d.4xlarge":      {"arch": "Intel", "vcpu": 16, "mem_gb": 64},
    "c5.xlarge":        {"arch": "Intel", "vcpu": 4,  "mem_gb": 8},
    "c5.2xlarge":       {"arch": "Intel", "vcpu": 8,  "mem_gb": 16},
    "c5.4xlarge":       {"arch": "Intel", "vcpu": 16, "mem_gb": 32},
    "r5.large":         {"arch": "Intel", "vcpu": 2,  "mem_gb": 16},
    "r5.4xlarge":       {"arch": "Intel", "vcpu": 16, "mem_gb": 128},
    "r5d.xlarge":       {"arch": "Intel", "vcpu": 4,  "mem_gb": 32},
    "r5d.2xlarge":      {"arch": "Intel", "vcpu": 8,  "mem_gb": 64},
    "r5.xlarge":        {"arch": "Intel", "vcpu": 4,  "mem_gb": 32},
    "r5.2xlarge":       {"arch": "Intel", "vcpu": 8,  "mem_gb": 64},
    "r5.8xlarge":       {"arch": "Intel", "vcpu": 32, "mem_gb": 256},
    "r5d.4xlarge":      {"arch": "Intel", "vcpu": 16, "mem_gb": 128},
    "r6i.4xlarge":      {"arch": "Intel", "vcpu": 16, "mem_gb": 128},
    "r6i.8xlarge":      {"arch": "Intel", "vcpu": 32, "mem_gb": 256},
    "r7i.xlarge":       {"arch": "Intel", "vcpu": 4,  "mem_gb": 32},
    "c5n.xlarge":       {"arch": "Intel", "vcpu": 4,  "mem_gb": 10},
    "c6i.2xlarge":      {"arch": "Intel", "vcpu": 8,  "mem_gb": 16},
    # c6i.24xlarge IS Intel silicon (arch records the DBX-side fact), but
    # its 48 OCPUs exceed AIDP intel.generic's 32-OCPU ceiling — the
    # override targets amd.generic (x86 too, 64 OCPU max) so the capacity
    # fits, per the same "when unsure, assume AMD" rule as the runtime
    # intel→amd fallback. Snaps to 64 OCPU / 256 GB.
    "c6i.24xlarge":     {"arch": "Intel", "vcpu": 96, "mem_gb": 192,
                         "aidp_shape_override": "amd.generic"},

    # ═══ GPU (NVIDIA) — mapped to gpu.a10 with per-node gpu count ════
    "g4dn.xlarge":      {"arch": "GPU", "vcpu": 4,   "mem_gb": 16,  "gpus": 1, "gpu_model": "T4"},
    "g4dn.2xlarge":     {"arch": "GPU", "vcpu": 8,   "mem_gb": 32,  "gpus": 1, "gpu_model": "T4"},
    "g4dn.4xlarge":     {"arch": "GPU", "vcpu": 16,  "mem_gb": 64,  "gpus": 1, "gpu_model": "T4"},
    "g4dn.8xlarge":     {"arch": "GPU", "vcpu": 32,  "mem_gb": 128, "gpus": 1, "gpu_model": "T4"},
    "g4dn.12xlarge":    {"arch": "GPU", "vcpu": 48,  "mem_gb": 192, "gpus": 4, "gpu_model": "T4"},
    "g4dn.16xlarge":    {"arch": "GPU", "vcpu": 64,  "mem_gb": 256, "gpus": 1, "gpu_model": "T4"},
    "g5.2xlarge":       {"arch": "GPU", "vcpu": 8,   "mem_gb": 32,  "gpus": 1, "gpu_model": "A10G"},
    "g5.4xlarge":       {"arch": "GPU", "vcpu": 16,  "mem_gb": 64,  "gpus": 1, "gpu_model": "A10G"},
    "g5.8xlarge":       {"arch": "GPU", "vcpu": 32,  "mem_gb": 128, "gpus": 1, "gpu_model": "A10G"},
    "g5.16xlarge":      {"arch": "GPU", "vcpu": 64,  "mem_gb": 256, "gpus": 1, "gpu_model": "A10G"},
    "g5.24xlarge":      {"arch": "GPU", "vcpu": 96,  "mem_gb": 384, "gpus": 4, "gpu_model": "A10G"},
    "g6.2xlarge":       {"arch": "GPU", "vcpu": 8,   "mem_gb": 32,  "gpus": 1, "gpu_model": "L4"},
    "g6.8xlarge":       {"arch": "GPU", "vcpu": 32,  "mem_gb": 128, "gpus": 1, "gpu_model": "L4"},
    "g6.16xlarge":      {"arch": "GPU", "vcpu": 64,  "mem_gb": 256, "gpus": 1, "gpu_model": "L4"},

    # ═══ Fleet pools (x86, Intel-vs-AMD not identifiable) ════════════
    # Per user guidance: default to AMD when we cannot identify the family.
    # Every entry here MUST carry ambiguous=True so the log flags them WARN.
    "r-fleet.xlarge":   {"arch": "AMD", "vcpu": 4,  "mem_gb": 32,  "ambiguous": True},
    "r-fleet.2xlarge":  {"arch": "AMD", "vcpu": 8,  "mem_gb": 64,  "ambiguous": True},
    "r-fleet.4xlarge":  {"arch": "AMD", "vcpu": 16, "mem_gb": 128, "ambiguous": True},
    "r-fleet.8xlarge":  {"arch": "AMD", "vcpu": 32, "mem_gb": 256, "ambiguous": True},
    "rd-fleet.xlarge":  {"arch": "AMD", "vcpu": 4,  "mem_gb": 32,  "ambiguous": True},
    "rd-fleet.2xlarge": {"arch": "AMD", "vcpu": 8,  "mem_gb": 64,  "ambiguous": True},
    "rd-fleet.8xlarge": {"arch": "AMD", "vcpu": 32, "mem_gb": 256, "ambiguous": True},
    "m-fleet.xlarge":   {"arch": "AMD", "vcpu": 4,  "mem_gb": 16,  "ambiguous": True},
}


# ─── Family-suffix fallback for unknown DBX types ────────────────────
#
# When a DBX node_type_id is not in the catalog above, guess its arch from
# the family prefix. Rules match AWS instance-type naming conventions.

# Order matters — check longer/more-specific patterns first. Each entry is
# (regex on the full node_type_id, arch, ambiguous?).
#
# Design: AWS instance-type naming follows <family><generation>[<suffix>]<size>
# where the suffix letters encode architecture / storage:
#   - "g"  → Graviton (ARM)   e.g. r6g, r7g, r8g, c7g, m7g
#   - "gd" → Graviton + local NVMe (ARM)  e.g. r6gd, r7gd
#   - "i"  → Intel Ice Lake / Sapphire Rapids   e.g. r6i, r7i, c6i, c7i
#   - "in", "iz" → Intel variants (network, high-freq)
#   - "a"  → AMD EPYC          e.g. r5a, r6a, m5a, c5a
#   - "ad" → AMD + local NVMe  e.g. r5ad
#   - r5, r5d, r5n, c5, c5d, c5n → Intel (5th gen predates AMD/ARM variants)
_FAMILY_ARCH_RULES: List[Tuple[re.Pattern, str, bool]] = [
    # GPU: g4dn, g5, g6, ...
    (re.compile(r"^g\d", re.I),                    "GPU",   False),
    # ARM fleet pools: rg-fleet, mg-fleet, cg-fleet, rgd-fleet
    (re.compile(r"^[a-z]+gd?-fleet\.", re.I),      "ARM",   False),
    # ARM Graviton by generation: r6g, r7g, r8g, r6gd, r7gd, c7g, m6g, m7g, m8g
    (re.compile(r"^[rmc]\d+gd?\.", re.I),          "ARM",   False),
    # Intel by suffix "i": r6i, r6in, r7i, r7iz, c6i, c6in, c7i, c7iz
    (re.compile(r"^[rmc]\d+i[a-z]*\.", re.I),      "Intel", False),
    # Intel 5th-gen (no arch suffix): r5, r5d, r5n, r5dn, c5, c5d, c5n
    (re.compile(r"^[rc]5[dn]{0,2}\.", re.I),       "Intel", False),
    # AMD by suffix "a": r5a, r6a, m5a, m6a, c5a, c6a (with optional d)
    (re.compile(r"^[rmc]\d+ad?\.", re.I),          "AMD",   False),
    # x86 fleet pools (Intel vs AMD not identifiable): r-fleet, rd-fleet,
    # m-fleet, md-fleet, c-fleet, cd-fleet
    (re.compile(r"^[rmc]d?-fleet\.", re.I),        "AMD",   True),
]


# ─── Public API ──────────────────────────────────────────────────────

@dataclass
class ShapeResolution:
    """Result of mapping one DBX node_type_id to an AIDP shape config."""
    dbx_node_type: str
    dbx_arch: str
    dbx_vcpu: int
    dbx_mem_gb: int
    aidp_shape: str
    ocpus: int
    memory_gb: int
    gpus: int = 0
    gpu_model: Optional[str] = None
    # 'catalog' | 'family-suffix' | 'unknown-default'
    resolution: str = "catalog"
    ambiguous: bool = False    # True for x86-fleet pools mapped to AMD
    note: str = ""             # human-readable summary for the log


def _ocpu_from_vcpu(vcpu: int, arch: str) -> int:
    """DBX vCPU → AIDP OCPU, per family formula.
    ARM: 1:1. Everything else: ceil(vcpu/2). Minimum 1 to satisfy AIDP's
    per-shape lower bound (AIDP rejects ocpus<1)."""
    if vcpu <= 0:
        return 1
    if arch == "ARM":
        return vcpu
    return max(1, math.ceil(vcpu / 2))


def _fallback_arch(node_type_id: str) -> Tuple[str, bool, str]:
    """Guess arch from family prefix. Returns (arch, ambiguous, source_note).
    Falls through to AMD if no rule matches."""
    if not node_type_id:
        return ("AMD", False, "empty node_type — default to AMD")
    for pat, arch, ambiguous in _FAMILY_ARCH_RULES:
        if pat.match(node_type_id):
            return (arch, ambiguous, f"family-suffix match: {pat.pattern}")
    return ("AMD", False, "no family-suffix match — default to AMD (per unknown-x86 rule)")


def resolve_shape(node_type_id: Optional[str]) -> ShapeResolution:
    """Map a DBX node_type_id to an AIDP shape resolution.

    Lookup order:
      1. Exact match in DBX_NODE_CATALOG.
      2. Family-suffix parsing (guess arch; guess vCPU/memory as small default).
      3. Absolute fallback: AMD, 2 OCPU, 16 GB.

    The returned ShapeResolution carries both the AIDP-side shape config and
    the DBX-side context (arch, vCPU, memory) so the caller can log the
    mapping decision for operator review.
    """
    node_type = node_type_id or ""

    # Path 1: exact catalog hit
    entry = DBX_NODE_CATALOG.get(node_type)
    if entry:
        arch = entry["arch"]
        vcpu = entry["vcpu"]
        mem_gb = entry["mem_gb"]
        gpus = entry.get("gpus", 0)
        gpu_model = entry.get("gpu_model")
        ambiguous = bool(entry.get("ambiguous"))
        # arch records the DBX-side FACT (what the AWS node actually was);
        # aidp_shape_override records a per-entry TARGET decision when the
        # arch-derived shape can't host the node (e.g. c6i.24xlarge's
        # 48 OCPUs exceed intel.generic's ceiling). Keeping the two apart
        # keeps the mapping log's source identity truthful.
        override = entry.get("aidp_shape_override")
        shape = override or ARCH_TO_AIDP_SHAPE[arch]
        ocpus = _ocpu_from_vcpu(vcpu, arch)
        if ambiguous:
            note = (f"catalog match, x86-fleet — Intel vs AMD not identifiable; "
                    f"defaulted to AMD per unknown-x86 rule")
        elif override:
            note = (f"catalog match; exact vCPU/memory preserved; shape "
                    f"overridden to {override} ({arch}-derived shape cannot "
                    f"host this node's capacity)")
        else:
            note = "catalog match; exact vCPU/memory preserved"
        return ShapeResolution(
            dbx_node_type=node_type, dbx_arch=arch,
            dbx_vcpu=vcpu, dbx_mem_gb=mem_gb,
            aidp_shape=shape, ocpus=ocpus, memory_gb=mem_gb,
            gpus=gpus, gpu_model=gpu_model,
            resolution="catalog", ambiguous=ambiguous, note=note,
        )

    # Path 2: family-suffix fallback
    if node_type:
        arch, ambiguous, suffix_note = _fallback_arch(node_type)
        # Without a catalog hit we don't know vCPU/memory — use a reasonable
        # small default (2 vCPU, 16 GB). The mapping note flags this.
        vcpu = 2
        mem_gb = 16
        shape = ARCH_TO_AIDP_SHAPE[arch]
        ocpus = _ocpu_from_vcpu(vcpu, arch)
        # GPU shapes MUST have gpus>=1 on AIDP — an unknown GPU family gets
        # the smallest single-GPU default. Non-GPU shapes get gpus=0.
        gpus = 1 if arch == "GPU" else 0
        gpu_model = "unknown" if arch == "GPU" else None
        note = (f"UNKNOWN DBX node_type — {suffix_note}; "
                f"vCPU/memory guessed as 2/16 GB"
                + (f", 1 GPU guessed" if arch == "GPU" else "")
                + " (review before running large workloads)")
        return ShapeResolution(
            dbx_node_type=node_type, dbx_arch=arch,
            dbx_vcpu=vcpu, dbx_mem_gb=mem_gb,
            aidp_shape=shape, ocpus=ocpus, memory_gb=mem_gb,
            gpus=gpus, gpu_model=gpu_model,
            resolution="family-suffix", ambiguous=ambiguous, note=note,
        )

    # Path 3: absolute fallback (empty node_type_id)
    # OCPU uses the AMD formula (ceil(vcpu/2)) for consistency with Path 2.
    _default_vcpu, _default_mem = 2, 16
    _default_ocpus = _ocpu_from_vcpu(_default_vcpu, "AMD")
    return ShapeResolution(
        dbx_node_type="", dbx_arch="AMD",
        dbx_vcpu=_default_vcpu, dbx_mem_gb=_default_mem,
        aidp_shape=AIDP_SHAPE_AMD, ocpus=_default_ocpus, memory_gb=_default_mem,
        resolution="unknown-default", ambiguous=False,
        note=f"no DBX node_type provided — defaulted to AMD {_default_ocpus} OCPU / {_default_mem} GB",
    )


def format_mapping_log_line(role: str, res: ShapeResolution) -> str:
    """Render a one-line log message describing the mapping decision.

    `role` is 'driver' or 'worker' (or similar). Format:
        [shape] driver: <dbx_type> (<arch>, <vcpu> vCPU / <mem_gb> GB[, <gpus>x<gpu_model>])
              → AIDP: <aidp_shape> (<ocpus> OCPU / <mem_gb> GB[, gpus=<n>])  [<note>]
    A WARN prefix is added for ambiguous fallbacks or family-suffix guesses.
    """
    is_warn = res.ambiguous or res.resolution != "catalog"
    prefix = "[shape][WARN]" if is_warn else "[shape]"

    dbx_extra = ""
    if res.gpus and res.gpu_model:
        dbx_extra = f", {res.gpus}x{res.gpu_model}"

    aidp_extra = ""
    if res.gpus:
        aidp_extra = f", gpus={res.gpus}"

    return (
        f"{prefix} {role}: {res.dbx_node_type or '(none)'} "
        f"({res.dbx_arch}, {res.dbx_vcpu} vCPU / {res.dbx_mem_gb} GB{dbx_extra}) "
        f"→ AIDP: {res.aidp_shape} ({res.ocpus} OCPU / {res.memory_gb} GB{aidp_extra}) "
        f"[{res.note}]"
    )
