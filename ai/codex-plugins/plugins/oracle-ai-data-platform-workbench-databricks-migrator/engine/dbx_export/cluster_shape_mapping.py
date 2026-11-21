#!/usr/bin/env python3
"""
AIDP cluster shape mapping & normalization.

Problem
-------
Databricks/AWS lets a cluster use any OCPU/memory combination. AIDP does not:
each shape allows only a DISCRETE set of OCPU counts, and for each OCPU count
only a DISCRETE list of memory values (e.g. amd.generic 4 OCPU -> exactly
32, 64 or 128 GB — so 4/16 fails AND 4/48 would fail too). A Databricks spec
copied verbatim therefore often fails cluster creation with a 400.

The rules below were LIVE-VERIFIED by probing the create-cluster API with
invalid bodies and recording the exact validation errors — first on
2026-07-29 (us-ashburn-1), then on 2026-07-30 across FOUR regions
(us-ashburn-1, us-phoenix-1, ap-mumbai-1, ap-tokyo-1): all four returned
IDENTICAL matrices, so the limits are service constants, not regional data,
and ONE global table serves every region. Example messages:

    Invalid driverShapeConfig.memoryInGBs.  Valid values are: [32, 64, 128].
    Invalid driverShapeConfig.ocpus.  Valid values are: [1, 2, 4, 8, 16, 32, 64].
    driverConfig.driverShapeConfig.memoryInGBs must be greater than or equal to 16
    minWorkerCount should be greater than or equal to 1 for spark cluster
    workerConfig.maxWorkerCount must be less than or equal to 2000
    autoTerminationMinutes must be greater than or equal to 5
    autoTerminationMinutes must be less than or equal to 59940
    Unsupported Spark version: ... Supported versions are: 2.4.4, 3.0.2,
    3.0.2_runtime_v2, 3.2.1, 3.5.0

To re-verify the table by hand (e.g. after an AIDP platform update, or to
add a shape like gpu.a10): POST an intentionally invalid create body
(ocpus=999, then memoryInGBs=17 per valid OCPU count) to
.../workspaces/<ws>/clusters and copy the valid lists out of the 400
messages — the probes create nothing. See _meta.method in the table.

This module owns the mapping and the fix:

1. ``config/aidp_shape_limits.json`` — the SINGLE source of truth for the
   per-shape valid OCPU counts and, per OCPU count, the valid memory list.
   Loaded by ``load_shape_limits()``; selection precedence: explicit path
   arg > $AIDP_SHAPE_LIMITS_PATH > the committed default.
   ``_meta.verified_in_regions`` records where the values were probed.
2. ``normalize_shape_config()`` — snaps any requested (ocpus, memory) onto
   the nearest valid combo (never downsizing below the request when a bigger
   valid value exists) and returns an audit trail of every adjustment.
3. ``parse_limits_from_error()`` / ``is_limits_validation_error()`` —
   extract valid values from an AIDP 400 message so callers can self-heal
   the limits file and retry (the detector is defined via the parser, so
   the AIDP-wording knowledge has exactly one home).

Scope note: this module is only about VALIDITY (what AIDP accepts). The
Databricks node_type_id -> shape/OCPU/memory TRANSLATION lives exclusively in
dbx_export/node_type_catalog.py (resolve_shape) — do not add a second
node-type table here; two catalogs drift and the exporter only reads that one.
"""
from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

PROJECT_DIR = Path(__file__).resolve().parent.parent
# The global limits table (limits are identical in every probed region —
# see the module docstring). Path resolution at call time (_limits_path):
#   1. explicit config_path arg (internal API)
#   2. $AIDP_SHAPE_LIMITS_PATH (the --shape-limits-path CLI flag sets this)
#   3. this committed default
LIMITS_CONFIG_PATH = PROJECT_DIR / "config" / "aidp_shape_limits.json"

# Interprocess lock for the limits file (parallel exporters/migrations all
# self-heal into the same store). Shared, always-real lock — no silent no-op
# fallback (see dbx_export/file_lock.py).
from .file_lock import FileLock as _InterprocessFileLock

# Cluster-level constraints (all LIVE-VERIFIED 2026-07-29). Enforced by the
# converter (dbx_export/cluster_converter.py), which also owns the
# DBX-specific semantics (autotermination_minutes=0 -> omit the key).
MAX_WORKER_COUNT = 2000       # "maxWorkerCount must be <= 2000"
# Two validation layers exist for autoTerminationMinutes: a schema bound of
# [5, 59940] AND a stricter business rule "Please set an idle timeout between
# 15 minutes and 44640 minutes only" (hit live on an otherwise-valid create).
# The stricter rule is the effective one.
AUTO_TERMINATION_MIN = 15     # minutes
AUTO_TERMINATION_MAX = 44640  # minutes (31 days)
# Supported Spark versions (live-verified): 2.4.4, 3.0.2, 3.0.2_runtime_v2,
# 3.2.1, 3.5.0 — the exporter pins AIDP_SPARK_VERSION and never selects.
GLOBAL_MEMORY_GB_MIN = 16     # schema-level floor, overrides per-OCPU lists


def _limits_path(config_path: Optional[Path] = None) -> Path:
    """Resolve the limits file: arg > $AIDP_SHAPE_LIMITS_PATH > the
    committed global default. See the LIMITS_CONFIG_PATH comment."""
    if config_path:
        return Path(config_path)
    env = os.environ.get("AIDP_SHAPE_LIMITS_PATH")
    if env:
        return Path(env)
    return Path(LIMITS_CONFIG_PATH)


def _read_limits_file(path: Path, required: bool) -> dict:
    """Read the raw JSON object. Missing file: {} when not required, else
    ShapeMappingError. A CORRUPT file always raises — it must never silently
    become an empty table (a later save would clobber recoverable data)."""
    try:
        text = path.read_text()
    except FileNotFoundError as exc:
        if not required:
            return {}
        raise ShapeMappingError(
            f"Shape limits file unavailable ({path}): {exc}. "
            f"Restore the committed config/aidp_shape_limits.json"
        ) from exc
    try:
        raw = json.loads(text)
        if not isinstance(raw, dict):
            raise ValueError(f"expected a JSON object, got {type(raw).__name__}")
    except ValueError as exc:
        raise ShapeMappingError(
            f"Shape limits file is CORRUPT ({path}): {exc}. Refusing to treat "
            f"it as empty — inspect/fix it, or restore the committed "
            f"config/aidp_shape_limits.json"
        ) from exc
    return raw


def _atomic_write_json(path: Path, obj: dict) -> None:
    """Write via temp file + os.replace so readers never see partial JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def check_limits_table(config_path: Optional[Path] = None) -> dict:
    """Validate the resolved limits table BEFORE any conversion work.
    Returns {"path", "status"} with status one of:
      "usable"  — table present with shape entries
      "missing" — file absent or has no shape entries (lazy self-heal can
                  populate it from create-API validation errors)
      "broken"  — unreadable JSON (never overwritten; operator must fix)
    """
    path = _limits_path(config_path)
    try:
        raw = _read_limits_file(path, required=False)
    except ShapeMappingError:
        return {"path": path, "status": "broken"}
    shapes = {k for k, v in raw.items()
              if not k.startswith("_") and isinstance(v, dict)}
    if not shapes:
        return {"path": path, "status": "missing"}
    return {"path": path, "status": "usable"}


def load_shape_limits(config_path: Optional[Path] = None,
                      required: bool = True) -> dict:
    """Load the shape-limits table — the single source of truth (there is no
    in-code copy). Path resolution: see _limits_path().

    Returns ``{shape_name: limits_dict}``; metadata keys (``_meta`` etc.) are
    filtered out. ``required=True`` (default): a missing file raises
    ShapeMappingError telling the operator how to regenerate it;
    ``required=False``: a missing file returns ``{}`` (used by startup checks,
    conversion pass-through, and lazy self-healing). A corrupt file ALWAYS
    raises.
    """
    raw = _read_limits_file(_limits_path(config_path), required)
    return {shape: vals for shape, vals in raw.items()
            if not shape.startswith("_") and isinstance(vals, dict)}


def update_shape_limits(updater: Callable[[dict], None],
                        config_path: Optional[Path] = None,
                        meta_updates: Optional[dict] = None,
                        replace: bool = False) -> dict:
    """Interprocess-safe read-modify-write TRANSACTION on the limits file.

    Takes the file lock, re-reads the CURRENT table (so values learned by a
    concurrent process are preserved), applies ``updater(limits)`` in place,
    and writes the result atomically (metadata keys preserved). Returns the
    updated table. A corrupt file raises — it is never clobbered.

    ``replace=True`` discards the existing table AND metadata and starts
    from empty (the updater sees {}). This is reserved for callers that
    intentionally rebuild the complete table.

    ``meta_updates`` merges keys into the ``_meta`` block within the same
    transaction — lazy self-healing uses it to mark a contradicted or newly
    learned shape as partially verified. It may be a plain dict, or a callable
    ``(current_meta, limits_after_update) -> dict`` for updates that must be
    computed against the file's CURRENT state under the lock.
    """
    path = _limits_path(config_path)
    with _InterprocessFileLock(str(path)):
        raw = _read_limits_file(path, required=False)
        current_meta = _meta_of(raw)
        if replace:
            meta = {}
            limits: dict = {}
        else:
            meta = {k: v for k, v in raw.items() if k.startswith("_")}
            limits = {k: v for k, v in raw.items()
                      if not k.startswith("_") and isinstance(v, dict)}
        updater(limits)
        if meta_updates:
            updates = (meta_updates(dict(current_meta), limits)
                       if callable(meta_updates) else meta_updates)
            if updates:
                meta.setdefault("_meta", {})
                if isinstance(meta["_meta"], dict):
                    meta["_meta"].update(updates)
        out = dict(meta)
        out.update({k: v for k, v in limits.items() if not k.startswith("_")})
        _atomic_write_json(path, out)
    return limits


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ShapeAdjustment:
    """One field the normalizer changed, with the reason — for reports/logs."""
    field_name: str
    requested: float
    applied: float
    reason: str


@dataclass(frozen=True)
class NormalizedShape:
    shape: str
    ocpus: int
    memory_gb: int
    gpus: int = 0
    adjustments: list = field(default_factory=list)

    @property
    def was_adjusted(self) -> bool:
        return bool(self.adjustments)

    def describe(self) -> str:
        if not self.adjustments:
            return f"{self.shape}: {self.ocpus} OCPU / {self.memory_gb} GB (unchanged)"
        parts = "; ".join(
            f"{a.field_name} {a.requested:g}->{a.applied:g} ({a.reason})"
            for a in self.adjustments
        )
        return f"{self.shape}: {self.ocpus} OCPU / {self.memory_gb} GB [adjusted: {parts}]"


class ShapeMappingError(ValueError):
    """Raised when a request cannot be normalized into any valid combo."""


class ShapeCapacityError(ShapeMappingError):
    """The requested capacity cannot be satisfied as specified: per-node
    OCPU/memory above the shape maximum, a guaranteed/fixed worker count
    above AIDP's cluster maximum, or an invalid min>max worker range.

    AIDP cannot satisfy the request, and delivering LESS would silently
    under-provision the workload — deferring the failure to runtime, where
    it is far harder to trace back here. Unsatisfiable requests therefore
    fail loudly at conversion time; the operator resizes deliberately.
    (Autoscale CEILINGS are the exception — an allowance, not a provision —
    and are clamped with a log line instead; see convert_dbx_cluster.)"""


def _meta_of(raw: dict) -> dict:
    """The ``_meta`` block of a raw limits file, or {}."""
    meta = raw.get("_meta")
    return meta if isinstance(meta, dict) else {}


def _snap_up(value: float, valid: list) -> int:
    """Smallest valid entry >= value; raises when the request exceeds all.

    Never snaps down: delivering less than the request would defer the
    failure to runtime (see ShapeCapacityError)."""
    candidates = [v for v in sorted(valid) if v >= value]
    if not candidates:
        raise ShapeCapacityError(
            f"requested {value:g} exceeds the maximum valid value "
            f"{int(max(valid))} (valid: {sorted(int(v) for v in valid)})")
    return int(candidates[0])


def normalize_shape_config(
    ocpus: float,
    memory_gb: float,
    shape: str = "amd.generic",
    gpus: int = 0,
    limits: Optional[dict] = None,
) -> NormalizedShape:
    """Snap (ocpus, memory_gb) onto a valid AIDP combo for ``shape``.

    AIDP only accepts discrete values (both OCPU count and memory), so this
    snaps UP to the next valid step — the workload never gets less than it
    asked for. A request above the shape maximum raises ShapeCapacityError:
    AIDP cannot satisfy it, and under-provisioning would defer the failure
    to a runtime OOM. Every change is recorded in ``adjustments``.

    Raises ShapeMappingError for unknown shapes or inconsistent limit tables.
    """
    all_limits = limits or load_shape_limits()
    if shape not in all_limits:
        raise ShapeMappingError(
            f"Unknown shape '{shape}'. Known shapes: {sorted(all_limits)}"
        )
    rules = all_limits[shape]
    adjustments: list = []

    # --- GPU shapes: snap to the nearest fixed bundle -----------------------
    if "fixed_combos" in rules:
        combos = rules["fixed_combos"]
        want_gpus = max(1, int(gpus))
        combo = min(combos, key=lambda c: (abs(c["gpus"] - want_gpus), c["gpus"]))
        if combo["gpus"] != gpus:
            adjustments.append(ShapeAdjustment(
                "gpus", gpus, combo["gpus"], "GPU shapes only support fixed bundles"))
        if combo["ocpus"] != ocpus:
            adjustments.append(ShapeAdjustment(
                "ocpus", ocpus, combo["ocpus"],
                f"{combo['gpus']} GPU bundle is fixed at {combo['ocpus']} OCPU"))
        mem = int(combo.get("memoryInGBs", memory_gb) or memory_gb)
        if mem != memory_gb:
            adjustments.append(ShapeAdjustment(
                "memory_gb", memory_gb, mem, "fixed GPU bundle memory"))
        return NormalizedShape(shape, combo["ocpus"], mem, combo["gpus"], adjustments)

    ocpus_valid = sorted(int(v) for v in rules.get("ocpus_valid", []))
    memory_by_ocpu = rules.get("memory_by_ocpu", {})

    # --- OCPU: snap up to the next valid count ------------------------------
    # A table entry may be PARTIAL (e.g. built incrementally by the self-heal
    # path from one 400 message): without an ocpus_valid list we keep the
    # requested count (rounded up to a whole number) and only validate memory.
    if ocpus_valid:
        if ocpus > max(ocpus_valid):
            raise ShapeCapacityError(
                f"{ocpus:g} OCPUs exceeds the maximum of {max(ocpus_valid)} "
                f"for '{shape}' (valid: {ocpus_valid})")
        oc = _snap_up(ocpus, ocpus_valid)
        if oc != ocpus:
            adjustments.append(ShapeAdjustment(
                "ocpus", ocpus, oc,
                f"OCPU count must be one of {ocpus_valid} (snapped up)"))
    else:
        oc = int(math.ceil(ocpus))
        if oc != ocpus:
            adjustments.append(ShapeAdjustment(
                "ocpus", ocpus, oc, "OCPUs must be a whole number (rounded up)"))

    # --- Memory: snap up within the valid list for that OCPU count ----------
    valid_mem = memory_by_ocpu.get(str(oc)) or memory_by_ocpu.get(oc)
    if not valid_mem:
        raise ShapeMappingError(
            f"Limits table for '{shape}' has no memory list for {oc} OCPU — "
            f"restore the committed config/aidp_shape_limits.json or let "
            f"lazy API-error learning fill it in"
        )
    # Enforce the schema-level global floor on top of the per-OCPU list
    # (live-verified: the API lists 8 GB for 1 OCPU but rejects < 16 GB).
    effective_mem = [v for v in valid_mem if v >= GLOBAL_MEMORY_GB_MIN] or valid_mem
    if memory_gb > max(effective_mem):
        raise ShapeCapacityError(
            f"{memory_gb:g} GB exceeds the maximum of {max(effective_mem)} GB "
            f"for {oc} OCPU on '{shape}' — raise the OCPU count or lower the "
            f"memory request")
    mem = _snap_up(memory_gb, effective_mem)
    if mem != memory_gb:
        adjustments.append(ShapeAdjustment(
            "memory_gb", memory_gb, mem,
            f"memory for {oc} OCPU on {shape} must be one of "
            f"{effective_mem} (snapped up)"))

    return NormalizedShape(shape, oc, mem, 0, adjustments)


# ---------------------------------------------------------------------------
# Self-healing: learn real limits from AIDP 400 validation messages
# ---------------------------------------------------------------------------

# Real message shapes (live-captured 2026-07-29):
#   Invalid driverShapeConfig.memoryInGBs.  Valid values are: [32, 64, 128].
#   Invalid driverShapeConfig.ocpus.  Valid values are: [1, 2, 4, 8, 16, 32, 64].
#   Invalid executorShapeConfig.memoryInGBs.  Valid values are: [16, 32, 64].
#   ...memoryInGBs must be greater than or equal to 16
#   ...memoryInGBs must be less than or equal to 2000
_VALID_VALUES_RE = re.compile(
    r"Invalid\s+\w*ShapeConfig\.(memoryInGBs|ocpus)\.\s*Valid values are:\s*"
    r"\[([0-9,\s]+)\]", re.I)
_GE_RE = re.compile(r"memoryInGBs\s+must\s+be\s+greater\s+than\s+or\s+equal\s+to\s+(\d+)", re.I)
_LE_RE = re.compile(r"memoryInGBs\s+must\s+be\s+less\s+than\s+or\s+equal\s+to\s+(\d+)", re.I)


def is_limits_validation_error(error_text: str) -> bool:
    """True when an AIDP create error carries learnable shape limits — i.e.
    parse_limits_from_error() would extract something. This is the ONLY gate
    callers may use to decide whether a 400 is a limits error: keeping the
    detector defined in terms of the parser means AIDP wording changes can
    never make them drift apart."""
    return bool(parse_limits_from_error(error_text))


def parse_limits_from_error(error_text: str, ocpus: Optional[int] = None) -> dict:
    """Best-effort extraction of limits from an AIDP 400 message.

    Returns a partial dict with any of:
      - ``memory_valid_values`` / ``ocpus_valid`` (from "Valid values are: [...]")
      - ``memory_gb_min`` / ``memory_gb_max`` (from the >=16 / <=2000 bounds)
    Empty dict when nothing matched — callers should then surface the raw
    error instead of retrying blindly.
    """
    out: dict = {}
    for m in _VALID_VALUES_RE.finditer(error_text):
        vals = [int(x) for x in m.group(2).replace(" ", "").split(",") if x]
        if m.group(1).lower() == "memoryingbs":
            out["memory_valid_values"] = vals
        else:
            out["ocpus_valid"] = vals
    m = _GE_RE.search(error_text)
    if m:
        out["memory_gb_min"] = int(m.group(1))
    m = _LE_RE.search(error_text)
    if m:
        out["memory_gb_max"] = int(m.group(1))
    return out


def apply_learned_limits(limits: dict, shape: str, ocpus: int, learned: dict) -> bool:
    """Merge a parse_limits_from_error() result into a limits table in place.

    Returns True if anything changed (so the caller knows a retry makes sense).
    """
    changed = False
    entry = limits.setdefault(shape, {})
    if "ocpus_valid" in learned:
        if entry.get("ocpus_valid") != learned["ocpus_valid"]:
            entry["ocpus_valid"] = learned["ocpus_valid"]
            changed = True
    if "memory_valid_values" in learned and ocpus:
        by_ocpu = entry.setdefault("memory_by_ocpu", {})
        if by_ocpu.get(str(ocpus)) != learned["memory_valid_values"]:
            by_ocpu[str(ocpus)] = learned["memory_valid_values"]
            changed = True
    return changed
