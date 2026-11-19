#!/usr/bin/env python3
# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v1.0 as shown at
# https://oss.oracle.com/licenses/upl/
"""OCI compute capacity — ARM + AMD capacity-estimate report for any OCI region.

Answers: "How many VM nodes of size S does OCI estimate it can launch right now
in region R, for ARM (Ampere) and for AMD?" Useful when sizing Spark clusters
for services where the node shape matters, such as OCI Data Flow, OCI Compute,
or Oracle AI Data Platform clusters (ARM vs AMD).

Each number comes from OCI's CreateComputeCapacityReport API at one EXACT
(shape, OCPUs, memory) configuration. It is a best-effort, point-in-time
estimate for THAT configuration — not a hardware inventory, not a reservation,
and not a launch guarantee.

Count visibility: depending on your tenancy's access level, OCI may return the
availability status (AVAILABLE / OUT_OF_HOST_CAPACITY) without a count. The
report then marks the value with a dagger ("†") instead of guessing.

Region-generic: ADs, shape families, their limits and their processors are all
discovered at runtime from ListShapes, so the report works in any region and
picks up existing and future families with no code change. Multi-AD regions are
probed in EVERY AD and consolidated (region total, per-AD in parentheses).
--ad restricts to one AD.

OCPU vs vCPU depends on the processor (Oracle "OCPUs and vCPUs"):
  Ampere Altra (A1)          1 OCPU = 1 vCPU
  AmpereOne    (A2, A4)      1 OCPU = 2 vCPUs
  x86          (AMD, Intel)  1 OCPU = 2 vCPUs
ListShapes has no vCPU field, so the ratio is derived from each shape's
processor description and shapes are GROUPED by it — every report row then has
one true vCPU size. An unrecognized processor shows "?" rather than a guess.
Only non-GPU ARM and AMD VM shapes are probed (standard and DenseIO); GPU,
bare-metal, Intel and ".Generic" placeholder shapes are skipped.

Modes
-----
Default — ratio sweep: for each OCPU size, probe at memory = size x ratio
  (defaults: ARM 8 GB per OCPU, AMD 16 GB per OCPU).
--ocpus N --memory-gb M — fit check for one explicit configuration.
--show-shapes — list the discovered shapes, their processors and limits.

Usage
-----
    python3 oci_capacity_report.py                        # profile's region
    python3 oci_capacity_report.py --region us-ashburn-1  # all ADs, consolidated
    python3 oci_capacity_report.py --region us-ashburn-1 --detail
    python3 oci_capacity_report.py --region us-ashburn-1 --ad AD-2
    python3 oci_capacity_report.py --sizes 8,32 --arm-ratio 8 --amd-ratio 16
    python3 oci_capacity_report.py --ocpus 32 --memory-gb 512
    python3 oci_capacity_report.py --show-shapes
    python3 oci_capacity_report.py --profile <session-profile> --auth security_token

Exit codes
----------
  0  every probe returned a definite answer with a count
     (AVAILABLE with a count, OUT_OF_HOST_CAPACITY, or a rejected config)
  3  every probe returned a definite status, but OCI did not return a count
     for at least one AVAILABLE shape (totals are lower bounds)
  2  coverage incomplete: some probe failed, timed out, was missing or ambiguous
  1  setup error (config, auth, bad arguments)

Requirements
------------
- `oci` CLI on PATH (https://docs.oracle.com/iaas/Content/API/SDKDocs/cliinstall.htm)
- `~/.oci/config` (or $OCI_CLI_CONFIG_FILE) with the chosen profile; API key by
  default, or a session token with --auth security_token (or $OCI_CLI_AUTH)
- IAM:  Allow group <your-group> to manage compute-capacity-reports in tenancy
        Allow group <your-group> to inspect instance-family in tenancy   (ListShapes)
"""
from __future__ import annotations

import argparse
import configparser
import json
import math
import os
import re
import subprocess
import sys
import time
from typing import Any

# Shape entries sent per capacity-report request. 3 is a conservative value
# that has worked in every tenancy we tested; the accepted maximum can vary by
# tenancy, so it is configurable (--batch-size).
DEFAULT_BATCH_SIZE = 3
CLI_TIMEOUT_S = 180                  # one OCI CLI call; a hang becomes REQUEST_FAILED
THROTTLE_RETRIES = 3                 # retries for a throttled (HTTP 429) call
THROTTLE_BACKOFF_S = 2               # first backoff; doubles on each retry
DEFAULT_SIZES = (1, 2, 4, 8, 16, 32, 64)
# OCI CLI auth method passed as `--auth` (set from --auth in main). None keeps
# the CLI default: API key, or whatever $OCI_CLI_AUTH selects.
AUTH: str | None = None

# Result statuses (OCI's availability-status values plus the script's own).
COUNTED = {"AVAILABLE", "OUT_OF_HOST_CAPACITY"}           # definite, with a count
NO_COUNT = "AVAILABLE_NO_COUNT"                           # definite, count not returned
NOT_PROBEABLE = {"INVALID_CONFIG", "HARDWARE_NOT_SUPPORTED"}  # definite, not launchable


# ---------------------------------------------------------------- config

def _config_path() -> str:
    return os.environ.get("OCI_CLI_CONFIG_FILE",
                          os.path.expanduser("~/.oci/config"))


def _profile_section(profile: str) -> configparser.SectionProxy:
    path = _config_path()
    if not os.path.exists(path):
        sys.exit(f"ERROR: OCI config not found: {path}")
    cfg = configparser.ConfigParser()
    cfg.read(path)
    if profile not in cfg:
        sys.exit(f"ERROR: profile [{profile}] not found in {path}")
    return cfg[profile]


def _tenancy_from_profile(profile: str) -> str:
    sec = _profile_section(profile)
    if "tenancy" not in sec:
        sys.exit(f"ERROR: profile [{profile}] has no `tenancy` entry in {_config_path()}")
    return sec["tenancy"]


def _region_from_profile(profile: str) -> str:
    sec = _profile_section(profile)
    if "region" not in sec:
        sys.exit(f"ERROR: profile [{profile}] has no `region` entry; pass --region")
    return sec["region"]


def _is_throttled(err: str) -> bool:
    return "TooManyRequests" in err or '"status": 429' in err


def _oci(args: list[str], profile: str, region: str) -> tuple[Any, str | None]:
    """Run one OCI CLI command; returns (parsed JSON, None) or (None, error text).

    A throttled call (HTTP 429 / TooManyRequests) is retried with exponential
    backoff up to THROTTLE_RETRIES times; any other failure is returned as-is.
    """
    cmd = ["oci", *args, "--config-file", _config_path(),
           "--profile", profile, "--region", region, "--output", "json"]
    if AUTH:
        cmd += ["--auth", AUTH]
    for attempt in range(THROTTLE_RETRIES + 1):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=CLI_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            return None, f"OCI CLI call timed out after {CLI_TIMEOUT_S}s"
        except FileNotFoundError:
            sys.exit("ERROR: `oci` CLI not found on PATH")
        if r.returncode == 0:
            try:
                return json.loads(r.stdout), None
            except json.JSONDecodeError:
                return None, f"unparseable response: {r.stdout[:200]}"
        err = (r.stderr or r.stdout).strip()
        if attempt < THROTTLE_RETRIES and _is_throttled(err):
            time.sleep(THROTTLE_BACKOFF_S * 2 ** attempt)
            continue
        return None, err[:400]
    return None, "unreachable"


# ------------------------------------------------------- shape discovery

def _arch(shape: dict) -> str | None:
    p = (shape.get("processor-description") or "").lower()
    name = shape.get("shape", "")
    if not name.startswith("VM.") or ".GPU" in name or name.endswith(".Generic"):
        return None
    if "ampere" in p:
        return "ARM"
    if "amd" in p:
        return "AMD"
    return None


def vcpu_per_ocpu(arch: str, processor: str) -> int | None:
    """vCPUs per OCPU from the processor (Oracle "OCPUs and vCPUs").

    Altra (A1) = 1; AmpereOne (A2, A4, incl. "AmpereOne-M") = 2; x86 = 2.
    An unrecognized Ampere processor returns None so the report prints "?"
    instead of applying a ratio that might be wrong for a new family.
    """
    if arch == "AMD":
        return 2
    p = re.sub(r"[^a-z]", "", (processor or "").lower())    # drop ®, ™, spaces
    if "ampereone" in p:
        return 2
    if "altra" in p:
        return 1
    return None


def group_key(shape: dict) -> tuple[str, int | None]:
    """Report group: architecture + vCPUs per OCPU, so each row has one vCPU size."""
    return (shape["arch"], shape["vpo"])


def discover_shapes(profile: str, region: str, tenancy: str, ad: str) -> list[dict]:
    """Return ARM/AMD VM shapes offered in the AD, with limits from ListShapes."""
    data, err = _oci(["compute", "shape", "list", "--compartment-id", tenancy,
                      "--availability-domain", ad, "--all"], profile, region)
    if data is None:
        sys.exit(f"ERROR: ListShapes failed: {err}")
    seen: dict[str, dict] = {}
    for s in data.get("data", []):
        arch = _arch(s)
        if not arch or s["shape"] in seen:
            continue
        oo = s.get("ocpu-options")
        mo = s.get("memory-options")
        fixed_oc = s.get("ocpus")
        fixed_mem = s.get("memory-in-gbs")
        # Per Oracle's Shape contract, a NULL ocpu-options / memory-options
        # means that dimension is FIXED at `ocpus` / `memoryInGBs` — never
        # "unrestricted". Fall back to min=max=fixed value.
        if oo:
            min_oc, max_oc = oo.get("min"), oo.get("max")
        else:
            min_oc = max_oc = fixed_oc
        if mo:
            min_mem, max_mem = mo.get("min-in-g-bs"), mo.get("max-in-g-bs")
            min_per, max_per = mo.get("min-per-ocpu-in-gbs"), mo.get("max-per-ocpu-in-gbs")
        else:
            min_mem = max_mem = fixed_mem
            min_per = max_per = None   # implied by fixed ocpu+mem
        proc = s.get("processor-description") or ""
        seen[s["shape"]] = {
            "shape": s["shape"], "arch": arch, "proc": proc,
            "vpo": vcpu_per_ocpu(arch, proc),
            "flexible": bool(s.get("is-flexible")),
            "ocpus": fixed_oc, "mem": fixed_mem,
            "min_oc": min_oc, "max_oc": max_oc,
            "min_mem": min_mem, "max_mem": max_mem,
            "min_per_oc": min_per, "max_per_oc": max_per,
        }
    return sorted(seen.values(), key=lambda x: (x["arch"], x["shape"]))


def fits(shape: dict, oc: float, mem: float) -> tuple[bool, str]:
    """Does (oc, mem) satisfy this shape's published limits? Returns (ok, reason)."""
    if not shape["flexible"]:
        if shape["ocpus"] == oc and shape["mem"] == mem:
            return True, ""
        return False, f"fixed {_g(shape['ocpus'])}/{_g(shape['mem'])}"
    if shape["min_oc"] is None and shape["max_oc"] is None:
        return False, "no OCPU metadata"
    if shape["min_mem"] is None and shape["max_mem"] is None:
        return False, "no memory metadata"
    checks = [
        (shape["min_oc"], shape["max_oc"], oc, "ocpus"),
        (shape["min_mem"], shape["max_mem"], mem, "memory"),
        (shape["min_per_oc"], shape["max_per_oc"], mem / oc, "GB/OCPU"),
    ]
    for lo, hi, v, label in checks:
        if lo is not None and v < lo:
            return False, f"{label} {v:g} < min {lo:g}"
        if hi is not None and v > hi:
            return False, f"{label} {v:g} > max {hi:g}"
    return True, ""


# ---------------------------------------------------------- capacity API

def _send(profile, region, tenancy, ad, batch):
    payload = []
    for shape, oc, mem, flex in batch:
        item: dict[str, Any] = {"instanceShape": shape}
        if flex:
            item["instanceShapeConfig"] = {"ocpus": oc, "memoryInGBs": mem}
        payload.append(item)
    return _oci(["compute", "compute-capacity-report", "create",
                 "--compartment-id", tenancy, "--availability-domain", ad,
                 "--shape-availabilities", json.dumps(payload)], profile, region)


def _parse(batch, data) -> dict[tuple, dict]:
    """Map one capacity-report response onto the batch's probes.

    AVAILABLE without a count is reported as AVAILABLE_NO_COUNT: OCI can return
    the status without `available-count` (tenancy access level). Never invent a
    number. OUT_OF_HOST_CAPACITY is a genuine zero.
    """
    out: dict[tuple, dict] = {}
    by_shape: dict[str, list[dict]] = {}
    for row in (data.get("data", {}).get("shape-availabilities") or []):
        by_shape.setdefault(row.get("instance-shape"), []).append(row)
    for shape, oc, mem, _flex in batch:
        key = (shape, oc, mem)
        rows = by_shape.get(shape, [])
        if not rows:
            out[key] = {"status": "MISSING_FROM_RESPONSE", "count": None, "error": None}
        elif len(rows) > 1:
            out[key] = {"status": "AMBIGUOUS", "count": None,
                        "error": f"{len(rows)} rows for one probe"}
        else:
            row = rows[0]
            status = row.get("availability-status") or "UNKNOWN"
            count = row.get("available-count")
            if status == "AVAILABLE" and count is None:
                status = NO_COUNT
            elif status == "OUT_OF_HOST_CAPACITY":
                count = 0
            elif status == "HARDWARE_NOT_SUPPORTED":
                count = None
            out[key] = {"status": status, "count": count, "error": None}
    return out


def probe(profile: str, region: str, tenancy: str, ad: str,
          specs: list[tuple[str, float, float, bool]],
          batch_size: int = DEFAULT_BATCH_SIZE) -> dict[tuple, dict]:
    """specs: (shape, ocpus, mem, flexible). Returns {(shape,oc,mem): result}.

    - Batches never contain the same shape twice, so response rows are matched
      by shape name (no reliance on array order / echoed config).
    - OCI rejects a WHOLE batch with InvalidParameter if ANY one config in it is
      invalid (observed: VM.DenseIO.E6.Ax.Flex rejected 8 OCPU/128 GB and took
      the other shapes in its batch down with it). On a failed batch every spec
      is retried ALONE; a solo InvalidParameter is a definite answer ->
      INVALID_CONFIG. Any other failure stays REQUEST_FAILED (incomplete).
    """
    results: dict[tuple, dict] = {}
    pending = list(specs)
    while pending:
        batch, rest, used = [], [], set()
        for sp in pending:
            if sp[0] not in used and len(batch) < batch_size:
                batch.append(sp)
                used.add(sp[0])
            else:
                rest.append(sp)
        pending = rest

        data, err = _send(profile, region, tenancy, ad, batch)
        if data is not None:
            results.update(_parse(batch, data))
            continue
        if len(batch) == 1:
            results.update(_solo_failure(batch[0], err))
            continue
        for sp in batch:                           # isolate the bad one
            d1, e1 = _send(profile, region, tenancy, ad, [sp])
            if d1 is not None:
                results.update(_parse([sp], d1))
            else:
                results.update(_solo_failure(sp, e1))
    return results


def _solo_failure(sp, err) -> dict[tuple, dict]:
    status = "INVALID_CONFIG" if "InvalidParameter" in (err or "") else "REQUEST_FAILED"
    return {(sp[0], sp[1], sp[2]): {"status": status, "count": None, "error": err}}


# ----------------------------------------------------------------- report

DAGGER = "†"


def _g(v) -> str:
    """Format an optional number; None -> '?' (never crash on missing metadata)."""
    return "?" if v is None else f"{v:g}"


def _rng(lo, hi) -> str:
    if lo is None and hi is None:
        return "?"
    if lo == hi:
        return _g(lo)
    return f"{_g(lo)}-{_g(hi)}"


def _short(shape: str) -> str:
    return shape.replace("VM.Standard.", "").replace(".Flex", "").replace("VM.", "")


def _ad_short(ad: str) -> str:
    """'xxxx:US-ASHBURN-AD-2' -> 'AD-2'."""
    tail = ad.split(":")[-1]
    return "AD-" + tail.rsplit("-", 1)[-1] if "-AD-" in tail else tail


def _vcpus(oc: float, vpo: int | None) -> str:
    return "?" if vpo is None else f"{oc * vpo:g}"


def _group_label(key: tuple[str, int | None]) -> str:
    arch, vpo = key
    return f"{arch} ({'? vCPU' if vpo is None else f'{vpo} vCPU'}/OCPU)"


def _group_order(key: tuple[str, int | None]):
    return (key[0] != "ARM", key[1] is None, key[1] or 0)


def _groups(ads, shapes_by_ad) -> list[tuple[str, int | None]]:
    """Report groups present in these ADs: ARM before AMD, then by vCPU/OCPU."""
    return sorted({group_key(s) for ad in ads for s in shapes_by_ad[ad]}, key=_group_order)


def _fmt_total(total: int, hidden: bool, incomplete: bool) -> str:
    """Counted total, plus markers: † = some AVAILABLE shape had no count
    (true value is higher), * = some probe gave no definite answer."""
    if hidden and total == 0:
        s = f"avail{DAGGER}"
    else:
        s = f"{total:,}" + (f"+{DAGGER}" if hidden else "")
    return s + ("*" if incomplete else "")


def _cell_part(sh: dict, r: dict) -> str:
    name = _short(sh["shape"])
    st = r["status"]
    if st in COUNTED:
        return f"{name}={r['count']:,}"
    if st == NO_COUNT:
        return f"{name}=avail{DAGGER}"
    if st == "INVALID_CONFIG":
        return f"{name}=invalid-cfg"
    if st == "HARDWARE_NOT_SUPPORTED":
        return f"{name}=not-supported"
    return f"{name}={st}"


def _collect(profile, region, tenancy, ads, shapes_by_ad, group_specs, batch_size):
    """group_specs: {group: [(oc, mem), ...]} -> per (group, oc, mem, ad):
    {"total": int, "hidden": bool, "incomplete": bool, "any": bool, "parts": [str]}"""
    agg: dict[tuple, dict] = {}
    for ad in ads:
        shapes = shapes_by_ad[ad]
        specs, plan = [], {}
        for grp, pairs in group_specs.items():
            for oc, mem in pairs:
                entries = []
                for sh in (s for s in shapes if group_key(s) == grp):
                    ok, _why = fits(sh, oc, mem)
                    entries.append((sh, ok))
                    if ok:
                        specs.append((sh["shape"], oc, mem, sh["flexible"]))
                plan[(grp, oc, mem)] = entries
        res = probe(profile, region, tenancy, ad, specs, batch_size) if specs else {}
        for (grp, oc, mem), entries in plan.items():
            cell = {"total": 0, "hidden": False, "incomplete": False, "any": False, "parts": []}
            for sh, ok in entries:
                if not ok:
                    continue
                cell["any"] = True
                r = res[(sh["shape"], oc, mem)]
                if r["status"] in COUNTED:
                    cell["total"] += r["count"]
                elif r["status"] == NO_COUNT:
                    cell["hidden"] = True
                elif r["status"] not in NOT_PROBEABLE:
                    cell["incomplete"] = True
                cell["parts"].append(_cell_part(sh, r))
            agg[(grp, oc, mem, ad)] = cell
    return agg


def _limitations(hidden_seen: bool) -> None:
    print("Limitations: best-effort, point-in-time estimate from OCI's capacity report;")
    print("  not a reservation or launch guarantee. It does not consider capacity")
    print("  reservations or tenancy service limits/quotas (check those separately), and")
    print("  results can vary by tenancy.")
    if hidden_seen:
        print(f"{DAGGER}  NOTE: OCI reported AVAILABLE but returned NO COUNT for some shapes.")
        print("   Depending on your tenancy's access level, OCI may return availability")
        print(f"   without a count. Totals marked {DAGGER} are lower bounds; the actual number")
        print("   is unknown.")


def _vcpu_note() -> None:
    print("Sizes are in OCPUs (OCI's unit). vCPUs per OCPU depend on the processor:")
    print("  Ampere Altra (A1) 1, AmpereOne (A2/A4) 2, x86 (AMD/Intel) 2. Each section")
    print("  groups shapes with the same ratio. To compare sections, match rows by vCPU")
    print("  and memory, not OCPU.")


def sweep(profile, region, tenancy, ads, shapes_by_ad, ratios, sizes, detail,
          batch_size=DEFAULT_BATCH_SIZE) -> int:
    """Ratio sweep. ratios: {arch: GB per OCPU}. Returns the exit code (0, 2 or 3)."""
    groups = [g for g in _groups(ads, shapes_by_ad) if g[0] in ratios]
    group_specs = {g: [(float(s), float(s * ratios[g[0]])) for s in sizes] for g in groups}
    agg = _collect(profile, region, tenancy, ads, shapes_by_ad, group_specs, batch_size)
    incomplete_seen = hidden_seen = False
    multi = len(ads) > 1
    for grp in groups:
        ratio = ratios[grp[0]]
        fams = sorted({_short(s["shape"]) for ad in ads for s in shapes_by_ad[ad]
                       if group_key(s) == grp})
        print()
        print(f"=== {_group_label(grp)} @ 1 OCPU : {ratio:g} GB   (region total across "
              f"{len(ads)} AD{'s' if multi else ''})   families: {', '.join(fams)} ===")
        print(f"{'OCPU/VM':>8} {'vCPU/VM':>8} {'RAM/VM(GB)':>11} {'Estimate':>13}   "
              f"{'Per-AD' if multi else 'Per-family'}")
        print("-" * 96)
        for oc, mem in group_specs[grp]:
            total, hid, inc, any_ok, parts = 0, False, False, False, []
            for ad in ads:
                c = agg[(grp, oc, mem, ad)]
                total += c["total"]
                hid |= c["hidden"]
                inc |= c["incomplete"]
                any_ok |= c["any"]
                if multi:
                    parts.append(f"{_ad_short(ad)}={_fmt_total(c['total'], c['hidden'], c['incomplete'])}"
                                 if c["any"] else f"{_ad_short(ad)}=—")
                else:
                    parts.extend(c["parts"])
            if not any_ok:
                tot = "—"
            else:
                tot = _fmt_total(total, hid, inc)
                hidden_seen |= hid
                incomplete_seen |= inc
            print(f"{oc:>8g} {_vcpus(oc, grp[1]):>8} {mem:>11g} {tot:>13}   ({', '.join(parts)})")
            if detail and multi:
                for ad in ads:
                    c = agg[(grp, oc, mem, ad)]
                    if c["any"]:
                        print(f"{'':>45}{_ad_short(ad)}: {', '.join(c['parts'])}")
    print()
    print("Estimate = arithmetic sum of OCI's per-shape capacity estimates at that exact")
    print("  size" + (", across all ADs." if multi else "."))
    print("  Each estimate is for that shape+config+AD alone; the sum is NOT a")
    print("  simultaneously launchable count, even within one AD. A cluster usually")
    print("  lands in ONE AD, so the per-AD figure is the more relevant estimate.")
    print("Rows are alternative scenarios — do NOT add rows together.")
    _vcpu_note()
    print("—  = no discovered family supports that size here")
    if incomplete_seen:
        print("*  = INCOMPLETE: some shapes returned no definite answer (run --detail).")
    _limitations(hidden_seen)
    return 2 if incomplete_seen else (3 if hidden_seen else 0)


def fit_check(profile, region, tenancy, ads, shapes_by_ad, oc, mem,
              batch_size=DEFAULT_BATCH_SIZE) -> int:
    """One explicit configuration across every AD and shape. Returns the exit code."""
    print()
    print(f"Target: {oc:g} OCPU x {mem:g} GB ({mem/oc:.2f} GB/OCPU)")
    incomplete = False
    totals: dict[tuple, dict] = {}
    print(f"\n{'AD':<6} {'Shape':<26} {'Arch':<5} {'vCPU':>5} {'Status':<22} {'Estimate':>11}")
    print("-" * 84)
    for ad in ads:
        specs, rows = [], []
        for sh in shapes_by_ad[ad]:
            ok, _why = fits(sh, oc, mem)
            if ok:
                specs.append((sh["shape"], oc, mem, sh["flexible"]))
                rows.append(sh)
        res = probe(profile, region, tenancy, ad, specs, batch_size) if specs else {}
        for sh in rows:
            r = res[(sh["shape"], oc, mem)]
            st = r["status"]
            t = totals.setdefault(group_key(sh), {"total": 0, "hidden": False})
            if st in COUNTED:
                t["total"] += r["count"]
                cnt = f"{r['count']:,}"
            elif st == NO_COUNT:
                t["hidden"] = True
                cnt = f"not returned{DAGGER}"
            elif st in NOT_PROBEABLE:
                cnt = "—"
            else:
                incomplete, cnt = True, "N/A"
            print(f"{_ad_short(ad):<6} {sh['shape']:<26} {sh['arch']:<5} "
                  f"{_vcpus(oc, sh['vpo']):>5} {st:<22} {cnt:>11}")
    print()
    print(f"Region total at {oc:g} OCPU x {mem:g} GB"
          + ("   (* incomplete — some shapes N/A)" if incomplete else "") + ":")
    for grp in sorted(totals, key=_group_order):
        t = totals[grp]
        print(f"  {_group_label(grp):<22} {_fmt_total(t['total'], t['hidden'], False):>10}"
              f"   ({_vcpus(oc, grp[1])} vCPU per node)")
    # Groups with different vCPUs per OCPU are different machine sizes at the
    # same OCPU count. Point at the comparable runs instead of letting the
    # reader compare the totals above directly.
    ratios_present = sorted({g[1] for g in totals if g[1] is not None})
    if len(ratios_present) > 1:
        print("  Not comparable at the same OCPU count (vCPUs per OCPU differ). Same vCPUs:")
        for vpo in ratios_present:
            for other in ratios_present:
                if other == vpo:
                    continue
                need = oc * vpo / other
                label = f"{other}-vCPU/OCPU shapes at {oc * vpo:g} vCPU"
                if need.is_integer():
                    print(f"    {label}: --ocpus {need:g} --memory-gb {mem:g}")
                else:
                    print(f"    {label}: no whole-OCPU equivalent")
    print()
    hidden_seen = any(t["hidden"] for t in totals.values())
    _limitations(hidden_seen)
    return 2 if incomplete else (3 if hidden_seen else 0)


# ------------------------------------------------------------------- main

def _pos_finite(name: str, v: float) -> float:
    if v is None or not math.isfinite(v) or v <= 0:
        sys.exit(f"ERROR: --{name} must be a positive finite number (got {v})")
    return v


def resolve_ad(requested: str, all_ads: list[str]) -> str:
    """Accept the full AD name or its short form (e.g. "AD-2", case-insensitive)."""
    if requested in all_ads:
        return requested
    short = [ad for ad in all_ads if _ad_short(ad).lower() == requested.strip().lower()]
    if len(short) == 1:
        return short[0]
    sys.exit(f"ERROR: --ad {requested!r} not found; use one of "
             f"{', '.join(_ad_short(a) for a in all_ads)} (or the full AD name)")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="OCI ARM/AMD capacity-estimate report")
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--auth", default=None,
                    help="OCI CLI auth method passed as --auth, e.g. security_token for a "
                         "session-token profile (default: the CLI default or $OCI_CLI_AUTH)")
    ap.add_argument("--region", default=None,
                    help="OCI region, e.g. us-ashburn-1 (default: the profile's region)")
    ap.add_argument("--ad", default=None,
                    help="restrict to ONE availability domain, e.g. AD-2 or the full "
                         "AD name (default: ALL ADs, consolidated)")
    ap.add_argument("--tenancy-ocid", default=None,
                    help="ROOT tenancy OCID (default: from profile)")
    ap.add_argument("--arm-ratio", type=float, default=8.0, help="GB of memory per OCPU, ARM")
    ap.add_argument("--amd-ratio", type=float, default=16.0, help="GB of memory per OCPU, AMD")
    ap.add_argument("--sizes", default=",".join(map(str, DEFAULT_SIZES)))
    ap.add_argument("--ocpus", type=float)
    ap.add_argument("--memory-gb", type=float)
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                    help=f"shape entries per capacity-report request (default {DEFAULT_BATCH_SIZE})")
    ap.add_argument("--detail", action="store_true",
                    help="multi-AD regions: also print per-AD per-family breakdown")
    ap.add_argument("--show-shapes", action="store_true",
                    help="print discovered ARM/AMD shapes + limits per AD and exit")
    a = ap.parse_args(argv)
    global AUTH
    AUTH = a.auth

    if (a.ocpus is None) != (a.memory_gb is None):
        sys.exit("ERROR: --ocpus and --memory-gb must be used together")
    _pos_finite("arm-ratio", a.arm_ratio)
    _pos_finite("amd-ratio", a.amd_ratio)
    if a.ocpus is not None:
        _pos_finite("ocpus", a.ocpus)
        _pos_finite("memory-gb", a.memory_gb)
    if a.batch_size < 1:
        sys.exit("ERROR: --batch-size must be >= 1")
    try:
        sizes = [int(x) for x in a.sizes.split(",")]
    except ValueError:
        sys.exit(f"ERROR: --sizes must be comma-separated integers: {a.sizes!r}")
    if any(s <= 0 for s in sizes):
        sys.exit("ERROR: --sizes must all be positive")

    tenancy = a.tenancy_ocid or _tenancy_from_profile(a.profile)
    region = a.region or _region_from_profile(a.profile)
    ads_data, err = _oci(["iam", "availability-domain", "list",
                          "--compartment-id", tenancy], a.profile, region)
    if ads_data is None:
        sys.exit(f"ERROR listing ADs: {err}")
    all_ads = [x["name"] for x in ads_data.get("data", [])]
    if not all_ads:
        sys.exit(f"ERROR: no ADs in {region}")
    ads = [resolve_ad(a.ad, all_ads)] if a.ad else all_ads

    shapes_by_ad = {ad: discover_shapes(a.profile, region, tenancy, ad) for ad in ads}
    print(f"Region : {region}   ADs : {', '.join(_ad_short(x) for x in ads)}"
          f"{'' if a.ad else ' (all, consolidated)'}")
    print(f"Profile: {a.profile}")

    if a.show_shapes:
        for ad in ads:
            print(f"\n[{_ad_short(ad)}]")
            print(f"{'Shape':<26} {'Arch':<5} {'vCPU/OCPU':>9} {'Flex':<5} {'OCPU':>11} "
                  f"{'Mem GB':>13} {'GB/OCPU':>9}  Processor")
            for s in shapes_by_ad[ad]:
                if s["flexible"]:
                    oc, mg, po = _rng(s["min_oc"], s["max_oc"]), _rng(s["min_mem"], s["max_mem"]), _rng(s["min_per_oc"], s["max_per_oc"])
                else:
                    oc, mg, po = _g(s["ocpus"]), _g(s["mem"]), "fixed"
                print(f"{s['shape']:<26} {s['arch']:<5} {_g(s['vpo']):>9} {str(s['flexible']):<5} "
                      f"{oc:>11} {mg:>13} {po:>9}  {s['proc'][:40]}")
        return 0

    if a.ocpus is not None:
        return fit_check(a.profile, region, tenancy, ads, shapes_by_ad, a.ocpus, a.memory_gb,
                         a.batch_size)
    return sweep(a.profile, region, tenancy, ads, shapes_by_ad,
                 {"ARM": a.arm_ratio, "AMD": a.amd_ratio}, sizes, a.detail, a.batch_size)


if __name__ == "__main__":
    sys.exit(main())
