"""
AIDP Per-Owner Budget Utility
=============================
Reusable cost-observability + budget-enforcement helpers for AIDP workspaces,
shared by the observability notebook (read-only reporting) and the enforcer
notebook (live pause/stop). Keeps the cost model and budget logic in one place.

Concept (prepaid/allowance model): each owner has a budget cap (e.g. $20,000),
either resetting per calendar month ("monthly") or a fixed depleting allowance
("total"). Spend accrues per owner; when an owner crosses their cap the enforcer
pauses that owner's job schedules and stops the clusters they created so spend
cannot overrun.

Cost model (official AIDP Units mapping):
    AMD   OCPU/hr = 67 units   (amd.* shapes)
    ARM   OCPU/hr = 27 units   (arm.* shapes)
    Intel OCPU/hr = 87 units   (intel.* shapes)
    Memory  GB/hr =  3 units   (all CPU families)
    NVIDIA GPU/hr = 4110 units (per GPU; CPU + memory + block already included)
  cluster_units_hr = driver_units + billed_workers * worker_units, where each
  node's units use its OWN shape family (read from driverShape / workerShape).
  Each run's cost = units_hr * runtime_hrs * price_per_unit_hr, attributed to
  the run launcher (jobRuns.createdByName).

Auth: pass in an OCI API-key signer + the workspace base URL (same pattern as
the rest of aidp_compat — never resource principal).
"""

import datetime
import json
import os

import requests as _http


# ─── Auth + base URL ────────────────────────────────────────────────
# Self-contained, generalized resolution — nothing tenancy-specific is baked
# in. Every value is layered: explicit arg → env var → OCI config profile, and
# region is derived from the lake OCID (it is embedded in the OCID). Auth is
# always the OCI CLI config-file API key — never resource principal (project
# invariant). A hard error is raised only when identity is genuinely nowhere.
def load_oci_config(config_file=None, profile=None):
    """Load an OCI config dict, discovering path/profile the standard way
    (arg → env → SDK default ~/.oci/config)."""
    import oci
    config_file = (config_file or os.environ.get("OCI_CONFIG_FILE")
                   or oci.config.DEFAULT_LOCATION)
    profile = (profile or os.environ.get("OCI_CONFIG_PROFILE")
               or oci.config.DEFAULT_PROFILE)
    return oci.config.from_file(config_file, profile)


def make_signer(config_file=None, profile=None, cfg=None):
    """API-key signer.

    Discovery order:
      1. `cfg` arg (explicit config dict) → file-based signer
      2. On cluster: aidputils.secrets.get(migration_tool_api, key=...) fetch-on-demand
      3. Laptop dev: standard OCI SDK resolution (arg → env → ~/.oci/config)

    Cluster path (2) never touches disk. Env-var overrides (AIDP_CRED_OCI_NAME
    and AIDP_CRED_OCI_KEY_*) let the migrator's kernel bootstrap wire in custom
    credential/key names from cfg.aidp.oci_credentials without editing this file.
    """
    import oci, sys
    # (1) Explicit cfg dict overrides everything — preserves callers that pass one.
    if cfg is not None:
        return oci.signer.Signer(
            tenancy=cfg["tenancy"], user=cfg["user"], fingerprint=cfg["fingerprint"],
            private_key_file_location=cfg["key_file"],
            pass_phrase=oci.config.get_config_value_or_default(cfg, "pass_phrase"))
    # (2) Cluster path — aidputils fetch-on-demand via __main__.
    # AIDP kernel injects `aidputils` as a global on `__main__`. It is NOT
    # installable as a Python module, so `import aidputils` fails; we must
    # look it up via `sys.modules['__main__']` from module scope. See
    # aidp_compat/fs.py::_get_aidputils_from_kernel for the full rationale.
    aidputils = getattr(sys.modules.get("__main__"), "aidputils", None)
    if aidputils is not None:
        cred = os.environ.get("AIDP_CRED_OCI_NAME") or "migration_tool_api"
        def _k(f):
            return os.environ.get("AIDP_CRED_OCI_KEY_" + f.upper()) or {
                "user": "user", "fingerprint": "fingerprint",
                "tenancy": "tenancy", "region": "region",
                "private_key_content": "key_file_content",
            }[f]
        return oci.signer.Signer(
            tenancy=aidputils.secrets.get(cred, key=_k("tenancy")).strip(),
            user=aidputils.secrets.get(cred, key=_k("user")).strip(),
            fingerprint=aidputils.secrets.get(cred, key=_k("fingerprint")).strip(),
            private_key_file_location=None,
            private_key_content=aidputils.secrets.get(cred, key=_k("private_key_content")))
    # (3) Laptop CLI fallback — used by `python3 -m aidp_compat.aidp_budget_enforcer`.
    # Reads ~/.oci/config from the operator's machine. NOT hit on cluster.
    cfg = load_oci_config(config_file, profile)
    return oci.signer.Signer(
        tenancy=cfg["tenancy"], user=cfg["user"], fingerprint=cfg["fingerprint"],
        private_key_file_location=cfg["key_file"],
        pass_phrase=oci.config.get_config_value_or_default(cfg, "pass_phrase"))


def region_from_lake_ocid(lake_ocid):
    """Region is embedded in the lake OCID: ocid1.<res>.<realm>.<region>.<id>
    (segment index 3, e.g. 'ap-mumbai-1'). None if the OCID carries none."""
    parts = (lake_ocid or "").split(".")
    return parts[3] if len(parts) > 3 and parts[3] else None


def resolve_workspace(lake_ocid=None, workspace_id=None, region=None, cfg=None):
    """Resolve (lake_ocid, workspace_id, region) for any tenancy: arg → env →
    OCI profile. Only lake_ocid + workspace_id are required (they are the
    workspace identity); region is derived from the lake OCID. Custom OCI-
    profile keys 'aidp_lake_ocid' / 'aidp_workspace_id' are also honored."""
    if cfg is None:
        try:
            cfg = load_oci_config()
        except Exception:
            cfg = {}
    lake_ocid = (lake_ocid or os.environ.get("AIDP_LAKE_OCID")
                 or cfg.get("aidp_lake_ocid"))
    workspace_id = (workspace_id or os.environ.get("AIDP_WORKSPACE_ID")
                    or cfg.get("aidp_workspace_id"))
    region = (region or region_from_lake_ocid(lake_ocid)
              or os.environ.get("AIDP_REGION") or cfg.get("region"))
    missing = [name for name, val in (
        ("lake OCID ($AIDP_LAKE_OCID or OCI profile 'aidp_lake_ocid')", lake_ocid),
        ("workspace ID ($AIDP_WORKSPACE_ID or OCI profile 'aidp_workspace_id')", workspace_id),
        ("region (not derivable from the given lake OCID; set $AIDP_REGION)", region),
    ) if not val]
    if missing:
        raise ValueError("Could not resolve AIDP workspace — missing: "
                         + "; ".join(missing))
    return lake_ocid, workspace_id, region


def base_url(lake_ocid=None, workspace_id=None, region=None, cfg=None):
    """Workspace base URL for any tenancy, resolved via resolve_workspace()."""
    lake_ocid, workspace_id, region = resolve_workspace(
        lake_ocid, workspace_id, region, cfg)
    return (f"https://aidp.{region}.oci.oraclecloud.com/20240831/"
            f"dataLakes/{lake_ocid}/workspaces/{workspace_id}")


# ─── REST helpers ───────────────────────────────────────────────────
def _get(signer, url, params=None):
    r = _http.get(url, auth=signer, params=params,
                  headers={"Accept": "application/json"}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {url}: {r.text[:200]}")
    return r


def paginate(signer, base, path, params=None):
    params = dict(params or {}); params.setdefault("limit", 25)
    page = None
    while True:
        if page:
            params["page"] = page
        r = _get(signer, f"{base}/{path}", params)
        for item in r.json().get("items", []):
            yield item
        page = r.headers.get("opc-next-page")
        if not page:
            break


def month_key(epoch_ms):
    """YYYY-MM (UTC) for a run start time in epoch-ms."""
    d = datetime.datetime.fromtimestamp((epoch_ms or 0) / 1000.0,
                                        tz=datetime.timezone.utc)
    return f"{d.year:04d}-{d.month:02d}"


# Official AIDP Units per OCPU by CPU shape family (memory always 3 units/GB).
OCPU_UNITS = {"amd": 67, "arm": 27, "intel": 87}
GPU_UNITS_PER_GPU = 4110          # NVIDIA GPU/hr; CPU + memory + block included
DEFAULT_OCPU_FAMILY = "amd"       # fallback when shape string is unrecognized


def _family(shape):
    """Map a shape string like 'amd.generic' / 'intel.gpu' to its unit family."""
    s = (shape or "").lower()
    for fam in OCPU_UNITS:
        if s.startswith(fam):
            return fam
    return DEFAULT_OCPU_FAMILY


def node_units_hr(shape, ocpus, gpus, mem_gb, units_per_gb):
    """Units/hr for a single node, using its own shape family.
       GPU shapes bill per GPU (CPU + memory already bundled in the GPU rate)."""
    if gpus and gpus > 0:
        return GPU_UNITS_PER_GPU * gpus
    return OCPU_UNITS.get(_family(shape), OCPU_UNITS[DEFAULT_OCPU_FAMILY]) * (ocpus or 0) \
        + units_per_gb * (mem_gb or 0)


# ─── Cost model ─────────────────────────────────────────────────────
class CostModel:
    """Shape-aware AIDP-units cost model. units_per_ocpu is kept for backward
    compatibility but is ignored when the cluster's shape family is known."""
    def __init__(self, signer, base, units_per_ocpu=67, units_per_gb=3,
                 worker_count="max", price_per_unit_hr=0.001):
        self.signer, self.base = signer, base
        self.u_ocpu, self.u_gb = units_per_ocpu, units_per_gb   # u_ocpu: legacy fallback only
        self.worker_count, self.price = worker_count, price_per_unit_hr
        self._shape = {}

    def _billed_workers(self, s):
        if self.worker_count == "min": return s["min"]
        if self.worker_count == "avg": return (s["min"] + s["max"]) / 2.0
        return s["max"]

    def cluster_units_hr(self, ckey):
        if ckey not in self._shape:
            if not ckey:
                self._shape[ckey] = None
            else:
                try:
                    c = _get(self.signer, f"{self.base}/clusters/{ckey}").json()
                    dc = c.get("driverConfig") or {}
                    drv = dc.get("driverShapeConfig") or {}
                    wc = c.get("workerConfig") or {}
                    wrk = wc.get("workerShapeConfig") or {}
                    self._shape[ckey] = {
                        "ds": dc.get("driverShape"), "ws": wc.get("workerShape"),
                        "do": drv.get("ocpus", 0) or 0, "dgpu": drv.get("gpus", 0) or 0,
                        "dg": drv.get("memoryInGBs", 0) or 0,
                        "wo": wrk.get("ocpus", 0) or 0, "wgpu": wrk.get("gpus", 0) or 0,
                        "wg": wrk.get("memoryInGBs", 0) or 0,
                        "min": wc.get("minWorkerCount", 0) or 0,
                        "max": wc.get("maxWorkerCount", 0) or 0}
                except RuntimeError:
                    self._shape[ckey] = None
        s = self._shape[ckey]
        if not s:
            return None
        n = self._billed_workers(s)
        u = (node_units_hr(s["ds"], s["do"], s["dgpu"], s["dg"], self.u_gb)
             + n * node_units_hr(s["ws"], s["wo"], s["wgpu"], s["wg"], self.u_gb))
        return u or None


# ─── Collect spend per owner / month ────────────────────────────────
def collect_spend(signer, base, model, max_runs=10000000):
    """Returns (jobs, rows, newest_ms, estimated). rows = per-run dicts:
       {owner, month, cost, job_key, estimated}.

    A run whose cluster shape can't be resolved (deleted cluster / permission
    denied) is NOT dropped and NOT silently zero-costed — that would let an
    over-budget owner escape enforcement. Instead its cost is CONSERVATIVELY
    ESTIMATED: priced at the most expensive shape we *did* resolve for that
    same owner (falling back to the workspace-wide most expensive shape). This
    over-counts rather than under-counts, keeping budget enforcement safe. Such
    rows carry estimated=True and are also returned in `estimated` for review.
    """
    jobs = {}
    for j in paginate(signer, base, "jobs"):
        jobs[j["key"]] = {"name": j.get("name"), "clusters": j.get("clusters", []) or [],
                          "owner": j.get("createdByName")}

    def job_units(jk):
        for ck in jobs.get(jk, {}).get("clusters", []):
            u = model.cluster_units_hr(ck)
            if u:
                return u
        return None

    # Pass 1: pull runs, pricing what we can and remembering the max resolved
    # units/hr per owner + workspace-wide (the conservative fallback basis).
    newest, seen = 0, 0
    priced, unpriced = [], []
    owner_max_units, global_max_units = {}, 0.0
    for run in paginate(signer, base, "jobRuns"):
        end = run.get("endTime") or run.get("startTime") or 0
        newest = max(newest, end)
        seen += 1
        if seen > max_runs:
            break
        owner = run.get("createdByName") or "(unknown)"
        dur = run.get("executionDuration")
        if dur is None and run.get("startTime") and run.get("endTime"):
            dur = run["endTime"] - run["startTime"]
        hours = (dur or 0) / 3600_000.0
        month = month_key(run.get("startTime") or end)
        u = job_units(run.get("jobKey"))
        if u:
            owner_max_units[owner] = max(owner_max_units.get(owner, 0.0), u)
            global_max_units = max(global_max_units, u)
            priced.append((run, owner, month, hours, u))
        else:
            unpriced.append((run, owner, month, hours))

    # Pass 2: emit rows. Unpriced runs get the conservative estimate.
    rows, estimated = [], []
    for run, owner, month, hours, u in priced:
        rows.append({"owner": owner, "month": month, "job_key": run.get("jobKey"),
                     "cost": u * hours * model.price, "estimated": False})
    for run, owner, month, hours in unpriced:
        est_u = owner_max_units.get(owner) or global_max_units
        est_cost = est_u * hours * model.price
        rows.append({"owner": owner, "month": month, "job_key": run.get("jobKey"),
                     "cost": est_cost, "estimated": True})
        estimated.append({"run_key": run.get("key"), "job_key": run.get("jobKey"),
                          "owner": owner, "estimated_units_hr": est_u,
                          "estimated_cost": round(est_cost, 2)})

    if estimated:
        basis = "owner's max resolved shape (else workspace-wide max)"
        print(f"[WARN] {len(estimated)} run(s) had an unresolvable cluster shape "
              f"(deleted / permission denied). Cost CONSERVATIVELY ESTIMATED at "
              f"the {basis} so enforcement is not under-counted:")
        for e in estimated:
            print(f"        - run={e['run_key']} job={e['job_key']} owner={e['owner']} "
                  f"est=${e['estimated_cost']} (@{e['estimated_units_hr']:.0f} units/hr)")
    return jobs, rows, newest, estimated


# ─── Budget evaluation ──────────────────────────────────────────────
def evaluate(rows, budget_cfg, newest_ms):
    """Compare per-owner spend to caps. Returns list of status dicts (no actions)."""
    default_b = budget_cfg.get("default",
                               {"cap_usd": 20000, "period": "monthly", "warn_pct": 80, "enforce": True})
    owner_b = budget_cfg.get("owners", {})
    cur_month = month_key(newest_ms) if newest_ms else None

    total, month = {}, {}
    for r in rows:
        total[r["owner"]] = total.get(r["owner"], 0.0) + r["cost"]
        if r["month"] == cur_month:
            month[r["owner"]] = month.get(r["owner"], 0.0) + r["cost"]

    report = []
    for owner in sorted(set(total) | set(owner_b)):
        b = owner_b.get(owner, default_b)
        period, cap = b.get("period", "monthly"), b.get("cap_usd", 20000)
        warn, enforce = b.get("warn_pct", 80), b.get("enforce", True)
        spent = month.get(owner, 0.0) if period == "monthly" else total.get(owner, 0.0)
        pct = (spent / cap * 100) if cap else 0
        status = "OVER" if spent >= cap else ("WARN" if pct >= warn else "OK")
        report.append({"owner": owner, "period": period, "month": cur_month,
                       "cap_usd": round(cap, 2), "spent_usd": round(spent, 2),
                       "pct": round(pct, 1), "status": status, "enforce": enforce})
    return report


# ─── Enforcement actions (LIVE writes) ──────────────────────────────
def pause_job(signer, base, job_key, dry_run=False):
    full = _get(signer, f"{base}/jobs/{job_key}").json()
    sched = full.get("schedule")
    if not sched or sched.get("pauseStatus") == "PAUSED":
        return "already-paused-or-no-schedule"
    if dry_run:
        return "WOULD-pause"
    sched["pauseStatus"] = "PAUSED"
    r = _http.put(f"{base}/jobs/{job_key}", auth=signer,
                  headers={"Content-Type": "application/json"},
                  json={**full, "schedule": sched}, timeout=60)
    return f"paused({r.status_code})"


def stop_cluster(signer, base, cluster_key, dry_run=False):
    c = _get(signer, f"{base}/clusters/{cluster_key}").json()
    if c.get("state") in ("STOPPED", "STOPPING"):
        return "already-stopped"
    if dry_run:
        return "WOULD-stop"
    r = _http.post(f"{base}/clusters/{cluster_key}/actions/stop", auth=signer,
                   headers={"Content-Type": "application/json"}, timeout=60)
    return f"stop({r.status_code})"


def enforce_owner(signer, base, owner, jobs, dry_run=False):
    """Pause schedules + stop clusters owned by an over-budget owner."""
    actions, clusters = [], set()
    for jk, j in jobs.items():
        if j.get("owner") == owner:
            actions.append(("job", j["name"] or jk, pause_job(signer, base, jk, dry_run)))
            clusters.update(j.get("clusters", []))
    for ck in clusters:
        if ck:
            actions.append(("cluster", ck, stop_cluster(signer, base, ck, dry_run)))
    return actions


# ─── History (/Workspace JSON) for month-on-month ───────────────────
class HistoryCorruptError(RuntimeError):
    """The history file exists but could not be parsed. Raised instead of
    silently discarding prior snapshots (which would let the next save
    overwrite all history with a single data point)."""


def load_history(path):
    """Load snapshot history. Returns an empty history only when the file does
    not exist. If the file exists but is corrupt, raise HistoryCorruptError
    rather than silently returning empty — overwriting real history would
    destroy every prior snapshot enforcement relies on."""
    if not os.path.exists(path):
        return {"snapshots": []}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        raise HistoryCorruptError(
            f"history file {path} exists but could not be parsed ({e}). "
            f"Refusing to overwrite it and lose prior snapshots — move or "
            f"repair the file, then re-run.") from e


def save_snapshot(path, report, newest_ms):
    hist = load_history(path)   # raises on corrupt existing file — no clobber
    hist["snapshots"].append({"month": month_key(newest_ms) if newest_ms else None,
                              "newest_run_ms": newest_ms, "report": report})
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    # Atomic write: temp file in the same dir + os.replace so a crash mid-write
    # can never leave a truncated/corrupt history behind.
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(hist, f, indent=2)
    os.replace(tmp, path)
    return hist
