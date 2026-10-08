"""What a migration allocated in AIDP, which phase allocated it, and what bills.

Read from the run's own artifacts, so it can list only what THIS migration
created:

  provision_result.json   workspace, the clusters it proves it created,
                          jobs, uploaded files, and the two catalog names
                          (job parameters: listed apart, never billed)
  resources.jsonl         every catalog the `catalog` stage created or
                          reused, only `created` being an allocation (its
                          result file is overwritten on each run, so it
                          cannot be the record)
  deploy / structure      schemas and tables the structure step verified
  teardown_result.json    what was stopped or deleted, and when

Billing is CLASSIFIED, never priced. AIDP rates depend on the tenancy's
contract; a number not taken from the operator's rate card would read as
authoritative and be wrong. What IS measured is exposure: how long each
cluster ran, times the shape it was provisioned with, as OCPU-hours.
"""
from __future__ import annotations

import datetime
import inspect
import json
import pathlib

from .stages import STAGES
from .tokens import read_stage_log

__all__ = ["BILLING", "build_resources", "record_resource",
           "render_resources_section", "resources_by_phase"]

LEDGER = "resources.jsonl"

# kind -> (billing class, what drives it)
BILLING = {
    "cluster": ("compute while ACTIVE",
                "driver + worker OCPU and memory while the cluster is ACTIVE; "
                "a STOPPED cluster carries no compute charge"),
    "catalog:INTERNAL": ("storage for data held",
                         "managed Delta storage, billed on the data it holds; "
                         "empty tables hold only metadata"),
    "catalog:EXTERNAL": ("no direct charge identified",
                         "a metadata pointer at Snowflake that stores no data; "
                         "its crawls run queries on the Snowflake warehouse, "
                         "which Snowflake bills"),
    "workspace": ("no direct charge identified",
                  "a container for files, jobs and clusters"),
    "job": ("via cluster compute",
            "a definition costs nothing by itself; every run bills as the "
            "compute of the cluster it runs on"),
    "workspace_files": ("storage (negligible)",
                        "scripts, plans and reports in the workspace"),
    "structure": ("storage for data held",
                  "schemas and empty tables: metadata only until rows are "
                  "copied"),
}

# The same stopped set teardown uses, DELETED included: a cluster teardown
# verified gone is not billing.
from target.teardown import RELEASED_ACTIONS, STOPPED_STATES as _STOPPED


def _phase(stage: str) -> str:
    return next((s["phase"] for s in STAGES if s["stage"] == stage), "other")


def _load(out: pathlib.Path, name: str):
    path = out / name
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except ValueError:
        return None


def record_resource(out_dir, **fields) -> None:
    """Append one allocation to the ledger. Never raises."""
    rec = {"at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           **fields}
    try:
        with (pathlib.Path(out_dir) / LEDGER).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")
    except OSError:
        pass


def _ledger(out: pathlib.Path) -> list[dict]:
    path = out / LEDGER
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def _cluster_shape() -> dict:
    """The shape `provision` asks for, read from its own defaults so this
    cannot drift from what was requested."""
    from target.provision_api import build_cluster_body
    p = inspect.signature(build_cluster_body).parameters
    ocpus = p["ocpus"].default
    return {"ocpus": ocpus, "memory_gbs": p["memory_gbs"].default,
            "min_workers": p["min_workers"].default,
            "max_workers": p["max_workers"].default,
            "min_ocpus": ocpus * (1 + p["min_workers"].default),
            "max_ocpus": ocpus * (1 + p["max_workers"].default)}


def _ts(value):
    if not value:
        return None
    t = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=datetime.timezone.utc)


def _res(kind: str, stage: str, **fields) -> dict:
    key = kind if kind != "catalog" else f'catalog:{fields.get("type")}'
    billing, driver = BILLING.get(key, ("unknown", ""))
    return {"kind": kind, "allocated_by": stage, "phase": _phase(stage),
            "billing": billing, "billing_driver": driver, **fields}


def build_resources(out_dir) -> dict:
    out = pathlib.Path(out_dir)
    prov = _load(out, "provision_result.json") or {}
    if not prov or prov.get("dry_run"):
        return {"resources": [], "accruing_now": [], "compute": [],
                "not_allocated": [], "provenance_unknown": [],
                "snowflake_usage": _snowflake_usage(out),
                "note": "provision never ran for real, so this migration "
                        "allocated nothing in AIDP"}

    runs = read_stage_log(out)
    provision_ends = [r["ended_at"] for r in runs
                      if r["stage"] == "provision" and r.get("exit_code") == 0]
    created_at = provision_ends[0] if provision_ends else None

    teardown = _load(out, "teardown_result.json") or {}
    released = {}
    if teardown and not teardown.get("dry_run"):
        # The run that wrote teardown_result.json, WHATEVER its exit code:
        # teardown exits 1 when any one target is unverified, and the ones
        # it did verify are released all the same.
        ends = [r["ended_at"] for r in runs if r["stage"] == "teardown"]
        for step in teardown.get("steps") or []:
            action = step.get("action")
            if (step.get("verified") and step.get("cluster")
                    and action in RELEASED_ACTIONS):
                released[step["cluster"]] = {
                    "state": (RELEASED_ACTIONS[action]
                              or str(step.get("state") or "STOPPED")),
                    "at": (step.get("at") or teardown.get("at")
                           or (ends[-1] if ends else None)),
                    "action": teardown.get("action")}

    shape = _cluster_shape()
    resources: list[dict] = []
    ws = prov.get("workspace") or {}
    if ws.get("key"):
        resources.append(_res("workspace", "provision", name=ws.get("name"),
                              key=ws["key"], state="ACTIVE (kept)"))

    # Only what the record PROVES this migration created: a cluster it
    # adopted, or the existing one a warehouse maps to, is not its
    # allocation and not its bill (target/provenance.py).
    from target.provenance import (CREATED, NOT_CREATED, UNKNOWN,
                                   cluster_records)
    clusters, seen = [], set()
    for rec in cluster_records(prov):
        if rec["provenance"] == CREATED and rec["cluster"] not in seen:
            seen.add(rec["cluster"])
            clusters.append((rec["record"], rec["role"]))
    observed = {r["key"]: r for r in _ledger(out)
                if r.get("kind") == "cluster" and r.get("created_at")}
    for c, role in clusters:
        rel = released.get(c["key"])
        state = (rel["state"] if rel else "ACTIVE (not torn down)")
        # An observed creation time beats the run-log estimate: the first
        # provision run that exited 0 may have been a dry run.
        if c["key"] in observed:
            start = observed[c["key"]]["created_at"]
            source = observed[c["key"]].get("source") or "ledger"
        elif c.get("created_at"):
            start, source = c["created_at"], "provision"
        else:
            start = created_at
            source = "approximate (end of first provision run)"
        hours = None
        if start and not (rel and not rel.get("at")):
            # Released at an unrecorded time: the window is unknown, and
            # "until now" would bill a stopped cluster for nothing.
            end = _ts(rel["at"]) if rel else \
                datetime.datetime.now(datetime.timezone.utc)
            hours = round((end - _ts(start)).total_seconds() / 3600, 2)
        resources.append(_res(
            "cluster", "provision", name=c.get("name"), key=c["key"],
            role=role, state=state,
            shape=(f'driver {shape["ocpus"]} OCPU / {shape["memory_gbs"]} GB + '
                   f'{shape["min_workers"]}-{shape["max_workers"]} workers x '
                   f'{shape["ocpus"]} OCPU / {shape["memory_gbs"]} GB'),
            created_at=start, created_at_source=source,
            released_by="teardown" if rel else None,
            released_at=rel.get("at") if rel else None,
            running_hours=hours,
            ocpu_hours=(round(hours * shape["min_ocpus"], 2),
                        round(hours * shape["max_ocpus"], 2)) if hours else None))

    for step in prov.get("steps") or []:
        if step.get("step") == "job" and step.get("verified"):
            resources.append(_res("job", "provision",
                                  name=str(step["detail"]).split(" ")[0],
                                  key=None, state="defined"))
    files = [s for s in prov.get("steps") or []
             if s.get("step") in ("upload", "notebook") and s.get("verified")]
    if files:
        resources.append(_res("workspace_files", "provision",
                              name="backup-snowflake-migration/", key=None,
                              count=len(files), state="kept"))

    # A catalog is this migration's allocation only on evidence that it
    # CREATED it: a ledger row, or an executed catalog_result, with action
    # `created`. provision's two catalog names are job parameters (it
    # creates no catalog), and a `reused` catalog existed before; both are
    # listed apart, never billed here.
    cats: dict[str, dict] = {}
    others: dict[str, dict] = {}
    for name, ctype in ((prov.get("external_catalog"), "EXTERNAL"),
                        (prov.get("target_catalog"), "INTERNAL")):
        if name:
            others[name] = {"type": ctype,
                            "why": "named in the job parameters by "
                                   "provision, not verified to exist"}
    evidence = []
    last = _load(out, "catalog_result.json") or {}
    if last.get("catalog") and not last.get("dry_run"):
        evidence.append((last["catalog"], last.get("catalog_type") or "?",
                         "catalog", last.get("action")))
    for rec in _ledger(out):
        if rec.get("kind") == "catalog" and rec.get("name"):
            evidence.append((rec["name"], rec.get("type") or "?",
                             rec.get("stage", "catalog"), rec.get("action")))
    for name, ctype, stage, action in evidence:
        if action == "created":
            cats[name] = {"type": ctype, "stage": stage}
        elif action == "reused" and name not in cats:
            others[name] = {"type": ctype,
                            "why": "existed before this migration (the "
                                   "catalog stage reused it)"}
    for name, c in sorted(cats.items()):
        resources.append(_res("catalog", c["stage"], name=name, key=name,
                              type=c["type"], state="ACTIVE (kept)"))
    not_allocated = [{"kind": "catalog", "name": name, "type": c["type"],
                      "why": c["why"]}
                     for name, c in sorted(others.items()) if name not in cats]
    # Neither billed nor "not allocated": a record written before
    # provenance cannot tell (target/provenance.py).
    unknown = []
    for rec in cluster_records(prov):
        if rec["provenance"] == UNKNOWN and rec["cluster"] not in seen:
            seen.add(rec["cluster"])
            unknown.append({"kind": "cluster", "name": rec["name"],
                            "key": rec["cluster"], "why": rec["why"]})
    for rec in cluster_records(prov):
        if rec["provenance"] == NOT_CREATED and rec["cluster"] not in seen:
            seen.add(rec["cluster"])
            not_allocated.append({"kind": "cluster", "name": rec["name"],
                                  "key": rec["cluster"], "why": rec["why"]})

    deployed = _load(out, "deploy_result.json") or {}
    structure = _load(out, "run_snowmig_01_structure.json") or {}
    if deployed and not deployed.get("dry_run") and deployed.get("verified"):
        resources.append(_res("structure", "deploy",
                              name=deployed.get("catalog_in_scope"), key=None,
                              count=deployed.get("verified"),
                              state="created (empty)"))
    elif structure.get("ok"):
        resources.append(_res("structure", "structure-workflow",
                              name=prov.get("target_catalog"), key=None,
                              count=None, state="created (empty)"))

    accruing = [r for r in resources
                if (r["kind"] == "cluster" and r["state"] not in _STOPPED)
                or r["billing"] == "storage for data held"]
    return {"resources": resources, "accruing_now": accruing,
            "not_allocated": not_allocated, "provenance_unknown": unknown,
            "shape": shape, "snowflake_usage": _snowflake_usage(out),
            "note": ""}


def _snowflake_usage(out: pathlib.Path) -> dict:
    """What ran on the SOURCE's warehouse -- billed by Snowflake, not AIDP."""
    inv = _load(out, "inventory.json") or {}
    drivers = []
    if inv.get("row_count_mode") == "exact":
        drivers.append(f'assess --row-counts exact: one COUNT(*) per object '
                       f'({len(inv.get("inventory") or [])} objects)')
    if inv:
        drivers.append("assess, deps, maintenance, security and compute: "
                       "INFORMATION_SCHEMA / ACCOUNT_USAGE / SHOW reads")
    if (out / "run_snowmig_00_discover.json").is_file():
        drivers.append("S6 discovery workflow: INFORMATION_SCHEMA reads from "
                       "the cluster")
    drivers.append("the EXTERNAL catalog's crawls and connection tests")
    return {"warehouse": (inv.get("session") or {}).get("WH"),
            "drivers": drivers}


def resources_by_phase(res: dict) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in res.get("resources") or []:
        out.setdefault(r["phase"], []).append(r)
    return out


def _label(r: dict) -> str:
    name = f'`{r.get("name")}`' if r.get("name") else "—"
    extra = r.get("type") or r.get("role") or (
        f'{r["count"]} item(s)' if r.get("count") else "")
    return f"{name} {extra}".strip()


def render_resources_section(res: dict) -> list[str]:
    out = ["## Allocated resources and billing", ""]
    if not res.get("resources"):
        out += [res.get("note") or "Nothing was allocated in AIDP.", ""]
    else:
        out += ["Everything this migration allocated in AIDP, which phase "
                "allocated it, its state now, and whether it contributes to "
                "billing. **Classified, not priced**: confirm rates against "
                "the tenancy's AIDP rate card.", "",
                "| Resource | Kind | Allocated by (phase) | State now | Billing "
                "| Why |", "|---|---|---|---|---|---|"]
        for r in res["resources"]:
            out.append(f'| {_label(r)} | {r["kind"]} | `{r["allocated_by"]}` '
                       f'({r["phase"]}) | {r["state"]} | **{r["billing"]}** | '
                       f'{r["billing_driver"]} |')
        out.append("")
        for r in (x for x in res["resources"] if x["kind"] == "cluster"):
            if (r.get("running_hours") is None and r["state"] in _STOPPED
                    and r.get("created_at")):
                out.append(
                    f'- **Compute exposure — `{r["name"]}`**: ran from '
                    f'{r["created_at"]} (source: {r.get("created_at_source")}) '
                    f'until teardown left it {r["state"]}, at a time the '
                    f'record does not carry; running hours unknown.')
            if r.get("running_hours") is not None:
                lo, hi = r["ocpu_hours"]
                end = r.get("released_at") or "now (still running)"
                out.append(
                    f'- **Compute exposure — `{r["name"]}`**: ran from '
                    f'{r["created_at"]} (source: {r.get("created_at_source")}) '
                    f'to {end}, **{r["running_hours"]} h**, '
                    f'shape {r["shape"]} ⇒ **{lo}–{hi} OCPU-hours** '
                    f'(workers autoscale between the bounds).')
        others = res.get("not_allocated") or []
        if others:
            out += ["", "**Named or used, but not allocated by this "
                    "migration** (not billed here): " + "; ".join(
                        f'`{r.get("name") or r.get("key")}` {r["kind"]} — '
                        f'{r["why"]}' for r in others)]
        unknown = res.get("provenance_unknown") or []
        if unknown:
            out += ["", "**Provenance unknown** (named in a record written "
                    "before provenance; not billed here, confirm in the "
                    "console): " + "; ".join(
                        f'`{r.get("name") or r.get("key")}` '
                        f'(`{r.get("key")}`) {r["kind"]}' for r in unknown)]
        accruing = res.get("accruing_now") or []
        out += ["", "**Still accruing now:** " + (
            "; ".join(f'{_label(r)} — {r["billing"]}' for r in accruing)
            if accruing else "nothing — every cluster is stopped and no "
                             "data is stored.")]
    sf = res.get("snowflake_usage") or {}
    if sf:
        out += ["", f'**Snowflake side** (billed by Snowflake, on warehouse '
                f'`{sf.get("warehouse") or "?"}`): '
                + "; ".join(sf.get("drivers") or []) + "."]
    out.append("")
    return out
