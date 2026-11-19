"""Which clusters a provision record PROVES this migration created.

`teardown` and the billing report act on that proof and on nothing else. A
key in provision_result.json is not proof: provision records one for every
cluster it touches -- created, adopted with --reuse-existing, name_taken,
and the cluster `compute.warehouse_clusters: existing` maps every warehouse
to. Only `created: true`, written on the create path (or carried forward
from an earlier executed record of the same workspace), is.

A record written before provenance was recorded has no `created` field; its
steps are then the evidence, read the same positive way: a `created` step
for that cluster. Without one, a keyed cluster's provenance is UNKNOWN, not
"not created": the documented `--reuse-existing` plan push records the
migration's OWN cluster as `reused`, so such a record cannot tell this
migration's cluster from somebody else's. Only a `uses_existing` step (the
cluster `compute.warehouse_clusters: existing` maps the warehouses to) still
proves "not created". A later push keeps an unknown cluster unknown
(`provenance_unknown: true`, see provisioning.carry_forward).
"""
from __future__ import annotations

__all__ = ["CREATED", "REQUESTED", "NOT_CREATED", "UNKNOWN",
           "cluster_records"]

CREATED = "created"
REQUESTED = "requested"
NOT_CREATED = "not_created"
UNKNOWN = "unknown"
_EXISTING = "uses_existing"

UNKNOWN_WHY = ("provision_result.json predates provenance (no `created` "
               "field) and does not show whether this migration created this "
               "cluster: a --reuse-existing re-push records the migration's "
               "own cluster as reused. Confirm in the console whether this "
               "migration created it, and stop or delete it there; teardown "
               "did not touch it")


def _legacy(prov: dict, step: str, detail: str | None,
            existing: str | None = None) -> str | None:
    for s in prov.get("steps") or []:
        if s.get("step") != step:
            continue
        text = str(s.get("detail") or "")
        if (existing is not None and s.get("action") == "uses_existing"
                and text.startswith(existing)):
            return _EXISTING
        if detail is not None and not (
                text == detail or text.startswith((detail + " ",
                                                   detail + ":"))):
            continue
        if s.get("action") == "created":
            return CREATED
        if s.get("action") == "create_requested":
            return REQUESTED
    return None


def _verdict(rec: dict, legacy) -> tuple[str, str]:
    if "created" in rec:
        if rec.get("created") is True and rec.get("key"):
            return CREATED, ""
        if rec.get("create_requested") and not rec.get("key"):
            return REQUESTED, ""
        if (rec.get("provenance_unknown") and rec.get("key")
                and not rec.get("uses_existing")):
            return UNKNOWN, UNKNOWN_WHY
    else:
        found = legacy()
        if found == CREATED and rec.get("key"):
            return CREATED, ""
        if found == REQUESTED and not rec.get("key"):
            return REQUESTED, ""
        if (rec.get("key") and found != _EXISTING
                and not rec.get("uses_existing")):
            return UNKNOWN, UNKNOWN_WHY
    if rec.get("uses_existing"):
        return NOT_CREATED, ("the existing cluster "
                             "`compute.warehouse_clusters: existing` maps "
                             "this warehouse to; this migration did not "
                             "create it")
    return NOT_CREATED, ("already on the workspace when provision looked "
                         "(adopted with --reuse-existing, or name taken); "
                         "this migration did not create it")


def cluster_records(prov: dict) -> list[dict]:
    """Every cluster the record names, each with its provenance:
    {cluster, name, role, workspace, datalake_ocid, provenance, why,
    record}.

    `cluster` is the key (None for a create that was accepted but never
    listed). `role` of an earlier push's allocation is the role it was
    recorded with. Nothing here is looked up by name."""
    workspace = (prov.get("workspace") or {}).get("key")
    out = []

    def add(rec: dict, role: str, legacy) -> None:
        provenance, why = _verdict(rec, legacy)
        if provenance == NOT_CREATED and not rec.get("key"):
            return                   # nothing to act on, nothing it holds
        out.append({"cluster": rec.get("key"), "name": rec.get("name"),
                    "role": role,
                    "workspace": rec.get("workspace") or workspace,
                    "datalake_ocid": (rec.get("datalake_ocid")
                                      or prov.get("datalake_ocid")),
                    "provenance": provenance, "why": why, "record": rec})

    cl = prov.get("cluster") or {}
    if cl:
        add(cl, "migration cluster", lambda: _legacy(prov, "cluster", None))
    for wc in prov.get("warehouse_clusters") or []:
        detail = f'{wc.get("warehouse")} -> {wc.get("name")}'
        existing = f'{wc.get("warehouse")} -> existing cluster '
        add(wc, f'warehouse cluster for {wc.get("warehouse")}',
            lambda d=detail, e=existing: _legacy(prov, "warehouse-cluster",
                                                 d, e))
    # What earlier pushes allocated and this one did not re-record (see
    # provisioning.carry_forward), each with its own workspace key.
    for rec in prov.get("earlier_allocations") or []:
        if rec.get("kind", "cluster") == "cluster":
            add(rec, str(rec.get("role") or "cluster"), lambda: None)
    return out
