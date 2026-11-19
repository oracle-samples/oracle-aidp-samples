"""Create the AIDP target catalog itself. `call(operation, **kwargs)` injected,
same convention as `catalog_deploy.py`, so every decision here is unit-tested
with no environment.

DEFAULT IS EXTERNAL/SNOWFLAKE, NOT STANDARD. An EXTERNAL catalog is a
registered, read-only pointer at the live Snowflake source -- no managed
storage, no copy, nothing to keep in sync. A STANDARD catalog holds managed
Delta tables, which is real storage with real blast radius, so it is created
only when a caller asks for it by name -- never by default.

What a STANDARD catalog gets here is the CONTAINER and nothing else (runbook
S4). Its schemas and tables are created on AIDP compute by the structure
workflow (S10), never through the control-plane CRUD API: that POST returns
202 Accepted and can create nothing at all, so a table "created" that way is
unverifiable. That narrower refusal is the one that still stands.
"""
from __future__ import annotations

import time
from typing import Callable, Iterable

from .catalog_api import (CATALOG_TYPES, build_catalog_body,
                          normalize_catalog_type)

__all__ = ["RefusedToExecute", "ensure_catalog"]


class RefusedToExecute(RuntimeError):
    """A catalog shape this module will not build was asked for."""


_STANDARD_CONTAINER_ONLY = (
    "the STANDARD catalog CONTAINER was created, and nothing inside it. Its "
    "schemas and tables are created on AIDP compute (a Spark cluster) by the "
    "structure workflow, where each create is read back. Create the structure "
    "at runbook S10 with `snowmig.py run --job snowmig_01_structure` (one "
    "workflow per schema, from the approved ddl_plan.json placed on the "
    "workspace by `snowmig.py provision --execute`). `notebook --upload` is "
    "not the path for this: with `--execute` it is refused.")


def _find_catalog(call, display_name: str) -> dict | None:
    """The catalog as the server holds it, matched case-insensitively.

    A failed LISTING is raised, not swallowed. "We could not look" and "it is
    not there" lead to opposite decisions: swallowing the first one made a
    transient read error read as "absent", and the next step is a create.
    """
    wanted = display_name.strip().lower()
    payload = call("list_catalogs")
    for item in payload.get("items") or []:
        name = str(item.get("displayName") or item.get("key") or "").lower()
        if name == wanted:
            return item
    return None


def _poll_for_catalog(call, display_name: str,
                      delays: tuple[float, ...]) -> dict | None:
    """Read the catalog back until it appears, or the budget runs out.

    Creation is ASYNCHRONOUS and can fail SILENTLY: POST returns 202 Accepted
    with an empty body and no `opc-work-request-id`, so there is no waiter and
    the return value is not the claim -- the object appears seconds later, or
    never. `catalog_deploy.py` learned this the hard way; the same rule applies
    here. Reading back exactly once, immediately, reports every SUCCESSFUL
    create as pending.

    A listing that fails mid-poll is treated as "not visible yet" rather than
    as a failed create, because it is not evidence either way.
    """
    for attempt in range(len(delays) + 1):
        try:
            found = _find_catalog(call, display_name)
        except Exception:
            found = None
        if found is not None:
            return found
        if attempt < len(delays):
            time.sleep(delays[attempt])
    return None


def ensure_catalog(*, display_name: str, call: Callable[..., dict],
                   catalog_type: str = "EXTERNAL",
                   source_type: str | None = "SNOWFLAKE",
                   connection: dict | None = None, description: str = "",
                   properties: dict | None = None,
                   verify_delays: tuple[float, ...] = (3.0, 5.0, 10.0, 15.0),
                   created_here: Iterable[str] = (),
                   reuse_existing: bool = False,
                   ) -> dict:
    """Create `display_name` if absent, or reuse the existing catalog only
    when this migration may.

    EXTERNAL registers a read-only pointer at the source. INTERNAL -- which the
    runbook calls STANDARD, and which is accepted as an alias -- creates the
    managed CONTAINER only (runbook S4) and reports `container_only`, so a
    caller can never read it as "the tables exist"; those are made on compute
    by the structure workflow. Any other shape is refused.

    A catalog already carrying the name (matched case-insensitively, as the
    server does) is reused only when `created_here` -- the keys and names the
    resource ledger records this migration creating -- includes it, or the
    caller passed `reuse_existing`. Otherwise it is refused: the migration
    would write into a catalog it never created. A catalog of the other type
    is refused in every case; an EXTERNAL request "reusing" an INTERNAL
    catalog registers no Snowflake connection at all.
    """
    requested = str(catalog_type or "").strip().upper()
    catalog_type = normalize_catalog_type(catalog_type)
    if catalog_type not in CATALOG_TYPES:
        raise RefusedToExecute(
            f"unknown catalog_type {requested!r}; this module creates only "
            f"{' and '.join(CATALOG_TYPES)} (the runbook's STANDARD is an "
            f"accepted alias for INTERNAL)")
    is_external = catalog_type == "EXTERNAL"

    found = _find_catalog(call, display_name)
    if found is not None:
        listed_name = found.get("displayName") or found.get("key")
        listed_type = str(found.get("catalogType") or "").upper() or "UNKNOWN"
        key = found.get("key") or listed_name
        if listed_type != catalog_type:
            raise RefusedToExecute(
                f"a catalog named {listed_name!r} already exists as "
                f"{listed_type}, not {catalog_type}; it was NOT reused, "
                f"with or without --reuse-existing. Choose another catalog "
                f"name")
        mine = {str(n).lower() for n in created_here if n}
        owned = bool({str(key).lower(), str(listed_name).lower()} & mine)
        if not owned and not reuse_existing:
            raise RefusedToExecute(
                f"a catalog named {listed_name!r} ({listed_type}, key "
                f"{key}) already exists and no record of this migration "
                f"shows it created it; it was NOT reused. Choose another "
                f"catalog name, or pass --reuse-existing if you really "
                f"mean to migrate into it")
        return {"catalog": display_name, "action": "reused",
                "catalog_type": listed_type, "key": key,
                "reused_because": ("created by this migration" if owned
                                   else "--reuse-existing")}

    # A STANDARD catalog carries no sourceType and no connectionDetails: it is
    # managed storage, not a pointer at a source. Passing a Snowflake
    # connection along with one would attach the source credential to a
    # catalog that has no use for it.
    body = build_catalog_body(
        display_name, catalog_type=catalog_type,
        source_type=source_type if is_external else None,
        description=description,
        connection=connection if is_external else None,
        properties=properties)
    call("create_catalog", body=body)

    created = _poll_for_catalog(call, display_name, verify_delays)
    out = {"catalog": display_name,
           "action": "created" if created is not None else "create_requested",
           "catalog_type": catalog_type,
           "key": (created or {}).get("key") or display_name,
           "verified": created is not None}
    if not is_external:
        # Never let a created container read as a created structure.
        out["container_only"] = True
        out["note"] = _STANDARD_CONTAINER_ONLY
    return out
