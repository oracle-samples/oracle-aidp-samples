"""Outbound Snowflake shares -> an AIDP Delta Sharing plan. Never executed.

An outbound share is a live contract: a consumer account queries it today,
and at cutover it keeps reading a source that has stopped moving. The census
names the share (CENSUS.md); this module says what replaces it on AIDP --
Delta Sharing: a share, its data assets, its recipients -- and, as loudly,
what does not map:

  * A RECIPIENT IS NOT A SNOWFLAKE ACCOUNT. Each consumer account becomes one
    Delta Sharing recipient, which receives an activation and reads with a
    Delta Sharing client. Queries the consumer runs against the share in its
    own Snowflake account do not carry; that is a conversation with them.
  * Only a TABLE the plan migrates has an AIDP asset to share. A refused
    object has nothing to share (`no_target`); a migrating view is not a
    verified Delta Sharing data asset (`view_unverified`: materialise it as a
    table to share it); an external or Iceberg table exists on AIDP only once
    it is registered (`register_first`, EXTERNAL_REGISTRATION.md).
  * Snowflake applies masking and row-access policies to what a share
    consumer sees; Delta Sharing ships the table as stored. A shared table
    with a policy exposure (security.json) is HELD (`hold_exposure`) and left
    out of the steps: publishing it hands raw values to another organisation.
    Without security.json every table is held as `hold_unchecked` -- "not
    checked" is not "clean".
  * Live facts: `GET /shares` and `GET /recipients` answer 200 on AIDP
    (`aidp delta-share list` / `list-recipients`). The mutating commands
    (`create`, `manage-data-asset`, `create-recipient`, `manage-access`) are
    named from the CLI's command map; their bodies are NOT verified, so each
    step says what the body must hold and never invents a flag.

Reads Snowflake read-only: one `SHOW SHARES` (account-scoped) and one
`DESCRIBE SHARE` per OUTBOUND share on a database in scope. An inbound share
is listed and not described: it is the provider's data, not ours to publish.
"""
from __future__ import annotations

import datetime
import re
from typing import Callable

from snowflake_source.dialect import lexer

__all__ = ["build_share_plan", "render_share_plan", "LIST_COMMAND"]

LIST_COMMAND = ("aidp delta-share list --instance-id <DATALAKE_OCID> "
                "--auth api_key --region <region>")
LIST_RECIPIENTS_COMMAND = ("aidp delta-share list-recipients --instance-id "
                           "<DATALAKE_OCID> --auth api_key --region <region>")

_CONTAINERS = ("DATABASE", "SCHEMA")

_STATUS_TEXT = {
    "share": "add as a data asset",
    "hold_exposure": "HELD -- carries a policy Delta Sharing does not apply",
    "hold_unchecked": "HELD -- policy exposure not checked (run `security`)",
    "view_unverified": "not added -- a view is not a verified data asset",
    "register_first": "not yet -- register it in place first",
    "no_target": "nothing to share -- not migrating",
}


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _parts(name: str) -> list[str]:
    """`DB.SCHEMA."Mixed.Name"` -> ['DB', 'SCHEMA', 'Mixed.Name']."""
    return [p[1:-1].replace('""', '"') if p.startswith('"') else p
            for p in re.findall(r'"(?:[^"]|"")*"|[^.]+', name or "")]


def _recipient_name(account: str) -> str:
    """ORG.ACCOUNT -> account, in the lower-case charset AIDP names use."""
    base = account.strip().rsplit(".", 1)[-1].lower()
    return re.sub(r"[^a-z0-9_]", "_", base) or "recipient"


def _exposures(security: dict | None, ident: str) -> list[str]:
    return [f'{e.get("policy_kind")} {e.get("policy")}'
            + (f' on {e["column"]}' if e.get("column") else "")
            for e in (security or {}).get("exposures") or []
            if e.get("object") == ident]


def _object(row: dict, can: dict, cannot: dict, security: dict | None) -> dict:
    ident = ".".join(_parts(str(row.get("name") or "")))
    kind = str(row.get("kind") or "").upper()
    entry = {"source_identifier": ident, "kind": kind, "target": None,
             "status": "no_target", "detail": ""}
    if ident in can:
        entry["target"] = can[ident]["target"]
        if can[ident].get("object_type") == "VIEW":
            entry.update(status="view_unverified", detail=(
                "a view on AIDP is not a verified Delta Sharing data asset; "
                "materialise it as a table to share it, or confirm that the "
                "share accepts views"))
        elif security is None:
            entry.update(status="hold_unchecked", detail=(
                "security.json was not read, so whether a masking or "
                "row-access policy protects this table for share consumers "
                "is unknown; run `security` and re-run `share-plan`"))
        else:
            lost = _exposures(security, ident)
            if lost:
                entry.update(status="hold_exposure", detail=(
                    "Snowflake applies " + "; ".join(lost) + " to share "
                    "consumers; Delta Sharing publishes the table as stored, "
                    "so adding it hands raw values to another organisation. "
                    "Share a restricted table or view built for them instead"))
            else:
                entry.update(status="share", detail="migrates; no policy "
                             "exposure recorded in security.json")
    elif ident in cannot:
        c = cannot[ident]
        if c.get("category") == "register_in_place":
            entry.update(status="register_first", detail=(
                "not copied; it exists on AIDP only once registered over OCI "
                "Object Storage (EXTERNAL_REGISTRATION.md) -- share it then"))
        else:
            entry["detail"] = f'not migrating ({c.get("category")}): {c.get("reason")}'
    else:
        entry["detail"] = ("not in the inventory: outside the assessed scope, "
                           "so this plan has no AIDP object for it")
    return entry


def _steps(aidp_share: str, comment: str, objects: list[dict],
           recipients: list[dict]) -> list[dict]:
    assets = [o for o in objects if o["status"] == "share"]
    if not assets:
        return []
    steps = [{"command": "create", "subject": aidp_share,
              "body": f"the share: name `{aidp_share}`"
                      + (f", description \"{comment}\"" if comment else "")}]
    steps += [{"command": "manage-data-asset", "subject": o["target"],
               "body": f"add table `{o['target']}` to share `{aidp_share}`"}
              for o in assets]
    steps += [{"command": "create-recipient", "subject": r["recipient"],
               "body": f"recipient `{r['recipient']}` for consumer account "
                       f"{r['consumer_account']}"} for r in recipients]
    steps += [{"command": "manage-access", "subject": r["recipient"],
               "body": f"grant recipient `{r['recipient']}` access to share "
                       f"`{aidp_share}`"} for r in recipients]
    return steps


def build_share_plan(run_sql: Callable[..., list[dict]], inventory: dict,
                     plan: dict, *, security: dict | None = None) -> dict:
    notes: list[str] = []
    scope = {str(d).upper() for d in inventory.get("databases_in_scope") or []}
    can = {c["source_identifier"]: c for c in plan.get("can_migrate") or []}
    cannot = {c["source_identifier"]: c for c in plan.get("cannot_migrate") or []}
    try:
        rows = list(run_sql("show shares"))
    except Exception as exc:
        notes.append(f"SHOW SHARES: {str(exc)[:200]}")
        rows = []

    shares = []
    for row in sorted(rows, key=lambda r: str(r.get("name"))):
        name = str(row.get("name") or "")
        direction = str(row.get("kind") or "").upper()
        database = str(row.get("database_name") or "")
        share = {"name": name, "direction": direction, "database": database,
                 "comment": row.get("comment") or "",
                 "aidp_share": _recipient_name(name) if direction == "OUTBOUND" else None,
                 "recipients": [], "objects": [], "containers": [],
                 "steps": [], "note": ""}
        if direction != "OUTBOUND":
            share["note"] = ("inbound: a provider's data read by this account; "
                             "nothing here is ours to publish, and whatever "
                             "reads it loses its source at cutover (CENSUS.md)")
            shares.append(share)
            continue
        share["recipients"] = [
            {"consumer_account": a.strip(), "recipient": _recipient_name(a)}
            for a in str(row.get("to") or "").split(",") if a.strip()]
        if database.upper() not in scope:
            share["note"] = (f"on database {database or '?'}, outside the "
                             f"assessed scope: not described, no plan")
            shares.append(share)
            continue
        try:
            described = list(run_sql(f"describe share {lexer.quote_ident(name)}"))
        except Exception as exc:
            share["note"] = f"DESCRIBE SHARE failed: {str(exc)[:200]}"
            notes.append(f"DESCRIBE SHARE {name}: {str(exc)[:200]}")
            shares.append(share)
            continue
        for obj in described:
            kind = str(obj.get("kind") or "").upper()
            if kind in _CONTAINERS:
                share["containers"].append(f'{kind} {obj.get("name")}')
                continue
            share["objects"].append(_object(obj, can, cannot, security))
        share["steps"] = _steps(share["aidp_share"], share["comment"],
                                share["objects"], share["recipients"])
        if not share["steps"]:
            share["note"] = ("nothing in it is shareable as planned yet, so no "
                             "steps are generated; see each object's status")
        shares.append(share)

    return {"generated_at": _now(), "executed": False,
            "security_checked": security is not None,
            "shares": shares,
            "outbound": sum(1 for s in shares if s["direction"] == "OUTBOUND"),
            "verified_commands": [LIST_COMMAND, LIST_RECIPIENTS_COMMAND],
            "unreadable": notes}


def render_share_plan(sp: dict) -> str:
    out = ["# Outbound shares — AIDP Delta Sharing plan", "",
           "> **Nothing here is executed.** This is the plan for replacing each "
           "outbound Snowflake share with an AIDP Delta Sharing share. Every "
           "step publishes data outside the tenancy: confirm scope and "
           "recipient with the data owner before running any of it.", ""]
    shares = sp.get("shares") or []
    if not shares:
        out += ["No share was visible to this role (`SHOW SHARES` returned "
                "none). A share visible only to its owner role is absent, not "
                "proof there is none.", ""]
    out += ["## What does not carry", "",
            "- **A recipient is not a Snowflake account.** Each consumer "
            "account becomes one Delta Sharing recipient, which gets an "
            "activation and reads with a Delta Sharing client. Queries they "
            "run in their own Snowflake account against the share stop "
            "working at cutover.",
            "- **Policies do not travel into a share.** Snowflake masks and "
            "filters for share consumers; Delta Sharing ships the table as "
            "stored. A shared table carrying a policy is **HELD** below and "
            "is in no step.",
            "- Only a table the plan migrates is an asset; a view, a refused "
            "object and a table not yet registered are listed with why.",
            ""]
    if not sp.get("security_checked"):
        out += ["> `security.json` was not read, so every table is HELD as "
                "unchecked. Run `security`, then `share-plan` again.", ""]
    out += ["## Commands", "",
            "Live-verified on AIDP (read-only `GET /shares`, `GET /recipients`):",
            "", "```bash", *(sp.get("verified_commands") or []), "```", "",
            "The steps below name `aidp delta-share <command>` and what its body "
            "must hold. The bodies are **not** live-verified: take the shape "
            "from `aidp delta-share <command> --help`, and persist each body "
            "before running it.", ""]
    for s in shares:
        out += [f'## `{s["name"]}` — {s["direction"].lower() or "?"}'
                + (f' on `{s["database"]}`' if s["database"] else ""), ""]
        if s["direction"] != "OUTBOUND":
            out += [s["note"], ""]
            continue
        out += [f'AIDP share: `{s["aidp_share"]}`', "",
                "| Consumer account | Recipient |", "|---|---|"]
        out += [f'| {r["consumer_account"]} | `{r["recipient"]}` |'
                for r in s["recipients"]] + [""]
        if s["objects"]:
            out += ["| Shared object | Kind | AIDP target | Status | Why |",
                    "|---|---|---|---|---|"]
            out += [f'| `{o["source_identifier"]}` | {o["kind"]} | '
                    + (f'`{o["target"]}`' if o["target"] else "-")
                    + f' | {_STATUS_TEXT.get(o["status"], o["status"])} | '
                    f'{o["detail"]} |' for o in s["objects"]] + [""]
        if s["containers"]:
            out += ["Containers granted to the share (not assets): "
                    + ", ".join(f"`{c}`" for c in s["containers"]), ""]
        if s["steps"]:
            out += ["| # | `aidp delta-share` | Body must hold |", "|---:|---|---|"]
            out += [f'| {i} | `{st["command"]}` | {st["body"]} |'
                    for i, st in enumerate(s["steps"], 1)] + [""]
        if s["note"]:
            out += [s["note"], ""]
    if sp.get("unreadable"):
        out += ["## Could not be read", ""]
        out += [f"- {n}" for n in sp["unreadable"]] + [""]
    return "\n".join(out)
