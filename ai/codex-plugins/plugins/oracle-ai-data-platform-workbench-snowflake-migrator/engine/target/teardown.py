"""Terminate the compute a migration allocated (the default scope) -- or,
when asked for, remove its credential or everything it created (see
`teardown_everything` below).

Reads provision_result.json, and acts only on the clusters that record
PROVES this migration created (`created: true`, see target/provenance.py):
the migration cluster and any warehouse clusters provision made. A cluster
the record names but did not create -- adopted with --reuse-existing, or
the one `compute.warehouse_clusters: existing` points at -- is listed as
"not this migration's, left alone" and never stopped or deleted. One a record
written before provenance cannot place (see target/provenance.py) is not
touched either, and is a failed step asking for the console. It never
lists the workspace and picks clusters by name, which is how a teardown
takes somebody else's compute with it.

Kept, on purpose: the workspace (scripts, plans, report/output -- the record
of the run), the catalogs (the migration's output) and the jobs (the
registered S11 copy scripts a later data move will run).

`stop` (default) is reversible: a stopped cluster costs nothing and the
registered jobs still point at it. `delete` is final, and the copy jobs then
point at a cluster that no longer exists. Dry run unless execute, and every
cluster is read back until it is stopped or gone -- a 2xx on the action is
not the claim.
"""
from __future__ import annotations

import datetime
import time

from .provenance import (CREATED, NOT_CREATED, REQUESTED, UNKNOWN,
                         cluster_records)

__all__ = ["ACTIONS", "RELEASED_ACTIONS", "SCOPES", "STOPPED_STATES",
           "catalogs_created", "ledger_workspace_rows", "teardown",
           "teardown_everything", "render_teardown"]

ACTIONS = ("stop", "delete")
# One stopped set, shared with the billing report (report/resources.py).
STOPPED_STATES = frozenset({"STOPPED", "INACTIVE", "TERMINATED", "DELETED"})
_STOPPED = STOPPED_STATES
# A VERIFIED step with one of these actions released its cluster: the
# state it was left in, None meaning "the state the step read back".
RELEASED_ACTIONS = {"stopped": None, "already_stopped": None,
                    "deleted": "DELETED", "already_gone": "DELETED"}


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


KEPT = ["the workspace (scripts, plans, report/output — the run's record)",
        "the catalogs (the migration's output)",
        "the jobs (the registered S11 copy scripts)"]


def _targets(prov: dict) -> tuple[list[dict], list[dict], list[dict]]:
    """(targets, left_alone, unresolved). A target is a cluster the record
    proves this migration created -- including those an earlier push
    created, each in its own workspace; everything else it names with a key
    is left alone and said so, once per key. `unresolved` are failed steps,
    never acted on: creates this migration asked for and never saw listed
    (never looked up by name), and keyed clusters whose provenance the
    record cannot tell (it predates provenance)."""
    records = cluster_records(prov)
    targets, left, seen, unknown = [], [], set(), []
    for rec in records:
        at = (rec["workspace"], rec["cluster"])
        if rec["provenance"] == CREATED and at not in seen:
            seen.add(at)
            targets.append({"cluster": rec["cluster"], "name": rec["name"],
                            "role": rec["role"],
                            "workspace": rec["workspace"],
                            "datalake_ocid": rec["datalake_ocid"]})
    for rec in records:
        # Could not tell is neither "ours" nor "somebody else's": not
        # touched, and counted, so the run cannot report success over it.
        at = (rec["workspace"], rec["cluster"])
        if rec["provenance"] == UNKNOWN and at not in seen:
            seen.add(at)
            unknown.append({"cluster": rec["cluster"], "name": rec["name"],
                            "role": rec["role"],
                            "workspace": rec["workspace"],
                            "action": "provenance_unknown", "verified": False,
                            "detail": rec["why"]})
    for rec in records:
        at = (rec["workspace"], rec["cluster"])
        if rec["provenance"] == NOT_CREATED and at not in seen:
            seen.add(at)
            left.append({"cluster": rec["cluster"], "name": rec["name"],
                         "role": rec["role"], "why": rec["why"]})
    unkeyed, named = [], set()
    for rec in records:
        at = (rec["workspace"], rec["name"])
        if rec["provenance"] == REQUESTED and at not in named:
            named.add(at)
            unkeyed.append({
                "cluster": None, "name": rec["name"], "role": rec["role"],
                "workspace": rec["workspace"], "action": "key_unknown",
                "verified": False,
                "detail": f'requested by this migration (the create was '
                          f'accepted), but its key was never recorded. Look '
                          f'it up by name `{rec["name"]}` in workspace '
                          f'{rec["workspace"]} in the console and confirm '
                          f'it is this migration\'s before terminating it; '
                          f'teardown never picks a cluster by name'})
    return targets, left, unknown + unkeyed


def _state(call, workspace: str, key: str):
    items = call("list_clusters", workspace=workspace).get("items") or []
    for item in items:
        if item.get("key") == key or item.get("id") == key:
            return str(item.get("state") or item.get("lifecycleState")
                       or "").upper()
    return None                      # not listed: gone


def teardown(call, prov: dict, *, action: str, execute: bool,
             delays: tuple[float, ...] = (10.0, 20.0, 30.0, 30.0, 60.0,
                                          60.0),
             datalake_ocid: str | None = None) -> dict:
    """Stop or delete what `prov` proves this migration created.

    `datalake_ocid` is the aiDataPlatform this teardown's transport talks
    to. A cluster recorded in ANOTHER one is not looked for here (its
    workspace key means nothing on this platform, and "not listed" would
    read as gone); it is a failed step naming the platform to re-run
    against."""
    if action not in ACTIONS:
        raise ValueError(f"unknown teardown action {action!r}; expected one "
                         f"of {', '.join(ACTIONS)}")
    workspace = (prov.get("workspace") or {}).get("key")
    base = {"dry_run": not execute, "action": action, "workspace": workspace,
            "kept": KEPT, "steps": []}
    if prov.get("dry_run"):
        return {**base, "verified": 0,
                "note": "provision never ran for real, so this migration "
                        "allocated nothing to terminate"}
    targets, left_alone, unkeyed = _targets(prov)
    base["left_alone"] = left_alone
    if not workspace and not targets and not unkeyed:
        # An EXECUTED record naming no workspace is not evidence of an empty
        # migration: its push halted before a key was recorded, and it
        # cannot show what an earlier push allocated.
        return {**base, "verified": 0, "unknown": True,
                "note": "the executed provision record names no workspace "
                        "key (its push halted before one was recorded), so "
                        "this teardown cannot tell what the migration "
                        "allocated; nothing was touched. Check the console "
                        "and PROVISION.md of the push that created the "
                        "environment."}
    if not execute:
        base["steps"] = [{**t, "action": f"would {action}", "verified": None}
                         for t in targets] + unkeyed
        return {**base, "verified": 0, "note": ""}

    for t in targets:
        step = dict(t)
        where = t.get("workspace") or workspace
        if (datalake_ocid and t.get("datalake_ocid")
                and t["datalake_ocid"] != datalake_ocid):
            step.update(action="not_reached", verified=False,
                        detail=f'recorded in aiDataPlatform '
                               f'{t["datalake_ocid"]}, not the one this '
                               f'teardown targets; re-run teardown with '
                               f'--datalake-ocid {t["datalake_ocid"]}')
            base["steps"].append(step)
            continue
        try:
            before = _state(call, where, t["cluster"])
            if action == "stop" and before in _STOPPED:
                step.update(action="already_stopped", verified=True,
                            state=before, at=_now())
                base["steps"].append(step)
                continue
            if before is None:
                step.update(action="already_gone", verified=True, state=None,
                            at=_now())
                base["steps"].append(step)
                continue
            call(f"{action}_cluster", workspace=where, cluster=t["cluster"])
        except Exception as exc:
            # Refused, or never sent: nothing changed as far as we know.
            step.update(action="failed", verified=False,
                        detail=str(exc)[:200])
            base["steps"].append(step)
            continue
        # From here the action WAS accepted. A read-back that errors is
        # "requested, outcome unknown", never "failed": the record must not
        # lose that the delete (or stop) was sent.
        state = before
        done = False
        try:
            for wait in (0.0, *delays):
                time.sleep(wait)
                state = _state(call, where, t["cluster"])
                done = ((state is None) if action == "delete"
                        else (state in _STOPPED))
                if done:
                    break
        except Exception as exc:
            step.update(action=f"{action}_requested", verified=False,
                        state=None, at=_now(),
                        detail=f"{action} sent and accepted; the read-back "
                               f"failed ({str(exc)[:160]}), so its outcome "
                               f"is unknown -- check the console")
            base["steps"].append(step)
            continue
        verb = {"stop": "stopped", "delete": "deleted"}[action]
        # Stamped when it was read back: the billing report's release
        # time, whatever the exit code of the run as a whole.
        step.update(action=verb if done else f"{action}_requested",
                    verified=done, state=state, at=_now())
        base["steps"].append(step)
    # Counted, so the run cannot report full success over them.
    base["steps"] += unkeyed
    return {**base, "note": "", "at": _now(),
            "verified": sum(1 for s in base["steps"] if s["verified"])}


def _render_scoped(res: dict) -> str:
    """TEARDOWN.md for --scope credential | all."""
    title = ("everything this migration created" if res.get("scope") == "all"
             else "the Snowflake credential on the workspace")
    out = [f"# Teardown — {title}", ""]
    if res.get("dry_run"):
        out += ["**DRY RUN — nothing was changed.** Re-run with `--execute` "
                "to delete what is listed below, in this order.", ""]
    if res.get("note"):
        out += [res["note"], ""]
    out += ["| Object | Kind | Action | Verified | Detail |",
            "|---|---|---|---|---|"]
    for s in res.get("steps") or []:
        verified = {True: "yes", False: "**no**", None: "—"}[s.get("verified")]
        name = s.get("name") or s.get("catalog") or s.get("cluster") or "?"
        key = s.get("cluster") or s.get("key")
        label = f"`{name}`" + (f" (`{key}`)" if key and key != name else "")
        detail = " ".join(str(s.get("detail") or s.get("state") or "—")
                          .split()).replace("|", "\\|")
        out.append(f'| {label} | {s.get("kind")} | {s.get("action")} | '
                   f'{verified} | {detail} |')
    if res.get("left_alone"):
        out += ["", "Not this migration's, or not reachable from here — left "
                "alone:", ""]
        out += [f'- `{x.get("name") or x.get("cluster")}` ({x.get("kind")}): '
                f'{x.get("why")}' for x in res["left_alone"]]
    if res.get("kept"):
        out += ["", "Kept:", ""] + [f"- {k}" for k in res["kept"]]
    if res.get("scope") == "all":
        out += ["", "⚠️ Deleting is final. Only what the record proves this "
                "migration created is on this list: a workspace or cluster "
                "adopted with `--reuse-existing`, and a catalog the catalog "
                "stage reused, are never deleted here."]
    return "\n".join(out) + "\n"


def render_teardown(res: dict) -> str:
    if res.get("scope") in ("credential", "all"):
        return _render_scoped(res)
    out = ["# Teardown — the compute this migration allocated", ""]
    if res.get("dry_run"):
        out += [f'**DRY RUN — nothing was changed.** Re-run with `--execute` '
                f'to {res["action"]} the clusters below.', ""]
    if res.get("note"):
        out += [res["note"], ""]
    out += ["| Cluster | Role | Action | Verified | State |",
            "|---|---|---|---|---|"]
    for s in res.get("steps") or []:
        verified = {True: "yes", False: "**no**", None: "—"}[s.get("verified")]
        key = (f'`{s.get("cluster")}`' if s.get("cluster")
               else "key never recorded")
        out.append(f'| `{s.get("name")}` ({key}) | '
                   f'{s.get("role")} | {s.get("action")} | {verified} | '
                   f'{s.get("state") or s.get("detail") or "—"} |')
    if res.get("left_alone"):
        out += ["", "Not this migration's — left alone (named in "
                "provision_result.json, but not created by this migration, so "
                "never stopped or deleted here):", ""]
        out += [f'- `{s.get("name") or s.get("cluster")}` '
                f'(`{s.get("cluster")}`), {s.get("role")}: {s.get("why")}'
                for s in res["left_alone"]]
    out += ["", "Kept:", ""] + [f"- {k}" for k in res.get("kept") or []]
    if res.get("action") == "delete":
        gone = [s for s in res.get("steps") or [] if s.get("verified")
                and s.get("action") in ("deleted", "already_gone")]
        if res.get("dry_run"):
            out += ["", "⚠️ `delete` is final: once run with `--execute`, "
                    "the registered copy jobs will point at a cluster that is "
                    "gone and must be re-bound before a data move. `stop` is "
                    "the reversible choice."]
        elif gone:
            out += ["", "⚠️ `delete` is final: the registered copy jobs bound "
                    "to " + ", ".join(f'`{s.get("name")}`' for s in gone)
                    + " now point at a cluster that no longer exists and "
                    "must be re-bound before a data move."]
    return "\n".join(out) + "\n"


# --- teardown --scope credential | all ---------------------------------------
#
# The default scope (`compute`, above) releases the migration's clusters and
# keeps its output. Two further scopes are opt-in:
#
#   credential  removes only the Snowflake credential `provision
#               --source-config` placed on the workspace -- the copy jobs can
#               no longer read Snowflake afterwards;
#   all         UNDOES the migration: credential, jobs, clusters, the
#               catalogs it created and the workspace, each only where the
#               record proves this migration created it. The INTERNAL
#               catalog holds the migrated tables, so it is deleted only with
#               `include_data`; without it, it is listed as kept.
#
# Every delete is read back (the listing no longer carries it; an async
# operation read to SUCCEEDED first). A 2xx is not the claim, and a delete
# whose outcome could not be read is `delete_requested`, never `deleted`.

SCOPES = ("compute", "credential", "all")

# An async delete (catalog, workspace) measured live at ~75-130 s.
_ASYNC_DELAYS = (5.0, 10.0, 10.0, 15.0, 15.0, 20.0, 30.0, 30.0, 30.0, 60.0,
                 60.0)


def catalogs_created(ledger: list[dict]) -> list[dict]:
    """The catalogs the resource ledger says this migration CREATED (the
    catalog stage records one row per create or reuse), one per key.
    A reused catalog is not this migration's and is never deleted -- but
    once a key is recorded `created`, a later `reused` row does not change
    that: re-running `catalog --execute` finds the catalog this migration
    created and records it reused, which is no proof someone else owns it.
    The same rule report/resources.py follows."""
    by_key: dict[str, dict] = {}
    for row in ledger or []:
        if row.get("kind") != "catalog":
            continue
        key = str(row.get("key") or row.get("name") or "")
        if key and (row.get("action") == "created"
                    or (by_key.get(key) or {}).get("action") != "created"):
            by_key[key] = row
    return [{"catalog": key, "name": row.get("name") or key,
             "type": str(row.get("type") or "").upper()}
            for key, row in by_key.items() if row.get("action") == "created"]


def ledger_workspace_rows(ledger: list[dict], kind: str,
                          workspace: str) -> list[str]:
    """Names the ledger records this migration creating on `workspace`
    (`jobs --register`'s jobs and notebooks), each once, in order."""
    names: list[str] = []
    for row in ledger or []:
        if (row.get("kind") == kind and row.get("workspace") == workspace
                and row.get("action") in ("created", "create_requested")
                and row.get("name") and row["name"] not in names):
            names.append(str(row["name"]))
    return names


def _job_names(prov: dict) -> list[str]:
    """Jobs the record names on this migration's workspace, minus the ones
    a push already deleted. On a workspace this migration CREATED every one
    of them is its own; on a reused workspace only a job a push recorded as
    `created`, or one the carried `created_jobs` record names."""
    ws_created = bool((prov.get("workspace") or {}).get("created"))
    deleted = set(prov.get("deleted_copy_jobs") or [])
    names = []
    for step in prov.get("steps") or []:
        if step.get("step") != "job":
            continue
        name = str(step.get("detail") or "").split(" ", 1)[0].rstrip(":")
        if not name or name in deleted or name in names:
            continue
        if step.get("action") in ("stale_deleted",):
            continue
        if ws_created or step.get("action") == "created":
            names.append(name)
    for job in prov.get("copy_jobs") or []:
        name = job.get("job")
        if name and name not in deleted and name not in names and (
                ws_created or job.get("status") == "created"):
            names.append(name)
    # Created by an earlier push and recorded `reused` by this one: the
    # carried ownership record still names it.
    for job in prov.get("created_jobs") or []:
        name = job.get("name")
        if name and name not in deleted and name not in names:
            names.append(name)
    return names


def _listed(call, operation: str, name: str, **kw) -> dict | None:
    """The listed item carrying `name`, or None. One the listing still
    shows as DELETED is gone; DELETING is not yet."""
    for item in call(operation, **kw).get("items") or []:
        if name in (item.get("key"), item.get("displayName"),
                    item.get("name"), item.get("path")):
            state = str(item.get("lifecycleState") or item.get("state")
                        or "").upper()
            return None if state == "DELETED" else item
    return None


def _async_wait(call, envelope: dict, delays, sleep) -> tuple[str, str | None]:
    """(status, error) of the async operation a delete answered with.
    `NO_KEY` when the envelope carried none (a synchronous delete)."""
    from .provisioning import async_operation_key
    key = async_operation_key(envelope or {})
    if not key:
        return "NO_KEY", None
    last = "IN_PROGRESS"
    for delay in delays:
        sleep(delay)
        op = call("get_async_operation", key=key)
        last = str(op.get("status") or op.get("lifecycleState")
                   or "IN_PROGRESS").upper()
        if last in ("SUCCEEDED", "SUCCESS", "FAILED", "CANCELED",
                    "CANCELLED"):
            err = (f'{op.get("errorCode")}: {op.get("errorMessage")}'
                   if op.get("errorCode") or op.get("errorMessage") else None)
            return last, err
    return last, f"async operation {key} still {last} after the poll budget"


def _gone_after(call, delete, listed, delays, sleep) -> tuple[bool, str]:
    """Send `delete`, follow its async operation if it names one, then read
    the listing back. (verified, detail)."""
    envelope = delete()
    status, err = _async_wait(call, envelope, delays, sleep)
    if status in ("FAILED", "CANCELED", "CANCELLED"):
        return False, f"the delete's async operation ended {status}: {err}"
    for wait in (0.0, 5.0, 10.0, 20.0):
        sleep(wait)
        if listed() is None:
            return True, ("gone from the listing"
                          + (" (async operation SUCCEEDED)"
                             if status in ("SUCCEEDED", "SUCCESS") else ""))
    return False, ("delete sent and accepted, but it is still listed"
                   + (f" ({err})" if err else ""))


def teardown_everything(call, prov: dict, *, scope: str, execute: bool,
                        ledger: list[dict] | None = None,
                        include_data: bool = False,
                        datalake_ocid: str | None = None,
                        delays: tuple[float, ...] = (10.0, 20.0, 30.0, 30.0,
                                                     60.0, 60.0),
                        async_delays: tuple[float, ...] = _ASYNC_DELAYS,
                        sleep=None) -> dict:
    """Remove the credential (`scope="credential"`) or everything this
    migration created (`scope="all"`), in dependency order: credential,
    jobs, clusters, catalogs, workspace. Dry run unless `execute`."""
    if scope not in ("credential", "all"):
        raise ValueError(f"teardown_everything scope {scope!r}; expected "
                         f"credential or all")
    sleep = sleep or time.sleep
    ws = prov.get("workspace") or {}
    ws_key, ws_created = ws.get("key"), bool(ws.get("created"))
    base = {"dry_run": not execute, "scope": scope, "action": "delete",
            "workspace": ws_key, "include_data": include_data,
            "steps": [], "kept": [], "left_alone": []}
    if prov.get("dry_run"):
        return {**base, "verified": 0,
                "note": "provision never ran for real, so this migration "
                        "created nothing to remove"}
    if not ws_key:
        return {**base, "verified": 0, "unknown": True,
                "note": "the executed provision record names no workspace "
                        "key, so this teardown cannot tell what the "
                        "migration created; nothing was touched"}
    if (datalake_ocid and prov.get("datalake_ocid")
            and prov["datalake_ocid"] != datalake_ocid):
        return {**base, "verified": 0, "unknown": True,
                "note": f'the record names aiDataPlatform '
                        f'{prov["datalake_ocid"]}, not the one this '
                        f'teardown targets; re-run with --datalake-ocid '
                        f'{prov["datalake_ocid"]}'}

    # What is to go, in the order it goes.
    plan: list[dict] = []
    credentials = list(dict.fromkeys(
        list(prov.get("credential_objects") or [])
        + list(prov.get("credential_unconfirmed") or [])))
    for path in credentials:
        plan.append({"kind": "credential", "name": path})
    for other in prov.get("earlier_credential_objects") or []:
        base["left_alone"].append({
            "name": other.get("path"), "kind": "credential",
            "why": f'placed on another workspace ({other.get("workspace")}) '
                   f'by an earlier push; remove it there'})
    if scope == "all":
        # The provisioned jobs, then the ones `jobs --register` created
        # (recorded in the ledger, not in provision_result.json).
        job_names = _job_names(prov)
        for name in ledger_workspace_rows(ledger or [], "job", ws_key):
            if name not in job_names:
                job_names.append(name)
        for name in job_names:
            plan.append({"kind": "job", "name": name})
        if not ws_created:
            # On a workspace this migration did not create, its generated
            # notebooks go one by one; on its own, with the workspace.
            for path in ledger_workspace_rows(ledger or [], "ws_object",
                                              ws_key):
                plan.append({"kind": "notebook", "name": path})
        targets, left, unresolved = _targets(prov)
        for t in targets:
            plan.append({"kind": "cluster", **t})
        base["left_alone"] += [{**x, "kind": "cluster"} for x in left]
        for cat in catalogs_created(ledger or []):
            if cat["type"] == "INTERNAL" and not include_data:
                base["kept"].append(
                    f'catalog `{cat["name"]}` (INTERNAL): it holds the '
                    f'migrated tables and their rows; pass --include-data to '
                    f'delete it with them')
                continue
            plan.append({"kind": "catalog", **cat})
        if ws_created:
            plan.append({"kind": "workspace", "name": ws.get("name"),
                         "key": ws_key})
        else:
            base["kept"].append(
                f'workspace `{ws.get("name")}`: this migration did not '
                f'create it (adopted with --reuse-existing), so it and the '
                f'backup-snowflake-migration/ folder stay')
        base["steps"] += [dict(u, kind="cluster") for u in unresolved]
    else:
        base["kept"].append("everything else: the workspace, its jobs, the "
                            "clusters and the catalogs (the copy jobs can no "
                            "longer read Snowflake without the credential)")
    if not credentials:
        base["kept"].append("no credential object is on record (provision "
                            "ran without --source-config)")

    if not execute:
        base["steps"] = [{**p, "action": "would delete", "verified": None}
                         for p in plan] + base["steps"]
        return {**base, "verified": 0, "note": ""}

    done_steps = []
    for p in plan:
        step = dict(p)
        try:
            if p["kind"] in ("credential", "notebook"):
                parent, _, leaf = p["name"].rstrip("/").rpartition("/")
                verified, detail = _gone_after(
                    call,
                    lambda: call("delete_ws_object", workspace=ws_key,
                                 path=p["name"]),
                    lambda: _listed(call, "list_ws_objects", p["name"],
                                    workspace=ws_key, path=parent),
                    (), sleep)
            elif p["kind"] == "job":
                found = _listed(call, "list_jobs", p["name"],
                                workspace=ws_key)
                if found is None:
                    step.update(action="already_gone", verified=True,
                                at=_now())
                    done_steps.append(step)
                    continue
                key = found.get("key") or found.get("id")
                step["key"] = key
                verified, detail = _gone_after(
                    call,
                    lambda: call("delete_job", workspace=ws_key, job_key=key),
                    lambda: _listed(call, "list_jobs", p["name"],
                                    workspace=ws_key),
                    (), sleep)
            elif p["kind"] == "cluster":
                # The same delete-and-read-back as the compute scope; the
                # step keeps `cluster` and `at`, so the billing report
                # releases it like any other deleted cluster.
                where = p.get("workspace") or ws_key
                if (datalake_ocid and p.get("datalake_ocid")
                        and p["datalake_ocid"] != datalake_ocid):
                    step.update(action="not_reached", verified=False,
                                detail=f'recorded in aiDataPlatform '
                                       f'{p["datalake_ocid"]}; re-run with '
                                       f'--datalake-ocid {p["datalake_ocid"]}')
                    done_steps.append(step)
                    continue
                before = _state(call, where, p["cluster"])
                if before is None:
                    step.update(action="already_gone", verified=True,
                                state=None, at=_now())
                    done_steps.append(step)
                    continue
                call("delete_cluster", workspace=where, cluster=p["cluster"])
                state, gone = before, False
                try:
                    for wait in (0.0, *delays):
                        sleep(wait)
                        state = _state(call, where, p["cluster"])
                        if state is None:
                            gone = True
                            break
                except Exception as exc:
                    # Accepted, then unreadable: requested, never "failed".
                    step.update(action="delete_requested", verified=False,
                                state=None, at=_now(),
                                detail=f"delete sent and accepted; the "
                                       f"read-back failed ({str(exc)[:160]})")
                    done_steps.append(step)
                    continue
                step.update(action="deleted" if gone else "delete_requested",
                            verified=gone, state=state, at=_now())
                done_steps.append(step)
                continue
            elif p["kind"] == "catalog":
                verified, detail = _gone_after(
                    call,
                    lambda: call("delete_catalog", catalog=p["catalog"],
                                 forced=p["type"] == "INTERNAL"),
                    lambda: _listed(call, "list_catalogs", p["catalog"]),
                    async_delays, sleep)
            else:  # workspace
                verified, detail = _gone_after(
                    call,
                    lambda: call("delete_workspace", workspace=ws_key),
                    lambda: _listed(call, "list_workspaces", ws_key),
                    async_delays, sleep)
        except Exception as exc:
            step.update(action="failed", verified=False,
                        detail=str(exc)[:200], at=_now())
            done_steps.append(step)
            continue
        step.update(action="deleted" if verified else "delete_requested",
                    verified=verified, detail=detail, at=_now())
        done_steps.append(step)
    base["steps"] = done_steps + base["steps"]
    return {**base, "note": "", "at": _now(),
            "verified": sum(1 for s in base["steps"] if s.get("verified"))}
