"""Connectivity and permission smoke test across both ends.

Source needs READ. Destination needs READ and, to be proven, WRITE.

Proving write means actually writing, so the probe creates a clearly-named
schema at the DESTINATION and then removes it again.

The no-DROP rule is a SOURCE guarantee -- nothing is ever written to or dropped
from Snowflake, whatever the credential permits. It does not extend to AIDP,
which is where this plugin legitimately creates objects, so cleaning up its own
probe schema there is correct rather than forbidden. An earlier version applied
the source rule to the destination and therefore could not clean up, which is
why the probe had to stay off.

It is still opt-in, because it writes. The DROP names exactly the one schema
this run created, under a per-run unique name (a failed create permanently
poisons the name, and DELETE does not recover it), and is never CASCADE. If
cleanup fails, the report names what was left.

The probe is also skipped, with a note, when the target catalog is EXTERNAL:
a read-only pointer at the live Snowflake source accepts no writes by design,
and probing it would report FAIL against a destination that works.

Both ends take an injected run_sql, so this is unit-testable with no connection.
Every check captures its own failure: one denied privilege should not hide the
result of the others.

The verdict is three-valued. PASS means both ends were checked and every check
passed. FAIL means a check that ran failed. PARTIAL means no executed check
failed but the destination was never checked -- the target coordinates were
not all supplied -- and that is not a pass: it is the default first run with
the example config, and it used to render as PASS everywhere. `ok` keeps its
narrower meaning, "no executed check failed", so skipped and failed stay the
opposite findings they are.
"""
from __future__ import annotations

import uuid

__all__ = ["PROBE_SCHEMA", "run_smoke", "smoke_verdict"]

PROBE_SCHEMA = "snowmig_permission_probe"


def _probe_schema_name() -> str:
    """A fresh probe name per run.

    A failed create permanently poisons that name in the schema -- verified
    live, and DELETE does not recover it -- so a fixed probe name would be
    unusable ever after the first failure.
    """
    return f"{PROBE_SCHEMA}_{uuid.uuid4().hex[:8]}"


def _like_literal(name: str) -> str:
    """Escape LIKE wildcards. The probe name is full of `_`, which matches any
    single character, so an unescaped pattern is not the name."""
    return (name.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
                .replace("'", "\\'"))

# Excluded when auto-picking a database to probe: their INFORMATION_SCHEMA is
# not representative of the customer's own objects.
_SYSTEM_DBS = frozenset({"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"})


def _check(name: str, fn) -> dict:
    try:
        detail = fn()
        return {"name": name, "ok": True, "detail": detail or "ok"}
    except Exception as exc:
        return {"name": name, "ok": False, "detail": str(exc)[:300]}


def smoke_verdict(result: dict) -> str:
    """PASS | PARTIAL | FAIL for a smoke result, including one written before
    the `verdict` key existed: ok + a skipped destination was never a pass."""
    verdict = result.get("verdict")
    if verdict:
        return verdict
    if not result.get("ok"):
        return "FAIL"
    return "PARTIAL" if (result.get("destination") or {}).get("skipped") else "PASS"


def run_smoke(*, source_run_sql, target=None, dest_call=None,
              write_probe: bool = False, database: str | None = None) -> dict:
    source: dict = {"reachable": False, "checks": []}
    try:
        ident = source_run_sql(
            "select current_user() U, current_account() A, current_region() R, "
            "current_role() ROLE")[0]
        source.update(reachable=True, user=ident.get("U"), account=ident.get("A"),
                      region=ident.get("R"), role=ident.get("ROLE"))
    except Exception as exc:
        source["error"] = str(exc)[:300]
        return {"ok": False, "complete": False, "verdict": "FAIL",
                "source": source,
                "destination": {"skipped": True,
                                "reason": "source is unreachable"}}

    # INFORMATION_SCHEMA must be QUALIFIED: a fresh session has no current
    # database, so an unqualified reference fails with 090105 even for
    # ACCOUNTADMIN. Probe a real database rather than relying on session state.
    def _probe_database() -> str:
        if database:
            return database
        for row in source_run_sql("show databases"):
            if row["name"] not in _SYSTEM_DBS:
                return row["name"]
        raise RuntimeError("no non-system database is visible to this role")

    def _read_information_schema() -> str:
        db = _probe_database()
        count = source_run_sql(
            f'select count(*) N from "{db}".information_schema.tables')[0]["N"]
        return f"{count} table(s) readable in {db}.INFORMATION_SCHEMA"

    source["checks"] = [
        _check("list databases",
               lambda: f'{len(source_run_sql("show databases"))} database(s) visible'),
        _check("read INFORMATION_SCHEMA", _read_information_schema),
    ]

    destination: dict
    if target is None or dest_call is None:
        destination = {
            "skipped": True,
            "reason": ("AIDP target coordinates were not supplied, so the "
                       "destination was not checked"),
            "checks": [], "write_verified": False,
            "write_note": "not verified: no target supplied", "left_behind": []}
    else:
        destination = {"skipped": False, "checks": [], "write_verified": False,
                       "left_behind": [], "catalog": target.catalog,
                       "cluster": target.cluster_id}

        # The catalog's TYPE decides which probes make sense. An EXTERNAL
        # catalog is a registered, read-only pointer at the live Snowflake
        # source: a write probe against it would report FAIL against a
        # destination that works -- exactly the failure class this smoke test
        # exists to prevent. Unresolvable is reported as "unknown", never
        # silently assumed either way.
        catalog_type = None
        try:
            for item in dest_call("list_catalogs").get("items") or []:
                name = str(item.get("displayName") or item.get("key")
                           or "").lower()
                if name == target.catalog.lower():
                    catalog_type = (str(item.get("catalogType") or "").upper()
                                    or None)
                    break
        except Exception:
            catalog_type = None
        destination["catalog_type"] = catalog_type or "unknown"

        # Read: list the catalog's schemas through the CATALOG API. The SQL
        # endpoint returns 404, so using it reported FAIL against a
        # destination that works.
        destination["checks"].append(_check(
            "read target catalog",
            lambda: f'{len(dest_call("list_schemas", catalog=target.catalog).get("items") or [])} '
                    "schema(s) visible"))

        if not write_probe:
            destination["write_note"] = (
                "not verified: the write probe is opt-in because it writes. It "
                "creates one schema and removes it again. Re-run with "
                "--write-probe --execute to prove write access.")
        elif catalog_type == "EXTERNAL":
            destination["write_note"] = (
                f"not applicable: {target.catalog} is an EXTERNAL catalog — a "
                f"registered, read-only pointer at the live Snowflake source. "
                f"It accepts no writes by design, so skipping the probe is "
                f"correct, not a failure. Write access is only meaningful for "
                f"a STANDARD catalog.")
        elif not destination["checks"][0]["ok"]:
            destination["write_note"] = (
                "not attempted: the catalog could not be read, so a write "
                "probe would only restate the same failure.")
        else:
            probe = _probe_schema_name()
            probe_fqn = f"{target.catalog}.{probe}"
            created = False
            try:
                dest_call("create_schema", catalog=target.catalog, schema=probe,
                          body={"displayName": probe,
                                "catalogName": target.catalog,
                                "description": "snowmig write probe"})
                created = True
            except Exception as exc:
                destination["write_note"] = f"not verified: {str(exc)[:200]}"
                destination["checks"].append(
                    {"name": "write probe schema", "ok": False,
                     "detail": str(exc)[:200]})

            if created:
                # Creates are asynchronous and can fail silently, so the call
                # returning is not the claim -- visibility is.
                try:
                    items = dest_call(
                        "list_schemas", catalog=target.catalog).get("items") or []
                    visible = any(str(i.get("key", "")).lower()
                                  .endswith("." + probe.lower()) for i in items)
                except Exception:
                    visible = False

                if visible:
                    destination["write_verified"] = True
                    destination["checks"].append(
                        {"name": "write probe schema", "ok": True,
                         "detail": f"{probe_fqn} created and visible"})
                else:
                    destination["write_note"] = (
                        f"not verified: the create returned but {probe_fqn} is "
                        f"not visible, so write is unproven. Creates are "
                        f"asynchronous, so write access is confirmed only "
                        f"once the created object is read back.")
                    destination["checks"].append(
                        {"name": "write probe schema", "ok": False,
                         "detail": "created without error but not visible"})

                try:
                    dest_call("delete_schema", catalog=target.catalog,
                              schema=probe)
                    if destination["write_verified"]:
                        destination["write_note"] = (
                            f"verified: created {probe_fqn} and cleaned up "
                            f"after itself. Nothing was left behind.")
                except Exception as exc:
                    destination["left_behind"] = [probe_fqn]
                    destination["write_note"] = (
                        f"write probe finished, but cleanup failed "
                        f"({str(exc)[:120]}). {probe_fqn} still exists — "
                        f"remove it manually.")
                    destination["checks"].append(
                        {"name": "write probe cleanup", "ok": False,
                         "detail": str(exc)[:200]})

    all_checks = source["checks"] + destination.get("checks", [])
    ok = bool(all_checks) and all(c["ok"] for c in all_checks)
    complete = not destination["skipped"]
    verdict = "FAIL" if not ok else ("PASS" if complete else "PARTIAL")
    return {"ok": ok, "complete": complete, "verdict": verdict,
            "source": source, "destination": destination}
