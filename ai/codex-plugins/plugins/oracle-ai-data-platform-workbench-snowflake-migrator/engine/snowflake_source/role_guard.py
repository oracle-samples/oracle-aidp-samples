"""Prove the session's role is read-only BEFORE anything is discovered.

The transport refuses every non-read verb, so this plugin never writes to
Snowflake. A credential that reaches someone else is not bound by the
transport, though: what it can do is what its ROLE can do. So the first
thing a session does, before discovery or copy planning, is read the grants
of the role it holds and stop if any of them is a write on the source.

The check fails CLOSED. A role whose grants cannot be read, a session with
no current role, or a grants listing cut off at Snowflake's 10,000-row cap
is refused the same as a role that holds INSERT: in each case the plugin
cannot prove the credential is read-only, and it does not guess.

Pure except for `run_sql`, which is injected; a fake cursor tests all of it.
"""
from __future__ import annotations

import json
from typing import Callable

from .conn import AuthError

__all__ = ["RoleNotReadOnly", "WRITE_PRIVILEGES", "NON_SOURCE_CLASSES",
           "SHOW_GRANTS_CAP", "read_role_grants", "assert_role_read_only",
           "describe_role_grants"]

# A privilege that changes or removes a source object, or creates one inside
# it. OWNERSHIP implies every other. Any privilege spelled `CREATE ...` is
# a write too (CREATE TABLE, CREATE SCHEMA, CREATE DYNAMIC TABLE, ...) and
# is matched by prefix rather than listed.
WRITE_PRIVILEGES = frozenset({
    "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
    "OWNERSHIP", "MODIFY", "WRITE", "EVOLVE SCHEMA", "REBUILD",
})

# Object classes a grant can be ON that are not source data. USAGE and
# OPERATE on a WAREHOUSE are what resuming it needs; CREATE DATABASE on the
# ACCOUNT creates something new rather than writing the source. None of
# these is a table, view, schema or database of the estate being migrated.
NON_SOURCE_CLASSES = frozenset({
    "ACCOUNT", "WAREHOUSE", "ROLE", "USER", "INTEGRATION",
    "RESOURCE_MONITOR", "NETWORK_POLICY", "DATABASE_ROLE",
    "APPLICATION_ROLE", "COMPUTE_POOL", "REPLICATION_GROUP",
    "FAILOVER_GROUP", "CONNECTION", "APPLICATION", "APPLICATION_PACKAGE",
    "SHARE", "EXTERNAL_VOLUME",
})

# SHOW GRANTS returns at most this many rows. A listing that long may have
# been cut, so it cannot prove anything.
SHOW_GRANTS_CAP = 10_000


class RoleNotReadOnly(AuthError):
    """The role is not provably read-only on the source. Nothing ran."""


def _lower_keys(row: dict) -> dict:
    return {str(k).lower(): v for k, v in (row or {}).items()}


def _quote_role(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _database_of(name: str) -> str:
    """The first identifier of a dotted object name, unquoted and folded.

    SHOW GRANTS names a table `DB.SCHEMA.TABLE`, a schema `DB.SCHEMA` and a
    database `DB`, quoting any part that needs it. Only the database part
    is wanted here, so a quoted first part is read up to its closing quote
    and kept as written (a quoted identifier IS its case); an unquoted one
    folds to upper case, as Snowflake resolves it. `_unquote` applies the
    same rule to the config's `database:`, so the two compare equal.
    """
    text = str(name or "").strip()
    if text.startswith('"'):
        end, i = -1, 1
        while i < len(text):
            if text[i] == '"':
                if i + 1 < len(text) and text[i + 1] == '"':
                    i += 2
                    continue
                end = i
                break
            i += 1
        return text[1:end].replace('""', '"') if end > 0 else text[1:]
    return text.split(".", 1)[0].upper()


def _is_write(privilege: str) -> bool:
    p = str(privilege or "").strip().upper()
    return p in WRITE_PRIVILEGES or p.startswith("CREATE")


def _unquote(value) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1].replace('""', '"')
    return text.upper()


def _session_roles(run_sql: Callable[..., list]) -> tuple[str, list[str]]:
    rows = run_sql("select current_role() R, current_secondary_roles() S")
    row = _lower_keys(rows[0]) if rows else {}
    role = row.get("r")
    if not role:
        raise RoleNotReadOnly(
            "the session has no current role, so its privileges cannot be "
            "read. Set `role:` in the migration config to a read-only role.")
    secondary: list[str] = []
    raw = row.get("s")
    if raw:
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            names = str((parsed or {}).get("roles") or "")
        except (ValueError, TypeError, AttributeError):
            names = ""
        secondary = [n.strip() for n in names.split(",")
                     if n.strip() and n.strip() != str(role)]
    return str(role), secondary


def read_role_grants(run_sql: Callable[..., list], *,
                     database: str | None = None) -> dict:
    """Every grant the session's roles hold, judged against the source.

    Returns evidence, never a credential:

        role, secondary_roles  -- what the session holds
        grants_read            -- rows read across SHOW GRANTS TO ROLE
        by_privilege           -- {privilege: count} over the source
        write_grants           -- the offending rows, each
                                  {role, privilege, granted_on, name}
        out_of_scope_writes    -- writes on OTHER databases (reported only)
        read_only              -- True when write_grants is empty

    Raises RoleNotReadOnly when the grants cannot be read in full: the
    caller must treat that as a failure, not as a pass.
    """
    role, secondary = _session_roles(run_sql)
    scope = _unquote(database) if database else None
    grants_read = 0
    by_privilege: dict[str, int] = {}
    writes: list[dict] = []
    out_of_scope = 0
    for name in [role, *secondary]:
        try:
            rows = run_sql(f"show grants to role {_quote_role(name)}")
        except Exception as exc:
            raise RoleNotReadOnly(
                f"the grants of role {name} could not be read "
                f"({str(exc)[:200]}). The migration does not run against a "
                f"role whose privileges it cannot prove read-only.") from exc
        if len(rows) >= SHOW_GRANTS_CAP:
            raise RoleNotReadOnly(
                f"role {name} holds {len(rows)} grants, at or over SHOW "
                f"GRANTS' {SHOW_GRANTS_CAP}-row cap; the listing may be "
                f"cut, so it cannot prove the role is read-only. Use a "
                f"narrower role for the migration.")
        grants_read += len(rows)
        for raw in rows:
            row = _lower_keys(raw)
            granted_on = str(row.get("granted_on") or "").upper()
            privilege = str(row.get("privilege") or "").upper()
            obj = str(row.get("name") or "")
            if granted_on in NON_SOURCE_CLASSES:
                continue
            if scope and _database_of(obj) != scope:
                if _is_write(privilege):
                    out_of_scope += 1
                continue
            by_privilege[privilege] = by_privilege.get(privilege, 0) + 1
            if _is_write(privilege):
                writes.append({"role": name, "privilege": privilege,
                               "granted_on": granted_on, "name": obj})
    return {"role": role, "secondary_roles": secondary,
            "database": scope, "grants_read": grants_read,
            "by_privilege": dict(sorted(by_privilege.items())),
            "write_grants": writes, "out_of_scope_writes": out_of_scope,
            "read_only": not writes}


def describe_role_grants(evidence: dict) -> str:
    """One line of evidence for a report or a console. No values."""
    roles = evidence["role"] + (
        f' (+ secondary {", ".join(evidence["secondary_roles"])})'
        if evidence.get("secondary_roles") else "")
    where = (f' on {evidence["database"]}' if evidence.get("database")
             else " on the account's objects")
    held = ", ".join(f"{p} x{n}" for p, n in evidence["by_privilege"].items()
                     ) or "none"
    extra = (f'; {evidence["out_of_scope_writes"]} write grant(s) on other '
             f'databases, outside this migration'
             if evidence.get("out_of_scope_writes") else "")
    if evidence["read_only"]:
        return (f"role {roles} is read-only{where}: "
                f'{evidence["grants_read"]} grant(s) read, held: {held}{extra}')
    shown = [f'{w["privilege"]} on {w["granted_on"]} {w["name"]}'
             for w in evidence["write_grants"][:5]]
    more = len(evidence["write_grants"]) - len(shown)
    return (f"role {roles} holds write privilege(s){where}: "
            + ", ".join(shown) + (f" (+{more} more)" if more > 0 else "")
            + ". The migration role must be read-only: grant it USAGE and "
              "SELECT only, or set `role:` to one that is" + extra)


def assert_role_read_only(run_sql: Callable[..., list], *,
                          database: str | None = None) -> dict:
    """The evidence, or RoleNotReadOnly. Call before the first read."""
    evidence = read_role_grants(run_sql, database=database)
    if not evidence["read_only"]:
        raise RoleNotReadOnly(describe_role_grants(evidence))
    return evidence
