"""Prove the session's role is read-only BEFORE anything is discovered.

The transport refuses every non-read verb, so this plugin never writes to
Snowflake. A credential that reaches someone else is not bound by the
transport, though: what it can do is what its ROLE can do. So the first
thing a session does, before discovery or copy planning, is read the grants
of the role it holds and stop if any of them is a write on the source.

A role holds more than its own grants. `GRANT ROLE SYSADMIN TO ROLE MIG`
shows up in `SHOW GRANTS TO ROLE MIG` as one row, `USAGE on ROLE SYSADMIN`,
and nothing SYSADMIN itself holds is listed there -- yet MIG can do all of
it. The same goes for a database role (`USAGE on DATABASE_ROLE DB.WRITER`).
So the check WALKS the hierarchy: every role or database role a listing
names is listed in turn, with a visited set so a cycle terminates, and a
write found anywhere in the tree is a write the session can make. Each
offending grant says which inherited role carried it.

The check fails CLOSED. A role whose grants cannot be read, a session with
no current role, a secondary-roles setting whose roles cannot be named, or
a grants listing cut off at Snowflake's 10,000-row cap is refused the same
as a role that holds INSERT: in each case the plugin cannot prove the
credential is read-only, and it does not guess.

Pure except for `run_sql`, which is injected; a fake cursor tests all of it.
"""
from __future__ import annotations

import json
from typing import Callable

from .conn import AuthError

__all__ = ["RoleNotReadOnly", "WRITE_PRIVILEGES", "NON_SOURCE_CLASSES",
           "INHERITED_ROLE_CLASSES", "SHOW_GRANTS_CAP", "read_role_grants",
           "assert_role_read_only", "describe_role_grants"]

# A privilege that changes or removes a source object, or creates one inside
# it. OWNERSHIP implies every other. Any privilege spelled `CREATE ...` is
# a write too (CREATE TABLE, CREATE SCHEMA, CREATE DYNAMIC TABLE, ...) and
# is matched by prefix rather than listed.
WRITE_PRIVILEGES = frozenset({
    "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
    "OWNERSHIP", "MODIFY", "WRITE", "EVOLVE SCHEMA", "REBUILD",
})

# A grant ON one of these is not a privilege on data: it is another role the
# grantee holds, together with everything THAT role holds. Its grants are
# read too (`SHOW GRANTS TO ROLE` / `SHOW GRANTS TO DATABASE ROLE`).
INHERITED_ROLE_CLASSES = {"ROLE": "role", "DATABASE_ROLE": "database role"}

# Object classes a grant can be ON that are not source data. USAGE and
# OPERATE on a WAREHOUSE are what resuming it needs; CREATE DATABASE on the
# ACCOUNT creates something new rather than writing the source. None of
# these is a table, view, schema or database of the estate being migrated.
# ROLE and DATABASE_ROLE are listed for completeness: a grant on one of
# them is followed into that role's grants before this set is consulted.
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


def _split_identifier(name: str) -> list[str]:
    """The parts of a dotted identifier as SHOW GRANTS prints it, unquoted.

    A quoted part is read up to its closing quote (a doubled quote inside
    is one quote) and kept as written; an unquoted part is kept as written
    too, since SHOW GRANTS prints the stored name. The parts are requoted
    one by one to address the object again, so a dot inside a quoted part
    stays inside it.
    """
    text = str(name or "").strip()
    parts: list[str] = []
    buf: list[str] = []
    quoted = False
    i = 0
    while i < len(text):
        ch = text[i]
        if quoted:
            if ch == '"':
                if i + 1 < len(text) and text[i + 1] == '"':
                    buf.append('"')
                    i += 2
                    continue
                quoted = False
            else:
                buf.append(ch)
        elif ch == '"':
            quoted = True
        elif ch == ".":
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf))
    return parts


def _sql_name(listed: str) -> str:
    """A name from a SHOW GRANTS row, requoted part by part for a statement."""
    return ".".join(_quote_role(p) for p in _split_identifier(listed))


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


_SECONDARY_HELP = (
    "Run with --only-primary-role to scope the session to `role:` alone, or "
    "set the user's DEFAULT_SECONDARY_ROLES to none.")


def _roles_granted_to_user(run_sql: Callable[..., list], role: str
                           ) -> list[str]:
    """Every role granted to the session's user, besides `role`.

    `USE SECONDARY ROLES ALL` puts every role granted to the user in effect,
    and `current_secondary_roles()` names them -- except when it does not
    (an empty `roles` next to `value: ALL`). The user's own grants list is
    the authority then; when it cannot be read the session is refused,
    because the roles in effect are unknown.
    """
    try:
        rows = run_sql("select current_user() U")
        user = _lower_keys(rows[0]).get("u") if rows else None
        if not user:
            raise ValueError("the session has no current user")
        listing = run_sql(f"show grants to user {_quote_role(user)}")
    except Exception as exc:
        raise RoleNotReadOnly(
            "secondary roles are active (USE SECONDARY ROLES ALL) but the "
            f"roles in effect could not be read ({str(exc)[:200]}). The "
            "migration does not run on a session whose roles it cannot "
            f"name. {_SECONDARY_HELP}") from exc
    if len(listing) >= SHOW_GRANTS_CAP:
        raise RoleNotReadOnly(
            f"the user holds {len(listing)} role grants, at or over SHOW "
            f"GRANTS' {SHOW_GRANTS_CAP}-row cap; the listing may be cut, "
            f"so the roles in effect are unknown. {_SECONDARY_HELP}")
    names: list[str] = []
    for raw in listing:
        name = str(_lower_keys(raw).get("role") or "").strip()
        if name and name != role and name not in names:
            names.append(name)
    return names


def _session_roles(run_sql: Callable[..., list]) -> tuple[str, list[str]]:
    rows = run_sql("select current_role() R, current_secondary_roles() S")
    row = _lower_keys(rows[0]) if rows else {}
    role = row.get("r")
    if not role:
        raise RoleNotReadOnly(
            "the session has no current role, so its privileges cannot be "
            "read. Set `role:` in the migration config to a read-only role.")
    role = str(role)
    names, mode = "", ""
    raw = row.get("s")
    if raw:
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            names = str((parsed or {}).get("roles") or "")
            mode = str((parsed or {}).get("value") or "").strip().upper()
        except (ValueError, TypeError, AttributeError) as exc:
            raise RoleNotReadOnly(
                "current_secondary_roles() returned a shape this cannot read, "
                "so the roles in effect are unknown and the session is not "
                f"provably read-only. {_SECONDARY_HELP}") from exc
    secondary = []
    for part in names.split(","):
        name = part.strip()
        if name and name != role and name not in secondary:
            secondary.append(name)
    if mode == "ALL" and not secondary:
        # Every role granted to the user is in effect, unnamed. Name them
        # from the user's grants, or stop: an empty list here would pass a
        # session whose other roles were never checked.
        secondary = _roles_granted_to_user(run_sql, role)
    return role, secondary


def _list_grants(run_sql: Callable[..., list], kind: str, sql_name: str,
                 label: str) -> list:
    """One `SHOW GRANTS TO <kind> <name>`, complete or refused."""
    try:
        rows = run_sql(f"show grants to {kind} {sql_name}")
    except Exception as exc:
        raise RoleNotReadOnly(
            f"the grants of {label} could not be read "
            f"({str(exc)[:200]}). The migration does not run against a "
            f"role whose privileges it cannot prove read-only.") from exc
    if len(rows) >= SHOW_GRANTS_CAP:
        raise RoleNotReadOnly(
            f"{label} holds {len(rows)} grants, at or over SHOW GRANTS' "
            f"{SHOW_GRANTS_CAP}-row cap; the listing may be cut, so it "
            f"cannot prove the role is read-only. Use a narrower role for "
            f"the migration.")
    return rows


def read_role_grants(run_sql: Callable[..., list], *,
                     database: str | None = None) -> dict:
    """Every grant the session's roles hold, judged against the source.

    The session's primary and secondary roles are listed with `SHOW GRANTS
    TO ROLE`, and every role or database role a listing names is listed in
    turn, so a write inherited through the role hierarchy counts the same
    as one granted directly.

    Returns evidence, never a credential:

        role, secondary_roles  -- what the session holds
        inherited_roles        -- roles reached through those, as
                                  "role NAME" / "database role DB.NAME"
        grants_read            -- rows read across every SHOW GRANTS
        by_privilege           -- {privilege: count} over the source
        write_grants           -- the offending rows, each
                                  {role, privilege, granted_on, name, via}
                                  (`via` names the inherited role chain
                                  that carried it, None for a direct grant)
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
    inherited: list[str] = []
    out_of_scope = 0
    # (session role it descends from, kind, quoted name, inherited chain,
    #  what to call it in a refusal)
    pending = [(name, "role", _quote_role(name), None, f"role {name}")
               for name in [role, *secondary]]
    seen = {(kind, sql_name) for _, kind, sql_name, _, _ in pending}
    while pending:
        held_by, kind, sql_name, via, label = pending.pop(0)
        rows = _list_grants(run_sql, kind, sql_name, label)
        grants_read += len(rows)
        for raw in rows:
            row = _lower_keys(raw)
            granted_on = str(row.get("granted_on") or "").upper()
            privilege = str(row.get("privilege") or "").upper()
            obj = str(row.get("name") or "")
            if granted_on in INHERITED_ROLE_CLASSES:
                # Another role this one holds: its grants are ours too.
                sub_kind = INHERITED_ROLE_CLASSES[granted_on]
                sub_sql = _sql_name(obj)
                if (sub_kind, sub_sql) not in seen:
                    seen.add((sub_kind, sub_sql))
                    shown = obj if sub_kind == "role" else f"{sub_kind} {obj}"
                    inherited.append(f"{sub_kind} {obj}")
                    pending.append((held_by, sub_kind, sub_sql,
                                    shown if via is None
                                    else f"{via} -> {shown}",
                                    f"{sub_kind} {obj} (granted to role "
                                    f"{held_by})"))
                continue
            if granted_on in NON_SOURCE_CLASSES:
                continue
            # A row with no object name cannot be placed outside the source,
            # so it is judged as inside it.
            if scope and obj and _database_of(obj) != scope:
                if _is_write(privilege):
                    out_of_scope += 1
                continue
            by_privilege[privilege] = by_privilege.get(privilege, 0) + 1
            if _is_write(privilege):
                writes.append({"role": held_by, "privilege": privilege,
                               "granted_on": granted_on, "name": obj,
                               "via": via})
    return {"role": role, "secondary_roles": secondary,
            "inherited_roles": inherited,
            "database": scope, "grants_read": grants_read,
            "by_privilege": dict(sorted(by_privilege.items())),
            "write_grants": writes, "out_of_scope_writes": out_of_scope,
            "read_only": not writes}


def _roles_text(evidence: dict) -> str:
    text = evidence["role"]
    if evidence.get("secondary_roles"):
        text += f' (+ secondary {", ".join(evidence["secondary_roles"])})'
    if evidence.get("inherited_roles"):
        text += f' (+ inherited {", ".join(evidence["inherited_roles"])})'
    return text


def _write_text(w: dict) -> str:
    text = f'{w["privilege"]} on {w["granted_on"]} {w["name"] or "?"}'
    return text + (f' via {w["via"]}' if w.get("via") else "")


def describe_role_grants(evidence: dict) -> str:
    """One line of evidence for a report or a console. No values."""
    roles = _roles_text(evidence)
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
    shown = [_write_text(w) for w in evidence["write_grants"][:5]]
    more = len(evidence["write_grants"]) - len(shown)
    return (f"role {roles} holds write privilege(s){where}: "
            + ", ".join(shown) + (f" (+{more} more)" if more > 0 else "")
            + ". The migration role must be read-only: grant it USAGE and "
              "SELECT only and no role that holds more, or set `role:` to "
              "one that is" + extra)


def assert_role_read_only(run_sql: Callable[..., list], *,
                          database: str | None = None) -> dict:
    """The evidence, or RoleNotReadOnly. Call before the first read."""
    evidence = read_role_grants(run_sql, database=database)
    if not evidence["read_only"]:
        raise RoleNotReadOnly(describe_role_grants(evidence))
    return evidence
