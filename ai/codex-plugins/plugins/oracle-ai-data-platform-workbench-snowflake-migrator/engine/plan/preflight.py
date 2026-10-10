"""Read the connection config back to the user, then test it. Pure + injected.

A migration begins with a config file the user filled in, and the failures
that cost the most are the ones nobody looked at: a host that is the account
locator instead of the account URL, a role that cannot see the database, a
warehouse that is suspended and not resumable, a key path that points at
nothing. Each of those surfaces minutes or hours later as something that
reads like a plugin bug.

So this module does two things, in this order:

  1. **Echo** every field the config carries, with each credential shown as
     its SOURCE -- the file's basename and whether it is owner-only -- never
     as its value or its full path, so the user can confirm or correct it
     before anything runs. The echo is the deliverable even when the tests
     pass. A credential set INLINE is reported as refused, with the `*_path`
     field that replaces it.
  2. **Test** what can be tested cheaply: the source with a read, the
     destination with a list -- and, before any discovery, the ROLE: its
     grants are read back and the check fails if it can write to the source
     (`snowflake_source.role_guard`). Each check captures its own failure so
     one denial does not hide the rest.

Nothing here writes. `run_sql` and `call` are injected, so the whole thing is
unit-tested with no environment.
"""
from __future__ import annotations

from typing import Callable

from migration_config import (
    INLINE_SECRET_ALTERNATIVES, SECRET_FIELDS, SECRET_PATH_FIELDS,
    credential_sources)
from snowflake_source.dialect import lexer
from snowflake_source.role_guard import describe_role_grants, read_role_grants

__all__ = ["REQUIRED_FIELDS", "SECRET_PATH_FIELDS", "SECRET_INLINE_FIELDS",
           "describe_config", "run_preflight", "render_preflight_report"]

REQUIRED_FIELDS = ("account", "user", "warehouse", "database", "auth")

# Fields whose VALUE *would be* the credential. None is accepted: the one
# config file travels, and a secret in it travels with it. Each is reported
# as refused, naming the `*_path` field that replaces it, and its value is
# never rendered. (The tuple is migration_config.SECRET_FIELDS, re-exported
# under the name this module always had.)
SECRET_INLINE_FIELDS = SECRET_FIELDS

# What each field is for, in the words a user needs to confirm it.
_FIELD_HELP = {
    "account": "the Snowflake account identifier, e.g. ORG-ACCOUNT",
    "host": "the Account/Server URL from the Snowflake console; derived from "
            "`account` when absent",
    "user": "the service user the migration reads as",
    "role": "the role whose grants decide what is visible (optional, but a "
            "missing role means the user's default)",
    "warehouse": "the warehouse that runs the reads; it must be resumable",
    "database": "the database being migrated",
    "schema": "any REAL schema, used only to scope the connector's pushdown "
              "session (the connector refuses INFORMATION_SCHEMA there)",
    "auth": "keypair (preferred) or password",
}


def describe_config(config: dict) -> dict:
    """{fields, missing, unknown, secrets} — what the config says, safely.

    Every value is echoed EXCEPT a credential path's content, which is never
    read. A field that is absent is reported as absent rather than defaulted
    silently.
    """
    fields: list[dict] = []
    for name in ("account", "host", "user", "role", "warehouse", "database",
                 "schema", "auth"):
        value = config.get(name)
        fields.append({"field": name,
                       "value": (str(value) if value is not None else None),
                       "purpose": _FIELD_HELP.get(name, ""),
                       "derived": (name == "host" and not value)})

    # Each credential by its SOURCE: the file's basename and whether it is
    # owner-only (`migration_config.credential_sources` does the stat and
    # the mode check; on Windows the note says the check was skipped). The
    # full path is not echoed -- a directory can name a user or a host --
    # and the content is never read here.
    secrets: list[dict] = []
    for source in credential_sources(config):
        secrets.append({"field": source["field"], "source": source["source"],
                        "name": source["name"], "inline": False,
                        "exists": source["ok"],
                        "note": source["protection"] if source["ok"] else
                        f'REFUSED — {source["protection"]}'})
    for name in SECRET_INLINE_FIELDS:
        if not config.get(name):
            continue
        # The value is never touched. It is not accepted either: the check
        # fails and names the field that replaces it.
        secrets.append({"field": name, "source": "inline", "name": None,
                        "inline": True, "exists": False,
                        "note": (f"REFUSED — an inline credential is not "
                                 f"accepted (this file travels). Move the "
                                 f"value to a file readable by you alone "
                                 f"and set "
                                 f"`{INLINE_SECRET_ALTERNATIVES[name]}:` "
                                 f"to it")})

    known = ({f["field"] for f in fields} | set(SECRET_PATH_FIELDS)
             | set(SECRET_INLINE_FIELDS) | {"port"})
    return {
        "fields": fields,
        "secrets": secrets,
        "missing": [f for f in REQUIRED_FIELDS if not config.get(f)],
        "unknown": sorted(k for k in config if k not in known),
        "host_effective": (str(config.get("host")) if config.get("host")
                           else (f'{config.get("account")}'
                                 f'.snowflakecomputing.com'
                                 if config.get("account") else None)),
    }


def _check(name: str, fn) -> dict:
    try:
        return {"name": name, "ok": True, "detail": str(fn() or "ok")}
    except Exception as exc:
        return {"name": name, "ok": False, "detail": str(exc)[:300]}


def run_preflight(config: dict, *, run_sql: Callable[..., list] | None = None,
                  call: Callable[..., dict] | None = None,
                  catalog: str | None = None) -> dict:
    """Echo the config, then test whichever ends were supplied."""
    described = describe_config(config)
    checks: list[dict] = []

    if described["missing"]:
        checks.append({
            "name": "config completeness", "ok": False,
            "detail": "missing required field(s): "
                      + ", ".join(described["missing"])})
    else:
        checks.append({"name": "config completeness", "ok": True,
                       "detail": "every required field is present"})

    for secret in described["secrets"]:
        # An inline credential has no file to find, so calling the check
        # "credential file" would invite the user to go looking for one; it
        # is refused, and the detail says where the value belongs.
        label = ("credential (inline, refused)" if secret.get("inline")
                 else "credential file")
        checks.append({"name": f'{label} ({secret["field"]})',
                       "ok": secret["exists"], "detail": secret["note"]})

    role_grants = None
    if run_sql is not None:
        def identity():
            row = run_sql("select current_user() U, current_role() R, "
                          "current_warehouse() W, current_database() D")[0]
            return (f'user={row.get("U")} role={row.get("R")} '
                    f'warehouse={row.get("W")} database={row.get("D")}')

        checks.append(_check("source identity", identity))

        # BEFORE any discovery: the role's own grants, read back, and a FAIL
        # when it can write to the source -- or when the grants cannot be
        # read at all, which proves nothing and is treated the same.
        def role_read_only():
            nonlocal role_grants
            role_grants = read_role_grants(
                run_sql, database=config.get("database"))
            detail = describe_role_grants(role_grants)
            if not role_grants["read_only"]:
                raise RuntimeError(detail)
            return detail

        checks.append(_check("source role is read-only", role_read_only))

        def visible():
            db = lexer.config_name(config.get("database") or "")
            rows = run_sql(f"show schemas in database {lexer.qualify(db)}")
            return f"{len(rows)} schema(s) visible in {db}"

        checks.append(_check("source database visible", visible))

        def session_schema():
            # `schema:` is NOT a discovery filter -- discovery reads
            # INFORMATION_SCHEMA for the whole database. It is only the real
            # schema the AIDP connector needs to open a pushdown session, and
            # the connector resolves it by FETCHING ITS RELATIONS: a schema
            # that exists but holds nothing comes back as
            # `DATA_ACCESS_LAYER_0031 - Schema: X not found`, which reads as a
            # missing schema rather than an empty one. PUBLIC is the usual
            # default and the usual casualty. Caught here, this costs a
            # second; caught on the cluster it costs a job run.
            db, schema = config.get("database"), config.get("schema")

            def candidates() -> str:
                # DERIVED, never hardcoded: which schemas are populated is a
                # property of the account in front of us, so the suggestion
                # is read from it. A migrator that shipped a default schema
                # name would be guessing about someone else's estate.
                try:
                    rows = run_sql(
                        "select TABLE_SCHEMA S, count(*) N from "
                        f'"{db}".INFORMATION_SCHEMA.TABLES '
                        "where TABLE_SCHEMA <> 'INFORMATION_SCHEMA' "
                        "group by TABLE_SCHEMA order by 2 desc limit 3")
                except Exception:
                    return ""
                usable = [f'{r.get("S")} ({r.get("N")})' for r in rows
                          if int(r.get("N") or 0) > 0]
                return (f". Schemas in {db} that would work: "
                        + ", ".join(usable)) if usable else ""

            if not schema:
                raise RuntimeError(
                    "no `schema:` set. Connector mode needs a REAL, NON-EMPTY "
                    "schema to scope its pushdown session" + candidates())
            # Qualified by database on purpose: the operator's session has no
            # current database (the transport does not `USE` one), so a bare
            # INFORMATION_SCHEMA is `090105 (22000): This session does not
            # have a current database`.
            lit = str(schema).replace("'", "''")
            rows = run_sql(
                f'select count(*) N from "{db}".INFORMATION_SCHEMA.TABLES '
                f"where TABLE_SCHEMA = '{lit}'")
            n = int((rows[0] or {}).get("N") or 0) if rows else 0
            if n == 0:
                raise RuntimeError(
                    f"{schema} holds no relations in {db}, so the AIDP "
                    f"connector will reject it with DATA_ACCESS_LAYER_0031 "
                    f"mid-run. Point `schema:` at a schema that has tables — "
                    f"it scopes the session only, never what is discovered"
                    + candidates())
            return (f"{schema} is a real schema with {n} relation(s); it "
                    f"scopes the connector session only, not the discovery")

        checks.append(_check("connector session schema", session_schema))
    else:
        checks.append({"name": "source", "ok": None,
                       "detail": "not checked: no Snowflake transport was "
                                 "supplied. A skip is not a pass"})

    if call is not None and catalog:
        def catalog_type():
            for item in call("list_catalogs").get("items") or []:
                name = str(item.get("displayName") or item.get("key") or "")
                if name.lower() == catalog.lower():
                    return (f'{name} is '
                            f'{item.get("catalogType") or "UNKNOWN"}')
            raise RuntimeError(f"catalog {catalog} was not found on this "
                               f"DataLake")

        checks.append(_check("destination catalog", catalog_type))
    else:
        checks.append({"name": "destination", "ok": None,
                       "detail": "not checked: no AIDP target was supplied. "
                                 "A skip is not a pass"})

    failed = [c for c in checks if c["ok"] is False]
    skipped = [c for c in checks if c["ok"] is None]
    return {"config": described, "checks": checks,
            # The grants evidence, so the report can show WHAT the role
            # holds, not only that the check passed. No value in it.
            "role_grants": role_grants,
            "ok": not failed,
            "failed": len(failed), "skipped": len(skipped)}


def render_preflight_report(result: dict) -> str:
    cfg = result["config"]
    lines = ["# Preflight — confirm the connection config, then test it", "",
             "**Read this table back to the user and ask them to confirm it.** "
             "A wrong host, role or warehouse here surfaces much later as "
             "something that looks like a tool failure.", "",
             "| Field | Value | What it is |", "|---|---|---|"]
    for field in cfg["fields"]:
        value = field["value"]
        shown = (f'`{value}`' if value else "*(absent)*")
        if field["derived"] and cfg["host_effective"]:
            shown = f'*(derived)* `{cfg["host_effective"]}`'
        lines.append(f'| `{field["field"]}` | {shown} | {field["purpose"]} |')
    lines.append("")

    if cfg["secrets"]:
        lines += ["Credentials are read from these FILES at call time, "
                  "shown by name only; their contents and directories are "
                  "never echoed:", ""]
        for secret in cfg["secrets"]:
            mark = "✅" if secret["exists"] else "❌"
            where = ("*inline* — not accepted" if secret.get("inline")
                     else f'{secret.get("source", "file")} `{secret["name"]}`')
            lines.append(f'- {mark} `{secret["field"]}`: {where} '
                         f'— {secret["note"]}')
        lines.append("")
    else:
        lines += ["⚠️ **No credential in the config at all** — no "
                  "`password_path:`, `key_path:` or `pat_path:`. The auth "
                  "mode below cannot work until one names a file readable "
                  "by you alone. (An inline value is not accepted.)", ""]

    if cfg["missing"]:
        lines += [f'❌ **Missing required field(s):** '
                  f'{", ".join(cfg["missing"])}', ""]
    if cfg["unknown"]:
        lines += [f'⚠️ Unrecognised key(s) in the config, ignored: '
                  f'{", ".join(cfg["unknown"])}', ""]

    lines += ["## Checks", "", "| Check | Result | Detail |", "|---|---|---|"]
    for check in result["checks"]:
        mark = {True: "PASS", False: "**FAIL**", None: "SKIPPED"}[check["ok"]]
        lines.append(f'| {check["name"]} | {mark} | {check["detail"]} |')
    lines += ["",
              f'{result["failed"]} failed · {result["skipped"]} skipped. '
              f'A skipped check is not a pass: say which end was not tested.',
              ""]

    grants = result.get("role_grants")
    if grants:
        # The evidence behind the role check, so the account owner can
        # confirm the role is the one they meant and holds only reads.
        roles = grants["role"] + (
            f' (+ secondary: {", ".join(grants["secondary_roles"])})'
            if grants.get("secondary_roles") else "")
        scope = grants.get("database") or "every database"
        names = "`, `".join(grants.get("inherited_roles") or [])
        walked = (f" and every role granted to it (`{names}`)"
                  if names else "")
        lines += ["## Source role grants", "",
                  f'Role `{roles}`, {grants["grants_read"]} grant(s) read '
                  f'with `SHOW GRANTS TO ROLE` across the role{walked}; '
                  f'privileges on {scope}:', "",
                  "| Privilege | Grants |", "|---|---|"]
        lines += [f"| `{p}` | {n} |" for p, n in grants["by_privilege"].items()]
        if grants["write_grants"]:
            lines += ["", "**Write privileges on the source — the migration "
                          "role must not hold these, directly or through a "
                          "role granted to it:**", ""]
            lines += [f'- `{w["privilege"]}` on {w["granted_on"]} '
                      f'`{w["name"]}` (role `{w["role"]}`'
                      + (f', via `{w["via"]}`' if w.get("via") else "") + ")"
                      for w in grants["write_grants"]]
        if grants.get("out_of_scope_writes"):
            lines += ["", f'{grants["out_of_scope_writes"]} write grant(s) '
                          f'on other databases are outside this migration '
                          f'and were not counted.']
        lines.append("")
    return "\n".join(lines)
