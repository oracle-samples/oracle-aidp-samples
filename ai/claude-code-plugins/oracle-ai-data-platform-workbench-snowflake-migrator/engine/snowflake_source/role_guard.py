"""Prove the session's role is read-only BEFORE anything is discovered.

The transport refuses every non-read verb, so this plugin never writes to
Snowflake. A credential that reaches someone else is not bound by the
transport, though: what it can do is what its ROLES can do. So the first
thing a session does, before discovery or copy planning, is read the grants
of the roles it holds and stop if any of them is a write on the source --
or can become one.

A role holds more than its own grants. `GRANT ROLE SYSADMIN TO ROLE MIG`
shows up in `SHOW GRANTS TO ROLE MIG` as one row, `USAGE on ROLE SYSADMIN`,
and nothing SYSADMIN itself holds is listed there -- yet MIG can do all of
it. The same goes for a database role (`USAGE on DATABASE_ROLE DB.WRITER`).
So the check WALKS the hierarchy: every role or database role a listing
names is listed in turn, with a visited set so a cycle terminates, and a
write found anywhere in the tree is a write the session can make. Each
offending grant says which inherited role carried it.

Two roles are in every session without any listing naming them, and both
are walked too. PUBLIC is held by every role and never appears as a
`USAGE on ROLE PUBLIC` row, so it is listed on its own for every session.
And every role granted to the session's USER is one `USE ROLE` away for
whoever holds the credential, whatever role the session started with: the
user's grants (`SHOW GRANTS TO USER`) are read and each role they name is
walked like the session's own. A user who holds only the migration role
passes as before; the README asks for exactly that. The listing has to be
one the session could have produced: the user holds the role it is
running as and every secondary role in effect, so a listing that does not
name them is cut or filtered and proves nothing, and a row that names no
role at all (a null or blank `role`, a row without one, a row that is not
a mapping) is refused the same way -- the roles the credential reaches are
unknown. PUBLIC is the one role no user listing names, so a session that
runs as PUBLIC is not asked to find it there.

Not every grant is ON source data, and the ones that are not are judged
too. MANAGE GRANTS on the ACCOUNT lets a role grant itself any privilege,
INSERT on every table included; OWNERSHIP of a role or a user lets it take
over what they hold; CREATE ROLE, CREATE USER, EXECUTE TASK, the APPLY ...
POLICY privileges, CREATE SHARE and CREATE INTEGRATION each widen what the
credential can do or where the data can go. A grant on such a class is
therefore judged against a per-class ALLOWLIST of privileges that cannot
lead to a source write (`NON_SOURCE_PRIVILEGES`: USAGE and OPERATE on a
warehouse, USAGE on an integration, MONITOR USAGE on the account, ...), and
anything else on it is refused with the escalation named. On a source
class a privilege is a known read (`READ_PRIVILEGES`), a known write, or
unknown -- and unknown is refused as well, on every class. The check
cannot prove a privilege it does not know is harmless.

A policy or a tag is judged wherever it lives. A masking policy in another
database can be applied to the source's columns, and whoever owns it can
change its body and with it what those columns show; the same holds for a
row access, aggregation or projection policy, a tag, an authentication
policy or a network rule. So on these classes (`GOVERNANCE_CLASSES`)
anything beyond a read is a write on the source, in whatever database the
object sits.

The `database:` of the migration scopes the check to the estate being
read: a write on a table of ANOTHER database is reported, not refused,
because it is not a write on the source. That carve-out applies only to a
grant the check can PLACE -- a database-contained source class
(`SCOPED_SOURCE_CLASSES`) whose object name carries the database part --
and only to a privilege whose reach is BOUNDED by that object: INSERT or
OWNERSHIP on a table of another database is a write on that table and
nothing more. Two kinds of privilege reach past their object and are
refused in whatever class and database a row reports them
(`_reaches_past`). One is an escalation -- a privilege named in
`ESCALATIONS`, or any `MANAGE ...`, `APPLY ...` or `EXECUTE ...`
privilege: MANAGE GRANTS reported ON a database still lets the role grant
itself anything. The other is the privilege that RUNS an owner's-rights
object (`EXECUTABLE_CLASSES`): a task, pipe, alert, dynamic table or
service runs its body with its OWNER's privileges, not the caller's, and
OPERATE on one is what resumes, refreshes or executes it; a procedure,
notebook or Streamlit app runs the same way and USAGE is what calls it.
What such a body does is set by its owner's grants, which this check
never reads, so a task in another database owned by a privileged role
whose body writes the source is a write the session can trigger: OPERATE
on it is refused wherever the task lives, and USAGE of a procedure
OUTSIDE the migration's database is refused too (inside it, discovery
needs it; see the allowances below). OWNERSHIP of a task or procedure in
another database stays a reported write: an object the role owns runs
with the role's own privileges, which this check is proving read-only. A
refused privilege on any other class, or on a row
whose class or name is blank or malformed, is a refusal whatever the
scope too: an escalation on the account, the organisation, a listing or a
class Snowflake adds tomorrow is never "another database".

The scope is exactly the databases discovery will read: the config's
`database:`, or every database a repeated `--database` names, each resolved
the way discovery resolves it (`lexer.config_name`: `sales_db` is SALES_DB,
`'"MyDb"'` is MyDb, `my-db` is `my-db` as written, which SHOW GRANTS quotes
the same way). Any other shape -- an empty list, a member that is not a
name -- refuses the session (`_scope_of`): a scope read from the wrong
thing would place every real database "outside the migration".

Placing a grant is strict, because a row that is excused is a row that is
not judged. The name must have exactly the parts SHOW GRANTS prints for
its class -- DB for a database, DB.SCHEMA for a schema, DB.SCHEMA.OBJECT
for everything else (a future row names its container, DB.<TABLE> or
DB.SCHEMA.<TABLE>, so two or three) -- and every unquoted part before the
object part must be an identifier: an unquoted identifier cannot contain
whitespace, so `SALES_DB .PUBLIC.ORDERS` or a two-part table name on a
current grant is a malformed row, judged INSIDE the scope. An unquoted
database part is compared with its whitespace removed and folded to upper
case, a quoted one as written. Class and privilege names are read with the
whitespace SHOW GRANTS never prints removed, so a padded or null column is
not a loophole either: a blank class refuses the row, a role grant whose
name is blank, has a blank part or does not have the parts its class
prints (one for a role, DB.NAME for a database role) is refused rather
than listed as `""`, and a role-like class that is not exactly ROLE or
DATABASE_ROLE (an application or instance role) is refused unwalked.

A system role (ACCOUNTADMIN, SECURITYADMIN, USERADMIN, SYSADMIN, ORGADMIN,
GLOBALORGADMIN) anywhere in the walked hierarchy, or granted to the user,
is refused without listing it: what those roles can do is set by
Snowflake, not by the rows `SHOW GRANTS` returns for them. The name is
compared with the STORED name, case-sensitively: `current_role()`, the
user listing and `SHOW GRANTS` all print a role's stored name unquoted,
and Snowflake stores an unquoted identifier in upper case, so a role named
exactly SYSADMIN is the system role and a distinct role created as
`"sysadmin"` (stored in lower case) is not -- it is walked and judged by
its grants like any other role. A quoted name, should a listing ever print
one, is read as its stored text.

Future grants are read too (`SHOW FUTURE GRANTS TO ROLE`, and `TO DATABASE
ROLE` for a database role). A future INSERT on a schema is an INSERT on
every table created there later -- a write the session can make -- and is
judged by the same rules as a grant on an existing object.

Two allowances are deliberate and worth knowing. CREATE DATABASE on the
account passes: a database the role creates -- empty, or a zero-copy
CLONE of the source -- is the role's own and never a write on the source,
and discovery is scoped to `database:`, so nothing in it is read; a clone
is still a copy of the estate under the role's control, which is why the
README asks the account owner not to grant it. USAGE on a PROCEDURE in the
migration's database (or with no `database:` set) passes because discovery
reads procedure DDL and needs it; a procedure that runs with its owner's
rights would let a leaked credential CALL a write the owner can make, so
the README asks that the migration role be granted no procedures it does
not need. Discovery never reads a procedure in another database, so
USAGE of one there is refused: nothing needs it, and its owner may write
the source.

The check fails CLOSED. A role whose grants or future grants cannot be
read, a session with no current role, a secondary-roles setting that is
missing or whose roles cannot be named, a user whose roles cannot be
listed or whose listing names no role or omits one the session holds, a
grants row that is not a mapping, or a listing cut off at Snowflake's
10,000-row cap is refused the same as a role that holds INSERT: in each
case the plugin cannot prove the credential is read-only, and it does not
guess.

Pure except for `run_sql`, which is injected; a fake cursor tests all of it.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Callable

from .conn import AuthError
from .dialect import lexer

__all__ = ["RoleNotReadOnly", "READ_PRIVILEGES", "WRITE_PRIVILEGES",
           "NON_SOURCE_PRIVILEGES", "NON_SOURCE_CLASSES", "ESCALATIONS",
           "GOVERNANCE_CLASSES", "SCOPED_SOURCE_CLASSES",
           "EXECUTABLE_CLASSES", "SYSTEM_ROLES",
           "INHERITED_ROLE_CLASSES", "PUBLIC_ROLE", "SHOW_GRANTS_CAP",
           "read_role_grants", "assert_role_read_only",
           "describe_role_grants"]

# The privileges on a source object (database, schema, table, view, stage,
# function, ...) that only read it or make it addressable. Anything else on
# a source class is refused: a known write by name, an unknown privilege as
# unproven. REFERENCE_USAGE lets a secure view reference the object;
# IMPORTED PRIVILEGES on a shared database confers what the share grants,
# and a share is read-only.
READ_PRIVILEGES = frozenset({
    "USAGE", "SELECT", "REFERENCES", "MONITOR", "READ", "REFERENCE_USAGE",
    "IMPORTED PRIVILEGES",
})

# A privilege that changes or removes a source object, or creates one inside
# it. OWNERSHIP implies every other. Any privilege spelled `CREATE ...` is
# a write too (CREATE TABLE, CREATE SCHEMA, CREATE DYNAMIC TABLE, ...) and
# is matched by prefix rather than listed. OPERATE on a task, pipe, alert
# or dynamic table starts or refreshes something that writes; APPLY on a
# policy or tag changes what a source object shows. Both are reads only on
# a warehouse, which the class allowlist below says.
WRITE_PRIVILEGES = frozenset({
    "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
    "OWNERSHIP", "MODIFY", "WRITE", "EVOLVE SCHEMA", "REBUILD", "OPERATE",
    "APPLY",
})

# A grant ON one of these is not a privilege on data: it is another role the
# grantee holds, together with everything THAT role holds. Its grants are
# read too (`SHOW GRANTS TO ROLE` / `SHOW GRANTS TO DATABASE ROLE`). Any
# other class whose name says ROLE (APPLICATION_ROLE, INSTANCE_ROLE, ...)
# is a role this check does not walk, and USAGE of it is refused unproven.
INHERITED_ROLE_CLASSES = {"ROLE": "role", "DATABASE_ROLE": "database role"}

# Held by every role in the account and never listed as a `USAGE on ROLE`
# row, so it is walked on its own for every session.
PUBLIC_ROLE = "PUBLIC"

# Object classes a grant can be ON that are not source data, each with the
# privileges on it that cannot lead to a write on the source. A privilege
# on one of these classes that is NOT listed is refused, whatever it is:
# the escalations in `ESCALATIONS` with their reason, anything else as
# unknown. None of these classes is a table, view, schema or database of
# the estate being migrated, so a grant on one is not counted among the
# source privileges either, and the `database:` scope never places one
# outside the migration.
#
#   WAREHOUSE    USAGE and OPERATE are what resuming the warehouse needs;
#                MONITOR reads its history; MODIFY changes its size, which
#                is the warehouse's business, not the source's.
#   INTEGRATION  USAGE of an existing integration (an external stage or
#                the key-pair/SSO integration the session authenticates
#                through); creating one is refused below.
#   ACCOUNT      MONITOR USAGE / EXECUTION / SECURITY read account history.
#                IMPORTED PRIVILEGES reads what another account shared.
#                CREATE DATABASE and CREATE WAREHOUSE are kept because
#                neither writes the source: a database the role creates is
#                the role's own, and discovery is scoped to `database:` so
#                nothing in it is read. Note that `CREATE DATABASE ... CLONE
#                <source>` needs only this and USAGE on the source, and
#                yields a writable copy of the estate under the role's
#                control -- not a write on the source, so not refused here,
#                but a reason for the account owner to withhold it (the
#                README says so). MANAGE GRANTS is NOT here: with it the
#                role grants itself anything.
#   USER         MONITOR reads that user's query history and nothing else.
#   the rest     USAGE or MONITOR of an existing resource monitor, network
#                policy, compute pool, connection, replication or failover
#                group, external volume, application or share.
#   ORGANIZATION nothing: every privilege on it is administrative.
#   ROLE, DATABASE_ROLE, APPLICATION_ROLE
#                nothing. USAGE on a ROLE or DATABASE_ROLE is the inherited
#                role itself and is followed before this table is read;
#                OWNERSHIP of a role or user is an escalation; an
#                application role's grants are not walked, so USAGE of one
#                is refused unproven.
_USAGE_MONITOR = frozenset({"USAGE", "MONITOR"})
NON_SOURCE_PRIVILEGES: dict[str, frozenset] = {
    "WAREHOUSE": frozenset({"USAGE", "OPERATE", "MONITOR", "MODIFY"}),
    "INTEGRATION": frozenset({"USAGE"}),
    "ACCOUNT": frozenset({"MONITOR USAGE", "MONITOR EXECUTION",
                          "MONITOR SECURITY", "IMPORTED PRIVILEGES",
                          "CREATE DATABASE", "CREATE WAREHOUSE"}),
    "RESOURCE_MONITOR": _USAGE_MONITOR,
    "NETWORK_POLICY": _USAGE_MONITOR,
    "COMPUTE_POOL": _USAGE_MONITOR,
    "CONNECTION": _USAGE_MONITOR,
    "REPLICATION_GROUP": _USAGE_MONITOR,
    "FAILOVER_GROUP": _USAGE_MONITOR,
    "EXTERNAL_VOLUME": _USAGE_MONITOR,
    "APPLICATION": _USAGE_MONITOR,
    "APPLICATION_PACKAGE": _USAGE_MONITOR,
    "SHARE": _USAGE_MONITOR,
    "ORGANIZATION": frozenset(),
    "ROLE": frozenset(),
    "DATABASE_ROLE": frozenset(),
    "USER": frozenset({"MONITOR"}),
    "APPLICATION_ROLE": frozenset(),
}
NON_SOURCE_CLASSES = frozenset(NON_SOURCE_PRIVILEGES)

# Objects that govern what the source shows, who may see it or where it may
# go, wherever they live. A policy created in another database is applied
# to the source's columns and rows all the same, and its owner can change
# its body; so on these classes anything beyond a read is a write on the
# source and the `database:` scope does not excuse it. The value says what
# the object governs, for the refusal.
GOVERNANCE_CLASSES = {
    "MASKING_POLICY": "what the columns it is applied to show",
    "ROW_ACCESS_POLICY": "which rows the tables it is applied to show",
    "AGGREGATION_POLICY": "how the tables it is applied to may be queried",
    "PROJECTION_POLICY": "which columns the tables it is applied to may "
                         "project",
    "JOIN_POLICY": "how the tables it is applied to may be joined",
    "PRIVACY_POLICY": "what the tables it is applied to disclose",
    "TAG": "the tags that drive the policies on source objects",
    "AUTHENTICATION_POLICY": "how users sign in",
    "PASSWORD_POLICY": "how users sign in",
    "SESSION_POLICY": "how long a session lives",
    "PACKAGES_POLICY": "which packages the account's code may load",
    "NETWORK_RULE": "where the account may connect to and from",
}

# The database-contained classes a grant can be PLACED by: a name of the
# form DB.SCHEMA.OBJECT (DB.SCHEMA for a schema, DB for a database). Only
# a refused privilege on one of these, naming a database other than the
# migration's `database:`, is "outside this migration" and reported rather
# than refused -- and only when its reach stays on that object (see
# `EXECUTABLE_CLASSES` and `_reaches_past`). A refused privilege on any
# other class is a refusal whatever the scope. Reads on any class pass
# regardless.
SCOPED_SOURCE_CLASSES = frozenset({
    "DATABASE", "SCHEMA", "TABLE", "VIEW", "MATERIALIZED_VIEW",
    "DYNAMIC_TABLE", "EXTERNAL_TABLE", "EVENT_TABLE", "HYBRID_TABLE",
    "ICEBERG_TABLE", "STAGE", "FILE_FORMAT", "FUNCTION", "PROCEDURE",
    "SEQUENCE", "STREAM", "TASK", "PIPE", "ALERT", "STREAMLIT", "MODEL",
    "NOTEBOOK", "DATASET", "SEMANTIC_VIEW", "CORTEX_SEARCH_SERVICE",
    "SECRET", "SERVICE", "IMAGE_REPOSITORY", "GIT_REPOSITORY", "SNAPSHOT",
    "DBT_PROJECT",
})

# Source classes whose body runs with its OWNER's privileges rather than
# the caller's, each with the privilege that RUNS it. A task is resumed or
# executed with OPERATE (EXECUTE TASK, ALTER TASK ... RESUME), a pipe
# refreshed or resumed, an alert resumed, a dynamic table refreshed or
# resumed, a service resumed; a procedure is CALLed, a notebook or
# Streamlit app run, with USAGE. What the body does is set by the owner's
# grants, which this check never reads: whatever the object's owner may
# write, the role that runs it writes too -- so a task in another
# database owned by a privileged role whose body writes the source is a
# write on the source the session can trigger, and the `database:` scope
# never excuses the run privilege on one of these (`_reaches_past`). The
# run privilege is judged by the ordinary rules inside the scope: OPERATE
# on a task is a write there as before, and USAGE of a procedure,
# notebook or Streamlit app in the migration's own database passes
# because discovery reads their DDL. Outside the scope it is refused:
# discovery reads nothing there, so nothing needs it. Any OTHER privilege
# on these classes stays bounded by its object -- the owner of a task runs
# it with the owner's own privileges, which are the ones this check is
# proving read-only -- so OWNERSHIP of a task in another database is a
# reported write like OWNERSHIP of a table there.
EXECUTABLE_CLASSES: dict[str, str] = {
    "TASK": "OPERATE",
    "PIPE": "OPERATE",
    "ALERT": "OPERATE",
    "DYNAMIC_TABLE": "OPERATE",
    "SERVICE": "OPERATE",
    "PROCEDURE": "USAGE",
    "NOTEBOOK": "USAGE",
    "STREAMLIT": "USAGE",
}

# Why a privilege is refused when the reason is a known path from "not a
# write" to "a write": each of these lets the role widen its own reach,
# change what the source shows, or move the data. None of them acts on
# the object it is granted on -- MANAGE GRANTS reaches every grant in the
# account whatever row reports it -- so a privilege named here, or any
# privilege spelled `MANAGE ...`, `APPLY ...` or `EXECUTE ...`, is refused
# on EVERY class and in EVERY database (`_escalates`): the `database:`
# scope never excuses one. A privilege on a non-source class that is
# neither allowed above nor named here (MANAGE ACCOUNT SUPPORT CASES,
# CREATE ACCOUNT, ...) is refused as unknown: harmless or not, the check
# cannot say. OWNERSHIP is not here: owning an object reaches exactly that
# object, so on a source class it is a write on it (and excused by the
# scope like any write on another database), and on a role, user, account,
# warehouse or integration it is refused with `_OWNERSHIP` as the reason.
ESCALATIONS = {
    "MANAGE GRANTS": ("lets the role grant itself any privilege, including "
                      "writes on every table"),
    "CREATE ROLE": "lets the role create roles it owns and grant them on",
    "CREATE USER": "lets the role create a user it owns and sign in as it",
    "CREATE INTEGRATION": ("lets the role stand up a path for data to leave "
                           "the account"),
    "CREATE SHARE": "lets the role share the source out of the account",
    "APPLY MASKING POLICY": "lets the role change what source columns show",
    "APPLY ROW ACCESS POLICY": ("lets the role change which source rows are "
                                "visible"),
    "APPLY AGGREGATION POLICY": ("lets the role change how the source may be "
                                 "queried"),
    "APPLY TAG": "lets the role change the tags that drive source policies",
    "APPLY AUTHENTICATION POLICY": ("lets the role change how users sign "
                                    "in"),
    "EXECUTE TASK": "lets the role run tasks, which write",
    "EXECUTE MANAGED TASK": "lets the role run serverless tasks, which write",
    "MANAGE WAREHOUSES": ("lets the role control every warehouse, more than "
                          "reading needs"),
}

_ESCALATION_PREFIXES = ("MANAGE ", "APPLY ", "EXECUTE ")

_WRITE = "a write on the source"
_OWNERSHIP = ("the owner of an object can grant itself anything the object "
              "confers: a role's grants, a user's sign-in")
_UNKNOWN = ("not a privilege this check knows to be read-only, so it is "
            "refused unproven")
_NO_CLASS = ("a grants row with no object class cannot be placed or judged, "
             "so it is refused")
_ROLE_UNWALKED = ("USAGE of a role this check has not walked -- only ROLE "
                  "and DATABASE_ROLE are followed -- so what it holds is "
                  "unproven")
_NO_ROLE_NAME = ("a role grant that names no role cannot be walked, so what "
                 "the role holds is unproven and the row is refused")


def _unplaced_run(granted_on: str, privilege: str) -> str:
    kind = granted_on.lower().replace("_", " ")
    return (f"{privilege} on a {kind} is allowed only inside the migration's "
            f"database, where discovery reads it, and this row's name is not "
            f"one SHOW GRANTS prints for a {kind} (DB.SCHEMA.OBJECT), so it "
            f"cannot be placed in any database; the allowance does not "
            f"apply and the row is refused")


def _owners_rights(granted_on: str, privilege: str) -> str:
    kind = granted_on.lower().replace("_", " ")
    one = ("an " if kind[:1] in "aeiou" else "a ") + kind
    verb = "call" if privilege == "USAGE" else "run"
    return (f"{privilege} on {one} lets the role {verb} it, and {one} runs "
            f"with its OWNER's privileges, not the role's, so what it does "
            f"is set by grants this check does not read and may write the "
            f"source; outside the migration's database nothing reads it, "
            f"so the grant is refused in any scope")

# Roles whose reach Snowflake defines; a session that is, holds or inherits
# one is refused without reading its grants. Compared with the stored name
# as the listings print it, case-sensitively: an unquoted identifier is
# stored in upper case, so exactly SYSADMIN is the system role and a
# distinct role created as "sysadmin" is not (`_refuse_system_role`).
SYSTEM_ROLES = frozenset({
    "ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN", "ORGADMIN",
    "GLOBALORGADMIN",
})

# SHOW GRANTS returns at most this many rows. A listing that long may have
# been cut, so it cannot prove anything.
SHOW_GRANTS_CAP = 10_000


class RoleNotReadOnly(AuthError):
    """The role is not provably read-only on the source. Nothing ran."""


def _lower_keys(row) -> dict:
    """The row with lower-cased column names; a row that is not a mapping
    refuses the session rather than crashing it."""
    if row is None:
        return {}
    if not isinstance(row, Mapping):
        raise RoleNotReadOnly(
            f"a grants row this check cannot read ({type(row).__name__} "
            f"rather than a mapping of columns), so the role's privileges "
            f"are unknown and the session is not provably read-only.")
    return {str(k).lower(): v for k, v in row.items()}


def _norm(value) -> str:
    """Upper-cased, with the whitespace SHOW GRANTS never prints removed:
    leading, trailing and doubled spaces. A None is the empty string."""
    return " ".join(str(value if value is not None else "").split()).upper()


def _class_of(row: dict) -> str:
    """The object class of a grants row, as a single token. SHOW GRANTS says
    `granted_on`, SHOW FUTURE GRANTS `grant_on`; a class printed with a
    space (`DATABASE ROLE`) is the same class as with an underscore."""
    return _norm(row.get("granted_on") or row.get("grant_on")
                 ).replace(" ", "_")


def _quote_role(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _split_identifier_parts(name: str) -> list[tuple[str, bool]]:
    """The parts of a dotted identifier as SHOW GRANTS prints it, each with
    whether it was quoted.

    A quoted part is read up to its closing quote (a doubled quote inside
    is one quote) and kept as written; an unquoted part is kept as written
    too, since SHOW GRANTS prints the stored name. The parts are requoted
    one by one to address the object again, so a dot inside a quoted part
    stays inside it.
    """
    text = str(name or "").strip()
    parts: list[tuple[str, bool]] = []
    buf: list[str] = []
    quoted = False
    was_quoted = False
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
            was_quoted = True
        elif ch == ".":
            parts.append(("".join(buf), was_quoted))
            buf = []
            was_quoted = False
        else:
            buf.append(ch)
        i += 1
    parts.append(("".join(buf), was_quoted))
    return parts


def _split_identifier(name: str) -> list[str]:
    """The parts of a dotted identifier, unquoted (see
    `_split_identifier_parts`)."""
    return [part for part, _ in _split_identifier_parts(name)]


def _sql_name(listed: str) -> str:
    """A name from a SHOW GRANTS row, requoted part by part for a statement."""
    return ".".join(_quote_role(p) for p in _split_identifier(listed))


def _database_of(name: str) -> str:
    """The first identifier of a dotted object name, unquoted and folded.

    SHOW GRANTS names a table `DB.SCHEMA.TABLE`, a schema `DB.SCHEMA` and a
    database `DB`, quoting any part that needs it. Only the database part
    is wanted here, so a quoted first part is read up to its closing quote
    and kept as written (a quoted identifier IS its case); an unquoted one
    folds to upper case, as Snowflake resolves it, with the whitespace an
    unquoted identifier cannot contain removed (`_norm`), so a padded part
    is the same database as an unpadded one and never "another database".
    `_scope_of` resolves the config's `database:` with the rule discovery
    uses (`lexer.config_name`), which agrees with this one on every name
    SHOW GRANTS can print: an unquoted identifier folds on both sides, and
    a name that is not one (`my-db`, `Sales DB`) is quoted by SHOW GRANTS
    and kept verbatim by both.
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
    return _norm(text.split(".", 1)[0])


def _placeable(granted_on: str, obj: str, *, future: bool = False) -> bool:
    """True when the grant can be PLACED in a database: a database-contained
    source class, named the way SHOW GRANTS names one.

    `DB` for a database, `DB.SCHEMA` for a schema, `DB.SCHEMA.OBJECT` for
    everything else -- exactly that many parts on a current grant, since
    SHOW GRANTS always prints the full name; a future grant names the
    container the objects will be created in (`DB.<TABLE>` or
    `DB.SCHEMA.<TABLE>`), so two or three. Every unquoted part before the
    object part must be an identifier, and an unquoted identifier cannot
    contain whitespace (the object part may: a function's signature has
    `F(VARCHAR, NUMBER)`). A grant on any other class, or whose name is
    blank, has the wrong number of parts or a malformed container part,
    cannot be placed.
    """
    if granted_on not in SCOPED_SOURCE_CLASSES:
        return False
    parts = _split_identifier_parts(obj) if obj else []
    if granted_on == "DATABASE":
        allowed = (1,)
    elif granted_on == "SCHEMA":
        allowed = (2,)
    elif future:
        allowed = (2, 3)
    else:
        allowed = (3,)
    if len(parts) not in allowed:
        return False
    for text, quoted in parts[:-1] or parts[:1]:
        if not text or (not quoted and text.split() != [text]):
            return False
    return True


def _outside_scope(granted_on: str, obj: str, scope: frozenset, *,
                   future: bool = False) -> bool:
    """True when the grant is provably on an object of a database OTHER
    than every one in `scope` (the databases discovery will read).

    A row that cannot be placed (`_placeable`) is judged inside the scope:
    it cannot be shown to be elsewhere, and a row the check cannot place
    is a row it must judge.
    """
    return (_placeable(granted_on, obj, future=future)
            and _database_of(obj) not in scope)


def _is_write(privilege: str) -> bool:
    p = _norm(privilege)
    return p in WRITE_PRIVILEGES or p.startswith("CREATE")


def _escalates(privilege: str) -> bool:
    """True for a privilege whose reach is not bounded by the object it is
    granted on: one named in `ESCALATIONS`, or any `MANAGE ...`, `APPLY ...`
    or `EXECUTE ...` privilege. Such a grant is refused on whatever class
    and in whatever database a row reports it; the `database:` scope
    excuses only a write that stays on its object."""
    return (privilege in ESCALATIONS
            or privilege.startswith(_ESCALATION_PREFIXES))


def _runs_owner(privilege: str, granted_on: str) -> bool:
    """True for the privilege that runs an owner's-rights object: OPERATE
    on a task, pipe, alert, dynamic table or service, USAGE of a procedure,
    notebook or Streamlit app (`EXECUTABLE_CLASSES`)."""
    return EXECUTABLE_CLASSES.get(granted_on) == privilege


def _reaches_past(privilege: str, granted_on: str) -> bool:
    """True when the privilege's reach is not bounded by the object a row
    reports it on, so the `database:` scope cannot excuse it: an
    escalation (`_escalates`), or the run privilege of an owner's-rights
    object (`_runs_owner`), whose body may write the source from any
    database. The scope excuses only a write that stays on its object."""
    return _escalates(privilege) or _runs_owner(privilege, granted_on)


def _refusal(privilege: str, granted_on: str, *,
             outside: bool = False) -> str | None:
    """Why a grant of `privilege` ON a `granted_on` is refused; None when it
    cannot lead to a write on the source. `outside` says the grant has been
    placed on another database than the migration's.

    A blank class is refused on sight. A non-source class is judged by its
    allowlist; a role class the walk does not follow is refused unwalked;
    a governance class (policy, tag, network rule) allows reads only; the
    run privilege of an owner's-rights object outside the migration's
    database is refused with the owner named; a source class allows the
    read list, and names an escalation before a write so the refusal says
    what the privilege reaches. Either way a privilege the check does not
    know is refused: the burden of proof is on the grant.
    """
    if not granted_on:
        return _NO_CLASS
    allowed = NON_SOURCE_PRIVILEGES.get(granted_on)
    if allowed is not None:
        if privilege in allowed:
            return None
        if privilege == "OWNERSHIP":
            return _OWNERSHIP
        if privilege in ESCALATIONS:
            return ESCALATIONS[privilege]
        if "ROLE" in granted_on:
            return _ROLE_UNWALKED
        return _UNKNOWN
    if "ROLE" in granted_on:
        # INSTANCE_ROLE, or a role class Snowflake adds later: USAGE of it
        # is a role the walk did not follow, anything else an escalation
        # or unknown.
        if privilege == "OWNERSHIP":
            return _OWNERSHIP
        if privilege in ESCALATIONS:
            return ESCALATIONS[privilege]
        return _ROLE_UNWALKED
    governs = GOVERNANCE_CLASSES.get(granted_on)
    if governs is not None:
        if privilege in READ_PRIVILEGES:
            return None
        return (f"a {granted_on.lower().replace('_', ' ')} governs "
                f"{governs}, in whichever database it lives, so owning or "
                f"changing one is a write on the source")
    if outside and _runs_owner(privilege, granted_on):
        return _owners_rights(granted_on, privilege)
    if privilege in READ_PRIVILEGES:
        return None
    if privilege in ESCALATIONS:
        return ESCALATIONS[privilege]
    if _is_write(privilege):
        return _WRITE
    return _UNKNOWN


_SCOPE_HELP = (
    "The scope must be the database `database:` names, or the databases "
    "`--database` lists, each a non-empty string; the session is refused "
    "rather than judged against a scope it cannot read.")


def _scope_of(database) -> frozenset | None:
    """The databases discovery will read, as the set of names Snowflake
    resolves them to -- or None when no scope was given, in which case
    every grant is judged inside the migration.

    One name (the config's `database:`) or a list of them (what a repeated
    `--database` builds). Each is resolved with `lexer.config_name`, the
    rule discovery and the data plane apply to the same value, so the
    guard and the readers agree by construction: `sales_db` names
    SALES_DB, `'"MyDb"'` names MyDb, and `my-db` or `Sales DB` -- not a
    plain identifier, so meant verbatim -- stays as written, which is how
    SHOW GRANTS prints it (quoted). A name a listing PRINTS is already
    stored and goes through `_stored_name` instead.

    Fails closed on any other shape. A list that is empty, a member that
    is not a string or is blank, a mapping, a number: none of these names
    the databases discovery will read, and a scope read from the wrong
    thing (the text of a list, say) would place every real database
    "outside the migration" and turn every write into a report. A blank
    string alone is no scope, like None.
    """
    if database is None:
        return None
    if isinstance(database, str):
        if not database.strip():
            return None
        names = [database]
    elif isinstance(database, (list, tuple, set, frozenset)):
        names = list(database)
        if not names:
            raise RoleNotReadOnly(
                f"the database scope is an empty list, so the databases "
                f"this migration reads are unknown and the role cannot be "
                f"judged against them. {_SCOPE_HELP}")
    else:
        raise RoleNotReadOnly(
            f"the database scope is a {type(database).__name__}, not a "
            f"database name or a list of them, so the role cannot be "
            f"judged against the databases this migration reads. "
            f"{_SCOPE_HELP}")
    scope = set()
    for name in names:
        if not isinstance(name, str) or not name.strip():
            raise RoleNotReadOnly(
                f"the database scope names a "
                f"{'blank name' if isinstance(name, str) else type(name).__name__}"
                f" where a database name is expected, so the role cannot "
                f"be judged against the databases this migration reads. "
                f"{_SCOPE_HELP}")
        scope.add(lexer.config_name(name))
    return frozenset(scope)


def _stored_name(value) -> str:
    """A role name as a listing prints it, read as the stored name it is.

    `current_role()`, `current_secondary_roles()`, `SHOW GRANTS TO USER`
    and a `USAGE on ROLE` row all print the stored name unquoted, and a
    stored name IS its case: SYSADMIN and a role created as `"sysadmin"`
    are two roles. So nothing is folded. A quoted name, should a listing
    ever print one, is read as its text."""
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1].replace('""', '"')
    return text


def _refuse_system_role(name: str, described: str) -> None:
    """Stop if `name` is a system role. `described` names it and says where
    in the hierarchy it was found. The stored name is compared as printed,
    case-sensitively: exactly SYSADMIN is the system role, `sysadmin` is a
    role someone created and is walked like any other."""
    if _stored_name(name) not in SYSTEM_ROLES:
        return
    raise RoleNotReadOnly(
        f"{described} is a system role: what a system role can do is set by "
        f"Snowflake, not by its grants listing, so the session is refused "
        f"without reading it and is not provably read-only. The migration "
        f"role must not be, hold or inherit "
        f"{', '.join(sorted(SYSTEM_ROLES))}, and the service user must hold "
        f"no such role; set `role:` to a dedicated read-only role.")


_SECONDARY_HELP = (
    "Run with --only-primary-role to scope the session to `role:` alone, or "
    "set the user's DEFAULT_SECONDARY_ROLES to none.")

_USER_HELP = (
    "Every role granted to the user is one USE ROLE away for whoever holds "
    "the credential (and all of them are in effect under USE SECONDARY "
    "ROLES ALL), so the migration does not run on a session whose user's "
    "roles it cannot name. Grant the service user the migration role alone, "
    "and let it read its own grants.")


def _roles_granted_to_user(run_sql: Callable[..., list], held: list[str]
                           ) -> tuple[str, list[str]]:
    """The session's user and every role granted to it besides `held`, the
    primary and secondary roles the session already has.

    The credential can `USE ROLE` any of them, so each is judged like the
    session's own role. `USE SECONDARY ROLES ALL` puts all of them in
    effect at once, named by `current_secondary_roles()` -- except when it
    does not (an empty `roles` next to `value: ALL`); the user's grants list
    is the authority either way. When it cannot be read the session is
    refused, because the roles the credential reaches are unknown.

    The listing is believed only when it is one this session could have
    produced. The user holds every role in `held` -- it is running as them
    -- so a listing that does not name each of them is cut or filtered,
    and a row that names no role (a null or blank `role`, a row without
    the column, a row that is not a mapping) is a listing the check cannot
    read: both are refused rather than read as "the user holds nothing
    else". PUBLIC is never named by a user listing, so it is not looked
    for; it is walked for every session regardless.
    """
    try:
        rows = run_sql("select current_user() U")
        user = _lower_keys(rows[0]).get("u") if rows else None
        if not user:
            raise ValueError("the session has no current user")
        listing = run_sql(f"show grants to user {_quote_role(user)}")
    except RoleNotReadOnly:
        raise
    except Exception as exc:
        raise RoleNotReadOnly(
            "the roles granted to the session's user could not be read "
            f"({str(exc)[:200]}). {_USER_HELP}") from exc
    if len(listing) >= SHOW_GRANTS_CAP:
        raise RoleNotReadOnly(
            f"the user holds {len(listing)} role grants, at or over SHOW "
            f"GRANTS' {SHOW_GRANTS_CAP}-row cap; the listing may be cut, "
            f"so the roles it reaches are unknown. {_USER_HELP}")
    listed: list[str] = []
    for raw in listing:
        value = _lower_keys(raw).get("role")
        name = str(value if value is not None else "").strip()
        if not name:
            raise RoleNotReadOnly(
                f"a row of the grants of user {user} names no role (the "
                f"`role` column is missing or blank), so the roles the "
                f"credential reaches are unknown and the session is not "
                f"provably read-only. {_USER_HELP}")
        if name not in listed:
            listed.append(name)
    # (exactly PUBLIC, the stored name: a role created as "public" is a
    #  role the user was granted, and the listing has to name it)
    unnamed = [name for name in held
               if name not in listed and _stored_name(name) != PUBLIC_ROLE]
    if unnamed:
        raise RoleNotReadOnly(
            f"the grants of user {user} do not name {', '.join(unnamed)}, "
            f"which the session is running as, so the listing is cut or "
            f"filtered and cannot say which roles the credential reaches; "
            f"the session is not provably read-only. {_USER_HELP}")
    return str(user), [name for name in listed if name not in held]


def _session_roles(run_sql: Callable[..., list]) -> tuple[str, list[str]]:
    rows = run_sql("select current_role() R, current_secondary_roles() S")
    row = _lower_keys(rows[0]) if rows else {}
    role = row.get("r")
    if not role:
        raise RoleNotReadOnly(
            "the session has no current role, so its privileges cannot be "
            "read. Set `role:` in the migration config to a read-only role.")
    role = str(role)
    raw = row.get("s")
    if raw is None or raw == "":
        raise RoleNotReadOnly(
            "current_secondary_roles() returned nothing, so the roles in "
            "effect are unknown and the session is not provably read-only. "
            f"{_SECONDARY_HELP}")
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(parsed, Mapping):
            raise TypeError("not an object")
        names = str(parsed.get("roles") or "")
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
    return role, secondary


def _list_grants(run_sql: Callable[..., list], kind: str, sql_name: str,
                 label: str, *, future: bool = False) -> list:
    """One `SHOW [FUTURE] GRANTS TO <kind> <name>`, complete or refused."""
    what = "future grants" if future else "grants"
    try:
        rows = run_sql(f"show {what} to {kind} {sql_name}")
    except Exception as exc:
        raise RoleNotReadOnly(
            f"the {what} of {label} could not be read "
            f"({str(exc)[:200]}). The migration does not run against a "
            f"role whose privileges it cannot prove read-only.") from exc
    if len(rows) >= SHOW_GRANTS_CAP:
        raise RoleNotReadOnly(
            f"{label} holds {len(rows)} {what}, at or over SHOW GRANTS' "
            f"{SHOW_GRANTS_CAP}-row cap; the listing may be cut, so it "
            f"cannot prove the role is read-only. Use a narrower role for "
            f"the migration.")
    return rows


def read_role_grants(run_sql: Callable[..., list], *,
                     database=None) -> dict:
    """Every grant the session's roles hold, judged against the source.

    `database` is the scope: the config's `database:` (one name) or the
    list a repeated `--database` builds -- exactly the databases discovery
    will read, resolved as discovery resolves them (`_scope_of`). None
    judges every grant inside the migration; any other shape is refused.

    The session's primary and secondary roles, every other role granted to
    its user, and PUBLIC are listed with `SHOW GRANTS TO ROLE` and `SHOW
    FUTURE GRANTS TO ROLE`, and every role or database role a listing names
    is listed in turn, so a write inherited through the role hierarchy
    counts the same as one granted directly. A system role met anywhere
    refuses the session before it is listed.

    Returns evidence, never a credential:

        role, secondary_roles  -- what the session holds
        user, user_roles       -- the session's user and the other roles
                                  granted to it (each walked too)
        database, databases    -- the scope: one printable string (the
                                  names joined) and the resolved names;
                                  None and [] without a scope
        inherited_roles        -- roles reached through those, as
                                  "role NAME" / "database role DB.NAME";
                                  "role PUBLIC" is always among them
        grants_read            -- rows read across every SHOW GRANTS
        future_grants_read     -- rows read across every SHOW FUTURE GRANTS
        by_privilege           -- {privilege: count} over the source; a
                                  future grant counts as "PRIV (future)"
        write_grants           -- the refused rows, each
                                  {role, privilege, granted_on, name, via,
                                   future, reason}
                                  (`role` is the session, user or PUBLIC
                                  role it descends from; `via` names the
                                  inherited role chain that carried it,
                                  None for a direct grant; `reason` says
                                  why it is refused: a write, an
                                  escalation, or unknown)
        out_of_scope_writes    -- refused rows provably on OTHER databases
                                  (reported only)
        read_only              -- True when write_grants is empty

    Raises RoleNotReadOnly when the grants cannot be read in full, or a
    system role is in the hierarchy: the caller must treat that as a
    failure, not as a pass.
    """
    role, secondary = _session_roles(run_sql)
    _refuse_system_role(role, f"the session's role {role}")
    for name in secondary:
        _refuse_system_role(name, f"the session's secondary role {name}")
    user, user_roles = _roles_granted_to_user(run_sql, [role, *secondary])
    # (the listing has already been checked to name `role` and each
    #  secondary role; `user_roles` is what the user holds beyond them)
    for name in user_roles:
        _refuse_system_role(name, f"role {name}, granted to user {user},")
    scope = _scope_of(database)
    grants_read = 0
    future_read = 0
    by_privilege: dict[str, int] = {}
    writes: list[dict] = []
    inherited: list[str] = []
    out_of_scope = 0
    # (session role it descends from, kind, quoted name, inherited chain,
    #  what to call it in a refusal)
    pending = [(name, "role", _quote_role(name), None, f"role {name}")
               for name in [role, *secondary]]
    pending += [(name, "role", _quote_role(name), None,
                 f"role {name} (granted to user {user})")
                for name in user_roles]
    seen = {(kind, sql_name) for _, kind, sql_name, _, _ in pending}
    public = ("role", _quote_role(PUBLIC_ROLE))
    if public not in seen:
        # Never named by any listing, held by every role: walked for every
        # session, and reported with the inherited roles.
        seen.add(public)
        inherited.append(f"role {PUBLIC_ROLE}")
        pending.append((PUBLIC_ROLE, *public, None,
                        f"role {PUBLIC_ROLE} (held by every role)"))
    while pending:
        held_by, kind, sql_name, via, label = pending.pop(0)
        for future in (False, True):
            rows = _list_grants(run_sql, kind, sql_name, label, future=future)
            if future:
                future_read += len(rows)
            else:
                grants_read += len(rows)
            for raw in rows:
                row = _lower_keys(raw)
                granted_on = _class_of(row)
                privilege = _norm(row.get("privilege"))
                obj = str(row.get("name") or "").strip()
                if (granted_on in INHERITED_ROLE_CLASSES
                        and privilege == "USAGE" and not future):
                    # Another role this one holds: its grants are ours too.
                    sub_kind = INHERITED_ROLE_CLASSES[granted_on]
                    parts = _split_identifier(obj)
                    if (len(parts) != (1 if sub_kind == "role" else 2)
                            or any(not part.strip() for part in parts)):
                        # A role grant that names no role -- a blank name,
                        # a blank part (`SALES_DB.""`), or not the parts
                        # its class prints (NAME for a role, DB.NAME for a
                        # database role): nothing to walk, so nothing
                        # proven. Refused, not listed as `""`.
                        writes.append({"role": held_by, "privilege": privilege,
                                       "granted_on": granted_on, "name": obj,
                                       "via": via, "future": future,
                                       "reason": _NO_ROLE_NAME})
                        continue
                    sub_sql = _sql_name(obj)
                    if (sub_kind, sub_sql) not in seen:
                        seen.add((sub_kind, sub_sql))
                        shown = (obj if sub_kind == "role"
                                 else f"{sub_kind} {obj}")
                        chain = shown if via is None else f"{via} -> {shown}"
                        if sub_kind == "role":
                            _refuse_system_role(
                                obj, f"role {obj}, granted to role "
                                     f"{held_by} via {chain},")
                        inherited.append(f"{sub_kind} {obj}")
                        pending.append((held_by, sub_kind, sub_sql, chain,
                                        f"{sub_kind} {obj} (granted to role "
                                        f"{held_by})"))
                    continue
                # Only a grant that can be PLACED on another database is
                # outside the migration; a blank, short or malformed name,
                # a class that is not database-contained, a policy or a
                # tag is judged as inside it.
                outside = bool(scope) and _outside_scope(
                    granted_on, obj, scope, future=future)
                reason = _refusal(privilege, granted_on, outside=outside)
                if (scope and reason is None
                        and _runs_owner(privilege, granted_on)
                        and not _placeable(granted_on, obj, future=future)):
                    # The run privilege of an owner's-rights object passes
                    # only INSIDE the scope, where discovery reads it. A
                    # row whose name cannot be placed (two or four parts,
                    # a malformed container) is not shown to be inside:
                    # it gets no allowance, and is refused.
                    reason = _unplaced_run(granted_on, privilege)
                if granted_on in NON_SOURCE_CLASSES:
                    # Not a source object: not counted among the source
                    # privileges, refused only when it leads somewhere.
                    if reason is not None:
                        writes.append({"role": held_by, "privilege": privilege,
                                       "granted_on": granted_on, "name": obj,
                                       "via": via, "future": future,
                                       "reason": reason})
                    continue
                # And only a privilege whose reach stays on its object is
                # excused there: MANAGE GRANTS reported ON another database
                # still reaches every grant in the account, and OPERATE on
                # a task there runs its owner's body, which may write the
                # source -- so an escalation or the run privilege of an
                # owner's-rights object is refused in any scope.
                if (outside and not (reason is not None
                                     and _reaches_past(privilege, granted_on))):
                    if reason is not None:
                        out_of_scope += 1
                    continue
                key = f"{privilege} (future)" if future else privilege
                by_privilege[key] = by_privilege.get(key, 0) + 1
                if reason is not None:
                    writes.append({"role": held_by, "privilege": privilege,
                                   "granted_on": granted_on, "name": obj,
                                   "via": via, "future": future,
                                   "reason": reason})
    return {"role": role, "secondary_roles": secondary,
            "user": user, "user_roles": user_roles,
            "inherited_roles": inherited,
            # `database` is the scope as one printable string (the names
            # joined when `--database` listed several); `databases` the
            # resolved names themselves.
            "database": ", ".join(sorted(scope)) if scope else None,
            "databases": sorted(scope) if scope else [],
            "grants_read": grants_read,
            "future_grants_read": future_read,
            "by_privilege": dict(sorted(by_privilege.items())),
            "write_grants": writes, "out_of_scope_writes": out_of_scope,
            "read_only": not writes}


def _roles_text(evidence: dict) -> str:
    text = evidence["role"]
    if evidence.get("secondary_roles"):
        text += f' (+ secondary {", ".join(evidence["secondary_roles"])})'
    if evidence.get("user_roles"):
        text += (f' (+ granted to user {evidence.get("user")}: '
                 f'{", ".join(evidence["user_roles"])})')
    if evidence.get("inherited_roles"):
        # PUBLIC is in every list; the roles this session reached are the
        # news, so they are named first.
        public = f"role {PUBLIC_ROLE}"
        named = [r for r in evidence["inherited_roles"] if r != public]
        named += [r for r in evidence["inherited_roles"] if r == public]
        text += f' (+ inherited {", ".join(named)})'
    return text


def _write_text(w: dict, primary: str) -> str:
    text = (("future " if w.get("future") else "")
            + f'{w["privilege"]} on {w["granted_on"] or "?"} {w["name"] or "?"}')
    text += f' via {w["via"]}' if w.get("via") else ""
    if w.get("role") and w["role"] != primary:
        text += f' held by role {w["role"]}'
    return text + (f' ({w["reason"]})' if w.get("reason") else "")


def describe_role_grants(evidence: dict) -> str:
    """One line of evidence for a report or a console. No values."""
    roles = _roles_text(evidence)
    where = (f' on {evidence["database"]}' if evidence.get("database")
             else " on the account's objects")
    held = ", ".join(f"{p} x{n}" for p, n in evidence["by_privilege"].items()
                     ) or "none"
    read = f'{evidence["grants_read"]} grant(s)'
    if evidence.get("future_grants_read"):
        read += f' and {evidence["future_grants_read"]} future grant(s)'
    extra = (f'; {evidence["out_of_scope_writes"]} refused grant(s) on other '
             f'databases, outside this migration'
             if evidence.get("out_of_scope_writes") else "")
    if evidence["read_only"]:
        return (f"role {roles} is read-only{where}: {read} read, "
                f"held: {held}{extra}")
    shown = [_write_text(w, evidence["role"])
             for w in evidence["write_grants"][:5]]
    more = len(evidence["write_grants"]) - len(shown)
    return (f"role {roles} holds privilege(s) that write the source or lead "
            f"to a write{where}: "
            + ", ".join(shown) + (f" (+{more} more)" if more > 0 else "")
            + ". The migration role must be read-only: grant it USAGE and "
              "SELECT only, nothing on the account beyond warehouse usage, "
              "no role that holds more, and nothing to PUBLIC or to the "
              "service user beyond it, or set `role:` to one that is"
            + extra)


def assert_role_read_only(run_sql: Callable[..., list], *,
                          database=None) -> dict:
    """The evidence, or RoleNotReadOnly. Call before the first read.
    `database`: one name or the list discovery will read (see
    `read_role_grants`)."""
    evidence = read_role_grants(run_sql, database=database)
    if not evidence["read_only"]:
        raise RoleNotReadOnly(describe_role_grants(evidence))
    return evidence
