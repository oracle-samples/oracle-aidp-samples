"""Everything in the estate that is NOT a table or a view. Read-only.

`assess` ran `SHOW TABLES` and `SHOW VIEWS` and nothing else, so the plan
reported "7 of 7 objects can move" while procedures, UDFs, tasks, streams,
stages, pipes, sequences and file formats sat there unexamined. The number was
true of what had been LOOKED AT, and overstated coverage of the estate.

NOTHING HERE IS MIGRATABLE BY THIS PLUGIN. Every entry carries
`migratable: False`, a reason, and a pointer at the AIDP capability that would
carry the workload. A pointer is not a promise: no equivalent is generated,
because a plausible-but-wrong procedure translation is worse than an honest
gap.

Two source choices worth keeping:

  * PROCEDURES and FUNCTIONS come from INFORMATION_SCHEMA, not from SHOW.
    `SHOW PROCEDURES IN SCHEMA` returns Snowflake's own built-ins -- 33 of them
    on a completely empty schema -- and INFORMATION_SCHEMA does not. It also
    carries the LANGUAGE, which is what decides the effort.
  * Reads are scoped per DATABASE, not per schema. INFORMATION_SCHEMA is a
    per-database view and SHOW accepts `IN DATABASE`, so the whole census costs
    about ten queries per database rather than ten per schema.
  * Except for the handful of things that are not in a database at all. Shares,
    roles, network policies, applications, compute pools and replication
    and failover groups belong to the
    ACCOUNT, so a `"scope": "account"` entry is read ONCE per run no matter how
    many databases are in scope. Reading them per database would issue the same
    statement N times and count every row N times.

And one limit that shapes every number here: SHOW and INFORMATION_SCHEMA are
PRIVILEGE-FILTERED. An object the current role holds no privilege on is simply
absent from the result, and the statement still succeeds. So a count is a lower
bound as seen by that role, a zero means "none visible" rather than "none
exist", and the scope statement says so instead of declaring the migratable
count to be the whole estate. `unreadable` remains the separate case where the
statement itself failed.
"""
from __future__ import annotations

import collections
import datetime
import re
import json
from typing import Callable

from ..dialect import lexer
from .catalog import SHOW_PAGE_SIZE, show_paged

__all__ = ["KINDS", "LANGUAGE_VERDICTS", "VISIBILITY_GRANTS", "build_census",
           "name_at", "secondary_roles_active"]

# What a COMPLETE census needs the role to hold, per Snowflake's documentation
# of each source. Listed in CENSUS.md's header. Documentation, not a probe:
# confirm against SHOW GRANTS in the account before relying on it.
VISIBILITY_GRANTS: tuple[tuple[str, str], ...] = (
    ("procedures and UDFs", "USAGE on each (or OWNERSHIP); INFORMATION_SCHEMA "
                            "lists only those"),
    ("sequences, stages, file formats", "USAGE on each (or OWNERSHIP)"),
    ("pipes", "MONITOR or OPERATE on each (or OWNERSHIP)"),
    ("tasks", "MONITOR or OPERATE on each (or OWNERSHIP)"),
    ("streams", "SELECT on each (or OWNERSHIP)"),
    ("materialized views, dynamic tables", "any privilege on each; SELECT is "
                                           "enough"),
    ("alerts", "MONITOR or OPERATE on each (or OWNERSHIP)"),
    ("secrets, network rules", "USAGE on each (or OWNERSHIP). The census names "
                               "them and never reads a secret value"),
    ("Streamlit apps, notebooks", "USAGE on each (or OWNERSHIP)"),
    ("services, compute pools", "USAGE or MONITOR on each (or OWNERSHIP); the "
                                "account-level MONITOR USAGE covers all of "
                                "them"),
    ("shares (account-scoped)", "OWNERSHIP of each outbound share, or IMPORT "
                                "SHARE for an inbound one; a role without "
                                "either sees an empty list, and an outbound "
                                "share is a live contract with a consumer "
                                "account"),
    ("roles (account-scoped)", "MANAGE GRANTS, or a role the grant graph "
                               "already reaches; anything less returns part of "
                               "the hierarchy, not the hierarchy"),
    ("network policies (account-scoped)", "OWNERSHIP, or the account-level "
                                          "ATTACH POLICY privilege"),
    ("applications (account-scoped)", "USAGE on each installed application (or "
                                      "OWNERSHIP)"),
    ("replication and failover groups (account-scoped)",
     "OWNERSHIP, MONITOR or REPLICATE on each group; one SHOW REPLICATION "
     "GROUPS lists both kinds"),
    ("all of the above at once", "an owner or governance role that holds a "
                                 "privilege on every object -- or cross-check "
                                 "the counts against SNOWFLAKE.ACCOUNT_USAGE, "
                                 "which is not privilege-filtered and needs "
                                 "IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE"),
)

# language -> what it would take on AIDP. Effort is a triage band, not an
# estimate: it says which pile the object belongs in.
LANGUAGE_VERDICTS: dict[str, dict] = {
    "SQL": {
        "effort": "MEDIUM",
        "aidp_path": "Snowflake Scripting is procedural SQL with no Spark "
                     "equivalent. Orchestration becomes an AIDP Job; the body "
                     "becomes Spark SQL in a notebook task.",
    },
    "JAVASCRIPT": {
        "effort": "HIGH",
        "aidp_path": "There is no JavaScript runtime on AIDP. The logic has to "
                     "be rewritten in Python or Spark SQL, which means it has "
                     "to be understood first, not translated.",
    },
    "PYTHON": {
        "effort": "MEDIUM",
        "aidp_path": "The closest path on AIDP. A Snowpark Python handler is "
                     "not a Spark UDF, so the body needs reworking, but the "
                     "language and most libraries carry over.",
    },
    "JAVA": {
        "effort": "MEDIUM",
        "aidp_path": "Repackage as a Spark UDF and install the JAR on the "
                     "cluster. Mechanical, but the JAR has to be rebuilt "
                     "against the Spark API.",
    },
    "SCALA": {
        "effort": "MEDIUM",
        "aidp_path": "Repackage as a Spark UDF. Scala is native to Spark, so "
                     "this is usually the least painful of the handler "
                     "languages.",
    },
}

_UNKNOWN_LANGUAGE = {
    "effort": "UNKNOWN",
    "aidp_path": "The handler language was not recognised, so no path is "
                 "proposed. Inspect it before estimating.",
}

# An external function has no handler language by construction -- the body is
# somewhere else entirely -- so it must not fall through to _UNKNOWN_LANGUAGE,
# which would report a missing language as the thing to go and inspect.
_EXTERNAL_FUNCTION_VERDICT = {
    "effort": "HIGH",
    "aidp_path": "There is no external-function object on AIDP. The remote "
                 "service keeps working; what has to be rebuilt is the call: "
                 "an egress path from the cluster to that endpoint, the API "
                 "integration's authentication re-established outside "
                 "Snowflake, and each call site rewritten as an explicit "
                 "request from the job rather than a function in a SELECT.",
}

# ---------------------------------------------------------------------------
# Two reads return rows that are not all the same thing. A `refine` hook on the
# spec looks at one row and says what it actually is. It only ever narrows: if
# the deciding column is absent the hook returns nothing and the row keeps the
# kind and the reason the census has always given it.
# ---------------------------------------------------------------------------

_STAGE_EXTERNAL_REASON = (
    "an EXTERNAL stage already points at a bucket in object storage, so the "
    "files are not inside Snowflake and nothing has to be unloaded: AIDP can "
    "be pointed at the same location as a volume. What does not travel is the "
    "storage integration behind it, so the location has to be confirmed and "
    "the credential re-created in the AIDP credential store before the first "
    "read.")

_STAGE_INTERNAL_REASON = (
    "an INTERNAL stage holds its files inside Snowflake's own storage, where "
    "nothing outside Snowflake can read them. Every file has to be unloaded to "
    "object storage first (`GET`, or `COPY INTO` an external stage) before "
    "AIDP can see it -- this plugin moves no bytes, so that unload is work "
    "nobody has scheduled yet.")

_UDTF_REASON = (
    "a UDTF returns a table, not a value. Spark has no table-function object, "
    "so the rewrite is not one object: every query that calls it becomes a "
    "join against a view, a `LATERAL VIEW explode`, or a DataFrame "
    "transformation, and each call site has to be found and changed.")

_EXTERNAL_FUNCTION_REASON = (
    "an external function is not code in the database at all: it is a call out "
    "to a remote endpoint through an API integration. Nothing on AIDP receives "
    "that call, so until the endpoint, its authentication and a network path "
    "from the cluster are re-established, every query that calls it fails "
    "rather than returning a wrong answer.")


def _refine_stage(row: dict) -> dict | None:
    """INTERNAL and EXTERNAL stages are the same row with opposite verdicts."""
    stage_type = str(row.get("STAGE_TYPE") or "")
    if not stage_type:
        return None
    detail = f"type={stage_type}"
    url = str(row.get("STAGE_URL") or "")
    if url:
        detail += f" url={url}"
    region = str(row.get("STAGE_REGION") or "")
    if region:
        detail += f" region={region}"
    external = "external" in stage_type.lower()
    return {"reason": _STAGE_EXTERNAL_REASON if external
                      else _STAGE_INTERNAL_REASON,
            "detail": detail}


_SHARE_INBOUND_REASON = (
    "an INBOUND share is a database this account reads from a provider, and "
    "it is not this account's data to move: nothing in it is copied by the "
    "migration, and any table or view built on top of it loses its source at "
    "cutover. The provider has to be asked for another delivery route before "
    "anything downstream of it is planned.")


def _refine_stream(row: dict) -> dict | None:
    """The table a stream reads, from SHOW STREAMS' `table_name` (live
    shape: `SNOWMIG_COVERAGE.CORE.ORDERS`). `ddl` turns on the Delta change
    data feed for that table; a stream on a view reads no table here."""
    table = row.get("table_name")
    if not table or str(row.get("source_type") or "Table").lower() != "table":
        return None
    return {"on_table": str(table)}


def _refine_share(row: dict) -> dict | None:
    """An inbound share and an outbound one break in opposite directions."""
    direction = str(row.get("kind") or "").strip().upper()
    if not direction:
        return None
    detail = direction
    consumer = str(row.get("to") or "").strip()
    if consumer:
        detail += f" to {consumer}"
    database = str(row.get("database_name") or "").strip()
    if database:
        detail += f" on {database}"
    if direction == "INBOUND":
        return {"reason": _SHARE_INBOUND_REASON, "detail": detail}
    return {"detail": detail}


_FAILOVER_GROUP_REASON = (
    "a failover group is the account's disaster-recovery contract: the "
    "objects it lists can be promoted in the secondary account if this one "
    "fails. It does not follow the migration, so from cutover the migrated "
    "data has no failover at all until an OCI-side recovery design exists -- "
    "and a failover exercised after cutover promotes a Snowflake copy that "
    "stopped changing the day the target took over.")


def _refine_group(row: dict) -> dict | None:
    """SHOW REPLICATION GROUPS lists replication AND failover groups, told
    apart by `type`. One read, so a failover group is counted once; reading
    SHOW FAILOVER GROUPS as well would count it twice."""
    kind = str(row.get("type") or "").strip().upper()
    if not kind:
        return None
    detail = f"type={kind}"
    for field in ("object_types", "allowed_accounts", "replication_schedule"):
        value = str(row.get(field) or "").strip()
        if value:
            detail += f" {field}={value.replace(' ', '')}"
    if kind == "FAILOVER":
        return {"kind": "FAILOVER_GROUP", "reason": _FAILOVER_GROUP_REASON,
                "detail": detail}
    return {"detail": detail}


def _refine_function(row: dict) -> dict | None:
    """A scalar UDF, a UDTF and an external function are three verdicts.

    `IS_EXTERNAL` and `API_INTEGRATION` are columns of
    INFORMATION_SCHEMA.FUNCTIONS (live-verified 2026-09-22; the documented
    name `IS_EXTERNAL_FUNCTION` is rejected as an invalid identifier, which is
    the failure the degraded path below exists for). There is no
    `IS_TABLE_FUNCTION` column: a
    UDTF is identified by its return type, which `DATA_TYPE` reports as
    `TABLE (...)`. If none of the three is present the row is a FUNCTION, which
    is what it was before.
    """
    if str(row.get("IS_EXTERNAL") or "").strip().upper() in ("YES", "Y", "TRUE"):
        api = str(row.get("API_INTEGRATION") or "").strip()
        detail = str(row.get("ARGUMENT_SIGNATURE") or "")
        if api:
            detail = f"{detail} api_integration={api}".strip()
        return {"kind": "EXTERNAL_FUNCTION",
                "reason": _EXTERNAL_FUNCTION_REASON,
                "detail": detail,
                "language": None,
                "effort": _EXTERNAL_FUNCTION_VERDICT["effort"],
                "aidp_path": _EXTERNAL_FUNCTION_VERDICT["aidp_path"]}
    if str(row.get("DATA_TYPE") or "").strip().upper().startswith("TABLE"):
        return {"kind": "UDTF", "reason": _UDTF_REASON}
    return None


# The statements that put rows in a table, matched over CODE only (literals,
# quoted identifiers and comments blanked), then the target is parsed from the
# raw text at the same offset so a quoted name keeps its case. COPY INTO
# @stage and COPY INTO 's3://...' are unloads: no identifier follows, so they
# are not writes. What a CALLed procedure writes is not on the row at all.
_WRITE_VERB = re.compile(
    r"\b(?:insert\s+(?:overwrite\s+)?into|merge\s+into|copy\s+into"
    r"|delete\s+from|update|truncate(?:\s+table)?(?:\s+if\s+exists)?"
    r"|create\s+(?:or\s+replace\s+)?(?:(?:local|global)\s+)?"
    r"(?:transient\s+|temporary\s+|temp\s+|volatile\s+)?table"
    r"(?:\s+if\s+not\s+exists)?)(?=\s)", re.IGNORECASE)
_IDENT_PART = r'(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)'
# The gap after the verb is skipped in the RAW text: the mask blanks a quoted
# name or a literal to spaces, and skipping it there runs straight across it.
_TARGET = re.compile(rf"\s+({_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}})")
_PART = re.compile(_IDENT_PART)
# A word in target position that is not a table: MERGE's `THEN UPDATE SET`,
# and `IDENTIFIER($var)`, whose table is a runtime value.
_NOT_A_TABLE = {"SET", "IDENTIFIER"}


def _part(text: str) -> str:
    if text.startswith('"'):
        return text[1:-1].replace('""', '"')
    return text.upper()


def written_tables(body: str, db: str, schema: str) -> list[str]:
    """Fully-qualified tables a pipe or task body writes, in first-seen order.

    An unqualified name resolves against the object's OWN database and schema
    -- live-verified 2026-09-25: a task run from a session with no current
    database wrote the table in the task's schema. Empty means none could be
    read from the body, not that the body writes nothing.
    """
    body = body or ""
    mask = lexer.code_only(body)
    found: list[str] = []
    for m in _WRITE_VERB.finditer(mask):
        target = _TARGET.match(body, m.end())
        if not target:
            continue
        parts = [_part(p) for p in _PART.findall(target.group(1))]
        if len(parts) == 1 and not target.group(1).startswith('"') \
                and parts[0] in _NOT_A_TABLE:
            continue
        full = ".".join([db, schema][:3 - len(parts)] + parts)
        if full not in found:
            found.append(full)
    return found


def name_at(body: str, pos: int) -> tuple[list[str], int] | None:
    """The one- to three-part object name that starts after whitespace at
    `pos` in `body`, as (parts, end) -- quoted parts keep their case, bare
    ones fold -- or None when no name stands there. The reading
    `written_tables` applies to a write target."""
    target = _TARGET.match(body, pos)
    if not target:
        return None
    return [_part(p) for p in _PART.findall(target.group(1))], target.end()


def _task_name(text: str, db: str, schema: str) -> str:
    """A predecessor as SHOW TASKS spells it -> DB.SCHEMA.TASK.

    Each part may be double-quoted (case kept) or bare (folded). A bare or
    two-part name resolves in the task's OWN database and schema, the rule
    `written_tables` follows for a body.
    """
    parts = [_part(p) for p in _PART.findall(str(text))]
    if not parts or len(parts) > 3:
        raise ValueError(f"not a task name: {text!r}")
    return ".".join([db, schema][:3 - len(parts)] + parts)


def _predecessors(value, db: str, schema: str) -> list[str]:
    """SHOW TASKS `predecessors`: a JSON array of names, held in a STRING
    (live 2026-09-29: "[]" on a root task). Raises when it is not one --
    the caller keeps the raw text rather than reading it as "no parents"."""
    items = value if isinstance(value, list) else json.loads(str(value))
    if not isinstance(items, list):
        raise ValueError(f"predecessors is not a JSON array: {value!r}")
    return [_task_name(i, db, schema) for i in items]


# Facts that are object BODIES: a task's SQL, a dynamic table's query. A
# body can carry literals -- a COPY INTO's
# CREDENTIALS, an EXECUTE IMMEDIATE's password -- and the census lands in
# plan.json/inventory.json, which `provision` uploads to the workspace. So
# they are kept only with --capture-definitions, as every other body is.
_BODY_FACTS = ("definition", "text")


def _source_facts(spec: dict, row: dict, db: str | None, schema: str,
                  identifier: str, notes: list[str], *,
                  include_bodies: bool = False) -> dict | None:
    """What a generated job needs, copied from the row the census already
    read -- no statement is added for it. Only fields the row CARRIES are
    kept: an absent column stays absent rather than reading as "none". A
    body (`_BODY_FACTS`) is kept only with `include_bodies`; without it
    `body_captured: false` says so, and a generated job names the flag."""
    keep = spec.get("facts")
    if not keep:
        return None
    facts = {f: _iso(row[f]) for f in keep if f in row
             and (include_bodies or f not in _BODY_FACTS)}
    if not include_bodies and any(f in row for f in keep
                                  if f in _BODY_FACTS):
        facts["body_captured"] = False
    if "predecessors" in facts and db is not None:
        raw = facts.pop("predecessors")
        if raw in (None, ""):
            facts["predecessors"] = []
        else:
            try:
                facts["predecessors"] = _predecessors(raw, db, str(schema))
            except (ValueError, TypeError) as exc:
                # Not "no parents": a child read as a root would be
                # generated as a job with its own schedule.
                facts["predecessors_unread"] = raw
                notes.append(f"{spec['kind']} {identifier}: predecessors "
                             f"not readable ({str(exc)[:160]}), so its place "
                             f"in the task graph is unknown")
    return facts


# Each kind: where to read it, how to name it, and why it cannot migrate here.
# `facts` names the SHOW columns a generated AIDP job needs (see
# target/generated_jobs.py); they are kept on the entry as `source_facts`.
KINDS: tuple[dict, ...] = (
    {"kind": "PROCEDURE", "source": "information_schema",
     "relation": "procedures", "name_col": "PROCEDURE_NAME",
     "schema_col": "PROCEDURE_SCHEMA", "lang_col": "PROCEDURE_LANGUAGE",
     "sig_col": "ARGUMENT_SIGNATURE", "owner_col": "PROCEDURE_OWNER",
     "reason": "stored procedures are code, not structure. AIDP has no "
               "procedure object; the workload moves to a Job or a notebook "
               "and the body has to be rewritten."},
    {"kind": "FUNCTION", "source": "information_schema",
     "relation": "functions", "name_col": "FUNCTION_NAME",
     "schema_col": "FUNCTION_SCHEMA", "lang_col": "FUNCTION_LANGUAGE",
     "sig_col": "ARGUMENT_SIGNATURE", "owner_col": "FUNCTION_OWNER",
     "extra_cols": ("DATA_TYPE", "IS_EXTERNAL", "API_INTEGRATION"),
     "sub_kinds": ("UDTF", "EXTERNAL_FUNCTION"), "refine": _refine_function,
     "reason": "a UDF is code. Spark UDFs exist but the handler contract "
               "differs, so the body has to be reworked rather than copied."},
    {"kind": "SEQUENCE", "source": "information_schema",
     "relation": "sequences", "name_col": "SEQUENCE_NAME",
     "schema_col": "SEQUENCE_SCHEMA",
     "reason": "Delta has no sequence object. Surrogate keys need a different "
               "strategy -- identity columns, a hash, or generation upstream "
               "-- and the choice changes the data."},
    {"kind": "STAGE", "source": "information_schema",
     "relation": "stages", "name_col": "STAGE_NAME",
     "schema_col": "STAGE_SCHEMA",
     "extra_cols": ("STAGE_TYPE", "STAGE_URL", "STAGE_REGION"),
     "refine": _refine_stage,
     "reason": "a stage points at storage and carries credentials. The AIDP "
               "equivalent is an object-storage location plus a credential in "
               "the credential store; neither is inferable from here."},
    {"kind": "FILE_FORMAT", "source": "information_schema",
     "relation": "file_formats", "name_col": "FILE_FORMAT_NAME",
     "schema_col": "FILE_FORMAT_SCHEMA",
     "reason": "file formats are named parse options for COPY. On AIDP the "
               "same options become Spark reader options at each read site."},
    {"kind": "PIPE", "source": "information_schema",
     "relation": "pipes", "name_col": "PIPE_NAME",
     "schema_col": "PIPE_SCHEMA",
     # DEFINITION is `COPY INTO <table> FROM @stage`: the table it loads.
     "extra_cols": ("DEFINITION",), "writes_col": "DEFINITION",
     "degraded_note": "the pipe definitions were not readable, so the "
                      "table each pipe loads is not named",
     "reason": "Snowpipe is continuous ingestion. It has no AIDP object; it "
               "becomes a streaming job or a scheduled load, which is an "
               "architecture decision (see the data-movement options)."},
    {"kind": "TASK", "source": "show", "relation": "tasks",
     "writes_col": "definition",
     "facts": ("schedule", "predecessors", "condition", "warehouse", "state",
               "definition", "allow_overlapping_execution",
               "task_relations"),
     "reason": "a task is a scheduler. AIDP Jobs are the equivalent, but the "
               "schedule, dependencies and body all have to be re-expressed. "
               "**A task that populates a migrated table means that table "
               "stops being populated after cutover.**"},
    {"kind": "ALERT", "source": "show", "relation": "alerts",
     "reason": "an alert is a condition on a schedule plus the action it "
               "takes. Nothing runs it on AIDP, so an alert watching a table "
               "that migrates stops firing at cutover -- and the first sign of "
               "that is the incident it existed to catch, arriving unannounced."},
    {"kind": "SECRET", "source": "show", "relation": "secrets",
     "reason": "a secret holds the credential a stage, pipe or external "
               "function authenticates with, and its value cannot be read out "
               "of Snowflake by anyone, including this census. Every consumer "
               "that depends on it has to be re-credentialled from the "
               "original source into the AIDP credential store, or it "
               "authenticates against nothing and fails on first use."},
    {"kind": "NETWORK_RULE", "source": "show", "relation": "network rules",
     "reason": "a network rule is the list of addresses an integration or "
               "policy is allowed to reach. It is configuration, not data: the "
               "OCI equivalent is a security list, NSG or private endpoint, "
               "written by whoever owns the tenancy. Until it exists the "
               "migrated workload either cannot reach the remote host at all, "
               "or reaches hosts Snowflake was keeping it away from."},
    {"kind": "STREAMLIT", "source": "show", "relation": "streamlits",
     "reason": "a Streamlit app is a user-facing application whose code lives "
               "in Snowflake and runs on a warehouse. AIDP has no equivalent "
               "runtime, so at cutover the app stops existing and everyone who "
               "opened it daily has nothing to open."},
    {"kind": "NOTEBOOK", "source": "show", "relation": "notebooks",
     "reason": "a Snowflake notebook is cells plus a warehouse and a Snowpark "
               "session. AIDP has notebooks, but none of that binding is "
               "portable, so nothing carries across on its own: each notebook "
               "is rewritten against Spark, and any of them that someone runs "
               "on a schedule stops running meanwhile."},
    {"kind": "SERVICE", "source": "show", "relation": "services",
     "reason": "a Snowpark Container Service runs a container image next to "
               "the data, and something calls its endpoint. AIDP has no "
               "container runtime, so the service and every caller stop at "
               "cutover; rehosting the image is an OCI decision (Container "
               "Instances or OKE) outside this plugin."},
    {"kind": "STREAM", "source": "show", "relation": "streams",
     "refine": _refine_stream,
     "facts": ("table_name", "mode", "source_type", "type", "stale"),
     "reason": "a stream is CDC state. Delta Change Data Feed is the nearest "
               "equivalent, but stream offsets do not transfer, so consumers "
               "restart from a new baseline."},
    {"kind": "MATERIALIZED_VIEW", "source": "show", "relation": "materialized views",
     "facts": ("text", "invalid", "invalid_reason"),
     "reason": "Snowflake maintains materialized views automatically. AIDP has "
               "no equivalent. Its current contents migrate as a table "
               "snapshot (PLANNED_OBJECTS.md); what does not migrate is the "
               "maintenance -- the refresh is a generated job, created "
               "unscheduled, where its query translates, and the customer "
               "then owns it."},
    {"kind": "DYNAMIC_TABLE", "source": "show", "relation": "dynamic tables",
     "facts": ("target_lag", "refresh_mode", "text", "warehouse",
               "scheduling_state"),
     "reason": "a dynamic table is a declarative pipeline with a target "
               "lag. AIDP has no equivalent object. Its current contents "
               "migrate as a table snapshot (PLANNED_OBJECTS.md); what does "
               "not migrate is the pipeline -- the refresh is a generated "
               "job, created unscheduled, whose cadence must be chosen "
               "deliberately (the target lag is recorded, not applied)."},

    # Account-scoped. Read once per run, not once per database: these objects
    # do not live in a database, and asking per database would count each of
    # them as many times as there are databases in scope.
    {"kind": "SHARE", "source": "show", "relation": "shares",
     "scope": "account", "refine": _refine_share,
     "reason": "a share is a live contract with another account: a consumer "
               "is reading through it right now. Nothing in AIDP replays it, "
               "so at cutover the consumer's queries keep working against a "
               "source that has stopped moving, and then start failing -- and "
               "they find out before you do unless every share on this list "
               "has been worked through with its owner first."},
    {"kind": "ROLE", "source": "show", "relation": "roles",
     "scope": "account",
     "reason": "roles are the access model. No role, grant or hierarchy is "
               "replayed on AIDP -- it is a separate model with its own "
               "per-resource permissions -- so the target starts with only "
               "what the migration account created, and every privilege has "
               "to be re-granted deliberately from this list rather than "
               "inherited."},
    {"kind": "NETWORK_POLICY", "source": "show", "relation": "network policies",
     "scope": "account",
     "reason": "a network policy is the account's ingress allowlist: it "
               "decides who may connect at all. It does not travel. The "
               "equivalent is OCI network control (NSG, security list, "
               "private endpoint) configured by whoever owns the tenancy, and "
               "until that is done the target is reachable on terms Snowflake "
               "was restricting."},
    {"kind": "APPLICATION", "source": "show", "relation": "applications",
     "scope": "account",
     "reason": "a native app is third-party code installed into the account "
               "and versioned by its provider. There is nothing here to "
               "translate and nothing to copy: whatever the app was doing has "
               "to be sourced again on AIDP or done without, and that is a "
               "procurement question, not a migration step."},
    {"kind": "REPLICATION_GROUP", "source": "show",
     "relation": "replication groups", "scope": "account",
     "sub_kinds": ("FAILOVER_GROUP",), "refine": _refine_group,
     "reason": "a replication group copies databases -- and sometimes roles, "
               "warehouses and integrations -- to another Snowflake account "
               "on a schedule. Nothing replicates the AIDP target: after "
               "cutover the secondary account holds a copy of a source that "
               "has stopped changing, and whatever reads that secondary is "
               "reading a frozen estate. The target's own replication "
               "(OCI Object Storage replication, a second region) is a "
               "separate decision this plugin does not make."},
    {"kind": "COMPUTE_POOL", "source": "show", "relation": "compute pools",
     "scope": "account",
     "reason": "a compute pool is the node pool the container services above "
               "run on. AIDP has no equivalent, so it is listed to size what "
               "rehosting those services on OCI would need -- and to show "
               "that the bill for them does not disappear with the warehouse "
               "bill."},
)


def _iso(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _select(run_sql, db: str, spec: dict, cols: list[str]) -> list[dict]:
    return run_sql(
        f'select {", ".join(cols)} from {lexer.qualify(db)}.information_schema.'
        f'{spec["relation"]} order by 1')


def _read_information_schema(run_sql, db: str, spec: dict
                             ) -> tuple[list[dict], bool]:
    """Rows, and whether the discriminating columns had to be given up.

    `extra_cols` are what tells an EXTERNAL stage from an INTERNAL one and a
    UDTF from a scalar UDF. They are documented columns, but a column name
    this plugin cannot confirm against the account it is pointed at must not
    cost the whole kind: if the wider select fails, the census re-asks for the
    columns it has always read and says it lost the distinction. One extra
    statement, and only on the failing path.
    """
    cols = [spec["name_col"], spec["schema_col"]]
    for key in ("lang_col", "sig_col", "owner_col"):
        if spec.get(key):
            cols.append(spec[key])
    extra = [c for c in (spec.get("extra_cols") or ())]
    if extra:
        try:
            return _select(run_sql, db, spec, cols + extra), False
        except Exception:
            pass
    return _select(run_sql, db, spec, cols), bool(extra)


def _read_show(run_sql, db: str | None, spec: dict
               ) -> tuple[list[dict], str | None]:
    """Rows, and why they are capped (None when complete).

    One bare SHOW stops at 10,000 rows and still succeeds, so an account
    with more roles, or a database with more tasks, streams or tags, read as
    "10000 found / yes". `show_paged` pages past the cap where that is exact
    and otherwise says the count is capped.
    """
    if db is None:                      # account-scoped: no IN DATABASE
        return show_paged(run_sql, f'show {spec["relation"]}')
    return show_paged(
        run_sql, f'show {spec["relation"]} in database {lexer.qualify(db)}')


def _role_text(role: str | None, secondary: list[str] | None = None) -> str:
    """How to name the authority a count was produced under.

    Naming only the primary role is wrong whenever secondary roles are
    active: the read had their privileges too, so a reader who takes the
    sentence at face value concludes a restricted role is sufficient when it
    is not.
    """
    base = f"role `{role}`" if role else "the current role"
    if secondary:
        return (base + " **plus secondary role(s) "
                + ", ".join(f"`{r}`" for r in secondary)
                + "**, whose privileges these reads also had")
    return base


def _visibility_note(role: str | None,
                     secondary: list[str] | None = None) -> str:
    lines = [
        f"**Counted as visible to {_role_text(role, secondary)}.** "
        "Snowflake's SHOW and "
        "INFORMATION_SCHEMA return only the objects the current role holds a "
        "privilege on, and a statement that returns nothing still succeeds -- "
        "so every count below is a lower bound, and a zero means *none "
        "visible*, not *none exist*. A complete census needs (per Snowflake "
        "documentation; confirm against `SHOW GRANTS` in your account):", ""]
    lines += [f"- {what}: {grant}" for what, grant in VISIBILITY_GRANTS]
    return "\n".join(lines)


def _summary(count: int, readable: bool, scope: str, note: str,
             role: str | None, *, denied: list[str] | None = None,
             answered: int = 0, secondary: list[str] | None = None) -> dict:
    """One row of the counts table.

    `count` is None only when NOBODY looked. Where some databases answered
    and others were denied, the count is what was actually seen -- hiding it
    would contradict the object table it came from -- and the note says which
    databases are missing from it.
    """
    denied = list(denied or [])
    if denied and answered:
        return {
            "count": count,
            "readable": False,
            "unread": "partial",
            "denied_databases": denied,
            "scope": scope,
            "note": (f"{count} found in the database(s) that answered; "
                     f"not visible to {_role_text(role, secondary)} in "
                     f'{", ".join(denied)}, so this is a lower bound'),
        }
    if readable and not count:
        note = (f"0 visible to {_role_text(role, secondary)}; a lower "
                f"bound, not a total")
    return {"count": count if readable else None,
            "readable": readable,
            "unread": None if readable else "denied",
            "denied_databases": denied,
            "scope": scope,
            "note": note or f"{count} found"}


def _not_distinguishable(parent: str, scope: str) -> dict:
    """Read and counted, just not told apart -- which is not the same as 0."""
    return {"count": None, "readable": False, "unread": "degraded",
            "scope": scope,
            "note": (f"counted under {parent}: the detail columns that tell "
                     f"them apart were not readable, so this is not "
                     f"distinguishable rather than none")}


def secondary_roles_active(value) -> list[str]:
    """The roles in effect BESIDES the current one, from
    `CURRENT_SECONDARY_ROLES()`.

    Snowflake returns a JSON object: `{"roles":"A,B","value":"ALL"}` when
    secondary roles are active, and an empty `roles` when they are not. Any
    shape this cannot read yields no roles rather than a guess -- an empty
    list here means "none named", and the caller must not read it as "none
    active" when the field was missing entirely.
    """
    if not value:
        return []
    text = str(value)
    if text.strip().startswith("{"):
        try:
            text = json.loads(text).get("roles", "")
        except (ValueError, AttributeError):
            return []
    return [r.strip() for r in str(text).split(",") if r.strip()]


def _role_text(role: str | None, secondary: list[str] | None = None) -> str:
    """The authority a count was produced under. Naming only the primary role
    is wrong whenever secondary roles are active: the read had theirs too."""
    base = f"role `{role}`" if role else "the current role"
    if secondary:
        return (base + " **plus secondary role(s) "
                + ", ".join(f"`{r}`" for r in secondary)
                + "**, whose privileges these reads also had")
    return base


def build_census(run_sql: Callable[..., list[dict]], databases: list[str], *,
                 include_definitions: bool = False,
                 role: str | None = None,
                 secondary_roles: list[str] | None = None) -> dict:
    notes: list[str] = []
    kinds: dict[str, dict] = {}
    objects: list[dict] = []
    secondary = list(secondary_roles or [])

    for spec in KINDS:
        kind = spec["kind"]
        scope = spec.get("scope", "database")
        sub_kinds = tuple(spec.get("sub_kinds") or ())
        tally: dict[str, int] = {k: 0 for k in (kind,) + sub_kinds}
        # Per database, because a role's privileges are. One denial used to
        # null the count for the whole kind, including rows already counted
        # in the databases that did answer.
        denied: list[str] = []
        answered = 0
        degraded = False
        capped: list[str] = []
        note = ""
        # An account-scoped kind is read once. `None` is the "no database"
        # target, not a database named None.
        for db in ([None] if scope == "account" else list(databases)):
            try:
                if spec["source"] == "information_schema":
                    rows, degraded_here = _read_information_schema(
                        run_sql, db, spec)
                    degraded = degraded or degraded_here
                else:
                    rows, cap = _read_show(run_sql, db, spec)
                    if cap:
                        capped.append(db if db else "account")
                        where = f"in {db}" if db else "account-scoped read"
                        notes.append(f"{kind} {where}: {cap}")
            except Exception as exc:
                denied.append(db if db else "account")
                note = str(exc)[:200]
                where = f"in {db}" if db else "account-scoped read"
                notes.append(f"{kind} {where}: {note}")
                continue
            answered += 1
            for row in rows:
                entry = _entry(kind, spec, db, row,
                               include_definitions=include_definitions,
                               notes=notes)
                objects.append(entry)
                tally[entry["kind"]] = tally.get(entry["kind"], 0) + 1
        readable = not denied
        kinds[kind] = _summary(tally[kind], readable, scope, note, role,
                               denied=denied, answered=answered,
                               secondary=secondary)
        if capped:
            # Not a privilege gap: a role that sees everything would get the
            # same truncated answer, so the note must not promise grants fix it.
            kinds[kind]["capped"] = True
            kinds[kind]["note"] += (
                f"; stopped at the {SHOW_PAGE_SIZE:,}-row SHOW cap in "
                f'{", ".join(capped)} and could not be paged, so this is a '
                f"lower bound whatever the role's grants")
        if readable and degraded:
            kinds[kind]["note"] += "; " + (
                spec.get("degraded_note")
                or f"the detail columns were not readable, so every row is "
                   f"reported as a plain {kind.lower().replace('_', ' ')}")
        for sub in sub_kinds:
            # A sub-kind exists only while the deciding column can be read.
            # Without it these rows are still counted -- under the parent kind
            # -- so reporting 0 here would claim there are none of them, when
            # the truth is that they are not currently distinguishable.
            kinds[sub] = (
                _not_distinguishable(kind, scope) if readable and degraded
                else _summary(tally[sub], readable, scope, note, role,
                              denied=denied, answered=answered,
                              secondary=secondary))

    by_language = collections.Counter(
        o["language"] for o in objects if o.get("language"))
    by_kind = collections.Counter(o["kind"] for o in objects)
    by_effort = collections.Counter(
        o["effort"] for o in objects if o.get("effort"))

    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "databases_in_scope": list(databases),
        "kinds": kinds,
        "objects": objects,
        "total": len(objects),
        "by_kind": dict(by_kind),
        "by_language": dict(by_language),
        "by_effort": dict(by_effort),
        "unreadable": notes,
        "role": role,
        "secondary_roles": secondary,
        "completeness": "visible-to-role",
        "visibility_note": _visibility_note(role, secondary),
        "scope_statement": _scope_statement(len(objects), by_kind, kinds, role, secondary),
    }


def _entry(kind: str, spec: dict, db: str | None, row: dict, *,
           include_definitions: bool, notes: list[str] | None = None) -> dict:
    """One census row. Never raises on what the row CONTAINS.

    The refine hook and the body scan read free text a scanner may not
    understand -- a `// don't` comment in a task body used to raise out of
    here, past the per-database try, and take `assess` down with no artifact
    written. Either failure now costs that one object its refinement or its
    linkage; the object is still counted, and `notes` (the census's
    `unreadable` list) names it with the error.
    """
    notes = notes if notes is not None else []
    if spec["source"] == "information_schema":
        name = row.get(spec["name_col"])
        schema = row.get(spec["schema_col"])
    else:
        name = row.get("name")
        schema = row.get("schema_name") or row.get("database_name") or "?"

    # An account object has no database and no schema, and inventing one --
    # "None.None.MY_SHARE" -- would read as a real path to a real place.
    identifier = (str(name) if db is None else f"{db}.{schema}.{name}")

    entry = {
        "kind": kind,
        "source_identifier": identifier,
        "detail": "",
        "migratable": False,          # never true here, by construction
        "reason": spec["reason"],
        "language": None,
        "effort": None,
        "aidp_path": None,
    }

    if spec.get("sig_col") and row.get(spec["sig_col"]):
        entry["detail"] = str(row[spec["sig_col"]])
    elif spec["source"] == "show":
        # Labelled by the field it came from. `state=1 day` for a dynamic
        # table's target lag and `state=EGRESS` for a network rule's mode
        # were both wrong under the one label.
        for field in ("state", "target_lag", "mode"):
            if row.get(field):
                entry["detail"] = f"{field}={row[field]}"
                break
        else:
            entry["detail"] = ""

    language = None
    unknown_language = False
    if spec.get("lang_col"):
        language = str(row.get(spec["lang_col"]) or "").upper() or None
        verdict = LANGUAGE_VERDICTS.get(language, _UNKNOWN_LANGUAGE)
        entry["language"] = language
        entry["effort"] = verdict["effort"]
        entry["aidp_path"] = verdict["aidp_path"]
        unknown_language = verdict is _UNKNOWN_LANGUAGE

    # The row may not be the kind the spec assumed. `refine` narrows it and is
    # allowed to overrule the language verdict, because an external function
    # has no handler language to have a verdict about.
    try:
        narrowed = (spec.get("refine") or (lambda _row: None))(row) or {}
    except Exception as exc:
        # Refinement only ever narrows, so without it the row keeps the kind
        # and reason it would have had with the deciding column absent.
        narrowed = {}
        notes.append(f"{kind} {identifier}: not refined "
                     f"({str(exc)[:200]}); reported as a plain "
                     f"{kind.lower().replace('_', ' ')}")
    entry.update(narrowed)

    # Appended after the narrowing so it lands on the reason the object
    # actually got -- and not at all when `refine` said there is no handler
    # language here, where "not recognised" would be the wrong complaint.
    if unknown_language and "language" not in narrowed:
        entry["reason"] = (
            f'{entry["reason"]} Handler language {language!r} was not '
            f"recognised, so no path is proposed.")

    # The table a pipe or task fills. Named, so the plan can say which
    # migrating table stops being populated at cutover; absent, not empty,
    # when the body names none this can read.
    if spec.get("writes_col") and db is not None and row.get(spec["writes_col"]):
        try:
            writes = written_tables(str(row[spec["writes_col"]]), db,
                                    str(schema))
        except Exception as exc:
            # Not "writes none": the body could not be scanned, so which
            # table it fills is unknown, and the detail says so.
            writes = []
            entry["detail"] = (f'{entry["detail"]} '
                               f'writes not determined').strip()
            notes.append(f"{kind} {identifier}: body not scannable "
                         f"({str(exc)[:200]}), so the table(s) it writes are "
                         f"not determined; the object is still counted")
        if writes:
            entry["writes"] = writes
            entry["detail"] = (f'{entry["detail"]} '
                               f'writes={",".join(writes)}').strip()

    # A task's schedule and graph, a dynamic table's lag, a stream's base
    # table: always kept. A task's body and a dynamic table's query only
    # with --capture-definitions (see _BODY_FACTS); a job generated without
    # one says which flag keeps it. (A materialized view's query is also on
    # SHOW VIEWS, which the inventory keeps as the view text.)
    facts = _source_facts(spec, row, db, str(schema), identifier, notes,
                          include_bodies=include_definitions)
    if facts is not None:
        entry["source_facts"] = facts

    if include_definitions:
        for key in ("PROCEDURE_DEFINITION", "FUNCTION_DEFINITION", "text",
                    "definition", "DEFINITION"):
            if row.get(key):
                entry["definition"] = str(row[key])
                break
    return entry


def _scope_statement(total: int, by_kind, kinds: dict,
                     role: str | None = None,
                     secondary: list[str] | None = None) -> str:
    denied = [k for k, v in kinds.items() if v.get("unread") == "denied"]
    indistinct = [k for k, v in kinds.items() if v.get("unread") == "degraded"]
    capped = [k for k, v in kinds.items() if v.get("capped")]
    who = _role_text(role, secondary)
    if total == 0 and not denied:
        # Every statement succeeded and returned nothing. With a minimal
        # read-only role that is the EXPECTED result on an estate full of
        # tasks and procedures, so it must not become a claim of completeness.
        return (f"**No procedures, UDFs, tasks, streams, materialized or "
                f"dynamic tables, stages, pipes, sequences, file formats, "
                f"alerts, secrets, network rules, Streamlit apps, notebooks "
                f"or services -- and no share, role, network policy, "
                f"application, compute pool, replication or failover group "
                f"in the account -- "
                f"were visible to {who}.** SHOW and INFORMATION_SCHEMA return "
                f"only the objects the role holds a privilege on, so this zero "
                f"means *none visible*, not *none exist*, and it is a lower "
                f"bound. Do not treat the migratable count as the size of the "
                f"estate until a role that can see these kinds has run the census "
                f"— CENSUS.md lists the grants.")
    parts = ", ".join(f"{n} {k.lower().replace('_', ' ')}(s)"
                      for k, n in sorted(by_kind.items()))
    text = (f"**{total} object(s) in this estate cannot be migrated by this "
            f"plugin**: {parts}. They are code, schedulers or storage "
            f"definitions rather than structure, so the migratable count "
            f"covers tables and views only — it is not the size of the "
            f"estate. Counted as visible to {who}: SHOW and INFORMATION_SCHEMA "
            f"are privilege-filtered, so {total} is a lower bound.")
    if denied:
        text += (f" **{', '.join(denied)} could not be read**, so even this "
                 f"count is a floor, not a total.")
    if capped:
        text += (f" **{', '.join(capped)} stopped at the SHOW result cap** "
                 f"and could not be paged, so those counts are a floor that "
                 f"no grant would raise.")
    if indistinct:
        text += (f" {', '.join(indistinct)} could not be told apart from the "
                 f"kind they are counted under, so they are reported as *not "
                 f"distinguishable*, not as none.")
    return text
