"""What protects the data today, and what arrives without it. Read-only.

The only gap in this plugin with a data-EXPOSURE consequence. A Snowflake
column carrying a masking policy lands on AIDP as plain Delta with no policy,
so values that were masked for most roles become readable by anyone who can
read the table. Same for a row access policy: the row filter simply is not
there. Nothing in the plugin said so, and the properties were being scrubbed
silently.

REPORTS; CHANGES NOTHING, and generates no equivalent. AIDP's answer is
restricted views plus ontology sensitivity -- there is no masking REST API --
so a mechanical translation is not available even in principle, and a
half-right one would be worse than a stated gap.

The dangerous failure mode here is a FALSE NEGATIVE, and it has two sources.
If ACCOUNT_USAGE cannot be read we cannot prove the absence of policies, so
`exposure_count` is None and the statement says the question is unanswered.
And ACCOUNT_USAGE.POLICY_REFERENCES -- the only attachment source -- lags up
to ~2 hours, so a policy attached shortly before the run is not in it yet
while SHOW MASKING/ROW ACCESS POLICIES already lists the policy object. When
policy objects exist and no attachment is visible, the verdict is
UNCONFIRMED, not clean. Neither case may read as "no policies found".

There is a third source, and it is the worst one: naming a policy kind the
tool never enumerated. The summary sentence used to state that no masking,
row-access, AGGREGATION or PROJECTION policy was attached while the last two
were never asked for, and the staleness tripwire summed only masking +
row_access, so a defined-but-unattached aggregation policy could not raise
UNCONFIRMED. All four kinds are now enumerated, all four feed the tripwire,
and the sentence is built from the kinds that actually answered -- a kind
whose SHOW was denied is named as "not visible to this role", never counted
as zero and never covered by a clean verdict.

Tag attachments (ACCOUNT_USAGE.TAG_REFERENCES) and grants beyond
TABLE / VIEW / MATERIALIZED_VIEW are read for the same reason: a tag count
with no attachment list is a number with no verdict, and an access model
missing every schema, database, warehouse, stage, procedure and function
grant cannot be reconstructed on the target. Both are reported; neither is
replayed.
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect import lexer
from .catalog import SHOW_PAGE_SIZE, show_paged

__all__ = ["build_security", "POLICY_CONSEQUENCE", "POLICY_KINDS",
           "POLICY_KIND_LABELS", "GRANT_CLASSES"]

# The policy kinds this module enumerates as OBJECTS, in report order. The
# summary sentence is built from this tuple and from which of them actually
# answered, so it can never name a kind that was not asked for.
POLICY_KINDS = ("masking", "row_access", "aggregation", "projection")

POLICY_KIND_LABELS = {
    "masking": "masking",
    "row_access": "row-access",
    "aggregation": "aggregation",
    "projection": "projection",
}

# The SHOW behind each kind. All four are on the read-only allowlist.
_POLICY_SHOW = {
    "masking": "masking policies",
    "row_access": "row access policies",
    "aggregation": "aggregation policies",
    "projection": "projection policies",
}

POLICY_CONSEQUENCE = {
    "MASKING_POLICY": (
        "the column arrives UNMASKED. Every role that can read the target "
        "table sees the raw value, including values Snowflake was masking for "
        "most roles."),
    "ROW_ACCESS_POLICY": (
        "the row filter is not carried over, so the target table exposes ALL "
        "rows to every role that can read it, not the subset each role could "
        "see in Snowflake."),
    "AGGREGATION_POLICY": (
        "the aggregation constraint is not carried over, so queries that "
        "Snowflake would have forced to aggregate can return individual rows."),
    "PROJECTION_POLICY": (
        "the projection constraint is not carried over, so columns Snowflake "
        "prevented from being selected become selectable."),
}

_GENERIC_CONSEQUENCE = (
    "the policy is not carried over, so whatever it restricted is "
    "unrestricted on the target.")

_AIDP_PATH = (
    "This plugin does not translate masking policies. On AIDP the "
    "equivalent is a restricted view over the table plus ontology "
    "sensitivity classification, granted per role -- a design decision, not "
    "a translation.")

# Where attachments come from, and how stale that can be. Named in the
# artifact so a clean verdict carries its own caveat.
_ATTACHMENT_SOURCE = ("SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES (lags up to "
                      "~2 hours behind DDL)")

# Tag attachments have their own view and its own identical lag. Counting tag
# OBJECTS without reading attachments made a classification-driven governance
# model look empty, which is the same false negative as above.
_TAG_ATTACHMENT_SOURCE = ("SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES (lags up to "
                          "~2 hours behind DDL)")

# The read that does not lag. Live 2026-09-22: a masking policy, a row-access
# policy and a tag were attached to a seeded table while both ACCOUNT_USAGE
# views still returned nothing, so the report hedged (correctly) over an
# answer that was readable the whole time. <db>.INFORMATION_SCHEMA holds two
# table functions that answer per object with no lag and need no IMPORTED
# PRIVILEGES ON DATABASE SNOWFLAKE -- one round trip per object, so they are
# what decides, and the account-wide views become corroboration.
_LIVE_POLICY_SOURCE = ("<db>.INFORMATION_SCHEMA.POLICY_REFERENCES, read per "
                       "object (no lag)")
_LIVE_TAG_SOURCE = ("<db>.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS, read "
                    "per object (no lag)")

# One round trip per in-scope object. Past this, the per-object read is
# skipped and said to be skipped: a stage that silently takes twenty minutes
# is its own failure, and the pre-existing lagging verdict still stands.
LIVE_ATTACHMENT_BUDGET = 500

_TAG_CONSEQUENCE = (
    "the tag and its value do not travel, so anything keyed off this "
    "classification -- a policy, an access rule, an audit query -- has "
    "nothing to key off on the target.")

_TAG_AIDP_PATH = (
    "This plugin does not translate tags. The nearest AIDP equivalent is "
    "ontology sensitivity classification applied per object, which is a "
    "design decision rather than a translation.")

# What GRANTS_TO_ROLES.granted_on is asked for. Reading only TABLE / VIEW /
# MATERIALIZED_VIEW made every grant on a schema, database, warehouse, stage,
# procedure or function invisible, so the target access model could not be
# reconstructed from the report. Reported, never replayed, either way.
GRANT_CLASSES = (
    "TABLE", "VIEW", "MATERIALIZED_VIEW", "EXTERNAL_TABLE", "DYNAMIC_TABLE",
    "DATABASE", "SCHEMA", "WAREHOUSE", "STAGE", "PROCEDURE", "FUNCTION",
    "FILE_FORMAT", "SEQUENCE", "STREAM", "TASK", "PIPE", "TAG",
    "MASKING_POLICY", "ROW_ACCESS_POLICY", "AGGREGATION_POLICY",
    "PROJECTION_POLICY", "INTEGRATION",
)

# Classes that have no database of their own, so a scope filter on
# DATABASE_NAME would silently drop them.
_ACCOUNT_SCOPED_CLASSES = ("WAREHOUSE", "INTEGRATION")


def _show(run_sql, what: str, db: str) -> tuple[list[dict], str | None]:
    """Rows, and why they are capped (None when complete). A bare SHOW stops
    at 10,000 rows and still succeeds; see catalog.show_paged."""
    return show_paged(run_sql, f"show {what} in database {lexer.qualify(db)}")


def _collect(run_sql, what: str, databases: list[str],
             notes: list[str]) -> dict:
    items: list[dict] = []
    readable = True
    capped: list[str] = []
    note = ""
    for db in databases:
        try:
            rows, cap = _show(run_sql, what, db)
            if cap:
                capped.append(db)
                notes.append(f"SHOW {what.upper()} in {db}: {cap}")
            for row in rows:
                items.append({
                    "name": row.get("name"),
                    "database": row.get("database_name") or db,
                    "schema": row.get("schema_name"),
                    "kind": row.get("kind") or what,
                })
        except Exception as exc:
            readable = False
            note = str(exc)[:200]
            notes.append(f"SHOW {what.upper()} in {db}: {note}")
    if readable and not items:
        # SHOW is privilege-filtered, so this zero is a statement about the
        # role. The verdict that matters comes from ACCOUNT_USAGE, which is
        # account-wide; this table is context and must not read as proof.
        note = (f"0 visible to the current role: SHOW {what.upper()} lists "
                f"only objects the role owns or holds a privilege on. The "
                f"attachment verdict comes from ACCOUNT_USAGE and is "
                f"account-wide.")
    elif not readable:
        # The count is None, and the words have to agree with it: a refused
        # SHOW means "not visible to this role", which is not a zero.
        note = (f"not visible to this role: SHOW {what.upper()} was refused "
                f"-- {note}")
    if readable and capped:
        note = (f"{len(items)} found -- stopped at the {SHOW_PAGE_SIZE:,}-row "
                f'SHOW cap in {", ".join(capped)} and could not be paged, so '
                f"this is a lower bound whatever the role's grants")
    return {"readable": readable, "note": note or f"{len(items)} found",
            "count": len(items) if readable else None, "items": items,
            "capped": bool(capped)}


def _entity_literal(db, schema, name) -> str:
    """The object as the table functions want it: a single-quoted literal
    holding a double-quoted three-part identifier.

    Both layers of quoting matter. A name holding a double quote has to reach
    Snowflake with that quote doubled and the whole name quoted, and the
    result then sits inside a SQL string literal, so its single quotes are
    doubled in turn.
    """
    return lexer.sql_literal(lexer.qualify(str(db), str(schema), str(name)))


def _live_attachments(run_sql, records: list[dict], notes: list[str], *,
                      budget: int) -> dict:
    """Policy and tag attachments read per object, with no ~2 h lag.

    Returns what was read AND whether every object answered. An object that
    could not be read is named, never dropped: a partial read that renders as
    a complete one is the failure this whole module exists to prevent (I3).
    """
    total = len(records)
    if not total:
        return {"attempted": False, "complete": False, "objects": 0,
                "probed": 0, "failed": [], "policy_attachments": [],
                "tag_attachments": [], "budget": budget,
                "reason": "no in-scope objects to read"}
    if total > budget:
        return {"attempted": False, "complete": False, "objects": total,
                "probed": 0, "failed": [], "policy_attachments": [],
                "tag_attachments": [], "budget": budget,
                "reason": (f"{total} in-scope object(s) is over the "
                           f"{budget}-object budget for a read that costs one "
                           f"round trip per object, so it was not attempted. "
                           f"The lagging account-wide view is the only source "
                           f"below. Raise the budget to read them.")}

    policies: list[dict] = []
    tags: list[dict] = []
    failed: list[dict] = []
    probed = 0
    # Two reads per object, and they fail independently: a role can hold one
    # and not the other. Counting them together let an all-denied tag read
    # ride on a successful policy read and report itself as measured.
    policy_ok = tag_ok = 0
    for rec in records:
        ident = rec["source_identifier"]
        db = rec.get("source_database")
        schema = rec.get("source_schema")
        name = ident.split(".", 2)[2] if ident.count(".") >= 2 else ident
        literal = _entity_literal(db, schema, name)
        qdb = lexer.quote_ident(str(db))
        # POLICY_REFERENCES takes the object's own domain; the tag function
        # rejects every domain but `table` (live: "Please use object type
        # TABLE for all kinds of table-like objects").
        domain = "VIEW" if rec.get("object_type") == "VIEW" else "TABLE"
        object_read = True
        try:
            rows = run_sql(
                f"select policy_name POLICY_NAME, policy_kind POLICY_KIND, "
                f"ref_column_name REF_COLUMN_NAME "
                f"from table({qdb}.information_schema.policy_references("
                f"ref_entity_name => '{literal}', "
                f"ref_entity_domain => '{domain}'))")
        except Exception as exc:
            object_read = False
            failed.append({"object": ident, "error": str(exc)[:200]})
            rows = []
        else:
            policy_ok += 1
        for r in rows:
            kind = str(r.get("POLICY_KIND") or "").upper()
            policies.append({
                "object": ident,
                "column": r.get("REF_COLUMN_NAME"),
                "policy": r.get("POLICY_NAME"),
                "policy_kind": kind,
                "source": "live",
                "severity": "HIGH",
                "consequence": POLICY_CONSEQUENCE.get(kind,
                                                      _GENERIC_CONSEQUENCE),
                "aidp_path": _AIDP_PATH,
            })
        try:
            rows = run_sql(
                f"select tag_database TAG_DATABASE, tag_schema TAG_SCHEMA, "
                f"tag_name TAG_NAME, tag_value TAG_VALUE, level LEVEL, "
                f"column_name COLUMN_NAME "
                f"from table({qdb}.information_schema."
                f"tag_references_all_columns('{literal}', 'table'))")
        except Exception as exc:
            if object_read:
                failed.append({"object": ident, "error": str(exc)[:200]})
            object_read = False
            rows = []
        else:
            tag_ok += 1
        seen_table_level: set[str] = set()
        for r in rows:
            level = str(r.get("LEVEL") or "").upper()
            tag = (f'{r.get("TAG_DATABASE")}.{r.get("TAG_SCHEMA")}.'
                   f'{r.get("TAG_NAME")}')
            # A table-level tag comes back once per column. Four columns are
            # not four findings, and the tag is not on a column at all.
            if level == "TABLE":
                if tag in seen_table_level:
                    continue
                seen_table_level.add(tag)
            tags.append({
                "source": "live",
                "object": ident,
                "column": None if level == "TABLE" else r.get("COLUMN_NAME"),
                "level": level or None,
                "domain": rec.get("object_type"),
                "tag": tag,
                "value": r.get("TAG_VALUE"),
                "consequence": _TAG_CONSEQUENCE,
                "aidp_path": _TAG_AIDP_PATH,
            })
        if object_read:
            probed += 1

    for f in failed:
        notes.append(f'per-object attachment read on {f["object"]}: '
                     f'{f["error"]}')
    return {"attempted": True, "complete": not failed, "objects": total,
            "probed": probed, "failed": failed, "budget": budget,
            "policy_read_objects": policy_ok, "tag_read_objects": tag_ok,
            "policy_complete": policy_ok == total,
            "tag_complete": tag_ok == total,
            "policy_attachments": policies, "tag_attachments": tags,
            "reason": ""}


def build_security(run_sql: Callable[..., list[dict]], inventory: dict, *,
                   include_grants: bool = True,
                   live_attachment_budget: int = LIVE_ATTACHMENT_BUDGET
                   ) -> dict:
    notes: list[str] = []
    databases = inventory.get("databases_in_scope") or []
    records = inventory.get("inventory") or []
    in_scope = {r["source_identifier"].upper() for r in records}

    policies = {k: _collect(run_sql, _POLICY_SHOW[k], databases, notes)
                for k in POLICY_KINDS}
    policies["tags"] = _collect(run_sql, "tags", databases, notes)

    # The load-bearing query: which objects and columns actually have a policy
    # attached. Policies existing is not the risk; policies ATTACHED to
    # something we are migrating is.
    exposures: list[dict] = []
    out_of_scope = 0
    references_readable = True
    try:
        refs = run_sql(
            "select ref_database_name REF_DATABASE_NAME, "
            "ref_schema_name REF_SCHEMA_NAME, ref_entity_name REF_ENTITY_NAME, "
            "ref_column_name REF_COLUMN_NAME, policy_kind POLICY_KIND, "
            "policy_name POLICY_NAME "
            "from snowflake.account_usage.policy_references")
    except Exception as exc:
        references_readable = False
        refs = []
        notes.append(f"ACCOUNT_USAGE.POLICY_REFERENCES: {str(exc)[:200]}")

    for r in refs:
        ident = (f'{r.get("REF_DATABASE_NAME")}.{r.get("REF_SCHEMA_NAME")}.'
                 f'{r.get("REF_ENTITY_NAME")}')
        kind = str(r.get("POLICY_KIND") or "").upper()
        if ident.upper() not in in_scope:
            out_of_scope += 1
            continue
        exposures.append({
            "object": ident,
            "column": r.get("REF_COLUMN_NAME"),
            "policy": r.get("POLICY_NAME"),
            "policy_kind": kind,
            "source": "account_usage",
            "severity": "HIGH",
            "consequence": POLICY_CONSEQUENCE.get(kind, _GENERIC_CONSEQUENCE),
            "aidp_path": _AIDP_PATH,
        })

    # A secure view's whole point is hiding its definition and restricting
    # row visibility. Neither survives, and the view still gets created.
    secure_views = [
        {"object": r["source_identifier"], "severity": "HIGH",
         "consequence": "SECURE was not carried over. The target view's "
                        "definition is visible and Snowflake's secure-view "
                        "row-visibility guarantees do not apply.",
         "aidp_path": _AIDP_PATH}
        for r in records
        if r.get("object_type") == "VIEW"
        and str((r.get("source_metadata") or {}).get("is_secure", "")).lower()
        in ("true", "y", "yes", "on")]

    # The lag-free read. Where it answered for every object it is the
    # verdict, and the account-wide views above become corroboration; where it
    # could not, the lagging source and its hedge are all there is.
    live = _live_attachments(run_sql, records, notes,
                             budget=live_attachment_budget)
    if live["attempted"]:
        # UNION, not replacement. The two sources disagree in both directions:
        # the account view lags behind an attachment just made, and it also
        # keeps showing one that was just removed. Dropping either side would
        # hide an exposure, so both are kept and each says where it came from.
        # A row only the lagging view has is flagged as needing confirmation
        # rather than silently believed or silently dropped.
        seen = {(e["object"], e.get("column"), e.get("policy"))
                for e in live["policy_attachments"]}
        stale_only = []
        for e in exposures:
            if (e["object"], e.get("column"), e.get("policy")) in seen:
                continue
            e = dict(e)
            if live["complete"] or e["object"] not in {
                    f["object"] for f in live["failed"]}:
                e["needs_confirmation"] = (
                    "seen only in the ~2 h-stale account view and not in the "
                    "per-object read: either it was detached inside the lag "
                    "window, or the per-object read could not see it. Confirm "
                    "before treating it either way.")
            stale_only.append(e)
        exposures = list(live["policy_attachments"]) + stale_only

    # Tag OBJECTS were counted and their attachments never read, so a
    # classification-driven governance model rendered as an empty table. Same
    # unreadable handling and same latency caveat as POLICY_REFERENCES.
    tag_references = _tag_references(run_sql, in_scope, notes)
    if live["attempted"] and (live["tag_read_objects"]
                              or tag_references.get("measured")):
        seen_tags = {(a["object"], a.get("column"), a.get("tag"))
                     for a in live["tag_attachments"]}
        stale_tags = [a for a in (tag_references.get("attachments") or [])
                      if (a["object"], a.get("column"), a.get("tag"))
                      not in seen_tags]
        merged_tags = list(live["tag_attachments"]) + stale_tags
        source = _LIVE_TAG_SOURCE
        if not live["tag_read_objects"]:
            source = _TAG_ATTACHMENT_SOURCE
        elif stale_tags:
            source = f"{_LIVE_TAG_SOURCE}; and {_TAG_ATTACHMENT_SOURCE}"
        tag_references = {
            "measured": True,
            "attachments": merged_tags,
            "count": len(merged_tags),
            "out_of_scope": tag_references.get("out_of_scope", 0),
            "carried_over": False,
            "source": source,
            "note": (f"{len(merged_tags)} in-scope tag attachment(s); "
                     f'{live["tag_read_objects"]} of {live["objects"]} '
                     f"object(s) read directly"),
            "complete": live["tag_complete"],
        }

    grants = {"measured": False, "by_object": {}, "note": "not requested",
              "classes_requested": list(GRANT_CLASSES)}
    if include_grants:
        grants = _grants(run_sql, in_scope, databases, notes)

    # Policy OBJECTS that exist while POLICY_REFERENCES shows no attachment
    # anywhere. The view lags up to ~2 hours, so this is the signature of a
    # policy attached shortly before the run -- the empty attachment list is
    # then unconfirmed, not clean. A reference to an out-of-scope object
    # accounts for the policy and does not trigger it.
    defined = sum((policies[k]["count"] or 0) for k in POLICY_KINDS)
    enumerated = [POLICY_KIND_LABELS[k] for k in POLICY_KINDS
                  if policies[k]["readable"]]
    unenumerated = [POLICY_KIND_LABELS[k] for k in POLICY_KINDS
                    if not policies[k]["readable"]]
    # A defined policy attached to nothing is only UNCONFIRMED while the
    # lagging view is the only source. Once every in-scope object has been
    # read directly, an empty list is a read result, not a maybe.
    live_settled = live["attempted"] and live.get("policy_complete", False)
    unattached = (defined if (references_readable and not exposures
                              and out_of_scope == 0 and not live_settled)
                  else 0)

    count = None if (not references_readable and not live_settled) \
        else len(exposures)
    if live_settled:
        attachment_source = _LIVE_POLICY_SOURCE
    elif live["attempted"]:
        attachment_source = (f'{_LIVE_POLICY_SOURCE}, for the '
                             f'{live["probed"]} of {live["objects"]} object(s) '
                             f'it could read; {_ATTACHMENT_SOURCE} otherwise')
    else:
        attachment_source = _ATTACHMENT_SOURCE
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "policies": policies,
        "policy_kinds_enumerated": enumerated,
        "policy_kinds_unenumerated": unenumerated,
        "policy_references_readable": references_readable,
        "policy_references_out_of_scope": out_of_scope,
        "policies_defined_without_attachment": unattached,
        "attachment_source": attachment_source,
        "live_attachments": live,
        "exposures": exposures,
        "exposure_count": count,
        "secure_views": secure_views,
        "tag_references": tag_references,
        "grants": grants,
        "unreadable": notes,
        "statement": _statement(count, secure_views,
                                references_readable or live_settled,
                                unattached, kinds_enumerated=enumerated,
                                kinds_unenumerated=unenumerated,
                                live=live, tags=tag_references),
    }


def _tag_references(run_sql, in_scope: set[str], notes: list[str]) -> dict:
    """Which migrated objects and columns carry a tag. Reported, never replayed.

    A tag count with no attachment list is a number with no verdict. This
    mirrors the POLICY_REFERENCES block exactly, including the rule that an
    unreadable view yields `measured: False` and a `count` of None, never 0.
    """
    try:
        rows = run_sql(
            "select tag_database TAG_DATABASE, tag_schema TAG_SCHEMA, "
            "tag_name TAG_NAME, tag_value TAG_VALUE, "
            "object_database OBJECT_DATABASE, object_schema OBJECT_SCHEMA, "
            "object_name OBJECT_NAME, column_name COLUMN_NAME, "
            "domain DOMAIN "
            "from snowflake.account_usage.tag_references")
    except Exception as exc:
        notes.append(f"ACCOUNT_USAGE.TAG_REFERENCES (tag attachments): "
                     f"{str(exc)[:200]}")
        return {"measured": False, "attachments": [], "count": None,
                "out_of_scope": 0, "carried_over": False,
                "source": _TAG_ATTACHMENT_SOURCE, "note": str(exc)[:200]}

    attachments: list[dict] = []
    out_of_scope = 0
    for r in rows:
        ident = (f'{r.get("OBJECT_DATABASE")}.{r.get("OBJECT_SCHEMA")}.'
                 f'{r.get("OBJECT_NAME")}')
        if ident.upper() not in in_scope:
            out_of_scope += 1
            continue
        attachments.append({
            "object": ident,
            "column": r.get("COLUMN_NAME"),
            "domain": r.get("DOMAIN"),
            "tag": (f'{r.get("TAG_DATABASE")}.{r.get("TAG_SCHEMA")}.'
                    f'{r.get("TAG_NAME")}'),
            "value": r.get("TAG_VALUE"),
            "consequence": _TAG_CONSEQUENCE,
            "aidp_path": _TAG_AIDP_PATH,
        })
    return {"measured": True, "attachments": attachments,
            "count": len(attachments), "out_of_scope": out_of_scope,
            "carried_over": False, "source": _TAG_ATTACHMENT_SOURCE,
            "note": f"{len(attachments)} in-scope tag attachment(s)"}


def _grants(run_sql, in_scope: set[str], databases: list[str],
            notes: list[str]) -> dict:
    """Who can read what today. Reported, never replayed.

    `by_object` stays what it always was: the migrated tables and views and
    the roles that hold a privilege on them. `by_class` is everything else in
    the scoped databases -- schema, database, warehouse, stage, procedure,
    function and the rest of GRANT_CLASSES -- which used to be read as
    nothing at all. The report names the classes that were asked for, so a
    class that is absent from the answer is distinguishable from a class that
    was never in the question.
    """
    classes = ", ".join(f"'{c}'" for c in GRANT_CLASSES)
    try:
        rows = run_sql(
            "select name NAME, table_schema TABLE_SCHEMA, "
            "table_catalog DATABASE_NAME, granted_on GRANTED_ON, "
            "privilege PRIVILEGE, grantee_name GRANTEE_NAME, count(*) GRANTS "
            "from snowflake.account_usage.grants_to_roles "
            f"where deleted_on is null and granted_on in ({classes}) "
            "group by 1, 2, 3, 4, 5, 6")
    except Exception as exc:
        notes.append(f"ACCOUNT_USAGE.GRANTS_TO_ROLES (grants): {str(exc)[:200]}")
        return {"measured": False, "by_object": {}, "by_class": {},
                "classes_requested": list(GRANT_CLASSES), "out_of_scope": 0,
                "note": str(exc)[:200]}

    scope = {str(d).upper() for d in databases if d}
    by_object: dict[str, list[dict]] = collections.defaultdict(list)
    classed: dict[str, dict] = {}
    out_of_scope = 0
    for r in rows:
        granted_on = str(r.get("GRANTED_ON") or "").upper() or "UNSPECIFIED"
        db = r.get("DATABASE_NAME")
        ident = ".".join(str(p) for p in (db, r.get("TABLE_SCHEMA"),
                                          r.get("NAME")) if p)
        if ident.upper() in in_scope:
            by_object[ident].append({"role": r.get("GRANTEE_NAME"),
                                     "privilege": r.get("PRIVILEGE")})
        if not _grant_in_scope(granted_on, db, r.get("NAME"), scope):
            out_of_scope += 1
            continue
        entry = classed.setdefault(
            granted_on, {"grants": 0, "roles": set(), "objects": set()})
        entry["grants"] += 1
        entry["roles"].add(r.get("GRANTEE_NAME"))
        entry["objects"].add(ident)
    by_class = {k: {"grants": v["grants"],
                    "roles": sorted(x for x in v["roles"] if x),
                    "objects": len(v["objects"])}
                for k, v in sorted(classed.items())}
    return {"measured": True, "by_object": dict(by_object),
            "by_class": by_class,
            "classes_requested": list(GRANT_CLASSES),
            "out_of_scope": out_of_scope,
            "note": f"{len(by_object)} in-scope object(s) with explicit "
                    f"grants; {len(by_class)} object class(es) seen",
            "carried_over": False}


def _grant_in_scope(granted_on: str, db, name, scope: set[str]) -> bool:
    """Is this grant about something inside the migration scope?

    A DATABASE grant names the database in NAME, not in TABLE_CATALOG, and a
    WAREHOUSE or INTEGRATION grant has no database at all -- filtering on
    TABLE_CATALOG alone would drop both classes silently.
    """
    if not scope:
        return True
    if granted_on == "DATABASE":
        return str(name or "").upper() in scope
    if db:
        return str(db).upper() in scope
    return granted_on in _ACCOUNT_SCOPED_CLASSES


_LAG = ("ACCOUNT_USAGE.POLICY_REFERENCES lags up to ~2 hours behind DDL, so "
        "a policy attached inside that window is not visible here yet")


def _join(labels) -> str:
    labels = list(labels)
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    return ", ".join(labels[:-1]) + f" or {labels[-1]}"


def _statement(count, secure_views: list[dict], references_readable: bool,
               defined_without_attachment: int = 0, *,
               kinds_enumerated=(), kinds_unenumerated=(), live=None,
               tags=None) -> str:
    """The one sentence a reader takes away. It may only name what was asked.

    I3: "could not look" never renders as zero. The clean verdict is built
    from the kinds that actually answered, so a kind whose SHOW was denied
    cannot be covered by it -- it gets said out loud instead.

    Tags likewise: the clean sentence used to say "no tag is attached"
    without ever being handed the tag read, so a PII-tagged column with no
    masking policy got a headline denying the tag the table below listed.
    """
    kinds_enumerated = list(kinds_enumerated)
    kinds_unenumerated = list(kinds_unenumerated)
    live = live or {}
    tags = tags or {}
    tag_count = tags.get("count") if tags.get("measured") else None
    live_settled = bool(live.get("attempted") and live.get("complete"))
    if not references_readable:
        return ("**Policy attachments could not be read**, so whether any "
                "column is masked or any table row-filtered is UNKNOWN. This "
                "is not the same as finding none: grant "
                "`SNOWFLAKE.ACCOUNT_USAGE` and re-run before anyone concludes "
                "the estate is unprotected data.")
    parts = []
    if count:
        parts.append(
            f"**{count} exposure(s):** that many masked columns or "
            f"row-filtered tables are being migrated, and the protection does "
            f"not travel with them. Values masked in Snowflake arrive readable")
    if secure_views:
        parts.append(f"**{len(secure_views)} secure view(s)** lose SECURE")
    # Kept out of `parts` until the end: a tag is not a protection the
    # clone strips, so on its own it must not displace the policy verdict.
    tag_part = (f"**{tag_count} tag attachment(s)** on migrated objects do "
                f"not travel: nothing on the target carries the "
                f"classification, so anything keyed off it has nothing to key "
                f"off") if tag_count else ""
    if defined_without_attachment:
        parts.append(
            f"**{defined_without_attachment} policy object(s) "
            f"({_join(kinds_enumerated) or 'no kind enumerated'}) exist in "
            f"the migrated database(s) but ACCOUNT_USAGE.POLICY_REFERENCES "
            f"lists no attachment.** {_LAG}. Treat exposure as UNCONFIRMED, "
            f"not zero: re-run `security` after the lag, or check the "
            f"attachments live (`DESCRIBE MASKING POLICY`, the "
            f"INFORMATION_SCHEMA POLICY_REFERENCES table function) before the "
            f"clone is used")
    if kinds_unenumerated:
        # Naming the kind is the whole point: a verdict that silently covers
        # a policy kind whose SHOW was denied is the clean-verdict-on-an-
        # unasked-question failure this module exists to prevent.
        parts.append(
            f"**{_join(kinds_unenumerated)} policy objects could not be "
            f"enumerated** (the SHOW was denied to this role, so they are "
            f"*not visible to this role* rather than absent). Nothing here "
            f"covers them, and the empty attachment list cannot be "
            f"corroborated for those kinds. {_LAG}. Re-run with a role that "
            f"can see them before anyone concludes the estate is unprotected "
            f"data")
    # Objects the per-object read could not reach are the ones the verdict
    # cannot speak for, so they are named rather than counted as clean.
    unreachable = [f["object"] for f in (live.get("failed") or [])]
    if unreachable:
        shown = ", ".join(f"`{o}`" for o in unreachable[:5])
        if len(unreachable) > 5:
            shown += f" and {len(unreachable) - 5} more"
        parts.append(
            f"**{len(unreachable)} object(s) could not be read directly** "
            f"({shown}), so nothing above is a verdict about them")
    if not parts:
        tail = f" {tag_part}." if tag_part else ""
        if live_settled:
            # "no tag" only when the tag read was measured and came back
            # empty -- never inferred from the policy read.
            no_tag = " and no tag" if tag_count == 0 else ""
            return (f"No {_join(kinds_enumerated)} policy{no_tag} is "
                    f"attached to anything being migrated, and no secure "
                    f"views are in scope. Read per object from "
                    f"INFORMATION_SCHEMA, so this is current rather than "
                    f"subject to the ~2 h ACCOUNT_USAGE lag.{tail}")
        return (f"No {_join(kinds_enumerated)} policy is attached to anything "
                "being migrated, and no secure views are in scope. Nothing is "
                "protected today that the migration would strip -- as of "
                f"{_LAG}.{tail}")
    if tag_part:
        parts.append(tag_part)
    return " · ".join(parts) + ". Resolve before the clone is used for anything real."
