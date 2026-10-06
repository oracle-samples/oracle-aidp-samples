"""Dependency edges between estate objects. Read-only.

Dual-source on purpose. OBJECT_DEPENDENCIES lives in SNOWFLAKE.ACCOUNT_USAGE and
needs a grant a customer may refuse, so building the plan step on it alone would
make the whole feature hostage to a privilege.

  1. preferred -- SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
  2. fallback  -- parse the view DDL already captured during inventory

The two are also MERGED. OBJECT_DEPENDENCIES lags DDL by up to ~3 hours, so
a view created or altered shortly before the run has no row there yet while
the query itself succeeds. A view with no ACCOUNT_USAGE edge is parsed from
its DDL instead, and `source_used` says so: `account_usage_empty` when the
view returned nothing within the inventory, `account_usage+parsed_ddl` when
it covered some views but not others. A readable-but-empty result is never
presented as authoritative lineage.

`source` is recorded per edge so a plan built on parsed DDL is never presented as
authoritative lineage.

Edge direction: {"from": dependent, "to": dependency}. `to` is created first.
Endpoints are the exact inventory `source_identifier` values, so they match
the plan's nodes: a quoted identifier keeps its case in Snowflake, and an
edge spelled any other way is silently dropped by the wave computation.

The one exception is a VIEW's reference to an object OUTSIDE the inventory
(another database, a UDF, anything not being migrated). It is kept as an
edge marked `outside_inventory: True`, its `to` the name as the source gave
it, and listed in `unresolved_references`. Both sources used to drop it, so
a view joining an in-scope table with OTHERDB.S.FACTS kept only its in-scope
edge, was planned as ordered under "views follow their base tables", and
failed at create because the target has no FACTS. The planner reads the
marked edge and refuses the view as depending on something not migrating.
A table's outside reference (a sequence behind a DEFAULT) is not kept: its
CREATE TABLE does not resolve it, and the DEFAULT is reported as not carried.
"""
from __future__ import annotations

import re
from typing import Callable

from ..dialect import lexer

__all__ = ["extract_dependencies", "parse_view_references"]

_IDENT = r'[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+"'
_NAME = re.compile(rf'(?:{_IDENT})(?:\s*\.\s*(?:{_IDENT})){{0,2}}')
_PART = re.compile(_IDENT)
# Found in the CODE mask, so a FROM in a comment, a literal or the GET_DDL
# `COMMENT='...'` header is not one. The raw pattern matched all three and
# fabricated edges -- and cycles -- from prose.
_KEYWORD = re.compile(r"\b(FROM|JOIN)\b", re.IGNORECASE)
_ALIAS = re.compile(r"\s*(?:AS\s+)?([A-Za-z_][A-Za-z0-9_$]*|\"(?:[^\"]|\"\")+\")",
                    re.IGNORECASE)
_WORD_BEFORE = re.compile(r"([A-Za-z_][A-Za-z0-9_$]*)\s*$")
_WORD_AFTER = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_$]*)")
# Words that end a FROM item, so they are never read as its alias -- or
# open one that is not a named relation (TABLE(...), VALUES, LATERAL).
_NOT_ALIAS = {
    "TABLE", "VALUES", "UNNEST", "IDENTIFIER",
    "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS",
    "NATURAL", "ON", "USING", "GROUP", "ORDER", "HAVING", "QUALIFY", "LIMIT",
    "OFFSET", "FETCH", "UNION", "EXCEPT", "MINUS", "INTERSECT", "WINDOW",
    "LATERAL", "AT", "BEFORE", "CHANGES", "SAMPLE", "TABLESAMPLE", "PIVOT",
    "UNPIVOT", "MATCH_RECOGNIZE", "ASOF", "SELECT", "FROM"}


def _skip_parens(mask: str, i: int) -> int:
    """Index just past the parenthesis group opening at mask[i]."""
    depth = 0
    for j in range(i, len(mask)):
        if mask[j] == "(":
            depth += 1
        elif mask[j] == ")":
            depth -= 1
            if depth == 0:
                return j + 1
    return len(mask)


def parse_view_references(ddl: str, *, default_db: str,
                          default_schema: str) -> list[str]:
    """Extract fully-qualified referenced object names from a view body.

    Every relation of a FROM list is read -- `from ORDERS o, Z_CUST c` has
    two -- not only the first: a dropped one is an edge the wave order never
    sees, and the view can be created before a view it reads. A bare name
    that is one of the view's own CTEs, where the CTE is visible, is not a
    table.
    """
    if not ddl:
        return []
    try:
        mask = lexer.code_only(ddl)
        ctes = lexer.cte_scopes(ddl)
    except lexer.UnterminatedLiteral:
        # The planner refuses this view as unparseable with the same lexer,
        # so it is never created and its own references order nothing.
        return []

    found: set[str] = set()

    def skip_blank(pos: int) -> int:
        # Whitespace and comments are blank in the mask; so is a quoted
        # identifier, which is where a relation may start.
        while pos < len(mask) and mask[pos].isspace() and ddl[pos] != '"':
            pos += 1
        return pos

    def take(pos: int) -> int | None:
        """Read one relation at `pos`; return the index after it (and its
        alias), or None when what stands there is not a relation."""
        pos = skip_blank(pos)
        if pos >= len(mask):
            return None
        if mask[pos] == "(":                     # a subquery: its own FROMs
            pos = _skip_parens(mask, pos)       # are found by the outer scan
        else:
            # Names are read from the RAW text -- a quoted part is blanked in
            # the mask -- but must start in code or at a quoted identifier.
            m = _NAME.match(ddl, pos)
            if m is None or (mask[pos] != ddl[pos] and ddl[pos] != '"'):
                return None
            raw_parts = _PART.findall(m.group(0))
            if len(raw_parts) == 1:
                one = raw_parts[0]
                if one.upper() in _NOT_ALIAS:
                    return None                 # LATERAL, TABLE(...) forms
                # Compared as lexer.cte_scopes records a CTE name: unquoted
                # folds to upper case, quoted is exact.
                key = (one[1:-1].replace('""', '"') if one.startswith('"')
                       else one.upper())
                if any(n == key and lo <= pos < hi for n, lo, hi in ctes):
                    raw_parts = []              # the view's own CTE
            if raw_parts:
                parts = [p.strip('"').replace('""', '"') for p in raw_parts]
                parts = [default_db, default_schema][:3 - len(parts)] + parts
                found.add(".".join(p.upper() for p in parts))
            pos = m.end()
        alias = _ALIAS.match(ddl, pos)
        if alias and alias.group(1).upper() not in _NOT_ALIAS:
            pos = alias.end()
        return pos

    # A FROM that is not a FROM clause: `IS DISTINCT FROM b` and
    # `EXTRACT(year FROM ts)` are followed by an expression, whose column
    # name would otherwise be read as a table the view depends on; in
    # `NTH_VALUE(x, n) FROM FIRST OVER (...)` it is a window modifier, and
    # FIRST / LAST would become an outside edge to an object that does not
    # exist -- which refuses the view.
    keywords = list(_KEYWORD.finditer(mask))
    starts = {kw.start() for kw in keywords}
    enclosing: dict[int, int | None] = {}
    opener: dict[int, int] = {}                 # `)` index -> its `(`
    stack: list[int] = []
    for i, ch in enumerate(mask):
        if i in starts:
            enclosing[i] = stack[-1] if stack else None
        if ch == "(":
            stack.append(i)
        elif ch == ")" and stack:
            opener[i] = stack.pop()

    def word_before(i: int) -> str:
        m = _WORD_BEFORE.search(mask[max(0, i - 200):i])
        return m.group(1).upper() if m else ""

    def not_a_clause(kw: re.Match) -> bool:
        if kw.group(1).upper() != "FROM":
            return False
        if word_before(kw.start()) == "DISTINCT":
            return True
        opened = enclosing.get(kw.start())
        if opened is not None and word_before(opened) == "EXTRACT":
            return True
        before = mask[:kw.start()].rstrip()
        after = _WORD_AFTER.match(mask, kw.end())
        return (before.endswith(")") and after is not None
                and after.group(1).upper() in ("FIRST", "LAST")
                and word_before(opener.get(len(before) - 1, 0)) == "NTH_VALUE")

    for kw in keywords:
        if not_a_clause(kw):
            continue
        pos = take(kw.end())
        # Only a FROM carries a comma list; a JOIN names one relation.
        while pos is not None and kw.group(1).upper() == "FROM":
            pos = skip_blank(pos)
            if pos >= len(mask) or mask[pos] != ",":
                break
            pos = take(pos + 1)
    return sorted(found)


def _outside(dependent: str, name: str, kind: str, source: str) -> dict:
    return {"from": dependent, "to": name, "kind": kind, "source": source,
            "outside_inventory": True}


def _from_account_usage(run_sql: Callable[..., list[dict]],
                        by_upper: dict[str, str],
                        view_ids: set[str]) -> list[dict]:
    rows = run_sql(
        "select referencing_database || '.' || referencing_schema || '.' || "
        "       referencing_object_name as REFERENCING, "
        "       referenced_database || '.' || referenced_schema || '.' || "
        "       referenced_object_name as REFERENCED, "
        "       referencing_object_domain as REFERENCING_TYPE, "
        "       referenced_object_domain as REFERENCED_TYPE "
        "from snowflake.account_usage.object_dependencies")
    edges = []
    for r in rows:
        # Matched case-insensitively, emitted in the inventory's spelling.
        dependent = by_upper.get(str(r.get("REFERENCING") or "").upper())
        referenced = str(r.get("REFERENCED") or "")
        dependency = by_upper.get(referenced.upper())
        kind = f'{r.get("REFERENCING_TYPE")}->{r.get("REFERENCED_TYPE")}'
        if dependent and dependency and dependent != dependency:
            edges.append({
                "from": dependent, "to": dependency,
                "kind": kind,
                "source": "account_usage",
            })
        elif dependent in view_ids and dependency is None and referenced:
            edges.append(_outside(dependent, referenced, kind, "account_usage"))
    return edges


_AUTHORITATIVE_NOTE = ("ACCOUNT_USAGE.OBJECT_DEPENDENCIES: authoritative "
                       "lineage for all object types")


def _from_parsed_ddl(records: list[dict],
                     by_upper: dict[str, str]) -> tuple[list[dict], set[str]]:
    """view->object edges parsed from the DDL captured at inventory time."""
    edges: list[dict] = []
    unresolved: set[str] = set()
    for rec in records:
        if rec.get("object_type") != "VIEW":
            continue
        dependent = rec["source_identifier"]
        for ref in parse_view_references(
                rec.get("view_ddl_get_ddl") or rec.get("view_text_show") or "",
                default_db=rec["source_database"],
                default_schema=rec["source_schema"]):
            dependency = by_upper.get(ref)
            if dependency is None:
                unresolved.add(ref)
                edges.append(_outside(dependent, ref, "VIEW->OBJECT",
                                      "parsed_ddl"))
            elif dependency != dependent:
                edges.append({"from": dependent, "to": dependency,
                              "kind": "VIEW->OBJECT", "source": "parsed_ddl"})
    return edges, unresolved


def extract_dependencies(run_sql: Callable[..., list[dict]],
                         inventory: dict) -> dict:
    records = inventory.get("inventory", [])
    # Upper-cased name -> the inventory's exact spelling. Unambiguous because
    # the inventory HALTs on identifier-case collisions before this stage
    # runs (snowmig.py); were that gate ever bypassed, this map would be
    # last-wins and could attach an edge to the wrong twin.
    by_upper = {r["source_identifier"].upper(): r["source_identifier"]
                for r in records}
    views = [r for r in records if r.get("object_type") == "VIEW"]
    view_ids = {r["source_identifier"] for r in views}

    try:
        au_edges = _from_account_usage(run_sql, by_upper, view_ids)
    except Exception as exc:
        note = (f"ACCOUNT_USAGE.OBJECT_DEPENDENCIES unavailable ({exc}); fell back to "
                "parsing view DDL. Covers view->object edges ONLY -- not "
                "authoritative lineage.")
        edges, unresolved = _from_parsed_ddl(views, by_upper)
        return {"edges": edges, "source_used": "parsed_ddl", "coverage_note": note,
                "unresolved_references": sorted(unresolved), "warning": None}

    covered = {e["from"] for e in au_edges}
    uncovered = [r for r in views if r["source_identifier"] not in covered]
    au_outside = {e["to"] for e in au_edges if e.get("outside_inventory")}
    if not uncovered:
        return {"edges": au_edges, "source_used": "account_usage",
                "coverage_note": _AUTHORITATIVE_NOTE,
                "unresolved_references": sorted(au_outside),
                "views_without_account_usage_edge": [], "warning": None}

    # Readable, but not populated for these views. The query succeeding is
    # not evidence that the lineage is complete: a freshly prepared estate
    # lands here, and so does a view whose only references lie outside the
    # inventory. Parse what DDL we hold and say plainly what is still
    # unordered, so the plan cannot imply an order it does not have.
    parsed, unresolved = _from_parsed_ddl(uncovered, by_upper)
    missing = sorted(r["source_identifier"] for r in uncovered)
    ordered_by_parse = {e["from"] for e in parsed}
    unordered = [i for i in missing if i not in ordered_by_parse]
    source_used = ("account_usage_empty" if not au_edges
                   else "account_usage+parsed_ddl")
    note = (f"ACCOUNT_USAGE.OBJECT_DEPENDENCIES was readable but returned no "
            f"edge within the inventory for {len(missing)} of {len(views)} "
            f"view(s). The view lags DDL by up to ~3 hours, so a view created "
            f"or altered shortly before this run is not in it yet. Their DDL "
            f"was parsed instead (view->object edges only) -- NOT "
            f"authoritative lineage for those views.")
    warning = (f"{len(missing)} view(s) have no ACCOUNT_USAGE lineage edge "
               f"(the view lags DDL by up to ~3 h); their DDL was parsed instead")
    if unordered:
        warning += (f". {len(unordered)} still have no edge from either source "
                    f"and are ordered by size only: {', '.join(unordered)}")
    warning += ". Re-run `deps` after the lag before relying on the wave order."
    return {"edges": au_edges + parsed, "source_used": source_used,
            "coverage_note": note,
            "unresolved_references": sorted(unresolved | au_outside),
            "views_without_account_usage_edge": missing, "warning": warning}
