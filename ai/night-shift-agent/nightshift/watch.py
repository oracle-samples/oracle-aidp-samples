"""Validation of watched / drafted statements before anything is executed (moved out of the heartbeat job so the
drafting step and the measurement script share it, and so it is unit-testable with a stub Spark session).

Trust boundary: the watch list and model drafts are untrusted text. A statement is accepted only if
  - the table, ts and key names are plain identifiers and the statement is one plain MERGE (no ';', comments, braces);
  - the platform's own parser (``parsePlan`` -> ``toJSON``) sees exactly one MergeIntoTable between two plain relations,
    and every node class / function name of the tree is on the allowlist (subqueries, table-valued sources,
    reflection and anything else unlisted are rejected by name);
  - the MERGE target is the declared table and the source is a plain identifier equal to the declared source;
  - the ts / key columns exist on the table.
"""
from __future__ import annotations

import json
import re
from typing import Any

from nightshift.merge_adapt import AdaptedMerge, adapt_merge

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUALIFIED = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
FORMAT_PREFIXES = {"delta", "parquet", "json", "csv", "text", "orc", "avro", "binaryfile", "jdbc", "iceberg"}
ALLOWED_FUNCTIONS = {"coalesce", "upper", "lower", "trim", "date_trunc", "to_timestamp", "to_date", "abs", "round"}
# parsed-tree node classes a plain MERGE between two tables may contain
ALLOWED_NODES = {
    "MergeIntoTable", "UnresolvedRelation", "SubqueryAlias", "AliasIdentifier", "UnresolvedAttribute", "UnresolvedStar", "Literal",
    "And", "Or", "Not", "EqualTo", "EqualNullSafe", "GreaterThan", "GreaterThanOrEqual", "LessThan", "LessThanOrEqual",
    "IsNull", "IsNotNull", "In", "Cast", "Coalesce", "Upper", "Lower", "StringTrim", "Alias",
    "InsertStarAction", "UpdateStarAction", "InsertAction", "UpdateAction", "Assignment",
}


def parse_tree(spark: Any, stmt: str) -> dict | list:
    """The platform parser's view of the statement, as JSON."""
    plan = spark._jsparkSession.sessionState().sqlParser().parsePlan(stmt)
    return json.loads(plan.toJSON())


def walk_tree(tree: Any) -> tuple[list[str], list[str]]:
    """(classes seen in order, violations) over a parsed tree."""
    seen: list[str] = []
    bad: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            cls = str(node.get("class", "")).rsplit(".", 1)[-1]
            if cls:
                seen.append(cls)
                if cls == "UnresolvedFunction":
                    parts = node.get("nameParts") or []
                    if isinstance(parts, str):   # toJSON renders Seq[String] as "[a, b]"
                        parts = [x.strip() for x in parts.strip("[]").split(",") if x.strip()]
                    name = ".".join(str(x) for x in parts).lower()
                    if len(parts) != 1 or name not in ALLOWED_FUNCTIONS:
                        bad.append(f"function {name or '?'}")
                elif cls not in ALLOWED_NODES:
                    bad.append(cls)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(tree)
    return seen, bad


def checked_watch(spark: Any, w: dict, require_source: bool = False) -> tuple[str, str, list[str], AdaptedMerge]:
    """Validate one watch entry / draft ``{"table","ts","statement","source"?,"key"?}``; returns
    (table, ts, key_cols, adapted) or raises ValueError with the reason. A model draft is checked with
    ``require_source=True`` so its USING table is always pinned to the declared source (the harness swaps the source
    for a staged batch, so an unpinned source would never be exercised by replay)."""
    if not isinstance(w, dict):
        raise ValueError(f"entry is {type(w).__name__}, not an object")
    if require_source and not w.get("source"):
        raise ValueError("a draft must be checked against a declared source table")
    table, ts, stmt = str(w["table"]), str(w["ts"]), str(w["statement"]).strip().rstrip(";")
    if not _QUALIFIED.match(table):
        raise ValueError(f"watched table is not a plain identifier: {table!r}")
    if not _IDENT.match(ts):
        raise ValueError(f"watched ts is not a plain identifier: {ts!r}")
    if ";" in stmt or "--" in stmt or "/*" in stmt or "{" in stmt or "}" in stmt:
        raise ValueError("statement must be one plain MERGE without comments, ';' or braces")
    if not re.match(r"^MERGE\s+INTO\s", stmt, re.I):
        raise ValueError("statement must start with MERGE INTO")
    try:
        tree = parse_tree(spark, stmt)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"statement does not parse: {type(e).__name__}")
    seen, bad = walk_tree(tree)
    if "DeleteAction" in seen:
        # the watched table is the statement's own output, so it holds no row the branch would remove: replay would
        # pass without ever running the DELETE, and say nothing about it
        raise ValueError("a MERGE with a DELETE branch is not graded: replay cannot exercise the delete")
    if seen.count("MergeIntoTable") != 1 or seen[0] != "MergeIntoTable":
        raise ValueError("exactly one MERGE INTO is allowed, and nothing else")
    if seen.count("UnresolvedRelation") != 2:
        raise ValueError("MERGE target and source must both be plain tables (no subqueries)")
    if bad:
        raise ValueError(f"statement uses constructs outside the allowlist: {sorted(set(bad))[:6]}")
    a = adapt_merge(stmt)
    if not a.template.lower().startswith("merge into {target}"):
        raise ValueError("statement must begin with MERGE INTO <declared table>")
    src = a.source.strip()
    if "`" in src or "/" in src or ":" in src or not _QUALIFIED.match(src):
        raise ValueError(f"MERGE source must be a plain table identifier, got {src!r}")
    if src.split(".")[0].lower() in FORMAT_PREFIXES:
        raise ValueError(f"MERGE source must not be a data-source path: {src!r}")
    if w.get("source") and src.lower() != str(w["source"]).lower():
        raise ValueError(f"MERGE source {src!r} is not the declared source {w['source']!r}")
    if a.target.replace("`", "").lower() != table.lower():
        raise ValueError(f"statement target {a.target!r} is not the declared table {table!r}")
    keys = [str(k) for k in (w.get("key") or list(a.key_cols))]
    if not keys or any(not _IDENT.match(k) for k in keys):
        raise ValueError(f"keys are not plain identifiers: {keys!r}")
    if ts.lower() in {k.lower() for k in keys}:
        # a correction is the same key with a later ordering value: with the ordering column in the key it becomes a
        # new key, WHEN MATCHED never fires, and the revision check would pass without exercising anything
        raise ValueError(f"the ordering column cannot be a key: {ts!r} is in the key {keys}")
    return table, ts, keys, a


def table_columns(spark: Any, table: str) -> list[tuple[str, str]]:
    """(name, type) from DESCRIBE, stopping at the partition block; names must be plain identifiers."""
    out: list[tuple[str, str]] = []
    for r in spark.sql(f"DESCRIBE {table}").collect():
        c = str(r[0]).strip()
        if not c or c.startswith("#"):
            break
        if not _IDENT.match(c):
            raise ValueError(f"column name is not a plain identifier: {c!r}")
        out.append((c, str(r[1])))
    return out


def require_delta(spark: Any, table: str) -> None:
    detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].asDict()
    if str(detail.get("format", "")).lower() != "delta":
        raise ValueError(f"{table} is not a Delta table (format {detail.get('format')!r}); only Delta tables are replayed")


__all__ = ["checked_watch", "table_columns", "require_delta", "walk_tree", "parse_tree",
           "ALLOWED_FUNCTIONS", "ALLOWED_NODES", "FORMAT_PREFIXES"]
