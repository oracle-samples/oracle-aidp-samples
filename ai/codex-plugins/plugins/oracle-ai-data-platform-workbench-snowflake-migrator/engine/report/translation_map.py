"""The translation map: every translation this migration session made.

Each view's rewrite lives in its own DDL statement and each column's type
mapping in its own inventory record, so "what did the translator do to this
estate?" had no answer short of reading every object. This module folds the
session's artifacts into one map:

  types         -- every Snowflake type seen -> the Spark type it became,
                   with how many columns and which objects
  dialect_rules -- every translator rule, and what this estate made of it:
                   applied, refused, or never encountered
  names         -- every source identifier -> its target key, and the verdict
  ddl_rules     -- how often each DDL audit rule fired

Pure function over artifacts, zero I/O. Views are re-run through the same
`translate_view_body` the DDL stage calls, so the map cannot disagree with
what was emitted -- and a view the planner refused still shows WHICH rule
refused it, which the DDL payload records only as prose. A view refused for
its KIND (secure: plan.build.object_kind_block, checked first as plan and
ddl check it) is never translated; it is counted as refused by kind. A plain
materialized view or a dynamic table is NOT refused: the plan migrates it as
a table snapshot (plan.build.snapshot_kind), so it has its own bucket, with
the refresh verdict the plan recorded, and no dialect rule is counted for it
here -- its query is translated only for the generated refresh.
"""
from __future__ import annotations

import collections

from plan.build import object_kind_block, snapshot_kind
from snowflake_source.dialect.translate import RULES
from snowflake_source.dialect.views import extract_view_body, translate_view_body

__all__ = ["build_translation_map"]

# Types whose precision decides the target, so two NUMBERs that map
# differently are not merged into one row.
_PRECISION_TYPES = ("NUMBER", "DECIMAL", "NUMERIC", "FIXED")


def _source_type(col: dict) -> str:
    dt = str(col.get("DATA_TYPE") or "?").upper()
    p, s = col.get("NUMERIC_PRECISION"), col.get("NUMERIC_SCALE")
    if dt in _PRECISION_TYPES and p is not None:
        return f"{dt}({p},{s if s is not None else 0})"
    return dt


def _types(records: list[dict]) -> list[dict]:
    buckets: dict[tuple, dict] = {}
    for rec in records:
        for col in rec.get("columns") or []:
            src, tgt = _source_type(col), col.get("target_type")
            row = buckets.setdefault((src, tgt), {
                "source_type": src, "target_type": tgt, "columns": 0,
                "objects": set()})
            row["columns"] += 1
            row["objects"].add(rec["source_identifier"])
    out = []
    for row in buckets.values():
        tgt = row["target_type"]
        status = ("unmapped" if not tgt
                  else "identical" if tgt.upper() == row["source_type"]
                  else "mapped")
        out.append({**row, "objects": sorted(row["objects"]), "status": status})
    return sorted(out, key=lambda r: (r["status"] != "unmapped",
                                      -r["columns"], r["source_type"]))


def _snapshots(records: list[dict], plan: dict) -> list[dict]:
    """Objects the plan migrates as a TABLE SNAPSHOT, with the refresh
    verdict it recorded. With a plan, only what it migrates: one it blocked,
    restricted out or cascaded out is not "migrated as a snapshot". Without
    a plan (no can_migrate list), every snapshot-kind object, refresh None.
    """
    planned = plan.get("can_migrate")
    refresh = {c["source_identifier"]: (c.get("refresh") or {}).get("verdict")
               for c in planned or []}
    return [{"source_identifier": rec["source_identifier"], "kind": kind,
             "refresh": refresh.get(rec["source_identifier"])}
            for rec in records
            if (kind := snapshot_kind(rec))
            and (planned is None or rec["source_identifier"] in refresh)]


def _dialect(records: list[dict]) -> tuple[list[dict], dict]:
    applied: dict[str, set] = collections.defaultdict(set)
    refused: dict[str, set] = collections.defaultdict(set)
    details: dict[str, list[str]] = collections.defaultdict(list)
    views = {"translated": 0, "verbatim": 0, "refused": 0, "no_sql": 0,
             "unparseable": 0, "blocked_by_kind": []}
    for rec in records:
        if rec.get("object_type") != "VIEW":
            continue
        ident = rec["source_identifier"]
        # A table snapshot (a plain materialized view) is not refused: it is
        # counted in _snapshots, and its query is translated only for the
        # generated refresh, which plan.json records.
        if snapshot_kind(rec):
            continue
        # The check plan and ddl apply first: a secure view is refused for
        # what it IS, so no dialect rule ever touches it. Translating it
        # here reported rules applied to SQL never emitted.
        block = object_kind_block(rec)
        if block:
            views["blocked_by_kind"].append(
                {"source_identifier": ident, "kind": block[0]})
            continue
        ddl = rec.get("view_ddl_get_ddl") or rec.get("view_text_show")
        if not ddl:
            views["no_sql"] += 1
            continue
        try:
            result = translate_view_body(extract_view_body(ddl))
        except ValueError:
            # Captured, and not readable as a view: not "no SQL captured".
            views["unparseable"] += 1
            continue
        for a in result.applied:
            applied[a["rule_id"]].add(ident)
        for u in result.unsupported:
            refused[u["rule_id"]].add(ident)
            if u["detail"] not in details[u["rule_id"]]:
                details[u["rule_id"]].append(u["detail"])
        if result.unsupported:
            views["refused"] += 1
        elif result.applied:
            views["translated"] += 1
        else:
            views["verbatim"] += 1

    rules = []
    for rule in RULES:
        # A rule can apply to one view and refuse another; refused wins the
        # headline because it is the one that blocks something.
        if refused.get(rule.rule_id):
            outcome = "refused"
        elif applied.get(rule.rule_id):
            outcome = "applied"
        else:
            outcome = "not_encountered"
        rules.append({
            "rule_id": rule.rule_id, "construct": rule.construct,
            "description": rule.description, "status": rule.status,
            "outcome": outcome,
            "objects": sorted(applied.get(rule.rule_id, set())
                              | refused.get(rule.rule_id, set())),
            "applied_to": sorted(applied.get(rule.rule_id, set())),
            "refused_in": sorted(refused.get(rule.rule_id, set())),
            "refusal_detail": details.get(rule.rule_id, []),
        })
    return rules, views


def _names(records: list[dict], plan: dict) -> list[dict]:
    targets = plan.get("target_names") or {}
    cannot = {c["source_identifier"]: c.get("category")
              for c in plan.get("cannot_migrate") or []}
    can = {c["source_identifier"] for c in plan.get("can_migrate") or []}
    out = []
    for rec in records:
        ident = rec["source_identifier"]
        target = targets.get(ident)
        verdict = ("can_migrate" if ident in can
                   else f"cannot_migrate: {cannot[ident]}" if ident in cannot
                   else "not_planned")
        out.append({"source": ident, "object_type": rec.get("object_type"),
                    "target": target,
                    "case_folded": bool(target) and target != ident
                    and target.lower() == ident.lower(),
                    "renamed": bool(target)
                    and target.lower() != ident.lower(),
                    "verdict": verdict})
    return sorted(out, key=lambda n: n["source"])


def _containers(records: list[dict], plan: dict) -> tuple[list, list]:
    """Catalog and schema renames, derived from the object targets: the plan
    maps objects, so a schema's target is whatever its objects landed in."""
    targets = plan.get("target_names") or {}
    schemas: dict[str, str] = {}
    catalogs: dict[str, str] = {}
    for rec in records:
        ident = rec["source_identifier"]
        target = targets.get(ident)
        if not target:
            continue
        db, schema, _ = ident.split(".", 2)
        t_cat, t_schema, _ = target.split(".", 2)
        schemas.setdefault(f"{db}.{schema}", f"{t_cat}.{t_schema}")
        catalogs.setdefault(db, t_cat)
    return ([{"source": k, "target": v, "renamed": v.lower() != k.lower()}
             for k, v in sorted(catalogs.items())],
            [{"source": k, "target": v, "renamed": v.lower() != k.lower()}
             for k, v in sorted(schemas.items())])


def build_translation_map(inventory: dict, plan: dict,
                          ddl_payload: dict | None) -> dict:
    records = (inventory or {}).get("inventory") or []
    types = _types(records)
    rules, views = _dialect(records)
    ddl_rules = collections.Counter(
        r["rule_id"] for s in (ddl_payload or {}).get("statements") or []
        for r in s.get("rules_applied") or [])
    catalogs, schemas = _containers(records, plan or {})
    names = _names(records, plan or {})
    snapshots = _snapshots(records, plan or {})
    # Snapshot-kind objects the plan does not migrate (restricted, blocked,
    # cascaded out): counted, so every materialized view and dynamic table
    # appears in some count.
    not_planned = sum(1 for rec in records if snapshot_kind(rec)) - len(snapshots)
    return {
        "catalogs": catalogs,
        "schemas": schemas,
        "types": types,
        "dialect_rules": rules,
        "names": names,
        "ddl_rules": dict(sorted(ddl_rules.items())),
        "views_blocked_by_kind": views["blocked_by_kind"],
        "table_snapshots": snapshots,
        "ddl_ran": ddl_payload is not None,
        "totals": {
            "objects": len(records),
            "columns": sum(t["columns"] for t in types),
            "distinct_source_types": len({t["source_type"] for t in types}),
            "unmapped_columns": sum(t["columns"] for t in types
                                    if t["status"] == "unmapped"),
            "views_translated": views["translated"],
            "views_verbatim": views["verbatim"],
            "views_refused": views["refused"],
            "views_without_sql": views["no_sql"],
            "views_unparseable": views["unparseable"],
            "views_blocked_by_kind": len(views["blocked_by_kind"]),
            "table_snapshots": len(snapshots),
            "snapshots_not_planned": not_planned,
            "rules_applied": sum(1 for r in rules if r["outcome"] == "applied"),
            "rules_refused": sum(1 for r in rules if r["outcome"] == "refused"),
            "renamed_objects": sum(1 for n in names if n["renamed"]),
            "renamed_schemas": sum(1 for x in schemas if x["renamed"]),
            "refused_for_length": sum(
                1 for n in names
                if n["verdict"] == "cannot_migrate: target_key_too_long"),
        },
    }
