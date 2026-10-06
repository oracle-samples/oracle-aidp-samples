#!/usr/bin/env python3
"""Plan vs reality: what landed in the target catalog, what did not, and why.

Runs on AIDP compute; READS ONLY. Joins three sources of truth —

  * `discovery_manifest.json`  what the external catalog exposed,
  * `structure_report_*.json` / `copy_report_*.json`  what the scripts claim,
  * the TARGET CATALOG itself  what actually exists (SHOW TABLES, and counts
    with --counts, one batched UNION ALL of three-part names per 50 tables) —

into `reconciliation.json` and `MIGRATION_REPORT.md`: one row per table with
its structure status, copy status, live existence, and live row count. The
catalog is consulted directly so the report cannot be flattered by a stale
script report: an object a report calls verified but the catalog no longer
holds -- or, with --counts, no longer holds at the verified row count -- is
flagged, and an object in the catalog that no report claims is flagged the
other way. A script report written for a DIFFERENT target -- another
catalog, or another schema than the one the approved plan names -- is
ignored (and said so), not applied to this one.

The target schema is resolved as 01_create_structure and 02_copy_schema
resolve it: the approved plan's `target_fqn` for the source schema, and only
where the plan is silent the one the reports recorded (else the source
schema's own name). Schema names compare case-insensitively, as Spark
resolves them.

"Could not look" never renders as zero: an unreadable schema is marked
UNREADABLE, distinct from empty.

Views are listed per schema, never omitted, with what the structure report
records for them: 01_create_structure --mode ddl-plan creates the plan's
views, so a view it created is VIEW_CREATED and one whose CREATE failed is
VIEW_FAILED (a problem verdict); a manifest view the plan does not carry is
VIEW_NOT_IN_PLAN, a planned one not created yet (dry run) is
VIEW_NOT_CREATED_YET, and one no ddl-plan run recorded (--mode ctas or
manifest, or no structure report) is VIEW_NOT_CREATED_BY_THIS_PATH, with the
report saying how views do get created.

A dynamic table or materialized view the approved plan migrates as a TABLE
SNAPSHOT (a TABLE statement with `snapshot_of`) is reconciled as a table --
structure, copy, existence, count -- wherever the manifest lists it:
discovery files a materialized view under `views`, and live 2026-09-29 this
report listed the snapshot as a view nobody created.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys

# /Workspace is the live-verified mount of the workspace tree on cluster
# filesystems (probed 2026-09-16 on a real cluster).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    read_plan_json, read_report_json, write_step_output)

DEFAULT_REPORTS_DIR = "/Workspace/backup-snowflake-migration/reports"
# The step's own values also go here, next to the accumulated run report
# the CLI publishes after every stage. `--output-dir ''` switches it off.
DEFAULT_OUTPUT_DIR = "/Workspace/report/output"
MANIFEST_NAME = "discovery_manifest.json"

# Verdicts that mean something is WRONG, as opposed to not done yet. A
# migration runs schema by schema, so "this table was never attempted" is the
# normal state of most of the estate for most of the project -- exiting
# non-zero on it would make every partial run look broken, which is how a
# real signal gets ignored.
PROBLEM_VERDICTS = ("MISSING_DESPITE_REPORT", "VIEW_MISSING_DESPITE_REPORT",
                    "STRUCTURE_FAILED",
                    "STRUCTURE_TYPE_DRIFT", "STRUCTURE_ONLY_COPY_FAILED",
                    "COUNT_DRIFT", "TARGET_UNREADABLE", "VIEW_FAILED")

# A view's structure status -> its verdict. Anything else (no record, or
# `not_created_by_this_path` from --mode ctas / manifest) is
# VIEW_NOT_CREATED_BY_THIS_PATH.
_VIEW_VERDICTS = {"created": "VIEW_CREATED", "failed": "VIEW_FAILED",
                  "not_in_plan": "VIEW_NOT_IN_PLAN",
                  "dry_run": "VIEW_NOT_CREATED_YET",
                  "not_attempted": "VIEW_NOT_CREATED_YET"}


def q(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def log(msg: str) -> None:
    print(f"[reconcile] {msg}", flush=True)


def fail(msg: str) -> int:
    """Report a refusal on BOTH streams and return 1.

    A notebook task captures stdout only: live, a script that exited 1 via a
    stderr-only message produced a job failure with NO explanation anywhere.
    """
    print(f"ERROR: {msg}", flush=True)
    print(f"error: {msg}", file=sys.stderr)
    return 1


def _load(reports: pathlib.Path, name: str) -> dict | None:
    path = reports / name
    return read_report_json(path) if path.exists() else None


def _for_catalog(report: dict | None,
                 target_catalog: str) -> tuple[dict | None, str | None]:
    """`report` if it was written for `target_catalog`, else (None, target).

    A report is evidence about the catalog it was written against and no
    other: 01 and 02 already refuse to resume from a report whose `target`
    differs, and the reports directory is shared, so after re-pointing at
    another catalog a copy report verified against the old one is still on
    disk. A report with no `target` (older shape) stays trusted, as 02 does.
    Catalog names compare case-insensitively, as Spark resolves them.
    """
    if not report:
        return None, None
    target = str(report.get("target") or "")
    if target and target.split(".", 1)[0].lower() != target_catalog.lower():
        return None, target
    return report, None


def _for_schema(report: dict | None,
                target_schema: str) -> tuple[dict | None, str | None]:
    """`report` if it was written for `target_schema`, else (None, target).

    The catalog check alone let a copy report verified against `lake.CORE`
    (the pre-fix copy's schema) certify `lake.db_core`, where the plan put
    the tables and where they held 0 rows: MIGRATED_VERIFIED, exit 0.
    """
    if not report:
        return report, None
    target = str(report.get("target") or "")
    if "." in target and \
            target.split(".", 1)[1].casefold() != target_schema.casefold():
        return None, target
    return report, None


def plan_targets(ddl_plan: dict,
                 target_catalog: str) -> dict[str, set[str]]:
    """`{source_schema: {target_schema}}` from the plan's TABLE statements
    in `target_catalog` -- the same `target_fqn` 01 creates the tables at."""
    out: dict[str, set[str]] = {}
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(source) != 3 or len(target) != 3:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        if target[0].casefold() != target_catalog.casefold():
            continue
        out.setdefault(source[1], set()).add(target[1])
    return out


def plan_views(ddl_plan: dict, target_catalog: str) -> dict[str, set[str]]:
    """`{source_schema: {view}}` for the plan's VIEW statements in
    `target_catalog`: the views 01_create_structure --mode ddl-plan creates.
    A table snapshot is a TABLE statement, so it is not one of them."""
    out: dict[str, set[str]] = {}
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(source) != 3 or len(target) != 3:
            continue
        if str(stmt.get("object_type") or "").upper() != "VIEW":
            continue
        if target[0].casefold() != target_catalog.casefold():
            continue
        out.setdefault(source[1], set()).add(source[2])
    return out


# A table in the target that no structure report records. Live 2026-09-29
# (50k-table estate): a structure run stopped after an hour had created 16
# tables after its last report write, and they read as verified structure.
UNRECORDED_REASON = (
    "in the target, but no structure report records it: a structure run "
    "stopped after creating it and before writing its report, or it was "
    "created outside this migration. Its layout has NOT been checked "
    "against the plan; re-run 01_create_structure, which checks it and "
    "records it")
PLANNED_VIEW_REASON = (
    "in the approved plan; no structure run has recorded it yet. "
    "01_create_structure --mode ddl-plan creates the plan's views after "
    "every table")


def plan_snapshots(ddl_plan: dict,
                   target_catalog: str) -> dict[str, list[str]]:
    """`{source_schema: [name]}` the plan migrates as a TABLE SNAPSHOT in
    `target_catalog`: tables here, whatever list the manifest put them in."""
    out: dict[str, list[str]] = {}
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(source) != 3 or len(target) != 3 or not stmt.get("snapshot_of"):
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        if target[0].casefold() != target_catalog.casefold():
            continue
        names = out.setdefault(source[1], [])
        if source[2] not in names:
            names.append(source[2])
    return out


def _target_schema(schema: str, planned: set[str],
                   reports: list[dict]) -> str:
    """The plan's schema; where the plan is silent, the recorded one."""
    recorded = [str(r.get("target")).split(".", 1)[1] for r in reports
                if r and "." in str(r.get("target") or "")]
    if planned:
        for name in sorted(planned):
            if any(name.casefold() == r.casefold() for r in recorded):
                return name
        return sorted(planned)[0]
    return recorded[0] if recorded else schema


# What Spark says when a table, or the schema holding it, is simply not
# there -- the same markers 02_copy_schema reads as "absent" (a test pins the
# two lists equal). Only these mean "absent"; any other error is "could not
# look".
_NOT_FOUND = ("TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND",
              "NoSuchTableException", "NoSuchNamespaceException",
              "NoSuchDatabaseException", "Table or view not found")


def _live_tables(spark, catalog: str, schema: str) -> set[str] | None:
    """Lower-cased table names the catalog holds; an EMPTY set when the
    schema does not exist yet; None when it could not be read.

    A schema the target has not created is a schema not migrated yet. Every
    SHOW TABLES error used to read as "could not look", so live, a
    schema-by-schema run exited 1 with every not-yet-created schema
    TARGET_UNREADABLE -- after each schema but the last.
    """
    try:
        rows = spark.sql(f"SHOW TABLES IN {q(catalog)}.{q(schema)}").collect()
    except Exception as exc:
        text = str(exc).lower()
        if any(marker.lower() in text for marker in _NOT_FOUND):
            return set()
        return None
    out = set()
    for r in rows:
        d = {k.lower(): v for k, v in r.asDict().items()}
        out.add(str(d.get("tablename") or d.get("name") or "").lower())
    return out


def _live_views(spark, catalog: str, schema: str) -> set[str] | None:
    """Lower-cased view names from SHOW VIEWS; None when it could not be
    read (not every catalog supports it)."""
    try:
        rows = spark.sql(f"SHOW VIEWS IN {q(catalog)}.{q(schema)}").collect()
    except Exception:
        return None
    out = set()
    for r in rows:
        d = {k.lower(): v for k, v in r.asDict().items()}
        out.add(str(d.get("viewname") or d.get("tablename")
                    or d.get("name") or "").lower())
    return out


def _view_exists(spark, fqn: str, name: str, live: set[str] | None,
                 views: set[str] | None) -> bool | None:
    """Whether the view is in the target -- LOOKED FOR, not taken from the
    structure report. True/False, or None when it could not be established.

    SHOW TABLES lists views on some catalogs; SHOW VIEWS on others; failing
    both, the view itself is described. A not-found answer is "no"; any
    other error is "could not look".
    """
    if live is not None and name in live:
        return True
    if views is not None:
        return name in views
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}").collect()
        return True
    except Exception as exc:
        text = str(exc).lower()
        if any(marker.lower() in text for marker in _NOT_FOUND):
            return False
        return None


# Tables per batched count: one UNION ALL of fully qualified COUNT(*)s.
# 50 is the size the copy's batched source count was live-verified at
# (25 s, 2026-09-29).
COUNT_CHUNK = 50


def _target_counts(spark, catalog: str, schema: str,
                   tables: list[str]) -> dict[str, tuple[int | None, str | None]]:
    """`{table: (count, None) | (None, error)}`, ONE query per chunk.

    A COUNT(*) per table was a Spark job per table. Each branch is a
    three-part name tagged with its POSITION, so no table name has to
    survive being a string literal. One unreadable table fails the whole
    query and a batch cannot say which, so that chunk falls back to a count
    per table: the unreadable one keeps its own error, the rest are counted.
    """
    out: dict[str, tuple[int | None, str | None]] = {}

    def fqn(table: str) -> str:
        return f"{q(catalog)}.{q(schema)}.{q(table)}"

    for start in range(0, len(tables), COUNT_CHUNK):
        chunk = tables[start:start + COUNT_CHUNK]
        sql = " UNION ALL ".join(
            f"SELECT {i} AS `i`, COUNT(*) AS `n` FROM {fqn(t)}"
            for i, t in enumerate(chunk))
        try:
            got = {int(r["i"]): int(r["n"]) for r in spark.sql(sql).collect()}
            if sorted(got) != list(range(len(chunk))):
                raise RuntimeError(f"the batched count answered {len(got)} "
                                   f"of {len(chunk)} table(s)")
            for i, table in enumerate(chunk):
                out[table] = (got[i], None)
            continue
        except Exception as exc:
            log(f"{schema}: batched count of {len(chunk)} table(s) failed "
                f"({str(exc)[:120]}); counting them one by one")
        for table in chunk:
            try:
                out[table] = (spark.sql(
                    f"SELECT COUNT(*) AS n FROM {fqn(table)}"
                ).collect()[0]["n"], None)
            except Exception as exc:
                out[table] = (None, str(exc)[:200])
    return out


def reconcile(spark, *, manifest: dict, target_catalog: str,
              reports: pathlib.Path, counts: bool,
              planned_targets: dict[str, set[str]] | None = None,
              planned_snapshots: dict[str, list[str]] | None = None,
              planned_views: dict[str, set[str]] | None = None) -> dict:
    out = {"target_catalog": target_catalog,
           "generated_at": datetime.datetime.now(
               datetime.timezone.utc).isoformat(),
           "schemas": [], "totals": {}}
    tally: dict[str, int] = {}

    for schema_rec in manifest["schemas"]:
        schema = schema_rec["name"]
        # The plan's table snapshots are tables: added to the manifest's
        # tables, and taken out of its views (where a materialized view is).
        snapshots = list((planned_snapshots or {}).get(schema) or [])
        listed = {t["name"] for t in schema_rec["tables"]}
        tables = list(schema_rec["tables"]) + [
            {"name": n} for n in snapshots if n not in listed]
        manifest_views = [v for v in schema_rec.get("views") or []
                          if v["name"] not in snapshots]
        structure, s_other = _for_catalog(
            _load(reports, f"structure_report_{schema.lower()}.json"),
            target_catalog)
        copy, c_other = _for_catalog(
            _load(reports, f"copy_report_{schema.lower()}.json"),
            target_catalog)
        # The structure report first: it records where 01 created the
        # tables, which is where the copy has to have put the rows.
        target_schema = _target_schema(
            schema, (planned_targets or {}).get(schema) or set(),
            [structure, copy])
        structure, s_wrong = _for_schema(structure, target_schema)
        copy, c_wrong = _for_schema(copy, target_schema)
        ignored = {kind: other for kind, other in
                   (("structure", s_other or s_wrong),
                    ("copy", c_other or c_wrong)) if other}
        for kind, other in ignored.items():
            log(f"{schema}: the {kind} report targets {other}, not "
                f"{target_catalog}.{target_schema} — ignored for this target")
        live = _live_tables(spark, target_catalog, target_schema)
        live_counts = (_target_counts(
            spark, target_catalog, target_schema,
            [t["name"] for t in tables
             if t["name"].lower() in live])
            if counts and live is not None else {})

        rows = []
        for table in tables:
            name = table["name"]
            s_status = ((structure or {}).get("objects", {})
                        .get(name, {}).get("status", "not_attempted"))
            c_rec = (copy or {}).get("tables", {}).get(name, {})
            c_status = c_rec.get("status", "not_attempted")
            exists = (None if live is None else name.lower() in live)
            reason = None

            if live is None:
                verdict = "TARGET_UNREADABLE"
            elif not exists and (s_status in ("created", "already_existed")
                                 or c_status in ("verified",
                                                 "verified_with_conversion")):
                verdict = "MISSING_DESPITE_REPORT"
            elif s_status == "failed":
                # The CREATE raised. Whether or not something by that name
                # is there now, nobody has checked it: the operator has to
                # act, so this is not "never attempted". `not_in_plan` and
                # `dry_run` are intentional, and not problems.
                verdict = "STRUCTURE_FAILED"
            elif not exists and s_status == "not_in_plan":
                # Left out of the approved plan (an S9 scope reduction, or a
                # table the engine blocked): not pending. Live (2026-09-29)
                # a 4-table plan reported its 996 out-of-scope tables as
                # "not migrated yet -- expected while the migration is still
                # running", which they never will be under that plan.
                verdict = "NOT_IN_PLAN"
            elif not exists:
                verdict = "NOT_MIGRATED"
            elif s_status == "type_drift":
                # The table is there with a layout the plan did not produce.
                # A copy into it can verify counts and still have landed rows
                # in the wrong columns, so this outranks any copy status.
                verdict = "STRUCTURE_TYPE_DRIFT"
            elif c_status == "target_missing":
                # The copy found no table here, and the catalog lists one
                # now: the copy never ran against it. Falling through to
                # STRUCTURE_ONLY put "does not exist" beside "In target:
                # yes" under "No table is in a problem state".
                verdict = "STRUCTURE_ONLY_COPY_FAILED"
                reason = ("the copy recorded target_missing, but the "
                          "catalog lists it now: nothing was copied into "
                          "it. Re-run 02_copy_schema. (copy: "
                          + str(c_rec.get("reason") or "no reason") + ")")
            elif c_status == "verified":
                verdict = "MIGRATED_VERIFIED"
            elif c_status == "verified_with_conversion":
                # Counts verified, but a column was copied under a type the
                # plan never reviewed (mapping.source_type_drift: convert).
                # Opted into, so not a problem; never a plain pass either.
                verdict = "MIGRATED_WITH_CONVERSION"
                reason = "converted after a source type change: " + ", ".join(
                    f'{c} ({d.get("planned")} -> {d.get("live")}, into '
                    f'{d.get("target")})' for c, d in
                    (c_rec.get("source_type_drift") or {}).items())
            elif c_status in ("count_mismatch", "sum_mismatch",
                              "sum_not_comparable", "type_drift", "failed"):
                verdict = "STRUCTURE_ONLY_COPY_FAILED"
            elif c_status == "skipped_nonempty" and \
                    c_rec.get("source_count") is not None and \
                    c_rec.get("target_count") != c_rec.get("source_count"):
                # The record carries both counts and they disagree: the
                # status alone cannot make that a pass.
                verdict = "STRUCTURE_ONLY_COPY_FAILED"
            elif c_status == "skipped_nonempty":
                verdict = "PRESENT_NOT_REVERIFIED"
            else:
                verdict = "STRUCTURE_ONLY"
            # Only against a structure report for THIS target: with none
            # (tables made by `deploy`, which writes no report, or a report
            # for another target), "a structure run stopped after creating
            # it" would send the operator to the wrong fix.
            if (exists and verdict == "STRUCTURE_ONLY" and structure
                    and name not in structure.get("objects", {})):
                s_status, reason = "unrecorded", UNRECORDED_REASON

            row = {"table": name, "structure": s_status, "copy": c_status,
                   "exists_in_target": exists, "verdict": verdict,
                   "reason": reason or c_rec.get("reason")
                             or (structure or {}).get("objects", {})
                             .get(name, {}).get("reason")}
            if counts and exists:
                row["target_count"], error = live_counts.get(
                    name, (None, "not counted"))
                if error is not None:
                    row["count_error"] = error
                # The live count is compared, not just printed: a verified
                # table emptied or changed out of band since the copy is a
                # problem, not a pass. A count that could not be read is
                # not drift, and a report that never recorded one has
                # nothing to compare against.
                reported = c_rec.get("target_count")
                if verdict in ("MIGRATED_VERIFIED",
                               "MIGRATED_WITH_CONVERSION") \
                        and row["target_count"] is not None \
                        and isinstance(reported, int) \
                        and row["target_count"] != reported:
                    verdict = row["verdict"] = "COUNT_DRIFT"
                    row["reason"] = (f"the copy report verified {reported:,} "
                                     f"row(s); the target now holds "
                                     f"{row['target_count']:,}. Changed "
                                     f"since the copy, not by it")
            rows.append(row)
            tally[verdict] = tally.get(verdict, 0) + 1

        # Views: every manifest view is listed rather than silently absent,
        # with what 01 recorded for it. This used to read a hard-coded
        # VIEW_NOT_CREATED_BY_THIS_PATH, so a view 01 had FAILED to create
        # came out under "No table is in a problem state", exit 0. SHOW
        # TABLES lists views on some catalogs and not others, and nothing
        # here looks for views specifically, so "not listed" is None (could
        # not look), never "no".
        view_rows = []
        live_views = None          # read once per schema, only if needed
        s_views = (structure or {}).get("views") or {}
        s_objects = (structure or {}).get("objects") or {}
        for view in manifest_views:
            name = view["name"]
            s_rec = s_views.get(name)
            if s_rec is None and str((s_objects.get(name) or {})
                                     .get("kind") or "").upper() == "VIEW":
                # A report written while views were recorded in `objects`.
                s_rec = s_objects[name]
            s_rec = s_rec or {}
            s_view = s_rec.get("status", "not_attempted")
            v_reason = s_rec.get("reason")
            if live is None:
                v_exists, v_verdict = None, "TARGET_UNREADABLE"
            elif not s_rec and name in (planned_views or {}).get(schema, ()):
                # Planned, and no run has recorded it: a ddl-plan run that
                # has not reached its view phase, not a path that never
                # creates views.
                v_exists = True if name.lower() in live else None
                v_verdict, v_reason = "VIEW_NOT_CREATED_YET", PLANNED_VIEW_REASON
            else:
                v_exists = True if name.lower() in live else None
                v_verdict = ((_VIEW_VERDICTS.get(s_view)
                              if s_rec or s_view != "not_attempted" else None)
                             or "VIEW_NOT_CREATED_BY_THIS_PATH")
            if v_verdict == "VIEW_CREATED":
                # The structure report says created; the target has to
                # agree. A view dropped since, or created somewhere else,
                # used to read VIEW_CREATED and exit 0 on 01's word alone.
                t_fqn = str(s_rec.get("target_fqn") or "")
                written = t_fqn.rsplit(".", 1)[-1] if t_fqn else name
                t_name = written.lower()     # listings are compared folded
                if live_views is None and not (t_name in (live or set())):
                    live_views = _live_views(spark, target_catalog,
                                             target_schema)
                v_exists = _view_exists(
                    spark, f"{q(target_catalog)}.{q(target_schema)}."
                           f"{q(written)}", t_name, live, live_views)
                if v_exists is False:
                    v_verdict = "VIEW_MISSING_DESPITE_REPORT"
                    v_reason = ("the structure report records it created, "
                                "but the target does not have it: dropped "
                                "since, or created elsewhere. Re-run "
                                "01_create_structure")
                elif v_exists is None:
                    v_verdict = "TARGET_UNREADABLE"
                    v_reason = ("the structure report records it created, "
                                "but whether the target has it could not be "
                                "read (SHOW TABLES, SHOW VIEWS and DESCRIBE "
                                "all failed)")
            view_rows.append({"view": name, "structure": s_view,
                              "exists_in_target": v_exists,
                              "verdict": v_verdict,
                              "reason": v_reason})
            tally[v_verdict] = tally.get(v_verdict, 0) + 1

        known = ({t["name"].lower() for t in tables}
                 | {v["name"].lower() for v in manifest_views})
        unclaimed = sorted(live - known) if live is not None else []
        out["schemas"].append({
            "schema": schema, "target_schema": target_schema,
            "target_readable": live is not None,
            "tables": rows,
            "views": view_rows,
            "in_target_but_not_in_manifest": unclaimed,
            "reports_ignored_for_other_catalog": ignored})

    out["totals"] = tally
    return out


def render(rec: dict) -> str:
    totals = rec["totals"]
    problems = sum(totals.get(v, 0) for v in PROBLEM_VERDICTS)
    lines = [
        "# Migration report — plan vs what the target catalog actually holds",
        "",
        f'Target: `{rec["target_catalog"]}` · generated {rec["generated_at"]}',
        "",
        "Verdicts: " + ", ".join(f'{k} = {v}'
                                 for k, v in sorted(totals.items())),
        "",
        (f'**{problems} object(s) need attention** '
         f'({", ".join(PROBLEM_VERDICTS)}).' if problems else
         "No table is in a problem state. Anything below that is not "
         "migrated simply has not been attempted yet — a migration runs "
         "schema by schema, so that is the expected middle of the project, "
         "not a failure."),
        "",
    ]
    for s in rec["schemas"]:
        lines += [f'## `{s["schema"]}` → `{rec["target_catalog"]}.'
                  f'{s["target_schema"]}`', ""]
        if not s["target_readable"]:
            lines += ["**Target schema UNREADABLE — nothing below is "
                      "confirmed, and this is not the same as empty.**", ""]
        for kind, other in (s.get("reports_ignored_for_other_catalog")
                            or {}).items():
            lines += [f"The {kind} report on disk targets `{other}`, not "
                      f"this target — ignored here.", ""]
        lines += ["| Table | Structure | Copy | In target | Verdict | Why |",
                  "|---|---|---|---|---|---|"]
        for t in s["tables"]:
            exists = {True: "yes", False: "**no**", None: "?"}[t["exists_in_target"]]
            count = (f' ({t["target_count"]:,} rows)'
                     if t.get("target_count") is not None else "")
            reason = (t.get("reason") or "").replace("|", "\\|")[:120]
            lines.append(f'| {t["table"]}{count} | {t["structure"]} | '
                         f'{t["copy"]} | {exists} | {t["verdict"]} | {reason} |')
        if s.get("views"):
            how = ("01_create_structure --mode ddl-plan creates the views "
                   "the approved plan carries, after every table.")
            if any(v["verdict"] == "VIEW_NOT_CREATED_BY_THIS_PATH"
                   for v in s["views"]):
                how += (" No ddl-plan run recorded the views marked "
                        "VIEW_NOT_CREATED_BY_THIS_PATH (--mode ctas and "
                        "manifest create tables only); create them with "
                        "that stage or `snowmig deploy --execute` (catalog "
                        "API) and verify them against the source.")
            lines += ["", "### Views", "",
                      how + " \"Listed\" is what SHOW TABLES returned; `?` "
                      "means it was not looked for.", "",
                      "| View | Structure | Listed in target | Verdict | Why |",
                      "|---|---|---|---|---|"]
            for v in s["views"]:
                listed = {True: "yes", None: "?"}.get(v["exists_in_target"], "?")
                reason = (v.get("reason") or "").replace("|", "\\|")[:120]
                lines.append(f'| {v["view"]} | {v["structure"]} | {listed} | '
                             f'{v["verdict"]} | {reason} |')
        if s["in_target_but_not_in_manifest"]:
            lines += ["", f'⚠️ In the target but in no report: '
                          f'{", ".join(s["in_target_but_not_in_manifest"])} — '
                          f'someone else\'s objects, or a stale manifest.']
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target-catalog", required=True)
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                    help="where this step saves its values (report/output in "
                         "the workspace); '' to skip")
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--ddl-plan",
                    help="path to ddl_plan.json (default: ../plan/"
                         "ddl_plan.json next to --reports-dir); its "
                         "target_fqn names the target schema, as in "
                         "01_create_structure")
    ap.add_argument("--counts", action="store_true",
                    help="also read a live COUNT(*) per existing table (one "
                         "batched query per 50 tables), and "
                         "flag a verified table whose count has changed since "
                         "the copy verified it (COUNT_DRIFT)")
    args = ap.parse_args(argv)

    reports = pathlib.Path(args.reports_dir)
    manifest = _load(reports, MANIFEST_NAME)
    if manifest is None:
        return fail(f"error: {reports / MANIFEST_NAME} not found; run 00_discover "
              f"first")

    ddl_path = (pathlib.Path(args.ddl_plan) if args.ddl_plan
                else reports.parent / "plan" / "ddl_plan.json")
    planned = None
    snapshots = None
    views = None
    if ddl_path.is_file():
        try:
            ddl_plan = read_plan_json(ddl_path)
        except ValueError as exc:
            return fail(str(exc))
        planned = plan_targets(ddl_plan, args.target_catalog)
        snapshots = plan_snapshots(ddl_plan, args.target_catalog)
        views = plan_views(ddl_plan, args.target_catalog)
    elif args.ddl_plan:
        return fail(f"error: --ddl-plan {ddl_path} is not there")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    rec = reconcile(spark, manifest=manifest,
                    target_catalog=args.target_catalog, reports=reports,
                    counts=args.counts, planned_targets=planned,
                    planned_snapshots=snapshots, planned_views=views)
    (reports / "reconciliation.json").write_text(json.dumps(rec, indent=2), encoding="utf-8")
    (reports / "MIGRATION_REPORT.md").write_text(render(rec), encoding="utf-8")
    log(f"totals: {rec['totals']}")
    log(f"-> {reports / 'MIGRATION_REPORT.md'}")
    write_step_output(args.output_dir, "S11_reconcile.json", {
        "step": "S11", "stage": "reconcile",
        "target_catalog": args.target_catalog, "totals": rec["totals"]})

    problems = sum(rec["totals"].get(v, 0) for v in PROBLEM_VERDICTS)
    pending = sum(v for k, v in rec["totals"].items()
                  if k not in PROBLEM_VERDICTS
                  and k not in ("MIGRATED_VERIFIED",
                                "MIGRATED_WITH_CONVERSION",
                                "PRESENT_NOT_REVERIFIED",
                                "VIEW_CREATED", "VIEW_NOT_IN_PLAN",
                                "VIEW_NOT_CREATED_BY_THIS_PATH",
                                "NOT_IN_PLAN"))
    scoped_out = rec["totals"].get("NOT_IN_PLAN", 0)
    if scoped_out:
        log(f"{scoped_out} table(s) are not in the approved plan "
            f"(NOT_IN_PLAN) -- left out by its scope, not pending.")
    if pending:
        log(f"{pending} table(s) not migrated yet — expected while the "
            f"migration is still running, schema by schema. Not an error.")
    if problems:
        log(f"{problems} object(s) in a PROBLEM state "
            f"({', '.join(PROBLEM_VERDICTS)}) — see the report.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
