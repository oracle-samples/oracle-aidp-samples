#!/usr/bin/env python3
"""Copy ONE schema's tables from the external catalog into Delta, verified.

Runs on AIDP compute. Per table:

  1. read the source count;
  2. move the rows —
       skip-existing (default): only into a table with 0 rows; a table that
                                already holds rows is `skipped_nonempty`
                                when its count equals the source's and
                                `count_mismatch` when it does not -- a
                                re-run never softens a recorded failure;
       append:                  INSERT INTO ... SELECT <columns>;
       overwrite:               INSERT OVERWRITE ... SELECT <columns>
                                (rewrites ROWS, never drops the table);
     the source's columns are named, paired with the target's by name and
     listed in the target's order, so a source whose columns were reordered
     since the plan still lands each value in its own column;
  3. VERIFY: target count == source count (both read AFTER the copy), and
     with --verify counts+sums an exact SUM over every DECIMAL column OF
     THE SOURCE, at the SOURCE's scale on both sides -- in connector mode
     summed IN SNOWFLAKE (`sum("C")::VARCHAR`, one qualified pushdown), so
     digits the read lost are not lost from the check too -- compared as
     exact decimals. Floats are never summed for equality — float
     tolerance is wrong for money. A total past 38 digits cannot be held
     by either engine's SUM (DECIMAL(38, s) has 38 - s integer digits;
     Snowflake errors, non-ANSI Spark returns NULL): that column is listed
     under `sums_not_comparable` and the table is `sum_not_comparable`.

Before any row moves, and in both verify modes, the live source's columns
are checked against the target's BY NAME: a source column the target lacks,
or a target column the source lacks (renamed, dropped, added since the
plan), has no right place to land, so that table is recorded `type_drift`
and NOT copied. The source's DECIMAL columns are then checked against the
target's types: a target column that is not DECIMAL, or a DECIMAL with fewer
integer digits or a smaller scale, would be rounded or truncated by the
INSERT with the row count intact -- `type_drift` too, and NOT copied. In
connector mode EVERY column's live type is then checked against the type the
plan's spec was decided for (`source_type`): a column whose type changed
after the plan was approved would run a conversion chosen for its old type.
With `mapping.source_type_drift: refuse` (the default, recorded in
ddl_plan.json by `ddl`) the table is `type_drift` and NOT copied; with
`convert` the column is read under its NEW type into the existing target
column, the table's record names it under `source_type_drift` with a
warning, and a table whose counts then verify is `verified_with_conversion`,
never plain `verified`.

CONNECTOR MODE READS EACH TABLE WITH ONE QUALIFIED PUSHDOWN, never the
connector's table read (minutes a table, and lossy for NUMBER, TIME and
TIMESTAMP fractions and offsets; VECTOR, MAP and structured OBJECT tables
cannot be opened that way). The plan's per-column spec (`columns` on each TABLE
statement: read_expr, convert_expr) builds
`SELECT <read_expr> AS "<name>", ... FROM "DB"."SCHEMA"."TABLE"`, and the
INSERT selects `<convert_expr> AS <target column>` in the target's order.
A column the spec does not cover -- every column, for an older plan -- is
read bare, which is the connector's typing, and the table's record names
it under `read.bare`. The live source's columns come from
INFORMATION_SCHEMA (one query per chunk) for the pre-flight above.

The copy's claim is the verification, not the INSERT returning: every
table's row count is read back and compared with the source.

CONSISTENCY: each table is read at its own moment. If the source is still
being written, per-table counts can be exact and the SCHEMA still be
internally inconsistent. For a cutover: freeze writers or copy from a
point-in-time Snowflake CLONE. The report records copy timestamps so drift is
attributable.

Resumable: a table the report records as `verified` is skipped (--force
re-copies). Failures are recorded and the run continues; the report is the
deliverable. In `--mode append` each table's record is written the moment
its copy finishes; a table whose rows have landed but whose chunk's source
recount is still to come is written PROVISIONALLY as `failed` with
`insert_completed` and `awaiting_source_recount`. `--mode append` refuses a
table whose record says `insert_completed` (its rows are already there):
re-copy it with `--mode overwrite`. Outside `--mode append` the report is
written at most every REPORT_WRITE_INTERVAL seconds (and at every chunk's
end), so a job that stops can lose its last few records; the report's `run`
marker says a run did not finish, and `--mode append` then refuses the
tables that run could have touched -- resume in the stopped run's own mode
first. A later run over a narrower --tables scope carries the others
forward, still refused for append, until a run covers them.
"""
from __future__ import annotations

import argparse
import datetime
import decimal
import hashlib
import json
import pathlib
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    SOURCE_MODES, SnowflakeSource, SourceConfigError, _sql_ident,
    live_copy_expressions, load_source_config, read_plan_json,
    read_report_json, write_step_output)

# /Workspace is the live-verified mount of the workspace tree on cluster
# filesystems (probed 2026-09-16 on a real cluster).
DEFAULT_REPORTS_DIR = "/Workspace/backup-snowflake-migration/reports"
# The step's own values also go here, next to the accumulated run report
# the CLI publishes after every stage. `--output-dir ''` switches it off.
DEFAULT_OUTPUT_DIR = "/Workspace/report/output"
MANIFEST_NAME = "discovery_manifest.json"

_DECIMAL = re.compile(r"^decimal\((\d+)\s*,\s*(\d+)\)$", re.IGNORECASE)

# Copy statuses that mean the table is NOT verified. A later run that copies
# nothing (skip-existing over a table with rows) never softens one of these.
_COPY_FAILURES = ("count_mismatch", "sum_mismatch", "sum_not_comparable",
                  "type_drift", "failed")

# Copy statuses that mean the table is done and verified. The second is a
# table with a column copied under `mapping.source_type_drift: convert`: its
# counts verified, but a column was converted from a type nobody reviewed.
_COPY_DONE = ("verified", "verified_with_conversion")

# `mapping.source_type_drift`, recorded in ddl_plan.json by `ddl`.
SOURCE_TYPE_DRIFT_MODES = ("refuse", "convert")

# Structure statuses that mean the table IS there. A copy that then cannot
# find it has not "nothing to do": it failed to copy into a created table.
_STRUCTURE_PRESENT = ("created", "already_existed")


def q(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def three(*parts: str) -> str:
    return ".".join(q(p) for p in parts)


def log(msg: str) -> None:
    print(f"[copy] {msg}", flush=True)


def fail(msg: str) -> int:
    """Report a refusal on BOTH streams and return 1.

    A notebook task captures stdout only: live, a script that exited 1 via a
    stderr-only message produced a job failure with NO explanation anywhere.
    """
    print(f"ERROR: {msg}", flush=True)
    print(f"error: {msg}", file=sys.stderr)
    return 1


def _same(a: str, b: str) -> bool:
    """Spark resolves catalog and schema names case-insensitively, so two
    targets that differ only in case are the same place. Comparing them as
    exact strings dropped the structure report for `lake.core` when this run
    spelled it `lake.CORE`, and the copy fell back to the whole manifest."""
    return str(a).casefold() == str(b).casefold()


def planned_target_schemas(ddl_plan: dict, schema: str) -> set[str]:
    """Every target schema the approved plan puts source schema `schema` in.

    The same reading as 01_create_structure's `targets_from_ddl_plan`: a
    TABLE statement with a three-part `source_identifier` and a three-part
    `target_fqn`. 01 creates the table where the plan says, so the copy has
    to look there too -- deriving the schema from `--schema` again copied
    into `lake.CORE` while 01 had created `lake.db_core`.
    """
    out: set[str] = set()
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(source) != 3 or len(target) != 3:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        if source[1] == schema:
            out.add(target[1])
    return out


def planned_tables(ddl_plan: dict, schema: str,
                   target_schema: str) -> set[str]:
    """Casefolded source table names the plan puts at `target_schema`.

    The per-table reading of `planned_target_schemas`. `target_missing` for
    one of these is a failure whatever the structure report says: with
    `--tables` beside a report for another target, or before 01 ran at all,
    there is no report to say the table should be there -- and the copy
    exited 0 with 0 rows for a table the reviewed plan places here.
    """
    out: set[str] = set()
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(source) != 3 or len(target) != 3:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        if source[1] == schema and _same(target[1], target_schema):
            out.add(source[2].casefold())
    return out


def planned_snapshots(ddl_plan: dict, schema: str) -> list[str]:
    """Source names in `schema` the plan migrates as a TABLE SNAPSHOT.

    A TABLE statement with `snapshot_of`: a dynamic table or materialized
    view whose rows are copied into a table. In-AIDP discovery files a
    materialized view under the manifest's `views`, so the table filter
    below kept it out of the copy (live 2026-09-29: never copied). The
    plan's statement says it is a table; a real VIEW statement never is.
    """
    out: list[str] = []
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        if len(source) != 3 or source[1] != schema:
            continue
        if not stmt.get("snapshot_of"):
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        if source[2] not in out:
            out.append(source[2])
    return out


def plan_catalogs(ddl_plan: dict) -> set[str]:
    """Every catalog the plan targets (01 refuses a run for another one)."""
    out = set()
    for stmt in ddl_plan.get("statements") or []:
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(target) == 3:
            out.add(target[0])
    return out


def resolve_target_schema(schema: str, planned: set[str],
                          override: str | None) -> tuple[str | None, str | None]:
    """`(target_schema, None)`, or `(None, refusal)`: the rule 01 applies.

    The approved plan decides; `--target-schema` may restate it (in any
    case) but not contradict it; only where the plan is silent does the
    source schema name stand in.
    """
    if override:
        if not planned:
            return override, None
        match = next((p for p in sorted(planned) if _same(p, override)), None)
        if match is None:
            return None, (
                f"error: --target-schema {override!r} contradicts the "
                f"approved plan, which puts {schema} in "
                f"{', '.join(sorted(planned))} -- where 01_create_structure "
                f"created it. The plan is the reviewed artifact; change it, "
                f"or drop the flag.")
        return match, None
    if len(planned) == 1:
        return next(iter(planned)), None
    if len(planned) > 1:
        return None, (
            f"error: the approved plan puts source schema {schema} in more "
            f"than one target schema ({', '.join(sorted(planned))}); this "
            f"stage copies one schema per run. Pass --target-schema to say "
            f"which.")
    return schema, None


def _count(spark, fqn: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]


def _column_types(spark, fqn: str) -> dict[str, str]:
    """{column: data_type} from DESCRIBE, in column order; lower-cased types.

    Columns end at the first blank or `#` row (Delta's metadata section).
    """
    out: dict[str, str] = {}
    for row in spark.sql(f"DESCRIBE {fqn}").collect():
        name = str(row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        out[name] = str(row["data_type"] or "").strip().lower()
    return out


def _decimal_columns(types: dict[str, str]) -> list[tuple[str, int, int]]:
    """[(column, precision, scale)] for every DECIMAL column in `types`."""
    out = []
    for name, data_type in types.items():
        m = _DECIMAL.match(data_type)
        if m:
            out.append((name, int(m.group(1)), int(m.group(2))))
    return out


def _layout_drift(src_types: dict[str, str],
                  tgt_types: dict[str, str]) -> tuple[dict, str] | None:
    """`(drift, why)` when the source and target columns are not the same
    NAMES, else None. Case-insensitive, as Spark resolves column names.

    The target was checked against the plan by the structure step; nothing
    checked it against the LIVE source, which can have been rebuilt since.
    Column ORDER is not drift: the INSERT names every column and pairs them
    by name. A name that is on one side only is, since its values have no
    right place to land -- positionally, an email ended up in `city`.
    """
    for side, types in (("source", src_types), ("target", tgt_types)):
        folded: dict[str, list[str]] = {}
        for name in types:
            folded.setdefault(name.casefold(), []).append(name)
        clash = [names for names in folded.values() if len(names) > 1]
        if clash:
            return ({f"{side}_names_differing_only_in_case": clash[0]},
                    f"the {side} has columns whose names differ only in "
                    f"case ({', '.join(clash[0])}); Spark resolves them as "
                    f"one name, so which value lands where cannot be "
                    f"decided")
    src = {n.casefold() for n in src_types}
    tgt = {n.casefold() for n in tgt_types}
    not_on_target = [n for n in src_types if n.casefold() not in tgt]
    not_in_source = [n for n in tgt_types if n.casefold() not in src]
    if not (not_on_target or not_in_source):
        return None
    parts = []
    if not_on_target:
        parts.append(f"source column(s) {', '.join(not_on_target)} are not "
                     f"on the target")
    if not_in_source:
        parts.append(f"target column(s) {', '.join(not_in_source)} are not "
                     f"in the source")
    return ({"not_on_target": not_on_target, "not_in_source": not_in_source},
            "; ".join(parts) + " -- renamed, dropped or added since the plan")


def _column_pairs(src_types: dict[str, str],
                  tgt_types: dict[str, str]) -> list[tuple[str, str]]:
    """`[(target_column, source_column)]` in the TARGET's order, paired by
    name. Only called once `_layout_drift` found the same names both sides."""
    by_fold = {n.casefold(): n for n in src_types}
    return [(t, by_fold[t.casefold()]) for t in tgt_types]


def _type_drift(src_types: dict[str, str], tgt_types: dict[str, str]) -> dict:
    """Source DECIMAL columns the target cannot hold without silent loss.

    Keyed off the SOURCE: a source decimal whose target column is not a
    decimal, or a decimal with fewer integer digits (precision - scale) or a
    smaller scale, would be rounded, truncated or overflowed by the INSERT's
    store-assignment cast -- with the row count intact. A wider target is
    fine. Non-decimal columns are the structure stage's business.
    """
    by_lower = {k.lower(): v for k, v in tgt_types.items()}
    drift = {}
    for name, precision, scale in _decimal_columns(src_types):
        target = by_lower.get(name.lower())
        m = _DECIMAL.match(target or "")
        if not m or int(m.group(2)) < scale or \
                int(m.group(1)) - int(m.group(2)) < precision - scale:
            drift[name] = {"source": src_types[name],
                           "target": target or "<missing>"}
    return drift


def _planned_type_drift(live_types: dict[str, str],
                        spec: list[dict] | None) -> dict:
    """Columns whose LIVE source type is not the one the plan's spec was
    decided for: `{column: {"planned": ..., "live": ...}}`.

    Every column, not only DECIMAL ones. The spec's read and conversion were
    chosen for the planned type; run on another type they can round values
    or turn them NULL with the row count intact. A spec entry without a
    `source_type` (a plan written before it was recorded) cannot be
    compared and is left out.
    """
    live = {k.casefold(): (k, v) for k, v in live_types.items()}
    drift = {}
    for entry in spec or []:
        if not isinstance(entry, dict) or not entry.get("source_type"):
            continue
        found = live.get(str(entry.get("name") or "").casefold())
        if found is None:
            continue            # a missing column is the layout check's
        name, live_type = found
        planned = str(entry["source_type"]).strip().lower()
        if live_type.strip().lower() != planned:
            drift[name] = {"planned": planned, "live": live_type}
    return drift


def _decimal_sums(spark, fqn: str,
                  columns: list[tuple[str, int]]) -> tuple[dict, dict]:
    """`({column: total text or None}, {column: non-NULL count or None})`.

    The non-NULL count rides in the same query: non-ANSI Spark returns NULL
    for a DECIMAL(38, s) SUM that overflows, so a NULL total over values
    that are there is an overflow, not an empty column. None for a count
    means it was not read.
    """
    if not columns:
        return {}, {}
    selects = ", ".join(
        f"CAST(SUM(CAST({q(c)} AS DECIMAL(38,{s}))) AS STRING) AS {q(c)}, "
        f"COUNT({q(c)}) AS {q(f'snowmig_nonnull_{i}')}"
        for i, (c, s) in enumerate(columns))
    row = spark.sql(f"SELECT {selects} FROM {fqn}").collect()[0].asDict()
    return ({c: row.get(c) for c, _s in columns},
            {c: row.get(f"snowmig_nonnull_{i}")
             for i, (c, _s) in enumerate(columns)})


# What Spark says when a table, or the schema holding it, is simply not
# there. Only these mean "absent".
_NOT_FOUND = ("TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND",
              "NoSuchTableException", "NoSuchNamespaceException",
              "NoSuchDatabaseException", "Table or view not found")


def _target_exists(spark, tgt: str) -> bool:
    """True when DESCRIBE works, False when Spark says it is not there.

    Any OTHER error propagates. Every DESCRIBE error used to read as
    "absent": a metastore timeout or a persistent INSUFFICIENT_PERMISSIONS
    on a table 01 had just created became `target_missing` with the error
    thrown away -- "could not look" recorded as "not there", on every re-run.
    """
    try:
        spark.sql(f"DESCRIBE {tgt}")
        return True
    except Exception as exc:
        text = str(exc)
        if any(marker.lower() in text.lower() for marker in _NOT_FOUND):
            return False
        raise


def column_specs(ddl_plan: dict, schema: str) -> dict[str, list[dict]]:
    """`{source table: [column spec]}` for source schema `schema`.

    Each TABLE statement of a current plan carries `columns`: per column its
    source `name`, `target_type`, `read_expr` (a Snowflake expression over
    the quoted source column) and `convert_expr` (a Spark expression over a
    column of that exact name). A statement without it -- an older plan --
    contributes nothing, and the copy reads that table's columns bare.
    """
    out: dict[str, list[dict]] = {}
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        if len(source) != 3 or source[1] != schema:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        if isinstance(stmt.get("columns"), list):
            out[source[2]] = stmt["columns"]
    return out


# What a column read BARE does not carry: the connector's own typing, live
# 2026-09-29. Said on every table that has one, so a copy of an older plan
# never reads as the exact one.
_BARE_NOTE = ("read bare (the plan carries no read/convert expression for "
              "them), so they arrive as the connector types them: NUMBER "
              "beyond ~10 significant digits, TIME/TIMESTAMP fractions and "
              "TIMESTAMP_TZ offsets are NOT carried exactly, and a VECTOR, "
              "MAP or structured OBJECT column cannot be read at all. "
              "Re-run the migrator's `ddl` stage for per-column exact reads")


def column_reads(pairs: list[tuple[str, str]],
                 spec: list[dict] | None) -> list[dict]:
    """One read per target column, in the TARGET's order.

    `pairs` is `[(target_column, live_source_column)]`. A column the plan's
    spec covers is read with its `read_expr` and converted with its
    `convert_expr`; any other is read bare (`"NAME"`) and taken as it comes
    -- the documented fallback for an older plan.
    """
    by_fold = {str(c.get("name")).casefold(): c for c in spec or []
               if isinstance(c, dict) and c.get("name")}
    out = []
    for target, source in pairs:
        entry = by_fold.get(source.casefold())
        if entry and entry.get("read_expr") and entry.get("convert_expr"):
            out.append({"target": target, "name": str(entry["name"]),
                        "read_expr": str(entry["read_expr"]),
                        "convert_expr": str(entry["convert_expr"]),
                        "from_plan": True})
        else:
            out.append({"target": target, "name": source,
                        "read_expr": _sql_ident(source),
                        "convert_expr": q(source), "from_plan": False})
    return out


def _read_record(reads: list[dict]) -> dict:
    """What the read carried, column by column, for the table's record."""
    rec = {"via": "one qualified pushdown SELECT",
           "from_plan": [r["name"] for r in reads if r["from_plan"]],
           "bare": [r["name"] for r in reads if not r["from_plan"]]}
    if rec["bare"]:
        rec["note"] = f"{len(rec['bare'])} column(s) {_BARE_NOTE}"
    return rec


def _view_name(schema: str, table: str) -> str:
    """A temp view name unique to (schema, table), exact case included.

    Spark resolves view names case-insensitively, so `Orders` and `ORDERS`
    shared `snowmig_src_s_orders` -- harmless one table at a time, and one
    table's rows in the other's target once tables are copied in parallel.
    """
    digest = hashlib.sha1(f"{schema}\x00{table}".encode("utf-8")).hexdigest()
    readable = re.sub(r"[^a-z0-9_]", "_", f"{schema}_{table}".lower())[:80]
    return f"snowmig_src_{readable}_{digest[:12]}"


def copy_table(source, schema: str, table: str, tgt: str, *, mode: str,
               verify: str, retries: int = 2, retry_base_delay: float = 30.0,
               retry_multiplier: float = 2.0,
               source_count: int | None = None,
               live_columns: dict | list | None = None,
               column_spec: list[dict] | None = None,
               defer_recount: bool = False,
               source_type_drift: str = "refuse") -> dict:
    """Copy ONE table and verify it.

    Connector mode reads the table with ONE qualified pushdown built from
    the plan's column spec (`_copy_pushdown`), its live columns taken from
    INFORMATION_SCHEMA (`live_columns`, batched by the caller; looked up
    here when not given). External-catalog mode reads the three-part name
    as it always did (`_copy`): the spec's read expressions are Snowflake
    SQL and have no Snowflake session to run in there.
    """
    spark = source.spark
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # A table with no target is a FINDING, not a crash. Live, the copy died
    # on the sixth table of a schema because the approved plan covered five
    # and the manifest listed a thousand -- taking the whole run with it.
    try:
        exists = _target_exists(spark, tgt)
    except Exception as exc:
        return {"status": "failed", "started_at": started,
                "reason": f"could not DESCRIBE {tgt}: {str(exc)[:300]}. "
                          f"Whether it exists is UNKNOWN, so nothing was "
                          f"copied. NOT verified."}
    if not exists:
        return {"status": "target_missing", "started_at": started,
                "reason": f"{tgt} does not exist, so there is nothing to copy "
                          f"into. Most often the table is not in the approved "
                          f"plan (structure reports it `not_in_plan`); run "
                          f"01_create_structure for it first if it should be."}

    if getattr(source, "mode", None) == "connector":
        if live_columns is None:
            live_columns = source.live_columns(schema, [table]).get(table)
        return _copy_pushdown(
            source, schema, table, tgt, live_types=dict(live_columns or {}),
            spec=column_spec, mode=mode, verify=verify, retries=retries,
            retry_base_delay=retry_base_delay,
            retry_multiplier=retry_multiplier, started=started,
            source_count=source_count, defer_recount=defer_recount,
            source_type_drift=source_type_drift)

    view = _view_name(schema, table)
    src = source.register_temp_view(schema, table, view)
    try:
        return _copy(spark, src, tgt, mode=mode, verify=verify,
                     retries=retries, retry_base_delay=retry_base_delay,
                     retry_multiplier=retry_multiplier, started=started,
                     source_count=source_count)
    finally:
        source.drop_temp_view(view)


def _preflight(src_types: dict[str, str], tgt_types: dict[str, str], *,
               source_count, started: str) -> dict | None:
    """The refusal for a layout the rows cannot land in safely, or None.

    Metadata only, before anything is written and in both verify modes.
    """
    if not src_types or not tgt_types:
        # Nothing to pair by name: could not look, not a match.
        return {"status": "failed", "source_count": source_count,
                "started_at": started,
                "reason": f"DESCRIBE of the "
                          f"{'source' if not src_types else 'target'} "
                          f"returned no columns, so its layout cannot be "
                          f"compared with the other side's. NOT copied."}
    layout = _layout_drift(src_types, tgt_types)
    if layout:
        drift, why = layout
        return {"status": "type_drift", "layout_drift": drift,
                "source_count": source_count, "started_at": started,
                "reason": f"{why}. The source's columns are not the "
                          f"target's, so its rows have no right place to "
                          f"land. NOT copied. Re-plan the table from the "
                          f"live source, or restore the source's layout."}
    drift = _type_drift(src_types, tgt_types)
    if drift:
        return {"status": "type_drift", "type_drift": drift,
                "source_count": source_count, "started_at": started,
                "reason": f"{len(drift)} DECIMAL column(s) are narrower or "
                          f"not DECIMAL on the target; an INSERT would round "
                          f"or truncate them with the row count unchanged. "
                          f"NOT copied. Recreate the table from the approved "
                          f"plan."}
    return None


def _skip_existing(spark, tgt: str, *, mode: str, source_count: int,
                   started: str) -> dict | None:
    """The skip-existing verdict for a target that already holds rows."""
    target_rows = _count(spark, tgt)
    if mode != "skip-existing" or target_rows == 0:
        return None
    # A verification, not a bypass: a target that holds rows but not the
    # source's count is a mismatch whether or not this run wrote it.
    if target_rows != source_count:
        return {"status": "count_mismatch", "source_count": source_count,
                "target_count": target_rows, "started_at": started,
                "reason": f"target already holds {target_rows} row(s) "
                          f"but the source has {source_count}; nothing "
                          f"was copied. Use --mode overwrite to rewrite "
                          f"it. NOT verified."}
    return {"status": "skipped_nonempty", "source_count": source_count,
            "target_count": target_rows, "started_at": started,
            "reason": f"target already holds {target_rows} row(s); use "
                      f"--mode overwrite to rewrite them or append to add"}


def _insert(spark, statement: str, tgt: str, *, retries: int,
            retry_base_delay: float, retry_multiplier: float) -> str | None:
    """Run the INSERT with the retry policy; the last error, or None."""
    last_error = None
    for attempt in range(retries + 1):
        try:
            spark.sql(statement)
            return None
        except Exception as exc:
            last_error = str(exc)[:400]
            if attempt < retries:
                # Exponential, like the engine's shared policy: a failed Delta
                # INSERT commits nothing, so repeating it is safe.
                wait = retry_base_delay * retry_multiplier ** (attempt)
                log(f"  RETRY {tgt}: attempt {attempt + 2}/{retries + 1} in "
                    f"{wait:.1f}s after: {last_error[:120]}")
                time.sleep(wait)
    return last_error


def _copy(spark, src: str, tgt: str, *, mode: str, verify: str,
          retries: int, retry_base_delay: float, retry_multiplier: float = 2.0,
          started: str,
          source_count: int | None = None) -> dict:
    # The batched count from the caller when there is one: a per-table
    # COUNT(*) opens its own Snowflake session in connector mode.
    if source_count is None:
        source_count = _count(spark, src)

    # Pre-flight, before anything is written and in both verify modes:
    # metadata only (DESCRIBE on the registered source and on the target).
    src_types = _column_types(spark, src)
    tgt_types = _column_types(spark, tgt)
    refusal = _preflight(src_types, tgt_types, source_count=source_count,
                         started=started)
    if refusal:
        return refusal
    skipped = _skip_existing(spark, tgt, mode=mode, source_count=source_count,
                             started=started)
    if skipped:
        return skipped

    # Every source column named, in the TARGET's order: an INSERT fills the
    # target positionally, and `SELECT *` is the SOURCE's order, so a source
    # rebuilt with two columns swapped landed each value in the other's
    # column with the counts intact. (No target column list: the select
    # list already is the target's order, and it keeps the statement the
    # plain INSERT ... SELECT every Delta version accepts.)
    select = ", ".join(q(s) for _t, s in _column_pairs(src_types, tgt_types))
    verb = "INSERT OVERWRITE" if mode == "overwrite" else "INSERT INTO"
    statement = f"{verb} {tgt} SELECT {select} FROM {src}"
    last_error = _insert(spark, statement, tgt, retries=retries,
                         retry_base_delay=retry_base_delay,
                         retry_multiplier=retry_multiplier)
    if last_error is not None:
        return {"status": "failed", "source_count": source_count,
                "started_at": started, "reason": last_error}

    out = {"started_at": started,
           "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "mode": mode}

    # The verification IS the claim. The rows have landed by now, so a
    # failure from here on must say so: a `failed` record that looked like a
    # failed INSERT would invite an `append` re-run that duplicates every row.
    try:
        return _verify(spark, src, tgt, out, verify=verify,
                       source_count=source_count, src_types=src_types)
    except Exception as exc:
        return _verify_raised(out, exc)


def _verify_raised(out: dict, exc: Exception) -> dict:
    out.update(status="failed", insert_completed=True,
               reason=f"the INSERT completed but the verification raised: "
                      f"{str(exc)[:300]}. NOT verified; re-copy with "
                      f"--mode overwrite, not append")
    return out


def _source_count(source, schema: str, table: str) -> int:
    """One table's qualified source count (the batched call, for one)."""
    return int(source.source_counts(schema, [table])[table])


def _copy_pushdown(source, schema: str, table: str, tgt: str, *,
                   live_types: dict[str, str], spec: list[dict] | None,
                   mode: str, verify: str, retries: int,
                   retry_base_delay: float, retry_multiplier: float,
                   started: str, source_count: int | None,
                   defer_recount: bool = False,
                   source_type_drift: str = "refuse") -> dict:
    """The connector-mode copy: ONE qualified pushdown per table.

    `live_types` is the live source's `{column: type}` from
    INFORMATION_SCHEMA -- what the pre-flight compares with the target, as
    DESCRIBE of the source did on the old path. The read is built only
    after the pre-flight passes, from the target's columns in the target's
    order, so a refused table costs no source read at all.
    """
    spark = source.spark
    if not live_types:
        return {"status": "failed", "source_count": source_count,
                "started_at": started,
                "reason": f"INFORMATION_SCHEMA.COLUMNS lists no columns for "
                          f"{schema}.{table}: dropped or renamed since the "
                          f"plan, or not visible to this role. Its layout "
                          f"cannot be compared with the target's. NOT "
                          f"copied."}
    if source_count is None:
        source_count = _source_count(source, schema, table)
    tgt_types = _column_types(spark, tgt)
    refusal = _preflight(live_types, tgt_types, source_count=source_count,
                         started=started)
    if refusal:
        return refusal
    skipped = _skip_existing(spark, tgt, mode=mode, source_count=source_count,
                             started=started)
    if skipped:
        return skipped

    drift = _planned_type_drift(live_types, spec)
    if drift and source_type_drift != "convert":
        cols = ", ".join(f"{c} ({d['planned']} -> {d['live']})"
                         for c, d in drift.items())
        return {"status": "type_drift", "source_type_drift": drift,
                "source_count": source_count, "started_at": started,
                "reason": f"{len(drift)} source column(s) changed type "
                          f"since the plan was approved: {cols}. The plan's "
                          f"conversion was decided for the old type and "
                          f"could round values or turn them NULL with the "
                          f"row count intact. NOT copied. Re-run assess and "
                          f"plan to pick up the new type, or set "
                          f"mapping.source_type_drift: convert and re-run "
                          f"ddl (mapping.source_type_drift: refuse)."}

    reads = column_reads(_column_pairs(live_types, tgt_types), spec)
    converted = {}
    by_fold = {k.casefold(): k for k in tgt_types}
    for r in reads:
        if r["name"] not in drift:
            continue
        target_type = tgt_types[by_fold[r["target"].casefold()]]
        r["read_expr"], r["convert_expr"] = live_copy_expressions(
            drift[r["name"]]["live"], target_type, name=r["name"])
        converted[r["name"]] = {
            **drift[r["name"]], "target": target_type,
            "read_expr": r["read_expr"], "convert_expr": r["convert_expr"],
            "warning": f"copied under the mapping rules for its NEW type "
                       f"{drift[r['name']]['live']} into the existing "
                       f"{target_type} column (mapping.source_type_drift: "
                       f"convert). This conversion was never reviewed: "
                       f"values the target type cannot hold may be "
                       f"rounded or NULL, which the row count does not "
                       f"show"}
    read = _read_record(reads)
    if converted:
        read["converted"] = converted
    view = _view_name(schema, table)
    try:
        try:
            src = source.register_columns_view(
                schema, table, view,
                [(r["name"], r["read_expr"]) for r in reads])
        except Exception as exc:
            # Nothing was written: the read could not even be opened (live,
            # a bare VECTOR column is "Type:50003 is not a valid Types.java
            # value" at this point).
            return {"status": "failed", "source_count": source_count,
                    "started_at": started, "read": read,
                    "reason": f"the source read could not be opened: "
                              f"{str(exc)[:300]}. NOT copied."}
        # Each converted value aliased to its TARGET column, in the target's
        # order: the INSERT lands by position, and the select list IS the
        # target's column list (the form every Delta version accepts).
        select = ", ".join(f"{r['convert_expr']} AS {q(r['target'])}"
                           for r in reads)
        verb = "INSERT OVERWRITE" if mode == "overwrite" else "INSERT INTO"
        last_error = _insert(spark, f"{verb} {tgt} SELECT {select} FROM {src}",
                             tgt, retries=retries,
                             retry_base_delay=retry_base_delay,
                             retry_multiplier=retry_multiplier)
        if last_error is not None:
            return {"status": "failed", "source_count": source_count,
                    "started_at": started, "reason": last_error,
                    "read": read}
        out = {"started_at": started,
               "finished_at": datetime.datetime.now(
                   datetime.timezone.utc).isoformat(),
               "mode": mode, "read": read}
        if converted:
            out["source_type_drift"] = converted
        try:
            return _verify(spark, src, tgt, out, verify=verify,
                           source_count=source_count, src_types=live_types,
                           recount=lambda: _source_count(source, schema,
                                                         table),
                           defer=defer_recount,
                           source_sums=lambda cols: source.source_sums(
                               schema, table, cols))
        except Exception as exc:
            return _verify_raised(out, exc)
    finally:
        source.drop_temp_view(view)


# The status of a record whose source recount is still to come: the chunk's
# after-count is ONE batched query, read once every table in it is copied.
# Never written to a report as a status -- `_provisional` is what the report
# holds until `settle` replaces it.
AWAITING_RECOUNT = "awaiting_source_recount"

_PROVISIONAL_REASON = (
    "the INSERT completed; this table's source after-count is read once "
    "every table in its chunk is copied, and that has not happened yet. If "
    "this record is still here, the run stopped first. NOT verified; re-copy "
    "with --mode overwrite, not append")


def _provisional(result: dict) -> dict:
    """What the report holds for a table awaiting its chunk's recount.

    Written the moment the table's copy finishes, so a job that dies before
    the chunk settles -- a timeout, a lost driver -- leaves every table that
    holds rows with a record saying so. It is the record a verification
    that raised writes: `failed`, `insert_completed`, "not append"; the
    chunk's settle replaces it with the real verdict.
    """
    rec = {k: v for k, v in result.items() if k != "_pending"}
    rec.update(status="failed", insert_completed=True,
               awaiting_source_recount=True, reason=_PROVISIONAL_REASON)
    return rec


def _sums_equal(a, b) -> bool:
    """Two totals as EXACT decimals: `1.50` and `1.5` are one number.

    The source total is Snowflake's text and the target's is Spark's, and
    the two render trailing zeros differently; comparing the strings would
    call equal sums different. Nothing is rounded: Decimal equality is
    exact at any precision. None (SQL NULL) equals only None.
    """
    if a is None or b is None:
        return a is None and b is None
    try:
        return decimal.Decimal(str(a)) == decimal.Decimal(str(b))
    except decimal.InvalidOperation:
        return str(a) == str(b)


# What neither engine's SUM can hold, said on every counts+sums result.
_SUM_LIMIT = (
    "a total past 38 digits (DECIMAL(38, scale) holds 38 - scale integer "
    "digits) cannot be compared -- Snowflake's SUM errors, non-ANSI Spark's "
    "SUM returns NULL -- and is listed under sums_not_comparable")

# How the pushdown path sums, recorded on every counts+sums result.
_SUM_METHOD_SNOWFLAKE = (
    "source: Snowflake SUM(column)::VARCHAR in one qualified pushdown "
    "(exact NUMBER arithmetic at the source, past the connector's typing); "
    "target: Spark SUM(CAST(column AS DECIMAL(38, source scale))); "
    "compared as exact decimals; " + _SUM_LIMIT)
_SUM_METHOD_SPARK = (
    "source and target: Spark SUM(CAST(column AS DECIMAL(38, source "
    "scale))); compared as exact decimals; " + _SUM_LIMIT)

# What an overflowing decimal SUM says: Snowflake (100046, "Number out of
# representable range"; the text is Snowflake's documented error, not yet
# seen on the live estate) and Spark in ANSI mode.
_OVERFLOW = ("out of representable range", "100046", "numeric value out of "
             "range", "NUMERIC_VALUE_OUT_OF_RANGE", "ARITHMETIC_OVERFLOW",
             "DECIMAL_PRECISION_EXCEEDS")


def _is_overflow(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker.lower() in text for marker in _OVERFLOW)


def _fits_38(total, scale: int) -> bool:
    """Whether a total fits DECIMAL(38, scale): 38 - scale integer digits."""
    try:
        value = decimal.Decimal(str(total))
    except decimal.InvalidOperation:
        return True
    digits = value.adjusted() + 1 if value != 0 else 0
    return digits <= 38 - scale


def _snowflake_sums(source_sums, columns: list[tuple[str, int]],
                    not_comparable: dict) -> dict:
    """The Snowflake-side totals; an overflow is found column by column.

    ONE query for the table; only when Snowflake refuses it as an overflow
    is each column summed alone, so the columns that fit are still compared
    and the ones that do not are named. Any other error propagates -- that
    is a verification that raised.
    """
    names = [c for c, _s in columns]
    if not names:
        return {}
    try:
        return source_sums(names)
    except Exception as exc:
        if not _is_overflow(exc):
            raise
    out: dict = {}
    for c, scale in columns:
        try:
            out.update(source_sums([c]))
        except Exception as exc:
            if not _is_overflow(exc):
                raise
            not_comparable[c] = (
                f"Snowflake's SUM overflowed: the total exceeds 38 digits at "
                f"scale {scale} (NUMBER(38,{scale}) holds {38 - scale} "
                f"integer digit(s)) -- {str(exc)[:160]}")
    return out


def _sum_check(spark, src: str, tgt: str, src_types: dict[str, str],
               source_sums=None) -> dict:
    """The exact decimal-sum comparison, as a part `_settle` applies.

    The SOURCE's decimal columns, at the SOURCE's scale on both sides: the
    target is at least as wide (the pre-flight refused it otherwise), so the
    comparison is exact rather than rounded to whatever the target happens
    to be. `source_sums(columns)` sums the source where it lives (connector
    mode: in Snowflake); without it the registered source is summed in
    Spark, as the external-catalog path always has.
    """
    columns = [(c, s) for c, _p, s in _decimal_columns(src_types)]
    not_comparable: dict[str, str] = {}
    if source_sums is not None:
        src_sums = _snowflake_sums(source_sums, columns, not_comparable)
        src_nonnull: dict = {}
    else:
        src_sums, src_nonnull = _decimal_sums(spark, src, columns)
    for c, scale in columns:
        total = src_sums.get(c)
        if c in not_comparable:
            continue
        if total is not None and not _fits_38(total, scale):
            not_comparable[c] = (
                f"the source total {total} exceeds 38 digits at scale "
                f"{scale}: DECIMAL(38,{scale}) holds {38 - scale} integer "
                f"digit(s), and Spark's SUM over the target returns NULL "
                f"past that (non-ANSI)")
        elif total is None and src_nonnull.get(c):
            not_comparable[c] = (
                f"Spark's SUM over the source's {src_nonnull[c]} non-NULL "
                f"value(s) is NULL: the total exceeds 38 digits at scale "
                f"{scale} (non-ANSI Spark returns NULL on a DECIMAL "
                f"overflow)")
    comparable = [(c, s) for c, s in columns if c not in not_comparable]
    tgt_sums, tgt_nonnull = _decimal_sums(spark, tgt, comparable)
    drift = {}
    for c, scale in comparable:
        source_total, target_total = src_sums.get(c), tgt_sums.get(c)
        if target_total is None and tgt_nonnull.get(c):
            if source_total is not None:
                not_comparable[c] = (
                    f"Spark's SUM over the target's {tgt_nonnull[c]} "
                    f"non-NULL value(s) is NULL (a DECIMAL(38,{scale}) "
                    f"overflow, non-ANSI Spark) while the source total "
                    f"{source_total} fits: the target may hold other values, "
                    f"or Spark overflowed on a partial sum")
                continue
            # The source has no values to add and the target has some: a
            # real difference, though both totals read NULL.
            drift[c] = {"source": None, "target": None,
                        "target_non_null_values": tgt_nonnull[c]}
            continue
        if not _sums_equal(source_total, target_total):
            drift[c] = {"source": source_total, "target": target_total}
    return {"columns": [c for c, _ in columns],
            "method": _SUM_METHOD_SNOWFLAKE if source_sums is not None
            else _SUM_METHOD_SPARK,
            "not_comparable": not_comparable,
            "drift": drift}


def _settle(out: dict, *, source_count: int, src_after: int, tgt_after: int,
            sums: dict | None = None) -> dict:
    """The verdict from the counts read AFTER the copy (and the sums)."""
    out.update(source_count=src_after, target_count=tgt_after)
    if src_after != source_count:
        out["source_moved_during_copy"] = (
            f"source count changed {source_count} -> {src_after} during the "
            f"copy; the source is still being written")
    if tgt_after != src_after:
        out["status"] = "count_mismatch"
        out["reason"] = (f"target has {tgt_after} row(s), source has "
                         f"{src_after}. NOT verified.")
        return out
    if sums is not None:
        not_comparable = sums.get("not_comparable") or {}
        out["decimal_columns_checked"] = [c for c in sums["columns"]
                                          if c not in not_comparable]
        if sums.get("method"):
            out["sum_method"] = sums["method"]
        if not sums["columns"]:
            out["decimal_columns_note"] = ("the source has no DECIMAL "
                                           "columns; counts are the whole check")
        if not_comparable:
            out["sums_not_comparable"] = not_comparable
        if sums["drift"]:
            out["status"] = "sum_mismatch"
            out["sum_drift"] = sums["drift"]
            out["reason"] = (f"{len(sums['drift'])} decimal column(s) do not "
                             f"sum equal. NOT verified.")
            return out
        if not_comparable:
            out["status"] = "sum_not_comparable"
            out["reason"] = (
                f"counts equal, but {len(not_comparable)} decimal column(s) "
                f"({', '.join(not_comparable)}) total more than 38 digits, "
                f"which neither Snowflake's nor Spark's SUM can hold, so "
                f"their sums were NOT compared"
                + (f" (the other {len(out['decimal_columns_checked'])} sum "
                   f"equal)" if out["decimal_columns_checked"] else "")
                + ". NOT verified by sums. Re-copying does not change this: "
                  "re-copy the table with --mode overwrite --verify counts "
                  "to accept the count check, or compare those columns "
                  "another way.")
            return out
    # A converted column verified by counts is still a conversion nobody
    # reviewed: never plain `verified`.
    out["status"] = ("verified_with_conversion"
                     if out.get("source_type_drift") else "verified")
    return out


def settle(out: dict, src_after: int) -> dict:
    """Finish a record `_verify(defer=True)` left awaiting its recount."""
    pending = out.pop("_pending")
    out.pop("status", None)
    return _settle(out, source_count=pending["source_count"],
                   src_after=src_after, tgt_after=pending["target_count"],
                   sums=pending["sums"])


def _verify(spark, src: str, tgt: str, out: dict, *, verify: str,
            source_count: int, src_types: dict[str, str],
            recount=None, defer: bool = False, source_sums=None) -> dict:
    """Counts after the copy (and sums), then the verdict.

    `defer`: the target's count and the sums are read now, while the source
    view is registered; the source's after-count is left to the caller,
    which reads a whole chunk's in ONE batched query and calls `settle`.
    The verdict is the same one `_settle` gives here.
    """
    if defer:
        tgt_after = _count(spark, tgt)
        sums = (_sum_check(spark, src, tgt, src_types, source_sums)
                if verify == "counts+sums" else None)
        out["status"] = AWAITING_RECOUNT
        out["_pending"] = {"source_count": source_count,
                           "target_count": tgt_after, "sums": sums}
        return out
    # The source's count AFTER the copy: a COUNT(*) over the registered
    # source, or -- in connector mode -- the qualified pushdown count.
    src_after = recount() if recount else _count(spark, src)
    tgt_after = _count(spark, tgt)
    if tgt_after != src_after or verify != "counts+sums":
        return _settle(out, source_count=source_count, src_after=src_after,
                       tgt_after=tgt_after)
    return _settle(out, source_count=source_count, src_after=src_after,
                   tgt_after=tgt_after,
                   sums=_sum_check(spark, src, tgt, src_types, source_sums))


# Tables per chunk: one batched count BEFORE the chunk is copied and one
# AFTER it, and one INFORMATION_SCHEMA read. 50 is the size live-verified
# for one UNION ALL count (25 s, 2026-09-29).
COUNT_CHUNK = 50

# Tables copied at once. Live 2026-09-29 (100-row tables, qualified
# pushdown): 17.5 s a table serially, 5.9 s a table in 8 threads.
DEFAULT_PARALLEL = 8


def _batched_counts(source, schema: str, tables: list[str],
                    when: str) -> dict[str, int]:
    """`{table: count}` in ONE round trip, or {} -- the caller then counts
    each table on its own. Never raises: a count it could not batch is a
    count it reads one at a time, not a failed copy."""
    try:
        out = source.source_counts(schema, tables)
        log(f"source counts {when} the copy: {len(out)} table(s) in one "
            f"round trip")
        return out
    except Exception as exc:
        log(f"batched source counts {when} the copy unavailable "
            f"({str(exc)[:120]}); falling back to one count per table")
        return {}


def _copy_chunk(source, args, chunk: list[str], *, target_schema: str,
                connector: bool, specs: dict,
                on_done=None,
                source_type_drift: str = "refuse") -> dict[str, dict]:
    """Copy one chunk of tables, `args.parallel` at a time; `{table: result}`.

    Every table's result comes from the same `copy_table` on the same facts
    whatever the thread count -- only the moment it runs differs. In
    connector mode the source's after-count is left open by each table
    (`defer_recount`) and read here for the whole chunk in ONE query once
    every INSERT in it has finished, so a source that grew while its table
    was copied still shows as `count_mismatch`.

    `on_done(table, result)` is called from the worker the moment each
    table's copy returns -- before the chunk settles -- so the caller can
    write it (a result still `AWAITING_RECOUNT` as `_provisional`).
    """
    counts = _batched_counts(source, args.schema, chunk, "before")
    live: dict = {}
    live_error = None
    if connector:
        try:
            live = source.live_columns(args.schema, chunk)
        except Exception as exc:
            live_error = str(exc)[:300]
            log(f"the live source's columns could not be read "
                f"({live_error[:120]}); no table in this chunk is copied "
                f"without them")

    def one(name: str) -> dict:
        result = _one(name)
        if on_done is not None:
            on_done(name, result)
        return result

    def _one(name: str) -> dict:
        clock = time.monotonic()
        result = _one_untimed(name)
        # The elapsed time per table is the number that sizes a real
        # copy; the report held only its two timestamps.
        result["elapsed_s"] = round(time.monotonic() - clock, 1)
        return result

    def _one_untimed(name: str) -> dict:
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        tgt = three(args.target_catalog, target_schema, name)
        log(f"{args.schema}.{name}: copying ({args.mode})")
        try:
            if live_error is not None:
                raise RuntimeError(
                    f"the live source's columns could not be read from "
                    f"INFORMATION_SCHEMA ({live_error}), so its layout "
                    f"cannot be compared with the target's. NOT copied.")
            return copy_table(source, args.schema, name, tgt, mode=args.mode,
                              verify=args.verify,
                              source_count=counts.get(name),
                              retries=args.retries,
                              retry_base_delay=args.retry_base_delay,
                              retry_multiplier=args.retry_multiplier,
                              live_columns=(live.get(name, {}) if connector
                                            else None),
                              column_spec=specs.get(name),
                              defer_recount=connector,
                              source_type_drift=source_type_drift)
        except Exception as exc:
            # A failure is a finding, not the end of the run: live, one
            # connector login timeout would otherwise end the schema with
            # the failing table unrecorded and the rest never attempted.
            log(f"{args.schema}.{name}: FAILED — {str(exc)[:200]}")
            return {"status": "failed", "started_at": started,
                    "reason": str(exc)[:400]}

    workers = max(1, min(args.parallel, len(chunk)))
    if workers == 1:
        results = {name: one(name) for name in chunk}
    else:
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix="snowmig-copy") as pool:
            results = dict(zip(chunk, pool.map(one, chunk)))

    pending = [n for n in chunk if results[n].get("status") == AWAITING_RECOUNT]
    if pending:
        after = _batched_counts(source, args.schema, pending, "after")
        for name in pending:
            try:
                src_after = (after[name] if name in after
                             else _source_count(source, args.schema, name))
                results[name] = settle(results[name], src_after)
            except Exception as exc:
                rec = results[name]
                rec.pop("_pending", None)
                rec.pop("status", None)
                results[name] = _verify_raised(rec, exc)
    return results


# Live 2026-09-29 (scale test): the report is a growing file on the
# /Workspace mount, and one write per table was the per-table cost at
# thousands of tables. Outside --mode append it is written at most this
# often, and always at a chunk's end.
REPORT_WRITE_INTERVAL = 15.0
_last_write: dict[str, float] = {}


def _write_report(report: dict, path: pathlib.Path, *,
                  force: bool = True) -> None:
    """Write the report; with force=False, only if REPORT_WRITE_INTERVAL has
    passed since the last write of this file. Every caller that can lose
    a record a resumed run would act on passes force=True."""
    import time
    now = time.monotonic()
    key = str(path)
    if not force and now - _last_write.get(key, float("-inf")) < REPORT_WRITE_INTERVAL:
        return
    _last_write[key] = now
    report["updated_at"] = datetime.datetime.now(
        datetime.timezone.utc).isoformat()
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def _record(report: dict, path: pathlib.Path, args, name: str, result: dict,
            *, objects: dict | None, in_plan: set, target: str,
            target_schema: str, prior: dict | None = None) -> int:
    """Write one table's result into the report; 1 if it is a failure.

    `prior`: the record this run started from, when the report may already
    hold this run's own provisional record for the table.
    """
    if prior is None:
        prior = report["tables"].get(name, {})
    tgt = three(args.target_catalog, target_schema, name)
    failure = 0
    if result["status"] == "skipped_nonempty" and \
            prior.get("status") in _COPY_FAILURES:
        # Nothing was copied, so nothing was re-verified: a recorded
        # failure stands until a real re-copy verifies the table.
        result = dict(prior, reason=(
            f"{prior.get('reason') or prior['status']} (a re-run in "
            f"skip-existing mode left the target untouched; use --mode "
            f"overwrite to re-copy and re-verify it)"))
    # `target_missing` is a finding for a table the structure step never
    # created (not in the plan). For one it records as there, or one the
    # approved plan places at this target, the copy moved nothing into a
    # table that should exist: a failure, or the job reads SUCCESS with
    # 0 rows copied. The plan counts on its own: `--tables` beside a
    # report for another target, or a copy run before 01, has no report
    # for this target to say so.
    s_status = (objects or {}).get(name, {}).get("status")
    if result["status"] == "target_missing" and \
            s_status in _STRUCTURE_PRESENT:
        result["reason"] = (
            f"the structure report records this table `{s_status}` in "
            f"{target}, yet {tgt} is not there now -- dropped since, or "
            f"created somewhere else. NOT copied.")
        failure = 1
    elif result["status"] == "target_missing" and \
            name.casefold() in in_plan:
        result["reason"] = (
            f"the approved plan places this table in {target}, yet {tgt} "
            f"is not there -- 01_create_structure has not created it "
            f"there (run it first), or it was dropped since. NOT copied.")
        failure = 1
    elif result["status"] not in (*_COPY_DONE, "skipped_nonempty",
                                  "target_missing"):
        failure = 1
    report["tables"][name] = result
    # Per table only under --mode append (a resumed append re-appends
    # an unrecorded table); otherwise throttled -- see _write_report.
    _write_report(report, path, force=args.mode == "append")
    elapsed = result.get("elapsed_s")
    log(f"{args.schema}.{name}: {result['status']} "
        f"({result.get('target_count', '?')} row(s)"
        + (f", {elapsed:.0f}s" if isinstance(elapsed, (int, float)) else "")
        + ")")
    return failure


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-mode", choices=list(SOURCE_MODES),
                    default="connector",
                    help="how to READ the source (see snowmig_source.py). "
                         "connector is the default and needs no catalog "
                         "crawl")
    ap.add_argument("--source-config",
                    help="JSON/YAML connection config (connector mode)")
    ap.add_argument("--source-catalog",
                    help="the registered EXTERNAL catalog "
                         "(external-catalog mode)")
    ap.add_argument("--target-catalog", required=True)
    ap.add_argument("--schema", required=True,
                    help="ONE schema per run — that is the operating unit")
    ap.add_argument("--target-schema", default=None,
                    help="default: the schema the approved plan's "
                         "target_fqn names for --schema, as in "
                         "01_create_structure; --schema itself only where "
                         "the plan is silent. Refused when it contradicts "
                         "the plan")
    ap.add_argument("--ddl-plan",
                    help="path to ddl_plan.json (default: ../plan/"
                         "ddl_plan.json next to --reports-dir); read for "
                         "the target schema and each table's per-column "
                         "read spec")
    ap.add_argument("--tables", nargs="*", default=None,
                    help="subset; default: every table the manifest lists")
    ap.add_argument("--mode", choices=("skip-existing", "append", "overwrite"),
                    default="skip-existing")
    ap.add_argument("--verify", choices=("counts", "counts+sums"),
                    default="counts")
    # The same shape as the engine's `retry:` policy: attempts, then
    # base_delay * multiplier ** (attempt - 1) between them.
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--retry-base-delay", type=float, default=30.0)
    ap.add_argument("--retry-multiplier", type=float, default=2.0)
    ap.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL,
                    help=f"tables copied at once (default "
                         f"{DEFAULT_PARALLEL}; 1 = one after another). Each "
                         f"table's record is the same at any value")
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                    help="where this step saves its values (report/output in "
                         "the workspace); '' to skip")
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="re-copy tables already recorded as verified; needs "
                         "--mode overwrite or append, since skip-existing "
                         "cannot re-copy a table that holds rows")
    args = ap.parse_args(argv)

    if args.source_catalog and \
            args.source_catalog.lower() == args.target_catalog.lower():
        return fail("error: source and target catalog are the same.")
    if args.parallel < 1:
        return fail(f"error: --parallel {args.parallel}: at least 1 (1 copies "
                    f"the tables one after another)")
    if args.force and args.mode == "skip-existing":
        # Under the default mode --force copied nothing (skip-existing never
        # writes into a table with rows) and overwrote a `verified` record
        # with `skipped_nonempty`. Refuse rather than guess at overwrite.
        return fail("error: --force re-copies tables already recorded as "
                    "verified, which --mode skip-existing cannot do; pass "
                    "--mode overwrite (rewrites rows) or --mode append")

    reports = pathlib.Path(args.reports_dir)
    manifest = json.loads((reports / MANIFEST_NAME).read_text(encoding="utf-8"))
    record = next((s for s in manifest["schemas"] if s["name"] == args.schema),
                  None)
    if record is None:
        return fail(f"error: schema {args.schema!r} not in the manifest")

    # The approved plan decides the target namespace, exactly as it does for
    # 01_create_structure. A plan that is not there is a silent plan (01 in
    # --mode ctas or manifest never reads one); a plan the operator NAMED
    # and that is not there is a mistake worth stopping on.
    ddl_path = (pathlib.Path(args.ddl_plan) if args.ddl_plan
                else reports.parent / "plan" / "ddl_plan.json")
    planned: set[str] = set()
    ddl_plan = None
    if ddl_path.is_file():
        try:
            ddl_plan = read_plan_json(ddl_path)
        except ValueError as exc:
            return fail(str(exc))
        stray = {c for c in plan_catalogs(ddl_plan)
                 if not _same(c, args.target_catalog)}
        if stray:
            return fail(
                f"error: the approved plan targets catalog(s) "
                f"{', '.join(sorted(stray))}, and this run was given "
                f"--target-catalog {args.target_catalog}; "
                f"01_create_structure refuses that pair, so there is "
                f"nothing here it created. Point this run at the catalog "
                f"the plan names.")
        planned = planned_target_schemas(ddl_plan, args.schema)
    elif args.ddl_plan:
        return fail(f"error: --ddl-plan {ddl_path} is not there")
    target_schema, refusal = resolve_target_schema(
        args.schema, planned, args.target_schema)
    if refusal:
        return fail(refusal)
    log(f"target schema {target_schema}: "
        + ("from the approved plan" if planned else
           "from --target-schema" if args.target_schema else
           "the source schema's own name (the plan names none for it)"))

    path = reports / f"copy_report_{args.schema.lower()}.json"
    target = f"{args.target_catalog}.{target_schema}"
    in_plan = (planned_tables(ddl_plan, args.schema, target_schema)
               if ddl_plan else set())

    # What the structure step recorded for THIS target. A report for another
    # target is not evidence about this one -- and falling back to the whole
    # manifest because of it is how a drifted table, excluded there, came
    # back into the copy's scope. Refused unless --tables names the scope.
    structure_path = reports / f"structure_report_{args.schema.lower()}.json"
    objects = None
    if structure_path.is_file():
        s_prior = read_report_json(structure_path)
        s_target = s_prior.get("target")
        if s_target and not _same(s_target, target):
            if not args.tables:
                # --target-schema is a way out only where the plan is
                # silent; where it names a target, that flag is refused as a
                # contradiction, so offering it pointed at a dead end.
                way_out = ("" if planned else
                           f"pass --target-schema "
                           f"{s_target.split('.', 1)[-1]} (the plan names no "
                           f"target for this schema), ")
                return fail(
                    f"error: the structure report for {args.schema} targets "
                    f"{s_target}, and this copy resolves {target}. Taking "
                    f"the scope from the manifest instead would copy into "
                    f"tables the structure step never created or checked "
                    f"there. Re-run 01_create_structure (it creates what "
                    f"the plan names), {way_out}or pass --tables")
            log(f"the structure report targets {s_target}, not {target}; "
                f"not used for this run (--tables sets the scope)")
        else:
            objects = s_prior.get("objects") or {}

    report = {"schema": args.schema, "tables": {}, "target": target}
    if path.exists():
        prior = read_report_json(path)
        # Resumability is keyed by SOURCE schema, so a report written against
        # a DIFFERENT target must not let this run skip copies as already
        # verified (the same trap the structure script hit live). Compared
        # case-insensitively: `lake.CORE` and `lake.core` are one schema.
        if prior.get("target") and not _same(prior["target"], target):
            log(f"the previous report targeted {prior['target']}, not "
                f"{target} — starting a fresh record for this target")
            path.with_suffix(
                f".{prior['target'].replace('.', '_')}.json").write_text(
                    json.dumps(prior, indent=2), encoding="utf-8")
        else:
            report = prior
            report["target"] = target

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    try:
        config = (load_source_config(args.source_config)
                  if args.source_config else None)
        source = SnowflakeSource(spark, mode=args.source_mode, config=config,
                                 external_catalog=args.source_catalog)
    except SourceConfigError as exc:
        return fail(f"error: {exc}")
    report["source"] = source.describe()

    if args.tables:
        # Deduplicated: the copies run in parallel, and a name given twice
        # was copied by two workers at once (both count 0 rows, both INSERT,
        # and they share one temp view).
        names = list(dict.fromkeys(args.tables))
    else:
        # Default to what the structure step created for THIS target, when it
        # left a report: the manifest is the whole estate, and copying into
        # tables nobody approved is not a default worth having. A table it
        # found already there WITH the planned layout counts; one it recorded
        # as `type_drift` never does -- the copy below is a positional INSERT
        # INTO ... SELECT *, and that layout is not the plan's.
        # Tables only: a VIEW the structure step created (an older report
        # records views in `objects`), or any name the manifest does not
        # list as a table (nor the plan as a table snapshot, below), is
        # never an INSERT target -- live, the copy
        # scoped "7 table(s) ... (the manifest lists 4)" and wrote into
        # the schema's views.
        # The plan's table snapshots count as tables wherever the manifest
        # put them (a materialized view is under its `views`, or absent).
        snapshots = (planned_snapshots(ddl_plan, args.schema)
                     if ddl_plan else [])
        manifest_tables = {t["name"] for t in record["tables"]} | set(snapshots)
        created = [n for n, rec in (objects or {}).items()
                   if rec.get("status") in _STRUCTURE_PRESENT
                   and str(rec.get("kind") or "").upper() != "VIEW"
                   and n in manifest_tables]
        if created:
            names = created
            log(f"scope: {len(names)} table(s) the structure step created for "
                f"{target} (the manifest lists "
                f'{len(record["tables"])} for this schema)')
        else:
            # The `created` filter above never runs when nothing was created,
            # and that is exactly the all-drift schema: a re-plan over tables
            # that all pre-exist with the old layout records every one of
            # them `type_drift`. They are excluded here too, or the fallback
            # copies into the very layout the structure step refused.
            drifted = {n for n, r in (objects or {}).items()
                       if r.get("status") == "type_drift"}
            names = [t["name"] for t in record["tables"]
                     if t["name"] not in drifted]
            names += [n for n in snapshots
                      if n not in drifted and n not in names]
            if objects is None:
                why = f"no structure report for {target} was found"
            else:
                # The report IS there; saying it was not pointed the operator
                # away from the real cause (a plan that never covered this
                # schema). Tables it created nothing for will come back
                # `target_missing` below.
                nip = sum(1 for r in objects.values()
                          if r.get("status") == "not_in_plan")
                why = (f"the structure report for {target} records 0 created "
                       f"table(s) ({nip} not_in_plan, {len(drifted)} "
                       f"type_drift, {len(objects) - nip - len(drifted)} "
                       f"other) -- re-run 01_create_structure with the right "
                       f"ddl_plan.json, or pass --tables")
                if drifted:
                    why += (f"; {len(drifted)} type_drift table(s) excluded "
                            f"-- recreate them from the approved plan")
                if drifted and not names:
                    # Zero iterations below would be exit 0: a copy job that
                    # did nothing, reported as a success.
                    return fail(
                        f"error: every table the manifest lists for "
                        f"{args.schema} is recorded type_drift in the "
                        f"structure report for {target}; nothing to copy. "
                        f"Recreate them from the approved plan "
                        f"(01_create_structure) or pass --tables to override")
            log(f"scope: all {len(names)} table(s) the manifest lists for "
                f"this schema"
                + (f" or the plan migrates as a table snapshot "
                   f"({len(snapshots)})" if snapshots else "")
                + f"; {why}")

    todo = [n for n in names
            if args.force
            or report["tables"].get(n, {}).get("status") not in _COPY_DONE]
    for name in names:
        if name not in todo:
            log(f"skip {args.schema}.{name}: already verified")
        elif args.dry_run:
            log(f"DRY RUN: would copy {args.schema}.{name} -> "
                f"{three(args.target_catalog, target_schema, name)} "
                f"({args.mode}, verify={args.verify}, "
                f"source={args.source_mode}, parallel={args.parallel})")
    if args.dry_run:
        todo = []

    # Connector mode reads each table with ONE qualified pushdown built from
    # the plan's per-column spec, and checks the LIVE source's columns --
    # from INFORMATION_SCHEMA, one query per chunk -- against the target's
    # first. External-catalog mode reads the three-part name as before.
    connector = getattr(source, "mode", None) == "connector"
    specs = column_specs(ddl_plan, args.schema) if ddl_plan else {}
    # What to do with a column whose live type is not the planned one. An
    # unknown value is refused like the default: `convert` is opt-in.
    drift_mode = str((ddl_plan or {}).get("source_type_drift")
                     or "refuse").strip().lower()
    if drift_mode not in SOURCE_TYPE_DRIFT_MODES:
        log(f"ddl_plan.json carries source_type_drift={drift_mode!r}, not "
            f"one of {', '.join(SOURCE_TYPE_DRIFT_MODES)}; refusing drifted "
            f"columns")
        drift_mode = "refuse"
    if connector and todo:
        without = [n for n in todo if n not in specs]
        log(f"reads: {len(todo) - len(without)} table(s) with the plan's "
            f"per-column read spec, {len(without)} read bare"
            + (" (an older ddl_plan.json, or no plan)" if without else ""))
        log(f"source type drift: {drift_mode} (mapping.source_type_drift in "
            f"ddl_plan.json) -- a column whose live type is not the planned "
            f"one is " + ("copied under its new type and the table recorded "
                          "verified_with_conversion" if drift_mode == "convert"
                          else "refused and its table recorded type_drift"))
    elif specs and todo:
        log("the plan's per-column read spec is NOT applied in "
            "external-catalog mode (its read expressions are Snowflake SQL); "
            "columns are read as the external catalog types them")
    log(f"{len(todo)} table(s) to copy, {args.parallel} at a time, in "
        f"chunk(s) of {COUNT_CHUNK}")

    failures = 0
    # The run marker. A run outside --mode append writes its report at most
    # every REPORT_WRITE_INTERVAL seconds, so one that stops can leave a
    # table holding rows with no record. Appending after it would add those
    # rows a second time; a resume in the stopped run's own mode re-checks
    # every table instead (skip-existing skips one that holds rows).
    # The marker names the tables the run may touch (`todo`). A finished run
    # over a narrower --tables scope does not clear an unfinished run's
    # other tables: they are carried forward (`carried`), unverified, and
    # the marker stays unfinished until a run covers them.
    prev_run = report.get("run") or {}
    unfinished = (bool(prev_run) and not prev_run.get("finished")
                  and prev_run.get("mode") not in (None, "append"))
    at_risk = None
    if unfinished and "todo" in prev_run:
        at_risk = [n for n in dict.fromkeys(
                       [*(prev_run.get("carried") or []),
                        *(prev_run.get("todo") or [])])
                   if report["tables"].get(n, {}).get("status")
                   not in _COPY_DONE]
    if not args.dry_run and args.mode == "append" and unfinished:
        blocked = (todo if at_risk is None
                   else [n for n in todo if n in set(at_risk)])
        if blocked:
            listed = ", ".join(blocked[:20]) + (
                f" and {len(blocked) - 20} more" if len(blocked) > 20 else "")
            return fail(
                f"error: the previous copy run of {args.schema} (--mode "
                f"{prev_run.get('mode')}, started "
                f"{prev_run.get('started_at')}) did not finish, and its last "
                f"records may not have been written: a table it copied in its "
                f"last seconds can hold rows with no record, and --mode "
                f"append would add them a second time. Tables at risk: "
                f"{listed}. Resume with --mode {prev_run.get('mode')} first; "
                f"append once that run has finished.")
    if not args.dry_run:
        carried = ([n for n in at_risk if n not in set(todo)]
                   if unfinished and at_risk is not None else [])
        report["run"] = {"mode": args.mode, "finished": False,
                         "started_at": datetime.datetime.now(
                             datetime.timezone.utc).isoformat(),
                         "todo": list(todo)}
        if carried:
            report["run"].update(carried=carried,
                                 carried_mode=prev_run.get("carried_mode")
                                 or prev_run.get("mode"))
            log(f"{len(carried)} table(s) from an unfinished earlier run are "
                f"outside this run's scope and stay unverified; --mode append "
                f"is refused for them until a run covers them")
        _write_report(report, path)
    # A record that says the rows already landed (a verification that
    # raised, or a provisional record a stopped run left) is refused in
    # append mode: appending adds every one of those rows a second time.
    if args.mode == "append":
        landed = [n for n in todo
                  if report["tables"].get(n, {}).get("insert_completed")]
        for name in landed:
            prior = report["tables"][name]
            failures += _record(report, path, args, name, dict(
                prior, status="failed", reason=(
                    f"NOT copied: --mode append refused, because the "
                    f"previous run's INSERT into this table completed and "
                    f"was never verified -- its rows are there, and "
                    f"appending would add them a second time. Re-copy with "
                    f"--mode overwrite. (previous: "
                    f"{str(prior.get('reason') or prior.get('status'))[:300]})")),
                objects=objects, in_plan=in_plan, target=target,
                target_schema=target_schema)
        todo = [n for n in todo if n not in landed]

    lock = threading.Lock()
    for start in range(0, len(todo), COUNT_CHUNK):
        chunk = todo[start:start + COUNT_CHUNK]
        # The records this chunk starts from, and the report's order before
        # it: the settled chunk is written in the manifest's order whatever
        # order its workers finished in.
        priors = {n: report["tables"].get(n, {}) for n in chunk}
        before = list(report["tables"])
        recorded: set[str] = set()

        def done(name, result, priors=priors, recorded=recorded):
            nonlocal failures
            with lock:
                if result.get("status") == AWAITING_RECOUNT:
                    report["tables"][name] = _provisional(result)
                    # Per table only under --mode append (a resumed append re-appends
                    # an unrecorded table); otherwise throttled -- see _write_report.
                    _write_report(report, path, force=args.mode == "append")
                    return
                failures += _record(report, path, args, name, result,
                                    objects=objects, in_plan=in_plan,
                                    target=target, target_schema=target_schema,
                                    prior=priors[name])
                recorded.add(name)

        results = _copy_chunk(source, args, chunk, target_schema=target_schema,
                              connector=connector, specs=specs, on_done=done,
                              source_type_drift=drift_mode)
        with lock:
            for name in chunk:
                if name not in recorded:
                    failures += _record(
                        report, path, args, name, results[name],
                        objects=objects, in_plan=in_plan, target=target,
                        target_schema=target_schema, prior=priors[name])
            seen = set(before)
            order = before + [n for n in chunk if n not in seen]
            report["tables"] = {n: report["tables"][n] for n in order
                                if n in report["tables"]}
            _write_report(report, path)

    if not args.dry_run:
        # Still unfinished while tables from an earlier stopped run remain
        # outside every run that has finished since.
        report["run"].update(finished=not report["run"].get("carried"),
                             finished_at=datetime.datetime.now(
                                 datetime.timezone.utc).isoformat())
        _write_report(report, path)
    statuses = {}
    for t in report["tables"].values():
        statuses[t["status"]] = statuses.get(t["status"], 0) + 1
    log(f"{args.schema}: {statuses} -> {path}")
    write_step_output(args.output_dir, f"S11_copy_{args.schema.lower()}.json", {
        "step": "S11", "stage": "copy", "schema": args.schema,
        "mode": args.mode, "statuses": statuses, "failures": failures})
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
