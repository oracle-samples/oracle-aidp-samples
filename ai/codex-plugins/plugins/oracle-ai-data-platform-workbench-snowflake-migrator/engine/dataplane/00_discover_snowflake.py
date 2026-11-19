#!/usr/bin/env python3
"""Discover the Snowflake estate from inside AIDP. Writes discovery_manifest.json.

USE `connector`. Discovering schemas and tables by walking three-part names
against an EXTERNAL catalog is the slow path and should not be the starting
point -- see the table below before reaching for it.

  connector (default)   ONE pushdown query pair against INFORMATION_SCHEMA
                        returns every schema, table and column in the
                        database. This is the mode that scales: a
                        200k-table estate costs a handful of queries, not a
                        DESCRIBE per object. It needs only the credentials
                        smoke already proved -- no catalog crawl. A table
                        holding a VECTOR / MAP / OBJECT / ARRAY column
                        costs one more read: its full column types, which
                        INFORMATION_SCHEMA does not carry, come from
                        GET_DDL, batched 50 tables to a qualified pushdown.

  external-catalog      SHOW SCHEMAS/TABLES + DESCRIBE per object against a
                        registered EXTERNAL catalog. Its cost grows with the
                        object count, so it does not finish at estate scale,
                        and it sees only what the crawler already discovered
                        -- one more precondition that has to be true first.
                        Use it only when a user explicitly asks.

The estate is READ-ONLY under both modes: the AIDP Snowflake connector is
read-only in 4.0, and an external catalog refuses DDL by contract. Nothing is
written anywhere except --reports-dir.

External-catalog mode is resumable per schema (each is flushed immediately);
connector mode returns the estate whole, so there is nothing to resume.

`--schemas` is pushed into the INFORMATION_SCHEMA queries as a predicate, not
applied after the fetch, and a scoped run MERGES into the manifest: the named
schemas are refreshed and every other schema is kept. A named schema that
returns no rows keeps its previous discovery, gains an error, and fails the
run; `--force` drops it instead. `--force` alone (no `--schemas`)
re-discovers the whole estate from an empty manifest.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import re
import sys
import traceback

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    SOURCE_MODES, SnowflakeSource, SourceConfigError, _database_name,
    _sql_literal, load_source_config, q, write_step_output)

# /Workspace is the live-verified mount of the workspace tree on cluster
# filesystems (probed 2026-09-16 on a real cluster).
DEFAULT_REPORTS_DIR = "/Workspace/backup-snowflake-migration/reports"
# The step's own values also go here, next to the accumulated run report
# the CLI publishes after every stage. `--output-dir ''` switches it off.
DEFAULT_OUTPUT_DIR = "/Workspace/report/output"
MANIFEST_NAME = "discovery_manifest.json"
# Runbook S6: the manifest is backed up, dated, BEFORE any later stage reads
# it. The reports/ copy is the working one and a re-run merges into it; the
# dated copy is what a later reader can trust was the input. It goes to the
# `backup/` folder beside `reports/` unless --backup-dir says otherwise.
BACKUP_FOLDER_NAME = "backup"

# Snowflake's own system schema: never a migration target.
_SYSTEM_SCHEMAS = {"information_schema"}

# `{where}` is the --schemas predicate, or nothing; `{flags}` is
# _KIND_FLAGS_SELECT, or nothing on the fallback read.
#
# TABLE_TYPE, IS_TRANSIENT and COMMENT are written to the manifest, not only
# read: the bridge maps them onto the same `source_metadata` keys a live
# `assess` takes from SHOW TABLES. TABLE_TYPE once chose only the tables or
# the views bucket, so an EVENT or EXTERNAL TABLE planned from a manifest
# was can_migrate while the laptop path refused it.
_TABLES_SQL = (
    "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_TRANSIENT, ROW_COUNT, "
    "BYTES, COMMENT{flags} "
    "from INFORMATION_SCHEMA.TABLES{where} order by TABLE_SCHEMA, TABLE_NAME")

# Dynamic, Iceberg and hybrid tables are BASE TABLEs by TABLE_TYPE; only
# these columns tell them apart. They are younger than the rest of the view,
# so an account that lacks one gets the narrower read and a recorded gap --
# not a whole discovery lost to `invalid identifier`, and not a guess.
_KIND_FLAGS = ("IS_DYNAMIC", "IS_ICEBERG", "IS_HYBRID")
_KIND_FLAGS_SELECT = "".join(f", {f}" for f in _KIND_FLAGS)

# COLUMN_DEFAULT, IDENTITY_* and COMMENT are here because the planning
# stages warn on them (R22, R23) and carry the comment into the CREATE
# TABLE. Without them a manifest-planned estate gets a DDL plan that is
# silent about defaults and identity columns that stop working at cutover,
# while the same estate planned from a laptop warns about both.
_COLUMNS_SQL = (
    "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, "
    "DATA_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE, "
    "CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, IDENTITY_START, "
    "IDENTITY_INCREMENT, COMMENT, COLLATION_NAME, DATETIME_PRECISION "
    "from INFORMATION_SCHEMA.COLUMNS{where} "
    "order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION")


def _schema_predicate(wanted: list[str] | None) -> str:
    """` where TABLE_SCHEMA in (...)` for --schemas, or an empty string.

    Pushed into the query rather than applied after the fetch. An unfiltered
    read over a large database's INFORMATION_SCHEMA can exceed Snowflake's
    result cap ("Information schema query returned too much data"), and a
    --schemas scope that only filtered client-side could do nothing about
    it. TABLE_SCHEMA is compared as a string value, so the names are
    literals, not identifiers.
    """
    if not wanted:
        return ""
    names = ", ".join(f"'{_sql_literal(s)}'" for s in wanted)
    return f" where TABLE_SCHEMA in ({names})"


def log(msg: str) -> None:
    print(f"[discover] {msg}", flush=True)


def fail(msg: str) -> int:
    """Report a refusal on BOTH streams and return 1.

    A notebook task captures stdout only: live, a script that exited 1 via a
    stderr-only message produced a job failure with NO explanation anywhere.
    """
    print(f"ERROR: {msg}", flush=True)
    print(f"error: {msg}", file=sys.stderr)
    return 1


def _plain(value):
    """JSON-safe scalar.

    Snowflake numerics arrive as `decimal.Decimal` through the connector and
    `json.dumps` refuses them ("Object of type Decimal is not JSON
    serializable") -- which happened AFTER a successful 1065-relation read,
    so the whole discovery was lost at the write. Integral values keep their
    exactness as ints; everything else becomes a string rather than a lossy
    float.
    """
    if value is None or isinstance(value, (int, str, bool)):
        return value
    try:
        as_int = int(value)
    except (TypeError, ValueError):
        return str(value)
    return as_int if as_int == value else str(value)


def _rows(df) -> list[dict]:
    return [r.asDict() for r in df.collect()]


def _first_key(row: dict, *candidates: str):
    """SHOW column names vary by engine version; take the first match rather
    than assuming one, and fail loudly when none is there."""
    for key in candidates:
        if key in row:
            return row[key]
    lowered = {k.lower(): v for k, v in row.items()}
    for key in candidates:
        if key.lower() in lowered:
            return lowered[key.lower()]
    raise KeyError(f"none of {candidates} in row: {sorted(row)}")


def _snowflake_type(col: dict) -> str:
    """The SOURCE type as Snowflake reports it, precision preserved.

    Deliberately NOT translated: translation belongs to the engine's type
    mapper, which refuses rather than guesses. This manifest records what IS.
    """
    base = str(col.get("DATA_TYPE") or "").upper()
    precision = _plain(col.get("NUMERIC_PRECISION"))
    scale = _plain(col.get("NUMERIC_SCALE"))
    length = _plain(col.get("CHARACTER_MAXIMUM_LENGTH"))
    if base in ("NUMBER", "DECIMAL", "NUMERIC") and precision is not None:
        return f"{base}({int(precision)},{int(scale or 0)})"
    if base in ("TEXT", "VARCHAR", "CHAR", "STRING") and length is not None:
        return f"{base}({int(length)})"
    return base


# Columns whose INFORMATION_SCHEMA type hides what the engine's mapper needs:
# `VECTOR`, `MAP`, `OBJECT`, `ARRAY` carry no element types there (live
# 2026-09-29), and a TIME/TIMESTAMP with no DATETIME_PRECISION carries no
# fraction. The SAME rule as `snowflake_source.dialect.types.
# needs_type_detail`, which this script cannot import (it is uploaded as a
# standalone file); tests/test_type_detail.py holds the two together.
_TYPE_DETAIL_BASES = ("VECTOR", "MAP", "OBJECT", "ARRAY", "GEOGRAPHY",
                      "GEOMETRY")
_TIME_BASES = ("TIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ",
               "TIMESTAMP_TZ")
# GET_DDL calls per pushdown. The live 50-table qualified UNION ALL count
# answered in 25 s; a pushdown per table costs ~8.5 s each.
_DDL_CHUNK = 50


def _needs_type_detail(data_type, datetime_precision=None) -> bool:
    base = str(data_type or "").strip().upper().split("(")[0].strip()
    if base in _TYPE_DETAIL_BASES:
        return True
    return base in _TIME_BASES and datetime_precision in (None, "")


def _scan_to(text: str, start: int, stop: str) -> int:
    """Index of the first `stop` character at depth 0 from `start`, outside
    string literals and quoted identifiers; len(text) when there is none.

    Snowflake's lexing: a backslash escapes inside '...', a doubled quote is
    a literal quote in either kind.
    """
    depth, i, n = 0, start, len(text)
    while i < n:
        c = text[i]
        if c in ("'", '"'):
            i += 1
            while i < n:
                if c == "'" and text[i] == "\\":
                    i += 2
                    continue
                if text[i] == c:
                    if text[i:i + 2] == c * 2:
                        i += 2
                        continue
                    break
                i += 1
        elif c == "(" and "(" in stop and depth == 0:
            return i
        elif c == "(":
            depth += 1
        elif c == ")":
            if depth == 0 and ")" in stop:
                return i
            depth -= 1
        elif c in stop and depth == 0:
            return i
        i += 1
    return n


_WORD = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
_TYPE_WORDS = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\s+(?:PRECISION|VARYING)\b)?",
                         re.IGNORECASE)
_TABLE_CONSTRAINT_WORDS = {"CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN"}
# A clustering clause that precedes the column list:
# `create or replace TABLE T cluster by (K)(cols...)`, or `LINEAR(K)`.
_CLUSTER_BY_TAIL = re.compile(r"\bcluster\s+by(?:\s+linear)?\s*$",
                              re.IGNORECASE)


def _column_list_opener(text: str) -> int:
    """Index of the "(" that opens the column list, or len(text).

    The first top-level "(" is the column list unless it belongs to a
    `cluster by (...)` written before it -- the order sqlglot's Snowflake
    dialect accepts, and likely GET_DDL's for a clustered table (not
    captured live). That group, nested expression keys included, is
    skipped; the clause written after the list needs nothing.
    """
    pos = 0
    while True:
        opener = _scan_to(text, pos, "(")
        if opener >= len(text):
            return len(text)
        if not _CLUSTER_BY_TAIL.search(text[pos:opener]):
            return opener
        pos = _scan_to(text, opener + 1, ")") + 1


def _ddl_column_types(ddl: str) -> dict[str, str]:
    """{column: declared type} from GET_DDL('TABLE') text.

    Only the type is taken -- its first word(s) and one balanced
    parenthesised group, `MAP(VARCHAR(16777216), NUMBER(38,0))` -- never the
    NOT NULL / DEFAULT / COMMENT after it, whose literals may hold commas
    and parentheses. A table constraint line (`primary key (ID)`) is not a
    column. A `cluster by (...)` before the column list is skipped. Text it
    cannot read yields what it could, possibly nothing.
    """
    text = ddl or ""
    opener = _column_list_opener(text)
    if opener >= len(text):
        return {}
    out: dict[str, str] = {}
    pos = opener + 1
    while pos < len(text):
        end = _scan_to(text, pos, ",)")
        item = text[pos:end].strip()
        pos = end + 1
        name, rest = None, ""
        if item.startswith('"'):
            close = 1
            while close < len(item):
                if item[close] == '"':
                    if item[close:close + 2] == '""':
                        close += 2
                        continue
                    break
                close += 1
            name, rest = item[1:close].replace('""', '"'), item[close + 1:]
        elif item:
            m = _WORD.match(item)
            if m and m.group(0).upper() not in _TABLE_CONSTRAINT_WORDS:
                name, rest = m.group(0), item[m.end():]
        rest = rest.lstrip()
        m = _TYPE_WORDS.match(rest) if name is not None else None
        if m:
            type_end = m.end()
            after = rest[type_end:].lstrip()
            if after.startswith("("):
                offset = len(rest) - len(after)
                type_end = min(_scan_to(rest, offset + 1, ")") + 1, len(rest))
            out[name] = rest[:type_end].strip()
        if end >= len(text) or text[end] == ")":
            break
    return out


def _qualified_literal(database: str, schema: str, table: str) -> str:
    """'"DB"."SCHEMA"."TABLE"' as a Snowflake string literal, for GET_DDL.

    The database is the config's value resolved the way every other
    pushdown resolves it (`_database_name`): quoted verbatim, a config's
    `snowmig_db` named a lower-case database that does not exist, and every
    type-detail read failed while the counts and copies worked."""
    name = ".".join('"' + str(p).replace('"', '""') + '"'
                    for p in (_database_name(database), schema, table))
    return "'" + _sql_literal(name) + "'"


def _get_ddl_sql(database: str, batch: list[tuple[str, str]]) -> str:
    return " union all ".join(
        f"select '{_sql_literal(schema)}' as SNOWMIG_SCHEMA, "
        f"'{_sql_literal(table)}' as SNOWMIG_TABLE, "
        f"get_ddl('table', {_qualified_literal(database, schema, table)}) "
        f"as SNOWMIG_DDL" for schema, table in batch)


def read_type_details(source: SnowflakeSource,
                      by_object: dict[tuple[str, str], list[dict]],
                      tables: list[tuple[str, str]]) -> None:
    """Record `type_detail` on every column that needs it, or say why not.

    There is no DESCRIBE through the connector: its pushdown takes a query.
    GET_DDL is a SELECT, its CREATE TABLE text spells the same full types
    DESCRIBE does, and many calls ride one UNION ALL -- fully qualified,
    because the pushdown session has no current schema and an unqualified
    name fails live ("Object does not exist"). A chunk that fails is retried
    table by table, so one unreadable table does not cost the other 49.
    Where nothing can be read the column carries `type_detail_unread`; the
    engine's mapper then treats the element types as unknown and says so.
    """
    def needing(key):
        return [c for c in by_object.get(key, [])
                if _needs_type_detail(c.get("data_type"),
                                      c.get("datetime_precision"))]

    wanted = [key for key in tables if needing(key)]
    if not wanted:
        return

    def mark(key, reason):
        for col in needing(key):
            col.setdefault("type_detail_unread", reason)

    try:
        database = source.database()
    except Exception:
        database = None
    if not database:
        for key in wanted:
            mark(key, "no source database is configured to qualify the "
                      "GET_DDL read with, and an unqualified name fails "
                      "through the pushdown; element types UNREAD")
        log(f"type detail: {len(wanted)} table(s) need it and no database "
            f"is configured to qualify GET_DDL; recorded as UNREAD")
        return

    def run(batch):
        rows = _rows(source.pushdown(_get_ddl_sql(database, batch)))
        return {(str(r.get("SNOWMIG_SCHEMA")), str(r.get("SNOWMIG_TABLE"))):
                r.get("SNOWMIG_DDL") for r in rows}

    ddls: dict[tuple[str, str], object] = {}
    for start in range(0, len(wanted), _DDL_CHUNK):
        batch = wanted[start:start + _DDL_CHUNK]
        try:
            ddls.update(run(batch))
            continue
        except Exception as exc:
            if len(batch) == 1:
                mark(batch[0], f"GET_DDL failed: {str(exc)[:200]}")
                continue
        for key in batch:
            try:
                ddls.update(run([key]))
            except Exception as exc:
                mark(key, f"GET_DDL failed: {str(exc)[:200]}")

    read = 0
    for key in wanted:
        if key not in ddls:
            mark(key, "GET_DDL returned no row for this table")
            continue
        types = _ddl_column_types(str(ddls[key] or ""))
        for col in needing(key):
            detail = types.get(col["name"])
            if detail:
                col["type_detail"] = detail
                col.pop("type_detail_unread", None)
                read += 1
            else:
                col.setdefault("type_detail_unread",
                               "the GET_DDL text carried no type for this "
                               "column that could be read")
    log(f"type detail: {read} column(s) read through GET_DDL for "
        f"{len(wanted)} table(s)")


def discover_via_connector(source: SnowflakeSource, *,
                           wanted: list[str] | None,
                           exclude: set[str]) -> list[dict]:
    """Every schema/table/column of the database (or of `wanted`), in two
    queries."""
    # INFORMATION_SCHEMA is per-database. The connector's `schema` option
    # must name a REAL schema (it rejects INFORMATION_SCHEMA itself with
    # DATA_ACCESS_LAYER_0031); the SQL below then reads INFORMATION_SCHEMA
    # relative to the database. Both established live.
    where = _schema_predicate(wanted)
    flags_unread = None
    try:
        tables = _rows(source.pushdown(
            _TABLES_SQL.format(where=where, flags=_KIND_FLAGS_SELECT)))
    except Exception as exc:
        if "invalid identifier" not in str(exc).lower():
            raise
        flags_unread = str(exc)[:300]
        log(f"INFORMATION_SCHEMA.TABLES has no {'/'.join(_KIND_FLAGS)} here "
            f"({flags_unread}); re-reading without them. Dynamic, Iceberg "
            f"and hybrid tables are NOT told apart from base tables in "
            f"this manifest")
        tables = _rows(source.pushdown(
            _TABLES_SQL.format(where=where, flags="")))
    columns = _rows(source.pushdown(_COLUMNS_SQL.format(where=where)))
    log(f"INFORMATION_SCHEMA: {len(tables)} relation(s), "
        f"{len(columns)} column(s), in {3 if flags_unread else 2} queries"
        + (f" scoped to {len(wanted)} schema(s)" if wanted else ""))

    by_object: dict[tuple[str, str], list[dict]] = {}
    for col in columns:
        key = (str(col["TABLE_SCHEMA"]), str(col["TABLE_NAME"]))
        by_object.setdefault(key, []).append(
            # The formatted `type` is for humans reading the manifest. The
            # four raw INFORMATION_SCHEMA fields below are what the migrator's
            # type mapper consumes, and they are carried verbatim: re-parsing
            # "NUMBER(38,0)" back into precision and scale would be a second,
            # lossier implementation of something we already have exactly.
            {"name": str(col["COLUMN_NAME"]),
             "type": _snowflake_type(col),
             "nullable": str(col.get("IS_NULLABLE") or "").upper() != "NO",
             "ordinal_position": _plain(col.get("ORDINAL_POSITION")),
             "data_type": _plain(col.get("DATA_TYPE")),
             "numeric_precision": _plain(col.get("NUMERIC_PRECISION")),
             "numeric_scale": _plain(col.get("NUMERIC_SCALE")),
             "character_maximum_length": _plain(
                 col.get("CHARACTER_MAXIMUM_LENGTH")),
             # A collated text column compares differently once it is a
             # Delta STRING; the engine's mapper warns on it.
             "collation": _plain(col.get("COLLATION_NAME")),
             # Precision 9 (Snowflake's default) loses three digits at the
             # Spark read; the mapper warns on anything above 6.
             "datetime_precision": _plain(col.get("DATETIME_PRECISION")),
             # What R22/R23 and the column comment read. Selecting them was
             # half a fix: until they were written here too, every manifest
             # said "facts unknown" and a re-run could never change that.
             "column_default": _plain(col.get("COLUMN_DEFAULT")),
             "identity_start": _plain(col.get("IDENTITY_START")),
             "identity_increment": _plain(col.get("IDENTITY_INCREMENT")),
             "comment": _plain(col.get("COMMENT")),
             # Read, so a None above is a real "none", never "unknown".
             "facts_recorded": True})

    # Views are not copied, so only a base table's structured columns are
    # worth the extra read.
    read_type_details(source, by_object, [
        (str(rel["TABLE_SCHEMA"]), str(rel["TABLE_NAME"])) for rel in tables
        if "VIEW" not in str(rel.get("TABLE_TYPE") or "BASE TABLE").upper()
        and str(rel["TABLE_SCHEMA"]).lower() not in exclude
        and not (wanted and str(rel["TABLE_SCHEMA"]) not in wanted)])

    schemas: dict[str, dict] = {}
    for rel in tables:
        schema = str(rel["TABLE_SCHEMA"])
        if schema.lower() in exclude or (wanted and schema not in wanted):
            continue
        name = str(rel["TABLE_NAME"])
        kind = str(rel.get("TABLE_TYPE") or "BASE TABLE").upper()
        record = schemas.setdefault(
            schema, {"name": schema, "tables": [], "views": [], "errors": []})
        entry = {"name": name,
                 "columns": by_object.get((schema, name), []),
                 "source_rows": _plain(rel.get("ROW_COUNT")),
                 "source_bytes": _plain(rel.get("BYTES")),
                 # Recorded as Snowflake reports them; the bridge maps them
                 # onto the planner's kind flags.
                 "table_type": kind}
        for field in ("IS_TRANSIENT", "COMMENT") + _KIND_FLAGS:
            if rel.get(field) is not None:
                entry[field.lower()] = _plain(rel.get(field))
        if flags_unread:
            entry["kind_flags_unread"] = flags_unread
        if not entry["columns"]:
            record["errors"].append(
                {"object": name, "kind": kind,
                 "error": "INFORMATION_SCHEMA.COLUMNS returned no column for "
                          "this relation, so it is INCOMPLETE, not empty"})
        bucket = record["views"] if "VIEW" in kind else record["tables"]
        bucket.append(entry)
    return [schemas[k] for k in sorted(schemas)]


def _describe(source: SnowflakeSource, schema: str, name: str) -> list[dict]:
    """Ordered [{name, type}] for one object. DESCRIBE emits section markers
    (`# Partitioning`, blank names) after the column list; stop at the first."""
    columns: list[dict] = []
    fqn = f"{q(source.external_catalog)}.{q(schema)}.{q(name)}"
    for row in _rows(source.spark.sql(f"DESCRIBE {fqn}")):
        col = str(row.get("col_name") or "").strip()
        if not col or col.startswith("#"):
            break
        columns.append({"name": col, "type": str(row.get("data_type") or "")})
    return columns


def discover_schema_via_catalog(source: SnowflakeSource, schema: str) -> dict:
    out = {"name": schema, "tables": [], "views": [], "errors": []}
    catalog = q(source.external_catalog)

    for row in _rows(source.spark.sql(f"SHOW TABLES IN {catalog}.{q(schema)}")):
        name = str(_first_key(row, "tableName", "table_name", "name"))
        try:
            out["tables"].append(
                {"name": name, "columns": _describe(source, schema, name)})
        except Exception as exc:  # one object must not eat the schema
            out["errors"].append({"object": name, "kind": "TABLE",
                                  "error": str(exc)[:300]})

    # SHOW VIEWS may not be supported against every external catalog; an
    # unreadable view list is RECORDED, never silently read as "no views".
    try:
        for row in _rows(source.spark.sql(
                f"SHOW VIEWS IN {catalog}.{q(schema)}")):
            name = str(_first_key(row, "viewName", "view_name", "name"))
            try:
                columns = _describe(source, schema, name)
            except Exception as exc:
                columns = []
                out["errors"].append({"object": name, "kind": "VIEW",
                                      "error": str(exc)[:300]})
            out["views"].append({"name": name, "columns": columns})
    except Exception as exc:
        out["errors"].append({"object": "*", "kind": "VIEW_LIST",
                              "error": f"SHOW VIEWS failed: {str(exc)[:300]}"})
    return out


def _unanswered_schemas(existing: list[dict], requested: set[str],
                        fresh: list[dict], exclude: set[str], *,
                        force: bool, keep: list[dict]) -> int:
    """Failures for --schemas names that returned no rows; the kept entries
    are appended to `keep`.

    Zero rows is not "the schema is gone". After a grant revocation
    INFORMATION_SCHEMA returns nothing, with no error, and a misspelt or
    wrongly-cased name does the same. The scoped merge used to drop the
    named schema and add nothing, so SALES vanished from the manifest --
    and from reconcile, failed copy and all -- with exit 0. Now the prior
    discovery is kept with the reason attached and the run fails; --force
    is the deliberate drop.
    """
    returned = {s["name"] for s in fresh}
    prior = {s["name"]: s for s in existing}
    failures = 0
    for name in sorted(requested - returned):
        if name.lower() in exclude:
            continue
        alike = sorted(n for n in prior if n.lower() == name.lower()
                       and n != name)
        hint = (f"; schema names are case-sensitive, and the manifest has "
                f"{', '.join(alike)}" if alike else "")
        if force:
            log(f"--schemas {name} returned no rows and --force was given: "
                + ("dropped from the manifest" if name in prior
                   else "nothing of that name to drop") + hint)
            continue
        failures += 1
        reason = (f"re-discovery with --schemas {name} returned no rows: not "
                  f"visible to this role, empty, or misspelled{hint}. The "
                  f"previous discovery is kept; --force drops it")
        log(f"SCHEMA RETURNED NO ROWS: {reason}")
        if name in prior:
            entry = dict(prior[name])
            entry["errors"] = [e for e in entry.get("errors") or []
                               if e.get("kind") != "REDISCOVERY"] + [
                {"object": "*", "kind": "REDISCOVERY", "error": reason}]
            keep.append(entry)
    return failures


def render_summary(manifest: dict) -> str:
    src = manifest.get("source") or {}
    where = (f'catalog `{src.get("external_catalog")}`'
             if src.get("mode") == "external-catalog"
             else f'`{src.get("host")}` / `{src.get("database")}` as '
                  f'`{src.get("user")}`')
    lines = ["# Discovery — what the Snowflake source exposes", "",
             f'Mode: `{src.get("mode")}` · {where}',
             f'{len(manifest["schemas"])} schema(s) · generated '
             f'{manifest.get("generated_at", "?")}', "",
             "| Schema | Tables | Views | Columns | Errors |",
             "|---|---|---|---|---|"]
    for s in manifest["schemas"]:
        cols = sum(len(t["columns"]) for t in s["tables"] + s["views"])
        mark = " ⚠️" if s["errors"] else ""
        lines.append(f'| {s["name"]} | {len(s["tables"])} | {len(s["views"])} '
                     f'| {cols} | {len(s["errors"])}{mark} |')
    lines += ["",
              "A schema with errors is INCOMPLETE, not empty — re-run with "
              "`--schemas <name>` (add `--force` in external-catalog mode) after "
              "fixing the cause; the other schemas are kept.", ""]
    return "\n".join(lines)


def backup_manifest(manifest: dict, backup_dir: str,
                    now: datetime.datetime | None = None) -> pathlib.Path | None:
    """Write the dated copy of the manifest into `backup_dir`; '' skips.

    Named by the UTC time of the run, never overwritten: every discovery
    leaves its own copy, so the input to a plan can be found again after a
    later re-run has merged new schemas into reports/.
    """
    if not backup_dir:
        return None
    stamp = (now or datetime.datetime.now(datetime.timezone.utc)
             ).strftime("%Y%m%dT%H%M%SZ")
    folder = pathlib.Path(backup_dir)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{pathlib.Path(MANIFEST_NAME).stem}_{stamp}.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-mode", choices=list(SOURCE_MODES),
                    default="connector",
                    help="connector (default): read Snowflake directly from "
                         "the cluster — one INFORMATION_SCHEMA query pair for "
                         "the whole estate. external-catalog: SHOW/DESCRIBE "
                         "against a registered EXTERNAL catalog, which needs "
                         "a successful crawl")
    ap.add_argument("--source-config",
                    help="JSON/YAML connection config (connector mode)")
    ap.add_argument("--source-catalog",
                    help="the registered EXTERNAL catalog "
                         "(external-catalog mode)")
    ap.add_argument("--session-schema",
                    help="a REAL schema used only to scope the connector's "
                         "pushdown session (default: `schema` from the source "
                         "config). The connector refuses INFORMATION_SCHEMA "
                         "here, so one real schema name is required")
    ap.add_argument("--schemas", nargs="*", default=None,
                    help="only these schemas (default: all). Pushed into the "
                         "INFORMATION_SCHEMA queries as a predicate, and "
                         "merged into the existing manifest: the other "
                         "schemas are kept")
    ap.add_argument("--exclude-schemas", nargs="*", default=[])
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                    help="where this step saves its values (report/output in "
                         "the workspace); '' to skip")
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--backup-dir", default=None,
                    help="where the dated copy of the manifest is written "
                         "(runbook S6). Default: the backup/ folder beside "
                         "--reports-dir; '' to skip")
    ap.add_argument("--force", action="store_true",
                    help="rediscover the named --schemas even if the manifest "
                         "already carries them (the others are kept), and "
                         "drop a named schema that returns no rows instead "
                         "of keeping it and failing; without --schemas, "
                         "rediscover the whole estate")
    args = ap.parse_args(argv)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    try:
        config = (load_source_config(args.source_config)
                  if args.source_config else None)
        source = SnowflakeSource(spark, mode=args.source_mode, config=config,
                                 external_catalog=args.source_catalog,
                                 session_schema=args.session_schema)
    except SourceConfigError as exc:
        return fail(f"error: {exc}")

    reports = pathlib.Path(args.reports_dir)
    reports.mkdir(parents=True, exist_ok=True)
    manifest_path = reports / MANIFEST_NAME
    identity = (source.external_catalog if args.source_mode
                == "external-catalog" else source.database())

    manifest = {"schemas": []}
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        # The identity guard holds under --force too: a --force against a
        # manifest written for another database must not overwrite it.
        if existing.get("source_identity") not in (None, identity):
            return fail(f"error: {manifest_path} describes "
                  f"{existing.get('source_identity')!r}, not {identity!r}. "
                  f"Use a fresh --reports-dir.")
        if args.force and not args.schemas:
            log("--force without --schemas: rediscovering the whole estate")
        else:
            manifest = existing

    manifest.update(source=source.describe(), source_identity=identity,
                    generated_at=datetime.datetime.now(
                        datetime.timezone.utc).isoformat())
    log(f"source: {json.dumps(source.describe())}")

    exclude = {s.lower() for s in args.exclude_schemas} | _SYSTEM_SCHEMAS
    failures = 0

    if args.source_mode == "connector":
        try:
            fresh = discover_via_connector(
                source, wanted=args.schemas, exclude=exclude)
        except Exception:
            failures += 1
            log(f"DISCOVERY FAILED:\n{traceback.format_exc(limit=3)}")
        else:
            if args.schemas:
                # A scoped run MERGES. Assigning the filtered result over the
                # loaded manifest used to drop every other schema -- exactly
                # what DISCOVERY.md's own "re-run with --schemas <name>"
                # advice then did to a finished discovery. An unscoped run is
                # the whole estate and stays authoritative.
                requested = set(args.schemas)
                kept = [s for s in manifest["schemas"]
                        if s["name"] not in requested]
                failures += _unanswered_schemas(
                    manifest["schemas"], requested, fresh, exclude,
                    force=args.force, keep=kept)
                fresh = sorted(kept + fresh, key=lambda s: s["name"])
            manifest["schemas"] = fresh
    else:
        done = {s["name"] for s in manifest["schemas"]}
        listed = [str(_first_key(r, "namespace", "databaseName",
                                 "schema_name", "name"))
                  for r in _rows(spark.sql(
                      f"SHOW SCHEMAS IN {q(source.external_catalog)}"))]
        wanted = [s for s in (args.schemas or listed)
                  if s.lower() not in exclude]
        log(f"{len(wanted)} schema(s) to discover; {len(done)} already done")
        for schema in wanted:
            if schema in done and not args.force:
                log(f"skip {schema}: already discovered (--force to redo)")
                continue
            try:
                record = discover_schema_via_catalog(source, schema)
            except Exception:
                failures += 1
                log(f"SCHEMA FAILED {schema}:\n{traceback.format_exc(limit=3)}")
                record = {"name": schema, "tables": [], "views": [],
                          "errors": [{"object": "*", "kind": "SCHEMA",
                                      "error": traceback.format_exc(
                                          limit=1)[-300:]}]}
            manifest["schemas"] = sorted(
                [s for s in manifest["schemas"] if s["name"] != schema]
                + [record], key=lambda s: s["name"])
            # Flush after EVERY schema: a large estate resumes, not restarts.
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            log(f"{schema}: {len(record['tables'])} table(s), "
                f"{len(record['views'])} view(s)")

    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (reports / "DISCOVERY.md").write_text(render_summary(manifest), encoding="utf-8")
    total = sum(len(s["tables"]) for s in manifest["schemas"])
    log(f"{len(manifest['schemas'])} schema(s), {total} table(s) "
        f"-> {manifest_path}")
    log(f"summary -> {reports / 'DISCOVERY.md'}")
    backup_dir = (str(reports.parent / BACKUP_FOLDER_NAME)
                  if args.backup_dir is None else args.backup_dir)
    backup_path = backup_manifest(manifest, backup_dir)
    if backup_path:
        log(f"backup -> {backup_path}")
    write_step_output(args.output_dir, "S06_discover.json", {
        "step": "S06", "stage": "discover",
        "schemas": len(manifest["schemas"]), "tables": total,
        "views": sum(len(s["views"]) for s in manifest["schemas"]),
        "failures": failures, "manifest": str(manifest_path),
        "backup": str(backup_path) if backup_path else None})

    if not manifest["schemas"]:
        log("ZERO schemas discovered. In external-catalog mode that usually "
            "means the catalog has not completed a crawl yet (check its "
            "refresh status and the crawler's network path to Snowflake); in "
            "connector mode it means these credentials see nothing, or "
            "DISCOVERY FAILED above (read that traceback first). Either "
            "way it is a FINDING, not a success.")
        return 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
