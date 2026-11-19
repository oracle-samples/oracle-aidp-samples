"""Read-only Snowflake estate inventory. I/O injected as `run_sql`.

Issues only SHOW / SELECT / GET_DDL / DESCRIBE. Cannot modify the estate.

Things are captured HERE rather than reconstructed later, because they
cannot be recovered afterwards:
  * identifier_case_form -- SHOW output tells us which form was used
  * numeric precision/scale -- from INFORMATION_SCHEMA, never from sampled data
  * view SQL, verbatim -- both SHOW VIEWS.text and GET_DDL()
  * column DEFAULT and IDENTITY -- the two facts that change what an INSERT
    does after cutover; read here, decided in ddl/plan
  * PK / UNIQUE / FK -- see extract/constraints.py
  * a structured column's full type -- `VECTOR(FLOAT, 4)`, `MAP(K, V)`,
    `OBJECT(f T, ...)`, `ARRAY(T)` -- which INFORMATION_SCHEMA reduces to
    its first word. DESCRIBE TABLE, only for a table that holds one (see
    `_type_details`), recorded per column as `type_detail`

A per-object failure is recorded in extraction_notes and extraction continues.
"no objects" and "extraction failed" are different outcomes and must not be
conflated.

Two cost decisions, because an assessment must not be expensive:

  * ROW COUNTS default to SHOW metadata, which Snowflake maintains and serves
    for free and which is exact for a table. `COUNT(*)` is opt-in. On a VIEW a
    count has no metadata to read and must EXECUTE the view, so views are not
    counted unless asked -- on a wide join that is minutes of warehouse time
    per view, spent during what the user asked to be an assessment.
  * SHOW output is PAGINATED. SHOW caps at 10k rows, and a silently truncated
    inventory is the worst outcome available here: it looks complete.
"""
from __future__ import annotations

import collections
import datetime
from typing import Callable

from ..dialect import lexer
from ..dialect.identifiers import case_form, detect_collisions
from ..dialect.types import map_type, needs_type_detail
from .constraints import build_constraints

__all__ = ["build_inventory", "SYSTEM_DBS", "ROW_COUNT_MODES", "SHOW_PAGE_SIZE",
           "show_paged"]

SYSTEM_DBS = frozenset({"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"})

ROW_COUNT_MODES = ("metadata", "exact", "none")

# Snowflake truncates SHOW at 10k rows. Page just under it.
SHOW_PAGE_SIZE = 10_000

# Everything SHOW already hands us that we might need later. Free to capture,
# and the maintenance/layout group is the entire input to the maintenance
# assessment -- without it that question cannot even be asked.
_META_KEYS = ("rows", "bytes", "created_on", "comment", "owner",
              # layout and maintenance
              "cluster_by", "automatic_clustering", "change_tracking",
              "retention_time", "search_optimization",
              "search_optimization_bytes", "search_optimization_progress",
              # table kind, which changes what maintenance even applies
              "kind", "is_dynamic", "is_iceberg", "is_secure", "is_materialized",
              "is_external", "is_hybrid", "is_event", "is_immutable",
              "enable_schema_evolution")

# Deliberately NOT called exact. Snowflake maintains this count and it agrees
# with COUNT(*) for a settled standard table, but it can lag very recent DML
# and is not maintained for external tables. Labelling it "exact" would be the
# same overstatement as reporting a structure clone as a data clone.
_METADATA_COUNT_NOTE = (
    "Snowflake's maintained row count, from SHOW. Free to read, and agrees "
    "with COUNT(*) for a settled standard table; it can lag very recent DML "
    "and is not maintained for external tables. Use --row-counts exact for a "
    "verified COUNT(*).")

_VIEW_COUNT_NOTE = (
    "not counted: a view has no stored row count, so counting it means "
    "executing the view. Re-run with --row-counts exact to count views.")


def _jsonable(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    return value


def _show_all(run_sql: Callable[..., list[dict]], statement: str) -> list[dict]:
    """Run a SHOW statement, following pages until it stops filling one.

    SHOW returns at most 10k rows. `LIMIT n FROM '<name>'` resumes after a
    given name, and SHOW orders by name, so paging is exact rather than
    best-effort.

    The FROM argument is a plain NAME STRING, not a LIKE pattern: `_` and
    `%` are literal there. LIKE-escaping the cursor put a backslash before
    every underscore, naming an object that does not exist, and the walk
    resumed wherever that sorted -- repeating a page and silently dropping
    the tail of any schema with more than one page. Only the quote needs
    doubling.
    """
    rows: list[dict] = []
    cursor: str | None = None
    while True:
        page_sql = f"{statement} limit {SHOW_PAGE_SIZE}"
        if cursor is not None:
            literal = lexer.sql_literal(cursor)
            page_sql += f" from '{literal}'"
        page = run_sql(page_sql)
        rows.extend(page)
        if len(page) < SHOW_PAGE_SIZE:
            return rows
        last = page[-1].get("name")
        if not last or last == cursor:
            # No usable cursor: stop rather than loop forever, and say so.
            return rows
        cursor = last


def show_paged(run_sql: Callable[..., list[dict]], statement: str
               ) -> tuple[list[dict], str | None]:
    """Run a SHOW that may hit the row cap. Returns (rows, capped), where
    `capped` is None for a complete read and otherwise says why the rows are
    a lower bound.

    For the census and security reads, which are database- or
    account-scoped rather than schema-scoped. The bare statement goes first,
    so a result under the cap costs exactly one statement, as it always did.
    Only a result AT the cap is paged, and the paging is only trusted where
    the rows show it can be: a `FROM '<name>'` cursor is exact when the
    output is sorted by name, each page starts strictly after the cursor,
    and no other row shares the cursor's name. An `IN DATABASE` result
    ordered schema-first, a name that repeats across schemas at a page
    boundary, or a SHOW that refuses LIMIT/FROM cannot be paged that way,
    and the read is reported capped rather than silently short.
    """
    rows = list(run_sql(statement))
    if len(rows) < SHOW_PAGE_SIZE:
        return rows, None
    cap = (f"the SHOW result stopped at the {SHOW_PAGE_SIZE:,}-row cap and "
           f"could not be paged")
    names = [r.get("name") for r in rows]
    if not all(isinstance(n, str) and n for n in names) or names != sorted(names):
        return rows, f"{cap}: its rows are not in name order, so a name cursor would skip rows"
    page = rows
    while len(page) >= SHOW_PAGE_SIZE:
        cursor = page[-1]["name"]
        if names.count(cursor) > 1:
            return rows, (f"{cap}: the name {cursor!r} at the page boundary "
                          f"is shared by more than one object")
        literal = lexer.sql_literal(cursor)
        try:
            page = list(run_sql(
                f"{statement} limit {SHOW_PAGE_SIZE} from '{literal}'"))
        except Exception as exc:
            return rows, f"{cap}: LIMIT/FROM was refused -- {str(exc)[:160]}"
        more = [r.get("name") for r in page]
        if not all(isinstance(n, str) and n for n in more) \
                or more != sorted(more) or (more and more[0] <= cursor):
            return rows, f"{cap}: a page did not resume after its cursor"
        rows.extend(page)
        names.extend(more)
    return rows, None


def build_inventory(run_sql: Callable[..., list[dict]],
                    databases: list[str] | None = None, *,
                    row_counts: str = "metadata",
                    semi_structured: str = "block",
                    geospatial: str = "block",
                    timestamp_ntz: str = "preserve") -> dict:
    if row_counts not in ROW_COUNT_MODES:
        raise ValueError(
            f"unknown row_counts mode {row_counts!r}; expected one of "
            f"{list(ROW_COUNT_MODES)}")

    notes: list[str] = []
    session = run_sql(
        "select current_user() U, current_account() A, current_region() R, "
        "current_role() ROLE, current_warehouse() WH, current_version() V, "
        # CURRENT_ROLE alone is not the authority a read ran under: with
        # secondary roles active every role granted to the user is in
        # effect, so a count attributed to a restricted role can have been
        # produced with ACCOUNTADMIN. Live-verified 2026-09-23.
        "current_secondary_roles() SECONDARY_ROLES")[0]

    if not databases:
        databases = [r["name"] for r in _show_all(run_sql, "show databases")
                     if r["name"] not in SYSTEM_DBS]

    inventory: list[dict] = []
    for db in databases:
        try:
            schemas = [r["name"] for r
                       in _show_all(run_sql, f"show schemas in database {lexer.qualify(db)}")
                       if r["name"] != "INFORMATION_SCHEMA"]
        except Exception as exc:
            notes.append(f"database {db}: {exc}")
            continue

        # Once per database, not once per schema: the three SHOWs are
        # database-scoped. A failure is a note, not an empty estate.
        constraints = build_constraints(run_sql, db, notes, schemas=schemas)

        for schema in schemas:
            columns, columns_error = _columns(run_sql, db, schema, notes)
            for kind, show in (("TABLE", "tables"), ("VIEW", "views")):
                try:
                    objects = _show_all(
                        run_sql,
                        f"show {show} in schema {lexer.qualify(db, schema)}")
                except Exception as exc:
                    notes.append(f"{db}.{schema} {show}: {exc}")
                    continue
                for obj in objects:
                    inventory.append(
                        _record(run_sql, db, schema, kind, obj,
                                columns.get(obj["name"], []),
                                row_counts=row_counts, notes=notes,
                                semi_structured=semi_structured,
                                geospatial=geospatial,
                                timestamp_ntz=timestamp_ntz,
                                constraints=constraints.get(
                                    f'{db}.{schema}.{obj["name"]}', []),
                                columns_error=columns_error))

    collisions = detect_collisions([r["source_identifier"] for r in inventory])
    return {
        "probed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "session": {k: _jsonable(v) for k, v in session.items()},
        "databases_in_scope": databases,
        "row_count_mode": row_counts,
        "semi_structured_mode": semi_structured,
        "geospatial_mode": geospatial,
        "timestamp_ntz_mode": timestamp_ntz,
        "object_count": len(inventory),
        "counts_by_type": dict(collections.Counter(r["object_type"] for r in inventory)),
        "identifier_case_collisions": collisions,
        "extraction_notes": notes,
        "inventory": inventory,
    }


def _columns(run_sql, db: str, schema: str, notes: list[str]
             ) -> tuple[dict[str, list[dict]], str | None]:
    """Column metadata for one schema, keyed by object name, and the error
    text when the read FAILED (None when it answered).

    Read per schema rather than per database: one unfiltered query over a large
    database's INFORMATION_SCHEMA.COLUMNS can exceed Snowflake's result limit
    and fail, taking every object's types with it.

    The error is returned, not only noted, because an empty dict is also what
    a schema with nothing visible returns. Without it every object in a
    schema whose read timed out was typed over zero columns and came out
    `supported`.
    """
    by_obj: dict[str, list[dict]] = collections.defaultdict(list)
    try:
        rows = run_sql(
            f"select table_schema, table_name, ordinal_position, column_name, "
            f"data_type, is_nullable, numeric_precision, numeric_scale, "
            f"character_maximum_length, datetime_precision, comment, "
            # A collated text column maps to a bytewise STRING; the
            # collation is what says its comparisons change.
            f"collation_name, "
            # A column's DEFAULT and its identity sequence are the two facts
            # that make a post-cutover INSERT behave differently: an insert
            # Snowflake would have populated arrives NULL, or fails. They are
            # columns of INFORMATION_SCHEMA.COLUMNS and cost nothing extra to
            # read here; what is DONE with them is decided in ddl/plan.
            f"column_default, identity_start, identity_increment "
            f"from {lexer.qualify(db)}.information_schema.columns "
            f"where table_schema = %(schema)s "
            f"order by table_name, ordinal_position", {"schema": schema})
    except Exception as exc:
        notes.append(f"{db}.{schema} columns: {exc}")
        # Never an empty string: the caller tests this for truthiness, and
        # TimeoutError() has no message -- a blank here read as success.
        return by_obj, str(exc)[:300] or type(exc).__name__
    for c in rows:
        by_obj[c["TABLE_NAME"]].append(c)
    return by_obj, None


def _row_count(run_sql, db: str, schema: str, name: str, kind: str, *,
               row_counts: str, obj: dict, notes: list[str]) -> dict:
    """(count, source, note) for one object, per the chosen strategy."""
    if row_counts == "none":
        return {"row_count_exact": None, "row_count_source": "not_counted",
                "row_count_note": "row counts were not requested"}

    if row_counts == "metadata":
        if kind == "VIEW":
            return {"row_count_exact": None, "row_count_source": "not_counted",
                    "row_count_note": _VIEW_COUNT_NOTE}
        rows = obj.get("rows")
        if rows is None:
            return {"row_count_exact": None, "row_count_source": "not_counted",
                    "row_count_note": "SHOW returned no row count for this object"}
        return {"row_count_exact": rows, "row_count_source": "show_metadata",
                "row_count_note": _METADATA_COUNT_NOTE}

    try:
        value = run_sql(
            f"select count(*) N from {lexer.qualify(db, schema, name)}")[0]["N"]
    except Exception as exc:
        detail = str(exc)[:200]
        notes.append(f"{db}.{schema}.{name}: row count failed: {detail}")
        return {"row_count_exact": None, "row_count_source": "error",
                "row_count_note": detail}
    return {"row_count_exact": value, "row_count_source": "count_query",
            "row_count_note": "exact, from COUNT(*)"}


def _compatibility(blocked_reasons: list[str],
                   columns_error: str | None) -> str:
    """`unassessed` when the column read failed: no blocked reason over no
    columns is an absence of evidence, and must not read as `supported`."""
    if columns_error:
        return "unassessed"
    return "blocked" if blocked_reasons else "supported"


def _type_details(run_sql, db: str, schema: str, name: str,
                  columns: list[dict], notes: list[str]
                  ) -> tuple[dict[str, str], str | None]:
    """({column: DESCRIBE type}, error) for one table, or ({}, None) when no
    column of it needs the read.

    INFORMATION_SCHEMA.COLUMNS answers `VECTOR`, `MAP`, `OBJECT` or `ARRAY`
    and stops; DESCRIBE TABLE's `type` spells the element types (live
    2026-09-29, the shapes in tests/test_type_detail.py). One DESCRIBE per
    table would be a round trip per table on a real estate, so it is issued
    only when `needs_type_detail` says a column needs it -- a table of
    NUMBER and VARCHAR costs exactly what it did before. DESCRIBE is a read
    (conn.READ_ONLY_VERBS); a failure is returned, not raised, so the table
    is still inventoried and its columns say the detail is UNREAD.
    """
    if not any(needs_type_detail(c.get("DATA_TYPE"),
                                 c.get("DATETIME_PRECISION")) for c in columns):
        return {}, None
    try:
        rows = run_sql(f"describe table {lexer.qualify(db, schema, name)}")
    except Exception as exc:
        error = str(exc)[:200] or type(exc).__name__
        notes.append(f"{db}.{schema}.{name}: DESCRIBE TABLE for the full "
                     f"column types failed ({error}); structured columns are "
                     f"mapped as if their element types were unknown")
        return {}, error
    return {str(r["name"]): str(r["type"]) for r in rows
            if r.get("name") is not None and r.get("type")}, None


def _record(run_sql, db: str, schema: str, kind: str, obj: dict,
            columns: list[dict], *, row_counts: str, notes: list[str],
            semi_structured: str = "block", geospatial: str = "block",
            timestamp_ntz: str = "preserve",
            constraints: list[dict] | None = None,
            columns_error: str | None = None) -> dict:
    name = obj["name"]
    blocked_reasons: list[str] = []
    warnings: list[str] = []
    type_notes: list[str] = []

    details, detail_error = ({}, None)
    if kind == "TABLE":
        details, detail_error = _type_details(run_sql, db, schema, name,
                                              columns, notes)

    enriched = []
    for c in columns:
        c = dict(c)
        if kind == "TABLE" and needs_type_detail(
                c.get("DATA_TYPE"), c.get("DATETIME_PRECISION")):
            detail = details.get(str(c.get("COLUMN_NAME")))
            if detail:
                c["type_detail"] = detail
            else:
                # Never "plain": the column may be a typed VECTOR or OBJECT
                # nobody read, and the mapper has to be able to say so.
                c["type_detail_unread"] = (
                    f"DESCRIBE TABLE failed: {detail_error}" if detail_error
                    else "DESCRIBE TABLE returned no row for this column")
        m = map_type(c.get("DATA_TYPE"),
                     precision=c.get("NUMERIC_PRECISION"),
                     scale=c.get("NUMERIC_SCALE"),
                     char_length=c.get("CHARACTER_MAXIMUM_LENGTH"),
                     semi_structured=semi_structured,
                     geospatial=geospatial,
                     timestamp_ntz=timestamp_ntz,
                     collation=c.get("COLLATION_NAME"),
                     datetime_precision=c.get("DATETIME_PRECISION"),
                     type_detail=c.get("type_detail"),
                     type_detail_unread=c.get("type_detail_unread"))
        if m.blocked:
            blocked_reasons.append(f'{c["COLUMN_NAME"]}: {m.reason}')
        if m.warning:
            warnings.append(f'{c["COLUMN_NAME"]}: {m.warning}')
        # Notes are informational and must not raise the object's risk level,
        # so they are kept apart from warnings.
        if m.note and m.note not in type_notes:
            type_notes.append(m.note)
        enriched.append({**c, "target_type": m.spark_type})

    rec = {
        "source_identifier": f"{db}.{schema}.{name}",
        "object_type": kind,
        "source_database": db,
        "source_schema": schema,
        "identifier_case_form": case_form(name),
        "migration_status": "discovered",
        "compatibility_status": _compatibility(blocked_reasons, columns_error),
        "blocked_reasons": blocked_reasons,
        "warnings": warnings,
        "type_notes": type_notes,
        "evidence_location": f"show {kind.lower()}s in {db}.{schema}",
        # Whether the verdict above was computed over columns anyone READ.
        # "ok" with zero columns is a visibility fact; "failed" is a read
        # that never answered, and its error travels with the record.
        "columns_read": "failed" if columns_error else "ok",
        "columns": enriched,
        # PK/UNIQUE/FK as the source declares them. The DDL rule that says
        # they are "captured in the inventory, not emitted as DDL" is only
        # true because this key is populated.
        "constraints": list(constraints or []),
        "source_metadata": {k: _jsonable(obj[k]) for k in _META_KEYS if k in obj},
    }
    if columns_error:
        rec["columns_read_error"] = columns_error
    rec.update(_row_count(run_sql, db, schema, name, kind,
                          row_counts=row_counts, obj=obj, notes=notes))

    if kind == "VIEW":
        # Captured verbatim so a translation can be diffed against the source.
        # Migratability is decided by the dialect translator in plan/build.py,
        # not asserted here: this module reports what IS, not what we will do.
        rec["view_text_show"] = obj.get("text")
        try:
            rec["view_ddl_get_ddl"] = run_sql(
                "select get_ddl('view', %(f)s) D",
                {"f": lexer.qualify(db, schema, name)})[0]["D"]
        except Exception as exc:
            rec["view_ddl_error"] = str(exc)[:200]
            notes.append(f"{db}.{schema}.{name}: GET_DDL failed: {str(exc)[:200]}")
    return rec
