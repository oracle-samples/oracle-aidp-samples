"""Table constraints for one Snowflake database. I/O injected as `run_sql`.

Why this exists: the DDL audit trail asserted that PK/FK/UNIQUE were
"captured in the inventory" (rule R20), and nothing captured them. A rule
that states a falsehood is worse than a missing rule, because it is the part
of the report a reviewer trusts to be mechanical.

Three SHOW statements per database, all on the read-only transport's
allowlist:

    SHOW PRIMARY KEYS IN DATABASE <db>
    SHOW UNIQUE KEYS IN DATABASE <db>
    SHOW IMPORTED KEYS IN DATABASE <db>

A read that comes back at the 10,000-row SHOW cap is re-read per schema
(`... IN SCHEMA`), and a schema at the cap per table (`... IN TABLE`, for
the tables INFORMATION_SCHEMA.TABLE_CONSTRAINTS lists), so a large database
costs more statements rather than silently losing keys.

SHOW is used rather than `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` because these
statements carry the COLUMN NAMES and their ordinal position
(`key_sequence`), which is the whole point: "ORDERS has a primary key" is not
actionable, "ORDERS(ORDER_ID, LINE_NO) is the primary key" is.
`IMPORTED KEYS` is the child-side view, so a foreign key is recorded on the
table that HAS it.

WHAT IS NOT HERE, and why:
  * CHECK. Snowflake does not support CHECK constraints at all -- UNIQUE,
    PRIMARY KEY, FOREIGN KEY and NOT NULL are the whole set -- so the one
    constraint class Delta genuinely ENFORCES has nothing to carry over.
    That is a fact about the source, not a gap in this reader, and the DDL
    rule says so rather than leaving a reader to assume CHECK was missed.
  * NOT NULL. It is a column property and comes from
    `INFORMATION_SCHEMA.COLUMNS.is_nullable`, which the column reader
    already selects and the DDL emits.

Nothing here decides anything: it reports what the source declares. Snowflake
does not ENFORCE PK/UNIQUE/FK either (they are metadata, and RELY governs
whether the optimiser trusts them), and `rely` is carried so that is
visible rather than assumed.
"""
from __future__ import annotations

from typing import Callable

from ..dialect import lexer

__all__ = ["build_constraints", "CONSTRAINT_STATEMENTS"]

CONSTRAINT_STATEMENTS = ("show primary keys in database",
                         "show unique keys in database",
                         "show imported keys in database")

# A SHOW stops at this many rows and still succeeds. Live 2026-09-29: SHOW
# PRIMARY KEYS IN DATABASE over 20,000 keyed tables returned exactly 10,000
# rows, and the other 10,000 primary keys read as "none declared". These
# SHOWs take no LIMIT/FROM, so a read at the cap is narrowed instead: schema
# by schema, then table by table (see _complete_schema).
SHOW_ROW_CAP = 10_000


def _get(row: dict, key: str):
    """One SHOW cell, whatever case the driver returned the key in."""
    if key in row:
        return row[key]
    for k, v in row.items():
        if str(k).lower() == key:
            return v
    return None


def _sequence(row: dict) -> int:
    try:
        return int(_get(row, "key_sequence") or 0)
    except (TypeError, ValueError):
        return 0


def _keyed(rows: list[dict], constraint_type: str, prefix: str = "") -> dict:
    """Group key rows into one entry per constraint, columns in key order."""
    grouped: dict[tuple, dict] = {}
    for row in rows:
        db = _get(row, f"{prefix}database_name")
        schema = _get(row, f"{prefix}schema_name")
        table = _get(row, f"{prefix}table_name")
        if not (db and schema and table):
            continue
        name = (_get(row, "constraint_name") or _get(row, "fk_name")
                or _get(row, "pk_name") or constraint_type)
        ident = f"{db}.{schema}.{table}"
        entry = grouped.setdefault((ident, name), {
            "constraint_type": constraint_type, "name": str(name),
            "columns": [], "rely": _get(row, "rely"), "_rows": []})
        entry["_rows"].append(row)
    out: dict[tuple, dict] = {}
    for key, entry in grouped.items():
        rows_in_order = sorted(entry.pop("_rows"), key=_sequence)
        entry["columns"] = [str(_get(r, f"{prefix}column_name"))
                            for r in rows_in_order
                            if _get(r, f"{prefix}column_name") is not None]
        out[key] = entry
    return out


def _complete_schema(run_sql, db: str, schema: str, what: str,
                     constraint_type: str, prefix: str, capped: list[dict]
                     ) -> tuple[list[dict], str | None]:
    """A schema's key rows when its SHOW is itself at the cap: (rows, why
    incomplete or None).

    The tables that declare this constraint type come from INFORMATION_SCHEMA.
    TABLE_CONSTRAINTS, which has no row cap but no column names; each table
    not proven complete in the capped result is then read on its own. A
    result in name order is complete for every table but its last one (a key
    can straddle the cap); a result in any other order proves nothing.
    """
    try:
        listed = run_sql(
            f"select table_name from {lexer.qualify(db)}"
            f".information_schema.table_constraints "
            f"where table_schema = %(schema)s and constraint_type = %(type)s",
            {"schema": schema, "type": constraint_type})
    except Exception as exc:
        return capped, (f"the tables declaring it could not be listed from "
                        f"INFORMATION_SCHEMA.TABLE_CONSTRAINTS: {str(exc)[:160]}")
    tables = sorted({str(_get(r, "table_name")) for r in listed
                     if _get(r, "table_name") is not None})
    names = [str(_get(r, f"{prefix}table_name")) for r in capped]
    trusted = (set(names) - {names[-1]}) if names and names == sorted(names) else set()
    rows = [r for r in capped if str(_get(r, f"{prefix}table_name")) in trusted]
    failed = []
    for table in tables:
        if table in trusted:
            continue
        try:
            rows.extend(run_sql(
                f"show {what} in table {lexer.qualify(db, schema, table)}"))
        except Exception as exc:
            failed.append(f"{table} ({str(exc)[:80]})")
    if failed:
        return rows, (f"{len(failed)} table(s) could not be read one at a "
                      f"time: {', '.join(failed[:5])}"
                      f"{', ...' if len(failed) > 5 else ''}")
    return rows, None


def _read_kind(run_sql, db: str, what: str, constraint_type: str,
               prefix: str, schemas: list[str] | None,
               note: Callable[[str], None]) -> list[dict]:
    """Every key row of one kind in the database, narrowed past the cap."""
    statement = f"show {what} in database"
    try:
        rows = run_sql(f"{statement} {lexer.qualify(db)}")
    except Exception as exc:
        note(f"{db} {statement}: {str(exc)[:200]}")
        return []
    # Exactly the cap is the only suspicious count: a result LONGER than it
    # proves no cap applied (live, SHOW IMPORTED KEYS returned 12,000 rows,
    # and re-reading it per schema only cost time).
    if len(rows) != SHOW_ROW_CAP:
        return rows
    capped = (f"{db} {statement}: {len(rows):,} rows, the SHOW cap, so "
              f"{constraint_type} constraints past it")
    if not schemas:
        note(f"{capped} are NOT recorded (no schema list to narrow the read)")
        return rows
    out: list[dict] = []
    for schema in schemas:
        where = lexer.qualify(db, schema)
        try:
            part = run_sql(f"show {what} in schema {where}")
        except Exception as exc:
            note(f"{capped}: the per-schema re-read of {db}.{schema} "
                 f"failed, so its {constraint_type} constraints are NOT "
                 f"recorded -- {str(exc)[:160]}")
            continue
        if len(part) == SHOW_ROW_CAP:
            part, why = _complete_schema(run_sql, db, schema, what,
                                         constraint_type, prefix, part)
            if why:
                note(f"{db}.{schema} show {what} in schema: at the SHOW cap "
                     f"too, and {why}; {constraint_type} constraints there "
                     f"are INCOMPLETE")
        out.extend(part)
    return out


def build_constraints(run_sql: Callable[..., list[dict]], db: str,
                      notes: list[str] | None = None,
                      schemas: list[str] | None = None
                      ) -> dict[str, list[dict]]:
    """`{source_identifier: [constraint, ...]}` for one database.

    A read that fails is recorded in `notes` and the others still run: a role
    that cannot see one of these views must not cost the estate the other
    two, and "not read" must not read as "none declared". A read at the SHOW
    row cap is re-read per schema in `schemas` (the database's schemas, from
    the caller), and a schema at the cap table by table; what still cannot
    be read is a note, never silence.
    """
    by_table: dict[str, list[dict]] = {}

    def note(text: str) -> None:
        if notes is not None:
            notes.append(text)

    for what, constraint_type in (("primary keys", "PRIMARY KEY"),
                                  ("unique keys", "UNIQUE")):
        rows = _read_kind(run_sql, db, what, constraint_type, "", schemas, note)
        for (ident, _), entry in _keyed(rows, constraint_type).items():
            by_table.setdefault(ident, []).append(entry)

    # IMPORTED KEYS is the CHILD side: the table that holds the foreign key.
    fk_rows = _read_kind(run_sql, db, "imported keys", "FOREIGN KEY", "fk_",
                         schemas, note)
    # Indexed once: rescanning every FK row for every key took 321 s of pure
    # Python at 12,000 foreign keys (live 2026-09-29), and grows with the
    # square of the count.
    by_key: dict[tuple[str, str], list[dict]] = {}
    for r in fk_rows:
        ident = (f'{_get(r, "fk_database_name")}.{_get(r, "fk_schema_name")}'
                 f'.{_get(r, "fk_table_name")}')
        by_key.setdefault((ident, str(_get(r, "fk_name") or "FOREIGN KEY")),
                          []).append(r)
    for (ident, _), entry in _keyed(fk_rows, "FOREIGN KEY", "fk_").items():
        rows = list(by_key.get((ident, entry["name"]), []))
        rows.sort(key=_sequence)
        first = rows[0] if rows else {}
        entry["references"] = ".".join(
            str(_get(first, f"pk_{part}_name") or "")
            for part in ("database", "schema", "table"))
        entry["referenced_columns"] = [str(_get(r, "pk_column_name"))
                                       for r in rows
                                       if _get(r, "pk_column_name") is not None]
        by_table.setdefault(ident, []).append(entry)

    for entries in by_table.values():
        entries.sort(key=lambda e: (e["constraint_type"], e["name"]))
    return by_table
