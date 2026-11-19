"""Target DDL from the export's own declared types.

Why this exists. A generated notebook MERGEs into its target and emits no
DDL, so on a fresh workspace there is nothing to merge into. The runtime
guard creates the table empty from the batch schema, which makes a first
run complete -- but the column types then come from the DataFrame, not from
the Informatica target definition. ``NUMBER(10,0)`` arrives as ``BIGINT``,
and a ``SUM`` over ``NUMBER(12,2)`` arrives as ``DECIMAL(22,2)``. That is a
silent fidelity loss: nothing fails, and the target's declared precision is
simply gone.

The export carries ``DATATYPE``, ``PRECISION`` and ``SCALE`` for every
target column, so the faithful type is *derivable* rather than guessed.
This module emits one ``CREATE TABLE IF NOT EXISTS ... USING delta`` per
target from those declarations.

What it deliberately does not do: run. The DDL is written next to the
notebooks for a human to apply, because creating tables in a catalog is not
something a migration tool should do as a side effect of converting code.

Aggregate widening is reported, not resolved. Where a target column is fed
by a COUNT, SUM or AVG, the value Spark computes will not have the target's
declared type -- COUNT produces ``BIGINT``, and SUM/AVG widen a decimal.
Applying this DDL and then MERGEing can therefore fail on a type mismatch,
which is a real conflict between two defensible positions: the export's
declared type, and the type the arithmetic produces. Rather than pick
silently, the generated file names the affected columns in a comment so the
person applying it decides. See ``references/aidp-runtime-constraints.md``.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# Base relational type -> Spark SQL DDL type. Precision and scale are
# applied separately for the decimal family.
#
# This intentionally mirrors ``reconciler.comparators._TYPE_MAP``, which
# maps the same base types to Spark type *class names* for schema
# comparison. Two tables that must agree are a drift risk, so
# ``tests/test_ddl_generator.py`` asserts every base type known to the
# comparator is known here too.
_DDL_TYPE: dict[str, str] = {
    # Oracle
    "NUMBER": "DECIMAL", "FLOAT": "DOUBLE", "BINARY_FLOAT": "FLOAT",
    "BINARY_DOUBLE": "DOUBLE", "VARCHAR2": "STRING", "NVARCHAR2": "STRING",
    "CHAR": "STRING", "NCHAR": "STRING", "CLOB": "STRING", "NCLOB": "STRING",
    "DATE": "TIMESTAMP", "TIMESTAMP": "TIMESTAMP", "BLOB": "BINARY",
    "RAW": "BINARY", "LONG": "STRING", "LONG RAW": "BINARY",
    "XMLTYPE": "STRING",
    # SQL Server / generic
    "INT": "INT", "INTEGER": "INT", "BIGINT": "BIGINT", "SMALLINT": "SMALLINT",
    "TINYINT": "SMALLINT", "BIT": "BOOLEAN", "DECIMAL": "DECIMAL",
    "NUMERIC": "DECIMAL", "MONEY": "DECIMAL", "SMALLMONEY": "DECIMAL",
    "REAL": "FLOAT", "VARCHAR": "STRING", "NVARCHAR": "STRING",
    "TEXT": "STRING", "DATETIME": "TIMESTAMP", "DATETIME2": "TIMESTAMP",
    "SMALLDATETIME": "TIMESTAMP", "TIME": "STRING", "UNIQUEIDENTIFIER": "STRING",
    "IMAGE": "BINARY", "VARBINARY": "BINARY",
    # Postgres / MySQL / further SQL Server spellings. Added because the
    # drift guard in tests/test_ddl_generator.py found the comparator knew
    # them and this table did not, which would have silently made each of
    # these columns STRING.
    "BOOL": "BOOLEAN", "BOOLEAN": "BOOLEAN", "BYTEA": "BINARY",
    "CHARACTER VARYING": "STRING", "DOUBLE PRECISION": "DOUBLE",
    "SERIAL": "INT", "BIGSERIAL": "BIGINT", "MEDIUMINT": "INT",
    "JSON": "STRING", "JSONB": "STRING", "UUID": "STRING",
    "LONGTEXT": "STRING", "MEDIUMTEXT": "STRING", "TINYTEXT": "STRING",
    "NTEXT": "STRING", "DATETIMEOFFSET": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP", "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    # Informatica's own transformation-port spellings
    "STRING": "STRING", "NSTRING": "STRING", "TEXT ": "STRING",
    "INTEGER ": "INT", "DOUBLE": "DOUBLE", "SMALL INT": "SMALLINT",
    # The spelling PowerCenter itself uses for a 16-bit port (the
    # converters already recognise it); without it the column became
    # STRING and escaped the write's declared-range check.
    "SMALL INTEGER": "SMALLINT",
    "BIGINT ": "BIGINT", "DECIMAL ": "DECIMAL", "BINARY": "BINARY",
    "DATE/TIME": "TIMESTAMP",
}

# Delta's ceiling. A declared precision above this cannot be represented.
_MAX_PRECISION = 38

_AGGREGATE_CALL = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|MEDIAN|STDDEV|VARIANCE|PERCENTILE)\s*\(",
    re.IGNORECASE,
)
# MIN and MAX return their argument's type, so they do not widen. The rest do.
_WIDENING = {"COUNT", "SUM", "AVG", "MEDIAN", "STDDEV", "VARIANCE", "PERCENTILE"}


@dataclass
class TargetDDL:
    """One target's CREATE TABLE, plus what the caller should know about it."""

    target_name: str
    table: str                       # as the notebook addresses it
    sql: str
    columns: int = 0
    # Columns whose declared type will not match what the notebook computes.
    widened: list = field(default_factory=list)
    # Declarations we could not translate faithfully.
    notes: list = field(default_factory=list)

    @property
    def has_warnings(self) -> bool:
        return bool(self.widened or self.notes)


def _ddl_type(datatype: str, precision: int, scale: int) -> tuple[str, Optional[str]]:
    """(spark_ddl_type, note_if_not_faithful).

    An unknown declaration becomes STRING with a note rather than a guess:
    a wrong numeric type silently truncates, while STRING is visibly wrong
    and cannot lose digits.
    """
    raw = (datatype or "").strip()
    if not raw:
        return "STRING", "no DATATYPE in the export; defaulted to STRING"
    base = raw.upper().split("(")[0].strip()
    mapped = _DDL_TYPE.get(base)
    if mapped is None:
        return "STRING", f"unknown datatype {raw!r}; defaulted to STRING"
    if mapped != "DECIMAL":
        return mapped, None

    p = int(precision or 0)
    s = int(scale or 0)
    note = None
    if p <= 0:
        # PR #6 established decimal(38, scale) as the fallback when the
        # export carries no precision.
        p, note = _MAX_PRECISION, (
            f"no PRECISION in the export; used DECIMAL({_MAX_PRECISION},{s})"
        )
    if p > _MAX_PRECISION:
        note = (f"declared PRECISION {p} exceeds Delta's maximum "
                f"{_MAX_PRECISION}; clamped, which can truncate")
        p = _MAX_PRECISION
    if s > p:
        note = f"declared SCALE {s} exceeds precision {p}; scale clamped"
        s = p
    return f"DECIMAL({p},{s})", note


def _col_name(f) -> str:
    """A target column's name, whichever model shape carries it.

    A target's fields are ``FieldMapping`` (``target_field``); a
    transformation's are ``TransformationField`` (``name``). Both reach this
    module, so neither spelling is assumed.
    """
    return (getattr(f, "target_field", "") or getattr(f, "name", "") or "").strip()


def _widening_aggregates(mapping) -> dict[str, str]:
    """Target column -> the aggregate function that feeds it.

    Read off the Aggregator's output ports: their expressions are the
    aggregate calls, and the port name is the column the target receives.
    MIN/MAX are excluded because they return their argument's type.
    """
    out: dict[str, str] = {}
    for tx in getattr(mapping, "transformations", []) or []:
        if "AGGREGAT" not in str(getattr(tx, "type", "")).upper():
            continue
        for f in getattr(tx, "fields", []) or []:
            m = _AGGREGATE_CALL.search(getattr(f, "expression", "") or "")
            if m and m.group(1).upper() in _WIDENING:
                out[_col_name(f).upper()] = m.group(1).upper()
    return out


def _sequence_fed_columns(mapping, target) -> dict[str, str]:
    """Target column -> the Sequence Generator that feeds it.

    A Sequence Generator's NEXTVAL arrives as BIGINT whatever the target
    declares: ``infa_compat.sequence(...).assign()`` ends in
    ``value.cast("long")`` (``engine/infa_compat/sequence.py``). So a key
    column declared ``NUMBER(10,0)`` is handed a ``bigint``, and Delta
    refuses the MERGE with ``DELTA_FAILED_TO_MERGE_FIELDS`` -- found on a
    live AIDP cluster in PR #11, on an SCD2 surrogate key.

    The column is found by following CONNECTORs forward from the NEXTVAL
    port rather than by matching port names, because a port renamed on the
    way to the target would silently escape a name match -- the same class
    of bug as the ``_M``/``_D`` suffix guessing the Joiner used to do.
    """
    conns = list(getattr(mapping, "connectors", []) or [])
    if not conns:
        return {}
    target_names = {
        n.upper() for n in (
            getattr(target, "name", ""),
            getattr(target, "table_name", ""),
        ) if n
    }

    out: dict[str, str] = {}
    for tx in getattr(mapping, "transformations", []) or []:
        if "SEQUENCE" not in str(getattr(tx, "type", "")).upper():
            continue
        # Every port of this Sequence Generator that carries a value
        # downstream. NEXTVAL is the one that matters; CURRVAL is the same
        # long.
        starts = [(tx.name, _col_name(f)) for f in (getattr(tx, "fields", []) or [])
                  if _col_name(f)]
        if not starts:
            starts = [(tx.name, "NEXTVAL")]
        seen = set()
        queue = list(starts)
        while queue:
            inst, fld = queue.pop()
            if (inst, fld) in seen:
                continue
            seen.add((inst, fld))
            for c in conns:
                if (c.from_instance or "").upper() != (inst or "").upper():
                    continue
                if (c.from_field or "").upper() != (fld or "").upper():
                    continue
                if (c.to_instance or "").upper() in target_names:
                    out[(c.to_field or "").upper()] = tx.name
                else:
                    queue.append((c.to_instance, c.to_field))
    return out


def target_ddl(mapping, target, catalog_qualified: bool = True) -> TargetDDL:
    """Build one target's CREATE TABLE from its declared column types."""
    parts = [p for p in (getattr(target, "db_name", ""),
                         getattr(target, "owner", ""),
                         getattr(target, "table_name", "") or target.name) if p]
    if not catalog_qualified and len(parts) == 3:
        parts = parts[1:]
    table = ".".join(parts) if parts else target.name

    cols, notes, widened = [], [], []
    widening = _widening_aggregates(mapping)
    for col, seq in _sequence_fed_columns(mapping, target).items():
        # A sequence wins over an aggregate label if both somehow apply:
        # the BIGINT is the harder constraint.
        widening[col] = f"SEQUENCE {seq}"
    for f in getattr(target, "fields", []) or []:
        name = _col_name(f)
        if not name:
            notes.append("a target column has no name in the export; skipped")
            continue
        ddl_type, note = _ddl_type(getattr(f, "datatype", ""),
                                   getattr(f, "precision", 0),
                                   getattr(f, "scale", 0))
        # NOT NULL only where the export says so. Defaulting to NOT NULL
        # would reject rows the source happily produced.
        null = "" if getattr(f, "nullable", True) else " NOT NULL"
        cols.append(f"  {name} {ddl_type}{null}")
        if note:
            notes.append(f"{name}: {note}")
        agg = widening.get(name.upper())
        if agg:
            widened.append(f"{name} ({agg})")

    if not cols:
        notes.append("target carries no column definitions in the export")

    header = [f"-- {table}",
              f"-- Generated from the Informatica target definition "
              f"'{target.name}' in mapping '{getattr(mapping, 'name', '?')}'.",
              "-- Types come from the export's DATATYPE/PRECISION/SCALE, not "
              "from the DataFrame."]
    if widened:
        header += [
            "--",
            "-- REVIEW: the value the notebook computes for these columns "
            "will NOT have",
            "-- the declared type below. COUNT gives BIGINT; SUM and AVG "
            "widen a decimal;",
            "-- a Sequence Generator's NEXTVAL is always BIGINT. Applying "
            "this DDL as-is",
            "-- can make the MERGE fail on a type mismatch "
            "(DELTA_FAILED_TO_MERGE_FIELDS,",
            "-- seen live on an SCD2 surrogate key). Decide per column "
            "whether the declared",
            "-- type or the computed type is the one you want:",
        ] + [f"--   {w}" for w in widened]
    if notes:
        header += ["--"] + [f"-- NOTE: {n}" for n in notes]

    body = (f"CREATE TABLE IF NOT EXISTS {table} (\n"
            + ",\n".join(cols) + "\n) USING delta;\n") if cols else ""
    return TargetDDL(target_name=target.name, table=table,
                     sql="\n".join(header) + "\n" + body,
                     columns=len(cols), widened=widened, notes=notes)


def mapping_ddl(mapping, catalog_qualified: bool = True) -> list:
    """Every target's DDL for one mapping."""
    # A flat-file target is a file, not a table: no CREATE TABLE.
    return [target_ddl(mapping, t, catalog_qualified)
            for t in (getattr(mapping, "targets", []) or [])
            if not getattr(t, "flat_file", None)]
