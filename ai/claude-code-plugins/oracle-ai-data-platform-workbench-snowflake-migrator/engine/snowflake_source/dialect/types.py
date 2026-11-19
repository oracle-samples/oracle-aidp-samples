"""Snowflake type -> Spark/Delta type. Pure functions, zero I/O.

Precision and scale are always passed in from INFORMATION_SCHEMA.COLUMNS and are
never inferred from sampled data: NUMBER is Snowflake's default numeric type, and
getting its scale wrong does not raise -- it silently changes values.

An unmapped type is BLOCKED, never approximated.

Two switches exist because default-deny with no alternative is not a usable
tool. Semi-structured columns (VARIANT/OBJECT/ARRAY) are everywhere in a real
Snowflake estate -- order payloads, event bodies -- and ONE of them blocked the
whole table, with no way to proceed. So:

  * semi_structured="block" (default) -- a typed struct/map/array design is a
    decision to make with the customer, not one to guess
  * semi_structured="string"          -- carry the JSON as text, explicitly and
    loudly, and revisit it later

Geospatial types get their OWN switch: deciding to carry JSON as text is not
the same decision as carrying a geography as text. `geospatial="wkt"` carries
the value as WKT text instead of GeoJSON.

STRUCTURED types are typed, not untyped JSON, so the semi-structured switch
does not hold them. With the full type DESCRIBE TABLE spells (`type_detail`,
read by extract/catalog.py and the in-AIDP discovery), VECTOR(FLOAT|INT, n)
becomes ARRAY<FLOAT|INT>, MAP(K, V) MAP<STRING, v'>, OBJECT(f T, ...)
STRUCT<f: t', ...> and ARRAY(T) ARRAY<t'>. Live 2026-09-29 the AIDP
connector could not open a table holding any of them, while a pushdown of
the column as JSON text parsed with Spark `from_json` gave the typed values
exactly -- target/ddl.py records that read per column. Without the detail a
VECTOR or MAP is blocked (its element type is unknown, not guessed) and an
OBJECT or ARRAY is the semi-structured case it always was.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

__all__ = ["TypeMapping", "map_type", "SEMI_STRUCTURED_MODES",
           "GEOSPATIAL_MODES", "TIMESTAMP_NTZ_MODES", "needs_type_detail",
           "TYPE_DETAIL_BASES", "TypeSyntaxError", "parse_type",
           "structured_spark_type", "time_format", "copy_expressions",
           "source_type_key"]

SEMI_STRUCTURED_MODES = ("block", "string")
# TIMESTAMP_NTZ is preserved by default because bare TIMESTAMP is
# session-timezone-dependent and the wrong choice shifts every timestamp. The
# AIDP catalog API cannot express timestamp_ntz, so a target may require the
# downgrade -- but that is a decision the TRANSLATOR records, with a warning,
# rather than something a transport does silently.
TIMESTAMP_NTZ_MODES = ("preserve", "timestamp")
GEOSPATIAL_MODES = ("block", "string", "wkt")


# Types whose INFORMATION_SCHEMA.COLUMNS row hides what the mapper needs.
# Live 2026-09-29: DATA_TYPE answers `VECTOR`, `MAP`, `OBJECT`, `ARRAY` and
# stops, while DESCRIBE TABLE spells `VECTOR(FLOAT, 4)`,
# `MAP(VARCHAR(16777216), NUMBER(38,0))`, `OBJECT(X NUMBER(38,0), ...)`,
# `ARRAY(NUMBER(38,0))` -- or a bare `OBJECT` / `ARRAY` for the untyped,
# semi-structured kind. The geospatial pair is read too, so its record says
# what the source declared rather than what a default assumed.
TYPE_DETAIL_BASES = ("VECTOR", "MAP", "OBJECT", "ARRAY", "GEOGRAPHY",
                     "GEOMETRY")
# DESCRIBE's `TIME(3)` / `TIMESTAMP_TZ(9)` adds only the fractional
# precision, which INFORMATION_SCHEMA carries as DATETIME_PRECISION. Nearly
# every table of a real estate holds a timestamp, so these are read only
# when that precision is missing -- otherwise the bound is gone.
_TIME_BASES = ("TIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ",
               "TIMESTAMP_TZ")


class TypeSyntaxError(ValueError):
    """A DESCRIBE / GET_DDL type string this parser cannot read."""


@dataclass
class _Node:
    """One parsed Snowflake type: `MAP(VARCHAR(16777216), NUMBER(38,0))`."""
    base: str
    args: list = field(default_factory=list)       # integers
    children: list = field(default_factory=list)   # element / key+value
    fields: list = field(default_factory=list)     # (name, _Node)
    not_null: bool = False

    def render(self) -> str:
        if self.fields:
            inner = ", ".join(f"{_sf_ident(n)} {t.render()}"
                              for n, t in self.fields)
        elif self.children:
            inner = ", ".join([c.render() for c in self.children]
                              + [str(a) for a in self.args])
        else:
            inner = ",".join(str(a) for a in self.args)
        return f"{self.base}({inner})" if inner else self.base


def _sf_ident(name: str) -> str:
    return name if re.match(r"^[A-Z_][A-Z0-9_$]*$", name) \
        else '"' + name.replace('"', '""') + '"'


_TOKEN = re.compile(
    r"""\s*(?:(?P<q>"(?:[^"]|"")*")|(?P<s>'(?:[^'\\]|\\.|'')*')"""
    r"""|(?P<n>\d+)|(?P<w>[A-Za-z_][A-Za-z0-9_$]*)|(?P<p>[(),]))""")


def _tokens(text: str) -> list[tuple[str, str]]:
    text = str(text or "").strip()
    out, pos = [], 0
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise TypeSyntaxError(f"cannot read {text[pos:pos + 24]!r}")
        out.append((m.lastgroup, m.group(m.lastgroup)))
        pos = m.end()
    return out


def parse_type(text: str) -> _Node:
    """A DESCRIBE TABLE `type` string, parsed. Raises TypeSyntaxError."""
    toks = _tokens(text)
    if not toks:
        raise TypeSyntaxError("empty type")
    node, i = _parse(toks, 0)
    if i != len(toks):
        raise TypeSyntaxError(f"unexpected {toks[i][1]!r} after the type")
    return node


def _at(toks, i) -> tuple[str | None, str | None]:
    return toks[i] if i < len(toks) else (None, None)


def _parse(toks, i) -> tuple[_Node, int]:
    kind, word = _at(toks, i)
    if kind != "w":
        raise TypeSyntaxError(f"expected a type name, found {word!r}")
    node = _Node(word.upper())
    i += 1
    if node.base == "DOUBLE" and (_at(toks, i)[1] or "").upper() == "PRECISION":
        i += 1
    if _at(toks, i) == ("p", "("):
        i += 1
        while True:
            kind, value = _at(toks, i)
            if node.base == "OBJECT":
                if kind not in ("w", "q"):
                    raise TypeSyntaxError(f"expected a field name, found {value!r}")
                name = value[1:-1].replace('""', '"') if kind == "q" else value
                child, i = _parse(toks, i + 1)
                node.fields.append((name, child))
            elif kind == "n":
                node.args.append(int(value))
                i += 1
            elif node.base in ("ARRAY", "MAP", "VECTOR"):
                child, i = _parse(toks, i)
                node.children.append(child)
            else:
                raise TypeSyntaxError(f"unexpected {value!r} in {node.base}(...)")
            kind, value = _at(toks, i)
            if (kind, value) == ("p", ","):
                i += 1
                continue
            if (kind, value) == ("p", ")"):
                i += 1
                break
            raise TypeSyntaxError(f"expected , or ) in {node.base}(...)")
    while _at(toks, i)[0] == "w":
        word = toks[i][1].upper()
        if word == "NOT" and (_at(toks, i + 1)[1] or "").upper() == "NULL":
            node.not_null, i = True, i + 2
        elif word == "NULL":
            i += 1
        elif word == "COLLATE" and _at(toks, i + 1)[0] == "s":
            i += 2
        else:
            break
    return node, i


_FLOATS = {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL"}
_TEXTS = {"TEXT", "VARCHAR", "CHAR", "CHARACTER", "STRING", "NCHAR",
          "NVARCHAR", "NVARCHAR2", "CHAR VARYING"}


class _NotCarried(Exception):
    """A type inside a structured type that a typed Spark column cannot hold."""


def _spark_field(name: str) -> str:
    # Bare when Spark reads it bare, so the planned type reduces to what
    # DESCRIBE renders (`struct<X:decimal(38,0)>`) under 01's comparison.
    return name if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name) \
        else "`" + name.replace("`", "``") + "`"


def _structured(node: _Node, caveats: list[str]) -> str:
    """The Spark type of one node INSIDE a structured type."""
    if node.not_null and "NOT NULL" not in caveats:
        caveats.append("NOT NULL")
    base = node.base
    if base in _NUMERIC:
        precision = node.args[0] if node.args else 38
        scale = node.args[1] if len(node.args) > 1 else 0
        return f"DECIMAL({precision},{scale})"
    if base in _FLOATS:
        return "DOUBLE"
    if base in _TEXTS:
        return "STRING"
    if base == "BOOLEAN":
        return "BOOLEAN"
    if base == "ARRAY" and len(node.children) == 1:
        return f"ARRAY<{_structured(node.children[0], caveats)}>"
    if base == "OBJECT" and node.fields:
        return "STRUCT<" + ", ".join(
            f"{_spark_field(n)}: {_structured(t, caveats)}"
            for n, t in node.fields) + ">"
    if base == "MAP" and len(node.children) == 2:
        key, value = node.children
        if key.base in _NUMERIC:
            caveats.append(f"key {key.render()}")
        elif key.base not in _TEXTS:
            raise _NotCarried(key.render())
        return f"MAP<STRING, {_structured(value, caveats)}>"
    raise _NotCarried(node.render())


def structured_spark_type(node: _Node) -> tuple[str | None, list[str], str | None]:
    """(spark type, caveats, what cannot be carried) for a structured node.

    `caveats` are the narrowings the typed mapping makes: `NOT NULL` inside
    the type, a `key NUMBER(..)` carried as text. The third value names the
    inner type no typed Spark column holds -- the column then falls back to
    JSON text, or blocks.
    """
    caveats: list[str] = []
    if node.base == "VECTOR":
        element = node.children[0].base if node.children else ""
        if element in ("FLOAT", "FLOAT4", "REAL"):
            return "ARRAY<FLOAT>", caveats, None
        if element in ("INT", "INTEGER"):
            return "ARRAY<INT>", caveats, None
        return None, caveats, node.render()
    try:
        return _structured(node, caveats), caveats, None
    except _NotCarried as exc:
        return None, caveats, str(exc)


def time_format(precision) -> str:
    """The TO_VARCHAR format that renders a TIME with every digit it holds.

    The connector's own read dropped TIME(3)'s fraction ("12:34:56"); this
    format gave "12:34:56.789" live. Unknown precision reads all nine.
    """
    try:
        digits = int(precision)
    except (TypeError, ValueError):
        digits = 9
    digits = max(0, min(digits, 9))
    return "HH24:MI:SS" + (f".FF{digits}" if digits else "")


# The TO_VARCHAR formats the exact reads use (live 2026-09-29: the connector
# dropped TIMESTAMP_NTZ(9)'s last digits and TIMESTAMP_TZ's offset; these
# gave "2026-09-25 01:02:03.123456789" and the instant with its offset).
# The zoned form is ISO-8601 with the offset attached, which Spark's CAST
# to TIMESTAMP parses as an instant.
_NTZ_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9"
_TZ_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM'
_ZONED = ("TIMESTAMP_TZ", "TIMESTAMP_LTZ", "TIMESTAMP")
_UNTYPED_JSON = ("VARIANT", "OBJECT", "ARRAY", "MAP")


def _sf_quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _spark_quote(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _spark_literal(value: str) -> str:
    # Spark escapes with a backslash, not by doubling (see ddl.quote_spark_string).
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def copy_expressions(data_type, target_type: str, *, name: str,
                     type_detail: str | None = None,
                     datetime_precision=None,
                     geospatial: str | None = None) -> tuple[str, str]:
    """(read_expr, convert_expr) for one column: the per-column copy spec.

    `read_expr` is Snowflake SQL over the double-quoted source column, never
    aliased; `convert_expr` is Spark SQL over a column named exactly the
    source name, backtick-quoted, producing `target_type`. Every read is a
    plain expression inside a SELECT -- a cast, TO_VARCHAR, TO_JSON,
    ST_ASWKT -- so the statement the copy stage wraps them in stays a read.

    The live facts behind each (2026-09-29): NUMBER and FLOAT lose digits
    through the connector and are exact as text; TIME / TIMESTAMP fractions
    and offsets are dropped unless formatted; VECTOR / MAP / structured
    OBJECT cannot be opened at all and parse exactly from JSON text.
    """
    col, out = _sf_quote(name), _spark_quote(name)
    key = _base(data_type)
    target = str(target_type or "")
    upper = target.upper()
    if upper.startswith(("ARRAY<", "MAP<", "STRUCT<")):
        read = f"{col}::ARRAY::VARCHAR" if key == "VECTOR" \
            else f"{col}::VARIANT::VARCHAR"
        # A STRUCT's field names are matched to the JSON keys exactly, so
        # they are never case-folded; a keyword-only type is written the
        # way the contract spells it.
        schema = target if "STRUCT<" in upper else target.lower()
        return read, f"from_json({out}, {_spark_literal(schema)})"
    if key in _NUMERIC:
        return f"{col}::VARCHAR", f"CAST({out} AS {target})"
    if key in _FLOATS or key == "DOUBLE PRECISION":
        return f"TO_VARCHAR({col}, 'TME')", f"CAST({out} AS {target})"
    if key == "TIME":
        if datetime_precision in (None, "") and type_detail:
            try:
                datetime_precision = _detail_precision(parse_type(type_detail))
            except TypeSyntaxError:
                pass
        return f"TO_VARCHAR({col}, '{time_format(datetime_precision)}')", out
    if key == "TIMESTAMP_NTZ":
        return f"TO_VARCHAR({col}, '{_NTZ_FORMAT}')", f"CAST({out} AS {target})"
    if key in _ZONED:
        return f"TO_VARCHAR({col}, '{_TZ_FORMAT}')", f"CAST({out} AS {target})"
    if key in _GEOSPATIAL:
        if geospatial == "wkt":
            return f"ST_ASWKT({col})", out
        return f"ST_ASGEOJSON({col})::VARCHAR", out
    if key in _UNTYPED_JSON and upper == "STRING":
        return f"TO_JSON({col}::VARIANT)", out
    return col, out


def source_type_key(data_type, precision=None, scale=None) -> str:
    """One column's source type as the copy's pre-flight compares it.

    NUMBER(p,s) is `decimal(p,s)`; anything else is INFORMATION_SCHEMA's own
    type name lower-cased. The plan records this per column (`source_type`)
    and the copy stage reads the live source the same way
    (`SnowflakeSource.live_columns`), so a column whose type changed after
    the plan was approved is caught before its old conversion runs. A
    parity test holds the two spellings together.
    """
    kind = str(data_type or "").strip()
    if kind.upper() in ("NUMBER", "DECIMAL", "NUMERIC") and \
            precision is not None:
        kind = f"decimal({int(precision)},{int(scale or 0)})"
    return kind.lower()


def _detail_precision(node: _Node | None):
    return node.args[0] if node is not None and node.args else None


def _base(data_type) -> str:
    """`VECTOR(FLOAT, 4)` -> `VECTOR`; `timestamp_tz` -> `TIMESTAMP_TZ`."""
    return str(data_type or "").strip().upper().split("(")[0].strip()


def needs_type_detail(data_type, datetime_precision=None) -> bool:
    """Whether this column's full type must be read from DESCRIBE TABLE
    (or, inside AIDP, GET_DDL) before the mapper can decide it.

    The rule that keeps the extra read bounded: a table costs a DESCRIBE
    only when one of its columns answers True here.
    """
    base = _base(data_type)
    if base in TYPE_DETAIL_BASES:
        return True
    return base in _TIME_BASES and datetime_precision in (None, "")


@dataclass(frozen=True)
class TypeMapping:
    spark_type: str | None
    blocked: bool = False
    reason: str | None = None
    warning: str | None = None
    # Informational only, and deliberately NOT a warning: notes do not raise an
    # object's risk level. A warning on every integer column would mark every
    # table MEDIUM and drown the warnings that matter.
    note: str | None = None


_DIRECT = {
    "TEXT": "STRING", "VARCHAR": "STRING", "CHAR": "STRING", "STRING": "STRING",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "BINARY": "BINARY", "VARBINARY": "BINARY",
    "FLOAT": "DOUBLE", "FLOAT4": "DOUBLE", "FLOAT8": "DOUBLE",
    "DOUBLE": "DOUBLE", "DOUBLE PRECISION": "DOUBLE", "REAL": "DOUBLE",
}

_NUMERIC = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
            "SMALLINT", "TINYINT", "BYTEINT"}

_SEMI_STRUCTURED = {
    "VARIANT": "semi-structured; needs an explicit struct/map/array target design",
    "OBJECT": "semi-structured; needs an explicit struct/map target design",
    "ARRAY": "semi-structured; needs an explicit array target design",
}

_GEOSPATIAL = {
    "GEOGRAPHY": "no Spark/Delta target type",
    "GEOMETRY": "no Spark/Delta target type",
}

_SEMI_STRUCTURED_AS_STRING = (
    "carried as STRING: the JSON text is preserved verbatim, but this is NOT a "
    "typed mapping. Nothing on the target can address a field inside it, and "
    "any query using Snowflake path syntax will not work until a struct/map "
    "design is agreed. Deliberately deferred, not solved.")

_GEOSPATIAL_AS_STRING = (
    "carried as STRING: the value arrives as GeoJSON text (ST_ASGEOJSON; the "
    "form the connector returned live) with no spatial type, index or "
    "predicate support on the target. Spatial queries will not work until a "
    "target design is agreed.")

_GEOSPATIAL_AS_WKT = (
    "carried as STRING in WKT (ST_ASWKT, live-verified: "
    "`POINT(-122.35 37.55)`), with no spatial type, index or predicate "
    "support on the target. Spatial queries will not work until a target "
    "design is agreed.")

_GEOMETRY_SRID = (
    " WKT carries no SRID, so a GEOMETRY's spatial reference is not in the "
    "value (ST_ASEWKT would carry it; GEOGRAPHY is always WGS 84)")

# Snowflake integer aliases are all NUMBER(38,0).
_INTEGER_ALIASES = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}


def _collation_warning(collation: str) -> str:
    """A collated column keeps its text and loses its comparison rules.

    Delta compares STRING bytewise. `COLLATE 'en-ci'` made 'abc' = 'ABC' true
    in Snowflake; on the target it is false, and every join, GROUP BY,
    DISTINCT, ORDER BY and uniqueness check on the column moves with it --
    while row-count reconciliation still passes. The value is not damaged,
    so this warns rather than blocks.
    """
    return (f"collation '{collation}' does not travel: Delta compares STRING "
            f"bytewise, so comparisons, sorting and uniqueness on this column "
            f"become binary (case- and accent-sensitive) on the target -- "
            f"equality, joins, GROUP BY / DISTINCT and ORDER BY can change "
            f"their results")


# Spark TIMESTAMP and TIMESTAMP_NTZ hold microseconds.
_SPARK_TIMESTAMP_PRECISION = 6


def _precision_warning(key: str, datetime_precision) -> str | None:
    """Digits below the microsecond are dropped at the read.

    Precision 9 is Snowflake's DEFAULT, so this fires on most timestamp
    columns of a real estate. It is a warning, not a note: values that
    differed only in their last three digits compare equal on the target,
    and the counts+sums verification sums DECIMAL columns only, so nothing
    downstream notices.
    """
    try:
        precision = int(datetime_precision)
    except (TypeError, ValueError):
        return None
    if precision <= _SPARK_TIMESTAMP_PRECISION:
        return None
    return (f"{key} precision {precision}: sub-microsecond digits are "
            f"truncated (Spark stores microseconds), so values that differ "
            f"only below the microsecond arrive equal")


def _join_warnings(*warnings: str | None) -> str | None:
    """TypeMapping carries one warning; two facts about a column are both
    kept rather than the second overwriting the first."""
    kept = [w for w in warnings if w]
    return "; ".join(kept) if kept else None


def _semi_structured(key: str, mode: str, extra: str | None = None
                     ) -> TypeMapping:
    """The untyped-JSON verdict, with an optional sentence about why a
    typed mapping was not reached."""
    reason = _SEMI_STRUCTURED.get(key, _SEMI_STRUCTURED["VARIANT"])
    if mode == "block":
        return TypeMapping(None, True, f"{key}: {reason}"
                           + (f"; {extra}" if extra else ""))
    return TypeMapping("STRING", warning=f"{key} {_SEMI_STRUCTURED_AS_STRING}"
                       + (f" {extra}" if extra else ""))


def _typed(detail: _Node, spark: str, caveats: list[str]) -> TypeMapping:
    """A structured type mapped to a typed Spark column."""
    rendered = detail.render()
    warnings = []
    if detail.base == "VECTOR" and detail.args:
        warnings.append(
            f"{rendered} -> {spark}: the dimension {detail.args[0]} is not "
            f"enforced by Delta (an array of any length can be inserted "
            f"after cutover), and Snowflake's VECTOR functions have no "
            f"Spark equivalent")
    for caveat in caveats:
        if caveat == "NOT NULL":
            warnings.append(f"{rendered}: a NOT NULL inside the type is not "
                            f"carried; the Spark type accepts nulls there")
        elif caveat.startswith("key "):
            warnings.append(
                f"{rendered}: the MAP {caveat} is carried as STRING -- JSON "
                f"object keys are text and Spark's from_json reads map keys "
                f"as strings -- so each key arrives as its exact decimal "
                f"text, and a lookup must use the text form")
    note = (f"{rendered} -> {spark}: typed. It is read from Snowflake as JSON "
            f"text and parsed with from_json on AIDP, because the connector "
            f"cannot open a table holding this type (live-verified); Snowflake "
            f"path syntax (col:field) becomes Spark's col.field / col['key']")
    return TypeMapping(spark, warning=_join_warnings(*warnings), note=note)


def _unread_sentence(unread: str | None) -> str:
    return (f"the type detail was UNREAD ({unread})" if unread else
            "the type detail was not read by this inventory")


def map_type(data_type: str, *, precision: int | None = None,
             scale: int | None = None, char_length: int | None = None,
             semi_structured: str = "block",
             geospatial: str = "block",
             timestamp_ntz: str = "preserve",
             collation: str | None = None,
             datetime_precision: int | None = None,
             type_detail: str | None = None,
             type_detail_unread: str | None = None) -> TypeMapping:
    """One column's target type and what the mapping costs.

    `collation` is INFORMATION_SCHEMA.COLUMNS.COLLATION_NAME. It only means
    anything on a text column; NULL or empty is the default bytewise
    comparison, which Delta shares.

    `datetime_precision` is DATETIME_PRECISION, read for the timestamp
    types and for the TIME read format.

    `type_detail` is DESCRIBE TABLE's full type (`VECTOR(FLOAT, 4)`), and
    `type_detail_unread` why an extractor that tried could not read it.
    Neither set means the inventory never tried, and the verdict is the one
    it always was.
    """
    if semi_structured not in SEMI_STRUCTURED_MODES:
        raise ValueError(
            f"unknown semi_structured mode {semi_structured!r}; expected one of "
            f"{list(SEMI_STRUCTURED_MODES)}")
    if timestamp_ntz not in TIMESTAMP_NTZ_MODES:
        raise ValueError(
            f"unknown timestamp_ntz mode {timestamp_ntz!r}; expected one of "
            f"{list(TIMESTAMP_NTZ_MODES)}")
    if geospatial not in GEOSPATIAL_MODES:
        raise ValueError(
            f"unknown geospatial mode {geospatial!r}; expected one of "
            f"{list(GEOSPATIAL_MODES)}")
    if data_type is None:
        return TypeMapping(None, True, "missing data_type")
    key = " ".join(data_type.strip().upper().split())

    detail: _Node | None = None
    detail_error = None
    if type_detail:
        try:
            detail = parse_type(type_detail)
        except TypeSyntaxError as exc:
            detail_error = f"type detail {type_detail!r} could not be read: {exc}"
    unread = type_detail_unread or detail_error

    if key in ("VECTOR", "MAP"):
        if detail is None or detail.base != key or not detail.children:
            return TypeMapping(
                None, True,
                f"{key}: its element type{'s are' if key == 'MAP' else ' is'} "
                f"not in INFORMATION_SCHEMA, and {_unread_sentence(unread)}. "
                f"Refusing to guess it: re-run assess (DESCRIBE TABLE) or "
                f"the in-AIDP discovery (GET_DDL) so the full type is read")
        spark, caveats, missing = structured_spark_type(detail)
        if spark:
            return _typed(detail, spark, caveats)
        if key == "VECTOR":
            return TypeMapping(None, True,
                               f"{detail.render()}: no Spark element type")
        return _semi_structured(
            key, semi_structured,
            f"{detail.render()} holds {missing}, which a typed Spark MAP "
            f"does not carry, so it is JSON text")

    if key in ("OBJECT", "ARRAY") and detail is not None \
            and detail.base == key and (detail.fields or detail.children):
        spark, caveats, missing = structured_spark_type(detail)
        if spark:
            return _typed(detail, spark, caveats)
        return _semi_structured(
            key, semi_structured,
            f"The structured {detail.render()} holds {missing}, which a "
            f"typed Spark column does not carry here, so it is JSON text.")

    if key in _SEMI_STRUCTURED:
        extra = None
        if key != "VARIANT" and unread and detail is None:
            extra = (f"Whether it is a STRUCTURED {key} is unknown: "
                     f"{_unread_sentence(unread)}, so any typed shape it has "
                     f"is carried as JSON text too.")
        return _semi_structured(key, semi_structured, extra)

    if key in _GEOSPATIAL:
        if geospatial == "block":
            return TypeMapping(None, True, f"{key}: {_GEOSPATIAL[key]}")
        if geospatial == "wkt":
            return TypeMapping(
                "STRING", warning=f"{key} {_GEOSPATIAL_AS_WKT}"
                + (_GEOMETRY_SRID if key == "GEOMETRY" else ""))
        return TypeMapping("STRING", warning=f"{key} {_GEOSPATIAL_AS_STRING}")

    if datetime_precision in (None, "") and detail is not None:
        datetime_precision = _detail_precision(detail)

    if key in _NUMERIC:
        if precision is None:
            return TypeMapping(
                None, True,
                f"{key} with no numeric_precision from INFORMATION_SCHEMA; "
                "refusing to guess precision")
        resolved = f"DECIMAL({precision},{scale if scale is not None else 0})"
        note = None
        if key in _INTEGER_ALIASES or (key in ("NUMBER", "DECIMAL", "NUMERIC")
                                       and not scale):
            note = (f"{key} -> {resolved}. Faithful: every Snowflake integer is "
                    f"NUMBER(38,0). Spark would normally use BIGINT here, so "
                    f"expect DECIMAL in the target schema and in any downstream "
                    f"cast. Fidelity was chosen over familiarity.")
        return TypeMapping(resolved, note=note)

    if key == "TIMESTAMP_NTZ":
        truncated = _precision_warning(key, datetime_precision)
        if timestamp_ntz == "preserve":
            return TypeMapping("TIMESTAMP_NTZ", warning=truncated)
        return TypeMapping(
            "TIMESTAMP",
            warning=_join_warnings(
                "TIMESTAMP_NTZ -> TIMESTAMP: the AIDP catalog cannot "
                "express timestamp_ntz, so the timezone-naive type is "
                "downgraded. TIMEZONE SEMANTICS DIFFER -- Spark TIMESTAMP "
                "is session-timezone-dependent, so the same value can read "
                "back differently depending on the session timezone",
                truncated))
    if key in ("TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"):
        return TypeMapping(
            "TIMESTAMP",
            warning=_join_warnings(
                f"{key} -> Spark TIMESTAMP: timezone semantics differ; "
                "Spark TIMESTAMP is session-timezone-dependent",
                # Live: the connector dropped the offset outright. The read
                # now carries it (TZH:TZM), so the INSTANT is exact; Spark
                # TIMESTAMP still has nowhere to keep the offset itself.
                "the instant is exact, but the source's UTC offset "
                "(e.g. +05:30) is not stored -- Spark TIMESTAMP has none"
                if key == "TIMESTAMP_TZ" else None,
                _precision_warning(key, datetime_precision)))

    if key == "TIME":
        # Spark has no TIME type. STRING preserves the value but changes
        # ordering and comparison semantics, so it must not be silent.
        return TypeMapping(
            "STRING",
            warning=f"TIME -> STRING: Spark has no TIME type. It is read as "
                    f"TO_VARCHAR(.., '{time_format(datetime_precision)}'), so "
                    f"every fractional digit arrives (the connector's own "
                    f"read dropped them, live-verified). Ordering and "
                    f"comparison become string operations -- right only "
                    f"because every value has the same fixed width -- and "
                    f"time arithmetic does not work on the target")

    if key in _DIRECT:
        warning = None
        if _DIRECT[key] == "STRING" and char_length is not None:
            warning = (f"declared length {char_length} is not enforced by Delta; "
                       "recorded only")
        if _DIRECT[key] == "STRING" and collation and str(collation).strip():
            warning = _join_warnings(
                warning, _collation_warning(str(collation).strip()))
        return TypeMapping(_DIRECT[key], warning=warning)

    return TypeMapping(None, True, f"unmapped Snowflake type: {key}")
