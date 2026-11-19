"""Spelling-tolerant lookup helpers for Informatica transformation properties.

``normalize_key``/``get_ci`` were originally written inside
``parsers/xml_parser.py`` to make the *parsers'* own TABLEATTRIBUTE
reads tolerant of casing/underscore-vs-space variance across PowerCenter
exports. found eight more call sites -- in ``converters/``,
``generators/``, and ``handlers/`` -- doing the same exact-match
``tx.properties.get("Some Spelling")`` lookup on the dict these parsers
produce, with the same silent-default failure mode on any spelling variance.

Those consumers importing from ``parsers/xml_parser`` would be backwards
(non-parser code reaching into a parser module for a utility that has
nothing to do with XML) and risks a future import cycle if a parser ever
needs something from ``converters/`` or ``handlers/``. This module is the
neutral home both sides import from without either direction being
"backwards". ``parsers/xml_parser.py`` re-exports both names so its own
existing call sites (and anything already importing them from there, e.g.
tests) keep working unchanged.

``is_truthy`` and ``aggregator_group_by_fallback`` also live here, for the
same reason: an Aggregator's GROUP BY derivation is needed identically by
``parsers/xml_parser.py`` (PowerCenter XML) and ``parsers/iics_parser.py``
(IICS JSON), and this module is already the shared, direction-neutral home
both import from.
"""

import re

from .models import DataFlowDirection, TransformationField

__all__ = [
    "normalize_key",
    "get_ci",
    "is_truthy",
    "aggregator_group_by_fallback",
]


def normalize_key(name: str) -> str:
    """Canonicalize a TABLEATTRIBUTE/property name for spelling-tolerant
    lookup: lowercase, then collapse runs of whitespace and/or underscores
    into a single space. "Sql Query", "SQL_QUERY", and "sql query" all
    normalize to "sql query".

    This is the ported-from-upstream fix for a defect class where our port
    read a TABLEATTRIBUTE by one exact spelling while the upstream Rust
    parser (the Rust reference implementation, powercentre.rs's lowercased TABLEATTRIBUTE
    match arms, :216-239) reads a whole set of spellings -- any casing or
    spacing variance in the export silently yielded an empty string
    (a dropped filter condition, SQL override, or join condition), never
    an error. Normalize once here, then match a spelling *set* per concept
    (see ``get_ci``) instead of chaining ``.get("Sql Query") or .get("SQL
    Query") or .get("sql_query")``.
    """
    return re.sub(r"[\s_]+", " ", name.strip().lower())


def get_ci(properties: dict, *names: str, default: str = "") -> str:
    """Spelling-tolerant lookup into a ``{raw_attribute_name: value}`` dict
    (e.g. a transformation's collected TABLEATTRIBUTEs, or ``tx.properties``).

    Every key in ``properties`` and every candidate in ``names`` is run
    through ``normalize_key`` before comparison, so callers list a concept's
    accepted spellings in natural form (``get_ci(table_attrs, "sql query",
    "sqlovrd")``) and get a match regardless of the export's casing or
    underscore/space convention. Returns ``default`` if none match.
    """
    if not names:
        return default
    normalized = {normalize_key(k): v for k, v in properties.items()}
    for name in names:
        key = normalize_key(name)
        if key in normalized:
            return normalized[key]
    return default


def is_truthy(value: str) -> bool:
    """Whether an Informatica boolean-ish attribute/flag value means "yes".

    Accepts the usual Informatica truthy spellings -- ``YES``, ``TRUE``,
    ``1``, ``Y`` -- case-insensitively, with surrounding whitespace ignored.
    Ported from the upstream Rust parser's ``is_truthy_attr``
    (the Rust reference implementation, src/parser/powercentre.rs), which a PowerCenter
    ``GROUPBY``/``ISGROUPBY`` or ``MASTER``/``ISMASTER`` TRANSFORMFIELD
    attribute uses.
    """
    return value.strip().upper() in ("YES", "TRUE", "1", "Y")


def _expr_references_port(expr_upper: str, port_name_upper: str) -> bool:
    """Whether the already-uppercased ``expr_upper`` references the
    already-uppercased ``port_name_upper`` as a whole identifier token.

    Word-boundary matching avoids a substring false-positive such as a port
    named ``AMOUNT`` matching inside an expression referencing
    ``LINE_AMOUNT``. Ported from the Rust reference implementation, src/ast.rs::
    expr_references_port.
    """
    if not port_name_upper:
        return False
    pattern = r"(?<![A-Za-z0-9_])" + re.escape(port_name_upper) + r"(?![A-Za-z0-9_])"
    return re.search(pattern, expr_upper) is not None


def aggregator_group_by_fallback(fields: list) -> list:
    """Derive an Aggregator's GROUP BY keys when no port carries an explicit
    group-by flag (the caller's job is to check for an explicit flag FIRST
    and only fall back to this heuristic when none is found -- see
    ``aggregator_group_keys`` in the Rust reference implementation, src/ast.rs, which this
    ports).

    Grouping is never inferred from an OUTPUT/INPUT_OUTPUT pass-through port
    that merely lacks an expression -- a pass-through port is a projected
    column, not a grouping key (a bug the Rust reference implementation has since fixed).
    Only Input-direction ports with NO expression at all are candidates, and
    only if they are not themselves consumed by an aggregate expression (a
    value port fed into SUM/AVG/etc. is an aggregation input, not a grouping
    key) -- checked with whole-identifier, word-boundary matching so e.g. a
    port named ``AMOUNT`` is not excluded by an expression that only
    mentions ``LINE_AMOUNT``.
    """
    agg_expr_uppers = [
        f.expression.upper()
        for f in fields
        if isinstance(f, TransformationField)
        and f.direction != DataFlowDirection.INPUT
        and f.expression
    ]
    return [
        f.name
        for f in fields
        if isinstance(f, TransformationField)
        and f.direction == DataFlowDirection.INPUT
        and not f.expression
        and not any(
            _expr_references_port(expr, f.name.upper()) for expr in agg_expr_uppers
        )
    ]
