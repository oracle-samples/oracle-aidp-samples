"""Informatica ``DECODE`` fallthrough semantics -- shared seam.

Same rule as ``datemask.py``: ``DECODE`` itself is
Class A -- a pure scalar expression with no runtime state, so the
deterministic compiler keeps emitting it INLINE as a chained
``F.when(...).otherwise(...)`` expression (25 tests already cover that
path in ``test_expression_converter.py``; wrapping it in a library call
would add indirection for zero correctness gain, per the design). What moves
here is the *pair/fallthrough resolution logic* -- "walk (search, result)
pairs in order, first match wins, run out of pairs and land on the
default" -- so the compiler's inline emitter and any Class C call site
that needs the same fallthrough evaluated on a concrete Python value
(driver-side config resolution, a lookup default policy expressed as a
DECODE-shaped rule, etc.) share one implementation of that walk instead of
two copies that can drift.

Two entry points:

- :func:`decode_fallthrough` -- runtime evaluator. Takes a concrete value
  and returns the concrete result, exactly like Informatica's
  ``DECODE(value, search1, result1, search2, result2, ..., default)``
  evaluated for one row. This is the Class C public API named in the
  task brief.
- :func:`build_when_chain` -- code-shape helper used by
  ``expression_converter.py``'s ``_convert_decode`` to build the inline
  Spark expression string. Moved here (not duplicated) so both consumers
  walk the same pair/default-splitting logic
  (:func:`split_pairs_and_default`).
"""
from __future__ import annotations

from typing import Any, Optional, Sequence

_SENTINEL = object()


def split_pairs_and_default(
    values: Sequence[Any],
) -> tuple[list[tuple[Any, Any]], Any]:
    """Split a flat ``[search1, result1, search2, result2, ..., [default]]``
    sequence into ``(pairs, default)``.

    An odd count means the last element is a trailing default (Informatica
    ``DECODE``'s own convention: an unpaired final argument is the
    fallthrough value, returned when nothing else matches). An even count
    means there is no explicit default -- callers get ``None`` back for it,
    matching Informatica's behavior of returning ``NULL`` when nothing
    matches and no default was supplied.
    """
    items = list(values)
    default: Any = None
    if len(items) % 2 == 1:
        default = items.pop()
    pairs = [(items[i], items[i + 1]) for i in range(0, len(items), 2)]
    return pairs, default


def decode_fallthrough(value: Any, *values: Any, default: Any = _SENTINEL) -> Any:
    """Runtime evaluator for Informatica's ``DECODE`` fallthrough.

    ``decode_fallthrough(value, search1, result1, search2, result2, ...)``
    walks the ``(search, result)`` pairs in order and returns the first
    ``result`` whose ``search`` equals ``value`` (Python ``==``, so this
    inherits Python's numeric/str equality -- callers comparing a Spark
    column value pulled to the driver should ensure the same type
    coercion the mapping intended has already happened before calling
    this).

    If nothing matches: an explicit trailing value in ``values`` (odd
    total count) is used as the default; otherwise the ``default=``
    keyword is used if given; otherwise ``None`` (Informatica's ``DECODE``
    itself returns ``NULL`` for an unmatched value with no default
    clause -- this mirrors that rather than raising, since a raise here
    would make this function unusable as a straight ``DECODE`` drop-in
    for the common no-default case).

    This is a Python-value evaluator, not a Spark expression builder --
    for emitting inline Spark code from already-converted argument
    strings, see ``build_when_chain`` (used by
    ``expression_converter.py``). Do not call this per-row from a UDF on
    a large DataFrame; that defeats Class A's whole reason for existing
    (pure scalar expressions get pushed down as native Spark, never a
    Python UDF). This function is for driver-side / config-resolution
    call sites where a concrete value is already in hand.
    """
    pairs, trailing_default = split_pairs_and_default(values)
    for search, result in pairs:
        if value == search:
            return result
    if trailing_default is not None:
        return trailing_default
    if default is not _SENTINEL:
        return default
    return None


def build_when_chain(
    value_expr: str,
    pairs: Sequence[tuple[str, str]],
    default_expr: str = "F.lit(None)",
) -> str:
    """Build the inline PySpark expression string for a DECODE call,
    given already-converted argument strings (the compiler has already
    recursively converted every sub-expression by the time this is
    called -- this function only assembles the chain).

    ``expression_converter.py``'s ``_convert_decode`` is the sole caller;
    this used to be inlined there and is relocated here verbatim (
    Step 2) so it shares :func:`split_pairs_and_default`'s pair-splitting
    convention with :func:`decode_fallthrough` instead of the compiler
    re-deriving "odd count means trailing default" on its own.
    """
    if not pairs:
        return f"F.lit(None).otherwise({default_expr})"
    chain = "F.when(" + _match(value_expr, pairs[0][0]) + f", {pairs[0][1]})"
    for search, result in pairs[1:]:
        chain += ".when(" + _match(value_expr, search) + f", {result})"
    return f"{chain}.otherwise({default_expr})"


# Converted forms of an Informatica NULL literal, as the compiler emits them.
_NULL_LITERALS = frozenset({"F.lit(None)", "None", "F.lit(null)"})


def _match(value_expr: str, search: str) -> str:
    """The condition for one DECODE branch.

    ``DECODE`` matches NULL to NULL -- ``DECODE(x, NULL, 'missing')`` fires
    when x is NULL. SQL's ``x = NULL`` is never true, so emitting ``==``
    for a NULL search value produces a branch that can never be taken and
    silently falls through to the default. Comparison to a NULL literal
    therefore becomes ``.isNull()``.
    """
    if search.strip() in _NULL_LITERALS:
        return f"{value_expr}.isNull()"
    return f"{value_expr} == {search}"
