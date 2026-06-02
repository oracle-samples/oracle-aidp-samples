"""Filter composition helpers shared across collection collectors.

The engine and the sampler both compose two filter clauses — the
dataset-level ``DatasetConfig.filter`` (broadly-scoping guard:
soft-delete exclusions, PII / tenant predicates) and a per-
collector ``collection.filter`` (narrowly-scoping for one
validation). Pre-2026-05-09 the engine's three collector call
sites composed these with **override** semantics
(``collection.filter or filter_expr``), silently dropping the
dataset-level guard whenever both were set. The sampler always
AND-combined; this module promotes the sampler's helper so
both code paths share one implementation.

The composition is **post-render**: callers render each filter
independently (Jinja → SQL string) before invoking
``and_combine_filters``. That prevents the rendered-empty Jinja
trap — a template like ``filter: "{{ optional_predicate }}"``
that resolves to ``""`` would otherwise compose to
``(rendered_dataset) AND ()`` (invalid SQL) under a pre-render
shape. Empty / whitespace-only operands are normalised to
``None`` before composition so a runtime-empty input behaves
identically to an explicitly-unset one.

The helper returns an instance of :class:`_RenderedSql` (a thin
``str`` subclass) so downstream collectors that own their own
``context.render(self.filter_expr)`` step can detect the
pre-rendered case and skip the second render. Re-rendering an
already-resolved string is normally idempotent on plain SQL,
but breaks when a rendered value happens to contain literal
``{{ }}`` markers (an exotic but possible result of templating
filter values out of upstream variables).

See ``docs/features/and-combine-collector-filter/plan.md`` for
the design rationale, Decision Pins, and migration shape.
"""

from __future__ import annotations


class _RenderedSql(str):
    """Marker subclass identifying a string that has already been
    Jinja-rendered. Collectors that own their own render step
    check ``isinstance(value, _RenderedSql)`` and skip the second
    render, preserving the rendered text byte-for-byte. Behaves
    like ``str`` everywhere else (SQL backends, logging,
    persistence — all unchanged).

    Internal to :mod:`qualifire.collection._filters` and the
    three filtered collectors. External callers shouldn't need
    to construct one; they get instances back from
    :func:`and_combine_filters`.
    """

    __slots__ = ()


def and_combine_filters(
    dataset_filter: str | None,
    collector_filter: str | None,
) -> str | None:
    """AND-combine the dataset-level and collector-level filter
    clauses. Either or both may be ``None`` / empty / whitespace-
    only (all three normalise to "no filter").

    Parameters
    ----------
    dataset_filter:
        The rendered ``DatasetConfig.filter`` string, or ``None``
        when the dataset declares no filter. Whitespace-only
        strings are treated as ``None``.
    collector_filter:
        The rendered collector-level ``filter`` string (the
        ``filter`` field on ``AggregationCollectionConfig`` /
        ``ProfilingCollectionConfig`` /
        ``MetricsCollectionConfig`` / ``SampleCollectionConfig``),
        or ``None`` when no collector filter is set. Whitespace-
        only treated as ``None``.

    Returns
    -------
    str | None
        ``None`` when both operands normalise to nothing.
        The other operand verbatim when exactly one is set.
        ``f"({dataset_filter}) AND ({collector_filter})"`` when
        both are set — each operand wrapped in its own pair of
        parentheses to keep operator-precedence safe in the
        presence of top-level ``OR`` clauses (Decision Pin 4).

    Notes
    -----
    Operands that are *already* parenthesised on input get a
    second pair: ``and_combine_filters("(a)", "b")`` returns
    ``"((a)) AND (b)"``. SQL engines treat double-parens as
    no-ops; "smart" stripping would require parsing SQL
    fragments, which is out of scope and risky.

    Caller contract: pass already-Jinja-rendered strings. The
    helper does not own a context. The engine's three collector
    call sites render each side via ``self.context.render(...)``
    before invoking this function (see
    ``QualifireEngine._collect``); the sampler does the same at
    ``qualifire/collection/sampler.py``.
    """
    ds = _normalise(dataset_filter)
    co = _normalise(collector_filter)

    if ds is None and co is None:
        return None
    if ds is None:
        return _RenderedSql(co)
    if co is None:
        return _RenderedSql(ds)
    return _RenderedSql(f"({ds}) AND ({co})")


def _normalise(value: str | None) -> str | None:
    """Return ``None`` for ``None`` / empty / whitespace-only
    inputs; otherwise return ``value`` unchanged.

    Whitespace normalisation is the runtime-safety
    counterpart to the Pydantic config-load coercion on
    ``AggregationCollectionConfig.filter`` /
    ``ProfilingCollectionConfig.filter`` /
    ``MetricsCollectionConfig.filter``. The Pydantic layer
    catches static YAML (``filter: ""``); this helper catches
    the dynamic case where a Jinja template resolves to empty
    or whitespace at render time.

    Returns the *original* string — not a stripped copy — when
    the input has any non-whitespace content. The composition
    contract preserves operator-authored whitespace inside
    multi-clause filters (``"a   AND   b"``).
    """
    if value is None:
        return None
    if not value.strip():
        return None
    return value


def render_filter_once(
    filter_expr: str | None,
    context: "Any",  # quoted to avoid heavy import here
) -> str | None:
    """Render ``filter_expr`` exactly once.

    The engine's ``QualifireEngine._compose_collector_filter``
    already renders each side and AND-combines the result,
    returning a :class:`_RenderedSql` instance. Re-running
    ``context.render`` on that instance is normally a no-op
    but breaks when the rendered output happens to contain
    literal ``{{ }}`` markers (an exotic but possible result
    of templating filter values out of upstream variables).
    Detecting the marker subclass keeps the collector's render
    path defensive without changing the public ``filter_expr``
    contract.

    Used by ``AggregationCollector.collect``,
    ``MetricsCollector.collect``, and
    ``ProfilingCollector.collect``. Returns ``None`` for ``None``
    inputs; otherwise returns the rendered string (skipping the
    render for ``_RenderedSql`` inputs).
    """
    if filter_expr is None:
        return None
    if isinstance(filter_expr, _RenderedSql):
        return str(filter_expr)
    return context.render(filter_expr)
