"""Lookup transformation semantics: multiple-match policy, NULL-key
handling, static-vs-dynamic cache ( Class C).

A broadcast join is the easy part -- any generator can emit that inline.
What makes this Class C is the **multiple-match policy**
(first/last/error/all), which is a decision about which lookup ROW wins
when more than one row in the lookup source shares the same key, and
which is identical across every mapping that uses a Lookup transformation
and silently wrong in any one of them if hand-rolled per call site.

Policy vocabulary and how it maps to Informatica's own options
----------------------------------------------------------------
Informatica's Lookup transformation ships four "Lookup Policy on
Multiple Match" options: Use First Value, Use Last Value, Use Any Value,
Report Error. This module's ``policy`` values are ``"first"``, ``"last"``,
``"error"``, and ``"all"`` (per the task brief). The mapping is direct
for three of them; ``"all"`` (return every matching row, i.e. do not
de-duplicate the lookup side at all -- an intentional fan-out) is this
library's addition, not one of Informatica's four. Conversely,
Informatica's "Use Any Value" has NO distinct implementation here: without
a live Integration Service export to check, this project cannot tell
whether "Any" means "physically first row encountered" or something
implementation-defined, and guessing silently is exactly what this
project forbids. **Assumption, flagged rather than guessed**: a mapping
carrying "Use Any Value" should be treated as ``policy="first"`` at
conversion time (documented here, not decided silently inside this
module) until a real export settles it.

NULL-key handling: Informatica lookup semantics never match a NULL key
(standard SQL equality -- ``NULL = NULL`` is not true). This module joins
on plain column-name equality (Spark's named-columns join form), which
already has that behavior built in, and uses ``how="left"`` so an input
row with no matching lookup row (whether because the key is NULL or
because there's genuinely no match) is preserved with NULL lookup columns
rather than dropped -- matching a connected Lookup's pass-through
behavior.

Cache: only ``cache="static"`` is implemented (build once, broadcast,
reuse for the life of the job -- Informatica's default "static cache").
``cache="dynamic"`` (a cache that mutates in place as the same session
inserts new target rows, with Informatica's ``NewLookupRow`` insert/
update signal) is a materially different, stateful algorithm and is
**not implemented** -- it raises :class:`NotImplementedError` with an
explanation rather than silently running a static broadcast join and
producing wrong results for a mapping that actually needed dynamic
semantics.

**Unverified against a live Spark runtime.** ``pyspark`` is only ever
imported inside :func:`cached_lookup` (deferred, per the "must import
cleanly without PySpark" requirement), and no local SparkSession is
available in this development environment. Tests exercise the emitted
*call structure* (which ``Window``/``F`` calls happen, in what order, with
what arguments) against a fake stand-in for the ``pyspark.sql.functions``/
``Window`` surface this module touches -- not real Spark execution.

**Open question, genuinely unresolved: dotted join-key names.** Three
corpus fixtures with a real Lookup name their join key with a literal
dot in it (e.g. ``"SQ_EMPLOYEES.EMP_ID"`` -- the Source Qualifier
instance name Informatica put on one side of its own lookup condition,
carried through by ``transformation_converter.py``'s
``withColumnRenamed`` step so both sides share that exact literal
string as a column name). The previous inline implementation joined via
bracket indexing (``df["SQ_EMPLOYEES.EMP_ID"]``); :func:`cached_lookup`
now joins via Spark's ``on=[...]`` named-column list form instead
(``df.join(lookup_df, on=keys, how="left")``). Whether Spark resolves a
literal-dotted string identically under both forms is **not settled** --
a dot inside a column-name string is also PySpark's syntax for a
qualified/nested reference (``"table.column"`` / struct field access),
so ``on=["SQ_EMPLOYEES.EMP_ID"]`` could plausibly be parsed as a
qualifier lookup rather than a literal column name. PySpark is not
installed in this environment, so this cannot be settled locally --
**this needs verification on a live Spark session** before anyone
relies on it. No normalization is applied here: see
:func:`cached_lookup`'s own docstring for why a "strip the prefix"
defensive fix was considered and rejected as not obviously safe.
"""
from __future__ import annotations

from typing import Any, Optional, Sequence, Union

VALID_POLICIES = ("first", "last", "error", "all")


class LookupMultipleMatchError(Exception):
    """Raised when ``policy="error"`` and more than one lookup row shares
    a key -- Informatica's "Report Error" multiple-match policy stops the
    session rather than silently choosing a row.
    """


def _normalize_keys(on: Union[str, Sequence[str]]) -> list[str]:
    keys = [on] if isinstance(on, str) else list(on)
    if not keys:
        raise ValueError("cached_lookup requires at least one join key column in `on`")
    return keys


def cached_lookup(
    df: Any,
    lookup_df: Any,
    on: Union[str, Sequence[str]],
    policy: str = "first",
    cache: str = "static",
    order_by: Optional[str] = None,
) -> Any:
    """Left-join ``df`` to ``lookup_df`` on ``on``, resolving multiple
    lookup-side matches per ``policy``.

    Args:
        df: the pipeline DataFrame being enriched (input rows are always
            preserved -- this is a left join, never inner, matching a
            connected Lookup transformation's pass-through behavior).
        lookup_df: the reference/dimension DataFrame to look up into.
        on: join key column name(s), present in both ``df`` and
            ``lookup_df``. **Dotted names (e.g.
            ``"SQ_EMPLOYEES.EMP_ID"`` -- see the module docstring)
            are passed through UNCHANGED to Spark's ``on=[...]``
            named-column join form. Whether Spark resolves that
            literal string identically to the previous bracket-
            indexing implementation is UNVERIFIED -- no live Spark
            session is available here to check, and a dot in a column
            name is also valid Spark syntax for a qualified/nested
            reference, so this is a real, not theoretical, ambiguity.**
            A "strip the ``<instance>.`` prefix" normalization was
            considered and rejected: this function never aliases
            ``lookup_df`` itself, so there is no reliable
            "``lookup_df``'s own alias" to match the prefix against --
            the dotted prefix in the corpus fixtures is the *source*
            Qualifier's instance name, not anything derived from
            ``lookup_df``, so stripping it here would be a guess, not
            a safe simplification. Verify on a live Spark session
            before changing this.
        policy: ``"first"``/``"last"`` -- keep one lookup row per key,
            breaking ties by ``order_by`` if given, else by the order
            ``lookup_df`` was read in (approximated with
            ``F.monotonically_increasing_id()`` -- see the caveat in
            :func:`_dedupe_first_last`). ``"error"`` -- raise
            :class:`LookupMultipleMatchError` if any key has more than
            one lookup row. ``"all"`` -- no de-duplication; a key with N
            lookup rows fans out to N result rows for that input row.
        cache: only ``"static"`` is implemented; ``"dynamic"`` raises
            :class:`NotImplementedError` (see module docstring).
        order_by: optional explicit ordering column for ``"first"``/
            ``"last"`` tie-breaking. Prefer this over the
            ``monotonically_increasing_id()`` default whenever the
            lookup source has a natural ordering column (e.g. a load
            timestamp or ID) -- it is more reliable than relying on
            physical read order, which Spark does not guarantee is
            stable across a shuffle.

    Never downgrades a policy to a cheaper one and never drops rows
    silently -- an unrecognized ``policy`` or unsupported ``cache`` value
    raises immediately rather than falling back to some default
    behavior.
    """
    if policy not in VALID_POLICIES:
        raise ValueError(
            f"Unknown multiple-match policy {policy!r} -- expected one of "
            f"{VALID_POLICIES}. See module docstring for how these map to "
            f"Informatica's Use First/Use Last/Use Any/Report Error options."
        )
    if cache != "static":
        raise NotImplementedError(
            f"cache={cache!r} is not implemented -- only 'static' (build "
            f"the cache once, broadcast, reuse for the life of the job) is "
            f"supported. A dynamic cache (mutated in place as the session "
            f"inserts new target rows, with Informatica's NewLookupRow "
            f"insert/update signal) is a different, stateful algorithm "
            f"this library does not implement; do not treat this as a "
            f"static-cache fallback."
        )

    keys = _normalize_keys(on)

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    if policy == "all":
        resolved_lookup = lookup_df
    elif policy == "error":
        _assert_no_duplicate_keys(lookup_df, keys, F)
        resolved_lookup = lookup_df
    else:
        resolved_lookup = _dedupe_first_last(lookup_df, keys, policy, order_by, F, Window)

    return df.join(resolved_lookup, on=keys, how="left")


def _assert_no_duplicate_keys(lookup_df: Any, keys: list[str], F: Any) -> None:
    dup_groups = lookup_df.groupBy(*keys).count().filter(F.col("count") > 1)
    if dup_groups.limit(1).count() > 0:
        raise LookupMultipleMatchError(
            f"policy='error': the lookup source has more than one row for "
            f"at least one key combination in {keys} -- Informatica's "
            f"'Report Error' multiple-match policy stops the session "
            f"rather than guessing which row should win."
        )


def _dedupe_first_last(
    lookup_df: Any,
    keys: list[str],
    policy: str,
    order_by: Optional[str],
    F: Any,
    Window: Any,
) -> Any:
    """Keep exactly one row per ``keys`` combination, breaking ties by
    ``order_by`` (ascending for "first", descending for "last") if given,
    else by ``F.monotonically_increasing_id()`` over the DataFrame as
    handed in.

    Caveat carried in the module docstring: ``monotonically_increasing_id``
    reflects physical partition/row order at the point this function
    runs, which is a reasonable proxy for "the order the lookup source
    was read in" ONLY if nothing has shuffled ``lookup_df`` beforehand.
    Callers with a lookup source where read-order matters and might have
    been shuffled should pass an explicit ``order_by`` column instead.
    """
    used_synthetic_order = order_by is None
    order_col = order_by if order_by is not None else "_infa_lookup_seq"
    base = lookup_df
    if used_synthetic_order:
        base = base.withColumn(order_col, F.monotonically_increasing_id())
    direction = F.col(order_col).asc() if policy == "first" else F.col(order_col).desc()
    window = Window.partitionBy(*keys).orderBy(direction)
    ranked = base.withColumn("_infa_lookup_rn", F.row_number().over(window))
    deduped = ranked.filter(F.col("_infa_lookup_rn") == 1).drop("_infa_lookup_rn")
    if used_synthetic_order:
        deduped = deduped.drop(order_col)
    return deduped


def lookup_join(
    df: Any,
    lookup_df: Any,
    condition: str,
    policy: str = "first",
    order_by: Optional[Sequence[str]] = None,
) -> Any:
    """Connected Lookup with Informatica's semantics, for any condition.

    ``condition`` is a Spark SQL predicate over ``df``'s columns and
    ``lookup_df``'s columns, whose names must not overlap (the generator
    qualifies every lookup port as ``<lookup>__<port>``). Equality,
    ``<>``, ``<``, ``<=``, ``>``, ``>=`` and literals all work -- an
    effective-dated lookup (``EFF_FROM <= IN_DATE AND EFF_TO >= IN_DATE``)
    is not an equi-join, so :func:`cached_lookup`'s ``on=`` keys cannot
    express it.

    Multiple matches are resolved PER INPUT ROW, which is what
    Informatica does: "Use First Value" / "Use Last Value" take the first
    / last matching row of the cache, and the cache is ordered by the
    lookup ports (the ORDER BY the Integration Service generates lists
    them all, NULL highest) -- pass them in port order as ``order_by``.
    "Report Error" raises :class:`LookupMultipleMatchError` when any
    input row matches more than one lookup row. "all" keeps every match.

    Every input row is kept (left join); no match -- including a NULL
    key, since NULL = anything is not true -- leaves the lookup columns
    NULL.
    """
    if policy not in VALID_POLICIES:
        raise ValueError(
            f"Unknown multiple-match policy {policy!r} -- expected one of {VALID_POLICIES}"
        )
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    overlap = set(df.columns) & set(lookup_df.columns)
    if overlap:
        raise ValueError(
            f"lookup_join: column name(s) {sorted(overlap)} exist on both sides; "
            f"qualify the lookup's columns first"
        )
    rid = "__infa_lkp_rid"
    left = df.withColumn(rid, F.monotonically_increasing_id())
    joined = left.join(F.broadcast(lookup_df), on=F.expr(condition), how="left")
    if policy == "all":
        return joined.drop(rid)
    if policy == "error":
        dup = joined.groupBy(rid).count().filter(F.col("count") > 1)
        if dup.limit(1).count() > 0:
            raise LookupMultipleMatchError(
                "policy='error': an input row matches more than one lookup row -- "
                "Informatica's 'Report Error' multiple-match policy does not choose one."
            )
        return joined.drop(rid)
    cols = list(order_by or lookup_df.columns)
    if policy == "first":
        order = [F.col(c).asc_nulls_last() for c in cols]
    else:
        order = [F.col(c).desc_nulls_first() for c in cols]
    w = Window.partitionBy(rid).orderBy(*order)
    return (
        joined.withColumn("__infa_lkp_rn", F.row_number().over(w))
        .filter(F.col("__infa_lkp_rn") == 1)
        .drop("__infa_lkp_rn", rid)
    )
