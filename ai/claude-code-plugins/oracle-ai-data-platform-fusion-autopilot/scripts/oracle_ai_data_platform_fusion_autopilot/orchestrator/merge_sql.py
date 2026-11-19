"""Neutral SQL-string helpers for MERGE rendering.

This module hosts the explicit-column-list MERGE clause helpers
(``build_explicit_when_matched_clause`` / ``build_explicit_when_not_matched_clause``)
that the content-pack strategy executors render against silver/gold
targets. Kept in a neutral standard-library-only module so any future
caller — engine-side or schema-side — can import it without dragging
in orchestrator state.

Bronze's payload-diff helpers (``_payload_diff_predicate_sql`` /
``_natural_key_join_sql``) live next to the bronze adapter in
``orchestrator/__init__.py`` and are re-exported by
``orchestrator/merge_helpers.py``; they stay there because they're
only consumed in-module.

Function names are intentionally **public** (no leading underscore)
because they cross module boundaries. Same convention as
``runtime.enrich_bronze_audit_cols``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable


def build_explicit_when_matched_clause(
    columns: "Iterable[str]",
    *,
    payload_diff: str | None = None,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str:
    """Build an explicit-column-list ``WHEN MATCHED`` clause for use when
    target-wider schema drift requires omitting target-only columns
    from the UPDATE.

    The V1 shape ``WHEN MATCHED [AND (predicate)] THEN UPDATE SET *``
    expands ``SET *`` at plan time using the union of source + target
    columns; when target carries columns source lacks, ``SET *`` either
    silently NULLs them or raises ``AnalysisException`` (Spark-version
    dependent). The explicit form lists exactly which columns the
    UPDATE should touch, preserving any target-only columns by
    omission.

    When ``payload_diff`` is non-None, the AND-clause gates the UPDATE so
    unchanged-payload rows don't propagate ``_extract_ts`` rewrites
    downstream.

    Args:
        columns: The list of columns to UPDATE on a matched row.
            Typically ``reconcile.common_columns +
            reconcile.source_only_columns`` from
            :func:`state._ensure_target_schema_for_merge` — i.e.,
            every column the source DataFrame carries (target-only
            columns are excluded by being absent from this list).
        payload_diff: Optional payload-diff predicate from
            :func:`_payload_diff_predicate_sql`. When given,
            the clause becomes ``WHEN MATCHED AND ({payload_diff})
            THEN UPDATE SET ...``. When ``None``, no AND-gate is
            inserted (V1 behavior for layers that don't use the
            payload-diff optimization — silver/gold under current
            shipped state).
        target_alias: SQL alias of the MERGE target. Default
            ``"target"``.
        src_alias: SQL alias of the MERGE source. Default ``"src"``.

    Returns:
        SQL fragment starting with ``WHEN MATCHED`` and ending with
        ``UPDATE SET <explicit list>``. No leading or trailing
        whitespace; safe to inline into a multi-line MERGE template.

    Examples:
        >>> build_explicit_when_matched_clause(["col_a", "col_b"])
        'WHEN MATCHED THEN UPDATE SET target.col_a = src.col_a, target.col_b = src.col_b'
        >>> build_explicit_when_matched_clause(
        ...     ["col_a"],
        ...     payload_diff="target.col_a IS DISTINCT FROM src.col_a",
        ... )
        'WHEN MATCHED AND (target.col_a IS DISTINCT FROM src.col_a) THEN UPDATE SET target.col_a = src.col_a'
    """
    cols = tuple(columns)
    if not cols:
        # Defensive — caller should pass at least one column.
        raise ValueError(
            "build_explicit_when_matched_clause: columns is empty; "
            "the MERGE would have no UPDATE targets. Pass at least "
            "the natural-key columns."
        )
    set_clause = ", ".join(
        f"{target_alias}.{c} = {src_alias}.{c}" for c in cols
    )
    if payload_diff is not None:
        return f"WHEN MATCHED AND ({payload_diff}) THEN UPDATE SET {set_clause}"
    return f"WHEN MATCHED THEN UPDATE SET {set_clause}"


def build_explicit_when_not_matched_clause(
    columns: "Iterable[str]",
    *,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str:
    """Build an explicit-column-list ``WHEN NOT MATCHED`` clause.

    Mirrors :func:`build_explicit_when_matched_clause` — when target
    has columns source lacks, ``INSERT *`` would either silently
    leave them NULL or raise. The explicit form names the source
    columns + lists them in ``VALUES (...)``, leaving target-only
    columns to take their Delta-declared default (typically NULL).

    Args:
        columns: Source-side column list. Same shape as the matched
            clause — ``reconcile.common_columns +
            reconcile.source_only_columns``.
        target_alias: SQL alias of the MERGE target. Default
            ``"target"`` (unused by INSERT itself, but kept for
            signature symmetry).
        src_alias: SQL alias of the MERGE source. Default ``"src"``.

    Returns:
        SQL fragment starting with ``WHEN NOT MATCHED`` and ending with
        ``INSERT (col1, col2, ...) VALUES (src.col1, src.col2, ...)``.

    Examples:
        >>> build_explicit_when_not_matched_clause(["col_a", "col_b"])
        'WHEN NOT MATCHED THEN INSERT (col_a, col_b) VALUES (src.col_a, src.col_b)'
    """
    del target_alias  # Signature symmetry with the matched-clause helper.
    cols = tuple(columns)
    if not cols:
        raise ValueError(
            "build_explicit_when_not_matched_clause: columns is empty; "
            "INSERT requires at least one column. Pass at least "
            "the natural-key columns."
        )
    insert_cols = ", ".join(cols)
    values = ", ".join(f"{src_alias}.{c}" for c in cols)
    return f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({values})"


__all__ = [
    "build_explicit_when_matched_clause",
    "build_explicit_when_not_matched_clause",
]
