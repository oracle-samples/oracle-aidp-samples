"""MERGE-related SQL composition helpers.

Content-pack strategy executors import MERGE helpers from this public
module instead of reaching through package internals. The source-of-truth
functions stay in ``orchestrator/__init__.py`` and this module
**re-exports** them so the import path
``oracle_ai_data_platform_fusion_autopilot.orchestrator.merge_helpers`` is
stable without rewriting existing callsites.

Why re-export instead of move
-----------------------------
The existing helpers have golden-snapshot SQL tests. Moving them would
require updating every import sitewide and would risk subtle behavioral
drift. The re-export approach gives content-pack execution a canonical
location without disturbing established callsites.

This module also houses public wrappers and composition helpers:

* :func:`build_natural_key_join_sql` — public-named variant of the
  re-exported ``_natural_key_join_sql`` (no leading underscore;
  v2 callers should prefer this name).
* :func:`build_payload_diff_predicate_sql` — public-named variant of
  ``_payload_diff_predicate_sql``.
* :func:`compose_merge_sql` — assembles the full ``MERGE INTO ... USING
  (...) ON ... WHEN MATCHED ... WHEN NOT MATCHED ...`` statement from
  the node's natural key + source SQL + an optional payload-diff
  predicate.

Maintainer notes
----------------
MERGE rendering must preserve NULL-safe natural-key joins, payload-diff-gated
updates, and target schema reconciliation. These are runtime correctness
requirements, not formatting preferences.
"""

from __future__ import annotations

from collections.abc import Iterable

# Re-export the established helpers under the public module path.
from . import (
    _natural_key_join_sql as _v1_natural_key_join_sql,
    _payload_diff_predicate_sql as _v1_payload_diff_predicate_sql,
)
from .state import (
    _ensure_target_schema_for_merge as _v1_ensure_target_schema_for_merge,
)

__all__ = [
    # Re-exports (source-of-truth; same identity).
    "_natural_key_join_sql",
    "_payload_diff_predicate_sql",
    "_ensure_target_schema_for_merge",
    # Public-named wrappers.
    "build_natural_key_join_sql",
    "build_payload_diff_predicate_sql",
    "ensure_target_schema_for_merge",
    # MERGE statement composer.
    "compose_merge_sql",
]


# Re-exports — these are the same callable objects as the v1 originals;
# tests asserting `obj is module._natural_key_join_sql` will pass under
# both import paths.
_natural_key_join_sql = _v1_natural_key_join_sql
_payload_diff_predicate_sql = _v1_payload_diff_predicate_sql
_ensure_target_schema_for_merge = _v1_ensure_target_schema_for_merge


def build_natural_key_join_sql(
    natural_key: str | tuple[str, ...] | list[str],
    *,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str:
    """Public-named NULL-safe natural-key join predicate builder.

    Content-pack callers use this name; existing callers keep the
    underscore-prefixed alias. Behavior is identical.

    Accepts a plain ``list[str]`` in addition to ``str`` / ``tuple[str,
    ...]`` to match ``NodeYaml.refresh.incremental.natural_key`` which
    is a list.
    """
    if isinstance(natural_key, list):
        natural_key = tuple(natural_key)
    return _natural_key_join_sql(
        natural_key, target_alias=target_alias, src_alias=src_alias
    )


def build_payload_diff_predicate_sql(
    data_columns: Iterable[str],
    *,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str | None:
    """Public-named payload-diff predicate builder.

    Same behaviour as v1; ``None`` return signals the caller to fall
    back to unconditional ``UPDATE SET *``.
    """
    return _payload_diff_predicate_sql(
        data_columns, target_alias=target_alias, src_alias=src_alias
    )


def ensure_target_schema_for_merge(*args, **kwargs):  # type: ignore[no-untyped-def]
    """Public-named schema-reconciliation helper.

    Re-exports ``orchestrator.state._ensure_target_schema_for_merge``.
    Strategy executors call this before running MERGE so nullable-column
    additions in the rendered source DataFrame are auto-added to the
    target Delta table.
    """
    return _ensure_target_schema_for_merge(*args, **kwargs)


# ---------------------------------------------------------------------------
# compose_merge_sql — full MERGE statement assembly
# ---------------------------------------------------------------------------


def compose_merge_sql(
    *,
    target: str,
    source_sql: str,
    natural_key: list[str] | tuple[str, ...],
    payload_diff_predicate: str | None = None,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str:
    """Assemble the full ``MERGE INTO`` statement for the content-pack ``merge`` strategy.

    Shape:

    ::

        MERGE INTO <target> AS <target_alias>
        USING (<source_sql>) AS <src_alias>
        ON <natural-key NULL-safe predicate>
        WHEN MATCHED [AND <payload-diff predicate>] THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *

    The optional payload-diff predicate gates updates so unchanged rows
    don't rewrite their audit columns each cycle. Supply ``None`` to disable.

    Args:
        target: fully-qualified target table identifier (already
            allowlist-checked by the renderer / orchestrator).
        source_sql: the rendered SELECT that produces the merge source
            DataFrame's rows. May contain ``:param`` markers — the
            caller supplies the corresponding ``args=`` dict to
            ``spark.sql``.
        natural_key: list of natural-key column names. Single-element
            list is fine; empty list raises.
        payload_diff_predicate: optional ``IS DISTINCT FROM`` predicate
            from :func:`build_payload_diff_predicate_sql`. When
            non-None, gates the ``WHEN MATCHED`` clause; the resulting
            UPDATE only fires on rows whose payload changed.
        target_alias / src_alias: SQL aliases. Defaults match the v1
            helpers so existing tests stay stable.

    Returns:
        The full MERGE INTO statement as a single string (no trailing
        semicolon; the rendered SQL is a single statement without terminator).

    Raises:
        ValueError: ``natural_key`` is empty.
    """
    if not natural_key:
        raise ValueError(
            "compose_merge_sql: natural_key is empty. The MERGE strategy "
            "requires a natural key (AIDPF-2020)."
        )

    on_predicate = build_natural_key_join_sql(
        list(natural_key), target_alias=target_alias, src_alias=src_alias
    )

    if payload_diff_predicate is not None:
        when_matched = (
            f"WHEN MATCHED AND ({payload_diff_predicate}) THEN UPDATE SET *"
        )
    else:
        when_matched = "WHEN MATCHED THEN UPDATE SET *"

    return (
        f"MERGE INTO {target} AS {target_alias}\n"
        f"USING ({source_sql}) AS {src_alias}\n"
        f"ON {on_predicate}\n"
        f"{when_matched}\n"
        f"WHEN NOT MATCHED THEN INSERT *"
    )
