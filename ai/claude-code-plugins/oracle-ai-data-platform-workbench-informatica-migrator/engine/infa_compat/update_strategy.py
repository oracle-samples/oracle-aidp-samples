"""Update Strategy transformation: DD_INSERT/DD_UPDATE/DD_DELETE/DD_REJECT
routing ( Class C).

Informatica's Update Strategy transformation tags every row with one of
four codes (``DD_INSERT=0``, ``DD_UPDATE=1``, ``DD_DELETE=2``,
``DD_REJECT=3``) and the session applies each partition differently at
the target. That is repetitive, mechanical, and -- per the task brief --
"easy to get subtly wrong" in exactly one place across many generated
mappings: silently dropping ``DD_REJECT`` rows instead of routing them
somewhere durable is the single worst failure this module exists to
prevent (rejects are supposed to land in a reject file/table for
operator review, per Informatica's own session-level "reject file"
concept -- silently dropping them hides data-quality problems that the
mapping author explicitly tried to surface).

Two-step API, deliberately: :func:`apply_update_strategy` only SPLITS
the DataFrame into the four partitions (a pure Spark ``filter`` per
code -- cheap, lazy, no action forced). :func:`write_update_strategy`
then does the actual writes, honoring the same Delta/ADW write-strategy
split as ``write_strategies.py``: Delta gets a real
``DeltaTable.merge`` with an UPDATE-only or DELETE-only branch; ADW gets
the stage-via-JDBC-overwrite-then-driver-side-MERGE algorithm (via the
shared ``_adw_runtime`` helper), because external catalogs have no
Spark-side MERGE at all.

This mirrors, but does not import from, ``engine/infa2aidp/generators/
write_strategies.py``: that module emits notebook SOURCE TEXT for the
workstation-side generator; this module actually EXECUTES the write at
cluster runtime. Same algorithmic split (explicit ``target_catalog_type``,
never inferred; never silently downgrade an unsatisfiable operation to
something that loses rows), different consumer, so it is a separate
module rather than a shared import across the workstation/cluster
package boundary (``infa_compat`` must stay installable standalone on
the cluster with no dependency on ``infa2aidp``).

**Unverified against a live Spark/Delta or ADW runtime** -- see the
module docstrings of ``lookup.py``/``sequence.py`` for the same caveat;
``pyspark``/``delta``/``oracledb`` are only ever touched inside function
bodies.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, Optional, Sequence, Union

from . import _adw_runtime


class UpdateStrategyCode(IntEnum):
    """Informatica's ``DD_*`` constants, as literally documented."""

    DD_INSERT = 0
    DD_UPDATE = 1
    DD_DELETE = 2
    DD_REJECT = 3


class NonIntegerStrategyColumnError(TypeError):
    """Raised by :func:`apply_update_strategy` when ``strategy_col`` is
    not an integer-typed Spark column.

    Informatica's own expression-language names for these codes --
    ``DD_INSERT``, ``DD_UPDATE``, ``DD_DELETE``, ``DD_REJECT`` -- are
    STRING labels, which makes writing e.g. ``F.lit("DD_INSERT")``
    upstream the natural-looking thing to do. Filtering a string column
    against the :class:`UpdateStrategyCode` INTEGER literals then
    matches nothing on every one of the four partitions: no error, no
    rows, no signal -- exactly the silent-failure class this project
    exists to eliminate. This exception turns that into a loud failure
    at the point the mismatch actually exists.
    """


# Spark's own `DataFrame.dtypes` short names for every integer type Delta/
# ADW plausibly stores a small code column as. `apply_update_strategy` is
# always called with a REAL DataFrame (never at LLM/graph-build time, when
# only a column NAME is known) -- so, unlike the generator/system-prompt
# side of this contract, a schema-based check here is reliable, not a
# guess. That is why this module checks dtype rather than sniffing values.
_INTEGER_DTYPES = frozenset({"tinyint", "smallint", "int", "bigint"})


def _assert_strategy_col_is_integer_typed(df: Any, strategy_col: str) -> None:
    """Raise :class:`NonIntegerStrategyColumnError` if ``strategy_col``
    is present on ``df`` with a non-integer Spark dtype (most commonly:
    a STRING column holding ``"DD_INSERT"``/etc. instead of the
    :class:`UpdateStrategyCode` integer). If ``strategy_col`` is absent
    from ``df`` entirely, this deliberately does nothing -- the
    subsequent ``.filter()`` call will raise Spark's own
    ``AnalysisException`` for the unknown column, which is already loud.
    """
    dtypes = dict(df.dtypes)
    if strategy_col not in dtypes:
        return
    actual = dtypes[strategy_col]
    if actual not in _INTEGER_DTYPES:
        raise NonIntegerStrategyColumnError(
            f"apply_update_strategy(strategy_col={strategy_col!r}) requires an "
            f"INTEGER-typed column holding UpdateStrategyCode values "
            f"(DD_INSERT=0, DD_UPDATE=1, DD_DELETE=2, DD_REJECT=3), but "
            f"{strategy_col!r} has Spark dtype {actual!r}. Do NOT write the "
            f"Informatica expression-language names as strings (e.g. "
            f'F.lit("DD_INSERT")) into this column -- that would filter '
            f"against the integer codes and silently produce four EMPTY "
            f"partitions with no error. Cast the column to an integer type "
            f"and use UpdateStrategyCode.DD_INSERT / .DD_UPDATE / .DD_DELETE / "
            f".DD_REJECT (or the equivalent literal ints) instead."
        )


@dataclass
class UpdateStrategyResult:
    """The four partitions produced by :func:`apply_update_strategy`.
    Each is a lazy DataFrame filter -- no action has been forced yet.
    """

    inserts: Any
    updates: Any
    deletes: Any
    rejects: Any


def apply_update_strategy(df: Any, strategy_col: str = "DD_STRATEGY") -> UpdateStrategyResult:
    """Split ``df`` into insert/update/delete/reject partitions by the
    integer code in ``strategy_col``.

    Deferred ``pyspark`` import. Filters on the literal
    :class:`UpdateStrategyCode` integer values, never on a string label
    -- Informatica's own ``DD_INSERT``/etc. expression constants resolve
    to these integers, and the deterministic compiler emits the same
    integers when it builds ``strategy_col`` upstream (Class A territory;
    this module only consumes the result).

    Raises :class:`NonIntegerStrategyColumnError` immediately if
    ``strategy_col`` is present but not integer-typed (e.g. a mapping
    that wrote the ``"DD_INSERT"``/etc. STRING names instead of the
    integer codes) -- checked via ``df.dtypes`` before any filter runs,
    so this fails loudly instead of returning four correct-looking but
    silently EMPTY partitions.
    """
    _assert_strategy_col_is_integer_typed(df, strategy_col)

    from pyspark.sql import functions as F

    def _part(code: UpdateStrategyCode) -> Any:
        # The routing column is bookkeeping, not target data: it is dropped
        # from every partition so a downstream whenMatchedUpdateAll() /
        # insertAll() / append does not try to write a DD_STRATEGY column
        # the target table does not have.
        return df.filter(F.col(strategy_col) == int(code)).drop(strategy_col)

    return UpdateStrategyResult(
        inserts=_part(UpdateStrategyCode.DD_INSERT),
        updates=_part(UpdateStrategyCode.DD_UPDATE),
        deletes=_part(UpdateStrategyCode.DD_DELETE),
        rejects=_part(UpdateStrategyCode.DD_REJECT),
    )


def write_update_strategy(
    result: UpdateStrategyResult,
    target: str,
    keys: Sequence[str],
    reject_sink: Union[str, Callable[[Any], None]],
    *,
    target_catalog_type: str = "delta",
    spark: Any = None,
    jdbc_options: Optional[dict] = None,
    adw_connection: Any = None,
    staging_table: Optional[str] = None,
    update_columns: Optional[Sequence[str]] = None,
) -> None:
    """Apply each partition of ``result`` to ``target``, and route
    ``rejects`` to ``reject_sink`` -- NEVER dropped.

    ``reject_sink`` is a required argument (no default) by design: a
    caller must always say where rejects go, exactly like an Informatica
    session always has a configured reject file whether or not any row
    ever lands in it. Pass either a table name (rejects are appended to
    it via the same write path as ``target``) or a callable
    ``reject_sink(df)`` for a custom sink (e.g. a volume path, an
    external logging call).

    ``target_catalog_type`` picks Delta vs ADW semantics (
    ), always explicit, never inferred:

    - ``"delta"`` (default): updates/deletes go through a real
      ``DeltaTable.forName(target).merge()`` with an UPDATE-only or
      DELETE-only branch (no insert clause -- an update-strategy UPDATE
      partition must never silently create new rows for keys that don't
      exist, and vice versa for DELETE); inserts append (or merge-insert
      if ``keys`` given, for idempotency, mirroring
      ``DeltaWriteStrategy``'s own choice for a keyed INSERT).
    - ``"adw"``: inserts go via plain JDBC append; updates/deletes are
      staged via JDBC overwrite into a staging table and then applied
      with a driver-side Oracle UPDATE-only or DELETE-only MERGE (no
      Spark-side MERGE exists for an external catalog -- see
      ``_adw_runtime.py``). Requires ``jdbc_options`` and
      ``adw_connection``.

    Raises rather than silently downgrading when an operation is
    unsatisfiable on the chosen target -- e.g. an ADW UPDATE/DELETE with
    no ``keys`` has no MERGE match condition to build, and this raises
    instead of falling back to a full-table overwrite (which would
    delete every row the mapping meant to preserve -- the worst failure
    class in this project).
    """
    if target_catalog_type not in ("delta", "adw"):
        raise ValueError(
            f"Unknown target_catalog_type {target_catalog_type!r} -- expected "
            f"'delta' or 'adw'. This is never inferred; pass it explicitly."
        )
    keys = list(keys)

    if target_catalog_type == "delta":
        _write_delta(result, target, keys, spark, update_columns)
    else:
        if jdbc_options is None or adw_connection is None:
            raise ValueError(
                "target_catalog_type='adw' requires both jdbc_options= and "
                "adw_connection= (see _adw_runtime.py / write_strategies.py's "
                "AdwWriteStrategy for how the wallet-staged connection and "
                "options are built)"
            )
        if not keys:
            raise ValueError(
                "target_catalog_type='adw' update/delete routing has no key "
                "columns to build a MERGE match condition from. Silently "
                "falling back to a full-table overwrite would delete every "
                "row this mapping meant to preserve -- add key columns "
                "before calling write_update_strategy()."
            )
        _write_adw(result, target, keys, jdbc_options, adw_connection, staging_table)

    _write_rejects(result.rejects, reject_sink, target_catalog_type, spark, jdbc_options)


def _write_rejects(
    rejects: Any,
    reject_sink: Union[str, Callable[[Any], None]],
    target_catalog_type: str,
    spark: Any,
    jdbc_options: Optional[dict],
) -> None:
    if callable(reject_sink):
        reject_sink(rejects)
        return
    # A table name: append, same write mode regardless of target type --
    # a reject table is always additive bookkeeping, never a merge target.
    if target_catalog_type == "adw":
        (
            rejects.write.format("jdbc")
            .options(**(jdbc_options or {}))
            .option("dbtable", reject_sink)
            .mode("append")
            .save()
        )
    else:
        # Several target instances of one table share its reject table, and
        # each one's rows arrive with that instance's column types (a
        # sequence BIGINT on the insert path, the lookup's DECIMAL on the
        # expire path): an append of a different type fails on Delta.
        _names.align_to_table(spark, rejects, reject_sink).write.mode("append").saveAsTable(reject_sink)


from . import _names  # noqa: E402  (module-level; see _names docstring)


def _one_per_key(df: Any, keys: list, last: bool) -> Any:
    """One row per key, the last (or first) in the DataFrame's row order."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    order = F.col("__infa_upd_ord").desc() if last else F.col("__infa_upd_ord").asc()
    w = Window.partitionBy(*keys).orderBy(order)
    return (
        df.withColumn("__infa_upd_ord", F.monotonically_increasing_id())
        .withColumn("__infa_upd_rn", F.row_number().over(w))
        .filter(F.col("__infa_upd_rn") == 1)
        .drop("__infa_upd_ord", "__infa_upd_rn")
    )


def _write_delta(result: UpdateStrategyResult, target: str, keys: list, spark: Any,
                 update_columns: Optional[Sequence[str]] = None) -> None:
    from delta.tables import DeltaTable

    if keys:
        match_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)

        if not spark.catalog.tableExists(target):
            # First run on a fresh workspace: there is nothing to update or
            # delete yet, and DeltaTable.forName() raises on a missing
            # table. Create the target empty from the insert partition's
            # schema so the three MERGEs below have a table to work on.
            result.inserts.limit(0).write.format("delta").saveAsTable(target)

        # DD_UPDATE sets the columns connected to the target instance and
        # nothing else, as Informatica's UPDATE does. UPDATE SET * also
        # overwrote every unconnected column with NULL (SCD2's expire path
        # connects only the key, the end date and the current flag).
        set_cols = [c for c in (update_columns or []) if c not in keys]
        # Several rows for one key in a run are applied in row order, as the
        # Integration Service applies them: the LAST update wins, the FIRST
        # insert lands (a second would violate the primary key). One MERGE
        # with two source rows matching one target row is refused by Delta.
        # ... and cast to the target's column types first (see align_to_table).
        updates = _names.align_to_table(spark, _one_per_key(result.updates, keys, last=True), target)
        deletes = _names.align_to_table(spark, _one_per_key(result.deletes, keys, last=True), target)
        inserts = _names.align_to_table(spark, _one_per_key(result.inserts, keys, last=False), target)
        merge = DeltaTable.forName(spark, _names.delta_name(spark, target)).alias("t").merge(
            updates.alias("s"), match_cond
        )
        if set_cols:
            merge.whenMatchedUpdate(set={c: f"s.`{c}`" for c in set_cols}).execute()
        else:
            merge.whenMatchedUpdateAll().execute()

        DeltaTable.forName(spark, _names.delta_name(spark, target)).alias("t").merge(
            deletes.alias("s"), match_cond
        ).whenMatchedDelete().execute()

        DeltaTable.forName(spark, _names.delta_name(spark, target)).alias("t").merge(
            inserts.alias("s"), match_cond
        ).whenNotMatchedInsertAll().execute()
    else:
        # No keys: UPDATE/DELETE can't be targeted at all without a match
        # condition -- raise rather than guessing a key or silently
        # skipping the partition.
        raise ValueError(
            "write_update_strategy(target_catalog_type='delta') requires "
            "`keys` to build the MERGE match condition for the UPDATE and "
            "DELETE partitions; INSERT alone could fall back to append, "
            "but this module never partially executes an update-strategy "
            "write -- add key columns."
        )


def _write_adw(
    result: UpdateStrategyResult,
    target: str,
    keys: list,
    jdbc_options: dict,
    adw_connection: Any,
    staging_table: Optional[str],
) -> None:
    stg = staging_table or f"{target}_UPD_STG"
    match_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    # Inserts: plain JDBC append, no staging needed (append is DML-only,
    # not excluded by the "external catalogs support no DDL" rule).
    (
        result.inserts.write.format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", target)
        .mode("append")
        .save()
    )

    try:
        # Updates: stage, then an UPDATE-only MERGE from the driver.
        _adw_runtime.stage_via_jdbc_overwrite(result.updates, stg, jdbc_options)
        update_cols = [c for c in result.updates.columns if c not in keys]
        set_clause = ", ".join(f't."{c}" = s."{c}"' for c in update_cols)
        update_sql = (
            f"MERGE INTO {target} t USING {stg} s ON ({match_cond}) "
            f"WHEN MATCHED THEN UPDATE SET {set_clause}"
        )
        _adw_runtime.run_statements_transactionally(adw_connection, [update_sql])
    finally:
        _adw_runtime.drop_staging_table_best_effort(adw_connection, stg)

    try:
        # Deletes: stage, then a DELETE-shaped MERGE (Oracle has no
        # standalone WHEN MATCHED THEN DELETE -- DELETE is only valid as
        # a sub-clause of UPDATE, so a no-op self-assignment on the first
        # key column makes every matched row eligible for the DELETE
        # WHERE, unconditionally).
        _adw_runtime.stage_via_jdbc_overwrite(result.deletes, stg, jdbc_options)
        noop_col = keys[0]
        delete_sql = (
            f"MERGE INTO {target} t USING {stg} s ON ({match_cond}) "
            f'WHEN MATCHED THEN UPDATE SET t."{noop_col}" = t."{noop_col}" '
            f"DELETE WHERE (1=1)"
        )
        _adw_runtime.run_statements_transactionally(adw_connection, [delete_sql])
    finally:
        _adw_runtime.drop_staging_table_best_effort(adw_connection, stg)
