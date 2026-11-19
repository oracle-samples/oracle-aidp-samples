"""SCD Type 2 effective-dating merge ( Class C).

Long, mechanical MERGE shapes where a wrong emission silently loses or
duplicates rows -- per the task brief, this is the function that gets
the heaviest test coverage in this package.

The algorithm, in both target flavors, is the same two steps:

1. **Expire**: for every key in ``source`` that already has a current
   row in ``target``, set that current row's ``effective_to`` to the
   incoming row's ``effective_from`` and flip its ``current_flag`` to
   the "not current" value.
2. **Insert**: append every row of ``source`` (freshly stamped
   ``current_flag`` = "current" and ``effective_to`` = ``high_date``) as
   the new current version.

Assumption carried over unchanged from ``write_strategies.py``'s
pre-existing ``_scd2_write`` (documented there, repeated here for the
same reason): ``source`` is assumed to already contain ONLY the rows
that need a new current version -- new keys plus keys whose attributes
changed. Filtering out unchanged keys (an attribute-level diff against
the current row) is a Lookup+comparison job upstream of this call, not
something ``scd2_merge`` re-derives -- re-deriving it here would require
re-reading the current target inside this function and duplicating logic
that mapping-specific Lookup/Router transformations already do. If that
assumption is ever violated (``source`` contains rows identical to what's
already current), the visible effect is a harmless but wasteful
expire-and-reinsert of an unchanged row, NOT data loss -- worth stating
plainly since it is the one assumption this function's correctness rests
on that it cannot verify for itself.

**Never assumes Delta.** Per and the task brief's hard
requirement: ``DeltaTable.forName()`` is Delta-only; an ADW external
catalog is overwrite-only via Spark JDBC, with no ``MERGE INTO`` at all
from Spark. ``target_catalog_type`` is always an explicit argument
(default ``"delta"``), dispatching to a completely different algorithm
for ``"adw"`` (stage via JDBC overwrite, then two driver-side Oracle SQL
statements via ``python-oracledb``) -- never inferred, and never
downgraded to a plain overwrite ( calls that out by name as the
worst failure class: it would delete every row the mapping meant to
preserve).

**Unverified against a live Spark/Delta or ADW runtime** -- no local
SparkSession or ADW connection in this development environment.
``pyspark``/``delta``/``oracledb`` are only ever touched inside
:func:`scd2_merge`'s body, never at module import time. Tests exercise
the emitted call/SQL structure via fakes, not real execution.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence

from . import _adw_runtime


class Scd2ConfigError(ValueError):
    """Raised for a caller-supplied configuration this function refuses
    to guess its way around (missing ADW plumbing, no keys, etc.) --
    distinct from a plain ``ValueError`` so callers can catch it
    specifically.
    """


@dataclass
class Scd2MergeResult:
    """What actually ran. Deliberately does NOT claim row counts this
    function cannot verify without forcing an extra action/query this
    module has no business forcing on every call (e.g. Delta's
    ``.merge().execute()`` does not return affected-row counts through
    the DataFrame API used here) -- reporting a fabricated count would
    be worse than reporting none.
    """

    target_catalog_type: str
    expired: bool
    inserted: bool


def scd2_merge(
    spark: Any,
    target: str,
    source: Any,
    keys: Sequence[str],
    effective_from: str,
    effective_to: str,
    current_flag: str,
    high_date: str = "9999-12-31",
    *,
    target_catalog_type: str = "delta",
    current_flag_values: tuple = ("Y", "N"),
    jdbc_options: Optional[dict] = None,
    adw_connection: Any = None,
    staging_table: Optional[str] = None,
) -> Scd2MergeResult:
    """Apply an SCD2 expire-then-insert merge of ``source`` into
    ``target``.

    Args:
        spark: the active SparkSession (used for the Delta path; ignored
            but accepted for the ADW path so both branches share one call
            signature).
        target: fully-qualified target table name.
        source: DataFrame of rows needing a new current version (see
            module docstring for the "already filtered to changed+new
            keys" assumption).
        keys: business/natural key column(s) -- NEVER a surrogate key
            ('s ``write_strategies.py`` documents the same
            preference for natural keys in a MERGE condition, since a
            ``row_number()``-derived surrogate is not stable across
            runs).
        effective_from: column name holding the new row's start-of-
            validity date. Used both to stamp the new row and, on the
            Delta/ADW expire step, as the value written into the
            OUTGOING row's ``effective_to`` (half-open interval: the old
            row's validity ends exactly where the new one's begins).
        effective_to: column name to stamp with ``high_date`` on the new
            row, and with the incoming ``effective_from`` on the expired
            row.
        current_flag: column name to stamp with
            ``current_flag_values[0]`` (new row) / ``[1]`` (expired row).
        high_date: the effective-to sentinel for a currently-valid row.
            ``HIGH_DATE`` is standard Informatica SCD2 practice, not a
            warehouse-specific convention.
        target_catalog_type: ``"delta"`` (default) or ``"adw"`` -- always
            explicit, never inferred.
        current_flag_values: ``(current, not_current)`` literal pair
            stamped into ``current_flag``. Defaults to ``("Y", "N")``,
            Informatica's own common convention -- **assumption, flagged
            rather than guessed**: some exports use ``(1, 0)`` instead;
            pass the pair explicitly when converting a mapping that uses
            the numeric convention.
        jdbc_options / adw_connection / staging_table: required for
            ``target_catalog_type="adw"`` -- see ``_adw_runtime.py``.

    Raises :class:`Scd2ConfigError` for any unsatisfiable combination
    (no keys, ADW without connection/options) rather than silently doing
    something narrower than what was asked for.
    """
    if target_catalog_type not in ("delta", "adw"):
        raise Scd2ConfigError(
            f"Unknown target_catalog_type {target_catalog_type!r} -- "
            f"expected 'delta' or 'adw'. Never inferred; pass it explicitly."
        )
    keys = list(keys)
    if not keys:
        raise Scd2ConfigError(
            "scd2_merge requires at least one business key in `keys` -- "
            "an SCD2 expire step with no match condition cannot be run "
            "without guessing, and guessing here risks expiring or "
            "preserving the wrong rows."
        )

    current_value, not_current_value = current_flag_values
    stamped_source = _stamp_new_current_rows(
        source, current_flag, current_value, effective_to, high_date
    )

    if target_catalog_type == "delta":
        return _scd2_merge_delta(
            spark, target, stamped_source, keys, effective_from, effective_to,
            current_flag, current_value, not_current_value,
        )
    if adw_connection is None or jdbc_options is None:
        raise Scd2ConfigError(
            "target_catalog_type='adw' requires both jdbc_options= and "
            "adw_connection= -- external catalogs have no Spark-side "
            "MERGE, so the expire step must run as real SQL from the "
            "driver via an already-open python-oracledb connection."
        )
    return _scd2_merge_adw(
        target, stamped_source, keys, effective_from, effective_to,
        current_flag, current_value, not_current_value,
        jdbc_options, adw_connection, staging_table,
    )


def _stamp_new_current_rows(
    source: Any, current_flag: str, current_value: Any, effective_to: str, high_date: str,
) -> Any:
    """Force the new-current-version columns onto every row of
    ``source`` -- this function owns stamping them rather than trusting
    the caller got it right, since a wrong stamp here silently produces
    a target with two "current" rows for the same key or a current row
    that never expires.
    """
    from pyspark.sql import functions as F

    # ``high_date`` arrives as a string ('9999-12-31'); the effective-to
    # column is a DATE or TIMESTAMP. Appending a string-typed column onto a
    # Delta table with a timestamp column fails schema enforcement, and an
    # Oracle INSERT ... SELECT from a VARCHAR staging column fails too.
    # Cast to the column's existing type when the source already carries
    # it, else to TIMESTAMP (Informatica's Date/Time).
    target_type = dict(getattr(source, "dtypes", []) or []).get(effective_to) or "timestamp"
    return source.withColumn(current_flag, F.lit(current_value)).withColumn(
        effective_to, F.lit(high_date).cast(target_type)
    )


def _scd2_merge_delta(
    spark: Any,
    target: str,
    stamped_source: Any,
    keys: list,
    effective_from: str,
    effective_to: str,
    current_flag: str,
    current_value: Any,
    not_current_value: Any,
) -> Scd2MergeResult:
    from delta.tables import DeltaTable
    from . import _names

    match_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    stamped_source = _names.align_to_table(spark, stamped_source, target)
    expire_source = stamped_source.select(*keys, effective_from).distinct()

    (
        DeltaTable.forName(spark, _names.delta_name(spark, target))
        .alias("t")
        .merge(expire_source.alias("s"), f"{match_cond} AND t.{current_flag} = '{current_value}'")
        .whenMatchedUpdate(
            set={
                effective_to: f"s.{effective_from}",
                current_flag: f"'{not_current_value}'",
            }
        )
        .execute()
    )
    stamped_source.write.mode("append").saveAsTable(target)
    return Scd2MergeResult(target_catalog_type="delta", expired=True, inserted=True)


def _scd2_merge_adw(
    target: str,
    stamped_source: Any,
    keys: list,
    effective_from: str,
    effective_to: str,
    current_flag: str,
    current_value: Any,
    not_current_value: Any,
    jdbc_options: dict,
    adw_connection: Any,
    staging_table: Optional[str],
) -> Scd2MergeResult:
    stg = staging_table or f"{target}_SCD2_STG"
    match_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    _adw_runtime.stage_via_jdbc_overwrite(stamped_source, stg, jdbc_options)
    try:
        expire_sql = (
            f"MERGE INTO {target} t USING {stg} s ON ({match_cond}) "
            f"WHEN MATCHED AND t.{current_flag} = '{current_value}' THEN UPDATE SET "
            f"t.{effective_to} = s.{effective_from}, t.{current_flag} = '{not_current_value}'"
        )
        cols = ", ".join(f'"{c}"' for c in stamped_source.columns)
        vals = ", ".join(f's."{c}"' for c in stamped_source.columns)
        insert_sql = f"INSERT INTO {target} ({cols}) SELECT {vals} FROM {stg} s"
        _adw_runtime.run_statements_transactionally(adw_connection, [expire_sql, insert_sql])
    finally:
        _adw_runtime.drop_staging_table_best_effort(adw_connection, stg)

    return Scd2MergeResult(target_catalog_type="adw", expired=True, inserted=True)
