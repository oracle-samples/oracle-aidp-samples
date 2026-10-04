"""Pluggable per-target-catalog write strategies for generated notebooks.

the generator used to emit ``DeltaTable.forName``
unconditionally in every write cell. That is a managed-Delta-only API and
fails at runtime the instant a mapping's target is registered against an
ADW (or any other external) catalog -- and this project's validation
scenario is exactly that: an ADW target with an SCD2 upsert. So that was
the very first thing it would hit.

``WriteStrategy`` is the seam that closes the gap. ``notebook_generator.py``
builds the table name and resolves key columns (it already owned that
logic before this module existed), then hands off to whichever strategy
matches the run's ``target_catalog_type`` to decide *how* to write --
Delta MERGE/append/overwrite, or the ADW-shaped JDBC-overwrite (+ driver-
side Oracle MERGE for upserts).

Two implementations:

- ``DeltaWriteStrategy`` -- the managed-Delta behavior. Moved verbatim out
  of ``NotebookGenerator._merge_write`` / ``_scd2_write``
  so the diff for that step is a pure relocation, reviewable on its own,
  before ``AdwWriteStrategy`` was added.

- ``AdwWriteStrategy`` -- external (ADW/ALH/ATP -- one Oracle 26ai family,
  same JDBC driver, same wallet flow) catalogs. Per the AIDP connector
  reference (``aidp-alh``): writes go via Spark JDBC
  (``df.write.format("jdbc")``), never ``saveAsTable()``. Overwrite only --
  external catalogs don't support DDL, so no ``MERGE INTO`` from Spark and
  no Delta merge. An upsert is therefore a different *algorithm*, not a
  different API call: stage the delta via a JDBC overwrite into a staging
  table, then run a real Oracle ``MERGE INTO`` from the driver via
  ``python-oracledb``, then drop the staging table. UNVERIFIED -- derived
  from the connector reference, never run against a live ADW; every code
  path this strategy emits carries that marker so it can't be mistaken for
  tested behavior.

Neither strategy infers ``target_catalog_type`` from a table name, a
connection string, or anything else -- it is always an explicit input
(``get_write_strategy`` below), defaulting to ``"delta"``. Guessing here is
exactly what this project forbids (a wrong guess against a live ADW
target is a silent-corruption bug, not a cosmetic one).

Neither strategy ever silently downgrades an unsatisfiable load strategy
(e.g. an UPSERT with no key columns) to an overwrite -- that would delete
rows the mapping meant to preserve, the worst failure class in this
project. It emits the same ``# REVIEW REQUIRED: ...`` marker
established for an unresolvable Joiner master side / underdetermined
transformation order, reused here rather than inventing a second
mechanism.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from .ddl_generator import _DDL_TYPE, _ddl_type, _sequence_fed_columns

from ..models import FieldMapping, LoadStrategy, Mapping, TargetDefinition, TransformationType

# Suffixes that mark a column as a generated/surrogate key (row_number()-style
# sequence, not a stable business identifier) -- non-deterministic across
# runs, so never preferred for a MERGE/upsert match condition when a natural
# key is available. Shared by both the Delta merge-key resolution and the
# ADW upsert's key resolution.
_SURROGATE_SUFFIXES = ("_SK", "_WID", "_SID", "_KEY")


def _has_scd2_pattern(mapping: Optional[Mapping]) -> bool:
    """Detect an SCD2 pattern from transformation names/types.

    Duplicated (not imported) from ``NotebookGenerator._has_scd2_pattern``
    on purpose: that method is also used by unrelated transformation-cell
    builders that stay in ``notebook_generator.py``, and importing it back
    from there would create an import cycle (``notebook_generator.py`` must
    import this module for ``WriteStrategy``/``DeltaWriteStrategy``/
    ``AdwWriteStrategy``). The two copies are ~10 lines and mechanical;
    keeping this module import-free of ``notebook_generator`` is worth it.
    """
    if mapping is None:
        return False
    for tx in mapping.transformations:
        name_upper = tx.name.upper()
        if "SCD2" in name_upper or "SCD_TYPE2" in name_upper:
            return True
        if tx.type == TransformationType.ROUTER:
            for group in (tx.router_groups or []):
                gname = (group.get("name", "") or "").upper()
                if "NEW" in gname or "CHANGED" in gname or "EXPIRE" in gname:
                    return True
    return False


def _find_field(target: Optional[TargetDefinition], candidates: list[str]) -> str:
    """Resolve a target field name from a list of candidates.

    Duplicated (not imported) from ``NotebookGenerator._find_field`` -- see
    ``_has_scd2_pattern`` above for why. Guards ``target is None`` (the
    unit-test / mapping-less call shape) by falling back to the first
    candidate, exactly the same fallback the original used for "declared
    fields exist but none of the candidates matched."
    """
    target_fields = set()
    if target is not None:
        for f in target.fields:
            if isinstance(f, FieldMapping):
                target_fields.add(f.target_field.upper())
            elif isinstance(f, dict):
                target_fields.add((f.get("target_field") or f.get("name", "")).upper())
    for candidate in candidates:
        if candidate.upper() in target_fields:
            return candidate
    return candidates[0]


class WriteStrategy(ABC):
    """Emits the lines that write ``df_var`` to ``table`` for one target,
    given the mapping's declared load strategy and its already-resolved key
    columns.

    Returned lines are pre-indented four spaces, ready to drop inside the
    caller's surrounding ``try:`` block (matches the convention the
    generator's write cells already used before this abstraction existed).

    ``mapping``/``target`` are the full parsed objects when the caller has
    them -- used for the natural-vs-surrogate-key and SCD2-field-name
    heuristics carried over from the pre-Task-25 code. Every implementation
    must degrade gracefully when either is ``None``: unit tests exercise
    ``emit_write`` in isolation with just a table name, a load strategy and
    a resolved key-column list, and the generator's multi-target write path
    (``_target_write_cell_for``) also calls in with ``mapping=None`` /
    ``target=None`` once it has already resolved ``key_columns`` itself.
    """

    @abstractmethod
    def emit_write(
        self,
        mapping: Optional[Mapping],
        target: Optional[TargetDefinition],
        table: str,
        load_strategy: LoadStrategy,
        key_columns: list[str],
        df_var: str = "df_final",
    ) -> list[str]:
        raise NotImplementedError


def _declared_numeric_ranges(target: Optional[TargetDefinition]) -> list[tuple]:
    """[(column, precision, scale)] for the target's declared numerics.

    Read off the export, which carries DATATYPE/PRECISION/SCALE for every
    target column. A string column cannot overflow in the same silent way.
    The integer family has a range too, but it is the Spark type's, not
    10^precision -- see :func:`_declared_integer_ranges`.
    """
    out = []
    for f in getattr(target, "fields", []) or []:
        name = (getattr(f, "target_field", "") or getattr(f, "name", "") or "").strip()
        if not name:
            continue
        base = str(getattr(f, "datatype", "") or "").upper().split("(")[0].strip()
        if base not in ("NUMBER", "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY"):
            continue
        prec = int(getattr(f, "precision", 0) or 0)
        scale = int(getattr(f, "scale", 0) or 0)
        if prec <= 0:
            continue
        out.append((name, prec, scale))
    return out


# The Spark integer types a declared base type can be created as.
_SPARK_INTEGER_TYPES = ("TINYINT", "SMALLINT", "INT", "BIGINT")


def _declared_integer_ranges(target: Optional[TargetDefinition]) -> list[tuple]:
    """[(column, declared_base, spark_type)] for the target's declared integers.

    These overflow exactly like the decimals do -- 3000000000 into an INT
    is the case :func:`_range_check_lines` was written for -- but their
    limit is the Spark type's range, not 10^precision: an Informatica
    ``integer`` carries PRECISION 10 and still holds only 2^31.

    The Spark type is looked up in the DDL generator's own table rather
    than restated here, so the bound checked is the bound of the column
    that DDL creates. That is why a declared TINYINT is checked against
    SMALLINT: the DDL widens it (SQL Server's TINYINT is 0..255, which
    Spark's signed byte cannot hold), and refusing 200 for a column that
    stores it would be a check firing on good data.
    """
    out = []
    for f in getattr(target, "fields", []) or []:
        name = (getattr(f, "target_field", "") or getattr(f, "name", "") or "").strip()
        if not name:
            continue
        base = str(getattr(f, "datatype", "") or "").upper().split("(")[0].strip()
        spark_type = _DDL_TYPE.get(base)
        if spark_type in _SPARK_INTEGER_TYPES:
            out.append((name, base, spark_type))
    return out


def _range_check_lines(df_var: str, target: Optional[TargetDefinition],
                       indent: str = "    ") -> list[str]:
    """Refuse to write a numeric value that does not fit its declared column.

    ``spark.sql.storeAssignmentPolicy`` is pinned ``LEGACY`` so that a
    migrated expression keeps Informatica's permissive evaluation. On the
    WRITE path that same setting makes Spark wrap instead of raising:
    inserting 3000000000 into an INT column stores -1294967296 -- a
    negative number where the source had a positive one, with no error
    anywhere (measured; see references/spark-4-behaviour-differences.md).

    Spark 4 would raise instead, and the pin suppresses that. Rather than
    leave the choice open, this closes it: the pin stays, because migrated
    expression logic depends on it, and the write becomes loud instead of
    lossy.

    It raises rather than wrapping or dropping. Informatica would have sent
    the offending row to the session's reject file and carried on, which is
    neither of Spark's two behaviours; routing rows to a reject table is
    modelled in ``infa_compat.update_strategy`` for the update-strategy
    path but is not available for every write, so the honest default here is
    to stop with the column, the count and the declared precision named,
    rather than to write a value the source never contained.

    The test is the cast itself, not a comparison against 10^(p-s). A
    ``try_cast`` to the column's type is NULL exactly where the value does
    not fit, so it agrees with the write by construction: it rounds HALF_UP
    to the scale as the write's cast does (99999.6 fits NUMBER(5,0) before
    rounding and not after -- the write would have stored NULL), and it is
    exact decimal arithmetic, where a power of ten computed in double
    refused 999999999999999999 for NUMBER(18,0) although it fits.
    """
    # Cast to the type the DDL creates, not the declaration: the DDL clamps
    # a precision above 38 and a scale above the precision, and DECIMAL(40,2)
    # or DECIMAL(3,5) in the check failed every write, valid data included.
    ranges = [(c, f"NUMBER({p},{s})", _ddl_type("NUMBER", p, s)[0])
              for c, p, s in _declared_numeric_ranges(target)]
    ranges += [(c, base if base == t else f"{base} (Spark {t})", t)
               for c, base, t in _declared_integer_ranges(target)]
    if not ranges:
        return []
    spec = ", ".join(f'("{c}", "{d}", "{t}")' for c, d, t in ranges)
    return [
        "",
        f"{indent}# Values that do not fit the target's DECLARED precision.",
        f"{indent}# storeAssignmentPolicy is LEGACY, so Spark WRAPS rather than",
        f"{indent}# raising on the write -- 3000000000 into an INT becomes",
        f"{indent}# -1294967296. This makes that impossible to happen quietly.",
        f"{indent}_DECLARED_RANGES = [{spec}]",
        f"{indent}_over = []",
        f"{indent}# Spark resolves column names case-insensitively, so a target",
        f"{indent}# declared Qty still checks the DataFrame's QTY.",
        f"{indent}_cols = {{_k.upper(): _k for _k in {df_var}.columns}}",
        f"{indent}for _c, _decl, _t in _DECLARED_RANGES:",
        f"{indent}    _c = _cols.get(_c.upper())",
        f"{indent}    if _c is None:",
        f"{indent}        continue",
        f"{indent}    _q = '`' + _c.replace('`', '``') + '`'",
        f"{indent}    # try_cast is NULL exactly where the value does not fit _t, after",
        f"{indent}    # the rounding the write's own cast applies. Integers go through",
        f"{indent}    # an exact DECIMAL first: try_cast refuses a fractional STRING",
        f"{indent}    # ('12.5') that the write's cast simply truncates, and anything",
        f"{indent}    # past DECIMAL(38,18) is past BIGINT too. A value that is not a",
        f"{indent}    # number at all is not an overflow and is left alone.",
        f"{indent}    _v = _q if _t.startswith('DECIMAL') else f'try_cast({{_q}} AS DECIMAL(38,18))'",
        f"{indent}    _n = {df_var}.filter(F.expr(",
        f"{indent}        f'try_cast({{_v}} AS {{_t}}) IS NULL AND try_cast({{_q}} AS DOUBLE) IS NOT NULL'",
        f"{indent}    )).count()",
        f"{indent}    if _n:",
        f"{indent}        _over.append((_c, _decl, _n))",
        f"{indent}if _over:",
        f"{indent}    _msg = '; '.join(",
        f"{indent}        f\"{{c}}: {{n}} row(s) exceed the declared {{d}}\"",
        f"{indent}        for c, d, n in _over",
        f"{indent}    )",
        f"{indent}    raise ValueError(",
        f"{indent}        f\"Refusing to write: {{_msg}}. storeAssignmentPolicy is \"",
        f"{indent}        f\"LEGACY, so Spark would wrap these silently rather than \"",
        f"{indent}        f\"raise. Informatica would have rejected the row(s) to the \"",
        f"{indent}        f\"session reject file. Widen the target column, or handle \"",
        f"{indent}        f\"the rows upstream.\"",
        f"{indent}    )",
    ]


class DeltaWriteStrategy(WriteStrategy):
    """Managed-Delta target: ``DeltaTable.forName()`` / ``.merge()`` for
    upsert-shaped strategies, plain ``saveAsTable()`` overwrite/append
    otherwise.

    This is the pre-Task-25 behavior, moved here verbatim (Step 2) from
    ``NotebookGenerator._target_write_cell`` / ``_merge_write`` /
    ``_scd2_write``. See the test suite for the before/after hash
    comparison proving every fixture's generated notebook is byte-identical
    to what the old inline code produced.
    """

    def emit_write(
        self,
        mapping: Optional[Mapping],
        target: Optional[TargetDefinition],
        table: str,
        load_strategy: LoadStrategy,
        key_columns: list[str],
        df_var: str = "df_final",
    ) -> list[str]:
        """Every strategy gets the declared-range check first.

        Prepended here rather than inside each branch so no write path can
        be added later that skips it -- see :func:`_range_check_lines` for
        why a wrapped value is worse than a stopped job.
        """
        return (_range_check_lines(df_var, target)
                + self._emit_write_inner(mapping, target, table,
                                         load_strategy, key_columns, df_var))

    def _emit_write_inner(
        self,
        mapping: Optional[Mapping],
        target: Optional[TargetDefinition],
        table: str,
        load_strategy: LoadStrategy,
        key_columns: list[str],
        df_var: str = "df_final",
    ) -> list[str]:
        if load_strategy in (LoadStrategy.UPSERT, LoadStrategy.SCD_TYPE1, LoadStrategy.SCD_TYPE2):
            return self._merge_write(mapping, target, table, load_strategy, key_columns, df_var)
        elif load_strategy == LoadStrategy.TRUNCATE_INSERT:
            return [f'    infa_compat.align_to_table(spark, {df_var}, "{table}")'
                    f'.write.mode("overwrite").saveAsTable("{table}")']
        elif load_strategy == LoadStrategy.INSERT:
            # If target has primary keys, use MERGE for idempotency
            if key_columns:
                return self._merge_write(mapping, target, table, load_strategy, key_columns, df_var)
            else:
                # The table's column types, not the DataFrame's: a Delta append
                # of a DECIMAL(11,0) into DECIMAL(10,0) is refused.
                return [f'    infa_compat.align_to_table(spark, {df_var}, "{table}")'
                        f'.write.mode("append").saveAsTable("{table}")']
        elif load_strategy == LoadStrategy.UPDATE:
            return self._merge_write(mapping, target, table, load_strategy, key_columns, df_var)
        elif load_strategy == LoadStrategy.DELETE:
            lines = [f"    # DELETE strategy — implement via Delta MERGE"]
            lines.extend(self._merge_write(mapping, target, table, load_strategy, key_columns, df_var))
            return lines
        else:
            # Default: use MERGE if keys exist (idempotent), overwrite otherwise
            if key_columns:
                return self._merge_write(mapping, target, table, load_strategy, key_columns, df_var)
            else:
                return [f'    infa_compat.align_to_table(spark, {df_var}, "{table}")'
                    f'.write.mode("overwrite").saveAsTable("{table}")']

    def _merge_write(
        self,
        mapping: Optional[Mapping],
        target: Optional[TargetDefinition],
        table: str,
        load_strategy: LoadStrategy,
        key_columns: list[str],
        df_var: str,
    ) -> list[str]:
        """Generate Delta Lake MERGE statement lines.

        Prefers natural keys (e.g., SOURCE_SYSTEM + SOURCE_ORDER_ID, CLAIM_ID)
        over surrogate keys (_SK, _WID) for the merge condition, since surrogate
        keys from row_number() are non-deterministic across runs.
        """
        all_keys = list(key_columns)

        # Prefer natural keys over surrogate keys for MERGE
        natural_keys = [k for k in all_keys
                        if not k.upper().endswith(_SURROGATE_SUFFIXES)]
        surrogate_keys = [k for k in all_keys
                          if k.upper().endswith(_SURROGATE_SUFFIXES)]

        # If we have natural keys, use them; otherwise search target fields
        if natural_keys:
            keys = natural_keys
        else:
            surr_upper = {s.upper() for s in surrogate_keys}
            if target is not None:
                for f in target.fields:
                    fname = f.target_field if isinstance(f, FieldMapping) else (f.get("target_field") or f.get("name", ""))
                    is_not_null = False
                    if isinstance(f, FieldMapping):
                        is_not_null = not f.nullable
                    elif isinstance(f, dict):
                        is_not_null = f.get("nullable", "NULL") == "NOT NULL"
                    fupper = fname.upper()
                    if fupper in surr_upper:
                        continue
                    if fupper.endswith("_ID"):
                        natural_keys.append(fname)
                    elif is_not_null and fupper.endswith(("_SYSTEM", "_SOURCE", "_CODE")) and fupper not in [k.upper() for k in natural_keys]:
                        natural_keys.insert(0, fname)
            if natural_keys:
                keys = natural_keys
            elif all_keys:
                keys = all_keys
            elif target is not None and target.fields:
                # Nothing declared as a key -- fall back to the target's
                # own first column rather than a fixed naming convention.
                first = target.fields[0]
                fname = (
                    first.target_field if isinstance(first, FieldMapping)
                    else (first.get("target_field") or first.get("name", ""))
                )
                keys = [fname] if fname else ["<key_column>"]
            else:
                keys = ["<key_column>"]

        is_scd2 = (
            (target is not None and target.scd_type == 2)
            or load_strategy == LoadStrategy.SCD_TYPE2
            or _has_scd2_pattern(mapping)
        )

        if is_scd2:
            return self._scd2_write(mapping, target, table, keys, df_var)

        match_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)

        # Columns a Sequence Generator feeds, that the match condition does
        # NOT use. These must survive a matched update unchanged.
        #
        # The merge condition above deliberately prefers a natural key,
        # because a surrogate key from a Sequence Generator is a fresh value
        # on every run. But whenMatchedUpdateAll() then writes that fresh
        # value over the stored one, so a row that already exists gets a NEW
        # surrogate key each run -- re-running a load silently re-keys the
        # dimension, and any table holding a foreign key to it now points at
        # the wrong row. Informatica does not do this: NEXTVAL is consumed
        # by the insert path, and an update leaves the key alone.
        #
        # Upper-cased, and compared upper-cased in the emitted code: the
        # DataFrame carries the target's own spelling (Surrogate_Key), and a
        # case-sensitive test let that column through to the update.
        frozen = sorted(
            c.upper() for c in _sequence_fed_columns(mapping, target)
            if c.upper() not in {k.upper() for k in keys}
        )

        lines = [
            f"    # NOTE: DeltaTable.forName() is Delta-only (see "
            f"references/aidp-runtime-constraints.md) -- if `{table}` is "
            f"registered against an external (ADW-backed) catalog rather "
            f"than a managed Delta table, this merge will fail and needs "
            f"a catalog-type branch here.",
            f'    if not spark.catalog.tableExists("{table}"):',
            f"        # First run on a fresh workspace: the migrator emits no target",
            f"        # DDL and DeltaTable.forName() raises DELTA_MISSING_DELTA_TABLE",
            f"        # (seen on AIDP 2026-09-24). Create the target empty from this",
            f"        # batch's schema so the MERGE has a table to merge into.",
            f"        # REVIEW: the column types then come from the DataFrame, not",
            f"        # from the Informatica target definition -- run your own DDL",
            f"        # first where precision/scale matter.",
            f'        logger.warning("Target {table} does not exist -- creating it from the DataFrame schema")',
            f'        {df_var}.limit(0).write.format("delta").saveAsTable("{table}")',
            f"    from delta.tables import DeltaTable",
            # The target's column types, not the DataFrame's: a Delta MERGE
            # refuses a mismatch (DELTA_FAILED_TO_MERGE_FIELDS).
            f'    {df_var} = infa_compat.align_to_table(spark, {df_var}, "{table}")',
            # A one-part name fails DeltaTable.forName on AIDP once the session
            # has USEd an AIDP catalog (see infa_compat._names).
            (f'    target_table = DeltaTable.forName(spark, "{table}")' if "." in table
             else f'    target_table = DeltaTable.forName(spark, infa_compat.delta_name(spark, "{table}"))'),
            f"    (",
            f"        target_table.alias(\"t\")",
            f"        .merge({df_var}.alias(\"s\"), \"{match_cond}\")",
        ]
        if load_strategy == LoadStrategy.DELETE:
            lines.append("        .whenMatchedDelete()")
        elif frozen:
            # Update every column EXCEPT the sequence-fed ones. The set is
            # built from the DataFrame at run time rather than listed here,
            # so a column added upstream is still updated.
            lines.insert(
                0,
                f"    _FROZEN_ON_UPDATE = {{{', '.join(repr(c) for c in frozen)}}}"
                f"  # Sequence Generator keys: set on insert, never re-written",
            )
            lines.append(
                f"        .whenMatchedUpdate(set={{"
                f"c: f's.{{c}}' for c in {df_var}.columns"
                f" if c.upper() not in _FROZEN_ON_UPDATE}})"
            )
            lines.append("        .whenNotMatchedInsertAll()")
        else:
            lines.append("        .whenMatchedUpdateAll()")
            lines.append("        .whenNotMatchedInsertAll()")
        lines.append("        .execute()")
        lines.append("    )")
        return lines

    def _scd2_write(
        self,
        mapping: Optional[Mapping],
        target: Optional[TargetDefinition],
        table: str,
        keys: list[str],
        df_var: str = "df_final",
    ) -> list[str]:
        """SCD Type 2 write: expire-old + insert-new, via infa_compat.

        Used to hand-roll the expire-then-insert MERGE inline (a second,
        untested copy of exactly what ``infa_compat.scd2_merge`` (
        Class C) already implements and tests). Calling the library
        instead of duplicating it also fixes two latent issues the inline
        version carried:

        - The inline version stamped the expired row's end-date with a
          freshly evaluated ``current_date()``, independent of whatever
          value upstream cells put in the new row's start-date column --
          two calls to "now" that are not guaranteed to agree.
          ``scd2_merge`` ties the expired row's ``effective_to`` directly
          to the incoming row's ``effective_from``, closing the interval
          with no gap or overlap.
        - The inline version's expire step only ran against a
          hardcoded ``df_changed_records`` variable, guarded by a bare
          ``except NameError: pass`` for when a mapping's Router branches
          happened to use a different name -- silently skipping the
          expire step in that case. ``scd2_merge`` derives the expire
          step from ``source`` (this call's own ``df_var``), so there is
          no separate variable name to get wrong.

        Surrogate-key generation (for target schemas with one) also moves
        off ``F.monotonically_increasing_id()``/``row_number()`` -- exactly
        the restart-unsafe, non-contiguous pattern calls out by
        name -- onto ``infa_compat.sequence()``.
        """
        # Find business key vs surrogate key
        biz_keys = [k for k in keys if not k.upper().endswith(("_SK", "_WID", "_KEY", "_SID"))]
        sk_col = next((k for k in keys if k.upper().endswith(("_SK", "_WID", "_KEY", "_SID"))), None)
        if not biz_keys and mapping is not None:
            # If no clear business key, look for _ID columns in target
            for t in mapping.targets:
                for f in t.fields:
                    name = f.target_field if isinstance(f, FieldMapping) else (f.get("target_field") or f.get("name", ""))
                    if name.upper().endswith("_ID") and name.upper() != (sk_col or "").upper():
                        biz_keys.append(name)
                        break
        if not biz_keys:
            biz_keys = keys[:1]

        # Detect SCD2 field names
        eff_start = _find_field(target, ["EFF_START_DATE", "EFFECTIVE_FROM_DATE", "START_DATE"])
        eff_end = _find_field(target, ["EFF_END_DATE", "EFFECTIVE_TO_DATE", "END_DATE"])
        current_flag = _find_field(target, ["CURRENT_FLAG", "CURRENT_FLG", "IS_CURRENT"])

        lines = [
            f"    # SCD Type 2: expire-old + insert-new -- call the tested",
            f"    # infa_compat.scd2_merge() (Class C) instead of",
            f"    # hand-rolling the MERGE; see engine/infa_compat/SUPPORTED_OPERATIONS.md.",
            f"    import infa_compat",
            f"    {df_var}.unpersist()  # Release stale cache before infa_compat re-reads it",
        ]

        if sk_col:
            lines.extend([
                f"",
                f"    # Surrogate key for new rows -- infa_compat.sequence() owns",
                f"    # restart-safety/cache-size; never F.monotonically_increasing_id().",
                f'    _sk_seq = infa_compat.sequence(',
                f'        spark, "{table}_{sk_col}_SEQ", target_catalog_type="delta",',
                f'        start=(spark.table("{table}").agg(F.coalesce(F.max("{sk_col}"), F.lit(0))).first()[0] or 0) + 1,',
                f"    )",
                f'    {df_var} = _sk_seq.assign({df_var}, "{sk_col}")',
            ])

        lines.extend([
            f"",
            f"    # Expire the prior current row (matched on business key",
            f"    # {', '.join(biz_keys)}, never the surrogate key) and insert",
            f"    # {df_var} as the new current version -- both steps in one call.",
            f"    # CURRENT_FLAG/EFF_START_DATE on {df_var} already set by",
            f"    # upstream transformation cells; scd2_merge re-stamps",
            f"    # CURRENT_FLAG + EFF_END_DATE itself so it can't be left stale.",
            f"    infa_compat.scd2_merge(",
            f"        spark,",
            f'        target="{table}",',
            f"        source={df_var},",
            f"        keys={biz_keys!r},",
            f'        effective_from="{eff_start}",',
            f'        effective_to="{eff_end}",',
            f'        current_flag="{current_flag}",',
            f'        target_catalog_type="delta",',
            f"    )",
        ])
        return lines


class AdwWriteStrategy(WriteStrategy):
    """External (ADW/ALH/ATP) catalog target.

    UNVERIFIED -- derived from the AIDP connector reference (``aidp-alh``),
    never run against a live ADW. Every branch below emits a comment
    marker saying so, so the generated notebook cannot be mistaken for
    tested behavior.

    Facts this rests on (see aidp-alh SKILL.md, not re-derived here):
    - Writes go via Spark JDBC (``df.write.format("jdbc")``), never
      ``saveAsTable()`` / the ``aidataplatform`` format handler.
    - Overwrite only -- external catalogs don't support DDL. No
      ``MERGE INTO`` from Spark, no Delta merge, no conditional append.
    - An upsert is therefore staged (JDBC overwrite into a staging table)
      then merged on the database via a real Oracle ``MERGE INTO``, run
      from the driver via ``python-oracledb``, then the staging table is
      dropped.
    - The wallet must be staged under ``/tmp`` (never ``/Workspace`` --
      FUSE-mounted, wrong UID, fails as ``Errno 107``), written with
      ``os.open(path, os.O_WRONLY | os.O_CREAT, 0o666)`` since
      ``os.chmod`` is a no-op on FUSE.
    - Instance/Resource Principal are blocked in AIDP notebooks (IMDS is
      unreachable, RP tokens aren't provided) -- auth is wallet, IAM
      DB-token, or API key, read at runtime from the environment /
      credential store. Never a literal credential in generated code.

    Plain (keyless) INSERT still uses JDBC ``append`` mode, mirroring the
    Delta strategy's choice for the same case -- ``append`` is a DML-only
    operation (no DDL), so it's not excluded by the "overwrite only /
    external catalogs don't support DDL" rule the way a Spark-side MERGE
    is. Every other unsatisfiable-without-a-key case (UPSERT, SCD1, SCD2,
    UPDATE, DELETE with no key columns) is a REVIEW REQUIRED item, never a
    silent downgrade to overwrite -- that would delete rows the mapping
    meant to preserve.
    """

    _MERGE_SHAPED = (
        LoadStrategy.UPSERT,
        LoadStrategy.SCD_TYPE1,
        LoadStrategy.SCD_TYPE2,
        LoadStrategy.UPDATE,
        LoadStrategy.DELETE,
    )

    def emit_write(
        self,
        mapping: Optional[Mapping],
        target: Optional[TargetDefinition],
        table: str,
        load_strategy: LoadStrategy,
        key_columns: list[str],
        df_var: str = "df_final",
    ) -> list[str]:
        if load_strategy in self._MERGE_SHAPED:
            if not key_columns:
                return self._review_required_no_keys(table, load_strategy)
            return self._stage_and_merge(table, key_columns, df_var, load_strategy)

        if load_strategy == LoadStrategy.INSERT:
            if key_columns:
                return self._stage_and_merge(table, key_columns, df_var, load_strategy)
            return self._jdbc_append(table, df_var)

        if load_strategy == LoadStrategy.TRUNCATE_INSERT:
            return self._jdbc_overwrite(table, df_var)

        # Unknown/default strategy: mirror DeltaWriteStrategy's own fallback
        # (MERGE if keys exist, overwrite otherwise) rather than inventing a
        # third behavior for a case the LoadStrategy enum doesn't actually
        # have room for today.
        if key_columns:
            return self._stage_and_merge(table, key_columns, df_var, load_strategy)
        return self._jdbc_overwrite(table, df_var)

    # ------------------------------------------------------------------
    # Shared setup: wallet staging + JDBC options. Emitted inline (not as a
    # library call) so the generated cell is fully self-contained and the
    # exact FUSE-safe staging fact (os.open, not os.chmod) is visible in
    # the notebook itself, not hidden behind a helper import.
    # ------------------------------------------------------------------

    @staticmethod
    def _type_fidelity_lines(df_var: str) -> list[str]:
        """A runtime check of Spark types against Oracle's published mapping.

        Oracle documents how each Spark SQL type lands in
        Oracle/Exadata, and several mappings lose data:

        - ``MapType`` has **no Oracle target type at all**
        - ``DoubleType`` becomes ``NUMBER(38,10)``, so a double's ~15-17
          significant digits are capped at ten decimal places
        - ``BooleanType`` becomes ``VARCHAR2``, i.e. text rather than a
          numeric flag
        - ``ArrayType``/``StructType`` are serialised to text and capped at
          4000 characters
        - ``DecimalType(p,s)`` is the only exact numeric path

        This is emitted rather than checked at generation time on purpose:
        the DataFrame's real types depend on every conversion upstream of
        the write, so only the notebook knows them. It matters because
        ``storeAssignmentPolicy=LEGACY`` is pinned for the write -- which
        makes Spark truncate rather than raise, so without this the loss is
        silent exactly where a reconciliation would care.
        """
        return [
            "",
            "    # Type fidelity against Oracle's published Spark->Oracle mapping.",
            "    # storeAssignmentPolicy is LEGACY, so Spark truncates instead of",
            "    # raising -- this check is what makes that visible.",
            "    _ORACLE_TYPE_RISK = {",
            '        "map": ("BLOCKED", "no Oracle target type exists for a map"),',
            '        "double": ("LOSSY", "becomes NUMBER(38,10): ~15-17 significant '
            'digits capped at 10 decimal places -- cast to decimal(p,s) instead"),',
            '        "float": ("APPROXIMATE", "becomes FLOAT(126), binary floating '
            'point on both sides"),',
            '        "boolean": ("LOSSY", "becomes VARCHAR2 text, not a numeric flag"),',
            '        "array": ("SERIALISED", "flattened to VARCHAR2(4000) text"),',
            '        "struct": ("SERIALISED", "flattened to VARCHAR2(4000) text"),',
            '        "interval": ("MANUAL", "convert to string yourself first"),',
            "    }",
            f"    _risks = []",
            f"    for _name, _dtype in {df_var}.dtypes:",
            '        _base = _dtype.split("<")[0].split("(")[0].strip().lower()',
            "        if _base in _ORACLE_TYPE_RISK:",
            "            _level, _why = _ORACLE_TYPE_RISK[_base]",
            '            _risks.append(f"  {_level}: {_name} ({_dtype}) -- {_why}")',
            "    if _risks:",
            '        _blocked = [r for r in _risks if r.strip().startswith("BLOCKED")]',
            '        _msg = "Spark->Oracle type fidelity:\\n" + "\\n".join(_risks)',
            "        if _blocked:",
            '            raise ValueError("Cannot write to Oracle -- " + _msg)',
            '        print("REVIEW REQUIRED: " + _msg)',
            "",
        ]

    @staticmethod
    def _setup_lines() -> list[str]:
        return [
            "    # ADW target -- UNVERIFIED (derived from the AIDP connector",
            "    # reference aidp-alh, never run against a live ADW). ALH/ADW/ATP",
            "    # are one Oracle 26ai family -- same JDBC driver, same wallet flow.",
            "    import os",
            "",
            "    # Wallet must be staged under /tmp -- NOT the notebook workspace",
            "    # filesystem, which is FUSE-mounted, so the JDBC driver process",
            "    # (a different UID) cannot read it reliably (fails as Errno 107).",
            "    # os.chmod is a no-op on FUSE, so use os.open with an explicit",
            "    # mode instead.",
            "    def _stage_adw_wallet_to_tmp() -> str:",
            "        import base64, zipfile, io",
            "        wallet_dir = os.environ.get(\"ADW_WALLET_STAGE_DIR\", \"/tmp/adw_wallet\")",
            "        os.makedirs(wallet_dir, exist_ok=True)",
            "        # Credentials/wallet content are read at runtime from the",
            "        # environment / credential store -- never a literal here.",
            "        wallet_bytes = base64.b64decode(os.environ[\"ADW_WALLET_B64\"])",
            "        with zipfile.ZipFile(io.BytesIO(wallet_bytes)) as zf:",
            "            for name in zf.namelist():",
            "                data = zf.read(name)",
            "                dest = os.path.join(wallet_dir, name)",
            "                fd = os.open(dest, os.O_WRONLY | os.O_CREAT, 0o666)",
            "                with os.fdopen(fd, \"wb\") as fh:",
            "                    fh.write(data)",
            "        return wallet_dir",
            "",
            "    _adw_tns_admin = _stage_adw_wallet_to_tmp()",
            "    _adw_url = (",
            "        f\"jdbc:oracle:thin:@{os.environ['ADW_TNS_SERVICE']}\"",
            "        f\"?TNS_ADMIN={_adw_tns_admin}\"",
            "    )",
            "    _adw_opts = {",
            "        \"url\": _adw_url,",
            "        \"driver\": \"oracle.jdbc.OracleDriver\",",
            "        \"user\": os.environ[\"ADW_USER\"],",
            "        \"password\": os.environ[\"ADW_PASSWORD\"],",
            "        \"oracle.net.tns_admin\": _adw_tns_admin,",
            "    }",
        ]

    def _jdbc_overwrite(self, table: str, df_var: str) -> list[str]:
        lines = self._setup_lines()
        lines.extend(self._type_fidelity_lines(df_var))
        lines.extend([
            "    (",
            f"        {df_var}.write",
            '        .format("jdbc")',
            "        .options(**_adw_opts)",
            f'        .option("dbtable", "{table}")',
            '        .mode("overwrite")',
            "        .save()",
            "    )",
        ])
        return lines

    def _jdbc_append(self, table: str, df_var: str) -> list[str]:
        lines = self._setup_lines()
        lines.extend(self._type_fidelity_lines(df_var))
        lines.extend([
            "    (",
            f"        {df_var}.write",
            '        .format("jdbc")',
            "        .options(**_adw_opts)",
            f'        .option("dbtable", "{table}")',
            '        .mode("append")',
            "        .save()",
            "    )",
        ])
        return lines

    def _review_required_no_keys(self, table: str, load_strategy: LoadStrategy) -> list[str]:
        """No key columns for an upsert-shaped strategy on an external
        catalog that only supports overwrite. Downgrading silently to a
        full-table overwrite would delete every row the mapping meant to
        preserve -- the worst failure class in this project -- so this is
        a REVIEW REQUIRED item instead: fail loudly, write nothing.
        """
        return [
            f"    # REVIEW REQUIRED: {load_strategy.value} into `{table}` on an",
            f"    # ADW (external-catalog) target has no key columns to build a",
            f"    # MERGE match condition from. External catalogs only support a",
            f"    # full-table JDBC overwrite from Spark -- silently downgrading",
            f"    # to that would delete every row this mapping meant to",
            f"    # preserve. Add key columns to the mapping's target definition",
            f"    # (or an explicit business key) before running this notebook.",
            f"    raise NotImplementedError(",
            f'        "REVIEW REQUIRED: no key columns for {load_strategy.value} '
            f'into `{table}` on an ADW target -- see comment above"',
            f"    )",
        ]

    def _stage_and_merge(
        self,
        table: str,
        key_columns: list[str],
        df_var: str,
        load_strategy: LoadStrategy,
    ) -> list[str]:
        """Stage the delta via a JDBC overwrite into a staging table, then
        run a real Oracle MERGE INTO from the driver via python-oracledb,
        then drop the staging table. This is a different *algorithm* from
        the Delta path's DeltaTable.merge -- external catalogs are
        overwrite-only from Spark, so the merge itself has to happen on
        the database, not in Spark.

        SCD1/SCD2/UPDATE/UPSERT all resolve to the same UPDATE+INSERT
        MERGE shape here (the upstream transformation cells are already
        responsible for stamping any SCD columns -- e.g. EFF_END_DATE /
        CURRENT_FLAG -- onto the incoming rows, exactly as the Delta path
        assumes for the same fields). DELETE resolves to a MERGE with a
        DELETE-only WHEN MATCHED clause. Both shapes are UNVERIFIED.
        """
        staging_table = f"{table}_ADW_STG"
        lines = self._setup_lines()
        lines.extend(self._type_fidelity_lines(df_var))
        lines.extend([
            "",
            "    # Stage the delta into a staging table -- overwrite only, no DDL",
            "    # needed (the staging table already exists / is truncated by",
            "    # this overwrite).",
            "    (",
            f"        {df_var}.write",
            '        .format("jdbc")',
            "        .options(**_adw_opts)",
            f'        .option("dbtable", "{staging_table}")',
            '        .mode("overwrite")',
            "        .save()",
            "    )",
            "",
            "    # Run the real MERGE on the database -- Spark JDBC has no",
            "    # merge/upsert writer, so this must happen from the driver.",
            "    import oracledb",
            f"    _key_columns = {key_columns!r}",
            "    _conn = oracledb.connect(",
            "        user=os.environ[\"ADW_USER\"],",
            "        password=os.environ[\"ADW_PASSWORD\"],",
            "        dsn=os.environ[\"ADW_TNS_SERVICE\"],",
            "        config_dir=_adw_tns_admin,",
            "        wallet_location=_adw_tns_admin,",
            "        wallet_password=os.environ.get(\"ADW_WALLET_PASSWORD\", os.environ[\"ADW_PASSWORD\"]),",
            "    )",
            "    try:",
            "        _cur = _conn.cursor()",
            f"        _cols = {df_var}.columns",
            "        _match_cond = \" AND \".join(f't.\"{c}\" = s.\"{c}\"' for c in _key_columns)",
        ])
        if load_strategy == LoadStrategy.DELETE:
            # Oracle's MERGE has no standalone "WHEN MATCHED THEN DELETE" --
            # DELETE is only valid as a sub-clause of UPDATE (it deletes the
            # just-updated row when the DELETE WHERE condition holds). Use a
            # no-op UPDATE SET (first key column to itself) so every matched
            # row gets deleted unconditionally.
            lines.extend([
                "        _noop_col = _key_columns[0]",
                '        _merge_sql = f"""',
                f"            MERGE INTO {table} t",
                f"            USING {staging_table} s",
                "            ON ({_match_cond})",
                '            WHEN MATCHED THEN UPDATE SET t."{_noop_col}" = t."{_noop_col}"',
                "            DELETE WHERE (1=1)",
                '        """',
            ])
        else:
            lines.extend([
                "        _update_cols = [c for c in _cols if c not in _key_columns]",
                "        _set_clause = \", \".join(f't.\"{c}\" = s.\"{c}\"' for c in _update_cols)",
                "        _insert_cols = \", \".join(f'\"{c}\"' for c in _cols)",
                "        _insert_vals = \", \".join(f's.\"{c}\"' for c in _cols)",
                '        _merge_sql = f"""',
                f"            MERGE INTO {table} t",
                f"            USING {staging_table} s",
                "            ON ({_match_cond})",
                "            WHEN MATCHED THEN UPDATE SET {_set_clause}",
                "            WHEN NOT MATCHED THEN INSERT ({_insert_cols}) VALUES ({_insert_vals})",
                '        """',
            ])
        lines.extend([
            "        _cur.execute(_merge_sql)",
            "        _conn.commit()",
            "    finally:",
            "        try:",
            f'            _cur.execute("DROP TABLE {staging_table} PURGE")',
            "            _conn.commit()",
            "        finally:",
            "            _cur.close()",
            "            _conn.close()",
        ])
        return lines


_STRATEGIES: dict[str, type[WriteStrategy]] = {
    "delta": DeltaWriteStrategy,
    "adw": AdwWriteStrategy,
}


def get_write_strategy(target_catalog_type: str) -> WriteStrategy:
    """Resolve a :class:`WriteStrategy` for an explicit catalog type.

    ``target_catalog_type`` is always an explicit input (config/CLI ->
    ``run_migration`` -> here), defaulting to ``"delta"`` -- it is never
    inferred from a table name, connection string, or anything else.
    """
    key = (target_catalog_type or "delta").strip().lower()
    try:
        return _STRATEGIES[key]()
    except KeyError:
        raise ValueError(
            f"Unknown target_catalog_type {target_catalog_type!r} -- expected "
            f"one of {sorted(_STRATEGIES)}. This is never inferred from a "
            f"table name or connection string; pass it explicitly."
        ) from None
