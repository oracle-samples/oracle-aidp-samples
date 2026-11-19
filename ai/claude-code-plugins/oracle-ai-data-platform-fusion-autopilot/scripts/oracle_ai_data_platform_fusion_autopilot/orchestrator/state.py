"""``fusion_autopilot_state`` Delta-table contract.

Schema and per-step write logic for the state table that records every
orchestrator step's outcome. Single source of truth for the table's DDL +
the canonical INSERT shape.

Two-layer failure semantics:
  - ``ensure_state_table`` is HARD — failure halts the run before any
    module dispatch (high-probability structural problems like catalog
    typo, missing schema, DDL grant misconfig).
  - ``write_state_row`` is wrapped by ``runtime._safe_write_state_row``,
    which logs WARN and continues on per-step failures (transient flakes
    shouldn't kill a long medallion run).

``read_last_watermark`` returns the most-recent ``status='success'``
row's ``last_watermark`` for a given ``(dataset_id, layer)`` pair, ordered
by ``last_run_at DESC, last_watermark DESC NULLS LAST LIMIT 1``. Read is
SOFT: an underlying Spark/metastore exception returns ``None`` and emits
a structured WARN log carrying the marker ``watermark_read_soft_failed``
that operators can grep / alert on. No-prior-row and NULL-watermark
cases still return ``None``.

Resume + multi-row semantics
============================

The table is **append-only**. A normal (non-resumed) run writes one row
per dataset_id. **A resumed run may write multiple rows per
(run_id, dataset_id)** — for example a `failed` row from the original
attempt + a `resumed_skipped` carry-forward + an eventual `success`
under the resume can all coexist under the same `run_id`. This is
intentional (preserves the CLAUDE.md medallion `_run_id` invariant —
gold/silver `<layer>_run_id` columns join 1:1 to a single logical
pipeline run, never split across resume attempts).

Consequences for consumers:
  * **Read from the ``fusion_autopilot_state_latest`` Delta VIEW**
    (created by ``ensure_state_table``). It projects one row per
    ``(run_id, dataset_id)`` via
    ``ROW_NUMBER() OVER (PARTITION BY run_id, dataset_id ORDER BY
    last_run_at DESC)`` and is the safe default.
  * Naïve queries against the raw table
    (``WHERE status='failed'``, ``COUNT(*)``, ``SUM(row_count)``)
    over-count failures and miscount datasets on resumed runs.
  * The operator-facing global "latest snapshot across all runs"
    query in ``commands/run.py`` partitions by ``dataset_id`` alone
    (no ``run_id``) — different aggregation, kept inline.

The table also carries ``plan_hash`` and ``plan_snapshot`` columns —
the resume drift gate's metadata. Legacy rows written by earlier
plugin builds land NULL on both; ``read_resumable_state`` rejects
them as non-resumable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
    OrchestratorConfigError,
)

#: Raised when the state-table's storage location holds files but no metastore
#: entry (orphaned) AND the location is NOT a valid Delta table, so it can't be
#: adopted in place. The recoverable case (valid Delta log) self-heals silently.
AIDPF_4021_STATE_LOCATION_ORPHANED = "AIDPF-4021"

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from .runtime import RunStep

logger = logging.getLogger(__name__)

# Stable marker string embedded in the WARN log when
# ``read_last_watermark`` soft-fails (Spark/metastore exception swallowed
# → ``None`` return). Part of the public audit-signal contract — operator
# alerting / log shippers key off this exact string. Do NOT rename
# without coordinating with the documented watermark-read regression tests.
WATERMARK_READ_SOFT_FAILED_MARKER: Literal[
    "watermark_read_soft_failed"
] = "watermark_read_soft_failed"


def _normalize_to_utc(ts: "datetime | None") -> "datetime | None":
    """Coerce a Spark-returned ``datetime`` to aware UTC.

    Spark ``TIMESTAMP`` columns deserialize to Python ``datetime`` with
    session-dependent ``tzinfo`` — naive on some builds, aware-with-
    session-zone on others. The state-table write path always persists
    ``datetime.now(timezone.utc) - WATERMARK_SAFETY_WINDOW``, so any
    naive value coming back is a session-precision artifact of the same
    aware-UTC value that was written. Normalizing at the read boundary
    keeps every downstream comparison (monotonicity check, arithmetic)
    from raising ``TypeError: can't compare offset-naive and offset-
    aware datetimes`` — which would surface as a spurious step failure
    and cascade-skip rather than the intended
    ``WatermarkMonotonicityError``.
    """
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# DDL — fusion_autopilot_state schema
# ---------------------------------------------------------------------------

_STATE_TABLE_NAME = "fusion_autopilot_state"
_STATE_LATEST_VIEW_NAME = "fusion_autopilot_state_latest"


def _state_table_path(paths: TablePaths) -> str:
    """3-part path: ``{catalog}.{bronze_schema}.fusion_autopilot_state``."""
    return paths.bronze(_STATE_TABLE_NAME)


def _state_latest_view_path(paths: TablePaths) -> str:
    """3-part path for the latest-per-(run_id, dataset_id) VIEW."""
    return paths.bronze(_STATE_LATEST_VIEW_NAME)


def _ddl(table_path: str) -> str:
    """Append-only. Each orchestrator step writes exactly one row.
    ``skip_reason`` is the structured discriminator for cascade /
    aborted / resume-skip rows (nullable for non-skipped /
    non-resumed-skipped rows).

    ``plan_hash`` + ``plan_snapshot`` carry the resume drift gate's
    metadata. Both nullable so the table accepts rows written by
    earlier plugin builds during the migration window;
    ``read_resumable_state`` rejects a run whose rows have NULL on
    either column as non-resumable.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {table_path} (
          run_id           STRING       NOT NULL,
          dataset_id       STRING       NOT NULL,
          layer            STRING       NOT NULL,
          mode             STRING       NOT NULL,
          last_watermark   TIMESTAMP             ,
          last_run_at      TIMESTAMP    NOT NULL,
          status           STRING       NOT NULL,
          row_count        BIGINT                ,
          error_message    STRING                ,
          skip_reason      STRING                ,
          duration_seconds DOUBLE       NOT NULL,
          plan_hash        STRING                ,
          plan_snapshot    STRING
        )
        USING DELTA
        PARTITIONED BY (layer)
    """


_FIX21_NEW_COLUMNS: tuple[tuple[str, str], ...] = (
    ("plan_hash", "STRING"),
    ("plan_snapshot", "STRING"),
)


def _existing_state_columns(spark: "SparkSession", table_path: str) -> set[str]:
    """Return the set of column names currently on ``table_path``.

    Uses ``DESCRIBE TABLE`` because it's supported by both vanilla
    Spark and Databricks Delta — ``spark.catalog.listColumns`` exists
    too but goes through a different code path that the orchestrator
    doesn't otherwise exercise.

    DESCRIBE TABLE emits column rows followed by metadata rows for
    partitioning / detailed-info; the metadata block opens with a
    ``#``-prefixed marker row (``# Partitioning``, etc.), so we
    short-circuit at the first row whose ``col_name`` starts with
    ``#``. Defensive against the row class lacking ``col_name``
    (some Spark forks return the field as ``column``); falls back to
    the first column of the row tuple.
    """
    rows = spark.sql(f"DESCRIBE TABLE {table_path}").collect()
    columns: set[str] = set()
    for row in rows:
        try:
            name = row["col_name"]
        except (KeyError, TypeError, IndexError):
            try:
                name = row[0]
            except (KeyError, TypeError, IndexError):
                continue
        if not name or name.startswith("#"):
            # End of column block / metadata marker row.
            break
        columns.add(name)
    return columns


def _ensure_target_table_exists(
    spark: "SparkSession",
    target: str,
    schema: "object",
) -> None:
    """Idempotent ``CREATE TABLE IF NOT EXISTS`` for a Delta target whose
    schema is supplied by the source DataFrame.

    Used pre-MERGE for fresh-tenant bronze writes. A bronze MERGE against a
    non-existent target raises ``TABLE_OR_VIEW_NOT_FOUND`` on the first
    incremental run when seed never created that bronze table. On existing
    tables this is a no-op.

    ``schema`` is a Spark ``StructType`` from the source DataFrame; columns
    are emitted in the SAME order, each with the ``simpleString()`` form
    of its data type.

    This helper only owns the simple "create if missing" path. The
    dropped-target silent-corruption guard lives in
    :func:`oracle_ai_data_platform_fusion_autopilot.orchestrator.preflight._preflight_incremental_cursors`
    as ``IncrementalTargetMissingError``. Run-level preflight blocks the
    unsafe operator-dropped-target sequence (target missing AND prior
    cursor non-null) BEFORE any node dispatch can reach this helper, so
    by the time this is called, either (a) the target legitimately
    doesn't exist on disk because the layer's prior cursor is NULL
    (fresh-tenant first-seed bronze) and we should create it, OR (b) the
    target exists and this is a no-op. The unsafe (c) "target missing
    AND cursor non-null" path is unreachable.
    """
    if spark.catalog.tableExists(target):
        return
    col_specs = ", ".join(
        f"{f.name} {f.dataType.simpleString()}" for f in schema.fields
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({col_specs}) USING DELTA")


def _build_add_columns_ddl(table_path: str, missing: list[tuple[str, str]]) -> str:
    """Schema-aware ``ALTER TABLE ... ADD COLUMNS (...)`` for the
    given ``(name, type)`` pairs.

    Spark SQL grammar does NOT accept ``IF NOT EXISTS`` inside the
    ``ADD COLUMNS`` clause — emitting that would fail the parser at
    every ``ensure_state_table`` call (i.e. every run + every
    resume). Caller (``ensure_state_table``) introspects the existing
    columns via :func:`_existing_state_columns` and invokes this
    helper only when at least one column is missing.
    """
    # Column names interpolate unquoted into the ADD COLUMNS clause (mutating a
    # customer table's schema). Validate each name against the central
    # identifier allowlist. dtype is Spark's own ``simpleString()`` (e.g.
    # ``decimal(28,8)``) and is NOT an identifier, so it is not allowlisted.
    from oracle_ai_data_platform_fusion_autopilot.config.paths import _validate_identifier

    for name, _dtype in missing:
        _validate_identifier("ADD COLUMNS column", name)
    col_specs = ", ".join(f"{name} {dtype}" for name, dtype in missing)
    return f"ALTER TABLE {table_path} ADD COLUMNS ({col_specs})"


# ---------------------------------------------------------------------------
# Schema evolution under MERGE
# ---------------------------------------------------------------------------


def _existing_state_columns_with_types(
    spark: "SparkSession", table_path: str
) -> list[tuple[str, str]]:
    """Return ``(col_name, data_type)`` pairs for ``table_path`` in
    physical order.

    Mirrors :func:`_existing_state_columns` but preserves the
    ``DESCRIBE TABLE`` result row order and pairs each name with its
    Spark-normalized ``simpleString()`` data type. Used by
    :func:`_ensure_target_schema_for_merge` to detect type-conflicts and to
    compute the target's physical column order for explicit-column-list MERGE
    generation.

    Same defensive row-shape fallback as the names-only variant:
    tries ``row["col_name"]`` first, falls back to ``row[0]`` /
    ``row[1]`` for Spark forks with a different row class. Stops at
    the first ``#``-prefixed metadata marker.
    """
    rows = spark.sql(f"DESCRIBE TABLE {table_path}").collect()
    out: list[tuple[str, str]] = []
    for row in rows:
        try:
            name = row["col_name"]
            dtype = row["data_type"]
        except (KeyError, TypeError, IndexError):
            try:
                name = row[0]
                dtype = row[1]
            except (KeyError, TypeError, IndexError):
                continue
        if not name or name.startswith("#"):
            break
        out.append((name, dtype))
    return out


@dataclass(frozen=True)
class SchemaReconcileResult:
    """Outcome of :func:`_ensure_target_schema_for_merge`.

    Result states:
      - **No drift**: ``source_only_columns`` and ``target_only_columns``
        both empty. Caller proceeds with V1 ``UPDATE SET *`` / ``INSERT *``
        MERGE shape.
      - **Source-wider only**: ``source_only_columns`` non-empty,
        ``target_only_columns`` empty. The helper has already emitted
        ``ALTER TABLE ... ADD COLUMNS (...)`` for the new columns;
        target now matches source. Caller proceeds with V1 shape.
      - **Target-wider only** (or **both**): ``target_only_columns``
        non-empty. Caller MUST switch to explicit-column-list MERGE
        syntax over ``common_columns + source_only_columns``;
        target-only columns are preserved by being omitted from the
        UPDATE/INSERT lists.
      - **Type-conflict**: NOT a returned result —
        :class:`SchemaEvolutionTypeConflictError` is raised before any
        ALTER or MERGE.

    "Cold start" (target missing) is NOT a returned result either —
    it's a precondition violation that raises explicit ``RuntimeError``
    (caller must invoke ``_ensure_target_table_exists`` first, or rely
    on incremental preflight for silver/gold). Explicit
    ``if not ...: raise`` survives ``python -O`` (asserts would not).
    """

    common_columns: tuple[str, ...]
    """Intersection of source and target column names, in target's
    physical order. Stable across runs for golden-snapshot SQL tests."""

    source_only_columns: tuple[str, ...]
    """Columns in source not previously in target. AFTER the helper
    runs, these have been ALTERed INTO the target (post-helper, the
    target's physical schema includes them)."""

    target_only_columns: tuple[str, ...]
    """Columns in target not in source. Non-empty → caller MUST use
    explicit-column-list MERGE syntax to avoid disturbing them."""


def _ensure_target_schema_for_merge(
    spark: "SparkSession",
    target: str,
    source_columns: "Iterable[str]",
    source_schema_struct: "StructType",
) -> SchemaReconcileResult:
    """Reconcile target Delta-table schema with source DataFrame's schema
    before an incremental MERGE.

    Resolves the four schema-drift modes documented in
    :class:`SchemaReconcileResult`. The helper composes three existing
    primitives (:func:`_existing_state_columns_with_types`,
    :func:`_build_add_columns_ddl`, and ``spark.catalog.tableExists``)
    plus :class:`SchemaEvolutionTypeConflictError` for the unresolvable
    type-conflict case.

    ``source_columns`` is materialized to a tuple at the top so the
    helper can iterate it multiple times (set construction +
    source-order preservation for ``source_only`` + per-column
    type lookup). Generators/``iter(...)`` inputs are safe.

    Precondition: ``spark.catalog.tableExists(target)`` is True.
    Callers satisfy this differently per layer:
      - Bronze (orchestrator dispatch): ``_do_bronze`` invokes
        :func:`_ensure_target_table_exists` immediately before.
      - Silver/gold (orchestrator dispatch):
        :func:`_preflight_incremental_cursors` certifies target
        existence at run-level before any node dispatch.
      - Silver/gold (standalone notebook use): caller's responsibility;
        helper raises ``RuntimeError`` with remediation pointer.

    Ordering invariant (Stage A5): type-conflict detection runs BEFORE
    any ALTER. A partial-ALTER on the target when an operator hasn't
    decided how to handle a type conflict would leave the schema half-
    reconciled.

    Args:
        spark: SparkSession with metastore access to ``target``.
        target: Fully-qualified Delta table identifier (e.g.
            ``fusion_catalog.bronze.ap_invoices``).
        source_columns: Iterable of source DataFrame column names.
            Tuple-coerced internally; generators are safe.
        source_schema_struct: PySpark ``StructType`` from the source
            DataFrame. Used to (a) detect type conflicts via
            ``simpleString()`` comparison, and (b) supply the data-type
            DDL for any ``source_only`` columns being ALTER-ed in.

    Returns:
        A :class:`SchemaReconcileResult` describing the post-helper
        state. Callers branch on ``target_only_columns`` to decide
        between V1 and explicit-column-list MERGE shapes.

    Raises:
        RuntimeError: ``target`` does not exist (precondition
            violation).
        SchemaEvolutionTypeConflictError: one or more columns shared
            between source and target have incompatible types.
            Auto-promotion is out of scope (operator must decide).
    """
    # Materialize up-front so generators/iterables aren't consumed by
    # the first iteration. This is load-bearing — the helper iterates
    # source_columns three times below.
    source_columns = tuple(source_columns)

    # Precondition — target must already exist. Explicit `if not
    # ...: raise RuntimeError(...)` rather than `assert` so Python -O
    # doesn't strip the check.
    if not spark.catalog.tableExists(target):
        raise RuntimeError(
            f"target {target!r} must exist before reconciliation; "
            f"call _ensure_target_table_exists first (bronze) or rely "
            f"on the IncrementalTargetMissingError preflight "
            f"(silver/gold under orchestrator dispatch)"
        )

    target_cols_with_types = _existing_state_columns_with_types(spark, target)
    target_names_ordered = [n for n, _ in target_cols_with_types]
    target_set = set(target_names_ordered)

    # Map source-side names to their simpleString types for both
    # conflict detection AND ALTER-DDL generation.
    source_types = {
        f.name: f.dataType.simpleString() for f in source_schema_struct.fields
    }

    # A5 — detect type conflicts BEFORE any ALTER.
    conflicts: list[tuple[str, str, str]] = []
    for name, target_type in target_cols_with_types:
        if name in source_types and source_types[name] != target_type:
            conflicts.append((name, source_types[name], target_type))
    if conflicts:
        # Lazy import to avoid circular: errors.py is imported by
        # __init__.py at package load.
        from .errors import SchemaEvolutionTypeConflictError

        raise SchemaEvolutionTypeConflictError(
            target=target, conflicts=conflicts
        )

    # Diffs over the materialized tuple.
    source_set = set(source_columns)
    source_only = tuple(c for c in source_columns if c not in target_set)
    target_only = tuple(c for c in target_names_ordered if c not in source_set)
    common = tuple(c for c in target_names_ordered if c in source_set)

    # Source-wider: emit ALTER TABLE ADD COLUMNS so target catches up
    # with source before the MERGE.
    if source_only:
        new_col_specs = [(c, source_types[c]) for c in source_only]
        alter_ddl = _build_add_columns_ddl(target, new_col_specs)
        spark.sql(alter_ddl)

    return SchemaReconcileResult(
        common_columns=common,
        source_only_columns=source_only,
        target_only_columns=target_only,
    )


def _latest_view_ddl(table_path: str, view_path: str) -> str:
    """Delta VIEW projecting one row per ``(run_id, dataset_id)`` —
    the latest terminal state by ``last_run_at``.

    Resumed runs append multiple rows per ``(run_id, dataset_id)`` (a
    failed attempt + resumed-skipped carry-forward + eventual success
    may all coexist under the same ``run_id``). This VIEW collapses
    that to a single-row-per-pair projection so consumers don't have
    to remember the window pattern. Dashboard / alert / ad-hoc queries
    SHOULD ``SELECT FROM fusion_autopilot_state_latest`` rather than the
    raw table.

    ``CREATE OR REPLACE VIEW`` is idempotent and updates the
    definition in place if the projected columns change in a future
    release.
    """
    return f"""
        CREATE OR REPLACE VIEW {view_path} AS
        WITH ranked AS (
          SELECT
            run_id, dataset_id, layer, mode, last_watermark,
            last_run_at, status, row_count, error_message,
            skip_reason, duration_seconds, plan_hash, plan_snapshot,
            ROW_NUMBER() OVER (
              PARTITION BY run_id, dataset_id
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {table_path}
        )
        SELECT
          run_id, dataset_id, layer, mode, last_watermark, last_run_at,
          status, row_count, error_message, skip_reason,
          duration_seconds, plan_hash, plan_snapshot
        FROM ranked
        WHERE rn = 1
    """


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ensure_schemas(spark: "SparkSession", paths: TablePaths) -> None:
    """Idempotent ``CREATE SCHEMA IF NOT EXISTS`` for the bronze / silver /
    gold medallion namespaces.

    Fresh-tenant prerequisite. The catalog (``aidp.catalog``) is expected to
    already exist — it's a one-time admin/provisioning step — but on a brand
    new catalog the medallion *schemas* do not. Nothing else in the bundle
    creates them: seed writes tables via ``saveAsTable`` /
    ``CREATE OR REPLACE TABLE`` and even the state table at
    ``{catalog}.{bronze_schema}.fusion_autopilot_state`` assumes the schema is
    already there. Without this, the very first write dies inside the
    metastore at ``HMSCatalog.createTable`` with
    ``InvalidObjectException: There is no database <catalog>.<schema>``.

    Runs BEFORE :func:`ensure_state_table`'s state-table DDL so the state
    write itself succeeds on a fresh catalog. ``IF NOT EXISTS`` makes every
    statement a no-op on established tenants, and duplicate schema names
    (e.g. a bundle that points bronze/silver/gold at one schema) collapse
    harmlessly.

    Identifier safety: ``paths.catalog`` and the three schema names were
    validated against the strict SQL-identifier regex at ``TablePaths``
    construction, so unquoted interpolation here cannot inject.
    """
    seen: set[str] = set()
    for schema in (paths.bronze_schema, paths.silver_schema, paths.gold_schema):
        if schema in seen:
            continue
        seen.add(schema)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {paths.catalog}.{schema}")


def _is_non_empty_location_error(exc: Exception) -> bool:
    """True when a CREATE TABLE failed because the target's storage location
    already holds files but the metastore has no entry (orphaned location).

    Delta surfaces this as ``[DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION]``;
    some engines word it ``... location ... is not empty``. Match both.
    """
    msg = str(exc)
    return (
        "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION" in msg
        or ("location" in msg.lower() and "not empty" in msg.lower())
    )


def _extract_location_from_error(msg: str) -> "str | None":
    """Pull the storage URI the non-empty-location error names. Delta embeds
    the exact path in the message; prefer it (authoritative for THIS failure).
    """
    import re

    m = re.search(r"[a-z0-9]+://[^\s'\")\]]+", msg)
    if not m:
        return None
    # Trim trailing sentence punctuation the regex may have swept up.
    return m.group(0).rstrip(".,;)")


def _derive_state_table_location(
    spark: "SparkSession", paths: TablePaths
) -> "str | None":
    """Fallback when the error message carries no path: derive the managed
    location from the bronze schema's ``db_location`` (DESCRIBE SCHEMA
    EXTENDED exposes it inside a Properties tuple) + the table name.
    """
    import re

    try:
        rows = spark.sql(
            f"DESCRIBE SCHEMA EXTENDED {paths.catalog}.{paths.bronze_schema}"
        ).collect()
    except Exception:
        return None
    loc = None
    for r in rows:
        for i in range(len(r)):
            v = str(r[i])
            m = re.search(r"db_location,([a-z0-9]+://[^),]+)", v)
            if m:
                loc = m.group(1).strip()
            elif loc is None and "://" in v and v.strip().endswith(".db"):
                loc = v.strip()
    if not loc:
        return None
    return f"{loc.rstrip('/')}/{_STATE_TABLE_NAME}"


def _location_is_delta(spark: "SparkSession", location: str) -> bool:
    """True when ``location`` is a valid Delta table (has a ``_delta_log``).
    Factored out so tests can stub it without a real Delta runtime.
    """
    try:
        from delta.tables import DeltaTable

        return bool(DeltaTable.isDeltaTable(spark, location))
    except Exception:
        return False


def _create_or_adopt_state_table(
    spark: "SparkSession", paths: TablePaths, table_path: str
) -> None:
    """Create the state table, self-healing the orphaned-location case.

    Fresh-tenant reality: a prior aborted run can leave Delta files at the
    state table's managed location while the metastore entry is gone. A plain
    ``CREATE TABLE IF NOT EXISTS`` then dies with
    ``DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION``. The state table is the
    orchestrator's own append-only run log (disposable bookkeeping), so when
    the orphaned location is a VALID Delta table we adopt it in place
    (``CREATE TABLE ... USING DELTA LOCATION``) — the seed proceeds, no data
    touched, no operator action needed.

    Only raises (``AIDPF-4021``) when the location holds files but is NOT a
    valid Delta table — an ambiguous case we will not auto-delete. The
    operator clears that one prefix and re-runs.
    """
    try:
        spark.sql(_ddl(table_path))
        return
    except Exception as exc:  # re-raised below unless it's the known case
        if not _is_non_empty_location_error(exc):
            raise
        location = (
            _extract_location_from_error(str(exc))
            or _derive_state_table_location(spark, paths)
        )
        if location and _location_is_delta(spark, location):
            logger.warning(
                "state table %s: orphaned-but-valid Delta location %s — "
                "adopting in place (fresh-tenant self-heal)",
                table_path, location,
            )
            spark.sql(
                f"CREATE TABLE {table_path} USING DELTA LOCATION '{location}'"
            )
            return
        raise OrchestratorConfigError(
            f"{AIDPF_4021_STATE_LOCATION_ORPHANED}: the state-table location "
            f"for {table_path} "
            f"({location or 'path not reported in the Delta error'}) holds "
            f"files but is not a valid Delta table and is not registered in "
            f"the catalog. The bundle will not auto-delete it. Inspect that "
            f"one object-storage prefix; if it is leftover garbage from an "
            f"aborted run, delete ONLY that prefix and re-run seed "
            f"({_STATE_TABLE_NAME} is disposable run-audit history, not "
            f"source data)."
        ) from exc


def ensure_state_table(spark: "SparkSession", paths: TablePaths) -> None:
    """HARD prerequisite — create the state table if missing AND probe
    writeability via INSERT/DELETE sentinel. Raises on any failure;
    the run loop's caller (``orchestrator.run``) lets this propagate
    uncaught so a structural problem halts BEFORE any module dispatch.

    First action is :func:`ensure_schemas` so the bronze/silver/gold
    namespaces exist before the state-table DDL — otherwise a fresh catalog
    fails at the state write with ``no database <catalog>.<bronze_schema>``.
    The CREATE then routes through :func:`_create_or_adopt_state_table`, which
    self-heals an orphaned-but-valid Delta location (a prior aborted run's
    remnant) by adopting it in place rather than dying with
    ``DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION``.

    Catches the high-probability failure modes:
      - wrong ``aidp.catalog`` (Spark AnalysisException at the DDL step)
      - missing ``aidp.bronzeSchema`` (same)
      - DDL/DML grant misconfig (PermissionError-shaped exception)
      - vault OCID unreachable for credential-bearing Delta paths

    After the CREATE, runs ``ALTER TABLE ADD COLUMNS IF NOT EXISTS``
    to ensure tables created by earlier plugin builds gain
    ``plan_hash`` + ``plan_snapshot``, then ``CREATE OR REPLACE VIEW
    fusion_autopilot_state_latest`` so consumers have a one-row-per-
    ``(run_id, dataset_id)`` projection without remembering the window
    pattern. Both are idempotent.

    The probe writes a sentinel row with ``run_id='__ensure_probe__'``
    and ``status='probe'`` (NOT one of the four canonical values) so
    consumer queries that filter by canonical status never see it; the
    sentinel is deleted immediately after insertion.
    """
    # Fresh-tenant guard — create bronze/silver/gold schemas before any
    # state-table DDL. The state table lives in the bronze schema, so on a
    # brand new catalog this MUST run first or the CREATE below fails with
    # "no database <catalog>.<bronze_schema>". Idempotent on existing tenants.
    ensure_schemas(spark, paths)
    table_path = _state_table_path(paths)
    view_path = _state_latest_view_path(paths)
    # Create the state table, self-healing an orphaned-but-valid Delta
    # location (fresh-tenant remnant of a prior aborted run) by adopting it
    # in place. Raises AIDPF-4021 only when the location is non-empty garbage.
    _create_or_adopt_state_table(spark, paths, table_path)
    # Schema-aware additive migration. `CREATE TABLE IF NOT EXISTS`
    # is a no-op when the table exists, so the new columns need an
    # `ALTER TABLE` migration to materialize on tables created by
    # earlier plugin builds. We can't write `ADD COLUMNS IF NOT
    # EXISTS (...)` — Spark SQL grammar rejects that — so introspect
    # the existing columns and ADD only the ones that are missing.
    # ALTER is skipped entirely when both are already present, which is the
    # common case for tables created with the current schema.
    existing_cols = _existing_state_columns(spark, table_path)
    missing = [
        (name, dtype) for name, dtype in _FIX21_NEW_COLUMNS
        if name not in existing_cols
    ]
    if missing:
        spark.sql(_build_add_columns_ddl(table_path, missing))
    # Idempotent view definition — CREATE OR REPLACE updates the
    # projection in place if the columns evolve.
    spark.sql(_latest_view_ddl(table_path, view_path))
    # Writeability probe — INSERT + DELETE sentinel.
    # Live-evidence fix (2026-05-17): every VALUES literal needs an explicit
    # CAST. Delta's strict type-merging refuses to coerce DECIMAL(2,1) → DOUBLE
    # on the `0.0` literal, and NULL needs a typed CAST for the nullable
    # columns. Unit tests with fake-Spark didn't catch this because they
    # accept any value; only the real Delta writer enforces the schema.
    # `plan_hash` + `plan_snapshot` are nullable in the schema, so the
    # probe writes NULL for both — keeps the sentinel row distinguishable
    # from real run rows (which carry non-NULL values when the
    # orchestrator stamps them).
    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds,
           plan_hash, plan_snapshot)
        VALUES
          ('__ensure_probe__', '__probe__', 'bronze', 'seed',
           CAST(NULL AS TIMESTAMP), current_timestamp(), 'probe',
           CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS STRING),
           CAST(0.0 AS DOUBLE),
           CAST(NULL AS STRING), CAST(NULL AS STRING))
        """
    )
    spark.sql(
        f"DELETE FROM {table_path} WHERE run_id = '__ensure_probe__'"
    )


def write_state_row(
    spark: "SparkSession", paths: TablePaths, step: "RunStep"
) -> None:
    """Insert one row into ``fusion_autopilot_state``. Raw write — failures
    propagate. The orchestrator's ``_safe_write_state_row`` wrapper in
    ``runtime.py`` catches + logs the WARN per the soft-write contract.
    """
    table_path = _state_table_path(paths)
    # Build the INSERT via parameterized literals. Spark SQL doesn't
    # have native prepared statements for CREATE/INSERT, but quoting
    # via repr() + ``f""""`` is safe for the strict-SQL-identifier
    # values we accept (TablePaths._validate_identifier enforces).
    # The user-controlled values (error_message especially) need
    # escaping; we use a single-quote-doubled escape consistent with
    # Delta's SQL parser.

    # Every NULL value needs a typed CAST because Delta's schema-merge refuses
    # bare NULL -> BIGINT/STRING promotion. Same approach as the state-table
    # writeability probe.
    def _q(s: str | None) -> str:
        """Quote a string literal — None → typed CAST(NULL AS STRING)."""
        if s is None:
            return "CAST(NULL AS STRING)"
        escaped = s.replace("'", "''")
        return f"'{escaped}'"

    def _ts(t: "datetime | None") -> str:
        if t is None:
            return "CAST(NULL AS TIMESTAMP)"
        return f"TIMESTAMP '{t.isoformat(sep=' ')}'"

    def _bigint(n: int | None) -> str:
        if n is None:
            return "CAST(NULL AS BIGINT)"
        return f"CAST({n} AS BIGINT)"

    def _double(d: float) -> str:
        # Bare `0.0` is DECIMAL(2,1); needs explicit DOUBLE cast for Delta.
        return f"CAST({d} AS DOUBLE)"

    # Persist ``step.last_watermark`` as the output cursor. For bronze it is
    # captured pre-extract as ``extract_started_at - WATERMARK_SAFETY_WINDOW``;
    # empty deltas preserve the prior cursor. ``watermark_used`` is the input
    # cursor and stays in-memory only on ``RunStep`` for debug/logs/repr; no
    # state column carries it.
    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds,
           plan_hash, plan_snapshot)
        VALUES
          ({_q(step.run_id)},
           {_q(step.dataset_id)},
           {_q(step.layer)},
           {_q(step.mode)},
           {_ts(step.last_watermark)},
           current_timestamp(),
           {_q(step.status)},
           {_bigint(step.row_count)},
           {_q(step.error_message)},
           {_q(step.skip_reason)},
           {_double(step.duration_seconds)},
           {_q(step.plan_hash)},
           {_q(step.plan_snapshot)})
        """
    )


def write_fingerprint_skip_row(
    spark: "SparkSession",
    paths: TablePaths,
    *,
    run_id: str,
    prior_fingerprint: str,
    current_fingerprint: str,
) -> None:
    """Append a ``--force-fingerprint-skip`` audit row to
    ``fusion_autopilot_state``.

    Dedicated single-purpose helper because
    :func:`write_state_row` takes a :class:`RunStep` whose
    ``mode``/``status`` Literal types reject ``"fingerprint_skip"``
    / ``"warn"``. The v1 DDL at ``state.py:135-152`` accepts any
    string for those columns; this helper writes a raw INSERT
    bypassing the Python-side Literal narrowing.

    Row carries:

    * ``dataset_id = "__fingerprint_skip__"`` — reserved ``__*__`` sentinel so
      the resume reader excludes it from every derived node set (succeeded /
      scope / legacy-detection); it never collides with a real node id.
    * ``layer = "bronze"`` (the fingerprint is whole-bronze-schema).
    * ``mode = "fingerprint_skip"`` — distinguishes from
      ``seed``/``incremental`` rows in audit queries.
    * ``last_watermark = NULL`` — there's no watermark for this row.
    * ``last_run_at = current_timestamp()`` — when the bypass fired.
    * ``status = "success"`` — the existing schema accepts any
      string at the DB level; "success" keeps the row from being
      mistaken for a failure. The audit signal lives in
      ``skip_reason``.
    * ``skip_reason`` — encodes the bypass + prior/current
      fingerprints (truncated to 24 chars each + ``...``).
    * ``duration_seconds = 0.0``.

    Args:
        spark: active Spark session.
        paths: bundle's ``TablePaths``.
        run_id: Same run_id the rest of the run would use; the drift
            artifact, skip audit row, and
            RunSummary all share one id).
        prior_fingerprint: pinned profile fingerprint.
        current_fingerprint: live bronze fingerprint computed at
            preflight.
    """
    table_path = _state_table_path(paths)

    def _q(s: str) -> str:
        escaped = s.replace("'", "''")
        return f"'{escaped}'"

    # Truncated fingerprints — 24 chars is enough to disambiguate
    # in audit queries without bloating the column. ``current`` may be
    # None when --force-fingerprint-skip bypassed a failed probe (e.g. a
    # bronze table unreachable); render it as "unprobed" rather than crash.
    def _short(fp: "str | None") -> str:
        return f"{fp[:24]}..." if fp else "unprobed"

    skip_reason = (
        f"--force-fingerprint-skip; "
        f"prior={_short(prior_fingerprint)} "
        f"current={_short(current_fingerprint)}"
    )

    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds,
           plan_hash, plan_snapshot)
        VALUES
          ({_q(run_id)},
           '__fingerprint_skip__',
           'bronze',
           'fingerprint_skip',
           CAST(NULL AS TIMESTAMP),
           current_timestamp(),
           'success',
           CAST(0 AS BIGINT),
           CAST(NULL AS STRING),
           {_q(skip_reason)},
           CAST(0.0 AS DOUBLE),
           CAST(NULL AS STRING),
           CAST(NULL AS STRING))
        """
    )


def write_plan_hash_repin_row(
    spark: "SparkSession",
    paths: TablePaths,
    *,
    run_id: str,
    dataset_id: str,
    layer: str,
    expected_plan_hash: str,
    prior_plan_hash: str,
) -> None:
    """Append a ``--repin-plan-hash`` audit row to ``fusion_autopilot_state``.

    Mirrors :func:`write_fingerprint_skip_row`: a raw INSERT that bypasses
    the :class:`RunStep` ``mode``/``status`` Literal narrowing (which would
    reject ``"plan_hash_repin"``). Written when the operator passes the
    hidden ``--repin-plan-hash`` break-glass flag and the AIDPF-4040
    continuity gate would otherwise have blocked an incremental — the
    operator asserts the plan edit was deliberate, so the gate is bypassed
    and this row records the bypass for the SOX trail.

    The node's own success row (written immediately after, by the normal
    execution path) pins the NEW ``expected_plan_hash``, so subsequent
    incrementals compare clean against it.

    Row carries:

    * ``dataset_id`` — the node the operator repinned (NOT a sentinel, so
      audit queries attribute the bypass to a specific node).
    * ``layer`` — the node's layer.
    * ``mode = "plan_hash_repin"`` — distinguishes from ``seed`` /
      ``incremental`` rows in audit queries.
    * ``status = "success"`` — DB accepts any string; the audit signal
      lives in ``skip_reason``. "success" keeps the row from being
      mistaken for a failure.
    * ``skip_reason`` — encodes the bypass + truncated prior/expected
      plan hashes.

    Args:
        spark: active Spark session.
        paths: bundle's ``TablePaths``.
        run_id: the run_id this bypass fired under.
        dataset_id: the node whose plan-hash was repinned.
        layer: the node's layer.
        expected_plan_hash: the freshly-computed (new) plan-hash now pinned.
        prior_plan_hash: the prior successful row's plan-hash that diverged.
    """
    table_path = _state_table_path(paths)

    def _q(s: str) -> str:
        escaped = s.replace("'", "''")
        return f"'{escaped}'"

    def _short(h: "str | None") -> str:
        return f"{h[:24]}..." if h else "unknown"

    skip_reason = (
        f"--repin-plan-hash; "
        f"prior={_short(prior_plan_hash)} "
        f"expected={_short(expected_plan_hash)}"
    )

    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds,
           plan_hash, plan_snapshot)
        VALUES
          ({_q(run_id)},
           {_q(dataset_id)},
           {_q(layer)},
           'plan_hash_repin',
           CAST(NULL AS TIMESTAMP),
           current_timestamp(),
           'success',
           CAST(0 AS BIGINT),
           CAST(NULL AS STRING),
           {_q(skip_reason)},
           CAST(0.0 AS DOUBLE),
           {_q(expected_plan_hash)},
           CAST(NULL AS STRING))
        """
    )


def _build_last_watermark_query(
    paths: TablePaths,
    dataset_id: str,
    layer: str,
) -> "tuple[str, str]":
    """Construct the ``SELECT last_watermark FROM fusion_autopilot_state``
    query shared by :func:`read_last_watermark` (soft-fail) and
    :func:`read_last_watermark_strict` (strict-fail).

    Returns ``(table_path, query)`` so callers can include the path in
    failure messages without re-computing it. User-controlled
    identifiers are escaped via the same single-quote-doubling pattern
    used elsewhere in this module (see ``write_state_row``'s local
    ``_q``).

    Factored out so the two read variants can't drift — both must
    issue byte-identical SQL so their behavior differs only in the
    failure-mode contract, not in what gets returned on success.
    """
    def _q(s: str | None) -> str:
        if s is None:
            return "CAST(NULL AS STRING)"
        escaped = s.replace("'", "''")
        return f"'{escaped}'"

    table_path = _state_table_path(paths)
    query = f"""
        SELECT last_watermark
        FROM {table_path}
        WHERE dataset_id = {_q(dataset_id)}
          AND layer = {_q(layer)}
          AND status = 'success'
        ORDER BY last_run_at DESC, last_watermark DESC NULLS LAST
        LIMIT 1
    """
    return table_path, query


def _extract_watermark_from_rows(rows: "list") -> "datetime | None":
    """Convert the rows returned by the last-watermark query into an
    aware UTC ``datetime`` or ``None``. Shared by both read variants.

    Returns ``None`` when no rows came back (no ``status='success'``
    row for the pair) OR when the row's ``last_watermark`` field is
    SQL NULL. Both cases are legitimate ("no prior cursor") and the
    callers treat them identically.
    """
    if not rows:
        return None
    return _normalize_to_utc(rows[0]["last_watermark"])


def read_last_watermark(
    spark: "SparkSession",
    paths: TablePaths,
    dataset_id: str,
    layer: Literal["bronze", "silver", "gold"] = "bronze",
) -> "datetime | None":
    """Return the most-recent ``status='success'`` row's
    ``last_watermark`` for ``(dataset_id, layer)``, as an aware UTC
    ``datetime``. Returns ``None`` when:

    - no ``status='success'`` row exists for the pair;
    - the most-recent success row has ``last_watermark IS NULL``
      (e.g. a true-first-empty bronze run, or a successful replace-style
      silver/gold row that does not advance a watermark);
    - the underlying Spark/metastore read raises (soft-fail: log
      WARN + return ``None``; the exception is swallowed).

    Ordering — ``last_run_at DESC, last_watermark DESC NULLS LAST
    LIMIT 1``. ``last_run_at`` is the primary key for persisted state
    rows (see ``state.py:194`` and ``read_resumable_state``);
    the secondary key breaks ties deterministically by preferring the
    row that recorded more progress, aligning with the monotonicity
    invariant. There is no ``finished_at`` column on the schema —
    do not order by it.

    Read is issued via ``spark.sql(...)`` (matching
    ``read_resumable_state`` so the same in-memory
    ``_FakeSpark`` test harness works); user-controlled identifiers
    (``dataset_id``, ``layer``) are escaped via the helper inside
    :func:`_build_last_watermark_query` to defeat apostrophe-bearing
    strings without falling through to the DataFrame API (which would
    require pyspark at import time and break the unit-test
    environment).

    Failure semantics: a Spark/SQL exception logs a structured WARN
    carrying ``dataset_id``, ``layer``, ``repr(exc)`` and the stable
    marker ``"watermark_read_soft_failed"`` (see
    :data:`WATERMARK_READ_SOFT_FAILED_MARKER`), then returns ``None``.
    Operators monitor for the marker to detect the documented
    empty-delta + read-failure regression (LIMITS.md F6).

    See also — :func:`read_last_watermark_strict`. The strict-fail variant is
    for preflight gates where a transient metastore failure must NOT be
    confused with "no prior cursor" because the soft return value ``None`` is
    ambiguous between the two. Dispatch-path callers keep using this soft
    variant; its swallow-and-continue contract is load-bearing for transient
    flake tolerance during a long medallion run.
    """
    _table_path, query = _build_last_watermark_query(paths, dataset_id, layer)
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:
        logger.warning(
            "%s dataset_id=%r layer=%r exc=%r",
            WATERMARK_READ_SOFT_FAILED_MARKER,
            dataset_id,
            layer,
            exc,
        )
        return None

    return _extract_watermark_from_rows(rows)


def read_last_watermark_strict(
    spark: "SparkSession",
    paths: TablePaths,
    dataset_id: str,
    layer: Literal["bronze", "silver", "gold"] = "bronze",
) -> "datetime | None":
    """Strict-fail variant of :func:`read_last_watermark` for preflight gates.

    Returns the same value as the soft variant on success: a
    ``datetime`` for an existing ``status='success'`` row with a
    non-NULL ``last_watermark``, or ``None`` for "no row found" /
    "row exists but ``last_watermark`` IS NULL". Differs only in
    failure semantics — on a Spark/metastore exception this raises
    :class:`StateReadFailedError` (chaining the underlying exception
    via ``raise ... from cause``) instead of logging WARN and
    returning ``None``.

    **Caller contract** — preflight gates only. The soft variant's
    swallow-and-continue semantics are load-bearing for the dispatch
    path's transient-flake tolerance (see module docstring lines
    7-13). Do NOT call this from per-step dispatch code; a transient
    metastore hiccup would cascade-skip downstream nodes for the cost
    of one minor flake.

    The query is byte-identical to :func:`read_last_watermark` — both
    delegate to :func:`_build_last_watermark_query`, so their
    return-value contracts can't drift. Only the exception handling
    differs.
    """
    # Local import to avoid a module-import cycle (errors → state):
    # state.py is imported by orchestrator/__init__.py BEFORE errors.py
    # in some bootstrap paths.
    from .errors import StateReadFailedError

    table_path, query = _build_last_watermark_query(paths, dataset_id, layer)
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:
        raise StateReadFailedError(
            dataset_id=dataset_id,
            layer=layer,
            table_path=table_path,
            cause=exc,
        ) from exc

    return _extract_watermark_from_rows(rows)


# ---------------------------------------------------------------------------
# Resume-time state read
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResumeContext:
    """Snapshot of ``fusion_autopilot_state`` for a single ``run_id`` at
    resume-time. Returned by :func:`read_resumable_state` and consumed
    by the orchestrator's resume flow.

    ``succeeded``: set of ``dataset_id`` whose latest terminal status
    under this ``run_id`` is ``'success'`` or ``'resumed_skipped'``.
    Both count as "done, don't dispatch again" — the second case is a
    carry-forward from a prior resume of this same run_id, and a
    re-resume must treat it as already done (otherwise the contract
    breaks on a re-resume of an already-resumed run).

    ``plan_hash`` / ``plan_snapshot``: the single non-NULL values
    observed across the run's rows. ``read_resumable_state`` rejects
    runs whose rows are missing either, so consumers can assume both
    are populated.

    ``succeeded_schemas``: ``dataset_id`` → ``effective_schema`` for
    succeeded bronze nodes, parsed out of the snapshot. The resume
    flow uses this to compute the post-preflight plan hash without
    re-probing BICC for already-succeeded nodes.

    ``succeeded_row_counts``: ``(dataset_id, layer)`` → most-recent
    non-NULL ``row_count`` observed for that pair under this
    ``run_id``. Carry-forwarded into ``RunStep.resumed_skip`` so the
    latest-row projection (and the ``fusion_autopilot_state_latest``
    VIEW) preserve the original logical row count instead of NULL.
    Walks back past any ``resumed_skipped`` rows (those have NULL
    row_count by definition — no work done) to the actual success
    row. **Tuple key**: matches the state table's
    primary-key grain; today no shipped registry entry reuses a
    ``dataset_id`` across layers, but a future addition that did
    would silently collide under the prior ``str``-only key.

    ``succeeded_last_watermarks``: ``(dataset_id, layer)``
    → most-recent ``last_watermark`` observed for that pair under
    this ``run_id``. Carry-forwarded into ``RunStep.resumed_skip``
    so a resumed-skip row preserves the original bronze run's
    persisted cursor on the ``fusion_autopilot_state_latest``
    projection rather than regressing it to NULL. Same tuple-key
    rationale as ``succeeded_row_counts``.

    ``original_started_at``: earliest ``last_run_at`` for this run_id.
    Surfaced in the resume-banner so the operator sees how old the
    checkpoint is.
    """

    run_id: str
    succeeded: frozenset[str]
    plan_hash: str
    plan_snapshot: str
    succeeded_schemas: "dict[str, str]"
    succeeded_row_counts: "dict[tuple[str, str], int]"
    succeeded_last_watermarks: "dict[tuple[str, str], datetime | None]"
    original_started_at: "datetime"


_RESUMABLE_TERMINAL_STATUSES = (
    "success", "failed", "skipped", "resumed_skipped", "deferred",
)


def read_resumable_state(
    spark: "SparkSession",
    paths: TablePaths,
    run_id: str,
) -> "ResumeContext":
    """Read ``fusion_autopilot_state`` for ``run_id`` and return a
    ``ResumeContext`` summarizing what already succeeded + the stored
    plan-hash / plan-snapshot for drift comparison.

    SQL contract — the ``run_id`` filter MUST live inside the
    ranked CTE, before ``ROW_NUMBER()``. On a shared state table,
    a global window across multiple runs would pick the wrong row
    when two runs touched the same ``dataset_id``. The in-CTE filter
    constrains the window to this run_id alone:

        WITH ranked AS (
          SELECT ..., ROW_NUMBER() OVER (
            PARTITION BY dataset_id ORDER BY last_run_at DESC
          ) AS rn
          FROM <state_table>
          WHERE run_id = :resume_run_id
            AND status IN (<terminal>)
        )
        SELECT ... FROM ranked WHERE rn = 1

    Failure modes (all raise to the caller, which lets them propagate
    so the CLI exits 2 cleanly via OrchestratorConfigError):

      * Zero rows for ``run_id`` ⇒ ``ResumeRunNotFoundError``.
      * Any row has ``plan_hash IS NULL`` or
        ``plan_snapshot IS NULL`` ⇒
        ``ResumeRunNotResumableError`` (legacy row or partially-
        migrated write path; no degraded-metadata fallback).
      * Multiple distinct non-NULL ``plan_hash`` values across the
        result set ⇒ ``RuntimeError`` (state corruption — the
        orchestrator never writes more than one hash per run_id).
    """
    # Local imports to avoid circular dep with errors.py at module
    # load (state.py is imported very early in orchestrator init).
    from .errors import (
        ResumeRunNotFoundError,
        ResumeRunNotResumableError,
    )

    table_path = _state_table_path(paths)
    status_list = ", ".join(f"'{s}'" for s in _RESUMABLE_TERMINAL_STATUSES)
    # The `run_id` filter is parameterized via repr() to defeat
    # injection. TablePaths.__post_init__ already validates the
    # table_path components; the caller-supplied run_id is the only
    # value originating outside the trusted boundary.
    escaped_run_id = run_id.replace("'", "''")
    # Partition by (dataset_id, layer), the state-table primary-key grain. The
    # tuple-keyed ``succeeded_row_counts`` / ``succeeded_last_watermarks`` dicts
    # prevent a future dataset_id reused across layers from silently dropping
    # the upper-layer row from the window.
    query = f"""
        WITH ranked AS (
          SELECT
            dataset_id, status, plan_hash, plan_snapshot,
            last_run_at, layer, mode,
            ROW_NUMBER() OVER (
              PARTITION BY dataset_id, layer
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
        )
        SELECT dataset_id, status, plan_hash, plan_snapshot,
               last_run_at, layer
        FROM ranked
        WHERE rn = 1
    """
    rows = spark.sql(query).collect()

    if not rows:
        raise ResumeRunNotFoundError(
            f"--resume: no rows in fusion_autopilot_state for run_id={run_id!r}. "
            f"Check the value (operator typo?) or use `aidp-fusion-autopilot "
            f"status` to list recent run_ids."
        )

    # Validate that every row has both drift-gate metadata fields populated.
    null_hash_dsids = [r["dataset_id"] for r in rows if r["plan_hash"] is None]
    null_snapshot_dsids = [r["dataset_id"] for r in rows if r["plan_snapshot"] is None]
    if null_hash_dsids:
        raise ResumeRunNotResumableError(
            f"--resume: run_id={run_id!r} is not resumable — "
            f"{len(null_hash_dsids)} row(s) lack plan_hash. This run "
            f"was written by an earlier plugin build that didn't store "
            f"drift-gate metadata; re-run from scratch."
        )
    if null_snapshot_dsids:
        raise ResumeRunNotResumableError(
            f"--resume: run_id={run_id!r} is not resumable — "
            f"{len(null_snapshot_dsids)} row(s) have plan_hash set but "
            f"plan_snapshot is NULL (partially-migrated write path). "
            f"Re-run from scratch."
        )

    # Verify the plan_hash is consistent across all rows. A run never
    # writes more than one hash; multiple values means state corruption.
    distinct_hashes = {r["plan_hash"] for r in rows}
    if len(distinct_hashes) > 1:  # pragma: no cover — corruption guard
        raise RuntimeError(
            f"--resume: run_id={run_id!r} state corruption — multiple "
            f"distinct plan_hash values found: {sorted(distinct_hashes)}. "
            f"Each run_id writes exactly one hash."
        )
    plan_hash = next(iter(distinct_hashes))
    plan_snapshot = rows[0]["plan_snapshot"]

    # `succeeded` includes BOTH 'success' AND 'resumed_skipped' so a
    # re-resume of an already-resumed run treats carry-forwards as
    # already done. See ResumeContext docstring for rationale.
    succeeded: set[str] = {
        r["dataset_id"]
        for r in rows
        if r["status"] in ("success", "resumed_skipped")
    }

    # Parse `succeeded_schemas` out of the snapshot's `nodes` list —
    # bronze nodes only (silver/gold/deferred have effective_schema="").
    succeeded_schemas: dict[str, str] = {}
    import json as _json
    try:
        snapshot = _json.loads(plan_snapshot)
        for node in snapshot.get("nodes", []):
            ds_id = node.get("dataset_id")
            schema = node.get("effective_schema") or ""
            if ds_id in succeeded and schema:
                succeeded_schemas[ds_id] = schema
    except (ValueError, TypeError):  # pragma: no cover — corruption guard
        # If the snapshot is unparseable, treat the run as non-resumable.
        # The schema migration writes valid JSON so this only fires on
        # a hand-edited row.
        raise ResumeRunNotResumableError(
            f"--resume: run_id={run_id!r} plan_snapshot is not valid "
            f"JSON. State row was hand-edited or written by a broken "
            f"build; re-run from scratch."
        )

    original_started_at = min(r["last_run_at"] for r in rows)

    # Build succeeded_row_counts: for each succeeded (dataset_id, layer),
    # the most-recent non-NULL row_count under this run_id. A
    # `resumed_skipped` row has NULL row_count by definition (no work
    # done), so on a re-resume the latest terminal row may be NULL;
    # walk back to find the actual success row's count. Done as a
    # second small query so the existing latest-per-(dataset, layer)
    # window doesn't need to widen.
    #
    # Partition and dict key use (dataset_id, layer) tuples matching the
    # state-table primary-key grain. See ResumeContext docstring for rationale.
    row_count_query = f"""
        WITH ranked AS (
          SELECT dataset_id, layer, row_count, last_run_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY dataset_id, layer
                   ORDER BY last_run_at DESC
                 ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
            AND row_count IS NOT NULL
        )
        SELECT dataset_id, layer, row_count FROM ranked WHERE rn = 1
    """
    rc_rows = spark.sql(row_count_query).collect()
    succeeded_row_counts: dict[tuple[str, str], int] = {}
    for r in rc_rows:
        ds_id = r["dataset_id"]
        layer = r["layer"]
        if ds_id in succeeded:
            succeeded_row_counts[(ds_id, layer)] = int(r["row_count"])

    # Build succeeded_last_watermarks: for each succeeded
    # (dataset_id, layer), the most-recent ``last_watermark`` (which
    # may be NULL — e.g. replace-style silver/gold rows or a true-first-empty
    # bronze run). Unlike succeeded_row_counts, we DO NOT
    # filter out NULL ``last_watermark`` rows in the WHERE clause —
    # a success row with NULL watermark is legitimate, and carrying ``None``
    # forward is the correct behavior. The latest terminal row per pair wins.
    last_watermark_query = f"""
        WITH ranked AS (
          SELECT dataset_id, layer, last_watermark, last_run_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY dataset_id, layer
                   ORDER BY last_run_at DESC
                 ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
        )
        SELECT dataset_id, layer, last_watermark FROM ranked WHERE rn = 1
    """
    lw_rows = spark.sql(last_watermark_query).collect()
    succeeded_last_watermarks: dict[tuple[str, str], datetime | None] = {}
    for r in lw_rows:
        ds_id = r["dataset_id"]
        layer = r["layer"]
        if ds_id in succeeded:
            succeeded_last_watermarks[(ds_id, layer)] = _normalize_to_utc(
                r["last_watermark"]
            )

    return ResumeContext(
        run_id=run_id,
        succeeded=frozenset(succeeded),
        plan_hash=plan_hash,
        plan_snapshot=plan_snapshot,
        succeeded_schemas=succeeded_schemas,
        succeeded_row_counts=succeeded_row_counts,
        succeeded_last_watermarks=succeeded_last_watermarks,
        original_started_at=original_started_at,
    )


# ---------------------------------------------------------------------------
# Content-pack resume reader
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CPResumeContext:
    """Resume snapshot for a default-flipped (content-pack) run.

    Differs from :class:`ResumeContext` in two structural ways
    reflecting the content-pack write path (``sql_runner._write_success_rows``):

    * **Per-node ``plan_hash``** — sql_runner writes a hash computed
      over the rendered SQL + output schema for each silver/gold node.
      Each node gets its OWN hash, so the v1 invariant "single
      plan_hash across the whole run" doesn't hold. The mixed shape
      (bronze rows from the legacy backend carry a run-level v1 hash;
      silver/gold rows carry per-node hashes) is the WAI design.

    * **``plan_snapshot`` may be ``None``** — sql_runner writes
      ``plan_snapshot=None`` on every CP row. A default-flipped run
      that also ran bronze through the legacy backend carries the v1
      bronze snapshot under those rows; pure CP runs have no snapshot
      at all.

    The dispatcher uses :func:`read_content_pack_resumable_state` to
    build this and threads it through bronze + cp branches the same
    way the v1 reader's :class:`ResumeContext` is threaded.

    Attributes:
        run_id: the run identifier (echoes the input).
        succeeded: dataset_ids whose latest terminal status is
            ``'success'`` or ``'resumed_skipped'``. Same semantics as
            :class:`ResumeContext.succeeded`.
        bronze_plan_snapshot: a v1-shape plan snapshot lifted from any
            bronze row that carries one. ``None`` for pure silver/gold-
            only runs. The dispatcher uses this for identity-drift
            checks (skipped when ``None``) and bare-resume scope
            reconstruction (falls back to ``scope_*`` fields).
        scope_datasets: dataset_ids observed across the run's rows —
            the actual scope the original run dispatched. Used to
            reconstruct ``--datasets`` for bare-resume when the
            snapshot is absent.
        scope_layers: layers observed across the run's rows.
        succeeded_row_counts: ``(dataset_id, layer)`` →
            most-recent non-NULL ``row_count`` (same as :class:`ResumeContext`).
        succeeded_last_watermarks: same shape as :class:`ResumeContext`.
        original_started_at: earliest ``last_run_at`` for this run_id.
    """

    run_id: str
    succeeded: frozenset[str]
    bronze_plan_snapshot: "str | None"
    scope_datasets: tuple[str, ...]
    scope_layers: tuple[str, ...]
    succeeded_row_counts: "dict[tuple[str, str], int]"
    succeeded_last_watermarks: "dict[tuple[str, str], datetime | None]"
    original_started_at: "datetime"
    # Durable run manifest (feature: fail-fast-seed-validation). Raw JSON from
    # the reserved ``__run_manifest__`` row, or None for a legacy/pre-feature
    # run (or an unmigrated table). Default None keeps every existing
    # constructor call valid.
    run_manifest_raw: "str | None" = None
    # Distinct EXECUTION modes (``mode IN ('seed','incremental')``) observed
    # across this run's real node rows — used for legacy (no-manifest) mode
    # inference / mixed-mode rejection.
    historical_exec_modes: tuple[str, ...] = ()


def read_content_pack_resumable_state(
    spark: "SparkSession",
    paths: TablePaths,
    run_id: str,
) -> "CPResumeContext":
    """Resume reader tolerant of the content-pack write path.

    Same SQL-query shape as :func:`read_resumable_state` but with the
    v1 invariants relaxed for the realities of how
    ``sql_runner._write_success_rows`` persists state:

    * **Allows per-node ``plan_hash`` variance** — no "single hash
      across all rows" check; each row carries its own hash.
    * **Allows ``plan_snapshot IS NULL``** — content-pack rows
      legitimately write None. The v1 reader's ``ResumeRunNotResumableError``
      branch would falsely reject these.
    * **Reconstructs scope from rows** — exposes the dataset/layer
      tuples observed under this run_id so the dispatcher can rebuild
      ``(datasets, layers)`` for bare-resume without depending on a
      snapshot.

    Still raises:
        ResumeRunNotFoundError: zero terminal rows for ``run_id``.

    Does NOT raise on null snapshot or hash mismatch — the v1 reader's
    invariants don't apply to the CP write path. Returns a
    :class:`CPResumeContext`.
    """
    # Local import — avoid an import cycle into errors.py at module load.
    from .errors import ResumeRunNotFoundError

    table_path = _state_table_path(paths)
    status_list = ", ".join(f"'{s}'" for s in _RESUMABLE_TERMINAL_STATUSES)
    escaped_run_id = run_id.replace("'", "''")
    query = f"""
        WITH ranked AS (
          SELECT
            dataset_id, status, plan_hash, plan_snapshot,
            last_run_at, layer, mode,
            ROW_NUMBER() OVER (
              PARTITION BY dataset_id, layer
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
        )
        SELECT dataset_id, status, plan_hash, plan_snapshot,
               last_run_at, layer, mode
        FROM ranked
        WHERE rn = 1
    """
    rows = spark.sql(query).collect()

    if not rows:
        raise ResumeRunNotFoundError(
            f"--resume: no rows in fusion_autopilot_state for run_id={run_id!r}. "
            f"Check the value (operator typo?) or use `aidp-fusion-autopilot "
            f"status` to list recent run_ids."
        )

    # Reserved ``__*__`` synthetic ids (gate markers, run manifest) are NEVER
    # real nodes — exclude them from every derived node set so a resume can't
    # mistake a synthetic row for succeeded/scope work.
    def _is_real_node(dataset_id: str) -> bool:
        return not (dataset_id.startswith("__") and dataset_id.endswith("__"))

    # EXECUTION rows only: audit modes (plan_hash_repin / fingerprint_skip) can
    # carry status='success' on a REAL node id but never represent executed
    # work — a resume that counted them would SKIP work that never ran. The
    # execution predicate is the shared guard for every derived set.
    _EXEC_MODES = ("seed", "incremental")

    def _is_exec_row(r) -> bool:
        return _is_real_node(r["dataset_id"]) and r["mode"] in _EXEC_MODES

    # Succeeded set — includes BOTH 'success' AND 'resumed_skipped' so a
    # re-resume of an already-resumed run treats carry-forwards as done. Counts
    # only EXECUTION rows on real nodes.
    succeeded: set[str] = {
        r["dataset_id"]
        for r in rows
        if r["status"] in ("success", "resumed_skipped") and _is_exec_row(r)
    }

    # Distinct execution modes across ALL historical rows (UNRANKED) — used for
    # legacy mixed-mode rejection. Must query every execution row, NOT the
    # latest-per-node survivors: an older `incremental` row hidden by a newer
    # `seed` row for the SAME node would vanish under ranking, so the mixed
    # physical baseline (which no forward --mode can repair) would go
    # undetected. Reserved `__*__` ids and audit modes are excluded.
    _mode_rows = spark.sql(
        f"SELECT DISTINCT mode FROM {table_path} "
        f"WHERE run_id = '{escaped_run_id}' "
        f"AND mode IN ('seed', 'incremental') "
        f"AND dataset_id NOT LIKE '\\_\\_%\\_\\_'"
    ).collect()
    historical_exec_modes = tuple(sorted({r["mode"] for r in _mode_rows}))

    # Durable run manifest ingestion — FAIL CLOSED, never fail open (Finding 1).
    #   * The ``run_manifest`` COLUMN being absent means a pre-feature table that
    #     could never have a manifest → legitimate legacy path (raw=None).
    #   * If the column exists, read EVERY reserved ``__run_manifest__`` row (no
    #     ``LIMIT 1``, no ``IS NOT NULL`` filter). Zero rows → legacy (raw=None).
    #   * A manifest row present but with a NULL payload, or MULTIPLE rows with
    #     conflicting payloads (immutability violated), or a Spark READ error —
    #     each surfaces as AIDPF-4022 (ManifestInvalidError), NOT the legacy
    #     path. Misclassifying any of these as legacy would reconstruct scope
    #     from only surviving rows, omit never-dispatched nodes, and bypass the
    #     identity / profile / topology guards.
    from .run_manifest import (
        AIDPF_4022_MANIFEST_COMMIT_FAILED,
        ManifestInvalidError,
    )

    run_manifest_raw: str | None = None
    if "run_manifest" in _existing_state_columns(spark, table_path):
        try:
            _mrows = spark.sql(
                f"SELECT run_manifest FROM {table_path} "
                f"WHERE run_id = '{escaped_run_id}' "
                f"AND dataset_id = '__run_manifest__'"
            ).collect()
        except Exception as exc:  # a real read failure is NOT "no manifest"
            raise ManifestInvalidError(
                f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: could not read the run "
                f"manifest for run_id={run_id!r} ({exc}). Resume aborted (fail "
                f"closed); start a fresh `--mode seed`."
            ) from exc
        if _mrows:
            _payloads = {r["run_manifest"] for r in _mrows}
            if len(_payloads) != 1:
                raise ManifestInvalidError(
                    f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: run_id={run_id!r} has "
                    f"{len(_mrows)} __run_manifest__ rows with conflicting "
                    f"payloads — the manifest must be a single immutable row. "
                    f"Non-resumable; start a fresh `--mode seed`."
                )
            _only = next(iter(_payloads))
            # Reject NULL, non-string, AND empty/blank payloads (an empty string
            # is corruption, not "no manifest" — accepting it would let the
            # dispatcher fall back to legacy reconstruction and bypass the
            # fail-closed guards).
            if not isinstance(_only, str) or not _only.strip():
                raise ManifestInvalidError(
                    f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: the __run_manifest__ "
                    f"row for run_id={run_id!r} has a missing/empty payload "
                    f"(corrupt manifest). Non-resumable; start a fresh "
                    f"`--mode seed`."
                )
            run_manifest_raw = _only

    # Lift a plan_snapshot from any row that carries one (bronze rows
    # from the legacy backend write one; CP rows do not). Take the
    # newest first by ``last_run_at`` so a refreshed identity reflects
    # the most recent successful seed cycle.
    bronze_plan_snapshot: str | None = None
    snap_candidates = sorted(
        (r for r in rows if r["plan_snapshot"]),
        key=lambda r: r["last_run_at"],
        reverse=True,
    )
    if snap_candidates:
        bronze_plan_snapshot = snap_candidates[0]["plan_snapshot"]

    # Scope reconstruction from the rows themselves — EXECUTION rows only.
    # Excludes reserved ``__*__`` markers (e.g. ``__fingerprint_skip__``,
    # ``__run_manifest__``) AND audit-mode rows on real ids (``plan_hash_repin``
    # is written on a real node id BEFORE the node executes). An audit-only row
    # must neither activate legacy reconstruction nor pollute the reconstructed
    # ``--datasets`` scope with a node that never ran.
    scope_datasets = tuple(
        sorted({r["dataset_id"] for r in rows if _is_exec_row(r)})
    )
    scope_layers = tuple(
        sorted({r["layer"] for r in rows if _is_exec_row(r)})
    )
    original_started_at = min(r["last_run_at"] for r in rows)

    # row_counts + last_watermarks queries — same SQL shape as
    # read_resumable_state; tuple-keyed by (dataset_id, layer).
    row_count_query = f"""
        WITH ranked AS (
          SELECT dataset_id, layer, row_count, last_run_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY dataset_id, layer
                   ORDER BY last_run_at DESC
                 ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
            AND row_count IS NOT NULL
        )
        SELECT dataset_id, layer, row_count FROM ranked WHERE rn = 1
    """
    rc_rows = spark.sql(row_count_query).collect()
    succeeded_row_counts: dict[tuple[str, str], int] = {}
    for r in rc_rows:
        ds_id = r["dataset_id"]
        layer = r["layer"]
        if ds_id in succeeded:
            succeeded_row_counts[(ds_id, layer)] = int(r["row_count"])

    last_watermark_query = f"""
        WITH ranked AS (
          SELECT dataset_id, layer, last_watermark, last_run_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY dataset_id, layer
                   ORDER BY last_run_at DESC
                 ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
        )
        SELECT dataset_id, layer, last_watermark FROM ranked WHERE rn = 1
    """
    lw_rows = spark.sql(last_watermark_query).collect()
    succeeded_last_watermarks: dict[tuple[str, str], datetime | None] = {}
    for r in lw_rows:
        ds_id = r["dataset_id"]
        layer = r["layer"]
        if ds_id in succeeded:
            succeeded_last_watermarks[(ds_id, layer)] = _normalize_to_utc(
                r["last_watermark"]
            )

    return CPResumeContext(
        run_id=run_id,
        succeeded=frozenset(succeeded),
        bronze_plan_snapshot=bronze_plan_snapshot,
        scope_datasets=scope_datasets,
        scope_layers=scope_layers,
        succeeded_row_counts=succeeded_row_counts,
        succeeded_last_watermarks=succeeded_last_watermarks,
        original_started_at=original_started_at,
        run_manifest_raw=run_manifest_raw,
        historical_exec_modes=historical_exec_modes,
    )


__all__ = [
    "AIDPF_4021_STATE_LOCATION_ORPHANED",
    "ensure_schemas",
    "ensure_state_table",
    "write_state_row",
    "write_fingerprint_skip_row",
    "read_last_watermark",
    "read_resumable_state",
    "read_content_pack_resumable_state",
    "ResumeContext",
    "CPResumeContext",
    "WATERMARK_READ_SOFT_FAILED_MARKER",
    "_ensure_target_table_exists",
]
