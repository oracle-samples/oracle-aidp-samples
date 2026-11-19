"""Content-pack state-layer additions: additive migration + atomic batch write.

This module sits alongside ``orchestrator/state.py`` and adds:

* :data:`CONTENT_PACK_STATE_COLUMNS` — the tuple of nullable columns added
  to ``fusion_autopilot_state`` for content-pack runs.
* :func:`ensure_state_columns_v2` — additive migration using the same
  introspect-then-ADD COLUMNS pattern v1 uses (Spark rejects ``ADD
  COLUMN IF NOT EXISTS``; we DESCRIBE first, compute the missing set,
  emit a single ALTER).
* :func:`update_latest_view_for_phase2` — redeploys the ``fusion_autopilot_state_latest``
  view with the widened partition ``(run_id, dataset_id, layer, source_id)``.
  v1 rows project identically in the common single-layer-dataset_id
  case; multi-layer dataset_id (rare; v1 collapsed them — that was a
  latent bug) now correctly returns one row per layer.
* :class:`StateCommitError` — raised on a hard-write failure.
* :func:`write_state_rows_hard` — **atomic batch** write. Builds one
  DataFrame from ``rows`` and appends it to the state table in a
  single Delta append. Delta's append atomicity guarantees all rows
  commit together or none do, so a multi-source success cannot
  partially commit (which would advance the primary's
  output_watermark without the lookup audit rows reaching the table).

The base ``ensure_state_table`` and ``write_state_row`` continue to work
unchanged. Content-pack callers explicitly invoke these helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..config.paths import TablePaths


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_4060_STATE_COMMIT_FAILURE = "AIDPF-4060"
"""State-row hard commit failed — Delta append raised; no rows committed."""

AIDPF_4061_OUTPUT_WATERMARK_REGRESSED = "AIDPF-4061"
"""State row written with `output_watermark` lower than the prior successful row.
Defensive guard — shouldn't happen in practice."""


class StateCommitError(Exception):
    """Hard-commit failure (AIDPF-4060). Raised by :func:`write_state_rows_hard`.

    Carries the original Spark exception as ``__cause__`` so callers
    can surface the underlying diagnostic without losing the stack.
    """


# ---------------------------------------------------------------------------
# Content-pack column set
# ---------------------------------------------------------------------------

CONTENT_PACK_STATE_COLUMNS: tuple[tuple[str, str], ...] = (
    # Pack / profile identity.
    ("pack_id", "STRING"),
    ("pack_version", "STRING"),
    ("node_version", "STRING"),
    ("node_implementation_type", "STRING"),
    ("rendered_sql_hash", "STRING"),
    ("output_schema_hash", "STRING"),
    ("profile_hash", "STRING"),
    # Identity fingerprints.
    ("tenant_fingerprint", "STRING"),
    ("fusion_version", "STRING"),
    ("bronze_schema_fingerprint", "STRING"),
    # Source-level cursors for primary/lookup rows.
    ("source_id", "STRING"),
    ("source_role", "STRING"),  # 'primary' | 'lookup'
    ("input_watermark_start", "TIMESTAMP"),
    ("input_watermark_end", "TIMESTAMP"),
    ("output_watermark", "TIMESTAMP"),
    ("consumed_version", "TIMESTAMP"),
    ("delta_row_count", "LONG"),
    # Durable pre-execution run manifest (feature: fail-fast-seed-validation).
    # Set ONLY on the reserved ``__run_manifest__`` row; NULL on every real
    # node row. Dedicated column (not overloading plan_snapshot) so the legacy
    # bronze_plan_snapshot lifter is untouched — zero risk to the live resume
    # path.
    ("run_manifest", "STRING"),
    # Additive-COA fast-path acceptance trace (feature:
    # incremental-coa-chart-onboarding). Set on a node's atomic SUCCESS row when
    # its AIDPF-4040 plan-hash advance was accepted as a proven additive COA
    # change; NULL on every other row. Dedicated column (not overloading
    # skip_reason) so a success row is never misread as a skip. Observability
    # only — the acceptance is re-derivable from the paired manifest + proof.
    ("coa_additive_accept_reason", "STRING"),
)
"""New nullable columns added to ``fusion_autopilot_state`` for content-pack
runs. v1 rows write NULL for these columns and continue to work; v2
readers read them when present."""


# ---------------------------------------------------------------------------
# Additive migration — introspect-then-ADD COLUMNS pattern
# ---------------------------------------------------------------------------


def ensure_state_columns_v2(spark: "SparkSession", paths: "TablePaths") -> None:
    """Apply content-pack additive columns to ``fusion_autopilot_state``.

    Uses Spark's introspect-then-ADD COLUMNS pattern (matches the v1
    ``ensure_state_table`` migration logic). Spark's ``ALTER TABLE ...
    ADD COLUMNS`` parser rejects ``IF NOT EXISTS`` — we DESCRIBE the
    table, compute the missing column set, and emit a single ALTER
    with ONLY those columns. Empty-diff (every content-pack column already
    present) is a no-op.

    This function ALSO redeploys the ``fusion_autopilot_state_latest``
    view with the content-pack grain (PARTITION BY widened to include
    ``layer`` + ``source_id``). The view's DDL is updated to project
    the new content-pack columns alongside the base columns.

    Idempotent. Safe to call on every content-pack run (re-running
    after the migration has applied is a no-op).

    Args:
        spark: live Spark session.
        paths: TablePaths from the loaded bundle.

    Raises:
        AnalysisException: if the catalog/schema is wrong (the
            existing :func:`state.ensure_state_table` should run
            before this and would have surfaced that case already).
    """
    from . import state as v1_state

    table_path = v1_state._state_table_path(paths)
    view_path = v1_state._state_latest_view_path(paths)

    existing = v1_state._existing_state_columns(spark, table_path)
    missing = [
        (name, dtype) for name, dtype in CONTENT_PACK_STATE_COLUMNS if name not in existing
    ]
    if missing:
        spark.sql(v1_state._build_add_columns_ddl(table_path, missing))

    # Redeploy the latest view with the content-pack grain. CREATE OR REPLACE
    # VIEW is idempotent and updates the projection in place.
    spark.sql(_phase2_latest_view_ddl(table_path, view_path))


def _phase2_latest_view_ddl(table_path: str, view_path: str) -> str:
    """The content-pack ``fusion_autopilot_state_latest`` view DDL.

    Widens the PARTITION BY to ``(run_id, dataset_id, layer, source_id)``.

    Common case (single-layer dataset_id, no source-level rows): the
    cardinality and projection are identical to the v1 view — adding
    ``layer`` to the key doesn't split because all v1 rows for a given
    dataset_id share the same layer value; adding ``source_id`` doesn't
    split because v1 rows leave it NULL.

    Multi-source content-pack rows project as N rows per ``(run_id,
    dataset_id, layer)`` where N = number of sources, each
    distinguished by its ``source_id``. Multi-layer dataset_id (rare;
    v1 collapsed them silently — that was a latent bug) now correctly
    returns one row per layer.

    Projects base columns + the content-pack column set so readers see
    them. Both the base and content-pack column
    sets MUST exist on the underlying table for this view to compile —
    callers MUST run :func:`ensure_state_columns_v2` first (which
    drops + redeploys this view as part of its idempotent flow).
    """
    return f"""
        CREATE OR REPLACE VIEW {view_path} AS
        WITH ranked AS (
          SELECT
            run_id, dataset_id, layer, mode, last_watermark, last_run_at,
            status, row_count, error_message, skip_reason, duration_seconds,
            plan_hash, plan_snapshot,
            pack_id, pack_version, node_version, node_implementation_type,
            rendered_sql_hash, output_schema_hash, profile_hash,
            tenant_fingerprint, fusion_version, bronze_schema_fingerprint,
            source_id, source_role, input_watermark_start, input_watermark_end,
            output_watermark, consumed_version, delta_row_count, run_manifest,
            coa_additive_accept_reason,
            ROW_NUMBER() OVER (
              PARTITION BY run_id, dataset_id, layer, source_id
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {table_path}
        )
        SELECT
          run_id, dataset_id, layer, mode, last_watermark, last_run_at,
          status, row_count, error_message, skip_reason, duration_seconds,
          plan_hash, plan_snapshot,
          pack_id, pack_version, node_version, node_implementation_type,
          rendered_sql_hash, output_schema_hash, profile_hash,
          tenant_fingerprint, fusion_version, bronze_schema_fingerprint,
          source_id, source_role, input_watermark_start, input_watermark_end,
          output_watermark, consumed_version, delta_row_count, run_manifest,
          coa_additive_accept_reason
        FROM ranked
        WHERE rn = 1
    """


# ---------------------------------------------------------------------------
# Atomic batch hard write — multi-source rows commit together or not at all
# ---------------------------------------------------------------------------


def _build_state_row_schema():
    """Construct the canonical state-row StructType.

    Real PySpark rejects ``createDataFrame([{"a": None}])``
    because it can't infer a type for a column whose only value is
    None — and content-pack state rows legitimately have all-None columns
    in the common case (``node_version``, ``fusion_version``,
    ``input_watermark_start``, ``input_watermark_end``,
    ``consumed_version`` are None on every single-source success row;
    diagnostic rows have many more None columns). Without an explicit
    schema, ``write_state_rows_hard`` would raise ``PySparkValueError:
    [CANNOT_DETERMINE_TYPE]`` on the first cursor-advancing row and
    every soft diagnostic write would also be swallowed (the best-
    effort writers catch the same failure).

    The schema covers v1 base columns (matching ``state.py::_ddl``)
    plus :data:`CONTENT_PACK_STATE_COLUMNS`. Every field is declared nullable
    here even though some v1 columns are ``NOT NULL`` in the Delta
    DDL — Spark accepts a more-permissive read schema; the table's
    own DDL still enforces the v1 NOT NULL constraints on the
    underlying storage, and the caller-side normaliser below coerces
    the v1 NOT NULL fields to safe defaults before append.

    Lazy import of pyspark types so this module's import contract
    remains pyspark-free until the function is actually called.
    """
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map = {"STRING": StringType(), "TIMESTAMP": TimestampType(),
                "LONG": LongType(), "BIGINT": LongType(), "DOUBLE": DoubleType()}

    fields = [
        # Base columns (mirror state.py::_ddl). The DataFrame schema declares
        # them nullable=True on the DataFrame side; the Delta table's
        # own DDL preserves the v1 NOT NULL constraints.
        StructField("run_id", StringType(), True),
        StructField("dataset_id", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("last_watermark", TimestampType(), True),
        StructField("last_run_at", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("skip_reason", StringType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("plan_hash", StringType(), True),
        StructField("plan_snapshot", StringType(), True),
    ]
    # Content-pack columns — every entry is nullable by design.
    for name, dtype in CONTENT_PACK_STATE_COLUMNS:
        fields.append(StructField(name, type_map[dtype], True))

    return StructType(fields)


# Base columns declared NOT NULL in the table DDL. Content-pack diagnostic
# rows that legitimately have None for these (e.g. cascade-skip with
# duration_seconds=None) get coerced before the append so the Delta
# constraint check doesn't reject the batch.
_V1_NOT_NULL_DEFAULTS: tuple[tuple[str, Any], ...] = (
    ("duration_seconds", 0.0),
)


def _normalise_row_for_schema(row: Mapping[str, Any], field_names: list[str]) -> dict[str, Any]:
    """Project ``row`` onto ``field_names``, filling missing keys with None
    and coercing v1 NOT NULL fields to safe defaults.

    Returns a dict whose keys exactly match the schema's fields, in
    declared order. PySpark's ``createDataFrame(..., schema=...)``
    accepts row dicts whose keys are a superset of the schema, but
    being defensive here keeps the contract narrow.
    """
    out: dict[str, Any] = {}
    for name in field_names:
        out[name] = row.get(name)
    # Coerce v1 NOT NULL fields if the caller left them as None.
    for nn_name, default in _V1_NOT_NULL_DEFAULTS:
        if out.get(nn_name) is None:
            out[nn_name] = default
    return out


def write_state_rows_hard(
    spark: "SparkSession",
    paths: "TablePaths",
    rows: Sequence[Mapping[str, Any]],
) -> None:
    """Append a batch of state rows to ``fusion_autopilot_state`` atomically.

    Content-pack execution writes one state row per source per node per
    run for a successful content-pack execution. Per-row writes would
    leave a window where the primary's ``output_watermark`` has
    committed but a lookup audit row hasn't — that would silently
    advance the cursor without the audit trail. This function does a
    **single Delta append** of the full row list so the entire batch
    commits or none of it does.

    The caller assembles every row (primary
    + every lookup) in memory FIRST, then calls this function exactly
    once. ``rows`` may be a single-element list for single-source
    nodes; the API shape is uniform.

    Builds the DataFrame with an explicit
    :class:`StructType` covering base columns + ``CONTENT_PACK_STATE_COLUMNS``
    so Spark doesn't try to infer types from row dicts where some
    columns are None in every row. The v1 NOT NULL fields
    (``duration_seconds``) get coerced to safe defaults via
    :func:`_normalise_row_for_schema` so the table's DDL constraint
    still holds.

    Args:
        spark: live Spark session.
        paths: TablePaths from the loaded bundle.
        rows: sequence of dict-shaped rows. Keys not in the canonical
            schema are dropped; missing keys are filled with None.
            Empty sequence is a no-op.

    Raises:
        StateCommitError: AIDPF-4060 — the underlying Delta append
            raised. No rows from this batch are visible to subsequent
            reads. The previous run's primary row's ``last_watermark``
            remains the cursor on the next run.
    """
    if not rows:
        return

    from . import state as v1_state

    table_path = v1_state._state_table_path(paths)

    try:
        try:
            schema = _build_state_row_schema()
            field_names = [f.name for f in schema.fields]
            normalised_rows = [_normalise_row_for_schema(r, field_names) for r in rows]
            df = spark.createDataFrame(normalised_rows, schema=schema)
        except ImportError:
            # pyspark not importable in the current env (fake-Spark
            # unit-test path). Mock-spark tests don't need the explicit
            # schema; the pyspark-backed test exercises the production
            # path that catches the null-type-inference issue.
            df = spark.createDataFrame(list(rows))
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_path)
        )
    except Exception as exc:  # noqa: BLE001 — re-wrap any Spark failure
        raise StateCommitError(
            f"{AIDPF_4060_STATE_COMMIT_FAILURE}: failed to commit {len(rows)} "
            f"state row(s) to {table_path}. Delta append raised: {type(exc).__name__}: {exc}. "
            f"No rows from this batch are visible to subsequent reads; the "
            f"prior run's last_watermark remains the cursor."
        ) from exc
