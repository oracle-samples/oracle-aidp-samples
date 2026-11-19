"""PySpark-backed test for write_state_rows_hard's schema path.

The mock-spark tests in tests/unit/test_state_phase2.py exercise the
ImportError fallback (pyspark not installed in the default venv). This
file's tests are gated by ``pytest.importorskip('pyspark')`` and run
ONLY when pyspark is available — they verify the production path that
builds an explicit StructType for ``spark.createDataFrame``.

Without this coverage, real PySpark would raise:
    PySparkValueError: [CANNOT_DETERMINE_TYPE]
on any content-pack cursor-advancing row (because single-source success rows
legitimately have all-None values in ``node_version`` /
``fusion_version`` / ``input_watermark_start`` / etc.).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("content_pack-state-write-test")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# Schema construction
# ---------------------------------------------------------------------------


class TestStateRowSchema:
    def test_schema_covers_v1_and_content_pack_columns(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.state_phase2 import (
            CONTENT_PACK_STATE_COLUMNS,
            _build_state_row_schema,
        )
        schema = _build_state_row_schema()
        names = {f.name for f in schema.fields}
        # v1 base columns.
        for required in (
            "run_id", "dataset_id", "layer", "mode", "last_watermark",
            "last_run_at", "status", "row_count", "error_message",
            "skip_reason", "duration_seconds", "plan_hash", "plan_snapshot",
        ):
            assert required in names, f"missing v1 column {required!r}"
        # content-pack columns.
        for name, _ in CONTENT_PACK_STATE_COLUMNS:
            assert name in names, f"missing content-pack column {name!r}"

    def test_all_fields_nullable(self) -> None:
        """The content-pack caller-side normaliser handles v1 NOT NULL coercion;
        the StructType itself declares every field nullable so a row with
        all-None nullable columns doesn't fail Spark's strict mode."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.state_phase2 import (
            _build_state_row_schema,
        )
        schema = _build_state_row_schema()
        for field in schema.fields:
            assert field.nullable, f"{field.name} should be nullable"


# ---------------------------------------------------------------------------
# createDataFrame with all-None nullable columns succeeds
# ---------------------------------------------------------------------------


class TestCreateDataFrameWithAllNoneColumns:
    """The reviewer's concrete case: a content-pack single-source success row
    has node_version, fusion_version, input_watermark_start,
    input_watermark_end, consumed_version all None. Without an explicit
    schema, real PySpark raises CANNOT_DETERMINE_TYPE."""

    def test_content_pack_single_source_success_row_builds_via_explicit_schema(self, spark) -> None:
        """The same row dict that the dispatcher produces — every
        nullable column None — must build a DataFrame without raising."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.state_phase2 import (
            _build_state_row_schema,
            _normalise_row_for_schema,
        )

        now = datetime.now(timezone.utc)
        # Representative single-source success row (mirrors the dispatcher's
        # _assemble_success_state_rows shape).
        row = {
            "run_id": "cp-test",
            "dataset_id": "dim_thing",
            "layer": "silver",
            "mode": "seed",
            "last_watermark": None,
            "last_run_at": now,
            "status": "success",
            "row_count": 42,
            "error_message": None,
            "skip_reason": None,
            "duration_seconds": 0.5,
            "plan_hash": "h",
            "plan_snapshot": None,
            "pack_id": "p",
            "pack_version": "1.0.0",
            "node_version": None,
            "node_implementation_type": "sql",
            "rendered_sql_hash": "rsh",
            "output_schema_hash": "osh",
            "profile_hash": "ph",
            "tenant_fingerprint": "t",
            "fusion_version": None,
            "bronze_schema_fingerprint": "bsf",
            "source_id": "erp_thing",
            "source_role": "primary",
            "input_watermark_start": None,
            "input_watermark_end": None,
            "output_watermark": None,
            "consumed_version": None,
            "delta_row_count": 42,
        }
        schema = _build_state_row_schema()
        normalised = _normalise_row_for_schema(row, [f.name for f in schema.fields])
        # This is what the production path does. Without schema=schema
        # this would raise on real PySpark.
        df = spark.createDataFrame([normalised], schema=schema)
        # Confirm the DataFrame materialises.
        assert df.count() == 1
        collected = df.collect()[0]
        assert collected["dataset_id"] == "dim_thing"
        assert collected["row_count"] == 42

    def test_cascade_skip_row_with_many_none_columns_builds(self, spark) -> None:
        """The cascade-skip diagnostic row has even more None columns —
        every cursor field, every hash, row_count, plan_hash, etc. Must
        still build a DataFrame without raising."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.state_phase2 import (
            _build_state_row_schema,
            _normalise_row_for_schema,
        )

        now = datetime.now(timezone.utc)
        row = {
            "run_id": "cp-test",
            "dataset_id": "mart_x",
            "layer": "gold",
            "mode": "seed",
            "last_watermark": None,
            "last_run_at": now,
            "status": "skipped",
            "row_count": None,
            "error_message": "cascade: upstream 'dim_a' failed",
            "skip_reason": "cascade",
            "duration_seconds": None,  # v1 NOT NULL — coerced to 0.0 by normaliser
            "plan_hash": None,
            "plan_snapshot": None,
            "source_id": None,
            "source_role": "primary",
            # Every other content-pack column omitted — normaliser fills with None.
        }
        schema = _build_state_row_schema()
        normalised = _normalise_row_for_schema(row, [f.name for f in schema.fields])
        # The v1 NOT NULL coercion fires: normaliser sets duration_seconds=0.0.
        assert normalised["duration_seconds"] == 0.0
        df = spark.createDataFrame([normalised], schema=schema)
        assert df.count() == 1
        collected = df.collect()[0]
        assert collected["status"] == "skipped"
        assert collected["skip_reason"] == "cascade"
        assert collected["duration_seconds"] == 0.0

    def test_multi_source_batch_builds_atomically(self, spark) -> None:
        """The atomic-batch contract: primary + N lookup rows all build
        in a single DataFrame via createDataFrame."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.state_phase2 import (
            _build_state_row_schema,
            _normalise_row_for_schema,
        )

        now = datetime.now(timezone.utc)
        base = {
            "run_id": "cp-test",
            "dataset_id": "dim_thing",
            "layer": "silver",
            "mode": "incremental",
            "last_run_at": now,
            "status": "success",
            "duration_seconds": 1.0,
            "pack_id": "p", "pack_version": "1.0.0",
            "node_implementation_type": "sql",
        }
        primary_row = {**base, "source_id": "erp_thing", "source_role": "primary",
                       "row_count": 100, "last_watermark": now, "output_watermark": now,
                       "delta_row_count": 100}
        lookup_row = {**base, "source_id": "ref_table", "source_role": "lookup",
                      "consumed_version": now}

        schema = _build_state_row_schema()
        field_names = [f.name for f in schema.fields]
        rows = [
            _normalise_row_for_schema(primary_row, field_names),
            _normalise_row_for_schema(lookup_row, field_names),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        assert df.count() == 2
        # Both source roles preserved.
        roles = sorted(r["source_role"] for r in df.collect())
        assert roles == ["lookup", "primary"]


# ---------------------------------------------------------------------------
# Locks the reviewer's failure mode: bare createDataFrame WITHOUT schema raises
# ---------------------------------------------------------------------------


class TestBareCreateDataFrameRegressionGuard:
    """This test characterises what would happen WITHOUT the schema fix —
    a regression that drops the schema= kwarg would re-introduce the
    PySparkValueError. Acts as a tripwire."""

    def test_bare_createdataframe_with_all_none_column_raises(self, spark) -> None:
        """Confirms the underlying PySpark behaviour the fix protects against.
        Documents the failure mode so a future contributor doesn't 'simplify'
        the code by removing the explicit schema."""
        row_with_all_none_column = {"a": None, "b": 1}
        with pytest.raises(Exception):  # PySparkValueError or AnalysisException
            spark.createDataFrame([row_with_all_none_column]).collect()
