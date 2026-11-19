"""Unit tests for ``orchestrator/state_phase2.py``."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.state_phase2 import (
    AIDPF_4060_STATE_COMMIT_FAILURE,
    CONTENT_PACK_STATE_COLUMNS,
    StateCommitError,
    _phase2_latest_view_ddl,
    ensure_state_columns_v2,
    write_state_rows_hard,
)


# ---------------------------------------------------------------------------
# CONTENT_PACK_STATE_COLUMNS — locks the migration column set
# ---------------------------------------------------------------------------


class TestContentPackStateColumns:
    def test_includes_pack_profile_identity(self) -> None:
        names = {n for n, _ in CONTENT_PACK_STATE_COLUMNS}
        for required in ("pack_id", "pack_version", "node_implementation_type",
                         "rendered_sql_hash", "output_schema_hash", "profile_hash"):
            assert required in names

    def test_includes_identity_fingerprints(self) -> None:
        names = {n for n, _ in CONTENT_PACK_STATE_COLUMNS}
        for required in ("tenant_fingerprint", "fusion_version", "bronze_schema_fingerprint"):
            assert required in names

    def test_includes_source_level_cursors(self) -> None:
        names = {n for n, _ in CONTENT_PACK_STATE_COLUMNS}
        for required in ("source_id", "source_role", "input_watermark_start",
                         "input_watermark_end", "output_watermark", "consumed_version",
                         "delta_row_count"):
            assert required in names

    def test_no_duplicate_names(self) -> None:
        names = [n for n, _ in CONTENT_PACK_STATE_COLUMNS]
        assert len(names) == len(set(names))


# ---------------------------------------------------------------------------
# ensure_state_columns_v2 — introspect-then-ADD COLUMNS pattern
# ---------------------------------------------------------------------------


class TestEnsureStateColumnsV2:
    def _fake_paths(self) -> MagicMock:
        paths = MagicMock()
        return paths

    def _fake_spark_with_describe(self, existing: list[str]) -> MagicMock:
        spark = MagicMock()
        describe_df = MagicMock()
        describe_df.collect.return_value = [
            {"col_name": c, "data_type": "string", "comment": None} for c in existing
        ]
        # ensure_state_table's introspect uses row["col_name"]; the dict
        # access works with the dict-shaped rows above.
        spark.sql.return_value = describe_df
        return spark

    def test_migration_emits_alter_only_for_missing_columns(self, monkeypatch) -> None:
        """If 3 of the 17 content-pack columns are already present, the ALTER
        statement includes the other 14."""
        # Stub _state_table_path / _state_latest_view_path / _build_add_columns_ddl
        # / _existing_state_columns to control inputs precisely.
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state

        monkeypatch.setattr(v1_state, "_state_table_path", lambda paths: "cat.bronze.fusion_autopilot_state")
        monkeypatch.setattr(v1_state, "_state_latest_view_path", lambda paths: "cat.bronze.fusion_autopilot_state_latest")

        # Existing: pack_id, pack_version + the v1 base columns (which
        # aren't in CONTENT_PACK_STATE_COLUMNS so they're orthogonal).
        already_have = {"pack_id", "pack_version", "node_version"}
        monkeypatch.setattr(
            v1_state, "_existing_state_columns",
            lambda spark, table: already_have,
        )

        # Capture the ALTER DDL that gets built.
        built_ddls: list[str] = []

        def fake_build_ddl(table_path: str, missing: list[tuple[str, str]]) -> str:
            ddl = f"ALTER TABLE {table_path} ADD COLUMNS ({', '.join(f'{n} {t}' for n, t in missing)})"
            built_ddls.append(ddl)
            return ddl

        monkeypatch.setattr(v1_state, "_build_add_columns_ddl", fake_build_ddl)

        spark = MagicMock()
        ensure_state_columns_v2(spark, self._fake_paths())

        # The ALTER must include the columns NOT in `already_have`.
        assert len(built_ddls) == 1
        for name, _ in CONTENT_PACK_STATE_COLUMNS:
            if name in already_have:
                assert name not in built_ddls[0]
            else:
                assert name in built_ddls[0]

    def test_migration_is_noop_when_all_content_pack_columns_present(self, monkeypatch) -> None:
        """If every content-pack column already exists, no ALTER is emitted —
        only the view DDL is run."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state

        monkeypatch.setattr(v1_state, "_state_table_path", lambda paths: "t")
        monkeypatch.setattr(v1_state, "_state_latest_view_path", lambda paths: "v")
        monkeypatch.setattr(
            v1_state, "_existing_state_columns",
            lambda spark, table: {n for n, _ in CONTENT_PACK_STATE_COLUMNS},
        )
        build_calls = []
        monkeypatch.setattr(
            v1_state, "_build_add_columns_ddl",
            lambda *a, **kw: (build_calls.append((a, kw)), "ALTER TABLE noop")[1],
        )

        spark = MagicMock()
        ensure_state_columns_v2(spark, self._fake_paths())

        # No ALTER built (the migration short-circuits on empty diff).
        assert build_calls == []
        # Spark.sql was still called — for the view DDL.
        assert spark.sql.call_count == 1
        assert "CREATE OR REPLACE VIEW v" in spark.sql.call_args.args[0]

    def test_migration_idempotency_second_call_is_noop(self, monkeypatch) -> None:
        """Calling ensure_state_columns_v2 twice in a row: the second
        call sees all columns present and only redeploys the view."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state

        monkeypatch.setattr(v1_state, "_state_table_path", lambda paths: "t")
        monkeypatch.setattr(v1_state, "_state_latest_view_path", lambda paths: "v")

        # Simulate the table starting empty of content-pack cols, then having
        # them all after first call.
        call_state = {"count": 0}

        def fake_existing(spark, table):
            call_state["count"] += 1
            if call_state["count"] == 1:
                return set()
            return {n for n, _ in CONTENT_PACK_STATE_COLUMNS}

        monkeypatch.setattr(v1_state, "_existing_state_columns", fake_existing)

        alter_calls: list[str] = []
        monkeypatch.setattr(
            v1_state, "_build_add_columns_ddl",
            lambda table, missing: (alter_calls.append(missing), "ALTER")[1],
        )

        spark = MagicMock()
        ensure_state_columns_v2(spark, self._fake_paths())
        ensure_state_columns_v2(spark, self._fake_paths())

        # First call built the ALTER with all 17 columns. Second call
        # built nothing.
        assert len(alter_calls) == 1
        assert len(alter_calls[0]) == len(CONTENT_PACK_STATE_COLUMNS)


# ---------------------------------------------------------------------------
# Latest view DDL — content-pack grain locked
# ---------------------------------------------------------------------------


class TestContentPackLatestViewDDL:
    def test_partition_by_includes_layer_and_source_id(self) -> None:
        ddl = _phase2_latest_view_ddl("cat.bronze.state", "cat.bronze.latest")
        assert "PARTITION BY run_id, dataset_id, layer, source_id" in ddl

    def test_view_projects_v1_columns_for_backward_compat(self) -> None:
        ddl = _phase2_latest_view_ddl("t", "v")
        for col in ("last_watermark", "last_run_at", "status", "plan_hash"):
            assert col in ddl

    def test_view_projects_content_pack_columns(self) -> None:
        ddl = _phase2_latest_view_ddl("t", "v")
        for col in ("pack_id", "rendered_sql_hash", "source_id", "output_watermark"):
            assert col in ddl


# ---------------------------------------------------------------------------
# write_state_rows_hard — atomic batch contract
# ---------------------------------------------------------------------------


class TestWriteStateRowsHard:
    def _setup(self, monkeypatch):
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        monkeypatch.setattr(v1_state, "_state_table_path", lambda paths: "cat.bronze.fusion_autopilot_state")

    def test_empty_rows_is_noop(self, monkeypatch) -> None:
        self._setup(monkeypatch)
        spark = MagicMock()
        write_state_rows_hard(spark, MagicMock(), [])
        spark.createDataFrame.assert_not_called()
        spark.sql.assert_not_called()

    def test_single_row_creates_dataframe_and_appends(self, monkeypatch) -> None:
        self._setup(monkeypatch)
        spark = MagicMock()
        df = MagicMock()
        spark.createDataFrame.return_value = df

        rows = [{"run_id": "r1", "dataset_id": "x", "layer": "silver", "source_id": None,
                 "source_role": "primary"}]
        write_state_rows_hard(spark, MagicMock(), rows)

        # Atomic batch contract: ONE createDataFrame call regardless of
        # the input shape. We don't assert the exact args here — the
        # round-16 schema fix routes through _normalise_row_for_schema +
        # an explicit StructType when pyspark is importable, so the
        # createDataFrame call shape differs depending on the runtime.
        # The semantic locked here is "single batch write, not per-row".
        assert spark.createDataFrame.call_count == 1
        # Single .write.format("delta").mode("append").option(...).saveAsTable(...) chain.
        df.write.format.assert_called_with("delta")
        df.write.format("delta").mode.assert_called_with("append")

    def test_multi_row_creates_single_dataframe_for_batch(self, monkeypatch) -> None:
        """Atomic batch contract: ONE createDataFrame call regardless of
        row count — never per-row.
        """
        self._setup(monkeypatch)
        spark = MagicMock()
        df = MagicMock()
        spark.createDataFrame.return_value = df

        rows = [
            {"run_id": "r1", "dataset_id": "x", "source_id": "primary_src", "source_role": "primary"},
            {"run_id": "r1", "dataset_id": "x", "source_id": "lookup_a", "source_role": "lookup"},
            {"run_id": "r1", "dataset_id": "x", "source_id": "lookup_b", "source_role": "lookup"},
        ]
        write_state_rows_hard(spark, MagicMock(), rows)

        # Exactly ONE createDataFrame call — the entire batch goes through
        # a single Delta append. If a future regression switches to per-row
        # writes, this assertion catches it. Per-row counts are the only
        # contract; arg shape varies by pyspark availability (schema-
        # explicit when pyspark importable; inferred when not).
        assert spark.createDataFrame.call_count == 1
        # The call's first positional arg has the same number of rows as
        # the input — the round-16 normaliser fills missing keys with None
        # but preserves the row count.
        called_rows = spark.createDataFrame.call_args[0][0]
        assert len(called_rows) == len(rows)

    def test_delta_append_failure_wraps_as_state_commit_error(self, monkeypatch) -> None:
        self._setup(monkeypatch)
        spark = MagicMock()
        df = MagicMock()
        # Simulate the Delta append raising.
        df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.side_effect = (
            RuntimeError("simulated AnalysisException")
        )
        spark.createDataFrame.return_value = df

        rows = [{"run_id": "r1"}]
        with pytest.raises(StateCommitError) as exc_info:
            write_state_rows_hard(spark, MagicMock(), rows)
        assert AIDPF_4060_STATE_COMMIT_FAILURE in str(exc_info.value)
        # Underlying exception preserved as __cause__.
        assert isinstance(exc_info.value.__cause__, RuntimeError)
