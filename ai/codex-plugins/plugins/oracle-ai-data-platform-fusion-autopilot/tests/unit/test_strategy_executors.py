"""Unit tests for ``orchestrator/strategy_executors.py`` (Phase 2 Steps 5-6).

Uses fake Spark (unittest.mock) to verify:

* execute_replace issues CREATE OR REPLACE TABLE with args=rendered.params.
* execute_merge issues the empty-delta probe first, skips MERGE when 0
  rows + flags merge_skipped_empty_delta=True.
* execute_merge with non-empty delta runs the full MERGE INTO with
  args=rendered.params.
* Param-binding security: a malicious profile value (DROP TABLE string)
  lands in args=, never as a substring of the SQL passed to spark.sql.
* execute_strategy dispatcher routes by mode + strategy and rejects
  unsupported strategies with AIDPF-4030.
* Target identifier allowlist rejects unsafe values.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    RenderedSql,
    RunContext,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.strategy_executors import (
    AIDPF_4030_UNSUPPORTED_STRATEGY,
    AIDPF_4031_TARGET_IDENTIFIER_REJECTED,
    StrategyExecutionResult,
    TargetIdentifierError,
    UnsupportedStrategyError,
    execute_merge,
    execute_replace,
    execute_strategy,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


NODE_YAML_REPLACE = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
refresh:
  seed:
    strategy: replace
"""

NODE_YAML_MERGE = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
      watermark:
        column: _extract_ts
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    naturalKey: [thing_id]
    watermark:
      source: erp_thing
      column: _extract_ts
"""


def _load_node(yaml_text: str) -> NodeYaml:
    import yaml
    raw = yaml.safe_load(yaml_text)
    return NodeYaml.model_validate(raw)


def _default_ctx() -> RunContext:
    return RunContext(
        catalog="fusion_catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="run-2026-06-06-001",
        active_profile_name="finance-default",
        prior_watermark={},
        mode="seed",
        bronze_table_for_source={"erp_thing": "fusion_catalog.bronze.erp_thing"},
    )


def _fake_spark_with_row_count(row_count: int) -> MagicMock:
    """A MagicMock spark whose .sql() returns a DataFrame whose .collect()
    yields a single row with the desired row count."""
    spark = MagicMock(name="SparkSession")
    df = MagicMock(name="DataFrame")
    df.collect.return_value = [(row_count,)] if row_count is not None else []
    spark.sql.return_value = df
    return spark


def _fake_spark_for_merge(*, probe_rows: int, target_row_count: int) -> MagicMock:
    """Fake Spark for execute_merge. The .sql() mock returns different
    DataFrames per call:
    - First call (probe): DataFrame whose .collect() returns probe rows.
    - Subsequent calls: DataFrame whose .collect() returns the target
      row count.
    """
    spark = MagicMock(name="SparkSession")

    probe_df = MagicMock(name="ProbeDataFrame")
    probe_df.collect.return_value = [(1,)] * probe_rows  # any non-empty list signals "has rows"

    source_df = MagicMock(name="SourceDataFrame")
    # Used for createOrReplaceTempView and ensure_target_schema_for_merge
    source_df.createOrReplaceTempView = MagicMock()

    merge_result_df = MagicMock(name="MergeResultDataFrame")
    merge_result_df.collect.return_value = [(target_row_count,)]

    spark.sql.side_effect = [probe_df, source_df, MagicMock(), merge_result_df]
    return spark


# ---------------------------------------------------------------------------
# Target identifier allowlist
# ---------------------------------------------------------------------------


class TestTargetIdentifierAllowlist:
    def test_safe_three_part_target_accepted(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.strategy_executors import (
            _check_target_identifier,
        )
        assert _check_target_identifier("fusion_catalog.silver.dim_thing") == "fusion_catalog.silver.dim_thing"

    def test_unsafe_target_rejected(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.strategy_executors import (
            _check_target_identifier,
        )
        with pytest.raises(TargetIdentifierError) as exc_info:
            _check_target_identifier('evil"; DROP TABLE')
        assert AIDPF_4031_TARGET_IDENTIFIER_REJECTED in str(exc_info.value)

    def test_too_many_segments_rejected(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.strategy_executors import (
            _check_target_identifier,
        )
        with pytest.raises(TargetIdentifierError):
            _check_target_identifier("a.b.c.d")


# ---------------------------------------------------------------------------
# execute_replace
# ---------------------------------------------------------------------------


class TestExecuteReplace:
    def test_issues_create_or_replace_with_params(self) -> None:
        spark = _fake_spark_with_row_count(42)
        node = _load_node(NODE_YAML_REPLACE)
        rendered = RenderedSql(
            sql="SELECT 1 AS thing_id",
            params={"run_id": "run-x"},
            hash_input="h",
        )
        result = execute_replace(
            spark, node, rendered, "fusion_catalog.silver.dim_thing", _default_ctx()
        )

        # The first .sql() call must be the CREATE OR REPLACE statement
        # with args=rendered.params.
        first_call = spark.sql.call_args_list[0]
        stmt = first_call.args[0]
        kwargs = first_call.kwargs
        assert "CREATE OR REPLACE TABLE fusion_catalog.silver.dim_thing USING DELTA AS" in stmt
        assert "SELECT 1 AS thing_id" in stmt
        assert kwargs["args"] == {"run_id": "run-x"}

        assert isinstance(result, StrategyExecutionResult)
        assert result.strategy == "replace"
        assert result.rows_scanned == 42
        assert result.merge_skipped_empty_delta is False

    def test_param_binding_contains_malicious_string_only_in_args(self) -> None:
        """A profile-string value containing SQL injection must land in
        ``args=`` only — NOT inlined into the SQL statement.
        """
        spark = _fake_spark_with_row_count(0)
        node = _load_node(NODE_YAML_REPLACE)
        rendered = RenderedSql(
            sql="SELECT :profile_v AS x",
            params={"profile_v": "'; DROP TABLE evil; --"},
            hash_input="h",
        )
        execute_replace(spark, node, rendered, "fusion_catalog.silver.dim_thing", _default_ctx())

        first_call = spark.sql.call_args_list[0]
        stmt = first_call.args[0]
        kwargs = first_call.kwargs
        # The malicious payload must NOT be inlined into the SQL.
        assert "DROP TABLE" not in stmt
        # It MUST appear in args.
        assert "DROP TABLE" in kwargs["args"]["profile_v"]

    def test_unsafe_target_rejected_before_spark_call(self) -> None:
        spark = _fake_spark_with_row_count(0)
        node = _load_node(NODE_YAML_REPLACE)
        rendered = RenderedSql(sql="SELECT 1", params={}, hash_input="h")
        with pytest.raises(TargetIdentifierError):
            execute_replace(spark, node, rendered, 'evil"; DROP TABLE', _default_ctx())
        # Spark.sql must not have been called.
        spark.sql.assert_not_called()


# ---------------------------------------------------------------------------
# execute_merge — empty delta short-circuit + full MERGE
# ---------------------------------------------------------------------------


class TestExecuteMergeEmptyDelta:
    def test_empty_source_skips_merge_and_flags_result(self) -> None:
        """When the probe returns 0 rows, no MERGE is issued and the
        result flags merge_skipped_empty_delta=True so the caller can
        preserve the prior watermark (PLAN §11.7)."""
        spark = MagicMock(name="SparkSession")
        empty_probe = MagicMock(name="EmptyProbe")
        empty_probe.collect.return_value = []  # 0 rows
        spark.sql.return_value = empty_probe

        node = _load_node(NODE_YAML_MERGE)
        rendered = RenderedSql(
            sql="SELECT thing_id FROM staging WHERE 1=0",
            params={"watermark_erp_thing": "2026-06-01"},
            hash_input="h",
        )
        result = execute_merge(
            spark, node, rendered, "fusion_catalog.silver.dim_thing", _default_ctx()
        )

        assert result.strategy == "merge"
        assert result.merge_skipped_empty_delta is True
        assert result.rows_scanned == 0

        # Only the probe SELECT should have been issued. No MERGE INTO.
        all_stmts = [c.args[0] for c in spark.sql.call_args_list]
        assert any("LIMIT 1" in s for s in all_stmts)
        assert not any("MERGE INTO" in s for s in all_stmts)

    def test_empty_probe_uses_args_for_params(self) -> None:
        spark = MagicMock(name="SparkSession")
        empty_probe = MagicMock(name="EmptyProbe")
        empty_probe.collect.return_value = []
        spark.sql.return_value = empty_probe

        node = _load_node(NODE_YAML_MERGE)
        rendered = RenderedSql(
            sql="SELECT * FROM s WHERE _extract_ts > :watermark_erp_thing",
            params={"watermark_erp_thing": "2026-06-01"},
            hash_input="h",
        )
        execute_merge(spark, node, rendered, "fusion_catalog.silver.dim_thing", _default_ctx())

        first_call = spark.sql.call_args_list[0]
        assert first_call.kwargs["args"] == {"watermark_erp_thing": "2026-06-01"}


class TestExecuteMergeNonEmptyDelta:
    """The non-empty-delta path that actually reaches schema-reconcile +
    MERGE. Previously uncovered — every merge test used the empty-delta
    early return, so a wrong-signature call to the schema-reconcile helper
    (``target_table=``/``source_df=`` instead of
    ``target=``/``source_columns=``/``source_schema_struct=``) slipped past
    unit tests and only blew up on a live incremental (TypeError)."""

    def test_nonempty_delta_calls_reconcile_with_real_signature(self) -> None:
        """REGRESSION: the executor must call the schema-reconcile helper
        with kwargs that bind to the REAL
        ``state._ensure_target_schema_for_merge`` signature. The spy binds
        every call against that signature, so a wrong-keyword call raises
        TypeError here (as it did live) instead of passing silently."""
        import inspect
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            merge_helpers,
            state,
        )

        real_sig = inspect.signature(state._ensure_target_schema_for_merge)
        seen: dict[str, bool] = {}

        def _signature_checking_spy(*args, **kwargs):  # type: ignore[no-untyped-def]
            real_sig.bind(*args, **kwargs)  # TypeError if the executor passes bad kwargs
            seen["called"] = True
            return MagicMock(name="SchemaReconcileResult")

        spark = _fake_spark_for_merge(probe_rows=3, target_row_count=10)
        node = _load_node(NODE_YAML_MERGE)
        rendered = RenderedSql(
            sql="SELECT * FROM s WHERE _extract_ts > :watermark_erp_thing",
            params={"watermark_erp_thing": "2026-06-01"},
            hash_input="h",
        )

        with patch.object(
            merge_helpers, "ensure_target_schema_for_merge", _signature_checking_spy
        ):
            result = execute_merge(
                spark, node, rendered, "fusion_catalog.silver.dim_thing", _default_ctx()
            )

        assert seen.get("called") is True, "schema-reconcile was never reached"
        assert result.strategy == "merge"
        assert result.merge_skipped_empty_delta is False
        assert result.rows_scanned == 10
        # A real MERGE INTO must have been issued (not the empty-delta skip).
        all_stmts = [c.args[0] for c in spark.sql.call_args_list]
        assert any("MERGE INTO" in s for s in all_stmts)


# ---------------------------------------------------------------------------
# execute_strategy dispatcher
# ---------------------------------------------------------------------------


class TestExecuteStrategyDispatcher:
    def test_seed_mode_dispatches_to_replace(self) -> None:
        spark = _fake_spark_with_row_count(5)
        node = _load_node(NODE_YAML_REPLACE)
        rendered = RenderedSql(sql="SELECT 1", params={}, hash_input="h")
        result = execute_strategy(
            spark,
            node=node,
            rendered=rendered,
            target="cat.silver.t",
            ctx=_default_ctx(),
            mode="seed",
        )
        assert result.strategy == "replace"

    def test_incremental_mode_dispatches_to_merge(self) -> None:
        # Empty delta → quick return; we just need to confirm the dispatch.
        spark = MagicMock()
        empty_probe = MagicMock()
        empty_probe.collect.return_value = []
        spark.sql.return_value = empty_probe

        node = _load_node(NODE_YAML_MERGE)
        rendered = RenderedSql(
            sql="SELECT 1 AS thing_id",
            params={},
            hash_input="h",
        )
        result = execute_strategy(
            spark,
            node=node,
            rendered=rendered,
            target="cat.silver.t",
            ctx=_default_ctx(),
            mode="incremental",
        )
        assert result.strategy == "merge"

    def test_unsupported_strategy_raises_4030(self) -> None:
        """Construct a node fixture whose schema is fine but whose
        strategy isn't in the supported set. (Schema-level validation
        accepts ``append`` even though Phase 2 doesn't execute it.)"""
        node_yaml = NODE_YAML_REPLACE.replace("strategy: replace", "strategy: append").replace(
            "outputSchema:",
            # append strategy requires uniqueOn / unique_natural_key per R8 — add it.
            "outputSchema:\n",
        )
        # Build via a workaround: load and then swap seed strategy.
        node = _load_node(NODE_YAML_REPLACE)
        object.__setattr__(node.refresh.seed, "strategy", "append")

        spark = MagicMock()
        rendered = RenderedSql(sql="SELECT 1", params={}, hash_input="h")
        with pytest.raises(UnsupportedStrategyError) as exc_info:
            execute_strategy(
                spark,
                node=node,
                rendered=rendered,
                target="cat.silver.t",
                ctx=_default_ctx(),
                mode="seed",
            )
        assert AIDPF_4030_UNSUPPORTED_STRATEGY in str(exc_info.value)

    def test_incremental_mode_without_incremental_block_errors(self) -> None:
        node = _load_node(NODE_YAML_REPLACE)  # has only seed.strategy
        spark = MagicMock()
        rendered = RenderedSql(sql="SELECT 1", params={}, hash_input="h")
        with pytest.raises(ValueError, match="no refresh.incremental"):
            execute_strategy(
                spark,
                node=node,
                rendered=rendered,
                target="cat.silver.t",
                ctx=_default_ctx(),
                mode="incremental",
            )
