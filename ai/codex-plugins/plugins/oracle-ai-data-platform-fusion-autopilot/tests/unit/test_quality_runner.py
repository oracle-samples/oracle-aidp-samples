"""Unit tests for ``orchestrator/quality_runner.py`` (Phase 2 Step 8)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.quality_runner import (
    AIDPF_8010_QUALITY_TEST_FAILED,
    AIDPF_8011_QUALITY_TEST_DEFERRED,
    QualityReport,
    run_quality_tests,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import RunContext
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml


def _ctx() -> RunContext:
    return RunContext(
        catalog="c", bronze_schema="b", silver_schema="s", gold_schema="g", run_id="r",
        active_profile_name="finance-default",
    )


def _node_with_quality(tests_yaml: str) -> NodeYaml:
    yaml_text = f"""
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
quality:
  tests:
{tests_yaml}
"""
    return NodeYaml.model_validate(yaml.safe_load(yaml_text))


# ---------------------------------------------------------------------------
# No tests declared — empty report
# ---------------------------------------------------------------------------


class TestNoTestsDeclared:
    def test_no_quality_block_returns_empty_report(self) -> None:
        node_yaml = """
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
        node = NodeYaml.model_validate(yaml.safe_load(node_yaml))
        target_df = MagicMock()
        report = run_quality_tests(MagicMock(), node, target_df, _ctx())
        assert report.ok
        assert report.failures == ()
        assert report.passed == ()
        assert report.deferred == ()
        target_df.count.assert_not_called()  # No tests = no probe.


# ---------------------------------------------------------------------------
# Implemented test types
# ---------------------------------------------------------------------------


class TestImplementedTests:
    def test_row_count_min_passes(self) -> None:
        node = _node_with_quality("    - type: row_count_min\n      min: 5\n")
        target_df = MagicMock()
        target_df.count.return_value = 10
        report = run_quality_tests(MagicMock(), node, target_df, _ctx())
        assert report.ok
        assert len(report.passed) == 1
        assert report.passed[0].test_type == "row_count_min"

    def test_row_count_min_fails(self) -> None:
        node = _node_with_quality("    - type: row_count_min\n      min: 100\n")
        target_df = MagicMock()
        target_df.count.return_value = 3
        report = run_quality_tests(MagicMock(), node, target_df, _ctx())
        assert not report.ok
        assert report.failures[0].test_type == "row_count_min"
        assert AIDPF_8010_QUALITY_TEST_FAILED in report.failures[0].message
        assert report.failures[0].failing_row_count == 97

    def test_not_null_unique_accepted_values_use_pyspark_helpers(self, monkeypatch) -> None:
        """The runner imports ``pyspark.sql.functions`` lazily inside each
        runner. We can't easily fake a full pyspark stack, but we can
        confirm the dispatcher routes to the correct branch by stubbing
        a result and asserting the test result's type and status.

        Real pyspark integration is exercised in Step 13's integration
        tests (gated by AIDP_FUSION_AUTOPILOT_RUN_SPARK_TESTS=1).
        """
        # row_count_min uses no pyspark helpers — covered above. The other
        # three (not_null/unique/accepted_values) require pyspark to be
        # importable. If pyspark is installed in the venv, the tests run
        # for real; if not, this test skips.
        pytest.importorskip("pyspark")

        # not_null happy path
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("qruntest").getOrCreate()
        try:
            target = spark.createDataFrame([(1, "a"), (2, "b")], ["thing_id", "name"])
            node = _node_with_quality(
                "    - type: not_null\n      columns: [thing_id, name]\n"
            )
            report = run_quality_tests(spark, node, target, _ctx())
            assert report.ok
            assert len(report.passed) == 1
            assert report.passed[0].test_type == "not_null"
        finally:
            spark.stop()

    def test_not_null_fails_on_null_value(self) -> None:
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("qrtnn").getOrCreate()
        try:
            target = spark.createDataFrame([(1, "a"), (2, None)], ["thing_id", "name"])
            node = _node_with_quality(
                "    - type: not_null\n      columns: [name]\n"
            )
            report = run_quality_tests(spark, node, target, _ctx())
            assert not report.ok
            assert report.failures[0].test_type == "not_null"
            assert report.failures[0].failing_row_count == 1
        finally:
            spark.stop()

    def test_unique_fails_on_duplicate_key(self) -> None:
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("qrtu").getOrCreate()
        try:
            target = spark.createDataFrame(
                [(1, "a"), (1, "b"), (2, "c")], ["thing_id", "name"]
            )
            node = _node_with_quality(
                "    - type: unique\n      columns: [thing_id]\n"
            )
            report = run_quality_tests(spark, node, target, _ctx())
            assert not report.ok
            assert report.failures[0].test_type == "unique"
        finally:
            spark.stop()

    def test_accepted_values_fails_on_outside_value(self) -> None:
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("qrtav").getOrCreate()
        try:
            target = spark.createDataFrame(
                [(1, "active"), (2, "inactive"), (3, "frobnicated")],
                ["thing_id", "status"],
            )
            node = _node_with_quality(
                "    - type: accepted_values\n      column: status\n      values: [active, inactive]\n"
            )
            report = run_quality_tests(spark, node, target, _ctx())
            assert not report.ok
            assert report.failures[0].test_type == "accepted_values"
            assert report.failures[0].failing_row_count == 1
        finally:
            spark.stop()


# ---------------------------------------------------------------------------
# Deferred test types — recognised but reported as deferred
# ---------------------------------------------------------------------------


class TestDeferredTests:
    @pytest.mark.parametrize(
        "test_type, extra_fields",
        [
            ("freshness", "      column: _silver_built_at\n      maxAgeHours: 24\n"),
            ("row_count_delta", "      tolerancePct: 10.0\n"),
            ("reconcile_to", "      source: erp_thing\n      aggregate: SUM(amount)\n"),
            (
                "referential_integrity",
                "      column: supplier_id\n      references: dim_supplier.supplier_id\n",
            ),
            (
                "custom",
                "      implementation: my.module:my_check\n      args: {threshold: 5}\n",
            ),
        ],
    )
    def test_deferred_test_type_reported_not_executed(
        self, test_type: str, extra_fields: str
    ) -> None:
        tests_yaml = f"    - type: {test_type}\n{extra_fields}"
        node = _node_with_quality(tests_yaml)
        target_df = MagicMock()  # Never touched for deferred tests.
        report = run_quality_tests(MagicMock(), node, target_df, _ctx())
        assert report.ok  # Deferred tests don't fail.
        assert len(report.deferred) == 1
        assert report.deferred[0].test_type == test_type
        assert report.deferred[0].status == "deferred"
        assert AIDPF_8011_QUALITY_TEST_DEFERRED in report.deferred[0].message
        # The runner did NOT probe the DataFrame for deferred tests.
        target_df.count.assert_not_called()

    def test_deferred_tests_do_not_block_cursor(self) -> None:
        """PLAN §11.9: deferred results are informational, not blocking."""
        node = _node_with_quality(
            "    - type: freshness\n      column: x\n      maxAgeHours: 24\n"
        )
        report = run_quality_tests(MagicMock(), node, MagicMock(), _ctx())
        # report.ok == True; caller treats as 'safe to advance cursor'.
        assert report.ok


# ---------------------------------------------------------------------------
# Mixed report — implemented passes + deferred + implemented fails
# ---------------------------------------------------------------------------


class TestMixedReports:
    def test_mixed_results_categorised_correctly(self) -> None:
        node = _node_with_quality(
            "    - type: row_count_min\n      min: 5\n"
            "    - type: freshness\n      column: x\n      maxAgeHours: 24\n"
            "    - type: row_count_min\n      min: 1000\n"
        )
        target_df = MagicMock()
        target_df.count.return_value = 10  # Passes 5, fails 1000.

        report = run_quality_tests(MagicMock(), node, target_df, _ctx())
        assert len(report.passed) == 1
        assert len(report.failures) == 1
        assert len(report.deferred) == 1
        assert not report.ok  # ANY failure means not ok.
