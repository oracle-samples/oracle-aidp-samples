"""The gate and the metadata-arm verifier share ONE Tier-B probe definition.

The COA remediation safety claim — *"a derived arm is verified by the same
probe that caught the bad singleton"* — holds only if there is literally one
SQL implementation. These tests pin it:

* the SQL ``node_preflight._evaluate_coa`` issues (via ``coa_probe``) is
  byte-identical to what ``coa_probe``'s pure builders return for the same
  inputs, and
* the verifier-facing I/O helpers (``probe_chart`` / ``coa_chart_active``)
  issue exactly those built strings — no second formulation anywhere.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from oracle_ai_data_platform_fusion_autopilot.orchestrator import coa_probe
from oracle_ai_data_platform_fusion_autopilot.orchestrator.node_preflight import (
    _evaluate_coa,
)

TABLE = "cat.bronze.gl_coa"

GL_COA_COLUMNS = [
    "CodeCombinationCodeCombinationId",
    "CodeCombinationChartOfAccountsId",
    "CodeCombinationSegment1",
    "CodeCombinationSegment2",
    "CodeCombinationSegment3",
    "CodeCombinationAccountType",
    "CodeCombinationEnabledFlag",
]

SINGLETON_COA = {
    "default": {
        "balancingSegment": "CodeCombinationSegment1",
        "costCenterSegment": "CodeCombinationSegment2",
        "naturalAccountSegment": "CodeCombinationSegment3",
    }
}


def _recording_spark(chart_rows: dict[str, int], na_total: int = 500,
                     na_ambiguous: int = 0):
    """Fake Spark that RECORDS every query verbatim while answering the
    DESCRIBE / chart-active / Tier-B shapes (same routing as the preflight
    gate's own test fixture)."""
    spark = MagicMock()
    queries: list[str] = []

    def _sql(query: str):
        queries.append(query)
        df = MagicMock()
        q = " ".join(query.split())
        if q.startswith("DESCRIBE TABLE"):
            df.collect.return_value = [(c, "string", None) for c in GL_COA_COLUMNS]
        elif "GROUP BY CAST(CodeCombinationChartOfAccountsId AS STRING)" in q:
            df.collect.return_value = list(chart_rows.items())
        else:  # Tier-B natural-account aggregate
            df.collect.return_value = [(na_total, na_ambiguous)]
        return df

    spark.sql.side_effect = _sql
    return spark, queries


class TestGateSqlIsTheSharedBuilderOutput:
    def test_chart_active_query_byte_identical(self) -> None:
        spark, queries = _recording_spark({"138": 5000})
        _evaluate_coa(spark, TABLE, "gl_coa", SINGLETON_COA, set())
        expected = coa_probe.chart_active_sql(TABLE)
        assert expected in queries, (
            "the gate's chart-active query must be exactly "
            "coa_probe.chart_active_sql(table)"
        )

    def test_tier_b_query_byte_identical_per_chart(self) -> None:
        spark, queries = _recording_spark({"138": 5000, "22625": 300})
        _evaluate_coa(spark, TABLE, "gl_coa", SINGLETON_COA, set())
        for chart_id in ("138", "22625"):
            expected = coa_probe.natural_account_probe_sql(
                TABLE, chart_id, "CodeCombinationSegment3"
            )
            assert expected in queries, (
                f"the gate's Tier-B query for chart {chart_id} must be exactly "
                f"coa_probe.natural_account_probe_sql(...)"
            )


class TestVerifierHelpersIssueTheSameStrings:
    def test_probe_chart_issues_the_built_sql(self) -> None:
        spark, queries = _recording_spark({})
        probe = coa_probe.probe_chart(
            spark, TABLE, chart_id="138", na_col="CodeCombinationSegment3",
            active_rows=5000,
        )
        assert queries == [
            coa_probe.natural_account_probe_sql(
                TABLE, "138", "CodeCombinationSegment3"
            )
        ]
        assert probe is not None
        assert probe.chart_id == "138"
        assert probe.active_row_count == 5000
        assert probe.natural_account_distinct == 500
        assert probe.natural_account_ambiguous == 0

    def test_coa_chart_active_issues_the_built_sql(self) -> None:
        spark, queries = _recording_spark({"138": 5000, "9001": 3})
        active = coa_probe.coa_chart_active(spark, TABLE)
        assert queries == [coa_probe.chart_active_sql(TABLE)]
        assert active == {"138": 5000, "9001": 3}


class TestChartProbeFromRow:
    def test_null_coalescing_matches_gate_behaviour(self) -> None:
        probe = coa_probe.chart_probe_from_row("138", 10, (None, None))
        assert probe.natural_account_distinct == 0
        assert probe.natural_account_ambiguous == 0
        assert probe.active_row_count == 10

    def test_values_pass_through(self) -> None:
        probe = coa_probe.chart_probe_from_row("138", 10, (402, 212))
        assert probe.natural_account_distinct == 402
        assert probe.natural_account_ambiguous == 212


class TestProbeCharts:
    ARMS = {"default": {"coa.natural_account": "CodeCombinationSegment3"}}

    def test_orders_by_active_rows_desc_and_respects_cap(self) -> None:
        spark, queries = _recording_spark({})
        chart_active = {"a": 10, "b": 300, "c": 200}
        probes = coa_probe.probe_charts(
            spark, TABLE, self.ARMS, chart_active, max_charts=2,
        )
        assert set(probes) == {"b", "c"}  # largest active counts first
        assert queries == [
            coa_probe.natural_account_probe_sql(TABLE, "b", "CodeCombinationSegment3"),
            coa_probe.natural_account_probe_sql(TABLE, "c", "CodeCombinationSegment3"),
        ]

    def test_per_chart_failure_is_fail_soft_absence(self) -> None:
        spark = MagicMock()

        def _sql(query: str):
            if "'boom'" in query:
                raise RuntimeError("constrained session")
            df = MagicMock()
            df.collect.return_value = [(50, 0)]
            return df

        spark.sql.side_effect = _sql
        probes = coa_probe.probe_charts(
            spark, TABLE, self.ARMS, {"boom": 100, "ok": 90},
        )
        assert set(probes) == {"ok"}  # absent, never a fabricated pass

    def test_chart_without_natural_account_arm_is_skipped(self) -> None:
        spark, queries = _recording_spark({})
        probes = coa_probe.probe_charts(
            spark, TABLE, {"default": {}}, {"138": 100},
        )
        assert probes == {}
        assert queries == []
