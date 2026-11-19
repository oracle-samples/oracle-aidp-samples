"""CLI-level run-verdict wiring (AC-4 / AC-6) — both execution paths.

End-to-end through the click CLI with the dispatch/orchestrator boundary
mocked: a valid ``--dry-run`` exits 0 with no failure block on BOTH paths
(rule R0 — ``RunSummary.empty()`` is zero steps + a POPULATED plan, exactly
the shape rule R4 would otherwise misclassify), and a job that reports
``SUCCESS`` while its summary carries a failed ``__coa_gate__`` step exits 1
with the ``RUN VERDICT: ABORTED`` block — the incident this feature closes.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from oracle_ai_data_platform_fusion_autopilot import cli
from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
    PlanNode,
    RunStep,
    RunSummary,
)

PLAN = (
    PlanNode(dataset_id="gl_coa", layer="bronze"),
    PlanNode(dataset_id="dim_account", layer="silver"),
)


def _step(dataset_id: str, status: str = "success", *, error_message=None,
          skip_reason=None) -> RunStep:
    return RunStep(
        run_id="run-1", dataset_id=dataset_id, layer="bronze", mode="seed",
        status=status, row_count=None, duration_seconds=0.0,
        error_message=error_message, watermark_used=None, last_watermark=None,
        skip_reason=skip_reason,
    )


def _summary(steps=(), *, expected=None) -> RunSummary:
    base = RunSummary.empty("test", "seed", plan=PLAN)
    return RunSummary(
        run_id="run-1", started_at=base.started_at,
        finished_at=base.finished_at, bundle_project="test", mode="seed",
        steps=tuple(steps), plan=PLAN,
        expected_terminal_node_ids=expected,
    )


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
    return tmp_path


class TestDryRunExitsZeroOnBothPaths:
    def test_dispatch_dry_run_populated_plan_exits_0(self, project) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            return_value=RunSummary.empty("test", "seed", plan=PLAN),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--dry-run"],
            )
        assert result.exit_code == 0, result.output
        assert "RUN VERDICT" not in result.output

    def test_inline_dry_run_populated_plan_exits_0(self, project) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            return_value=RunSummary.empty("test", "seed", plan=PLAN),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline", "--dry-run"],
            )
        assert result.exit_code == 0, result.output
        assert "RUN VERDICT" not in result.output


class TestJobSuccessNeverMasksTheRunVerdict:
    COA_GATE_STEP_KWARGS = dict(
        error_message=(
            "[AIDPF-2018] gl_coa has 41 active charts of accounts but the "
            "profile has only a singleton COA mapping. [AIDPF-2017] chart "
            "'138' contradiction."
        ),
    )

    def test_dispatch_success_with_coa_gate_abort_exits_1(self, project) -> None:
        summary = _summary(
            [
                _step("gl_coa"),
                _step("__coa_gate__", "failed", **self.COA_GATE_STEP_KWARGS),
            ],
            expected=("gl_coa", "dim_account"),
        )
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            return_value=summary,
        ):
            result = CliRunner().invoke(cli.main, ["run", "--mode", "seed"])
        assert result.exit_code == 1, result.output
        assert "RUN VERDICT: ABORTED" in result.output
        assert "AIDPF-2018" in result.output
        # §9.3.4b: P2 registered the flag — the executable remediation
        # loop is advertised (and introspection-verified as registered).
        assert "--resolve-coa-from-metadata" in result.output

    def test_inline_expected_node_without_row_is_unproven_exit_1(
        self, project,
    ) -> None:
        summary = _summary(
            [_step("gl_coa")], expected=("gl_coa", "dim_account"),
        )
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            return_value=summary,
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 1, result.output
        assert "RUN VERDICT: UNPROVEN" in result.output
        assert "AIDPF-4023" in result.output

    def test_completed_run_with_expected_set_exits_0(self, project) -> None:
        summary = _summary(
            [_step("gl_coa"), _step("dim_account")],
            expected=("gl_coa", "dim_account"),
        )
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            return_value=summary,
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 0, result.output
        assert "RUN VERDICT" not in result.output


class TestStatusFallbackPrintsBothQueries:
    def test_pyspark_unavailable_prints_per_dataset_and_reserved_queries(
        self, project, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import sys

        # `from pyspark.sql import SparkSession` must raise ImportError.
        monkeypatch.setitem(sys.modules, "pyspark", None)
        monkeypatch.setitem(sys.modules, "pyspark.sql", None)
        result = CliRunner().invoke(cli.main, ["status"])
        assert result.exit_code == 0, result.output
        assert "pyspark not available locally" in result.output
        # Per-dataset query (byte-identical filter) AND the reserved-row
        # verdict query are both offered for notebook execution.
        assert result.output.count("NOT LIKE") == 1
        assert "run-level verdict" in result.output
        assert "dataset_id LIKE" in result.output
