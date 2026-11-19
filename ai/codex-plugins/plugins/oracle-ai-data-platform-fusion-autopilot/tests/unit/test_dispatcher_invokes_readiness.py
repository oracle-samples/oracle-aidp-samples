"""The content-pack backend invokes the bronze readiness gate.

As of Option A, the gate fires automatically for **silver/gold-only
runs** (marts in scope, no bronze nodes this run → bronze pre-exists in
AIDP): it ``DESCRIBE``s the landed bronze tables and batch-validates
every in-scope mart's required columns BEFORE any mart runs. ``dry_run``
still skips it (no DESCRIBE work during planning). Full seeds (bronze in
scope) do NOT fire it — the pre-extraction PVO source gate already
fail-fasts them, and an all-or-nothing gate would regress their
per-node cascade. (The legacy ``enable_bronze_readiness_gate`` flag still
force-enables it for back-compat callers.)

When the gate raises, the dispatcher catches it, appends a synthetic
gate-failure ``RunStep`` to the merged ``RunSummary``, and returns
normally (no raise across the CLI boundary). The CLI translates the
failed step to a non-zero exit via the standard
``summary.has_failures()`` check.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    _run_content_pack_backend,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.bronze_readiness import (
    AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
    BronzeReadinessGateError,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    make_filesystem_base_resolver,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile,
)
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_BUNDLE = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project" / "bundle.yaml"
FIXTURE_PACK = REPO_ROOT / "tests" / "fixtures" / "content_packs" / "phase2_test_pack"
FIXTURE_PROFILE = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project" / "profiles" / "phase2-fixture.yaml"


@pytest.fixture
def pack():
    return load_full_chain(
        FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK),
    )


@pytest.fixture
def profile():
    return load_tenant_profile(FIXTURE_PROFILE)


class TestDispatcherInvokesReadinessGate:
    def test_gate_auto_invoked_for_silver_gold_only(self, monkeypatch, pack, profile) -> None:
        """Option A: a silver/gold-only run (marts in scope, NO bronze this
        run → bronze pre-exists) auto-fires the readiness gate even with
        ``enable_bronze_readiness_gate=False`` — to validate the landed
        bronze tables before any mart runs."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            bronze_readiness, sql_runner,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            state as v1_state, state_phase2,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        gate_calls = []
        # Stub to record + pass (don't run the real DESCRIBE against the mock).
        monkeypatch.setattr(
            bronze_readiness, "assert_bronze_readiness",
            lambda *a, **kw: gate_calls.append(kw),
        )

        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        monkeypatch.setattr(
            sql_runner, "execute_node",
            lambda *a, **kw: NodeExecutionResult(status="success", row_count=0),
        )
        fake_spark = MagicMock(name="FakeSpark")
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        _run_content_pack_backend(
            bundle_path=FIXTURE_BUNDLE,
            spark=None,
            mode="seed",
            datasets=None,
            layers=["silver", "gold"],
            dry_run=False,
            resume_run_id=None,
            resolved_pack=pack,
            tenant_profile=profile,
            enable_bronze_readiness_gate=False,  # auto-enabled for silver/gold-only
        )
        assert len(gate_calls) == 1, (
            "readiness gate did not auto-fire for a silver/gold-only run"
        )
        assert gate_calls[0]["cp_filter"] == (None, ["silver", "gold"])

    def test_gate_NOT_invoked_in_dry_run(self, monkeypatch, pack, profile) -> None:
        """``dry_run=True`` MUST skip the gate — no Spark DESCRIBE work
        during planning."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import bronze_readiness

        gate_calls = []
        monkeypatch.setattr(
            bronze_readiness, "assert_bronze_readiness",
            lambda *a, **kw: gate_calls.append(kw),
        )

        _run_content_pack_backend(
            bundle_path=FIXTURE_BUNDLE,
            spark=None,
            mode="seed",
            datasets=None,
            layers=["silver", "gold"],
            dry_run=True,
            resume_run_id=None,
            resolved_pack=pack,
            tenant_profile=profile,
            enable_bronze_readiness_gate=True,  # still ignored under dry_run
        )
        assert gate_calls == []

    def test_gate_failure_returns_summary_without_silver_gold_state(
        self, monkeypatch, pack, profile,
    ) -> None:
        """When the gate raises, the dispatcher catches the exception,
        appends a synthetic gate-failure RunStep, and RETURNS the
        summary. No silver/gold state row is written for any node.
        """
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            bronze_readiness, sql_runner, state as v1_state, state_phase2,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )

        execute_node_calls = []
        monkeypatch.setattr(
            sql_runner, "execute_node",
            lambda *a, **kw: (execute_node_calls.append(kw) or
                              NodeExecutionResult(status="success", row_count=0)),
        )

        fake_spark = MagicMock(name="FakeSpark")
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        # Force the gate to raise.
        def fake_gate(*args, **kwargs):
            raise BronzeReadinessGateError(
                message="test forced failure",
                gaps={"erp_thing": {"table_missing": True}},
            )
        monkeypatch.setattr(bronze_readiness, "assert_bronze_readiness", fake_gate)

        summary = _run_content_pack_backend(
            bundle_path=FIXTURE_BUNDLE,
            spark=None,
            mode="seed",
            datasets=None,
            layers=["silver", "gold"],
            dry_run=False,
            resume_run_id=None,
            resolved_pack=pack,
            tenant_profile=profile,
            enable_bronze_readiness_gate=True,
        )

        # Returned summary contains the gate-failure step.
        assert len(summary.steps) == 1
        step = summary.steps[0]
        assert step.status == "failed"
        assert step.dataset_id == "__bronze_readiness_gate__"
        assert step.layer == "silver"
        assert AIDPF_2071_BRONZE_READINESS_GATE_FAILED in (step.error_message or "")
        # No real node was dispatched (execute_node was not called).
        assert execute_node_calls == [], (
            "execute_node was called after gate failure — silver/gold "
            "MUST NOT run when bronze readiness fails"
        )


class TestMartOnlyDetection:
    """Mart-only detection keys off the REQUESTED layers (operator intent),
    not plan contents: ``--layers silver,gold`` = bronze not in scope →
    bronze is NOT re-seeded; the readiness gate validates landed tables."""

    def test_silver_gold_layers_is_mart_only(self):
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import _is_mart_only_run
        assert _is_mart_only_run(["silver", "gold"]) is True
        assert _is_mart_only_run(["silver"]) is True
        assert _is_mart_only_run(["gold"]) is True

    def test_bronze_in_layers_is_not_mart_only(self):
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import _is_mart_only_run
        # bronze requested → full run → bronze IS seeded
        assert _is_mart_only_run(["bronze", "silver", "gold"]) is False
        assert _is_mart_only_run(["bronze"]) is False  # bronze-only

    def test_no_layer_filter_is_not_mart_only(self):
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import _is_mart_only_run
        # None / empty = all layers = full run
        assert _is_mart_only_run(None) is False
        assert _is_mart_only_run([]) is False
