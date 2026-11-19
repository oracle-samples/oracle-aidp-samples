"""The top-level dispatcher invokes the Fusion PVO drift gate (AIDPF-2072)
before the bronze branch.

Companion to ``test_dispatcher_invokes_pvo_drift.py`` (helper-level)
and ``test_dispatcher_invokes_readiness.py`` (sibling gate).
This module proves the dispatcher-side wiring: the gate runs first,
its failure short-circuits the run, and the bronze branch never
dispatches when the gate trips.

The full live-BICC end-to-end probe is the deferred parity-test trail
at ``tests/parity/test_fusion_pvo_drift_e2e.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    _dispatch_content_pack_run,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    make_filesystem_base_resolver,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.fusion_pvo_drift import (
    AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
)
from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunStep
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile,
)


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


class TestDispatcherRunsPvoDriftGate:
    def test_gate_failure_blocks_bronze_dispatch(
        self, monkeypatch, pack, profile,
    ) -> None:
        """When the PVO drift gate trips, the dispatcher returns a one-
        step ``RunSummary`` carrying the AIDPF-2072 gate-failure step
        and the recursive ``run()`` call for the bronze branch is NEVER
        invoked.
        """
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        # Spy + force the gate to "fail".
        gate_calls: list[dict] = []

        def fake_gate(*, run_id, mode, **kwargs):
            gate_calls.append({"run_id": run_id, "mode": mode, **kwargs})
            return RunStep.gate_failed(
                run_id=run_id,
                mode=mode,
                layer="bronze",
                gate_dataset_id="__fusion_pvo_drift_gate__",
                aidpf_code=AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
                error_message="test-forced PVO drift",
            )

        monkeypatch.setattr(_o, "_run_fusion_pvo_drift_gate", fake_gate)

        # If the dispatcher ever calls the recursive run() for bronze,
        # fail the test loudly. The gate fires BEFORE the bronze
        # branch — proving order means proving non-invocation here.
        bronze_run_calls: list[dict] = []
        original_run = _o.run

        def trap_run(*args, **kwargs):
            # Only the bronze branch's recursive call passes
            #. The outer call from
            # the test still routes through the patched dispatcher
            # via run() -> _dispatch_content_pack_run.
            if kwargs.get("execution_backend") == "legacy-python":
                bronze_run_calls.append(kwargs)
                raise AssertionError(
                    "bronze branch dispatched after PVO drift gate "
                    "failed — the gate MUST short-circuit the run"
                )
            return original_run(*args, **kwargs)

        monkeypatch.setattr(_o, "run", trap_run)

        summary = _dispatch_content_pack_run(
            bundle_path=FIXTURE_BUNDLE,
            spark=MagicMock(name="FakeSpark"),
            mode="seed",
            datasets=None,
            layers=None,
            resume_run_id=None,
            resolved_pack=pack,
            tenant_profile=profile,
            force_fingerprint_skip=False,
        )

        # Gate was called exactly once.
        assert len(gate_calls) == 1, (
            f"gate called {len(gate_calls)} times; expected exactly 1"
        )
        # Returned summary holds the gate-failure step only.
        assert len(summary.steps) == 1
        step = summary.steps[0]
        assert step.status == "failed"
        assert step.dataset_id == "__fusion_pvo_drift_gate__"
        assert step.layer == "bronze"
        assert AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED in (step.error_message or "")
        # Bronze branch never ran.
        assert bronze_run_calls == [], (
            "bronze branch executed despite the PVO drift gate failing"
        )

    def test_gate_NOT_invoked_when_bronze_out_of_scope(
        self, monkeypatch, pack, profile,
    ) -> None:
        """``--layers silver`` (or any silver/gold-only filter) keeps
        ``scope.bronze_filter`` ``None``. The PVO drift gate has no
        live PVO to probe against and MUST NOT run.
        """
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            sql_runner, state as v1_state, state_phase2, bronze_readiness,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )

        gate_calls: list[dict] = []
        monkeypatch.setattr(
            _o, "_run_fusion_pvo_drift_gate",
            lambda **kw: (gate_calls.append(kw) or None),
        )

        # Mock out everything below the dispatcher so the cp branch can
        # run to completion without Spark.
        fake_spark = MagicMock(name="FakeSpark")
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(
            sql_runner, "execute_node",
            lambda *a, **kw: NodeExecutionResult(status="success", row_count=0),
        )
        # Bypass the fingerprint gate; it needs a real backend.
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import preflight_evidence
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.preflight_evidence import (
            PreflightOutcome,
        )
        monkeypatch.setattr(
            preflight_evidence, "check_bronze_fingerprint_drift",
            lambda **kw: PreflightOutcome(
                kind="ok",
                diagnostic_path=None,
                summary=None,
                prior_fingerprint=None,
                current_fingerprint=None,
            ),
        )
        # No bronze readiness gate — silver/gold-only direct call.
        monkeypatch.setattr(
            bronze_readiness, "assert_bronze_readiness",
            lambda *a, **kw: None,
        )

        _dispatch_content_pack_run(
            bundle_path=FIXTURE_BUNDLE,
            spark=fake_spark,
            mode="seed",
            datasets=None,
            layers=["silver"],
            resume_run_id=None,
            resolved_pack=pack,
            tenant_profile=profile,
            force_fingerprint_skip=False,
        )
        assert gate_calls == [], (
            "PVO drift gate fired on a silver-only filter; the gate "
            "MUST only run when scope.bronze_filter is not None"
        )
