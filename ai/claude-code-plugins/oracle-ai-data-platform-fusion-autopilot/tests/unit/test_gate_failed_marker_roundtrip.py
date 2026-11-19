"""Phase 5 Step 5 — verify the synthetic gate-failure ``RunStep``
round-trips through the marker channel.

The dispatch package serialises ``RunSummary`` via ``to_marker_dict()``
and reconstructs on the laptop side via ``from_marker_dict()``. The
Phase 5 gate-failure RunStep (``RunStep.gate_failed(...)``) MUST
survive this round-trip without a marker schema change so REST-
dispatch invocations surface the gate failure to the operator.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.orchestrator.bronze_readiness import (
    AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
)
from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
    RunStep,
    RunSummary,
)


def _make_summary_with_gate_step() -> RunSummary:
    step = RunStep.gate_failed(
        run_id="phase5-roundtrip-id",
        mode="seed",
        layer="silver",
        gate_dataset_id="__bronze_readiness_gate__",
        aidpf_code=AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
        error_message="readiness gate failed for ['erp_thing']",
    )
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    return RunSummary(
        run_id="phase5-roundtrip-id",
        started_at=now,
        finished_at=now,
        bundle_project="roundtrip-test",
        mode="seed",
        steps=(step,),
    )


class TestGateFailedMarkerRoundTrip:
    def test_to_marker_dict_includes_gate_failed_step(self) -> None:
        summary = _make_summary_with_gate_step()
        payload = summary.to_marker_dict()
        assert "steps" in payload
        assert len(payload["steps"]) == 1
        step_dict = payload["steps"][0]
        assert step_dict["dataset_id"] == "__bronze_readiness_gate__"
        assert step_dict["status"] == "failed"
        assert AIDPF_2071_BRONZE_READINESS_GATE_FAILED in step_dict["error_message"]

    def test_round_trip_preserves_gate_failure_step(self) -> None:
        summary = _make_summary_with_gate_step()
        payload = summary.to_marker_dict()
        reconstructed = RunSummary.from_marker_dict(payload)
        assert reconstructed.run_id == summary.run_id
        assert len(reconstructed.steps) == 1
        step = reconstructed.steps[0]
        assert step.dataset_id == "__bronze_readiness_gate__"
        assert step.layer == "silver"
        assert step.status == "failed"
        assert AIDPF_2071_BRONZE_READINESS_GATE_FAILED in (step.error_message or "")
