"""Phase 5 — Step 2d Fusion PVO drift gate is callable through the
dispatcher contract.

The full integration (live BICC probe → ``extract_pvo(...).schema``
returning a real ``StructType``, the dispatcher actually calling
``assert_fusion_pvo_compatibility``) requires a real BICC connection
and is exercised only by the live parity-test trail
(``tests/parity/test_fusion_pvo_drift_e2e.py``, deferred —
requires real Spark + Fusion).

This test suite validates the helper's contract surface at unit
level: the gate raises ``FusionPvoDriftError`` carrying AIDPF-2072
and the dispatcher's ``RunStep.gate_failed`` factory accepts it.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.fusion_pvo_drift import (
    AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
    FusionPvoDriftError,
    assert_fusion_pvo_compatibility,
)
from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunStep


class TestPvoDriftGateContract:
    def test_gate_error_carries_aidpf_2072(self, tmp_path) -> None:
        # Trigger a drift directly — required column missing.
        with pytest.raises(FusionPvoDriftError) as exc:
            from unittest.mock import MagicMock
            fake_pack = MagicMock()
            fake_pack.silver = {}
            fake_pack.gold = {}

            class _FakeNode:
                id = "fake"
                layer = "silver"
                depends_on = type("D", (), {"bronze": [type("S", (), {"id": "ap_invoices"})()], "silver": []})()
                required_columns = {"ap_invoices": ["needed_col"]}
                refresh = type("R", (), {"incremental": None})()

            fake_pack.silver = {"fake": _FakeNode()}

            assert_fusion_pvo_compatibility(
                live_pvo_columns={"ap_invoices": {"other_col": "string"}},
                resolved_pack=fake_pack,
                cp_filter=(None, ["silver"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=None,
                run_id="r1",
                diagnostics_root=tmp_path / "diag",
            )
        assert AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED in str(exc.value)
        assert "needed_col" in str(exc.value)

    def test_gate_failed_runstep_factory_accepts_drift_error_payload(self) -> None:
        """The dispatcher's gate-failure RunStep factory accepts the
        AIDPF-2072 payload shape (the same shape AIDPF-2071 produces)."""
        step = RunStep.gate_failed(
            run_id="r1",
            mode="seed",
            layer="bronze",
            gate_dataset_id="__fusion_pvo_drift_gate__",
            aidpf_code=AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
            error_message="test PVO drift",
        )
        assert step.status == "failed"
        assert step.dataset_id == "__fusion_pvo_drift_gate__"
        assert step.layer == "bronze"
        assert AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED in (step.error_message or "")
