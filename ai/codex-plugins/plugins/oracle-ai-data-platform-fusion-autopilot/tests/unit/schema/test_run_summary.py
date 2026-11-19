"""P1.5ε §Step 1b — schema/run_summary.py marker (de)serialization tests.

Locks the marker JSON contract that the dispatch package's notebook builder
emits cluster-side and that ``dispatch_via_rest`` parses laptop-side.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
    MARKER_SCHEMA_VERSION,
    PlanNode,
    RunStep,
    RunSummary,
)


def _make_step(**overrides) -> RunStep:
    base = dict(
        run_id="run-1",
        dataset_id="ap_invoices",
        layer="bronze",
        mode="seed",
        status="success",
        row_count=42,
        duration_seconds=1.5,
        error_message=None,
        watermark_used=None,
        last_watermark=datetime(2026, 6, 3, 14, 0, 0, tzinfo=timezone.utc),
        skip_reason=None,
        plan_hash="hash-abc",
        plan_snapshot="{...}",
    )
    base.update(overrides)
    return RunStep(**base)


def _make_summary(steps: tuple[RunStep, ...] = ()) -> RunSummary:
    return RunSummary(
        run_id="run-1",
        started_at=datetime(2026, 6, 3, 14, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 6, 3, 14, 5, 0, tzinfo=timezone.utc),
        bundle_project="cecl-finance-lake",
        mode="seed",
        steps=steps,
        recommendations=("consider X",),
    )


class TestRunStepMarker:
    def test_roundtrip_preserves_all_fields(self) -> None:
        original = _make_step()
        restored = RunStep.from_marker_dict(original.to_marker_dict())
        assert restored == original

    def test_datetime_serialized_as_iso_z(self) -> None:
        step = _make_step(
            last_watermark=datetime(2026, 6, 3, 14, 0, 0, tzinfo=timezone.utc),
        )
        payload = step.to_marker_dict()
        assert payload["last_watermark"] == "2026-06-03T14:00:00Z"

    def test_none_fields_preserved_as_none(self) -> None:
        step = _make_step(error_message=None, watermark_used=None, plan_hash=None)
        payload = step.to_marker_dict()
        assert payload["error_message"] is None
        assert payload["watermark_used"] is None
        assert payload["plan_hash"] is None
        restored = RunStep.from_marker_dict(payload)
        assert restored.error_message is None
        assert restored.watermark_used is None
        assert restored.plan_hash is None

    def test_naive_datetime_assumed_utc(self) -> None:
        # Defense in depth: a naive datetime that slips through from somewhere
        # is serialized as UTC rather than crashing.
        step = _make_step(
            last_watermark=datetime(2026, 6, 3, 14, 0, 0),
        )
        payload = step.to_marker_dict()
        assert payload["last_watermark"] == "2026-06-03T14:00:00Z"

    def test_missing_required_field_raises_value_error(self) -> None:
        payload = _make_step().to_marker_dict()
        payload.pop("run_id")
        with pytest.raises(ValueError, match="'run_id'"):
            RunStep.from_marker_dict(payload)


class TestRunSummaryDiagnostics:
    def test_diagnostics_roundtrip_through_marker(self) -> None:
        diag = {
            "schemaVersion": 1, "runId": "run-1", "tenant": "t",
            "errorCode": "AIDPF-4071", "errorMessage": "missing",
            "generatedAt": "2026-06-11T00:00:00+00:00",
            "node": "ap_payments",
            "datastore": "FscmTopModelAM...PaymentHistoryDistributionExtractPVO",
            "missingColumns": ["ApPayHistDistInvoicePaymentId"],
            "pvoColumns": [{"name": "ApPaymentHistDistsInvoicePaymentId",
                            "type": "decimal(18,0)", "nullable": True}],
        }
        summary = RunSummary(
            run_id="run-1",
            started_at=datetime(2026, 6, 3, 14, 0, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 6, 3, 14, 5, 0, tzinfo=timezone.utc),
            bundle_project="p", mode="seed", steps=(),
            diagnostics=(diag,),
        )
        restored = RunSummary.from_marker_dict(summary.to_marker_dict())
        assert restored.diagnostics == (diag,)

    def test_diagnostics_default_empty_when_absent(self) -> None:
        # A marker from a pre-diagnostics wheel has no "diagnostics" key.
        payload = _make_summary().to_marker_dict()
        payload.pop("diagnostics", None)
        restored = RunSummary.from_marker_dict(payload)
        assert restored.diagnostics == ()


class TestRunSummaryMarker:
    def test_empty_steps_roundtrip(self) -> None:
        summary = _make_summary(steps=())
        restored = RunSummary.from_marker_dict(summary.to_marker_dict())
        assert restored.run_id == summary.run_id
        assert restored.started_at == summary.started_at
        assert restored.finished_at == summary.finished_at
        assert restored.bundle_project == summary.bundle_project
        assert restored.mode == summary.mode
        assert restored.steps == ()
        assert restored.recommendations == ("consider X",)

    def test_multi_step_roundtrip(self) -> None:
        steps = (
            _make_step(dataset_id="ap_invoices", layer="bronze"),
            _make_step(dataset_id="dim_supplier", layer="silver"),
            _make_step(dataset_id="supplier_spend", layer="gold"),
        )
        summary = _make_summary(steps=steps)
        restored = RunSummary.from_marker_dict(summary.to_marker_dict())
        assert restored.steps == steps

    def test_marker_includes_schema_version(self) -> None:
        payload = _make_summary().to_marker_dict()
        assert payload["schema_version"] == MARKER_SCHEMA_VERSION

    def test_wrong_schema_version_rejected(self) -> None:
        payload = _make_summary().to_marker_dict()
        payload["schema_version"] = 99
        with pytest.raises(ValueError, match="unsupported schema_version"):
            RunSummary.from_marker_dict(payload)

    def test_missing_schema_version_rejected(self) -> None:
        payload = _make_summary().to_marker_dict()
        del payload["schema_version"]
        with pytest.raises(ValueError, match="unsupported schema_version"):
            RunSummary.from_marker_dict(payload)

    def test_plan_and_prereqs_not_round_tripped(self) -> None:
        # Marker payload deliberately omits plan/prereqs — they carry engine
        # spec objects on the in-process path and don't JSON-serialize.
        summary = _make_summary()
        payload = summary.to_marker_dict()
        assert "plan" not in payload
        assert "prereqs" not in payload
        restored = RunSummary.from_marker_dict(payload)
        assert restored.plan is None
        assert restored.prereqs is None

    def test_counter_properties_after_restore(self) -> None:
        steps = (
            _make_step(status="success"),
            _make_step(dataset_id="dim_supplier", status="failed", error_message="err"),
            _make_step(dataset_id="dim_account", status="skipped", skip_reason="cascade"),
        )
        restored = RunSummary.from_marker_dict(_make_summary(steps=steps).to_marker_dict())
        assert restored.succeeded == 1
        assert restored.failed == 1
        assert restored.skipped == 1


class TestBackCompatReExport:
    """The schema-level location is canonical; ``orchestrator.runtime``
    re-exports the names for back-compat. Identity must hold so existing
    ``isinstance`` checks keep working."""

    def test_runstep_identity_across_modules(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import (
            RunStep as FromRuntime,
        )
        assert FromRuntime is RunStep

    def test_runsummary_identity_across_modules(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import (
            RunSummary as FromRuntime,
        )
        assert FromRuntime is RunSummary

class TestPlanNode:
    def test_default_status_is_eligible(self) -> None:
        node = PlanNode(dataset_id="ap_invoices", layer="bronze")
        assert node.status == "eligible"
        assert node.reason is None

    def test_deferred_with_reason(self) -> None:
        node = PlanNode(
            dataset_id="dim_org",
            layer="silver",
            status="deferred",
            reason="P1.7",
        )
        assert node.layer == "silver"
        assert node.reason == "P1.7"

    def test_frozen(self) -> None:
        node = PlanNode(dataset_id="ap_invoices", layer="bronze")
        with pytest.raises(Exception):  # FrozenInstanceError subclass varies
            node.dataset_id = "other"  # type: ignore[misc]
