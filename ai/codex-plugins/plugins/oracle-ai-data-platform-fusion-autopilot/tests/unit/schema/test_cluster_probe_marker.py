"""Unit tests for ``schema.cluster_probe_marker`` (Phase 4.1 / D3).

Covers the envelope discrimination, walker-outcome shape invariants,
``Literal[1]`` marker version gating, and base64/JSON round-trip
behaviour the cluster→laptop hop relies on.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    ColumnInfo,
)
from oracle_ai_data_platform_fusion_autopilot.schema.cluster_probe_marker import (
    CandidateAttemptMarker,
    ClusterProbeEnvelope,
    ClusterProbeMarker,
    ColumnInfoMarker,
    WalkerOutcomeMarker,
)


# ---------------------------------------------------------------------------
# Builders — terse helpers so the assertions read clearly
# ---------------------------------------------------------------------------


def _marker(**overrides) -> ClusterProbeMarker:
    base = dict(
        markerVersion=1,
        tenant="demo-pod",
        bronzeFingerprint="sha256:abc123",
        observedSchema={"erp_suppliers": [{"name": "Segment1", "type": "string"}]},
        walkerResults=[
            {
                "name": "supplier_natural_key",
                "kind": "columnAliases",
                "outcome": "auto_resolved",
                "chosen": "SEGMENT1",
            }
        ],
        dispatchedAt=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc).isoformat(),
    )
    base.update(overrides)
    return ClusterProbeMarker.model_validate(base)


class TestColumnInfoMarker:
    def test_round_trips_with_canonical_columninfo(self) -> None:
        ci = ColumnInfo(name="Segment1", type="string", nullable=True)
        marker = ColumnInfoMarker.from_column_info(ci)
        assert marker.name == "Segment1"
        assert marker.type == "string"
        assert marker.nullable is True
        # Round-trip back.
        assert marker.to_column_info() == ci

    def test_rejects_unknown_field(self) -> None:
        with pytest.raises(ValidationError):
            ColumnInfoMarker.model_validate(
                {"name": "x", "type": "string", "nullable": True, "extra": "no"}
            )


class TestClusterProbeMarker:
    def test_marker_version_literal_rejects_future_version(self) -> None:
        # The whole point of the pinned Literal over `int` — a future
        # cluster emitting an UNKNOWN markerVersion must NOT silently
        # coerce on the laptop. Version 2 became VALID with the optional
        # coaMetadata section (feature coa-mapping-auto-remediation), so
        # the unknown-version property now pins at 3.
        with pytest.raises(ValidationError) as exc:
            _marker(markerVersion=3)
        assert "markerVersion" in str(exc.value) or "Literal" in str(exc.value)

    def test_marker_version_2_is_valid_with_optional_coa_metadata(self) -> None:
        marker = _marker(markerVersion=2)
        assert marker.marker_version == 2
        assert marker.coa_metadata is None  # v2 with the section absent is fine

    def test_marker_version_default_is_1(self) -> None:
        # Constructor-level default still works (used in tests + the
        # cluster cell when authors don't pass the field explicitly).
        marker = _marker()
        assert marker.marker_version == 1

    def test_aliased_fields_round_trip(self) -> None:
        marker = _marker()
        dumped = marker.model_dump(by_alias=True, mode="json")
        # By-alias keys are what go on the wire.
        assert "markerVersion" in dumped
        assert "bronzeFingerprint" in dumped
        assert "observedSchema" in dumped
        assert "walkerResults" in dumped
        assert "dispatchedAt" in dumped
        # Re-validate from the wire form.
        restored = ClusterProbeMarker.model_validate(dumped)
        assert restored.tenant == marker.tenant
        assert restored.bronze_fingerprint == marker.bronze_fingerprint


class TestWalkerOutcomeMarker:
    def test_auto_resolved_requires_chosen(self) -> None:
        with pytest.raises(ValidationError):
            WalkerOutcomeMarker.model_validate(
                {
                    "name": "supplier_natural_key",
                    "kind": "columnAliases",
                    "outcome": "auto_resolved",
                    # chosen missing
                }
            )

    def test_auto_resolved_forbids_matched(self) -> None:
        with pytest.raises(ValidationError):
            WalkerOutcomeMarker.model_validate(
                {
                    "name": "supplier_natural_key",
                    "kind": "columnAliases",
                    "outcome": "auto_resolved",
                    "chosen": "SEGMENT1",
                    "matched": ["SEGMENT1", "Segment1"],  # cross-pollution
                }
            )

    def test_multi_match_requires_two_or_more(self) -> None:
        with pytest.raises(ValidationError):
            WalkerOutcomeMarker.model_validate(
                {
                    "name": "invoice_currency_code",
                    "kind": "columnAliases",
                    "outcome": "multi_match",
                    "matched": ["ApInvoicesCurrencyCode"],  # only 1
                }
            )

    def test_multi_match_forbids_chosen(self) -> None:
        with pytest.raises(ValidationError):
            WalkerOutcomeMarker.model_validate(
                {
                    "name": "invoice_currency_code",
                    "kind": "columnAliases",
                    "outcome": "multi_match",
                    "matched": ["A", "B"],
                    "chosen": "A",  # operator-decision belongs laptop-side
                }
            )

    def test_no_match_requires_candidates_tried(self) -> None:
        with pytest.raises(ValidationError):
            WalkerOutcomeMarker.model_validate(
                {
                    "name": "supplier_natural_key",
                    "kind": "columnAliases",
                    "outcome": "no_match",
                    # candidates_tried empty
                }
            )

    def test_no_match_with_attempts_validates(self) -> None:
        outcome = WalkerOutcomeMarker.model_validate(
            {
                "name": "supplier_natural_key",
                "kind": "columnAliases",
                "outcome": "no_match",
                "candidatesTried": [
                    {
                        "candidate": "SEGMENT1",
                        "outcome": "column_not_found",
                    },
                ],
            }
        )
        assert len(outcome.candidates_tried) == 1
        assert outcome.candidates_tried[0].candidate == "SEGMENT1"


class TestClusterProbeEnvelope:
    def test_ok_true_requires_marker(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ClusterProbeEnvelope.model_validate({"ok": True})
        assert "marker" in str(exc.value)

    def test_ok_true_forbids_error_fields(self) -> None:
        with pytest.raises(ValidationError):
            ClusterProbeEnvelope.model_validate(
                {
                    "ok": True,
                    "marker": _marker().model_dump(by_alias=True, mode="json"),
                    "errorType": "RuntimeError",  # cross-pollution
                }
            )

    def test_ok_false_requires_error_type(self) -> None:
        with pytest.raises(ValidationError) as exc:
            ClusterProbeEnvelope.model_validate({"ok": False})
        assert "errorType" in str(exc.value)

    def test_ok_false_forbids_marker(self) -> None:
        with pytest.raises(ValidationError):
            ClusterProbeEnvelope.model_validate(
                {
                    "ok": False,
                    "marker": _marker().model_dump(by_alias=True, mode="json"),
                    "errorType": "RuntimeError",
                }
            )

    def test_happy_path_round_trip_through_base64(self) -> None:
        # End-to-end: build envelope → JSON → base64 → decode → parse.
        # Mirrors what the cluster cell emits and what the laptop helper
        # decodes in plan.md Step 4 step 5.
        envelope = ClusterProbeEnvelope(ok=True, marker=_marker())
        wire = envelope.model_dump_json(by_alias=True)
        token = base64.b64encode(wire.encode("utf-8")).decode("ascii")
        # Laptop side:
        decoded = base64.b64decode(token).decode("utf-8")
        payload = json.loads(decoded)
        restored = ClusterProbeEnvelope.model_validate(payload)
        assert restored.ok is True
        assert restored.marker is not None
        assert restored.marker.tenant == "demo-pod"

    def test_error_envelope_round_trip(self) -> None:
        envelope = ClusterProbeEnvelope(
            ok=False,
            error_type="RuntimeError",
            error_message="bronze table missing",
            traceback="...",
        )
        wire = envelope.model_dump_json(by_alias=True)
        restored = ClusterProbeEnvelope.model_validate_json(wire)
        assert restored.ok is False
        assert restored.error_type == "RuntimeError"
        assert restored.marker is None
