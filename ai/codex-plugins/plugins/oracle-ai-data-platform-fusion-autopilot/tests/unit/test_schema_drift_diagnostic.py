"""Tests for the Phase 3c schema-drift diagnostic models + writer.

Covers:

* Pydantic round-trip with full drift + affected-VP payload.
* Pydantic round-trip with empty datasetDeltas (the Phase 3c default
  shape — fingerprint-only drift; column-level diff lands in
  feature #3d).
* `errorCode` Literal rejects values other than `"AIDPF-2012"`.
* Writer creates the file under the expected path.
* Writer refuses overwrite.
* Unsafe `run_id` rejected.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    AIDPF_2012_SCHEMA_DRIFT_DETECTED,
    AffectedVariationPoint,
    ColumnTypeChange,
    DatasetSchemaDelta,
    DiagnosticArtifactAlreadyExistsError,
    ObservedColumn,
    SchemaDriftDiagnosticV1,
    SchemaDriftFailure,
    write_schema_drift_diagnostic,
)
from oracle_ai_data_platform_fusion_autopilot.schema.path_segment import (
    UnsafePathSegmentError,
)


_TS = datetime(2026, 6, 6, 12, 0, 0, tzinfo=timezone.utc)


def _drift_artifact(
    *,
    dataset_deltas: list[DatasetSchemaDelta] | None = None,
    affected_vps: list[AffectedVariationPoint] | None = None,
) -> SchemaDriftDiagnosticV1:
    return SchemaDriftDiagnosticV1(
        runId="cp-test-run-1",
        tenant="finance-default",
        errorCode=AIDPF_2012_SCHEMA_DRIFT_DETECTED,
        errorMessage="bronze schema fingerprint drift detected",
        generatedAt=_TS,
        schemaDrift=SchemaDriftFailure(
            priorFingerprint="sha256:" + "a" * 64,
            currentFingerprint="sha256:" + "b" * 64,
            pinnedAt=_TS,
            datasetDeltas=dataset_deltas or [],
            affectedVariationPoints=affected_vps or [],
        ),
    )


class TestSchemaDriftRoundTrip:
    def test_phase3c_default_fingerprint_only(self) -> None:
        """v0.3 default — datasetDeltas empty, affectedVariationPoints
        populated from the pinned-profile diff."""
        artifact = _drift_artifact(
            affected_vps=[
                AffectedVariationPoint(
                    name="invoice_currency_code",
                    kind="columnAliases",
                    pinnedCandidate="ApInvoicesInvoiceCurrencyCode",
                    stillExistsOnBronze=False,
                ),
            ],
        )
        payload = json.loads(artifact.model_dump_json(by_alias=True))
        assert payload["errorCode"] == "AIDPF-2012"
        assert payload["tenant"] == "finance-default"
        assert payload["schemaDrift"]["datasetDeltas"] == []
        vp = payload["schemaDrift"]["affectedVariationPoints"][0]
        assert vp["pinnedCandidate"] == "ApInvoicesInvoiceCurrencyCode"
        assert vp["stillExistsOnBronze"] is False
        # Re-validation round-trips.
        SchemaDriftDiagnosticV1.model_validate(payload)

    def test_full_payload_with_deltas(self) -> None:
        """Forward-compat shape (feature #3d will populate this)."""
        artifact = _drift_artifact(
            dataset_deltas=[
                DatasetSchemaDelta(
                    datasetId="ap_invoices",
                    addedColumns=[ObservedColumn(name="NewCol", type="string")],
                    removedColumns=[
                        ObservedColumn(name="ApInvoicesInvoiceCurrencyCode", type="string")
                    ],
                    typeChangedColumns=[
                        ColumnTypeChange(
                            name="AmountValue",
                            priorType="decimal(18,2)",
                            currentType="decimal(20,4)",
                        ),
                    ],
                ),
            ],
            affected_vps=[
                AffectedVariationPoint(
                    name="invoice_currency_code",
                    kind="columnAliases",
                    pinnedCandidate="ApInvoicesInvoiceCurrencyCode",
                    stillExistsOnBronze=False,
                ),
            ],
        )
        payload = json.loads(artifact.model_dump_json(by_alias=True))
        delta = payload["schemaDrift"]["datasetDeltas"][0]
        assert delta["datasetId"] == "ap_invoices"
        assert delta["addedColumns"][0]["name"] == "NewCol"
        assert delta["removedColumns"][0]["name"] == "ApInvoicesInvoiceCurrencyCode"
        assert delta["typeChangedColumns"][0]["priorType"] == "decimal(18,2)"

    def test_invalid_error_code_rejected(self) -> None:
        with pytest.raises(Exception):
            SchemaDriftDiagnosticV1(
                runId="r",
                tenant="t",
                errorCode="AIDPF-9999",  # not in Literal
                errorMessage="x",
                generatedAt=_TS,
                schemaDrift=SchemaDriftFailure(
                    priorFingerprint="p",
                    currentFingerprint="c",
                    pinnedAt=_TS,
                ),
            )

    def test_tenant_field_required(self) -> None:
        """Unlike AIDPF-1020 (tenant=None), AIDPF-2012 requires
        tenant — drift occurs in the context of a known tenant."""
        # Constructing with tenant=None must fail validation.
        with pytest.raises(Exception):
            SchemaDriftDiagnosticV1(
                runId="r",
                tenant=None,  # type: ignore[arg-type]
                errorCode="AIDPF-2012",
                errorMessage="x",
                generatedAt=_TS,
                schemaDrift=SchemaDriftFailure(
                    priorFingerprint="p",
                    currentFingerprint="c",
                    pinnedAt=_TS,
                ),
            )


class TestSchemaDriftWriter:
    def test_writes_under_diagnostics_subtree(self, tmp_path: Path) -> None:
        artifact = _drift_artifact()
        result = write_schema_drift_diagnostic(tmp_path, "cp-test-1", artifact)
        assert result == (
            tmp_path / ".aidp" / "diagnostics" / "cp-test-1" / "AIDPF-2012.json"
        )
        loaded = json.loads(result.read_text(encoding="utf-8"))
        assert loaded["errorCode"] == "AIDPF-2012"

    def test_refuses_overwrite(self, tmp_path: Path) -> None:
        artifact = _drift_artifact()
        write_schema_drift_diagnostic(tmp_path, "cp-test-2", artifact)
        with pytest.raises(DiagnosticArtifactAlreadyExistsError):
            write_schema_drift_diagnostic(tmp_path, "cp-test-2", artifact)

    @pytest.mark.parametrize(
        "run_id",
        ["../escape", "foo/bar", "..", ""],
    )
    def test_unsafe_run_id_rejected(
        self, tmp_path: Path, run_id: str
    ) -> None:
        artifact = _drift_artifact()
        with pytest.raises(UnsafePathSegmentError):
            write_schema_drift_diagnostic(tmp_path, run_id, artifact)
