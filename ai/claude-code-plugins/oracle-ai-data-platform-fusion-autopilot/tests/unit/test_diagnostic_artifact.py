"""Unit tests for :mod:`oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact`.

Covers:

* Model round-trip via ``model_validate`` / ``model_dump_json`` for both
  ``VariationPointDiagnosticV1`` and ``IdentityDiagnosticV1``.
* Writer creates the file under the correct
  ``<workdir>/.aidp/diagnostics/<run_id>/`` subtree.
* Writer refuses to overwrite an existing file at the same path
  (``run_id`` + variation-point name collision).
* **Two unresolved required columnAliases in one run produce two
  distinct files** — directly addresses the round-2 review's
  aggregation-strategy blocker.
* One ``AIDPF-1020`` artifact lands without any variation-point files
  (identity gate fires before walk).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
    CandidateProbeOutcome,
    DiagnosticArtifactAlreadyExistsError,
    IdentityDiagnosticV1,
    IdentityProbeFailure,
    ObservedColumn,
    VariationPointDiagnosticV1,
    VariationPointFailure,
    write_identity_diagnostic,
    write_variation_diagnostic,
)


def _make_variation_artifact(
    *,
    error_code: str = AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    name: str = "invoice_currency_code",
    kind: str = "columnAliases",
) -> VariationPointDiagnosticV1:
    return VariationPointDiagnosticV1(
        runId="run-test-1",
        tenant="acme-prod",
        errorCode=error_code,
        errorMessage=f"variation point {name!r} has no matching candidate",
        generatedAt=datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc),
        variationPoint=VariationPointFailure(
            name=name,
            kind=kind,
            appliesTo="bronze.ap_invoices",
            candidatesTried=[
                CandidateProbeOutcome(
                    candidate="ApInvoicesInvoiceCurrencyCode",
                    outcome="column_not_found",
                ),
                CandidateProbeOutcome(
                    candidate="ApInvoicesCurrencyCode",
                    outcome="column_not_found",
                ),
            ],
            observedBronzeSchema=[
                ObservedColumn(name="ApInvoicesId", type="bigint"),
                ObservedColumn(name="ApInvoicesAmount", type="decimal(18,2)"),
            ],
        ),
    )


def _make_identity_artifact() -> IdentityDiagnosticV1:
    return IdentityDiagnosticV1(
        runId="run-test-1",
        tenant=None,
        errorCode=AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
        errorMessage="operator identity not resolvable from any source",
        generatedAt=datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc),
        identityProbe=IdentityProbeFailure(
            probedSources=["--operator", "AIDP_OPERATOR", "USER"],
        ),
    )


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


class TestVariationDiagnosticRoundTrip:
    def test_validates_and_dumps_with_aliases(self) -> None:
        artifact = _make_variation_artifact()
        serialised = artifact.model_dump_json(by_alias=True)
        payload = json.loads(serialised)
        assert payload["schemaVersion"] == 1
        assert payload["errorCode"] == "AIDPF-2010"
        assert payload["tenant"] == "acme-prod"
        assert payload["variationPoint"]["name"] == "invoice_currency_code"
        assert payload["variationPoint"]["appliesTo"] == "bronze.ap_invoices"
        assert len(payload["variationPoint"]["candidatesTried"]) == 2
        # Re-parse should succeed.
        again = VariationPointDiagnosticV1.model_validate(payload)
        assert again.variation_point.name == "invoice_currency_code"

    def test_invalid_error_code_rejected(self) -> None:
        with pytest.raises(Exception):
            VariationPointDiagnosticV1(
                runId="r",
                tenant="t",
                errorCode="AIDPF-9999",  # not in the Literal allow-list
                errorMessage="bad",
                generatedAt=datetime.now(timezone.utc),
                variationPoint=_make_variation_artifact().variation_point,
            )


class TestIdentityDiagnosticRoundTrip:
    def test_validates_and_dumps(self) -> None:
        artifact = _make_identity_artifact()
        payload = json.loads(artifact.model_dump_json(by_alias=True))
        assert payload["schemaVersion"] == 1
        assert payload["errorCode"] == "AIDPF-1020"
        assert payload["tenant"] is None
        assert payload["identityProbe"]["probedSources"] == [
            "--operator",
            "AIDP_OPERATOR",
            "USER",
        ]
        again = IdentityDiagnosticV1.model_validate(payload)
        assert again.identity_probe.probed_sources == ["--operator", "AIDP_OPERATOR", "USER"]

    def test_tenant_must_be_none(self) -> None:
        """Identity gate fires before tenant is known — tenant=non-None on
        a 1020 artifact is a bug; the model rejects it."""
        with pytest.raises(Exception):
            IdentityDiagnosticV1(
                runId="r",
                tenant="acme",  # must be None for 1020
                errorCode="AIDPF-1020",
                errorMessage="x",
                generatedAt=datetime.now(timezone.utc),
                identityProbe=IdentityProbeFailure(
                    probedSources=["USER"],
                ),
            )


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


class TestVariationWriter:
    def test_writes_under_diagnostics_subtree(self, tmp_path: Path) -> None:
        artifact = _make_variation_artifact()
        result_path = write_variation_diagnostic(tmp_path, "run-test-1", artifact)
        assert result_path == (
            tmp_path
            / ".aidp"
            / "diagnostics"
            / "run-test-1"
            / "AIDPF-2010__invoice_currency_code.json"
        )
        assert result_path.exists()
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        assert payload["variationPoint"]["name"] == "invoice_currency_code"

    def test_refuses_to_overwrite(self, tmp_path: Path) -> None:
        artifact = _make_variation_artifact()
        write_variation_diagnostic(tmp_path, "run-test-1", artifact)
        with pytest.raises(DiagnosticArtifactAlreadyExistsError):
            write_variation_diagnostic(tmp_path, "run-test-1", artifact)

    def test_two_unresolved_columnaliases_one_run_yield_two_files(
        self, tmp_path: Path
    ) -> None:
        """Round-2 review blocker: aggregation must not collapse to one
        file. Two failing required columnAliases in the same run produce
        two distinct files keyed by ``<vp-name>``."""
        invoice_currency = _make_variation_artifact(
            name="invoice_currency_code"
        )
        coa_balancing = _make_variation_artifact(
            name="coa_balancing_segment"
        )
        path1 = write_variation_diagnostic(tmp_path, "run-test-1", invoice_currency)
        path2 = write_variation_diagnostic(tmp_path, "run-test-1", coa_balancing)
        assert path1 != path2
        assert path1.name == "AIDPF-2010__invoice_currency_code.json"
        assert path2.name == "AIDPF-2010__coa_balancing_segment.json"
        # Both files in the same run_id directory.
        assert path1.parent == path2.parent == (
            tmp_path / ".aidp" / "diagnostics" / "run-test-1"
        )
        assert path1.exists() and path2.exists()

    def test_semantic_variant_artifact_uses_2011_prefix(self, tmp_path: Path) -> None:
        artifact = _make_variation_artifact(
            error_code=AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
            name="cancelled_status",
            kind="semanticVariants",
        )
        result_path = write_variation_diagnostic(tmp_path, "run-test-1", artifact)
        assert result_path.name == "AIDPF-2011__cancelled_status.json"


class TestIdentityWriter:
    def test_writes_under_diagnostics_subtree(self, tmp_path: Path) -> None:
        artifact = _make_identity_artifact()
        result_path = write_identity_diagnostic(tmp_path, "run-test-1", artifact)
        assert result_path == (
            tmp_path / ".aidp" / "diagnostics" / "run-test-1" / "AIDPF-1020.json"
        )
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        assert payload["errorCode"] == "AIDPF-1020"
        assert payload["tenant"] is None

    def test_refuses_to_overwrite(self, tmp_path: Path) -> None:
        artifact = _make_identity_artifact()
        write_identity_diagnostic(tmp_path, "run-test-1", artifact)
        with pytest.raises(DiagnosticArtifactAlreadyExistsError):
            write_identity_diagnostic(tmp_path, "run-test-1", artifact)

    def test_one_1020_artifact_no_variation_files(self, tmp_path: Path) -> None:
        """Identity gate fires before walk; the diagnostics directory
        contains exactly one file."""
        write_identity_diagnostic(tmp_path, "run-test-1", _make_identity_artifact())
        diag_dir = tmp_path / ".aidp" / "diagnostics" / "run-test-1"
        files = sorted(p.name for p in diag_dir.iterdir())
        assert files == ["AIDPF-1020.json"]


# ---------------------------------------------------------------------------
# AIDPF-4071 — bronze source-column-missing diagnostic (model + writer)
# ---------------------------------------------------------------------------


def _bronze_4071_artifact():
    from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
        BronzeSourceColumnMissingV1,
    )
    return BronzeSourceColumnMissingV1.model_validate({
        "schemaVersion": 1,
        "runId": "run-test-4071",
        "tenant": "demo-pod",
        "errorCode": "AIDPF-4071",
        "errorMessage": "bronze node 'ap_payments' declares column(s) absent…",
        "generatedAt": "2026-06-11T00:00:00+00:00",
        "node": "ap_payments",
        "datastore": "FscmTopModelAM.FinExtractAM.ApBiccExtractAM.PaymentHistoryDistributionExtractPVO",
        "missingColumns": ["ApPayHistDistInvoicePaymentId"],
        "pvoColumns": [
            {"name": "ApPaymentHistDistsInvoicePaymentId", "type": "decimal(18,0)", "nullable": True},
            {"name": "ApPaymentHistDistsAmount", "type": "decimal(38,30)", "nullable": True},
        ],
    })


class TestBronzeSourceColumnMissingDiagnostic:
    def test_model_roundtrip(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            BronzeSourceColumnMissingV1,
        )
        artifact = _bronze_4071_artifact()
        payload = json.loads(artifact.model_dump_json(by_alias=True))
        again = BronzeSourceColumnMissingV1.model_validate(payload)
        assert again.node == "ap_payments"
        assert again.missing_columns == ["ApPayHistDistInvoicePaymentId"]
        assert again.pvo_columns[0].name == "ApPaymentHistDistsInvoicePaymentId"

    def test_writes_under_diagnostics_subtree_with_node_discriminator(self, tmp_path: Path) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            write_bronze_source_column_missing_diagnostic,
        )
        result_path = write_bronze_source_column_missing_diagnostic(
            tmp_path, "run-test-4071", _bronze_4071_artifact()
        )
        assert result_path == (
            tmp_path / ".aidp" / "diagnostics" / "run-test-4071"
            / "AIDPF-4071__ap_payments.json"
        )
        body = json.loads(result_path.read_text())
        assert body["errorCode"] == "AIDPF-4071"
        assert body["node"] == "ap_payments"
        assert body["pvoColumns"][0]["name"] == "ApPaymentHistDistsInvoicePaymentId"
