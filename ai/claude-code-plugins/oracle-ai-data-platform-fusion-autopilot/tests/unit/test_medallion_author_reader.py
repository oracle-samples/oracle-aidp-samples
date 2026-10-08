"""Unit tests for :mod:`medallion_author.reader`.

Round-trips feature #2's diagnostic artifacts through the skill's
reader. Covers:

* Happy path: multiple 2010 / 2011 failures parse correctly.
* Refuse-to-proceed gates: identity-gate present, unknown schemaVersion,
  malformed JSON, empty directory.
* ``run_id`` auto-discovery picks the latest directory.
* Explicit ``run_id`` override points at the specified directory.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.medallion_author.reader import (
    DiagnosticReadResult,
    read_run,
)
from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
    AIDPF_2012_SCHEMA_DRIFT_DETECTED,
    AffectedVariationPoint,
    CandidateProbeOutcome,
    IdentityDiagnosticV1,
    IdentityProbeFailure,
    ObservedColumn,
    SchemaDriftDiagnosticV1,
    SchemaDriftFailure,
    VariationPointDiagnosticV1,
    VariationPointFailure,
    write_identity_diagnostic,
    write_schema_drift_diagnostic,
    write_variation_diagnostic,
)


_TS = datetime(2026, 6, 6, 12, 0, 0, tzinfo=timezone.utc)


def _variation_artifact(
    *,
    name: str,
    error_code: str = AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    kind: str = "columnAliases",
) -> VariationPointDiagnosticV1:
    return VariationPointDiagnosticV1(
        runId="run-test",
        tenant="finance-default",
        errorCode=error_code,
        errorMessage=f"{name!r} unresolved",
        generatedAt=_TS,
        variationPoint=VariationPointFailure(
            name=name,
            kind=kind,
            appliesTo="bronze.ap_invoices",
            candidatesTried=[
                CandidateProbeOutcome(candidate="X", outcome="column_not_found"),
            ],
            observedBronzeSchema=[
                ObservedColumn(name="ApInvoicesXCurrCode", type="string"),
            ],
        ),
    )


def _drift_artifact(
    *,
    affected_vps: list[AffectedVariationPoint] | None = None,
) -> SchemaDriftDiagnosticV1:
    return SchemaDriftDiagnosticV1(
        runId="run-test",
        tenant="finance-default",
        errorCode=AIDPF_2012_SCHEMA_DRIFT_DETECTED,
        errorMessage="drift",
        generatedAt=_TS,
        schemaDrift=SchemaDriftFailure(
            priorFingerprint="sha256:" + "a" * 64,
            currentFingerprint="sha256:" + "b" * 64,
            pinnedAt=_TS,
            affectedVariationPoints=affected_vps or [],
        ),
    )


def _identity_artifact() -> IdentityDiagnosticV1:
    return IdentityDiagnosticV1(
        runId="run-test",
        tenant=None,
        errorCode=AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
        errorMessage="identity unresolved",
        generatedAt=_TS,
        identityProbe=IdentityProbeFailure(probedSources=["USER"]),
    )


class TestHappyPath:
    def test_single_2010_failure_parses(self, tmp_path: Path) -> None:
        artifact = _variation_artifact(name="invoice_currency_code")
        write_variation_diagnostic(tmp_path, "run-test", artifact)
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-test")
        assert result.run_id == "run-test"
        assert len(result.variation_failures) == 1
        assert result.variation_failures[0].variation_point.name == (
            "invoice_currency_code"
        )
        assert result.can_proceed()

    def test_multiple_failures_parse_independently(self, tmp_path: Path) -> None:
        write_variation_diagnostic(
            tmp_path, "run-test", _variation_artifact(name="invoice_currency_code")
        )
        write_variation_diagnostic(
            tmp_path, "run-test", _variation_artifact(name="vendor_id")
        )
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-test")
        names = sorted(f.variation_point.name for f in result.variation_failures)
        assert names == ["invoice_currency_code", "vendor_id"]
        assert result.can_proceed()

    def test_mixed_2010_and_2011_parse(self, tmp_path: Path) -> None:
        write_variation_diagnostic(
            tmp_path, "run-test", _variation_artifact(name="vendor_id")
        )
        write_variation_diagnostic(
            tmp_path,
            "run-test",
            _variation_artifact(
                name="cancelled_status",
                error_code=AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
                kind="semanticVariants",
            ),
        )
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-test")
        kinds = sorted(f.variation_point.kind for f in result.variation_failures)
        assert kinds == ["columnAliases", "semanticVariants"]


class TestRefuseGates:
    def test_identity_gate_blocks(self, tmp_path: Path) -> None:
        write_identity_diagnostic(tmp_path, "run-test", _identity_artifact())
        # Also a 2010 in the same dir — but identity should still block.
        write_variation_diagnostic(
            tmp_path, "run-test", _variation_artifact(name="x")
        )
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-test")
        assert result.has_identity_failure
        assert not result.can_proceed()

    def test_unknown_schema_version_blocks(self, tmp_path: Path) -> None:
        diag_dir = tmp_path / ".aidp" / "diagnostics" / "run-test"
        diag_dir.mkdir(parents=True)
        (diag_dir / "AIDPF-2010__future.json").write_text(
            json.dumps({"schemaVersion": 99, "errorCode": "AIDPF-2010"}),
            encoding="utf-8",
        )
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-test")
        assert result.has_unknown_schema_version
        assert not result.can_proceed()

    def test_malformed_json_surfaced(self, tmp_path: Path) -> None:
        diag_dir = tmp_path / ".aidp" / "diagnostics" / "run-test"
        diag_dir.mkdir(parents=True)
        (diag_dir / "AIDPF-2010__bad.json").write_text("not json", encoding="utf-8")
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-test")
        assert result.has_malformed_artifacts
        assert not result.can_proceed()

    def test_empty_directory(self, tmp_path: Path) -> None:
        (tmp_path / ".aidp" / "diagnostics" / "run-empty").mkdir(parents=True)
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-empty")
        assert result.is_empty
        assert not result.can_proceed()

    def test_missing_diagnostics_root(self, tmp_path: Path) -> None:
        result = read_run(tmp_path / ".aidp" / "diagnostics", "anything")
        assert result.is_empty


class TestSchemaDriftRecognition:
    """Phase 3c — reader recognizes AIDPF-2012 artifacts."""

    def test_drift_artifact_parses(self, tmp_path: Path) -> None:
        write_schema_drift_diagnostic(tmp_path, "run-drift-1", _drift_artifact())
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-drift-1")
        assert result.schema_drift_failure is not None
        assert result.schema_drift_failure.error_code == "AIDPF-2012"

    def test_drift_only_directory_refuses_proceed(
        self, tmp_path: Path
    ) -> None:
        """Drift-only (no 2010/2011) → skill should refuse: drift
        recovery is `bootstrap --refresh`, not a skill draft."""
        write_schema_drift_diagnostic(tmp_path, "run-drift-only", _drift_artifact())
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-drift-only")
        assert result.schema_drift_failure is not None
        assert result.has_drift_only
        assert not result.can_proceed()

    def test_drift_plus_2010_proceeds_on_2010(self, tmp_path: Path) -> None:
        """Drift + 2010 in the same directory → skill surfaces both;
        operator can still act on 2010."""
        write_schema_drift_diagnostic(tmp_path, "run-mixed-1", _drift_artifact())
        write_variation_diagnostic(
            tmp_path, "run-mixed-1",
            _variation_artifact(name="invoice_currency_code"),
        )
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-mixed-1")
        assert result.schema_drift_failure is not None
        assert result.variation_failures  # non-empty
        assert not result.has_drift_only
        assert result.can_proceed()


class TestRunIdResolution:
    def test_auto_discovers_latest_run(self, tmp_path: Path) -> None:
        for run_id in (
            "20260601T120000Z-aaaaaaaa",
            "20260605T120000Z-bbbbbbbb",
            "20260603T120000Z-cccccccc",
        ):
            write_variation_diagnostic(
                tmp_path, run_id, _variation_artifact(name="x")
            )
        result = read_run(tmp_path / ".aidp" / "diagnostics", None)
        # Lexicographic max == chronological latest (ISO prefix).
        assert result.run_id == "20260605T120000Z-bbbbbbbb"

    def test_explicit_run_id_overrides_auto(self, tmp_path: Path) -> None:
        for run_id in ("20260601T120000Z-aaa", "20260605T120000Z-bbb"):
            write_variation_diagnostic(
                tmp_path, run_id, _variation_artifact(name="x")
            )
        result = read_run(
            tmp_path / ".aidp" / "diagnostics", "20260601T120000Z-aaa"
        )
        assert result.run_id == "20260601T120000Z-aaa"


# ---------------------------------------------------------------------------
# Phase 4.1 / D3 — cluster-dispatch artifacts (AIDPF-2048 / 2049)
# are recognised + skipped, not parsed into skill state
# ---------------------------------------------------------------------------


class TestPhase41ClusterDispatchScope:
    """The skill exists to recover from variation-point failures
    (2010/2011) and surface drift remediation (2012). Cluster-dispatch
    failures (2048/2049) are operator-actionable infrastructure issues
    — the skill recognises them but refuses to draft, with a
    structured hand-off rather than the wrong-flow remediation that
    would come from misclassifying them as ``malformed_paths``.
    """

    def _write_2048(self, tmp_path: Path, run_id: str = "run-cluster") -> Path:
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            ClusterDispatchDiagnosticV1,
            write_cluster_dispatch_diagnostic,
        )

        artifact = ClusterDispatchDiagnosticV1(
            runId=run_id,
            tenant="demo-pod",
            errorCode="AIDPF-2048",
            errorMessage="upload failed",
            generatedAt=_TS,
            clusterDispatch={
                "failedStep": "upload_notebook",
                "causeType": "DispatchUploadError",
                "causeMessage": "HTTP 500",
            },
        )
        return write_cluster_dispatch_diagnostic(tmp_path, run_id, artifact)

    def _write_2049(self, tmp_path: Path, run_id: str = "run-cluster") -> Path:
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            ClusterMarkerDiagnosticV1,
            write_cluster_marker_diagnostic,
        )

        artifact = ClusterMarkerDiagnosticV1(
            runId=run_id,
            tenant="demo-pod",
            errorCode="AIDPF-2049",
            errorMessage="envelope missing",
            generatedAt=_TS,
            clusterMarker={
                "kind": "envelope_missing",
                "stdoutExcerpt": "no marker here\n",
                "stdoutLogPath": "cluster_stdout.log",
            },
        )
        return write_cluster_marker_diagnostic(
            tmp_path, run_id, artifact, stdout_full="no marker here\n"
        )

    def test_2048_lands_in_cluster_dispatch_skipped_paths(
        self, tmp_path: Path
    ) -> None:
        path = self._write_2048(tmp_path)
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-cluster")
        # Recognised + skipped, NOT misclassified as malformed.
        assert path in result.cluster_dispatch_skipped_paths
        assert path not in result.malformed_paths
        assert result.variation_failures == []
        assert result.has_cluster_dispatch_failures is True

    def test_2049_lands_in_cluster_dispatch_skipped_paths(
        self, tmp_path: Path
    ) -> None:
        path = self._write_2049(tmp_path)
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-cluster")
        assert path in result.cluster_dispatch_skipped_paths
        # The companion `cluster_stdout.log` lives in the same dir
        # but isn't a *.json artifact — the reader's glob excludes it.

    def test_2048_present_blocks_can_proceed(self, tmp_path: Path) -> None:
        # Even with a real 2010 failure ready to draft, a 2048 in the
        # same dir blocks the skill — operator must fix the dispatch
        # issue and re-run bootstrap before the skill is useful.
        write_variation_diagnostic(
            tmp_path, "run-cluster", _variation_artifact(name="x")
        )
        self._write_2048(tmp_path)
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-cluster")
        assert result.has_cluster_dispatch_failures is True
        assert result.can_proceed() is False

    def test_2048_alone_makes_dir_non_empty(self, tmp_path: Path) -> None:
        # ``is_empty`` must return False so the skill surfaces the
        # cluster-dispatch failure (and its remediation hint) rather
        # than the "nothing to draft" no-op message.
        self._write_2048(tmp_path)
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-cluster")
        assert result.is_empty is False

    def test_unknown_code_still_lands_in_malformed(self, tmp_path: Path) -> None:
        # Regression check — the new scope branch is additive; codes
        # that aren't in either the parse set OR the cluster-dispatch
        # set still land in malformed_paths (today's existing
        # behaviour).
        run_id = "run-unknown"
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            _atomic_write_json,
            _diagnostics_dir,
        )

        diag_dir = _diagnostics_dir(tmp_path, run_id)
        diag_dir.mkdir(parents=True, exist_ok=True)
        unknown = diag_dir / "AIDPF-9999.json"
        _atomic_write_json(
            unknown,
            json.dumps({"schemaVersion": 1, "errorCode": "AIDPF-9999"}),
        )
        result = read_run(tmp_path / ".aidp" / "diagnostics", run_id)
        assert unknown in result.malformed_paths
        assert result.cluster_dispatch_skipped_paths == []


# ---------------------------------------------------------------------------
# AIDPF-4071 — bronze source-column-missing (runtime seed gate)
# ---------------------------------------------------------------------------


def _write_4071(tmp_path: Path, run_id: str, node: str = "ap_payments") -> None:
    from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
        BronzeSourceColumnMissingV1,
        write_bronze_source_column_missing_diagnostic,
    )
    artifact = BronzeSourceColumnMissingV1.model_validate({
        "schemaVersion": 1, "runId": run_id, "tenant": "demo-pod",
        "errorCode": "AIDPF-4071", "errorMessage": "missing",
        "generatedAt": "2026-06-11T00:00:00+00:00",
        "node": node,
        "datastore": "FscmTopModelAM.FinExtractAM.ApBiccExtractAM.PaymentHistoryDistributionExtractPVO",
        "missingColumns": ["ApPayHistDistInvoicePaymentId"],
        "pvoColumns": [{"name": "ApPaymentHistDistsInvoicePaymentId",
                        "type": "decimal(18,0)", "nullable": True}],
    })
    write_bronze_source_column_missing_diagnostic(tmp_path, run_id, artifact)


class TestAidpf4071Reader:
    def test_4071_parsed_into_source_column_failures(self, tmp_path: Path) -> None:
        _write_4071(tmp_path, "run-4071")
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-4071")
        assert len(result.source_column_failures) == 1
        f = result.source_column_failures[0]
        assert f.node == "ap_payments"
        assert f.missing_columns == ["ApPayHistDistInvoicePaymentId"]
        assert f.pvo_columns[0].name == "ApPaymentHistDistsInvoicePaymentId"

    def test_4071_is_skill_recoverable(self, tmp_path: Path) -> None:
        _write_4071(tmp_path, "run-4071")
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-4071")
        assert not result.is_empty
        assert result.can_proceed()

    def test_multiple_4071_nodes_parse_independently(self, tmp_path: Path) -> None:
        _write_4071(tmp_path, "run-4071", node="ap_payments")
        _write_4071(tmp_path, "run-4071", node="po_orders")
        result = read_run(tmp_path / ".aidp" / "diagnostics", "run-4071")
        assert {f.node for f in result.source_column_failures} == {"ap_payments", "po_orders"}
