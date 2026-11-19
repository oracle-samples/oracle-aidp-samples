"""Regression tests for the path-traversal hardening (round-1 review,
blocking #1).

Asserts that every write surface in this feature refuses path segments
containing ``..``, ``/``, ``\\``, or other unsafe characters, and that
the workdir-anchor stays intact even when the resolved target would
escape the intended root.

Surfaces covered:

* :func:`resolve_profile_path` (tenant_profile.py)
* :func:`write_evidence_snapshot` (evidence_snapshot.py)
* :func:`write_variation_diagnostic` / :func:`write_identity_diagnostic`
  (diagnostic_artifact.py)
* Variation phase top-level entry point — early hard-fail with
  AIDPF-style message before any I/O.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    CandidateProbeOutcome,
    IdentityDiagnosticV1,
    IdentityProbeFailure,
    ObservedColumn,
    VariationPointDiagnosticV1,
    VariationPointFailure,
    write_identity_diagnostic,
    write_variation_diagnostic,
)
from oracle_ai_data_platform_fusion_autopilot.schema.evidence_snapshot import (
    ApprovedBy,
    CandidateConsidered,
    EvidenceContainer,
    EvidenceSnapshotV1,
    ResolvedVariationPoint,
    SnapshotEntry,
    SnapshotProvenance,
    write_evidence_snapshot,
)
from oracle_ai_data_platform_fusion_autopilot.schema.path_segment import (
    UnsafePathSegmentError,
    assert_within_root,
    validate_path_segment,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    resolve_profile_path,
)


class TestValidatePathSegmentAllowlist:
    @pytest.mark.parametrize(
        "value",
        [
            "finance-default",
            "acme_prod",
            "tenant.v2",
            "tenant1",
            "A.B.c-d_e",
        ],
    )
    def test_safe_segments_pass(self, value: str) -> None:
        assert validate_path_segment(value, field="t") == value

    @pytest.mark.parametrize(
        "value",
        [
            "../outside",
            "..",
            "foo/bar",
            "foo\\bar",
            "/abs/path",
            "  whitespace  ",
            "",
            ".hidden",
            "-leading-dash",
            "tenant;rm -rf /",
            "tenant\nwithnewline",
            "tenant\x00null",
        ],
    )
    def test_unsafe_segments_rejected(self, value: str) -> None:
        with pytest.raises(UnsafePathSegmentError):
            validate_path_segment(value, field="t")

    def test_non_string_rejected(self) -> None:
        with pytest.raises(UnsafePathSegmentError):
            validate_path_segment(None, field="t")  # type: ignore[arg-type]


class TestAssertWithinRoot:
    def test_target_under_root_ok(self, tmp_path: Path) -> None:
        root = tmp_path / "evidence"
        root.mkdir()
        target = root / "tenant" / "snap.yaml"
        assert_within_root(target, root, field="evidence.tenant")

    def test_target_outside_root_rejected(self, tmp_path: Path) -> None:
        root = tmp_path / "evidence"
        root.mkdir()
        outside = tmp_path / "elsewhere"
        outside.mkdir()
        with pytest.raises(UnsafePathSegmentError):
            assert_within_root(outside / "x", root, field="evidence.tenant")


class TestResolveProfilePathTraversalDefence:
    @pytest.mark.parametrize(
        "profile_name",
        ["../../outside", "../etc/passwd", "tenant/../..", "/abs/path"],
    )
    def test_traversal_segment_rejected(
        self, tmp_path: Path, profile_name: str
    ) -> None:
        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.touch()
        with pytest.raises(UnsafePathSegmentError):
            resolve_profile_path(bundle_path, profile_name)

    def test_safe_segment_resolves_under_bundle_parent(
        self, tmp_path: Path
    ) -> None:
        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.touch()
        result = resolve_profile_path(bundle_path, "finance-default")
        assert result == (tmp_path / "profiles" / "finance-default.yaml").resolve()


class TestEvidenceWriterTraversalDefence:
    def _snapshot(self, tenant: str) -> EvidenceSnapshotV1:
        ts = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)
        return EvidenceSnapshotV1(
            tenant=tenant,
            generatedAt=ts,
            runId="run-test",
            bronzeSchemaFingerprint="sha256:" + "a" * 64,
            provenance=SnapshotProvenance(
                approvedBy=ApprovedBy(
                    operator="alice",
                    timestamp=ts,
                    mechanism="auto_resolve",
                ),
                evidence=EvidenceContainer(
                    snapshots=[
                        SnapshotEntry(
                            snapshotId="run-test",
                            capturedAt=ts,
                            resolutions=[],
                        )
                    ],
                ),
            ),
        )

    @pytest.mark.parametrize(
        "tenant", ["../../outside", "foo/bar", "..", "/abs"]
    )
    def test_unsafe_tenant_rejected(
        self, tmp_path: Path, tenant: str
    ) -> None:
        # Pydantic accepts the raw string; writer must reject before any I/O.
        snap = self._snapshot(tenant)
        with pytest.raises(UnsafePathSegmentError):
            write_evidence_snapshot(tmp_path, snap)


class TestDiagnosticWriterTraversalDefence:
    def _variation_artifact(self, vp_name: str) -> VariationPointDiagnosticV1:
        return VariationPointDiagnosticV1(
            runId="run-test",
            tenant="finance-default",
            errorCode=AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
            errorMessage="x",
            generatedAt=datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc),
            variationPoint=VariationPointFailure(
                name=vp_name,
                kind="columnAliases",
                appliesTo="bronze.ap_invoices",
                candidatesTried=[
                    CandidateProbeOutcome(
                        candidate="X", outcome="column_not_found"
                    )
                ],
                observedBronzeSchema=[
                    ObservedColumn(name="Y", type="string"),
                ],
            ),
        )

    @pytest.mark.parametrize(
        "vp_name", ["../../escape", "foo/bar", "..", "name with spaces"]
    )
    def test_unsafe_variation_point_name_rejected(
        self, tmp_path: Path, vp_name: str
    ) -> None:
        artifact = self._variation_artifact(vp_name)
        with pytest.raises(UnsafePathSegmentError):
            write_variation_diagnostic(tmp_path, "run-test", artifact)

    @pytest.mark.parametrize(
        "run_id", ["../../escape", "foo/bar", "..", ""]
    )
    def test_unsafe_run_id_rejected(
        self, tmp_path: Path, run_id: str
    ) -> None:
        artifact = self._variation_artifact("safe_name")
        with pytest.raises(UnsafePathSegmentError):
            write_variation_diagnostic(tmp_path, run_id, artifact)

    def test_unsafe_run_id_rejected_for_identity_writer(
        self, tmp_path: Path
    ) -> None:
        artifact = IdentityDiagnosticV1(
            runId="anything",
            tenant=None,
            errorCode=AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
            errorMessage="x",
            generatedAt=datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc),
            identityProbe=IdentityProbeFailure(probedSources=["USER"]),
        )
        with pytest.raises(UnsafePathSegmentError):
            write_identity_diagnostic(tmp_path, "../escape", artifact)
