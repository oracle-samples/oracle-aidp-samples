"""Unit tests for :mod:`oracle_ai_data_platform_fusion_autopilot.schema.evidence_snapshot`.

Covers:

* Model round-trip via Pydantic.
* Writer creates files under ``<workdir>/evidence/<tenant>/<iso-ts>.yaml``.
* Writer PRESERVES prior snapshots — directory accumulates per
  §9.5.7 #2.
* Writer refuses to overwrite an existing snapshot file at the same
  timestamp stem.
* Nested ``provenance.evidence.snapshots[].resolutions[]`` shape is
  exactly what skill (feature #3) parses.
* ``mechanism: auto_resolve`` is accepted (extension to the §9.5.9 enum).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.schema.evidence_snapshot import (
    ApprovedBy,
    CandidateConsidered,
    EvidenceContainer,
    EvidenceSnapshotAlreadyExistsError,
    EvidenceSnapshotV1,
    ResolvedVariationPoint,
    SnapshotEntry,
    SnapshotProvenance,
    write_evidence_snapshot,
)


def _make_snapshot(
    *,
    tenant: str = "acme-prod",
    generated_at: datetime | None = None,
    mechanism: str = "terminal_prompt",
) -> EvidenceSnapshotV1:
    if generated_at is None:
        generated_at = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)
    return EvidenceSnapshotV1(
        tenant=tenant,
        generatedAt=generated_at,
        runId="run-test-1",
        bronzeSchemaFingerprint="sha256:" + "a" * 64,
        provenance=SnapshotProvenance(
            approvedBy=ApprovedBy(
                operator="operator@example.com",
                timestamp=generated_at,
                mechanism=mechanism,  # type: ignore[arg-type]
            ),
            evidence=EvidenceContainer(
                snapshots=[
                    SnapshotEntry(
                        snapshotId=generated_at.isoformat(),
                        capturedAt=generated_at,
                        resolutions=[
                            ResolvedVariationPoint(
                                name="invoice_currency_code",
                                kind="columnAliases",
                                chosenCandidate="ApInvoicesInvoiceCurrencyCode",
                                candidatesConsidered=[
                                    CandidateConsidered(
                                        candidate="ApInvoicesInvoiceCurrencyCode",
                                        outcome="matched",
                                    ),
                                    CandidateConsidered(
                                        candidate="ApInvoicesCurrencyCode",
                                        outcome="matched",
                                    ),
                                ],
                                evidence={"rowsObserved": 4_700_000},
                            ),
                        ],
                    ),
                ],
            ),
        ),
    )


class TestSnapshotRoundTrip:
    def test_validates_and_dumps_with_aliases(self) -> None:
        snap = _make_snapshot()
        payload = snap.model_dump(by_alias=True, mode="json")
        assert payload["schemaVersion"] == 1
        # Nested PLAN-canonical shape — feature #3 parses this exact path.
        snapshot_entry = payload["provenance"]["evidence"]["snapshots"][0]
        assert snapshot_entry["resolutions"][0]["chosenCandidate"] == (
            "ApInvoicesInvoiceCurrencyCode"
        )
        # Re-validation round-trips.
        again = EvidenceSnapshotV1.model_validate(payload)
        assert again.tenant == "acme-prod"

    def test_auto_resolve_mechanism_accepted(self) -> None:
        """`auto_resolve` is the §9.5.9 enum extension for the
        single-candidate case — accepted by the schema."""
        snap = _make_snapshot(mechanism="auto_resolve")
        assert snap.provenance.approved_by.mechanism == "auto_resolve"

    def test_invalid_mechanism_rejected(self) -> None:
        with pytest.raises(Exception):
            _make_snapshot(mechanism="manual_override")  # not in the enum


class TestEvidenceWriter:
    def test_writes_under_evidence_tenant_subtree(self, tmp_path: Path) -> None:
        snap = _make_snapshot()
        result = write_evidence_snapshot(tmp_path, snap)
        # Colons in ISO timestamp are replaced with hyphens for filesystem safety.
        assert result.parent == tmp_path / "evidence" / "acme-prod"
        assert result.name.endswith(".yaml")
        loaded = yaml.safe_load(result.read_text(encoding="utf-8"))
        assert loaded["schemaVersion"] == 1
        # Nested shape persists through YAML round-trip.
        snapshot_entry = loaded["provenance"]["evidence"]["snapshots"][0]
        assert snapshot_entry["resolutions"][0]["chosenCandidate"] == (
            "ApInvoicesInvoiceCurrencyCode"
        )

    def test_refuses_to_overwrite_same_timestamp(self, tmp_path: Path) -> None:
        snap = _make_snapshot()
        write_evidence_snapshot(tmp_path, snap)
        with pytest.raises(EvidenceSnapshotAlreadyExistsError):
            write_evidence_snapshot(tmp_path, snap)

    def test_preserves_prior_snapshots(self, tmp_path: Path) -> None:
        """The §9.5.7 #2 accumulation rule — old snapshots stay on
        disk after each new write."""
        ts1 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts3 = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
        p1 = write_evidence_snapshot(tmp_path, _make_snapshot(generated_at=ts1))
        p2 = write_evidence_snapshot(tmp_path, _make_snapshot(generated_at=ts2))
        p3 = write_evidence_snapshot(tmp_path, _make_snapshot(generated_at=ts3))
        # All three files coexist.
        assert p1.exists()
        assert p2.exists()
        assert p3.exists()
        files = sorted(p.name for p in (tmp_path / "evidence" / "acme-prod").iterdir())
        assert len(files) == 3

    def test_tenant_directories_are_isolated(self, tmp_path: Path) -> None:
        write_evidence_snapshot(tmp_path, _make_snapshot(tenant="acme-prod"))
        write_evidence_snapshot(tmp_path, _make_snapshot(tenant="globex-prod"))
        acme = (tmp_path / "evidence" / "acme-prod")
        globex = (tmp_path / "evidence" / "globex-prod")
        assert acme.exists() and globex.exists()
        assert sorted(p.name for p in acme.iterdir()) != []
        assert sorted(p.name for p in globex.iterdir()) != []
