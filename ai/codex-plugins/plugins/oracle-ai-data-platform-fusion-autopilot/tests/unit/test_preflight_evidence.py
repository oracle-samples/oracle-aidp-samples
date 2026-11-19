"""Tests for :mod:`orchestrator.preflight_evidence` (Phase 3c).

Covers:

* Match → kind="match"; no artifact written.
* Mismatch → kind="drift"; artifact written; affectedVariationPoints
  computed correctly.
* mode="seed" → kind="skip_seed"; probe NOT called.
* force_skip → kind="skip_force_flag"; probe IS called (UNCONDITIONAL
  round-1 fix); both fingerprints populated.
* Legacy fingerprint (None / placeholder / malformed) →
  kind="skip_legacy_profile"; WARN log once.
* Round-trip with bootstrap: starter pack pinned via the same
  algorithm yields kind="match".
* dataset_ids source consistency: probe pulls from
  pack.bronze_yaml["datasets"] (NOT bundle.datasets).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.preflight_evidence import (
    PreflightOutcome,
    _bronze_dataset_ids,
    _is_legacy_fingerprint,
    _reset_legacy_warn,
    check_bronze_fingerprint_drift,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    compute_bronze_fingerprint,
    ColumnInfo,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
STARTER_PACK = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


# ---------------------------------------------------------------------------
# Test fixtures (mocked Spark + bundle/pack/profile)
# ---------------------------------------------------------------------------


def _row(name: str, type_: str = "string"):
    return {"col_name": name, "data_type": type_, "comment": None}


def _mock_spark(per_dataset_cols: dict[str, list[str]]) -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        dataset = target.split(".")[-1]
        cols = per_dataset_cols.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = [_row(c) for c in cols]
        return df

    spark.sql.side_effect = _sql
    return spark


def _mock_bundle(*, profile_name: str = "finance-default"):
    """Mock bundle. ``profile_name`` becomes ``bundle.content_pack.profile``
    — the key Phase 3d uses to resolve the snapshot path (mirrors
    bootstrap's `tenant_name = bundle.content_pack.profile or .name`)."""
    bundle = MagicMock(name="bundle")
    bundle.aidp.catalog = "cat"
    bundle.aidp.bronze_schema = "bronze"
    bundle.content_pack.profile = profile_name
    bundle.content_pack.name = "fusion-finance-starter"
    return bundle


def _mock_pack(
    datasets: list[str],
    *,
    semantic_variants: dict[str, list[tuple[str, str]]] | None = None,
):
    """Mock a ResolvedPack.

    ``semantic_variants``: optional mapping of VP name → list of
    ``(candidate_id, detect_column)`` tuples. Used by the Phase 3c
    drift gate's affected-VP computation: for each pinned semantic VP,
    the gate looks up the pinned candidate id in
    ``pack.pack.semantic_variants[name].candidates`` and reads its
    ``detect.column_exists`` to decide ``stillExistsOnBronze``.
    Defaults to ``{}`` — callers that don't exercise the semantic
    branch don't need to wire it.
    """
    pack = MagicMock(name="pack")
    pack.bronze_yaml = {"datasets": [{"id": d} for d in datasets]}
    sv_map: dict[str, MagicMock] = {}
    for vp_name, cand_list in (semantic_variants or {}).items():
        variant = MagicMock(name=f"sv-{vp_name}")
        cand_mocks = []
        for cand_id, detect_col in cand_list:
            c = MagicMock(name=f"cand-{cand_id}")
            c.id = cand_id
            c.detect.column_exists = detect_col
            cand_mocks.append(c)
        variant.candidates = cand_mocks
        sv_map[vp_name] = variant
    pack.pack.semantic_variants = sv_map
    return pack


def _mock_profile(
    *,
    pinned: str | None,
    resolved_column: dict[str, str] | None = None,
    resolved_semantic: dict[str, str] | None = None,
):
    profile = MagicMock(name="profile")
    profile.tenant = "finance-default"
    profile.pinned_at = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    profile.bronze_schema_fingerprint = pinned
    profile.resolved.column = resolved_column or {}
    profile.resolved.semantic = resolved_semantic or {}
    return profile


@pytest.fixture(autouse=True)
def _reset_warn():
    _reset_legacy_warn()
    yield
    _reset_legacy_warn()


# ---------------------------------------------------------------------------
# Match path
# ---------------------------------------------------------------------------


class TestMatch:
    def test_matching_fingerprint_returns_match(self, tmp_path: Path) -> None:
        # Compute the live fingerprint for the fixture bronze, then
        # use it as the "pinned" value → match.
        observed = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
            ]
        }
        pinned = compute_bronze_fingerprint(observed=observed)

        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ap_invoices": ["ApInvoicesInvoiceCurrencyCode"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned=pinned),
            run_id="cp-test-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "match"
        assert outcome.diagnostic_path is None
        # No artifact written under workdir.
        assert not (tmp_path / ".aidp" / "diagnostics").exists()


# ---------------------------------------------------------------------------
# Drift path
# ---------------------------------------------------------------------------


class TestDrift:
    def test_mismatch_returns_drift_and_writes_artifact(
        self, tmp_path: Path
    ) -> None:
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ap_invoices": ["ApInvoicesXCurrCode"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(
                pinned="sha256:" + "a" * 64,
                resolved_column={"invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"},
            ),
            run_id="cp-drift-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        # Artifact written at the expected path.
        assert outcome.diagnostic_path == (
            tmp_path / ".aidp" / "diagnostics" / "cp-drift-1" / "AIDPF-2012.json"
        )
        assert outcome.diagnostic_path.exists()
        # Hand-off message references the failing VP.
        assert "AIDPF-2012" in outcome.summary
        assert "bootstrap --refresh" in outcome.summary

    def test_affected_variation_points_diff(self, tmp_path: Path) -> None:
        """columnAlias-pinned values check column existence directly;
        semantic-pinned values resolve through the pack's candidate
        list to their detect column (review-driven fix — the prior
        version always reported ``stillExistsOnBronze=True`` for
        semantic VPs, hiding real drift signal)."""
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                # cancelled_date detect column still present here so the
                # semantic VP reports True; the columnAlias pins are
                # both absent.
                {"ap_invoices": ["ApInvoicesCurrencyCode", "ApInvoicesCancelledDate"]}
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(
                ["ap_invoices"],
                semantic_variants={
                    "cancelled_status": [
                        ("cancelled_date", "ApInvoicesCancelledDate"),
                        ("cancelled_flag", "ApInvoicesCancelledFlag"),
                    ],
                },
            ),
            profile=_mock_profile(
                pinned="sha256:" + "a" * 64,
                resolved_column={
                    "invoice_currency_code": "ApInvoicesInvoiceCurrencyCode",  # gone
                    "vendor_id": "VENDORID",  # absent too
                },
                resolved_semantic={"cancelled_status": "cancelled_date"},
            ),
            run_id="cp-drift-2",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json
        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        affected = payload["schemaDrift"]["affectedVariationPoints"]
        by_name = {vp["name"]: vp for vp in affected}
        # Both columnAlias-pinned values are missing from bronze.
        assert by_name["invoice_currency_code"]["stillExistsOnBronze"] is False
        assert by_name["vendor_id"]["stillExistsOnBronze"] is False
        # Semantic VP: pinned 'cancelled_date' candidate's detect column
        # (ApInvoicesCancelledDate) IS still present → True.
        assert by_name["cancelled_status"]["stillExistsOnBronze"] is True

    def test_semantic_variant_detect_column_dropped_surfaces_drift(
        self, tmp_path: Path
    ) -> None:
        """Review finding (P3c-review #2): when bootstrap pinned
        ``cancelled_status: cancelled_date`` because
        ``ApInvoicesCancelledDate`` existed, and Fusion later drops /
        renames that detect column, the AIDPF-2012 artifact MUST
        report ``stillExistsOnBronze: False`` so the operator sees the
        real semantic-VP drift signal. Prior code falsely reported
        True (treated the candidate id as opaque)."""
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                # ApInvoicesCancelledDate is gone; only the flag column
                # remains (so a `bootstrap --refresh` would re-pin to
                # cancelled_flag).
                {"ap_invoices": ["ApInvoicesCancelledFlag"]}
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(
                ["ap_invoices"],
                semantic_variants={
                    "cancelled_status": [
                        ("cancelled_date", "ApInvoicesCancelledDate"),
                        ("cancelled_flag", "ApInvoicesCancelledFlag"),
                    ],
                },
            ),
            profile=_mock_profile(
                pinned="sha256:" + "a" * 64,
                resolved_semantic={"cancelled_status": "cancelled_date"},
            ),
            run_id="cp-drift-sv",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json
        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        affected = payload["schemaDrift"]["affectedVariationPoints"]
        by_name = {vp["name"]: vp for vp in affected}
        assert by_name["cancelled_status"]["stillExistsOnBronze"] is False
        assert by_name["cancelled_status"]["pinnedCandidate"] == "cancelled_date"

    def test_semantic_variant_pinned_id_no_longer_in_pack_surfaces_drift(
        self, tmp_path: Path
    ) -> None:
        """If the pack version changed and the pinned candidate id is
        no longer declared (e.g., overlay removed it), the gate can't
        resolve a detect column → surfaces ``stillExistsOnBronze=False``
        so the operator gets actionable signal instead of a silent
        True. The downstream renderer will fail later anyway."""
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                {"ap_invoices": ["ApInvoicesCancelledDate"]}
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(
                ["ap_invoices"],
                # Only cancelled_flag declared now — the pinned
                # cancelled_date id was removed from the pack.
                semantic_variants={
                    "cancelled_status": [
                        ("cancelled_flag", "ApInvoicesCancelledFlag"),
                    ],
                },
            ),
            profile=_mock_profile(
                pinned="sha256:" + "a" * 64,
                resolved_semantic={"cancelled_status": "cancelled_date"},
            ),
            run_id="cp-drift-pack",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json
        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        by_name = {
            vp["name"]: vp
            for vp in payload["schemaDrift"]["affectedVariationPoints"]
        }
        assert by_name["cancelled_status"]["stillExistsOnBronze"] is False


# ---------------------------------------------------------------------------
# Skip paths
# ---------------------------------------------------------------------------


class TestSkipPaths:
    def test_seed_mode_skips_without_probe(self, tmp_path: Path) -> None:
        spark = _mock_spark({})
        outcome = check_bronze_fingerprint_drift(
            spark=spark,
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="cp-seed-1",
            mode="seed",
            workdir=tmp_path,
        )
        assert outcome.kind == "skip_seed"
        # Probe NOT called.
        spark.sql.assert_not_called()

    def test_force_skip_probes_and_records_both_fingerprints(
        self, tmp_path: Path
    ) -> None:
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ap_invoices": ["ApInvoicesXCurrCode"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="cp-force-1",
            mode="incremental",
            workdir=tmp_path,
            force_skip=True,
        )
        assert outcome.kind == "skip_force_flag"
        # Both fingerprints populated — the audit row needs them.
        assert outcome.prior_fingerprint == "sha256:" + "a" * 64
        assert outcome.current_fingerprint is not None
        assert outcome.current_fingerprint.startswith("sha256:")
        # No artifact written under workdir.
        assert not (tmp_path / ".aidp" / "diagnostics").exists()

    @pytest.mark.parametrize(
        "pinned",
        [
            None,
            "sha256:placeholder-finance-default-2026-06-05",
            "not-a-fingerprint",
            "sha256:tooshort",
        ],
    )
    def test_legacy_fingerprint_variants_skip(
        self, tmp_path: Path, pinned: str | None
    ) -> None:
        spark = _mock_spark({})
        outcome = check_bronze_fingerprint_drift(
            spark=spark,
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned=pinned),
            run_id="cp-legacy-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "skip_legacy_profile"
        # Probe NOT attempted on legacy profiles.
        spark.sql.assert_not_called()


# ---------------------------------------------------------------------------
# Round-trip + helpers
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_bronze_dataset_ids_from_pack(self) -> None:
        """Round-1 finding: dataset_ids MUST come from
        pack.bronze_yaml["datasets"], NOT bundle.datasets."""
        pack = _mock_pack(["erp_suppliers", "ap_invoices", "gl_coa"])
        assert _bronze_dataset_ids(pack) == ["erp_suppliers", "ap_invoices", "gl_coa"]

    def test_starter_pack_round_trip(self, tmp_path: Path) -> None:
        """Compute fingerprint from a starter-pack-shaped observation,
        feed it back in as pinned → match. Pins the round-trip
        contract."""
        observed = {
            "erp_suppliers": [ColumnInfo(name="VENDORID", type="string")],
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
            ],
        }
        pinned = compute_bronze_fingerprint(observed=observed)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                {
                    "erp_suppliers": ["VENDORID"],
                    "ap_invoices": ["ApInvoicesInvoiceCurrencyCode"],
                }
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["erp_suppliers", "ap_invoices"]),
            profile=_mock_profile(pinned=pinned),
            run_id="cp-roundtrip-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "match"


class TestLegacyDetection:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, True),
            ("", True),
            ("sha256:placeholder-x", True),
            ("not-a-fingerprint", True),
            ("sha256:abc", True),  # too short
            ("sha256:" + "a" * 64, False),  # valid
            ("sha256:" + "A" * 64, True),  # uppercase hex — strict regex
        ],
    )
    def test_legacy_fingerprint_classification(
        self, value: str | None, expected: bool
    ) -> None:
        assert _is_legacy_fingerprint(value) is expected


# ---------------------------------------------------------------------------
# Phase 3d — datasetDeltas population
# ---------------------------------------------------------------------------


from oracle_ai_data_platform_fusion_autopilot.orchestrator.preflight_evidence import (
    _compute_dataset_deltas,
    _load_snapshot_if_present,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_snapshot import (
    BronzeSchemaSnapshotV1,
    from_observed as snapshot_from_observed,
    write_bronze_schema_snapshot,
)


def _write_snapshot(
    tmp_path: Path,
    *,
    tenant: str,
    pinned_at: datetime,
    fingerprint: str,
    observed: dict[str, list[ColumnInfo]],
) -> Path:
    snap = snapshot_from_observed(
        tenant=tenant,
        pinned_at=pinned_at,
        fingerprint=fingerprint,
        observed=observed,
    )
    return write_bronze_schema_snapshot(tmp_path, tenant, snap)


class TestPhase3dDatasetDeltasComputation:
    """Pure-function tests for ``_compute_dataset_deltas`` — no preflight."""

    def test_no_changes_returns_empty(self) -> None:
        observed = {
            "ap_invoices": [
                ColumnInfo(name="A", type="bigint"),
                ColumnInfo(name="B", type="string"),
            ]
        }
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=observed,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=observed)
        assert deltas == []

    def test_added_and_removed_columns_in_same_dataset(self) -> None:
        prior = {
            "ap_invoices": [
                ColumnInfo(name="A", type="bigint"),
                ColumnInfo(name="B", type="string"),
            ]
        }
        live = {
            "ap_invoices": [
                ColumnInfo(name="A", type="bigint"),
                ColumnInfo(name="C", type="double"),
            ]
        }
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert len(deltas) == 1
        d = deltas[0]
        assert d.dataset_id == "ap_invoices"
        assert {c.name for c in d.added_columns} == {"C"}
        assert {c.name for c in d.removed_columns} == {"B"}
        assert d.type_changed_columns == []

    def test_type_change_only_goes_into_type_changed_list(self) -> None:
        prior = {"ds": [ColumnInfo(name="amount", type="bigint")]}
        live = {"ds": [ColumnInfo(name="amount", type="string")]}
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert len(deltas) == 1
        d = deltas[0]
        assert d.added_columns == []
        assert d.removed_columns == []
        assert len(d.type_changed_columns) == 1
        tc = d.type_changed_columns[0]
        assert tc.name == "amount"
        assert tc.prior_type == "bigint"
        assert tc.current_type == "string"

    def test_case_only_diff_invisible(self) -> None:
        """Canonicalisation parity (review #3): a case-only diff in a
        column name maps to zero deltas — same as the fingerprint."""
        prior = {"ds": [ColumnInfo(name="ApInvoicesAmount", type="bigint")]}
        live = {"ds": [ColumnInfo(name="APINVOICESAMOUNT", type="bigint")]}
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert deltas == []

    def test_whitespace_only_diff_invisible(self) -> None:
        prior = {"ds": [ColumnInfo(name="foo ", type="string")]}
        live = {"ds": [ColumnInfo(name=" foo", type="string")]}
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert deltas == []

    def test_type_case_only_diff_invisible(self) -> None:
        prior = {"ds": [ColumnInfo(name="amount", type="BIGINT")]}
        live = {"ds": [ColumnInfo(name="amount", type="bigint")]}
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert deltas == []

    def test_type_actual_change_preserves_original_casing(self) -> None:
        """Original casing on prior_type / current_type is preserved for
        display even though the comparison is case-insensitive."""
        prior = {"ds": [ColumnInfo(name="amount", type="bigint")]}
        live = {"ds": [ColumnInfo(name="amount", type="string")]}
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert deltas[0].type_changed_columns[0].prior_type == "bigint"
        assert deltas[0].type_changed_columns[0].current_type == "string"

    def test_dataset_only_in_snapshot_surfaces_all_removed(self) -> None:
        prior = {"orphan": [ColumnInfo(name="x", type="string")]}
        live: dict[str, list[ColumnInfo]] = {}
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert len(deltas) == 1
        assert deltas[0].dataset_id == "orphan"
        assert [c.name for c in deltas[0].removed_columns] == ["x"]
        assert deltas[0].added_columns == []

    def test_dataset_only_in_observed_surfaces_all_added(self) -> None:
        prior: dict[str, list[ColumnInfo]] = {}
        live = {"newcomer": [ColumnInfo(name="x", type="string")]}
        snapshot = snapshot_from_observed(
            tenant="t",
            pinned_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=prior,
        )
        deltas = _compute_dataset_deltas(snapshot=snapshot, observed=live)
        assert len(deltas) == 1
        assert deltas[0].dataset_id == "newcomer"
        assert [c.name for c in deltas[0].added_columns] == ["x"]
        assert deltas[0].removed_columns == []


class TestPhase3dDriftBranchPopulation:
    """End-to-end: drift fires, snapshot present → datasetDeltas populated."""

    def test_drift_with_snapshot_populates_dataset_deltas(
        self, tmp_path: Path
    ) -> None:
        prior_observed = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
            ]
        }
        prior_fingerprint = compute_bronze_fingerprint(observed=prior_observed)
        _write_snapshot(
            tmp_path,
            tenant="finance-default",
            pinned_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            fingerprint=prior_fingerprint,
            observed=prior_observed,
        )
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                {
                    "ap_invoices": [
                        "ApInvoicesCurrencyCode"  # column renamed
                    ]
                }
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned=prior_fingerprint),
            run_id="cp-3d-drift-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        deltas = payload["schemaDrift"]["datasetDeltas"]
        assert len(deltas) == 1
        assert deltas[0]["datasetId"] == "ap_invoices"
        added = [c["name"] for c in deltas[0]["addedColumns"]]
        removed = [c["name"] for c in deltas[0]["removedColumns"]]
        assert added == ["ApInvoicesCurrencyCode"]
        assert removed == ["ApInvoicesInvoiceCurrencyCode"]

    def test_snapshot_absent_emits_empty_dataset_deltas_and_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        caplog.set_level(logging.WARNING)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ap_invoices": ["ApInvoicesCurrencyCode"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="cp-3d-drift-noSnap",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        assert payload["schemaDrift"]["datasetDeltas"] == []
        assert any(
            "Pinned schema snapshot absent" in r.getMessage()
            for r in caplog.records
        )

    def test_snapshot_fingerprint_mismatch_with_profile_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Snapshot and profile carry different fingerprints (profile/snapshot
        desync). Helper degrades to empty deltas + WARN; drift signal
        still emitted."""
        import logging

        caplog.set_level(logging.WARNING)
        # Build a healthy snapshot, then change the profile's pinned
        # fingerprint to something else.
        prior_observed = {"ds": [ColumnInfo(name="A", type="string")]}
        prior_fingerprint = compute_bronze_fingerprint(observed=prior_observed)
        _write_snapshot(
            tmp_path,
            tenant="finance-default",
            pinned_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            fingerprint=prior_fingerprint,
            observed=prior_observed,
        )
        # Profile pinned a *different* fingerprint.
        other_fingerprint = "sha256:" + "f" * 64
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ds": ["A_renamed"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ds"]),
            profile=_mock_profile(pinned=other_fingerprint),
            run_id="cp-3d-desync",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        assert payload["schemaDrift"]["datasetDeltas"] == []
        assert any(
            "differs from profile fingerprint" in r.getMessage()
            for r in caplog.records
        )

    def test_snapshot_malformed_emits_empty_dataset_deltas_and_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        caplog.set_level(logging.WARNING)
        (tmp_path / "profiles").mkdir()
        (
            tmp_path / "profiles" / "finance-default.schema-snapshot.yaml"
        ).write_text("not: [valid: yaml", encoding="utf-8")
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ds": ["A"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ds"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="cp-3d-malformed",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        assert payload["schemaDrift"]["datasetDeltas"] == []
        assert any(
            "Pinned schema snapshot unparseable" in r.getMessage()
            for r in caplog.records
        )

    def test_snapshot_content_hand_edited_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Snapshot's metadata fingerprint still matches the profile, but
        its `datasets` list was hand-edited so a fresh recompute over
        the contents produces a different hash. Helper degrades."""
        import logging

        import yaml

        caplog.set_level(logging.WARNING)
        prior_observed = {"ds": [ColumnInfo(name="A", type="string")]}
        prior_fingerprint = compute_bronze_fingerprint(observed=prior_observed)
        snapshot_path = _write_snapshot(
            tmp_path,
            tenant="finance-default",
            pinned_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            fingerprint=prior_fingerprint,
            observed=prior_observed,
        )
        # Hand-edit: drop the only column but keep the fingerprint field.
        raw = yaml.safe_load(snapshot_path.read_text(encoding="utf-8"))
        raw["datasets"][0]["columns"] = []
        snapshot_path.write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")

        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ds": ["B"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ds"]),
            profile=_mock_profile(pinned=prior_fingerprint),
            run_id="cp-3d-handedited",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        assert payload["schemaDrift"]["datasetDeltas"] == []
        assert any(
            "content/metadata fingerprint desync" in r.getMessage()
            for r in caplog.records
        )

    def test_load_snapshot_match_returns_model(self, tmp_path: Path) -> None:
        prior_observed = {"ds": [ColumnInfo(name="A", type="string")]}
        prior_fingerprint = compute_bronze_fingerprint(observed=prior_observed)
        _write_snapshot(
            tmp_path,
            tenant="finance-default",
            pinned_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            fingerprint=prior_fingerprint,
            observed=prior_observed,
        )
        loaded = _load_snapshot_if_present(
            bundle_path=tmp_path / "bundle.yaml",
            profile_name="finance-default",
            profile=_mock_profile(pinned=prior_fingerprint),
        )
        assert isinstance(loaded, BronzeSchemaSnapshotV1)
        assert loaded.bronze_schema_fingerprint == prior_fingerprint

    def test_profile_name_drives_snapshot_path_not_tenant_field(
        self, tmp_path: Path
    ) -> None:
        """Reviewer-caught (round 1, BLOCKING): bootstrap keys the
        snapshot on ``bundle.contentPack.profile``, but a pre-3d
        profile YAML may carry a hand-authored ``tenant:`` value that
        differs from the active profile name. Preflight MUST resolve
        the snapshot by the active profile name (matching bootstrap's
        key), NOT by the loaded ``TenantProfile.tenant`` field. Without
        this, ``bootstrap --refresh`` back-fills the snapshot at
        ``profiles/<contentPack.profile>.schema-snapshot.yaml`` but
        preflight looks under ``profiles/<profile.tenant>.schema-snapshot.yaml``
        and never finds it — the documented remediation appears
        broken.
        """
        prior_observed = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
            ]
        }
        prior_fingerprint = compute_bronze_fingerprint(observed=prior_observed)
        # Bootstrap-side: write under the active profile name.
        _write_snapshot(
            tmp_path,
            tenant="phase2-fixture",
            pinned_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            fingerprint=prior_fingerprint,
            observed=prior_observed,
        )
        # Profile YAML carries a DIFFERENT `tenant:` value (hand-authored
        # pre-3d profile). Preflight must use bundle.contentPack.profile
        # ("phase2-fixture"), not profile.tenant ("acme-prod").
        profile = _mock_profile(pinned=prior_fingerprint)
        profile.tenant = "acme-prod"

        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                {"ap_invoices": ["ApInvoicesCurrencyCode"]}  # renamed
            ),
            bundle=_mock_bundle(profile_name="phase2-fixture"),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=profile,
            run_id="cp-3d-key-mismatch",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(
            outcome.diagnostic_path.read_text(encoding="utf-8")
        )
        deltas = payload["schemaDrift"]["datasetDeltas"]
        # If preflight had keyed on profile.tenant="acme-prod" the
        # snapshot would have been absent and deltas would be empty.
        # Keying on bundle.content_pack.profile="phase2-fixture" finds
        # it and populates the diff.
        assert len(deltas) == 1
        added = {c["name"] for c in deltas[0]["addedColumns"]}
        removed = {c["name"] for c in deltas[0]["removedColumns"]}
        assert added == {"ApInvoicesCurrencyCode"}
        assert removed == {"ApInvoicesInvoiceCurrencyCode"}


# ---------------------------------------------------------------------------
# Absence tolerance — baseline-filled effective fingerprint
# (feature: bronze-fingerprint-gate-scope)
# ---------------------------------------------------------------------------

from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe import (
    BronzeProbeFailure,
    UnsafeIdentifierError,
)


def _mock_spark_partial(
    present: dict[str, list[str]],
    absent: set[str] = frozenset(),
    broken: dict[str, str] | None = None,
) -> MagicMock:
    """Fake Spark: DESCRIBE raises NOT_FOUND for ``absent`` tables, raises the
    given message for ``broken`` tables, returns rows otherwise."""
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        table = target.split(".")[-1]
        if table in absent:
            raise Exception(
                f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{target}` "
                f"cannot be found."
            )
        if broken and table in broken:
            raise Exception(broken[table])
        df = MagicMock(name=f"df_{table}")
        df.collect.return_value = [_row(c) for c in present.get(table, [])]
        df.take.return_value = [_row(c) for c in present.get(table, [])][:1]
        return df

    spark.sql.side_effect = _sql
    return spark


class TestAbsenceTolerance:
    """One missing bronze table must not abort the gate — but every present
    table stays fully drift-checked, and pack-set changes still drift."""

    def _pin(self, tmp_path: Path, observed: dict) -> str:
        """Pin fingerprint + write a healthy snapshot for ``observed``."""
        pinned = compute_bronze_fingerprint(observed=observed)
        _write_snapshot(
            tmp_path,
            tenant="finance-default",
            pinned_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            fingerprint=pinned,
            observed=observed,
        )
        return pinned

    def test_absent_with_baseline_matches(self, tmp_path: Path) -> None:
        pin_observed = {
            "a_tbl": [ColumnInfo(name="ColA", type="string")],
            "b_tbl": [ColumnInfo(name="ColB", type="string")],
        }
        pinned = self._pin(tmp_path, pin_observed)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark_partial({"a_tbl": ["ColA"]}, absent={"b_tbl"}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["a_tbl", "b_tbl"]),
            profile=_mock_profile(pinned=pinned),
            run_id="abs-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "match"
        assert outcome.absent_datasets == ("b_tbl",)
        # Effective fingerprint == pin (baseline-filled), surfaced verbatim.
        assert outcome.current_fingerprint == pinned
        assert "b_tbl" in outcome.summary
        assert not (tmp_path / ".aidp" / "diagnostics").exists()

    def test_absent_plus_present_drift(self, tmp_path: Path) -> None:
        pin_observed = {
            "a_tbl": [ColumnInfo(name="ColA", type="string")],
            "b_tbl": [ColumnInfo(name="ColB", type="string")],
        }
        pinned = self._pin(tmp_path, pin_observed)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark_partial(
                {"a_tbl": ["ColA_renamed"]}, absent={"b_tbl"}
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["a_tbl", "b_tbl"]),
            profile=_mock_profile(pinned=pinned),
            run_id="abs-2",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        assert outcome.absent_datasets == ("b_tbl",)
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        deltas = payload["schemaDrift"]["datasetDeltas"]
        # Baseline-filled b_tbl diffs as identical -> NO false delta for it.
        assert [d["datasetId"] for d in deltas] == ["a_tbl"]

    def test_set_removal_regression(self, tmp_path: Path) -> None:
        """Round-1 Finding 1 (Blocking): one current table absent + one
        snapshot dataset REMOVED from the pack -> drift, not match."""
        pin_observed = {
            "a_tbl": [ColumnInfo(name="ColA", type="string")],
            "b_tbl": [ColumnInfo(name="ColB", type="string")],
        }
        pinned = self._pin(tmp_path, pin_observed)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark_partial({}, absent={"a_tbl"}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["a_tbl"]),  # b_tbl removed from the pack
            profile=_mock_profile(pinned=pinned),
            run_id="abs-3",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        deltas = {d["datasetId"]: d for d in payload["schemaDrift"]["datasetDeltas"]}
        # b_tbl's "all columns removed" is a TRUE signal (gone from the pack).
        assert "b_tbl" in deltas
        assert [c["name"] for c in deltas["b_tbl"]["removedColumns"]] == ["ColB"]
        # a_tbl was baseline-filled -> identical -> no delta.
        assert "a_tbl" not in deltas

    def test_unpinned_absent_tolerated(
        self, tmp_path: Path, caplog: "pytest.LogCaptureFixture"
    ) -> None:
        """A dataset declared AFTER the pin and never landed is tolerated."""
        import logging

        caplog.set_level(logging.WARNING)
        pin_observed = {"a_tbl": [ColumnInfo(name="ColA", type="string")]}
        pinned = self._pin(tmp_path, pin_observed)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark_partial({"a_tbl": ["ColA"]}, absent={"c_tbl"}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["a_tbl", "c_tbl"]),  # c_tbl post-pin addition
            profile=_mock_profile(pinned=pinned),
            run_id="abs-4",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "match"
        assert outcome.absent_datasets == ("c_tbl",)
        assert any("unpinned" in r.getMessage() for r in caplog.records)

    def test_absent_no_snapshot_raises_with_remediation(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(BronzeProbeFailure) as exc_info:
            check_bronze_fingerprint_drift(
                spark=_mock_spark_partial({"a_tbl": ["ColA"]}, absent={"b_tbl"}),
                bundle=_mock_bundle(),
                bundle_path=tmp_path / "bundle.yaml",
                pack=_mock_pack(["a_tbl", "b_tbl"]),
                profile=_mock_profile(pinned="sha256:" + "a" * 64),
                run_id="abs-5",
                mode="incremental",
                workdir=tmp_path,
            )
        assert "bootstrap --refresh" in str(exc_info.value)

    def test_absent_no_snapshot_force_skip_bypasses(self, tmp_path: Path) -> None:
        """Round-1 Rec 1: the no-snapshot raise is a BronzeProbeFailure, so
        the break-glass reach is byte-identical to today."""
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark_partial({"a_tbl": ["ColA"]}, absent={"b_tbl"}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["a_tbl", "b_tbl"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="abs-6",
            mode="incremental",
            workdir=tmp_path,
            force_skip=True,
        )
        assert outcome.kind == "skip_force_flag"
        assert outcome.current_fingerprint is None

    def test_catalog_failure_raises_and_force_skip_bypasses(
        self, tmp_path: Path
    ) -> None:
        """A non-not-found failure (permissions / broken catalog) fails closed
        without the flag and is bypassed with it — unchanged contract."""
        kwargs = dict(
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["a_tbl", "b_tbl"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="abs-7",
            mode="incremental",
            workdir=tmp_path,
        )
        broken_spark = _mock_spark_partial(
            {"a_tbl": ["ColA"]}, broken={"b_tbl": "PERMISSION_DENIED: nope"}
        )
        with pytest.raises(BronzeProbeFailure):
            check_bronze_fingerprint_drift(spark=broken_spark, **kwargs)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark_partial(
                {"a_tbl": ["ColA"]}, broken={"b_tbl": "PERMISSION_DENIED: nope"}
            ),
            force_skip=True,
            **kwargs,
        )
        assert outcome.kind == "skip_force_flag"

    def test_affected_vp_in_absent_dataset_still_exists(
        self, tmp_path: Path
    ) -> None:
        """Round-1 Rec 2: a pinned VP whose column lives ONLY in an absent
        (baseline-filled) table must NOT be reported as no-longer-exists."""
        pin_observed = {
            "a_tbl": [ColumnInfo(name="ColA", type="string")],
            "b_tbl": [ColumnInfo(name="VpOnlyColumn", type="string")],
        }
        pinned = self._pin(tmp_path, pin_observed)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark_partial(
                {"a_tbl": ["ColA_renamed"]}, absent={"b_tbl"}
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["a_tbl", "b_tbl"]),
            profile=_mock_profile(
                pinned=pinned,
                resolved_column={"invoice_currency": "VpOnlyColumn"},
            ),
            run_id="abs-8",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json

        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        vps = {
            v["name"]: v
            for v in payload["schemaDrift"]["affectedVariationPoints"]
        }
        assert vps["invoice_currency"]["stillExistsOnBronze"] is True

    def test_unsafe_identifier_never_bypassable(self, tmp_path: Path) -> None:
        """Round-2 review: the injection guard is raised pre-Spark and is NOT
        translated into a (force-skippable) BronzeProbeFailure."""
        spark = MagicMock(name="spark")
        bundle = _mock_bundle()
        bundle.aidp.catalog = "cat; DROP TABLE x"
        with pytest.raises(UnsafeIdentifierError):
            check_bronze_fingerprint_drift(
                spark=spark,
                bundle=bundle,
                bundle_path=tmp_path / "bundle.yaml",
                pack=_mock_pack(["a_tbl"]),
                profile=_mock_profile(pinned="sha256:" + "a" * 64),
                run_id="abs-9",
                mode="incremental",
                workdir=tmp_path,
                force_skip=True,
            )
        assert spark.sql.call_count == 0
