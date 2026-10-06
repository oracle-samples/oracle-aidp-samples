"""Tests for the ``bootstrap --refresh`` drift-detection algorithm.

The plan's Step 9 drift-case matrix:

| Drift case | Algorithm outcome |
|---|---|
| Fingerprint identical | No-op exit 0 |
| Cosmetic columns added (no pinned candidate affected) | Re-walk → same pinned value → silent re-pin → new fingerprint + new evidence snapshot |
| Pinned candidate column removed, lower-priority candidate exists | Walker drops to lower-priority → confirmation prompt |
| Pinned candidate column removed, no fallback | NoMatch → AIDPF-2010 artifact |
| Higher-priority candidate now present | Walker promotes → confirmation prompt (pinned value changes) |
| Same-name type drift on pinned candidate | Walker still matches (existence-based); fingerprint update lands new evidence snapshot |
| Semantic-variant detect clause newly satisfied or newly broken | Walker re-evaluates; same outcome paths as columnAliases |

Plus the §9.5.5 invariant: bootstrap NEVER emits AIDPF-2012 (runtime
preflight owns that error code).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
    RefreshRequiresConfirmation,
    VariationPhaseOptions,
    run_variation_phase,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_ROOT = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


DEMO_POD_BRONZE_BASELINE: dict[str, list[str]] = {
    "erp_suppliers": ["VENDORID", "SEGMENT1"],
    "ap_invoices": ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCancelledDate"],
    "gl_coa": [
        "CodeCombinationSegment1",
        "CodeCombinationSegment2",
        "CodeCombinationSegment3",
    ],
    "gl_period_balances": ["PeriodNetCredit"],
}


def _row(col_name: str, data_type: str = "string"):
    return {"col_name": col_name, "data_type": data_type, "comment": None}


def _mock_spark(per_table_columns: dict[str, list[str]]) -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        dataset = target.split(".")[-1]
        cols = per_table_columns.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = [_row(c) for c in cols]
        return df

    spark.sql.side_effect = _sql
    return spark


@pytest.fixture
def bundle_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("USER", "alice@oracle.com")
    bundle_yaml = tmp_path / "bundle.yaml"
    bundle_yaml.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "aidp-fusion-autopilot/v1",
                "version": "0.2.0",
                "project": "test",
                "fusion": {
                    "serviceUrl": "https://example.invalid",
                    "username": "stub",
                    "password": "stub",
                    "externalStorage": "stub",
                },
                "aidp": {
                    "catalog": "cat",
                    "bronzeSchema": "bronze",
                    "silverSchema": "silver",
                    "goldSchema": "gold",
                    "storageFormat": "delta",
                },
                "datasets": [
                    {"id": "erp_suppliers", "mode": "full"},
                    {"id": "ap_invoices", "mode": "incremental"},
                    {"id": "gl_coa", "mode": "full"},
                    {"id": "gl_period_balances", "mode": "full"},
                ],
                "contentPack": {
                    "name": "fusion-finance-starter",
                    "path": str(PACK_ROOT),
                    "profile": "finance-default",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return tmp_path


def _load_bundle(bundle_path: Path) -> Bundle:
    raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    return Bundle.model_validate(raw)


def _do_initial_bootstrap(bundle_dir: Path, bronze: dict[str, list[str]]) -> dict:
    """Run an initial (non-refresh) bootstrap and return the profile dict."""
    bundle = _load_bundle(bundle_dir / "bundle.yaml")
    outcome = run_variation_phase(
        bundle,
        bundle_dir / "bundle.yaml",
        options=VariationPhaseOptions(
            spark_session=_mock_spark(bronze),
            non_interactive=True, accept_coa_convention=True,
        ),
    )
    assert outcome.exit_code == 0, "initial bootstrap should succeed"
    return yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Drift case 1 — fingerprint identical → no-op
# ---------------------------------------------------------------------------


class TestNoDriftNoOp:
    def test_identical_fingerprint_no_op(self, bundle_dir: Path) -> None:
        _do_initial_bootstrap(bundle_dir, DEMO_POD_BRONZE_BASELINE)
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        # Run --refresh with the same bronze.
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE_BASELINE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 0
        # No-op — profile_path / evidence_path are None.
        assert outcome.profile_path is None
        assert outcome.evidence_path is None
        assert "no drift" in outcome.summary.lower()


# ---------------------------------------------------------------------------
# Drift case 2 — cosmetic column added (pinned candidate still matches)
# ---------------------------------------------------------------------------


class TestCosmeticDriftReWritesEvidence:
    def test_added_unrelated_column_writes_new_evidence(
        self, bundle_dir: Path
    ) -> None:
        _do_initial_bootstrap(bundle_dir, DEMO_POD_BRONZE_BASELINE)
        # Drifted: add a cosmetic column nobody references.
        drifted = {
            **DEMO_POD_BRONZE_BASELINE,
            "erp_suppliers": [*DEMO_POD_BRONZE_BASELINE["erp_suppliers"], "NewCosmeticCol"],
        }
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 0
        assert outcome.profile_path is not None
        assert outcome.evidence_path is not None
        # Two evidence snapshots in the directory (initial + refresh).
        snaps = sorted((bundle_dir / "evidence" / "finance-default").iterdir())
        assert len(snaps) == 2, "old evidence must be preserved"


# ---------------------------------------------------------------------------
# Drift case 3 — pinned candidate column removed, no fallback → AIDPF-2010
# ---------------------------------------------------------------------------


class TestPinnedRemovedNoFallback:
    def test_drops_to_aidpf_2010_when_required_vp_missing(
        self, bundle_dir: Path
    ) -> None:
        _do_initial_bootstrap(bundle_dir, DEMO_POD_BRONZE_BASELINE)
        # Drifted: drop VENDORID. Single-candidate columnAlias → NoMatch.
        drifted = {
            **DEMO_POD_BRONZE_BASELINE,
            "erp_suppliers": ["SEGMENT1"],
        }
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 1
        names = [p.name for p in outcome.diagnostic_paths]
        assert "AIDPF-2010__vendor_id.json" in names
        # Profile unchanged — prior profile remains.
        assert outcome.profile_path is None


# ---------------------------------------------------------------------------
# Drift case 4 — higher-priority candidate now present → refresh non-interactive refuses
# ---------------------------------------------------------------------------


class TestRefreshChangePinnedValueRefusedInNonInteractive:
    def test_invoice_currency_promotion_refused_in_non_interactive(
        self, bundle_dir: Path
    ) -> None:
        # Initial bootstrap on a bronze that pins ApInvoicesCurrencyCode
        # (only the second-priority candidate exists).
        initial = {
            **DEMO_POD_BRONZE_BASELINE,
            "ap_invoices": ["ApInvoicesCurrencyCode", "ApInvoicesCancelledDate"],
        }
        _do_initial_bootstrap(bundle_dir, initial)
        profile = yaml.safe_load(
            (bundle_dir / "profiles" / "finance-default.yaml").read_text(
                encoding="utf-8"
            )
        )
        assert profile["resolved"]["column"]["invoice_currency_code"] == (
            "ApInvoicesCurrencyCode"
        )

        # Now refresh with the higher-priority candidate present.
        drifted = {
            **DEMO_POD_BRONZE_BASELINE,
            "ap_invoices": [
                "ApInvoicesInvoiceCurrencyCode",
                "ApInvoicesCurrencyCode",
                "ApInvoicesCancelledDate",
            ],
        }
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        with pytest.raises(RefreshRequiresConfirmation):
            run_variation_phase(
                bundle,
                bundle_dir / "bundle.yaml",
                options=VariationPhaseOptions(
                    spark_session=_mock_spark(drifted),
                    non_interactive=True, accept_coa_convention=True,
                    refresh=True,
                ),
            )


# ---------------------------------------------------------------------------
# Drift case 5 — refresh with scripted resolution (cli_flag mechanism)
# ---------------------------------------------------------------------------


class TestRefreshWithScriptedResolution:
    def test_scripted_resolution_resolves_promotion(self, bundle_dir: Path) -> None:
        # Initial: only ApInvoicesCurrencyCode present.
        initial = {
            **DEMO_POD_BRONZE_BASELINE,
            "ap_invoices": ["ApInvoicesCurrencyCode", "ApInvoicesCancelledDate"],
        }
        _do_initial_bootstrap(bundle_dir, initial)

        # Refresh: both candidates present + scripted resolution picks the promotion.
        drifted = {
            **DEMO_POD_BRONZE_BASELINE,
            "ap_invoices": [
                "ApInvoicesInvoiceCurrencyCode",
                "ApInvoicesCurrencyCode",
                "ApInvoicesCancelledDate",
            ],
        }
        resolutions_file = bundle_dir / "refresh_resolutions.json"
        resolutions_file.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "tenant": "finance-default",
                    "resolutions": [
                        {
                            "name": "invoice_currency_code",
                            "kind": "columnAliases",
                            "chosenCandidate": "ApInvoicesInvoiceCurrencyCode",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted),
                refresh=True,
                resolutions_path=resolutions_file,
            ),
        )
        assert outcome.exit_code == 0
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        assert profile["resolved"]["column"]["invoice_currency_code"] == (
            "ApInvoicesInvoiceCurrencyCode"
        )


# ---------------------------------------------------------------------------
# Invariant — bootstrap NEVER emits AIDPF-2012
# ---------------------------------------------------------------------------


class TestBootstrapNeverEmits2012:
    """The §9.5.5 contract — `--refresh` must NEVER write an AIDPF-2012
    artifact. Runtime preflight (feature #4) owns 2012."""

    def test_refresh_against_drifted_bronze_no_2012(self, bundle_dir: Path) -> None:
        _do_initial_bootstrap(bundle_dir, DEMO_POD_BRONZE_BASELINE)
        drifted = {
            **DEMO_POD_BRONZE_BASELINE,
            "erp_suppliers": [*DEMO_POD_BRONZE_BASELINE["erp_suppliers"], "X"],
        }
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 0
        # No diagnostic artifacts at all on resolved drift.
        for diag_name in (p.name for p in outcome.diagnostic_paths):
            assert "2012" not in diag_name

    def test_refresh_with_nomatch_emits_2010_not_2012(
        self, bundle_dir: Path
    ) -> None:
        _do_initial_bootstrap(bundle_dir, DEMO_POD_BRONZE_BASELINE)
        drifted = {
            **DEMO_POD_BRONZE_BASELINE,
            "erp_suppliers": ["SEGMENT1"],  # drop VENDORID
        }
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 1
        names = [p.name for p in outcome.diagnostic_paths]
        # 2010 only — no 2012.
        assert all("AIDPF-2010" in n for n in names)
        assert all("2012" not in n for n in names)
