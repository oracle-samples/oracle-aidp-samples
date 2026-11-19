"""Regression tests for the ``--refresh`` pinned-value-change
confirmation prompt (round-1 review, blocking #2).

The prior implementation printed a ``Confirm? (y/N)`` line but never
read input — every interactive refresh silently accepted the change.
The fix:

1. Read an actual y/N answer; default no.
2. On decline, abort the run with exit code 1; do NOT write profile
   or evidence.
3. On accept, record ``mechanism: terminal_prompt`` in the snapshot's
   provenance.

The ``--resolutions`` flag also gains a new acceptance path for
``--refresh``: scripted approval of an AutoResolved promotion records
``mechanism: cli_flag`` instead of being rejected as extraneous
(round-1 review, should-fix #1).
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


# Initial-onboard bronze: only ApInvoicesCurrencyCode (the lower-priority
# candidate) exists; bootstrap pins it.
_INITIAL_BRONZE = {
    "erp_suppliers": ["VENDORID", "SEGMENT1"],
    "ap_invoices": ["ApInvoicesCurrencyCode", "ApInvoicesCancelledDate"],
    "gl_coa": [
        "CodeCombinationSegment1",
        "CodeCombinationSegment2",
        "CodeCombinationSegment3",
    ],
    "gl_period_balances": [],
}

# Refresh bronze (AutoResolved promotion): the higher-priority
# ApInvoicesInvoiceCurrencyCode now exists; the lower-priority
# ApInvoicesCurrencyCode no longer does. The walker returns AutoResolved
# on the higher-priority candidate; refresh would change the pinned
# value from the lower-priority candidate to the higher-priority one
# WITHOUT going through a MultiMatch prompt — exactly the case that
# exercises the refresh y/N confirmation in isolation.
_DRIFTED_BRONZE = {
    **_INITIAL_BRONZE,
    "ap_invoices": [
        "ApInvoicesInvoiceCurrencyCode",
        "ApInvoicesCancelledDate",
    ],
}

# Refresh bronze (MultiMatch + promotion): both candidates present —
# used by the scripted-acceptance test to drive the multi-match prompt
# AND the refresh-change confirmation.
_DRIFTED_BRONZE_MULTI = {
    **_INITIAL_BRONZE,
    "ap_invoices": [
        "ApInvoicesInvoiceCurrencyCode",
        "ApInvoicesCurrencyCode",
        "ApInvoicesCancelledDate",
    ],
}


def _do_initial_bootstrap(bundle_dir: Path) -> None:
    bundle = _load_bundle(bundle_dir / "bundle.yaml")
    outcome = run_variation_phase(
        bundle,
        bundle_dir / "bundle.yaml",
        options=VariationPhaseOptions(
            spark_session=_mock_spark(_INITIAL_BRONZE),
            non_interactive=True, accept_coa_convention=True,
        ),
    )
    assert outcome.exit_code == 0
    profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
    assert profile["resolved"]["column"]["invoice_currency_code"] == (
        "ApInvoicesCurrencyCode"
    )


class TestRefreshDeclinePath:
    """Operator declines the change → refresh aborts; profile +
    evidence unchanged."""

    def test_decline_aborts_run(self, bundle_dir: Path) -> None:
        _do_initial_bootstrap(bundle_dir)
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        evidence_dir = bundle_dir / "evidence" / "finance-default"
        before_evidence_count = len(list(evidence_dir.iterdir()))
        profile_before = (bundle_dir / "profiles" / "finance-default.yaml").read_text(
            encoding="utf-8"
        )

        # Operator types "n" → decline.
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(_DRIFTED_BRONZE),
                refresh=True,
                input_fn=lambda _: "n",
            ),
        )
        assert outcome.exit_code == 1
        assert "declined" in outcome.summary.lower() or "abort" in outcome.summary.lower()
        # Profile unchanged.
        profile_after = (bundle_dir / "profiles" / "finance-default.yaml").read_text(
            encoding="utf-8"
        )
        assert profile_after == profile_before
        # No new evidence snapshot.
        assert len(list(evidence_dir.iterdir())) == before_evidence_count

    def test_default_enter_aborts_run(self, bundle_dir: Path) -> None:
        """Empty input (operator pressed Enter) → default-No → abort."""
        _do_initial_bootstrap(bundle_dir)
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(_DRIFTED_BRONZE),
                refresh=True,
                input_fn=lambda _: "",
            ),
        )
        assert outcome.exit_code == 1


class TestRefreshAcceptPath:
    """Operator types y → refresh writes new profile + evidence."""

    def test_accept_writes_new_profile(self, bundle_dir: Path) -> None:
        _do_initial_bootstrap(bundle_dir)
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(_DRIFTED_BRONZE),
                refresh=True,
                input_fn=lambda _: "y",
            ),
        )
        assert outcome.exit_code == 0
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        # Promotion landed: now pinned to the higher-priority candidate.
        assert profile["resolved"]["column"]["invoice_currency_code"] == (
            "ApInvoicesInvoiceCurrencyCode"
        )
        # Mechanism recorded as terminal_prompt (y/N was the interaction).
        assert profile["provenance"]["approvedBy"]["mechanism"] == "terminal_prompt"


class TestRefreshScriptedAcceptance:
    """`--resolutions` accepting a CHANGED AutoResolved promotion
    records the cli_flag mechanism and proceeds without an interactive
    prompt — the round-1 should-fix #1 case. Round-2 review caught that
    the original test used a MultiMatch bronze rather than an
    AutoResolved-only one, masking the bug; this test now uses
    `_DRIFTED_BRONZE` (only the higher-priority candidate present) so
    the walker returns AutoResolved and the scripted-acceptance path
    actually fires."""

    def test_resolutions_accepts_autoresolved_promotion(
        self, bundle_dir: Path
    ) -> None:
        _do_initial_bootstrap(bundle_dir)
        resolutions = bundle_dir / "approve.json"
        resolutions.write_text(
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
                # AutoResolved-only: only the higher-priority candidate
                # is present on the refreshed bronze. Walker returns
                # AutoResolved(chosen=ApInvoicesInvoiceCurrencyCode);
                # prior pinned = ApInvoicesCurrencyCode → change requires
                # operator approval. --resolutions supplies that approval.
                spark_session=_mock_spark(_DRIFTED_BRONZE),
                refresh=True,
                non_interactive=True, accept_coa_convention=True,  # would otherwise refuse silent change
                resolutions_path=resolutions,
                # No input_fn — prompt must NOT be invoked.
            ),
        )
        assert outcome.exit_code == 0
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        assert profile["resolved"]["column"]["invoice_currency_code"] == (
            "ApInvoicesInvoiceCurrencyCode"
        )
        # cli_flag wins precedence over auto_resolve.
        assert profile["provenance"]["approvedBy"]["mechanism"] == "cli_flag"

    def test_resolutions_rejects_candidate_not_on_bronze(
        self, bundle_dir: Path
    ) -> None:
        """The scripted entry's chosenCandidate MUST equal the
        walker's AutoResolved value. Naming a candidate the walker did
        NOT pick (e.g. one that doesn't exist on the refreshed bronze)
        is a rejected as a bad candidate — accepting it would silently
        pin a value that isn't actually on bronze."""
        _do_initial_bootstrap(bundle_dir)
        resolutions = bundle_dir / "bad.json"
        resolutions.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "tenant": "finance-default",
                    "resolutions": [
                        {
                            "name": "invoice_currency_code",
                            "kind": "columnAliases",
                            # The walker picked ApInvoicesInvoiceCurrencyCode
                            # because that's the only candidate present in
                            # _DRIFTED_BRONZE. Pinning ApInvoicesCurrencyCode
                            # would be a lie — it's not on bronze any more.
                            "chosenCandidate": "ApInvoicesCurrencyCode",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        from oracle_ai_data_platform_fusion_autopilot.schema.resolutions_input import (
            ResolutionsFileBadCandidate,
        )
        with pytest.raises(ResolutionsFileBadCandidate):
            run_variation_phase(
                bundle,
                bundle_dir / "bundle.yaml",
                options=VariationPhaseOptions(
                    spark_session=_mock_spark(_DRIFTED_BRONZE),
                    refresh=True,
                    non_interactive=True, accept_coa_convention=True,
                    resolutions_path=resolutions,
                ),
            )


class TestNonInteractiveStillRefusesWithoutResolutions:
    """`--refresh --non-interactive` without a matching `--resolutions`
    entry still raises (regression on the existing behaviour after the
    fix)."""

    def test_non_interactive_no_resolutions_still_raises(
        self, bundle_dir: Path
    ) -> None:
        _do_initial_bootstrap(bundle_dir)
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        with pytest.raises(RefreshRequiresConfirmation):
            run_variation_phase(
                bundle,
                bundle_dir / "bundle.yaml",
                options=VariationPhaseOptions(
                    spark_session=_mock_spark(_DRIFTED_BRONZE),
                    refresh=True,
                    non_interactive=True, accept_coa_convention=True,
                ),
            )
