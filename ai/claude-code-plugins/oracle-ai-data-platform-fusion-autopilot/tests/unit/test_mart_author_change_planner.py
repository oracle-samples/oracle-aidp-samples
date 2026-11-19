from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_planner():
    module_path = (
        Path(__file__).resolve().parents[2]
        / "skills"
        / "mart-author"
        / "change_planner.py"
    )
    spec = importlib.util.spec_from_file_location("mart_author_change_planner", module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _base_payload(field: dict) -> dict:
    return {
        "request": {
            "id": "custom_gold",
            "targetLayer": "gold",
            "grain": ["business_id"],
            "isAggregate": False,
            "columns": [{"name": "business_id", "pii": "none"}],
        },
        "fields": [field],
    }


def test_rung4_warns_when_pvo_classification_is_missing() -> None:
    planner = _load_planner()

    result = planner.plan(
        _base_payload(
            {
                "name": "legacy_amount",
                "source": "pvo_only",
                "pvo": "LegacySnapshotExtractPVO",
                "sourceColumn": "Amount",
            }
        )
    )

    assert result["decision"] == "rung_4_new_bronze_plus_node"
    assert result["requiresNewBronze"] is True
    assert any("missing pvoClassification" in w for w in result["warnings"])
    assert (
        result["pvoClassifications"]["LegacySnapshotExtractPVO"][
            "requiresExplicitApproval"
        ]
        is True
    )
    assert result["nodeSpecs"][0]["pvoClassification"] is None


def test_rung4_transaction_pvo_recommends_incremental_when_evidence_is_present() -> None:
    planner = _load_planner()

    result = planner.plan(
        _base_payload(
            {
                "name": "invoice_promised_date",
                "source": "pvo_only",
                "pvo": "InvoiceHeaderExtractPVO",
                "sourceColumn": "ApInvoicesPromisedDate",
                "pvoClassification": "transaction_change_feed",
                "metadataLastUpdateColumns": ["ApInvoicesLastUpdateDate"],
                "watermarkColumn": "ApInvoicesLastUpdateDate",
                "businessSemanticsConfirmed": True,
            }
        )
    )

    assert result["warnings"] == []
    assert (
        result["pvoClassifications"]["InvoiceHeaderExtractPVO"][
            "incrementalCapableRecommendation"
        ]
        is True
    )
    assert result["nodeSpecs"][0]["pvoClassification"] == "transaction_change_feed"
    assert result["nodeSpecs"][0]["incrementalCapableRecommendation"] is True


def test_rung4_transaction_pvo_accepts_custom_metadata_watermark_name() -> None:
    planner = _load_planner()

    result = planner.plan(
        _base_payload(
            {
                "name": "custom_status",
                "source": "pvo_only",
                "pvo": "CustomTransactionExtractPVO",
                "sourceColumn": "StatusCode",
                "pvoClassification": "transaction_change_feed",
                "metadataLastUpdateColumns": [{"name": "ObjectLastChangedTs"}],
                "businessSemanticsConfirmed": True,
            }
        )
    )

    assert result["warnings"] == []
    assert (
        result["pvoClassifications"]["CustomTransactionExtractPVO"][
            "lastUpdateDateColumn"
        ]
        == "ObjectLastChangedTs"
    )


def test_rung4_period_windowable_snapshot_requires_policy_or_approval() -> None:
    planner = _load_planner()

    result = planner.plan(
        _base_payload(
            {
                "name": "balance_cube",
                "source": "pvo_only",
                "pvo": "BalanceExtractPVO",
                "sourceColumn": "BalancePeriodNetDr",
                "pvoClassification": "period_windowable_snapshot",
            }
        )
    )

    warnings = "\n".join(result["warnings"])
    assert "extract_window policy" in warnings
    assert "explicit user approval" in warnings
    assert (
        result["pvoClassifications"]["BalanceExtractPVO"][
            "incrementalCapableRecommendation"
        ]
        is False
    )
    assert (
        result["pvoClassifications"]["BalanceExtractPVO"][
            "requiresExtractWindowPolicy"
        ]
        is True
    )


def test_rung4_period_windowable_snapshot_accepts_policy_as_safety_path() -> None:
    planner = _load_planner()

    result = planner.plan(
        _base_payload(
            {
                "name": "balance_cube",
                "source": "pvo_only",
                "pvo": "BalanceExtractPVO",
                "sourceColumn": "BalancePeriodNetDr",
                "pvoClassification": "period_windowable_snapshot",
                "hasExtractWindowPolicy": True,
            }
        )
    )

    assert result["warnings"] == []
    assert (
        result["pvoClassifications"]["BalanceExtractPVO"][
            "requiresExplicitApproval"
        ]
        is False
    )
