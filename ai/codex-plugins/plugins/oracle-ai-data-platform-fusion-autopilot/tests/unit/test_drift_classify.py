"""Tests for the fusion-drift-doctor classifier — Fusion PVO column drift routing.

The load-bearing case: a PVO column the pack maps via columnAliases gets renamed.
If another declared candidate matches -> bootstrap --refresh; if none -> overlay.
"""
from __future__ import annotations

import sys
from pathlib import Path

_SKILL = Path(__file__).resolve().parents[2] / "skills" / "fusion-drift-doctor"
sys.path.insert(0, str(_SKILL))

import drift_classify  # noqa: E402


def _alias(name, source, resolved, candidates):
    return {"name": name, "source": source, "resolved": resolved, "candidates": candidates}


def test_present_no_drift():
    out = drift_classify.classify({
        "live": {"ap_invoices": ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesVendorId"]},
        "aliases": [_alias("invoice_currency_code", "ap_invoices",
                           "ApInvoicesInvoiceCurrencyCode",
                           ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"])],
    })
    assert out["ok"] is True
    assert out["findings"][0]["status"] == "present"


def test_renamed_resolvable_routes_to_bootstrap_refresh():
    # resolved column gone, but the OTHER declared candidate is now present.
    out = drift_classify.classify({
        "live": {"ap_invoices": ["ApInvoicesCurrencyCode", "ApInvoicesVendorId"]},
        "aliases": [_alias("invoice_currency_code", "ap_invoices",
                           "ApInvoicesInvoiceCurrencyCode",
                           ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"])],
    })
    f = out["findings"][0]
    assert f["status"] == "renamed_resolvable"
    assert f["observedCandidate"] == "ApInvoicesCurrencyCode"
    assert out["routes"]["bootstrap_refresh"] == ["invoice_currency_code"]
    assert out["ok"] is False


def test_needs_overlay_when_no_candidate_present():
    # A brand-new column name the pack never anticipated -> medallion-author.
    out = drift_classify.classify({
        "live": {"ap_invoices": ["ApInvoicesTrxCurrency", "ApInvoicesVendorId"]},
        "aliases": [_alias("invoice_currency_code", "ap_invoices",
                           "ApInvoicesInvoiceCurrencyCode",
                           ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"])],
    })
    f = out["findings"][0]
    assert f["status"] == "needs_overlay"
    assert out["routes"]["medallion_author"] == ["invoice_currency_code"]
    assert "ApInvoicesTrxCurrency" in f["liveColumns"]   # surfaced to help author the overlay


def test_missing_literal_routes_to_investigate():
    out = drift_classify.classify({
        "live": {"ap_invoices": ["ApInvoicesCurrencyCode"]},
        "aliases": [],
        "required_literals": [{"source": "ap_invoices", "column": "ApInvoicesVendorId"}],
    })
    f = out["findings"][0]
    assert f["status"] == "missing_literal"
    # Primary route stays investigate (human decides legitimacy) ...
    assert out["routes"]["investigate"] == ["ApInvoicesVendorId"]
    # ... and the legitimate-absence remediation is surfaced machine-readably
    # (relaxRequiredColumns overlay via /medallion-author) without auto-routing.
    assert f["relax_route"] == "medallion_author"
    assert out["routes"]["relax_overlay"] == ["ApInvoicesVendorId"]


def test_case_insensitive_match():
    out = drift_classify.classify({
        "live": {"erp_suppliers": ["vendorid"]},
        "aliases": [_alias("vendor_id", "erp_suppliers", "VENDORID", ["VENDORID"])],
    })
    assert out["findings"][0]["status"] == "present"


def test_mixed_routes_summary():
    out = drift_classify.classify({
        "live": {"ap_invoices": ["ApInvoicesCurrencyCode"], "gl_coa": ["WeirdSeg"]},
        "aliases": [
            _alias("invoice_currency_code", "ap_invoices", "ApInvoicesInvoiceCurrencyCode",
                   ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"]),     # resolvable
            _alias("coa_balancing_segment", "gl_coa", "CodeCombinationSegment1",
                   ["CodeCombinationSegment1"]),                                      # overlay
        ],
    })
    assert out["routes"]["bootstrap_refresh"] == ["invoice_currency_code"]
    assert out["routes"]["medallion_author"] == ["coa_balancing_segment"]
    assert out["ok"] is False
