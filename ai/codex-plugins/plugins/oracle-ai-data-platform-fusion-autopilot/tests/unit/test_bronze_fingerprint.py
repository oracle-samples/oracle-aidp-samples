"""Unit tests for :mod:`oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint`.

Feature #2 (this feature) PINS the fingerprint; feature #4
(``v2-phase-3c-runtime-preflight-evidence``) will COMPARE the live
bronze against the pinned value. The tests below pin the algorithm
properties both features rely on:

* Empty input deterministic + same-input idempotent.
* Cosmetic invariance: column reordering / nullability flip / duplicate
  rows do not change the fingerprint.
* Drift sensitivity: any real type change OR a new / removed column
  produces a different fingerprint.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    BRONZE_AUDIT_COLUMNS,
    ColumnInfo,
    compute_bronze_fingerprint,
    strip_audit_columns,
)


class TestEmptyAndIdempotent:
    def test_empty_observation_returns_deterministic_hash(self) -> None:
        fp1 = compute_bronze_fingerprint(observed={})
        fp2 = compute_bronze_fingerprint(observed={})
        assert fp1 == fp2
        assert fp1.startswith("sha256:")
        # 64 hex chars after the prefix.
        assert len(fp1) == len("sha256:") + 64

    def test_same_observation_idempotent(self) -> None:
        observed = {
            "erp_suppliers": [
                ColumnInfo(name="VENDORID", type="string"),
                ColumnInfo(name="SEGMENT1", type="string"),
            ],
        }
        assert compute_bronze_fingerprint(observed=observed) == compute_bronze_fingerprint(
            observed=observed
        )


class TestCosmeticInvariance:
    def test_column_reordering_yields_same_fingerprint(self) -> None:
        ordered = {
            "erp_suppliers": [
                ColumnInfo(name="VENDORID", type="string"),
                ColumnInfo(name="SEGMENT1", type="string"),
            ],
        }
        reversed_order = {
            "erp_suppliers": [
                ColumnInfo(name="SEGMENT1", type="string"),
                ColumnInfo(name="VENDORID", type="string"),
            ],
        }
        assert compute_bronze_fingerprint(observed=ordered) == compute_bronze_fingerprint(
            observed=reversed_order
        )

    def test_nullability_flip_yields_same_fingerprint(self) -> None:
        nullable = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string", nullable=True),
            ],
        }
        not_null = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string", nullable=False),
            ],
        }
        assert compute_bronze_fingerprint(observed=nullable) == compute_bronze_fingerprint(
            observed=not_null
        )

    def test_duplicate_column_rows_dropped(self) -> None:
        observed = {
            "gl_coa": [
                ColumnInfo(name="CodeCombinationSegment1", type="string"),
                ColumnInfo(name="CodeCombinationSegment1", type="string"),
            ],
        }
        deduped = {
            "gl_coa": [
                ColumnInfo(name="CodeCombinationSegment1", type="string"),
            ],
        }
        assert compute_bronze_fingerprint(observed=observed) == compute_bronze_fingerprint(
            observed=deduped
        )

    def test_dataset_ordering_yields_same_fingerprint(self) -> None:
        forward = {
            "ap_invoices": [ColumnInfo(name="x", type="string")],
            "gl_coa": [ColumnInfo(name="y", type="string")],
        }
        reversed_obs = {
            "gl_coa": [ColumnInfo(name="y", type="string")],
            "ap_invoices": [ColumnInfo(name="x", type="string")],
        }
        assert compute_bronze_fingerprint(observed=forward) == compute_bronze_fingerprint(
            observed=reversed_obs
        )

    def test_case_normalised(self) -> None:
        """``VENDORID`` and ``vendorid`` resolve to the same column for
        fingerprint purposes — Fusion BICC sometimes ships UPPER, the
        spark connector sometimes lowercases."""
        upper = {"erp_suppliers": [ColumnInfo(name="VENDORID", type="STRING")]}
        lower = {"erp_suppliers": [ColumnInfo(name="vendorid", type="string")]}
        assert compute_bronze_fingerprint(observed=upper) == compute_bronze_fingerprint(
            observed=lower
        )


class TestDriftSensitivity:
    def test_real_type_change_yields_different_fingerprint(self) -> None:
        before = {
            "gl_period_balances": [
                ColumnInfo(name="PeriodNetCredit", type="string"),
            ],
        }
        after = {
            "gl_period_balances": [
                ColumnInfo(name="PeriodNetCredit", type="bigint"),
            ],
        }
        assert compute_bronze_fingerprint(observed=before) != compute_bronze_fingerprint(
            observed=after
        )

    def test_added_column_yields_different_fingerprint(self) -> None:
        before = {
            "erp_suppliers": [ColumnInfo(name="VENDORID", type="string")],
        }
        after = {
            "erp_suppliers": [
                ColumnInfo(name="VENDORID", type="string"),
                ColumnInfo(name="SEGMENT1", type="string"),
            ],
        }
        assert compute_bronze_fingerprint(observed=before) != compute_bronze_fingerprint(
            observed=after
        )

    def test_removed_column_yields_different_fingerprint(self) -> None:
        before = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesCancelledDate", type="timestamp"),
                ColumnInfo(name="ApInvoicesCancelledFlag", type="string"),
            ],
        }
        after = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesCancelledDate", type="timestamp"),
            ],
        }
        assert compute_bronze_fingerprint(observed=before) != compute_bronze_fingerprint(
            observed=after
        )

    def test_added_dataset_yields_different_fingerprint(self) -> None:
        before = {
            "erp_suppliers": [ColumnInfo(name="VENDORID", type="string")],
        }
        after = {
            "erp_suppliers": [ColumnInfo(name="VENDORID", type="string")],
            "ap_invoices": [ColumnInfo(name="ApInvoicesId", type="bigint")],
        }
        assert compute_bronze_fingerprint(observed=before) != compute_bronze_fingerprint(
            observed=after
        )


class TestFingerprintShape:
    def test_returns_sha256_prefixed_hex(self) -> None:
        fp = compute_bronze_fingerprint(
            observed={"erp_suppliers": [ColumnInfo(name="VENDORID", type="string")]}
        )
        assert fp.startswith("sha256:")
        suffix = fp[len("sha256:"):]
        assert len(suffix) == 64
        # Each char is a lowercase hex digit.
        assert all(c in "0123456789abcdef" for c in suffix)


class TestAuditColumnFiltering:
    """Bronze audit columns (`_extract_ts` / `_source_pvo` / `_run_id` /
    `_watermark_used`) are added by the bronze adapter post-extract and
    are NOT part of the BICC PVO contract. `DESCRIBE TABLE` against
    bronze Delta tables emits them; the fingerprint must strip them so
    the Phase 5 drift gate's "snapshot vs live BICC" comparison stays
    on a single axis.
    """

    _AUDIT_COLS = (
        "_extract_ts",
        "_source_pvo",
        "_run_id",
        "_watermark_used",
    )

    def test_known_audit_names_in_constant(self) -> None:
        assert BRONZE_AUDIT_COLUMNS == frozenset(self._AUDIT_COLS)

    def test_fingerprint_ignores_audit_columns(self) -> None:
        pure_bicc = {
            "erp_suppliers": [
                ColumnInfo(name="VENDORID", type="bigint"),
                ColumnInfo(name="SEGMENT1", type="string"),
            ],
        }
        with_audit = {
            "erp_suppliers": [
                ColumnInfo(name="VENDORID", type="bigint"),
                ColumnInfo(name="SEGMENT1", type="string"),
                ColumnInfo(name="_extract_ts", type="timestamp"),
                ColumnInfo(name="_source_pvo", type="string"),
                ColumnInfo(name="_run_id", type="string"),
                ColumnInfo(name="_watermark_used", type="timestamp"),
            ],
        }
        assert compute_bronze_fingerprint(
            observed=pure_bicc
        ) == compute_bronze_fingerprint(observed=with_audit)

    def test_strip_removes_audit_preserves_source(self) -> None:
        observed = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceNum", type="string"),
                ColumnInfo(name="_extract_ts", type="timestamp"),
                ColumnInfo(name="ApInvoicesAmount", type="decimal(38,30)"),
                ColumnInfo(name="_RUN_ID", type="string"),  # case-insensitive
            ],
        }
        stripped = strip_audit_columns(observed)
        names = [c.name for c in stripped["ap_invoices"]]
        assert names == ["ApInvoicesInvoiceNum", "ApInvoicesAmount"]

    def test_strip_is_pure_does_not_mutate_input(self) -> None:
        observed = {
            "erp_suppliers": [
                ColumnInfo(name="VENDORID", type="bigint"),
                ColumnInfo(name="_extract_ts", type="timestamp"),
            ],
        }
        _ = strip_audit_columns(observed)
        assert len(observed["erp_suppliers"]) == 2  # unchanged

    def test_audit_only_dataset_yields_same_fingerprint_as_empty(self) -> None:
        """A dataset whose only columns are audit cols fingerprints
        identically to an empty-column dataset — both contribute zero
        BICC schema content."""
        only_audit = {
            "erp_suppliers": [
                ColumnInfo(name=n, type="string") for n in self._AUDIT_COLS
            ],
        }
        empty = {"erp_suppliers": []}
        assert compute_bronze_fingerprint(
            observed=only_audit
        ) == compute_bronze_fingerprint(observed=empty)
