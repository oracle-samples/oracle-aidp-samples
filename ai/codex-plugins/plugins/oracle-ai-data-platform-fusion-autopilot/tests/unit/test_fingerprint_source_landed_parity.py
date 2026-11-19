"""Fingerprint parity guard (Risk R1, plan Step 7).

The fresh-tenant feature's correctness hinges on one property: the
``bronzeSchemaFingerprint`` computed from the BICC **source** schema (no
audit columns) must equal the one later computed from the **landed** Delta
table (audit columns present but stripped). If they diverge, the runtime
``check_bronze_fingerprint_drift`` gate would fire on the first post-seed
run. These tests pin that equality so a regression is caught in CI, not on a
live tenant.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    BRONZE_AUDIT_COLUMNS,
    ColumnInfo,
    compute_bronze_fingerprint,
)

# Real (non-audit) columns a node exposes — identical on both sides.
_REAL = [
    ColumnInfo(name="VendorId", type="bigint"),
    ColumnInfo(name="Segment1", type="string"),
    ColumnInfo(name="Amount", type="decimal(38,2)"),
]

_AUDIT = [ColumnInfo(name=n, type="string") for n in sorted(BRONZE_AUDIT_COLUMNS)]


class TestSourceLandedParity:
    def test_audit_columns_do_not_change_fingerprint(self) -> None:
        """Source (no audit cols) == landed (real + audit cols)."""
        source_observed = {"erp_suppliers": list(_REAL)}
        landed_observed = {"erp_suppliers": list(_REAL) + list(_AUDIT)}
        assert (
            compute_bronze_fingerprint(observed=source_observed)
            == compute_bronze_fingerprint(observed=landed_observed)
        )

    def test_audit_interleaved_with_real_columns(self) -> None:
        """Audit columns interleaved (as a real DESCRIBE might emit) still
        strip out to the same fingerprint."""
        landed_interleaved = {
            "erp_suppliers": [
                _REAL[0],
                _AUDIT[0],
                _REAL[1],
                _AUDIT[1],
                _REAL[2],
            ]
        }
        source_observed = {"erp_suppliers": list(_REAL)}
        assert (
            compute_bronze_fingerprint(observed=source_observed)
            == compute_bronze_fingerprint(observed=landed_interleaved)
        )

    def test_column_reorder_is_stable(self) -> None:
        """Cosmetic column reordering (Spark physical order) does not change
        the fingerprint — so source and landed orderings agree."""
        source_observed = {"erp_suppliers": list(_REAL)}
        landed_reordered = {"erp_suppliers": list(reversed(_REAL)) + list(_AUDIT)}
        assert (
            compute_bronze_fingerprint(observed=source_observed)
            == compute_bronze_fingerprint(observed=landed_reordered)
        )

    def test_genuine_type_difference_still_diverges(self) -> None:
        """Guard the guard: a real type coercion (source bigint vs landed
        string) MUST diverge — otherwise the parity test would be vacuous and
        mask the R1 risk it exists to catch."""
        source_observed = {"erp_suppliers": [ColumnInfo(name="VendorId", type="bigint")]}
        landed_coerced = {"erp_suppliers": [ColumnInfo(name="VendorId", type="string")]}
        assert (
            compute_bronze_fingerprint(observed=source_observed)
            != compute_bronze_fingerprint(observed=landed_coerced)
        )

    def test_multi_dataset_parity(self) -> None:
        source = {
            "erp_suppliers": list(_REAL),
            "ap_invoices": [ColumnInfo(name="InvoiceId", type="bigint")],
        }
        landed = {
            "erp_suppliers": list(_REAL) + list(_AUDIT),
            "ap_invoices": [ColumnInfo(name="InvoiceId", type="bigint"), *_AUDIT],
        }
        assert (
            compute_bronze_fingerprint(observed=source)
            == compute_bronze_fingerprint(observed=landed)
        )
