"""Unit tests for the curated PVO catalog."""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import (
    CATALOG,
    PvoKind,
    get,
    list_confirmed,
    list_verify_live,
)


class TestCatalog:
    def test_supplier_extract_confirmed(self) -> None:
        e = get("erp_suppliers")
        # Full AM-hierarchy verified live 2026-04-30 against saasfademo1
        # (pdf1's "FscmTopModelAM.SupplierExtractPVO" was abbreviated).
        assert e.datastore == "FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO"
        assert e.confirmed is True
        assert e.kind == PvoKind.EXTRACT_PVO

    def test_po_orders_confirmed(self) -> None:
        e = get("po_orders")
        # Verified live 2026-04-30 — pdf2's bare `FscmTopModelAM.PrcExtractPO` was abbreviated.
        assert e.datastore == "FscmTopModelAM.PrcExtractAM.PoBiccExtractAM.PurchasingDocumentHeaderExtractPVO"
        assert e.confirmed is True

    def test_scm_items_confirmed(self) -> None:
        e = get("scm_items")
        # Verified live 2026-04-30 — pdf2's bare `ItemExtractPVO` was abbreviated.
        assert e.datastore == "FscmTopModelAM.ScmExtractAM.EgpBiccExtractAM.ItemExtractPVO"
        assert e.confirmed is True

    def test_hcm_worker_assignments_for_saas_batch(self) -> None:
        e = get("hcm_worker_assignments")
        assert e.datastore == "workerAssignmentExtracts"
        assert e.schema == "HCM"
        assert e.confirmed is True

    def test_unknown_id_raises_with_helpful_message(self) -> None:
        with pytest.raises(KeyError, match="unknown dataset id"):
            get("does_not_exist")

    def test_list_confirmed_includes_all_blog_pvos(self) -> None:
        confirmed_ids = {e.id for e in list_confirmed()}
        # The four PVO names lifted verbatim from pdf1 + pdf2:
        assert confirmed_ids >= {
            "erp_suppliers",        # pdf1 Step 3
            "po_orders",            # pdf2 p2 default
            "scm_items",            # pdf2 p2 default
            "hcm_worker_assignments",  # pdf2 p4 saas-batch path
        }

    def test_list_verify_live_is_empty(self) -> None:
        # As of 2026-04-30 all PVOs in the catalog are confirmed against the live BICC catalog
        # (saasfademo1). list_verify_live() returns nothing until new placeholders are added.
        assert list_verify_live() == []

    def test_gl_trio_confirmed(self) -> None:
        # GL trio uses the verified-live FinExtractAM.GlBiccExtractAM family.
        for gl_id, expected_suffix in [
            ("gl_journal_lines", "JournalHeaderExtractPVO"),
            ("gl_period_balances", "BalanceExtractPVO"),
            ("gl_coa", "CodeCombinationExtractPVO"),
        ]:
            e = get(gl_id)
            assert e.confirmed is True, f"{gl_id} should be confirmed"
            assert e.datastore.startswith("FscmTopModelAM.FinExtractAM.GlBiccExtractAM."), (
                f"{gl_id} datastore {e.datastore!r} should be under GlBiccExtractAM"
            )
            assert e.datastore.endswith(expected_suffix), (
                f"{gl_id} datastore {e.datastore!r} should end with {expected_suffix}"
            )

    def test_all_bronze_tables_under_fusion_catalog(self) -> None:
        for entry in CATALOG.values():
            assert entry.bronze_table.startswith("fusion_catalog.bronze."), (
                f"{entry.id} bronze_table={entry.bronze_table!r} doesn't follow "
                "fusion_catalog.bronze.* convention"
            )

    def test_unique_ids(self) -> None:
        ids = [e.id for e in CATALOG.values()]
        assert len(ids) == len(set(ids)), "duplicate ids in CATALOG"

    def test_datastore_names_mostly_unique(self) -> None:
        # Most datastore names are unique. The exception is `ar_aging` which uses the same
        # TransactionHeaderExtractPVO as `ar_invoices` (Fusion has no direct AR-Aging PVO; the
        # gold AR-aging mart is computed downstream from invoices + receipts). Allow up to 1
        # duplicate, document any others.
        datastores = [e.datastore for e in CATALOG.values()]
        from collections import Counter
        counts = Counter(datastores)
        dupes = {ds: n for ds, n in counts.items() if n > 1}
        assert len(dupes) <= 1, f"unexpected duplicate datastore names: {dupes}"


class TestExtractColumnsDefault:
    def test_default_is_empty_meaning_all_columns(self) -> None:
        e = get("erp_suppliers")
        # Empty list = pull all columns (BICC default). Pdf1 Pro Tip
        # recommends pruning later; default-empty is correct for v0.1.
        assert e.extract_columns == []
