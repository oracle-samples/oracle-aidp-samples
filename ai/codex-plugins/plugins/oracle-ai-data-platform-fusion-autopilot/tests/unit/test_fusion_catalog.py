"""Unit tests for the curated PVO catalog."""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.fusion_catalog import (
    CATALOG,
    PvoKind,
    get,
    list_confirmed,
    list_verify_live,
)


class TestCatalog:
    def test_supplier_extract_confirmed(self) -> None:
        e = get("erp_suppliers")
        # Full AM-hierarchy verified live 2026-04-30 against demo-pod
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
        # (demo-pod). list_verify_live() returns nothing until new placeholders are added.
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

    def test_all_bronze_table_names_are_bare_identifiers(self) -> None:
        # Replaces test_all_bronze_tables_under_fusion_catalog (2026-05-17 — §4.8a Option A
        # migration). bronze_table_name is the single segment; the catalog + bronze_schema
        # come from bundle.yaml.aidp.*. Dots/hyphens rejected.
        import re
        ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        for entry in CATALOG.values():
            assert ident_re.match(entry.bronze_table_name), (
                f"{entry.id} bronze_table_name={entry.bronze_table_name!r} is not a single "
                "SQL identifier — must match ^[A-Za-z_][A-Za-z0-9_]*$"
            )
            assert "." not in entry.bronze_table_name, (
                f"{entry.id}: bronze_table_name must not contain a dot "
                "(catalog/schema prefix lives in TablePaths, not the catalog)"
            )

    def test_ar_aging_not_in_catalog(self) -> None:
        # Regression guard for P3.35 (2026-05-17): _AR_AGING was deleted as a
        # documentation-only duplicate of ar_invoices's datastore. Re-adding it
        # would resurrect the cross-layer collision against GOLD_MARTS["ar_aging"].
        assert "ar_aging" not in CATALOG, (
            "ar_aging was removed from catalog (P3.35); gold ar_aging mart "
            "references ar_invoices + ar_receipts directly."
        )

    def test_ap_aging_periods_in_catalog(self) -> None:
        # P3.36 (2026-05-17): _AP_AGING.id renamed to "ap_aging_periods" to
        # disambiguate from the gold ap_aging mart and align with bronze_table_name.
        assert "ap_aging_periods" in CATALOG, "ap_aging_periods missing after P3.36 rename"
        assert "ap_aging" not in CATALOG, "ap_aging bronze id removed by P3.36 rename"
        entry = CATALOG["ap_aging_periods"]
        assert entry.datastore == "FscmTopModelAM.FinExtractAM.ApBiccExtractAM.AgingPeriodHeaderExtractPVO"
        assert entry.bronze_table_name == "ap_aging_periods"

    def test_hcm_worker_assignments_kind_is_saas_batch(self) -> None:
        # R2 (2026-05-17): hcm_worker_assignments uses a saas-batch REST extractor,
        # NOT BICC. The kind tag steers the orchestrator to KNOWN_DEFERRED_DATASETS
        # (vs BRONZE_EXTRACTS) until a concrete saas-batch extractor ships.
        entry = get("hcm_worker_assignments")
        assert entry.kind == PvoKind.SAAS_BATCH, (
            f"hcm_worker_assignments.kind should be SAAS_BATCH "
            f"(not BICC-extractable); got {entry.kind}"
        )

    def test_unique_ids(self) -> None:
        ids = [e.id for e in CATALOG.values()]
        assert len(ids) == len(set(ids)), "duplicate ids in CATALOG"

    def test_datastore_names_unique(self) -> None:
        # Tightened from "mostly unique" after P3.35 deleted the _AR_AGING
        # documentation-only duplicate of ar_invoices's datastore. Strict
        # uniqueness now — if a future entry needs to share a datastore,
        # justify it explicitly here.
        datastores = [e.datastore for e in CATALOG.values()]
        from collections import Counter
        counts = Counter(datastores)
        dupes = {ds: n for ds, n in counts.items() if n > 1}
        assert not dupes, f"duplicate datastore names: {dupes}"


class TestExtractColumnsDefault:
    def test_default_is_empty_meaning_all_columns(self) -> None:
        e = get("erp_suppliers")
        # Empty list = pull all columns (BICC default). Pdf1 Pro Tip
        # recommends pruning later; default-empty is correct for v0.1.
        assert e.extract_columns == []
