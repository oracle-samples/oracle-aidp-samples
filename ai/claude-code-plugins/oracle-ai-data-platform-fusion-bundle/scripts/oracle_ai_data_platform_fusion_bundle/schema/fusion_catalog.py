"""Curated catalog of Fusion BICC PVOs the bundle knows how to extract.

Catalog is **annotated**: each entry records whether the PVO is a dedicated
``ExtractPVO`` (optimized for bulk; pdf1 Pro Tip recommends these) or an
``OTBI`` reporting PVO (NOT recommended for bulk). The orchestrator refuses
``OTBI`` entries with a clear warning unless the user explicitly opts in.

PVO names that ship with a ✅ are confirmed verbatim from the published Oracle
material:
- pdf1 Step 3 code block (BICC blog)
- pdf2 p2 default values (ateam blog)
- the official Oracle AIDP sample notebook
- the existing ``aidp-fusion-bicc`` connector skill

PVO names without ✅ require live validation against the customer's pod
during ``aidp-fusion-bundle catalog probe`` — Fusion releases vary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Final


class PvoKind(Enum):
    EXTRACT_PVO = "ExtractPVO"
    """Dedicated bulk-extract PVO. Optimized; pdf1 Pro Tip recommends."""

    OTBI = "OTBI"
    """OTBI reporting PVO. NOT recommended for bulk extraction; orchestrator refuses by default."""


@dataclass(frozen=True)
class PvoEntry:
    """One curated dataset entry."""

    id: str
    """Bundle's logical id (matches ``DatasetSpec.id`` in bundle.yaml)."""

    datastore: str
    """The BICC datastore (PVO) name — passed verbatim to the ``datastore`` Spark option."""

    schema: str
    """BICC offering schema. Common values: ``Financial``, ``ERP``, ``HCM``, ``SCM``."""

    bronze_table: str
    """Three-part name: ``{catalog}.{bronze_schema}.{table}``."""

    description: str
    kind: PvoKind = PvoKind.EXTRACT_PVO
    confirmed: bool = False
    """``True`` if the datastore name is verbatim from a published Oracle source."""

    incremental_capable: bool = True
    """Whether ``fusion.initial.extract-date`` is meaningful for this dataset."""

    extract_columns: list[str] = field(default_factory=list)
    """Optional column projection (pdf1 Pro Tip: prune to what you need; default = all)."""


# ---------------------------------------------------------------------------
# v1 (ERP-Finance) catalog
# ---------------------------------------------------------------------------

# Confirmed PVOs (from blogs / official sample / connector skill):
_SUPPLIER_EXTRACT = PvoEntry(
    id="erp_suppliers",
    # NB: pdf1 wrote `FscmTopModelAM.SupplierExtractPVO` but that's abbreviated;
    # the live catalog requires the full AM-hierarchy. Verified against saasfademo1
    # 2026-04-30 — returned 229 rows / 143 columns. See
    # tests/live/TC1_TC7_results.md and feedback_pdf1_pvo_names_abbreviated.md.
    datastore="FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.erp_suppliers",
    description="Supplier master — live-validated 2026-04-30 against saasfademo1 (229 rows).",
    confirmed=True,
)

_PRC_EXTRACT_PO = PvoEntry(
    id="po_orders",
    # Verified live 2026-04-30 — pdf2's bare `FscmTopModelAM.PrcExtractPO` was abbreviated;
    # full path required.
    datastore="FscmTopModelAM.PrcExtractAM.PoBiccExtractAM.PurchasingDocumentHeaderExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.po_orders",
    description="Purchase order headers — verified live 2026-04-30 against saasfademo1 BICC catalog.",
    confirmed=True,
)

_ITEM_EXTRACT = PvoEntry(
    id="scm_items",
    # Verified live 2026-04-30 — pdf2's bare `ItemExtractPVO` was abbreviated; full AM-hierarchy required.
    datastore="FscmTopModelAM.ScmExtractAM.EgpBiccExtractAM.ItemExtractPVO",
    schema="SCM",
    bronze_table="fusion_catalog.bronze.scm_items",
    description="Item master — verified live 2026-04-30 against saasfademo1 BICC catalog.",
    confirmed=True,
    incremental_capable=True,
)

# Verified live 2026-04-30 — all datastore names confirmed against /biacm/rest/meta/datastores
# on saasfademo1 (Casey.Brown / BIAdmin role). See feedback_pdf1_pvo_names_abbreviated.md.

_GL_JOURNAL_LINES = PvoEntry(
    id="gl_journal_lines",
    datastore="FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.gl_journal_headers",
    description="GL journal headers — verified-live PVO name. (Use JournalLineExtractPVO under FinGlJrnlEntriesAM for line-level granularity.)",
    confirmed=True,
)

_GL_PERIOD_BALANCES = PvoEntry(
    id="gl_period_balances",
    datastore="FscmTopModelAM.FinExtractAM.GlBiccExtractAM.BalanceExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.gl_period_balances",
    description="GL period balances — verified-live PVO name (monthly snapshot).",
    incremental_capable=False,
    confirmed=True,
)

_GL_COA = PvoEntry(
    id="gl_coa",
    datastore="FscmTopModelAM.FinExtractAM.GlBiccExtractAM.CodeCombinationExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.gl_coa",
    description="Chart of accounts (code combinations) — verified-live PVO name. Source for dim_account.",
    incremental_capable=False,
    confirmed=True,
)

_AR_INVOICES = PvoEntry(
    id="ar_invoices",
    # Note: in Fusion AR, "invoices" are stored as AR Transactions.
    datastore="FscmTopModelAM.FinExtractAM.ArBiccExtractAM.TransactionHeaderExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.ar_invoices",
    description="AR invoices (Fusion AR Transaction Headers) — verified-live PVO name.",
    confirmed=True,
)

_AR_RECEIPTS = PvoEntry(
    id="ar_receipts",
    datastore="FscmTopModelAM.FinExtractAM.ArBiccExtractAM.ReceiptHeaderExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.ar_receipts",
    description="AR receipts — verified-live PVO name.",
    confirmed=True,
)

_AR_AGING = PvoEntry(
    id="ar_aging",
    # Fusion BICC has no direct "AR Aging" PVO. The aging gold mart is computed
    # downstream from ArBiccExtractAM.TransactionHeader + ReceiptHeader. The
    # IexBiccExtractAM.AgingBucketsExtractPVO is just the bucket-config, not aged data.
    datastore="FscmTopModelAM.FinExtractAM.ArBiccExtractAM.TransactionHeaderExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.ar_aging",
    description="AR aging — Fusion has no direct AR-Aging PVO; the gold supplier_spend-style mart is computed from ar_invoices + ar_receipts downstream.",
    incremental_capable=False,
    confirmed=True,
)

_AP_INVOICES = PvoEntry(
    id="ap_invoices",
    # Verified live 2026-04-30 (49,985 rows extracted in TC8).
    datastore="FscmTopModelAM.FinExtractAM.ApBiccExtractAM.InvoiceHeaderExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.ap_invoices",
    description="AP invoices — live-validated 2026-04-30 (49,985 rows from saasfademo1).",
    confirmed=True,
)

_AP_PAYMENTS = PvoEntry(
    id="ap_payments",
    datastore="FscmTopModelAM.FinExtractAM.ApBiccExtractAM.PaymentHistoryDistributionExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.ap_payments",
    description="AP payments (Payment History Distribution) — verified-live PVO name.",
    confirmed=True,
)

_AP_AGING = PvoEntry(
    id="ap_aging",
    # Same as AR — no direct aged-transactions PVO. AgingPeriodHeader is just config.
    # AP aging is computed downstream from ap_invoices + ap_payments + AgingPeriodHeaderExtractPVO buckets.
    datastore="FscmTopModelAM.FinExtractAM.ApBiccExtractAM.AgingPeriodHeaderExtractPVO",
    schema="Financial",
    bronze_table="fusion_catalog.bronze.ap_aging_periods",
    description="AP aging period definitions — bucket configs only; aging gold mart computed downstream from ap_invoices + ap_payments.",
    incremental_capable=False,
    confirmed=True,
)

_PO_RECEIPTS = PvoEntry(
    id="po_receipts",
    datastore="FscmTopModelAM.ScmExtractAM.RcvBiccExtractAM.ReceivingReceiptTransactionExtractPVO",
    schema="SCM",
    bronze_table="fusion_catalog.bronze.po_receipts",
    description="PO receipts (Receiving Receipt Transactions) — verified-live PVO name. Note: lives under ScmExtractAM (not PrcExtractAM); schema is SCM.",
    confirmed=True,
)

# v2 deliverable — uses saas-batch REST path, NOT BICC:
_HCM_WORKER_ASSIGNMENTS = PvoEntry(
    id="hcm_worker_assignments",
    datastore="workerAssignmentExtracts",  # confirmed in pdf2 p4 (saas-batch)
    schema="HCM",
    bronze_table="fusion_catalog.bronze.hcm_worker_assignments",
    description="HCM worker assignments — saas-batch REST extractor (pdf2 p4). v2 deliverable.",
    confirmed=True,
)


CATALOG: Final[dict[str, PvoEntry]] = {
    e.id: e
    for e in (
        # ✅ All confirmed against live BICC catalog 2026-04-30 (saasfademo1)
        _SUPPLIER_EXTRACT,
        _PRC_EXTRACT_PO,
        _ITEM_EXTRACT,
        _HCM_WORKER_ASSIGNMENTS,
        _GL_JOURNAL_LINES,
        _GL_PERIOD_BALANCES,
        _GL_COA,
        _AR_INVOICES,
        _AR_RECEIPTS,
        _AR_AGING,
        _AP_INVOICES,
        _AP_PAYMENTS,
        _AP_AGING,
        _PO_RECEIPTS,
    )
}


def get(id: str) -> PvoEntry:
    """Look up a curated PVO by logical id. Raises :class:`KeyError` if unknown."""
    if id not in CATALOG:
        raise KeyError(
            f"unknown dataset id: {id!r}. Known ids: {sorted(CATALOG.keys())}. "
            "Run `aidp-fusion-bundle catalog list` to see the full catalog."
        )
    return CATALOG[id]


def list_confirmed() -> list[PvoEntry]:
    """Return only the ✅-confirmed PVOs (verbatim from published Oracle material)."""
    return [e for e in CATALOG.values() if e.confirmed]


def list_verify_live() -> list[PvoEntry]:
    """Return PVOs whose datastore name needs ``catalog probe`` against a live pod."""
    return [e for e in CATALOG.values() if not e.confirmed]
