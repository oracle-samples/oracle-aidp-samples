"""Curated catalog of Fusion BICC PVOs the bundle knows how to extract.

Catalog is **annotated**: each entry records whether the PVO is a dedicated
``ExtractPVO`` (optimized for bulk; pdf1 Pro Tip recommends these) or an
``OTBI`` reporting PVO (NOT recommended for bulk). The orchestrator refuses
``OTBI`` entries with a clear warning unless the user explicitly opts in.

PVO names marked ``confirmed=True`` are confirmed verbatim from published
Oracle material or live catalog probes:
- pdf1 Step 3 code block (BICC blog)
- pdf2 p2 default values (ateam blog)
- the official Oracle AIDP sample notebook
- the existing ``aidp-fusion-bicc`` connector skill

PVO names without ✅ require live validation against the customer's pod
during ``aidp-fusion-autopilot catalog probe`` — Fusion releases vary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Final


class PvoKind(Enum):
    EXTRACT_PVO = "ExtractPVO"
    """Dedicated bulk-extract PVO. Optimized; pdf1 Pro Tip recommends. Eligible for a bronze node in the content pack."""

    OTBI = "OTBI"
    """OTBI reporting PVO. NOT recommended for bulk extraction; orchestrator refuses by default."""

    SAAS_BATCH = "SaasBatch"
    """saas-batch REST extractor (NOT BICC). No concrete extractor ships yet, so packs cannot wire a bronze node against this kind today."""


# Single-segment SQL-identifier regex — matches paths.py's _SQL_IDENTIFIER_RE.
# bronze_table_name must be a bare identifier so TablePaths.bronze() can
# compose the 3-part name via {catalog}.{bronze_schema}.{bronze_table_name}.
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class PvoEntry:
    """One curated dataset entry."""

    id: str
    """Bundle's logical id (matches ``DatasetSpec.id`` in bundle.yaml)."""

    datastore: str
    """The BICC datastore (PVO) name — passed verbatim to the ``datastore`` Spark option."""

    schema: str
    """BICC offering schema. Common values: ``Financial``, ``ERP``, ``HCM``, ``SCM``."""

    bronze_table_name: str
    """Single-segment table name (the ``{table}`` part only).

    The catalog and bronze schema come from the tenant's ``bundle.yaml`` via
    ``TablePaths.bronze(self.bronze_table_name)`` — never hardcode the prefix
    here. Decouples the catalog-declared table name from the tenant's
    catalog/schema configuration. Validated as a single SQL identifier
    (``^[A-Za-z_][A-Za-z0-9_]*$``) at construction; dots/hyphens are rejected.

    May differ from ``id``: e.g. ``gl_journal_lines`` (id) →
    ``gl_journal_headers`` (table name, matching the PVO's actual grain).
    """

    description: str
    kind: PvoKind = PvoKind.EXTRACT_PVO
    confirmed: bool = False
    """``True`` if the datastore name is verbatim from a published Oracle source."""

    incremental_capable: bool = True
    """Whether ``fusion.initial.extract-date`` is meaningful for this dataset."""

    natural_key: "str | tuple[str, ...]" = ""
    """Natural-key column(s) used by bronze MERGE for upsert semantics.

    Single-column keys are stored as a bare string (e.g. ``"SEGMENT1"``);
    composite keys as a tuple of column names. The MERGE ``ON`` predicate
    joins target/src on each member (composite → ``AND``-conjunction of
    per-column equalities).

    Default ``""`` keeps the dataclass backwards-compatible for code paths
    that don't perform MERGE (preflight discovery, dry-run plan rendering).
    The orchestrator's bronze MERGE path raises a clear error rather than
    silently dispatching with an empty key.

    Keys are verified against shipped builders that read the bronze table or
    against live BICC catalog probes. Inferred entries must be confirmed via
    ``catalog probe`` before bronze MERGE is enabled for that PVO.
    """

    extract_columns: list[str] = field(default_factory=list)
    """Optional column projection (pdf1 Pro Tip: prune to what you need; default = all)."""

    def __post_init__(self) -> None:
        if not isinstance(self.bronze_table_name, str):
            raise TypeError(
                f"bronze_table_name must be a string; got {type(self.bronze_table_name).__name__} "
                f"({self.bronze_table_name!r})"
            )
        if not _SQL_IDENTIFIER_RE.match(self.bronze_table_name):
            raise ValueError(
                f"bronze_table_name={self.bronze_table_name!r} is not a valid single SQL identifier — "
                f"must match ^[A-Za-z_][A-Za-z0-9_]*$. The catalog and bronze schema come from "
                f"bundle.yaml.aidp.*; only the bare table name lives here."
            )


# ---------------------------------------------------------------------------
# ERP-Finance catalog
# ---------------------------------------------------------------------------

# Confirmed PVOs (from blogs / official sample / connector skill):
_SUPPLIER_EXTRACT = PvoEntry(
    id="erp_suppliers",
    # Public examples sometimes abbreviate the datastore; BICC requires the
    # full AM hierarchy for this extract.
    datastore="FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO",
    schema="Financial",
    bronze_table_name="erp_suppliers",
    description="Supplier master — validated BICC ExtractPVO.",
    confirmed=True,
    # Natural key used by dim_supplier as supplier_number.
    natural_key="SEGMENT1",
)

_PRC_EXTRACT_PO = PvoEntry(
    id="po_orders",
    # Public examples may abbreviate this name; the full AM hierarchy is
    # required by BICC.
    datastore="FscmTopModelAM.PrcExtractAM.PoBiccExtractAM.PurchasingDocumentHeaderExtractPVO",
    schema="Financial",
    bronze_table_name="po_orders",
    description="Purchase order headers — validated BICC ExtractPVO.",
    confirmed=True,
    # Inferred from PurchasingDocumentHeaderExtractPVO naming convention;
    # verify via `catalog probe` before enabling bronze MERGE.
    natural_key="PoHeadersAllPoHeaderId",
)

_ITEM_EXTRACT = PvoEntry(
    id="scm_items",
    # Public examples may abbreviate this name; the full AM hierarchy is
    # required by BICC.
    datastore="FscmTopModelAM.ScmExtractAM.EgpBiccExtractAM.ItemExtractPVO",
    # The PVO lives under ScmExtractAM in the AM hierarchy, but some tenants
    # publish it under the Financial offering. Use schemaOverrides when a
    # tenant exposes it under a different offering.
    schema="Financial",
    bronze_table_name="scm_items",
    description="Item master — validated BICC ExtractPVO.",
    confirmed=True,
    incremental_capable=True,
    # Inferred composite key at item x inventory-org grain; verify via
    # `catalog probe` before enabling bronze MERGE.
    natural_key=("EgpSystemItemsBInventoryItemId", "EgpSystemItemsBOrganizationId"),
)

# Datastore names are curated against Oracle samples and live BICC catalog
# probes. Abbreviated names from public examples are not always accepted by
# BICC; prefer the full AM hierarchy stored here.

_GL_JOURNAL_LINES = PvoEntry(
    id="gl_journal_lines",
    datastore="FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO",
    schema="Financial",
    bronze_table_name="gl_journal_headers",
    description="GL journal headers — verified-live PVO name. (Use JournalLineExtractPVO under FinGlJrnlEntriesAM for line-level granularity.)",
    confirmed=True,
    # Inferred header-level key; verify via `catalog probe`.
    natural_key="GlJeHeadersJeHeaderId",
)

_GL_PERIOD_BALANCES = PvoEntry(
    id="gl_period_balances",
    datastore="FscmTopModelAM.FinExtractAM.GlBiccExtractAM.BalanceExtractPVO",
    schema="Financial",
    bronze_table_name="gl_period_balances",
    description="GL period balances — verified-live PVO name (monthly snapshot).",
    incremental_capable=False,
    confirmed=True,
    # Seven-column composite key. Some tenants contain NULL translated flags,
    # so bronze MERGE predicates for this PVO must use NULL-safe comparison.
    natural_key=(
        "BalanceLedgerId",
        "BalanceCodeCombinationId",
        "BalancePeriodYear",
        "BalancePeriodNum",
        "BalanceCurrencyCode",
        "BalanceActualFlag",
        "BalanceTranslatedFlag",
    ),
)

_GL_COA = PvoEntry(
    id="gl_coa",
    datastore="FscmTopModelAM.FinExtractAM.GlBiccExtractAM.CodeCombinationExtractPVO",
    schema="Financial",
    bronze_table_name="gl_coa",
    description="Chart of accounts (code combinations) — verified-live PVO name. Source for dim_account.",
    incremental_capable=True,
    confirmed=True,
    # Natural key used by dim_account as account_id.
    natural_key="CodeCombinationCodeCombinationId",
)

_AR_INVOICES = PvoEntry(
    id="ar_invoices",
    # Note: in Fusion AR, "invoices" are stored as AR Transactions.
    datastore="FscmTopModelAM.FinExtractAM.ArBiccExtractAM.TransactionHeaderExtractPVO",
    schema="Financial",
    bronze_table_name="ar_invoices",
    description="AR invoices (Fusion AR Transaction Headers) — verified-live PVO name.",
    confirmed=True,
    # Inferred key; verify via `catalog probe`.
    natural_key="RaCustTrxAllCustomerTrxId",
)

_AR_RECEIPTS = PvoEntry(
    id="ar_receipts",
    datastore="FscmTopModelAM.FinExtractAM.ArBiccExtractAM.ReceiptHeaderExtractPVO",
    schema="Financial",
    bronze_table_name="ar_receipts",
    description="AR receipts — verified-live PVO name.",
    confirmed=True,
    # Inferred key; verify via `catalog probe`.
    natural_key="ArCashReceiptsAllCashReceiptId",
)

_AP_INVOICES = PvoEntry(
    id="ap_invoices",
    # Validated against live BICC catalog probes.
    datastore="FscmTopModelAM.FinExtractAM.ApBiccExtractAM.InvoiceHeaderExtractPVO",
    schema="Financial",
    bronze_table_name="ap_invoices",
    description="AP invoices — validated BICC ExtractPVO.",
    confirmed=True,
    # Inferred from ApInvoices* prefix convention and shipped mart column
    # reads; verify via `catalog probe`.
    natural_key="ApInvoicesInvoiceId",
)

_AP_PAYMENTS = PvoEntry(
    id="ap_payments",
    datastore="FscmTopModelAM.FinExtractAM.ApBiccExtractAM.PaymentHistoryDistributionExtractPVO",
    schema="Financial",
    bronze_table_name="ap_payments",
    description="AP payments (Payment History Distribution) — verified-live PVO name.",
    confirmed=True,
    # Inferred composite at payment x distribution grain; verify via
    # `catalog probe` before enabling bronze MERGE for this PVO.
    natural_key=(
        "ApPayHistDistInvoicePaymentId",
        "ApPayHistDistPaymentHistDistId",
    ),
)

_AP_AGING_PERIODS = PvoEntry(
    id="ap_aging_periods",
    # Renamed from "ap_aging" 2026-05-17 to disambiguate from the gold ap_aging
    # mart (cross-layer name collision). The PVO is bucket-period configs
    # (AgingPeriodHeaderExtractPVO), not aged transactions; the new name
    # accurately reflects the content. AP aging is computed downstream from
    # ap_invoices + ap_payments + these AgingPeriodHeader buckets.
    datastore="FscmTopModelAM.FinExtractAM.ApBiccExtractAM.AgingPeriodHeaderExtractPVO",
    schema="Financial",
    bronze_table_name="ap_aging_periods",
    description="AP aging period definitions — bucket configs only; aging gold mart computed downstream from ap_invoices + ap_payments.",
    incremental_capable=True,
    confirmed=True,
    # Inferred key; verify via `catalog probe`.
    natural_key="AgingPeriodHeaderAgingPeriodId",
)

_PO_RECEIPTS = PvoEntry(
    id="po_receipts",
    datastore="FscmTopModelAM.ScmExtractAM.RcvBiccExtractAM.ReceivingReceiptTransactionExtractPVO",
    # The PVO lives under ScmExtractAM in the AM hierarchy, but some tenants
    # publish it under the Financial offering. Use schemaOverrides when a
    # tenant exposes it under SCM.
    schema="Financial",
    bronze_table_name="po_receipts",
    description="PO receipts (Receiving Receipt Transactions) — verified-live PVO name. Lives under ScmExtractAM in the AM hierarchy; BICC offering schema is tenant-dependent (see schema field comment).",
    confirmed=True,
    # Inferred key; verify via `catalog probe`.
    natural_key="RcvTransactionsTransactionId",
)

# Uses the saas-batch REST path, not BICC. The kind=SAAS_BATCH tag marks this
# entry as not eligible for a content-pack bronze node until a concrete
# saas-batch extractor ships.
_HCM_WORKER_ASSIGNMENTS = PvoEntry(
    id="hcm_worker_assignments",
    datastore="workerAssignmentExtracts",  # confirmed in pdf2 p4 (saas-batch)
    schema="HCM",
    bronze_table_name="hcm_worker_assignments",
    description="HCM worker assignments — saas-batch REST extractor.",
    kind=PvoKind.SAAS_BATCH,
    confirmed=True,
)


CATALOG: Final[dict[str, PvoEntry]] = {
    e.id: e
    for e in (
        # Curated entries with validated datastore names.
        _SUPPLIER_EXTRACT,
        _PRC_EXTRACT_PO,
        _ITEM_EXTRACT,
        _HCM_WORKER_ASSIGNMENTS,
        _GL_JOURNAL_LINES,
        _GL_PERIOD_BALANCES,
        _GL_COA,
        _AR_INVOICES,
        _AR_RECEIPTS,
        _AP_INVOICES,
        _AP_PAYMENTS,
        _AP_AGING_PERIODS,
        _PO_RECEIPTS,
    )
}


def get(id: str) -> PvoEntry:
    """Look up a curated PVO by logical id. Raises :class:`KeyError` if unknown."""
    if id not in CATALOG:
        raise KeyError(
            f"unknown dataset id: {id!r}. Known ids: {sorted(CATALOG.keys())}. "
            "Run `aidp-fusion-autopilot catalog list` to see the full catalog."
        )
    return CATALOG[id]


def list_confirmed() -> list[PvoEntry]:
    """Return only the ✅-confirmed PVOs (verbatim from published Oracle material)."""
    return [e for e in CATALOG.values() if e.confirmed]


def list_verify_live() -> list[PvoEntry]:
    """Return PVOs whose datastore name needs ``catalog probe`` against a live pod."""
    return [e for e in CATALOG.values() if not e.confirmed]
