"""Neutral metadata for the bronze / silver / gold registries.

This module is the data half of the data/behavior split applied to
``orchestrator/registry.py`` (P1.5ε-fix9). It defines frozen dataclasses
that mirror the runnable specs minus their ``Callable`` builder fields,
plus the three ``KNOWN_DEFERRED_*`` maps that name what's promised but
not yet shipped.

Boundary contract: this module MUST NOT import from ``orchestrator/*``,
``dimensions/*``, ``transforms/*``, or ``extractors/*``. The dispatch
package's §4.3 import-boundary depends on ``schema.plan_resolver``
consuming only this neutral metadata — pulling builder callables in here
would re-introduce engine modules into the dispatcher's import graph and
fail ``tests/unit/dispatch/test_imports.py``.

Adding a new shipped extractor / dim / mart is a two-step edit:
  1. Update the relevant metadata map below (bronze / silver / gold).
  2. Update the matching snapshot test in
     ``tests/unit/schema/test_registry_metadata.py``.

The matching ``orchestrator/registry.py`` map is derived from the
metadata + the engine-side builder callable at module-import time.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeExtractMetadata:
    """Neutral mirror of :class:`orchestrator.registry.BronzeExtractSpec`
    minus the runtime concerns. Two-field DTO: customer-facing dataset id
    + the PVO id resolved through ``schema.fusion_catalog``.
    """

    dataset_id: str
    pvo_id: str


@dataclass(frozen=True)
class SilverDimMetadata:
    """Neutral mirror of :class:`orchestrator.registry.SilverDimSpec`
    minus the ``builder`` callable. ``natural_key`` is the silver-side
    projected column name used by the incremental MERGE ON predicate;
    empty string for parameter-driven dims (today only ``dim_calendar``).
    """

    dataset_id: str
    depends_on_bronze: tuple[str, ...]
    natural_key: "str | tuple[str, ...]" = ""


@dataclass(frozen=True)
class GoldMartMetadata:
    """Neutral mirror of :class:`orchestrator.registry.GoldMartSpec`
    minus the ``builder`` callable. ``incremental_capable=False`` routes
    the mart through seed-shape ``CREATE OR REPLACE TABLE`` regardless
    of orchestrator mode (see GoldMartSpec docstring for the V1-exempt
    marts and their rationale).
    """

    dataset_id: str
    depends_on_bronze: tuple[str, ...]
    depends_on_silver: tuple[str, ...]
    natural_key: "str | tuple[str, ...]" = ""
    incremental_capable: bool = True


# ---------------------------------------------------------------------------
# Shipped metadata — runnable today
# ---------------------------------------------------------------------------
#
# Each map is the projected line-by-line image of the matching runnable
# registry in ``orchestrator/registry.py``. When a new shipped module is
# added there, update the matching map here AND update the snapshot test
# in ``tests/unit/schema/test_registry_metadata.py``.

BRONZE_EXTRACT_METADATA: dict[str, BronzeExtractMetadata] = {
    "erp_suppliers":      BronzeExtractMetadata("erp_suppliers",      "erp_suppliers"),
    "ap_invoices":        BronzeExtractMetadata("ap_invoices",        "ap_invoices"),
    "ap_payments":        BronzeExtractMetadata("ap_payments",        "ap_payments"),
    "ar_invoices":        BronzeExtractMetadata("ar_invoices",        "ar_invoices"),
    "ar_receipts":        BronzeExtractMetadata("ar_receipts",        "ar_receipts"),
    "gl_coa":             BronzeExtractMetadata("gl_coa",             "gl_coa"),
    "gl_journal_lines":   BronzeExtractMetadata("gl_journal_lines",   "gl_journal_lines"),
    "gl_period_balances": BronzeExtractMetadata("gl_period_balances", "gl_period_balances"),
    "po_orders":          BronzeExtractMetadata("po_orders",          "po_orders"),
    "po_receipts":        BronzeExtractMetadata("po_receipts",        "po_receipts"),
    "scm_items":          BronzeExtractMetadata("scm_items",          "scm_items"),
}

SILVER_DIM_METADATA: dict[str, SilverDimMetadata] = {
    "dim_supplier": SilverDimMetadata(
        dataset_id="dim_supplier",
        depends_on_bronze=("erp_suppliers",),
        natural_key="supplier_number",
    ),
    "dim_account": SilverDimMetadata(
        dataset_id="dim_account",
        depends_on_bronze=("gl_coa",),
        natural_key="account_id",
    ),
    "dim_calendar": SilverDimMetadata(
        dataset_id="dim_calendar",
        depends_on_bronze=(),
        natural_key="",
    ),
}

GOLD_MART_METADATA: dict[str, GoldMartMetadata] = {
    "supplier_spend": GoldMartMetadata(
        dataset_id="supplier_spend",
        depends_on_bronze=("ap_invoices",),
        depends_on_silver=("dim_supplier",),
        natural_key=(
            "vendor_id", "currency_code", "supplier_number",
            "supplier_name", "business_relationship", "approval_status",
        ),
        incremental_capable=False,
    ),
    "gl_balance": GoldMartMetadata(
        dataset_id="gl_balance",
        depends_on_bronze=("gl_period_balances",),
        depends_on_silver=("dim_account",),
        natural_key=(
            "ledger_id", "account_id", "period_year", "period_num",
            "currency_code", "actual_flag", "translated_flag",
        ),
        incremental_capable=True,
    ),
    "ap_aging": GoldMartMetadata(
        dataset_id="ap_aging",
        depends_on_bronze=("ap_invoices",),
        depends_on_silver=("dim_supplier",),
        natural_key=(
            "vendor_id", "currency_code", "supplier_number",
            "supplier_name", "business_relationship", "aging_bucket",
        ),
        incremental_capable=False,
    ),
}


# ---------------------------------------------------------------------------
# Deferred registries — names that resolve to a DeferredSpec at runtime
# ---------------------------------------------------------------------------
#
# Reason strings are operator-facing — they surface in the rendered run
# summary and the BACKLOG cross-reference. Snapshot-tested in
# ``tests/unit/schema/test_registry_metadata.py`` so a typo fails loudly.

KNOWN_DEFERRED_DATASETS: dict[str, str] = {
    "hcm_worker_assignments": "BACKLOG P2.11 — saas-batch REST extractor (kind=SAAS_BATCH), not BICC",
    "ap_aging_periods": (
        "BACKLOG P1.10b — bronze for AgingPeriodHeader bucket configs; "
        "gold ap_aging mart computed downstream from ap_invoices + ap_payments + bucket configs"
    ),
}

KNOWN_DEFERRED_DIMS: dict[str, str] = {
    "dim_org":  "P1.7 — HCM org dim, blocked on customer HCM pod (P3.8)",
    "dim_item": "P1.6 — inventory item dim, no shipped consumer yet",
}

KNOWN_DEFERRED_MARTS: dict[str, str] = {
    "ar_aging":   "P1.10 — accounts-receivable aging gold mart, not yet shipped",
    "po_backlog": "P1.11 — open POs by supplier × due date, not yet shipped",
}


__all__ = [
    "BronzeExtractMetadata",
    "SilverDimMetadata",
    "GoldMartMetadata",
    "BRONZE_EXTRACT_METADATA",
    "SILVER_DIM_METADATA",
    "GOLD_MART_METADATA",
    "KNOWN_DEFERRED_DATASETS",
    "KNOWN_DEFERRED_DIMS",
    "KNOWN_DEFERRED_MARTS",
]
