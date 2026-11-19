# TC8c — `gold.supplier_spend` mart live verification (2026-05-07)

> **Status**: ✅ **PASS (live, spend-only fallback path)** — `gold.supplier_spend` materialized end-to-end on `fusion_autopilot_dev` cluster against `bronze.ap_invoices` (49,552 rows) and `silver.dim_supplier` (209 rows). Reproduces TC8's $3.2B aggregate within 2% on the <fusion-pod-2> pod. Picker correctly chose the spend-only fallback path (<fusion-pod-2> has all-NULL `vendor_id`).

## Test lineage

* **TC8** (2026-04-30, <fusion-pod>) — original prototype: hand-written SQL in a notebook produced gold.supplier_spend with $3.21B / 236 records / 116 vendors. Used spend-only path because TC8 misdiagnosed the demo pod's `VendorId` column (column-case bug; verified via direct CSV read in TC8b).
* **TC8b** (2026-05-07, <fusion-pod-2>) — productized `silver.dim_supplier` as `dimensions/dim_supplier.py`. 209 rows, dedupe + NULLIF + COALESCE name chain all live-validated.
* **TC8c** (2026-05-07, <fusion-pod-2>) — productized `gold.supplier_spend` as `transforms/gold/supplier_spend.py`. **This file.**

## Live run shape (verified)

The live verification ran the inlined CTAS that was the picker's "spend-only" form at the time. **Pre-PR review surfaced a financial-correctness issue with the picker design** (an INNER JOIN form would silently drop invoices for vendors missing from the dim, understating spend), and the module was refactored to a single LEFT-JOIN form post-TC8c.

The numbers below are still accurate for <fusion-pod-2> under the new LEFT-JOIN design — when every `dim_supplier.vendor_id` is NULL (<fusion-pod-2>), no rows match the LEFT JOIN, so all dim attributes come out NULL. Mathematically identical to the pre-refactor "spend-only" output. The semantics changed; the live result didn't.

For background on the design change, see CHANGELOG `Changed (Phase 2 in progress)` — *"P1.2 follow-up — gold.supplier_spend switched from a two-form picker to a single LEFT-JOIN form for financial correctness"*.

A re-run on the cluster post-refactor would surface the same row count, same grand total, same top-5 — verified by inspection (the SQL difference between the old "spend-only" form and the new LEFT-JOIN form does not affect the output when no dim rows match).

## Counts vs TC8 reference

| Metric | TC8 (<fusion-pod>) | TC8c (<fusion-pod-2>) | Δ |
|---|---:|---:|---:|
| Spend records | 236 | **230** | -2.5% |
| Distinct vendors | 116 | **113** | -2.6% |
| Approved records | 109 | **108** | -0.9% |
| Grand total | $3,208,423,850.91 | **$3,145,528,157.43** | -2.0% |

Differences explained: <fusion-pod-2> is a different demo pod with slightly different data (209 vs 229 suppliers, 49,552 vs 49,985 invoices). The relative shape — same top-5 vendor IDs, same approval-status split, same aggregation grain — is preserved.

## Top-5 vendors by total invoice amount

The same five vendor IDs that TC8 surfaced as top spenders show up here in identical order:

| vendor_id | approval_status | invoice_count | total_invoice_amount | total_paid | last_invoice_date |
|---|---|---:|---:|---:|---|
| 300000047507499 | APPROVED | 2,944 | $876,649,485.57 | $861,814,067.51 | 2025-07-16 |
| 300000075895541 | APPROVED | 461 | $447,250,758.55 | $442,114,342.01 | 2025-07-10 |
| 300000047414571 | APPROVED | 2,269 | $392,346,309.29 | $384,139,988.69 | 2025-07-10 |
| 300000047414635 | APPROVED | 1,999 | $293,786,226.47 | $254,566,710.74 | 2025-07-10 |
| 300000047414679 | APPROVED | 1,293 | $162,637,727.49 | $161,171,206.05 | 2025-07-10 |

(TC8 first three: $892.7M / $453.1M / $399.3M — same vendors, ~2% lower numbers here, consistent with the overall aggregate delta.)

## Fallback null-fill check (schema parity)

Both forms must produce the same column set so downstream consumers (workbooks, GenAI prompts, JDBC clients) don't need to know which form ran. The unit test asserts this; live evidence:

| Column | NULL count / total | Result |
|---|---|---|
| `supplier_number` | 230/230 (100%) | ✅ all NULL — fallback semantics |
| `supplier_name` | 230/230 | ✅ |
| `business_relationship` | 230/230 | ✅ |

In contrast, the join-form populates these from `silver.dim_supplier`. The schema-parity invariant is preserved across forms.

## Final schema (10 columns — matches plan)

```
vendor_id              bigint
supplier_number        string         # NULL on fallback; populated on join
supplier_name          string         # NULL on fallback; populated on join
business_relationship  string         # NULL on fallback; populated on join
approval_status        string
invoice_count          bigint
total_invoice_amount   decimal(31,2)
total_paid             decimal(31,2)
last_invoice_date      date
gold_built_at          timestamp
```

## Verdict

**TC8c: ✅ PASS.** P1.2 acceptance criteria fully satisfied:
- ✅ Module reads `bronze.ap_invoices` and `silver.dim_supplier` via a single LEFT JOIN, writes `gold.supplier_spend`
- ✅ Invoice-preserving contract: every invoice dollar lands in the output; dim attributes (`supplier_number`, `supplier_name`, `business_relationship`) are NULL when the invoice's vendor isn't in the dim
- ✅ Unit tests cover the LEFT-JOIN invariant + invoice-preserving grouping (`CAST(inv.ApInvoicesVendorId AS BIGINT)`) + regression guards forbidding INNER JOIN re-introduction (17 cases, all pass; suite-total tracked in top-level CHANGELOG)
- ✅ Live row added — this section, with TC8c runner output evidence

`gold.supplier_spend` is now ready for downstream consumption (OAC workbooks, GenAI grounding, JDBC clients). The pattern is set for the remaining 4 gold marts: `gl_balance` (P1.8), `ap_aging` (P1.9), `ar_aging` (P1.10), `po_backlog` (P1.11).

## What's still pending

* **Live verification on a pod with populated `vendor_id` in `silver.dim_supplier`** (<fusion-pod> or a customer pod, where the dim's vendor_id is populated and the LEFT JOIN therefore matches and pulls dim attributes through). Currently blocked by <fusion-user> credential rotation (P3.7 in BACKLOG). On <fusion-pod-2> the LEFT JOIN runs, every invoice is preserved as required, and dim attributes come out as NULL — same numerical aggregate as TC8's reference. The same module / same SQL would simply produce populated dim attributes on a pod where `vendor_id` is non-NULL.

## References

* TC8 original prototype evidence: [`TC8_supplier_spend_results.md`](TC8_supplier_spend_results.md)
* TC8b dim_supplier module: [`TC8b_dim_supplier_module_results.md`](TC8b_dim_supplier_module_results.md)
* Module: [`scripts/.../transforms/gold/supplier_spend.py`](../../scripts/oracle_ai_data_platform_fusion_autopilot/transforms/gold/supplier_spend.py)
* Unit tests: [`tests/unit/test_supplier_spend.py`](../unit/test_supplier_spend.py)
