# TC24 — `gold.ap_aging` live verification on `fusion_autopilot_dev`

> Date: 2026-05-10
> Cluster: `fusion_autopilot_dev` (workspace `aidp` demo-pod demo pod)
> Probe runner: `TC24_ap_aging_probe_runner.py` (local, gitignored)
> Live runner: `TC24_ap_aging_live_runner.py` (local, gitignored)
> Bronze source: `fusion_catalog.bronze.ap_invoices` (49,552 rows; BICC `InvoiceHeaderExtractPVO`)
> Mart shipped: `fusion_catalog.gold.ap_aging` (132 rows, 87 vendors × 12 currencies × 4 buckets)

## Pre-flight probe outcomes (paste these into the module docstring's tenant-shape note)

| Gate | Outcome on `fusion_autopilot_dev` | Decision |
|---|---|---|
| Currency presence (HARD) | `ApInvoicesInvoiceCurrencyCode` ✅ | ship |
| Real-due-date population | `terms_frac = 1.000000`, `coalesced_frac = 1.000000` (`DueDate` column absent on pod) | `due_date_mode='real'`, target `gold.ap_aging` |
| NULL `InvoiceDate` fraction | `0/49,552 = 0.00%` | `null_invoice_date_policy='drop'` |
| Credit-memo presence | 20 negative-open rows (`min_open_amount = -85,407.00`, `sum_credit_open = -126,269.47`) | `<> 0` filter (invariant — does NOT depend on this finding) |
| Cancelled column | `ApInvoicesCancelledDate` (NULL = not cancelled); 617/49,552 = 1.2% cancelled | `cancelled_col='ApInvoicesCancelledDate'`, `cancelled_kind='date'` |
| Invoice-type column | `ApInvoicesInvoiceTypeLookupCode` (STANDARD 8,998 + AWT 847 + DEBIT 16 + PREPAYMENT 5 + 3 misc) | informational only |

Detected via `ap_aging.detect_ap_aging_params(spark)` — same result.

## Build (stage 1)

```
✅ CREATE OR REPLACE TABLE fusion_catalog.gold.ap_aging — 21.4s
```

Build is idempotent (`CREATE OR REPLACE`); reruns produce the same shape.

## Grain sanity (stage 2)

| metric | value | check |
|---|---|---|
| total rows | **132** | — |
| distinct (vendor, currency, bucket) keys | 132 | ✅ `total_rows == distinct_grain_keys` → grain unique |
| distinct vendors | 87 | — |
| distinct currencies | 12 | matches probe |
| distinct buckets | 4 | ≤5 (no `current` bucket on this pod; see below) |
| `SUM(open_invoice_count)` | 9,873 | exactly matches probe |

### Why no `current` bucket on this pod

`bronze.ap_invoices` carries data from 2012 onward (oldest sample row: `2012-01-21`). With `as_of_date = CURRENT_DATE() = 2026-05-10`, even the most recent open invoice is comfortably >30 days past due, so every open row falls in `1-30` / `31-60` / `61-90` / `91+`. Only USD has rows in `1-30` (1 vendor, 2 invoices), `31-60` (1 vendor, 2 invoices), and `61-90` (1 vendor, 2 invoices). Everything else is `91+`. This is a demo-pod data-age artifact; production pods with current AP activity will populate `current` heavily.

## Per-currency × per-bucket totals (stage 3)

**Do NOT publish a cross-currency sum.** Cross-currency aggregation requires FX conversion, which is a consumer concern (same lesson as `TC23_gl_balance_results.md:77`).

| currency | bucket | vendor_buckets | invoices | total_open |
|---|---|---:|---:|---:|
| AED | 91+ | 1 | 1 | 12,600.00 |
| AUD | 91+ | 3 | 14 | 1,443,825.80 |
| CHF | 91+ | 8 | 15 | 36,213.49 |
| CNY | 91+ | 5 | 21 | 72,050,708.82 |
| EUR | 91+ | 22 | 2,208 | 51,529,397.50 |
| GBP | 91+ | 15 | 2,212 | 47,964,686.65 |
| INR | 91+ | 5 | 18 | 283,926.26 |
| JPY | 91+ | 9 | 51 | 366,549.00 |
| MXN | 91+ | 1 | 1 | 789.00 |
| PLN | 91+ | 2 | 5 | 1,006.00 |
| TRY | 91+ | 1 | 1 | 500.00 |
| USD | 1-30  | 1 |    2 | 1,631.55 |
| USD | 31-60 | 1 |    2 | 1,752.00 |
| USD | 61-90 | 1 |    2 | 1,576.80 |
| USD | 91+   | 57 | 5,320 | 163,253,953.37 |

## Per-currency reconciliation vs bronze (stage 4) — **financial-correctness check**

`SUM(gold.ap_aging.open_amount) GROUP BY currency_code` must equal
`SUM(bronze.ap_invoices.invoice_amount - COALESCE(amount_paid, 0)) GROUP BY currency_code`
filtered to the same open-population predicate the mart uses.

| ccy | gold_total | bronze_total | delta |
|---|---:|---:|---:|
| USD | 163,258,913.72 | 163,258,913.72 | 0.00 |
| CNY | 72,050,708.82 | 72,050,708.82 | 0.00 |
| EUR | 51,529,397.50 | 51,529,397.50 | 0.00 |
| GBP | 47,964,686.65 | 47,964,686.65 | 0.00 |
| AUD | 1,443,825.80 | 1,443,825.80 | 0.00 |
| JPY | 366,549.00 | 366,549.00 | 0.00 |
| INR | 283,926.26 | 283,926.26 | 0.00 |
| CHF | 36,213.49 | 36,213.49 | 0.00 |
| AED | 12,600.00 | 12,600.00 | 0.00 |
| PLN | 1,006.00 | 1,006.00 | 0.00 |
| MXN | 789.00 | 789.00 | 0.00 |
| TRY | 500.00 | 500.00 | 0.00 |

**`max |delta| = 0.00`** across all 12 currencies. The CTE filters and aggregation produce results that round-trip exactly with bronze.

## `due_date_source` distribution (stage 5)

| source | rows | share |
|---|---:|---:|
| `terms_date` | 9,873 | **100.00%** |
| `due_date` | 0 | 0.00% (column absent on this pod) |
| `net30_fallback` | 0 | 0.00% |

No NET-30 fallback is invoked. The mart-name gate's 80% threshold (PLAN §3.2) is satisfied with massive margin (100%); shipping as `gold.ap_aging` rather than `gold.ap_outstanding_by_invoice_age` is correct.

## Credit-open sign-check (stage 6) — **reviewer Blocker #1 validation**

| currency | credit_invoices | credit_open_total |
|---|---:|---:|
| USD | 13 | -122,700.03 |
| CNY | 2 | -1,917.63 |
| EUR | 1 | -831.81 |
| JPY | 3 | -541.00 |
| PLN | 1 | -279.00 |
| **total** | **20** | **-126,269.47** |

Matches the probe (`negative_open_count = 20`, `sum_credit_open = -126,269.47`) exactly.

**Validates reviewer Blocker #1**: credit memos / overpayment offsets exist across **5 different currencies** on this very pod. Had the plan's earlier "downgrade to `> 0` if pod has no credits" clause been kept, the demo-pod measurement would have looked like "no credits, safe to use `> 0`" if it had happened to be just one currency or one row. Holding the `<> 0` filter invariant across tenants is what protects against losing $126,269 of valid negative open balances across 20 invoices in 5 currencies. This is the strongest empirical case for the invariant being a product rule and not a per-pod heuristic.

## Top vendor in `91+` bucket per currency (stage 7) — face-value sanity

| ccy | vendor_id | invoices | open_amount | max_days_past_due |
|---|---:|---:|---:|---:|
| CNY | 300,000,075,469,611 | 12 | 71,501,763.45 | 4,268 |
| USD | 300,000,047,414,635 | 616 | 43,675,696.82 | 3,686 |
| EUR | 300,000,051,066,172 | 1,233 | 35,041,980.95 | 1,865 |
| GBP | 300,000,049,521,222 | 1,038 | 30,101,821.03 | 4,430 |
| AUD | 300,000,047,414,503 | 6 | 822,168.60 | 1,540 |
| JPY | 300,000,047,414,635 | 9 | 228,204.00 | 4,048 |
| INR | 300,000,047,507,546 | 2 | 202,500.00 | 2,535 |
| CHF | 300,000,180,153,159 | 2 | 26,960.00 | 2,313 |
| AED | 300,000,278,628,804 | 1 | 12,600.00 | 824 |
| PLN | 300,000,047,414,503 | 2 | 996.00 | 2,781 |

USD vendor `300,000,047,414,635` is the same vendor that dominated TC8 (`supplier_spend`) live — consistent across marts. Days-past-due in the thousands is expected on demo-pod because the bronze data is historical (oldest invoice 2012).

## `silver.dim_supplier` coverage (stage 8) — demo-pod finding, not a bug

| metric | value |
|---|---:|
| total rows in gold.ap_aging | 132 |
| rows with NULL supplier_number (LEFT-JOIN miss) | 132 |
| missing-dim fraction | **100.00%** |

Every AP invoice's `vendor_id` is missing from `silver.dim_supplier` on this pod. This is the same demo-pod quirk previously observed in TC8c (`supplier_spend`): the demo pod's `bronze.erp_suppliers` (SupplierExtractPVO, ~229 rows) carries supplier-master records whose IDs don't intersect with the `ApInvoicesVendorId` values used on AP invoices (different Fusion ID concepts on this pod's seed data — likely SUPPLIER_ID vs PARTY_ID/VENDOR_ID).

**This is upstream data, not a mart-correctness issue.** The LEFT JOIN preserves all 132 fact rows with NULL supplier attributes, exactly as the financial-correctness invariant requires. Production pods with proper supplier-master linkage will see normal coverage. Consumers slicing by `supplier_name` will see "(NULL)" on this pod; slicing by `vendor_id` works.

## Schema findings — the "plugin-portable" knobs

| knob | value used here | meaning |
|---|---|---|
| `due_date_mode` | `"real"` | `gold.ap_aging` shape |
| `terms_date_col` | `"ApInvoicesTermsDate"` | present (100% populated on open pop) |
| `due_date_col` | `None` | **column absent on this pod** — plugin must support absence |
| `cancelled_col` | `"ApInvoicesCancelledDate"` | non-NULL = cancelled |
| `cancelled_kind` | `"date"` | (vs `"flag"` variant on other tenants) |
| `currency_col` | `"ApInvoicesInvoiceCurrencyCode"` | universal so far |
| `null_invoice_date_policy` | `"drop"` | 0% NULL `InvoiceDate` on this pod |
| `as_of_date_expr` | `"CURRENT_DATE()"` (default) | runtime, captured in `as_of_date` audit column |
| `auto_detect` | `True` (default) | `detect_ap_aging_params(spark)` populated the three knobs above automatically |
| `due_date_mode` (passed) | `"auto"` (default) | `build()` measured coverage (1.000) and routed to `"real"` automatically |
| `real_mode_gate_threshold` | `0.80` (default) | controls the auto routing; deployments can raise for stricter gates |

`ap_aging.build(spark)` with all defaults (`auto_detect=True`, `due_date_mode="auto"`) reproduces this run. The build path computes coalesced coverage on the open-invoice population and decides real vs proxy mode via the 80% gate before assembling the SQL — see `decide_due_date_mode()`. A tenant where TermsDate exists but is sparsely populated will route to proxy mode (`gold.ap_outstanding_by_invoice_age`) automatically instead of silently shipping fake due-date aging under the canonical name.

## What this validates

1. **Currency-in-grain**: 12 currencies, per-currency reconciliation passes with `delta = 0.00` everywhere. Cross-currency sum is suppressed by the consumer query, not the mart.
2. **`<> 0` filter invariant** (reviewer Blocker #1): real credit memos exist across 5 currencies; the `<> 0` rule preserves $126,269 of valid negative open balances that `> 0` would have dropped.
3. **`due_date_mode` is a public parameter** (reviewer Blocker #2): module ships both modes; live exercise picks `real` based on probe; unit tests cover both.
4. **No silent NET-30 under canonical name** (reviewer Blocker #3): 100% `terms_date`, 0 NET-30 fallback on this pod; the gate would route to proxy mode (different table name) if coverage fell below 80%.
5. **NULL `invoice_date` policy** (should-fix): WHERE filters NULL invoice_date; 0 dropped on this pod since `InvoiceDate` is 100% populated.
6. **Plugin-portability**: the module's schema-variant knobs handle the `DueDate`-absent / `CancelledDate`-not-`CancelledFlag` shape of this pod without a code change. `detect_ap_aging_params` auto-detected.
7. **Idempotent build** + **single-scan SQL** + **broadcast-friendly LEFT JOIN** to a tiny dim → 21.4s for 9,873 open rows → 132 grain rows.

## Acceptance against PLAN §8

* [x] `transforms/gold/ap_aging.py` exists; follows the gl_balance / supplier_spend pattern
* [x] Writes `gold.ap_aging` Delta table on `fusion_autopilot_dev`
* [x] Currency in grain — per-currency-per-bucket emitted; no cross-currency sum published
* [x] Mart-name gate honoured — per-column and coalesced fractions recorded; decision (real mode) matches the 80% gate outcome
* [x] Credit handling reported (table in stage 6); filter remains invariant `<> 0`
* [x] Both modes shipped — `TARGET_GOLD_TABLE_REAL` and `TARGET_GOLD_TABLE_PROXY` both exported; unit tests cover both
* [x] NULL invoice-date policy — `drop` selected based on probe; 0 NULL invoice-dates on this pod
* [x] Unit tests: 40 passing in `tests/unit/test_ap_aging.py`
* [x] Sample SQL committed (the per-currency-per-bucket query in stage 3)
* [x] Live evidence in `tests/live/TC24_ap_aging_results.md` showing real row counts + per-currency-per-bucket totals + per-currency reconciliation against bronze + `due_date_source` distribution
* [ ] BACKLOG.md flipped to `[~]` with commit SHA — pending commit
* [x] Module docstring documents: currency-in-grain rule, mart-name gate decision, due-date strategy, NET-30 fallback share, credit-handling decision, hardcoded-bucket policy, as-of-date injection contract

## Column schema (gold.ap_aging — real mode shipped here, 19 columns)

| # | column | type | source |
|---|---|---|---|
| 1 | `vendor_id` | BIGINT | `o.vendor_id` (fact-side) |
| 2 | `currency_code` | STRING | `UPPER(ApInvoicesInvoiceCurrencyCode)` |
| 3 | `supplier_number` | STRING | `ds.supplier_number` (NULL when dim miss) |
| 4 | `supplier_name` | STRING | `ds.supplier_name` (NULL when dim miss) |
| 5 | `business_relationship` | STRING | `ds.business_relationship` (NULL when dim miss) |
| 6 | `aging_bucket` | STRING | `CASE ... END` (5 values, or 6 with `unknown_date` policy) |
| 7 | `bucket_basis` | STRING | `'due_date'` (real) or `'invoice_date'` (proxy) |
| 8 | `open_invoice_count` | BIGINT | `COUNT(*)` |
| 9 | `open_amount` | DECIMAL(28,2) | `ROUND(SUM(open_amount), 2)` |
| 10 | `invoice_amount_total` | DECIMAL(28,2) | `ROUND(SUM(invoice_amount), 2)` |
| 11 | `amount_paid_total` | DECIMAL(28,2) | `ROUND(SUM(amount_paid), 2)` |
| 12 | `credit_open_amount` | DECIMAL(28,2) | `ROUND(SUM(CASE WHEN open_amount<0 ...), 2)` |
| 13 | `credit_open_count` | BIGINT | `SUM(CASE WHEN open_amount<0 THEN 1 ...)` |
| 14 | `net30_fallback_count` | BIGINT | real-mode only |
| 15 | `terms_date_count` | BIGINT | real-mode only |
| 16 | `due_date_count` | BIGINT | real-mode only |
| 17 | `oldest_invoice_date` | DATE | `MIN(invoice_date)` |
| 18 | `max_days_past_due` (real) / `max_days_outstanding` (proxy) | INT | `MAX(DATEDIFF(as_of, due_date))` / `MAX(DATEDIFF(as_of, invoice_date))` |
| 19 | `as_of_date` | DATE | `CAST(as_of_date_expr AS DATE)` |
| 20 | `gold_built_at` | TIMESTAMP | `current_timestamp()` at build time |

20 columns total. The mode-specific column name (`max_days_past_due` vs
`max_days_outstanding`) self-documents semantics at *schema* time, in
addition to the runtime `bucket_basis` audit column. Calling
invoice-age "days past due" in proxy mode would reintroduce the exact
semantic confusion the mart-name gate (PLAN §3.2) exists to prevent.

## Followups / non-blockers for v0.2.0

1. **`dim_supplier` coverage on demo-pod** — 0% match. Tracked separately; production pods will have higher coverage. Same finding as TC8c.
2. **`current` bucket empty** — demo-pod data-age artifact; production pods with active AP will populate it.
3. **AP installment schedule PVO** (`InvoiceInstallmentExtractPVO`) — out of scope for v0.2.0 per PLAN §7. Would be needed only if a tenant required per-installment aging and real header due-date coverage fell below 80% (which would otherwise route to proxy mode). Not needed here.
