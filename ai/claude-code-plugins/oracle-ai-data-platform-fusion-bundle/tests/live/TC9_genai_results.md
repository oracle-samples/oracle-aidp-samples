# TC9 — GenAI agent grounded on supplier-spend gold mart, live results 2026-04-30

End-to-end use case from pdf1 §"GenAI agent development" proven against `fusion_catalog.gold.supplier_spend`.

## Setup

- **Cluster**: `tpcds` (workspace `54368733-3a17-47a1-b231-869d8ae2a048`)
- **Model**: `openai.gpt-5.4` (called via Spark SQL `ai_generate('openai.gpt-5.4', <prompt>)`)
- **Source data**: `fusion_catalog.gold.supplier_spend` (236 rows, $3.2B grand total, 116 vendors — built in TC8)
- **Grounding**: top-20 vendor rollups passed inline in prompt as plain-text rows

## Pattern verified

```sql
SELECT ai_generate('openai.gpt-5.4', ?) AS analysis
-- bind parameter ?: prompt with embedded data context (vendor_id, total_amount, paid, etc.)
```

Important: `ai_generate(model, prompt)` — model is the FIRST argument (counterintuitive; corrected after initial probe failure). See [project_aidp_ai_generate_signature.md](../../../../../.claude/projects/c--Users-anuma-aidp/memory/project_aidp_ai_generate_signature.md).

## Verbatim agent output

> **Top vendor by spend:** `vendor_id=300000047507499` is the largest supplier by approved invoice spend, with **$892,703,802.70** across **2,970** invoices; **$863,306,889.17** has been paid. Last invoice date: **2025-09-23**.
>
> **Spend concentration:** Total spend across these top-20 suppliers is **$3,409,905,222.50**. The top vendor, `300000047507499`, represents **26.18%** of that total (**$892,703,802.70 / $3,409,905,222.50**).
>
> **Records warranting review:** All listed records are **APPROVED**, so there are **no unapproved suppliers** in this rollup. However, there are unusual items that merit review:
>   - `vendor_id=300000075469611`: **$71,509,450.35** total across only **18** invoices, but only **$7,686.90** paid; last invoice date is **2018-12-21**, which is highly stale.
>   - `vendor_id=-10016`: negative/nonstandard vendor ID with **11,009** invoices totaling **$46,075,293.14**; although almost fully paid (**$46,073,099.20**), the ID format suggests a master data or placeholder vendor review.
>   - Several approved vendors have large unpaid balances, including:
>     - `300000047414635`: **$39,916,205.26** unpaid
>     - `300000051066172`: **$34,561,671.61** unpaid
>     - `300000047507546`: **$40,597,415.38** unpaid
>     - `300000049521222`: **$34,554,820.23** unpaid
>
> **Actionable AP recommendation:** Prioritize an aging and exception review of **approved but underpaid/high-balance suppliers**, starting with `300000075469611` and the largest unpaid-balance vendors (`300000047507546`, `300000047414635`, `300000051066172`, `300000049521222`). This should confirm whether balances reflect valid open liabilities, payment holds, disputes, or stale/duplicate invoices, and whether the supplier master for `vendor_id=-10016` needs correction.

## Quality assessment

| Test | Pass? | Evidence |
|---|---|---|
| Reads grounded data correctly | ✅ | Quoted `vendor_id=300000047507499` $892.7M / 2,970 invoices verbatim from the input |
| Computes math accurately | ✅ | `892,703,802.70 / 3,409,905,222.50 = 26.18%` correct |
| Identifies real anomalies | ✅ | Caught the negative `vendor_id=-10016`, the 2018-12-21 stale invoice, large unpaid balances |
| Actionable output | ✅ | Recommends aging review starting with specific vendors |
| No hallucination | ✅ | Every numeric value cited maps back to an input row |

**Status: PASS.** The "Custom GenAI agents grounded on Fusion data" use case from the bundle's pitch is real and works on the demo pod with no extra wiring beyond the gold mart.

## Bundle implications

- The bundle's example notebooks should include a `ai_generate('openai.gpt-5.4', ...)` call against gold.supplier_spend as a flagship demo.
- The plan's TC9 row referencing "ai_generate(...)" should note the correct (model, prompt) arg order.
- Future v2 work: add a CLI helper `aidp-fusion-bundle agent ask "your question"` that wraps this pattern.
