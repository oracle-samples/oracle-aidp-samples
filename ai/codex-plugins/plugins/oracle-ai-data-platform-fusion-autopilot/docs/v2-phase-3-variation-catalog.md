---
title: Phase 3 variation-point audit — v1 modules → v2 pack vocabulary
generated_for: v2-phase-3-export-modules-to-sql
date: 2026-06-05
updated: 2026-06-06 (round-1 rework — widen gold projections to v1 parity)
---

> Historical report - not part of the current workflow. For current setup and
> operator guidance, use [project_setup.md](project_setup.md) and
> [../workflow.md](../workflow.md).

# Variation-point catalog

This document audits every tenant-variation knob the five v1 silver/gold
modules currently probe at runtime and maps each one to the v2 content-pack
vocabulary (`columnAliases.<name>` / `semanticVariants.<name>` /
`profile.<key>` / `column.<name>` / `semantic.<name>` renderer tokens).

**Evidence discipline (PLAN §13.3.2)**: Phase 3 catalogs only what v1 modules
already probe against real tenants OR what published Oracle source
documents. No speculative additions.

## pack.yaml declarations as shipped

| v1 probe | pack.yaml binding | Renderer token | Source module |
|---|---|---|---|
| `SEGMENT1` (supplier natural key) | `columnAliases.supplier_natural_key` (candidates: `[SEGMENT1]`) | `{{ column.supplier_natural_key }}` | `dim_supplier.py` (line 93) |
| `VENDORID` | `columnAliases.vendor_id` (candidates: `[VENDORID]`) | `{{ column.vendor_id }}` | `dim_supplier.py` (line 101) |
| `KNOWN_CURRENCY_COL_ALIASES` | `columnAliases.invoice_currency_code` (candidates: `[ApInvoicesInvoiceCurrencyCode, ApInvoicesCurrencyCode]`) | `{{ column.invoice_currency_code }}` | `supplier_spend.py` (lines 77–80), `ap_aging.py` (line 557) |
| `cancelled_*` probe (date / flag) | `semanticVariants.cancelled_status` (candidates: `cancelled_date`, `cancelled_flag`) | `{{ semantic.cancelled_status }}` | `ap_aging.py` (lines 544–553) |
| COA `balancing` role source column | `columnAliases.coa_balancing_segment` (candidates: `[CodeCombinationSegment1]`) | `{{ column.coa_balancing_segment }}` | `dim_account.py` (DEFAULT_SEMANTIC_SEGMENT_MAP position 1) |
| COA `cost_center` role source column | `columnAliases.coa_cost_center_segment` (candidates: `[CodeCombinationSegment2]`) | `{{ column.coa_cost_center_segment }}` | `dim_account.py` (position 2) |
| COA `natural_account` role source column | `columnAliases.coa_natural_account_segment` (candidates: `[CodeCombinationSegment3]`) | `{{ column.coa_natural_account_segment }}` | `dim_account.py` (position 3) |

That is the complete set of declared variation points as of Phase 3
round-3 (restored after the round-2 regression).

## Round-3 restore notes — what changed since the round-2 rollback

### COA segment handling

The initial Phase 3 commit (round-1) declared three `columnAliases`
(`coa_balancing_segment` / `coa_cost_center_segment` /
`coa_natural_account_segment`) so `dim_account.sql` could read the
three Fusion COA role-aliased source columns through
`{{ column.coa_*_segment }}` substitution.

Round-2 misread external-reviewer "v1 row-equivalence" pressure as
"v2 SQL text must match v1 SQL text" and DELETED the three
columnAliases + hardcoded `CodeCombinationSegment1/2/3` directly into
`dim_account.sql`. That rollback was an architectural regression
under PLAN §9.5.4: the engine substitutes variation-point tokens at
runtime from values that `bootstrap` resolves at onboarding;
v1-equivalence on demo-pod is *satisfied* by the variation-point
design (resolved values happen to match v1's hardcodes), not in
conflict with it.

Round-3 (feature `v2-phase-3-fix-variation-points`) restores the
three columnAliases + the three `{{ column.* }}` tokens. On
demo-pod's conventional COA, bootstrap auto-resolves each role to
its single conventional candidate (`Segment1` / `Segment2` /
`Segment3`); the renderer substitutes those identifiers into
`dim_account.sql`; output rows are byte-identical to v1.

**Two honest limits remain (`LIMITS.md P3-L2`):**

1. **Only three of six v1 COA roles are declared.** `subaccount` /
   `product` / `intercompany` are NOT in `columnAliases`;
   `dim_account.sql` emits them via positional hardcoded references
   (`Segment4 AS subaccount`, etc.). A tenant whose `subaccount`
   role lives at a non-conventional position triggers the same
   skill-overlay path described below, but the lack of a declared
   role-alias means the overlay must declare it from scratch.

2. **`columnAliases` existence-based resolution cannot disambiguate
   role-positioning.** Each declared role's candidate list is a
   single conventional default (e.g.,
   `coa_balancing_segment.candidates: [CodeCombinationSegment1]`).
   On a non-conventional tenant where balancing actually lives at
   `Segment4`, `Segment1` STILL exists in bronze — Fusion always
   emits all six — so bootstrap auto-resolves to `Segment1` and
   silently mis-resolves. The recovery path is for the operator to
   pre-author an overlay extending the candidate list with the
   role's actual source column (or hand-edit
   `profile.resolved.column.coa_<role>_segment` directly) BEFORE
   running bootstrap.

**Future architectural fix** (out of v0.3 scope): a new
`{{ coa.<role> }}` renderer token consuming
`profile.chartOfAccounts.<role>Segment` integers makes
role-positioning explicit rather than relying on existence-based
resolution. Bootstrap prompts the operator at onboarding for each
role's position (or reads from a structured `chartOfAccounts`
profile block); the renderer emits `CodeCombinationSegment<N>` based
on the resolved integer. This requires (a) new renderer vocabulary,
(b) bootstrap UX changes, (c) live-tenant evidence justifying the
work — none in v0.3 scope.

### AP aging — proxy mode only (intentional v2 narrow)

v1 `transforms/gold/ap_aging.py` defaults `due_date_mode='auto'` and
probes `ApInvoicesTermsDate` / `ApInvoicesDueDate` coverage at runtime
(`detect_ap_aging_params` at lines 504–568). If coverage exceeds
`DEFAULT_REAL_MODE_GATE_THRESHOLD` (10% — line 663), v1 switches to
**real mode** which buckets on `DATEDIFF(<snapshot>, due_date)`, emits
`bucket_basis = 'due_date'`, renames the max column to
`max_days_past_due`, and adds three provenance counts
(`due_date_count`, `terms_date_count`, `net30_fallback_count`). Otherwise
v1 falls back to **proxy mode** (line 487+) using
`DATEDIFF(<snapshot>, invoice_date)`, `bucket_basis = 'invoice_date'`,
`max_days_outstanding`.

The v2 content-pack `ap_aging.sql` ships **proxy mode only** as an
intentional scope decision. Three reasons:

1. **Runtime coverage probing is exactly what ADR-0014 removes.** v2
   replaces "detect column at runtime + branch" with declarative
   variation-point resolution at bootstrap. The auto-detection logic
   in v1 (`detect_ap_aging_params` line 504+) is the canonical example
   of v1 behaviour we're refactoring out, not preserving.
2. **The two modes have different output schemas.** Real mode emits
   `max_days_past_due` + provenance counts; proxy mode emits
   `max_days_outstanding`. The renderer's static-token vocabulary
   cannot conditionally select between two schema shapes from one
   template — that's a different renderer feature (`outputSchema`
   per-mode variants) which is not v0.3.
3. **The on-pack `dashboards/payables.yaml` and `executive_cfo.yaml`
   bind the proxy-mode column shape** (`age_bucket`, `open_amount`).
   Shipping real-mode would force a dashboard rewrite that's
   independent of the migration's stated value.

**Out-of-scope for Phase 3 — tracked for follow-up**: auto/real-mode
AP aging is a future feature that needs (a) a renderer extension for
optional column projection / two-schema variants, (b) declarative
tenant-side coverage threshold configuration, and (c) live evidence
that any demo-pod-or-comparable tenant has Terms/Due-date coverage
above the threshold (the v1 default 10%).

**Acceptance impact**: the parity harness ships proxy-mode-only fixture
rows so v1 and v2 land in proxy mode under the same conditions. Tenants
whose live AP data would auto-route to real mode under v1 will see
different `ap_aging` output than v2 — that's a documented divergence,
not a bug. See `LIMITS.md` for the resulting Phase 3 limitation entry.

## `dim_calendar` (builtin) — no SQL template

ADR-0011: `dim_calendar` stays `implementation.type: builtin`. Phase 3's
contribution is the content-pack builtin-dispatch path (Step 3) and the
widened `outputSchema` (16 columns matching the actual builtin emit at
`dim_calendar.py:94-138`). No variation points — the calendar is
parameter-driven, not bronze-driven.

Calendar parameters (consumed by the new `dim_calendar_adapter` per Step 3):

| Parameter | Source (precedence: tenant profile → pack default → builtin default) |
|---|---|
| `start_date` | `profile.profile.calendar.startDate` → `pack.pack.profiles[<active>].calendar.startDate` (`'2020-01-01'`) → builtin default (`'2020-01-01'`) |
| `end_date` | `profile.profile.calendar.endDate` → pack default (`'2030-12-31'`) → builtin (`'2030-12-31'`) |
| `fiscal_start_month` | `profile.profile.calendar.fiscalStartMonth` → pack default (`1`) → builtin (`1`) |
| `silver_table` | `f"{ctx.catalog}.{ctx.silver_schema}.{node.target}"` |
| `run_id` | `ctx.run_id` |

## Snapshot-date handling — Step 2 dedicated token

`ap_aging.py` uses `CURRENT_DATE()` inline for bucket anchoring. v2 SQL
template uses the new dedicated `{{ snapshot_date }}` renderer token
(Step 2), NOT `{{ profile.snapshotDate }}` (the generic profile resolver
binds values as parameters, not as DATE expressions). Semantics:

- `profile.profile.snapshotDate` absent / empty → emit literal
  `CURRENT_DATE()` (production default).
- Present + valid ISO date (`^\d{4}-\d{2}-\d{2}$`) → bind as
  `:snapshot_date` parameter (test determinism).
- Anything else → reject with `AIDPF-5013` (`InvalidSnapshotDateError`).

## Audit discoveries — DEFERRED (NOT added in Phase 3)

### `ApInvoicesCancelDate` alias variant

`ap_aging.py:546-549` probes a third cancelled-status variant
(`ApInvoicesCancelDate` — the alias variant some Fusion extracts use,
omitting the "led" suffix). The v1 comment marks it as an alias of
`ApInvoicesCancelledDate` with identical semantics. **Deferred** to a
follow-up fix-commit when a live tenant hits the variant under
content-pack execution.

### Optional `terms_date_col` / `due_date_col`

Covered above under "AP aging — proxy mode only". Deferred for the
same reasons as the auto/real-mode split.

## Summary

| v1 probe | Disposition in Phase 3 |
|---|---|
| `SEGMENT1` / `VENDORID` | Declared as `columnAliases.supplier_natural_key` / `vendor_id` |
| `KNOWN_CURRENCY_COL_ALIASES` | Declared as `columnAliases.invoice_currency_code` |
| `cancelled_*` (date / flag) | Declared as `semanticVariants.cancelled_status` (two candidates) |
| `semantic_segment_map` — three role aliases (`balancing` / `cost_center` / `natural_account`) | **DECLARED** as `columnAliases.coa_{balancing,cost_center,natural_account}_segment` — single-candidate-per-role lists pinning conventional `Segment1/2/3`. `dim_account.sql` substitutes via `{{ column.coa_*_segment }}`. Output column names stay `company` / `cost_center` / `account` per v1 convention. (Round-3 restore after the round-2 rollback regression.) |
| `semantic_segment_map` — three other roles (`subaccount` / `product` / `intercompany`) | Hardcoded positional in `dim_account.sql` (lines 19–21). Limit captured in `LIMITS.md P3-L2` (gap 1); skill-overlay recovery path documented. |
| COA role-positioning on non-conventional tenants | Limit captured in `LIMITS.md P3-L2` (gap 2). `columnAliases` existence-based resolution cannot disambiguate; pre-authored overlay or hand-edited profile required BEFORE bootstrap. Future `{{ coa.<role> }}` renderer token is the architectural fix; out of v0.3 scope. |
| `CURRENT_DATE()` anchor | **NEW** renderer token `{{ snapshot_date }}` (Step 2) |
| `dim_calendar` parameters | **NEW** builtin adapter (Step 3) |
| `due_date_mode='auto'` runtime probe | **DEFERRED** — v2 ships proxy-mode-only; auto/real awaits a renderer feature + live-tenant evidence |
| `ApInvoicesCancelDate` alias variant | **DEFERRED** — fix-commit when live tenant hits it |
