# Phase 4 multi-tenant coverage

> Historical report - not part of the current workflow. For current setup and
> operator guidance, use [project_setup.md](project_setup.md) and
> [../workflow.md](../workflow.md).

`tests/parity/test_dual_runner_profiles.py` runs the dual-runner harness
across two profiles. This document records the cross-profile diff (which
rows the cancelled-status semantic filters; how `snapshot_date` shifts
ap_aging bucket arithmetic) so a Phase 5 reviewer can predict what
each profile produces without running the harness.

## Profiles

| Profile | `semantic.cancelled_status` | `profile.snapshotDate` | `bronzeSchemaFingerprint` |
|---|---|---|---|
| `finance-default` | `cancelled_date` (filter: `ApInvoicesCancelledDate IS NULL`) | `2026-06-05` | `sha256:placeholder-finance-default-2026-06-05` |
| `finance-alt-cancelled-flag` | `cancelled_flag` (filter: `COALESCE(...CancelledFlag, 'N') != 'Y'`) | `2025-12-31` | `sha256:placeholder-parity-tenant-b-2026-06-05` |

## Cross-profile diff (synthetic fixture)

### `bronze_fixtures.py` (finance-default fixture)

`ap_invoices` carries one cancelled invoice via `CancelledDate` populated;
the `cancelled_date IS NULL` filter excludes it. Three active invoices
flow into `ap_aging` + `supplier_spend`.

### `bronze_fixtures_tenant_b.py` (finance-alt-cancelled-flag fixture)

`ap_invoices` carries three rows — one active, one with `CancelledFlag='Y'`,
one with `CancelledFlag='N'`. The `COALESCE(..., 'N') != 'Y'` filter
keeps the active + the 'N' row; the 'Y' row is excluded.

### Row-count impact per output node

| Node | finance-default | finance-alt-cancelled-flag |
|---|---|---|
| `dim_supplier` | unchanged (same `erp_suppliers` fixture) | unchanged |
| `dim_account` | unchanged (same `gl_coa` fixture) | unchanged |
| `dim_calendar` | unchanged (parameter-driven) | shifted by `snapshotDate` — start/end already cover both dates so identical row counts |
| `gl_balance` | unchanged (no ap_invoices dependency) | unchanged |
| `supplier_spend` | filters cancelled by date; counts active invoices | filters cancelled by flag; counts active + 'N'-flagged invoices |
| `ap_aging` | bucket arithmetic anchored at `2026-06-05` | bucket arithmetic anchored at `2025-12-31` → buckets re-assigned per row |

The contract Phase 4 asserts is: **within a profile, both backends
produce identical outputs**. The cross-profile diff is informational —
both profiles read the same SQL templates; the diff just walks what
the variation-point machinery DOES.

## Out of scope (EXPLAINED-DIVERGENCE — Phase 5 prerequisite)

**Non-conventional COA segment positioning** is NOT validated by
Phase 4. The v0.3 pack vocabulary hardcodes the conventional six-segment
shape in `silver/dim_account.sql`; a tenant whose `balancing` lives at
`CodeCombinationSegment4` instead of `Segment1` cannot be expressed by
the existing `columnAliases` / `semanticVariants` blocks. The fix is a
future `{{ coa.<role> }}` renderer token that reads
`profile.chartOfAccounts.<role>Segment` integers — out of v0.3 scope.

Phase 5's default-flip PR inherits this as a documented limitation
when it claims "plugin-portable" status; live evidence on a real
non-conventional tenant is the gating artefact.

## Fingerprint identity contract

Both profiles carry distinct synthetic fingerprints — not real
tenant fingerprints. Bootstrap on a real pod would compute the actual
sha256 over the live bronze schema. The PAIRED
`finance-alt-cancelled-flag.schema-snapshot.yaml` carries the same
synthetic fingerprint so cluster-side preflight finds equality (no
spurious AIDPF-2012 during the multi-tenant parity test).
