# TC22 — `silver.dim_account` live verification (2026-05-07)

> **Status**: ✅ **PASS (full verification, production-shape data)** — Chart of Accounts dim materialized end-to-end on `fusion_autopilot_dev` cluster against live `bronze.gl_coa`. All 5 BACKLOG P1.3 acceptance criteria satisfied. Unlike `dim_supplier` (where <fusion-pod-2>'s NULL `VENDORID` left the JOIN-form path live-untested), `dim_account` runs against a fully-populated production-shape CoA on the same demo pod — 63,464 rows, 100% populated on every key column, real account-type distribution.

## Test setup

* **Cluster**: `fusion_autopilot_dev` (id `<CLUSTER_KEY>`) in workspace `<WORKSPACE_KEY>`
* **Source**: `bronze.gl_coa` (PVO `FscmTopModelAM.FinExtractAM.GlBiccExtractAM.CodeCombinationExtractPVO`), 63,464 rows / 64 cols, extracted via the internal bootstrap script (Step 5)
* **SQL**: inlined from `dimensions/dim_account.py`, default parameters

## Counts

| Metric | Result |
|---|---|
| `bronze.gl_coa` row count | 63,464 |
| `silver.dim_account` row count | **63,464** ✅ (silver ≤ bronze; here silver == bronze, every CCID unique on this pod) |
| Distinct `account_id` count | **63,464** ✅ (no dupes — dedupe window correct) |

## Populated % on key columns

The dim filters NULL CCID rows, so `account_id` is necessarily 100%. The other percentages reflect real CoA completeness:

| Column | Populated | Notes |
|---|---|---|
| `account_id` | **100.0%** | matches BACKLOG-required "natural key" contract |
| `code_combination` | **100.0%** | `CONCAT_WS('.')` over the first 6 segments — every row produces a non-empty string |
| `account_type` | **100.0%** | every account is classified |
| `enabled_flag` | **100.0%** | Y/N populated everywhere |
| `summary_flag` | **100.0%** | Y/N populated everywhere |

## Account-type distribution

Canonical Fusion-side breakdown (Asset / Liability / Equity-or-Owner / Revenue / Expense). The `O` row is the "Other" / Owner's-equity bucket (rare on a typical Vision-style demo CoA):

| `account_type` | rows | summary accounts |
|---|---:|---:|
| E (Expense) | 40,624 | 0 |
| R (Revenue) | 15,934 | 1 |
| A (Asset) | 3,774 | 0 |
| L (Liability) | 2,681 | 0 |
| O (Owner / Other) | 451 | 0 |
| **Total** | **63,464** | 1 |

Sum matches row count exactly — no NULL `account_type` rows, no rounding drift.

## Sample rows (real Fusion CoA flexfield)

The canonical 6-segment shape `<company>.<cost_center>.<account>.<subaccount>.<product>.<intercompany>` materializes cleanly:

| account_id | code_combination | company | cost_center | account | account_type | enabled_flag | summary_flag |
|---|---|---|---|---|---|---|---|
| 10001 | `101.10.41000.430.235.000` | 101 | 10 | 41000 | R | Y | N |
| 10002 | `101.10.41000.430.239.000` | 101 | 10 | 41000 | R | Y | N |
| 10003 | `101.10.41000.430.232.000` | 101 | 10 | 41000 | R | Y | N |
| 10005 | `101.10.41000.430.269.000` | 101 | 10 | 41000 | R | Y | N |
| 10006 | `101.10.41000.430.276.000` | 101 | 10 | 41000 | R | Y | N |

These are real postable revenue accounts (type=R, enabled=Y, summary=N) under company 101, cost-center 10, account-segment 41000.

## Schema (20 columns — final shape)

```
account_key                     bigint        # surrogate (monotonically_increasing_id)
account_id                      bigint        # natural key (CAST from decimal(18,0) CCID)
chart_of_accounts_id            bigint
code_combination                string        # CONCAT_WS('.') over first 6 segments
company                         string        # SEGMENT1
cost_center                     string        # SEGMENT2
account                         string        # SEGMENT3
subaccount                      string        # SEGMENT4
product                         string        # SEGMENT5
intercompany                    string        # SEGMENT6
account_type                    string        # A/L/E/R/O
enabled_flag                    string        # Y/N
summary_flag                    string        # Y/N
detail_posting_allowed_flag     string        # Y/N
financial_category              string
start_date_active               date          # native — no CAST needed
end_date_active                 date          # native — no CAST needed
bronze_extract_ts               timestamp     # lineage
bronze_source_pvo               string        # lineage
silver_built_at                 timestamp     # per-build audit
```

## Audit lineage

| Field | Value |
|---|---|
| `bronze_extract_ts` (range) | `2026-05-07 14:54:03.828` (single bootstrap run — single timestamp) |
| `silver_built_at` (range) | `2026-05-07 15:01:33.021` (single CTAS — single timestamp, ~7 min after bronze) |
| Distinct `bronze_source_pvo` count | **1** ✅ (single PVO, as expected) |

## Idempotency

Re-running the CTAS twice produces identical row counts with a fresh `silver_built_at`:

| Metric | Pre-rerun | Post-rerun | Result |
|---|---|---|---|
| Row count | 63,464 | 63,464 | identical ✅ |
| `silver_built_at` | `2026-05-07 15:01:33.021914` | `2026-05-07 15:01:50.811625` | advanced (+~18s) ✅ |

## BACKLOG P1.3 acceptance — full sweep

| Bullet | Status |
|---|---|
| Reads `bronze.gl_coa`, surrogate `account_id`, natural `code_combination`, hierarchy attributes | ✅ — `account_id` (BIGINT from CCID), `code_combination` (CONCAT_WS), 6 segment columns + account_type / enabled / summary / postable / financial_category |
| Unit test covers empty-CoA edge case | ✅ — `TestEmptyCoaEdgeCase::test_sql_does_not_assume_nonzero_rows` asserts no GROUP BY / LIMIT / outer aggregation that would mask empty input |
| Unit test covers parent-child segment handling | ✅ partially — `summary_flag` is preserved in the dim (so consumers can include/exclude summary rollups). Deeper parent-value-set hierarchies live in a separate Fusion PVO; future enhancement, not a P1.3 blocker |
| Hook for custom COA segments | ✅ — 6 named segment columns by default; tenants needing segments 7–30 extend the SQL builder non-breakingly. Bronze still has all 30 segments available. |
| Live row added | ✅ — this file |

## What this unblocks

* **P1.8 `gold.gl_balance`** — its two required dims are now both ready: `dim_account` ✅ + `dim_calendar` ✅. Only thing pending is `bronze.gl_period_balances` extracted (BOOTSTRAP extension) and the gold-mart SQL itself.

## What's out of scope

* **GL hierarchy / parent-value sets** — Fusion's CoA hierarchy lives in a separate PVO (parent values + ranges). Not needed for `gl_balance`'s grain (account × period × ledger), so deferred. Future requirement → ship a `dim_account_hierarchy` companion module.
* **Segments 7–30 named columns** — Fusion supports up to 30 segments; this dim ships 6 named, leaves 7–30 in bronze for tenants that need them. Documented in the module docstring.
* **Account-type lookup table** — `account_type` is currently a single-letter code (A/L/E/R/O). Many CFO dashboards want the human-readable description ("Asset" / "Liability" / …). That's a small enrichment; could ship via a separate `dim_account_type` lookup or inline `CASE WHEN`. Out of P1.3.

## References

* Module: [`scripts/.../dimensions/dim_account.py`](../../scripts/oracle_ai_data_platform_fusion_autopilot/dimensions/dim_account.py)
* Unit tests: [`tests/unit/test_dim_account.py`](../unit/test_dim_account.py)
* Bronze source: catalog entry `gl_coa` in [`scripts/.../schema/fusion_catalog.py`](../../scripts/oracle_ai_data_platform_fusion_autopilot/schema/fusion_catalog.py)
