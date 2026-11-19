# TC31_v2_cli_content_pack_seed_live — Live evidence trail

**Status:** PASS — captured 2026-06-10/11 on `demo-pod` via a dedicated
dev cluster. Operator-driven REST dispatch through the production CLI
(`aidp-fusion-autopilot run`, content-pack backend — the only backend
post-Phase-9).

Infra identifiers (datalake OCID, workspace/cluster keys, pod URL, BICC
user, storage profile, password) are intentionally omitted per the
repo redaction rule; they live in the gitignored `dev/` config. `run_id`s
are shown as 8-char prefixes.

## What this validates

The v2 content-pack medallion seed runs end-to-end through the real CLI
on a live Fusion tenant, plus the six fixes/features on branch
`fix/v2-seed-bronze-gates-marker-4071`.

## A — full medallion seed (all 10 nodes)

`aidp-fusion-autopilot run --mode seed` (bundle declares the finance gold
marts; D-1 transitive include pulls silver + bronze). `run_id 6694262a…`,
**10 success / 0 failed / 0 skipped**, ~28 min wall.

| dataset_id | layer | status | row_count | dur (s) |
|---|---|---|---|---|
| ap_invoices | bronze | SUCCESS | 49,552 | 88.5 |
| erp_suppliers | bronze | SUCCESS | 209 | 85.2 |
| gl_coa | bronze | SUCCESS | 63,464 | 87.4 |
| gl_period_balances | bronze | SUCCESS | 11,211,211 | 1057.3 |
| dim_account | silver | SUCCESS | 63,464 | 19.7 |
| dim_calendar | silver | SUCCESS | 4,018 | 51.1 |
| dim_supplier | silver | SUCCESS | 209 | 43.1 |
| ap_aging | gold | SUCCESS | 131 | 45.1 |
| gl_balance | gold | SUCCESS | 10,184,102 | 74.7 |
| supplier_spend | gold | SUCCESS | 309 | 40.0 |

Row counts match the Phase-4 baseline expectations recorded in
`TC_phase5_v2_default_seed_live.md` exactly. SOX audit trail verified:
`dim_account` 63,464/63,464 and `ap_aging` 131/131 rows carry the run's
`silver_run_id` / `gold_run_id`.

## B — the six fixes proven

1. **`content_pack_staging` bronze staging (`AIDPF-1045`)** — before the
   fix, every content-pack CLI dispatch failed `AIDPF-1045
   LayerFilterEmptiedPlanError` because `bronze/*.yaml` nodes were never
   staged to the cluster (empty `pack.bronze`). After: bronze nodes
   resolve and execute (table A).
2. **`AIDPF-4070` bronze subset + case-insensitive** — bronze writes the
   full raw PVO (143–232 cols) vs the curated `outputSchema`; Step-8 now
   asserts declared ⊆ materialised, case-insensitive. Live bronze passed.
3. **Marker base64 fix** — before, a run with any failed step degraded to
   `DISPATCH_MARKER_DEGRADED` (regex-recovered run_id, no table). After,
   the CLI renders the step table on success **and** failure (see C/D).
4. **`dispatch_v2_seed` stray-comma** — generated notebook compiles.
5. **Bronze YAML type alignment** — 19 columns aligned to live BICC
   (`long`→`decimal(18,0)/(38,30)`, `timestamp`→`date`); A passed Step-8.
6. **`AIDPF-4071` pre-ingest gate** — see C.

## C — `AIDPF-4071` pre-ingest gate (fail-fast)

Injected a column absent from the PVO (`DRIFT_TEST_NONEXISTENT_COL`) into
`erp_suppliers.outputSchema` and ran `--datasets erp_suppliers`:

- `run_id 37ed8760…` — `erp_suppliers` failed `source_schema_missing`
  (`AIDPF-4071`) in **12.45 s** (metadata probe only — vs ~85 s for a
  real extract). No row pull. Fail-fast confirmed.
- `run_id ae93a1d5…` — same, plus the laptop wrote
  `.aidp/diagnostics/<run_id>/AIDPF-4071__erp_suppliers.json` carrying the
  missing column + the full live PVO schema (143 cols, name+type). This is
  the `medallion-author` input.

## D — marker base64 before/after (same drift case)

- **Before:** `[DISPATCH_MARKER_DEGRADED] … summary marker is unparseable.
  Recovered run_id=… from regex fallback`. No table.
- **After (`run_id 64c05f05…`):** the CLI renders the Run summary table
  with `erp_suppliers │ bronze │ FAILED`, `0 success · 1 failed`, total
  32.6 s. `grep DISPATCH_MARKER_DEGRADED` → 0.

## E — P3-L3 bronze node corrections, live-verified (Option A)

Five never-live-validated bronze nodes had names+types corrected to the
live PVO (core-exact matching). A `--datasets`-scoped bronze seed
materialized **4 of 5** with real data:

| node | rows | cols |
|---|---|---|
| ap_payments | 3,476,916 | 40 |
| ar_invoices | 187,970 | 250 |
| ar_receipts | 64,007 | 139 |
| po_orders | 16,769 | 158 |

`scm_items`: name fix correct (passes the gate) but the `ItemExtractPVO`
extract creates no table on this tenant — a separate, deeper issue
deferred to Option B (`LIMITS.md` P3-L3 / `dev/PLAN…md` §27).

## F — upfront source gate (fail-fast, pre-extract)

The `AIDPF-4071` gate runs as a single metadata-only probe over all
in-scope bronze PVOs **before any extraction**, checking each bronze
node's declared `outputSchema` columns AND the columns in-scope
silver/gold nodes require from it (transitive, audit cols excluded).

Live-verified (`run_id fee8c664…`): a **valid bronze** (`erp_suppliers`,
all declared cols present in PVO) with an **invalid silver need**
(`dim_supplier` injected to require a column absent from the PVO) aborted
the run in **0.00 s of extraction** — `erp_suppliers` FAILED, `ap_invoices`
+ `dim_supplier` + `supplier_spend` SKIPPED (aborted), the
`AIDPF-4071__erp_suppliers.json` diagnostic written (missing col + full
143-col PVO schema), `DISPATCH_MARKER_DEGRADED` → 0. Confirms a silver/gold
need its bronze source can't satisfy fails in seconds, before the
multi-minute pull — not after.

## G — mart-only run does NOT re-seed bronze

`run --mode seed --layers silver,gold` now keys off the requested layers:
bronze stays in the plan for lineage but is NOT executed; the marts rebuild
against the LANDED bronze tables. Live (`run_id fe9da2e5…`): all 6 marts
SUCCESS — `dim_account` 63,464 / `dim_calendar` 4,018 / `dim_supplier` 209 /
`ap_aging` 131 / `gl_balance` 10,184,102 / `supplier_spend` 309 — in
**~5 min (300s), zero bronze rows, no re-extract** (`gl_balance` read 10.18M
from the existing table). Before the fix this re-extracted all bronze (~28
min, incl. the ~18-min `gl_period_balances` pull).

## H — missing bronze table → fail fast with seed-command suggestion

A mart-only run validates the landed bronze via the readiness gate before
any mart runs. Live: dropped `bronze.erp_suppliers`, then
`run --layers silver,gold` aborted in **0.00s** with one
`__bronze_readiness_gate__` FAILED step:

```
[AIDPF-2071] bronze readiness gate failed for tables: ['erp_suppliers'].
  - erp_suppliers: table missing (run `aidp-fusion-autopilot run --layers bronze
    --datasets erp_suppliers` first).
```

Running that suggested command then restored the table (`erp_suppliers`
209 rows, `run_id 1abcd6df…`) — proving the remediation the gate hands the
operator actually works.

**First-seed blocker found + fixed here:** the suggested re-seed initially
crashed because `bronze_extract` preflight `DESCRIBE`d its own
not-yet-existent target (uncaught `AnalysisException`). That blocks the
**first-ever seed of any bronze node on a fresh tenant** — hidden until now
only because the starter pack's bronze tables pre-existed. Fixed:
preflight skips table-introspection for `bronze_extract` nodes (source is
the PVO). Re-seed then succeeded.

## Notes

- 7 starter-pack bronze nodes with no downstream silver/gold shipped
  never-live-validated column names. Option A (§E) corrected 5; 2
  (`gl_journal_lines`, `po_receipts`) + the `scm_items` extract issue
  remain in Option B (`LIMITS.md` P3-L3, `dev/PLAN…md` §27). The
  `AIDPF-4071` gate diagnoses any remaining mismatch automatically.
- Non-`demo-pod` tenant evidence (P3.7 / P3.9) is still outstanding for
  any "plugin-portable" claim — this run proves the demo pod only.
