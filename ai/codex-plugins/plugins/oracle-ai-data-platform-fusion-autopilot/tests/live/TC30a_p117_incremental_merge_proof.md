# TC30a — P1.17 incremental MERGE silver/gold proof (+ TC28b clock-skew) on `fusion_autopilot_dev`

**Test case ID**: TC30a (silver/gold MERGE path proof via synthetic Delta DML on the `demo-pod` demo pod).
**Stage E plan items**: E1a + E2 (`docs/features/p1.17-incremental-merge/plan.md`).
**Status**: ✅ **EXECUTED 2026-06-02** on `fusion_autopilot_dev` cluster / `playground` workspace. All 11 per-layer state-table assertions passed.

## What this verifies

TC30a is the **gating live-evidence test for P1.17 merge**. It exercises the full silver/gold MERGE write path against real Delta tables on a real AIDP cluster, using a synthetic Delta DML edit on bronze to simulate a Fusion-side delta that BICC's demo-pod path cannot manufacture (Fusion data is frozen on `demo-pod`).

The proof covers four contracts from the plan's Stage B:

1. **Bronze BICC short-circuit (§B6 source-count empty-delta gate)** — when BICC returns zero rows under `--mode incremental`, the bronze cursor is preserved (not advanced to the new run's clock), and no MERGE commit appears in Delta history.
2. **Silver `dim_supplier` MERGE (§B2 silver template + §B8a layer-local cursor)** — the synthetic edit on `bronze.erp_suppliers` is picked up by the source predicate `WHERE _extract_ts > <prior_silver_watermark>`, and the silver cursor advances to the edit's `_extract_ts` value (un-windowed `MAX(bronze_extract_ts)`).
3. **Gold `supplier_spend` exempt-path full-recompute (§B2 + §C1a)** — flagged `incremental_capable=False`, the mart always emits seed-shape SQL. The gold cursor still advances (post-build MAX(bronze_extract_ts) capture fires in BOTH modes per §B8), incorporating the synthetic edit on `bronze.ap_invoices`.
4. **No-op-cycle cursor preservation (§E1a Run C)** — when no bronze delta arrives between two incremental runs, all four layer-local cursors are preserved at the prior values.

Additionally pinned in the same dispatch:

- **TC28b clock-skew probe (E2)** — a single round-trip via `extract_pvo` measures the AIDP↔BICC↔OCI Object Storage path latency; asserts the observed value < `bundle.incremental.watermark_safety_window_seconds`.

## Live evidence — three orchestrator runs (A=seed, B=incremental w/ DML, C=incremental no-op)

### Bundle scope

Narrow — `erp_suppliers` + `ap_invoices` bronze + `dim_supplier` + `dim_calendar` silver + `supplier_spend` gold. `bundle.incremental.watermarkSafetyWindowSeconds: 3600`.

### Run IDs

| run | mode | run_id | scope |
|-----|------|--------|-------|
| A | seed | `<run-A>` | Baseline materialization. Captures bronze + silver + gold cursors in `fusion_autopilot_state`. |
| B | incremental | `<run-B>` | After synthetic DML on BOTH bronze tables (one row each). |
| C | incremental | `<run-C>` | No DML between B and C — true no-op cycle. |

### Synthetic Delta DML inject (between Run A and Run B)

Two rows touched, both `_extract_ts = T_A + 10 minutes`:

- `bronze.ap_invoices` — `ApInvoicesInvoiceId = 1` (one row, payload unchanged, `_extract_ts` bumped). Exercises the gold `supplier_spend` exempt-path full-recompute (which reads `bronze.ap_invoices`).
- `bronze.erp_suppliers` — `SEGMENT1 = "1252"` (one row, payload unchanged, `_extract_ts` bumped). Exercises the silver `dim_supplier` MERGE (which reads `bronze.erp_suppliers`).

### TC28b — clock-skew probe (Phase 0 of the notebook)

| measurement | value |
|---|---|
| AIDP→BICC round-trip (`extract_pvo` warm-up call) | **32.2s** |
| `bundle.incremental.watermark_safety_window_seconds` | 3600s |
| Skew < safety window? | **✅ True** |

The round-trip is small relative to the safety window (~0.9% of the budget). Production operators on a new tenant should re-run this probe with their own connection latency to confirm the default 3600s window absorbs their observed skew comfortably (per the operator playbook in plan §F1).

### Per-layer assertions — all 11 passed

| # | assertion | result | detail |
|---|---|:-:|---|
| 1 | bronze.ap_invoices cursor PRESERVED Run A→B | ✅ | pre=post=`<T_ap_A>` |
| 2 | bronze.erp_suppliers cursor PRESERVED Run A→B | ✅ | pre=post=`<T_sup_A>` |
| 3 | silver.dim_supplier cursor **ADVANCED** Run A→B (synthetic DML triggers MERGE) | ✅ | pre=`<T_silver_A>` → post=`<T_synthetic>` |
| 4 | gold.supplier_spend cursor **ADVANCED** Run A→B (exempt full-recompute picks up DML) | ✅ | pre=`<T_gold_A>` → post=`<T_synthetic>` |
| 5 | bronze.ap_invoices row_count UNCHANGED Run A→B (no MERGE inserts) | ✅ | A=49,552 B=49,552 |
| 6 | bronze.erp_suppliers row_count UNCHANGED Run A→B | ✅ | A=209 B=209 |
| 7 | gold.supplier_spend row_count UNCHANGED Run A→B (exempt-path: full recompute → same grain) | ✅ | A=309 B=309 |
| 8 | bronze.ap_invoices cursor PRESERVED Run B→C | ✅ | B=C=`<T_ap_A>` (preserved twice) |
| 9 | bronze.erp_suppliers cursor PRESERVED Run B→C | ✅ | B=C=`<T_sup_A>` |
| 10 | silver.dim_supplier cursor PRESERVED Run B→C (no-op cycle) | ✅ | B=C=`<T_synthetic>` |
| 11 | gold.supplier_spend cursor PRESERVED Run B→C | ✅ | B=C=`<T_synthetic>` |

### Two cursor semantics — visible in the state-table snapshot

The post-Run-B state-table snapshot makes the §B8/§B8a "two cursor semantics by layer" contract concrete:

```
+-----------------------+--------+---------+--------------------------+
| dataset_id:layer      | mode   | row_cnt | last_watermark            |
+-----------------------+--------+---------+--------------------------+
| ap_invoices:bronze    | incr.  |  49552  | <T_ap_A>      (windowed) |
| erp_suppliers:bronze  | incr.  |    209  | <T_sup_A>     (windowed) |
| dim_supplier:silver   | incr.  |    209  | <T_synthetic> (un-wind.) |
| dim_calendar:silver   | incr.  |   4018  | NULL          (exempt)   |
| supplier_spend:gold   | incr.  |    309  | <T_synthetic> (un-wind.) |
+-----------------------+--------+---------+--------------------------+
```

- **Bronze rows carry the windowed cursor** (`extract_started_at - safety_window`) — Run A's `extract_started_at` minus 1h. Used as input to the NEXT incremental run's `extract_pvo(watermark=…)` call.
- **Silver/gold rows carry the un-windowed cursor** (`MAX(bronze_extract_ts)` over the materialized table) — Run B's value reflects the synthetic edit's `_extract_ts` directly. Used as MERGE source predicate for the NEXT incremental run AND as audit/monitoring freshness signal.
- **`dim_calendar.last_watermark = NULL`** — exempt per Invariant 3; no source watermark to capture (parameter-driven calendar generation).

Concrete numbers from the run (redacted to relative `T_*` symbols for the public branch; raw timestamps in the executed notebook stored privately at `/tmp/p117-tc30a-<stamp>/tc30a_executed.ipynb`):

- `T_ap_A` = `2026-06-01T23:19:44.682964` (Run A's bronze.ap_invoices cursor)
- `T_sup_A` = `2026-06-01T23:18:42.573787` (Run A's bronze.erp_suppliers cursor)
- `T_silver_A` = `2026-06-02T00:18:42.573787` (Run A's silver.dim_supplier cursor — un-windowed, equals bronze _extract_ts + 1h gap)
- `T_gold_A` = `2026-06-02T00:19:44.682964` (Run A's gold.supplier_spend cursor)
- `T_synthetic` = `2026-06-02T00:27:52` (the DML-bumped `_extract_ts`, post-Run-B silver+gold cursors)

The **gap invariant** between bronze and silver/gold (§B8) holds exactly:

- `T_silver_A − T_ap_A` = `2026-06-02T00:18:42.573787 − 2026-06-01T23:18:42.573787` = **3600s** = WATERMARK_SAFETY_WINDOW
- `T_gold_A − T_ap_A` = `2026-06-02T00:19:44.682964 − 2026-06-01T23:19:44.682964` = **3600s**

After Run B, the gap invariant deliberately breaks (silver/gold cursors are pulled forward to T_synthetic by the synthetic edit's _extract_ts, while bronze cursors stay preserved). This is the documented behavior per plan §E1a: "Gap invariant DOES NOT apply on this test — the 3600s gap holds only when BICC drove the bronze advance AND silver/gold advanced in the same cycle." On a real non-demo tenant (TC30b) where BICC drives bronze, the gap would resume after the first real-delta cycle.

## What this DOES NOT verify (deferred per plan)

- **BICC → bronze MERGE with `source_delta_count > 0`** — demo-pod Fusion data is frozen so BICC cannot return a non-empty delta. The bronze MERGE template + the BICC→bronze threading are pinned by:
    - Unit tests `tests/unit/test_p117_orchestrator_dispatch.py::TestBronzeMergeSql` (SQL shape) + `TestExtractPvoWatermarkThreading` (kwarg threading).
    - TC30a's BICC-call assertion: `extract_pvo` received the windowed prior watermark on Run B (assertable from the bronze closure trace; the synthetic-DML short-circuit then preserves the cursor).
    - TC30b (deferred to BACKLOG P3.7 / P3.9) closes the gap with real Fusion-side churn.

- **gl_balance row-level MERGE** — the narrow scope used here doesn't include `gl_period_balances` bronze (~11M rows; too costly on the demo pod). The MERGE template is structurally identical to the silver `dim_supplier` MERGE (both use B2's row-level template with NULL-safe `<=>` on the natural key), so silver MERGE evidence covers the contract transitively. The composite-key NULL-safety (`translated_flag` NULL on demo-pod) is pinned by unit test `TestGLBalanceIncrementalSQL::test_incremental_emits_merge_with_composite_null_safe_on_clause`.

- **`incremental_capable=False` PVO behavior under MERGE** (`gl_period_balances`, `gl_coa`, `ap_aging_periods`) — same scope-exclusion reason; pinned by unit test `TestExtractPvoWatermarkThreading::test_incremental_capable_false_pvo_threads_none`.

These three gaps are explicit V1 scope boundaries (deferred to TC30b on a non-demo tenant + P1.17b for aggregate marts + P1.17d for schema-evolution).

## Dispatch metadata (redacted)

- AIDP REST host: `https://datalake.us-ashburn-1.oci.oraclecloud.com`
- Workspace: `playground` (display name; UUID redacted)
- Cluster: `fusion_autopilot_dev` (display name; UUID redacted)
- Workspace path: `/Workspace/Shared/p1.17-stage-e/tc30a_e1a_e2.ipynb`
- Wheel: P1.17 HEAD (built fresh on dispatch).
- jobKey / jobRunKey / taskRunKey: operator-redacted.
- Bundle config: narrow scope (custom, inlined in the dispatcher at `/tmp/tc30a_dispatch.py`).

## Cross-references

- `docs/features/p1.17-incremental-merge/plan.md` §E1a + §E2 — the gating contract this evidence file ships.
- `docs/features/p1.17-incremental-merge/plan.md` §B6/§B8/§B8a — the contracts the assertions pin.
- `tests/live/TC22b_TC23b_TC24b_p117_seed_regression.md` — companion file documenting the seed-mode regression check.
- `tests/live/TC_E4_xxhash_surrogate_stability.md` — companion file documenting P1.19's surrogate-key stability.
- `tests/live/TC28_orchestrator_incremental_infra.md` — β.1 infrastructure baseline (pre-P1.17, gate preserved).
- `tests/live/TC30b_*.md` (FUTURE — BACKLOG P3.7 / P3.9) — non-demo tenant BICC→bronze MERGE with real Fusion-side churn.
- `tests/unit/test_p117_*.py` — unit-test surface (77 tests) pinning the SQL shapes + dispatch contracts that this live evidence verifies in execution.
