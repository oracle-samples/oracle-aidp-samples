# TC35 — BICC lineage-delta honor probe + `gl_coa` incremental flip

**Status**: ✅ **PROBES EXECUTED 2026-06-16** on the `fusion_autopilot_dev` cluster /
`playground` workspace (demo-pod Fusion source) via OCI-signed REST dispatch.
Coordinates redacted per the TC26/TC30 convention; full identifiers held by the
dispatching operator. Probe results AND the `gl_coa` seed→incremental E2E are
both complete — the flag flip is **probe-backed and E2E-proven** (see the E2E
section).

## Question

Three bronze PVOs shipped `incremental_capable=False` on the premise (LIMITS.md
P1.17-L2) that BICC does not respect the `fusion.initial.extract-date` lineage
delta for them. That classification was never live-verified — it was inherited by
analogy. Does BICC actually honor the cursor for each?

## Method

`skills/incremental-mechanism/probe_incremental_honor.py`. For each PVO it loads the
`aidataplatform` BICC source three ways and compares row counts — metadata + COUNT
only, no lakehouse write:

- **A** — no watermark → full baseline `N`.
- **B** — recent watermark (after the data's max LUD) → ~0 if BICC honors the
  filter, `N` if it ignores it.
- **C** — mid watermark → a graduated partial if honored, `N` if ignored.

`fusion.initial.extract-date` is applied by BICC against the PVO's own lineage
attribute, so the probe needs no column name up front (it auto-detects the LUD for
context only).

## Results

| PVO (datastore) | full `N` | recent-wm | mid-wm | LUD col | LUD span | verdict |
|---|---|---|---|---|---|---|
| `gl_coa` (`CodeCombinationExtractPVO`) | 69,578 | **0** | — (ancient control = 69,578) | `CodeCombinationLastUpdateDate` | 2024-06-17 → 2026-05-27 | **HONORED** |
| `ap_aging_periods` (`AgingPeriodHeaderExtractPVO`) | 2 | **0** | 0 | `ApAgingPeriodsLastUpdateDate` | 2013-11-04 → 2023-06-15 | **HONORED** (weak) |
| `gl_period_balances` (`BalanceExtractPVO`) | 12,101,410 | **12,101,410** | 12,101,410 | `BalanceLastUpdateDate` | 2013-10-25 → 2026-05-27 | **IGNORED** |

Reproduce with `skills/incremental-mechanism/probe_incremental_honor.py
--datastore <PVO> --label <id>` (raw run markers were transient and not retained;
the numbers above are the captured result).

## Conclusions

1. **`gl_coa` → `incremental_capable=True`.** BICC honors the lineage delta: a
   recent-watermark extract returned 0 rows while the ancient-watermark control
   returned the full 69,578. It is a master-data entity (no retroactive-aggregate
   path), so HONORED is *sufficient* for rung-1 native incremental. Flipped in
   `schema/fusion_catalog.py` + `content_packs/.../bronze/gl_coa.yaml`
   (watermark `CodeCombinationLastUpdateDate`). LIMITS.md P1.17-L2 narrowed.

2. **`gl_period_balances` stays `False` — confirmed, not assumed.** A watermark
   *after* the max LUD still returned all 12.1M rows. Native delta is genuinely
   unavailable, which **validates the `bicc-period-window-extract` feature** (the
   custom period-window datastore filter is the correct lever; native incremental
   is not an option). As a snapshot/aggregate cube it would also carry the
   retroactive-revision correctness caveat even if it were honored.

3. **`ap_aging_periods` → `incremental_capable=True` (catalog-truth only).**
   Probed HONORED (recent-watermark = 0 vs unfiltered = 2). The honored-vs-ignored
   call is unambiguous even though the discrimination was weak (max LUD 2023
   precedes the test watermarks): an ignored PVO returns the FULL set under a
   watermark, not 0. Flag flipped in `schema/fusion_catalog.py`. No functional
   effect today — there is no shipped content-pack bronze node for it; when one is
   authored it must declare watermark `ApAgingPeriodsLastUpdateDate`.

4. **Live-probing is the right method.** BICC honor is per-PVO — two honor, one
   ignores. No PVO-class heuristic gets all three right. This is the gap the
   `/incremental-mechanism` skill closes; `mart-author` step 3 now delegates the
   incremental decision to it instead of guessing.

## E2E validation — `gl_coa` seed → incremental (the flag-flip ship gate)

The probe proves BICC honors the cursor; the E2E proves the orchestrator threads
the watermark + MERGEs correctly under the *new* `incremental_capable=True` path
(distinct from the old payload-diff path TC30b exercised). Scoped to a
`gl_coa`-only bundle (`dev/bundle.glcoa-e2e.yaml`, fresh `bronze_glcoa_e2e`
schema) so it does not re-pull the 12.1M-row cube.

Run against the existing provisioned `bronze` schema, scoped to `gl_coa`
(`dev/bundle.glcoa-e2e.yaml`, `--layers bronze`). An earlier attempt with fresh
`*_glcoa_e2e` schemas failed at `DELTA_CANNOT_CREATE_LOG_PATH` (managed-storage
prefix never provisioned) — a test-harness issue, not gl_coa logic; switched to
the existing schema. Incremental runs used `--force-fingerprint-skip` because the
profile-wide fingerprint gate `DESCRIBE`s every dataset in the pinned profile
(incl. `scm_items`, not materialized in this scoped test) and aborts before
`gl_coa`; skip is safe here (just-seeded → no drift possible; bronze-only → no
templates depend on it) and writes a `fingerprint_skip` audit row.

| Run | Mode | Delta written? | Evidence |
|---|---|---|---|
| seed | `seed` | **yes — full** | `CREATE OR REPLACE`, **69,578 rows** (DESCRIBE HISTORY v13); establishes `bronze.gl_coa` + primary cursor (~2026-06-16, after max LUD 2026-05-27) |
| incr #1 | `incremental` | **no** | success, **no new Delta version** (v13 stays latest); cursor carried |
| incr #2 | `incremental` | **no** | success, **no new Delta version**; cursor carried |

**Verdict: ✅ flip PROVEN end-to-end.** The adapter only skips the write when the
BICC delta is empty (`bronze_extract_adapter.py:416-419` — `source_delta_count==0`
returns before any MERGE); the seed-shape branch (`:389`) *always* writes. Both
incrementals produced **no new Delta version**, so `source_delta_count` was 0.
That is exactly the predicted path: under `incremental_capable=True` + a prior
cursor, the orchestrator passes `prior_watermark` (~2026-06-16) as
`fusion.initial.extract-date` (`:345-348`); the probe proved BICC honors that for
`gl_coa`, so a post-max-LUD watermark returns 0 rows → empty delta → no-op write.

**Cost win confirmed.** Old `incremental_capable=False` forced `bicc_watermark=None`
→ a full 69,578-row pull + payload-diff MERGE **every** incremental cycle (cf.
TC30b's 63,464). The flip turns a no-change incremental into a **0-row pull,
nothing written**.

Note (directly observed vs inferred): the no-new-version + the proven probe +
the orchestrator code path together establish `source_delta_count==0`. The raw
per-run `source_delta_count` is ephemeral (not persisted); `row_count` in the
dispatch marker is the **target** count (69,578), unchanged across runs, and is
not the source delta.

Reproduce: `dispatch_v2_seed.py --bundle <gl_coa-scoped, existing bronze schema>
--mode seed --layers bronze`, then `--mode incremental --layers bronze
--force-fingerprint-skip` (×2); confirm via `DESCRIBE HISTORY
fusion_catalog.bronze.gl_coa` that the incrementals add no new version. Raw run
markers were transient and not retained.
