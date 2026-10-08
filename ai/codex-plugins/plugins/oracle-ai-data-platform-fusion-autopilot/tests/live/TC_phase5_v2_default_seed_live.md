# TC_phase5_v2_default_seed_live — Live evidence trail (Phase 5)

**Status:** PENDING — operator-driven cluster session required.

## Purpose

Step 10 of the Phase 5 plan. Validates that the default-flipped
`aidp-fusion-autopilot run --mode seed` (no explicit `--execution-backend`)
produces the same output on demo-pod that the explicit Phase-4
`--execution-backend content-pack` run produced (TC_phase4_v2_seed_live).

Four validations gated by this evidence trail:

| Validation | What it proves |
|---|---|
| A — fresh-tenant default seed | The flipped default path runs end-to-end without an explicit `--execution-backend` flag AND extracts bronze first (Step 2b Option A scope split). |
| B — stale-bronze guard | When bronze tables exist but a required column is missing, `AIDPF-2071` fires BEFORE any silver/gold node runs. |
| C — `--resume <run_id>` | Step 9b: re-running with `--resume <run_id>` adopts the supplied id, joins with the prior run's state rows, and idempotently re-attempts non-success nodes. |
| D — `--layers silver,gold` against pre-seeded bronze | Today's content-pack behavior (silver/gold against pre-seeded bronze) is preserved as an opt-in path even after the default flip. |

## Procedure (to be executed by operator)

1. Run validation A from a fresh tenant pod:
   ```
   aidp-fusion-autopilot run --mode seed
   ```
   Capture: `RunSummary.run_id`, per-step `dataset_id` + `row_count`,
   bronze + silver + gold audit columns all bearing the same `run_id`.
2. Run validation B: drop a required column from one bronze table via
   a tampered profile, re-invoke; capture the AIDPF-2071 diagnostic
   JSON.
3. Run validation C: take the failed run_id from B, fix the column,
   re-invoke with `--resume <id>`; capture `RunSummary.run_id ==
   <id>`.
4. Run validation D: `--layers silver,gold` against the pre-seeded
   bronze from A; capture green completion + zero bronze RunSteps.

## Expected outputs (from Phase 4 baseline)

| Node | Layer | Expected rows |
|---|---|---|
| dim_supplier | silver | 209 |
| dim_account | silver | 63,464 |
| dim_calendar | silver | 4,018 |
| ap_aging | gold | 131 |
| gl_balance | gold | 10,184,102 |
| supplier_spend | gold | 309 |

## Capture location

Live-evidence JSONs land under `tests/live/phase5_dispatch_*.json`
following the Phase 4 dispatcher conventions. Final evidence summary
is committed to this file once the operator runs the four validations.
