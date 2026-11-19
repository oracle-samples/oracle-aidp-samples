# TC33 — Content-pack incremental run (AIDPF-4040 plan-hash fix)

**Test case ID**: TC33
**Status**: ✅ **EXECUTED 2026-06-15** on the `cecl-finance-lake` dev cluster / `dev` env via REST dispatch (auth → wheel build → notebook upload → create job → submit run → poll → SUCCESS).
**Tracks**: first live-green **content-pack `--mode incremental`** run end-to-end; closes the AIDPF-4040 plan-hash-drift defect that made content-pack incremental impossible (only `--mode seed` had ever been live-proven).

## What this verifies

- **AIDPF-4040 plan-hash continuity gate passes on incremental-after-seed.** The
  gate compares the incremental run's content-pack plan-hash to the prior
  successful seed's pinned hash. Before the fix the hashes never matched, so
  4040 fired on *every* content-pack incremental/resume.
- **Replace-strategy gold node merges under incremental.** `ar_invoice_summary`
  (overlay `fusion-finance-ar-ext`, `refresh.incremental.strategy: replace`)
  rebuilds in full each cycle — 49 rows, same as seed.
- **REST dispatch wheel carries the renderer fix.** Fresh wheel built on the
  re-seed (`wheel cache miss`), reused on the incremental (`wheel cache hit`) —
  same artifact, so the cluster ran the fixed `_build_hash_input`.

## Root cause (fixed)

`orchestrator/sql_renderer.py::_build_hash_input` hashed **every** SQL param
*with its value*. Two of those params are **per-run**, not plan-shape:

| Param | Source token | Why it changes every run |
|---|---|---|
| `run_id` | `{{ run_id_literal }}` | the orchestrator run id (audit column) |
| `watermark_<source>` | `{{ watermark_predicate }}` | the delta cursor — advances each cycle |

Including their values made the §11.9 plan-hash **run-dependent**, so the
incremental's hash could never equal the seed's pinned hash → AIDPF-4040.

**Fix**: exclude `run_id` and `watermark_*` param *values* from the hash input.
Their `:markers` still appear in the canonical SQL, so a genuine template /
outputSchema / variation-point / profile change is still caught — only run
identity and the advancing cursor are excluded. Regression tests:
`tests/unit/test_sql_renderer.py::TestHashDeterminism::{test_run_id_value_does_not_shift_hash,
test_watermark_cursor_value_does_not_shift_hash}` (render twice with different
run_ids / cursors → identical plan-hash).

## Live runs (UUIDs redacted)

### 1. Re-seed (re-pin the hash with the fixed renderer)
```
aidp-fusion-autopilot run --mode seed --datasets ar_invoice_summary \
    --layers gold --force-fingerprint-skip
```
- wheel cache miss → `python -m build` (hash `3702fcf1…`) — fix is in this wheel
- job SUCCESS · orchestrator run_id `1473…` (redacted)

| dataset_id | layer | status | row_count | duration_s |
|---|---|---|---|---|
| ar_invoice_summary | gold | SUCCESS | 49 | 43.95 |

### 2. Incremental (the AIDPF-4040 proof)
```
aidp-fusion-autopilot run --mode incremental --datasets ar_invoice_summary \
    --layers gold --force-fingerprint-skip
```
- wheel cache **hit** `3702fcf1…` (same fixed artifact)
- job SUCCESS · orchestrator run_id `ee04…` (redacted) — **no AIDPF-4040**

| dataset_id | layer | status | row_count | duration_s |
|---|---|---|---|---|
| ar_invoice_summary | gold | SUCCESS | 49 | 11.62 |

`1 success · 0 failed · 0 skipped · 0 deferred`.

## Notes / scope

- `--force-fingerprint-skip` used because the dev bundle includes datasets whose
  bronze tables aren't all materialized on this pod (break-glass, dev only —
  never in production; route real drift to `/fusion-drift-doctor`).
- `ar_invoice_summary` is **replace-strategy**, so this proves the plan-hash gate
  for replace marts (mode-independent SQL — only `run_id` differed seed↔incr).
  **Row-grain MERGE nodes** (silver dims, `gl_balance`) additionally render a
  *different SQL body* on incremental (`<col> > :watermark_src`) vs seed (`1=1`),
  so their seed↔incremental plan-hash still differs on the SQL text — a separate,
  larger fix (hash the template, not the per-mode rendered SQL). Tracked as a
  follow-up; this TC does not claim MERGE-node incremental is live-green.
