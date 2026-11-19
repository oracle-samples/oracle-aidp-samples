# TC34 — MERGE-node content-pack incremental (mode-normalized plan-hash)

**Test case ID**: TC34
**Status**: ✅ **EXECUTED + GREEN 2026-06-16** on the dev cluster / `dev` env via
REST dispatch. Seed → incremental → incremental on the row-grain MERGE node
`gl_balance`, all SUCCESS, **no AIDPF-4040** on either incremental, gold node
executed via **MERGE** (not replace).
**Tracks**: closes `LIMITS.md` **P-incr-L1** (mode-normalized plan-hash /
Approach 3) — first live-green **MERGE-node** `--mode incremental`. Sibling to
TC33 (which proved the *replace*-strategy incremental on `ar_invoice_summary`).

## What this verifies

- **AIDPF-4040 passes on a MERGE node's first incremental-after-seed.** Before
  the fix, `{{ watermark_predicate }}` rendered `1=1` on seed vs
  `<col> > :watermark` on incremental, so the seed-pinned plan-hash and the
  incremental's diverged on SQL text and the continuity gate false-tripped. The
  mode-normalized hash (Approach 3) makes them equal; the incremental sails past
  the gate into execution.
- **Gold node executes a true delta-MERGE, not a rebuild.** `gl_balance`
  incremental ran ~3× the seed's wall time (207s / 256s vs 73s) — the signature
  of `MERGE INTO` over 10.18M rows vs `CREATE OR REPLACE`.
- **Steady-state holds.** Incremental #2 compared against incremental #1's pin
  (not the seed) and stayed clean — repeated incrementals don't drift.

## Live runs (run_ids truncated)

Wheel `a5ac395…` (carries both the plan-hash fix and the MERGE-executor fix
below). Cluster ACTIVE; `--force-fingerprint-skip` (dev bundle includes datasets
whose bronze isn't all materialized on this pod — break-glass, dev only).

| Step | run_id | dataset | layer | status | rows | strategy | duration |
|---|---|---|---|---|---|---|---|
| Seed | `e1aa1c32…` | dim_account | silver | SUCCESS | 63,464 | replace | 43.5s |
| Seed | `e1aa1c32…` | gl_balance | gold | SUCCESS | 10,184,102 | replace | 73.7s |
| Inc #1 | `6e062821…` | dim_account | silver | SUCCESS | 63,464 | merge | 31.3s |
| Inc #1 | `6e062821…` | gl_balance | gold | SUCCESS | 10,184,102 | **merge** | 207.8s |
| Inc #2 | `869c320a…` | dim_account | silver | SUCCESS | 63,464 | merge | 54.7s |
| Inc #2 | `869c320a…` | gl_balance | gold | SUCCESS | 10,184,102 | **merge** | 256.6s |

Each run: `2 success · 0 failed · 0 skipped · 0 deferred`. Row count is stable at
10,184,102 across all three — the GL balances cube doesn't grow between no-change
cycles (bronze `gl_period_balances` re-extracts full and dedupes on the natural
key; the gold MERGE re-applies the same key set). Correct.

## Bug found + fixed during this test (the live run earned its keep)

The first incremental attempt **passed AIDPF-4040** (plan-hash fix confirmed)
but then failed with:

```
TypeError: _ensure_target_schema_for_merge() got an unexpected keyword
argument 'target_table'
```

The silver/gold MERGE strategy executor
(`orchestrator/strategy_executors.py`) called the schema-reconcile helper with
made-up keyword names (`target_table=`, `source_df=`) instead of the real
signature (`target=`, `source_columns=`, `source_schema_struct=`). **Latent**
defect: every merge *unit* test used the empty-delta early-return path, which
returns before the reconcile call, so the non-empty MERGE path had never
executed until this live run.

**Fix:** corrected the call to match the real signature (mirrors the working
bronze caller `bronze_extract_adapter.py:412`). **Regression guard:** new
`tests/unit/test_strategy_executors.py::TestExecuteMergeNonEmptyDelta` exercises
the non-empty path and binds the reconcile call against the real
`state._ensure_target_schema_for_merge` signature, so a wrong-keyword call now
fails in unit tests, not just live.

## Notes / scope

- Run UUIDs truncated; pod URL / OCIDs / storage-profile name omitted per the
  repo redaction rule.
- `gl_period_balances` still full-re-extracts each cycle (`P1.17-L2`,
  `incremental_capable=False`) — the gold MERGE on top is still a correct delta;
  the extract cost is the subject of the `bicc-period-window-extract` feature.
