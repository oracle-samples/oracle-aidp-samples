# TC-E4 — P1.19 xxhash64 surrogate-key stability on `fusion_autopilot_dev`

**Test case ID**: TC-E4 (corresponds to plan item E4 — surrogate-key stability live evidence).
**Stage E plan item**: E4 (`docs/features/p1.17-incremental-merge/plan.md`).
**Status**: ✅ **EXECUTED 2026-06-02** on `fusion_autopilot_dev` cluster.

## What this verifies

P1.19 (bundled with P1.17 per plan §B9) swaps Phase α's `monotonically_increasing_id()` surrogate to `xxhash64(CAST(<natural_key> AS STRING))`. The Phase α surrogate was partition-local and non-deterministic — two CTAS over identical bronze data produced different surrogate values, which would silently invalidate any downstream cache keyed on the surrogate after the first incremental MERGE refresh.

E4 asserts the new contract: two independent builds of `silver.dim_supplier` over the SAME bronze snapshot produce **byte-identical** `supplier_key` values for every supplier.

## Procedure

A custom dispatcher (`/tmp/e4_xxhash_stability_dispatch.py`) generated a self-contained notebook that:

1. Re-ran the orchestrator on `bronze.erp_suppliers` only (`--mode seed`, `--datasets erp_suppliers`, `--layers bronze`) to ensure a fresh bronze under the P1.17 wheel.
2. Called `dim_supplier.build(...)` directly with `silver_table="fusion_catalog.silver.dim_supplier_e4_run1"`.
3. Called `dim_supplier.build(...)` again with `silver_table="fusion_catalog.silver.dim_supplier_e4_run2"` (different target table; same source bronze, same SQL renderer).
4. Joined the two silver tables on `supplier_number` (the natural key, projected from `SEGMENT1`).
5. Counted matching vs mismatching `supplier_key` values across the join.
6. Sampled 5 surrogate pairs to inspect the actual hash values.
7. Dropped the two probe tables (clean-up — no artifacts left on the cluster).

## Live result

```json
{
  "tc": "E4",
  "total_pairs": 209,
  "matching": 209,
  "mismatching": 0,
  "sample_surrogates": [
    {"supplier_number": "1252", "key_run1": -1165185201079306363, "key_run2": -1165185201079306363},
    {"supplier_number": "1253", "key_run1":  4368700969463313237, "key_run2":  4368700969463313237},
    {"supplier_number": "1254", "key_run1":  3731928847013494481, "key_run2":  3731928847013494481},
    {"supplier_number": "1255", "key_run1":  5604204300271517168, "key_run2":  5604204300271517168},
    {"supplier_number": "1256", "key_run1":  6823987153092733692, "key_run2":  6823987153092733692}
  ]
}
```

- **209/209 pairs match byte-for-byte.** Zero mismatches.
- Surrogate values are **deterministic BIGINTs** produced by `xxhash64(CAST(SEGMENT1 AS STRING))`. Both runs produce identical hashes because the input (`SEGMENT1`) is identical and `xxhash64` is a pure deterministic function (unlike `monotonically_increasing_id()` which depends on partition ordering).

## Failure mode this test guards against

A future contributor reverts P1.19 (e.g., switches back to `monotonically_increasing_id()` or some other non-deterministic surrogate). Under that regression:

- `matching` drops from 209 to 0
- `mismatching` jumps from 0 to 209
- The assertion in the test notebook fires (`AssertionError: P1.19 xxhash64 stability FAILED`)
- The AIDP JobRun status becomes `FAILED`

The same surrogate-stability contract is also pinned by the unit test
`tests/unit/test_p117_builder_merge_sql.py::TestSurrogateKeyStabilityShape` — that test asserts the SQL string emits `xxhash64(CAST(SEGMENT1 AS STRING))`, while E4 asserts the SQL **evaluates** to identical values on real Spark + Delta.

## Cross-references

- `docs/features/p1.17-incremental-merge/plan.md` §B9 — P1.19 surrogate-key contract.
- `tests/unit/test_p117_builder_merge_sql.py::TestSurrogateKeyStabilityShape` — unit-level SQL-shape pin.
- `scripts/oracle_ai_data_platform_fusion_autopilot/dimensions/dim_supplier.py` — surrogate SQL line.
- `scripts/oracle_ai_data_platform_fusion_autopilot/dimensions/dim_account.py` — same swap for `dim_account.account_key` (covered transitively because the same SQL renderer pattern is used).

## Dispatch metadata (redacted)

- AIDP wheel: P1.17 head (built fresh on dispatch).
- Cluster: `fusion_autopilot_dev` (ACTIVE state at dispatch time).
- Workspace path: `/Workspace/Shared/p1.17-stage-e/e4_xxhash_stability.ipynb`.
- jobKey / jobRunKey / taskRunKey: operator-redacted.
