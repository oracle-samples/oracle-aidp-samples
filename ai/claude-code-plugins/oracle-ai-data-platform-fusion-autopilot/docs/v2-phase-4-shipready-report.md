# Phase 4 ship-ready report — dual-runner parity gate

> Historical report - not part of the current workflow. For current setup and
> operator guidance, use [project_setup.md](project_setup.md) and
> [../workflow.md](../workflow.md).

Phase 5 reads this document when deciding whether to flip the default
``--execution-backend`` to ``content-pack``. One row per PLAN §15 Phase 4
exit-criteria item; each row carries STATUS / EVIDENCE / BLOCKS_PHASE_5.

## Legend

| STATUS | Meaning |
|---|---|
| `PASS` | Test exists, runs green; invariant proven. |
| `EXPLAINED-DIVERGENCE` | Backends differ intentionally; divergence documented + not gating. |
| `OPERATOR-PENDING` | Code shipped (test or dispatcher); evidence requires an operator-driven live run. |
| `FAIL` | Test exists and is RED, OR a contracted invariant is unmet. |

`BLOCKS_PHASE_5: true` → Phase 5 cannot flip the default until the row
clears. `false` → informational only.

## PLAN §15 Phase 4 exit criteria

**Honesty rule for STATUS values**:
- `SCAFFOLD-COMMITTED` — assertion code shipped and imports / collects cleanly, but the harness has NOT been executed end-to-end green against any environment. Cannot be cited as parity proof until row's host test actually runs PASS.
- `PASS` — assertion code shipped AND a real test run has been observed green; the evidence pointer references a passing pytest invocation or committed live-evidence file.
- `OPERATOR-PENDING` — code shipped, but evidence requires an operator-driven action (live cluster dispatch, post-merge regression run on the parity-marker suite).
- `FAIL` — known broken; gating.
- `EXPLAINED-DIVERGENCE` — intentional cross-backend gap, codified in code + tests; non-gating.

Rows that depend on the dual-runner harness actually running green are SCAFFOLD-COMMITTED until **P4-L2 (runtime fixes)** lands. P4-L2 details at the bottom of this report.

| # | Item | STATUS | EVIDENCE | BLOCKS_PHASE_5 |
|---|---|---|---|---|
| 1 | `tests/parity/test_dual_runner_e2e.py` committed; both backends through `orchestrator.run` end-to-end | SCAFFOLD-COMMITTED | `tests/parity/test_dual_runner_e2e.py::TestStep2_SeedModeParity`. Collects 20 tests cleanly via `pytest --collect-only -m parity`. Local-mode execution surfaces P4-L2 runtime bugs (orchestrator Delta-CTAS path drops `:run_id` binding; v1 gold ap_aging reads target before creation). | **true** (P4-L2 must land before this can flip to PASS) |
| 2 | Per-node seed-mode parity passes for all 6 nodes | SCAFFOLD-COMMITTED | `TestStep2_SeedModeParity::test_state_row_equiv` + `::test_materialized_rows_equiv` (parametrized over `EXPECTED_SEED_NODES`). Test code present + collects; assertions have not yet run green against any environment because P4-L2 blocks execution. | **true** (paired with row 1) |
| 3 | Per-node `fusion_autopilot_state` equivalence via three-tier contract | SCAFFOLD-COMMITTED | `tests/parity/dual_runner_helpers.py::assert_state_rows_equiv` (Tier A semantic / Tier B watermark cross-shape / Tier C v2-only fields). Helper logic is implemented; not yet exercised against a green dual-runner run. | **true** (paired with row 1) |
| 4 | `assert_run_summary_equiv` confirms `RunSummary` shape matches | SCAFFOLD-COMMITTED | `TestStep2_SeedModeParity::test_run_summary_equivalence`. Helper code in `dual_runner_helpers.py::assert_run_summary_equiv`. Not yet exercised against green runs. | **true** (paired with row 1) |
| 5 | Incremental mode parity passes | SCAFFOLD-COMMITTED | `TestStep3_IncrementalParity::test_incremental_advances_watermark_and_preserves_plan_hash`. Code present; depends on seed-mode running first → blocked by P4-L2. | **true** (paired with row 1) |
| 6 | Cascade-abort parity passes | SCAFFOLD-COMMITTED | `TestStep4_CascadeAbort::test_v2_cascade_only_on_dependents` + `::test_v1_abort_after_first_failure`. Code present; depends on backend run loops completing → blocked by P4-L2. | **true** (paired with row 1) |
| 7 | Resume parity passes v1; xfails v2 with AIDPF-1032 reason | SCAFFOLD-COMMITTED | `TestStep5_Resume::test_v1_resume_reattempts_non_success_nodes` (legacy leg) + `::test_v2_resume_currently_rejected` (xfail-strict). v1 leg also depends on the legacy harness running green (P4-L2). | **true** (AIDPF-1032 + P4-L2 both must clear) |
| 8 | Multi-tenant: `finance-alt-cancelled-flag.yaml` + bronze fixtures + paired snapshot | PASS | `tests/parity/fixtures/profiles/finance-alt-cancelled-flag.yaml`, `tests/parity/fixtures/profiles/finance-alt-cancelled-flag.schema-snapshot.yaml`, `tests/parity/bronze_fixtures_tenant_b.py`. Files exist + load via `load_tenant_profile`. | false |
| 9 | `test_dual_runner_profiles.py` passes both profiles through both backends | SCAFFOLD-COMMITTED | `tests/parity/test_dual_runner_profiles.py::TestStep6_MultiTenantParity` (parametrized over `finance-default` + `finance-alt-cancelled-flag`). Same P4-L2 dependency as the foundation harness. | **true** (paired with row 1) |
| 10 | Preflight gates covered (5 gates) — BEHAVIORAL | SCAFFOLD-COMMITTED (structural) | `tests/unit/test_v2_preflight_gates.py` currently exercises structural shape (exception classes, error-code constants, `NodeExecutionResult` literals, snapshot path resolution, fingerprint outcome enum on seed-mode + placeholder-fingerprint paths) — NOT behavioral end-to-end coverage. Reviewer Round-1 finding: structural tests can pass while the production gates regress. Phase 4.1 must add **behavioral** tests that invoke production gate entry points with real failing inputs (fingerprint mismatch → exit 14 + diagnostic + no-state-write, `--force-fingerprint-skip` audit row, profile-hash drift from prior state row, schema-snapshot drift through the production preflight loader, missing-cursor incremental dispatch). Until those land, this row stays SCAFFOLD-COMMITTED, not PASS. | **true** (behavioral coverage must land before Phase 5 flip) |
| 11 | Per-gate behaviour table | PASS | `docs/v2-phase-4-preflight-coverage.md` — documents per-gate v1↔v2 surface map. Doc-only, no test dependency. | false |
| 12 | Hard cursor commit failure (Step 7a) | SCAFFOLD-COMMITTED | `TestStep7a_HardCursorCommitFailure::test_state_commit_failure_blocks_cursor_advance`. Code present (Direct injection pattern; the assertion chain asserts §11.9 atomic-commit invariant + no spurious advance + retry advances correctly). Same P4-L2 dependency. | **true** (paired with row 1) |
| **13** | **Source-level cursor rows (§11.10 primary/lookup)** | SCAFFOLD-COMMITTED | `TestStep2_SeedModeParity::test_gl_balance_multi_source_cursor_policy` asserts `source_role='lookup'` row for `dim_account` on the `gl_balance` node + `output_watermark=NULL`. Test code present; not yet exercised against a green run (P4-L2). | **true** (paired with row 1) |
| **14** | **Hard cursor commit failure (§11.9 atomic-commit)** | SCAFFOLD-COMMITTED | Same as row 12. v1 leg is EXPLAINED-DIVERGENCE (no equivalent §11.9 boundary). | **true** (paired with row 1) |
| **15** | **Multi-source cursor policy (v0.3 primary/lookup contract)** | SCAFFOLD-COMMITTED | `TestStep2_SeedModeParity::test_gl_balance_multi_source_cursor_policy`. Same P4-L2 dependency. | **true** (paired with row 1) |
| 16 | Live tenant evidence on demo-pod (v1 + v2 + A/B + incremental) | OPERATOR-PENDING | Dispatcher at `tests/live/dispatch_v2_seed.py` is parametrized (no hardcoded OCIDs) and stages the Phase 3d snapshot alongside the profile. **`--ab` mode is now A/B-safe**: requires `--v1-bundle` and `--v2-bundle` flags with distinct `aidp.bronzeSchema` / `silverSchema` / `goldSchema` per backend; refuses to run with one shared bundle (Round-2 reviewer finding). Round-2 also requires the operator to commit `TC<N>_v1_seed.md` + `TC<N>_v2_seed.md` + `TC<N>_v2_vs_v1_parity.md` (+ incremental variants) — the dispatcher emits a structured per-backend JSON marker; the operator pastes it into the markdown template alongside row-count / schema / `xxhash64_agg` checksum / audit-column presence verification. | **true** |
| 17 | A/B uses isolated schemas + shared bronze snapshot | OPERATOR-PENDING | Same as row 16. The dispatcher enforces the `--v1-bundle ≠ --v2-bundle` precondition; operators are still responsible for the shared frozen bronze-snapshot copy (one-shot bronze extract into `bronze_live_snapshot`, then `CREATE TABLE bronze_v{1,2}.<id> AS SELECT * FROM bronze_live_snapshot.<id>` for each dataset). Documented in the dispatcher's runbook section. | **true** (paired with row 16) |
| 18 | Live materialized-output parity (row count + schema + checksum + audit presence) | OPERATOR-PENDING | Captured in the A/B markdown files. The checksum projection (`xxhash64_agg` over non-audit columns) is documented in `plan.md` Step 8 + operator commits verbatim. | **true** (paired with row 16) |
| 19 | Concurrent-runs precheck — documents Phase γ behaviour | SCAFFOLD-COMMITTED | `tests/parity/test_concurrent_runs.py::TestStep9_ConcurrentRunsBehaviour::test_two_concurrent_seeds_observed_behaviour`. Code present; collects cleanly. The host test depends on a clean dual-runner foundation (it invokes `orchestrator.run` twice in threads) → blocked by P4-L2. P4-L1 LIMITS row (Phase γ behaviour) authored after the first green run. | **true** (paired with row 1) |
| 20 | Documentation updates (CLAUDE.md / content_pack_execution.md / PLAN §15 / §25) | PASS (partial) | `CLAUDE.md` v1+v2 coexistence section refreshed; `docs/content_pack_execution.md` Phase 4 subsection appended. PLAN.md §15 Phase 4 checkboxes will be ticked by Phase 5's default-flip PR (it consumes this report). Any new error codes — NONE registered by Phase 4; AIDPF-2012 (Phase 3c), AIDPF-4040/4060/4070 (Phase 2) are the only ones asserted. | false |
| 21 | All Phase 1 + Phase 2 + Phase 3 tests still pass (1290 test floor) | PASS | `.venv/bin/python -m pytest tests/unit/` → 1665 pass + 1 pre-existing red on `test_pyspark_unavailable_falls_back` (unchanged baseline from Phase 3d main). New: 9 preflight gate tests pass. 29 parity tests collect cleanly under `pytest --collect-only -m parity`. | false |

## EXPLAINED-DIVERGENCE rows (documented, non-gating)

The following cross-backend differences are codified in tests +
documentation and intentionally NOT asserted as parity failures:

| Divergence | Where | Justification |
|---|---|---|
| Cascade-abort: v1 sweeps ALL plan nodes (`_abort_remaining` → `skipped_aborted`); v2 cascade-only-on-dependents | `TestStep4_CascadeAbort` asserts each side's actual behaviour | v1 contract pre-dates Phase 2; harmonization would change v1's audit completeness. Phase 5 decides: port v1 semantics onto v2 OR bless cascade-only-on-dependents as the v2 contract + update v1 docs. |
| Plan-hash inputs (v1 uses `compute_legacy_python_plan_hash`; v2 uses `compute_content_pack_plan_hash`) | `plan_hash` column excluded from `assert_state_rows_equiv` Tier A; same-backend stability checked in Step 3 | The two backends hash semantically different inputs (Python module shape vs rendered SQL + profile). Cross-backend equality is not a meaningful invariant. |
| Fingerprint gate (Phase 3c, `AIDPF-2012`): v2 raises and exits 14; v1 has no gate | `TestGate2_TenantFingerprint::test_legacy_placeholder_fingerprint_warns_and_proceeds` + `TestGate6_LegacyHasNoFingerprintGate` | Gate is a v2-only contract. Pre-existing v1 deployments would break under a hard fingerprint gate; introducing one would require a separate v1 migration feature. |
| Hard cursor commit (§11.9, `AIDPF-4060`): v2 catches `StateCommitError` → `state_commit_failed`; v1 has no equivalent boundary | `TestStep7a_HardCursorCommitFailure` v2-only + plan §11.9 documented; v1 commits state inline in its per-node loop | Phase 5 inherits this if it wants symmetric §11.9 semantics. |
| Non-conventional COA segment positioning NOT validated | Step 6 / `docs/v2-phase-4-multi-tenant-coverage.md` | v0.3 pack vocabulary cannot express the override; awaits new `{{ coa.<role> }}` renderer tokens + live evidence in a real non-conventional tenant. Phase 5 prerequisite. |
| Resume on content-pack returns AIDPF-1032 | `TestStep5_Resume::test_v2_resume_currently_rejected` (xfail-strict) | Phase 2 deferral. Phase 5 prerequisite (row 7). |

## Phase 5 prerequisites (gating)

Items the Phase 5 default-flip PR cannot land without:

1. **P4-L2 — runtime fixes in the orchestrator** so the dual-runner harness actually runs green end-to-end. Affects rows 1, 2, 3, 4, 5, 6, 7, 9, 12, 13, 14, 15, 19. See P4-L2 details below.
2. **Row 10 — behavioral preflight tests** (Phase 4.1 follow-up). Phase 4 ships structural shape checks only; behavioral coverage of the actual production gate entrypoints is required before Phase 5 can claim "preflight gates verified".
3. **Row 7** — AIDPF-1032 resolved on the content-pack backend, OR the resume xfail explicitly accepted as a permanent capability gap with a documented operator workaround.
4. **Row 16** — Live A/B evidence captured on at least the `demo-pod` demo pod (rows 16/17/18 are paired).
5. **STRETCH** — Live evidence on at least one non-`demo-pod` tenant per §13.3 "plugin-portability evidence on at least one non-demo pod". If access is not available by Phase 5 cutover, this becomes a documented limit Phase 5 inherits.

## P4-L<n> LIMITS entries

To be authored once their host tests run end-to-end locally; placeholders:

- **`P4-L1`** — concurrent-runs observed behaviour (Step 9): captures whether Delta `ConcurrentAppendException` fires, whether state-row interleaving leaves a coherent terminal view, and what operator-discipline guidance lands until Phase γ ships locking. Source: `tests/parity/test_concurrent_runs.py` observed output.
- **`P4-L2` — local-mode parity execution requires runtime fixes**. The dual-runner harness boots Spark + Delta cleanly, loads both bundles, seeds bronze across both isolated schemas, and dispatches `orchestrator.run` for both backends. Two downstream issues surface during the actual node dispatches against the synthetic fixture: (1) v1 backend's `ap_aging` mart references `gold_<suffix>.ap_aging` before the table exists (likely a dispatch-ordering bug in the legacy gold loop on Delta-local-mode warehouses; production runs against Delta on cluster don't hit this); (2) v2 backend's `dim_account` CTAS raises `UNBOUND_SQL_PARAMETER: run_id` — the rendered SQL carries `:run_id` but `strategy_executors.execute_strategy` doesn't bind it on the Delta CTAS path (Phase 3's direct-SQL harness binds via `spark.sql(ctas, args=params)`). Both issues are downstream of Phase 4's gate; Phase 4 commits the harness scaffold + test code; **Phase 4.1 (separate ticket) ships the runtime fixes that make the assertions actually run green**. Live cluster execution via the `tests/live/dispatch_v2_seed.py` dispatcher path is unaffected (cluster-side Delta + the v2 backend's full Job/Run flow bind params correctly per the TC29 evidence).

## Sign-off cadence

Phase 5's default-flip PR must reference this report + cite the resolution of each `BLOCKS_PHASE_5: true` row. If a `true` row becomes a permanent `EXPLAINED-DIVERGENCE` (no fix planned), Phase 5 must update this report with the new STATUS before merging.
