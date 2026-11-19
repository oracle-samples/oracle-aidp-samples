# Phase 4 preflight + drift-gate coverage

> Historical report - not part of the current workflow. For current setup and
> operator guidance, use [project_setup.md](project_setup.md) and
> [../workflow.md](../workflow.md).

Per-gate behaviour matrix. Recorded so Phase 5's default-flip PR can
trace each preflight surface to a Phase 4 test + the corresponding v1
behaviour.

| Gate | Error code / class | v1 behaviour | v2 behaviour | Divergence | Phase 4 test |
|---|---|---|---|---|---|
| **Dropped target** (P1.17c) | `IncrementalTargetMissingError` (OrchestratorConfigError) | Raises — same code path | Raises — same code path | None | `TestGate1_DroppedTarget::test_exception_class_carries_expected_attrs` |
| **Tenant fingerprint** (Phase 3c) | `AIDPF-2012` + diagnostic JSON + exit 14 | NO GATE — runs incremental against any bronze | Gate fires on mismatch; `--force-fingerprint-skip` writes audit row | **EXPLAINED-DIVERGENCE** — v2-only gate; pre-existing v1 deployments would break with a hard v1 gate | `TestGate2_TenantFingerprint::test_seed_mode_skips_gate` + `::test_legacy_placeholder_fingerprint_warns_and_proceeds` + `TestGate6_LegacyHasNoFingerprintGate::test_legacy_backend_does_not_import_fingerprint_check` |
| **Profile-hash drift** | `AIDPF-4040` returned in `NodeExecutionResult(status='resume_drift_blocked', ...)` (NOT raised) | NO GATE — v1 has no plan-hash concept | Returns state row + `resume_drift_blocked` status | EXPLAINED-DIVERGENCE — same family as fingerprint gate | `TestGate3_ProfileHashDrift::test_error_constant_format` + `::test_node_execution_result_status_enum_includes_drift_status` |
| **Schema drift** (Phase 3d `datasetDeltas`) | `AIDPF-2012` + populated `datasetDeltas` (`addedColumns` / `removedColumns` / `typeChangedColumns`) | NO GATE — type changes propagate as SQL-level coercion failures or silent success | Gate fires + populated deltas (snapshot present), warn-and-proceed + empty deltas (snapshot absent/desynced/unparseable) | EXPLAINED-DIVERGENCE | `TestGate4_SchemaDrift::test_added_and_removed_and_type_changed_columns` + `::test_snapshot_missing_is_graceful_degrade_not_failure` |
| **Missing cursor** | `IncrementalCursorMissingError` (OrchestratorConfigError) | Raises | Raises | None | `TestGate5_MissingCursor::test_exception_class_present_and_orchestrator_config_error` |
| **Hard commit failure** (§11.9, Step 7a) | `state_phase2.StateCommitError(AIDPF-4060)` → `NodeExecutionResult(status='state_commit_failed', ...)` | NO BOUNDARY — v1 commits state inline in per-node loop | Catch at `sql_runner.py:346` converts to `state_commit_failed` + prior cursor remains authoritative + retry advances correctly | EXPLAINED-DIVERGENCE | `TestStep7a_HardCursorCommitFailure::test_state_commit_failure_blocks_cursor_advance` |

## Sources

- `scripts/oracle_ai_data_platform_fusion_autopilot/orchestrator/errors.py` — Exception classes
- `scripts/oracle_ai_data_platform_fusion_autopilot/orchestrator/preflight.py:473` — `IncrementalTargetMissingError` raise site
- `scripts/oracle_ai_data_platform_fusion_autopilot/orchestrator/preflight_evidence.py:114` — `check_bronze_fingerprint_drift` (Phase 3c)
- `scripts/oracle_ai_data_platform_fusion_autopilot/orchestrator/sql_runner.py:258` — Profile-hash drift return
- `scripts/oracle_ai_data_platform_fusion_autopilot/orchestrator/sql_runner.py:346` — `StateCommitError` catch (Step 7a)
- `scripts/oracle_ai_data_platform_fusion_autopilot/schema/bronze_schema_snapshot.py` — Phase 3d snapshot loader
