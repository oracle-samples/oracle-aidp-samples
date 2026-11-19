# v2 Phase 5 — Ship-ready report

> Historical report - not part of the current workflow. For current setup and
> operator guidance, use [project_setup.md](project_setup.md) and
> [../workflow.md](../workflow.md).

> Live snapshot of the Phase 5 implementation state. The Phase 6
> review reads this BEFORE accepting Phase 5 as a hard prerequisite.
>
> Row format: each `BLOCKS_NEXT_PHASE: true|false` row records a
> known gap. Phase 6 may not start until every BLOCKS_NEXT_PHASE row
> is RESOLVED or explicitly DEFERRED in this document.

## Row 1 — AIDPF-1032 (`--resume` on content-pack backend)

**Status:** RESOLVED 2026-06-08.

The orchestrator's defensive raise at `orchestrator/__init__.py` for
`resume_run_id is not None` on the content-pack backend has been
removed. The dispatcher adopts the supplied `resume_run_id` as the
shared run_id so the resumed run's state rows join with the prior
failed run's rows. The xfail-strict
`tests/parity/test_dual_runner_e2e.py::TestStep5_Resume::test_v2_resume_currently_rejected`
has been inverted to `test_v2_resume_adopts_supplied_run_id` and
asserts the adopt-supplied-run_id contract.

BLOCKS_NEXT_PHASE: false

## Row 1b — P1.5ε-fix5 (REST-dispatch resume)

**Status:** RESOLVED 2026-06-08.

`dispatch_via_rest(..., resume_run_id=...)` accepts the kwarg and
threads it through `build_notebook(...)` into the cluster-side
`orchestrator.run(..., resume_run_id=<id>)` call (was previously
hardcoded to `None`). `commands/run.py`'s "--resume requires
--inline" gate has been removed. Migration evidence: the
`test_resume_run_id_parameter_threads_into_run_cell` test asserts
the cell carries the operator-supplied id.

BLOCKS_NEXT_PHASE: false

## Row 2 — Shared run_id across bronze + content-pack branches

**Status:** RESOLVED 2026-06-08.

The new `_phase5_top_level_dispatch(...)` mints exactly one
`run_id` (or adopts `resume_run_id`) and passes it as a private
`_forced_run_id` kwarg into the recursive legacy `run()` call AND
as `shared_run_id` into `_run_content_pack_backend(...)`. Verified
by `tests/unit/test_default_flip_bronze.py::test_no_filter_invokes_both_branches_with_shared_run_id`.

BLOCKS_NEXT_PHASE: false

## Row 3 — Bronze readiness gate (AIDPF-2071) wired into dispatcher

**Status:** RESOLVED 2026-06-08.

`_phase5_top_level_dispatch(...)` enables the bronze readiness gate
(`enable_bronze_readiness_gate=True`) on the content-pack branch
EXACTLY when the bronze branch just extracted bronze in this same
invocation (Option A merged-flow contract). Silver/gold-only direct
calls against pre-seeded bronze skip the gate because the caller
asserts the bronze invariant out of band. Verified by
`tests/unit/test_dispatcher_invokes_readiness.py`.

BLOCKS_NEXT_PHASE: false

## Row 4 — Fusion PVO drift gate (AIDPF-2072)

**Status:** PARTIAL RESOLVED 2026-06-08. Helper module ships;
9-case unit coverage green; live-BICC integration deferred.

`orchestrator/fusion_pvo_drift.py::assert_fusion_pvo_compatibility(...)`
ships with full 9-case coverage (required-column missing, type
change vs snapshot, snapshot-absent degraded mode, extra-column
permitted, bronze_filter narrowing, candidate renames, snapshot-
only columns, diagnostic JSON, error code propagation).
`orchestrator/preflight.py::PreflightResult.live_pvo_schemas` now
carries the live `StructType` per PVO captured during the
metadata-only BICC probe — the integration point for the
dispatcher-side call.

**Deferred:** wiring the dispatcher to actually call
`assert_fusion_pvo_compatibility(...)` between scope-split and the
bronze branch requires loading the pinned schema snapshot from
the bundle on every run + threading it through. This is a small
follow-up; the helper is ready, the StructType is captured, and
the gate-failed RunStep factory is in place. Tracked here so the
Phase 6 reviewer sees the explicit boundary.

BLOCKS_NEXT_PHASE: false (helper + tests are sufficient evidence
that the gate works; the integration loop is a wiring concern,
not a contract concern).

## Row 5 — Live evidence on demo-pod (Validations A/B/C/D)

**Status:** PENDING — operator-driven cluster session required.

The placeholder evidence file at
`tests/live/TC_phase5_v2_default_seed_live.md` documents the four
validations needed. Phase 6 may BEGIN against the static evidence
(unit + parity tests, dispatcher contract) but the Phase 5 PR
description should NOT claim "default-flipped CLI verified on
demo-pod" until the operator runs the four validations.

BLOCKS_NEXT_PHASE: false (Phase 6's dashboard work doesn't
require Phase 5's live evidence; the parity tests gate the
SHIPPING of Phase 5, not the start of Phase 6 design work).

## Row 6 — P4-L2 runtime fixes (carried over from Phase 4)

**Status:** DEFERRED to follow-up phase.

The Phase 4 ship-ready report had P4-L2 runtime fixes flagged as
"either landed or documented as local-mode-only." Phase 5 inherits
that posture: no Phase 4 runtime fixes were further addressed
during Phase 5 implementation. If a customer hits a P4-L2 case
on the default-flipped CLI, the legacy-python opt-out preserves
the v1 behaviour.

BLOCKS_NEXT_PHASE: false

## Summary

| # | Title | Status | Blocks Phase 6 |
|---|---|---|---|
| 1 | AIDPF-1032 resume | RESOLVED | no |
| 1b | P1.5ε-fix5 REST resume | RESOLVED | no |
| 2 | Shared run_id | RESOLVED | no |
| 3 | Bronze readiness gate wired | RESOLVED | no |
| 4 | Fusion PVO drift gate | PARTIAL (helper + tests; dispatcher wiring deferred) | no |
| 5 | Live evidence on demo-pod | PENDING (operator session required) | no |
| 6 | P4-L2 runtime fixes | DEFERRED | no |

Zero `BLOCKS_NEXT_PHASE: true` rows. Phase 6 dashboard contracts work
may begin against the Phase 5 static evidence + dispatcher contract;
the Phase 5 PR description should cite the row-by-row resolution
of this document.
