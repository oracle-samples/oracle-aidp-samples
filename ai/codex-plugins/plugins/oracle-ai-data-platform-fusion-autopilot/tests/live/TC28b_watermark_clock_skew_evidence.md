# TC28b — AIDP-vs-Fusion clock-skew probe (β.1 safety-window justification)

**Test case ID**: TC28b
**Status**: 🟡 **PENDING** — depends on a working BICC extract path on the test tenant; same operational blocker as TC28 bronze evidence.
**Tracks**: P1.5β.1 Stage E2 acceptance criterion from `docs/features/p1.5b-orchestrator-incremental/plan.md`.

## What this verifies

The β.1 watermark capture uses a hardcoded constant `WATERMARK_SAFETY_WINDOW = timedelta(hours=1)`. This probe documents that the chosen window is wider than the observed AIDP-vs-Fusion wall-clock skew on the test tenant — without that evidence, a wrong choice of window could permanently skip rows on the next P1.17-enabled incremental run.

The probe captures two timestamps within the same operator session:

1. **AIDP-side wall clock** — `datetime.now(timezone.utc)` on the AIDP Spark cluster.
2. **Fusion-side wall clock** — captured from Fusion using a deterministic fallback ladder (per the plan's A2 decision gate):
   1. **Primary** — BICC small-PVO extract metadata field (`lastExtractEndDate` / `extractDate`).
   2. **Fallback** — OAC SQL `SELECT SYSTIMESTAMP FROM DUAL`.
   3. If neither succeeds, document the failure mode in this file and add a LIMITS entry — the tenant CANNOT enable `--mode incremental` in P1.17 until a clock-skew probe path is established.

The acceptance assertion: `abs(aidp_now - fusion_now) < WATERMARK_SAFETY_WINDOW`.

## Why TC28b is pending β.1

The β.1 plan defers this evidence to "first non-demo tenant onboarding for P1.17 enablement" — it is **one-time evidence per tenant**, not a recurring test or CI gate. On the test cluster + Fusion source used for TC28's seed-run evidence (operator-redacted pod), the BICC reader layer hits an `uncategorized BICC reader failure` on `.load()` AFTER credential auth succeeds. The primary ladder rung (BICC metadata) needs a working BICC extract on at least one PVO; until that operational blocker is resolved this probe cannot run on this tenant.

The fallback OAC SYSTIMESTAMP path is also unavailable here because the OAC connection on this tenant is the Phase α one (configured for the `dashboard install` workflow) and is not currently wired to execute arbitrary SQL via the OAC REST endpoint from a dispatch session.

## What β.1 ships WITHOUT TC28b

The `WATERMARK_SAFETY_WINDOW = timedelta(hours=1)` constant is a defensive default chosen wider than typical NTP-synced drift between OCI-hosted AIDP and Fusion Cloud (sub-second to sub-minute on a healthy tenant). The unit tests pin the gap-invariant contract (`_extract_ts - last_watermark == WATERMARK_SAFETY_WINDOW` exactly under the default) so any future change to the constant is exercised end-to-end without needing a fresh live probe.

The cursor captured by β.1 is **NOT consumed by `extract_pvo`** in this PR — the `NotImplementedError` gate at `__init__.py:641-645` stays. P1.17 is the PR that:

- Threads the captured cursor into `extract_pvo(watermark=...)`.
- Introduces the `incremental.watermark_safety_window_seconds` `bundle.yaml` override.
- Requires TC28b live evidence as a precondition for enabling `--mode incremental` on a given tenant.

Until P1.17, the constant could be changed by editing `runtime.py` directly without operational risk — no row-level data correctness depends on its value in β.1.

## Re-execution procedure (when BICC unblocks)

```python
# 1. AIDP-side capture (inside an AIDP notebook on the test cluster)
from datetime import datetime, timezone
aidp_now = datetime.now(timezone.utc)

# 2a. Fusion-side primary — BICC metadata after a no-op tiny-PVO extract
from oracle_ai_data_platform_fusion_autopilot.extractors import bicc as _bicc
# Pick a tiny PVO (e.g. LookupTypeExtractPVO) and run a one-shot extract;
# read the snapshot timestamp from the BICC response metadata.
# (Exact field name + access pattern: confirm via the extractor's debug
# logging or the BICC client response object.)

# 2b. Fusion-side fallback — OAC SYSTIMESTAMP
# (Requires an OAC datasource pointing at the Fusion DB AND a REST endpoint
# capable of executing ad-hoc SQL. See docs/oac_rest_api_setup.md.)

# 3. Skew assertion
from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import WATERMARK_SAFETY_WINDOW
skew = abs(aidp_now - fusion_now)
print(f"aidp_now = {aidp_now} (UTC)")
print(f"fusion_now = {fusion_now} (UTC)")
print(f"|skew| = {skew}")
print(f"WATERMARK_SAFETY_WINDOW = {WATERMARK_SAFETY_WINDOW}")
assert skew < WATERMARK_SAFETY_WINDOW, (
    f"observed skew {skew} ≥ WATERMARK_SAFETY_WINDOW {WATERMARK_SAFETY_WINDOW} — "
    f"widen the constant in runtime.py before enabling --mode incremental "
    f"on this tenant (P1.17 will introduce a per-tenant bundle.yaml override)."
)
```

Report both timestamps (operator-redacted to minute-precision is fine; the **magnitude of skew** is what matters, not the absolute clocks), which ladder rung succeeded (1 = BICC, 2 = OAC), and whether the assertion passed.

## Acceptance for P1.17 enablement on a tenant

Before P1.17 enables `--mode incremental` on tenant `<X>`:

1. TC28b lands here with `skew < WATERMARK_SAFETY_WINDOW` evidence captured on `<X>`.
2. If neither ladder rung worked, add a LIMITS entry naming `<X>` as ineligible for incremental mode until a probe path is established.
3. If `skew >= WATERMARK_SAFETY_WINDOW`, widen the constant (in β.1) or the per-tenant `bundle.yaml` override (in P1.17 once shipped) before enablement.

## Cross-references

- Plan A2 decision gate: `docs/features/p1.5b-orchestrator-incremental/plan.md` (Stage A2)
- Plan E2 acceptance: `docs/features/p1.5b-orchestrator-incremental/plan.md` (Stage E2)
- Sibling TC28: `tests/live/TC28_orchestrator_incremental_infra.md`
- OAC connection setup (fallback rung 2): `docs/oac_rest_api_setup.md`
- LIMITS L5 (`--mode incremental` gated until P1.17), L6 (empty-delta + soft-fail regression)
