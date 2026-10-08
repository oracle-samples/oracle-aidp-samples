# ADR-0021 — Content pack is the silver/gold node registry (Phase 5)

## Status

Accepted — 2026-06-08. Ships with Phase 5 (default execution backend
flipped to content-pack).

## Context

Pre-Phase-5, the orchestrator's silver/gold registry lived in Python:

```python
# orchestrator/registry.py:189-222
_SILVER_BUILDERS = {
    "dim_supplier": dim_supplier.build,
    "dim_account":  dim_account.build,
    "dim_calendar": dim_calendar.build,
}
_GOLD_BUILDERS = {
    "supplier_spend": supplier_spend.build,
    "gl_balance":     gl_balance.build,
    "ap_aging":       ap_aging.build,
}
SILVER_DIMS / GOLD_MARTS = { wrap each in a Spec dataclass }
```

The orchestrator imported these dicts (`orchestrator/__init__.py:55-60`)
and walked them to build the run plan. Customers wanting a new
dimension or mart had to:

1. Write Python at `dimensions/dim_<name>.py` or
   `transforms/gold/<name>.py`.
2. **Edit the plugin's own `orchestrator/registry.py`** to import it.
3. Fork or vendor the plugin.

This violated the plugin's mission (per CODEX.md): the fusion-autopilot
"must run on any Fusion ERP/HCM/SCM tenant, not just the demo-pod
demo pod. Hardcoded tenant-specific assumptions are bugs."

The pack-driven execution path shipped in Phase 2 (`--execution-backend
content-pack`) but the default flag was `legacy-python`, so default
invocations still walked `SILVER_DIMS` / `GOLD_MARTS`.

Phase 4's dual-runner parity gate (live evidence on demo-pod,
2026-06-07; 6/6 GREEN) closed the gate on flipping the default; Phase 5
makes the flip + wires the `python_legacy` runtime adapter so customer
v1 modules referenced from a pack node continue to dispatch through
the content-pack execution path.

## Decision

**The content pack — not Python — is the source of truth for the
silver/gold node registry.** The orchestrator discovers nodes by
walking `resolved_pack.silver` / `resolved_pack.gold` (each a `dict[id,
NodeYaml]`); per-node `implementation.type` discriminates the runtime
path:

| `implementation.type` | Runtime path |
|---|---|
| `sql` | Render via `sql_renderer.render_node_sql(...)` + execute via the strategy executors (Phase 2). |
| `builtin` | Invoke the registered builtin adapter (e.g. `dim_calendar_adapter.run`). |
| `python_legacy` | Invoke the v1 callable via `python_legacy_adapter.invoke_legacy_callable(...)` (Phase 5). |

The v1 `dimensions/dim_*.py` / `transforms/gold/*.py` modules stay as
**frozen reference implementations** per CODEX.md "v1 + v2
coexistence" through Phase 9. The `--execution-backend=legacy-python`
opt-out still walks `SILVER_DIMS` / `GOLD_MARTS` byte-for-byte
unchanged; it emits a one-line deprecation warning on every
invocation.

The default flip lands together with two new preflight gates that the
content-pack backend now runs before any silver/gold work:

* **AIDPF-2071 (bronze readiness)** — every in-scope silver/gold
  node's transitive bronze dependency must exist AND surface every
  declared `requiredColumns` + per-node `watermark.column`.
* **AIDPF-2072 (Fusion PVO drift)** — the live Fusion PVO schema
  (probed metadata-only via `extract_pvo(...).schema`) must match
  the pinned per-dataset snapshot from Phase 3d AND surface every
  required column.

Both gates emit consolidated diagnostic JSONs under
`.aidp/diagnostics/<run_id>/AIDPF-207[12].json` with the standard
two-step remediation runbook (`bootstrap --refresh` then the
`/medallion-author` Codex skill).

## Consequences

### Positive

* **Plugin-portable extension model.** Customers add a new silver dim
  or gold mart by adding `content_packs/<pack-id>/silver/<id>.yaml` +
  `<id>.sql` (or a `python_legacy` callable for existing modules). No
  plugin fork; no `registry.py` edit.
* **Pack as the audit surface.** `pack.yaml` + `silver/*.yaml` +
  `gold/*.yaml` are the human-readable, versioned, reviewable source
  of truth for "what does this tenant's pipeline build, and how?". The
  v1 backend's "Python is the registry" model required a code review
  to answer the same question.
* **Preflight catches drift BEFORE bronze extract cost.** Phase 5's
  AIDPF-2072 probes the live Fusion PVO BEFORE the orchestrator pulls
  data. Pre-Phase-5, a renamed Fusion column surfaced only after the
  bronze extract had landed.

### Negative / Costs

* **The `python_legacy` adapter is permanent through Phase 9.**
  Customers who shipped v1 module extensions before Phase 5 need to
  declare a `python_legacy` node in their overlay pack to keep their
  modules dispatching. Phase 9 deletes the v1 modules entirely; the
  `python_legacy` adapter survives for customer-shipped overlays
  beyond that.
* **Test-migration cost.** Tests that called
  `orchestrator.run(bundle_path)` without an `execution_backend` flag
  now run through the content-pack path by default. Tests asserting
  v1-specific log lines or side effects need to either pass
  `execution_backend="legacy-python"` explicitly or migrate to the
  content-pack contract.

## Alternatives considered

* **Option B — Bronze precondition fail-closed (`AIDPF-1041`).** Reject
  default-flipped invocations when bronze tables don't exist, asking
  the operator to run `--layers bronze --execution-backend=legacy-python`
  first. Rejected: hostile UX vs Option A's "extract bronze first
  then content-pack" orchestration. AIDPF-2071 (Phase 5 Step 2c) is
  a strict superset of Option B's existence check (adds per-column
  validation).
* **Deferring the default flip to Phase 6.** Rejected: Phase 4's live
  evidence already proves parity; deferring would just delay the
  customer-extensibility win that motivated the v2 work in the first
  place.

## References

* CODEX.md "v1 + v2 coexistence (migration state)" — the
  frozen-reference contract for v1 modules through Phase 9.
* `docs/features/v2-phase-5-registry-content-pack-driven/plan.md` —
  the full Phase 5 implementation plan (incl. Step 1's
  python_legacy adapter survey, Step 2b's Option A scope split, Step
  2c/2d gate algorithms).
* `tests/parity/test_dual_runner_e2e.py` — Phase 4's parity-gate
  harness that gated the default flip.
* `tests/live/TC_phase4_v2_seed_live.md` — Phase 4's live evidence
  trail (demo-pod; 2026-06-07; 6/6 GREEN).
* ADR-0011 — `dim_calendar` builtin adapter (the Phase 3 pattern
  the python_legacy adapter mirrors).
* ADR-0014 — Variation-resolution-at-bootstrap (reinforces "pack is
  the source of truth"; ADR-0021 extends the contract to the
  registry itself).
