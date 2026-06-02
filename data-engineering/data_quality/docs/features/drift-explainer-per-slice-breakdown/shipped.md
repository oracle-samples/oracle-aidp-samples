---
shipped: 2026-05-10
---

# Shipped: drift-explainer-per-slice-breakdown

## Summary

Opt-in `drift_breakdown_by_slice` flag on `AnomalyModelConfig`
and `PatternModelConfig`. When True, the first 3 entries of
`details["value_drift_explainer"]` (with kind in
`{numeric, onehot, boolean}`) carry a `per_slice` block — one
entry per past slice in the sampler's `past_pdfs` order. Lets
operators answer "which past partition shifted?" during triage —
gradual vs sudden drift signal — without bloating the notification
body or the persisted payload.

## Key Changes

- **`qualifire/validation/_drift_explainer.py`** — new
  `breakdown_by_slice: bool = False` kwarg on `explain_value_drift`.
  Three new module-level constants: `_PER_ENTRY_PER_SLICE_MAX_BYTES
  = 3072`, `_PER_SLICE_TOP_N = 3`,
  `_PER_SLICE_SUPPORTED_KINDS = {numeric, onehot, boolean}`.
  New `_attach_per_slice` helper applies an explicit a/b/c gate
  (top-3 + supported kind + mapped) before invoking
  `_build_per_slice` to emit per-kind `current_vs_slice` shape.
  Per-entry truncation drops the `per_slice` block and sets
  `per_slice_truncated=True` when adding it would push the entry
  past 3 KB; `_enforce_payload_budget` (the existing 16 KB total
  cap) runs unchanged after.
- **`qualifire/core/config.py`** — `drift_breakdown_by_slice:
  bool = False` field on `AnomalyModelConfig` and
  `PatternModelConfig`.
- **`qualifire/validation/{pattern_check,isolation_forest}.py`** —
  validator `__init__` accepts `drift_breakdown_by_slice` param;
  `validate()` passes it through as `breakdown_by_slice=` to
  `explain_value_drift`.
- **`qualifire/core/engine.py`** — at validator instantiation
  (lines 1630/1651), reads `model.drift_breakdown_by_slice` and
  forwards via `getattr` (defensive default `False` if any future
  model config drops the field).
- **`tests/test_validation/test_drift_explainer_per_slice.py`** —
  12 tests covering T1 (flag=False is no-op), T2 (top-3 carry,
  4 slices each), T3 (numeric shape), T4 (onehot shape), T5
  (boolean shape), T6 (datetime omitted), T7 (top-5 union with
  only first-3 per_slice), T8 (truncation under 3 KB cap), T9
  (unmapped + failed paths skip per_slice), plus a JSON
  round-trip pin.
- **`docs/CHANGELOG.md`** — Enhancement entry.

## Files Changed

7 files; +408 / −7 lines.

## Plan PR

[#23](https://github.com/amitranjan-oracle/qualifire/pull/23).

## Review Cycles

Plan: 2 adversarial + 2 codex rounds. Codex R1 found 1 MAJOR
(missing `label_encoded` and `unknown` kinds in
`EncodedFeatureSpec`) + 1 MEDIUM (gate semantics not explicit);
R2 PASS at v4.

Implementation: 2 codex rounds, both PASS independently. R2
confirmed boolean null-handling, onehot raw-column lookup,
truncation interaction with `_enforce_payload_budget`, and engine
defensive-default behavior.

## Local Test Results

- 1504 passed, 2 skipped (+12 new tests; up from 1492).
- No regressions; full suite green.
