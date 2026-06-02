---
shipped: 2026-05-10
---

# Shipped: Per-Feature Value Drift Explainer

## Summary

Augments shape (Isolation Forest) and pattern (Random Forest
two-sample) validators so each entry in
`details.top_contributing_features` carries a parallel-list
per-feature value-shift summary describing *how* the column's
distribution changed between the current sample and the union
of past slices. Closes the "SHAP says this column drove the
alert; how exactly did it shift?" question without operator
having to drop into raw storage.

## Key Changes

- Per-feature kind-aware summaries:
  - **Numeric**: quantile triple (p50, p99) + mean / std + min / max + null_pct.
  - **Boolean**: true_rate.
  - **Datetime**: ISO min / max.
  - **One-hot**: per-bin rate.
  - **Label-encoded**: top-5 category mix with parallel-array alignment.
- Notifier bodies show the top-3 summaries inline as `→ {summary}`
  bullets under the existing `• {feature}` SHAP bullets.
- Schema parallel-length / parallel-order with
  `top_contributing_features` so readers can `zip()` the two;
  over-budget entries placeholdered with `kind="truncated"` to
  preserve the invariant.
- Per-validator opt-out flag `model.explain_value_drift: bool = True`
  on `AnomalyModelConfig` and `PatternModelConfig`.
- Plot helper `qualifire.reporting.plots.plot_value_drift(...)`
  for notebook consumption.
- Encoder reverse-mapping exposed: `encode_columns` return shape
  changed from `(X, feature_names)` to
  `(X, feature_names, encoding_map)`.

## Files Changed

19 files; +1,746 / −22 lines. New tests in
`test_validation/test_isolation_forest.py` (+62 cases),
`test_validation/test_pattern_check.py` (+55 cases),
plus storage round-trip on SQLite.

## Plan PR

[#15](https://github.com/amitranjan-oracle/qualifire/pull/15) —
`feat(feature-value-drift-explainer): per-feature value drift explainer`

## Follow-ups Captured (not shipped here)

- [`details-json-backend-roundtrip-parity`](../details-json-backend-roundtrip-parity/idea.md)
  — extend round-trip tests to Delta / JDBC / external_catalog.
- [`drift-explainer-per-slice-breakdown`](../drift-explainer-per-slice-breakdown/idea.md)
  — opt-in per-past-slice breakdown for triage.
