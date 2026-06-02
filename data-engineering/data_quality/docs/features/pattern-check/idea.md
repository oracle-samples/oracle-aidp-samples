---
id: pattern-check
name: Pattern Check Validation (Random Forest Two-Sample)
type: Feature
priority: P1
effort: Medium
impact: High
created: 2026-04-21
---

# Pattern Check Validation (Random Forest Two-Sample)

## Problem Statement

Qualifire already ships two validation types that touch "something looks
off" territory:

- `drift` (`HistoricalValidator`) — compares a **single metric's scalar
  value** against its historical mean/z-score.
- `shape` (`AnomalyDetectionValidator`, Isolation Forest) — scores
  **individual rows** against a reference set and flags per-row
  outliers.

Neither answers the question a user naturally asks when they want to
know whether "this run looks different from past runs": **can the
current batch of rows be told apart from past batches across many
features at once?** That is a batch-level, multivariate question, and
the mathematically direct way to answer it is a **classifier two-sample
test** — train a supervised classifier to separate past-run rows (label
0) from current-run rows (label 1); if the classifier's AUC is
meaningfully above 0.5, the distributions differ, and SHAP on the
classifier names the features driving the separation.

Consequences of not having this today:

- Gradual distribution drift with no individual outliers can slip past
  `shape` entirely (no row is isolated, but *together* they shift).
- Per-metric `drift` misses multivariate shifts that only become
  visible when features are considered jointly.
- Users who want a "did this run's data look like past runs?" signal
  have to infer it from proxy metrics.

## Proposed Solution (High Level)

Introduce a **new validation type** — `pattern` in YAML, implemented as
`pattern_check` — that runs a Random Forest two-sample classifier
against a sample from the current run vs. samples from past runs
(honoring the existing `step` dimension used by `drift`), and raises
based on a configurable AUC threshold. SHAP over the trained
classifier explains which features are driving the separation.

Vocabulary after this lands:

- `drift` — "is *this metric's number* off its historical mean?"
- `shape` — "which *individual rows* look wrong vs. a reference?"
- `pattern` — "does the *overall pattern* of this run differ from past
  runs?"

Sane defaults (overridable via config):

- `n_estimators=200`, `max_depth=8`, `class_weight="balanced"`,
  `random_state=42`.
- AUC threshold ≈ `0.65` (below → pass, above → drift detected).
- Sample sizes, past-run lookback, and `step` bucketing reuse the
  semantics already established by `drift`/`shape` where applicable.

SHAP output should surface the top-N features driving the two-sample
separation in the validation result so downstream notifications and
reports can show *why* the pattern changed, not just *that* it
changed.

The existing `shape` / Isolation Forest validation is **unchanged** —
users keep the per-row outlier semantics they rely on.

(Full design — exact config schema, row sampling contract, interaction
with `step`, handling of categorical features, edge cases around tiny
samples or unchanged past runs, SHAP output shape, and the test matrix
— belongs in `plan.md`.)

## Affected Areas

- validation (new `pattern_check` validator module + registration)
- configuration (new `PatternValidationConfig`, wired into
  `ValidationConfig` union)
- shap-explanations
- documentation (`docs/validators.md`, `docs/configuration.md`,
  `README.md`, programmatic API docs)
- unit-tests (new validator + config parsing)
- end-to-end-tests (extend the existing E2E suite to exercise
  `pattern` alongside `drift`, `shape`, `trend`, `slo`, `threshold`)
