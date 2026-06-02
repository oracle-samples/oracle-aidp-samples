---
id: feature-value-drift-explainer
name: Per-Feature Value Drift Explainer
type: Enhancement
priority: P2
effort: Medium
impact: Medium
created: 2026-05-07
---

# Per-Feature Value Drift Explainer

## Problem Statement

When `shape` (Isolation Forest) or `pattern` (Random Forest two-sample)
fails, SHAP names the top contributing features in the alert body — e.g.
*"top contributing features: amount, channel_mobile, merchant_id"*. That
tells the reviewer *which* columns mattered, but not *how the values
shifted*.

Non-technical reviewers (data product owners, business analysts on the
distribution list) can't act on "amount mattered". They need the next
question answered: **how did `amount` change?** — did it skew higher,
fatter-tailed, did a category collapse onto a single value, did nulls
spike?

Today the only way to answer that is to crack open the source data and
plot it manually. Most reviewers don't, and the alert ends up in the
"Bob will look at it" pile. The signal we already have (the sample
DataFrames behind the SHAP scores) goes unused.

## Why It Matters

- Closes the loop between detection and triage. Right now an alert says
  "this column is suspicious"; with this it says "this column is
  suspicious, and here's exactly how it shifted."
- Brings non-technical reviewers into the workflow. Q3 directive on
  notifications already pushed top features into Slack/email — this is
  the next layer that makes those features *interpretable*.
- Reuses signal already collected. The sampler retains current and past
  DataFrames; the validator already runs SHAP. We compute per-feature
  comparison stats from data we have in memory.

## Who Benefits

- On-call engineers triaging shape/pattern alerts at 2 AM.
- Data product owners and BAs subscribed to high-severity Slack
  channels.
- Auditors who need to explain *why* a partition was flagged.

## Affected Areas

- `qualifire/validation/pattern_check.py` (the SHAP path)
- `qualifire/validation/isolation_forest.py` (shape SHAP path)
- `qualifire/validation/_encoding.py` (need access to pre-encoded
  values, not just feature matrices)
- `qualifire/notification/base.py` (extend `format_validation_details`
  to render the new comparison block)
- `qualifire/reporting/plots.py` and HTML report (visual rendering for
  non-Slack consumers)

## Open Questions for Planning

These belong in `plan.md`, not here — listing for context:

- Numeric features: which summary best conveys "skewed higher" — mean
  +/- stddev, p50/p90/p99 quantile triple, or a delta on each?
- Categorical features: top-N category mix (current vs past), or just
  flag the category with the largest swing?
- Encoded-only features (one-hot bins, label-encoded high-cardinality):
  do we recover the original values, or report on the encoded form?
- Notification budget: how much of this lands in Slack vs falls back to
  the HTML report / system table?
- Partition fairness: comparing current to "past" — do we compare
  against the union of past slices or the most recent past?
