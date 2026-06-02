---
id: drift-explainer-per-slice-breakdown
name: Value Drift Explainer — Optional Per-Past-Slice Breakdown
type: Enhancement
priority: P3
effort: Small
impact: Low
created: 2026-05-10
---

# Value Drift Explainer — Optional Per-Past-Slice Breakdown

## Problem Statement

Today's `value_drift_explainer` (shipped in
`feature-value-drift-explainer`, PR #15) compares the current
sample against the **union** of all past slices. That mirrors
SHAP's training contract — the classifier sees label-0 as one
combined past corpus — and keeps the persisted payload tight.

But there's a residual operator question: *which* past partition
shifted? When `past_dates=4` and the explainer says
`amount p99 +67%`, the operator doesn't know whether week-1
already started drifting or it was a sudden week-4 shift. The
sampler already retains the per-slice DataFrames in
`metadata["past_dfs"]`, so the data is on hand; we just don't
expose it.

## Why It Matters

- Triage. A gradual drift is a different remediation than a
  sudden break — gradual usually means an upstream slow-burn
  config drift; sudden often means a deploy or pipeline change
  on a known date.
- Auditing. "Which week did this start?" is the first question
  an auditor asks once the alert is acknowledged.
- Cheap to add. The data is already in memory; the cost is a
  bigger persisted payload.

## Proposed Solution (sketch)

Operator opt-in:

```yaml
model:
  explain_value_drift: true        # existing
  drift_breakdown_by_slice: true   # NEW; default false
```

When set, the explainer adds a `per_slice` field to each entry:

```python
{
  "feature": "amount",
  ...,
  "per_slice": [
    {"label": "past_1", "current_vs_slice": {"mean_pct": 0.10, ...}},
    {"label": "past_2", "current_vs_slice": {"mean_pct": 0.30, ...}},
    {"label": "past_3", "current_vs_slice": {"mean_pct": 0.50, ...}},
    {"label": "past_4", "current_vs_slice": {"mean_pct": 0.67, ...}},
  ],
}
```

Notification body remains unchanged (still top-3 union summaries).
The per-slice block lands in `details_json` and the dashboard
detail panel.

## Default

`drift_breakdown_by_slice = False`. The 16 KB payload cap doesn't
have headroom for `past_dates × top-5 features × full stat block`
without truncation, so we pin opt-in.

## Who Benefits

- Operators triaging drift alerts who need the trend shape, not
  just the union delta.
- Auditors reconstructing when a drift started.

## Affected Areas

- `qualifire/validation/_drift_explainer.py` — accept the new
  flag, walk past_pdfs as separate slices when set.
- `qualifire/validation/{pattern_check,isolation_forest}.py` —
  thread the flag through.
- `qualifire/core/config.py` — `drift_breakdown_by_slice` on
  `AnomalyModelConfig` and `PatternModelConfig`.
- Notification body — leave unchanged.
- Dashboard detail panel — render a sparkline per feature when
  `per_slice` is present (depends on
  `dashboard-rich-detail-panel`).
- Tests + docs.

## Open Questions for Planning

- Payload size — under `past_dates=8 × top-5 features` the
  per-slice block alone could exceed the existing 16 KB cap.
  Either bump the cap (storage cost) or limit per-slice
  breakdown to top-3 features instead of top-5.
- Numeric vs categorical — for a categorical feature the
  per-slice block needs to express *which* category shifted at
  each slice; full top-N category mix per slice is probably
  too verbose. Plan phase decides.

## Why Deferred

`feature-value-drift-explainer` deferred this in its plan as
"interesting but blows up the notification body and the
`details_json` payload. Defer to a follow-up if operators ask
for it." This idea captures the follow-up.
