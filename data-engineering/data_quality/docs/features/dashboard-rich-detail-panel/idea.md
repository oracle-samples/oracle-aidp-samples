---
id: dashboard-rich-detail-panel
name: Interactive Dashboard — Rich Detail Panel for Validation Rows
type: Feature
priority: P2
effort: Medium
impact: Medium
created: 2026-05-08
---

# Interactive Dashboard — Rich Detail Panel for Validation Rows

## Problem Statement

The interactive dashboard's table view shows one row per
`ValidationResult`, with `validation_message` as the only diagnostic
column. For pattern / shape validators that's a single AUC or
anomaly score; for drift it's a `deviation_pct` summary; for
threshold it's an `actual vs expected` line. The structured
`details` dict each validator already populates — SHAP top
contributing features, Prophet's `[yhat_lower, yhat_upper]` band,
drift's `mean_past` / `stddev` / `z_score`, threshold's per-bound
breakdown — never reaches the dashboard. Operators investigating
an alert have to drop down to the system table or notification
output to get the *why*.

The notification side already renders these details
(`format_validation_details` in `qualifire/notification/base.py`):
SHAP top features as bullet points, drift's signed deviation /
z-score / rate-of-change, forecast's prediction band vs observed,
SLO's age vs threshold, threshold's `expected_value` summary. The
dashboard has the same data but doesn't surface it.

## Why this matters

Validators ARE doing the explanation work. Discarding it at the
dashboard surface forces operators to context-switch into raw
storage when an alert fires — which is exactly when they have the
least time. A single click should reveal *what changed*, not just
*that something changed*.

For pattern/shape specifically, "AUC = 0.84" is a much weaker
signal than "AUC = 0.84, top contributors: region_us +0.32,
amount +0.18, product_id +0.11" — the second tells the operator
which column to look at first.

## Proposed Solution (sketch — design lives in `/feature-plan`)

A click-to-expand row detail panel:

- **Inline summary** — for pattern/shape, append a compact top-3
  feature list to the message column (`AUC=0.84 · top:
  region_us(+0.32), amount(+0.18), product_id(+0.11)`). Keep the
  message string short enough to scan.
- **Click-to-expand panel** — clicking a table row slides out a
  detail pane with full SHAP table, plotted prediction band for
  forecast, side-by-side `actual / expected / threshold` for
  threshold, partition-anchored history mini-chart for drift, and
  the underlying `details` JSON dump.
- **Generalisation** — driven off the system table's `details`
  column, so any future validator that populates `details`
  automatically renders without dashboard JS changes.

## Affected Areas
- storage (`read_health_data` — project `details` JSON)
- reporting (`html_report.py` — snapshot serialisation +
  detail-panel JS + per-validator renderers)
- core/models (decide whether to expose `details` on the snapshot
  Pydantic model)

## Notes / Open Questions

- Snapshot size — `details` blobs can be large (SHAP arrays).
  Consider a server-side cap (top-K only) so the embedded SNAPSHOT
  JSON stays within sensible bounds for inline rendering.
- Per-validator renderer registry vs a single generic renderer.
  Generic is simpler; per-validator is prettier (e.g. Plotly band
  for forecast).
- Privacy / PII — details may include sample rows or column values.
  An audit pass is warranted before we ship this to environments
  where the dashboard is shared.
- Captured 2026-05-08 from notebook_end_to_end work, after
  appending top-3 SHAP features to pattern/shape `message`
  strings as a stop-gap.
- 2026-05-10 update: `feature-value-drift-explainer` (PR #15,
  merged) added a new `details["value_drift_explainer"]` block —
  parallel-list per-feature value-shift summaries with per-kind
  stats (numeric quantiles, boolean true_rate, datetime ISO
  range, one-hot per-bin rate, label-encoded top-N category mix).
  This panel must render that block. Notebook `plot_value_drift`
  in `qualifire.reporting.plots` is the static-plot reference;
  the dashboard needs the equivalent click-to-expand inline form.
