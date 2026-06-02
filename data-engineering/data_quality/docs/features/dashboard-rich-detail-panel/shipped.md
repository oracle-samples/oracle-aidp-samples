---
shipped: 2026-05-10
---

# Shipped: Interactive Dashboard — Rich Detail Panel for Validation Rows

## Summary

The per-partition history table on the validation-selected view
of the interactive dashboard now carries a `▸` toggle column
that opens a per-row detail panel rendering the full
`details_json` payload via per-validator-type renderers
(shape / pattern / drift / trend / threshold / slo plus a
generic JSON fallback and a distinct Qualifire-internal-failure
renderer). The static `generate_html_report` is unchanged.

## Key Changes

- Per-validator-type renderer registry on the validation-selected
  view; pattern / shape rows surface SHAP top features
  parallel-zipped with `value_drift_explainer` summaries.
- New `_safe_json_for_html_script` helper escapes `</`, U+2028,
  U+2029, `<!--` before SNAPSHOT interpolation (closes
  `<script>` breakout vector).
- Path-aware redaction (whole-key regex
  `^(password|secret|token|api[_-]?key|credential)$`) at non-
  explainer paths; explainer category-disclosure path exempt.
- Multi-stage size caps: 8 KB per-row, 16 KB explainer payload,
  8 MB total snapshot via O(N) running totals (was O(N²)).
- Collision-free per-row toggle keys via
  `JSON.stringify([...natural-key...])`.
- Renderers per kind: `renderShapePattern`, `renderDrift`,
  `renderForecast`, `renderThreshold`, `renderSLO`,
  `renderGeneric`, `renderInternalFailure`.
- New `qf-detail-toggle` column; preserves open state via
  `openDetailRows` Set across re-renders.

## Files Changed

New test files:
- `tests/test_reporting/test_snapshot_details.py` (35 cases)
- `tests/test_reporting/test_dashboard_renderers_node.py` (~10 cases — Node-based JS test harness via subprocess + DOM stubs)
- `tests/test_interactive_dashboard.py` extended with `TestRichDetailPanel` (4 cases)

Plus `qualifire/reporting/_snapshot_details.py` (new) and
substantial extensions to `qualifire/reporting/html_report.py`.

## Plan PR

[#18](https://github.com/amitranjan-oracle/qualifire/pull/18) —
`feat(dashboard-rich-detail-panel): click-to-expand row detail panel`

## Review Cycles

Plan: 4 codex rounds + 2 adversarial. Implementation: 2 codex
impl-review rounds (R1 had 3 BLOCKERs / 4 HIGHs / 4 MEDIUMs /
1 LOW; R2 closed all; R2 verdict PASS).

## Follow-ups Captured (not shipped here)

None — the feature shipped clean. Subsequent work
(`backfill-followups-and-polish`, value-drift explainer
follow-ups) tracked separately.
