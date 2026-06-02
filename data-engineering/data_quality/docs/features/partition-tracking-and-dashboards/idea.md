---
id: partition-tracking-and-dashboards
name: Partition Tracking, Signed Drift, Descriptions, and Dashboards
type: Feature
priority: P0
effort: Large
impact: High
created: 2026-05-06
---

# Partition Tracking, Signed Drift, Descriptions, and Dashboards

## Problem Statement

Five concrete UX defects surfaced while running the manual smoke
notebook against a real dataset, plus two coupled gaps in the data
plane that made historical comparisons less expressive than they
needed to be:

1. **Threshold validators crashed on NULL metrics.** Aggregations
   that legitimately return NULL (`SUM(...) / NULLIF(COUNT(...),0)`
   when no rows match the day's filter, `AVG(amount)` over an empty
   filter, all-NULL columns) tripped `float(None)` deep inside
   threshold/historical/forecast and surfaced as a synthetic
   `threshold_error` row with a low-level Python message — not a
   structured WARNING. Operators couldn't distinguish "data was
   missing" from "code is broken."

2. **No partition identity on persisted rows.** The system table had
   `run_timestamp` (when the validation executed) but nothing
   identifying which logical partition (date / hour / week) the row
   *described*. Drift / forecast checks bucketed history by
   `run_timestamp` only, which conflates a daily ETL job's twice-a-day
   reruns with two distinct partitions, and breaks down entirely when
   late-arriving data is processed for an old partition. The right
   anchor is the partition's logical timestamp, not the wall clock.

3. **Drift and z-score were absolute-only.** `deviation_pct`,
   `deviation_abs`, and `z_score` reported with `abs()` applied,
   throwing away sign. A 50% revenue *drop* and a 50% revenue
   *spike* are not the same incident in most domains, but the system
   alerted them identically. Configs couldn't express asymmetric
   tolerances ("warn on drops > 10% but allow spikes up to 25%").

4. **No rate-of-change measure.** `deviation_pct` compares against
   the *mean* of past values; teams asked for a measure that
   compares against the *immediate prior* partition. Standard
   deviation / z-score doesn't substitute — it's about spread, not
   step-over-step direction.

5. **`dataset_name` and `validation_name` arrived without context.**
   Persisted rows had identifiers but no human description, forcing
   dashboard viewers to consult the YAML to understand "what does
   `null_pid_share` mean and why does it matter?"

6. **HTML reports were white-only.** The static health report and the
   rendered IFrame had hardcoded white backgrounds; in VS Code's
   dark-mode notebook webview every cell that rendered the report
   suddenly became a glaring white box.

7. **No interactive dashboard for drill-down.** The static health
   report aggregates everything across N days; users wanted to pick
   a dataset, then a validation, then see that single validation's
   per-partition history with severity colouring — the standard
   "where is it failing and when?" investigation flow.

8. **`dimension_value` persisted as `'_default'` even when no
   dimension was configured.** Made every system-table row look
   like it had a dimension, polluted dashboards with `_default`
   labels, and conflated "no dimension" with "the literal string
   `'_default'`."

## Proposed Solution (high-level)

Two coupled investments under one feature:

**Data-plane: partition tracking + descriptions + soft dimensions.**
Make `partition_ts` a first-class column that every persisted row
carries; let history reads anchor on it (anchor − k·step). Add
`expected_value`, `actual_value_text`, `dataset_description`, and
`validation_description` columns so dashboards have everything they
need without re-querying. Persist `dimension_value` as NULL when
the dataset has no dimension (preserving the `COALESCE(..., '_default')`
read invariant).

**Surface: notebooks + interactive dashboard + dark mode.** Build a
manual smoke notebook that exercises every collector / validator
type with a per-partition flow. Build a separate setup notebook for
the kernel/JDK gotchas. Build two dashboard notebooks
(`dashboard_html.ipynb` + `dashboard_charts.ipynb`) that read the
system table backend-agnostically and render via static HTML,
interactive Plotly, matplotlib, and combinations thereof. Make every
HTML output dark-mode adaptive.

Plus targeted validator fixes: NULL-safe metric coercion (no more
`float(None)` crashes), signed deviation/z-score, new
`rate_of_change_pct` / `rate_of_change_abs` measures, and
asymmetric `{min, max}` threshold bounds alongside the legacy
bare-number form.

## Affected Areas

- core (config, models, engine, duration parsing)
- collection (aggregation collector — partition_ts injection)
- validation (threshold, historical, forecast, slo)
- storage (5 backends — sqlite, delta, external_catalog, jdbc, base
  protocol — schema additions, partition-anchored reads, extended
  read_health_data column set)
- reporting (html_report — dark mode + new interactive dashboard)
- api (Qualifire — new partition_ts and description kwargs;
  interactive_dashboard method)
- tests (5 new test files; 3 existing tests updated)
- notebooks (4 manual notebooks: notebook, pyspark_setup,
  dashboard_html, dashboard_charts)
- docs (README updated for new features)
