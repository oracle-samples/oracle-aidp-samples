---
shipped: 2026-05-07
---

# Shipped — partition-tracking-and-dashboards

What actually landed for this feature, by commit. Plan-side intent
lives in [`plan.md`](plan.md); this file is the retrospective —
what shipped, and where each piece lives in the source tree.

## Commit map

| Commit | Title | Round |
|---|---|---|
| `be7edd4` | `feat: partition_ts as first-class anchor + interactive dashboards` | Phases 0–10 |
| `d1bc9ed` | `feat: reporting package, docs reorg, gap tests` | Phase 11 baseline |
| `f673e98` | `refactor: tighten step / partition_ts / dimension contract` | R1 fix |
| `7eebd14` | `fix: install path + test stability (B2-B5)` | R1 fix |
| `3939075` | `refactor: major findings sweep (M1, M2/M3, M4, M5, M6, M8, M9/M10)` | R1 fix |
| `2f1111a` | `chore: minor hygiene (m1, m2, m4, m5, m7)` | R1 fix |
| `5493535` | `docs: final sync — README + CONTRIBUTING + storage Protocol` | R1 fix |
| `567ccd8` | `fix: critical correctness + identity contract (R2-1)` | R2 fix — sampler shifted_ctx, SQLite dedupe, forecast prediction frame, dimension JSON canonical form |

The first commit (`be7edd4`) covers Phases 0–10 as designed in the
plan; the rest are review-driven refinements landed across two
adversarial / Codex review rounds. R1 closed eight blockers from the
first review; R2-1 closed the three critical correctness bugs found
in the second pass. Subsequent R2 commits cover doc / API alignment
(R2-2), architecture cleanup (R2-3), and minor hygiene (R2-4).

## What's new — feature-facing

### Partition anchoring (`be7edd4`)

`partition_ts` becomes a first-class column on the system table and
a first-class field on `CollectionResult`. The aggregation collector
injects a `qf_partition_ts` expression into its SELECT and stamps
each `CollectionResult` with the resolved value.

Drift and forecast validators read history via
`SystemTableStorage.read_metric_history_by_partition(anchor_ts,
count, step, …)` — exact-match `IN`-clause on
`anchor − k·step` for `k=1..count`. Forecast uses `partition_ts`
as Prophet's `ds` axis (was `run_timestamp`); backfills and
notebook re-runs no longer cluster history at one wall-clock
moment.

### Signed historical comparisons (`be7edd4`)

`HistoricalValidator` emits five **signed** measures:
`deviation_pct`, `deviation_abs`, `z_score`, `rate_of_change_pct`,
`rate_of_change_abs`. Threshold values accept either a bare number
(legacy symmetric absolute bound) or a dict (`{min, max}` for
asymmetric signed bounds; either side optional).

### Dataset / validation descriptions (`be7edd4`)

`DatasetConfig.description` and per-validation `description` flow
through to `dataset_description` / `validation_description` columns
on every persisted row, so dashboards can surface them without an
extra metadata lookup.

### Dark-mode adaptive HTML + interactive dashboard (`be7edd4`)

`generate_html_report` and `generate_health_html` swap to CSS
variables overridden under `@media (prefers-color-scheme: dark)`.
`generate_interactive_html(storage, …)` is a new Plotly-backed
drill-down dashboard with dataset / validation pickers,
per-partition history plots, dark-mode adaptation, and
`inline_plotly_js` for offline / sandboxed iframes.

### Reporting package (`d1bc9ed`)

`qualifire/reporting/__init__.py` is now a public surface re-exporting
the full health / charts / storage helpers. Notebooks under
`tests/manual/dashboard_*.ipynb` are thin demos that import from this
surface — no inlined matplotlib gridspec or Plotly subplot stitching.

| Function | Source | Purpose |
|---|---|---|
| `make_storage(backend, system_table, jdbc_config=, spark=)` | `reporting/storage_factory.py` | Backend-agnostic factory |
| `load_health_dataframe(storage, days=)` | `reporting/system_table.py` | Coerced pandas DataFrame for chart helpers |
| `build_result_from_system_table(storage, run_id=, days=)` | `reporting/system_table.py` | Reconstruct `QualifireResult` from persisted rows |
| `plot_executive_summary[_interactive]` | `reporting/plots.py` | KPI cards + trend + worst offenders |
| `plot_dataset_day_heatmap` | `reporting/plots.py` | matplotlib dataset × day heatmap |
| `plot_validation_history_interactive` | `reporting/plots.py` | Plotly per-validation history |
| `plot_severity_hierarchy` | `reporting/plots.py` | Plotly sunburst + treemap |
| `plot_severity_by_type` | `reporting/plots.py` | Stacked bar |
| `plot_metric_distribution_by_severity` | `reporting/plots.py` | Plotly violin |

`read_health_data` SELECT extended on all four storage backends to
include `run_id`, `owner`, `bu` so reconstruction can group correctly
and per-run HTML reports show the right Owner / BU.

### Docs reorganisation (`d1bc9ed`)

```
docs/
├── validators/
│   ├── README.md          # validator index / chooser
│   ├── slo.md             (was slo_check.md)
│   ├── threshold.md       (extracted from validators.md)
│   ├── drift.md           (extracted from validators.md)
│   ├── forecast.md        (was trend_check.md)
│   ├── shape.md           (was shape_check.md)
│   ├── pattern.md         (was pattern_check.md)
│   └── partition_anchoring.md   (new — partition_ts / step contract)
```

`docs/validators.md` and the four `*_check.md` files removed.

## Review follow-ups

`f673e98` and `7eebd14` address the issues raised by Codex review +
self adversarial review pass. See [`.ongoing-review.md`](../../../.ongoing-review.md)
(local-only, gitignored) for the full review trail. Highlights:

### `f673e98` — design-shape tightening

- **`step` is required and non-nullable** on `HistoricalCompareConfig`,
  `ForecastModelConfig`, and `SampleHistoryConfig`. Pydantic rejects
  configs missing `step` at config-load time. No baked-in default,
  no `null` form — always an explicit ISO 8601 duration.
- **ISO 8601 only.** `parse_duration` rejects the legacy compact form
  (`"1D"`, `"7D"`, `"1H"`, `"1D4H"`) with a pointed migration error
  pointing at `P1D` / `P7D` / `PT1H` / `P1DT4H`. `format_duration` was
  removed; SLO display now uses `format_duration_iso`. Test fixtures,
  YAML examples, and validator docs swept to ISO form (178
  conversions across 33 files).
- **Sampler is partition-anchored.** `SamplerCollector` (used by
  shape / pattern) computes past slices as `slice_value − k·step`.
  Wall-clock fallback (`date.today()`) removed; missing slicing
  predicate or `step` raises `MissingPartitionAnchorError`.
  `history_filters` mode (explicit per-slice predicates) bypasses
  the anchor math but still requires `step` for reproducibility.

  *(R3 follow-up: the sampler API was reshaped — see the round-3
  section below. Operators provide `slice_column` + `slice_value`
  for shape / pattern. The `cr.partition_ts − k·step` model still
  applies to drift / forecast.)*
- **`_default` sentinel removed.** All four storage backends, the
  identity helpers, validators, and reporting layer now use NULL for
  the no-dimension case. SELECT predicates use NULL-safe equality:
  `dimension_value IS ?` (SQLite), `dimension_value <=> ?` (Spark),
  parameterized `IS NULL` / `=` (JDBC). 91 references across the
  source + tests cleaned up.
- **drift/threshold doc payload reconciled.** The new
  `validators/drift.md` and `validators/threshold.md` "Result payload"
  sections now match what the validators actually emit
  (`past_values`, `mean_past`, `stddev`, the five signed measures,
  `cold_start`). No more references to non-existent fields like
  `details.measures` or `details.violated_bounds`.

### `7eebd14` — install path + test stability

- **`pandas>=2.2`** added to the `viz` extra (and `requirements-viz.txt`,
  and the `all` extra). The reporting helpers depend on `pandas` and
  use `groupby().apply(include_groups=False)` (pandas ≥ 2.2). Without
  this, the documented `pip install -r requirements-viz.txt` would
  fail on a clean env.
- **`owner` / `bu` in `read_health_data` SELECT.**
  `build_result_from_system_table` already passes them through; the
  SELECT was missing them, so per-run HTML reports showed blank
  Owner / BU. Added a roundtrip test.
- **Relative timestamps in test fixtures.**
  `tests/test_reporting_charts.py:seeded_storage` previously hardcoded
  `2026-05-0N`, which would silently age out of the
  `read_health_data(days=30)` window. Fixture now anchors at
  `datetime.now() − N days` so it stays inside the window
  indefinitely.

### `f673e98` storage factory unification

`qualifire/storage/factory.py:open_storage(...)` is the single
source of truth for "open a SystemTableStorage handle for backend
X". Both `Qualifire._init_storage()` and
`qualifire.reporting.make_storage()` delegate to it; the JDBC config
normalisation and the Spark / sqlite split live in one place. The
notebook factory accepts both `JDBCConfig` and a flat dict (the YAML
shape) and normalises before delegating.

## Test coverage

| Suite | Tests | Notes |
|---|---|---|
| Unit + integration | 863 passing | Up from 683 on `main` before this feature. |
| Industry e2e packs | 81 (subset of 858) | All three packs (retail / fs / life sciences) green on pandas + Spark. |
| Manual notebooks | nbclient-verified | `dashboard_html.ipynb` and `dashboard_charts.ipynb` execute end-to-end against seeded sqlite; `notebook_overall.ipynb` and `notebook_complex_types.ipynb` run on macOS Corretto 8 + Spark 3.5.0. |

New test files:

- `tests/test_partition_ts.py` — partition tracking end-to-end.
- `tests/test_signed_drift.py` — signed math + asymmetric thresholds
  + df-mode partition_ts forwarding.
- `tests/test_columns_descriptions.py` — descriptions + dim NULL
  persistence.
- `tests/test_interactive_dashboard.py` — interactive HTML rendering.
- `tests/test_validation/test_null_safe.py` — NULL-safe arithmetic
  on the validators.
- `tests/test_reporting_charts.py` — full surface coverage for the
  new reporting helpers (factory, dataframe coercion, result
  reconstruction, all aggregate plots, owner/bu roundtrip).

## Migration

The library is in active development; no operator-facing migration
guide is shipped. Configs from earlier branches will need:

- `step` set explicitly on every drift / forecast / shape /
  pattern rule (no defaults).
- ISO 8601 form everywhere (`P1D` / `P7D` / `PT1H`) — legacy compact
  rejected at config-load time with a clear error.
- `partition_ts` set on every `DatasetConfig` that drives drift /
  forecast (mandatory anchor for those validators' history reads).
- For shape / pattern (post-R3): set `slice_column` + `slice_value`
  on the sample collection, *or* supply explicit
  `history.filters`. `partition_ts` on the dataset is still useful
  for stamping persisted rows but is no longer the sampler anchor.

## Exit criteria — met

- 863 tests green at original ship; 874 green after R3 follow-ups
  (see below).
- All four storage backends (sqlite / delta / external_catalog /
  jdbc) speak the new partition-anchored read protocol.
- Documented `pip install -r requirements-viz.txt` produces a
  working dashboard install on a clean env.
- Interactive dashboard renders in VS Code (inline Plotly), classic
  Jupyter, and a standalone browser (CDN).
- No `_default` sentinel, no legacy compact duration, no
  wall-clock-anchored history reads anywhere in the codebase.

## Round 3 follow-ups (post-ship)

Three review rounds (R1, R2, R3) ran on this branch. R1 (8 blocking
+ 10 major + 7 minor) and R2 (3 critical + 7 major + 7 minor + 4
plan-drift) were resolved during the original ship body. R3
captured directives that landed after — shipped as follow-up
commits on the same branch:

* **Sampler API restructure.** Free-form Jinja sampler filter
  replaced with a structured `slice_column` + `slice_value` pair
  plus `DatasetConfig.filter` plumbed through as `filter_expr` for
  AND-combination per slice. Naming evolved through review passes
  to `slice_column` / `slice_value`. Public API (`shape_check`,
  `pattern_check`), engine plumbing, YAML / Python examples, and
  validator docs all updated.
  (`19652fe` → `150a006` → `48f024f`)

* **Notification enrichment.** Slack, email, and webhook bodies now
  include the per-validator diagnostic block: top SHAP contributors
  for shape / pattern, current vs past mean and all signed measures
  for drift, prediction band for trend, threshold / SLO bounds.
  Webhook payload gained `details`, `expected_value`, and
  `dimension_value` so downstream pipelines no longer have to
  round-trip the system table. (`13b7944`)

* **Documentation expansion.** New `docs/collectors/` (per-collector
  pages: aggregation, metrics, profiling, sample, custom_query,
  recency), `docs/notifications/` (per-channel pages: email, slack,
  webhook, grouping), `docs/validators/validator_collector_matrix.md`
  (compatibility matrix with examples for every valid pair), and
  `docs/jinja_rendering.md` (context vs extra precedence, where
  Jinja runs). YAML examples gained `partition_ts` on every dataset
  that reads history; `historical_check.yaml` gained signed-bound
  examples; `custom_sql_query_check.yaml` rewritten to demonstrate
  both rolled-up and per-dimension threshold patterns. (`75ba26e`)

* **Backlog captured.** `feature-value-drift-explainer` (P2,
  Medium) — show per-feature value comparison after SHAP names a
  top contributor. Two existing-feature scope additions on
  `metrics-backfill-and-soft-delete`: skip-if-cached for
  non-backfill runs; WAP backfill collects from `target_table`.

## What's deferred to follow-up features

Items raised during R3 but intentionally not absorbed into this
feature:

- Per-feature value drift explainer (separate feature idea).
- Skip-if-cached on non-backfill runs (folded into the backfill
  feature scope).
- WAP backfill from target_table (folded into the backfill feature
  scope).
- HTML report auto-includes the SHAP plot (currently the helper is
  exported but not wired into the default report template).
- Per-feature value distribution comparison in alerts (covered by
  the new feature-value-drift-explainer idea).
