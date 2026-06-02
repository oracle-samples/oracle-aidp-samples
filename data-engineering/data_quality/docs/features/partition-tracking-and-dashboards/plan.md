# Implementation Plan — Partition Tracking, Signed Drift, Descriptions, and Dashboards

## Phase 0 — NULL-safe metric coercion (defensive prerequisite)

Validators were crashing on NULL aggregation results before any of
the downstream improvements could land. Fix this first so the
notebook can run end-to-end while we iterate on the larger pieces.

**Scope gate:** every validator that does `float(cr.metric_value)`
gains a `cr.metric_value is None` guard that emits a structured
`_empty_data_result()` honouring `on_empty_data` policy.

**Files**: `qualifire/validation/{threshold,historical,forecast}.py`.

**Exit criteria**: full pytest sweep green. Existing
`test_engine_routing.py::test_sample_collection_routed` updated to
match the new contract (NULL-safe path returns WARNING, not raises).

---

## Phase 1 — Config + storage column additions

Land the schema changes that everything else builds on. Bundle so a
single `_ensure_columns` migration sweep covers all four backends in
one shot.

### 1a. New columns

Schema (`qualifire/storage/base.py`):

| Column | Type | Purpose |
|---|---|---|
| `partition_ts` | TIMESTAMP / TEXT | Resolved partition identity for the row |
| `expected_value` | STRING (JSON) | Threshold bounds, JSON-serialised |
| `actual_value_text` | STRING | Human-readable actual value (e.g. ISO duration) |
| `dataset_description` | STRING | Stamped on every row produced by the dataset |
| `validation_description` | STRING | Stamped on every validation row |

DDL updates: `SYSTEM_TABLE_DDL` + `COLUMN_DEFINITIONS` map +
SQLite/Delta hardcoded `CREATE TABLE` strings. JDBC + ExternalCatalog
already iterate over `SYSTEM_TABLE_COLUMNS` so they inherit
automatically.

Write paths: each backend's `write_results` learns to JSON-serialise
the new `expected_value` column the same way it does `details_json`.

### 1b. Config field additions

- `DatasetConfig.partition_ts: str | None = None` — Jinja-rendered
  expression evaluated by collectors / engine.
- `DatasetConfig.description: str | None = None` — propagated to
  every row via the engine.
- `DatasetConfig.name: str | None` — relax (was required) +
  `_default_name` validator that defaults `name` to `table` for
  single-table datasets. Query/wap/df-only datasets must still name
  themselves explicitly (history-key collision risk).
- `description: str | None = None` on every `*ValidationConfig`
  variant (SLO, Threshold, Historical, Forecast, AnomalyDetection,
  Pattern).
- `QualifireConfig.partition_ts: str | None` — run-level fallback.

### 1c. Model field additions

- `CollectionResult.partition_ts: datetime | None`
- `CollectionResult.dimension_value: str | None = None` (was
  `"_default"`)
- `ValidationResult.dimension_value: str | None = None` (was
  `"_default"`)
- `ValidationResult.actual_value_text: str | None = None`

`metric_key_for` and `validation_key_for` translate `None` to
`"_default"` for the read-side identity invariant (the storage
backends' `COALESCE(..., '_default') = ?` predicate).

**Exit criteria**: existing tests updated for the new defaults
(`test_models.py::test_default_dimension_value`,
`test_collectors.py::test_dimension_field_accepted_but_unused`).
Schema migration tests pass on SQLite (existing test in
`test_storage/test_sqlite.py::test_schema_migration_adds_missing_columns`
re-runs against the bigger column set).

---

## Phase 2 — Collector and engine plumbing

### 2a. AggregationCollector injects `qf_partition_ts`

When the dataset declares `partition_ts`, render it through Jinja
first, then inject `<expr> AS qf_partition_ts` into the SELECT. Pop
the synthetic column out of the metrics dict and stamp the value on
each `CollectionResult.partition_ts`. Both dimensional (GROUP BY)
and non-dimensional paths handle it. Constant expressions (e.g. a
Jinja-rendered date literal) work for non-grouped aggregations;
column refs require GROUP BY participation (we let Spark complain).

### 2b. Engine: resolve run-level partition_ts + thread descriptions

- `_resolve_run_level_partition_ts(expr, ctx)`: render Jinja, parse
  result as ISO datetime / date literal (with quote stripping). For
  literals (`'{{ ds }}'` ⇒ `'2026-04-28'`) → returns datetime; for
  column refs / SQL functions → returns None. Used as the floor for
  rows whose collector didn't produce a per-row partition_ts (SLO,
  sample-based, engine warnings, notifications).
- `_partition_ts_for_validation(vr, ds)`: match validation rows back
  to their source CollectionResult by `(metric_name,
  dimension_value)` and inherit `cr.partition_ts`. Per-row wins;
  run-level fallback when no match.
- `_build_validation_and_collection_rows`: pre-compute description
  maps and run-level partition_ts per dataset; stamp every row
  (validation + collection + engine warning + notification) with
  partition_ts, expected_value, actual_value_text,
  dataset_description, validation_description.
- `_engine_warning_row` and notification-row builder both receive
  the new column set.

### 2c. API surface

`Qualifire.validate(...)` and `Qualifire.validate_query(...)` learn
a `partition_ts: str | None = None` kwarg. Every builder method
(`slo_check`, `threshold_check`, `drift_check`, `trend_check`,
`shape_check`, `pattern_check`) learns a `description=` kwarg.

### 2d. Query-mode preserve

`_run_dataset` rebuilds `DatasetConfig` for query/df mode (swap in
the temp view as effective table); the rebuild now preserves
`partition_ts` and `description` so they survive into the rebuilt
config (otherwise query-based datasets silently lose
partition-anchored history reads).

**Exit criteria**: new test suite `test_partition_ts.py` (23 tests)
covers DatasetConfig defaults, ISO duration parsing,
`partition_lookback_anchors`, schema migration, partition-anchored
reads (daily + hourly), dimension filtering, and engine
round-trip persistence.

---

## Phase 3 — Partition-anchored history reads

### 3a. Duration parsing extended

`qualifire/core/duration.py`:

- `parse_duration` now accepts ISO 8601 (`P7D`, `PT1H`, `P2DT12H`)
  alongside the legacy compact form. Months/years rejected.
- New `partition_lookback_anchors(anchor, count, step)` helper —
  returns the list `[anchor − k·step for k in 1..count]` as
  datetimes, most-recent-first.

### 3b. New storage method on every backend

`read_metric_history_by_partition(table, metric, anchor_ts, count,
step, dimension_value="_default")` — exact-match `IN`-clause on
`anchor − k·step` for k=1..count. Dedupes per-partition_ts
(collection rows preferred over validation rows). Implemented for
SQLite, Delta, ExternalCatalog, and JDBC. Added to the protocol.

### 3c. Validators switch to partition-anchored when possible

`HistoricalValidator._validate_one` and
`ForecastValidator._validate_one`: when `cr.partition_ts is not None
and step is set`, call `read_metric_history_by_partition`. Otherwise
fall back to `read_metric_history` (legacy run_timestamp ordering).
Pass `cr.dimension_value or "_default"` to storage so non-dimensional
rows still match the read predicate.

**Exit criteria**: `test_partition_ts.py` covers daily and hourly
lookback patterns end-to-end.

---

## Phase 4 — Signed drift + rate-of-change

### 4a. Validator math

`HistoricalValidator._validate_one`:

- `deviation_abs` = `current - mean_past` (signed; was `abs(...)`)
- `deviation_pct` = `deviation_abs / abs(mean_past) * 100` (signed)
- `z_score` = `(current - mean_past) / stddev` (signed; was
  `abs(...)`)
- New: `rate_of_change_abs` = `current - prev_value` (signed; vs.
  immediate prior partition `past_values[0]`)
- New: `rate_of_change_pct` = same scaled to %

### 4b. Threshold comparison logic

`HistoricalValidator._check_thresholds` rewritten to support both
forms per measure:

- Bare number (`{deviation_pct: 25}`) → symmetric absolute legacy
  semantic (`abs(value) > 25` ⇒ violated). Preserves existing
  industry configs untouched.
- Dict (`{deviation_pct: {min: -10, max: 25}}`) → asymmetric signed
  bounds. Either bound optional (min-only / max-only).

`HistoricalThresholds.warning` / `.error` Pydantic types relaxed
from `dict[str, float]` to `dict[str, float | dict[str, float]]`.

### 4c. Validation result enrichment

`expected_value` on the persisted ValidationResult now carries the
full thresholds object (warning + error bounds) rather than just
`{mean, stddev}`. Mean/stddev/past_values/all-measure-values move to
`details`.

**Exit criteria**: new `test_signed_drift.py` (11 tests) covers
signed deviation, signed z-score, rate-of-change pct/abs, legacy
bare-number bound (back-compat), `{min, max}` form, min-only,
max-only.

---

## Phase 5 — SLO ISO 8601 freshness output

`format_duration_iso(td)` helper added to
`qualifire/core/duration.py`. SLO validator emits:

- `metric_value` = numeric seconds (lands in `metric_value` column).
- `actual_value_text` = ISO duration `"P1DT2H30M15S"` (lands in
  `actual_value_text` column).
- `expected_value` = `{warning: ISO_DURATION, error: ISO_DURATION}`.

Operators get both forms — numeric for charts/SQL, ISO string for
human-readable display.

---

## Phase 6 — Dark-mode adaptive HTML

`qualifire/reporting/html_report.py`:

- CSS variables (`--bg`, `--surface`, `--text`, `--border`,
  `--header-bg`, `--row-hover`, `--bar-track`, etc.) defined under
  `:root`.
- `@media (prefers-color-scheme: dark)` overrides each variable.
- Both `generate_html_report` (per-run) and `generate_health_html`
  (aggregate) use the same variable set.
- Severity colours unchanged (already legible on both backgrounds).

---

## Phase 7 — Interactive HTML dashboard

New `generate_interactive_html(storage, output_path, days)` in
`html_report.py`:

- Reads `storage.read_health_data(days)`, normalises into a JSON
  snapshot, embeds in the HTML.
- Plotly via CDN (`https://cdn.plot.ly/plotly-2.35.2.min.js`).
- Controls: time-range input, dataset selector, validation selector,
  reset button.
- Charts: severity pie, daily pass-rate trend with overlay total
  bars, per-validation history line with severity-coloured markers
  and dimension-grouped traces.
- Tables: dataset list (drill into a dataset by clicking),
  validation list (drill into a validation), per-validation runs.
- Context panel: surfaces dataset_description and validation_description
  when drilled in.
- Dark/light auto-adapts via the same CSS variables as the static
  reports.

`Qualifire.interactive_dashboard(output_path, days)` API method
wires it into the public surface.

`read_health_data` extended on all four backends to return:
`partition_ts`, `metric_value`, `expected_value`, `actual_value_text`,
`validation_message`, `dataset_description`, `validation_description`,
`dimension_value`, `table_name` — everything the dashboard consumes.

---

## Phase 8 — Manual notebooks

Four notebooks under `tests/manual/`:

### `pyspark_setup.ipynb`
Setup helper. Covers venv creation, VS Code kernel selection, the
macOS Downloads-folder `PermissionError` gotcha, and a runtime
fallback that pip-installs pyspark + scrubs Downloads from
`sys.path` if needed.

### `notebook.ipynb`
The full smoke walkthrough:
- Sections 0–1: setup, JDK selection (Java 8/11/17 only — Java 24
  rejected because Hadoop 3.3.4 calls the removed
  `Subject.getSubject(AccessControlContext)`), Spark session
  bootstrap with metastore URL pointed at WORK_DIR (avoids the
  Derby lock issue), Qualifire init.
- Section 2: per-partition helpers (`seed_partition`,
  `validate_partition`, `process_partition`) — usable in a loop or
  manually.
- Section 3: SLO + threshold combo with partition_ts.
- Section 4: threshold via `validate_query` with NULLIF + NULL-safe
  validators.
- Sections 4a–4e: drift / forecast / shape (Isolation Forest +
  SHAP) / pattern (RF + SHAP) / profiling / custom_query / metrics.
- Sections 5–7: Jinja templating, system table inspection,
  notifications.
- Sections 8 + 8a: static health dashboard + interactive dashboard.
- Section 9: cleanup (including stray `metastore_db` removal).

### `dashboard_html.ipynb`
Programmatic HTML dashboards. Multi-backend (sqlite / jdbc /
external_catalog / delta) via a `make_storage(...)` factory. Renders
health + interactive + per-run reports to a user-supplied
`OUTPUT_DIR`. Configuration is in-cell (AIDP has no terminal);
env vars are a fallback.

### `dashboard_charts.ipynb`
Visualizations via matplotlib + plotly at two granularities:
- Executive (sections 3–4): cards + daily trend + worst offenders
  in matplotlib (PNG export); donut + interactive trend in plotly.
- Steward (sections 5–7): heatmap (matplotlib), per-validation
  history with severity-coloured markers (plotly), sunburst +
  treemap (plotly) for hierarchical breakdown.
- Section 8: severity distribution (matplotlib stacked bar) +
  metric-value violin (plotly).

---

## Phase 9 — Docs + tests

### Tests added
- `tests/test_partition_ts.py` (23 tests) — partition tracking
  end-to-end.
- `tests/test_signed_drift.py` (11 tests) — signed math + rate of
  change + threshold bound forms.
- `tests/test_columns_descriptions.py` (4 tests) — new column
  persistence: SLO ISO duration, NULL dimension, partition_ts
  run-level fallback, expected_value JSON serialization.
- `tests/test_interactive_dashboard.py` (4 tests) — descriptions
  round-trip, interactive HTML generation, empty-storage fallback,
  Qualifire method wrapper.
- `tests/test_validation/test_null_safe.py` (4 tests) — NULL-safe
  threshold / historical / forecast validators.

### Tests updated
- `tests/test_models.py::test_default_dimension_value` — now expects
  `None`.
- `tests/test_collection/test_collectors.py::test_dimension_field_accepted_but_unused`
  — now expects `None`.
- `tests/test_engine_routing.py::test_sample_collection_routed` —
  updated to expect WARNING result rather than raise.

### Docs updated
- `README.md` — Key Features section gains bullets for partition_ts,
  signed comparisons + rate of change, descriptions, interactive
  dashboard. Drift example updated to use ISO step (`P7D`) and the
  asymmetric `{min, max}` bound form. Architecture tree note
  updated.

---

## Hard-stop conditions (any one means stop and revisit)

- Any pre-existing test fails after a phase. Investigate before
  proceeding.
- Schema migration on a non-empty existing system table loses
  rows. (Verified via the existing
  `test_schema_migration_adds_missing_columns`-style test.)
- A backend's `read_metric_history_by_partition` returns rows
  outside the requested anchor set.
- The interactive dashboard fails to render in VS Code's notebook
  webview (CSP / iframe sandbox issues).

## Phase 10 — Post-capture polish

Items surfaced after running the manual notebook end-to-end and
inspecting the persisted system table.

### 10a. Notebook bug — MetricsCollectionConfig

`tests/manual/notebook.ipynb` cell `47468799` (section 4e —
profiling/custom-query/metrics) passed a list to
`MetricsCollectionConfig.metrics`; the field is `dict[str, str]`
(name → SQL expression). Fixed in-place to a dict with three
sample metrics: `row_count`, `non_null_amount`, `distinct_pids`.

### 10b. Interactive dashboard inline rendering

`generate_interactive_html(...)` gains an `inline_plotly_js: bool =
False` parameter. When True, the HTML embeds the full Plotly bundle
inline (~3.5 MB) instead of referencing the CDN — required for
inline rendering inside VS Code's notebook webview, which sandboxes
the iframe and blocks external `<script src="cdn.plot.ly...">`
loads. CDN remains the default for files served through a real
browser.

`Qualifire.interactive_dashboard(...)` mirrors the new flag.

The notebook cell `c1ca3576` updated to:
1. Save a CDN-based copy to disk (small file, full interactivity in
   any browser).
2. Open the saved file via `webbrowser.open(f"file://{path}")` so
   users get the real interactive view automatically.
3. Embed the inline-Plotly form via `display(HTML(...))` so the
   notebook output is also interactive without leaving the kernel.

### 10c. Two-dimension regression test

`tests/test_partition_ts.py` gains
`TestTwoDimensionalCollection` — two tests covering an
`AggregationCollector` configured with `dimensions=['region',
'product']`:

- **Collector emits per-dim-combo**: 4 combos × 1 metric = 4
  results; each carries the same partition_ts (constant expression
  evaluates uniformly across the GROUP BY rows); each
  `dimension_value` is a JSON-encoded `{region, product}` object
  with sorted keys (write/read consistency invariant).
- **Per-segment history reads**: SQLite
  `read_metric_history_by_partition` returns rows for the requested
  dim combo only. Partial-key queries (e.g. only `region`) return
  zero rows — confirms `dimension_value` is treated as an opaque
  string identity, not a partial JSON match.

### 10d. Clarifications surfaced during integration

These were observations from running against a populated system
table; documenting them here so they don't recur as bug reports:

- **`validation_name` is NULL on collection rows.** This is by
  design. `record_type='collection'` rows are metric collection
  events (one CollectionResult per (metric, dim) emitted by the
  collector); they aren't tied to any single validation, so
  `validation_name` stays NULL. The corresponding validation rows
  (`record_type='validation'`) carry the validation_name. Filter on
  `record_type` when you want one or the other.
- **`partition_ts` NULL on engine-warning + notification rows is
  also by design.** Those rows are run-scoped events
  (`qualifire.persistence`, `qualifire.suppression_read`,
  notification audit), not partition-scoped. Validation and
  collection rows always carry `partition_ts` when the dataset
  declared `partition_ts` (per-row from the aggregation collector,
  or run-level fallback for SLO/sample/empty-data flows).
- **`dimension_value` NULL means "no dimension configured".** The
  dataclass default is `None`; storage persists it as SQL NULL.
  All read predicates use `COALESCE(dimension_value, '_default') =
  ?` so dimensional and non-dimensional rows compose correctly. The
  `_default` literal only appears as a read API parameter (or
  legacy data); operators inspecting raw rows should expect NULL
  for the no-dimension case.
- **Stale rows from earlier runs may still show
  `dimension_value='_default'`** after upgrade. The schema
  migration adds new columns but does not rewrite existing rows.
  After upgrading, either truncate-and-rerun the system table for a
  clean view, or accept the mixed state — reads handle both
  uniformly via the COALESCE invariant.
- **VS Code notebook kernels don't auto-reload edited package
  modules.** After any `qualifire.*` source edit, restart the
  kernel before re-running notebook cells. Otherwise users see
  `unexpected keyword argument 'description'`-style errors against
  the cached old signature.

### 10e. Post-capture exit criteria

- 812 tests green (810 from the original Phase 0–9 + 2 new
  two-dimension tests in Phase 10c).
- Manual notebook runs end-to-end with no errors after kernel
  restart, including the interactive dashboard cell rendering
  inline (via `inline_plotly_js=True`) AND opening in a browser
  via `webbrowser.open(...)`.
- `tests/test_feature_workflow_hygiene.py::test_no_untracked_shipped_md_files`
  passes: no premature shipped.md files in the feature directory.

---

## Exit criteria (cumulative)

- All 812 tests green (683 prior + 129 added/updated across the
  six new test files and 3 updated tests).
- Industry e2e suite (81 tests) green.
- Manual notebook runs end-to-end on macOS Corretto 8 + Spark
  3.5.0, including the metrics cell (Phase 10a fix).
- Interactive dashboard renders correctly in VS Code (via inline
  Plotly), classic Jupyter, and a standalone browser (CDN).
- No premature shipped.md leakage (hygiene test passes).
