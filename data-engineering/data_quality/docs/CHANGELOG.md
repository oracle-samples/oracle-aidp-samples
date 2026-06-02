# Changelog

User-facing notes for breaking changes, new features, and migration
steps. Patch-level / non-user-facing changes are tracked in git only.

## Unreleased

### Fixed

- **External-catalog system table now lands in the underlying
  database, not Object Storage.** `ExternalCatalogStorage.
  _create_system_table` previously issued raw
  ``spark.sql("CREATE TABLE IF NOT EXISTS ...")`` DDL, which on
  AIDP Workbench fell through to Spark's default DataSource v1
  handler and created a Delta table in OCI Object Storage —
  even when the external catalog was mounted over Oracle ALH /
  ADW / ATP. The system table never reached the underlying
  Oracle database. Fixed by switching to
  ``empty_df.write.format("aidataplatform").mode("ignore")
  .saveAsTable(self._table)`` — the explicit AIDP data source
  forces Spark to route the create through the AIDP connector
  instead of falling through to the session-default source. The
  connector then issues a native Oracle ``CREATE TABLE`` against
  the underlying Autonomous Database, honoring the Master
  Catalog mapping for the 3-part name. **Verified end-to-end
  against a live AIDP Workbench cluster mounted over ADW: the
  system table now lands as a native Oracle table.**

  Two intermediate fixes were tried first and ruled out:
  ``empty_df.write.mode("ignore").saveAsTable(...)`` (no
  format) lowered in Spark V2 to
  ``CreateTableAsSelect(..., ignoreIfExists = true)`` — AIDP's
  connector treated the zero-row branch as a no-op.
  ``empty_df.writeTo(...).create()`` (the V2 ``CreateTable``
  primitive) required the catalog plugin to intercept CREATE
  TABLE DDL, but AIDP's external-catalog feature only wires the
  plugin for **read** paths; DDL falls through to the default
  source. Explicit ``format("aidataplatform")`` is the only
  shape that actually creates the table in ADW.

  **Backend scope narrowed: this storage backend is now AIDP-
  only.** The ``aidataplatform`` data source must be on the
  Spark classpath. Non-AIDP deployments (Unity Catalog,
  Iceberg REST, governed Hive) should use
  ``system_table_backend: delta`` or ``jdbc`` instead.
  ``docs/configuration.md`` updated to reflect the narrowed
  scope. No public-API change; no YAML change for existing
  AIDP deployments.

  Behaviour preserved across the fix: the probe-then-create
  sequence, the four-bucket error classification
  (namespace-not-found / read-only / privilege-denied /
  connector-rejected) with structured remediation hints, the
  strict 3-part identifier validation, and the
  ``skip.oos.staging=true`` writer option on subsequent
  ``insertInto`` writes. New race-window guard:
  ``TableAlreadyExistsException`` raised by ``saveAsTable``
  (when the table appears between the DESCRIBE probe and the
  create) is treated as success rather than an error. Updated
  `qualifire/storage/external_catalog.py`; mocked tests
  rewritten in `tests/test_storage/test_spark_storages.py` plus
  one new race-window test. The end-to-end real-Spark tests in
  `test_external_catalog_real.py`, `test_details_json_roundtrip.py`,
  and `test_bulk_history_contract.py` skip on local CI because
  the ``aidataplatform`` connector isn't on the classpath; they
  remain runnable inside AIDP Workbench.

### Breaking

- **Cleanup — backwards-compat shims removed
  (`cleanup-backcompat-shims`).** qualifire is pre-release with no
  external users, so every migration shim that existed solely to
  warn / re-shape old YAML or storage rows is deleted. Concrete
  removals:
  - `_REMOVED_FIELDS_GUIDANCE` + `_reject_removed_fields` (caught
    stale `suppress_repeat_alerts:`) and its 6 attach sites.
  - `_reject_legacy_backend_field` (caught the old top-level
    `backend:` YAML key).
  - `CustomQueryCollectionConfig._reject_nonempty_filter` and its
    "Evolution across review rounds" narrative.
  - `expected_value_compact: bool = True` flag — engine now
    always emits the compact `expected_value` shape.
  - Bare-number `HistoricalThresholds` form
    (`{deviation_pct: 25}`) — drop the symmetric-interpretation
    branch in `HistoricalValidator`; only signed `{min, max}`
    dicts are accepted now. README and `docs/configuration.md`
    no longer teach the dual shape.
  - Storage `COALESCE(is_active, 'true') = 'true'` defensive
    filter — every storage backend now uses plain
    `is_active = 'true'` reads. `write_results` on all 4
    backends defaults `is_active` to `"true"` when callers
    omit it, so the column is never NULL in newly-written rows.
  - "Round-N / Codex round X / pre-2026" review-history
    narrative across `qualifire/api.py`, `qualifire/core/{config,
    engine,secrets}.py`, `qualifire/notification/_redact.py`,
    `qualifire/reporting/{health,html_report,_snapshot_details}.py`,
    `qualifire/validation/{_encoding,pattern_check}.py`, and
    `qualifire/storage/external_catalog.py`. Substantive WHY
    comments preserved; only the round-labelling phrasing went.

  Tests rewritten: every `HistoricalThresholds` usage in
  `tests/` now uses `{min, max}` dicts; `test_legacy_*` test
  cases for the deleted shims (`test_legacy_bare_number_symmetric`,
  `test_legacy_rows_without_record_type`,
  `test_legacy_null_is_active_treated_active`,
  `test_legacy_backend_field_errors`,
  `TestRemovedFieldRejection`, `test_expected_value_compact_can_be_disabled`)
  deleted; the dedicated `tests/test_custom_query_filter_removed.py`
  module deleted.

- **`skip_if_cached` renamed to `skip_recollection`; eligibility
  rewritten to data-presence (`skip-recollection`).** The runtime
  flag rename is a clean break — no deprecation alias. The
  pre-pass's three layered restrictions are removed:
  (a) the `cache_eligible(val_config)` validator-type whitelist
      (Threshold / Historical / Forecast only) is **deleted**.
  (b) The dimensional-collector early-return is **removed**;
      dimensions are now enumerated from the persisted system-
      table rows at the partition anchor via a new per-backend
      `read_collection_dim_values_at_partition` helper.
  (c) The filtered-collector runtime bypass is **removed** —
      filter scope is NOT in the natural key. A row persisted
      under one filter scope replays against a current run with
      a different filter (same trade-off as `skip_revalidation`).
  Pattern / AnomalyDetection / SLO continue to fall through the
  pre-pass via the `hasattr(val_config, "expected_metrics")`
  guard. SLO is OUT OF SCOPE for skip-recollection — its
  datetime `metric_value` round-trip would type-error; captured
  as `slo-recency-skip-recollection` follow-up backlog if
  operator demand arises.

  CLI: `--skip-if-cached` → `--skip-recollection` on
  `qualifire run` and `qualifire backfill`.

  API: `Qualifire.run_config(..., skip_recollection=False)` /
  `run_config_parsed` / `backfill` / `validate` (NOT on
  `validate_query` / `write_audit_publish` — mirrors the actual
  `skip_if_cached` surface).

  Storage: new `read_collection_dim_values_at_partition` helper
  on all 4 backends (SQLite, Delta, ExternalCatalog, JDBC).
  Existing `read_collection_metric_at_partition` unchanged.

  Cleanup: `qualifire/core/backfill_eligibility.py` deleted
  (single-purpose helper, no other consumers). 2 stale test
  classes (`TestSkipIfCachedRuntimeBypass`, `TestCacheBypassScope`)
  in `tests/test_collection/test_filters.py` deleted — they
  asserted the removed contract.

- **`suppress_repeat_alerts` removed; runtime `skip_renotification`
  flag added.** The per-validation YAML field that gated duplicate-
  alert suppression on a per-rule basis is removed. The mechanism
  is replaced by a single runtime flag on `Qualifire.run_config()`,
  `Qualifire.run_config_parsed()`, `Qualifire.backfill()`,
  `Qualifire.validate()`, `Qualifire.validate_query()`,
  `Qualifire.write_audit_publish()`, and the CLI's
  `qualifire run --skip-renotification` / `qualifire backfill
  --skip-renotification`. **Default flips from `True` (per-validation
  suppress) to `False` (runtime opt-in)** — retries and replays
  re-page on every run by default; explicitly opt in with the new
  flag for backfill replays / CI rehearsals. Synthetic forensic
  rows now use `notification_status='skipped'` (was `'suppressed'`).
  YAMLs carrying the removed field fail loud at parse time with
  guidance pointing at the new runtime flag (see
  `qualifire/core/config.py:_REMOVED_FIELDS_GUIDANCE`).

### Documentation

- **Configuration reference audit (`configuration-reference-audit`).**
  `docs/configuration.md` extended from 665 → ~1100 lines.
  Adds field-reference tables (name / type / default / accepted
  forms) + worked YAML examples + 2-4 pitfall bullets for the
  10 highest-priority config surfaces: `QualifireConfig`,
  `DatasetConfig`, `ThresholdValidationConfig`,
  `HistoricalValidationConfig` (drift),
  `ForecastValidationConfig`, `PatternValidationConfig`,
  `AnomalyDetectionValidationConfig`, `SLOValidationConfig`,
  `AggregationCollectionConfig`, `SampleCollectionConfig`,
  plus nested sub-types (rules / compare / model / recency /
  history). Includes a "shared threshold bounds vocabulary"
  mini-section. The 4 less-common collectors (Profiling /
  Metrics / CustomQuery / standalone Recency) remain in the
  `configuration-reference-collectors-extension` backlog.
  Also fixes a stale `list[str]` example in
  `docs/collectors/aggregation.md` and `README.md` (the form
  was rejected at config-load months ago; the docs hadn't
  caught up). And corrects the `DatasetConfig` class
  docstring's "mutually exclusive" claim — `table` + `df`
  actually coexist (df supplies data, table supplies identity).

- **Pydantic config-class docstring audit
  (`pydantic-docstring-audit`).** Class-level docstrings
  added or extended on the 10 highest-priority Pydantic
  config classes in `qualifire/core/config.py`:
  `QualifireConfig`, `DatasetConfig`,
  `ThresholdValidationConfig`, `HistoricalValidationConfig`
  (drift), `ForecastValidationConfig`,
  `PatternValidationConfig`,
  `AnomalyDetectionValidationConfig`, `SLOValidationConfig`,
  `AggregationCollectionConfig`, `SampleCollectionConfig`.
  Each carries a one-paragraph operator-mental-model intro,
  a smallest-legal-config YAML example, and a cross-link to
  the right downstream doc. Existing substantive docstrings
  (Aggregation, Sample) merged + preserved rather than
  overwritten. Provides primary source for the deferred
  `configuration-reference-audit` backlog item.

- **Deck-content port — evaluation-grade docs surface
  (`deck-content-port-to-repo-docs`).** Ports the
  evaluator-facing content from an unversioned pitch-deck
  draft into the repo so "Why Qualifire", competitive
  positioning, and the visual architecture mental model
  live in versioned docs rather than a separate marketing
  artefact. Four surfaces touched:
  - **`README.md`** — new "Why Qualifire" section with a
    persona table (Data engineer / Pipeline owner / On-call
    / Compliance / SecOps / Platform team) inserted between
    the opening summary and `## Key Features`. No mermaid
    in README (it is the PyPI `long_description` and PyPI
    does not render mermaid).
  - **`docs/comparison.md`** — new file. Neutral-tone
    comparison against Great Expectations, Deequ / PyDeequ,
    Soda Core OSS, dbt tests + dbt-expectations, Pandera,
    and Monte Carlo / Bigeye / Anomalo. Includes a
    landscape mermaid (in-compute vs SaaS), a 7-column
    capability table, per-library prose, an explicit
    "what Qualifire is NOT trying to be" scope block, and
    a one-paragraph positioning statement.
  - **`docs/validators/README.md`** — new mermaid
    complexity ladder above the existing chooser table,
    new mermaid decision flowchart + three concrete
    scenarios (revenue ratio drop → drift; encoding
    migration → shape; correlated-columns ETL bug →
    pattern) below it, and a tradeoff summary table. The
    existing text-only chooser table is preserved
    unchanged as the canonical fallback for rendering
    surfaces without mermaid support.
  - **`docs/architecture.md`** — new `## 0. The four
    layers at a glance` mermaid decomposition (Engine →
    Collection → Validation → System table ⇄ Notification)
    before the existing value-flow §1; new "Sequence
    view" mermaid sequence diagram inside §1 showing one
    `qf.validate(...)` call; new `## 2b. Stateless vs
    stateful validators` section between §2a (backfill
    loop) and §3 with a mermaid + table + prose tying the
    read-pattern split to `qf.backfill(...)`. The opening
    scope block at lines 6-12 is updated to count the
    sections accurately (six, not the stale "three") and
    to drop the obsolete reference to the
    `architecture-backfill-loop-diagram` backlog item
    (shipped as §2a in PR #31).

- **Collectors reference extension
  (`configuration-reference-collectors-extension`).**
  `docs/configuration.md` extended with field-reference
  tables + worked YAML + Common pitfalls subsections for
  the 4 collector surfaces deferred from
  `configuration-reference-audit`:
  `ProfilingCollectionConfig` (compact field table with
  `column_profiles: dict[str, ColumnProfileOverride]`
  nested sub-table; deep examples remain in
  `## Profiling Collection`), `MetricsCollectionConfig`,
  `CustomQueryCollectionConfig` (calls out the
  `_reject_nonempty_filter` hard-fail with the exact
  config-load error message + migration recipe), and
  standalone `RecencyCollectionConfig` (quotes the exact
  per-strategy hard-fail messages from
  `_validate_strategy_fields`). The Coverage footer now
  reads "14 audited surfaces" — all collector + validator
  + dataset / root config types covered. The companion
  `docs/architecture.md` §3 is reduced to a one-line
  pointer; no remaining "What's NOT in this doc yet"
  backlog items.

- **Backfill loop architecture diagram
  (`architecture-backfill-loop-diagram`).** New "## 2a.
  Backfill loop" section in `docs/architecture.md` with an
  ASCII diagram tracing `run_backfill` →  `_resolve_scopes` →
  `_WorkUnit` build → serial / `ThreadPoolExecutor` dispatch
  → per-anchor `_process_anchor` (Pass 1a pre-engine reads,
  Pass 1b conditional bulk tombstone, single `_run_anchor_once`
  call, Pass 2 diff build) → `BackfillReport`. Prose covers
  the four operator-recurring questions: tombstone-before-
  engine ordering, the parallel-mode `notifiers={}` forcing,
  per-anchor `partition_ts` anchoring on
  `_read_original_value` / `_read_severity_before`, and why
  sample-based validators surface as `status="skipped"`. §3
  ("What's NOT in this doc yet") trimmed to the one remaining
  deferred backlog item (`configuration-reference-collectors-extension`).

### Tests / CI

- **Broken-link smoke test for live docs (`docs-lint-ci`).**
  New `tests/test_docs_links.py` walks `docs/*.md` (non-
  recursive) + `README.md`, extracts every inline markdown
  link, and asserts internal targets resolve. Catches stale
  cross-references on every test run. First-run flush:
  fixed a stale `tests/manual/dashboard_charts.ipynb` →
  `tests/manual/local/dashboard_charts.ipynb` reference in
  `docs/programmatic_api.md`.

### Bug Fixes

- **`severity_before` no longer always-None in backfill reports
  (`backfill-severity-before-broken-readback`).**
  `qualifire/core/backfill.py:_read_severity_before` previously
  called `read_validation_history(validation_name="")` which the
  storage backends' exact-match filter never accepted, so every
  backfill emitted `severity_before=None`. Knock-on:
  `_classify_diff` over-reported `refreshed` when only severity
  flipped between runs. Fix: thread real validation_names (from
  `scope.validations × expected_metrics()`) and the partition
  anchor through; reuse the `read_validations_at_partition`
  helper (skip-revalidation, PR #24) to scope the read to the
  exact partition. Multiple validators on the same metric
  aggregate via max severity (ERROR > WARNING > PASS).
  `_extract_metric_severity` rewritten symmetrically so the
  diff classification compares like-with-like.

### Documentation

- **README refresh + new `docs/architecture.md`
  (`comprehensive-config-and-architecture-docs`).** Focused
  minimum: README absorbs recently-shipped capabilities (the
  three skip-* flags, drift explainer per-slice, column-name
  redaction, backfill+soft-delete, partition tracking, dashboard,
  WAP, signed drift, rate-of-change). New
  `docs/architecture.md` traces an end-to-end value flow from
  YAML through engine to storage to next-run history read,
  documents the WAP lifecycle, and explains the rank-then-filter
  storage read contract. README's notifier-precedence claim
  corrected to two layers (YAML + programmatic) per the actual
  `_effective_notifiers` contract. New "Best Practices" section
  on README covers the runtime skip-* flags, history warm-up,
  partition_ts consistency, redaction scope, dashboard cadence,
  and WAP for partial-day backfills. Comprehensive Pydantic
  reference + 4 other deferred surfaces captured as backlog
  idea.md files (`configuration-reference-audit`,
  `pydantic-docstring-audit`,
  `architecture-backfill-loop-diagram`, `docs-lint-ci`,
  `configuration-reference-collectors-extension`).

### Enhancements

- **Skip-revalidation runtime flag (`skip-revalidation`).** New
  `Qualifire.run_config(..., skip_revalidation=False)` /
  `run_config_parsed` / `backfill` / `validate` kwarg + CLI
  `--skip-revalidation` on `qualifire run` and
  `qualifire backfill`, plus `QualifireContext.skip_revalidation`.
  When True, the engine reads all active validation rows at the
  dataset's resolved `partition_ts` for the validator's
  `validation_name` (or `validation_name.<sub>`) and replays
  them as `ValidationResult`s with
  `details["from_cache"] = True`; the validator's compute is
  bypassed. Filter-scope is NOT in the lookup key — same shape
  as `skip_recollection`. Faster retries / cheaper backfill
  replays / reproducible CI snapshots without re-running
  Prophet fits or RandomForest cross-validations whose verdicts
  are already persisted. New per-backend storage helper
  `read_validations_at_partition`. Tombstoned (`is_active=
  'false'`) rows fall through to the validator.

- **Per-past-slice breakdown for value-drift explainer
  (`drift-explainer-per-slice-breakdown`).** Opt-in
  `drift_breakdown_by_slice: bool = False` flag on
  `AnomalyModelConfig` and `PatternModelConfig`. When True, the
  first 3 entries of `details["value_drift_explainer"]` (with
  kind in {numeric, onehot, boolean}) carry a `per_slice: list`
  block — one entry per past slice in the sampler's `past_pdfs`
  order. Per-kind `current_vs_slice` shape: numeric →
  `{mean_pct, p99_pct, null_pct_abs}`; onehot → `{rate_pp}`;
  boolean → `{true_rate_pp}`. Other kinds (datetime,
  label_encoded, unknown) and unmapped/failed entries get no
  per_slice block. Per-entry truncation against
  `_PER_ENTRY_PER_SLICE_MAX_BYTES = 3072` drops the per_slice
  block (sets `per_slice_truncated=True`) when adding it would
  push the entry past 3 KB. Notification body unchanged; the
  per_slice block lands in `details_json` only.

- **Column-name redaction for SHAP / drift / schema-change emission
  (`webhook-payload-redaction`).** Operators in regulated industries
  can list sensitive source columns via
  `Qualifire(redacted_columns=[...], allowlist_columns=[...])`
  (instance level) or `DatasetConfig.redacted_columns` /
  `allowlist_columns` (dataset level); listed names are replaced
  with the placeholder `<redacted>` in
  `details["top_contributing_features"]`,
  `details["value_drift_explainer"]` (identifying fields only;
  numeric `current` / `past` / `delta` blocks pass through), and
  `pattern_check` / `isolation_forest` schema-change emission
  (lists + message string). Compute-time redaction — the redacted
  name never enters details, so all downstream egress (system-
  table write, dashboard, programmatic API, third-party webhook)
  inherits the protection without per-channel wiring. Denylist
  takes precedence over allowlist; one-sided allowlist semantics
  documented in `qualifire/core/_redaction.py`.

- **`details_json` cross-backend round-trip parity tests.** New
  `tests/test_storage/test_details_json_roundtrip.py` pins a
  kitchen-sink payload through `write_results → read_health_data`
  on SQLite (always), ExternalCatalog and Delta (when PySpark /
  delta-spark are importable), and JDBC (Spark-side serialization
  always; real-driver round-trip env-gated on
  `QUALIFIRE_JDBC_TEST_URL`). Catches a backend that mangles,
  truncates, or drops the column before it ships. The previously-
  SQLite-only `test_value_drift_explainer_roundtrips_through_details_json`
  in `test_sqlite.py` is removed (superseded by the parity suite).

- **Backfill follow-ups + polish.** Six follow-ups deferred from
  PR #13 land together. (a) `Qualifire.backfill(parallelism=N)`
  + `--parallelism N` (default 1, cap 64) — outer-anchor fan-out
  via `ThreadPoolExecutor`. Includes a contained refactor:
  `_run_anchor_once` so the engine runs **once** per anchor
  instead of once per metric. Per-anchor write/notification
  counts therefore drop in serial mode too — see "Compatibility"
  below. (b) `WAPConfig.partition_column` auto-detect from a
  bare-identifier `effective_partition_ts` (run-level
  inheritance honoured). Stricter `_BARE_IDENT_RE` than the
  existing explicit-set validator. (c)
  `read_validation_history_bulk(limit > 1)` across SQLite,
  Delta, ExternalCatalog, JDBC — two-stage CTE with
  partition-aware tombstone semantics. (d) Configurable
  `Qualifire.backfill(max_partitions=N)` + `--max-partitions N`
  (default 10000). (e) CLI: `errored` summary lines now route
  to stderr (non-`--json` mode). `BackfillReport` gains
  `notifications_suppressed: bool` so operators can detect
  when alerts were silenced under parallelism.

### Compatibility

- **Backfill engine-call count change (intentional).** With
  the engine-once refactor, `Qualifire.backfill(...)` invokes
  the underlying engine **once per anchor** instead of once per
  metric in scope. Per-anchor write/notification counts drop
  proportionally. No operator action required; the per-metric
  diffs in `BackfillReport.partitions` are unchanged.
- **`read_validation_history_bulk(limit=1)` semantics aligned.**
  Multi-row support brings the bulk path's tombstone semantics
  into line with `read_validation_history`. A tombstone for
  partition P now hides only P (not sibling partitions for the
  same key). Affects the notification-suppression caller —
  per-partition rather than per-key surfaces.

- **Interactive dashboard — click-to-expand row detail panel.** The
  per-partition history table on the validation-selected view now
  carries a `▸` toggle column that opens a per-row detail panel
  rendering the full `details_json` payload via per-validator-type
  renderers (`shape` / `pattern` / `drift` / `trend` / `threshold` /
  `slo` plus a generic JSON fallback and a distinct Qualifire-
  internal-failure renderer). Pattern / shape rows surface the
  SHAP top features parallel-zipped with the
  `value_drift_explainer` summaries; the operator no longer has
  to drop into raw storage to see *how* the column shifted. The
  static `generate_html_report` is unchanged. Snapshot
  serializer applies path-aware redaction (whole-key regex
  `^(password|secret|token|api[_-]?key|credential)$`) at non-
  explainer paths, plus a per-row 8 KB cap and a multi-stage
  8 MB total ceiling so long time-window dashboards stay
  renderable. `<script>` breakout is closed by a new
  `_safe_json_for_html_script` helper that escapes `</`, U+2028,
  U+2029, and `<!--` before SNAPSHOT interpolation. The shape
  and pattern validator pages gain a "Shared dashboards / PII"
  subsection naming the explainer category-disclosure path and
  the operator-side controls (`explain_value_drift=False`,
  `exclude_columns`, upstream filtering / hashing).

- **Per-feature value drift explainer for `shape` and `pattern`.**
  When SHAP names top contributing features in a `shape` (Isolation
  Forest) or `pattern` (Random Forest two-sample) alert,
  `details["value_drift_explainer"]` now carries a parallel-list
  per-feature value-shift summary describing *how* the column's
  distribution changed between the current sample and the union
  of past slices: numeric quantile triple + mean/std + min/max +
  null_pct, boolean true_rate, datetime ISO min/max, one-hot
  per-bin rate, label-encoded top-5 category mix. Notifier bodies
  show the top-3 summaries inline as `→ {summary}` under the
  existing `• {feature}` SHAP bullets. Schema is parallel-length
  / parallel-order with `top_contributing_features` so readers
  can `zip()` the two; over-budget entries (16 KB payload cap)
  are placeholdered with `kind="truncated"` to preserve the
  invariant. New per-validator opt-out flag
  `model.explain_value_drift: bool = True` on
  `AnomalyModelConfig` and `PatternModelConfig`. Plot helper
  `qualifire.reporting.plots.plot_value_drift(...)` for notebook
  consumption. See `docs/validators/pattern.md § Value Drift
  Explainer` for the full schema. `qualifire.validation._encoding.encode_columns`
  return shape changed from `(X, feature_names)` to
  `(X, feature_names, encoding_map)` to expose the encoder
  reverse-mapping; the encoder is internal so no external
  callers are affected.

### Breaking

- **Collector `filter:` is now AND-combined with `DatasetConfig.filter`.**
  Pre-2026-05-09, when both a dataset-level `filter:` and a
  per-collector `filter:` were set on the same validation, the
  collector filter silently overrode the dataset filter. The
  dataset-level safety guard (soft-delete exclusions, PII / tenant
  scoping, partition pinning) was dropped from that validator's
  row scope. The fix: filters AND-combine across all four
  filtered collector types — `aggregation`, `profiling`,
  `metrics`, and `sample` (the sampler always AND-combined; this
  release brings the other three in line). `custom_query` rejects
  `filter:` at config load, unchanged.

  **Composition contract**: each side's Jinja templates are
  rendered independently before composition; the resulting
  rendered strings are normalised (empty / whitespace-only →
  `None`) and then composed as `(<dataset>) AND (<collector>)`.
  Already-parenthesised operands keep their inner parens.

  **Migration**:

  - **Embrace the AND-combine.** Most operators wanted this all
    along. Thresholds may need re-tuning if the now-narrower row
    scope changes the metric value; expect first-run alert flap
    on validators where dual filters were already set.
  - **Move the broader filter into the collector.** Drop
    `DatasetConfig.filter` and put the broader predicate inside
    each validator's collector-level `filter:`. When several
    validators on the same dataset share the dataset filter and
    only one needs the override semantics, this means
    duplicating the predicate across the others.

  Worked example:

  ```yaml
  # Pre-change (override): scoped to "all returned orders globally"
  filter: "region = 'US'"
  validations:
    - type: threshold
      collection:
        type: aggregation
        expressions: { row_count: "COUNT(*)" }
        filter: "status = 'returned'"

  # Post-change (AND): scoped to "US returned orders only"
  # Same YAML, narrower scope. Counts/sums/ratios may all drop.
  ```

  Empty / whitespace-only `filter:` values now coerce to `None`
  at config load on `AggregationCollectionConfig`,
  `ProfilingCollectionConfig`, and `MetricsCollectionConfig`. The
  same normalisation applies at runtime to Jinja templates that
  resolve to empty / whitespace, so a collector
  `filter: "{{ optional_predicate }}"` with the variable
  resolving to `""` no longer produces invalid `() AND (...)`
  SQL — it composes as if no collector filter were set. Authoritative
  helper: `qualifire.collection._filters.and_combine_filters`.

  Cache interaction: `_try_skip_if_cached` now bypasses the
  cache short-circuit for any collector whose dataset or
  collector filter is set. Override-era cached metric rows (one
  filter scope) cannot silently replay against the new AND
  scope. Unfiltered collectors still benefit from the cache as
  before. A future `cache-key-filter-identity` follow-up will
  add filter-aware cache lookup so filtered collectors can
  cache too.

  See `docs/validators/validator_collector_matrix.md` ›
  Filter precedence for the full composition contract.

- **`backend:` field removed from YAML schema.** The top-level
  `backend:` field never drove runtime backend selection (it was a
  label). YAMLs that still carry it now raise `QualifireConfigError`
  at `load_config()` time with a migration message. Drop the line
  from your YAMLs and pass the runtime backend directly to
  `Qualifire.from_config(...)` or `Qualifire(backend=...)`.

- **Single notifier-precedence contract** — programmatic notifiers
  (constructor `notifiers={}`, `from_config(notifiers={})`,
  `register_notifier(...)`) ALWAYS win over a YAML
  `notifications:` block of the same name. Previous behaviour had
  YAML clobbering programmatic notifiers on every
  `run_config_parsed` call, which surprised operators who passed
  test/capture notifiers expecting them to stick. The new contract
  is consistent across all engine entrypoints
  (`run_config_parsed`, `backfill`, `validate`,
  `validate_query`, `write_audit_publish`).

- **`warn_on_reinit=True` default everywhere.** Storage-swap during
  `run_config_parsed` / `backfill` / `deactivate_metric` now emits
  `QualifireReinitWarning` by default. Pass `warn_on_reinit=False`
  to silence when the swap is intentional (e.g. CLI paths where
  construction and execution use the same YAML).

### New

- **`Qualifire.from_config(path, *, backend, **overrides)`** — new
  classmethod factory. Lifts `owner`, `bu`, `system_table`,
  `system_table_backend`, and `jdbc` from a YAML config onto the
  instance; runtime `backend` is the only required Python kwarg.
  See `docs/programmatic_api.md#initialization` for full details.

### Migration

```diff
  # config.yaml
  owner: "team"
  bu: "finance"
- backend: "spark"
  system_table: "catalog.schema.qf_history"
  system_table_backend: "external_catalog"
```

```diff
- qf = Qualifire(
-     backend=SparkBackend(spark),
-     system_table="catalog.schema.qf_history",
-     system_table_backend="external_catalog",
-     owner="team",
-     bu="finance",
- )
+ qf = Qualifire.from_config('config.yaml', backend=SparkBackend(spark))
  qf.run_config('config.yaml')
```
