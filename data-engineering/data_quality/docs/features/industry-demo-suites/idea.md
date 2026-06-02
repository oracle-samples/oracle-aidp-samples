---
id: industry-demo-suites
name: Industry Demo and E2E Test Suites
type: Feature
priority: P1
effort: Large
impact: High
created: 2026-04-18
---

# Industry Demo and E2E Test Suites

## Problem Statement
The shipped `e2e-validation-test-suite` proves the framework works on a single
synthetic sales dataset. That is enough for a regression gate, but it does not
show how Qualifire behaves on the kinds of tables prospects actually operate
(retail, life sciences, financial services), and it does not prove every
check type behaves correctly across diverse schemas, distributions, and
failure modes.

We need industry-flavored demos that **also** act as end-to-end tests: each
demo is a self-contained pack (seeded generator + YAML/Python config + README +
HTML health report) that a reader can run, read, and adapt. The same run is
the CI regression gate.

## Decisions Locked (for plan-readiness)

These are deliberately locked in `idea.md` so `plan.md` starts with a bounded
scope. The plan may refine *how*, not *whether*.

### 0. Sequencing — runs after `pattern-check`

This feature **must run after `pattern-check` (backlog) has shipped** so the
industry demos can exercise the new `pattern` validation type alongside
the other five. Implementation on the industry packs pauses at the
harness-extract boundary (Step 1 in `plan.md`) until `pattern-check` is
merged; once merged, the coverage matrices below activate.

Rationale: the demos are the canonical proof that "every shipped validation
type works end-to-end on realistic data across backends." Shipping the
demos *without* `pattern` would either (a) lock in an incomplete matrix
that has to be amended a second time after `pattern-check` lands, or
(b) mis-represent coverage in the README and health reports. One bounded
wait is cheaper than two rounds of matrix edits.

### 1. Relationship to `e2e-validation-test-suite` — extend, do not fork
- New work lives under `tests/test_e2e_industries/<industry>/` as pytest
  modules that **reuse** the existing harness in `tests/test_e2e.py`:
  `E2EPandasBackend`, backend parametrization, system-table persistence
  pattern, and data-driven pass/fail rules (no threshold gaming).
- **No second test framework, no parallel CI gate.** The industry packs
  are added to the same `pytest` entrypoint and the same Makefile
  `test` target that gates releases today. *Clarification added during
  plan pre-flight:* the only workflow currently in
  `.github/workflows/` is `feature-review.yml`, which runs Codex
  review — not pytest. Adding a **single** pytest-running workflow
  (`tests.yml`) that invokes the same entrypoint locally used is **not**
  a "parallel CI gate" in the spirit of this rule; it is how the
  single gate reaches CI for the first time. What this rule still
  forbids: a separate industry-packs-only workflow, a separate
  industry-packs-only pytest entrypoint, or a second Make target
  that gates only the new tests.
- Shared generators, fixtures, and helpers promoted out of `test_e2e.py` into
  `tests/_e2e_support/` only as needed to avoid duplication; refactor is in
  scope.

### 2. Coverage matrix — what "all check types" means here
After `pattern-check` ships, Qualifire will ship six validation types (see
`docs/validators.md`): **SLO, Threshold, Drift, Trend, Shape, Pattern**.
Collection types: aggregation, profiling, metrics, sample, custom_query
(see `docs/configuration.md`).

**Mandatory per industry:**
- All 6 validation types exercised, each with ≥1 legitimate pass and ≥1
  legitimate fail scenario driven by generated data patterns.
- ≥1 multi-table JOIN check, implemented as a query-backed dataset
  (`DatasetConfig(query=..., dimensions=..., measures=...)` +
  `ThresholdValidationConfig`), matching the shipped
  `TestE2EQueryDataset.test_join_query` pattern. Do **not** use
  `CustomQueryCollectionConfig` for the JOIN — that collector
  aggregates to single-row SQL and no longer accepts a `filter`
  (`config.py:_reject_nonempty_filter`), so it cannot express a
  fact × dimension JOIN over source rows.
- ≥1 lateness (SLO) scenario using `max_column` recency.
- ≥1 schema-drift signal exercised through the Shape check (schema change
  between historical and current sample).
- ≥1 metric trend scenario using Prophet with seeded history.
- ≥1 outlier/anomaly scenario exercised through Shape.
- ≥1 batch-level distribution shift exercised through the Pattern check
  (Random Forest two-sample AUC above threshold, SHAP naming the driving
  features).

**Backend compatibility contract.** The shipped E2E suite documents that
Shape and multi-table query/JOIN scenarios are Pandas-only today
(`docs/features/e2e-validation-test-suite/shipped.md:45`). Pattern is
treated the same way by default — it shares Shape's sklearn + SHAP
dependency shape and its row-sampling model — and the `pattern-check`
plan may loosen this if it delivers a Spark path; if it does, this table
is updated in the same PR. Locking that in here so the matrix cannot be
silently reduced:

| Validation / scenario                     | PandasBackend | SparkBackend                  |
|-------------------------------------------|---------------|-------------------------------|
| SLO                                       | Required      | Required                      |
| Threshold                                 | Required      | Required                      |
| Drift                                     | Required      | Required                      |
| Trend (Prophet)                           | Required      | Required                      |
| Shape (Isolation Forest + SHAP)           | Required      | Allowed Pandas-only           |
| Pattern (Random Forest two-sample + SHAP) | Required      | Allowed Pandas-only           |
| Multi-table JOIN via `DatasetConfig(query=...)` | Required | Allowed Pandas-only           |

Any Spark skip must be an explicit `pytest.skip(reason=...)` with the reason
"SparkBackend not supported for <scenario> — see shipped E2E notes", not a
silent omission. If a future change removes a backend limitation, the
corresponding skip must be removed in the same PR.

**Datasets per industry (3–4, ≥30 daily partitions, few thousand rows
max each, with realistic nulls / outliers / mixed types):**

| Industry            | Datasets                                                          |
|---------------------|-------------------------------------------------------------------|
| Retail              | `sales_fact`, `inventory_fact`, `products_dim`, `stores_dim`      |
| Life Sciences       | `lab_results_fact`, `adverse_events_fact`, `patients_dim`, `sites_dim` |
| Financial Services  | `transactions_fact`, `market_prices_fact`, `accounts_dim`, `merchants_dim` |

**Explicitly out of scope:** WAP scenarios, Delta-specific recency strategies
(`delta_log`, `metadata`), non-SQLite storage backends, notification/email
delivery, any check type not in the six above. Anything added beyond the
matrix above requires a scope-guard review. Implementing `pattern-check`
itself is *not* in scope for this feature — it is a prerequisite tracked
under `docs/features/pattern-check/`.

### 3. Artifact and CI contract
Per industry, a demo produces:

| Artifact                         | Tracked location (per industry)                | Generated output location                                                              | Blocking in CI?     |
|----------------------------------|------------------------------------------------|----------------------------------------------------------------------------------------|---------------------|
| Seeded data generator            | `tests/_e2e_support/<industry>/generate.py`    | —                                                                                      | No (used by tests)  |
| Qualifire YAML config            | `examples/industries/<industry>/config.yaml`   | —                                                                                      | Lint-checked only   |
| README                           | `examples/industries/<industry>/README.md`     | —                                                                                      | No                  |
| Pytest module(s)                 | `tests/test_e2e_industries/<industry>/`        | —                                                                                      | **Yes — pass/fail** |
| HTML health report               | *(not tracked)*                                | Local: pytest `tmp_path`. CI: `artifacts/industry-reports/<industry>/report.html` | **Yes — pass/fail** (see below) |

- **Generated reports are never written into the tracked `examples/` tree.**
  Locally they are written under the pytest-provided `tmp_path` fixture so
  parallel test runs cannot clobber each other and the worktree stays clean.
  In CI they are written under `artifacts/industry-reports/<industry>/report.html`
  (a path outside the source tree) and uploaded as a build artifact. The
  tracked `examples/industries/<industry>/README.md` documents how to
  regenerate and where the output lands; it does not contain the report.
- **Report generation is a blocking assertion, not a best-effort side
  artifact.** Each industry test must (a) invoke the existing `HealthReporter`
  + `generate_health_html()`, (b) assert the returned HTML is non-empty, and
  (c) assert the output file exists and is non-empty on disk. A raised
  exception, empty HTML, or missing file fails the test. This makes the
  report a first-class deliverable consistent with the problem statement.
- **Caller must create the output directory.** The current writer in
  `qualifire/reporting/html_report.py` (both `generate_html_report` at line
  110 and `generate_health_html` at line 241) calls
  `Path(output_path).write_text(...)` with no `mkdir`, so passing a path whose
  parent does not exist raises `FileNotFoundError`. Every industry test (and
  any helper that wraps the report call) must run
  `Path(output_path).parent.mkdir(parents=True, exist_ok=True)` before
  invoking `generate_health_html`. Changing the writer itself to create
  parents is out of scope for this feature.
- **CI asserts:** all pytest modules in `tests/test_e2e_industries/` pass,
  including the report-generation assertions above.
- **Runtime budget:** ≤3 minutes per industry, ≤10 minutes total for the
  industry-demos portion of the E2E job on CI hardware. **These budgets cover
  the full backend matrix** (Pandas + Spark where required per §2), not a
  single backend pass.
- **Size budget:** ≤5 MB of generated artifacts per industry (HTML + any
  CSVs the report links to); README/config in the tracked tree are excluded
  from this budget.
- **Invocation:** `pytest tests/test_e2e_industries/ -v` runs the full set.
  A convenience `make demo-<industry>` target is nice-to-have, not required.

### 4. Data packaging — seeded runtime generation, not committed CSVs
- Generated at pytest setup time with `numpy` + fixed rotating string
  lists for any human-readable categorical columns (merchant / product
  / site labels), using a per-industry fixed `RandomState`. Matches the
  existing `test_e2e.py` pattern (numpy-only). `faker` is **not** a
  pinned repo dependency, so no new dependency is introduced for this
  feature.
- Generated data is **not** committed to the repo. Repo stays small; demos
  stay reproducible via seed.
- Regeneration workflow: edit the generator in
  `tests/_e2e_support/<industry>/generate.py`, run `pytest`; no data files to
  re-upload.
- Generation budget: <30 s per industry during test setup; ≤10k rows per
  dataset.

## Open Questions for `/feature-plan`
These are **implementation-shape** questions, not scope questions. The plan
must answer them; the answers must stay inside the decisions above.

- Exact column lists per dataset and which column drives each check type.
- Per-industry seed values and exact realistic-noise distributions (null
  rates, outlier injection rates, weekday seasonality parameters).
- Whether the 12 demos share one orchestrator fixture or each is fully
  self-contained.
- Where shared helpers land inside `tests/_e2e_support/` vs. inlined.
- Whether the existing `test_e2e.py` tests get renamed/moved for consistency
  (refactor allowed but not required).

## Affected Areas
- `tests/test_e2e_industries/` (new)
- `tests/_e2e_support/` (new shared fixtures/generators)
- `tests/test_e2e.py` (light refactor to promote shared helpers)
- `examples/industries/<industry>/` (new YAML configs and READMEs only;
  generated HTML reports are **not** written here — see §3)
- `qualifire/reporting/` (consumer of existing `HealthReporter` — no new
  public API expected)
- CI: existing Makefile `test` target + feature-review workflow (no new job)
