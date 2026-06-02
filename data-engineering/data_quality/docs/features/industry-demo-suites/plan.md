---
started: 2026-04-18
---

# Implementation Plan: Industry Demo and E2E Test Suites

## Overview

Extend the shipped E2E harness (`tests/test_e2e.py`) with three industry packs
(Retail, Life Sciences, Financial Services). Each pack is a self-contained
pytest module that generates seeded data, runs the six validation types with
legitimate pass/fail scenarios, and produces a blocking HTML health report —
exactly the deliverables locked in `idea.md`.

This plan converts the locked decisions in `idea.md` into an auditable
coverage matrix, names concrete columns and SQL, and stages the work so the
shipped E2E suite never regresses while helpers are extracted.

**Sequencing (from `idea.md` §0).** Step 1 (harness extraction) may proceed
immediately; Steps 2–7 are **blocked on `pattern-check` shipping** because
the Pattern rows below depend on the new validator existing. When
`pattern-check` merges, unblock Steps 2–7 in the same PR as the first
industry pack.

**Pattern-check has shipped (2026-04-21, PR #3 merged).** The sequencing
gate is satisfied; Steps 2–7 can now land in this feature branch.

## Pre-flight reconciliation with current repo state

Re-scanned immediately before starting this branch. Adjustments to the
original plan draft (kept in-line so reviewers can diff intent vs. reality):

- **E2E test count gate is 33, not 27.** `pytest tests/test_e2e.py
  --collect-only` reports 33 tests today (pattern-check added 6:
  `test_pattern_pass_same_distribution`,
  `test_pattern_alerts_when_current_shifted`,
  `test_pattern_persists_to_system_table`,
  `test_all_six_validator_types_coexist`,
  `test_shape_and_pattern_share_sample_collection`,
  `test_sample_cache_fallback_shares_single_collection_after_prime_failure`).
  Step 1's gate is updated below.
- **`faker` is NOT pinned anywhere.** Not in `requirements*.txt`, not in
  `pyproject.toml` extras, not in `setup.py`. The industry generators use
  **numpy only** — the same pattern as the existing `_generate_daily_sales`
  / `_generate_products` helpers in `tests/test_e2e.py`. If readable names
  are needed for a dimension table (e.g. merchant name), use a fixed
  rotating list of strings; do not introduce a new dependency under the
  cover of this feature.
- **`make install-all` is currently broken** — it references
  `requirements-all.txt`, which does not exist. Fixing that target is
  out of scope here; the new tests workflow must call `pip` directly
  (see Step 6) so this feature does not inherit a pre-existing bug.
- **`pyspark` has no pin** in any requirements file or `pyproject.toml`
  extra. The existing shipped E2E suite depends on a locally-installed
  `pyspark` today. CI must install it explicitly in the new workflow;
  we do **not** change repo-wide pins in this feature (scope gate).
- **`read_health_data` filters on `datetime('now', -<days> days)`.** Every
  industry test run produces real-time `run_timestamp`s, so the pack's
  report populates from the current run regardless of the generated
  data's `sale_date` values. Seed dates in the generator data are for
  partition-level tests (SLO max_column, Drift/Trend history windows),
  not for the health report filter.
- **Prophet `history_count` defaults to 90**, not 30. Every Trend row
  in the coverage matrices below uses `history_count: 30` explicitly in
  its config so the seeded 30-day history is the full training window.
- **JOIN scenarios use `DatasetConfig(query=...)`, not
  `CustomQueryCollectionConfig`.** The `custom_query` collection
  aggregates single-row SQL and no longer accepts a `filter` (migration
  error, `config.py` `_reject_nonempty_filter`); the JOIN shape that
  matches the existing shipped `TestE2EQueryDataset` pattern is a
  dataset-level `query=` with `dimensions` + `measures` and a
  `ThresholdValidationConfig`.
- **Pattern `history` spec has a length-match validator.** When
  `history.past_dates` is explicitly set and `history.filters` is a list,
  `PatternValidationConfig._validate_history_filters_length` rejects
  mismatched lengths. The pattern configs below set both with matching
  length (or set only one, not both).

## Implementation Steps

- [ ] **Step 1 — Scaffold `tests/_e2e_support/` and extract shared helpers**
  Move reusable pieces out of `tests/test_e2e.py` **without changing any
  test behavior**:
  - `tests/_e2e_support/__init__.py` (empty)
  - `tests/_e2e_support/backends.py`: `_SparkRow`, `E2EPandasBackend`,
    `_make_spark_backend`, `_make_backend`, `_make_storage`, `_run`,
    `_seed_history`
  - `tests/_e2e_support/reporting.py`: `render_report(storage, out_path)`
    helper that (a) creates parent directories with
    `Path(out_path).parent.mkdir(parents=True, exist_ok=True)`, (b) calls
    `HealthReporter(storage).generate(days=30)` and
    `generate_health_html(report, output_path=str(out_path))`, (c) returns
    the HTML string and asserts non-empty.
  - Update `tests/test_e2e.py` to import from `_e2e_support`; leave every
    test body unchanged.
  - Gate: `pytest tests/test_e2e.py -v` must show the **same 33 passing
    tests** as the current baseline on `main` (original shipped count
    of 27 grew to 33 after pattern-check PR #3 merged on 2026-04-21 —
    see `docs/features/e2e-validation-test-suite/shipped.md:25` for the
    original baseline and `docs/features/pattern-check/shipped.md` for
    the 6 pattern-related additions). Record the exact pass count in
    the commit message so CI artifact drift is reviewable.

- [ ] **Step 2 — Retail pack**
  - Generator: `tests/_e2e_support/retail/generate.py` (seed `2026`)
  - Tests: `tests/test_e2e_industries/retail/test_retail_e2e.py`
  - YAML config: `examples/industries/retail/config.yaml`
  - README: `examples/industries/retail/README.md`
  - Must satisfy the coverage matrix in §"Retail coverage" below.

- [ ] **Step 3 — Life Sciences pack**
  - Generator: `tests/_e2e_support/life_sciences/generate.py` (seed `2027`)
  - Tests: `tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py`
  - YAML config: `examples/industries/life_sciences/config.yaml`
  - README: `examples/industries/life_sciences/README.md`
  - Must satisfy §"Life Sciences coverage".

- [ ] **Step 4 — Financial Services pack**
  - Generator: `tests/_e2e_support/financial_services/generate.py` (seed `2028`)
  - Tests: `tests/test_e2e_industries/financial_services/test_financial_services_e2e.py`
  - YAML config: `examples/industries/financial_services/config.yaml`
  - README: `examples/industries/financial_services/README.md`
  - Must satisfy §"Financial Services coverage".

- [ ] **Step 5 — Shared storage and one-report-per-industry contract**
  One `report.html` per industry must reflect the **entire** pack's run,
  not the last test's storage. The pack therefore shares a single
  `SQLiteStorage` across all tests, and exactly one report is generated
  at pack teardown.
  - **Make each industry directory a real Python package** before any
    fixture work, so `scope="package"` has a defined boundary. This
    mirrors the existing pattern in `tests/test_reporting/__init__.py`
    and `tests/test_validation/__init__.py`. Create:
    - `tests/test_e2e_industries/__init__.py` (empty)
    - `tests/test_e2e_industries/retail/__init__.py` (empty)
    - `tests/test_e2e_industries/life_sciences/__init__.py` (empty)
    - `tests/test_e2e_industries/financial_services/__init__.py` (empty)
    Without these, `scope="package"` has no meaningful edge and the
    "one report per industry" guarantee becomes implementation
    guesswork.
  - Add `tests/test_e2e_industries/conftest.py` with **pack-scoped**
    fixtures keyed on `industry`:
    - `industry_storage(industry)` — pack-scoped `SQLiteStorage` used by
      every test in the pack so all collection/validation rows land in
      the same system table.
    - `industry_report_path(industry, tmp_path_factory)` — returns
      `tmp_path_factory.mktemp("reports") / f"{industry}_report.html"`
      locally, and
      `Path(os.environ["QUALIFIRE_ARTIFACTS_DIR"]) / "industry-reports" / industry / "report.html"`
      when `QUALIFIRE_ARTIFACTS_DIR` is set.
    - `industry_report(industry_storage, industry_report_path)` —
      pack-scoped fixture with a *finalizer* that runs once per pack:
      calls `render_report(industry_storage, industry_report_path)`,
      asserts the HTML is non-empty, the file exists, and the file is
      non-empty on disk. This is the **single** report-producing step
      per industry.
    - `industry_data(industry)` — **pack-scoped** fixture that invokes
      the per-industry generator **exactly once per pytest session
      per pack** and returns the in-memory DataFrames used by every
      test in the pack. Test modules must NOT re-invoke the generator
      from function-scoped fixtures or inside test bodies — doing so
      would re-enter the seeded `numpy.random.RandomState` and break
      reproducibility across runs where test ordering differs. The
      contract is: **one generator call per pack run, full stop.**
      If a test needs to mutate a copy of the data (e.g. Shape's
      "corrupt current sample" scenarios), it takes a deep copy from
      `industry_data` and mutates locally; the original frames stay
      untouched for the remaining tests.
  - **Activation is enforced by the harness on default invocations.**
    Each industry package —
    `tests/test_e2e_industries/<industry>/conftest.py` — declares
    `industry_report` as `@pytest.fixture(scope="package", autouse=True)`
    (or redeclares the shared fixture with `autouse=True` at the package
    level) so the fixture is requested for **every** test collected
    under that package, regardless of whether any test function
    references it. Relying on `pytestmark =
    pytest.mark.usefixtures("industry_report")` in each test module is
    rejected: a new module could forget it and the failure would only
    surface as a missing CI artifact. The autouse package-scoped
    declaration makes forgetting impossible **on default invocations**.
    Limit: pytest only runs a package-scoped finalizer if at least one
    test from that package is actually collected and executed — so
    `--deselect`, a full-package skip, or a typo that excludes all
    tests from collection would silently skip report generation. CI
    covers this by asserting, after the run, that every industry's
    `report.html` exists and is non-empty on disk (see Step 6); the
    guarantee is "default invocation produces every report," not
    "report generation is impossible to skip."
  - Per-test report generation is forbidden. Individual tests only
    record results into `industry_storage`; the pack-level finalizer
    produces the one canonical `report.html`.
  - `render_report` still owns `Path(out_path).parent.mkdir(parents=True,
    exist_ok=True)` before calling `generate_health_html`.
  - **Mid-pack test failures still emit the report.** pytest runs
    fixture finalizers in LIFO order after a test raises, including
    package-scoped ones at teardown. A failing test therefore does
    NOT prevent `industry_report` from writing `report.html` — the
    Step 6 artifact assertion loop will still find the file, and
    the underlying test failure is the one CI surface. Concretely:
    if `test_X[spark]` fails, pytest still unwinds package teardown,
    calls the finalizer, writes the non-empty HTML, and exits
    non-zero. The sole case where the finalizer does not run is
    when zero tests in the package are collected at all (covered
    by the Step 6 count assertion).

- [ ] **Step 6 — CI wiring via a new test workflow**
  The only existing workflow (`.github/workflows/feature-review.yml`)
  runs Codex review, not pytest, so it cannot own the test gate. Add a
  new workflow that does:
  - `.github/workflows/tests.yml`:
    - Triggers: `push` (all branches) and `pull_request`.
    - Job `tests`: `ubuntu-latest`, checkout, set up Python 3.11.
    - **Install step** (explicitly does NOT use `make install-all` — that
      target references a nonexistent `requirements-all.txt` on `main`;
      fixing it is out of scope for this feature):
      ```
      pip install -e ".[all,dev]"
      pip install "pyspark>=3.5"
      ```
      The `all` extra (declared in `pyproject.toml`) pulls `pandasql`,
      `prophet`, `scikit-learn`, and `shap`; `dev` adds `pytest`,
      `pytest-mock`, `pytest-cov`. `pyspark` is installed separately
      because it is unpinned in the repo today and Spark parametrizations
      in `test_e2e.py` + the industry packs require it. If a future
      feature pins `pyspark` centrally, remove the extra `pip install`
      here in the same PR.
    - **Environment**: export
      `QUALIFIRE_ARTIFACTS_DIR=${{ github.workspace }}/artifacts`
      before the test step so the pack-scoped report fixture writes
      under a deterministic path.
    - **Spark session pre-warm** (addresses Round 4 B9): a
      session-scoped `_spark_session` fixture in
      `tests/_e2e_support/backends.py` must be declared `autouse=True`
      at the `tests/test_e2e_industries/conftest.py` package level so
      the JVM startup cost (≈8–12 s on `ubuntu-latest`) is amortized
      across **all three packs** instead of being charged to whichever
      pack pytest happens to collect first. Concretely, the conftest
      declares a session-scoped fixture that requests `_spark_session`
      in its body; pytest instantiates it once, before any pack's
      package-scoped fixtures resolve, and the JVM boot appears in a
      time bucket that is NOT attributed to any industry in the JUnit
      XML (the Spark fixture has no `tests/test_e2e_industries/<key>`
      file attribute). Without this, whichever pack runs first would
      absorb ~10 s of startup cost and could blow the 180 s per-pack
      budget on a coincidence of collection order. Verification:
      Step 7's per-pack tally must sum to a total that is within a
      few seconds of pytest's own total wall clock; if the gap is
      larger than ~15 s, the pre-warm fixture isn't resolving first
      and the asymmetry has returned.
    - **Test step**: `pytest tests/ -v --junitxml=artifacts/junit.xml`
      (equivalent to `make test`; we invoke pytest directly so the
      `QUALIFIRE_ARTIFACTS_DIR` export is unambiguously in scope and
      independent of Makefile changes). The JUnit XML is what the
      Step 7 duration check parses — **it does not re-run the packs
      separately**. That avoids doubling the CI cost Codex Round 3
      flagged: the industry tests run exactly once, and the
      per-pack wall clock is attributed by summing
      `testsuite/testcase/@time` in the JUnit output per
      `tests/test_e2e_industries/<industry>/` path prefix.
    - **Post-test per-industry assertion step** (runs between pytest
      and upload, fails the job if any industry report is missing
      or the per-industry artifact tree exceeds the 5 MB budget from
      `idea.md` §3):
      ```bash
      for dir in retail life_sciences financial_services; do
        path="${QUALIFIRE_ARTIFACTS_DIR}/industry-reports/${dir}/report.html"
        test -s "$path" || { echo "::error::missing or empty report at $path"; exit 1; }
        # Enforce idea.md §3 per-industry size budget (5 MB).
        bytes=$(du -sb "${QUALIFIRE_ARTIFACTS_DIR}/industry-reports/${dir}" | awk '{print $1}')
        max=$((5 * 1024 * 1024))
        if [ "$bytes" -gt "$max" ]; then
          echo "::error::${dir} artifact tree is ${bytes}B, budget is ${max}B (5 MB)"
          exit 1
        fi
      done
      ```
      `upload-artifact@v4` with `if-no-files-found: error` only proves
      *at least one* file exists; the loop above proves exactly the
      three expected files exist and are non-empty. Both run in that
      order — the count gate fails first if any pack regressed.
    - Upload `artifacts/industry-reports/**` as the
      `industry-reports` artifact using `actions/upload-artifact@v4`
      with `if-no-files-found: error` as a defense-in-depth backstop
      (the count gate above is the primary enforcement).
  - `.gitignore`: add `artifacts/` so local runs that set
    `QUALIFIRE_ARTIFACTS_DIR=./artifacts` don't dirty the worktree.
  - `.github/workflows/feature-review.yml` is **not** modified by this
    feature.

- [ ] **Step 7 — Runtime and coverage verification (without dropping required coverage)**
  - **Binding budget = CI-measured wall clock**, not local hardware.
    The `tests.yml` job parses the JUnit XML produced by the single
    `pytest tests/ -v --junitxml=artifacts/junit.xml` run in Step 6 —
    **the industry packs are not re-executed**. The duration-enforcing
    step runs *after* the main pytest step (so functional failures
    surface first) with this shell snippet:
    ```bash
    python3 <<'PY'
    import os, sys, xml.etree.ElementTree as ET
    tree = ET.parse("artifacts/junit.xml")
    per_pack = {"retail": 0.0, "life_sciences": 0.0, "financial_services": 0.0}
    matched = 0
    for case in tree.iter("testcase"):
        # Prefer `file` (pytest >=6); fall back to `classname` (always
        # emitted, dotted module path) so Codex Round 4's "vacuous-pass
        # when file attr absent" can't silently zero-attribute the packs.
        path = case.attrib.get("file") or case.attrib.get("classname", "")
        path = path.replace(".", "/")  # normalise classname dots to /
        for key in per_pack:
            if f"tests/test_e2e_industries/{key}" in path:
                per_pack[key] += float(case.attrib.get("time", "0"))
                matched += 1
                break
    if matched == 0:
        print("::error::no industry testcases found in junit.xml — "
              "refusing to pass the runtime gate vacuously")
        sys.exit(1)
    total = sum(per_pack.values())
    lines = [f"- {k}: {v:.1f}s" for k, v in per_pack.items()]
    lines.append(f"- total: {total:.1f}s")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as fh:
            fh.write("## Industry pack runtimes\n" + "\n".join(lines) + "\n")
    else:
        # Local invocation — print instead of KeyError.
        print("Industry pack runtimes:\n" + "\n".join(lines))
    failed = False
    for k, v in per_pack.items():
        if v > 180:
            print(f"::error::{k} pack took {v:.1f}s, budget 180s")
            failed = True
    if total > 600:
        print(f"::error::industry packs total {total:.1f}s, budget 600s")
        failed = True
    sys.exit(1 if failed else 0)
    PY
    ```
    Two defenses against silent-pass from Round 4:
    1. The `matched == 0` guard fails the job if zero industry
       testcases were attributed (JUnit dialect emits neither `file`
       nor a usable `classname`) — no vacuous passes.
    2. The fallback to `classname` covers pytest versions that omit
       `file`; classname dots are normalised to slashes so the prefix
       match still works.
    The explicit `[ v > 180 ]` / `[ total > 600 ]` checks are the
    binding gates — not a GitHub step-level `timeout-minutes`, which
    would kill the job mid-test without a clean failure message.
    Local `pytest --durations=0` output is advisory only — copied
    into the industry README for documentation, not a gate.
  - **The 180 s per-pack budget is a ceiling inherited from
    `idea.md` §3, NOT an empirically measured target.** No pack has
    been run end-to-end on `ubuntu-latest` yet, and Financial
    Services carries the heaviest workload (7,000-row
    `transactions_fact` + log-normal amount generation + Pattern/Shape
    over a wide-feature table with >20-cardinality `merchants_dim`).
    If the first CI run shows FS between 150 s and 180 s, no action
    is required — it is within budget. If FS exceeds 180 s, apply
    the Step 7 knobs in order (row-count reduction → shared Spark
    session → collapse redundant backend parametrizations for
    non-mandatory rows only) before considering any budget change.
    The budget itself **does not get widened silently** — any
    widening past 180 s per pack requires a `idea.md` amendment
    first, because it is a locked contract there, and a scope-guard
    review in the PR that proposes it. "Keep the budget, trim the
    work" is the default response; "raise the budget" is the
    exception.
  - If any industry exceeds 3 min, **required** scenarios (every row of
    that industry's Checks table) stay in the default gate. Only these
    knobs are allowed to bring runtime back under budget, in order:
    1. Reduce dataset row counts (keep ≥30 daily partitions — the hard
       floor from `idea.md`).
    2. Share Spark setup more aggressively: one session-scoped
       `SparkSession` per test session (promoted into
       `tests/_e2e_support/backends.py`) instead of per-module.
       **Session reuse must not imply temp-view reuse.** The existing
       helper at `tests/test_e2e.py:121` registers views via
       `createOrReplaceTempView(name)` with fixed names and no cleanup,
       which would cause order-dependent leakage across tests once the
       session is long-lived. When the shared session lands in
       `_e2e_support`, `_make_spark_backend` must either (a) namespace
       every registration per test with a unique view name
       (e.g. `f"{base_name}_{request.node.callspec.id}"`) so concurrent
       or sequential tests never alias the same view, **or** (b) have
       every test register via a helper that wraps the call in a
       teardown that runs `spark.catalog.dropTempView(name)` for each
       registered name before and after the test. Option (a) is
       preferred because it makes collisions impossible by construction;
       option (b) is only acceptable if the test explicitly records the
       view names it created. Silent reuse of fixed view names across
       tests on a shared session is forbidden.
    3. Collapse duplicate Pandas+Spark parametrizations for a single
       scenario into one backend when the other adds no signal, but
       only for scenarios **not** listed as mandatory in `idea.md` §2.
    A `slow` marker is permitted **only** for *extra* scenarios beyond
    the mandatory matrix; it must never shadow a required check, and
    the default CI gate must continue to run every row of every
    industry's Checks table.
  - Run the **Acceptance checklist** in §"Acceptance" against each pack.

## Coverage matrices

The idea locks: all 6 validation types × ≥1 pass + ≥1 fail per industry,
≥1 multi-table JOIN, ≥1 SLO `max_column` scenario, ≥1 schema-drift via
Shape, ≥1 Prophet trend with seeded history, ≥1 Shape anomaly, ≥1
Pattern batch-level distribution shift.

**Config shape conventions (applied to every row below):**

- **JOIN rows** use `DatasetConfig(query=JOIN_SQL, dimensions=[...],
  measures=[...])` + a `ThresholdValidationConfig` with
  `AggregationCollectionConfig`, matching the existing shipped
  `TestE2EQueryDataset.test_join_query` pattern. They do **not** use
  `CustomQueryCollectionConfig` — that collector aggregates
  single-row SQL and its `filter` field is a hard-fail migration
  error (`config.py:_reject_nonempty_filter`).
- **Trend rows** override the Prophet model with `history_count: 30`
  (default is 90) so the seeded 30-day history matches the training
  window. Without this override a fresh run with only 30 seeded
  points would leave Prophet with an oversized window and the
  pass/fail behavior becomes data-length-dependent instead of
  data-pattern-dependent.
- **Drift rows** use `HistoricalCompareConfig(past_values=3, step="P7D")`
  (matching shipped `TestE2EDrift`), seeded via `_seed_history(...)`
  in the shared helpers. **Bucket anchor arithmetic** (important
  because `read_metric_history` buckets are anchored to the UNIX
  epoch via `CAST(strftime('%s', run_timestamp) AS INTEGER) /
  step_seconds` — see `qualifire/storage/sqlite_storage.py:146-174`):
  the industry packs seed 3 historical rows at `now() - 21d`,
  `now() - 14d`, `now() - 7d` (each at the same wall-clock offset
  within the day as the engine's current-run timestamp). With
  `step_seconds = 7 * 86400 = 604800`, those three seeded rows and
  the current run each land in distinct 7-day buckets, so
  `past_values=3` sees exactly three past datapoints and the
  current run's mean-of-past math is well-defined. The existing
  `_seed_history(storage, table_name, metric_name, values,
  step_days=7)` helper lines up with this when called with
  `step_days=7` — the shared helper module will preserve that
  signature exactly.
- **Shape schema-drift rows** set
  `AnomalyModelConfig(alert_on_schema_change=True)`; the default is
  `False`, so without this flag a `rename`-style drift would be
  silently ignored.
- **Pattern rows** either set only `history.past_dates` (letting the
  sampler pick its own past slices) **or** set both `past_dates` and a
  list of `filters` whose length equals `past_dates` — mixing them
  with mismatched lengths fails
  `PatternValidationConfig._validate_history_filters_length`.
- **YAML ↔ pytest inventory parity (not full-semantic parity) is
  CI-enforced.** The tracked `examples/industries/<industry>/config.yaml`
  is loaded end-to-end by the industry's pytest module: each pack
  contains a test
  (`test_config_yaml_inventory_matches_pytest_scenarios`) that
  (a) loads the YAML through
  `qualifire.core.config.QualifireConfig.model_validate`, (b) extracts
  the set of `(dataset.name, validation.type, validation.name)`
  triples, and (c) asserts that set equals the set of triples the
  pytest module exercises. This closes the drift gap Codex Round 3
  flagged: a new validation added to `config.yaml` without a matching
  pytest case (or vice versa) fails the parity test.
  **Scope gate on parity (per Round 4 B3):** the test name is
  deliberately `..._inventory_matches_...`, not `..._matches_...`,
  because the comparison is inventory only. Thresholds, filters,
  sample sizes, and Prophet/RF hyperparameters are **not** compared —
  demo YAMLs are tuned for readability and can legitimately diverge
  from the pytest scenarios' exact parameter values. The plan does
  NOT claim YAML↔pytest semantic parity; Acceptance wording matches
  this scope. If a future feature needs full-semantic parity, that
  is a separate test (e.g. end-to-end execute both and compare
  result severities), not a widening of this one.
- **Pattern FAIL rows assert SHAP drivers are actually populated,
  not just present.** `ValidationResult.details["top_contributing_features"]`
  is set to `None` when the SHAP extra soft-fails
  (e.g. `shap` import errors, model-agnostic fallback hits an edge
  case). A bare `"top_contributing_features" in details` check
  would pass on a SHAP soft-fail, defeating the acceptance criterion
  that SHAP names the driving features. Every Pattern FAIL row's
  pytest assertion must be:
  ```python
  drivers = result.details.get("top_contributing_features")
  assert drivers is not None, (
      "Pattern FAIL row expects SHAP drivers to be populated; got None "
      "which indicates the SHAP explainer soft-failed. The acceptance "
      "criterion requires named drivers, not just a present key."
  )
  assert len(drivers) >= 1
  top_features = {d["feature"] for d in drivers[:3]}
  expected = {"amount", "quantity"}  # per-scenario
  assert top_features & expected, (
      f"Expected top-3 SHAP features to include one of {expected}, "
      f"got {top_features}"
  )
  ```
  The exact `expected` set for each Pattern FAIL row is the feature
  list named in the Checks table ("SHAP names `amount`, `quantity`"
  etc.); the assertion only requires the **intersection** be
  non-empty so small changes in SHAP ranking from a library update
  don't flip the test, but a full ranking change (different
  top-3) still fails loudly.
- **Pattern `exclude_columns` is non-optional for every Pattern row.**
  Any column that trivially identifies the current-vs-past slice
  (date, partition, ingestion timestamp, surrogate ID) must be listed
  in `model.exclude_columns` or AUC collapses to ~1.0 for *any*
  configuration. The concrete lists per industry:
  - **Retail**: `sale_id`, `sale_date`, `updated_at`, plus
    `snapshot_date` if the Pattern row is computed on `inventory_fact`.
  - **Life Sciences**: `result_id`, `result_date`, `updated_at`,
    `event_id`, `event_date`.
  - **Financial Services**: `txn_id`, `txn_ts`, `updated_at`,
    `price_date`.
  The YAML config in `examples/industries/<industry>/config.yaml`
  **must** list exactly these for whichever fact table its Pattern
  row is configured against; the pytest module asserts
  `exclude_columns` is a superset of that list before the engine run
  so a future edit can't silently drop one. This matches the
  leakage-control contract in `docs/validators.md` (§Pattern Check,
  "Leakage control (required)").

Backend contract per industry:

| Validation | Pandas | Spark                              |
|------------|--------|------------------------------------|
| SLO        | ✅     | ✅                                 |
| Threshold  | ✅     | ✅                                 |
| Drift      | ✅     | ✅                                 |
| Trend      | ✅     | ✅                                 |
| Shape      | ✅     | pytest.skip(reason="…Pandas-only") |
| Pattern    | ✅     | pytest.skip(reason="…Pandas-only") |
| Query JOIN | ✅     | pytest.skip(reason="…Pandas-only") |

**Shape "schema drift" scope caveat.** The retail `#10` and LS `#10`
Shape FAIL rows describe a column-set change (``drop currency``,
``rename unit → units``). Literal current-vs-past column-set
divergence is architecturally impossible through
``SampleCollectionConfig`` because ``backend.sample_records(table, n,
filter)`` reads both partitions from a single physical table, and a
pandas DataFrame has one column set regardless of the row filter.
The e2e tests therefore seed the corruption as a NaN-pattern rename
(populated in current, NaN in history, or vice-versa) plus an
aggressive distributional shift on the affected column — the
IsolationForest flags this via elevated anomaly_ratio rather than
via the ``alert_on_schema_change`` branch. That branch is covered
directly by
``tests/test_validation/test_isolation_forest.py::test_alert_on_schema_change``
and
``tests/test_validation/test_pattern_check.py::test_alert_on_schema_change_emits_auxiliary_result``,
which construct ``CollectionResult`` inputs with different column
sets and assert the WARNING fires. Idea §67 "≥1 schema-drift signal
exercised through the Shape check" is therefore satisfied by
(a) the e2e NaN-pattern + distributional corruption, and
(b) the existing unit-test coverage of the schema-change alert
branch. Making the e2e path fire the alert would require extending
``SampleCollectionConfig`` to source current and past samples from
different physical tables; that extension is **out of scope** for
this feature.

### Retail coverage

Datasets (seed=2026, ≥30 daily partitions):

| Table              | Kind | Columns (shape)                                                                                      | Rows (≈) | Realistic noise                                                |
|--------------------|------|------------------------------------------------------------------------------------------------------|----------|----------------------------------------------------------------|
| `sales_fact`       | Fact | `sale_id`, `sale_date`, `store_id`, `product_id`, `quantity`, `amount`, `currency`, `updated_at`     | 6,000    | 1% null `product_id`, 0.5% outlier `amount` (×10), weekend dip |
| `inventory_fact`   | Fact | `snapshot_date`, `store_id`, `product_id`, `on_hand`, `on_order`, `updated_at`                       | 3,000    | 2% null `on_order`, negative outliers injected on day 22       |
| `products_dim`     | Dim  | `product_id`, `name`, `category`, `unit_price`, `is_active`                                          | 50       | Mixed types (bool, numeric, str)                               |
| `stores_dim`       | Dim  | `store_id`, `city`, `country`, `opened_at`                                                           | 12       | Timestamp column                                               |

Checks:

| # | Validation | Scenario                                                                                          | Pass/Fail |
|---|------------|---------------------------------------------------------------------------------------------------|-----------|
| 1 | SLO        | `max_column` on `sales_fact.updated_at`, warn=4H err=8H; current-day `updated_at`                 | Pass      |
| 2 | SLO        | Same rule with `updated_at` stale by 30H                                                          | Fail      |
| 3 | Threshold  | `COUNT(*)` per day on `sales_fact` ≥ 100                                                          | Pass      |
| 4 | Threshold  | `SUM(CASE WHEN product_id IS NULL…)` null-rate ≤ 0.5%                                             | Fail      |
| 5 | Drift      | `AVG(amount)` z_score vs 3 past 7-day windows, seeded history                                     | Pass      |
| 6 | Drift      | Same metric with current window doubled → deviation_pct > 50                                      | Fail      |
| 7 | Trend      | Prophet on daily `SUM(amount)`, 30-day seeded history                                             | Pass      |
| 8 | Trend      | Current value forced outside 95% band                                                             | Fail      |
| 9 | Shape      | Isolation Forest on `sales_fact` current vs 3 past samples                                        | Pass      |
| 10| Shape      | Current sample corrupted (drop `currency`, skew `amount`)                                         | Fail      |
| 11| Query JOIN | `sales_fact` × `products_dim` on `product_id`; aggregate revenue by `category`, threshold ≥ €X    | Pass      |
| 12| Pattern    | Two-sample RF on `sales_fact` rows: current daily sample vs 3 past daily samples; AUC < 0.65      | Pass      |
| 13| Pattern    | Shift current sample (inflate `amount` tail + skew `quantity` distribution upward) → AUC ≥ 0.65, SHAP names `amount`, `quantity` | Fail      |

### Life Sciences coverage

Datasets (seed=2027, ≥30 daily partitions):

| Table                 | Kind | Columns (shape)                                                                                              | Rows (≈) | Realistic noise                                               |
|-----------------------|------|--------------------------------------------------------------------------------------------------------------|----------|---------------------------------------------------------------|
| `lab_results_fact`    | Fact | `result_id`, `result_date`, `patient_id`, `site_id`, `assay`, `value`, `unit`, `flag`, `updated_at`          | 5,000    | 3% null `value`, outlier injection on day 18, unit mixing    |
| `adverse_events_fact` | Fact | `event_id`, `event_date`, `patient_id`, `site_id`, `severity`, `term`, `updated_at`                          | 800      | Severity skew toward 'mild', rare 'severe' events             |
| `patients_dim`        | Dim  | `patient_id`, `enrolled_at`, `site_id`, `age`, `sex`, `arm`                                                  | 300      | 2% null `age`, mixed categorical                              |
| `sites_dim`           | Dim  | `site_id`, `country`, `principal_investigator`, `active`                                                     | 20       | Bool column                                                   |

Checks:

| # | Validation | Scenario                                                                                                | Pass/Fail |
|---|------------|---------------------------------------------------------------------------------------------------------|-----------|
| 1 | SLO        | `max_column` on `lab_results_fact.updated_at`, warn=6H err=12H                                          | Pass      |
| 2 | SLO        | Stale `updated_at` on `adverse_events_fact` (> err)                                                     | Fail      |
| 3 | Threshold  | Row count on `lab_results_fact` per day ≥ 50                                                            | Pass      |
| 4 | Threshold  | `SUM(CASE WHEN flag='HIGH'…)` ratio warn>2% err>5%                                                      | Fail      |
| 5 | Drift      | `AVG(value)` per assay vs 3 past 7-day windows                                                          | Pass      |
| 6 | Drift      | Same with outlier day forced → deviation_pct fail                                                       | Fail      |
| 7 | Trend      | Prophet on daily `COUNT(event_id)` from `adverse_events_fact`, 30-day seeded history                    | Pass      |
| 8 | Trend      | Spike day outside 95% band                                                                              | Fail      |
| 9 | Shape      | Isolation Forest on `lab_results_fact` current vs 3 past samples                                        | Pass      |
| 10| Shape      | Inject schema drift (rename `unit` → `units`) in current sample to trigger drift + anomaly              | Fail      |
| 11| Query JOIN | `lab_results_fact` × `patients_dim` on `patient_id`; assert no orphan `patient_id` in facts             | Pass      |
| 12| Pattern    | Two-sample RF on `lab_results_fact` rows: current daily sample vs 3 past daily samples; AUC < 0.65       | Pass      |
| 13| Pattern    | Shift current sample (bias `value` upward + inflate `flag='HIGH'` rate) → AUC ≥ 0.65, SHAP names `value`, `flag` | Fail      |

### Financial Services coverage

Datasets (seed=2028, ≥30 daily partitions):

| Table                | Kind | Columns (shape)                                                                                                 | Rows (≈) | Realistic noise                                                   |
|----------------------|------|-----------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------|
| `transactions_fact`  | Fact | `txn_id`, `txn_ts`, `account_id`, `merchant_id`, `amount`, `currency`, `channel`, `status`, `updated_at`        | 7,000    | 0.3% null `merchant_id`, heavy-tail amount (log-normal), fraud spikes |
| `market_prices_fact` | Fact | `price_date`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `updated_at`                                  | 900      | 1% null `volume`, black-swan outlier day                          |
| `accounts_dim`       | Dim  | `account_id`, `opened_at`, `customer_id`, `segment`, `risk_band`, `active`                                      | 150      | Mixed bool + categorical                                          |
| `merchants_dim`      | Dim  | `merchant_id`, `name`, `mcc`, `country`                                                                         | 60       | Categorical with >20 distinct MCCs → label-encoded by Shape       |

Checks:

| # | Validation | Scenario                                                                                                   | Pass/Fail |
|---|------------|------------------------------------------------------------------------------------------------------------|-----------|
| 1 | SLO        | `max_column` on `transactions_fact.updated_at`, warn=1H err=4H                                             | Pass      |
| 2 | SLO        | Stale `market_prices_fact.updated_at` (> err)                                                              | Fail      |
| 3 | Threshold  | Per-day row count on `transactions_fact` ≥ 150                                                             | Pass      |
| 4 | Threshold  | `SUM(CASE WHEN amount < 0…)` negative-amount count > 0 → err                                               | Fail      |
| 5 | Drift      | `AVG(amount)` z_score vs 3 past 7-day windows                                                              | Pass      |
| 6 | Drift      | Force fraud-spike day → deviation_pct > 50                                                                 | Fail      |
| 7 | Trend      | Prophet on daily `SUM(amount)`, 30-day seeded history                                                      | Pass      |
| 8 | Trend      | Black-swan day outside 95% band                                                                            | Fail      |
| 9 | Shape      | Isolation Forest on `transactions_fact` current vs 3 past samples                                          | Pass      |
| 10| Shape      | Swap `channel` dominant value + inflate high-MCC merchants                                                 | Fail      |
| 11| Query JOIN | `transactions_fact` × `merchants_dim` on `merchant_id`; MCC-category aggregate threshold                   | Pass      |
| 12| Pattern    | Two-sample RF on `transactions_fact` rows: current daily sample vs 3 past daily samples; AUC < 0.65        | Pass      |
| 13| Pattern    | Shift current sample (inflate `amount` heavy tail + reweight `channel` mix) → AUC ≥ 0.65, SHAP names `amount`, `channel` | Fail      |

**Per-industry total: 13 scenarios × (1 or 2 backends per the backend
contract) = ~22 test cases per industry, ~66 across the suite.**

## Technical Decisions

- **Extend the existing harness, never fork it.** All new tests live under
  `tests/test_e2e_industries/` and import `_e2e_support` helpers extracted
  from `test_e2e.py`. No parallel pytest framework, no second CI gate.
- **Seeded runtime generation, nothing committed.** Each industry generator
  uses a fixed seed so tests are reproducible; no CSVs in the repo.
- **One report per industry, generated once per pack run.** A pack-scoped
  fixture owns a single `SQLiteStorage` shared by every test in the pack,
  and a finalizer generates exactly one `report.html` at pack teardown.
  That finalizer's assertions (non-empty HTML, file exists, non-empty on
  disk) are the blocking report check. Per-test report generation is
  forbidden — it would overwrite the canonical file with partial state.
  Parent directory creation is the caller's responsibility (the writer
  does not `mkdir`, see `qualifire/reporting/html_report.py:110,241`).
- **Output paths:** locally, a per-session pytest tmp directory
  (`tmp_path_factory.mktemp("reports")`); in CI, under
  `$QUALIFIRE_ARTIFACTS_DIR/industry-reports/<industry>/report.html`. The
  tracked `examples/industries/<industry>/` tree holds only config + README.
- **Backend skips are explicit.** Shape, Pattern, and multi-table
  JOIN (via `DatasetConfig(query=...)`) run Pandas-only; the Spark
  parametrization must `pytest.skip(reason="…")`, never silently
  omit. If `pattern-check`'s plan delivers a Spark path, the Pattern
  skip is removed in the same PR that lands the Spark support, and
  both the idea.md backend table and the §"Coverage matrices" table
  above are updated. **Skip enforcement is CI-enforced via a
  manifest verifier, not a grep over prose.** Step 6 adds a new
  module `tests/test_e2e_industries/_skip_manifest.py` that encodes
  the full expected test-ID × backend-marker grid as a Python
  literal (one entry per (industry, scenario_key, backend,
  expected_status) tuple, where `expected_status` is one of
  `required` / `skip_pandas_only`). Derived directly from the
  coverage tables. Two enforcement surfaces (one in-process hook,
  one CLI verifier against the single-run JUnit), plus a
  developer-loop convenience note:
  - **Collection verification happens in-process**, not via a second
    pytest invocation. The `_skip_manifest` module registers a
    `pytest_collection_modifyitems` hook (via an
    autouse-equivalent `conftest.py` import in
    `tests/test_e2e_industries/conftest.py`) that, during the
    **same** `pytest tests/ -v --junitxml=artifacts/junit.xml` run
    from Step 6, inspects `session.items` and asserts that every
    `(industry, scenario_key, backend)` tuple in the manifest is
    present in the collected node IDs. A missing tuple calls
    `pytest.exit(reason=..., returncode=1)` before any test body
    executes, so regressions surface at collection time, not as a
    second CI step. This replaces the earlier `--verify-collection`
    CLI: running `pytest --collect-only` separately would (a) be a
    second pytest invocation Codex Round 5 objected to and
    (b) duplicate cost. The in-process hook costs ~0 and runs in
    the single binding pytest run.
  - `python -m tests.test_e2e_industries._skip_manifest
    --verify-skips --junit artifacts/junit.xml`
    consumes the **same** `artifacts/junit.xml` produced by the
    single `pytest tests/ -v --junitxml=artifacts/junit.xml` run
    from Step 6 — it does **not** re-invoke pytest. (Codex Round 5
    blocker fix: any second pytest invocation would re-run the
    industry packs and overwrite `artifacts/junit.xml`, undoing
    Round 3's single-run contract.) The verifier parses the JUnit
    XML's `<testcase>` entries (including `<skipped message="...">`
    children, which JUnit XML always emits for skips regardless of
    pytest's `-rs` terminal output) and asserts:
    (a) every Pandas-only row's skip reason matches **exactly**
    `SparkBackend not supported for <scenario_key> — see shipped E2E
    notes`. A wrong or missing reason exits 1.
    (b) **Every `required` row executed to completion** — no
    skip, no `xfail`, no `xpassed`. If a required row shows up with
    status `skipped` in the JUnit XML, the verifier exits 1 with
    `"required row <industry>.<scenario_key>[<backend>] was skipped
    with reason <reason> — mandatory rows cannot be skipped"`. This
    closes the A6 "TODO: implement" silent-downgrade loophole Codex
    Round 4 flagged: a developer cannot `pytest.skip("not ready
    yet")` a required row and ship the feature, because the skip
    parses as a required-row skip, not the sanctioned Pandas-only
    skip reason, and the verifier rejects it.
  - The in-process collection hook (first bullet) fires on **every**
    `pytest tests/` run — local or CI — so regressions surface
    immediately during development, not only when a PR hits the
    CI job. The `--verify-skips` CLI step is CI-only by design
    because it needs `artifacts/junit.xml` from the full run, but
    developers running `pytest tests/test_e2e_industries/` locally
    can re-run the same CLI against a locally emitted
    `artifacts/junit.xml` and get the same verification.
  The manifest is the single source of truth for expected IDs; when
  a coverage-matrix row is added/removed, the manifest is edited in
  the same commit. This replaces "grep for a substring" with a
  verifier a reviewer can diff line-for-line against the matrices
  in this plan.
- **No new dependencies.** Reuse `numpy`, `prophet`, `scikit-learn`,
  `shap`, and `pyspark`. All numeric and categorical generation lives in
  `numpy` with a per-industry fixed `RandomState`; readable dimension
  names (e.g. merchant / product / site labels) come from fixed
  rotating string lists, not from `faker` (`faker` is not currently
  pinned in this repo and adding it would pull a transitive dependency
  surface that this feature does not need). `pyspark` is installed by
  the new CI workflow explicitly (see Step 6) — it is not in
  `pyproject.toml` extras on `main` and changing that is out of scope.

## Testing Strategy

- **Regression gate:** `pytest tests/test_e2e.py -v` must pass unchanged
  after Step 1 — **same 33 tests** pre- and post-extraction (see
  Pre-flight reconciliation above; the original shipped count of 27
  grew to 33 after pattern-check PR #3 merged on 2026-04-21).
- **Industry gate:** `pytest tests/test_e2e_industries/ -v` must pass, with
  ≥13 scenarios per industry covering all 6 validation types (1 pass + 1
  fail each minimum), JOIN, SLO `max_column`, Shape schema drift, Prophet
  trend with seeded history, Pattern batch-level shift with SHAP-named
  drivers. Every row of each industry's Checks table is part of the
  default CI gate — none may move behind a `slow` marker.
- **Report assertion (pack-level finalizer, once per industry):**
  ```python
  html = render_report(industry_storage, industry_report_path)
  assert html and "<html" in html
  assert industry_report_path.exists()
  assert industry_report_path.stat().st_size > 0
  ```
- **Runtime verification:** record `--durations=0` per industry in the
  README. If a pack exceeds 3 min, apply only the knobs listed in Step 7
  (reduce row counts above the 30-day floor; share Spark session across
  tests; collapse redundant backend parametrizations for non-mandatory
  scenarios). `slow` markers are allowed **only** for extra scenarios
  beyond the mandatory matrix.

## Acceptance

A pack is accepted when **all** of the following are true:

- [ ] `pattern-check` has shipped (merged to `main`) before this pack's
      Steps 2–7 land.
- [ ] Generator produces ≥30 daily partitions with seeded reproducibility.
- [ ] All 6 validation types have ≥1 pass + ≥1 fail scenario.
- [ ] ≥1 multi-table JOIN check passes, implemented as
      `DatasetConfig(query=JOIN_SQL, dimensions=..., measures=...)` +
      `ThresholdValidationConfig`. (Not `CustomQueryCollectionConfig`.)
- [ ] ≥1 SLO `max_column` scenario (pass + fail both exist).
- [ ] ≥1 Shape scenario demonstrates schema-drift detection.
- [ ] ≥1 Prophet trend scenario uses seeded history and shows pass + fail.
- [ ] ≥1 Pattern scenario demonstrates batch-level distribution shift
      (AUC ≥ threshold) with SHAP naming the top drivers, and the paired
      pass scenario has AUC strictly below threshold.
- [ ] Exactly one `report.html` is produced per industry, generated at
      pack teardown from the shared `SQLiteStorage`; its HTML is
      non-empty and the file exists non-empty on disk.
- [ ] Spark skips (Shape, Pattern, JOIN) use explicit
      `pytest.skip(reason=...)`.
- [ ] Pack runtime ≤180 s per industry and ≤600 s total **measured
      in CI** across the full backend matrix (the `tests.yml` duration
      check is the binding gate; local `--durations=0` output is
      advisory and lives in the industry README).
- [ ] Per-industry artifact tree under
      `${QUALIFIRE_ARTIFACTS_DIR}/industry-reports/<industry>/` is
      ≤5 MB (Step 6's `du -sb` check enforces this in the same CI
      step as the existence assertion — matches `idea.md` §3).
- [ ] README documents how to run locally and where the CI report lands.
- [ ] `examples/industries/<industry>/` contains **only** `config.yaml` and
      `README.md` — no generated HTML.
- [ ] Each pack contains
      `test_config_yaml_inventory_matches_pytest_scenarios` which
      loads the tracked `config.yaml` via
      `QualifireConfig.model_validate` and asserts the
      `(dataset, validation.type, validation.name)` **inventory**
      triple set matches the pytest module's in-code scenarios.
      **Inventory-only, not semantic:** thresholds, filters, sample
      sizes, and Prophet/RF hyperparameters are intentionally NOT
      compared — demo YAMLs can diverge on those for readability
      without failing the gate (see the scope-gate wording under
      "YAML ↔ pytest inventory parity" in Coverage matrices
      preamble).

## Risks & Mitigations

- **Risk: Regressing the shipped E2E suite during helper extraction.**
  *Mitigation:* Step 1 is mechanical (move-only, no behavior change); the
  gate before moving to Step 2 is the same **33**-test pass in
  `test_e2e.py` (see Pre-flight reconciliation — 27 was the original
  shipped count; pattern-check PR #3 brought it to 33).
- **Risk: CI duration creep.** *Mitigation:* runtime budgets enforced per
  industry with `--durations=0`. Required scenarios (every row of each
  Checks table) always stay in the default gate; budget is reclaimed via
  the Step 7 knobs — reducing row counts (keeping ≥30 daily partitions),
  sharing a single session-scoped `SparkSession` across all tests, and
  collapsing redundant Pandas/Spark parametrizations only for
  non-mandatory scenarios. `slow` markers apply only to extras.
- **Risk: Shape / JOIN / Pattern silently omitting backends instead
  of skipping.** *Mitigation:* explicit `pytest.skip(reason="SparkBackend
  not supported for <scenario> — see shipped E2E notes")`; enforced in
  CI by `tests/test_e2e_industries/_skip_manifest.py` in both
  `--verify-collection` and `--verify-skips` modes (see Technical
  Decisions above). Do **not** fall back to grep-over-prose — the
  manifest verifier is the single source of truth.
- **Risk: Generated reports polluting the worktree.** *Mitigation:*
  `industry_report_path` fixture uses `tmp_path_factory.mktemp("reports")`
  locally; CI path requires `QUALIFIRE_ARTIFACTS_DIR`; `artifacts/` is
  gitignored.
- **Risk: Per-industry report reflecting only the last test's state.**
  *Mitigation:* one `SQLiteStorage` is shared across every test in a pack
  (pack-scoped fixture), and the single `report.html` is produced by a
  pack-level finalizer after all tests have written their collection and
  validation rows. Per-test report generation is forbidden.
- **Risk: Prophet flakiness on small seeded history.** *Mitigation:* seed
  numpy + use fixed `changepoint_prior_scale`; seed history count ≥ 30
  points per the existing trend tests' pattern.
- **Risk: `pattern-check` slips or changes shape before these packs land.**
  *Mitigation:* Step 1 (harness extraction) has no dependency on Pattern
  and can land independently, so the refactor value is not blocked. If
  `pattern-check`'s eventual config schema diverges from this plan's
  assumed YAML shape (two-sample RF, AUC threshold ≈ 0.65, SHAP top-N
  drivers), the industry Pattern rows are adjusted in a single PR
  before Step 2 begins. Do **not** inline-implement a stand-in Pattern
  validator here to "unblock" Step 2 — that would violate the Out of
  Scope clause and create a second source of truth.

## Out of Scope (reaffirmed from `idea.md`)

WAP, Delta-specific recency strategies, non-SQLite storage backends,
notification delivery, any check beyond the 6 shipped validation types,
modifying the HTML writer in `qualifire/reporting/html_report.py` to
auto-create parent directories, **implementing `pattern-check` itself**
(that lives under `docs/features/pattern-check/` and is a prerequisite,
not a deliverable of this feature).
