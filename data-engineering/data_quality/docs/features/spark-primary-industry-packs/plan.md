---
started: 2026-04-21
---

# Implementation Plan: Spark-Primary Industry Packs + Examples README Cleanup

## Overview

Close the Spark coverage gap that shipped with `industry-demo-suites`:
Shape, Pattern, and Query-JOIN scenarios currently skip the Spark
backend via `tests/test_e2e_industries/_skip_manifest.py`
(`skip_pandas_only`). That was a scope convention, not a library
constraint — Shape and Pattern validators already auto-convert Spark
DataFrames via `.toPandas()` (`qualifire/validation/isolation_forest.py:107-117`,
`qualifire/validation/pattern_check.py:119-129`), and
`SparkBackend.execute_sql` supports the JOIN path natively.

Flipping the manifest also makes the user-facing README inaccurate:
`examples/industries/*/README.md` leak two test-harness facts (the
"Spark skips … sanctioned by `_skip_manifest.py`" trailer and the
"pandas only" Backends column) that describe pytest choices, not YAML
properties. Both scopes ship together because fixing the README
without closing the Spark gap would just trade one inaccuracy for
another.

**Interpretation of "Spark-primary" in idea.md.** `idea.md:30-35`
says "treating pandas as the optional/secondary backend for these
demo packs." That is the **feature's framing** (Spark is the target
backend of interest), **not** a directive to demote pandas. The
concrete deliverable is: flip the five `skip_pandas_only` Spark rows
to `required`, so Spark runs every scenario in CI. Pandas stays
`required` on every row, because (a) removing pandas coverage would
lose regression protection on the validators' pandas paths, and (b)
dropping pandas parametrizations is a scope explosion not sanctioned
by `idea.md` (the "Affected Areas" list in `idea.md:58-63` does not
include deleting pandas parametrizations). If a future feature wants
to actually demote pandas to optional, it should be its own
P-prioritized item.

**Non-goals locked in `idea.md`** — not in scope here:
- No changes to validator-internal Spark → pandas conversion strategy.
  `.toPandas()` on a 10k-row demo frame is acceptable; distributed
  Spark-native Isolation Forest / RF two-sample is a separate research
  item.
- No change to CI gate runtime budgets below 180s/pack or 600s/total
  unless forced by measurement (see Risk R3 below).
- No rewrite of the sample collection cache or `SparkBackend.sample_records`
  contract. Those shipped in `industry-demo-suites` and are already
  reviewed.
- **No demotion of pandas to optional.** See interpretation note
  above. Pandas stays `required` on every row per current manifest.

## Pre-flight reconciliation with current repo state

Scanned on branch creation (commit `78722f7`). Facts the plan depends on:

- **`_skip_manifest.py` row shape** — `_ROWS_PER_INDUSTRY` is a tuple of
  `(scenario_key, pandas_status, spark_status)`. Flipping Spark coverage
  is a one-line-per-row edit: `"skip_pandas_only"` → `"required"` for
  the five affected keys (`shape_pass`, `shape_fail`, `query_join`,
  `pattern_pass`, `pattern_fail`). The manifest expands to
  `INDUSTRIES × _ROWS_PER_INDUSTRY` so this single edit flips 15
  parametrizations (5 scenarios × 3 industries).
- **Coupling between the manifest flip and the test-body edits.**
  `pytest_collection_modifyitems` in
  `tests/test_e2e_industries/conftest.py:51-81` enforces only that the
  expected *node IDs* exist in the collection — it does **not** inspect
  `skip_pandas_only` vs `required`. The *runtime* skip comes from
  `_skip_spark(...)` inside each affected test function
  (`tests/test_e2e_industries/retail/test_retail_e2e.py:76-78`). The
  post-run `verify_skips` CLI
  (`tests/test_e2e_industries/_skip_manifest.py:198-252`) is what ties
  them together: it ERRORs if a `required` row is skipped OR if a
  `skip_pandas_only` row executed. So the manifest flip and the
  `_skip_spark` removal are **mutually dependent** — neither is
  executable as a standalone commit because `verify_skips` will fail
  whichever half lands alone. Step 1 and Step 2 must land as one atomic
  change in this branch (separate commits are fine as long as no
  intermediate push leaves `verify_skips` red).
- **Hardcoded `"pandas"` backend in affected tests** — five scenario
  functions per industry still call `_make_backend("pandas", {...})`
  even though `backend_type` is parametrized (`[pandas, spark]`). The
  Spark branch is guarded by the `_skip_spark()` call at the top of
  each. A full inventory (15 functions) was grepped before writing
  this plan — every hardcoded call must flip to `_make_backend(backend_type, ...)`
  **and** every `_skip_spark(...)` line at the top must be removed.
- **`_skip_spark()` helper becomes unused** — once all five removals
  land per industry, `_skip_spark` and the related import of
  `sanctioned_skip_reason` in each `test_<industry>_e2e.py` become
  dead code. Remove both, or keep the helper file-local and unused
  (explicit removal preferred to keep review surface honest).
- **`SparkBackend.sample_records`** (`qualifire/backends/spark_backend.py:77-82`)
  uses `ORDER BY RAND() LIMIT n` with no seed — this is already flagged
  in `qualifire/core/engine.py:77` and mitigated by `_SampleCache` so
  Shape and Pattern on the *same sample spec* share rows within a
  single run. Reproducibility **across runs** is not guaranteed — see
  Risk R1.
- **Demo data volumes** (grounded in generator code):
  - Retail `sales_fact` — weekend base 150, weekday base 210 per day
    × 31 days ≈ **6.0k** total rows
    (`tests/_e2e_support/retail/generate.py:76-77`).
  - Life Sciences `lab_results_fact` — weekend base 140, weekday
    base 175 per day × 31 days ≈ **5.27k** total rows
    (`tests/_e2e_support/life_sciences/generate.py:105-121`).
  - Financial Services `transactions_fact` — weekend base 180,
    weekday base 250 per day × 31 days ≈ **7.23k** total rows
    (`tests/_e2e_support/financial_services/generate.py:107-125`).
  Validators use `n_records=500` (Pattern) and per-day filtered
  samples (Shape). Per-day partition for the *current* row is
  drawn from `np.random.normal(base_n, 0.07 * base_n)` (financial
  services) or `0.08 * base_n` (retail) or `0.06 * base_n` (life
  sciences) — i.e., not a hard ceiling but a tight distribution
  around the per-industry base (retail 150/210, life sciences
  140/175, financial services 180/250). Upper tail with σ ≈
  0.07–0.08 of the base and a single-day draw is well under 500
  for every industry (3σ upper bound < 320 for financial services,
  < 260 for retail, < 210 for life sciences). So
  `sample_records(n=500, filter='date == <current>')` returns the
  **full** current-day partition on both backends — sampling
  membership variance is bounded on the Shape path. Pattern with
  `n_records=500` against the same current partition is similarly
  the full set. Spark `ORDER BY RAND()` changes row **order**
  within the returned full partition, not membership (see next
  bullet).
- **`pandasql` + `pandas.DataFrame` registered through
  `_make_spark_backend`** — `createDataFrame` rejects pandas columns
  that mix types in ways Spark's type inference cannot handle. The
  `None`-normalization guard at `tests/_e2e_support/backends.py:76-78`
  already handles NaN → None for nullable inference. Pattern-fail and
  Shape-fail mutate the pandas frame *before* the backend call, so
  `createDataFrame` sees a clean, nullable-consistent frame.
- **Mutation patterns in the failing scenarios**:
  - `test_shape_fail` — mask-based `corrupted.loc[mask, "currency"] = None;
    corrupted.loc[mask, "amount"] *= 10.0`. Pandas df → Spark via
    `createDataFrame` preserves nulls (post NaN→None normalization).
    Verified by hand against the backends helper.
  - `test_pattern_fail` — mask-based `shifted.loc[mask, "amount"] =
    rng.uniform(...)`. Same guarantees.
- **Report / storage plumbing** — Shape and Pattern already persist to
  the shared `SQLiteStorage` regardless of backend
  (`tests/_e2e_support/backends.py:91-94` — `_make_storage` returns
  `SQLiteStorage(db_path=":memory:")`, in-memory per `engine.run()`
  and used unconditionally by both backends). The per-pack autouse
  `_industry_report` fixture (one per industry — defined at
  `tests/test_e2e_industries/retail/conftest.py:22`,
  `tests/test_e2e_industries/life_sciences/conftest.py:22`, and
  `tests/test_e2e_industries/financial_services/conftest.py:22`)
  calls `render_pack_report` from the parent conftest
  (`tests/test_e2e_industries/conftest.py:169`, which uses
  `_report_path_for` at `:149-156`) to write
  `$QUALIFIRE_ARTIFACTS_DIR/industry-reports/<industry>/report.html`
  at package teardown. Neither the fixture nor the helper cares
  which backend populated the storage. Spark runs will appear
  alongside pandas rows in `report.html`.

  **Critical caveat** (drives Step 3's mechanism choice): the
  shipped `_make_storage` hardcodes `db_path=":memory:"`, so the
  SQLite DB is **per-process and never persisted to disk**. This
  rules out any post-pytest `sqlite3` query against a DB file for
  severity capture — no file exists. Step 3's severity-gate
  instead uses stdout-captured `[SEVERITY-GATE]` print lines
  (mechanism detailed in Step 3).
- **Existing 33-test E2E baseline** (`tests/test_e2e.py`) — this
  feature does not touch the shipped E2E tree. Gate: `pytest
  tests/test_e2e.py -v` must still show 33 passing. Test count
  verified via `pytest tests/test_e2e.py --collect-only -q` ⇒
  `33 tests collected`, confirmed on branch creation. The file has
  21 top-level `def test_*` entries; parametrization inflates the
  collected count to 33.
- **Test count delta** — 15 scenarios flip from `skip_pandas_only` to
  `required` on Spark, adding **15 Spark executions** to the pack run.
  Previously 15 were `SKIPPED` with the sanctioned reason; they'll now
  execute. Per-module total is 27 (13 scenarios × 2 backends = 26
  parametrizations, **plus** one non-parametrized
  `test_config_yaml_inventory_matches_pytest_scenarios` —
  `tests/test_e2e_industries/retail/test_retail_e2e.py:616`,
  `tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py:666`,
  `tests/test_e2e_industries/financial_services/test_financial_services_e2e.py:620`).
  Industry tree total: **81** tests (27 × 3). Combined with the
  baseline `tests/test_e2e.py` (33), the **E2E subtrees** total
  **114** tests. Only status distribution changes, not count.
  **Note**: `pytest tests/` (the CI invocation, `pyproject.toml:49-54`
  sets `testpaths = ["tests"]`) also collects the rest of the `tests/`
  tree — `test_api.py`, `test_cli.py`, `test_config.py`,
  `test_engine.py`, `test_backends/`, `test_collection/`,
  `test_notification/`, `test_reporting/`, `test_storage/`,
  `test_validation/`, etc. This plan makes **no** claim about the
  full `pytest tests/` count. Exit criterion #4 asserts only the 114
  E2E-subtree tests; the full `pytest tests/` assertion is "no
  regression vs. main", not a specific integer.
- **`pattern_fail` hard-depends on `shap`.** All three modules call
  `pytest.importorskip("shap", ...)` at the top of `test_pattern_fail`
  (`tests/test_e2e_industries/retail/test_retail_e2e.py:571`,
  `tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py:622`,
  `tests/test_e2e_industries/financial_services/test_financial_services_e2e.py:572`).
  After the manifest flip, `pattern_fail[spark]` is `required`;
  `verify_skips` will ERROR if a `required` row is skipped for any
  reason, including missing `shap`. Prereq: any environment (local or
  CI) running this feature's gate **must** have `shap` installed, or
  the gate is red. CI already installs `shap` via the existing
  `tests.yml` workflow — no change needed there — but local
  verification runs (Step 3, Step 6) must ensure `shap` is present
  before the spot-runs happen.
- **Spark row-order non-determinism reaches Pattern CV folds.**
  `SparkBackend.sample_records` (`qualifire/backends/spark_backend.py:77-82`)
  returns rows in `ORDER BY RAND()` order even when `LIMIT n` exceeds
  the partition size. `PatternCheckValidator` then builds CV folds
  with `StratifiedKFold(n_splits=..., shuffle=True, random_state=...)`
  (`qualifire/validation/pattern_check.py:269-273`). Although
  `StratifiedKFold` is shuffle-seeded, the **input row order before
  shuffle** differs across Spark runs, so fold membership is
  reproducibly random per run but not reproducible *across* runs.
  This was previously inaccurately described as bounded-by-partition.
  The `_SampleCache` mitigation (`qualifire/core/engine.py:73-118`)
  only stabilizes rows **within a single `engine.run()` call**; each
  industry test invokes `_run(...)` once, with one sample-based
  validator in the dataset, so the cache never has a second caller to
  share with, and row-order variance remains. See Decision §2 and
  Risk R1 for the scope response.

## Decisions Locked

### 1. Flip the manifest; don't delete the `skip_pandas_only` machinery.
Keep `sanctioned_skip_reason()` and the `--verify-skips` CLI intact
for future use. Only rewrite `_ROWS_PER_INDUSTRY` rows. This preserves
the round-trip safety net for any future Pandas-only skip we might
need, and keeps review churn confined to *values*, not *structure*.

### 2. No threshold recalibration unless a measurement demands it.
Shape (`anomaly_score`) and Pattern (`auc`) thresholds were chosen for
the corruption magnitudes coded in the mutation steps, not for the
sampling backend. For the affected scenarios, `n_records` ≥ partition
size on both backends, so **row membership** is stable (the entire
filtered partition is returned). **Row order** is not stable on Spark
(`ORDER BY RAND()`; see pre-flight note on CV-fold non-determinism),
which changes Pattern's `StratifiedKFold` fold composition.

Empirical evidence we rely on is **what the current test code asserts**,
not a narrative claim about absolute AUC magnitudes:
- `test_pattern_fail` (all three industries) asserts
  `severity in (Severity.WARNING, Severity.ERROR)` and
  `actual_value > 0.60` (retail 593–594, life_sciences 643–644,
  financial_services 598–599). Thresholds passed to
  `_pattern_config` are `warning_auc=0.60, error_auc=0.80` (retail
  586, life_sciences 636, financial_services 591).
- `test_pattern_pass` asserts `severity == Severity.PASS` and
  `0.0 <= actual_value <= 1.0` (retail 561–565, life_sciences 609–613,
  financial_services 558–562). `_pattern_config`'s default
  `warning_auc=0.65, error_auc=0.80`
  (`tests/test_e2e_industries/retail/test_retail_e2e.py:526-547`;
  equivalent defaults at life_sciences 574 and financial_services 523)
  and `test_pattern_pass` calls it without overrides — so the PASS
  branch requires AUC strictly below the **0.65** warning floor.
The gap between what pandas AUC lands at today and the warn/error
thresholds is **measured** by Step 3's three-run repetition — the plan
does not assert a specific numerical margin up-front, because the
reviewable artifact should be the three-run measurement, not an
unverifiable "well above 0.80" claim.
**Hard stop**: Step 3 spot-runs each newly-enabled Spark scenario
**three times in succession** (not once) before declaring the Spark
path stable. Any severity **change** across the three runs
(pandas-style flake) → stop, capture reproducer, amend plan before
flipping thresholds. A single run cannot distinguish calibration from
flake.

### 3. README values map to runtime reality, not YAML metadata.
`examples/industries/*/README.md` currently says "pandas only" on the
Shape / Pattern / Query-JOIN rows. The **retail README** breaks
those out into five separate rows (`shape_pass`, `shape_fail`,
`query_join`, `pattern_pass`, `pattern_fail`) — see
`examples/industries/retail/README.md:27-31`. The **life sciences**
and **financial services** READMEs collapse the pass/fail pairs into
three rows (`shape_pass`/`shape_fail`, `query_join`,
`pattern_pass`/`pattern_fail`) — see
`examples/industries/life_sciences/README.md:23-25` and
`examples/industries/financial_services/README.md:23-25`. After the
flip, the affected *scenarios* run on both backends with the *same*
YAML. Change each affected cell to `pandas, spark` (parity with the
other rows) **and** drop the `Spark skips for …` trailer paragraph
from each of the three READMEs. Do **not** delete the column — the
column is how the reader answers "which backends can I run this demo
on?" and we want that answer to survive the next new scenario
someone adds. Work inventory (for reviewer sanity-checking): **5
cells + 1 trailer** in retail, **3 cells + 1 trailer** each in life
sciences and financial services.

### 4. Literal schema-drift scope caveat stays.
The Shape `shape_fail` scenario's docstring already explains why
literal column-set drift is architecturally impossible via
`SampleCollectionConfig` (single-table sampler — one column set per
frame). That caveat survives the Spark flip; the Spark run exercises
the same distributional drift, not literal drift.

### 5. CI duration budget holds unless measured otherwise.
The shipped CI contract is 180s/pack, 600s/total. JVM boot is
pre-warmed via the session-scoped `_prewarm_spark_session` fixture
(`tests/test_e2e_industries/conftest.py:89-141`), so added Spark cost
is per-scenario: `createDataFrame` + `.toPandas()` round-trip on
≤10k rows. Plan hard-stop: if the first full CI run blows a
per-pack budget, re-measure locally, and if the overrun is bounded
(<20%), propose a targeted budget raise in the same PR (not a
wholesale lift). If the overrun exceeds 20%, stop and hand back.

## Implementation Steps

**Step 1 and Step 2 are mutually dependent** — they must be in the same
branch tip before any push lands on `origin/spark-primary-industry-packs`.
The intermediate state (manifest flipped but `_skip_spark` still
skipping, or vice-versa) leaves `verify_skips --junit` red; that gate
is run by the shipped CI workflow (`.github/workflows/tests.yml`) and
a push of only half the change would turn CI red on the feature branch
immediately. Sequencing inside the branch: Step 1 first (one-line edit
per row, easy to revert), Step 2 second (larger diff), Step 3 verifies
before any Step 4 / Step 6 docs-and-validation work.

**Commit-boundary contract (required for rollback playbook to work).**
Each Step below MUST land as its own commit on the feature branch,
named in the commit subject so the rollback playbook can reference it
unambiguously. Required commit sequence (exactly these subjects, in
this order):
1. `feat(spark-primary): step 1 — flip skip_pandas_only to required`
2. `feat(spark-primary): step 2 — unlock pandas-hardcoded callsites`
3. `test(spark-primary): step 3 — verify 3× severity uniformity`
   (no code change beyond the already-in-commit-2 print lines; this
   commit carries Step 3 logs and any plan amendments triggered by
   the spot-runs, OR is empty-but-annotated if the plan emerged
   unchanged)
4. `docs(spark-primary): step 4 — refresh README + config.yaml comments`
5. `chore(spark-primary): step 5 — retain sanctioned_skip_reason (negative step)`
   (retention record; zero code diff)
6. `chore(spark-primary): step 6 — full-tree verification logs`
   (Step 6's local run produces JUnit logs; this commit lands any
   plan addenda surfaced by the full-tree run, OR is empty-but-annotated)
Do **not** squash Steps 1 + 2 into a single commit even though they
are mutually dependent at push time — the rollback playbook's
Case B (`git revert --no-commit` chain) requires them to be
separately addressable. "Mutually dependent at push" means they must
be in the same **branch tip** before any push, not in the same commit.
Commits 3, 5, and 6 may be empty if their logical work is already
captured in earlier commits; use `git commit --allow-empty` with a
body explaining why (e.g., "Step 3 spot-runs passed without plan
amendment — see local log at /tmp/qa/step3/"). Empty commits here are
a deliberate audit-trail choice, not bookkeeping waste.

- [ ] **Step 1 — Flip the manifest.**
  Edit `tests/test_e2e_industries/_skip_manifest.py:53-68` —
  change `"skip_pandas_only"` to `"required"` for `shape_pass`,
  `shape_fail`, `query_join`, `pattern_pass`, `pattern_fail`. No
  structural changes (keep `Entry`, `MANIFEST`, `verify_skips`).
  Gate: `python3 -m tests.test_e2e_industries._skip_manifest --help`
  still works; `len(MANIFEST) == 78`.

- [ ] **Step 2 — Unlock the 15 hardcoded `pandas` call-sites and
  refresh every stale prose that implies Spark is skipped.**
  In each of `tests/test_e2e_industries/{retail,life_sciences,financial_services}/test_*_e2e.py`:
  - Remove the top-of-function `_skip_spark(backend_type, "<scenario>")`
    line for shape_pass, shape_fail, query_join, pattern_pass, pattern_fail.
  - Change `_make_backend("pandas", {...})` → `_make_backend(backend_type, {...})`
    in those same five functions.
  - Delete the now-unused `_skip_spark` helper **and** its
    `sanctioned_skip_reason` import from each industry test module.
  - Update the **module docstrings** that declare
    "Pandas-only scenarios (shape / pattern / query_join) must skip
    for `backend_type == 'spark'`…"
    (`tests/test_e2e_industries/retail/test_retail_e2e.py:8-10`,
    `tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py:12-14`,
    `tests/test_e2e_industries/financial_services/test_financial_services_e2e.py:8-10`)
    to reflect that shape/pattern/query_join now run on both backends.
  - Update the **per-section banner comments** that label Shape,
    Pattern, and Query JOIN as `Pandas-only`:
    - Retail: `test_retail_e2e.py:385-387` (Shape),
      `test_retail_e2e.py:478-480` (Query JOIN),
      `test_retail_e2e.py:521-523` (Pattern).
    - Life Sciences: `test_life_sciences_e2e.py:430-432` (Shape),
      `test_life_sciences_e2e.py:524-526` (Query JOIN),
      `test_life_sciences_e2e.py:569-571` (Pattern).
    - Financial Services: `test_financial_services_e2e.py:383-385`
      (Shape), `test_financial_services_e2e.py:518-520`
      (Pattern). (Financial Services' Query JOIN section header
      does not carry a Pandas-only label; verify on edit.)
  - Update the **manifest backend-contract comment** at
    `tests/test_e2e_industries/_skip_manifest.py:48-51` — the
    "Shape / Pattern / Query JOIN → Pandas required, Spark
    skip_pandas_only" line becomes false after Step 1 and must be
    rewritten to "all thirteen scenarios → both backends required"
    (or equivalent). Also update any in-file row comment that says
    "pandas-only" adjacent to the five flipped rows.
  - Update the **prewarm fixture comment** at
    `tests/test_e2e_industries/conftest.py:117-120` that claims
    missing `pyspark` means "every Spark test will skip via the
    backend fixture". That's been false since the shipped
    `_make_spark_backend` imports `pyspark` at module level
    (`tests/_e2e_support/backends.py:54-58`) — missing `pyspark`
    raises `ImportError`, which pytest treats as an **error**, not
    a skip. Rewrite the comment to describe the actual behavior
    (error at backend construction; prewarm skips only because it
    wraps the import in `try/except ImportError` to avoid a hard
    crash during the session-scoped prewarm, letting the
    per-parametrization error surface at the test level instead).
  - **Add six `[SEVERITY-GATE]` diagnostic `print` lines** — one
    inside each of the six functions whose severity assertion is a
    range rather than an exact match (Step 3, Group B). Exact
    insertion points, placed immediately before the
    `assert vr[0].severity in (Severity.WARNING, Severity.ERROR)`
    line:
    - `tests/test_e2e_industries/retail/test_retail_e2e.py:475` (shape_fail;
      function def at `:433`),
      `:593` (pattern_fail; function def at `:568`).
    - `tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py:521`
      (shape_fail; function def at `:478`),
      `:643` (pattern_fail; function def at `:616`).
    - `tests/test_e2e_industries/financial_services/test_financial_services_e2e.py:474`
      (shape_fail; function def at `:431`),
      `:598` (pattern_fail; function def at `:565`).
    Each cited assert-line is inside a function that takes
    `backend_type` as a parameter (verified against the six `def`
    headers above); the earlier rounds' references to
    `:524`/`:518` were section-banner lines for the Shape header
    comment (handled by the banner-comment bullet), not the assert
    insertion point.
    Line to add (verbatim, with the industry token filled in per
    file):
    ```python
    print(
        f"[SEVERITY-GATE] <industry>/<scenario_key> "
        f"{backend_type} {vr[0].validation_type}="
        f"{vr[0].severity.name}",
        flush=True,
    )
    ```
    The three industries' `<industry>` token is `retail`,
    `life_sciences`, `financial_services` respectively; the
    `<scenario_key>` token is `shape_fail` or `pattern_fail` to
    match the corresponding function. Each line is retained in
    the merged code as permanent instrumentation — see Step 3
    mechanism note for the retention rationale.
  Gate (at end of Step 2): `pytest tests/test_e2e_industries/<industry>/test_<industry>_e2e.py -v`
  shows 27 tests collected (13 scenarios × 2 backends + 1 YAML-inventory
  test), and no `SKIPPED` lines matching the sanctioned-reason string
  (`SparkBackend not supported for …`). Additional gate runs **only
  after Step 4** (because Step 4 is where README / YAML comment
  cleanup happens): `grep -R -n -i 'pandas-only\|pandas only'
  tests/test_e2e_industries/ examples/industries/` returns matches
  **only** inside `_skip_manifest.py` (retained `skip_pandas_only`
  symbol and adjacent docstring — Decision §1). Any match in
  `examples/industries/*/README.md` or `examples/industries/*/config.yaml`
  is a Step 4 bug; any match in `tests/test_e2e_industries/*_e2e.py`
  outside `_skip_manifest.py` is a Step 2 bug.

- [ ] **Step 3 — Verify Spark paths run end-to-end and are not flaky.**
  For each industry, spot-run **only the Spark variants** of the five
  newly-enabled scenarios, three times in succession per scenario
  (Decision §2 — single-run severity is insufficient to distinguish
  calibration from CV-fold-shuffle flake). Severity-assertion shapes
  split the five scenarios into two gating groups:

  - **Group A — exact-PASS assertions (pytest exit code self-gates).**
    `test_shape_pass`, `test_query_join`, `test_pattern_pass` all
    assert `severity == Severity.PASS` (or
    `result.overall_severity == Severity.PASS`) exactly. Line-level
    citations for the three PASS asserts per industry (function def
    followed by assert line):
    - retail — shape_pass `:413` / `:430`; query_join `:483` /
      `:518`; pattern_pass `:550` / `:564`.
    - life_sciences — shape_pass `:458` / `:475`; query_join
      `:529` / `:566`; pattern_pass `:598` / `:612`.
    - financial_services — shape_pass `:411` / `:428`; query_join
      `:482` / `:515`; pattern_pass `:547` / `:561`.
    Any oscillation away from PASS flips pytest exit code from 0 —
    three consecutive exit-0 runs is itself the uniformity proof.
    No extra capture needed for Group A.
  - **Group B — accepted-range assertions (value capture required).**
    `test_shape_fail` and `test_pattern_fail` assert
    `severity in (Severity.WARNING, Severity.ERROR)`. Line-level
    citations (function def followed by assert line):
    - retail — shape_fail `:433` / `:475`; pattern_fail `:568` /
      `:593`.
    - life_sciences — shape_fail `:478` / `:521`; pattern_fail
      `:616` / `:643`.
    - financial_services — shape_fail `:431` / `:474`; pattern_fail
      `:565` / `:598`.
    A WARN↔ERROR oscillation passes every run's assertion (exit 0)
    but still indicates calibration is marginal. Group B **must**
    emit the actual severity value per run so the 3× gate can
    compare it.

  **Severity-capture mechanism (Group B).** Modify the six
  `test_shape_fail` + `test_pattern_fail` functions (2 per industry ×
  3 industries = 6 additions, performed as part of Step 2's test-body
  edits) to print the observed severity immediately before the
  accepted-range assertion:

  ```python
  # In test_shape_fail and test_pattern_fail, right before
  # `assert vr[0].severity in (Severity.WARNING, Severity.ERROR)`:
  print(
      f"[SEVERITY-GATE] <industry>/<scenario_key> "
      f"{backend_type} {vr[0].validation_type}={vr[0].severity.name}",
      flush=True,
  )
  ```

  These are permanent diagnostic prints (not Step-3-only scaffolding):
  they cost nothing on green runs, document which branch was taken,
  and give any future debugger an unambiguous cross-run signal without
  re-running the scenario. Review rationale: six one-line `print`
  calls against six existing test functions is cheaper than a
  conftest-level plugin or stdout-parsing-via-HTML, and avoids mutating
  the shipped `_make_storage` helper (which hardcodes `db_path=":memory:"`
  at `tests/_e2e_support/backends.py:91-94` and persists nothing to
  disk across pytest invocations — so any post-hoc SQL capture is
  structurally impossible without that change).

  Step 3 spot-run command (per industry). The block is wrapped in a
  `bash <<'BASH' … BASH` heredoc so the `PIPESTATUS` syntax works
  identically on macOS (parent shell is zsh by default — zsh's
  equivalent is `$pipestatus[1]`, a different name and 1-indexed,
  which would silently produce an empty string under bash syntax)
  and on GitHub Actions `ubuntu-latest` (bash). Run the block
  verbatim from any parent shell — zsh, bash, or `sh` — and the
  heredoc redirects execution into bash:
  ```bash
  bash <<'BASH'
  set -o pipefail   # required: tee masks pytest's exit code without this
  mkdir -p /tmp/qa/step3
  : > /tmp/qa/step3/<industry>-stdout.log
  : > /tmp/qa/step3/<industry>-exit-codes.txt
  for i in 1 2 3; do
    QUALIFIRE_ARTIFACTS_DIR=/tmp/qa/step3/run${i}-<industry> \
    pytest tests/test_e2e_industries/<industry>/test_<industry>_e2e.py \
      -v -s --tb=short \
      -k '(shape or pattern or query_join) and spark' \
      --junitxml=/tmp/qa/step3/<industry>-run${i}.xml \
      2>&1 | tee -a /tmp/qa/step3/<industry>-stdout.log
    ec=${PIPESTATUS[0]}   # pytest's actual exit code, not tee's
    echo "run${i}=${ec}" >> /tmp/qa/step3/<industry>-exit-codes.txt
    if [ "${ec}" -ne 0 ]; then
      echo "HARD-STOP: pytest run${i} for <industry> exited ${ec}" >&2
      exit 1
    fi
  done
  BASH
  ```
  Three moving parts are independently required and none of them
  alone is sufficient:
  - `set -o pipefail` changes the pipe's exit status to the
    rightmost non-zero exit code so that `${PIPESTATUS[0]}` is not
    the only signal carrier (it also makes `$?` after the pipe
    usable as a secondary check).
  - `${PIPESTATUS[0]}` is what actually captures pytest's exit code
    (not `tee`'s). A bare `pytest … | tee …` followed by `$?`
    without pipefail returns tee's exit code (normally 0), silently
    swallowing a mid-run assertion failure.
  - The explicit `if [ "${ec}" -ne 0 ]; then exit 1; fi` branch is
    what **actually aborts the loop**. `set -o pipefail` alone does
    not cause the `for` loop to stop — pipefail only propagates the
    exit code through the pipe; loop termination requires the
    explicit test + `exit 1`. Without this branch, a failing run1
    would still run run2 and run3, and only the last run's exit
    code would reach the outer shell.
  `exit-codes.txt` captures all three exit codes for post-hoc audit
  (even though the loop stops on the first non-zero, the file
  preserves the history of the partial run for the PR body).
  Notes on the invocation:
  - `-s` (equivalently `--capture=no`) is required for the
    `[SEVERITY-GATE]` prints to reach stdout; without it pytest
    buffers test stdout and the grep below finds nothing.
  - `QUALIFIRE_ARTIFACTS_DIR` is set to a **fresh per-iteration
    directory** so each run's report artifacts (written by the
    autouse `_industry_report` fixture — per-industry defs at
    `tests/test_e2e_industries/{retail,life_sciences,financial_services}/conftest.py:22`,
    using `_report_path_for` helper at
    `tests/test_e2e_industries/conftest.py:149-156`) land in a
    distinct tree. The shipped `SQLiteStorage` is in-memory per run
    so there is no DB state to reset; the per-iteration dir exists
    purely to keep JUnit / HTML artifacts disambiguated for
    post-hoc inspection.
  - Step 3 JUnit XML paths (`/tmp/qa/step3/<industry>-run${i}.xml`)
    and artifact dirs (`/tmp/qa/step3/run${i}-<industry>`) are
    **disjoint** from Step 6's paths (`/tmp/qa/junit.xml`,
    `/tmp/qa/junit-main.xml`, `/tmp/qa/junit-feature-nonE2E.xml`
    with `QUALIFIRE_ARTIFACTS_DIR=/tmp/qa`) — no collision even if
    Step 3 and Step 6 are re-run without a global
    `rm -rf /tmp/qa` between them.

  **Gate script** (runs after the 3× loop per industry):
  ```
  python3 <<'PY'
  import re, sys, collections, pathlib
  industry = "<industry>"
  log = pathlib.Path(f"/tmp/qa/step3/{industry}-stdout.log").read_text()
  pat = re.compile(r"\[SEVERITY-GATE\] (\S+) (\S+) (\S+)=(\S+)")
  seen = collections.defaultdict(list)
  for scenario, backend, vtype, sev in pat.findall(log):
      seen[(scenario, backend, vtype)].append(sev)
  expected_keys = 2  # shape_fail + pattern_fail per industry
  if len(seen) != expected_keys:
      print(f"CAPTURE INCOMPLETE: got {len(seen)} keys, expected {expected_keys}")
      print(dict(seen)); sys.exit(2)
  bad = {k: v for k, v in seen.items() if len(set(v)) > 1 or len(v) != 3}
  if bad:
      print(f"SEVERITY DRIFT across 3 runs: {bad}"); sys.exit(1)
  print(f"UNIFORM across 3 runs: {dict(seen)}")
  PY
  ```
  Exit-0 from this script plus exit-0 on all three `pytest`
  invocations together satisfies Exit criterion #6.

  Expected each run:
  - `test_shape_pass[spark]` → PASS (anomaly_score below warn)
  - `test_shape_fail[spark]` → WARNING or ERROR (expected distributional drift fires)
  - `test_query_join[spark]` → PASS. The concrete assertion differs
    per industry: retail asserts a SUM revenue floor on a `sales ×
    products` join (`tests/test_e2e_industries/retail/test_retail_e2e.py:483-518`);
    financial services asserts a `total_volume` aggregate on a
    `transactions × merchants` join
    (`tests/test_e2e_industries/financial_services/test_financial_services_e2e.py:482-515`);
    life sciences asserts `orphan_count = 0` on a `labs LEFT JOIN
    patients` dimension check
    (`tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py:529-566`).
    All three are single-PASS scenarios on the shipped code; Spark
    simply needs to produce identical numerics via the `.execute_sql`
    path.
  - `test_pattern_pass[spark]` → PASS (AUC strictly below the 0.65
    warning floor on unshifted data)
  - `test_pattern_fail[spark]` → WARNING or ERROR (AUC > 0.60 on
    shifted data, SHAP drivers not None)
  Hard-stop trigger (Decision §2): **any** severity change across the
  three runs for the same scenario → stop, capture reproducer, amend
  plan before proceeding. Uniform severity across three runs is the
  stability gate this plan requires before Step 4 edits documentation.
  Prereq: the industry E2E paths reach **five** optional third-party
  Python modules plus one JVM, each gated differently:
  - `scikit-learn` — `pytest.importorskip("sklearn")` via
    `_require_sklearn()` helper at the top of every Shape and Pattern
    test (retail 72–73, life_sciences 72–73, financial_services 69–70;
    called by `test_shape_*`, `test_pattern_pass`, `test_pattern_fail`).
    Missing → SKIP → after Step 1, `verify_skips`-red.
  - `prophet` — `pytest.importorskip("prophet")` inside each Trend test
    (retail 305 and 347, life_sciences 357 and 391,
    financial_services 314 and 348). **Trend rows are already
    `("required", "required")` in the shipped manifest**
    (`tests/test_e2e_industries/_skip_manifest.py:61-62`), so missing
    `prophet` means the four trend parametrizations per pack SKIP
    → `verify_skips` ERRORs on any `required` skipped row
    (`tests/test_e2e_industries/_skip_manifest.py:229`). This is
    **not** a new exposure introduced by this feature — it's the
    pre-existing contract. Local spot-runs for Step 3 don't invoke
    trend (see Step 3 selector), but Step 6's full-tree run does and
    must have `prophet` installed, same as today.
  - `shap` — `pytest.importorskip("shap")` inside each
    `test_pattern_fail` (retail 571, life_sciences 622,
    financial_services 572). Missing → SKIP → after Step 1,
    `verify_skips`-red (Pattern-fail is now `required`).
  - `pyspark` — **not** gated via `importorskip`. It is imported
    directly by `_make_spark_backend`
    (`tests/_e2e_support/backends.py:54-58`) and by the prewarm
    fixture (`tests/test_e2e_industries/conftest.py:115-121`). The
    prewarm fixture wraps `getOrCreate()` in a broad-exception guard
    that emits a `RuntimeWarning` if Spark is broken-but-installed,
    but missing `pyspark` entirely makes `_make_spark_backend` raise
    at backend construction — which fails the Spark parametrizations
    **as errors**, not skips. Mandatory in any environment running
    this feature's Spark paths.
  - `pandasql` — **not** gated via `importorskip`, but hard-imported
    inside `PandasBackend.execute_sql`
    (`qualifire/backends/pandas_backend.py:28-36`) which raises
    `ImportError` if missing. Every `test_query_join[pandas]` hits
    this path (retail 483–518, life_sciences 529–566,
    financial_services 482–515). It's listed as a `[dev]` extra in
    `pyproject.toml:40`, so `pip install -e ".[all,dev]"` covers it
    — but any local environment without the `[dev]` extra will
    error (not skip) on pandas query_join, which is a `required`
    row in the manifest.
  - **Java runtime** — `SparkSession.builder.getOrCreate()` needs a
    JVM on PATH. CI installs Temurin 17 via `actions/setup-java`
    **before** pip-install (`.github/workflows/tests.yml:22-27`),
    and it is non-negotiable; pyspark alone will raise
    `JAVA_HOME`/`PySparkRuntimeError` at session build time. Local
    verification must have Java 17 (or a compatible JDK) on PATH.

  CI installs all Python modules via `pip install -e ".[all,dev]"`
  plus an explicit `pip install "pyspark>=3.5"`
  (`.github/workflows/tests.yml:28-36`). Local verification must
  match — do **not** spot-run with any of these missing, and do
  **not** rely on the plan's "just `shap`" framing that was corrected
  over rounds 2-4.

- [ ] **Step 4 — Update coverage docs in each industry README **and**
  YAML inline comments.**
  For each of `examples/industries/{retail,life_sciences,financial_services}/`:
  - **README.md**:
    - Replace every "pandas only" Backends cell with "pandas, spark"
      (parity with the other rows). **Retail** has 5 such cells
      (`README.md:27-31`); **life sciences** has 3 such cells
      (`README.md:23-25`); **financial services** has 3 such cells
      (`README.md:23-25`).
    - Delete the trailer paragraph in each README that begins
      "Spark skips for `shape`, `query_join`, and `pattern` are …".
  - **config.yaml** — delete the `(Pandas-only)` suffix in inline
    section-header comments:
    - `examples/industries/retail/config.yaml:147` (Shape),
      `examples/industries/retail/config.yaml:190` (Query JOIN),
      `examples/industries/retail/config.yaml:216` (Pattern).
    - Same three-section pattern exists in
      `examples/industries/life_sciences/config.yaml:154` and
      `examples/industries/financial_services/config.yaml:135`;
      audit and flip every `(Pandas-only)` comment there.
  - Leave the shipped "running the YAML standalone" + pytest sections
    and all YAML *configuration values* (thresholds, filters,
    hyperparameters) untouched — only inline comments change.
  Gate: `grep -R -n -i 'pandas-only\|pandas only' examples/industries/`
  returns zero matches after Step 4. (Plan rationale: `idea.md:60`
  scopes both README and `config.yaml` under "examples/industries
  docs cleanup"; limiting Step 4 to README-only would leave the
  `config.yaml` comments stale.)

- [ ] **Step 5 — Keep `sanctioned_skip_reason` and the
  `skip_pandas_only` branch in `_skip_manifest.py`.**
  After Step 2, `sanctioned_skip_reason` is no longer imported from any
  industry test, and no manifest row is `skip_pandas_only`. Both the
  function and the branch remain **intentionally retained** (Decision
  §1) as a round-trip safety net for any future Pandas-only skip; this
  step is a negative step — explicitly do **not** remove them, even
  though a linter or reviewer may flag them as dead.

- [ ] **Step 6 — Full-tree verification pass (pytest + shipped CI
  gates).**
  The shipped CI workflow (`.github/workflows/tests.yml`) has **four**
  gates, not one. Step 6 must reproduce all four locally:
  1. Pytest collection + execution:
     ```
     QUALIFIRE_ARTIFACTS_DIR=/tmp/qa pytest tests/ -v \
       --junitxml=/tmp/qa/junit.xml
     ```
     The full `pytest tests/` invocation matches `pyproject.toml:49-54`
     and `.github/workflows/tests.yml:41-42`. It collects the whole
     `tests/` tree (not just E2E subtrees); the concrete assertion
     this plan makes is scoped to the E2E subtrees — see below.
  2. Per-industry report existence + 5 MB budget
     (`.github/workflows/tests.yml:44-58`) — each
     `$QUALIFIRE_ARTIFACTS_DIR/industry-reports/<industry>/report.html`
     exists and the tree is ≤ 5 MB per pack.
     **Portability note**: the shipped gate uses `du -sb` (GNU
     coreutils, byte-accurate). On Darwin `du -sb` is not a valid
     flag combination. Locally on macOS substitute `du -sk … | awk
     '{print $1 * 1024}'` or `find … -type f -exec stat -f%z {} \; |
     awk '{s+=$1} END {print s}'` to reproduce the same byte budget
     check; the **CI gate itself is authoritative** — if local and
     CI diverge, CI wins. This is a verification-reproducibility
     caveat, not a plan deliverable.
  3. Skip-manifest verification
     (`.github/workflows/tests.yml:60-61`):
     ```
     python3 -m tests.test_e2e_industries._skip_manifest \
       --verify-skips --junit /tmp/qa/junit.xml
     ```
     Must return 0. After the flip, **no** row should be skipped with
     the sanctioned reason; `verify_skips` ERRORs if any `required`
     row is missing or skipped.
  4. Per-pack / total runtime budgets
     (`.github/workflows/tests.yml:63-103`) — ≤ 180s per pack,
     ≤ 600s total, JUnit-XML-derived. Plan hard-stop: if this fails,
     Decision §5 governs response.

  Expected pytest results (E2E subtrees only; the plan makes no claim
  about the rest of `tests/`):
  - 33 tests pass in `tests/test_e2e.py` (baseline, unchanged).
  - 3 × 27 = 81 tests in `tests/test_e2e_industries/` run, all pass.
  - Across those two subtrees, **no** test is SKIPPED with the
    `SparkBackend not supported for …` reason.
  - Additional tests under `tests/` (api, cli, engine, backends,
    validation, etc.) must be **no-regression**. The naïve
    `passed >= main_passed` check is **insufficient**: this feature
    converts 15 previously-skipped Spark rows into executions, so a
    +15 Spark delta on the feature branch can mask a separate -N
    pandas regression elsewhere. Measurement therefore excludes the
    E2E subtrees this feature modifies, and compares like-for-like:
    1. Capture `main`'s result counts at branch-creation time — run
       `main` in a git worktree (`git worktree add /tmp/qa-main main`)
       to avoid disturbing the feature-branch working tree:
       ```
       pytest /tmp/qa-main/tests -v \
         --ignore=/tmp/qa-main/tests/test_e2e.py \
         --ignore=/tmp/qa-main/tests/test_e2e_industries \
         --junitxml=/tmp/qa/junit-main.xml
       ```
       Record `passed_main_nonE2E`, `failed_main_nonE2E`,
       `errors_main_nonE2E`, `skipped_main_nonE2E` from the JUnit.
    2. On the feature branch, the same invocation excluding E2E:
       ```
       pytest tests/ -v \
         --ignore=tests/test_e2e.py \
         --ignore=tests/test_e2e_industries \
         --junitxml=/tmp/qa/junit-feature-nonE2E.xml
       ```
    3. Exit gate: **every** count must hold exactly —
       `passed_feature_nonE2E == passed_main_nonE2E`,
       `failed_feature_nonE2E == failed_main_nonE2E`,
       `errors_feature_nonE2E == errors_main_nonE2E`,
       `skipped_feature_nonE2E == skipped_main_nonE2E`.
       Any deviation in non-E2E buckets → STOP, investigate, file
       as out-of-scope regression. The feature touches only
       `tests/test_e2e_industries/` + `examples/industries/` + three
       plan/doc files, so this gate is expected to hold trivially.
    4. For the E2E subtrees themselves, the absolute counts are
       plan-locked at 33 + 81 = 114 (Exit criterion #4) and don't
       need a main comparator.
    If a `main` worktree / baseline cannot be captured locally,
    **CI is not a drop-in replacement** — the shipped `tests.yml`
    only runs the suite on the current ref; it does not compute a
    `main` delta. What CI *does* cover:
    - the E2E subtrees (`pytest tests/` runs everything; junit is
      parsed for industry-pack runtime + skip-manifest enforcement);
    - any absolute test failure or error in non-E2E suites, because
      `pytest tests/` returns non-zero and the workflow fails.
    What CI *does not* cover without the main comparator:
    - a non-E2E regression that converts PASS → SKIP (SKIP doesn't
      fail the run);
    - a hidden parametrization count reduction that leaves
      pass-count unchanged but loses coverage.

    **Diff-scope machine gate (replaces the prior manual-reasoning
    fallback).** When the `main` worktree baseline cannot be captured,
    the plan still requires a command-verifiable contract that
    non-E2E code is structurally untouched. Run:
    ```bash
    git diff --name-only main..HEAD \
      | grep -vE '^(tests/test_e2e_industries/|examples/industries/|docs/features/spark-primary-industry-packs/|\.gitignore$)' \
      | grep -v '^$' || true
    ```
    Gate: the command must print **zero** file paths. Any file
    outside the four whitelisted prefixes printing here is a hard
    stop — the non-E2E regression surface is no longer structurally
    zero, and the fallback is invalid. In that case, the main
    worktree comparator becomes mandatory (loop back to step 1
    above and install whatever is needed to run it — no exceptions,
    no PR-body reasoning substitute). This gate's success is
    required to be pasted verbatim into the PR body with the three
    exit codes `0 / 0 / 0` from (a) the `git diff` command, (b)
    `grep -v` exit 1 meaning no matches found (but `|| true`
    normalizes to 0), (c) the final invocation's exit code — so
    the reviewer can re-run and verify. No human judgment required
    beyond checking the empty output.

    **Whitelisting `.gitignore` (post-merge addendum).** Commit
    `89974c5` added `.claude/` to `.gitignore` — per-user Claude
    Code session state (`scheduled_tasks.lock`,
    `settings.local.json`) that is functionally inert noise for
    every contributor. Pure hygiene chore, zero impact on test
    collection / runtime / import resolution. Adding `.gitignore`
    to the whitelist (the fourth alternation above) keeps the
    machine gate honest: the file IS in scope for this PR by
    explicit decision, just not in scope for the validator /
    pack code that the original three-prefix whitelist guarded.
    The post-merge non-E2E comparator (run via
    `/tmp/qualifire-pr5-comparator` worktree) confirmed exact
    parity with `origin/main` — 618 = 618 non-E2E tests passing
    on both sides — verifying the change is regression-free.

## Rollback playbook (if an upstream break is discovered after push)

**Preconditions.** The Implementation-Steps preamble's commit-boundary
contract is in force — each Step landed as its own commit with the
subject line prefix specified there. Identify each commit's SHA via
`git log --oneline --grep='feat(spark-primary): step 1'` etc. before
executing any revert. If commits were squashed or reordered in
violation of the contract, STOP and hand-unwind before attempting
these recipes.

If Step 3 passes locally but Spark flakiness appears in CI or downstream
environments, rollback options depend on which part fails:

**Case A — Only Step 4 (README + config.yaml comments) fails review.**
Revertable independently; no test or gate depends on README text or
YAML comments beyond human reading.
```
git revert <step-4 commit SHA>
```
Safe in isolation because Step 4 touches `examples/industries/*/README.md`
and `examples/industries/*/config.yaml` comment lines only — neither
`verify_skips` nor `pytest` reads those.

**Case B — Step 3 uncovers Spark flakiness (Pattern CV
non-determinism or shape calibration drift).** Steps 1+2 must revert
**together as a single squashed revert commit** to avoid the
`verify_skips`-red intermediate state that the Step 1/2 mutual
dependency note calls out. The sequence:
```
git revert --no-commit <step-2 commit SHA>
git revert --no-commit <step-1 commit SHA>
git commit -m "Revert Steps 1+2 (atomic)"
```
`--no-commit` chains the reverts into a single working-tree change; a
single `git commit` at the end lands one atomic revert, skipping the
intermediate red state. The Step 2 revert will also back out the six
`[SEVERITY-GATE]` print lines — that's desired, they're coupled to
the Spark paths being enabled.

Commits 3, 5, and 6 (if non-empty) can be individually reverted via
`git revert <sha>` because their diffs are plan/log/zero. If Case A
also applies alongside Case B, revert Step 4 first in its own commit,
then run the atomic 1+2 revert.

**Never** do a two-commit revert of Step 1 then Step 2 separately —
that reintroduces the red state the plan's Implementation-Steps
preamble warns about.

## Scope gates (hard stops)

- **Validator internal rewrite** — if the `.toPandas()` conversion in
  Shape or Pattern is inadequate for the Spark path (memory, schema,
  or numerical divergence), STOP. Raise a new feature under
  `docs/features/` for distributed Shape/Pattern. Do not monkey-patch
  the validator inside this PR.
- **`SparkBackend.sample_records` redesign** — if sampling variance
  breaks Shape or Pattern thresholds, STOP. Capture the reproducer
  and raise a separate feature for seeded Spark sampling (discussed in
  `qualifire/core/engine.py:77`). Do not introduce it under cover of
  this feature.
- **Budget overrun > 20%** — STOP, see Decision §5.

## Risks

### R1 — Spark sampling non-determinism reaches Pattern CV folds.
`SparkBackend.sample_records` uses unseeded `ORDER BY RAND() LIMIT n`
(`qualifire/backends/spark_backend.py:77-82`). In the affected
scenarios, `n_records` ≥ partition size so *row membership* is stable
(the entire filtered partition comes back), but *row order* differs
across runs. Downstream, `PatternCheckValidator` uses
`StratifiedKFold(shuffle=True, random_state=...)`
(`qualifire/validation/pattern_check.py:269-273`) — a seeded shuffle
over a differently-ordered input produces different fold membership
between runs, so AUC can vary. IsolationForest (Shape) is
order-invariant at fit time, so Shape is unaffected. **Mitigation**:
Step 3's three-run repetition explicitly targets this; Decision §2
hard-stops on any severity variance across the three runs. The
fallback (if variance > acceptable): raise a separate feature for
seeded `SparkBackend.sample_records` and do not ship the Pattern
Spark parametrization in this PR — see Scope gates.

### R2 — `createDataFrame` fails on mutated pandas frames with nullable
columns. `_make_spark_backend` normalizes NaN → None before
`createDataFrame`, so `shape_fail`'s `currency = None` and
`pattern_fail`'s reassignment both produce a nullable-consistent
frame. **Mitigation**: Step 3's spot-runs verify each of the five
mutated scenarios individually.

### R3 — Adding 15 Spark runs blows the 180s per-pack budget.
Each Spark test pays ~1–2s for `createDataFrame` + `.toPandas()` on
a ≤7k-row frame, plus one-off sampler plan compilation. Upper bound:
~30s added per pack. JVM boot is already amortized by
`_prewarm_spark_session`. **Mitigation**: Step 6 verification gate
catches overruns; Decision §5 governs response.

### R4 — Example YAMLs drift from pytest inventory parity.
The `test_config_yaml_inventory_matches_pytest_scenarios` gate
enforces **scenario-name inventory** between YAML and pytest
(retail 616, life_sciences 666, financial_services 620). Step 4
updates **inline comments** inside each `config.yaml` (dropping the
`(Pandas-only)` suffix on Shape / Pattern / Query JOIN section
headers) but does **not** touch scenario names, `datasets[]` entries,
`validations[]` entries, or any YAML *value*. The parity gate reads
scenario names only, so comment-only edits keep the gate green.
**Mitigation**: Step 4's sub-checklist limits YAML edits to
comment-only changes; the post-Step-4 CI run re-executes the parity
test as an automatic gate.

### R5 — Reviewer (Codex) flags the `_skip_spark` + `sanctioned_skip_reason`
machinery as dead code and demands removal of the whole
`skip_pandas_only` branch from `_skip_manifest.py`. Decision §1 locks
**retention** — the branch stays for future re-use. If review
pressure mounts, defend by pointing at §1's rationale; do not delete.

## Exit criteria

1. `tests/test_e2e_industries/_skip_manifest.py` shows no `"skip_pandas_only"`
   values in `_ROWS_PER_INDUSTRY`.
2. All 15 affected test functions use `_make_backend(backend_type, ...)`
   and have no `_skip_spark` call; the helper `_skip_spark` and the
   `sanctioned_skip_reason` import are removed from the three industry
   test modules.
3. Three industry READMEs show "pandas, spark" on every row and no
   trailer paragraph about skip sanctioning.
4. E2E subtrees: **33** base + **81** industry = **114** tests pass,
   no industry rows SKIPPED with the `SparkBackend not supported for …`
   reason. Additional tests under `tests/` (api, cli, engine,
   backends, validation, etc.) are no-regression vs. main. Per-pack
   wall ≤ 180s, total ≤ 600s (enforced from JUnit XML by
   `.github/workflows/tests.yml:63-103`). Per-industry `report.html`
   exists and the industry artifact tree is ≤ 5 MB
   (`.github/workflows/tests.yml:44-58`).
5. `verify_skips --junit` returns 0 (no unsanctioned skips; no
   `skip_pandas_only` expectations outstanding).
6. Step 3's three-run spot-check passes its two-part gate:
   (a) all three `pytest` invocations per industry exit 0 (self-gates
   Group A — the three exact-PASS scenarios); and (b) the stdout-log
   severity-uniformity script (see Step 3 command block) exits 0 for
   each of the three industries — i.e., for every
   `(industry, scenario_key ∈ {shape_fail, pattern_fail}, validation_type)`
   tuple, the three severity values captured via `[SEVERITY-GATE]`
   prints are all equal. Uniformity is verified by the scripted
   Python comparison in Step 3, not by human review. Total captured
   `[SEVERITY-GATE]` lines across all three industries: 18
   (3 runs × 2 fail-scenarios × 3 industries).
7. `sanctioned_skip_reason` and the `skip_pandas_only` enum branch
   still exist post-PR (retention per Decision §1). Machine gate:
   `python3 -c "from tests.test_e2e_industries._skip_manifest import
   sanctioned_skip_reason, Entry; assert sanctioned_skip_reason"`
   returns 0, and `grep -R 'skip_pandas_only'
   tests/test_e2e_industries/_skip_manifest.py` returns at least one
   match. A future reviewer removing these symbols would fail this
   assertion on the following PR, not on this one.
8. The six `[SEVERITY-GATE]` print lines added in Step 2 still
   exist post-PR as permanent instrumentation (see Step 3 mechanism
   rationale — they give any future debugger an unambiguous
   cross-run severity signal without re-running). Machine gate
   (count + semantic binding):
   ```bash
   # (a) Exactly six [SEVERITY-GATE] occurrences remain in the three
   #     industry test modules.
   COUNT=$(grep -R --include='test_*_e2e.py' -c '\[SEVERITY-GATE\]' \
         tests/test_e2e_industries/ \
         | awk -F: '{s+=$2} END {print s}')
   test "$COUNT" -eq 6 || { echo "GATE FAIL: expected 6 [SEVERITY-GATE] lines, got $COUNT"; exit 1; }

   # (b) Every [SEVERITY-GATE] line emits {vr[0].severity.name} (not a
   #     literal like "WARNING") and {backend_type} + {vr[0].validation_type}
   #     and is adjacent to a `severity in (Severity.WARNING, Severity.ERROR)`
   #     assertion (i.e., bound to the range-accept site it documents,
   #     not copy-pasted somewhere else).
   python3 - <<'PY'
   import pathlib, re, sys
   paths = sorted(pathlib.Path("tests/test_e2e_industries").rglob("test_*_e2e.py"))
   required_frag = (
       r"vr\[0\]\.severity\.name",   # must emit the enum name, not a literal
       r"vr\[0\]\.validation_type",  # must emit the validation type
       r"\{backend_type\}",          # must emit the backend label
   )
   bad = []
   for p in paths:
       text = p.read_text()
       # Find each [SEVERITY-GATE] occurrence (string literal context).
       for m in re.finditer(r"\[SEVERITY-GATE\]", text):
           start = max(0, text.rfind("print(", 0, m.start()))
           end = text.find(")", m.end())
           if end < 0:
               bad.append((str(p), "no closing ')' for print()"))
               continue
           block = text[start:end + 1]
           for frag in required_frag:
               if not re.search(frag, block):
                   bad.append((str(p), f"missing {frag!r} near offset {m.start()}"))
           # Verify the adjacent line asserts the range (within 4 lines after).
           lineno = text.count("\n", 0, m.start())
           lines = text.splitlines()
           window = "\n".join(lines[lineno:lineno + 5])
           if "severity in (Severity.WARNING, Severity.ERROR)" not in window:
               bad.append((str(p), f"no range-accept assert within 5 lines after offset {m.start()}"))
   if bad:
       for path, reason in bad:
           print(f"GATE FAIL {path}: {reason}")
       sys.exit(1)
   print("SEVERITY-GATE semantics OK for all six sites.")
   PY
   ```
   Both (a) and (b) exit 0 is required. This prevents a future
   reviewer from "fixing" a print into a literal (`severity=WARNING`)
   that would silently mask WARN↔ERROR oscillation while keeping the
   regex and count intact. Retention rationale is the same contract
   as Exit criterion 7: instrumentation lines that cost nothing on
   green runs but preserve cross-run diagnostic signal are retained
   symmetrically with the Pandas-only-skip machinery.

## Out of scope (explicit, for reviewer)

- Distributed-native Shape / Pattern validators.
- Seeded `SparkBackend.sample_records`.
- New demo YAML features or scenario additions.
- CI workflow file changes beyond what the existing gate runs (the
  existing `tests.yml` installs pyspark and runs the exact gate this
  plan depends on).
- Widening demo data volume past 10k rows/dataset.
