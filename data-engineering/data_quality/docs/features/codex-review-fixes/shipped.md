---
shipped: 2026-04-20
---

# Shipped: Address Codex Review Findings

## Summary

Phase 1 (correctness bugs) and Phase 3 (README / docs cleanup) from
the Codex-review-response plan. Closes the silent-correctness hazards
in `validate(df=...)`, `custom_query.filter`, and config-level backend
wiring, plus the documented-but-broken paths in the CLI. Phase 2
(JDBC wire-up, Pandas support-matrix decision, `step` bucketing)
remains in the plan and will ship as follow-up PRs per the pinned
sequencing.

Fifteen rounds of Codex adversarial review iterated on the design and
implementation. Three commits land on `main`.

## Key Changes

**Correctness fixes (silent-wrong-answer class)**
- `validate(df=...)` now actually validates the caller's DataFrame,
  routed through the engine's existing backend-agnostic
  `register_table` / `createOrReplaceTempView` path so Pandas and
  Spark both work. Temp-view teardown is guaranteed on success and
  error paths via `_drop_temp_view`.
- `custom_query.filter` is rejected at config load with a hard
  migration error pointing users to inline `WHERE` inside `sql`
  (it was accepted and silently ignored before). The dead `filter`
  arg is removed from the collector.
- `validate_query(...)` now requires an explicit `name=` whenever a
  history-backed validator (historical / forecast / anomaly) is
  attached. Previously two unrelated `validate_query()` calls defaulted
  to `name="custom_query"` and silently shared drift/forecast
  baselines in storage.

**Contract / fail-fast fixes**
- `system_table_backend` is now a `Literal` on the config model, and
  unwired values (e.g. `jdbc`) fail at config load with a clear
  migration pointer instead of crashing deep inside `_init_storage`.
- `dataset.df` is rejected when sourced from YAML — it is a
  programmatic-only handle with no file representation.
- `run_config()` is split into `run_config_parsed()`; the CLI reads
  the config file exactly once and hands the parsed object to the
  engine, closing the TOCTOU window between preflight validate and
  execution.
- `load_config()` owns the `Config validation failed:` prefix on every
  raise; CLI subcommands print `str(e)` verbatim (eliminates the
  doubled-prefix operators saw on every bad-config path).

**Storage reinit — rollback-safe**
- Omitted `system_table_backend` in a parsed config no longer
  silently flips instances to `external_catalog`. Pydantic v2's
  `model_fields_set` is used to distinguish "caller omitted" from
  "caller explicitly set the default."
- A failed `_init_storage()` during a table/backend migration leaves
  the instance unchanged. `self.system_table`,
  `self.system_table_backend`, and `self._storage` are committed
  atomically only after init succeeds, so a retry still sees a
  pending migration rather than silently reusing the stale backend.
- Reinitialized storage is persisted to `self._storage`, closing the
  write-to-new / read-from-old split where later API calls reverted
  to the old backend.

**Docs / repo hygiene**
- `README.md`, `docs/configuration.md`, `docs/programmatic_api.md`
  aligned with the current behavior (no dangling
  `requirements-all.txt` reference, etc.).
- `.envrc` added to `.gitignore` — the working-tree copy hardcoded a
  macOS-only Java 8 path that would have broken Linux/Windows
  contributors and pinned CI to an EOL JDK.

## Files Changed

- `qualifire/api.py`
- `qualifire/cli.py`
- `qualifire/core/config.py`
- `qualifire/core/engine.py`
- `qualifire/collection/custom_query.py`
- `README.md`
- `docs/configuration.md`
- `docs/programmatic_api.md`
- `.gitignore`
- `tests/test_cli.py`
- `tests/test_cli_run.py`
- `tests/test_config.py`
- `tests/test_custom_query_filter_removed.py`
- `tests/test_engine.py`
- `tests/test_validate_df.py`
- `docs/features/codex-review-fixes/plan.md`

Total: 16 files, ~3,990 insertions / ~160 deletions across three
commits (`55b49b2`, `704d160`, `420dd35`).

## Testing

- New tests land alongside each fix; every regression test fails on
  `main` and passes after the fix.
- `tests/test_validate_df.py` (new) — end-to-end coverage for the
  `validate(df=...)` engine path (Spark + Pandas branches, teardown
  on raise, temp-view naming, sanitized-name non-leak).
- `tests/test_custom_query_filter_removed.py` (new) — asserts
  `custom_query.filter` raises `QualifireConfigError` at load and
  that the collector no longer accepts the arg.
- `tests/test_cli.py` additions:
  - `TestRunConfigParsedStorageReinit`,
    `TestRunConfigParsedStoragePersistence`,
    `TestRunConfigParsedBackendOmitted`,
    `TestRunConfigParsedReinitFailureRollback` — full coverage of the
    storage-reinit rollback contract across rounds 11–13.
  - `TestCLIConfigErrorPrefixNotDoubled` — literal count assertion so
    the doubled-prefix regression would fail loud.
  - `TestValidateQueryRequiresNameForHistoryBackedValidators` — gates
    the history-keying contract for `validate_query()`.
- 203 change-scoped tests green; 3 pre-existing pyspark-dependent
  failures in `tests/test_cli_run.py` are not on the fix path (no
  pyspark in the dev environment, same behavior on `main`).

## Notes

- **Phase 2 follow-ups are still in the plan, not this PR:**
  - **P2.1 JDBC backend** — Path B (implement) pinned.
    `Qualifire.__init__` and `_init_storage()` need a `jdbc`
    branch; docs need the settings section.
  - **P2.2 Pandas** — Path A (demote docs) pinned. Mark Pandas
    experimental; list which collectors work and which don't; no
    code changes outside docs.
  - **P2.3 `step` bucketing** — Path B (implement) pinned. Needs
    `run_timestamp`-based bucketing across sqlite / external-catalog
    / delta / jdbc storages, with record-type precedence preserved
    and null-`run_timestamp` rows excluded.
- `(owner, bu, name)` storage-key namespacing for query history was
  deferred — `_require_unique_dataset_names` already rejects
  duplicate YAML dataset names, so namespacing is a secondary
  hardening rather than a silent-correctness closer. If a future
  review round surfaces a multi-tenant scenario it ships there.
- **Review iteration count was unusually high (15 rounds).** Each
  round either closed a new finding or tightened a previous fix.
  The pattern is documented in `plan.md` for future reviewers; no
  action item from it other than "expect 1–3 rounds per PR" as the
  baseline.

---

## Phase 2 — shipped 2026-04-20 (PR #2)

Phase 2 lands the three follow-ups that Phase 1 deferred. Ten
rounds of Codex adversarial review iterated on the design before
merge; every agreed finding became a regression test.

### Summary

- **P2.1 JDBC backend wire-up** — `system_table_backend='jdbc'` now
  routes through a real `JDBCStorage` rather than raising "not
  supported." Connection settings flow through a top-level `jdbc:`
  YAML block or `Qualifire(jdbc=JDBCConfig(...))`. `JDBCStorage`
  gained a schema-validation pass on existing tables, a
  connectivity probe that dispatches dialect-specific SQL (Oracle
  `SELECT 1 FROM DUAL` vs ANSI `SELECT 1`), and a `read_health_data`
  implementation so `qualifire report` works on JDBC.
- **P2.2 Pandas demotion** — Pandas docs demoted to dev/test only;
  the YAML `backend:` field documents the supported values
  explicitly.
- **P2.3 `step` bucketing across all storage backends** —
  `rule.compare.step` / `model.step` now actually buckets history
  on SQLite, Delta, External Catalog, and JDBC. Bucketing
  preserves the collection-before-validation precedence and
  excludes null-`run_timestamp` rows. The `step` defaults (`"P7D"`
  for historical, `"P1D"` for forecast) match the long-standing
  shipped behavior on `main`; setting `step: null` in YAML opts in
  to raw history.

### Key Changes (Phase 2)

**JDBC backend**
- `JDBCStorage.initialize()` now raises on connectivity failure,
  missing driver, or CREATE-table failure (previously logged +
  returned silently, hiding misconfiguration).
- `_bounded_history_read` pushes `ORDER BY run_timestamp DESC
  FETCH FIRST N ROWS ONLY` into the JDBC subquery with a LIMIT
  fallback for MySQL/SQLite dialects, capping remote work on
  long-lived system tables.
- `_bucketed_history_read` casts `run_timestamp` to `timestamp`
  before `unix_timestamp()` so engine-written ISO strings with
  microseconds (`str(datetime.now())` / `.isoformat()`) parse under
  Spark's default timeParserPolicy.
- Existing JDBC tables are schema-validated at init; drifted
  schemas surface a clear error instead of failing mid-run.
- Bucketed reads include a sparse-history fallback anchored to
  `MAX(run_timestamp)` so valid baselines survive deploy freezes
  and idle periods.
- All user-supplied names (`table_name`, `metric_name`,
  `dataset_name`, `validation_name`) are quote-escaped in JDBC
  subqueries since Spark's subquery-table interface is not
  parameterized.

**Step bucketing**
- `parse_step_seconds()` is the single entry point every backend
  uses to turn a `step` duration into integer seconds.
  `QualifireConfigError` for empty / zero / invalid durations.
- Pydantic field validators on `HistoricalCompareConfig.step`,
  `ForecastModelConfig.step`, and `SampleHistoryConfig.step` call
  `parse_step_seconds` at config-load time so typos like
  `step: 7dy` fail in `validate-config` instead of mid-run.
- Non-JDBC backends bucket via SQL window functions; JDBC buckets
  in Spark on the returned DataFrame.

**Config contract**
- `JDBCConfig.url` is optional at the model level so a templated
  `jdbc:` block can coexist with a non-JDBC backend (e.g. a shared
  YAML whose URL is only defined in prod). The model validator
  enforces URL presence only when the active backend needs it.
- `Qualifire(...)` rejects a `jdbc` branch paired with a
  non-Spark backend (Pandas) with an explicit ValueError instead
  of `AttributeError` from `self.backend.spark`.

### Files Changed (Phase 2)

- `qualifire/api.py`
- `qualifire/cli.py`
- `qualifire/core/config.py`
- `qualifire/core/duration.py`
- `qualifire/storage/jdbc_storage.py`
- `qualifire/storage/sqlite_storage.py`
- `qualifire/storage/delta_storage.py`
- `qualifire/storage/external_catalog.py`
- `qualifire/validation/forecast.py`
- `qualifire/validation/historical.py`
- `README.md`
- `docs/configuration.md`
- `docs/programmatic_api.md`
- `tests/conftest.py` (hostname resolution fix for Spark driver
  bind on macOS dev laptops)
- `tests/test_api.py`
- `tests/test_cli.py`
- `tests/test_storage/test_spark_storages.py`
- `tests/test_storage/test_step_bucketing.py`
- `tests/test_storage/test_jdbc_bucketing_real.py` (new)
- `tests/test_validation/test_forecast.py`
- `tests/test_validation/test_historical.py`

Total (Phase 2): 21 files, ~3,110 insertions / ~187 deletions
across eleven commits.

### Testing (Phase 2)

- 596 tests green on full suite (including new real-SparkSession
  regressions for the JDBC timestamp-parse fix).
- Every Phase 2 review-round finding shipped with a regression
  test that fails on the pre-fix code and passes after.
- `tests/conftest.py` auto-detects unresolvable hostnames and
  sets `SPARK_LOCAL_IP=127.0.0.1` so the Spark driver binds on
  dev laptops without manual `/etc/hosts` hacks.

### Notes (Phase 2)

- **Review iteration count (Phase 2): 10 rounds.** Higher than the
  "1–3 rounds per PR" baseline. Each round surfaced a real
  finding that either rested on a subtle laziness in Spark JDBC
  reads (DataFrame reader is lazy; schema introspection forces
  remote parse; exceptions don't fire until an action), a silent
  defaults migration hazard (flipping `step` defaults would have
  changed alert rates on every existing YAML), or a timestamp-
  format mismatch that only bites with real production data.
- **Step defaults preserved from `main`.** Round 9 caught an
  in-flight change that was flipping `HistoricalCompareConfig.step`
  and `ForecastModelConfig.step` to `None`. That would have been a
  baseline-semantics migration for every existing drift/forecast
  rule that omitted `step`. Reverted to `"P7D"` / `"P1D"`; operators
  who want raw history can set `step: null` explicitly.
- **JDBC timestamp parsing.** Round 10 caught that
  `F.unix_timestamp(F.col("run_timestamp"))` fails under Spark's
  default (CORRECTED) timeParserPolicy on engine-written strings
  with microseconds — every JDBC bucketed read would silently drop
  rows or crash. Fix is `F.col("run_timestamp").cast("timestamp")`
  before `unix_timestamp`; Catalyst's ISO-8601 parser handles both
  separators and optional fractional seconds.
