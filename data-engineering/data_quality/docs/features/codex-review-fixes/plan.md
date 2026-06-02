---
started: 2026-04-18
---

# Implementation Plan: Address Codex Review Findings

## Overview

Address the six critical findings and one secondary issue raised by
an external Codex review of the core library. This plan is the
authoritative design and execution tracker — it captures evidence
lines, proposed changes per item, tests, exit criteria, and decision
points. It has been through two rounds of external review on design;
the decisions below reflect the iterated outcome.

Work is grouped into three phases:

- **Phase 1** — unambiguous correctness bugs. Fixed first, shipped as
  one PR. No decisions needed.
- **Phase 2** — support-matrix decisions (JDBC, Pandas, `step`). Each
  has a cheap "strip from docs" path and an expensive "full wiring"
  path. Requires explicit user decisions before implementation.
- **Phase 3** — README fix and test gap recap.

## Implementation Steps

### Phase 1 — Correctness bugs (PR 1)

- [x] **P1.1** — Fix `validate(df=...)` no-op in `qualifire/api.py:144`
  - [x] Extend `DatasetConfig` to carry an optional runtime-only `df`
        handle (not serialized)
  - [x] Teach `_run_dataset()` / `_materialize_query()` in
        `qualifire/core/engine.py` to accept a pre-built DataFrame
        alongside `query` strings
  - [x] **Register via the engine's existing backend-agnostic path, not
        Spark-specific code.** The current `_materialize_query` at
        `qualifire/core/engine.py:326-332` already branches:
        `hasattr(self.backend, "register_table")` →
        `self.backend.register_table(view_name, df)` for Pandas, else
        `df.createOrReplaceTempView(view_name)` for Spark. The `df`
        path must reuse this exact branching so Pandas works
        out-of-the-box alongside Spark.
  - [x] Reuse the existing naming pattern `_qf_{dataset_name}_{run_id[:8]}`
        from `engine.py:322` (already collision-safe via `run_id`
        entropy). Do not introduce a parallel naming scheme. The
        sanitized-name concern from `wap/pattern.py:108` only applies
        if we derive the view name from a user-supplied dotted
        identifier; the engine's `_qf_`-prefixed pattern sidesteps it.
  - [x] Guarantee teardown on success and error paths via the existing
        backend-agnostic `_drop_temp_view()` at `engine.py:360-371`
        (uses `backend.drop_table()` on Pandas, `DROP VIEW IF EXISTS`
        on Spark). Mirror the try/drop flow at `engine.py:186-217`.
  - [x] Never reuse the original `table` name as the temp view name
  - [x] Regression test (Spark): `df=` pointing at data differing from
        a same-named permanent Spark table, assert metrics reflect `df`
  - [x] Regression test (Pandas): construct `PandasBackend` with a
        registered table `"prod.table"`, call `validate(table="prod.table",
        df=<other_pandas_df>, ...)`, assert metrics reflect the passed
        DataFrame rather than the pre-registered table
  - [x] Teardown test (Spark): after `validate(df=...)` returns
        (success and raise paths), temp view not present in
        `spark.catalog.listTables()`
  - [x] Teardown test (Pandas): after `validate(df=...)` returns, the
        generated view name is no longer in `backend._tables`; the
        original registered `"prod.table"` is still present
  - [x] Name-collision test: two back-to-back `validate(df=...)` calls
        in the same session (both backends) don't error

- [x] **P1.2** — Hard-remove `custom_query.filter` (does not wrap; contract mismatch)
  - **Decision pinned:** hard-remove. Rationale: the field has never
    functioned — anyone who "had it in their config" was already
    getting silent wrong behavior. Soft-deprecate would extend that
    silence for another release, when silence is the original bug. A
    hard error on the field at config-load time tells affected users
    what to do instead (put `WHERE` inside `sql`). If this is not the
    call you want, flag it before PR 1 starts — otherwise Phase 1 is
    decision-free and can proceed.
  - **Rejection mechanism:** simply dropping `filter` from
    `CustomQueryCollectionConfig` is **not sufficient**. Pydantic
    `BaseModel` defaults to `extra="ignore"`, and the repo's config
    models in `qualifire/core/config.py` do not set
    `model_config = ConfigDict(extra="forbid")` anywhere (no
    `extra=` / `model_config` / `ConfigDict` matches in the file). A
    removed field would therefore keep loading silently under the
    new schema — the exact bug we're trying to close.
  - [x] Remove `filter` from `CustomQueryCollectionConfig` at
        `qualifire/core/config.py:125`.
  - [x] Add a `@model_validator(mode="before")` to
        `CustomQueryCollectionConfig` that raises `ValueError` if
        `"filter"` is present in the incoming dict. Message should
        name the field and point to the `sql`-embedded `WHERE`
        alternative. Keep blast radius narrow — do not flip the whole
        repo to `extra="forbid"` in this PR.
  - [x] Stop forwarding `filter` at `qualifire/core/engine.py:492` / `:495`
  - [x] Remove `filter_expr` parameter from
        `CustomQueryCollector.__init__` in
        `qualifire/collection/custom_query.py`
  - [x] Remove `filter` example and reference from
        `docs/configuration.md:250` and any builder examples
  - [x] Schema test (positive): loading a config with
        `custom_query.filter` set raises a `pydantic.ValidationError`
        whose `errors()` output names the `filter` field and the
        migration hint (inspect `.errors()[0]["msg"]`).
  - [x] Schema test (negative): loading a config without `filter`
        continues to load successfully — confirms the validator only
        fires on the removed field, not on other unknown keys.
  - [x] Remove any existing test asserting `filter_expr` threading
        (none existed)

- [x] **P1.3** — `qualifire report` fail-fast on non-SQLite without Spark
  - [x] In `_cmd_report` at `qualifire/cli.py:131`, after the PySpark
        import attempt, if `backend is None` and
        `config.system_table_backend != "sqlite"`, print an explicit
        message to stderr and return non-zero before constructing
        `Qualifire(...)`
  - [x] Negative CLI test in `tests/test_cli_run.py`: monkeypatch
        PySpark import to fail, run `_cmd_report` with
        `system_table_backend: external_catalog`, assert non-zero
        exit + expected stderr string
  - [x] Keep SQLite fallback working (existing test at
        `tests/test_cli_run.py:115` must still pass)

### Phase 3 — Secondary fix (bundled into PR 1)

- [x] **P3.1** — Fix README `requirements-all.txt` reference at
      `README.md:39`. Replace with `pip install 'qualifire[all]'`.

### Gate: PR 1 review

- [ ] Run `/feature-review-plan codex-review-fixes` to open a draft PR
      with this plan for external review
- [ ] Address any reviewer feedback
- [ ] Submit Phase 1 + P3.1 code via `/feature-review-impl codex-review-fixes`
- [x] **Codex adversarial review round 1 — both HIGH findings addressed:**
  - [x] **Finding #1 (identity leak):** Temp-view swap at
        `engine.py:232-239` / `:261-269` was leaking into
        `DatasetResult.table` (persistence `table_name`) and the
        `HistoricalValidator` / `ForecastValidator` `table_name=`
        arguments, so every `validate(df=...)` wrote under a unique
        temp-view key and history reads cold-started forever.
        **Fix:** capture `logical_table` in `_run_dataset` before the
        swap (`engine.py:177-188`) and thread it through
        `_run_single_validation` and `_validate`; the two history-backed
        validators now receive `table_name=logical_table`
        (`engine.py:635,644`). `DatasetResult.table` also uses
        `logical_table` (`engine.py:274`).
  - [x] **Finding #2 (unsanitized view name):** `_qf_{name}_{run_id}`
        f-string at `engine.py:322` broke on FQN/hyphenated dataset
        names (`db.prod-table` → Spark parses `_qf_db.prod-table_...`
        as a qualified reference). **Fix:** extracted a
        `_make_view_name` helper with `_VIEW_NAME_UNSAFE_RE =
        re.compile(r"[^A-Za-z0-9_]")` and 64-char cap; both
        `_materialize_query` and `_materialize_df` now call it.
  - [x] Regression tests added in `tests/test_validate_df.py`:
        `test_fqn_table_name_is_sanitized_in_view`,
        `test_persistence_uses_caller_table_not_temp_view`,
        `test_dataset_result_table_reflects_caller_not_temp_view`,
        `test_drift_check_finds_prior_history_under_logical_table`
        (end-to-end: seeds history under logical table, asserts drift
        check finds it — fails if lookup mis-keys on the temp view).
- [x] **Codex adversarial review round 2 — both findings addressed:**
  - [x] **[HIGH] View-name injectivity**: naive sanitize
        (`[^A-Za-z0-9_]` → `_`) + `[:64]` truncate collapsed
        `orders.us` / `orders-us` and any two long names diverging past
        char 64 onto the same temp view. With `dataset_parallelism > 1`
        in one run, that meant cross-dataset reads or cleanup races.
        **Fix:** mix a 10-char sha1 of the *full unsanitized* name into
        the identifier, keep a readable sanitized prefix, and cap the
        budget to 63 chars (PostgreSQL limit). `_make_view_name`
        (`engine.py:358-387`) is now injective by construction.
  - [x] **[MEDIUM] Legacy `filter: null` rejection**: the model
        validator raised on bare key presence, breaking any templated
        YAML that emits `filter: null` / `filter: ""`. **Fix:** treat
        `None`/whitespace-only values as absent and strip them;
        only real filter expressions still raise with the migration
        hint (`config.py:132-157`).
  - [x] Regression tests (round 2):
        `test_punctuation_variants_do_not_collide`,
        `test_long_names_diverging_past_truncation_do_not_collide`,
        `test_view_name_stays_within_identifier_limit`,
        `test_view_name_contains_only_safe_chars`,
        `test_parallel_datasets_with_colliding_sanitized_names`
        (end-to-end, `dataset_parallelism=2` with colliding sanitized
        names, asserts both datasets produce distinct results),
        `test_filter_null_is_treated_as_absent`,
        `test_filter_empty_string_is_treated_as_absent`,
        `test_filter_whitespace_only_is_treated_as_absent`,
        `test_non_empty_filter_still_rejected`.
- [x] **Codex adversarial review round 3 — both findings addressed:**
  - [x] **[HIGH] `custom_query.filter` rollout hazard:** the round-1
        decision of "hard-remove at config load" turned a historically
        silent bug into a deploy-blocking failure. Version-bumping a
        running deployment would take it straight to a config-load
        failure on startup. **Fix:** downgrade to deprecation. The
        validator now strips `filter` and emits both a
        `DeprecationWarning` (for dev/test visibility) and a
        `logger.warning` (for production log pipelines, since the
        `DeprecationWarning` is filtered out by default in prod).
        Empty / null values still load silently. A future release may
        promote this to a hard error with a proper migration window
        (`config.py:132-171`). This supersedes the round-1 "hard-remove"
        decision in Technical Decisions §2.
  - [x] **[MEDIUM] `DatasetConfig.df` half-wired:** `df` was a public
        field and the engine had a real code path for it, but
        `_require_source()` still only recognized `table`/`query`/`wap`,
        so `DatasetConfig(df=...)` raised at the model layer. Only
        usable through `Qualifire.validate()`'s wrapper; a programmatic
        / engine-level integration trap. **Fix:** make `df` a
        first-class source. `table` and `df` may coexist (df supplies
        the data, table supplies the logical identity). `query`, `wap`,
        and `table/df` remain three mutually-exclusive source modes
        (`config.py:411-451`).
  - [x] Regression tests (round 3):
        `test_non_empty_filter_warns_and_is_stripped`,
        `test_non_empty_filter_in_dict_form_warns`,
        `test_non_empty_filter_logs_warning`,
        `test_legacy_config_does_not_fail_startup`,
        `test_filter_{null,empty_string,whitespace_only}_is_treated_as_absent_silently`
        (no warning on templated empty values),
        `test_df_only_is_accepted`,
        `test_df_plus_table_is_accepted`,
        `test_df_plus_query_is_rejected`,
        `test_no_source_still_rejected`.
  - [x] Updated `test_config.py` error-message regex from `"Only one"`
        to `"mutually exclusive"` (semantics unchanged; wording
        clarified to reflect the new three-mode grouping).
- [x] **Codex adversarial review round 4 — both findings addressed:**
  - [x] **[HIGH] `run_id` fragment not sanitized in view name:**
        `_make_view_name` (`engine.py:388`) sanitized `dataset_name`
        and mixed in a sha1 digest, then appended
        `self.context.run_id[:8]` verbatim. Default run_ids are
        uuid4 hex (alphanumeric, safe), but callers can set a
        custom `run_id` via `QualifireContext(run_id=...)` — a
        human-readable value like `"2026-04-20"` or a dotted
        release tag like `"prod.daily.v2"` would embed `-`/`.`
        straight into the temp-view identifier. Spark would parse
        the result as a qualified reference / arithmetic
        expression; PostgreSQL would reject it outright. **Fix:**
        run the `run_id[:8]` slice through the same
        `_VIEW_NAME_UNSAFE_RE` regex used for the dataset name
        (`engine.py:388-395`). The full identifier now matches
        `[A-Za-z0-9_]+` regardless of caller run_id shape. Chose
        in-place sanitization over hashing the run_id so the
        existing `test_engine.py` run-id assertion for alphanumeric
        `"abc12345"` still passes (no test churn for the common
        case).
  - [x] **[MEDIUM] `Qualifire.validate()` still required `table`:**
        Round-3 made `df` a first-class source on `DatasetConfig`
        and threaded `logical_table` through the engine, but the
        programmatic entry point at `api.py:144` still declared
        `table: str` (non-optional). Df-only callers had to invent
        a fake table name that then became the persistence /
        drift-history key. **Fix:** change to
        `table: str | None = None` and add an early `ValueError`
        when neither `table` nor `df` is supplied. `dataset_name`
        falls back through `name or table or "dataframe"`. The
        engine already handles `ds_config.table is None` correctly
        (`engine.py:189`: `logical_table = ds_config.table or
        ds_config.name`), so drift/forecast history lookups find
        their data under a stable dataset-name identifier
        (`api.py:144-195`).
  - [x] Regression tests (round 4) in `tests/test_validate_df.py`:
        `TestRunIdSanitizedInViewName` (4 tests —
        hyphen/dot/mixed-punctuation run_ids produce
        `[A-Za-z0-9_]+` view names; end-to-end spark-mock
        validation with `run_id="2026-04-20"` asserts the
        `createOrReplaceTempView` name and the teardown `DROP VIEW`
        both use sanitized identifiers),
        `TestValidateDfOnly` (4 tests — df-only call accepted,
        persistence keys on dataset name, drift lookup finds
        seeded history under the dataset name, neither-table-nor-df
        raises a clear `ValueError`).
- [x] **Codex adversarial review round 5 — both findings addressed:**
  - [x] **[HIGH] Unnamed `validate(df=...)` shared a persistent
        identity:** round 4 introduced a defensive fallback chain
        `name or table or "dataframe"` so df-only callers could
        omit both. Codex flagged that as a silent identity-bleed:
        every unnamed df-only call would persist under
        `table_name = "dataframe"`, so drift reads, forecast
        history, and repeat-alert suppression would cross unrelated
        callers' state. **Fix:** reject df-only calls without an
        explicit `name`. `Qualifire.validate()` now raises a clear
        `ValueError` when `table is None and name is None`, and the
        docstring calls out that `name` is required in df-only
        mode (`api.py:175-201`). The caller owns the identity
        choice — no silent default that collapses history across
        callers.
  - [x] **[MEDIUM] Duplicate dataset names shared a temp view
        within one run:** `_make_view_name` is injective on
        `dataset_name`, but the run-id fragment is shared across
        datasets in a single run. Two datasets with the *same*
        `name` therefore deterministically produced the same view,
        and under `dataset_parallelism > 1` one could overwrite or
        drop the other's temp view mid-validation. Per-dataset
        nonces would paper over that, but the deeper issue is that
        history keys, persistence, and reporting all key on
        `dataset_name` too — duplicate names are a latent bug
        everywhere downstream, not just in the view identifier.
        **Fix:** enforce unique dataset names at config load via
        a `model_validator` on `QualifireConfig` (`config.py:486-516`).
        Duplicates fail fast with a clear message at the config
        boundary instead of surfacing as a confusing parallel-run
        race.
  - [x] Regression tests (round 5) in `tests/test_validate_df.py`:
        `TestValidateDfRequiresNameWhenNoTable` (3 tests —
        `df` without `name` or `table` raises; `df` + `name`
        alone accepted; end-to-end assertion that two unrelated
        unnamed df-only attempts both raise *and* leave no
        `"dataframe"` rows in the system table),
        `TestDuplicateDatasetNamesRejected` (2 tests — config
        with two datasets sharing a name raises `ValidationError`
        at `QualifireConfig` construction; distinct names that
        share a sanitized prefix like `orders.us` / `orders-us`
        still load, guarding the round-2 collision regression
        against over-triggering). *Superseded in round 6:
        duplicate-name hard-reject was downgraded to a
        `DeprecationWarning` with per-instance view-name
        disambiguation; test class renamed to
        `TestDuplicateDatasetNamesDeprecated`.*
- [x] **Codex adversarial review round 6 — both findings addressed:**
  - [x] **[HIGH] JDBC system_table was not actually gated:**
        round 3's P1.3 fail-fast in `cli.py:_cmd_report` only
        covered the no-PySpark case. With PySpark installed,
        a JDBC-backed config fell through to
        `Qualifire.__init__` → `_init_storage`, which raised a
        cryptic generic "Unknown system_table_backend:
        jdbc" — readable as a typo rather than an
        intentionally-unimplemented backend. **Fix:** two
        belt-and-braces gates. `cli.py:_cmd_report` now
        rejects `system_table_backend == "jdbc"` *before*
        the PySpark try/except, with stderr explicitly
        naming JDBC as "not yet supported (Phase 2 P2.1)"
        (`cli.py:125-148`). `Qualifire._init_storage` grows
        an explicit `jdbc` branch with the same message, so
        programmatic callers hit the same wall
        (`api.py:97-106`). Generic else branch enumerates
        supported backends for any future unknown values.
  - [x] **[HIGH] Duplicate-name hard-reject was a rollout
        hazard:** round 5's `model_validator` raised
        `ValidationError` on duplicate dataset names at
        config load. Codex flagged that any deployment
        currently carrying duplicate names (even with the
        race latent) would *fail to upgrade* past this
        library version — a worse failure mode than the race
        itself. The race is also addressable without
        rejecting the config. **Fix:** matches the round-3
        `custom_query.filter` pattern — downgrade to
        `DeprecationWarning` + `logger.warning` so upgrades
        proceed, operators see the warning in both dev and
        prod, and the underlying temp-view race is
        *independently* neutralized by giving each
        `DatasetConfig` instance a distinct view name.
        `_make_view_name` grows an optional `instance_key`
        parameter (`engine.py:361-398`); `_instance_key_for`
        derives a 4-hex-char key from the dataset's position
        in `config.datasets` (stable and debuggable) with an
        `id()`-based fallback. Budget stays within the
        63-char PostgreSQL identifier limit. Both
        `_materialize_query` and `_materialize_df` pass the
        instance key, so parallel validation of duplicate
        names gets distinct temp views while the config
        still loads.
  - [x] Regression tests (round 6):
        `tests/test_cli.py::TestCLIReportJDBCGate` (2
        tests — `report` with `system_table_backend: jdbc`
        returns 1 with stderr mentioning JDBC and either
        "not yet support" or "P2.1"; direct
        `Qualifire(system_table_backend="jdbc", ...)`
        raises `ValueError` matching `jdbc.*not yet
        support`).
        `tests/test_validate_df.py::TestDuplicateDatasetNamesDeprecated`
        (4 tests — duplicate names emit
        `DeprecationWarning` and load successfully;
        `logger.warning` fires for production visibility
        where `DeprecationWarning` is filtered; distinct
        `DatasetConfig` instances with the same name get
        distinct view names via `_instance_key_for`;
        round-2 sanitized-prefix collision guard still
        holds). *Round-7 follow-up: added a 5th test
        (`test_value_equal_datasets_get_distinct_view_names`)
        that actually exercises the duplicate case with
        identical `table` values — the original 3rd test
        used distinct tables and silently dodged the
        regression below.*
- [x] **Codex adversarial review round 7 — both findings addressed:**
  - [x] **[HIGH] `_instance_key_for` used value-equality,
        not identity, reintroducing the collision for
        value-equal `DatasetConfig` objects:** Pydantic
        `BaseModel` compares by value, and
        `list.index(ds_config)` is equality-based. So two
        fully identical duplicates (same name *and* same
        table) both resolved to position `0` → same
        `instance_key` → same temp view under
        `dataset_parallelism > 1`. Round-6's mitigation
        was a no-op for this exact case. **Fix:** replace
        `list.index()` with an explicit identity scan
        (`for idx, cfg in enumerate(...) if cfg is ds_config`)
        so value-equal siblings still get distinct indices
        (`engine.py:414-434`). The 4-hex budget, fallback
        path, and debuggability (indices = positions in
        `config.datasets`) are all preserved.
  - [x] **[HIGH] Drift-history regression tests grepped
        for the wrong no-history message:** both
        `test_drift_check_finds_prior_history_under_logical_table`
        and `test_drift_history_lookup_keys_on_dataset_name_for_df_only`
        asserted no VR contained the substring
        `"historical values"` — but that message is emitted
        by the `"error"` branch at
        `historical.py:100`, not the actual cold-start
        path at `historical.py:114` (which emits
        `"No historical data for '{metric}', skipping
        comparison"` with `details={"cold_start": True}`).
        So if a `logical_table` routing regression
        returned zero history rows, the grep missed it
        and the tests still passed. **Fix:** positive
        assertion on `not vr.details.get("cold_start")`
        — the cold-start flag is the deterministic
        signal that the lookup returned zero rows,
        independent of which message the validator
        chose (`tests/test_validate_df.py:308-385` and
        `:744-820`).
  - [x] Regression tests (round 7):
        `TestDuplicateDatasetNamesDeprecated::test_value_equal_datasets_get_distinct_view_names`
        (fully identical duplicates — same name *and*
        table — get distinct `instance_key` and view
        names; the existing test used distinct tables
        and would have passed with the buggy
        `list.index` path). Drift tests now assert on
        `details["cold_start"]` absence instead of
        a message substring that doesn't match the
        relevant code path. *Class renamed in round 8
        to `TestDuplicateDatasetNamesRejected` after
        hard-fail was restored — tests were rewritten
        accordingly.*
- [x] **Codex adversarial review round 8 — both findings addressed:**
  - [x] **[HIGH / BLOCKING] Duplicate dataset names were
        a correctness bug, not just a rollout warning:**
        round 6 softened the hard-fail to a
        `DeprecationWarning` to avoid a rollout hazard
        (deployments carrying duplicate names would fail
        to upgrade). Codex flagged in round 8 that the
        name is *also* the history lookup key
        (`engine.py:697,706`), the persistence key
        (`engine.py:855,889`), the notification dedupe
        key, and the health report grouping key
        (`reporting/health.py:64`). So two datasets
        sharing a name still silently mix drift history,
        cross-suppress each other's alerts, and collapse
        into one row in reports — a data-integrity
        failure no warning can prevent. **Fix:** restore
        the hard-fail at config load. The round-6
        "rollout hazard" is exactly the right forcing
        function — any deployment with duplicate names is
        already producing corrupt persisted data and
        needs to rename regardless; failing on upgrade
        converts a silent corruption into a loud config
        error. Engine-level per-instance view
        disambiguation stays in place as belt-and-braces
        for programmatic callers that bypass the
        validator (`config.py:485-527`).
  - [x] **[MEDIUM / SHOULD-FIX] Docs still advertised
        JDBC as a supported backend:** `README.md:418,446`,
        `docs/configuration.md:12`, and
        `docs/programmatic_api.md:14` all listed
        `jdbc` in support tables or comment enumerations
        of `system_table_backend` values — but
        rounds 3 and 6 made the runtime reject `jdbc`
        until Phase 2 (P2.1). Users following the docs
        hit a guaranteed runtime failure. **Fix:** mark
        JDBC as "planned / not yet wired (Phase 2 P2.1)"
        in every doc, and note the runtime rejection in
        the support-matrix and config comments so the
        failure mode is visible at read time, not at
        startup.
  - [x] Regression tests (round 8):
        `TestDuplicateDatasetNamesRejected` (4 tests —
        duplicate names raise `ValidationError` at
        config load with an enumerated duplicates list;
        value-equal `DatasetConfig` instances that
        bypass the validator via `model_construct` still
        get distinct engine-level `instance_key`s and
        view names; distinct same-prefix names still
        load). Class previously
        `TestDuplicateDatasetNamesDeprecated`.
- [x] **Codex adversarial review round 9 — three findings addressed:**
  - [x] **[HIGH] `custom_query.filter` silently stripped
        for non-empty values, shipping wrong metrics:**
        round 3 downgraded the hard-fail to a
        `DeprecationWarning` + silent strip. A legacy
        config with a *real* predicate like
        `filter: "region = 'US'"` then loaded
        successfully but validated the **unfiltered**
        aggregate, so the check ran against the wrong
        slice of data. The warning lands in logs the
        operator may not monitor; the metric is wrong
        either way. **Fix:** hard-fail for any non-empty
        filter value at config load with a migration
        error that echoes the offending value *and*
        points at the correct fix (`WHERE` inside `sql`).
        Empty / null values (`filter: null`, `""`,
        whitespace) still load silently — those are a
        templating artifact, not user intent, and
        rejecting them would be the exact rollout hazard
        round 3 correctly vetoed (`config.py:136-176`).
  - [x] **[HIGH] Duplicate-name hard-fail surfaced as a
        CLI traceback, not a clean exit:** round 8
        restored the hard-fail, but `_cmd_run` and
        `_cmd_report` still called `load_config()`
        outside any `QualifireConfigError` handler. So
        any deployment whose config trips the new
        validator got a raw Python traceback instead of
        an exit-1 with a remediation line — making
        automated recovery materially harder (log
        pipelines can't key on "exit 1 + Config
        validation failed: ..."). **Fix:** wrap
        `load_config()` in both commands with the same
        `QualifireConfigError` handler
        `_cmd_validate_config` uses, plus move the call
        *before* the PySpark import so config errors
        don't pay the SparkSession cost
        (`cli.py:76-106, 134-145`).
  - [x] **[MEDIUM] CLI error paths were untested:** the
        new validators (duplicate names, legacy filter,
        JDBC) could regress into tracebacks without any
        CI signal. **Fix:** `TestCLIConfigErrorHandling`
        (4 tests — `run` and `report` both exit 1 with
        `"Config validation failed"` on stderr and *no*
        `"Traceback"`, for duplicate-name configs and
        legacy-filter configs).
  - [x] Regression tests (round 9):
        `TestCustomQueryFilterRejected` (10 tests —
        non-empty `filter` raises a migration error via
        both kwarg and `model_validate` paths; error
        echoes the offending value and names the `WHERE
        / sql` fix; null / empty / whitespace values
        load silently under `warnings.simplefilter("error")`;
        unknown extra keys still silently ignored).
        `TestCLIConfigErrorHandling` (4 tests listed
        above). Class renamed from
        `TestCustomQueryFilterDeprecated`.
- [x] **Codex adversarial review round 10 — two findings
      addressed (first review flagged both; a confirming
      second review re-raised the backend finding and
      didn't find further blockers):**
  - [x] **[HIGH] Unsupported `system_table_backend`
        values pass config-load and fail later in
        runtime-only paths:** `QualifireConfig.system_table_backend`
        was an unconstrained `str`, so `validate-config`
        accepted `system_table_backend: bogus` or
        `system_table_backend: jdbc` as "valid" and the
        failure only surfaced at runtime — as the
        misleading "requires PySpark for
        system_table_backend='bogus'" branch in
        `_cmd_report`, or a constructor `ValueError`
        traceback in `_cmd_run` on Spark-enabled machines.
        That broke the contract that a green
        `validate-config` means the config is safe to
        hand to `run`. **Fix:** constrain the field to
        `Literal["external_catalog", "delta", "sqlite",
        "jdbc"]` so Pydantic rejects typos at
        `model_validate()` with its enum-error listing
        the allowed values, and add a new
        `_reject_unwired_backend` model validator that
        fails `jdbc` at config-load with the P2.1
        migration pointer. This supersedes the
        round-6 CLI-level JDBC gate in `_cmd_report`
        (now unreachable and removed). The
        `Qualifire._init_storage` runtime JDBC branch
        stays as belt-and-braces for programmatic
        callers that bypass `load_config()`
        (`config.py:478-535, cli.py:134-145`).
  - [x] **[MEDIUM] `df` in file-based configs masquerades
        as a valid dataset source:** round 4 promoted
        `df` to a first-class source on `DatasetConfig`,
        and `_require_source` treats any non-`None` `df`
        as a legitimate source mode. Because
        `load_config()` parses YAML/JSON into the same
        model, a file config containing `df: []`,
        `df: {}`, or `df: null` now passes
        `validate-config` — but no live in-process
        DataFrame can exist in a YAML file, so the value
        is necessarily a typo or a templating artifact.
        The failure was deferred until runtime, where
        `_materialize_df` raised an opaque type error.
        **Fix:** reject any `df:` key inside a dataset
        entry in `load_config()` *before* model
        validation, with an error that names the
        offending dataset and points at the correct fix
        (programmatic API, or `table`/`query`/`wap` for
        YAML). Keeps `df` programmatic-only without
        touching the model, so existing API callers
        still work (`config.py:560-593`).
  - [x] Regression tests (round 10):
        `TestCLIBackendConfigValidation` (5 tests —
        `validate-config`/`run`/`report` reject bogus
        backend cleanly and don't leak the misleading
        "requires PySpark" message; `validate-config`
        and `run` reject `jdbc` at load-time with the
        P2.1 pointer; no tracebacks anywhere).
        `TestCLIDfFromYamlRejected` (4 tests —
        `df: []`, `df: {}`, `df: null` all rejected
        through both `validate-config` and `run`, error
        names the dataset and suggests the correct
        remediation). Existing `TestCLIReportJDBCGate`
        tests still green because the new message
        threads `jdbc` + "not yet support" + "P2.1"
        through the `QualifireConfigError` wrapper.
        *Round-11 supersession: `df: null` is now
        tolerated — see the round-11 entry below.*
- [x] **Codex adversarial review round 11 — two findings
      addressed:**
  - [x] **[HIGH] `qualifire run` read the config file
        twice, opening a TOCTOU window:** `_cmd_run`
        called `load_config(args.config)` for preflight
        and then `qf.run_config(args.config)`, which
        re-read and re-parsed the same path. A file-swap
        racer between the two reads could report
        "validation passed" for snapshot A and execute
        snapshot B. Worse, `Qualifire.run_config` only
        reinitialized storage when `system_table`
        changed, so a backend-only swap in snapshot B
        (e.g. `sqlite` → `delta` at the same path)
        reused the stale storage wiring silently and
        wrote history through the wrong backend. **Fix:**
        add `Qualifire.run_config_parsed(config)` — now
        the single source of truth for config-driven
        execution — and have `run_config(path)` delegate
        to it. `_cmd_run` hands the already-parsed
        preflight config to `run_config_parsed`, so the
        file is read exactly once. `run_config_parsed`
        also reinitializes storage when *either*
        `system_table` *or* `system_table_backend`
        changes, closing the backend-swap hole
        (`api.py:117-206, cli.py:116-126`).
  - [x] **[HIGH] Round-10 `df:` gate hard-failed on
        `df: null` — a startup-breaking compatibility
        change:** `main` ignored `df` as an extra field
        entirely; round 10 rejected every `df:` key
        regardless of value. That turns benign
        templating artifacts (`df: null` from a YAML
        emitter that writes null for absent fields, or
        `df:` left behind next to a valid `table:`) into
        an immediate `QualifireConfigError` on every CLI
        entry point on upgrade. Codex round 11 flagged
        the hard-fail as an unnecessary rollout hazard —
        null is inert by construction, cannot be
        confused with a live DataFrame, and was ignored
        on main. **Fix:** the gate now strips `df: null`
        silently (mirroring the round-3 / round-9
        tolerance pattern for `custom_query.filter:
        null`) and keeps the hard-fail for non-null
        payloads (list, dict, scalar, string — the
        actual correctness hazard the round-10 reviewer
        flagged) (`config.py:581-618`).
  - [x] Regression tests (round 11):
        replaced the round-10
        `test_validate_config_rejects_df_null` test
        with:
          - `TestCLIDfNullToleratedInYaml` (2 tests —
            `df: null` next to `table:` loads silently;
            `df: null` alone surfaces the *correct*
            missing-source error, not the round-10
            "df cannot be set from a config file"
            false-positive).
          - `test_validate_config_rejects_df_string`
            (replaces the null-rejection coverage with
            a realistic non-null mistake — `df:
            "some/path.parquet"` still fails hard).
          - `TestCLIRunNoDoubleFileRead` (1 test —
            patches `load_config` with a counter and
            asserts `_cmd_run` invokes it exactly once,
            while also asserting the engine receives a
            `QualifireConfig` object rather than a path
            string, locking in the TOCTOU guarantee).
          - `TestRunConfigParsedStorageReinit` (1 test —
            Qualifire instance built with
            `system_table_backend="sqlite"` run against
            a config that swaps only the backend to
            `external_catalog` — `_init_storage` must
            fire; round-10 reinit logic would have
            missed this).
- [x] **Codex adversarial review round 12 — two findings
      addressed:**
  - [x] **[HIGH] Reinitialized storage never persisted
        back onto the instance:** round 11 added the
        backend-change reinit path, but the newly-built
        storage handle lived only in a local variable.
        The current engine invocation used it, and every
        subsequent call (`validate`, `validate_query`,
        `health_report`, a second `run_config_parsed`)
        silently reverted to `self._storage`, which
        still pointed at the old backend. That is a
        write-to-new / read-from-old split: a migration
        appeared to succeed once and then corrupted
        history on every call after. The second call
        also kept hitting the same reinit trigger because
        `self.system_table_backend` was being updated
        but `self._storage` wasn't — effectively an
        infinite reinit loop across calls. **Fix:**
        assign the reinitialized handle to
        `self._storage` *after* `_init_storage()`
        succeeds, so failure doesn't leave the instance
        half-swapped (`api.py:190-204`).
  - [x] **[MEDIUM] Omitted `system_table_backend` in
        a parsed config silently flipped instances to
        `external_catalog`:** `QualifireConfig.system_table_backend`
        defaults to `"external_catalog"`, so a raw
        attribute comparison cannot distinguish "caller
        omitted the field" from "caller explicitly set
        external_catalog". Round-11's
        `config.system_table_backend != self.system_table_backend`
        therefore fired for every programmatic caller
        who built `Qualifire(..., system_table_backend=
        "sqlite")` and passed a parsed config without
        the field, silently redirecting history to a
        catalog that may not even exist in the
        environment. **Fix:** gate the comparison on
        Pydantic v2's `config.model_fields_set` — which
        records exactly which fields the caller
        supplied — and inherit the instance's current
        backend whenever the field was omitted. Also
        stopped unconditionally copying
        `config.system_table_backend` onto `self` when
        only the table changed (same defaulted-value
        hazard) (`api.py:186-202`).
  - [x] Regression tests (round 12):
        - `TestRunConfigParsedStoragePersistence`
          (3 tests — `_storage` holds the reinitialized
          handle; a second call with the same overrides
          is a no-op, not an infinite reinit loop; a
          follow-up read goes to the new backend, not
          the stale one).
        - `TestRunConfigParsedBackendOmitted` (3 tests
          — omitted backend preserves instance setting
          and does NOT reinit; table-only change keeps
          the instance's current backend even though
          reinit fires; explicit backend match is a
          no-op). Round-11's
          `TestRunConfigParsedStorageReinit` still
          passes because it sets `system_table_backend`
          explicitly in the `QualifireConfig` ctor,
          putting the field in `model_fields_set`.
- [x] **Codex adversarial review round 13 — one finding
      addressed:**
  - [x] **[HIGH] Failed storage reinit leaves the
        instance half-swapped and suppresses future
        retries:** the round-12 implementation mutated
        `self.system_table` and `self.system_table_backend`
        *before* calling `_init_storage()`. If init
        raised during a real migration scenario (bad
        catalog permissions, invalid table path,
        transient backend init failure), the instance
        was left in a split-brain state: metadata
        pointed at the new target, but `self._storage`
        still referenced the old backend. On the next
        call, `table_changed` / `backend_changed`
        evaluated False against those already-mutated
        fields — so the retry silently skipped
        reinitialization and kept writing history to
        the stale storage. Exactly the
        rollback/idempotency hazard this whole
        storage-reinit path was supposed to close.
        **Fix:** (1) `_init_storage()` now accepts
        keyword-only `system_table` / `system_table_backend`
        overrides (defaulting to the instance fields)
        so callers can probe a *candidate* target
        without pre-mutating `self`. (2)
        `run_config_parsed()` computes candidate
        table/backend in locals, calls
        `_init_storage(system_table=..., system_table_backend=...)`,
        and only assigns `self.system_table`,
        `self.system_table_backend`, and `self._storage`
        together *after* init succeeds. A raise leaves
        the instance unchanged, so a retry with the
        same config still sees a pending migration
        (`api.py:84-137` `_init_storage`;
        `api.py:215-240` `run_config_parsed` reinit
        block).
  - [x] Regression tests (round 13):
        - `TestRunConfigParsedReinitFailureRollback`
          (3 tests —
          `test_failed_reinit_preserves_instance_state`:
          a `_init_storage` that raises leaves
          `self.system_table`, `self.system_table_backend`,
          and `self._storage` unchanged;
          `test_retry_after_failure_still_reinits`:
          first call raises, second call with the same
          config triggers a fresh `_init_storage` rather
          than a silent no-op, proving the retry is not
          suppressed by pre-mutated instance state;
          `test_failed_reinit_passes_candidate_not_self`:
          `_init_storage` is invoked with the candidate
          table/backend as kwargs — not via re-reading
          `self` — guarding against regressions that
          reintroduce pre-mutation). Existing stubs in
          `TestRunConfigParsedStorageReinit`,
          `TestRunConfigParsedStoragePersistence`, and
          `TestRunConfigParsedBackendOmitted` were
          updated to accept `**kwargs` to match the new
          `_init_storage` signature. All 198
          change-scoped tests still pass; the 3
          pyspark-dependent `tests/test_cli_run.py`
          failures are pre-existing on `main` (no
          pyspark in the dev environment).
- [x] **Codex adversarial review round 14 — two findings
      addressed (one HIGH from the adversarial pass, one
      should-fix flagged by two independent external
      reviewers):**
  - [x] **[HIGH] Query-backed history keyed on bare
        `dataset.name` — unrelated `validate_query()`
        calls share drift/forecast baselines:**
        `validate_query()` defaulted `name="custom_query"`
        for every call, and history-backed validators
        (Historical, Forecast, AnomalyDetection) key
        storage reads and writes on that dataset name as
        the `logical_table`. Two programmatic
        `validate_query()` calls against *different* SQL
        but without explicit names therefore both read
        and wrote the same `"custom_query"` history,
        producing false drift / forecast / anomaly
        verdicts with no runtime error — exactly the
        silent-correctness class this feature set was
        meant to close. YAML-loaded configs are not
        affected because `_require_unique_dataset_names`
        already rejects duplicate dataset names at config
        load (round 8). **Fix:** mirror the existing
        `validate(df=..., name=...)` gate. Reject
        `validate_query()` without an explicit `name`
        when any history-backed validator
        (`HistoricalValidationConfig`,
        `ForecastValidationConfig`, or
        `AnomalyDetectionValidationConfig`) is attached.
        Stateless validators (SLO, threshold) still use
        the default `"custom_query"` name. Codex listed
        three options; this is option 3 — the smallest
        fix that closes the reported hazard without
        touching the storage schema. `(owner, bu, name)`
        namespacing of the history key would additionally
        guard a multi-tenant instance, but the current
        `_require_unique_dataset_names` gate makes that a
        secondary concern and it's deferred as a Phase 2
        item if it surfaces in a later review round
        (`api.py:44-57` module-level
        `_has_history_backed_validator` helper;
        `api.py:368-430` `validate_query` guard).
  - [x] **[should-fix] Doubled `Config validation failed:`
        prefix in CLI output:** `load_config()` raises
        `QualifireConfigError("Config validation failed: {e}")`
        and all three CLI subcommands (`validate-config`,
        `run`, `report`) also prepended
        `"Config validation failed: "` before printing.
        Operators saw
        `Config validation failed: Config validation failed: …`
        on every bad-config path. Two independent
        external reviewers called this out as noisy but
        non-correctness. **Fix:** make `load_config()`
        own the prefix for *every* raise it emits
        (file-not-found, YAML parse error, non-mapping
        top-level, `df:` rejection, `model_validate`
        failure). CLI subcommands now print `str(e)`
        verbatim, yielding a single clean prefix. Gives
        operators one grep anchor (`Config validation
        failed:`) for any config-load error and
        eliminates the duplication
        (`config.py:582-636` unified prefix in
        `load_config`; `cli.py:66-74, 92-100, 143-151`
        print `str(e)` verbatim).
  - [x] Regression tests (round 14):
        - `TestCLIConfigErrorPrefixNotDoubled` (3 tests
          — one each for `validate-config`, `run`,
          `report` — asserts exactly one
          `"Config validation failed"` occurrence in
          stderr for a config with a bogus
          `system_table_backend`. Literal count (not
          substring) so a regression that reintroduces
          the CLI-side prefix would be caught).
        - `TestValidateQueryRequiresNameForHistoryBackedValidators`
          (5 tests — no-name + Historical raises;
          no-name + Forecast raises; no-name +
          AnomalyDetection raises; threshold-only call
          *still* accepts the default name; explicit
          name allows history-backed validators to run,
          so the gate forces identity choice rather
          than blocks the feature).
- [x] **Codex adversarial review round 15 — one finding
      addressed (one stale external comment verified
      and dismissed):**
  - [x] **[MEDIUM] `.envrc` committed as repo default
        hardcodes macOS-only Java 8 path:** the
        working-tree `.envrc` contained
        `export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)`,
        which is a macOS-only helper (`/usr/libexec/java_home`
        does not exist on Linux/Windows) and forces JDK 8 on
        every contributor who runs `direnv allow` — even
        though the rest of the project does not otherwise
        pin a Java version. Committing it as a repo default
        would silently break contributors on Linux, quietly
        downgrade contributors on JDK 11/17, and steer CI
        toward an EOL JDK. **Fix:** added `.envrc` to
        `.gitignore` (next to `.env`). The file stays local
        for the author's environment; contributors who want
        the same ergonomics can opt in with their own
        `.envrc`. No committed setup template was added —
        the decision is "keep it local" per Codex's first
        recommended option, not "replace with documented
        opt-in." If a shared template is wanted later it
        should be `.envrc.example` with a platform-agnostic
        `JAVA_HOME` discovery (not hardcoded to
        `/usr/libexec/java_home`).
  - [x] **[external comment — stale, dismissed]** One
        external reviewer flagged a doubled
        `"Config validation failed:"` prefix in CLI output.
        Verified against the current branch: that finding
        describes the pre-round-14 state. Commit `704d160`
        already moved prefix ownership into `load_config()`
        and made CLI subcommands print `str(e)` verbatim.
        `grep -n "Config validation failed" qualifire/cli.py`
        confirms the phrase appears only in explanatory
        comments, never in `print(...)` calls. No code
        change for this comment.
  - No regression tests added for the `.envrc` change —
    `.gitignore` entries are assertion-free by nature and
    the file is untracked, so there is nothing to unit-test.
    The dismissed external comment is already covered by
    `TestCLIConfigErrorPrefixNotDoubled` from round 14.
- [ ] Iterate on code review (expect 1–3 rounds per
      `~/.claude/CLAUDE.md` guidance)

### Phase 2 — Support-matrix decisions (PR 2, PR 3, PR 4 — one each)

Decisions pinned 2026-04-18. Each ships as its own PR after Phase 1
lands.

- [ ] **P2.1 JDBC backend — Path B (implement).** Rationale: JDBC is a
      required support target.
  - [ ] Extend `Qualifire.__init__` at `qualifire/api.py:60-75` to
        accept JDBC connection settings (URL, driver, user, password,
        extra properties). Decide whether to pass as kwargs or a
        dedicated `JDBCConfig` helper during implementation.
  - [ ] Add a branch in `_init_storage()` at
        `qualifire/api.py:84-99` for `system_table_backend == "jdbc"`
        that instantiates `JDBCStorage` with the supplied settings.
  - [ ] Extend YAML config loader in `qualifire/core/config.py` to
        accept the same JDBC settings at the top level (mirror the
        existing `system_table_backend` handling).
  - [ ] API-level init test: construct
        `Qualifire(..., system_table_backend="jdbc", ...)` with a
        fake/in-memory JDBC shim and assert the returned storage is
        `JDBCStorage`.
  - [ ] Update docs in `docs/configuration.md:12`,
        `docs/programmatic_api.md:14`, `README.md:446` to describe the
        required JDBC settings, removing the "documented but broken"
        gap flagged in the review.

- [ ] **P2.2 Pandas first-class support — Path A (demote docs).**
      Rationale: Spark is the primary target; Pandas users are a
      smaller audience and full parity isn't worth the surface-area
      expansion right now.
  - [ ] Update `docs/programmatic_api.md:24` and `:202` to mark Pandas
        experimental. Explicitly list which collectors work and which
        don't (`CustomQueryCollector` and `RecencyCollector` call
        `.collect()` directly — see `custom_query.py:50-51`,
        `recency.py:63-64`, `:89`).
  - [ ] Add a "Pandas backend limitations" section in
        `docs/programmatic_api.md` with a pointer to the Spark backend
        for production use.
  - [ ] Leave `PandasBackend` implementation untouched. Leave
        `E2EPandasBackend` shim in `tests/test_e2e.py:111` untouched.
  - [ ] No code changes outside docs.

- [ ] **P2.3 `step` semantics — Path B (implement).** Rationale:
      required for drift/forecast correctness under irregular
      schedules.
  - [ ] Implement `run_timestamp`-based bucketing in
        `qualifire/storage/sqlite_storage.py:103` (use
        `strftime('%s', run_timestamp) / step_seconds` as the bucket
        key; parse `step` strings like `"P7D"` / `"P1D"` into seconds).
  - [ ] Mirror the implementation in
        `qualifire/storage/external_catalog.py:87` using Spark SQL
        (`unix_timestamp(run_timestamp) div step_seconds`).
  - [ ] Mirror in `qualifire/storage/delta_storage.py:104`.
  - [ ] Mirror in `qualifire/storage/jdbc_storage.py` (from P2.1
        Path B) — otherwise JDBC would regress on `step` support.
  - [ ] **Preserve the existing record-type precedence.** All three
        storages currently order by
        `CASE WHEN record_type = 'collection' THEN 0 ELSE 1 END, run_timestamp DESC`
        (`sqlite_storage.py:118-120`, `external_catalog.py:101-103`,
        `delta_storage.py:118-120`). Within each bucket the
        implementation **must prefer
        `record_type = 'collection'` first, then newest
        `run_timestamp`** before applying `limit`. A naive
        `GROUP BY floor(run_timestamp / step)` that picks row-latest
        inside a bucket would silently substitute validation rows for
        collection rows and change drift/forecast inputs.
  - [ ] Exclude rows with null `run_timestamp` from history results.
  - [ ] Update `qualifire/validation/historical.py:79` and
        `qualifire/validation/forecast.py:103` to pass the configured
        `step` through to `read_metric_history()` instead of only
        `limit`.
  - [ ] Integration test: persist dense collection-only history
        spanning multiple weeks; call
        `read_metric_history(step="P7D")`; assert one row per week is
        returned (not the latest N rows).
  - [ ] **Mixed-record-type regression test:** seed history where each
        bucket contains both a validation row (later
        `run_timestamp`) and a collection row (earlier
        `run_timestamp`); assert the collection row is returned for
        that bucket.
  - [ ] Null-`run_timestamp` legacy row inserted manually and
        confirmed excluded.
  - [ ] Document the precise semantics in `docs/validators.md` and
        `docs/trend_check.md` alongside the implementation.

## Technical Decisions

These were settled during the review-iteration cycle:

1. **`validate(df=...)` goes through the engine, not the API layer.**
   Temp-view lifecycle stays owned by the engine so cleanup is
   guaranteed. Ad hoc registration in `Qualifire.validate()` would
   risk leaked views and dotted-name errors.

2. **`custom_query.filter` is removed, not wrapped.** The collector's
   contract is aggregated single-row SQL. Wrapping with outer `WHERE`
   breaks common queries like `SELECT COUNT(*) AS cnt FROM t`. Users
   needing filtering put `WHERE` inside their custom `sql`.

3. **If `step` is implemented, it's defined against `run_timestamp`,
   not `collected_at`**, using bucketing semantics. Matches the
   storage protocol ordering (`qualifire/storage/base.py:110`), avoids
   a contract change, and sidesteps null-`collected_at` handling on
   legacy rows.

4. **`step` bucketing preserves `record_type = 'collection'` precedence
   within each bucket.** Drift/forecast quality depends on collection
   rows being chosen over validation rows when both exist for the same
   period — this invariant is already in the storages' `ORDER BY` and
   must survive the bucketing layer.

5. **Phase 1 ships independently of Phase 2 decisions.** Correctness
   bugs don't wait on support-matrix calls.

## Testing Strategy

- Every Phase 1 item ships with at least one regression test that
  fails on `main` and passes after the fix.
- Tests must avoid the mock patterns Codex flagged as hiding real
  bugs — specifically `E2EPandasBackend` (`tests/test_e2e.py:111`)
  and mock storages that bypass cadence logic
  (`tests/test_validation/test_historical_extra.py:15`,
  `tests/test_validation/test_forecast.py:21`).
- If P2.2 Path B is chosen, the `E2EPandasBackend` shim is deleted as
  part of the change.
- If P2.3 Path B is chosen, `step` gets integration tests that
  actually persist dense history and assert bucketing — not
  pre-selected mock returns.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Fixing `validate(df=...)` via engine plumbing leaks a temp view if an exception escapes the try/except | Guaranteed teardown in `finally` (or equivalent), mirrored test that asserts catalog is clean after a raising validator |
| Removing `custom_query.filter` breaks users whose configs set it (it never worked, but loaded silently) | Hard-raise at config load with an explicit migration message pointing to `WHERE` inside `sql` — pinned as Phase 1 behavior |
| `step` bucketing on `run_timestamp` differs subtly from what some users assume (if they imagined `collected_at`) | Document the precise semantics in `docs/validators.md` and `docs/trend_check.md` alongside the implementation |
| Phase 2 decisions stall, blocking the whole feature | Phase 1 ships as an independent PR; Phase 2 items ship one at a time as decisions land |
| Codex re-review catches further issues on the fix PRs | Expected, per `~/.claude/CLAUDE.md`. Budget 1–3 review rounds per PR |

## Open Questions

All open questions resolved 2026-04-18. Decisions recorded inline in
the Phase 2 section above. Sequencing: PR 1 = Phase 1 + P3.1,
PR 2 = P2.1 JDBC, PR 3 = P2.2 Pandas docs, PR 4 = P2.3 `step`.

## References

- Feature idea: [`idea.md`](./idea.md)
