---
started: 2026-05-07
---

# Implementation Plan: External Catalog System-Table Write Path for AIDP

## Overview

Two surgical changes to `ExternalCatalogStorage`:

1. **Switch `write_results` from SQL `INSERT INTO ... VALUES` to
   `df.write.mode("append").saveAsTable(...)`.** This mirrors the
   existing working pattern in `DeltaStorage` and is the portable
   path that every Spark-served catalog accepts.
2. **Replace the upfront `CREATE TABLE IF NOT EXISTS` in
   `initialize()` with `CREATE SCHEMA IF NOT EXISTS`.**
   `saveAsTable` creates the underlying table on first write, so
   the table-DDL is redundant. What `saveAsTable` does **not**
   create is the parent namespace — making the namespace
   provisioning explicit gives a clear early failure when it's
   missing.

No new types, no public API change, no migration of stored data.
Existing operators with a working `external_catalog` setup see
identical end state; operators previously broken on managed
Iceberg / catalog-mode-restricted tables start working.

### Stated outcome (from `idea.md`)

```python
# Before: blocked operators on AIDP managed catalogs
qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="catalog.fresh_schema.qf_history",
    system_table_backend="external_catalog",
    owner="data-team", bu="finance",
)
qf.run_config("config.yaml")
# → opaque "operation not supported on this table" or
#   "namespace not found" error mid-run

# After: namespace created up-front, writes go through saveAsTable
qf.run_config("config.yaml")
# → run completes; rows land in catalog.fresh_schema.qf_history
```

### What this plan is NOT

- **Not** a rewrite of `_ensure_columns()` (the ALTER TABLE
  migration path). It already tolerates a missing table via the
  `try / except: return` early-out — no work needed.
- **Not** a change to `DeltaStorage`, `JDBCStorage`, or
  `SQLiteStorage`. Each backend keeps its own write path; this
  feature touches only `ExternalCatalogStorage`.
- **Not** a change to the system-table column contract.
  `COLUMN_DEFINITIONS` in `qualifire/storage/base.py` remains the
  source of truth; the explicit Spark `StructType` passed to
  `createDataFrame` is built from it (mirrored from the same data
  `_ensure_columns` already uses for ALTER TABLE migrations).
  Note this **does** move the runtime schema source from
  `SYSTEM_TABLE_DDL` (the SQL string) to the StructType derived
  from `COLUMN_DEFINITIONS` — an internal-implementation pivot
  that R6 review forced; the external operator-facing contract
  (column names + types) is unchanged.
- **Not** a CREATE NAMESPACE shim for catalogs that prefer
  `CREATE NAMESPACE` over `CREATE SCHEMA`. Spark SQL accepts both
  forms as synonyms; if a future catalog rejects `CREATE SCHEMA`
  we'll add a fallback then.
- **Not** an update to the AIDP shipped install path or
  requirements files. (Bundled requirements rename rides this
  branch by happenstance; it's tracked as a separate
  packaging-cleanup commit.)

## Decision Pins

1. **Mirror `DeltaStorage.write_results` shape exactly.** Build
   `Row(**cleaned)` instances per row, then
   `spark.createDataFrame(spark_rows)`, then
   `df.write.mode("append").saveAsTable(self._table)`. Same
   per-column cleaning logic (float for `metric_value`,
   `json.dumps` for dict-typed `details_json` / `expected_value`,
   `str(v)` for everything else, `None` passthrough). This isn't
   a copy-paste win — it's deliberate convergence on a single
   write shape across the two Spark-side backends.

2. **Drop the upfront `CREATE TABLE IF NOT EXISTS`.** The
   `saveAsTable(mode="append")` call creates the underlying
   table on the first write with column types inferred from the
   DataFrame. Keeping the SYSTEM_TABLE_DDL CREATE TABLE risks
   format mismatch when the catalog default differs from what
   `saveAsTable` would produce (e.g., catalog defaults to Parquet
   but `saveAsTable` writes Delta) — append-mode writes to a
   pre-existing table preserve its format and we'd hit a
   table-format-mismatch error.

3. **Run `CREATE SCHEMA IF NOT EXISTS` for 2-part and 3-part
   table names.** Parse `self._table` on `.`. If 3-part
   (`catalog.schema.table`), namespace = `catalog.schema`. If
   2-part (`schema.table`), namespace = `schema`. If 1-part, no
   DDL — single-name tables use the session's current namespace.
   Spark SQL's `CREATE SCHEMA IF NOT EXISTS` is the standard form
   and is accepted by every catalog in scope.

4. **Tests assert the new shape and pin down "no CREATE TABLE
   issued."** The previous `test_initialize_creates_table` is
   replaced by two tests: one asserting `CREATE SCHEMA IF NOT
   EXISTS cat.schema` is the first SQL call AND no `CREATE TABLE`
   ever runs (catches a regression that re-introduces the
   problematic DDL); one asserting 1-part names skip the
   `CREATE SCHEMA` step.

5. **`test_write_results` follows `DeltaStorage`'s pattern.**
   Mock `spark.createDataFrame` to return a `MagicMock`,
   construct one row, assert `createDataFrame` called once,
   assert the rows passed in carry the expected fields, assert
   `mock_df.write.mode("append")` chain reaches
   `saveAsTable("cat.schema.history")`.

6. **`test_write_empty_is_noop` flips its assertion.** Was
   `spark.sql.assert_not_called()`; becomes
   `spark.createDataFrame.assert_not_called()` — the noop signal
   moved with the write path.

7. **(R6) Pass an explicit Spark `StructType` to
   `createDataFrame`.** Built from `COLUMN_DEFINITIONS` (the
   same source `_ensure_columns` already uses), with `StringType`
   for STRING fields, `DoubleType` for `metric_value`,
   `TimestampType` for the timestamp columns; everything
   nullable. Without this, Spark inference on a row with all-None
   optional columns raises "cannot determine type for None"
   before `saveAsTable` even runs, and on success the first
   table created carries STRING for fields the contract declares
   TIMESTAMP. The runtime schema source moves from
   `SYSTEM_TABLE_DDL` (the SQL string) to the StructType derived
   from `COLUMN_DEFINITIONS`; the operator-facing contract is
   unchanged.

8. **(R6) ISO-format timestamp strings parse to `datetime`
   inline.** Schema-typed `TimestampType` fields require real
   datetimes, not strings. `write_results` parses ISO timestamps
   in the cleaning loop; `ValueError` falls back to `None` so a
   single malformed row doesn't abort the batch.

9. **(R6) `CREATE SCHEMA IF NOT EXISTS` is best-effort.** Wrapped
   in try/except + warning. Some Unity Catalog modes and
   governed Iceberg catalogs gate the action verb behind a
   privilege check regardless of `IF NOT EXISTS`. An operator
   with append-only privilege on a pre-existing schema must still
   get past `initialize()`; missing-schema failures still surface
   at first `write_results` from the catalog's own error.

10. **(R6 round-4 — final) Strict 3-part
    ``catalog.schema.table`` identifier.** AIDP, Unity Catalog,
    Iceberg REST, and every other external catalog this backend
    targets uses 3-part identifiers. Operators with 1-part
    (default-namespace) or 2-part (schema.table) shapes have
    other backends that fit better — `delta`, `sqlite`, `jdbc`
    — and the error message points at them. The 3-part contract
    plus per-segment regex validation
    (``[A-Za-z_][A-Za-z0-9_]*``) closes the SQL-clause injection
    vector entirely: there's no LOCATION-clause or trailing-
    whitespace or embedded-semicolon shape that fits in three
    well-formed segments. The earlier "well-formed, not length-
    bounded" iteration (round 3) over-corrected after round 1's
    over-strictness; round 4 picks the contract that actually
    matches operator usage.

11. **(R6) `_ensure_columns` DESCRIBE except logs at WARNING.**
    Round 1 used DEBUG, which round-2 review correctly flagged
    as too quiet — real auth / connectivity / metastore errors
    were invisible in default operator logs. Round 2 bumped the
    level so failures are visible while preserving best-effort
    migration semantics for the legitimate "table doesn't
    exist yet" case.

12. **(R6 round-2) Malformed timestamps fail loud.** Round 1
    coerced ``ValueError`` from ``datetime.fromisoformat`` to
    ``None``; round 2 review correctly flagged that as data
    corruption for system-table infrastructure (drift / forecast
    / identity-contract lookbacks key on
    ``run_timestamp`` / ``partition_ts``). The current path
    raises with column name + bad value + producer guidance
    pointing at the calling collector / validator.

13. **(R6 round-3) Real-Spark integration coverage for the
    all-null optional-columns path.** The mocked test in round 2
    proved a schema argument was passed but did not prove
    PySpark accepts the constructed Row + StructType shape. A
    real-Spark test ``test_external_catalog_real.py`` exercises
    ``initialize()`` + ``write_results`` + ``SELECT *``
    round-trip against a live SparkSession (in-memory catalog,
    ``default`` namespace), verifying NULL preservation on
    optional columns and TIMESTAMP coercion on the ISO-string
    inputs.

## Scope gates (hard-stop conditions)

1. **Scope gate — only `ExternalCatalogStorage`.** If a refactor
   into a shared "Spark-side storage" base class becomes
   tempting, stop. The two backends keep meaningfully different
   read paths (Delta has `OPTIMIZE`; external_catalog doesn't);
   coupling the write paths via inheritance is not warranted.
2. **Scope gate — no system-table schema change.**
   `SYSTEM_TABLE_DDL` doesn't move and doesn't change shape.
   `saveAsTable` discovers columns from the DataFrame; the
   schema source of truth stays put.
3. **Scope gate — no new optional dependency.** PySpark is
   already required to use `ExternalCatalogStorage` at all (it
   takes a `SparkSession` in `__init__`). DataFrame writer is
   built-in.
4. **Scope gate — `_ensure_columns()` semantics stay
   best-effort.** The ALTER TABLE migration path tolerates a
   non-existent table via `try / except: return`. On a fresh
   install, `initialize()` runs CREATE SCHEMA, `_ensure_columns`
   no-ops (no table yet), first `write_results` creates the
   table via saveAsTable. On subsequent runs, `_ensure_columns`
   does its migration work normally. *(R6 sub-update:
   diagnostics may change — the DESCRIBE except is now a
   WARNING-level log so connectivity failures are visible.
   Behavior — best-effort, no-throw — is preserved; only the
   log volume changes.)*
5. **Correctness gate — no behavior change for working
   operators.** Anyone with a fully provisioned catalog +
   pre-existing system table + working CREATE TABLE + working
   INSERT INTO must see identical end state. The new write path
   appends to the same table; the new initialize creates a
   schema that already exists (no-op). No data migration; no
   re-shaping of historical rows.
6. **Hard-stop — saveAsTable and INSERT INTO produce
   semantically equivalent results.** If a behavioral diff
   surfaces during testing (column order matters, NULL handling
   differs, timestamp coercion differs), stop and revise.

## Implementation Phases

This feature is small enough that the phases are commit-sized.
Each phase is independently shippable / revertible.

### Phase 1 — Switch `write_results` to saveAsTable

- [x] **P1.1** — Replace SQL VALUES construction in
      `qualifire/storage/external_catalog.py:write_results` with
      the `Row` + `createDataFrame` + `saveAsTable("append")`
      shape. Mirror `DeltaStorage.write_results` exactly.
- [x] **P1.2** — Update `tests/test_storage/test_spark_storages.py
      ::TestExternalCatalogStorage::test_write_results` to assert
      the new path: `spark.createDataFrame.assert_called_once()`,
      verify rows carry expected fields, verify
      `mock_df.write.mode("append").saveAsTable("cat.schema.history")`.
- [x] **P1.3** — Update `test_write_empty_is_noop` to assert
      `spark.createDataFrame.assert_not_called()` (the noop
      signal moved).
- [x] **Exit:** `pytest -q` 895 passing.
- [x] **Commit:** `fe14d4a fix(storage): external catalog writes
      via saveAsTable, not INSERT INTO VALUES`.

### Phase 2 — Replace `CREATE TABLE` with `CREATE SCHEMA` in initialize

- [x] **P2.1** — `initialize()` parses `self._table` on `.` and
      runs `CREATE SCHEMA IF NOT EXISTS <namespace>` for 2-part
      and 3-part table names. 1-part names skip the DDL.
- [x] **P2.2** — Drop the `SYSTEM_TABLE_DDL` import (no longer
      needed in this module).
- [x] **P2.3** — Replace `test_initialize_creates_table` with
      two tests:
    - `test_initialize_creates_namespace_not_table` — asserts
      `CREATE SCHEMA IF NOT EXISTS cat.schema` is the first SQL
      call AND no `CREATE TABLE` ever issues
    - `test_initialize_skips_namespace_for_unqualified_table` —
      1-part names produce zero `CREATE SCHEMA` calls
- [x] **Exit:** `pytest -q` 896 passing (+1 new namespace test).
- [x] **Commit:** `22ecd88 fix(storage): ExternalCatalogStorage
      .initialize creates the namespace, not the table`.

### Phase 3 — R6 review fixes (post-shipped)

Phase 1 and 2 shipped, then a review round (one adversarial pass +
one codex feature-review-impl pass) flagged correctness gaps the
plan didn't initially cover. All findings shipped in one commit
because they share the same root cause cluster (dropping the
upfront DDL exposed schema-inference and namespace-DDL fragility).

- [x] **P3.1** — `_system_table_spark_schema()` builds a
      `StructType` from `COLUMN_DEFINITIONS`. Passed explicitly
      to `createDataFrame(rows, schema=...)`. Removes the
      "cannot determine type for None" failure on all-None
      optional columns and pins fresh-table types to the
      contract. (Decision Pin 7.)
- [x] **P3.2** — ISO-format timestamp strings parsed to
      `datetime` inline in the cleaning loop. Required because
      schema-typed `TimestampType` fields don't accept strings.
      (Decision Pin 8.)
- [x] **P3.3** — `CREATE SCHEMA IF NOT EXISTS` wrapped in
      try/except + warning. Append-only privileged operators on
      governed catalogs proceed past `initialize()`. (Decision
      Pin 9.)
- [x] **P3.4** — Strict identifier-shape validation: 1/2/3-part
      OK, 4+ raises `ValueError`. (Decision Pin 10.)
- [x] **P3.5** — `_ensure_columns` DESCRIBE except logs at
      DEBUG. Real auth / network failures leave a breadcrumb.
      (Decision Pin 11.)
- [x] **P3.6** — Tests added: explicit-schema reaches
      createDataFrame with the right types on load-bearing
      fields, all-None optional columns succeed, CREATE SCHEMA
      failure tolerated, 4-part identifier rejected.
- [x] **Exit:** `pytest -q` 899 passing (+3 new R6 tests).
- [x] **Commit:** `0261738 fix(storage): R6 review — explicit
      StructType, best-effort CREATE SCHEMA, identifier-shape
      validation`.

### Phase 4 — R6 round-2 review fixes

Round 2 review caught defects in the round-1 fixes themselves:
silent NULL coercion of malformed timestamps (data corruption),
DEBUG-only logging for `_ensure_columns` failures (the round-1
"narrow the except" concern was dodged, not addressed), and an
ambiguous CREATE SCHEMA warning.

- [x] **P4.1** — Malformed ISO timestamp now raises `ValueError`
      with column name + bad value + producer hint. (Decision
      Pin 12.)
- [x] **P4.2** — `_ensure_columns` DESCRIBE except: DEBUG →
      WARNING, with all three failure causes named (table-not-
      found-yet, unsupported-DESCRIBE, broken connection).
      (Decision Pin 11 update.)
- [x] **P4.3** — CREATE SCHEMA warning rewritten to distinguish
      harmless (schema exists, no privilege) from real (next
      `write_results` will fail with namespace-not-found).
- [x] **P4.4** — Full-StructType test added: every
      `SYSTEM_TABLE_COLUMNS` entry present in order, every
      field nullable, every COLUMN_DEFINITIONS type maps
      correctly.
- [x] **P4.5** — Plan scope gate 4 rewritten to allow diagnostic
      changes while preserving the behavioral contract.
- [x] **Exit:** `pytest -q` 901 passing (+2 new R6 round-2
      tests).
- [x] **Commit:** `6ec835a fix(storage): R6 round-2 review —
      fail-loud on bad timestamps, observable schema /
      _ensure_columns logging`.

### Phase 5 — R6 rounds 3 + 4 (identifier-shape iteration)

Three review iterations on the identifier shape. Round 1
landed a strict 1/2/3-part check; round 3 (after adversarial
review pushed back) relaxed to "well-formed, not length-
bounded"; the round-3 follow-up tightened per-segment regex
to plug SQL-clause injection; the operator then directed (final)
to enforce strict 3-part shape — the actual contract every
external-catalog target uses. The journey is documented for
posterity; the shipped behavior is the round-4 strict-3-part
form.

- [x] **P5.1** — Real-Spark test added in
      `tests/test_storage/test_external_catalog_real.py` —
      live SparkSession (`spark_catalog.default` namespace),
      full `initialize()` + `write_results` + `SELECT *`
      round-trip verifying NULL preservation on optional columns
      and TIMESTAMP coercion on ISO-string inputs. Mirrors the
      pattern in `test_jdbc_bucketing_real.py`. (Decision Pin
      13.)
- [x] **P5.2 (round 3 follow-up)** — Per-segment regex
      validation `[A-Za-z_][A-Za-z0-9_]*`. Closes the
      whitespace / SQL-clause injection vector the round-3
      adversarial review demonstrated.
- [x] **P5.3 (round 4 — final)** — Length check tightened to
      ``len(parts) == 3`` per operator directive. 1-part /
      2-part / 4+ part identifiers now raise at `initialize()`
      with a pointer at alternative backends (`delta`,
      `sqlite`, `jdbc`) for those shapes. Test set
      consolidated: a single `test_initialize_rejects_non_three
      _part_identifier` covers count failures (incl.
      empty-segment shapes that collapse to 4-part); a single
      `test_initialize_rejects_malformed_segment_in_3_part_
      identifier` covers per-segment regex failures on
      otherwise-3-part inputs (12 cases including SQL-injection
      vectors).
- [x] **Exit:** `pytest -q` 903 passing (+1 real-Spark
      round-trip test, +1 real-Spark malformed-timestamp test,
      +1 part-count rejection test; −1 from removing
      `test_initialize_skips_namespace_for_unqualified_table`,
      −1 from removing `test_initialize_supports_nested_namespace`,
      net +1 from prior-round 902 baseline; full sweep at 903).
- [x] **Commits:** `73e0906 fix(storage): R6 round-3 review —
      nested namespace support + real-Spark test coverage`,
      `155f33e fix(storage): R6 round-3 follow-up — strict
      per-segment identifier validation + real-Spark test
      cleanup`, *(this commit — strict 3-part)*.

## Testing Strategy

| Layer | What's tested | File |
|---|---|---|
| Unit | initialize creates namespace not table | `tests/test_storage/test_spark_storages.py::TestExternalCatalogStorage::test_initialize_creates_namespace_not_table` |
| Unit | initialize tolerates CREATE SCHEMA failure | `…test_initialize_tolerates_create_schema_failure` (R6) |
| Unit | initialize rejects non-3-part identifiers (1-/2-/4+/5- part + dot-collapse cases) | `…test_initialize_rejects_non_three_part_identifier` (R6 round-4) |
| Unit | initialize rejects malformed segments in 3-part identifiers (whitespace, leading digit, SQL-injection) | `…test_initialize_rejects_malformed_segment_in_3_part_identifier` (R6 round-4) |
| Unit | write_results goes through saveAsTable, asserts explicit StructType reaches createDataFrame | `…test_write_results` (R6 tightened) |
| Unit | all-None optional columns succeed (mocked) | `…test_write_results_handles_all_null_optional_columns` (R6) |
| Unit | malformed timestamp raises with column + value context | `…test_write_results_raises_on_malformed_timestamp` (R6 round-2) |
| Unit | full StructType: names + nullability + per-type mapping | `…test_system_table_spark_schema_full_coverage` (R6 round-2) |
| Unit | empty rows is noop (createDataFrame not called) | `…test_write_empty_is_noop` |
| Integration | real-Spark all-null round-trip + TIMESTAMP coercion | `tests/test_storage/test_external_catalog_real.py::test_write_results_round_trip_with_all_null_optional_columns` (R6 round-3) |
| Integration | real-Spark malformed-timestamp aborts before any write | `…test_write_results_raises_on_malformed_timestamp` (R6 round-3) |
| Integration | live AIDP managed-Iceberg first-run smoke | manual — run one validation against a brand-new namespace, verify rows land |

The mocked unit tests cover the contract; the AIDP smoke is the
ground truth.

## Acceptance / Exit criteria

The feature is shippable when **all** of these hold:

1. ✅ `pytest -q` passes; new tests cover Decision Pins 4–11.
2. ✅ A run against a fresh `catalog.new_schema.qf_history` (where
   `new_schema` doesn't exist) succeeds — namespace created on
   `initialize()`, table created on first `write_results`.
3. ✅ A run against an AIDP managed-Iceberg system table (where
   `INSERT INTO ... VALUES` previously failed) succeeds.
4. ✅ A run against a pre-existing fully-provisioned catalog +
   table is unchanged (no behavior diff for the happy path).
5. ✅ No `INSERT INTO` SQL appears in any test assertion or live
   code path for `ExternalCatalogStorage` (gate 4 — catches a
   regression).
6. ✅ (R6) An all-None optional-columns row succeeds — explicit
   StructType prevents the "cannot determine type" inference
   crash.
7. ✅ (R6) An operator with append-only catalog privilege (no
   schema-create privilege) gets past `initialize()` even when
   `CREATE SCHEMA` is rejected by the catalog.
8. ✅ (R6 round-4) Any non-3-part identifier (1-part default-
   namespace, 2-part schema.table, 4+ part nested namespace,
   leading/trailing/double-dot shapes that collapse to 4-part)
   raises `ValueError` at `initialize()` with a pointer at
   alternative backends (`delta`, `sqlite`, `jdbc`) for those
   shapes — no silent acceptance.
9. ✅ (R6 round-2) A non-ISO timestamp in
   `run_timestamp` / `partition_ts` / any other TIMESTAMP-typed
   column raises `ValueError` with the column name + bad value
   in the message — no silent NULL coercion of corrupt
   timestamps that downstream drift / forecast lookbacks key on.
10. ✅ (R6 round-2) The full `StructType` produced by
    `_system_table_spark_schema()` is asserted in tests —
    every `SYSTEM_TABLE_COLUMNS` entry present, every field
    nullable, every COLUMN_DEFINITIONS type maps to the right
    Spark type. Catches a future
    column-addition that misses the type_map.
11. ✅ (R6 round-4) Each segment of the 3-part identifier is
    regex-validated (`[A-Za-z_][A-Za-z0-9_]*`). Whitespace,
    leading digits, hyphens, semicolons, SQL comments, and
    `LOCATION`-clause-style injections in any segment raise
    `ValueError` at `initialize()` — the SQL-clause injection
    vector is closed entirely.
12. ✅ (R6 round-3) A real-Spark integration test exercises the
    full `initialize()` + `write_results` + `SELECT *`
    round-trip against a live SparkSession with all-None
    optional columns. NULL preservation on optional columns and
    TIMESTAMP coercion on ISO-string inputs are both verified
    against PySpark's actual write path, not just a mock.

## Out of Scope (explicit non-goals)

- Backporting the saveAsTable change to `JDBCStorage` /
  `SQLiteStorage`. Those backends use different write APIs
  (`df.write.jdbc`, parameterized `INSERT INTO ... VALUES`
  respectively) that are correct for their dialects.
- Adding a `CREATE NAMESPACE` fallback for catalogs that reject
  `CREATE SCHEMA`. Spark accepts both as synonyms; revisit if a
  catalog ever surfaces that doesn't.
- Logging the table format chosen by `saveAsTable` (Delta vs
  Parquet vs Iceberg) at info level. Useful diagnostic but
  separate concern; operators can DESCRIBE TABLE if they need to
  know.
- Migrating data from a previous-format table to a new-format
  table if the operator's catalog default has changed since they
  first provisioned. That's a one-off operator concern, not
  something Qualifire should automate.

## Risks

- **Catalog default format mismatch on saveAsTable.** Catalogs
  with a default of "managed Iceberg" produce Iceberg tables;
  catalogs defaulting to Hive Parquet produce Parquet. Both work
  for our read/write needs, but operators expecting one and
  getting the other may be surprised. Mitigation: explicit
  smoke-test in Acceptance #2; documentation in
  `docs/notifications/jdbc.md` *(or wherever external_catalog is
  documented)* covers the catalog-default behavior.
- **Pre-existing tables with stale columns** — covered by
  `_ensure_columns()` already; no new risk introduced.
- **Spark `createDataFrame` schema inference vs.
  `SYSTEM_TABLE_DDL` declared types.** ISO timestamp strings
  written through saveAsTable land as STRING columns, not
  TIMESTAMP. `DeltaStorage` already operates this way and reads
  back successfully — Spark coerces on read. This is documented
  in `_ensure_columns` (round-6 review fix).

## Effort estimate

Small. ~30 lines of code change + ~20 lines of test updates +
this plan/idea/shipped. Total ~3 hours including the
adversarial-style retro test ("no CREATE TABLE ever issued").
